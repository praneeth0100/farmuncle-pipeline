"""Arrival volumes (tonnes) per crop and mandi, from Agmarknet's public price-trend report (see docs/arrivals.md):

  arrivals/{crop}/{district}.json   {v, crop, district, start: "2018-01", unit: "tonnes", markets: {mandi_id: [t or null]},
                                     done: {"YYYY-MM-W": "date fetched"}, updated_at}

One value per month block: days 1-8, 9-15, 16-23, 24-end (Agmarknet's weeks, not Monday weeks); index
(year - 2018) * 48 + (month - 1) * 4 + block - 1. Agmarknet reports a commodity per market, so every variety at a mandi
shares one volume. A market that matches no mandi is kept under "?<its Agmarknet name>" so nothing fetched is lost.

Each run first refreshes the newest blocks of every pair (stalest first), then backfills older months newest-first, until
its time budget runs out; the next run carries on from what each file's `done` says. Values only ever get added: a later
empty answer never erases a stored number, and no market is dropped. Reads live/crop files and catalog/mandis.json from
R2; never touches facts/daily or the warehouse.

Usage: python -m farmuncle_pipeline.ingestion.publish_arrivals [--budget-minutes N] [--limit N]
"""

from __future__ import annotations

import argparse
import calendar
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime

from farmuncle_pipeline.ingestion.publish_live import IST, _encode, _get_json, slugify
from farmuncle_pipeline.ingestion.publish_weather import Pace

API = "https://api.agmarknet.gov.in/v1"
WEEKLY = API + "/price-trend/wholesale-arrivals-weekly"
FILTERS = API + "/daily-price-arrival/filters"
START_YEAR = 2018
GAP_S = float(os.environ.get("ARRIVALS_GAP_S", "1.0"))  # the server answers 429 to bursts; one call a second by default
BACKOFF_S = (60, 180, 600)  # waits after a 429 before giving up on the run (what was fetched is still saved)
QUIET_AFTER = 6  # calls with no market at all: the crop isn't traded there; check its newest block weekly, skip backfill
SAVE_EVERY_S = 600
MONTHS = {m.lower(): i for i, m in enumerate(calendar.month_abbr) if m}
STATE_ALIAS = {"keralam": "kerala", "puducherry": "pondicherry"}
DISTRICT_ALIAS = {"tn.thiruvarur": "Tiruvarur", "cg.janjgir": "Janjgeer-Champa", "ka.bangalore": "Bengaluru"}
_COLUMN = re.compile(r"arrivals_(\d+)_\d+_([a-z]+)_(\d{4})$")
_NOISE = re.compile(r"\s*\b(apmc|mandi|market|f\s*&?\s*v|grain|sandhai|committee)\b\s*")  # as identity_client._MANDI_NOISE


class RateLimited(Exception):
    pass


def block_of(day: int) -> int:
    return 1 if day <= 8 else 2 if day <= 15 else 3 if day <= 23 else 4


def index(year: int, month: int, block: int) -> int:
    return (year - START_YEAR) * 48 + (month - 1) * 4 + block - 1


def parse_column(key: str) -> int | None:
    """'arrivals_9_15_jun_2018' -> its block index; None for other columns and years before START_YEAR."""
    m = _COLUMN.match(key)
    if not m or m.group(2)[:3] not in MONTHS or int(m.group(3)) < START_YEAR:
        return None
    return index(int(m.group(3)), MONTHS[m.group(2)[:3]], block_of(int(m.group(1))))


def market_key(name: str) -> str:
    return slugify(_NOISE.sub(" ", re.sub(r"[()]", " ", name or "").lower()))


def market_index(mandis: list[dict]) -> dict[str, dict[str, str]]:
    """district -> {name key -> mandi id}, merged mandis pointing at the one they were merged into."""
    by_id = {m["id"]: m for m in mandis}

    def live(mid: str) -> str:
        seen = set()
        while by_id.get(mid, {}).get("merged_into_id") and mid not in seen:
            seen.add(mid)
            mid = by_id[mid]["merged_into_id"]
        return mid
    out: dict[str, dict[str, str]] = {}
    for m in sorted(mandis, key=lambda m: m.get("status") != "ACTIVE"):  # an active mandi wins a shared name
        for k in (market_key(m.get("name")), m["id"].rsplit(".", 1)[1], market_key(m.get("label"))):
            if k:
                out.setdefault(m["district"], {}).setdefault(k, live(m["id"]))
    return out


def agmarknet_ids(filters: dict, crops: list[dict], states: list[dict], districts: set[str]):
    """Our crop, state and district ids -> Agmarknet's, by name (our catalogue was built from Agmarknet's names)."""
    data = filters["data"]
    key = lambda s: re.sub(r"[^a-z0-9]", "", (s or "").lower())  # noqa: E731
    ag_crop = {key(c["cmdt_name"]): c["cmdt_id"] for c in data["cmdt_data"]}
    ag_state = {key(s["state_name"]): s["state_id"] for s in data["state_data"]}
    ag_district = {(d["state_id"], slugify(d["district_name"])): d["id"] for d in data["district_data"] if d["state_id"]}
    crop = {c["id"]: ag_crop.get(key(c["name"])) for c in crops}
    state = {}
    for s in states:
        k = key(s["name"])
        state[s["id"]] = ag_state.get(k) or ag_state.get(STATE_ALIAS.get(k, ""))
    district = {}
    for d in districts:
        s, name = d.split(".", 1)
        district[d] = ag_district.get((state.get(s), slugify(DISTRICT_ALIAS.get(d, name))))
    return crop, state, district


def merge(doc: dict, rows: list[dict], markets: dict[str, str]) -> int:
    """Adds one answer's numbers to the file; returns how many values were new or changed. A null never erases a number."""
    changed = 0
    for row in rows:
        name = (row.get("market") or "").strip()
        if not name or name.lower() == "total":
            continue
        mid = markets.get(market_key(name)) or f"?{name}"
        series = doc["markets"].setdefault(mid, [])
        for col, value in row.items():
            i = parse_column(col)
            if i is None or value is None:
                continue
            series.extend([None] * (i + 1 - len(series)))
            if series[i] is None or abs(series[i] - float(value)) > 1e-9:
                series[i] = round(float(value), 3)
                changed += 1
    return changed


def calls_due(doc: dict, today: date) -> tuple[list[tuple[int, int, int]], list[tuple[int, int, int]]]:
    """(refresh, backfill) calls as (year, month, week). A week-w call answers blocks w and w-1, so weeks 2 and 4 cover a
    month; the newest block (and, early in a month, last month's last) is asked again each day it isn't done today."""
    done = doc["done"]
    b = block_of(today.day)
    newest = [(today.year, today.month, b)]
    if b == 1:
        y, m = (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)
        newest.append((y, m, 4))
    quiet = not doc["markets"] and len(done) >= QUIET_AFTER
    if quiet:
        last = max(done.values())
        refresh = newest if (today - date.fromisoformat(last)).days >= 7 else []
        return refresh, []
    refresh = [c for c in newest if done.get("%d-%02d-%d" % c) != today.isoformat()]
    backfill = []
    y, m = today.year, today.month
    while y >= START_YEAR:
        for w in (4, 2):
            c = (y, m, w)
            if c not in newest and "%d-%02d-%d" % c not in done and date(y, m, 1) <= today:
                if not (y == today.year and m == today.month and w > b):
                    backfill.append(c)
        y, m = (y, m - 1) if m > 1 else (y - 1, 12)
    return refresh, backfill


def pairs(get_json) -> list[tuple[str, str]]:
    """(crop, district) for every crop and mandi in today's live files."""
    out = set()
    crop_states = (get_json("live/crop_states.json") or {}).get("crops", {})
    jobs = [(c, s) for c, states in crop_states.items() for s in states]
    with ThreadPoolExecutor(32) as pool:
        for c, doc in zip((c for c, _ in jobs), pool.map(lambda cs: get_json(f"live/crop/{cs[0]}/{cs[1]}.json"), jobs)):
            for row in (doc or {}).get("rows", []):
                out.add((c, row["mandi_id"].rsplit(".", 1)[0]))
    return sorted(out)


def run(store, fetch, now: datetime, budget_s: float, limit: int | None = None, gap: float = GAP_S, log=print) -> dict:
    """fetch(url, params) -> parsed JSON; raises RateLimited on 429."""
    deadline = time.monotonic() + budget_s
    today = now.astimezone(IST).date()
    get_json = lambda k: _get_json(store, k)  # noqa: E731
    catalog = get_json("catalog/mandis.json")["mandis"]
    crops = get_json("catalog/crops.json")["crops"]
    states = get_json("catalog/states.json")["states"]
    todo = pairs(get_json)[:limit]
    crop_id, state_id, district_id = agmarknet_ids(fetch(FILTERS, {}), crops, states, {d for _, d in todo})
    todo = [(c, d) for c, d in todo if crop_id.get(c) and state_id.get(d.split(".")[0]) and district_id.get(d)]
    markets = market_index(catalog)
    docs: dict[tuple[str, str], dict] = {}
    dirty: set[tuple[str, str]] = set()
    stats = {"pairs": len(todo), "calls": 0, "values": 0, "rate_limited": False}
    pace = Pace(gap)

    def doc_of(p):
        if p not in docs:
            docs[p] = get_json(f"arrivals/{p[0]}/{p[1]}.json") or {
                "v": 1, "crop": p[0], "district": p[1], "start": f"{START_YEAR}-01", "unit": "tonnes",
                "blocks": "1-8,9-15,16-23,24-end", "markets": {}, "done": {}}
        return docs[p]

    def save():
        batch = [(p, docs[p]) for p in dirty]
        stamp = now.isoformat(timespec="seconds")
        with ThreadPoolExecutor(32) as pool:
            list(pool.map(lambda pd: store.put(f"arrivals/{pd[0][0]}/{pd[0][1]}.json", _encode({**pd[1], "updated_at": stamp}),
                                               "application/json"), batch))
        dirty.clear()
        log(f"[arrivals] saved {len(batch)} files; {stats}")

    def call(p, c) -> bool:
        """One report; False when the run must stop (budget spent or still rate-limited after backing off)."""
        if time.monotonic() > deadline:
            return False
        c_, d = p
        params = {"report_mode": "Marketwise", "commodity": crop_id[c_], "year": c[0], "month": c[1], "week": c[2],
                  "state": state_id[d.split(".")[0]], "district": district_id[d]}
        for wait in (*BACKOFF_S, None):
            pace.wait()
            try:
                reply = fetch(WEEKLY, params)
                break
            except RateLimited:
                if wait is None or time.monotonic() + wait > deadline:
                    stats["rate_limited"] = True
                    return False
                log(f"[arrivals] 429; waiting {wait}s")
                time.sleep(wait)
        stats["calls"] += 1
        doc = doc_of(p)
        stats["values"] += merge(doc, reply.get("rows") or [], markets.get(d, {}))
        doc["done"]["%d-%02d-%d" % c] = today.isoformat()
        dirty.add(p)
        return True

    last_save = time.monotonic()

    def checkpoint():
        nonlocal last_save
        if time.monotonic() - last_save > SAVE_EVERY_S:
            save()
            last_save = time.monotonic()

    stop = False
    with ThreadPoolExecutor(32) as pool:
        list(pool.map(doc_of, todo))
    plans = {p: calls_due(docs[p], today) for p in todo}
    for p in sorted(todo, key=lambda p: max(docs[p]["done"].values(), default="")):  # least recently fetched first
        for c in plans[p][0]:
            if not call(p, c):
                stop = True
                break
        if stop:
            break
        checkpoint()
    back = lambda c: (today.year - c[0]) * 12 + today.month - c[1]  # noqa: E731 — months before this one
    deepest = max((back(c) for p in todo for c in plans[p][1]), default=-1)
    for depth in range(deepest + 1):  # month by month, newest first, across all pairs
        if stop:
            break
        for p in todo:
            for c in (c for c in plans[p][1] if back(c) == depth):
                if not call(p, c):
                    stop = True
                    break
            if stop:
                break
            checkpoint()
    save()
    return stats


def _http(url: str, params: dict):
    import requests

    reply = requests.get(url, params=params, timeout=60, headers={"User-Agent": "farmuncle-pipeline (github.com/praneeth0100/farmuncle-pipeline)"})
    if reply.status_code == 429:
        raise RateLimited()
    reply.raise_for_status()
    return reply.json()


def main() -> None:
    from farmuncle_pipeline.archive.store import R2Store

    parser = argparse.ArgumentParser(description="Arrival volumes per crop and mandi from Agmarknet.")
    parser.add_argument("--budget-minutes", type=float, default=300)
    parser.add_argument("--limit", type=int, default=None, help="only the first N crop+district pairs (a trial run)")
    args = parser.parse_args()
    if not R2Store.configured():
        print("[arrivals] MANDI_R2_* credentials are not set; nothing published.")
        return
    print(f"[arrivals] {run(R2Store.from_env(), _http, datetime.now(IST), args.budget_minutes * 60, args.limit)}")


if __name__ == "__main__":
    main()
