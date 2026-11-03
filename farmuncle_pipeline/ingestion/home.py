"""home/{mandi}.json: everything Screen 1 shows for a farmer at one mandi, worked out once on the server.

For every crop, variety and grade reported within 200 km in the last 14 days, the price a card shows, chosen by the
ladder: reported today, nearest first (home, then its neighbours, out to 200 km); else the latest report, nearest
first. A price flagged odd is used only when nothing else exists. Every row carries its own date, so a new day never
empties the file, and the 7D/1M/6M/1Y prices from the nightly key stats.
"""

from __future__ import annotations

import gzip
import json
from collections import defaultdict
from datetime import date, timedelta

WINDOW_DAYS = 14
ROWS_PER_CROP = 6
BETTER = 1.05  # a nearby mandi must pay at least 5% more today to be worth a trip
HOME_BUDGET = 60_000  # gzipped bytes
NEAREST = 6  # a used mandi's nearest neighbours are refreshed with it


def latest_rows(rows: list[dict], today: date) -> dict[tuple, dict]:
    """rows: {key, date, modal, min, max}. key -> its latest report in the window and the report before it."""
    by_key: dict[tuple, dict[str, dict]] = defaultdict(dict)
    for r in rows:
        by_key[r["key"]][r["date"]] = r
    cutoff, out = (today - timedelta(days=WINDOW_DAYS)).isoformat(), {}
    for k, days in by_key.items():
        ds = sorted(d for d in days if d <= today.isoformat())
        if not ds or ds[-1] < cutoff:
            continue
        last = days[ds[-1]]
        out[k] = {"date": ds[-1], "modal": last["modal"], "min": last["min"], "max": last["max"],
                  "prev": days[ds[-2]]["modal"] if len(ds) > 1 else None, "prev_date": ds[-2] if len(ds) > 1 else None}
    return out


def homes_to_build(all_ids, used: set | None, neighbours: dict) -> set:
    """Every mandi when `used` is None (nightly, or no user list); else each used mandi and its nearest neighbours."""
    if used is None:
        return set(all_ids)
    return {m for u in used for m in [u, *(other for other, _ in neighbours.get(u, [])[:NEAREST])]} & set(all_ids)


def build_home(*, homes, latest, neighbours, coords, labels, stats, odd, today: date, stamp: str) -> dict[str, dict]:
    by_mandi: dict[str, list] = defaultdict(list)
    for (pid, crop, variety, grade), row in latest.items():
        by_mandi[pid].append(((crop, variety, grade), row))
    stat_keys, t = stats.get("keys", {}), today.isoformat()
    out = {}
    for h in homes:
        reach = [(h, 0), *neighbours.get(h, [])]
        cands: dict[tuple, list] = defaultdict(list)  # (crop, variety, grade) -> [(km, mandi, row)], nearest first
        for pid, km in reach:
            for cvg, row in by_mandi.get(pid, ()):
                cands[cvg].append((km, pid, row))
        used, crops = {h: 0}, defaultdict(list)
        for cvg, options in cands.items():
            def pick(today_only, allow_odd):
                return next((o for o in options if (not today_only or o[2]["date"] == t)
                             and (allow_odd or (o[1], *cvg) not in odd)), None)
            km, pid, row = pick(True, False) or pick(False, False) or pick(True, True) or pick(False, True)
            item = {"variety": cvg[1], "grade": cvg[2], "mandi": pid, **row,
                    "then": stat_keys.get(f"{pid}|{cvg[0]}|{cvg[1]}|{cvg[2] or ''}", {})}
            if (pid, *cvg) in odd:
                item["odd"] = True
            if row["date"] == t:
                best = max((o for o in options if o[2]["date"] == t and (o[1], *cvg) not in odd), key=lambda o: o[2]["modal"],
                           default=None)
                if best and best[2]["modal"] >= row["modal"] * BETTER:
                    item["better"] = {"mandi": best[1], "modal": best[2]["modal"]}
                    used[best[1]] = best[0]
            used[pid] = km
            crops[cvg[0]].append((len(options), item, any(o[1] == h for o in options)))
        ranked = sorted(crops.items(), key=lambda kv: (-(sum(n for n, _, _ in kv[1]) + 2 * any(a for _, _, a in kv[1])), kv[0]))
        out[f"home/{h}.json"] = {
            "v": 2, "mandi_id": h, "label": labels.get(h, h), "state": h.split(".")[0], "as_of": t, "updated_at": stamp,
            "stats_as_of": stats.get("as_of"),
            "mandis": {m: {"label": labels.get(m, m), "km": km, **({"approx": True} if coords.get(m, (0, 0, False))[2] else {})}
                       for m, km in used.items()},
            "crops": [{"crop": crop, "rows": [item for _, item, _ in sorted(items, key=lambda x: (-x[0], x[1]["variety"], x[1]["grade"] or ""))][:ROWS_PER_CROP]}
                      for crop, items in ranked]}
    return out


def trim(payload: dict, limit: int = HOME_BUDGET) -> bytes:
    """The file as bytes within budget, dropping the least popular crops (the end of the list) until it fits."""
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    crops = payload.get("crops", [])
    while len(gzip.compress(data)) > limit and crops:
        crops = crops[:max(0, int(len(crops) * 0.9))] if len(crops) > 10 else crops[:-1]
        data = json.dumps({**payload, "crops": crops}, ensure_ascii=False, separators=(",", ":")).encode()
    return data


TOP_N, TOP_DAYS = 10, 3


def build_top(*, latest, stats, labels, odd, today: date, stamp: str) -> dict[str, dict]:
    """top/{crop}.json: per variety and grade, India's 10 best-paying mandis (reported in the last 3 days) and the 10
    biggest risers over about a week. Odd prices never rank."""
    cutoff, stat_keys = (today - timedelta(days=TOP_DAYS)).isoformat(), stats.get("keys", {})
    lines: dict[str, dict[tuple, list]] = defaultdict(lambda: defaultdict(list))
    for k, row in latest.items():
        if row["date"] >= cutoff and k not in odd:
            lines[k[1]][(k[2], k[3])].append((k[0], row))
    out = {}
    for crop, by_vg in lines.items():
        result = []
        for (variety, grade), entries in sorted(by_vg.items(), key=lambda kv: (kv[0][0], kv[0][1] or "")):
            def entry(pid, row, **extra):
                return {"mandi": pid, "label": labels.get(pid, pid), "state": pid.split(".")[0], "modal": row["modal"],
                        "date": row["date"], **extra}
            rises = []
            for pid, row in entries:
                then = stat_keys.get(f"{pid}|{crop}|{variety}|{grade or ''}", {}).get("7D")
                if then and then[0]:
                    rises.append((row["modal"] / then[0] - 1, pid, row, then))
            result.append({
                "variety": variety, "grade": grade,
                "by_price": [entry(p, r) for p, r in sorted(entries, key=lambda e: -e[1]["modal"])[:TOP_N]],
                "by_rise": [entry(p, r, then=t, pct=round(x * 100)) for x, p, r, t in sorted(rises, key=lambda e: -e[0])[:TOP_N]]})
        out[f"top/{crop}.json"] = {"v": 1, "crop": crop, "as_of": today.isoformat(), "updated_at": stamp, "lines": result}
    return out
