"""Nightly: build the chart and card files for the app from the R2 archive plus the warehouse window (spec 6.3 to 6.5, section 8).

  series/{crop}/{mandi}/{variety}.json    last 365 days, daily
  history/{crop}/{mandi}/{variety}.json   weekly, as far back as the archive goes (v 2)
  insights/{crop}/{mandi}/{variety}.json  change, 52-week range, price position, seasonal index, nearby, outlook

Reads facts/daily/*.parquet (read only). Writes only the three prefixes above, meta/analytics_hashes.json (the version
of every file written, so unchanged files are skipped) and meta/manifest.json (only analytics_built_at).
Blocks that need more history than the archive holds yet come out with available: false and fill in as it grows.

shortcut: every loaded day of every key is held in memory (about 1.7M rows for 90 days). Fine to roughly one year;
before loading several years, aggregate month by month instead.
"""

from __future__ import annotations

import io
import math
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from statistics import mean, median

from farmuncle_pipeline.archive.store import R2Store
from farmuncle_pipeline.ingestion.publish_live import (
    IST, MANIFEST_KEY, SLUGS_KEY, _encode, _fetch, _fetch_prices, _get_bytes, _get_json, _price, assign_slugs,
    content_hash, make_key_fn, slugify)

HASHES_KEY = "meta/analytics_hashes.json"
BUDGET = {"series/": 8_000, "history/": 12_000, "insights/": 3_000}  # gzipped bytes, spec 6.7
WINDOW_DAYS = 3  # warehouse days; older days come from the archive
MIN_HISTORY_WEEKS = 8  # a history file shorter than this adds nothing over the series
NEARBY_KM = 100
WORKERS = 48
COLS = ["mandi_id", "crop_id", "variety_id", "grade_id", "price_date", "modal_price", "min_price", "max_price"]


def archive_days(store) -> list[date]:
    return sorted(date.fromisoformat(k[len("facts/daily/date="):-len(".parquet")]) for k in store.list("facts/daily/date="))


def read_day(store, day: date) -> list[dict]:
    import pyarrow.parquet as pq

    raw = _get_bytes(store, f"facts/daily/date={day.isoformat()}.parquet")
    return [] if raw is None else pq.read_table(io.BytesIO(raw), columns=COLS).to_pylist()


def add_rows(series: dict, rows, key_of, grade_name: dict) -> None:
    """series[(mandi, crop, variety)][date] = (rank, modal, min, max); one grade per day: FAQ first, else by grade name."""
    for r in rows:
        modal = _price(r["modal_price"])
        k = key_of(r) if modal is not None else None
        if k is None:
            continue
        g = grade_name.get(r.get("grade_id")) or ""
        rank, d = (g != "faq", g), date.fromisoformat(r["price_date"])
        cur = series[k].get(d)
        if cur is None or rank <= cur[0]:
            series[k][d] = (rank, modal, _price(r["min_price"]), _price(r["max_price"]))


def _r2(x):
    return None if x is None else round(x, 2)


def series_file(k, days: dict, end: date, stamp: str) -> dict | None:
    ds = sorted(d for d in days if end - timedelta(days=364) <= d <= end)
    if not ds:
        return None
    cols = ([], [], [])
    for i in range((ds[-1] - ds[0]).days + 1):
        v = days.get(ds[0] + timedelta(days=i))
        for col, x in zip(cols, v[1:] if v else (None, None, None)):
            col.append(x)
    return {"v": 1, "crop_id": k[1], "mandi_id": k[0], "variety_id": k[2], "updated_at": stamp,
            "start": ds[0].isoformat(), "modal": cols[0], "min": cols[1], "max": cols[2]}


def history_file(k, days: dict, stamp: str) -> dict | None:
    weeks: dict[date, list] = {}
    for d, (_, modal, mn, mx) in days.items():
        y, w, _ = d.isocalendar()
        a = weeks.setdefault(date.fromisocalendar(y, w, 1), [[], [], []])
        a[0].append(modal)
        if mn is not None:
            a[1].append(mn)
        if mx is not None:
            a[2].append(mx)
    first, last = min(weeks), max(weeks)
    n = (last - first).days // 7 + 1
    if n < MIN_HISTORY_WEEKS:
        return None
    avg, lo, hi = [], [], []
    for i in range(n):
        a = weeks.get(first + timedelta(weeks=i))
        avg.append(_r2(mean(a[0])) if a else None)
        lo.append(min(a[1]) if a and a[1] else None)
        hi.append(max(a[2]) if a and a[2] else None)
    y, w, _ = first.isocalendar()
    return {"v": 2, "crop_id": k[1], "mandi_id": k[0], "variety_id": k[2], "updated_at": stamp,
            "start_week": f"{y}-W{w:02d}", "avg": avg, "min": lo, "max": hi}


def _pct(days: dict, end: date, span: int, back: int, need: int) -> int | None:
    """Percent change of the mean modal over `span` days ending at `end` against the same span ending `back` days earlier."""
    def window(e):
        return [days[d][1] for d in (e - timedelta(days=i) for i in range(span)) if d in days]
    now, before = window(end), window(end - timedelta(days=back))
    if len(now) < need or len(before) < need:
        return None
    return round((mean(now) / mean(before) - 1) * 100)


def monthly_ratios(days: dict, before_year: int) -> dict[int, dict[int, float]]:
    """{year: {month: month median / that year's median}} for the years before `before_year`."""
    by = defaultdict(lambda: defaultdict(list))
    for d, v in days.items():
        if d.year < before_year:
            by[d.year][d.month].append(v[1])
    out = {}
    for y, months in by.items():
        ymed = median([x for m in months.values() for x in m])
        out[y] = {m: median(v) / ymed for m, v in months.items()}
    return out


def seasonal_index(ratios: dict) -> list | None:
    years = [r for r in ratios.values() if len(r) >= 8]  # a year counts when at least 8 months were reported
    if len(years) < 3:
        return None
    idx = [[r[m] for r in years if m in r] for m in range(1, 13)]
    return None if any(not x for x in idx) else [round(median(x), 2) for x in idx]


def _index_at(idx: list, d: date) -> float:
    """Monthly index interpolated to a day (each month's value sits at its middle)."""
    dim = ((d.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)).day
    pos = d.month - 1 + (d.day - 0.5) / dim - 0.5
    i, frac = math.floor(pos), pos - math.floor(pos)
    return idx[i % 12] * (1 - frac) + idx[(i + 1) % 12] * frac


def _direction(mid: float, base: float) -> str:
    return "up" if mid >= base * 1.03 else "down" if mid <= base * 0.97 else "flat"


def outlook(days: dict, end: date, idx: list | None) -> dict:
    """Spec 8.6: seasonal baseline 14 days ahead, band and hit rate from weekly backtests."""
    def base_at(d):
        v = [days[x][1] for x in (d - timedelta(days=i) for i in range(7)) if x in days]
        return mean(v) if v else None
    base = base_at(end)
    if idx is None or base is None:
        return {"available": False}
    ratios, hits = [], []
    d = min(days) + timedelta(days=6)
    while d + timedelta(days=14) <= end:
        b, actual = base_at(d), days.get(d + timedelta(days=14))
        if b is not None and actual is not None:
            pred = b * _index_at(idx, d + timedelta(days=14)) / _index_at(idx, d)
            ratios.append(actual[1] / pred)
            hits.append(_direction(pred, b) == _direction(actual[1], b))
        d += timedelta(days=7)
    if len(ratios) < 20:
        return {"available": False}
    mid = base * _index_at(idx, end + timedelta(days=14)) / _index_at(idx, end)
    rs = sorted(ratios)
    low, high = mid * rs[int(0.1 * (len(rs) - 1))], mid * rs[int(math.ceil(0.9 * (len(rs) - 1)))]
    hit = sum(hits[-20:]) / 20
    width = (high - low) / mid
    conf = "high" if hit >= 0.7 and width <= 0.2 else "low" if hit < 0.55 or width > 0.4 else "medium"
    return {"available": True, "horizon_days": 14, "direction": _direction(mid, base), "low": _r2(low), "mid": _r2(mid),
            "high": _r2(high), "confidence": conf, "hit_rate": round(hit, 2), "backtests": len(ratios)}


def _km(a, b) -> float:
    la1, lo1, la2, lo2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2
    return 12742 * math.asin(math.sqrt(h))


def nearby(k, coords: dict, recent: dict) -> list:
    """Spec 8.4: mandis within 100 km that reported the same variety (else the same crop) in the last 3 days; top 5 by price."""
    here = coords.get(k[0])
    if here is None:
        return []
    near = [(m, v, modal, d, km) for m, v, modal, d in recent.get(k[1], ()) if m != k[0] and m in coords
            and (km := _km(here, coords[m])) <= NEARBY_KM]
    near = [n for n in near if n[1] == k[2]] or near
    near.sort(key=lambda n: -n[2])
    return [{"mandi_id": m, "km": round(km), "modal": modal, "date": d.isoformat()} for m, _, modal, d, km in near[:5]]


def insights_file(k, days: dict, end: date, coords: dict, recent: dict, stamp: str) -> dict | None:
    year = [d for d in days if end - timedelta(days=364) <= d <= end]
    if not year:
        return None
    last = max(year)
    last_price = days[last][1]
    prev = max((d for d in days if d < last and (last - d).days <= 7), default=None)
    out = {"v": 1, "crop_id": k[1], "mandi_id": k[0], "variety_id": k[2], "updated_at": stamp, "as_of": last.isoformat(),
           "last_price": last_price,
           "change": {"d1": _r2(last_price - days[prev][1]) if prev else None, "d7_pct": _pct(days, last, 7, 7, 3),
                      "d30_pct": _pct(days, last, 30, 30, 10), "d180_pct": _pct(days, last, 30, 180, 10),
                      "d365_pct": _pct(days, last, 30, 365, 10)}}  # d180/d365 compare 30-day means (spec 16 names, no formula)
    covered = (max(year) - min(year)).days >= 300
    lo, hi = min(year, key=lambda d: days[d][1]), max(year, key=lambda d: days[d][1])
    out["range_52w"] = ({"available": True, "low": days[lo][1], "low_date": lo.isoformat(), "high": days[hi][1],
                         "high_date": hi.isoformat()} if covered else {"available": False})
    ratios = monthly_ratios(days, last.year)
    past = [r[last.month] for y, r in ratios.items() if y >= last.year - 10 and last.month in r]
    today_ratio = last_price / median(days[d][1] for d in year)
    if len(past) >= 3:
        lower = sum(r < today_ratio for r in past)
        share = lower / len(past)
        out["price_position"] = {"available": True, "month": last.month, "years_compared": len(past), "years_lower": lower,
                                 "label": "high" if share >= 0.7 else "low" if share <= 0.3 else "normal"}
    else:
        out["price_position"] = {"available": False}
    idx = seasonal_index(ratios)
    if idx:
        ranked = sorted(range(12), key=lambda m: idx[m])
        out["seasonal"] = {"available": True, "years_used": sum(len(r) >= 8 for r in ratios.values()), "index": idx,
                           "best_months": [m + 1 for m in ranked[:-3:-1]], "worst_months": [m + 1 for m in ranked[:2]]}
    else:
        out["seasonal"] = {"available": False}
    out["nearby"] = nearby(k, coords, recent)
    out["outlook"] = outlook(days, last, idx)
    out["quality"] = {"report_days_90": sum(1 for d in days if last - timedelta(days=89) <= d <= last),
                      "last_report": last.isoformat()}
    return out


def _check_budget(key: str, data: bytes) -> None:
    import gzip

    for prefix, limit in BUDGET.items():
        if key.startswith(prefix) and len(gzip.compress(data)) > limit:
            raise RuntimeError(f"{key} is over the {limit} byte gzip budget")


def publish(db, store, now: datetime | None = None) -> dict:
    now = now or datetime.now(IST)
    today = now.astimezone(IST).date()
    stamp = now.astimezone(IST).isoformat(timespec="seconds")
    print("[publish_analytics] loading", flush=True)
    mandis = _fetch(db, "mandis", "id,public_id,state,status,merged_into_id,latitude,longitude")
    crops = _fetch(db, "crops", "id,name,merged_into_id")
    reg = assign_slugs(crops, _fetch(db, "varieties", "id,crop_id,name"), _get_json(store, SLUGS_KEY) or {})
    key_of = make_key_fn(mandis, crops, reg)
    grade_name = {g["id"]: slugify(g["name"]) for g in _fetch(db, "grades", "id,name")}
    series: dict = defaultdict(dict)
    window_start = today - timedelta(days=WINDOW_DAYS)
    old = [d for d in archive_days(store) if d < window_start]
    with ThreadPoolExecutor(16) as pool:
        for i in range(0, len(old), 32):  # a batch at a time keeps the raw rows from piling up
            for rows in pool.map(lambda d: read_day(store, d), old[i:i + 32]):
                add_rows(series, rows, key_of, grade_name)
    add_rows(series, _fetch_prices(db, window_start), key_of, grade_name)
    end = max(d for days in series.values() for d in days)
    coords = {m["public_id"]: (m["latitude"], m["longitude"]) for m in mandis
              if m.get("latitude") is not None and m.get("longitude") is not None and not m.get("merged_into_id")}
    recent: dict = defaultdict(list)  # crop -> reports in the last 3 days, for nearby()
    for k, days in series.items():
        d = max(days)
        if (end - d).days < 3:
            recent[k[1]].append((k[0], k[2], days[d][1], d))
    print(f"[publish_analytics] {len(series)} keys up to {end}", flush=True)

    hashes = _get_json(store, HASHES_KEY) or {}
    written = {"series": 0, "history": 0, "insights": 0}

    def one(item):
        k, days = item
        out = {}
        for prefix, payload in (("series", series_file(k, days, end, stamp)), ("history", history_file(k, days, stamp)),
                                ("insights", insights_file(k, days, end, coords, recent, stamp))):
            if payload is None:
                continue
            key = f"{prefix}/{k[1]}/{k[0]}/{k[2]}.json"
            h = content_hash(payload)
            if hashes.get(key) != h:
                data = _encode(payload)
                _check_budget(key, data)
                store.put(key, data, "application/json")
                out[key] = (prefix, h)
        return out

    with ThreadPoolExecutor(WORKERS) as pool:
        for out in pool.map(one, series.items()):
            for key, (prefix, h) in out.items():
                hashes[key] = h
                written[prefix] += 1
    store.put(HASHES_KEY, _encode(hashes), "application/json")
    manifest = _get_json(store, MANIFEST_KEY) or {"v": 1, "live_updated_at": None, "catalog_version": 1, "min_app_schema": 1}
    store.put(MANIFEST_KEY, _encode({**manifest, "analytics_built_at": stamp}), "application/json")
    return {"keys": len(series), "as_of": end.isoformat(), **written}


def main() -> None:
    import os

    from supabase import create_client

    if not R2Store.configured():
        print("[publish_analytics] MANDI_R2_* credentials are not set; nothing published.")
        return
    db = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
    print(f"[publish_analytics] {publish(db, R2Store.from_env())}")


if __name__ == "__main__":
    main()
