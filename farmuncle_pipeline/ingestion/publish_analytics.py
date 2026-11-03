"""Nightly: chart files for the app and the price-change stats behind every card, from the R2 archive plus the warehouse.

  chart/{crop}/{mandi}.json   every variety and grade at one mandi: daily for the last 365 days, weekly before that
  meta/key_stats.json         per mandi|crop|variety|grade, the price about 7 days, 1 month, 6 months and 1 year back
                              (read by publish_live for the home files; not read by the app)

DuckDB reads facts/daily/*.parquet straight from R2 (read only) and does the heavy lifting, so years of history never
sit in Python memory. Crop, variety and grade stay separate keys throughout. Only crop+mandi pairs with an active key
(10+ report days in the last 90) get a chart, and a chart is written only when its content changed
(meta/chart_hashes.json). meta/manifest.json gets analytics_built_at.
"""

from __future__ import annotations

import gzip
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta

from farmuncle_pipeline.archive.store import R2Store
from farmuncle_pipeline.ingestion.publish_live import (
    IST, MANIFEST_KEY, SLUGS_KEY, STATS_KEY, _encode, _fetch, _fetch_prices, _get_json, _price, assign_slugs, content_hash, slugify)

CHART_HASHES_KEY = "meta/chart_hashes.json"
CHART_BUDGET = 80_000  # gzipped bytes
WINDOW_DAYS = 3  # warehouse days; older days come from the archive
ACTIVE_REPORTS, ACTIVE_DAYS = 10, 90
WORKERS = 48
# period -> (days back, slack): the latest report on or before that day, at most `slack` days earlier
ANCHORS = {"7D": (7, 3), "1M": (30, 10), "6M": (182, 10), "1Y": (365, 10)}


def archive_days(store) -> list[date]:
    return sorted(date.fromisoformat(k[len("facts/daily/date="):-len(".parquet")]) for k in store.list("facts/daily/date="))


def warehouse_start(archived: list[date], today: date) -> date:
    """First day to read from the warehouse: the usual window, or earlier when archive_aged missed nights and newer days
    are still only in the warehouse (reading a fixed window would leave a hole in every file)."""
    window = today - timedelta(days=WINDOW_DAYS)
    return min(window, max(archived) + timedelta(days=1)) if archived else window


def id_maps(mandis, crops, varieties, grades, reg) -> dict[str, list[tuple]]:
    """Warehouse ids -> published slugs, merges followed to the survivor (same rules as publish_live.make_key_fn)."""
    by_id = {m["id"]: m for m in mandis}

    def survivor(m):
        while m.get("merged_into_id") and m["merged_into_id"] in by_id:
            m = by_id[m["merged_into_id"]]
        return m["public_id"]
    crop_merge = {c["id"]: c["merged_into_id"] for c in crops if c.get("merged_into_id")}
    variety_merge = {v["id"]: v["merged_into_id"] for v in varieties if v.get("merged_into_id")}
    return {
        "mandi_map": [(m["id"], survivor(m)) for m in mandis],
        "crop_map": [(c["id"], s) for c in crops if (s := reg["crops"].get(str(crop_merge.get(c["id"], c["id"]))))],
        "variety_map": [(v["id"], s) for v in varieties if (s := reg["varieties"].get(str(variety_merge.get(v["id"], v["id"]))))],
        "grade_map": [(g["id"], slugify(g["name"])) for g in grades],
    }


def load(con, facts_glob: str | None, warehouse_rows: list[dict], maps: dict) -> date | None:
    """Builds table prices(mandi, crop, variety, grade, d, modal, mn, mx): one row per key and day (same-day duplicates
    averaged). Returns the newest day, or None when there is nothing."""
    import pyarrow as pa

    for name, rows in maps.items():
        con.register(name, pa.table({"id": [r[0] for r in rows], "slug": [r[1] for r in rows]}))
    cols = ["mandi_id", "crop_id", "variety_id", "grade_id", "price_date", "modal_price", "min_price", "max_price"]
    wh = [{**{c: r.get(c) for c in cols[:5]}, "modal_price": _price(r["modal_price"]), "min_price": _price(r["min_price"]),
           "max_price": _price(r["max_price"])} for r in warehouse_rows]
    con.register("wh", pa.Table.from_pylist(wh, schema=pa.schema([
        ("mandi_id", pa.int64()), ("crop_id", pa.int64()), ("variety_id", pa.int64()), ("grade_id", pa.int64()),
        ("price_date", pa.string()), ("modal_price", pa.float64()), ("min_price", pa.float64()), ("max_price", pa.float64())])))
    # Every archived day is read: a day both archived and still in the warehouse is the same rows, averaged away.
    facts = f"SELECT {', '.join(cols)} FROM read_parquet('{facts_glob}', union_by_name = true) UNION ALL " if facts_glob else ""
    con.execute(f"""
        CREATE OR REPLACE TABLE prices AS
        WITH raw AS ({facts} SELECT {', '.join(cols)} FROM wh)
        SELECT m.slug AS mandi, c.slug AS crop, coalesce(v.slug, c.slug || '.other') AS variety, coalesce(g.slug, '') AS grade,
               CAST(price_date AS DATE) AS d, round(avg(CAST(modal_price AS DOUBLE)), 2) AS modal,
               round(avg(nullif(CAST(min_price AS DOUBLE), 0)), 2) AS mn, round(avg(nullif(CAST(max_price AS DOUBLE), 0)), 2) AS mx
        FROM raw JOIN mandi_map m ON raw.mandi_id = m.id JOIN crop_map c ON raw.crop_id = c.id
        LEFT JOIN variety_map v ON raw.variety_id = v.id LEFT JOIN grade_map g ON raw.grade_id = g.id
        WHERE CAST(modal_price AS DOUBLE) > 0
        GROUP BY ALL""")
    return con.execute("SELECT max(d) FROM prices").fetchone()[0]


def charts(con, end: date, stamp: str):
    """Yields (key, payload) per crop+mandi with an active key, lines sorted by variety then grade.
    days: [offset from start, modal, min, max]; weeks: [weeks from weekly_start, average modal]."""
    start = end - timedelta(days=364)
    cur = con.execute("""
        WITH groups AS (
            SELECT DISTINCT crop, mandi FROM prices GROUP BY crop, mandi, variety, grade
            HAVING count(*) FILTER (WHERE d > $end - INTERVAL 90 DAY) >= $reports),
        p AS (SELECT * FROM prices JOIN groups USING (crop, mandi)),
        daily AS (SELECT crop, mandi, variety, grade, list(struct_pack(d := d, p := modal, lo := mn, hi := mx) ORDER BY d) AS days
                  FROM p WHERE d >= $start GROUP BY ALL),
        weekly AS (SELECT crop, mandi, variety, grade, list(struct_pack(w := w, a := a) ORDER BY w) AS weeks
                   FROM (SELECT crop, mandi, variety, grade, CAST(date_trunc('week', d) AS DATE) AS w, round(avg(modal), 2) AS a
                         FROM p WHERE d < $start GROUP BY ALL) GROUP BY ALL)
        SELECT crop, mandi, variety, grade, days, weeks FROM daily FULL JOIN weekly USING (crop, mandi, variety, grade)
        ORDER BY crop, mandi, variety, grade""", {"end": end, "start": start, "reports": ACTIVE_REPORTS})
    group, lines = None, []
    while batch := cur.fetchmany(2000):
        for crop, mandi, variety, grade, days, weeks in batch:
            if group and group != (crop, mandi):
                yield _chart(group, lines, start, stamp)
                lines = []
            group = (crop, mandi)
            lines.append((variety, grade or None, days or [], weeks or []))
    if group:
        yield _chart(group, lines, start, stamp)


def _chart(group, lines, start: date, stamp: str):
    crop, mandi = group
    first_week = min((w["w"] for _, _, _, weeks in lines for w in weeks), default=None)
    payload = {
        "v": 2, "crop": crop, "mandi_id": mandi, "updated_at": stamp, "start": start.isoformat(),
        "lines": [{"variety": v, "grade": g, "days": [[(x["d"] - start).days, x["p"], x["lo"], x["hi"]] for x in days]}
                  for v, g, days, _ in lines if days],
        "weekly_start": first_week.isoformat() if first_week else None,
        "weekly": [{"variety": v, "grade": g, "weeks": [[(x["w"] - first_week).days // 7, x["a"]] for x in weeks]}
                   for v, g, _, weeks in lines if weeks]}
    return f"chart/{crop}/{mandi}.json", payload


def key_stats(con, end: date) -> dict:
    """{"as_of", "keys": {"mandi|crop|variety|grade": {"7D": [price, date], ...}}} for keys reported in the last 14 days.
    A period is missing when nothing was reported near its day (never a made-up number)."""
    picks = ", ".join(
        f"max_by(struct_pack(p := modal, d := d), d) FILTER (WHERE d BETWEEN $end - INTERVAL {n + slack} DAY "
        f"AND $end - INTERVAL {n} DAY) AS \"{period}\"" for period, (n, slack) in ANCHORS.items())
    rows = con.execute(f"""
        SELECT mandi, crop, variety, grade, {picks} FROM prices WHERE d >= $end - INTERVAL 380 DAY
        GROUP BY ALL HAVING max(d) >= $end - INTERVAL 14 DAY""", {"end": end}).fetchall()
    keys = {}
    for mandi, crop, variety, grade, *found in rows:
        keys[f"{mandi}|{crop}|{variety}|{grade}"] = {p: [f["p"], f["d"].isoformat()] for p, f in zip(ANCHORS, found) if f}
    return {"v": 1, "as_of": end.isoformat(), "keys": keys}


def _fit(key: str, payload: dict) -> bytes | None:
    """The chart as bytes within budget: weekly history is dropped first; None when even that is too big."""
    for candidate in (payload, {**payload, "weekly_start": None, "weekly": []}):
        data = _encode(candidate)
        if len(gzip.compress(data)) <= CHART_BUDGET:
            return data
    print(f"[publish_analytics] {key} is over {CHART_BUDGET} bytes gzipped even without weekly history; skipped")
    return None


def _r2_glob(con) -> str:
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"CREATE SECRET r2 (TYPE r2, KEY_ID '{os.environ['MANDI_R2_ACCESS_KEY_ID']}', "
                f"SECRET '{os.environ['MANDI_R2_SECRET_ACCESS_KEY']}', ACCOUNT_ID '{os.environ['MANDI_R2_ACCOUNT_ID']}')")
    return f"r2://{os.environ.get('MANDI_R2_BUCKET') or 'agrouncle-mandi'}/facts/daily/*.parquet"


def publish(db, store, now: datetime | None = None, facts_glob: str | None = None) -> dict:
    import duckdb

    now = now or datetime.now(IST)
    today = now.astimezone(IST).date()
    stamp = now.astimezone(IST).isoformat(timespec="seconds")
    print("[publish_analytics] loading", flush=True)
    mandis = _fetch(db, "mandis", "id,public_id,merged_into_id")
    crops = _fetch(db, "crops", "id,name,merged_into_id")
    varieties = _fetch(db, "varieties", "id,crop_id,name,merged_into_id")
    reg = assign_slugs(crops, varieties, _get_json(store, SLUGS_KEY) or {})
    maps = id_maps(mandis, crops, varieties, _fetch(db, "grades", "id,name"), reg)
    archived = archive_days(store)
    window_start = warehouse_start(archived, today)
    con = duckdb.connect()
    if archived and facts_glob is None:
        facts_glob = _r2_glob(con)
    end = load(con, facts_glob if archived else None, _fetch_prices(db, window_start), maps)
    if end is None:
        return {"as_of": None, "charts": 0, "written": 0}
    print(f"[publish_analytics] prices up to {end}", flush=True)

    hashes = _get_json(store, CHART_HASHES_KEY) or {}
    count, written, batch = 0, 0, []

    def flush():
        with ThreadPoolExecutor(WORKERS) as pool:
            list(pool.map(lambda kd: store.put(kd[0], kd[1], "application/json"), batch))
        batch.clear()
    for key, payload in charts(con, end, stamp):
        count += 1
        h = content_hash(payload)
        if hashes.get(key) == h or (data := _fit(key, payload)) is None:
            continue
        batch.append((key, data))
        hashes[key] = h
        written += 1
        if len(batch) >= 500:
            flush()
    flush()
    stats = key_stats(con, end)
    store.put(STATS_KEY, _encode(stats), "application/json")
    store.put(CHART_HASHES_KEY, _encode(hashes), "application/json")
    manifest = _get_json(store, MANIFEST_KEY) or {"v": 1, "live_updated_at": None, "catalog_version": 1, "min_app_schema": 1}
    store.put(MANIFEST_KEY, _encode({**manifest, "analytics_built_at": stamp}), "application/json")
    return {"as_of": end.isoformat(), "charts": count, "written": written, "stat_keys": len(stats["keys"])}


def main() -> None:
    from supabase import create_client

    if not R2Store.configured():
        print("[publish_analytics] MANDI_R2_* credentials are not set; nothing published.")
        return
    db = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
    print(f"[publish_analytics] {publish(db, R2Store.from_env())}")


if __name__ == "__main__":
    main()
