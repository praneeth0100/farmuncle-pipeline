"""Publish the app's live JSON to R2 after each live_tick (spec 6.1, 6.2, 6.6, section 16).

Reads the warehouse (last 3 days of prices plus master data) and writes only new objects:
  live/mandi/{public_id}.json          changed mandis only (content hash, ignoring updated_at)
  live/crop/{crop_slug}/{state}.json   changed files only
  catalog/{states,mandis,crops}.json   rewritten together, only when one of them changes (bumps catalog_version)
  meta/crop_slugs.json                 frozen crop and variety slugs (a slug is never reassigned)
  meta/live_hashes.json                version (content hash) of every live file written so far
  meta/manifest.json                   written last, so the app never sees it ahead of its files
Nothing under facts/, quarantine/ or catalog/*.parquet is touched.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
from concurrent.futures import ThreadPoolExecutor
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone

from farmuncle_pipeline.archive.export import PAGE
from farmuncle_pipeline.archive.store import R2Store

IST = timezone(timedelta(hours=5, minutes=30))
WINDOW_DAYS = 3  # the warehouse keeps 3 days; older days come from the R2 archive (read only)
RECENT_CROPS_DAYS = 14  # spec 6.2; prev_modal and recent_crops look back this far
WRITERS = 16
BUDGET = {"live/mandi/": 10_000, "live/crop/": 10_000, "catalog/mandis.json": 250_000}  # gzipped bytes, spec 6.7
HASHES_KEY, MANIFEST_KEY, SLUGS_KEY = "meta/live_hashes.json", "meta/manifest.json", "meta/crop_slugs.json"


def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (text or "").lower()).strip("_")


def _fetch(db, table: str, select: str, **eq) -> list[dict]:
    rows, start = [], 0
    while True:
        q = db.table(table).select(select)
        for col, val in eq.items():
            q = q.eq(col, val)
        page = q.order("id").range(start, start + PAGE - 1).execute().data or []
        rows.extend(page)
        if len(page) < PAGE:
            return rows
        start += PAGE


def _fetch_prices(db, since: date) -> list[dict]:
    rows, start = [], 0
    while True:
        page = (db.table("mandi_daily_prices")
                .select("id,mandi_id,crop_id,variety_id,grade_id,price_date,modal_price,min_price,max_price,created_at")
                .gte("price_date", since.isoformat()).order("price_date").order("id").range(start, start + PAGE - 1).execute().data or [])
        rows.extend(page)
        if len(page) < PAGE:
            return rows
        start += PAGE


def assign_slugs(crops: list[dict], varieties: list[dict], registry: dict) -> dict:
    """Extend the frozen registry {"crops": {id: slug}, "varieties": {id: slug}} for ids not yet in it."""
    reg = {"crops": dict(registry.get("crops", {})), "varieties": dict(registry.get("varieties", {}))}
    taken = set(reg["crops"].values())
    for c in sorted(crops, key=lambda c: c["id"]):
        if str(c["id"]) in reg["crops"]:
            continue
        slug = slugify(c["name"]) or f"crop_{c['id']}"
        if slug in taken:
            slug = f"{slug}_{c['id']}"
        reg["crops"][str(c["id"])] = slug
        taken.add(slug)
    taken_v = set(reg["varieties"].values())
    for v in sorted(varieties, key=lambda v: v["id"]):
        cslug = reg["crops"].get(str(v["crop_id"]))
        if str(v["id"]) in reg["varieties"] or cslug is None:
            continue
        slug = f"{cslug}.{slugify(v['name']) or 'other'}"
        if slug in taken_v:
            slug = f"{slug}_{v['id']}"
        reg["varieties"][str(v["id"])] = slug
        taken_v.add(slug)
    return reg


def _ts(value: str | None) -> str | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(IST).isoformat(timespec="seconds") if value else None


def _price(value) -> float | None:
    return round(float(value), 2) if value is not None and float(value) > 0 else None


def make_key_fn(mandis, crops, reg):
    """Row (warehouse or archive ids) -> (public_id, crop_slug, variety_id), or None when an id has no slug.
    A merged mandi or crop resolves to its survivor."""
    crop_merge = {c["id"]: c["merged_into_id"] for c in crops if c.get("merged_into_id")}
    mandi_by_id = {m["id"]: m for m in mandis}

    def key_of(r):
        m = mandi_by_id.get(r["mandi_id"])
        while m and m.get("merged_into_id") and m["merged_into_id"] in mandi_by_id:
            m = mandi_by_id[m["merged_into_id"]]
        cslug = reg["crops"].get(str(crop_merge.get(r["crop_id"], r["crop_id"])))
        if not m or not cslug:
            return None
        vslug = reg["varieties"].get(str(r["variety_id"])) if r.get("variety_id") else None
        return (m["public_id"], cslug, vslug or f"{cslug}.other")

    return key_of


def build_files(*, prices, mandis, crops, varieties, grades, registry, today: date, now: datetime) -> dict:
    """Pure: warehouse rows in, {key: payload} out (live files only, plus the updated registry under SLUGS_KEY)."""
    reg = assign_slugs(crops, varieties, registry)
    key_of = make_key_fn(mandis, crops, reg)
    grade_name = {g["id"]: slugify(g["name"]) for g in grades}
    stamp = now.astimezone(IST).isoformat(timespec="seconds")
    # (public_id, crop, variety, grade) -> {date: row}
    series: dict[tuple, dict[str, dict]] = defaultdict(dict)
    crop_days: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))
    for r in prices:
        k = key_of(r)
        if not k or _price(r["modal_price"]) is None:
            continue
        key = (*k, grade_name.get(r.get("grade_id")) or None)
        series[key][r["price_date"]] = r
        crop_days[k[0]][r["price_date"]].add(k[1])

    out: dict[str, dict] = {}
    items_by_mandi: dict[str, list] = defaultdict(list)
    for (pid, cslug, vid, grade), days in sorted(series.items(), key=lambda kv: tuple(str(x) for x in kv[0])):
        today_row = days.get(today.isoformat())
        if not today_row:
            continue
        earlier = max((d for d in days if d < today.isoformat()), default=None)
        items_by_mandi[pid].append({
            "crop_id": cslug, "variety_id": vid, "grade": grade,
            "modal": _price(today_row["modal_price"]), "min": _price(today_row["min_price"]), "max": _price(today_row["max_price"]),
            "prev_modal": _price(days[earlier]["modal_price"]) if earlier else None, "prev_date": earlier,
            "reported_at": _ts(today_row["created_at"])})
    cutoff = (today - timedelta(days=RECENT_CROPS_DAYS)).isoformat()
    for pid, by_day in crop_days.items():
        counts = Counter(c for d, crops_ in by_day.items() if d >= cutoff for c in crops_)
        out[f"live/mandi/{pid}.json"] = {
            "v": 1, "mandi_id": pid, "date": today.isoformat(), "updated_at": stamp,
            "items": items_by_mandi.get(pid, []),
            "recent_crops": [c for c, _ in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]}

    rows_by_file: dict[str, list] = defaultdict(list)
    for pid, items in items_by_mandi.items():
        state = pid.split(".")[0]
        best: dict[str, dict] = {}
        for it in items:  # one row per mandi and variety: prefer FAQ, else the first grade by name
            cur = best.get(it["variety_id"])
            if cur is None or (it["grade"] == "faq" and cur["grade"] != "faq"):
                best[it["variety_id"]] = it
        for vid, it in best.items():
            rows_by_file[f"live/crop/{it['crop_id']}/{state}.json"].append({
                "mandi_id": pid, "variety_id": vid, "modal": it["modal"], "min": it["min"], "max": it["max"],
                "prev_modal": it["prev_modal"]})
    for key, rows in rows_by_file.items():
        _, _, cslug, fname = key.split("/")
        out[key] = {"v": 1, "crop_id": cslug, "state": fname[:-5], "date": today.isoformat(), "updated_at": stamp,
                    "rows": sorted(rows, key=lambda r: (r["mandi_id"], r["variety_id"]))}
    out[SLUGS_KEY] = reg
    return out


def build_catalogs(*, mandis, crops, varieties, codes: dict, registry: dict) -> dict:
    """Pure: {catalog key: payload without "version"}. Merged mandis stay listed so the app can follow merged_into_id."""
    pid = {m["id"]: m["public_id"] for m in mandis}
    live = [m for m in mandis if m.get("status") == "ACTIVE" and not m.get("merged_into_id")]
    states = sorted(({"id": codes[s], "name": s} for s in {m["state"] for m in live} if s in codes), key=lambda s: s["id"])
    mandi_rows = [{
        "id": m["public_id"], "name": m.get("display_name") or m["name"], "district": ".".join(m["public_id"].split(".")[:2]),
        "state": codes.get(m["state"]) or m["public_id"].split(".")[0],
        "lat": m.get("latitude"), "lng": m.get("longitude"), "status": m.get("status"),
        "merged_into_id": pid.get(m.get("merged_into_id")), "names": {}  # shortcut: no translations in the warehouse yet
    } for m in sorted(mandis, key=lambda m: m["public_id"])]
    reg_c, reg_v = registry["crops"], registry["varieties"]
    by_crop = defaultdict(list)
    for v in sorted(varieties, key=lambda v: v["id"]):
        if not v.get("merged_into_id") and str(v["id"]) in reg_v and v.get("status", "ACTIVE") == "ACTIVE":
            by_crop[v["crop_id"]].append({"id": reg_v[str(v["id"])], "name": v["name"]})
    crop_rows = []
    for c in sorted(crops, key=lambda c: reg_c.get(str(c["id"]), "")):
        slug = reg_c.get(str(c["id"]))
        if not slug or c.get("merged_into_id") or c.get("status", "ACTIVE") != "ACTIVE":
            continue
        vs = by_crop[c["id"]]
        if f"{slug}.other" not in {v["id"] for v in vs}:  # items without a variety are published as crop.other
            vs.append({"id": f"{slug}.other", "name": "Other"})
        crop_rows.append({"id": slug, "name": c["name"], "category": (c.get("category") or "").lower() or None,
                          "unit": "animal" if (c.get("display_unit") or "").lower() == "animal" else "kg",
                          "names": {}, "varieties": vs})
    return {"catalog/states.json": {"v": 1, "states": states}, "catalog/mandis.json": {"v": 1, "mandis": mandi_rows},
            "catalog/crops.json": {"v": 1, "crops": crop_rows}}


def archive_rows(store, today: date) -> list[dict]:
    """Daily rows older than the warehouse window, from the R2 archive (read only); days not archived yet are skipped."""
    import io

    import pyarrow.parquet as pq

    def read(day: date) -> list[dict]:
        raw = _get_bytes(store, f"facts/daily/date={day.isoformat()}.parquet")
        if raw is None:
            return []
        cols = ["mandi_id", "crop_id", "variety_id", "grade_id", "price_date", "modal_price", "min_price", "max_price"]
        return pq.read_table(io.BytesIO(raw), columns=cols).to_pylist()

    days = [today - timedelta(days=n) for n in range(WINDOW_DAYS + 1, RECENT_CROPS_DAYS + 1)]
    with ThreadPoolExecutor(WRITERS) as pool:
        return [r for rows in pool.map(read, days) for r in rows]


def content_hash(payload: dict) -> str:
    body = {k: v for k, v in payload.items() if k != "updated_at"}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]


def _encode(payload) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()


def _check_budget(key: str, data: bytes) -> None:
    for prefix, limit in BUDGET.items():
        if key.startswith(prefix) and len(gzip.compress(data)) > limit:
            raise RuntimeError(f"{key} is {len(gzip.compress(data))} bytes gzipped, over the {limit} budget")


def _get_bytes(store, key: str) -> bytes | None:
    try:
        return store.get(key)
    except Exception as exc:  # a missing object is the first run; anything else must not look like one
        if getattr(exc, "response", {}).get("Error", {}).get("Code") in ("NoSuchKey", "404", "NotFound"):
            return None
        raise


def _get_json(store, key: str):
    raw = _get_bytes(store, key)
    return None if raw is None else json.loads(raw)


def publish(db, store, now: datetime | None = None) -> dict:
    now = now or datetime.now(IST)
    today = now.astimezone(IST).date()
    print("[publish_live] reading the warehouse", flush=True)
    mandis = _fetch(db, "mandis", "id,public_id,name,display_name,state,status,merged_into_id,latitude,longitude")
    crops = _fetch(db, "crops", "id,name,category,display_unit,status,merged_into_id")
    varieties = _fetch(db, "varieties", "id,crop_id,name,status,merged_into_id")
    codes = {r["state"]: r["code"] for r in _fetch_state_codes(db)}
    files = build_files(
        prices=archive_rows(store, today) + _fetch_prices(db, today - timedelta(days=WINDOW_DAYS)),
        mandis=mandis, crops=crops, varieties=varieties, grades=_fetch(db, "grades", "id,name"),
        registry=_get_json(store, SLUGS_KEY) or {}, today=today, now=now)
    print(f"[publish_live] built {len(files) - 1} files", flush=True)
    registry = files.pop(SLUGS_KEY)
    hashes = _get_json(store, HASHES_KEY) or {}
    stamp = now.astimezone(IST).isoformat(timespec="seconds")
    # a file we published earlier with no rows in the window now must say "no data" instead of keeping stale prices
    for key in hashes:
        if key.startswith("live/mandi/") and key not in files:
            pid = key[len("live/mandi/"):-5]
            files[key] = {"v": 1, "mandi_id": pid, "date": today.isoformat(), "updated_at": stamp, "items": [], "recent_crops": []}
        elif key.startswith("live/crop/") and key not in files:
            _, _, cslug, fname = key.split("/")
            files[key] = {"v": 1, "crop_id": cslug, "state": fname[:-5], "date": today.isoformat(), "updated_at": stamp, "rows": []}
    changed = {k: p for k, p in files.items() if hashes.get(k) != content_hash(p)}
    encoded = {k: _encode(p) for k, p in changed.items()}
    for k, data in encoded.items():
        _check_budget(k, data)  # fail before writing anything
    with ThreadPoolExecutor(WRITERS) as pool:  # one put is ~0.5 s; a first run writes ~1,500 files
        list(pool.map(lambda kv: store.put(kv[0], kv[1], "application/json"), encoded.items()))
    hashes.update({k: content_hash(changed[k]) for k in encoded})

    manifest = _get_json(store, MANIFEST_KEY) or {}
    version = int(manifest.get("catalog_version") or 1)
    catalogs = build_catalogs(mandis=mandis, crops=crops, varieties=varieties, codes=codes, registry=registry)
    new = {k: content_hash(p) for k, p in catalogs.items()}
    if any(hashes.get(k) != h for k, h in new.items()):
        version += 1 if any(k in hashes for k in new) else 0  # the first publish keeps the manifest's version
        for k, p in catalogs.items():
            data = _encode({**p, "version": version})
            _check_budget(k, data)
            store.put(k, data, "application/json")
        hashes.update(new)
    store.put(SLUGS_KEY, _encode(registry), "application/json")
    store.put(HASHES_KEY, _encode(hashes), "application/json")
    store.put(MANIFEST_KEY, _encode({
        "v": 1, "live_updated_at": stamp, "analytics_built_at": manifest.get("analytics_built_at"),
        "catalog_version": version, "min_app_schema": manifest.get("min_app_schema", 1)}), "application/json")
    return {"files": len(files), "written": len(changed), "catalog": version}


def _fetch_state_codes(db) -> list[dict]:
    return db.table("state_codes").select("state,code").execute().data or []


def main() -> None:
    import os

    from supabase import create_client

    if not R2Store.configured():
        print("[publish_live] MANDI_R2_* credentials are not set; nothing published.")
        return
    store = R2Store.from_env()
    print(f"[publish_live] {publish(create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"]), store)}")


if __name__ == "__main__":
    main()
