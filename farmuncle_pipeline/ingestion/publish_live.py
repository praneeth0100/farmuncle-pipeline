"""Publish the app's live JSON to R2 after each live_tick (spec 6.1, 6.2, 6.6, section 16).

Reads the warehouse (last 3 days of prices plus master data) and writes only new objects:
  live/mandi/{public_id}.json          changed mandis only (content hash, ignoring updated_at)
  live/crop/{crop_slug}/{state}.json   changed files only
  catalog/states.json                  rewritten only when the state list changes
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
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone

from farmuncle_pipeline.archive.export import PAGE
from farmuncle_pipeline.archive.store import R2Store

IST = timezone(timedelta(hours=5, minutes=30))
WINDOW_DAYS = 3  # the warehouse keeps 3 days; prev_modal can only look that far back
RECENT_CROPS_DAYS = 14  # spec 6.2; shortcut: the warehouse only holds WINDOW_DAYS, so recent_crops covers those
BUDGET = {"live/mandi/": 10_000, "live/crop/": 10_000}  # gzipped bytes, spec 6.7
HASHES_KEY, MANIFEST_KEY, SLUGS_KEY, STATES_KEY = (
    "meta/live_hashes.json", "meta/manifest.json", "meta/crop_slugs.json", "catalog/states.json")


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
                .gte("price_date", since.isoformat()).order("id").range(start, start + PAGE - 1).execute().data or [])
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


def build_files(*, prices, mandis, crops, varieties, grades, registry, today: date, now: datetime) -> dict:
    """Pure: warehouse rows in, {key: payload} out (live files only, plus the updated registry under SLUGS_KEY)."""
    reg = assign_slugs(crops, varieties, registry)
    crop_merge = {c["id"]: c["merged_into_id"] for c in crops if c.get("merged_into_id")}
    mandi_by_id = {m["id"]: m for m in mandis}

    def survivor(mid):  # a merged mandi publishes under its survivor's id
        m = mandi_by_id.get(mid)
        while m and m.get("merged_into_id") and m["merged_into_id"] in mandi_by_id:
            m = mandi_by_id[m["merged_into_id"]]
        return m

    grade_name = {g["id"]: slugify(g["name"]) for g in grades}
    stamp = now.astimezone(IST).isoformat(timespec="seconds")
    # (public_id, crop, variety, grade) -> {date: row}
    series: dict[tuple, dict[str, dict]] = defaultdict(dict)
    crop_days: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))
    for r in prices:
        m = survivor(r["mandi_id"])
        cid = crop_merge.get(r["crop_id"], r["crop_id"])
        cslug = reg["crops"].get(str(cid))
        if not m or not cslug or _price(r["modal_price"]) is None:
            continue
        vslug = reg["varieties"].get(str(r["variety_id"])) if r.get("variety_id") else None
        key = (m["public_id"], cslug, vslug or f"{cslug}.other", grade_name.get(r.get("grade_id")) or None)
        series[key][r["price_date"]] = r
        crop_days[m["public_id"]][r["price_date"]].add(cslug)

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


def content_hash(payload: dict) -> str:
    body = {k: v for k, v in payload.items() if k != "updated_at"}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]


def _encode(payload) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()


def _check_budget(key: str, data: bytes) -> None:
    for prefix, limit in BUDGET.items():
        if key.startswith(prefix) and len(gzip.compress(data)) > limit:
            raise RuntimeError(f"{key} is {len(gzip.compress(data))} bytes gzipped, over the {limit} budget")


def _get_json(store, key: str):
    try:
        return json.loads(store.get(key))
    except Exception as exc:  # a missing object is the first run; anything else must not look like one
        code = getattr(exc, "response", {}).get("Error", {}).get("Code") if hasattr(exc, "response") else None
        if code in ("NoSuchKey", "404", "NotFound"):
            return None
        raise


def publish(db, store, now: datetime | None = None) -> dict:
    now = now or datetime.now(IST)
    today = now.astimezone(IST).date()
    mandis = _fetch(db, "mandis", "id,public_id,state,status,merged_into_id")
    crops = _fetch(db, "crops", "id,name,merged_into_id")
    files = build_files(
        prices=_fetch_prices(db, today - timedelta(days=WINDOW_DAYS)), mandis=mandis, crops=crops,
        varieties=_fetch(db, "varieties", "id,crop_id,name"), grades=_fetch(db, "grades", "id,name"),
        registry=_get_json(store, SLUGS_KEY) or {}, today=today, now=now)
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
    for k, data in encoded.items():
        store.put(k, data, "application/json")
        hashes[k] = content_hash(changed[k])

    codes = {r["state"]: r["code"] for r in _fetch_state_codes(db)}
    active = {m["state"] for m in mandis if m.get("status") == "ACTIVE" and not m.get("merged_into_id")}
    states = sorted(({"id": codes[s], "name": s} for s in active if s in codes), key=lambda s: s["id"])
    manifest = _get_json(store, MANIFEST_KEY) or {}
    old_states = _get_json(store, STATES_KEY)
    version = int(manifest.get("catalog_version") or 1)
    if old_states is None or old_states.get("states") != states:
        version = version + 1 if old_states is not None else version
        store.put(STATES_KEY, _encode({"v": 1, "version": version, "states": states}), "application/json")
    store.put(SLUGS_KEY, _encode(registry), "application/json")
    store.put(HASHES_KEY, _encode(hashes), "application/json")
    store.put(MANIFEST_KEY, _encode({
        "v": 1, "live_updated_at": stamp, "analytics_built_at": manifest.get("analytics_built_at"),
        "catalog_version": version, "min_app_schema": manifest.get("min_app_schema", 1)}), "application/json")
    return {"files": len(files), "written": len(changed), "states": len(states)}


def _fetch_state_codes(db) -> list[dict]:
    return db.table("state_codes").select("state,code").execute().data or []


def main() -> None:
    from farmuncle_pipeline.ingest_common import validate_startup

    if not R2Store.configured():
        print("[publish_live] MANDI_R2_* credentials are not set; nothing published.")
        return
    store = R2Store.from_env()
    print(f"[publish_live] {publish(validate_startup().supabase, store)}")


if __name__ == "__main__":
    main()
