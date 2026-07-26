"""
FarmUncle v2 — sync_storefront.py

Purpose:
    Mirror the app-facing subset of the warehouse database into the
    storefront Supabase project (a separate account the FarmUncle app
    connects to for price_cache reads). Syncs the four dimension
    tables (crops, mandis, varieties, grades) plus price_cache.

    Deliberately NOT synced: mandi_daily_prices and every raw/audit/
    ingestion-machinery table. The storefront is a clean read-only
    serving layer, not a mirror of the whole warehouse. The app queries
    the warehouse directly (a separate connection) for chart/technical
    drill-down views that need day-by-day history.

    Only ACTIVE crops/mandis are synced. Since varieties/grades in the
    warehouse can reference a crop/variety that is NOT active (MERGED/
    INACTIVE/UNKNOWN), every child table is filtered down to rows whose
    parent actually made it into the storefront — otherwise the FK
    constraints on the storefront side reject the insert.

    price_cache carries real variety_id/grade_id foreign keys (not just
    the denormalized variety/grade text) as of the 2026-07-26 migration
    on both warehouse and storefront — the app should join on these ids,
    not string-match variety/grade text against varieties.name/grades.name.

Run order matters (FK dependencies on the storefront side):
    crops -> mandis -> varieties -> grades -> price_cache

Secrets required (GitHub Actions):
    SUPABASE_URL, SUPABASE_SERVICE_KEY
        - already exist in this repo; these point at the WAREHOUSE
          project. Read-only use here.
    STOREFRONT_SUPABASE_URL, STOREFRONT_SUPABASE_SERVICE_KEY
        - NEW secrets, pointing at the separate storefront project's
          Settings -> API -> Project URL / service_role key.
"""
from __future__ import annotations

import os
import sys

from supabase import create_client, Client

PAGE_SIZE = 1000
UPSERT_BATCH_SIZE = 500


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"ERROR: missing required environment variable {name}", file=sys.stderr)
        sys.exit(1)
    return value


def fetch_all(client: Client, table: str, columns: str, filters=None) -> list[dict]:
    """Page through a table in chunks of PAGE_SIZE, applying optional
    equality filters, and return every row."""
    rows: list[dict] = []
    start = 0
    while True:
        query = client.table(table).select(columns)
        if filters:
            for col, val in filters.items():
                query = query.eq(col, val)
        query = query.range(start, start + PAGE_SIZE - 1)
        result = query.execute()
        batch = result.data or []
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        start += PAGE_SIZE
    return rows


def upsert_in_batches(client: Client, table: str, rows: list[dict], on_conflict: str) -> None:
    if not rows:
        print(f"[{table}] 0 rows, skipping")
        return
    for i in range(0, len(rows), UPSERT_BATCH_SIZE):
        chunk = rows[i:i + UPSERT_BATCH_SIZE]
        client.table(table).upsert(chunk, on_conflict=on_conflict).execute()
    print(f"[{table}] upserted {len(rows)} rows")


def sync_crops(warehouse: Client, storefront: Client) -> set[int]:
    rows = fetch_all(
        warehouse, "crops", "id,name,category,unit",
        filters={"status": "ACTIVE"},
    )
    upsert_in_batches(storefront, "crops", rows, on_conflict="id")
    return {r["id"] for r in rows}


def sync_mandis(warehouse: Client, storefront: Client) -> set[int]:
    rows = fetch_all(
        warehouse, "mandis",
        "id,slug,name,state,district,taluk,latitude,longitude,location_confidence",
        filters={"status": "ACTIVE"},
    )
    upsert_in_batches(storefront, "mandis", rows, on_conflict="id")
    return {r["id"] for r in rows}


def sync_varieties(warehouse: Client, storefront: Client, active_crop_ids: set[int]) -> set[int]:
    all_rows = fetch_all(warehouse, "varieties", "id,crop_id,name")
    rows = [r for r in all_rows if r["crop_id"] in active_crop_ids]
    upsert_in_batches(storefront, "varieties", rows, on_conflict="id")
    dropped = len(all_rows) - len(rows)
    if dropped:
        print(f"[varieties] dropped {dropped} rows referencing an inactive crop")
    return {r["id"] for r in rows}


def sync_grades(warehouse: Client, storefront: Client, active_variety_ids: set[int]) -> None:
    all_rows = fetch_all(warehouse, "grades", "id,variety_id,name")
    rows = [r for r in all_rows if r["variety_id"] in active_variety_ids]
    upsert_in_batches(storefront, "grades", rows, on_conflict="id")
    dropped = len(all_rows) - len(rows)
    if dropped:
        print(f"[grades] dropped {dropped} rows referencing an inactive variety")


def sync_price_cache(
    warehouse: Client,
    storefront: Client,
    active_crop_ids: set[int],
    active_mandi_ids: set[int],
    active_variety_ids: set[int],
) -> None:
    # price_cache is a full snapshot table (not append-only), so a full
    # replace is the correct semantics: rows that vanished from the
    # warehouse (e.g. a mandi/crop/variety combo no longer reporting)
    # must vanish from the storefront too.
    all_rows = fetch_all(
        warehouse, "price_cache",
        "mandi_id,crop_id,variety,grade,variety_id,grade_id,latest_price_date,"
        "modal_price,min_price,max_price,source_mandi_name,change_1d,change_1d_pct,"
        "change_7d,change_7d_pct,change_1m,change_1m_pct",
    )
    rows = [
        r for r in all_rows
        if r["mandi_id"] in active_mandi_ids
        and r["crop_id"] in active_crop_ids
        and r["variety_id"] in active_variety_ids
    ]

    # Delete-all then insert. mandi_id is always a positive bigint, so
    # this filter matches every existing row without needing a
    # driver-level "delete everything" call.
    storefront.table("price_cache").delete().gt("mandi_id", 0).execute()

    for i in range(0, len(rows), UPSERT_BATCH_SIZE):
        chunk = rows[i:i + UPSERT_BATCH_SIZE]
        storefront.table("price_cache").insert(chunk).execute()

    print(f"[price_cache] replaced with {len(rows)} rows "
          f"({len(all_rows) - len(rows)} dropped as inactive)")


def main() -> None:
    warehouse_url = _require_env("SUPABASE_URL")
    warehouse_key = _require_env("SUPABASE_SERVICE_KEY")
    storefront_url = _require_env("STOREFRONT_SUPABASE_URL")
    storefront_key = _require_env("STOREFRONT_SUPABASE_SERVICE_KEY")

    warehouse = create_client(warehouse_url, warehouse_key)
    storefront = create_client(storefront_url, storefront_key)

    # Order matters: parents before children (FK constraints on storefront).
    active_crop_ids = sync_crops(warehouse, storefront)
    active_mandi_ids = sync_mandis(warehouse, storefront)
    active_variety_ids = sync_varieties(warehouse, storefront, active_crop_ids)
    sync_grades(warehouse, storefront, active_variety_ids)
    sync_price_cache(warehouse, storefront, active_crop_ids, active_mandi_ids, active_variety_ids)

    print("Storefront sync complete.")


if __name__ == "__main__":
    main()