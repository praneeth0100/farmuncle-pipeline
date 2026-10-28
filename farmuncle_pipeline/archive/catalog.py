"""Snapshot the small dimension tables (the ids the daily facts refer to) to R2 as Parquet."""

from __future__ import annotations

import io
import json

from farmuncle_pipeline.archive.export import PAGE

CATALOG_TABLES = ("mandis", "crops", "varieties", "grades")


def export_table(supabase, store, table: str) -> int:
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows: list[dict] = []
    start = 0
    while True:
        page = supabase.table(table).select("*").order("id").range(start, start + PAGE - 1).execute().data or []
        rows.extend(page)
        if len(page) < PAGE:
            break
        start += PAGE
    clean = [{k: (json.dumps(v) if isinstance(v, (dict, list)) else v) for k, v in r.items()} for r in rows]
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(clean), buf, compression="zstd")
    store.put(f"catalog/{table}.parquet", buf.getvalue())
    return len(rows)


def export_catalog(supabase, store) -> dict:
    return {t: export_table(supabase, store, t) for t in CATALOG_TABLES}
