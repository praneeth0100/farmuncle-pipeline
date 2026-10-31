"""Export one day of prices from the warehouse to R2 as Parquet, verified before anything is deleted."""

from __future__ import annotations

import hashlib
import io
import json
from dataclasses import dataclass
from datetime import date

PAGE = 1000
FACT_COLUMNS = (
    "mandi_id", "crop_id", "variety_id", "grade_id", "price_date", "modal_price", "min_price", "max_price",
    "unit", "source", "source_mandi_id", "quality_score", "quality_components",
)


class ArchiveError(RuntimeError):
    """The upload could not be verified; nothing may be deleted from the warehouse."""


@dataclass(frozen=True)
class DayArchive:
    day: date
    rows: int
    bytes: int
    sha256: str | None
    quarantined: int = 0
    r2_rows: int | None = None  # rows read back from the stored file; None when the read-back could not run


def day_key(day: date) -> str:
    return f"facts/daily/date={day.isoformat()}.parquet"


def manifest_key(day: date) -> str:
    return f"facts/manifest/date={day.isoformat()}.json"


def fetch_day_rows(supabase, day: date) -> list[dict]:
    rows: list[dict] = []
    last_id = 0  # keyset paging: a deep offset on a full day hits the database's statement timeout
    while True:
        page = (
            supabase.table("mandi_daily_prices").select("id," + ",".join(FACT_COLUMNS))
            .eq("price_date", day.isoformat()).gt("id", last_id).order("id").limit(PAGE).execute().data or []
        )
        rows.extend(page)
        if len(page) < PAGE:
            return rows
        last_id = page[-1]["id"]


def rows_to_parquet(rows: list[dict]) -> bytes:
    import pyarrow as pa
    import pyarrow.parquet as pq

    columns = {c: [r.get(c) for r in rows] for c in FACT_COLUMNS}
    columns["quality_components"] = [json.dumps(v, separators=(",", ":")) if v is not None else None for v in columns["quality_components"]]
    buf = io.BytesIO()
    pq.write_table(pa.table(columns), buf, compression="zstd")
    return buf.getvalue()


QUARANTINE_COLUMNS = (
    "id", "batch_id", "resource", "row_data", "created_at", "error_message", "error_code", "review_status", "review_note",
)


def quarantine_key(day: date) -> str:
    return f"quarantine/date={day.isoformat()}.parquet"


def fetch_quarantine_rows(supabase, day: date) -> list[dict]:
    """Doubtful rows (quarantined, merge collisions) for the day; delete_price_window removes them with it."""
    rows: list[dict] = []
    last_id = 0  # keyset paging, as in fetch_day_rows
    while True:
        page = (
            supabase.table("data_quality_issues").select(",".join(QUARANTINE_COLUMNS))
            .eq("row_data->>price_date", day.isoformat()).gt("id", last_id).order("id").limit(PAGE).execute().data or []
        )
        rows.extend(page)
        if len(page) < PAGE:
            return rows
        last_id = page[-1]["id"]


def _archive_quarantine(supabase, store, day: date) -> int:
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows = fetch_quarantine_rows(supabase, day)
    if not rows:
        return 0
    columns = {c: [r.get(c) for r in rows] for c in QUARANTINE_COLUMNS}
    columns["row_data"] = [json.dumps(v, separators=(",", ":")) if v is not None else None for v in columns["row_data"]]
    buf = io.BytesIO()
    pq.write_table(pa.table(columns), buf, compression="zstd")
    data, key = buf.getvalue(), quarantine_key(day)
    store.put(key, data)
    if store.size(key) != len(data):
        raise ArchiveError(f"{key}: stored size does not match the {len(data)} bytes sent")
    return len(rows)


def _stored_rows(store, key: str) -> int | None:
    """Row count of the file as R2 holds it. Record-only: a failed read-back returns None, it never stops the day."""
    try:
        import pyarrow.parquet as pq

        return pq.ParquetFile(io.BytesIO(store.get(key))).metadata.num_rows
    except Exception as exc:
        print(f"[archive] read-back of {key} failed, not checked: {exc}")
        return None


def _archived_rows(store, day: date) -> int | None:
    """Rows the day's manifest says R2 already holds; None when there is no manifest (or it cannot be read)."""
    try:
        return int(json.loads(store.get(manifest_key(day)))["rows"])
    except Exception:
        return None


def archive_day(supabase, store, day: date) -> DayArchive:
    """Upload the day's Parquet and a manifest; raise ArchiveError unless the stored size matches.

    Never replaces a day R2 already holds with fewer rows: a day left with a few hundred rows by a failed pull
    was archived over its full file (2 to 27 June 2026: ~500 rows each instead of ~20,000).

    The day's quarantined rows go to R2 too, so deleting the window never loses a doubtful record.
    """
    quarantined = _archive_quarantine(supabase, store, day)
    rows = fetch_day_rows(supabase, day)
    if not rows:
        return DayArchive(day, 0, 0, None, quarantined)
    data = rows_to_parquet(rows)
    key = day_key(day)
    held = _archived_rows(store, day)
    if held is not None and held > len(rows):
        raise ArchiveError(f"{key}: R2 already holds {held} rows for this day and the warehouse has {len(rows)}; not replacing it with less")
    store.put(key, data)
    if store.size(key) != len(data):
        raise ArchiveError(f"{key}: stored size does not match the {len(data)} bytes sent")
    digest = hashlib.sha256(data).hexdigest()
    manifest = {"date": day.isoformat(), "rows": len(rows), "bytes": len(data), "sha256": digest, "key": key, "quarantined": quarantined}
    store.put(manifest_key(day), json.dumps(manifest, indent=1).encode(), "application/json")
    r2_rows = _stored_rows(store, key)
    if r2_rows is not None and r2_rows != len(rows):
        print(f"[archive] ROW COUNT MISMATCH {day}: sent {len(rows)}, R2 holds {r2_rows} (recorded, not stopping)")
    return DayArchive(day, len(rows), len(data), digest, quarantined, r2_rows)
