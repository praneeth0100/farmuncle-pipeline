"""Move a range of FINAL days from the warehouse to R2, one day at a time, then delete each day after its upload is verified.

Usage:
    python -m farmuncle_pipeline.ingestion.archive_range --start 2026-10-19 --end 2026-10-27
Days that are not FINAL in ingest_days are skipped, never archived.
"""

from __future__ import annotations

import argparse
from datetime import date

from farmuncle_pipeline.archive.catalog import export_catalog
from farmuncle_pipeline.archive.export import archive_day
from farmuncle_pipeline.archive.store import R2Store
from farmuncle_pipeline.ingest_common import validate_startup


def final_days(rows: list[dict], start: date, end: date) -> list[date]:
    """Days in [start, end] whose ingest_days status is FINAL, oldest first."""
    days = sorted(date.fromisoformat(r["price_date"]) for r in rows if r["status"] == "FINAL")
    return [d for d in days if start <= d <= end]


def main() -> None:
    parser = argparse.ArgumentParser(description="Archive a range of FINAL days to R2.")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)

    if not R2Store.configured():
        print("[archive_range] MANDI_R2_* credentials are not set; nothing archived or deleted.")
        return
    ctx = validate_startup()
    db = ctx.supabase
    store = R2Store.from_env()
    store.ensure_bucket()
    store.selfcheck()

    rows = db.table("ingest_days").select("price_date,status").gte("price_date", start.isoformat()).lte("price_date", end.isoformat()).execute().data or []
    days = final_days(rows, start, end)
    skipped = sorted({r["price_date"] for r in rows} - {d.isoformat() for d in days})
    if skipped:
        print(f"[archive_range] skipped, not FINAL: {', '.join(skipped)}")
    for day in days:
        result = archive_day(db, store, day)  # raises ArchiveError unless the upload is verified
        db.rpc("delete_price_window", {"p_start": day.isoformat(), "p_end": day.isoformat()}).execute()
        print(f"[archive_range] {day}: {result.rows} rows archived, {result.quarantined} quarantined, removed from the warehouse")
    if days:
        db.rpc("refresh_price_reference").execute()
        export_catalog(db, store)
    print(f"[archive_range] days archived: {len(days)}")


if __name__ == "__main__":
    main()
