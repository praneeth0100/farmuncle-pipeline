"""Copy FINAL days still inside the live window to R2. Never deletes anything.

Days older than the live window are archived and deleted by archive_aged / archive_history; this job only
puts a verified second copy of the newer days in R2, so it can run beside them without touching their data.

Usage:
    python -m farmuncle_pipeline.ingestion.archive_copy [--start YYYY-MM-DD] [--end YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta

from farmuncle_pipeline.archive.catalog import export_catalog
from farmuncle_pipeline.archive.export import archive_day
from farmuncle_pipeline.archive.store import R2Store
from farmuncle_pipeline.archive.tracker import record_day
from farmuncle_pipeline.ingest_common import validate_startup
from farmuncle_pipeline.ingestion.archive_range import final_days


def main() -> None:
    parser = argparse.ArgumentParser(description="Copy FINAL warehouse days to R2 without deleting them.")
    parser.add_argument("--start", default=(date.today() - timedelta(days=12)).isoformat())
    parser.add_argument("--end", default="2999-12-31")
    args = parser.parse_args()
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)

    if not R2Store.configured():
        print("[archive_copy] MANDI_R2_* credentials are not set; nothing copied.")
        return
    ctx = validate_startup()
    db = ctx.supabase
    store = R2Store.from_env()
    store.ensure_bucket()
    store.selfcheck()

    rows = db.table("ingest_days").select("price_date,status").gte("price_date", start.isoformat()).lte("price_date", end.isoformat()).execute().data or []
    days = final_days(rows, start, end)
    copied = 0
    for day in days:
        try:
            result = archive_day(db, store, day)  # raises ArchiveError unless the upload is verified
        except Exception as exc:
            print(f"[archive_copy] {day} not copied: {exc}")
            record_day(db, day, "FAILED", error=str(exc))
            continue
        if result.rows:  # a day the warehouse no longer holds is already archived; leave its record alone
            record_day(db, day, "ARCHIVED", result=result)
            copied += 1
            print(f"[archive_copy] {day}: {result.rows} rows copied, {result.quarantined} quarantined")
    if copied:
        export_catalog(db, store)
    print(f"[archive_copy] days copied: {copied}")


if __name__ == "__main__":
    main()
