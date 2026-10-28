"""Move days that aged out of the warehouse's live window to R2, then delete them from the warehouse.

Nightly. Only final days (or untracked / past the give-up age) are touched; see aged_price_dates().
"""

from __future__ import annotations

from datetime import date

from farmuncle_pipeline.archive.catalog import export_catalog
from farmuncle_pipeline.archive.export import archive_day
from farmuncle_pipeline.archive.store import R2Store
from farmuncle_pipeline.ingest_common import validate_startup


def main() -> None:
    if not R2Store.configured():
        print("[archive] MANDI_R2_* credentials are not set; nothing archived or deleted.")
        return
    ctx = validate_startup()
    db = ctx.supabase
    store = R2Store.from_env()
    store.ensure_bucket()
    store.selfcheck()
    raw = db.rpc("aged_price_dates", {"p_keep_days": 11}).execute().data or []
    days = [date.fromisoformat(r if isinstance(r, str) else r["aged_price_dates"]) for r in raw]
    for day in days:
        result = archive_day(db, store, day)  # raises unless the upload is verified
        db.rpc("delete_price_window", {"p_start": day.isoformat(), "p_end": day.isoformat()}).execute()
        print(f"[archive_aged] {day}: {result.rows} rows archived and removed from the warehouse")
    if days:
        db.rpc("refresh_price_reference").execute()
        export_catalog(db, store)
    print(f"[archive_aged] days archived: {len(days)}")


if __name__ == "__main__":
    main()
