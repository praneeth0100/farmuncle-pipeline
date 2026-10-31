"""Fill history backwards, 10 days at a time, into R2 (see farmuncle_pipeline/archive/history.py).

Usage:
    python -m farmuncle_pipeline.ingestion.archive_history [--max-windows N] [--budget-minutes M] [--oldest YYYY-MM-DD]
Needs MANDI_R2_ACCOUNT_ID / MANDI_R2_ACCESS_KEY_ID / MANDI_R2_SECRET_ACCESS_KEY (and optionally MANDI_R2_BUCKET).
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone

from farmuncle_pipeline.archive.catalog import export_catalog
from farmuncle_pipeline.archive.export import archive_day
from farmuncle_pipeline.archive.history import DEFAULT_OLDEST, run_backward
from farmuncle_pipeline.archive.state import SupabaseArchiveState
from farmuncle_pipeline.archive.store import R2Store
from farmuncle_pipeline.archive.tracker import failed_days, record_day, typical_rows
from farmuncle_pipeline.core.resource2_pipeline import ingest_resource2_for_date
from farmuncle_pipeline.ingest_common import validate_startup

JOB_NAME = "archive_history"
_IST = timezone(timedelta(hours=5, minutes=30))


def main() -> None:
    parser = argparse.ArgumentParser(description="Archive price history to R2, newest window first.")
    parser.add_argument("--max-windows", type=int, default=None)
    parser.add_argument("--budget-minutes", type=float, default=300.0)
    parser.add_argument("--oldest", default=DEFAULT_OLDEST.isoformat())
    args = parser.parse_args()

    if not R2Store.configured():
        print("[archive] MANDI_R2_* credentials are not set; nothing archived or deleted.")
        return
    ctx = validate_startup()
    store = R2Store.from_env()
    store.ensure_bucket()
    store.selfcheck()
    db = ctx.supabase

    done = run_backward(
        today=datetime.now(_IST).date(),
        oldest=date.fromisoformat(args.oldest),
        state=SupabaseArchiveState(db),
        ingest_day=lambda d: ingest_resource2_for_date(ctx, target_date=d, job_name=JOB_NAME),
        archive_day=lambda d: archive_day(db, store, d),
        delete_window=lambda s, e: db.rpc("delete_price_window", {"p_start": s.isoformat(), "p_end": e.isoformat()}).execute(),
        refresh_reference=lambda: db.rpc("refresh_price_reference").execute(),
        after_window=lambda w: export_catalog(db, store),
        leftover_days=lambda after, up_to: sorted({
            *(date.fromisoformat(r if isinstance(r, str) else r["price_days_between"])
              for r in db.rpc("price_days_between", {"p_from": after.isoformat(), "p_to": up_to.isoformat()}).execute().data or []),
            *failed_days(db, after, up_to),
        }),
        record=lambda day, status, **kw: record_day(db, day, status, **kw),
        typical_rows=typical_rows(db),
        has_unfinal_rows=lambda d: bool(db.table("mandi_daily_prices").select("id").eq("price_date", d.isoformat())
                                        .neq("source", "resource_2").limit(1).execute().data),
        max_windows=args.max_windows,
        budget_seconds=args.budget_minutes * 60,
    )
    print(f"[{JOB_NAME}] windows completed this run: {done}")


if __name__ == "__main__":
    main()
