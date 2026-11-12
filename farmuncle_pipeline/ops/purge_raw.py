"""Delete raw price entries older than the retention window (default 3 days).

The raw table is append-only; purge_raw_price_entries() is the one function allowed to delete from it.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from postgrest import ReturnMethod

from farmuncle_pipeline.ingest_common import validate_startup

KEEP_DAYS = 3
KEEP_CALL_LOG_DAYS = 14  # api_call_logs: enough for the freshness check and a fortnight of debugging


def main() -> None:
    parser = argparse.ArgumentParser(description="Purge raw price entries past the retention window.")
    parser.add_argument("--keep-days", type=int, default=KEEP_DAYS)
    args = parser.parse_args()

    ctx = validate_startup()
    deleted = ctx.supabase.rpc("purge_raw_price_entries", {"p_keep_days": args.keep_days}).execute().data
    print(f"[purge_raw] deleted {deleted} raw entries older than {args.keep_days} days")
    superseded = ctx.supabase.rpc("purge_superseded_live_raw").execute().data
    print(f"[purge_raw] deleted {superseded} live (Resource 1) raw entries for days Resource 2 has finalised")
    # The call log grew by every API page ever fetched (311k rows on 12 Nov 2026), counting against the database quota.
    cutoff = (datetime.now(timezone.utc) - timedelta(days=KEEP_CALL_LOG_DAYS)).isoformat()
    ctx.supabase.table("api_call_logs").delete(returning=ReturnMethod.minimal).lt("called_at", cutoff).execute()
    print(f"[purge_raw] deleted api_call_logs older than {KEEP_CALL_LOG_DAYS} days")


if __name__ == "__main__":
    main()
