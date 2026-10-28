"""Delete raw price entries older than the retention window (default 10 days).

The raw table is append-only; purge_raw_price_entries() is the one function allowed to delete from it.
"""

from __future__ import annotations

import argparse

from farmuncle_pipeline.ingest_common import validate_startup

KEEP_DAYS = 10


def main() -> None:
    parser = argparse.ArgumentParser(description="Purge raw price entries past the retention window.")
    parser.add_argument("--keep-days", type=int, default=KEEP_DAYS)
    args = parser.parse_args()

    ctx = validate_startup()
    deleted = ctx.supabase.rpc("purge_raw_price_entries", {"p_keep_days": args.keep_days}).execute().data
    print(f"[purge_raw] deleted {deleted} raw entries older than {args.keep_days} days")


if __name__ == "__main__":
    main()
