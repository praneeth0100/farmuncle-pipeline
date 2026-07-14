"""
FarmUncle v2 — review_quality_issues.py
Ops tooling (not a Phase C ingestion script — no batch lifecycle, does
not touch `mandi_daily_prices`, not meant to run on a schedule).

Purpose (module-level):
    A human-facing triage tool for `data_quality_issues` — the rows
    `upsert_price_rows` (see `price_writer.py`) quarantined because
    they violated a database constraint at write time (e.g.
    `chk_prices_min_max`, the 2026-06-29 incident: a government
    min_price/max_price 10x data-entry typo). Those rows are recorded
    but nothing reviews them automatically — this script is that
    review step: list what's PENDING, then mark each one CORRECTED
    (a human fixed the underlying data and it's been re-ingested via
    the `manual` source) or DISCARDED (genuinely bad data, not worth
    fixing, safe to ignore going forward).

    Deliberately separate from the ingestion scripts in
    `farmuncle_pipeline/ingestion/`: this doesn't fetch from the
    government API, doesn't open an `ingestion_batches` row, and isn't
    meant to run unattended on a schedule — it's a human sitting down
    and making judgment calls, which is why `data_quality_issues` has
    `review_status`/`reviewed_at`/`review_note` columns in the first
    place rather than a fully automated resolution path.

Usage:
    List the oldest 20 PENDING issues (default):
        python -m farmuncle_pipeline.ops.review_quality_issues list

    List more, or include already-reviewed rows:
        python -m farmuncle_pipeline.ops.review_quality_issues list --limit 50
        python -m farmuncle_pipeline.ops.review_quality_issues list --status CORRECTED

    Mark one row reviewed after looking at it:
        python -m farmuncle_pipeline.ops.review_quality_issues resolve 42 \\
            --status DISCARDED --note "confirmed government typo, min/max swapped"
        python -m farmuncle_pipeline.ops.review_quality_issues resolve 42 \\
            --status CORRECTED --note "re-entered manually, see manual batch 2026-07-15"
"""

from __future__ import annotations

import argparse
import sys

from farmuncle_pipeline.config import ConfigError
from farmuncle_pipeline.core.startup_validation import validate_startup

_VALID_REVIEW_STATUSES = ("PENDING", "CORRECTED", "DISCARDED")


def _list_issues(supabase, *, status: str, limit: int) -> None:
    """
    Purpose:
        Print a compact, human-scannable summary of
        `data_quality_issues` rows matching `status`, oldest first (so
        the longest-neglected ones surface first).
    Inputs:
        supabase: an already-constructed Supabase client.
        status: one of `_VALID_REVIEW_STATUSES` to filter on.
        limit: max rows to print.
    Outputs:
        None (prints to stdout).
    Failure modes:
        Raises `ConfigError` if the query fails.
    """
    try:
        result = (
            supabase.table("data_quality_issues")
            .select("id,batch_id,resource,error_code,error_message,row_data,created_at")
            .eq("review_status", status)
            .order("created_at", desc=False)
            .limit(limit)
            .execute()
        )
    except Exception as exc:
        raise ConfigError(f"Failed to list data_quality_issues: {exc}") from exc

    rows = result.data or []
    if not rows:
        print(f"[review_quality_issues] no rows with review_status={status!r}.")
        return

    print(f"[review_quality_issues] {len(rows)} row(s) with review_status={status!r}:\n")
    for row in rows:
        rd = row.get("row_data") or {}
        price_bits = ", ".join(
            f"{k}={rd[k]}" for k in ("modal_price", "min_price", "max_price") if k in rd
        )
        print(
            f"  id={row['id']}  batch_id={row['batch_id']}  resource={row['resource']}\n"
            f"    error_code={row.get('error_code')}  {row.get('error_message', '')[:160]}\n"
            f"    row: mandi_id={rd.get('mandi_id')} crop_id={rd.get('crop_id')} "
            f"price_date={rd.get('price_date')}  {price_bits}\n"
            f"    created_at={row['created_at']}\n"
        )


def _resolve_issue(supabase, *, issue_id: int, status: str, note: str | None) -> None:
    """
    Purpose:
        Mark one `data_quality_issues` row as reviewed.
    Inputs:
        supabase: an already-constructed Supabase client.
        issue_id: the row's `id`.
        status: "CORRECTED" or "DISCARDED" (not "PENDING" — that's the
            default state, not something to set back to).
        note: optional free-text reasoning, stored in `review_note`
            for future reference.
    Outputs:
        None (prints confirmation to stdout).
    Failure modes:
        Raises `ConfigError` if the update fails or matches no row.
    """
    if status == "PENDING":
        raise ConfigError(
            "Refusing to set review_status back to PENDING — that's the default "
            "state for a new issue, not something to resolve back to."
        )
    try:
        result = (
            supabase.table("data_quality_issues")
            .update(
                {
                    "review_status": status,
                    "review_note": note,
                    "reviewed_at": "now()",
                }
            )
            .eq("id", issue_id)
            .execute()
        )
    except Exception as exc:
        raise ConfigError(f"Failed to update data_quality_issues id={issue_id}: {exc}") from exc

    if not result.data:
        raise ConfigError(f"No data_quality_issues row with id={issue_id} was found.")
    print(f"[review_quality_issues] id={issue_id} marked {status}.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    list_parser = sub.add_parser("list", help="List quarantined rows awaiting review.")
    list_parser.add_argument(
        "--status", default="PENDING", choices=_VALID_REVIEW_STATUSES,
        help="Filter by review_status (default: PENDING).",
    )
    list_parser.add_argument(
        "--limit", type=int, default=20, help="Max rows to print (default: 20).",
    )

    resolve_parser = sub.add_parser("resolve", help="Mark one row as reviewed.")
    resolve_parser.add_argument("issue_id", type=int, help="The data_quality_issues.id to resolve.")
    resolve_parser.add_argument(
        "--status", required=True, choices=("CORRECTED", "DISCARDED"),
        help="What the review concluded.",
    )
    resolve_parser.add_argument(
        "--note", default=None, help="Free-text note explaining the resolution.",
    )

    args = parser.parse_args()

    try:
        ctx = validate_startup()
    except ConfigError as exc:
        print(f"[review_quality_issues] startup failed: {exc}", file=sys.stderr)
        sys.exit(1)

    supabase = ctx.supabase

    try:
        if args.command == "list":
            _list_issues(supabase, status=args.status, limit=args.limit)
        elif args.command == "resolve":
            _resolve_issue(supabase, issue_id=args.issue_id, status=args.status, note=args.note)
    except ConfigError as exc:
        print(f"[review_quality_issues] {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
