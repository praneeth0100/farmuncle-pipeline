"""
FarmUncle v2 — ops_dashboard.py
Phase D, Step 20 (Master Build Specification §21/§22).

Purpose (module-level):
    §22 lists twelve health-check numbers and notes that every one of
    them "now exists in queryable tables as of Phase C — but no
    dashboard (Step 20) reads it yet. Every number above is currently
    only inspectable via direct SQL." This script is that dashboard:
    it calls the single `get_ops_dashboard()` RPC (one round trip, all
    the arithmetic done server-side in SQL, per invariant 3) and
    prints the same two-column layout from the spec.

    `get_ops_dashboard()` is revoked from anon/authenticated and
    granted only to service_role — this is pipeline operational
    metadata, not app data, and has no RLS policy of its own. This
    script must therefore run with the service-role key, same as
    every other script in this package (`validate_startup()` already
    enforces that).

Explicitly out of scope for this file:
    - Storage Used (Account 1 / Account 2 / R2) — no usage-tracking
      table exists yet for either Postgres account or the R2 bucket;
      left as "not tracked" below rather than a fabricated number.
    - Any alerting/paging — this is a read/print tool. Alert routing
      already exists separately in nightly_quality_audit.py's
      quality_alerts writes.

Usage:
    python -m farmuncle_pipeline.ops.ops_dashboard
"""

from __future__ import annotations

from farmuncle_pipeline.core.startup_validation import validate_startup


def _fmt_batch(label: str, batch: dict | None) -> str:
    if not batch:
        return f"{label:<24} (none yet)"
    return (
        f"{label:<24} {batch['started_at']}  {batch['status']:<8} "
        f"rows_processed={batch['rows_processed']}"
    )


def render_ops_dashboard(data: dict) -> str:
    """
    Purpose:
        Format the get_ops_dashboard() payload into the health-check
        layout from §22, plus the KPI line above it.
    Inputs:
        data: the jsonb payload returned by get_ops_dashboard().
    Outputs:
        A print-ready multi-line string.
    """
    compression = data.get("last_compression")
    compression_line = (
        f"{compression['status']} ({compression['week_start_date']}, "
        f"hash={compression['verification_hash'][:12]}...)"
        if compression
        else "(no compression run yet)"
    )
    coverage = data.get("latest_coverage")
    coverage_line = f"{coverage['coverage_pct']}% ({coverage['report_date']})" if coverage else "(no report yet)"

    lines = [
        f"FarmUncle v2 — Ops Dashboard   generated {data['generated_at']}",
        "=" * 70,
        "KPIs",
        "-" * 70,
        f"  Avg API latency (7d):     {data['avg_api_latency_ms_7d']} ms",
        f"  Retry % (7d):             {data['retry_pct_7d']}%",
        f"  Coverage %:               {coverage_line}",
        f"  New entities (7d):        {data['new_entities_7d']}",
        f"  Merge candidates:         {data['merge_candidates']}",
        f"  Auto-created %:           {data['auto_created_pct']}%",
        f"  Manual reviews pending:   {data['pending_reviews']}",
        "",
        "Health checks",
        "-" * 70,
        _fmt_batch("Last Live Tick", data["last_live_tick"]),
        _fmt_batch("Last Daily Rewrite", data["last_daily_rewrite"]),
        f"{'Rows Ingested Today':<24} {data['rows_ingested_today']}",
        f"{'Raw Records (total)':<24} {data['raw_records_total']}",
        f"{'Success % (7d)':<24} {data['success_pct_7d']}%",
        f"{'Failure % (7d)':<24} {data['failure_pct_7d']}%",
        f"{'Retry Queue (depth)':<24} {data['retry_queue_depth']}",
        f"{'Compression Status':<24} {compression_line}",
        f"{'Storage Used':<24} (not tracked — no usage table yet)",
    ]
    return "\n".join(lines)


def run_ops_dashboard(ctx) -> None:
    """
    Purpose:
        Fetch and print the dashboard. Deliberately writes nothing —
        this is a read-only reporting tool, unlike the other ops/
        scripts which leave durable records behind.
    Inputs:
        ctx: a `StartupContext` from `validate_startup()`.
    Outputs:
        None (prints to stdout).
    """
    result = ctx.supabase.rpc("get_ops_dashboard", {}).execute()
    print(render_ops_dashboard(result.data))


def main() -> None:
    ctx = validate_startup()
    run_ops_dashboard(ctx)


if __name__ == "__main__":
    main()
