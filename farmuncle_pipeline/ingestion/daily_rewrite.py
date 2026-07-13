"""
FarmUncle v2 — daily_rewrite.py
Phase C, Step 15.

Purpose (module-level):
    "15. daily_rewrite.py — Resource 2, honors §8 precedence, pauses
    per §16" (Master Build Specification §21, Phase C). Runs the
    shared Resource 2 ingestion pipeline (`resource2_pipeline.py`) for
    a single date — today — and handles what's genuinely specific to
    this script: the §12 concurrency guard's clean-exit behavior, and
    the §16 "Resource 2 unreachable 3+ days" outage alert.

    This script is a single run, meant to be invoked once daily in the
    evening (the schedule itself is Phase D's job; this script has no
    internal scheduling and does not loop).

Why this is a thin wrapper (Step 16 update):
    At Step 15, this file contained the full fetch-all-states →
    parse → identity → quality → precedence-upsert pipeline directly.
    Step 16's `historical_backfill.py` needs byte-for-byte the same
    pipeline, run once per date across an arbitrary range instead of
    once for today — so that pipeline moved to
    `resource2_pipeline.ingest_resource2_for_date` (Never-Do Rule §2;
    same reasoning as the Step 14→15 extractions). This file now only
    contains what's actually specific to being "the once-daily,
    today-only" caller: which date to pass in, and the §16 alert.

Which calendar day this run targets:
    §8 calls Resource 2 "evening-finalized" and §9 says its records are
    "complete finalized daily records by evening." Read together, this
    means: by the time this script is meant to run (evening), the
    CURRENT day's records are already finalized — so this script
    targets today (IST), not yesterday. If the actual GitHub Actions
    schedule (Phase D) ends up running this before evening IST, this
    assumption should be revisited; that is a scheduling decision, not
    something this script can detect or correct for itself.

§16 recovery — "Resource 2 unreachable 3+ days":
    After a run is closed FAILED (a genuine total outage — see
    `resource2_pipeline.py`'s distinction between that and ordinary
    PARTIAL failures), this script checks the last 3 `ingestion_batches`
    rows for `job_name="daily_rewrite"`; if all 3 (including the one
    just closed) are FAILED, it raises a `quality_alerts` row with
    `severity=HIGH`, per §16's explicit instruction. This is specific
    to `daily_rewrite`'s own job history/cadence — `historical_backfill`
    does not get this same alert (see that script's docstring).

    This script does NOT implement "backlog processes oldest-first on
    return" — that is a recovery *procedure* spanning multiple future
    runs / possibly manual intervention, out of scope here.

What this script deliberately does NOT do:
    - Call `refresh_price_cache` — that RPC/table don't exist in the
      live schema yet.
    - Backfill historical dates — that's `historical_backfill.py`
      (Step 16), which calls the same shared pipeline per date.
    - Retry a failed page inline — recorded in `failed_pages`,
      `retry_failed_pages.py` (Step 17)'s job.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from farmuncle_pipeline.core.batch_lifecycle import JobAlreadyRunningError
from farmuncle_pipeline.ingest_common import ConfigError, IngestionBatchStatus, STATES, validate_startup
from farmuncle_pipeline.core.resource2_pipeline import ingest_resource2_for_date

JOB_NAME = "daily_rewrite"
_IST = timezone(timedelta(hours=5, minutes=30))

# Number of most-recent daily_rewrite runs (including the one that just
# failed) to inspect for the §16 "3+ days unreachable" outage alert.
_OUTAGE_ALERT_LOOKBACK = 3


def _raise_outage_alert_if_needed(supabase, *, batch_id: str) -> None:
    """
    Purpose:
        After a `daily_rewrite` run is closed FAILED, check whether
        this is the 3rd consecutive FAILED run for this job (§16:
        "Resource 2 unreachable 3+ days ... raises quality_alert
        (HIGH)"). If so, insert that alert. A single or second
        consecutive failure is expected/tolerable and does not alert —
        only a genuine multi-day pattern does.
    Inputs:
        supabase: an already-constructed Supabase client.
        batch_id: the `ingestion_batches.id` that was just closed
            FAILED (included in the alert for traceability).
    Outputs:
        None.
    Failure modes:
        Raises `ConfigError` if the lookback query or the alert insert
        itself fails.
    """
    try:
        response = (
            supabase.table("ingestion_batches")
            .select("status,started_at")
            .eq("job_name", JOB_NAME)
            .order("started_at", desc=True)
            .limit(_OUTAGE_ALERT_LOOKBACK)
            .execute()
        )
    except Exception as exc:
        raise ConfigError(f"Failed to check recent {JOB_NAME} batch history: {exc}") from exc

    recent = response.data or []
    if len(recent) < _OUTAGE_ALERT_LOOKBACK:
        return  # not enough history yet to call this a 3-day pattern

    if not all(row["status"] == IngestionBatchStatus.FAILED.value for row in recent):
        return

    try:
        supabase.table("quality_alerts").insert(
            {
                "severity": "HIGH",
                "message": (
                    f"{JOB_NAME} has failed {_OUTAGE_ALERT_LOOKBACK} consecutive runs — "
                    f"Resource 2 may be unreachable (§16 recovery procedure)"
                ),
                "batch_id": batch_id,
            }
        ).execute()
    except Exception as exc:
        raise ConfigError(f"Failed to write §16 outage quality_alerts row: {exc}") from exc


def run_daily_rewrite(ctx) -> None:
    """
    Purpose:
        Run the shared Resource 2 pipeline for today (IST), then check
        the §16 outage-alert condition if this run itself failed
        entirely.
    Inputs:
        ctx: a `StartupContext` from `validate_startup()`.
    Outputs:
        None.
    Failure modes:
        Re-raises any exception from the pipeline after it has already
        marked both batch rows FAILED (see
        `resource2_pipeline.ingest_resource2_for_date`).
        `JobAlreadyRunningError` is caught here and treated as a clean,
        expected early exit — a concurrent `daily_rewrite` run already
        in progress is not an error worth failing this invocation for.
    """
    today = datetime.now(_IST).date()

    try:
        result = ingest_resource2_for_date(ctx, target_date=today, job_name=JOB_NAME)
    except JobAlreadyRunningError as exc:
        print(f"[daily_rewrite] {exc} — exiting cleanly.")
        return

    print(
        f"[daily_rewrite] {result.final_status.value} — {result.rows_processed} row(s) processed, "
        f"{result.rows_failed} skipped (parse/identity), "
        f"{result.precedence_skipped} skipped (§8 precedence), "
        f"{result.states_with_any_success}/{len(STATES)} states yielded data, "
        f"{result.pages_fetched} page(s) fetched"
    )

    if result.final_status == IngestionBatchStatus.FAILED:
        try:
            _raise_outage_alert_if_needed(ctx.supabase, batch_id=result.batch_id)
        except ConfigError as alert_exc:
            # The batch itself is already correctly closed FAILED — a
            # failure to also write the §16 alert shouldn't crash an
            # otherwise-handled run. Surfaced loudly since it's a real
            # gap (the alert didn't go out), just not a reason to fail
            # this run's own exit status.
            print(f"[daily_rewrite] WARNING: §16 outage-alert check failed: {alert_exc}")


def main() -> None:
    ctx = validate_startup()
    run_daily_rewrite(ctx)


if __name__ == "__main__":
    main()
