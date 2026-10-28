"""
FarmUncle v2 — daily_rewrite.py
Phase C, Step 15.

Purpose (module-level):
    "15. daily_rewrite.py — Resource 2, honors §8 precedence, pauses
    per §16" (Master Build Specification §21, Phase C). Runs the
    shared Resource 2 ingestion pipeline (`resource2_pipeline.py`) for
    each date still waiting for final data and handles what's genuinely specific to
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

Which calendar day(s) this run targets:
    §8/§9 originally assumed Resource 2 is "evening-finalized" for the
    current calendar day. Real-world observation on 2026-07-13 showed
    this is wrong: Resource 2 lags 1-2 days, variably, behind the
    calendar date, so a run that only ever asked for "today" found
    nothing on every single invocation.

    Phase 2.1 (replaces the fixed 3-day window of 2026-07-14): every date
    is a row in `ingest_days` (see `core/ingest_days.py`). Each run
    seeds the dates up to yesterday, then works through the PENDING
    ones, oldest first, until it is out of time or day budget. A date
    becomes FINAL only when Resource 2 delivered it completely, so a
    day the government has not published yet, or that failed, is
    simply tried again the next night, however long that takes. On
    Sundays the last 14 days are reopened to pick up late corrections.
    Today is never attempted: Resource 2 has no same-day data.

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

import time
from datetime import date, datetime, timedelta, timezone

from farmuncle_pipeline.core.batch_lifecycle import JobAlreadyRunningError
from farmuncle_pipeline.core.ingest_days import (
    decide,
    pending_days,
    recheck_recent_days,
    record_day_result,
    seed_pending_days,
)
from farmuncle_pipeline.government_constants import QUERY_STATES
from farmuncle_pipeline.ingest_common import ConfigError, IngestionBatchStatus, validate_startup
from farmuncle_pipeline.core.resource2_pipeline import ingest_resource2_for_date
from farmuncle_pipeline.core.price_cache import refresh_price_cache

JOB_NAME = "daily_rewrite"
_IST = timezone(timedelta(hours=5, minutes=30))

# Per-run limits. A date takes about 11 minutes (33+ state queries);
# the workflow timeout is 120 minutes, so stop starting new dates after
# 100 minutes. A long backlog (an outage) is cleared over several nights.
TIME_BUDGET_SECONDS = 100 * 60
MAX_DAYS_PER_RUN = 10
RECHECK_DAYS = 14  # reopened on Sundays

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


def run_daily_rewrite(
    ctx,
    *,
    today: date | None = None,
    clock=time.monotonic,
    time_budget_seconds: float = TIME_BUDGET_SECONDS,
    max_days: int = MAX_DAYS_PER_RUN,
) -> None:
    """
    Purpose:
        Run the shared Resource 2 pipeline once for every date that is
        still PENDING in `ingest_days` (oldest first), record whether
        each became FINAL, then refresh `price_cache` and check the §16
        outage-alert condition if the last date attempted failed
        entirely.
    Inputs:
        ctx: a `StartupContext` from `validate_startup()`.
        today / clock / time_budget_seconds / max_days: for tests and
            tuning; defaults are today in IST, the wall clock, and the
            module constants.
    Outputs:
        None.
    Failure modes:
        A single date's catastrophic failure is logged and recorded
        (the date stays PENDING) and does not stop the other dates.
        `JobAlreadyRunningError` ends the run cleanly: a concurrent
        `daily_rewrite` is already in progress.
    """
    supabase = ctx.supabase
    today = today or datetime.now(_IST).date()

    seed_pending_days(supabase, today=today)
    if today.weekday() == 6:  # Sunday: pick up late v2 corrections
        recheck_recent_days(supabase, today=today, days=RECHECK_DAYS)

    started = clock()
    attempted = 0
    last_result = None

    for target_date in pending_days(supabase, before=today):
        if attempted >= max_days or clock() - started > time_budget_seconds:
            print(f"[daily_rewrite] stopping after {attempted} date(s); the rest wait for the next run.")
            break
        attempted += 1
        try:
            result = ingest_resource2_for_date(ctx, target_date=target_date, job_name=JOB_NAME)
        except JobAlreadyRunningError as exc:
            print(f"[daily_rewrite] {exc} — exiting cleanly.")
            return
        except Exception as exc:
            print(f"[daily_rewrite] {target_date}: FAILED — {exc}")
            record_day_result(
                supabase, price_date=target_date, final=False, v2_rows=0, note=f"error: {exc}"[:300]
            )
            last_result = None
            continue

        decision = decide(
            final_status=result.final_status.value,
            total_records=result.total_records,
            price_date=target_date,
            today=today,
        )
        note = decision.note
        if note and not decision.final and getattr(result, "error_summary", None):
            note = f"{note}: {result.error_summary}"[:300]
        record_day_result(
            supabase,
            price_date=target_date,
            final=decision.final,
            v2_rows=result.total_records,
            note=note,
        )
        last_result = result
        print(
            f"[daily_rewrite] {target_date}: {result.final_status.value} — "
            f"{'FINAL' if decision.final else 'PENDING'} — {result.rows_processed} row(s) processed, "
            f"{result.rows_failed} skipped (parse/identity), "
            f"{result.precedence_skipped} skipped (§8 precedence), "
            f"{result.states_with_any_success}/{len(QUERY_STATES)} state queries yielded data, "
            f"{result.pages_fetched} page(s) fetched"
        )

    # §15 Cache Invalidation Policy: refresh price_cache after every
    # daily_rewrite run, regardless of per-date outcome above — a
    # partial run (some dates succeeded, some didn't) still means
    # newer data may exist than what's currently cached. Non-fatal on
    # failure; see price_cache.py docstring.
    refresh_price_cache(supabase, caller="daily_rewrite")

    if last_result is not None and last_result.final_status == IngestionBatchStatus.FAILED:
        try:
            _raise_outage_alert_if_needed(supabase, batch_id=last_result.batch_id)
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
