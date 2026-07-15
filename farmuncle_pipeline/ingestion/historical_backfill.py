"""
FarmUncle v2 — historical_backfill.py
Phase C, Step 16.

Purpose (module-level):
    "16. historical_backfill.py" (Master Build Specification §21,
    Phase C). Runs the shared Resource 2 ingestion pipeline
    (`resource2_pipeline.ingest_resource2_for_date` — the same one
    `daily_rewrite.py` uses for "today") once per date across an
    operator-specified historical date range, populating
    `mandi_daily_prices` for dates prior to today. Per §10 assumption
    3 ("Resource 2's historical data, once published, doesn't change
    retroactively"), a backfilled date is treated the same as any
    other Resource 2 write — same §8 precedence enforcement, same
    identity/quality pipeline — there is no separate "historical mode"
    logic in the pipeline itself, only in which date(s) this script
    asks it to run for.

Why this only targets Resource 2, not Resource 1:
    Resource 1 is explicitly "today only, best-effort" (§9) — it has
    no historical data to backfill. Resource 2 is "authoritative daily
    + historical" (§9), so it is the only source this script needs.

Date range boundary — why `--end-date` must be strictly before today:
    `daily_rewrite.py` owns "today" (§6.6 data ownership: it targets
    the current IST date every run). This script is for CLOSED,
    already-past days only — it refuses to run against today or a
    future date, both to keep that ownership line clean and because
    Resource 2 may not even have finalized data for today yet
    depending on time of day (§9). This is enforced before any network
    call is made (see `_validate_date_range`).

Per-date resilience vs. whole-run concurrency:
    A catastrophic failure on ONE date (already marked FAILED by
    `ingest_resource2_for_date` itself) does not abort the rest of the
    range — a transient failure on 2019-03-14 shouldn't prevent
    2019-03-15 through 2019-03-20 from being attempted (§24 performance
    target treats backfill in week-sized chunks, implying exactly this
    kind of multi-day resilience). `JobAlreadyRunningError`, however,
    DOES abort the entire remaining range immediately: it means another
    `historical_backfill` invocation (or a crashed prior one that never
    closed its batch — a genuine operational condition requiring manual
    investigation, not something this script auto-recovers from) is
    already using the `"historical_backfill"` job-name lock (§12), and
    since every date in this run shares that same job_name, retrying
    the next date would hit the identical guard.

What this script deliberately does NOT do:
    - Raise a §16 outage `quality_alerts` row on failure — that alert
      is specific to `daily_rewrite`'s own "is Resource 2 reachable
      right now" question; a backfill run failing on some historical
      dates says nothing about current-day reachability (see
      `daily_rewrite.py`'s docstring).
    - Retry a failed page inline — recorded in `failed_pages`,
      `retry_failed_pages.py` (Step 17)'s job.
    - Write to Cloudflare R2 / the historical Supabase project
      ("Account 2") — that is `weekly_compress`'s job (storage/
      module, a later phase); this script only populates Account 1's
      `mandi_daily_prices`, same table `daily_rewrite`/`live_tick` use.

Usage:
    python historical_backfill.py --start-date 2026-06-01 --end-date 2026-06-07
    python historical_backfill.py --start-date 2026-06-01
        (--end-date omitted => defaults to --start-date, i.e. one day)
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone

from farmuncle_pipeline.core.batch_lifecycle import JobAlreadyRunningError
from farmuncle_pipeline.ingest_common import IngestionBatchStatus, STATES, validate_startup
from farmuncle_pipeline.core.resource2_pipeline import ingest_resource2_for_date

JOB_NAME = "historical_backfill"
_IST = timezone(timedelta(hours=5, minutes=30))


class DateRangeError(ValueError):
    """Purpose: raised by `_validate_date_range` for any CLI date-range
    problem (bad order, touching today/future) — caught in `main` and
    reported as a clean usage error, not a stack trace, since this is
    always an operator input mistake, never a runtime/network fault."""


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise DateRangeError(f"Invalid date {value!r} — expected YYYY-MM-DD") from exc


def _validate_date_range(start: date, end: date) -> None:
    """
    Purpose:
        Enforce the two invariants this script requires before making
        any network call: `start <= end`, and `end` is strictly before
        today (IST) — see module docstring's "Date range boundary"
        section for why today itself is off-limits.
    Inputs:
        start / end: the parsed `--start-date`/`--end-date` values.
    Outputs:
        None.
    Failure modes:
        Raises `DateRangeError` for either violation.
    """
    if start > end:
        raise DateRangeError(f"--start-date ({start}) is after --end-date ({end})")

    today_ist = datetime.now(_IST).date()
    if end >= today_ist:
        raise DateRangeError(
            f"--end-date ({end}) must be strictly before today ({today_ist}, IST) — "
            f"today's data is daily_rewrite.py's job, not historical_backfill.py's"
        )


def _date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def run_historical_backfill(ctx, *, start_date: date, end_date: date) -> None:
    """
    Purpose:
        Run `ingest_resource2_for_date` once for every date from
        `start_date` to `end_date` inclusive, printing a per-date
        summary line and a final range summary. See module docstring
        for the resilience/abort rules this follows.
    Inputs:
        ctx: a `StartupContext` from `validate_startup()`.
        start_date / end_date: already validated by
            `_validate_date_range` before this is called.
    Outputs:
        None.
    Failure modes:
        Does not raise for an individual date's catastrophic failure
        (logged and skipped — that date's batch rows are already
        correctly marked FAILED by `ingest_resource2_for_date` itself).
        Re-raises `JobAlreadyRunningError` after logging it, since that
        aborts the entire run rather than being per-date resilient
        (see module docstring).
    """
    succeeded = partial = failed = 0

    for target_date in _date_range(start_date, end_date):
        try:
            result = ingest_resource2_for_date(ctx, target_date=target_date, job_name=JOB_NAME)
        except JobAlreadyRunningError as exc:
            print(
                f"[historical_backfill] {exc} — aborting remaining range "
                f"({target_date} through {end_date} not attempted)."
            )
            raise
        except Exception as exc:
            # ingest_resource2_for_date already marked this date's batch
            # rows FAILED before re-raising — a catastrophic failure on
            # one date should not stop the rest of the range.
            print(f"[historical_backfill] {target_date}: FAILED — {exc}")
            failed += 1
            continue

        if result.final_status == IngestionBatchStatus.SUCCESS:
            succeeded += 1
        elif result.final_status == IngestionBatchStatus.PARTIAL:
            partial += 1
        else:
            failed += 1

        print(
            f"[historical_backfill] {target_date}: {result.final_status.value} — "
            f"{result.rows_processed} row(s) processed, {result.rows_failed} skipped (parse/identity), "
            f"{result.rows_quarantined} quarantined (constraint violation), "
            f"{result.precedence_skipped} skipped (§8 precedence), "
            f"{result.states_with_any_success}/{len(STATES)} states yielded data, "
            f"{result.pages_fetched} page(s) fetched -- "
            f"timing: fetch={result.fetch_seconds:.1f}s, identity={result.identity_seconds:.1f}s, "
            f"upsert={result.upsert_seconds:.1f}s -- "
            f"identity resolution: mandi cache={result.identity_stats['mandi_cache_hits']}/"
            f"preload={result.identity_stats['mandi_preload_hits']}/"
            f"rpc={result.identity_stats['mandi_rpc_calls']}, "
            f"crop cache={result.identity_stats['crop_cache_hits']}/"
            f"preload={result.identity_stats['crop_preload_hits']}/"
            f"rpc={result.identity_stats['crop_rpc_calls']}"
        )

    total = succeeded + partial + failed
    print(
        f"[historical_backfill] range {start_date} to {end_date} complete — "
        f"{total} date(s) attempted: {succeeded} SUCCESS, {partial} PARTIAL, {failed} FAILED"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill FarmUncle v2 mandi_daily_prices from Resource 2 for a historical date range."
    )
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD, inclusive")
    parser.add_argument(
        "--end-date",
        required=False,
        default=None,
        help="YYYY-MM-DD, inclusive. Defaults to --start-date (i.e. a single day) if omitted.",
    )
    args = parser.parse_args()

    try:
        start_date = _parse_date(args.start_date)
        end_date = _parse_date(args.end_date) if args.end_date else start_date
        _validate_date_range(start_date, end_date)
    except DateRangeError as exc:
        parser.error(str(exc))  # prints usage + message, exits nonzero
        return

    ctx = validate_startup()
    run_historical_backfill(ctx, start_date=start_date, end_date=end_date)


if __name__ == "__main__":
    main()