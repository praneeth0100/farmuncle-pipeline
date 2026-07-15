"""
FarmUncle v2 — resource2_pipeline.py
Phase C, Step 16 (extracted from Step 15's `daily_rewrite.py`).

Purpose (module-level):
    The full "ingest one date's worth of Resource 2 data" pipeline:
    acquire the batch/concurrency guard, page through every state,
    parse, resolve identity, score quality, enforce §8 precedence,
    upsert, and close out both batch rows. `daily_rewrite.py` (Step 15)
    runs this once for today; `historical_backfill.py` (Step 16) runs
    it once per date across an arbitrary historical range. Both need
    byte-for-byte identical behavior per date — the only things that
    legitimately differ between them are WHICH date(s) to run it for,
    what `job_name` to tag the batch rows with (so the §12 concurrency
    guard and §16 outage-alert logic, which are keyed by `job_name`,
    stay correctly separated per caller), and what each caller does
    with the aggregate results afterward (`daily_rewrite.py` runs its
    own §16 check; `historical_backfill.py` prints a per-date and
    range summary — neither of those belongs in this module, since
    they're caller-specific, not part of "ingest one date").

Why this wasn't its own module at Step 15:
    Same reasoning as `resource_client.py`/`price_writer.py` at Step
    15 itself: `daily_rewrite.py` had exactly one caller for this logic
    until `historical_backfill.py` needed it too. Pulled out now that a
    second real caller exists, per Never-Do Rule §2 — not a
    speculative Step 15 rewrite.

What stays with the caller, not here:
    - `JobAlreadyRunningError` is NOT caught here — it propagates to
      the caller, since `daily_rewrite.py` and `historical_backfill.py`
      legitimately want different behavior on it (a clean single-run
      exit vs. aborting an entire date-range run before processing any
      date).
    - The §16 "3 consecutive FAILED" outage-alert check stays in
      `daily_rewrite.py` — it is specific to daily_rewrite's own job
      history and cadence; §16 says nothing about historical_backfill
      raising the same alert, and it shouldn't (a backfill run failing
      says nothing about whether Resource 2 is currently reachable for
      *today*).
    - Any CLI/date-range/looping logic stays in `historical_backfill.py`.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date

from farmuncle_pipeline.core.batch_lifecycle import (
    JobAlreadyRunningError,  # re-exported so callers can catch it without a second import
    RawApiBatchHandle,
    complete_batch,
    complete_raw_batch,
    insert_failed_page,
    start_batch,
    start_raw_batch,
)
from farmuncle_pipeline.core.raw_dedup import upsert_raw_price_entries_batch
from farmuncle_pipeline.core.identity_client import IdentityClient
from farmuncle_pipeline.ingest_common import (
    ApiCallStatus,
    DEFAULT_HTTP_HEADERS,
    IngestionBatchStatus,
    PARSER_VERSION,
    Resource,
    STATES,
    Source,
    log_api_call,
)
from farmuncle_pipeline.core.price_writer import filter_rows_by_precedence, upsert_price_rows
from farmuncle_pipeline.core.record_processor import process_records
from farmuncle_pipeline.core.resource_client import fetch_page, parse_agmarknet_record

# Same fixed default as live_tick.py/daily_rewrite.py — neither
# government resource carries a per-row unit field.
_RAW_UNIT_DEFAULT = "kg"


@dataclass(frozen=True)
class PipelineResult:
    """Purpose: everything a caller needs to report on or react to one
    date's ingestion run, without re-deriving it from batch rows.

    fetch_seconds/identity_seconds/upsert_seconds (added 2026-07-14,
    Step 1 of the historical_backfill performance investigation): wall-
    clock time for each of the three phases below, so callers can print
    a per-date breakdown instead of guessing where time went. This is
    diagnostic instrumentation, not a permanent fix — see
    HANDOUT_backfill_performance.md for the investigation this
    supports.

    identity_stats (added 2026-07-15, Fix #1 follow-up): the
    `IdentityClient.stats()` snapshot for this date's run — see that
    method's docstring. Answers the next question after
    identity_seconds alone: not just "how long did identity resolution
    take" but "was it slow because the preload snapshot isn't matching,
    or is preload working fine and the cost is elsewhere"."""
    batch_id: str
    target_date: date
    final_status: IngestionBatchStatus
    rows_processed: int
    rows_failed: int
    rows_quarantined: int
    precedence_skipped: int
    states_with_any_success: int
    pages_fetched: int
    total_records: int
    error_summary: str | None
    fetch_seconds: float
    identity_seconds: float
    upsert_seconds: float
    identity_stats: dict[str, int]


def _fetch_all_resource_2_pages(
    *, supabase, batch_id: str, raw_batch_id: str, date_str: str, api_key: str, runtime, job_name: str
) -> tuple[list, bool, int, int]:
    """
    Purpose:
        Page through Resource 2, once per state in `STATES`, for a
        single date, accumulating every state's records. Unlike
        Resource 1 (queried by date alone), Resource 2's contract
        requires enumerating states (§9) — this function is the loop
        that does that, delegating the actual page-by-page fetch to
        the shared `resource_client.fetch_page`. Unchanged from Step
        15's `daily_rewrite.py` version except that `job_name` is now
        a parameter (Step 15 hardcoded it to `"daily_rewrite"`, since
        it was the only caller; now `"historical_backfill"` needs the
        exact same function tagging its own `api_call_logs` rows
        correctly).

        2026-07-14: also reconciles each state's actually-collected
        record count against the `total` field the government API
        itself reports per response — the same `QUALITY-001` ("Coverage
        mismatch", §7) check added to `live_tick.py`'s Resource 1 loop,
        after confirming Resource 1's nationwide stream was silently
        losing 46% of a day's records past the Elasticsearch
        `max_result_window` ceiling. Ported here defensively: Resource
        2 hits the same underlying government API/Elasticsearch
        backend, so the same per-state ceiling could in principle be
        hit here too, even though no concrete loss has yet been
        confirmed for Resource 2 specifically.
    Inputs:
        supabase / batch_id / raw_batch_id / date_str / api_key /
            runtime: as in Step 15.
        job_name: tags `api_call_logs.job_name` so calls from
            `daily_rewrite` vs `historical_backfill` remain
            distinguishable in that table.
    Outputs:
        4-tuple: (all records fetched across every state's successful
        pages, whether every state's pagination completed cleanly AND
        reconciled against the government's own reported total, number
        of states that produced at least one successful page, total
        pages attempted across all states).
    Failure modes:
        None raised directly — a page-level failure is recorded (see
        `insert_failed_page`) and a coverage mismatch is only logged
        (mirrors `live_tick.py`: the `failed_pages` retry model doesn't
        fit a coverage gap, since the missing records never fit in any
        single page to begin with); both are reflected in the returned
        counts rather than raised.
    """
    records: list = []
    all_states_complete = True
    states_with_any_success = 0
    total_pages = 0

    for state in STATES:
        offset = 0
        page_in_state = 0
        state_had_success = False
        state_record_count = 0
        reported_total = None

        while True:
            page_in_state += 1
            total_pages += 1
            params = {
                "api-key": api_key,
                "format": "json",
                "limit": runtime.page_size,
                "offset": offset,
                "filters[Arrival_Date]": date_str,
                "filters[State]": state,
            }
            result = fetch_page(
                url=runtime.api_base_resource_2,
                params=params,
                headers=DEFAULT_HTTP_HEADERS,
                timeout=runtime.api_timeout_seconds,
                max_retries=runtime.max_retries,
                retry_delay_seconds=runtime.retry_delay_seconds,
            )

            log_api_call(
                supabase,
                batch_id=batch_id,
                job_name=job_name,
                resource=Resource.RESOURCE_2,
                duration_ms=result.duration_ms,
                status=ApiCallStatus.SUCCESS if result.ok else ApiCallStatus.FAILURE,
                page=page_in_state,
                rows=len(result.records) if result.ok else None,
                error_code=None if result.ok else "INGEST-001",
            )

            if not result.ok:
                insert_failed_page(
                    supabase,
                    batch_id=batch_id,
                    resource=Resource.RESOURCE_2,
                    page=page_in_state,
                    error_code="INGEST-002",
                    error_message=(
                        f"[state={state}] "
                        f"{result.error or 'unknown error after exhausting retries'}"
                    ),
                )
                all_states_complete = False
                break  # move on to the next state; don't abandon the whole date

            if reported_total is None:
                reported_total = result.raw_response.get("total")

            # Batched raw-dedup write (2026-07-13 fix) — see
            # raw_dedup.upsert_raw_price_entries_batch's docstring.
            parsed_page = [
                parsed
                for parsed in (parse_agmarknet_record(rec) for rec in result.records)
                if parsed is not None
            ]
            upsert_raw_price_entries_batch(
                supabase,
                resource=Resource.RESOURCE_2.value,
                batch_id=raw_batch_id,
                parser_version=PARSER_VERSION,
                parsed_records=parsed_page,
            )
            records.extend(result.records)
            state_record_count += len(result.records)
            state_had_success = True

            if len(result.records) < runtime.page_size:
                break  # genuine end of pagination for this state
            offset += runtime.page_size

        if state_had_success:
            states_with_any_success += 1

        if reported_total is not None and state_record_count < reported_total:
            print(
                f"[resource2_pipeline] QUALITY-001 WARNING: {state} reported total="
                f"{reported_total} but only {state_record_count} record(s) collected "
                f"for {date_str} -- coverage gap, not a page failure."
            )
            all_states_complete = False

    return records, all_states_complete, states_with_any_success, total_pages


def ingest_resource2_for_date(ctx, *, target_date: date, job_name: str) -> PipelineResult:
    """
    Purpose:
        Run the complete Resource 2 ingestion pipeline for exactly one
        calendar date: acquire the §12 concurrency guard (keyed on
        `job_name`), fetch every state's data for `target_date`,
        resolve identities, score quality, enforce §8 precedence,
        upsert, and close out both batch rows.
    Inputs:
        ctx: a `StartupContext` from `validate_startup()`.
        target_date: the calendar date to fetch Resource 2 data for.
        job_name: tags both batch rows and `api_call_logs` — callers
            use a distinct value per script (`"daily_rewrite"` vs
            `"historical_backfill"`) so the §12 concurrency guard and
            batch history queries stay correctly scoped per caller.
    Outputs:
        `PipelineResult`. Note this function does NOT return early on
        `JobAlreadyRunningError` — see Failure modes.
    Failure modes:
        Raises `batch_lifecycle.JobAlreadyRunningError` if a batch with
        this `job_name` is already RUNNING — deliberately NOT caught
        here (see module docstring: callers want different behavior on
        this). Any other exception is caught internally, both batch
        rows are marked FAILED with the exception's message as
        `error_summary`, and the exception is re-raised after that
        cleanup — mirrors `live_tick.py`/Step 15's `daily_rewrite.py`.
    """
    supabase = ctx.supabase
    runtime = ctx.app_config.runtime
    date_str = target_date.strftime("%d/%m/%Y")

    batch = start_batch(
        supabase,
        job_name=job_name,
        resource=Resource.RESOURCE_2,
        date_range_start=target_date,
        date_range_end=target_date,
    )

    raw_batch: RawApiBatchHandle | None = None
    rows_processed = 0
    rows_failed = 0
    rows_quarantined = 0

    try:
        raw_batch = start_raw_batch(
            supabase,
            job_name=job_name,
            resource=Resource.RESOURCE_2,
            date_range_start=target_date,
            date_range_end=target_date,
        )

        identity = IdentityClient(supabase)
        identity.preload()
        unit = identity.resolve_unit(_RAW_UNIT_DEFAULT)

        _fetch_start = time.monotonic()
        records, all_states_complete, states_with_any_success, pages_fetched = (
            _fetch_all_resource_2_pages(
                supabase=supabase,
                batch_id=batch.id,
                raw_batch_id=raw_batch.id,
                date_str=date_str,
                api_key=ctx.secrets.data_gov_api_key,
                runtime=runtime,
                job_name=job_name,
            )
        )
        fetch_seconds = time.monotonic() - _fetch_start

        # A total outage (zero states yielded even one successful page)
        # is worse than "some states/pages failed" — see daily_rewrite's
        # §16 handling, which relies on this distinction.
        total_outage = states_with_any_success == 0

        _identity_start = time.monotonic()
        result = process_records(
            records,
            identity=identity,
            unit=unit,
            source=Source.RESOURCE_2,
            batch_id=batch.id,
            raw_api_batch_id=raw_batch.id,
            job_name=job_name,
        )
        identity_seconds = time.monotonic() - _identity_start
        price_rows = result.price_rows
        rows_failed += result.rows_failed
        rows_processed += len(price_rows)

        _upsert_start = time.monotonic()
        price_rows, precedence_skipped = filter_rows_by_precedence(
            supabase, price_rows, Source.RESOURCE_2
        )
        upsert_result = upsert_price_rows(
            supabase,
            price_rows,
            runtime.batch_size,
            batch_id=batch.id,
            resource=Resource.RESOURCE_2,
        )
        rows_quarantined = upsert_result.rows_quarantined
        upsert_seconds = time.monotonic() - _upsert_start

        if total_outage:
            final_status = IngestionBatchStatus.FAILED
        elif (not all_states_complete) or rows_failed > 0 or rows_quarantined > 0:
            final_status = IngestionBatchStatus.PARTIAL
        else:
            final_status = IngestionBatchStatus.SUCCESS

        error_summary = None
        if total_outage:
            error_summary = (
                "every state failed to yield even one successful page — "
                "Resource 2 appears unreachable"
            )
        elif not all_states_complete:
            error_summary = (
                "one or more states' pages failed after exhausting retries, or a "
                "state's collected total didn't reconcile against the government's "
                "reported total (QUALITY-001) — see failed_pages and run logs"
            )
        elif rows_failed:
            error_summary = f"{rows_failed} row(s) skipped — malformed data or identity resolution failure"
        elif rows_quarantined:
            error_summary = (
                f"{rows_quarantined} row(s) violated a database constraint at upsert "
                f"(e.g. chk_prices_min_max) and were quarantined — see data_quality_issues"
            )
        elif precedence_skipped:
            error_summary = (
                f"{precedence_skipped} row(s) skipped — a higher-precedence "
                f"(manual) row already exists for that business key (§8, not a failure)"
            )

        complete_raw_batch(
            supabase,
            batch_id=raw_batch.id,
            status=final_status,
            total_pages=pages_fetched,
            total_records=len(records),
            error_summary=error_summary,
        )
        complete_batch(
            supabase,
            batch_id=batch.id,
            status=final_status,
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            error_summary=error_summary,
        )

        return PipelineResult(
            batch_id=batch.id,
            target_date=target_date,
            final_status=final_status,
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            rows_quarantined=rows_quarantined,
            precedence_skipped=precedence_skipped,
            states_with_any_success=states_with_any_success,
            pages_fetched=pages_fetched,
            total_records=len(records),
            error_summary=error_summary,
            fetch_seconds=fetch_seconds,
            identity_seconds=identity_seconds,
            upsert_seconds=upsert_seconds,
            identity_stats=identity.stats(),
        )

    except Exception as exc:
        error_summary = str(exc)[:500]
        complete_batch(
            supabase,
            batch_id=batch.id,
            status=IngestionBatchStatus.FAILED,
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            error_summary=error_summary,
        )
        if raw_batch is not None:
            complete_raw_batch(
                supabase,
                batch_id=raw_batch.id,
                status=IngestionBatchStatus.FAILED,
                error_summary=error_summary,
            )
        raise