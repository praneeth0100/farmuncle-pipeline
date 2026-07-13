"""
FarmUncle v2 — live_tick.py
Phase C, Step 14.

Purpose (module-level):
    "14. live_tick.py — Resource 1, today-only, every 3h" (Master Build
    Specification §21, Phase C). Fetches TODAY's data from the
    government's live feed (Resource 1, §9: best-effort, today-only,
    intraday-updated) and ingests it end-to-end: raw storage, identity
    resolution, quality scoring, and an upsert into `mandi_daily_prices`
    tagged `source=resource_1`.

    This script is a single run, meant to be invoked by a scheduler
    every 3 hours (the "every 3h" cadence itself — the GitHub Actions
    cron config — is Phase D's job, Step 18; this script has no
    internal scheduling and does not loop).

Precedence note (§8):
    Resource 1 rows this script writes are NOT the final word for a
    given day — Resource 2 (via `daily_rewrite.py`, Step 15) is
    authoritative and evening-finalized, ranking above Resource 1 in
    §8's precedence order. Nothing in this script enforces that
    precedence at write time (both resources' rows for the same
    business key legitimately coexist as distinct
    `(mandi, crop, variety, date, ...)`-keyed rows here since `source`
    is not part of the uniqueness constraint) — reconciling/superseding
    same-day rows across resources is `daily_rewrite`'s job description
    ("honors §8 precedence"), not this script's.

What this script deliberately does NOT do (see reasoning inline / in
imported modules):
    - Call `refresh_price_cache` — that RPC and its `price_cache` table
      do not exist yet in the live schema (config_validator.py's
      "Known deviations" #1). Calling it would just fail. Cache
      refresh will be wired in once that table exists.
    - Backfill historical dates, or accept a date/mode CLI argument —
      that's `historical_backfill.py` (Step 16) and `daily_rewrite.py`
      (Step 15)'s job respectively. `sync_prices.py`/`sync_prices_v2.py`
      (v1 reference) both folded backfill+quick+daily modes into one
      script; v2 deliberately splits that back out along the Phase C
      step boundaries the spec already defines, rather than carrying
      the v1 script's multi-mode design forward.
    - Retry a failed page inline — a page that fails after exhausting
      retries is recorded in `failed_pages` and this run moves on
      (marking itself PARTIAL); retrying it is `retry_failed_pages.py`
      (Step 17)'s job, per invariant 6 ("every failed API page is
      persisted in a table, never a local file") and §5's module
      structure (retry is its own script, not folded into live_tick).

Provenance — what was reused from the v1 reference scripts and why:
    - Resource 1's field names (`commodity`, `market`, `state`,
      `district`, `variety`, `arrival_date`, `modal_price`,
      `min_price`, `max_price`) and its `DD/MM/YYYY` date format: these
      are facts about the government API's response shape, not design
      decisions — reused as-is from `sync_prices.py`/`sync_prices_v2.py`.
    - The HTTP headers in `config.DEFAULT_HTTP_HEADERS` (User-Agent
      etc.): inert, non-secret constants, already carried into Step 13.
    - Passing request parameters via `requests`' `params=` dict (not
      manual string concatenation): this is what makes §9's "special-
      character name issues (e.g. `&` in `F&V`)" a non-issue — `requests`
      URL-encodes values automatically. Both v1 scripts already did
      this correctly; v2 keeps the same approach.
    What was NOT reused: v1's `fetch_page` returning `None` on failure
    (conflating "failed after retries" with "genuine end of pagination"
    — exactly the bug §9 calls out and requires fixing here via an
    explicit `ok` flag, see `PageFetchResult`); v1's local
    `sync_failures.json` file for tracking failed pages (replaced by
    the `failed_pages` table, invariant 6); v1's hardcoded
    `PAGE_SIZE`/`CHUNK_SIZE`/retry constants (replaced by
    `system_config` via `ctx.app_config.runtime`); v1's `int(float(...))`
    price truncation (prices are stored as `numeric` in the live
    schema — truncating to `int` would silently lose precision, so
    this script keeps them as `float`); v1's field name `date` for the
    price-date column (the live schema calls it `price_date`).
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

_IST = timezone(timedelta(hours=5, minutes=30))

from farmuncle_pipeline.core.batch_lifecycle import (
    JobAlreadyRunningError,
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
    Source,
    log_api_call,
    time_api_call,
    validate_startup,
)
from farmuncle_pipeline.core.price_writer import filter_rows_by_precedence, upsert_price_rows
from farmuncle_pipeline.core.record_processor import process_records
from farmuncle_pipeline.core.resource_client import PageFetchResult, fetch_page, parse_agmarknet_record
JOB_NAME = "live_tick"

# Government Resource 1/2 responses carry no per-row unit field (see
# both reference scripts — `p_unit="kg"` was hardcoded there too).
# Routed through `normalize_unit` (see `IdentityClient.resolve_unit`)
# rather than used raw, per invariant 3.
_RAW_UNIT_DEFAULT = "kg"


# =============================================================================
# Resource 1 page fetching — §9's "ok flag" fix
#
# `PageFetchResult`/`fetch_page` now live in `resource_client.py` (Step
# 15 extracted them there once `daily_rewrite.py` became a second real
# caller needing identical retry/backoff/explicit-ok-flag behavior —
# see that module's docstring). Byte-for-byte unchanged from the
# version verified in Step 14's production audit; imported below, not
# redefined.
# =============================================================================


def _fetch_all_resource_1_pages(
    *, supabase, batch_id: str, raw_batch_id: str, date_str: str, api_key: str, runtime
) -> tuple[list, bool, int]:
    """
    Purpose:
        Page through Resource 1 for a single date until pagination
        genuinely ends or a page fails after exhausting retries.
        Writes one `api_call_logs` row and, on success, one
        `raw_api_records` row per page fetched (invariant 1: raw
        payloads are immutable and written as-is).
    Inputs:
        supabase: an already-constructed client.
        batch_id: `ingestion_batches.id` for `api_call_logs`/
            `failed_pages` correlation.
        raw_batch_id: `raw_api_batches.id` for `raw_api_records`
            correlation.
        date_str: today's date as `DD/MM/YYYY` (Resource 1's expected
            `filters[arrival_date]` format).
        api_key: `ctx.secrets.data_gov_api_key`.
        runtime: `ctx.app_config.runtime`.
    Outputs:
        3-tuple: (all records fetched across all successful pages,
        whether pagination completed cleanly, number of pages
        attempted).
    Failure modes:
        None raised directly — a page-level failure is recorded (see
        `insert_failed_page`) and reflected in the returned `bool`
        rather than raised, so a partial day's data is still ingested
        instead of being discarded wholesale.
    """
    records: list = []
    offset = 0
    page_number = 0
    complete = True

    while True:
        page_number += 1
        params = {
            "api-key": api_key,
            "format": "json",
            "limit": runtime.page_size,
            "offset": offset,
            "filters[arrival_date]": date_str,
        }
        result = fetch_page(
            url=runtime.api_base_resource_1,
            params=params,
            headers=DEFAULT_HTTP_HEADERS,
            timeout=runtime.api_timeout_seconds,
            max_retries=runtime.max_retries,
            retry_delay_seconds=runtime.retry_delay_seconds,
        )

        log_api_call(
            supabase,
            batch_id=batch_id,
            job_name=JOB_NAME,
            resource=Resource.RESOURCE_1,
            duration_ms=result.duration_ms,
            status=ApiCallStatus.SUCCESS if result.ok else ApiCallStatus.FAILURE,
            page=page_number,
            rows=len(result.records) if result.ok else None,
            # INGEST-001 (API timeout) is the closest §7 code for an
            # api_call_logs-level failure, whatever its exact cause
            # (timeout or connection error) — §7 defines no separate
            # "connection error" code.
            error_code=None if result.ok else "INGEST-001",
        )

        if not result.ok:
            # This is the §9 fix in effect: an exhausted-retries page
            # is recorded explicitly (INGEST-002: pagination failure)
            # rather than silently treated as "pagination ended here".
            insert_failed_page(
                supabase,
                batch_id=batch_id,
                resource=Resource.RESOURCE_1,
                page=page_number,
                error_code="INGEST-002",
                error_message=result.error or "unknown error after exhausting retries",
            )
            complete = False
            break

        # Batched raw-dedup write (2026-07-13 fix): one RPC round-trip
        # for the whole page instead of one per record — see
        # raw_dedup.upsert_raw_price_entries_batch's docstring for why
        # this replaced the original per-record loop.
        parsed_page = [
            parsed
            for parsed in (parse_agmarknet_record(rec) for rec in result.records)
            if parsed is not None
        ]
        upsert_raw_price_entries_batch(
            supabase,
            resource=Resource.RESOURCE_1.value,
            batch_id=raw_batch_id,
            parser_version=PARSER_VERSION,
            parsed_records=parsed_page,
        )
        records.extend(result.records)

        if len(result.records) < runtime.page_size:
            break  # genuine end of pagination — a short page that DID succeed
        offset += runtime.page_size
        time.sleep(1)

    return records, complete, page_number


# =============================================================================
# Parsing
#
# `parse_resource_1_record` now lives in `resource_client.py` as
# `parse_agmarknet_record` (Step 15 extracted it once `daily_rewrite.py`
# became a second caller needing byte-for-byte identical parsing for
# Resource 2 — see that module's docstring). Imported below under its
# original name so nothing else in this file needed to change.
# =============================================================================


# =============================================================================
# Upsert
#
# `_upsert_price_rows` now lives in `price_writer.py` as the public
# `upsert_price_rows`, alongside the new `filter_rows_by_precedence`
# that Step 15 requires (§8) — see that module's docstring for why
# both belong together and why this script now calls the precedence
# filter before upserting (it did not need to at Step 14, since
# Resource 2 data did not yet exist).
# =============================================================================
# =============================================================================

def run_live_tick(ctx) -> None:
    """
    Purpose:
        Execute one full live_tick run: acquire the §12 concurrency
        guard, fetch today's Resource 1 data, resolve identities, score
        quality, upsert prices, and close out both batch rows — always,
        even on failure (a batch left RUNNING would permanently block
        every future run of this job, see `batch_lifecycle.py`).
    Inputs:
        ctx: a `StartupContext` from `validate_startup()`.
    Outputs:
        None.
    Failure modes:
        Re-raises any exception after marking both batch rows FAILED,
        so a GitHub Actions run surfaces as a failed step rather than
        a silently swallowed error. `JobAlreadyRunningError` is instead
        caught and treated as a clean, expected early exit (see
        `batch_lifecycle.py`'s module docstring).
    """
    supabase = ctx.supabase
    runtime = ctx.app_config.runtime
    today = datetime.now(_IST).date()
    date_str = today.strftime("%d/%m/%Y")

    try:
        batch = start_batch(
            supabase,
            job_name=JOB_NAME,
            resource=Resource.RESOURCE_1,
            date_range_start=today,
            date_range_end=today,
        )
    except JobAlreadyRunningError as exc:
        print(f"[live_tick] {exc} — exiting cleanly.")
        return

    raw_batch: RawApiBatchHandle | None = None
    rows_processed = 0
    rows_failed = 0

    try:
        raw_batch = start_raw_batch(
            supabase,
            job_name=JOB_NAME,
            resource=Resource.RESOURCE_1,
            date_range_start=today,
            date_range_end=today,
        )

        identity = IdentityClient(supabase)
        unit = identity.resolve_unit(_RAW_UNIT_DEFAULT)

        records, pagination_complete, pages_fetched = _fetch_all_resource_1_pages(
            supabase=supabase,
            batch_id=batch.id,
            raw_batch_id=raw_batch.id,
            date_str=date_str,
            api_key=ctx.secrets.data_gov_api_key,
            runtime=runtime,
        )

        result = process_records(
            records,
            identity=identity,
            unit=unit,
            source=Source.RESOURCE_1,
            batch_id=batch.id,
            raw_api_batch_id=raw_batch.id,
            job_name=JOB_NAME,
        )
        price_rows = result.price_rows
        rows_failed += result.rows_failed
        rows_processed += len(price_rows)

        # §8: a Resource 1 row must never overwrite an existing row from
        # a higher-precedence source (resource_2 or manual) for the same
        # business key. This did not matter at Step 14 (Resource 2 data
        # did not exist yet) but does now that daily_rewrite.py (Step 15)
        # writes real resource_2 rows — see price_writer.py's docstring.
        price_rows, precedence_skipped = filter_rows_by_precedence(
            supabase, price_rows, Source.RESOURCE_1
        )
        upsert_price_rows(supabase, price_rows, runtime.batch_size)

        is_partial = (not pagination_complete) or rows_failed > 0
        final_status = IngestionBatchStatus.PARTIAL if is_partial else IngestionBatchStatus.SUCCESS

        # precedence_skipped is deliberately NOT part of is_partial — a
        # row losing to an existing higher-precedence (resource_2/manual)
        # row is §8 working as designed, not a failure. Still surfaced
        # below for visibility since it's an unusual/notable event.
        error_summary = None
        if not pagination_complete:
            error_summary = "one or more pages failed after exhausting retries — see failed_pages"
        elif rows_failed:
            error_summary = f"{rows_failed} row(s) skipped — malformed data or identity resolution failure"
        elif precedence_skipped:
            error_summary = (
                f"{precedence_skipped} row(s) skipped — a higher-precedence "
                f"(resource_2/manual) row already exists for that business key (§8, not a failure)"
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

        print(
            f"[live_tick] {final_status.value} — {len(price_rows)} row(s) upserted, "
            f"{rows_failed} skipped (parse/identity), {precedence_skipped} skipped (§8 precedence), "
            f"{pages_fetched} page(s) fetched"
            + ("" if pagination_complete else " (pagination incomplete — see failed_pages)")
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


def main() -> None:
    ctx = validate_startup()
    run_live_tick(ctx)


if __name__ == "__main__":
    main()
