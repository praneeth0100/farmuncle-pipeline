"""
FarmUncle v2 — retry_failed_pages.py
Phase C, Step 17 (final step of Phase C).

Purpose (module-level):
    "17. retry_failed_pages.py" (Master Build Specification §21,
    Phase C). Per §6.6's data-ownership table, `failed_pages` rows are
    written by the ingestion scripts on failure and resolved by this
    script on success — this is that "on success" half. For every
    `PENDING` row, reconstructs the exact original request (resource,
    date, page offset, and — for Resource 2 — state), retries it, and
    on success runs that page's records through the FULL pipeline
    (raw storage, identity resolution, quality scoring, §8 precedence,
    upsert) rather than only patching the audit trail — a page that
    failed originally never made it into `mandi_daily_prices` at all,
    so recovering only `raw_api_records` while leaving that gap would
    be an incomplete fix.

Reconstructing a failed page's original request:
    `failed_pages` stores `resource`, `page` (an integer), and
    `error_code`/`error_message` — but NOT the date or (for Resource 2)
    the state that page belonged to, since neither column exists in
    the frozen Phase B schema. Both are recovered rather than stored
    redundantly:
      - Date: every batch this pipeline creates is single-day
        (`date_range_start == date_range_end` — see
        `batch_lifecycle.start_batch`'s callers), so
        `failed_pages.batch_id → ingestion_batches.date_range_start`
        gives the exact date.
      - State (Resource 2 only): `resource2_pipeline.py` deliberately
        prefixes every Resource-2 `failed_pages.error_message` with
        `"[state=XXX] ..."` for exactly this purpose (see that
        module's docstring) — parsed back out here.
      - Offset: `resource_client.fetch_page`'s pagination always uses
        `offset = (page_number - 1) * page_size`, 1-indexed — the same
        arithmetic run in reverse.

    KNOWN LIMITATION: offset reconstruction uses the CURRENT
    `system_config.PAGE_SIZE`. If `PAGE_SIZE` changed between the
    original failure and this retry, the reconstructed offset would be
    wrong. `PAGE_SIZE` is not expected to change often (§17's
    system_config is deliberately static, operational configuration),
    so this is accepted as a documented risk rather than solved by
    adding a `page_size`-at-failure-time column — a real fix would be
    a schema change to `failed_pages` itself, which is out of scope
    for a Phase C ingestion script to decide unilaterally (would need
    an ADR per §19).

Why a failed retry leaves the row PENDING rather than escalating it:
    `failed_pages.status` is only `PENDING`/`RESOLVED` (no
    `PERMANENTLY_FAILED` — see the live schema's `failed_pages_status_check`).
    There is also no retry-count column. So a page that fails again on
    retry simply stays `PENDING` for the next scheduled invocation of
    this script — the "backoff" between attempts is the scheduling
    interval between runs (Phase D's job), not something tracked or
    enforced here. An operator watching `quality_alerts`/the
    `Retry Queue (depth)` KPI (§22) would notice a page that never
    clears after many scheduled retries and can investigate manually;
    this script does not itself escalate or alert on that pattern.

What this script deliberately does NOT do:
    - Raise a `quality_alerts` row for pages that keep failing — no
      such alert is specified anywhere in the spec for this script
      (§16's outage alert is `daily_rewrite`-specific — see that
      script's docstring).
    - Retry pages from `historical_backfill` any differently than
      `live_tick`/`daily_rewrite` ones — a failed page is a failed
      page regardless of which script originally recorded it; this
      script only cares about `resource`/`batch_id`/`page`.
"""

from __future__ import annotations

from datetime import datetime

from farmuncle_pipeline.core.batch_lifecycle import (
    JobAlreadyRunningError,
    RawApiBatchHandle,
    complete_batch,
    complete_raw_batch,
    insert_raw_api_record,
    start_batch,
    start_raw_batch,
)
from farmuncle_pipeline.core.identity_client import IdentityClient
from farmuncle_pipeline.ingest_common import (
    ApiCallStatus,
    ConfigError,
    DEFAULT_HTTP_HEADERS,
    FailedPageStatus,
    IngestionBatchStatus,
    PARSER_VERSION,
    Resource,
    Source,
    log_api_call,
    utcnow_iso,
    validate_startup,
)
from farmuncle_pipeline.core.price_writer import filter_rows_by_precedence, upsert_price_rows
from farmuncle_pipeline.core.record_processor import process_records
from farmuncle_pipeline.core.resource_client import fetch_page

JOB_NAME = "retry_failed_pages"

# Same fixed default as the other ingestion scripts — neither
# government resource carries a per-row unit field.
_RAW_UNIT_DEFAULT = "kg"

_RESOURCE_TO_SOURCE = {
    Resource.RESOURCE_1: Source.RESOURCE_1,
    Resource.RESOURCE_2: Source.RESOURCE_2,
}


def _parse_state_from_error_message(error_message: str | None) -> str | None:
    """
    Purpose:
        Recover the state a Resource 2 failed page belonged to from
        the `"[state=XXX] ..."` prefix `resource2_pipeline.py`
        deliberately writes into `error_message` for exactly this
        purpose (see module docstring).
    Inputs:
        error_message: `failed_pages.error_message`, possibly `None`.
    Outputs:
        The state string, or `None` if the prefix is missing/malformed
        (e.g. a legacy or manually-inserted row) — callers treat `None`
        as "cannot retry this row" and skip it rather than guessing.
    Failure modes:
        None raised.
    """
    if not error_message or not error_message.startswith("[state="):
        return None
    end = error_message.find("]")
    if end == -1:
        return None
    return error_message[len("[state=") : end] or None


def _fetch_pending_failed_pages(supabase) -> list[dict]:
    """Purpose: every PENDING failed_pages row, oldest first (a simple,
    defensible default for a retry queue — see §16's "oldest-first"
    recovery language, applied here at the page level)."""
    try:
        response = (
            supabase.table("failed_pages")
            .select("id,batch_id,resource,page,error_code,error_message,created_at")
            .eq("status", FailedPageStatus.PENDING.value)
            .order("created_at")
            .execute()
        )
    except Exception as exc:
        raise ConfigError(f"Failed to fetch PENDING failed_pages rows: {exc}") from exc
    return response.data or []


def _fetch_batch_target_date(supabase, batch_id: str):
    """Purpose: recover the single calendar date a failed page's parent
    batch targeted (see module docstring's date-recovery note). Returns
    `None` if the batch row can't be found or has no date range set —
    callers skip the row rather than guessing a date."""
    try:
        response = (
            supabase.table("ingestion_batches")
            .select("date_range_start")
            .eq("id", batch_id)
            .limit(1)
            .execute()
        )
    except Exception as exc:
        raise ConfigError(f"Failed to fetch batch {batch_id!r} for date lookup: {exc}") from exc

    rows = response.data or []
    if not rows or not rows[0].get("date_range_start"):
        return None
    return datetime.strptime(rows[0]["date_range_start"], "%Y-%m-%d").date()


def _mark_resolved(supabase, failed_page_id: int) -> None:
    try:
        supabase.table("failed_pages").update(
            {"status": FailedPageStatus.RESOLVED.value, "resolved_at": utcnow_iso()}
        ).eq("id", failed_page_id).execute()
    except Exception as exc:
        raise ConfigError(f"Failed to mark failed_pages id={failed_page_id} RESOLVED: {exc}") from exc


def run_retry_failed_pages(ctx) -> None:
    """
    Purpose:
        Attempt every currently-PENDING failed page once: reconstruct
        its request, retry it, and on success run it through the full
        pipeline (raw storage, identity, quality, §8 precedence,
        upsert) and mark it RESOLVED. A page that fails again simply
        stays PENDING for a future invocation (see module docstring).
    Inputs:
        ctx: a `StartupContext` from `validate_startup()`.
    Outputs:
        None.
    Failure modes:
        `JobAlreadyRunningError` is caught and treated as a clean early
        exit. Any other exception marks the overall batch (and any
        per-resource raw batches already opened) FAILED before
        re-raising — mirrors every other Phase C script.
    """
    supabase = ctx.supabase
    runtime = ctx.app_config.runtime

    try:
        batch = start_batch(supabase, job_name=JOB_NAME, resource=None)
    except JobAlreadyRunningError as exc:
        print(f"[retry_failed_pages] {exc} — exiting cleanly.")
        return

    raw_batches: dict[Resource, RawApiBatchHandle] = {}
    raw_batch_stats: dict[Resource, dict] = {}
    resolved = still_pending = skipped = rows_processed = rows_failed = 0

    try:
        identity = IdentityClient(supabase)
        unit = identity.resolve_unit(_RAW_UNIT_DEFAULT)

        for fp in _fetch_pending_failed_pages(supabase):
            target_date = _fetch_batch_target_date(supabase, fp["batch_id"])
            if target_date is None:
                print(
                    f"[retry_failed_pages] failed_pages id={fp['id']}: parent batch/date "
                    f"not found, skipping (needs manual investigation)."
                )
                skipped += 1
                continue

            resource_enum = Resource(fp["resource"])
            state = None
            if resource_enum == Resource.RESOURCE_2:
                state = _parse_state_from_error_message(fp.get("error_message"))
                if state is None:
                    print(
                        f"[retry_failed_pages] failed_pages id={fp['id']}: could not recover "
                        f"state from error_message, skipping (needs manual investigation)."
                    )
                    skipped += 1
                    continue

            date_str = target_date.strftime("%d/%m/%Y")
            offset = (fp["page"] - 1) * runtime.page_size
            url = (
                runtime.api_base_resource_1
                if resource_enum == Resource.RESOURCE_1
                else runtime.api_base_resource_2
            )
            params = {
                "api-key": ctx.secrets.data_gov_api_key,
                "format": "json",
                "limit": runtime.page_size,
                "offset": offset,
                "filters[arrival_date]": date_str,
            }
            if state is not None:
                params["filters[state]"] = state

            result = fetch_page(
                url=url,
                params=params,
                headers=DEFAULT_HTTP_HEADERS,
                timeout=runtime.api_timeout_seconds,
                max_retries=runtime.max_retries,
                retry_delay_seconds=runtime.retry_delay_seconds,
            )

            log_api_call(
                supabase,
                batch_id=batch.id,
                job_name=JOB_NAME,
                resource=resource_enum,
                duration_ms=result.duration_ms,
                status=ApiCallStatus.SUCCESS if result.ok else ApiCallStatus.FAILURE,
                page=fp["page"],
                rows=len(result.records) if result.ok else None,
                error_code=None if result.ok else "INGEST-001",
            )

            if not result.ok:
                print(
                    f"[retry_failed_pages] failed_pages id={fp['id']}: retry failed again "
                    f"({result.error}) — left PENDING for a future attempt."
                )
                still_pending += 1
                continue

            raw_batch = raw_batches.get(resource_enum)
            if raw_batch is None:
                raw_batch = start_raw_batch(supabase, job_name=JOB_NAME, resource=resource_enum)
                raw_batches[resource_enum] = raw_batch
                raw_batch_stats[resource_enum] = {"pages": 0, "records": 0}

            raw_payload = {"state": state, "response": result.raw_response} if state else result.raw_response
            insert_raw_api_record(
                supabase,
                raw_batch_id=raw_batch.id,
                resource=resource_enum,
                page_number=fp["page"],
                raw_payload=raw_payload,
                parser_version=PARSER_VERSION,
            )
            raw_batch_stats[resource_enum]["pages"] += 1
            raw_batch_stats[resource_enum]["records"] += len(result.records)

            source = _RESOURCE_TO_SOURCE[resource_enum]
            proc = process_records(
                result.records,
                identity=identity,
                unit=unit,
                source=source,
                batch_id=batch.id,
                raw_api_batch_id=raw_batch.id,
                job_name=JOB_NAME,
            )
            kept_rows, _precedence_skipped = filter_rows_by_precedence(supabase, proc.price_rows, source)
            upsert_price_rows(supabase, kept_rows, runtime.batch_size)

            rows_processed += len(kept_rows)
            rows_failed += proc.rows_failed

            _mark_resolved(supabase, fp["id"])
            resolved += 1
            print(f"[retry_failed_pages] failed_pages id={fp['id']}: RESOLVED ({len(kept_rows)} row(s)).")

        for resource_enum, raw_batch in raw_batches.items():
            stats = raw_batch_stats[resource_enum]
            complete_raw_batch(
                supabase,
                batch_id=raw_batch.id,
                status=IngestionBatchStatus.SUCCESS,
                total_pages=stats["pages"],
                total_records=stats["records"],
            )

        final_status = (
            IngestionBatchStatus.SUCCESS
            if still_pending == 0 and skipped == 0
            else IngestionBatchStatus.PARTIAL
        )
        error_summary = None
        if still_pending or skipped:
            error_summary = f"{still_pending} page(s) failed again, {skipped} page(s) skipped (unrecoverable context)"

        complete_batch(
            supabase,
            batch_id=batch.id,
            status=final_status,
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            error_summary=error_summary,
        )

        print(
            f"[retry_failed_pages] {final_status.value} — {resolved} page(s) resolved, "
            f"{still_pending} still pending, {skipped} skipped, {rows_processed} row(s) upserted, "
            f"{rows_failed} row(s) skipped (parse/identity)."
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
        for raw_batch in raw_batches.values():
            complete_raw_batch(
                supabase,
                batch_id=raw_batch.id,
                status=IngestionBatchStatus.FAILED,
                error_summary=error_summary,
            )
        raise


def main() -> None:
    ctx = validate_startup()
    run_retry_failed_pages(ctx)


if __name__ == "__main__":
    main()
