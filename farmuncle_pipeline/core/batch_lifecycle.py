"""
FarmUncle v2 — batch_lifecycle.py
Phase C, Step 14 (introduced here, not scoped to Step 13, because it is
genuinely shared: daily_rewrite.py / historical_backfill.py /
retry_failed_pages.py — Steps 15-17 — all need the identical
raw_api_batches + ingestion_batches lifecycle. Writing it once now,
rather than duplicating it inside live_tick.py, is what Never-Do
Rules §2 ("no business logic duplicated across scripts") requires.

Purpose (module-level):
    Owns the two batch-tracking concepts the Master Build Specification
    keeps deliberately distinct (§6.1):
      - `raw_api_batches` / `raw_api_records` — the bronze layer.
        Group verbatim government API pages under one immutable-record
        batch. A row here answers "what did the API literally say?"
      - `ingestion_batches` — the ops layer. Every other table
        (api_call_logs, failed_pages, mandi_daily_prices, audit_events,
        entity_history, coverage_reports, quality_alerts) correlates
        back to THIS table, not raw_api_batches. A row here answers
        "what did this pipeline run do?"
    `mandi_daily_prices` carries lineage to both, independently
    (`batch_id` -> ingestion_batches, `raw_api_batch_id` ->
    raw_api_batches) — per spec invariant 8, this module keeps that
    distinction intact rather than collapsing them into one id.

Concurrency guard (Master Build Specification §12 / Never-Do Rules §2
"never let two instances of the same job run concurrently without the
advisory lock") — documented deviation from the literal word
"advisory lock":
    A genuine Postgres session-scoped advisory lock
    (`pg_advisory_lock`) does not hold reliably across this stack.
    Every call this codebase makes goes through PostgREST over
    Supabase's pooled connections (transaction-mode pooling) — there
    is no guarantee two HTTP requests from the same script share the
    same underlying Postgres session, so a lock "acquired" in one RPC
    call is not dependably still held by the time the job's real work
    runs, and is released the instant that HTTP request completes.
    Building on that primitive here would produce a lock that looks
    correct in code and does nothing in production.

    Instead, `start_batch` enforces the actual invariant §12 wants —
    at most one RUNNING row per job_name — directly and atomically at
    the database level, via a partial unique index applied alongside
    this step:
        CREATE UNIQUE INDEX uq_ingestion_batches_running_job
            ON ingestion_batches (job_name) WHERE status = 'RUNNING';
    A second concurrent `start_batch` call for the same job_name gets
    a unique-violation from Postgres on INSERT, which this module
    surfaces as `JobAlreadyRunningError`. This is mutual exclusion
    that is actually durable in a stateless-connection architecture,
    rather than one that only appears to be.

Explicitly out of scope for this file:
    - Government API fetching, parsing, or retry/backoff around it
      (the calling script's job — it knows the resource-specific
      shape; this module only tracks batches)
    - Identity resolution (find_or_create_* calls)
    - `api_call_logs` writes (see `logging_utils.log_api_call`)
    - `failed_pages` writes (the calling script has the page/resource
      context this module doesn't hold)
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING

from farmuncle_pipeline.config import ConfigError, IngestionBatchStatus, Resource

if TYPE_CHECKING:
    from supabase import Client


class JobAlreadyRunningError(Exception):
    """
    Raised by `start_batch` when a RUNNING `ingestion_batches` row
    already exists for the requested `job_name` — see module
    docstring's concurrency-guard section. Callers (e.g. `live_tick.py`
    `main()`) should catch this and exit 0 quietly: this is an
    expected, non-error outcome of the §12 guard doing its job, not a
    pipeline failure.
    """


# Any single ingestion run (one live_tick invocation, one
# historical_backfill date) was originally assumed to take 1-2 minutes
# based on live_tick's speed. Confirmed 2026-07-14 that assumption was
# wrong for historical_backfill specifically: a single Resource 2 date
# with heavy historical volume took ~15-20 minutes across all 33
# states (many multi-page states, 500 rows/page). A 30-minute
# threshold would risk auto-reaping a genuinely slow-but-real run —
# exactly the failure mode this mechanism exists to avoid causing.
# 90 minutes gives real runs (even unusually slow ones, retries
# included) a wide safety margin, while still being far below the 6+
# hours an actually-orphaned lock sat for in the incident that
# motivated this fix. Tune upward further if a real date ever
# legitimately approaches this.
_STALE_LOCK_MINUTES = 90


def _reap_stale_running_lock(client: "Client", *, job_name: str) -> None:
    """
    Purpose:
        Before attempting to acquire the §12 concurrency lock, check
        whether an existing RUNNING row for this `job_name` is old
        enough to be confidently treated as orphaned (see
        `_STALE_LOCK_MINUTES` above) rather than a genuine in-progress
        run. If so, close it out as FAILED with a clear, greppable
        note, so `start_batch`'s insert can proceed normally instead
        of raising `JobAlreadyRunningError` against a lock nothing is
        actually holding anymore.
    Inputs:
        client: an already-constructed Supabase client.
        job_name: the job_name about to attempt `start_batch`.
    Outputs:
        None. Has no effect if no RUNNING row exists, or if the
        RUNNING row found is still within the stale-lock threshold
        (left alone — that's a real concurrent run, not an orphan).
    Failure modes:
        Swallows its own errors (network, unexpected response shape)
        rather than raising — this is a best-effort self-healing step
        layered in front of the guard, not a replacement for it. If
        this check itself fails for any reason, `start_batch` still
        falls through to its normal insert-and-let-the-unique-index-
        decide behavior, so a bug here can never weaken the guard,
        only fail to pre-empt it.
    """
    try:
        response = (
            client.table("ingestion_batches")
            .select("id,started_at")
            .eq("job_name", job_name)
            .eq("status", IngestionBatchStatus.RUNNING.value)
            .order("started_at", desc=True)
            .limit(1)
            .execute()
        )
    except Exception:
        return  # best-effort — fall through to the normal insert/guard path

    rows = response.data or []
    if not rows:
        return

    stuck_id = rows[0]["id"]
    started_at_raw = rows[0]["started_at"]
    try:
        started_at = datetime.fromisoformat(started_at_raw.replace("Z", "+00:00"))
    except Exception:
        return  # unparseable timestamp — don't guess, leave the row alone

    age_minutes = (datetime.now(timezone.utc) - started_at).total_seconds() / 60
    if age_minutes < _STALE_LOCK_MINUTES:
        return  # plausibly still a real, in-progress run — leave it alone

    try:
        client.table("ingestion_batches").update(
            {
                "status": IngestionBatchStatus.FAILED.value,
                "completed_at": _utcnow_iso(),
                "error_summary": (
                    f"Auto-reaped: RUNNING for {age_minutes:.0f} min (threshold "
                    f"{_STALE_LOCK_MINUTES} min) — almost certainly an orphaned lock "
                    f"from a crashed/cancelled/timed-out run, not a genuine "
                    f"in-progress job. Closed automatically by start_batch's "
                    f"stale-lock check so a new run isn't blocked indefinitely."
                ),
            }
        ).eq("id", stuck_id).eq("status", IngestionBatchStatus.RUNNING.value).execute()
    except Exception:
        return  # best-effort — if this update fails, fall through as before


# =============================================================================
# ULID generation (Master Build Specification §3)
# =============================================================================
# Hand-rolled rather than a third-party dependency: the format is small,
# stable, and this keeps the ingestion pipeline's dependency footprint
# to `requests` + `supabase` only, consistent with config.py/
# config_validator.py's existing "no extra dependencies" posture.

_CROCKFORD_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def generate_ulid() -> str:
    """
    Purpose:
        Produce a ULID: 48-bit millisecond timestamp + 80-bit
        randomness, Crockford base32 encoded, 26 characters,
        lexicographically sortable by creation time. Used for both
        `raw_api_batches.id` and `ingestion_batches.id`, per spec §3
        ("ULID (stored as text) — sortable by time, globally unique").
    Inputs:
        None.
    Outputs:
        26-character ULID string.
    Failure modes:
        None.
    """
    timestamp_ms = int(time.time() * 1000)

    ts_chars = []
    t = timestamp_ms
    for _ in range(10):
        ts_chars.append(_CROCKFORD_ALPHABET[t & 0x1F])
        t >>= 5
    timestamp_part = "".join(reversed(ts_chars))

    rand_bits = random.getrandbits(80)
    rand_chars = []
    r = rand_bits
    for _ in range(16):
        rand_chars.append(_CROCKFORD_ALPHABET[r & 0x1F])
        r >>= 5
    random_part = "".join(reversed(rand_chars))

    return timestamp_part + random_part


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# =============================================================================
# ingestion_batches (ops layer)
# =============================================================================

@dataclass(frozen=True)
class IngestionBatchHandle:
    """
    Purpose:
        Handle to a RUNNING `ingestion_batches` row, returned by
        `start_batch`. Every subsequent write in the run (api_call_logs,
        failed_pages, mandi_daily_prices, audit correlation) uses
        `.id` as its `batch_id` foreign key.
    """
    id: str
    job_name: str
    resource: str | None
    started_at: str


def start_batch(
    client: "Client",
    *,
    job_name: str,
    resource: Resource | None = None,
    date_range_start: date | None = None,
    date_range_end: date | None = None,
) -> IngestionBatchHandle:
    """
    Purpose:
        Insert a new RUNNING `ingestion_batches` row. This is the one
        required first write of every Phase C ingestion script's run.
    Inputs:
        client: an already-constructed Supabase client (e.g.
            `StartupContext.supabase`).
        job_name: e.g. "live_tick" — must be stable across runs of the
            same script, since it's the concurrency-guard key.
        resource: the Resource this run is working, or None for a run
            that spans/doesn't map to a single resource.
        date_range_start / date_range_end: the date(s) this run covers.
            For `live_tick`, both are today's date.
    Outputs:
        `IngestionBatchHandle` for the new RUNNING row.
    Failure modes:
        Raises `JobAlreadyRunningError` if another instance of the same
        `job_name` is already RUNNING (see module docstring). Raises
        `ConfigError` for any other insert failure (network, unexpected
        constraint violation, RLS blocking the write).
    """
    batch_id = generate_ulid()
    _reap_stale_running_lock(client, job_name=job_name)
    payload = {
        "id": batch_id,
        "job_name": job_name,
        "resource": resource.value if resource is not None else None,
        "status": IngestionBatchStatus.RUNNING.value,
        "date_range_start": date_range_start.isoformat() if date_range_start else None,
        "date_range_end": date_range_end.isoformat() if date_range_end else None,
    }
    try:
        response = client.table("ingestion_batches").insert(payload).execute()
    except Exception as exc:  # postgrest.exceptions.APIError and friends
        message = str(exc)
        if "uq_ingestion_batches_running_job" in message or (
            "duplicate key" in message.lower() and "ingestion_batches" in message.lower()
        ):
            raise JobAlreadyRunningError(
                f"job_name={job_name!r} already has a RUNNING ingestion_batches "
                "row — refusing to start a second concurrent instance (spec §12)."
            ) from exc
        raise ConfigError(
            f"Failed to insert ingestion_batches row (job_name={job_name!r}): {exc}"
        ) from exc

    if not response.data:
        raise ConfigError(
            "ingestion_batches insert returned no data — expected exactly one "
            "row back. This usually means RLS silently blocked the insert."
        )

    row = response.data[0]
    return IngestionBatchHandle(
        id=row["id"],
        job_name=row["job_name"],
        resource=row.get("resource"),
        started_at=row["started_at"],
    )


def complete_batch(
    client: "Client",
    *,
    batch_id: str,
    status: IngestionBatchStatus,
    rows_processed: int = 0,
    rows_failed: int = 0,
    error_summary: str | None = None,
) -> None:
    """
    Purpose:
        Close out an `ingestion_batches` row: sets its terminal status
        and `completed_at`. Every run must call this exactly once,
        even on failure — an ingestion_batches row left RUNNING
        forever would permanently block future runs of the same
        job_name via the §12 concurrency guard, so callers should wrap
        their work in try/finally (see `live_tick.py`).
    Inputs:
        client: an already-constructed Supabase client.
        batch_id: the `ingestion_batches.id` to close.
        status: SUCCESS, PARTIAL, or FAILED (never RUNNING — see
            Failure modes).
        rows_processed / rows_failed: row-level counters for this run.
        error_summary: short human-readable failure description, if
            status is PARTIAL or FAILED.
    Outputs:
        None.
    Failure modes:
        Raises `ValueError` if `status` is RUNNING (a caller error —
        this function only ever moves a batch OUT of RUNNING). Raises
        `ConfigError` if the update itself fails.
    """
    if status == IngestionBatchStatus.RUNNING:
        raise ValueError(
            "complete_batch: status must be SUCCESS, PARTIAL, or FAILED — "
            "not RUNNING (that's what start_batch already set)."
        )

    payload = {
        "status": status.value,
        "rows_processed": rows_processed,
        "rows_failed": rows_failed,
        "error_summary": error_summary,
        "completed_at": _utcnow_iso(),
    }
    try:
        client.table("ingestion_batches").update(payload).eq("id", batch_id).execute()
    except Exception as exc:
        raise ConfigError(
            f"Failed to close ingestion_batches row {batch_id!r}: {exc}"
        ) from exc


# =============================================================================
# raw_api_batches (bronze layer)
# =============================================================================

@dataclass(frozen=True)
class RawApiBatchHandle:
    """Handle to a RUNNING `raw_api_batches` row, returned by
    `start_raw_batch`. Every `raw_api_records` page write in the run
    uses `.id` as its `batch_id` foreign key; `mandi_daily_prices`
    rows derived from those pages use it as `raw_api_batch_id`."""
    id: str
    job_name: str
    resource: str


def start_raw_batch(
    client: "Client",
    *,
    job_name: str,
    resource: Resource,
    date_range_start: date | None = None,
    date_range_end: date | None = None,
) -> RawApiBatchHandle:
    """
    Purpose:
        Insert a new RUNNING `raw_api_batches` row, to group the
        verbatim government API pages this run is about to fetch.
        Unlike `ingestion_batches`, this table has no §12 concurrency
        guard — multiple concurrent raw batches (even for the same
        job_name) are harmless, since raw_api_records is purely
        additive/immutable and nothing enforces "at most one" here.
    Inputs / Outputs / Failure modes: mirror `start_batch` above,
        against `raw_api_batches` instead of `ingestion_batches`
        (different columns: `total_pages`/`total_records` instead of
        `rows_processed`/`rows_failed` — see `complete_raw_batch`).
    """
    batch_id = generate_ulid()
    payload = {
        "id": batch_id,
        "job_name": job_name,
        "resource": resource.value,
        "status": IngestionBatchStatus.RUNNING.value,
        "date_range_start": date_range_start.isoformat() if date_range_start else None,
        "date_range_end": date_range_end.isoformat() if date_range_end else None,
    }
    try:
        response = client.table("raw_api_batches").insert(payload).execute()
    except Exception as exc:
        raise ConfigError(
            f"Failed to insert raw_api_batches row (job_name={job_name!r}): {exc}"
        ) from exc

    if not response.data:
        raise ConfigError("raw_api_batches insert returned no data.")

    row = response.data[0]
    return RawApiBatchHandle(id=row["id"], job_name=row["job_name"], resource=row["resource"])


def complete_raw_batch(
    client: "Client",
    *,
    batch_id: str,
    status: IngestionBatchStatus,
    total_pages: int = 0,
    total_records: int = 0,
    error_summary: str | None = None,
) -> None:
    """Close out a `raw_api_batches` row. Mirrors `complete_batch`
    above; see that function's docstring for the shared rationale
    (must always be called, even on failure)."""
    if status == IngestionBatchStatus.RUNNING:
        raise ValueError(
            "complete_raw_batch: status must be SUCCESS, PARTIAL, or FAILED."
        )

    payload = {
        "status": status.value,
        "total_pages": total_pages,
        "total_records": total_records,
        "error_summary": error_summary,
        "completed_at": _utcnow_iso(),
    }
    try:
        client.table("raw_api_batches").update(payload).eq("id", batch_id).execute()
    except Exception as exc:
        raise ConfigError(
            f"Failed to close raw_api_batches row {batch_id!r}: {exc}"
        ) from exc


def insert_raw_api_record(
    client: "Client",
    *,
    raw_batch_id: str,
    resource: Resource,
    page_number: int,
    raw_payload: dict,
    parser_version: int,
) -> None:
    """
    Purpose:
        Write one verbatim government API page response into
        `raw_api_records`. Per invariant 1, these rows are never
        edited or deleted after this — the live schema enforces that
        with an immutability trigger (Phase A, Step 1), so this
        function only ever inserts.
    Inputs:
        client: an already-constructed Supabase client.
        raw_batch_id: the `raw_api_batches.id` this page belongs to.
        resource: which government resource this page came from.
        page_number: 1-indexed page number within this batch's
            pagination.
        raw_payload: the full, unmodified JSON response body from the
            government API for this page (not just its `records` list
            — the whole response, so nothing is lost if the API's
            envelope fields ever matter for debugging or replay).
        parser_version: `config.PARSER_VERSION` at the time of fetch —
            lets a future schema-shift be detected by comparing this
            against rows written under an older parser.
    Outputs:
        None.
    Failure modes:
        Raises `ConfigError` if the insert fails (network, FK
        violation on a bad raw_batch_id, RLS).
    """
    payload = {
        "batch_id": raw_batch_id,
        "resource": resource.value,
        "page_number": page_number,
        "raw_payload": raw_payload,
        "parser_version": parser_version,
    }
    try:
        client.table("raw_api_records").insert(payload).execute()
    except Exception as exc:
        raise ConfigError(
            f"Failed to insert raw_api_records row (raw_batch_id={raw_batch_id!r}, "
            f"page={page_number}): {exc}"
        ) from exc


# =============================================================================
# failed_pages (ops layer — recorded by the calling script, not this
# module's own retry logic, since only the caller knows the resource/
# page/error context — but the insert itself is centralized here to
# avoid a fourth place in the codebase that knows failed_pages' shape)
# =============================================================================

def insert_failed_page(
    client: "Client",
    *,
    batch_id: str,
    resource: Resource,
    page: int,
    error_code: str,
    error_message: str,
) -> None:
    """
    Purpose:
        Record one page that failed after exhausting retries — the
        durable, queryable replacement for a page silently disappearing
        (spec §1 invariant 6: "every failed API page is persisted in a
        table, never a local file"; this is why `sync_prices_v2.py`'s
        local `sync_failures.json` file has no equivalent in v2).
    Inputs:
        client: an already-constructed Supabase client.
        batch_id: the `ingestion_batches.id` this failure belongs to.
        resource: which government resource the page came from.
        page: the page number that failed.
        error_code: one of spec §7's error codes (INGEST-001,
            INGEST-002, ...).
        error_message: human-readable detail for triage.
    Outputs:
        None.
    Failure modes:
        Raises `ConfigError` if the insert fails.
    """
    payload = {
        "batch_id": batch_id,
        "resource": resource.value,
        "page": page,
        "error_code": error_code,
        "error_message": error_message,
        "status": "PENDING",
    }
    try:
        client.table("failed_pages").insert(payload).execute()
    except Exception as exc:
        raise ConfigError(
            f"Failed to insert failed_pages row (batch_id={batch_id!r}, page={page}): {exc}"
        ) from exc


# =============================================================================
# data_quality_issues (ops layer — permanent bad-data quarantine, NOT
# retryable like failed_pages)
#
# Why this is a separate table from failed_pages rather than reusing it:
#   failed_pages is keyed at PAGE granularity and exists so
#   retry_failed_pages.py can re-fetch that exact page later — it is
#   for TRANSIENT failures (network blip, rate limit, timeout) where
#   trying again is the right move.
#   A `chk_prices_min_max` violation (or similar row-level constraint
#   violation) is neither page-level nor transient: it's one bad row
#   inside an otherwise-good page, and re-fetching the same page will
#   produce the identical bad row forever (2026-06-29's
#   min_price=83548/max_price=8354 is a permanent government
#   data-entry error, not a fluke). Filing it under failed_pages would
#   make retry_failed_pages.py loop on something it can never fix.
#   This table is the durable record of "this specific row will never
#   be stored, and here's why" — for human triage, not automated retry.
# =============================================================================

def insert_data_quality_issue(
    client: "Client",
    *,
    batch_id: str,
    resource: Resource,
    row: dict,
    error_code: str | None,
    error_message: str,
) -> None:
    """
    Purpose:
        Record one price row that was dropped from `mandi_daily_prices`
        because it violated a database constraint (or some other
        row-level rejection) at upsert time — the row-level counterpart
        to `insert_failed_page`'s page-level record. See this
        function's module-section docstring for why this is a
        different table from `failed_pages`.
    Inputs:
        client: an already-constructed Supabase client.
        batch_id: the `ingestion_batches.id` this row's run belongs to.
        resource: which government resource the row came from.
        row: the full row dict that was rejected (mandi_id, crop_id,
            variety, price_date, modal_price, min_price, max_price,
            source, etc.) — stored verbatim so triage never has to
            reconstruct it from logs.
        error_code: the Postgres SQLSTATE if available (e.g. "23514"
            for a check-constraint violation), else None.
        error_message: the database's error message, for triage.
    Outputs:
        None.
    Failure modes:
        Raises `ConfigError` if the insert fails. Deliberately NOT
        swallowed — losing the quarantine record would silently
        re-create the exact "row just vanishes" problem this table
        exists to prevent.
    """
    payload = {
        "batch_id": batch_id,
        "resource": resource.value,
        "row_data": row,
        "error_code": error_code,
        "error_message": error_message,
    }
    try:
        client.table("data_quality_issues").insert(payload).execute()
    except Exception as exc:
        raise ConfigError(
            f"Failed to insert data_quality_issues row (batch_id={batch_id!r}, "
            f"row={row!r}): {exc}"
        ) from exc