"""
FarmUncle v2 — logging_utils.py
Phase C, Step 13 (part 1 of 2 — see also `startup_validation.py`,
combined for import purposes by `ingest_common.py`).

Purpose (module-level):
    Implements Master Build Specification §18's logging standard as a
    real queryable table write, not a free-form log line: "Every log
    line: timestamp · batch_id · job_name · resource · page · duration
    · rows · status · error_code. api_call_logs table implements this
    as a queryable table, not just a log format." This module is the
    one place in the pipeline that writes to `api_call_logs` — no
    ingestion script should `client.table("api_call_logs").insert(...)`
    directly (Never-Do Rules §2, no duplicated business logic).

Explicitly out of scope for this file:
    - Constructing a Supabase client (caller passes one in, already
      built by `startup_validation.validate_startup`)
    - Batch lifecycle (`ingestion_batches` rows) — that is
      `batch_manager`'s job (Phase C, Step 14+), not this module's
    - Retry/backoff around the log write itself — a failed log write
      raises immediately; callers decide what to do (see
      `log_api_call`'s Failure modes)
    - Any log destination other than `api_call_logs` — no file
      logging, no stdout-only logging as the primary record. Console
      echo (see `echo=`) is a convenience for watching a live GitHub
      Actions run, not a substitute for the DB row.

Live schema this module was written against (verified via the
connected Supabase MCP against wqccgjmvslevkglfkmtc, Step 8's build
log): `api_call_logs(batch_id, job_name, resource, page, duration_ms,
rows, status, error_code, called_at)`, FK restrict to
`ingestion_batches.id`, CHECK `chk_api_call_logs_error_code_on_failure`
(a FAILURE row must carry a non-null error_code). This module enforces
that same rule client-side (see `log_api_call`) so a caller gets a
clear `ConfigError`-free `ValueError` immediately, in Python, instead
of a less legible Postgres constraint-violation round-trip.
"""

from __future__ import annotations

import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from typing import TYPE_CHECKING, Iterator

from farmuncle_pipeline.config import ApiCallStatus, ConfigError, Resource

if TYPE_CHECKING:
    # Import only for type checkers; this module never constructs a
    # client itself (see module docstring's "Explicitly out of scope").
    from supabase import Client


@dataclass(frozen=True)
class ApiCallLogEntry:
    """
    Purpose:
        Strongly typed record of a single logged API call, returned by
        `log_api_call` after a successful insert, so callers can
        inspect what was actually written (e.g. the DB-assigned `id`)
        without re-querying.
    Inputs:
        N/A (data container).
    Outputs:
        N/A (data container).
    Failure modes:
        None.
    """
    id: int
    batch_id: str
    job_name: str
    resource: str | None
    page: int | None
    duration_ms: int
    rows: int | None
    status: str
    error_code: str | None
    called_at: str


def log_api_call(
    client: "Client",
    *,
    batch_id: str,
    job_name: str,
    resource: Resource | None,
    duration_ms: int,
    status: ApiCallStatus,
    page: int | None = None,
    rows: int | None = None,
    error_code: str | None = None,
    echo: bool = True,
) -> ApiCallLogEntry:
    """
    Purpose:
        Write one §18-compliant row to `api_call_logs`. This is the
        only sanctioned way any Phase C script records an API call.
    Inputs:
        client: an already-constructed Supabase client (service-role),
            typically `StartupContext.supabase` from
            `startup_validation.validate_startup`.
        batch_id: the `ingestion_batches.id` this call belongs to.
            Required — `api_call_logs.batch_id` has no default and an
            FK restrict to `ingestion_batches` (Step 8 build log), so
            an invalid batch_id will surface as a Postgres FK error
            (see Failure modes), not silently succeed.
        job_name: e.g. "live_tick", "daily_rewrite" — free text,
            mirrors `ingestion_batches.job_name`.
        resource: `Resource.RESOURCE_1` / `Resource.RESOURCE_2`, or
            `None` for calls not tied to a specific government
            resource (matches the nullable `api_call_logs.resource`
            column).
        duration_ms: wall-clock duration of the call in milliseconds.
            Use `time_api_call()` below to measure this without manual
            bookkeeping.
        status: `ApiCallStatus.SUCCESS` or `ApiCallStatus.FAILURE`.
        page: page number within the resource's pagination, if
            applicable.
        rows: row/record count returned by the call, if applicable.
        error_code: required (and validated client-side, see Failure
            modes) when `status` is `FAILURE`. One of the spec §7
            error codes (`INGEST-001`, `INGEST-002`, etc.) — this
            module does not itself validate against that vocabulary,
            since new codes may be added without a config.py change.
        echo: if True (default), also print a single §18-formatted
            line to stdout, for a human watching a live CI run. This
            is a convenience only — the `api_call_logs` row is the
            durable record regardless of `echo`.
    Outputs:
        `ApiCallLogEntry` reflecting the row as written (including the
        DB-assigned `id` and server-assigned `called_at`).
    Failure modes:
        Raises `ValueError` (not `ConfigError` — this is a call-site
        programming error, not an environment/config problem) if
        `status` is `FAILURE` and `error_code` is falsy, matching the
        live `chk_api_call_logs_error_code_on_failure` constraint —
        callers get this immediately rather than a Postgres round-trip.
        Raises `ConfigError` if the insert itself fails (network error,
        FK violation on a bad `batch_id`, or any other Postgres
        rejection) — a log write failing is itself a startup/runtime
        integrity problem per §17's spirit, not something to swallow
        silently.
    """
    if status == ApiCallStatus.FAILURE and not error_code:
        raise ValueError(
            "log_api_call: status=FAILURE requires a non-empty error_code "
            "(mirrors the live chk_api_call_logs_error_code_on_failure "
            "constraint on api_call_logs)."
        )

    payload = {
        "batch_id": batch_id,
        "job_name": job_name,
        "resource": resource.value if resource is not None else None,
        "page": page,
        "duration_ms": duration_ms,
        "rows": rows,
        "status": status.value,
        "error_code": error_code,
    }

    try:
        response = client.table("api_call_logs").insert(payload).execute()
    except Exception as exc:  # postgrest.exceptions.APIError and friends
        raise ConfigError(
            f"Failed to write api_call_logs row (batch_id={batch_id!r}, "
            f"job_name={job_name!r}): {exc}"
        ) from exc

    if not response.data:
        raise ConfigError(
            "api_call_logs insert returned no data — expected exactly "
            "one row back. This usually means RLS silently blocked the "
            "insert; check the service_role key is actually being used."
        )

    row = response.data[0]
    entry = ApiCallLogEntry(
        id=row["id"],
        batch_id=row["batch_id"],
        job_name=row["job_name"],
        resource=row.get("resource"),
        page=row.get("page"),
        duration_ms=row["duration_ms"],
        rows=row.get("rows"),
        status=row["status"],
        error_code=row.get("error_code"),
        called_at=row["called_at"],
    )

    if echo:
        _echo_log_line(entry)

    return entry


def _echo_log_line(entry: ApiCallLogEntry) -> None:
    """
    Purpose:
        Print a single §18-formatted line to stdout (SUCCESS) or
        stderr (FAILURE) for live visibility during a GitHub Actions
        run. Not the durable record — see module docstring.
    Inputs:
        entry: the `ApiCallLogEntry` just written.
    Outputs:
        None (writes to stdout/stderr).
    Failure modes:
        None.
    """
    line = (
        f"{entry.called_at} \u00b7 {entry.batch_id} \u00b7 {entry.job_name} "
        f"\u00b7 {entry.resource} \u00b7 {entry.page} \u00b7 "
        f"{entry.duration_ms}ms \u00b7 {entry.rows} \u00b7 {entry.status} "
        f"\u00b7 {entry.error_code}"
    )
    stream = sys.stderr if entry.status == ApiCallStatus.FAILURE.value else sys.stdout
    print(line, file=stream)


@contextmanager
def time_api_call() -> Iterator["_Timer"]:
    """
    Purpose:
        Small context manager so callers don't hand-roll
        `perf_counter()` bookkeeping around every government API call
        just to get a `duration_ms` for `log_api_call`.
    Inputs:
        None.
    Outputs:
        Yields a `_Timer` whose `.duration_ms` is 0 until the `with`
        block exits, then holds the elapsed wall-clock time in
        milliseconds.
    Failure modes:
        None — timing happens regardless of whether the wrapped code
        raises; `.duration_ms` reflects time elapsed up to the
        exception if the block fails.

    Example:
        with time_api_call() as timer:
            response = requests.get(url, timeout=30)
        log_api_call(client, ..., duration_ms=timer.duration_ms, ...)
    """
    timer = _Timer()
    start = perf_counter()
    try:
        yield timer
    finally:
        timer.duration_ms = int((perf_counter() - start) * 1000)


@dataclass
class _Timer:
    """Mutable holder for `time_api_call`'s elapsed duration; not part
    of this module's public surface beyond that context manager."""
    duration_ms: int = 0


def utcnow_iso() -> str:
    """
    Purpose:
        Single place that formats "now" as an ISO-8601 UTC timestamp,
        for any caller that needs to stamp a value client-side (e.g. a
        `failed_pages`/`ingestion_batches` write in a future step) with
        the same convention this module uses. `api_call_logs.called_at`
        itself is server-assigned (`default now()`, per the Step 8
        build log) and does NOT need this — it's provided for
        consistency elsewhere in Phase C, not used internally here.
    Inputs:
        None.
    Outputs:
        ISO-8601 string, e.g. "2026-07-10T12:00:00+00:00".
    Failure modes:
        None.
    """
    return datetime.now(timezone.utc).isoformat()
