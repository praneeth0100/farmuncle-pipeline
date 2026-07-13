"""
FarmUncle v2 — ingest_common.py
Phase C, Step 13 — CLOSES this step.

Purpose (module-level):
    The single shared module named in the Master Build Specification's
    Phase C plan: "13. Shared `ingest_common.py` (§17 startup
    validation, §18 logging)". Every Phase C ingestion script
    (live_tick, daily_rewrite, historical_backfill, retry_failed_pages,
    weekly_compress) imports from HERE, not from `config`,
    `config_validator`, `startup_validation`, or `logging_utils`
    individually — this is the one place those four modules'
    public surfaces are assembled for consumption.

    This module contains no logic of its own. It is a facade:
    startup validation (§17) is implemented in `startup_validation.py`
    (built on `config.py` + `config_validator.py`); logging (§18) is
    implemented in `logging_utils.py`. Splitting the implementation
    across those focused, independently-testable files — instead of
    one large module — was a deliberate choice for this build (see the
    session's build discussion), while still producing the single
    `ingest_common.py` import surface the spec's Phase C plan names.

Explicitly out of scope for this file:
    - Any validation, logging, or client-construction logic itself
      (re-exported only — see the modules listed above)
    - Batch lifecycle, retry/backoff, advisory locks, identity
      resolution, or government API fetching — none of these are part
      of spec Step 13 ("§17 startup validation, §18 logging" only).
      They belong to `ingestion/` scripts and the `identity/` module,
      Phase C Steps 14 onward, per spec §5's module structure, and
      must not be added here speculatively.

Typical usage in a Step 14+ script's `main`:

    from ingest_common import validate_startup, log_api_call, time_api_call, Resource, ApiCallStatus

    def main() -> None:
        ctx = validate_startup()          # §17 — raises ConfigError and exits if anything is wrong
        # ... create an ingestion_batches row (Step 14's job, not this module's) ...
        with time_api_call() as timer:
            response = fetch_resource_1_page(...)
        log_api_call(                      # §18
            ctx.supabase,
            batch_id=batch_id,
            job_name="live_tick",
            resource=Resource.RESOURCE_1,
            duration_ms=timer.duration_ms,
            status=ApiCallStatus.SUCCESS,
            rows=len(response),
        )
"""

from __future__ import annotations

# --- config.py: secrets, versions, vocabulary enums --------------------
from farmuncle_pipeline.config import (
    AppConfig,
    ApiCallStatus,
    ConfigError,
    DEFAULT_HTTP_HEADERS,
    FailedPageStatus,
    IngestionBatchStatus,
    NORMALIZATION_VERSION,
    PARSER_VERSION,
    Resource,
    RPC_VERSION,
    RuntimeConfig,
    SCHEMA_VERSION,
    Secrets,
    Source,
    TAXONOMY_VERSION,
    all_versions,
)

# --- government_constants.py: domain data, not "configuration" ---------
from farmuncle_pipeline.government_constants import STATES

# --- config_validator.py: §17, live-schema half -------------------------
from farmuncle_pipeline.core.config_validator import SchemaValidationResult, validate_live_schema

# --- startup_validation.py: §17, composition + entry point -------------
from farmuncle_pipeline.core.startup_validation import StartupContext, validate_startup

# --- logging_utils.py: §18 -----------------------------------------------
from farmuncle_pipeline.core.logging_utils import ApiCallLogEntry, log_api_call, time_api_call, utcnow_iso

__all__ = [
    # config.py
    "AppConfig",
    "ApiCallStatus",
    "ConfigError",
    "DEFAULT_HTTP_HEADERS",
    "FailedPageStatus",
    "IngestionBatchStatus",
    "NORMALIZATION_VERSION",
    "PARSER_VERSION",
    "Resource",
    "RPC_VERSION",
    "RuntimeConfig",
    "SCHEMA_VERSION",
    "Secrets",
    "Source",
    "TAXONOMY_VERSION",
    "all_versions",
    # government_constants.py
    "STATES",
    # config_validator.py
    "SchemaValidationResult",
    "validate_live_schema",
    # startup_validation.py
    "StartupContext",
    "validate_startup",
    # logging_utils.py
    "ApiCallLogEntry",
    "log_api_call",
    "time_api_call",
    "utcnow_iso",
]
