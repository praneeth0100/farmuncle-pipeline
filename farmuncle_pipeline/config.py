"""
FarmUncle v2 — config.py
Phase C, Step 13A (revised).

Purpose (module-level):
    Single source of truth for all environment-derived secrets, all
    database-derived runtime configuration (system_config), and all
    version/vocabulary constants shared across the ingestion pipeline
    (live_tick, daily_rewrite, historical_backfill, retry_failed_pages,
    weekly_compress, and the quality/identity modules).

Explicitly out of scope for this file (owned elsewhere, per the Master
Build Specification's module boundaries, §5 and §17):
    - Constructing a Supabase client
    - Making HTTP requests to the government API
    - Retry / backoff execution
    - Logging
    - Batch lifecycle management
    - Identity resolution (find_or_create_* calls)
    - Any other business logic

This module never talks to the network or to Supabase directly. Callers
are responsible for fetching `system_config` rows (e.g. via
`supabase.table("system_config").select("key,value").execute().data`)
and passing them into `RuntimeConfig.from_rows(...)`. This keeps
config.py a pure, dependency-light, easily unit-testable module.

Government-domain constants (e.g. the per-state list used to paginate
Resource 2) live in `government_constants.py`, not here — see that
module's docstring for the rationale.

NOTE on scope vs. Step 13B: this module validates that secrets exist
and that `system_config` rows are well-formed and individually sane
(right type, right range, internally consistent with each other, e.g.
MERGE_THRESHOLD >= FUZZY_THRESHOLD). It deliberately does NOT validate:
    - that required tables exist in the live schema
    - that required RPCs exist, with the expected signature
    - that the live schema is compatible with the version constants
      defined below (SCHEMA_VERSION, PARSER_VERSION, etc.)
    - that all expected migrations have been applied
That broader "is the live database actually the database this code
expects" validation is Step 13B's responsibility, layered on top of
this module, not duplicated here.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, fields
from enum import Enum
from typing import Final, Iterable, Mapping


# =============================================================================
# Errors
# =============================================================================

class ConfigError(Exception):
    """
    Purpose:
        Raised for any configuration problem — missing secret, missing
        or malformed system_config row, or a value that fails
        validation. Callers (ingestion scripts) are expected to let
        this propagate and crash the process before any work begins,
        per the Master Build Specification §17 startup-validation
        requirement ("every script fails fast before doing any work if
        a secret is missing ... or RPC/schema version mismatch").
    Inputs:
        Standard Exception message.
    Outputs:
        N/A.
    Failure modes:
        N/A — this *is* the failure-mode signal.
    """


# =============================================================================
# Version constants (Master Build Specification §3 — ID & Versioning Strategy)
# =============================================================================
# All five version columns described in §3 are seeded at 1 across every
# table in the live schema (verified against wqccgjmvslevkglfkmtc: crops
# .taxonomy_version, mandi_aliases/crop_aliases.normalization_version,
# ingestion_batches/raw_api_batches.schema_version all default to 1).
# raw_api_records.parser_version and mandi_daily_prices.parser_version /
# .normalization_version carry NO column default in the live schema —
# they must be supplied explicitly by the writer on every insert. These
# constants are that single source of truth; nothing else in the
# pipeline should hardcode a version number.
#
# Per Master Build Specification §19 / Never-Do Rules §2: never change
# the parser without bumping PARSER_VERSION, and never bump a version
# constant without a corresponding ADR (§17 of this spec's ADR process).

SCHEMA_VERSION: Final[int] = 1          # ingestion_batches.schema_version, raw_api_batches.schema_version
TAXONOMY_VERSION: Final[int] = 1        # crops.taxonomy_version
NORMALIZATION_VERSION: Final[int] = 1   # mandi_aliases/crop_aliases/mandi_daily_prices.normalization_version
RPC_VERSION: Final[int] = 1             # entity_history.rpc_version
PARSER_VERSION: Final[int] = 1          # raw_api_records.parser_version, mandi_daily_prices.parser_version


def all_versions() -> dict[str, int]:
    """
    Purpose:
        Return all five §3 version constants together as a single dict,
        for callers that need to stamp/log/compare the full version
        set at once (e.g. a batch header, a structured log line per
        §18, or Step 13B's future schema-compatibility check) instead
        of importing each constant individually.
    Inputs:
        None.
    Outputs:
        dict with keys "schema_version", "taxonomy_version",
        "normalization_version", "rpc_version", "parser_version",
        mapping to the module-level constants above.
    Failure modes:
        None.
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "taxonomy_version": TAXONOMY_VERSION,
        "normalization_version": NORMALIZATION_VERSION,
        "rpc_version": RPC_VERSION,
        "parser_version": PARSER_VERSION,
    }


# =============================================================================
# Shared vocabulary constants — mirror live CHECK constraints exactly
# =============================================================================
# Inspected directly against wqccgjmvslevkglfkmtc via pg_constraint /
# information_schema. These enums exist so that job scripts never
# hand-type a status/resource/source string (source of the "duplicated
# business logic across scripts" failure mode called out in §0 of the
# spec). If the live CHECK constraint ever changes, this is the one
# place to update.


class Resource(str, Enum):
    """
    Government data source identifier.

    Matches the CHECK constraint shared by raw_api_batches.resource,
    raw_api_records.resource, ingestion_batches.resource,
    api_call_logs.resource, and failed_pages.resource — all of which
    only permit resource_1 / resource_2 (no "manual" — manual entries
    never come from a government feed page, so they're out of scope
    for these tables).
    """
    RESOURCE_1 = "resource_1"  # live feed, today-only, best-effort (§9)
    RESOURCE_2 = "resource_2"  # authoritative daily + historical (§9)


class Source(str, Enum):
    """
    Provenance of a canonical entity or price row.

    Matches the CHECK constraint on mandis.ingested_from,
    crops.ingested_from, mandi_aliases.source, crop_aliases.source,
    and mandi_daily_prices.source — all three of RESOURCE_1, RESOURCE_2,
    and MANUAL are permitted here, unlike `Resource` above.
    """
    RESOURCE_1 = "resource_1"
    RESOURCE_2 = "resource_2"
    MANUAL = "manual"


class IngestionBatchStatus(str, Enum):
    """Matches ingestion_batches.status / raw_api_batches.status CHECK."""
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


class FailedPageStatus(str, Enum):
    """Matches failed_pages.status CHECK."""
    PENDING = "PENDING"
    RESOLVED = "RESOLVED"


class ApiCallStatus(str, Enum):
    """Matches api_call_logs.status CHECK."""
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"

class AlertSeverity(str, Enum):
    """Matches quality_alerts.severity CHECK (chk_quality_alerts_severity)."""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class AlertStatus(str, Enum):
    """Matches quality_alerts.status CHECK (chk_quality_alerts_status)."""
    OPEN = "OPEN"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    RESOLVED = "RESOLVED"


class AlertEntityType(str, Enum):
    """Matches quality_alerts.entity_type CHECK
    (chk_quality_alerts_entity_type) — nullable; only set when an
    alert is about one specific mandi or crop, not an aggregate
    finding (duplicate rows, coverage gaps, retry-queue backlog)."""
    MANDI = "mandi"
    CROP = "crop"


# =============================================================================
# Government API constants
# =============================================================================
# Request *headers* are inert, non-secret, non-environment-specific
# constants reused from sync_prices_v2.py per task scope ("Reuse ONLY
# from the old scripts: Government API endpoints, headers"). No request
# logic lives here — the ingestion module (Step 14+) imports this and
# does the fetching.
#
# API *endpoint URLs* are deliberately NOT hardcoded here. They are
# environment/deployment configuration, no different in kind from
# PAGE_SIZE or API_TIMEOUT below, and belong in system_config for the
# same reason: so an endpoint change is a data update, not a code
# deploy.
#
# API_BASE_RESOURCE_1 / API_BASE_RESOURCE_2 are REQUIRED
# system_config keys, on the same footing as PAGE_SIZE et al.
# `system_config` is the only source of truth for these endpoints —
# there is no hardcoded fallback anywhere in this module, per
# Never-Do Rules §2 ("no business logic duplicated") and §17 ("do not
# guess").
#
# NOTE: a Phase C migration will INSERT the two corresponding rows
# ("API_BASE_RESOURCE_1", "API_BASE_RESOURCE_2") into the live
# system_config table before Step 14 (live_tick.py) is built. Until
# that migration lands, `RuntimeConfig.from_rows` will correctly
# raise `ConfigError` against the current live schema — this is
# intended, not a bug: a missing required endpoint is a hard stop, not
# a silent default.

DEFAULT_HTTP_HEADERS: Final[Mapping[str, str]] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}


# =============================================================================
# Secrets (Master Build Specification §17 — GitHub Secrets only)
# =============================================================================

_REQUIRED_SECRET_ENV_VARS: Final[tuple[str, ...]] = (
    "SUPABASE_URL",
    "SUPABASE_SERVICE_KEY",
    "DATA_GOV_API_KEY",
    "HISTORY_SUPABASE_URL",
    "HISTORY_SUPABASE_SERVICE_KEY",
    "R2_ACCOUNT_ID",
    "R2_ACCESS_KEY_ID",
    "R2_SECRET_ACCESS_KEY",
)


@dataclass(frozen=True)
class Secrets:
    """
    Purpose:
        Strongly typed, immutable container for every secret the v2
        pipeline needs, loaded once at process startup.
    Inputs:
        None directly — populated via `Secrets.from_env`.
    Outputs:
        N/A (data container).
    Failure modes:
        None on its own; construction failures are raised by
        `from_env`, not by this class.
    """
    supabase_url: str
    supabase_service_key: str
    data_gov_api_key: str
    history_supabase_url: str
    history_supabase_service_key: str
    r2_account_id: str
    r2_access_key_id: str
    r2_secret_access_key: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Secrets":
        """
        Purpose:
            Load and validate all required secrets from environment
            variables in a single fail-fast pass, per §17's startup
            validation requirement.
        Inputs:
            env: optional mapping to read from instead of the real
                process environment (`os.environ`). Intended for tests;
                production callers omit this.
        Outputs:
            A populated, immutable `Secrets` instance.
        Failure modes:
            Raises `ConfigError` listing every missing environment
            variable at once (not just the first one found) if one or
            more of the required variables in `_REQUIRED_SECRET_ENV_VARS`
            is absent or empty.
        """
        source = env if env is not None else os.environ
        missing = [
            name for name in _REQUIRED_SECRET_ENV_VARS
            if not source.get(name)
        ]
        if missing:
            raise ConfigError(
                "Missing required environment variable(s): "
                f"{', '.join(missing)}. All of "
                f"{', '.join(_REQUIRED_SECRET_ENV_VARS)} must be set "
                "(GitHub Secrets in production, per spec §17)."
            )
        return cls(
            supabase_url=source["SUPABASE_URL"],
            supabase_service_key=source["SUPABASE_SERVICE_KEY"],
            data_gov_api_key=source["DATA_GOV_API_KEY"],
            history_supabase_url=source["HISTORY_SUPABASE_URL"],
            history_supabase_service_key=source["HISTORY_SUPABASE_SERVICE_KEY"],
            r2_account_id=source["R2_ACCOUNT_ID"],
            r2_access_key_id=source["R2_ACCESS_KEY_ID"],
            r2_secret_access_key=source["R2_SECRET_ACCESS_KEY"],
        )


# =============================================================================
# Runtime configuration (Master Build Specification §17 — system_config)
# =============================================================================
# Live system_config contents (verified against wqccgjmvslevkglfkmtc,
# 8 seed rows, all key/value as text):
#   PAGE_SIZE=500   MAX_RETRIES=3   RETRY_DELAY=5   FUZZY_THRESHOLD=0.75
#   MERGE_THRESHOLD=0.90   API_TIMEOUT=30   BATCH_SIZE=1000
#   QUALITY_THRESHOLD=0.95

_EXPECTED_SYSTEM_CONFIG_KEYS: Final[tuple[str, ...]] = (
    "PAGE_SIZE",
    "MAX_RETRIES",
    "RETRY_DELAY",
    "FUZZY_THRESHOLD",
    "MERGE_THRESHOLD",
    "API_TIMEOUT",
    "BATCH_SIZE",
    "QUALITY_THRESHOLD",
    "API_BASE_RESOURCE_1",
    "API_BASE_RESOURCE_2",
)


def _validate_and_index_rows(
    rows: Iterable[Mapping[str, str]],
) -> dict[str, str]:
    """
    Purpose:
        Validate the raw shape of `system_config` rows before any
        key-specific parsing happens: every row must carry a "key" and
        a "value", keys must be non-empty, and no key may appear twice.
        This is deliberately separate from the type/range validation in
        `RuntimeConfig.from_rows` — malformed *rows* (wrong shape) and
        invalid *values* (right shape, wrong content) are different
        failure classes and are reported as such.
    Inputs:
        rows: an iterable of mappings, each expected to have at least a
            "key" and "value" string field.
    Outputs:
        A dict mapping each validated key to its (still-raw-string)
        value.
    Failure modes:
        Raises `ConfigError`, listing every malformed row found in a
        single pass, if any row is missing "key", is missing "value",
        has an empty/blank "key", or if the same key appears more than
        once across all rows.
    """
    errors: list[str] = []
    indexed: dict[str, str] = {}
    seen_keys: set[str] = set()

    for position, row in enumerate(rows):
        if "key" not in row:
            errors.append(f"row {position} is missing a \"key\" field: {row!r}")
            continue

        key = row["key"]
        if not isinstance(key, str) or key.strip() == "":
            errors.append(f"row {position} has an empty or blank \"key\": {row!r}")
            continue

        if "value" not in row:
            errors.append(f"row {position} (key={key!r}) is missing a \"value\" field")
            continue

        if key in seen_keys:
            errors.append(f"duplicate system_config key: {key!r}")
            continue

        seen_keys.add(key)
        indexed[key] = row["value"]

    if errors:
        raise ConfigError(
            "Malformed system_config row(s): " + "; ".join(errors)
        )

    return indexed


@dataclass(frozen=True)
class RuntimeConfig:
    """
    Purpose:
        Strongly typed, immutable, validated view of the `system_config`
        table. This is the only place in the pipeline that should know
        the raw string keys used in that table.
    Inputs:
        None directly — populated via `RuntimeConfig.from_rows`.
    Outputs:
        N/A (data container).
    Failure modes:
        None on its own; construction failures are raised by
        `from_rows`, not by this class.
    """
    page_size: int
    max_retries: int
    retry_delay_seconds: int
    fuzzy_threshold: float
    merge_threshold: float
    api_timeout_seconds: int
    batch_size: int
    quality_threshold: float
    # Required — see `_EXPECTED_SYSTEM_CONFIG_KEYS` above. system_config
    # is the only source of truth for these; there is no fallback.
    api_base_resource_1: str
    api_base_resource_2: str

    @classmethod
    def from_rows(cls, rows: Iterable[Mapping[str, str]]) -> "RuntimeConfig":
        """
        Purpose:
            Convert raw `system_config` rows (as returned by a Supabase
            `select("key,value")` on that table) into a validated,
            strongly typed `RuntimeConfig`. Deliberately takes plain
            row mappings rather than a Supabase client/response object,
            so this module has zero dependency on the supabase-py
            library or network access.
        Inputs:
            rows: an iterable of mappings, each with at least a "key"
                and "value" string field (extra fields, e.g.
                "description"/"updated_at", are ignored).
        Outputs:
            A populated, immutable `RuntimeConfig` instance.
        Failure modes:
            Raises `ConfigError` if:
              - any row is malformed (missing "key"/"value", empty key,
                or a key duplicated across rows — see
                `_validate_and_index_rows`),
              - any key in `_EXPECTED_SYSTEM_CONFIG_KEYS` is absent from
                `rows` (per spec: "Do not guess" — a missing
                system_config row is a hard stop, not a silent default),
              - a value cannot be parsed as the expected type
                (int/float),
              - a parsed value fails range validation (e.g. a threshold
                outside [0, 1], a non-positive page size), or
              - `merge_threshold` is lower than `fuzzy_threshold` (a
                fuzzy match must already clear the lower bar before the
                higher bar can permit an unattended merge — see spec
                §13 `merge_entity` and §6.5 merge policy), or
              - `API_BASE_RESOURCE_1` / `API_BASE_RESOURCE_2` is missing
                or blank (`system_config` is the only source of truth
                for these endpoints — no hardcoded fallback exists in
                this module).
            All problems found at a given stage are collected and raised
            together in one `ConfigError`, not one-at-a-time, so a
            misconfigured environment can be fixed in a single pass.
        """
        raw = _validate_and_index_rows(rows)

        missing = [k for k in _EXPECTED_SYSTEM_CONFIG_KEYS if k not in raw]
        if missing:
            raise ConfigError(
                "Missing required system_config key(s): "
                f"{', '.join(missing)}. Expected all of "
                f"{', '.join(_EXPECTED_SYSTEM_CONFIG_KEYS)} to exist as "
                "rows in the system_config table."
            )

        errors: list[str] = []

        def parse_int(key: str) -> int:
            try:
                return int(raw[key])
            except ValueError:
                errors.append(f"{key}={raw[key]!r} is not a valid integer")
                return 0

        def parse_float(key: str) -> float:
            try:
                return float(raw[key])
            except ValueError:
                errors.append(f"{key}={raw[key]!r} is not a valid float")
                return 0.0

        page_size = parse_int("PAGE_SIZE")
        max_retries = parse_int("MAX_RETRIES")
        retry_delay_seconds = parse_int("RETRY_DELAY")
        fuzzy_threshold = parse_float("FUZZY_THRESHOLD")
        merge_threshold = parse_float("MERGE_THRESHOLD")
        api_timeout_seconds = parse_int("API_TIMEOUT")
        batch_size = parse_int("BATCH_SIZE")
        quality_threshold = parse_float("QUALITY_THRESHOLD")

        if not errors:
            if page_size <= 0:
                errors.append(f"PAGE_SIZE must be positive, got {page_size}")
            if max_retries < 0:
                errors.append(f"MAX_RETRIES must be >= 0, got {max_retries}")
            if retry_delay_seconds < 0:
                errors.append(f"RETRY_DELAY must be >= 0, got {retry_delay_seconds}")
            if api_timeout_seconds <= 0:
                errors.append(f"API_TIMEOUT must be positive, got {api_timeout_seconds}")
            if batch_size <= 0:
                errors.append(f"BATCH_SIZE must be positive, got {batch_size}")
            for name, value in (
                ("FUZZY_THRESHOLD", fuzzy_threshold),
                ("MERGE_THRESHOLD", merge_threshold),
                ("QUALITY_THRESHOLD", quality_threshold),
            ):
                if not (0.0 <= value <= 1.0):
                    errors.append(f"{name} must be between 0 and 1, got {value}")
            if not errors and merge_threshold < fuzzy_threshold:
                errors.append(
                    "MERGE_THRESHOLD "
                    f"({merge_threshold}) must be >= FUZZY_THRESHOLD "
                    f"({fuzzy_threshold})"
                )

        # Required keys: presence already guaranteed by the `missing`
        # check above (both are in `_EXPECTED_SYSTEM_CONFIG_KEYS`). An
        # empty/blank string is still not a usable endpoint, so it's
        # rejected here rather than silently accepted.
        api_base_resource_1 = raw["API_BASE_RESOURCE_1"]
        if api_base_resource_1.strip() == "":
            errors.append("API_BASE_RESOURCE_1 is present but empty")

        api_base_resource_2 = raw["API_BASE_RESOURCE_2"]
        if api_base_resource_2.strip() == "":
            errors.append("API_BASE_RESOURCE_2 is present but empty")

        if errors:
            raise ConfigError(
                "Invalid system_config value(s): " + "; ".join(errors)
            )

        return cls(
            page_size=page_size,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
            fuzzy_threshold=fuzzy_threshold,
            merge_threshold=merge_threshold,
            api_timeout_seconds=api_timeout_seconds,
            batch_size=batch_size,
            quality_threshold=quality_threshold,
            api_base_resource_1=api_base_resource_1,
            api_base_resource_2=api_base_resource_2,
        )

    def as_dict(self) -> dict[str, int | float | str]:
        """
        Purpose:
            Flatten this config into a plain dict, for structured
            logging (§18) or inclusion in an `ingestion_batches` /
            `api_call_logs` row without callers needing to know the
            dataclass's field names ahead of time.
        Inputs:
            None (uses self).
        Outputs:
            dict mapping each field name to its value.
        Failure modes:
            None.
        """
        return {f.name: getattr(self, f.name) for f in fields(self)}


# =============================================================================
# Top-level config bundle
# =============================================================================

@dataclass(frozen=True)
class AppConfig:
    """
    Purpose:
        Single object every ingestion script depends on: secrets +
        validated runtime config + the version constants, bundled
        together so a script only needs one `load_config(...)` call at
        startup.
    Inputs:
        None directly — populated via `AppConfig.load`.
    Outputs:
        N/A (data container).
    Failure modes:
        None on its own; construction failures are raised by `load`.
    """
    secrets: Secrets
    runtime: RuntimeConfig
    schema_version: int = SCHEMA_VERSION
    taxonomy_version: int = TAXONOMY_VERSION
    normalization_version: int = NORMALIZATION_VERSION
    rpc_version: int = RPC_VERSION
    parser_version: int = PARSER_VERSION

    @classmethod
    def load(
        cls,
        system_config_rows: Iterable[Mapping[str, str]],
        env: Mapping[str, str] | None = None,
    ) -> "AppConfig":
        """
        Purpose:
            Single startup entry point: validates secrets and
            system_config together and returns one immutable config
            object, or raises before any ingestion work begins.
        Inputs:
            system_config_rows: rows fetched from the `system_config`
                table by the caller (see `RuntimeConfig.from_rows`).
            env: optional environment mapping override (tests only).
        Outputs:
            A populated `AppConfig`.
        Failure modes:
            Raises `ConfigError` (propagated from `Secrets.from_env` or
            `RuntimeConfig.from_rows`) if either secrets or runtime
            config are missing or invalid. Callers should not catch
            this — per §17, a script should fail fast and exit
            nonzero, not proceed with partial configuration.

            Note: this does NOT validate that the live schema has the
            tables/RPCs the pipeline needs, nor that it's compatible
            with `schema_version`/`parser_version`/etc. above — that is
            Step 13B's job, layered on top of a successfully loaded
            `AppConfig`, not folded into this method.
        """
        return cls(
            secrets=Secrets.from_env(env),
            runtime=RuntimeConfig.from_rows(system_config_rows),
        )

    def version_summary(self) -> dict[str, int]:
        """
        Purpose:
            Instance-level convenience wrapper around the module-level
            `all_versions()`, for callers that already hold an
            `AppConfig` and want the version set without a second
            import.
        Inputs:
            None (uses self).
        Outputs:
            Same shape as `all_versions()`: dict with keys
            "schema_version", "taxonomy_version",
            "normalization_version", "rpc_version", "parser_version".
        Failure modes:
            None.
        """
        return {
            "schema_version": self.schema_version,
            "taxonomy_version": self.taxonomy_version,
            "normalization_version": self.normalization_version,
            "rpc_version": self.rpc_version,
            "parser_version": self.parser_version,
        }
