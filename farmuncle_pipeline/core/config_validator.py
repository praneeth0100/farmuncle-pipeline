"""
FarmUncle v2 — config_validator.py
Phase C, Step 13B.

Purpose (module-level):
    Answers the question `config.py` explicitly declines to answer:
    "is the LIVE Supabase project actually the database this codebase
    expects?" `config.py` validates secrets and `system_config` rows
    in isolation (well-formed, internally consistent); it never talks
    to Supabase. This module is the layer on top: it inspects the live
    schema — tables, RPC functions and their signatures, and the two
    version-identity RPCs — and raises before any ingestion script
    (live_tick, daily_rewrite, historical_backfill, retry_failed_pages,
    weekly_compress) does real work, per Master Build Specification
    §17 ("every script fails fast ... if a dependent table doesn't
    exist, RPC/schema version mismatch").

Explicitly out of scope for this file:
    - Loading secrets or system_config values (config.py's job —
      this module receives an already-loaded `Secrets`/`AppConfig`)
    - Ingestion, batching, retry, or logging execution
    - Identity resolution or any other business logic
    - Any Postgres INSERT/UPDATE/DELETE — every check in this module
      is read-only: HTTP GET against PostgREST's self-describing
      OpenAPI document, plus (for exactly two RPCs, see below) a POST
      call to functions verified side-effect-free at the SQL level.

How table/RPC existence is checked, and why:
    There is no Postgres superuser connection available at runtime —
    production scripts only hold `SUPABASE_SERVICE_KEY`, a PostgREST
    API key, not a raw `psql` connection string (§17's secret list has
    no `SUPABASE_DB_PASSWORD` or similar). So this module cannot query
    `information_schema` or `pg_proc` directly. Instead it fetches
    PostgREST's own auto-generated OpenAPI document from
    `GET {SUPABASE_URL}/rest/v1/` (PostgREST's long-standing
    self-description endpoint — every exposed table becomes a
    `definitions`/path entry, every exposed function becomes a
    `/rpc/<name>` path) and treats that document as the live schema's
    ground truth. This is pure read-only introspection: nothing in
    this module ever calls `find_or_create_mandi`, `find_or_create_crop`,
    or `merge_entity` — calling those for real to "check they exist"
    would create real rows (as Phase B's Step 9 build log notes
    happened by accident once already), which is exactly the kind of
    business-logic side effect this module must never cause.

    Two RPCs ARE actually invoked (via POST, real network calls):
    `current_normalization_version()` and `rpc_version_identity()`.
    Both were inspected directly against the live database
    (`pg_get_functiondef`) before being called here and confirmed to
    be `LANGUAGE sql IMMUTABLE`, single-statement `SELECT <literal>`
    bodies with no table access and no side effects — calling them is
    equivalent to reading a constant, not "doing business logic".
    They are the live schema's only mechanism for asserting
    version compatibility (see "Known deviations" below), so calling
    them is unavoidable if that compatibility is to be checked at all.

Known deviations from the Master Build Specification, observed
against the live project (wqccgjmvslevkglfkmtc) and deliberately
followed here rather than guessed around, per the instruction to
treat the live schema as source of truth:

    1. `price_cache` (table) and `refresh_price_cache` (RPC) are
       listed in spec §6.1 / §13 but do NOT exist in the live schema
       as of this writing — Phase A/B's build logs never built them
       (only Phase A-B's 17 tables + 3 mutating RPCs + 4 normalize
       RPCs + 3 helper RPCs were built). They are intentionally
       EXCLUDED from `_REQUIRED_TABLES` / `_REQUIRED_RPCS` below. Add
       them once a future migration actually creates them — do not
       add them speculatively, or this validator will permanently
       fail against a live schema that (correctly, for its current
       phase) doesn't have them yet.

    2. Spec §3 defines five version columns (`schema_version`,
       `taxonomy_version`, `normalization_version`, `rpc_version`,
       `parser_version`). The live schema only exposes live-identity
       RPCs for two of them (`current_normalization_version`,
       `rpc_version_identity`) — there is no
       `current_schema_version()` / `current_taxonomy_version()` /
       `current_parser_version()` equivalent. `schema_version` is
       instead checked indirectly, by confirming every table in
       `_REQUIRED_TABLES` is present (§21's migration-per-table build
       log is the closest live proxy for "schema_version" available
       over the REST API). `taxonomy_version` and `parser_version`
       have NO live-schema check available at all — they are per-row
       values stamped by writers (see config.py's version-constants
       comment), not schema-level facts, so there is nothing for this
       module to introspect. This gap is real, not an oversight; the
       replay-from-raw invariant (§1, invariant 10) is v2's actual
       safety net for those two, not a startup check.

    3. Applied-migrations count (`list_migrations`) is only visible
       through Supabase's Management API, which requires a personal
       access token — not one of the §17 GitHub Secrets available to
       a running ingestion script. This module cannot and does not
       attempt to verify migration history at runtime; table/RPC
       presence (checked here) is the runtime-visible proxy for it.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Final, Mapping

import requests

from farmuncle_pipeline.config import AppConfig, ConfigError, NORMALIZATION_VERSION, RPC_VERSION, Secrets

# =============================================================================
# Required live-schema surface (Phase A + Phase B only — see "Known
# deviations" #1 above for what is deliberately excluded)
# =============================================================================

_REQUIRED_TABLES: Final[tuple[str, ...]] = (
    "raw_api_batches",
    "raw_api_records",
    "mandis",
    "crops",
    "mandi_aliases",
    "crop_aliases",
    "entity_history",
    "audit_events",
    "system_config",
    "ingestion_batches",
    "failed_pages",
    "api_call_logs",
    "mandi_daily_prices",
    "compression_runs",
    "historical_jobs",
    "coverage_reports",
    "quality_alerts",
)

# RPC name -> the exact set of named parameters `pg_get_function_identity_
# arguments` reports live, verified directly against wqccgjmvslevkglfkmtc
# for every entry below. A live signature that has extra, missing, or
# renamed parameters relative to this set is exactly the "RPC version
# mismatch" §17 asks startup validation to catch.
_REQUIRED_RPCS: Final[Mapping[str, frozenset[str]]] = {
    "find_or_create_mandi": frozenset(
        {"p_name", "p_state", "p_district", "p_lat", "p_lng", "p_source"}
    ),
    "find_or_create_crop": frozenset({"p_name", "p_unit", "p_source"}),
    "merge_entity": frozenset(
        {
            "p_entity_type",
            "p_source_id",
            "p_target_id",
            "p_reason",
            "p_merge_method",
            "p_merge_confidence",
            "p_created_by",
        }
    ),
    "normalize_market_name": frozenset({"p_name"}),
    "normalize_crop_name": frozenset({"p_name"}),
    "normalize_unit": frozenset({"p_unit"}),
    "normalize_variety": frozenset({"p_variety"}),
    # Zero-argument helper/identity RPCs — still required to exist.
    "current_normalization_version": frozenset(),
    "rpc_version_identity": frozenset(),
}

_DEFAULT_TIMEOUT_SECONDS: Final[float] = 10.0


# =============================================================================
# Result container
# =============================================================================

@dataclass(frozen=True)
class SchemaValidationResult:
    """
    Purpose:
        Successful-outcome record returned by `validate_live_schema`.
        Exists so callers that want to log/print what was confirmed
        (rather than just catching the absence of an exception) have
        something to inspect, without this module doing any logging
        itself (logging is explicitly out of scope — see module
        docstring).
    Inputs:
        N/A (data container).
    Outputs:
        N/A (data container).
    Failure modes:
        None.
    """
    tables_checked: tuple[str, ...]
    rpcs_checked: tuple[str, ...]
    live_normalization_version: int
    live_rpc_version: int


# =============================================================================
# OpenAPI introspection (read-only)
# =============================================================================

def _fetch_openapi_spec(secrets: Secrets, timeout: float) -> dict:
    """
    Purpose:
        Fetch PostgREST's self-generated OpenAPI/Swagger document from
        the live project's REST root. This is the only mechanism this
        module uses to learn what tables and RPCs actually exist —
        see the module docstring's "How table/RPC existence is
        checked" section for why.
    Inputs:
        secrets: a loaded `Secrets` instance (needs `supabase_url` and
            `supabase_service_key`).
        timeout: request timeout in seconds.
    Outputs:
        The parsed JSON document as a dict.
    Failure modes:
        Raises `ConfigError` if the request fails outright (network
        error, non-200 status, or a response body that isn't valid
        JSON) — a live schema that can't be introspected at all is
        itself a startup-blocking condition, per §17.
    """
    url = f"{secrets.supabase_url.rstrip('/')}/rest/v1/"
    headers = {
        "apikey": secrets.supabase_service_key,
        "Authorization": f"Bearer {secrets.supabase_service_key}",
        "Accept": "application/openapi+json",
    }
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        raise ConfigError(
            f"Could not reach Supabase REST root ({url}) to validate the "
            f"live schema: {exc}. Startup validation cannot proceed "
            "without confirming the live schema matches this codebase."
        ) from exc

    if response.status_code != 200:
        raise ConfigError(
            f"Supabase REST root ({url}) returned HTTP "
            f"{response.status_code} instead of the expected OpenAPI "
            "document. Cannot validate the live schema; check "
            "SUPABASE_URL / SUPABASE_SERVICE_KEY are correct and the "
            "project is not paused."
        )

    try:
        return response.json()
    except ValueError as exc:
        raise ConfigError(
            f"Supabase REST root ({url}) returned a 200 response that "
            f"was not valid JSON: {exc}. Cannot validate the live schema."
        ) from exc


def _extract_rpc_param_names(spec: dict, rpc_name: str) -> set[str] | None:
    """
    Purpose:
        Best-effort extraction of an RPC's parameter names from the
        OpenAPI document, tolerant of the two shapes PostgREST has
        historically emitted (Swagger 2.0's `parameters`/`definitions`
        pair, and OpenAPI 3's `requestBody`/`schema` pair), since
        which shape a given Supabase project serves is not something
        this module controls or can assume without observing it live.
    Inputs:
        spec: the parsed OpenAPI document from `_fetch_openapi_spec`.
        rpc_name: the function name, e.g. "find_or_create_mandi".
    Outputs:
        The set of parameter names found, or `None` if the path exists
        but this module could not determine its shape confidently
        enough to extract parameters (existence is still checked
        separately and does not depend on this).
    Failure modes:
        None — returns `None` rather than raising on an unrecognized
        shape, since full signature introspection is best-effort on
        top of the hard existence check (see `_check_rpcs`).
    """
    path_entry = spec.get("paths", {}).get(f"/rpc/{rpc_name}")
    if not isinstance(path_entry, dict):
        return None
    post_entry = path_entry.get("post")
    if not isinstance(post_entry, dict):
        return None

    # OpenAPI 3 shape: requestBody.content.application/json.schema.properties
    request_body = post_entry.get("requestBody")
    if isinstance(request_body, dict):
        schema = (
            request_body.get("content", {})
            .get("application/json", {})
            .get("schema", {})
        )
        properties = schema.get("properties")
        if isinstance(properties, dict):
            return set(properties.keys())

    # Swagger 2.0 shape: parameters: [{in: body, schema: {$ref: "#/definitions/..."}}]
    parameters = post_entry.get("parameters")
    if isinstance(parameters, list):
        for param in parameters:
            if not isinstance(param, dict) or param.get("in") != "body":
                continue
            ref = param.get("schema", {}).get("$ref", "")
            def_name = ref.rsplit("/", 1)[-1] if ref else ""
            definition = spec.get("definitions", {}).get(def_name)
            if isinstance(definition, dict):
                properties = definition.get("properties")
                if isinstance(properties, dict):
                    return set(properties.keys())

    return None


def _check_tables(spec: dict) -> list[str]:
    """
    Purpose:
        Confirm every table in `_REQUIRED_TABLES` is present in the
        live OpenAPI document's `definitions` (Swagger 2.0) or
        `components.schemas` (OpenAPI 3) — whichever the live project
        serves.
    Inputs:
        spec: the parsed OpenAPI document.
    Outputs:
        List of human-readable error strings, one per missing table.
        Empty list means every required table was found.
    Failure modes:
        None — never raises; the caller (`validate_live_schema`)
        collects and raises once, per config.py's "collect every
        problem before raising" convention.
    """
    known_names = set(spec.get("definitions", {}).keys())
    known_names |= set(spec.get("components", {}).get("schemas", {}).keys())
    # Some PostgREST versions also list every table as a top-level path
    # (e.g. "/mandis"); fall back to that if `definitions` came back
    # empty for some reason, rather than false-failing everything.
    if not known_names:
        known_names = {
            p.lstrip("/") for p in spec.get("paths", {}).keys()
            if not p.startswith("/rpc/")
        }

    errors = []
    for table in _REQUIRED_TABLES:
        if table not in known_names:
            errors.append(
                f"Required table '{table}' was not found in the live "
                "Supabase schema (checked via PostgREST's OpenAPI "
                "document). Either it was never created, or the service "
                "key does not have SELECT exposed on it."
            )
    return errors


def _check_rpcs(spec: dict) -> list[str]:
    """
    Purpose:
        Confirm every RPC in `_REQUIRED_RPCS` exists as a `/rpc/<name>`
        path, and — where extractable — that its parameter names match
        exactly what this codebase expects.
    Inputs:
        spec: the parsed OpenAPI document.
    Outputs:
        List of human-readable error strings. Empty list means every
        required RPC was found with a matching (or unverifiable but
        present) signature.
    Failure modes:
        None — never raises; see `_check_tables`.
    """
    errors = []
    paths = spec.get("paths", {})
    for rpc_name, expected_params in _REQUIRED_RPCS.items():
        if f"/rpc/{rpc_name}" not in paths:
            errors.append(
                f"Required RPC '{rpc_name}' was not found in the live "
                f"Supabase schema (no /rpc/{rpc_name} path in the "
                "OpenAPI document). Either it was never created, or "
                "EXECUTE was not granted to a role this service key "
                "resolves to."
            )
            continue

        actual_params = _extract_rpc_param_names(spec, rpc_name)
        if actual_params is None:
            # Path exists but this module couldn't confidently parse its
            # parameter shape (see _extract_rpc_param_names docstring).
            # Existence is confirmed; signature is simply unverified,
            # not treated as a failure.
            continue

        if actual_params != expected_params:
            missing = expected_params - actual_params
            extra = actual_params - expected_params
            detail_parts = []
            if missing:
                detail_parts.append(f"missing {sorted(missing)}")
            if extra:
                detail_parts.append(f"unexpected extra {sorted(extra)}")
            errors.append(
                f"RPC '{rpc_name}' signature mismatch: "
                f"{'; '.join(detail_parts)}. This codebase expects "
                f"exactly {sorted(expected_params)}."
            )
    return errors


def _check_version_identity(secrets: Secrets, timeout: float) -> tuple[list[str], int, int]:
    """
    Purpose:
        Call the two live version-identity RPCs
        (`current_normalization_version`, `rpc_version_identity`) and
        compare their results against this codebase's
        `NORMALIZATION_VERSION` / `RPC_VERSION` constants (config.py
        §3). This is the one place this module performs real POST
        requests against RPC endpoints — see the module docstring for
        why that's still "read-only" in effect.
    Inputs:
        secrets: a loaded `Secrets` instance.
        timeout: request timeout in seconds.
    Outputs:
        A 3-tuple: (list of error strings, live_normalization_version,
        live_rpc_version). The two integers are 0 if the corresponding
        call failed outright (an error string will explain why; 0 is
        never treated as a real version value by the caller).
    Failure modes:
        None raised directly — network/HTTP problems are converted
        into error strings in the returned list, consistent with
        `_check_tables` / `_check_rpcs`, so `validate_live_schema` can
        report every problem in one pass.
    """
    headers = {
        "apikey": secrets.supabase_service_key,
        "Authorization": f"Bearer {secrets.supabase_service_key}",
        "Content-Type": "application/json",
    }
    base = secrets.supabase_url.rstrip("/")
    errors: list[str] = []
    live_normalization_version = 0
    live_rpc_version = 0

    for rpc_name, expected, attr_name in (
        ("current_normalization_version", NORMALIZATION_VERSION, "normalization_version"),
        ("rpc_version_identity", RPC_VERSION, "rpc_version"),
    ):
        try:
            response = requests.post(
                f"{base}/rest/v1/rpc/{rpc_name}", headers=headers, json={}, timeout=timeout
            )
        except requests.RequestException as exc:
            errors.append(
                f"Could not call live version-identity RPC '{rpc_name}': {exc}"
            )
            continue

        if response.status_code != 200:
            errors.append(
                f"Live version-identity RPC '{rpc_name}' returned HTTP "
                f"{response.status_code} instead of 200: {response.text[:200]}"
            )
            continue

        try:
            live_value = int(response.json())
        except (ValueError, TypeError) as exc:
            errors.append(
                f"Live version-identity RPC '{rpc_name}' did not return "
                f"an integer: {exc}"
            )
            continue

        if attr_name == "normalization_version":
            live_normalization_version = live_value
        else:
            live_rpc_version = live_value

        if live_value != expected:
            errors.append(
                f"Version mismatch on '{rpc_name}': live schema reports "
                f"{live_value}, but this codebase's config.py expects "
                f"{expected}. Either config.py's constant is stale, or "
                "the live schema was migrated without updating it — "
                "per Never-Do Rules §2, these must never drift apart "
                "silently."
            )

    return errors, live_normalization_version, live_rpc_version


# =============================================================================
# Public entry point
# =============================================================================

def validate_live_schema(
    secrets: Secrets,
    timeout: float = _DEFAULT_TIMEOUT_SECONDS,
) -> SchemaValidationResult:
    """
    Purpose:
        Single startup entry point for this module: confirms the live
        Supabase project has every table and RPC this codebase depends
        on (with matching RPC signatures where verifiable), and that
        the live schema's version identity matches config.py's version
        constants. Intended to be called once, immediately after
        `AppConfig.load(...)` succeeds, before any ingestion script
        does real work.
    Inputs:
        secrets: a loaded `Secrets` instance (this module does not
            load secrets itself — see module docstring).
        timeout: per-request timeout in seconds, shared by all HTTP
            calls this function makes. Defaults to 10s.
    Outputs:
        A `SchemaValidationResult` on success.
    Failure modes:
        Raises `ConfigError` listing every problem found across all
        three checks (tables, RPCs, version identity) in one pass —
        not just the first one — mirroring config.py's
        `RuntimeConfig.from_rows` convention, so a misconfigured or
        under-migrated environment can be fixed without repeated
        round-trips. Also raises `ConfigError` (from
        `_fetch_openapi_spec`) if the live schema cannot be
        introspected at all, before any of the three checks can run.
    """
    spec = _fetch_openapi_spec(secrets, timeout)

    errors: list[str] = []
    errors.extend(_check_tables(spec))
    errors.extend(_check_rpcs(spec))
    version_errors, live_normalization_version, live_rpc_version = _check_version_identity(
        secrets, timeout
    )
    errors.extend(version_errors)

    if errors:
        raise ConfigError(
            "Live Supabase schema validation failed with "
            f"{len(errors)} problem(s):\n- " + "\n- ".join(errors)
        )

    return SchemaValidationResult(
        tables_checked=_REQUIRED_TABLES,
        rpcs_checked=tuple(_REQUIRED_RPCS.keys()),
        live_normalization_version=live_normalization_version,
        live_rpc_version=live_rpc_version,
    )


# =============================================================================
# Manual / CI smoke-test entry point
# =============================================================================

def _fetch_system_config_rows(secrets: Secrets, timeout: float) -> list[dict]:
    """
    Purpose:
        Fetch `system_config` rows over REST for the `__main__` smoke
        test below, so a human can run
        `python config_validator.py` end-to-end (secrets + runtime
        config + live schema) without wiring up a full ingestion
        script first. Not used by `validate_live_schema` itself, and
        not imported by other Phase C modules — this is a standalone
        convenience, not a shared code path.
    Inputs:
        secrets: a loaded `Secrets` instance.
        timeout: request timeout in seconds.
    Outputs:
        List of {"key": ..., "value": ...} dicts.
    Failure modes:
        Raises `ConfigError` on any HTTP/network failure, consistent
        with the rest of this module.
    """
    headers = {
        "apikey": secrets.supabase_service_key,
        "Authorization": f"Bearer {secrets.supabase_service_key}",
    }
    url = f"{secrets.supabase_url.rstrip('/')}/rest/v1/system_config?select=key,value"
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        raise ConfigError(f"Could not fetch system_config rows: {exc}") from exc


if __name__ == "__main__":
    try:
        _secrets = Secrets.from_env()
        _rows = _fetch_system_config_rows(_secrets, _DEFAULT_TIMEOUT_SECONDS)
        _app_config = AppConfig.load(_rows)
        _result = validate_live_schema(_secrets)
    except ConfigError as exc:
        print(f"CONFIG VALIDATION FAILED:\n{exc}", file=sys.stderr)
        sys.exit(1)

    print("Config validation passed.")
    print(f"  Tables checked: {len(_result.tables_checked)}")
    print(f"  RPCs checked: {len(_result.rpcs_checked)}")
    print(f"  Live normalization_version: {_result.live_normalization_version}")
    print(f"  Live rpc_version: {_result.live_rpc_version}")
    print(f"  Codebase version summary: {_app_config.version_summary()}")
