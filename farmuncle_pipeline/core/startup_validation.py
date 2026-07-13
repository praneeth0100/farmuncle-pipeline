"""
FarmUncle v2 — startup_validation.py
Phase C, Step 13 (part 2 of 2 — see also `logging_utils.py`, combined
for import purposes by `ingest_common.py`).

Purpose (module-level):
    The single §17 entry point every ingestion script (live_tick,
    daily_rewrite, historical_backfill, retry_failed_pages,
    weekly_compress) calls exactly once, before doing any real work:
    "every script fails fast before doing any work if a secret is
    missing, DB unreachable, a dependent table doesn't exist, RPC/
    schema version mismatch." This module is the composition point
    that ties together the three modules that individually implement
    each piece of that sentence:
        - `config.Secrets.from_env` / `config.AppConfig.load`  → "a
          secret is missing" / system_config malformed
        - `config_validator.validate_live_schema`              → "DB
          unreachable" / "a dependent table doesn't exist" / "RPC/
          schema version mismatch"
    No validation logic is duplicated here — this module only
    sequences calls into those two and constructs the one Supabase
    client every script needs, so that construction isn't repeated
    (and potentially done inconsistently) in five separate scripts.

Explicitly out of scope for this file:
    - Any of the validation logic itself (lives in `config.py` /
      `config_validator.py` — this module only calls it)
    - Logging (see `logging_utils.py`)
    - Batch lifecycle, retry, identity resolution, or any other
      business logic (Phase C, Step 14+)

Why this module constructs the Supabase client (unlike `config.py`,
which explicitly does not):
    Every ingestion script needs exactly one authenticated client, and
    needs it before it can even fetch `system_config` rows to build an
    `AppConfig`. Building it once here — rather than each script
    calling `create_client(...)` itself — is the same "don't duplicate
    business logic across scripts" principle (Never-Do Rules §2)
    applied to client construction, not just RPC calls.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from supabase import Client, create_client

from farmuncle_pipeline.config import AppConfig, ConfigError, Secrets
from farmuncle_pipeline.core.config_validator import SchemaValidationResult, validate_live_schema


@dataclass(frozen=True)
class StartupContext:
    """
    Purpose:
        Everything a Phase C ingestion script needs to begin real
        work, bundled into one object returned by `validate_startup`.
        A script that has one of these has already passed every §17
        startup check.
    Inputs:
        N/A (data container).
    Outputs:
        N/A (data container).
    Failure modes:
        None on its own; construction failures are raised by
        `validate_startup`, not by this class.
    """
    secrets: Secrets
    app_config: AppConfig
    supabase: Client
    schema_validation: SchemaValidationResult | None


def validate_startup(
    env: Mapping[str, str] | None = None,
    *,
    skip_schema_validation: bool = False,
    schema_validation_timeout: float = 10.0,
) -> StartupContext:
    """
    Purpose:
        Run every §17 startup check in sequence and return a ready-to-
        use `StartupContext`, or raise before any ingestion work
        begins. This is the one function every Phase C script's `main`
        should call first.
    Inputs:
        env: optional environment mapping override (tests only; see
            `Secrets.from_env`). Production callers omit this.
        skip_schema_validation: if True, skips the
            `config_validator.validate_live_schema` network calls
            (OpenAPI fetch + two version-identity RPC calls). Intended
            ONLY for unit tests that already mock `Secrets`/`AppConfig`
            and don't want a real network dependency — a production
            script must never set this True, since doing so defeats
            the exact "RPC/schema version mismatch" check §17 asks
            for.
        schema_validation_timeout: passed through to
            `validate_live_schema`'s `timeout` parameter.
    Outputs:
        A populated `StartupContext`. Its `schema_validation` field is
        `None` only when `skip_schema_validation=True`.
    Failure modes:
        Raises `ConfigError` — propagated from `Secrets.from_env`,
        `AppConfig.load`, the `system_config` fetch, or
        `validate_live_schema`, whichever fails first. Callers should
        not catch this: per §17, a script should fail fast and exit
        nonzero. The sequence deliberately stops at the first failure
        (secrets before system_config before live-schema) rather than
        collecting across all three stages, since a missing secret
        (e.g. no `SUPABASE_URL`) makes the later stages impossible to
        even attempt — this differs from `RuntimeConfig.from_rows` and
        `validate_live_schema`, which each collect every problem
        *within* their own stage before raising.
    """
    secrets = Secrets.from_env(env)

    try:
        supabase = create_client(secrets.supabase_url, secrets.supabase_service_key)
    except Exception as exc:
        raise ConfigError(
            f"Could not construct a Supabase client from SUPABASE_URL / "
            f"SUPABASE_SERVICE_KEY: {exc}"
        ) from exc

    system_config_rows = _fetch_system_config_rows(supabase)
    app_config = AppConfig.load(system_config_rows, env)

    schema_validation: SchemaValidationResult | None = None
    if not skip_schema_validation:
        schema_validation = validate_live_schema(
            secrets, timeout=schema_validation_timeout
        )

    return StartupContext(
        secrets=secrets,
        app_config=app_config,
        supabase=supabase,
        schema_validation=schema_validation,
    )


def _fetch_system_config_rows(supabase: Client) -> list[dict]:
    """
    Purpose:
        Fetch every row of `system_config` via the already-constructed
        client, for `AppConfig.load`. Kept as a private helper (not
        re-exported by `ingest_common.py`) since it's a one-line
        implementation detail of `validate_startup`, not something
        another script should call independently — a script that
        wants `system_config` should go through `validate_startup` and
        read `.app_config.runtime`, not re-fetch it.
    Inputs:
        supabase: an already-constructed Supabase client.
    Outputs:
        List of `{"key": ..., "value": ...}` dicts.
    Failure modes:
        Raises `ConfigError` if the fetch itself fails (network error,
        table missing, RLS blocking the service key) — this is
        deliberately NOT folded into `config_validator`'s table-
        existence check, because `AppConfig.load` needs these rows
        before schema validation even runs (see `validate_startup`'s
        Failure modes on sequencing).
    """
    try:
        response = supabase.table("system_config").select("key,value").execute()
    except Exception as exc:
        raise ConfigError(f"Failed to fetch system_config rows: {exc}") from exc

    if not response.data:
        raise ConfigError(
            "system_config returned zero rows. Expected at least the 8 "
            "seed rows from Step 5b — either the table is empty or the "
            "service key cannot read it."
        )
    return response.data
