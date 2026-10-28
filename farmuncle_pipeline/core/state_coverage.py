"""
FarmUncle v2 — state_coverage.py

Purpose (module-level):
    Catch a state name the pipeline does not query at all. Each state is
    fetched with its own filter, so a renamed state returns zero rows,
    reconciles against a government total of zero, and looks like a
    success. This compares the nationwide `total` for the date (one
    extra one-record request) with the sum of the per-state totals that
    were collected; a real shortfall raises a `quality_alerts` row.

    Not a retry: a missing state is a configuration problem, fixed by
    adding the new spelling to `government_constants.STATE_ALIASES`.
"""

from __future__ import annotations

from farmuncle_pipeline.config import ConfigError, Resource
from farmuncle_pipeline.core.api_params import build_params
from farmuncle_pipeline.core.resource_client import fetch_page

ERROR_CODE = "QUALITY-002"


def gap_size(
    nationwide_total: int | None,
    covered_total: int,
    *,
    tolerance_pct: float = 2.0,
    min_records: int = 25,
) -> int:
    """
    The number of records the nationwide total says exist but the
    per-state queries did not account for, or 0 when that is within
    tolerance. Today's data keeps arriving while a run is in progress
    (the nationwide total is read after the state loop), so a small
    shortfall is normal.
    """
    if nationwide_total is None:
        return 0
    gap = nationwide_total - covered_total
    if gap < min_records or gap <= nationwide_total * tolerance_pct / 100:
        return 0
    return gap


def fetch_nationwide_total(
    *, resource: Resource, api_key: str, date_str: str, url: str, headers: dict,
    timeout: int, max_retries: int, retry_delay_seconds: int,
) -> int | None:
    """The government's own count of records for the date, or None if the call fails."""
    result = fetch_page(
        url=url,
        params=build_params(
            resource=resource, api_key=api_key, page_size=1, offset=0, date_str=date_str
        ),
        headers=headers,
        timeout=timeout,
        max_retries=max_retries,
        retry_delay_seconds=retry_delay_seconds,
    )
    if not result.ok:
        return None
    try:
        return int(result.raw_response.get("total"))
    except (TypeError, ValueError):
        return None


def alert_state_coverage_gap(
    supabase, *, batch_id: str, resource: Resource, date_str: str,
    gap: int, nationwide_total: int, covered_total: int,
) -> bool:
    """
    Write one HIGH `quality_alerts` row per resource and date while none
    is open. Returns True if a row was inserted.
    """
    prefix = f"State coverage gap for {resource.value} on {date_str}"
    try:
        existing = (
            supabase.table("quality_alerts")
            .select("id")
            .eq("status", "OPEN")
            .eq("error_code", ERROR_CODE)
            .like("message", prefix + "%")
            .limit(1)
            .execute()
        )
        if existing.data:
            return False
        supabase.table("quality_alerts").insert(
            {
                "severity": "HIGH",
                "error_code": ERROR_CODE,
                "batch_id": batch_id,
                "message": (
                    f"{prefix}: {gap} of {nationwide_total} records are under a state name "
                    f"this pipeline does not query (queried states account for {covered_total}). "
                    f"A state was probably renamed; add the new spelling to STATE_ALIASES."
                ),
            }
        ).execute()
    except Exception as exc:
        raise ConfigError(f"Failed to write state-coverage quality_alerts row: {exc}") from exc
    return True
