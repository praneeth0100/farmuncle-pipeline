"""
FarmUncle v2 — resource_client.py
Phase C, Step 15 (extracted from Step 14's `live_tick.py` — see note
below on why this extraction happens now, not at Step 14).

Purpose (module-level):
    The single §9 "explicit `ok` flag" page-fetcher, shared by every
    ingestion script that pages through a government resource
    (`live_tick.py` / Resource 1, `daily_rewrite.py` / Resource 2, and
    `historical_backfill.py` — Step 16 — later). This module contains
    NO resource-specific logic (no knowledge of Resource 1's
    date-offset pagination vs Resource 2's per-state pagination, no
    field-name parsing) — it only knows how to fetch one page, given a
    fully-formed URL/params/headers, retrying on transient failures
    and reporting an explicit `ok` flag rather than conflating
    "failed after retries" with "genuine end of pagination" (the exact
    bug §9 calls out).

Why this wasn't its own module at Step 14:
    At Step 14, `fetch_page`/`PageFetchResult` had exactly one caller
    (`live_tick.py`), and pulling out a single-caller helper
    speculatively — before a second real caller existed — would have
    been exactly the kind of premature abstraction the Step 13/14
    build sessions explicitly avoided elsewhere (see `batch_lifecycle.py`
    /`identity_client.py`/`quality_scoring.py`, which WERE pulled out at
    Step 14 specifically because Steps 15+ were already known to need
    them). Now that `daily_rewrite.py` (Step 15) needs the identical
    retry/backoff/explicit-ok-flag behavior against a different URL,
    the second caller exists, so per Never-Do Rule §2 ("no business
    logic duplicated across scripts") this is pulled out for real
    rather than copy-pasted into `daily_rewrite.py`.

    `live_tick.py` (Step 14) is updated to import `fetch_page`/
    `PageFetchResult` from here instead of defining them locally. This
    is a pure extraction — the function body is unchanged byte-for-
    byte from the version verified in Step 14's production audit — not
    a rewrite of Step 14's already-approved logic.

Explicitly out of scope for this file:
    - Anything resource-specific: date formatting, per-state vs
      per-date pagination strategy, field parsing, raw_api_records
      writes, failed_pages writes. All of that stays in the calling
      script (`live_tick.py` / `daily_rewrite.py`), which knows which
      resource it's talking to.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import requests


@dataclass(frozen=True)
class PageFetchResult:
    """
    Purpose:
        Result of one page fetch attempt (including its internal
        retries). The `ok` field is the explicit fix §9 calls for:
        "failed-after-retries currently indistinguishable from genuine
        end [of pagination] — must fix in v2's fetch_page (explicit ok
        flag)." `ok=False` means every retry was exhausted and this
        page's contents are unknown; `ok=True` with `records` shorter
        than the page size means the pagination genuinely ended here.
        These are never conflated by a caller that checks `ok` first.
    """
    ok: bool
    records: list
    raw_response: dict
    error: str | None
    duration_ms: int


def fetch_page(
    *,
    url: str,
    params: dict,
    headers: dict,
    timeout: int,
    max_retries: int,
    retry_delay_seconds: int,
) -> PageFetchResult:
    """
    Purpose:
        Fetch one page of a government resource's results, retrying on
        timeout or connection error up to `max_retries` additional
        times, with linear backoff (`retry_delay_seconds *
        attempt_number`). Resource-agnostic: works identically for
        Resource 1 (`live_tick.py`) or Resource 2 (`daily_rewrite.py`)
        since both are plain paginated JSON endpoints differing only
        in their URL/params, which the caller supplies.
    Inputs:
        url: the resource's base URL (from `system_config` via
            `ctx.app_config.runtime` — never hardcoded).
        params: query parameters — passed through to `requests` as a
            dict, not string-concatenated, so `&`/special characters in
            values (e.g. a commodity named "F&V") are encoded correctly
            (§9).
        headers: `DEFAULT_HTTP_HEADERS`.
        timeout / max_retries / retry_delay_seconds: from
            `ctx.app_config.runtime` (system_config-driven, not
            hardcoded).
    Outputs:
        `PageFetchResult`. Never raises for ordinary network/HTTP
        failures — those are captured in `ok=False` / `error` so the
        caller can log and record a `failed_pages` row instead of the
        process crashing mid-run.
    Failure modes:
        None raised directly.
    """
    last_error: str | None = None
    attempts = max(max_retries, 0) + 1
    start = time.perf_counter()

    for attempt in range(attempts):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            duration_ms = int((time.perf_counter() - start) * 1000)
            return PageFetchResult(
                ok=True,
                records=data.get("records", []),
                raw_response=data,
                error=None,
                duration_ms=duration_ms,
            )
        except requests.exceptions.Timeout as exc:
            last_error = f"timeout: {exc}"
        except requests.exceptions.RequestException as exc:
            last_error = f"request error: {exc}"
        except ValueError as exc:  # response.json() failed to parse
            last_error = f"invalid JSON response: {exc}"

        if attempt < attempts - 1:
            time.sleep(retry_delay_seconds * (attempt + 1))

    duration_ms = int((time.perf_counter() - start) * 1000)
    return PageFetchResult(
        ok=False, records=[], raw_response={}, error=last_error, duration_ms=duration_ms
    )


# =============================================================================
# Record parsing — shared between Resource 1 and Resource 2
#
# Extracted from `live_tick.py`'s `parse_resource_1_record` (Step 14),
# despite the "resource_1" name it had there: the actual field names
# and `DD/MM/YYYY` date format are a fact about the government API's
# shared response contract (see both v1 reference scripts), not
# something specific to Resource 1. `daily_rewrite.py` (Step 15) needs
# byte-for-byte the same parsing for Resource 2 records, so this is
# pulled out here rather than duplicated. `live_tick.py` imports this
# under its original name (`parse_resource_1_record`) via an alias, so
# none of its own call sites needed to change.
# =============================================================================
def parse_agmarknet_record(rec: dict) -> dict | None:
    """
    Purpose:
        Extract and lightly type-convert the fields this pipeline
        needs from one raw government-API record (Resource 1 and
        Resource 2 do NOT share the same field-name casing in practice
        — Resource 1 uses lowercase keys like "commodity", Resource 2
        uses Title_Case keys like "Commodity", "Arrival_Date",
        "Modal_Price", confirmed against the live API's own field
        metadata on 2026-07-13. This function looks fields up
        case-insensitively so it works correctly for either resource).
        The `DD/MM/YYYY` date format is the government API's own
        contract, not a per-resource design decision.
    Inputs:
        rec: one record dict from a resource's `records` list.
    Outputs:
        A dict with keys commodity/market/state/district/raw_variety/
        price_date/modal_price/min_price/max_price, or `None` if a
        required field (commodity, market, state, or a parseable
        arrival_date) is missing — callers count `None` results as
        skipped/failed rows.
    Failure modes:
        None raised — malformed input produces `None`, not an
        exception, since one bad row in a page of 500 should not abort
        the page.
    """
    rec_ci = {str(k).lower(): v for k, v in rec.items()}

    commodity = (rec_ci.get("commodity") or "").strip()
    market = (rec_ci.get("market") or "").strip()
    state = (rec_ci.get("state") or "").strip()
    district = (rec_ci.get("district") or "").strip() or None
    raw_variety = (rec_ci.get("variety") or "").strip()
    arrival_date = rec_ci.get("arrival_date")

    if not commodity or not market or not state or not arrival_date:
        return None

    try:
        day, month, year = str(arrival_date).split("/")
        price_date = f"{year}-{month}-{day}"
    except (ValueError, AttributeError):
        return None

    def _parse_price(value) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    return {
        "commodity": commodity,
        "market": market,
        "state": state,
        "district": district,
        "raw_variety": raw_variety,
        "price_date": price_date,
        "modal_price": _parse_price(rec_ci.get("modal_price")),
        "min_price": _parse_price(rec_ci.get("min_price")),
        "max_price": _parse_price(rec_ci.get("max_price")),
    }