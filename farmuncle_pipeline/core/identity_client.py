"""
FarmUncle v2 — identity_client.py
Phase C, Step 14 (shared helper — daily_rewrite.py, Step 15, needs
identical mandi/crop/variety/unit resolution against Resource 2 data).

Purpose (module-level):
    Thin, memoized wrapper around the four identity/normalization RPCs
    every ingestion script needs: `find_or_create_mandi`,
    `find_or_create_crop`, `normalize_variety`, `normalize_unit`. Per
    invariant 3 ("All identity resolution happens through RPCs — never
    duplicated in Python") and invariant 9 ("No business logic
    duplicated across scripts"), this module contains NO resolution or
    normalization logic of its own — every decision about what a
    market/crop/variety/unit name canonically means is made server-
    side, inside the RPC. This module only calls those RPCs and adds
    process-local memoization, so a run with (for example) 400 price
    rows from 40 distinct mandis doesn't make 400 RPC round-trips for
    mandi resolution when 40 would do.

"First seen this run" signal:
    `find_or_create_mandi`/`find_or_create_crop` return only an id —
    there is no live-schema signal for "was this row actually just
    INSERTed vs already existed" (adding one would mean changing an
    already-built, already-tested Phase A RPC, which is out of scope
    for a Phase C ingestion script and not something to do
    speculatively). What this module CAN report, honestly, is whether
    an id was resolved via this run's own in-memory cache or required
    an RPC call — i.e. "first time this script has seen this name in
    THIS run", not "this entity was newly created in the database".
    `quality_scoring.py` uses this as an imperfect but real proxy
    (an entity resolved repeatedly within one run is actively trading
    at multiple price points right now; one seen only once is more
    likely an edge case) — see that module's docstring for how it's
    used, and this caveat for what it actually means.

Explicitly out of scope for this file:
    - Any resolution/normalization logic (lives entirely in the RPCs)
    - Batch lifecycle (`batch_lifecycle.py`)
    - Quality scoring (`quality_scoring.py`)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from farmuncle_pipeline.config import ConfigError, Source

if TYPE_CHECKING:
    from supabase import Client


@dataclass(frozen=True)
class ResolvedEntity:
    """Purpose: an entity id plus whether resolving it required an RPC
    call in this run (cache miss) or was served from this run's own
    memoization (cache hit). See module docstring's "First seen this
    run" section for what `first_seen_this_run` does and does not mean."""
    id: int
    first_seen_this_run: bool


class IdentityClient:
    """
    Purpose:
        One instance per script run, holding that run's memoization
        caches. Deliberately NOT a module-level global cache — a
        long-running process reusing stale ids across unrelated runs
        would be a correctness risk (e.g. across a merge_entity call
        made between runs), and each ingestion script invocation is a
        short-lived process anyway (GitHub Actions runners are fully
        ephemeral, per spec §10 assumption 5), so there's no benefit
        to caching beyond one run's lifetime.
    """

    def __init__(self, client: "Client") -> None:
        self._client = client
        self._mandi_cache: dict[tuple[str, str, str | None], int] = {}
        self._crop_cache: dict[str, int] = {}
        self._variety_cache: dict[str, str] = {}
        self._unit_cache: dict[str, str] = {}

    def resolve_mandi(
        self,
        *,
        name: str,
        state: str,
        district: str | None,
        source: Source,
        latitude: float | None = None,
        longitude: float | None = None,
    ) -> ResolvedEntity:
        """
        Purpose:
            Resolve (or create) a canonical mandi id via
            `find_or_create_mandi`, memoized per (name, state) for the
            life of this `IdentityClient`.
        Inputs:
            name / state / district: as reported by the government API
                for this row.
            source: `Source.RESOURCE_1` / `Source.RESOURCE_2` /
                `Source.MANUAL` — passed through as `p_source`.
            latitude / longitude: optional, if the resource provides
                them (Resource 1/2 government feeds do not; reserved
                for a future resource that does).
        Outputs:
            `ResolvedEntity`.
        Failure modes:
            Raises `ConfigError` if the RPC call itself fails.
        """
        key = (name, state, district)
        if key in self._mandi_cache:
            return ResolvedEntity(id=self._mandi_cache[key], first_seen_this_run=False)

        try:
            result = self._client.rpc(
                "find_or_create_mandi",
                {
                    "p_name": name,
                    "p_state": state,
                    "p_district": district,
                    "p_lat": latitude,
                    "p_lng": longitude,
                    "p_source": source.value,
                },
            ).execute()
        except Exception as exc:
            raise ConfigError(
                f"find_or_create_mandi failed (name={name!r}, state={state!r}): {exc}"
            ) from exc

        mandi_id = result.data
        self._mandi_cache[key] = mandi_id
        return ResolvedEntity(id=mandi_id, first_seen_this_run=True)

    def resolve_crop(self, *, name: str, unit: str, source: Source) -> ResolvedEntity:
        """
        Purpose:
            Resolve (or create) a canonical crop id via
            `find_or_create_crop`, memoized per commodity name for the
            life of this `IdentityClient`.
        Inputs:
            name: commodity name as reported by the government API.
            unit: passed through as `p_unit` (already normalized by
                the caller via `normalize_unit`, see `resolve_unit`
                below).
            source: `Source.RESOURCE_1` / `Source.RESOURCE_2` /
                `Source.MANUAL`.
        Outputs:
            `ResolvedEntity`.
        Failure modes:
            Raises `ConfigError` if the RPC call itself fails.
        """
        if name in self._crop_cache:
            return ResolvedEntity(id=self._crop_cache[name], first_seen_this_run=False)

        try:
            result = self._client.rpc(
                "find_or_create_crop",
                {"p_name": name, "p_unit": unit, "p_source": source.value},
            ).execute()
        except Exception as exc:
            raise ConfigError(f"find_or_create_crop failed (name={name!r}): {exc}") from exc

        crop_id = result.data
        self._crop_cache[name] = crop_id
        return ResolvedEntity(id=crop_id, first_seen_this_run=True)

    def resolve_variety(self, raw_variety: str) -> str:
        """
        Purpose:
            Normalize a variety string via the `normalize_variety` RPC,
            memoized per raw input string. `mandi_daily_prices.variety`
            is a plain text column (not FK'd to a canonical table), but
            it still participates in the business-key uniqueness
            constraint, so unnormalized spelling variants (casing,
            whitespace, "Other" vs "other") would silently fragment
            what should be the same row — hence still routing through
            the RPC rather than a Python `.strip()`/`.title()`, per
            invariant 3.
        Inputs:
            raw_variety: variety string as reported by the government
                API (may be blank).
        Outputs:
            Normalized variety string.
        Failure modes:
            Raises `ConfigError` if the RPC call fails.
        """
        if raw_variety in self._variety_cache:
            return self._variety_cache[raw_variety]

        try:
            result = self._client.rpc(
                "normalize_variety", {"p_variety": raw_variety}
            ).execute()
        except Exception as exc:
            raise ConfigError(
                f"normalize_variety failed (raw_variety={raw_variety!r}): {exc}"
            ) from exc

        normalized = result.data
        self._variety_cache[raw_variety] = normalized
        return normalized

    def resolve_unit(self, raw_unit: str) -> str:
        """
        Purpose:
            Normalize a unit string via the `normalize_unit` RPC,
            memoized per raw input. Per that RPC's own build note
            (Phase A, Step 4), it never silently defaults an unmapped
            unit to "kg" — an unrecognized unit is a real failure this
            function lets propagate, not something to paper over here.
        Inputs:
            raw_unit: unit string (government Resource 1/2 responses
                do not carry a per-row unit field, so callers currently
                pass a fixed default — see `live_tick.py`).
        Outputs:
            Normalized unit string.
        Failure modes:
            Raises `ConfigError` if the RPC call fails (including if
            the RPC itself rejects an unmapped unit).
        """
        if raw_unit in self._unit_cache:
            return self._unit_cache[raw_unit]

        try:
            result = self._client.rpc("normalize_unit", {"p_unit": raw_unit}).execute()
        except Exception as exc:
            raise ConfigError(f"normalize_unit failed (raw_unit={raw_unit!r}): {exc}") from exc

        normalized = result.data
        self._unit_cache[raw_unit] = normalized
        return normalized
