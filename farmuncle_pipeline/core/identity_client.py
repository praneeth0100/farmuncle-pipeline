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

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from farmuncle_pipeline.config import ConfigError, Source

if TYPE_CHECKING:
    from supabase import Client


# =============================================================================
# Preload snapshot (Fix #1, 2026-07-14 performance investigation)
#
# Why this exists:
#   Measured on real historical_backfill runs, identity resolution
#   (find_or_create_mandi / find_or_create_crop RPC calls) was 75-77%
#   of total per-date runtime — because the same ~2,800 mandis and
#   ~330 crops repeat every single day, nationwide, forever, and this
#   client only ever memoized within ONE run: every new date started
#   from an empty cache and re-asked the database "does this exist?"
#   for entities it had already resolved the day before, and the day
#   before that.
#
# What this DOES replicate from the RPCs (confirmed by reading the
# live function definitions in Supabase, not guessed):
#   - `normalize_market_name`/`normalize_crop_name`: lowercase, trim,
#     "&" -> " and ", strip [.,], collapse whitespace. Copied here
#     verbatim as `_normalize_market_name`/`_normalize_crop_name`.
#   - The EXACT-match lookup: mandis by
#     (normalized_name, state, district) where status='ACTIVE';
#     crops by normalized_name where status='ACTIVE'.
#   - The ALIAS-match lookup: mandi_aliases/crop_aliases by
#     normalized_alias (this table isn't state-scoped server-side,
#     so neither is the preloaded alias dict here).
#
# What this deliberately DOES NOT replicate (and why that's safe):
#   - Fuzzy matching (pg_trgm `similarity() >= FUZZY_THRESHOLD`,
#     currently 0.75). Replicating Postgres trigram similarity in
#     Python risks subtly disagreeing with the server on a near-match
#     ("Benny Hills" vs "Benny Hills APMC") — which could silently
#     create a duplicate entity. Anything that misses BOTH the exact
#     and alias preload dicts below still falls through to the real
#     `find_or_create_mandi`/`find_or_create_crop` RPC, unchanged,
#     exactly as it worked before this fix — fuzzy matching and
#     entity creation still only ever happen server-side.
#   - Entity creation itself — same reasoning.
#
# Net effect: a name this run has genuinely seen before (exact or
# already-aliased) skips the RPC round trip entirely. A name that's
# new, or only a near-match, pays the same RPC cost as before — this
# fix only removes redundant work, it never changes which entity a
# row resolves to.
# =============================================================================

def _normalize_market_name(name: str | None) -> str:
    """Python mirror of the `normalize_market_name` SQL function —
    keep these in exact sync; if that RPC's normalization ever
    changes, this must change with it or preload hits will silently
    stop matching what the database would have matched."""
    s = (name or "").strip().lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[.,]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _normalize_crop_name(name: str | None) -> str:
    """Python mirror of the `normalize_crop_name` SQL function — same
    body as `_normalize_market_name` today (both RPCs happen to be
    identical), kept as a separate function since there's no
    guarantee they stay identical, and this file shouldn't assume
    they do."""
    s = (name or "").strip().lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[.,]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _normalize_variety(raw_variety: str | None) -> str:
    """Python mirror of the `normalize_variety` SQL function — verified
    against the live definition (2026-07-14): unlike
    `normalize_market_name`/`normalize_crop_name`, this one is a pure
    SQL function with no table lookups, no aliasing, and no fuzzy
    matching (`LANGUAGE sql IMMUTABLE`) — just
    blank/null -> 'other', else lowercase + trim + collapse whitespace.
    Because there is nothing server-side this could disagree with
    (no data to be stale against), `resolve_variety` calls this
    directly instead of going through the RPC at all — unlike mandi/
    crop resolution, there's no unreplicated fuzzy/creation path this
    needs to fall back to. If `normalize_variety` in the database is
    ever changed to reference a table (aliases, canonicalization,
    etc.), this mirror must be updated to match or reintroduce an RPC
    fallback."""
    if raw_variety is None or raw_variety.strip() == "":
        return "other"
    return re.sub(r"\s+", " ", raw_variety.strip().lower()).strip()


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

        Callers SHOULD call `preload()` once, immediately after
        construction and before any `resolve_mandi`/`resolve_crop`
        calls, to get the performance benefit described in this
        module's "Preload snapshot" section above. Calling `preload()`
        is optional, not required for correctness — if it's never
        called, every resolve falls back to the RPC exactly as it did
        before this fix existed.
    """

    def __init__(self, client: "Client") -> None:
        self._client = client
        self._mandi_cache: dict[tuple[str, str, str | None], int] = {}
        self._crop_cache: dict[str, int] = {}
        self._variety_cache: dict[str, str] = {}
        self._unit_cache: dict[str, str] = {}

        # Preload snapshot state — empty/unused until `preload()` is
        # called. `_preloaded` gates whether resolve_mandi/resolve_crop
        # even attempt the snapshot dicts, so a caller that never
        # calls `preload()` gets identical behavior to before this fix.
        self._preloaded = False
        self._mandi_exact: dict[tuple[str, str, str | None], int] = {}
        self._mandi_alias: dict[str, int] = {}
        self._crop_exact: dict[str, int] = {}
        self._crop_alias: dict[str, int] = {}

    def preload(self) -> None:
        """
        Purpose:
            Bulk-load every ACTIVE mandi/crop and their known aliases
            into memory in four queries total, so that
            `resolve_mandi`/`resolve_crop` can skip the RPC round trip
            entirely for any name this run sees that's an exact or
            already-aliased match — see this module's "Preload
            snapshot" section for what is and isn't replicated, and
            why. Call this once per `IdentityClient` instance, before
            any resolve_mandi/resolve_crop calls.
        Inputs:
            None (uses the client passed to `__init__`).
        Outputs:
            None. Populates internal snapshot dicts and sets
            `_preloaded = True`.
        Failure modes:
            Raises `ConfigError` if any of the four bulk selects fail.
            Deliberately not caught/swallowed — a partial or failed
            preload silently falling back to "acts like preload never
            happened" would be fine correctness-wise, but a failure
            here usually means something is wrong with the connection
            that resolve calls are about to hit anyway, so surfacing
            it immediately is more useful than a confusing failure
            later.
        """
        try:
            mandi_rows = (
                self._client.table("mandis")
                .select("id,normalized_name,state,district")
                .eq("status", "ACTIVE")
                .execute()
            ).data or []
            mandi_alias_rows = (
                self._client.table("mandi_aliases")
                .select("mandi_id,normalized_alias")
                .execute()
            ).data or []
            crop_rows = (
                self._client.table("crops")
                .select("id,normalized_name")
                .eq("status", "ACTIVE")
                .execute()
            ).data or []
            crop_alias_rows = (
                self._client.table("crop_aliases")
                .select("crop_id,normalized_alias")
                .execute()
            ).data or []
        except Exception as exc:
            raise ConfigError(f"IdentityClient.preload failed: {exc}") from exc

        self._mandi_exact = {
            (row["normalized_name"], row["state"], row["district"]): row["id"]
            for row in mandi_rows
        }
        self._mandi_alias = {
            row["normalized_alias"]: row["mandi_id"] for row in mandi_alias_rows
        }
        self._crop_exact = {row["normalized_name"]: row["id"] for row in crop_rows}
        self._crop_alias = {
            row["normalized_alias"]: row["crop_id"] for row in crop_alias_rows
        }
        self._preloaded = True

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
            Resolve (or create) a canonical mandi id. Checks, in
            order: (1) this run's own memoization cache, (2) if
            `preload()` was called, the preloaded exact-match and
            alias-match snapshots (no network call), (3) the
            `find_or_create_mandi` RPC — which also handles fuzzy
            matching and entity creation, neither of which are
            replicated locally (see module docstring).
        Inputs:
            name / state / district: as reported by the government API
                for this row.
            source: `Source.RESOURCE_1` / `Source.RESOURCE_2` /
                `Source.MANUAL` — passed through as `p_source`.
            latitude / longitude: optional, if the resource provides
                them (Resource 1/2 government feeds do not; reserved
                for a future resource that does).
        Outputs:
            `ResolvedEntity`. `first_seen_this_run` is False for both
            run-cache hits and preload-snapshot hits (both mean "this
            entity was already known, not newly created") and True
            only when the RPC itself was called and returned.
        Failure modes:
            Raises `ConfigError` if the RPC call itself fails.
        """
        key = (name, state, district)
        if key in self._mandi_cache:
            return ResolvedEntity(id=self._mandi_cache[key], first_seen_this_run=False)

        if self._preloaded:
            normalized = _normalize_market_name(name)
            preload_id = self._mandi_exact.get((normalized, state, district))
            if preload_id is None:
                preload_id = self._mandi_alias.get(normalized)
            if preload_id is not None:
                self._mandi_cache[key] = preload_id
                return ResolvedEntity(id=preload_id, first_seen_this_run=False)

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
            Resolve (or create) a canonical crop id. Same three-step
            order as `resolve_mandi` (run cache, then preload
            snapshot if `preload()` was called, then the
            `find_or_create_crop` RPC for anything not already known).
        Inputs:
            name: commodity name as reported by the government API.
            unit: passed through as `p_unit` (already normalized by
                the caller via `normalize_unit`, see `resolve_unit`
                below).
            source: `Source.RESOURCE_1` / `Source.RESOURCE_2` /
                `Source.MANUAL`.
        Outputs:
            `ResolvedEntity`. `first_seen_this_run` is False for both
            run-cache hits and preload-snapshot hits, True only when
            the RPC itself was called.
        Failure modes:
            Raises `ConfigError` if the RPC call itself fails.
        """
        if name in self._crop_cache:
            return ResolvedEntity(id=self._crop_cache[name], first_seen_this_run=False)

        if self._preloaded:
            normalized = _normalize_crop_name(name)
            preload_id = self._crop_exact.get(normalized)
            if preload_id is None:
                preload_id = self._crop_alias.get(normalized)
            if preload_id is not None:
                self._crop_cache[name] = preload_id
                return ResolvedEntity(id=preload_id, first_seen_this_run=False)

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
            Normalize a variety string. `mandi_daily_prices.variety`
            is a plain text column (not FK'd to a canonical table),
            but it still participates in the business-key uniqueness
            constraint, so unnormalized spelling variants (casing,
            whitespace, "Other" vs "other") would silently fragment
            what should be the same row.

            Computed locally via `_normalize_variety` rather than the
            `normalize_variety` RPC — confirmed (2026-07-14) that RPC
            is a pure SQL function with no table lookups or fuzzy
            logic, so there's nothing server-side to disagree with,
            unlike `resolve_mandi`/`resolve_crop`. Before this change,
            this was the dominant remaining identity-resolution cost
            after the mandi/crop preload landed: ~500+ distinct
            varieties/day, each needing an RPC round trip on every
            single run (never preloadable the way mandi/crop were,
            since there's no `varieties` table to preload from — this
            fix removes the round trip entirely instead).
        Inputs:
            raw_variety: variety string as reported by the government
                API (may be blank).
        Outputs:
            Normalized variety string.
        Failure modes:
            None raised — pure string transformation.
        """
        if raw_variety in self._variety_cache:
            return self._variety_cache[raw_variety]

        normalized = _normalize_variety(raw_variety)
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