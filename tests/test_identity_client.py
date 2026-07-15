"""
FarmUncle v2 — tests/test_identity_client.py
Phase D.5, second test target.

Why this file (of the three Phase D.5 named as highest priority): this
is the exact code the 2026-07-14/15 performance work (preload snapshot)
touched, and is about to be touched again once the preload hit-rate
instrumentation (parked performance item) is added — locking down its
three-step resolution order (run cache -> preload snapshot -> RPC)
now means that work can proceed without silently changing which
entity a row resolves to.

What's tested:
    - `_normalize_market_name` / `_normalize_crop_name` /
      `_normalize_variety`: the Python mirrors of the SQL
      normalization functions. These MUST stay in exact sync with the
      live RPCs (see the module's own docstring) — a test here can't
      verify the SQL side, but it pins down what the Python side
      currently does, so a future accidental change is caught instead
      of silently drifting.
    - `resolve_mandi` / `resolve_crop`: the three-step order (run
      cache hit -> preload exact/alias hit -> RPC fallback) and that
      `first_seen_this_run` is reported correctly at each step (False
      for cache/preload hits, True only for an actual RPC call).
    - `resolve_variety`: pure local normalization, memoized, no RPC
      call ever made.
    - `resolve_unit`: RPC-backed, memoized so a repeated raw_unit only
      calls the RPC once.

What's deliberately NOT tested here:
    - The real `find_or_create_mandi`/`find_or_create_crop` RPCs'
      fuzzy-matching or creation behavior — those live entirely
      server-side (by design, see the module docstring's invariant 3
      note) and are out of scope for a Python-side unit test.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from farmuncle_pipeline.config import Source
from farmuncle_pipeline.core.identity_client import (
    IdentityClient,
    _normalize_crop_name,
    _normalize_market_name,
    _normalize_variety,
)
from tests.fakes import FakeResponse


# =============================================================================
# Normalization mirrors
# =============================================================================

@pytest.mark.parametrize(
    "raw,expected",
    [
        ("  Guntur APMC  ", "guntur apmc"),
        ("Kadapa & Cuddapah", "kadapa and cuddapah"),
        ("Anantapur, Market.", "anantapur market"),
        ("Multiple   Spaces  Here", "multiple spaces here"),
        ("", ""),
        (None, ""),
    ],
)
def test_normalize_market_name(raw, expected):
    assert _normalize_market_name(raw) == expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Bengal Gram (Gram)(Whole)", "bengal gram (gram)(whole)"),
        ("Tomato & Onion", "tomato and onion"),
        ("  Rice.  ", "rice"),
    ],
)
def test_normalize_crop_name(raw, expected):
    assert _normalize_crop_name(raw) == expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("FAQ", "faq"),
        ("  Local  ", "local"),
        ("", "other"),
        (None, "other"),
        ("  ", "other"),
        ("Extra   Spaces  Variety", "extra spaces variety"),
    ],
)
def test_normalize_variety(raw, expected):
    assert _normalize_variety(raw) == expected


# =============================================================================
# Fakes for IdentityClient
# =============================================================================

class _FakeTable:
    def __init__(self, rows: list[dict]):
        self._rows = rows
        self._range: tuple[int, int] | None = None

    def select(self, *_a, **_k):
        return self

    def eq(self, *_a, **_k):
        return self

    def range(self, start: int, end: int):
        # Mirrors real Supabase/PostgREST `.range()`: inclusive start/end
        # row indices. Without this, the fake can't reproduce the
        # default-row-cap bug that motivated `_fetch_all_rows`.
        self._range = (start, end)
        return self

    def execute(self):
        if self._range is None:
            return FakeResponse(data=self._rows)
        start, end = self._range
        return FakeResponse(data=self._rows[start : end + 1])


class _FakeRpcCall:
    def __init__(self, data):
        self._data = data

    def execute(self):
        return FakeResponse(data=self._data)


class FakeIdentityDBClient:
    """Purpose: stands in for the Supabase client `IdentityClient`
    wraps. Holds fixed table contents for `preload()` and configurable
    RPC return values, while recording every RPC call made so tests
    can assert exactly how many (and which) RPC round-trips happened —
    the whole point of the preload optimization is reducing that
    count, so being able to assert on it directly is the point of this
    fake."""

    def __init__(
        self,
        *,
        mandis=None,
        mandi_aliases=None,
        crops=None,
        crop_aliases=None,
        rpc_responses=None,
    ):
        self._tables = {
            "mandis": mandis or [],
            "mandi_aliases": mandi_aliases or [],
            "crops": crops or [],
            "crop_aliases": crop_aliases or [],
        }
        self._rpc_responses = rpc_responses or {}
        self.rpc_calls: list[tuple[str, dict]] = []

    def table(self, name):
        return _FakeTable(self._tables[name])

    def rpc(self, name, params):
        self.rpc_calls.append((name, dict(params)))
        value = self._rpc_responses.get(name)
        if callable(value):
            value = value(params)
        return _FakeRpcCall(value)


# =============================================================================
# resolve_mandi
# =============================================================================

def test_resolve_mandi_falls_back_to_rpc_when_not_preloaded_and_reports_first_seen():
    client = FakeIdentityDBClient(rpc_responses={"find_or_create_mandi": 501})
    identity = IdentityClient(client)  # preload() deliberately not called

    result = identity.resolve_mandi(name="Guntur APMC", state="Andhra Pradesh", district="Guntur", source=Source.RESOURCE_2)

    assert result.id == 501
    assert result.first_seen_this_run is True
    assert len(client.rpc_calls) == 1
    assert client.rpc_calls[0][0] == "find_or_create_mandi"


def test_resolve_mandi_run_cache_hit_skips_second_rpc_call():
    client = FakeIdentityDBClient(rpc_responses={"find_or_create_mandi": 501})
    identity = IdentityClient(client)

    first = identity.resolve_mandi(name="Guntur APMC", state="Andhra Pradesh", district="Guntur", source=Source.RESOURCE_2)
    second = identity.resolve_mandi(name="Guntur APMC", state="Andhra Pradesh", district="Guntur", source=Source.RESOURCE_2)

    assert first.id == second.id == 501
    assert second.first_seen_this_run is False
    assert len(client.rpc_calls) == 1  # only the first call actually hit the RPC


def test_resolve_mandi_preload_exact_match_skips_rpc_entirely():
    client = FakeIdentityDBClient(
        mandis=[{"id": 42, "normalized_name": "guntur apmc", "state": "Andhra Pradesh", "district": "Guntur"}],
        rpc_responses={"find_or_create_mandi": 999},  # would be wrong if this got called
    )
    identity = IdentityClient(client)
    identity.preload()

    result = identity.resolve_mandi(name="Guntur APMC", state="Andhra Pradesh", district="Guntur", source=Source.RESOURCE_2)

    assert result.id == 42
    assert result.first_seen_this_run is False
    assert client.rpc_calls == []  # preload hit — no RPC round trip at all


def test_resolve_mandi_preload_alias_match_skips_rpc():
    client = FakeIdentityDBClient(
        mandis=[],  # no exact match on purpose
        mandi_aliases=[{"mandi_id": 77, "normalized_alias": "gtr mkt"}],
        rpc_responses={"find_or_create_mandi": 999},
    )
    identity = IdentityClient(client)
    identity.preload()

    result = identity.resolve_mandi(name="GTR Mkt", state="Andhra Pradesh", district="Guntur", source=Source.RESOURCE_2)

    assert result.id == 77
    assert result.first_seen_this_run is False
    assert client.rpc_calls == []


def test_resolve_mandi_preload_miss_falls_through_to_rpc():
    # A genuinely new mandi name (not in either preload snapshot) must
    # still reach the RPC — preload must never suppress a real
    # resolution, only skip the round trip for known entities.
    client = FakeIdentityDBClient(
        mandis=[{"id": 1, "normalized_name": "some other mandi", "state": "Andhra Pradesh", "district": "Guntur"}],
        rpc_responses={"find_or_create_mandi": 888},
    )
    identity = IdentityClient(client)
    identity.preload()

    result = identity.resolve_mandi(name="Brand New Mandi", state="Andhra Pradesh", district="Guntur", source=Source.RESOURCE_2)

    assert result.id == 888
    assert result.first_seen_this_run is True
    assert len(client.rpc_calls) == 1


# =============================================================================
# resolve_crop
# =============================================================================

def test_resolve_crop_preload_exact_match_skips_rpc():
    client = FakeIdentityDBClient(
        crops=[{"id": 10, "normalized_name": "tomato"}],
        rpc_responses={"find_or_create_crop": 999},
    )
    identity = IdentityClient(client)
    identity.preload()

    result = identity.resolve_crop(name="Tomato", unit="kg", source=Source.RESOURCE_2)

    assert result.id == 10
    assert result.first_seen_this_run is False
    assert client.rpc_calls == []


def test_resolve_crop_preload_alias_match_skips_rpc():
    client = FakeIdentityDBClient(
        crops=[],
        crop_aliases=[{"crop_id": 55, "normalized_alias": "tomatoes"}],
        rpc_responses={"find_or_create_crop": 999},
    )
    identity = IdentityClient(client)
    identity.preload()

    result = identity.resolve_crop(name="Tomatoes", unit="kg", source=Source.RESOURCE_2)

    assert result.id == 55
    assert result.first_seen_this_run is False
    assert client.rpc_calls == []


def test_resolve_crop_run_cache_hit_skips_second_rpc_call():
    client = FakeIdentityDBClient(rpc_responses={"find_or_create_crop": 10})
    identity = IdentityClient(client)

    first = identity.resolve_crop(name="Tomato", unit="kg", source=Source.RESOURCE_2)
    second = identity.resolve_crop(name="Tomato", unit="kg", source=Source.RESOURCE_2)

    assert first.id == second.id == 10
    assert second.first_seen_this_run is False
    assert len(client.rpc_calls) == 1


# =============================================================================
# stats() — hit/miss counters (Fix #1 follow-up)
# =============================================================================

def test_stats_starts_at_zero_for_all_six_counters():
    identity = IdentityClient(FakeIdentityDBClient())

    assert identity.stats() == {
        "mandi_cache_hits": 0,
        "mandi_preload_hits": 0,
        "mandi_rpc_calls": 0,
        "crop_cache_hits": 0,
        "crop_preload_hits": 0,
        "crop_rpc_calls": 0,
    }


def test_stats_counts_mandi_cache_preload_and_rpc_tiers_separately():
    client = FakeIdentityDBClient(
        mandis=[{"id": 42, "normalized_name": "guntur apmc", "state": "Andhra Pradesh", "district": "Guntur"}],
        rpc_responses={"find_or_create_mandi": 888},
    )
    identity = IdentityClient(client)
    identity.preload()

    # One preload hit...
    identity.resolve_mandi(name="Guntur APMC", state="Andhra Pradesh", district="Guntur", source=Source.RESOURCE_2)
    # ...one RPC fallback for a genuinely new name...
    identity.resolve_mandi(name="Brand New Mandi", state="Andhra Pradesh", district="Guntur", source=Source.RESOURCE_2)
    # ...and a repeat of the new name should now be a run-cache hit.
    identity.resolve_mandi(name="Brand New Mandi", state="Andhra Pradesh", district="Guntur", source=Source.RESOURCE_2)

    stats = identity.stats()
    assert stats["mandi_preload_hits"] == 1
    assert stats["mandi_rpc_calls"] == 1
    assert stats["mandi_cache_hits"] == 1
    # Crop counters are untouched by mandi resolutions.
    assert stats["crop_cache_hits"] == 0
    assert stats["crop_preload_hits"] == 0
    assert stats["crop_rpc_calls"] == 0


def test_stats_counts_crop_cache_preload_and_rpc_tiers_separately():
    client = FakeIdentityDBClient(
        crops=[{"id": 10, "normalized_name": "tomato"}],
        rpc_responses={"find_or_create_crop": 999},
    )
    identity = IdentityClient(client)
    identity.preload()

    identity.resolve_crop(name="Tomato", unit="kg", source=Source.RESOURCE_2)  # preload hit
    identity.resolve_crop(name="Brand New Crop", unit="kg", source=Source.RESOURCE_2)  # rpc
    identity.resolve_crop(name="Brand New Crop", unit="kg", source=Source.RESOURCE_2)  # cache hit

    stats = identity.stats()
    assert stats["crop_preload_hits"] == 1
    assert stats["crop_rpc_calls"] == 1
    assert stats["crop_cache_hits"] == 1
    assert stats["mandi_cache_hits"] == 0
    assert stats["mandi_preload_hits"] == 0
    assert stats["mandi_rpc_calls"] == 0


# =============================================================================
# preload() pagination — the 2026-07-15 default-row-cap bug
# =============================================================================

def test_preload_fetches_more_than_one_page_of_mandis():
    # 2,300 mandis is deliberately > IdentityClient._PAGE_SIZE (1000),
    # reproducing the real bug: an unranged select silently truncated
    # to the first page, so most mandis were never in the snapshot and
    # every one of them paid a full RPC round trip, every single day.
    mandis = [
        {"id": i, "normalized_name": f"mandi {i}", "state": "Andhra Pradesh", "district": None}
        for i in range(2300)
    ]
    client = FakeIdentityDBClient(mandis=mandis, rpc_responses={"find_or_create_mandi": -1})
    identity = IdentityClient(client)
    identity.preload()

    # A mandi that only existed on the third page (past two 1000-row
    # pages) must still resolve via preload, not fall through to RPC.
    result = identity.resolve_mandi(name="Mandi 2299", state="Andhra Pradesh", district=None, source=Source.RESOURCE_2)
    assert result.id == 2299
    assert client.rpc_calls == []


def test_preload_fetches_more_than_one_page_of_mandi_aliases():
    aliases = [
        {"mandi_id": i, "normalized_alias": f"alias {i}"} for i in range(2300)
    ]
    client = FakeIdentityDBClient(mandi_aliases=aliases, rpc_responses={"find_or_create_mandi": -1})
    identity = IdentityClient(client)
    identity.preload()

    result = identity.resolve_mandi(name="Alias 2299", state="Andhra Pradesh", district=None, source=Source.RESOURCE_2)
    assert result.id == 2299
    assert client.rpc_calls == []


def test_preload_pagination_stops_exactly_on_a_page_boundary():
    # Exactly PAGE_SIZE rows (1000) must not trigger a spurious extra
    # request for a phantom second page.
    mandis = [
        {"id": i, "normalized_name": f"mandi {i}", "state": "Andhra Pradesh", "district": None}
        for i in range(1000)
    ]
    client = FakeIdentityDBClient(mandis=mandis)
    identity = IdentityClient(client)
    identity.preload()

    assert len(identity._mandi_exact) == 1000


# =============================================================================
# resolve_variety — pure local normalization, never an RPC
# =============================================================================

def test_resolve_variety_never_calls_rpc_and_is_memoized():
    client = FakeIdentityDBClient()
    identity = IdentityClient(client)

    first = identity.resolve_variety("  FAQ  ")
    second = identity.resolve_variety("  FAQ  ")

    assert first == second == "faq"
    assert client.rpc_calls == []  # confirms this never round-trips, by design


# =============================================================================
# resolve_unit — RPC-backed, memoized
# =============================================================================

def test_resolve_unit_calls_rpc_once_per_distinct_raw_unit():
    client = FakeIdentityDBClient(rpc_responses={"normalize_unit": "kg"})
    identity = IdentityClient(client)

    first = identity.resolve_unit("kg")
    second = identity.resolve_unit("kg")

    assert first == second == "kg"
    assert len(client.rpc_calls) == 1
    assert client.rpc_calls[0] == ("normalize_unit", {"p_unit": "kg"})