"""
FarmUncle v2 — tests/test_raw_dedup.py

Why this file: flagged in the 2026-07-18 audit handout as a real gap
-- no test file existed for raw_dedup.py at all, despite it being
where most of the 2026-07-22 grade-wiring session's actual work
landed. The highest-value thing to pin down here is the
`p_raw_grade` (single-entry RPC) vs `"grade"` (batch RPC's JSON
entries) naming split: it's a real inconsistency in the deployed
Postgres RPCs, not a Python bug, and it would be easy for a future
session to "fix" it into an actual bug by making the two functions
consistent with each other instead of each matching its own RPC.

What's tested:
    - `_content_hash`: deterministic, key-order independent, and
      scoped to exactly modal/min/max price (not market/commodity/
      etc. -- two records with identical prices but different
      identity fields hash the same, which is intentional: content
      hash is about "did the price change", not "is this the same
      record").
    - `upsert_raw_price_entry`: builds the RPC call with the correct
      `p_raw_grade` parameter name, defaults district/raw_variety/
      raw_grade to "" when falsy, passes content_hash/payload/
      batch_id/parser_version through, and unpacks
      (entry_id, is_new) from the RPC response correctly.
    - `upsert_raw_price_entries_batch`: empty input is a no-op (no
      RPC call, returns (0, 0)); each entry is built with the
      `"grade"` key (not `"raw_grade"`) reading from
      `parsed["raw_grade"]`, pinning the naming split down explicitly
      so a future "cleanup" that makes both functions use the same
      key name would fail these tests instead of silently breaking
      against the live RPC; missing `"raw_grade"` key in a parsed
      record doesn't raise (uses `.get`, not `[]`); district/
      raw_variety default to "" when falsy; return value is
      (rows_written, rows_new) read off what the RPC actually
      returns, not computed client-side (server does the real
      dedup collapsing, per the 2026-07-22 handout's "2 identical-key
      entries in one page -> collapses to 1 row" live-tested finding
      -- this suite can't re-verify that against Postgres, only that
      the Python layer reports back whatever the RPC said).

What's deliberately NOT tested here:
    - The live RPCs' own dedup/collapse semantics (content-hash
      match -> touch only; content change -> update in place) --
      that's Postgres's job, already live-tested by hand against
      `ltradoxvyxwszcoqiirk` per the handout, not something a fake
      client can meaningfully re-verify.
    - `uq_raw_price_entries_identity`'s `COALESCE` behavior -- a
      schema-level guarantee, not Python logic.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from farmuncle_pipeline.core.raw_dedup import (
    _content_hash,
    upsert_raw_price_entry,
    upsert_raw_price_entries_batch,
)
from tests.fakes import FakeResponse


# =============================================================================
# fakes
# =============================================================================

class _FakeRpcCall:
    def __init__(self, data):
        self._data = data

    def execute(self):
        return FakeResponse(data=self._data)


class FakeRawDedupClient:
    """Purpose: stands in for the Supabase client raw_dedup.py calls.
    Records every RPC call (name + params) so tests can assert exactly
    what was sent to Postgres, and returns a configurable canned
    response shaped like the real RPC's actual return rows."""

    def __init__(self, *, rpc_response=None):
        self._rpc_response = rpc_response
        self.rpc_calls: list[tuple[str, dict]] = []

    def rpc(self, name, params):
        self.rpc_calls.append((name, dict(params)))
        return _FakeRpcCall(self._rpc_response)


def _make_parsed(**overrides) -> dict:
    """A parse_agmarknet_record()-shaped dict with sane defaults,
    overridable per test."""
    base = {
        "market": "Badnawar",
        "state": "Madhya Pradesh",
        "district": "Dhar",
        "commodity": "Onion",
        "raw_variety": "Local",
        "raw_grade": "FAQ",
        "price_date": "2026-07-10",
        "modal_price": 900.0,
        "min_price": 800.0,
        "max_price": 1000.0,
    }
    base.update(overrides)
    return base


# =============================================================================
# _content_hash
# =============================================================================

def test_content_hash_deterministic_regardless_of_key_order():
    a = _content_hash({"modal_price": 900.0, "min_price": 800.0, "max_price": 1000.0})
    b = _content_hash({"max_price": 1000.0, "modal_price": 900.0, "min_price": 800.0})

    assert a == b


def test_content_hash_changes_when_a_price_changes():
    a = _content_hash({"modal_price": 900.0, "min_price": 800.0, "max_price": 1000.0})
    b = _content_hash({"modal_price": 901.0, "min_price": 800.0, "max_price": 1000.0})

    assert a != b


def test_content_hash_only_reflects_the_payload_given_not_identity_fields():
    # Content hash is computed purely from the payload dict it's
    # handed (modal/min/max price) -- it has no way to "see" market,
    # commodity, grade, etc. Two different records with identical
    # prices necessarily hash the same; distinguishing them is the
    # job of the RPC's business key, not the content hash.
    payload_1 = {"modal_price": 900.0, "min_price": 800.0, "max_price": 1000.0}
    payload_2 = {"modal_price": 900.0, "min_price": 800.0, "max_price": 1000.0}

    assert _content_hash(payload_1) == _content_hash(payload_2)


# =============================================================================
# upsert_raw_price_entry
# =============================================================================

def test_upsert_raw_price_entry_sends_p_raw_grade_param_name():
    client = FakeRawDedupClient(rpc_response=[{"entry_id": 42, "is_new": True}])

    upsert_raw_price_entry(
        client,
        resource="resource_2",
        market="Badnawar",
        state="Madhya Pradesh",
        district="Dhar",
        commodity="Onion",
        raw_variety="Local",
        raw_grade="FAQ",
        price_date="2026-07-10",
        modal_price=900.0,
        min_price=800.0,
        max_price=1000.0,
        batch_id="batch-1",
        parser_version=3,
    )

    assert len(client.rpc_calls) == 1
    name, params = client.rpc_calls[0]
    assert name == "upsert_raw_price_entry"
    # The single-entry RPC's real parameter name -- must stay
    # p_raw_grade, matching the live RPC signature exactly.
    assert params["p_raw_grade"] == "FAQ"
    assert "raw_grade" not in params
    assert "grade" not in params


def test_upsert_raw_price_entry_returns_entry_id_and_is_new():
    client = FakeRawDedupClient(rpc_response=[{"entry_id": 42, "is_new": False}])

    entry_id, is_new = upsert_raw_price_entry(
        client,
        resource="resource_2",
        market="Badnawar",
        state="Madhya Pradesh",
        district="Dhar",
        commodity="Onion",
        raw_variety="Local",
        raw_grade="FAQ",
        price_date="2026-07-10",
        modal_price=900.0,
        min_price=800.0,
        max_price=1000.0,
        batch_id="batch-1",
        parser_version=3,
    )

    assert entry_id == 42
    assert is_new is False


def test_upsert_raw_price_entry_defaults_falsy_district_variety_grade_to_empty_string():
    client = FakeRawDedupClient(rpc_response=[{"entry_id": 1, "is_new": True}])

    upsert_raw_price_entry(
        client,
        resource="resource_1",
        market="Some Market",
        state="Kerala",
        district=None,
        commodity="Tomato",
        raw_variety="",
        raw_grade="",
        price_date="2026-07-10",
        modal_price=50.0,
        min_price=40.0,
        max_price=60.0,
        batch_id="batch-2",
        parser_version=3,
    )

    _, params = client.rpc_calls[0]
    assert params["p_district"] == ""
    assert params["p_raw_variety"] == ""
    assert params["p_raw_grade"] == ""


def test_upsert_raw_price_entry_passes_content_hash_and_payload():
    client = FakeRawDedupClient(rpc_response=[{"entry_id": 1, "is_new": True}])

    upsert_raw_price_entry(
        client,
        resource="resource_2",
        market="Badnawar",
        state="Madhya Pradesh",
        district="Dhar",
        commodity="Onion",
        raw_variety="Local",
        raw_grade="FAQ",
        price_date="2026-07-10",
        modal_price=900.0,
        min_price=800.0,
        max_price=1000.0,
        batch_id="batch-1",
        parser_version=3,
    )

    _, params = client.rpc_calls[0]
    expected_hash = _content_hash(
        {"modal_price": 900.0, "min_price": 800.0, "max_price": 1000.0}
    )
    assert params["p_content_hash"] == expected_hash
    assert params["p_payload"] == {
        "modal_price": 900.0,
        "min_price": 800.0,
        "max_price": 1000.0,
    }
    assert params["p_batch_id"] == "batch-1"
    assert params["p_parser_version"] == 3


# =============================================================================
# upsert_raw_price_entries_batch
# =============================================================================

def test_upsert_raw_price_entries_batch_empty_input_is_a_noop():
    client = FakeRawDedupClient(rpc_response=[{"entry_id": 1, "is_new": True}])

    rows_written, rows_new = upsert_raw_price_entries_batch(
        client,
        resource="resource_2",
        batch_id="batch-1",
        parser_version=3,
        parsed_records=[],
    )

    assert rows_written == 0
    assert rows_new == 0
    assert client.rpc_calls == []


def test_upsert_raw_price_entries_batch_uses_grade_key_not_raw_grade_key():
    # Pins the real, deployed-RPC naming inconsistency: the batch RPC
    # reads `entry->>'grade'`, not `'raw_grade'`, even though the
    # value comes from `parsed["raw_grade"]`. This must NOT be
    # "cleaned up" to match the single-entry RPC's p_raw_grade name --
    # that would break against the live RPC.
    client = FakeRawDedupClient(rpc_response=[{"entry_id": 1, "is_new": True}])
    parsed = [_make_parsed(raw_grade="Non-FAQ")]

    upsert_raw_price_entries_batch(
        client,
        resource="resource_2",
        batch_id="batch-1",
        parser_version=3,
        parsed_records=parsed,
    )

    _, params = client.rpc_calls[0]
    entry = params["p_entries"][0]
    assert entry["grade"] == "Non-FAQ"
    assert "raw_grade" not in entry
    assert "p_raw_grade" not in entry


def test_upsert_raw_price_entries_batch_missing_raw_grade_key_defaults_to_empty_string():
    # parsed_records built from parse_agmarknet_record always carries
    # "raw_grade", but the batch function reads it with .get(), not
    # [], so a record missing the key entirely (e.g. a hand-built
    # test fixture, or a future caller) must not raise.
    client = FakeRawDedupClient(rpc_response=[{"entry_id": 1, "is_new": True}])
    parsed = _make_parsed()
    del parsed["raw_grade"]

    upsert_raw_price_entries_batch(
        client,
        resource="resource_2",
        batch_id="batch-1",
        parser_version=3,
        parsed_records=[parsed],
    )

    _, params = client.rpc_calls[0]
    assert params["p_entries"][0]["grade"] == ""


def test_upsert_raw_price_entries_batch_defaults_falsy_district_and_variety_to_empty_string():
    client = FakeRawDedupClient(rpc_response=[{"entry_id": 1, "is_new": True}])
    parsed = [_make_parsed(district=None, raw_variety="")]

    upsert_raw_price_entries_batch(
        client,
        resource="resource_1",
        batch_id="batch-1",
        parser_version=3,
        parsed_records=parsed,
    )

    _, params = client.rpc_calls[0]
    entry = params["p_entries"][0]
    assert entry["district"] == ""
    assert entry["raw_variety"] == ""


def test_upsert_raw_price_entries_batch_passes_resource_batch_id_parser_version_per_entry():
    client = FakeRawDedupClient(rpc_response=[{"entry_id": 1, "is_new": True}])
    parsed = [_make_parsed(), _make_parsed(commodity="Tomato")]

    upsert_raw_price_entries_batch(
        client,
        resource="resource_2",
        batch_id="batch-99",
        parser_version=5,
        parsed_records=parsed,
    )

    _, params = client.rpc_calls[0]
    for entry in params["p_entries"]:
        assert entry["resource"] == "resource_2"
        assert entry["batch_id"] == "batch-99"
        assert entry["parser_version"] == 5


def test_upsert_raw_price_entries_batch_reports_rows_written_and_rows_new_from_rpc_response():
    # rows_written/rows_new are read off whatever the RPC actually
    # returned, not computed client-side -- the server does the real
    # within-page dedup collapsing (per the 2026-07-22 live-tested
    # finding: 2 identical-key entries in one page collapse to 1 row).
    # Here the RPC reports back 2 rows total, only 1 of them new,
    # regardless of how many entries were sent.
    client = FakeRawDedupClient(
        rpc_response=[
            {"entry_id": 1, "is_new": True},
            {"entry_id": 2, "is_new": False},
        ]
    )
    parsed = [_make_parsed(), _make_parsed(commodity="Tomato"), _make_parsed(commodity="Potato")]

    rows_written, rows_new = upsert_raw_price_entries_batch(
        client,
        resource="resource_2",
        batch_id="batch-1",
        parser_version=3,
        parsed_records=parsed,
    )

    assert rows_written == 2
    assert rows_new == 1


def test_upsert_raw_price_entries_batch_single_rpc_call_regardless_of_page_size():
    client = FakeRawDedupClient(
        rpc_response=[{"entry_id": i, "is_new": True} for i in range(5)]
    )
    parsed = [_make_parsed(commodity=f"Crop {i}") for i in range(5)]

    upsert_raw_price_entries_batch(
        client,
        resource="resource_2",
        batch_id="batch-1",
        parser_version=3,
        parsed_records=parsed,
    )

    # The whole point of the batch RPC (2026-07-13 incident fix): one
    # round-trip per page, not one per record.
    assert len(client.rpc_calls) == 1
    assert len(client.rpc_calls[0][1]["p_entries"]) == 5
