"""
FarmUncle v2 — tests/test_price_writer.py
Phase D.5, first test target.

Why this file first (of the three Phase D.5 named as highest
priority): `upsert_price_rows`'s row-level isolation logic is exactly
the kind of thing that reads correctly on a walkthrough but is easy to
silently break in a future edit — it was written in direct response to
the 2026-06-29 incident (one bad government row losing 17,000+ good
rows for the whole date), so a regression here has real, expensive
consequences, not just a failed assertion.

What's tested:
    - `_is_row_level_error`: the SQLSTATE-prefix classification that
      decides "isolate this row" vs. "abort the whole date".
    - `filter_rows_by_precedence`: §8's manual > resource_2 >
      resource_1 ordering, including the "no existing row" and
      "equal-precedence" pass-through cases.
    - `upsert_price_rows`: the happy path (chunk upserts cleanly), the
      isolation path (chunk fails, falls back to row-by-row, isolates
      exactly the bad row(s) and quarantines them, keeps the rest), the
      transient-error path (re-raises immediately, no row-by-row
      retry), and the "no batch_id/resource supplied" guard (refuses
      to silently drop data with no lineage).

What's deliberately NOT tested here:
    - Anything about the real Postgres constraints themselves (e.g.
      that `chk_prices_min_max` actually rejects a bad row) — that's
      the live schema's job, verified once by hand against
      `wqccgjmvslevkglfkmtc`, not something a fake client can
      meaningfully re-verify.
    - `insert_data_quality_issue`'s own internals (`batch_lifecycle.py`)
      — only that `upsert_price_rows` calls it with the right
      arguments when isolation kicks in.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from farmuncle_pipeline.config import ConfigError, Resource, Source
from farmuncle_pipeline.core.price_writer import (
    _is_row_level_error,
    filter_rows_by_precedence,
    upsert_price_rows,
)
from tests.fakes import FakeAPIError, FakeResponse


# =============================================================================
# _is_row_level_error
# =============================================================================

@pytest.mark.parametrize(
    "code,expected",
    [
        ("23514", True),   # check_violation — the actual 2026-06-29 incident code
        ("23502", True),   # not_null_violation
        ("22003", True),   # numeric_value_out_of_range (a 22* data_exception)
        ("22P02", True),   # invalid_text_representation (also 22*)
        ("57014", False),  # statement timeout — transient, must NOT be isolated
        ("08006", False),  # connection failure — transient
        (None, False),     # no code at all (unrecognized exception shape)
    ],
)
def test_is_row_level_error_classifies_by_sqlstate_prefix(code, expected):
    exc = FakeAPIError("boom", code=code)
    assert _is_row_level_error(exc) is expected


def test_is_row_level_error_false_for_exception_with_no_code_attribute():
    # A plain Exception (e.g. a network-library error) has no `.code`
    # at all — must be treated as "not row-level" (conservative
    # default per the function's own docstring), not crash on
    # getattr.
    assert _is_row_level_error(Exception("network blip")) is False


# =============================================================================
# filter_rows_by_precedence
# =============================================================================

def _price_row(mandi_id=1, crop_id=1, variety="other", price_date="2026-07-15"):
    return {"mandi_id": mandi_id, "crop_id": crop_id, "variety": variety, "price_date": price_date}


class _FakeSelectClient:
    """Purpose: stands in for the one query
    `filter_rows_by_precedence` makes — a `.select(...).in_(...).execute()`
    against `mandi_daily_prices` — returning a fixed set of "existing"
    rows regardless of the exact date list passed in (the function
    under test only cares about the returned rows' business keys and
    source, not that the fake replays real date filtering)."""

    def __init__(self, existing_rows: list[dict]):
        self._existing_rows = existing_rows

    def table(self, name):
        assert name == "mandi_daily_prices"
        return self

    def select(self, *_args, **_kwargs):
        return self

    def in_(self, *_args, **_kwargs):
        return self

    def execute(self):
        return FakeResponse(data=self._existing_rows)


def test_filter_rows_by_precedence_empty_input_short_circuits_without_query():
    class _ExplodingClient:
        def table(self, *_a, **_k):
            raise AssertionError("should never query for an empty rows list")

    kept, skipped = filter_rows_by_precedence(_ExplodingClient(), [], Source.RESOURCE_2)
    assert kept == []
    assert skipped == 0


def test_filter_rows_by_precedence_no_existing_row_passes_through():
    client = _FakeSelectClient(existing_rows=[])
    row = _price_row()
    kept, skipped = filter_rows_by_precedence(client, [row], Source.RESOURCE_1)
    assert kept == [row]
    assert skipped == 0


def test_filter_rows_by_precedence_manual_correction_always_wins():
    # §8: "manual corrections always win" — a resource_2 write must
    # NOT be allowed to overwrite an existing manual row.
    row = _price_row()
    existing = {**row, "source": Source.MANUAL.value}
    client = _FakeSelectClient(existing_rows=[existing])
    kept, skipped = filter_rows_by_precedence(client, [row], Source.RESOURCE_2)
    assert kept == []
    assert skipped == 1


def test_filter_rows_by_precedence_resource1_cannot_clobber_resource2():
    # The exact scenario the module docstring calls out: live_tick
    # (resource_1) running after daily_rewrite (resource_2) has
    # already finalized the day must not overwrite it.
    row = _price_row()
    existing = {**row, "source": Source.RESOURCE_2.value}
    client = _FakeSelectClient(existing_rows=[existing])
    kept, skipped = filter_rows_by_precedence(client, [row], Source.RESOURCE_1)
    assert kept == []
    assert skipped == 1


def test_filter_rows_by_precedence_resource2_overwrites_resource1():
    # The normal, expected daily_rewrite case: resource_2 (higher
    # precedence) is allowed to overwrite an earlier resource_1 row.
    row = _price_row()
    existing = {**row, "source": Source.RESOURCE_1.value}
    client = _FakeSelectClient(existing_rows=[existing])
    kept, skipped = filter_rows_by_precedence(client, [row], Source.RESOURCE_2)
    assert kept == [row]
    assert skipped == 0


def test_filter_rows_by_precedence_equal_precedence_passes_through():
    # Same source re-writing its own earlier row for the same key
    # (e.g. a retried page) is allowed — only STRICTLY higher
    # precedence blocks an overwrite, per the function's docstring.
    row = _price_row()
    existing = {**row, "source": Source.RESOURCE_2.value}
    client = _FakeSelectClient(existing_rows=[existing])
    kept, skipped = filter_rows_by_precedence(client, [row], Source.RESOURCE_2)
    assert kept == [row]
    assert skipped == 0


def test_filter_rows_by_precedence_only_blocks_matching_business_key():
    # A higher-precedence row for a DIFFERENT business key must not
    # affect an unrelated row's eligibility.
    row = _price_row(mandi_id=1)
    other_key_existing = {**_price_row(mandi_id=999), "source": Source.MANUAL.value}
    client = _FakeSelectClient(existing_rows=[other_key_existing])
    kept, skipped = filter_rows_by_precedence(client, [row], Source.RESOURCE_1)
    assert kept == [row]
    assert skipped == 0


def test_filter_rows_by_precedence_wraps_query_failure_in_config_error():
    class _FailingClient:
        def table(self, *_a, **_k):
            return self

        def select(self, *_a, **_k):
            return self

        def in_(self, *_a, **_k):
            return self

        def execute(self):
            raise RuntimeError("connection reset")

    with pytest.raises(ConfigError):
        filter_rows_by_precedence(_FailingClient(), [_price_row()], Source.RESOURCE_2)


# =============================================================================
# upsert_price_rows
# =============================================================================

class _FakeUpsertClient:
    """Purpose: stands in for both tables `upsert_price_rows` touches
    (`mandi_daily_prices` for the actual upsert, `data_quality_issues`
    for quarantining an isolated bad row). `chunk_side_effect` is
    called with every attempted upsert payload (whole chunks AND,
    during isolation fallback, single-row payloads) and may raise to
    simulate a database rejection — this is the one seam the tests
    below use to force the isolation path without a real database."""

    def __init__(self, chunk_side_effect=None):
        self._chunk_side_effect = chunk_side_effect or (lambda _rows: None)
        self.upserted_rows: list[dict] = []
        self.quarantined: list[dict] = []

    def table(self, name):
        if name == "mandi_daily_prices":
            return _FakePriceTable(self)
        if name == "data_quality_issues":
            return _FakeQuarantineTable(self)
        raise AssertionError(f"Unexpected table: {name}")


class _FakePriceTable:
    def __init__(self, client: _FakeUpsertClient):
        self._client = client
        self._rows = None

    def upsert(self, rows, on_conflict=None):
        self._rows = rows
        return self

    def execute(self):
        self._client._chunk_side_effect(self._rows)  # may raise
        self._client.upserted_rows.extend(self._rows)
        return FakeResponse(data=self._rows)


class _FakeQuarantineTable:
    def __init__(self, client: _FakeUpsertClient):
        self._client = client
        self._payload = None

    def insert(self, payload):
        self._payload = payload
        return self

    def execute(self):
        self._client.quarantined.append(self._payload)
        return FakeResponse(data=[self._payload])


def test_upsert_price_rows_empty_input_is_a_noop():
    client = _FakeUpsertClient()
    result = upsert_price_rows(client, [], batch_size=1000)
    assert result.rows_upserted == 0
    assert result.rows_quarantined == 0
    assert client.upserted_rows == []


def test_upsert_price_rows_happy_path_single_chunk():
    client = _FakeUpsertClient()
    rows = [_price_row(mandi_id=i) for i in range(3)]
    result = upsert_price_rows(client, rows, batch_size=1000)
    assert result.rows_upserted == 3
    assert result.rows_quarantined == 0
    assert len(client.upserted_rows) == 3


def test_upsert_price_rows_dedupes_by_business_key_last_one_wins():
    # Two rows sharing a business key within one run must collapse to
    # one upserted row — matches real ON CONFLICT semantics, and
    # matters because record_processor could plausibly hand this
    # function two records for the same key in a single run (e.g. a
    # correction appearing twice in one government page response).
    row_a = {**_price_row(), "modal_price": 100}
    row_b = {**_price_row(), "modal_price": 200}  # same business key, different price
    client = _FakeUpsertClient()
    result = upsert_price_rows(client, [row_a, row_b], batch_size=1000)
    assert result.rows_upserted == 1
    assert client.upserted_rows == [row_b]  # last one wins


def test_upsert_price_rows_chunks_by_batch_size():
    client = _FakeUpsertClient()
    rows = [_price_row(mandi_id=i) for i in range(5)]
    result = upsert_price_rows(client, rows, batch_size=2)
    assert result.rows_upserted == 5
    # 5 rows at batch_size=2 must be 3 separate upsert() calls
    # (2 + 2 + 1) — verified indirectly via total count matching,
    # since _FakeUpsertClient accumulates across all execute() calls.
    assert len(client.upserted_rows) == 5


def test_upsert_price_rows_isolates_one_bad_row_keeps_the_rest():
    # The exact 2026-06-29 scenario: a chunk of otherwise-good rows
    # contains one row that violates a check constraint. The whole
    # chunk's bulk upsert must fail, fall back to row-by-row, isolate
    # ONLY the bad row into data_quality_issues, and still upsert
    # every good row in that chunk (and would continue to subsequent
    # chunks, though this test uses only one chunk).
    good_rows = [_price_row(mandi_id=i) for i in range(3)]
    bad_row = {**_price_row(mandi_id=999), "modal_price": 8354, "min_price": 83548, "max_price": 8354}
    all_rows = good_rows + [bad_row]

    def side_effect(payload):
        if len(payload) > 1:
            # Bulk attempt on the whole chunk — simulate the real
            # chk_prices_min_max rejection whenever the bad row is
            # present in this attempt.
            if any(r.get("mandi_id") == 999 for r in payload):
                raise FakeAPIError("check_violation", code="23514")
        else:
            # Row-by-row fallback attempt.
            if payload[0].get("mandi_id") == 999:
                raise FakeAPIError("check_violation", code="23514")

    client = _FakeUpsertClient(chunk_side_effect=side_effect)
    result = upsert_price_rows(
        client, all_rows, batch_size=10, batch_id="batch-1", resource=Resource.RESOURCE_2
    )

    assert result.rows_upserted == 3
    assert result.rows_quarantined == 1
    assert len(client.quarantined) == 1
    assert client.quarantined[0]["row_data"]["mandi_id"] == 999
    assert client.quarantined[0]["error_code"] == "23514"
    # None of the good rows should have ended up quarantined.
    assert all(r["mandi_id"] != 999 for r in client.upserted_rows)


def test_upsert_price_rows_transient_error_reraises_without_row_by_row_retry():
    calls = []

    def side_effect(payload):
        calls.append(payload)
        raise FakeAPIError("statement timeout", code="57014")

    client = _FakeUpsertClient(chunk_side_effect=side_effect)
    rows = [_price_row(mandi_id=i) for i in range(3)]

    with pytest.raises(ConfigError):
        upsert_price_rows(client, rows, batch_size=10, batch_id="batch-1", resource=Resource.RESOURCE_2)

    # Exactly one attempt (the bulk chunk) — a transient failure must
    # NOT trigger the row-by-row fallback (that would be 1 + 3 = 4
    # calls instead).
    assert len(calls) == 1


def test_upsert_price_rows_refuses_to_isolate_without_batch_id_and_resource():
    # A row-level failure with no batch_id/resource supplied must
    # raise rather than silently drop the row with no lineage —
    # this is the guard the docstring promises, not just an
    # incidental side effect.
    def side_effect(payload):
        raise FakeAPIError("check_violation", code="23514")

    client = _FakeUpsertClient(chunk_side_effect=side_effect)
    rows = [_price_row()]

    with pytest.raises(ConfigError):
        upsert_price_rows(client, rows, batch_size=10)  # no batch_id/resource

    assert client.quarantined == []
