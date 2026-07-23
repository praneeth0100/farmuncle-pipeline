"""
FarmUncle v2 — tests/test_integration_pipeline.py
Phase D.5, Step 21 (Integration tests).

Why this file, given process_records and upsert_price_rows each
already have their own unit test files:
    Every existing test file stops at its own module's boundary —
    test_record_processor.py asserts the *shape* of the row dict
    `process_records` produces; test_price_writer.py asserts what
    `upsert_price_rows` does with a row dict handed to it directly.
    Neither proves the row `process_records` actually produces is one
    `upsert_price_rows` will accept and store correctly — the exact
    kind of interface mismatch (a renamed field, a type
    `upsert_price_rows` doesn't expect) that both modules' own tests
    are individually blind to. This file wires the two together with
    no gap in between, the way live_tick.py and daily_rewrite.py
    actually call them in production.

What's tested:
    - A raw Resource 1 record flows through `process_records` and the
      resulting row is accepted and stored by `upsert_price_rows`
      unmodified — the full happy path, one raw record in, one stored
      row out.
    - A batch mixing one well-formed and one malformed record: the
      malformed one is dropped by `process_records` (counted in
      `rows_failed`) and never even reaches `upsert_price_rows`, and
      the well-formed one is stored — proves the two modules'
      row-count bookkeeping stays consistent across the boundary.

What's deliberately NOT tested here:
    - Anything about *why* a record is malformed or *how* identity
      resolution works — both already covered by
      test_record_processor.py / test_identity_client.py. This file
      only cares whether the join between the two modules holds.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from farmuncle_pipeline.config import Source
from farmuncle_pipeline.core.price_writer import upsert_price_rows
from farmuncle_pipeline.core.record_processor import process_records
from tests.fakes import FakeResponse
from tests.test_record_processor import _KNOWN_CROP, _KNOWN_MANDI, _RESOURCE_1_RECORD, _make_identity

_MALFORMED_RECORD = {
    # Missing "market"/"commodity" entirely — the well-formed-vs-
    # malformed distinction test_record_processor.py already exercises,
    # reused here just to prove it never reaches upsert_price_rows.
    "state": "Andhra Pradesh",
    "district": "Guntur",
    "variety": "FAQ",
    "arrival_date": "15/07/2026",
    "modal_price": "2500",
    "min_price": "2000",
    "max_price": "3000",
}


class _FakeUpsertTable:
    """Purpose: same real-upsert-semantics fake as
    test_replay_idempotency.py (keyed dict, not an append-only list) —
    an integration test should catch a duplicate-row bug just as
    readily as a replay test would."""

    def __init__(self):
        self.store: dict[tuple, dict] = {}

    def table(self, name):
        assert name == "mandi_daily_prices"
        return self

    def upsert(self, rows, on_conflict=None):
        self._pending = rows
        return self

    def execute(self):
        for row in self._pending:
            key = (row["mandi_id"], row["crop_id"], row["variety"], row.get("grade") or "", row["price_date"])
            self.store[key] = row
        return FakeResponse(data=self._pending)


def test_full_pipeline_one_well_formed_record_end_to_end():
    identity = _make_identity(mandis=[_KNOWN_MANDI], crops=[_KNOWN_CROP])
    result = process_records(
        [_RESOURCE_1_RECORD],
        identity=identity,
        unit="Rs./Quintal",
        source=Source.RESOURCE_1,
        batch_id="batch-1",
        raw_api_batch_id="raw-1",
        job_name="live_tick",
    )
    assert result.rows_failed == 0
    assert len(result.price_rows) == 1

    table = _FakeUpsertTable()
    upsert_result = upsert_price_rows(table, result.price_rows, batch_size=1000, batch_id="batch-1", resource=Source.RESOURCE_1)

    assert upsert_result.rows_upserted == 1
    assert len(table.store) == 1
    stored = next(iter(table.store.values()))
    assert stored["mandi_id"] == _KNOWN_MANDI["id"]
    assert stored["crop_id"] == _KNOWN_CROP["id"]
    assert stored["modal_price"] == 2500.0


def test_full_pipeline_malformed_record_never_reaches_price_writer():
    identity = _make_identity(mandis=[_KNOWN_MANDI], crops=[_KNOWN_CROP])
    result = process_records(
        [_RESOURCE_1_RECORD, _MALFORMED_RECORD],
        identity=identity,
        unit="Rs./Quintal",
        source=Source.RESOURCE_1,
        batch_id="batch-2",
        raw_api_batch_id="raw-2",
        job_name="live_tick",
    )
    assert result.rows_failed == 1
    assert len(result.price_rows) == 1  # only the well-formed one made it through

    table = _FakeUpsertTable()
    upsert_result = upsert_price_rows(table, result.price_rows, batch_size=1000, batch_id="batch-2", resource=Source.RESOURCE_1)

    assert upsert_result.rows_upserted == 1
    assert len(table.store) == 1
