"""
FarmUncle v2 — tests/test_replay_idempotency.py
Phase D.5, Step 22 (Replay tests).

Why this matters as its own category, separate from
test_price_writer.py's existing happy-path/isolation tests:
    "Replay" isn't a new code path — it's a property that must hold
    *across* two separate calls to `upsert_price_rows` with the same
    input, which no existing test asserts. This is exactly the
    scenario daily_rewrite.py's own lookback window creates on
    purpose (re-processing the last N days every run, per §8/module
    docstring) and that retry_failed_pages.py creates by accident
    (retrying a page that may have partially succeeded before
    failing). Both rely on `on_conflict=mandi_id,crop_id,variety,
    grade,price_date` making a repeat upsert of identical data a true
    no-op at the database level — this file is a fake table that
    actually enforces that upsert semantic (keyed dict, last-write-wins
    per key), unlike test_price_writer.py's `_FakePriceTable` which
    just appends every attempt to a list and was never meant to answer
    "did replaying this create a duplicate."

What's tested:
    - Calling `upsert_price_rows` twice with the identical row set
      results in exactly one stored row per business key, not two.
    - A replay where one row's value changed (e.g. a government
      revision) correctly overwrites in place rather than adding a
      second row for that key.
    - The row-count instrumentation stays accurate across the two
      calls — a "hidden" duplicate wouldn't just be a storage bug,
      it would also silently inflate `rows_processed` in
      `ingestion_batches`, which is why this checks both the fake
      table's true stored state AND `UpsertResult.rows_upserted`.

What's deliberately NOT tested here:
    - The in-memory same-call dedup (`deduped: dict[tuple, dict]`
      inside `upsert_price_rows` itself) — that's a different
      mechanism (collapsing duplicate keys *within* one input list)
      already implicitly exercised by every other price_writer test
      passing single-row-per-key input; this file is specifically
      about the cross-call, database-level guarantee.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from farmuncle_pipeline.core.price_writer import upsert_price_rows
from tests.fakes import FakeResponse


def _price_row(mandi_id=1, crop_id=1, variety="FAQ", grade=None, price_date="2026-07-15", modal_price=2500):
    return {
        "mandi_id": mandi_id,
        "crop_id": crop_id,
        "variety": variety,
        "grade": grade,
        "price_date": price_date,
        "modal_price": modal_price,
    }


class _RealUpsertSemanticsTable:
    """Purpose: unlike test_price_writer.py's `_FakePriceTable` (which
    just appends every attempted row), this fake actually enforces
    `on_conflict=(mandi_id,crop_id,variety,grade,price_date)` upsert
    semantics — a dict keyed by that business key, so a second upsert
    of the same key overwrites in place. This is the one property
    replay/idempotency tests need that the existing price_writer fake
    was never built to check. `grade` added 2026-07-21/22 alongside
    the price_writer.py business-key fix."""

    def __init__(self):
        self.store: dict[tuple, dict] = {}

    def table(self, name):
        assert name == "mandi_daily_prices"
        return self

    def upsert(self, rows, on_conflict=None):
        assert on_conflict == "mandi_id,crop_id,variety,grade,price_date"
        self._pending = rows
        return self

    def execute(self):
        for row in self._pending:
            key = (row["mandi_id"], row["crop_id"], row["variety"], row.get("grade") or "", row["price_date"])
            self.store[key] = row
        return FakeResponse(data=self._pending)


def test_replaying_identical_batch_does_not_duplicate_stored_rows():
    client = _RealUpsertSemanticsTable()
    rows = [_price_row(mandi_id=i) for i in range(5)]

    first = upsert_price_rows(client, rows, batch_size=1000)
    second = upsert_price_rows(client, rows, batch_size=1000)

    assert first.rows_upserted == 5
    assert second.rows_upserted == 5  # both calls report success...
    assert len(client.store) == 5     # ...but storage has exactly 5 rows, not 10


def test_replay_with_a_revised_value_overwrites_not_appends():
    # Simulates a government data revision: the same business key
    # comes back with a different modal_price on the next daily_rewrite
    # run. Must overwrite, not create a second row for that key.
    client = _RealUpsertSemanticsTable()
    original = _price_row(modal_price=2500)
    revised = _price_row(modal_price=2650)

    upsert_price_rows(client, [original], batch_size=1000)
    upsert_price_rows(client, [revised], batch_size=1000)

    assert len(client.store) == 1
    key = (1, 1, "FAQ", "", "2026-07-15")
    assert client.store[key]["modal_price"] == 2650


def test_replaying_across_three_runs_stays_stable():
    # Three repeated replays (mirrors a lookback window re-processing
    # the same day across three consecutive daily_rewrite runs) must
    # converge to the same stored state each time, not drift.
    client = _RealUpsertSemanticsTable()
    rows = [_price_row(mandi_id=i) for i in range(10)]

    results = [upsert_price_rows(client, rows, batch_size=1000) for _ in range(3)]

    assert all(r.rows_upserted == 10 for r in results)
    assert len(client.store) == 10
