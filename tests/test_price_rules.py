"""Price sanity rules applied before upsert, and the livestock id list."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from farmuncle_pipeline.config import PER_ANIMAL_CROP_IDS, Resource
from farmuncle_pipeline.core.price_writer import price_rule_violation, upsert_price_rows
from tests.test_price_writer import _FakeUpsertClient, _price_row


def _row(**kw):
    return {**_price_row(), "unit": "kg", "modal_price": 40.0, "min_price": 30.0, "max_price": 50.0, **kw}


def test_normal_row_has_no_violation():
    assert price_rule_violation(_row()) is None


def test_zero_or_negative_modal_is_rejected():
    assert price_rule_violation(_row(modal_price=0)) == "DQ-PRICE-NONPOSITIVE"
    assert price_rule_violation(_row(modal_price=-5)) == "DQ-PRICE-NONPOSITIVE"


def test_kg_price_under_one_rupee_is_rejected():
    assert price_rule_violation(_row(modal_price=0.03)) == "DQ-PRICE-FLOOR"


def test_animal_rows_are_not_held_to_the_kg_floor():
    assert price_rule_violation(_row(unit="animal", modal_price=0.5)) is None


def test_missing_modal_is_left_to_the_database():
    assert price_rule_violation(_row(modal_price=None)) is None


def test_bad_rows_are_quarantined_without_touching_the_table():
    client = _FakeUpsertClient()
    rows = [_row(mandi_id=1), _row(mandi_id=2, modal_price=0.03), _row(mandi_id=3, modal_price=0)]
    result = upsert_price_rows(client, rows, batch_size=100, batch_id="b1", resource=Resource.RESOURCE_2)
    assert result.rows_upserted == 1
    assert result.rows_quarantined == 2
    assert [r["mandi_id"] for r in client.upserted_rows] == [1]
    assert {q["error_code"] for q in client.quarantined} == {"DQ-PRICE-FLOOR", "DQ-PRICE-NONPOSITIVE"}


def test_all_livestock_crops_are_per_animal():
    # 320 He Buffalo and 321 She Goat were stored per kg by mistake.
    assert {320, 321} <= PER_ANIMAL_CROP_IDS
