"""Price sanity rules applied before upsert, and the livestock id list."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from farmuncle_pipeline.config import PER_ANIMAL_CROP_IDS, Resource
from farmuncle_pipeline.core import price_writer
from farmuncle_pipeline.core.price_writer import apply_price_rules, upsert_price_rows
from tests.fakes import FakeResponse
from tests.test_price_writer import _FakeUpsertClient, _price_row


def _row(**kw):
    return {**_price_row(), "unit": "kg", "modal_price": 40.0, "min_price": 30.0, "max_price": 50.0,
            "quality_components": {}, **kw}


def test_normal_row_passes_unchanged():
    row = _row()
    assert apply_price_rules(row, median=40.0) == (row, None)


def test_zero_or_negative_modal_is_rejected():
    assert apply_price_rules(_row(modal_price=0), 40.0) == (None, "DQ-PRICE-NONPOSITIVE")
    assert apply_price_rules(_row(modal_price=-5), 40.0) == (None, "DQ-PRICE-NONPOSITIVE")


def test_per_kg_entry_is_rescaled_and_flagged():
    # Spinach: the mandi typed Rs 7.8/kg, the pipeline divided it by 100 again.
    row = _row(modal_price=0.078, min_price=0.05, max_price=0.1)
    fixed, code = apply_price_rules(row, median=16.0)
    assert code is None
    assert (fixed["modal_price"], fixed["min_price"], fixed["max_price"]) == pytest.approx((7.8, 5.0, 10.0))
    assert fixed["quality_components"]["scale_corrected"] is True
    assert row["modal_price"] == 0.078  # input not mutated


def test_far_from_the_crop_median_even_after_rescale_is_quarantined():
    # Onion: 0.014 -> 1.4 against a Rs 29 median is garbage, not a scale slip.
    assert apply_price_rules(_row(modal_price=0.014), 29.0) == (None, "DQ-PRICE-FLOOR")
    # and the other direction: rescaled value far above the median
    assert apply_price_rules(_row(modal_price=0.9), 16.0) == (None, "DQ-PRICE-FLOOR")


def test_no_reference_median_means_quarantine():
    assert apply_price_rules(_row(modal_price=0.078), None) == (None, "DQ-PRICE-FLOOR")


def test_rescale_must_land_at_or_above_one_rupee():
    assert apply_price_rules(_row(modal_price=0.004), 1.0) == (None, "DQ-PRICE-FLOOR")


def test_animal_and_missing_modal_are_left_alone():
    animal = _row(unit="animal", modal_price=0.5)
    assert apply_price_rules(animal, None) == (animal, None)
    unknown = _row(modal_price=None)
    assert apply_price_rules(unknown, None) == (unknown, None)


class _MedianClient(_FakeUpsertClient):
    def __init__(self, medians):
        super().__init__()
        self._medians = medians

    def rpc(self, name, params=None):
        assert name == "crop_price_medians"
        data = [{"crop_id": c, "median": m} for c, m in self._medians.items()]
        return type("R", (), {"execute": lambda self_: FakeResponse(data=data)})()


def test_upsert_rescues_quarantines_and_keeps_good_rows():
    price_writer._median_cache.clear()
    client = _MedianClient({1: 16.0})
    rows = [
        _row(mandi_id=1),
        _row(mandi_id=2, modal_price=0.078, min_price=0.05, max_price=0.1),
        _row(mandi_id=3, modal_price=0.014, min_price=0.01, max_price=0.02),
        _row(mandi_id=4, modal_price=0),
    ]
    result = upsert_price_rows(client, rows, batch_size=100, batch_id="b1", resource=Resource.RESOURCE_2)
    assert result.rows_upserted == 2
    assert result.rows_quarantined == 2
    assert sorted(r["mandi_id"] for r in client.upserted_rows) == [1, 2]
    assert {q["error_code"] for q in client.quarantined} == {"DQ-PRICE-FLOOR", "DQ-PRICE-NONPOSITIVE"}


def test_median_lookup_failure_falls_back_to_quarantine():
    price_writer._median_cache.clear()
    client = _FakeUpsertClient()  # has no rpc(): lookup fails
    result = upsert_price_rows(client, [_row(modal_price=0.078)], batch_size=10,
                               batch_id="b1", resource=Resource.RESOURCE_2)
    assert result.rows_upserted == 0 and result.rows_quarantined == 1


def test_all_livestock_crops_are_per_animal():
    assert {320, 321} <= PER_ANIMAL_CROP_IDS
