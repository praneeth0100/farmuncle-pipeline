"""Price sanity rules applied before upsert, the per-kg-reported rule, and the market blocklist."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from farmuncle_pipeline.config import PER_ANIMAL_CROP_IDS, Resource, Source
from farmuncle_pipeline.core.price_writer import price_rule_violation, upsert_price_rows
from farmuncle_pipeline.core.record_processor import process_records
from farmuncle_pipeline.core.resource_client import parse_agmarknet_record
from tests.test_price_writer import _FakeUpsertClient, _price_row
from tests.test_record_processor import _KNOWN_CROP, _KNOWN_MANDI, _RESOURCE_1_RECORD, _make_identity


def _row(**kw):
    return {**_price_row(), "unit": "kg", "modal_price": 40.0, **kw}


def _process(**overrides):
    identity = _make_identity(mandis=[_KNOWN_MANDI], crops=[_KNOWN_CROP])
    return process_records(
        [{**_RESOURCE_1_RECORD, **overrides}], identity=identity, unit="kg", source=Source.RESOURCE_1,
        batch_id="b", raw_api_batch_id="r", job_name="t",
    )


# --- hard price rules -------------------------------------------------------

def test_normal_row_has_no_violation():
    assert price_rule_violation(_row()) is None


def test_zero_or_negative_modal_is_rejected():
    assert price_rule_violation(_row(modal_price=0)) == "DQ-PRICE-NONPOSITIVE"
    assert price_rule_violation(_row(modal_price=-5)) == "DQ-PRICE-NONPOSITIVE"


def test_kg_price_under_one_rupee_is_rejected():
    assert price_rule_violation(_row(modal_price=0.01)) == "DQ-PRICE-FLOOR"


def test_animal_and_missing_modal_are_left_alone():
    assert price_rule_violation(_row(unit="animal", modal_price=0.5)) is None
    assert price_rule_violation(_row(modal_price=None)) is None


def test_bad_rows_are_quarantined_without_touching_the_table():
    client = _FakeUpsertClient()
    rows = [_row(mandi_id=1), _row(mandi_id=2, modal_price=0.01), _row(mandi_id=3, modal_price=0)]
    result = upsert_price_rows(client, rows, batch_size=100, batch_id="b1", resource=Resource.RESOURCE_2)
    assert result.rows_upserted == 1
    assert result.rows_quarantined == 2
    assert [r["mandi_id"] for r in client.upserted_rows] == [1]
    assert {q["error_code"] for q in client.quarantined} == {"DQ-PRICE-FLOOR", "DQ-PRICE-NONPOSITIVE"}


# --- government prices are per quintal, except a few markets that report per kg ---

_KG_MARKET = {"market": "Pune(Manjri) APMC", "state": "Maharashtra"}


def test_quintal_price_is_divided_by_100():
    row = _process(modal_price="2500", min_price="2000", max_price="3000").price_rows[0]
    assert (row["modal_price"], row["min_price"], row["max_price"]) == (25.0, 20.0, 30.0)
    assert "unit_reported_kg" not in row["quality_components"]


def test_per_kg_market_price_under_100_is_not_divided_again():
    row = _process(modal_price="38", min_price="30", max_price="45", **_KG_MARKET).price_rows[0]
    assert (row["modal_price"], row["min_price"], row["max_price"]) == (38.0, 30.0, 45.0)
    assert row["quality_components"]["unit_reported_kg"] is True


def test_per_kg_market_still_divides_its_quintal_prices():
    row = _process(modal_price="2500", min_price="2000", max_price="3000", **_KG_MARKET).price_rows[0]
    assert row["modal_price"] == 25.0
    assert "unit_reported_kg" not in row["quality_components"]


def test_other_markets_never_skip_the_division_even_for_small_prices():
    # Guntur is not a per-kg market: 38 is a slip and becomes 0.38, which the writer quarantines.
    row = _process(modal_price="38", min_price="30", max_price="45").price_rows[0]
    assert row["modal_price"] == 0.38
    assert "unit_reported_kg" not in row["quality_components"]


def test_exactly_100_in_a_per_kg_market_is_a_quintal_price():
    assert _process(modal_price="100", min_price="90", max_price="110", **_KG_MARKET).price_rows[0]["modal_price"] == 1.0


# --- blocked markets ---------------------------------------------------------

def test_blocked_market_is_dropped_by_the_parser():
    assert parse_agmarknet_record({**_RESOURCE_1_RECORD, "market": "Patti APMC", "state": "Punjab"}) is None
    assert parse_agmarknet_record({**_RESOURCE_1_RECORD, "market": " patti apmc ", "state": "punjab"}) is None
    assert parse_agmarknet_record({**_RESOURCE_1_RECORD, "market": "Patti APMC", "state": "Haryana"}) is not None


def test_blocked_market_is_skipped_silently_not_counted_as_a_failure():
    identity = _make_identity(mandis=[_KNOWN_MANDI], crops=[_KNOWN_CROP])
    result = process_records(
        [{**_RESOURCE_1_RECORD, "market": "Patti APMC", "state": "Punjab"}], identity=identity,
        unit="kg", source=Source.RESOURCE_1, batch_id="b", raw_api_batch_id="r", job_name="t",
    )
    assert result.price_rows == [] and result.rows_failed == 0


def test_all_livestock_crops_are_per_animal():
    assert {320, 321} <= PER_ANIMAL_CROP_IDS
