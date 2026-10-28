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


def _process(per_kg=False, **overrides):
    identity = _make_identity(mandis=[_KNOWN_MANDI], crops=[_KNOWN_CROP])
    if per_kg:  # mandi 42 / crop 10 is a learned per-kg pair
        identity.per_kg_pairs = frozenset({(42, 10)})
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


# --- government prices are per quintal, except market+crop pairs learned to report per kg ---

def test_quintal_price_is_divided_by_100():
    row = _process(modal_price="2500", min_price="2000", max_price="3000").price_rows[0]
    assert (row["modal_price"], row["min_price"], row["max_price"]) == (25.0, 20.0, 30.0)
    assert "unit_reported_kg" not in row["quality_components"]


def test_learned_per_kg_pair_under_100_is_not_divided_again():
    row = _process(per_kg=True, modal_price="38", min_price="30", max_price="45").price_rows[0]
    assert (row["modal_price"], row["min_price"], row["max_price"]) == (38.0, 30.0, 45.0)
    assert row["quality_components"]["unit_reported_kg"] is True


def test_learned_per_kg_pair_still_divides_a_quintal_sized_price():
    row = _process(per_kg=True, modal_price="2500", min_price="2000", max_price="3000").price_rows[0]
    assert row["modal_price"] == 25.0
    assert "unit_reported_kg" not in row["quality_components"]


def test_unknown_pair_never_skips_the_division_even_for_a_small_price():
    # Not a learned pair: 38 is a slip and becomes 0.38, which the writer quarantines.
    row = _process(modal_price="38", min_price="30", max_price="45").price_rows[0]
    assert row["modal_price"] == 0.38
    assert "unit_reported_kg" not in row["quality_components"]


def test_exactly_100_is_a_quintal_price():
    assert _process(per_kg=True, modal_price="100", min_price="90", max_price="110").price_rows[0]["modal_price"] == 1.0


def test_preload_learns_per_kg_pairs_from_the_database():
    from tests.test_identity_client import FakeIdentityDBClient
    from farmuncle_pipeline.core.identity_client import IdentityClient

    class _Rpc(FakeIdentityDBClient):
        def rpc(self, name, params=None):
            if name == "per_kg_pairs":
                data = [{"mandi_id": 42, "crop_id": 10}]
                return type("R", (), {"execute": lambda s: type("D", (), {"data": data})()})()
            return super().rpc(name, params or {})

    identity = IdentityClient(_Rpc(mandis=[_KNOWN_MANDI], crops=[_KNOWN_CROP], rpc_responses={}))
    identity.preload()
    assert identity.per_kg_pairs == frozenset({(42, 10)})


def test_preload_survives_a_failing_pair_lookup_by_assuming_quintal():
    identity = _make_identity(mandis=[_KNOWN_MANDI], crops=[_KNOWN_CROP])  # fake has no per_kg_pairs
    assert identity.per_kg_pairs == frozenset()


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
