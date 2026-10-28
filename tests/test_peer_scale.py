"""Peer comparison: a price 100x below the same crop elsewhere is multiplied back."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from farmuncle_pipeline.config import Source
from farmuncle_pipeline.core.identity_client import IdentityClient
from farmuncle_pipeline.core.peer_scale import peer_scale
from farmuncle_pipeline.core.record_processor import process_records
from tests.test_identity_client import FakeIdentityDBClient
from tests.test_record_processor import _KNOWN_CROP, _KNOWN_MANDI, _RESOURCE_1_RECORD, _make_identity


# --- the pure rule -----------------------------------------------------------

def test_spinach_example_from_the_owner():
    # Peers: Rs 4000/quintal -> Rs 40/kg. One mandi typed Rs 40/kg, which became 0.4.
    assert peer_scale(0.4, 0.3, 0.5, median=40.0) == (100, 40.0, 30.0, 50.0)


def test_only_x100_is_ever_applied():
    # 0.04 would need x1000 to reach 40: not guessed, left alone.
    assert peer_scale(0.04, 0.03, 0.05, median=40.0) is None


def test_jaggery_style_price_a_bit_above_the_median_is_accepted():
    # Gur(Jaggery): 1.0 -> 100 against a median of 43 is a real price.
    assert peer_scale(1.0, 1.0, 1.0, median=43.0)[1] == 100.0


def test_normal_price_is_left_alone():
    assert peer_scale(38.0, 30.0, 45.0, median=40.0) is None


def test_a_merely_cheap_price_is_left_alone():
    # 10x below the median is within natural spread (variety, grade, season): no mechanism, no fix.
    assert peer_scale(3.0, 2.0, 4.0, median=40.0) is None
    assert peer_scale(2.0, 1.0, 3.0, median=40.0) is None


def test_no_peer_median_means_no_change():
    assert peer_scale(0.4, 0.3, 0.5, median=None) is None


def test_no_clean_power_of_ten_means_no_change():
    assert peer_scale(0.005, 0.005, 0.005, median=40.0) is None  # x1000 = 5, x100 = 0.5: both far off


def test_float_noise_is_rounded_away():
    assert peer_scale(0.07, 0.07, 0.07, median=7.0)[1] == 7.0  # 0.07*100 is 7.000000000000001


def test_missing_modal_is_left_alone():
    assert peer_scale(None, None, None, median=40.0) is None


def test_missing_min_and_max_stay_missing():
    assert peer_scale(0.4, None, None, median=40.0) == (100, 40.0, None, None)


# --- wired into the pipeline -------------------------------------------------

def _identity(medians=None, per_kg=()):
    identity = _make_identity(mandis=[_KNOWN_MANDI], crops=[_KNOWN_CROP])
    identity.crop_medians = medians or {}
    identity.per_kg_pairs = frozenset(per_kg)
    return identity


def _process(identity, **overrides):
    return process_records(
        [{**_RESOURCE_1_RECORD, **overrides}], identity=identity, unit="kg", source=Source.RESOURCE_1,
        batch_id="b", raw_api_batch_id="r", job_name="t",
    ).price_rows[0]


def test_per_kg_entry_is_restored_end_to_end():
    # raw 40/kg typed by one mandi: /100 = 0.4 -> back to 40, flagged.
    row = _process(_identity({10: 40.0}), modal_price="40", min_price="30", max_price="50")
    assert (row["modal_price"], row["min_price"], row["max_price"]) == (40.0, 30.0, 50.0)
    assert row["quality_components"]["peer_scaled"] == 100


def test_normal_quintal_price_is_untouched_end_to_end():
    row = _process(_identity({10: 40.0}), modal_price="4000", min_price="3500", max_price="4500")
    assert row["modal_price"] == 40.0
    assert "peer_scaled" not in row["quality_components"]


def test_learned_per_kg_pair_is_not_scaled_twice():
    row = _process(_identity({10: 40.0}, per_kg=[(42, 10)]), modal_price="40", min_price="30", max_price="50")
    assert row["modal_price"] == 40.0
    assert row["quality_components"]["unit_reported_kg"] is True
    assert "peer_scaled" not in row["quality_components"]


def test_without_medians_the_old_behaviour_holds():
    assert _process(_identity({}), modal_price="40", min_price="30", max_price="50")["modal_price"] == 0.4


def test_preload_loads_crop_medians_and_survives_failure():
    class _Rpc(FakeIdentityDBClient):
        def rpc(self, name, params=None):
            if name == "crop_price_medians":
                data = [{"crop_id": 10, "median": "40.5"}]
                return type("R", (), {"execute": lambda s: type("D", (), {"data": data})()})()
            return super().rpc(name, params or {})

    ok = IdentityClient(_Rpc(mandis=[_KNOWN_MANDI], crops=[_KNOWN_CROP], rpc_responses={}))
    ok.preload()
    assert ok.crop_medians == {10: 40.5}
    assert _make_identity(mandis=[_KNOWN_MANDI], crops=[_KNOWN_CROP]).crop_medians == {}


# --- too high: a price 100x above the crop elsewhere is divided back -----------------------------

def test_a_price_100_times_too_high_is_divided_by_100():
    assert peer_scale(4000.0, 3500.0, 4500.0, median=40.0) == (0.01, 40.0, 35.0, 45.0)


def test_a_merely_expensive_price_is_left_alone():
    assert peer_scale(80.0, 70.0, 90.0, median=40.0) is None      # 2x
    assert peer_scale(400.0, 350.0, 450.0, median=40.0) is None   # 10x: /100 = 4, no match


def test_too_high_end_to_end():
    row = _process(_identity({10: 40.0}), modal_price="400000", min_price="350000", max_price="450000")
    assert (row["modal_price"], row["min_price"], row["max_price"]) == (40.0, 35.0, 45.0)
    assert row["quality_components"]["peer_scaled"] == 0.01
