"""
tests/test_state_names.py

data.gov.in has renamed states over time (Kerala -> Keralam, Uttrakhand ->
Uttarakhand, Pondicherry -> Puducherry). Querying only today's names returns
nothing for older dates and, worse, looks like a success. Two defences:

1. every spelling is queried and mapped to one canonical state, so identity
   resolution never creates a second set of mandis under an old spelling;
2. the sum of the per-state totals is compared with the nationwide total, so
   a state name we do not query at all raises an alert.
"""

from __future__ import annotations

import pytest

from farmuncle_pipeline.config import Resource
from farmuncle_pipeline.core import state_coverage
from farmuncle_pipeline.core.raw_dedup import upsert_raw_price_entries_batch
from farmuncle_pipeline.core.resource_client import PageFetchResult, parse_agmarknet_record
from farmuncle_pipeline.core.state_coverage import (
    alert_state_coverage_gap,
    fetch_nationwide_total,
    gap_size,
)
from farmuncle_pipeline.government_constants import QUERY_STATES, STATES, canonical_state

from tests.fakes import FakeResponse


# ---- spellings -------------------------------------------------------------

def test_every_canonical_state_is_queried():
    for s in STATES:
        assert s in QUERY_STATES


@pytest.mark.parametrize(
    "old,new",
    [("Kerala", "Keralam"), ("Uttrakhand", "Uttarakhand"), ("Pondicherry", "Puducherry")],
)
def test_old_spellings_are_queried_and_mapped(old, new):
    assert old in QUERY_STATES
    assert new in STATES
    assert canonical_state(old) == new


def test_unknown_and_current_names_pass_through():
    assert canonical_state("Karnataka") == "Karnataka"
    assert canonical_state("  Assam ") == "Assam"
    assert canonical_state("Ladakh") == "Ladakh"


def test_no_duplicate_spellings():
    assert len(QUERY_STATES) == len(set(QUERY_STATES))


# ---- parsing keeps the raw spelling, uses the canonical one ---------------

def _rec(state):
    return {
        "State": state, "District": "Thrissur", "Market": "Mannuthy", "Commodity": "Banana",
        "Variety": "Nendran", "Grade": "FAQ", "Arrival_Date": "01/06/2024",
        "Min_Price": "4000", "Max_Price": "5000", "Modal_Price": "4500",
    }


def test_parse_maps_state_but_keeps_the_raw_one():
    parsed = parse_agmarknet_record(_rec("Kerala"))
    assert parsed["state"] == "Keralam"
    assert parsed["raw_state"] == "Kerala"


def test_parse_of_a_current_name_is_unchanged():
    parsed = parse_agmarknet_record(_rec("Karnataka"))
    assert parsed["state"] == parsed["raw_state"] == "Karnataka"


class _Rpc:
    def __init__(self):
        self.entries = None

    def rpc(self, name, args):
        self.entries = args["p_entries"]
        return self

    def execute(self):
        return FakeResponse(data=[])


def test_raw_storage_keeps_the_government_spelling():
    c = _Rpc()
    upsert_raw_price_entries_batch(
        c, resource="resource_2", batch_id="b", parser_version=1,
        parsed_records=[parse_agmarknet_record(_rec("Kerala"))],
    )
    assert c.entries[0]["state"] == "Kerala"


# ---- coverage gap ---------------------------------------------------------

def test_no_gap_when_totals_match():
    assert gap_size(18700, 18700) == 0


def test_small_difference_is_within_tolerance():
    assert gap_size(18700, 18500) == 0          # 1.07 %
    assert gap_size(100, 90) == 0               # under the 25-record floor


def test_a_missing_state_is_a_gap():
    assert gap_size(18700, 17000) == 1700


def test_covered_more_than_nationwide_is_not_a_gap():
    assert gap_size(100, 150) == 0


def test_unknown_nationwide_total_is_not_a_gap():
    assert gap_size(None, 500) == 0


class _Alerts:
    """Fake of the quality_alerts table: an existing OPEN alert list and an insert log."""

    def __init__(self, existing=None):
        self.existing = existing or []
        self.inserted = []
        self.filters = []

    def table(self, name):
        assert name == "quality_alerts"
        return self

    def select(self, *_):
        return self

    def eq(self, col, val):
        self.filters.append((col, val))
        return self

    def like(self, col, val):
        self.filters.append((col, val))
        return self

    def limit(self, _):
        return self

    def insert(self, payload):
        self.inserted.append(payload)
        return self

    def execute(self):
        return FakeResponse(data=self.existing if self.inserted == [] else [{}])


def test_alert_is_written_once_per_resource_and_day():
    c = _Alerts()
    assert alert_state_coverage_gap(
        c, batch_id="b", resource=Resource.RESOURCE_2, date_str="01/08/2026",
        gap=1700, nationwide_total=18700, covered_total=17000,
    ) is True
    row = c.inserted[0]
    assert row["severity"] == "HIGH"
    assert row["error_code"] == "QUALITY-002"
    assert "resource_2" in row["message"] and "01/08/2026" in row["message"]
    assert "1700" in row["message"]


def test_alert_is_not_repeated_while_one_is_open():
    c = _Alerts(existing=[{"id": 1}])
    assert alert_state_coverage_gap(
        c, batch_id="b", resource=Resource.RESOURCE_2, date_str="01/08/2026",
        gap=1700, nationwide_total=18700, covered_total=17000,
    ) is False
    assert c.inserted == []


def test_fetch_nationwide_total_reads_total(monkeypatch):
    seen = {}

    def fake_fetch(**kw):
        seen.update(kw["params"])
        return PageFetchResult(ok=True, records=[{}], raw_response={"total": 18700}, error=None, duration_ms=1)

    monkeypatch.setattr(state_coverage, "fetch_page", fake_fetch)
    total = fetch_nationwide_total(
        resource=Resource.RESOURCE_1, api_key="k", date_str="01/08/2026",
        url="u", headers={}, timeout=1, max_retries=0, retry_delay_seconds=0,
    )
    assert total == 18700
    assert seen["limit"] == 1
    assert "filters[arrival_date]" in seen
    assert not any("tate" in k for k in seen)


def test_fetch_nationwide_total_is_none_when_the_call_fails(monkeypatch):
    monkeypatch.setattr(
        state_coverage, "fetch_page",
        lambda **kw: PageFetchResult(ok=False, records=[], raw_response={}, error="x", duration_ms=1),
    )
    assert fetch_nationwide_total(
        resource=Resource.RESOURCE_2, api_key="k", date_str="d",
        url="u", headers={}, timeout=1, max_retries=0, retry_delay_seconds=0,
    ) is None
