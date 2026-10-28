"""Fail-fast breaker, host fallback and the run-level alerts."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from farmuncle_pipeline.config import Resource
from farmuncle_pipeline.core import resource_client
from farmuncle_pipeline.core.outage import (
    OutageBreaker,
    alert_empty_feed,
    alert_feed_outage,
    alert_quarantine_flood,
    quarantine_flood,
)
from farmuncle_pipeline.core.resource_client import alternate_url, fetch_page
from tests.fakes import FakeResponse


# --- breaker -----------------------------------------------------------------

def test_breaker_trips_after_consecutive_failures_and_resets_on_success():
    b = OutageBreaker(max_consecutive=3)
    for ok in (False, False, True, False, False):
        b.record(ok)
    assert not b.tripped
    b.record(False)
    assert b.tripped


# --- host fallback -----------------------------------------------------------

PRIMARY = "https://www.data.gov.in/backend/dataapi/v1/resource/abc"
LEGACY = "https://api.data.gov.in/resource/abc"


def test_alternate_url_swaps_the_two_known_hosts_only():
    assert alternate_url(PRIMARY) == LEGACY
    assert alternate_url(LEGACY) == PRIMARY
    assert alternate_url("https://example.test/r") is None


class _Resp:
    status_code = 200

    def raise_for_status(self):
        pass

    def json(self):
        return {"records": [{"a": 1}], "total": 1}


def _fetch(monkeypatch, behaviour):
    seen = []

    def fake_get(url, params=None, headers=None, timeout=None):
        seen.append(url)
        outcome = behaviour(url)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(resource_client.requests, "get", fake_get)
    monkeypatch.setattr(resource_client, "_swapped", False)
    result = fetch_page(url=PRIMARY, params={}, headers={}, timeout=1, max_retries=1, retry_delay_seconds=0)
    return result, seen


def test_falls_back_to_the_other_host_when_the_first_is_unreachable(monkeypatch):
    def behaviour(url):
        return requests.exceptions.ConnectionError("refused") if url == PRIMARY else _Resp()

    result, seen = _fetch(monkeypatch, behaviour)
    assert result.ok
    assert seen == [PRIMARY, PRIMARY, LEGACY]  # primary + its retry, then one try on the other host


def test_remembers_the_working_host_for_the_next_call(monkeypatch):
    def behaviour(url):
        return requests.exceptions.ConnectionError("refused") if url == PRIMARY else _Resp()

    _fetch(monkeypatch, behaviour)
    seen = []
    monkeypatch.setattr(resource_client.requests, "get", lambda url, **k: (seen.append(url), _Resp())[1])
    fetch_page(url=PRIMARY, params={}, headers={}, timeout=1, max_retries=1, retry_delay_seconds=0)
    assert seen == [LEGACY]


def test_http_errors_do_not_trigger_the_fallback(monkeypatch):
    class _Bad(_Resp):
        status_code = 403

        def raise_for_status(self):
            raise requests.exceptions.HTTPError("403 Client Error: Forbidden")

    result, seen = _fetch(monkeypatch, lambda url: _Bad())
    assert not result.ok and LEGACY not in seen


# --- alerts ------------------------------------------------------------------

class _AlertDb:
    def __init__(self, open_rows=()):
        self.open_rows = list(open_rows)
        self.inserted = []

    def table(self, name):
        assert name == "quality_alerts"
        return self

    def select(self, *a, **k):
        return self

    def eq(self, *a, **k):
        return self

    def like(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return self

    def insert(self, row):
        self.inserted.append(row)
        return self

    def execute(self):
        return FakeResponse(data=self.open_rows)


def test_quarantine_flood_needs_both_a_share_and_a_minimum_count():
    assert quarantine_flood(rows=1000, quarantined=60)           # 6%
    assert not quarantine_flood(rows=1000, quarantined=30)       # 3%
    assert not quarantine_flood(rows=40, quarantined=10)         # 25% but only 10 rows


def test_flood_alert_is_written_once():
    db = _AlertDb()
    assert alert_quarantine_flood(db, batch_id="b", resource=Resource.RESOURCE_1, rows=1000, quarantined=80)
    assert db.inserted[0]["error_code"] == "QUALITY-003" and db.inserted[0]["severity"] == "HIGH"
    again = _AlertDb(open_rows=[{"id": 1}])
    assert not alert_quarantine_flood(again, batch_id="b", resource=Resource.RESOURCE_1, rows=1000, quarantined=80)
    assert again.inserted == []


def test_outage_alert_names_the_failure():
    db = _AlertDb()
    assert alert_feed_outage(db, batch_id="b", resource=Resource.RESOURCE_2, failures=6, last_error="timeout: x")
    assert db.inserted[0]["error_code"] == "QUALITY-005"
    assert "6" in db.inserted[0]["message"] and "timeout" in db.inserted[0]["message"]


def test_empty_feed_alert():
    db = _AlertDb()
    assert alert_empty_feed(db, batch_id="b", resource=Resource.RESOURCE_1, date_str="29-10-2026")
    assert db.inserted[0]["error_code"] == "QUALITY-004"
