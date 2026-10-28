"""
tests/test_api_params.py

One function builds the government API query for every job. The two
resources use different filter field names (Resource 1: lowercase,
`state.keyword`; Resource 2: Title_Case). The retry job used lowercase
names for both, so a retried Resource 2 page ignored its filters and
came back as a nationwide page that was then marked RESOLVED
(audit fault 3).
"""

from __future__ import annotations

from farmuncle_pipeline.config import Resource
from farmuncle_pipeline.core.api_params import build_params
from farmuncle_pipeline.core.batch_lifecycle import insert_failed_page
from farmuncle_pipeline.ingestion.retry_failed_pages import _retry_state

from tests.fakes import FakeResponse


def _p(resource, **kw):
    base = dict(resource=resource, api_key="k", page_size=500, offset=0, date_str="01/08/2026")
    base.update(kw)
    return build_params(**base)


def test_resource_1_names():
    p = _p(Resource.RESOURCE_1, state="Kerala", offset=1000)
    assert p == {
        "api-key": "k",
        "format": "json",
        "limit": 500,
        "offset": 1000,
        "filters[arrival_date]": "01/08/2026",
        "filters[state.keyword]": "Kerala",
    }


def test_resource_2_names():
    p = _p(Resource.RESOURCE_2, state="Kerala")
    assert p == {
        "api-key": "k",
        "format": "json",
        "limit": 500,
        "offset": 0,
        "filters[Arrival_Date]": "01/08/2026",
        "filters[State]": "Kerala",
    }


def test_no_state_means_no_state_filter():
    assert not any("tate" in k for k in _p(Resource.RESOURCE_1))
    assert not any("tate" in k for k in _p(Resource.RESOURCE_2))


def test_the_two_resources_never_share_filter_names():
    f1 = {k for k in _p(Resource.RESOURCE_1, state="x") if k.startswith("filters")}
    f2 = {k for k in _p(Resource.RESOURCE_2, state="x") if k.startswith("filters")}
    assert f1.isdisjoint(f2)


class _Table:
    def __init__(self, sink):
        self.sink = sink

    def insert(self, payload):
        self.sink.append(payload)
        return self

    def execute(self):
        return FakeResponse(data=[{}])


class _Client:
    def __init__(self):
        self.inserted = []

    def table(self, name):
        assert name == "failed_pages"
        return _Table(self.inserted)


def test_failed_page_records_the_state_when_given():
    c = _Client()
    insert_failed_page(
        c, batch_id="b", resource=Resource.RESOURCE_1, page=2,
        error_code="INGEST-002", error_message="boom", state="Kerala",
    )
    assert c.inserted[0]["state"] == "Kerala"
    assert c.inserted[0]["page"] == 2


def test_failed_page_without_state_has_no_state_key():
    c = _Client()
    insert_failed_page(
        c, batch_id="b", resource=Resource.RESOURCE_2, page=1,
        error_code="INGEST-002", error_message="boom",
    )
    assert "state" not in c.inserted[0]


def test_retry_state_prefers_the_column():
    fp = {"resource": "resource_1", "state": "Kerala", "error_message": "x"}
    assert _retry_state(fp) == "Kerala"


def test_retry_state_falls_back_to_the_old_message_prefix():
    fp = {"resource": "resource_2", "state": None, "error_message": "[state=Assam] timeout"}
    assert _retry_state(fp) == "Assam"


def test_retry_state_is_none_when_unknown():
    assert _retry_state({"resource": "resource_1", "state": None, "error_message": "timeout"}) is None
    assert _retry_state({"resource": "resource_1", "error_message": None}) is None
