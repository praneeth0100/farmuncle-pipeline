"""
tests/test_resource_client.py

fetch_page must never report ok=True for a body that is not a real
{"records": [...]} page. data.gov.in answers quota and key errors with
HTTP 200 and a different body; treating that as "zero records" silently
drops a whole state-day (ADR 0002 / audit fault 2).
"""

from __future__ import annotations

import pytest
import requests

from farmuncle_pipeline.core import resource_client
from farmuncle_pipeline.core.resource_client import fetch_page


class _Resp:
    def __init__(self, body=None, status: int = 200, json_error: bool = False) -> None:
        self._body = body
        self.status_code = status
        self._json_error = json_error

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"{self.status_code}")

    def json(self):
        if self._json_error:
            raise ValueError("not json")
        return self._body


def _call(monkeypatch, responses, *, max_retries: int = 1):
    calls = []
    queue = list(responses)

    def fake_get(url, params=None, headers=None, timeout=None):
        calls.append(params)
        item = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(resource_client.requests, "get", fake_get)
    result = fetch_page(
        url="https://example.test/r",
        params={"offset": 0},
        headers={},
        timeout=1,
        max_retries=max_retries,
        retry_delay_seconds=0,
    )
    return result, calls


def test_valid_page_is_ok(monkeypatch):
    result, calls = _call(monkeypatch, [_Resp({"records": [{"a": 1}], "total": 1})])
    assert result.ok is True
    assert result.records == [{"a": 1}]
    assert len(calls) == 1


def test_genuinely_empty_page_is_ok(monkeypatch):
    result, _ = _call(monkeypatch, [_Resp({"records": [], "total": 0})])
    assert result.ok is True
    assert result.records == []


@pytest.mark.parametrize(
    "body",
    [
        {"message": "API rate limit exceeded"},        # quota error as HTTP 200
        {"error": "invalid api key", "status": "error"},
        {},                                             # empty object
        {"records": None},
        {"records": "oops"},                            # records not a list
        {"records": {"a": 1}},
        ["not", "a", "dict"],
        None,
    ],
)
def test_200_without_a_records_list_is_a_failed_page(monkeypatch, body):
    result, calls = _call(monkeypatch, [_Resp(body)], max_retries=2)
    assert result.ok is False
    assert result.records == []
    assert result.error
    assert len(calls) == 3  # first try plus 2 retries


def test_error_body_is_quoted_in_the_error(monkeypatch):
    result, _ = _call(monkeypatch, [_Resp({"message": "quota exceeded for key"})], max_retries=0)
    assert result.ok is False
    assert "quota exceeded for key" in result.error


def test_error_body_in_the_message_is_truncated(monkeypatch):
    result, _ = _call(monkeypatch, [_Resp({"message": "x" * 5000})], max_retries=0)
    assert result.ok is False
    assert len(result.error) < 600


def test_bad_page_then_good_page_recovers_on_retry(monkeypatch):
    result, calls = _call(
        monkeypatch,
        [_Resp({"message": "slow down"}), _Resp({"records": [{"a": 1}]})],
        max_retries=1,
    )
    assert result.ok is True
    assert result.records == [{"a": 1}]
    assert len(calls) == 2


def test_http_error_is_a_failed_page(monkeypatch):
    result, _ = _call(monkeypatch, [_Resp({"records": []}, status=503)], max_retries=0)
    assert result.ok is False


def test_invalid_json_is_a_failed_page(monkeypatch):
    result, _ = _call(monkeypatch, [_Resp(json_error=True)], max_retries=0)
    assert result.ok is False
    assert "invalid JSON" in result.error


def test_timeout_is_a_failed_page(monkeypatch):
    result, _ = _call(monkeypatch, [requests.exceptions.Timeout("slow")], max_retries=0)
    assert result.ok is False
    assert "timeout" in result.error
