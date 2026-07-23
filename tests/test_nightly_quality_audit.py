"""
FarmUncle v2 — tests/test_nightly_quality_audit.py
Phase D.5, Steps 24 (Duplicate tests) and 27 (Coverage tests).

Why these two checks specifically (of the audit's seven):
    `_check_duplicate_business_keys` and `_check_date_coverage` are the
    two with real conditional branches worth pinning down — the
    constraint-present-vs-missing fork in the duplicate check, and the
    gap-detection loop plus its own pagination-limit guard in the
    coverage check. The other five checks (ghost mandis/crops,
    coordinate mismatches, stale failed_pages) are structurally
    identical "query a view, wrap zero-or-more rows into Findings" —
    one of them (`_check_stale_failed_pages`) is included below as a
    single representative test of that shared shape and its
    ConfigError-wrapping behavior, rather than repeating the same
    assertion four more times for no new coverage.

What's tested:
    - `_check_duplicate_business_keys`: constraint-present short-circuit
      (no expensive scan run), constraint-missing fallback (reports the
      real offending row), and the query-failure -> ConfigError wrap.
    - `_check_date_coverage`: no-gap case, a real gap detected inside
      the ingested range, the empty-table (`min_date is None`)
      short-circuit, and the `_DATE_PAGE_SIZE` pagination guard firing
      as a ConfigError rather than silently returning a wrong,
      truncated picture of coverage.
    - `_check_stale_failed_pages`: representative single test of the
      "query -> zero rows is fine, N rows is one aggregate Finding"
      shape shared by the four checks not otherwise covered here.

What's deliberately NOT tested here:
    - `_insert_coverage_report` / `_insert_alert` / `_alert_already_open`
      — these are straightforward inserts against tables already
      exercised in spirit by the checks above; no branching logic of
      their own to pin down.
    - `run_nightly_quality_audit`'s own orchestration (calls all seven
      checks, inserts one coverage_report, inserts N alerts) — every
      piece it calls is covered individually above and in
      test_price_writer.py-style unit tests; an orchestration test
      would mostly be re-asserting Python's own control flow.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from farmuncle_pipeline.config import ConfigError
from farmuncle_pipeline.ops.nightly_quality_audit import (
    _check_date_coverage,
    _check_duplicate_business_keys,
    _check_stale_failed_pages,
)
from tests.fakes import FakeResponse


class _FakeTable:
    """Purpose: minimal fluent stand-in for one `.table(name)` call's
    query chain (`.select().eq().lt().range().execute()`), configured
    per-table with either fixed return data or an exception to raise.
    Every method except `.execute()` is a no-op that returns `self`,
    matching how little of postgrest's actual filtering these checks'
    tests need to exercise (see fakes.py's own rationale for this
    per-file, purpose-built approach over one universal fake)."""

    def __init__(self, data=None, count=None, raises=None):
        self._data = data
        self._count = count
        self._raises = raises

    def select(self, *_a, **_k):
        return self

    def eq(self, *_a, **_k):
        return self

    def lt(self, *_a, **_k):
        return self

    def range(self, *_a, **_k):
        return self

    def execute(self):
        if self._raises is not None:
            raise self._raises
        return FakeResponse(data=self._data, count=self._count)


class _FakeAuditClient:
    """Purpose: routes `.table(name)` to a pre-configured `_FakeTable`
    per view/table name, `AssertionError`ing on any name a given test
    didn't expect — catches a check querying the wrong view/table."""

    def __init__(self, tables: dict[str, _FakeTable]):
        self._tables = tables

    def table(self, name):
        if name not in self._tables:
            raise AssertionError(f"Unexpected table/view queried: {name}")
        return self._tables[name]


# =============================================================================
# _check_duplicate_business_keys
# =============================================================================

def test_duplicate_check_constraint_present_short_circuits_no_scan():
    # The expensive fallback view must never even be queried when the
    # constraint is confirmed present — asserted by simply not
    # registering v_audit_duplicate_price_keys in the fake client at
    # all, so querying it would raise AssertionError.
    client = _FakeAuditClient({
        "v_audit_business_key_constraint": _FakeTable(data=[{"constraint_present": True}]),
    })
    findings, count = _check_duplicate_business_keys(client)
    assert findings == []
    assert count == 0


def test_duplicate_check_constraint_missing_reports_real_offending_row():
    client = _FakeAuditClient({
        "v_audit_business_key_constraint": _FakeTable(data=[{"constraint_present": False}]),
        "v_audit_duplicate_price_keys": _FakeTable(data=[
            {"mandi_id": 5, "crop_id": 9, "variety": "FAQ", "price_date": "2026-07-15", "row_count": 2},
        ]),
    })
    findings, count = _check_duplicate_business_keys(client)
    assert count == 1
    assert len(findings) == 1
    assert findings[0].error_code == "AUDIT-003"
    assert "mandi_id=5" in findings[0].message
    assert "count=2" in findings[0].message


def test_duplicate_check_constraint_missing_no_rows_yet_still_alerts():
    # Constraint gone but no duplicates have actually landed yet —
    # still CRITICAL (nothing is stopping one now), just without a
    # concrete example row in the message.
    client = _FakeAuditClient({
        "v_audit_business_key_constraint": _FakeTable(data=[{"constraint_present": False}]),
        "v_audit_duplicate_price_keys": _FakeTable(data=[]),
    })
    findings, count = _check_duplicate_business_keys(client)
    assert count == 0
    assert len(findings) == 1
    assert "no duplicate rows yet" in findings[0].message


def test_duplicate_check_query_failure_wrapped_in_config_error():
    client = _FakeAuditClient({
        "v_audit_business_key_constraint": _FakeTable(raises=RuntimeError("connection reset")),
    })
    with pytest.raises(ConfigError):
        _check_duplicate_business_keys(client)


# =============================================================================
# _check_date_coverage
# =============================================================================

def test_date_coverage_empty_table_short_circuits():
    client = _FakeAuditClient({
        "v_audit_price_date_bounds": _FakeTable(data=[{"min_date": None, "max_date": None, "distinct_dates": 0}]),
    })
    findings, missing, expected, actual = _check_date_coverage(client)
    assert findings == [] and missing == [] and expected == 0 and actual == 0


def test_date_coverage_no_gap():
    client = _FakeAuditClient({
        "v_audit_price_date_bounds": _FakeTable(
            data=[{"min_date": "2026-07-01", "max_date": "2026-07-03", "distinct_dates": 3}]
        ),
        "v_audit_present_price_dates": _FakeTable(
            data=[{"price_date": d} for d in ("2026-07-01", "2026-07-02", "2026-07-03")]
        ),
    })
    findings, missing, expected, actual = _check_date_coverage(client)
    assert findings == []
    assert missing == []
    assert expected == 3
    assert actual == 3


def test_date_coverage_detects_real_gap_inside_range():
    # 07-02 missing from the middle of an already-ingested range —
    # exactly the "real gap, not an unbackfilled date" case the
    # function's docstring distinguishes.
    client = _FakeAuditClient({
        "v_audit_price_date_bounds": _FakeTable(
            data=[{"min_date": "2026-07-01", "max_date": "2026-07-03", "distinct_dates": 2}]
        ),
        "v_audit_present_price_dates": _FakeTable(
            data=[{"price_date": d} for d in ("2026-07-01", "2026-07-03")]
        ),
    })
    findings, missing, expected, actual = _check_date_coverage(client)
    assert missing == ["2026-07-02"]
    assert expected == 3
    assert actual == 2
    assert len(findings) == 1
    assert findings[0].error_code == "AUDIT-006"
    assert "2026-07-02" in findings[0].message


def test_date_coverage_pagination_guard_raises_config_error():
    # If present_rows comes back at/over _DATE_PAGE_SIZE, the function
    # must refuse to report a (silently truncated, wrong) coverage
    # picture rather than pretend it's complete.
    from farmuncle_pipeline.ops.nightly_quality_audit import _DATE_PAGE_SIZE

    client = _FakeAuditClient({
        "v_audit_price_date_bounds": _FakeTable(
            data=[{"min_date": "2026-01-01", "max_date": "2028-01-01", "distinct_dates": _DATE_PAGE_SIZE}]
        ),
        "v_audit_present_price_dates": _FakeTable(
            data=[{"price_date": "2026-01-01"}] * _DATE_PAGE_SIZE
        ),
    })
    with pytest.raises(ConfigError, match="_DATE_PAGE_SIZE needs raising"):
        _check_date_coverage(client)


def test_date_coverage_bounds_query_failure_wrapped_in_config_error():
    client = _FakeAuditClient({
        "v_audit_price_date_bounds": _FakeTable(raises=RuntimeError("timeout")),
    })
    with pytest.raises(ConfigError):
        _check_date_coverage(client)


# =============================================================================
# _check_stale_failed_pages (representative of the shared "query,
# eq/lt filter, count -> 0-or-1 Findings" shape)
# =============================================================================

def test_stale_failed_pages_zero_is_no_finding():
    client = _FakeAuditClient({"failed_pages": _FakeTable(count=0)})
    findings, count = _check_stale_failed_pages(client)
    assert findings == [] and count == 0


def test_stale_failed_pages_nonzero_raises_one_aggregate_finding():
    client = _FakeAuditClient({"failed_pages": _FakeTable(count=7)})
    findings, count = _check_stale_failed_pages(client)
    assert count == 7
    assert len(findings) == 1
    assert findings[0].error_code == "AUDIT-005"
    assert "7 failed_pages" in findings[0].message


def test_stale_failed_pages_query_failure_wrapped_in_config_error():
    client = _FakeAuditClient({"failed_pages": _FakeTable(raises=RuntimeError("boom"))})
    with pytest.raises(ConfigError):
        _check_stale_failed_pages(client)
