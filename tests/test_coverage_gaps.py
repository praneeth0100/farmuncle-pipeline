"""
tests/test_coverage_gaps.py

A state whose collected count is below the government's own total used to be
only printed (QUALITY-001), so it was never retried. It is now recorded as a
failed_pages row (page 1 of that state, so a retry re-fetches the whole
state) at most once per resource, state and date.
"""

from __future__ import annotations

from farmuncle_pipeline.config import Resource
from farmuncle_pipeline.core.batch_lifecycle import insert_coverage_gap

from tests.fakes import FakeResponse


class _Table:
    def __init__(self, db, name):
        self.db, self.name = db, name
        self._eq, self._in = [], []
        self._insert = None

    def select(self, *_):
        return self

    def eq(self, col, val):
        self._eq.append((col, val))
        return self

    def in_(self, col, vals):
        self._in.append((col, list(vals)))
        return self

    def insert(self, payload):
        self._insert = payload
        return self

    def execute(self):
        if self._insert is not None:
            self.db.inserted.append((self.name, self._insert))
            return FakeResponse(data=[self._insert])
        rows = self.db.rows[self.name]
        for col, val in self._eq:
            rows = [r for r in rows if r.get(col) == val]
        for col, vals in self._in:
            rows = [r for r in rows if r.get(col) in vals]
        return FakeResponse(data=rows)


class _Db:
    def __init__(self, failed_pages=(), batches=()):
        self.rows = {"failed_pages": list(failed_pages), "ingestion_batches": list(batches)}
        self.inserted = []

    def table(self, name):
        return _Table(self, name)


def _gap(db, **kw):
    args = dict(
        batch_id="b2", resource=Resource.RESOURCE_1, state="Karnataka",
        collected=400, reported_total=576, date_str="01/08/2026",
    )
    args.update(kw)
    return insert_coverage_gap(db, **args)


def test_a_new_gap_is_recorded_as_page_one_of_the_state():
    db = _Db()
    assert _gap(db) is True
    table, row = db.inserted[0]
    assert table == "failed_pages"
    assert row["state"] == "Karnataka"
    assert row["page"] == 1
    assert row["error_code"] == "QUALITY-001"
    assert row["status"] == "PENDING"
    assert row["resource"] == "resource_1"
    assert "400" in row["error_message"] and "576" in row["error_message"]


def test_the_same_gap_on_the_same_date_is_not_recorded_twice():
    db = _Db(
        failed_pages=[{"id": 1, "batch_id": "b1", "resource": "resource_1", "state": "Karnataka",
                       "error_code": "QUALITY-001", "status": "PENDING"}],
        batches=[{"id": "b1", "date_range_start": "2026-08-01"}],
    )
    assert _gap(db) is False
    assert db.inserted == []


def test_a_pending_gap_for_another_date_does_not_block():
    db = _Db(
        failed_pages=[{"id": 1, "batch_id": "b1", "resource": "resource_1", "state": "Karnataka",
                       "error_code": "QUALITY-001", "status": "PENDING"}],
        batches=[{"id": "b1", "date_range_start": "2026-07-31"}],
    )
    assert _gap(db) is True


def test_a_resolved_gap_does_not_block():
    db = _Db(
        failed_pages=[{"id": 1, "batch_id": "b1", "resource": "resource_1", "state": "Karnataka",
                       "error_code": "QUALITY-001", "status": "RESOLVED"}],
        batches=[{"id": "b1", "date_range_start": "2026-08-01"}],
    )
    assert _gap(db) is True


def test_a_different_state_or_resource_does_not_block():
    base = {"id": 1, "batch_id": "b1", "error_code": "QUALITY-001", "status": "PENDING"}
    db = _Db(
        failed_pages=[dict(base, resource="resource_1", state="Assam"),
                      dict(base, resource="resource_2", state="Karnataka")],
        batches=[{"id": "b1", "date_range_start": "2026-08-01"}],
    )
    assert _gap(db) is True
