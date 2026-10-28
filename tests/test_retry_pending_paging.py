"""
tests/test_retry_pending_paging.py

PostgREST returns at most 1,000 rows per request unless paged, so a retry
queue deeper than that was silently truncated to its first 1,000 rows.
"""

from __future__ import annotations

from farmuncle_pipeline.ingestion.retry_failed_pages import _fetch_pending_failed_pages

from tests.fakes import FakeResponse


class _Q:
    def __init__(self, rows):
        self.rows = rows
        self.range_args = []
        self.orders = []

    def table(self, name):
        assert name == "failed_pages"
        return self

    def select(self, *_):
        return self

    def eq(self, *_):
        return self

    def order(self, col, **_):
        self.orders.append(col)
        return self

    def range(self, lo, hi):
        self.range_args.append((lo, hi))
        self._slice = self.rows[lo : hi + 1]
        return self

    def execute(self):
        return FakeResponse(data=self._slice)


def test_every_pending_row_is_returned_across_pages():
    rows = [{"id": i} for i in range(2500)]
    q = _Q(rows)
    got = _fetch_pending_failed_pages(q)
    assert [r["id"] for r in got] == list(range(2500))
    assert len(q.range_args) == 3


def test_ordering_includes_the_id_so_pages_do_not_overlap():
    q = _Q([{"id": 1}])
    _fetch_pending_failed_pages(q)
    assert "id" in q.orders


def test_an_empty_queue():
    assert _fetch_pending_failed_pages(_Q([])) == []
