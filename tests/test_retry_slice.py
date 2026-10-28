"""
tests/test_retry_slice.py

When a page fails mid-state, the fetch loop stops that state, so every later
page of the state was never fetched either. The retry therefore has to re-fetch
from the failed page to the end of the state, not just the one page, and may
only call a coverage-gap row resolved once the government total is reached
(audit fault 3).
"""

from __future__ import annotations

from farmuncle_pipeline.core.resource_client import PageFetchResult
from farmuncle_pipeline.ingestion.retry_failed_pages import _is_resolved, _refetch_slice


def _ok(n, total=None):
    return PageFetchResult(
        ok=True, records=[{}] * n, raw_response={"total": total} if total is not None else {},
        error=None, duration_ms=1,
    )


def _bad():
    return PageFetchResult(ok=False, records=[], raw_response={}, error="boom", duration_ms=1)


def _run(pages, first_page=1, page_size=3):
    """pages: list of results returned for page first_page, first_page+1, ..."""
    seen_pages, handled = [], []

    def fetch(*, offset, page):
        seen_pages.append((page, offset))
        return pages[page - first_page]

    outcome = _refetch_slice(
        first_page=first_page, page_size=page_size, fetch=fetch,
        handle_page=lambda result, page: handled.append(page),
    )
    return outcome, seen_pages, handled


def test_single_short_page_finishes():
    outcome, seen, handled = _run([_ok(2)])
    assert outcome.ok and outcome.failed_page is None
    assert outcome.collected == 2
    assert seen == [(1, 0)]
    assert handled == [1]


def test_pages_are_followed_until_a_short_one():
    outcome, seen, handled = _run([_ok(3, total=7), _ok(3), _ok(1)])
    assert outcome.ok
    assert outcome.collected == 7
    assert outcome.reported_total == 7
    assert seen == [(1, 0), (2, 3), (3, 6)]
    assert handled == [1, 2, 3]


def test_starts_at_the_failed_page_not_the_first():
    outcome, seen, handled = _run([_ok(3), _ok(2)], first_page=4)
    assert seen == [(4, 9), (5, 12)]
    assert handled == [4, 5]
    assert outcome.collected == 5


def test_an_empty_page_ends_the_state():
    outcome, seen, _ = _run([_ok(0)])
    assert outcome.ok and outcome.collected == 0


def test_a_later_failing_page_is_reported_with_its_number():
    outcome, seen, handled = _run([_ok(3), _bad()])
    assert outcome.ok is False
    assert outcome.failed_page == 2
    assert handled == [1]          # the page that worked is kept
    assert outcome.collected == 3


def test_first_page_failing():
    outcome, _, handled = _run([_bad()], first_page=3)
    assert outcome.ok is False and outcome.failed_page == 3
    assert handled == []


def test_runaway_pagination_is_capped():
    full = [_ok(3)] * 500

    def fetch(*, offset, page):
        return full[0]

    outcome = _refetch_slice(first_page=1, page_size=3, fetch=fetch, handle_page=lambda r, p: None,
                             max_pages=5)
    assert outcome.ok is False
    assert outcome.failed_page == 6


# ---- when may a row be called resolved? ------------------------------------

def test_a_page_failure_row_resolves_when_the_slice_succeeds():
    outcome, _, _ = _run([_ok(3), _ok(1)])
    assert _is_resolved("INGEST-002", outcome) is True


def test_a_failed_slice_never_resolves():
    outcome, _, _ = _run([_bad()])
    assert _is_resolved("INGEST-002", outcome) is False
    assert _is_resolved("QUALITY-001", outcome) is False


def test_a_coverage_row_needs_the_government_total():
    short, _, _ = _run([_ok(3, total=10), _ok(3)] + [_ok(1)])           # 7 of 10
    assert _is_resolved("QUALITY-001", short) is False
    full, _, _ = _run([_ok(3, total=7), _ok(3), _ok(1)])                # 7 of 7
    assert _is_resolved("QUALITY-001", full) is True


def test_a_coverage_row_without_a_reported_total_resolves():
    outcome, _, _ = _run([_ok(2)])
    assert _is_resolved("QUALITY-001", outcome) is True
