"""History archiver: Parquet export to R2, verified upload, and the backward 10-day loop."""

from __future__ import annotations

import io
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from farmuncle_pipeline.archive.export import ArchiveError, archive_day, day_key, manifest_key, rows_to_parquet
from farmuncle_pipeline.archive.history import next_window, run_backward
from tests.fakes import FakeResponse


def _row(i, day="2026-10-01", **kw):
    return {"mandi_id": i, "crop_id": 1, "variety_id": 1, "grade_id": 1, "price_date": day, "modal_price": 25.5,
            "min_price": 20.0, "max_price": 30.0, "unit": "kg", "source": "resource_2",
            "source_mandi_id": i, "quality_score": 0.9, "quality_components": {"a": 1}, **kw}


# --- parquet -----------------------------------------------------------------

def test_parquet_round_trips_rows():
    data = rows_to_parquet([_row(1), _row(2)])
    table = pq.read_table(io.BytesIO(data))
    assert table.num_rows == 2
    assert table.column("mandi_id").to_pylist() == [1, 2]
    assert table.column("modal_price").to_pylist() == [25.5, 25.5]


def test_keys_are_partitioned_by_day():
    assert day_key(date(2026, 10, 1)) == "facts/daily/date=2026-10-01.parquet"
    assert manifest_key(date(2026, 10, 1)) == "facts/manifest/date=2026-10-01.json"


# --- archive_day -------------------------------------------------------------

class _Db:
    """Pages of rows for one day, honouring .range()."""

    def __init__(self, rows):
        self.rows = [{"id": i, **r} for i, r in enumerate(rows, 1)]
        self._after, self._page = 0, 1000

    quarantine: list = []

    def table(self, name):
        self.name = name
        return self

    def select(self, *a, **k):
        return self

    def eq(self, *a, **k):
        return self

    def order(self, *a, **k):
        return self

    def gt(self, _col, value):
        self._after = value
        return self

    def limit(self, n):
        self._page = n
        return self

    def execute(self):
        rows = self.rows if self.name == "mandi_daily_prices" else self.quarantine
        return FakeResponse(data=[r for r in rows if r["id"] > self._after][: self._page])


class _Store:
    def __init__(self, corrupt=False):
        self.objects, self.corrupt = {}, corrupt

    def put(self, key, data, content_type="application/octet-stream"):
        self.objects[key] = data

    def size(self, key):
        return len(self.objects[key]) + (1 if self.corrupt else 0)


def test_archive_day_uploads_parquet_and_manifest_and_verifies_size():
    rows = [_row(i) for i in range(2500)]  # more than one 1000-row page
    store = _Store()
    result = archive_day(_Db(rows), store, date(2026, 10, 1))
    assert result.rows == 2500
    assert pq.read_table(io.BytesIO(store.objects[day_key(date(2026, 10, 1))])).num_rows == 2500
    assert b'"rows": 2500' in store.objects[manifest_key(date(2026, 10, 1))]


def test_archive_day_refuses_when_the_stored_size_does_not_match():
    with pytest.raises(ArchiveError):
        archive_day(_Db([_row(1)]), _Store(corrupt=True), date(2026, 10, 1))


def test_an_empty_day_uploads_nothing():
    store = _Store()
    assert archive_day(_Db([]), store, date(2026, 10, 1)).rows == 0
    assert store.objects == {}


def test_quarantined_rows_are_archived_with_the_day_even_when_no_price_row_survived():
    db = _Db([])
    db.quarantine = [{"id": 1, "batch_id": "b", "resource": "r2", "row_data": {"price_date": "2026-10-01"},
                      "created_at": None, "error_message": "x", "error_code": "DQ-PRICE-FLOOR",
                      "review_status": None, "review_note": None}]
    store = _Store()
    result = archive_day(db, store, date(2026, 10, 1))
    assert result.quarantined == 1
    assert pq.read_table(io.BytesIO(store.objects["quarantine/date=2026-10-01.parquet"])).num_rows == 1


def test_a_window_that_would_overrun_the_budget_is_not_started():
    now = [0.0]
    log = []
    deps = _deps(log)
    plain_ingest = deps["ingest_day"]
    deps["ingest_day"] = lambda d: (now.__setitem__(0, now[0] + 120), plain_ingest(d))[1]  # each window takes 120s
    n = run_backward(today=date(2026, 10, 29), oldest=date(2018, 1, 1), state=_State(date(2026, 10, 18)),
                     window_days=1, budget_seconds=300, clock=lambda: now[0], **deps)
    assert n == 2  # a third would end at 360s, past the 300s budget


# --- windows -----------------------------------------------------------------

def test_next_window_is_ten_days_ending_at_the_given_day():
    w = next_window(date(2026, 10, 18), days=10)
    assert (w.start, w.end) == (date(2026, 10, 9), date(2026, 10, 18))


class _State:
    def __init__(self, next_end=None):
        self.next_end, self.done = next_end, False

    def get(self):
        return self.next_end

    def set(self, next_end, done=False):
        self.next_end, self.done = next_end, done


def _deps(log, *, fail_ingest_on=None, fail_archive=False):
    def ingest_day(d):
        log.append(("ingest", d))
        return SimpleNamespace(final_status="FAILED" if d == fail_ingest_on else "SUCCESS")

    def arch(d):
        if fail_archive:
            raise ArchiveError("boom")
        log.append(("archive", d))
        return SimpleNamespace(rows=10)

    return dict(
        ingest_day=ingest_day, archive_day=arch,
        delete_window=lambda s, e: log.append(("delete", s, e)),
        refresh_reference=lambda: log.append(("reference",)),
    )


def test_first_window_sits_just_before_the_live_window_and_steps_backwards():
    log, state = [], _State()
    run_backward(today=date(2026, 10, 29), oldest=date(2026, 8, 1), state=state, max_windows=1, **_deps(log))
    days = [e[1] for e in log if e[0] == "ingest"]
    assert days[0] == date(2026, 10, 18) and days[-1] == date(2026, 10, 9)   # newest first, 10 days
    assert state.next_end == date(2026, 10, 8)                               # next window ends the day before


def test_window_order_is_ingest_then_reference_then_archive_and_delete_each_day():
    log, state = [], _State()
    run_backward(today=date(2026, 10, 29), oldest=date(2026, 8, 1), state=state, max_windows=1, **_deps(log))
    kinds = [e[0] for e in log]
    assert kinds.index("reference") > max(i for i, k in enumerate(kinds) if k == "ingest")
    assert kinds[kinds.index("reference") + 1] == "archive"
    assert ("delete", date(2026, 10, 18), date(2026, 10, 18)) in log  # each day is deleted right after its upload
    assert log.index(("archive", date(2026, 10, 18))) < log.index(("delete", date(2026, 10, 18), date(2026, 10, 18)))


def test_a_failed_day_is_skipped_and_the_rest_of_the_window_is_archived_and_deleted():
    log, state = [], _State(next_end=date(2026, 10, 18))
    run_backward(today=date(2026, 10, 29), oldest=date(2026, 8, 1), state=state,
                 max_windows=1, **_deps(log, fail_ingest_on=date(2026, 10, 12)))
    assert ("archive", date(2026, 10, 12)) not in log and ("delete", date(2026, 10, 12), date(2026, 10, 12)) not in log
    assert ("archive", date(2026, 10, 18)) in log and ("delete", date(2026, 10, 18), date(2026, 10, 18)) in log
    assert state.next_end == date(2026, 10, 8)  # the window still advances


def test_an_archive_error_keeps_that_day_and_moves_on_without_deleting_it():
    log, state = [], _State(next_end=date(2026, 10, 18))
    run_backward(today=date(2026, 10, 29), oldest=date(2026, 8, 1), state=state,
                 max_windows=1, **_deps(log, fail_archive=True))
    assert not any(e[0] == "delete" for e in log)
    assert state.next_end == date(2026, 10, 8)


def test_stops_at_the_oldest_date_and_marks_done():
    log, state = [], _State(next_end=date(2026, 8, 5))
    run_backward(today=date(2026, 10, 29), oldest=date(2026, 8, 1), state=state, **_deps(log))
    deleted = [e for e in log if e[0] == "delete"]
    assert deleted == [("delete", date(2026, 8, d), date(2026, 8, d)) for d in range(5, 0, -1)]  # last window is clipped to the oldest date, one day at a time
    assert state.done


def test_a_finished_history_does_nothing():
    log, state = [], _State()
    state.done = True
    run_backward(today=date(2026, 10, 29), oldest=date(2026, 8, 1), state=state, **_deps(log))
    assert log == []


def test_range_archive_takes_only_final_days_inside_the_range():
    from farmuncle_pipeline.ingestion.archive_range import final_days

    rows = [
        {"price_date": "2026-10-28", "status": "PENDING"},
        {"price_date": "2026-10-27", "status": "FINAL"},
        {"price_date": "2026-10-19", "status": "FINAL"},
        {"price_date": "2026-10-18", "status": "FINAL"},
    ]
    assert final_days(rows, date(2026, 10, 19), date(2026, 10, 28)) == [date(2026, 10, 19), date(2026, 10, 27)]
