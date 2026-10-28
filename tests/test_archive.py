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
        self.rows, self._a, self._b = rows, 0, 999

    def table(self, name):
        assert name == "mandi_daily_prices"
        return self

    def select(self, *a, **k):
        return self

    def eq(self, *a, **k):
        return self

    def order(self, *a, **k):
        return self

    def range(self, a, b):
        self._a, self._b = a, b
        return self

    def execute(self):
        return FakeResponse(data=self.rows[self._a : self._b + 1])


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


def test_window_order_is_ingest_then_reference_then_archive_then_delete():
    log, state = [], _State()
    run_backward(today=date(2026, 10, 29), oldest=date(2026, 8, 1), state=state, max_windows=1, **_deps(log))
    kinds = [e[0] for e in log]
    assert kinds.index("reference") > max(i for i, k in enumerate(kinds) if k == "ingest")
    assert kinds.index("delete") > max(i for i, k in enumerate(kinds) if k == "archive")
    assert log[-1] == ("delete", date(2026, 10, 9), date(2026, 10, 18))


def test_a_failed_day_stops_the_window_without_archiving_or_deleting_or_advancing():
    log, state = [], _State(next_end=date(2026, 10, 18))
    with pytest.raises(RuntimeError):
        run_backward(today=date(2026, 10, 29), oldest=date(2026, 8, 1), state=state,
                     **_deps(log, fail_ingest_on=date(2026, 10, 12)))
    assert not any(e[0] in ("archive", "delete") for e in log)
    assert state.next_end == date(2026, 10, 18)


def test_an_archive_error_never_deletes_and_never_advances():
    log, state = [], _State(next_end=date(2026, 10, 18))
    with pytest.raises(ArchiveError):
        run_backward(today=date(2026, 10, 29), oldest=date(2026, 8, 1), state=state, **_deps(log, fail_archive=True))
    assert not any(e[0] == "delete" for e in log)
    assert state.next_end == date(2026, 10, 18)


def test_stops_at_the_oldest_date_and_marks_done():
    log, state = [], _State(next_end=date(2026, 8, 5))
    run_backward(today=date(2026, 10, 29), oldest=date(2026, 8, 1), state=state, **_deps(log))
    deleted = [e for e in log if e[0] == "delete"]
    assert deleted == [("delete", date(2026, 8, 1), date(2026, 8, 5))]  # last window is clipped to the oldest date
    assert state.done


def test_a_finished_history_does_nothing():
    log, state = [], _State()
    state.done = True
    run_backward(today=date(2026, 10, 29), oldest=date(2026, 8, 1), state=state, **_deps(log))
    assert log == []
