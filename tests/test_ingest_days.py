"""
tests/test_ingest_days.py

daily_rewrite used to re-check a fixed window (today and the 2 days before).
Anything older than that was never revisited after an outage, and v2 can
publish later than the window. Instead every day is tracked in `ingest_days`
and retried each night until v2 has it (Phase 2.1).
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from farmuncle_pipeline.config import IngestionBatchStatus
from farmuncle_pipeline.core import ingest_days
from farmuncle_pipeline.core.ingest_days import (
    decide,
    pending_days,
    record_day_result,
    recheck_recent_days,
    seed_pending_days,
)
from farmuncle_pipeline.ingestion import daily_rewrite

from tests.fakes import FakeResponse


# ---- in-memory stand-in for the three tables these functions touch ---------

class _Q:
    def __init__(self, db, name):
        self.db, self.name = db, name
        self.f = []
        self._order = None
        self._limit = None
        self._range = None
        self._write = None

    def select(self, *_):
        return self

    def eq(self, c, v):
        self.f.append(lambda r, c=c, v=v: r.get(c) == v)
        return self

    def lt(self, c, v):
        self.f.append(lambda r, c=c, v=v: r.get(c) is not None and r[c] < v)
        return self

    def gte(self, c, v):
        self.f.append(lambda r, c=c, v=v: r.get(c) is not None and r[c] >= v)
        return self

    def order(self, c, desc=False):
        self._order = (c, desc)
        return self

    def limit(self, n):
        self._limit = n
        return self

    def range(self, lo, hi):
        self._range = (lo, hi)
        return self

    def upsert(self, payload, on_conflict=None, ignore_duplicates=False):
        self._write = ("upsert", payload, ignore_duplicates)
        return self

    def update(self, payload):
        self._write = ("update", payload, False)
        return self

    def execute(self):
        rows = self.db.t[self.name]
        if self._write:
            kind, payload, ignore = self._write
            if kind == "upsert":
                for p in payload if isinstance(payload, list) else [payload]:
                    hit = next((r for r in rows if r["price_date"] == p["price_date"]), None)
                    if hit is None:
                        rows.append(dict(p))
                    elif not ignore:
                        hit.update(p)
                return FakeResponse(data=[])
            for r in [r for r in rows if all(f(r) for f in self.f)]:
                r.update(payload)
            return FakeResponse(data=[])
        out = [r for r in rows if all(f(r) for f in self.f)]
        if self._order:
            c, desc = self._order
            out = sorted(out, key=lambda r: r[c], reverse=desc)
        if self._range:
            lo, hi = self._range
            out = out[lo : hi + 1]
        if self._limit is not None:
            out = out[: self._limit]
        return FakeResponse(data=[dict(r) for r in out])


class _Db:
    def __init__(self, days=(), prices=()):
        self.t = {
            "ingest_days": [dict(d) for d in days],
            "mandi_daily_prices": [dict(p) for p in prices],
        }

    def table(self, name):
        return _Q(self, name)

    def days(self):
        return {r["price_date"]: r for r in self.t["ingest_days"]}


D = date(2026, 10, 28)  # "today" in these tests


def iso(d):
    return d.isoformat()


# ---- decide ---------------------------------------------------------------

def test_a_complete_day_is_final():
    d = decide(final_status="SUCCESS", total_records=18000, price_date=D - timedelta(days=1), today=D)
    assert d.final is True


def test_a_partial_day_stays_pending():
    d = decide(final_status="PARTIAL", total_records=17000, price_date=D - timedelta(days=2), today=D)
    assert d.final is False


def test_a_failed_run_never_finalizes_even_for_an_old_day():
    d = decide(final_status="FAILED", total_records=0, price_date=D - timedelta(days=90), today=D)
    assert d.final is False


def test_a_day_with_no_v2_data_waits_then_gives_up_after_a_week():
    young = decide(final_status="SUCCESS", total_records=0, price_date=D - timedelta(days=3), today=D)
    old = decide(final_status="SUCCESS", total_records=0, price_date=D - timedelta(days=7), today=D)
    assert young.final is False
    assert old.final is True and old.note


def test_a_stubbornly_partial_day_gives_up_after_two_weeks():
    young = decide(final_status="PARTIAL", total_records=500, price_date=D - timedelta(days=13), today=D)
    old = decide(final_status="PARTIAL", total_records=500, price_date=D - timedelta(days=14), today=D)
    assert young.final is False
    assert old.final is True and "partial" in old.note


# ---- seeding --------------------------------------------------------------

def test_first_run_starts_from_the_newest_price_in_the_warehouse():
    db = _Db(prices=[{"price_date": "2026-08-03"}, {"price_date": "2026-08-04"}])
    added = seed_pending_days(db, today=D)
    assert added[0] == date(2026, 8, 4)
    assert added[-1] == D - timedelta(days=1)
    assert len(added) == (D - date(2026, 8, 4)).days
    assert all(r["status"] == "PENDING" for r in db.t["ingest_days"])


def test_later_runs_start_after_the_newest_final_day_and_keep_existing_rows():
    db = _Db(days=[
        {"price_date": "2026-10-24", "status": "FINAL", "attempts": 1},
        {"price_date": "2026-10-26", "status": "PENDING", "attempts": 3},
    ])
    added = seed_pending_days(db, today=D)
    assert added == [date(2026, 10, 25), date(2026, 10, 26), date(2026, 10, 27)]
    assert db.days()["2026-10-26"]["attempts"] == 3          # not reset
    assert db.days()["2026-10-24"]["status"] == "FINAL"


def test_nothing_is_seeded_when_already_up_to_date():
    db = _Db(days=[{"price_date": "2026-10-27", "status": "FINAL", "attempts": 1}])
    assert seed_pending_days(db, today=D) == []


def test_with_no_history_at_all_it_starts_two_weeks_back():
    db = _Db()
    added = seed_pending_days(db, today=D)
    assert added[0] == D - timedelta(days=14)
    assert added[-1] == D - timedelta(days=1)


# ---- pending list ----------------------------------------------------------

def test_pending_days_are_oldest_first_and_exclude_today_and_final():
    db = _Db(days=[
        {"price_date": "2026-10-27", "status": "PENDING"},
        {"price_date": "2026-10-20", "status": "PENDING"},
        {"price_date": "2026-10-22", "status": "FINAL"},
        {"price_date": "2026-10-28", "status": "PENDING"},
    ])
    assert pending_days(db, before=D) == [date(2026, 10, 20), date(2026, 10, 27)]


def test_pending_days_pages_through_a_long_backlog():
    start = date(2026, 1, 1)
    rows = [{"price_date": iso(start + timedelta(days=i)), "status": "PENDING"} for i in range(2300)]
    got = pending_days(_Db(days=rows), before=date(2040, 1, 1))
    assert len(got) == 2300 and got == sorted(got)


# ---- recording ---------------------------------------------------------------

def test_recording_a_pending_result_counts_the_attempt():
    db = _Db(days=[{"price_date": "2026-10-27", "status": "PENDING", "attempts": 2}])
    record_day_result(db, price_date=date(2026, 10, 27), final=False, v2_rows=900, note="partial")
    row = db.days()["2026-10-27"]
    assert row["status"] == "PENDING" and row["attempts"] == 3 and row["v2_rows"] == 900
    assert row["last_error"] == "partial"


def test_recording_a_final_result_stamps_final_at():
    db = _Db(days=[{"price_date": "2026-10-27", "status": "PENDING", "attempts": 0}])
    record_day_result(db, price_date=date(2026, 10, 27), final=True, v2_rows=18000, note=None)
    row = db.days()["2026-10-27"]
    assert row["status"] == "FINAL" and row["final_at"] and row["last_error"] is None


def test_recording_creates_the_row_for_a_manually_backfilled_day():
    db = _Db()
    record_day_result(db, price_date=date(2026, 8, 9), final=True, v2_rows=15000, note=None)
    assert db.days()["2026-08-09"]["status"] == "FINAL"
    assert db.days()["2026-08-09"]["attempts"] == 1


def test_the_weekly_recheck_reopens_only_recent_final_days():
    db = _Db(days=[
        {"price_date": "2026-10-10", "status": "FINAL"},   # 18 days back: left alone
        {"price_date": "2026-10-20", "status": "FINAL"},
        {"price_date": "2026-10-27", "status": "FINAL"},
    ])
    recheck_recent_days(db, today=D, days=14)
    s = {k: v["status"] for k, v in db.days().items()}
    assert s == {"2026-10-10": "FINAL", "2026-10-20": "PENDING", "2026-10-27": "PENDING"}


# ---- the nightly job ------------------------------------------------------------

class _Result:
    def __init__(self, status, records=18000, batch="b"):
        self.final_status = status
        self.total_records = records
        self.batch_id = batch
        self.rows_processed = records
        self.rows_failed = 0
        self.precedence_skipped = 0
        self.states_with_any_success = 30
        self.pages_fetched = 40


@pytest.fixture
def job(monkeypatch):
    calls = {"dates": [], "alerts": [], "cache": 0}
    results = {}

    def fake_ingest(ctx, *, target_date, job_name):
        calls["dates"].append(target_date)
        r = results.get(target_date, _Result(IngestionBatchStatus.SUCCESS))
        if isinstance(r, Exception):
            raise r
        return r

    monkeypatch.setattr(daily_rewrite, "ingest_resource2_for_date", fake_ingest)
    monkeypatch.setattr(daily_rewrite, "refresh_price_cache", lambda *a, **k: calls.__setitem__("cache", calls["cache"] + 1))
    monkeypatch.setattr(daily_rewrite, "_raise_outage_alert_if_needed", lambda sb, batch_id: calls["alerts"].append(batch_id))

    class Ctx:
        def __init__(self, db):
            self.supabase = db

    return calls, results, Ctx


def test_nightly_run_works_through_the_backlog_oldest_first(job):
    calls, results, Ctx = job
    db = _Db(days=[
        {"price_date": "2026-10-25", "status": "PENDING", "attempts": 0},
        {"price_date": "2026-10-24", "status": "FINAL", "attempts": 1},
    ])
    daily_rewrite.run_daily_rewrite(Ctx(db), today=D)
    assert calls["dates"] == [date(2026, 10, 25), date(2026, 10, 26), date(2026, 10, 27)]
    assert {v["status"] for v in db.days().values()} == {"FINAL"}
    assert calls["cache"] == 1


def test_a_day_v2_has_not_published_is_retried_the_next_night(job):
    calls, results, Ctx = job
    db = _Db(days=[{"price_date": "2026-10-26", "status": "FINAL", "attempts": 1}])
    results[date(2026, 10, 27)] = _Result(IngestionBatchStatus.SUCCESS, records=0)
    daily_rewrite.run_daily_rewrite(Ctx(db), today=D)
    assert db.days()["2026-10-27"]["status"] == "PENDING"
    calls["dates"].clear()
    results[date(2026, 10, 27)] = _Result(IngestionBatchStatus.SUCCESS, records=17000)
    daily_rewrite.run_daily_rewrite(Ctx(db), today=D + timedelta(days=1))
    assert date(2026, 10, 27) in calls["dates"]
    assert db.days()["2026-10-27"]["status"] == "FINAL"
    assert db.days()["2026-10-27"]["attempts"] == 2


def test_one_failing_day_does_not_stop_the_others(job):
    calls, results, Ctx = job
    db = _Db(days=[{"price_date": "2026-10-25", "status": "FINAL", "attempts": 1}])
    results[date(2026, 10, 26)] = RuntimeError("boom")
    daily_rewrite.run_daily_rewrite(Ctx(db), today=D)
    assert calls["dates"] == [date(2026, 10, 26), date(2026, 10, 27)]
    assert db.days()["2026-10-26"]["status"] == "PENDING"
    assert "boom" in db.days()["2026-10-26"]["last_error"]
    assert db.days()["2026-10-27"]["status"] == "FINAL"


def test_a_concurrent_run_ends_the_job_cleanly(job):
    calls, results, Ctx = job
    from farmuncle_pipeline.core.batch_lifecycle import JobAlreadyRunningError
    db = _Db(days=[{"price_date": "2026-10-25", "status": "FINAL", "attempts": 1}])
    results[date(2026, 10, 26)] = JobAlreadyRunningError("running")
    daily_rewrite.run_daily_rewrite(Ctx(db), today=D)
    assert calls["dates"] == [date(2026, 10, 26)]     # stops, does not touch the next day
    assert calls["cache"] == 0


def test_the_time_budget_stops_new_days_from_starting(job):
    calls, results, Ctx = job
    db = _Db(days=[{"price_date": "2026-10-10", "status": "FINAL", "attempts": 1}])
    ticks = iter([0, 10, 5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000, 5000])
    daily_rewrite.run_daily_rewrite(Ctx(db), today=D, clock=lambda: next(ticks), time_budget_seconds=1000)
    assert len(calls["dates"]) == 1
    left = [k for k, v in db.days().items() if v["status"] == "PENDING"]
    assert left                                       # the rest wait for tomorrow


def test_the_day_cap_limits_one_run(job):
    calls, results, Ctx = job
    db = _Db(days=[{"price_date": "2026-09-01", "status": "FINAL", "attempts": 1}])
    daily_rewrite.run_daily_rewrite(Ctx(db), today=D, max_days=4)
    assert len(calls["dates"]) == 4
    assert calls["dates"] == sorted(calls["dates"])


def test_sunday_reopens_the_last_two_weeks(job):
    calls, results, Ctx = job
    sunday = date(2026, 11, 1)
    assert sunday.weekday() == 6
    db = _Db(days=[{"price_date": "2026-10-31", "status": "FINAL", "attempts": 1}])
    daily_rewrite.run_daily_rewrite(Ctx(db), today=sunday)
    assert date(2026, 10, 31) in calls["dates"]


def test_a_failed_last_day_checks_the_outage_alert(job):
    calls, results, Ctx = job
    db = _Db(days=[{"price_date": "2026-10-26", "status": "FINAL", "attempts": 1}])
    results[date(2026, 10, 27)] = _Result(IngestionBatchStatus.FAILED, records=0, batch="bad")
    daily_rewrite.run_daily_rewrite(Ctx(db), today=D)
    assert calls["alerts"] == ["bad"]
    assert db.days()["2026-10-27"]["status"] == "PENDING"
