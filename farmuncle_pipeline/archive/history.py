"""Fill history backwards: today -> 2018 in 10-day windows.

Each window goes one day at a time: ingest the day (Resource 2) into the warehouse, export it to R2, verify it and
delete that day, then the next. The reference stats are snapshotted once per window, after its first day is in.
A day that fails is skipped and listed,
and the rest of the window goes on; the window then advances. A skipped day stays in the warehouse
(nothing is deleted without a verified upload).
The latest `live_days` stay in the warehouse for the app; they are archived when they age out.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, timedelta

DEFAULT_OLDEST = date(2018, 1, 1)
MAX_FAILURES_IN_A_ROW = 3  # then the run stops for now; the days are in archive_days as FAILED and retried next run
LIVE_DAYS = 3  # days kept in the warehouse for the app; keep equal to delete_price_window's guard


@dataclass(frozen=True)
class Window:
    start: date
    end: date

    def days(self):
        """Newest first."""
        d = self.end
        while d >= self.start:
            yield d
            d -= timedelta(days=1)


def next_window(end: date, days: int = 10, oldest: date | None = None) -> Window:
    start = end - timedelta(days=days - 1)
    if oldest and start < oldest:
        start = oldest
    return Window(start, end)


def _pull(ingest_day, day):
    """Pull a day; pull it once more if this pass met mandis/crops/varieties/grades the preload did not know.

    22 and 23 Sep stored 2% fewer rows on the first pull (21,760 vs 22,230) than on the retry, with the same
    24,042 records from the API. A pass that resolved names outside its preload may have merged rows, so the
    second pass (which preloads them) decides what is archived.
    """
    result = ingest_day(day)
    stats = getattr(result, "identity_stats", None) or {}
    if any(v for k, v in stats.items() if k.endswith("_rpc_calls")):
        result = ingest_day(day)
    return result


def _settle(days, *, ingest_day, archive_day, delete_window, refresh_reference, record=None,
            max_failures: int = MAX_FAILURES_IN_A_ROW) -> tuple[list[tuple[date, str]], bool]:
    """Pull, verify and delete one day at a time. A day that fails is skipped and returned; the rest go on.

    A day that fails to ingest or upload is cleared from the warehouse too and recorded as FAILED with the reason:
    this loop pulled it from Resource 2 itself, so nothing is lost, and failed days would otherwise pile up
    (Jan 2026: ~350,000 rows). A retry pulls it again.

    One day at a time keeps the warehouse at most one extra day (~17,000 rows) deep, so a failing step leaves
    one day behind, not a whole window. After `max_failures` days in a row fail, the days not yet tried are
    recorded as FAILED (retried next run) and `stopped` is True: a database that times out on every day made
    runs last 3 to 5 hours for nothing.
    """
    record = record or (lambda *a, **k: None)  # bookkeeping only: it must never decide anything
    days = list(days)
    failed: list[tuple[date, str]] = []
    refreshed = False
    streak = 0

    def fail(day, reason, status="FAILED", **kw):
        nonlocal streak
        streak += 1
        failed.append((day, reason))
        record(day, status, **kw)

    def drop(day) -> str:
        """Clear what a failed day left in the warehouse (it is pulled again when retried), so it cannot pile up."""
        try:
            delete_window(day, day)
            return "; warehouse rows removed, pulled again on retry"
        except Exception as exc:
            return f"; warehouse rows kept, cleanup delete failed: {exc}"

    for i, day in enumerate(days):
        if streak >= max_failures:
            reason = f"not tried: {max_failures} days in a row failed"
            for rest in days[i:]:
                failed.append((rest, reason))
                record(rest, "FAILED", error=reason)
            print(f"[archive_history] stopping: {reason}")
            return failed, True
        try:
            status = _pull(ingest_day, day).final_status
            if str(getattr(status, "value", status)) == "FAILED":
                raise RuntimeError("ingest returned FAILED")
        except Exception as exc:
            reason = f"ingest: {exc}{drop(day)}"
            fail(day, reason, error=reason)
            continue
        if not refreshed:  # the reference numbers need one day in; they can wait, the days must not stop
            refreshed = True
            try:
                refresh_reference()
            except Exception as exc:
                print(f"[archive_history] reference refresh failed, continuing: {exc}")
        try:
            archived = archive_day(day)   # raises ArchiveError unless the upload is verified
        except Exception as exc:
            reason = f"upload: {exc}{drop(day)}"
            fail(day, reason, error=reason)
            continue
        try:
            delete_window(day, day)   # straight after its verified upload
            record(day, "DELETED", result=archived)
            streak = 0
        except Exception as exc:      # the upload is already verified; the day is retried next run
            fail(day, f"uploaded, delete failed: {exc}", "ARCHIVED", result=archived, error=f"delete failed: {exc}")
    return failed, False


def run_backward(
    *, today: date, oldest: date, state, ingest_day, archive_day, delete_window, refresh_reference,
    after_window=None, max_windows: int | None = None, window_days: int = 10, live_days: int = LIVE_DAYS,
    budget_seconds: float | None = None, clock=time.monotonic, leftover_days=None, record=None,
) -> int:
    """Process windows until `oldest`, the window cap or the time budget. Returns windows completed.

    `leftover_days(after, up_to)` lists days still in the warehouse between the processed range and the
    live window (days skipped earlier). They are retried first, every run, so a skipped day is not lost.
    """
    began = clock()
    done_count = 0
    settle = dict(ingest_day=ingest_day, archive_day=archive_day, delete_window=delete_window,
                  refresh_reference=refresh_reference, record=record)
    frontier = state.get()
    if leftover_days and frontier:
        retry = sorted(leftover_days(frontier + timedelta(days=1), today - timedelta(days=live_days + 1)), reverse=True)
        for day, reason in _settle(retry, **settle)[0]:  # a stopped retry just moves on to the windows
            print(f"[archive_history] retry: {day} still not archived: {reason}")
    longest = 0.0  # a window that would overrun the budget is not started (the job timeout would kill it half-done)
    while not getattr(state, "done", False):
        if max_windows is not None and done_count >= max_windows:
            break
        if budget_seconds is not None and clock() - began + longest >= budget_seconds:
            break
        window_began = clock()
        end = state.get() or (today - timedelta(days=live_days + 1))
        if end < oldest:
            state.set(end, done=True)
            break
        window = next_window(end, window_days, oldest)

        # A day that fails never stops the window: the others are archived and deleted. A failed day
        # stays in the warehouse (nothing is deleted without a verified upload) and is listed for retry.
        failed, stopped = _settle(window.days(), **settle)
        if after_window:
            after_window(window)
        for day, reason in failed:
            print(f"[archive_history] {day} not archived this run: {reason}")

        previous_day = window.start - timedelta(days=1)
        state.set(previous_day, done=previous_day < oldest)
        if stopped:  # not counted as completed: the workflow only chains the next run after a completed window
            break
        done_count += 1
        longest = max(longest, clock() - window_began)
    return done_count
