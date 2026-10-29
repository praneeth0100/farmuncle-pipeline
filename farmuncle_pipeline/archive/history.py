"""Fill history backwards: today -> 2018 in 10-day windows.

Each window: ingest its days (Resource 2) into the warehouse, snapshot reference stats, then for each day
export it to R2, verify it and delete that day from the warehouse. A day that fails is skipped and listed,
and the rest of the window goes on; the window then advances. A skipped day stays in the warehouse
(nothing is deleted without a verified upload).
The latest `live_days` stay in the warehouse for the app; they are archived when they age out.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, timedelta

DEFAULT_OLDEST = date(2018, 1, 1)


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


def _settle(days, *, ingest_day, archive_day, delete_window, refresh_reference) -> list[tuple[date, str]]:
    """Pull, verify and delete each day. A day that fails is skipped and returned; the rest go on."""
    ready: list[date] = []
    failed: list[tuple[date, str]] = []
    for day in days:
        try:
            status = ingest_day(day).final_status
            if str(getattr(status, "value", status)) == "FAILED":
                raise RuntimeError("ingest returned FAILED")
            ready.append(day)
        except Exception as exc:
            failed.append((day, str(exc)))
    try:
        refresh_reference()
    except Exception as exc:          # the reference numbers can wait; the days must not stop
        print(f"[archive_history] reference refresh failed, continuing: {exc}")
    for day in ready:
        try:
            archive_day(day)          # raises ArchiveError unless the upload is verified
        except Exception as exc:
            failed.append((day, str(exc)))
            continue
        try:
            delete_window(day, day)   # one day at a time, straight after its verified upload
        except Exception as exc:      # the upload is already verified; the day is retried next run
            failed.append((day, f"uploaded, delete failed: {exc}"))
    return failed


def run_backward(
    *, today: date, oldest: date, state, ingest_day, archive_day, delete_window, refresh_reference,
    after_window=None, max_windows: int | None = None, window_days: int = 10, live_days: int = 10,
    budget_seconds: float | None = None, clock=time.monotonic, leftover_days=None,
) -> int:
    """Process windows until `oldest`, the window cap or the time budget. Returns windows completed.

    `leftover_days(after, up_to)` lists days still in the warehouse between the processed range and the
    live window (days skipped earlier). They are retried first, every run, so a skipped day is not lost.
    """
    began = clock()
    done_count = 0
    settle = dict(ingest_day=ingest_day, archive_day=archive_day, delete_window=delete_window,
                  refresh_reference=refresh_reference)
    frontier = state.get()
    if leftover_days and frontier:
        retry = sorted(leftover_days(frontier + timedelta(days=1), today - timedelta(days=live_days + 1)), reverse=True)
        for day, reason in _settle(retry, **settle):
            print(f"[archive_history] retry: {day} still not archived, left in the warehouse: {reason}")
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
        failed = _settle(window.days(), **settle)
        if after_window:
            after_window(window)
        for day, reason in failed:
            print(f"[archive_history] {day} not archived this run, left in the warehouse: {reason}")

        previous_day = window.start - timedelta(days=1)
        state.set(previous_day, done=previous_day < oldest)
        done_count += 1
        longest = max(longest, clock() - window_began)
    return done_count
