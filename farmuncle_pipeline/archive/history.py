"""Fill history backwards: today -> 2018 in 10-day windows.

Each window: ingest its days (Resource 2) into the warehouse, snapshot reference stats, export every
day to R2 and verify it, delete the window from the warehouse, then move on. State advances only after
the delete, and a failure anywhere leaves the window to be redone (every step is idempotent).
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


def run_backward(
    *, today: date, oldest: date, state, ingest_day, archive_day, delete_window, refresh_reference,
    after_window=None, max_windows: int | None = None, window_days: int = 10, live_days: int = 10,
    budget_seconds: float | None = None, clock=time.monotonic,
) -> int:
    """Process windows until `oldest`, the window cap or the time budget. Returns windows completed."""
    began = clock()
    done_count = 0
    while not getattr(state, "done", False):
        if max_windows is not None and done_count >= max_windows:
            break
        if budget_seconds is not None and clock() - began >= budget_seconds:
            break
        end = state.get() or (today - timedelta(days=live_days + 1))
        if end < oldest:
            state.set(end, done=True)
            break
        window = next_window(end, window_days, oldest)

        for day in window.days():
            status = ingest_day(day).final_status
            if str(getattr(status, "value", status)) == "FAILED":
                raise RuntimeError(f"ingest failed for {day}; window {window.start}..{window.end} left for the next run")
        refresh_reference()
        for day in window.days():
            archive_day(day)          # raises ArchiveError unless the upload is verified
        delete_window(window.start, window.end)
        if after_window:
            after_window(window)

        previous_day = window.start - timedelta(days=1)
        state.set(previous_day, done=previous_day < oldest)
        done_count += 1
    return done_count
