"""Where the backward history fill has got to (one row in archive_state)."""

from __future__ import annotations

from datetime import date

KEY = "history"


class SupabaseArchiveState:
    def __init__(self, supabase) -> None:
        self._db = supabase
        self.done = False
        self._windows = 0
        rows = self._db.table("archive_state").select("done,windows_done").eq("key", KEY).limit(1).execute().data or []
        if rows:
            self.done, self._windows = bool(rows[0]["done"]), int(rows[0]["windows_done"])

    def get(self) -> date | None:
        rows = self._db.table("archive_state").select("next_end").eq("key", KEY).limit(1).execute().data or []
        value = rows[0]["next_end"] if rows else None
        return date.fromisoformat(value) if value else None

    def set(self, next_end: date, done: bool = False) -> None:
        self._windows += 1
        self.done = done
        self._db.table("archive_state").upsert(
            {"key": KEY, "next_end": next_end.isoformat(), "done": done, "windows_done": self._windows},
            on_conflict="key",
        ).execute()
