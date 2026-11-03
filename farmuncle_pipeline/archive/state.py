"""Where the backward history fill has got to (one row in archive_state)."""

from __future__ import annotations

from datetime import date

KEY = "history"


class SupabaseArchiveState:
    def __init__(self, supabase) -> None:
        self._db = supabase
        self.done = False
        self._windows = 0
        self._outages = 0
        rows = (self._db.table("archive_state").select("done,windows_done,outage_tries").eq("key", KEY).limit(1)
                .execute().data or [])
        if rows:
            self.done, self._windows = bool(rows[0]["done"]), int(rows[0]["windows_done"])
            self._outages = int(rows[0].get("outage_tries") or 0)

    def get(self) -> date | None:
        rows = self._db.table("archive_state").select("next_end").eq("key", KEY).limit(1).execute().data or []
        value = rows[0]["next_end"] if rows else None
        return date.fromisoformat(value) if value else None

    def set(self, next_end: date, done: bool = False) -> None:
        self._windows += 1
        self.done = done
        self._outages = 0  # a new window starts with no redos
        self._db.table("archive_state").upsert(
            {"key": KEY, "next_end": next_end.isoformat(), "done": done, "windows_done": self._windows, "outage_tries": 0},
            on_conflict="key",
        ).execute()

    def outages(self) -> int:
        """How many times the current window was redone because every day in it failed."""
        return self._outages

    def note_outage(self) -> None:
        self._outages += 1
        self._db.table("archive_state").update({"outage_tries": self._outages}).eq("key", KEY).execute()
