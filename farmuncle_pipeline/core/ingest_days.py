"""
FarmUncle v2 — ingest_days.py

Purpose (module-level):
    Tracks, per calendar date, whether Resource 2 (the final feed) has
    been fully ingested. `daily_rewrite` used to re-check a fixed
    3-day window (ADR 0003), so after any outage the older days were
    never revisited, and a v2 lag longer than the window silently
    stopped being covered. Now every date is a row in `ingest_days`:
    PENDING until a run proves v2 is complete for it, then FINAL. Each
    night `daily_rewrite` works through the PENDING dates, oldest first.

When a day counts as FINAL (`decide`):
    - the run finished SUCCESS (every state fetched and reconciled
      against the government's totals) and v2 returned records;
    - v2 returned nothing for 7 days after the date (it has none to
      give; recorded as final with 0 rows);
    - the run stayed PARTIAL for 14 days (a persistent gap; the gap
      itself stays in `failed_pages` / `quality_alerts`).
    A FAILED run is never final.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from farmuncle_pipeline.config import ConfigError

PENDING = "PENDING"
FINAL = "FINAL"

GIVE_UP_EMPTY_DAYS = 7
GIVE_UP_PARTIAL_DAYS = 14
FIRST_RUN_LOOKBACK_DAYS = 14   # with no history anywhere, start this far back
_PAGE = 1000                   # PostgREST returns at most this many rows per request
_TABLE = "ingest_days"


@dataclass(frozen=True)
class DayDecision:
    final: bool
    note: str | None


def decide(*, final_status: str, total_records: int, price_date: date, today: date) -> DayDecision:
    """Purpose: turn one run's outcome for a date into FINAL or PENDING (see module docstring)."""
    age = (today - price_date).days
    if final_status == "FAILED":
        return DayDecision(False, "run failed")
    if total_records == 0:
        if age >= GIVE_UP_EMPTY_DAYS:
            return DayDecision(True, f"v2 had no records {GIVE_UP_EMPTY_DAYS}+ days after the date")
        return DayDecision(False, "no v2 data yet")
    if final_status == "SUCCESS":
        return DayDecision(True, None)
    if age >= GIVE_UP_PARTIAL_DAYS:
        return DayDecision(True, f"gave up after {GIVE_UP_PARTIAL_DAYS} days: still partial")
    return DayDecision(False, "partial")


def _first(client, query_builder) -> dict | None:
    try:
        rows = query_builder.execute().data or []
    except Exception as exc:
        raise ConfigError(f"ingest_days query failed: {exc}") from exc
    return rows[0] if rows else None


def seed_pending_days(client, *, today: date) -> list[date]:
    """
    Purpose:
        Make sure every date up to yesterday has a row. Starts the day
        after the newest FINAL date; with none, from the newest price
        already in the warehouse; with none, 14 days back. Existing
        rows (and their attempt counts) are left alone.
    Outputs:
        Every date from the start through yesterday (empty if up to date).
    """
    yesterday = today - timedelta(days=1)
    newest_final = _first(
        client,
        client.table(_TABLE).select("price_date").eq("status", FINAL).order("price_date", desc=True).limit(1),
    )
    if newest_final:
        start = date.fromisoformat(newest_final["price_date"]) + timedelta(days=1)
    else:
        newest_price = _first(
            client,
            client.table("mandi_daily_prices").select("price_date").order("price_date", desc=True).limit(1),
        )
        start = (
            date.fromisoformat(newest_price["price_date"])
            if newest_price
            else today - timedelta(days=FIRST_RUN_LOOKBACK_DAYS)
        )
    if start > yesterday:
        return []
    dates = [start + timedelta(days=i) for i in range((yesterday - start).days + 1)]
    try:
        for i in range(0, len(dates), 500):
            client.table(_TABLE).upsert(
                [{"price_date": d.isoformat(), "status": PENDING} for d in dates[i : i + 500]],
                on_conflict="price_date",
                ignore_duplicates=True,
            ).execute()
    except Exception as exc:
        raise ConfigError(f"Failed to seed ingest_days: {exc}") from exc
    return dates


def recheck_recent_days(client, *, today: date, days: int = 14) -> None:
    """Purpose: reopen FINAL days from the last `days` days so late v2 corrections are picked up."""
    cutoff = (today - timedelta(days=days)).isoformat()
    try:
        client.table(_TABLE).update({"status": PENDING, "final_at": None}).eq("status", FINAL).gte(
            "price_date", cutoff
        ).execute()
    except Exception as exc:
        raise ConfigError(f"Failed to reopen recent ingest_days: {exc}") from exc


def pending_days(client, *, before: date) -> list[date]:
    """Purpose: PENDING dates strictly before `before`, oldest first (today never has v2 data)."""
    out: list[date] = []
    try:
        while True:
            page = (
                client.table(_TABLE)
                .select("price_date")
                .eq("status", PENDING)
                .lt("price_date", before.isoformat())
                .order("price_date")
                .range(len(out), len(out) + _PAGE - 1)
                .execute()
            ).data or []
            out.extend(date.fromisoformat(r["price_date"]) for r in page)
            if len(page) < _PAGE:
                return out
    except Exception as exc:
        raise ConfigError(f"Failed to list pending ingest_days: {exc}") from exc


def record_day_result(client, *, price_date: date, final: bool, v2_rows: int, note: str | None) -> None:
    """Purpose: store one run's outcome for a date and count the attempt. Creates the row if absent
    (a day filled by a manual `historical_backfill` run was never seeded)."""
    now = datetime.now(timezone.utc).isoformat()
    existing = _first(
        client,
        client.table(_TABLE).select("attempts").eq("price_date", price_date.isoformat()).limit(1),
    )
    row = {
        "price_date": price_date.isoformat(),
        "status": FINAL if final else PENDING,
        "attempts": (existing or {}).get("attempts", 0) + 1,
        "last_attempt_at": now,
        "last_error": note,
        "v2_rows": v2_rows,
        "final_at": now if final else None,
    }
    try:
        client.table(_TABLE).upsert(row, on_conflict="price_date").execute()
    except Exception as exc:
        raise ConfigError(f"Failed to record ingest_days result for {price_date}: {exc}") from exc
