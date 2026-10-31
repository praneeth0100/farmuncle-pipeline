"""Note each day's archive state in archive_days. Bookkeeping only: nothing here may stop or change a run."""

from __future__ import annotations

from datetime import date

SLIVER_SHARE = 0.3  # a day with under 30% of the usual rows is incomplete (a Sunday is about 60%)
MAX_TRIES_PER_DAY = 5  # after this many tries a FAILED day is left in archive_days for a look, not retried every run


def record_day(db, day, status: str, *, result=None, error: str | None = None) -> None:
    try:
        db.rpc("record_archive_day", {
            "p_day": day.isoformat(), "p_status": status,
            "p_rows": getattr(result, "rows", None), "p_bytes": getattr(result, "bytes", None),
            "p_quarantined": getattr(result, "quarantined", None), "p_r2_rows": getattr(result, "r2_rows", None),
            "p_error": error[:500] if error else None,
        }).execute()
    except Exception as exc:
        print(f"[archive] could not record {day} as {status}: {exc}")


def failed_days(db, after, up_to) -> list[date]:
    """Days the tracker holds as FAILED. A day that failed before ingesting has no warehouse rows to find it by."""
    rows = (db.table("archive_days").select("day").eq("status", "FAILED")
            .lt("attempts", MAX_TRIES_PER_DAY).gte("day", after.isoformat()).lte("day", up_to.isoformat()).execute().data or [])
    return [date.fromisoformat(r["day"]) for r in rows]


def typical_rows(db) -> int | None:
    """Median rows of the last 14 archived days; None when it cannot be read (then no day is judged by size)."""
    try:
        rows = (db.table("archive_days").select("rows_sent").in_("status", ["ARCHIVED", "DELETED"]).gt("rows_sent", 0)
                .order("day", desc=True).limit(14).execute().data or [])
        sent = sorted(r["rows_sent"] for r in rows)
        return sent[len(sent) // 2] if sent else None
    except Exception:
        return None


def incomplete_reason(rows: int, typical: int | None) -> str | None:
    """Why a day that did upload is still not whole. 2 to 27 June 2026 went to R2 with ~500 rows instead of ~20,000."""
    if typical and rows < SLIVER_SHARE * typical:
        return f"incomplete: {rows} rows, a normal day has about {typical}; archived as it is, pulled again on retry"
    return None
