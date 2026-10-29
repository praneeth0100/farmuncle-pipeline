"""Note each day's archive state in archive_days. Bookkeeping only: nothing here may stop or change a run."""

from __future__ import annotations


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
