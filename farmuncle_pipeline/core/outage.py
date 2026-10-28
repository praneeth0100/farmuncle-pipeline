"""Run-level guards: stop grinding through a dead feed, and raise alerts a person will see.

QUALITY-003  too many rows quarantined in one run
QUALITY-004  a feed that should have data returned none
QUALITY-005  the feed stopped answering (consecutive failed pages)
Each alert is written once while an OPEN one for the same resource/day exists.
"""

from __future__ import annotations

from farmuncle_pipeline.config import ConfigError, Resource

FLOOD_SHARE_PCT = 5.0
FLOOD_MIN_ROWS = 20


class OutageBreaker:
    """Counts consecutive failed page fetches; `tripped` means stop asking the feed."""

    def __init__(self, max_consecutive: int = 6) -> None:
        self.max_consecutive = max_consecutive
        self.consecutive = 0
        self.last_error: str | None = None

    def record(self, ok: bool, error: str | None = None) -> None:
        if ok:
            self.consecutive = 0
        else:
            self.consecutive += 1
            self.last_error = error

    @property
    def tripped(self) -> bool:
        return self.consecutive >= self.max_consecutive


def quarantine_flood(*, rows: int, quarantined: int) -> bool:
    return quarantined >= FLOOD_MIN_ROWS and rows > 0 and quarantined * 100 / rows >= FLOOD_SHARE_PCT


def _alert_once(supabase, *, code: str, batch_id: str, prefix: str, message: str, severity: str = "HIGH") -> bool:
    try:
        existing = (
            supabase.table("quality_alerts").select("id").eq("status", "OPEN").eq("error_code", code)
            .like("message", prefix + "%").limit(1).execute()
        )
        if existing.data:
            return False
        supabase.table("quality_alerts").insert(
            {"severity": severity, "error_code": code, "batch_id": batch_id, "message": message}
        ).execute()
    except Exception as exc:
        raise ConfigError(f"Failed to write {code} quality_alerts row: {exc}") from exc
    return True


def alert_quarantine_flood(supabase, *, batch_id: str, resource: Resource, rows: int, quarantined: int) -> bool:
    prefix = f"Quarantine flood for {resource.value}"
    return _alert_once(
        supabase, code="QUALITY-003", batch_id=batch_id, prefix=prefix,
        message=f"{prefix}: {quarantined} of {rows} rows ({quarantined * 100 / rows:.1f}%) were quarantined in one run. "
                f"A feed field or unit probably changed; look at data_quality_issues for this batch.",
    )


def alert_empty_feed(supabase, *, batch_id: str, resource: Resource, date_str: str) -> bool:
    prefix = f"Empty feed for {resource.value} on {date_str}"
    return _alert_once(
        supabase, code="QUALITY-004", batch_id=batch_id, prefix=prefix,
        message=f"{prefix}: the feed answered but returned no records for a day that should have them.",
    )


def alert_feed_outage(supabase, *, batch_id: str, resource: Resource, failures: int, last_error: str | None) -> bool:
    prefix = f"Feed outage for {resource.value}"
    return _alert_once(
        supabase, code="QUALITY-005", batch_id=batch_id, prefix=prefix, severity="CRITICAL",
        message=f"{prefix}: {failures} consecutive page fetches failed, run stopped early. Last error: {last_error}",
    )
