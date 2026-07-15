"""
FarmUncle v2 — nightly_quality_audit.py
Phase D, Step 19 (Master Build Specification §21).

Purpose (module-level):
    The nightly audit named in §21/§22/§26 of the spec, none of which
    existed in code until now — every prior "audit" of this pipeline
    (ghost-entity checks, duplicate-row checks, coverage checks) was a
    person running SQL by hand against `wqccgjmvslevkglfkmtc` (see the
    2026-07-15 session's manual audit). This script is that manual
    process turned into something that runs unattended every night and
    leaves a durable, queryable record behind instead of a chat
    transcript.

    Six checks, chosen because each one was independently verified by
    hand at least once already this project and is cheap to compute
    from tables that already exist:
      1. Ghost mandis   — ACTIVE mandis with zero mandi_daily_prices rows
      2. Ghost crops    — ACTIVE crops with zero mandi_daily_prices rows
      3. Duplicate business-key rows in mandi_daily_prices — should be
         structurally impossible given upsert_price_rows' own
         in-memory dedup + the table's unique constraint; checked
         anyway as a safety net, not because a hit is expected.
      4. Data-quality-issue backlog — PENDING data_quality_issues
         (see review_quality_issues.py) older than
         `_STALE_REVIEW_DAYS` with no human triage yet.
      5. Failed-page backlog — PENDING failed_pages older than
         `_STALE_FAILED_PAGE_HOURS` that retry_failed_pages.py hasn't
         cleared on its own twice-daily schedule.
      6. Date coverage gaps — any calendar date strictly between the
         earliest and latest `mandi_daily_prices.price_date` that has
         zero rows at all (a day that should have arrived and simply
         didn't, as opposed to genuinely not-yet-backfilled range).

Why five new views exist alongside this script (`add_nightly_audit_views`
migration — v_audit_ghost_mandis, v_audit_ghost_crops,
v_audit_duplicate_price_keys, v_audit_price_date_bounds,
v_audit_present_price_dates), instead of this script computing checks
1/2/3/6 by paging entire tables into Python and diffing them there:
    Every other set-based question this pipeline needs answered
    (identity resolution, normalization) is pushed into the database
    rather than reimplemented in Python, per invariant 3. A "which
    mandis have zero price rows" or "which business keys are
    duplicated" question is the same kind of thing — cheap for
    Postgres to compute directly, expensive and fragile to compute by
    paginating ~2,800 mandis and 500,000+ price rows through
    `supabase-py`'s default 1,000-row response cap and diffing sets in
    application code. The views are plain `select`, read-only, queried
    through the exact same `supabase.table(...)` interface as every
    real table — this script never runs raw SQL.

    Findings are written to two tables that already existed in the
    Phase B schema but nothing wrote to before this script:
      - One `coverage_reports` row per run (scope='NIGHTLY_AUDIT'),
        summarizing all six checks in `details` (jsonb) — this is the
        "coverage report" §26's success criteria refers to.
      - One `quality_alerts` row per *new* finding, linked back to
        that `coverage_reports` row via `coverage_report_id`.

Idempotency (why re-running this doesn't spam duplicate alerts):
    Before inserting a `quality_alerts` row, this script checks for an
    existing OPEN alert with the same `error_code` and (`entity_id` if
    the check is entity-scoped, else the same `message`). If one
    already exists, it's left alone — not duplicated, not touched.
    This means: an operator can ACKNOWLEDGE or RESOLVE an alert via
    direct SQL/RPC (§20's Manual Review SOP — no tooling for this
    exists yet, same status note as the spec's) and it will only
    reappear if the underlying problem is still there NEXT time this
    script runs and finds it again, not on every run in between.

Explicitly out of scope for this script:
    - Fixing anything it finds (ghost entities, duplicates, backlogs)
      — this is detection and alerting only, per §20's split between
      automated detection and human review.
    - Per-state coverage reconciliation for a single date — that's
      QUALITY-001, already implemented inline in
      `resource2_pipeline.py`'s and `live_tick.py`'s own fetch loops
      (print-only, batch-scoped). This script's coverage check is
      date-level ("did this whole date arrive at all"), a different
      and complementary question from "did this date's Karnataka page
      undercount vs. the government's own reported total."
    - `daily_rewrite.py`'s §16 outage-alert logic (3 consecutive
      FAILUREs) — stays there, unrelated to this script's checks.

Usage:
    python -m farmuncle_pipeline.ops.nightly_quality_audit
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from farmuncle_pipeline.config import AlertEntityType, AlertSeverity, AlertStatus, ConfigError
from farmuncle_pipeline.core.startup_validation import validate_startup

# A PENDING data_quality_issues row older than this with no review
# yet is worth surfacing — §20's Manual Review SOP has no cadence
# specified, so this is a deliberate, documented default rather than
# a value pulled from anywhere in the spec.
_STALE_REVIEW_DAYS = 3

# retry_failed_pages.py runs twice daily (06:00 / 16:00 UTC). A
# PENDING failed_pages row that survives more than one full day has
# already had at least two retry attempts and failed both — worth a
# human look rather than waiting indefinitely.
_STALE_FAILED_PAGE_HOURS = 24

# v_audit_present_price_dates is fetched in one page of this size —
# comfortably above the 3,650 rows a full 10-year backfill will
# eventually produce, so this stays a single page for years.
_DATE_PAGE_SIZE = 5000


@dataclass
class Finding:
    """Purpose: one thing this audit found, before it's turned into a
    quality_alerts row. Kept as an intermediate dataclass (rather than
    inserting directly inline in each check function) so `main()` can
    print one consistent summary line per finding regardless of which
    check produced it."""
    error_code: str
    severity: AlertSeverity
    message: str
    entity_type: AlertEntityType | None = None
    entity_id: int | None = None


@dataclass
class AuditSummary:
    """Purpose: the six checks' raw counts, written verbatim into
    `coverage_reports.details` — the full audit result, independent of
    which findings were new vs. already-alerted."""
    ghost_mandis: int = 0
    ghost_crops: int = 0
    duplicate_business_keys: int = 0
    stale_data_quality_issues: int = 0
    stale_failed_pages: int = 0
    missing_dates: list[str] = field(default_factory=list)


def _check_ghost_mandis(supabase) -> tuple[list[Finding], int]:
    """Purpose: ACTIVE mandis with zero mandi_daily_prices rows — the
    exact v1 failure mode (51% ghost mandis) this rebuild exists to
    prevent. Queries `v_audit_ghost_mandis` (see module docstring for
    why this is a view, not a Python-side diff)."""
    try:
        rows = supabase.table("v_audit_ghost_mandis").select("id,normalized_name,state").execute().data or []
    except Exception as exc:
        raise ConfigError(f"Ghost-mandi check failed: {exc}") from exc

    findings = [
        Finding(
            error_code="AUDIT-001",
            severity=AlertSeverity.HIGH,
            message=(
                f"Mandi id={row['id']} ({row['normalized_name']!r}, {row['state']}) "
                f"is ACTIVE but has zero mandi_daily_prices rows."
            ),
            entity_type=AlertEntityType.MANDI,
            entity_id=row["id"],
        )
        for row in rows
    ]
    return findings, len(rows)


def _check_ghost_crops(supabase) -> tuple[list[Finding], int]:
    """Purpose: same check as `_check_ghost_mandis`, for crops, via
    `v_audit_ghost_crops`."""
    try:
        rows = supabase.table("v_audit_ghost_crops").select("id,normalized_name").execute().data or []
    except Exception as exc:
        raise ConfigError(f"Ghost-crop check failed: {exc}") from exc

    findings = [
        Finding(
            error_code="AUDIT-002",
            severity=AlertSeverity.HIGH,
            message=(
                f"Crop id={row['id']} ({row['normalized_name']!r}) is ACTIVE but "
                f"has zero mandi_daily_prices rows."
            ),
            entity_type=AlertEntityType.CROP,
            entity_id=row["id"],
        )
        for row in rows
    ]
    return findings, len(rows)


def _check_duplicate_business_keys(supabase) -> tuple[list[Finding], int]:
    """Purpose: rows sharing (mandi_id, crop_id, variety, price_date),
    via `v_audit_duplicate_price_keys` — should be structurally
    impossible (upsert_price_rows dedupes in-memory before every chunk
    upsert, and the table's own unique constraint would reject a true
    duplicate insert). Checked as a safety net, not because a hit is
    expected; a hit here would mean something upstream of the upsert
    path changed in a way that defeated both protections at once,
    which is exactly the kind of thing worth a CRITICAL alert rather
    than silent trust that it can't happen."""
    try:
        rows = (
            supabase.table("v_audit_duplicate_price_keys")
            .select("mandi_id,crop_id,variety,price_date,row_count")
            .execute()
            .data
            or []
        )
    except Exception as exc:
        raise ConfigError(f"Duplicate business-key check failed: {exc}") from exc

    if not rows:
        return [], 0

    finding = Finding(
        error_code="AUDIT-003",
        severity=AlertSeverity.CRITICAL,
        message=(
            f"{len(rows)} business-key(s) have duplicate rows in mandi_daily_prices "
            f"(e.g. mandi_id={rows[0]['mandi_id']}, crop_id={rows[0]['crop_id']}, "
            f"variety={rows[0]['variety']!r}, price_date={rows[0]['price_date']}, "
            f"count={rows[0]['row_count']}) — should be structurally impossible, "
            f"investigate the upsert path immediately."
        ),
    )
    return [finding], len(rows)


def _check_stale_data_quality_issues(supabase) -> tuple[list[Finding], int]:
    """Purpose: PENDING data_quality_issues older than
    `_STALE_REVIEW_DAYS` — rows `review_quality_issues.py` exists to
    triage, but nothing forces that to actually happen. One aggregate
    finding (not one per row) to avoid flooding quality_alerts if a
    backlog builds up; the count itself is the useful signal, and
    `review_quality_issues.py list` is how an operator drills in."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=_STALE_REVIEW_DAYS)).isoformat()
    try:
        result = (
            supabase.table("data_quality_issues")
            .select("id", count="exact")
            .eq("review_status", "PENDING")
            .lt("created_at", cutoff)
            .execute()
        )
    except Exception as exc:
        raise ConfigError(f"Stale data_quality_issues check failed: {exc}") from exc

    count = result.count or 0
    if count == 0:
        return [], 0

    finding = Finding(
        error_code="AUDIT-004",
        severity=AlertSeverity.MEDIUM,
        message=(
            f"{count} data_quality_issues row(s) have been PENDING review for "
            f"more than {_STALE_REVIEW_DAYS} day(s) — see "
            f"`python -m farmuncle_pipeline.ops.review_quality_issues list`."
        ),
    )
    return [finding], count


def _check_stale_failed_pages(supabase) -> tuple[list[Finding], int]:
    """Purpose: PENDING failed_pages older than
    `_STALE_FAILED_PAGE_HOURS` — pages retry_failed_pages.py has had
    at least two scheduled chances (06:00/16:00 UTC) to clear and
    hasn't. One aggregate finding, same reasoning as the
    data_quality_issues check above."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=_STALE_FAILED_PAGE_HOURS)).isoformat()
    try:
        result = (
            supabase.table("failed_pages")
            .select("id", count="exact")
            .eq("status", "PENDING")
            .lt("created_at", cutoff)
            .execute()
        )
    except Exception as exc:
        raise ConfigError(f"Stale failed_pages check failed: {exc}") from exc

    count = result.count or 0
    if count == 0:
        return [], 0

    finding = Finding(
        error_code="AUDIT-005",
        severity=AlertSeverity.MEDIUM,
        message=(
            f"{count} failed_pages row(s) have been PENDING for more than "
            f"{_STALE_FAILED_PAGE_HOURS}h, surviving at least one scheduled "
            f"retry_failed_pages.py run — investigate why the retry keeps failing."
        ),
    )
    return [finding], count


def _check_date_coverage(supabase) -> tuple[list[Finding], list[str], int, int]:
    """Purpose: any calendar date strictly inside
    [min(price_date), max(price_date)] with zero mandi_daily_prices
    rows at all — a day that should be there (it's inside the range
    already ingested) and simply isn't. Deliberately bounded to the
    already-ingested range, not "every day since 10 years ago" —
    Phase E's backfill isn't complete yet, so dates outside the
    current min/max are "not yet backfilled," a known and separately
    tracked state, not a gap. Reads bounds from
    `v_audit_price_date_bounds` and the actual present dates from
    `v_audit_present_price_dates`.
    Returns: (findings, missing_date_strings, expected_day_count, actual_day_count).
    """
    try:
        bounds_rows = (
            supabase.table("v_audit_price_date_bounds")
            .select("min_date,max_date,distinct_dates")
            .execute()
            .data
            or []
        )
    except Exception as exc:
        raise ConfigError(f"Coverage bounds query failed: {exc}") from exc

    if not bounds_rows or bounds_rows[0]["min_date"] is None:
        return [], [], 0, 0

    min_date = datetime.strptime(bounds_rows[0]["min_date"], "%Y-%m-%d").date()
    max_date = datetime.strptime(bounds_rows[0]["max_date"], "%Y-%m-%d").date()
    expected_days = (max_date - min_date).days + 1

    try:
        present_rows = (
            supabase.table("v_audit_present_price_dates")
            .select("price_date")
            .range(0, _DATE_PAGE_SIZE - 1)
            .execute()
            .data
            or []
        )
    except Exception as exc:
        raise ConfigError(f"Coverage present-dates query failed: {exc}") from exc

    if len(present_rows) >= _DATE_PAGE_SIZE:
        raise ConfigError(
            f"v_audit_present_price_dates returned >= {_DATE_PAGE_SIZE} rows — "
            f"_DATE_PAGE_SIZE needs raising before this check can be trusted."
        )

    present_dates = {row["price_date"] for row in present_rows}
    missing: list[str] = []
    d = min_date
    while d <= max_date:
        d_str = d.isoformat()
        if d_str not in present_dates:
            missing.append(d_str)
        d += timedelta(days=1)

    findings = [
        Finding(
            error_code="AUDIT-006",
            severity=AlertSeverity.HIGH,
            message=(
                f"{d_str} has zero mandi_daily_prices rows despite falling inside "
                f"the already-ingested range ({min_date} to {max_date}) — a real "
                f"gap, not an unbackfilled date."
            ),
        )
        for d_str in missing
    ]
    return findings, missing, expected_days, len(present_dates)


def _alert_already_open(supabase, finding: Finding) -> bool:
    """Purpose: idempotency check (see module docstring) — is there
    already an OPEN quality_alerts row for this exact finding, so this
    run should skip re-inserting it?"""
    query = (
        supabase.table("quality_alerts")
        .select("id")
        .eq("status", AlertStatus.OPEN.value)
        .eq("error_code", finding.error_code)
    )
    if finding.entity_id is not None:
        query = query.eq("entity_id", finding.entity_id)
    else:
        query = query.eq("message", finding.message)
    try:
        result = query.limit(1).execute()
    except Exception as exc:
        raise ConfigError(f"Failed to check existing quality_alerts for dedup: {exc}") from exc
    return bool(result.data)


def _insert_alert(supabase, finding: Finding, *, coverage_report_id: int) -> None:
    payload = {
        "severity": finding.severity.value,
        "error_code": finding.error_code,
        "message": finding.message,
        "entity_type": finding.entity_type.value if finding.entity_type else None,
        "entity_id": finding.entity_id,
        "coverage_report_id": coverage_report_id,
        "status": AlertStatus.OPEN.value,
    }
    try:
        supabase.table("quality_alerts").insert(payload).execute()
    except Exception as exc:
        raise ConfigError(f"Failed to insert quality_alerts row ({finding.error_code}): {exc}") from exc


def _insert_coverage_report(
    supabase, *, expected_days: int, actual_days: int, summary: AuditSummary
) -> int:
    coverage_pct = round((actual_days / expected_days) * 100, 2) if expected_days else 0.0
    payload = {
        "report_date": date.today().isoformat(),
        "scope": "NIGHTLY_AUDIT",
        "expected_count": expected_days,
        "actual_count": actual_days,
        "coverage_pct": coverage_pct,
        "details": {
            "ghost_mandis": summary.ghost_mandis,
            "ghost_crops": summary.ghost_crops,
            "duplicate_business_keys": summary.duplicate_business_keys,
            "stale_data_quality_issues": summary.stale_data_quality_issues,
            "stale_failed_pages": summary.stale_failed_pages,
            "missing_dates": summary.missing_dates,
        },
    }
    try:
        result = supabase.table("coverage_reports").insert(payload).execute()
    except Exception as exc:
        raise ConfigError(f"Failed to insert coverage_reports row: {exc}") from exc
    rows = result.data or []
    if not rows:
        raise ConfigError("coverage_reports insert returned no row — cannot link quality_alerts to it.")
    return rows[0]["id"]


def run_nightly_quality_audit(ctx) -> None:
    """
    Purpose:
        Run all six checks, write one `coverage_reports` row
        summarizing them, and insert a `quality_alerts` row for every
        *new* finding (see module docstring for the idempotency rule).
        Deliberately has no ingestion_batches lifecycle — this isn't a
        government-API ingestion run (mirrors review_quality_issues.py
        in that respect: ops tooling, not a Phase C script).
    Inputs:
        ctx: a `StartupContext` from `validate_startup()`.
    Outputs:
        None (prints a summary; durable record lives in
        coverage_reports/quality_alerts).
    Failure modes:
        Any check's `ConfigError` propagates and aborts the run
        immediately — a half-finished audit that silently drops the
        remaining checks would be worse than a clearly failed run an
        operator notices via the GitHub Actions job status.
    """
    supabase = ctx.supabase
    summary = AuditSummary()
    all_findings: list[Finding] = []

    ghost_mandi_findings, summary.ghost_mandis = _check_ghost_mandis(supabase)
    all_findings.extend(ghost_mandi_findings)

    ghost_crop_findings, summary.ghost_crops = _check_ghost_crops(supabase)
    all_findings.extend(ghost_crop_findings)

    dup_findings, summary.duplicate_business_keys = _check_duplicate_business_keys(supabase)
    all_findings.extend(dup_findings)

    stale_dqi_findings, summary.stale_data_quality_issues = _check_stale_data_quality_issues(supabase)
    all_findings.extend(stale_dqi_findings)

    stale_fp_findings, summary.stale_failed_pages = _check_stale_failed_pages(supabase)
    all_findings.extend(stale_fp_findings)

    coverage_findings, summary.missing_dates, expected_days, actual_days = _check_date_coverage(supabase)
    all_findings.extend(coverage_findings)

    coverage_report_id = _insert_coverage_report(
        supabase, expected_days=expected_days, actual_days=actual_days, summary=summary
    )

    new_alerts = 0
    skipped_existing = 0
    for finding in all_findings:
        if _alert_already_open(supabase, finding):
            skipped_existing += 1
            continue
        _insert_alert(supabase, finding, coverage_report_id=coverage_report_id)
        new_alerts += 1

    coverage_pct = round((actual_days / expected_days) * 100, 2) if expected_days else 0.0
    print(
        f"[nightly_quality_audit] coverage_reports id={coverage_report_id} "
        f"({actual_days}/{expected_days} days, {coverage_pct}%). "
        f"{len(all_findings)} finding(s): {new_alerts} new alert(s) raised, "
        f"{skipped_existing} already OPEN and left as-is.\n"
        f"  ghost_mandis={summary.ghost_mandis} ghost_crops={summary.ghost_crops} "
        f"duplicate_business_keys={summary.duplicate_business_keys} "
        f"stale_data_quality_issues={summary.stale_data_quality_issues} "
        f"stale_failed_pages={summary.stale_failed_pages} "
        f"missing_dates={summary.missing_dates}"
    )


def main() -> None:
    ctx = validate_startup()
    run_nightly_quality_audit(ctx)


if __name__ == "__main__":
    main()
