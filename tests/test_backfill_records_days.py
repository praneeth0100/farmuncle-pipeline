"""
tests/test_backfill_records_days.py

Days filled by a manual historical_backfill run are recorded in ingest_days,
so the nightly job does not fetch them again and the table stays the single
list of what is final.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from farmuncle_pipeline.config import IngestionBatchStatus
from farmuncle_pipeline.ingestion import historical_backfill

from tests.test_ingest_days import _Db, _Result


def test_each_backfilled_date_is_recorded(monkeypatch):
    today = datetime.now(historical_backfill._IST).date()
    complete, partial = today - timedelta(days=4), today - timedelta(days=3)
    results = {
        complete: _Result(IngestionBatchStatus.SUCCESS, records=15000),
        partial: _Result(IngestionBatchStatus.PARTIAL, records=9000),
    }
    for r in results.values():
        r.rows_quarantined = 0
        r.fetch_seconds = r.identity_seconds = r.upsert_seconds = 0.0
        r.identity_stats = {k: 0 for k in (
            "mandi_cache_hits", "mandi_preload_hits", "mandi_rpc_calls",
            "crop_cache_hits", "crop_preload_hits", "crop_rpc_calls")}

    monkeypatch.setattr(
        historical_backfill, "ingest_resource2_for_date",
        lambda ctx, *, target_date, job_name: results[target_date],
    )
    monkeypatch.setattr(historical_backfill, "refresh_price_cache", lambda *a, **k: None)

    class Ctx:
        supabase = _Db()

    historical_backfill.run_historical_backfill(
        Ctx(), start_date=complete, end_date=partial
    )
    days = Ctx.supabase.days()
    assert days[complete.isoformat()]["status"] == "FINAL" and days[complete.isoformat()]["v2_rows"] == 15000
    assert days[partial.isoformat()]["status"] == "PENDING"      # partial and young: still to be retried
