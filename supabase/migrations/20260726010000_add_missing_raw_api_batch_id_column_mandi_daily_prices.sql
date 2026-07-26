-- Applied directly against flxjrcbhmcuaynctokpv (Claude session, 2026-07-26).
-- record_processor.py (line 160) writes a "raw_api_batch_id" field on every
-- mandi_daily_prices row - a second, separate lineage field from
-- "batch_id" (which references ingestion_batches, the Phase C
-- identity/price PROCESSING run). This one tracks which Phase A/B raw-FETCH
-- run (raw_api_batches) originally supplied the data - same distinction as
-- raw_price_entries.first_seen_batch_id/last_seen_batch_id
-- (see 20260726000100_refix_raw_price_entries_batch_id_fk.sql).
--
-- Confirmed live: PGRST204 "Could not find the 'raw_api_batch_id' column of
-- 'mandi_daily_prices' in the schema cache" on run
-- 01KYE304S8KRJZGFTDEG3AXBS4 (2026-07-26 02:10 UTC), chunk size=19 - the
-- run that got furthest yet (19 rows collected across 33 state pages)
-- before failing at the same final upsert step as the batch_id gap.
--
-- Cross-checked every other field record_processor.py writes against the
-- live column list after this fix - all 17 fields now have a matching
-- column, no further gaps found on this table.
alter table mandi_daily_prices
  add column if not exists raw_api_batch_id text references raw_api_batches(id);

create index if not exists idx_mandi_daily_prices_raw_api_batch_id on mandi_daily_prices(raw_api_batch_id);
