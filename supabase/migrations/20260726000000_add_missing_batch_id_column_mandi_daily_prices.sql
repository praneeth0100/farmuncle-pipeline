-- Applied directly against flxjrcbhmcuaynctokpv (Claude session, 2026-07-26).
-- record_processor.py (line ~159) writes a "batch_id" field on every
-- mandi_daily_prices row for lineage tracking (which ingestion batch wrote
-- this row) - consistent with the batch-lineage tracking already present on
-- raw_price_entries (first_seen_batch_id/last_seen_batch_id). The column
-- didn't exist, so every real upsert call failed with PGRST204:
--   "Could not find the 'batch_id' column of 'mandi_daily_prices' in the schema cache"
-- Confirmed live in ingestion_batches.error_summary for run
-- 01KYDW08M4A1DGPD4ZGVY8W17N (2026-07-26 00:08 UTC) - rows_processed=2,
-- upsert step failed, no rows ever reached mandi_daily_prices as a result.

-- References ingestion_batches, NOT raw_api_batches - this column tracks
-- which identity/price PROCESSING run (Phase C) wrote the row, distinct
-- from raw_price_entries' first_seen_batch_id/last_seen_batch_id which
-- track the raw FETCH run (Phase A/B, raw_api_batches) - see the sibling
-- migration 20260726000100_refix_raw_price_entries_batch_id_fk.sql for that
-- distinction being corrected on the other table.
alter table mandi_daily_prices
  add column if not exists batch_id text references ingestion_batches(id);

create index if not exists idx_mandi_daily_prices_batch_id on mandi_daily_prices(batch_id);
