-- Applied directly against flxjrcbhmcuaynctokpv (Claude session, 2026-07-25).
-- mandi_daily_prices had ZERO unique constraints in this project. The
-- ingestion pipeline's upsert_price_rows() calls:
--   .upsert(chunk, on_conflict="mandi_id,crop_id,variety,grade,price_date,source_mandi_id")
-- which requires a matching unique/exclusion constraint to exist, or every
-- upsert call fails outright with:
--   "there is no unique or exclusion constraint matching the ON CONFLICT specification"
-- Table was confirmed empty (0 rows) at the time this was applied, so this
-- was a zero-risk change with no backfill needed.

-- variety/grade are written as normalized strings by the pipeline (never
-- NULL - identity_client.py defaults blank to 'other'), and source_mandi_id
-- is always the resolved mandi id (never NULL either). Enforcing
-- NOT NULL DEFAULT '' on variety/grade closes the standard "NULL breaks
-- ON CONFLICT matching" landmine defensively, matching the same
-- coalesce(variety,'') treatment refresh_price_cache() already uses when
-- reading this table.
alter table mandi_daily_prices
  alter column variety set default '',
  alter column variety set not null,
  alter column grade set default '',
  alter column grade set not null;

alter table mandi_daily_prices
  add constraint uq_prices_business_key_v2
  unique (mandi_id, crop_id, variety, grade, price_date, source_mandi_id);
