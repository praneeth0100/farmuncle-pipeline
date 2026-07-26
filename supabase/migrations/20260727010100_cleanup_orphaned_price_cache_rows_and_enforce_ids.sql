-- 68 price_cache rows (0.27% of the table, at time of writing) had no matching
-- (mandi_id, crop_id, variety, grade) combo left in mandi_daily_prices at all —
-- the source data had aged out of the rolling raw-data window, but
-- refresh_price_cache() only ever inserts/updates, never prunes rows whose
-- source combo vanished. These were stale "latest price" rows being served
-- with no current backing data. Confirmed via manual spot-check: 0 matching
-- mandi_daily_prices rows for a sample before deleting.

delete from public.price_cache
where variety_id is null or grade_id is null;

-- Now that every remaining row has real ids, enforce it structurally so a
-- future row can never be inserted without them.
alter table public.price_cache
  alter column variety_id set not null,
  alter column grade_id set not null;

create index if not exists idx_price_cache_variety_id on public.price_cache(variety_id);
create index if not exists idx_price_cache_grade_id on public.price_cache(grade_id);
