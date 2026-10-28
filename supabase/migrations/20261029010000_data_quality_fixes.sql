-- Data quality fixes found by the 2026-10-28 audit.
-- Every removed row is first copied into data_quality_issues, so nothing is lost.

-- 1. Same mandi/crop/variety/grade/date reported twice (differing only in source_mandi_id).
--    Keep the row from the mandi's own id, then the newest.
with ranked as (
  select id, row_number() over (
           partition by mandi_id, crop_id, variety_id, grade_id, price_date
           order by (source_mandi_id = mandi_id) desc, updated_at desc, id desc) rn
  from mandi_daily_prices
), doomed as (
  select p.* from mandi_daily_prices p join ranked r on r.id = p.id where r.rn > 1
), logged as (
  insert into data_quality_issues (batch_id, resource, row_data, error_code, error_message)
  select batch_id, source, to_jsonb(d), 'DQ-DUPLICATE', 'same mandi/crop/variety/grade/date reported twice' from doomed d
  returning 1
)
delete from mandi_daily_prices where id in (select id from doomed);

-- 2. Livestock stored per kg by mistake: put them back to per animal (they were divided by 100).
update crops set category = 'Livestock' where id in (320, 321) and category is null;
update mandi_daily_prices
   set modal_price = modal_price * 100, min_price = min_price * 100, max_price = max_price * 100, unit = 'animal'
 where crop_id in (320, 321) and unit = 'kg';

-- 3. Prices that cannot be real: zero/negative, or under Rs 1/kg.
with doomed as (
  select * from mandi_daily_prices
   where modal_price <= 0 or (unit = 'kg' and modal_price < 1)
), logged as (
  insert into data_quality_issues (batch_id, resource, row_data, error_code, error_message)
  select batch_id, source, to_jsonb(d),
         case when modal_price <= 0 then 'DQ-PRICE-NONPOSITIVE' else 'DQ-PRICE-FLOOR' end,
         'removed by 2026-10-29 audit: modal_price=' || modal_price
    from doomed d
  returning 1
)
delete from mandi_daily_prices where id in (select id from doomed);

-- 3b. Modal price outside its own min..max range (government entry errors).
with doomed as (
  select * from mandi_daily_prices
   where min_price > modal_price or modal_price > max_price or min_price > max_price
), logged as (
  insert into data_quality_issues (batch_id, resource, row_data, error_code, error_message)
  select batch_id, source, to_jsonb(d), 'DQ-PRICE-ORDER', 'min/modal/max out of order'
    from doomed d
  returning 1
)
delete from mandi_daily_prices where id in (select id from doomed);

-- 4. Constraints so they cannot come back (the writer quarantines on 23514).
alter table mandi_daily_prices
  add constraint chk_prices_min_max
    check (min_price is null or max_price is null or modal_price is null
           or (min_price <= modal_price and modal_price <= max_price)),
  add constraint chk_prices_sane
    check (modal_price is null or (modal_price > 0 and (unit <> 'kg' or modal_price >= 1)));

-- 5. Two identical indexes and one that is a prefix of them: pure space waste.
drop index if exists idx_prices_cache_lookup;
drop index if exists idx_prices_mandi_crop;

-- 6. Raw retention: keep N days of raw observations (call from a scheduled job).
create or replace function purge_raw_price_entries(p_keep_days int default 10)
returns bigint language plpgsql as $$
declare n bigint;
begin
  delete from raw_price_entries where last_seen_at < now() - make_interval(days => p_keep_days);
  get diagnostics n = row_count;
  return n;
end $$;
