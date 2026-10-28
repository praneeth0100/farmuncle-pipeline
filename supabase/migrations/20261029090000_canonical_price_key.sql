-- One price per (mandi, crop, variety, grade, day), by canonical ids. The old key
-- (mandi, crop, variety text, grade text, day, source_mandi_id) let the same price in twice when two
-- source markets were merged into one mandi, or when text differed only by case.
-- uq_prices_business_key_v2 stays until the new writer is deployed, then is dropped.

-- 1. Remove any duplicates that appeared since the last cleanup (logged, keeping the mandi's own row).
with ranked as (
  select id, row_number() over (
           partition by mandi_id, crop_id, variety_id, grade_id, price_date
           order by case source when 'manual' then 3 when 'resource_2' then 2 when 'resource_1' then 1 else 0 end desc,
                     (source_mandi_id = mandi_id) desc, updated_at desc, id desc) rn
    from mandi_daily_prices
), doomed as (
  select p.* from mandi_daily_prices p join ranked r on r.id = p.id where r.rn > 1
), logged as (
  insert into data_quality_issues (batch_id, resource, row_data, error_code, error_message)
  select batch_id, source, to_jsonb(d), 'DQ-DUPLICATE', 'same mandi/crop/variety/grade/date reported twice' from doomed d
  returning 1
)
delete from mandi_daily_prices where id in (select id from doomed);

-- 2. The ids are the key, so they must exist.
alter table mandi_daily_prices alter column variety_id set not null, alter column grade_id set not null;

-- 3. The new key.
alter table mandi_daily_prices
  add constraint uq_prices_canonical_key unique (mandi_id, crop_id, variety_id, grade_id, price_date);

-- 4. The nightly audit watches the new key (same columns as before).
create or replace view v_audit_business_key_constraint as
  select exists (select 1 from pg_constraint where conname = 'uq_prices_canonical_key' and contype = 'u') as constraint_present;

create or replace view v_audit_duplicate_price_keys as
  select mandi_id, crop_id, min(variety) as variety, min(grade) as grade, price_date,
         min(source_mandi_id) as source_mandi_id, count(*) as row_count
    from mandi_daily_prices
   group by mandi_id, crop_id, variety_id, grade_id, price_date
  having count(*) > 1;
