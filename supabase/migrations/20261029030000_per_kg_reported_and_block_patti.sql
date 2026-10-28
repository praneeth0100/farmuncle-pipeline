-- 0. raw_price_entries is append-only (trigger). Allow deletes only inside a transaction that opts in
--    with app.allow_raw_purge, which is what purge_raw_price_entries() does. Ad hoc deletes stay blocked.
create or replace function trg_raw_price_entries_no_delete_fn() returns trigger language plpgsql as $$
begin
  if current_setting('app.allow_raw_purge', true) = 'on' then
    return old;
  end if;
  raise exception 'raw_price_entries rows are immutable -- delete is not permitted';
end;
$$;

create or replace function purge_raw_price_entries(p_keep_days int default 10)
returns bigint language plpgsql as $$
declare n bigint;
begin
  perform set_config('app.allow_raw_purge', 'on', true);
  delete from raw_price_entries where last_seen_at < now() - make_interval(days => p_keep_days);
  get diagnostics n = row_count;
  return n;
end $$;

-- 1. Patti APMC (Punjab) sends placeholder prices (0.01, 1) for everything: remove it for good.
--    The pipeline drops this market at parse time (BLOCKED_MARKETS), so it cannot come back.
delete from mandi_duplicate_review_queue where mandi_id_a in (select id from mandis where name = 'Patti APMC' and state = 'Punjab')
   or mandi_id_b in (select id from mandis where name = 'Patti APMC' and state = 'Punjab');
delete from mandi_daily_prices where mandi_id in (select id from mandis where name = 'Patti APMC' and state = 'Punjab')
   or source_mandi_id in (select id from mandis where name = 'Patti APMC' and state = 'Punjab');
delete from price_cache where mandi_id in (select id from mandis where name = 'Patti APMC' and state = 'Punjab');
select set_config('app.allow_raw_purge', 'on', true);
delete from raw_price_entries where market = 'Patti APMC' and state = 'Punjab';
delete from data_quality_issues where row_data->>'source_mandi_name' = 'Patti APMC';
update mandis set merged_into_id = null where merged_into_id in (select id from mandis where name = 'Patti APMC' and state = 'Punjab');
delete from mandis where name = 'Patti APMC' and state = 'Punjab';

-- 2. A government price under Rs 100 per quintal is a per-kg entry: the pipeline now keeps it as is.
--    Re-store the quarantined sub-Rs-1 rows that way (reported value = stored x 100, must be >= Rs 1).
with cand as (
  select q.id qid, d.*
    from data_quality_issues q
    cross join lateral jsonb_populate_record(null::mandi_daily_prices, q.row_data) d
   where q.error_code = 'DQ-PRICE-FLOOR' and q.review_status = 'PENDING'
     and d.unit = 'kg' and d.modal_price * 100 >= 1
), ins as (
  insert into mandi_daily_prices
    (mandi_id, crop_id, variety, grade, variety_id, grade_id, price_date, modal_price, min_price, max_price,
     unit, source, source_mandi_id, source_mandi_name, quality_score, quality_components,
     parser_version, normalization_version, batch_id, raw_api_batch_id)
  select mandi_id, crop_id, variety, grade, variety_id, grade_id, price_date,
         round(modal_price * 100, 2), round(min_price * 100, 2), round(max_price * 100, 2),
         unit, source, source_mandi_id, source_mandi_name, quality_score,
         coalesce(quality_components, '{}'::jsonb) || '{"unit_reported_kg": true}'::jsonb,
         parser_version, normalization_version, batch_id, raw_api_batch_id
    from cand
  on conflict do nothing
  returning mandi_id, crop_id, variety, grade, price_date, source_mandi_id
)
update data_quality_issues q
   set review_status = 'CORRECTED', reviewed_at = now(), review_note = 'per-kg entry kept as reported (unit_reported_kg)'
  from cand c join ins i using (mandi_id, crop_id, variety, grade, price_date, source_mandi_id)
 where q.id = c.qid;

-- rows restored by the earlier median-based rescue get the same flag
update mandi_daily_prices
   set quality_components = quality_components || '{"unit_reported_kg": true}'::jsonb
 where quality_components->>'scale_corrected' = 'true';

-- 3. The median helper is no longer used.
drop function if exists crop_price_medians(int);
