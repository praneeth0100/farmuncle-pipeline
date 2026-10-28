-- Which market+crop pairs report Rs/kg instead of Rs/quintal, learned from history so it works
-- for every market. A pair qualifies when, on at least 3 days, >= 80% of its prices (as the
-- government sent them) are between Rs 1 and Rs 100. Quarantined sub-Rs-1 rows count as evidence.
create or replace function per_kg_pairs(p_min_days int default 3, p_min_share numeric default 0.8)
returns table (mandi_id bigint, crop_id bigint) language sql stable as $$
  with obs as (
    select p.mandi_id, p.crop_id, p.price_date,
           p.modal_price * case when coalesce(p.quality_components->>'unit_reported_kg', '') = 'true' then 1 else 100 end as raw
      from mandi_daily_prices p
     where p.unit = 'kg' and p.price_date >= current_date - 180
    union all
    select (q.row_data->>'mandi_id')::bigint, (q.row_data->>'crop_id')::bigint,
           (q.row_data->>'price_date')::date, (q.row_data->>'modal_price')::numeric * 100
      from data_quality_issues q
     where q.error_code = 'DQ-PRICE-FLOOR' and q.review_status = 'PENDING' and q.row_data->>'unit' = 'kg'
  )
  select o.mandi_id, o.crop_id
    from obs o
   group by o.mandi_id, o.crop_id
  having count(distinct o.price_date) >= p_min_days
     and avg((o.raw >= 1 and o.raw < 100)::int) >= p_min_share
$$;

revoke execute on function per_kg_pairs(int, numeric) from public, anon, authenticated;
grant execute on function per_kg_pairs(int, numeric) to service_role;

-- Rows kept as per-kg for pairs that are NOT learned go back to quarantine.
with doomed as (
  select p.id from mandi_daily_prices p
   where p.quality_components->>'unit_reported_kg' = 'true'
     and not exists (select 1 from per_kg_pairs() k where k.mandi_id = p.mandi_id and k.crop_id = p.crop_id)
), gone as (
  delete from mandi_daily_prices where id in (select id from doomed)
  returning mandi_id, crop_id, variety, grade, price_date, source_mandi_id
)
update data_quality_issues q
   set review_status = 'PENDING', reviewed_at = null, review_note = 'not a learned per-kg pair: back in quarantine'
 where q.review_status = 'CORRECTED' and q.error_code = 'DQ-PRICE-FLOOR'
   and exists (select 1 from gone g
                where g.mandi_id = (q.row_data->>'mandi_id')::bigint and g.crop_id = (q.row_data->>'crop_id')::bigint
                  and g.price_date = (q.row_data->>'price_date')::date and g.variety = q.row_data->>'variety'
                  and coalesce(g.grade, '') = coalesce(q.row_data->>'grade', '')
                  and g.source_mandi_id = (q.row_data->>'source_mandi_id')::bigint);

-- Quarantined sub-Rs-1 rows that belong to a learned pair are per-kg prices: store them as reported.
with cand as (
  select q.id qid, d.*
    from data_quality_issues q
    cross join lateral jsonb_populate_record(null::mandi_daily_prices, q.row_data) d
    join per_kg_pairs() k on k.mandi_id = d.mandi_id and k.crop_id = d.crop_id
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
   set review_status = 'CORRECTED', reviewed_at = now(), review_note = 'learned per-kg pair: stored as reported'
  from cand c join ins i using (mandi_id, crop_id, variety, grade, price_date, source_mandi_id)
 where q.id = c.qid;
