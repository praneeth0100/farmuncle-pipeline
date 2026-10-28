-- Reference prices for the under-Rs-1/kg rule, and recovery of the rows the first
-- audit quarantined by a blunt floor. A per-kg entry typed into a per-quintal field
-- arrives ~100x too small; if x100 lands within 0.3-3x of the crop's median it is a
-- scale slip and is stored corrected (flagged), otherwise it stays quarantined.

create or replace function crop_price_medians(p_min_rows int default 30)
returns table (crop_id bigint, median numeric) language sql stable as $$
  select crop_id, percentile_cont(0.5) within group (order by modal_price)::numeric
    from mandi_daily_prices
   where unit = 'kg' and modal_price >= 1
   group by crop_id
  having count(*) >= p_min_rows
$$;

with cand as (
  select q.id qid, d.*
    from data_quality_issues q
    cross join lateral jsonb_populate_record(null::mandi_daily_prices, q.row_data) d
    join crop_price_medians() m on m.crop_id = d.crop_id
   where q.error_code = 'DQ-PRICE-FLOOR' and q.review_status = 'PENDING'
     and d.unit = 'kg' and d.modal_price > 0
     and d.modal_price * 100 >= 1
     and d.modal_price * 100 / m.median between 0.3 and 3
), ins as (
  insert into mandi_daily_prices
    (mandi_id, crop_id, variety, grade, variety_id, grade_id, price_date, modal_price, min_price, max_price,
     unit, source, source_mandi_id, source_mandi_name, quality_score, quality_components,
     parser_version, normalization_version, batch_id, raw_api_batch_id)
  select mandi_id, crop_id, variety, grade, variety_id, grade_id, price_date,
         round(modal_price * 100, 2), round(min_price * 100, 2), round(max_price * 100, 2),
         unit, source, source_mandi_id, source_mandi_name, quality_score,
         coalesce(quality_components, '{}'::jsonb) || '{"scale_corrected": true}'::jsonb,
         parser_version, normalization_version, batch_id, raw_api_batch_id
    from cand
  on conflict do nothing
  returning mandi_id, crop_id, variety, grade, price_date, source_mandi_id
)
update data_quality_issues q
   set review_status = 'CORRECTED', reviewed_at = now(), review_note = 'rescaled x100 (scale_corrected)'
  from cand c join ins i using (mandi_id, crop_id, variety, grade, price_date, source_mandi_id)
 where q.id = c.qid;
