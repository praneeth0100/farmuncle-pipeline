-- peer_scale is x100 only, matching 0.3-3x of the crop median. Undo the x1000 guesses and re-run x100
-- with the wider band (e.g. Gur(Jaggery) 1.0 -> 100 against a median of 43 is a real price).

-- 1. x1000 rows go back to quarantine.
with gone as (
  delete from mandi_daily_prices where quality_components->>'peer_scaled' = '1000'
  returning mandi_id, crop_id, variety, grade, price_date, source_mandi_id
)
update data_quality_issues q
   set review_status = 'PENDING', reviewed_at = null, review_note = 'peer_scaled x1000 withdrawn'
 where q.review_status = 'CORRECTED' and q.review_note like 'peer_scaled%'
   and exists (select 1 from gone g
                where g.mandi_id = (q.row_data->>'mandi_id')::bigint and g.crop_id = (q.row_data->>'crop_id')::bigint
                  and g.price_date = (q.row_data->>'price_date')::date and g.variety = q.row_data->>'variety'
                  and coalesce(g.grade, '') = coalesce(q.row_data->>'grade', '')
                  and g.source_mandi_id = (q.row_data->>'source_mandi_id')::bigint);

-- 2. stored rows a clean x100 brings to 0.3-3x of the crop median.
with med as (select crop_id, median from crop_price_medians(180, 30))
update mandi_daily_prices p
   set modal_price = round(p.modal_price * 100, 2), min_price = round(p.min_price * 100, 2),
       max_price = round(p.max_price * 100, 2),
       quality_components = coalesce(p.quality_components, '{}'::jsonb) || '{"peer_scaled": 100}'::jsonb
  from med
 where med.crop_id = p.crop_id and p.unit = 'kg' and p.modal_price < med.median * 0.1
   and p.modal_price * 100 between med.median * 0.3 and med.median * 3
   and coalesce(p.quality_components->>'unit_reported_kg', '') <> 'true'
   and p.quality_components->>'peer_scaled' is null;

-- 3. quarantined sub-Rs-1 rows the same rule fixes.
with med as (select crop_id, median from crop_price_medians(180, 30)),
cand as (
  select q.id qid, d.*
    from data_quality_issues q
    cross join lateral jsonb_populate_record(null::mandi_daily_prices, q.row_data) d
    join med on med.crop_id = d.crop_id
   where q.error_code = 'DQ-PRICE-FLOOR' and q.review_status = 'PENDING'
     and d.unit = 'kg' and d.modal_price > 0 and d.modal_price < med.median * 0.1
     and d.modal_price * 100 between med.median * 0.3 and med.median * 3 and d.modal_price * 100 >= 1
), ins as (
  insert into mandi_daily_prices
    (mandi_id, crop_id, variety, grade, variety_id, grade_id, price_date, modal_price, min_price, max_price,
     unit, source, source_mandi_id, source_mandi_name, quality_score, quality_components,
     parser_version, normalization_version, batch_id, raw_api_batch_id)
  select mandi_id, crop_id, variety, grade, variety_id, grade_id, price_date,
         round(modal_price * 100, 2), round(min_price * 100, 2), round(max_price * 100, 2),
         unit, source, source_mandi_id, source_mandi_name, quality_score,
         coalesce(quality_components, '{}'::jsonb) || '{"peer_scaled": 100}'::jsonb,
         parser_version, normalization_version, batch_id, raw_api_batch_id
    from cand
  on conflict do nothing
  returning mandi_id, crop_id, variety, grade, price_date, source_mandi_id
)
update data_quality_issues q
   set review_status = 'CORRECTED', reviewed_at = now(), review_note = 'peer_scaled: multiplied back to match the crop'
  from cand c join ins i using (mandi_id, crop_id, variety, grade, price_date, source_mandi_id)
 where q.id = c.qid;
