-- Reference prices for peer_scale(): median Rs/kg per crop over recent days, all markets.
create or replace function crop_price_medians(p_days int default 60, p_min_rows int default 30)
returns table (crop_id bigint, median numeric) language sql stable as $$
  select p.crop_id, percentile_cont(0.5) within group (order by p.modal_price)::numeric
    from mandi_daily_prices p
   where p.unit = 'kg' and p.modal_price >= 1 and p.price_date >= current_date - p_days
   group by p.crop_id
  having count(*) >= p_min_rows
$$;
revoke execute on function crop_price_medians(int, int) from public, anon, authenticated;
grant execute on function crop_price_medians(int, int) to service_role;

-- Apply the same rule to rows already stored: a price under a tenth of the crop median that a
-- clean x100 / x1000 brings next to that median (0.4-2x) is multiplied back and flagged.
with med as (select crop_id, median from crop_price_medians(180, 30)),
fix as (
  select p.id, f.factor
    from mandi_daily_prices p
    join med using (crop_id)
    cross join lateral (
      select x.factor from (values (100), (1000)) x(factor)
       where p.modal_price * x.factor between med.median * 0.4 and med.median * 2
       order by abs(ln(p.modal_price * x.factor / med.median)) limit 1
    ) f
   where p.unit = 'kg' and p.modal_price < med.median * 0.1
     and coalesce(p.quality_components->>'unit_reported_kg', '') <> 'true'
     and p.quality_components->>'peer_scaled' is null
)
update mandi_daily_prices p
   set modal_price = round(p.modal_price * fix.factor, 2),
       min_price = round(p.min_price * fix.factor, 2),
       max_price = round(p.max_price * fix.factor, 2),
       quality_components = coalesce(p.quality_components, '{}'::jsonb) || jsonb_build_object('peer_scaled', fix.factor)
  from fix where p.id = fix.id;

-- Quarantined sub-Rs-1 rows that the same rule fixes are stored multiplied back.
with med as (select crop_id, median from crop_price_medians(180, 30)),
cand as (
  select q.id qid, d.*, f.factor
    from data_quality_issues q
    cross join lateral jsonb_populate_record(null::mandi_daily_prices, q.row_data) d
    join med on med.crop_id = d.crop_id
    cross join lateral (
      select x.factor from (values (100), (1000)) x(factor)
       where d.modal_price * x.factor between med.median * 0.4 and med.median * 2
       order by abs(ln(d.modal_price * x.factor / med.median)) limit 1
    ) f
   where q.error_code = 'DQ-PRICE-FLOOR' and q.review_status = 'PENDING'
     and d.unit = 'kg' and d.modal_price > 0 and d.modal_price < med.median * 0.1
     and d.modal_price * f.factor >= 1
), ins as (
  insert into mandi_daily_prices
    (mandi_id, crop_id, variety, grade, variety_id, grade_id, price_date, modal_price, min_price, max_price,
     unit, source, source_mandi_id, source_mandi_name, quality_score, quality_components,
     parser_version, normalization_version, batch_id, raw_api_batch_id)
  select mandi_id, crop_id, variety, grade, variety_id, grade_id, price_date,
         round(modal_price * factor, 2), round(min_price * factor, 2), round(max_price * factor, 2),
         unit, source, source_mandi_id, source_mandi_name, quality_score,
         coalesce(quality_components, '{}'::jsonb) || jsonb_build_object('peer_scaled', factor),
         parser_version, normalization_version, batch_id, raw_api_batch_id
    from cand
  on conflict do nothing
  returning mandi_id, crop_id, variety, grade, price_date, source_mandi_id
)
update data_quality_issues q
   set review_status = 'CORRECTED', reviewed_at = now(), review_note = 'peer_scaled: multiplied back to match the crop'
  from cand c join ins i using (mandi_id, crop_id, variety, grade, price_date, source_mandi_id)
 where q.id = c.qid;
