-- crop_price_medians ran live_crop_medians() twice per call (once for the rows, once inside NOT EXISTS),
-- each a scan of 60 days of prices. It now runs once. Same result.
create or replace function crop_price_medians(p_days int default 60, p_min_rows int default 30)
returns table (crop_id bigint, median numeric) language sql stable as $$
  with l as materialized (select * from live_crop_medians(p_days, p_min_rows))
  select l.crop_id, l.median from l
  union all
  select r.crop_id, r.median from crop_price_reference r
   where not exists (select 1 from l where l.crop_id = r.crop_id)
$$;
