-- The warehouse keeps the last 3 days for the app (was 10); older days live in R2.
-- Keep LIVE_DAYS in farmuncle_pipeline/archive/history.py equal to the number here.

create or replace function public.delete_price_window(p_start date, p_end date)
 returns jsonb
 language plpgsql
as $function$
declare n_prices bigint; n_raw bigint; n_dq bigint;
begin
  if p_end < p_start then raise exception 'delete_price_window: end % is before start %', p_end, p_start; end if;
  if p_end > current_date - 3 then
    raise exception 'delete_price_window: refusing to delete inside the live window (end % is within the last 3 days)', p_end;
  end if;
  perform set_config('app.allow_raw_purge', 'on', true);
  delete from mandi_daily_prices where price_date between p_start and p_end;
  get diagnostics n_prices = row_count;
  delete from raw_price_entries where price_date between p_start and p_end;
  get diagnostics n_raw = row_count;
  delete from data_quality_issues where row_data ? 'price_date' and (row_data->>'price_date')::date between p_start and p_end;
  get diagnostics n_dq = row_count;
  return jsonb_build_object('prices', n_prices, 'raw', n_raw, 'quarantine', n_dq);
end $function$;

create or replace function public.aged_price_dates(p_keep_days integer default 4)
 returns setof date
 language sql
 stable
as $function$
  select d.price_date
    from (select distinct price_date from mandi_daily_prices where price_date <= current_date - p_keep_days) d
    left join ingest_days i on i.price_date = d.price_date
   where i.price_date is null or i.status = 'FINAL' or d.price_date <= current_date - 14
   order by d.price_date
$function$;
