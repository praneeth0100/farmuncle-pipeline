-- Once a day is FINAL (Resource 2 has replaced the hourly Resource 1 pulls), the Resource 1 raw observations
-- for that day are only wasted space: the facts are in mandi_daily_prices and then in R2.
create or replace function purge_superseded_live_raw()
returns bigint language plpgsql as $$
declare n bigint;
begin
  perform set_config('app.allow_raw_purge', 'on', true);
  delete from raw_price_entries r
   using ingest_days i
   where r.resource = 'resource_1' and i.price_date = r.price_date and i.status = 'FINAL';
  get diagnostics n = row_count;
  return n;
end $$;

revoke execute on function purge_superseded_live_raw() from public, anon, authenticated;
grant execute on function purge_superseded_live_raw() to service_role;
