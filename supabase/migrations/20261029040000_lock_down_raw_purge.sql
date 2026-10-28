-- Only the service role may purge raw entries, and never "everything older than now" by accident.
create or replace function purge_raw_price_entries(p_keep_days int default 10)
returns bigint language plpgsql as $$
declare n bigint;
begin
  if p_keep_days < 1 then
    raise exception 'p_keep_days must be at least 1 (got %)', p_keep_days;
  end if;
  perform set_config('app.allow_raw_purge', 'on', true);
  delete from raw_price_entries where last_seen_at < now() - make_interval(days => p_keep_days);
  get diagnostics n = row_count;
  return n;
end $$;

revoke execute on function purge_raw_price_entries(int) from public, anon, authenticated;
grant execute on function purge_raw_price_entries(int) to service_role;
