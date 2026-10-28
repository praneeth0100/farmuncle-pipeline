-- Progress of the backward history fill (today -> 2018 in 10-day windows) and the list of days that
-- have aged out of the warehouse's live window and can move to R2.
create table if not exists archive_state (
  key text primary key,
  next_end date,
  done boolean not null default false,
  windows_done int not null default 0,
  updated_at timestamptz not null default now()
);
alter table archive_state enable row level security;

-- Days older than the live window that are final (or untracked, or past the give-up age).
create or replace function aged_price_dates(p_keep_days int default 11)
returns setof date language sql stable as $$
  select d.price_date
    from (select distinct price_date from mandi_daily_prices where price_date <= current_date - p_keep_days) d
    left join ingest_days i on i.price_date = d.price_date
   where i.price_date is null or i.status = 'FINAL' or d.price_date <= current_date - 14
   order by d.price_date
$$;

revoke execute on function aged_price_dates(int) from public, anon, authenticated;
grant execute on function aged_price_dates(int) to service_role;
