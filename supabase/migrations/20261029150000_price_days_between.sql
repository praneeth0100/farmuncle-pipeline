-- Days that still have prices in the warehouse inside a range. The history job uses it to retry
-- days it skipped earlier (they stay in the warehouse until they are archived).
create or replace function price_days_between(p_from date, p_to date) returns setof date
language sql stable as $$
    select distinct price_date from mandi_daily_prices
    where price_date between p_from and p_to
    order by price_date;
$$;

revoke execute on function price_days_between(date, date) from public, anon, authenticated;
grant execute on function price_days_between(date, date) to service_role;
