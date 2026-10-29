-- Lets the day-by-day lookups (precedence check, day export) use the date and id together.
-- Without it the planner walked the primary key and read ~240k rows per 1,000-row page (2 s each).
create index if not exists idx_mandi_daily_prices_date_id on mandi_daily_prices (price_date, id);
