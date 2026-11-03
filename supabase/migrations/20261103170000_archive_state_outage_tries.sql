-- How many times the backfill redid its current window because every day in it failed (an outage).
-- After two redos the window's days are recorded FAILED and the backfill moves on.
alter table archive_state add column if not exists outage_tries integer not null default 0;
