-- Phase 2.1: one row per calendar date, so daily_rewrite can retry every date
-- until Resource 2 (the final feed) has delivered it completely, instead of
-- re-checking a fixed 3-day window (ADR 0003).
--
-- PENDING: not yet proven complete (not published, partial, or failed).
-- FINAL:   complete, or given up on (see core/ingest_days.py `decide`).
--
-- Apply BEFORE enabling daily_rewrite: the startup validator requires the table.
-- RLS on with no policies, like every other warehouse table: only the service key reads it.

create table if not exists public.ingest_days (
    price_date       date primary key,
    status           text not null default 'PENDING' check (status in ('PENDING', 'FINAL')),
    attempts         int not null default 0,
    last_attempt_at  timestamptz,
    last_error       text,
    v2_rows          int,
    final_at         timestamptz,
    first_seen_at    timestamptz not null default now()
);

create index if not exists idx_ingest_days_status_date on public.ingest_days (status, price_date);

alter table public.ingest_days enable row level security;

-- service_role already gets table grants through the default privileges set in
-- 20260725161000_grant_service_role_privileges.sql.
