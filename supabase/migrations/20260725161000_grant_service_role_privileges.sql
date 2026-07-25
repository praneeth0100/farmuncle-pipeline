-- The exact same "service_role had zero grants" gotcha that was hit and fixed on
-- the old project (2026-07-24) recurred here because a fresh Supabase project
-- doesn't grant service_role anything by default either. First real live_tick run
-- against this new project failed immediately: "permission denied for table
-- system_config" -- SQL-editor testing never catches this because the editor runs
-- as superuser, bypassing grants entirely. Only a real PostgREST/service_role call
-- (like live_tick actually running) surfaces it.

grant usage on schema public to service_role;
grant all privileges on all tables in schema public to service_role;
grant all privileges on all sequences in schema public to service_role;
grant execute on all functions in schema public to service_role;
alter default privileges in schema public grant all privileges on tables to service_role;
alter default privileges in schema public grant all privileges on sequences to service_role;
alter default privileges in schema public grant execute on functions to service_role;
