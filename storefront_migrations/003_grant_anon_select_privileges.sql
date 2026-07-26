-- RLS policies control row-level access, but the app's anon key still needs the
-- underlying table-level GRANT to read anything at all — this was missing too.

grant usage on schema public to anon;

grant select on public.crops to anon;
grant select on public.mandis to anon;
grant select on public.varieties to anon;
grant select on public.grades to anon;
grant select on public.price_cache to anon;
