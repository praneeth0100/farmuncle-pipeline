-- This storefront project was missing standard service_role table grants —
-- sync_storefront.py's upserts failed with "permission denied for table crops"
-- until this was applied. RLS policies alone are not enough; the underlying
-- GRANT is a separate layer in Postgres's permission model.

grant usage on schema public to service_role;

grant select, insert, update, delete on public.crops to service_role;
grant select, insert, update, delete on public.mandis to service_role;
grant select, insert, update, delete on public.varieties to service_role;
grant select, insert, update, delete on public.grades to service_role;
grant select, insert, update, delete on public.price_cache to service_role;

grant usage on all sequences in schema public to service_role;
