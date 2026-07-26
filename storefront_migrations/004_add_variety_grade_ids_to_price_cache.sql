-- Mirrors 20260727010000_add_variety_grade_ids_to_price_cache.sql (warehouse side).
-- price_cache.variety/grade were text-only here too — this adds the same real
-- foreign keys. sync_storefront.py (updated separately) now populates these on
-- every sync via a full delete+reinsert, so no manual backfill needed here —
-- just run the sync once after this migration.

alter table public.price_cache
  add column if not exists variety_id bigint references public.varieties(id),
  add column if not exists grade_id bigint references public.grades(id);

create index if not exists idx_price_cache_variety_id on public.price_cache(variety_id);
create index if not exists idx_price_cache_grade_id on public.price_cache(grade_id);

grant select, insert, update, delete on public.price_cache to service_role;
