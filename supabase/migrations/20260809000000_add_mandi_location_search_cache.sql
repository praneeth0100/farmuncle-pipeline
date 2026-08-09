-- Location search cache for the v2 (Places API based) mandi geocoder.
-- See farmuncle_pipeline/geocoding/cache.py for usage. No TTL/expiry in
-- this first version -- a market's physical location doesn't go stale;
-- clear rows manually if a query ever needs a forced re-fetch.

create table if not exists public.mandi_location_search_cache (
    id bigint generated always as identity primary key,
    provider text not null,
    query text not null,
    response jsonb not null,
    created_at timestamptz not null default now(),
    unique (provider, query)
);

comment on table public.mandi_location_search_cache is
    'Caches raw provider (google_places/osm_nominatim) search responses '
    'by exact query string, so repeated dev/test runs of the geocoder '
    'don''t re-spend API credits on identical queries. Not a source of '
    'truth for mandi coordinates -- mandis.latitude/longitude is.';

-- service_role already gets table grants via the existing
-- `alter default privileges ... grant ... on tables` in
-- 20260725161000_grant_service_role_privileges.sql, so no explicit
-- grant needed here (matches this migration's own convention).
