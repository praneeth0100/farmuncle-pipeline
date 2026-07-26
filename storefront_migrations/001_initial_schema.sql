-- FarmUncle Storefront Schema — separate Supabase account from the warehouse.
-- Applied directly against project ref xpkrjhcqqsoqsmcsaiar ("farmuncle main").
-- This is the read-only serving layer the app connects to.

create table if not exists public.crops (
  id bigint primary key,
  name text not null,
  category text,
  unit text,
  updated_at timestamptz not null default now()
);

create table if not exists public.mandis (
  id bigint primary key,
  slug text unique not null,
  name text not null,
  state text not null,
  district text not null,
  taluk text,
  latitude double precision,
  longitude double precision,
  location_confidence text,
  updated_at timestamptz not null default now()
);

create table if not exists public.varieties (
  id bigint primary key,
  crop_id bigint not null references public.crops(id),
  name text not null,
  updated_at timestamptz not null default now()
);

create table if not exists public.grades (
  id bigint primary key,
  variety_id bigint not null references public.varieties(id),
  name text not null,
  updated_at timestamptz not null default now()
);

create table if not exists public.price_cache (
  mandi_id bigint not null references public.mandis(id),
  crop_id bigint not null references public.crops(id),
  variety text not null default '',
  grade text not null default '',
  latest_price_date date,
  modal_price numeric,
  min_price numeric,
  max_price numeric,
  source_mandi_name text,
  change_1d numeric,
  change_1d_pct numeric,
  change_7d numeric,
  change_7d_pct numeric,
  change_1m numeric,
  change_1m_pct numeric,
  refreshed_at timestamptz not null default now(),
  primary key (mandi_id, crop_id, variety, grade)
);

create index if not exists idx_price_cache_crop on public.price_cache(crop_id);
create index if not exists idx_price_cache_mandi on public.price_cache(mandi_id);
create index if not exists idx_mandis_state_district on public.mandis(state, district);
create index if not exists idx_varieties_crop on public.varieties(crop_id);
create index if not exists idx_grades_variety on public.grades(variety_id);

-- Public read-only access (app uses anon key)
alter table public.crops enable row level security;
alter table public.mandis enable row level security;
alter table public.varieties enable row level security;
alter table public.grades enable row level security;
alter table public.price_cache enable row level security;

create policy "public read" on public.crops for select using (true);
create policy "public read" on public.mandis for select using (true);
create policy "public read" on public.varieties for select using (true);
create policy "public read" on public.grades for select using (true);
create policy "public read" on public.price_cache for select using (true);
