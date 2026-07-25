-- FarmUncle v2 rebuild, 2026-07-25, on new project (flxjrcbhmcuaynctokpv) after the
-- old one (ltradoxvyxwszcoqiirk) died from a disk-full crash loop on the Free plan.
--
-- CORRECTED THIS TIME vs the old project:
--   1. variety/grade are properly crop-scoped from day one: crop -> variety -> grade.
--      ("Other" alone meant 240 different things depending on the crop -- proven live
--      on 2026-07-25 against the old project's real data before it died.)
--   2. mandi_daily_prices carries source_mandi_id/source_mandi_name AND the widened
--      6-column business key from the very first row, not backfilled after 468k rows
--      already existed without it.
--   3. price_cache's change_1d/7d/1m columns exist and are actually computed from day
--      one (see refresh_price_cache in migration 004).

create table mandis (
    id bigint generated always as identity primary key,
    slug text not null unique,
    name text not null,
    normalized_name text not null,
    state text not null,
    district text not null,
    taluk text,
    latitude double precision,
    longitude double precision,
    location_confidence text check (location_confidence in ('EXACT','APMC','DISTRICT','STATE','UNKNOWN')),
    status text not null default 'ACTIVE' check (status in ('ACTIVE','MERGED','INACTIVE','UNKNOWN')),
    review_status text not null default 'AUTO_CREATED' check (review_status in ('AUTO_CREATED','NEEDS_REVIEW','VERIFIED','REJECTED')),
    merged_into_id bigint references mandis(id),
    merge_reason text,
    merge_method text check (merge_method in ('EXACT','NORMALIZED','FUZZY','MANUAL')),
    merge_confidence numeric,
    merged_at timestamptz,
    ingested_from text,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    last_verified_at timestamptz,
    created_at timestamptz not null default now()
);
create unique index uq_mandis_business_key on mandis(normalized_name, state, district);
create index idx_mandis_status on mandis(status);
create index idx_mandis_review_status on mandis(review_status);
create index idx_mandis_last_seen_at on mandis(last_seen_at);

create table crops (
    id bigint generated always as identity primary key,
    name text not null,
    normalized_name text not null unique,
    category text,
    unit text,
    taxonomy_version int not null default 1,
    status text not null default 'ACTIVE' check (status in ('ACTIVE','MERGED','INACTIVE','UNKNOWN')),
    review_status text not null default 'AUTO_CREATED' check (review_status in ('AUTO_CREATED','NEEDS_REVIEW','VERIFIED','REJECTED')),
    merged_into_id bigint references crops(id),
    merge_reason text,
    merge_method text check (merge_method in ('EXACT','NORMALIZED','FUZZY','MANUAL')),
    merge_confidence numeric,
    merged_at timestamptz,
    ingested_from text,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    last_verified_at timestamptz,
    created_at timestamptz not null default now()
);
create index idx_crops_status on crops(status);
create index idx_crops_review_status on crops(review_status);

create table mandi_aliases (
    id bigint generated always as identity primary key,
    mandi_id bigint not null references mandis(id) on delete cascade,
    alias_name text not null,
    normalized_alias text not null,
    match_method text check (match_method in ('EXACT','NORMALIZED','FUZZY','MANUAL')),
    match_confidence numeric,
    approved boolean not null default false,
    source text,
    normalization_version int not null default 1,
    created_at timestamptz not null default now(),
    unique (normalized_alias)
);
create index idx_mandi_aliases_mandi_id on mandi_aliases(mandi_id);

create table crop_aliases (
    id bigint generated always as identity primary key,
    crop_id bigint not null references crops(id) on delete cascade,
    alias_name text not null,
    normalized_alias text not null,
    match_method text check (match_method in ('EXACT','NORMALIZED','FUZZY','MANUAL')),
    match_confidence numeric,
    approved boolean not null default false,
    source text,
    normalization_version int not null default 1,
    created_at timestamptz not null default now(),
    unique (normalized_alias)
);
create index idx_crop_aliases_crop_id on crop_aliases(crop_id);

create table district_aliases (
    id bigint generated always as identity primary key,
    state text not null,
    alias_district text not null,
    canonical_district text not null,
    reason text,
    created_at timestamptz not null default now(),
    unique (state, alias_district)
);

-- variety/grade -- crop-scoped from day one (the correction)
create table varieties (
    id bigint generated always as identity primary key,
    crop_id bigint not null references crops(id),
    name text not null,
    normalized_name text not null,
    created_at timestamptz not null default now(),
    unique (crop_id, normalized_name)
);
create index idx_varieties_crop_id on varieties(crop_id);

create table variety_aliases (
    id bigint generated always as identity primary key,
    variety_id bigint not null references varieties(id),
    alias_name text not null,
    normalized_alias text not null,
    match_method text check (match_method in ('EXACT','NORMALIZED','FUZZY','MANUAL')),
    approved boolean not null default false,
    created_at timestamptz not null default now(),
    unique (variety_id, normalized_alias)
);

create table grades (
    id bigint generated always as identity primary key,
    variety_id bigint not null references varieties(id),
    name text not null,
    normalized_name text not null,
    created_at timestamptz not null default now(),
    unique (variety_id, normalized_name)
);
create index idx_grades_variety_id on grades(variety_id);

create table mandi_daily_prices (
    id bigint generated always as identity primary key,
    mandi_id bigint not null references mandis(id),
    crop_id bigint not null references crops(id),
    variety text,
    grade text,
    variety_id bigint references varieties(id),
    grade_id bigint references grades(id),
    price_date date not null,
    modal_price numeric,
    min_price numeric,
    max_price numeric,
    unit text,
    source text not null check (source in ('manual','resource_2','resource_1')),
    source_mandi_id bigint references mandis(id),
    source_mandi_name text,
    parser_version int,
    normalization_version int,
    quality_score numeric,
    quality_components jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
-- correction: 6-column key from day one so one merged city can hold prices from
-- more than one physical source market on the same day without a later ALTER.
create unique index uq_prices_business_key on mandi_daily_prices(mandi_id, crop_id, variety, grade, price_date, source_mandi_id);
create index idx_prices_date on mandi_daily_prices(price_date);
create index idx_prices_mandi_crop on mandi_daily_prices(mandi_id, crop_id);
create index idx_prices_variety_id on mandi_daily_prices(variety_id);
create index idx_prices_grade_id on mandi_daily_prices(grade_id);

create table suspicious_price_review (
    id bigint generated always as identity primary key,
    original_id bigint,
    mandi_id bigint,
    crop_id bigint,
    variety text,
    grade text,
    price_date date,
    min_price numeric,
    max_price numeric,
    modal_price numeric,
    unit text,
    reason text,
    flagged_at timestamptz default now(),
    reviewed boolean default false,
    corrected_min numeric,
    corrected_max numeric,
    corrected_modal numeric,
    reviewer_notes text
);

create table mandi_duplicate_review_queue (
    id bigint generated always as identity primary key,
    mandi_id_a bigint not null references mandis(id),
    mandi_id_b bigint not null references mandis(id),
    state text,
    district text,
    similarity numeric,
    created_at timestamptz not null default now(),
    resolved boolean not null default false
);

create table crop_duplicate_review_queue (
    id bigint generated always as identity primary key,
    crop_id_a bigint not null references crops(id),
    crop_id_b bigint not null references crops(id),
    similarity numeric,
    created_at timestamptz not null default now(),
    resolved boolean not null default false
);
