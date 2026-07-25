create extension if not exists pg_trgm with schema extensions;

create or replace function normalize_unit(p_unit text) returns text
language sql immutable as $$
    select lower(trim(coalesce(p_unit, '')));
$$;

create or replace function normalize_district(p_state text, p_district text) returns text
language plpgsql stable as $$
declare
    v_canonical text;
begin
    select canonical_district into v_canonical
    from district_aliases
    where state = p_state and alias_district = trim(p_district)
    limit 1;
    return coalesce(v_canonical, trim(p_district));
end;
$$;

-- strips market-type/generic suffix words so "Jabalpur", "Jabalpur APMC",
-- "Jabalpur(F V)", "Jabalpur Mandi" all normalize to the same match key --
-- town-level identity merge is the deliberate policy (2026-07-23 decision).
create or replace function _mandi_match_key(p_name text) returns text
language sql immutable as $$
    select trim(regexp_replace(
        lower(regexp_replace(p_name, '[\(\)]', ' ', 'g')),
        '\s*\y(apmc|mandi|market|f\s*&?\s*v|grain|sandhai|committee)\y\s*', ' ', 'g'
    ));
$$;

create or replace function _crop_match_key(p_name text) returns text
language sql immutable as $$
    select trim(regexp_replace(lower(p_name), '\s+', ' ', 'g'));
$$;

-- find_or_create_mandi -- signature matches identity_client.py's resolve_mandi exactly
create or replace function find_or_create_mandi(
    p_name text, p_state text, p_district text,
    p_lat double precision, p_lng double precision, p_source text
) returns bigint
language plpgsql as $$
declare
    v_key text := _mandi_match_key(p_name);
    v_district text := normalize_district(p_state, p_district);
    v_id bigint;
    v_match_id bigint;
    v_sim numeric;
begin
    select id into v_id from mandis
    where normalized_name = v_key and state = p_state and district = v_district
      and status = 'ACTIVE'
    limit 1;

    if v_id is not null then
        update mandis set last_seen_at = now() where id = v_id;
        return v_id;
    end if;

    select mandi_id into v_id from mandi_aliases
    where normalized_alias = v_key and approved = true
    limit 1;

    if v_id is not null then
        update mandis set last_seen_at = now() where id = v_id;
        return v_id;
    end if;

    -- fuzzy match within the same district -- logs an UNAPPROVED alias, still
    -- resolves to the match so ingestion isn't blocked, but now with a trail
    select m.id, similarity(m.normalized_name, v_key) into v_match_id, v_sim
    from mandis m
    where m.state = p_state and m.district = v_district and m.status = 'ACTIVE'
    order by similarity(m.normalized_name, v_key) desc
    limit 1;

    if v_match_id is not null and v_sim > 0.5 then
        insert into mandi_aliases (mandi_id, alias_name, normalized_alias, match_method, match_confidence, approved, source)
        values (v_match_id, p_name, v_key, 'FUZZY', v_sim, false, p_source)
        on conflict (normalized_alias) do nothing;
        update mandis set last_seen_at = now() where id = v_match_id;
        return v_match_id;
    end if;

    insert into mandis (slug, name, normalized_name, state, district, latitude, longitude, ingested_from)
    values (
        v_key || '-' || substr(md5(random()::text), 1, 6),
        p_name, v_key, p_state, v_district, p_lat, p_lng, p_source
    )
    returning id into v_id;

    return v_id;
end;
$$;

-- find_or_create_crop -- signature matches identity_client.py's resolve_crop exactly.
-- CORRECTION vs the old project: fuzzy matches now log an unapproved alias
-- (auditable, reviewable) instead of merging with zero trail.
create or replace function find_or_create_crop(
    p_name text, p_unit text, p_source text
) returns bigint
language plpgsql as $$
declare
    v_key text := _crop_match_key(p_name);
    v_id bigint;
    v_match_id bigint;
    v_sim numeric;
begin
    select id into v_id from crops where normalized_name = v_key and status = 'ACTIVE' limit 1;
    if v_id is not null then
        update crops set last_seen_at = now() where id = v_id;
        return v_id;
    end if;

    select crop_id into v_id from crop_aliases where normalized_alias = v_key and approved = true limit 1;
    if v_id is not null then
        update crops set last_seen_at = now() where id = v_id;
        return v_id;
    end if;

    select c.id, similarity(c.normalized_name, v_key) into v_match_id, v_sim
    from crops c where c.status = 'ACTIVE'
    order by similarity(c.normalized_name, v_key) desc
    limit 1;

    if v_match_id is not null and v_sim > 0.6 then
        insert into crop_aliases (crop_id, alias_name, normalized_alias, match_method, match_confidence, approved, source)
        values (v_match_id, p_name, v_key, 'FUZZY', v_sim, false, p_source)
        on conflict (normalized_alias) do nothing;
        update crops set last_seen_at = now() where id = v_match_id;
        return v_match_id;
    end if;

    insert into crops (name, normalized_name, unit, ingested_from)
    values (p_name, v_key, normalize_unit(p_unit), p_source)
    returning id into v_id;

    return v_id;
end;
$$;

-- find_or_create_variety / find_or_create_grade -- NEW this rebuild. Correctly
-- scoped: variety by crop_id, grade by variety_id. No fuzzy matching needed --
-- research on 2026-07-25 showed both are clean (no casing/whitespace dupes) once
-- properly scoped; the real risk was cross-crop collision ("Other" meant 240
-- different things), which crop-scoping eliminates structurally.
create or replace function find_or_create_variety(p_crop_id bigint, p_raw_text text) returns bigint
language plpgsql as $$
declare
    v_norm text := lower(trim(p_raw_text));
    v_id bigint;
begin
    if p_raw_text is null or trim(p_raw_text) = '' then
        return null;
    end if;

    select id into v_id from varieties where crop_id = p_crop_id and normalized_name = v_norm;
    if v_id is not null then return v_id; end if;

    select v.variety_id into v_id from variety_aliases v
    where v.normalized_alias = v_norm and v.approved = true
      and v.variety_id in (select id from varieties where crop_id = p_crop_id);
    if v_id is not null then return v_id; end if;

    insert into varieties (crop_id, name, normalized_name)
    values (p_crop_id, p_raw_text, v_norm)
    on conflict (crop_id, normalized_name) do update set name = excluded.name
    returning id into v_id;

    return v_id;
end;
$$;

create or replace function find_or_create_grade(p_variety_id bigint, p_raw_text text) returns bigint
language plpgsql as $$
declare
    v_norm text := lower(trim(p_raw_text));
    v_id bigint;
begin
    if p_variety_id is null or p_raw_text is null or trim(p_raw_text) = '' then
        return null;
    end if;

    select id into v_id from grades where variety_id = p_variety_id and normalized_name = v_norm;
    if v_id is not null then return v_id; end if;

    insert into grades (variety_id, name, normalized_name)
    values (p_variety_id, p_raw_text, v_norm)
    on conflict (variety_id, normalized_name) do update set name = excluded.name
    returning id into v_id;

    return v_id;
end;
$$;
