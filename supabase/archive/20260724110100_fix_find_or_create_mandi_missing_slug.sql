-- find_or_create_mandi never populated `mandis.slug` (NOT NULL, unique)
-- since that column was added to the schema -- every auto-create of a
-- brand-new mandi hit `null value in column "slug" violates not-null
-- constraint` and the calling pipeline (live_tick.py) silently skipped
-- that row. No crash, no alert -- the price data for that mandi/day
-- was just quietly dropped. Discovered 2026-07-24 from a live run
-- (Gujarat mandis: Devgadhbaria APMC, Kalol(Veg,Market,Kalol) APMC,
-- Dahod(Veg. Market) APMC).
--
-- Fix: slug must be set at INSERT time (the NOT NULL fires before any
-- later UPDATE could run), which means the row's own id has to be
-- known in advance. mandis.id is GENERATED ALWAYS AS IDENTITY, so we
-- pull the next value from its sequence explicitly and insert with
-- OVERRIDING SYSTEM VALUE. Slug format matches the convention already
-- used by every existing mandi row: slugified-name + "-" + id
-- (e.g. "kurnool-apmc-1", "praneeth-f-v-6").

create or replace function public.find_or_create_mandi(
    p_name text, p_state text, p_district text,
    p_lat double precision default null, p_lng double precision default null,
    p_source text default 'resource_1'
)
returns bigint
language plpgsql
set search_path to 'public', 'extensions', 'pg_temp'
as $function$
DECLARE
    v_normalized text := normalize_market_name(p_name);
    v_canon_normalized text := _canonicalize_place_words(p_state, v_normalized);
    v_match_key text := _mandi_match_key(v_canon_normalized);
    v_district text := normalize_district(p_state, p_district);
    v_clean_name text := initcap(v_match_key);
    v_mandi_id bigint;
    v_best_match_id bigint;
    v_best_sim numeric;
    v_slug_base text;
BEGIN
    SELECT id INTO v_mandi_id FROM mandis
    WHERE normalized_name = v_normalized AND state = p_state AND district = v_district AND status = 'ACTIVE';
    IF v_mandi_id IS NOT NULL THEN RETURN v_mandi_id; END IF;

    SELECT mandi_id INTO v_mandi_id FROM mandi_aliases
    WHERE normalized_alias = v_normalized AND approved = true
      AND mandi_id IN (SELECT id FROM mandis WHERE state = p_state AND district = v_district AND status = 'ACTIVE');
    IF v_mandi_id IS NOT NULL THEN RETURN v_mandi_id; END IF;

    SELECT id, similarity(_mandi_match_key(_canonicalize_place_words(p_state, normalized_name)), v_match_key)
    INTO v_best_match_id, v_best_sim
    FROM mandis
    WHERE state = p_state AND district = v_district AND status = 'ACTIVE'
    ORDER BY similarity(_mandi_match_key(_canonicalize_place_words(p_state, normalized_name)), v_match_key) DESC LIMIT 1;

    IF v_best_sim >= 0.75 THEN
        INSERT INTO mandi_aliases (mandi_id, alias_name, normalized_alias, match_method, match_confidence, approved, source, real_mandi_name)
        VALUES (v_best_match_id, p_name, v_normalized, 'FUZZY', v_best_sim, false, p_source,
                (SELECT name FROM mandis WHERE id = v_best_match_id));
        RETURN v_best_match_id;
    END IF;

    v_mandi_id := nextval('public.mandis_id_seq');
    v_slug_base := trim(both '-' from lower(regexp_replace(v_clean_name, '[^a-zA-Z0-9]+', '-', 'g')));

    INSERT INTO mandis (id, name, normalized_name, state, district, latitude, longitude, review_status, slug)
    OVERRIDING SYSTEM VALUE
    VALUES (v_mandi_id, v_clean_name, v_normalized, p_state, v_district, p_lat, p_lng, 'AUTO_CREATED',
            v_slug_base || '-' || v_mandi_id::text);

    RETURN v_mandi_id;
END;
$function$;
