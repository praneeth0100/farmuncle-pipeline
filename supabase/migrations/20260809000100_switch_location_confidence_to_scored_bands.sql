-- The old EXACT/APMC/DISTRICT/STATE/UNKNOWN vocabulary described WHERE
-- a coordinate came from (a market vs a district centroid, etc). The
-- v2 (Places-API-based) geocoder instead scores every candidate and
-- classifies it into HIGH/MEDIUM/LOW/REVIEW confidence bands (see
-- farmuncle_pipeline/geocoding/scorer.py) -- a more honest signal,
-- since it reflects how much evidence supports the match rather than
-- which tier of query happened to succeed.
--
-- Safe to do as a hard swap, not an additive union: confirmed live
-- (2026-08-05 audit) that 0/2767 mandis had ever actually been
-- geocoded before the run this migration follows, and that run's 18
-- rows are being rolled back to NULL separately (see
-- rollback_geocode_run.sql) as mislabeled town-centroid matches. So
-- there is no real data on the old vocabulary to migrate/preserve.
--
-- UNRESOLVED is intentionally NOT in the allowed list: a mandi with no
-- confident match just gets left with location_confidence = NULL
-- (blueprint's own "leave coordinates empty" policy) rather than
-- storing a confidence label for the absence of a result.

alter table public.mandis
    drop constraint if exists mandis_location_confidence_check;

alter table public.mandis
    add constraint mandis_location_confidence_check
    check (location_confidence in ('HIGH', 'MEDIUM', 'LOW', 'REVIEW'));

create or replace function public.update_mandi_location(
    p_mandi_id bigint,
    p_latitude double precision,
    p_longitude double precision,
    p_location_confidence text,
    p_source text
) returns boolean
language plpgsql as $$
declare
    v_status text;
    v_old_confidence text;
    v_old_latitude double precision;
    v_old_longitude double precision;
    -- Updated rank ladder matching the new HIGH/MEDIUM/LOW/REVIEW
    -- vocabulary. Same "never let a worse result clobber a better one"
    -- protection as before, just against the new bands.
    v_confidence_rank constant jsonb := '{
        "REVIEW": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3
    }'::jsonb;
begin
    select status, location_confidence, latitude, longitude
    into v_status, v_old_confidence, v_old_latitude, v_old_longitude
    from mandis
    where id = p_mandi_id
    for update;

    if not found then
        return false;
    end if;

    if v_status <> 'ACTIVE' then
        return false;
    end if;

    if v_old_confidence is not null
       and coalesce((v_confidence_rank -> v_old_confidence)::int, 0)
           > coalesce((v_confidence_rank -> p_location_confidence)::int, 0)
    then
        return false;
    end if;

    update mandis
    set latitude = p_latitude,
        longitude = p_longitude,
        location_confidence = p_location_confidence,
        last_verified_at = now()
    where id = p_mandi_id;

    insert into audit_events (entity_type, entity_id, event_type, details)
    values (
        'mandi',
        p_mandi_id,
        'location_geocoded',
        jsonb_build_object(
            'old_latitude', v_old_latitude,
            'old_longitude', v_old_longitude,
            'old_confidence', v_old_confidence,
            'new_latitude', p_latitude,
            'new_longitude', p_longitude,
            'new_confidence', p_location_confidence,
            'source', p_source
        )
    );

    return true;
end;
$$;
