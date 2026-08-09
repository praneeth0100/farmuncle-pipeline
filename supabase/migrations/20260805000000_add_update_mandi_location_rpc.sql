-- 2026-08-05: geocode_mandis.py (and geocode_kerala_villages.py) have called
-- supabase.rpc("update_mandi_location", ...) for every write since the
-- 2026-07-16 EXACT-only revision, but this function was never actually
-- created in this project. Confirmed directly against the live database
-- (flxjrcbhmcuaynctokpv): 0/2767 mandis have ever had coordinates written,
-- and `update_mandi_location` is absent from information_schema.routines.
-- The script's own docstring described the intended behavior in detail
-- (never clobber a better confidence with a worse one, skip non-ACTIVE
-- mandis, write an audit trail) -- this migration implements exactly that
-- contract so the script can finally run.
--
-- Note on the audit trail: the script's docstring says results go to
-- `entity_history`, but that table is defined and never actually written
-- to anywhere in this codebase -- every real RPC (merge_entity, etc.)
-- writes to `audit_events` instead. This follows the convention that's
-- actually live and used, not the stale docstring claim.

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
    -- Higher rank = more trustworthy. Mirrors the location_confidence
    -- check constraint on mandis (EXACT/APMC/DISTRICT/STATE/UNKNOWN).
    v_confidence_rank constant jsonb := '{
        "UNKNOWN": 0, "STATE": 1, "DISTRICT": 2, "APMC": 3, "EXACT": 4
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

    -- Only ACTIVE mandis get touched -- a MERGED mandi's coordinates
    -- don't matter (its prices already moved to the survivor), and
    -- writing to it would be silently pointless at best.
    if v_status <> 'ACTIVE' then
        return false;
    end if;

    -- A worse-confidence result can never clobber a better one already on
    -- file. In practice geocode_mandis.py only ever calls this with
    -- 'EXACT' (the best tier), so today this mainly protects a second run
    -- from downgrading a manually-verified row -- but it's cheap insurance
    -- against any future caller passing a coarser confidence.
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

-- service_role already gets execute on all functions via the
-- `alter default privileges ... grant execute on functions` in
-- 20260725161000_grant_service_role_privileges.sql, so no explicit
-- grant is needed here.
