-- Found while checking config_validator.py's requirements against the real live
-- schema (2026-07-25): two version-identity RPCs it actually invokes over REST
-- (current_normalization_version, rpc_version_identity) didn't exist yet, and
-- merge_entity's params weren't prefixed with p_ like every other RPC in this
-- schema, inconsistent with config_validator.py's own signature check.
--
-- Also fixes a real bug from this same session: an earlier CREATE OR REPLACE of
-- upsert_raw_price_entry with a different parameter list (adding p_raw_grade)
-- created a SECOND overloaded function instead of replacing the first, since
-- Postgres treats differing signatures as distinct functions. The old, broken
-- 11-param version (missing p_raw_grade) was still live until this migration.

create or replace function current_normalization_version() returns int
language sql immutable as $$ select 1; $$;

create or replace function rpc_version_identity() returns int
language sql immutable as $$ select 1; $$;

grant execute on function current_normalization_version() to service_role;
grant execute on function rpc_version_identity() to service_role;

drop function if exists merge_entity(text,bigint,bigint,text,text,numeric);

create or replace function merge_entity(
    p_entity_type text, p_source_id bigint, p_target_id bigint,
    p_reason text, p_merge_method text, p_merge_confidence numeric
) returns boolean
language plpgsql as $$
begin
    if p_entity_type = 'crop' then
        if exists (select 1 from crops where id = p_source_id and status = 'MERGED') then
            return false;
        end if;
        update crop_aliases set crop_id = p_target_id where crop_id = p_source_id;
        delete from mandi_daily_prices p
        where p.crop_id = p_source_id
          and exists (
              select 1 from mandi_daily_prices t
              where t.crop_id = p_target_id and t.mandi_id = p.mandi_id
                and t.variety = p.variety and t.grade = p.grade and t.price_date = p.price_date
                and t.source_mandi_id is not distinct from p.source_mandi_id
          );
        update mandi_daily_prices set crop_id = p_target_id where crop_id = p_source_id;
        update crops set status = 'MERGED', merged_into_id = p_target_id, merge_reason = p_reason,
            merge_method = p_merge_method, merge_confidence = p_merge_confidence, merged_at = now()
        where id = p_source_id;
        insert into audit_events (entity_type, entity_id, event_type, details)
        values ('crop', p_source_id, 'entity_merged', jsonb_build_object('target_id', p_target_id, 'reason', p_reason));
        return true;
    elsif p_entity_type = 'mandi' then
        perform union_merge_mandi(p_target_id, p_source_id, p_reason);
        return true;
    end if;
    return false;
end;
$$;

drop function if exists upsert_raw_price_entry(text,text,text,text,text,text,date,text,jsonb,text,int);
