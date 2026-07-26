-- Root cause found while investigating a "1 variety dropped, 1 grade dropped" line
-- in sync_storefront.py's output: merge_entity('crop', ...) (see
-- 20260725162000_version_identity_rpcs_and_merge_entity_fix.sql) repoints
-- mandi_daily_prices.crop_id and marks the crop MERGED, but never touches
-- varieties/grades at all. varieties/grades also had no merge-tracking columns,
-- unlike crops, so there was no way to even represent "this variety was merged
-- into that one."
--
-- Confirmed concrete damage from this gap: crop 202 ("nigella") was merged into
-- crop 197 ("nigella seeds") on 2026-07-26. Variety 608 ("kalonji", child of 202)
-- was never repointed or merged. Two mandi_daily_prices rows ended up with
-- crop_id = 197 (correctly repointed) but variety_id = 608 (still pointing at
-- the merged-away crop's child) — an internally inconsistent row. Crop 197
-- already had its own equivalent variety (593, "kalonji/nigella"), so this was a
-- genuine duplicate, not just an orphan.

-- 1. Give varieties/grades the same merge-tracking shape crops already has.
alter table public.varieties
  add column if not exists status text not null default 'ACTIVE',
  add column if not exists merged_into_id bigint references public.varieties(id),
  add column if not exists merge_reason text,
  add column if not exists merge_method text,
  add column if not exists merge_confidence numeric,
  add column if not exists merged_at timestamptz;

alter table public.grades
  add column if not exists status text not null default 'ACTIVE',
  add column if not exists merged_into_id bigint references public.grades(id),
  add column if not exists merge_reason text,
  add column if not exists merge_method text,
  add column if not exists merge_confidence numeric,
  add column if not exists merged_at timestamptz;

create index if not exists idx_varieties_status on public.varieties(status);
create index if not exists idx_grades_status on public.grades(status);

-- 2. Rewrite merge_entity() to add 'variety'/'grade' branches, and make the 'crop'
-- branch cascade into them automatically instead of leaving children orphaned.
create or replace function merge_entity(
    p_entity_type text, p_source_id bigint, p_target_id bigint,
    p_reason text, p_merge_method text, p_merge_confidence numeric
) returns boolean
language plpgsql as $$
declare
    v_variety record;
    v_grade record;
    v_match_id bigint;
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

        -- Cascade: every ACTIVE variety still hanging off the now-merged crop must
        -- either merge into a same-named variety under the survivor crop, or (if no
        -- name clash) simply move house to the survivor crop.
        for v_variety in
            select * from varieties where crop_id = p_source_id and status = 'ACTIVE'
        loop
            select id into v_match_id from varieties
            where crop_id = p_target_id and normalized_name = v_variety.normalized_name and status = 'ACTIVE'
            limit 1;

            if v_match_id is not null then
                perform merge_entity('variety', v_variety.id, v_match_id, p_reason,
                                      'auto_cascade_from_crop_merge', p_merge_confidence);
            else
                update varieties set crop_id = p_target_id where id = v_variety.id;
            end if;
        end loop;

        return true;

    elsif p_entity_type = 'variety' then
        if exists (select 1 from varieties where id = p_source_id and status = 'MERGED') then
            return false;
        end if;

        -- Cascade grades first, same pattern as crop -> variety above.
        for v_grade in
            select * from grades where variety_id = p_source_id and status = 'ACTIVE'
        loop
            select id into v_match_id from grades
            where variety_id = p_target_id and normalized_name = v_grade.normalized_name and status = 'ACTIVE'
            limit 1;

            if v_match_id is not null then
                perform merge_entity('grade', v_grade.id, v_match_id, p_reason,
                                      'auto_cascade_from_variety_merge', p_merge_confidence);
            else
                update grades set variety_id = p_target_id where id = v_grade.id;
            end if;
        end loop;

        delete from mandi_daily_prices p
        where p.variety_id = p_source_id
          and exists (
              select 1 from mandi_daily_prices t
              where t.variety_id = p_target_id and t.mandi_id = p.mandi_id and t.crop_id = p.crop_id
                and t.grade_id is not distinct from p.grade_id and t.price_date = p.price_date
                and t.source_mandi_id is not distinct from p.source_mandi_id
          );
        update mandi_daily_prices set variety_id = p_target_id where variety_id = p_source_id;

        update varieties set status = 'MERGED', merged_into_id = p_target_id, merge_reason = p_reason,
            merge_method = p_merge_method, merge_confidence = p_merge_confidence, merged_at = now()
        where id = p_source_id;

        insert into audit_events (entity_type, entity_id, event_type, details)
        values ('variety', p_source_id, 'entity_merged', jsonb_build_object('target_id', p_target_id, 'reason', p_reason));

        return true;

    elsif p_entity_type = 'grade' then
        if exists (select 1 from grades where id = p_source_id and status = 'MERGED') then
            return false;
        end if;

        delete from mandi_daily_prices p
        where p.grade_id = p_source_id
          and exists (
              select 1 from mandi_daily_prices t
              where t.grade_id = p_target_id and t.mandi_id = p.mandi_id and t.crop_id = p.crop_id
                and t.variety_id is not distinct from p.variety_id and t.price_date = p.price_date
                and t.source_mandi_id is not distinct from p.source_mandi_id
          );
        update mandi_daily_prices set grade_id = p_target_id where grade_id = p_source_id;

        update grades set status = 'MERGED', merged_into_id = p_target_id, merge_reason = p_reason,
            merge_method = p_merge_method, merge_confidence = p_merge_confidence, merged_at = now()
        where id = p_source_id;

        insert into audit_events (entity_type, entity_id, event_type, details)
        values ('grade', p_source_id, 'entity_merged', jsonb_build_object('target_id', p_target_id, 'reason', p_reason));

        return true;

    elsif p_entity_type = 'mandi' then
        perform union_merge_mandi(p_target_id, p_source_id, p_reason);
        return true;
    end if;

    return false;
end;
$$;

grant execute on function merge_entity(text,bigint,bigint,text,text,numeric) to service_role;

-- 3. Retroactively fix the one existing case (crop 202 -> 197 merged before this fix
-- existed): variety 608 "kalonji" cascades into 593 "kalonji/nigella", which in turn
-- cascades grade 1890 "faq" into 1865 "faq". This repoints the mandi_daily_prices
-- rows that were left inconsistent (crop_id=197, variety_id=608).
select merge_entity(
    'variety', 608, 593,
    'retroactive fix: crop 202 (nigella) was merged into 197 (nigella seeds) on 2026-07-26 '
    'via merge_entity, but that function did not yet cascade into varieties/grades, '
    'leaving this variety orphaned and mandi_daily_prices rows internally inconsistent',
    'retroactive_cascade_fix', 1.0
);
