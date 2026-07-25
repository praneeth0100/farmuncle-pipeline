-- union_merge_mandi -- reused verbatim from the working, live-proven version built
-- and used for 50 real merges on 2026-07-25 on the old project. Never deletes data
-- on a real collision: identical price -> dedupe to one row; differing price ->
-- keep BOTH, told apart by source_mandi_id/source_mandi_name.
CREATE OR REPLACE FUNCTION public.union_merge_mandi(p_survivor_id bigint, p_loser_id bigint, p_reason text DEFAULT 'apmc_fv_union_merge')
RETURNS TABLE(deduped_identical bigint, kept_both_differing bigint, moved_no_collision bigint)
LANGUAGE plpgsql
SET search_path TO 'public'
AS $$
DECLARE
  v_deduped bigint := 0;
  v_kept_both bigint := 0;
  v_moved bigint := 0;
BEGIN
  UPDATE mandi_daily_prices SET source_mandi_id = mandi_id, source_mandi_name = (SELECT name FROM mandis WHERE id = mandi_id)
  WHERE mandi_id IN (p_survivor_id, p_loser_id) AND source_mandi_id IS NULL;

  WITH dupes AS (
    SELECT l.id AS loser_row_id
    FROM mandi_daily_prices l
    JOIN mandi_daily_prices s
      ON s.mandi_id = p_survivor_id AND l.mandi_id = p_loser_id
     AND s.crop_id = l.crop_id AND s.variety = l.variety AND s.grade = l.grade AND s.price_date = l.price_date
     AND s.modal_price IS NOT DISTINCT FROM l.modal_price
     AND s.min_price IS NOT DISTINCT FROM l.min_price
     AND s.max_price IS NOT DISTINCT FROM l.max_price
  )
  DELETE FROM mandi_daily_prices WHERE id IN (SELECT loser_row_id FROM dupes);
  GET DIAGNOSTICS v_deduped = ROW_COUNT;

  SELECT count(*) INTO v_kept_both
  FROM mandi_daily_prices l
  JOIN mandi_daily_prices s
    ON s.mandi_id = p_survivor_id AND l.mandi_id = p_loser_id
   AND s.crop_id = l.crop_id AND s.variety = l.variety AND s.grade = l.grade AND s.price_date = l.price_date;

  UPDATE mandi_daily_prices SET mandi_id = p_survivor_id, updated_at = now() WHERE mandi_id = p_loser_id;
  GET DIAGNOSTICS v_moved = ROW_COUNT;

  UPDATE mandis SET status = 'MERGED', merged_into_id = p_survivor_id, merge_reason = p_reason, merged_at = now()
  WHERE id = p_loser_id;

  UPDATE mandi_aliases SET mandi_id = p_survivor_id WHERE mandi_id = p_loser_id;

  RETURN QUERY SELECT v_deduped, v_kept_both, v_moved;
END;
$$;

-- merge_entity -- generic mandi/crop merge, simpler "collisions keep target's row"
-- rule per spec §6.5. Used for crop merges (crops don't need union_merge_mandi's
-- keep-both nuance -- collisions there are true duplicates by definition).
create or replace function merge_entity(
    entity_type text, source_id bigint, target_id bigint,
    reason text, merge_method text, merge_confidence numeric
) returns boolean
language plpgsql as $$
begin
    if entity_type = 'crop' then
        if exists (select 1 from crops where id = source_id and status = 'MERGED') then
            return false;
        end if;
        update crop_aliases set crop_id = target_id where crop_id = source_id;
        delete from mandi_daily_prices p
        where p.crop_id = source_id
          and exists (
              select 1 from mandi_daily_prices t
              where t.crop_id = target_id and t.mandi_id = p.mandi_id
                and t.variety = p.variety and t.grade = p.grade and t.price_date = p.price_date
                and t.source_mandi_id is not distinct from p.source_mandi_id
          );
        update mandi_daily_prices set crop_id = target_id where crop_id = source_id;
        update crops set status = 'MERGED', merged_into_id = target_id, merge_reason = reason,
            merge_method = merge_entity.merge_method, merge_confidence = merge_entity.merge_confidence, merged_at = now()
        where id = source_id;
        insert into audit_events (entity_type, entity_id, event_type, details)
        values ('crop', source_id, 'entity_merged', jsonb_build_object('target_id', target_id, 'reason', reason));
        return true;
    elsif entity_type = 'mandi' then
        perform union_merge_mandi(target_id, source_id, reason);
        return true;
    end if;
    return false;
end;
$$;

create or replace function verify_merge_integrity() returns table(issue text, entity_id bigint, detail text)
language sql stable as $$
    select 'price row on MERGED mandi', p.mandi_id, 'mandi_daily_prices.id=' || p.id
    from mandi_daily_prices p join mandis m on m.id = p.mandi_id where m.status = 'MERGED'
    union all
    select 'price row on MERGED crop', p.crop_id, 'mandi_daily_prices.id=' || p.id
    from mandi_daily_prices p join crops c on c.id = p.crop_id where c.status = 'MERGED';
$$;

create or replace function sweep_duplicate_mandis() returns int
language plpgsql as $$
declare
    v_count int := 0;
begin
    insert into mandi_duplicate_review_queue (mandi_id_a, mandi_id_b, state, district, similarity)
    select a.id, b.id, a.state, a.district, similarity(a.normalized_name, b.normalized_name)
    from mandis a
    join mandis b on b.state = a.state and b.district = a.district and b.id > a.id
    where a.status = 'ACTIVE' and b.status = 'ACTIVE'
      and similarity(a.normalized_name, b.normalized_name) > 0.4
      and not exists (
          select 1 from mandi_duplicate_review_queue q
          where (q.mandi_id_a = a.id and q.mandi_id_b = b.id) or (q.mandi_id_a = b.id and q.mandi_id_b = a.id)
      );
    get diagnostics v_count = row_count;
    return v_count;
end;
$$;

create or replace function sweep_duplicate_crops() returns int
language plpgsql as $$
declare
    v_count int := 0;
begin
    insert into crop_duplicate_review_queue (crop_id_a, crop_id_b, similarity)
    select a.id, b.id, similarity(a.normalized_name, b.normalized_name)
    from crops a
    join crops b on b.id > a.id
    where a.status = 'ACTIVE' and b.status = 'ACTIVE'
      and similarity(a.normalized_name, b.normalized_name) > 0.5
      and not exists (
          select 1 from crop_duplicate_review_queue q
          where (q.crop_id_a = a.id and q.crop_id_b = b.id) or (q.crop_id_a = b.id and q.crop_id_b = a.id)
      );
    get diagnostics v_count = row_count;
    return v_count;
end;
$$;

-- refresh_price_cache -- correction vs old project: tolerance-window change_1d/7d/1m
-- computed here from day one. Nulls, not fabricated zeros, when no prior price
-- exists in-window.
create or replace function refresh_price_cache() returns void
language plpgsql as $$
begin
    insert into price_cache (mandi_id, crop_id, variety, grade, latest_price_date, modal_price, min_price, max_price, source_mandi_name, refreshed_at)
    select distinct on (mandi_id, crop_id, coalesce(variety,''), coalesce(grade,''))
        mandi_id, crop_id, coalesce(variety,''), coalesce(grade,''), price_date, modal_price, min_price, max_price, source_mandi_name, now()
    from mandi_daily_prices
    order by mandi_id, crop_id, coalesce(variety,''), coalesce(grade,''), price_date desc
    on conflict (mandi_id, crop_id, variety, grade) do update
        set latest_price_date = excluded.latest_price_date,
            modal_price = excluded.modal_price,
            min_price = excluded.min_price,
            max_price = excluded.max_price,
            source_mandi_name = excluded.source_mandi_name,
            refreshed_at = now();

    update price_cache pc set
        change_1d = pc.modal_price - prior.modal_price,
        change_1d_pct = case when prior.modal_price > 0 then round(100.0*(pc.modal_price - prior.modal_price)/prior.modal_price, 2) end,
        last_recomputed_at = now()
    from (
        select distinct on (mandi_id, crop_id, coalesce(variety,''), coalesce(grade,''))
            mandi_id, crop_id, coalesce(variety,'') as variety, coalesce(grade,'') as grade, modal_price
        from mandi_daily_prices p
        where p.price_date between (select max(price_date) from mandi_daily_prices) - 3 and (select max(price_date) from mandi_daily_prices) - 1
        order by mandi_id, crop_id, coalesce(variety,''), coalesce(grade,''), price_date desc
    ) prior
    where pc.mandi_id = prior.mandi_id and pc.crop_id = prior.crop_id and pc.variety = prior.variety and pc.grade = prior.grade;

    update price_cache pc set
        change_7d = pc.modal_price - prior.modal_price,
        change_7d_pct = case when prior.modal_price > 0 then round(100.0*(pc.modal_price - prior.modal_price)/prior.modal_price, 2) end
    from (
        select distinct on (mandi_id, crop_id, coalesce(variety,''), coalesce(grade,''))
            mandi_id, crop_id, coalesce(variety,'') as variety, coalesce(grade,'') as grade, modal_price
        from mandi_daily_prices p
        where p.price_date between (select max(price_date) from mandi_daily_prices) - 10 and (select max(price_date) from mandi_daily_prices) - 4
        order by mandi_id, crop_id, coalesce(variety,''), coalesce(grade,''), price_date desc
    ) prior
    where pc.mandi_id = prior.mandi_id and pc.crop_id = prior.crop_id and pc.variety = prior.variety and pc.grade = prior.grade;

    update price_cache pc set
        change_1m = pc.modal_price - prior.modal_price,
        change_1m_pct = case when prior.modal_price > 0 then round(100.0*(pc.modal_price - prior.modal_price)/prior.modal_price, 2) end
    from (
        select distinct on (mandi_id, crop_id, coalesce(variety,''), coalesce(grade,''))
            mandi_id, crop_id, coalesce(variety,'') as variety, coalesce(grade,'') as grade, modal_price
        from mandi_daily_prices p
        where p.price_date between (select max(price_date) from mandi_daily_prices) - 35 and (select max(price_date) from mandi_daily_prices) - 25
        order by mandi_id, crop_id, coalesce(variety,''), coalesce(grade,''), price_date desc
    ) prior
    where pc.mandi_id = prior.mandi_id and pc.crop_id = prior.crop_id and pc.variety = prior.variety and pc.grade = prior.grade;
end;
$$;

-- upsert_raw_price_entry -- signature matches raw_dedup.py's single-row caller
-- exactly, INCLUDING p_raw_grade (added 2026-07-22 upstream; missed on the first
-- draft of this rebuild, caught by grepping the real call site).
create or replace function upsert_raw_price_entry(
    p_resource text, p_market text, p_state text, p_district text, p_commodity text,
    p_raw_variety text, p_raw_grade text, p_price_date date, p_content_hash text, p_payload jsonb,
    p_batch_id text, p_parser_version int
) returns table(entry_id bigint, is_new boolean)
language plpgsql as $$
declare
    v_id bigint;
    v_is_new boolean;
begin
    insert into raw_price_entries (
        resource, market, state, district, commodity, raw_variety, raw_grade, price_date,
        content_hash, payload, parser_version, first_seen_batch_id, last_seen_batch_id
    ) values (
        p_resource, p_market, p_state, p_district, p_commodity, p_raw_variety, p_raw_grade, p_price_date,
        p_content_hash, p_payload, p_parser_version, p_batch_id, p_batch_id
    )
    on conflict (resource, market, state, coalesce(district,''), commodity, coalesce(raw_variety,''), coalesce(raw_grade,''), price_date, content_hash)
    do update set last_seen_batch_id = p_batch_id, last_seen_at = now()
    returning raw_price_entries.id, (xmax = 0) into v_id, v_is_new;

    return query select v_id, v_is_new;
end;
$$;

-- upsert_raw_price_entries_batch -- bulk version live_tick actually calls.
-- p_entries: jsonb array, each object keyed exactly as raw_dedup.py builds it:
-- resource, market, state, district, commodity, raw_variety, "grade" (NOT
-- raw_grade -- documented inconsistency in raw_dedup.py's own docstring,
-- matched here deliberately, not silently worked around), price_date,
-- content_hash, payload, batch_id, parser_version.
create or replace function upsert_raw_price_entries_batch(p_entries jsonb)
returns table(entry_id bigint, is_new boolean)
language plpgsql as $$
begin
    return query
    insert into raw_price_entries (
        resource, market, state, district, commodity, raw_variety, raw_grade, price_date,
        content_hash, payload, parser_version, first_seen_batch_id, last_seen_batch_id
    )
    select
        e->>'resource', e->>'market', e->>'state', e->>'district', e->>'commodity',
        e->>'raw_variety', e->>'grade', (e->>'price_date')::date,
        e->>'content_hash', e->'payload', (e->>'parser_version')::int,
        e->>'batch_id', e->>'batch_id'
    from jsonb_array_elements(p_entries) as e
    on conflict (resource, market, state, coalesce(district,''), commodity, coalesce(raw_variety,''), coalesce(raw_grade,''), price_date, content_hash)
    do update set last_seen_batch_id = excluded.last_seen_batch_id, last_seen_at = now()
    returning raw_price_entries.id, (xmax = 0);
end;
$$;
