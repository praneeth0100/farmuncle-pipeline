-- Repair the production code/schema contract after the Supabase-project move.
-- Raw batch IDs are ULIDs (text) everywhere; raw observations are immutable.

begin;

-- 1. Align raw lineage columns with raw_api_batches.id (text ULID).
alter table public.raw_api_records
  alter column batch_id type text using batch_id::text;
alter table public.raw_price_entries
  alter column first_seen_batch_id type text using first_seen_batch_id::text,
  alter column last_seen_batch_id type text using last_seen_batch_id::text;

-- A unique identity alone forced an UPDATE of historic raw content whenever
-- a price changed. Include the content hash so each distinct observation is
-- retained and only an unchanged observation is touched.
drop index if exists public.uq_raw_price_entries_identity;
create unique index uq_raw_price_entries_identity_content
  on public.raw_price_entries (
    resource, market, state, coalesce(district, ''), commodity,
    coalesce(raw_variety, ''), coalesce(grade, ''), price_date, content_hash
  );

-- The batch RPC depends on the single-row RPC, so replace both signatures.
drop function if exists public.upsert_raw_price_entries_batch(jsonb);
drop function if exists public.upsert_raw_price_entry(text, text, text, text, text, text, text, date, text, jsonb, bigint, integer);

create function public.upsert_raw_price_entry(
  p_resource text, p_market text, p_state text, p_district text,
  p_commodity text, p_raw_variety text, p_raw_grade text,
  p_price_date date, p_content_hash text, p_payload jsonb,
  p_batch_id text, p_parser_version integer
)
returns table(entry_id bigint, is_new boolean)
language plpgsql
set search_path = public, extensions, pg_temp
as $$
declare v_id bigint;
begin
  select id into v_id
  from public.raw_price_entries
  where resource = p_resource
    and market = p_market
    and state = p_state
    and coalesce(district, '') = coalesce(p_district, '')
    and commodity = p_commodity
    and coalesce(raw_variety, '') = coalesce(p_raw_variety, '')
    and coalesce(grade, '') = coalesce(p_raw_grade, '')
    and price_date = p_price_date
    and content_hash = p_content_hash
  for update;

  if v_id is null then
    insert into public.raw_price_entries (
      resource, market, state, district, commodity, raw_variety, grade,
      price_date, payload, content_hash, parser_version,
      first_seen_batch_id, first_seen_at, last_seen_batch_id, last_seen_at, created_at
    ) values (
      p_resource, p_market, p_state, p_district, p_commodity, p_raw_variety, p_raw_grade,
      p_price_date, p_payload, p_content_hash, p_parser_version,
      p_batch_id, now(), p_batch_id, now(), now()
    ) returning id into v_id;
    return query select v_id, true;
  end if;

  update public.raw_price_entries
  set last_seen_batch_id = p_batch_id, last_seen_at = now()
  where id = v_id;
  return query select v_id, false;
end;
$$;

create function public.upsert_raw_price_entries_batch(p_entries jsonb)
returns table(entry_id bigint, is_new boolean)
language plpgsql
set search_path = public, extensions, pg_temp
as $$
begin
  return query
  select r.entry_id, r.is_new
  from jsonb_array_elements(coalesce(p_entries, '[]'::jsonb)) as elem(entry)
  cross join lateral public.upsert_raw_price_entry(
    elem.entry->>'resource', elem.entry->>'market', elem.entry->>'state',
    elem.entry->>'district', elem.entry->>'commodity', elem.entry->>'raw_variety',
    elem.entry->>'grade', (elem.entry->>'price_date')::date,
    elem.entry->>'content_hash', elem.entry->'payload', elem.entry->>'batch_id',
    (elem.entry->>'parser_version')::integer
  ) as r(entry_id, is_new);
end;
$$;

-- 2. Enforce raw-data immutability in the database, not merely by convention.
create or replace function public.prevent_raw_api_record_mutation()
returns trigger language plpgsql set search_path = public, extensions, pg_temp as $$
begin
  raise exception 'raw_api_records are immutable';
end;
$$;

create or replace function public.guard_raw_price_entry_update()
returns trigger language plpgsql set search_path = public, extensions, pg_temp as $$
begin
  if new.resource is distinct from old.resource
     or new.market is distinct from old.market
     or new.state is distinct from old.state
     or new.district is distinct from old.district
     or new.commodity is distinct from old.commodity
     or new.raw_variety is distinct from old.raw_variety
     or new.grade is distinct from old.grade
     or new.price_date is distinct from old.price_date
     or new.payload is distinct from old.payload
     or new.content_hash is distinct from old.content_hash
     or new.parser_version is distinct from old.parser_version
     or new.first_seen_batch_id is distinct from old.first_seen_batch_id
     or new.first_seen_at is distinct from old.first_seen_at
     or new.created_at is distinct from old.created_at then
    raise exception 'raw_price_entries content is immutable';
  end if;
  return new;
end;
$$;

drop trigger if exists trg_raw_api_records_no_update on public.raw_api_records;
drop trigger if exists trg_raw_api_records_no_delete on public.raw_api_records;
create trigger trg_raw_api_records_no_update before update on public.raw_api_records
  for each row execute function public.prevent_raw_api_record_mutation();
create trigger trg_raw_api_records_no_delete before delete on public.raw_api_records
  for each row execute function public.prevent_raw_api_record_mutation();
drop trigger if exists trg_raw_price_entries_guard_update on public.raw_price_entries;
drop trigger if exists trg_raw_price_entries_no_delete on public.raw_price_entries;
create trigger trg_raw_price_entries_guard_update before update on public.raw_price_entries
  for each row execute function public.guard_raw_price_entry_update();
create trigger trg_raw_price_entries_no_delete before delete on public.raw_price_entries
  for each row execute function public.prevent_raw_api_record_mutation();

-- 3. RLS provides defence in depth. Pipeline writes continue via service_role,
-- while anon/authenticated have no table grants or policies.
do $$
declare r record;
begin
  for r in select c.relname
           from pg_class c join pg_namespace n on n.oid = c.relnamespace
           where n.nspname = 'public' and c.relkind = 'r'
  loop
    execute format('alter table public.%I enable row level security', r.relname);
  end loop;
end;
$$;

-- 4. Move the extension out of the exposed schema and freeze search_path for
-- application functions. Extension-owned functions are deliberately skipped.
create schema if not exists extensions;
alter extension pg_trgm set schema extensions;
do $$
declare r record;
begin
  for r in
    select p.oid::regprocedure as signature
    from pg_proc p join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'public' and p.prokind = 'f'
      and not exists (
        select 1 from pg_depend d
        where d.classid = 'pg_proc'::regclass and d.objid = p.oid and d.deptype = 'e'
      )
  loop
    execute format('alter function %s set search_path = public, extensions, pg_temp', r.signature);
  end loop;
end;
$$;

-- 5. Cover every foreign key with an index unless an existing index already
-- begins with the same FK column list.
do $$
declare r record;
begin
  for r in
    select c.conrelid::regclass as rel, c.conname,
           array_agg(a.attname order by k.ordinality) as cols
    from pg_constraint c
    join unnest(c.conkey) with ordinality as k(attnum, ordinality) on true
    join pg_attribute a on a.attrelid = c.conrelid and a.attnum = k.attnum
    where c.contype = 'f' and c.connamespace = 'public'::regnamespace
    group by c.oid, c.conrelid, c.conname
  loop
    if not exists (
      select 1 from pg_index i
      where i.indrelid = r.rel
        and i.indisvalid
        and (i.indkey::smallint[])[1:cardinality(r.cols)] = (
          select array_agg(attnum::smallint order by ordinality)
          from unnest((select conkey from pg_constraint where conrelid = r.rel and conname = r.conname)) with ordinality as x(attnum, ordinality)
        )
    ) then
      execute format('create index %I on %s (%s)',
        left('idx_fk_' || replace(r.rel::text, '.', '_') || '_' || r.conname, 63),
        r.rel,
        array_to_string(array(select format('%I', c) from unnest(r.cols) c), ', '));
    end if;
  end loop;
end;
$$;

commit;
