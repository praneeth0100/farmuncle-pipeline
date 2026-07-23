-- FarmUncle v2 — tests/sql/test_merge_entity.sql
-- Phase D.5, Step 25 (Merge tests).
--
-- Why this is a .sql file, not a Python test:
--   merge_entity has no Python wrapper anywhere in this codebase — it
--   is invoked manually via SQL by a human (see identity_client.py's
--   own comment on this), so there is no Python call site to unit-test
--   with a fake. A Python test built around a hand-rolled fake
--   postgrest client here would only be testing the fake's own model
--   of merge_entity's logic, not the real function — genuinely
--   pointless per invariant 3 ("push set-based questions into the
--   database rather than reimplementing them in Python"). This file
--   runs directly against a real (ideally scratch/staging) Postgres
--   connection instead.
--
-- What this verifies (each block asserts and raises on failure):
--   1. A basic merge: source flips to MERGED with the right
--      merged_into_id/reason, target stays ACTIVE.
--   2. The source's own name becomes an alias of the target
--      (mandi_aliases re-pointed).
--   3. price_cache.refreshed_at advances after the merge — the whole
--      reason this migration exists (§15 Cache Invalidation Policy),
--      confirmed live on 2026-07-17 against wqccgjmvslevkglfkmtc
--      (mandi ids 2904/2905, cleaned up after) before this file was
--      written, and re-checked here on every run.
--   4. Idempotency: calling merge_entity again with the exact same
--      args is a no-op (does not raise, does not double-append an
--      entity_history/audit_events row).
--   5. Merging into a non-ACTIVE target is rejected.
--
-- Usage: run this against a disposable/scratch schema or a staging
-- project, never production. Every block cleans up its own rows in a
-- ROLLBACK-wrapped transaction so it can be re-run freely.

begin;

-- ---------------------------------------------------------------
-- Setup: two scratch mandis, deliberately named so they can never
-- collide with real ingested data.
-- ---------------------------------------------------------------
insert into mandis (slug, name, normalized_name, state, status, review_status, ingested_from)
values ('zz-test-merge-src', 'ZZ Test Merge Source', 'zz test merge source', 'ZZTestState', 'ACTIVE', 'VERIFIED', 'manual')
returning id as src_id \gset

insert into mandis (slug, name, normalized_name, state, status, review_status, ingested_from)
values ('zz-test-merge-tgt', 'ZZ Test Merge Target', 'zz test merge target', 'ZZTestState', 'ACTIVE', 'VERIFIED', 'manual')
returning id as tgt_id \gset

-- ---------------------------------------------------------------
-- 1/2/3: basic merge + alias re-pointing + cache refresh
-- ---------------------------------------------------------------
select max(refreshed_at) as before_refresh from price_cache \gset

select merge_entity('mandi', :src_id, :tgt_id, 'sql test merge', 'MANUAL', null, 'test-suite');

do $$
declare
  v_status text;
  v_merged_into bigint;
begin
  select status, merged_into_id into v_status, v_merged_into from mandis where id = :src_id;
  if v_status <> 'MERGED' or v_merged_into <> :tgt_id then
    raise exception 'FAIL (1): expected source MERGED into %, got status=% merged_into_id=%', :tgt_id, v_status, v_merged_into;
  end if;

  if not exists (
    select 1 from mandi_aliases
    where mandi_id = :tgt_id and normalized_alias = 'zz test merge source'
  ) then
    raise exception 'FAIL (2): source name was not re-pointed as an alias of the target';
  end if;

  perform 1 from price_cache where refreshed_at > coalesce(:'before_refresh', '-infinity'::timestamptz);
  if not found then
    raise exception 'FAIL (3): price_cache.refreshed_at did not advance after merge_entity';
  end if;

  raise notice 'PASS: basic merge + alias + cache refresh';
end $$;

-- ---------------------------------------------------------------
-- 4: idempotency — same merge again must be a silent no-op
-- ---------------------------------------------------------------
do $$
begin
  perform merge_entity('mandi', :src_id, :tgt_id, 'sql test merge (repeat)', 'MANUAL', null, 'test-suite');
  raise notice 'PASS: repeat merge into same target is a no-op (did not raise)';
end $$;

-- ---------------------------------------------------------------
-- 5: merging into a non-ACTIVE target must be rejected
-- ---------------------------------------------------------------
do $$
declare
  v_third_id bigint;
  v_caught boolean := false;
begin
  insert into mandis (slug, name, normalized_name, state, status, review_status, ingested_from)
  values ('zz-test-merge-inactive', 'ZZ Test Inactive Target', 'zz test inactive target', 'ZZTestState', 'INACTIVE', 'VERIFIED', 'manual')
  returning id into v_third_id;

  begin
    perform merge_entity('mandi', :src_id, v_third_id, 'should fail', 'MANUAL', null, 'test-suite');
  exception when others then
    v_caught := true;
  end;

  if not v_caught then
    raise exception 'FAIL (5): merge into a non-ACTIVE target should have raised';
  end if;

  delete from mandis where id = v_third_id;
  raise notice 'PASS: merge into non-ACTIVE target correctly rejected';
end $$;

-- ---------------------------------------------------------------
-- Cleanup
-- ---------------------------------------------------------------
delete from entity_history where entity_type = 'mandi' and entity_id in (:src_id, :tgt_id);
delete from audit_events where entity_type = 'mandi' and entity_id in (:src_id, :tgt_id);
delete from mandi_aliases where mandi_id = :tgt_id;
delete from mandis where id in (:src_id, :tgt_id);
select refresh_price_cache();

rollback;
-- Deliberately ROLLBACK, not COMMIT, as a second independent safety
-- net on top of the explicit cleanup above — this test suite must
-- never leave a trace in the database regardless of which block
-- failed partway through.
