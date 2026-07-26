-- variety_id/grade_id on mandi_daily_prices have existed in the schema since
-- the 2026-07-25 rebuild (20260725160000_core_identity_and_price_schema.sql)
-- but were never populated -- record_processor.py only ever wrote the plain-
-- text variety/grade columns via resolve_variety/resolve_grade, and nothing
-- called find_or_create_variety/find_or_create_grade (both of which already
-- existed server-side, unused). Confirmed via code trace, not assumption:
-- this was NOT a bug, it was an explicit 2026-07-21 deferral documented in
-- resolve_grade's old docstring -- left as-is until the user decided
-- precision was worth the extra RPC calls (2026-07-26).
--
-- The application-side fix (record_processor.py now calls
-- identity.resolve_variety_id/resolve_grade_id for every new row, via the
-- ALREADY-NORMALIZED variety/grade text -- see identity_client.py) covers
-- everything ingested from here on. This migration is the one-time backfill
-- for rows that existed before that fix landed.
--
-- Uses the same already-stored normalized variety/grade text as input,
-- for consistency with the live code path (a blank source variety/grade
-- normalizes to the text "other" before storage, so this creates/reuses a
-- real per-crop "other" variety, and a real per-variety "other" grade,
-- rather than leaving those rows NULL).
--
-- Safe to re-run: both UPDATEs are scoped to `... is null`, so already-
-- backfilled rows are left untouched.

update mandi_daily_prices
set variety_id = find_or_create_variety(crop_id, variety)
where variety_id is null;

update mandi_daily_prices
set grade_id = find_or_create_grade(variety_id, grade)
where grade_id is null and variety_id is not null;
