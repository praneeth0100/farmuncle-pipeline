-- Applied directly against flxjrcbhmcuaynctokpv (Claude session, 2026-07-26).
-- variety_aliases already existed in the schema and find_or_create_variety
-- already checked it (approved=true) before creating new rows - it was
-- just empty (0 rows), so it never fired. This is the first real alias,
-- found via pg_trgm similarity search across all 905 varieties, scoped
-- per-crop (checked crop_id matched, not just text similarity globally).
--
-- Reviewed ~50 high-similarity candidate pairs by hand before deciding
-- what to merge. Most were REJECTED as genuinely different varieties
-- despite high text similarity - e.g. "cotton (ginned)" vs "cotton
-- (unginned)" (different products), "mtu-1001" vs "mtu-1008" (different
-- named cultivars), "basmati 1509" vs "basmati" (a specific certified
-- variety vs generic), "sona masuri (old)" vs "sona masuri new" (an
-- intentional, meaningful market distinction, not a typo). Only one pair
-- was a confident pure punctuation variant: "i.r. 64" vs "i.r.-64"
-- (similarity=1.0, Paddy(Common), crop_id=4) - IR-64 is a real, singular,
-- widely-grown rice variety; the hyphenated form is not a different
-- cultivar, just different punctuation of the same name.
--
-- LESSON LEARNED, IMPORTANT: adding the alias row ALONE did not fix
-- anything - verified live: find_or_create_variety(4,'i.r.-64') still
-- returned the duplicate's own id after the alias was inserted, because
-- find_or_create_variety checks for an EXACT match in `varieties` FIRST,
-- and only falls through to the alias table when no exact match exists.
-- Since "i.r.-64" already existed as its own established row (12 rows of
-- real price history under it, vs 32 under "i.r. 64"), the exact-match
-- branch always won and the alias was silently inert. The actual fix
-- required merging the pre-existing duplicate away, not just adding an
-- alias pointing at it - the alias only prevents this from happening
-- again for text that hasn't already been created as its own row.

-- Step 0: the alias itself (kept for future occurrences of this spelling).
insert into variety_aliases (variety_id, alias_name, normalized_alias, match_method, approved)
values (125, 'I.R.-64', 'i.r.-64', 'FUZZY', true);

-- Step 1: remap the 12 existing price rows from the duplicate variety
-- (id 194) and its grades to the canonical variety's (id 125) matching
-- grades, joined by normalized_name - all 3 grades under the duplicate
-- (faq/non-faq/local) already existed under the canonical variety too.
update mandi_daily_prices mdp
set variety_id = 125,
    grade_id = g125.id
from grades g194
join grades g125 on g125.variety_id = 125 and g125.normalized_name = g194.normalized_name
where g194.variety_id = 194
  and mdp.variety_id = 194
  and mdp.grade_id = g194.id;

-- Step 2: delete the now-orphaned grade rows under the duplicate variety.
delete from grades where variety_id = 194;

-- Step 3: delete the duplicate variety itself, so future exact-match
-- lookups for "i.r.-64" fall through to the alias from Step 0 instead of
-- re-matching this row.
delete from varieties where id = 194;

-- Verified post-migration: 44 rows on canonical variety (32+12, zero
-- lost), 0 rows/grades/variety remaining under the old duplicate id,
-- find_or_create_variety(4, 'i.r.-64') now correctly returns 125.
