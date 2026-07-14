# ADR 0001: Raw Storage Deduplication (page-row → entry-row)

**Status:** Accepted, implemented, live-verified
**Date:** 2026-07-13
**Owner (module):** `farmuncle_pipeline/core/raw_dedup.py`
**Related:** Master Build Spec §1 (invariants), §2 (never-do rules), §6/§6.1,
§13, §14, §19, §21 (Step 18.5, unnumbered), Appendix: Raw Storage
Deduplication

## Context

`raw_api_records` stored one row per fetched page (up to 500 mandi
entries bundled into a single row's payload). This was all-or-nothing:
there was no way to recognize that only, say, 60% of a page's entries
were unchanged from the last fetch — the whole page was either new or
it wasn't. Every 3-hourly `live_tick` run re-wrote full pages of data
that, in practice, barely changes tick to tick, since most mandis only
report a fresh price once a day at most.

This surfaced as a real problem, not a theoretical one: the same
per-page writes were also implicated in the timeout incident this
session diagnosed separately (batched RPC fix, see git history same
day) — large pages meant large round-trips, and large round-trips
meant runs creeping toward GitHub Actions' 15-minute timeout.
Deduplicating at the entry level, not just batching the RPC call,
was the other half of making raw storage cheap enough to run every
3 hours indefinitely without runaway row growth.

## Decision

**1. Identity key is the government's raw fields, not our resolved IDs.**
A "record" is identified by `(resource, market, state, district,
commodity, raw_variety, price_date)` exactly as the source API sends
them — not `mandi_id` / `crop_id`. Our identity-resolution layer
(fuzzy matching, aliases) is still being actively refined; if dedup
depended on it, a future improvement to mandi matching could silently
change what counts as "the same record" for old data. Keying off the
source's own raw text keeps the two systems independent, as intended
by the original bronze/silver/gold separation.

**2. Storage shape changes from one-row-per-page to one-row-per-entry.**
This is the real structural change. `raw_api_records` is frozen (no
new writes, existing rows untouched, not dropped) starting from the
switchover deploy. `raw_price_entries` is the new active raw layer,
one row per individual reported (market, commodity, variety, date)
observation.

**3. A content-hash "memory" table decides write vs. touch.**
For each incoming entry: hash its price payload
(`modal_price`/`min_price`/`max_price`), look up
`(resource, market, state, district, commodity, raw_variety,
price_date)` in `raw_price_entries`.
- Not present, or present with a different `content_hash` → insert/
  update in full, this is new information.
- Present with the same `content_hash` → touch `last_seen_at` /
  `last_seen_batch_id` only; no new row, no rewrite of `payload`.

**4. `raw_api_batches` is unaffected.** It remains one row per run
regardless of how much content in that run was actually new vs.
touched — it answers "did a fetch happen and how did it go," not
"how much data changed."

## Implementation

- Migration `raw_price_entries_dedup` (2026-07-13 11:22 UTC) — creates
  `raw_price_entries` with `first_seen_batch_id`/`last_seen_batch_id`/
  `first_seen_at`/`last_seen_at`/`content_hash`/`payload`.
- RPC `upsert_raw_price_entry` (atomic insert-or-touch, one call per
  record) added same day, later joined by `upsert_raw_price_entries_batch`
  (2026-07-13 16:30 UTC, plus a within-page dedup follow-up fix at
  16:30:47 UTC) once per-record round-trips were separately identified
  as a timeout risk. Both migrations are additive; neither edits a
  merged migration.
- Call sites updated: `live_tick.py`, `resource2_pipeline.py`,
  `retry_failed_pages.py` — all write through the batch RPC now.

## Verification (live, 2026-07-13)

- `raw_api_records`: last write 14:56 UTC, frozen since (0 writes
  after switchover deploy).
- `raw_price_entries`: 9,679 rows across 2 live_tick runs so far.
- **1,815 of those rows show `last_seen_batch_id != first_seen_batch_id`**
  — i.e., real repeat entries from the second run were correctly
  recognized as unchanged and touched in place, not duplicated. This
  is direct evidence the dedup logic works under real production
  traffic, not just in isolated testing.

## Consequences

- Raw storage growth is now proportional to *actual change volume*,
  not fetch frequency — the intended outcome.
- `raw_api_records` sits as dead weight until a deliberate decision is
  made to archive or drop it. Not urgent, but not free either (storage
  cost, and a second raw table a future reader has to know to ignore).
- Any future change to what counts as "the same entry" (e.g. adding a
  field to the identity key) is itself a schema change and would need
  its own ADR, per §19's Never-Do exception rule.

## Open / not done yet

- `raw_api_records` archival/drop decision — not made.
- Longer-horizon verification (a week or more of runs) to confirm
  dedup ratio holds steady rather than degrading as data volume grows.
- No test files exist yet covering this path (Phase D.5 is still at
  zero test files project-wide, per current top-of-mind status).
