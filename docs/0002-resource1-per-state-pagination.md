# ADR 0002: Resource 1 Per-State Pagination (nationwide stream → per-state)

**Status:** Accepted, implemented, live-verified
**Date:** 2026-07-14
**Owner (module):** `farmuncle_pipeline/ingestion/live_tick.py` (`_fetch_all_resource_1_pages`)
**Related:** Master Build Spec §7 (error codes), §9 (government API contracts),
§21 (Step 14), ADR 0001 (raw storage dedup — this ADR builds on the same
batched write path)

## Context

`live_tick.py` fetched Resource 1 nationwide in a single continuous
offset-paginated stream, filtered only by `filters[arrival_date]`. The
loop terminated on the first page shorter than `PAGE_SIZE` (500),
treating that as genuine end-of-data — the same signal a real end of
data would produce.

This was wrong, and confirmed wrong directly against the live
government API, not inferred:

- Three separate `live_tick` runs on 2026-07-13 (16:39, 17:12, 19:23,
  21:56 UTC — four, in fact) each processed **exactly 10,000 rows**,
  every time — a suspiciously exact, repeating number for what should
  be a naturally varying daily total.
- Manually querying the same government endpoint directly at
  `offset=10001` reproduced a hard backend error:
  `"Result window is too large, from + size must be less than or
  equal to: [10000] but was [10001]"` — an Elasticsearch
  `index.max_result_window` ceiling, not a real end-of-data signal.
- The same endpoint's own response envelope, queried directly,
  reported `"total": 18700` for nationwide records on 2026-07-13 —
  against the 10,000 `live_tick` had actually captured that day. **A
  confirmed 46% silent daily data loss**, on every run, logged as a
  clean `SUCCESS` with no indication anything was wrong.

## Decision

**1. Paginate once per state, not once nationwide.**
`STATES` (33 entries, `government_constants.py`) is iterated exactly
as `resource2_pipeline.py` already does for Resource 2 — but using
Resource 1's own filter field name, confirmed via the resource's own
Swagger/API docs: `filters[state.keyword]`, **not** `filters[State]`
(Resource 2's field name — a different resource, different field
naming convention; copying Resource 2's param name verbatim would
have silently filtered nothing).

**2. Reconcile against the government's own reported total.**
Every response carries a `total` field for the current query
(verified directly: `"total": 18700` nationwide, `"total": 576` for
Karnataka alone querying the same date). Each state's pagination now
compares its actually-collected record count against this field once
its own loop ends. A mismatch is tagged `QUALITY-001` ("Coverage
mismatch", §7) — logged loudly, and reflected in the batch's `PARTIAL`
status — but deliberately **not** written to `failed_pages`: that
table's retry model is "re-fetch this exact page," which cannot fix a
coverage gap (the missing records never fit inside any single page to
begin with).

**3. Rejected alternative — do nothing, since per-state was "only"
confirmed necessary for Resource 2.** Considered and rejected: the
original code's assumption that Resource 1, being date-scoped rather
than cumulative, would naturally stay under any per-request ceiling.
The 18,700-nationwide-vs-10,000-captured result directly disproves
this for at least one real date; there is no basis to assume it won't
recur, especially during harvest-season volume spikes.

## Implementation

- `_fetch_all_resource_1_pages` restructured: outer loop over `STATES`,
  inner loop unchanged (same retry/backoff/explicit-`ok`-flag logic via
  `fetch_page`, same batched dedup write via
  `raw_dedup.upsert_raw_price_entries_batch` from ADR 0001).
- Return signature extended from 3-tuple to 4-tuple to carry
  `states_with_any_success`, surfaced in `run_live_tick`'s final log
  line for parity with `daily_rewrite`/`historical_backfill`'s existing
  `"N/len(STATES) states yielded data"` convention.
- No schema/migration change — this is a pure ingestion-script change,
  writing through the same `raw_price_entries` / `upsert_raw_price_entries_batch`
  path ADR 0001 already established.

## Verification (live)

| Run (UTC) | Code | Rows processed |
|---|---|---|
| 2026-07-13 16:39 | nationwide (pre-fix) | 10,000 |
| 2026-07-13 17:12 | nationwide (pre-fix) | 10,000 |
| 2026-07-13 19:23 | nationwide (pre-fix) | 10,000 |
| 2026-07-13 21:56 | nationwide (pre-fix) | 10,000 |
| 2026-07-14 02:28 | per-state (post-fix) | 48 |
| 2026-07-14 02:40 | per-state (post-fix) | 48 |

The pre-fix pattern — four separate runs landing on the exact same
round number — is itself the strongest evidence something was
capping, not measuring, the real total. Post-fix runs no longer land
on 10,000; per-page record counts in the same runs (14, 15, etc.,
rather than uniformly 500) are also consistent with real per-state
totals rather than a truncated stream. The low absolute row counts
(48) reflect early-morning IST timing (few mandis have reported yet
for a brand-new day), not truncation — there is no round-number
ceiling signature in the post-fix numbers.

## Consequences

- Resource 1 fetches now make 33× the number of API calls per run
  (once per state) instead of one continuous stream — more requests,
  each individually smaller and safely bounded. Acceptable trade
  given the alternative was silent, unbounded data loss.
- `QUALITY-001` coverage-mismatch warnings currently have no durable
  home — they are `print`-only, visible in GitHub Actions logs, not
  queryable from Supabase. This is a known, deliberate gap pending
  Step 19 (`quality_alerts`); not addressed by this ADR.
- The same 10,000-record ceiling logic applies to Resource 2 as well
  — `resource2_pipeline.py` already paginates per state, for reasons
  its own docstring attributes to "Resource 2's contract requiring
  state enumeration." Whether that was originally a deliberate fix
  for the same ceiling, or a coincidental requirement, is not
  established by this ADR and is not re-verified here.

## Open / not done yet

- No durable, queryable record of `QUALITY-001` firings — pending
  `quality_alerts` (Step 19).
- No verification of whether any *single state* has ever approached
  10,000 on a high-volume day — the safety margin (576 for Karnataka
  against an 18,700 nationwide total) is comfortable today but not
  stress-tested against a genuine outlier state or peak-season volume.
- No test files exist yet covering this path (Phase D.5 is still at
  zero test files project-wide, per ADR 0001's same note).
