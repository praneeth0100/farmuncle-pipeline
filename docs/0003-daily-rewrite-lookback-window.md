# ADR 0003: daily_rewrite Rolling Lookback Window (today-only → 3-day window)

**Status:** Accepted, implemented, live-verified
**Date:** 2026-07-14
**Owner (module):** `farmuncle_pipeline/ingestion/daily_rewrite.py`
**Related:** Master Build Spec §8 (precedence), §9 (government API
contracts), §16 (outage alert), §21 (Step 15), ADR 0001 (raw storage
dedup — this script writes through the same path)

## Context

`daily_rewrite.py`'s original docstring stated its target-date
reasoning explicitly: "§9 says [Resource 2's] records are 'complete
finalized daily records by evening.' ... this script targets today
(IST), not yesterday." The code matched: `target_date = today`, every
run, no exceptions.

This assumption was wrong, confirmed directly against real run
history, not inferred:

- Two consecutive `daily_rewrite` runs on 2026-07-13 (16:53:41 and
  16:57:27 UTC — both well into the evening, ruling out "ran before
  evening" as the explanation) **both processed exactly 0 rows.**
- In fact, per the `ingestion_batches` history checked while drafting
  this ADR, **`daily_rewrite` had processed 0 rows on every single run
  in its history up to this fix** — Resource 2 data for "today" was
  never actually available at the time this script ran, on any day
  observed. The original "by evening" assumption undercounted the
  real publication lag.

## Decision

**1. Replace the single-date target with a short rolling lookback
window, rather than guessing a fixed corrected offset.** The real lag
was observed to be at least 1 day, but the exact figure was not
independently confirmed prior to this fix, and government publication
timing is not guaranteed to be perfectly consistent day to day.
Hardcoding "shift by exactly 1 day" risks being wrong in the same way
the original "today" assumption was wrong, just by a different fixed
amount. A rolling window self-corrects regardless of the exact lag,
at the cost of repeating work on already-finalized dates.

**2. `REWRITE_LOOKBACK_DAYS = 3`.** Each evening run now attempts
`today`, `today - 1`, and `today - 2` in sequence, via the same
`ingest_resource2_for_date` function `historical_backfill.py` already
calls in a per-date loop (Step 16) — proven safe to call repeatedly
across dates within one script execution, since each call fully
completes (acquires and releases its own `ingestion_batches` /
`raw_api_batches` rows) before the next date's call begins.

**3. Repeating an already-finalized date is accepted as a safe,
intentional no-op, not waste to be optimized away.** `daily_rewrite`
writes are idempotent (invariant 7) and precedence-safe (§8) — running
`ingest_resource2_for_date` again against a date that's already fully
finalized re-confirms identical content (dedup handles this cheaply,
per ADR 0001) rather than causing incorrect duplicate state.

**4. The §16 outage alert stays keyed to `today`'s attempt only.**
Widening it to "any of the 3 dates in this run's window failed" would
make the alert fire on the *expected*, routine "today has no data
yet" outcome — which is not an outage, it's Resource 2 behaving
normally. Only `today`'s own 3-consecutive-run failure pattern is
alert-worthy, unchanged from the original design.

## Implementation

- `run_daily_rewrite` restructured: `for offset in range(REWRITE_LOOKBACK_DAYS)`
  loop replacing the single `ingest_resource2_for_date(ctx,
  target_date=today, ...)` call.
- A single date's exception is caught, logged, and does not abort the
  remaining window (mirrors `historical_backfill.py`'s existing
  per-date try/except shape) — except `JobAlreadyRunningError`, which
  aborts the whole window immediately, since a concurrent run already
  in progress means retrying here would just collide again.
- `todays_result` tracked separately from the loop's other dates,
  specifically so the §16 alert check (point 4 above) still only
  looks at `today`'s own outcome.
- No schema/migration change.

## Verification (live)

**Before fix** — every observed run, 0 rows, every time:

| Run (UTC) | Target date (old code) | Rows processed |
|---|---|---|
| 2026-07-13 16:53:41 | today (`2026-07-13`) | 0 |
| 2026-07-13 16:57:27 | today (`2026-07-13`) | 0 |

**After fix** — one script invocation now produces 3 separate
`ingestion_batches` rows, one per date in the window:

| Run (UTC) | Target date (new code) | Rows processed |
|---|---|---|
| 2026-07-14 02:46:29 | today (`2026-07-14`) | 0 — correctly still unpublished |
| 2026-07-14 02:47:22 | today − 1 (`2026-07-13`) | **18,303** |
| 2026-07-14 02:58:47 | today − 2 (`2026-07-12`) | **13,561** |

Cross-checked against `mandi_daily_prices` directly — before this fix,
`source = 'resource_2'` had **zero rows in the table, ever**. After:

| price_date | source | rows |
|---|---|---|
| 2026-07-13 | resource_2 | 16,151 |
| 2026-07-12 | resource_2 | 12,991 |

(Row counts differ from `rows_processed` above because
`rows_processed` counts every record the pipeline touched, including
ones filtered out by §8 precedence or identity-resolution failures
before reaching `mandi_daily_prices`.) `2026-07-13`'s Resource 1 rows
dropped from covering the full day to 2,063 remaining — the rest
correctly superseded by the newly-arrived Resource 2 data, §8
precedence working as designed the moment real Resource 2 data
existed to enforce precedence against.

## Consequences

- `daily_rewrite` now makes up to 3× the API/database work per
  evening run. Bounded and accepted — 2 of the 3 dates are typically
  cheap no-ops once the window "catches up" to steady state (only the
  newly-published date each day should ever produce real new content;
  the other 1-2 are re-confirmations).
- The *actual* Resource 2 publication lag is now empirically closer to
  1 day than "same evening" — this ADR does not update §9's prose in
  the Master Build Spec itself; that's a separate documentation
  action, not a code change, and is still outstanding.
- If the real lag ever exceeds `REWRITE_LOOKBACK_DAYS - 1` (2 days),
  this fix silently stops covering it again, the same failure mode as
  before, just with a larger margin. Not proven impossible, only
  observed not to have happened yet.

## Open / not done yet

- Master Build Spec §9's prose ("complete finalized daily records by
  evening") is now known to be inaccurate and has not yet been
  corrected to reflect the observed ~1-day lag.
- No confirmation of *why* the lag exists or whether it's consistent
  day-to-day — only that a 3-day window was sufficient to observe it
  resolve twice (2026-07-12 and 2026-07-13 both recovered in the same
  run).
- No test files exist yet covering this path (same project-wide gap
  noted in ADR 0001 and ADR 0002).
