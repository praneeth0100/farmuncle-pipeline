# ADR 0004: History backfill — failed days, outages, 10 years

**Status:** Accepted, merged (PRs #74, #75, #76)
**Date:** 2026-11-03
**Owner (module):** `farmuncle_pipeline/archive/history.py`, `farmuncle_pipeline/archive/tracker.py`,
`farmuncle_pipeline/archive/state.py`, `farmuncle_pipeline/ingestion/archive_history.py`

## Context

`archive_history` walks backwards from today in 10-day windows: pull each day from Resource 2, upload it to R2,
verify, delete it from the warehouse. On 2026-11-03 it had reached 19 Sep 2024 with 17 days `FAILED`, and:

- three failed days in a row stopped the whole run, and the untried days were recorded as "not tried", which
  used one of their tries;
- a failed day was retried up to 5 times, and days kept in the warehouse (live or manual prices) were retried
  every run with no limit;
- the oldest date was 1 Jan 2018, but the owner wants 10 years, and has checked the government feed holds them.

## Decision

1. **Oldest date: 1 Nov 2016** (`DEFAULT_OLDEST`).
2. **A failed day never stops the run.** It is recorded `FAILED` with its reason and the next day goes on.
3. **Three tries per day** (`MAX_TRIES_PER_DAY = 3`: the first try and two retries). After that the day stays
   `FAILED` in `archive_days` for a look and is not retried. `given_up_days` takes these days out of the retry
   list, including days whose warehouse rows were kept.
4. **Outage = a window in which no day went through.** Failure records are held until the window ends. If no day
   reached R2, the database or the feed was down, so the failures (and the retries before them) are not
   recorded, use no tries, and the window position does not move.
5. **An outage window is redone at most twice** (`OUTAGE_REDOS = 2`, counted in `archive_state.outage_tries`,
   reset when the window moves). On the third all-failed run its days are recorded `FAILED` like any other and
   the backfill moves on, so it cannot get stuck on one window.
6. The 17 days `FAILED` on 2026-11-03 were set to 3+ attempts by hand, so none is tried again.

## Consequences

- No 3-in-a-row stop: a database that times out on every day now spends a whole window (and the job's 5-hour
  budget) before the outage rule acts.
- A given-up day can be retried by lowering its `attempts` in `archive_days` below 3.
- At about 10 days per run, Sep 2024 back to Nov 2016 is about 290 windows: roughly two weeks while each run
  starts the next.
- Unchanged: a day in R2 is never replaced by one with fewer rows; nothing is deleted from the warehouse
  without a verified upload.
