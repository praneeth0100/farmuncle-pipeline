# FarmUncle v2 — Session Handoff Brief (2026-07-14)

**Purpose of this file:** if this chat session ends, paste this file + `FarmUncle_v2_Master_Build_Spec.md` into a new session and everything below is enough to continue exactly where this session left off — no re-explaining needed.

---

## ✅ Done and shipped today (all verified live, not just written)

1. **Raw storage deduplication** — `raw_api_records` (page-level, unbounded growth) replaced by `raw_price_entries` (content-addressed, entry-level). Confirmed live: 46,655+ rows, dedup ratio holding under real traffic. → **ADR 0001**, `docs/0001-raw-storage-deduplication.md`.

2. **Resource 1 per-state pagination fix** — nationwide single-stream fetch was silently truncating at exactly 10,000 records (government API's Elasticsearch `max_result_window` ceiling). Confirmed: real nationwide total on 2026-07-13 was 18,700 vs. 10,000 captured — **46% silent daily data loss**, every run, logged as clean `SUCCESS`. Fixed via per-state pagination (`filters[state.keyword]`) + `QUALITY-001` total-reconciliation check. → **ADR 0002**, `docs/0002-resource1-per-state-pagination.md`.

3. **`daily_rewrite` lookback window fix** — was targeting `today` only; Resource 2 was confirmed to **never** have same-day data (0 rows processed on every run, ever, prior to the fix). Fixed to a rolling 3-day window (`REWRITE_LOOKBACK_DAYS = 3`). Confirmed live: recovered 18,303 + 13,561 rows for 2026-07-13 and 2026-07-12 in one run — `mandi_daily_prices.source='resource_2'` went from **zero rows ever** to real, precedence-correct data. → **ADR 0003**, `docs/0003-daily-rewrite-lookback-window.md`.

4. **Repo cleanup** — all 3 ADRs committed and pushed (`docs/`, commit `e7dba0e`). 4 accidental terminal-output files identified and deleted (were never real project files — `less` pager captures and an empty redirect).

5. **Master Build Spec updated to match reality** — §6.1 (table registry), §14 (retention), §19 (ADR status), §21 (Phase D), §9 (Resource 1/2 corrections), closing "Next action" — all now reflect the above 3 fixes. This is the copy to keep using going forward.

---

## 🔴 New gaps discovered today (not yet built, not yet even started)

These are real, confirmed via direct Supabase queries this session — not assumptions:

| Gap | Confirmed state | Why it matters |
|---|---|---|
| **Geocoding** | `select count(*) filter (where latitude is not null) from mandis` → **0 of 2,033** | Columns (`latitude`/`longitude`/`location_confidence`) have existed since Step 6; nothing has ever populated them. Dead schema. |
| **RLS policies** | RLS enabled on all 18 tables; `select * from pg_policies` → **zero rows, any table** | Currently safe (deny-by-default), but the day the app connects, every query will return empty — no read policy exists yet. Cheap to fix now, will silently block later if forgotten. |
| **`price_cache`** | Still not built (known since earlier in session) | `mandi_daily_prices` is the de facto app-read table for now; fine at current scale, not the intended long-term design. |

---

## 🟡 In-progress / needs your decision to continue

**Step 19 — nightly quality audit job → `quality_alerts`.** Schema already confirmed ready:
- `coverage_reports` has `expected_count`/`actual_count`/`coverage_pct` — built for exactly this.
- `quality_alerts` has `error_code` + `coverage_report_id` — `QUALITY-001` (currently print-only in GitHub Actions logs, from ADR 0002) finally gets a durable, queryable home here.

**Open design decision, blocking — needs your call before code gets written:**
What counts as "expected" coverage each night? Two options:
- **(A) Re-fetch the government API's `total` field** for a spot sample — most accurate (this is literally how the 18,700-vs-10,000 bug was originally caught), costs real API calls nightly.
- **(B) Compare against your own trailing baseline** (same day last week / rolling 7-day average) — free, but wouldn't have caught the per-state bug until a week of drift.
- Leaning recommendation from last message: **A** for the actual coverage check, **B** as a secondary "is today unusual" signal. Not yet confirmed by you.

**Say this to resume:** *"Let's go with [A/B/both] for Step 19"* and I'll write the actual script + any migration needed.

---

## 📋 Full priority list for "production ready," in recommended order

1. **Step 19** (blocked on the A/B decision above) — closes the `QUALITY-001` durability gap from ADR 0002.
2. **RLS policies** — cheap, should happen before app connection is even attempted, easy to forget once other work resumes.
3. **Geocoding** — needs its own scoping conversation (data source for coordinates hasn't been discussed yet — government data doesn't include lat/long directly, so this needs an external geocoding approach decided first).
4. **`price_cache`** — build once Step 19 + RLS are settled, since app-connection readiness depends on both anyway.
5. **Phase D.5 (testing)** — 0 test files project-wide, still true, still not started.
6. **Phase E, Step 29** — run `historical_backfill.py` for real historical dates (script ready, unused).
7. **Phase F (rollback plan)**, **Phase G (ops runbook)** — not started, lower urgency until closer to real launch.
8. **`weekly_compress`/R2 archive** — `raw_api_records` (193 rows, frozen) sits as harmless dead weight; not urgent, but the intended long-term home for it per ADR 0001's "Consequences" section.

---

## How to resume in a new session

1. Upload `FarmUncle_v2_Master_Build_Spec.md` (the current one, already updated today) + this brief.
2. Say what you want to pick up — either the Step 19 decision above, or any item from the priority list.
3. Everything in this brief is independently verifiable against the live Supabase project (`wqccgjmvslevkglfkmtc`) and the GitHub repo (`praneeth0100/farmuncle-pipeline`) — a new session can re-check any claim here directly rather than trusting it blindly.
