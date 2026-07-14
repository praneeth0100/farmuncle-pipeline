# FarmUncle v2 — Master Build Specification (Final, Merged)
**This document is fully self-contained.** Paste this alone into a fresh session and everything needed to execute is here. Pair it with `FarmUncle_v2_Operations_Runbook.md` (separate file, not yet written — see Phase G) once the system is live.

**How to use this:** Read start to finish once. Then say **"code step N"** for whichever step you're ready for, and only that step gets built.

**Revision note (2026-07-11):** This revision updates status through Phase C (Steps 13–17, complete), the post-Phase-C repository restructure, and Phase D Step 18 (GitHub Actions automation, complete). See the new appendices at the end for full build logs. Everything else in this document is unchanged from the original.

**Revision note (2026-07-13):** This revision documents an out-of-sequence storage-optimization change (not one of the original 33 numbered steps): `raw_api_records`' unbounded per-page growth (16 MB / 863 rows — by far the dominant storage cost, vs <1 MB for every other table combined) is addressed with a new deduplicated, record-level raw store, `raw_price_entries`. See §6.1, §14, §2, and the new **Appendix: Raw Storage Deduplication** for full detail. Phase D, Step 19 (nightly quality audit) remains the next *numbered* step and was not affected by this change.

---

## 0. Context

FarmUncle is a React Native/Expo agricultural price app for Indian farmers, backed by Supabase ("Account 1" = live app data, "Account 2" = historical archive — **Account 2 does not exist yet, see §17 note below**) and eventually Cloudflare R2 (not yet populated). Two rounds of audit on the v1 backend found identity-resolution logic duplicated across 4 scripts with no normalization (3,044 ghost mandis, 230 uncategorized crops); a Postgres fix that was correctly built but never wired into production; a compression job whose delete step silently never fired; two backfill scripts colliding on shared progress state; and a failure-retry mechanism that can't survive GitHub Actions' ephemeral runners.

**Conclusion:** the individual design ideas were sound — the failure mode was drift between what was designed/fixed and what was actually deployed, plus no durable operational visibility. This spec makes that class of drift structurally impossible, and captures the operational knowledge (ownership, lifecycle, precedence, retention, concurrency, recovery, risk) normally only learned after months of running a pipeline in production.

### 0.1 Build target: brand-new Supabase account (superseded decision, 2026-07-09)

v2 is being built from scratch in a **new Supabase project/account** (`wqccgjmvslevkglfkmtc`), entirely separate from the existing "agro uncle" v1 project. This replaces the originally-planned in-place coexistence approach. The old project keeps serving the live app, untouched, until v2 is ready and validated.

---

## 1. System Invariants (the project's constitution)

1. Raw API records are immutable — never edited, never deleted.
2. No script inserts directly into `mandis` or `crops`. Ever.
3. All identity resolution happens through RPCs — never duplicated in Python.
4. Every write belongs to an ingestion batch.
5. Every mutation that changes a canonical entity creates an audit event.
6. Every failed API page is persisted in a table, never a local file.
7. Every production workflow is idempotent.
8. Every table with derived data carries lineage back to its batch/raw record.
9. No business logic is duplicated across scripts.
10. Every job must be replayable from raw data alone.

**Status note (2026-07-13, invariants 1 & 10):** `raw_price_entries` (see §6.1) satisfies both invariants under a content-addressed model rather than a one-row-per-fetch model: a repeat observation of already-known content never edits or deletes the existing row — it only updates `last_seen_batch_id`/`last_seen_at` on it (invariant 1 holds), and every batch is still fully traceable back to real raw content via `first_seen_batch_id`/`last_seen_batch_id` (invariant 10 holds) even though most batches no longer physically duplicate content they've already seen.

---

## 2. Never-Do Rules (hard guardrails — no exceptions without an ADR)

- Never edit `raw_api_records`. (As of 2026-07-13, `live_tick.py`/`resource2_pipeline.py`/`retry_failed_pages.py` no longer *write* to `raw_api_records` at all — see §6.1 — but the rule, and the existing rows, stand unchanged: nothing already written to it is ever edited or deleted.)
- Never edit `raw_price_entries`' `payload`/`content_hash`/`first_seen_*` columns once written — only `last_seen_batch_id`/`last_seen_at` may be touched, and only via `upsert_raw_price_entry`, never a direct `UPDATE`.
- Never bypass identity RPCs for mandi/crop resolution.
- Never write directly into `price_cache` outside `refresh_price_cache`.
- Never delete canonical entities (`mandis`/`crops`) — lifecycle them instead (§4.4).
- Never rerun a merged migration manually.
- Never merge entities without an `entity_merged` audit event.
- Never disable a quality check to unblock a deploy.
- Never change the parser without bumping `parser_version`.
- Never deploy to production without passing replay tests.
- Never let two instances of the same job run concurrently without the advisory lock (§12).

Any exception requires a new ADR (§17) explaining why the rule doesn't apply, reviewed before the exception ships.

**Phase C note:** §2's "no business logic duplicated across scripts" rule drove a chain of extractions through Steps 14–17 — `batch_lifecycle.py`/`identity_client.py`/`quality_scoring.py` (Step 14), `resource_client.py`/`price_writer.py` (Step 15), `resource2_pipeline.py` (Step 16), `record_processor.py` (Step 17). See Appendix: Phase C Build Log for the full reasoning trail — the pattern each time was "a second real caller needs identical logic," not speculative abstraction ahead of need.

---

## 3. ID & Versioning Strategy

| Entity | ID type | Reason |
|---|---|---|
| `mandis`, `crops` | `bigint` (existing slug kept as secondary `slug` column) | Stable, human-debuggable |
| `ingestion_batches`, `raw_api_batches` | `ULID` (stored as `text`) | Sortable by time, globally unique |
| High-volume rows (prices, logs) | `bigint identity` | Cheapest for high insert volume |

**Version columns:** `schema_version` (batches), `taxonomy_version` (crops), `normalization_version` (aliases/matches/prices), `rpc_version` (identity RPC calls), and **`parser_version`** (on raw→parsed transformation).

---

## 4. Naming Conventions

| Object | Convention | Example |
|---|---|---|
| Tables | `snake_case`, plural | `mandi_aliases` |
| Columns | `snake_case` | `normalized_name` |
| Enums | `UPPER_CASE` | `NEEDS_REVIEW` |
| Functions/RPCs | `verb_object()` | `find_or_create_mandi()` |
| Indexes | `idx_<table>_<column(s)>` | `idx_mandis_status` |
| Check constraints | `chk_<table>_<rule>` | `chk_batches_status_notnull` |
| Foreign keys | `fk_<table>_<ref_table>` | `fk_prices_mandis` |

---

## 5. Module Structure

```
identity/          → find_or_create_mandi, find_or_create_crop, merge_entity RPCs, alias tables
normalization/      → normalize_market_name, normalize_crop_name, normalize_variety, normalize_unit
ingestion/          → live_tick.py, daily_rewrite.py, historical_backfill.py, retry_failed_pages.py
quality/            → coverage checks, quality_alerts, nightly audit job
storage/            → weekly_compress.py, R2 archive writer, historical_jobs
```

**Post-Phase-C note:** `identity/`/`normalization/` are RPC-only (live in Postgres, not Python files). The Python side of `ingestion/` now also has a `core/` sibling package holding everything Never-Do §2 required to be shared rather than duplicated across the four ingestion scripts. See Appendix: Repository Restructure for the actual on-disk layout, which is:

```
farmuncle_pipeline/
├── config.py, government_constants.py, ingest_common.py
├── core/          (config_validator, startup_validation, logging_utils,
│                    batch_lifecycle, identity_client, quality_scoring,
│                    resource_client, price_writer, record_processor,
│                    resource2_pipeline)
└── ingestion/     (live_tick, daily_rewrite, historical_backfill,
                     retry_failed_pages)
```

---

## 6. Data Layers & Schema Design

```
raw (bronze)                 canonical (silver)            derived (gold)              metadata (ops)
  raw_api_batches               mandis                        price_cache                 ingestion_batches
  raw_api_records                crops                          search_index (future)       api_call_logs
  raw_price_entries              mandi_daily_prices              analytics (future)          failed_pages
                                  mandi_aliases / crop_aliases                                coverage_reports
                                                                                               quality_alerts
                                                                                               compression_runs
                                                                                               historical_jobs
                                                                                               audit_events
                                                                                               entity_history
```

**`raw_api_records` (frozen 2026-07-13) / `raw_price_entries` (✅ built 2026-07-13, active):** as of 2026-07-13, new raw writes go to `raw_price_entries` (deduped, record-level) instead of `raw_api_records` (page-level, no dedup). `raw_api_records`' existing rows are untouched and permanent — it isn't dropped, just no longer written to. See §6.1 for the full comparison and the **Appendix: Raw Storage Deduplication** for why.

### 6.1 Table registry

| Table | Purpose | Source of truth? | Regenerable? | Owner module |
|---|---|---|---|---|
| `raw_api_records` | Verbatim government API responses, **one row per fetched page** — ⏸ **frozen 2026-07-13: existing rows kept forever, no new writes** (see §6.1 note below) | Yes, for everything written before 2026-07-13 | N/A | ingestion (retired write path) |
| `raw_price_entries` | Deduplicated raw price observations, **one row per distinct (market, commodity, variety, date, resource, content) combination ever seen** — repeats update `last_seen_batch_id`/`last_seen_at` instead of inserting a duplicate — ✅ **built 2026-07-13, active write path for `live_tick.py`/`resource2_pipeline.py`/`retry_failed_pages.py`** | **Yes — ultimate source of truth, going forward** | N/A | ingestion (`raw_dedup.py`) |
| `mandis` / `crops` | Canonical entities | Yes | Yes, by replaying raw through identity RPCs | identity |
| `mandi_aliases` / `crop_aliases` | Name variants → canonical entity | Yes | Partially | identity |
| `mandi_daily_prices` | One record per `(mandi, crop, variety, date)` | Yes, once rewrite has run | Yes, from raw | ingestion |
| `price_cache` | Disposable app-read optimization only | No | Yes, always | ingestion |
| `ingestion_batches`, `api_call_logs`, `failed_pages` | Operational run history | Yes (as history) | No | ingestion |
| `coverage_reports`, `quality_alerts` | QA output | Yes (as record) | Yes, recomputable | quality |
| `compression_runs`, `historical_jobs` | Storage tier history | Yes | No | storage |
| `audit_events`, `entity_history` | Full mutation trail | Yes | No | identity |

### 6.2 Canonical reference tables
- `mandis`: `slug`, `name`, `normalized_name`, `state`, `district`, `taluk`, `latitude`, `longitude`, `location_confidence`, `merged_into_id`, `merge_reason`, `merged_at`, `status`, `ingested_from`, `first_seen_at`, `last_seen_at`, `last_verified_at`, `review_status`
- `crops`: `name`, `normalized_name`, `category`, `taxonomy_version`, `unit`, `review_status`, `ingested_from`, `first_seen_at`, `last_seen_at`, `last_verified_at`, `merged_into_id`

**Constraints:** alias must reference an `ACTIVE` entity · `MERGED` entities cannot receive new price rows (enforced via trigger, built Step 9) · batch `status` NOT NULL · `NEEDS_REVIEW` entities excluded from app-facing views.

### 6.3 Business keys
- Mandi: `(normalized_name, state, district)` — **live-verified `uq_mandis_business_key`; see Appendix: Phase C Build Log for a real bug this caught (identity cache keyed only on name+state, missing district)**
- Crop: `(normalized_name)`
- Price row: `(mandi_id, crop_id, variety, date)` — enforced via `uq_prices_business_key`

### 6.4 Full entity lifecycle
```
AUTO_CREATED → NEEDS_REVIEW → VERIFIED → ACTIVE → MERGED → ARCHIVED
                            ↘ REJECTED → (deleted only if never received price data)
```

### 6.5 Merge policy
Aliases re-pointed · coordinates: better `location_confidence` wins, logged in `entity_history` · price rows re-pointed, collisions keep target's row · `price_cache` invalidated and rebuilt · single `entity_merged` audit event mandatory. `merge_method` (`EXACT|NORMALIZED|FUZZY|MANUAL`) + `merge_confidence` (0–1) recorded on every merge.

### 6.6 Data ownership

| Data | Owner process | Who else may modify it |
|---|---|---|
| `mandis` / `crops` rows | Identity RPCs (`find_or_create_*`) | `merge_entity` only |
| `mandi_daily_prices` | `daily_rewrite` | `live_tick` (today-only, lower precedence — **§8 precedence now actually enforced in code, see `price_writer.filter_rows_by_precedence`, Appendix: Phase C Build Log**) |
| `price_cache` | `refresh_price_cache` | Nobody |
| `quality_alerts` | Nightly audit job | `daily_rewrite` also writes here now (§16 outage alert — see Appendix) |
| `audit_events` / `entity_history` | Identity RPCs / `merge_entity` | Append-only |
| `failed_pages` | Ingestion scripts (on failure) | `retry_failed_pages` (on success) |

### 6.7 Derived tables
`price_cache`, future `search_index`/`analytics`. Always droppable/rebuildable, never back these up.

### 6.8 Index strategy
- `mandis`: `PK(id)` · `UNIQUE(normalized_name, state, district)` · `idx_mandis_status` · `idx_mandis_review_status` · `idx_mandis_last_seen_at`
- `crops`: `PK(id)` · `UNIQUE(normalized_name)` · `idx_crops_status` · `idx_crops_review_status`
- `mandi_daily_prices`: `PK(id)` · `UNIQUE(mandi_id, crop_id, variety, date)` · `idx_prices_date` · `idx_prices_mandi_crop` — ✅ **built Step 9**
- `mandi_aliases` / `crop_aliases`: `UNIQUE(normalized_alias)` · `idx_aliases_entity_id`
- `failed_pages`: `idx_failed_pages_status` · `idx_failed_pages_batch_id` — ✅ **built Step 7**
- `ingestion_batches`: `idx_ingestion_batches_status` · `idx_ingestion_batches_job_name` · `idx_ingestion_batches_started_at` — ✅ **built Step 7**; **`uq_ingestion_batches_running_job` (partial unique on job_name WHERE status='RUNNING') — ✅ built Step 14, live-verified as the actual §12 concurrency mechanism**
- `api_call_logs`: `idx_api_call_logs_batch_id` · `idx_api_call_logs_status` · `idx_api_call_logs_called_at` · `idx_api_call_logs_error_code` — ✅ **built Step 8**
- `compression_runs`: `idx_compression_runs_status` · `idx_compression_runs_week_start_date` — ✅ **built Step 10**
- `historical_jobs`: `idx_historical_jobs_status` · `idx_historical_jobs_compression_run_id` — ✅ **built Step 10**
- `coverage_reports`: `idx_coverage_reports_report_date` · `idx_coverage_reports_batch_id` — ✅ **built Step 11**
- `quality_alerts`: `idx_quality_alerts_severity` · `idx_quality_alerts_created_at` · `idx_quality_alerts_status` · `idx_quality_alerts_batch_id` — ✅ **built Step 11**
- `audit_events`: `idx_audit_events_batch_id` · `idx_audit_events_entity_id` (composite `entity_type, entity_id`) — ✅ **built Step 12**
- `entity_history`: `idx_entity_history_batch_id` · `idx_entity_history_entity_id` (composite `entity_type, entity_id`) — ✅ **built Step 12**

### 6.9 Foreign key policy

| Relationship | Behavior | Reason |
|---|---|---|
| `mandi_daily_prices.mandi_id → mandis.id` | `RESTRICT` | ✅ built Step 9 |
| `mandi_daily_prices.crop_id → crops.id` | `RESTRICT` | ✅ built Step 9 |
| `mandi_daily_prices.batch_id → ingestion_batches.id` | `RESTRICT` | Lineage (invariant 8) — ✅ built Step 9 |
| `mandi_daily_prices.raw_api_batch_id → raw_api_batches.id` | `RESTRICT` | Lineage — ✅ built Step 9 |
| `api_call_logs.batch_id → ingestion_batches.id` | `RESTRICT` | ✅ built Step 8 |
| `failed_pages.batch_id → ingestion_batches.id` | `RESTRICT` | Never delete a batch with unresolved failures — ✅ built Step 7 |
| `mandi_aliases.mandi_id → mandis.id` | `CASCADE` | Alias meaningless without parent |
| `audit_events.entity_id → mandis/crops.id` | `SET NULL` | Preserve audit trail (polymorphic — enforced at RPC level, not a literal FK; see `idx_audit_events_entity_id`) |
| `historical_jobs.compression_run_id → compression_runs.id` | `RESTRICT` | Lineage from R2 write attempt back to its compression decision — ✅ built Step 10 |
| `coverage_reports.batch_id → ingestion_batches.id` | `RESTRICT` | Correlate coverage check to the batch it audited — ✅ built Step 11 |
| `quality_alerts.batch_id → ingestion_batches.id` | `RESTRICT` | ✅ built Step 11 |
| `quality_alerts.coverage_report_id → coverage_reports.id` | `RESTRICT` | ✅ built Step 11 |
| `audit_events.batch_id → ingestion_batches.id` | `RESTRICT` | Correlates event to the run that caused it — ✅ built Step 12 |
| `entity_history.batch_id → ingestion_batches.id` | `RESTRICT` | Correlates diff to the run that caused it — ✅ built Step 12 |

Default for anything not listed: `RESTRICT`.

---

## 7. Data Dictionary

**`mandis.location_confidence`** — `EXACT` · `APMC` · `DISTRICT` · `STATE` · `UNKNOWN`
**`mandis.status` / `crops.status`** — `ACTIVE` · `MERGED` · `INACTIVE` · `UNKNOWN`
**`mandis.review_status` / `crops.review_status`** — `AUTO_CREATED` · `NEEDS_REVIEW` · `VERIFIED` · `REJECTED`
**`quality_alerts.severity`** — `LOW` · `MEDIUM` · `HIGH` · `CRITICAL`
**`quality_alerts.status`** — `OPEN` · `ACKNOWLEDGED` · `RESOLVED` — ✅ **built Step 11**
**`ingestion_batches.status`** — `RUNNING` · `SUCCESS` · `PARTIAL` · `FAILED`
**`compression_runs.status`** — `PENDING` · `WRITTEN` · `VERIFIED` · `DELETED` · `FAILED`
**`historical_jobs.status`** — `PENDING` · `RUNNING` · `SUCCESS` · `FAILED` — ✅ **built Step 10**
**`failed_pages.status`** — `PENDING` · `RESOLVED` — ✅ **built Step 7; note: no `PERMANENTLY_FAILED` state and no retry-count column — see `retry_failed_pages.py`'s docstring (Step 17) for the operational implication**
**`merge_method`** — `EXACT` · `NORMALIZED` · `FUZZY` · `MANUAL`

**Error codes:** `INGEST-001` API timeout · `INGEST-002` Pagination failure · `IDENTITY-001` No mandi match found · `IDENTITY-002` Duplicate alias detected · `QUALITY-001` Coverage mismatch · `STORAGE-001` Compression verification hash mismatch

**Quality score components (defined at Step 9 build):** `mandi_daily_prices.quality_score` (numeric 0–1) is a composite stored alongside `quality_components` (jsonb breakdown), e.g. `source_confidence`, `completeness`, `entity_verified`, `price_sanity`. ✅ **Computation logic built Step 14 (`quality_scoring.py`) — unweighted mean of the four components, `source_confidence` per §8 (manual=1.0, resource_2=0.9, resource_1=0.7).**

---

## 8. Authoritative Source Precedence

```
1. Manual corrections (human-approved, always wins)
2. Resource 2 (evening-finalized, authoritative government data)
3. Resource 1 (live feed, best-effort, today only)
4. Auto-created / review-queue defaults (lowest)
```

✅ **Enforced in code as of Step 15/16 — `price_writer.filter_rows_by_precedence`, called by both `live_tick.py` and `daily_rewrite.py`/`historical_backfill.py`/`retry_failed_pages.py` before every upsert.** This was NOT enforced at Step 14 (Resource 2 data didn't exist yet); building `daily_rewrite.py` at Step 15 surfaced the gap and it was closed symmetrically in both directions (resource_2 must not overwrite manual; resource_1 must not overwrite resource_2 or manual).

---

## 9. Government API Contracts

**Resource 1 (live feed):** today's arrivals, intraday updates; historical-date filter honoring unconfirmed; special-character name issues (e.g. `&` in "F&V"); pagination `PAGE_SIZE=500`, short page = end-of-data, but failed-after-retries currently indistinguishable from genuine end — must fix in v2's `fetch_page` (explicit `ok` flag). Tag `INGEST-002`. ✅ **Built Step 14 (`resource_client.fetch_page`), live-verified.** ⚠️ **Correction (2026-07-14, ADR 0002):** a single nationwide offset stream was confirmed to silently truncate at exactly 10,000 records (government API's Elasticsearch `index.max_result_window` ceiling) — real nationwide total on 2026-07-13 was 18,700 against a captured 10,000, a confirmed 46% silent daily loss logged as clean `SUCCESS`. Fixed via per-state pagination (`filters[state.keyword]`) plus a `total`-field reconciliation check, tagged `QUALITY-001`. See ADR 0002 for full verification.

**Resource 2 (authoritative daily + historical):** ~~complete finalized daily records by evening~~ **correction (2026-07-14, ADR 0003): this was wrong** — confirmed via real run history that `daily_rewrite` processed 0 rows on every single run prior to the fix, because Resource 2 data for "today" was never actually available same-evening as originally assumed; the real lag is empirically closer to 1 day. Not available same-day; rate limits/latency not yet measured — add to `api_call_logs` (✅ table built Step 8) from day one. ✅ **Built Step 15/16 — per-state pagination (`government_constants.STATES`) since Resource 2 requires enumerating states, unlike Resource 1.** ✅ **`daily_rewrite` fixed to a rolling 3-day lookback window (`REWRITE_LOOKBACK_DAYS`) instead of a single `today` target — see ADR 0003 for full verification, including recovery of 2026-07-12 and 2026-07-13's previously-never-captured Resource 2 data.**

---

## 10. Known Assumptions
1. Government commodity IDs/names don't change identity over time.
2. One mandi reports at most one authoritative price per crop+variety+day (post-rewrite).
3. Resource 2's historical data, once published, doesn't change retroactively. ✅ **This assumption is load-bearing for `historical_backfill.py` (Step 16) — a backfilled date is treated identically to any other resource_2 write, no special "historical mode" logic.**
4. Absence from Resource 2 for a commodity+day means no trade, not a dropped record.
5. GitHub Actions runners are fully ephemeral. ✅ **Directly informs `retry_failed_pages.py`'s design (Step 17) — no in-memory retry state survives between runs, everything durable lives in `failed_pages`.**

---

## 11. Risk Register

| Risk | Impact | Mitigation |
|---|---|---|
| Government API changes schema/field names | High | Raw immutable storage + `parser_version` |
| Supabase (Account 1) outage | High | Replay batches from raw data |
| R2 unavailable | Low | Retry historical writes, `historical_jobs` tracks independently — ✅ table built Step 10 |
| Wrong alias/merge | Medium | `entity_history` + re-pointable |
| Taxonomy/category scheme changes | Medium | `taxonomy_version` |
| Resource 2 outage (multi-day) | Medium | Recovery procedure §16 — ✅ **implemented Step 15: `daily_rewrite.py` raises a `quality_alerts` HIGH row after 3 consecutive FAILED runs** |
| Two same-name jobs run concurrently | Medium | Advisory locks, §12 — ✅ **implemented as a partial unique index, Step 14, live-verified** |
| Silent data growth past storage budget | Medium | `compression_runs` verification hash + monitoring — ✅ table built Step 10 |
| PAGE_SIZE changes between a page's original failure and its retry | Low | Known, documented, unmitigated — see `retry_failed_pages.py`'s docstring (Step 17); would need a schema change (ADR) to fully close |

---

## 12. Concurrency Rules
`live_tick` and `daily_rewrite` may run concurrently (different date ranges). Two instances of the *same* job must not — advisory lock keyed on `job_name`. `weekly_compress` must never run concurrently with `daily_rewrite` on overlapping dates.

✅ **Implemented as `uq_ingestion_batches_running_job`, a partial unique index on `ingestion_batches(job_name) WHERE status='RUNNING'` (Step 14) — a literal Postgres advisory lock was deliberately rejected since Supabase's PostgREST pooler doesn't guarantee the same connection per request. Live-verified: `live_tick`/`daily_rewrite`/`historical_backfill`/`retry_failed_pages` all use distinct `job_name` values and were confirmed to run concurrently without conflict; two instances of the same job_name were confirmed to conflict correctly.** Also now enforced at the GitHub Actions layer (Step 18) via `concurrency:` groups per workflow, `cancel-in-progress: false` — deliberately not cancelling, since a cancelled run is SIGKILLed before its own exception handler can close the batch out of RUNNING, which would leave a stale row blocking every future run.

---

## 13. RPC Contracts

| RPC | Input | Output | Side effects | Idempotent? | Transactional? | Retry-safe? |
|---|---|---|---|---|---|---|
| `find_or_create_crop` | `p_name, p_unit, p_source` | `crop_id` | Insert into `crops`/`crop_aliases`, `entity_history` | Yes | Yes | Yes |
| `find_or_create_mandi` | `p_name, p_state, p_district, p_lat, p_lng, p_source` | `mandi_id` | Insert into `mandis`/`mandi_aliases`, `entity_history` | Yes | Yes | Yes |
| `refresh_price_cache` | none | none | Upserts `price_cache` | Yes | Yes | Yes |
| `merge_entity` | `entity_type, source_id, target_id, reason, merge_method, merge_confidence` | success/fail | Re-points aliases/prices, `audit_events` + `entity_history` | No — explicit no-op if already merged | Yes | Yes |
| `upsert_raw_price_entry` ✅ **built 2026-07-13** | `p_resource, p_market, p_state, p_district, p_commodity, p_raw_variety, p_price_date, p_content_hash, p_payload, p_batch_id, p_parser_version` | `entry_id, is_new` (`is_new=false` on a repeat of already-known content) | Insert into `raw_price_entries`, or touch `last_seen_batch_id`/`last_seen_at` on an existing row (`xmax`-based `ON CONFLICT DO UPDATE`) | Yes | Yes | Yes |

✅ **All RPC signatures live-verified against `pg_proc` as of the Phase C production audit — exact parameter names confirmed to match every Python caller (`identity_client.py`). `refresh_price_cache` is not called by any Phase C script — that table/RPC don't exist in the live schema yet (see §25).**

---

## 14. Data Retention & Backup Policy

| Data | Retention |
|---|---|
| `raw_api_records` (frozen 2026-07-13) / `raw_api_batches` | Forever (pre-2026-07-13 rows only; no new `raw_api_records` rows going forward) |
| `raw_price_entries` | Forever — but content-addressed, so this is now *distinct values ever observed*, not *fetches ever made*; growth is driven by real price changes, not by run frequency |
| `ingestion_batches`, `audit_events`, `entity_history` | Forever |
| `api_call_logs`, `coverage_reports` | 2 years |
| `quality_alerts` | Forever |
| `compression_runs` | Forever |
| `mandi_daily_prices` (Account 1) | 30 days rolling |
| Weekly aggregates (Account 2 / R2) | 10 years |
| `price_cache` | N/A, always current |

Backup: daily automated Supabase backup · weekly schema export · monthly R2 verification · quarterly restore drill.

**Status note:** the 30-day-rolling enforcement and the Account 2/R2 weekly-aggregate pipeline (`weekly_compress`) are **not built yet** — Phase C only populates `mandi_daily_prices`; nothing currently prunes or archives it. See §25/Next Steps. **Update 2026-07-13:** `raw_price_entries` (see §6.1) removes most of the *urgency* behind `weekly_compress` — the dominant, unbounded-growth cost (`raw_api_records`, 16 MB / 863 rows as of 2026-07-13) is now flat going forward — but `weekly_compress`/R2/Account 2 themselves are still not built, and `mandi_daily_prices`' 30-day rolling rule is still unenforced.

---

## 15. Cache Invalidation Policy
Refresh after every `daily_rewrite` · after `live_tick` (affected pairs, last 2 days) · immediately after `merge_entity` · never on plain taxonomy correction alone (but `taxonomy_version` bump triggers cache metadata refresh).

**Status note:** unchanged from original — `refresh_price_cache` / `price_cache` don't exist yet, so nothing in Phase C actually calls this policy yet. Reserved for whenever that table is built.

---

## 16. Recovery Procedures
**Resource 2 unreachable 3+ days:** `live_tick` continues; `daily_rewrite` pauses, marks batch `FAILED`, raises `quality_alert` (HIGH). Backlog processes oldest-first on return. `weekly_compress` skips weeks with unresolved `FAILED` batches.

✅ **Implemented Step 15.** A single run is marked `FAILED` (not `PARTIAL`) only on a genuine total outage (zero states yielded any successful page) — ordinary partial page failures still produce `PARTIAL` and land in `failed_pages`, per invariant 6. After a `FAILED` close, `daily_rewrite.py` checks the last 3 `ingestion_batches` rows for its own `job_name`; if all 3 are `FAILED`, it writes the `quality_alerts` HIGH row. Live-verified against real batch history and a real insert. "Backlog processes oldest-first" is NOT implemented as an automated procedure — `historical_backfill.py` (Step 16) can be pointed at the backlog manually, but nothing triggers that automatically yet.

**General rollback:** stop v2 workflows → restore v1 workflows → repoint app to v1 → restore archived snapshot if touched → replay affected batches. **Not exercised or automated — still a manual procedure (Phase F, not started).**

---

## 17. Configuration, Secrets & Startup Validation

**Centralized config (✅ table `system_config` built Step 5b):** `PAGE_SIZE`, `MAX_RETRIES`, `RETRY_DELAY`, `FUZZY_THRESHOLD` (=0.75), `MERGE_THRESHOLD`, `API_TIMEOUT`, `BATCH_SIZE`, `QUALITY_THRESHOLD`, plus `API_BASE_RESOURCE_1`/`API_BASE_RESOURCE_2` (added Step 14 — 10 seed rows total, up from the original 8).

**Secrets — GitHub Secrets only:** `DATA_GOV_API_KEY`, `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `HISTORY_SUPABASE_URL`, `HISTORY_SUPABASE_SERVICE_KEY`, `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`

**Status note (important, as of 2026-07-11):** only `DATA_GOV_API_KEY`/`SUPABASE_URL`/`SUPABASE_SERVICE_KEY` are actually consumed by any Phase C script today. `HISTORY_SUPABASE_URL`/`HISTORY_SUPABASE_SERVICE_KEY` are for "Account 2," which **does not exist yet** — no second Supabase project has been created. `R2_ACCOUNT_ID`/`R2_ACCESS_KEY_ID`/`R2_SECRET_ACCESS_KEY` are for `weekly_compress`'s archive writer, also **not built yet**. All 8 are still required to be *present* (even as placeholder values) because `config.Secrets.from_env` validates all 8 unconditionally — this was a deliberate Step 13 design choice (validate the full eventual secret surface up front) that now means placeholder values are needed for 5 of the 8 until Phases E/storage are actually built.

**Startup validation:** every script fails fast before doing any work if a secret is missing, DB unreachable, a dependent table doesn't exist, RPC/schema version mismatch. ✅ **Built Step 13 (`config.py`/`config_validator.py`/`startup_validation.py`), live-verified.**

---

## 18. Logging Standard
No free-form log lines. Every log line: `timestamp · batch_id · job_name · resource · page · duration · rows · status · error_code`. ✅ `api_call_logs` table built Step 8 implements this as a queryable table, not just a log format. ✅ **`logging_utils.log_api_call` (Step 13) is the single sanctioned writer — live-verified, including its client-side guard matching the live `chk_api_call_logs_error_code_on_failure` constraint.**

---

## 19. Schema Evolution, ADRs & Documentation Standards
Additive and backward-compatible migrations only; never edit a merged migration; deprecate before dropping. ADRs in `docs/adr/`. Documentation standard: every RPC/workflow/migration begins with `Purpose / Inputs / Outputs / Failure modes / Owner (module)`.

**Status note (2026-07-14):** three ADRs now exist in `docs/` (not `docs/adr/` — flat `docs/` was used instead, a fine convention for a project this size): `0001-raw-storage-deduplication.md`, `0002-resource1-per-state-pagination.md`, `0003-daily-rewrite-lookback-window.md` — covering, respectively, the raw layer's shift to content-addressed record-level storage, Resource 1's shift to per-state pagination (confirmed 46% silent daily data loss from an Elasticsearch `max_result_window` ceiling), and `daily_rewrite`'s shift from a single `today` target to a rolling 3-day lookback window (confirmed 0 rows processed on every run prior to the fix). All three are pushed to the repo (`git` commit `e7dba0e`, 2026-07-14). The earlier open question — "should a formal ADR folder be used going forward" — is settled: yes, and it now has real precedent to follow.

---

## 20. Manual Review SOP
```
Every morning
  → Open pending review queue (review_status = NEEDS_REVIEW)
  → For each: Approve (→ VERIFIED) · Reject (→ REJECTED) · or Merge (→ merge_entity)
  → Every action creates an entity_history row automatically
  → Goal: queue empty, or every remaining item has a documented reason
```
**Status note:** no UI or tooling exists yet for an operator to actually do this — it's currently only possible via direct SQL/RPC calls. Not part of Phase C; likely Phase D (ops dashboard, Step 20) or later.

---

## 21. Build Phases

Every phase follows the same per-step contract: **Deliverables → Acceptance Criteria → Verification (SQL/test) → Rollback**.

### PHASE A — Foundation schema — ✅ COMPLETE (2026-07-09, see Appendix: Phase A Build Log)
1. Raw layer — ✅
2. Canonical tables (`mandis`, `crops`) — ✅
3. Alias tables — ✅
4. Normalization functions — ✅
5. Identity resolution RPCs v2 — ✅ (pulled forward minimal `entity_history`/`audit_events` as 5a, `system_config` as 5b)
6. `merge_entity` RPC — ✅

### PHASE B — Operational metadata schema — ✅ COMPLETE (2026-07-09, see Appendix: Phase B Build Log)
7. `ingestion_batches` + `failed_pages` — ✅
8. `api_call_logs` — ✅
9. Price table with lineage + component quality score (`mandi_daily_prices`) — ✅
10. `compression_runs` + `historical_jobs` — ✅
11. `coverage_reports` + `quality_alerts` — ✅
12. Extended `audit_events` + `entity_history` with remaining operational fields — ✅

### PHASE C — Ingestion scripts — ✅ COMPLETE (2026-07-10/11, see Appendix: Phase C Build Log)
13. Shared `ingest_common.py` (§17 startup validation, §18 logging) — ✅ COMPLETE
14. `live_tick.py` — Resource 1, today-only, every 3h — ✅ COMPLETE. Two production bugs found and fixed during audit: mandi identity cache keyed on (name, state) instead of (name, state, district); `date.today()` used runner-local/UTC time instead of IST.
15. `daily_rewrite.py` — Resource 2, honors §8 precedence, pauses per §16 — ✅ COMPLETE. Surfaced that §8 precedence wasn't enforced anywhere yet — closed via new `price_writer.filter_rows_by_precedence`, applied to both `live_tick.py` and `daily_rewrite.py`.
16. `historical_backfill.py` — ✅ COMPLETE. CLI date-range tool over the shared Resource 2 pipeline (extracted as `resource2_pipeline.py`).
17. `retry_failed_pages.py` — ✅ COMPLETE. Reconstructs a failed page's original request and runs a successful retry through the full pipeline, not just an audit-trail patch.

**Post-Phase-C: repository restructure — ✅ COMPLETE** (see Appendix: Repository Restructure).

### PHASE D — Automation & observability — ⏳ IN PROGRESS
18. GitHub Actions rebuilt, concurrency-guarded, writes `ingestion_batches` — ✅ COMPLETE (see Appendix: Phase D Build Log).
18.5 (unnumbered, out-of-sequence). Raw storage deduplication (`raw_price_entries`) — ✅ COMPLETE 2026-07-13 (see Appendix: Raw Storage Deduplication). Not one of the original 33 steps — a storage-driven interrupt, not a renumbering of the plan below.
19. Nightly quality audit job → `quality_alerts` — ⏳ next
20. Ops dashboard (§22) — ⏳ not started

### PHASE D.5 — Testing — ⏳ not started (formal suite)
21. Integration tests · 22. Replay tests · 23. Failure-injection tests · 24. Duplicate tests · 25. Merge tests · 26. Compression tests · 27. Coverage tests
**Status note:** extensive ad hoc live verification has been done at every step — but nothing has been assembled into a repeatable, checked-in test suite yet.

### PHASE E — Migration (new account, v1 untouched throughout)
28. v2 schema applied to new project — ✅ done
29. Full historical backfill into v2 — ⏳ script exists (Step 16) but has NOT been run against real historical data yet.
30. Run v2's live pipeline against real data for an observation window (1–2 weeks), spot-check vs v1 — ⏳ not started
31. Fix anything surfaced; repeat until quality gates (§26) met — ⏳ not started
32. Repoint app to new project → archive old project — ⏳ not started

### PHASE F — Rollback (§16) — ⏳ not started (procedure written above, untested/unautomated)

### PHASE G — Operations Runbook — ⏳ not started
33. Write `FarmUncle_v2_Operations_Runbook.md`

### PHASE H — Release Process (every step, not just launch)
```
Developer writes code → Migration reviewed (§4/§6.8/§6.9) → Integration tests pass
→ Replay tests pass → Shadow run → Production → 24h observation → Tag release, update ADR
```
**Status note:** followed informally at every step; formal "Shadow run → 24h observation" not yet exercised since nothing ran on a real schedule until Phase D Step 18, just completed.

---

## 22. Observability: KPIs & Health Checks

**KPIs:** Average RPC latency · Average API latency · Retry % · Coverage % · New entities/day · Merge candidates/day · Auto-created % · Manual reviews pending · Compression duration · Batch duration

**Dashboard health checks:**
```
Last Live Tick (time, status)        Retry Queue (depth)
Last Daily Rewrite (time, status)    Compression Status (last run, hash result)
Rows Ingested Today                  Storage Used (Account 1 / Account 2 / R2)
Raw Records (total)                  Coverage %
Pending Reviews (count)              Merge Candidates (count)
Success % (rolling 7d)               Failure % (rolling 7d)
```
**Status note:** all of this data now exists in queryable tables as of Phase C — but no dashboard (Step 20) reads it yet. Every number above is currently only inspectable via direct SQL.

---

## 23. Capacity Planning
Tracked quarterly: expected rows/day, storage growth/month, API call volume vs rate limits, archive size/compression ratio. **Not started — no real production volume exists yet to measure.**

---

## 24. Performance Targets

| Job | Target |
|---|---|
| Live sync | < 5 min |
| Daily rewrite | < 30 min |
| Historical backfill, per week | < 10 min |
| Identity RPC lookup | < 20 ms |
| Normalization function | O(1) |

**Status note:** these targets were used directly to set GitHub Actions `timeout-minutes` values at Step 18 — not yet validated against real sustained production load, since no scheduled run has executed against real volume yet.

---

## 25. Future Extensions (reserved, not built now)
Multi-language names · farmer-reported corrections · weather integration · commodity image metadata · price forecasting/ML · notifications/subscriptions · market geofencing · analytics warehouse · third-party API versioning.

**Also effectively reserved, not yet built:** `price_cache`/`refresh_price_cache` (table/RPC don't exist), Account 2 (a second Supabase project — `HISTORY_SUPABASE_*` secrets are placeholders until this exists), `weekly_compress.py` and the R2 archive writer (`R2_*` secrets are placeholders until this is built).

---

## 26. Success Criteria
- ✓ Zero duplicate mandis/crops · zero orphan prices/cache rows · zero uncategorized crops outside review queue
- ✓ 100% batches replayable from raw · 100% price rows carry lineage
- ✓ Retry queue empty/draining · compression hash-verified weekly
- ✓ Coverage reports passing · nightly audit clean or triaged
- ✓ Every table documented · every enum and error code documented
- ✓ Every index and FK policy decision documented
- ✓ Manual Review SOP followed daily
- ✓ Release process followed for every deploy
- ✓ Operations Runbook exists
- ✓ App running on v2 only, v1 archived

**Status against this list as of 2026-07-11:** schema/pipeline-level criteria are structurally satisfied and live-verified. Process-level criteria (nightly audit, coverage reports, manual review SOP, ops runbook, app cutover) are not yet reached — depend on Phase D.19–20, E, F, G.

---

## Appendix: System Dependency Graph

```
Government API (Resource 1 / Resource 2)
       │
       ▼
raw_api_batches
       │
       ▼
raw_api_records  ── (immutable, §1)
       │
       ▼
Parser (parser_version, §3)
       │
       ▼
Identity RPCs (find_or_create_*, §13)
       │
       ├────────► mandis     ──┐
       │                       │
       ├────────► crops     ──┤
       │                       │
       │            merge_entity (§6.5)
       ▼
mandi_daily_prices (source precedence, §8) ── ✅ built Step 9, precedence enforced Step 15/16
       │
       ├────────► price_cache (refresh_price_cache) — NOT BUILT
       │
       ├────────► coverage_reports → quality_alerts (§20 SOP) ── ✅ built Step 11, daily_rewrite writes here too (§16 alert, Step 15)
       │
       └────────► weekly_compress (compression_runs) — NOT BUILT
                    │
                    ▼
                 Account 2 (historical Supabase) — NOT CREATED
                    │
                    ▼
                    R2 (verified, §14.2) ── historical_jobs tracks independently, table built Step 10, writer NOT BUILT
```

---

## Appendix: Phase A Build Log (completed 2026-07-09)

Built and verified against the new Supabase project (`wqccgjmvslevkglfkmtc`), clean slate, zero legacy tables.

**Step 1 — Raw layer.** `raw_api_batches`, `raw_api_records`, indexes, immutability trigger, FK restrict. Verified: insert/select works; UPDATE/DELETE rejected (`P0001`); FK restrict blocks deleting a batch with child records.

**Step 2 — Canonical tables.** `mandis`, `crops`, business-key uniqueness, lifecycle/review-status/location-confidence enums, merge-status consistency check, no-self-merge check. Verified: duplicate business key rejected; inconsistent merge-status rejected.

**Step 3 — Alias tables.** `mandi_aliases`, `crop_aliases` with match_method/confidence/approved, unique normalized alias, trigger enforcing alias→`ACTIVE` entity only.

**Step 4 — Normalization functions.** `normalize_market_name/crop_name/variety/unit`, versioned. **`normalize_unit` returns `'unknown'` rather than raising for an unmapped unit; `identity_client.py`'s docstring claim that it "rejects" an unmapped unit is stale relative to actual live behavior — caught during the Phase C audit but not fixed since no current script exercises an unmapped unit (both scripts always pass a fixed `"kg"` default).** Deviation fixed: alias tables were missing `normalization_version`; added via `ALTER TABLE`.

**Step 5 — Identity resolution RPCs.** `find_or_create_mandi`, `find_or_create_crop`; resolution order exact key → exact alias → fuzzy (state-scoped) → create. Deviation fixed (5a): pulled minimal `entity_history`/`audit_events` forward from Phase B. Deviation fixed (5b): `FUZZY_THRESHOLD` was hardcoded at 0.85 — moved to `system_config`, retuned to 0.75.

**Step 6 — `merge_entity` RPC.** Re-points aliases, transfers coordinates only if source has strictly better `location_confidence`, writes `entity_history` + `entity_merged` audit event. Deviation fixed: `mandis.merge_reason`/`merged_at` added via additive `ALTER TABLE`. Price-row re-pointing deferred (table didn't exist yet).

**Net result:** every table at 0 rows except `system_config` (8 seed rows, by design — later 10, see Step 14).

---

## Appendix: Phase B Build Log (completed 2026-07-09)

**Step 7 — `ingestion_batches` + `failed_pages`.** `status` CHECK (`RUNNING|SUCCESS|PARTIAL|FAILED`), `completed_at >= started_at` check, 3 indexes. `failed_pages`: FK restrict, `resolved_at`/`status` consistency check, 2 indexes. Re-verified independently: all constraints present via `pg_constraint`.

**Step 8 — `api_call_logs`.** `batch_id, job_name, resource, page, duration_ms, rows, status, error_code, called_at`. FK restrict. `chk_api_call_logs_error_code_on_failure`. 4 indexes. Verified live.

**Step 9 — `mandi_daily_prices`.** Full lineage columns, `quality_score`/`quality_components`, business-key unique constraint, FK restricts, `chk_prices_min_max`. Same-step addition: `trg_check_price_entities_not_merged` trigger. Verified via fixtures created through real RPCs only, per invariant 2.

**Step 10 — `compression_runs` + `historical_jobs`.** Status vocabularies, business key, order check, FK restrict, indexes. Verified.

**Step 11 — `coverage_reports` + `quality_alerts`.** Business keys, `coverage_pct` 0–100, severity/status vocabularies, `chk_quality_alerts_resolved_consistency`, optional FK restricts, indexes. Verified.

**Step 12 — Extended `audit_events` + `entity_history`.** Nullable `batch_id` + FK restrict added to both (additive). Deviation caught: promised-but-missing `entity_id` indexes from Phase A finally added. Verified.

**Net result:** Phase B complete, all operational metadata tables built exactly per §6.8/§6.9, no test data leaked at any step.

---

## Appendix: Phase C Build Log (completed 2026-07-10/11)

Built and live-verified against `wqccgjmvslevkglfkmtc` throughout — every constraint/trigger/RPC/concurrency claim below was tested with real inserts/RPC calls against the live schema, not mocked, with full cleanup back to baseline after each session.

**Step 13 — `ingest_common.py` (+ `config.py`, `config_validator.py`, `startup_validation.py`, `logging_utils.py`, `government_constants.py`).** Two gaps found and closed before Step 14 could run: `system_config` was missing `API_BASE_RESOURCE_1`/`API_BASE_RESOURCE_2` (inserted, sourced from real endpoint IDs); `government_constants.py` didn't exist despite being imported (added `STATES`). `ingest_common.py` is a pure facade.

**Step 14 — `live_tick.py`.** New shared modules: `batch_lifecycle.py` (ULID generation, batch CRUD, the §12 concurrency guard as a partial unique index), `identity_client.py` (memoized RPC wrapper), `quality_scoring.py` (pure-function quality math). §9's "explicit ok flag" fix built into `fetch_page`.

Full production audit found and fixed two real bugs:
1. **Mandi identity cache bug** — keyed on `(name, state)`, omitting `district`, while the live business key and RPC lookup are `(name, state, district)`. Reproduced against the original code (Kurnool incorrectly reused Anantapur's cached id); fixed and re-verified.
2. **Timezone bug** — `date.today()` used UTC instead of IST; fixed with an explicit IST timezone constant.

Also flagged, not fixed at the time (zero functional risk): a dead `time_api_call` import (later removed during Step 17's cleanup), and a duplicated `_utcnow_iso()` helper.

**Step 15 — `daily_rewrite.py`.** Extracted `resource_client.py` and `price_writer.py` (new `filter_rows_by_precedence`) — §8 precedence enforcement, the real deliverable of this step, since it was never enforced until Resource 2 data existed. Verified all three precedence cases against real live-DB row shapes. §16 outage alert built and live-verified, including a self-caught bug where a secondary failure in the alert-check itself could have masked the original exception.

**Step 16 — `historical_backfill.py`.** Extracted `resource2_pipeline.py` (full per-date pipeline, `daily_rewrite.py` became a thin wrapper). CLI validated before any network call. Per-date resilience vs. whole-run abort both tested. Concurrency guard confirmed independent from `daily_rewrite`/`live_tick` (different `job_name`s).

**Step 17 — `retry_failed_pages.py`.** Extracted `record_processor.py` (third occurrence of parse→identity→quality→row-dict). Caught a leftover dead `dataclass` import in `live_tick.py` missed since Step 15. `failed_pages` doesn't store enough to reconstruct a request — solved via `batch_id → ingestion_batches.date_range_start` (date), a parsed `"[state=XXX]"` error_message prefix (state), and reversed offset arithmetic. Documented, unfixed limitation: offset reconstruction uses the *current* `PAGE_SIZE`. On success, a recovered page runs through the full pipeline, not just an audit-trail patch.

**Net result:** 17 Python files, full-system syntax + real-import check passing together, every RPC signature and query-builder call verified against live `pg_proc`/`pg_indexes` or the installed `postgrest-py` library, zero mocked database tests.

---

## Appendix: Repository Restructure (completed 2026-07-11)

Deferred deliberately until Phase C was fully done. Moved from a flat single directory into `farmuncle_pipeline/{core,ingestion}`, all imports rewritten to absolute, `__init__.py` added at all three package levels. Verified: full syntax + real-import check across every file from repo root, `live_tick` run via `python -m farmuncle_pipeline.ingestion.live_tick` confirmed failing exactly at the expected point (missing secrets), `historical_backfill`'s argparse re-confirmed working post-move.

Delivered with `.vscode/settings.json`/`launch.json`, `README.md` (structure map + the load-bearing run rule: always `python -m farmuncle_pipeline.ingestion.<script>` from repo root), `requirements.txt` (`supabase`, `requests` — confirmed the only two real third-party deps), `.env.example`.

---

## Appendix: Phase D Build Log — Step 18 (completed 2026-07-11)

Four workflows, YAML-validated, cross-checked against §24 for `timeout-minutes`:

| Workflow | Trigger | Concurrency group | Timeout |
|---|---|---|---|
| `live_tick` | cron, every 3h | `live-tick` | 15 min (§24 target: <5 min) |
| `daily_rewrite` | cron, 14:30 UTC = 20:00 IST | `daily-rewrite` | 45 min (§24 target: <30 min) |
| `retry_failed_pages` | cron, 06:00 & 16:00 UTC | `retry-failed-pages` | 30 min |
| `historical_backfill` | `workflow_dispatch` only (+ date inputs) | `historical-backfill` | none (GitHub's 6h ceiling) |

All four use `cancel-in-progress: false` — cancelling would SIGKILL a run before its own exception handler can close `ingestion_batches` out of RUNNING, leaving a stale row blocking every future run; queuing avoids that.

The 14:30 UTC cron settles the open assumption from Step 15's docstring about what time daily_rewrite actually runs — 20:00 IST chosen as a reasonable "evening-finalized" time per §8/§9, not yet validated against Resource 2's real-world finalization behavior since no scheduled run has executed yet.

**Status note (secrets):** only 3 of the 8 referenced secrets (`SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `DATA_GOV_API_KEY`) are real/required for anything to function; the other 5 need only be non-empty placeholders until Account 2/R2 are built.

---

---

## Appendix: Raw Storage Deduplication (completed 2026-07-13)

**Not one of the original 33 numbered steps** — an out-of-sequence, storage-driven change, done in between Step 18 and Step 19 rather than as part of the planned sequence.

**Problem:** `raw_api_records` (one row per fetched page, every `live_tick` run, every 3h, forever, no dedup) had grown to 16 MB across 863 rows — the dominant storage cost in the system by a wide margin (every other table combined was under 1 MB). Root cause: most individual price records are unchanged between consecutive 3-hourly fetches, but the old design re-stored the entire page regardless, so unchanged content was being physically duplicated on every run.

**Design considered and rejected:** deleting raw data immediately after each fetch, or after a fixed short window (e.g. 1 day). Both were rejected — raw data is the only mechanism for retroactively fixing parser bugs, fixing mis-resolved mandi identities, proving disputes, and running replay tests (invariants 1 and 10); deleting it trades away all four permanently. A 1-day window additionally conflicts with the planned `weekly_compress` cadence (§14) — raw would be deleted before the weekly archive job ever ran against it.

**Design built:** content-addressed, record-level dedup rather than page-level or time-based deletion.
- New table `raw_price_entries`: one row per distinct `(resource, market, state, district, commodity, raw_variety, price_date, content_hash)` combination ever observed. `content_hash` is a SHA-256 of the record's actual price fields (`modal_price`/`min_price`/`max_price`).
- New RPC `upsert_raw_price_entry`: a single atomic `INSERT ... ON CONFLICT ... DO UPDATE`, using Postgres' `xmax` to report whether the call wrote genuinely new content (`is_new=true`) or only touched `last_seen_batch_id`/`last_seen_at` on an already-known value (`is_new=false`). Live-verified: 3 calls (2 identical + 1 distinct) correctly produced 2 stored rows, not 3.
- New module `raw_dedup.py` (owner: ingestion) wraps the RPC call and the hashing.
- `insert_raw_api_record` calls removed from all three real call sites — `live_tick.py` (`_fetch_all_resource_1_pages`), `resource2_pipeline.py` (shared by `daily_rewrite.py`/`historical_backfill.py`), and `retry_failed_pages.py` — each replaced with a per-record loop calling `parse_agmarknet_record` (already existed, reused as-is per §2's no-duplicated-logic rule) then `upsert_raw_price_entry`.

**What did NOT change:**
- `raw_api_batches`/`ingestion_batches` — still exactly one header row per run regardless of how much content in that run was new vs. already-known. Batch-level lineage (§8, invariant 8) is untouched.
- `mandi_daily_prices` — writes exactly as before; this change is purely about the raw layer underneath it.
- Existing `raw_api_records` rows (863 rows / 16 MB as of 2026-07-13) — not deleted, not migrated, not backfilled into `raw_price_entries`. They remain the historical record for everything fetched before this change shipped. `raw_api_records` the table is not dropped, only frozen (no new writes).

**Explicitly not done yet:**
- No formal `docs/adr/` entry written (§19) — recommended before/alongside this shipping to production traffic, not done as part of this pass.
- No retroactive backfill of pre-2026-07-13 `raw_api_records` content into `raw_price_entries` — if that's ever wanted (e.g. to unify historical queries against one raw table), it's a separate, deliberately-deferred decision.
- Not validated against real production fetch volume yet — the upsert logic was verified with synthetic test rows (inserted and cleaned up directly against `raw_price_entries`), not yet against a real `live_tick` run. Recommended check once deployed: `select is_new, count(*) from raw_price_entries group by is_new` across a couple of real runs — expect mostly `is_new=true` on the first run, mostly `is_new=false` on subsequent unchanged runs.
- `weekly_compress`/R2/Account 2 (§14, §25) — still not built. This change removes the *urgency* (the dominant growth source is now flat) but not the plan itself.

---

**Next action:** two open items —
- **Phase D, Step 19** — nightly quality audit job → `quality_alerts` (say "code step 19"). Timely: both ADR 0002 and ADR 0002's own "Open / not done yet" section flag `QUALITY-001` coverage-mismatch warnings as currently `print`-only with no durable, queryable home — Step 19 is exactly what fixes that.
- **Phase E, Step 29** — run `historical_backfill.py` for real against actual historical dates (script exists and is workflow_dispatch-capable, just hasn't been pointed at real data yet)

(Resolved since the previous revision: documentation debt — three ADRs now live in `docs/`, see §19's status note — and real-traffic verification — confirmed live via ADR 0001/0002/0003's own "Verification" sections, all checked against real `ingestion_batches`/`mandi_daily_prices` data, not synthetic tests.)
