# Mandi & Crop Duplicate Review Playbook (FarmUncle v2)

For any future Claude session picking up mandi/crop identity dedup work on `flxjrcbhmcuaynctokpv`. Written after a full day-one review pass (78 mandi pairs, 15 crop pairs) that included getting some calls wrong and having to revert them — the mistakes are documented here on purpose so they don't repeat.

---

## 1. Where duplicate candidates come from

- `sweep_duplicate_mandis()` / `sweep_duplicate_crops()` generate candidates automatically. A pair only reaches the queue if it clears, **in this order**:
  1. Fuzzy name similarity (trigram) above threshold
  2. Same state
  3. Same district
- Once a pair exists in `mandi_duplicate_review_queue` / `crop_duplicate_review_queue` — **regardless of resolution status** — the sweep will never insert it again. So every decision is permanent unless someone manually deletes the row. This means a wrong decision doesn't just sit wrong, it actively blocks the pair from ever resurfacing for review. Get it right, or revert it properly (see §5).
- **Varieties and grades have no such sweep, no review queue, and no `status`/`merged_into_id` columns at all** (confirmed 2026-07-26). Any variety/grade duplicate found so far (e.g. IR-64) was found by manual `pg_trgm` similarity scan, not automation. If this session is asked to review varieties/grades, that infrastructure has to be built first or the review has to be done fully ad hoc.

## 2. The core rule (read this before touching anything)

**For mandi pairs:** merge only if the distinguishing part of the name is a *generic market-type label* — not a real place.

| Merge on sight | Do NOT merge without independent confirmation |
|---|---|
| "Sub yard", "Grain Market", "Veg Yard", "VFPCK Market" | A real village/town name in the suffix, e.g. `Kendrapara(Marshaghai)`, `Barshi(Vairag)`, `Rahuri(Vambori)` |
| Spelling/transliteration variant of the same word, e.g. `Kattakada`/`Kattakkada`, `Sausar`/`Saunsar` | Two names that are both real, independently-existing towns even if geographically close, e.g. `Tharad` vs `Thara` (Banaskantha district — genuinely separate) |
| A locality/neighborhood *within* one city, e.g. `Kolhapur(Laxmipuri)`, `Godhra(Timbaroad)` | Anything you're not confident is a sub-locality vs. a separate town — when in doubt, don't merge |

**Why price data is NOT the tiebreaker for mandi identity**, even though it's tempting:
- With only a few days of live history, most pairs share 0-2 crop/variety/grade combos on the same day — far too thin a sample. A single matching or mismatching price is coincidence-prone in either direction (two real, separate small towns can easily report an identical round number for a staple crop; a true duplicate can show a one-off reporting glitch).
- **Real-world geography knowledge beats database price evidence for this specific question.** Two towns being the same place is a fact about the world, not a fact price data can reliably establish with under ~30 days of history. If you (the assistant) aren't confident a place name is real vs. a locality, say so and ask, or leave it pending — don't guess.
- The one exception: a **large, stable, multi-day price divergence** between two well-known real city sub-yards (e.g. Pune's Moshi/Pimpri/Manjri yards, Bengaluru's Binny Mill FF&V yard) IS good evidence they're genuinely separate physical markets — that's a case of price data confirming what's already independently known to be true (these are documented, distinct APMC yards), not price data doing the identification on its own.

**For crop pairs**, the logic is different and price data is more useful, because there's no "place name" concept — the real question is "different product" vs "same product, different label":
- If the two crops show a **real, consistent price gap on the same day at the same market** — that's solid evidence of a genuinely different product (e.g. Beans ₹2300 vs Bunch Beans ₹1700 at the same mandi, same day, repeated the next day too). This worked well and should be the primary test for crop pairs.
- Watch out for the "different product form" pattern: whole vs. dal/split (Moong/Moong Dal, Tur/Tur Dal), raw vs. processed (Wheat/Wheat Atta, Groundnut/Groundnut pods raw), or genuinely different item (Cotton/Cotton Seed, Potato/Sweet Potato, Coconut/Tender Coconut). These look like near-duplicates by name but are legitimately separate line items in government price data — reject as separate on sight, no price check needed, this pattern is well-established.
- Don't assume a name that "sounds like a misspelling" actually is one — check the data. `Duster Beans` looked like a typo for `Cluster Beans` but turned out to be a genuinely different, consistently cheaper product at the same market. Similarly `Rose(Loose)` vs `Rose(Local)` looked like a formatting variant but showed a stable 1.5-3x price gap.
- A genuine same-crop merge candidate: near-identical price, same mandi, same day, same grade, and the underlying `variety` text is literally the same word under two different `crop_id`s (e.g. "nigella"/"nigella seeds" both use variety text "kalonji").

## 3. Step-by-step process for a batch of pending pairs

1. Pull the pending rows: `select ... from mandi_duplicate_review_queue where resolved is not true` (join to `mandis`/`crops` for names).
2. Classify each pair by name alone first, using the table in §2 — this resolves most pairs without touching price data at all:
   - Cross-town spurious match (shared boilerplate suffix like "Uzhavar Sandhai" or "VFPCK Market" inflating similarity despite genuinely different place names) → confirmed separate, no price check needed.
   - Same-town, different market type (e.g. `Panruti(Uzhavar Sandhai)` vs `Panruti APMC`) → confirmed separate (different produce categories — a farmer market and a wholesale APMC in the same town are not the same physical market).
   - Generic type suffix or spelling variant → merge.
   - Real place-name suffix → do NOT merge on this pass; flag as needing independent confirmation (real-world lookup) or more accumulated history.
3. For crop pairs, run the same-day price-divergence check described in §2 as the primary evidence.
4. Spot-check your own classification with a real query before batch-applying it — don't trust the pattern blind. A cheap way: pick 2-3 pairs you're about to auto-reject/merge and pull their actual shared-combo data to confirm the reasoning holds for this specific batch.
5. Execute decisions (see §4), then immediately update `resolution` (see §6).
6. Run `select * from verify_merge_integrity();` after every batch — should return empty. This is cheap and catches orphaned price rows immediately.

## 4. Execution — the actual functions

- **Mandi merge:** `select union_merge_mandi(survivor_id, loser_id, 'reason text');`
  - Mechanics: same-day/combo price collisions with **identical** values get deduped (loser's row deleted — this is the only case where a row is actually destroyed, not just moved). Collisions with **differing** values are **both kept**, distinguished by `source_mandi_id`/`source_mandi_name` on the row. Non-colliding rows just get their `mandi_id` moved to the survivor. Aliases move from loser to survivor. Loser's `mandis.status` becomes `MERGED`.
  - **This means "1+1=2" only when the prices differ. When they're identical, the duplicate is deleted, not kept twice.** Know which case you're in before treating a merge as risk-free — see the revert section below for why this matters.
- **Crop merge:** `select merge_entity('crop', source_id, target_id, 'reason', 'MANUAL', confidence);`
  - `merge_method` check constraint only accepts `EXACT`/`NORMALIZED`/`FUZZY`/`MANUAL` — not e.g. `MANUAL_REVIEW`, it will reject the insert.
- **No `merge_variety`/`merge_grade` function exists.** A variety/grade merge (e.g. IR-64) currently has to be done by hand: insert an approved alias row, then manually remap/delete the duplicate's price rows and delete the duplicate variety/grade row. Consider building proper merge functions for these (mirroring `merge_entity`) before doing another one, since it's now happened at least twice.

## 5. How to revert a wrong merge — and the critical thing to check first

Before reverting, **check whether the merge deduped or kept-both** for that specific pair:

```sql
-- did any of the loser's rows survive under source_mandi_id, or were they deleted as exact dupes?
select count(*) from mandi_daily_prices
where mandi_id = <survivor_id> and source_mandi_id = <loser_id>;
```

- **If rows exist with that `source_mandi_id`** (the "kept both" case): fully reversible, no data loss.
  ```sql
  update mandi_daily_prices set mandi_id = <loser_id>
  where mandi_id = <survivor_id> and source_mandi_id = <loser_id>;

  update mandis set status = 'ACTIVE', merged_into_id = null, merge_reason = null, merged_at = null
  where id = <loser_id>;
  ```
- **If the count is zero** (the "deduped" case, e.g. Champua/Jhumpura): the loser's rows were deleted outright during the merge because they were exact duplicates of what the survivor already had. Reverting the *identity* (mandi status back to `ACTIVE`) is still correct and safe, but **the historical row-level data for the loser cannot be restored** — it's gone, not just hidden. In practice no information is lost (the numbers are, by definition, identical to what the survivor already has recorded), but the loser's own attribution for those specific days is gone permanently unless you re-pull from the source API.
- Either way, after restoring `mandis.status = 'ACTIVE'`, double-check `mandi_aliases` for anything that moved from loser to survivor during the original merge — if any exist, they need to move back too, or future ingestion of the loser's name could misroute to the survivor. Check with:
  ```sql
  select * from mandi_aliases where mandi_id in (<survivor_id>, <loser_id>);
  ```
  Also worth directly confirming the resolver will behave correctly going forward — `find_or_create_mandi`'s exact-match step filters `status = 'ACTIVE'`, so once you've restored the loser to `ACTIVE`, new ingestion should resolve to it correctly as long as no stray alias points elsewhere.
- Update the queue row's `resolution` to say it was reverted and why — don't leave it looking like a clean merge.

## 6. Logging — always update `resolution`, not just `resolved`

Neither `union_merge_mandi` nor `merge_entity` sets `resolved = true` on the queue row automatically. You have to do it yourself after every decision:

```sql
update mandi_duplicate_review_queue
set resolved = true, resolution = '<short machine-readable reason>'
where id = <queue_row_id>;
```

Use a consistent short-code style so a future session can `group by resolution` and understand the whole queue at a glance, e.g.:
- `merged_generic_type_suffix_or_spelling_variant_same_city`
- `confirmed_separate_diff_locality_spurious_suffix_match`
- `confirmed_separate_diff_market_type`
- `confirmed_separate_known_distinct_apmc_subyards_large_price_divergence`
- `REVERTED_<date>: <why>`

Without this, `resolved = true` alone can't distinguish a true merge from a confirmed-separate from a corrected mistake — and that distinction matters a lot when someone (human or Claude) comes back to audit the batch later.

## 7. Mistakes made on the first full pass (2026-07-26) — don't repeat these

1. **Merged all 30 remaining mandi pairs on a "the app only shows the town name anyway" rationale**, treating that as license to ignore identity correctness. This is wrong even if the product-display argument sounds reasonable — a wrong identity merge still corrupts the underlying data (mislabels which physical market a price actually came from), even if the app UI never shows the difference today. Product display requirements are not a substitute for correct entity resolution.
2. **Tried a "same price = same market" theory as the deciding factor**, and it produced a wrong call (Tharad/Thara, two real towns) before being caught by the user's own geography knowledge, not by anything in the database. The theory isn't wrong in principle (see §2's crop-price-gap section, where a similar idea worked), but it's unreliable for *mandi identity* specifically at low sample sizes, and it should never override a known real-world fact about place names.
3. **Lesson embedded in the rule in §2**: for place identity, name-pattern classification (grounded in real-world knowledge of Indian geography) is more reliable than price statistics on a handful of days. For product identity (crops), price divergence at the same market on the same day is a reliable and primary signal. Don't swap these two approaches between the two entity types.

## 8. Quick reference — useful queries

```sql
-- pending mandi pairs with names
select q.id, ma.name name_a, mb.name name_b, q.state, q.district, q.similarity
from mandi_duplicate_review_queue q
join mandis ma on ma.id = q.mandi_id_a
join mandis mb on mb.id = q.mandi_id_b
where q.resolved is not true order by q.similarity desc;

-- same-day shared combos + price comparison for a mandi pair (id_a, id_b)
select crop_id, variety, price_date, modal_price
from mandi_daily_prices
where mandi_id in (<id_a>, <id_b>)
  and (crop_id, variety, price_date) in (
    select crop_id, variety, price_date from mandi_daily_prices where mandi_id = <id_a>
    intersect
    select crop_id, variety, price_date from mandi_daily_prices where mandi_id = <id_b>
  )
order by price_date;

-- integrity check, run after every batch
select * from verify_merge_integrity();

-- raw fuzzy-match scan for varieties (no automated sweep exists yet — see §1)
select v1.id, v1.name, v2.id, v2.name, v1.crop_id,
  similarity(v1.normalized_name, v2.normalized_name) as sim
from varieties v1
join varieties v2 on v1.crop_id = v2.crop_id and v1.id < v2.id
where similarity(v1.normalized_name, v2.normalized_name) > 0.4
order by sim desc;
```
