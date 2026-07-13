"""
FarmUncle v2 — price_writer.py
Phase C, Step 15 (extracted from Step 14's `live_tick.py`, plus new
logic Step 15 requires — see module docstring below for why both
belong together in one new shared module).

Purpose (module-level):
    Two responsibilities, both about writing to `mandi_daily_prices`,
    both needed identically by every script that writes price rows:

    1. `upsert_price_rows` — the chunked, business-key-deduplicated
       upsert itself. Extracted verbatim from `live_tick.py`'s
       `_upsert_price_rows` (Step 14) now that `daily_rewrite.py`
       (Step 15) is a second real caller — the function was already
       fully source-agnostic (source is a field in each row dict, not
       a parameter), so this is a pure extraction, not a rewrite.

    2. `filter_rows_by_precedence` — NEW at Step 15, and the reason
       this module exists rather than just re-exporting the old
       private helper. §8 defines an authoritative precedence order
       (manual > resource_2 > resource_1 > auto-created defaults) that
       NOTHING enforced at Step 14, because Resource 2 data did not
       exist yet — `live_tick.py` could safely upsert unconditionally
       since Resource 1 was the only writer in existence. Now that
       `daily_rewrite.py` writes real Resource 2 rows, two precedence
       violations become possible if left unenforced:
         a) `daily_rewrite` (Resource 2) overwriting a `manual`
            correction for the same business key — explicitly
            forbidden by §8 ("manual corrections ... always wins").
         b) `live_tick` (Resource 1) — which per its own §12/data-
            ownership note may legitimately run again *after*
            `daily_rewrite` has already written the day's authoritative
            Resource 2 rows — silently clobbering that authoritative
            data with a lower-precedence Resource 1 row.
       Both are real, not hypothetical: `live_tick` runs every 3h
       around the clock (per its own module docstring) and
       `daily_rewrite` runs once, in the evening, per spec §9/§21 — so
       a `live_tick` run scheduled after that evening run will, on any
       ordinary day, attempt to write the same day's business keys
       right after `daily_rewrite` already finalized them. This module
       is what makes §8 actually hold given that real scheduling
       overlap, for both callers, symmetrically.

    `live_tick.py` (Step 14) is updated to call
    `filter_rows_by_precedence` before its (extracted, unchanged)
    upsert — this is a necessary correctness fix surfaced BY Step 15's
    existence, not a speculative Step 14 rewrite (mirrors how Step 14
    itself patched two gaps in Step 13's delivery before it could run
    at all — see that step's build notes).

Explicitly out of scope for this file:
    - Anything resource-specific (HTTP fetching, field parsing — see
      `resource_client.py`)
    - Identity resolution, quality scoring (see `identity_client.py`,
      `quality_scoring.py`)
"""

from __future__ import annotations

from farmuncle_pipeline.config import ConfigError, Source

# §8's precedence order, expressed as a rank: higher wins. A row is
# only allowed to overwrite an existing row at the same business key
# if its source's rank is >= the existing row's rank. Manual is
# highest (§8: "always wins"); resource_2 next (evening-finalized,
# authoritative); resource_1 lowest of the three automated writers.
_SOURCE_RANK: dict[str, int] = {
    Source.MANUAL.value: 3,
    Source.RESOURCE_2.value: 2,
    Source.RESOURCE_1.value: 1,
}


def filter_rows_by_precedence(client, rows: list[dict], new_source: Source) -> tuple[list[dict], int]:
    """
    Purpose:
        Given a batch of parsed/identity-resolved/quality-scored price
        rows all coming from `new_source`, drop any row whose business
        key already has a row in `mandi_daily_prices` with a
        STRICTLY HIGHER-precedence source (§8) than `new_source`. Rows
        with no existing row, or an existing row of equal-or-lower
        precedence, pass through unchanged and are still eligible for
        `upsert_price_rows`.
    Inputs:
        client: an already-constructed Supabase client.
        rows: row dicts as built by the caller (must each contain
            mandi_id/crop_id/variety/price_date — the same four fields
            that make up `mandi_daily_prices`' business key).
        new_source: the `Source` all of `rows` share (a single script
            run only ever writes one source).
    Outputs:
        `(kept_rows, skipped_count)` — `kept_rows` is the subset safe
        to upsert; `skipped_count` is how many were dropped because a
        higher-precedence row already exists (report this in the
        batch's `error_summary`/logs — a nonzero count is expected and
        healthy behavior, not a failure, but it is worth surfacing).
    Failure modes:
        Raises `ConfigError` if the existing-rows lookup itself fails
        (network error, unexpected response shape). Never raises for
        an empty `rows` list (returns `([], 0)` immediately without a
        network call).
    """
    if not rows:
        return [], 0

    new_rank = _SOURCE_RANK.get(new_source.value, 0)
    price_dates = sorted({row["price_date"] for row in rows})

    try:
        response = (
            client.table("mandi_daily_prices")
            .select("mandi_id,crop_id,variety,price_date,source")
            .in_("price_date", price_dates)
            .execute()
        )
    except Exception as exc:
        raise ConfigError(
            f"Failed to fetch existing mandi_daily_prices rows for precedence check "
            f"(price_dates={price_dates!r}): {exc}"
        ) from exc

    existing_rank: dict[tuple, int] = {}
    for existing in response.data or []:
        key = (existing["mandi_id"], existing["crop_id"], existing["variety"], existing["price_date"])
        existing_rank[key] = _SOURCE_RANK.get(existing["source"], 0)

    kept: list[dict] = []
    skipped = 0
    for row in rows:
        key = (row["mandi_id"], row["crop_id"], row["variety"], row["price_date"])
        if existing_rank.get(key, 0) > new_rank:
            skipped += 1
            continue
        kept.append(row)

    return kept, skipped


def upsert_price_rows(client, rows: list[dict], batch_size: int) -> None:
    """
    Purpose:
        Upsert parsed price rows into `mandi_daily_prices` in chunks of
        `batch_size` (from system_config, not a hardcoded constant).
        Deduplicates by the table's actual business key first, since
        two records for the same (mandi, crop, variety, date) within
        one run would otherwise both attempt to occupy the same
        upserted row — the last one wins, matching Postgres
        `ON CONFLICT` semantics for a batch upsert.

        Callers should run rows through `filter_rows_by_precedence`
        first — this function has no source-precedence awareness of
        its own and will happily overwrite a higher-precedence row if
        asked to; that check is deliberately a separate step (see this
        module's docstring for why).
    Inputs:
        client: an already-constructed Supabase client.
        rows: parsed, identity-resolved, quality-scored row dicts.
        batch_size: chunk size for each upsert call.
    Outputs:
        None.
    Failure modes:
        Raises `ConfigError` if any chunk's upsert fails.
    """
    if not rows:
        return

    deduped: dict[tuple, dict] = {}
    for row in rows:
        key = (row["mandi_id"], row["crop_id"], row["variety"], row["price_date"])
        deduped[key] = row
    unique_rows = list(deduped.values())

    for i in range(0, len(unique_rows), batch_size):
        chunk = unique_rows[i : i + batch_size]
        try:
            client.table("mandi_daily_prices").upsert(
                chunk, on_conflict="mandi_id,crop_id,variety,price_date"
            ).execute()
        except Exception as exc:
            raise ConfigError(
                f"Failed to upsert mandi_daily_prices chunk (size={len(chunk)}): {exc}"
            ) from exc
