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

from dataclasses import dataclass

from farmuncle_pipeline.config import ConfigError, Resource, Source
from farmuncle_pipeline.core.batch_lifecycle import insert_data_quality_issue

# Postgres SQLSTATE prefixes that indicate a permanent, row-specific
# rejection (bad data), as opposed to a transient/infra failure
# (network, auth, pooler). Only these are eligible for the
# chunk->row isolation fallback in `upsert_price_rows` below — anything
# else (timeouts, connection resets, 5xx) is re-raised immediately,
# since retrying those row-by-row would just fail hundreds of times in
# a row for no benefit and burn a lot of time before still failing the
# date.
#   23514 check_violation      (e.g. chk_prices_min_max)
#   23502 not_null_violation
#   22*   data_exception        (invalid_text_representation, numeric
#                                 out of range, etc.)
_ROW_LEVEL_ERROR_CODE_PREFIXES = ("23514", "23502", "22")


def _is_row_level_error(exc: Exception) -> bool:
    """True if `exc` looks like a permanent, single-row data problem
    (safe to isolate and quarantine) rather than a transient/infra
    failure (which should abort the whole date, not be retried 1000x
    row-by-row). Falls back to False (i.e. "not row-level, re-raise")
    for anything that isn't a recognizable postgrest APIError with a
    SQLSTATE code — being conservative here is deliberate."""
    code = getattr(exc, "code", None)
    if not code:
        return False
    return any(code.startswith(prefix) for prefix in _ROW_LEVEL_ERROR_CODE_PREFIXES)


@dataclass(frozen=True)
class UpsertResult:
    """Purpose: bundles what happened during `upsert_price_rows` so
    callers can report it (and decide PARTIAL vs SUCCESS) instead of
    just getting a bare None back. `quarantined` rows are ALREADY
    durably recorded in `data_quality_issues` by the time this is
    returned — callers don't need to do anything further with them
    except surface the count in their own summary/error_summary."""
    rows_upserted: int
    rows_quarantined: int

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


def upsert_price_rows(
    client,
    rows: list[dict],
    batch_size: int,
    *,
    batch_id: str | None = None,
    resource: Resource | None = None,
) -> UpsertResult:
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

        ISOLATION (added after the 2026-06-29 incident: a single row
        with min_price=83548/max_price=8354 — a government 10x
        data-entry error — hit `chk_prices_min_max` and aborted the
        entire date, losing 17000+ already-fetched, already-identity-
        resolved rows for every other row in that chunk and every
        chunk after it):
        Each chunk is first attempted as a single bulk upsert (the
        fast path — this is what makes bulk upsert worth doing at
        all). If, and only if, that bulk call fails with a
        recognizably row-level, permanent error (see
        `_is_row_level_error` — check/not-null/data-exception
        violations, NOT network/timeout/5xx), the chunk is retried
        row-by-row to find and isolate the specific offending row(s):
        every row that upserts fine on its own is kept, every row that
        fails on its own is quarantined into `data_quality_issues`
        (via `insert_data_quality_issue`) and excluded — the rest of
        the chunk, and every subsequent chunk, still gets written.
        A transient/infra failure (anything `_is_row_level_error`
        returns False for) is NOT retried row-by-row — it's re-raised
        immediately, same as before, since row-by-row retry of an
        infra failure just fails hundreds of times for no benefit.
    Inputs:
        client: an already-constructed Supabase client.
        rows: parsed, identity-resolved, quality-scored row dicts.
        batch_size: chunk size for each upsert call.
        batch_id: the `ingestion_batches.id` for this run — required
            to quarantine a row; if a row-level error occurs and
            batch_id/resource were not supplied, the error is
            re-raised instead (quarantining without lineage would
            create an untraceable record).
        resource: which government resource these rows came from —
            required alongside batch_id, same reasoning.
    Outputs:
        `UpsertResult(rows_upserted, rows_quarantined)`.
    Failure modes:
        Raises `ConfigError` if a chunk's bulk upsert fails for a
        non-row-level reason, if a quarantine insert itself fails, or
        if a row-level error occurs but batch_id/resource weren't
        supplied (nowhere safe to record what was dropped).
    """
    if not rows:
        return UpsertResult(rows_upserted=0, rows_quarantined=0)

    deduped: dict[tuple, dict] = {}
    for row in rows:
        key = (row["mandi_id"], row["crop_id"], row["variety"], row["price_date"])
        deduped[key] = row
    unique_rows = list(deduped.values())

    table = client.table("mandi_daily_prices")
    total_upserted = 0
    total_quarantined = 0

    for i in range(0, len(unique_rows), batch_size):
        chunk = unique_rows[i : i + batch_size]
        try:
            table.upsert(chunk, on_conflict="mandi_id,crop_id,variety,price_date").execute()
            total_upserted += len(chunk)
            continue
        except Exception as exc:
            if not _is_row_level_error(exc):
                raise ConfigError(
                    f"Failed to upsert mandi_daily_prices chunk (size={len(chunk)}): {exc}"
                ) from exc
            # Fall through to row-by-row isolation below.

        if batch_id is None or resource is None:
            raise ConfigError(
                f"Row-level upsert failure in a chunk of size {len(chunk)}, but no "
                f"batch_id/resource was supplied to quarantine the offending row(s) — "
                f"cannot safely drop data with no lineage. Pass batch_id and resource "
                f"to upsert_price_rows to enable isolation."
            )

        for row in chunk:
            try:
                table.upsert([row], on_conflict="mandi_id,crop_id,variety,price_date").execute()
                total_upserted += 1
            except Exception as row_exc:
                if not _is_row_level_error(row_exc):
                    raise ConfigError(
                        f"Failed to upsert an individual mandi_daily_prices row during "
                        f"isolation fallback: {row_exc}"
                    ) from row_exc
                insert_data_quality_issue(
                    client,
                    batch_id=batch_id,
                    resource=resource,
                    row=row,
                    error_code=getattr(row_exc, "code", None),
                    error_message=str(row_exc),
                )
                total_quarantined += 1

    return UpsertResult(rows_upserted=total_upserted, rows_quarantined=total_quarantined)