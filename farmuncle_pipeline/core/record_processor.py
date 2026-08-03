"""
FarmUncle v2 — record_processor.py
Phase C, Step 17 (extracted from Step 14/15's `live_tick.py` and
`resource2_pipeline.py`).

Purpose (module-level):
    The "turn a list of raw government-API records into
    `mandi_daily_prices`-ready row dicts" step: parse each record
    (`resource_client.parse_agmarknet_record`), resolve its mandi/crop/
    variety/grade through the RPC-backed `IdentityClient` (invariant 3
    — no identity logic duplicated in Python), score its quality
    (`quality_scoring.compute_quality`), and assemble the row dict
    `price_writer.upsert_price_rows` expects. `variety`/`grade` (text)
    are local-normalization values from `identity.resolve_variety`/
    `resolve_grade` — either may normalize to `"other"` when the
    government record didn't report one for this row. `variety_id`/
    `grade_id` (added 2026-07-26) are their FK counterparts, resolved
    via `identity.resolve_variety_id`/`resolve_grade_id` from those
    same normalized text values — see those methods' docstrings in
    identity_client.py for why the normalized (not raw) value is what
    gets passed through.

    This exact sequence appeared inline, identically, in both
    `live_tick.py` (Step 14, Resource 1) and `resource2_pipeline.py`
    (Step 15, Resource 2) — the only per-resource difference was which
    `Source` enum value to tag rows with, which was already a
    parameter-shaped difference, not a logic difference. Now that
    `retry_failed_pages.py` (Step 17) needs the same conversion for
    whatever single recovered page it's processing (which may be
    either resource), duplicating this a third time would violate
    Never-Do Rule §2 — so it's extracted here, and `live_tick.py`/
    `resource2_pipeline.py` are updated to call it instead of
    inlining it (pure extraction, behavior unchanged).

Explicitly out of scope for this file:
    - Fetching records over HTTP (`resource_client.py`)
    - Writing rows to `mandi_daily_prices` (`price_writer.py`)
    - Batch lifecycle (`batch_lifecycle.py`)
"""

from __future__ import annotations

from dataclasses import dataclass

from farmuncle_pipeline.config import ConfigError, NORMALIZATION_VERSION, PARSER_VERSION, PER_ANIMAL_CROP_IDS, Source
from farmuncle_pipeline.core.identity_client import IdentityClient
from farmuncle_pipeline.core.quality_scoring import compute_quality
from farmuncle_pipeline.core.resource_client import parse_agmarknet_record


@dataclass(frozen=True)
class ProcessResult:
    """Purpose: the price-row dicts ready for `price_writer.upsert_price_rows`,
    plus how many input records were skipped (malformed or failed identity
    resolution) — callers fold this count into their own `rows_failed`."""
    price_rows: list[dict]
    rows_failed: int


def process_records(
    records: list,
    *,
    identity: IdentityClient,
    unit: str,
    source: Source,
    batch_id: str,
    raw_api_batch_id: str,
    job_name: str,
) -> ProcessResult:
    """
    Purpose:
        Convert raw government-API records into `mandi_daily_prices`
        row dicts: parse, resolve identity, score quality. A record
        that fails to parse (missing required fields — see
        `parse_agmarknet_record`) or fails identity resolution (an RPC
        call itself erroring — see `IdentityClient`) is counted and
        skipped, not raised, so one bad record in a page of hundreds
        doesn't abort the rest.
    Inputs:
        records: raw record dicts from a resource's `records` list
            (Resource 1 or Resource 2 — same field shape, see
            `resource_client.parse_agmarknet_record`).
        identity: a single `IdentityClient` instance for this run (so
            memoization works across every record processed).
        unit: already-normalized unit string (see
            `IdentityClient.resolve_unit`) — neither resource carries a
            per-row unit field, so callers pass one fixed, normalized
            default. Used only for `resolve_crop`'s first-creation
            bookkeeping (`crops.unit` when a brand-new crop is inserted);
            the actual per-row `unit` stored on `mandi_daily_prices` is
            computed independently below, per-crop, since 2026-08-03
            (see `PER_ANIMAL_CROP_IDS` in config.py).
        source: `Source.RESOURCE_1` or `Source.RESOURCE_2` — tags every
            resulting row and is passed to `compute_quality` for its
            `source_confidence` component.
        batch_id / raw_api_batch_id: lineage fields (invariant 8),
            stamped onto every resulting row.
        job_name: used only for the print statement on an identity-
            resolution failure, so log output is attributable to the
            calling script.
    Outputs:
        `ProcessResult`.
    Failure modes:
        None raised — see Purpose.
    """
    price_rows: list[dict] = []
    rows_failed = 0

    for rec in records:
        parsed = parse_agmarknet_record(rec)
        if parsed is None:
            rows_failed += 1
            continue

        try:
            mandi = identity.resolve_mandi(
                name=parsed["market"],
                state=parsed["state"],
                district=parsed["district"],
                source=source,
            )
            crop = identity.resolve_crop(name=parsed["commodity"], unit=unit, source=source)
            variety = identity.resolve_variety(parsed["raw_variety"])
            grade = identity.resolve_grade(parsed["raw_grade"])
            # 2026-07-26: FK-id counterparts to the text variety/grade above
            # (see identity_client.py's resolve_variety_id/resolve_grade_id
            # docstrings) — same try/except as everything else in this block
            # since these are real RPC calls that can fail like any other
            # identity resolution here.
            variety_id = identity.resolve_variety_id(crop_id=crop.id, variety=variety)
            grade_id = identity.resolve_grade_id(variety_id=variety_id, grade=grade)
        except ConfigError as exc:
            print(f"[{job_name}] identity resolution failed, skipping row: {exc}")
            rows_failed += 1
            continue

        # 2026-08-03 fix: government price is Rs/Quintal for everything
        # except real livestock (per-animal transactions) — see
        # PER_ANIMAL_CROP_IDS docstring in config.py. Convert to true
        # Rs/kg here, per-row, now that we know which crop this is;
        # the `unit` param passed into this function is only used above
        # for resolve_crop's first-creation bookkeeping, not for what
        # gets stored on the row.
        is_animal = crop.id in PER_ANIMAL_CROP_IDS
        row_unit = "animal" if is_animal else "kg"
        if is_animal:
            row_modal_price = parsed["modal_price"]
            row_min_price = parsed["min_price"]
            row_max_price = parsed["max_price"]
        else:
            row_modal_price = parsed["modal_price"] / 100 if parsed["modal_price"] is not None else None
            row_min_price = parsed["min_price"] / 100 if parsed["min_price"] is not None else None
            row_max_price = parsed["max_price"] / 100 if parsed["max_price"] is not None else None

        quality = compute_quality(
            source=source,
            modal_price=row_modal_price,
            min_price=row_min_price,
            max_price=row_max_price,
            variety=variety,
            mandi_newly_created=mandi.first_seen_this_run,
            crop_newly_created=crop.first_seen_this_run,
        )

        price_rows.append(
            {
                "mandi_id": mandi.id,
                "crop_id": crop.id,
                "variety": variety,
                "grade": grade,
                "variety_id": variety_id,
                "grade_id": grade_id,
                "price_date": parsed["price_date"],
                "modal_price": row_modal_price,
                "min_price": row_min_price,
                "max_price": row_max_price,
                "unit": row_unit,
                "source": source.value,
                # 2026-07-25 fix: these were never being written at all (confirmed
                # via full-codebase grep — zero prior references). source_mandi_id
                # is the resolved mandi id at write time; source_mandi_name is the
                # literal raw market string as reported by the government record
                # for THIS row, independent of whatever mandi it resolved to.
                # KNOWN LIMITATION: once two mandis are merged (union_merge_mandi),
                # future records under either original raw name will both resolve
                # to the survivor's mandi.id here, so source_mandi_id alone will
                # not keep re-distinguishing them going forward — only
                # source_mandi_name (the raw string) will. A fully durable fix
                # needs a raw-source identity independent of the canonical
                # mandi_id, which the schema doesn't have yet.
                "source_mandi_id": mandi.id,
                "source_mandi_name": parsed["market"],
                "batch_id": batch_id,
                "raw_api_batch_id": raw_api_batch_id,
                "parser_version": PARSER_VERSION,
                "normalization_version": NORMALIZATION_VERSION,
                "quality_score": quality.score,
                "quality_components": quality.components,
            }
        )

    return ProcessResult(price_rows=price_rows, rows_failed=rows_failed)
