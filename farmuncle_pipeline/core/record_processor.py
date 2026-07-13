"""
FarmUncle v2 — record_processor.py
Phase C, Step 17 (extracted from Step 14/15's `live_tick.py` and
`resource2_pipeline.py`).

Purpose (module-level):
    The "turn a list of raw government-API records into
    `mandi_daily_prices`-ready row dicts" step: parse each record
    (`resource_client.parse_agmarknet_record`), resolve its mandi/crop/
    variety through the RPC-backed `IdentityClient` (invariant 3 — no
    identity logic duplicated in Python), score its quality
    (`quality_scoring.compute_quality`), and assemble the row dict
    `price_writer.upsert_price_rows` expects.

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

from farmuncle_pipeline.config import ConfigError, NORMALIZATION_VERSION, PARSER_VERSION, Source
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
            default.
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
        except ConfigError as exc:
            print(f"[{job_name}] identity resolution failed, skipping row: {exc}")
            rows_failed += 1
            continue

        quality = compute_quality(
            source=source,
            modal_price=parsed["modal_price"],
            min_price=parsed["min_price"],
            max_price=parsed["max_price"],
            variety=variety,
            mandi_newly_created=mandi.first_seen_this_run,
            crop_newly_created=crop.first_seen_this_run,
        )

        price_rows.append(
            {
                "mandi_id": mandi.id,
                "crop_id": crop.id,
                "variety": variety,
                "price_date": parsed["price_date"],
                "modal_price": parsed["modal_price"],
                "min_price": parsed["min_price"],
                "max_price": parsed["max_price"],
                "unit": unit,
                "source": source.value,
                "batch_id": batch_id,
                "raw_api_batch_id": raw_api_batch_id,
                "parser_version": PARSER_VERSION,
                "normalization_version": NORMALIZATION_VERSION,
                "quality_score": quality.score,
                "quality_components": quality.components,
            }
        )

    return ProcessResult(price_rows=price_rows, rows_failed=rows_failed)
