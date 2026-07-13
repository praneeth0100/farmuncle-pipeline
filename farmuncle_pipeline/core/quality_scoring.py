"""
FarmUncle v2 — quality_scoring.py
Phase C, Step 14 (shared helper — daily_rewrite.py, Step 15, needs the
identical component definitions for Resource 2 rows, so this is a
standalone module rather than logic embedded in live_tick.py).

Purpose (module-level):
    Computes `mandi_daily_prices.quality_score` (0-1) and its
    `quality_components` (jsonb) breakdown. The schema reserves this
    shape as of Step 9 but explicitly defers the computation itself:
    "Exact computation logic is a Phase C concern (daily_rewrite/
    live_tick will populate it)." This module is that concern.

Components (four, matching the schema-reservation comment at spec §7):
    source_confidence — how much this resource is trusted in isolation,
        per §8's precedence order. Resource 1 is "best-effort" (§9);
        Resource 2 is "authoritative"; manual is human-approved
        (always wins). This is a static per-source value, not
        per-row-computed.
    completeness — whether this row has everything a price record
        should have: modal/min/max price and a real (non-blank)
        variety.
    entity_verified — whether the mandi/crop this row references is
        already an established entity, vs. one this exact call just
        auto-created. Deliberately read from the identity-resolution
        module's own cache-hit/miss signal (see `identity.py`) rather
        than issuing a second query per row to check
        `review_status` — a freshly AUTO_CREATED entity is inherently
        less trustworthy than one already seen before, and the caller
        already knows which happened without an extra round-trip.
    price_sanity — whether min <= modal <= max and all three are
        positive numbers.

    quality_score is the unweighted mean of the four components. A
    weighted scheme is not specified anywhere in the Master Build
    Specification, so an equal-weight mean is the least-assuming
    choice available; revisiting the weights (if real data suggests
    one component should dominate) is a follow-up, not something to
    guess at now.

Explicitly out of scope for this file:
    - Any database read/write (pure function, no Supabase dependency)
    - Identity resolution itself (caller passes in whether the entity
      was newly created)
"""

from __future__ import annotations

from dataclasses import dataclass

from farmuncle_pipeline.config import Source


# Static per-source confidence, per spec §8's precedence order
# (manual > resource_2 > resource_1 > auto-created/review-queue
# defaults). MANUAL is out of scope for live_tick/daily_rewrite (only
# ever written by the future manual-review tooling), included here so
# this table is the single place all three live, per Never-Do Rules §2.
_SOURCE_CONFIDENCE: dict[Source, float] = {
    Source.MANUAL: 1.0,
    Source.RESOURCE_2: 0.9,
    Source.RESOURCE_1: 0.7,
}


@dataclass(frozen=True)
class QualityResult:
    """Purpose: bundles the scalar score with its component breakdown,
    ready to drop straight into a `mandi_daily_prices` insert payload
    (`quality_score=result.score`, `quality_components=result.components`)."""
    score: float
    components: dict[str, float]


def compute_quality(
    *,
    source: Source,
    modal_price: float | None,
    min_price: float | None,
    max_price: float | None,
    variety: str | None,
    mandi_newly_created: bool,
    crop_newly_created: bool,
) -> QualityResult:
    """
    Purpose:
        Compute the four-component quality score for one
        `mandi_daily_prices` row, per this module's docstring.
    Inputs:
        source: which resource/manual this row came from.
        modal_price / min_price / max_price: the row's price fields
            (already parsed to float; None if missing/unparsable).
        variety: the row's variety string (already normalized by the
            caller via the `normalize_variety` RPC).
        mandi_newly_created / crop_newly_created: True if this
            specific `find_or_create_*` call created a brand-new
            entity rather than resolving an existing one (i.e. a
            cache/lookup miss in the calling script's identity
            resolution for this run).
    Outputs:
        `QualityResult` with `score` in [0, 1] and a `components` dict
        with keys "source_confidence", "completeness",
        "entity_verified", "price_sanity".
    Failure modes:
        None — always returns a result; missing/invalid inputs lower
        the relevant component score rather than raising, since a
        malformed row should still be storable (and visibly
        low-quality), not rejected outright by this function (schema-
        level constraints like `chk_prices_min_max` are the actual
        gate for rows that can't be stored at all).
    """
    source_confidence = _SOURCE_CONFIDENCE.get(source, 0.5)

    has_prices = modal_price is not None and min_price is not None and max_price is not None
    has_variety = bool(variety and variety.strip())
    completeness = (1.0 if has_prices else 0.0) * 0.75 + (1.0 if has_variety else 0.0) * 0.25

    entity_verified = 1.0
    if mandi_newly_created:
        entity_verified -= 0.5
    if crop_newly_created:
        entity_verified -= 0.5
    entity_verified = max(entity_verified, 0.0)

    if has_prices and min_price >= 0 and max_price >= 0 and modal_price >= 0:
        price_sanity = 1.0 if (min_price <= modal_price <= max_price) else 0.3
    else:
        price_sanity = 0.0

    components = {
        "source_confidence": round(source_confidence, 4),
        "completeness": round(completeness, 4),
        "entity_verified": round(entity_verified, 4),
        "price_sanity": round(price_sanity, 4),
    }
    score = round(sum(components.values()) / len(components), 4)

    return QualityResult(score=score, components=components)
