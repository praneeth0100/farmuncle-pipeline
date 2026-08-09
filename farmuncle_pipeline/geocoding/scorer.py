"""
Candidate scoring and confidence classification.

This is the piece that actually enforces "market yard, not town centre"
-- providers.py filters out pure-admin-type results structurally, but a
result can pass that filter (have a real POI type) and still be a bad
match (wrong place, wrong district, or just Google/OSM's nearest guess
at an unindexed name). This module decides, for everything that
survives the type filter, how much to trust it.

Weights are intentionally plain module-level constants, not buried
inline, so they're easy to tune later without hunting through logic --
per the blueprint's "make them configurable" instruction.
"""

from .name_normalizer import core_name, name_match_score
from .market_terminology import MARKET_INDICATOR_WORDS
from .providers import haversine_km

# --- scoring weights (tune here, not inline) -------------------------
WEIGHT_NAME_MATCH_MAX = 35        # scaled by name_match_score (0-1)
WEIGHT_MARKET_WORD_IN_RESULT = 20 # candidate's own name contains a market term
WEIGHT_DISTRICT_TOKEN_MATCH = 15
WEIGHT_STATE_TOKEN_MATCH = 10
WEIGHT_POI_TYPE_BONUS = 10        # types suggest a real commercial/market place
WEIGHT_CROSS_CHECK_AGREEMENT = 15 # both providers agree within CROSS_CHECK_KM

CROSS_CHECK_KM = 3.0              # Google/OSM points this close count as "agree"
DEFAULT_DISTANCE_REJECT_KM = 60.0 # hard reject beyond this from district reference

# Places "types"/primaryType values that specifically suggest a market/
# commerce place (beyond just "not purely administrative"). Bonus only
# -- absence doesn't reject, since Places' type vocabulary for Indian
# APMC yards is inconsistent (often just "point_of_interest,establishment").
_MARKET_LIKE_TYPES = {
    "market", "grocery_or_supermarket", "supermarket", "food",
    "grocery_store", "wholesaler", "farm",
}


def _contains_token(haystack: str, needle: str) -> bool:
    if not haystack or not needle:
        return False
    return needle.strip().lower() in haystack.lower()


def score_candidate(candidate, mandi_name, district, state, distance_km=None):
    """
    Returns a dict: {score, reasons: [...], hard_reject: bool, reject_reason}.
    Does NOT itself decide accept/reject on score alone -- that's
    classify_confidence's job -- but DOES hard-reject here for things no
    score should be able to overcome (admin-only type, out-of-district
    distance), matching the blueprint's negative-scores-plus-hard-rejects
    split in §10/§13.
    """
    reasons = []

    if candidate.is_admin_only():
        return {
            "score": 0, "reasons": ["admin-only type (locality/political/etc), no POI signal"],
            "hard_reject": True, "reject_reason": "admin_only_type",
        }

    if distance_km is not None and distance_km > DEFAULT_DISTANCE_REJECT_KM:
        return {
            "score": 0,
            "reasons": [f"{distance_km:.1f}km from district reference point, exceeds {DEFAULT_DISTANCE_REJECT_KM}km bound"],
            "hard_reject": True, "reject_reason": "distance_out_of_bound",
        }

    score = 0.0

    nm_score = name_match_score(mandi_name, candidate.name)
    score += nm_score * WEIGHT_NAME_MATCH_MAX
    reasons.append(f"name_match={nm_score:.2f} (+{nm_score * WEIGHT_NAME_MATCH_MAX:.1f})")

    cand_core = core_name(candidate.name)
    if any(word in cand_core.split() or word in candidate.name.lower() for word in MARKET_INDICATOR_WORDS):
        score += WEIGHT_MARKET_WORD_IN_RESULT
        reasons.append(f"market word in result name (+{WEIGHT_MARKET_WORD_IN_RESULT})")

    if _contains_token(candidate.formatted_address, district):
        score += WEIGHT_DISTRICT_TOKEN_MATCH
        reasons.append(f"district '{district}' in formatted_address (+{WEIGHT_DISTRICT_TOKEN_MATCH})")

    if _contains_token(candidate.formatted_address, state):
        score += WEIGHT_STATE_TOKEN_MATCH
        reasons.append(f"state '{state}' in formatted_address (+{WEIGHT_STATE_TOKEN_MATCH})")

    type_hit = candidate.place_types & _MARKET_LIKE_TYPES
    if type_hit:
        score += WEIGHT_POI_TYPE_BONUS
        reasons.append(f"market-like type {type_hit} (+{WEIGHT_POI_TYPE_BONUS})")

    return {"score": round(score, 1), "reasons": reasons, "hard_reject": False, "reject_reason": None}


def cross_check_agrees(cand_a, cand_b) -> bool:
    if not cand_a or not cand_b:
        return False
    if cand_a.lat is None or cand_b.lat is None:
        return False
    return haversine_km(cand_a.lat, cand_a.lng, cand_b.lat, cand_b.lng) <= CROSS_CHECK_KM


def classify_confidence(score: float, has_cross_check_agreement: bool) -> str:
    """
    Bands per the blueprint (§11), with the cross-check agreement bonus
    already meant to be folded into `score` by the caller before this is
    called (WEIGHT_CROSS_CHECK_AGREEMENT) -- this function just applies
    the final cutoffs, plus one extra rule: a bare pass (50-69) that also
    has cross-check agreement gets bumped to MEDIUM, since two
    independent providers landing on the same point is real corroborating
    evidence even when the name-match signal alone was middling.
    """
    if score >= 90:
        return "HIGH"
    if score >= 70:
        return "MEDIUM"
    if score >= 50:
        return "MEDIUM" if has_cross_check_agreement else "LOW"
    if score > 0:
        return "REVIEW"
    return "UNRESOLVED"
