"""
Name normalization and fuzzy matching for mandi <-> Places-result names.

This is a direct port of the (already correct, already live-tested)
_core_name / _name_match_score / extract_place_name logic from
geocode_mandis.py's Geocoding-API version -- that part of the old
script was never the problem. What's changing in the v2 pipeline is
WHICH Google API is queried and WHAT counts as an acceptable result
(see providers.py and scorer.py); the name-comparison plumbing itself
is reused as-is rather than reinvented.
"""

import re
from difflib import SequenceMatcher

# Generic suffix/filler words that appear in mandi names but carry no
# place-identifying information. Kept in sync with geocode_mandis.py's
# list of the same name/purpose -- if you add a word there, add it here.
CORE_NAME_FILLER_WORDS = {
    "apmc", "mandi", "market", "committee", "sub-yard", "sub", "yard",
    "vfpck", "grain", "veg", "new", "old", "north", "south", "main",
    "tal", "taluka", "dist", "district", "agriculture", "produce",
    "uzhavar", "sandhai", "sandha",
}


def normalize_token(s: str) -> str:
    """Lowercase, strip whitespace/punctuation, so 'Sanwer' and 'sanwer,'
    compare equal."""
    return re.sub(r"[^a-z0-9]", "", s.lower())


def core_name(raw: str) -> str:
    """Strip parentheticals and generic filler words to get a place's
    "core" name for comparison, e.g. "Agriculture Produce Market
    Committee Katol" -> "katol"."""
    no_parens = re.sub(r"\([^)]*\)", " ", raw.lower())
    tokens = [
        t for t in re.split(r"[^a-z0-9]+", no_parens)
        if t and t not in CORE_NAME_FILLER_WORDS
    ]
    return " ".join(tokens)


def name_match_score(name_a: str, name_b: str) -> float:
    """Fuzzy match score (0-1) between two core names, combining a
    whole-string similarity ratio with a containment check."""
    ca, cb = core_name(name_a), core_name(name_b)
    if not ca or not cb:
        return 0.0
    ratio = SequenceMatcher(None, ca, cb).ratio()
    words_a, words_b = set(ca.split()), set(cb.split())
    contained = bool(words_a & words_b)
    return max(ratio, 1.0 if contained else 0.0)


def extract_place_name(name: str) -> str:
    """Strip common market/organization suffixes to get the underlying
    village/town name, e.g. "Amballur VFPCK Market" -> "Amballur". Used
    as a secondary query variant, never as the primary strategy -- the
    whole point of the v2 redesign is to find the MARKET, not settle
    for the town it sits in."""
    suffixes_to_strip = [
        "vfpck market", "vfpck  market", "market", "apmc",
        "sub-yard", "sub yard", "mandi", "vfpck",
    ]
    cleaned = name.strip()
    cleaned_lower = cleaned.lower()
    for suffix in suffixes_to_strip:
        if cleaned_lower.endswith(suffix):
            cleaned = cleaned[: len(cleaned) - len(suffix)].strip()
            cleaned_lower = cleaned.lower()
    if "(" in cleaned:
        cleaned = cleaned.split("(")[0].strip()
    return cleaned
