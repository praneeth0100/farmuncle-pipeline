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
    # Generic institutional/organizational words. Added 2026-08-10 after
    # a live incident: "Sahnewal APMC Mandi Board" vs "Punjab Mandi
    # Board Ludhiana" shared only the word "board" post-filtering, which
    # (pre-fix) was enough for name_match_score's containment rule to
    # force a perfect 1.0 match -- a district-level government office
    # got accepted as if it were the specific local market. Without
    # these in the filler set, ANY shared institutional word between a
    # mandi name and a state/district-level board's name causes the
    # same false match, regardless of the actual place. Stripping them
    # forces the comparison down to the real place-identifying tokens.
    "board", "society", "samiti", "office", "department", "govt",
    "government", "corporation", "union", "sangh", "seva",
    "cooperative", "co-operative", "state", "krishi", "upaj",
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
    whole-string similarity ratio with a containment check.

    NOTE on the containment bonus: this used to force a flat 1.0
    whenever the two names shared ANY post-filler word, on the theory
    that a shared token is strong evidence of the same place ("Katol"
    appearing in "Agriculture Produce Market Committee Katol"). That
    reasoning breaks down for institutional-sounding names that still
    have one coincidentally-shared word after filtering -- confirmed
    live 2026-08-10 with "Sahnewal ... Board" vs "Punjab ... Board
    Ludhiana" scoring a perfect 1.0 off the single word "board", even
    though the rest of the names share nothing and refer to completely
    different places. Expanding CORE_NAME_FILLER_WORDS closes that
    specific case, but as defense-in-depth this now gives containment a
    bounded bonus on top of the real similarity ratio instead of an
    outright override -- a single weak/generic shared word can no
    longer manufacture a perfect score by itself, while genuine
    same-place matches (which also share most of the rest of the
    string, so `ratio` is already decent) still land near 1.0."""
    ca, cb = core_name(name_a), core_name(name_b)
    if not ca or not cb:
        return 0.0
    ratio = SequenceMatcher(None, ca, cb).ratio()
    words_a, words_b = set(ca.split()), set(cb.split())
    contained = bool(words_a & words_b)
    return min(1.0, ratio + (0.35 if contained else 0.0))


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
