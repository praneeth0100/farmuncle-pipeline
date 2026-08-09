"""
State-aware agricultural market terminology.

Different Indian states call the same kind of place different things
(APMC in Maharashtra/Karnataka, Rythu Bazar in AP/Telangana, Uzhavar
Sandhai in Tamil Nadu, VFPCK market in Kerala, etc). A single generic
query template ("<name> market, <district>, <state>") systematically
under-performs in states whose real terminology doesn't contain the
word "market" at all -- Google Places is a text-match search, not a
concept search, so the actual local term needs to be in the query.

This is intentionally just a plain dict, not a class, so it's trivial
to extend: add a state, add a term to its list. Order matters a little
(first term is tried first in query generation) but isn't critical --
the scorer in scorer.py is what actually decides what counts as a
market-word match, not just this list's presence/absence.

DEFAULT_MARKET_TERMS is used for any state/UT not explicitly listed
below, so an unlisted state never falls through to zero search terms.
"""

STATE_MARKET_TERMS = {
    "Kerala": [
        "VFPCK", "farmers market", "wholesale market",
        "agricultural market", "regulated market", "market",
    ],
    "Tamil Nadu": [
        "Uzhavar Sandhai", "regulated market", "agricultural market",
        "market yard", "farmers market",
    ],
    "Andhra Pradesh": [
        "Rythu Bazar", "market yard", "Agricultural Market Committee",
        "AMC", "agricultural market",
    ],
    "Telangana": [
        "Rythu Bazar", "market yard", "Agricultural Market Committee",
        "AMC", "agricultural market",
    ],
    "Haryana": [
        "APMC", "New Grain Market", "Anaj Mandi", "Mandi",
        "Market Committee", "Market Yard",
    ],
    "Punjab": [
        "Mandi Board", "Grain Market", "Anaj Mandi",
        "Market Committee", "Mandi",
    ],
    "Rajasthan": [
        "Krishi Upaj Mandi", "Mandi Samiti", "Anaj Mandi",
        "Agricultural Market",
    ],
    "Uttar Pradesh": [
        "Krishi Utpadan Mandi Samiti", "Mandi Samiti", "Mandi",
        "Agricultural Market",
    ],
    "Madhya Pradesh": [
        "Krishi Upaj Mandi", "Mandi Samiti", "Mandi", "Market Yard",
    ],
    "Maharashtra": [
        "APMC", "Agricultural Produce Market Committee", "Market Yard",
        "Krushi Utpanna Bazar Samiti", "Mandi",
    ],
    "Karnataka": [
        "APMC", "Agricultural Produce Market", "Regulated Market",
        "Market Yard",
    ],
    "Gujarat": [
        "APMC", "Agricultural Market", "Market Yard",
    ],
    "West Bengal": [
        "Regulated Market Committee", "wholesale market",
        "agricultural market",
    ],
    "Odisha": [
        "Regulated Market Committee", "Market Yard", "agricultural market",
    ],
    "Chhattisgarh": [
        "Krishi Upaj Mandi", "Mandi Samiti", "Mandi",
    ],
    "Bihar": [
        "agricultural market", "market yard", "regulated market",
    ],
    "Jammu and Kashmir": [
        "fruit and vegetable market", "F&V mandi", "wholesale market",
        "agricultural market",
    ],
}

# Every state/UT not explicitly listed above falls back to this list,
# so query generation never comes up empty for an unmapped state.
DEFAULT_MARKET_TERMS = [
    "APMC", "agricultural market", "mandi", "market yard",
    "wholesale market", "regulated market",
]

# Words that, if present in a Places result's display name, count as a
# positive "this looks like a market, not just a town" signal in the
# scorer -- deliberately broader than any one state's term list, since
# a result can legitimately be named using a different state's
# terminology (data entry inconsistency) or a generic English term.
MARKET_INDICATOR_WORDS = {
    "apmc", "mandi", "market", "bazar", "bazaar", "sandhai", "santhai",
    "sandha", "rythu", "vfpck", "krishi", "upaj", "samiti", "yard",
    "committee", "anaj", "grain", "wholesale", "regulated", "amc",
    "produce", "utpanna", "utpadan", "krushi",
}


def market_terms_for_state(state: str):
    """Returns the ordered list of local market terminology for a state,
    falling back to DEFAULT_MARKET_TERMS for anything not explicitly
    mapped. Case-insensitive match on state name."""
    if not state:
        return DEFAULT_MARKET_TERMS
    for known_state, terms in STATE_MARKET_TERMS.items():
        if known_state.lower() == state.strip().lower():
            return terms
    return DEFAULT_MARKET_TERMS
