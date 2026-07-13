"""
FarmUncle v2 — government_constants.py
Phase C, Step 14 (fills a gap left dangling by Step 13).

Purpose (module-level):
    Holds government-domain constant data — as opposed to environment
    configuration (config.py) or live-schema facts (config_validator.py).
    Right now that's just `STATES`: the fixed list of state names
    Resource 2 expects for its `filters[State]` pagination parameter
    (see spec §9's Resource 2 contract — it requires enumerating states
    to page through, unlike Resource 1 which is queried by date alone).

    `ingest_common.py` (Step 13) already does `from government_constants
    import STATES`, so this module must exist for that import to
    succeed at all — it was referenced but never delivered in the
    Step 13 zip. Adding it here, now, rather than silently working
    around the missing import.

Why this isn't in config.py:
    config.py's own docstring draws this line already: "Government-
    domain constants ... live in government_constants.py, not here."
    STATES doesn't vary by environment or deployment (unlike
    PAGE_SIZE, API_BASE_RESOURCE_1, etc., which live in system_config)
    — it's a fact about the government API's contract, so a plain
    Python constant is appropriate; it does not belong in
    `system_config` alongside genuinely environment-specific values.

Provenance:
    Reused verbatim from `sync_prices_v2.py` (the only one of the two
    reference scripts that paginates Resource 2 by state). This is a
    domain fact — the exact state-name strings the government API
    expects — not a design decision, so it is copied rather than
    re-derived. `live_tick.py` (Step 14, Resource 1 only) does not
    currently use this list; it exists so `ingest_common.py` imports
    cleanly and so it's ready when `daily_rewrite.py` (Step 15,
    Resource 2) needs it.

Explicitly out of scope for this file:
    - Anything environment-specific (belongs in system_config, per
      config.py)
    - Any parsing, normalization, or request logic
"""

from __future__ import annotations

from typing import Final

STATES: Final[tuple[str, ...]] = (
    "Andhra Pradesh",
    "Arunachal Pradesh",
    "Assam",
    "Bihar",
    "Chattisgarh",
    "Goa",
    "Gujarat",
    "Haryana",
    "Himachal Pradesh",
    "Jharkhand",
    "Karnataka",
    "Keralam",
    "Madhya Pradesh",
    "Maharashtra",
    "Manipur",
    "Meghalaya",
    "Mizoram",
    "Nagaland",
    "Odisha",
    "Punjab",
    "Rajasthan",
    "Sikkim",
    "Tamil Nadu",
    "Telangana",
    "Tripura",
    "Uttar Pradesh",
    "Uttarakhand",
    "West Bengal",
    "NCT of Delhi",
    "Jammu and Kashmir",
    "Puducherry",
    "Chandigarh",
    "Andaman and Nicobar",
)
