"""Undo a decimal slip by comparing a price with the same crop in other markets.

A mandi that types Rs/kg into the per-quintal field arrives 100x too small after the divide-by-100
(raw 40 -> 0.4 while peers sit at 40). If the converted price is far below the crop's median and
multiplying by a power of ten lands next to that median, multiply it back.
"""

from __future__ import annotations

# Only powers of ten with a mechanism: 100 undoes the quintal divide, 1000 covers per-1000 quotes.
# 10 is excluded on purpose: natural spread (variety, grade, state) already puts honest prices there.
PEER_FACTORS = (100, 1000)
TRIGGER_BELOW = 0.1        # only prices under this fraction of the crop median are examined
MATCH_BAND = (0.4, 2.0)    # the multiplied price must land within this range of the median


def peer_scale(modal, min_price, max_price, median):
    """(factor, modal, min, max) with the prices multiplied back, or None to leave the row alone."""
    if modal is None or not median or modal >= median * TRIGGER_BELOW:
        return None
    fits = [(abs(modal * f / median - 1), f) for f in PEER_FACTORS if MATCH_BAND[0] <= modal * f / median <= MATCH_BAND[1]]
    if not fits:
        return None
    _, factor = min(fits)
    scale = lambda v: None if v is None else round(v * factor, 2)  # undo float noise (0.07*100)
    return factor, scale(modal), scale(min_price), scale(max_price)
