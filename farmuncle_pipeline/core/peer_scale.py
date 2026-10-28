"""Undo a decimal slip by comparing a price with the same crop in other markets.

A mandi that types Rs/kg into the per-quintal field arrives 100x too small after the divide-by-100
(raw 40 -> 0.4 while peers sit at 40). If the converted price is far below the crop's median and
multiplying by 100 lands next to that median, multiply it back.
"""

from __future__ import annotations

# Only x100: it undoes the quintal -> kg divide for a mandi that typed Rs/kg. Nothing else is guessed.
PEER_FACTOR = 100
TRIGGER_BELOW = 0.1        # only prices under this fraction of the crop median are examined
MATCH_BAND = (0.3, 3.0)    # the multiplied price must land within this range of the median


def peer_scale(modal, min_price, max_price, median):
    """(factor, modal, min, max) with the prices multiplied back, or None to leave the row alone."""
    if modal is None or not median or modal >= median * TRIGGER_BELOW:
        return None
    if not MATCH_BAND[0] <= modal * PEER_FACTOR / median <= MATCH_BAND[1]:
        return None
    scale = lambda v: None if v is None else round(v * PEER_FACTOR, 2)  # undo float noise (0.07*100)
    return PEER_FACTOR, scale(modal), scale(min_price), scale(max_price)
