"""Undo a 100x decimal slip by comparing a price with the same crop in other markets.

A mandi that types Rs/kg into the per-quintal field arrives 100x too small after the divide-by-100
(raw 40 -> 0.4 while peers sit at 40). If the converted price is far below the crop's median and
multiplying by 100 lands next to that median, multiply it back. The mirror case (a price 100x
above the crop) is divided by 100.
"""

from __future__ import annotations

# Only x100: it undoes the quintal -> kg divide for a mandi that typed Rs/kg. Nothing else is guessed.
PEER_FACTOR = 100
TRIGGER_BELOW = 0.1        # only prices under this fraction of the crop median are examined
TRIGGER_ABOVE = 10.0       # ... or over this multiple of it (the mirror case)
MATCH_BAND = (0.3, 3.0)    # the multiplied price must land within this range of the median


def peer_scale(modal, min_price, max_price, median):
    """(factor, modal, min, max) with the prices scaled back (factor 100 or 0.01), or None to leave the row alone."""
    if modal is None or not median:
        return None
    if modal < median * TRIGGER_BELOW:
        factor = PEER_FACTOR
    elif modal > median * TRIGGER_ABOVE:
        factor = 1 / PEER_FACTOR
    else:
        return None
    if not MATCH_BAND[0] <= modal * factor / median <= MATCH_BAND[1]:
        return None
    scale = lambda v: None if v is None else round(v * factor, 2)  # undo float noise (0.07*100)
    return factor, scale(modal), scale(min_price), scale(max_price)
