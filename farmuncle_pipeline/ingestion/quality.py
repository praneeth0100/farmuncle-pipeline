"""Prices that look wrong. They are still published, marked odd, and never chosen as a card's price while a normal one exists."""

from __future__ import annotations

from collections import defaultdict
from statistics import median


def odd_keys(rows: list[dict]) -> set[tuple]:
    """Rows have key, state, modal, min, max. Odd: modal outside its own min-max, or, among 5+ prices of the crop in the
    state, more than 4 robust deviations from their median and under half or over double it (the ₹3 tomato)."""
    odd, groups = set(), defaultdict(list)
    for r in rows:
        if r["min"] is not None and r["max"] is not None and not r["min"] <= r["modal"] <= r["max"]:
            odd.add(r["key"])
        groups[(r["key"][1], r["state"])].append(r)
    for group in groups.values():
        if len(group) < 5:
            continue
        mid = median(r["modal"] for r in group)
        spread = 1.4826 * median(abs(r["modal"] - mid) for r in group)
        for r in group:
            if abs(r["modal"] - mid) > 4 * spread and not mid / 2 <= r["modal"] <= mid * 2:
                odd.add(r["key"])
    return odd
