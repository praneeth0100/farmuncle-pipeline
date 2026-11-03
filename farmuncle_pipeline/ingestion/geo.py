"""Mandi places for the app files: coordinates for every mandi, display labels, and who is near whom."""

from __future__ import annotations

import math
import re
from collections import defaultdict
from statistics import mean

CELL = 2.0  # degrees; 200 km is under 2 cells of longitude anywhere in India


def fill_coords(mandis: list[dict]) -> dict[str, tuple[float, float, bool]]:
    """public_id -> (lat, lng, approx). A mandi without coordinates takes its district's centre, else its state's,
    marked approx. A state where no mandi is placed stays out (nothing honest to put there)."""
    district, state, out = defaultdict(list), defaultdict(list), {}
    for m in mandis:
        if m.get("latitude") is not None and m.get("longitude") is not None:
            point = (m["latitude"], m["longitude"])
            out[m["public_id"]] = (*point, False)
            district[".".join(m["public_id"].split(".")[:2])].append(point)
            state[m["state"]].append(point)
    for m in mandis:
        if m["public_id"] in out:
            continue
        points = district.get(".".join(m["public_id"].split(".")[:2])) or state.get(m["state"])
        if points:
            out[m["public_id"]] = (mean(p[0] for p in points), mean(p[1] for p in points), True)
    return out


def _clean(name: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"\s*\bAPMC\b\s*", " ", name, flags=re.I)).strip()


def labels(mandis: list[dict]) -> dict[str, str]:
    """Display names, as the app showed them: a name used twice in a state gets its yard words from the id
    ("Bengaluru · Binny Mill")."""
    seen: dict[str, int] = defaultdict(int)
    for m in mandis:
        seen[f"{m['state']}:{_clean(m.get('display_name') or m['name']).lower()}"] += 1
    out = {}
    for m in mandis:
        name = _clean(m.get("display_name") or m["name"])
        if seen[f"{m['state']}:{name.lower()}"] < 2:
            out[m["public_id"]] = name
            continue
        words = set(re.split(r"\W+", name.lower()))
        extra = [w for w in m["public_id"].split(".")[2].split("_")
                 if len(w) > 1 and w not in words and w not in ("ff", "apmc", "yard", "sub")]
        out[m["public_id"]] = f"{name} · {' '.join(w.capitalize() for w in extra)}" if extra else name
    return out


def km_between(a, b) -> float:
    la1, lo1, la2, lo2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2
    return 12742 * math.asin(math.sqrt(h))


def neighbours(coords: dict, km: int = 200) -> dict[str, list[tuple[str, int]]]:
    """public_id -> [(other, km)] within `km`, nearest first. A grid of 2-degree cells keeps it far from all pairs."""
    grid = defaultdict(list)
    for pid, (lat, lng, _) in coords.items():
        grid[(math.floor(lat / CELL), math.floor(lng / CELL))].append(pid)
    out = {}
    for pid, (lat, lng, _) in coords.items():
        y, x = math.floor(lat / CELL), math.floor(lng / CELL)
        near = []
        for dy in (-1, 0, 1):
            for dx in (-2, -1, 0, 1, 2):
                for other in grid.get((y + dy, x + dx), ()):
                    if other != pid and (d := km_between((lat, lng), coords[other])) <= km:
                        near.append((other, round(d)))
        out[pid] = sorted(near, key=lambda n: (n[1], n[0]))
    return out
