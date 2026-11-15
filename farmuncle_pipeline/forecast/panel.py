"""Weekly price panel for the forecast, read from the published chart files (the same numbers the app draws).

One series per crop + mandi + variety: grades merged the way the app merges them (on each day, and each week of older
history, the grade with the highest price wins), then the daily part averaged into Monday weeks. Chart files already follow
mandi aliases and merges, so nothing here re-maps ids. Weather comes from weather/history/{cell}.json (NASA POWER weeks).
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

FESTIVALS = Path(__file__).with_name("festivals.json")  # copied from the app (src/features/mandi-market/data)
WEATHER_START = date(2016, 1, 4)


def monday(d: date) -> date:
    return d - timedelta(days=d.weekday())


def chart_weeks(chart: dict) -> list[tuple]:
    """[(variety, week Monday, mean modal, report days or None)] for one chart file. Report days are known only for the daily
    part (about the last year); older weeks come pre-averaged and carry None."""
    out = []
    start = date.fromisoformat(chart["start"])
    daily: dict[str, dict[int, float]] = {}
    for line in chart.get("lines", []):
        best = daily.setdefault(line["variety"], {})
        for off, modal, *_ in line["days"]:
            if modal and modal > best.get(off, 0):
                best[off] = modal
    seen: set[tuple] = set()
    for variety, days in daily.items():
        weeks: dict[date, list[float]] = {}
        for off, modal in days.items():
            weeks.setdefault(monday(start + timedelta(days=off)), []).append(modal)
        for w, vals in weeks.items():
            out.append((variety, w, sum(vals) / len(vals), len(vals)))
            seen.add((variety, w))
    if chart.get("weekly_start"):
        w0 = date.fromisoformat(chart["weekly_start"])
        weekly: dict[str, dict[int, float]] = {}
        for line in chart.get("weekly", []):
            best = weekly.setdefault(line["variety"], {})
            for i, modal, *_ in line["weeks"]:
                if modal and modal > best.get(i, 0):
                    best[i] = modal
        for variety, weeks_ in weekly.items():
            for i, modal in weeks_.items():
                w = w0 + timedelta(weeks=i)
                if (variety, w) not in seen:  # the week the daily part starts in: the daily numbers win
                    out.append((variety, w, modal, None))
    return out


def pairs(get_json) -> list[tuple[str, str]]:
    """(crop, mandi) for every mandi in a live crop file: the pairs the app can open."""
    crops = (get_json("live/crop_states.json") or {}).get("crops", {})
    keys = [(c, s) for c, states in crops.items() for s in states]
    with ThreadPoolExecutor(32) as pool:
        files = pool.map(lambda cs: get_json(f"live/crop/{cs[0]}/{cs[1]}.json"), keys)
        return sorted({(c, r["mandi_id"]) for (c, _), f in zip(keys, files) for r in (f or {}).get("rows", [])})


def load_panel(get_json, crop_mandis: list[tuple[str, str]], workers: int = 32) -> pd.DataFrame:
    """Columns crop, mandi, variety, week (datetime64), price, days. Missing chart files are skipped."""
    def one(cm):
        chart = get_json(f"chart/{cm[0]}/{cm[1]}.json")
        return [(cm[0], cm[1], *row) for row in chart_weeks(chart)] if chart else []
    with ThreadPoolExecutor(workers) as pool:
        rows = [r for part in pool.map(one, crop_mandis) for r in part]
    df = pd.DataFrame(rows, columns=["crop", "mandi", "variety", "week", "price", "days"])
    df["week"] = pd.to_datetime(df["week"])
    df["days"] = df["days"].astype("float32")
    return df.sort_values(["crop", "mandi", "variety", "week"], ignore_index=True)


def festival_weeks() -> np.ndarray:
    """Sorted Monday weeks (datetime64[D]) that hold a festival."""
    data = json.loads(FESTIVALS.read_text(encoding="utf-8"))
    return np.array(sorted({monday(date.fromisoformat(d)) for f in data["festivals"] for d in f["dates"]}), dtype="datetime64[D]")


def load_weather(get_json, mandis: list[str], workers: int = 32) -> pd.DataFrame:
    """Columns mandi, week, rain, tmax: weekly rain total (mm) and hottest day (°C) at the mandi's grid cell."""
    cells = (get_json("catalog/weather_cells.json") or {}).get("cells", {})
    wanted = sorted({cells[m] for m in mandis if m in cells})
    with ThreadPoolExecutor(workers) as pool:
        files = dict(zip(wanted, pool.map(lambda c: get_json(f"weather/history/{c}.json"), wanted)))
    rows = []
    for cell, f in files.items():
        if not f:
            continue
        start = date.fromisoformat(f.get("start", WEATHER_START.isoformat()))
        rows += [(cell, start + timedelta(weeks=i), w[0], w[1]) for i, w in enumerate(f["weeks"]) if w]
    by_cell = pd.DataFrame(rows, columns=["cell", "week", "rain", "tmax"])
    by_cell["week"] = pd.to_datetime(by_cell["week"])
    link = pd.DataFrame([(m, cells[m]) for m in mandis if m in cells], columns=["mandi", "cell"])
    return link.merge(by_cell, on="cell").drop(columns="cell")
