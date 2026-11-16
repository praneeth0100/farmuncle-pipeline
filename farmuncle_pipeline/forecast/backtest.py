"""Rolling-origin backtest against "the price stays the same", the per-crop gate, and the plain-language report.

For each origin week T: train only on rows whose outcome week is before T, then forecast every anchor in the next `step`
weeks and compare with what happened. Ratio = mean |ln(actual/forecast)| / mean |ln(actual/this week's price)| (lower is
better, < 1 beats the baseline). Bands for fold i are calibrated on folds before i, so fold 0 only warms up the calibration
and is left out of every number.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from farmuncle_pipeline.forecast.model import H, Grid, band_scale, predict, train

GATE_RATIO = 0.9
GATE_COVERAGE = (0.75, 0.85)
MIN_ROWS = 300  # forecasts per crop and horizon before a crop can pass
MIN_SERIES = 5
APP_SEASONAL = {  # docs/forecast-backtest.md in the app: best blend at 7-12 weeks (3-6 weeks was 93-106% for all)
    "onion": 0.87, "potato": 0.88, "cabbage": 0.90, "green_chilli": 0.90, "tomato": 0.92, "brinjal": 0.93,
    "pomegranate": 0.95, "banana": 0.98}


def training_rows(grid: Grid, T: int, max_rows: int, rng: np.random.Generator) -> pd.DataFrame:
    s, t = grid.anchors(52, T)  # a year of lags before the first anchor
    keep = rng.random(len(s)) < min(1.0, max_rows / H / max(len(s), 1))
    df = grid.frame(s[keep], t[keep])
    return df[(df["t"] + df["h"] < T) & np.isfinite(df["y"])]


def run(grid: Grid, origins: list[int], step: int = 13, max_rows: int = 2_000_000, rounds: int | None = None,
        seed: int = 0, log=print) -> pd.DataFrame:
    """Every test forecast with y and lo/mid/hi (log changes, bands as the model gave them), fold, s, t, h, crop.
    calibrate() turns these into the numbers to report; scales() of all of them gives the final model's band factors."""
    rng = np.random.default_rng(seed)
    folds = []
    for i, T in enumerate(origins):
        tr = training_rows(grid, T, max_rows, rng)
        models = train(tr, **({"rounds": rounds} if rounds else {}), seed=seed)
        s, t = grid.anchors(T, T + step)
        te = grid.frame(s, t)
        te = te[np.isfinite(te["y"]) & (te["t"] + te["h"] < grid.T)]
        p = predict(models, te)
        folds.append(pd.DataFrame({"fold": i, "s": te["s"].to_numpy(), "t": te["t"].to_numpy(), "h": te["h"].astype(int).to_numpy(),
                                   "crop": te["crop"].astype(str).to_numpy(), "y": te["y"].to_numpy(),
                                   "lo": p["lo"].to_numpy(), "mid": p["mid"].to_numpy(), "hi": p["hi"].to_numpy()}))
        log(f"[forecast] fold {i}: origin {grid.week(T)}, trained on {len(tr):,} rows, tested {len(te):,}")
    return pd.concat(folds, ignore_index=True)


BUCKETS = ((1, 2), (3, 6), (7, 12))
MIN_CAL = 300  # points per crop and bucket for its own band factor; fewer -> the factor of every crop in that bucket


def bucket(h: int) -> str:
    return next(f"{a}-{b}" for a, b in BUCKETS if a <= h <= b)


def scales(rows: pd.DataFrame) -> dict[str, float]:
    """Band factors per "crop|bucket", plus "*|bucket" for crops with too few points (conformal, on out-of-fold rows)."""
    b = rows["h"].map(bucket)
    out = {}
    for key, g in rows.groupby(b):
        out[f"*|{key}"] = band_scale(g["y"].to_numpy(), g["lo"].to_numpy(), g["mid"].to_numpy(), g["hi"].to_numpy())
    for (crop, key), g in rows.groupby([rows["crop"], b]):
        if len(g) >= MIN_CAL:
            out[f"{crop}|{key}"] = band_scale(g["y"].to_numpy(), g["lo"].to_numpy(), g["mid"].to_numpy(), g["hi"].to_numpy())
    return out


def factors(scale: dict[str, float], crops, hs) -> np.ndarray:
    return np.array([scale.get(f"{c}|{bucket(int(h))}", scale.get(f"*|{bucket(int(h))}", 1.0)) for c, h in zip(crops, hs)])


def calibrate(rows: pd.DataFrame) -> pd.DataFrame:
    """Fold i's bands rescaled with factors fitted on folds < i (the last 52 weeks of them at most); fold 0 dropped."""
    parts = []
    for i in sorted(rows["fold"].unique())[1:]:
        f = rows[rows["fold"] == i].copy()
        past = rows[(rows["fold"] < i) & (rows["t"] >= f["t"].min() - 52)]
        k = factors(scales(past), f["crop"], f["h"])
        f["lo"], f["hi"] = f["mid"] - k * (f["mid"] - f["lo"]), f["mid"] + k * (f["hi"] - f["mid"])
        parts.append(f)
    return pd.concat(parts, ignore_index=True)


def summary(rows: pd.DataFrame) -> pd.DataFrame:
    """Per crop and horizon: n, series, ratio vs the baseline, band coverage, median band width (hi/lo)."""
    r = rows.assign(err=(rows["y"] - rows["mid"]).abs(), naive=rows["y"].abs(),
                    inside=(rows["y"] >= rows["lo"]) & (rows["y"] <= rows["hi"]), width=rows["hi"] - rows["lo"])
    g = r.groupby(["crop", "h"])
    out = pd.DataFrame({"n": g.size(), "series": g["s"].nunique(), "err": g["err"].mean(), "naive": g["naive"].mean(),
                        "coverage": g["inside"].mean(), "width": np.exp(g["width"].median())})
    out["ratio"] = out["err"] / out["naive"]
    return out.reset_index()


def gate(rows: pd.DataFrame, boot: int = 300, seed: int = 0) -> dict[str, dict]:
    """Crops that pass: {crop: {"horizons": [h...], "ratio": {bucket: r}, "ratio_upper": {...}, "coverage": {...}, "n": rows}}.
    A bucket (1-2, 3-6, 7-12 weeks) passes when its ratio is <= 0.9, the 90% upper bound of that ratio (bootstrap over
    anchor weeks, since one week's forecasts move together) is below 1, the band holds 75-85%, on 300+ forecasts from 5+
    lines. The line is drawn from today's price, so a crop shows the passing buckets that run without a gap up to 12 weeks."""
    rng = np.random.default_rng(seed)
    r = rows.assign(b=rows["h"].map(bucket), err=(rows["y"] - rows["mid"]).abs(), naive=rows["y"].abs(),
                    inside=((rows["y"] >= rows["lo"]) & (rows["y"] <= rows["hi"])).astype(float))
    out = {}
    for crop, c in r.groupby("crop"):
        stats = {}
        for key, g in c.groupby("b"):
            by_t = g.groupby("t")[["err", "naive"]].sum()
            w = rng.multinomial(len(by_t), np.full(len(by_t), 1 / len(by_t)), size=boot)
            upper = float(np.quantile(w @ by_t["err"].to_numpy() / np.maximum(w @ by_t["naive"].to_numpy(), 1e-9), 0.9))
            ratio = float(g["err"].sum() / max(g["naive"].sum(), 1e-9))
            cov = float(g["inside"].mean())
            ok = (ratio <= GATE_RATIO and upper < 1.0 and GATE_COVERAGE[0] <= cov <= GATE_COVERAGE[1]
                  and len(g) >= MIN_ROWS and g["s"].nunique() >= MIN_SERIES)
            stats[key] = (ok, round(ratio, 3), round(upper, 3), round(cov, 3))
        shown = []
        for lo, hi in reversed(BUCKETS):
            if not stats.get(f"{lo}-{hi}", (False,))[0]:
                break
            shown.insert(0, f"{lo}-{hi}")
        if shown:
            out[crop] = {"horizons": [h for key in shown for h in range(int(key.split("-")[0]), int(key.split("-")[1]) + 1)],
                         "ratio": {k: stats[k][1] for k in shown}, "ratio_upper": {k: stats[k][2] for k in shown},
                         "coverage": {k: stats[k][3] for k in shown}, "n": len(c)}
    return out


def _band(g: pd.DataFrame, lo: int, hi: int, col: str) -> float:
    s = g[(g["h"] >= lo) & (g["h"] <= hi)]
    if not len(s):
        return float("nan")
    return float((s["err"] * s["n"]).sum() / (s["naive"] * s["n"]).sum()) if col == "ratio" else float(
        (s[col] * s["n"]).sum() / s["n"].sum())


def report(table: pd.DataFrame, passed: dict, meta: dict, top: int = 60) -> str:
    """Markdown: what was tested, then one row per crop (most-tested first)."""
    pct = lambda x: "–" if x != x else f"{x * 100:.0f}%"  # noqa: E731
    lines = [
        f"# Price forecast backtest ({meta['date']})", "",
        f"Rolling-origin test: {meta['folds']} origins from {meta['first_origin']} to {meta['last_origin']}, "
        f"each forecasting the next {meta['step']} weeks of anchors 1-12 weeks ahead, trained only on what was known before the "
        f"origin. {meta['rows']:,} forecasts over {meta['series']:,} price lines ({meta['crops']} crops).", "",
        "**Ratio** = forecast error / error of \"the price stays the same\" (mean |ln actual/forecast|; below 100% beats it; "
        "shown in the app at 90% or less, with the 90% upper bound under 100%). **Band** = share of outcomes inside the 10-90% band "
        "(target 80%; shown at 75-85%). "
        "**App seasonal** = the app's seasonal method at 7-12 weeks (best blend), where it was tested.", "",
        f"Passed: {len(passed)} crops. Overall ratio: 1-2 wk {pct(meta['all'][0])}, 3-6 wk {pct(meta['all'][1])}, "
        f"7-12 wk {pct(meta['all'][2])}; band holds {pct(meta['all'][3])}.", "",
        "| Crop | Lines | Forecasts | 1-2 wk | 3-6 wk | 7-12 wk | App seasonal 7-12 wk | Band 3-6 wk | Band 7-12 wk | Band width 7-12 wk | Shown |",
        "|---|---|---|---|---|---|---|---|---|---|---|"]
    order = table.groupby("crop")["n"].sum().sort_values(ascending=False).index[:top]
    for crop in order:
        g = table[table["crop"] == crop]
        shown = passed.get(crop, {}).get("horizons")
        lines.append(
            f"| {crop} | {int(g['series'].max())} | {int(g['n'].sum()):,} | {pct(_band(g, 1, 2, 'ratio'))} | "
            f"{pct(_band(g, 3, 6, 'ratio'))} | {pct(_band(g, 7, 12, 'ratio'))} | {pct(APP_SEASONAL.get(crop, float('nan')))} | "
            f"{pct(_band(g, 3, 6, 'coverage'))} | {pct(_band(g, 7, 12, 'coverage'))} | ×{_band(g, 7, 12, 'width'):.2f} | "
            f"{f'{shown[0]}-12 wk' if shown else 'no'} |")
    return "\n".join(lines) + "\n"
