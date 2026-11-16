"""Price forecast: one LightGBM model per quantile (10/50/90%), pooled over every crop, mandi and horizon (h = 1..12 weeks).

Target: ln(price h weeks later / price this week), weekly mean modal. Every feature at anchor week t uses only weeks <= t
(the seasonal features look at the same weeks in earlier years, t - 52k + h, which is still before t because h < 52), so a
row never sees its future. Series live in dense series x week arrays: lags are column shifts, group medians are row
groups. Bands are widened or narrowed per horizon by a factor fitted on earlier backtest folds (calibration).
"""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

EPOCH = np.datetime64("2018-01-01", "D")  # a Monday; history starts in 2018 (archive_history, PR #109)
H = 12
QUANTILES = (0.1, 0.5, 0.9)
LAGS = (1, 2, 4, 8, 13, 26, 52)
SEASON_YEARS = 8
FFILL = 4  # a lag may borrow the nearest earlier week up to 4 weeks back
PARAMS = dict(learning_rate=0.05, num_leaves=63, min_data_in_leaf=300, feature_fraction=0.8, bagging_fraction=0.7,
              bagging_freq=1, lambda_l2=1.0, verbose=-1, num_threads=0)
ROUNDS = 400
CATEGORICAL = ["crop", "state"]


def _ffill(m: np.ndarray, limit: int) -> np.ndarray:
    return pd.DataFrame(m).T.ffill(limit=limit).T.to_numpy(np.float32)


def _shift(m: np.ndarray, k: int) -> np.ndarray:
    """Column t holds column t-k (k > 0: the past); NaN where it falls off the edge."""
    out = np.full_like(m, np.nan)
    if k > 0:
        out[:, k:] = m[:, :-k]
    elif k < 0:
        out[:, :k] = m[:, -k:]
    else:
        out[:] = m
    return out


def _rolling(m: np.ndarray, window: int, how: str, min_periods: int) -> np.ndarray:
    r = pd.DataFrame(m.T).rolling(window, min_periods=min_periods)
    return getattr(r, how)().to_numpy(np.float32).T


def _group(m: np.ndarray, gid: np.ndarray, how: str = "median") -> np.ndarray:
    """Per group and week, the median (or mean, or count) over the group's series; returned per series (row of its group)."""
    out = np.full_like(m, np.nan)
    order = np.argsort(gid, kind="stable")
    bounds = np.flatnonzero(np.diff(gid[order])) + 1
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)  # all-NaN weeks give NaN, as wanted
        for rows in np.split(order, bounds):
            block = m[rows]
            v = (np.nanmedian(block, 0) if how == "median" else np.nanmean(block, 0) if how == "mean"
                 else np.isfinite(block).sum(0).astype(np.float32))
            out[rows] = v
    return out


class Grid:
    """Dense arrays (series x Monday weeks from EPOCH) for log price, report days and weather, plus series labels."""

    def __init__(self, panel: pd.DataFrame, weather: pd.DataFrame | None = None, festival_weeks: np.ndarray | None = None):
        panel = panel[(panel["price"] > 0) & (panel["week"] >= pd.Timestamp(EPOCH))]
        keys = panel[["crop", "mandi", "variety"]].drop_duplicates().reset_index(drop=True)
        self.keys = keys
        sid = pd.MultiIndex.from_frame(keys).get_indexer(pd.MultiIndex.from_frame(panel[["crop", "mandi", "variety"]]))
        t = ((panel["week"].to_numpy("datetime64[D]") - EPOCH).astype(int) // 7)
        self.T = int(t.max()) + 1
        S = len(keys)
        self.lp = np.full((S, self.T), np.nan, np.float32)
        self.lp[sid, t] = np.log(panel["price"].to_numpy(np.float64))
        self.days = np.full((S, self.T), np.nan, np.float32)
        self.days[sid, t] = panel["days"].to_numpy(np.float32)
        parts = keys["mandi"].str.split(".", expand=True)
        self.state = parts[0].to_numpy()
        self.district = (parts[0] + "." + parts[1]).to_numpy()
        self.crop_state = pd.factorize(keys["crop"] + "|" + self.state)[0]
        self.crop_district = pd.factorize(keys["crop"] + "|" + self.district)[0]
        self.crop_id = pd.factorize(keys["crop"])[0]
        self.state_id = pd.factorize(self.state)[0]
        self.rain = self.tmax = None
        if weather is not None and len(weather):
            w = weather[weather["week"] >= pd.Timestamp(EPOCH)]
            mi = pd.Index(keys["mandi"].unique())
            wt = ((w["week"].to_numpy("datetime64[D]") - EPOCH).astype(int) // 7)
            ok = wt < self.T
            rain = np.full((len(mi), self.T), np.nan, np.float32)
            tmax = np.full((len(mi), self.T), np.nan, np.float32)
            mrow = mi.get_indexer(w["mandi"])
            ok &= mrow >= 0
            rain[mrow[ok], wt[ok]] = w["rain"].to_numpy(np.float32)[ok]
            tmax[mrow[ok], wt[ok]] = w["tmax"].to_numpy(np.float32)[ok]
            row = mi.get_indexer(keys["mandi"])
            self.rain, self.tmax = rain[row], tmax[row]
        self.fest = None
        if festival_weeks is not None and len(festival_weeks):
            ft = ((festival_weeks - EPOCH).astype(int) // 7)
            nxt = np.searchsorted(ft, np.arange(self.T))  # first festival week >= t
            self.fest = np.where(nxt < len(ft), ft[np.minimum(nxt, len(ft) - 1)] - np.arange(self.T), 99).astype(np.float32)
        self._prepared = False

    def week(self, t: int) -> np.datetime64:
        return EPOCH + np.timedelta64(int(t) * 7, "D")

    def prepare(self) -> None:
        """Arrays every feature reads; built once."""
        if self._prepared:
            return
        lp = self.lp
        self.lpf = _ffill(lp, FFILL)
        self.smooth = _rolling(lp, 3, "mean", 1)
        self.smooth = _shift(self.smooth, -1)  # centred: weeks t-1..t+1 (only ever read at weeks before the anchor)
        self.med52 = _rolling(lp, 52, "median", 8)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            self.yoy3 = np.nanmedian(np.stack([_shift(self.smooth, 52 * k) for k in (1, 2, 3)]), 0)  # same weeks, 3 years back
        seen = np.where(np.isfinite(lp), np.arange(self.T, dtype=np.float32), np.nan)
        self.since = np.arange(self.T, dtype=np.float32) - _ffill(_shift(seen, 1), self.T)  # weeks since the report before
        self.n12 = _rolling(np.isfinite(lp).astype(np.float32), 12, "sum", 1)
        self.std13 = _rolling(lp, 13, "std", 4)
        self.n8 = _rolling(np.isfinite(lp).astype(np.float32), 8, "sum", 1)
        r4 = lp - _shift(self.lpf, 4)
        r13 = lp - _shift(self.lpf, 13)
        r1 = lp - _shift(self.lpf, 1)
        r2 = lp - _shift(self.lpf, 2)
        self.g_r1 = _group(r1, self.crop_state)
        self.g_r2 = _group(r2, self.crop_state)
        self.n_r1 = _group(r1, self.crop_id)
        self.g_r4 = _group(r4, self.crop_state)
        self.s_r4 = _group(r4, self.state_id)  # every crop in the state: a market-wide move
        self.g_r13 = _group(r13, self.crop_state)
        self.n_r4 = _group(r4, self.crop_id)
        self.g_rel = lp - _group(self.lpf, self.crop_state)
        self.d_rel = lp - _group(self.lpf, self.crop_district)
        self.g_count = _group(lp, self.crop_state, "count")
        self.season, self.g_season, self.n_season = {}, {}, {}
        for h in range(1, H + 1):
            moves = np.stack([_shift(self.smooth, 52 * k - h) - _shift(self.smooth, 52 * k) for k in range(1, SEASON_YEARS + 1)])
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                s = np.nanmean(moves, 0).astype(np.float32)
            self.season[h] = s
            self.g_season[h] = _group(s, self.crop_state, "mean")
            self.n_season[h] = _group(s, self.crop_id, "mean")
        self.season_years = np.isfinite(np.stack([_shift(self.smooth, 52 * k) for k in range(1, SEASON_YEARS + 1)])).sum(0)
        if self.rain is not None:
            rain4 = _rolling(self.rain, 4, "sum", 3)
            rain13 = _rolling(self.rain, 13, "sum", 10)
            tmax4 = _rolling(self.tmax, 4, "mean", 3)
            clim = lambda m: np.nanmean(np.stack([_shift(m, 52 * k) for k in range(1, 9)]), 0)  # noqa: E731 earlier years only
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                self.rain4_anom = np.log1p(rain4) - np.log1p(clim(rain4))
                self.rain13_anom = np.log1p(rain13) - np.log1p(clim(rain13))
                self.tmax4_anom = tmax4 - clim(tmax4)
        self._prepared = True

    def anchors(self, t_from: int, t_to: int, step: int = 1) -> tuple[np.ndarray, np.ndarray]:
        """(series, week) pairs with a price at the week, weeks in [t_from, t_to), every `step` weeks, 3+ prices in 8 weeks."""
        self.prepare()
        cols = np.arange(max(t_from, 0), min(t_to, self.T), step)
        s, c = np.nonzero(np.isfinite(self.lp[:, cols]) & (self.n8[:, cols] >= 3))
        return s, cols[c]

    def frame(self, s: np.ndarray, t: np.ndarray, hs=range(1, H + 1)) -> pd.DataFrame:
        """One row per anchor and horizon: features, h, and y (the actual log change, NaN when not known)."""
        self.prepare()
        g = lambda m: m[s, t]  # noqa: E731
        base = {
            "crop": self.keys["crop"].to_numpy()[s], "state": self.state[s],
            **{f"r{k}": g(self.lp) - self.lpf[s, np.maximum(t - k, 0)] * np.where(t - k >= 0, 1, np.nan) for k in LAGS},
            "dev52": g(self.lp) - g(self.med52), "yoy3": g(self.lp) - g(self.yoy3), "std13": g(self.std13),
            "n8": g(self.n8), "n12": g(self.n12), "since": g(self.since), "days": g(self.days),
            "g_r1": g(self.g_r1), "g_r2": g(self.g_r2), "n_r1": g(self.n_r1), "s_r4": g(self.s_r4), "g_r4": g(self.g_r4), "g_r13": g(self.g_r13), "n_r4": g(self.n_r4), "g_rel": g(self.g_rel), "d_rel": g(self.d_rel),
            "g_count": g(self.g_count), "season_years": g(self.season_years).astype(np.float32),
            "woy": ((t % 52) + 1).astype(np.float32),
            "month": pd.DatetimeIndex(EPOCH + (t * 7).astype("timedelta64[D]")).month.to_numpy(np.float32),
        }
        if self.rain is not None:
            base |= {"rain4_anom": g(self.rain4_anom), "rain13_anom": g(self.rain13_anom), "tmax4_anom": g(self.tmax4_anom)}
        if self.fest is not None:
            base["fest_weeks"] = self.fest[t]
        parts = []
        for h in hs:
            tt = t + h
            known = tt < self.T
            y = np.full(len(s), np.nan, np.float32)
            y[known] = self.lp[s[known], tt[known]] - self.lp[s[known], t[known]]
            part = {**base, "h": np.full(len(s), h, np.float32), "season": g(self.season[h]), "g_season": g(self.g_season[h]),
                    "n_season": g(self.n_season[h]), "s": s, "t": t, "y": y}
            if self.fest is not None:
                part["fest_in_h"] = (self.fest[t] <= h).astype(np.float32)
            parts.append(pd.DataFrame(part))
        df = pd.concat(parts, ignore_index=True)
        wk = EPOCH + (df["t"].to_numpy() * 7).astype("timedelta64[D]")
        df["w"] = np.where((wk >= np.datetime64("2020-03-01")) & (wk < np.datetime64("2020-07-01")), 0.3, 1.0)  # lockdown
        for c in CATEGORICAL:
            df[c] = df[c].astype("category")
        return df


FEATURES_EXCLUDE = {"s", "t", "y", "w"}


def feature_names(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c not in FEATURES_EXCLUDE]


def train(df: pd.DataFrame, rounds: int | None = None, seed: int = 0) -> dict:
    """{quantile: booster}, trained on the rows with a known target."""
    import lightgbm as lgb

    df = df[np.isfinite(df["y"])]
    X = df[feature_names(df)]
    models = {}
    for q in QUANTILES:
        data = lgb.Dataset(X, df["y"], weight=df["w"], categorical_feature=CATEGORICAL, free_raw_data=False)
        models[q] = lgb.train({**PARAMS, "objective": "quantile", "alpha": q, "seed": seed}, data, rounds or ROUNDS)
    return models


def predict(models: dict, df: pd.DataFrame, k: np.ndarray | None = None) -> pd.DataFrame:
    """lo, mid, hi as log changes; bands sorted (no crossing) and, given k (one factor per row), scaled around the median."""
    X = df[feature_names(df)]
    q = np.sort(np.stack([models[k].predict(X) for k in QUANTILES], 1), 1)
    lo, mid, hi = q[:, 0], q[:, 1], q[:, 2]
    if k is not None:
        lo, hi = mid - k * (mid - lo), mid + k * (hi - mid)
    return pd.DataFrame({"lo": lo, "mid": mid, "hi": hi}, index=df.index)


def band_scale(y: np.ndarray, lo: np.ndarray, mid: np.ndarray, hi: np.ndarray, target: float = 0.8) -> float:
    """The factor k so that [mid - k(mid-lo), mid + k(hi-mid)] holds `target` of y: the target quantile of each outcome's
    distance from the median in units of its own half-band."""
    up, down = np.maximum(hi - mid, 1e-6), np.maximum(mid - lo, 1e-6)
    need = np.where(y >= mid, (y - mid) / up, (mid - y) / down)
    return float(np.quantile(need, target)) if len(need) else 1.0
