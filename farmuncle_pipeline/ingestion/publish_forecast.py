"""Daily: the 12-week price forecast (median plus a 10-90% band) per crop, mandi and variety, for the dotted line on Screen 3.

  forecast/{crop}/{mandi}.json   {v, crop, mandi_id, updated_at, trained_at, lines: [{variety, anchor_date, anchor_price,
                                  horizon_weeks: [{h, week, mid, lo, hi}]}], quality: {horizons, ratio_vs_naive, coverage}}
  forecast/crops.json            the crops that passed the backtest, with their quality (the app reads this first)
  meta/forecast/                 the models, band factors and gate (model.json), backtest report (backtest.md, backtest.json)

Reads only the published chart files, weather files and live crop lists (never facts/daily). Retrains at most once a week
(a rolling backtest, then a model on all history); other days reuse the stored model on the newest prices. Only crops that
passed the backtest gate are written, and only at the horizons that passed. A forecast file is rewritten only when its
content changed (meta/forecast_hashes.json). Nothing is deleted: the app shows a crop only while forecast/crops.json lists it.
"""

from __future__ import annotations

import argparse
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from farmuncle_pipeline.archive.store import R2Store
from farmuncle_pipeline.forecast import backtest as bt
from farmuncle_pipeline.forecast import model as fm
from farmuncle_pipeline.forecast import panel
from farmuncle_pipeline.ingestion.publish_live import IST, _encode, _get_json, content_hash

META = "meta/forecast/model.json"
HASHES_KEY = "meta/forecast_hashes.json"
CROPS_KEY = "forecast/crops.json"
RETRAIN_EVERY = timedelta(days=7)
FOLDS, STEP = 14, 8  # origins every 8 weeks over the last two years (the first only warms up the band calibration)
BACKTEST_ROWS, BACKTEST_ROUNDS = 1_500_000, 200  # lighter models per fold, so a retrain fits in about an hour
MAX_ROWS = 3_000_000
STALE_WEEKS = 2  # a line whose newest price is older than this gets no forecast
WORKERS = 48


def getter(store):
    return lambda key: _get_json(store, key)


def build_grid(get_json, log=print) -> fm.Grid:
    pairs = panel.pairs(get_json)
    p = panel.load_panel(get_json, pairs)
    w = panel.load_weather(get_json, sorted({m for _, m in pairs}))
    log(f"[forecast] {len(pairs):,} crop+mandi pairs, {p.groupby(['crop', 'mandi', 'variety']).ngroups:,} lines, "
        f"{len(p):,} weeks of prices, {len(w):,} weeks of weather")
    return fm.Grid(p, w, panel.festival_weeks())


def retrain(grid: fm.Grid, store, stamp: str, folds: int = FOLDS, log=print) -> dict:
    """Backtest, gate, final models; everything stored under meta/forecast/. Returns the model meta."""
    last = grid.T - 1
    origins = [last - fm.H - STEP * k for k in range(folds)][::-1]
    raw = bt.run(grid, origins, step=STEP, max_rows=BACKTEST_ROWS, rounds=BACKTEST_ROUNDS, log=log)
    rows = bt.calibrate(raw)
    table = bt.summary(rows)
    passed = bt.gate(rows)
    rr = rows.assign(e=(rows["y"] - rows["mid"]).abs(), n=rows["y"].abs(), i=(rows["y"] >= rows["lo"]) & (rows["y"] <= rows["hi"]))
    span = lambda a, b: float(rr[rr.h.between(a, b)].e.sum() / rr[rr.h.between(a, b)].n.sum())  # noqa: E731
    meta_report = {"date": stamp[:10], "folds": len(origins) - 1, "first_origin": str(grid.week(origins[1])),
                   "last_origin": str(grid.week(origins[-1])), "step": STEP, "rows": len(rows), "series": int(rows["s"].nunique()),
                   "crops": int(rows["crop"].nunique()), "all": [span(1, 2), span(3, 6), span(7, 12), float(rr.i.mean())]}
    store.put("meta/forecast/backtest.md", bt.report(table, passed, meta_report).encode(), "text/markdown")
    store.put("meta/forecast/backtest.json", _encode({"v": 1, **meta_report, "table": table.round(4).to_dict("records")}),
              "application/json")
    models = fm.train(bt.training_rows(grid, grid.T, MAX_ROWS, np.random.default_rng(1)))
    for q, booster in models.items():
        store.put(f"meta/forecast/q{int(q * 100)}.txt", booster.model_to_string().encode(), "text/plain")
    # The final model's bands: the factor per horizon fitted on every fold's raw bands.
    meta = {"v": 1, "trained_at": stamp, "scale": bt.scales(raw), "passed": passed, "overall": meta_report["all"]}
    store.put(META, _encode(meta), "application/json")
    meta = json.loads(_encode(meta))  # the same str keys a later run reads back, so file hashes match from day to day
    log(f"[forecast] trained; {len(passed)} crops passed: {sorted(passed)}")
    return meta


def load_models(store) -> dict:
    import lightgbm as lgb

    return {q: lgb.Booster(model_str=store.get(f"meta/forecast/q{int(q * 100)}.txt").decode()) for q in fm.QUANTILES}


def latest_anchors(grid: fm.Grid) -> tuple[np.ndarray, np.ndarray]:
    """Each line's newest week with a price, when it is recent and the line has 6+ prices in the last 12 weeks."""
    grid.prepare()
    has = np.isfinite(grid.lp)
    t = grid.T - 1 - np.argmax(has[:, ::-1], axis=1)
    s = np.arange(len(t))
    ok = has.any(1) & (t >= grid.T - 1 - STALE_WEEKS) & (grid.n12[s, t] >= 6)
    return s[ok], t[ok]


def forecasts(grid: fm.Grid, models: dict, meta: dict, stamp: str):
    """Yields (key, payload) per crop+mandi of a passed crop."""
    passed = meta["passed"]
    s, t = latest_anchors(grid)
    keep = np.isin(grid.keys["crop"].to_numpy()[s], list(passed))
    s, t = s[keep], t[keep]
    if not len(s):
        return
    df = grid.frame(s, t)
    p = fm.predict(models, df, bt.factors(meta["scale"], df["crop"].astype(str), df["h"]))
    df = pd.concat([df[["s", "t", "h"]], p], axis=1)
    df["h"] = df["h"].astype(int)
    keys = grid.keys
    by_pair: dict[tuple, list] = {}
    for (si, ti), g in df.groupby(["s", "t"], sort=False):
        crop, mandi, variety = keys.iloc[si]
        shown = set(passed[crop]["horizons"])
        anchor = float(np.exp(grid.lp[si, ti]))
        weeks = [{"h": int(r.h), "week": str(grid.week(ti + r.h)), "mid": round(anchor * float(np.exp(r.mid)), 2),
                  "lo": round(anchor * float(np.exp(r.lo)), 2), "hi": round(anchor * float(np.exp(r.hi)), 2)}
                 for r in g.sort_values("h").itertuples() if r.h in shown]
        by_pair.setdefault((crop, mandi), []).append(
            {"variety": variety, "anchor_date": str(grid.week(ti)), "anchor_price": round(anchor, 2), "horizon_weeks": weeks})
    for (crop, mandi), lines in sorted(by_pair.items()):
        q = passed[crop]
        yield f"forecast/{crop}/{mandi}.json", {
            "v": 1, "crop": crop, "mandi_id": mandi, "updated_at": stamp, "trained_at": meta["trained_at"],
            "lines": sorted(lines, key=lambda x: x["variety"]),
            "quality": {"horizons": q["horizons"], "ratio_vs_naive": q["ratio"], "ratio_upper": q["ratio_upper"],
                        "coverage": q["coverage"]}}


def publish(store, now: datetime | None = None, get_json=None, force_train: bool = False, folds: int = FOLDS, log=print) -> dict:
    now = now or datetime.now(IST)
    stamp = now.astimezone(IST).isoformat(timespec="seconds")
    grid = build_grid(get_json or getter(store), log)
    meta = _get_json(store, META)
    if force_train or not meta or now - datetime.fromisoformat(meta["trained_at"]) >= RETRAIN_EVERY:
        meta = retrain(grid, store, stamp, folds, log)
    models = load_models(store)
    hashes = _get_json(store, HASHES_KEY) or {}
    batch, count = [], 0
    for key, payload in forecasts(grid, models, meta, stamp):
        count += 1
        h = content_hash({k: v for k, v in payload.items() if k != "trained_at"})
        if hashes.get(key) != h:
            batch.append((key, _encode(payload)))
            hashes[key] = h
    with ThreadPoolExecutor(WORKERS) as pool:
        list(pool.map(lambda kd: store.put(kd[0], kd[1], "application/json"), batch))
    store.put(CROPS_KEY, _encode({"v": 1, "updated_at": stamp, "trained_at": meta["trained_at"], "crops": meta["passed"]}),
              "application/json")
    store.put(HASHES_KEY, _encode(hashes), "application/json")
    return {"files": count, "written": len(batch), "crops": len(meta["passed"]), "trained_at": meta["trained_at"]}


def main() -> None:
    parser = argparse.ArgumentParser(description="Price forecast files for Screen 3.")
    parser.add_argument("--train", action="store_true", help="retrain now, even if the model is less than a week old")
    args = parser.parse_args()
    if not R2Store.configured():
        print("[forecast] MANDI_R2_* credentials are not set; nothing published.")
        return
    print(f"[forecast] {publish(R2Store.from_env(), force_train=args.train or bool(os.environ.get('FORECAST_TRAIN')))}")


if __name__ == "__main__":
    main()
