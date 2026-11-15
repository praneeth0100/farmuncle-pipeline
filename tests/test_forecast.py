import json
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("lightgbm")

from farmuncle_pipeline.forecast import backtest as bt  # noqa: E402
from farmuncle_pipeline.forecast import model as fm  # noqa: E402
from farmuncle_pipeline.forecast import panel  # noqa: E402
from farmuncle_pipeline.ingestion import publish_forecast as pf  # noqa: E402
from farmuncle_pipeline.ingestion import publish_live as pl  # noqa: E402
from tests.test_publish_live import Missing  # noqa: E402


def test_chart_weeks_merges_grades_highest_wins_and_averages_days():
    chart = {"start": "2026-01-05",  # a Monday
             "lines": [{"variety": "onion.local", "grade": "faq", "days": [[0, 10.0, 8, 12], [2, 12.0, 9, 13], [7, 20.0, 1, 2]]},
                       {"variety": "onion.local", "grade": "medium", "days": [[0, 14.0, 9, 15]]}],
             "weekly_start": "2025-12-22",
             "weekly": [{"variety": "onion.local", "grade": "faq", "weeks": [[0, 30.0, 1, 2], [2, 99.0, 1, 2]]},
                        {"variety": "onion.local", "grade": "medium", "weeks": [[0, 31.0, 1, 2]]}]}
    rows = {(v, w): (p, d) for v, w, p, d in panel.chart_weeks(chart)}
    assert rows[("onion.local", date(2026, 1, 5))] == (13.0, 2)  # day 0: medium 14 beats faq 10; day 2: 12
    assert rows[("onion.local", date(2026, 1, 12))] == (20.0, 1)
    assert rows[("onion.local", date(2025, 12, 22))] == (31.0, None)  # weekly: highest grade
    assert rows[("onion.local", date(2026, 1, 5))][0] != 99.0  # the week the daily part covers keeps the daily numbers


def synthetic(n_mandis=8, weeks=330, crops=("onion", "tomato"), seed=0) -> pd.DataFrame:
    """Seasonal prices: a yearly wave each mandi follows, plus noise."""
    rng = np.random.default_rng(seed)
    rows = []
    start = date(2020, 1, 6)
    for crop in crops:
        for m in range(n_mandis):
            level = rng.uniform(15, 40)
            for k in range(weeks):
                w = start + timedelta(weeks=k)
                p = level * np.exp(0.4 * np.sin(2 * np.pi * k / 52) + rng.normal(0, 0.05))
                rows.append((crop, f"ka.d{m % 3}.m{m}", f"{crop}.local", pd.Timestamp(w), p, 3.0))
    return pd.DataFrame(rows, columns=["crop", "mandi", "variety", "week", "price", "days"])


def test_features_never_see_the_future():
    p = synthetic(n_mandis=4, weeks=200)
    a = fm.Grid(p, festival_weeks=panel.festival_weeks())
    t_anchor = 150
    cut = pd.Timestamp(fm.EPOCH) + pd.Timedelta(weeks=t_anchor)
    changed = p.copy()
    changed.loc[changed["week"] > cut, "price"] *= 5  # rewrite everything after the anchor week
    b = fm.Grid(changed, festival_weeks=panel.festival_weeks())
    s = np.arange(len(a.keys))
    t = np.full(len(s), t_anchor)
    fa, fb = a.frame(s, t), b.frame(s, t)
    cols = [c for c in fm.feature_names(fa) if c not in fm.CATEGORICAL]
    pd.testing.assert_frame_equal(fa[cols], fb[cols])
    assert not np.allclose(fa["y"], fb["y"])  # only the target moved


def test_band_scale_hits_the_target_coverage():
    rng = np.random.default_rng(0)
    y = rng.normal(0, 1, 20000)
    mid, lo, hi = np.zeros_like(y), np.full_like(y, -0.5), np.full_like(y, 0.5)  # far too narrow
    k = fm.band_scale(y, lo, mid, hi)
    inside = np.mean(np.abs(y) <= 0.5 * k)
    assert abs(inside - 0.8) < 0.01 and 2.4 < k < 2.8  # 1.2816 sigma / 0.5


def rows(crop, err_by_h, covered=0.8, weeks=40, lines=10):
    """Backtest rows where |y| = 1 and the forecast error is err_by_h[h-1], a share `covered` inside the band."""
    out = []
    for t in range(weeks):
        for s in range(lines):
            for h, e in enumerate(err_by_h, 1):
                inside = (t * lines + s) % 10 < covered * 10
                out.append({"crop": crop, "s": s, "t": t, "h": h, "y": 1.0, "mid": 1.0 - e,
                            "lo": 0.0 if inside else 1.5, "hi": 2.0})
    return pd.DataFrame(out)


def test_gate_shows_passing_buckets_without_a_gap_up_to_12():
    r = pd.concat([rows("onion", [0.8] * 2 + [0.85, 0.95, 0.95, 0.95] + [0.8] * 6), rows("tomato", [0.95] * 12),
                   rows("potato", [0.8] * 12, covered=0.6), rows("rare", [0.5] * 12, weeks=2, lines=2)])
    g = bt.gate(r)
    assert g["onion"]["horizons"] == [7, 8, 9, 10, 11, 12] and g["onion"]["ratio"] == {"7-12": 0.8}
    assert set(g) == {"onion"}  # onion: 1-2 passes but 3-6 (0.925) breaks the run to 12; tomato too weak, potato's band too narrow, rare too few


def test_band_factors_back_off_to_all_crops_when_a_crop_is_small():
    r = pd.concat([rows("onion", [0.0] * 12), rows("rare", [0.0] * 12, weeks=1, lines=2)])
    k = bt.scales(r)
    assert "onion|7-12" in k and "rare|7-12" not in k
    assert bt.factors(k, ["rare"], [9])[0] == k["*|7-12"]


class Store(dict):
    def put(self, key, data, content_type=""):
        self[key] = data

    def get(self, key):
        if key not in self:
            raise Missing()
        return self[key]


def test_publish_end_to_end_writes_only_passed_crops_and_skips_unchanged(monkeypatch):
    p = synthetic(n_mandis=12, weeks=340)
    grid = fm.Grid(p, festival_weeks=panel.festival_weeks())
    monkeypatch.setattr(pf, "build_grid", lambda get_json, log=print: grid)
    monkeypatch.setattr(fm, "ROUNDS", 30)
    monkeypatch.setattr(pf, "MAX_ROWS", 40_000)
    monkeypatch.setattr(pf, "STEP", 8)
    passed = {"onion": {"horizons": list(range(3, 13)), "ratio": {"3-6": 0.85, "7-12": 0.7},
                        "ratio_upper": {"3-6": 0.9, "7-12": 0.8}, "coverage": {"3-6": 0.8, "7-12": 0.8}, "n": 999}}
    monkeypatch.setattr(bt, "gate", lambda table: passed)  # the synthetic numbers do not matter here; the plumbing does
    store = Store()
    now = datetime(2026, 7, 6, 8, 0, tzinfo=pl.IST)
    out = pf.publish(store, now=now, get_json=lambda k: None, folds=3, log=lambda *a: None)
    assert out["crops"] == 1 and out["files"] == 12 and out["written"] == 12
    files = [k for k in store if k.startswith("forecast/") and k != pf.CROPS_KEY]
    assert all(k.startswith("forecast/onion/") for k in files)
    doc = json.loads(store[files[0]])
    line = doc["lines"][0]
    assert [w["h"] for w in line["horizon_weeks"]] == list(range(3, 13))
    assert all(w["lo"] <= w["mid"] <= w["hi"] for w in line["horizon_weeks"])
    assert doc["quality"]["horizons"] == list(range(3, 13))
    assert "backtest.md" in " ".join(store) and json.loads(store[pf.CROPS_KEY])["crops"] == json.loads(json.dumps(passed))
    again = pf.publish(store, now=now + timedelta(hours=1), get_json=lambda k: None, log=lambda *a: None)
    assert again["written"] == 0 and again["trained_at"] == out["trained_at"]  # model reused, nothing changed
