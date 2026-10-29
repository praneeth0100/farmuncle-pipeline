import io
import json
import math
from collections import defaultdict
from datetime import date, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

from farmuncle_pipeline.ingestion import publish_analytics as pa_
from farmuncle_pipeline.ingestion import publish_live as pl
from tests.test_publish_live import CROPS, DB, GRADES, MANDIS, VARIETIES, Missing

K = ("ka.dharwad.hubli", "onion", "onion.red")
END = date(2026, 10, 29)
STAMP = "2026-10-30T02:00:00+05:30"


def days_of(values: dict) -> dict:
    return {d: ((False, ""), v, v - 2, v + 2) for d, v in values.items()}


def seasonal_days(years=4):
    out, d = {}, END - timedelta(days=365 * years)
    while d <= END:
        out[d] = round(100 * (1 + 0.2 * math.sin(2 * math.pi * d.month / 12)) + (d - END).days / 365, 2)
        d += timedelta(days=1)
    return days_of(out)


def test_series_has_gaps_as_null_and_trims_ends():
    days = days_of({END - timedelta(days=4): 20.0, END - timedelta(days=2): 22.0, END - timedelta(days=1): 23.0})
    f = pa_.series_file(K, days, END, STAMP)
    assert f["start"] == "2026-10-25" and f["modal"] == [20.0, None, 22.0, 23.0]
    assert f["min"] == [18.0, None, 20.0, 21.0] and len(f["max"]) == 4
    assert pa_.series_file(K, days_of({END - timedelta(days=400): 5.0}), END, STAMP) is None


def test_history_is_weekly_and_skips_short_spans():
    days = days_of({END - timedelta(days=n): 10.0 + n for n in range(0, 70)})
    f = pa_.history_file(K, days, STAMP, min_weeks=8)
    assert f["v"] == 2 and f["start_week"] == "2026-W34" and len(f["avg"]) == 11
    assert f["min"][0] <= f["avg"][0] <= f["max"][0]
    assert pa_.history_file(K, days_of({END: 10.0, END - timedelta(days=9): 11.0}), STAMP, min_weeks=8) is None
    assert pa_.history_file(K, days, STAMP) is None  # under a year: the series already covers it


def test_change_blocks_and_unavailable_blocks_with_short_history():
    days = days_of({END - timedelta(days=n): 100.0 + (0 if n else 10) for n in range(0, 60)})
    f = pa_.insights_file(K, days, END, {}, {}, STAMP)
    assert f["last_price"] == 110.0 and f["change"]["d1"] == 10.0
    assert f["change"]["d7_pct"] == 1 and f["change"]["d365_pct"] is None
    assert f["range_52w"] == {"available": False} and f["price_position"] == {"available": False}
    assert f["seasonal"] == {"available": False} and f["outlook"] == {"available": False} and f["nearby"] == []
    assert f["quality"] == {"report_days_90": 60, "last_report": "2026-10-29"}


def test_seasonal_price_position_and_outlook_with_years_of_data():
    f = pa_.insights_file(K, seasonal_days(), END, {}, {}, STAMP)
    assert f["range_52w"]["available"] and f["range_52w"]["low"] < f["range_52w"]["high"]
    assert f["price_position"]["available"] and f["price_position"]["label"] in ("low", "normal", "high")
    s = f["seasonal"]
    assert s["available"] and len(s["index"]) == 12 and len(s["best_months"]) == 2
    assert s["best_months"][0] == s["index"].index(max(s["index"])) + 1
    o = f["outlook"]
    assert o["available"] and o["direction"] in ("up", "flat", "down") and o["low"] <= o["mid"] <= o["high"]
    assert o["confidence"] in ("low", "medium", "high") and o["backtests"] >= 20


def test_nearby_prefers_same_variety_within_100_km():
    coords = {"a": (15.0, 75.0), "b": (15.1, 75.0), "c": (15.2, 75.0), "far": (25.0, 75.0)}
    recent = {"onion": [("b", "onion.red", 30.0, END), ("c", "onion.other", 99.0, END), ("far", "onion.red", 50.0, END)]}
    got = pa_.nearby(("a", "onion", "onion.red"), coords, recent)
    assert got == [{"mandi_id": "b", "km": 11, "modal": 30.0, "date": "2026-10-29"}]
    got = pa_.nearby(("a", "onion", "onion.yellow"), coords, recent)  # no same variety: any variety of the crop
    assert [n["mandi_id"] for n in got] == ["c", "b"]
    assert pa_.nearby(("zz", "onion", "onion.red"), coords, recent) == []


def test_add_rows_prefers_faq_and_resolves_merged_mandi():
    reg = pl.assign_slugs(CROPS, VARIETIES, {})
    key_of = pl.make_key_fn(MANDIS, CROPS, reg)
    rows = [{"mandi_id": 2, "crop_id": 10, "variety_id": 100, "grade_id": g, "price_date": "2026-10-29", "modal_price": m,
             "min_price": m - 1, "max_price": m + 1} for g, m in ((6, 30.0), (5, 25.0))]
    series = defaultdict(dict)
    pa_.add_rows(series, rows, key_of, {5: "faq", 6: "local"})
    (k, days), = series.items()
    assert k == ("ka.dharwad.hubli", "onion", "onion.red") and days[END][1] == 25.0


class Store(dict):
    def put(self, key, data, content_type=""):
        self[key] = data

    def get(self, key):
        if key not in self:
            raise Missing()
        return self[key]

    def list(self, prefix):
        return [k for k in self if k.startswith(prefix)]


def test_active_keys_and_refresh_days():
    assert pa_.is_active(days_of({END - timedelta(days=n): 5.0 for n in range(10)}), END)
    assert not pa_.is_active(days_of({END - timedelta(days=n): 5.0 for n in range(9)}), END)
    assert not pa_.is_active(days_of({END - timedelta(days=100 + n): 5.0 for n in range(20)}), END)  # nothing in 90 days
    assert pa_.due("series", "series/a.json", {}, END)  # never written: now
    slots = {d: pa_.due("series", "series/a.json", {"series/a.json": "h"}, END + timedelta(days=d)) for d in range(6)}
    assert sum(slots.values()) == 2  # once every 3 days
    week = [pa_.due("insights", "insights/a.json", {"insights/a.json": "h"}, END + timedelta(days=d)) for d in range(14)]
    assert sum(week) == 2  # once every 7 days


def test_publish_writes_files_for_active_keys_and_skips_unchanged():
    store = Store()
    for n in range(12):  # 12 archived report days make the key active
        day = f"2026-10-{10 + n:02d}"
        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pylist([{"mandi_id": 1, "crop_id": 10, "variety_id": 100, "grade_id": 5, "price_date": day,
                                              "modal_price": 20.0 + n, "min_price": 18.0, "max_price": 22.0}]), buf)
        store[f"facts/daily/date={day}.parquet"] = buf.getvalue()
    prices = [{"id": 1, "mandi_id": 1, "crop_id": 10, "variety_id": 100, "grade_id": 5, "price_date": "2026-10-29",
               "modal_price": 24.5, "min_price": 21.5, "max_price": 27.5, "created_at": "2026-10-29T05:30:00+00:00"}]
    now = datetime(2026, 10, 30, 2, 0, tzinfo=pl.IST)
    out = pa_.publish(DB(prices), store, now)
    assert out["keys"] == 1 and out["as_of"] == "2026-10-29" and (out["series"], out["insights"], out["history"]) == (1, 1, 0)
    s = json.loads(store["series/onion/ka.dharwad.hubli/onion.red.json"])
    assert s["start"] == "2026-10-10" and s["modal"][0] == 20.0 and s["modal"][-1] == 24.5 and len(s["modal"]) == 20
    assert json.loads(store["meta/manifest.json"])["analytics_built_at"] == "2026-10-30T02:00:00+05:30"
    again = pa_.publish(DB(prices), store, now + timedelta(days=1))
    assert (again["series"], again["history"], again["insights"]) == (0, 0, 0)  # unchanged data is never rewritten
    sparse = pa_.publish(DB([]), Store({k: v for k, v in store.items() if k.startswith("facts/")}), now)
    assert sparse["series"] == 1  # 12 archive days alone are enough for an active key
