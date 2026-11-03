import json
from datetime import date, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

from farmuncle_pipeline.ingestion import publish_analytics as pa_
from farmuncle_pipeline.ingestion import publish_live as pl
from tests.test_publish_live import DB, GRADES, Missing

NOW = datetime(2026, 10, 30, 2, 0, tzinfo=pl.IST)
END = date(2026, 10, 29)
CHART = "chart/onion/ka.dharwad.hubli.json"


def fact(day: date, modal: float, grade=5, mandi=1):
    return {"mandi_id": mandi, "crop_id": 10, "variety_id": 100, "grade_id": grade, "price_date": day.isoformat(),
            "modal_price": modal, "min_price": modal - 2, "max_price": modal + 2}


class Store(dict):
    def put(self, key, data, content_type=""):
        self[key] = data

    def get(self, key):
        if key not in self:
            raise Missing()
        return self[key]

    def list(self, prefix):
        return [k for k in self if k.startswith(prefix)]


def archive(tmp_path, rows) -> tuple[Store, str]:
    """Writes archive rows as Parquet on disk (what DuckDB reads) and lists the same day files in the store (what the
    publisher reads to find the newest archived day). One file per day."""
    store, folder = Store(), tmp_path / "facts" / "daily"
    folder.mkdir(parents=True)
    by_day = {}
    for r in rows:
        by_day.setdefault(r["price_date"], []).append(r)
    for day, day_rows in by_day.items():
        pq.write_table(pa.Table.from_pylist(day_rows), folder / f"date={day}.parquet")
        store[f"facts/daily/date={day}.parquet"] = b""
    return store, str(folder / "*.parquet")


def db(prices=(), extra_grades=()):
    out = DB(list(prices))
    out.t["grades"] = GRADES + list(extra_grades)
    return out


def test_chart_keeps_each_grade_as_its_own_line(tmp_path):
    rows = [fact(END - timedelta(days=n), 20.0 + n) for n in range(1, 13)]
    rows += [fact(END - timedelta(days=n), 15.0, grade=6) for n in range(1, 13)]
    store, glob = archive(tmp_path, rows)
    pa_.publish(db(extra_grades=[{"id": 6, "name": "Local"}]), store, NOW, facts_glob=glob)
    chart = json.loads(store[CHART])
    assert [(line["variety"], line["grade"]) for line in chart["lines"]] == [("onion.red", "faq"), ("onion.red", "local")]
    assert chart["lines"][1]["days"][0][1:] == [15.0, 13.0, 17.0]


def test_chart_daily_last_365_and_weekly_before(tmp_path):
    rows = [fact(END - timedelta(days=n), 10.0 + n % 7) for n in range(1, 730)]
    store, glob = archive(tmp_path, rows)
    warehouse = [{**fact(END, 30.0), "id": 1, "created_at": "2026-10-29T05:30:00+00:00"}]
    pa_.publish(db(warehouse), store, NOW, facts_glob=glob)
    chart = json.loads(store[CHART])
    first = (END - timedelta(days=364)).replace(day=1)
    assert chart["start"] == first.isoformat()
    (line,) = chart["lines"]
    n = (END - first).days
    assert line["days"][-1] == [n, 30.0, 28.0, 32.0] and len(line["days"]) == n + 1  # today's warehouse price joins in
    assert date.fromisoformat(chart["weekly_start"]).weekday() == 0
    first_daily = date.fromisoformat(chart["start"])
    last_week = date.fromisoformat(chart["weekly_start"]) + timedelta(weeks=chart["weekly"][0]["weeks"][-1][0])
    assert last_week < first_daily


def test_key_stats_takes_report_on_or_before_target_within_slack(tmp_path):
    rows = [fact(END - timedelta(days=n), 20.0) for n in range(0, 7)]  # recent days for both mandis
    rows += [fact(END - timedelta(days=n), 20.0, mandi=3) for n in range(0, 7)]
    rows += [fact(END - timedelta(days=8), 18.0)]  # hubli: 8 days back, inside the 3-day slack of 7D
    rows += [fact(END - timedelta(days=12), 17.0, mandi=3)]  # pune: 12 days back, outside it
    rows += [fact(END - timedelta(days=n), 20.0, mandi=m) for m in (1, 3) for n in range(13, 20)]
    store, glob = archive(tmp_path, rows)
    pa_.publish(db(), store, NOW, facts_glob=glob)
    stats = json.loads(store["meta/key_stats.json"])
    assert stats["as_of"] == END.isoformat()
    assert stats["keys"]["ka.dharwad.hubli|onion|onion.red|faq"]["7D"] == [18.0, (END - timedelta(days=8)).isoformat()]
    assert "7D" not in stats["keys"]["mh.pune.pune|onion|onion.red|faq"]


def test_publish_writes_chart_and_stats_not_series(tmp_path):
    rows = [fact(END - timedelta(days=n), 20.0 + n) for n in range(1, 13)]  # 12 report days make the key active
    store, glob = archive(tmp_path, rows)
    out = pa_.publish(db(), store, NOW, facts_glob=glob)
    assert out["as_of"] == (END - timedelta(days=1)).isoformat() and out["written"] == 1
    assert CHART in store and "meta/key_stats.json" in store
    assert not [k for k in store if k.startswith(("series/", "insights/", "history/"))]
    assert json.loads(store["meta/manifest.json"])["analytics_built_at"] == "2026-10-30T02:00:00+05:30"
    again = pa_.publish(db(), store, NOW + timedelta(hours=1), facts_glob=glob)
    assert again["written"] == 0  # unchanged charts are never rewritten


def test_inactive_keys_get_no_chart(tmp_path):
    store, glob = archive(tmp_path, [fact(END - timedelta(days=n), 20.0) for n in range(1, 5)])  # 4 reports in 90 days
    pa_.publish(db(), store, NOW, facts_glob=glob)
    assert CHART not in store


def test_warehouse_read_reaches_back_to_the_last_archived_day():
    today = date(2026, 11, 3)
    assert pa_.warehouse_start([date(2026, 10, 30), date(2026, 10, 31)], today) == date(2026, 10, 31)  # usual night
    assert pa_.warehouse_start([date(2026, 10, 27)], today) == date(2026, 10, 28)  # archive_aged missed 3 nights
    assert pa_.warehouse_start([], today) == date(2026, 10, 31)


def test_a_new_day_without_new_prices_rewrites_no_chart(tmp_path):
    rows = [fact(END - timedelta(days=n), 20.0 + n) for n in range(1, 13)]
    store, glob = archive(tmp_path, rows)
    pa_.publish(db(), store, NOW, facts_glob=glob)
    store2, glob2 = archive(tmp_path / "b", rows + [fact(END, 30.0, mandi=3)])  # a new day, but only Pune reported
    store2.update({k: v for k, v in store.items() if not k.startswith("facts/")})
    out = pa_.publish(db(), store2, NOW + timedelta(days=1), facts_glob=glob2)
    assert out["written"] == 0  # Hubli's chart did not change, so it is not rewritten
