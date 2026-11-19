import json
from datetime import date, datetime

from farmuncle_pipeline.ingestion import publish_arrivals as pa
from farmuncle_pipeline.ingestion.publish_live import IST
from tests.test_publish_live import Missing


def test_columns_map_to_month_blocks():
    assert pa.parse_column("arrivals_1_8_jan_2018") == 0
    assert pa.parse_column("arrivals_9_15_jun_2018") == 5 * 4 + 1
    assert pa.parse_column("arrivals_16_23_nov_2026") == 8 * 48 + 10 * 4 + 2
    assert pa.parse_column("arrivals_24_31_dec_2019") == 48 + 47
    assert pa.parse_column("arrivals_9_15_jun_2017") is None  # before the start year
    assert pa.parse_column("change_over_previous_week") is None


def test_merge_adds_values_and_a_null_never_erases_one():
    doc = {"markets": {}}
    markets = {"chandwad": "mh.nashik.chandwad"}
    n = pa.merge(doc, [{"market": "APMC Chandwad ", "arrivals_9_15_jun_2018": 2832.0, "arrivals_1_8_jun_2018": 1835.0,
                        "change_over_previous_week": "54.3"},
                       {"market": "Baphana Agro Market Inc, Niphad", "arrivals_9_15_jun_2018": 5.0},
                       {"market": "Total", "arrivals_9_15_jun_2018": 9999.0}], markets)
    s = doc["markets"]["mh.nashik.chandwad"]
    assert n == 3 and s[21] == 2832.0 and s[20] == 1835.0
    assert doc["markets"]["?Baphana Agro Market Inc, Niphad"][21] == 5.0  # unmatched kept under its own name
    assert pa.merge(doc, [{"market": "APMC Chandwad", "arrivals_9_15_jun_2018": None}], markets) == 0
    assert doc["markets"]["mh.nashik.chandwad"][21] == 2832.0


def test_calls_due_covers_each_month_with_weeks_2_and_4_and_refreshes_the_newest():
    doc = {"markets": {}, "done": {}}
    refresh, backfill = pa.calls_due(doc, date(2026, 11, 18))  # block 3
    assert refresh == [(2026, 11, 3)]
    assert backfill[:3] == [(2026, 11, 2), (2026, 10, 4), (2026, 10, 2)]  # never week 4 of a month not yet there
    assert backfill[-1] == (2018, 1, 2) and len(backfill) == 1 + 2 * (8 * 12 + 10)
    refresh, _ = pa.calls_due(doc, date(2026, 12, 3))  # block 1: last month's last block is asked again too
    assert refresh == [(2026, 12, 1), (2026, 11, 4)]
    done = {"2026-11-3": "2026-11-18"}
    assert pa.calls_due({"markets": {"x": [1.0]}, "done": done}, date(2026, 11, 18))[0] == []


def test_a_pair_with_no_markets_goes_quiet():
    done = {f"2026-{m:02d}-2": "2026-11-10" for m in range(1, 7)}
    assert pa.calls_due({"markets": {}, "done": done}, date(2026, 11, 12)) == ([], [])
    assert pa.calls_due({"markets": {}, "done": done}, date(2026, 11, 18))[0] == [(2026, 11, 3)]


class Store(dict):
    def put(self, key, data, content_type=""):
        self[key] = data

    def get(self, key):
        if key not in self:
            raise Missing()
        return self[key]


FILTERS = {"data": {"cmdt_data": [{"cmdt_id": 23, "cmdt_name": "Onion"}],
                    "state_data": [{"state_id": 20, "state_name": "Maharashtra"}],
                    "district_data": [{"id": 361, "state_id": 20, "district_name": "Nashik"}]}}


def store_with_catalog():
    s = Store()
    put = lambda k, v: s.put(k, json.dumps(v).encode())  # noqa: E731
    put("catalog/mandis.json", {"mandis": [{"id": "mh.nashik.chandwad", "name": "APMC Chandwad", "district": "mh.nashik",
                                            "status": "ACTIVE", "merged_into_id": None, "label": "Chandwad"}]})
    put("catalog/crops.json", {"crops": [{"id": "onion", "name": "Onion"}]})
    put("catalog/states.json", {"states": [{"id": "mh", "name": "Maharashtra"}]})
    put("live/crop_states.json", {"crops": {"onion": ["mh"]}})
    put("live/crop/onion/mh.json", {"rows": [{"mandi_id": "mh.nashik.chandwad"}]})
    return s


def fake_agmarknet(limit_after=None):
    calls = []

    def fetch(url, params):
        if url == pa.FILTERS:
            return FILTERS
        calls.append(params)
        if limit_after is not None and len(calls) > limit_after:
            raise pa.RateLimited()
        y, m, w = params["year"], params["month"], params["week"]
        mon = date(y, m, 1).strftime("%b").lower()
        a, b = [(1, 8), (9, 15), (16, 23), (24, 30)][w - 1], [(1, 8), (9, 15), (16, 23), (24, 30)][max(w - 2, 0)]
        return {"rows": [{"market": "APMC Chandwad ", f"arrivals_{a[0]}_{a[1]}_{mon}_{y}": float(m * 10 + w),
                          f"arrivals_{b[0]}_{b[1]}_{mon}_{y}": float(m * 10 + w - 1)}]}
    return fetch, calls


def test_run_fetches_everything_once_then_only_refreshes(monkeypatch):
    monkeypatch.setattr(pa, "BACKOFF_S", ())
    store = store_with_catalog()
    fetch, calls = fake_agmarknet()
    now = datetime(2026, 11, 18, 9, 0, tzinfo=IST)
    out = pa.run(store, fetch, now, budget_s=60, gap=0, log=lambda *a: None)
    assert out["pairs"] == 1 and out["calls"] == 2 + 2 * (8 * 12 + 10) and not out["rate_limited"]
    doc = json.loads(store["arrivals/onion/mh.nashik.json"])
    s = doc["markets"]["mh.nashik.chandwad"]
    assert s[pa.index(2018, 1, 1)] == 11.0 and s[pa.index(2026, 11, 3)] == 113.0
    again = pa.run(store, fetch, now, budget_s=60, gap=0, log=lambda *a: None)
    assert again["calls"] == 0  # the newest block was already asked today
    later = pa.run(store, fetch, datetime(2026, 11, 19, 9, 0, tzinfo=IST), budget_s=60, gap=0, log=lambda *a: None)
    assert later["calls"] == 1


def test_run_stops_on_a_rate_limit_and_keeps_what_it_fetched(monkeypatch):
    monkeypatch.setattr(pa, "BACKOFF_S", ())
    store = store_with_catalog()
    fetch, _ = fake_agmarknet(limit_after=5)
    out = pa.run(store, fetch, datetime(2026, 11, 18, 9, 0, tzinfo=IST), budget_s=60, gap=0, log=lambda *a: None)
    assert out["rate_limited"] and out["calls"] == 5
    assert len(json.loads(store["arrivals/onion/mh.nashik.json"])["done"]) == 5


def test_report_names_match_our_mandis():
    markets = {"mumbai": "mh.mumbai.mumbai", "vellore": "tn.vellore.vellore", "shahpura": "mp.dindori.shahpura",
               "kawai_salpura_atru": "rj.baran.kawai_salpura_atru", "lasalgaon": "a", "lasalgaon_niphad": "b"}
    assert pa.find_mandi(markets, "Mumbai- Fruit Market") == "mh.mumbai.mumbai"
    assert pa.find_mandi(markets, "Vellore RM") == "tn.vellore.vellore"
    assert pa.find_mandi(markets, "Shahpura Niwas APMC") == "mp.dindori.shahpura"
    assert pa.find_mandi(markets, "Sub yard Kawai Salpura") == "rj.baran.kawai_salpura_atru"
    assert pa.find_mandi(markets, "Lasalgaon(Niphad) ") == "b"  # an exact key beats a prefix
    assert pa.find_mandi(markets, "Lasalgaon(Vinchur)") == "a"
    assert pa.find_mandi(markets, "Fulera - Kishangarh Renwal APMC") is None
