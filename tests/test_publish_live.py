import gzip
import json
from datetime import date, datetime

import pytest

from farmuncle_pipeline.ingestion import publish_live as pl
from tests.fakes import FakeResponse

NOW = datetime(2026, 10, 29, 9, 0, tzinfo=pl.IST)
TODAY = "2026-10-29"
MANDIS = [
    {"id": 1, "public_id": "ka.dharwad.hubli", "name": "Hubli", "state": "Karnataka", "status": "ACTIVE", "merged_into_id": None,
     "latitude": 15.36, "longitude": 75.12},
    {"id": 2, "public_id": "ka.dharwad.hubli_dup", "name": "Hubli Dup", "state": "Karnataka", "status": "MERGED", "merged_into_id": 1,
     "latitude": None, "longitude": None},
    {"id": 3, "public_id": "mh.pune.pune", "name": "Pune", "state": "Maharashtra", "status": "ACTIVE", "merged_into_id": None,
     "latitude": 18.5, "longitude": 73.8},
]
CROPS = [{"id": 10, "name": "Onion", "category": "Vegetable", "display_unit": "Quintal", "status": "ACTIVE", "merged_into_id": None},
         {"id": 11, "name": "Tomato", "category": None, "display_unit": "Animal", "status": "ACTIVE", "merged_into_id": None}]
VARIETIES = [{"id": 100, "crop_id": 10, "name": "Red", "status": "ACTIVE", "merged_into_id": None}]
GRADES = [{"id": 5, "name": "FAQ"}]


def price(mid, cid, d, modal, vid=None, gid=5):
    return {"id": 0, "mandi_id": mid, "crop_id": cid, "variety_id": vid, "grade_id": gid, "price_date": d,
            "modal_price": modal, "min_price": modal - 3, "max_price": modal + 3, "created_at": f"{d}T05:30:00+00:00"}


PRICES = [
    price(1, 10, TODAY, 24.5, 100), price(1, 10, "2026-10-28", 23.3, 100), price(2, 11, TODAY, 30),
    price(3, 10, "2026-10-28", 20, 100), price(1, 10, TODAY, 0, None),
]


def build(prices=PRICES, registry=None):
    return pl.build_files(prices=prices, mandis=MANDIS, crops=CROPS, varieties=VARIETIES, grades=GRADES,
                          registry=registry or {}, today=date(2026, 10, 29), now=NOW)


def test_mandi_and_crop_files_follow_contract():
    out = build()
    hubli = out["live/mandi/ka.dharwad.hubli.json"]
    assert hubli["items"][0] == {"crop_id": "onion", "variety_id": "onion.red", "grade": "faq", "modal": 24.5, "min": 21.5,
                                 "max": 27.5, "prev_modal": 23.3, "prev_date": "2026-10-28",
                                 "reported_at": "2026-10-29T11:00:00+05:30"}
    assert hubli["recent_crops"] == ["onion", "tomato"]  # merged mandi 2's tomato lands on its survivor
    assert hubli["updated_at"] == "2026-10-29T09:00:00+05:30"
    assert out["live/mandi/mh.pune.pune.json"]["items"] == []  # reported only yesterday
    assert out["live/crop/onion/ka.json"]["rows"] == [
        {"mandi_id": "ka.dharwad.hubli", "variety_id": "onion.red", "modal": 24.5, "min": 21.5, "max": 27.5, "prev_modal": 23.3}]
    assert "live/crop/onion/mh.json" not in out  # nothing today in Maharashtra


def test_slugs_are_frozen_and_unique():
    reg = pl.assign_slugs(CROPS + [{"id": 12, "name": "ONION!", "merged_into_id": None}], VARIETIES, {})
    assert reg["crops"] == {"10": "onion", "11": "tomato", "12": "onion_12"}
    assert reg["varieties"] == {"100": "onion.red"}
    renamed = [{"id": 10, "name": "Onion Big", "merged_into_id": None}]
    assert pl.assign_slugs(renamed, [], reg)["crops"]["10"] == "onion"


def test_hash_ignores_updated_at():
    a = build()["live/mandi/ka.dharwad.hubli.json"]
    assert pl.content_hash(a) == pl.content_hash({**a, "updated_at": "later"})


class Missing(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class Store(dict):
    def put(self, key, data, content_type=""):
        self[key] = data

    def get(self, key):
        if key not in self:
            raise Missing()
        return self[key]


class Query:
    def __init__(self, rows):
        self.rows = rows

    def select(self, *_a):
        return self

    def gte(self, *_a):
        return self

    def order(self, *_a):
        return self

    def range(self, start, end):
        self.rows = self.rows[start:end + 1]
        return self

    def execute(self):
        return FakeResponse(self.rows)


class DB:
    def __init__(self, prices):
        self.t = {"mandis": MANDIS, "crops": CROPS, "varieties": VARIETIES, "grades": GRADES, "mandi_daily_prices": prices,
                  "state_codes": [{"state": "Karnataka", "code": "ka"}, {"state": "Maharashtra", "code": "mh"}]}

    def table(self, name):
        return Query(list(self.t[name]))


def test_publish_writes_changed_only_and_manifest():
    store = Store()
    first = pl.publish(DB(PRICES), store, NOW)
    assert first["written"] == first["files"] == 4
    assert json.loads(store["catalog/mandis.json"])["version"] == 1
    assert [s["id"] for s in json.loads(store["catalog/states.json"])["states"]] == ["ka", "mh"]
    assert json.loads(store["meta/manifest.json"])["live_updated_at"] == "2026-10-29T09:00:00+05:30"
    assert set(json.loads(store["meta/crop_slugs.json"])["crops"].values()) == {"onion", "tomato"}
    later = datetime(2026, 10, 29, 10, 0, tzinfo=pl.IST)
    assert pl.publish(DB(PRICES), store, later)["written"] == 0  # same data an hour later
    assert json.loads(store["meta/manifest.json"])["live_updated_at"] == "2026-10-29T10:00:00+05:30"
    changed = PRICES + [price(3, 10, TODAY, 21, 100)]
    assert pl.publish(DB(changed), store, later)["written"] == 2  # pune mandi and the new onion/mh list


def test_catalogs_follow_contract():
    reg = pl.assign_slugs(CROPS, VARIETIES, {})
    cat = pl.build_catalogs(mandis=MANDIS, crops=CROPS, varieties=VARIETIES, codes={"Karnataka": "ka", "Maharashtra": "mh"}, registry=reg)
    assert cat["catalog/states.json"]["states"] == [{"id": "ka", "name": "Karnataka"}, {"id": "mh", "name": "Maharashtra"}]
    hubli, dup = cat["catalog/mandis.json"]["mandis"][:2]
    assert hubli == {"id": "ka.dharwad.hubli", "name": "Hubli", "district": "ka.dharwad", "state": "ka", "lat": 15.36,
                     "lng": 75.12, "status": "ACTIVE", "merged_into_id": None, "names": {}}
    assert dup["merged_into_id"] == "ka.dharwad.hubli"
    onion, tomato = cat["catalog/crops.json"]["crops"]
    assert onion["category"] == "vegetable" and onion["unit"] == "kg"
    assert onion["varieties"] == [{"id": "onion.red", "name": "Red"}, {"id": "onion.other", "name": "Other"}]
    assert tomato["unit"] == "animal" and tomato["category"] is None


def test_archive_rows_fill_prev_and_recent_crops():
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    rows = [{"mandi_id": 3, "crop_id": 11, "variety_id": None, "grade_id": 5, "price_date": "2026-10-20",
             "modal_price": 17.0, "min_price": 15.0, "max_price": 19.0}]
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), buf)
    store = Store({"facts/daily/date=2026-10-20.parquet": buf.getvalue()})
    got = pl.archive_rows(store, date(2026, 10, 29))
    assert got == rows
    out = build(got + [price(3, 11, TODAY, 18)])
    item = out["live/mandi/mh.pune.pune.json"]["items"][0]
    assert (item["prev_modal"], item["prev_date"]) == (17.0, "2026-10-20")
    assert out["live/mandi/mh.pune.pune.json"]["recent_crops"] == ["tomato"]
