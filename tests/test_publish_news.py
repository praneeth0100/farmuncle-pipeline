import gzip
import gzip as gz
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from farmuncle_pipeline.ingestion import publish_news as pn

FIX = Path(__file__).parent / "fixtures" / "news"
NOW = datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc)
CATALOG = {"catalog/mandis.json": {"v": 1, "mandis": [{"id": "ka.mysuru.m", "district": "ka.mysuru", "state": "ka"}]},
           "catalog/states.json": {"v": 1, "states": [{"id": "ka", "name": "Karnataka"}]}}
FEEDS = [{"id": "hindu_agri", "url": "https://h/agri", "outlet": "The Hindu", "language": "en", "level": "national", "states": [], "farm_only": True},
         {"id": "tv9kn", "url": "https://t/kn", "outlet": "TV9 Kannada", "language": "kn", "level": "state", "states": ["ka"], "farm_only": True}]
SAVED = {"id": "s1", "title": "Old saved story", "link": "https://t/s1", "outlet": "TV9 Kannada", "published": "2026-11-06T08:00:00Z",
         "lang": "kn", "image": None, "level": "state", "districts": [], "states": ["ka"], "farm": True, "feed": "tv9kn"}


class Store:
    def __init__(self, files=None):
        self.files = {k: json.dumps(v).encode() for k, v in (files or {}).items()}
        self.gz = set()

    def get(self, key):
        return self.files[key]

    def put(self, key, data, content_type="", gzip=False):
        self.files[key] = gz.compress(data) if gzip else data  # as R2Store.put does
        (self.gz.add if gzip else self.gz.discard)(key)

    def json(self, key):
        data = self.files[key]
        return json.loads(gzip.decompress(data) if key in self.gz else data)


def http(replies):
    calls = []

    def get(url, headers):
        calls.append((url, headers))
        reply = replies[url]
        if isinstance(reply, Exception):
            raise reply
        return reply
    get.calls = calls
    return get


def saved_state():
    return {"news/_state.json": {"v": 1, "feeds": {"tv9kn": {"etag": '"e1"', "modified": "Fri, 06 Nov 2026 08:00:00 GMT"}}, "stories": [SAVED]}}


def test_304_keeps_saved_stories():
    store = Store(CATALOG | saved_state())
    get = http({"https://h/agri": (200, {"ETag": '"h1"'}, (FIX / "hindu_agri.xml").read_bytes()), "https://t/kn": (304, {}, b"")})
    summary = pn.run(store, get, FEEDS, NOW)
    assert summary["tv9kn"] == {"status": 304, "items": 1}
    assert dict(get.calls)["https://t/kn"]["If-None-Match"] == '"e1"'
    assert dict(get.calls)["https://t/kn"]["If-Modified-Since"] == "Fri, 06 Nov 2026 08:00:00 GMT"
    assert [s["id"] for s in store.json("news/state/ka/kn.json")["stories"]] == ["s1"]
    assert store.json("news/_state.json")["feeds"]["tv9kn"]["etag"] == '"e1"'
    assert store.json("news/_state.json")["feeds"]["hindu_agri"]["etag"] == '"h1"'


def test_failed_feed_keeps_saved_stories_and_is_counted():
    store = Store(CATALOG | saved_state())
    get = http({"https://h/agri": (200, {}, (FIX / "hindu_agri.xml").read_bytes()), "https://t/kn": OSError("down")})
    pn.run(store, get, FEEDS, NOW)
    index = store.json("news/index.json")
    assert index["feeds"]["tv9kn"] == {"status": "error", "items": 1}
    assert index["feeds"]["hindu_agri"]["status"] == 200 and index["feeds"]["hindu_agri"]["items"] > 0
    assert "s1" in [s["id"] for s in store.json("news/state/ka/kn.json")["stories"]]


def test_more_than_half_failed_exits_nonzero():
    store = Store(CATALOG)
    get = http({"https://h/agri": OSError("x"), "https://t/kn": OSError("y")})
    with pytest.raises(SystemExit):
        pn.run(store, get, FEEDS, NOW)
    assert "news/index.json" in store.files  # the counts are still written, so the outage is visible


def test_writes_gzip_files_and_index():
    store = Store(CATALOG)
    get = http({"https://h/agri": (200, {}, (FIX / "hindu_agri.xml").read_bytes()), "https://t/kn": (200, {}, (FIX / "tv9kn.xml").read_bytes())})
    pn.run(store, get, FEEDS, NOW)
    india = store.json("news/india/en.json")
    assert india["v"] == 1 and len(india["stories"]) == 1  # two of the three fixture items are older than 72 h
    assert "feed" not in india["stories"][0] and "summary" not in india["stories"][0]
    assert {"news/india/en.json", "news/state/ka/kn.json", "news/index.json", "news/_state.json"} <= store.gz
    assert store.json("news/index.json")["files"]["news/india/en.json"] == 1
    assert dict(get.calls)["https://h/agri"]["User-Agent"].startswith("AgroUncleNews/1.0")


def test_a_file_that_empties_is_rewritten_empty():
    store = Store(CATALOG | {"news/index.json": {"v": 1, "files": {"news/world/en.json": 3}}})
    get = http({"https://h/agri": (200, {}, (FIX / "hindu_agri.xml").read_bytes()), "https://t/kn": (304, {}, b"")})
    pn.run(store, get, FEEDS, NOW)
    assert store.json("news/world/en.json")["stories"] == []


def test_feed_list_is_valid():
    feeds = pn.load_feeds()
    assert len({f["id"] for f in feeds}) == len(feeds) >= 20
    for f in feeds:
        assert f["url"].startswith("https://") and f["language"] in pn.LANGUAGES and f["level"] in pn.LEVELS
        assert (f["level"] in ("state", "city")) == bool(f["states"])
