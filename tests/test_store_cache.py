from farmuncle_pipeline.archive.store import R2Store, cache_seconds


class Client:
    def put_object(self, **kw):
        self.kw = kw


def test_json_gets_the_spec_cache_times_and_other_files_none():
    assert [cache_seconds(k) for k in ("meta/manifest.json", "live/mandi/a.json", "home/a.json", "chart/x/a.json", "catalog/states.json")] == [60, 600, 600, 3600, 3600]
    c = Client()
    R2Store(c, "b").put("live/crop/onion/ka.json", b"{}", "application/json")
    assert c.kw["CacheControl"] == "public, max-age=600"
    assert c.kw["ContentType"] == "application/json; charset=utf-8"  # phones decode a bare application/json as Latin-1
    R2Store(c, "b").put("facts/daily/date=2026-10-20.parquet", b"x")
    assert "CacheControl" not in c.kw


def test_news_files_cache_15_minutes():
    assert cache_seconds("news/india/en.json") == 900
    assert [cache_seconds(k) for k in ("meta/manifest.json", "live/a.json", "catalog/states.json")] == [60, 600, 3600]


def test_put_gzip_sets_encoding():
    import gzip

    c = Client()
    R2Store(c, "b").put("news/india/en.json", b'{"v":1}', "application/json", gzip=True)
    assert gzip.decompress(c.kw["Body"]) == b'{"v":1}'
    assert c.kw["ContentEncoding"] == "gzip"
    assert c.kw["CacheControl"] == "public, max-age=900"
    R2Store(c, "b").put("news/india/en.json", b"{}", "application/json")
    assert "ContentEncoding" not in c.kw and c.kw["Body"] == b"{}"
