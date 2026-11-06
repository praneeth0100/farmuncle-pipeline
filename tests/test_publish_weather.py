import json
from datetime import datetime, timezone

import pytest

from farmuncle_pipeline.ingestion import publish_weather as pw
from farmuncle_pipeline.weather.core import cell_of

NOW = datetime(2026, 11, 6, 6, 0, tzinfo=timezone.utc)
MANDIS = {"v": 1, "mandis": [
    {"id": "ka.bengaluru.a", "lat": 12.97, "lng": 77.59, "status": "ACTIVE"},
    {"id": "ka.bengaluru.b", "lat": 12.99, "lng": 77.61, "status": "ACTIVE"},
    {"id": "ka.kolar.kolar", "lat": 13.14, "lng": 78.13, "status": "ACTIVE"},
    {"id": "ka.nowhere.x", "lat": None, "lng": None, "status": "ACTIVE"},
    {"id": "ka.closed.y", "lat": 12.0, "lng": 77.0, "status": "INACTIVE"},
]}
POWER = {"properties": {"parameter": {"PRECTOTCORR": {f"202601{d:02d}": 1.0 for d in range(5, 12)},
                                      "T2M_MAX": {f"202601{d:02d}": 30.0 for d in range(5, 12)}}}}
METNO = {"properties": {"timeseries": [{"time": "2026-11-06T06:00:00Z", "data": {
    "instant": {"details": {"air_temperature": 29.0}}, "next_1_hours": {"details": {"precipitation_amount": 0.5}}}}]}}


class Store:
    def __init__(self, files=None):
        self.files = {k: json.dumps(v).encode() for k, v in (files or {}).items()}

    def get(self, key):
        return self.files[key]

    def put(self, key, data, content_type=""):
        self.files[key] = data

    def json(self, key):
        return json.loads(self.files[key])


def fetcher(payload, fail=()):
    calls = []

    def fetch(url, headers=None):
        calls.append(url)
        if any(f in url for f in fail):
            raise OSError("boom")
        return 200, {"Expires": "Thu, 06 Nov 2026 07:00:00 GMT", "Last-Modified": "Thu, 06 Nov 2026 05:30:00 GMT"}, payload
    fetch.calls = calls
    return fetch


def test_history_writes_one_file_per_cell_and_the_catalogue():
    store, fetch = Store({"catalog/mandis.json": MANDIS}), fetcher(POWER)
    summary = pw.run_history(store, fetch, NOW)
    cells = store.json("catalog/weather_cells.json")["cells"]
    assert set(cells) == {"ka.bengaluru.a", "ka.bengaluru.b", "ka.kolar.kolar"}
    assert cells["ka.bengaluru.a"] == cells["ka.bengaluru.b"] == cell_of(12.97, 77.59)
    assert len(fetch.calls) == 2 and summary["written"] == 2
    hist = store.json(f"weather/history/{cells['ka.kolar.kolar']}.json")
    assert hist["start"] == "2016-01-04" and hist["source"] == "NASA POWER"
    assert "start=20160104" in fetch.calls[0] and "time-standard=UTC" in fetch.calls[0]


def test_forecast_skips_unexpired_cells():
    cell = cell_of(13.14, 78.13)
    store = Store({"catalog/mandis.json": MANDIS, f"weather/forecast/{cell}.json": {"v": 1, "expires": "2026-11-06T07:00:00+00:00"}})
    fetch = fetcher(METNO)
    pw.run_forecast(store, fetch, NOW)
    assert len(fetch.calls) == 1  # Kolar's cell is still fresh; only Bengaluru's is asked
    fresh = store.json(f"weather/forecast/{cell_of(12.97, 77.59)}.json")
    assert fresh["days"][0]["date"] == "2026-11-06" and fresh["source"] == "MET Norway"
    assert "lat=" in fetch.calls[0] and len(fetch.calls[0].split("lat=")[1].split("&")[0].split(".")[1]) <= 4


def test_run_fails_only_above_twenty_percent_errors():
    one_of_two = fetcher(POWER, fail=("latitude=13.25",))
    with pytest.raises(SystemExit):
        pw.run_history(Store({"catalog/mandis.json": MANDIS}), one_of_two, NOW)
    many = {"v": 1, "mandis": [{"id": f"x.{i}", "lat": 10 + i, "lng": 76.0, "status": "ACTIVE"} for i in range(10)]}
    pw.run_history(Store({"catalog/mandis.json": many}), fetcher(POWER, fail=("latitude=10.25",)), NOW)  # 1 of 10: fine


def test_forecast_requests_stay_under_twenty_a_second():
    many = {"v": 1, "mandis": [{"id": f"x.{i}", "lat": 10 + i, "lng": 76.0, "status": "ACTIVE"} for i in range(8)]}
    times = []
    base = fetcher(METNO)

    def timed(url, headers=None):
        times.append(__import__("time").monotonic())
        return base(url, headers)
    pw.run_forecast(Store({"catalog/mandis.json": many}), timed, NOW)
    times.sort()
    assert len(times) == 8 and min(b - a for a, b in zip(times, times[1:])) >= 0.05 - 1e-3
