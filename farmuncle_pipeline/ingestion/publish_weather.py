"""Weather for Screen 3, per POWER grid cell (see docs/weather.md):

  catalog/weather_cells.json       mandi id → cell
  weather/history/<cell>.json      Monday weeks since 2016-01-04: [rain mm, max °C] or null (NASA POWER), weekly
  weather/forecast/<cell>.json     next days: rain mm, max °C (MET Norway), every 6 hours

Phones never call the weather services; they read these files. Coordinates come from the published catalog/mandis.json.

Usage: python -m farmuncle_pipeline.ingestion.publish_weather --history | --forecast [--limit N]
"""

from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

from farmuncle_pipeline.weather.core import cell_centre, cell_of, parse_metno, parse_power, weekly

HISTORY_START = date(2016, 1, 4)  # a Monday, like the price weeks
POWER_URL = ("https://power.larc.nasa.gov/api/temporal/daily/point?parameters=PRECTOTCORR,T2M_MAX&community=AG"
             "&latitude={lat}&longitude={lng}&start={start}&end={end}&format=JSON&time-standard=UTC")
METNO_URL = "https://api.met.no/weatherapi/locationforecast/2.0/compact?lat={lat:.4f}&lon={lng:.4f}"
MAX_FAILED = 0.2


def cells_for(mandis: list[dict]) -> dict[str, str]:
    return {m["id"]: cell_of(m["lat"], m["lng"]) for m in mandis
            if m.get("status") == "ACTIVE" and m.get("lat") is not None and m.get("lng") is not None}


def _encode(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()


def _catalogue(store) -> dict[str, str]:
    cells = cells_for(json.loads(store.get("catalog/mandis.json"))["mandis"])
    store.put("catalog/weather_cells.json", _encode({"v": 1, "cells": cells}), "application/json")
    return cells


def _run(cells: set[str], work, workers: int) -> dict:
    """Runs `work(cell)` for every cell; one failure is logged and skipped, too many fail the run."""
    failed: list[str] = []

    def safe(cell):
        for attempt in range(3):
            try:
                return work(cell)
            except Exception as error:  # noqa: BLE001 — any network or parse error: retry, then skip the cell
                if attempt == 2:
                    print(f"[weather] {cell} failed: {error}", flush=True)
                    failed.append(cell)
                    return False
                time.sleep(2 ** attempt)
    with ThreadPoolExecutor(workers) as pool:
        written = sum(1 for done in pool.map(safe, sorted(cells)) if done)
    summary = {"cells": len(cells), "written": written, "failed": len(failed)}
    print(f"[weather] {summary}", flush=True)
    if cells and len(failed) / len(cells) > MAX_FAILED:
        raise SystemExit(f"[weather] {len(failed)} of {len(cells)} cells failed")
    return summary


def run_history(store, fetch, now: datetime, limit: int | None = None) -> dict:
    cells = sorted(set(_catalogue(store).values()))[:limit]
    end = (now.astimezone(timezone.utc).date() - timedelta(days=1)).strftime("%Y%m%d")
    stamp = now.isoformat(timespec="seconds")

    def work(cell):
        lat, lng = cell_centre(cell)
        _, _, payload = fetch(POWER_URL.format(lat=lat, lng=lng, start=HISTORY_START.strftime("%Y%m%d"), end=end))
        weeks = weekly(parse_power(payload), HISTORY_START)
        store.put(f"weather/history/{cell}.json", _encode({"v": 1, "cell": cell, "lat": lat, "lng": lng,
                  "start": HISTORY_START.isoformat(), "weeks": weeks, "updated_at": stamp, "source": "NASA POWER"}),
                  "application/json")
        return True
    return _run(set(cells), work, workers=3)


def run_forecast(store, fetch, now: datetime, limit: int | None = None, contact: str = "") -> dict:
    cells = sorted(set(_catalogue(store).values()))[:limit]
    headers = {"User-Agent": f"AgroUncle/1.0 {contact}".strip(), "Accept-Encoding": "gzip"}

    def work(cell):
        key = f"weather/forecast/{cell}.json"
        try:
            old = json.loads(store.get(key))
        except Exception:  # noqa: BLE001 — no stored forecast yet
            old = None
        if old and old.get("expires") and datetime.fromisoformat(old["expires"]) > now:
            return False  # MET Norway: never ask again before Expires
        lat, lng = cell_centre(cell)
        ask = {**headers, **({"If-Modified-Since": old["last_modified"]} if old and old.get("last_modified") else {})}
        status, reply_headers, payload = fetch(METNO_URL.format(lat=lat, lng=lng), ask)
        expires = parsedate_to_datetime(reply_headers["Expires"]).isoformat() if reply_headers.get("Expires") else None
        if status == 304 and old:
            store.put(key, _encode({**old, "expires": expires}), "application/json")
            return False
        store.put(key, _encode({"v": 1, "cell": cell, "updated_at": now.isoformat(timespec="seconds"), "expires": expires,
                  "last_modified": reply_headers.get("Last-Modified"), "days": parse_metno(payload), "source": "MET Norway"}),
                  "application/json")
        return True
    return _run(set(cells), work, workers=5)


def _http(url: str, headers: dict | None = None):
    import requests

    reply = requests.get(url, headers=headers or {}, timeout=60)
    if reply.status_code == 304:
        return 304, reply.headers, None
    reply.raise_for_status()
    return reply.status_code, reply.headers, reply.json()


def main() -> None:
    from farmuncle_pipeline.archive.store import R2Store

    parser = argparse.ArgumentParser(description="Weather files for Screen 3.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--history", action="store_true")
    mode.add_argument("--forecast", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    store, now = R2Store.from_env(), datetime.now(timezone.utc)
    if args.history:
        run_history(store, _http, now, args.limit)
    else:
        run_forecast(store, _http, now, args.limit, os.environ.get("WEATHER_CONTACT") or "github.com/praneeth0100/farmuncle-pipeline")


if __name__ == "__main__":
    main()
