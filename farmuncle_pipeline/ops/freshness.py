"""Hourly: fail (so GitHub emails the owner) when the app's data has gone stale, read from the public manifest.

Analytics older than 30 hours means a nightly run was missed. Live prices older than 3 hours between 08:00 and 22:00 IST
means the hourly pull or publish stopped while mandis report.
"""

from __future__ import annotations

import json
import sys
import urllib.request
from datetime import datetime, timedelta

from farmuncle_pipeline.ingestion.publish_live import IST

ANALYTICS_MAX, LIVE_MAX = timedelta(hours=30), timedelta(hours=3)
DAY_HOURS = range(8, 22)


def problems(manifest: dict, now: datetime) -> list[str]:
    out = []
    built = manifest.get("analytics_built_at")
    if not built or now - datetime.fromisoformat(built) > ANALYTICS_MAX:
        out.append(f"analytics last built {built or 'never'}: charts and changes are stale")
    live = manifest.get("live_updated_at")
    if now.astimezone(IST).hour in DAY_HOURS and (not live or now - datetime.fromisoformat(live) > LIVE_MAX):
        out.append(f"live prices last published {live or 'never'}: today's prices are not reaching the app")
    return out


def main() -> None:
    base = sys.argv[1].rstrip("/")
    request = urllib.request.Request(f"{base}/meta/manifest.json", headers={"User-Agent": "farmuncle-freshness"})  # r2.dev refuses urllib's default
    with urllib.request.urlopen(request, timeout=30) as response:
        found = problems(json.load(response), datetime.now(IST))
    for p in found:
        print(f"::error::{p}")
    sys.exit(1 if found else 0)


if __name__ == "__main__":
    main()
