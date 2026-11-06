"""News for the app's News tab, from Indian publishers' RSS feeds (see docs/news.md):

  news/state/<state>/<lang>.json   state and city stories, plus national stories naming the state
  news/india/<lang>.json           national stories
  news/world/<lang>.json           world farm stories
  news/index.json                  files written, per-feed status and story counts
  news/_state.json                 the job's memory: ETag/Last-Modified per feed, stories kept for 72 h

Phones never call the publishers or Google; they read these files. Run every 30 minutes by .github/workflows/news.yml.

Usage: python -m farmuncle_pipeline.ingestion.publish_news [--dry-run --out DIR]
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from farmuncle_pipeline.news.core import Feed, build_files, is_farm, merge, parse_items, place_index, tag_places

FEEDS_FILE = Path(__file__).resolve().parent.parent / "news" / "feeds.yaml"
LANGUAGES, LEVELS = ("en", "hi", "kn", "te", "ta", "ml"), ("national", "state", "city", "world")
STATE_KEY, INDEX_KEY = "news/_state.json", "news/index.json"
REPO = "https://github.com/praneeth0100/farmuncle-pipeline"
PUBLIC_URL = "https://pub-671d7ccd15104d01bd8e0f04e8311f6b.r2.dev"


def load_feeds(path: Path = FEEDS_FILE) -> list[Feed]:
    import yaml

    return [{"states": [], "farm_only": False, **f} for f in yaml.safe_load(path.read_text(encoding="utf-8"))["feeds"]]


def _encode(payload: dict) -> bytes:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()


def _read(store, key: str, default):
    try:
        data = store.get(key)
    except Exception as exc:  # a missing file is the first run; anything else must not look like one (it would wipe the 72 h)
        if isinstance(exc, KeyError) or getattr(exc, "response", {}).get("Error", {}).get("Code") in ("NoSuchKey", "404", "NotFound"):
            return default
        raise
    return json.loads(gzip.decompress(data) if data[:2] == b"\x1f\x8b" else data)


def run(store, http, feeds: list[Feed], now: datetime, contact: str = "") -> dict:
    """Fetch every feed once, merge with the saved 72 h, write every file. Exits non-zero when more than half the feeds failed."""
    index = place_index(_read(store, "catalog/mandis.json", {"mandis": []})["mandis"], _read(store, "catalog/states.json", {"states": []})["states"])
    state = _read(store, STATE_KEY, {"feeds": {}, "stories": []})
    old_files = _read(store, INDEX_KEY, {}).get("files", {})
    agent = f"AgroUncleNews/1.0 (+{contact or REPO})"

    def fetch(feed: Feed):
        saved = state["feeds"].get(feed["id"], {})
        headers = {"User-Agent": agent, **({"If-None-Match": saved["etag"]} if saved.get("etag") else {}),
                   **({"If-Modified-Since": saved["modified"]} if saved.get("modified") else {})}
        try:
            status, reply_headers, body = http(feed["url"], headers)
            if status == 304:
                return feed, 304, saved, []
            stories = [tag_places(s, index) for s in parse_items(body, feed) if is_farm(s, feed.get("farm_only", False))]
            return feed, status, {"etag": reply_headers.get("ETag"), "modified": reply_headers.get("Last-Modified")}, stories
        except Exception as error:  # noqa: BLE001 — one dead feed must not stop the others; its saved stories stay
            print(f"[news] {feed['id']} failed: {error}", flush=True)
            return feed, "error", saved, []

    with ThreadPoolExecutor(8) as pool:
        results = list(pool.map(fetch, feeds))
    fresh = [{**{k: v for k, v in s.items() if k != "summary"}, "feed": feed["id"]} for feed, _, _, stories in results for s in stories]
    stories = merge(state["stories"], fresh, now)
    summary = {feed["id"]: {"status": status, "items": sum(1 for s in stories if s.get("feed") == feed["id"])}
               for feed, status, _, _ in results}

    files = build_files(stories, now)
    built = next(iter(files.values()))["built_at"] if files else now.strftime("%Y-%m-%dT%H:%M:%SZ")
    for key in old_files:  # a file whose stories all aged out is emptied, not left stale
        files.setdefault(key, {"v": 1, "built_at": built, "stories": []})
    for key, payload in files.items():
        store.put(key, _encode(payload), "application/json", gzip=True)
    store.put(INDEX_KEY, _encode({"v": 1, "built_at": built, "files": {k: len(p["stories"]) for k, p in sorted(files.items())},
                                  "feeds": summary}), "application/json", gzip=True)
    store.put(STATE_KEY, _encode({"v": 1, "feeds": {f["id"]: meta for f, _, meta, _ in results if meta}, "stories": stories}),
              "application/json", gzip=True)
    for feed_id, row in summary.items():
        print(f"[news] {feed_id}: {row['status']} {row['items']} stories", flush=True)
    failed = sum(1 for row in summary.values() if row["status"] == "error")
    if feeds and failed / len(feeds) > 0.5:
        raise SystemExit(f"[news] {failed} of {len(feeds)} feeds failed")
    return summary


def _http(url: str, headers: dict):
    import requests

    reply = requests.get(url, headers=headers, timeout=20)
    if reply.status_code == 304:
        return 304, reply.headers, b""
    reply.raise_for_status()
    return reply.status_code, reply.headers, reply.content


class DirStore:
    """--dry-run: writes plain JSON into a folder; reads the catalog from the public bucket and earlier runs from the folder."""

    def __init__(self, out: Path, public: str) -> None:
        self.out, self.public = out, public.rstrip("/")

    def get(self, key: str) -> bytes:
        path = self.out / key
        if path.exists():
            return path.read_bytes()
        if not key.startswith("catalog/"):
            raise KeyError(key)
        import requests

        reply = requests.get(f"{self.public}/{key}", headers={"User-Agent": "AgroUncleNews/1.0"}, timeout=60)
        reply.raise_for_status()
        return reply.content

    def put(self, key: str, data: bytes, content_type: str = "", gzip: bool = False) -> None:
        path = self.out / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)


def main() -> None:
    parser = argparse.ArgumentParser(description="News files for the app's News tab.")
    parser.add_argument("--dry-run", action="store_true", help="write into --out instead of R2")
    parser.add_argument("--out", type=Path, default=Path("news-out"))
    args = parser.parse_args()
    if args.dry_run:
        store = DirStore(args.out, os.environ.get("MANDI_DATA_URL") or PUBLIC_URL)
    else:
        from farmuncle_pipeline.archive.store import R2Store

        store = R2Store.from_env()
    run(store, _http, load_feeds(), datetime.now(timezone.utc), os.environ.get("NEWS_CONTACT", ""))


if __name__ == "__main__":
    main()
