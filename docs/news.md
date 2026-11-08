# News files

`farmuncle_pipeline/ingestion/publish_news.py`, run by `.github/workflows/news.yml` every 30 minutes, turns Indian
publishers' RSS feeds into small gzipped JSON files in R2. The app's News tab reads only these files; phones never call
the publishers or Google News.

## Files

| Key | What |
|---|---|
| `news/state/<state>/<lang>.json` | state and city stories for that state, plus national stories that name it |
| `news/india/<lang>.json` | national stories |
| `news/world/en.json` | world farm stories |
| `news/index.json` | `built_at`, story count per file, and per feed `{status: 200 \| 304 \| "error", items}` |
| `news/_state.json` | the job's memory: ETag / Last-Modified per feed and every story of the last 72 hours |

State codes are those of `catalog/mandis.json` (`ka`), district codes its `district` field (`ka.mysuru`). Languages:
`en hi kn te ta ml`. Every file is `{"v": 1, "built_at": "…Z", "stories": [...]}`, newest first, at most 150 stories:

```json
{ "id": "<sha1 of link>", "title": "…", "link": "https://…", "outlet": "The Hindu", "published": "2026-11-06T09:10:00Z",
  "lang": "en", "image": "https://…jpg", "level": "state", "districts": ["ka.mysuru"], "states": ["ka"], "farm": true }
```

`level` is the feed's: `national | state | city | world`. `farm` is true for stories from an agriculture section.
Files are gzipped (`Content-Encoding: gzip`) with `Cache-Control: public, max-age=900`.

## Each run

1. One conditional GET per feed (20 s timeout, User-Agent with `NEWS_CONTACT`). A feed that answers 304 or fails keeps
   its saved stories. The run fails only when more than half the feeds failed, after still writing the files and index.
2. Parse with `feedparser`. Picture: `media:content` / `media:thumbnail` (largest up to 800 px wide), then an image
   enclosure, then the first `<img>` in the item. `https://` only; article pages are never fetched.
3. Farm filter: agriculture sections keep everything; other feeds keep items whose headline or description has a farm
   word and no block word (Sensex, cricket, film…) in English or the feed's language. Lists in `news/core.py`.
4. Place tags: district and state names come from the codes (`bengaluru_rural` → "bengaluru rural", plus a distinctive
   single word), state names from `catalog/states.json`, and regional-script names from `news/place_names.yaml`
   (Telugu for every Andhra and Telangana district so far; other languages' stories land in State, not District).
   A mandi's whole name also names its district. A feed listing two states sends each story to the state it names;
   naming neither, to its `unplaced` states, else both. Prefer an outlet's one-state section feeds where they exist.
5. Merge with the saved stories, drop anything older than 72 hours, fold the same story from two outlets
   (headline-word overlap ≥ 0.5), write every file.

## The feed list

`farmuncle_pipeline/news/feeds.yaml`. Each entry: `id`, `outlet` (shown on the card), `url`, `language`, `level`,
`states` (state and city feeds), optional `districts` (city feeds), `unplaced`, `farm_only`, `upgrade_images`.

- **Drop a feed:** delete its line. Its stories age out of the files within 72 hours.
- **Add a feed:** check that it has items, that pictures come through, and that `level`/`states` are right; run
  `python -m farmuncle_pipeline.ingestion.publish_news --dry-run --out ./news-out` and read the per-feed counts.
- **A dead feed** shows in `news/index.json` as `"status": "error"` or `items: 0` for many runs.

## Publisher terms

The app shows headline, picture and outlet name, and opens the publisher's own page. The owner chose to ship before
checking each publisher's terms for commercial reuse (6 Nov 2026). Check them after launch and drop any feed whose
terms say no.
