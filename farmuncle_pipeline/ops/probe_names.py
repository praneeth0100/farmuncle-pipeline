"""Read-only: list market/crop names the feed used on given dates that our warehouse does not know.

Usage: python -m farmuncle_pipeline.ops.probe_names 2018-03-14 [2019-03-13 ...]
Prints, per unknown (state, district, market), the closest known market in the same state and district,
so renames (Bangalore -> Bengaluru) can be told apart from genuinely new markets. Writes nothing.
"""

from __future__ import annotations

import difflib
import os
import sys

import requests

from farmuncle_pipeline.core.identity_client import _mandi_match_key, _normalize_crop_name
from farmuncle_pipeline.government_constants import QUERY_STATES
from farmuncle_pipeline.ingest_common import validate_startup

URL = "https://www.data.gov.in/backend/dataapi/v1/resource/35985678-0d79-46b4-9ed6-6f13308a1d24"


def _rows(state: str, day: str, key: str):
    off = 0
    while True:
        r = requests.get(URL, params={"api-key": key, "format": "json", "limit": 1000, "offset": off,
                                      "filters[State]": state, "filters[Arrival_Date]": day}, timeout=60)
        r.raise_for_status()
        page = r.json().get("records") or []
        yield from page
        if len(page) < 1000:
            return
        off += 1000


def _all(db, table, cols):
    out, start = [], 0
    while True:
        page = db.table(table).select(cols).order("id").range(start, start + 999).execute().data or []
        out += page
        if len(page) < 1000:
            return out
        start += 1000


def main() -> None:
    key = os.environ["DATA_GOV_API_KEY"]
    db = validate_startup().supabase
    mandis = _all(db, "mandis", "id,name,state,district")
    aliases = {a["normalized_alias"] for a in _all(db, "mandi_aliases", "id,normalized_alias")}
    known = {(_mandi_match_key(m["name"]), m["state"]) for m in mandis}
    by_place: dict[tuple, list[str]] = {}
    for m in mandis:
        by_place.setdefault((m["state"], m["district"]), []).append(m["name"])
    crops = {c["normalized_name"] for c in _all(db, "crops", "id,normalized_name")}

    for day in sys.argv[1:]:
        seen_m, seen_c, rows = set(), set(), 0
        for state in QUERY_STATES:
            for r in _rows(state, day, key):
                rows += 1
                seen_m.add((r.get("State"), r.get("District"), r.get("Market")))
                seen_c.add(r.get("Commodity"))
        print(f"== {day}: {rows} rows, {len(seen_m)} markets, {len(seen_c)} commodities")
        unknown = [m for m in sorted(seen_m, key=str)
                   if (_mandi_match_key(m[2]), m[0]) not in known and _mandi_match_key(m[2]) not in aliases]
        print(f"-- {len(unknown)} markets not in the warehouse under that name")
        for st, dist, mk in unknown:
            near = difflib.get_close_matches(mk or "", by_place.get((st, dist), []), n=1, cutoff=0.5)
            print(f"   {st} | {dist} | {mk}  ->  closest in same district: {near[0] if near else '-'}")
        bad = sorted(c for c in seen_c if c and _normalize_crop_name(c) not in crops)
        print(f"-- {len(bad)} commodities not in the warehouse: {bad}")


if __name__ == "__main__":
    main()
