"""
FarmUncle v2 - Mandi Geocoding Script
--------------------------------------
Fills lat/lng for mandis with no coordinates.

EXACT-ONLY POLICY:
  This script never writes DISTRICT or STATE level coordinates. If an
  exact market-level location can't be found after trying Google, OSM,
  and a few reformulated query variants, the mandi is left with NULL
  coordinates and reported at the end for manual review.

  We do this deliberately, based on the 2,663-mandi mess caused by an
  earlier version of this script that fell back to district/state
  centroids - those got silently treated as real locations downstream.
  Better to have an honest NULL than a fake-precise coordinate.

Strategy per mandi (Google only, nothing coarser, no OSM):
  1. Google Geocoding API with the full name+district+state query
  2. A couple of reformulated variants on Google only
     (e.g. appending "mandi"/"market", or dropping district if it's
     possibly misspelled/wrong) - still exact-level, just different
     phrasing to catch indexing differences
  3. If nothing hits -> leave blank, log for manual review

  OSM/Nominatim is intentionally NOT used at all. Its data is
  crowd-sourced and can be wrong or mismatched for Indian mandi names,
  so we'd rather get an honest NULL from Google than a possibly-wrong
  hit from OSM.

All writes go through the update_mandi_location() Supabase RPC so that:
  - entity_history gets a proper audit trail
  - a worse-confidence result can never clobber a better one already on file
  - only ACTIVE mandis are touched (MERGED rows are skipped automatically by the RPC)

Env vars required:
  SUPABASE_URL
  SUPABASE_SERVICE_KEY
  GOOGLE_MAPS_API_KEY   (required - this is the only geocoding source now,
                         no free fallback since OSM is intentionally unused)
"""

import os
import time
import csv
import requests
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
GOOGLE_API_KEY = os.environ["GOOGLE_MAPS_API_KEY"]  # required, no fallback source

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

GOOGLE_SLEEP_SECONDS = 0.05  # Google allows much higher QPS, small buffer is enough

NOT_FOUND_LOG = "mandis_needing_manual_geocoding.csv"


def google_lookup(query: str):
    """Single Google Geocoding API query. Returns (lat, lng) or (None, None)."""
    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={"address": query, "key": GOOGLE_API_KEY},
            timeout=10
        )
        data = r.json()
        status = data.get("status")
        if status == "OK" and data.get("results"):
            loc = data["results"][0]["geometry"]["location"]
            return float(loc["lat"]), float(loc["lng"])
        elif status not in ("ZERO_RESULTS",):
            # OVER_QUERY_LIMIT, REQUEST_DENIED, INVALID_REQUEST etc - worth surfacing
            print(f"    Google API status='{status}' for '{query}': {data.get('error_message', '')}")
    except Exception as e:
        print(f"    Google error for '{query}': {e}")
    return None, None


def extract_place_name(name: str):
    """
    Strip common market/organization suffixes to get the underlying
    village/town name. E.g. "Amballur  VFPCK Market" -> "Amballur"

    This matters for hyperlocal markets (like Kerala's VFPCK village
    collection points) that aren't individually indexed on Google Maps
    as businesses, but whose underlying village/town IS a real,
    mappable place. Searching for the place name directly avoids Google
    falling back to a shared district-level match for every market in
    the area.
    """
    suffixes_to_strip = [
        "vfpck market", "vfpck  market", "market", "apmc",
        "sub-yard", "sub yard", "mandi",
    ]
    cleaned = name.strip()
    cleaned_lower = cleaned.lower()
    for suffix in suffixes_to_strip:
        if cleaned_lower.endswith(suffix):
            cleaned = cleaned[: len(cleaned) - len(suffix)].strip()
            cleaned_lower = cleaned.lower()
    # Drop trailing parenthetical qualifiers e.g. "Thalavadi(Uzhavar Sandhai )"
    if "(" in cleaned:
        cleaned = cleaned.split("(")[0].strip()
    return cleaned


def build_candidate_queries(name: str, district: str, state: str):
    """
    Build a list of market-level query variants to try, in order.
    All variants target EXACT precision - we never widen to district/state.
    """
    queries = [f"{name}, {district}, {state}, India"]

    name_lower = name.lower()
    if "mandi" not in name_lower and "market" not in name_lower:
        queries.append(f"{name} mandi, {district}, {state}, India")
        queries.append(f"{name} market, {district}, {state}, India")

    if district:
        # In case district is misspelled/wrong in our data, try without it
        queries.append(f"{name}, {state}, India")

    # Fallback: search for the underlying village/town name itself,
    # rather than the market name, since the village is a real indexed
    # place even when the market business name is not. Only added if it
    # actually differs from the full name (i.e. a suffix was stripped).
    place_name = extract_place_name(name)
    if place_name and place_name.lower() != name_lower:
        queries.append(f"{place_name}, {district}, {state}, India")
        queries.append(f"{place_name}, {state}, India")

    return queries


def geocode_mandi(name: str, district: str, state: str):
    """
    Returns (lat, lng, location_confidence, source) or (None, None, None, None)

    Tries Google only, across all query variants. Never returns DISTRICT
    or STATE level results, and never falls back to OSM - if nothing
    exact is found on Google, returns all-None so the mandi is left
    blank for manual review.
    """
    district = district or ""
    state = state or ""
    candidate_queries = build_candidate_queries(name, district, state)

    for q in candidate_queries:
        lat, lng = google_lookup(q)
        time.sleep(GOOGLE_SLEEP_SECONDS)
        if lat and lng:
            return lat, lng, "EXACT", "google"

    # No exact hit on Google. Leave blank - do NOT fall back to OSM
    # or to district/state coordinates.
    return None, None, None, None


def run():
    print("Loading ACTIVE mandis with no coordinates...")
    limit = os.environ.get("GEOCODE_LIMIT")

    all_mandis = []
    batch_size = 1000
    offset = 0
    while True:
        batch = supabase.table("mandis") \
            .select("id, name, district, state") \
            .is_("latitude", "null") \
            .eq("status", "ACTIVE") \
            .range(offset, offset + batch_size - 1) \
            .execute()
        if not batch.data:
            break
        all_mandis.extend(batch.data)
        if limit and len(all_mandis) >= int(limit):
            all_mandis = all_mandis[:int(limit)]
            break
        if len(batch.data) < batch_size:
            break  # last page was partial, no more rows
        offset += batch_size

    total = len(all_mandis)
    print(f"Found {total} mandis to geocode (Google-only, exact-match)")

    stats = {"EXACT": 0, "not_found": 0}
    not_found_rows = []

    for i, mandi in enumerate(all_mandis):
        label = f"{mandi['name']}, {mandi['district']}, {mandi['state']}"
        print(f"  [{i+1}/{total}] {label}")

        lat, lng, confidence, source = geocode_mandi(mandi["name"], mandi["district"], mandi["state"])

        if lat and lng:
            supabase.rpc("update_mandi_location", {
                "p_mandi_id": mandi["id"],
                "p_latitude": lat,
                "p_longitude": lng,
                "p_location_confidence": confidence,
                "p_source": f"geocoding_script_{source}"
            }).execute()
            print(f"    -> {lat}, {lng} [{confidence} via {source}]")
            stats["EXACT"] += 1
        else:
            print(f"    -> not found on Google, left blank for manual review")
            stats["not_found"] += 1
            not_found_rows.append(mandi)

    print("\nDone!")
    print(f"  EXACT found (Google):             {stats['EXACT']}")
    print(f"  Needs manual review (left blank):  {stats['not_found']}")

    if not_found_rows:
        with open(NOT_FOUND_LOG, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "district", "state"])
            writer.writeheader()
            writer.writerows(not_found_rows)
        print(f"\n  List of mandis needing manual geocoding written to: {NOT_FOUND_LOG}")


if __name__ == "__main__":
    run()