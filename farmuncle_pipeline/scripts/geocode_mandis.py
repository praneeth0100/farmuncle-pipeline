"""
FarmUncle v2 - Mandi Geocoding Script
--------------------------------------
Fills lat/lng for mandis with no coordinates.

Strategy per mandi:
  1. Try OpenStreetMap (Nominatim) at market-level precision -> EXACT
  2. Try OpenStreetMap at district-level                      -> DISTRICT
  3. Try Google Geocoding API at market-level (paid fallback)  -> EXACT
  4. Try OpenStreetMap at state-level                          -> STATE
  5. Give up, leave for manual review

All writes go through the update_mandi_location() Supabase RPC so that:
  - entity_history gets a proper audit trail
  - a worse-confidence result can never clobber a better one already on file
  - only ACTIVE mandis are touched (MERGED rows are skipped automatically by the RPC)

Env vars required:
  SUPABASE_URL
  SUPABASE_SERVICE_KEY
  GOOGLE_MAPS_API_KEY   (only needed if OSM fails and Google fallback is used)
"""

import os
import time
import requests
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
GOOGLE_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")  # optional - script still runs OSM-only if missing

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

NOMINATIM_HEADERS = {
    "User-Agent": "FarmUncle App geocoding mandis contact@farmuncle.in"
}

OSM_SLEEP_SECONDS = 1.0   # Nominatim's usage policy: max 1 request/sec
GOOGLE_SLEEP_SECONDS = 0.05  # Google allows much higher QPS, small buffer is enough


def osm_lookup(query: str):
    """Single Nominatim query. Returns (lat, lng) or (None, None)."""
    try:
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": query, "format": "json", "limit": 1},
            headers=NOMINATIM_HEADERS,
            timeout=10
        )
        results = r.json()
        if results:
            return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception as e:
        print(f"    OSM error for '{query}': {e}")
    return None, None


def google_lookup(query: str):
    """Single Google Geocoding API query. Returns (lat, lng) or (None, None)."""
    if not GOOGLE_API_KEY:
        return None, None
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


def geocode_mandi(name: str, district: str, state: str):
    """
    Returns (lat, lng, location_confidence, source) or (None, None, None, None)
    Tries OSM market-level -> Google market-level -> OSM district-level -> OSM state-level.

    District/state-level OSM results are only used as an absolute last resort
    (all EXACT-precision options exhausted), since stacking many mandis on one
    district-center point isn't useful on a map.
    """
    district = district or ""
    state = state or ""

    # 1. OSM market-level (best precision, free)
    lat, lng = osm_lookup(f"{name}, {district}, {state}, India")
    time.sleep(OSM_SLEEP_SECONDS)
    if lat and lng:
        return lat, lng, "EXACT", "osm"

    # 2. Google market-level (paid fallback, still EXACT precision - try this before
    #    giving up to district-level, since a district-center pin isn't useful)
    if GOOGLE_API_KEY:
        lat, lng = google_lookup(f"{name}, {district}, {state}, India")
        time.sleep(GOOGLE_SLEEP_SECONDS)
        if lat and lng:
            return lat, lng, "EXACT", "google"

    # 3. OSM district-level (last resort before giving up entirely)
    if district:
        lat, lng = osm_lookup(f"{district}, {state}, India")
        time.sleep(OSM_SLEEP_SECONDS)
        if lat and lng:
            return lat, lng, "DISTRICT", "osm"

    # 4. OSM state-level (absolute last resort)
    if state:
        lat, lng = osm_lookup(f"{state}, India")
        time.sleep(OSM_SLEEP_SECONDS)
        if lat and lng:
            return lat, lng, "STATE", "osm"

    return None, None, None, None


def run():
    limit = os.environ.get("GEOCODE_LIMIT")  # optional, e.g. "20" for a test run
    print("Loading ACTIVE mandis with no coordinates...")
    query = supabase.table("mandis") \
        .select("id, name, district, state") \
        .is_("latitude", "null") \
        .eq("status", "ACTIVE")
    if limit:
        query = query.limit(int(limit))
    mandis = query.execute()

    total = len(mandis.data)
    print(f"Found {total} mandis to geocode")
    if not GOOGLE_API_KEY:
        print("NOTE: GOOGLE_MAPS_API_KEY not set - running OSM-only (no paid fallback)")

    stats = {"EXACT": 0, "DISTRICT": 0, "STATE": 0, "not_found": 0, "google_used": 0}

    for i, mandi in enumerate(mandis.data):
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
            stats[confidence] += 1
            if source == "google":
                stats["google_used"] += 1
        else:
            print(f"    -> not found at any level")
            stats["not_found"] += 1

    print("\nDone!")
    print(f"  EXACT:      {stats['EXACT']}")
    print(f"  DISTRICT:   {stats['DISTRICT']}")
    print(f"  STATE:      {stats['STATE']}")
    print(f"  Not found:  {stats['not_found']}")
    print(f"  (of which Google fallback used: {stats['google_used']})")


if __name__ == "__main__":
    run()