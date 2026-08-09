"""
Location provider abstraction for mandi geocoding v2.

Two independent providers, per the blueprint (never blend them into
one function -- keep each swappable/testable on its own):

  GooglePlacesProvider  -- Places API (New) Text Search. This is the
      actual fix over the old script: Places indexes real-world POIs
      and businesses, so a query can return "APMC Karnal" as its own
      entity distinct from Karnal-the-town. The old script used the
      Geocoding API, which only ever resolves to administrative points
      (locality/village/district) and structurally cannot return a
      market as a distinct result -- that was the root cause of the
      "90% just town names" problem.

  OSMNominatimProvider  -- free, used as (a) a fallback when Places
      finds nothing, and (b) a cross-check to upgrade confidence when
      both providers agree. OSM sometimes has amenity=marketplace nodes
      for markets Google hasn't indexed as a business at all.

  DistrictReference      -- NOT a market-finding provider. Uses the
      Geocoding API deliberately (its town/district-level precision is
      exactly the right tool for this one job) to get a reference point
      per district, cached, so candidate markets can be distance-
      sanity-checked against their claimed district. Reusing the
      Geocoding API here is intentional, not a regression -- see
      module docstring above for why it's wrong for market search but
      right for this.

COST NOTE: field masks below request only Basic-tier fields (id,
displayName, formattedAddress, location, types) to keep Places Text
Search on the cheaper SKU. Google's exact field-to-SKU billing mapping
does change over time -- verify current pricing at
https://developers.google.com/maps/documentation/places/web-service/usage-and-billing
before running a large batch, don't just trust this comment long-term.
"""

import math
import time

import requests

PLACES_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
GEOCODING_URL = "https://maps.googleapis.com/maps/api/geocode/json"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# Basic-tier field mask -- see COST NOTE above.
PLACES_FIELD_MASK = (
    "places.id,places.displayName,places.formattedAddress,"
    "places.location,places.types,places.primaryType"
)

# Types that, on their own, mean Places resolved to an administrative
# area rather than an actual place/business -- same conceptual bug the
# old Geocoding-based script had, just checked against Places' type
# vocabulary instead of Geocoding's. A result whose types are a subset
# of this set gets rejected regardless of name-match score.
_ADMIN_ONLY_TYPES = {
    "locality", "political", "administrative_area_level_1",
    "administrative_area_level_2", "administrative_area_level_3",
    "administrative_area_level_4", "administrative_area_level_5",
    "country", "postal_code", "postal_town", "sublocality",
    "sublocality_level_1", "neighborhood",
}


class LocationCandidate:
    def __init__(self, name, lat, lng, place_types, formatted_address, provider, raw=None):
        self.name = name
        self.lat = lat
        self.lng = lng
        self.place_types = set(place_types or [])
        self.formatted_address = formatted_address or ""
        self.provider = provider
        self.raw = raw or {}

    def is_admin_only(self) -> bool:
        """True if every type on this result is an administrative/area
        type -- i.e. this is a locality/town/district, not an actual
        place. Empty types is treated as admin-only (fail closed)."""
        if not self.place_types:
            return True
        return self.place_types.issubset(_ADMIN_ONLY_TYPES)

    def __repr__(self):
        return f"<LocationCandidate {self.provider} '{self.name}' ({self.lat},{self.lng}) types={self.place_types}>"


class GooglePlacesProvider:
    def __init__(self, api_key, sleep_seconds=0.05, timeout=10):
        self.api_key = api_key
        self.sleep_seconds = sleep_seconds
        self.timeout = timeout

    def search(self, query: str, region_code: str = "IN"):
        """Returns a list of LocationCandidate, best-first (Places
        already ranks by relevance). Empty list on no results, API
        error, or exception -- callers treat that the same as ZERO_RESULTS
        used to be treated, never raise up into the batch loop."""
        try:
            resp = requests.post(
                PLACES_TEXT_SEARCH_URL,
                headers={
                    "Content-Type": "application/json",
                    "X-Goog-Api-Key": self.api_key,
                    "X-Goog-FieldMask": PLACES_FIELD_MASK,
                },
                json={"textQuery": query, "regionCode": region_code},
                timeout=self.timeout,
            )
            time.sleep(self.sleep_seconds)
            if resp.status_code != 200:
                # Surface Google's actual error message (not just the code) --
                # a bare "HTTP_403" tells you nothing about WHY (wrong API
                # enabled, key restriction, billing, quota, malformed
                # request); the response body's error.message field does.
                try:
                    err_msg = resp.json().get("error", {}).get("message", "")
                except Exception:
                    err_msg = ""
                status_detail = f"HTTP_{resp.status_code}: {err_msg or resp.text[:200]}"
                return [], {"status": status_detail}
            data = resp.json()
            places = data.get("places", [])
            candidates = []
            for p in places:
                loc = p.get("location", {})
                candidates.append(LocationCandidate(
                    name=p.get("displayName", {}).get("text", ""),
                    lat=loc.get("latitude"),
                    lng=loc.get("longitude"),
                    place_types=p.get("types", []),
                    formatted_address=p.get("formattedAddress", ""),
                    provider="google_places",
                    raw=p,
                ))
            return candidates, {"status": "OK" if candidates else "ZERO_RESULTS"}
        except Exception as e:
            return [], {"status": f"EXCEPTION: {e}"}


class OSMNominatimProvider:
    """Free fallback/cross-check. Nominatim's usage policy caps this at
    ~1 req/sec and requires a real User-Agent identifying the app --
    both enforced here."""

    def __init__(self, user_agent="FarmUncle-MandiGeocoder/2.0 (contact: repo issue tracker)",
                 sleep_seconds=1.1, timeout=10):
        self.user_agent = user_agent
        self.sleep_seconds = sleep_seconds
        self.timeout = timeout

    def search(self, query: str):
        try:
            resp = requests.get(
                NOMINATIM_URL,
                params={
                    "q": query, "format": "json", "addressdetails": 1,
                    "countrycodes": "in", "limit": 5,
                },
                headers={"User-Agent": self.user_agent},
                timeout=self.timeout,
            )
            time.sleep(self.sleep_seconds)  # Nominatim policy: max ~1 req/sec
            if resp.status_code != 200:
                return [], {"status": f"HTTP_{resp.status_code}"}
            results = resp.json()
            candidates = []
            for r in results:
                osm_class = r.get("class", "")
                osm_type = r.get("type", "")
                # Nominatim doesn't share Places' type vocabulary; map its
                # class/type into something is_admin_only() can read by
                # treating admin-boundary/place-node results as admin-only
                # and everything else (shop=marketplace, amenity=*, etc.)
                # as a real POI signal.
                types = [f"{osm_class}:{osm_type}"]
                if osm_class in ("boundary", "place") and osm_type in (
                    "administrative", "town", "village", "city",
                    "hamlet", "suburb",
                ):
                    types = ["locality", "political"]
                candidates.append(LocationCandidate(
                    name=r.get("display_name", "").split(",")[0],
                    lat=float(r["lat"]) if r.get("lat") else None,
                    lng=float(r["lon"]) if r.get("lon") else None,
                    place_types=types,
                    formatted_address=r.get("display_name", ""),
                    provider="osm_nominatim",
                    raw=r,
                ))
            return candidates, {"status": "OK" if candidates else "ZERO_RESULTS"}
        except Exception as e:
            return [], {"status": f"EXCEPTION: {e}"}


class DistrictReference:
    """Gets (and caches, in-process) a reference lat/lng per
    district+state using the Geocoding API -- deliberately, since
    district-level precision is exactly what that API is good at. Used
    only for the distance sanity check in scorer.py, never written to
    mandis.latitude/longitude directly."""

    def __init__(self, api_key, sleep_seconds=0.05, timeout=10):
        self.api_key = api_key
        self.sleep_seconds = sleep_seconds
        self.timeout = timeout
        self._cache = {}

    def get(self, district: str, state: str):
        key = (district or "").strip().lower(), (state or "").strip().lower()
        if key in self._cache:
            return self._cache[key]
        if not district and not state:
            self._cache[key] = None
            return None
        query = ", ".join(p for p in [district, state, "India"] if p)
        try:
            resp = requests.get(
                GEOCODING_URL,
                params={"address": query, "key": self.api_key},
                timeout=self.timeout,
            )
            time.sleep(self.sleep_seconds)
            data = resp.json()
            if data.get("status") == "OK" and data.get("results"):
                loc = data["results"][0]["geometry"]["location"]
                point = (float(loc["lat"]), float(loc["lng"]))
                self._cache[key] = point
                return point
        except Exception:
            pass
        self._cache[key] = None
        return None


def haversine_km(lat1, lng1, lat2, lng2) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))