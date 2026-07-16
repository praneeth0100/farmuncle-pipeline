"""
FarmUncle v2 - Kerala VFPCK Village Geocoding (targeted fix)
--------------------------------------------------------------
Purpose: re-geocode the 59 specific Kerala VFPCK-market mandis that
collapsed onto shared district-level coordinates in the main
geocode_mandis.py run, because Google doesn't index the market names
("Amballur VFPCK Market" etc.) as businesses.

Fix: query Google directly for the underlying VILLAGE name (extracted
from the market name), not the market name and not the district. This
is a much narrower, more targeted query than the main script uses.

Same EXACT-only, Google-only, no-OSM policy as the main script:
  - No DISTRICT/STATE fallback
  - Rejects low-precision (non-locality) matches
  - Leaves blank for manual review if nothing precise is found

Env vars required:
  SUPABASE_URL
  SUPABASE_SERVICE_KEY
  GOOGLE_MAPS_API_KEY
"""

import os
import time
import requests
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
GOOGLE_API_KEY = os.environ["GOOGLE_MAPS_API_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

GOOGLE_SLEEP_SECONDS = 0.05

# The 59 mandis needing a targeted village-name lookup, with the
# underlying village/place name already extracted by hand.
# Format: (mandi_id, village_name, district, state)
TARGET_MANDIS = [
    (690, "Alengad", "Thrissur", "Keralam"),
    (1494, "Amballur", "Thrissur", "Keralam"),
    (2346, "Chazhur", "Thrissur", "Keralam"),
    (502, "Chelakkara", "Thrissur", "Keralam"),
    (2071, "Kadukutty", "Thrissur", "Keralam"),
    (2073, "Karalam", "Thrissur", "Keralam"),
    (40, "Karuvannur", "Thrissur", "Keralam"),
    (1904, "Kodassery", "Thrissur", "Keralam"),
    (814, "Kuzhur", "Thrissur", "Keralam"),
    (563, "Marottichal", "Thrissur", "Keralam"),
    (2328, "Mattathur", "Thrissur", "Keralam"),
    (2152, "Melocr", "Thrissur", "Keralam"),
    (187, "Muriyad", "Thrissur", "Keralam"),
    (654, "Nooluvally", "Thrissur", "Keralam"),
    (1091, "Pananchery", "Thrissur", "Keralam"),
    (497, "Pariyaram", "Thrissur", "Keralam"),
    (1881, "Puthur", "Thrissur", "Keralam"),
    (2072, "Thottippal", "Thrissur", "Keralam"),
    (588, "Varandarappilly", "Thrissur", "Keralam"),
    (2650, "Veloorkkara", "Thrissur", "Keralam"),
    (2337, "Adithyapuram", "Kottayam", "Keralam"),
    (1828, "Athirampuzha", "Kottayam", "Keralam"),
    (1488, "Aymanam", "Kottayam", "Keralam"),
    (1119, "Ettumanoor", "Kottayam", "Keralam"),
    (795, "Kattampak", "Kottayam", "Keralam"),
    (2322, "Kottayam", "Kottayam", "Keralam"),
    (2786, "Kurichy", "Kottayam", "Keralam"),
    (2151, "Kuriem", "Kottayam", "Keralam"),
    (2142, "Thottuva", "Kottayam", "Keralam"),
    (1606, "Vakathanam", "Kottayam", "Keralam"),
    (2154, "Vempally", "Kottayam", "Keralam"),
    (1489, "Agali", "Palakad", "Keralam"),
    (1820, "Alenellur", "Palakad", "Keralam"),
    (2132, "Elevancheri", "Palakad", "Keralam"),
    (817, "Kadambazhi puram", "Palakad", "Keralam"),
    (800, "Karimpuzha", "Palakad", "Keralam"),
    (1491, "Kizhakkancheri", "Palakad", "Keralam"),
    (1842, "Kottayi", "Palakad", "Keralam"),
    (1994, "Moochamkundu", "Palakad", "Keralam"),
    (802, "Paliyamangalam", "Palakad", "Keralam"),
    (183, "Vaniyamkulam", "Palakad", "Keralam"),
    (774, "Vithinasserri", "Palakad", "Keralam"),
    (794, "Chathannoore", "Kollam", "Keralam"),
    (1997, "Inchakkad", "Kollam", "Keralam"),
    (49, "Kadakkal", "Kollam", "Keralam"),
    (1654, "Nedumpaikulam", "Kollam", "Keralam"),
    (2875, "Neduvathoor", "Kollam", "Keralam"),
    (1867, "Piravanthoor", "Kollam", "Keralam"),
    (1781, "Punalur", "Kollam", "Keralam"),
    (1914, "Sooranad", "Kollam", "Keralam"),
    (2343, "Thengamam", "Pathanamthitta", "Keralam"),
    (2331, "Edathwa", "Alappuzha", "Keralam"),
    (809, "Pandanadu", "Alappuzha", "Keralam"),
    (1827, "Vallikunnam", "Alappuzha", "Keralam"),
    (2333, "Venmony", "Alappuzha", "Keralam"),
    (1746, "Kolayad", "Kannur", "Keralam"),
    (2074, "Maloor", "Kannur", "Keralam"),
    (1990, "Pattiyam", "Kannur", "Keralam"),
    (2342, "Payam", "Kannur", "Keralam"),
]


def is_precise_enough(result: dict) -> bool:
    """Reject district/administrative-only matches - same logic as main script."""
    if result.get("partial_match"):
        return False
    location_type = result.get("geometry", {}).get("location_type")
    components = result.get("address_components", [])
    component_types = set()
    for c in components:
        component_types.update(c.get("types", []))
    has_fine_grained = bool(component_types & {
        "locality", "sublocality", "sublocality_level_1",
        "neighborhood", "postal_town", "village"
    })
    if location_type == "APPROXIMATE" and not has_fine_grained:
        return False
    if not has_fine_grained:
        return False
    return True


def google_lookup(query: str):
    """Single Google Geocoding API query. Returns (lat, lng, raw_result) or (None, None, None)."""
    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={"address": query, "key": GOOGLE_API_KEY},
            timeout=10
        )
        data = r.json()
        status = data.get("status")
        if status == "OK" and data.get("results"):
            result = data["results"][0]
            loc = result["geometry"]["location"]
            return float(loc["lat"]), float(loc["lng"]), result
        elif status not in ("ZERO_RESULTS",):
            print(f"    Google API status='{status}' for '{query}': {data.get('error_message', '')}")
    except Exception as e:
        print(f"    Google error for '{query}': {e}")
    return None, None, None


def run():
    total = len(TARGET_MANDIS)
    print(f"Geocoding {total} Kerala village mandis directly by village name...")

    stats = {"EXACT": 0, "REJECTED_LOW_PRECISION": 0, "not_found": 0}
    still_needs_review = []

    for i, (mandi_id, village, district, state) in enumerate(TARGET_MANDIS):
        query = f"{village}, {district}, {state}, India"
        print(f"  [{i+1}/{total}] {query}")

        lat, lng, result = google_lookup(query)
        time.sleep(GOOGLE_SLEEP_SECONDS)

        if lat and lng:
            if is_precise_enough(result):
                supabase.rpc("update_mandi_location", {
                    "p_mandi_id": mandi_id,
                    "p_latitude": lat,
                    "p_longitude": lng,
                    "p_location_confidence": "EXACT",
                    "p_source": "geocoding_script_google_village_name"
                }).execute()
                print(f"    -> {lat}, {lng} [EXACT - accepted]")
                stats["EXACT"] += 1
            else:
                print(f"    -> {lat}, {lng} [REJECTED - too coarse, likely district match]")
                stats["REJECTED_LOW_PRECISION"] += 1
                still_needs_review.append((mandi_id, village, district, state))
        else:
            print(f"    -> not found")
            stats["not_found"] += 1
            still_needs_review.append((mandi_id, village, district, state))

    print("\nDone!")
    print(f"  EXACT accepted:              {stats['EXACT']}")
    print(f"  Rejected (too coarse):       {stats['REJECTED_LOW_PRECISION']}")
    print(f"  Not found at all:            {stats['not_found']}")

    if still_needs_review:
        print(f"\n  Still needs manual review ({len(still_needs_review)}):")
        for mandi_id, village, district, state in still_needs_review:
            print(f"    id={mandi_id}  {village}, {district}, {state}")


if __name__ == "__main__":
    run()
