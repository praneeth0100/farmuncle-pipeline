"""
FarmUncle v2 - Mandi Geocoding Script (v2: Places-API based)
---------------------------------------------------------------
Replaces geocode_mandis.py's core strategy. That script used the
Geocoding API, which can only ever resolve to an administrative point
(locality/village/district) -- it cannot return a market/business as
its own entity. Every "EXACT" result it produced was actually a town
or village centroid wearing an EXACT label. This script uses the
Places API (Text Search) instead, which searches real-world POIs, so a
query can return the actual market yard as a distinct place.

Strategy per mandi:
  1. Build up to 2 Google Places queries: the mandi's state-specific
     market terminology term first (from market_terminology.py), a
     generic agricultural-market term second, only if the first found
     nothing usable.
  2. Every Places result is scored (scorer.py) -- results whose types
     are administrative-only (locality/political/etc, no POI signal)
     are hard-rejected before scoring even starts, so a town centroid
     can never win regardless of name similarity.
  3. OSM Nominatim is tried as a 3rd, free fallback (more query variants
     allowed here since there's no per-call cost) and as a cross-check:
     if Google's best candidate and OSM's best candidate agree within
     ~3km, that's real corroborating evidence and bumps confidence.
  4. A district-reference distance check (Geocoding API, once per
     district, cached) hard-rejects any candidate implausibly far from
     its claimed district.
  5. Every query, to every provider, goes through the same
     provider/query cache (mandi_location_search_cache table) so
     re-running this script during testing doesn't re-spend API
     credits on identical queries.
  6. If nothing survives with a real score, the mandi is left blank
     (NULL lat/lng, no confidence written) for manual review -- never a
     town centroid, never a guess.

DRY_RUN is on by default (blueprint's own mandatory requirement,
§22) -- nothing is written to Supabase unless DRY_RUN=false is set
explicitly. Always run a dry run first and spot-check the audit CSV.

Env vars required:
  SUPABASE_URL
  SUPABASE_SERVICE_KEY
  GOOGLE_MAPS_API_KEY     (used for both Places Text Search and the
                           district-reference Geocoding lookups)
Env vars optional:
  GEOCODE_LIMIT           (int, caps how many mandis this run processes)
  DRY_RUN                 ("true"/"false", default "true")
  BATCH_SIZE              (int, default 300 -- how many NULL-coordinate
                           mandis to pull per run; combine with
                           GEOCODE_LIMIT for smaller test batches)
"""

import csv
import os

from supabase import create_client

from farmuncle_pipeline.geocoding.cache import (
    SupabaseQueryCache, candidate_to_dict, dict_to_candidate,
)
from farmuncle_pipeline.geocoding.market_terminology import market_terms_for_state
from farmuncle_pipeline.geocoding.name_normalizer import extract_place_name
from farmuncle_pipeline.geocoding.providers import (
    GooglePlacesProvider, OSMNominatimProvider, DistrictReference, haversine_km,
)
from farmuncle_pipeline.geocoding.scorer import (
    score_candidate, cross_check_agrees, classify_confidence,
    WEIGHT_CROSS_CHECK_AGREEMENT,
)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
GOOGLE_API_KEY = os.environ["GOOGLE_MAPS_API_KEY"]

DRY_RUN = os.environ.get("DRY_RUN", "true").strip().lower() != "false"
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "300"))

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
cache = SupabaseQueryCache(supabase)
places = GooglePlacesProvider(GOOGLE_API_KEY)
osm = OSMNominatimProvider()
district_ref = DistrictReference(GOOGLE_API_KEY)

AUDIT_LOG = "mandi_geocoding_v2_audit.csv"
NOT_FOUND_LOG = "mandis_needing_manual_geocoding_v2.csv"


def cached_search(provider_name, provider_obj, query, is_places: bool):
    hit = cache.get(provider_name, query)
    if hit is not None:
        return [dict_to_candidate(d) for d in hit], {"status": "CACHE_HIT"}
    if is_places:
        candidates, diag = provider_obj.search(query)
    else:
        candidates, diag = provider_obj.search(query)
    cache.set(provider_name, query, [candidate_to_dict(c) for c in candidates])
    return candidates, diag


def build_places_queries(name, district, state):
    terms = market_terms_for_state(state)
    loc = ", ".join(p for p in [district, state, "India"] if p)
    queries = [f"{name} {terms[0]}, {loc}"]
    if len(terms) > 1:
        queries.append(f"{name} {terms[1]}, {loc}")
    return queries[:2]  # hard cap: 2 Places calls per mandi, cost control


def build_osm_queries(name, district, state):
    terms = market_terms_for_state(state)
    loc = ", ".join(p for p in [district, state, "India"] if p)
    queries = [f"{name}, {loc}"]
    for t in terms[:3]:
        queries.append(f"{name} {t}, {loc}")
    place_name = extract_place_name(name)
    if place_name.lower() != name.lower():
        queries.append(f"{place_name} market, {loc}")
        # Bare place-name query, no market suffix at all -- this is the
        # genuine last-resort fallback. It will often only surface an
        # admin-only (locality) result, which the scorer correctly
        # hard-rejects -- but if OSM happens to have the market itself
        # tagged as a node near the village, a plain place-name search
        # sometimes finds it when a suffixed query doesn't (Nominatim's
        # matching is stricter about extra tokens than Places' is).
        queries.append(f"{place_name}, {loc}")
    return queries


def best_from_provider(provider_name, candidates, mandi_name, district, state, ref_point):
    """Scores every candidate from one provider's result set, returns
    the best-scoring one (or None if all hard-rejected / empty)."""
    best = None
    best_result = None
    for c in candidates:
        dist_km = None
        if ref_point and c.lat is not None:
            dist_km = haversine_km(ref_point[0], ref_point[1], c.lat, c.lng)
        result = score_candidate(c, mandi_name, district, state, distance_km=dist_km)
        if result["hard_reject"]:
            continue
        if best is None or result["score"] > best["score"]:
            best, best_result = c, result
    return best, best_result


def geocode_mandi(name, district, state):
    """
    Returns (lat, lng, confidence, best_candidate_name, score, reasons,
    audit_trail) -- audit_trail is a list of per-query dicts for the CSV.
    """
    audit_trail = []
    ref_point = district_ref.get(district, state)

    google_candidates = []
    for q in build_places_queries(name, district, state):
        results, diag = cached_search("google_places", places, q, is_places=True)
        google_candidates.extend(results)
        audit_trail.append({"provider": "google_places", "query": q,
                             "status": diag.get("status"), "n_results": len(results)})
        if results:
            break  # first query that returns anything is enough to score from

    osm_candidates = []
    for q in build_osm_queries(name, district, state):
        results, diag = cached_search("osm_nominatim", osm, q, is_places=False)
        osm_candidates.extend(results)
        audit_trail.append({"provider": "osm_nominatim", "query": q,
                             "status": diag.get("status"), "n_results": len(results)})
        if results:
            break

    best_google, google_result = best_from_provider(
        "google_places", google_candidates, name, district, state, ref_point)
    best_osm, osm_result = best_from_provider(
        "osm_nominatim", osm_candidates, name, district, state, ref_point)

    agree = cross_check_agrees(best_google, best_osm)

    # Prefer Google's best if it exists (Places is the higher-trust
    # provider per the blueprint's provider priority), else fall back
    # to OSM's best. Apply the cross-check bonus to whichever is chosen
    # if the other provider corroborates it.
    if best_google is not None:
        chosen, chosen_result = best_google, google_result
    elif best_osm is not None:
        chosen, chosen_result = best_osm, osm_result
    else:
        return None, None, None, None, 0, ["no candidate survived scoring"], audit_trail

    final_score = chosen_result["score"]
    reasons = list(chosen_result["reasons"])
    if agree:
        final_score += WEIGHT_CROSS_CHECK_AGREEMENT
        reasons.append(f"cross-check: google/osm agree within {3.0}km (+{WEIGHT_CROSS_CHECK_AGREEMENT})")

    confidence = classify_confidence(final_score, agree)
    if confidence == "UNRESOLVED":
        return None, None, None, None, final_score, reasons, audit_trail

    return chosen.lat, chosen.lng, confidence, chosen.name, final_score, reasons, audit_trail


def run():
    print(f"Loading ACTIVE mandis with no coordinates (batch_size={BATCH_SIZE}, "
          f"DRY_RUN={DRY_RUN})...")
    limit = os.environ.get("GEOCODE_LIMIT")

    query = (
        supabase.table("mandis")
        .select("id, name, district, state")
        .is_("latitude", "null")
        .eq("status", "ACTIVE")
        .limit(BATCH_SIZE)
    )
    mandis = query.execute().data
    if limit:
        mandis = mandis[: int(limit)]

    total = len(mandis)
    print(f"Found {total} mandis to geocode (Places API v2, dry_run={DRY_RUN})")

    stats = {"HIGH": 0, "MEDIUM": 0, "LOW": 0, "REVIEW": 0, "unresolved": 0}
    not_found_rows = []
    audit_rows = []

    for i, mandi in enumerate(mandis):
        label = f"{mandi['name']}, {mandi['district']}, {mandi['state']}"
        print(f"  [{i+1}/{total}] {label}")

        lat, lng, confidence, matched_name, score, reasons, trail = geocode_mandi(
            mandi["name"], mandi["district"], mandi["state"]
        )
        found = lat is not None and lng is not None

        for t in trail:
            audit_rows.append({
                "mandi_id": mandi["id"], "mandi_name": mandi["name"],
                "district": mandi["district"], "state": mandi["state"],
                "provider": t["provider"], "query": t["query"],
                "status": t["status"], "n_results": t["n_results"],
                "final_confidence": confidence or "UNRESOLVED",
                "final_score": score, "matched_name": matched_name or "",
                "reasons": " | ".join(reasons) if reasons else "",
                "would_write": found and not DRY_RUN,
                "dry_run": DRY_RUN,
            })

        if found:
            print(f"    -> {matched_name!r} @ {lat},{lng} [{confidence}, score={score}]")
            stats[confidence] += 1
            if not DRY_RUN:
                supabase.rpc("update_mandi_location", {
                    "p_mandi_id": mandi["id"],
                    "p_latitude": lat,
                    "p_longitude": lng,
                    "p_location_confidence": confidence,
                    "p_source": "geocoding_v2_places_api",
                }).execute()
        else:
            print(f"    -> no confident market match (score={score}), left blank for manual review")
            stats["unresolved"] += 1
            not_found_rows.append(mandi)

    print("\nDone!")
    for band in ("HIGH", "MEDIUM", "LOW", "REVIEW"):
        print(f"  {band}: {stats[band]}")
    print(f"  Unresolved (left blank): {stats['unresolved']}")
    if DRY_RUN:
        print("\n  DRY RUN -- nothing was written to Supabase. Re-run with DRY_RUN=false to write.")

    if not_found_rows:
        with open(NOT_FOUND_LOG, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["id", "name", "district", "state"])
            w.writeheader()
            w.writerows(not_found_rows)
        print(f"  List of unresolved mandis: {NOT_FOUND_LOG}")

    if audit_rows:
        with open(AUDIT_LOG, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(audit_rows[0].keys()))
            w.writeheader()
            w.writerows(audit_rows)
        print(f"  Full audit trail: {AUDIT_LOG} -- spot-check 'matched_name' against "
              f"'mandi_name' before trusting a non-dry-run write.")


if __name__ == "__main__":
    run()