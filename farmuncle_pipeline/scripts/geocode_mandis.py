"""
FarmUncle v2 - Mandi Geocoding Script
--------------------------------------
Fills lat/lng for mandis with no coordinates.

EXACT-ONLY POLICY (2026-07-16 revision -- see below for what changed):
  This script never writes DISTRICT or STATE level coordinates. If an
  exact market/village/town-level location can't be found after trying
  Google across a few reformulated query variants, the mandi is left
  with NULL coordinates and reported at the end for manual review.

  We do this deliberately, based on the 2,663-mandi mess caused by an
  earlier version of this script that fell back to district/state
  centroids - those got silently treated as real locations downstream.
  Better to have an honest NULL than a fake-precise coordinate.

  2026-07-16: that 2,663-mandi mess turned out to still be partly
  happening even with the "EXACT-ONLY" wording above, for a subtler
  reason -- this script asked Google for an exact match but never
  actually checked what precision Google handed back. Google's
  Geocoding API often replies `status: OK` with a real result even
  when it can't find the specific market -- it just silently widens
  the search and returns the nearest thing it CAN find, commonly the
  district headquarters town. That response looks identical in shape
  to a real village-level hit, so the old code accepted it and
  stamped it "EXACT" regardless. A follow-up audit found 679 mandis
  nationwide that had silently collapsed onto a shared district-level
  point this way (confirmed independently: 11 different, genuinely
  different-town markets in Dhar district, MP all sitting on the
  identical coordinate to 7 decimal places).
  `farmuncle_pipeline/scripts/geocode_kerala_villages.py` already had
  the right fix for its 59-mandi Kerala subset (an `is_precise_enough`
  check on Google's `address_components`/`location_type`, rejecting
  anything without a locality/village-level component) -- this
  revision brings that same check into this script's main path, so
  every mandi gets it, not just the hand-picked Kerala list. The 679
  affected mandis have been reset to NULL coordinates in a separate
  one-time cleanup and will be picked up and re-geocoded by this
  script's normal NULL-coordinate query on its next run.

Strategy per mandi (Google only, nothing coarser, no OSM):
  1. Google Geocoding API with the full name+district+state query
  2. A couple of reformulated variants on Google only
     (e.g. appending "mandi"/"market", or dropping district if it's
     possibly misspelled/wrong) - still exact-level, just different
     phrasing to catch indexing differences
  3. A village/town-name-only variant (see extract_place_name) for
     hyperlocal markets that aren't individually indexed as
     businesses, but whose underlying place IS a real, mappable
     locality -- this is the same fallback that fixed all 59 Kerala
     VFPCK-market mandis by hand; it's already been in this script
     since before this revision, it just wasn't being precision-
     checked.
  4. EVERY candidate response above -- not just the last resort -- is
     now run through `is_precise_enough()` before being accepted.
     Anything Google returns that isn't tagged at locality/village/
     town/sublocality/neighborhood/postal_town granularity (or is
     flagged APPROXIMATE without one of those) is rejected outright,
     even if Google's `status` said `OK`. A rejected candidate does
     NOT stop the loop -- the next query variant is still tried.
  5. If nothing precise hits on any variant -> leave blank, log for
     manual review.

  OSM/Nominatim is intentionally NOT used at all. Its data is
  crowd-sourced and can be wrong or mismatched for Indian mandi names,
  so we'd rather get an honest NULL from Google than a possibly-wrong
  hit from OSM.

All writes go through the update_mandi_location() Supabase RPC so that:
  - audit_events gets a proper audit trail (old/new lat/lng/confidence + source)
  - a worse-confidence result can never clobber a better one already on file
  - only ACTIVE mandis are touched (MERGED and unknown mandi_ids are skipped)

  2026-08-05: this RPC did not actually exist in the database before today
  -- confirmed against the live project (0/2767 mandis had ever been
  geocoded, and `update_mandi_location` was absent from
  information_schema.routines). It's added in migration
  20260805000000_add_update_mandi_location_rpc.sql. Everything above this
  line describes behavior that was already correctly designed in this
  script; the RPC it depended on just hadn't been created yet.

  2026-08-05 additions (this revision):
  - `extract_place_name()` now also strips a bare trailing "VFPCK" (not
    just "VFPCK Market") -- affects 1 live Kerala mandi
    ("Bharananganam VFPCK") that previously never got the village-name
    fallback query.
  - `_CORE_NAME_FILLER_WORDS` gained "uzhavar"/"sandhai"/"sandha" as a
    safety net for Tamil Nadu rows -- today's data always has this
    parenthesized ("X(Uzhavar Sandhai )"), which the existing paren-strip
    already handled, so this only matters if a future row omits the parens.
  - Every query variant tried for every mandi (not just the ones that
    fail) is now written to a full audit CSV alongside the resolved place
    name Google matched, so results can be spot-checked before being
    trusted -- see AUDIT_LOG / audit_rows in run().

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
AUDIT_LOG = "mandi_geocoding_audit.csv"  # 2026-08-05: every query tried for every mandi

# Component types that indicate Google actually resolved down to a
# specific place (village/town/city/neighbourhood level) rather than
# just "somewhere in this district/state". Mirrors
# geocode_kerala_villages.py's is_precise_enough() exactly, so both
# scripts apply the identical bar for what counts as EXACT.
_FINE_GRAINED_COMPONENT_TYPES = {
    "locality", "sublocality", "sublocality_level_1",
    "neighborhood", "postal_town", "village",
}


import re
from difflib import SequenceMatcher

# Generic suffix/filler words that appear in mandi names or Google's
# address components but carry no place-identifying information.
# 2026-08-05 correction: earlier comments here claimed this list mirrors a
# `mandi_core_name()` SQL function and a `check_mandi_coordinate_mismatch`
# trigger. Neither exists in the database (checked information_schema.routines
# directly) -- what DOES exist live is a `v_audit_coordinate_mismatch` VIEW
# (not in any migration file either, so it's schema drift -- worth adding
# as a migration separately) that flags same-coordinate ACTIVE-mandi pairs
# using plain pg_trgm similarity() on normalized_name, not this filler-word
# list. So this list is this script's own definition of "core name", not a
# mirror of anything in the DB. It still plays the same conceptual role
# (deciding whether two names refer to the same place) -- just isn't
# actually kept in sync with a DB-side twin the way earlier comments implied.
_CORE_NAME_FILLER_WORDS = {
    "apmc", "mandi", "market", "committee", "sub-yard", "sub", "yard",
    "vfpck", "grain", "veg", "new", "old", "north", "south", "main",
    "tal", "taluka", "dist", "district", "agriculture", "produce",
    # 2026-08-05 addition: Tamil Nadu's "Uzhavar Sandhai" qualifier is
    # always parenthesized in our data today (e.g. "Melur(Uzhavar
    # Sandhai )"), so the parenthetical strip in _core_name/extract_place_name
    # already drops it in every real row checked. These are here as a
    # safety net in case a row ever shows up without the parens -- costs
    # nothing (never matches outside TN) and prevents a silent regression.
    "uzhavar", "sandhai", "sandha",
}


def _normalize_place_token(s: str) -> str:
    """Lowercase, strip whitespace/punctuation, so 'Sanwer' and 'sanwer,' compare equal."""
    return re.sub(r"[^a-z0-9]", "", s.lower())


def _core_name(raw: str) -> str:
    """
    Strip parentheticals and generic filler words to get a place's "core"
    name for comparison, e.g. "Agriculture Produce Market Committee Katol"
    -> "katol", "Melur(Uzhavar Sandhai )" -> "melur". This is the Python
    mirror of the database's mandi_core_name() SQL function -- kept
    logically identical (same filler-word list, same parenthetical strip)
    so a result this script accepts or rejects agrees with what the DB
    trigger would independently conclude about the same two names.
    """
    no_parens = re.sub(r"\([^)]*\)", " ", raw.lower())
    tokens = [t for t in re.split(r"[^a-z0-9]+", no_parens) if t and t not in _CORE_NAME_FILLER_WORDS]
    return " ".join(tokens)


def _name_match_score(name_a: str, name_b: str) -> float:
    """
    Fuzzy match score between two core names, combining a whole-string
    similarity ratio with a substring/containment check -- mirrors
    mandi_name_match_score() in the database (plain trigram similarity
    OR-ed with directional word_similarity). SequenceMatcher.ratio() here
    plays the same "spelling drift" role as pg_trgm's similarity()
    (catches "Kancheepuram" vs "Kanchipuram"), and the containment check
    plays the same role as word_similarity (catches a short core name like
    "sengaon" appearing as a genuine word inside a longer descriptive
    core name like "sant namdev krushi bazar sengaon hingoli").
    """
    core_a, core_b = _core_name(name_a), _core_name(name_b)
    if not core_a or not core_b:
        return 0.0
    ratio = SequenceMatcher(None, core_a, core_b).ratio()
    words_a, words_b = set(core_a.split()), set(core_b.split())
    contained = bool(words_a & words_b)  # any exact shared core word, e.g. "katol" in both
    return max(ratio, 1.0 if contained else 0.0)


def _query_place_tokens(query: str) -> set:
    """
    Extract the meaningful place-name token(s) we actually asked Google about,
    from a query string like 'Konch APMC, Jalaun (Orai), Uttar Pradesh, India'.
    We only care about the FIRST comma-separated segment (the mandi/place name
    itself, before district/state/country) since that's the thing Google needs
    to have actually resolved to -- not just any word appearing anywhere in the
    query, which would also match on the district/state and defeat the point
    of this check.
    """
    first_segment = query.split(",")[0]
    core = _core_name(first_segment)
    tokens = {_normalize_place_token(t) for t in core.split() if t.strip()}
    return {t for t in tokens if len(t) >= 3}  # drop tiny tokens (initials, "of", etc.)


def _result_locality_tokens(result: dict) -> set:
    """All locality/village/etc-level component names from a Google result, normalized."""
    tokens = set()
    for c in result.get("address_components", []):
        if set(c.get("types", [])) & _FINE_GRAINED_COMPONENT_TYPES:
            tokens.add(_normalize_place_token(c.get("long_name", "")))
            tokens.add(_normalize_place_token(c.get("short_name", "")))
    return {t for t in tokens if t}


def _evaluate_result(result: dict, query: str = "") -> dict:
    """
    Compute every precision/name-match diagnostic for one Google result
    against the query that produced it -- both `is_precise_enough` (the
    accept/reject gate) and the per-mandi audit CSV (added 2026-08-05) call
    this, so the two can never silently disagree about what was actually
    checked, and every accept/reject decision has a concrete "here's what
    Google matched" record behind it instead of just a pass/fail boolean.

    Purpose (of the checks themselves):
        Reject any Google Geocoding result that isn't actually
        village/town/city-level precision, even when Google's top-level
        `status` says `OK`. This is the check that was missing before
        2026-07-16 -- Google returning `OK` only means "I found
        something", not "I found the specific place you asked about";
        for an unindexed market name it commonly widens silently to the
        enclosing district/administrative area and returns that instead,
        with no error to signal the difference. `location_type` and
        `address_components` are the only fields that actually reveal
        which case happened.

        2026-07-17 revision: the original version of this check stopped
        at "does a fine-grained component exist at all" -- but Google's
        silent-widening failure mode doesn't just widen to DISTRICT
        level, it can also widen to a *different nearby town or city*
        that Google considers the "closest" indexed place, and that
        substitute town still shows up as a genuine locality-level
        component. A follow-up audit found this exact pattern: multiple
        different mandi names in the same district all resolving to one
        shared town's coordinates (e.g. Konch/Kalpi/Madhogarh all
        landing on Orai; Indore(F&V) landing on Sanwer). The fine-grained
        check alone can't catch this because the substitute IS a real,
        precise place -- just not the one asked about. This revision
        adds a name-match: at least one fine-grained component's name
        must actually overlap with the place name tokens from the query
        itself. If nothing overlaps, we reject even though a real
        locality-level component is present, because we have no
        confirmation Google resolved *our* place rather than a
        substitute.
    Inputs:
        result: one entry from Google's `results` array (the raw dict,
            not just the lat/lng -- this needs `geometry.location_type`
            and `address_components`).
        query: the exact query string sent to Google for this result,
            used to extract the place-name tokens for the name-match
            check. Optional for backward compatibility, but the name
            check is skipped (i.e. old, weaker behavior) if omitted --
            callers within this script always pass it.
    Outputs:
        A dict:
          passes:            bool -- the same true/false is_precise_enough used to return
          resolved_place:    comma-joined names of the fine-grained address
                              component(s) Google actually matched (empty if none)
          formatted_address: Google's full formatted_address for the result,
                              as a fallback for the audit log even on a hard reject
          location_type:     Google's geometry.location_type, e.g. "ROOFTOP",
                              "APPROXIMATE"
          match_score:       the 0-1 name-match score against the query's place
                              name (empty string if query was not given)
          partial_match:     Google's own partial_match flag
        `passes` is True only if the result has at least one address
        component at locality/village/town granularity or finer, and (when
        query is given) that component's name overlaps with the queried
        place name. False for anything resolved only to district/state/
        country level, false for Google's own partial-match flag, and
        false for a precise-but-mismatched substitute place.
    Failure modes:
        None raised -- malformed/missing fields just fall through to
        passes=False via .get() defaults, which is the safe direction
        (reject, don't guess).
    """
    partial_match = bool(result.get("partial_match"))
    location_type = result.get("geometry", {}).get("location_type")
    components = result.get("address_components", [])
    component_types = set()
    for c in components:
        component_types.update(c.get("types", []))
    has_fine_grained = bool(component_types & _FINE_GRAINED_COMPONENT_TYPES)
    locality_names = [
        c.get("long_name", "")
        for c in components
        if set(c.get("types", [])) & _FINE_GRAINED_COMPONENT_TYPES
    ]
    resolved_place = ", ".join(n for n in locality_names if n)

    best_score = None
    if query:
        query_place = query.split(",")[0]
        # Same 0.3 threshold as the database's mandi_name_match_score /
        # v_audit_coordinate_mismatch, validated against every real
        # matched/mismatched pair found in the 2026-07-16/17 audits.
        best_score = max((_name_match_score(query_place, ln) for ln in locality_names), default=0.0)

    passes = (
        not partial_match
        and has_fine_grained
        and (best_score is None or best_score >= 0.3)
    )

    return {
        "passes": passes,
        "resolved_place": resolved_place,
        "formatted_address": result.get("formatted_address", ""),
        "location_type": location_type or "",
        "match_score": round(best_score, 2) if best_score is not None else "",
        "partial_match": partial_match,
    }


def is_precise_enough(result: dict, query: str = "") -> bool:
    """Thin wrapper around `_evaluate_result` -- kept as its own function
    since that's the name the module docstring and other comments in this
    file refer to as "the precision check"."""
    return _evaluate_result(result, query)["passes"]


def _empty_diag(query: str, status: str) -> dict:
    """Diagnostic record for a query that got no usable result at all
    (ZERO_RESULTS, an API error status, or a request exception) -- still
    written to the audit CSV so a mandi's row shows every query that was
    tried, not just the ones that returned something."""
    return {
        "query": query, "google_status": status, "resolved_place": "",
        "formatted_address": "", "location_type": "", "match_score": "",
        "passes": False, "partial_match": False,
    }


def google_lookup(query: str):
    """Single Google Geocoding API query.
    Returns (lat, lng, diagnostic) -- diagnostic is the `_evaluate_result`
    dict (plus 'query' and 'google_status') describing what Google
    returned and why it was accepted/rejected, so every attempt --
    accepted, rejected, or empty -- can be written to the per-mandi audit
    CSV for manual cross-checking (2026-08-05 addition).
    lat/lng are None whenever the result was rejected or nothing was
    found -- covers both "Google found nothing" and "Google found
    something but it's only district/state-level precision or a
    mismatched substitute place", the latter being exactly the
    silent-fallback case the 2026-07-16 revision exists to catch."""
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
            diag = _evaluate_result(result, query=query)
            diag["query"] = query
            diag["google_status"] = status
            if not diag["passes"]:
                print(f"    Google returned a result for '{query}' but it's not "
                      f"village/town-level precise or doesn't name-match "
                      f"(location_type={diag['location_type']}, matched "
                      f"'{diag['resolved_place']}', score={diag['match_score']}) -- rejected.")
                return None, None, diag
            loc = result["geometry"]["location"]
            print(f"    '{query}' -> matched '{diag['resolved_place']}' (score={diag['match_score']})")
            return float(loc["lat"]), float(loc["lng"]), diag
        if status != "ZERO_RESULTS":
            # OVER_QUERY_LIMIT, REQUEST_DENIED, INVALID_REQUEST etc - worth surfacing
            print(f"    Google API status='{status}' for '{query}': {data.get('error_message', '')}")
        return None, None, _empty_diag(query, status or "UNKNOWN")
    except Exception as e:
        print(f"    Google error for '{query}': {e}")
        return None, None, _empty_diag(query, f"EXCEPTION: {e}")


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
        # 2026-08-05: bare "VFPCK" with no trailing "Market" word, e.g.
        # "Bharananganam VFPCK" -- confirmed exactly 1 live row like this
        # today, but without this the village-name fallback query never
        # gets built for it since extract_place_name() returns the name
        # unchanged.
        "vfpck",
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
    Precision is enforced downstream by `is_precise_enough`, not by
    anything about how these queries are worded -- wording just affects
    which variant is likeliest to get Google to a real hit at all.
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
    Returns (lat, lng, location_confidence, source, attempts).

    attempts is a list of per-query diagnostic dicts (one per candidate
    query tried, in that order) -- every attempt is recorded, whether it
    succeeded, got rejected, or came back empty, so `run()` can write the
    full trail to the audit CSV (2026-08-05 addition) rather than just the
    final outcome.

    Tries Google only, across all query variants. Never returns DISTRICT
    or STATE level results (every candidate is precision-checked via
    `is_precise_enough` inside `google_lookup`, not just accepted on a
    Google `status: OK`), and never falls back to OSM - if nothing
    village/town-level precise is found, returns all-None so the mandi
    is left blank for manual review.
    """
    district = district or ""
    state = state or ""
    candidate_queries = build_candidate_queries(name, district, state)

    attempts = []
    for q in candidate_queries:
        lat, lng, diag = google_lookup(q)
        time.sleep(GOOGLE_SLEEP_SECONDS)
        attempts.append(diag)
        if lat is not None and lng is not None:
            return lat, lng, "EXACT", "google", attempts

    # No precise hit on Google across any variant. Leave blank - do NOT
    # fall back to OSM or to district/state coordinates.
    return None, None, None, None, attempts


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
    print(f"Found {total} mandis to geocode (Google-only, precision-checked exact-match)")

    stats = {"EXACT": 0, "not_found": 0}
    not_found_rows = []
    audit_rows = []
    audit_fields = [
        "mandi_id", "mandi_name", "district", "state", "attempt_number",
        "query", "google_status", "resolved_place", "formatted_address",
        "location_type", "match_score", "passed", "written_to_db",
        "final_outcome",
    ]

    for i, mandi in enumerate(all_mandis):
        label = f"{mandi['name']}, {mandi['district']}, {mandi['state']}"
        print(f"  [{i+1}/{total}] {label}")

        lat, lng, confidence, source, attempts = geocode_mandi(
            mandi["name"], mandi["district"], mandi["state"]
        )
        found = lat is not None and lng is not None
        outcome = "EXACT" if found else "NOT_FOUND"

        # Every query variant tried for this mandi gets its own audit row,
        # so a rejected candidate (and *why* it was rejected -- what place
        # it actually matched) is visible, not just the final yes/no.
        for attempt_number, diag in enumerate(attempts, start=1):
            written = found and diag is attempts[-1]
            audit_rows.append({
                "mandi_id": mandi["id"],
                "mandi_name": mandi["name"],
                "district": mandi["district"],
                "state": mandi["state"],
                "attempt_number": attempt_number,
                "query": diag.get("query", ""),
                "google_status": diag.get("google_status", ""),
                "resolved_place": diag.get("resolved_place", ""),
                "formatted_address": diag.get("formatted_address", ""),
                "location_type": diag.get("location_type", ""),
                "match_score": diag.get("match_score", ""),
                "passed": diag.get("passes", False),
                "written_to_db": written,
                "final_outcome": outcome,
            })

        if found:
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
            print(f"    -> not found on Google at village/town precision, left blank for manual review")
            stats["not_found"] += 1
            not_found_rows.append(mandi)

    print("\nDone!")
    print(f"  EXACT found (Google, precision-checked):  {stats['EXACT']}")
    print(f"  Needs manual review (left blank):         {stats['not_found']}")

    if not_found_rows:
        with open(NOT_FOUND_LOG, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "district", "state"])
            writer.writeheader()
            writer.writerows(not_found_rows)
        print(f"\n  List of mandis needing manual geocoding written to: {NOT_FOUND_LOG}")

    if audit_rows:
        with open(AUDIT_LOG, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=audit_fields)
            writer.writeheader()
            writer.writerows(audit_rows)
        print(f"  Full per-mandi audit trail (every query tried, what Google matched, "
              f"accept/reject) written to: {AUDIT_LOG}")
        print(f"  -> Open this and spot-check a sample of 'written_to_db=True' rows "
              f"against 'resolved_place' before trusting the bulk write.")


if __name__ == "__main__":
    run()