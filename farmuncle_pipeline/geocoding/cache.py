"""
Location search cache -- blueprint §20.

Backed by a new table `mandi_location_search_cache` (migration in
supabase/migrations/, not created here). Purpose: during dev/testing
you will re-run the same 10-50 mandis repeatedly while tuning the
scorer -- without a cache, every re-run re-spends Places API credits on
identical queries. Cache entries never expire automatically in this
first version (no TTL logic) -- provider results for a physical
market's location don't go stale on any timescale that matters here;
clear the table manually if a provider's data for a query ever needs
to be force-refreshed.
"""

import json


class SupabaseQueryCache:
    def __init__(self, supabase_client):
        self.supabase = supabase_client

    def get(self, provider: str, query: str):
        """Returns the cached candidate list (as plain dicts, not
        LocationCandidate objects -- caller reconstructs) or None on a
        cache miss. Never raises -- a cache read failure is treated as
        a miss, not fatal, since the cache is a cost optimization, not
        a correctness dependency."""
        try:
            res = (
                self.supabase.table("mandi_location_search_cache")
                .select("response")
                .eq("provider", provider)
                .eq("query", query)
                .limit(1)
                .execute()
            )
            if res.data:
                return json.loads(res.data[0]["response"])
        except Exception:
            pass
        return None

    def set(self, provider: str, query: str, candidates_as_dicts):
        try:
            self.supabase.table("mandi_location_search_cache").upsert({
                "provider": provider,
                "query": query,
                "response": json.dumps(candidates_as_dicts),
            }, on_conflict="provider,query").execute()
        except Exception:
            pass  # cache write failure should never break a geocoding run


def candidate_to_dict(c):
    return {
        "name": c.name, "lat": c.lat, "lng": c.lng,
        "place_types": sorted(c.place_types),
        "formatted_address": c.formatted_address, "provider": c.provider,
    }


def dict_to_candidate(d, provider_cls=None):
    from .providers import LocationCandidate
    return LocationCandidate(
        name=d["name"], lat=d["lat"], lng=d["lng"],
        place_types=d["place_types"], formatted_address=d["formatted_address"],
        provider=d["provider"],
    )
