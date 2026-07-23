"""
FarmUncle v2 — price_cache.py
Phase C follow-up (RLS/price_cache gap closure).

Purpose (module-level):
    Thin wrapper around the `refresh_price_cache` RPC (§15 Cache
    Invalidation Policy: refresh after every `daily_rewrite`, after
    `live_tick`, and immediately after `merge_entity`). `price_cache`
    itself is disposable and always rebuildable (§6.6/§6.7) — a failed
    refresh here is never a reason to fail the calling ingestion run's
    own batch status. The next successful call (next scheduled run)
    catches up; the cache is at worst one run stale, never wrong.

Explicitly out of scope for this file:
    - Any logic about what belongs in price_cache (lives entirely in
      the `refresh_price_cache` RPC, server-side, per invariant 3).
    - Deciding *when* to refresh — callers (`daily_rewrite.py`,
      `live_tick.py`) decide that per §15.
"""

from __future__ import annotations

from supabase import Client


def refresh_price_cache(supabase: Client, *, caller: str) -> bool:
    """
    Purpose:
        Call the `refresh_price_cache` RPC and log the outcome.
    Inputs:
        supabase: an authenticated Supabase client (service role —
            price_cache has no anon/authenticated write policy, so
            this must run with a role that bypasses RLS).
        caller: name of the invoking script, for log clarity
            (e.g. "daily_rewrite", "live_tick").
    Outputs:
        True if the refresh succeeded, False otherwise.
    Failure modes:
        Any exception from the RPC call is caught and logged, never
        raised — see module docstring for why this must not fail
        the calling run.
    """
    try:
        supabase.rpc("refresh_price_cache", {}).execute()
        print(f"[{caller}] price_cache refreshed")
        return True
    except Exception as exc:
        print(f"[{caller}] WARNING: price_cache refresh failed (non-fatal): {exc}")
        return False
