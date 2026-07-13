"""
FarmUncle v2 — raw_dedup.py

Purpose:
    Content-addressed, deduplicated storage for individual raw price
    entries (as opposed to whole fetched pages). Replaces per-page
    raw_api_records writes in live_tick.py / resource2_pipeline.py /
    retry_failed_pages.py with a per-record dedup upsert: unchanged
    content across successive fetches is stored once and pointed at
    again, never duplicated.

Why this exists:
    raw_api_records stores one full page (~500 records) per fetch,
    forever, with no dedup -- the dominant storage cost in the system
    (16 MB / 863 rows vs <1 MB for everything else combined, as of
    2026-07-13). Most individual records are unchanged between
    consecutive 3-hourly live_tick runs. This module stores each
    individual record's content exactly once per distinct value it
    has ever taken, and just updates a "last seen in batch X" pointer
    on repeats.

Invariants preserved:
    - Nothing is ever edited or deleted (invariant 1) -- an unchanged
      record's existing row is only touched on last_seen_batch_id /
      last_seen_at, never on its actual payload.
    - Every row is replayable back to the batch that (re)observed it
      (invariant 10) via first_seen_batch_id / last_seen_batch_id.

Corresponding Supabase objects (migration: raw_price_entries_dedup):
    - table `raw_price_entries`
    - function `upsert_raw_price_entry(...)` (the atomic insert-or-touch)
"""

from __future__ import annotations

import hashlib
import json


def _content_hash(payload: dict) -> str:
    """Stable hash of the fields that constitute this record's 'content'."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def upsert_raw_price_entry(
    supabase,
    *,
    resource: str,
    market: str,
    state: str,
    district: str | None,
    commodity: str,
    raw_variety: str,
    price_date: str,  # ISO 'YYYY-MM-DD' -- matches parse_agmarknet_record's output
    modal_price,
    min_price,
    max_price,
    batch_id: str,
    parser_version: int,
) -> tuple[int, bool]:
    """
    Insert-or-touch a single raw price observation.

    Inputs:
        resource: "resource_1" or "resource_2" (pass Resource.X.value,
            not the enum member itself).
        market/state/district/commodity/raw_variety/price_date/
            modal_price/min_price/max_price: the dict returned by
            `parse_agmarknet_record`, unpacked.
        batch_id: the current run's `raw_api_batches.id`
            (`raw_batch_id` in the calling scripts) -- NOT
            `ingestion_batches.id`.
        parser_version: `PARSER_VERSION` from `ingest_common`, same as
            the old `insert_raw_api_record` calls used.

    Outputs:
        (entry_id, is_new_content) -- is_new_content is True only when
        this exact (key, content) combination has never been stored
        before; False means an existing row's last_seen pointer was
        updated and no new content was written.

    Failure modes:
        Raises whatever the Supabase client raises on RPC failure --
        deliberately not swallowed, since a raw-write failure here is
        exactly the kind of thing invariant 1 needs to surface, not hide.
    """
    payload = {
        "modal_price": modal_price,
        "min_price": min_price,
        "max_price": max_price,
    }
    content_hash = _content_hash(payload)

    result = supabase.rpc(
        "upsert_raw_price_entry",
        {
            "p_resource": resource,
            "p_market": market,
            "p_state": state,
            "p_district": district or "",
            "p_commodity": commodity,
            "p_raw_variety": raw_variety or "",
            "p_price_date": price_date,
            "p_content_hash": content_hash,
            "p_payload": payload,
            "p_batch_id": batch_id,
            "p_parser_version": parser_version,
        },
    ).execute()

    row = result.data[0]
    return row["entry_id"], row["is_new"]
