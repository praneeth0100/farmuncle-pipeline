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
    raw_grade: str,
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
        market/state/district/commodity/raw_variety/raw_grade/
            price_date/modal_price/min_price/max_price: the dict
            returned by `parse_agmarknet_record`, unpacked. `raw_grade`
            added 2026-07-22 -- may be an empty string (government
            didn't report one for this record), matching the live RPC's
            `p_raw_grade` and the `uq_raw_price_entries_identity`
            index's `COALESCE(grade, '')` treatment of it.
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
            "p_raw_grade": raw_grade or "",
            "p_price_date": price_date,
            "p_content_hash": content_hash,
            "p_payload": payload,
            "p_batch_id": batch_id,
            "p_parser_version": parser_version,
        },
    ).execute()

    row = result.data[0]
    return row["entry_id"], row["is_new"]


def upsert_raw_price_entries_batch(
    supabase,
    *,
    resource: str,
    batch_id: str,
    parser_version: int,
    parsed_records: list[dict],
) -> tuple[int, int]:
    """
    Insert-or-touch an entire page's worth of raw price observations in
    a single round-trip, instead of one RPC call per record.

    Root-cause fix (2026-07-13 incident): the original per-record
    `upsert_raw_price_entry` loop in live_tick.py /
    resource2_pipeline.py / retry_failed_pages.py made ~500 sequential
    RPC round-trips per page (~280ms each => ~2m20s/page), which blew
    GitHub Actions' 15-minute job timeout on any run needing more than
    ~6 pages, SIGKILLing the process mid-run and leaving a stale
    RUNNING row in `ingestion_batches` that then blocked every
    subsequent run via the §12 concurrency guard. This function
    batches the whole page into one call to the
    `upsert_raw_price_entries_batch` Postgres RPC (one
    INSERT ... ON CONFLICT DO UPDATE statement), cutting a page's raw
    writes from N round-trips to 1.

    Inputs:
        resource: "resource_1" or "resource_2" (pass Resource.X.value).
        batch_id: the current run's `raw_api_batches.id`
            (`raw_batch_id` in the calling scripts).
        parser_version: `PARSER_VERSION` from `ingest_common`.
        parsed_records: a list of dicts, each shaped like
            `parse_agmarknet_record`'s return value (market, state,
            district, commodity, raw_variety, raw_grade, price_date,
            modal_price, min_price, max_price). Callers should skip
            `None` results from `parse_agmarknet_record` before
            building this list. If empty, this function is a no-op and
            makes no RPC call. NOTE (2026-07-22): the live
            `upsert_raw_price_entries_batch` RPC reads this field as
            `entry->>'grade'`, not `'raw_grade'` -- inconsistent with
            the single-entry RPC's `p_raw_grade` parameter name, but
            that inconsistency exists in the deployed RPC itself; this
            function's entry-building below matches it exactly rather
            than silently working around it.

    Outputs:
        (rows_written, rows_new) -- rows_written is the number of
        distinct (key, content) combinations in this page after
        within-page deduplication (a page can legitimately contain the
        same record twice); rows_new is how many of those were never
        seen before across any batch (is_new=True).

    Failure modes:
        Raises whatever the Supabase client raises on RPC failure --
        same policy as `upsert_raw_price_entry`, not swallowed.
    """
    if not parsed_records:
        return 0, 0

    entries = []
    for parsed in parsed_records:
        payload = {
            "modal_price": parsed["modal_price"],
            "min_price": parsed["min_price"],
            "max_price": parsed["max_price"],
        }
        entries.append(
            {
                "resource": resource,
                "market": parsed["market"],
                "state": parsed["state"],
                "district": parsed["district"] or "",
                "commodity": parsed["commodity"],
                "raw_variety": parsed["raw_variety"] or "",
                "grade": parsed.get("raw_grade") or "",
                "price_date": parsed["price_date"],
                "content_hash": _content_hash(payload),
                "payload": payload,
                "batch_id": batch_id,
                "parser_version": parser_version,
            }
        )

    result = supabase.rpc(
        "upsert_raw_price_entries_batch",
        {"p_entries": entries},
    ).execute()

    rows = result.data or []
    rows_new = sum(1 for r in rows if r["is_new"])
    return len(rows), rows_new
