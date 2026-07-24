"""
FarmUncle v2 — tests/test_live_schema_contract.py

Why this file exists (2026-07-24 audit, item 2):
    Every other test in this suite runs against `tests/fakes.py`'s
    in-memory fake client. That's correct for testing our own Python
    control flow, but it is structurally blind to the exact class of
    bug that took the pipeline down on 2026-07-23/24: the deployed
    Postgres RPC's parameter type (`p_batch_id bigint`) silently
    drifting out of sync with what the application code has always
    sent (a ULID string). 92 fake-client tests passed the entire time
    that bug was live in production.

    This file is the one place in the suite that talks to a *real*
    Supabase project and calls the *real* `upsert_raw_price_entries_batch`
    RPC through `raw_dedup.upsert_raw_price_entries_batch` -- the exact
    function `live_tick.py` calls -- to prove the deployed schema and
    the deployed code still agree on the contract between them.

Opt-in only, by design:
    This test needs real credentials for a real project and it writes
    a real (though clearly tagged) row. It must never run as part of
    the normal `pytest` invocation used in CI or in
    `test_*` files that use fakes, and it must never be pointed at the
    production `agrouncle 2` project by accident. It is skipped unless
    ALL of the following env vars are set:
        LIVE_SCHEMA_TEST_SUPABASE_URL
        LIVE_SCHEMA_TEST_SUPABASE_SERVICE_KEY
    (deliberately NOT named SUPABASE_URL / SUPABASE_SERVICE_KEY, so
    sourcing a normal `.env` for local dev never accidentally enables
    this test.)

Run it manually, e.g. against a Supabase branch or the real project,
with:
    LIVE_SCHEMA_TEST_SUPABASE_URL=https://xxxx.supabase.co \\
    LIVE_SCHEMA_TEST_SUPABASE_SERVICE_KEY=eyJ... \\
    pytest tests/test_live_schema_contract.py -v -s

Cleanup:
    `raw_price_entries` is immutable by design (trigger-enforced,
    2026-07-24 migration) -- this test does NOT attempt to delete the
    row it writes, because doing so would require disabling that
    trigger, which defeats the point of it existing. Instead the test
    row is tagged with a market name no real government data will ever
    produce (`__SCHEMA_CONTRACT_TEST__`), so it's trivially
    identifiable and harmless to leave in place. If you want it gone
    anyway, it's one row; delete it manually via the SQL editor after
    temporarily disabling `trg_raw_price_entries_no_delete`.
"""

from __future__ import annotations

import os
import sys
import time
import secrets
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from farmuncle_pipeline.core.raw_dedup import upsert_raw_price_entries_batch

_URL_VAR = "LIVE_SCHEMA_TEST_SUPABASE_URL"
_KEY_VAR = "LIVE_SCHEMA_TEST_SUPABASE_SERVICE_KEY"

_missing_env = not (os.environ.get(_URL_VAR) and os.environ.get(_KEY_VAR))

pytestmark = pytest.mark.skipif(
    _missing_env,
    reason=(
        f"Opt-in live-DB test: set {_URL_VAR} and {_KEY_VAR} to run it. "
        "Skipped by default so normal `pytest` runs never touch a real project."
    ),
)


_CROCKFORD_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _fake_ulid() -> str:
    """
    A 26-char Crockford-base32 string shaped exactly like the real
    ULIDs `batch_lifecycle.start_raw_batch()` generates in production
    (10 chars of timestamp + 16 chars of randomness). Not a real ULID
    library encoding (no monotonicity guarantees) -- this test only
    needs something the RPC will accept as `text` and that looks like
    what production actually sends, not a spec-perfect ULID.
    """
    ts_ms = int(time.time() * 1000)
    chars = []
    n = ts_ms
    for _ in range(10):
        n, rem = divmod(n, 32)
        chars.append(_CROCKFORD_ALPHABET[rem])
    ts_part = "".join(reversed(chars))
    rand_part = "".join(secrets.choice(_CROCKFORD_ALPHABET) for _ in range(16))
    return ts_part + rand_part


def _make_test_record(commodity: str) -> dict:
    """One record shaped like `parse_agmarknet_record`'s output."""
    return {
        "market": "__SCHEMA_CONTRACT_TEST__",
        "state": "__SCHEMA_CONTRACT_TEST__",
        "district": "SchemaContractTest",
        "commodity": commodity,
        "raw_variety": "SchemaContractTestVariety",
        "raw_grade": "FAQ",
        "price_date": "2026-07-24",
        "modal_price": "1000",
        "min_price": "900",
        "max_price": "1100",
    }


def test_upsert_raw_price_entries_batch_accepts_real_ulid_batch_id_against_live_schema():
    """
    The regression this guards against: `p_batch_id` in the deployed
    `upsert_raw_price_entry` / `upsert_raw_price_entries_batch` RPCs
    silently declared as `bigint` while every caller in this codebase
    has only ever passed a 26-character Crockford-base32 ULID string.
    A fake client can't catch that -- only a real RPC call can.
    """
    from supabase import create_client

    client = create_client(os.environ[_URL_VAR], os.environ[_KEY_VAR])

    real_batch_id = _fake_ulid()  # e.g. "01J2Z8Q9K3M5N7P9R1S3T5V7W9"
    assert len(real_batch_id) == 26, "sanity check: this must look like a real batch id"

    rows_written, rows_new = upsert_raw_price_entries_batch(
        client,
        resource="resource_1",
        batch_id=real_batch_id,
        parser_version=1,
        parsed_records=[_make_test_record("__SCHEMA_CONTRACT_TEST_CROP__")],
    )

    # If the RPC still declared p_batch_id as bigint, this call raises
    # `invalid input syntax for type bigint: "<ulid>"` before we ever
    # get here -- that's the exact production failure this test exists
    # to catch on every future schema change.
    assert rows_written == 1
    assert rows_new is True or rows_new == 1

    written = (
        client.table("raw_price_entries")
        .select("id, first_seen_batch_id, last_seen_batch_id")
        .eq("market", "__SCHEMA_CONTRACT_TEST__")
        .eq("commodity", "__SCHEMA_CONTRACT_TEST_CROP__")
        .order("id", desc=True)
        .limit(1)
        .execute()
    )
    assert written.data, "expected the row we just wrote to be readable back"
    row = written.data[0]
    # Proves the batch id round-tripped as the *exact* ULID string, not
    # a truncated/coerced/reinterpreted value.
    assert row["first_seen_batch_id"] == real_batch_id
    assert row["last_seen_batch_id"] == real_batch_id
