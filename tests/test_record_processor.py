"""
FarmUncle v2 — tests/test_record_processor.py
Phase D.5, third test target.

Why this file: `process_records` is the glue between the two modules
already covered (`identity_client.py`, and indirectly `price_writer.py`
via the row shape it produces) plus `resource_client.parse_agmarknet_record`
and `quality_scoring.compute_quality` — it's the one place all four
come together into the actual row dict that ends up in
`mandi_daily_prices`. A mistake here (wrong field name, wrong lineage
stamping, swallowing an error silently in the wrong place) would be
invisible in any of the other three modules' own tests.

What's tested:
    - A well-formed record produces a correctly-shaped price row: every
      lineage field (batch_id, raw_api_batch_id, parser_version,
      normalization_version), every price field, and a quality score
      computed from the real `compute_quality` (not faked — it's a
      pure function, so there's no reason to fake it here).
    - A malformed record (missing a required field) is counted in
      `rows_failed` and produces no row — never raises.
    - A record whose identity resolution fails (the RPC itself errors)
      is likewise counted and skipped, not raised — this is the
      "one bad record in a page of 500 shouldn't abort the page"
      guarantee the module docstring promises.
    - Both Resource 1's lowercase field names and Resource 2's
      Title_Case field names resolve to the same row shape (exercises
      `parse_agmarknet_record`'s case-insensitive lookup through the
      one path that actually calls it in production).
    - `first_seen_this_run` (RPC call vs. cache/preload hit) correctly
      flows through into the row's `quality_components.entity_verified`
      — the one place a subtle wiring mistake between IdentityClient
      and quality_scoring would show up.

What's deliberately NOT re-tested here (already covered elsewhere):
    - `IdentityClient`'s own cache/preload/RPC resolution order — see
      `test_identity_client.py`.
    - `compute_quality`'s own component math — it's simple, pure, and
      exercised indirectly here; a dedicated `test_quality_scoring.py`
      would be reasonable follow-up but isn't this file's job.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from farmuncle_pipeline.config import NORMALIZATION_VERSION, PARSER_VERSION, Source
from farmuncle_pipeline.core.identity_client import IdentityClient
from farmuncle_pipeline.core.record_processor import process_records
from tests.test_identity_client import FakeIdentityDBClient

# A realistic Resource 1 record (lowercase keys) with everything
# process_records needs downstream.
_RESOURCE_1_RECORD = {
    "commodity": "Tomato",
    "market": "Guntur APMC",
    "state": "Andhra Pradesh",
    "district": "Guntur",
    "variety": "FAQ",
    "grade": "Non-FAQ",
    "arrival_date": "15/07/2026",
    "modal_price": "2500",
    "min_price": "2000",
    "max_price": "3000",
}

# The same record, Resource 2 shape (Title_Case keys) — per
# resource_client.py's own docstring, the two resources genuinely
# differ in casing in production.
_RESOURCE_2_RECORD = {
    "Commodity": "Tomato",
    "Market": "Guntur APMC",
    "State": "Andhra Pradesh",
    "District": "Guntur",
    "Variety": "FAQ",
    "Grade": "Non-FAQ",
    "Arrival_Date": "15/07/2026",
    "Modal_Price": "2500",
    "Min_Price": "2000",
    "Max_Price": "3000",
}

_KNOWN_MANDI = {"id": 42, "normalized_name": "guntur apmc", "state": "Andhra Pradesh", "district": "Guntur"}
_KNOWN_CROP = {"id": 10, "normalized_name": "tomato"}


def _make_identity(*, mandis=None, crops=None, rpc_responses=None) -> IdentityClient:
    client = FakeIdentityDBClient(
        mandis=mandis or [],
        crops=crops or [],
        rpc_responses=rpc_responses or {},
    )
    identity = IdentityClient(client)
    identity.preload()
    return identity


@pytest.mark.parametrize("record", [_RESOURCE_1_RECORD, _RESOURCE_2_RECORD], ids=["resource_1_casing", "resource_2_casing"])
def test_process_records_well_formed_record_produces_expected_row(record):
    identity = _make_identity(mandis=[_KNOWN_MANDI], crops=[_KNOWN_CROP])

    result = process_records(
        [record],
        identity=identity,
        unit="kg",
        source=Source.RESOURCE_2,
        batch_id="batch-123",
        raw_api_batch_id="raw-batch-456",
        job_name="test_job",
    )

    assert result.rows_failed == 0
    assert len(result.price_rows) == 1
    row = result.price_rows[0]

    # Identity fields — resolved via preload, both known entities.
    assert row["mandi_id"] == 42
    assert row["crop_id"] == 10
    assert row["variety"] == "faq"
    assert row["grade"] == "non-faq"
    assert row["price_date"] == "2026-07-15"

    # Price fields parsed to float.
    assert row["modal_price"] == 2500.0
    assert row["min_price"] == 2000.0
    assert row["max_price"] == 3000.0

    # Passed-through fields.
    assert row["unit"] == "kg"
    assert row["source"] == Source.RESOURCE_2.value

    # Lineage (invariant 8) — the whole reason batch_id/raw_api_batch_id
    # are parameters at all.
    assert row["batch_id"] == "batch-123"
    assert row["raw_api_batch_id"] == "raw-batch-456"
    assert row["parser_version"] == PARSER_VERSION
    assert row["normalization_version"] == NORMALIZATION_VERSION

    # Quality score present and internally consistent — both entities
    # were known (preload hit), so entity_verified should be a full 1.0
    # and price fields are sane, so price_sanity should be 1.0 too.
    assert row["quality_components"]["entity_verified"] == 1.0
    assert row["quality_components"]["price_sanity"] == 1.0
    assert 0.0 <= row["quality_score"] <= 1.0


def test_process_records_malformed_record_is_skipped_not_raised():
    # Missing "commodity" — parse_agmarknet_record returns None for
    # this, per its own docstring.
    bad_record = {**_RESOURCE_1_RECORD, "commodity": ""}
    identity = _make_identity(mandis=[_KNOWN_MANDI], crops=[_KNOWN_CROP])

    result = process_records(
        [bad_record],
        identity=identity,
        unit="kg",
        source=Source.RESOURCE_1,
        batch_id="batch-1",
        raw_api_batch_id="raw-1",
        job_name="test_job",
    )

    assert result.price_rows == []
    assert result.rows_failed == 1


def test_process_records_record_with_no_grade_reported_normalizes_to_other():
    # Not every government record carries a Grade field (confirmed
    # 2026-07-18: many F&V-type records omit it entirely) — this must
    # normalize to "other" the same way a missing variety does
    # (resolve_grade mirrors resolve_variety exactly), and must NOT
    # cause the record to be treated as malformed.
    record_without_grade = {k: v for k, v in _RESOURCE_1_RECORD.items() if k != "grade"}
    identity = _make_identity(mandis=[_KNOWN_MANDI], crops=[_KNOWN_CROP])

    result = process_records(
        [record_without_grade],
        identity=identity,
        unit="kg",
        source=Source.RESOURCE_1,
        batch_id="batch-1",
        raw_api_batch_id="raw-1",
        job_name="test_job",
    )

    assert result.rows_failed == 0
    assert len(result.price_rows) == 1
    assert result.price_rows[0]["grade"] == "other"


def test_process_records_identity_resolution_failure_is_skipped_not_raised(capsys):
    # find_or_create_mandi itself erroring (e.g. a real network/RPC
    # failure) must be caught and counted, not propagate and abort
    # every other record in the page.
    def _raise_mandi(_params):
        raise RuntimeError("simulated RPC failure")

    identity = _make_identity(
        mandis=[],  # no preload hit — forces the RPC path
        crops=[_KNOWN_CROP],
        rpc_responses={"find_or_create_mandi": _raise_mandi},
    )

    result = process_records(
        [_RESOURCE_1_RECORD],
        identity=identity,
        unit="kg",
        source=Source.RESOURCE_1,
        batch_id="batch-1",
        raw_api_batch_id="raw-1",
        job_name="test_job",
    )

    assert result.price_rows == []
    assert result.rows_failed == 1
    # The module docstring promises a print for attributability —
    # confirm it actually happens and names the calling job.
    captured = capsys.readouterr()
    assert "test_job" in captured.out
    assert "identity resolution failed" in captured.out


def test_process_records_mixed_batch_counts_independently():
    # One good record, one malformed, one identity failure — in one
    # call, to confirm failures in earlier records don't affect later
    # ones and counts are independent per record.
    bad_record = {**_RESOURCE_1_RECORD, "market": ""}

    def _raise_crop(_params):
        raise RuntimeError("simulated RPC failure")

    identity = _make_identity(
        mandis=[_KNOWN_MANDI],
        crops=[],  # no preload hit for crop — forces RPC, which we make fail
        rpc_responses={"find_or_create_crop": _raise_crop},
    )
    failing_identity_record = {**_RESOURCE_1_RECORD, "market": "Guntur APMC"}

    result = process_records(
        [_RESOURCE_1_RECORD, bad_record, failing_identity_record],
        identity=identity,
        unit="kg",
        source=Source.RESOURCE_1,
        batch_id="batch-1",
        raw_api_batch_id="raw-1",
        job_name="test_job",
    )

    # Both the first and third records hit the same failing crop RPC
    # (crop resolution is not preloaded here), so both fail identity
    # resolution; only the malformed one is a parse failure. All three
    # end up failed, for different, independently-counted reasons.
    assert result.price_rows == []
    assert result.rows_failed == 3


def test_process_records_newly_created_entities_lower_entity_verified_component():
    # An entity resolved via a genuine RPC call (not preload/cache) is
    # "newly created" from this run's perspective — compute_quality
    # must see that and lower entity_verified accordingly. This is the
    # one place a wiring mistake between IdentityClient's
    # first_seen_this_run and quality_scoring's
    # mandi_newly_created/crop_newly_created parameters would surface.
    identity = _make_identity(
        mandis=[],  # forces RPC for mandi
        crops=[],  # forces RPC for crop
        rpc_responses={"find_or_create_mandi": 42, "find_or_create_crop": 10},
    )

    result = process_records(
        [_RESOURCE_1_RECORD],
        identity=identity,
        unit="kg",
        source=Source.RESOURCE_1,
        batch_id="batch-1",
        raw_api_batch_id="raw-1",
        job_name="test_job",
    )

    assert result.rows_failed == 0
    row = result.price_rows[0]
    # Both mandi and crop were newly resolved via RPC this run ->
    # entity_verified = 1.0 - 0.5 - 0.5 = 0.0, per compute_quality's
    # own documented formula.
    assert row["quality_components"]["entity_verified"] == 0.0
