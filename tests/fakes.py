"""
FarmUncle v2 — tests/fakes.py
Phase D.5, supporting module (not a test file itself — no `test_`
prefix, so pytest won't try to collect it).

Purpose:
    Small, hand-written test doubles shared across the test suite —
    deliberately NOT a full postgrest/Supabase emulator. Each test
    file builds its own minimal fake table/client tailored to exactly
    the query shapes the code under test actually makes (see
    `test_price_writer.py`/`test_identity_client.py` for those). What's
    shared here is only the two things every fake needs regardless of
    which real call it's standing in for:
      - `FakeAPIError`: mimics postgrest's `APIError`, specifically
        the `.code` attribute `price_writer._is_row_level_error` reads
        to decide row-level vs. transient. The real exception type
        isn't imported here on purpose — the code under test only
        ever does `getattr(exc, "code", None)`, so a plain Exception
        subclass with that attribute is a faithful, dependency-free
        stand-in.
      - `FakeResponse`: mimics the `.data` (and sometimes `.count`)
        attribute every real postgrest/RPC response carries, which is
        all this codebase ever reads off a response.

Why not one universal fake client for the whole suite:
    A single do-everything fake postgrest emulator would itself become
    something worth testing, and would obscure exactly which query
    shape each test is asserting on. Small, purpose-built fakes per
    test file are more verbose but each one is readable in isolation —
    deliberately prioritized here given Phase D.5 is starting from
    zero coverage, not iterating on an existing suite.
"""

from __future__ import annotations


class FakeAPIError(Exception):
    """Purpose: stand-in for postgrest's real `APIError`. Only
    `.code` is read by the code under test (`price_writer.
    _is_row_level_error`), so only `.code` is provided here."""

    def __init__(self, message: str, code: str | None = None) -> None:
        super().__init__(message)
        self.code = code


class FakeResponse:
    """Purpose: stand-in for a postgrest/RPC response object. `.data`
    is read everywhere; `.count` only by the two `count="exact"`
    queries in `nightly_quality_audit.py` (not under test here, but
    kept generic enough to reuse there later)."""

    def __init__(self, data=None, count: int | None = None) -> None:
        self.data = data
        self.count = count
