"""Catalog snapshot and the real archive-state wrapper."""

from __future__ import annotations

import io
import sys
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from farmuncle_pipeline.archive.catalog import export_catalog
from farmuncle_pipeline.archive.state import SupabaseArchiveState
from tests.fakes import FakeResponse


class _Dims:
    def __init__(self):
        self.name = None
        self._a, self._b = 0, 999

    def table(self, name):
        self.name = name
        return self

    def select(self, *a, **k):
        return self

    def order(self, *a, **k):
        return self

    def range(self, a, b):
        self._a, self._b = a, b
        return self

    def execute(self):
        rows = [{"id": 1, "name": f"{self.name}-1", "tags": ["a"]}, {"id": 2, "name": f"{self.name}-2", "tags": None}]
        return FakeResponse(data=rows[self._a : self._b + 1])


class _Store:
    def __init__(self):
        self.objects = {}

    def put(self, key, data, content_type="application/octet-stream"):
        self.objects[key] = data


def test_catalog_tables_are_exported_as_parquet():
    store = _Store()
    assert export_catalog(_Dims(), store) == {"mandis": 2, "crops": 2, "varieties": 2, "grades": 2}
    assert pq.read_table(io.BytesIO(store.objects["catalog/crops.parquet"])).num_rows == 2


class _StateDb:
    def __init__(self, row=None):
        self.row, self.upserts = row, []

    def table(self, name):
        assert name == "archive_state"
        return self

    def select(self, *a, **k):
        return self

    def eq(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return self

    def upsert(self, payload, on_conflict=None):
        self.upserts.append(payload)
        return self

    def execute(self):
        return FakeResponse(data=[self.row] if self.row else [])


def test_state_starts_empty_then_records_progress():
    db = _StateDb()
    state = SupabaseArchiveState(db)
    assert state.get() is None and not state.done
    state.set(date(2026, 10, 8))
    assert db.upserts[-1]["next_end"] == "2026-10-08" and db.upserts[-1]["done"] is False
    state.set(date(2017, 12, 31), done=True)
    assert state.done and db.upserts[-1]["windows_done"] == 2


def test_state_reads_what_was_stored():
    state = SupabaseArchiveState(_StateDb({"next_end": "2026-10-08", "done": False, "windows_done": 3}))
    assert state.get() == date(2026, 10, 8)
