"""live_tick fails the GitHub run when the feed is down, so the owner is emailed and publish_live does not stamp stale data as new."""

from __future__ import annotations

import pytest

from farmuncle_pipeline.ingestion import live_tick


def test_main_exits_with_an_error_when_the_feed_was_down(monkeypatch):
    monkeypatch.setattr(live_tick, "validate_startup", lambda: object())
    monkeypatch.setattr(live_tick, "run_live_tick", lambda ctx: True)  # True: the outage breaker tripped
    with pytest.raises(SystemExit) as stopped:
        live_tick.main()
    assert stopped.value.code == 1


def test_main_ends_normally_when_the_feed_answered(monkeypatch):
    monkeypatch.setattr(live_tick, "validate_startup", lambda: object())
    monkeypatch.setattr(live_tick, "run_live_tick", lambda ctx: False)
    live_tick.main()
