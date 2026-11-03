from datetime import datetime

from farmuncle_pipeline.ops.freshness import problems
from farmuncle_pipeline.ingestion.publish_live import IST


def at(text):
    return datetime.fromisoformat(text).replace(tzinfo=IST)


def test_fresh_manifest_has_no_problems():
    m = {"analytics_built_at": "2026-11-03T02:00:00+05:30", "live_updated_at": "2026-11-03T13:10:00+05:30"}
    assert problems(m, at("2026-11-03T14:00:00")) == []


def test_old_analytics_and_old_live_in_the_day_are_problems():
    m = {"analytics_built_at": "2026-10-31T05:42:50+05:30", "live_updated_at": "2026-11-03T09:00:00+05:30"}
    out = problems(m, at("2026-11-03T14:00:00"))
    assert len(out) == 2 and "analytics" in out[0] and "live" in out[1]


def test_old_live_at_night_is_fine():
    m = {"analytics_built_at": "2026-11-03T02:00:00+05:30", "live_updated_at": "2026-11-02T21:00:00+05:30"}
    assert problems(m, at("2026-11-03T05:00:00")) == []


def test_missing_analytics_is_a_problem():
    assert problems({"live_updated_at": "2026-11-03T13:10:00+05:30"}, at("2026-11-03T14:00:00"))
