from datetime import datetime, timedelta

from farmuncle_pipeline.ingestion import publish_live as pl

T0 = datetime(2026, 10, 30, 10, 0, tzinfo=pl.IST)


def test_version_moves_at_most_once_a_day_and_never_loses_a_change():
    h = {}
    assert pl.catalog_step(True, 5, h, T0, first=True) == (True, 5)  # first publish keeps the manifest's version
    assert pl.catalog_step(False, 5, h, T0 + timedelta(hours=1), first=False) == (False, 5)
    assert pl.catalog_step(True, 5, h, T0 + timedelta(hours=2), first=False) == (True, 5)  # files follow, version holds
    assert pl.catalog_step(False, 5, h, T0 + timedelta(hours=5), first=False) == (False, 5)
    assert pl.catalog_step(True, 5, h, T0 + timedelta(hours=9), first=False) == (True, 5)
    assert pl.catalog_step(False, 5, h, T0 + timedelta(hours=25), first=False) == (True, 6)  # held change bumps after a day
    assert pl.catalog_step(False, 6, h, T0 + timedelta(hours=26), first=False) == (False, 6)
    assert pl.catalog_step(True, 6, h, T0 + timedelta(hours=27), first=False) == (True, 6)
    assert pl.catalog_step(True, 6, h, T0 + timedelta(hours=50), first=False) == (True, 7)
