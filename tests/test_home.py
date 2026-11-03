import gzip
import json
from datetime import date

from farmuncle_pipeline.ingestion import home

TODAY = date(2026, 11, 3)
T, Y = "2026-11-03", "2026-11-02"
NEIGHBOURS = {"h": [("a", 30), ("b", 120)], "a": [("h", 30), ("b", 100)], "b": [("a", 100), ("h", 120)], "lonely": []}
COORDS = {"h": (13.0, 78.0, False), "a": (13.2, 78.1, False), "b": (14.0, 78.0, True), "lonely": (20.0, 80.0, False)}
LABELS = {"h": "Home", "a": "Aville", "b": "Bpur", "lonely": "Lonely"}


def r(day, modal, prev=None, prev_date=None):
    return {"date": day, "modal": modal, "min": modal - 2, "max": modal + 2, "prev": prev, "prev_date": prev_date}


def key(mandi, grade="faq", crop="tomato", variety="tomato.local"):
    return (mandi, crop, variety, grade)


def build(latest, stats=None, odd=frozenset(), homes=("h",)):
    return home.build_home(homes=list(homes), latest=latest, neighbours=NEIGHBOURS, coords=COORDS, labels=LABELS,
                           stats=stats or {"as_of": Y, "keys": {}}, odd=set(odd), today=TODAY, stamp="2026-11-03T10:00:00+05:30")


def rows_of(out, crop="tomato", mandi="h"):
    return next(c["rows"] for c in out[f"home/{mandi}.json"]["crops"] if c["crop"] == crop)


def test_home_own_price_from_yesterday_beats_another_mandi_today():
    (row,) = rows_of(build({key("h"): r(Y, 20), key("a"): r(T, 25)}))
    assert (row["mandi"], row["date"]) == ("h", Y)  # the farmer's own mandi, still fresh


def test_home_older_own_price_loses_to_a_nearby_mandi_today():
    (row,) = rows_of(build({key("h"): r("2026-11-01", 20), key("a"): r(T, 25)}))
    assert (row["mandi"], row["date"], row["modal"]) == ("a", T, 25)


def test_home_falls_back_to_last_known_with_date():
    (row,) = rows_of(build({key("h"): r(Y, 20, prev=18, prev_date="2026-10-31")}))
    assert (row["mandi"], row["date"], row["prev"], row["prev_date"]) == ("h", Y, 18, "2026-10-31")


def test_home_keeps_grades_apart():
    rows = rows_of(build({key("h"): r(T, 20), key("h", "local"): r(T, 15)}))
    assert sorted((x["grade"], x["modal"]) for x in rows) == [("faq", 20), ("local", 15)]


def test_home_skips_odd_unless_only_choice():
    (row,) = rows_of(build({key("h"): r(T, 3), key("a"): r(T, 22)}, odd={key("h")}))
    assert row["mandi"] == "a" and "odd" not in row
    (row,) = rows_of(build({key("h"): r(T, 3)}, odd={key("h")}))
    assert row["mandi"] == "h" and row["odd"] is True


def test_home_adds_then_from_stats():
    stats = {"as_of": Y, "keys": {"a|tomato|tomato.local|faq": {"7D": [18, "2026-10-27"]}}}
    out = build({key("a"): r(T, 22)}, stats=stats)
    (row,) = rows_of(out)
    assert row["then"] == {"7D": [18, "2026-10-27"]} and out["home/h.json"]["stats_as_of"] == Y


def test_home_better_when_5_pct_more_in_reach():
    (row,) = rows_of(build({key("h"): r(T, 20), key("a"): r(T, 20.5), key("b"): r(T, 22)}))
    assert row["mandi"] == "h" and row["better"] == {"mandi": "b", "modal": 22}
    (row,) = rows_of(build({key("h"): r(T, 20), key("a"): r(T, 20.5)}))
    assert "better" not in row


def test_home_names_every_mandi_it_uses():
    out = build({key("b"): r(T, 22)})["home/h.json"]
    assert out["mandis"] == {"h": {"label": "Home", "km": 0}, "b": {"label": "Bpur", "km": 120, "approx": True}}
    assert (out["label"], out["state"], out["as_of"]) == ("Home", "h", T)


def test_home_orders_crops_home_first_then_popularity():
    latest = {key("a", crop="onion", variety="onion.red"): r(T, 30), key("b", crop="onion", variety="onion.red"): r(T, 31),
              key("h", crop="beans", variety="beans.x"): r(T, 40)}
    assert [c["crop"] for c in build(latest)["home/h.json"]["crops"]] == ["beans", "onion"]


def test_home_for_mandi_with_nothing_nearby_has_empty_crops():
    out = build({key("h"): r(T, 20)}, homes=("lonely",))
    assert out["home/lonely.json"]["crops"] == []


def test_trim_drops_least_popular_crops_under_limit():
    import hashlib
    noise = lambda i, j: hashlib.sha256(f"{i}-{j}".encode()).hexdigest()  # noqa: E731 - incompressible text
    payload = {"v": 2, "crops": [{"crop": f"c{i}", "rows": [{"variety": noise(i, j)} for j in range(6)]} for i in range(60)]}
    data = home.trim(payload, limit=2_000)
    kept = json.loads(data)["crops"]
    assert len(gzip.compress(data)) <= 2_000 and 0 < len(kept) < 60 and kept[0]["crop"] == "c0"


def test_hourly_run_rewrites_only_used_mandis_and_their_neighbours():
    assert home.homes_to_build(["h", "a", "b", "lonely"], used={"lonely"}, neighbours=NEIGHBOURS) == {"lonely"}
    assert home.homes_to_build(["h", "a", "b", "lonely"], used={"h"}, neighbours=NEIGHBOURS) == {"h", "a", "b"}
    assert home.homes_to_build(["h", "a", "b", "lonely"], used=None, neighbours=NEIGHBOURS) == {"h", "a", "b", "lonely"}


def test_latest_rows_keep_last_report_and_the_one_before():
    rows = [{"key": key("h"), "date": d, "modal": m, "min": m - 1, "max": m + 1} for d, m in ((Y, 20), (T, 22), ("2026-10-10", 9))]
    latest = home.latest_rows(rows, TODAY)
    assert latest[key("h")] == {"date": T, "modal": 22, "min": 21, "max": 23, "prev": 20, "prev_date": Y}


def test_top_file_ranks_by_price_and_rise_per_grade():
    latest = {key("h"): r(T, 20), key("a"): r(T, 25), key("b"): r(Y, 22), key("a", "local"): r(T, 15),
              key("lonely"): r("2026-10-25", 99)}  # too old to rank
    stats = {"keys": {"h|tomato|tomato.local|faq": {"7D": [10, "2026-10-27"]}, "a|tomato|tomato.local|faq": {"7D": [24, "2026-10-27"]}}}
    out = home.build_top(latest=latest, stats=stats, labels=LABELS, odd=set(), today=TODAY, stamp="s")
    lines = {(x["variety"], x["grade"]): x for x in out["top/tomato.json"]["lines"]}
    faq = lines[("tomato.local", "faq")]
    assert [m["mandi"] for m in faq["by_price"]] == ["a", "b", "h"]
    assert [(m["mandi"], m["pct"]) for m in faq["by_rise"]] == [("h", 100), ("a", 4)]
    assert [m["mandi"] for m in lines[("tomato.local", "local")]["by_price"]] == ["a"]


def test_home_lists_own_crops_first_then_fresh_then_old():
    latest = {key("a", crop="onion", variety="onion.red"): r(T, 30), key("b", crop="onion", variety="onion.red"): r(T, 31),
              key("a", crop="garlic", variety="garlic.x"): r("2026-10-25", 90), key("b", crop="garlic", variety="garlic.x"): r("2026-10-25", 95),
              key("b", crop="okra", variety="okra.x"): r(T, 40),
              key("h", crop="beans", variety="beans.x"): r("2026-10-30", 40)}
    assert [c["crop"] for c in build(latest)["home/h.json"]["crops"]] == ["beans", "onion", "okra", "garlic"]
