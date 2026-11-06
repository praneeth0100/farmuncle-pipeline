from datetime import datetime, timedelta, timezone
from pathlib import Path

from farmuncle_pipeline.news.core import build_files, is_farm, merge, parse_items, picture, place_index, same_story, tag_places

FIX = Path(__file__).parent / "fixtures" / "news"
NOW = datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc)


def feed(**kw):
    return {"id": "f", "url": "https://x", "language": "en", "level": "national", "states": [], "farm_only": False,
            "outlet": "Outlet", **kw}


def story(i, title="Tomato farmers protest", hours=1, **kw):
    return {"id": i, "title": title, "link": f"https://x/{i}", "outlet": "O",
            "published": (NOW - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ"), "lang": "en", "image": None,
            "level": "national", "districts": [], "states": [], "farm": False, **kw}


def test_parse_items_media_content():
    items = parse_items((FIX / "hindu_agri.xml").read_bytes(), feed(id="hindu_agri", outlet="The Hindu", farm_only=True))
    first = items[0]
    assert len(items) == 3
    assert first["title"] == "AgriNova: When a farm can text and call you!"
    assert first["outlet"] == "The Hindu" and first["published"] == "2026-11-04T07:17:48Z"
    assert first["image"].startswith("https://th-i.thgim.com/") and first["level"] == "national" and first["farm"] is True
    assert len(first["id"]) == 40 and first["districts"] == [] and first["states"] == []


def test_picture_from_enclosure_upgraded_and_img_in_content():
    rv = parse_items((FIX / "ruralvoice.xml").read_bytes(), feed(upgrade_images=True))
    assert rv[0]["image"].startswith("https://eng.ruralvoice.in/uploads/")
    assert "&#039;" not in rv[0]["summary"]
    kn = parse_items((FIX / "tv9kn.xml").read_bytes(), feed(language="kn", level="state", states=["ka"]))
    assert kn[0]["image"] == "https://images.tv9kannada.com/wp-content/uploads/2026/11/karmabhumi-express.jpg"
    assert kn[0]["states"] == ["ka"] and kn[0]["lang"] == "kn"
    ta = parse_items((FIX / "oneindia_ta.xml").read_bytes(), feed(language="ta"))
    assert ta and all(s["image"] is None for s in ta)


def test_picture_https_only():
    assert picture({"media_content": [{"url": "http://a/x.jpg", "medium": "image"}]}) is None
    assert parse_items((FIX / "ruralvoice.xml").read_bytes(), feed())[0]["image"] is None  # no upgrade flag → http dropped


def test_picture_prefers_largest_up_to_800():
    media = [{"url": "https://a/200.jpg", "width": "200"}, {"url": "https://a/800.jpg", "width": "800"}, {"url": "https://a/1600.jpg", "width": "1600"}]
    assert picture({"media_content": media}) == "https://a/800.jpg"
    assert picture({"media_thumbnail": [{"url": "https://a/1200.jpg", "width": "1200"}]}) == "https://a/1200.jpg"


def test_general_feed_needs_farm_word_and_no_block_word():
    assert not is_farm({"title": "Sensex rises as farmers cheer", "summary": "", "lang": "en"}, False)
    assert is_farm({"title": "Tomato farmers protest", "summary": "", "lang": "en"}, False)
    assert not is_farm({"title": "Old vehicle fees rise", "summary": "", "lang": "en"}, False)
    assert not is_farm({"title": "ಕ್ರಿಕೆಟ್ ಪಂದ್ಯ: ರೈತರು ವೀಕ್ಷಣೆ", "summary": "", "lang": "kn"}, False)
    assert is_farm({"title": "ರೈತರಿಗೆ ಬೆಳೆ ವಿಮೆ", "summary": "", "lang": "kn"}, False)
    assert is_farm({"title": "Sensex anything", "summary": "", "lang": "en"}, True)  # farm sections are trusted
    assert not is_farm({"title": "Pharmacy prices", "summary": "", "lang": "en"}, False)  # "farm" must start a word


MANDIS = [{"id": "ka.bengaluru_urban.binny_mill", "district": "ka.bengaluru_urban", "state": "ka"},
          {"id": "ka.bengaluru_rural.doddaballapur", "district": "ka.bengaluru_rural", "state": "ka"},
          {"id": "ka.mysuru.mysuru", "district": "ka.mysuru", "state": "ka"},
          {"id": "kl.ernakulam.x", "district": "kl.ernakulam", "state": "kl"}]
STATES = [{"id": "ka", "name": "Karnataka"}, {"id": "kl", "name": "Keralam"}]


def test_tag_places_by_codes():
    index = place_index(MANDIS, STATES)
    s = tag_places({"title": "Heavy rain in Bengaluru Urban", "summary": "", "districts": [], "states": []}, index)
    assert s["districts"] == ["ka.bengaluru_urban"] and s["states"] == ["ka"]
    s = tag_places({"title": "Rubber growers in Kerala", "summary": "Mysuru too", "districts": [], "states": []}, index)
    assert s["districts"] == ["ka.mysuru"] and s["states"] == ["ka", "kl"]
    s = tag_places({"title": "Bengaluru traffic", "summary": "", "districts": [], "states": ["ka"]}, index)
    assert s["districts"] == []  # "bengaluru" is in two districts, so alone it names neither
    lower = place_index([{"district": "ar.lower_subansiri", "state": "ar"}], [])
    assert tag_places({"title": "Prices lower in October", "summary": "", "districts": [], "states": []}, lower)["districts"] == []
    assert tag_places({"title": "Floods in Subansiri", "summary": "", "districts": [], "states": []}, lower)["districts"] == ["ar.lower_subansiri"]


def test_same_story_keeps_indic_marks():
    assert same_story("Onion prices crash in Nashik mandi", "Onion prices crash in Nashik mandi today")
    assert not same_story("Onion prices crash in Nashik", "Tomato farmers protest in Kolar")
    assert same_story("ರೈತರಿಗೆ ಬೆಳೆ ವಿಮೆ ಹಣ ಬಿಡುಗಡೆ", "ರೈತರಿಗೆ ಬೆಳೆ ವಿಮೆ ಹಣ ಬಿಡುಗಡೆ ಮಾಡಿದ ಸರ್ಕಾರ")


def test_merge_drops_older_than_72h_and_folds_duplicates():
    saved = [story("a", "Karnataka drought relief for paddy farmers", hours=5, states=["ka"]),
             story("old", hours=80), story("keep", "Wheat sowing begins early", hours=10)]
    fresh = [story("b", "Karnataka drought relief for paddy farmers announced", hours=2, image="https://i/b.jpg"),
             story("keep", "Wheat sowing begins early", hours=10, image="https://i/k.jpg")]
    out = merge(saved, fresh, NOW)
    assert [s["id"] for s in out] == ["b", "keep"]
    assert out[0]["states"] == ["ka"]  # the folded twin's places are kept
    assert out[1]["image"] == "https://i/k.jpg"  # a fresh copy replaces the saved one


def test_national_story_naming_state_lands_in_state_file():
    files = build_files([story("n", states=["ka"]), story("s", level="state", states=["ka"], lang="kn"),
                         story("w", level="world")], NOW)
    assert [s["id"] for s in files["news/state/ka/en.json"]["stories"]] == ["n"]
    assert [s["id"] for s in files["news/india/en.json"]["stories"]] == ["n"]
    assert [s["id"] for s in files["news/state/ka/kn.json"]["stories"]] == ["s"]
    assert [s["id"] for s in files["news/world/en.json"]["stories"]] == ["w"]
    assert files["news/india/en.json"]["v"] == 1 and files["news/india/en.json"]["built_at"] == "2026-11-06T12:00:00Z"


def test_build_files_newest_first_and_capped():
    files = build_files([story(str(i), f"Story number {i} unique words{i}", hours=i % 70) for i in range(200)], NOW)
    got = files["news/india/en.json"]["stories"]
    assert len(got) == 150 and got == sorted(got, key=lambda s: s["published"], reverse=True)
