from datetime import datetime, timedelta, timezone
from pathlib import Path

from farmuncle_pipeline.news.core import build_files, is_farm, merge, parse_items, picture, place_index, same_story, settle_states, tag_places

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
    assert not is_farm({"title": "Monsoon session of Parliament begins", "summary": "", "lang": "en"}, False)
    assert not is_farm({"title": "Body found at farmhouse", "summary": "", "lang": "en"}, False)
    assert not is_farm({"title": "Satellite pics show Ram Mandir in Ayodhya", "summary": "", "lang": "en"}, False)  # "mandi" must end a word
    assert is_farm({"title": "Onion arrivals rise at Lasalgaon mandis", "summary": "", "lang": "en"}, False)


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


def test_names_shared_by_two_states_or_common_words_tag_nothing():
    index = place_index([{"district": "hp.hamirpur", "state": "hp"}, {"district": "up.hamirpur", "state": "up"},
                         {"district": "tn.erode", "state": "tn"}, {"district": "ka.mysuru", "state": "ka"}], [])
    tag = lambda title: tag_places({"title": title, "summary": "", "districts": [], "states": []}, index)  # noqa: E731
    assert tag("Apple growers in Hamirpur") == {"title": "Apple growers in Hamirpur", "summary": "", "districts": [], "states": []}
    assert tag("Prices erode as rains lash the state")["districts"] == []
    assert tag("Mysuru silk")["districts"] == ["ka.mysuru"]


def test_http_article_links_are_dropped():
    xml = b"""<rss><channel><item><title>Farmers protest</title><link>http://x/a</link><pubDate>Fri, 06 Nov 2026 10:00:00 GMT</pubDate></item>
      <item><title>Farmers rally</title><link>https://x/b</link><pubDate>Fri, 06 Nov 2026 10:00:00 GMT</pubDate></item></channel></rss>"""
    assert [s["link"] for s in parse_items(xml, feed())] == ["https://x/b"]


def test_same_story_keeps_indic_marks():
    assert same_story("Onion prices crash in Nashik mandi", "Onion prices crash in Nashik mandi today")
    assert not same_story("Onion prices crash in Nashik", "Tomato farmers protest in Kolar")
    assert same_story("ರೈತರಿಗೆ ಬೆಳೆ ವಿಮೆ ಹಣ ಬಿಡುಗಡೆ", "ರೈತರಿಗೆ ಬೆಳೆ ವಿಮೆ ಹಣ ಬಿಡುಗಡೆ ಮಾಡಿದ ಸರ್ಕಾರ")


def test_merge_drops_older_than_72h_and_caps_future_times():
    saved = [story("a", "Karnataka drought relief for paddy farmers", hours=5, states=["ka"]),
             story("old", hours=80), story("keep", "Wheat sowing begins early", hours=10)]
    fresh = [story("b", "Karnataka drought relief for paddy farmers announced", hours=2, image="https://i/b.jpg"),
             story("keep", "Wheat sowing begins early", hours=10, image="https://i/k.jpg"), story("future", "Odd clock", hours=-30)]
    out = merge(saved, fresh, NOW)
    assert [s["id"] for s in out] == ["future", "b", "a", "keep"]  # twins stay apart in the saved state
    assert out[0]["published"] == "2026-11-06T12:00:00Z"  # a wrong feed clock cannot pin a story to the top for days
    assert out[3]["image"] == "https://i/k.jpg"  # a fresh copy replaces the saved one


def test_build_files_folds_twins_keeping_places_and_picture():
    files = build_files([story("b", "Karnataka drought relief for paddy farmers announced", hours=2),
                         story("a", "Karnataka drought relief for paddy farmers", hours=5, states=["ka"], image="https://i/a.jpg", farm=True)], NOW)
    [only] = files["news/india/en.json"]["stories"]
    assert only["id"] == "b" and only["states"] == ["ka"] and only["image"] == "https://i/a.jpg" and only["farm"] is True
    assert [s["id"] for s in files["news/state/ka/en.json"]["stories"]] == ["b"]


def test_national_story_naming_state_lands_in_state_file():
    files = build_files([story("n", "Ragi prices rise", states=["ka"]), story("s", level="state", states=["ka"], lang="kn"),
                         story("w", "Bluetongue spreads in Britain", level="world")], NOW)
    assert [s["id"] for s in files["news/state/ka/en.json"]["stories"]] == ["n"]
    assert [s["id"] for s in files["news/india/en.json"]["stories"]] == ["n"]
    assert [s["id"] for s in files["news/state/ka/kn.json"]["stories"]] == ["s"]
    assert [s["id"] for s in files["news/world/en.json"]["stories"]] == ["w"]
    assert files["news/india/en.json"]["v"] == 1 and files["news/india/en.json"]["built_at"] == "2026-11-06T12:00:00Z"


def test_build_files_newest_first_and_capped():
    files = build_files([story(str(i), f"w{i} x{i} y{i}", hours=i % 70) for i in range(200)], NOW)
    got = files["news/india/en.json"]["stories"]
    assert len(got) == 150 and got == sorted(got, key=lambda s: s["published"], reverse=True)


def test_farm_policy_and_politics_count_as_farm_news():
    farm = lambda title, lang="en": is_farm({"title": title, "summary": "", "lang": lang}, False)  # noqa: E731
    assert farm("Opposition demands legal guarantee on minimum support price in monsoon session")
    assert farm("Centre lifts onion export ban ahead of polls")
    assert farm("State announces loan waiver of Rs 2 lakh")
    assert farm("PM-Kisan 21st instalment released")
    assert farm("Paddy procurement begins in Punjab")
    assert farm("Drought declared in 120 taluks")
    assert farm("తెలంగాణలో రుణమాఫీ నిధులు విడుదల", "te")
    assert farm("धान की सरकारी खरीद शुरू, कर्ज माफी पर फैसला", "hi")
    assert farm("ಭತ್ತಕ್ಕೆ ಬೆಂಬಲ ಬೆಲೆ ಘೋಷಣೆ", "kn")
    assert not farm("Monsoon session of Parliament begins")  # "monsoon" here is not the rain
    assert not farm("Defence procurement deal signed")


def test_indic_crop_words_do_not_match_lookalike_words():
    farm = lambda title, lang: is_farm({"title": title, "summary": "", "lang": lang}, False)  # noqa: E731
    assert not farm("ಕ್ರೀಡೆಯಲ್ಲಿ ಆಸಕ್ತಿ ಬೆಳೆಸಿಕೊಳ್ಳಬೇಕು", "kn")  # ಬೆಳೆಸು "to develop", not ಬೆಳೆ "crop"
    assert not farm("ನಗರ ಬೆಳೆಯುತ್ತಿದ್ದಂತೆ ಸಂಚಾರ ಸಮಸ್ಯೆ", "kn")
    assert farm("ಮಳೆ ಕೊರತೆ, ಬೆಳೆ ನಷ್ಟ", "kn") and farm("ಕಬ್ಬು ಬೆಳೆಗಾರರಿಗೆ ಸಿಹಿ ಸುದ್ದಿ", "kn")
    assert not farm("క్యాన్సర్ కణితి కాదు.. పంటి ముక్క", "te")  # పంటి "tooth", not పంట "crop"
    assert farm("నీళ్లు లేవు.. పంటలు వేయొద్దు", "te")


TELUGU = {"ap": ["ఆంధ్రప్రదేశ్"], "tg": ["తెలంగాణ"], "ap.kurnool": ["కర్నూలు"], "tg.mahbubnagar": ["మహబూబ్‌నగర్"],
          "ap.anantapur": ["అనంతపుర"], "ap.ananthapuramu": ["అనంతపుర"]}
TWO_STATES = [{"district": "ap.kurnool", "state": "ap"}, {"district": "tg.khammam", "state": "tg"}]


def test_telugu_names_tag_districts_inside_inflected_words():
    index = place_index(TWO_STATES, [], TELUGU)
    tag = lambda title: tag_places({"title": title, "summary": "", "districts": [], "states": []}, index)  # noqa: E731
    s = tag("కర్నూలులో వర్షం")
    assert s["districts"] == ["ap.kurnool"] and s["states"] == ["ap"]
    assert tag("మహబూబ్నగర్ రైతులు")["states"] == ["tg"]  # with or without the invisible joiner U+200C
    assert tag("అనంతపురంలో పంట")["districts"] == ["ap.anantapur", "ap.ananthapuramu"]  # two catalog spellings, one place


def test_two_state_feed_story_goes_to_the_state_it_names():
    two = feed(language="te", level="state", states=["ap", "tg"])
    index = place_index(TWO_STATES, [], TELUGU)
    xml = """<rss><channel><item><title>{}</title><link>https://x/a</link><pubDate>Fri, 06 Nov 2026 10:00:00 GMT</pubDate></item></channel></rss>"""
    states = lambda title: settle_states(tag_places(parse_items(xml.format(title).encode(), two)[0], index), two)["states"]  # noqa: E731
    assert states("కర్నూలు రైతులకు సాయం") == ["ap"]
    assert states("తెలంగాణ రైతులకు సాయం") == ["tg"]
    assert states("రైతులకు సాయం") == ["ap", "tg"]  # names neither: both states' readers may want it
    assert states("కర్నూలు, తెలంగాణ రైతులు") == ["ap", "tg"]
    one = feed(language="te", level="state", states=["tg"])
    assert settle_states(tag_places(parse_items(xml.format("రైతులు").encode(), one)[0], index), one)["states"] == ["tg"]


def test_place_names_file_covers_every_andhra_and_telangana_district():
    import yaml

    names = yaml.safe_load((Path(__file__).parent.parent / "farmuncle_pipeline" / "news" / "place_names.yaml").read_text(encoding="utf-8"))
    assert sum(c.startswith("ap.") for c in names) == 31 and sum(c.startswith("tg.") for c in names) == 34
    assert names["ap"] and names["tg"] and all(names.values())
