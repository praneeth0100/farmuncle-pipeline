"""Pure news logic (see docs/news.md): publisher RSS/Atom → farm stories with pictures and place codes → per-file payloads.

No network and no R2 here; publish_news.py does the fetching and writing.
"""

from __future__ import annotations

import hashlib
import html
import re
import time
import unicodedata
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import TypedDict

import feedparser


class Feed(TypedDict, total=False):
    id: str
    url: str
    outlet: str
    language: str  # en hi kn te ta ml
    level: str  # national | state | city | world
    states: list[str]  # state codes from catalog/mandis.json
    districts: list[str]  # city feeds: the district codes the city sits in
    farm_only: bool  # an agriculture section: every item is farm news
    upgrade_images: bool  # the feed lists http:// pictures that its host also serves over https


# From the app's old googleNews.ts. Substring match on purpose: Indic words inflect at the end ("ರೈತ" catches "ರೈತರಿಗೆ").
AGRI_TERMS: dict[str, list[str]] = {
    "en": ["agricultur", "agrarian", "farm", "crop", r"mandis?\b", r"monsoon\b(?! session)", "harvest", "kisan", r"msp\b",
           "fertiliser", "fertilizer", "irrigation", "drought", "paddy", "sugarcane", "rythu",
           # Farm policy and politics that reach every farmer: support prices, loan waivers, PM-Kisan, export bans, procurement.
           "minimum support price", "loan waiver", r"pm[- ]kisan", "fasal bima", r"(onion|rice|wheat|sugar) exports?\b",
           r"export ban on (onion|rice|wheat|sugar)", r"(paddy|wheat|grain|crop) procurement"],
    "hi": ["कृषि", "किसान", "फसल", "मंडी", "खेती", "एमएसपी", "समर्थन मूल्य", "कर्ज माफी", "कर्जमाफी", "धान", "गेहूं", "गन्ना",
           "उर्वरक", "सिंचाई", "सूखा"],
    "te": ["వ్యవసాయం", "రైతు", "పంట(?!ి)","మద్దతు ధర", "రుణమాఫీ", "ధాన్యం", "ఎరువు", "కరువు"],
    "kn": ["ಕೃಷಿ", "ರೈತ", "ಬೆಳೆ(?![ಸಯದ])","ಬೆಂಬಲ ಬೆಲೆ", "ಸಾಲ ಮನ್ನಾ", "ಭತ್ತ", "ಕಬ್ಬು", "ರಸಗೊಬ್ಬರ", "ನೀರಾವರಿ"],
    "ta": ["விவசாய", "பயிர்", "ஆதரவு விலை", "கடன் தள்ளுபடி", "கரும்பு", "பாசன"],
    "ml": ["കൃഷി", "കർഷക", "വിള", "താങ്ങുവില", "നെല്ല്", "റബ്ബർ", "ജലസേചന"],
}
BLOCK_TERMS: dict[str, list[str]] = {
    "en": ["sensex", "nifty", "stock market", "share price", "cricket", "film", "movie", "bollywood", "box office", "farmhouse"],
    "hi": ["सेंसेक्स", "निफ्टी", "शेयर बाजार", "क्रिकेट", "फिल्म"],
    "te": ["సెన్సెక్స్", "నిఫ్టీ", "స్టాక్ మార్కెట్", "క్రికెట్", "సినిమా"],
    "kn": ["ಸೆನ್ಸೆಕ್ಸ್", "ನಿಫ್ಟಿ", "ಷೇರು", "ಕ್ರಿಕೆಟ್", "ಸಿನಿಮಾ"],
    "ta": ["சென்செக்ஸ்", "நிஃப்டி", "பங்குச் சந்தை", "கிரிக்கெட்", "சினிமா", "திரைப்பட"],
    "ml": ["സെൻസെക്സ്", "നിഫ്റ്റി", "ഓഹരി", "ക്രിക്കറ്റ്", "സിനിമ"],
}
# The published catalog spells some states unlike the press does.
STATE_ALIASES = {"kl": ["kerala"], "cg": ["chhattisgarh"], "dl": ["delhi"], "od": ["orissa"], "uk": ["uttaranchal"]}
# District-name words that are also everyday words or people's names, so alone they name no district.
NOT_PLACES = {"erode", "anand", "sagar", "krishna", "lower", "middle", "upper", "paschim", "purba", "uttara", "dakshina", "reddy", "gautam", "kabir", "ranga",
              "siddharth", "chhota", "dehat", "thana", "champa", "yamuna", "dwarka", "malwa", "sathya", "sitharama", "alluri",
              "somnath", "manpur", "khiri", "devbhumi", "sounth", "chattrapati", "jayashankar", "rajanna", "baroda"}
STORY_KEYS = ("id", "title", "link", "outlet", "published", "lang", "image", "level", "districts", "states", "farm")
KEEP_HOURS, MAX_STORIES, MAX_WIDTH = 72, 150, 800
_ISO = "%Y-%m-%dT%H:%M:%SZ"


def _text(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", value or ""))).strip()


def _https(url: str | None, upgrade: bool = False) -> str | None:
    if url and upgrade and url.startswith("http://"):
        url = "https://" + url[7:]
    return url if url and url.startswith("https://") else None


def picture(entry, upgrade: bool = False) -> str | None:
    """media:content / media:thumbnail (largest up to 800 px wide), then an image enclosure, then the first <img>."""
    media = [m for m in (entry.get("media_content") or []) + (entry.get("media_thumbnail") or [])
             if _https(m.get("url"), upgrade) and m.get("medium", "image") == "image" and m.get("type", "image/").startswith("image/")]
    if media:
        width = lambda m: int(m.get("width") or 0) if str(m.get("width") or "0").isdigit() else 0  # noqa: E731
        fits = [m for m in media if width(m) <= MAX_WIDTH]
        best = max(fits, key=width) if fits else min(media, key=width)
        return _https(best["url"], upgrade)
    for enc in entry.get("enclosures") or []:
        if (enc.get("type") or "").startswith("image/") and _https(enc.get("href"), upgrade):
            return _https(enc["href"], upgrade)
    for body in [c.get("value", "") for c in entry.get("content") or []] + [entry.get("summary", "")]:
        for src in re.findall(r"<img[^>]+src=[\"']([^\"']+)", body or ""):
            if _https(src, upgrade):
                return _https(html.unescape(src), upgrade)
    return None


def parse_items(xml: bytes, feed: Feed) -> list[dict]:
    """Stories in the published shape plus a transient "summary" (used for the farm filter and place tags)."""
    out = []
    for e in feedparser.parse(xml).entries:
        title, link = _text(e.get("title", "")), (e.get("link") or "").strip()
        stamp = e.get("published_parsed") or e.get("updated_parsed")
        if not title or not link.startswith("https://") or not stamp:
            continue
        out.append({"id": hashlib.sha1(link.encode()).hexdigest(), "title": title, "link": link, "outlet": feed["outlet"],
                    "published": time.strftime(_ISO, stamp), "lang": feed["language"], "image": picture(e, feed.get("upgrade_images", False)),
                    "level": feed["level"], "districts": list(feed.get("districts", [])), "states": list(feed.get("states", [])) if len(feed.get("states", [])) == 1 else [],  # settle_states decides for two-state feeds
                    "farm": bool(feed.get("farm_only")), "summary": _text(e.get("summary", ""))[:500]})
    return out


def _has(text: str, terms: list[str], latin: bool) -> bool:
    # All terms are regex stems. English ones must start a word ("farm" is not in "pharmacy"; "mandi" must also end one,
    # or "Mandir" matches). Indic ones match anywhere, since words inflect at the end, except lookalikes they exclude
    # (ಬೆಳೆಸು "to develop" is not ಬೆಳೆ "crop"; పంటి "tooth" is not పంట "crop").
    return any(re.search((r"\b" if latin else "") + t, text) for t in terms)


def is_farm(story: dict, farm_only: bool) -> bool:
    if farm_only:
        return True
    text, lang = f"{story['title']} {story.get('summary', '')}".lower(), story["lang"]
    if _has(text, BLOCK_TERMS["en"], True) or (lang != "en" and _has(text, BLOCK_TERMS.get(lang, []), False)):
        return False
    return _has(text, AGRI_TERMS["en"], True) or (lang != "en" and _has(text, AGRI_TERMS.get(lang, []), False))


class PlaceIndex(TypedDict):
    pattern: re.Pattern | None
    codes: dict[str, tuple[str, str | None]]  # lower-case name → (state code, district code or None)
    native: list[tuple[str, str, str | None]]  # regional-script name, state code, district code or None


def _plain(text: str) -> str:
    # Indic text carries invisible joiners in some spellings of the same word (మహబూబ్‌నగర్).
    return text.replace("‌", "").replace("‍", "")


def place_index(mandis: list[dict], states: list[dict], native: dict[str, list[str]] | None = None) -> PlaceIndex:
    """English names from the codes: "ka.bengaluru_urban" → "bengaluru urban", plus each 5+ letter word only one district has.
    `native` (place_names.yaml) adds regional-script names: code → names."""
    codes: dict[str, tuple[str, str | None]] = {}
    districts = {m["district"]: m["state"] for m in mandis if m.get("district") and m.get("state")}
    owners: dict[str, set[str]] = defaultdict(set)
    for code in districts:
        for word in code.split(".", 1)[1].split("_"):
            owners[word].add(code)
    full_states: dict[str, set[str]] = defaultdict(set)
    for code, state in districts.items():
        full_states[" ".join(code.split(".", 1)[1].split("_"))].add(state)
    for code, state in districts.items():
        words = code.split(".", 1)[1].split("_")
        name = " ".join(words)
        if len(full_states[name]) == 1 and name not in NOT_PLACES:  # Hamirpur is in two states: alone it names neither
            codes.setdefault(name, (state, code))
        for word in words:
            if len(word) >= 5 and owners[word] == {code} and word not in NOT_PLACES:
                codes.setdefault(word, (state, code))
    for s in states:
        for name in [s["name"].lower(), *STATE_ALIASES.get(s["id"], [])]:
            codes[name] = (s["id"], None)  # a state name wins over a district word of the same spelling
    names = sorted(codes, key=len, reverse=True)
    return {"pattern": re.compile(r"\b(" + "|".join(map(re.escape, names)) + r")\b") if names else None, "codes": codes,
            "native": [(_plain(name), code.split(".")[0], code if "." in code else None) for code, names in (native or {}).items() for name in names]}


def tag_places(story: dict, index: PlaceIndex) -> dict:
    found_d, found_s = set(story["districts"]), set(story["states"])
    text = f"{story['title']} {story.get('summary', '')}"
    if index["pattern"]:
        for name in index["pattern"].findall(text.lower()):
            state, district = index["codes"][name]
            found_s.add(state)
            if district:
                found_d.add(district)
    plain = _plain(text)
    for name, state, district in index["native"]:
        if name in plain:
            found_s.add(state)
            if district:
                found_d.add(district)
    return {**story, "districts": sorted(found_d), "states": sorted(found_s)}


def settle_states(story: dict, feed: Feed) -> dict:
    """A feed covering several states: the story goes to those it names, or to all of them when it names none."""
    covers = feed.get("states", [])
    if len(covers) < 2 or set(story["states"]) & set(covers):
        return story
    return {**story, "states": sorted({*story["states"], *covers})}


def _words(text: str) -> set[str]:
    # Keep letters, marks and digits: \w would split Indic words at their vowel signs.
    kept = "".join(c if c.isspace() or unicodedata.category(c)[0] in "LMN" else " " for c in text.lower())
    return {w for w in kept.split() if len(w) > 2}


def same_story(a: str, b: str) -> bool:
    x, y = _words(a), _words(b)
    return bool(x and y) and len(x & y) / len(x | y) >= 0.5


def merge(saved: list[dict], fresh: list[dict], now: datetime) -> list[dict]:
    """The saved state: by id (fresh copy wins), times capped at now, older than 72 h dropped, newest first. Not folded,
    so a story whose twin leaves its feed keeps its own places and picture."""
    stamp, cutoff = now.astimezone(timezone.utc).strftime(_ISO), (now - timedelta(hours=KEEP_HOURS)).astimezone(timezone.utc).strftime(_ISO)
    by_id = {s["id"]: {**s, "published": min(s["published"], stamp)} for s in [*saved, *fresh]}
    return sorted((s for s in by_id.values() if s["published"] >= cutoff), key=lambda s: s["published"], reverse=True)


def fold(stories: list[dict]) -> list[dict]:
    """The same story from two outlets (same language, headline-word overlap >= 0.5) becomes its newest copy,
    with both copies' places, the first picture and farm if either is."""
    out: list[dict] = []
    kept_words: list[set[str]] = []
    for s in sorted(stories, key=lambda s: s["published"], reverse=True):
        w = _words(s["title"])
        twin = next((i for i, k in enumerate(out) if k["lang"] == s["lang"] and w and kept_words[i]
                     and len(w & kept_words[i]) / len(w | kept_words[i]) >= 0.5), None)
        if twin is None:
            out.append(dict(s))
            kept_words.append(w)
            continue
        k = out[twin]
        k["districts"], k["states"] = sorted({*k["districts"], *s["districts"]}), sorted({*k["states"], *s["states"]})
        k["image"], k["farm"] = k["image"] or s["image"], k["farm"] or s["farm"]
    return out


def build_files(stories: list[dict], now: datetime) -> dict[str, dict]:
    """R2 key → payload. A national story naming a state is also written into that state's file."""
    files: dict[str, list[dict]] = defaultdict(list)
    for s in fold(stories):
        row = {k: s[k] for k in STORY_KEYS}
        if s["level"] == "world":
            files[f"news/world/{s['lang']}.json"].append(row)
            continue
        if s["level"] == "national":
            files[f"news/india/{s['lang']}.json"].append(row)
        for state in s["states"]:
            files[f"news/state/{state}/{s['lang']}.json"].append(row)
    built = now.astimezone(timezone.utc).strftime(_ISO)
    return {key: {"v": 1, "built_at": built, "stories": rows[:MAX_STORIES]} for key, rows in files.items()}
