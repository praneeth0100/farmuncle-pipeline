"""Copy the app map's fonts and India's terrain tiles into R2 under map/, and publish the app's map style. Run by hand.

The app's map (Screen 2) otherwise loads its fonts from Protomaps' GitHub pages and its hill shading from the public
Mapzen terrain bucket on AWS, neither ours. Re-runnable: files already in R2 are skipped.

Usage:
    python -m farmuncle_pipeline.ops.publish_map_assets [--maxzoom 10] [--style map/style.json]
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import math
import pathlib
import urllib.error
import urllib.parse
import urllib.request

from farmuncle_pipeline.archive.store import R2Store

FONTS = ("Noto Sans Regular", "Noto Sans Medium", "Noto Sans Italic")  # the fontstacks the app's style names
GLYPHS = "https://protomaps.github.io/basemaps-assets/fonts"
TERRAIN = "https://s3.amazonaws.com/elevation-tiles-prod/terrarium"
INDIA = (68.0, 6.0, 97.5, 37.1)  # what the app's map file covers: [west, south, east, north]
FOREVER = "public, max-age=31536000, immutable"  # a changed file gets a new key (style-v2, ...); these never change


def tile_range(z: int, west: float, south: float, east: float, north: float) -> tuple[range, range]:
    """The x and y tile numbers (slippy-map scheme) covering a box at zoom z."""
    n = 2 ** z
    x = lambda lon: min(n - 1, int((lon + 180) / 360 * n))
    y = lambda lat: min(n - 1, int((1 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2 * n))
    return range(x(west), x(east) + 1), range(y(north), y(south) + 1)


def jobs(maxzoom: int) -> list[tuple[str, str, str]]:
    """(R2 key, source URL, content type) for every font range and every India terrain tile up to maxzoom."""
    out = [(f"map/fonts/{stack}/{lo}-{lo + 255}.pbf", f"{GLYPHS}/{urllib.parse.quote(stack)}/{lo}-{lo + 255}.pbf", "application/x-protobuf")
           for stack in FONTS for lo in range(0, 65536, 256)]
    for z in range(maxzoom + 1):
        xs, ys = tile_range(z, *INDIA)
        out += [(f"map/terrain/{z}/{x}/{y}.png", f"{TERRAIN}/{z}/{x}/{y}.png", "image/png") for x in xs for y in ys]
    return out


def fetch(url: str) -> bytes | None:
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "AgroUncle map assets"}), timeout=60) as r:
            return r.read()
    except urllib.error.HTTPError as exc:
        if exc.code in (403, 404):  # a font range nobody uses, or a tile past the data: nothing to copy
            return None
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--maxzoom", type=int, default=10)
    parser.add_argument("--style", default="map/style.json", help="the app's map style (generated in the app repo)")
    args = parser.parse_args()

    store = R2Store.from_env(); store.selfcheck()
    have = set(store.list("map/"))
    todo = [j for j in jobs(args.maxzoom) if j[0] not in have]
    print(f"[map] {len(todo)} files to copy ({len(have)} already in R2)")

    def copy(job: tuple[str, str, str]) -> bool:
        key, url, kind = job; data = fetch(url)
        if data is None:
            return False
        store._client.put_object(Bucket=store.bucket, Key=key, Body=data, ContentType=kind, CacheControl=FOREVER)
        return True

    with cf.ThreadPoolExecutor(32) as pool:
        done = sum(pool.map(copy, todo))
    print(f"[map] copied {done}, skipped {len(todo) - done} (not served at the source)")

    style = pathlib.Path(args.style)
    if style.exists():
        store._client.put_object(Bucket=store.bucket, Key=f"map/{style.stem}.json", Body=style.read_bytes(),
                                 ContentType="application/json; charset=utf-8", CacheControl=FOREVER)
        print(f"[map] style published as map/{style.stem}.json")


if __name__ == "__main__":
    main()
