from farmuncle_pipeline.ops.publish_map_assets import INDIA, jobs, tile_range


def test_tile_range_covers_india_at_low_zoom():
    assert tile_range(0, *INDIA) == (range(0, 1), range(0, 1))
    xs, ys = tile_range(4, *INDIA)
    assert (xs.start, xs.stop, ys.start, ys.stop) == (11, 13, 6, 8)  # India sits in tiles x 11-12, y 6-7 at zoom 4


def test_jobs_are_every_font_range_and_india_terrain_without_repeats():
    js = jobs(6)
    keys = [k for k, _, _ in js]
    assert len(keys) == len(set(keys))
    assert sum(k.startswith("map/fonts/") for k in keys) == 3 * 256
    assert "map/fonts/Noto Sans Regular/0-255.pbf" in keys
    assert all(u.startswith("https://") for _, u, _ in js)
