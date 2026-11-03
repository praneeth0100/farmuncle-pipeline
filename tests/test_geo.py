from farmuncle_pipeline.ingestion import geo


def mandi(pid, name, state, lat=None, lng=None, status="ACTIVE"):
    return {"public_id": pid, "name": name, "state": state, "latitude": lat, "longitude": lng, "status": status,
            "merged_into_id": None}


def test_fill_coords_uses_district_then_state_centre():
    coords = geo.fill_coords([
        mandi("ka.kolar.a", "A", "Karnataka", 13.0, 78.0), mandi("ka.kolar.b", "B", "Karnataka", 13.2, 78.2),
        mandi("ka.kolar.c", "C", "Karnataka"),  # no coordinates, district has two
        mandi("ka.mysuru.d", "D", "Karnataka"),  # no coordinates, alone in its district
    ])
    assert coords["ka.kolar.a"] == (13.0, 78.0, False)
    lat, lng, approx = coords["ka.kolar.c"]
    assert (round(lat, 2), round(lng, 2), approx) == (13.1, 78.1, True)
    lat, lng, approx = coords["ka.mysuru.d"]
    assert (round(lat, 2), round(lng, 2), approx) == (13.1, 78.1, True)  # state centre


def test_fill_coords_skips_a_state_with_no_coordinates_at_all():
    assert "tn.x.y" not in geo.fill_coords([mandi("tn.x.y", "Y", "Tamil Nadu")])


def test_labels_disambiguate_same_name():
    out = geo.labels([mandi("ka.bengaluru.bengaluru", "Bengaluru APMC", "Karnataka"),
                      mandi("ka.bengaluru.binny_mill_ff_v_bengaluru", "Bengaluru", "Karnataka"),
                      mandi("ka.kolar.kolar", "Kolar", "Karnataka")])
    assert out == {"ka.bengaluru.bengaluru": "Bengaluru", "ka.bengaluru.binny_mill_ff_v_bengaluru": "Bengaluru · Binny Mill",
                   "ka.kolar.kolar": "Kolar"}


def test_neighbours_within_200_km_sorted():
    coords = {"home": (13.0, 78.0, False), "near": (13.1, 78.0, False), "mid": (14.0, 78.0, False), "far": (16.0, 78.0, False)}
    out = geo.neighbours(coords, km=200)
    assert out["home"] == [("near", 11), ("mid", 111)]
    assert [m for m, _ in out["far"]] == []
