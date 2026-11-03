from farmuncle_pipeline.ingestion.quality import odd_keys


def row(mandi, modal, lo=None, hi=None, crop="tomato", state="ka"):
    return {"key": (mandi, crop, f"{crop}.local", "faq"), "state": state, "modal": modal,
            "min": modal - 2 if lo is None else lo, "max": modal + 2 if hi is None else hi}


def test_modal_outside_range_is_odd():
    assert odd_keys([row("a", 30, lo=10, hi=20)]) == {("a", "tomato", "tomato.local", "faq")}


def test_three_rupee_tomato_is_odd():
    rows = [row(m, p) for m, p in zip("abcdef", (20, 22, 24, 26, 23, 21))] + [row("g", 3)]
    assert odd_keys(rows) == {("g", "tomato", "tomato.local", "faq")}


def test_fewer_than_five_rows_never_flagged_by_spread():
    assert odd_keys([row("a", 20), row("b", 22), row("c", 21), row("d", 3)]) == set()


def test_other_states_are_compared_separately():
    rows = [row(m, 20 + i) for i, m in enumerate("abcde")] + [row(m, 60 + i, state="tn") for i, m in enumerate("fghij")]
    assert odd_keys(rows) == set()
