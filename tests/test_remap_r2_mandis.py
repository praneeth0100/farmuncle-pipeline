import pyarrow as pa

from farmuncle_pipeline.ops.remap_r2_mandis import remap_table, survivors


def test_survivors_follow_a_merge_chain_to_the_active_mandi():
    rows = [{"id": 1, "status": "MERGED", "merged_into_id": 2}, {"id": 2, "status": "MERGED", "merged_into_id": 3},
            {"id": 3, "status": "ACTIVE", "merged_into_id": None}, {"id": 4, "status": "MERGED", "merged_into_id": 99}]
    assert survivors(rows) == {1: 3, 2: 3}  # 4 points nowhere active: left alone


def test_remap_keeps_every_row_and_remembers_the_archived_id():
    t = pa.table({"mandi_id": [1, 3, 5], "source_mandi_id": pa.array([None, None, 7], type=pa.int64()), "modal_price": [10, 20, 30]})
    new, changed = remap_table(t, {1: 3, 5: 3})
    assert changed == 2 and new.num_rows == 3
    assert new.column("mandi_id").to_pylist() == [3, 3, 3]
    assert new.column("source_mandi_id").to_pylist() == [1, None, 7]  # an existing source id is kept


def test_remap_handles_an_all_null_source_column():
    t = pa.table({"mandi_id": [1], "source_mandi_id": pa.array([None], type=pa.null())})
    new, _ = remap_table(t, {1: 3})
    assert new.column("source_mandi_id").to_pylist() == [1]


def test_drop_merge_copies_removes_only_remapped_rows_that_repeat_a_record():
    from farmuncle_pipeline.ops.remap_r2_mandis import drop_merge_copies

    t = pa.table({"mandi_id": [3, 3, 3, 3], "crop_id": [1, 1, 2, 1], "variety_id": [1, 1, 1, 1], "grade_id": [1, 1, 1, 1],
                  "price_date": ["d"] * 4, "modal_price": [10, 10, 20, 11], "min_price": [9] * 4, "max_price": [12] * 4})
    remapped = [False, True, True, True]  # row 0 was already mandi 3; rows 1-3 came from a merged mandi
    out, dropped = drop_merge_copies(t, remapped)
    assert dropped == 1  # row 1 repeats row 0 exactly
    assert out.column("modal_price").to_pylist() == [10, 20, 11]  # a different price is kept as its own row


def test_drop_merge_copies_keeps_repeats_that_were_already_in_the_file():
    from farmuncle_pipeline.ops.remap_r2_mandis import drop_merge_copies

    t = pa.table({"mandi_id": [3, 3], "crop_id": [1, 1], "variety_id": [1, 1], "grade_id": [1, 1],
                  "price_date": ["d", "d"], "modal_price": [10, 10], "min_price": [9, 9], "max_price": [12, 12]})
    out, dropped = drop_merge_copies(t, [False, False])
    assert dropped == 0 and out.num_rows == 2
