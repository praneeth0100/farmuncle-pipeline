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
