from farmuncle_pipeline.ops import sweep_duplicates as sd


class Rpc:
    def __init__(self, data):
        self.data = data

    def execute(self):
        return self


class DB:
    def __init__(self):
        self.called = []

    def rpc(self, name):
        self.called.append(name)
        return Rpc(len(self.called))


def test_sweep_runs_mandi_crop_and_variety_checks_only():
    db = DB()
    assert sd.sweep(db) == {"find_mandi_duplicates": 1, "sweep_duplicate_crops": 2, "sweep_duplicate_varieties": 3}
    assert "merge_entity" not in db.called
