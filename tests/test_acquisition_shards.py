import json

from server.common.acquisition_shards import AcquisitionShards


def test_restart_reuses_verified_shards_but_not_changed_scope(tmp_path):
    first = AcquisitionShards("daily", {"date": "2026-09-14"}, root=tmp_path)
    first.save(["000001"], {"close": 1.2345678901234567})
    restarted = AcquisitionShards("daily", {"date": "2026-09-14"}, root=tmp_path)
    assert restarted.load(["000001"]) == {"close": 1.2345678901234567}
    assert restarted.load(["000002"]) is None
    changed = AcquisitionShards("daily", {"date": "2026-09-15"}, root=tmp_path)
    assert changed.load(["000001"]) is None


def test_corrupt_completed_shard_is_not_reused(tmp_path):
    store = AcquisitionShards("daily", {}, root=tmp_path)
    store.save("a", {"rows": 12})
    path = store._path("a")
    record = json.loads(path.read_bytes())
    record["payload"]["rows"] = 13
    path.write_text(json.dumps(record))
    assert store.load("a") is None
    store.save("a", {"rows": 14})
    assert store.load("a") == {"rows": 14}
