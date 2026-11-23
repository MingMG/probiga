from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError

from server.common.formal_analysis_store import (
    FormalAnalysisArtifactIntegrityError,
    FormalAnalysisArtifactMissing,
    FormalAnalysisConflict,
    FormalAnalysisStore,
)
from server.db.formal_analysis_schema import (
    ARTIFACT_TABLE,
    MEMBER_TABLE,
    privileged_migrate_formal_analysis_schema,
)


GENERATION = "a" * 64
OTHER_GENERATION = "b" * 64


@pytest.fixture
def engine(tmp_path):
    db = create_engine(f"sqlite:///{tmp_path / 'formal-analysis.db'}")
    privileged_migrate_formal_analysis_schema(db)
    yield db
    db.dispose()


def test_restart_reuses_verified_objects_without_merging_generation_members(engine):
    store = FormalAnalysisStore(engine)
    payload = {"多头": ["000001", 2.0], "factor": {"p": None}}
    one_generation = store.put_many(GENERATION, "factor", {"B": payload, "A": payload})
    other = store.put(OTHER_GENERATION, "factor", "A", dict(reversed(list(payload.items()))))
    different = store.put(GENERATION, "score", "A", {"score": -1.25})
    assert list(one_generation) == ["A", "B"]
    assert one_generation == {"A": other, "B": other}
    with engine.connect() as connection:
        assert connection.execute(text(f"SELECT COUNT(*) FROM {ARTIFACT_TABLE}")).scalar_one() == 2
        assert connection.execute(text(f"SELECT COUNT(*) FROM {MEMBER_TABLE}")).scalar_one() == 4

    engine.dispose()
    restarted_engine = create_engine(engine.url)
    try:
        restarted = FormalAnalysisStore(restarted_engine)
        assert restarted.list_members(GENERATION, "factor") == one_generation
        assert restarted.list_members(OTHER_GENERATION, "factor") == {"A": other}
        assert restarted.get(GENERATION, "factor", "A") == payload
        assert restarted.get(OTHER_GENERATION, "score", "A") is None
        assert restarted.load_artifacts([different, other, other]) == {different: {"score": -1.25}, other: payload}
        assert restarted.put(GENERATION, "factor", "A", payload) == other
    finally:
        restarted_engine.dispose()


def test_conflict_is_atomic_and_caller_controls_transaction(engine):
    store = FormalAnalysisStore(engine)
    old = store.put(GENERATION, "score", "existing", {"winner": 1})
    with engine.begin() as connection:
        bound = FormalAnalysisStore(connection)
        with pytest.raises(FormalAnalysisConflict):
            bound.put_many(GENERATION, "score", {"new": [2], "existing": {"winner": 2}})
        assert bound.list_members(GENERATION, "score") == {"existing": old}
        bound.put(GENERATION, "score", "committed", {"score": 10})
    with engine.connect() as connection:
        transaction = connection.begin()
        bound = FormalAnalysisStore(connection)
        bound.put(GENERATION, "score", "rolled-back", {"score": 11})
        assert bound.get(GENERATION, "score", "rolled-back") == {"score": 11}
        transaction.rollback()
    assert store.get(GENERATION, "score", "rolled-back") is None
    assert store.get(GENERATION, "score", "committed") == {"score": 10}
    with engine.connect() as connection:
        assert connection.execute(text(f"SELECT COUNT(*) FROM {ARTIFACT_TABLE}")).scalar_one() == 2


def test_bulk_checkpoint_read_reuses_content_and_preserves_missing_keys(engine):
    store=FormalAnalysisStore(engine)
    payloads={'A':{'rows':[1,2]},'B':{'rows':[1,2]},'C':{'rows':[3]}}
    store.put_many(GENERATION,'metadata',payloads)
    store.put(OTHER_GENERATION,'metadata','A',{'other':True})
    assert store.get_many(GENERATION,'metadata',['missing','B','A','C','A'])==payloads
    assert store.get_many(GENERATION,'different',['A'])=={}
    assert store.get_many(OTHER_GENERATION,'metadata',['A'])=={'A':{'other':True}}


def test_bulk_checkpoint_read_enforces_combined_decoded_byte_budget(engine,monkeypatch):
    from server.common import formal_analysis_store as module
    store=FormalAnalysisStore(engine)
    store.put_many(GENERATION,'metadata',{'A':{'v':'a'*50},'B':{'v':'b'*50}})
    monkeypatch.setattr(module,'_BATCH_MAX_BYTES',80)
    assert store.get_many(GENERATION,'metadata',['A'])=={'A':{'v':'a'*50}}
    with pytest.raises(FormalAnalysisArtifactIntegrityError,match='byte limit'):
        store.get_many(GENERATION,'metadata',['A','B'])


def test_competing_frozen_cutoffs_converge_to_one_durable_value(engine):
    barrier = Barrier(2, timeout=5)

    def freeze(cutoff):
        barrier.wait()
        return FormalAnalysisStore(engine).get_or_put(GENERATION, "cutoff", "frozen", {"cutoff": cutoff})

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(freeze, "15:10")
        second = pool.submit(freeze, "15:11")
        received = first.result(timeout=10)
        assert received == second.result(timeout=10)
    engine.dispose()
    store = FormalAnalysisStore(engine)
    assert store.get_or_put(GENERATION, "cutoff", "frozen", {"cutoff": "15:12"}) == received
    with engine.connect() as connection:
        assert connection.execute(text(f"SELECT COUNT(*) FROM {ARTIFACT_TABLE}")).scalar_one() == 1


def test_corrupted_and_dangling_artifacts_are_distinct_from_absent_keys(engine):
    store = FormalAnalysisStore(engine)
    digest = store.put(GENERATION, "score", "tampered", {"score": 9})
    with engine.begin() as connection:
        connection.execute(text(f"UPDATE {ARTIFACT_TABLE} SET payload_zlib=x'ABCDEF' WHERE artifact_sha256=:digest"),
                           {"digest": digest})
        connection.execute(text(f"""
            INSERT INTO {MEMBER_TABLE} (generation_sha256,stage,unit_key,artifact_sha256)
            VALUES (:generation,'score','dangling',:digest)
        """), {"generation": GENERATION, "digest": "0" * 64})
    assert store.get(GENERATION, "score", "absent") is None
    with pytest.raises(FormalAnalysisArtifactIntegrityError):
        store.get(GENERATION, "score", "dangling")
    with pytest.raises(FormalAnalysisArtifactIntegrityError):
        store.load_artifact(digest)
    with pytest.raises(FormalAnalysisArtifactMissing):
        store.load_artifact("0" * 64)


def test_invalid_inputs_and_missing_schema_do_not_write(engine):
    store = FormalAnalysisStore(engine)
    for payload in (None, float("nan"), {1: "non-string key"}, {"arbitrary": (1, 2)}, "a" * (15 * 1024 * 1024)):
        with pytest.raises(ValueError):
            store.put(GENERATION, "score", "invalid", payload)
    with pytest.raises(ValueError):
        store.put_many(GENERATION, "score", {f"key-{i}": i for i in range(257)})
    with pytest.raises(ValueError):
        store.load_artifacts([GENERATION] * 9)
    assert store.list_members(GENERATION, "score") == {}

    without_schema = create_engine("sqlite://")
    try:
        with pytest.raises(OperationalError):
            FormalAnalysisStore(without_schema).put(GENERATION, "score", "first", {"score": 0})
        assert inspect(without_schema).get_table_names() == []
    finally:
        without_schema.dispose()
