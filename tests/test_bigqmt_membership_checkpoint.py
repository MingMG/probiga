from datetime import date, datetime
import gzip
import json
from unittest.mock import MagicMock

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from integrations.bigqmt import membership_checkpoint as checkpoint
from integrations.bigqmt import membership_snapshot as snapshot
from tools import run_big_qmt_bridge as consumer
from tools import sync_bigqmt_reference as membership


TARGET = date(2026, 9, 18)
BUILD = "a" * 40
CAPTURED = datetime(2026, 9, 18, 23, 55)
IDENTITY = {"strategy_build_sha": BUILD, "strategy_source_sha256": "b" * 64,
            "compatible_app_build_sha": BUILD, "strategy_compatibility_status": "EXACT_BUILD"}


@pytest.fixture
def capture_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("BIG_QMT_MEMBERSHIP_CAPTURE_DIR", str(tmp_path))
    monkeypatch.setattr(checkpoint, "validate_strategy_release_payload", lambda *_a, **_k: dict(IDENTITY))
    monkeypatch.setattr(consumer, "_assert_membership_runtime", lambda *_a: None)
    monkeypatch.setattr(snapshot, "ensure_membership_snapshot_tables", lambda *_a: {})
    monkeypatch.setattr(membership, "ensure_membership_snapshot_tables", lambda *_a: {})
    for name in ("MIN_CONCEPT_COUNT", "MIN_CONCEPT_RELATION_COUNT", "MIN_CONCEPT_STOCK_COUNT",
                 "MIN_INDUSTRY_RELATION_COUNT", "MIN_INDUSTRY_STOCK_COUNT"):
        monkeypatch.setattr(snapshot, name, 1)
    frames = {table: pd.DataFrame(columns=columns) for table, columns in membership.TARGET_COLUMNS.items()}
    records = {
        "si_all_code": {"stock_code": "600001", "short_name": "Stock", "exchange": "SH", "list_date": "2020-01-01"},
        "si_concept_code_east": {"concept_code": "C001", "name": "Concept"},
        "si_concept_constituent_east": {"concept_code": "C001", "stock_code": "600001", "short_name": "Stock"},
        "si_industry_sw": {"stock_code": "600001", "sw_code": "I001", "industry_name": "Industry", "industry_type": "SW1"},
    }
    for table, row in records.items():
        frames[table] = pd.DataFrame([{column: row.get(column, CAPTURED if column == "etl_sync_at" else "")
                                       for column in membership.TARGET_COLUMNS[table]}])
    capabilities = {"model_instance_id": "model-original"}
    evidence = {
        "collector_build_sha": BUILD, "started_at": "2026-09-18T23:54:00",
        "captured_at": CAPTURED.isoformat(),
        "identity": {**IDENTITY, "model_instance_id": "model-original"},
        "capabilities_before": dict(capabilities), "capabilities_after": dict(capabilities),
    }
    return checkpoint.MembershipCaptureStore(), frames, evidence


def _save(environment):
    store, frames, evidence = environment
    with store.locked():
        return store.save(target=TARGET, frames=frames, counts={"source": "gj_big_qmt_inner"},
                          evidence=evidence, expected_build_sha=BUILD)


def _database():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE si_trade_calendar (trade_date DATE, trade_status INTEGER)"))
        connection.execute(text("INSERT INTO si_trade_calendar VALUES ('2026-09-18', 1)"))
        connection.execute(text("""CREATE TABLE qmt_membership_snapshot_run (
            snapshot_date DATE, source TEXT, quality_status TEXT, capture_mode TEXT,
            concept_count INTEGER, concept_relation_count INTEGER, industry_count INTEGER,
            industry_relation_count INTEGER, concept_hash TEXT, industry_hash TEXT, captured_at DATETIME,
            UNIQUE(snapshot_date, source))"""))
        connection.execute(text("""CREATE TABLE qmt_concept_member_snapshot (
            snapshot_date DATE, source TEXT, concept_code TEXT, concept_name TEXT,
            stock_code TEXT, short_name TEXT, quality_status TEXT, captured_at DATETIME)"""))
        connection.execute(text("""CREATE TABLE qmt_industry_member_snapshot (
            snapshot_date DATE, source TEXT, industry_code TEXT, industry_name TEXT,
            industry_type TEXT, stock_code TEXT, short_name TEXT, quality_status TEXT, captured_at DATETIME)"""))
        for table in membership.TARGET_COLUMNS:
            connection.execute(text(f"CREATE TABLE {table} (sentinel TEXT)"))
            connection.execute(text(f"INSERT INTO {table} VALUES ('newer current state')"))
    return engine


def test_complete_capture_is_durable_and_keeps_original_evidence(capture_environment):
    capture = _save(capture_environment)
    store = checkpoint.MembershipCaptureStore()
    loaded = store.load(TARGET, expected_build_sha=BUILD)
    assert loaded == capture
    assert loaded["evidence"]["captured_at"] == CAPTURED.isoformat()
    assert store.pending_dates() == [TARGET]


def test_failed_database_publish_resumes_after_midnight_without_qmt(monkeypatch, capture_environment):
    capture = _save(capture_environment)
    store, _, _ = capture_environment
    bad_engine = MagicMock()
    bad_engine.connect.side_effect = ConnectionError("database offline")
    monkeypatch.setattr(membership, "_membership_decision_time", lambda now=None: now or datetime(2026, 9, 19, 0, 5))
    with pytest.raises(ConnectionError):
        consumer._run_membership_snapshot(bad_engine, TARGET, expected_build_sha=BUILD)
    assert store.load(TARGET, expected_build_sha=BUILD) == capture
    fetch = MagicMock(side_effect=AssertionError("historical capture must not call QMT"))
    monkeypatch.setattr(membership, "fetch_and_validate", fetch)
    engine = _database()
    result = consumer._run_membership_snapshot(engine, TARGET, expected_build_sha=BUILD)
    assert result["snapshot"]["status"] == "created"
    assert result["snapshot"]["captured_at"] == "2026-09-18 23:55:00"
    assert result["membership_publication_receipt"]["published_at"] == "2026-09-19 00:05:00"
    assert result["current_reference_published"] is False
    assert store.pending_dates() == []
    fetch.assert_not_called()
    with engine.connect() as connection:
        for table in membership.TARGET_COLUMNS:
            assert connection.execute(text(f"SELECT sentinel FROM {table}")).scalar() == "newer current state"


def test_new_capture_is_sealed_before_first_database_publication(monkeypatch, capture_environment):
    store, frames, evidence = capture_environment
    engine = _database()
    clock = {"now": CAPTURED}
    monkeypatch.setattr(membership, "_membership_decision_time", lambda now=None: now or clock["now"])
    fetch = MagicMock(side_effect=lambda _engine, **kwargs: kwargs["on_verified"]((frames, {}), evidence))
    monkeypatch.setattr(membership, "fetch_and_validate", fetch)
    publish = membership.publish_verified_capture
    def fail_after_capture(*_args, **_kwargs):
        assert store.load(TARGET, expected_build_sha=BUILD) is not None
        raise ConnectionError("database failed after source completed")
    monkeypatch.setattr(membership, "publish_verified_capture", fail_after_capture)
    with pytest.raises(ConnectionError):
        consumer._run_membership_snapshot(engine, TARGET, expected_build_sha=BUILD)
    clock["now"] = datetime(2026, 9, 19, 0, 10)
    monkeypatch.setattr(membership, "publish_verified_capture", publish)
    result = consumer._run_membership_snapshot(engine, TARGET, expected_build_sha=BUILD)
    assert result["snapshot"]["status"] == "created"
    assert fetch.call_count == 1


def test_capture_finishing_after_midnight_is_never_sealed(capture_environment):
    store, _, evidence = capture_environment
    evidence["captured_at"] = "2026-09-19T00:00:00"
    with pytest.raises(checkpoint.MembershipCaptureInvalid, match="post-close session"):
        _save(capture_environment)
    assert store.pending_dates() == []


def test_commit_before_process_exit_replays_idempotently(monkeypatch, capture_environment):
    capture = _save(capture_environment)
    engine = _database()
    monkeypatch.setattr(membership, "_membership_decision_time", lambda now=None: now or datetime(2026, 9, 20, 12))
    first = membership.publish_verified_capture(engine, capture, expected_build_sha=BUILD)
    assert first["snapshot"]["status"] == "created"
    # Simulate process exit after DB commit, before local completion/cleanup.
    second = consumer._run_membership_snapshot(engine, TARGET, expected_build_sha=BUILD)
    assert second["snapshot"]["status"] == "idempotent"
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM qmt_membership_snapshot_run")).scalar() == 1


@pytest.mark.parametrize("kind", ["checksum", "model", "source", "contract", "midnight", "before_close", "build", "frame_time"])
def test_untrusted_capture_is_blocked_without_silent_refetch(monkeypatch, capture_environment, kind):
    _save(capture_environment)
    store, _, _ = capture_environment
    path = store.path(TARGET)
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        payload = json.load(stream)
    if kind == "checksum":
        payload["frames"]["si_all_code"]["rows"][0]["short_name"] = "tampered"
    elif kind == "model":
        payload["evidence"]["capabilities_after"]["model_instance_id"] = "other-model"
    elif kind == "source":
        payload["evidence"]["identity"]["strategy_source_sha256"] = "f" * 64
    elif kind == "contract":
        payload["collector_contract_sha256"] = "f" * 64
    elif kind == "midnight":
        payload["evidence"]["captured_at"] = "2026-09-19T00:00:00"
    elif kind == "before_close":
        payload["evidence"]["started_at"] = "2026-09-18T15:09:59"
    elif kind == "build":
        payload["evidence"]["collector_build_sha"] = "c" * 40
    else:
        payload["frames"]["si_all_code"]["rows"][0]["etl_sync_at"] = "2026-09-19T00:00:00"
    if kind != "checksum":
        payload["sha256"] = checkpoint._digest({k: v for k, v in payload.items() if k != "sha256"})
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        json.dump(payload, stream)
    monkeypatch.setattr(membership, "fetch_and_validate", lambda *_a, **_k: pytest.fail("invalid facts recaptured"))
    with pytest.raises(checkpoint.MembershipCaptureInvalid):
        consumer._run_membership_snapshot(MagicMock(), TARGET, expected_build_sha=BUILD)
    assert path.is_file()


def test_atomic_write_failure_preserves_prior_capture(monkeypatch, capture_environment):
    store, _, _ = capture_environment
    monkeypatch.setattr(checkpoint, "_replace_with_retry", lambda *_a: (_ for _ in ()).throw(OSError("disk unavailable")))
    with pytest.raises(OSError):
        _save(capture_environment)
    assert not store.path(TARGET).exists()
    assert not list(store.root.glob(".writing-*"))


def test_existing_capture_never_overwritten(capture_environment):
    first = _save(capture_environment)
    store, frames, evidence = capture_environment
    frames["si_all_code"].loc[0, "short_name"] = "changed"
    with store.locked(), pytest.raises(checkpoint.MembershipCaptureInvalid, match="immutable"):
        store.save(target=TARGET, frames=frames, counts={}, evidence=evidence, expected_build_sha=BUILD)
    assert store.load(TARGET, expected_build_sha=BUILD) == first


def test_cross_release_replay_requires_unchanged_collector_and_source(monkeypatch, capture_environment):
    capture = _save(capture_environment)
    store, _, _ = capture_environment
    newer_build = "c" * 40
    def compatible(_payload, *, expected_build_sha, **_kwargs):
        return {**IDENTITY, "compatible_app_build_sha": expected_build_sha,
                "strategy_compatibility_status": "CONTENT_COMPATIBLE"}
    monkeypatch.setattr(checkpoint, "validate_strategy_release_payload", compatible)
    assert store.load(TARGET, expected_build_sha=newer_build) == capture
    monkeypatch.setattr(checkpoint, "validate_strategy_release_payload",
                        MagicMock(side_effect=RuntimeError("strategy content differs")))
    with pytest.raises(checkpoint.MembershipCaptureInvalid, match="strategy content differs"):
        store.load(TARGET, expected_build_sha=newer_build)
    assert store.path(TARGET).is_file()


def test_default_cli_captures_today_before_corrupt_old_pending(monkeypatch, capture_environment):
    store, frames, evidence = capture_environment
    bad_file = store.path(date(2026, 9, 17))
    bad_file.write_bytes(b"corrupted old capture")
    engine = _database()
    monkeypatch.setattr(membership.sys, "argv", ["sync_bigqmt_reference.py", "--apply", "--expected-build-sha", BUILD])
    monkeypatch.setattr(membership, "load_project_env", lambda: None)
    monkeypatch.setattr(membership, "create_tool_engine", lambda **_k: engine)
    monkeypatch.setattr(membership, "_membership_decision_time", lambda now=None: now or CAPTURED)
    monkeypatch.setattr(consumer, "_membership_snapshot_exists", lambda *_a, **_k: False)
    fetch = MagicMock(side_effect=lambda _engine, **kwargs: kwargs["on_verified"]((frames, {}), evidence))
    monkeypatch.setattr(membership, "fetch_and_validate", fetch)
    monkeypatch.setattr(membership, "publish_verified_capture", MagicMock(side_effect=ConnectionError("DB offline after capture")))
    with pytest.raises(ConnectionError, match="after capture"):
        membership.main()
    assert fetch.call_count == 1
    assert store.load(TARGET, expected_build_sha=BUILD) is not None
    assert bad_file.read_bytes() == b"corrupted old capture"
