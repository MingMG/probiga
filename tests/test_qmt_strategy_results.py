from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import uuid
import hashlib
from pathlib import Path
from threading import BoundedSemaphore, Event
from concurrent.futures import ThreadPoolExecutor
import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, inspect, select, text
from sqlalchemy.pool import StaticPool

from server.api import qmt_strategy_results as service
from server.api.admin_auth import is_admin_protected_path
from server.api.routers import qmt_ingest, qmt_strategy_results as read_router
from server.common.qmt_linux_ingest_protocol import new_request_headers, verify_signed_response
from server.common.qmt_strategy_result_schema import (
    INPUTS, JOBS, RESULTS, privileged_migrate_qmt_strategy_result_schema,
    validate_qmt_strategy_result_schema,
)
from server.engine import qmt_strategy_simulation as evaluator
from integrations.bigqmt.release_identity import render_strategy_artifact


BUILD = "a" * 40
NOW = datetime(2026, 10, 8, 10, 10, tzinfo=timezone.utc)
TARGET = "2026-09-30"
SECRET = "qmt-simulation-test-credential-long-enough"


def snapshot(target=TARGET):
    value = {
        "schema": evaluator.INPUT_SCHEMA, "trade_date": target, "mode": "REPLAY",
        "prepared_at": NOW.isoformat(), "decision_at": target + "T23:59:59",
        "market_clock": {"expected_trade_date": target, "current_closed_trade_date": TARGET,
                         "observed_at": NOW.isoformat()},
        "formula_contract": evaluator.formula_contract(),
        "simulation_only": True, "real_order_allowed": False,
        "v2": {"status": "DATA_BLOCKED", "trade_date": target, "reasons": ["PIT_BATCH_MISSING"],
               "frames": {}, "proofs": {}},
        "v3": {"status": "DATA_BLOCKED", "trade_date": target, "reasons": ["QMT_DAILY_WINDOW_MISSING"],
               "stocks": [], "market_features": {}},
    }
    value["input_hash"] = evaluator.snapshot_input_hash(value)
    return value


def bridge(build=BUILD):
    root = Path(__file__).resolve().parents[1]
    source = (root / "integrations/bigqmt/qmt_strategy/probiga_big_qmt_bridge.py").read_bytes()
    source_hash = hashlib.sha256(source).hexdigest()
    blob = hashlib.sha1(b"blob " + str(len(source)).encode("ascii") + b"\0" + source).hexdigest()
    rendered = render_strategy_artifact(source, build_sha=build, git_blob=blob, source_sha256=source_hash)
    return {"source": "gj_big_qmt_inner", "model_instance_id": "b" * 32,
            "strategy_build_sha": build, "strategy_git_blob": blob,
            "strategy_source_sha256": source_hash, "strategy_artifact_sha256": rendered["artifact_sha256"],
            "strategy_loaded_identity_sha256": rendered["identity_sha256"],
            "direct_acquisition_model_sha256": hashlib.sha256((root / "acquisition/qmt_model.py").read_bytes()).hexdigest(),
            "strategy_identity_frozen": True, "strategy_identity_status": "BOUND", "updated_at": NOW.isoformat()}


@pytest.fixture
def database(monkeypatch):
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    privileged_migrate_qmt_strategy_result_schema(engine)
    monkeypatch.setattr(service, "get_engine", lambda: engine)
    monkeypatch.setattr(service, "get_kline_engine", lambda: engine)
    monkeypatch.setattr(service, "runtime_component_build_sha", lambda _role: BUILD)
    monkeypatch.setattr(service, "_now", lambda: NOW)
    monkeypatch.setattr(service, "_PREPARE_SLOTS", BoundedSemaphore(2))
    monkeypatch.setattr(service, "authoritative_closed_trade_date", lambda _engine, **kwargs: TARGET)
    def prepare(_primary, _kline, target, *, run_mode=None):
        value = snapshot(target)
        value["mode"] = run_mode or value["mode"]
        value["input_hash"] = evaluator.snapshot_input_hash(value)
        return value
    monkeypatch.setattr(evaluator, "prepare_inputs", prepare)
    class ImmediateExecutor:
        def submit(self, function, *args):
            function(*args)
    monkeypatch.setattr(service, "_PREPARE_EXECUTOR", ImmediateExecutor())
    yield engine
    engine.dispose()


def issue(trade_date=TARGET, request_id=None):
    return service.prepare_strategy_inputs({"schema": service.INPUT_REQUEST_SCHEMA,
                                            "request_id": request_id or uuid.uuid4().hex,
                                            "edge_build_sha": BUILD, "trade_date": trade_date})


def commit_payload(issued):
    return {"schema": service.RESULT_COMMIT_SCHEMA, "edge_build_sha": BUILD,
            "snapshot_id": issued["snapshot_id"], "result": evaluator.evaluate_snapshot(issued["snapshot"]),
            "execution": {"origin": "WINDOWS_DAILY", "started_at": (NOW + timedelta(seconds=1)).isoformat(),
                          "finished_at": (NOW + timedelta(seconds=2)).isoformat(), "bridge_identity": bridge()}}


def test_runtime_schema_validation_is_read_only_and_migration_is_idempotent():
    engine = create_engine("sqlite:///:memory:")
    with pytest.raises(RuntimeError, match="not installed"):
        validate_qmt_strategy_result_schema(engine)
    assert inspect(engine).get_table_names() == []
    first = privileged_migrate_qmt_strategy_result_schema(engine)
    second = privileged_migrate_qmt_strategy_result_schema(engine)
    assert first == second
    assert first["runtime_ddl_required"] is False
    assert set(inspect(engine).get_table_names()) == {INPUTS.name, RESULTS.name, JOBS.name}


def test_issued_inputs_retain_build_hash_and_backdated_replay(database):
    issued = issue()
    assert issued["run_mode"] == "REPLAY"
    assert issued["edge_build_sha"] == BUILD
    assert len(issued["snapshot_sha256"]) == 64
    again = issue()
    assert again["snapshot_id"] == issued["snapshot_id"]
    pending = service.read_strategy_run(issued["snapshot_id"])
    assert pending["status"] == "AWAITING_EXECUTION"
    assert pending["result"] is None
    assert pending["real_order_allowed"] is False
    listing = service.read_strategy_results(TARGET)
    assert listing["status"] == "PENDING"
    assert listing["dates"] == [{"trade_date": TARGET, "run_count": 1}]
    assert len(listing["catalog"]["strategies"]) == 10
    assert len(listing["catalog"]["combinations"]) == 4


def test_commit_verifies_formula_and_idempotency_without_hiding_missing_data(database):
    issued = issue()
    payload = commit_payload(issued)
    first = service.commit_strategy_result(payload)
    second = service.commit_strategy_result(payload)
    assert first["status"] == second["status"] == "COMMITTED"
    assert first["execution_status"] == "DATA_BLOCKED"
    assert first["idempotent"] is False and second["idempotent"] is True
    assert first["result_hash"] == evaluator.canonical_hash(payload["result"])
    assert first["snapshot_id"] == issued["snapshot_id"]
    detail = service.read_strategy_run(first["run_uid"])
    assert detail["status"] == "DATA_BLOCKED"
    assert all(row["status"] == "DATA_BLOCKED" and row["blocked_reasons"]
               for row in detail["result"]["strategy_rows"])
    assert service.read_strategy_results(TARGET)["status"] == "AVAILABLE"
    with database.connect() as connection:
        assert connection.execute(select(func.count()).select_from(RESULTS)).scalar_one() == 1


@pytest.mark.parametrize("mutation", ["forged_reason", "real_orders", "excluded_strategy", "wrong_input"])
def test_commit_rejects_modified_result_and_has_no_database_effect(database, mutation):
    payload = commit_payload(issue())
    if mutation == "forged_reason":
        payload["result"]["strategy_rows"][0]["blocked_reasons"] = ["fabricated"]
    elif mutation == "real_orders":
        payload["result"]["real_order_allowed"] = True
    elif mutation == "excluded_strategy":
        payload["result"]["strategy_rows"][0]["strategy_key"] = "intraday_surprise"
    else:
        payload["result"]["input_hash"] = "0" * 64
    with pytest.raises(service.QmtStrategyResultError):
        service.commit_strategy_result(payload)
    with database.connect() as connection:
        assert connection.execute(select(func.count()).select_from(RESULTS)).scalar_one() == 0


def test_committed_result_cannot_be_overwritten(database):
    payload = commit_payload(issue())
    service.commit_strategy_result(payload)
    payload["execution"]["origin"] = "QMT_ENTRY"
    with pytest.raises(service.QmtStrategyResultError, match="immutable"):
        service.commit_strategy_result(payload)


def test_exact_issued_retry_returns_original_committed_receipt(database):
    issued = issue()
    payload = commit_payload(issued)
    service.commit_strategy_result(payload)
    again = issue()
    assert again["existing"] is True
    receipt = again["committed_receipt"]
    assert receipt["run_uid"] == issued["snapshot_id"]
    assert receipt["execution"]["finished_at"] == payload["execution"]["finished_at"]
    assert receipt["result_hash"] == evaluator.canonical_hash(payload["result"])


def test_repaired_same_session_can_issue_a_new_immutable_snapshot(database, monkeypatch):
    first = issue()
    service.commit_strategy_result(commit_payload(first))
    revised = snapshot()
    revised["v2"]["reasons"] = ["NEW_VERIFIED_SOURCE_REVISION"]
    revised["input_hash"] = evaluator.snapshot_input_hash(revised)
    monkeypatch.setattr(evaluator, "prepare_inputs", lambda *_args, **_kwargs: revised)
    second = issue()
    assert second["snapshot_id"] != first["snapshot_id"]
    assert second["existing"] is False
    assert service.read_strategy_run(first["snapshot_id"])["result"] == commit_payload(first)["result"]


def test_schedule_shows_failed_execution_without_exposing_raw_output(database):
    with database.begin() as connection:
        connection.execute(text("CREATE TABLE st_scheduled_tasks (task_name TEXT,task_type TEXT,cron_time TEXT,enabled INTEGER,script_path TEXT,script_args TEXT,interval_minutes INTEGER,last_run_at TEXT,last_run_status TEXT,last_run_duration INTEGER,last_run_output TEXT)"))
        connection.execute(text("INSERT INTO st_scheduled_tasks VALUES (:name,:type,'22:50',1,'tools/run_qmt_strategy_daily.py','--json',0,'2026-10-08 22:50:00','failed',5,:output)"),
                           {"name": "QMT每日策略模拟（10策略4组合）", "type": service.TASK_TYPE,
                            "output": "PRIVATE_DETAIL_SHOULD_NEVER_BE_RETURNED"})
    value = service.read_strategy_results(TARGET)
    assert value["schedule"]["status"] == "REGISTERED"
    assert value["schedule"]["last_run_status"] == "failed"
    assert "执行失败" in value["schedule"]["last_run_summary"]
    assert "PRIVATE_DETAIL" not in json.dumps(value)


def test_input_corruption_is_detected_on_read_and_commit(database):
    issued = issue()
    with database.begin() as connection:
        connection.execute(INPUTS.update().where(INPUTS.c.snapshot_id == issued["snapshot_id"])
                           .values(snapshot_json='{"schema":"tampered"}'))
    with pytest.raises(service.QmtStrategyResultError, match="integrity"):
        service.read_strategy_run(issued["snapshot_id"])
    with pytest.raises(service.QmtStrategyResultError, match="integrity"):
        service.commit_strategy_result(commit_payload(issued))


@pytest.mark.parametrize("field,value", [("strategy_build_sha", "9" * 40), ("strategy_identity_frozen", False),
                                         ("updated_at", "2026-10-07T10:10:00+00:00"), ("source", "other")])
def test_execution_requires_fresh_content_bound_qmt_identity(database, field, value):
    payload = commit_payload(issue())
    payload["execution"]["bridge_identity"][field] = value
    with pytest.raises(service.QmtStrategyResultError):
        service.commit_strategy_result(payload)


def test_content_compatible_old_native_bridge_preserves_actual_build(database):
    payload = commit_payload(issue())
    payload["execution"]["bridge_identity"] = bridge("7" * 40)
    saved = service.commit_strategy_result(payload)
    assert service.read_strategy_run(saved["run_uid"])["execution"]["bridge_identity"]["strategy_build_sha"] == "7" * 40


def test_next_day_exact_committed_retry_does_not_require_fresh_execution(database, monkeypatch):
    payload = commit_payload(issue())
    first = service.commit_strategy_result(payload)
    monkeypatch.setattr(service, "_now", lambda: NOW + timedelta(days=10))
    monkeypatch.setattr(service, "authoritative_closed_trade_date", lambda *_args, **_kwargs: "2026-10-09")
    monkeypatch.setattr(service, "_execution", lambda *_args: pytest.fail("must acknowledge original committed bytes, not revalidate as new execution"))
    second = service.commit_strategy_result(payload)
    assert second["idempotent"] is True and second["execution_hash"] == first["execution_hash"]
    assert service.read_strategy_run(second["run_uid"])["execution"] == payload["execution"]
    altered = deepcopy(payload)
    altered["execution"]["finished_at"] = (NOW + timedelta(days=10)).isoformat()
    with pytest.raises(service.QmtStrategyResultError, match="immutable"):
        service.commit_strategy_result(altered)


def test_read_rejects_snapshot_mode_scalar_tampering(database):
    issued = issue()
    with database.begin() as connection:
        connection.execute(INPUTS.update().where(INPUTS.c.snapshot_id == issued["snapshot_id"]).values(run_mode="DAILY"))
    with pytest.raises(service.QmtStrategyResultError, match="issued identity"):
        service.read_strategy_run(issued["snapshot_id"])


class DeferredExecutor:
    def __init__(self):
        self.calls = []

    def submit(self, function, *args):
        self.calls.append((function, args))


def request_payload(request_id="2" * 32, trade_date=""):
    return {"schema": service.INPUT_REQUEST_SCHEMA, "request_id": request_id,
            "edge_build_sha": BUILD, "trade_date": trade_date}


def test_async_queue_returns_without_fact_preparation_and_persists_binding(database, monkeypatch):
    monkeypatch.setattr(service, "_start_preparation", lambda *_args: None)
    monkeypatch.setattr(evaluator, "prepare_inputs", lambda *_args, **_kwargs: pytest.fail("HTTP must not prepare facts"))
    payload = request_payload()
    first, again = service.prepare_strategy_inputs(payload), service.prepare_strategy_inputs(payload)
    assert first["status"] == again["status"] == "QUEUED"
    assert first["request_id"] == payload["request_id"] and first["trade_date"] == TARGET
    assert all(first[key] is value for key, value in service._SAFE.items())
    with database.connect() as connection:
        assert connection.execute(select(func.count()).select_from(JOBS)).scalar_one() == 1
        assert connection.execute(select(func.count()).select_from(INPUTS)).scalar_one() == 0
    changed = {**payload, "trade_date": TARGET}
    with pytest.raises(service.QmtStrategyResultError, match="immutable"):
        service.prepare_strategy_inputs(changed)
    listing = service.read_strategy_results()
    assert listing["dates"] == [{"trade_date": TARGET, "run_count": 0}]
    assert listing["preparation_jobs"][0]["status"] == "QUEUED"


def test_real_background_prepare_does_not_hold_http_and_never_selects(database, monkeypatch, tmp_path):
    engine = create_engine("sqlite:///" + str(tmp_path / "durable-jobs.sqlite"), connect_args={"check_same_thread": False})
    privileged_migrate_qmt_strategy_result_schema(engine)
    monkeypatch.setattr(service, "get_engine", lambda: engine)
    monkeypatch.setattr(service, "get_kline_engine", lambda: engine)
    entered, release = Event(), Event()
    def prepare(_primary, _kline, target, *, run_mode=None):
        entered.set()
        if not release.wait(timeout=5):
            raise RuntimeError("test preparation release not observed")
        value = snapshot(target)
        value["mode"] = run_mode
        value["input_hash"] = evaluator.snapshot_input_hash(value)
        return value
    monkeypatch.setattr(evaluator, "prepare_inputs", prepare)
    monkeypatch.setattr(evaluator, "evaluate_snapshot", lambda *_args: pytest.fail("Linux preparation must not select strategies"))
    executor = ThreadPoolExecutor(max_workers=2)
    monkeypatch.setattr(service, "_PREPARE_EXECUTOR", executor)
    try:
        started = time.monotonic()
        response = service.prepare_strategy_inputs(request_payload())
        assert time.monotonic() - started < 2
        assert response["status"] == "PREPARING" and entered.wait(timeout=1)
        release.set()
        executor.shutdown(wait=True)
        issued = service.prepare_strategy_inputs(request_payload())
        assert issued["status"] == "ISSUED" and issued["request_id"] == response["request_id"]
    finally:
        release.set()
        executor.shutdown(wait=True)
        engine.dispose()


def test_job_cas_same_request_claims_once_and_local_queue_is_bounded(database, monkeypatch):
    deferred = DeferredExecutor()
    monkeypatch.setattr(service, "_PREPARE_EXECUTOR", deferred)
    first = service.prepare_strategy_inputs(request_payload())
    again = service.prepare_strategy_inputs(request_payload())
    second = service.prepare_strategy_inputs(request_payload("3" * 32))
    third = service.prepare_strategy_inputs(request_payload("4" * 32))
    assert first["status"] == again["status"] == second["status"] == "PREPARING"
    assert third["status"] == "QUEUED" and len(deferred.calls) == 2
    function, args = deferred.calls.pop(0)
    function(*args)
    issued = service.prepare_strategy_inputs(request_payload())
    assert issued["status"] == "ISSUED" and issued["request_id"] == first["request_id"]
    service.prepare_strategy_inputs(request_payload("4" * 32))
    assert len(deferred.calls) == 2


def test_restart_poll_recovers_expired_lease_and_old_worker_cannot_publish(database, monkeypatch):
    deferred = DeferredExecutor()
    monkeypatch.setattr(service, "_PREPARE_EXECUTOR", deferred)
    payload = request_payload()
    service.prepare_strategy_inputs(payload)
    _, old_args = deferred.calls[0]
    with database.connect() as connection:
        old_job = dict(connection.execute(select(JOBS)).mappings().one())
    later = NOW + timedelta(seconds=service._LEASE_SECONDS + 1)
    monkeypatch.setattr(service, "_now", lambda: later)
    # A restarted web process has fresh local capacity but the durable old lease.
    monkeypatch.setattr(service, "_PREPARE_SLOTS", BoundedSemaphore(2))
    service.prepare_strategy_inputs(payload)
    assert len(deferred.calls) == 2
    with database.connect() as connection:
        new_job = dict(connection.execute(select(JOBS)).mappings().one())
    assert new_job["lease_token"] != old_job["lease_token"] and new_job["attempt_count"] == 2
    assert service._renew_preparation_lease(database, payload["request_id"], old_args[2]) is False
    old_snapshot = snapshot()
    old_snapshot["mode"] = old_job["run_mode"]
    old_snapshot["input_hash"] = evaluator.snapshot_input_hash(old_snapshot)
    service._publish_preparation(database, old_job, old_args[2], old_snapshot)
    with database.connect() as connection:
        assert connection.execute(select(func.count()).select_from(INPUTS)).scalar_one() == 0
    function, args = deferred.calls[1]
    function(*args)
    assert service.prepare_strategy_inputs(payload)["status"] == "ISSUED"


def test_heartbeat_extends_only_its_current_live_lease(database, monkeypatch):
    deferred = DeferredExecutor()
    monkeypatch.setattr(service, "_PREPARE_EXECUTOR", deferred)
    service.prepare_strategy_inputs(request_payload())
    _, args = deferred.calls[0]
    monkeypatch.setattr(service, "_now", lambda: NOW + timedelta(seconds=60))
    assert service._renew_preparation_lease(database, "2" * 32, args[2]) is True
    assert service._renew_preparation_lease(database, "2" * 32, "f" * 32) is False
    with database.connect() as connection:
        job = connection.execute(select(JOBS)).mappings().one()
    assert job["lease_expires_at"] == (NOW + timedelta(seconds=180)).replace(tzinfo=None)


def test_failed_prepare_is_sanitized_terminal_and_new_request_can_retry(database, monkeypatch):
    def fail(*_args, **_kwargs):
        raise RuntimeError("SECRET_DATABASE_URL_SHOULD_NOT_BE_EXPOSED")
    monkeypatch.setattr(evaluator, "prepare_inputs", fail)
    first = service.prepare_strategy_inputs(request_payload())
    assert first["status"] == "FAILED" and first["error_code"] == "INPUT_PREPARATION_FAILED"
    assert first["request_id"] == "2" * 32 and first["trade_date"] == TARGET
    assert "SECRET_DATABASE_URL" not in json.dumps(first)
    monkeypatch.setattr(evaluator, "prepare_inputs", lambda *_args, **_kwargs: {**snapshot(), "mode": "DAILY", "input_hash": evaluator.snapshot_input_hash({**snapshot(), "mode": "DAILY"})})
    assert service.prepare_strategy_inputs(request_payload())["status"] == "FAILED"
    second = service.prepare_strategy_inputs(request_payload("3" * 32))
    assert second["status"] == "ISSUED"
    assert service.read_strategy_results()["preparation_jobs"]


def test_daily_preparation_rollover_fails_without_new_date_or_input(database, monkeypatch):
    deferred = DeferredExecutor()
    monkeypatch.setattr(service, "_PREPARE_EXECUTOR", deferred)
    payload = request_payload()
    first = service.prepare_strategy_inputs(payload)
    assert first["trade_date"] == TARGET and first["run_mode"] == "DAILY"
    monkeypatch.setattr(service, "authoritative_closed_trade_date", lambda *_args, **_kwargs: "2026-10-09")
    function, args = deferred.calls[0]
    function(*args)
    failed = service.prepare_strategy_inputs(payload)
    assert failed["status"] == "FAILED" and failed["error_code"] == "CLOSED_SESSION_ROLLOVER"
    assert failed["trade_date"] == TARGET
    with database.connect() as connection:
        assert connection.execute(select(func.count()).select_from(INPUTS)).scalar_one() == 0


def test_publish_and_job_issued_are_atomic_and_old_expired_token_cannot_publish(database, monkeypatch):
    deferred = DeferredExecutor()
    monkeypatch.setattr(service, "_PREPARE_EXECUTOR", deferred)
    service.prepare_strategy_inputs(request_payload(trade_date=TARGET))
    with database.connect() as connection:
        job = dict(connection.execute(select(JOBS)).mappings().one())
    from sqlalchemy import event
    def refuse_update(_connection, _cursor, statement, _parameters, _context, _many):
        if statement.startswith("UPDATE st_qmt_strategy_input_job") and "status=?" in statement:
            raise RuntimeError("simulate failure between snapshot INSERT and job publish")
    event.listen(database, "before_cursor_execute", refuse_update)
    with pytest.raises(RuntimeError):
        service._publish_preparation(database, job, job["lease_token"], snapshot())
    with database.connect() as connection:
        assert connection.execute(select(func.count()).select_from(INPUTS)).scalar_one() == 0
        assert connection.execute(select(JOBS.c.status)).scalar_one() == "PREPARING"
    event.remove(database, "before_cursor_execute", refuse_update)
    monkeypatch.setattr(service, "_now", lambda: NOW + timedelta(seconds=121))
    service._publish_preparation(database, job, job["lease_token"], snapshot())
    with database.connect() as connection:
        assert connection.execute(select(func.count()).select_from(INPUTS)).scalar_one() == 0


def test_preparation_poll_rejects_binding_scalar_tampering(database, monkeypatch):
    monkeypatch.setattr(service, "_start_preparation", lambda *_args: None)
    service.prepare_strategy_inputs(request_payload())
    with database.begin() as connection:
        connection.execute(JOBS.update().values(trade_date="2026-09-29"))
    with pytest.raises(service.QmtStrategyResultError, match="binding"):
        service.prepare_strategy_inputs(request_payload())


def test_default_date_comes_from_authoritative_calendar_not_latest_rows(database):
    issued = issue("")
    assert issued["trade_date"] == TARGET
    with pytest.raises(service.QmtStrategyResultError, match="not closed"):
        issue("2026-10-09")


def test_machine_operations_require_hmac_and_reads_keep_account_auth(monkeypatch):
    app = FastAPI()
    app.include_router(qmt_ingest.router, prefix="/api")
    app.include_router(read_router.router, prefix="/api")
    monkeypatch.setattr(qmt_ingest, "get_ai_bridge_config", lambda: {"token": SECRET})
    calls = []
    monkeypatch.setattr(qmt_ingest, "prepare_strategy_inputs", lambda payload: calls.append(payload) or {"status": "ISSUED", "snapshot_id": "a" * 32})
    payload = {"schema": service.INPUT_REQUEST_SCHEMA, "request_id": "9" * 32, "edge_build_sha": BUILD, "trade_date": ""}
    client = TestClient(app)
    assert client.post("/api/qmt-ingest/strategy-inputs", json=payload).status_code == 401
    assert calls == []
    response = client.post("/api/qmt-ingest/strategy-inputs", json=payload,
                           headers=new_request_headers(SECRET, payload))
    assert response.status_code == 200
    assert verify_signed_response(SECRET, response.json())["status"] == "ISSUED"
    tampered = {**payload, "trade_date": TARGET}
    assert client.post("/api/qmt-ingest/strategy-inputs", json=tampered,
                       headers=new_request_headers(SECRET, payload)).status_code == 401
    assert is_admin_protected_path("/api/qmt-ingest/strategy-results", "POST") is False
    assert is_admin_protected_path("/api/strategy-center/qmt-results", "GET") is True
    assert is_admin_protected_path("/api/strategy-center/qmt-results/performance", "GET") is True


def test_replay_performance_starts_after_actual_observation_and_does_not_fill_missing_prices():
    detail = {"run_uid": "a" * 32, "trade_date": TARGET, "run_mode": "REPLAY", "result": {
        "strategy_rows": [{"strategy_key": "main_wave", "name": "主升浪", "status": "COMPLETED",
                           "selected": [{"stock_code": "000001", "stock_name": "测试公司"},
                                        {"stock_code": "000002", "stock_name": "缺价公司"}]}], "combination_rows": []}}
    # September prices predate the October observation and are never used as
    # if the replay produced a September trade.
    rows = [{"stock_code": "000001", "trade_date": "2026-09-30", "open": 1, "close": 100, "volume": 1000},
            {"stock_code": "000001", "trade_date": "2026-10-09", "open": 10, "close": 11, "volume": 1000}]
    value = service._price_reference(detail, ["2026-09-30", "2026-10-09"], rows, NOW)
    entity = value["entities"][0]
    assert entity["picks"][0]["entry_date"] == "2026-10-09"
    assert entity["picks"][0]["return_pct"] == 10.0
    assert entity["picks"][1]["return_pct"] is None
    assert entity["avg_return_pct"] is None
    assert entity["verified_count"] == 1
    assert value["is_execution_return"] is False


def test_performance_waits_for_post_observation_session(database):
    payload = commit_payload(issue())
    service.commit_strategy_result(payload)
    value = service.read_strategy_performance(payload["snapshot_id"])
    assert value["status"] == "PENDING_FORWARD_DATA"
    assert value["entities"] == []


def test_forward_prices_exclude_unattested_duplicate_and_bind_exact_native_rows():
    from server.common.qmt_attestation_contract import ATTESTATION_PROTOCOL_VERSION

    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE sm_stock_kline (id INTEGER,stock_code TEXT,trade_date TEXT,k_type INTEGER,adjust_type INTEGER,`open` REAL,`close` REAL,high REAL,low REAL,pre_close REAL,volume REAL,amount REAL,received_at TEXT,data_version TEXT)"))
        connection.execute(text("CREATE TABLE qmt_stock_catalog_member (batch_id TEXT,stock_code TEXT,instrument_type TEXT,list_date TEXT,expire_date TEXT)"))
        connection.execute(text("CREATE TABLE qmt_kline_attestation_run (run_id TEXT,provider TEXT,status TEXT,finished_at TEXT)"))
        connection.execute(text("CREATE TABLE qmt_kline_attestation_row (run_id TEXT,target_id INTEGER,protocol_version TEXT,created_at TEXT,source_data_version TEXT,source_pre_close_origin TEXT,trade_date TEXT,stock_code TEXT,source_pre_close REAL,attested_open REAL,attested_close REAL,attested_high REAL,attested_low REAL,attested_volume REAL,attested_amount REAL)"))
        connection.execute(text("INSERT INTO sm_stock_kline VALUES (1,'000001','2026-10-09',1,0,10,11,11,9,10,100,1000,'2026-10-09 18:00:00','native-v1'),(2,'000001','2026-10-09',1,0,10,99,99,9,10,100,1000,'2026-10-09 18:00:00','foreign-v1')"))
        connection.execute(text("INSERT INTO qmt_stock_catalog_member VALUES ('catalog','000001','STOCK','2020-01-01',NULL)"))
        connection.execute(text("INSERT INTO qmt_kline_attestation_run VALUES ('native-run','gj_big_qmt_inner','COMPLETED','2026-10-09 19:00:00')"))
        connection.execute(text("INSERT INTO qmt_kline_attestation_row VALUES ('native-run',1,:protocol,'2026-10-09 19:00:00','native-v1','NATIVE_QMT','2026-10-09','000001',10,10,11,11,9,100,1000)"), {"protocol": ATTESTATION_PROTOCOL_VERSION})
    proofs = {"2026-10-09": {"run_finished_at": "2026-10-09 19:00:00", "catalog_batch_id": "catalog"}}
    with engine.connect() as connection:
        rows = service._load_forward_prices(connection, proofs, ["000001"], datetime(2026, 10, 10, tzinfo=timezone.utc))
        assert len(rows) == 1
        assert rows[0]["close"] == 11
    # A source row changed after attestation cannot retain its price authority.
    with engine.begin() as connection:
        connection.execute(text("UPDATE sm_stock_kline SET `close`=12 WHERE id=1"))
    with engine.connect() as connection:
        assert service._load_forward_prices(connection, proofs, ["000001"], datetime(2026, 10, 10, tzinfo=timezone.utc)) == []


def test_preparation_explicit_replay_uses_historical_cutoff_even_for_latest_closed_session(monkeypatch):
    from server.common import authoritative_market_clock

    monkeypatch.setattr(authoritative_market_clock, "authoritative_closed_trade_date", lambda *_args, **_kwargs: TARGET)
    cutoffs = []
    def v2(_engine, target, decision):
        cutoffs.append(decision)
        return {"status": "DATA_BLOCKED", "reasons": ["TEST_SOURCE_UNAVAILABLE"], "trade_date": target,
                "frames": {}, "proofs": {}}
    def v3(_primary, _kline, target, decision):
        cutoffs.append(decision)
        return {"status": "DATA_BLOCKED", "reasons": ["TEST_SOURCE_UNAVAILABLE"], "trade_date": target,
                "stocks": [], "market_features": {}}
    monkeypatch.setattr(evaluator, "_prepare_v2", v2)
    monkeypatch.setattr(evaluator, "_prepare_v3", v3)
    value = evaluator.prepare_inputs(object(), object(), TARGET, run_mode="REPLAY")
    assert value["mode"] == "REPLAY"
    assert value["decision_at"] == TARGET + "T23:59:59"
    assert [cutoff.isoformat() for cutoff in cutoffs] == [TARGET + "T23:59:59"] * 2
