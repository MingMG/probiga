from __future__ import annotations

from datetime import datetime
import hashlib
import json
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, text

from server.api import scheduler_runtime as scheduler
from server.api.routers import health, trading_v3
from server.common import component_release_attestation as attest
from server.common import daily_delivery_control as control
from server.common.component_release import build_component_release, validate_component_release
from server.common.release_data_readiness_contract import build_release_data_activation_receipt


WINDOWS = "a" * 40
LINUX = "b" * 40
PRIOR_LINUX = "c" * 40


def manifest(linux=LINUX, *, anchor=WINDOWS, digest="d" * 64):
    return build_component_release(
        linux_build_sha=linux, windows_build_sha=anchor,
        contract_build_sha=anchor, contract_sha256=digest,
        parent_linux_build_sha=anchor,
        scope="COORDINATED" if linux == anchor else "LINUX",
        created_at="2026-09-19T03:00:00Z",
    )


class Rows:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        return self.rows

    def __iter__(self):
        return iter(self.rows)


class Connection:
    def __init__(self, identities, lease=None):
        self.identities = identities
        self.lease = lease or [{"build_sha": LINUX, "poll_seconds": 60, "heartbeat_age_seconds": 2}]
    def execute(self, statement, parameters):
        sql = str(statement)
        assert sql.lstrip().startswith("SELECT")
        assert "st_scheduler_runtime" in sql
        return Rows(self.lease)


@pytest.fixture(autouse=True)
def verified_ledger_rows(monkeypatch):
    # Cryptographic and physical ledger validation has its own tests. These
    # fixtures provide already-verified identities to exercise runtime consumers.
    loader = attest.load_component_release_row

    def load(connection, build):
        if not isinstance(connection, Connection):
            return loader(connection, build)
        if build not in connection.identities:
            raise RuntimeError("signed component identity is unavailable")
        return validate_component_release(connection.identities[build])

    monkeypatch.setattr(attest, "load_component_release_row", load)


def test_compatible_intermediate_linux_producer_preserves_actual_build():
    connection = Connection({LINUX: manifest(), PRIOR_LINUX: manifest(PRIOR_LINUX)})
    result = attest.require_compatible_component_build(connection, PRIOR_LINUX, LINUX)
    assert result["linux_build_sha"] == PRIOR_LINUX
    assert result["contract_build_sha"] == WINDOWS


@pytest.mark.parametrize("build", [
    "", "0" * 40, "A" * 40, "a" * 39, "a" * 41, "x" * 40,
])
def test_component_build_must_be_exact_before_ledger_lookup(build, monkeypatch):
    loader = MagicMock()
    monkeypatch.setattr(attest, "load_component_release_row", loader)
    with pytest.raises(RuntimeError):
        attest.load_component_attestation(object(), build)
    loader.assert_not_called()


def test_component_attestation_uses_verified_signed_ledger(monkeypatch):
    connection = object()
    loader = MagicMock(return_value=manifest())
    monkeypatch.setattr(attest, "load_component_release_row", loader)
    assert attest.load_component_attestation(connection, LINUX) == manifest()
    loader.assert_called_once_with(connection, LINUX)


def test_component_signature_failure_is_never_replaced_by_self_seal(monkeypatch):
    monkeypatch.setattr(attest, "load_component_release_row", MagicMock(side_effect=RuntimeError("invalid signature")))
    with pytest.raises(RuntimeError, match="invalid signature"):
        attest.load_component_attestation(Connection({LINUX: manifest()}), LINUX)


def test_self_sealed_other_contract_is_not_compatible():
    connection = Connection({LINUX: manifest(), PRIOR_LINUX: manifest(PRIOR_LINUX, digest="e" * 64)})
    with pytest.raises(RuntimeError, match="producer contract differs"):
        attest.require_compatible_component_build(connection, PRIOR_LINUX, LINUX)


def test_windows_resolves_protected_linux_build_without_claiming_it_as_own():
    connection = Connection({LINUX: manifest(), WINDOWS: manifest(WINDOWS)})
    assert attest.resolve_active_linux_component(connection, WINDOWS, expected_poll_seconds=60) == manifest()


@pytest.mark.parametrize("age,duplicate", [(121, False), (-1, False), (2, True)])
def test_windows_rejects_expired_future_or_ambiguous_linux_lease(age, duplicate):
    lease = [{"build_sha": LINUX, "poll_seconds": 60, "heartbeat_age_seconds": age}]
    connection = Connection({LINUX: manifest(), WINDOWS: manifest(WINDOWS)}, lease * (2 if duplicate else 1))
    with pytest.raises(RuntimeError):
        attest.resolve_active_linux_component(connection, WINDOWS, expected_poll_seconds=60)


def test_windows_rejects_unattested_heartbeat_build():
    with pytest.raises(RuntimeError):
        attest.resolve_active_linux_component(Connection({WINDOWS: manifest(WINDOWS)}), WINDOWS, expected_poll_seconds=60)


def test_windows_activation_checks_actual_linux_receipt_and_own_qmt_receipt(monkeypatch):
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setattr(scheduler, "resolve_active_linux_component", lambda *args, **kwargs: manifest())
    monkeypatch.setattr(scheduler, "get_scheduler_runtime_config", lambda: {"poll_seconds": 60})
    current = {"instance_id": "linux-123", "host_name": "linux", "started_at": "2026-09-19T03:00:00"}
    linux_check = MagicMock(return_value=(True, {"current": current}))
    qmt_check = MagicMock(return_value=(True, {}))
    monkeypatch.setattr(scheduler, "check_linux_standalone_active_release", linux_check)
    monkeypatch.setattr(scheduler, "check_qmt_windows_edge_release_receipt", qmt_check)
    receipt = build_release_data_activation_receipt(
        build_sha=LINUX, scheduler_instance_id=current["instance_id"],
        scheduler_host_name="linux", scheduler_pid=123,
        scheduler_started_at=current["started_at"], activated_at="2026-09-19T03:00:02",
    )
    row = {"task_type": scheduler.RELEASE_DATA_ACTIVATION_TASK_TYPE, "status": "success",
           "exit_code": 0, "output": json.dumps(receipt), "host_name": "linux",
           "scheduler_instance_id": "linux-123", "build_sha": LINUX,
           "trigger_source": scheduler.RELEASE_DATA_ACTIVATION_TRIGGER_SOURCE}
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.mappings.return_value = [row]
    assert scheduler._windows_release_activation_ready(engine, build_sha=WINDOWS) == (True, "ready")
    assert linux_check.call_args.kwargs["expected_build_sha"] == LINUX
    assert qmt_check.call_args.kwargs["expected_build_sha"] == WINDOWS


def test_daily_api_does_not_accept_unattested_same_build_in_production(monkeypatch):
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    engine = MagicMock()
    monkeypatch.setattr(trading_v3, "require_compatible_component_build", MagicMock(side_effect=RuntimeError("missing")))
    assert trading_v3._daily_canonical_build_compatible(engine, LINUX, LINUX) is False


def test_health_rejects_component_file_database_disagreement(monkeypatch):
    monkeypatch.setattr(health, "load_runtime_component_release", lambda: manifest())
    monkeypatch.setattr(health, "get_engine", MagicMock())
    monkeypatch.setattr(health, "load_component_attestation", lambda *args: manifest(PRIOR_LINUX))
    assert health._component_release_readiness() == {"ready": False, "error_code": "component_release_identity_failed"}


def checkpoint(producer=LINUX, anchor=WINDOWS, stage="analysis_fast"):
    replay = '{"status":"COMPLETED"}'
    digest = hashlib.sha256(replay.encode()).hexdigest()
    core = {"schema": control.SCHEDULER_VALIDATION_EVIDENCE_SCHEMA,
            "run_uid": "f" * 32, "task_type": stage, "build_sha": producer,
            "contract_release_id": anchor, "target_trade_date": "2026-09-18",
            "status": "success", "exit_code": 0, "validation_checked": True,
            "validation_ok": True, "replay_output": replay,
            "replay_output_sha256": digest, "input_receipt_root_sha256": digest}
    evidence = {**core, "evidence_sha256": control.canonical_sha256(core)}
    attempt = {"scheduler_run_uid": "f" * 32, "status": "SUCCESS",
               "input_root_sha256": digest, "checkpoint_json": json.dumps(evidence)}
    session = {"release_id": anchor, "trade_date": "2026-09-18"}
    return attempt, session


def test_checkpoint_separates_producer_build_from_shared_session(monkeypatch):
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    connection = Connection({LINUX: manifest()})
    attempt, session = checkpoint()
    result = control._validated_completed_stage_checkpoint(attempt, session=session, stage_name="analysis_fast", engine=connection)
    assert result["build_sha"] == LINUX
    assert result["contract_release_id"] == WINDOWS


def test_checkpoint_cannot_relabel_linux_as_windows_or_change_epoch(monkeypatch):
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    connection = Connection({LINUX: manifest()})
    for stage, anchor in [("qmt_stock_daily_canonical", WINDOWS), ("analysis_fast", PRIOR_LINUX)]:
        attempt, session = checkpoint(stage=stage, anchor=anchor)
        with pytest.raises(control.DailyDeliveryFenceLost):
            control._validated_completed_stage_checkpoint(attempt, session=session, stage_name=stage, engine=connection)


def test_history_dependencies_use_fixed_owner_build(monkeypatch):
    monkeypatch.setattr(scheduler, "runtime_component_build_sha", lambda role, **kwargs: WINDOWS if role == "windows" else LINUX)
    assert scheduler._task_component_build_sha("qmt_stock_daily_canonical", LINUX) == WINDOWS
    assert scheduler._task_component_build_sha("analysis_fast", LINUX) == LINUX


def test_linux_and_windows_stage_attempts_share_contract_session_with_distinct_producers(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    control.privileged_migrate_daily_delivery_schema(engine)
    now = datetime(2026, 9, 18, 22, 0)
    monkeypatch.setattr(control, "_control_now", lambda: now)
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setattr(control, "load_component_attestation", lambda _connection, build: manifest(build))
    sessions = []
    for index, (stage, producer) in enumerate((("qmt_stock_daily_canonical", WINDOWS), ("analysis_fast", LINUX))):
        run_uid = str(index + 1) * 32
        attempt = control.start_daily_stage_attempt(
            engine, scheduler_run_uid=run_uid, stage_name=stage,
            trade_date="2026-09-18", release_id=WINDOWS,
            strategy_release_id="e" * 64, lease_owner=f"owner-{index}",
            lease_seconds=120,
        )
        sessions.append(attempt["session_uid"])
        raw, _session = checkpoint(producer=producer, stage=stage)
        evidence = json.loads(raw["checkpoint_json"])
        evidence.pop("evidence_sha256")
        evidence["run_uid"] = run_uid
        evidence["evidence_sha256"] = control.canonical_sha256(evidence)
        with engine.begin() as connection:
            control.finish_daily_stage_attempt(
                connection, scheduler_run_uid=run_uid, status="success",
                checkpoint=evidence, input_root_sha256=evidence["input_receipt_root_sha256"], now=now,
            )
    assert sessions[0] == sessions[1]
    with engine.connect() as connection:
        rows = connection.execute(text("SELECT status,checkpoint_json FROM st_daily_stage_attempt ORDER BY id")).mappings().all()
    assert [row["status"] for row in rows] == ["SUCCESS", "SUCCESS"]
    assert [json.loads(row["checkpoint_json"])["build_sha"] for row in rows] == [WINDOWS, LINUX]
