from __future__ import annotations

import json
import os
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from server.api import scheduler_runtime as scheduler
from server.common import component_release_attestation as attestation
from server.common.component_release import ComponentReleaseError, build_component_release


WINDOWS = "a" * 40
LINUX = "b" * 40
TARGET = "2026-09-18"
NOW = datetime(2026, 9, 21, 4, 10)
UPPER = "analysis_upper_evidence_prepare"


class Rows:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def __iter__(self):
        return iter(self.rows)


def history(task_type, index):
    producer = WINDOWS if scheduler._task_component_role(task_type) == "windows" else LINUX
    replay = json.dumps({"target_trade_date": TARGET})
    run_uid = f"{index:032x}"
    core = {
        "schema": scheduler._HISTORY_EVIDENCE_SCHEMA,
        "run_uid": run_uid, "task_type": task_type, "build_sha": producer,
        "status": "success", "exit_code": 0, "validation_checked": True,
        "validation_ok": True, "replay_output": replay,
        "input_receipt_root_sha256": scheduler._history_digest(replay),
        "target_trade_date": TARGET,
    }
    core["evidence_sha256"] = scheduler._history_digest(json.dumps(
        core, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str,
    ))
    return {
        "run_uid": run_uid, "task_type": task_type, "build_sha": producer,
        "status": "success", "exit_code": 0, "run_at": datetime(2026, 9, 18, 23),
        "finished_at": datetime(2026, 9, 18, 23, 1), "output": json.dumps(core),
    }


class Connection:
    def __init__(self):
        self.histories = [history(kind, i + 1) for i, kind in enumerate(
            scheduler._DAILY_ANALYSIS_EVIDENCE_DEPENDENCIES[UPPER]
        )]
        self.current = {"instance_id": "linux-123", "host_name": "linux",
                        "started_at": "2026-09-21T02:00:00"}
        receipt = scheduler.build_release_data_activation_receipt(
            build_sha=LINUX, scheduler_instance_id="linux-123", scheduler_host_name="linux",
            scheduler_pid=123, scheduler_started_at=self.current["started_at"],
            activated_at="2026-09-21T02:00:01",
        )
        self.activation = [{
            "task_type": scheduler.RELEASE_DATA_ACTIVATION_TASK_TYPE,
            "status": "success", "exit_code": 0, "output": json.dumps(receipt),
            "host_name": "linux", "scheduler_instance_id": "linux-123",
            "build_sha": LINUX, "trigger_source": scheduler.RELEASE_DATA_ACTIVATION_TRIGGER_SOURCE,
        }]
        self.lease = [{"build_sha": LINUX, "poll_seconds": 60, "heartbeat_age_seconds": 2}]
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append(sql)
        assert sql.lstrip().startswith("SELECT")
        if "FROM st_scheduler_runtime" in sql:
            return Rows(self.lease)
        if "FROM st_scheduled_tasks" in sql:
            return Rows([{"task_type": item["task_type"], "enabled": 1} for item in self.histories])
        if "FROM st_scheduled_task_history AS history" in sql:
            return Rows(self.histories)
        if "FROM st_scheduled_task_history" in sql:
            return Rows(self.activation)
        raise AssertionError(sql)


@pytest.fixture
def windows_dependencies(monkeypatch):
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", WINDOWS)
    monkeypatch.setenv("PROBIGA_SCHEDULER_EXECUTOR_ROLE", "qmt_windows_edge")
    platform = SimpleNamespace(**{name: getattr(os, name) for name in dir(os)})
    platform.name = "nt"
    monkeypatch.setattr(scheduler, "os", platform)
    local_calls = []

    def local_identity(role, *, expected_build_sha):
        local_calls.append(role)
        assert expected_build_sha == WINDOWS
        if role == "linux":
            raise ComponentReleaseError("Windows cannot establish the Linux component build")
        return WINDOWS

    monkeypatch.setattr(scheduler, "runtime_component_build_sha", local_identity)
    monkeypatch.setattr(scheduler, "get_scheduler_runtime_config", lambda: {
        "poll_seconds": 60, "max_concurrent_tasks": 1,
    })
    connection = Connection()
    manifests = {
        build: build_component_release(
            linux_build_sha=build, windows_build_sha=WINDOWS, contract_build_sha=WINDOWS,
            contract_sha256="d" * 64, parent_linux_build_sha=WINDOWS,
            scope="COORDINATED" if build == WINDOWS else "LINUX",
            created_at="2026-09-21T02:00:00Z",
        ) for build in (LINUX, WINDOWS)
    }
    # The ledger's signature/schema verification is separately tested; exercise
    # its real lease and contract resolver with already verified manifests here.
    ledger = MagicMock(side_effect=lambda conn, build: manifests[build])
    monkeypatch.setattr(attestation, "load_component_release_row", ledger)
    lease_check = MagicMock(return_value=(True, {"current": connection.current}))
    monkeypatch.setattr(scheduler, "check_linux_standalone_active_release", lease_check)
    monkeypatch.setattr(scheduler, "load_qmt_daily_market_truth", MagicMock())
    engine = MagicMock()
    engine.connect.return_value = connection
    row = {"task_type": UPPER, "_scheduler_target_trade_date": TARGET}
    return SimpleNamespace(connection=connection, engine=engine, row=row,
                           ledger=ledger, lease_check=lease_check, local_calls=local_calls)


def test_windows_owned_pipeline_uses_peer_proof_once_and_keeps_distinct_builds(windows_dependencies):
    state = windows_dependencies
    assert scheduler._strategy_pipeline_dependencies_ready(state.row, state.engine, NOW) == (True, "ready")
    assert "linux" not in state.local_calls
    state.lease_check.assert_called_once_with(state.connection, expected_build_sha=LINUX, expected_poll_seconds=60)
    assert sum("FROM st_scheduler_runtime" in sql for sql in state.connection.statements) == 1
    assert scheduler.scheduler_task_host_owner({**scheduler.WINDOWS_QMT_EDGE_TASKS_BY_TYPE[UPPER]}) == "qmt_windows_edge"


@pytest.mark.parametrize("fault", ["unsigned", "expired", "ambiguous", "activation_missing", "activation_instance", "activation_start", "lease_invalid"])
def test_unverified_peer_blocks_only_the_dependent_candidate(windows_dependencies, fault):
    state = windows_dependencies
    if fault == "unsigned":
        state.ledger.side_effect = RuntimeError("signature unavailable")
    elif fault == "expired":
        state.connection.lease[0]["heartbeat_age_seconds"] = 121
    elif fault == "ambiguous":
        state.connection.lease *= 2
    elif fault == "activation_missing":
        state.connection.activation = []
    elif fault == "activation_instance":
        state.connection.activation[0]["scheduler_instance_id"] = "linux-old"
    elif fault == "activation_start":
        state.connection.current["started_at"] = "2026-09-21T02:02:00"
    else:
        state.lease_check.return_value = (False, {})
    ready, reason = scheduler._strategy_pipeline_dependencies_ready(state.row, state.engine, NOW)
    assert not ready and reason.startswith("dependency_query_failed:")
    assert scheduler._strategy_pipeline_dependencies_ready({"task_type": "qmt_canonical_history_gap_repair"}, state.engine, NOW) == (True, "not_applicable")
    assert "linux" not in state.local_calls


def test_linux_history_cannot_be_relabelled_as_local_windows_build(windows_dependencies):
    state = windows_dependencies
    changed = next(item for item in state.connection.histories if item["task_type"] == "stock_finance")
    changed["build_sha"] = WINDOWS
    ready, reason = scheduler._strategy_pipeline_dependencies_ready(state.row, state.engine, NOW)
    assert not ready and reason == "stock_finance:history_identity_mismatch"


def test_release_dependency_recheck_uses_complete_peer_activation(windows_dependencies, monkeypatch):
    state = windows_dependencies
    rows = [{"task_type": item["task_type"], "proof_build": item["build_sha"]} for item in state.connection.histories]
    monkeypatch.setattr(scheduler, "_release_history_evidence_valid", lambda row, build: row["proof_build"] == build)
    assert scheduler._release_catchup_dependencies_ready(state.row, rows, engine=state.engine) == (True, "ready")
    # The same candidate must re-prove activation on the next observation.
    state.connection.activation = []
    ready, reason = scheduler._release_catchup_dependencies_ready(state.row, rows, engine=state.engine)
    assert not ready and reason.startswith("dependency_build_identity_unavailable:")


def test_blocked_peer_dependency_does_not_abort_owned_gap_dispatch(windows_dependencies, monkeypatch):
    state = windows_dependencies
    state.connection.activation = []
    gap = dict(scheduler.WINDOWS_QMT_EDGE_TASKS_BY_TYPE["qmt_canonical_history_gap_repair"], id=114)
    upper = dict(scheduler.WINDOWS_QMT_EDGE_TASKS_BY_TYPE[UPPER], id=101)
    foreign = {"id": 1, "task_type": "analysis_fast", "task_name": "Linux analysis", "script_path": "biz/analysis/sync_analysis_fast.py", "script_args": ""}
    candidates = [foreign, upper, gap]
    for row in candidates:
        row.update(cron_time="00:15", interval_minutes=0, enabled=1, date_param="",
                   last_run_at=None, last_triggered_at=None, last_run_status="", last_run_duration=0,
                   last_run_output="", _scheduler_target_trade_date=TARGET, _scheduler_target_available=True)
    result = MagicMock()
    keys = tuple(candidates[0])
    result.keys.return_value = keys
    # Main SELECT omits derived fields; here they stand in for read-only target attachment.
    result.fetchall.return_value = [tuple(row.get(key) for key in keys) for row in candidates]
    original_execute = state.connection.execute

    def execute(statement, params=None):
        if "WHERE enabled = 1 ORDER BY sort_order" in str(statement):
            return result
        return original_execute(statement, params)

    monkeypatch.setattr(state.connection, "execute", execute)
    monkeypatch.setattr(scheduler, "get_engine", lambda: state.engine)
    monkeypatch.setattr(scheduler, "_now_shanghai_naive", lambda: NOW)
    for name in ("_write_scheduler_heartbeat", "_retry_pending_terminal_writes", "_cleanup_stale_running_tasks", "_maybe_cleanup_history", "_attach_daily_recovery_targets", "_attach_research_pool_recovery_target"):
        monkeypatch.setattr(scheduler, name, MagicMock())
    monkeypatch.setattr(scheduler, "_release_catchup_disabled_for_deferred_database", lambda: True)
    monkeypatch.setattr(scheduler, "_qmt_windows_loop_activation_ready", lambda *a, **kw: (True, "ready"))
    monkeypatch.setattr(scheduler, "_qmt_windows_dispatch_preflight", lambda *a, **kw: (True, "ready"))
    monkeypatch.setattr(scheduler, "_standalone_heartbeat_allows_dispatch", lambda *a: (True, {}))
    monkeypatch.setattr(scheduler, "_wait_for_scheduler_poll", lambda *a: True)
    monkeypatch.setattr(scheduler, "_should_skip_non_trading_day", lambda *a: False)
    monkeypatch.setattr(scheduler, "_should_skip_outside_intraday_window", lambda *a: False)
    original_due = scheduler._release_build_catchup_allowed

    def owned_due(row, **kwargs):
        assert row["id"] != 1, "other-host task reached candidate evaluation"
        return original_due(row, **kwargs)

    monkeypatch.setattr(scheduler, "_release_build_catchup_allowed", owned_due)
    claims = MagicMock(return_value=True)
    monkeypatch.setattr(scheduler, "_claim_task_run", claims)
    monkeypatch.setattr(scheduler, "_task_history_start", MagicMock(return_value="f" * 32))
    threads = MagicMock()
    monkeypatch.setattr(scheduler.threading, "Thread", threads)
    sets = ("_running_task_ids", "_core_running_task_ids", "_fast_lane_running_task_ids", "_bulk_history_running_task_ids", "_quote_lane_running_task_ids", "_alert_lane_running_task_ids", "_delivery_lane_running_task_ids", "_exclusive_running_task_ids")
    for name in sets:
        monkeypatch.setattr(scheduler, name, set())
    monkeypatch.setattr(scheduler, "_scheduler_stopping", False)
    scheduler._check_and_run_tasks(mode="standalone")
    assert claims.call_count == 1
    assert threads.call_args.kwargs["name"] == "scheduler-task-114"
    assert "linux" not in state.local_calls
