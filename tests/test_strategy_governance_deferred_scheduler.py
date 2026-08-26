from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from server.api import scheduler_runtime
from server.api.routers import datasource as datasource_router
from server.api.routers import scheduler as scheduler_router
from server.common.scheduler_authority import DEFERRED_RELEASE_WRITER_TASK_TYPES
from tools import add_strategy_governance_task as task_installer
from tools import run_strategy_governance_daily as daily
from tools.strategy_governance_task_contract import TASK


class _RowsResult:
    def __init__(self, rows=None, *, rowcount=0):
        self._rows = list(rows or [])
        self.rowcount = rowcount

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)


class _DeferredDisableConnection:
    def __init__(self, row: dict):
        self.row = dict(row)
        self.statements: list[str] = []

    def execute(self, statement, params):
        sql = " ".join(str(statement).split())
        self.statements.append(sql)
        if sql.startswith("SELECT * FROM st_scheduled_tasks"):
            return _RowsResult([dict(self.row)])
        if sql.startswith("UPDATE st_scheduled_tasks SET enabled=0"):
            assert params == {
                "task_type": TASK["task_type"],
                "script_path": TASK["script_path"],
                "task_id": self.row["id"],
            }
            changed = int(self.row["enabled"] == 1)
            self.row["enabled"] = 0
            return _RowsResult(rowcount=changed)
        raise AssertionError(f"unexpected SQL: {sql}")


class _DeferredDisableEngine:
    def __init__(self, row: dict):
        self.connection = _DeferredDisableConnection(row)

    def begin(self):
        connection = self.connection

        class _Context:
            def __enter__(self):
                return connection

            def __exit__(self, *_args):
                return False

        return _Context()


def _governance_task_row(*, enabled: int = 1, status: str = "success") -> dict:
    return {
        "id": 917,
        "task_name": "动态策略治理每日更新",
        "task_type": TASK["task_type"],
        "script_path": TASK["script_path"],
        "enabled": enabled,
        "last_run_status": status,
    }


def test_deferred_disable_atomically_changes_only_enabled_and_snapshots(tmp_path):
    engine = _DeferredDisableEngine(_governance_task_row())
    snapshot = tmp_path / "governance-task.json"

    result = task_installer._deferred_disable_task(
        engine,
        snapshot_path=snapshot,
    )

    assert result == {
        "action": "disabled",
        "id": 917,
        "enabled": 0,
        "snapshot_file": str(snapshot),
        "schema_preparation_performed": False,
    }
    assert engine.connection.row["enabled"] == 0
    statements = engine.connection.statements
    assert sum(value.startswith("UPDATE ") for value in statements) == 1
    update = next(value for value in statements if value.startswith("UPDATE "))
    assert "SET enabled=0 WHERE" in update
    assert all("CREATE " not in value.upper() for value in statements)
    payload = json.loads(snapshot.read_text(encoding="utf-8"))
    assert payload["rows"] == [_governance_task_row()]


@pytest.mark.parametrize(
    "rows, message",
    [
        ([], "exactly one"),
        (
            [{**_governance_task_row(), "script_path": "tools/other.py"}],
            "identity is not exact",
        ),
        ([_governance_task_row(status="running")], "while it is running"),
    ],
)
def test_deferred_disable_requires_one_exact_non_running_task(rows, message):
    with pytest.raises(RuntimeError, match=message):
        task_installer._require_exact_deferred_task(rows)


def test_deferred_disable_main_requires_exact_mode_before_engine(monkeypatch):
    monkeypatch.setenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", "REQUIRED")
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setattr(task_installer, "load_project_env", lambda: None)
    monkeypatch.setattr(
        task_installer,
        "create_tool_engine",
        lambda: pytest.fail("database engine created before deferred mode guard"),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["add_strategy_governance_task.py", "--deferred-disable"],
    )

    with pytest.raises(SystemExit) as caught:
        task_installer.main()

    assert caught.value.code == 2


def test_deferred_disable_main_never_enters_schema_preparation(monkeypatch, capsys):
    engine = MagicMock()
    monkeypatch.setenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", "DEFERRED_DB")
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setattr(task_installer, "load_project_env", lambda: None)
    monkeypatch.setattr(task_installer, "create_tool_engine", lambda: engine)
    disable = MagicMock(return_value={
        "action": "already_disabled",
        "id": 917,
        "enabled": 0,
        "snapshot_file": None,
        "schema_preparation_performed": False,
    })
    monkeypatch.setattr(task_installer, "_deferred_disable_task", disable)
    monkeypatch.setattr(
        sys,
        "argv",
        ["add_strategy_governance_task.py", "--deferred-disable"],
    )

    assert task_installer.main() == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["deferred_disabled"] is True
    assert payload["result"]["schema_preparation_performed"] is False
    disable.assert_called_once_with(engine, snapshot_path=None)
    engine.dispose.assert_called_once()


def test_deferred_mode_rejects_manual_launch_before_claim(monkeypatch):
    claim = MagicMock()
    thread = MagicMock()
    monkeypatch.setenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", "DEFERRED_DB")
    monkeypatch.setattr(scheduler_runtime, "_claim_task_run", claim)
    monkeypatch.setattr(scheduler_runtime.threading, "Thread", thread)

    result = scheduler_runtime.launch_scheduler_task(
        _governance_task_row(),
        root=Path("E:/fake"),
        engine=MagicMock(),
    )

    assert result == {
        "accepted": False,
        "status": "governance_database_deferred",
        "task_id": 917,
        "task_name": "动态策略治理每日更新",
        "job_id": "",
    }
    claim.assert_not_called()
    thread.assert_not_called()


def test_deferred_mode_auto_dispatch_never_claims_governance(monkeypatch):
    stop_event = MagicMock()
    stop_event.is_set.return_value = False
    stop_event.wait.return_value = True
    engine = MagicMock()
    result = MagicMock()
    result.keys.return_value = [
        "id",
        "task_name",
        "task_type",
        "script_path",
        "script_args",
        "cron_time",
        "interval_minutes",
        "enabled",
        "date_param",
        "last_run_at",
        "last_triggered_at",
        "last_run_status",
        "last_run_duration",
    ]
    result.fetchall.return_value = [(
        917,
        "动态策略治理每日更新",
        TASK["task_type"],
        TASK["script_path"],
        "",
        "00:00",
        1,
        1,
        "",
        None,
        None,
        "success",
        0,
    )]
    engine.connect.return_value.__enter__.return_value.execute.return_value = result
    claim = MagicMock()
    thread = MagicMock()
    monkeypatch.setenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", "DEFERRED_DB")
    monkeypatch.setattr(scheduler_runtime, "get_engine", lambda: engine)
    monkeypatch.setattr(
        scheduler_runtime,
        "get_scheduler_runtime_config",
        lambda: {"poll_seconds": 15, "max_concurrent_tasks": 1},
    )
    monkeypatch.setattr(scheduler_runtime, "_write_scheduler_heartbeat", lambda *_: None)
    monkeypatch.setattr(
        scheduler_runtime,
        "_standalone_heartbeat_allows_dispatch",
        lambda *_: (True, {"errors": []}),
    )
    monkeypatch.setattr(scheduler_runtime, "_cleanup_stale_running_tasks", lambda *_: None)
    monkeypatch.setattr(scheduler_runtime, "_maybe_cleanup_history", lambda *_: None)
    monkeypatch.setattr(scheduler_runtime, "_claim_task_run", claim)
    monkeypatch.setattr(scheduler_runtime.threading, "Thread", thread)

    scheduler_runtime._check_and_run_tasks(
        mode="standalone",
        stop_event=stop_event,
    )

    claim.assert_not_called()
    thread.assert_not_called()


def test_deferred_mode_disables_release_replay_without_freezing_ordinary_data(
    monkeypatch,
):
    monkeypatch.setenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", "DEFERRED_DB")
    row = {
        "id": 918,
        "task_name": "daily finance",
        "task_type": "stock_finance",
        "cron_time": "00:00",
        "interval_minutes": 1,
        "last_run_at": None,
        "last_triggered_at": None,
        "last_run_status": "success",
        "_release_history_available": True,
        "_release_catchup_authorized": True,
    }

    assert scheduler_runtime._release_build_catchup_allowed(
        row,
        now=scheduler_runtime.datetime.now(),
    ) is False
    assert scheduler_runtime._release_build_catchup_pending(row) is False
    assert scheduler_runtime.strategy_governance_task_block_reason(row) == ""

    authorized, reason = scheduler_runtime._attach_release_catchup_authorization(
        MagicMock(),
        [row],
        mode="standalone",
        now=scheduler_runtime.datetime.now(),
    )
    assert authorized is False
    assert reason == "governance_database_deferred"
    assert row["_release_catchup_authorized"] is False


def test_deferred_mode_keeps_governance_and_v3_writer_fences(monkeypatch):
    monkeypatch.setenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", "DEFERRED_DB")

    assert scheduler_runtime.strategy_governance_task_block_reason(
        _governance_task_row()
    ) == "governance_database_deferred"
    assert {
        "trading_v3_close_decision",
        "trading_v3_premarket_review",
    }.issubset(DEFERRED_RELEASE_WRITER_TASK_TYPES)


def test_deferred_scheduler_launches_due_non_governance_data_task(monkeypatch):
    stop_event = MagicMock()
    stop_event.is_set.return_value = False
    stop_event.wait.return_value = True
    engine = MagicMock()
    result = MagicMock()
    result.keys.return_value = [
        "id",
        "task_name",
        "task_type",
        "script_path",
        "script_args",
        "cron_time",
        "interval_minutes",
        "enabled",
        "date_param",
        "last_run_at",
        "last_triggered_at",
        "last_run_status",
        "last_run_duration",
    ]
    result.fetchall.return_value = [(
        918,
        "daily finance",
        "stock_finance",
        "biz/stock_finance/sync_finance.py",
        "",
        "00:00",
        1,
        1,
        "",
        None,
        None,
        "success",
        0,
    )]
    engine.connect.return_value.__enter__.return_value.execute.return_value = result
    claim = MagicMock(return_value=True)
    thread = MagicMock()
    monkeypatch.setenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", "DEFERRED_DB")
    monkeypatch.setattr(scheduler_runtime, "get_engine", lambda: engine)
    monkeypatch.setattr(
        scheduler_runtime,
        "get_scheduler_runtime_config",
        lambda: {"poll_seconds": 15, "max_concurrent_tasks": 1},
    )
    monkeypatch.setattr(scheduler_runtime, "_write_scheduler_heartbeat", lambda *_: None)
    monkeypatch.setattr(
        scheduler_runtime,
        "_standalone_heartbeat_allows_dispatch",
        lambda *_: (True, {"errors": []}),
    )
    monkeypatch.setattr(scheduler_runtime, "_cleanup_stale_running_tasks", lambda *_: None)
    monkeypatch.setattr(scheduler_runtime, "_maybe_cleanup_history", lambda *_: None)
    monkeypatch.setattr(
        scheduler_runtime,
        "_attach_release_catchup_history",
        lambda *_: pytest.fail("deferred mode must not load release history"),
    )
    monkeypatch.setattr(
        scheduler_runtime,
        "_attach_release_catchup_expected_targets",
        lambda *_args, **_kwargs: pytest.fail(
            "deferred mode must not resolve release targets"
        ),
    )
    monkeypatch.setattr(
        scheduler_runtime,
        "_should_skip_task_for_host",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        scheduler_runtime,
        "_strategy_pipeline_dependencies_ready",
        lambda *_args, **_kwargs: (True, "ready"),
    )
    monkeypatch.setattr(
        scheduler_runtime,
        "_should_skip_non_trading_day",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        scheduler_runtime,
        "_should_skip_outside_intraday_window",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        scheduler_runtime,
        "_scheduler_lane_has_capacity",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(scheduler_runtime, "_claim_task_run", claim)
    monkeypatch.setattr(
        scheduler_runtime,
        "_task_history_start",
        lambda *_args, **_kwargs: "a" * 32,
    )
    monkeypatch.setattr(scheduler_runtime.threading, "Thread", thread)
    scheduler_runtime._running_task_ids.discard(918)

    scheduler_runtime._check_and_run_tasks(
        mode="standalone",
        stop_event=stop_event,
    )

    claim.assert_called_once()
    thread.assert_called_once()
    launched_row = thread.call_args.kwargs["args"][0]
    assert launched_row["task_type"] == "stock_finance"
    assert launched_row.get("_trigger_source") != "release_catchup"
    scheduler_runtime._running_task_ids.discard(918)


def test_deferred_mode_toggle_allows_disable_but_rejects_enable(monkeypatch):
    update = MagicMock()
    monkeypatch.setenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", "DEFERRED_DB")
    monkeypatch.setattr(scheduler_router, "update_scheduler_task", update)
    monkeypatch.setattr(scheduler_router, "get_engine", lambda: "engine")

    monkeypatch.setattr(
        scheduler_router,
        "_read_sql",
        lambda *_args, **_kwargs: [_governance_task_row(enabled=0)],
    )
    rejected = scheduler_router.toggle_task(917)
    assert rejected["enabled"] == 0
    assert rejected["status"] == "governance_database_deferred"
    update.assert_not_called()

    monkeypatch.setattr(
        scheduler_router,
        "_read_sql",
        lambda *_args, **_kwargs: [_governance_task_row(enabled=1)],
    )
    allowed = scheduler_router.toggle_task(917)
    assert allowed == {"id": 917, "enabled": 0}
    update.assert_called_once_with("engine", 917, {"enabled": 0})


def test_deferred_mode_datasource_toggle_cannot_reenable_governance(monkeypatch):
    execute = MagicMock()
    monkeypatch.setenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", "DEFERRED_DB")
    monkeypatch.setattr(datasource_router, "_execute_sql", execute)

    monkeypatch.setattr(
        datasource_router,
        "_read_sql",
        lambda *_args, **_kwargs: [_governance_task_row(enabled=0)],
    )
    rejected = datasource_router.toggle_task(917)
    assert rejected["enabled"] == 0
    assert rejected["status"] == "governance_database_deferred"
    execute.assert_not_called()

    monkeypatch.setattr(
        datasource_router,
        "_read_sql",
        lambda *_args, **_kwargs: [_governance_task_row(enabled=1)],
    )
    allowed = datasource_router.toggle_task(917)
    assert allowed == {"id": 917, "enabled": 0}
    execute.assert_called_once()


def test_daily_governance_deferred_mode_blocks_before_engine(monkeypatch, capsys):
    monkeypatch.setattr(
        daily,
        "_load_project_env",
        lambda: monkeypatch.setenv(
            "PROBIGA_STRATEGY_GOVERNANCE_MODE", "DEFERRED_DB"
        ),
    )
    monkeypatch.setattr(
        daily,
        "_create_tool_engine",
        lambda: pytest.fail("deferred governance created a database engine"),
    )
    monkeypatch.setattr(sys, "argv", ["run_strategy_governance_daily.py"])

    assert daily.main() == 2

    payload = json.loads(capsys.readouterr().out)
    assert payload == daily._deferred_database_blocked_output()
    assert payload["reason_code"] == "GOVERNANCE_DATABASE_DEFERRED"
    assert payload["allocations"] == [{
        "target_type": "CASH",
        "simulated_weight_pct": 100.0,
        "real_order_authority": False,
    }]
    assert daily.validate_cli_result(payload, 2) == "not_ready"
