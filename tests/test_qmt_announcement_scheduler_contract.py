from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from server.api.scheduler_runtime import (
    WINDOWS_QMT_BRIDGE_TASK_TYPES,
    _should_skip_task_for_host,
    evaluate_strategy_pipeline_dependencies,
)
from server.common.scheduler_args import build_scheduler_task_args
from server.common.scheduler_validation import scheduler_output_status
from server.common.qmt_announcement_pit import (
    QMT_ANNOUNCEMENT_TASK_SCHEMA,
    validate_task_result,
)
from tools.ensure_quality_gate import TASKS
from tools.add_qmt_announcement_task import (
    _require_unique_operation_tasks,
    _require_unique_task,
    _restore_snapshot,
    _verify_snapshot,
    _write_snapshot,
)
from tools.qmt_announcement_task_contract import (
    QMT_ANNOUNCEMENT_CHECKPOINT_DIR,
    TASK,
    validate_pipeline_order,
)
from tools.qmt_operations_task_contract import TASKS as QMT_OPERATIONS_TASKS
from tools.sync_qmt_announcement_pit import _checkpoint_root
from tools.strategy_governance_task_contract import TASK as GOVERNANCE_TASK


def _result(status="COMPLETE"):
    complete = status == "COMPLETE"
    return {
        "schema": QMT_ANNOUNCEMENT_TASK_SCHEMA,
        "status": status,
        "reason_code": (
            "QMT_ANNOUNCEMENT_FULL_MARKET_COMPLETE"
            if complete else "QMT_ANNOUNCEMENT_NO_PERMISSION_OR_QUERY_FAILED"
        ),
        "detail": "",
        "batch_id": "qmt-ann-20260825T182000-contract" if complete else "",
        "batch_root_hash": "a" * 64 if complete else "",
        "catalog_batch_id": "catalog",
        "catalog_manifest_hash": "b" * 64,
        "catalog_member_set_hash": "c" * 64,
        "stock_count": 5500 if complete else 0,
        "coverage_count": 5500 if complete else 0,
        "event_count": 100,
        "empty_stock_count": 5400,
        "fact_cutoff_at": "2026-08-25T18:20:00.000000",
        "decision_at": "2026-08-25T18:25:00.000000",
        "received_at": "2026-08-25T18:25:00.000000",
        "capture_seconds": 300,
        "window_start": "2026-07-26",
        "window_end": "2026-08-25",
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def _dependency(task_type, at, status="success"):
    return {
        "task_type": task_type,
        "enabled": 1,
        "last_triggered_at": at,
        "last_run_status": status,
    }


def test_frozen_cross_host_pipeline_order_is_qmt_then_analysis_then_governance():
    order = validate_pipeline_order(governance_cron=GOVERNANCE_TASK["cron_time"])
    assert order == {
        "qmt_announcement_minutes": 18 * 60 + 20,
        "analysis_minutes": 18 * 60 + 50,
        "governance_minutes": 22 * 60 + 35,
    }
    with pytest.raises(ValueError, match="30-minute"):
        validate_pipeline_order(analysis_cron="18:51")


def test_task_is_installed_by_quality_gate_and_owned_only_by_windows_qmt_host():
    installed = [item for item in TASKS if item["task_type"] == TASK["task_type"]]
    assert installed == [TASK]
    assert TASK["task_type"] in WINDOWS_QMT_BRIDGE_TASK_TYPES
    row = {"task_type": TASK["task_type"], "script_path": TASK["script_path"]}
    assert _should_skip_task_for_host(row, platform_name="nt") is False
    assert _should_skip_task_for_host(row, platform_name="posix") is True
    assert build_scheduler_task_args(
        TASK, TASK["script_path"], "2026-08-25"
    ) == [
        "--window-days",
        "30",
        "--batch-size",
        "100",
        "--checkpoint-dir",
        QMT_ANNOUNCEMENT_CHECKPOINT_DIR,
    ]


def test_five_frozen_qmt_operations_tasks_are_installed_and_host_owned():
    installed = {
        item["task_type"]: item
        for item in TASKS
        if item["task_type"] in {
            task["task_type"] for task in QMT_OPERATIONS_TASKS
        }
    }

    assert installed == {
        task["task_type"]: task for task in QMT_OPERATIONS_TASKS
    }
    for task_type in (
        "qmt_local_gap_repair_execute",
        "qmt_local_history_2024",
        "qmt_reference_incremental",
    ):
        assert task_type in WINDOWS_QMT_BRIDGE_TASK_TYPES
        assert _should_skip_task_for_host(
            {"task_type": task_type}, platform_name="posix"
        ) is True
        assert _should_skip_task_for_host(
            {"task_type": task_type}, platform_name="nt"
        ) is False


def test_production_deploy_prepares_state_roots_and_upserts_before_health():
    deploy = (
        Path(__file__).resolve().parents[1] / "deploy" / "production_deploy.sh"
    ).read_text(encoding="utf-8")

    assert (
        "QMT_FULL_MARKET_HISTORY_STATE_ROOT="
        "/var/lib/probiga/qmt-full-market-history"
    ) in deploy
    assert (
        "QMT_LOCAL_GAP_REPAIR_STATE_ROOT="
        "/var/lib/probiga/qmt-local-gap-repair"
    ) in deploy
    assert "prepare_qmt_full_market_history_state_root" in deploy
    assert "prepare_qmt_local_gap_repair_state_root" in deploy
    disabled = deploy.index("CUTOVER_STEP=install_qmt_operations_tasks_disabled")
    enabled = deploy.index("CUTOVER_STEP=enable_qmt_operations_tasks")
    new_snapshot = deploy.index(
        "CUTOVER_STEP=capture_qmt_announcement_task_after_enable"
    )
    strict_health = deploy.index("CUTOVER_STEP=verify_strategy_governance_before_start")
    assert disabled < enabled < new_snapshot < strict_health
    assert (
        '"$PREPARED_CODE_ROOT/tools/add_qmt_operations_tasks.py" --disabled'
        in deploy[disabled:enabled]
    )


def test_checkpoint_root_rejects_relative_and_descendant_symlink(
    monkeypatch, tmp_path
):
    monkeypatch.delenv("PROBIGA_DEPLOYMENT_MODE", raising=False)
    with pytest.raises(Exception, match="CHECKPOINT_ROOT_INVALID"):
        _checkpoint_root("relative/checkpoints")

    root = tmp_path / "checkpoints"
    root.mkdir(mode=0o700)
    target = tmp_path / "outside.json"
    target.write_text("{}", encoding="utf-8")
    link = root / "escaped.json"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation is unavailable")
    with pytest.raises(Exception, match="CHECKPOINT_ROOT_INVALID"):
        _checkpoint_root(str(root))


def test_production_checkpoint_root_is_frozen_and_must_preexist(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    with pytest.raises(Exception, match="CHECKPOINT_ROOT_INVALID"):
        _checkpoint_root(str(tmp_path / "mutable-other-root"))


def test_analysis_waits_for_today_terminal_qmt_task_but_accepts_data_blocked_terminal():
    now = datetime(2026, 8, 25, 18, 50)
    ready, reason = evaluate_strategy_pipeline_dependencies(
        "analysis_fast", [], now=now
    )
    assert ready is False
    assert reason == "qmt_announcement_pit:missing_or_duplicate"

    ready, reason = evaluate_strategy_pipeline_dependencies(
        "analysis_fast",
        [_dependency("qmt_announcement_pit", datetime(2026, 8, 25, 18, 20), "blocked")],
        now=now,
    )
    assert ready is True
    assert reason == "ready"


def test_governance_requires_analysis_to_have_run_after_qmt_terminal():
    now = datetime(2026, 8, 25, 22, 35)
    rows = [
        _dependency("qmt_announcement_pit", datetime(2026, 8, 25, 18, 20)),
        _dependency("analysis_fast", datetime(2026, 8, 25, 18, 50)),
    ]
    assert evaluate_strategy_pipeline_dependencies(
        "strategy_governance_daily", rows, now=now
    ) == (True, "ready")
    rows[1]["last_triggered_at"] = datetime(2026, 8, 25, 18, 10)
    assert evaluate_strategy_pipeline_dependencies(
        "strategy_governance_daily", rows, now=now
    )[0] is False


def test_scheduler_maps_machine_complete_and_data_blocked_without_false_success():
    complete = json.dumps(_result(), ensure_ascii=False)
    blocked = json.dumps(_result("DATA_BLOCKED"), ensure_ascii=False)
    assert validate_task_result(json.loads(complete), 0) == "complete"
    assert validate_task_result(json.loads(blocked), 2) == "data_blocked"
    assert scheduler_output_status(TASK, complete, return_code=0) == "success"
    assert scheduler_output_status(TASK, blocked, return_code=2) == "blocked"
    assert scheduler_output_status(
        TASK, complete + "\n" + complete, return_code=0
    ) == "failed"
    assert scheduler_output_status(TASK, complete, return_code=2) == "failed"


def test_scheduler_task_snapshot_restores_exact_predeploy_row(
    monkeypatch, tmp_path
):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_scheduled_tasks (
                id INTEGER PRIMARY KEY,
                task_type TEXT NOT NULL,
                script_path TEXT NOT NULL,
                cron_time TEXT NOT NULL,
                enabled INTEGER NOT NULL
            )
        """))
        connection.execute(
            text("""
                INSERT INTO st_scheduled_tasks
                (id, task_type, script_path, cron_time, enabled)
                VALUES (7, :task_type, :script_path, '18:10', 0)
            """),
            {
                "task_type": TASK["task_type"],
                "script_path": TASK["script_path"],
            },
        )
    snapshot = tmp_path / "qmt-task-before.json"
    _write_snapshot(snapshot, _require_unique_task(engine))
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE st_scheduled_tasks SET cron_time='18:20', enabled=1 "
                "WHERE id=7"
            )
        )
    with pytest.raises(RuntimeError, match="differs from sealed snapshot"):
        _verify_snapshot(engine, snapshot)
    monkeypatch.setattr(
        "tools.add_qmt_announcement_task.table_columns",
        lambda _engine: {"id", "task_type", "script_path", "cron_time", "enabled"},
    )
    assert _restore_snapshot(engine, snapshot)["action"] == (
        "restored_existing_task"
    )
    assert _verify_snapshot(engine, snapshot) == {
        "verified": True,
        "row_count": 1,
        "operation_row_count": 0,
    }


def test_qmt_cutover_snapshot_atomically_covers_all_five_operations_tasks(
    monkeypatch,
    tmp_path,
):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_scheduled_tasks (
                id INTEGER PRIMARY KEY,
                task_type TEXT NOT NULL,
                script_path TEXT NOT NULL,
                cron_time TEXT NOT NULL,
                enabled INTEGER NOT NULL
            )
        """))
        connection.execute(
            text(
                "INSERT INTO st_scheduled_tasks "
                "(id, task_type, script_path, cron_time, enabled) "
                "VALUES (7, :task_type, :script_path, '18:10', 0)"
            ),
            {
                "task_type": TASK["task_type"],
                "script_path": TASK["script_path"],
            },
        )
        for index, task in enumerate(QMT_OPERATIONS_TASKS, 20):
            connection.execute(
                text(
                    "INSERT INTO st_scheduled_tasks "
                    "(id, task_type, script_path, cron_time, enabled) "
                    "VALUES (:id, :task_type, :script_path, :cron_time, 0)"
                ),
                {
                    "id": index,
                    "task_type": task["task_type"],
                    "script_path": task["script_path"],
                    "cron_time": task["cron_time"],
                },
            )
    snapshot = tmp_path / "all-qmt-tasks-before.json"
    _write_snapshot(
        snapshot,
        _require_unique_task(engine),
        _require_unique_operation_tasks(engine),
    )
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE st_scheduled_tasks SET enabled=1, cron_time='23:59'")
        )
    monkeypatch.setattr(
        "tools.add_qmt_announcement_task.table_columns",
        lambda _engine: {
            "id", "task_type", "script_path", "cron_time", "enabled",
        },
    )

    restored = _restore_snapshot(engine, snapshot)

    assert restored["operation_row_count"] == 5
    assert set(restored["actions"]) == {
        TASK["task_type"],
        *(task["task_type"] for task in QMT_OPERATIONS_TASKS),
    }
    assert _verify_snapshot(engine, snapshot) == {
        "verified": True,
        "row_count": 1,
        "operation_row_count": 5,
    }
