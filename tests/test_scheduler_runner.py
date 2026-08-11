# -*- coding: utf-8 -*-
import json
from pathlib import Path
from subprocess import TimeoutExpired
from unittest.mock import MagicMock, patch

from server.common.scheduler_runner import run_scheduler_task_sync
from server.common.scheduler_validation import SchedulerValidationResult


def test_run_scheduler_task_sync_updates_status_and_returns_result():
    task = {"id": 12, "script_path": "tools/job.py", "script_args": "", "date_param": ""}
    engine = object()
    completed = MagicMock(returncode=0, stdout="ok", stderr="warn")

    with patch(
        "server.common.scheduler_runner.resolve_scheduler_script",
        return_value=Path("E:/repo/tools/job.py"),
    ), patch("server.common.scheduler_runner.Path.is_file", return_value=True), patch(
        "server.common.scheduler_runner.update_scheduler_task"
    ) as update_task, patch(
        "server.common.scheduler_runner.build_scheduler_task_args",
        return_value=["--today"],
    ) as build_args, patch(
        "server.common.scheduler_runner.build_child_env",
        return_value={"MYSQL_URL": "mysql://example"},
    ) as child_env, patch("server.common.scheduler_runner.subprocess.run", return_value=completed) as run:
        result = run_scheduler_task_sync(task, root=Path("E:/repo"), engine=engine, timeout_seconds=120)

    assert result == {"id": 12, "status": "success", "duration": 0, "output": "okwarn"}
    build_args.assert_called_once()
    child_env.assert_called_once_with(Path("E:/repo"), engine=engine)
    cmd = run.call_args.args[0]
    assert cmd[1].replace("\\", "/").endswith("E:/repo/tools/job.py")
    assert cmd[2] == "--today"
    assert run.call_args.kwargs["timeout"] == 120
    assert run.call_args.kwargs["encoding"] == "utf-8"
    assert run.call_args.kwargs["errors"] == "replace"
    assert update_task.call_args_list[0].args[:3] == (engine, 12, {"last_run_status": "running"})
    assert update_task.call_args_list[0].kwargs["now_columns"] == {"last_run_at", "last_triggered_at"}
    assert update_task.call_args_list[1].args[0] is engine
    assert update_task.call_args_list[1].args[1] == 12
    assert update_task.call_args_list[1].args[2]["last_run_status"] == "success"
    assert update_task.call_args_list[1].args[2]["last_run_output"] == "okwarn"


def test_run_scheduler_task_sync_marks_timeout():
    task = {"id": 12, "script_path": "tools/job.py", "script_args": "", "date_param": ""}
    engine = object()

    with patch(
        "server.common.scheduler_runner.resolve_scheduler_script",
        return_value=Path("E:/repo/tools/job.py"),
    ), patch("server.common.scheduler_runner.Path.is_file", return_value=True), patch(
        "server.common.scheduler_runner.update_scheduler_task"
    ) as update_task, patch(
        "server.common.scheduler_runner.build_scheduler_task_args",
        return_value=[],
    ), patch(
        "server.common.scheduler_runner.build_child_env",
        return_value={"MYSQL_URL": "mysql://example"},
    ), patch(
        "server.common.scheduler_runner.subprocess.run",
        side_effect=TimeoutExpired(["python", "job.py"], 60, output="partial out", stderr="partial err"),
    ):
        result = run_scheduler_task_sync(task, root=Path("E:/repo"), engine=engine, timeout_seconds=60)

    assert result["status"] == "timeout"
    assert "manual timeout of 60s" in result["output"]
    final_values = update_task.call_args_list[1].args[2]
    assert final_values["last_run_status"] == "timeout"
    assert "partial out" in final_values["last_run_output"]


def test_run_scheduler_task_sync_fails_when_data_validation_fails():
    task = {"id": 12, "task_type": "stock_kline", "script_path": "tools/job.py", "script_args": "", "date_param": ""}
    engine = object()
    completed = MagicMock(returncode=0, stdout="ok", stderr="")

    with patch(
        "server.common.scheduler_runner.resolve_scheduler_script",
        return_value=Path("E:/repo/tools/job.py"),
    ), patch("server.common.scheduler_runner.Path.is_file", return_value=True), patch(
        "server.common.scheduler_runner.update_scheduler_task"
    ) as update_task, patch(
        "server.common.scheduler_runner.build_scheduler_task_args",
        return_value=[],
    ), patch(
        "server.common.scheduler_runner.build_child_env",
        return_value={"MYSQL_URL": "mysql://example"},
    ), patch("server.common.scheduler_runner.subprocess.run", return_value=completed), patch(
        "server.common.scheduler_runner.validate_scheduler_task_result",
        return_value=SchedulerValidationResult(checked=True, ok=False, message="sm_stock_kline: only 0 rows"),
    ):
        result = run_scheduler_task_sync(task, root=Path("E:/repo"), engine=engine, timeout_seconds=60)

    assert result["status"] == "failed"
    final_values = update_task.call_args_list[1].args[2]
    assert final_values["last_run_status"] == "failed"
    assert "DATA_VALIDATION_FAILED: sm_stock_kline: only 0 rows" in final_values["last_run_output"]


def test_run_scheduler_task_sync_preserves_level1_blocked_semantics():
    task = {
        "id": 67,
        "task_type": "trading_v2_level1_validation",
        "script_path": "tools/validate_trading_v2_level1.py",
        "script_args": "",
        "date_param": "",
    }
    engine = object()
    completed = MagicMock(
        returncode=3,
        stdout=json.dumps({
            "status": "BLOCK",
            "consecutive_trade_days": 0,
            "evidence": {"details": "x" * 6000},
        }) + "\n",
        stderr="",
    )

    with patch(
        "server.common.scheduler_runner.resolve_scheduler_script",
        return_value=Path("E:/repo/tools/validate_trading_v2_level1.py"),
    ), patch("server.common.scheduler_runner.Path.is_file", return_value=True), patch(
        "server.common.scheduler_runner.update_scheduler_task"
    ) as update_task, patch(
        "server.common.scheduler_runner.build_scheduler_task_args",
        return_value=[],
    ), patch(
        "server.common.scheduler_runner.build_child_env",
        return_value={"MYSQL_URL": "mysql://example"},
    ), patch(
        "server.common.scheduler_runner.subprocess.run",
        return_value=completed,
    ), patch(
        "server.common.scheduler_runner.validate_scheduler_task_result"
    ) as validate_result:
        result = run_scheduler_task_sync(
            task,
            root=Path("E:/repo"),
            engine=engine,
            timeout_seconds=60,
        )

    assert result["status"] == "blocked"
    assert update_task.call_args_list[1].args[2]["last_run_status"] == (
        "blocked"
    )
    validate_result.assert_not_called()
