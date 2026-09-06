from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from server.api import scheduler_runtime
from server.api.routers import datasource


def _task_row(*, task_id: int = 9101, enabled: int = 1) -> dict:
    return {
        "id": task_id,
        "task_name": "datasource task",
        "task_type": "analysis_fast",
        "script_path": "biz/analysis/sync_analysis_fast.py",
        "script_args": "",
        "date_param": "",
        "enabled": enabled,
    }


def test_capital_flow_batch_is_grouped_under_eastmoney():
    providers = [
        provider
        for provider, config in datasource.DATASOURCE_CONFIG.items()
        if any(
            "capital_flow_batch_fast" in task_types
            for task_types in config["types"].values()
        )
    ]

    assert providers == ["东财"]


def test_datasource_run_uses_claimed_audited_scheduler_launcher():
    row = _task_row()
    engine = MagicMock()
    launch_result = {
        "accepted": True,
        "status": "running",
        "task_id": row["id"],
        "task_name": row["task_name"],
        "job_id": "audit-run-9101",
    }

    with patch.object(datasource, "_read_sql", return_value=[row]), patch.object(
        datasource, "get_engine", return_value=engine
    ), patch.object(
        datasource, "launch_scheduler_task", return_value=launch_result
    ) as launch:
        result = datasource.run_task(row["id"])

    launch.assert_called_once()
    assert launch.call_args.args == (row,)
    assert launch.call_args.kwargs["root"] == Path(datasource.__file__).resolve().parents[3]
    assert launch.call_args.kwargs["engine"] is engine
    assert result == {
        "id": row["id"],
        "duration": 0,
        "output": "任务已提交后台执行",
        **launch_result,
    }


def test_datasource_run_missing_task_does_not_launch():
    with patch.object(datasource, "_read_sql", return_value=[]), patch.object(
        datasource, "launch_scheduler_task"
    ) as launch:
        result = datasource.run_task(999999)

    assert result == {"error": "任务不存在"}
    launch.assert_not_called()


@pytest.mark.parametrize(
    ("runtime_patch", "enabled", "expected_status"),
    [
        ("other_host", 1, "delegated_to_other_host"),
        ("disabled", 0, "disabled"),
        ("already_claimed", 1, "already_running"),
    ],
)
def test_datasource_launcher_contract_fails_closed_before_worker(
    runtime_patch: str,
    enabled: int,
    expected_status: str,
):
    row = _task_row(task_id=9102, enabled=enabled)
    claim_result = runtime_patch != "already_claimed"
    other_host = runtime_patch == "other_host"

    with patch(
        "server.api.scheduler_runtime._should_skip_task_for_host",
        return_value=other_host,
    ), patch(
        "server.api.scheduler_runtime._claim_task_run",
        return_value=claim_result,
    ) as claim, patch(
        "server.api.scheduler_runtime._task_history_start"
    ) as history_start, patch(
        "server.api.scheduler_runtime.threading.Thread"
    ) as thread_cls, patch.dict(
        "server.api.scheduler_runtime.os.environ", {}, clear=True
    ):
        result = scheduler_runtime.launch_scheduler_task(
            row,
            root=Path("E:/fake"),
            engine=MagicMock(),
        )

    assert result["accepted"] is False
    assert result["status"] == expected_status
    history_start.assert_not_called()
    thread_cls.assert_not_called()
    if runtime_patch in {"other_host", "disabled"}:
        claim.assert_not_called()
    else:
        claim.assert_called_once()


def test_datasource_launcher_contract_requires_production_build_identity():
    row = _task_row(task_id=9103)
    with patch(
        "server.api.scheduler_runtime._should_skip_task_for_host",
        return_value=False,
    ), patch(
        "server.api.scheduler_runtime._claim_task_run"
    ) as claim, patch(
        "server.api.scheduler_runtime.threading.Thread"
    ) as thread_cls, patch.dict(
        "server.api.scheduler_runtime.os.environ",
        {"PROBIGA_DEPLOYMENT_MODE": "production"},
        clear=True,
    ):
        result = scheduler_runtime.launch_scheduler_task(
            row,
            root=Path("E:/fake"),
            engine=MagicMock(),
        )

    assert result["accepted"] is False
    assert result["status"] == "build_identity_unavailable"
    claim.assert_not_called()
    thread_cls.assert_not_called()


def test_datasource_launcher_contract_rejects_missing_audit_before_worker():
    row = _task_row(task_id=9104)
    with patch(
        "server.api.scheduler_runtime._should_skip_task_for_host",
        return_value=False,
    ), patch(
        "server.api.scheduler_runtime._claim_task_run",
        return_value=True,
    ), patch(
        "server.api.scheduler_runtime._task_history_start",
        return_value=None,
    ), patch(
        "server.api.scheduler_runtime.update_scheduler_task"
    ) as update_task, patch(
        "server.api.scheduler_runtime.threading.Thread"
    ) as thread_cls, patch.dict(
        "server.api.scheduler_runtime.os.environ", {}, clear=True
    ):
        result = scheduler_runtime.launch_scheduler_task(
            row,
            root=Path("E:/fake"),
            engine=MagicMock(),
        )

    assert result["accepted"] is False
    assert result["status"] == "audit_unavailable"
    assert update_task.call_args.args[2]["last_run_status"] == "failed"
    thread_cls.assert_not_called()


def test_datasource_launcher_runtime_blocks_unsafe_script_with_terminal_audit():
    row = {
        **_task_row(task_id=9105),
        "script_path": "../outside.py",
    }
    engine = MagicMock()
    with patch(
        "server.api.scheduler_runtime._task_history_start",
        return_value="a" * 32,
    ), patch(
        "server.api.scheduler_runtime.start_daily_stage_attempt",
        return_value=None,
    ), patch(
        "server.api.scheduler_runtime.resolve_scheduler_script",
        side_effect=scheduler_runtime.SchedulerScriptPolicyError("outside root"),
    ), patch(
        "server.api.scheduler_runtime.update_scheduler_task"
    ) as update_task, patch(
        "server.api.scheduler_runtime._task_history_finish"
    ) as history_finish:
        scheduler_runtime._run_task(row, Path("E:/fake"), engine)

    assert update_task.call_args.args[2]["last_run_status"] == "failed"
    assert "SCHEDULER_SCRIPT_BLOCKED" in update_task.call_args.args[2]["last_run_output"]
    assert history_finish.call_args.kwargs["status"] == "failed"
    assert history_finish.call_args.kwargs["exit_code"] == 126
