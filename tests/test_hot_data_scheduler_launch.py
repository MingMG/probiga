import inspect
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from server.api.routers import hot_data


def _registered_task(
    task_type: str,
    script_path: str = "biz/review/generate.py",
) -> dict:
    return {
        "id": 9201,
        "task_name": f"registered {task_type}",
        "task_type": task_type,
        "script_path": script_path,
        "script_args": "",
        "date_param": "persisted-value",
        "enabled": 1,
    }


def test_news_refresh_launches_only_the_formal_registered_sync_task():
    engine = MagicMock()
    with patch.object(hot_data, "get_engine", return_value=engine), patch.object(
        hot_data,
        "launch_registered_manual_task",
        return_value={"accepted": True, "status": "running", "job_id": "news-1"},
    ) as launch:
        result = hot_data.refresh_news_flash()

    launch.assert_called_once_with(
        engine,
        task_type="news_sync",
        expected_script_path="tools/sync_news_formal.py",
        script_args="--pages 2 --json",
        root=hot_data._ROOT,
    )
    assert result["accepted"] is True


def test_registered_hot_data_task_uses_unique_row_copy_and_launcher():
    persisted = _registered_task("daily_review")
    engine = MagicMock()
    launch_result = {
        "accepted": True,
        "status": "running",
        "task_id": persisted["id"],
        "job_id": "audit-run-9201",
    }
    with patch.object(hot_data, "_read_sql", return_value=[persisted]) as read_sql, patch.object(
        hot_data, "get_engine", return_value=engine
    ), patch.object(
        hot_data, "launch_scheduler_task", return_value=launch_result
    ) as launch:
        result = hot_data._launch_registered_scheduler_task(
            task_type="daily_review",
            expected_script_path="biz/review/generate.py",
            run_date="2026-08-25",
        )

    query = read_sql.call_args.args[0]
    assert "WHERE task_type = :task_type" in query
    assert "LIMIT 2" in query
    assert read_sql.call_args.args[1] == {"task_type": "daily_review"}
    launched_row = launch.call_args.args[0]
    assert launched_row is not persisted
    assert launched_row["date_param"] == "2026-08-25"
    assert persisted["date_param"] == "persisted-value"
    assert launch.call_args.kwargs == {"root": hot_data._ROOT, "engine": engine}
    assert result == {**launch_result, "task_type": "daily_review"}


@pytest.mark.parametrize(
    ("rows", "expected_status"),
    [
        ([], "task_registration_missing"),
        (
            [_registered_task("daily_review"), _registered_task("daily_review")],
            "task_registration_ambiguous",
        ),
    ],
)
def test_registered_hot_data_task_requires_exactly_one_row(rows, expected_status):
    with patch.object(hot_data, "_read_sql", return_value=rows), patch.object(
        hot_data, "launch_scheduler_task"
    ) as launch:
        result = hot_data._launch_registered_scheduler_task(
            task_type="daily_review",
            expected_script_path="biz/review/generate.py",
            run_date="2026-08-25",
        )

    assert result["accepted"] is False
    assert result["status"] == expected_status
    assert result["job_id"] == ""
    launch.assert_not_called()


def test_registered_hot_data_task_rejects_non_iso_date_before_database_read():
    with patch.object(hot_data, "_read_sql") as read_sql:
        with pytest.raises(ValueError):
            hot_data._launch_registered_scheduler_task(
                task_type="daily_review",
                expected_script_path="biz/review/generate.py",
                run_date="2026-08-25 --force",
            )
    read_sql.assert_not_called()


def test_registered_hot_data_task_rejects_script_contract_drift():
    row = _registered_task("daily_review", "tools/another_allowed_script.py")
    with patch.object(hot_data, "_read_sql", return_value=[row]), patch.object(
        hot_data, "launch_scheduler_task"
    ) as launch:
        result = hot_data._launch_registered_scheduler_task(
            task_type="daily_review",
            expected_script_path="biz/review/generate.py",
            run_date="2026-08-25",
        )

    assert result["accepted"] is False
    assert result["status"] == "task_contract_mismatch"
    launch.assert_not_called()


@pytest.mark.parametrize(
    ("operation", "task_type", "script_args"),
    [
        (
            "snapshot",
            "intraday_realtime",
            "--only snapshot --min-coverage 0.70 --archive-snapshot --skip-closed --json",
        ),
    ],
)
def test_realtime_refresh_uses_exact_registered_task_contract(
    operation,
    task_type,
    script_args,
):
    persisted = _registered_task(task_type, "tools/crawl_realtime_batch.py")
    engine = MagicMock()
    with patch.object(hot_data, "_read_sql", return_value=[persisted]), patch.object(
        hot_data, "get_engine", return_value=engine
    ), patch.object(
        hot_data,
        "launch_scheduler_task",
        return_value={
            "accepted": True,
            "status": "running",
            "task_id": persisted["id"],
            "job_id": "audit-run-realtime",
        },
    ) as launch:
        result = hot_data.realtime_refresh(operation)

    launched_row = launch.call_args.args[0]
    assert launched_row is not persisted
    assert launched_row["task_type"] == task_type
    assert launched_row["script_path"] == "tools/crawl_realtime_batch.py"
    assert launched_row["script_args"] == script_args
    assert launched_row["date_param"] == ""
    assert persisted["script_args"] == ""
    assert persisted["date_param"] == "persisted-value"
    assert result["accepted"] is True
    assert result["success"] is True
    assert result["status"] == "running"
    assert result["job_id"] == "audit-run-realtime"
    assert "已提交后台执行" in result["output"]


@pytest.mark.parametrize("operation", ["all", "flow", "concept", "index"])
def test_realtime_refresh_rejects_operation_without_unique_task(operation):
    with patch.object(hot_data, "_launch_registered_scheduler_task") as launch:
        result = hot_data.realtime_refresh(operation)

    assert result["accepted"] is False
    assert result["success"] is False
    assert result["status"] == "operation_not_registered"
    assert result["job_id"] == ""
    launch.assert_not_called()


@pytest.mark.parametrize(
    "launch_status",
    [
        "task_registration_missing",
        "task_registration_ambiguous",
        "delegated_to_other_host",
        "build_identity_unavailable",
        "disabled",
        "already_running",
        "audit_unavailable",
    ],
)
def test_realtime_refresh_preserves_fail_closed_launcher_decision(launch_status):
    with patch.object(
        hot_data,
        "_launch_registered_scheduler_task",
        return_value={
            "accepted": False,
            "status": launch_status,
            "job_id": "",
        },
    ) as launch:
        result = hot_data.realtime_refresh("snapshot")

    launch.assert_called_once_with(
        task_type="intraday_realtime",
        expected_script_path="tools/crawl_realtime_batch.py",
        script_args_override=(
            "--only snapshot --min-coverage 0.70 "
            "--archive-snapshot --skip-closed --json"
        ),
    )
    assert result["accepted"] is False
    assert result["success"] is False
    assert result["status"] == launch_status
    assert result["job_id"] == ""


@pytest.mark.parametrize(
    ("route", "task_type", "script_path"),
    [
        (hot_data.generate_daily_review, "daily_review", "biz/review/generate.py"),
        (
            hot_data.sync_sector_heat_today,
            "sector_heat_east",
            "tools/fetch_sector_heat_east_daily.py",
        ),
    ],
)
def test_hot_data_write_routes_return_async_job_identity(route, task_type, script_path):
    with patch.object(
        hot_data,
        "_launch_registered_scheduler_task",
        return_value={
            "accepted": True,
            "status": "running",
            "task_id": 9202,
            "job_id": "audit-run-9202",
        },
    ) as launch:
        result = route("2026-08-25")

    launch.assert_called_once_with(
        task_type=task_type,
        expected_script_path=script_path,
        run_date="2026-08-25",
    )
    assert result["accepted"] is True
    assert result["job_id"] == "audit-run-9202"
    assert result["status"] == "running"
    assert result["launch_status"] == "running"
    if task_type == "daily_review":
        assert result["duration"] == 0
    else:
        assert result["synced"] is None


@pytest.mark.parametrize(
    "launch_status",
    [
        "delegated_to_other_host",
        "build_identity_unavailable",
        "disabled",
        "already_running",
        "audit_unavailable",
    ],
)
@pytest.mark.parametrize(
    ("route", "task_type", "script_path"),
    [
        (hot_data.generate_daily_review, "daily_review", "biz/review/generate.py"),
        (
            hot_data.sync_sector_heat_today,
            "sector_heat_east",
            "tools/fetch_sector_heat_east_daily.py",
        ),
    ],
)
def test_hot_data_write_routes_preserve_fail_closed_launcher_decision(
    route,
    task_type,
    script_path,
    launch_status,
):
    with patch.object(
        hot_data,
        "_launch_registered_scheduler_task",
        return_value={
            "accepted": False,
            "status": launch_status,
            "job_id": "",
        },
    ) as launch:
        result = route("2026-08-25")

    launch.assert_called_once_with(
        task_type=task_type,
        expected_script_path=script_path,
        run_date="2026-08-25",
    )
    assert result["accepted"] is False
    assert result["status"] == launch_status
    assert result["launch_status"] == launch_status
    assert result["job_id"] == ""


def test_hot_data_write_routes_do_not_start_subprocesses_directly():
    for route in (
        hot_data.generate_daily_review,
        hot_data.sync_sector_heat_today,
        hot_data.realtime_refresh,
    ):
        source = inspect.getsource(route)
        assert "subprocess" not in source
        assert "_launch_registered_scheduler_task" in source
    assert "_job_begin" not in inspect.getsource(hot_data.realtime_refresh)


def test_hot_data_write_buttons_report_submission_not_false_completion():
    source = (
        Path(__file__).resolve().parents[1] / "server" / "static" / "js" / "app.js"
    ).read_text(encoding="utf-8")
    review = source.split("window.genReviewBtn = function", 1)[1].split(
        "window.exportReview", 1
    )[0]
    sector = source.split("window.syncSectorHeatBtn = function", 1)[1].split(
        "function getHeatColor", 1
    )[0]

    assert "if (!res.accepted)" in review
    assert "复盘生成任务已提交后台执行" in review
    assert "生成成功" not in review
    assert "LOADERS.review" not in review
    assert "if (!res.accepted)" in sector
    assert "东财同步任务已提交后台执行" in sector
    assert "条数据已入库" not in sector
    assert "switchSubView('sector', 'heat')" not in sector
