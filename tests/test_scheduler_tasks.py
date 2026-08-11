from __future__ import annotations

from unittest.mock import MagicMock

from server.common.scheduler_tasks import TASK_PAYLOAD_COLUMNS, task_payload, update_scheduler_tasks


def test_task_payload_filters_to_known_existing_scheduler_columns():
    task = {
        "task_name": "quality",
        "task_type": "quality_check",
        "script_path": "tools/data_quality_check.py",
        "script_args": "--json",
        "cron_time": "08:45",
        "enabled": 1,
        "ignored": "x",
    }
    columns = {"task_name", "script_path", "cron_time", "enabled", "ignored"}

    assert task_payload(task, columns, allowed_columns=TASK_PAYLOAD_COLUMNS) == {
        "task_name": "quality",
        "script_path": "tools/data_quality_check.py",
        "cron_time": "08:45",
        "enabled": 1,
    }


def test_update_scheduler_tasks_filters_columns_and_adds_now_updates():
    connect_conn = MagicMock()
    connect_conn.execute.return_value.fetchall.return_value = [
        ("id",),
        ("enabled",),
        ("last_run_at",),
        ("updated_at",),
    ]
    connect_ctx = MagicMock()
    connect_ctx.__enter__.return_value = connect_conn

    begin_conn = MagicMock()
    begin_conn.execute.return_value.rowcount = 1
    begin_ctx = MagicMock()
    begin_ctx.__enter__.return_value = begin_conn

    engine = MagicMock()
    engine.connect.return_value = connect_ctx
    engine.begin.return_value = begin_ctx

    rowcount = update_scheduler_tasks(
        engine,
        {"enabled": 0, "missing_column": "ignored"},
        lookup_where="id = :id",
        lookup_params={"id": 42},
        now_columns={"last_run_at"},
    )

    assert rowcount == 1
    sql = str(begin_conn.execute.call_args.args[0])
    params = begin_conn.execute.call_args.args[1]
    assert "`enabled` = :enabled" in sql
    assert "`last_run_at` = NOW()" in sql
    assert "`updated_at` = NOW()" in sql
    assert "missing_column" not in sql
    assert params == {"enabled": 0, "id": 42}
