from __future__ import annotations

import pytest
from sqlalchemy import create_engine, event, text

from server.common.scheduler_tasks import DEFAULT_SCHEDULER_COLUMNS, upsert_scheduler_task
from tools import add_qmt_operations_tasks as installer
from tools import add_qmt_announcement_task as announcement
from tools.qmt_operations_task_contract import (
    QMT_FULL_HISTORY_LOCK_PATH,
    QMT_FULL_HISTORY_LOG_PATH,
    QMT_FULL_HISTORY_STATE_ROOT,
    QMT_GAP_REPAIR_LOCK_PATH,
    QMT_GAP_REPAIR_STATE_ROOT,
    TASKS,
)


def test_frozen_qmt_operations_inventory_limits_active_repairs_to_recent_contract():
    assert len(TASKS) == 5
    assert len({task["task_type"] for task in TASKS}) == 5
    assert len({task["script_path"] for task in TASKS}) == 5
    assert {task["task_type"] for task in TASKS if not task["enabled"]} == {
        "qmt_local_history_2024", "qmt_local_gap_repair_execute"
    }
    reference = next(task for task in TASKS if task["task_type"] == "qmt_reference_incremental")
    assert "--catalog-only" in reference["script_args"]
    assert "--include-calendar" in reference["script_args"]
    full_history = next(
        task for task in TASKS if task["task_type"] == "qmt_local_history_2024"
    )
    gap_repair = next(
        task
        for task in TASKS
        if task["task_type"] == "qmt_local_gap_repair_execute"
    )
    assert f"--state-root {QMT_FULL_HISTORY_STATE_ROOT}" in full_history[
        "script_args"
    ]
    assert f"--lock-path {QMT_FULL_HISTORY_LOCK_PATH}" in full_history[
        "script_args"
    ]
    assert f"--log-path {QMT_FULL_HISTORY_LOG_PATH}" in full_history[
        "script_args"
    ]
    assert "--start-date 2026-01-01" in full_history["script_args"]
    assert "2024-01-01" not in full_history["script_args"]
    assert f"--state-root {QMT_GAP_REPAIR_STATE_ROOT}" in gap_repair[
        "script_args"
    ]
    assert f"--lock-path {QMT_GAP_REPAIR_LOCK_PATH}" in gap_repair[
        "script_args"
    ]
    assert all("ROOT/data" not in task["script_args"] for task in TASKS)
    assert all("data/runtime" not in task["script_args"] for task in TASKS)


def test_clean_inventory_install_upserts_all_five_disabled_then_enabled(
    monkeypatch,
):
    calls = []
    monkeypatch.setattr(installer, "_require_unique_tasks", lambda _engine: [])
    monkeypatch.setattr(
        installer,
        "upsert_scheduler_task",
        lambda engine, payload, **kwargs: calls.append(
            (engine, dict(payload), dict(kwargs))
        )
        or {"id": len(calls), "action": "inserted"},
    )
    engine = object()

    disabled = installer.install(engine, disabled=True)
    enabled = installer.install(engine, disabled=False)

    assert disabled["task_count"] == 5
    assert disabled["enabled"] is False
    assert enabled["task_count"] == 5
    assert enabled["enabled"] is True
    assert len(calls) == 10
    assert [call[1]["task_type"] for call in calls[:5]] == [
        task["task_type"] for task in TASKS
    ]
    assert all(call[1]["enabled"] == 0 for call in calls[:5])
    assert [call[1]["enabled"] for call in calls[5:]] == [task["enabled"] for task in TASKS]
    assert all(
        call[2]["lookup_where"]
        == "task_type=:task_type OR script_path=:script_path"
        for call in calls
    )


def test_bulk_history_prioritizes_2026_without_older_dates():
    task = next(task for task in TASKS if task["task_type"] == "qmt_local_history_2024")
    assert "--start-date 2026-01-01" in task["script_args"]
    assert "--start-date 2024-01-01" not in task["script_args"]


def _operations_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:")

    @event.listens_for(engine, "connect")
    def mysql_functions(connection, _record):
        connection.create_function("NOW", 0, lambda: "2026-11-03 06:00:00")
        connection.create_function(
            "LAST_INSERT_ID", 0,
            lambda: connection.execute("SELECT last_insert_rowid()").fetchone()[0],
        )

    columns = {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "task_name": "TEXT NOT NULL", "script_path": "TEXT NOT NULL",
        "cron_time": "TEXT NOT NULL", "enabled": "INTEGER NOT NULL",
        "last_run_status": "TEXT", "last_run_at": "DATETIME",
        "created_at": "DATETIME", "notification_policy": "TEXT",
        **DEFAULT_SCHEDULER_COLUMNS,
    }
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE st_scheduled_tasks (" + ",".join(
            f"`{name}` {definition}" for name, definition in columns.items()
        ) + ")"))
    return engine


def _operation_rows(engine):
    return {row["task_type"]: dict(row) for row in installer._require_unique_tasks(engine)}


@pytest.mark.parametrize("disabled", [False, True])
def test_real_operations_install_creates_missing_tasks_from_defaults(disabled):
    engine = _operations_engine()
    result = installer.install(engine, disabled=disabled)
    rows = _operation_rows(engine)
    assert all(item["action"] == "inserted" for item in result["results"].values())
    for frozen in TASKS:
        expected = {**frozen, "enabled": 0 if disabled else frozen["enabled"]}
        assert {key: rows[frozen["task_type"]][key] for key in expected} == expected


def test_real_full_cutover_preserves_operator_configuration_and_progress(tmp_path):
    engine = _operations_engine()
    installer.install(engine)
    upsert_scheduler_task(
        engine, announcement.TASK, lookup_where="task_type=:task_type",
        lookup_params={"task_type": announcement.TASK["task_type"]},
    )
    with engine.begin() as connection:
        for index, task in enumerate(TASKS):
            connection.execute(text("""
                UPDATE st_scheduled_tasks
                SET task_name=:name, group_name='operator-group',
                    script_args=:args, cron_time=:cron, interval_minutes=:interval,
                    date_param='--date {date}', enabled=:enabled,
                    description='operator description', sort_order=:sort,
                    notification_policy='existing-channel; push=false',
                    last_run_status='success', last_run_output='durable receipt',
                    last_run_at='2026-11-02 21:00:00',
                    last_triggered_at='2026-11-02 21:00:00', last_run_duration=42
                WHERE task_type=:kind
            """), {"name": f"operator name {index}",
                    "args": task["script_args"] + " --operator-option retained",
                    "cron": f"04:{10 + index}", "interval": 17 + index,
                    "enabled": index % 2, "sort": 100 + index,
                    "kind": task["task_type"]})
    before = _operation_rows(engine)
    snapshot = tmp_path / "qmt-before.json"
    announcement._write_snapshot(
        snapshot, announcement._require_unique_task(engine),
        announcement._require_unique_operation_tasks(engine),
    )

    installer.install(engine, disabled=True)
    staged = _operation_rows(engine)
    for kind, original in before.items():
        assert staged[kind] == {**original, "enabled": 0}
    announcement._restore_enabled_policy(engine, snapshot)
    assert _operation_rows(engine) == before

    # An ordinary installer invocation is not authority to enable a paused job.
    installer.install(engine)
    assert _operation_rows(engine) == before


def test_real_operations_install_rejects_duplicate_executor_identity_before_updates():
    engine = _operations_engine()
    installer.install(engine)
    task = TASKS[0]
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO st_scheduled_tasks
                (task_name,task_type,script_path,cron_time,enabled)
            VALUES ('conflicting identity','other_task',:path,'23:59',1)
        """), {"path": task["script_path"]})
        before = [dict(row) for row in connection.execute(text(
            "SELECT * FROM st_scheduled_tasks ORDER BY id"
        )).mappings()]

    with pytest.raises(RuntimeError, match="identity is not unique"):
        installer.install(engine, disabled=True)
    with engine.connect() as connection:
        assert [dict(row) for row in connection.execute(text(
            "SELECT * FROM st_scheduled_tasks ORDER BY id"
        )).mappings()] == before
