from __future__ import annotations

from tools import add_qmt_operations_tasks as installer
from tools.qmt_operations_task_contract import (
    QMT_FULL_HISTORY_LOCK_PATH,
    QMT_FULL_HISTORY_LOG_PATH,
    QMT_FULL_HISTORY_STATE_ROOT,
    QMT_GAP_REPAIR_LOCK_PATH,
    QMT_GAP_REPAIR_STATE_ROOT,
    TASKS,
)


def test_frozen_qmt_operations_inventory_has_five_unique_enabled_tasks():
    assert len(TASKS) == 5
    assert len({task["task_type"] for task in TASKS}) == 5
    assert len({task["script_path"] for task in TASKS}) == 5
    assert all(task["enabled"] == 1 for task in TASKS)
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
    assert "--start-date 2024-01-01" in full_history["script_args"]
    assert "2026-01-01" not in full_history["script_args"]
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
    assert all(call[1]["enabled"] == 1 for call in calls[5:])
    assert all(
        call[2]["lookup_where"]
        == "task_type=:task_type OR script_path=:script_path"
        for call in calls
    )
