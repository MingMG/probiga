from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from tools import add_qmt_announcement_task as announcement
from tools import check_strategy_governance_health as health
from tools import ensure_quality_gate as quality
from tools.qmt_host_ownership_contract import WINDOWS_QMT_EDGE_TASK_TYPES


@pytest.fixture
def engine():
    value = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with value.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_scheduled_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_name TEXT, task_type TEXT, group_name TEXT,
                script_path TEXT, script_args TEXT, cron_time TEXT,
                interval_minutes INTEGER, enabled INTEGER,
                description TEXT, sort_order INTEGER, date_param TEXT,
                last_run_status TEXT, last_run_output TEXT
            )
        """))
    yield value
    value.dispose()


def _insert(engine, task, *, enabled=None):
    payload = {key: task[key] for key in quality.TASK_PAYLOAD_COLUMNS}
    if enabled is not None:
        payload["enabled"] = enabled
    with engine.begin() as connection:
        connection.execute(text(
            "INSERT INTO st_scheduled_tasks (" + ",".join(payload) + ") VALUES ("
            + ",".join(":" + key for key in payload) + ")"
        ), payload)


def _rows(engine):
    with engine.connect() as connection:
        return [dict(row) for row in connection.execute(text(
            "SELECT * FROM st_scheduled_tasks ORDER BY id"
        )).mappings()]


def _snapshot(engine, tmp_path, *, enabled=0):
    for task in (announcement.TASK, *announcement.QMT_OPERATIONS_TASKS):
        _insert(engine, task, enabled=enabled)
    rows = _rows(engine)
    path = tmp_path / "old.json"
    announcement._write_snapshot(
        path, [row for row in rows if row["task_type"] == announcement.TASK["task_type"]],
        [row for row in rows if row["task_type"] != announcement.TASK["task_type"]],
    )
    return path


@pytest.mark.parametrize("old_enabled", [0, 1])
def test_enabled_restore_preserves_new_configuration_and_terminal_observation(
    engine, tmp_path, old_enabled,
):
    old = _snapshot(engine, tmp_path, enabled=old_enabled)
    with engine.begin() as connection:
        connection.execute(text("""
            UPDATE st_scheduled_tasks SET enabled=0, script_args='new-release',
                cron_time='12:34', last_run_status='failed',
                last_run_output='actual new terminal result'
        """))
    before = _rows(engine)
    result = announcement._restore_enabled_policy(engine, old)
    after = _rows(engine)
    assert result["restored_row_count"] == 6
    assert all(row["enabled"] == old_enabled for row in after)
    assert [{key: value for key, value in row.items() if key != "enabled"}
            for row in after] == [
        {key: value for key, value in row.items() if key != "enabled"}
        for row in before
    ]
    assert announcement._restore_enabled_policy(engine, old) == result


@pytest.mark.parametrize("mutation", ["missing", "id", "type", "script", "duplicate", "unexpected_enable"])
def test_enabled_restore_rejects_current_identity_drift_atomically(engine, tmp_path, mutation):
    old = _snapshot(engine, tmp_path)
    with engine.begin() as connection:
        if mutation == "missing":
            connection.execute(text("DELETE FROM st_scheduled_tasks WHERE id=6"))
        elif mutation == "id":
            connection.execute(text("UPDATE st_scheduled_tasks SET id=66 WHERE id=6"))
        elif mutation == "type":
            connection.execute(text("UPDATE st_scheduled_tasks SET task_type='foreign' WHERE id=6"))
        elif mutation == "script":
            connection.execute(text("UPDATE st_scheduled_tasks SET script_path='foreign.py' WHERE id=6"))
        elif mutation == "duplicate":
            connection.execute(text("""
                INSERT INTO st_scheduled_tasks(task_type,script_path,enabled)
                SELECT task_type,script_path,enabled FROM st_scheduled_tasks WHERE id=6
            """))
        else:
            connection.execute(text("UPDATE st_scheduled_tasks SET enabled=1 WHERE id=6"))
    before = _rows(engine)
    with pytest.raises(RuntimeError, match="policy"):
        announcement._restore_enabled_policy(engine, old)
    assert _rows(engine) == before


@pytest.mark.parametrize("field,value", [("enabled", True), ("enabled", 1.0), ("enabled", "0"),
                                          ("enabled", 2), ("id", True), ("id", 0),
                                          ("script_path", "wrong.py"), ("task_type", "foreign")])
def test_enabled_restore_rejects_untyped_or_foreign_snapshot(engine, tmp_path, field, value):
    old = _snapshot(engine, tmp_path)
    payload = json.loads(old.read_bytes())
    payload["rows"][0][field] = value
    old.write_text(json.dumps(payload), encoding="utf-8")
    before = _rows(engine)
    with pytest.raises(RuntimeError, match="policy"):
        announcement._restore_enabled_policy(engine, old)
    assert _rows(engine) == before


def test_enabled_restore_rejects_rows_in_wrong_snapshot_partition(engine, tmp_path):
    old = _snapshot(engine, tmp_path)
    payload = json.loads(old.read_bytes())
    payload["rows"], payload["operations"]["rows"][0] = (
        [payload["operations"]["rows"][0]], payload["rows"][0]
    )
    old.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(RuntimeError, match="misclassified"):
        announcement._restore_enabled_policy(engine, old)


def test_new_tasks_without_prior_policy_stay_fenced(engine, tmp_path):
    old = tmp_path / "old.json"
    announcement._write_snapshot(old, [], [])
    for task in (announcement.TASK, *announcement.QMT_OPERATIONS_TASKS):
        _insert(engine, task, enabled=0)
    result = announcement._restore_enabled_policy(engine, old)
    assert result["restored_row_count"] == 0
    assert result["new_task_policy"] == "PAUSED"
    assert all(row["enabled"] == 0 for row in _rows(engine))


@pytest.mark.parametrize("enabled", [0, 1])
def test_quality_reinstall_preserves_all_existing_windows_qmt_policy(engine, enabled):
    tasks = [task for task in quality.TASKS if task["task_type"] in WINDOWS_QMT_EDGE_TASK_TYPES]
    assert len(tasks) == 17
    for task in tasks:
        _insert(engine, task, enabled=enabled)
        with engine.begin() as connection:
            connection.execute(text(
                "UPDATE st_scheduled_tasks SET script_args='old code', cron_time='00:00' "
                "WHERE task_type=:task_type"
            ), {"task_type": task["task_type"]})
        assert quality.upsert_task(engine, task) == "updated"
    observed = {row["task_type"]: row for row in _rows(engine)}
    assert all(row["enabled"] == enabled for row in observed.values())
    for task in tasks:
        assert observed[task["task_type"]]["script_args"] == task["script_args"]
        assert observed[task["task_type"]]["cron_time"] == task["cron_time"]
    assert set(quality.validate_managed_task_contracts(
        engine, task_types=set(observed)
    ).values()) == {"paused" if enabled == 0 else "validated"}


def test_new_quality_qmt_task_is_not_implicitly_enabled(engine):
    task = next(task for task in quality.TASKS if task["task_type"] == "qmt_index_minute")
    assert task["enabled"] == 1
    assert quality.upsert_task(engine, task) == "inserted"
    assert _rows(engine)[0]["enabled"] == 0
    assert quality.validate_managed_task_contracts(
        engine, task_types={task["task_type"]}
    ) == {task["task_type"]: "paused"}


@pytest.mark.parametrize("task_type", ["capital_flow_batch_fast", "analysis_morning_strict"])
def test_non_qmt_definitions_keep_their_exact_enabled_safety_policy(engine, task_type):
    task = next(task for task in quality.TASKS if task["task_type"] == task_type)
    _insert(engine, task, enabled=1 - task["enabled"])
    with pytest.raises(RuntimeError, match="enabled"):
        quality.validate_managed_task_contracts(engine, task_types={task_type})
    quality.upsert_task(engine, task)
    assert _rows(engine)[0]["enabled"] == task["enabled"]
    assert quality.validate_managed_task_contracts(engine, task_types={task_type}) == {task_type: "validated"}


@pytest.mark.parametrize("value", [None, True, False, 0.0, 1.0, "0", -1, 2])
def test_quality_user_policy_has_no_bool_float_or_string_alias(value):
    expected = next(task for task in quality.TASKS if task["task_type"] == "qmt_index_minute")
    assert quality._managed_task_drift({**expected, "enabled": value}, expected) == {"enabled"}


def _health_fixture(engine):
    for task in (announcement.TASK, *announcement.QMT_OPERATIONS_TASKS):
        _insert(engine, task, enabled=(0 if task["task_type"] in WINDOWS_QMT_EDGE_TASK_TYPES else task["enabled"]))
    definitions = {task["task_type"]: task for task in quality.TASKS}
    for task_type in ("analysis_upper_evidence_prepare", "analysis_fast"):
        _insert(engine, definitions[task_type], enabled=0 if task_type in WINDOWS_QMT_EDGE_TASK_TYPES else 1)
    from tools.strategy_governance_task_contract import TASK
    _insert(engine, TASK, enabled=1)


def _health_check(engine, function):
    checks = {}
    with engine.connect() as connection:
        passed = function(connection, {"st_scheduled_tasks"},
                          lambda name, ok, detail: checks.update({name: (ok, detail)}))
    return passed, checks


def test_paused_qmt_health_is_valid_configuration_not_data_ready(engine):
    _health_fixture(engine)
    passed, checks = _health_check(engine, health._qmt_announcement_scheduler_checks)
    assert passed
    detail = checks["qmt_announcement_scheduler_task_contract"][1]
    assert detail["dispatch_status"] == "PAUSED"
    assert detail["data_readiness"] == "NOT_ASSERTED"
    passed, checks = _health_check(engine, health._qmt_operations_scheduler_checks)
    assert passed
    detail = checks["qmt_operations_scheduler_tasks_contract"][1]
    assert detail["dispatch_policies"]["qmt_reference_incremental"] == "PAUSED"
    assert detail["data_readiness"] == "NOT_ASSERTED"


@pytest.mark.parametrize("task_type,field,value,function", [
    ("qmt_announcement_pit", "script_args", "wrong", health._qmt_announcement_scheduler_checks),
    ("qmt_announcement_pit", "enabled", 2, health._qmt_announcement_scheduler_checks),
    ("analysis_upper_evidence_prepare", "cron_time", "23:59", health._qmt_announcement_scheduler_checks),
    ("analysis_fast", "enabled", 0, health._qmt_announcement_scheduler_checks),
    ("qmt_reference_incremental", "script_path", "wrong.py", health._qmt_operations_scheduler_checks),
    ("qmt_reference_incremental", "enabled", 2, health._qmt_operations_scheduler_checks),
    ("qmt_gap_repair_plan", "enabled", 0, health._qmt_operations_scheduler_checks),
])
def test_pause_does_not_waive_task_configuration_or_non_qmt_policy(
    engine, task_type, field, value, function,
):
    _health_fixture(engine)
    with engine.begin() as connection:
        connection.execute(text(f"UPDATE st_scheduled_tasks SET {field}=:value WHERE task_type=:task_type"),
                           {"value": value, "task_type": task_type})
    assert not _health_check(engine, function)[0]


def test_release_restores_only_sealed_old_policy_before_new_snapshot():
    deploy = (Path(__file__).resolve().parents[1] / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    disabled = deploy.index("CUTOVER_STEP=install_qmt_operations_tasks_disabled")
    restore = deploy.index("CUTOVER_STEP=restore_qmt_user_enabled_policy")
    captured = deploy.index("CUTOVER_STEP=capture_qmt_announcement_task_after_policy_restore")
    assert disabled < restore < captured < deploy.index("CUTOVER_STEP=verify_strategy_governance_before_start")
    assert "prepared_qmt_announcement_snapshot restore-enabled-policy" in deploy[restore:captured]
    assert '"$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT"' in deploy[restore:captured]
    assert "CUTOVER_STEP=enable_qmt_announcement_task" not in deploy
    assert "CUTOVER_STEP=enable_qmt_operations_tasks" not in deploy
    helper = deploy[deploy.index("prepared_qmt_announcement_snapshot() {"):]
    helper = helper[:helper.index("prepared_restore_and_verify_governance_snapshot()")]
    assert '"$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SHA" 600' in helper
    assert '"$mode" - < "$snapshot"' in helper
