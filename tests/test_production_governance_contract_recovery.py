from __future__ import annotations

from copy import deepcopy
from datetime import datetime, time
import io
import json
import re
from types import SimpleNamespace
from typing import Any

import pytest

from deploy import production_governance_contract_recovery as recovery
from server.common.scheduler_tasks import TASK_PAYLOAD_COLUMNS
from tools.strategy_governance_task_contract import TASK
from tools.qmt_announcement_task_contract import TASK as QMT_ANNOUNCEMENT_TASK
from tools.qmt_operations_task_contract import TASKS as QMT_OPERATION_TASKS


class _Result:
    def __init__(
        self,
        *,
        rows: list[Any] | None = None,
        scalar: Any = None,
        rowcount: int = 0,
    ) -> None:
        self._rows = rows or []
        self._scalar = scalar
        self.rowcount = rowcount

    def mappings(self) -> _Result:
        return self

    def all(self) -> list[Any]:
        return deepcopy(self._rows)

    def fetchall(self) -> list[Any]:
        return deepcopy(self._rows)

    def scalar_one_or_none(self) -> Any:
        return self._scalar


class _Connection:
    def __init__(self, engine: _Engine) -> None:
        self.engine = engine

    def execute(self, statement, params=None) -> _Result:
        sql = " ".join(str(statement).split())
        parameters = dict(params or {})
        self.engine.executions.append((sql, deepcopy(parameters)))
        if "information_schema.TABLES" in sql:
            return _Result(scalar=self.engine.table_engine)
        if "information_schema.COLUMNS" in sql and "DATA_TYPE" not in sql:
            return _Result(rows=[(column,) for column in sorted(self.engine.columns)])
        if "information_schema.COLUMNS" in sql:
            return _Result(
                rows=[
                    (
                        column,
                        "datetime" if column == "created_at" else "varchar",
                        "YES",
                        None,
                        "",
                    )
                    for column in sorted(self.engine.columns)
                ]
            )
        if sql.startswith("SELECT * FROM st_scheduled_tasks"):
            return _Result(rows=self.engine.rows)
        if sql.startswith("UPDATE st_scheduled_tasks SET"):
            if not self.engine.rows:
                return _Result(rowcount=0)
            row = self.engine.rows[0]
            if (
                row.get("id") != parameters.get("restore_id")
                or row.get("task_type") != parameters.get("identity_task_type")
                or row.get("script_path")
                != parameters.get("identity_script_path")
            ):
                return _Result(rowcount=0)
            for key in TASK_PAYLOAD_COLUMNS:
                if key in parameters:
                    row[key] = parameters[key]
            if "`updated_at`=`updated_at`" not in sql:
                row["updated_at"] = datetime(2099, 1, 1)
            if self.engine.tamper_after_update:
                row["last_run_status"] = "tampered"
            return _Result(rowcount=self.engine.update_rowcount)
        raise AssertionError(f"unexpected SQL: {sql}")


class _Context:
    def __init__(self, engine: _Engine, *, transactional: bool) -> None:
        self.engine = engine
        self.transactional = transactional
        self.before = deepcopy(engine.rows)

    def __enter__(self) -> _Connection:
        return _Connection(self.engine)

    def __exit__(self, exc_type, _exc, _traceback) -> bool:
        if exc_type is not None and self.transactional:
            self.engine.rows = self.before
        return False


class _Engine:
    def __init__(
        self,
        rows: list[dict[str, Any]],
        *,
        table_engine: str = "InnoDB",
    ) -> None:
        self.rows = deepcopy(rows)
        self.columns = set(rows[0]) if rows else set(_live_row())
        self.table_engine = table_engine
        self.dialect = SimpleNamespace(name="mysql")
        self.executions: list[tuple[str, dict[str, Any]]] = []
        self.tamper_after_update = False
        self.update_rowcount = 1

    def connect(self) -> _Context:
        return _Context(self, transactional=False)

    def begin(self) -> _Context:
        return _Context(self, transactional=True)


def _sealed_row() -> dict[str, Any]:
    return {
        "id": 218,
        **TASK,
        "cron_time": "22:35:00",
        "created_at": "2026-08-01 00:00:00",
        "updated_at": "2026-08-02 00:00:00",
        "etl_sync_at": "2026-08-03 00:00:00",
        "last_triggered_at": "2026-08-04 00:00:00",
        "last_run_at": "2026-08-04 00:01:00",
        "last_run_status": "old-sealed-status",
        "last_run_output": "old sealed output",
        "last_run_duration": 999,
        "future_runtime_column": "old sealed future value",
    }


def _live_row() -> dict[str, Any]:
    return {
        "id": 218,
        **TASK,
        "cron_time": time(22, 35),
        "created_at": datetime(2026, 8, 1),
        "updated_at": datetime(2026, 8, 24, 9, 0),
        "etl_sync_at": datetime(2026, 8, 24, 8, 59),
        "last_triggered_at": datetime(2026, 8, 24, 8, 58),
        "last_run_at": datetime(2026, 8, 24, 8, 59),
        "last_run_status": "success",
        "last_run_output": "live output must remain",
        "last_run_duration": 123,
        "future_runtime_column": "live future value must remain",
    }


@pytest.fixture
def install_schema_stub():
    def install(engine: _Engine) -> None:
        assert engine.columns

    return install


def _updates(engine: _Engine) -> list[tuple[str, dict[str, Any]]]:
    return [item for item in engine.executions if item[0].startswith("UPDATE ")]


def test_verify_ignores_live_runtime_and_audit_drift(install_schema_stub) -> None:
    engine = _Engine([_live_row()])
    install_schema_stub(engine)

    result = recovery.reconcile_contract(engine, _sealed_row(), action="verify")

    assert result == {
        "action": "verify",
        "changed": False,
        "id": 218,
        "verified": True,
    }
    assert not _updates(engine)
    select_sql = [sql for sql, _params in engine.executions if sql.startswith("SELECT *")]
    assert select_sql and all("FOR UPDATE" not in sql for sql in select_sql)


def test_restore_changes_only_stable_projection_and_preserves_live_history(
    install_schema_stub,
) -> None:
    live = _live_row()
    live["script_args"] = "--limit 1"
    volatile_before = {
        key: deepcopy(value)
        for key, value in live.items()
        if key not in TASK_PAYLOAD_COLUMNS
    }
    engine = _Engine([live])
    install_schema_stub(engine)

    result = recovery.reconcile_contract(engine, _sealed_row(), action="restore")

    assert result["changed"] is True
    assert engine.rows[0]["script_args"] == TASK["script_args"]
    assert {
        key: engine.rows[0][key] for key in volatile_before
    } == volatile_before
    updates = _updates(engine)
    assert len(updates) == 1
    sql, params = updates[0]
    assert "`script_args`=:script_args" in sql
    assert "`updated_at`=`updated_at`" in sql
    for forbidden in (
        "last_run_at",
        "last_run_status",
        "last_run_output",
        "etl_sync_at",
        "created_at",
        "future_runtime_column",
    ):
        assert forbidden not in sql
        assert forbidden not in params


def test_restore_is_idempotent_after_an_ambiguous_success(
    install_schema_stub,
) -> None:
    live = _live_row()
    live["enabled"] = 0
    engine = _Engine([live])
    install_schema_stub(engine)

    first = recovery.reconcile_contract(engine, _sealed_row(), action="restore")
    second = recovery.reconcile_contract(engine, _sealed_row(), action="restore")

    assert first["changed"] is True
    assert second["changed"] is False
    assert len(_updates(engine)) == 1


def test_restore_rolls_back_if_a_runtime_field_changes_during_update(
    install_schema_stub,
) -> None:
    live = _live_row()
    live["description"] = "drifted"
    engine = _Engine([live])
    engine.tamper_after_update = True
    install_schema_stub(engine)

    with pytest.raises(RuntimeError, match="runtime or audit fields changed"):
        recovery.reconcile_contract(engine, _sealed_row(), action="restore")

    assert engine.rows == [live]


def test_restore_rolls_back_an_impossible_multirow_update(
    install_schema_stub,
) -> None:
    live = _live_row()
    live["sort_order"] = 1
    engine = _Engine([live])
    engine.update_rowcount = 2
    install_schema_stub(engine)

    with pytest.raises(RuntimeError, match="changed many rows"):
        recovery.reconcile_contract(engine, _sealed_row(), action="restore")

    assert engine.rows == [live]


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda rows: rows.clear(), "not unique: 0"),
        (lambda rows: rows.append(deepcopy(rows[0])), "not unique: 2"),
        (lambda rows: rows[0].update(id=999), "task id differs"),
        (
            lambda rows: rows[0].update(script_path="tools/other.py"),
            "script_path differs",
        ),
    ],
)
def test_live_identity_failures_never_write(
    install_schema_stub, mutate, message: str
) -> None:
    rows = [_live_row()]
    mutate(rows)
    engine = _Engine(rows)
    install_schema_stub(engine)

    with pytest.raises(RuntimeError, match=message):
        recovery.reconcile_contract(engine, _sealed_row(), action="restore")

    assert not _updates(engine)


def test_restore_requires_mysql_innodb_and_complete_schema(
    install_schema_stub,
) -> None:
    engine = _Engine([_live_row()])
    engine.dialect.name = "sqlite"
    install_schema_stub(engine)
    with pytest.raises(RuntimeError, match="requires MySQL"):
        recovery.reconcile_contract(engine, _sealed_row(), action="restore")

    engine = _Engine([_live_row()], table_engine="MyISAM")
    install_schema_stub(engine)
    with pytest.raises(RuntimeError, match="must use InnoDB"):
        recovery.reconcile_contract(engine, _sealed_row(), action="restore")

    engine = _Engine([_live_row()])
    engine.columns.remove("description")
    with pytest.raises(RuntimeError, match="misses contract columns: description"):
        recovery.reconcile_contract(engine, _sealed_row(), action="restore")
    assert not _updates(engine)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda row: row.pop("sort_order"),
        lambda row: row.update(enabled=True),
        lambda row: row.update(cron_time="22:35:01"),
        lambda row: row.update(task_name="tampered task"),
    ],
)
def test_invalid_sealed_contract_is_rejected_before_writes(
    install_schema_stub, mutate
) -> None:
    engine = _Engine([_live_row()])
    install_schema_stub(engine)
    sealed = _sealed_row()
    mutate(sealed)

    with pytest.raises(RuntimeError):
        recovery.reconcile_contract(engine, sealed, action="restore")

    assert not _updates(engine)


@pytest.mark.parametrize("row_count", [0, 2])
def test_snapshot_reader_requires_exactly_one_row(row_count: int) -> None:
    payload = {
        "format_version": 1,
        "task_type": TASK["task_type"],
        "script_path": TASK["script_path"],
        "rows": [_sealed_row() for _ in range(row_count)],
    }
    import json

    with pytest.raises(RuntimeError, match="invalid sealed"):
        recovery._read_snapshot(io.StringIO(json.dumps(payload)))


@pytest.mark.parametrize(
    ("message", "expected_code"),
    [
        ("invalid sealed governance contract snapshot", "snapshot-envelope"),
        ("sealed governance task identity differs", "sealed-identity"),
        ("governance enabled must be int", "contract-shape"),
        ("st_scheduled_tasks must use InnoDB", "engine-schema"),
        ("live governance scheduler identity is not unique: 2", "live-count"),
        ("live governance scheduler task id differs", "live-id"),
        ("live governance scheduler task_type differs", "live-identity"),
        ("live governance scheduler contract differs from sealed NEW", "projection"),
        ("governance contract restore changed many rows", "update-rowcount"),
        (
            "governance runtime or audit fields changed during restore",
            "volatile-drift",
        ),
        ("database connection failed with private details", "database-runtime"),
    ],
)
def test_failure_diagnostics_are_bounded_static_codes(
    message: str, expected_code: str
) -> None:
    assert recovery._static_failure_code(RuntimeError(message)) == expected_code


def test_cli_failure_emits_only_the_static_code(monkeypatch, capsys) -> None:
    monkeypatch.setattr(recovery.sys, "stdin", io.StringIO("{}"))

    assert recovery.main(["verify"]) == 2

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == (
        "probiga_governance_contract_failure=snapshot-envelope\n"
    )


class _RollbackConnection:
    def __init__(self, engine: _RollbackEngine) -> None:
        self.engine = engine

    def execute(self, statement, params=None) -> _Result:
        sql = " ".join(str(statement).split())
        parameters = dict(params or {})
        self.engine.executions.append((sql, deepcopy(parameters)))
        if "information_schema.TABLES" in sql:
            return _Result(scalar=self.engine.table_engine)
        if "information_schema.COLUMNS" in sql:
            rows = []
            for column in sorted(self.engine.columns):
                if column == "created_at":
                    data_type, nullable, default, extra = (
                        self.engine.created_at_contract
                    )
                else:
                    data_type, nullable, default, extra = (
                        "varchar",
                        "YES",
                        None,
                        "",
                    )
                rows.append((column, data_type, nullable, default, extra))
            return _Result(rows=rows)
        if "information_schema.STATISTICS" in sql:
            return _Result(rows=[(column,) for column in self.engine.primary_key])
        if sql.startswith("SELECT ") and " FROM st_scheduled_tasks " in sql:
            selected = sql.split(" FROM st_scheduled_tasks ", 1)[0]
            columns = re.findall(r"`([^`]+)`", selected)
            matches = [
                row
                for row in self.engine.rows
                if (
                    row.get("id") == parameters.get("sealed_id")
                    or row.get("task_type") == parameters.get("task_type")
                    or row.get("task_type")
                    == parameters.get("task_type_alias")
                    or row.get("script_path") == parameters.get("script_path")
                )
            ]
            return _Result(
                rows=[{key: row.get(key) for key in columns} for row in matches]
            )
        if sql.startswith("UPDATE st_scheduled_tasks SET "):
            matches = [
                row
                for row in self.engine.rows
                if row.get("id") == parameters.get("restore_id")
                and row.get("task_type") == parameters.get("task_type")
                and row.get("script_path") == parameters.get("script_path")
            ]
            if len(matches) != 1:
                return _Result(rowcount=0)
            row = matches[0]
            bound_columns = re.findall(
                r"`([^`]+)`=:([A-Za-z][A-Za-z0-9_]*)", sql
            )
            for key, parameter_name in bound_columns:
                row[key] = parameters[parameter_name]
            if "updated_at" in self.engine.columns and "updated_at" not in (
                [column for column, _parameter in bound_columns]
            ) and "`updated_at`=`updated_at`" not in sql:
                row["updated_at"] = "2099-01-01 00:00:00"
            if self.engine.tamper_after_update:
                row["last_run_status"] = "tampered-after-update"
            return _Result(rowcount=self.engine.update_rowcount)
        if sql.startswith("DELETE FROM st_scheduled_tasks "):
            before = len(self.engine.rows)
            self.engine.rows = [
                row
                for row in self.engine.rows
                if not (
                    row.get("id") == parameters.get("restore_id")
                    and row.get("task_type") == parameters.get("task_type")
                    and row.get("script_path") == parameters.get("script_path")
                )
            ]
            return _Result(rowcount=before - len(self.engine.rows))
        if sql.startswith("INSERT INTO st_scheduled_tasks "):
            self.engine.rows.append(deepcopy(parameters))
            return _Result(rowcount=1)
        raise AssertionError(f"unexpected SQL: {sql}")


class _RollbackContext:
    def __init__(self, engine: _RollbackEngine, *, transactional: bool) -> None:
        self.engine = engine
        self.transactional = transactional
        self.before = deepcopy(engine.rows)

    def __enter__(self) -> _RollbackConnection:
        return _RollbackConnection(self.engine)

    def __exit__(self, exc_type, _exc, _traceback) -> bool:
        if exc_type is not None and self.transactional:
            self.engine.rows = self.before
        return False


class _RollbackEngine:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = deepcopy(rows)
        self.columns = set().union(*(set(row) for row in rows))
        self.columns.update({"id", "task_type", "script_path"})
        self.table_engine = "InnoDB"
        self.primary_key = ["id"]
        self.created_at_contract = ("datetime", "YES", None, "")
        self.dialect = SimpleNamespace(name="mysql")
        self.executions: list[tuple[str, dict[str, Any]]] = []
        self.tamper_after_update = False
        self.update_rowcount = 1

    def connect(self) -> _RollbackContext:
        return _RollbackContext(self, transactional=False)

    def begin(self) -> _RollbackContext:
        return _RollbackContext(self, transactional=True)


_OLD_COLUMNS = tuple(
    sorted(recovery._SCHEDULER_SNAPSHOT_COLUMNS - {"created_at"})
)


def _old_row(task: dict[str, Any], row_id: int) -> dict[str, Any]:
    row = {
        **task,
        "id": row_id,
        "cron_time": f"{task['cron_time']}:00",
        "date_param_desc": str(task.get("date_param_desc") or ""),
        "last_triggered_at": "2026-08-27 17:59:00",
        "last_run_output": "old-output",
        "last_run_duration": 83,
        "last_run_status": "old-success",
        "last_run_at": "2026-08-27 18:00:00",
        "etl_sync_at": "2026-08-27 18:01:00",
        "updated_at": "2026-08-27 18:02:00",
    }
    return {key: row.get(key) for key in _OLD_COLUMNS}


def _live_expanded_row(task: dict[str, Any], row_id: int) -> dict[str, Any]:
    return {
        **_old_row(task, row_id),
        "created_at": None,
    }


def _governance_rollback_payload(row: dict[str, Any]) -> dict[str, Any]:
    envelope = {
        "format_version": 1,
        "task_type": TASK["task_type"],
        "script_path": TASK["script_path"],
        "rows": [row],
    }
    return recovery._read_rollback_snapshot(
        io.StringIO(json.dumps(envelope)),
        snapshot_kind="rollback-governance",
    )


def _qmt_rollback_payload(
    operation_rows: list[dict[str, Any]],
    *,
    announcement_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    envelope = {
        "schema": "probiga.qmt-announcement-task-snapshot.v1",
        "task_type": QMT_ANNOUNCEMENT_TASK["task_type"],
        "script_path": QMT_ANNOUNCEMENT_TASK["script_path"],
        "rows": announcement_rows or [],
        "operations": {
            "task_types": sorted(task["task_type"] for task in QMT_OPERATION_TASKS),
            "script_paths": sorted(
                task["script_path"] for task in QMT_OPERATION_TASKS
            ),
            "rows": operation_rows,
        },
    }
    return recovery._read_rollback_snapshot(
        io.StringIO(json.dumps(envelope)), snapshot_kind="rollback-qmt"
    )


def test_old_governance_projection_verifies_after_additive_schema_expansion() -> None:
    sealed = _old_row(TASK, 218)
    live = _live_expanded_row(TASK, 218)
    engine = _RollbackEngine([live])

    result = recovery.reconcile_rollback_snapshot(
        engine, _governance_rollback_payload(sealed), action="verify"
    )

    assert result["verified"] is True
    selects = [
        sql
        for sql, _params in engine.executions
        if sql.startswith("SELECT ") and "FROM st_scheduled_tasks" in sql
    ]
    assert selects and all("SELECT *" not in sql for sql in selects)
    assert all("created_at" in sql for sql in selects)


def test_old_governance_restore_changes_only_snapshot_projection() -> None:
    sealed = _old_row(TASK, 218)
    live = _live_expanded_row(TASK, 218)
    live["script_args"] = "--tampered"
    live["last_run_status"] = "tampered"
    additive_before = {"created_at": live["created_at"]}
    engine = _RollbackEngine([live])
    payload = _governance_rollback_payload(sealed)

    first = recovery.reconcile_rollback_snapshot(engine, payload, action="restore")
    second = recovery.reconcile_rollback_snapshot(engine, payload, action="restore")

    assert first["changed_row_count"] == 1
    assert second["changed"] is False
    assert {key: engine.rows[0][key] for key in sealed} == sealed
    assert {key: engine.rows[0][key] for key in additive_before} == additive_before
    updates = [sql for sql, _params in engine.executions if sql.startswith("UPDATE ")]
    assert len(updates) == 1
    assert "`updated_at`=:preserve_updated_at" in updates[0]
    assert "created_at" not in updates[0]


def test_old_qmt_snapshot_restores_five_operations_and_absent_announcement() -> None:
    sealed_operations = [
        _old_row(task, 300 + index)
        for index, task in enumerate(QMT_OPERATION_TASKS)
    ]
    live_operations = [
        _live_expanded_row(task, 300 + index)
        for index, task in enumerate(QMT_OPERATION_TASKS)
    ]
    for row in live_operations:
        row["enabled"] = 0
    live_announcement = _live_expanded_row(QMT_ANNOUNCEMENT_TASK, 399)
    # A row created after the OLD zero-row snapshot legitimately receives the
    # additive column's live default.  Recovery must delete it, not require a
    # value the earlier snapshot could not have represented.
    live_announcement["created_at"] = "2026-08-28 00:00:00"
    engine = _RollbackEngine([live_announcement, *live_operations])
    payload = _qmt_rollback_payload(sealed_operations)

    result = recovery.reconcile_rollback_snapshot(engine, payload, action="restore")
    recovery.reconcile_rollback_snapshot(engine, payload, action="verify")

    assert result["changed_row_count"] == 6
    assert all(
        row["task_type"] != QMT_ANNOUNCEMENT_TASK["task_type"]
        for row in engine.rows
    )
    assert len(engine.rows) == 5
    assert all(row["enabled"] == 1 for row in engine.rows)
    assert all(row["last_run_output"] == "old-output" for row in engine.rows)
    assert all(row["created_at"] is None for row in engine.rows)


def _production_legacy_qmt_operation_rows() -> list[dict[str, Any]]:
    production_ids = {
        "qmt_nightly_reconciliation": 53,
        "qmt_gap_repair_plan": 54,
        "qmt_local_history_2024": 55,
        "qmt_reference_incremental": 56,
        "qmt_local_gap_repair_execute": 60,
    }
    rows = [
        _old_row(task, production_ids[str(task["task_type"])])
        for task in QMT_OPERATION_TASKS
    ]
    history = next(row for row in rows if row["id"] == 55)
    history["task_type"] = "qmt_local_history_2026"
    return rows


def test_production_legacy_qmt_history_identity_verifies_exact_old_snapshot() -> None:
    sealed_operations = _production_legacy_qmt_operation_rows()
    live_operations = [
        {**row, "created_at": None} for row in sealed_operations
    ]
    engine = _RollbackEngine(live_operations)

    result = recovery.reconcile_rollback_snapshot(
        engine,
        _qmt_rollback_payload(sealed_operations),
        action="verify",
    )

    assert result["verified"] is True
    assert next(row for row in engine.rows if row["id"] == 55)[
        "task_type"
    ] == "qmt_local_history_2026"


def test_production_legacy_qmt_history_identity_restores_current_alias() -> None:
    sealed_operations = _production_legacy_qmt_operation_rows()
    live_operations = [
        {**row, "created_at": None} for row in sealed_operations
    ]
    live_history = next(row for row in live_operations if row["id"] == 55)
    live_history["task_type"] = "qmt_local_history_2024"
    engine = _RollbackEngine(live_operations)

    result = recovery.reconcile_rollback_snapshot(
        engine,
        _qmt_rollback_payload(sealed_operations),
        action="restore",
    )

    assert result["changed_row_count"] == 1
    restored = next(row for row in engine.rows if row["id"] == 55)
    assert restored["task_type"] == "qmt_local_history_2026"
    recovery.reconcile_rollback_snapshot(
        engine,
        _qmt_rollback_payload(sealed_operations),
        action="verify",
    )


def test_production_legacy_qmt_history_alias_collision_fails_before_writes() -> None:
    sealed_operations = _production_legacy_qmt_operation_rows()
    live_operations = [
        {**row, "created_at": None} for row in sealed_operations
    ]
    current_alias = deepcopy(next(row for row in live_operations if row["id"] == 55))
    current_alias["id"] = 555
    current_alias["task_type"] = "qmt_local_history_2024"
    engine = _RollbackEngine([*live_operations, current_alias])

    with pytest.raises(RuntimeError, match="identity is not unique: 2"):
        recovery.reconcile_rollback_snapshot(
            engine,
            _qmt_rollback_payload(sealed_operations),
            action="restore",
        )

    assert not any(
        sql.startswith(("UPDATE ", "DELETE ", "INSERT "))
        for sql, _params in engine.executions
    )


def test_absent_old_qmt_history_deletes_legacy_alias_with_additive_value() -> None:
    all_operations = _production_legacy_qmt_operation_rows()
    sealed_operations = [row for row in all_operations if row["id"] != 55]
    live_operations = [{**row, "created_at": None} for row in all_operations]
    live_history = next(row for row in live_operations if row["id"] == 55)
    live_history["created_at"] = "2026-08-28 00:00:00"
    engine = _RollbackEngine(live_operations)

    result = recovery.reconcile_rollback_snapshot(
        engine,
        _qmt_rollback_payload(sealed_operations),
        action="restore",
    )

    assert result["changed_row_count"] == 1
    assert all(row["id"] != 55 for row in engine.rows)


def test_qmt_history_snapshot_rejects_unknown_and_dual_aliases() -> None:
    legacy = next(
        row
        for row in _production_legacy_qmt_operation_rows()
        if row["id"] == 55
    )
    unknown = deepcopy(legacy)
    unknown["task_type"] = "qmt_local_history_2025"
    with pytest.raises(RuntimeError, match="identity differs"):
        _qmt_rollback_payload([unknown])

    current = deepcopy(legacy)
    current["id"] = 555
    current["task_type"] = "qmt_local_history_2024"
    with pytest.raises(RuntimeError, match="invalid sealed"):
        _qmt_rollback_payload([legacy, current])


@pytest.mark.parametrize(
    "mutation",
    [
        lambda envelope: envelope.update(task_type="forged-governance"),
        lambda envelope: envelope["rows"][0].update(script_path="tools/evil.py"),
        lambda envelope: envelope["rows"][0].update(evil_column="DROP TABLE"),
        lambda envelope: envelope["rows"][0].pop("id"),
    ],
)
def test_old_snapshot_envelope_and_projection_tampering_is_rejected(mutation) -> None:
    envelope = {
        "format_version": 1,
        "task_type": TASK["task_type"],
        "script_path": TASK["script_path"],
        "rows": [_old_row(TASK, 218)],
    }
    mutation(envelope)

    with pytest.raises(RuntimeError):
        recovery._read_rollback_snapshot(
            io.StringIO(json.dumps(envelope)),
            snapshot_kind="rollback-governance",
        )


def test_old_projection_tamper_after_update_rolls_back_transaction() -> None:
    sealed = _old_row(TASK, 218)
    live = _live_expanded_row(TASK, 218)
    live["script_args"] = "--tampered"
    engine = _RollbackEngine([live])
    engine.tamper_after_update = True

    with pytest.raises(RuntimeError, match="projection differs"):
        recovery.reconcile_rollback_snapshot(
            engine, _governance_rollback_payload(sealed), action="restore"
        )

    assert engine.rows == [live]


def test_old_qmt_snapshot_rejects_cross_group_duplicate_ids() -> None:
    operation_rows = [
        _old_row(task, 300 + index)
        for index, task in enumerate(QMT_OPERATION_TASKS)
    ]
    announcement = _old_row(QMT_ANNOUNCEMENT_TASK, operation_rows[0]["id"])

    with pytest.raises(RuntimeError, match="invalid sealed"):
        _qmt_rollback_payload(
            operation_rows, announcement_rows=[announcement]
        )


def test_old_projection_rejects_live_alias_collision_before_writes() -> None:
    sealed = _old_row(TASK, 218)
    exact = _live_expanded_row(TASK, 218)
    collision = _live_expanded_row(TASK, 219)
    collision["task_type"] = "other-task"
    engine = _RollbackEngine([exact, collision])

    with pytest.raises(RuntimeError, match="not unique: 2"):
        recovery.reconcile_rollback_snapshot(
            engine, _governance_rollback_payload(sealed), action="restore"
        )

    assert not any(sql.startswith("UPDATE ") for sql, _params in engine.executions)


def test_old_projection_requires_innodb_primary_id_and_snapshot_columns() -> None:
    sealed = _old_row(TASK, 218)
    payload = _governance_rollback_payload(sealed)
    live = _live_expanded_row(TASK, 218)

    engine = _RollbackEngine([live])
    engine.table_engine = "MyISAM"
    with pytest.raises(RuntimeError, match="must use InnoDB"):
        recovery.reconcile_rollback_snapshot(engine, payload, action="verify")

    engine = _RollbackEngine([live])
    engine.primary_key = ["task_type"]
    with pytest.raises(RuntimeError, match="primary key differs"):
        recovery.reconcile_rollback_snapshot(engine, payload, action="verify")

    engine = _RollbackEngine([live])
    engine.columns.remove("description")
    with pytest.raises(RuntimeError, match="column surface differs"):
        recovery.reconcile_rollback_snapshot(engine, payload, action="verify")

    engine = _RollbackEngine([live])
    engine.created_at_contract = ("timestamp", "NO", "CURRENT_TIMESTAMP", "")
    with pytest.raises(RuntimeError, match="additive column contract differs"):
        recovery.reconcile_rollback_snapshot(engine, payload, action="verify")

    live["created_at"] = "2026-08-28 00:00:00"
    engine = _RollbackEngine([live])
    with pytest.raises(RuntimeError, match="additive column value differs"):
        recovery.reconcile_rollback_snapshot(engine, payload, action="verify")
