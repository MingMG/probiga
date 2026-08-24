from __future__ import annotations

from copy import deepcopy
from datetime import datetime, time
import io
from types import SimpleNamespace
from typing import Any

import pytest

from deploy import production_governance_contract_recovery as recovery
from server.common.scheduler_tasks import TASK_PAYLOAD_COLUMNS
from tools.strategy_governance_task_contract import TASK


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
        if "information_schema.COLUMNS" in sql:
            return _Result(rows=[(column,) for column in sorted(self.engine.columns)])
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
