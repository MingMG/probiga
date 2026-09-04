from __future__ import annotations

import inspect
from unittest.mock import MagicMock, patch

import pytest

from server.api import scheduler_runtime
from server.common import scheduler_task_history_schema as history_schema


def _index_shapes():
    return {
        (True, ("id",)),
        (True, ("run_uid",)),
        (False, ("task_id", "run_at")),
    }


def test_runtime_history_validation_contains_no_ddl() -> None:
    source = inspect.getsource(scheduler_runtime._ensure_task_history_table)
    assert "CREATE TABLE" not in source.upper()
    assert "ALTER TABLE" not in source.upper()
    assert "engine.begin" not in source
    assert "LIMIT 0" in source


def test_history_schema_validation_is_read_only() -> None:
    engine = MagicMock()
    with patch.object(
        history_schema,
        "_columns",
        return_value=set(history_schema.REQUIRED_COLUMNS),
    ), patch.object(
        history_schema,
        "_index_inventory",
        return_value=(_index_shapes(), {"PRIMARY", "uid", "task_run"}),
    ):
        result = history_schema.validate_scheduler_task_history_schema(engine)

    assert result["physical_contract_verified"] is True
    assert result["runtime_ddl_required"] is False
    assert result["read_only"] is True
    engine.begin.assert_not_called()


def test_history_schema_validation_rejects_missing_unique_audit_identity() -> None:
    engine = MagicMock()
    shapes = _index_shapes() - {(True, ("run_uid",))}
    with patch.object(
        history_schema,
        "_columns",
        return_value=set(history_schema.REQUIRED_COLUMNS),
    ), patch.object(
        history_schema,
        "_index_inventory",
        return_value=(shapes, {"PRIMARY", "task_run"}),
    ), pytest.raises(RuntimeError, match="physical contract differs"):
        history_schema.validate_scheduler_task_history_schema(engine)


def test_privileged_history_migration_is_idempotent_without_alter() -> None:
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    connection.execute.return_value.first.return_value = None
    validated = {
        "table": history_schema.TABLE_NAME,
        "physical_contract_verified": True,
        "runtime_ddl_required": False,
        "read_only": True,
    }
    with patch.object(
        history_schema,
        "_columns",
        return_value=set(history_schema.REQUIRED_COLUMNS),
    ), patch.object(
        history_schema,
        "_audit_column_contract",
        return_value={
            "run_uid": {"is_nullable": "NO", "default": None},
            "trigger_source": {
                "is_nullable": "NO",
                "default": "scheduled",
            },
        },
    ), patch.object(
        history_schema,
        "_index_inventory",
        return_value=(_index_shapes(), {"PRIMARY", "uid", "task_run"}),
    ), patch.object(
        history_schema,
        "validate_scheduler_task_history_schema",
        return_value=validated,
    ):
        result = history_schema.migrate_scheduler_task_history(engine)

    statements = [str(call.args[0]).upper() for call in connection.execute.call_args_list]
    assert any("CREATE TABLE IF NOT EXISTS" in sql for sql in statements)
    assert not any("ALTER TABLE" in sql for sql in statements)
    assert not any(sql.lstrip().startswith("UPDATE ") for sql in statements)
    assert result["added_columns"] == []
    assert result["added_indexes"] == []


def test_privileged_history_migration_upgrades_legacy_shape() -> None:
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    connection.execute.return_value.first.return_value = None
    core_columns = {"id", "task_id", "run_at", "status", "duration"}
    with patch.object(
        history_schema,
        "_columns",
        return_value=core_columns,
    ), patch.object(
        history_schema,
        "_audit_column_contract",
        return_value={
            "run_uid": {"is_nullable": "YES", "default": None},
            "trigger_source": {"is_nullable": "YES", "default": "scheduled"},
        },
    ), patch.object(
        history_schema,
        "_index_inventory",
        return_value=({(True, ("id",))}, {"PRIMARY"}),
    ), patch.object(
        history_schema,
        "validate_scheduler_task_history_schema",
        return_value={
            "table": history_schema.TABLE_NAME,
            "physical_contract_verified": True,
            "runtime_ddl_required": False,
            "read_only": True,
        },
    ):
        result = history_schema.migrate_scheduler_task_history(engine)

    statements = [str(call.args[0]).upper() for call in connection.execute.call_args_list]
    assert any("ADD COLUMN `RUN_UID`" in sql for sql in statements)
    assert any("SET RUN_UID=CONCAT" in sql for sql in statements)
    assert any("MODIFY COLUMN RUN_UID" in sql for sql in statements)
    assert any("ADD UNIQUE INDEX" in sql for sql in statements)
    assert any("ADD INDEX" in sql and "`TASK_ID`, `RUN_AT`" in sql for sql in statements)
    assert "run_uid" in result["added_columns"]


def test_runtime_account_cannot_invoke_privileged_history_migration() -> None:
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    connection.execute.side_effect = PermissionError("CREATE denied")
    with pytest.raises(PermissionError, match="CREATE denied"):
        history_schema.migrate_scheduler_task_history(engine)


def test_activation_grant_insert_trigger_requires_exact_migrator_session() -> None:
    statements = history_schema.scheduler_task_history_trigger_ddl_statements()

    assert len(statements) == 3
    activation = next(
        statement
        for statement in statements
        if "BEFORE INSERT" in statement.upper()
    )
    normalized = " ".join(activation.split())
    assert (
        "NEW.task_type = 'qmt_edge_release_request'" in normalized
    )
    assert "NEW.trigger_source = 'release_activation'" in normalized
    assert (
        "BINARY USER() <> BINARY "
        "'probiga_migrator@127.0.0.1'" in normalized
    )
    # CURRENT_USER() inside a definer trigger identifies the trigger definer,
    # not the client session attempting the INSERT.
    assert "CURRENT_USER()" not in activation.upper()
    assert "SIGNAL SQLSTATE '45000'" in normalized

    # This is the exact truth table implemented by the trigger predicate:
    # runtime is rejected at INSERT, while the one fixed migrator session is
    # allowed to reach the append-only ledger.
    task_type = "qmt_edge_release_request"
    trigger_source = "release_activation"
    for session_user, should_signal in (
        ("probiga_runtime@127.0.0.1", True),
        ("probiga_migrator@127.0.0.1", False),
    ):
        signal_condition = (
            task_type == "qmt_edge_release_request"
            and trigger_source == "release_activation"
            and session_user.encode("utf-8")
            != history_schema.QMT_EDGE_RELEASE_ACTIVATION_SESSION_USER.encode(
                "utf-8"
            )
        )
        assert signal_condition is should_signal
