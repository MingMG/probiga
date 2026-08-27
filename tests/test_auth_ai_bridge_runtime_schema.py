from __future__ import annotations

import inspect as python_inspect
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, text

from server.ai_bridge import schema as ai_bridge_schema
from server.auth import schema as auth_schema
from server.ai_bridge.schema import (
    ensure_ai_bridge_schema,
    privileged_migrate_ai_bridge_schema,
    reset_ai_bridge_schema_cache,
    validate_ai_bridge_runtime_schema,
)
from server.auth.schema import (
    ensure_auth_schema,
    privileged_migrate_auth_schema,
    reset_auth_schema_cache,
    validate_auth_runtime_schema,
)


def _sqlite_engine(tmp_path, name):
    return create_engine(f"sqlite:///{tmp_path / name}")


def test_privileged_auth_migration_then_runtime_read_only_validation(tmp_path):
    engine = _sqlite_engine(tmp_path, "auth-contract.db")
    try:
        migrated = privileged_migrate_auth_schema(engine)
        reset_auth_schema_cache(engine)
        ensure_auth_schema(engine)
        validated = validate_auth_runtime_schema(engine)
    finally:
        engine.dispose()
    assert migrated["bootstrap_singleton_verified"] is True
    assert validated["table_count"] == 4
    assert validated["runtime_ddl_required"] is False
    runtime_source = python_inspect.getsource(ensure_auth_schema)
    assert "create_all" not in runtime_source
    assert "insert(" not in runtime_source


def test_auth_runtime_fails_closed_without_privileged_migration(tmp_path):
    engine = _sqlite_engine(tmp_path, "missing-auth.db")
    try:
        with pytest.raises(RuntimeError, match="missing"):
            ensure_auth_schema(engine)
    finally:
        engine.dispose()


def test_auth_runtime_rejects_bootstrap_non_singleton(tmp_path):
    engine = _sqlite_engine(tmp_path, "auth-singleton.db")
    try:
        privileged_migrate_auth_schema(engine)
        with engine.begin() as connection:
            connection.execute(text(
                "INSERT INTO st_auth_bootstrap "
                "(id,registration_open,claimed_user_id,created_at,updated_at) "
                "SELECT 2,registration_open,claimed_user_id,created_at,updated_at "
                "FROM st_auth_bootstrap WHERE id=1"
            ))
        with pytest.raises(RuntimeError, match="singleton"):
            validate_auth_runtime_schema(engine)
    finally:
        engine.dispose()


def test_mysql_auth_bootstrap_contract_matches_created_primary_key() -> None:
    """Keep the production login schema check aligned with SQLAlchemy DDL."""

    assert auth_schema._AUTH_COLUMN_CONTRACTS["st_auth_bootstrap"]["id"] == (
        "int",
        "NO",
        "auto_increment",
    )


def test_privileged_ai_bridge_migration_then_runtime_read_only_validation(tmp_path):
    engine = _sqlite_engine(tmp_path, "ai-bridge-contract.db")
    try:
        migrated = privileged_migrate_ai_bridge_schema(engine)
        reset_ai_bridge_schema_cache(engine)
        ensure_ai_bridge_schema(engine)
        validated = validate_ai_bridge_runtime_schema(engine)
    finally:
        engine.dispose()

    assert migrated["physical_schema_verified"] is True
    assert validated["table_count"] == 1
    runtime_source = python_inspect.getsource(ensure_ai_bridge_schema)
    assert "create_all" not in runtime_source


def test_ai_bridge_runtime_fails_closed_on_incomplete_table(tmp_path):
    engine = _sqlite_engine(tmp_path, "bad-ai-bridge.db")
    try:
        with engine.begin() as connection:
            connection.execute(text(
                "CREATE TABLE st_ai_bridge_job (id INTEGER PRIMARY KEY)"
            ))
        with pytest.raises(RuntimeError, match="columns"):
            validate_ai_bridge_runtime_schema(engine)
    finally:
        engine.dispose()


def _mysql_ai_bridge_engine():
    engine = MagicMock()
    engine.dialect.name = "mysql"
    connection = engine.begin.return_value.__enter__.return_value
    return engine, connection


def _mysql_ai_bridge_columns(collation):
    rows = []
    for ordinal, (name, (column_type, nullable, extra)) in enumerate(
        ai_bridge_schema._AI_BRIDGE_COLUMN_CONTRACT.items(),
        1,
    ):
        character = column_type.split("(", 1)[0] in {"varchar", "text"}
        rows.append({
            "COLUMN_NAME": name,
            "ORDINAL_POSITION": ordinal,
            "COLUMN_TYPE": column_type,
            "IS_NULLABLE": nullable,
            "COLUMN_DEFAULT": None,
            "EXTRA": extra,
            "CHARACTER_SET_NAME": "utf8mb4" if character else None,
            "COLLATION_NAME": collation if character else None,
        })
    return rows


def _mysql_physical_connection(collation, *, column_rows=None):
    connection = MagicMock()
    table_result = MagicMock()
    table_result.mappings.return_value.one_or_none.return_value = {
        "ENGINE": "InnoDB",
        "TABLE_COLLATION": collation,
    }
    columns_result = MagicMock()
    columns_result.mappings.return_value.all.return_value = (
        column_rows or _mysql_ai_bridge_columns(collation)
    )
    connection.execute.side_effect = (table_result, columns_result)
    return connection


def _ai_bridge_pending_plan(fingerprint, *, target_contract_sha256=None):
    manifest = {
        "before_fingerprint": fingerprint,
        "fingerprint_columns": list(ai_bridge_schema._AI_BRIDGE_COLUMN_CONTRACT),
        "source_collation": ai_bridge_schema._AI_BRIDGE_LEGACY_COLLATION,
        "target_contract_sha256": (
            target_contract_sha256
            or ai_bridge_schema._ai_bridge_target_contract_sha256()
        ),
        "allowed_actions": [ai_bridge_schema._AI_BRIDGE_CONVERT_ACTION],
    }
    payload = {"table": ai_bridge_schema._AI_BRIDGE_TABLE, **manifest}
    record = ai_bridge_schema.make_evidence_record(
        recovery_version=ai_bridge_schema._AI_BRIDGE_RECOVERY_VERSION,
        source_table=ai_bridge_schema._AI_BRIDGE_TABLE,
        source_row_id=0,
        action="PHYSICAL_REWRITE_PLAN",
        business_key={"table": ai_bridge_schema._AI_BRIDGE_TABLE},
        source_row=manifest,
        plan_payload=payload,
    )
    return {
        "record": record,
        "business_key": {"table": ai_bridge_schema._AI_BRIDGE_TABLE},
        "source_row": manifest,
        "plan_payload": payload,
        "plan_sha256": record["plan_sha256"],
    }


def test_ai_bridge_legacy_contract_allows_collation_as_the_only_difference():
    connection = _mysql_physical_connection(
        ai_bridge_schema._AI_BRIDGE_LEGACY_COLLATION
    )

    actual = ai_bridge_schema._validate_mysql_physical_contract(
        connection,
        expected_collation=ai_bridge_schema._AI_BRIDGE_LEGACY_COLLATION,
    )

    assert set(actual) == set(ai_bridge_schema._AI_BRIDGE_COLUMN_CONTRACT)


def test_ai_bridge_legacy_contract_rejects_any_non_collation_difference():
    columns = _mysql_ai_bridge_columns(
        ai_bridge_schema._AI_BRIDGE_LEGACY_COLLATION
    )
    next(row for row in columns if row["COLUMN_NAME"] == "request_uid")[
        "COLUMN_TYPE"
    ] = "varchar(40)"
    connection = _mysql_physical_connection(
        ai_bridge_schema._AI_BRIDGE_LEGACY_COLLATION,
        column_rows=columns,
    )

    with pytest.raises(RuntimeError, match="request_uid"):
        ai_bridge_schema._validate_mysql_physical_contract(
            connection,
            expected_collation=ai_bridge_schema._AI_BRIDGE_LEGACY_COLLATION,
        )


def test_ai_bridge_read_only_plan_accepts_only_exact_collision_free_legacy():
    engine = MagicMock()
    engine.dialect.name = "mysql"
    connection = engine.connect.return_value.__enter__.return_value
    fingerprint = {"row_count": 4, "content_sha256": "c" * 64}
    inspector = MagicMock()
    inspector.get_table_names.return_value = [ai_bridge_schema._AI_BRIDGE_TABLE]
    with patch.object(
        ai_bridge_schema, "inspect", return_value=inspector
    ), patch.object(
        ai_bridge_schema,
        "_index_shapes",
        return_value=set(ai_bridge_schema._AI_BRIDGE_INDEX_CONTRACT),
    ), patch.object(
        ai_bridge_schema,
        "_mysql_table_collation",
        return_value=ai_bridge_schema._AI_BRIDGE_LEGACY_COLLATION,
    ), patch.object(
        ai_bridge_schema, "_validate_mysql_physical_contract"
    ), patch.object(
        ai_bridge_schema,
        "_request_uid_has_target_collation_duplicates",
        return_value=False,
    ), patch.object(
        ai_bridge_schema, "table_content_fingerprint", return_value=fingerprint
    ):
        plan = ai_bridge_schema.plan_ai_bridge_recovery(engine)

    assert plan["read_only"] is True
    assert plan["state"] == "LEGACY_GENERAL_CI"
    assert plan["ready_for_privileged_apply"] is True
    assert plan["allowed_actions"] == [
        ai_bridge_schema._AI_BRIDGE_CONVERT_ACTION
    ]
    assert plan["before_fingerprint"] == fingerprint
    engine.begin.assert_not_called()
    connection.execute.assert_not_called()


def test_ai_bridge_read_only_plan_fails_closed_on_target_collision():
    engine = MagicMock()
    engine.dialect.name = "mysql"
    inspector = MagicMock()
    inspector.get_table_names.return_value = [ai_bridge_schema._AI_BRIDGE_TABLE]
    with patch.object(
        ai_bridge_schema, "inspect", return_value=inspector
    ), patch.object(
        ai_bridge_schema,
        "_index_shapes",
        return_value=set(ai_bridge_schema._AI_BRIDGE_INDEX_CONTRACT),
    ), patch.object(
        ai_bridge_schema,
        "_mysql_table_collation",
        return_value=ai_bridge_schema._AI_BRIDGE_LEGACY_COLLATION,
    ), patch.object(
        ai_bridge_schema, "_validate_mysql_physical_contract"
    ), patch.object(
        ai_bridge_schema,
        "_request_uid_has_target_collation_duplicates",
        return_value=True,
    ), patch.object(
        ai_bridge_schema, "table_content_fingerprint"
    ) as fingerprint_rows:
        plan = ai_bridge_schema.plan_ai_bridge_recovery(engine)

    assert plan["state"] == "UNSUPPORTED"
    assert plan["ready_for_privileged_apply"] is False
    assert "request_uid collides" in plan["blocked_reason"]
    fingerprint_rows.assert_not_called()


def test_ai_bridge_exact_legacy_collation_is_fingerprinted_and_upgraded():
    engine, connection = _mysql_ai_bridge_engine()
    fingerprint = {"row_count": 4, "content_sha256": "a" * 64}
    events = []

    def execute(statement, *_args, **_kwargs):
        if "ALTER TABLE" in str(statement).upper():
            events.append("alter")
        return MagicMock()

    def persist(_connection, records):
        events.append(records[0]["action"])
        return {"evidence_verified": True, "evidence_row_count": len(records)}

    connection.execute.side_effect = execute
    with patch.object(
        ai_bridge_schema.metadata, "create_all"
    ), patch.object(
        ai_bridge_schema, "inspect", return_value=MagicMock()
    ), patch.object(
        ai_bridge_schema,
        "_index_shapes",
        return_value=set(ai_bridge_schema._AI_BRIDGE_INDEX_CONTRACT),
    ), patch.object(
        ai_bridge_schema, "ensure_evidence_table"
    ), patch.object(
        ai_bridge_schema, "load_pending_physical_rewrite_plan", return_value=None
    ), patch.object(
        ai_bridge_schema, "persist_and_verify_evidence", side_effect=persist
    ), patch.object(
        ai_bridge_schema,
        "_mysql_table_collation",
        return_value=ai_bridge_schema._AI_BRIDGE_LEGACY_COLLATION,
    ), patch.object(
        ai_bridge_schema, "_validate_mysql_physical_contract"
    ) as validate_physical, patch.object(
        ai_bridge_schema,
        "_request_uid_has_target_collation_duplicates",
        return_value=False,
    ), patch.object(
        ai_bridge_schema,
        "table_content_fingerprint",
        side_effect=(fingerprint, fingerprint),
    ) as fingerprint_rows, patch.object(
        ai_bridge_schema,
        "validate_ai_bridge_runtime_schema",
        return_value={"physical_schema_verified": True},
    ):
        result = privileged_migrate_ai_bridge_schema(engine)

    expected_collations = [
        call.kwargs["expected_collation"]
        for call in validate_physical.call_args_list
    ]
    assert expected_collations == [
        ai_bridge_schema._AI_BRIDGE_LEGACY_COLLATION,
        ai_bridge_schema._AI_BRIDGE_COLLATION,
    ]
    sql = [str(call.args[0]).upper() for call in connection.execute.call_args_list]
    assert any(
        "CONVERT TO CHARACTER SET UTF8MB4 COLLATE UTF8MB4_UNICODE_CI"
        in statement
        for statement in sql
    )
    assert fingerprint_rows.call_count == 2
    assert events == [
        "PHYSICAL_REWRITE_PLAN",
        "alter",
        "PHYSICAL_REWRITE_VERIFIED",
    ]
    assert result["normalized_legacy_collation"] is True
    assert result["physical_rewrite_evidence"]["content_verified"] is True


def test_ai_bridge_legacy_collation_rejects_target_request_uid_collision():
    engine, connection = _mysql_ai_bridge_engine()
    with patch.object(
        ai_bridge_schema.metadata, "create_all"
    ), patch.object(
        ai_bridge_schema, "inspect", return_value=MagicMock()
    ), patch.object(
        ai_bridge_schema,
        "_index_shapes",
        return_value=set(ai_bridge_schema._AI_BRIDGE_INDEX_CONTRACT),
    ), patch.object(
        ai_bridge_schema, "ensure_evidence_table"
    ), patch.object(
        ai_bridge_schema, "load_pending_physical_rewrite_plan", return_value=None
    ), patch.object(
        ai_bridge_schema, "persist_and_verify_evidence"
    ), patch.object(
        ai_bridge_schema,
        "_mysql_table_collation",
        return_value=ai_bridge_schema._AI_BRIDGE_LEGACY_COLLATION,
    ), patch.object(
        ai_bridge_schema, "_validate_mysql_physical_contract"
    ), patch.object(
        ai_bridge_schema,
        "_request_uid_has_target_collation_duplicates",
        return_value=True,
    ), patch.object(
        ai_bridge_schema, "table_content_fingerprint"
    ) as fingerprint_rows, pytest.raises(RuntimeError, match="request_uid collides"):
        privileged_migrate_ai_bridge_schema(engine)

    fingerprint_rows.assert_not_called()
    assert not any(
        "ALTER TABLE" in str(call.args[0]).upper()
        for call in connection.execute.call_args_list
    )


def test_ai_bridge_legacy_collation_rejects_non_collation_physical_drift():
    engine, connection = _mysql_ai_bridge_engine()
    with patch.object(
        ai_bridge_schema.metadata, "create_all"
    ), patch.object(
        ai_bridge_schema, "inspect", return_value=MagicMock()
    ), patch.object(
        ai_bridge_schema,
        "_index_shapes",
        return_value=set(ai_bridge_schema._AI_BRIDGE_INDEX_CONTRACT),
    ), patch.object(
        ai_bridge_schema, "ensure_evidence_table"
    ), patch.object(
        ai_bridge_schema, "load_pending_physical_rewrite_plan", return_value=None
    ), patch.object(
        ai_bridge_schema, "persist_and_verify_evidence"
    ), patch.object(
        ai_bridge_schema,
        "_mysql_table_collation",
        return_value=ai_bridge_schema._AI_BRIDGE_LEGACY_COLLATION,
    ), patch.object(
        ai_bridge_schema,
        "_validate_mysql_physical_contract",
        side_effect=RuntimeError("AI bridge physical column differs: request_uid"),
    ), patch.object(
        ai_bridge_schema, "table_content_fingerprint"
    ) as fingerprint_rows, pytest.raises(RuntimeError, match="request_uid"):
        privileged_migrate_ai_bridge_schema(engine)

    fingerprint_rows.assert_not_called()
    assert not any(
        "ALTER TABLE" in str(call.args[0]).upper()
        for call in connection.execute.call_args_list
    )


def test_ai_bridge_legacy_collation_rejects_content_fingerprint_change():
    engine, _connection = _mysql_ai_bridge_engine()
    with patch.object(
        ai_bridge_schema.metadata, "create_all"
    ), patch.object(
        ai_bridge_schema, "inspect", return_value=MagicMock()
    ), patch.object(
        ai_bridge_schema,
        "_index_shapes",
        return_value=set(ai_bridge_schema._AI_BRIDGE_INDEX_CONTRACT),
    ), patch.object(
        ai_bridge_schema, "ensure_evidence_table"
    ), patch.object(
        ai_bridge_schema, "load_pending_physical_rewrite_plan", return_value=None
    ), patch.object(
        ai_bridge_schema, "persist_and_verify_evidence"
    ), patch.object(
        ai_bridge_schema,
        "_mysql_table_collation",
        return_value=ai_bridge_schema._AI_BRIDGE_LEGACY_COLLATION,
    ), patch.object(
        ai_bridge_schema, "_validate_mysql_physical_contract"
    ), patch.object(
        ai_bridge_schema,
        "_request_uid_has_target_collation_duplicates",
        return_value=False,
    ), patch.object(
        ai_bridge_schema,
        "table_content_fingerprint",
        side_effect=(
            {"row_count": 4, "content_sha256": "a" * 64},
            {"row_count": 4, "content_sha256": "b" * 64},
        ),
    ), pytest.raises(RuntimeError, match="content fingerprint changed"):
        privileged_migrate_ai_bridge_schema(engine)


def test_ai_bridge_resumes_target_after_alter_and_writes_verified_only():
    engine, connection = _mysql_ai_bridge_engine()
    fingerprint = {"row_count": 4, "content_sha256": "a" * 64}
    pending = _ai_bridge_pending_plan(fingerprint)
    with patch.object(
        ai_bridge_schema.metadata, "create_all"
    ), patch.object(
        ai_bridge_schema, "inspect", return_value=MagicMock()
    ), patch.object(
        ai_bridge_schema,
        "_index_shapes",
        return_value=set(ai_bridge_schema._AI_BRIDGE_INDEX_CONTRACT),
    ), patch.object(
        ai_bridge_schema, "ensure_evidence_table"
    ), patch.object(
        ai_bridge_schema,
        "load_pending_physical_rewrite_plan",
        return_value=pending,
    ), patch.object(
        ai_bridge_schema, "verify_pending_plan_content", return_value=fingerprint
    ) as verify_pending, patch.object(
        ai_bridge_schema,
        "_mysql_table_collation",
        return_value=ai_bridge_schema._AI_BRIDGE_COLLATION,
    ), patch.object(
        ai_bridge_schema, "_validate_mysql_physical_contract"
    ), patch.object(
        ai_bridge_schema, "table_content_fingerprint", return_value=fingerprint
    ), patch.object(
        ai_bridge_schema,
        "persist_and_verify_evidence",
        return_value={"evidence_verified": True, "evidence_row_count": 1},
    ) as persist, patch.object(
        ai_bridge_schema,
        "validate_ai_bridge_runtime_schema",
        return_value={"physical_schema_verified": True},
    ):
        result = privileged_migrate_ai_bridge_schema(engine)

    verify_pending.assert_called_once_with(connection, pending)
    persisted = persist.call_args.args[1]
    assert [record["action"] for record in persisted] == [
        "PHYSICAL_REWRITE_VERIFIED"
    ]
    assert persisted[0]["plan_sha256"] == pending["plan_sha256"]
    assert result["physical_rewrite_evidence"]["resumed_pending_plan"] is True
    assert not any(
        "ALTER TABLE" in str(call.args[0]).upper()
        for call in connection.execute.call_args_list
    )


def test_ai_bridge_rejects_stale_pending_target_contract_before_recovery():
    engine, connection = _mysql_ai_bridge_engine()
    fingerprint = {"row_count": 4, "content_sha256": "a" * 64}
    pending = _ai_bridge_pending_plan(
        fingerprint,
        target_contract_sha256="0" * 64,
    )
    with patch.object(
        ai_bridge_schema.metadata, "create_all"
    ), patch.object(
        ai_bridge_schema, "inspect", return_value=MagicMock()
    ), patch.object(
        ai_bridge_schema,
        "_index_shapes",
        return_value=set(ai_bridge_schema._AI_BRIDGE_INDEX_CONTRACT),
    ), patch.object(
        ai_bridge_schema, "ensure_evidence_table"
    ), patch.object(
        ai_bridge_schema,
        "load_pending_physical_rewrite_plan",
        return_value=pending,
    ), patch.object(
        ai_bridge_schema, "verify_pending_plan_content"
    ) as verify_pending, pytest.raises(RuntimeError, match="contract differs"):
        privileged_migrate_ai_bridge_schema(engine)

    verify_pending.assert_not_called()
    assert not any(
        "ALTER TABLE" in str(call.args[0]).upper()
        for call in connection.execute.call_args_list
    )
