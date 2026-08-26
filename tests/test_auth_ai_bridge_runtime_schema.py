from __future__ import annotations

import inspect as python_inspect

import pytest
from sqlalchemy import create_engine, text

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
