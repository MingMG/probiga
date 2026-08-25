# -*- coding: utf-8 -*-
from __future__ import annotations

from threading import Lock
from weakref import WeakKeyDictionary

from sqlalchemy import BigInteger, Column, DateTime, Index, Integer, MetaData, String, Table, Text, inspect, text
from sqlalchemy.engine import Engine

metadata = MetaData()
BIGINT_PK = BigInteger().with_variant(Integer, "sqlite")

ai_bridge_job = Table(
    "st_ai_bridge_job",
    metadata,
    Column("id", BIGINT_PK, primary_key=True, autoincrement=True),
    Column("request_uid", String(36), nullable=False, unique=True),
    Column("owner_user_id", Integer, nullable=False),
    Column("channel", String(16), nullable=False),
    Column("question", Text, nullable=False),
    Column("answer", Text, nullable=True),
    Column("status", String(20), nullable=False),
    Column("provider_attempt", String(32), nullable=True),
    Column("source", String(32), nullable=True),
    Column("source_label", String(80), nullable=True),
    Column("error_message", String(1000), nullable=True),
    Column("worker_id", String(120), nullable=True),
    Column("attempts", Integer, nullable=False, default=0),
    Column("lease_expires_at", DateTime, nullable=True),
    Column("created_at", DateTime, nullable=False),
    Column("started_at", DateTime, nullable=True),
    Column("completed_at", DateTime, nullable=True),
    Column("updated_at", DateTime, nullable=False),
    mysql_charset="utf8mb4",
    mysql_collate="utf8mb4_unicode_ci",
    mysql_engine="InnoDB",
)
Index("ix_st_ai_bridge_job_owner_created", ai_bridge_job.c.owner_user_id, ai_bridge_job.c.created_at)
Index("ix_st_ai_bridge_job_queue", ai_bridge_job.c.status, ai_bridge_job.c.created_at)

_schema_lock = Lock()
_initialized_engines: WeakKeyDictionary[Engine, bool] = WeakKeyDictionary()


_AI_BRIDGE_COLUMN_CONTRACT = {
    "id": ("bigint", "NO", "auto_increment"),
    "request_uid": ("varchar(36)", "NO", ""),
    "owner_user_id": ("int", "NO", ""),
    "channel": ("varchar(16)", "NO", ""),
    "question": ("text", "NO", ""),
    "answer": ("text", "YES", ""),
    "status": ("varchar(20)", "NO", ""),
    "provider_attempt": ("varchar(32)", "YES", ""),
    "source": ("varchar(32)", "YES", ""),
    "source_label": ("varchar(80)", "YES", ""),
    "error_message": ("varchar(1000)", "YES", ""),
    "worker_id": ("varchar(120)", "YES", ""),
    "attempts": ("int", "NO", ""),
    "lease_expires_at": ("datetime", "YES", ""),
    "created_at": ("datetime", "NO", ""),
    "started_at": ("datetime", "YES", ""),
    "completed_at": ("datetime", "YES", ""),
    "updated_at": ("datetime", "NO", ""),
}
_AI_BRIDGE_INDEX_CONTRACT = {
    (True, ("id",)),
    (True, ("request_uid",)),
    (False, ("owner_user_id", "created_at")),
    (False, ("status", "created_at")),
}


def _index_shapes(inspector) -> set[tuple[bool, tuple[str, ...]]]:
    shapes: set[tuple[bool, tuple[str, ...]]] = set()
    primary = inspector.get_pk_constraint("st_ai_bridge_job").get("constrained_columns") or []
    if primary:
        shapes.add((True, tuple(str(value) for value in primary)))
    for item in inspector.get_unique_constraints("st_ai_bridge_job"):
        columns = item.get("column_names") or []
        if columns:
            shapes.add((True, tuple(str(value) for value in columns)))
    for item in inspector.get_indexes("st_ai_bridge_job"):
        columns = item.get("column_names") or []
        if columns:
            shapes.add((bool(item.get("unique")), tuple(str(value) for value in columns)))
    return shapes


def validate_ai_bridge_runtime_schema(engine: Engine) -> dict[str, object]:
    """Read-only worker queue schema validation."""
    inspector = inspect(engine)
    if "st_ai_bridge_job" not in set(inspector.get_table_names()):
        raise RuntimeError("AI bridge job table is missing")
    columns = {str(item["name"]) for item in inspector.get_columns("st_ai_bridge_job")}
    if columns != set(_AI_BRIDGE_COLUMN_CONTRACT):
        raise RuntimeError("AI bridge job columns differ")
    if not _AI_BRIDGE_INDEX_CONTRACT.issubset(_index_shapes(inspector)):
        raise RuntimeError("AI bridge job indexes differ")
    if engine.dialect.name == "mysql":
        with engine.connect() as connection:
            table_row = connection.execute(text(
                "SELECT ENGINE,TABLE_COLLATION FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='st_ai_bridge_job'"
            )).mappings().one_or_none()
            if table_row is None:
                raise RuntimeError("AI bridge physical table is missing")
            engine_name = str(table_row.get("ENGINE") or table_row.get("engine") or "")
            collation = str(table_row.get("TABLE_COLLATION") or table_row.get("table_collation") or "")
            if engine_name.casefold() != "innodb" or collation != "utf8mb4_unicode_ci":
                raise RuntimeError("AI bridge table engine/collation differs")
            rows = connection.execute(text(
                "SELECT COLUMN_NAME,ORDINAL_POSITION,COLUMN_TYPE,IS_NULLABLE,"
                "COLUMN_DEFAULT,EXTRA,"
                "CHARACTER_SET_NAME,COLLATION_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='st_ai_bridge_job'"
            )).mappings().all()
        actual = {}
        for row in rows:
            get = lambda key: row.get(key) if key in row else row.get(key.casefold())
            name = str(get("COLUMN_NAME") or "")
            actual[name] = (
                int(get("ORDINAL_POSITION") or 0),
                str(get("COLUMN_TYPE") or "").casefold(),
                str(get("IS_NULLABLE") or "").upper(),
                get("COLUMN_DEFAULT"),
                str(get("EXTRA") or "").casefold().replace("default_generated", "").strip(),
                str(get("CHARACTER_SET_NAME") or "").casefold() or None,
                str(get("COLLATION_NAME") or "").casefold() or None,
            )
        if set(actual) != set(_AI_BRIDGE_COLUMN_CONTRACT):
            raise RuntimeError("AI bridge physical columns differ")
        for ordinal, (name, (column_type, nullable, extra)) in enumerate(
            _AI_BRIDGE_COLUMN_CONTRACT.items(), 1
        ):
            character = column_type.split("(", 1)[0] in {"varchar", "text"}
            expected = (
                ordinal,
                column_type,
                nullable,
                None,
                extra,
                "utf8mb4" if character else None,
                "utf8mb4_unicode_ci" if character else None,
            )
            if actual[name] != expected:
                raise RuntimeError(f"AI bridge physical column differs: {name}")
    return {
        "schema": "probiga.ai-bridge-physical-contract.v1",
        "status": "HEALTHY",
        "table_count": 1,
        "physical_schema_verified": True,
        "runtime_ddl_required": False,
        "read_only": True,
    }


def privileged_migrate_ai_bridge_schema(engine: Engine) -> dict[str, object]:
    """Create the queue table in a writer-fenced release window."""
    metadata.create_all(engine, checkfirst=True)
    return validate_ai_bridge_runtime_schema(engine)


def ensure_ai_bridge_schema(engine: Engine) -> None:
    """Compatibility runtime guard; never performs DDL."""
    with _schema_lock:
        if _initialized_engines.get(engine):
            return
        validate_ai_bridge_runtime_schema(engine)
        _initialized_engines[engine] = True


def reset_ai_bridge_schema_cache(engine: Engine | None = None) -> None:
    with _schema_lock:
        if engine is None:
            _initialized_engines.clear()
        else:
            _initialized_engines.pop(engine, None)
