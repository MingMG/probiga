# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timezone
from threading import Lock
from weakref import WeakKeyDictionary

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    insert,
    inspect,
    select,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

metadata = MetaData()
BIGINT_PK = BigInteger().with_variant(Integer, "sqlite")

auth_user = Table(
    "st_auth_user",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("username", String(64), nullable=False),
    Column("username_norm", String(64), nullable=False, unique=True),
    Column("password_hash", String(255), nullable=False),
    Column("role", String(20), nullable=False, default="ADMIN"),
    Column("is_active", Boolean, nullable=False, default=True),
    Column("failed_login_count", Integer, nullable=False, default=0),
    Column("locked_until", DateTime, nullable=True),
    Column("password_changed_at", DateTime, nullable=False),
    Column("last_login_at", DateTime, nullable=True),
    Column("created_at", DateTime, nullable=False),
    Column("updated_at", DateTime, nullable=False),
    mysql_charset="utf8mb4",
    mysql_collate="utf8mb4_unicode_ci",
    mysql_engine="InnoDB",
)

auth_session = Table(
    "st_auth_session",
    metadata,
    Column("id", BIGINT_PK, primary_key=True, autoincrement=True),
    Column("user_id", Integer, ForeignKey("st_auth_user.id"), nullable=False),
    Column("token_hash", String(64), nullable=False, unique=True),
    Column("issued_at", DateTime, nullable=False),
    Column("refresh_after", DateTime, nullable=False),
    Column("expires_at", DateTime, nullable=False),
    Column("last_seen_at", DateTime, nullable=False),
    Column("revoked_at", DateTime, nullable=True),
    Column("client_ip", String(64), nullable=False, default=""),
    Column("user_agent", String(255), nullable=False, default=""),
    mysql_charset="utf8mb4",
    mysql_collate="utf8mb4_unicode_ci",
    mysql_engine="InnoDB",
)
Index("ix_st_auth_session_user_active", auth_session.c.user_id, auth_session.c.revoked_at, auth_session.c.expires_at)

auth_audit = Table(
    "st_auth_audit",
    metadata,
    Column("id", BIGINT_PK, primary_key=True, autoincrement=True),
    Column("user_id", Integer, nullable=True),
    Column("username", String(64), nullable=False, default=""),
    Column("event_type", String(40), nullable=False),
    Column("success", Boolean, nullable=False),
    Column("client_ip", String(64), nullable=False, default=""),
    Column("detail", Text, nullable=False),
    Column("created_at", DateTime, nullable=False),
    mysql_charset="utf8mb4",
    mysql_collate="utf8mb4_unicode_ci",
    mysql_engine="InnoDB",
)
Index("ix_st_auth_audit_created", auth_audit.c.created_at)

auth_bootstrap = Table(
    "st_auth_bootstrap",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("registration_open", Boolean, nullable=False, default=True),
    Column("claimed_user_id", Integer, nullable=True),
    Column("created_at", DateTime, nullable=False),
    Column("updated_at", DateTime, nullable=False),
    mysql_charset="utf8mb4",
    mysql_collate="utf8mb4_unicode_ci",
    mysql_engine="InnoDB",
)

_schema_lock = Lock()
_initialized_engines: WeakKeyDictionary[Engine, bool] = WeakKeyDictionary()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


_AUTH_COLUMN_CONTRACTS = {
    "st_auth_user": {
        "id": ("int", "NO", "auto_increment"),
        "username": ("varchar(64)", "NO", ""),
        "username_norm": ("varchar(64)", "NO", ""),
        "password_hash": ("varchar(255)", "NO", ""),
        "role": ("varchar(20)", "NO", ""),
        "is_active": ("tinyint(1)", "NO", ""),
        "failed_login_count": ("int", "NO", ""),
        "locked_until": ("datetime", "YES", ""),
        "password_changed_at": ("datetime", "NO", ""),
        "last_login_at": ("datetime", "YES", ""),
        "created_at": ("datetime", "NO", ""),
        "updated_at": ("datetime", "NO", ""),
    },
    "st_auth_session": {
        "id": ("bigint", "NO", "auto_increment"),
        "user_id": ("int", "NO", ""),
        "token_hash": ("varchar(64)", "NO", ""),
        "issued_at": ("datetime", "NO", ""),
        "refresh_after": ("datetime", "NO", ""),
        "expires_at": ("datetime", "NO", ""),
        "last_seen_at": ("datetime", "NO", ""),
        "revoked_at": ("datetime", "YES", ""),
        "client_ip": ("varchar(64)", "NO", ""),
        "user_agent": ("varchar(255)", "NO", ""),
    },
    "st_auth_audit": {
        "id": ("bigint", "NO", "auto_increment"),
        "user_id": ("int", "YES", ""),
        "username": ("varchar(64)", "NO", ""),
        "event_type": ("varchar(40)", "NO", ""),
        "success": ("tinyint(1)", "NO", ""),
        "client_ip": ("varchar(64)", "NO", ""),
        "detail": ("text", "NO", ""),
        "created_at": ("datetime", "NO", ""),
    },
    "st_auth_bootstrap": {
        "id": ("int", "NO", ""),
        "registration_open": ("tinyint(1)", "NO", ""),
        "claimed_user_id": ("int", "YES", ""),
        "created_at": ("datetime", "NO", ""),
        "updated_at": ("datetime", "NO", ""),
    },
}
_AUTH_INDEX_CONTRACTS = {
    "st_auth_user": {(True, ("id",)), (True, ("username_norm",))},
    "st_auth_session": {
        (True, ("id",)),
        (True, ("token_hash",)),
        (False, ("user_id", "revoked_at", "expires_at")),
    },
    "st_auth_audit": {(True, ("id",)), (False, ("created_at",))},
    "st_auth_bootstrap": {(True, ("id",))},
}


def _inspector_index_shapes(inspector, table_name: str) -> set[tuple[bool, tuple[str, ...]]]:
    shapes: set[tuple[bool, tuple[str, ...]]] = set()
    primary = inspector.get_pk_constraint(table_name).get("constrained_columns") or []
    if primary:
        shapes.add((True, tuple(str(value) for value in primary)))
    for item in inspector.get_unique_constraints(table_name):
        columns = item.get("column_names") or []
        if columns:
            shapes.add((True, tuple(str(value) for value in columns)))
    for item in inspector.get_indexes(table_name):
        columns = item.get("column_names") or []
        if columns:
            shapes.add((bool(item.get("unique")), tuple(str(value) for value in columns)))
    return shapes


def _validate_auth_relational_contract(engine: Engine) -> None:
    inspector = inspect(engine)
    available = set(inspector.get_table_names())
    missing_tables = sorted(set(_AUTH_COLUMN_CONTRACTS) - available)
    if missing_tables:
        raise RuntimeError(f"auth schema tables are missing: {missing_tables}")
    for table_name, expected_columns in _AUTH_COLUMN_CONTRACTS.items():
        actual_columns = {str(item["name"]) for item in inspector.get_columns(table_name)}
        if actual_columns != set(expected_columns):
            raise RuntimeError(f"auth schema columns differ: {table_name}")
        shapes = _inspector_index_shapes(inspector, table_name)
        if not _AUTH_INDEX_CONTRACTS[table_name].issubset(shapes):
            raise RuntimeError(f"auth schema indexes differ: {table_name}")
    foreign_keys = inspector.get_foreign_keys("st_auth_session")
    if not any(
        tuple(item.get("constrained_columns") or ()) == ("user_id",)
        and str(item.get("referred_table") or "") == "st_auth_user"
        and tuple(item.get("referred_columns") or ()) == ("id",)
        for item in foreign_keys
    ):
        raise RuntimeError("auth session user foreign key differs")


def _validate_auth_mysql_physical_contract(engine: Engine) -> None:
    with engine.connect() as connection:
        table_rows = connection.execute(text(
            "SELECT TABLE_NAME,ENGINE,TABLE_COLLATION FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE 'st_auth_%'"
        )).mappings().all()
        tables = {
            str(row.get("TABLE_NAME") or row.get("table_name") or ""): row
            for row in table_rows
        }
        if set(tables) != set(_AUTH_COLUMN_CONTRACTS):
            raise RuntimeError("auth physical table inventory differs")
        for table_name, row in tables.items():
            engine_name = str(row.get("ENGINE") or row.get("engine") or "")
            collation = str(row.get("TABLE_COLLATION") or row.get("table_collation") or "")
            if engine_name.casefold() != "innodb" or collation != "utf8mb4_unicode_ci":
                raise RuntimeError(f"auth table engine/collation differs: {table_name}")
        column_rows = connection.execute(text(
            "SELECT TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_TYPE,IS_NULLABLE,"
            "COLUMN_DEFAULT,EXTRA,"
            "CHARACTER_SET_NAME,COLLATION_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE 'st_auth_%'"
        )).mappings().all()
    actual: dict[str, dict[str, tuple[object, ...]]] = {
        name: {} for name in _AUTH_COLUMN_CONTRACTS
    }
    for row in column_rows:
        get = lambda key: row.get(key) if key in row else row.get(key.casefold())
        table_name = str(get("TABLE_NAME") or "")
        column_name = str(get("COLUMN_NAME") or "")
        extra = str(get("EXTRA") or "").casefold().replace("default_generated", "").strip()
        actual.setdefault(table_name, {})[column_name] = (
            int(get("ORDINAL_POSITION") or 0),
            str(get("COLUMN_TYPE") or "").casefold(),
            str(get("IS_NULLABLE") or "").upper(),
            get("COLUMN_DEFAULT"),
            extra,
            str(get("CHARACTER_SET_NAME") or "").casefold() or None,
            str(get("COLLATION_NAME") or "").casefold() or None,
        )
    for table_name, columns in _AUTH_COLUMN_CONTRACTS.items():
        if set(actual.get(table_name, {})) != set(columns):
            raise RuntimeError(f"auth physical columns differ: {table_name}")
        for ordinal, (column_name, (column_type, nullable, extra)) in enumerate(
            columns.items(), 1
        ):
            observed = actual[table_name][column_name]
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
            if observed != expected:
                raise RuntimeError(f"auth physical column differs: {table_name}.{column_name}")


def validate_auth_runtime_schema(engine: Engine) -> dict[str, object]:
    """Read-only authentication schema and singleton validation."""
    _validate_auth_relational_contract(engine)
    if engine.dialect.name == "mysql":
        _validate_auth_mysql_physical_contract(engine)
    with engine.connect() as connection:
        singleton = connection.execute(text(
            "SELECT COUNT(*) AS row_count,MIN(id) AS min_id,MAX(id) AS max_id "
            "FROM st_auth_bootstrap"
        )).mappings().one()
    if (
        int(singleton.get("row_count") or 0) != 1
        or int(singleton.get("min_id") or 0) != 1
        or int(singleton.get("max_id") or 0) != 1
    ):
        raise RuntimeError("auth bootstrap singleton differs")
    return {
        "schema": "probiga.auth-physical-contract.v1",
        "status": "HEALTHY",
        "table_count": len(_AUTH_COLUMN_CONTRACTS),
        "physical_schema_verified": True,
        "bootstrap_singleton_verified": True,
        "runtime_ddl_required": False,
        "read_only": True,
    }


def privileged_migrate_auth_schema(engine: Engine) -> dict[str, object]:
    """Create and seed authentication storage in a fenced release window."""
    metadata.create_all(engine, checkfirst=True)
    now = _utcnow()
    try:
        with engine.begin() as conn:
            rows = conn.execute(select(auth_bootstrap.c.id)).scalars().all()
            if not rows:
                conn.execute(insert(auth_bootstrap).values(
                    id=1,
                    registration_open=True,
                    claimed_user_id=None,
                    created_at=now,
                    updated_at=now,
                ))
            elif rows != [1]:
                raise RuntimeError("auth bootstrap legacy rows differ")
    except IntegrityError:
        # Another privileged migrator may have inserted the exact singleton.
        pass
    return validate_auth_runtime_schema(engine)


def ensure_auth_schema(engine: Engine) -> None:
    """Compatibility runtime guard; never creates tables or seed rows."""
    with _schema_lock:
        if _initialized_engines.get(engine):
            return
        validate_auth_runtime_schema(engine)
        _initialized_engines[engine] = True


def reset_auth_schema_cache(engine: Engine | None = None) -> None:
    """Testing and operational helper; does not alter database contents."""
    with _schema_lock:
        if engine is None:
            _initialized_engines.clear()
        else:
            _initialized_engines.pop(engine, None)
