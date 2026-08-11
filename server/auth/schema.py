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
    select,
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
)

_schema_lock = Lock()
_initialized_engines: WeakKeyDictionary[Engine, bool] = WeakKeyDictionary()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def ensure_auth_schema(engine: Engine) -> None:
    """Create the small authentication schema and its singleton bootstrap row."""
    with _schema_lock:
        if _initialized_engines.get(engine):
            return
        metadata.create_all(engine, checkfirst=True)
        now = _utcnow()
        try:
            with engine.begin() as conn:
                exists = conn.execute(
                    select(auth_bootstrap.c.id).where(auth_bootstrap.c.id == 1)
                ).scalar_one_or_none()
                if exists is None:
                    conn.execute(
                        insert(auth_bootstrap).values(
                            id=1,
                            registration_open=True,
                            claimed_user_id=None,
                            created_at=now,
                            updated_at=now,
                        )
                    )
        except IntegrityError:
            # A concurrent process inserted the singleton after our check.
            pass
        _initialized_engines[engine] = True


def reset_auth_schema_cache(engine: Engine | None = None) -> None:
    """Testing and operational helper; does not alter database contents."""
    with _schema_lock:
        if engine is None:
            _initialized_engines.clear()
        else:
            _initialized_engines.pop(engine, None)

