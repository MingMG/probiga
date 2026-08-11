# -*- coding: utf-8 -*-
from __future__ import annotations

from threading import Lock
from weakref import WeakKeyDictionary

from sqlalchemy import BigInteger, Column, DateTime, Index, Integer, MetaData, String, Table, Text
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
)
Index("ix_st_ai_bridge_job_owner_created", ai_bridge_job.c.owner_user_id, ai_bridge_job.c.created_at)
Index("ix_st_ai_bridge_job_queue", ai_bridge_job.c.status, ai_bridge_job.c.created_at)

_schema_lock = Lock()
_initialized_engines: WeakKeyDictionary[Engine, bool] = WeakKeyDictionary()


def ensure_ai_bridge_schema(engine: Engine) -> None:
    with _schema_lock:
        if _initialized_engines.get(engine):
            return
        metadata.create_all(engine, checkfirst=True)
        _initialized_engines[engine] = True


def reset_ai_bridge_schema_cache(engine: Engine | None = None) -> None:
    with _schema_lock:
        if engine is None:
            _initialized_engines.clear()
        else:
            _initialized_engines.pop(engine, None)
