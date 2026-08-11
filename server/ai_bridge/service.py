# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import and_, insert, or_, select, update
from sqlalchemy.engine import Engine

from server.ai_bridge.schema import ai_bridge_job, ensure_ai_bridge_schema

CHANNELS = {"stock", "general"}
SOURCES = {"codex_gpt", "deepseek_web"}
TERMINAL_STATUSES = {"completed", "failed"}


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.replace(microsecond=0).isoformat() + "Z"


def serialize_job(row: Any, *, include_question: bool = True) -> dict[str, Any]:
    data = dict(row)
    payload: dict[str, Any] = {
        "request_id": data["request_uid"],
        "channel": data["channel"],
        "status": data["status"],
        "answer": data.get("answer"),
        "provider_attempt": data.get("provider_attempt"),
        "source": data.get("source"),
        "source_label": data.get("source_label"),
        "error_message": data.get("error_message"),
        "attempts": int(data.get("attempts") or 0),
        "created_at": _iso(data.get("created_at")),
        "started_at": _iso(data.get("started_at")),
        "completed_at": _iso(data.get("completed_at")),
        "updated_at": _iso(data.get("updated_at")),
    }
    if include_question:
        payload["question"] = data["question"]
    return payload


def create_job(engine: Engine, *, owner_user_id: int, channel: str, question: str) -> dict[str, Any]:
    if channel not in CHANNELS:
        raise ValueError("unsupported channel")
    ensure_ai_bridge_schema(engine)
    now = utcnow()
    values = {
        "request_uid": str(uuid4()),
        "owner_user_id": int(owner_user_id),
        "channel": channel,
        "question": question,
        "answer": None,
        "status": "queued",
        "provider_attempt": None,
        "source": None,
        "source_label": None,
        "error_message": None,
        "worker_id": None,
        "attempts": 0,
        "lease_expires_at": None,
        "created_at": now,
        "started_at": None,
        "completed_at": None,
        "updated_at": now,
    }
    with engine.begin() as conn:
        result = conn.execute(insert(ai_bridge_job).values(**values))
        job_id = int(result.inserted_primary_key[0])
        row = conn.execute(select(ai_bridge_job).where(ai_bridge_job.c.id == job_id)).mappings().one()
    return serialize_job(row)


def get_job(engine: Engine, *, owner_user_id: int, request_uid: str) -> dict[str, Any] | None:
    ensure_ai_bridge_schema(engine)
    with engine.connect() as conn:
        row = conn.execute(
            select(ai_bridge_job).where(
                and_(
                    ai_bridge_job.c.request_uid == request_uid,
                    ai_bridge_job.c.owner_user_id == int(owner_user_id),
                )
            )
        ).mappings().first()
    return serialize_job(row) if row else None


def list_jobs(
    engine: Engine,
    *,
    owner_user_id: int,
    channel: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    ensure_ai_bridge_schema(engine)
    conditions = [ai_bridge_job.c.owner_user_id == int(owner_user_id)]
    if channel:
        if channel not in CHANNELS:
            raise ValueError("unsupported channel")
        conditions.append(ai_bridge_job.c.channel == channel)
    statement = (
        select(ai_bridge_job)
        .where(and_(*conditions))
        .order_by(ai_bridge_job.c.id.desc())
        .limit(max(1, min(int(limit), 50)))
    )
    with engine.connect() as conn:
        rows = conn.execute(statement).mappings().all()
    return [serialize_job(row) for row in rows]


def claim_job(engine: Engine, *, worker_id: str, lease_seconds: int) -> dict[str, Any] | None:
    ensure_ai_bridge_schema(engine)
    now = utcnow()
    expired = and_(
        ai_bridge_job.c.status == "processing",
        ai_bridge_job.c.lease_expires_at.is_not(None),
        ai_bridge_job.c.lease_expires_at < now,
    )
    statement = (
        select(ai_bridge_job)
        .where(or_(ai_bridge_job.c.status == "queued", expired))
        .order_by(ai_bridge_job.c.id.asc())
        .limit(1)
    )
    # Production currently runs a MySQL release without SKIP LOCKED support.
    # A regular row lock still serializes the small number of local workers and
    # keeps claiming atomic on both MySQL and SQLite-backed tests.
    statement = statement.with_for_update()
    with engine.begin() as conn:
        row = conn.execute(statement).mappings().first()
        if row is None:
            return None
        started_at = row["started_at"] or now
        conn.execute(
            update(ai_bridge_job)
            .where(ai_bridge_job.c.id == row["id"])
            .values(
                status="processing",
                provider_attempt=None,
                source=None,
                source_label=None,
                error_message=None,
                worker_id=worker_id,
                attempts=int(row["attempts"] or 0) + 1,
                lease_expires_at=now + timedelta(seconds=max(60, int(lease_seconds))),
                started_at=started_at,
                completed_at=None,
                updated_at=now,
            )
        )
        claimed = conn.execute(
            select(ai_bridge_job).where(ai_bridge_job.c.id == row["id"])
        ).mappings().one()
    return serialize_job(claimed)


def update_progress(
    engine: Engine,
    *,
    request_uid: str,
    worker_id: str,
    provider_attempt: str,
    lease_seconds: int,
) -> bool:
    if provider_attempt not in SOURCES:
        raise ValueError("unsupported provider")
    ensure_ai_bridge_schema(engine)
    now = utcnow()
    with engine.begin() as conn:
        result = conn.execute(
            update(ai_bridge_job)
            .where(
                and_(
                    ai_bridge_job.c.request_uid == request_uid,
                    ai_bridge_job.c.status == "processing",
                    ai_bridge_job.c.worker_id == worker_id,
                )
            )
            .values(
                provider_attempt=provider_attempt,
                lease_expires_at=now + timedelta(seconds=max(60, int(lease_seconds))),
                updated_at=now,
            )
        )
    return bool(result.rowcount)
def complete_job(
    engine: Engine,
    *,
    request_uid: str,
    worker_id: str,
    status: str,
    answer: str | None,
    source: str | None,
    source_label: str | None,
    error_message: str | None,
) -> bool:
    if status not in TERMINAL_STATUSES:
        raise ValueError("unsupported terminal status")
    if status == "completed" and (source not in SOURCES or answer is None):
        raise ValueError("completed jobs require an answer and valid source")
    ensure_ai_bridge_schema(engine)
    now = utcnow()
    with engine.begin() as conn:
        result = conn.execute(
            update(ai_bridge_job)
            .where(
                and_(
                    ai_bridge_job.c.request_uid == request_uid,
                    ai_bridge_job.c.status == "processing",
                    ai_bridge_job.c.worker_id == worker_id,
                )
            )
            .values(
                status=status,
                answer=answer if status == "completed" else None,
                source=source if status == "completed" else None,
                source_label=source_label if status == "completed" else None,
                error_message=(error_message or None) if status == "failed" else None,
                lease_expires_at=None,
                completed_at=now,
                updated_at=now,
            )
        )
    return bool(result.rowcount)
