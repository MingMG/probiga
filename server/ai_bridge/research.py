"""Fenced research transport over the existing AI bridge queue; no runtime DDL.

One UUID identifies one immutable research attempt.  An expired lease is never
reclaimed: the caller's deadline terminates the original job instead.
"""
from __future__ import annotations

import base64
import gzip
import hashlib
import json
import re
import time
import zlib
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping
from threading import Lock
from weakref import WeakKeyDictionary, WeakSet
from uuid import UUID

from sqlalchemy import and_, insert, select, text, update
from sqlalchemy.exc import IntegrityError

from server.ai_bridge.schema import ai_bridge_job, ensure_ai_bridge_schema
from server.common.batch_db import create_batch_engine

MAX_RAW_BYTES = 240_000
MAX_ENVELOPE_BYTES = 60_000
MAX_RESPONSE_CHARS = 60_000
REQUEST_SCHEMA = "probiga.research-request.v1"
RESULT_SCHEMA = "probiga.research-result.v1"
_HEX = re.compile(r"^[0-9a-f]{64}$")
_ENGINE_LOCK = Lock()
_RESEARCH_ENGINES = WeakKeyDictionary()
_BOUNDED_ENGINES = WeakSet()


def get_research_engine(engine):
    """A dedicated 8/15/15s MySQL pool preserving the exact caller URL.

    The central factory retains TLS policy. URL equality also guards database,
    host, credentials, driver and every URL option; no global pool is changed.
    SQLite callers retain the original test engine.
    """
    if engine.dialect.name == "sqlite":
        return engine
    if engine.dialect.name != "mysql" or not getattr(engine, "url", None):
        _fail("engine", "unsupported_research_database")
    with _ENGINE_LOCK:
        if engine in _BOUNDED_ENGINES:
            return engine
        cached = _RESEARCH_ENGINES.get(engine)
        if cached is not None:
            return cached
        source_url = engine.url
        bounded = create_batch_engine(url=source_url, pool_timeout=8,
            connect_args={"connect_timeout": 8, "read_timeout": 15, "write_timeout": 15})
        if bounded.dialect.name != "mysql" or bounded.url != source_url:
            bounded.dispose()
            _fail("engine", "research_database_identity_mismatch")
        _RESEARCH_ENGINES[engine] = bounded
        _BOUNDED_ENGINES.add(bounded)
        return bounded


class ResearchBridgeError(RuntimeError):
    pass


def _fail(stage: str, reason: str) -> None:
    raise ResearchBridgeError(f"RESEARCH_BRIDGE_FAILED stage={stage} reason={reason}")


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat(timespec="microseconds") + "Z" if value is not None else None


def _deadline(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("timezone required")
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    except (TypeError, ValueError) as exc:
        raise ValueError("deadline_at must be an aware ISO datetime") from exc


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def research_input_sha256(payload: Mapping[str, Any]) -> str:
    material = {key: value for key, value in payload.items()
                if key not in {"request_id", "deadline_at", "input_sha256"}}
    return hashlib.sha256(canonical_json(material).encode("utf-8")).hexdigest()


def encode_envelope(payload: Mapping[str, Any], *, schema: str = REQUEST_SCHEMA) -> str:
    raw = canonical_json(dict(payload)).encode("utf-8")
    if len(raw) > MAX_RAW_BYTES:
        raise ValueError("research payload exceeds decoded byte limit")
    envelope = canonical_json({"schema": schema, "encoding": "gzip+base64", "raw_size": len(raw),
                               "sha256": hashlib.sha256(raw).hexdigest(),
                               "data": base64.b64encode(gzip.compress(raw, mtime=0)).decode("ascii")})
    if not envelope.isascii() or len(envelope.encode("ascii")) > MAX_ENVELOPE_BYTES:
        raise ValueError("research envelope exceeds TEXT byte budget")
    return envelope


def decode_envelope(encoded: str, *, schema: str = REQUEST_SCHEMA) -> dict:
    if not isinstance(encoded, str) or not encoded.isascii() or len(encoded) > MAX_ENVELOPE_BYTES:
        raise ValueError("invalid research envelope size or encoding")
    try:
        wrapper = json.loads(encoded)
        if not isinstance(wrapper, dict):
            raise ValueError("invalid research envelope object")
        if (wrapper.get("schema") != schema or wrapper.get("encoding") != "gzip+base64"
                or type(wrapper.get("raw_size")) is not int or not 0 < wrapper["raw_size"] <= MAX_RAW_BYTES):
            raise ValueError("invalid research envelope contract")
        compressed = base64.b64decode(wrapper["data"], validate=True)
        decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
        raw = decompressor.decompress(compressed, MAX_RAW_BYTES + 1)
        if (len(raw) > MAX_RAW_BYTES or decompressor.unconsumed_tail or not decompressor.eof
                or decompressor.unused_data or len(raw) != wrapper["raw_size"]
                or hashlib.sha256(raw).hexdigest() != wrapper.get("sha256")):
            raise ValueError("research envelope size or hash mismatch")
        value = json.loads(raw.decode("utf-8"))
        if not isinstance(value, dict):
            raise ValueError("research envelope must decode to an object")
        return value
    except (KeyError, TypeError, ValueError, UnicodeError, zlib.error) as exc:
        raise ValueError("invalid research envelope") from exc


def validate_payload(payload: Mapping[str, Any]) -> dict:
    value = dict(payload)
    try:
        if str(UUID(str(value.get("request_id")))) != value.get("request_id"):
            raise ValueError("noncanonical request_id")
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError("request_id must be a canonical UUID") from exc
    if not isinstance(value.get("stage"), str) or not 1 <= len(value["stage"]) <= 80:
        raise ValueError("stage must name one bounded research unit")
    for key in ("input_sha256", "rules_sha256"):
        if not isinstance(value.get(key), str) or not _HEX.fullmatch(value[key]):
            raise ValueError(f"invalid {key}")
    if research_input_sha256(value) != value["input_sha256"]:
        raise ValueError("research input hash mismatch")
    messages = value.get("messages")
    if not isinstance(messages, list) or not 1 <= len(messages) <= 8:
        raise ValueError("messages must contain one to eight messages")
    if any(not isinstance(item, dict) or item.get("role") not in {"system", "user", "assistant"}
           or not isinstance(item.get("content"), str) for item in messages):
        raise ValueError("invalid research message")
    if not isinstance(value.get("response_schema"), dict) or not value["response_schema"]:
        raise ValueError("response_schema is required")
    _deadline(value.get("deadline_at"))
    encode_envelope(value)
    return value


def _statement(statement):
    return statement.prefix_with("/*+ MAX_EXECUTION_TIME(15000) */", dialect="mysql")


def _prepare(conn) -> None:
    if conn.dialect.name == "mysql":
        conn.execute(text("SET SESSION innodb_lock_wait_timeout=5"))
        conn.execute(text("SET SESSION max_execution_time=15000"))


def _select(request_id: str, *, locked: bool = False):
    statement = _statement(select(ai_bridge_job).where(and_(
        ai_bridge_job.c.request_uid == request_id, ai_bridge_job.c.channel == "research")))
    return statement.with_for_update() if locked else statement


def _state(row) -> dict:
    return {"request_id": row["request_uid"], "channel": "research", "status": row["status"],
            "worker_id": row["worker_id"], "claim_generation": int(row["attempts"] or 0),
            "lease_expires_at": _iso(row["lease_expires_at"]), "error_message": row["error_message"],
            "updated_at": _iso(row["updated_at"])}


def _result(row) -> dict:
    if row["answer"]:
        result = decode_envelope(row["answer"], schema=RESULT_SCHEMA)
        if (result.get("request_id") != row["request_uid"] or result.get("status") != row["status"]
                or result.get("worker_id") != row["worker_id"]
                or result.get("claim_generation") != int(row["attempts"] or 0)):
            _fail("readback", "stored_result_identity_mismatch")
        if row["status"] == "completed":
            payload = validate_payload(decode_envelope(row["question"]))
            _validate_completed(result.get("raw_response"), result.get("provider_receipt"), payload)
            if result.get("source") != "codex_gpt" or result.get("raw_sha256") != hashlib.sha256(result["raw_response"].encode("utf-8")).hexdigest():
                _fail("readback", "stored_result_source_or_hash_mismatch")
        return result
    return _state(row)


def _expire(conn, row, reason: str, now: datetime) -> None:
    conn.execute(update(ai_bridge_job).where(and_(ai_bridge_job.c.id == row["id"],
        ai_bridge_job.c.channel == "research", ai_bridge_job.c.status.in_(["queued", "processing"])))
        .values(status="failed", error_message=reason[:1000], completed_at=now,
                lease_expires_at=None, updated_at=now))


def _enqueue(engine, payload: dict) -> None:
    ensure_ai_bridge_schema(engine)  # Existing read-only physical-contract guard.
    now = utcnow()
    def verify(row):
        if row["channel"] != "research":
            _fail("enqueue", "request_id_channel_collision")
        old = validate_payload(decode_envelope(row["question"]))
        if old["input_sha256"] != payload["input_sha256"]:
            _fail("enqueue", "request_id_input_conflict")
    try:
        with engine.begin() as conn:
            _prepare(conn)
            row = conn.execute(_statement(select(ai_bridge_job).where(ai_bridge_job.c.request_uid == payload["request_id"]))).mappings().first()
            if row:
                verify(row)
                return
            conn.execute(insert(ai_bridge_job).values(
                request_uid=payload["request_id"], owner_user_id=0, channel="research",
                question=encode_envelope(payload), answer=None, status="queued", provider_attempt=None,
                source=None, source_label=None, error_message=None, worker_id=None, attempts=0,
                lease_expires_at=None, created_at=now, started_at=None, completed_at=None, updated_at=now))
    except IntegrityError:
        # The sole retry is a readback of a concurrent identical insert, never a new job.
        with engine.connect() as conn:
            _prepare(conn)
            row = conn.execute(_statement(select(ai_bridge_job).where(ai_bridge_job.c.request_uid == payload["request_id"]))).mappings().one()
            verify(row)


def _load(engine, request_id: str):
    with engine.connect() as conn:
        _prepare(conn)
        row = conn.execute(_select(request_id)).mappings().one_or_none()
        return dict(row) if row else None


def request_research(engine, *, payload: dict, timeout_seconds: float = 120,
                     on_event: Callable[[dict], None] | None = None) -> dict:
    """Submit once, poll the same UUID, and return the exact stored provider result."""
    if not 0 < float(timeout_seconds) <= 300:
        raise ValueError("timeout_seconds must be in (0, 300]")
    engine = get_research_engine(engine)
    payload = validate_payload(payload)
    deadline = min(time.monotonic() + float(timeout_seconds),
                   time.monotonic() + max(0, (_deadline(payload["deadline_at"]) - utcnow()).total_seconds()))
    try:
        _enqueue(engine, payload)
        while True:
            row = _load(engine, payload["request_id"])
            if row is None:
                _fail("poll", "durable_job_missing")
            if on_event:
                on_event({"stage": payload["stage"], "request_id": payload["request_id"],
                          "status": row["status"].upper(), "total_units": 1,
                          "completed_units": int(row["status"] == "completed"),
                          "current_unit": payload["stage"], "last_progress_at": _iso(row["updated_at"])})
            if row["status"] == "completed":
                return _result(row)
            if row["status"] == "failed":
                _fail(payload["stage"], str(row["error_message"] or "worker_failed"))
            stored_deadline = _deadline(decode_envelope(row["question"])["deadline_at"])
            if time.monotonic() >= deadline or utcnow() >= stored_deadline:
                with engine.begin() as conn:
                    _prepare(conn)
                    locked = conn.execute(_select(payload["request_id"], locked=True)).mappings().one()
                    if locked["status"] == "completed":
                        return _result(locked)
                    _expire(conn, locked, "request_total_deadline_exceeded", utcnow())
                _fail(payload["stage"], "request_total_deadline_exceeded")
            time.sleep(min(1.2, max(0, deadline - time.monotonic())))
    except ResearchBridgeError:
        raise
    except Exception as exc:
        _fail(payload["stage"], type(exc).__name__)


def claim_research(engine, *, worker_id: str, lease_seconds: int = 60) -> dict | None:
    engine = get_research_engine(engine)
    ensure_ai_bridge_schema(engine)
    with engine.begin() as conn:
        _prepare(conn)
        # Deadline cleanup is not a reclaim: a dead processing owner can never
        # create a parallel model run, even if its caller also crashed.
        stale = conn.execute(_statement(select(ai_bridge_job).where(and_(
            ai_bridge_job.c.channel == "research", ai_bridge_job.c.status == "processing",
            ai_bridge_job.c.lease_expires_at <= utcnow())).order_by(ai_bridge_job.c.id).limit(10))
            .with_for_update()).mappings().all()
        for old in stale:
            try:
                elapsed = _deadline(decode_envelope(old["question"])["deadline_at"]) <= utcnow()
            except (KeyError, ValueError):
                elapsed = True
            if elapsed:
                _expire(conn, old, "processing_request_deadline_exceeded", utcnow())
        # Bounded cleanup of queued invalid/expired requests. Processing jobs are
        # deliberately excluded even after their lease expires.
        for _ in range(10):
            statement = _statement(select(ai_bridge_job).where(and_(
                ai_bridge_job.c.channel == "research", ai_bridge_job.c.status == "queued",
                ai_bridge_job.c.attempts == 0)).order_by(ai_bridge_job.c.id).limit(1)).with_for_update()
            row = conn.execute(statement).mappings().first()
            if row is None:
                return None
            now = utcnow()
            try:
                payload = validate_payload(decode_envelope(row["question"]))
            except ValueError:
                _expire(conn, row, "invalid_request_envelope", now)
                continue
            if _deadline(payload["deadline_at"]) <= now:
                _expire(conn, row, "request_deadline_expired_before_claim", now)
                continue
            lease = min(_deadline(payload["deadline_at"]), now + timedelta(seconds=max(10, min(int(lease_seconds), 60))))
            conn.execute(update(ai_bridge_job).where(ai_bridge_job.c.id == row["id"]).values(
                status="processing", worker_id=worker_id, attempts=1, provider_attempt="codex_gpt",
                started_at=now, updated_at=now, lease_expires_at=lease))
            updated = dict(row, status="processing", worker_id=worker_id, attempts=1,
                           lease_expires_at=lease, updated_at=now)
            return {**_state(updated), "payload": payload}
    return None


def _owned(row, worker_id: str, generation: int, *, active: bool = False) -> bool:
    if row is None or row["worker_id"] != worker_id or int(row["attempts"] or 0) != generation:
        return False
    if active:
        payload = decode_envelope(row["question"])
        return bool(row["status"] == "processing" and row["lease_expires_at"]
                    and row["lease_expires_at"] > utcnow() and _deadline(payload["deadline_at"]) > utcnow())
    return True


def progress_research(engine, *, request_id: str, worker_id: str, claim_generation: int,
                      lease_seconds: int = 60) -> dict | None:
    engine = get_research_engine(engine)
    with engine.begin() as conn:
        _prepare(conn)
        row = conn.execute(_select(request_id, locked=True)).mappings().one_or_none()
        if not _owned(row, worker_id, claim_generation, active=True):
            return None
        now = utcnow()
        payload = decode_envelope(row["question"])
        lease = min(_deadline(payload["deadline_at"]), now + timedelta(seconds=max(10, min(int(lease_seconds), 60))))
        conn.execute(update(ai_bridge_job).where(ai_bridge_job.c.id == row["id"]).values(
            lease_expires_at=lease, updated_at=now))
        return _state(dict(row, lease_expires_at=lease, updated_at=now))


def _validate_completed(raw: Any, receipt: Any, payload: dict) -> None:
    if not isinstance(raw, str) or not raw or len(raw) > MAX_RESPONSE_CHARS:
        raise ValueError("invalid research raw response")
    if not isinstance(receipt, dict) or receipt.get("source") != "codex_gpt":
        raise ValueError("completed research requires the actual codex_gpt receipt")
    for key in ("request_id", "input_sha256", "rules_sha256"):
        if receipt.get(key) != payload[key]:
            raise ValueError(f"provider receipt {key} mismatch")
    raw_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    if receipt.get("raw_sha256") != raw_hash:
        raise ValueError("provider receipt raw_sha256 mismatch")


def complete_research(engine, *, request_id: str, worker_id: str, claim_generation: int,
                      status: str, raw_response: str | None = None,
                      provider_receipt: dict | None = None, error_message: str | None = None) -> dict | None:
    engine = get_research_engine(engine)
    if status not in {"completed", "failed"}:
        raise ValueError("invalid terminal research status")
    with engine.begin() as conn:
        _prepare(conn)
        row = conn.execute(_select(request_id, locked=True)).mappings().one_or_none()
        if not _owned(row, worker_id, claim_generation):
            return None
        payload = validate_payload(decode_envelope(row["question"]))
        if status == "completed":
            _validate_completed(raw_response, provider_receipt, payload)
        raw_hash = hashlib.sha256(raw_response.encode("utf-8")).hexdigest() if raw_response is not None else None
        if row["status"] == "completed":
            stored = _result(row)
            return stored if status == "completed" and stored.get("raw_sha256") == raw_hash else None
        if row["status"] == "failed":
            return _result(row) if status == "failed" else None
        if not _owned(row, worker_id, claim_generation, active=True):
            return None
        now = utcnow()
        result = {"request_id": request_id, "status": status, "worker_id": worker_id,
                  "claim_generation": claim_generation, "source": "codex_gpt",
                  "raw_response": raw_response if status == "completed" else None,
                  "raw_sha256": raw_hash if status == "completed" else None,
                  "provider_receipt": provider_receipt or {}, "error_message": error_message,
                  "completed_at": _iso(now)}
        encoded = encode_envelope(result, schema=RESULT_SCHEMA)
        conn.execute(update(ai_bridge_job).where(ai_bridge_job.c.id == row["id"]).values(
            status=status, answer=encoded, source="codex_gpt", source_label="GPT（Codex）",
            error_message=(error_message or "worker_failed")[:1000] if status == "failed" else None,
            completed_at=now, updated_at=now, lease_expires_at=None))
        return result


def research_state(engine, *, request_id: str, worker_id: str, claim_generation: int) -> dict | None:
    engine = get_research_engine(engine)
    row = _load(engine, request_id)
    return _result(row) if _owned(row, worker_id, claim_generation) else None
