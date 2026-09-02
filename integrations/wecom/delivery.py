# -*- coding: utf-8 -*-
"""Reliable, auditable delivery for WeCom group-bot Markdown messages.

The webhook is deliberately treated as a write-only secret: it is used for the
HTTP request, but is never included in logs, exceptions, or delivery receipts.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlsplit

import httpx
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.engine import Engine

from server.common.runtime_table_schema import (
    RuntimeColumn,
    RuntimeIndex,
    RuntimeTable,
    privileged_normalize_mysql_storage,
    validate_runtime_tables,
)


log = logging.getLogger(__name__)

# httpx's INFO request line includes the complete URL.  A WeCom webhook keeps
# its credential in that URL, so allowing the library logger to inherit the
# batch process' INFO level would write the secret into scheduler output.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

MAX_MARKDOWN_BYTES = 4000
DELIVERY_RECEIPT_TABLE = "sys_wecom_delivery_receipt"
DELIVERY_RECEIPT_STARTED_AT_INDEX = "idx_wecom_delivery_receipt_started_at"
IDEMPOTENT_DELIVERY_NAMESPACE = uuid.UUID("74c7cd4d-4383-49e7-a74f-b1897765b37b")

_RECEIPT_DDL = f"""
CREATE TABLE IF NOT EXISTS {DELIVERY_RECEIPT_TABLE} (
    delivery_id VARCHAR(36) NOT NULL PRIMARY KEY,
    delivery_kind VARCHAR(64) NOT NULL,
    webhook_kind VARCHAR(32) NOT NULL,
    content_sha256 CHAR(64) NOT NULL,
    content_bytes BIGINT NOT NULL,
    segment_count INT NOT NULL,
    delivered_count INT NOT NULL DEFAULT 0,
    status VARCHAR(16) NOT NULL,
    error_code VARCHAR(64) NULL,
    error_message VARCHAR(512) NULL,
    idempotency_key_sha256 CHAR(64) NULL,
    audit_identity_json TEXT NULL,
    segments_json TEXT NOT NULL,
    started_at DATETIME NOT NULL,
    finished_at DATETIME NULL,
    updated_at DATETIME NOT NULL
)
"""
_MYSQL_RECEIPT_DDL = (
    _RECEIPT_DDL.rstrip()
    + " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
)

_DELIVERY_RECEIPT_CONTRACT = {
    DELIVERY_RECEIPT_TABLE: RuntimeTable(
        columns={
            "delivery_id": RuntimeColumn("varchar", False, character_length=36),
            "delivery_kind": RuntimeColumn("varchar", False, character_length=64),
            "webhook_kind": RuntimeColumn("varchar", False, character_length=32),
            "content_sha256": RuntimeColumn("char", False, character_length=64),
            "content_bytes": RuntimeColumn("bigint", False, numeric_precision=19, numeric_scale=0),
            "segment_count": RuntimeColumn("int", False, numeric_precision=10, numeric_scale=0),
            "delivered_count": RuntimeColumn("int", False, numeric_precision=10, numeric_scale=0),
            "status": RuntimeColumn("varchar", False, character_length=16),
            "error_code": RuntimeColumn("varchar", True, character_length=64),
            "error_message": RuntimeColumn("varchar", True, character_length=512),
            "idempotency_key_sha256": RuntimeColumn("char", True, character_length=64),
            "audit_identity_json": RuntimeColumn("text", True, character_length=65535),
            "segments_json": RuntimeColumn("text", False, character_length=65535),
            "started_at": RuntimeColumn("datetime", False, datetime_precision=0),
            "finished_at": RuntimeColumn("datetime", True, datetime_precision=0),
            "updated_at": RuntimeColumn("datetime", False, datetime_precision=0),
        },
        indexes=(
            RuntimeIndex(("delivery_id",), unique=True),
            RuntimeIndex(("started_at",), unique=False),
        ),
    )
}


@dataclass(frozen=True)
class MarkdownSegment:
    """One payload and its exact, unmodified slice of the source content."""

    body: str
    message: str


@dataclass(frozen=True)
class DeliveryResult:
    delivery_id: str
    success: bool
    segment_count: int
    delivered_count: int
    content_sha256: str
    idempotent_replay: bool = False


class WeComDeliveryError(RuntimeError):
    """A safe-to-log delivery failure (never contains a webhook URL/key)."""

    def __init__(
        self,
        message: str,
        *,
        delivery_id: str,
        result: DeliveryResult | None = None,
    ) -> None:
        super().__init__(message)
        self.delivery_id = delivery_id
        self.result = result


def _utcnow_naive() -> datetime:
    # MySQL DATETIME is timezone-naive. Values are consistently stored as UTC.
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _webhook_redactions(webhook_url: str) -> tuple[str, ...]:
    """Extract transient secret values without retaining the webhook itself."""

    try:
        query_values = tuple(
            value
            for _name, value in parse_qsl(urlsplit(webhook_url).query, keep_blank_values=False)
            if value
        )
    except (TypeError, ValueError):
        query_values = ()
    return tuple(sorted({webhook_url, *query_values}, key=len, reverse=True))


def _safe_error_message(
    value: object,
    *,
    redactions: tuple[str, ...] = (),
    limit: int = 512,
) -> str:
    """Remove URLs and query secrets from a remotely supplied error string."""

    rendered = str(value or "").replace("\x00", " ")
    for secret_value in redactions:
        if secret_value:
            rendered = rendered.replace(secret_value, "[redacted]")
    rendered = re.sub(r"https?://\S+", "[redacted-url]", rendered, flags=re.IGNORECASE)
    rendered = re.sub(
        r"(?i)(key|token|secret|webhook)\s*[=:]\s*[^\s&,;]+",
        r"\1=[redacted]",
        rendered,
    )
    return rendered[:limit]


def _largest_utf8_prefix(text_value: str, byte_limit: int) -> int:
    """Return the largest character index whose UTF-8 prefix fits the limit."""

    low, high = 0, len(text_value)
    while low < high:
        mid = (low + high + 1) // 2
        if len(text_value[:mid].encode("utf-8")) <= byte_limit:
            low = mid
        else:
            high = mid - 1
    return low


def _take_body_prefix(text_value: str, byte_limit: int) -> tuple[str, str]:
    if byte_limit <= 0:
        raise ValueError("WeCom segment header leaves no room for content")
    if len(text_value.encode("utf-8")) <= byte_limit:
        return text_value, ""

    hard_end = _largest_utf8_prefix(text_value, byte_limit)
    if hard_end <= 0:
        raise ValueError("a single UTF-8 character does not fit in the WeCom segment")

    # Prefer a paragraph/line boundary when it is not pathologically early.
    preferred_end = 0
    minimum_preferred = max(1, hard_end // 4)
    for separator in ("\n\n", "\n"):
        position = text_value.rfind(separator, 0, hard_end)
        candidate = position + len(separator) if position >= 0 else 0
        if candidate >= minimum_preferred:
            preferred_end = candidate
            break
    end = preferred_end or hard_end
    return text_value[:end], text_value[end:]


def build_markdown_segments(
    content: str,
    *,
    title: str,
    max_bytes: int = MAX_MARKDOWN_BYTES,
) -> tuple[MarkdownSegment, ...]:
    """Split Markdown without loss, measuring the final payload in UTF-8 bytes.

    A message that already fits is sent unchanged. Multi-part messages receive a
    small sequence header; the header itself is included in the 4000-byte limit.
    """

    if not isinstance(content, str) or not content:
        raise ValueError("WeCom Markdown content must not be empty")
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")
    if len(content.encode("utf-8")) <= max_bytes:
        return (MarkdownSegment(body=content, message=content),)

    # `content_bytes` is a safe upper bound on the number of non-empty chunks.
    # Reserving that digit width avoids a circular split/count/header calculation.
    content_bytes = len(content.encode("utf-8"))
    digit_width = len(str(max(2, content_bytes)))
    widest_counter = "9" * digit_width
    widest_header = f"{title} ({widest_counter}/{widest_counter})\n\n"
    body_limit = max_bytes - len(widest_header.encode("utf-8"))
    if body_limit <= 0:
        raise ValueError("WeCom segment title is too long")

    bodies: list[str] = []
    remaining = content
    while remaining:
        body, remaining = _take_body_prefix(remaining, body_limit)
        bodies.append(body)

    total = len(bodies)
    segments = tuple(
        MarkdownSegment(
            body=body,
            message=f"{title} ({index}/{total})\n\n{body}",
        )
        for index, body in enumerate(bodies, start=1)
    )
    if "".join(segment.body for segment in segments) != content:
        raise AssertionError("WeCom segmentation changed source content")
    if any(len(segment.message.encode("utf-8")) > max_bytes for segment in segments):
        raise AssertionError("WeCom segment exceeds byte limit")
    return segments


def _validate_sqlite_delivery_receipt(engine: Engine) -> None:
    inspector = inspect(engine)
    if DELIVERY_RECEIPT_TABLE not in set(inspector.get_table_names()):
        raise RuntimeError("WeCom delivery receipt schema is not prepared")
    columns = {str(item["name"]) for item in inspector.get_columns(DELIVERY_RECEIPT_TABLE)}
    if columns != set(_DELIVERY_RECEIPT_CONTRACT[DELIVERY_RECEIPT_TABLE].columns):
        raise RuntimeError("WeCom delivery receipt columns differ")
    shapes = {
        tuple(str(column) for column in (item.get("column_names") or ()))
        for item in inspector.get_indexes(DELIVERY_RECEIPT_TABLE)
    }
    primary = tuple(
        str(column)
        for column in (
            inspector.get_pk_constraint(DELIVERY_RECEIPT_TABLE).get("constrained_columns")
            or ()
        )
    )
    if primary != ("delivery_id",) or ("started_at",) not in shapes:
        raise RuntimeError("WeCom delivery receipt indexes differ")


def validate_delivery_receipt_runtime(engine: Engine) -> dict[str, Any]:
    """Read-only validation used by every delivery attempt."""
    if engine.dialect.name == "mysql":
        validate_runtime_tables(
            engine,
            _DELIVERY_RECEIPT_CONTRACT,
            context="WeCom delivery receipt",
        )
    else:
        _validate_sqlite_delivery_receipt(engine)
    return {
        "schema": "probiga.wecom-delivery-receipt.v1",
        "status": "HEALTHY",
        "table_count": 1,
        "physical_schema_verified": True,
        "runtime_ddl_required": False,
        "read_only": True,
    }


def privileged_migrate_delivery_receipt_table(engine: Engine) -> dict[str, Any]:
    """Create/upgrade the receipt table only in the fenced release window."""

    with engine.begin() as connection:
        connection.execute(text(
            _MYSQL_RECEIPT_DDL
            if engine.dialect.name == "mysql"
            else _RECEIPT_DDL
        ))
        columns = {
            str(item["name"])
            for item in inspect(connection).get_columns(DELIVERY_RECEIPT_TABLE)
        }
        for column_name, column_ddl in (
            ("idempotency_key_sha256", "CHAR(64) NULL"),
            ("audit_identity_json", "TEXT NULL"),
        ):
            if column_name not in columns:
                connection.execute(text(
                    f"ALTER TABLE {DELIVERY_RECEIPT_TABLE} "
                    f"ADD COLUMN {column_name} {column_ddl}"
                ))
        indexes = inspect(connection).get_indexes(DELIVERY_RECEIPT_TABLE)
        index_shapes = {
            tuple(str(column) for column in (item.get("column_names") or ()))
            for item in indexes
        }
        if ("started_at",) not in index_shapes:
            used_names = {str(item.get("name") or "") for item in indexes}
            index_name = DELIVERY_RECEIPT_STARTED_AT_INDEX
            suffix = 2
            while index_name in used_names:
                index_name = f"{DELIVERY_RECEIPT_STARTED_AT_INDEX}_{suffix}"
                suffix += 1
            connection.execute(
                text(
                    f"CREATE INDEX `{index_name}` "
                    f"ON `{DELIVERY_RECEIPT_TABLE}` (`started_at`)"
                )
            )
        if engine.dialect.name == "mysql":
            privileged_normalize_mysql_storage(
                connection,
                _DELIVERY_RECEIPT_CONTRACT,
            )
    return validate_delivery_receipt_runtime(engine)


def ensure_delivery_receipt_table(engine: Engine) -> None:
    """Compatibility runtime guard; never performs persistent DDL."""
    validate_delivery_receipt_runtime(engine)


def _insert_receipt(
    engine: Engine,
    *,
    delivery_id: str,
    delivery_kind: str,
    webhook_kind: str,
    content_sha256: str,
    content_bytes: int,
    segment_count: int,
    started_at: datetime,
    idempotency_key_sha256: str | None = None,
    audit_identity_json: str | None = None,
) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                INSERT INTO {DELIVERY_RECEIPT_TABLE}
                    (delivery_id, delivery_kind, webhook_kind, content_sha256,
                     content_bytes, segment_count, delivered_count, status,
                     error_code, error_message, idempotency_key_sha256,
                     audit_identity_json, segments_json, started_at,
                     finished_at, updated_at)
                VALUES
                    (:delivery_id, :delivery_kind, :webhook_kind, :content_sha256,
                     :content_bytes, :segment_count, 0, 'STARTED',
                     NULL, NULL, :idempotency_key_sha256,
                     :audit_identity_json, :segments_json, :started_at,
                     NULL, :updated_at)
                """
            ),
            {
                "delivery_id": delivery_id,
                "delivery_kind": delivery_kind,
                "webhook_kind": webhook_kind,
                "content_sha256": content_sha256,
                "content_bytes": content_bytes,
                "segment_count": segment_count,
                "idempotency_key_sha256": idempotency_key_sha256,
                "audit_identity_json": audit_identity_json,
                "segments_json": "[]",
                "started_at": started_at,
                "updated_at": started_at,
            },
        )


def _read_receipt(engine: Engine, delivery_id: str) -> dict[str, Any] | None:
    with engine.connect() as connection:
        row = connection.execute(
            text(
                f"SELECT * FROM {DELIVERY_RECEIPT_TABLE} "
                "WHERE delivery_id=:delivery_id LIMIT 1"
            ),
            {"delivery_id": delivery_id},
        ).mappings().one_or_none()
    return dict(row) if row is not None else None


def _canonical_audit_identity(value: dict[str, Any] | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, dict) or not value:
        raise ValueError("WeCom audit identity must be a non-empty object")
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _idempotent_result(
    row: dict[str, Any],
    *,
    delivery_id: str,
    delivery_kind: str,
    webhook_kind: str,
    content_sha256: str,
    content_bytes: int,
    segment_count: int,
    idempotency_key_sha256: str,
    audit_identity_json: str | None,
) -> DeliveryResult:
    same_identity = (
        str(row.get("delivery_id") or "") == delivery_id
        and str(row.get("delivery_kind") or "") == delivery_kind
        and str(row.get("webhook_kind") or "") == webhook_kind
        and str(row.get("content_sha256") or "").lower() == content_sha256
        and int(row.get("content_bytes") or -1) == content_bytes
        and int(row.get("segment_count") or -1) == segment_count
        and str(row.get("idempotency_key_sha256") or "").lower()
        == idempotency_key_sha256
        and (row.get("audit_identity_json") or None) == audit_identity_json
    )
    if not same_identity:
        raise WeComDeliveryError(
            "idempotent delivery identity differs from its durable receipt",
            delivery_id=delivery_id,
        )
    status = str(row.get("status") or "").upper()
    delivered_count = int(row.get("delivered_count") or 0)
    if status != "SUCCEEDED" or delivered_count != segment_count:
        raise WeComDeliveryError(
            "prior idempotent delivery is not safely replayable",
            delivery_id=delivery_id,
        )
    return DeliveryResult(
        delivery_id=delivery_id,
        success=True,
        segment_count=segment_count,
        delivered_count=delivered_count,
        content_sha256=content_sha256,
        idempotent_replay=True,
    )


def _update_receipt(
    engine: Engine,
    *,
    delivery_id: str,
    delivered_count: int,
    status: str,
    segment_receipts: list[dict[str, Any]],
    error_code: str | None = None,
    error_message: str | None = None,
    finished: bool = False,
) -> None:
    now = _utcnow_naive()
    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                UPDATE {DELIVERY_RECEIPT_TABLE}
                SET delivered_count = :delivered_count,
                    status = :status,
                    error_code = :error_code,
                    error_message = :error_message,
                    segments_json = :segments_json,
                    finished_at = :finished_at,
                    updated_at = :updated_at
                WHERE delivery_id = :delivery_id
                """
            ),
            {
                "delivery_id": delivery_id,
                "delivered_count": delivered_count,
                "status": status,
                "error_code": error_code,
                "error_message": _safe_error_message(error_message) if error_message else None,
                "segments_json": json.dumps(segment_receipts, ensure_ascii=False, separators=(",", ":")),
                "finished_at": now if finished else None,
                "updated_at": now,
            },
        )


def _segment_failure(
    *,
    index: int,
    byte_count: int,
    error_code: str,
    error_message: str,
    http_status: int | None = None,
    errcode: object = None,
    redactions: tuple[str, ...] = (),
) -> dict[str, Any]:
    return {
        "index": index,
        "bytes": byte_count,
        "success": False,
        "http_status": http_status,
        "errcode": (
            None
            if errcode is None
            else _safe_error_message(errcode, redactions=redactions, limit=64)
        ),
        "error_code": error_code,
        "error_message": _safe_error_message(error_message, redactions=redactions),
        "attempted_at": _utcnow_naive().isoformat(timespec="milliseconds") + "Z",
    }


def deliver_markdown(
    webhook_url: str | None,
    content: str,
    *,
    engine: Engine,
    delivery_kind: str,
    title: str,
    webhook_kind: str = "briefing",
    timeout: float = 15.0,
    pause_seconds: float = 2.0,
    max_bytes: int = MAX_MARKDOWN_BYTES,
    idempotency_key: str | None = None,
    audit_identity: dict[str, Any] | None = None,
) -> DeliveryResult:
    """Deliver every segment and durably record its sanitized response summary.

    Any missing configuration, transport/HTTP failure, malformed response,
    non-zero WeCom ``errcode``, partial delivery, or receipt-write failure raises
    :class:`WeComDeliveryError`. This makes batch entry points fail non-zero.
    """

    normalized_delivery_kind = str(delivery_kind or "unknown")[:64]
    normalized_webhook_kind = str(webhook_kind or "unknown")[:32]
    normalized_idempotency_key = str(idempotency_key or "").strip()
    delivery_id = str(
        uuid.uuid5(IDEMPOTENT_DELIVERY_NAMESPACE, normalized_idempotency_key)
        if normalized_idempotency_key
        else uuid.uuid4()
    )
    encoded = content.encode("utf-8") if isinstance(content, str) else b""
    content_sha256 = hashlib.sha256(encoded).hexdigest()
    idempotency_key_sha256 = (
        hashlib.sha256(normalized_idempotency_key.encode("utf-8")).hexdigest()
        if normalized_idempotency_key
        else None
    )
    try:
        segments = build_markdown_segments(content, title=title, max_bytes=max_bytes)
        ensure_delivery_receipt_table(engine)
        audit_identity_json = _canonical_audit_identity(audit_identity)
        if audit_identity_json is not None and idempotency_key_sha256 is None:
            raise ValueError("WeCom audit identity requires an idempotency key")
        if idempotency_key_sha256 is not None:
            existing = _read_receipt(engine, delivery_id)
            if existing is not None:
                return _idempotent_result(
                    existing,
                    delivery_id=delivery_id,
                    delivery_kind=normalized_delivery_kind,
                    webhook_kind=normalized_webhook_kind,
                    content_sha256=content_sha256,
                    content_bytes=len(encoded),
                    segment_count=len(segments),
                    idempotency_key_sha256=idempotency_key_sha256,
                    audit_identity_json=audit_identity_json,
                )
        started_at = _utcnow_naive()
        try:
            _insert_receipt(
                engine,
                delivery_id=delivery_id,
                delivery_kind=normalized_delivery_kind,
                webhook_kind=normalized_webhook_kind,
                content_sha256=content_sha256,
                content_bytes=len(encoded),
                segment_count=len(segments),
                started_at=started_at,
                idempotency_key_sha256=idempotency_key_sha256,
                audit_identity_json=audit_identity_json,
            )
        except IntegrityError:
            existing = (
                _read_receipt(engine, delivery_id)
                if idempotency_key_sha256 is not None
                else None
            )
            if existing is None:
                raise
            return _idempotent_result(
                existing,
                delivery_id=delivery_id,
                delivery_kind=normalized_delivery_kind,
                webhook_kind=normalized_webhook_kind,
                content_sha256=content_sha256,
                content_bytes=len(encoded),
                segment_count=len(segments),
                idempotency_key_sha256=idempotency_key_sha256,
                audit_identity_json=audit_identity_json,
            )
    except WeComDeliveryError:
        raise
    except Exception as exc:
        raise WeComDeliveryError(
            f"delivery receipt initialization failed ({type(exc).__name__})",
            delivery_id=delivery_id,
        ) from exc

    receipts: list[dict[str, Any]] = []
    delivered_count = 0

    if not str(webhook_url or "").strip():
        error_code = "MISSING_WEBHOOK"
        error_message = f"{webhook_kind} webhook is not configured"
        try:
            _update_receipt(
                engine,
                delivery_id=delivery_id,
                delivered_count=0,
                status="FAILED",
                segment_receipts=receipts,
                error_code=error_code,
                error_message=error_message,
                finished=True,
            )
        except Exception as exc:
            raise WeComDeliveryError(
                f"delivery receipt finalization failed ({type(exc).__name__})",
                delivery_id=delivery_id,
            ) from exc
        raise WeComDeliveryError(error_message, delivery_id=delivery_id)

    webhook_url_value = str(webhook_url)
    redactions = _webhook_redactions(webhook_url_value)

    with httpx.Client(timeout=timeout) as client:
        for index, segment in enumerate(segments, start=1):
            payload_bytes = len(segment.message.encode("utf-8"))
            receipt: dict[str, Any]
            try:
                response = client.post(
                    webhook_url_value,
                    json={"msgtype": "markdown", "markdown": {"content": segment.message}},
                    timeout=timeout,
                )
            except httpx.HTTPError as exc:
                receipt = _segment_failure(
                    index=index,
                    byte_count=payload_bytes,
                    error_code="HTTP_REQUEST_FAILED",
                    error_message=type(exc).__name__,
                    redactions=redactions,
                )
            except Exception as exc:
                receipt = _segment_failure(
                    index=index,
                    byte_count=payload_bytes,
                    error_code="REQUEST_FAILED",
                    error_message=type(exc).__name__,
                    redactions=redactions,
                )
            else:
                http_status = int(response.status_code)
                if not 200 <= http_status < 300:
                    receipt = _segment_failure(
                        index=index,
                        byte_count=payload_bytes,
                        error_code="HTTP_STATUS",
                        error_message=f"HTTP {http_status}",
                        http_status=http_status,
                    )
                else:
                    try:
                        response_data = response.json()
                    except Exception as exc:
                        receipt = _segment_failure(
                            index=index,
                            byte_count=payload_bytes,
                            error_code="INVALID_JSON",
                            error_message=type(exc).__name__,
                            http_status=http_status,
                        )
                    else:
                        errcode = response_data.get("errcode") if isinstance(response_data, dict) else None
                        errmsg = response_data.get("errmsg") if isinstance(response_data, dict) else None
                        if errcode != 0:
                            receipt = _segment_failure(
                                index=index,
                                byte_count=payload_bytes,
                                error_code="WECOM_ERRCODE",
                                error_message=errmsg or "missing/non-zero errcode",
                                http_status=http_status,
                                errcode=errcode,
                                redactions=redactions,
                            )
                        else:
                            delivered_count += 1
                            receipt = {
                                "index": index,
                                "bytes": payload_bytes,
                                "success": True,
                                "http_status": http_status,
                                "errcode": "0",
                                "errmsg": _safe_error_message(errmsg or "ok", redactions=redactions),
                                "attempted_at": _utcnow_naive().isoformat(timespec="milliseconds") + "Z",
                            }

            receipt["message_sha256"] = hashlib.sha256(
                segment.message.encode("utf-8")
            ).hexdigest()
            receipts.append(receipt)
            failed_so_far = next((item for item in receipts if not item["success"]), None)
            interim_status = "PARTIAL" if failed_so_far else "SENDING"
            try:
                _update_receipt(
                    engine,
                    delivery_id=delivery_id,
                    delivered_count=delivered_count,
                    status=interim_status,
                    segment_receipts=receipts,
                    error_code=failed_so_far.get("error_code") if failed_so_far else None,
                    error_message=failed_so_far.get("error_message") if failed_so_far else None,
                )
            except Exception as exc:
                raise WeComDeliveryError(
                    f"delivery receipt update failed ({type(exc).__name__})",
                    delivery_id=delivery_id,
                ) from exc

            log.info(
                "WeCom delivery %s segment %d/%d: %s (%d bytes)",
                delivery_id,
                index,
                len(segments),
                "success" if receipt["success"] else receipt.get("error_code", "failed"),
                payload_bytes,
            )
            if index < len(segments) and pause_seconds > 0:
                time.sleep(pause_seconds)

    success = delivered_count == len(segments)
    status = "SUCCEEDED" if success else "PARTIAL" if delivered_count else "FAILED"
    failed_receipt = next((item for item in receipts if not item["success"]), None)
    error_code = failed_receipt.get("error_code") if failed_receipt else None
    error_message = failed_receipt.get("error_message") if failed_receipt else None
    result = DeliveryResult(
        delivery_id=delivery_id,
        success=success,
        segment_count=len(segments),
        delivered_count=delivered_count,
        content_sha256=content_sha256,
    )
    try:
        _update_receipt(
            engine,
            delivery_id=delivery_id,
            delivered_count=delivered_count,
            status=status,
            segment_receipts=receipts,
            error_code=error_code,
            error_message=error_message,
            finished=True,
        )
    except Exception as exc:
        raise WeComDeliveryError(
            f"delivery receipt finalization failed ({type(exc).__name__})",
            delivery_id=delivery_id,
            result=result,
        ) from exc

    if not success:
        raise WeComDeliveryError(
            f"WeCom delivery incomplete: {delivered_count}/{len(segments)} segments delivered",
            delivery_id=delivery_id,
            result=result,
        )
    return result


__all__ = [
    "DELIVERY_RECEIPT_TABLE",
    "DELIVERY_RECEIPT_STARTED_AT_INDEX",
    "MAX_MARKDOWN_BYTES",
    "DeliveryResult",
    "MarkdownSegment",
    "WeComDeliveryError",
    "build_markdown_segments",
    "deliver_markdown",
    "ensure_delivery_receipt_table",
]
