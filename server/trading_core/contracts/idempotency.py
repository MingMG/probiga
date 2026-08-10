"""Canonical SHA-256 keys for retry-safe execution operations."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

from .models import (
    ExecutionEventKind,
    ExecutionIntent,
    ExecutionResult,
    OrderSide,
    OrderType,
    TimeInForce,
)


def _integer(value: Any, field_name: str, *, minimum: int = 0) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field_name} must be an integer")
    if value < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}")
    return value


def _text(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _canonical(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("idempotency datetimes must be timezone-aware")
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("idempotency decimals must be finite")
        sign, digits, exponent = value.as_tuple()
        if not any(digits):
            return "0"
        while digits[-1] == 0:
            digits = digits[:-1]
            exponent += 1
        coefficient = "".join(str(digit) for digit in digits)
        point = len(coefficient) + exponent
        if point <= 0:
            rendered = "0." + "0" * (-point) + coefficient
        elif point >= len(coefficient):
            rendered = coefficient + "0" * (point - len(coefficient))
        else:
            rendered = coefficient[:point] + "." + coefficient[point:]
        return f"-{rendered}" if sign else rendered
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _canonical(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"unsupported idempotency value: {type(value).__name__}")


def _digest(namespace: str, payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        {
            "namespace": namespace,
            "payload": _canonical(payload),
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def execution_intent_idempotency_key(
    *,
    account_id: str,
    decision_id: str,
    instrument_id: str,
    side: Any,
    quantity: int,
    order_type: Any,
    time_in_force: Any,
    earliest_at: datetime,
    expires_at: datetime,
    limit_price: Decimal | None,
    rule_version: str,
    fee_profile_version: str,
    execution_policy_version: str,
    intent_version: int = 1,
) -> str:
    """Key one business intent without relying on a generated intent id."""

    account_id = _text(account_id, "account_id")
    decision_id = _text(decision_id, "decision_id")
    instrument_id = _text(instrument_id, "instrument_id")
    rule_version = _text(rule_version, "rule_version")
    fee_profile_version = _text(
        fee_profile_version,
        "fee_profile_version",
    )
    execution_policy_version = _text(
        execution_policy_version,
        "execution_policy_version",
    )
    _integer(quantity, "quantity", minimum=1)
    _integer(intent_version, "intent_version", minimum=1)
    side = OrderSide(side)
    order_type = OrderType(order_type)
    time_in_force = TimeInForce(time_in_force)
    return _digest(
        "trading-core.execution-intent.v1",
        {
            "account_id": account_id,
            "decision_id": decision_id,
            "instrument_id": instrument_id,
            "side": side,
            "quantity": quantity,
            "order_type": order_type,
            "time_in_force": time_in_force,
            "earliest_at": earliest_at,
            "expires_at": expires_at,
            "limit_price": limit_price,
            "rule_version": rule_version,
            "fee_profile_version": fee_profile_version,
            "execution_policy_version": execution_policy_version,
            "intent_version": intent_version,
        },
    )


def execution_result_idempotency_key(*, order_id: str, event_id: str) -> str:
    """Identify one external order event independently of its payload."""

    order_id = _text(order_id, "order_id")
    event_id = _text(event_id, "event_id")
    return _digest(
        "trading-core.execution-result.v1",
        {"order_id": order_id, "event_id": event_id},
    )


def execution_result_fingerprint(result: ExecutionResult) -> str:
    """Fingerprint stable business content for one external order event.

    ``received_at`` is deliberately excluded: it is local transport metadata
    and naturally changes when the same external event is redelivered.  The
    venue event identity and every business-relevant field remain covered so
    reusing an event id for a changed order event is still a hard conflict.
    """

    return _digest(
        "trading-core.execution-result-fingerprint.v2",
        {
            "intent_id": result.intent_id,
            "order_id": result.order_id,
            "event_id": result.event_id,
            "status": result.status,
            "occurred_at": result.occurred_at,
            "source_sequence": result.source_sequence,
            "last_fill_quantity": result.last_fill_quantity,
            "last_fill_price": result.last_fill_price,
            "reason_code": result.reason_code,
            "event_kind": ExecutionEventKind(result.event_kind),
        },
    )


def validate_intent_idempotency_key(intent: ExecutionIntent) -> bool:
    expected = execution_intent_idempotency_key(
        account_id=intent.account_id,
        decision_id=intent.decision_id,
        instrument_id=intent.instrument_id,
        side=intent.side,
        quantity=intent.quantity,
        order_type=intent.order_type,
        time_in_force=intent.time_in_force,
        earliest_at=intent.earliest_at,
        expires_at=intent.expires_at,
        limit_price=intent.limit_price,
        rule_version=intent.rule_version,
        fee_profile_version=intent.fee_profile_version,
        execution_policy_version=intent.execution_policy_version,
        intent_version=intent.intent_version,
    )
    return expected == intent.idempotency_key
