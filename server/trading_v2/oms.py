"""Deterministic V2 order state machine and idempotency keys."""
from __future__ import annotations

import hashlib
import json

from .domain import OrderStatus


ACTIVE_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.CREATED: frozenset({
        OrderStatus.RISK_APPROVED,
        OrderStatus.REJECTED,
        OrderStatus.CANCELLED,
        OrderStatus.EXPIRED,
    }),
    OrderStatus.RISK_APPROVED: frozenset({
        OrderStatus.QUEUED,
        OrderStatus.REJECTED,
        OrderStatus.CANCELLED,
        OrderStatus.EXPIRED,
    }),
    OrderStatus.QUEUED: frozenset({
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.EXPIRED,
    }),
    OrderStatus.PARTIALLY_FILLED: frozenset({
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.EXPIRED,
    }),
    OrderStatus.FILLED: frozenset(),
    OrderStatus.REJECTED: frozenset(),
    OrderStatus.CANCELLED: frozenset(),
    OrderStatus.EXPIRED: frozenset(),
}


def transition_order(previous: OrderStatus, next_status: OrderStatus) -> None:
    if next_status not in ACTIVE_TRANSITIONS[previous]:
        raise ValueError(f"illegal order transition: {previous} -> {next_status}")


def order_idempotency_key(
    *,
    account_id: str,
    decision_run_uid: str,
    intent_id: str,
    stock_code: str,
    side: str,
    target_quantity: int,
    intent_version: int,
) -> str:
    payload = [
        account_id,
        decision_run_uid,
        intent_id,
        stock_code,
        side,
        int(target_quantity),
        int(intent_version),
    ]
    return hashlib.sha256(
        json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def fill_idempotency_key(
    *,
    order_id: str,
    quote_event_id: str,
    match_event_id: str,
) -> str:
    if not quote_event_id or not match_event_id:
        raise ValueError("fill idempotency requires quote and match event ids")
    return hashlib.sha256(
        f"{order_id}|{quote_event_id}|{match_event_id}".encode("utf-8")
    ).hexdigest()
