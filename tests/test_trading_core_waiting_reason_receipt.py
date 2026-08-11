from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from server.integrations.v3_execution_projection import (
    ProjectionState,
    bind_v3_execution_plan,
    project_execution_result,
)
from server.trading_core.contracts import (
    ExecutionEventKind,
    ExecutionIntent,
    ExecutionResult,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    execution_intent_idempotency_key,
    execution_result_idempotency_key,
)
from server.trading_core.execution import (
    apply_execution_result,
    apply_execution_result_with_receipt,
    new_order_state,
    validate_order_transition_receipt,
)


NOW = datetime(2026, 8, 4, 1, 0, tzinfo=timezone.utc)


def _intent() -> ExecutionIntent:
    semantics = {
        "account_id": "paper-1",
        "decision_id": "decision-1",
        "instrument_id": "600001.SH",
        "side": OrderSide.BUY,
        "quantity": 100,
        "order_type": OrderType.LIMIT,
        "time_in_force": TimeInForce.DAY,
        "earliest_at": NOW,
        "expires_at": NOW + timedelta(hours=6),
        "limit_price": Decimal("10"),
        "rule_version": "rules-v1",
        "fee_profile_version": "fees-v1",
        "execution_policy_version": "execution-v1",
    }
    return ExecutionIntent(
        intent_id="intent-1",
        created_at=NOW - timedelta(minutes=1),
        idempotency_key=execution_intent_idempotency_key(**semantics),
        **semantics,
    )


def _result(
    *,
    event_id: str,
    status: OrderStatus,
    sequence: int,
    seconds: int,
    reason: str = "",
    event_kind: ExecutionEventKind = ExecutionEventKind.STATUS_TRANSITION,
) -> ExecutionResult:
    occurred_at = NOW + timedelta(seconds=seconds)
    return ExecutionResult(
        intent_id="intent-1",
        order_id="order-1",
        event_id=event_id,
        status=status,
        occurred_at=occurred_at,
        received_at=occurred_at + timedelta(milliseconds=1),
        source_sequence=sequence,
        idempotency_key=execution_result_idempotency_key(
            order_id="order-1",
            event_id=event_id,
        ),
        reason_code=reason,
        event_kind=event_kind,
    )


def _queued_state():
    state = new_order_state(order_id="order-1", intent=_intent())
    for result in (
        _result(
            event_id="accepted",
            status=OrderStatus.ACCEPTED,
            sequence=1,
            seconds=1,
        ),
        _result(
            event_id="queued",
            status=OrderStatus.QUEUED,
            sequence=2,
            seconds=2,
        ),
    ):
        state = apply_execution_result(state, result)
    return state


def _waiting_event(*, event_id="wait-1", sequence=3, reason="WAIT_NO_QUOTE"):
    return _result(
        event_id=event_id,
        status=OrderStatus.QUEUED,
        sequence=sequence,
        seconds=sequence,
        reason=reason,
        event_kind=ExecutionEventKind.WAITING_REASON_CHANGED,
    )


def test_waiting_reason_receipt_advances_one_unified_sequence_and_projects():
    queued = _queued_state()
    event = _waiting_event()
    receipt = apply_execution_result_with_receipt(queued, event)
    assert validate_order_transition_receipt(receipt) is True
    assert receipt.previous_state.status is OrderStatus.QUEUED
    assert receipt.previous_state.waiting_reason == ""
    assert receipt.current_state.status is OrderStatus.QUEUED
    assert receipt.current_state.waiting_reason == "WAIT_NO_QUOTE"
    assert receipt.current_state.cumulative_filled_quantity == 0
    assert receipt.current_state.last_source_sequence == 3

    binding = bind_v3_execution_plan(
        execution_plan_id="plan-1",
        source_intent_id="intent-1",
        source_order_id="order-1",
        bound_at=queued.created_at,
    )
    projection = project_execution_result(
        binding=binding,
        transition=receipt,
    )
    assert projection.state is ProjectionState.PAPER_QUEUED
    assert projection.source_sequence == 3
    assert projection.source_result_fingerprint == receipt.result_fingerprint
    assert projection.source_order_state_hash == receipt.current_state_hash


def test_waiting_reason_replay_is_exact_and_same_identity_conflict_is_fatal():
    queued = _queued_state()
    event = _waiting_event()
    receipt = apply_execution_result_with_receipt(queued, event)
    current = receipt.current_state
    assert apply_execution_result(current, event) is current
    with pytest.raises(ValueError, match="already-applied"):
        apply_execution_result_with_receipt(current, event)

    conflicting = replace(event, reason_code="WAIT_STALE_QUOTE")
    with pytest.raises(ValueError, match="different result payload"):
        apply_execution_result(current, conflicting)
    same_reason_new_event = _waiting_event(
        event_id="wait-2",
        sequence=4,
        reason="WAIT_NO_QUOTE",
    )
    with pytest.raises(ValueError, match="must change"):
        apply_execution_result(current, same_reason_new_event)


def test_ordinary_same_state_and_invalid_waiting_shapes_remain_rejected():
    queued = _queued_state()
    with pytest.raises(ValueError, match="illegal order transition"):
        apply_execution_result(
            queued,
            _result(
                event_id="ordinary-same-state",
                status=OrderStatus.QUEUED,
                sequence=3,
                seconds=3,
                reason="WAIT_NO_QUOTE",
            ),
        )
    with pytest.raises(ValueError, match="non-empty reason"):
        _waiting_event(reason="")
    with pytest.raises(ValueError, match="QUEUED or PARTIALLY_FILLED"):
        _result(
            event_id="bad-status",
            status=OrderStatus.ACCEPTED,
            sequence=3,
            seconds=3,
            reason="WAIT_NO_QUOTE",
            event_kind=ExecutionEventKind.WAITING_REASON_CHANGED,
        )
    with pytest.raises(ValueError, match="cannot carry a fill"):
        replace(
            _waiting_event(),
            last_fill_quantity=1,
            last_fill_price=Decimal("10"),
        )


@pytest.mark.parametrize(
    ("field_name", "forged_value"),
    (
        ("status", OrderStatus.ACCEPTED),
        ("reason_code", ""),
        ("last_fill_quantity", 1),
        ("event_kind", "WAITING_REASON_CHANGED"),
    ),
)
def test_frozen_result_bypass_is_revalidated_before_apply_and_receipt(
    field_name,
    forged_value,
):
    queued = _queued_state()
    forged = _waiting_event()
    object.__setattr__(forged, field_name, forged_value)
    with pytest.raises((TypeError, ValueError)):
        apply_execution_result(queued, forged)
    with pytest.raises((TypeError, ValueError)):
        apply_execution_result_with_receipt(queued, forged)


def test_frozen_receipt_payload_tamper_fails_full_revalidation():
    receipt = apply_execution_result_with_receipt(
        _queued_state(),
        _waiting_event(),
    )
    object.__setattr__(receipt.result, "reason_code", "WAIT_STALE_QUOTE")
    assert validate_order_transition_receipt(receipt) is False
