from __future__ import annotations

from dataclasses import FrozenInstanceError, fields, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from server.integrations.v3_execution_projection import (
    ProjectionState,
    V3ExecutionPlanBinding,
    V3ExecutionProjection,
    bind_v3_execution_plan,
    project_execution_result,
    validate_v3_execution_plan_binding,
)
from server.trading_core.contracts import (
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
    OrderState,
    OrderTransitionReceipt,
    apply_execution_result,
    apply_execution_result_with_receipt,
    new_order_state,
    order_state_fingerprint,
    validate_order_transition_receipt,
)
from server.trading_v2.execution import _v3_plan_state


NOW = datetime(2026, 8, 3, 2, 0, tzinfo=timezone.utc)
EARLIEST = NOW - timedelta(minutes=1)
EXPIRES = NOW + timedelta(hours=1)


def _intent(*, quantity: int) -> ExecutionIntent:
    semantics = {
        "account_id": "account-1",
        "decision_id": "decision-1",
        "instrument_id": "600001.SH",
        "side": OrderSide.BUY,
        "quantity": quantity,
        "order_type": OrderType.LIMIT,
        "time_in_force": TimeInForce.DAY,
        "earliest_at": EARLIEST,
        "expires_at": EXPIRES,
        "limit_price": Decimal("10.01"),
        "rule_version": "rules-v1",
        "fee_profile_version": "fees-v1",
        "execution_policy_version": "execution-v1",
    }
    return ExecutionIntent(
        intent_id="intent-1",
        created_at=EARLIEST,
        idempotency_key=execution_intent_idempotency_key(**semantics),
        **semantics,
    )


def _result(
    state: OrderState,
    status: OrderStatus,
    *,
    fill_quantity: int = 0,
    occurred_at: datetime = NOW,
    event_id: str | None = None,
) -> ExecutionResult:
    sequence = state.last_source_sequence + 1
    resolved_event_id = event_id or f"event-{sequence}-{status.value.lower()}"
    return ExecutionResult(
        intent_id=state.intent_id,
        order_id=state.order_id,
        event_id=resolved_event_id,
        status=status,
        occurred_at=occurred_at,
        received_at=occurred_at + timedelta(milliseconds=5),
        source_sequence=sequence,
        idempotency_key=execution_result_idempotency_key(
            order_id=state.order_id,
            event_id=resolved_event_id,
        ),
        last_fill_quantity=fill_quantity,
        last_fill_price=Decimal("10.01") if fill_quantity else None,
    )


def _apply(
    state: OrderState,
    status: OrderStatus,
    *,
    fill_quantity: int = 0,
    occurred_at: datetime = NOW,
) -> OrderState:
    return apply_execution_result(
        state,
        _result(
            state,
            status,
            fill_quantity=fill_quantity,
            occurred_at=occurred_at,
        ),
    )


def _receipt(
    state: OrderState,
    status: OrderStatus,
    *,
    fill_quantity: int = 0,
    occurred_at: datetime = NOW,
) -> OrderTransitionReceipt:
    return apply_execution_result_with_receipt(
        state,
        _result(
            state,
            status,
            fill_quantity=fill_quantity,
            occurred_at=occurred_at,
        ),
    )


def _target_receipt(
    status: OrderStatus,
    cumulative: int,
) -> OrderTransitionReceipt:
    requested = cumulative if status == OrderStatus.FILLED else max(200, cumulative + 1)
    state = new_order_state(order_id="order-1", intent=_intent(quantity=requested))

    if status == OrderStatus.ACCEPTED:
        return _receipt(state, status)
    if status in {OrderStatus.REJECTED, OrderStatus.CANCELLED} and not cumulative:
        return _receipt(state, status)
    if status == OrderStatus.EXPIRED and not cumulative:
        return _receipt(state, status, occurred_at=EXPIRES)

    state = _apply(state, OrderStatus.ACCEPTED)
    if status == OrderStatus.QUEUED:
        return _receipt(state, status)
    if status == OrderStatus.CANCEL_PENDING and not cumulative:
        return _receipt(state, status)
    if status == OrderStatus.PARTIALLY_FILLED:
        return _receipt(state, status, fill_quantity=cumulative)
    if status == OrderStatus.FILLED:
        return _receipt(state, status, fill_quantity=cumulative)

    state = _apply(
        state,
        OrderStatus.PARTIALLY_FILLED,
        fill_quantity=cumulative,
    )
    return _receipt(
        state,
        status,
        occurred_at=EXPIRES if status == OrderStatus.EXPIRED else NOW,
    )


def _binding(
    receipt: OrderTransitionReceipt,
    *,
    execution_plan_id: str = "plan-1",
    source_intent_id: str | None = None,
    source_order_id: str | None = None,
    bound_at: datetime | None = None,
) -> V3ExecutionPlanBinding:
    return bind_v3_execution_plan(
        execution_plan_id=execution_plan_id,
        source_intent_id=source_intent_id or receipt.result.intent_id,
        source_order_id=source_order_id or receipt.result.order_id,
        bound_at=bound_at or receipt.previous_state.created_at,
    )


def _project(status: OrderStatus, cumulative: int):
    receipt = _target_receipt(status, cumulative)
    return project_execution_result(
        binding=_binding(receipt),
        transition=receipt,
    )


@pytest.mark.parametrize(
    ("status", "cumulative"),
    (
        (OrderStatus.ACCEPTED, 0),
        (OrderStatus.QUEUED, 0),
        (OrderStatus.CANCEL_PENDING, 0),
        (OrderStatus.PARTIALLY_FILLED, 100),
        (OrderStatus.FILLED, 100),
        (OrderStatus.CANCELLED, 0),
        (OrderStatus.CANCELLED, 100),
        (OrderStatus.EXPIRED, 0),
        (OrderStatus.EXPIRED, 100),
        (OrderStatus.REJECTED, 0),
    ),
)
def test_projection_is_compatible_with_frozen_v2_v3_state_mapping(
    status: OrderStatus,
    cumulative: int,
):
    projection = _project(status, cumulative)

    assert projection.state.value == _v3_plan_state(status.value, cumulative)
    assert projection.source_order_status == status


def test_partial_fill_remains_visible_while_cancel_is_pending():
    projection = _project(OrderStatus.CANCEL_PENDING, 100)

    assert projection.cumulative_filled_quantity == 100
    assert projection.state == ProjectionState.PAPER_PARTIALLY_FILLED
    assert projection.source_order_status == OrderStatus.CANCEL_PENDING
    assert _v3_plan_state(OrderStatus.CANCEL_PENDING.value, 100) == "PAPER_QUEUED"


def test_projection_is_retry_and_timezone_stable_but_later_event_is_distinct():
    receipt = _target_receipt(OrderStatus.PARTIALLY_FILLED, 100)
    binding = _binding(receipt)
    first = project_execution_result(
        binding=binding,
        transition=receipt,
    )
    retry = project_execution_result(
        binding=binding,
        transition=receipt,
    )
    china = timezone(timedelta(hours=8))
    equivalent_result = replace(
        receipt.result,
        occurred_at=receipt.result.occurred_at.astimezone(china),
        received_at=receipt.result.received_at.astimezone(china),
    )
    equivalent_receipt = apply_execution_result_with_receipt(
        receipt.previous_state,
        equivalent_result,
    )
    equivalent = project_execution_result(
        binding=binding,
        transition=equivalent_receipt,
    )
    later_receipt = _receipt(
        receipt.current_state,
        OrderStatus.CANCEL_PENDING,
    )
    later = project_execution_result(
        binding=binding,
        transition=later_receipt,
    )

    assert first == retry == equivalent
    assert later.projection_id != first.projection_id
    assert first.occurred_at.tzinfo is timezone.utc


def test_source_event_identity_is_stable_across_plan_or_payload_conflict():
    receipt = _target_receipt(OrderStatus.PARTIALLY_FILLED, 100)
    plan_a = project_execution_result(
        binding=_binding(receipt, execution_plan_id="plan-a"),
        transition=receipt,
    )
    plan_b = project_execution_result(
        binding=_binding(receipt, execution_plan_id="plan-b"),
        transition=receipt,
    )
    conflicting_result = replace(receipt.result, last_fill_quantity=150)
    conflicting_receipt = apply_execution_result_with_receipt(
        receipt.previous_state,
        conflicting_result,
    )
    conflicting = project_execution_result(
        binding=_binding(conflicting_receipt, execution_plan_id="plan-a"),
        transition=conflicting_receipt,
    )

    assert plan_a.projection_id == plan_b.projection_id == conflicting.projection_id
    assert plan_a.payload_hash != plan_b.payload_hash
    assert plan_a.payload_hash != conflicting.payload_hash
    assert plan_a.source_result_idempotency_key == (
        conflicting.source_result_idempotency_key
    )


def test_execution_plan_binding_is_immutable_stable_and_self_validating():
    receipt = _target_receipt(OrderStatus.PARTIALLY_FILLED, 100)
    binding = _binding(receipt)
    china = timezone(timedelta(hours=8))
    equivalent = bind_v3_execution_plan(
        execution_plan_id=binding.execution_plan_id,
        source_intent_id=binding.source_intent_id,
        source_order_id=binding.source_order_id,
        bound_at=binding.bound_at.astimezone(china),
    )

    assert binding == equivalent
    assert validate_v3_execution_plan_binding(binding) is True
    assert binding.bound_at.tzinfo is timezone.utc
    with pytest.raises(FrozenInstanceError):
        binding.execution_plan_id = "other-plan"
    with pytest.raises(ValueError, match="binding_hash"):
        replace(binding, source_order_id="other-order")


def test_projector_rejects_mismatched_or_late_execution_plan_binding():
    receipt = _target_receipt(OrderStatus.PARTIALLY_FILLED, 100)

    with pytest.raises(ValueError, match="intent_id differs"):
        project_execution_result(
            binding=_binding(receipt, source_intent_id="other-intent"),
            transition=receipt,
        )
    with pytest.raises(ValueError, match="order_id differs"):
        project_execution_result(
            binding=_binding(receipt, source_order_id="other-order"),
            transition=receipt,
        )
    with pytest.raises(ValueError, match="must equal canonical order creation"):
        project_execution_result(
            binding=_binding(
                receipt,
                bound_at=receipt.previous_state.created_at
                + timedelta(microseconds=1),
            ),
            transition=receipt,
        )

    class ForgedBinding(V3ExecutionPlanBinding):
        pass

    with pytest.raises(TypeError, match="exactly V3ExecutionPlanBinding"):
        project_execution_result(
            binding=object.__new__(ForgedBinding),
            transition=receipt,
        )


def test_projection_revalidates_identity_binding_state_and_time_fields():
    projection = _project(OrderStatus.PARTIALLY_FILLED, 100)

    assert isinstance(projection, V3ExecutionProjection)
    with pytest.raises(ValueError, match="idempotency_key"):
        replace(
            projection,
            source_result_idempotency_key="0" * 64,
            payload_hash="0" * 64,
        )
    with pytest.raises(ValueError, match="transition_id"):
        replace(
            projection,
            source_transition_id="0" * 64,
            payload_hash="0" * 64,
        )
    with pytest.raises(ValueError, match="binding_hash"):
        replace(
            projection,
            source_binding_hash="0" * 64,
            payload_hash="0" * 64,
        )
    with pytest.raises(ValueError, match="state does not match"):
        replace(
            projection,
            state=ProjectionState.REJECTED,
            payload_hash="0" * 64,
        )
    with pytest.raises(ValueError, match="cannot carry a cumulative fill"):
        replace(
            projection,
            source_order_status=OrderStatus.REJECTED,
            payload_hash="0" * 64,
        )
    late_binding = bind_v3_execution_plan(
        execution_plan_id=projection.execution_plan_id,
        source_intent_id=projection.source_intent_id,
        source_order_id=projection.source_order_id,
        bound_at=projection.source_order_created_at + timedelta(microseconds=1),
    )
    with pytest.raises(ValueError, match="must equal source order creation"):
        replace(
            projection,
            source_binding_hash=late_binding.binding_hash,
            binding_bound_at=late_binding.bound_at,
            payload_hash="0" * 64,
        )
    future_created_at = projection.occurred_at + timedelta(microseconds=1)
    future_binding = bind_v3_execution_plan(
        execution_plan_id=projection.execution_plan_id,
        source_intent_id=projection.source_intent_id,
        source_order_id=projection.source_order_id,
        bound_at=future_created_at,
    )
    with pytest.raises(ValueError, match="event cannot precede"):
        replace(
            projection,
            source_binding_hash=future_binding.binding_hash,
            binding_bound_at=future_created_at,
            source_order_created_at=future_created_at,
            payload_hash="0" * 64,
        )


def test_after_state_cannot_mint_its_own_transition_receipt():
    receipt = _target_receipt(OrderStatus.PARTIALLY_FILLED, 100)

    with pytest.raises(ValueError, match="already-applied"):
        apply_execution_result_with_receipt(
            receipt.current_state,
            receipt.result,
        )


def test_fabricated_transition_or_cumulative_fails_receipt_validation():
    receipt = _target_receipt(OrderStatus.PARTIALLY_FILLED, 100)
    with pytest.raises(ValueError, match="execution history replay"):
        replace(
            receipt.current_state,
            cumulative_filled_quantity=150,
        )
    forged = object.__new__(OrderTransitionReceipt)
    for item in fields(OrderTransitionReceipt):
        object.__setattr__(
            forged,
            item.name,
            (
                "0" * 64
                if item.name == "payload_hash"
                else getattr(receipt, item.name)
            ),
        )

    assert validate_order_transition_receipt(forged) is False
    with pytest.raises(ValueError, match="receipt is invalid"):
        project_execution_result(
            binding=_binding(receipt),
            transition=forged,
        )


def test_applied_result_order_is_canonicalized_in_state_hash():
    receipt = _target_receipt(OrderStatus.CANCEL_PENDING, 100)
    reversed_state = replace(
        receipt.current_state,
        applied_results=tuple(reversed(receipt.current_state.applied_results)),
    )

    assert reversed_state == receipt.current_state
    assert order_state_fingerprint(reversed_state) == receipt.current_state_hash


def test_projection_rejects_forged_receipt_subclass():
    class ForgedReceipt(OrderTransitionReceipt):
        pass

    with pytest.raises(TypeError, match="exactly OrderTransitionReceipt"):
        project_execution_result(
            binding=_binding(
                _target_receipt(OrderStatus.ACCEPTED, 0)
            ),
            transition=object.__new__(ForgedReceipt),
        )
