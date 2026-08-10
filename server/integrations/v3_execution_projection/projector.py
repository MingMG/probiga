"""Deterministic V3 read-model events derived from verified OMS transitions.

The adapter is a pure projection boundary: no V2/V3 database access and no
account, order, or broker mutation.  A projection can be created only from a
fresh :class:`OrderTransitionReceipt` issued by the neutral OMS.  Its identity
is the stable source event ``(order_id, event_id)``; plan binding and all event
content live in ``payload_hash`` so conflicting deliveries collide instead of
silently acquiring a second identity.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
import hashlib
import json

from server.trading_core.contracts import (
    OrderStatus,
    execution_result_idempotency_key,
)
from server.trading_core.execution import (
    OrderTransitionReceipt,
    order_transition_id,
    validate_order_transition_receipt,
)


class ProjectionState(str, Enum):
    PAPER_QUEUED = "PAPER_QUEUED"
    PAPER_PARTIALLY_FILLED = "PAPER_PARTIALLY_FILLED"
    PAPER_FILLED = "PAPER_FILLED"
    PAPER_PARTIAL_CANCELLED = "PAPER_PARTIAL_CANCELLED"
    PAPER_PARTIAL_EXPIRED = "PAPER_PARTIAL_EXPIRED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _sha256(value: object, field_name: str) -> str:
    normalized = _required_text(value, field_name).lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    return normalized


def _quantity(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError("cumulative_filled_quantity must be an integer")
    if value < 0:
        raise ValueError("cumulative_filled_quantity must be non-negative")
    return value


def _aware_utc(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _canonical(value: object) -> object:
    if isinstance(value, datetime):
        return _aware_utc(value, "datetime").isoformat(timespec="microseconds")
    if isinstance(value, Decimal):
        sign, digits, exponent = value.as_tuple()
        if not any(digits):
            return {"sign": 0, "digits": "0", "exponent": 0}
        while digits and digits[-1] == 0:
            digits = digits[:-1]
            exponent += 1
        return {
            "sign": sign,
            "digits": "".join(str(digit) for digit in digits) or "0",
            "exponent": exponent,
        }
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (tuple, list)):
        return [_canonical(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _canonical(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"unsupported projection value: {type(value).__name__}")


def _digest(namespace: str, payload: dict[str, object]) -> str:
    canonical = json.dumps(
        {"namespace": namespace, "payload": _canonical(payload)},
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _projection_identity(*, order_id: str, event_id: str) -> str:
    return _digest(
        "v3.execution-projection.source-event.v1",
        {"source_order_id": order_id, "source_event_id": event_id},
    )


def _binding_identity(*, execution_plan_id: str) -> str:
    return _digest(
        "v3.execution-plan-binding.identity.v1",
        {"execution_plan_id": execution_plan_id},
    )


def _binding_payload(
    *,
    binding_id: str,
    execution_plan_id: str,
    source_intent_id: str,
    source_order_id: str,
    bound_at: datetime,
) -> dict[str, object]:
    return {
        "binding_id": binding_id,
        "execution_plan_id": execution_plan_id,
        "source_intent_id": source_intent_id,
        "source_order_id": source_order_id,
        "bound_at": bound_at,
    }


def _binding_hash(
    *,
    binding_id: str,
    execution_plan_id: str,
    source_intent_id: str,
    source_order_id: str,
    bound_at: datetime,
) -> str:
    return _digest(
        "v3.execution-plan-binding.payload.v1",
        _binding_payload(
            binding_id=binding_id,
            execution_plan_id=execution_plan_id,
            source_intent_id=source_intent_id,
            source_order_id=source_order_id,
            bound_at=bound_at,
        ),
    )


@dataclass(frozen=True, slots=True)
class V3ExecutionPlanBinding:
    """Immutable one-to-one binding from a V3 plan to a neutral OMS order."""

    binding_id: str
    binding_hash: str
    execution_plan_id: str
    source_intent_id: str
    source_order_id: str
    bound_at: datetime

    def __post_init__(self) -> None:
        for field_name in (
            "execution_plan_id",
            "source_intent_id",
            "source_order_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name),
            )
        for field_name in ("binding_id", "binding_hash"):
            object.__setattr__(
                self,
                field_name,
                _sha256(getattr(self, field_name), field_name),
            )
        object.__setattr__(
            self,
            "bound_at",
            _aware_utc(self.bound_at, "bound_at"),
        )
        expected_id = _binding_identity(
            execution_plan_id=self.execution_plan_id,
        )
        if self.binding_id != expected_id:
            raise ValueError("binding_id does not match execution_plan_id")
        expected_hash = _binding_hash(
            binding_id=self.binding_id,
            execution_plan_id=self.execution_plan_id,
            source_intent_id=self.source_intent_id,
            source_order_id=self.source_order_id,
            bound_at=self.bound_at,
        )
        if self.binding_hash != expected_hash:
            raise ValueError("binding_hash does not match binding fields")


def bind_v3_execution_plan(
    *,
    execution_plan_id: str,
    source_intent_id: str,
    source_order_id: str,
    bound_at: datetime,
) -> V3ExecutionPlanBinding:
    """Create a deterministic immutable plan/intent/order binding."""

    plan_id = _required_text(execution_plan_id, "execution_plan_id")
    intent_id = _required_text(source_intent_id, "source_intent_id")
    order_id = _required_text(source_order_id, "source_order_id")
    normalized_bound_at = _aware_utc(bound_at, "bound_at")
    binding_id = _binding_identity(execution_plan_id=plan_id)
    return V3ExecutionPlanBinding(
        binding_id=binding_id,
        binding_hash=_binding_hash(
            binding_id=binding_id,
            execution_plan_id=plan_id,
            source_intent_id=intent_id,
            source_order_id=order_id,
            bound_at=normalized_bound_at,
        ),
        execution_plan_id=plan_id,
        source_intent_id=intent_id,
        source_order_id=order_id,
        bound_at=normalized_bound_at,
    )


def validate_v3_execution_plan_binding(
    binding: V3ExecutionPlanBinding,
) -> bool:
    """Recompute a binding after deserialization or frozen-object bypasses."""

    if type(binding) is not V3ExecutionPlanBinding:
        raise TypeError("binding must be exactly V3ExecutionPlanBinding")
    try:
        expected = bind_v3_execution_plan(
            execution_plan_id=binding.execution_plan_id,
            source_intent_id=binding.source_intent_id,
            source_order_id=binding.source_order_id,
            bound_at=binding.bound_at,
        )
        return binding == expected
    except (AttributeError, TypeError, ValueError):
        return False


def _projection_payload(
    *,
    projection_id: str,
    execution_plan_id: str,
    source_binding_id: str,
    source_binding_hash: str,
    binding_bound_at: datetime,
    source_intent_id: str,
    source_order_id: str,
    source_order_created_at: datetime,
    source_event_id: str,
    source_sequence: int,
    source_result_idempotency_key: str,
    source_result_fingerprint: str,
    source_transition_id: str,
    source_transition_payload_hash: str,
    source_order_state_hash: str,
    source_order_status: OrderStatus,
    cumulative_filled_quantity: int,
    state: ProjectionState,
    occurred_at: datetime,
) -> dict[str, object]:
    return {
        "projection_id": projection_id,
        "execution_plan_id": execution_plan_id,
        "source_binding_id": source_binding_id,
        "source_binding_hash": source_binding_hash,
        "binding_bound_at": binding_bound_at,
        "source_intent_id": source_intent_id,
        "source_order_id": source_order_id,
        "source_order_created_at": source_order_created_at,
        "source_event_id": source_event_id,
        "source_sequence": source_sequence,
        "source_result_idempotency_key": source_result_idempotency_key,
        "source_result_fingerprint": source_result_fingerprint,
        "source_transition_id": source_transition_id,
        "source_transition_payload_hash": source_transition_payload_hash,
        "source_order_state_hash": source_order_state_hash,
        "source_order_status": source_order_status,
        "cumulative_filled_quantity": cumulative_filled_quantity,
        "state": state,
        "occurred_at": occurred_at,
    }


def _projection_state(
    status: OrderStatus,
    cumulative_filled_quantity: int,
) -> ProjectionState:
    if status == OrderStatus.FILLED:
        return ProjectionState.PAPER_FILLED
    if status == OrderStatus.PARTIALLY_FILLED:
        return ProjectionState.PAPER_PARTIALLY_FILLED
    if status == OrderStatus.CANCEL_PENDING:
        return (
            ProjectionState.PAPER_PARTIALLY_FILLED
            if cumulative_filled_quantity
            else ProjectionState.PAPER_QUEUED
        )
    if status == OrderStatus.CANCELLED:
        return (
            ProjectionState.PAPER_PARTIAL_CANCELLED
            if cumulative_filled_quantity
            else ProjectionState.CANCELLED
        )
    if status == OrderStatus.EXPIRED:
        return (
            ProjectionState.PAPER_PARTIAL_EXPIRED
            if cumulative_filled_quantity
            else ProjectionState.EXPIRED
        )
    if status == OrderStatus.REJECTED:
        return ProjectionState.REJECTED
    return ProjectionState.PAPER_QUEUED


@dataclass(frozen=True, slots=True)
class V3ExecutionProjection:
    projection_id: str
    payload_hash: str
    execution_plan_id: str
    source_binding_id: str
    source_binding_hash: str
    binding_bound_at: datetime
    source_intent_id: str
    source_order_id: str
    source_order_created_at: datetime
    source_event_id: str
    source_sequence: int
    source_result_idempotency_key: str
    source_result_fingerprint: str
    source_transition_id: str
    source_transition_payload_hash: str
    source_order_state_hash: str
    source_order_status: OrderStatus
    cumulative_filled_quantity: int
    state: ProjectionState
    occurred_at: datetime

    def __post_init__(self) -> None:
        for field_name in (
            "execution_plan_id",
            "source_intent_id",
            "source_order_id",
            "source_event_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name),
            )
        for field_name in (
            "projection_id",
            "payload_hash",
            "source_binding_id",
            "source_binding_hash",
            "source_result_idempotency_key",
            "source_result_fingerprint",
            "source_transition_id",
            "source_transition_payload_hash",
            "source_order_state_hash",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(getattr(self, field_name), field_name),
            )
        if (
            not isinstance(self.source_sequence, int)
            or isinstance(self.source_sequence, bool)
            or self.source_sequence < 1
        ):
            raise ValueError("source_sequence must be a positive integer")
        _quantity(self.cumulative_filled_quantity)
        object.__setattr__(
            self,
            "source_order_status",
            OrderStatus(self.source_order_status),
        )
        object.__setattr__(self, "state", ProjectionState(self.state))
        object.__setattr__(
            self,
            "binding_bound_at",
            _aware_utc(self.binding_bound_at, "binding_bound_at"),
        )
        object.__setattr__(
            self,
            "source_order_created_at",
            _aware_utc(
                self.source_order_created_at,
                "source_order_created_at",
            ),
        )
        object.__setattr__(
            self,
            "occurred_at",
            _aware_utc(self.occurred_at, "occurred_at"),
        )
        expected_id = _projection_identity(
            order_id=self.source_order_id,
            event_id=self.source_event_id,
        )
        if self.projection_id != expected_id:
            raise ValueError("projection_id does not match source event identity")
        expected_idempotency_key = execution_result_idempotency_key(
            order_id=self.source_order_id,
            event_id=self.source_event_id,
        )
        if self.source_result_idempotency_key != expected_idempotency_key:
            raise ValueError(
                "source_result_idempotency_key does not match source event"
            )
        expected_transition_id = order_transition_id(
            order_id=self.source_order_id,
            event_id=self.source_event_id,
        )
        if self.source_transition_id != expected_transition_id:
            raise ValueError("source_transition_id does not match source event")
        expected_binding_id = _binding_identity(
            execution_plan_id=self.execution_plan_id,
        )
        if self.source_binding_id != expected_binding_id:
            raise ValueError(
                "source_binding_id does not match execution_plan_id"
            )
        expected_binding_hash = _binding_hash(
            binding_id=self.source_binding_id,
            execution_plan_id=self.execution_plan_id,
            source_intent_id=self.source_intent_id,
            source_order_id=self.source_order_id,
            bound_at=self.binding_bound_at,
        )
        if self.source_binding_hash != expected_binding_hash:
            raise ValueError("source_binding_hash does not match projection binding")
        if self.binding_bound_at != self.source_order_created_at:
            raise ValueError(
                "execution plan binding must equal source order creation time"
            )
        if self.source_order_created_at > self.occurred_at:
            raise ValueError("source event cannot precede order creation")
        if self.source_order_status == OrderStatus.CREATED:
            raise ValueError("execution projection cannot carry CREATED status")
        if self.source_order_status in {
            OrderStatus.ACCEPTED,
            OrderStatus.QUEUED,
            OrderStatus.REJECTED,
        } and self.cumulative_filled_quantity:
            raise ValueError(
                f"{self.source_order_status.value} cannot carry a cumulative fill"
            )
        if self.source_order_status in {
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
        } and not self.cumulative_filled_quantity:
            raise ValueError(
                f"{self.source_order_status.value} requires a cumulative fill"
            )
        expected_state = _projection_state(
            self.source_order_status,
            self.cumulative_filled_quantity,
        )
        if self.state != expected_state:
            raise ValueError("projection state does not match source order status")
        expected_payload_hash = _digest(
            "v3.execution-projection.payload.v1",
            _projection_payload(
                projection_id=self.projection_id,
                execution_plan_id=self.execution_plan_id,
                source_binding_id=self.source_binding_id,
                source_binding_hash=self.source_binding_hash,
                binding_bound_at=self.binding_bound_at,
                source_intent_id=self.source_intent_id,
                source_order_id=self.source_order_id,
                source_order_created_at=self.source_order_created_at,
                source_event_id=self.source_event_id,
                source_sequence=self.source_sequence,
                source_result_idempotency_key=(
                    self.source_result_idempotency_key
                ),
                source_result_fingerprint=self.source_result_fingerprint,
                source_transition_id=self.source_transition_id,
                source_transition_payload_hash=(
                    self.source_transition_payload_hash
                ),
                source_order_state_hash=self.source_order_state_hash,
                source_order_status=self.source_order_status,
                cumulative_filled_quantity=self.cumulative_filled_quantity,
                state=self.state,
                occurred_at=self.occurred_at,
            ),
        )
        if self.payload_hash != expected_payload_hash:
            raise ValueError("payload_hash does not match projection fields")


def validate_v3_execution_projection(
    projection: V3ExecutionProjection,
) -> bool:
    """Recompute a projection after deserialization or frozen-object bypasses."""

    if type(projection) is not V3ExecutionProjection:
        raise TypeError("projection must be exactly V3ExecutionProjection")
    try:
        expected = V3ExecutionProjection(
            projection_id=projection.projection_id,
            payload_hash=projection.payload_hash,
            execution_plan_id=projection.execution_plan_id,
            source_binding_id=projection.source_binding_id,
            source_binding_hash=projection.source_binding_hash,
            binding_bound_at=projection.binding_bound_at,
            source_intent_id=projection.source_intent_id,
            source_order_id=projection.source_order_id,
            source_order_created_at=projection.source_order_created_at,
            source_event_id=projection.source_event_id,
            source_sequence=projection.source_sequence,
            source_result_idempotency_key=(
                projection.source_result_idempotency_key
            ),
            source_result_fingerprint=projection.source_result_fingerprint,
            source_transition_id=projection.source_transition_id,
            source_transition_payload_hash=(
                projection.source_transition_payload_hash
            ),
            source_order_state_hash=projection.source_order_state_hash,
            source_order_status=projection.source_order_status,
            cumulative_filled_quantity=(
                projection.cumulative_filled_quantity
            ),
            state=projection.state,
            occurred_at=projection.occurred_at,
        )
        return projection == expected
    except (AttributeError, TypeError, ValueError):
        return False


def project_execution_result(
    *,
    binding: V3ExecutionPlanBinding,
    transition: OrderTransitionReceipt,
) -> V3ExecutionProjection:
    """Create one projection from a freshly verified neutral OMS transition."""

    if type(transition) is not OrderTransitionReceipt:
        raise TypeError("transition must be exactly OrderTransitionReceipt")
    if not validate_order_transition_receipt(transition):
        raise ValueError("transition receipt is invalid")
    if type(binding) is not V3ExecutionPlanBinding:
        raise TypeError("binding must be exactly V3ExecutionPlanBinding")
    if not validate_v3_execution_plan_binding(binding):
        raise ValueError("execution plan binding is invalid")
    result = transition.result
    order_state = transition.current_state
    if binding.source_intent_id != result.intent_id:
        raise ValueError("execution plan binding intent_id differs from result")
    if binding.source_order_id != result.order_id:
        raise ValueError("execution plan binding order_id differs from result")
    if binding.bound_at != transition.previous_state.created_at:
        raise ValueError(
            "execution plan binding must equal canonical order creation time"
        )
    if order_state.status != result.status:
        raise ValueError("transition state and result status differ")
    if order_state.last_source_sequence != result.source_sequence:
        raise ValueError("projection requires the immediate post-event OMS state")

    cumulative = _quantity(order_state.cumulative_filled_quantity)
    if result.status in {
        OrderStatus.ACCEPTED,
        OrderStatus.QUEUED,
        OrderStatus.REJECTED,
    } and cumulative:
        raise ValueError(f"{result.status.value} cannot carry a cumulative fill")
    state = _projection_state(result.status, cumulative)
    projection_id = _projection_identity(
        order_id=result.order_id,
        event_id=result.event_id,
    )
    occurred_at = _aware_utc(result.occurred_at, "occurred_at")
    payload = _projection_payload(
        projection_id=projection_id,
        execution_plan_id=binding.execution_plan_id,
        source_binding_id=binding.binding_id,
        source_binding_hash=binding.binding_hash,
        binding_bound_at=binding.bound_at,
        source_intent_id=result.intent_id,
        source_order_id=result.order_id,
        source_order_created_at=order_state.created_at,
        source_event_id=result.event_id,
        source_sequence=result.source_sequence,
        source_result_idempotency_key=transition.source_idempotency_key,
        source_result_fingerprint=transition.result_fingerprint,
        source_transition_id=transition.transition_id,
        source_transition_payload_hash=transition.payload_hash,
        source_order_state_hash=transition.current_state_hash,
        source_order_status=order_state.status,
        cumulative_filled_quantity=cumulative,
        state=state,
        occurred_at=occurred_at,
    )
    return V3ExecutionProjection(
        projection_id=projection_id,
        payload_hash=_digest("v3.execution-projection.payload.v1", payload),
        execution_plan_id=binding.execution_plan_id,
        source_binding_id=binding.binding_id,
        source_binding_hash=binding.binding_hash,
        binding_bound_at=binding.bound_at,
        source_intent_id=result.intent_id,
        source_order_id=result.order_id,
        source_order_created_at=order_state.created_at,
        source_event_id=result.event_id,
        source_sequence=result.source_sequence,
        source_result_idempotency_key=transition.source_idempotency_key,
        source_result_fingerprint=transition.result_fingerprint,
        source_transition_id=transition.transition_id,
        source_transition_payload_hash=transition.payload_hash,
        source_order_state_hash=transition.current_state_hash,
        source_order_status=order_state.status,
        cumulative_filled_quantity=cumulative,
        state=state,
        occurred_at=occurred_at,
    )
