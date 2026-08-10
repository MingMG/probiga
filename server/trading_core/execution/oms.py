"""Retry-safe, immutable OMS state machine."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
import hashlib
import json
from typing import Any

from ..contracts import (
    ExecutionEventKind,
    ExecutionIntent,
    ExecutionResult,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from ..contracts.idempotency import (
    execution_result_fingerprint,
    execution_result_idempotency_key,
    validate_intent_idempotency_key,
)


ACTIVE_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.CREATED: frozenset(
        {
            OrderStatus.ACCEPTED,
            OrderStatus.REJECTED,
            OrderStatus.CANCELLED,
            OrderStatus.EXPIRED,
        }
    ),
    OrderStatus.ACCEPTED: frozenset(
        {
            OrderStatus.QUEUED,
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
            OrderStatus.CANCEL_PENDING,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        }
    ),
    OrderStatus.QUEUED: frozenset(
        {
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
            OrderStatus.CANCEL_PENDING,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        }
    ),
    OrderStatus.PARTIALLY_FILLED: frozenset(
        {
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
            OrderStatus.CANCEL_PENDING,
            OrderStatus.CANCELLED,
            OrderStatus.EXPIRED,
        }
    ),
    OrderStatus.CANCEL_PENDING: frozenset(
        {
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.EXPIRED,
        }
    ),
    OrderStatus.FILLED: frozenset(),
    OrderStatus.CANCELLED: frozenset(),
    OrderStatus.REJECTED: frozenset(),
    OrderStatus.EXPIRED: frozenset(),
}

TERMINAL_STATUSES = frozenset(
    {
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
    }
)

# These events assert that an order was submitted or executable, or that an
# execution occurred.  Their venue occurrence time must be inside the intent's
# execution window.  Terminal acknowledgements are intentionally absent: a
# venue may report cancellation or rejection after the business validity
# window has elapsed, and the OMS must still be able to converge to terminal
# state.
EXECUTION_WINDOW_STATUSES = frozenset(
    {
        OrderStatus.ACCEPTED,
        OrderStatus.QUEUED,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
    }
)


def _validated_result_copy(result: ExecutionResult) -> ExecutionResult:
    """Re-run exact event invariants before hashing, replaying, or applying."""

    if type(result) is not ExecutionResult:
        raise TypeError("result must be exactly ExecutionResult")
    if type(result.status) is not OrderStatus:
        raise ValueError("execution result status is not canonical")
    if type(result.event_kind) is not ExecutionEventKind:
        raise ValueError("execution result event_kind is not canonical")
    try:
        validated = ExecutionResult(
            intent_id=result.intent_id,
            order_id=result.order_id,
            event_id=result.event_id,
            status=result.status,
            occurred_at=result.occurred_at,
            received_at=result.received_at,
            source_sequence=result.source_sequence,
            idempotency_key=result.idempotency_key,
            last_fill_quantity=result.last_fill_quantity,
            last_fill_price=result.last_fill_price,
            reason_code=result.reason_code,
            event_kind=result.event_kind,
        )
    except AttributeError as exc:
        raise ValueError("execution result is incomplete") from exc
    if validated != result:
        raise ValueError("execution result is not in canonical validated form")
    return validated


@dataclass(frozen=True)
class OrderState:
    order_id: str
    intent_id: str
    status: OrderStatus
    requested_quantity: int
    cumulative_filled_quantity: int
    average_fill_price: Decimal | None
    created_at: datetime
    updated_at: datetime
    earliest_at: datetime
    expires_at: datetime
    order_type: OrderType
    time_in_force: TimeInForce
    last_source_sequence: int
    version: int
    waiting_reason: str = ""
    applied_results: tuple[tuple[str, str], ...] = ()
    execution_history: tuple[ExecutionResult, ...] = ()

    def __post_init__(self) -> None:
        for field_name in ("order_id", "intent_id"):
            value = getattr(self, field_name)
            if not isinstance(value, str):
                raise TypeError(f"{field_name} must be a string")
            normalized = value.strip()
            if not normalized:
                raise ValueError(f"{field_name} is required")
            object.__setattr__(self, field_name, normalized)
        object.__setattr__(self, "status", OrderStatus(self.status))
        if not isinstance(self.waiting_reason, str):
            raise TypeError("waiting_reason must be a string")
        normalized_waiting_reason = self.waiting_reason.strip()
        if normalized_waiting_reason and self.status not in {
            OrderStatus.QUEUED,
            OrderStatus.PARTIALLY_FILLED,
        }:
            raise ValueError(
                "waiting_reason requires QUEUED or PARTIALLY_FILLED state"
            )
        object.__setattr__(self, "waiting_reason", normalized_waiting_reason)
        object.__setattr__(self, "order_type", OrderType(self.order_type))
        object.__setattr__(
            self,
            "time_in_force",
            TimeInForce(self.time_in_force),
        )
        for value, field_name, minimum in (
            (self.requested_quantity, "requested_quantity", 1),
            (
                self.cumulative_filled_quantity,
                "cumulative_filled_quantity",
                0,
            ),
            (self.last_source_sequence, "last_source_sequence", 0),
            (self.version, "version", 1),
        ):
            if not isinstance(value, int) or isinstance(value, bool):
                raise TypeError(f"{field_name} must be an integer")
            if value < minimum:
                raise ValueError(f"{field_name} must be at least {minimum}")
        if not 0 <= self.cumulative_filled_quantity <= self.requested_quantity:
            raise ValueError("cumulative fill must be within requested quantity")
        if self.average_fill_price is not None:
            if (
                isinstance(self.average_fill_price, Decimal)
                and type(self.average_fill_price) is not Decimal
            ):
                raise TypeError(
                    "average_fill_price must not be a Decimal subclass"
                )
            converted = (
                self.average_fill_price
                if type(self.average_fill_price) is Decimal
                else Decimal(str(self.average_fill_price))
            )
            if not converted.is_finite():
                raise ValueError("average_fill_price must be finite")
            object.__setattr__(self, "average_fill_price", converted)
        if self.cumulative_filled_quantity == 0:
            if self.average_fill_price is not None:
                raise ValueError("average fill price requires a cumulative fill")
        elif self.average_fill_price is None or self.average_fill_price <= 0:
            raise ValueError("positive average fill price required after a fill")
        if self.status == OrderStatus.FILLED and (
            self.cumulative_filled_quantity != self.requested_quantity
        ):
            raise ValueError("FILLED state requires the requested quantity")
        if self.status == OrderStatus.PARTIALLY_FILLED and not (
            0 < self.cumulative_filled_quantity < self.requested_quantity
        ):
            raise ValueError("PARTIALLY_FILLED state requires an incomplete fill")
        for value, field_name in (
            (self.created_at, "created_at"),
            (self.updated_at, "updated_at"),
            (self.earliest_at, "earliest_at"),
            (self.expires_at, "expires_at"),
        ):
            if type(value) is not datetime:
                raise TypeError(f"{field_name} must be exactly datetime")
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{field_name} must be timezone-aware")
        if self.updated_at < self.created_at:
            raise ValueError("updated_at cannot precede created_at")
        if self.created_at > self.earliest_at:
            raise ValueError("created_at cannot follow earliest_at")
        if self.earliest_at >= self.expires_at:
            raise ValueError("earliest_at must precede expires_at")
        if type(self.applied_results) is not tuple:
            raise TypeError("applied_results must be a tuple")
        normalized_results: list[tuple[str, str]] = []
        for item in self.applied_results:
            if type(item) is not tuple or len(item) != 2:
                raise TypeError(
                    "applied_results entries must be exact (key, fingerprint) tuples"
                )
            key, fingerprint = item
            for value, field_name in (
                (key, "applied result key"),
                (fingerprint, "applied result fingerprint"),
            ):
                if not isinstance(value, str):
                    raise TypeError(f"{field_name} must be a string")
                if len(value) != 64 or any(
                    character not in "0123456789abcdef"
                    for character in value.lower()
                ):
                    raise ValueError(f"{field_name} must be a SHA-256 digest")
            normalized_results.append((key.lower(), fingerprint.lower()))
        keys = [key for key, _ in normalized_results]
        if len(keys) != len(set(keys)):
            raise ValueError("applied result idempotency keys must be unique")
        object.__setattr__(
            self,
            "applied_results",
            tuple(sorted(normalized_results)),
        )
        if type(self.execution_history) is not tuple:
            raise TypeError("execution_history must be a tuple")
        if any(type(item) is not ExecutionResult for item in self.execution_history):
            raise TypeError(
                "execution_history must contain exact ExecutionResult values"
            )
        if len(normalized_results) != self.last_source_sequence:
            raise ValueError(
                "applied result count must equal last_source_sequence"
            )
        if self.version != self.last_source_sequence + 1:
            raise ValueError("version must equal last_source_sequence plus one")
        if self.status == OrderStatus.CREATED and self.last_source_sequence != 0:
            raise ValueError("CREATED state cannot have execution results")
        if self.status != OrderStatus.CREATED and self.last_source_sequence == 0:
            raise ValueError("non-CREATED state requires an execution result")
        if self.status in {
            OrderStatus.CREATED,
            OrderStatus.ACCEPTED,
            OrderStatus.QUEUED,
            OrderStatus.REJECTED,
        } and self.cumulative_filled_quantity:
            raise ValueError(f"{self.status.value} state cannot carry a fill")
        if len(self.execution_history) != self.last_source_sequence:
            raise ValueError(
                "execution history count must equal last_source_sequence"
            )
        replay_status = OrderStatus.CREATED
        replay_cumulative = 0
        replay_value = Decimal("0")
        replay_updated_at = self.created_at
        replay_waiting_reason = ""
        replay_results: dict[str, str] = {}
        for expected_sequence, result in enumerate(
            self.execution_history,
            start=1,
        ):
            result = _validated_result_copy(result)
            if result.order_id != self.order_id or result.intent_id != self.intent_id:
                raise ValueError("execution history belongs to a different order")
            if result.source_sequence != expected_sequence:
                raise ValueError("execution history sequence must be contiguous")
            expected_key = execution_result_idempotency_key(
                order_id=result.order_id,
                event_id=result.event_id,
            )
            if result.idempotency_key != expected_key:
                raise ValueError("execution history idempotency key is invalid")
            if result.event_kind is ExecutionEventKind.WAITING_REASON_CHANGED:
                if result.status is not replay_status:
                    raise ValueError(
                        "waiting-reason history cannot change order status"
                    )
                if result.reason_code == replay_waiting_reason:
                    raise ValueError(
                        "waiting-reason history must change the reason"
                    )
                replay_waiting_reason = result.reason_code
            else:
                if result.status not in ACTIVE_TRANSITIONS[replay_status]:
                    raise ValueError(
                        "execution history contains an illegal transition"
                    )
                replay_waiting_reason = ""
            if result.status == OrderStatus.EXPIRED:
                if result.occurred_at < self.expires_at:
                    raise ValueError("execution history expires before expires_at")
            elif result.status in EXECUTION_WINDOW_STATUSES and (
                result.occurred_at < self.earliest_at
                or result.occurred_at >= self.expires_at
            ):
                raise ValueError("execution history is outside the execution window")
            if result.occurred_at < replay_updated_at:
                raise ValueError("execution history time moves backwards")
            replay_cumulative += result.last_fill_quantity
            if replay_cumulative > self.requested_quantity:
                raise ValueError("execution history overfills the order")
            if result.status == OrderStatus.PARTIALLY_FILLED and not (
                0 < replay_cumulative < self.requested_quantity
            ):
                raise ValueError("execution history has an invalid partial fill")
            if (
                result.status == OrderStatus.FILLED
                and replay_cumulative != self.requested_quantity
            ):
                raise ValueError("execution history has an invalid full fill")
            if result.last_fill_quantity:
                assert result.last_fill_price is not None
                replay_value += (
                    result.last_fill_price * result.last_fill_quantity
                )
            replay_status = result.status
            replay_updated_at = result.occurred_at
            replay_results[result.idempotency_key] = (
                execution_result_fingerprint(result)
            )
        replay_average = (
            replay_value / replay_cumulative if replay_cumulative else None
        )
        if (
            replay_status != self.status
            or replay_cumulative != self.cumulative_filled_quantity
            or replay_average != self.average_fill_price
            or replay_waiting_reason != self.waiting_reason
            or replay_updated_at != self.updated_at
            or tuple(sorted(replay_results.items())) != self.applied_results
        ):
            raise ValueError("order state differs from execution history replay")

    @property
    def remaining_quantity(self) -> int:
        return self.requested_quantity - self.cumulative_filled_quantity

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES


def _canonical(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
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
    raise TypeError(f"unsupported OMS fingerprint value: {type(value).__name__}")


def _digest(namespace: str, payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        {"namespace": namespace, "payload": _canonical(payload)},
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def order_state_fingerprint(state: OrderState) -> str:
    if type(state) is not OrderState:
        raise TypeError("state must be exactly OrderState")
    return _digest(
        "trading-core.order-state.v2",
        {
            "order_id": state.order_id,
            "intent_id": state.intent_id,
            "status": state.status,
            "requested_quantity": state.requested_quantity,
            "cumulative_filled_quantity": state.cumulative_filled_quantity,
            "average_fill_price": state.average_fill_price,
            "created_at": state.created_at,
            "updated_at": state.updated_at,
            "earliest_at": state.earliest_at,
            "expires_at": state.expires_at,
            "order_type": state.order_type,
            "time_in_force": state.time_in_force,
            "last_source_sequence": state.last_source_sequence,
            "version": state.version,
            "waiting_reason": state.waiting_reason,
            "applied_results": state.applied_results,
            "execution_history": tuple(
                execution_result_fingerprint(item)
                for item in state.execution_history
            ),
        },
    )


def order_transition_id(*, order_id: str, event_id: str) -> str:
    if not isinstance(order_id, str) or not order_id.strip():
        raise ValueError("order_id is required")
    if not isinstance(event_id, str) or not event_id.strip():
        raise ValueError("event_id is required")
    return _digest(
        "trading-core.order-transition.identity.v1",
        {"order_id": order_id.strip(), "event_id": event_id.strip()},
    )


@dataclass(frozen=True, slots=True, init=False)
class OrderTransitionReceipt:
    """Internally verifiable record of one neutral OMS state transition.

    Receipts are created only by :func:`apply_execution_result_with_receipt`.
    An idempotent replay reuses the originally persisted receipt/outbox event;
    it cannot mint a new receipt from an already-mutated after-state.

    The hashes detect inconsistent or mutated payloads.  They do not, by
    themselves, prove that the payload came from a canonical OMS repository;
    that authority check belongs at the persistence/outbox boundary.
    """

    previous_state: OrderState
    result: ExecutionResult
    current_state: OrderState
    source_event_id: str
    source_idempotency_key: str
    result_fingerprint: str
    previous_state_hash: str
    current_state_hash: str
    transition_id: str
    payload_hash: str

    @classmethod
    def _verified(
        cls,
        *,
        previous_state: OrderState,
        result: ExecutionResult,
        current_state: OrderState,
    ) -> "OrderTransitionReceipt":
        instance = object.__new__(cls)
        result_fingerprint = execution_result_fingerprint(result)
        previous_hash = order_state_fingerprint(previous_state)
        current_hash = order_state_fingerprint(current_state)
        transition_id = order_transition_id(
            order_id=result.order_id,
            event_id=result.event_id,
        )
        payload_hash = _digest(
            "trading-core.order-transition.payload.v2",
            {
                "transition_id": transition_id,
                "source_idempotency_key": result.idempotency_key,
                "result_fingerprint": result_fingerprint,
                "previous_state_hash": previous_hash,
                "current_state_hash": current_hash,
                "event_kind": result.event_kind,
                "previous_waiting_reason": previous_state.waiting_reason,
                "current_waiting_reason": current_state.waiting_reason,
            },
        )
        for field_name, value in (
            ("previous_state", previous_state),
            ("result", result),
            ("current_state", current_state),
            ("source_event_id", result.event_id),
            ("source_idempotency_key", result.idempotency_key),
            ("result_fingerprint", result_fingerprint),
            ("previous_state_hash", previous_hash),
            ("current_state_hash", current_hash),
            ("transition_id", transition_id),
            ("payload_hash", payload_hash),
        ):
            object.__setattr__(instance, field_name, value)
        return instance


def can_transition(previous: OrderStatus, next_status: OrderStatus) -> bool:
    return OrderStatus(next_status) in ACTIVE_TRANSITIONS[OrderStatus(previous)]


def transition_order(previous: OrderStatus, next_status: OrderStatus) -> OrderStatus:
    previous = OrderStatus(previous)
    next_status = OrderStatus(next_status)
    if not can_transition(previous, next_status):
        raise ValueError(f"illegal order transition: {previous} -> {next_status}")
    return next_status


def new_order_state(*, order_id: str, intent: ExecutionIntent) -> OrderState:
    if not isinstance(order_id, str):
        raise TypeError("order_id must be a string")
    if not order_id.strip():
        raise ValueError("order_id is required")
    if not validate_intent_idempotency_key(intent):
        raise ValueError("intent idempotency key does not match intent semantics")
    if (
        intent.order_type != OrderType.LIMIT
        or intent.time_in_force != TimeInForce.DAY
    ):
        raise ValueError(
            "the initial OMS release supports LIMIT + DAY intents only"
        )
    return OrderState(
        order_id=order_id,
        intent_id=intent.intent_id,
        status=OrderStatus.CREATED,
        requested_quantity=intent.quantity,
        cumulative_filled_quantity=0,
        average_fill_price=None,
        created_at=intent.created_at,
        updated_at=intent.created_at,
        earliest_at=intent.earliest_at,
        expires_at=intent.expires_at,
        order_type=intent.order_type,
        time_in_force=intent.time_in_force,
        last_source_sequence=0,
        version=1,
    )


def _validated_state_copy(state: OrderState) -> OrderState:
    """Re-run every invariant before trusting a state supplied to the OMS.

    Frozen dataclasses prevent normal assignment, but Python callers can still
    manufacture an exact instance with ``object.__new__``.  Reconstructing the
    value closes that boundary and also guarantees that the complete execution
    history, derived totals and idempotency map still agree.
    """

    if type(state) is not OrderState:
        raise TypeError("state must be exactly OrderState")
    try:
        validated = OrderState(
            order_id=state.order_id,
            intent_id=state.intent_id,
            status=state.status,
            requested_quantity=state.requested_quantity,
            cumulative_filled_quantity=state.cumulative_filled_quantity,
            average_fill_price=state.average_fill_price,
            created_at=state.created_at,
            updated_at=state.updated_at,
            earliest_at=state.earliest_at,
            expires_at=state.expires_at,
            order_type=state.order_type,
            time_in_force=state.time_in_force,
            last_source_sequence=state.last_source_sequence,
            version=state.version,
            waiting_reason=state.waiting_reason,
            applied_results=state.applied_results,
            execution_history=state.execution_history,
        )
    except AttributeError as exc:
        raise ValueError("order state is incomplete") from exc
    if validated != state:
        raise ValueError("order state is not in canonical validated form")
    return validated


def apply_execution_result(
    state: OrderState,
    result: ExecutionResult,
) -> OrderState:
    """Apply one event, making identical retries a no-op and conflicts fatal."""

    _validated_state_copy(state)
    result = _validated_result_copy(result)
    if result.order_id != state.order_id or result.intent_id != state.intent_id:
        raise ValueError("execution result does not belong to order state")
    expected_key = execution_result_idempotency_key(
        order_id=result.order_id,
        event_id=result.event_id,
    )
    if result.idempotency_key != expected_key:
        raise ValueError("execution result idempotency key is invalid")
    fingerprint = execution_result_fingerprint(result)
    applied = dict(state.applied_results)
    prior_fingerprint = applied.get(result.idempotency_key)
    if prior_fingerprint is not None:
        if prior_fingerprint != fingerprint:
            raise ValueError("idempotency key reused with different result payload")
        return state
    expected_source_sequence = state.last_source_sequence + 1
    if result.source_sequence != expected_source_sequence:
        raise ValueError(
            "execution result source_sequence must be contiguous per order; "
            f"expected {expected_source_sequence}, got {result.source_sequence}"
        )
    if result.status == OrderStatus.EXPIRED:
        if result.occurred_at < state.expires_at:
            raise ValueError("EXPIRED result cannot precede expires_at")
    elif result.status in EXECUTION_WINDOW_STATUSES and (
        result.occurred_at < state.earliest_at
        or result.occurred_at >= state.expires_at
    ):
        raise ValueError("execution result is outside the execution window")
    if result.occurred_at < state.updated_at:
        raise ValueError("execution result cannot precede current order state")
    if result.event_kind is ExecutionEventKind.WAITING_REASON_CHANGED:
        if result.status is not state.status:
            raise ValueError("waiting-reason event cannot change order status")
        if state.status not in {
            OrderStatus.QUEUED,
            OrderStatus.PARTIALLY_FILLED,
        }:
            raise ValueError(
                "waiting-reason event requires QUEUED or PARTIALLY_FILLED state"
            )
        if result.reason_code == state.waiting_reason:
            raise ValueError("waiting-reason event must change the reason")
        next_waiting_reason = result.reason_code
    else:
        transition_order(state.status, result.status)
        next_waiting_reason = ""

    cumulative = state.cumulative_filled_quantity + result.last_fill_quantity
    if cumulative > state.requested_quantity:
        raise ValueError("cumulative fill exceeds requested quantity")
    if result.status == OrderStatus.PARTIALLY_FILLED and not (
        0 < cumulative < state.requested_quantity
    ):
        raise ValueError("partial-fill state requires an incomplete positive fill")
    if result.status == OrderStatus.FILLED and cumulative != state.requested_quantity:
        raise ValueError("filled state requires full requested quantity")

    average = state.average_fill_price
    if result.last_fill_quantity:
        assert result.last_fill_price is not None
        previous_value = (
            (average or Decimal("0")) * state.cumulative_filled_quantity
        )
        average = (
            previous_value + result.last_fill_price * result.last_fill_quantity
        ) / cumulative

    applied[result.idempotency_key] = fingerprint
    return replace(
        state,
        status=result.status,
        cumulative_filled_quantity=cumulative,
        average_fill_price=average,
        updated_at=result.occurred_at,
        last_source_sequence=result.source_sequence,
        version=state.version + 1,
        waiting_reason=next_waiting_reason,
        applied_results=tuple(sorted(applied.items())),
        execution_history=(*state.execution_history, result),
    )


def apply_execution_result_with_receipt(
    state: OrderState,
    result: ExecutionResult,
) -> OrderTransitionReceipt:
    """Apply one previously unseen event and return its verified transition.

    A replay is deliberately rejected here.  Consumers should persist and
    retry the original receipt/outbox payload, which prevents an event's
    already-mutated after-state from being used as its own provenance.
    """

    if type(state) is not OrderState:
        raise TypeError("state must be exactly OrderState")
    result = _validated_result_copy(result)
    if result.idempotency_key in dict(state.applied_results):
        raise ValueError(
            "cannot mint a transition receipt for an already-applied result"
        )
    current_state = apply_execution_result(state, result)
    return OrderTransitionReceipt._verified(
        previous_state=state,
        result=result,
        current_state=current_state,
    )


def validate_order_transition_receipt(
    receipt: OrderTransitionReceipt,
) -> bool:
    """Recompute every receipt field and the underlying OMS transition."""

    if type(receipt) is not OrderTransitionReceipt:
        raise TypeError("receipt must be exactly OrderTransitionReceipt")
    try:
        if type(receipt.previous_state) is not OrderState:
            return False
        if type(receipt.result) is not ExecutionResult:
            return False
        if type(receipt.current_state) is not OrderState:
            return False
        if receipt.result.idempotency_key in dict(
            receipt.previous_state.applied_results
        ):
            return False
        expected_state = apply_execution_result(
            receipt.previous_state,
            receipt.result,
        )
        if expected_state != receipt.current_state:
            return False
        expected = OrderTransitionReceipt._verified(
            previous_state=receipt.previous_state,
            result=receipt.result,
            current_state=receipt.current_state,
        )
        return receipt == expected
    except (AttributeError, TypeError, ValueError):
        return False
