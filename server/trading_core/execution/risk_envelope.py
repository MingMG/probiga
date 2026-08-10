"""Pure absolute execution-risk limits with no strategy or market opinions.

The envelope consumes only a caller-supplied mechanical account snapshot and
one candidate order.  It owns no clock, database, broker, quote, ranking,
sector, theme, return, trend, or regime dependency.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP, localcontext
from enum import Enum
import hashlib
import json
from typing import Any

from ..contracts import OrderSide


_MAX_DECIMAL_DIGITS = 64
_MIN_DECIMAL_EXPONENT = -18
_MAX_DECIMAL_EXPONENT = 18
_MAX_INTEGER = 9_223_372_036_854_775_807


class RiskAccountStatus(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    SUSPENDED = "SUSPENDED"
    CLOSED = "CLOSED"


class RiskReconciliationStatus(str, Enum):
    PASS = "PASS"
    BLOCKED = "BLOCKED"
    UNKNOWN = "UNKNOWN"


class RiskNotionalRounding(str, Enum):
    DOWN = "ROUND_DOWN"
    HALF_UP = "ROUND_HALF_UP"


class RiskEnvelopeDecisionStatus(str, Enum):
    APPROVED = "APPROVED"
    BLOCKED = "BLOCKED"


class RiskEnvelopeReason(str, Enum):
    ACCOUNT_ID_MISMATCH = "ACCOUNT_ID_MISMATCH"
    ACCOUNT_NOT_ACTIVE = "ACCOUNT_NOT_ACTIVE"
    RECONCILIATION_NOT_PASS = "RECONCILIATION_NOT_PASS"
    SNAPSHOT_FROM_FUTURE = "SNAPSHOT_FROM_FUTURE"
    SNAPSHOT_STALE = "SNAPSHOT_STALE"
    ORDER_QUANTITY_LIMIT_EXCEEDED = "ORDER_QUANTITY_LIMIT_EXCEEDED"
    ORDER_NOTIONAL_LIMIT_EXCEEDED = "ORDER_NOTIONAL_LIMIT_EXCEEDED"
    INSTRUMENT_POSITION_LIMIT_EXCEEDED = (
        "INSTRUMENT_POSITION_LIMIT_EXCEEDED"
    )
    TOTAL_POSITION_NOTIONAL_LIMIT_EXCEEDED = (
        "TOTAL_POSITION_NOTIONAL_LIMIT_EXCEEDED"
    )
    TOTAL_PENDING_ORDER_NOTIONAL_LIMIT_EXCEEDED = (
        "TOTAL_PENDING_ORDER_NOTIONAL_LIMIT_EXCEEDED"
    )
    SELL_QUANTITY_EXCEEDS_POSITION = "SELL_QUANTITY_EXCEEDS_POSITION"
    INSUFFICIENT_AVAILABLE_CASH = "INSUFFICIENT_AVAILABLE_CASH"
    ABSOLUTE_CONCENTRATION_LIMIT_EXCEEDED = (
        "ABSOLUTE_CONCENTRATION_LIMIT_EXCEEDED"
    )


_REASON_ORDER = {
    reason: index for index, reason in enumerate(RiskEnvelopeReason)
}


def _text(value: object, field_name: str) -> str:
    if type(value) is not str:
        raise TypeError(f"{field_name} must be exactly str")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _integer(
    value: object,
    field_name: str,
    *,
    minimum: int = 0,
) -> int:
    if type(value) is not int:
        raise TypeError(f"{field_name} must be exactly int")
    if value < minimum or value > _MAX_INTEGER:
        raise ValueError(
            f"{field_name} must be between {minimum} and {_MAX_INTEGER}"
        )
    return value


def _decimal(
    value: object,
    field_name: str,
    *,
    positive: bool = False,
    non_negative: bool = False,
) -> Decimal:
    if type(value) is not Decimal:
        raise TypeError(f"{field_name} must be exactly Decimal")
    if not value.is_finite():
        raise ValueError(f"{field_name} must be finite")
    _, digits, exponent = value.as_tuple()
    if (
        len(digits) > _MAX_DECIMAL_DIGITS
        or exponent < _MIN_DECIMAL_EXPONENT
        or exponent > _MAX_DECIMAL_EXPONENT
    ):
        raise ValueError(f"{field_name} exceeds supported Decimal bounds")
    if positive and value <= 0:
        raise ValueError(f"{field_name} must be positive")
    if non_negative and value < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return value


def _aware(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def _duration(value: object, field_name: str) -> timedelta:
    if type(value) is not timedelta:
        raise TypeError(f"{field_name} must be exactly timedelta")
    if value < timedelta(0):
        raise ValueError(f"{field_name} must be non-negative")
    return value


def _sha256(value: object, field_name: str) -> str:
    normalized = _text(value, field_name).lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{field_name} must be a SHA-256 digest")
    return normalized


def _require_reconstructable(
    value: Any,
    expected_type: type,
    field_name: str,
) -> Any:
    """Rerun invariants and derived hashes after a frozen-object boundary."""

    if type(value) is not expected_type:
        raise TypeError(
            f"{field_name} must be exactly {expected_type.__name__}"
        )
    try:
        reconstructed = replace(value)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} cannot be reconstructed") from exc
    if reconstructed != value:
        raise ValueError(
            f"{field_name} differs from its canonical reconstructed value"
        )
    return value


def _canonical(value: Any) -> Any:
    if type(value) is datetime:
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if type(value) is date:
        return value.isoformat()
    if type(value) is timedelta:
        return (
            value.days * 86_400_000_000
            + value.seconds * 1_000_000
            + value.microseconds
        )
    if type(value) is Decimal:
        sign, digits, exponent = value.as_tuple()
        if not any(digits):
            return {"sign": 0, "digits": "0", "exponent": 0}
        while digits and digits[-1] == 0:
            digits = digits[:-1]
            exponent += 1
        return {
            "sign": sign,
            "digits": "".join(str(digit) for digit in digits),
            "exponent": exponent,
        }
    if isinstance(value, Enum):
        return value.value
    if type(value) in {list, tuple}:
        return [_canonical(item) for item in value]
    if type(value) is dict:
        return {
            str(key): _canonical(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if value is None or type(value) in {str, int, bool}:
        return value
    raise TypeError(f"unsupported risk hash value: {type(value).__name__}")


def _digest(namespace: str, payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        {"namespace": namespace, "payload": _canonical(payload)},
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _sum_decimals(values: tuple[Decimal, ...]) -> Decimal:
    if not values:
        return Decimal("0")
    precision = max(
        128,
        max(len(item.as_tuple().digits) for item in values)
        + len(str(len(values)))
        + 4,
    )
    with localcontext() as context:
        context.prec = precision
        return sum(values, Decimal("0"))


def _add(left: Decimal, right: Decimal) -> Decimal:
    with localcontext() as context:
        context.prec = 128
        return left + right


def _subtract_floor_zero(left: Decimal, right: Decimal) -> Decimal:
    with localcontext() as context:
        context.prec = 128
        return max(Decimal("0"), left - right)


def _multiply(left: Decimal, right: Decimal | int) -> Decimal:
    with localcontext() as context:
        context.prec = 128
        return left * right


def _remaining_position_notional(
    *,
    current_notional: Decimal,
    current_quantity: int,
    remaining_quantity: int,
    quantum: Decimal,
) -> Decimal:
    """Value a partial SELL at the snapshot mark, rounded conservatively up."""

    if current_quantity == 0 or remaining_quantity == 0:
        return Decimal("0")
    with localcontext() as context:
        context.prec = 128
        return (
            current_notional * remaining_quantity / current_quantity
        ).quantize(quantum, rounding=ROUND_UP)


@dataclass(frozen=True, slots=True)
class ExecutionRiskPosition:
    instrument_id: str
    quantity: int
    notional: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "instrument_id",
            _text(self.instrument_id, "instrument_id"),
        )
        quantity = _integer(self.quantity, "quantity")
        notional = _decimal(
            self.notional,
            "notional",
            non_negative=True,
        )
        if (quantity == 0) != (notional == 0):
            raise ValueError(
                "position quantity and notional must both be zero or positive"
            )


@dataclass(frozen=True, slots=True)
class ExecutionRiskAccountSnapshot:
    snapshot_id: str
    account_id: str
    account_status: RiskAccountStatus
    reconciliation_status: RiskReconciliationStatus
    observed_at: datetime
    account_equity: Decimal
    available_cash: Decimal
    total_pending_order_notional: Decimal
    positions: tuple[ExecutionRiskPosition, ...] = ()
    snapshot_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for field_name in ("snapshot_id", "account_id"):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        if type(self.account_status) is not RiskAccountStatus:
            raise TypeError("account_status must be exactly RiskAccountStatus")
        if type(self.reconciliation_status) is not RiskReconciliationStatus:
            raise TypeError(
                "reconciliation_status must be exactly RiskReconciliationStatus"
            )
        _aware(self.observed_at, "observed_at")
        object.__setattr__(
            self,
            "account_equity",
            _decimal(self.account_equity, "account_equity", positive=True),
        )
        object.__setattr__(
            self,
            "available_cash",
            _decimal(self.available_cash, "available_cash", non_negative=True),
        )
        object.__setattr__(
            self,
            "total_pending_order_notional",
            _decimal(
                self.total_pending_order_notional,
                "total_pending_order_notional",
                non_negative=True,
            ),
        )
        if type(self.positions) is not tuple:
            raise TypeError("positions must be exactly tuple")
        if any(type(item) is not ExecutionRiskPosition for item in self.positions):
            raise TypeError(
                "positions must contain exact ExecutionRiskPosition values"
            )
        for item in self.positions:
            _require_reconstructable(
                item,
                ExecutionRiskPosition,
                "position",
            )
        positions = tuple(
            sorted(self.positions, key=lambda item: item.instrument_id)
        )
        identifiers = tuple(item.instrument_id for item in positions)
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("positions contain duplicate instruments")
        object.__setattr__(self, "positions", positions)
        object.__setattr__(
            self,
            "snapshot_hash",
            _digest(
                "trading-core.execution-risk-account-snapshot.v1",
                {
                    "snapshot_id": self.snapshot_id,
                    "account_id": self.account_id,
                    "account_status": self.account_status,
                    "reconciliation_status": self.reconciliation_status,
                    "observed_at": self.observed_at,
                    "account_equity": self.account_equity,
                    "available_cash": self.available_cash,
                    "total_pending_order_notional": (
                        self.total_pending_order_notional
                    ),
                    "positions": tuple(
                        {
                            "instrument_id": item.instrument_id,
                            "quantity": item.quantity,
                            "notional": item.notional,
                        }
                        for item in positions
                    ),
                },
            ),
        )

    @property
    def total_position_notional(self) -> Decimal:
        return _sum_decimals(tuple(item.notional for item in self.positions))


@dataclass(frozen=True, slots=True)
class ExecutionRiskOrder:
    order_id: str
    account_id: str
    instrument_id: str
    side: OrderSide
    quantity: int
    limit_price: Decimal
    fee_reserve: Decimal = Decimal("0")
    order_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for field_name in ("order_id", "account_id", "instrument_id"):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        if type(self.side) is not OrderSide:
            raise TypeError("side must be exactly OrderSide")
        _integer(self.quantity, "quantity", minimum=1)
        object.__setattr__(
            self,
            "limit_price",
            _decimal(self.limit_price, "limit_price", positive=True),
        )
        object.__setattr__(
            self,
            "fee_reserve",
            _decimal(self.fee_reserve, "fee_reserve", non_negative=True),
        )
        object.__setattr__(
            self,
            "order_hash",
            _digest(
                "trading-core.execution-risk-order.v1",
                {
                    "order_id": self.order_id,
                    "account_id": self.account_id,
                    "instrument_id": self.instrument_id,
                    "side": self.side,
                    "quantity": self.quantity,
                    "limit_price": self.limit_price,
                    "fee_reserve": self.fee_reserve,
                },
            ),
        )


@dataclass(frozen=True, slots=True)
class NeutralExecutionRiskEnvelope:
    envelope_version: str
    snapshot_max_age: timedelta
    max_order_quantity: int
    max_order_notional: Decimal
    max_instrument_position_quantity: int
    max_total_position_notional: Decimal
    max_total_pending_order_notional: Decimal
    max_absolute_concentration: Decimal
    notional_quantum: Decimal = Decimal("0.01")
    notional_rounding: RiskNotionalRounding = RiskNotionalRounding.DOWN
    envelope_hash: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "envelope_version",
            _text(self.envelope_version, "envelope_version"),
        )
        _duration(self.snapshot_max_age, "snapshot_max_age")
        for field_name in (
            "max_order_quantity",
            "max_instrument_position_quantity",
        ):
            _integer(getattr(self, field_name), field_name)
        for field_name in (
            "max_order_notional",
            "max_total_position_notional",
            "max_total_pending_order_notional",
        ):
            object.__setattr__(
                self,
                field_name,
                _decimal(
                    getattr(self, field_name),
                    field_name,
                    non_negative=True,
                ),
            )
        concentration = _decimal(
            self.max_absolute_concentration,
            "max_absolute_concentration",
            non_negative=True,
        )
        if concentration > 1:
            raise ValueError("max_absolute_concentration cannot exceed one")
        object.__setattr__(
            self,
            "max_absolute_concentration",
            concentration,
        )
        object.__setattr__(
            self,
            "notional_quantum",
            _decimal(self.notional_quantum, "notional_quantum", positive=True),
        )
        if type(self.notional_rounding) is not RiskNotionalRounding:
            raise TypeError(
                "notional_rounding must be exactly RiskNotionalRounding"
            )
        object.__setattr__(
            self,
            "envelope_hash",
            _digest(
                "trading-core.neutral-execution-risk-envelope.v1",
                {
                    "envelope_version": self.envelope_version,
                    "snapshot_max_age": self.snapshot_max_age,
                    "max_order_quantity": self.max_order_quantity,
                    "max_order_notional": self.max_order_notional,
                    "max_instrument_position_quantity": (
                        self.max_instrument_position_quantity
                    ),
                    "max_total_position_notional": (
                        self.max_total_position_notional
                    ),
                    "max_total_pending_order_notional": (
                        self.max_total_pending_order_notional
                    ),
                    "max_absolute_concentration": (
                        self.max_absolute_concentration
                    ),
                    "notional_quantum": self.notional_quantum,
                    "notional_rounding": self.notional_rounding,
                },
            ),
        )

    def order_notional(self, order: ExecutionRiskOrder) -> Decimal:
        _require_reconstructable(
            self,
            NeutralExecutionRiskEnvelope,
            "envelope",
        )
        _require_reconstructable(order, ExecutionRiskOrder, "order")
        rounding = (
            ROUND_DOWN
            if self.notional_rounding == RiskNotionalRounding.DOWN
            else ROUND_HALF_UP
        )
        with localcontext() as context:
            context.prec = 128
            return _multiply(order.limit_price, order.quantity).quantize(
                self.notional_quantum,
                rounding=rounding,
            )


@dataclass(frozen=True, slots=True)
class NeutralExecutionRiskDecision:
    status: RiskEnvelopeDecisionStatus
    reason_codes: tuple[RiskEnvelopeReason, ...]
    envelope_version: str
    envelope_hash: str
    snapshot_id: str
    snapshot_hash: str
    order_id: str
    order_hash: str
    evaluated_at: datetime
    order_notional: Decimal
    cash_required: Decimal
    projected_instrument_quantity: int
    projected_instrument_notional: Decimal
    projected_total_position_notional: Decimal
    projected_total_pending_order_notional: Decimal
    projected_max_instrument_notional: Decimal
    decision_hash: str = field(init=False)

    def __post_init__(self) -> None:
        if type(self.status) is not RiskEnvelopeDecisionStatus:
            raise TypeError("status must be exactly RiskEnvelopeDecisionStatus")
        if type(self.reason_codes) is not tuple or any(
            type(item) is not RiskEnvelopeReason for item in self.reason_codes
        ):
            raise TypeError("reason_codes must contain exact RiskEnvelopeReason values")
        if len(self.reason_codes) != len(set(self.reason_codes)):
            raise ValueError("reason_codes must be unique")
        if tuple(sorted(self.reason_codes, key=_REASON_ORDER.__getitem__)) != self.reason_codes:
            raise ValueError("reason_codes must be in canonical order")
        if (self.status == RiskEnvelopeDecisionStatus.APPROVED) != (
            not self.reason_codes
        ):
            raise ValueError("decision status and reason_codes are inconsistent")
        object.__setattr__(
            self,
            "envelope_version",
            _text(self.envelope_version, "envelope_version"),
        )
        for field_name in (
            "envelope_hash",
            "snapshot_hash",
            "order_hash",
        ):
            object.__setattr__(
                self,
                field_name,
                _sha256(getattr(self, field_name), field_name),
            )
        for field_name in ("snapshot_id", "order_id"):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        _aware(self.evaluated_at, "evaluated_at")
        _integer(
            self.projected_instrument_quantity,
            "projected_instrument_quantity",
        )
        for field_name in (
            "order_notional",
            "cash_required",
            "projected_instrument_notional",
            "projected_total_position_notional",
            "projected_total_pending_order_notional",
            "projected_max_instrument_notional",
        ):
            object.__setattr__(
                self,
                field_name,
                _decimal(
                    getattr(self, field_name),
                    field_name,
                    non_negative=True,
                ),
            )
        object.__setattr__(
            self,
            "decision_hash",
            _digest(
                "trading-core.neutral-execution-risk-decision.v1",
                {
                    "status": self.status,
                    "reason_codes": self.reason_codes,
                    "envelope_version": self.envelope_version,
                    "envelope_hash": self.envelope_hash,
                    "snapshot_id": self.snapshot_id,
                    "snapshot_hash": self.snapshot_hash,
                    "order_id": self.order_id,
                    "order_hash": self.order_hash,
                    "evaluated_at": self.evaluated_at,
                    "order_notional": self.order_notional,
                    "cash_required": self.cash_required,
                    "projected_instrument_quantity": (
                        self.projected_instrument_quantity
                    ),
                    "projected_instrument_notional": (
                        self.projected_instrument_notional
                    ),
                    "projected_total_position_notional": (
                        self.projected_total_position_notional
                    ),
                    "projected_total_pending_order_notional": (
                        self.projected_total_pending_order_notional
                    ),
                    "projected_max_instrument_notional": (
                        self.projected_max_instrument_notional
                    ),
                },
            ),
        )


def evaluate_neutral_execution_risk(
    envelope: NeutralExecutionRiskEnvelope,
    *,
    account: ExecutionRiskAccountSnapshot,
    order: ExecutionRiskOrder,
    evaluated_at: datetime,
) -> NeutralExecutionRiskDecision:
    """Evaluate one order against absolute, strategy-free mechanical limits."""

    if type(envelope) is not NeutralExecutionRiskEnvelope:
        raise TypeError("envelope must be exactly NeutralExecutionRiskEnvelope")
    if type(account) is not ExecutionRiskAccountSnapshot:
        raise TypeError("account must be exactly ExecutionRiskAccountSnapshot")
    if type(order) is not ExecutionRiskOrder:
        raise TypeError("order must be exactly ExecutionRiskOrder")
    _require_reconstructable(
        envelope,
        NeutralExecutionRiskEnvelope,
        "envelope",
    )
    _require_reconstructable(
        account,
        ExecutionRiskAccountSnapshot,
        "account",
    )
    _require_reconstructable(order, ExecutionRiskOrder, "order")
    now = _aware(evaluated_at, "evaluated_at")
    order_notional = envelope.order_notional(order)
    cash_required = (
        _add(order_notional, order.fee_reserve)
        if order.side == OrderSide.BUY
        else Decimal("0")
    )

    positions = {item.instrument_id: item for item in account.positions}
    current = positions.get(order.instrument_id)
    current_quantity = current.quantity if current is not None else 0
    current_notional = current.notional if current is not None else Decimal("0")
    if order.side == OrderSide.BUY:
        projected_quantity = current_quantity + order.quantity
        projected_instrument_notional = _add(
            current_notional,
            order_notional,
        )
        projected_total_position_notional = _add(
            account.total_position_notional,
            order_notional,
        )
    else:
        projected_quantity = max(0, current_quantity - order.quantity)
        projected_instrument_notional = _remaining_position_notional(
            current_notional=current_notional,
            current_quantity=current_quantity,
            remaining_quantity=projected_quantity,
            quantum=envelope.notional_quantum,
        )
        projected_total_position_notional = _add(
            _subtract_floor_zero(
                account.total_position_notional,
                current_notional,
            ),
            projected_instrument_notional,
        )
    projected_notionals = tuple(
        projected_instrument_notional
        if item.instrument_id == order.instrument_id
        else item.notional
        for item in account.positions
    )
    if current is None:
        projected_notionals = (*projected_notionals, projected_instrument_notional)
    projected_max_instrument_notional = max(
        projected_notionals,
        default=Decimal("0"),
    )
    projected_total_pending_order_notional = _add(
        account.total_pending_order_notional,
        order_notional,
    )

    reasons: list[RiskEnvelopeReason] = []
    if order.account_id != account.account_id:
        reasons.append(RiskEnvelopeReason.ACCOUNT_ID_MISMATCH)
    if account.account_status != RiskAccountStatus.ACTIVE:
        reasons.append(RiskEnvelopeReason.ACCOUNT_NOT_ACTIVE)
    if account.reconciliation_status != RiskReconciliationStatus.PASS:
        reasons.append(RiskEnvelopeReason.RECONCILIATION_NOT_PASS)
    if account.observed_at > now:
        reasons.append(RiskEnvelopeReason.SNAPSHOT_FROM_FUTURE)
    elif now - account.observed_at > envelope.snapshot_max_age:
        reasons.append(RiskEnvelopeReason.SNAPSHOT_STALE)
    if order.quantity > envelope.max_order_quantity:
        reasons.append(RiskEnvelopeReason.ORDER_QUANTITY_LIMIT_EXCEEDED)
    if order_notional > envelope.max_order_notional:
        reasons.append(RiskEnvelopeReason.ORDER_NOTIONAL_LIMIT_EXCEEDED)
    if projected_quantity > envelope.max_instrument_position_quantity:
        reasons.append(
            RiskEnvelopeReason.INSTRUMENT_POSITION_LIMIT_EXCEEDED
        )
    if (
        projected_total_position_notional
        > envelope.max_total_position_notional
    ):
        reasons.append(
            RiskEnvelopeReason.TOTAL_POSITION_NOTIONAL_LIMIT_EXCEEDED
        )
    if (
        projected_total_pending_order_notional
        > envelope.max_total_pending_order_notional
    ):
        reasons.append(
            RiskEnvelopeReason.TOTAL_PENDING_ORDER_NOTIONAL_LIMIT_EXCEEDED
        )
    if order.side == OrderSide.SELL and order.quantity > current_quantity:
        reasons.append(RiskEnvelopeReason.SELL_QUANTITY_EXCEEDS_POSITION)
    if cash_required > account.available_cash:
        reasons.append(RiskEnvelopeReason.INSUFFICIENT_AVAILABLE_CASH)
    concentration_cap = _multiply(
        account.account_equity,
        envelope.max_absolute_concentration,
    )
    if projected_max_instrument_notional > concentration_cap:
        reasons.append(
            RiskEnvelopeReason.ABSOLUTE_CONCENTRATION_LIMIT_EXCEEDED
        )

    reason_codes = tuple(reasons)
    status = (
        RiskEnvelopeDecisionStatus.APPROVED
        if not reason_codes
        else RiskEnvelopeDecisionStatus.BLOCKED
    )
    return NeutralExecutionRiskDecision(
        status=status,
        reason_codes=reason_codes,
        envelope_version=envelope.envelope_version,
        envelope_hash=envelope.envelope_hash,
        snapshot_id=account.snapshot_id,
        snapshot_hash=account.snapshot_hash,
        order_id=order.order_id,
        order_hash=order.order_hash,
        evaluated_at=now,
        order_notional=order_notional,
        cash_required=cash_required,
        projected_instrument_quantity=projected_quantity,
        projected_instrument_notional=projected_instrument_notional,
        projected_total_position_notional=projected_total_position_notional,
        projected_total_pending_order_notional=(
            projected_total_pending_order_notional
        ),
        projected_max_instrument_notional=projected_max_instrument_notional,
    )


__all__ = [
    "ExecutionRiskAccountSnapshot",
    "ExecutionRiskOrder",
    "ExecutionRiskPosition",
    "NeutralExecutionRiskDecision",
    "NeutralExecutionRiskEnvelope",
    "RiskAccountStatus",
    "RiskEnvelopeDecisionStatus",
    "RiskEnvelopeReason",
    "RiskNotionalRounding",
    "RiskReconciliationStatus",
    "evaluate_neutral_execution_risk",
]
