"""Typed domain objects for deterministic V2 decisions and paper execution."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from enum import Enum
from typing import Any


MONEY_QUANTUM = Decimal("0.01")


def decimal_value(value: Any, default: str = "0") -> Decimal:
    if value is None or value == "":
        return Decimal(default)
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def money(value: Any) -> Decimal:
    return decimal_value(value).quantize(MONEY_QUANTUM, rounding=ROUND_DOWN)


def adverse_price(value: Any, tick_size: Any, *, side: "OrderSide") -> Decimal:
    price = decimal_value(value)
    tick = decimal_value(tick_size)
    if tick <= 0:
        raise ValueError("tick_size must be positive")
    rounding = ROUND_UP if side == OrderSide.BUY else ROUND_DOWN
    return (price / tick).to_integral_value(rounding=rounding) * tick


class ValueStrEnum(str, Enum):
    def __str__(self) -> str:
        return str(self.value)


class IntentAction(ValueStrEnum):
    OPEN = "OPEN"
    ADD = "ADD"
    HOLD = "HOLD"
    REDUCE = "REDUCE"
    EXIT = "EXIT"
    CANCEL = "CANCEL"


class OrderSide(ValueStrEnum):
    BUY = "BUY"
    SELL = "SELL"


class RiskDecisionStatus(ValueStrEnum):
    APPROVED = "APPROVED"
    REDUCED = "REDUCED"
    REJECTED = "REJECTED"


class PositionState(ValueStrEnum):
    OPENING = "OPENING"
    VALID_STRONG = "VALID_STRONG"
    VALID = "VALID"
    WEAKENED = "WEAKENED"
    BROKEN = "BROKEN"
    RISK_EXIT = "RISK_EXIT"
    EXIT_PENDING_T1 = "EXIT_PENDING_T1"
    EXIT_PENDING_LIQUIDITY = "EXIT_PENDING_LIQUIDITY"
    CLOSED = "CLOSED"


class OrderStatus(ValueStrEnum):
    CREATED = "CREATED"
    RISK_APPROVED = "RISK_APPROVED"
    QUEUED = "QUEUED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"


class WaitingReason(ValueStrEnum):
    WAIT_NO_QUOTE = "WAIT_NO_QUOTE"
    WAIT_STALE_QUOTE = "WAIT_STALE_QUOTE"
    WAIT_LIMIT_LOCK = "WAIT_LIMIT_LOCK"
    WAIT_SUSPENDED = "WAIT_SUSPENDED"
    WAIT_T1 = "WAIT_T1"
    WAIT_LIQUIDITY = "WAIT_LIQUIDITY"
    WAIT_SECTOR_CONFIRMATION = "WAIT_SECTOR_CONFIRMATION"
    WAIT_ENTRY_TREND_INVALID = "WAIT_ENTRY_TREND_INVALID"


@dataclass(frozen=True)
class InstrumentRule:
    stock_code: str
    rule_version: str
    security_type: str
    exchange: str
    effective_from: date
    effective_to: date | None
    can_buy: bool
    first_buy_minimum: int
    buy_lot_size: int
    sell_lot_size: int
    settlement_days: int
    tick_size: Decimal
    limit_ratio: Decimal | None
    is_suspended: bool = False
    permission_required: str = ""
    permission_confirmed: bool = False
    fee_profile_version: str = ""

    def validate_for_buy(self) -> str | None:
        if not self.can_buy:
            return "INSTRUMENT_NOT_BUYABLE"
        if self.is_suspended:
            return "INSTRUMENT_SUSPENDED"
        if self.permission_required and not self.permission_confirmed:
            return "INSTRUMENT_PERMISSION_UNKNOWN"
        if not self.fee_profile_version:
            return "FEE_PROFILE_UNCONFIRMED"
        if self.buy_lot_size <= 0 or self.first_buy_minimum <= 0 or self.tick_size <= 0:
            return "INSTRUMENT_RULE_INVALID"
        return None

    def floor_buy_quantity(self, quantity: int) -> int:
        if quantity < self.first_buy_minimum:
            return 0
        return quantity - quantity % self.buy_lot_size


@dataclass(frozen=True)
class AccountSnapshot:
    account_id: str
    equity: Decimal
    available_cash: Decimal
    peak_equity: Decimal
    current_market_value: Decimal = Decimal("0")
    current_open_risk: Decimal = Decimal("0")
    position_count: int = 0
    theme_position_counts: dict[str, int] = field(default_factory=dict)
    theme_market_values: dict[str, Decimal] = field(default_factory=dict)
    reconciliation_status: str = "PASS"
    account_status: str = "ACTIVE"

    @property
    def drawdown(self) -> Decimal:
        if self.peak_equity <= 0:
            return Decimal("0")
        return max(Decimal("0"), (self.peak_equity - self.equity) / self.peak_equity)


@dataclass(frozen=True)
class TradeIntent:
    intent_id: str
    account_id: str
    decision_run_uid: str
    strategy_version: str
    stock_code: str
    action: IntentAction
    current_quantity: int
    target_quantity: int
    target_weight: Decimal
    earliest_at: datetime
    expires_at: datetime
    limit_price: Decimal
    worst_price: Decimal
    initial_stop: Decimal
    protective_stop: Decimal
    invalidation_condition: str
    reason_code: str
    evidence: tuple[dict[str, Any], ...]
    idempotency_key: str
    theme_code: str = ""

    @property
    def requested_quantity(self) -> int:
        return abs(self.target_quantity - self.current_quantity)

    @property
    def side(self) -> OrderSide:
        return OrderSide.BUY if self.target_quantity > self.current_quantity else OrderSide.SELL


@dataclass(frozen=True)
class RiskDecision:
    intent_id: str
    status: RiskDecisionStatus
    requested_quantity: int
    approved_quantity: int
    trade_risk: Decimal
    post_single_weight: Decimal
    post_total_weight: Decimal
    post_theme_weight: Decimal
    post_open_risk: Decimal
    post_cash: Decimal
    checks: tuple[dict[str, Any], ...]
    first_failure: str = ""

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Quote:
    stock_code: str
    event_id: str
    quote_at: datetime
    received_at: datetime
    bid1: Decimal | None
    bid1_volume: int | None
    ask1: Decimal | None
    ask1_volume: int | None
    last_price: Decimal | None
    upper_limit: Decimal | None
    lower_limit: Decimal | None
    suspended: bool = False


@dataclass(frozen=True)
class MatchResult:
    status: str
    waiting_reason: str
    fill_quantity: int
    fill_price: Decimal | None
    event_id: str
    explanation: str


@dataclass(frozen=True)
class PositionFacts:
    current_state: PositionState
    current_quantity: int
    approved_target_quantity: int
    add_count: int
    average_cost: Decimal
    last_price: Decimal
    current_protective_stop: Decimal
    proposed_protective_stop: Decimal
    risk_event: bool = False
    hard_stop_breached: bool = False
    invalidated: bool = False
    trend_strong: bool = False
    trend_valid: bool = True
    theme_faded: bool = False
    can_sell_today: bool = True
    liquidity_available: bool = True


@dataclass(frozen=True)
class PositionDecision:
    previous_state: PositionState
    next_state: PositionState
    action: IntentAction
    target_quantity: int
    protective_stop: Decimal
    reason_code: str
