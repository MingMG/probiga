"""Immutable, strategy-neutral accounting calculations for one fill.

The state is an in-process value object, not a durable ledger.  A V2-compatible
writer may use the returned transition inside its existing database
transaction; this module owns no engine, table, account, order, or broker I/O.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from enum import Enum
import hashlib
import json
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..contracts import OrderSide
from ..market_rules.fees import FeeSchedule, incremental_order_fee_delta


class AccountingError(RuntimeError):
    pass


class AccountingInvariantError(AccountingError):
    pass


class AccountingIdempotencyConflict(AccountingError):
    pass


class InsufficientCashError(AccountingError):
    pass


class InsufficientSellableQuantityError(AccountingError):
    pass


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _integer(value: object, field_name: str, *, minimum: int = 0) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field_name} must be an integer")
    if value < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}")
    return value


def _decimal(value: object, field_name: str, *, minimum: Decimal | None = None) -> Decimal:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be decimal-like")
    try:
        if isinstance(value, Decimal):
            if type(value) is not Decimal:
                raise TypeError(f"{field_name} must not be a Decimal subclass")
            converted = value
        elif type(value) in {str, int, float}:
            converted = Decimal(str(value))
        else:
            raise TypeError(f"{field_name} must be decimal-like")
    except Exception as exc:
        raise TypeError(f"{field_name} must be decimal-like") from exc
    if not converted.is_finite():
        raise ValueError(f"{field_name} must be finite")
    if minimum is not None and converted < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}")
    return converted


def _aware(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field_name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def _trade_date(value: object, field_name: str) -> date:
    if type(value) is not date:
        raise TypeError(f"{field_name} must be exactly date")
    return value


def _canonical(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("accounting hash decimals must be finite")
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
    if hasattr(value, "as_dict"):
        return _canonical(value.as_dict())
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _canonical(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"unsupported accounting hash value: {type(value).__name__}")


def _hash(namespace: str, payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        {"namespace": namespace, "payload": _canonical(payload)},
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


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
    """Reject exact dataclass instances whose frozen fields were bypassed.

    ``frozen=True`` protects normal assignment, not ``object.__setattr__`` or
    ``object.__new__``.  Reconstructing through the public initializer reruns
    all invariants and verifies every derived hash before the value is trusted.
    """

    if type(value) is not expected_type:
        raise TypeError(
            f"{field_name} must be exactly {expected_type.__name__}"
        )
    try:
        reconstructed = replace(value)
    except (AttributeError, TypeError, ValueError) as exc:
        raise AccountingInvariantError(
            f"{field_name} cannot be reconstructed"
        ) from exc
    if reconstructed != value:
        raise AccountingInvariantError(
            f"{field_name} differs from reconstructed canonical value"
        )
    return value


def fee_schedule_fingerprint(schedule: FeeSchedule) -> str:
    _require_reconstructable(schedule, FeeSchedule, "schedule")
    return _hash(
        "trading-core.fee-schedule.v1",
        asdict(schedule),
    )


@dataclass(frozen=True, slots=True)
class SettlementEvidence:
    """Calendar-derived evidence for a BUY lot's first sellable session."""

    instrument_id: str
    instrument_rule_version: str
    instrument_rule_hash: str
    trade_date: date
    settlement_days: int
    trading_days: tuple[date, ...]
    calendar_version: str
    market_timezone: str
    settlement_date: date = field(init=False)
    calendar_hash: str = field(init=False)
    evidence_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for field_name in (
            "instrument_id",
            "instrument_rule_version",
            "calendar_version",
            "market_timezone",
        ):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        trade_date = _trade_date(self.trade_date, "trade_date")
        object.__setattr__(
            self,
            "instrument_rule_hash",
            _sha256(self.instrument_rule_hash, "instrument_rule_hash"),
        )
        settlement_days = _integer(
            self.settlement_days,
            "settlement_days",
        )
        if type(self.trading_days) is not tuple:
            raise TypeError("trading_days must be a tuple")
        days = tuple(
            _trade_date(item, "trading_day") for item in self.trading_days
        )
        if not days or days != tuple(sorted(set(days))):
            raise ValueError(
                "trading_days must be non-empty, increasing, and unique"
            )
        try:
            trade_index = days.index(trade_date)
        except ValueError as exc:
            raise ValueError("trade_date must be present in trading_days") from exc
        settlement_index = trade_index + settlement_days
        if settlement_index >= len(days):
            raise ValueError("calendar does not cover the settlement session")
        try:
            ZoneInfo(self.market_timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("market_timezone is unknown") from exc
        settlement_date = days[settlement_index]
        calendar_hash = _hash(
            "trading-core.trading-calendar.v1",
            {
                "calendar_version": self.calendar_version,
                "market_timezone": self.market_timezone,
                "trading_days": days,
            },
        )
        evidence_hash = _hash(
            "trading-core.settlement-evidence.v1",
            {
                "instrument_id": self.instrument_id,
                "instrument_rule_version": self.instrument_rule_version,
                "instrument_rule_hash": self.instrument_rule_hash,
                "trade_date": trade_date,
                "settlement_days": settlement_days,
                "settlement_date": settlement_date,
                "calendar_hash": calendar_hash,
            },
        )
        object.__setattr__(self, "trading_days", days)
        object.__setattr__(self, "settlement_date", settlement_date)
        object.__setattr__(self, "calendar_hash", calendar_hash)
        object.__setattr__(self, "evidence_hash", evidence_hash)


@dataclass(frozen=True, slots=True)
class AccountingFillRequest:
    fill_id: str
    idempotency_key: str
    order_id: str
    instrument_id: str
    side: OrderSide
    quantity: int
    price: Decimal
    trade_date: date
    filled_at: datetime
    fee_profile_version: str
    fee_schedule_hash: str
    market_timezone: str
    settlement_evidence: SettlementEvidence | None = None
    settlement_date: date = field(init=False)
    request_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for field_name in (
            "fill_id",
            "idempotency_key",
            "order_id",
            "instrument_id",
            "fee_profile_version",
            "market_timezone",
        ):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        object.__setattr__(self, "side", OrderSide(self.side))
        object.__setattr__(
            self,
            "fee_schedule_hash",
            _sha256(self.fee_schedule_hash, "fee_schedule_hash"),
        )
        _integer(self.quantity, "quantity", minimum=1)
        price = _decimal(self.price, "price", minimum=Decimal("0"))
        if price == 0:
            raise ValueError("price must be positive")
        object.__setattr__(self, "price", price)
        trade_date = _trade_date(self.trade_date, "trade_date")
        filled_at = _aware(self.filled_at, "filled_at")
        try:
            market_zone = ZoneInfo(self.market_timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("market_timezone is unknown") from exc
        if filled_at.astimezone(market_zone).date() != trade_date:
            raise ValueError(
                "filled_at does not fall on trade_date in market_timezone"
            )
        evidence_hash = None
        if self.side == OrderSide.BUY:
            evidence = _require_reconstructable(
                self.settlement_evidence,
                SettlementEvidence,
                "settlement_evidence",
            )
            if (
                evidence.instrument_id != self.instrument_id
                or evidence.trade_date != trade_date
                or evidence.market_timezone != self.market_timezone
            ):
                raise ValueError(
                    "settlement evidence does not match fill request"
                )
            settlement_date = evidence.settlement_date
            evidence_hash = evidence.evidence_hash
        else:
            if self.settlement_evidence is not None:
                raise ValueError("SELL request cannot carry settlement evidence")
            settlement_date = trade_date
        object.__setattr__(self, "settlement_date", settlement_date)
        payload = {
            "fill_id": self.fill_id,
            "idempotency_key": self.idempotency_key,
            "order_id": self.order_id,
            "instrument_id": self.instrument_id,
            "side": self.side,
            "quantity": self.quantity,
            "price": self.price,
            "trade_date": trade_date,
            "filled_at": self.filled_at,
            "settlement_date": settlement_date,
            "fee_profile_version": self.fee_profile_version,
            "fee_schedule_hash": self.fee_schedule_hash,
            "market_timezone": self.market_timezone,
            "settlement_evidence_hash": evidence_hash,
        }
        object.__setattr__(
            self,
            "request_hash",
            _hash("trading-core.accounting-fill-request.v1", payload),
        )


@dataclass(frozen=True, slots=True)
class AccountingFill:
    fill_id: str
    idempotency_key: str
    request_hash: str
    order_id: str
    instrument_id: str
    side: OrderSide
    quantity: int
    price: Decimal
    gross_amount: Decimal
    fee_amount: Decimal
    net_cash_amount: Decimal
    trade_date: date
    settlement_date: date
    filled_at: datetime
    fee_profile_version: str
    fee_schedule_hash: str
    settlement_evidence_hash: str | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "fill_id",
            "idempotency_key",
            "request_hash",
            "order_id",
            "instrument_id",
            "fee_profile_version",
        ):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        if len(self.request_hash) != 64 or any(
            item not in "0123456789abcdef" for item in self.request_hash.lower()
        ):
            raise ValueError("request_hash must be a SHA-256 digest")
        object.__setattr__(self, "request_hash", self.request_hash.lower())
        object.__setattr__(
            self,
            "fee_schedule_hash",
            _sha256(self.fee_schedule_hash, "fee_schedule_hash"),
        )
        if self.settlement_evidence_hash is not None:
            object.__setattr__(
                self,
                "settlement_evidence_hash",
                _sha256(
                    self.settlement_evidence_hash,
                    "settlement_evidence_hash",
                ),
            )
        object.__setattr__(self, "side", OrderSide(self.side))
        _integer(self.quantity, "quantity", minimum=1)
        for field_name, minimum in (
            ("price", Decimal("0.000000000000000001")),
            ("gross_amount", Decimal("0.000000000000000001")),
            ("fee_amount", Decimal("0")),
        ):
            object.__setattr__(
                self,
                field_name,
                _decimal(getattr(self, field_name), field_name, minimum=minimum),
            )
        object.__setattr__(
            self,
            "net_cash_amount",
            _decimal(self.net_cash_amount, "net_cash_amount"),
        )
        expected_cash = (
            -(self.gross_amount + self.fee_amount)
            if self.side == OrderSide.BUY
            else self.gross_amount - self.fee_amount
        )
        if self.net_cash_amount != expected_cash:
            raise AccountingInvariantError("fill cash effect is inconsistent")
        _trade_date(self.trade_date, "trade_date")
        settlement_date = _trade_date(
            self.settlement_date,
            "settlement_date",
        )
        if settlement_date < self.trade_date:
            raise ValueError("settlement_date cannot precede trade_date")
        if self.side == OrderSide.BUY:
            if self.settlement_evidence_hash is None:
                raise ValueError("BUY fill requires settlement evidence")
        elif (
            settlement_date != self.trade_date
            or self.settlement_evidence_hash is not None
        ):
            raise ValueError("SELL fill must settle on trade_date without evidence")
        _aware(self.filled_at, "filled_at")


@dataclass(frozen=True, slots=True)
class AccountingLot:
    lot_id: str
    instrument_id: str
    opened_fill_id: str
    acquired_on: date
    sellable_on: date
    original_quantity: int
    remaining_quantity: int
    cost_price: Decimal
    allocated_buy_fee: Decimal

    def __post_init__(self) -> None:
        for field_name in ("lot_id", "instrument_id", "opened_fill_id"):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        acquired = _trade_date(self.acquired_on, "acquired_on")
        sellable = _trade_date(self.sellable_on, "sellable_on")
        if sellable < acquired:
            raise ValueError("sellable_on cannot precede acquired_on")
        _integer(self.original_quantity, "original_quantity", minimum=1)
        _integer(self.remaining_quantity, "remaining_quantity")
        if self.remaining_quantity > self.original_quantity:
            raise ValueError("remaining_quantity cannot exceed original_quantity")
        object.__setattr__(
            self,
            "cost_price",
            _decimal(self.cost_price, "cost_price", minimum=Decimal("0")),
        )
        if self.cost_price == 0:
            raise ValueError("cost_price must be positive")
        object.__setattr__(
            self,
            "allocated_buy_fee",
            _decimal(
                self.allocated_buy_fee,
                "allocated_buy_fee",
                minimum=Decimal("0"),
            ),
        )


@dataclass(frozen=True, slots=True)
class AccountingCashMovement:
    movement_id: str
    business_key: str
    event_type: str
    amount: Decimal
    balance_after: Decimal
    occurred_at: datetime

    def __post_init__(self) -> None:
        for field_name in ("movement_id", "business_key", "event_type"):
            object.__setattr__(
                self,
                field_name,
                _text(getattr(self, field_name), field_name),
            )
        object.__setattr__(self, "amount", _decimal(self.amount, "amount"))
        object.__setattr__(
            self,
            "balance_after",
            _decimal(
                self.balance_after,
                "balance_after",
                minimum=Decimal("0"),
            ),
        )
        _aware(self.occurred_at, "occurred_at")


def _validate_exact_tuple(
    values: tuple[Any, ...],
    expected_type: type,
    field_name: str,
) -> tuple[Any, ...]:
    if type(values) is not tuple:
        raise TypeError(f"{field_name} must be a tuple")
    if any(type(item) is not expected_type for item in values):
        raise TypeError(f"{field_name} must contain exact {expected_type.__name__} values")
    return values


@dataclass(frozen=True, slots=True)
class AccountingState:
    opening_cash: Decimal
    cash_balance: Decimal
    fee_schedules: tuple[FeeSchedule, ...]
    currency_quantum: Decimal = Decimal("0.01")
    rounding_mode: str = ROUND_HALF_UP
    requests: tuple[AccountingFillRequest, ...] = ()
    fills: tuple[AccountingFill, ...] = ()
    lots: tuple[AccountingLot, ...] = ()
    cash_movements: tuple[AccountingCashMovement, ...] = ()
    state_hash: str = field(init=False)
    fee_schedule_hashes: tuple[str, ...] = field(init=False)

    def __post_init__(self) -> None:
        if type(self.fee_schedules) is not tuple or not self.fee_schedules:
            raise TypeError("fee_schedules must be a non-empty tuple")
        for item in self.fee_schedules:
            _require_reconstructable(item, FeeSchedule, "fee_schedules item")
        ordered_schedules = tuple(
            sorted(
                self.fee_schedules,
                key=lambda item: (
                    item.profile_version,
                    fee_schedule_fingerprint(item),
                ),
            )
        )
        if ordered_schedules != self.fee_schedules:
            raise ValueError("fee_schedules must be in canonical order")
        schedule_hashes = tuple(
            fee_schedule_fingerprint(item) for item in ordered_schedules
        )
        if len(schedule_hashes) != len(set(schedule_hashes)):
            raise ValueError("fee_schedules contain duplicate parameter sets")
        versions = [item.profile_version for item in ordered_schedules]
        if len(versions) != len(set(versions)):
            raise ValueError(
                "one fee profile version cannot identify different schedules"
            )
        if any(not item.round_notional_before_fees for item in ordered_schedules):
            raise ValueError(
                "accounting state requires per-fill notional rounding"
            )
        opening_cash = _decimal(
            self.opening_cash,
            "opening_cash",
            minimum=Decimal("0"),
        )
        cash_balance = _decimal(
            self.cash_balance,
            "cash_balance",
            minimum=Decimal("0"),
        )
        currency_quantum = _decimal(
            self.currency_quantum,
            "currency_quantum",
            minimum=Decimal("0"),
        )
        if currency_quantum == 0:
            raise ValueError("currency_quantum must be positive")
        if not isinstance(self.rounding_mode, str):
            raise TypeError("rounding_mode must be a string")
        if self.rounding_mode not in {ROUND_HALF_UP, ROUND_DOWN}:
            raise ValueError("unsupported rounding_mode")
        if any(
            currency_quantum != schedule.currency_quantum
            or self.rounding_mode != schedule.fee_rounding_mode
            for schedule in ordered_schedules
        ):
            raise AccountingInvariantError(
                "state money arithmetic differs from fee schedule"
            )
        for value, field_name in (
            (opening_cash, "opening_cash"),
            (cash_balance, "cash_balance"),
        ):
            if value != value.quantize(currency_quantum, rounding=self.rounding_mode):
                raise AccountingInvariantError(
                    f"{field_name} is not aligned to currency_quantum"
                )
        object.__setattr__(self, "opening_cash", opening_cash)
        object.__setattr__(self, "cash_balance", cash_balance)
        object.__setattr__(self, "currency_quantum", currency_quantum)
        object.__setattr__(self, "fee_schedule_hashes", schedule_hashes)
        schedules_by_hash = dict(zip(schedule_hashes, ordered_schedules))
        requests = _validate_exact_tuple(
            self.requests,
            AccountingFillRequest,
            "requests",
        )
        fills = _validate_exact_tuple(self.fills, AccountingFill, "fills")
        lots = _validate_exact_tuple(self.lots, AccountingLot, "lots")
        movements = _validate_exact_tuple(
            self.cash_movements,
            AccountingCashMovement,
            "cash_movements",
        )
        for values, expected_type, field_name in (
            (requests, AccountingFillRequest, "request"),
            (fills, AccountingFill, "fill"),
            (lots, AccountingLot, "lot"),
            (movements, AccountingCashMovement, "cash movement"),
        ):
            for item in values:
                _require_reconstructable(item, expected_type, field_name)
        fill_ids = [item.fill_id for item in fills]
        fill_keys = [item.idempotency_key for item in fills]
        lot_ids = [item.lot_id for item in lots]
        movement_ids = [item.movement_id for item in movements]
        for values, label in (
            (fill_ids, "fill ids"),
            (fill_keys, "fill idempotency keys"),
            (lot_ids, "lot ids"),
            (movement_ids, "cash movement ids"),
        ):
            if len(values) != len(set(values)):
                raise AccountingInvariantError(f"duplicate {label}")
        if len(fills) != len(movements):
            raise AccountingInvariantError("each fill requires one cash movement")
        if len(requests) != len(fills):
            raise AccountingInvariantError("each fill requires its canonical request")
        for request, fill in zip(requests, fills):
            if (
                request.fill_id != fill.fill_id
                or request.request_hash != fill.request_hash
                or request.fee_schedule_hash not in schedules_by_hash
                or request.fee_profile_version
                != schedules_by_hash[request.fee_schedule_hash].profile_version
                or fill.fee_schedule_hash != request.fee_schedule_hash
            ):
                raise AccountingInvariantError(
                    "fill/request/fee schedule binding differs"
                )
        running_cash = opening_cash
        for fill, movement in zip(fills, movements):
            expected_gross = (fill.price * fill.quantity).quantize(
                currency_quantum,
                rounding=self.rounding_mode,
            )
            if expected_gross <= 0 or fill.gross_amount != expected_gross:
                raise AccountingInvariantError(
                    "fill gross amount differs from rounded price times quantity"
                )
            for amount, label in (
                (fill.gross_amount, "gross amount"),
                (fill.fee_amount, "fee amount"),
                (fill.net_cash_amount, "net cash amount"),
            ):
                if amount != amount.quantize(
                    currency_quantum,
                    rounding=self.rounding_mode,
                ):
                    raise AccountingInvariantError(
                        f"fill {label} is not aligned to currency_quantum"
                    )
            if movement.movement_id != f"CASH:{fill.fill_id}":
                raise AccountingInvariantError("cash movement id is not bound to fill")
            if movement.business_key != f"FILL:{fill.idempotency_key}":
                raise AccountingInvariantError("cash movement is not bound to fill")
            if movement.amount != fill.net_cash_amount:
                raise AccountingInvariantError("cash movement amount differs from fill")
            if movement.occurred_at != fill.filled_at:
                raise AccountingInvariantError("cash movement time differs from fill")
            expected_event_type = (
                "BUY_FILL" if fill.side == OrderSide.BUY else "SELL_FILL"
            )
            if movement.event_type != expected_event_type:
                raise AccountingInvariantError("cash movement type differs from fill")
            running_cash = (running_cash + movement.amount).quantize(
                currency_quantum,
                rounding=self.rounding_mode,
            )
            if movement.balance_after != running_cash:
                raise AccountingInvariantError("cash movement running balance differs")
        if cash_balance != running_cash:
            raise AccountingInvariantError("cash balance does not reconcile")
        fill_positions: dict[str, int] = {}
        for item in fills:
            direction = 1 if item.side == OrderSide.BUY else -1
            fill_positions[item.instrument_id] = (
                fill_positions.get(item.instrument_id, 0)
                + direction * item.quantity
            )
        lot_positions: dict[str, int] = {}
        for lot in lots:
            if lot.remaining_quantity < 0 or lot.remaining_quantity > lot.original_quantity:
                raise AccountingInvariantError("lot remaining quantity is invalid")
            lot_positions[lot.instrument_id] = (
                lot_positions.get(lot.instrument_id, 0) + lot.remaining_quantity
            )
        buy_fills = {
            item.fill_id: item for item in fills if item.side == OrderSide.BUY
        }
        if {lot.opened_fill_id for lot in lots} != set(buy_fills):
            raise AccountingInvariantError("lots do not exactly cover BUY fills")
        for lot in lots:
            source_fill = buy_fills[lot.opened_fill_id]
            if (
                lot.lot_id != f"LOT:{source_fill.fill_id}"
                or lot.instrument_id != source_fill.instrument_id
                or lot.acquired_on != source_fill.trade_date
                or lot.sellable_on != source_fill.settlement_date
                or lot.original_quantity != source_fill.quantity
                or lot.cost_price != source_fill.price
                or lot.allocated_buy_fee != source_fill.fee_amount
            ):
                raise AccountingInvariantError("lot differs from its BUY fill")
        if fill_positions != lot_positions:
            raise AccountingInvariantError("fill and lot positions do not reconcile")
        replay = _replay_accounting_requests(
            opening_cash=opening_cash,
            requests=requests,
            fee_schedules=ordered_schedules,
        )
        if (
            replay.cash_balance != cash_balance
            or replay.fills != fills
            or replay.lots != lots
            or replay.cash_movements != movements
        ):
            raise AccountingInvariantError(
                "accounting state differs from canonical request replay"
            )
        payload = {
            "opening_cash": opening_cash,
            "cash_balance": cash_balance,
            "currency_quantum": currency_quantum,
            "rounding_mode": self.rounding_mode,
            "fee_schedule_hashes": schedule_hashes,
            "requests": [asdict(item) for item in requests],
            "fills": [asdict(item) for item in fills],
            "lots": [asdict(item) for item in lots],
            "cash_movements": [asdict(item) for item in movements],
        }
        object.__setattr__(
            self,
            "state_hash",
            _hash("trading-core.accounting-state.v1", payload),
        )

    @classmethod
    def empty(
        cls,
        initial_cash: Decimal,
        *,
        fee_schedule: FeeSchedule,
    ) -> "AccountingState":
        if type(fee_schedule) is not FeeSchedule:
            raise TypeError("fee_schedule must be exactly FeeSchedule")
        cash = _decimal(initial_cash, "initial_cash", minimum=Decimal("0"))
        quantum = fee_schedule.currency_quantum
        rounding_mode = fee_schedule.fee_rounding_mode
        cash = cash.quantize(quantum, rounding=rounding_mode)
        return cls(
            opening_cash=cash,
            cash_balance=cash,
            fee_schedules=(fee_schedule,),
            currency_quantum=quantum,
            rounding_mode=rounding_mode,
        )

    def position_quantity(self, instrument_id: str) -> int:
        instrument = _text(instrument_id, "instrument_id")
        return sum(
            lot.remaining_quantity
            for lot in self.lots
            if lot.instrument_id == instrument
        )

    def available_to_sell(self, instrument_id: str, trade_date: date) -> int:
        instrument = _text(instrument_id, "instrument_id")
        as_of = _trade_date(trade_date, "trade_date")
        return sum(
            lot.remaining_quantity
            for lot in self.lots
            if lot.instrument_id == instrument and lot.sellable_on <= as_of
        )

    def reconcile(self) -> dict[str, object]:
        _require_reconstructable(self, AccountingState, "accounting state")
        replay = _replay_accounting_requests(
            opening_cash=self.opening_cash,
            requests=self.requests,
            fee_schedules=self.fee_schedules,
        )
        cash_difference = self.cash_balance - replay.cash_balance
        position_difference = sum(
            abs(
                self.position_quantity(instrument)
                - sum(
                    lot.remaining_quantity
                    for lot in replay.lots
                    if lot.instrument_id == instrument
                )
            )
            for instrument in {
                *(lot.instrument_id for lot in self.lots),
                *(lot.instrument_id for lot in replay.lots),
            }
        )
        duplicate_fill_count = len(self.fills) - len(
            {item.idempotency_key for item in self.fills}
        )
        passed = (
            cash_difference == 0
            and position_difference == 0
            and duplicate_fill_count == 0
            and self.cash_balance >= 0
            and self.fills == replay.fills
            and self.lots == replay.lots
            and self.cash_movements == replay.cash_movements
        )
        return {
            "status": "PASS" if passed else "RECONCILIATION_BLOCKED",
            "cash_difference": str(cash_difference),
            "position_difference": position_difference,
            "duplicate_fill_count": duplicate_fill_count,
            "negative_cash": self.cash_balance < 0,
        }


@dataclass(frozen=True, slots=True)
class AccountingApplyResult:
    state: AccountingState
    fill: AccountingFill
    idempotent: bool

    def __post_init__(self) -> None:
        if type(self.state) is not AccountingState:
            raise TypeError("state must be exactly AccountingState")
        if type(self.fill) is not AccountingFill:
            raise TypeError("fill must be exactly AccountingFill")
        if not isinstance(self.idempotent, bool):
            raise TypeError("idempotent must be a bool")


def _money(value: Decimal, schedule: FeeSchedule) -> Decimal:
    return value.quantize(
        schedule.currency_quantum,
        rounding=schedule.fee_rounding_mode,
    )


def _consume_fifo(
    lots: tuple[AccountingLot, ...],
    *,
    instrument_id: str,
    quantity: int,
    trade_date: date,
) -> tuple[AccountingLot, ...]:
    remaining = quantity
    remaining_by_lot = {
        lot.lot_id: lot.remaining_quantity for lot in lots
    }
    for lot in sorted(lots, key=lambda item: (item.acquired_on, item.lot_id)):
        if (
            remaining == 0
            or lot.instrument_id != instrument_id
            or lot.sellable_on > trade_date
            or lot.remaining_quantity == 0
        ):
            continue
        consumed = min(remaining, lot.remaining_quantity)
        remaining -= consumed
        remaining_by_lot[lot.lot_id] -= consumed
    if remaining:
        raise AccountingInvariantError("sell lot consumption did not complete")
    return tuple(
        replace(lot, remaining_quantity=remaining_by_lot[lot.lot_id])
        for lot in lots
    )


@dataclass(frozen=True, slots=True)
class _AccountingReplay:
    cash_balance: Decimal
    fills: tuple[AccountingFill, ...]
    lots: tuple[AccountingLot, ...]
    cash_movements: tuple[AccountingCashMovement, ...]


def _replay_accounting_requests(
    *,
    opening_cash: Decimal,
    requests: tuple[AccountingFillRequest, ...],
    fee_schedules: tuple[FeeSchedule, ...],
) -> _AccountingReplay:
    """Reduce canonical requests without trusting any derived state fields."""

    schedules_by_hash = {
        fee_schedule_fingerprint(schedule): schedule
        for schedule in fee_schedules
    }
    if len(schedules_by_hash) != len(fee_schedules):
        raise AccountingInvariantError("fee schedule registry is not unique")
    cash_balance = _money(opening_cash, fee_schedules[0])
    fills: tuple[AccountingFill, ...] = ()
    lots: tuple[AccountingLot, ...] = ()
    movements: tuple[AccountingCashMovement, ...] = ()
    last_filled_at: datetime | None = None
    for request in requests:
        fee_schedule = schedules_by_hash.get(request.fee_schedule_hash)
        if fee_schedule is None:
            raise AccountingInvariantError(
                "request fee schedule is absent from registry"
            )
        schedule_hash = fee_schedule_fingerprint(fee_schedule)
        if request.fee_profile_version != fee_schedule.profile_version:
            raise AccountingInvariantError(
                "request fee profile differs from replay schedule"
            )
        if request.fee_schedule_hash != schedule_hash:
            raise AccountingInvariantError(
                "request fee schedule fingerprint differs"
            )
        if last_filled_at is not None and request.filled_at < last_filled_at:
            raise AccountingInvariantError(
                "fill requests must be ordered by filled_at"
            )
        last_filled_at = request.filled_at
        gross = _money(request.price * request.quantity, fee_schedule)
        if gross <= 0:
            raise AccountingInvariantError(
                "rounded fill notional must be positive"
            )
        prior_order_fills = tuple(
            item for item in fills if item.order_id == request.order_id
        )
        if any(
            item.side != request.side
            or item.instrument_id != request.instrument_id
            or item.fee_profile_version != request.fee_profile_version
            or item.fee_schedule_hash != schedule_hash
            for item in prior_order_fills
        ):
            raise AccountingInvariantError(
                "order fill identity changed across partial fills"
            )
        previous_notional = sum(
            (item.gross_amount for item in prior_order_fills),
            Decimal("0"),
        )
        previous_quantity = sum(item.quantity for item in prior_order_fills)
        fee_amount = incremental_order_fee_delta(
            side=request.side,
            schedule=fee_schedule,
            previous_notional=previous_notional,
            new_total_notional=previous_notional + gross,
            previous_quantity=previous_quantity,
            new_total_quantity=previous_quantity + request.quantity,
        ).total
        net_cash = _money(
            -(gross + fee_amount)
            if request.side == OrderSide.BUY
            else gross - fee_amount,
            fee_schedule,
        )
        next_cash = _money(cash_balance + net_cash, fee_schedule)
        if next_cash < 0:
            raise InsufficientCashError("fill would make cash negative")

        if request.side == OrderSide.BUY:
            assert request.settlement_evidence is not None
            lots = (
                *lots,
                AccountingLot(
                    lot_id=f"LOT:{request.fill_id}",
                    instrument_id=request.instrument_id,
                    opened_fill_id=request.fill_id,
                    acquired_on=request.trade_date,
                    sellable_on=request.settlement_date,
                    original_quantity=request.quantity,
                    remaining_quantity=request.quantity,
                    cost_price=request.price,
                    allocated_buy_fee=fee_amount,
                ),
            )
            settlement_evidence_hash = (
                request.settlement_evidence.evidence_hash
            )
        else:
            sellable = sum(
                lot.remaining_quantity
                for lot in lots
                if lot.instrument_id == request.instrument_id
                and lot.sellable_on <= request.trade_date
            )
            if sellable < request.quantity:
                raise InsufficientSellableQuantityError(
                    "T+N sellable quantity is insufficient"
                )
            lots = _consume_fifo(
                lots,
                instrument_id=request.instrument_id,
                quantity=request.quantity,
                trade_date=request.trade_date,
            )
            settlement_evidence_hash = None

        fill = AccountingFill(
            fill_id=request.fill_id,
            idempotency_key=request.idempotency_key,
            request_hash=request.request_hash,
            order_id=request.order_id,
            instrument_id=request.instrument_id,
            side=request.side,
            quantity=request.quantity,
            price=request.price,
            gross_amount=gross,
            fee_amount=fee_amount,
            net_cash_amount=net_cash,
            trade_date=request.trade_date,
            settlement_date=request.settlement_date,
            filled_at=request.filled_at,
            fee_profile_version=request.fee_profile_version,
            fee_schedule_hash=schedule_hash,
            settlement_evidence_hash=settlement_evidence_hash,
        )
        movement = AccountingCashMovement(
            movement_id=f"CASH:{request.fill_id}",
            business_key=f"FILL:{request.idempotency_key}",
            event_type=(
                "BUY_FILL" if request.side == OrderSide.BUY else "SELL_FILL"
            ),
            amount=net_cash,
            balance_after=next_cash,
            occurred_at=request.filled_at,
        )
        fills = (*fills, fill)
        movements = (*movements, movement)
        cash_balance = next_cash
    return _AccountingReplay(
        cash_balance=cash_balance,
        fills=fills,
        lots=lots,
        cash_movements=movements,
    )


def apply_fill(
    state: AccountingState,
    request: AccountingFillRequest,
    *,
    fee_schedule: FeeSchedule,
) -> AccountingApplyResult:
    """Apply one fill as a deterministic value transition."""

    if type(state) is not AccountingState:
        raise TypeError("state must be exactly AccountingState")
    if type(request) is not AccountingFillRequest:
        raise TypeError("request must be exactly AccountingFillRequest")
    if type(fee_schedule) is not FeeSchedule:
        raise TypeError("fee_schedule must be exactly FeeSchedule")
    _require_reconstructable(state, AccountingState, "accounting state")
    _require_reconstructable(request, AccountingFillRequest, "fill request")
    _require_reconstructable(fee_schedule, FeeSchedule, "fee schedule")
    if request.fee_profile_version != fee_schedule.profile_version:
        raise ValueError("fee profile version does not match fill request")
    schedule_hash = fee_schedule_fingerprint(fee_schedule)
    for registered in state.fee_schedules:
        if (
            registered.profile_version == fee_schedule.profile_version
            and fee_schedule_fingerprint(registered) != schedule_hash
        ):
            raise ValueError(
                "fee profile version is already bound to different parameters"
            )
    fee_schedules = state.fee_schedules
    if schedule_hash not in state.fee_schedule_hashes:
        fee_schedules = tuple(
            sorted(
                (*state.fee_schedules, fee_schedule),
                key=lambda item: (
                    item.profile_version,
                    fee_schedule_fingerprint(item),
                ),
            )
        )
    if request.fee_schedule_hash != schedule_hash:
        raise ValueError("fill request is bound to a different fee schedule")
    if state.currency_quantum != fee_schedule.currency_quantum:
        raise ValueError("accounting state currency quantum differs from fee schedule")
    if state.rounding_mode != fee_schedule.fee_rounding_mode:
        raise ValueError("accounting state rounding mode differs from fee schedule")
    if not fee_schedule.round_notional_before_fees:
        raise ValueError(
            "accounting transition currently requires per-fill notional rounding"
        )

    by_key = next(
        (item for item in state.fills if item.idempotency_key == request.idempotency_key),
        None,
    )
    by_id = next((item for item in state.fills if item.fill_id == request.fill_id), None)
    for existing in (by_key, by_id):
        if existing is None:
            continue
        if existing.request_hash != request.request_hash:
            raise AccountingIdempotencyConflict(
                "fill id or idempotency key was reused with different semantics"
            )
        return AccountingApplyResult(state=state, fill=existing, idempotent=True)

    requests = (*state.requests, request)
    replay = _replay_accounting_requests(
        opening_cash=state.opening_cash,
        requests=requests,
        fee_schedules=fee_schedules,
    )
    fill = replay.fills[-1]
    next_state = AccountingState(
        opening_cash=state.opening_cash,
        cash_balance=replay.cash_balance,
        fee_schedules=fee_schedules,
        currency_quantum=state.currency_quantum,
        rounding_mode=state.rounding_mode,
        requests=requests,
        fills=replay.fills,
        lots=replay.lots,
        cash_movements=replay.cash_movements,
    )
    return AccountingApplyResult(state=next_state, fill=fill, idempotent=False)


__all__ = [
    "AccountingApplyResult",
    "AccountingCashMovement",
    "AccountingError",
    "AccountingFill",
    "AccountingFillRequest",
    "AccountingIdempotencyConflict",
    "AccountingInvariantError",
    "AccountingLot",
    "AccountingState",
    "InsufficientCashError",
    "InsufficientSellableQuantityError",
    "SettlementEvidence",
    "apply_fill",
    "fee_schedule_fingerprint",
]
