"""Decimal, append-only paper ledger used by history and realtime matchers."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from .domain import OrderSide, decimal_value, money


@dataclass(frozen=True)
class FeeProfile:
    version: str
    buy_commission_rate: Decimal
    sell_commission_rate: Decimal
    minimum_commission: Decimal
    stamp_tax_sell_rate: Decimal
    transfer_fee_buy_rate: Decimal = Decimal("0")
    transfer_fee_sell_rate: Decimal = Decimal("0")
    other_buy_rate: Decimal = Decimal("0")
    other_sell_rate: Decimal = Decimal("0")
    other_buy_fixed: Decimal = Decimal("0")
    other_sell_fixed: Decimal = Decimal("0")
    other_buy_per_share: Decimal = Decimal("0")
    other_sell_per_share: Decimal = Decimal("0")

    def calculate(
        self,
        side: OrderSide,
        gross_amount: Decimal,
        *,
        quantity: int = 0,
    ) -> Decimal:
        gross_amount = decimal_value(gross_amount)
        if gross_amount <= 0:
            return Decimal("0.00")
        commission_rate = (
            self.buy_commission_rate
            if side == OrderSide.BUY
            else self.sell_commission_rate
        )
        commission = max(self.minimum_commission, gross_amount * commission_rate)
        transfer_rate = (
            self.transfer_fee_buy_rate
            if side == OrderSide.BUY
            else self.transfer_fee_sell_rate
        )
        stamp = gross_amount * self.stamp_tax_sell_rate if side == OrderSide.SELL else Decimal("0")
        other_rate = (
            self.other_buy_rate
            if side == OrderSide.BUY
            else self.other_sell_rate
        )
        other_fixed = (
            self.other_buy_fixed
            if side == OrderSide.BUY
            else self.other_sell_fixed
        )
        other_per_share = (
            self.other_buy_per_share
            if side == OrderSide.BUY
            else self.other_sell_per_share
        )
        return money(
            commission
            + gross_amount * transfer_rate
            + stamp
            + gross_amount * other_rate
            + other_fixed
            + Decimal(max(0, int(quantity))) * other_per_share
        )

    def calculate_incremental(
        self,
        side: OrderSide,
        *,
        previous_gross: Decimal,
        fill_gross: Decimal,
        previous_quantity: int = 0,
        fill_quantity: int = 0,
    ) -> Decimal:
        """Charge only the order-level fee delta on a partial fill."""
        before = self.calculate(
            side,
            previous_gross,
            quantity=previous_quantity,
        )
        after = self.calculate(
            side,
            decimal_value(previous_gross) + decimal_value(fill_gross),
            quantity=previous_quantity + fill_quantity,
        )
        return money(after - before)


@dataclass(frozen=True)
class LedgerFill:
    fill_id: str
    idempotency_key: str
    order_id: str
    stock_code: str
    side: OrderSide
    quantity: int
    price: Decimal
    gross_amount: Decimal
    fee_amount: Decimal
    net_cash_amount: Decimal
    trade_date: date
    filled_at: datetime


@dataclass
class LedgerLot:
    lot_id: str
    stock_code: str
    opened_fill_id: str
    opened_trade_date: date
    settlement_date: date
    original_quantity: int
    remaining_quantity: int
    cost_price: Decimal
    allocated_buy_fee: Decimal


@dataclass(frozen=True)
class CashEvent:
    event_id: str
    business_key: str
    event_type: str
    amount: Decimal
    balance_after: Decimal
    occurred_at: datetime


class LedgerBook:
    """In-memory deterministic ledger core.

    The database writer persists these immutable events in one transaction.
    This class contains the accounting rules shared by historical replay and
    realtime paper matching.
    """

    def __init__(self, *, initial_cash: Decimal):
        self.initial_cash = money(initial_cash)
        self.cash_balance = self.initial_cash
        self.fills: list[LedgerFill] = []
        self.lots: list[LedgerLot] = []
        self.cash_events: list[CashEvent] = [
            CashEvent(
                event_id="INITIAL_DEPOSIT",
                business_key="INITIAL_DEPOSIT",
                event_type="INITIAL_DEPOSIT",
                amount=self.initial_cash,
                balance_after=self.initial_cash,
                occurred_at=datetime.min,
            )
        ]
        self._fill_keys = set()
        self._cash_keys = {"INITIAL_DEPOSIT"}

    def available_to_sell(self, stock_code: str, trade_date: date) -> int:
        return sum(
            lot.remaining_quantity
            for lot in self.lots
            if lot.stock_code == stock_code
            and lot.remaining_quantity > 0
            and lot.settlement_date <= trade_date
        )

    def position_quantity(self, stock_code: str) -> int:
        return sum(
            lot.remaining_quantity
            for lot in self.lots
            if lot.stock_code == stock_code
        )

    def apply_fill(
        self,
        *,
        fill_id: str,
        idempotency_key: str,
        order_id: str,
        stock_code: str,
        side: OrderSide,
        quantity: int,
        price: Decimal,
        trade_date: date,
        filled_at: datetime,
        fee_profile: FeeProfile,
        settlement_date: date,
    ) -> LedgerFill:
        if idempotency_key in self._fill_keys:
            return next(item for item in self.fills if item.idempotency_key == idempotency_key)
        if quantity <= 0 or decimal_value(price) <= 0:
            raise ValueError("fill quantity and price must be positive")
        gross = money(decimal_value(price) * quantity)
        prior_order_fills = [
            item
            for item in self.fills
            if item.order_id == order_id and item.side == side
        ]
        fee = fee_profile.calculate_incremental(
            side,
            previous_gross=sum(
                (item.gross_amount for item in prior_order_fills),
                Decimal("0"),
            ),
            fill_gross=gross,
            previous_quantity=sum(
                item.quantity for item in prior_order_fills
            ),
            fill_quantity=quantity,
        )
        if side == OrderSide.BUY:
            cash_amount = money(-(gross + fee))
            if self.cash_balance + cash_amount < 0:
                raise ValueError("negative cash is forbidden")
        else:
            if self.available_to_sell(stock_code, trade_date) < quantity:
                raise ValueError("T+N available quantity is insufficient")
            cash_amount = money(gross - fee)

        next_cash = money(self.cash_balance + cash_amount)
        fill = LedgerFill(
            fill_id=fill_id,
            idempotency_key=idempotency_key,
            order_id=order_id,
            stock_code=stock_code,
            side=side,
            quantity=quantity,
            price=decimal_value(price),
            gross_amount=gross,
            fee_amount=fee,
            net_cash_amount=cash_amount,
            trade_date=trade_date,
            filled_at=filled_at,
        )
        if side == OrderSide.BUY:
            self.lots.append(
                LedgerLot(
                    lot_id=f"LOT:{fill_id}",
                    stock_code=stock_code,
                    opened_fill_id=fill_id,
                    opened_trade_date=trade_date,
                    settlement_date=settlement_date,
                    original_quantity=quantity,
                    remaining_quantity=quantity,
                    cost_price=decimal_value(price),
                    allocated_buy_fee=fee,
                )
            )
        else:
            remaining = quantity
            for lot in sorted(
                self.lots,
                key=lambda item: (item.opened_trade_date, item.lot_id),
            ):
                if (
                    lot.stock_code != stock_code
                    or lot.remaining_quantity <= 0
                    or lot.settlement_date > trade_date
                ):
                    continue
                consumed = min(remaining, lot.remaining_quantity)
                lot.remaining_quantity -= consumed
                remaining -= consumed
                if remaining == 0:
                    break
            if remaining:
                raise AssertionError("sell lot consumption invariant failed")
        cash_event = CashEvent(
            event_id=f"CASH:{fill_id}",
            business_key=f"FILL:{idempotency_key}",
            event_type="BUY_FILL" if side == OrderSide.BUY else "SELL_FILL",
            amount=cash_amount,
            balance_after=next_cash,
            occurred_at=filled_at,
        )
        self.cash_balance = next_cash
        self.fills.append(fill)
        self.cash_events.append(cash_event)
        self._fill_keys.add(idempotency_key)
        self._cash_keys.add(cash_event.business_key)
        return fill

    def equity(self, prices: dict[str, Decimal]) -> Decimal:
        market_value = sum(
            decimal_value(prices[lot.stock_code]) * lot.remaining_quantity
            for lot in self.lots
            if lot.remaining_quantity > 0 and lot.stock_code in prices
        )
        return money(self.cash_balance + market_value)

    def reconcile(self) -> dict[str, Any]:
        cash_from_events = money(sum((item.amount for item in self.cash_events), Decimal("0")))
        fill_position: dict[str, int] = {}
        for fill in self.fills:
            sign = 1 if fill.side == OrderSide.BUY else -1
            fill_position[fill.stock_code] = fill_position.get(fill.stock_code, 0) + sign * fill.quantity
        lot_position: dict[str, int] = {}
        for lot in self.lots:
            lot_position[lot.stock_code] = lot_position.get(lot.stock_code, 0) + lot.remaining_quantity
        cash_difference = money(self.cash_balance - cash_from_events)
        position_difference = sum(
            abs(fill_position.get(code, 0) - lot_position.get(code, 0))
            for code in set(fill_position) | set(lot_position)
        )
        duplicate_fill_count = len(self.fills) - len({item.idempotency_key for item in self.fills})
        checks = {
            "cash_difference": str(cash_difference),
            "position_difference": position_difference,
            "duplicate_fill_count": duplicate_fill_count,
            "negative_cash": self.cash_balance < 0,
        }
        passed = (
            abs(cash_difference) <= Decimal("0.01")
            and position_difference == 0
            and duplicate_fill_count == 0
            and self.cash_balance >= 0
        )
        return {"status": "PASS" if passed else "RECONCILIATION_BLOCKED", **checks}

    def snapshot(self) -> dict[str, Any]:
        return {
            "initial_cash": str(self.initial_cash),
            "cash_balance": str(self.cash_balance),
            "fills": [asdict(item) for item in self.fills],
            "lots": [asdict(item) for item in self.lots],
            "cash_events": [asdict(item) for item in self.cash_events],
            "reconciliation": self.reconcile(),
        }
