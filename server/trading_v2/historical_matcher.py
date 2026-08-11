"""Deterministic next-session daily-bar matcher for V2 historical replay."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .domain import MatchResult, OrderSide, WaitingReason, adverse_price


@dataclass(frozen=True)
class DailyBar:
    event_id: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    upper_limit: Decimal | None
    lower_limit: Decimal | None
    suspended: bool = False


@dataclass(frozen=True)
class IntradayExitEvent:
    triggered: bool
    reason_code: str
    price: Decimal | None


class HistoricalDailyMatcher:
    """Match at the next trading-day open with adverse execution assumptions."""

    def match_open(
        self,
        *,
        side: OrderSide,
        remaining_quantity: int,
        approved_remaining_quantity: int,
        limit_price: Decimal,
        bar: DailyBar | None,
        tick_size: Decimal,
        slippage_rate: Decimal = Decimal("0"),
        impact_rate: Decimal = Decimal("0"),
        liquidity_quantity: int | None = None,
    ) -> MatchResult:
        if bar is None:
            return self._wait(
                WaitingReason.WAIT_NO_QUOTE,
                "next-session daily bar is missing",
            )
        if bar.suspended or bar.volume <= 0 or bar.open <= 0:
            return self._wait(
                WaitingReason.WAIT_SUSPENDED,
                "next-session open is not tradable",
                bar.event_id,
            )
        if side == OrderSide.BUY:
            if bar.upper_limit is not None and bar.open >= bar.upper_limit:
                return self._wait(
                    WaitingReason.WAIT_LIMIT_LOCK,
                    "buy open is locked at upper limit",
                    bar.event_id,
                )
            adjusted = bar.open * (
                Decimal("1") + slippage_rate + impact_rate
            )
            fill_price = adverse_price(adjusted, tick_size, side=side)
            if bar.open > limit_price or fill_price > limit_price:
                return self._wait(
                    WaitingReason.WAIT_LIQUIDITY,
                    "open/adverse buy price exceeds order limit",
                    bar.event_id,
                )
        else:
            if bar.lower_limit is not None and bar.open <= bar.lower_limit:
                return self._wait(
                    WaitingReason.WAIT_LIMIT_LOCK,
                    "sell open is locked at lower limit",
                    bar.event_id,
                )
            adjusted = bar.open * (
                Decimal("1") - slippage_rate - impact_rate
            )
            fill_price = adverse_price(adjusted, tick_size, side=side)
            if bar.open < limit_price or fill_price < limit_price:
                return self._wait(
                    WaitingReason.WAIT_LIQUIDITY,
                    "open/adverse sell price is below order limit",
                    bar.event_id,
                )
        capacity = (
            max(0, int(liquidity_quantity))
            if liquidity_quantity is not None
            else max(0, int(bar.volume))
        )
        quantity = min(
            max(0, int(remaining_quantity)),
            max(0, int(approved_remaining_quantity)),
            capacity,
        )
        if quantity <= 0:
            return self._wait(
                WaitingReason.WAIT_LIQUIDITY,
                "historical executable quantity is zero",
                bar.event_id,
            )
        return MatchResult(
            status=(
                "FILLED"
                if quantity == remaining_quantity
                else "PARTIALLY_FILLED"
            ),
            waiting_reason="",
            fill_quantity=quantity,
            fill_price=fill_price,
            event_id=bar.event_id,
            explanation="matched at next-session open with adverse costs",
        )

    @staticmethod
    def resolve_long_exit(
        *,
        bar: DailyBar,
        protective_stop: Decimal | None,
        target_price: Decimal | None,
        tick_size: Decimal,
    ) -> IntradayExitEvent:
        """Use the protective stop first when one bar touches stop and target."""
        stop_hit = (
            protective_stop is not None
            and protective_stop > 0
            and bar.low <= protective_stop
        )
        target_hit = (
            target_price is not None
            and target_price > 0
            and bar.high >= target_price
        )
        if stop_hit:
            # A gap below the stop is executed at the worse opening price.
            raw = min(bar.open, protective_stop)
            return IntradayExitEvent(
                triggered=True,
                reason_code=(
                    "STOP_BEFORE_TARGET_CONSERVATIVE"
                    if target_hit
                    else "PROTECTIVE_STOP"
                ),
                price=adverse_price(raw, tick_size, side=OrderSide.SELL),
            )
        if target_hit:
            return IntradayExitEvent(
                triggered=True,
                reason_code="TARGET_REACHED",
                price=adverse_price(
                    target_price,
                    tick_size,
                    side=OrderSide.SELL,
                ),
            )
        return IntradayExitEvent(
            triggered=False,
            reason_code="NO_EXIT_EVENT",
            price=None,
        )

    @staticmethod
    def _wait(
        reason: WaitingReason,
        explanation: str,
        event_id: str = "",
    ) -> MatchResult:
        return MatchResult(
            status="WAITING",
            waiting_reason=reason.value,
            fill_quantity=0,
            fill_price=None,
            event_id=event_id,
            explanation=explanation,
        )
