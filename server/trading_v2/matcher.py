"""Deterministic Level-1 paper matcher with no last-price fallback."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from .domain import (
    MatchResult,
    OrderSide,
    Quote,
    WaitingReason,
    adverse_price,
)
from .policy import PortfolioPolicy, load_portfolio_policy


class PaperMatcher:
    def __init__(self, policy: PortfolioPolicy | None = None):
        self.policy = policy or load_portfolio_policy()

    def match(
        self,
        *,
        side: OrderSide,
        remaining_quantity: int,
        approved_remaining_quantity: int,
        limit_price: Decimal,
        quote: Quote | None,
        now: datetime,
        tick_size: Decimal,
        liquidity_quantity: int,
        slippage_rate: Decimal = Decimal("0"),
        impact_rate: Decimal = Decimal("0"),
    ) -> MatchResult:
        if quote is None:
            return self._wait(WaitingReason.WAIT_NO_QUOTE, "Level-1 quote is missing")
        age = (now - quote.quote_at).total_seconds()
        if age < 0 or age > self.policy.quote_max_age_seconds:
            return self._wait(
                WaitingReason.WAIT_STALE_QUOTE,
                f"quote age {age:.3f}s exceeds limit",
                quote.event_id,
            )
        if quote.suspended:
            return self._wait(WaitingReason.WAIT_SUSPENDED, "instrument is suspended", quote.event_id)

        if side == OrderSide.BUY:
            base_price = quote.ask1
            visible = quote.ask1_volume
            if base_price is None or visible is None or visible <= 0:
                return self._wait(WaitingReason.WAIT_NO_QUOTE, "ask1 price/volume is missing", quote.event_id)
            if quote.upper_limit is not None and base_price >= quote.upper_limit:
                return self._wait(WaitingReason.WAIT_LIMIT_LOCK, "buy side is locked at upper limit", quote.event_id)
            adjusted = base_price * (Decimal("1") + slippage_rate + impact_rate)
            fill_price = adverse_price(adjusted, tick_size, side=side)
            if base_price > limit_price or fill_price > limit_price:
                return self._wait(WaitingReason.WAIT_LIQUIDITY, "ask1/adverse price exceeds order limit", quote.event_id)
        else:
            base_price = quote.bid1
            visible = quote.bid1_volume
            if base_price is None or visible is None or visible <= 0:
                return self._wait(WaitingReason.WAIT_NO_QUOTE, "bid1 price/volume is missing", quote.event_id)
            if quote.lower_limit is not None and base_price <= quote.lower_limit:
                return self._wait(WaitingReason.WAIT_LIMIT_LOCK, "sell side is locked at lower limit", quote.event_id)
            adjusted = base_price * (Decimal("1") - slippage_rate - impact_rate)
            fill_price = adverse_price(adjusted, tick_size, side=side)
            if base_price < limit_price or fill_price < limit_price:
                return self._wait(WaitingReason.WAIT_LIQUIDITY, "bid1/adverse price is below order limit", quote.event_id)

        visible_cap = int(Decimal(visible) * self.policy.visible_level1_participation)
        quantity = min(
            max(0, int(remaining_quantity)),
            max(0, int(approved_remaining_quantity)),
            max(0, visible_cap),
            max(0, int(liquidity_quantity)),
        )
        if quantity <= 0:
            return self._wait(WaitingReason.WAIT_LIQUIDITY, "executable quantity is zero", quote.event_id)
        return MatchResult(
            status="FILLED" if quantity == remaining_quantity else "PARTIALLY_FILLED",
            waiting_reason="",
            fill_quantity=quantity,
            fill_price=fill_price,
            event_id=quote.event_id,
            explanation="matched against fresh Level-1 opposing quote",
        )

    @staticmethod
    def _wait(reason: WaitingReason, explanation: str, event_id: str = "") -> MatchResult:
        return MatchResult(
            status="WAITING",
            waiting_reason=reason.value,
            fill_quantity=0,
            fill_price=None,
            event_id=event_id,
            explanation=explanation,
        )


class PaperSnapshotMatcher:
    """Conservative ProBigA-only fallback when reliable Level-1 is unavailable."""

    def __init__(self, policy: PortfolioPolicy | None = None):
        self.policy = policy or load_portfolio_policy()

    def match(
        self,
        *,
        side: OrderSide,
        remaining_quantity: int,
        approved_remaining_quantity: int,
        limit_price: Decimal,
        quote: Quote | None,
        now: datetime,
        tick_size: Decimal,
        liquidity_quantity: int,
    ) -> MatchResult:
        if not self.policy.paper_snapshot_fallback:
            return PaperMatcher._wait(
                WaitingReason.WAIT_NO_QUOTE,
                "paper snapshot fallback is disabled",
            )
        if quote is None or quote.last_price is None:
            return PaperMatcher._wait(
                WaitingReason.WAIT_NO_QUOTE,
                "fresh paper snapshot price is missing",
                quote.event_id if quote else "",
            )
        age = (now - quote.quote_at).total_seconds()
        if (
            age < 0
            or age > self.policy.paper_snapshot_max_age_seconds
        ):
            return PaperMatcher._wait(
                WaitingReason.WAIT_STALE_QUOTE,
                f"paper snapshot age {age:.3f}s exceeds limit",
                quote.event_id,
            )
        if quote.suspended:
            return PaperMatcher._wait(
                WaitingReason.WAIT_SUSPENDED,
                "instrument is suspended",
                quote.event_id,
            )
        base_price = quote.last_price
        if side == OrderSide.BUY:
            if (
                quote.upper_limit is not None
                and base_price >= quote.upper_limit
            ):
                return PaperMatcher._wait(
                    WaitingReason.WAIT_LIMIT_LOCK,
                    "paper snapshot is locked at upper limit",
                    quote.event_id,
                )
            adjusted = base_price * (
                Decimal("1") + self.policy.paper_snapshot_slippage_rate
            )
            fill_price = adverse_price(adjusted, tick_size, side=side)
            if fill_price > limit_price:
                return PaperMatcher._wait(
                    WaitingReason.WAIT_LIQUIDITY,
                    "snapshot price plus paper slippage exceeds order limit",
                    quote.event_id,
                )
        else:
            if (
                quote.lower_limit is not None
                and base_price <= quote.lower_limit
            ):
                return PaperMatcher._wait(
                    WaitingReason.WAIT_LIMIT_LOCK,
                    "paper snapshot is locked at lower limit",
                    quote.event_id,
                )
            adjusted = base_price * (
                Decimal("1") - self.policy.paper_snapshot_slippage_rate
            )
            fill_price = adverse_price(adjusted, tick_size, side=side)
            if fill_price < limit_price:
                return PaperMatcher._wait(
                    WaitingReason.WAIT_LIQUIDITY,
                    "snapshot price minus paper slippage is below order limit",
                    quote.event_id,
                )
        quantity = min(
            max(0, int(remaining_quantity)),
            max(0, int(approved_remaining_quantity)),
            max(0, int(liquidity_quantity)),
        )
        if quantity <= 0:
            return PaperMatcher._wait(
                WaitingReason.WAIT_LIQUIDITY,
                "paper snapshot executable quantity is zero",
                quote.event_id,
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
            event_id=quote.event_id,
            explanation=(
                "matched against a fresh attested market snapshot with "
                "frozen paper slippage; this is not a Level-1 or broker fill"
            ),
        )
