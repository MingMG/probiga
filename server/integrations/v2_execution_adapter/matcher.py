"""Read-only mapping of frozen V2 Level-1 matcher inputs to trading_core.

This module has no repository, execution-service, ledger, broker, or database
dependency.  It is a characterization/differential entry point only; invoking
it cannot submit an order or mutate the canonical V2 paper account.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, tzinfo
from decimal import Decimal
from zoneinfo import ZoneInfo

from server.trading_core.contracts import OrderSide as NeutralOrderSide
from server.trading_core.contracts import OrderStatus
from server.trading_core.execution.matcher import (
    Level1Quote,
    LimitDayMatchRule,
    LimitDayOrder,
    MatchDecision,
    MatchPriceBand,
    MatchReason,
    match_limit_day,
)
from server.trading_v2.domain import OrderSide as V2OrderSide
from server.trading_v2.domain import Quote as V2Quote
from server.trading_v2.policy import PortfolioPolicy


V2_DEFAULT_TIMEZONE = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class V2NeutralMatcherInput:
    order: LimitDayOrder
    quote: Level1Quote | None
    rule: LimitDayMatchRule
    evaluated_at: datetime


@dataclass(frozen=True)
class V2MatchProjection:
    """The observable fields shared by V2 MatchResult and the neutral matcher."""

    status: str
    waiting_reason: str
    fill_quantity: int
    fill_price: Decimal | None
    event_id: str


def _aware(value: datetime, *, assume_timezone: tzinfo) -> datetime:
    if type(value) is not datetime:
        raise TypeError("V2 matcher times must be exact datetimes")
    if not isinstance(assume_timezone, tzinfo):
        raise TypeError("assume_timezone must be a tzinfo")
    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=assume_timezone)
    if value.utcoffset() is None:
        raise ValueError("V2 matcher time could not be made timezone-aware")
    return value


def _integer(value: object, field_name: str, *, minimum: int = 0) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field_name} must be an integer")
    if value < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}")
    return value


def map_v2_level1_match_inputs(
    *,
    side: V2OrderSide,
    remaining_quantity: int,
    approved_remaining_quantity: int,
    limit_price: Decimal,
    quote: V2Quote | None,
    now: datetime,
    tick_size: Decimal,
    liquidity_quantity: int,
    policy: PortfolioPolicy,
    slippage_rate: Decimal = Decimal("0"),
    impact_rate: Decimal = Decimal("0"),
    order_id: str = "v2-read-only-order",
    intent_id: str = "v2-read-only-intent",
    prior_filled_quantity: int = 0,
    last_source_sequence: int = 0,
    assume_timezone: tzinfo = V2_DEFAULT_TIMEZONE,
) -> V2NeutralMatcherInput:
    """Build a provably equivalent neutral input for one valid V2 matcher call.

    V2's standalone matcher receives remaining quantities rather than full
    order state.  The adapter reconstructs totals using
    ``prior_filled_quantity`` and clamps surplus approval because approval above
    the remaining order quantity is observationally irrelevant to V2's ``min``.
    """

    if not isinstance(policy, PortfolioPolicy):
        raise TypeError("policy must be a V2 PortfolioPolicy")
    side = V2OrderSide(side)
    remaining_quantity = _integer(
        remaining_quantity,
        "remaining_quantity",
        minimum=1,
    )
    approved_remaining_quantity = _integer(
        approved_remaining_quantity,
        "approved_remaining_quantity",
    )
    prior_filled_quantity = _integer(
        prior_filled_quantity,
        "prior_filled_quantity",
    )
    last_source_sequence = _integer(
        last_source_sequence,
        "last_source_sequence",
    )
    if not isinstance(liquidity_quantity, int) or isinstance(
        liquidity_quantity,
        bool,
    ):
        raise TypeError("liquidity_quantity must be an integer")

    evaluated_at = _aware(now, assume_timezone=assume_timezone)
    neutral_quote = None
    quote_time = evaluated_at
    band = None
    if quote is not None:
        if not isinstance(quote, V2Quote):
            raise TypeError("quote must be a V2 Quote or None")
        quote_time = _aware(quote.quote_at, assume_timezone=assume_timezone)
        received_at = _aware(
            quote.received_at,
            assume_timezone=assume_timezone,
        )
        neutral_quote = Level1Quote(
            instrument_id=quote.stock_code,
            quote_id=quote.event_id,
            observed_at=quote_time,
            received_at=received_at,
            bid_price=quote.bid1,
            bid_quantity=(
                None
                if quote.bid1_volume is None
                else max(0, int(quote.bid1_volume))
            ),
            ask_price=quote.ask1,
            ask_quantity=(
                None
                if quote.ask1_volume is None
                else max(0, int(quote.ask1_volume))
            ),
            suspended=quote.suspended,
        )
        if quote.lower_limit is not None or quote.upper_limit is not None:
            band = MatchPriceBand(
                instrument_id=quote.stock_code,
                trade_date=quote_time.date(),
                as_of=quote_time,
                source="v2-quote-event",
                lower=quote.lower_limit,
                upper=quote.upper_limit,
            )

    earliest_at = min(evaluated_at, quote_time)
    expires_at = max(evaluated_at, quote_time) + timedelta(days=1)
    requested_quantity = prior_filled_quantity + remaining_quantity
    approved_quantity = prior_filled_quantity + min(
        remaining_quantity,
        approved_remaining_quantity,
    )
    order = LimitDayOrder(
        order_id=order_id,
        intent_id=intent_id,
        instrument_id=(
            quote.stock_code if quote is not None else "V2-UNKNOWN-INSTRUMENT"
        ),
        side=(
            NeutralOrderSide.BUY
            if side == V2OrderSide.BUY
            else NeutralOrderSide.SELL
        ),
        requested_quantity=requested_quantity,
        approved_quantity=approved_quantity,
        cumulative_filled_quantity=prior_filled_quantity,
        limit_price=limit_price,
        earliest_at=earliest_at,
        expires_at=expires_at,
        updated_at=earliest_at,
        last_source_sequence=last_source_sequence,
        status=(
            OrderStatus.PARTIALLY_FILLED
            if prior_filled_quantity
            else OrderStatus.QUEUED
        ),
    )
    rule = LimitDayMatchRule(
        rule_version=f"v2-level1:{policy.version}:{policy.config_hash}",
        tick_size=tick_size,
        quote_max_age=timedelta(seconds=policy.quote_max_age_seconds),
        visible_volume_participation=policy.visible_level1_participation,
        maximum_fill_quantity=max(0, liquidity_quantity),
        price_band=band,
        price_band_max_age=(
            timedelta(seconds=policy.quote_max_age_seconds)
            if band is not None
            else None
        ),
        # V2 accepts absent/one-sided bands and uses them only for adverse-edge
        # locks.  These two flags make that legacy behavior explicit instead of
        # weakening the neutral defaults.
        require_complete_price_band=False,
        enforce_price_band_bounds=False,
        slippage_rate=slippage_rate,
        impact_rate=impact_rate,
        block_adverse_limit_lock=True,
    )
    return V2NeutralMatcherInput(
        order=order,
        quote=neutral_quote,
        rule=rule,
        evaluated_at=evaluated_at,
    )


def project_neutral_match_to_v2(decision: MatchDecision) -> V2MatchProjection:
    if not isinstance(decision, MatchDecision):
        raise TypeError("decision must be a MatchDecision")
    reason = decision.reason.value
    if decision.reason == MatchReason.WAIT_FUTURE_QUOTE:
        # V2 groups future and stale quotes under WAIT_STALE_QUOTE.
        reason = "WAIT_STALE_QUOTE"
    if decision.status.value not in {
        "WAITING",
        "PARTIALLY_FILLED",
        "FILLED",
    }:
        raise ValueError(
            f"neutral status {decision.status.value} has no V2 matcher projection"
        )
    return V2MatchProjection(
        status=decision.status.value,
        waiting_reason=reason,
        fill_quantity=decision.fill_quantity,
        fill_price=decision.fill_price,
        event_id=decision.quote_id,
    )


def match_v2_level1_read_only(**kwargs: object) -> V2MatchProjection:
    """Map and evaluate a V2 Level-1 call without touching V2 execution state."""

    inputs = map_v2_level1_match_inputs(**kwargs)  # type: ignore[arg-type]
    decision = match_limit_day(
        order=inputs.order,
        quote=inputs.quote,
        rule=inputs.rule,
        evaluated_at=inputs.evaluated_at,
    )
    return project_neutral_match_to_v2(decision)


__all__ = [
    "V2MatchProjection",
    "V2NeutralMatcherInput",
    "map_v2_level1_match_inputs",
    "match_v2_level1_read_only",
    "project_neutral_match_to_v2",
    "V2_DEFAULT_TIMEZONE",
]
