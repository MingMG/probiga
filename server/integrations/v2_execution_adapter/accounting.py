"""Pure V2 fee/accounting mapping with no database or order submission I/O."""
from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from typing import Sequence
from zoneinfo import ZoneInfo

from server.trading_core.accounting import (
    AccountingApplyResult,
    AccountingFillRequest,
    AccountingState,
    SettlementEvidence,
    apply_fill,
    fee_schedule_fingerprint,
)
from server.trading_core.contracts import OrderSide
from server.trading_v2.config import canonical_json_hash
from server.trading_v2.domain import InstrumentRule
from server.trading_v2.ledger import FeeProfile

from .fees import v2_fee_profile_to_neutral_schedule


V2_MARKET_TIMEZONE = "Asia/Shanghai"


def empty_v2_accounting_state(
    initial_cash: object,
    *,
    fee_profile: FeeProfile,
) -> AccountingState:
    """Create an empty state using the frozen V2 money arithmetic."""

    if type(fee_profile) is not FeeProfile:
        raise TypeError("fee_profile must be exactly V2 FeeProfile")
    schedule = v2_fee_profile_to_neutral_schedule(fee_profile)
    return AccountingState.empty(
        initial_cash,  # type: ignore[arg-type]
        fee_schedule=schedule,
    )


def build_v2_accounting_fill_request(
    *,
    fill_id: str,
    idempotency_key: str,
    order_id: str,
    instrument_id: str,
    side: OrderSide,
    quantity: int,
    price: Decimal,
    trade_date: date,
    filled_at: datetime,
    fee_profile: FeeProfile,
    trading_days: Sequence[date],
    calendar_version: str,
    instrument_rule: InstrumentRule,
) -> AccountingFillRequest:
    """Bind a fill to frozen V2 fee and calendar/rule evidence."""

    if type(fee_profile) is not FeeProfile:
        raise TypeError("fee_profile must be exactly V2 FeeProfile")
    if type(instrument_rule) is not InstrumentRule:
        raise TypeError("instrument_rule must be exactly V2 InstrumentRule")
    if instrument_rule.stock_code != instrument_id:
        raise ValueError("instrument rule does not match instrument_id")
    if not (
        instrument_rule.effective_from <= trade_date
        and (
            instrument_rule.effective_to is None
            or trade_date <= instrument_rule.effective_to
        )
    ):
        raise ValueError("instrument rule is not effective on trade_date")
    if instrument_rule.fee_profile_version != fee_profile.version:
        raise ValueError("instrument rule and fee profile versions differ")
    side = OrderSide(side)
    if type(filled_at) is not datetime:
        raise TypeError("filled_at must be exactly datetime")
    if filled_at.tzinfo is None or filled_at.utcoffset() is None:
        filled_at = filled_at.replace(tzinfo=ZoneInfo(V2_MARKET_TIMEZONE))
    schedule = v2_fee_profile_to_neutral_schedule(fee_profile)
    settlement_evidence = None
    if side == OrderSide.BUY:
        settlement_evidence = SettlementEvidence(
            instrument_id=instrument_id,
            instrument_rule_version=instrument_rule.rule_version,
            instrument_rule_hash=canonical_json_hash(asdict(instrument_rule)),
            trade_date=trade_date,
            settlement_days=instrument_rule.settlement_days,
            trading_days=tuple(trading_days),
            calendar_version=calendar_version,
            market_timezone=V2_MARKET_TIMEZONE,
        )
    return AccountingFillRequest(
        fill_id=fill_id,
        idempotency_key=idempotency_key,
        order_id=order_id,
        instrument_id=instrument_id,
        side=side,
        quantity=quantity,
        price=price,
        trade_date=trade_date,
        filled_at=filled_at,
        fee_profile_version=schedule.profile_version,
        fee_schedule_hash=fee_schedule_fingerprint(schedule),
        market_timezone=V2_MARKET_TIMEZONE,
        settlement_evidence=settlement_evidence,
    )


def apply_v2_compatible_fill(
    state: AccountingState,
    request: AccountingFillRequest,
    *,
    fee_profile: FeeProfile,
) -> AccountingApplyResult:
    """Calculate a V2-compatible accounting transition without persisting it."""

    if type(fee_profile) is not FeeProfile:
        raise TypeError("fee_profile must be exactly V2 FeeProfile")
    return apply_fill(
        state,
        request,
        fee_schedule=v2_fee_profile_to_neutral_schedule(fee_profile),
    )


__all__ = [
    "V2_MARKET_TIMEZONE",
    "apply_v2_compatible_fill",
    "build_v2_accounting_fill_request",
    "empty_v2_accounting_state",
]
