"""Read-only mapping of the frozen V2 paper snapshot fallback.

The adapter is a differential characterization boundary only.  It has no
repository, engine, ledger writer, broker, or order-submission dependency.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, tzinfo
from decimal import Decimal

from server.trading_core.contracts import OrderSide as NeutralOrderSide
from server.trading_core.contracts import OrderStatus
from server.trading_core.execution import (
    AttestedSnapshotQuote,
    LimitDayOrder,
    MatchDecision,
    MatchPriceBand,
    MatchReason,
    SnapshotEvidenceKind,
    SnapshotLiquidityEvidence,
    SnapshotMatchRule,
    match_attested_snapshot,
    snapshot_attestation_hash,
)
from server.trading_v2.domain import OrderSide as V2OrderSide
from server.trading_v2.domain import Quote as V2Quote
from server.trading_v2.config import canonical_json_hash
from server.trading_v2.policy import PortfolioPolicy

from .matcher import V2_DEFAULT_TIMEZONE, V2MatchProjection


V2_SNAPSHOT_SOURCE = "v2-paper-snapshot"


@dataclass(frozen=True, slots=True)
class V2NeutralSnapshotMatcherInput:
    order: LimitDayOrder
    quote: AttestedSnapshotQuote | None
    rule: SnapshotMatchRule
    evaluated_at: datetime


def _aware(value: object, *, assume_timezone: tzinfo) -> datetime:
    if type(value) is not datetime:
        raise TypeError("V2 snapshot times must be exact datetimes")
    if not isinstance(assume_timezone, tzinfo):
        raise TypeError("assume_timezone must be a tzinfo")
    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=assume_timezone)
    if value.utcoffset() is None:
        raise ValueError("V2 snapshot time could not be made timezone-aware")
    return value


def _integer(
    value: object,
    field_name: str,
    *,
    minimum: int | None = 0,
) -> int:
    if type(value) is not int:
        raise TypeError(f"{field_name} must be exactly int")
    if minimum is not None and value < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}")
    return value


def map_v2_snapshot_match_inputs(
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
    order_id: str = "v2-read-only-snapshot-order",
    intent_id: str = "v2-read-only-snapshot-intent",
    prior_filled_quantity: int = 0,
    last_source_sequence: int = 0,
    assume_timezone: tzinfo = V2_DEFAULT_TIMEZONE,
) -> V2NeutralSnapshotMatcherInput:
    if type(policy) is not PortfolioPolicy:
        raise TypeError("policy must be exactly V2 PortfolioPolicy")
    side = V2OrderSide(side)
    # Frozen V2 clamps these values only when it computes the fill.  The
    # neutral order contract requires a positive outstanding quantity, so the
    # read-only adapter uses one inert unit when V2's remaining value is <= 0
    # and forces the synthetic compatibility cap to zero below.
    v2_remaining = _integer(
        remaining_quantity,
        "remaining_quantity",
        minimum=None,
    )
    v2_approved_remaining = _integer(
        approved_remaining_quantity,
        "approved_remaining_quantity",
        minimum=None,
    )
    remaining = max(1, v2_remaining)
    approved_remaining = max(0, v2_approved_remaining)
    prior_filled = _integer(
        prior_filled_quantity,
        "prior_filled_quantity",
    )
    sequence = _integer(last_source_sequence, "last_source_sequence")
    if type(liquidity_quantity) is not int:
        raise TypeError("liquidity_quantity must be exactly int")

    evaluated_at = _aware(now, assume_timezone=assume_timezone)
    observed_at = evaluated_at
    neutral_quote = None
    band = None
    instrument_id = "V2-UNKNOWN-INSTRUMENT"
    if quote is not None:
        if type(quote) is not V2Quote:
            raise TypeError("quote must be exactly V2 Quote or None")
        instrument_id = quote.stock_code
        observed_at = _aware(
            quote.quote_at,
            assume_timezone=assume_timezone,
        )
        received_at = _aware(
            quote.received_at,
            assume_timezone=assume_timezone,
        )
        compatibility_quantity = (
            max(0, liquidity_quantity) if v2_remaining > 0 else 0
        )
        synthetic_content_hash = canonical_json_hash(
            {
                "compatibility_only": True,
                "instrument_id": instrument_id,
                "snapshot_id": quote.event_id,
                "observed_at": observed_at,
                "received_at": received_at,
                "last_price": quote.last_price,
                "lower_limit": quote.lower_limit,
                "upper_limit": quote.upper_limit,
                "suspended": quote.suspended,
                "liquidity_quantity": compatibility_quantity,
            }
        )
        liquidity_evidence = SnapshotLiquidityEvidence(
            evidence_kind=(
                SnapshotEvidenceKind.SYNTHETIC_STANDALONE_COMPATIBILITY
            ),
            source_provider=V2_SNAPSHOT_SOURCE,
            source_batch_id=f"compat:{quote.event_id}",
            # This is a locally computed content digest.  It is not a receipt
            # and does not authenticate the V2 quote's provider.
            source_payload_hash=synthetic_content_hash,
            source_receipt_hash=None,
            quality_status="NOT_ASSESSED",
            source_count=1,
            standalone_compatibility_quantity=compatibility_quantity,
        )
        attestation = snapshot_attestation_hash(
            instrument_id=instrument_id,
            snapshot_id=quote.event_id,
            observed_at=observed_at,
            received_at=received_at,
            last_price=quote.last_price,
            source=V2_SNAPSHOT_SOURCE,
            liquidity_evidence_hash=liquidity_evidence.evidence_hash,
            suspended=quote.suspended,
        )
        neutral_quote = AttestedSnapshotQuote(
            instrument_id=instrument_id,
            snapshot_id=quote.event_id,
            observed_at=observed_at,
            received_at=received_at,
            last_price=quote.last_price,
            source=V2_SNAPSHOT_SOURCE,
            attestation_hash=attestation,
            liquidity_evidence=liquidity_evidence,
            suspended=quote.suspended,
        )
        if quote.lower_limit is not None or quote.upper_limit is not None:
            band = MatchPriceBand(
                instrument_id=instrument_id,
                trade_date=observed_at.date(),
                as_of=observed_at,
                source="v2-snapshot-event",
                lower=quote.lower_limit,
                upper=quote.upper_limit,
            )

    earliest_at = min(evaluated_at, observed_at)
    expires_at = max(evaluated_at, observed_at) + timedelta(days=1)
    requested_quantity = prior_filled + remaining
    approved_quantity = prior_filled + min(remaining, approved_remaining)
    order = LimitDayOrder(
        order_id=order_id,
        intent_id=intent_id,
        instrument_id=instrument_id,
        side=(
            NeutralOrderSide.BUY
            if side == V2OrderSide.BUY
            else NeutralOrderSide.SELL
        ),
        requested_quantity=requested_quantity,
        approved_quantity=approved_quantity,
        cumulative_filled_quantity=prior_filled,
        limit_price=limit_price,
        earliest_at=earliest_at,
        expires_at=expires_at,
        updated_at=earliest_at,
        last_source_sequence=sequence,
        status=(
            OrderStatus.PARTIALLY_FILLED
            if prior_filled
            else OrderStatus.QUEUED
        ),
    )
    rule = SnapshotMatchRule(
        rule_version=f"v2-snapshot:{policy.version}:{policy.config_hash}",
        enabled=policy.paper_snapshot_fallback,
        tick_size=tick_size,
        quote_max_age=timedelta(
            seconds=policy.paper_snapshot_max_age_seconds
        ),
        allowed_sources=(V2_SNAPSHOT_SOURCE,),
        allow_synthetic_compatibility_evidence=True,
        slippage_rate=policy.paper_snapshot_slippage_rate,
        price_band=band,
        price_band_max_age=(
            timedelta(seconds=policy.paper_snapshot_max_age_seconds)
            if band is not None
            else None
        ),
        # These flags explicitly reproduce frozen V2 behavior for the golden
        # differential.  New neutral callers retain fail-closed defaults.
        require_complete_price_band=False,
        enforce_price_band_bounds=False,
        block_adverse_limit_lock=True,
    )
    return V2NeutralSnapshotMatcherInput(
        order=order,
        quote=neutral_quote,
        rule=rule,
        evaluated_at=evaluated_at,
    )


def project_neutral_snapshot_match_to_v2(
    decision: MatchDecision,
) -> V2MatchProjection:
    if type(decision) is not MatchDecision:
        raise TypeError("decision must be exactly MatchDecision")
    reason = decision.reason.value
    if decision.reason in {
        MatchReason.WAIT_FUTURE_QUOTE,
        MatchReason.WAIT_PRE_ORDER_QUOTE,
        MatchReason.WAIT_OUT_OF_ORDER_QUOTE,
    }:
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


def match_v2_snapshot_read_only(**kwargs: object) -> V2MatchProjection:
    inputs = map_v2_snapshot_match_inputs(**kwargs)  # type: ignore[arg-type]
    decision = match_attested_snapshot(
        order=inputs.order,
        quote=inputs.quote,
        rule=inputs.rule,
        evaluated_at=inputs.evaluated_at,
    )
    return project_neutral_snapshot_match_to_v2(decision)


__all__ = [
    "V2NeutralSnapshotMatcherInput",
    "V2_SNAPSHOT_SOURCE",
    "map_v2_snapshot_match_inputs",
    "match_v2_snapshot_read_only",
    "project_neutral_snapshot_match_to_v2",
]
