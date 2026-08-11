"""Deterministic synthetic scenario for isolated V2 evidence DB acceptance.

The scenario covers all five execution-evidence writers with content-hash-only
authority.  It supplies canonical V2 fact rows but never creates an account,
order, fill, cash, or risk ledger outside the existing V2 tables.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from decimal import Decimal
import hashlib
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from server.trading_v2.domain import OrderStatus
from server.trading_v2.execution_evidence import (
    AuthorityStatus,
    CanonicalJson,
    CashEventBinding,
    EvidenceProvenance,
    FillExecutionEvidence,
    HistoryOrigin,
    MarketCalendarEvidence,
    OrderTransitionEvidence,
    OrderTransitionKind,
    QuoteReceiptEvidence,
    QuoteReceiptType,
    validate_cash_event_binding,
    validate_fill_execution_evidence,
    validate_market_calendar_evidence,
    validate_order_transition_evidence,
    validate_quote_receipt_evidence,
)


ZONE = ZoneInfo("Asia/Shanghai")
TRADE_DATE = date(2026, 8, 3)
ACCOUNT_ID = "mysql57-paper-account"
CREATED_ORDER_ID = "mysql57-order-created"
FILL_ORDER_ID = "mysql57-order-fill"
FILL_ID = "mysql57-fill-1"
STOCK_CODE = "600000.SH"
QUOTE_EVENT_ID = "c" * 64
MATCH_EVENT_ID = "8" * 64
FEE_PROFILE_VERSION = "fee-mysql57-v1"
INSTRUMENT_RULE_VERSION = "rule-mysql57-v1"
CONFLICT_ACCOUNT_ID = "mysql57-conflict-account"
CONFLICT_FILL_ORDER_ID = "mysql57-conflict-order-fill"
CONFLICT_CREATED_ORDER_ID = "mysql57-conflict-order-created"
CONFLICT_FILL_ID = "mysql57-conflict-fill-1"
CONFLICT_QUOTE_EVENT_ID = "d" * 64
CONFLICT_MATCH_EVENT_ID = "9" * 64
CONFLICT_CASH_EVENT_ID = "mysql57-conflict-cash-genesis"


@dataclass(frozen=True, slots=True)
class CanonicalSeedRow:
    table: str
    values: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class BehavioralEvidenceCase:
    evidence_type: str
    table: str
    primary_column: str
    primary_value: str
    evidence: object
    update_guard_message: str
    delete_guard_message: str
    rollback_dependencies: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class BehavioralScenario:
    seed_rows: tuple[CanonicalSeedRow, ...]
    cases: tuple[BehavioralEvidenceCase, ...]


@dataclass(frozen=True, slots=True)
class ConflictingBehavioralEvidencePair:
    evidence_type: str
    table: str
    primary_column: str
    natural_key_columns: tuple[str, ...]
    natural_key_values: tuple[Any, ...]
    left: BehavioralEvidenceCase
    right: BehavioralEvidenceCase


@dataclass(frozen=True, slots=True)
class ConflictingDoubleWriterScenario:
    seed_rows: tuple[CanonicalSeedRow, ...]
    pairs: tuple[ConflictingBehavioralEvidencePair, ...]


def _aware(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 8, 3, hour, minute, second, tzinfo=ZONE)


def _naive(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 8, 3, hour, minute, second)


def _provenance() -> EvidenceProvenance:
    return EvidenceProvenance(
        history_origin=HistoryOrigin.START_AFTER_UNKNOWN,
        history_origin_id="mysql57-behavioral-cutover",
        history_origin_at=_aware(7),
        authority_status=AuthorityStatus.CONTENT_HASH_ONLY,
    )


def _calendar(
    *,
    trade_date: date = TRADE_DATE,
    calendar_version: str = "calendar-mysql57-v1",
) -> MarketCalendarEvidence:
    day_text = trade_date.isoformat()
    next_day_text = (trade_date + timedelta(days=1)).isoformat()
    value = MarketCalendarEvidence(
        market_code="SSE",
        trade_date=trade_date,
        calendar_version=calendar_version,
        market_timezone="Asia/Shanghai",
        calendar_payload=CanonicalJson.from_value(
            {
                "coverage_end_at": _aware(23, 59, 59),
                "coverage_start_at": _aware(0),
                "sessions": [
                    {
                        "session_id": "MORNING",
                        "opens_at": "09:30:00",
                        "closes_at": "11:30:00",
                    },
                    {
                        "session_id": "AFTERNOON",
                        "opens_at": "13:00:00",
                        "closes_at": "15:00:00",
                    },
                ],
                "trading_days": [day_text, next_day_text],
            }
        ),
        source_provider="synthetic-calendar-registry",
        source_payload=CanonicalJson.from_value(
            {
                "calendar_version": calendar_version,
                "market_code": "SSE",
                "published_at": _aware(7, 30),
                "trade_date": day_text,
            }
        ),
        available_at=_aware(8),
        provenance=_provenance(),
    )
    validate_market_calendar_evidence(value)
    return value


def _created_order_payload() -> CanonicalJson:
    return CanonicalJson.from_value(
        {
            "account_id": ACCOUNT_ID,
            "created_at": _aware(9),
            "earliest_at": _aware(9, 30),
            "expires_at": _aware(15),
            "idempotency_key": "1" * 64,
            "intent_id": "mysql57-intent-created",
            "limit_price": "10.000000",
            "order_id": CREATED_ORDER_ID,
            "order_type": "LIMIT",
            "quantity": 100,
            "side": "BUY",
            "stock_code": STOCK_CODE,
        }
    )


def _fill_order_payload() -> CanonicalJson:
    return CanonicalJson.from_value(
        {
            "account_id": ACCOUNT_ID,
            "created_at": _aware(9),
            "earliest_at": _aware(9, 30),
            "expires_at": _aware(15),
            "idempotency_key": "4" * 64,
            "intent_id": "mysql57-intent-fill",
            "limit_price": "10.000000",
            "order_id": FILL_ORDER_ID,
            "order_type": "LIMIT",
            "quantity": 100,
            "side": "BUY",
            "stock_code": STOCK_CODE,
        }
    )


def _quote_row_payload() -> dict[str, Any]:
    return {
        "ask1": "10.000000",
        "ask1_volume": 1200,
        "bid1": "9.990000",
        "bid1_volume": 1000,
        "created_at": _aware(10),
        "last_price": "10.000000",
        "lower_limit": "8.820000",
        "payload_hash": QUOTE_EVENT_ID,
        "pre_close": "9.800000",
        "quote_at": _aware(9, 59, 59),
        "quote_event_id": QUOTE_EVENT_ID,
        "received_at": _aware(10),
        "source_batch_id": "mysql57-quote-batch-1",
        "source_provider": "synthetic-quote-adapter",
        "stock_code": STOCK_CODE,
        "suspended": False,
        "upper_limit": "10.780000",
    }


def _quote() -> QuoteReceiptEvidence:
    receipt_payload = CanonicalJson.from_value(
        {
            "quote_event_id": QUOTE_EVENT_ID,
            "quote_row": _quote_row_payload(),
            "source_batch_id": "mysql57-quote-batch-1",
            "source_payload_hash": QUOTE_EVENT_ID,
            "source_provider": "synthetic-quote-adapter",
        }
    )
    value = QuoteReceiptEvidence(
        quote_event_id=QUOTE_EVENT_ID,
        stock_code=STOCK_CODE,
        trade_date=TRADE_DATE,
        market_timezone="Asia/Shanghai",
        quote_at=_aware(9, 59, 59),
        received_at=_aware(10),
        available_at=_aware(10, 0, 1),
        source_provider="synthetic-quote-adapter",
        source_batch_id="mysql57-quote-batch-1",
        source_payload_hash=QUOTE_EVENT_ID,
        receipt_type=QuoteReceiptType.OTHER,
        receipt_payload=receipt_payload,
        provenance=_provenance(),
        source_receipt_id="mysql57-synthetic-quote-receipt-1",
        source_receipt_hash="b" * 64,
    )
    validate_quote_receipt_evidence(value)
    return value


def _fill_payload() -> CanonicalJson:
    idempotency_key = hashlib.sha256(
        f"{FILL_ORDER_ID}|{QUOTE_EVENT_ID}|{MATCH_EVENT_ID}".encode("utf-8")
    ).hexdigest()
    return CanonicalJson.from_value(
        {
            "account_id": ACCOUNT_ID,
            "created_at": _aware(10, 0, 3),
            "fee_amount": "0.30",
            "fill_id": FILL_ID,
            "filled_at": _aware(10, 0, 2),
            "gross_amount": "1000.00",
            "idempotency_key": idempotency_key,
            "match_event_id": MATCH_EVENT_ID,
            "net_cash_amount": "-1000.30",
            "order_id": FILL_ORDER_ID,
            "price": "10.000000",
            "quantity": 100,
            "quote_event_id": QUOTE_EVENT_ID,
            "side": "BUY",
            "stock_code": STOCK_CODE,
        }
    )


def _fee_schedule() -> CanonicalJson:
    return CanonicalJson.from_value(
        {
            "buy_commission_rate": "0.0003000000",
            "confirmation_status": "CONFIRMED",
            "created_at": datetime(2026, 1, 1, tzinfo=ZONE),
            "effective_from": date(2026, 1, 1),
            "effective_to": None,
            "evidence_hash": "e" * 64,
            "fee_profile_version": FEE_PROFILE_VERSION,
            "minimum_commission": "0.00",
            "other_fee_json": {},
            "security_type": "EQUITY",
            "sell_commission_rate": "0.0003000000",
            "stamp_tax_sell_rate": "0.0005000000",
            "transfer_fee_buy_rate": "0.0000100000",
            "transfer_fee_sell_rate": "0.0000100000",
        }
    )


def _instrument_rule() -> CanonicalJson:
    return CanonicalJson.from_value(
        {
            "buy_lot_size": 100,
            "can_buy": True,
            "created_at": datetime(2026, 1, 1, tzinfo=ZONE),
            "effective_from": date(2026, 1, 1),
            "effective_to": None,
            "exchange_code": "SSE",
            "fee_profile_version": FEE_PROFILE_VERSION,
            "first_buy_minimum": 100,
            "limit_ratio": "0.10000000",
            "permission_confirmed": True,
            "permission_required": "NONE",
            "rule_version": INSTRUMENT_RULE_VERSION,
            "security_type": "EQUITY",
            "sell_lot_size": 100,
            "settlement_days": 1,
            "source_snapshot_hash": "f" * 64,
            "special_treatment": False,
            "stock_code": STOCK_CODE,
            "suspended": False,
            "tick_size": "0.010000",
        }
    )


def _fill(
    calendar: MarketCalendarEvidence,
    quote: QuoteReceiptEvidence,
) -> FillExecutionEvidence:
    order_payload = _fill_order_payload()
    fill_payload = _fill_payload()
    fee_schedule = _fee_schedule()
    instrument_rule = _instrument_rule()
    settlement = CanonicalJson.from_value(
        {
            "calendar_evidence_hash": calendar.evidence_hash,
            "instrument_rule_hash": instrument_rule.payload_hash,
            "settlement_date": "2026-08-04",
            "settlement_days": 1,
            "stock_code": STOCK_CODE,
            "trade_date": "2026-08-03",
        }
    )
    matcher_request = CanonicalJson.from_value(
        {
            "calendar_evidence_hash": calendar.evidence_hash,
            "matcher_version": "matcher-mysql57-v1",
            "order_id": FILL_ORDER_ID,
            "order_payload_hash": order_payload.payload_hash,
            "quote_event_id": quote.quote_event_id,
            "quote_evidence_hash": quote.evidence_hash,
        }
    )
    matcher_response = CanonicalJson.from_value(
        {
            "fill_price": "10.000000",
            "fill_quantity": 100,
            "match_event_id": MATCH_EVENT_ID,
            "matcher_request_hash": matcher_request.payload_hash,
            "order_id": FILL_ORDER_ID,
            "quote_event_id": quote.quote_event_id,
            "side": "BUY",
            "status": "FILLED",
        }
    )
    accounting_request = CanonicalJson.from_value(
        {
            "account_id": ACCOUNT_ID,
            "calendar_evidence_hash": calendar.evidence_hash,
            "fee_amount": "0.30",
            "fee_schedule_hash": fee_schedule.payload_hash,
            "fill_id": FILL_ID,
            "gross_amount": "1000.00",
            "instrument_rule_hash": instrument_rule.payload_hash,
            "matcher_output_hash": matcher_response.payload_hash,
            "net_cash_amount": "-1000.30",
            "order_id": FILL_ORDER_ID,
            "price": "10.000000",
            "quantity": 100,
            "quote_evidence_hash": quote.evidence_hash,
            "settlement_evidence_hash": settlement.payload_hash,
            "side": "BUY",
            "stock_code": STOCK_CODE,
        }
    )
    value = FillExecutionEvidence(
        fill_id=FILL_ID,
        order_id=FILL_ORDER_ID,
        order_fill_sequence=1,
        account_id=ACCOUNT_ID,
        stock_code=STOCK_CODE,
        fill_payload=fill_payload,
        order_payload=order_payload,
        quote_evidence=quote,
        calendar_evidence=calendar,
        fee_profile_version=FEE_PROFILE_VERSION,
        fee_security_type="EQUITY",
        fee_effective_from=date(2026, 1, 1),
        fee_effective_to=None,
        fee_created_at=datetime(2026, 1, 1, tzinfo=ZONE),
        fee_schedule=fee_schedule,
        instrument_rule_version=INSTRUMENT_RULE_VERSION,
        instrument_rule_effective_from=date(2026, 1, 1),
        instrument_rule_effective_to=None,
        instrument_rule_created_at=datetime(2026, 1, 1, tzinfo=ZONE),
        instrument_rule=instrument_rule,
        matcher_version="matcher-mysql57-v1",
        matcher_request=matcher_request,
        matcher_response=matcher_response,
        accounting_request=accounting_request,
        settlement_evidence=settlement,
        executed_at=_aware(10, 0, 2),
        bound_at=_aware(10, 0, 4),
        provenance=_provenance(),
    )
    validate_fill_execution_evidence(value)
    return value


def _cash_payload() -> CanonicalJson:
    return CanonicalJson.from_value(
        {
            "account_id": ACCOUNT_ID,
            "amount": "100000.00",
            "balance_after": "100000.00",
            "business_event_key": f"{ACCOUNT_ID}:INITIAL_DEPOSIT",
            "cash_event_id": "mysql57-cash-genesis",
            "created_at": _aware(8),
            "event_type": "INITIAL_DEPOSIT",
            "occurred_at": _aware(8),
            "related_fill_id": None,
            "related_order_id": None,
            "reversal_of": None,
        }
    )


def _seed_rows() -> tuple[CanonicalSeedRow, ...]:
    fill_idempotency_key = hashlib.sha256(
        f"{FILL_ORDER_ID}|{QUOTE_EVENT_ID}|{MATCH_EVENT_ID}".encode("utf-8")
    ).hexdigest()
    return (
        CanonicalSeedRow(
            "st_trade_account_v2",
            {
                "account_id": ACCOUNT_ID,
                "account_name": "MySQL 5.7 behavioral account",
                "status": "ACTIVE",
                "initial_cash": Decimal("100000.00"),
                "cash_balance": Decimal("100000.00"),
                "peak_equity": Decimal("100000.00"),
                "policy_version": "mysql57-policy-v1",
                "policy_hash": "3" * 64,
                "fee_profile_version": None,
                "instrument_rule_version": None,
                "real_trading_enabled": 0,
                "created_at": _naive(7),
                "updated_at": _naive(10, 0, 4),
            },
        ),
        CanonicalSeedRow(
            "st_order_v2",
            {
                "order_id": CREATED_ORDER_ID,
                "account_id": ACCOUNT_ID,
                "intent_id": "mysql57-intent-created",
                "stock_code": STOCK_CODE,
                "side": "BUY",
                "order_type": "LIMIT",
                "limit_price": Decimal("10.000000"),
                "quantity": 100,
                "filled_quantity": 0,
                "status": "CREATED",
                "waiting_reason": None,
                "earliest_at": _naive(9, 30),
                "expires_at": _naive(15),
                "idempotency_key": "1" * 64,
                "created_at": _naive(9),
                "updated_at": _naive(9),
            },
        ),
        CanonicalSeedRow(
            "st_order_v2",
            {
                "order_id": FILL_ORDER_ID,
                "account_id": ACCOUNT_ID,
                "intent_id": "mysql57-intent-fill",
                "stock_code": STOCK_CODE,
                "side": "BUY",
                "order_type": "LIMIT",
                "limit_price": Decimal("10.000000"),
                "quantity": 100,
                "filled_quantity": 100,
                "status": "FILLED",
                "waiting_reason": None,
                "earliest_at": _naive(9, 30),
                "expires_at": _naive(15),
                "idempotency_key": "4" * 64,
                "created_at": _naive(9),
                "updated_at": _naive(10, 0, 3),
            },
        ),
        CanonicalSeedRow(
            "st_quote_event_v2",
            {
                "quote_event_id": QUOTE_EVENT_ID,
                "stock_code": STOCK_CODE,
                "quote_at": _naive(9, 59, 59),
                "received_at": _naive(10),
                "bid1": Decimal("9.990000"),
                "bid1_volume": 1000,
                "ask1": Decimal("10.000000"),
                "ask1_volume": 1200,
                "last_price": Decimal("10.000000"),
                "pre_close": Decimal("9.800000"),
                "upper_limit": Decimal("10.780000"),
                "lower_limit": Decimal("8.820000"),
                "suspended": 0,
                "source_provider": "synthetic-quote-adapter",
                "source_batch_id": "mysql57-quote-batch-1",
                "payload_hash": QUOTE_EVENT_ID,
                "created_at": _naive(10),
            },
        ),
        CanonicalSeedRow(
            "st_fee_profile_v2",
            {
                "fee_profile_version": FEE_PROFILE_VERSION,
                "effective_from": date(2026, 1, 1),
                "effective_to": None,
                "security_type": "EQUITY",
                "buy_commission_rate": Decimal("0.0003000000"),
                "sell_commission_rate": Decimal("0.0003000000"),
                "minimum_commission": Decimal("0.00"),
                "stamp_tax_sell_rate": Decimal("0.0005000000"),
                "transfer_fee_buy_rate": Decimal("0.0000100000"),
                "transfer_fee_sell_rate": Decimal("0.0000100000"),
                "other_fee_json": "{}",
                "evidence_hash": "e" * 64,
                "confirmation_status": "CONFIRMED",
                "created_at": datetime(2026, 1, 1),
            },
        ),
        CanonicalSeedRow(
            "st_instrument_rule_v2",
            {
                "stock_code": STOCK_CODE,
                "rule_version": INSTRUMENT_RULE_VERSION,
                "effective_from": date(2026, 1, 1),
                "effective_to": None,
                "security_type": "EQUITY",
                "exchange_code": "SSE",
                "can_buy": 1,
                "first_buy_minimum": 100,
                "buy_lot_size": 100,
                "sell_lot_size": 100,
                "settlement_days": 1,
                "tick_size": Decimal("0.010000"),
                "limit_ratio": Decimal("0.10000000"),
                "special_treatment": 0,
                "suspended": 0,
                "permission_required": "NONE",
                "permission_confirmed": 1,
                "fee_profile_version": FEE_PROFILE_VERSION,
                "source_snapshot_hash": "f" * 64,
                "created_at": datetime(2026, 1, 1),
            },
        ),
        CanonicalSeedRow(
            "st_fill_v2",
            {
                "fill_id": FILL_ID,
                "order_id": FILL_ORDER_ID,
                "account_id": ACCOUNT_ID,
                "stock_code": STOCK_CODE,
                "side": "BUY",
                "quantity": 100,
                "price": Decimal("10.000000"),
                "gross_amount": Decimal("1000.00"),
                "fee_amount": Decimal("0.30"),
                "net_cash_amount": Decimal("-1000.30"),
                "quote_event_id": QUOTE_EVENT_ID,
                "match_event_id": MATCH_EVENT_ID,
                "idempotency_key": fill_idempotency_key,
                "filled_at": _naive(10, 0, 2),
                "created_at": _naive(10, 0, 3),
            },
        ),
        CanonicalSeedRow(
            "st_cash_ledger_v2",
            {
                "cash_event_id": "mysql57-cash-genesis",
                "account_id": ACCOUNT_ID,
                "business_event_key": f"{ACCOUNT_ID}:INITIAL_DEPOSIT",
                "event_type": "INITIAL_DEPOSIT",
                "amount": Decimal("100000.00"),
                "balance_after": Decimal("100000.00"),
                "related_order_id": None,
                "related_fill_id": None,
                "reversal_of": None,
                "occurred_at": _naive(8),
                "created_at": _naive(8),
            },
        ),
    )


def build_behavioral_scenario() -> BehavioralScenario:
    """Build a self-contained five-writer CONTENT_HASH_ONLY scenario."""

    calendar = _calendar()
    quote = _quote()
    fill = _fill(calendar, quote)
    cash = CashEventBinding(
        cash_event_id="mysql57-cash-genesis",
        account_id=ACCOUNT_ID,
        account_sequence=0,
        cash_event_type="INITIAL_DEPOSIT",
        cash_event_payload=_cash_payload(),
        occurred_at=_aware(8),
        bound_at=_aware(8, 0, 1),
        provenance=_provenance(),
    )
    order = OrderTransitionEvidence(
        order_id=CREATED_ORDER_ID,
        account_id=ACCOUNT_ID,
        order_payload=_created_order_payload(),
        transition_sequence=0,
        from_status=OrderStatus.CREATED,
        to_status=OrderStatus.CREATED,
        previous_filled_quantity=0,
        next_filled_quantity=0,
        transition_kind=OrderTransitionKind.ORDER_CREATED,
        source_event_type="ORDER_CREATED",
        source_event_id=f"{CREATED_ORDER_ID}:created",
        source_event_hash="2" * 64,
        occurred_at=_aware(9),
        recorded_at=_aware(9, 0, 1),
        provenance=_provenance(),
    )
    validate_cash_event_binding(cash)
    validate_order_transition_evidence(order)

    cases = (
        BehavioralEvidenceCase(
            "MARKET_CALENDAR",
            "st_market_calendar_evidence_v2",
            "calendar_evidence_id",
            calendar.calendar_evidence_id,
            calendar,
            "calendar evidence is append only",
            "calendar evidence cannot be deleted",
        ),
        BehavioralEvidenceCase(
            "QUOTE_RECEIPT",
            "st_quote_receipt_evidence_v2",
            "quote_evidence_id",
            quote.quote_evidence_id,
            quote,
            "quote evidence is append only",
            "quote evidence cannot be deleted",
        ),
        BehavioralEvidenceCase(
            "FILL_EXECUTION",
            "st_fill_execution_evidence_v2",
            "fill_execution_evidence_id",
            fill.fill_execution_evidence_id,
            fill,
            "fill evidence is append only",
            "fill evidence cannot be deleted",
            ("MARKET_CALENDAR", "QUOTE_RECEIPT"),
        ),
        BehavioralEvidenceCase(
            "CASH_EVENT",
            "st_cash_event_binding_v2",
            "cash_binding_id",
            cash.cash_binding_id,
            cash,
            "cash evidence is append only",
            "cash evidence cannot be deleted",
        ),
        BehavioralEvidenceCase(
            "ORDER_TRANSITION",
            "st_order_transition_v2",
            "transition_id",
            order.transition_id,
            order,
            "order transition is append only",
            "order transition cannot be deleted",
        ),
    )
    return BehavioralScenario(_seed_rows(), cases)


def _conflict_quote_row_payload() -> dict[str, Any]:
    value = dict(_quote_row_payload())
    value.update(
        {
            "payload_hash": CONFLICT_QUOTE_EVENT_ID,
            "quote_event_id": CONFLICT_QUOTE_EVENT_ID,
            "source_batch_id": "mysql57-conflict-quote-batch",
        }
    )
    return value


def _conflict_quote() -> QuoteReceiptEvidence:
    value = QuoteReceiptEvidence(
        quote_event_id=CONFLICT_QUOTE_EVENT_ID,
        stock_code=STOCK_CODE,
        trade_date=TRADE_DATE,
        market_timezone="Asia/Shanghai",
        quote_at=_aware(9, 59, 59),
        received_at=_aware(10),
        available_at=_aware(10, 0, 1),
        source_provider="synthetic-quote-adapter",
        source_batch_id="mysql57-conflict-quote-batch",
        source_payload_hash=CONFLICT_QUOTE_EVENT_ID,
        receipt_type=QuoteReceiptType.OTHER,
        receipt_payload=CanonicalJson.from_value(
            {
                "quote_event_id": CONFLICT_QUOTE_EVENT_ID,
                "quote_row": _conflict_quote_row_payload(),
                "source_batch_id": "mysql57-conflict-quote-batch",
                "source_payload_hash": CONFLICT_QUOTE_EVENT_ID,
                "source_provider": "synthetic-quote-adapter",
            }
        ),
        provenance=_provenance(),
        source_receipt_id="mysql57-conflict-quote-receipt-left",
        source_receipt_hash="a" * 64,
    )
    validate_quote_receipt_evidence(value)
    return value


def _conflict_fill_order_payload() -> CanonicalJson:
    return CanonicalJson.from_value(
        {
            "account_id": ACCOUNT_ID,
            "created_at": _aware(9),
            "earliest_at": _aware(9, 30),
            "expires_at": _aware(15),
            "idempotency_key": "6" * 64,
            "intent_id": "mysql57-conflict-intent-fill",
            "limit_price": "10.000000",
            "order_id": CONFLICT_FILL_ORDER_ID,
            "order_type": "LIMIT",
            "quantity": 100,
            "side": "BUY",
            "stock_code": STOCK_CODE,
        }
    )


def _conflict_fill_payload() -> CanonicalJson:
    idempotency_key = hashlib.sha256(
        (
            f"{CONFLICT_FILL_ORDER_ID}|{QUOTE_EVENT_ID}|"
            f"{CONFLICT_MATCH_EVENT_ID}"
        ).encode("utf-8")
    ).hexdigest()
    return CanonicalJson.from_value(
        {
            "account_id": ACCOUNT_ID,
            "created_at": _aware(10, 5, 3),
            "fee_amount": "0.30",
            "fill_id": CONFLICT_FILL_ID,
            "filled_at": _aware(10, 5, 2),
            "gross_amount": "1000.00",
            "idempotency_key": idempotency_key,
            "match_event_id": CONFLICT_MATCH_EVENT_ID,
            "net_cash_amount": "-1000.30",
            "order_id": CONFLICT_FILL_ORDER_ID,
            "price": "10.000000",
            "quantity": 100,
            "quote_event_id": QUOTE_EVENT_ID,
            "side": "BUY",
            "stock_code": STOCK_CODE,
        }
    )


def _conflict_fill(
    calendar: MarketCalendarEvidence,
    quote: QuoteReceiptEvidence,
    *,
    matcher_version: str,
) -> FillExecutionEvidence:
    order_payload = _conflict_fill_order_payload()
    fill_payload = _conflict_fill_payload()
    fee_schedule = _fee_schedule()
    instrument_rule = _instrument_rule()
    settlement = CanonicalJson.from_value(
        {
            "calendar_evidence_hash": calendar.evidence_hash,
            "instrument_rule_hash": instrument_rule.payload_hash,
            "settlement_date": "2026-08-04",
            "settlement_days": 1,
            "stock_code": STOCK_CODE,
            "trade_date": "2026-08-03",
        }
    )
    matcher_request = CanonicalJson.from_value(
        {
            "calendar_evidence_hash": calendar.evidence_hash,
            "matcher_version": matcher_version,
            "order_id": CONFLICT_FILL_ORDER_ID,
            "order_payload_hash": order_payload.payload_hash,
            "quote_event_id": quote.quote_event_id,
            "quote_evidence_hash": quote.evidence_hash,
        }
    )
    matcher_response = CanonicalJson.from_value(
        {
            "fill_price": "10.000000",
            "fill_quantity": 100,
            "match_event_id": CONFLICT_MATCH_EVENT_ID,
            "matcher_request_hash": matcher_request.payload_hash,
            "order_id": CONFLICT_FILL_ORDER_ID,
            "quote_event_id": quote.quote_event_id,
            "side": "BUY",
            "status": "FILLED",
        }
    )
    accounting_request = CanonicalJson.from_value(
        {
            "account_id": ACCOUNT_ID,
            "calendar_evidence_hash": calendar.evidence_hash,
            "fee_amount": "0.30",
            "fee_schedule_hash": fee_schedule.payload_hash,
            "fill_id": CONFLICT_FILL_ID,
            "gross_amount": "1000.00",
            "instrument_rule_hash": instrument_rule.payload_hash,
            "matcher_output_hash": matcher_response.payload_hash,
            "net_cash_amount": "-1000.30",
            "order_id": CONFLICT_FILL_ORDER_ID,
            "price": "10.000000",
            "quantity": 100,
            "quote_evidence_hash": quote.evidence_hash,
            "settlement_evidence_hash": settlement.payload_hash,
            "side": "BUY",
            "stock_code": STOCK_CODE,
        }
    )
    value = FillExecutionEvidence(
        fill_id=CONFLICT_FILL_ID,
        order_id=CONFLICT_FILL_ORDER_ID,
        order_fill_sequence=1,
        account_id=ACCOUNT_ID,
        stock_code=STOCK_CODE,
        fill_payload=fill_payload,
        order_payload=order_payload,
        quote_evidence=quote,
        calendar_evidence=calendar,
        fee_profile_version=FEE_PROFILE_VERSION,
        fee_security_type="EQUITY",
        fee_effective_from=date(2026, 1, 1),
        fee_effective_to=None,
        fee_created_at=datetime(2026, 1, 1, tzinfo=ZONE),
        fee_schedule=fee_schedule,
        instrument_rule_version=INSTRUMENT_RULE_VERSION,
        instrument_rule_effective_from=date(2026, 1, 1),
        instrument_rule_effective_to=None,
        instrument_rule_created_at=datetime(2026, 1, 1, tzinfo=ZONE),
        instrument_rule=instrument_rule,
        matcher_version=matcher_version,
        matcher_request=matcher_request,
        matcher_response=matcher_response,
        accounting_request=accounting_request,
        settlement_evidence=settlement,
        executed_at=_aware(10, 5, 2),
        bound_at=_aware(10, 5, 4),
        provenance=_provenance(),
    )
    validate_fill_execution_evidence(value)
    return value


def _conflict_cash_payload() -> CanonicalJson:
    return CanonicalJson.from_value(
        {
            "account_id": CONFLICT_ACCOUNT_ID,
            "amount": "50000.00",
            "balance_after": "50000.00",
            "business_event_key": (
                f"{CONFLICT_ACCOUNT_ID}:INITIAL_DEPOSIT"
            ),
            "cash_event_id": CONFLICT_CASH_EVENT_ID,
            "created_at": _aware(8, 5),
            "event_type": "INITIAL_DEPOSIT",
            "occurred_at": _aware(8, 5),
            "related_fill_id": None,
            "related_order_id": None,
            "reversal_of": None,
        }
    )


def _conflict_created_order_payload() -> CanonicalJson:
    return CanonicalJson.from_value(
        {
            "account_id": ACCOUNT_ID,
            "created_at": _aware(9, 5),
            "earliest_at": _aware(9, 30),
            "expires_at": _aware(15),
            "idempotency_key": "7" * 64,
            "intent_id": "mysql57-conflict-intent-created",
            "limit_price": "10.000000",
            "order_id": CONFLICT_CREATED_ORDER_ID,
            "order_type": "LIMIT",
            "quantity": 100,
            "side": "BUY",
            "stock_code": STOCK_CODE,
        }
    )


def _conflict_seed_rows() -> tuple[CanonicalSeedRow, ...]:
    fill_idempotency_key = hashlib.sha256(
        (
            f"{CONFLICT_FILL_ORDER_ID}|{QUOTE_EVENT_ID}|"
            f"{CONFLICT_MATCH_EVENT_ID}"
        ).encode("utf-8")
    ).hexdigest()
    return (
        CanonicalSeedRow(
            "st_quote_event_v2",
            {
                "quote_event_id": CONFLICT_QUOTE_EVENT_ID,
                "stock_code": STOCK_CODE,
                "quote_at": _naive(9, 59, 59),
                "received_at": _naive(10),
                "bid1": Decimal("9.990000"),
                "bid1_volume": 1000,
                "ask1": Decimal("10.000000"),
                "ask1_volume": 1200,
                "last_price": Decimal("10.000000"),
                "pre_close": Decimal("9.800000"),
                "upper_limit": Decimal("10.780000"),
                "lower_limit": Decimal("8.820000"),
                "suspended": 0,
                "source_provider": "synthetic-quote-adapter",
                "source_batch_id": "mysql57-conflict-quote-batch",
                "payload_hash": CONFLICT_QUOTE_EVENT_ID,
                "created_at": _naive(10),
            },
        ),
        CanonicalSeedRow(
            "st_order_v2",
            {
                "order_id": CONFLICT_FILL_ORDER_ID,
                "account_id": ACCOUNT_ID,
                "intent_id": "mysql57-conflict-intent-fill",
                "stock_code": STOCK_CODE,
                "side": "BUY",
                "order_type": "LIMIT",
                "limit_price": Decimal("10.000000"),
                "quantity": 100,
                "filled_quantity": 100,
                "status": "FILLED",
                "waiting_reason": None,
                "earliest_at": _naive(9, 30),
                "expires_at": _naive(15),
                "idempotency_key": "6" * 64,
                "created_at": _naive(9),
                "updated_at": _naive(10, 5, 3),
            },
        ),
        CanonicalSeedRow(
            "st_fill_v2",
            {
                "fill_id": CONFLICT_FILL_ID,
                "order_id": CONFLICT_FILL_ORDER_ID,
                "account_id": ACCOUNT_ID,
                "stock_code": STOCK_CODE,
                "side": "BUY",
                "quantity": 100,
                "price": Decimal("10.000000"),
                "gross_amount": Decimal("1000.00"),
                "fee_amount": Decimal("0.30"),
                "net_cash_amount": Decimal("-1000.30"),
                "quote_event_id": QUOTE_EVENT_ID,
                "match_event_id": CONFLICT_MATCH_EVENT_ID,
                "idempotency_key": fill_idempotency_key,
                "filled_at": _naive(10, 5, 2),
                "created_at": _naive(10, 5, 3),
            },
        ),
        CanonicalSeedRow(
            "st_trade_account_v2",
            {
                "account_id": CONFLICT_ACCOUNT_ID,
                "account_name": "MySQL 5.7 conflict account",
                "status": "ACTIVE",
                "initial_cash": Decimal("50000.00"),
                "cash_balance": Decimal("50000.00"),
                "peak_equity": Decimal("50000.00"),
                "policy_version": "mysql57-policy-v1",
                "policy_hash": "5" * 64,
                "fee_profile_version": None,
                "instrument_rule_version": None,
                "real_trading_enabled": 0,
                "created_at": _naive(7),
                "updated_at": _naive(8, 5, 1),
            },
        ),
        CanonicalSeedRow(
            "st_cash_ledger_v2",
            {
                "cash_event_id": CONFLICT_CASH_EVENT_ID,
                "account_id": CONFLICT_ACCOUNT_ID,
                "business_event_key": (
                    f"{CONFLICT_ACCOUNT_ID}:INITIAL_DEPOSIT"
                ),
                "event_type": "INITIAL_DEPOSIT",
                "amount": Decimal("50000.00"),
                "balance_after": Decimal("50000.00"),
                "related_order_id": None,
                "related_fill_id": None,
                "reversal_of": None,
                "occurred_at": _naive(8, 5),
                "created_at": _naive(8, 5),
            },
        ),
        CanonicalSeedRow(
            "st_order_v2",
            {
                "order_id": CONFLICT_CREATED_ORDER_ID,
                "account_id": ACCOUNT_ID,
                "intent_id": "mysql57-conflict-intent-created",
                "stock_code": STOCK_CODE,
                "side": "BUY",
                "order_type": "LIMIT",
                "limit_price": Decimal("10.000000"),
                "quantity": 100,
                "filled_quantity": 0,
                "status": "CREATED",
                "waiting_reason": None,
                "earliest_at": _naive(9, 30),
                "expires_at": _naive(15),
                "idempotency_key": "7" * 64,
                "created_at": _naive(9, 5),
                "updated_at": _naive(9, 5),
            },
        ),
    )


def _behavioral_case(
    evidence_type: str,
    table: str,
    primary_column: str,
    evidence: object,
) -> BehavioralEvidenceCase:
    primary_value = str(getattr(evidence, primary_column))
    return BehavioralEvidenceCase(
        evidence_type=evidence_type,
        table=table,
        primary_column=primary_column,
        primary_value=primary_value,
        evidence=evidence,
        update_guard_message="unused by conflicting double-writer probe",
        delete_guard_message="unused by conflicting double-writer probe",
    )


def build_conflicting_double_writer_scenario(
    base: BehavioralScenario,
) -> ConflictingDoubleWriterScenario:
    """Build two legal, different contents for each fixed business key."""

    by_type = {case.evidence_type: case for case in base.cases}
    base_calendar = by_type["MARKET_CALENDAR"].evidence
    base_quote = by_type["QUOTE_RECEIPT"].evidence
    if type(base_calendar) is not MarketCalendarEvidence:
        raise TypeError("base calendar evidence type drifted")
    if type(base_quote) is not QuoteReceiptEvidence:
        raise TypeError("base quote evidence type drifted")

    calendar_left = _calendar(calendar_version="calendar-conflict-v1")
    calendar_right = replace(calendar_left, available_at=_aware(8, 0, 1))
    quote_left = _conflict_quote()
    quote_right = replace(
        quote_left,
        source_receipt_id="mysql57-conflict-quote-receipt-right",
        source_receipt_hash="b" * 64,
    )
    fill_left = _conflict_fill(
        base_calendar,
        base_quote,
        matcher_version="matcher-conflict-left",
    )
    fill_right = _conflict_fill(
        base_calendar,
        base_quote,
        matcher_version="matcher-conflict-right",
    )
    cash_left = CashEventBinding(
        cash_event_id=CONFLICT_CASH_EVENT_ID,
        account_id=CONFLICT_ACCOUNT_ID,
        account_sequence=0,
        cash_event_type="INITIAL_DEPOSIT",
        cash_event_payload=_conflict_cash_payload(),
        occurred_at=_aware(8, 5),
        bound_at=_aware(8, 5, 1),
        provenance=_provenance(),
    )
    cash_right = replace(cash_left, bound_at=_aware(8, 5, 2))
    order_left = OrderTransitionEvidence(
        order_id=CONFLICT_CREATED_ORDER_ID,
        account_id=ACCOUNT_ID,
        order_payload=_conflict_created_order_payload(),
        transition_sequence=0,
        from_status=OrderStatus.CREATED,
        to_status=OrderStatus.CREATED,
        previous_filled_quantity=0,
        next_filled_quantity=0,
        transition_kind=OrderTransitionKind.ORDER_CREATED,
        source_event_type="ORDER_CREATED",
        source_event_id=f"{CONFLICT_CREATED_ORDER_ID}:created",
        source_event_hash="5" * 64,
        occurred_at=_aware(9, 5),
        recorded_at=_aware(9, 5, 1),
        provenance=_provenance(),
    )
    order_right = replace(order_left, source_event_hash="6" * 64)

    for value, validator in (
        (calendar_left, validate_market_calendar_evidence),
        (calendar_right, validate_market_calendar_evidence),
        (quote_left, validate_quote_receipt_evidence),
        (quote_right, validate_quote_receipt_evidence),
        (fill_left, validate_fill_execution_evidence),
        (fill_right, validate_fill_execution_evidence),
        (cash_left, validate_cash_event_binding),
        (cash_right, validate_cash_event_binding),
        (order_left, validate_order_transition_evidence),
        (order_right, validate_order_transition_evidence),
    ):
        validator(value)

    raw_pairs = (
        (
            "MARKET_CALENDAR",
            "st_market_calendar_evidence_v2",
            "calendar_evidence_id",
            ("market_code", "trade_date", "calendar_version"),
            ("SSE", TRADE_DATE, "calendar-conflict-v1"),
            calendar_left,
            calendar_right,
        ),
        (
            "QUOTE_RECEIPT",
            "st_quote_receipt_evidence_v2",
            "quote_evidence_id",
            ("quote_event_id",),
            (CONFLICT_QUOTE_EVENT_ID,),
            quote_left,
            quote_right,
        ),
        (
            "FILL_EXECUTION",
            "st_fill_execution_evidence_v2",
            "fill_execution_evidence_id",
            ("fill_id",),
            (CONFLICT_FILL_ID,),
            fill_left,
            fill_right,
        ),
        (
            "CASH_EVENT",
            "st_cash_event_binding_v2",
            "cash_binding_id",
            ("cash_event_id",),
            (CONFLICT_CASH_EVENT_ID,),
            cash_left,
            cash_right,
        ),
        (
            "ORDER_TRANSITION",
            "st_order_transition_v2",
            "transition_id",
            ("order_id", "transition_sequence"),
            (CONFLICT_CREATED_ORDER_ID, 0),
            order_left,
            order_right,
        ),
    )
    pairs = tuple(
        ConflictingBehavioralEvidencePair(
            evidence_type=evidence_type,
            table=table,
            primary_column=primary_column,
            natural_key_columns=natural_columns,
            natural_key_values=natural_values,
            left=_behavioral_case(
                evidence_type, table, primary_column, left
            ),
            right=_behavioral_case(
                evidence_type, table, primary_column, right
            ),
        )
        for (
            evidence_type,
            table,
            primary_column,
            natural_columns,
            natural_values,
            left,
            right,
        ) in raw_pairs
    )
    for pair in pairs:
        if pair.left.primary_value == pair.right.primary_value:
            raise RuntimeError(
                f"{pair.evidence_type} conflicting contents share a primary id"
            )
    return ConflictingDoubleWriterScenario(_conflict_seed_rows(), pairs)


__all__ = [
    "BehavioralEvidenceCase",
    "BehavioralScenario",
    "CanonicalSeedRow",
    "ConflictingBehavioralEvidencePair",
    "ConflictingDoubleWriterScenario",
    "build_behavioral_scenario",
    "build_conflicting_double_writer_scenario",
]
