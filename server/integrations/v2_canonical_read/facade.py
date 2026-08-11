"""Canonical V2 read facade over one caller-owned SQLAlchemy transaction.

The facade never creates an engine, starts or ends a transaction, or emits
write SQL.  Its content root proves only the deterministic content returned by
the supplied transaction.  It is *not* proof that the connection targets the
authoritative V2 database; source authority must be established outside this
module.

The current V2 schema does not persist every historical fee/rule/calendar and
quote-receipt binding on a fill.  Those gaps are reported as an explicit
``BLOCKED`` replay capability.  The facade does not fill them from whichever
configuration happens to be current at read time.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_DOWN, Decimal
from enum import Enum
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

from server.integrations.v2_execution_adapter.fees import (
    v2_fee_profile_to_neutral_schedule,
)
from server.trading_core.accounting import fee_schedule_fingerprint
from server.trading_v2.config import canonical_json_hash
from server.trading_v2.domain import (
    InstrumentRule,
    IntentAction,
    OrderStatus,
    PositionState,
)
from server.trading_v2.ledger import FeeProfile


V2_MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")
SNAPSHOT_SCHEMA_VERSION = "canonical-v2-transaction-snapshot.v1"
CONSISTENT_SNAPSHOT_ISOLATION_LEVELS = frozenset(
    {"REPEATABLE READ", "SERIALIZABLE"}
)
ACCOUNT_STATUSES = frozenset(
    {"ACTIVE", "CONFIG_BLOCKED", "RECONCILIATION_BLOCKED"}
)
INTENT_ACTIONS = frozenset(item.value for item in IntentAction) | frozenset(
    {"BUY", "SELL"}
)
ORDER_STATUSES = frozenset(item.value for item in OrderStatus)
POSITION_STATES = frozenset(item.value for item in PositionState)
RECEIPT_QUALITY_STATUSES = frozenset({"PASS", "BLOCK"})


class V2CanonicalReadError(RuntimeError):
    """Base error for caller/transaction failures at the read boundary."""


class V2CanonicalSnapshotInvariantError(V2CanonicalReadError):
    """Canonical rows are present but cannot describe one valid V2 state."""


class V2CapabilityStatus(str, Enum):
    """Facade capability, deliberately narrower than execution readiness.

    ``MATERIALIZED_SNAPSHOT_READY`` says only that current rows formed one
    internally consistent transaction snapshot.  It does not attest session,
    quote, source, or execution authority.
    """

    MATERIALIZED_SNAPSHOT_READY = "CURRENT_MATERIALIZED_SNAPSHOT_READY"
    AUTHORITATIVE_REPLAY_BLOCKED = "AUTHORITATIVE_REPLAY_BLOCKED"
    SNAPSHOT_READ_BLOCKED = "SNAPSHOT_READ_BLOCKED"


class V2ContentRootSemantics(str, Enum):
    TRANSACTION_CONTENT_ONLY = "TRANSACTION_CONTENT_ONLY_NOT_SOURCE_AUTHORITY"


@dataclass(frozen=True, slots=True)
class V2CapabilityBlocker:
    code: str
    missing_bindings: tuple[str, ...]
    reason: str

    def __post_init__(self) -> None:
        if type(self.code) is not str or not self.code:
            raise TypeError("blocker code must be a non-empty str")
        if type(self.missing_bindings) is not tuple or any(
            type(item) is not str or not item for item in self.missing_bindings
        ):
            raise TypeError("missing_bindings must contain non-empty strings")
        if self.missing_bindings != tuple(sorted(set(self.missing_bindings))):
            raise ValueError("missing_bindings must be sorted and unique")
        if type(self.reason) is not str or not self.reason:
            raise TypeError("blocker reason must be a non-empty str")


@dataclass(frozen=True, slots=True)
class V2TradeAccountRow:
    account_id: str
    account_name: str
    status: str
    initial_cash: Decimal
    cash_balance: Decimal
    peak_equity: Decimal
    policy_version: str
    policy_hash: str
    fee_profile_version: str | None
    instrument_rule_version: str | None
    real_trading_enabled: bool
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class V2TradeIntentRow:
    intent_id: str
    account_id: str
    decision_run_uid: str
    strategy_version: str
    stock_code: str
    theme_code: str
    action: str
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
    evidence_json: str
    intent_version: int
    idempotency_key: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class V2OrderRow:
    order_id: str
    account_id: str
    intent_id: str
    stock_code: str
    side: str
    order_type: str
    limit_price: Decimal
    quantity: int
    filled_quantity: int
    status: str
    waiting_reason: str | None
    earliest_at: datetime
    expires_at: datetime
    idempotency_key: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class V2FillRow:
    fill_id: str
    order_id: str
    account_id: str
    stock_code: str
    side: str
    quantity: int
    price: Decimal
    gross_amount: Decimal
    fee_amount: Decimal
    net_cash_amount: Decimal
    quote_event_id: str
    match_event_id: str
    idempotency_key: str
    filled_at: datetime
    created_at: datetime


@dataclass(frozen=True, slots=True)
class V2PositionLotRow:
    lot_id: str
    account_id: str
    stock_code: str
    theme_code: str
    strategy_version: str
    opened_fill_id: str
    opened_trade_date: date
    settlement_date: date
    original_quantity: int
    remaining_quantity: int
    cost_price: Decimal
    allocated_buy_fee: Decimal
    position_state: str
    approved_target_quantity: int
    add_count: int
    initial_stop: Decimal
    protective_stop: Decimal
    invalidation_condition: str
    version: int
    created_at: datetime
    closed_at: datetime | None


@dataclass(frozen=True, slots=True)
class V2CashLedgerRow:
    cash_event_id: str
    account_id: str
    business_event_key: str
    event_type: str
    amount: Decimal
    balance_after: Decimal
    related_order_id: str | None
    related_fill_id: str | None
    reversal_of: str | None
    occurred_at: datetime
    created_at: datetime


@dataclass(frozen=True, slots=True)
class V2FeeProfileRow:
    fee_profile_version: str
    effective_from: date
    effective_to: date | None
    security_type: str
    buy_commission_rate: Decimal
    sell_commission_rate: Decimal
    minimum_commission: Decimal
    stamp_tax_sell_rate: Decimal
    transfer_fee_buy_rate: Decimal
    transfer_fee_sell_rate: Decimal
    other_fee_json: str
    evidence_hash: str
    confirmation_status: str
    created_at: datetime
    fee_schedule_hash: str


@dataclass(frozen=True, slots=True)
class V2InstrumentRuleRow:
    stock_code: str
    rule_version: str
    effective_from: date
    effective_to: date | None
    security_type: str
    exchange_code: str
    can_buy: bool
    first_buy_minimum: int
    buy_lot_size: int
    sell_lot_size: int
    settlement_days: int
    tick_size: Decimal
    limit_ratio: Decimal | None
    special_treatment: bool
    suspended: bool
    permission_required: str
    permission_confirmed: bool
    fee_profile_version: str
    source_snapshot_hash: str
    created_at: datetime
    adapter_instrument_rule_hash: str
    instrument_rule_hash: str


@dataclass(frozen=True, slots=True)
class V2TradeCalendarRow:
    calendar_year: int
    trade_date: date
    trade_status: int
    day_week: int | None
    etl_sync_at: datetime | None


@dataclass(frozen=True, slots=True)
class V2QmtMinuteReceiptRow:
    receipt_id: str
    trade_date: date
    first_trade_time: datetime
    last_trade_time: datetime
    expected_count: int
    observed_count: int
    coverage: Decimal
    row_count: int
    source_provider: str
    capture_mode: str
    forward_eligible: bool
    quality_status: str
    evidence_json: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class V2PublicQuoteReceiptRow:
    batch_id: str
    trade_date: date
    quote_at: datetime
    received_at: datetime
    expected_count: int
    observed_count: int
    coverage: Decimal
    provider_count: int
    minimum_sources_per_symbol: int
    agreement_ratio: Decimal
    source_provider: str
    maximum_price_deviation_pct: Decimal
    maximum_source_latency_seconds: Decimal
    quality_status: str
    provider_status_json: str
    evidence_json: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class V2QmtRealtimeReceiptRow:
    receipt_id: str
    source_provider: str
    source_snapshot_token: str
    source_full_file_token: str
    source_generated_at: datetime
    heartbeat_at: datetime
    expected_count: int
    observed_count: int
    coverage: Decimal
    published_at: datetime
    capture_mode: str
    quality_status: str
    evidence_json: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class V2RowManifestEntry:
    table_name: str
    ordinal: int
    primary_key: tuple[str, ...]
    row_hash: str

    def __post_init__(self) -> None:
        if type(self.table_name) is not str or not self.table_name:
            raise TypeError("table_name must be non-empty str")
        if type(self.ordinal) is not int or self.ordinal < 0:
            raise TypeError("ordinal must be a non-negative int")
        if type(self.primary_key) is not tuple or not self.primary_key:
            raise TypeError("primary_key must be a non-empty tuple")
        if any(type(item) is not str or not item for item in self.primary_key):
            raise TypeError("primary_key values must be non-empty str")
        _sha256(self.row_hash, "row_hash")


@dataclass(frozen=True, slots=True)
class V2RowManifest:
    schema_version: str
    entries: tuple[V2RowManifestEntry, ...]
    row_count: int
    root_hash: str

    def __post_init__(self) -> None:
        if self.schema_version != SNAPSHOT_SCHEMA_VERSION:
            raise ValueError("unsupported row manifest schema version")
        if type(self.entries) is not tuple or any(
            type(item) is not V2RowManifestEntry for item in self.entries
        ):
            raise TypeError("entries must contain exact manifest entries")
        if type(self.row_count) is not int or self.row_count != len(self.entries):
            raise ValueError("row_count does not match manifest entries")
        _sha256(self.root_hash, "root_hash")


@dataclass(frozen=True, slots=True)
class V2CanonicalSnapshot:
    schema_version: str
    account_id: str
    knowledge_at: datetime
    account: V2TradeAccountRow
    intents: tuple[V2TradeIntentRow, ...]
    orders: tuple[V2OrderRow, ...]
    fills: tuple[V2FillRow, ...]
    lots: tuple[V2PositionLotRow, ...]
    cash_ledger: tuple[V2CashLedgerRow, ...]
    fee_profiles: tuple[V2FeeProfileRow, ...]
    instrument_rules: tuple[V2InstrumentRuleRow, ...]
    trade_calendar: tuple[V2TradeCalendarRow, ...]
    qmt_minute_receipts: tuple[V2QmtMinuteReceiptRow, ...]
    public_quote_receipts: tuple[V2PublicQuoteReceiptRow, ...]
    qmt_realtime_receipts: tuple[V2QmtRealtimeReceiptRow, ...]
    row_manifest: V2RowManifest
    transaction_content_root_hash: str
    root_semantics: V2ContentRootSemantics = (
        V2ContentRootSemantics.TRANSACTION_CONTENT_ONLY
    )
    source_authority_verified: bool = False

    def __post_init__(self) -> None:
        if self.schema_version != SNAPSHOT_SCHEMA_VERSION:
            raise ValueError("unsupported snapshot schema version")
        if type(self.account_id) is not str or not self.account_id:
            raise TypeError("account_id must be a non-empty str")
        if type(self.knowledge_at) is not datetime:
            raise TypeError("knowledge_at must be exactly datetime")
        if self.knowledge_at.tzinfo is None or self.knowledge_at.utcoffset() is None:
            raise ValueError("knowledge_at must be timezone-aware")
        if type(self.account) is not V2TradeAccountRow:
            raise TypeError("account must be exactly V2TradeAccountRow")
        if self.account.account_id != self.account_id:
            raise ValueError("snapshot account_id differs from account row")
        exact_tuple_types = (
            (self.intents, V2TradeIntentRow, "intents"),
            (self.orders, V2OrderRow, "orders"),
            (self.fills, V2FillRow, "fills"),
            (self.lots, V2PositionLotRow, "lots"),
            (self.cash_ledger, V2CashLedgerRow, "cash_ledger"),
            (self.fee_profiles, V2FeeProfileRow, "fee_profiles"),
            (self.instrument_rules, V2InstrumentRuleRow, "instrument_rules"),
            (self.trade_calendar, V2TradeCalendarRow, "trade_calendar"),
            (
                self.qmt_minute_receipts,
                V2QmtMinuteReceiptRow,
                "qmt_minute_receipts",
            ),
            (
                self.public_quote_receipts,
                V2PublicQuoteReceiptRow,
                "public_quote_receipts",
            ),
            (
                self.qmt_realtime_receipts,
                V2QmtRealtimeReceiptRow,
                "qmt_realtime_receipts",
            ),
        )
        for values, expected, name in exact_tuple_types:
            if type(values) is not tuple or any(type(item) is not expected for item in values):
                raise TypeError(f"{name} must contain exact {expected.__name__} rows")
        if type(self.row_manifest) is not V2RowManifest:
            raise TypeError("row_manifest must be exactly V2RowManifest")
        if self.transaction_content_root_hash != self.row_manifest.root_hash:
            raise ValueError("snapshot root differs from row manifest root")
        _sha256(
            self.transaction_content_root_hash,
            "transaction_content_root_hash",
        )
        if self.root_semantics is not V2ContentRootSemantics.TRANSACTION_CONTENT_ONLY:
            raise ValueError("content root semantics cannot claim source authority")
        if self.source_authority_verified is not False:
            raise ValueError("this facade cannot attest source authority")


@dataclass(frozen=True, slots=True)
class V2CanonicalReadResult:
    capability_status: V2CapabilityStatus
    blockers: tuple[V2CapabilityBlocker, ...]
    snapshot: V2CanonicalSnapshot | None

    def __post_init__(self) -> None:
        if type(self.capability_status) is not V2CapabilityStatus:
            raise TypeError("capability_status must be exactly V2CapabilityStatus")
        if type(self.blockers) is not tuple or any(
            type(item) is not V2CapabilityBlocker for item in self.blockers
        ):
            raise TypeError("blockers must contain exact V2CapabilityBlocker values")
        codes = tuple(item.code for item in self.blockers)
        if codes != tuple(sorted(set(codes))):
            raise ValueError("blockers must be sorted by unique code")
        if self.capability_status is V2CapabilityStatus.MATERIALIZED_SNAPSHOT_READY:
            if self.blockers or type(self.snapshot) is not V2CanonicalSnapshot:
                raise ValueError(
                    "MATERIALIZED_SNAPSHOT_READY requires a snapshot and no blockers"
                )
        elif self.capability_status is V2CapabilityStatus.AUTHORITATIVE_REPLAY_BLOCKED:
            if not self.blockers or type(self.snapshot) is not V2CanonicalSnapshot:
                raise ValueError(
                    "AUTHORITATIVE_REPLAY_BLOCKED requires snapshot and blockers"
                )
        elif self.capability_status is V2CapabilityStatus.SNAPSHOT_READ_BLOCKED:
            if not self.blockers or self.snapshot is not None:
                raise ValueError(
                    "SNAPSHOT_READ_BLOCKED requires blockers and no snapshot"
                )
        if self.snapshot is not None and type(self.snapshot) is not V2CanonicalSnapshot:
            raise TypeError("snapshot must be exactly V2CanonicalSnapshot or None")

    @property
    def blocker_codes(self) -> tuple[str, ...]:
        return tuple(item.code for item in self.blockers)


class _SchemaReadBlocked(RuntimeError):
    def __init__(self, table_name: str, reason: str) -> None:
        self.table_name = table_name
        self.reason = reason
        super().__init__(f"{table_name}:{reason}")


ACCOUNT_COLUMNS = (
    "account_id", "account_name", "status", "initial_cash", "cash_balance",
    "peak_equity", "policy_version", "policy_hash", "fee_profile_version",
    "instrument_rule_version", "real_trading_enabled", "created_at", "updated_at",
)
INTENT_COLUMNS = (
    "intent_id", "account_id", "decision_run_uid", "strategy_version",
    "stock_code", "theme_code", "action", "current_quantity", "target_quantity",
    "target_weight", "earliest_at", "expires_at", "limit_price", "worst_price",
    "initial_stop", "protective_stop", "invalidation_condition", "reason_code",
    "evidence_json", "intent_version", "idempotency_key", "created_at",
)
ORDER_COLUMNS = (
    "order_id", "account_id", "intent_id", "stock_code", "side", "order_type",
    "limit_price", "quantity", "filled_quantity", "status", "waiting_reason",
    "earliest_at", "expires_at", "idempotency_key", "created_at", "updated_at",
)
FILL_COLUMNS = (
    "fill_id", "order_id", "account_id", "stock_code", "side", "quantity",
    "price", "gross_amount", "fee_amount", "net_cash_amount", "quote_event_id",
    "match_event_id", "idempotency_key", "filled_at", "created_at",
)
LOT_COLUMNS = (
    "lot_id", "account_id", "stock_code", "theme_code", "strategy_version",
    "opened_fill_id", "opened_trade_date", "settlement_date", "original_quantity",
    "remaining_quantity", "cost_price", "allocated_buy_fee", "position_state",
    "approved_target_quantity", "add_count", "initial_stop", "protective_stop",
    "invalidation_condition", "version", "created_at", "closed_at",
)
CASH_COLUMNS = (
    "cash_event_id", "account_id", "business_event_key", "event_type", "amount",
    "balance_after", "related_order_id", "related_fill_id", "reversal_of",
    "occurred_at", "created_at",
)
FEE_COLUMNS = (
    "fee_profile_version", "effective_from", "effective_to", "security_type",
    "buy_commission_rate", "sell_commission_rate", "minimum_commission",
    "stamp_tax_sell_rate", "transfer_fee_buy_rate", "transfer_fee_sell_rate",
    "other_fee_json", "evidence_hash", "confirmation_status", "created_at",
)
RULE_COLUMNS = (
    "stock_code", "rule_version", "effective_from", "effective_to", "security_type",
    "exchange_code", "can_buy", "first_buy_minimum", "buy_lot_size",
    "sell_lot_size", "settlement_days", "tick_size", "limit_ratio",
    "special_treatment", "suspended", "permission_required",
    "permission_confirmed", "fee_profile_version", "source_snapshot_hash", "created_at",
)
CALENDAR_COLUMNS = (
    "calendar_year", "trade_date", "trade_status", "day_week", "etl_sync_at",
)
QMT_MINUTE_RECEIPT_COLUMNS = (
    "receipt_id", "trade_date", "first_trade_time", "last_trade_time",
    "expected_count", "observed_count", "coverage", "row_count", "source_provider",
    "capture_mode", "forward_eligible", "quality_status", "evidence_json", "created_at",
)
PUBLIC_RECEIPT_COLUMNS = (
    "batch_id", "trade_date", "quote_at", "received_at", "expected_count",
    "observed_count", "coverage", "provider_count", "minimum_sources_per_symbol",
    "agreement_ratio", "source_provider", "maximum_price_deviation_pct",
    "maximum_source_latency_seconds", "quality_status", "provider_status_json",
    "evidence_json", "created_at",
)
QMT_REALTIME_RECEIPT_COLUMNS = (
    "receipt_id", "source_provider", "source_snapshot_token", "source_full_file_token",
    "source_generated_at", "heartbeat_at", "expected_count", "observed_count",
    "coverage", "published_at", "capture_mode", "quality_status", "evidence_json",
    "created_at",
)


ACCOUNT_SQL = """
SELECT account_id, account_name, status, initial_cash, cash_balance,
       peak_equity, policy_version, policy_hash, fee_profile_version,
       instrument_rule_version, real_trading_enabled, created_at, updated_at
FROM st_trade_account_v2
WHERE account_id = :account_id
ORDER BY account_id
"""
INTENT_SQL = """
SELECT intent_id, account_id, decision_run_uid, strategy_version, stock_code,
       theme_code, action, current_quantity, target_quantity, target_weight,
       earliest_at, expires_at, limit_price, worst_price, initial_stop,
       protective_stop, invalidation_condition, reason_code, evidence_json,
       intent_version, idempotency_key, created_at
FROM st_trade_intent_v2
WHERE account_id = :account_id
ORDER BY created_at, intent_id
"""
ORDER_SQL = """
SELECT order_id, account_id, intent_id, stock_code, side, order_type,
       limit_price, quantity, filled_quantity, status, waiting_reason,
       earliest_at, expires_at, idempotency_key, created_at, updated_at
FROM st_order_v2
WHERE account_id = :account_id
ORDER BY created_at, order_id
"""
FILL_SQL = """
SELECT fill_id, order_id, account_id, stock_code, side, quantity, price,
       gross_amount, fee_amount, net_cash_amount, quote_event_id,
       match_event_id, idempotency_key, filled_at, created_at
FROM st_fill_v2
WHERE account_id = :account_id
ORDER BY filled_at, fill_id
"""
LOT_SQL = """
SELECT lot_id, account_id, stock_code, theme_code, strategy_version,
       opened_fill_id, opened_trade_date, settlement_date, original_quantity,
       remaining_quantity, cost_price, allocated_buy_fee, position_state,
       approved_target_quantity, add_count, initial_stop, protective_stop,
       invalidation_condition, version, created_at, closed_at
FROM st_position_lot_v2
WHERE account_id = :account_id
ORDER BY opened_trade_date, lot_id
"""
CASH_SQL = """
SELECT cash_event_id, account_id, business_event_key, event_type, amount,
       balance_after, related_order_id, related_fill_id, reversal_of,
       occurred_at, created_at
FROM st_cash_ledger_v2
WHERE account_id = :account_id
ORDER BY occurred_at, cash_event_id
"""
RULE_SQL = """
SELECT stock_code, rule_version, effective_from, effective_to, security_type,
       exchange_code, can_buy, first_buy_minimum, buy_lot_size, sell_lot_size,
       settlement_days, tick_size, limit_ratio, special_treatment, suspended,
       permission_required, permission_confirmed, fee_profile_version,
       source_snapshot_hash, created_at
FROM st_instrument_rule_v2
WHERE stock_code IN :stock_codes
ORDER BY stock_code, effective_from, rule_version
"""
FEE_SQL = """
SELECT fee_profile_version, effective_from, effective_to, security_type,
       buy_commission_rate, sell_commission_rate, minimum_commission,
       stamp_tax_sell_rate, transfer_fee_buy_rate, transfer_fee_sell_rate,
       other_fee_json, evidence_hash, confirmation_status, created_at
FROM st_fee_profile_v2
WHERE fee_profile_version IN :fee_versions
ORDER BY fee_profile_version, security_type, effective_from
"""
CALENDAR_SQL = """
SELECT calendar_year, trade_date, trade_status, day_week, etl_sync_at
FROM si_trade_calendar
WHERE trade_date BETWEEN :start_date AND :end_date
ORDER BY trade_date, calendar_year
"""
QMT_MINUTE_RECEIPT_SQL = """
SELECT receipt_id, trade_date, first_trade_time, last_trade_time,
       expected_count, observed_count, coverage, row_count, source_provider,
       capture_mode, forward_eligible, quality_status, evidence_json, created_at
FROM st_qmt_minute_sync_receipt_v2
WHERE trade_date BETWEEN :receipt_start_date AND :receipt_end_date
  AND first_trade_time <= :knowledge_at
ORDER BY trade_date, first_trade_time, last_trade_time, source_provider,
         capture_mode, receipt_id
"""
PUBLIC_RECEIPT_SQL = """
SELECT batch_id, trade_date, quote_at, received_at, expected_count,
       observed_count, coverage, provider_count, minimum_sources_per_symbol,
       agreement_ratio, source_provider, maximum_price_deviation_pct,
       maximum_source_latency_seconds, quality_status, provider_status_json,
       evidence_json, created_at
FROM st_public_quote_receipt_v2
WHERE trade_date BETWEEN :receipt_start_date AND :receipt_end_date
  AND quote_at <= :knowledge_at
ORDER BY quote_at, batch_id
"""
QMT_REALTIME_RECEIPT_SQL = """
SELECT receipt_id, source_provider, source_snapshot_token,
       source_full_file_token, source_generated_at, heartbeat_at,
       expected_count, observed_count, coverage, published_at, capture_mode,
       quality_status, evidence_json, created_at
FROM st_qmt_realtime_sync_receipt_v2
WHERE source_generated_at >= :receipt_start_at
  AND source_generated_at < :receipt_end_exclusive_at
  AND source_generated_at <= :knowledge_at
ORDER BY source_generated_at, published_at, receipt_id
"""


def _sha256(value: object, field_name: str) -> str:
    if type(value) is not str:
        raise TypeError(f"{field_name} must be exactly str")
    normalized = value.lower()
    if len(normalized) != 64 or any(item not in "0123456789abcdef" for item in normalized):
        raise ValueError(f"{field_name} must be a SHA-256 digest")
    return normalized


def _text_value(row: Mapping[str, Any], name: str, *, optional: bool = False) -> str | None:
    value = row[name]
    if value is None and optional:
        return None
    if type(value) is not str or (not optional and not value):
        raise V2CanonicalSnapshotInvariantError(f"{name} must be non-empty str")
    return value


def _integer_value(
    row: Mapping[str, Any],
    name: str,
    *,
    minimum: int | None = None,
) -> int:
    value = row[name]
    if type(value) is not int:
        raise V2CanonicalSnapshotInvariantError(f"{name} must be exactly int")
    if minimum is not None and value < minimum:
        raise V2CanonicalSnapshotInvariantError(f"{name} is below minimum")
    return value


def _calendar_integer_value(
    row: Mapping[str, Any],
    name: str,
    *,
    optional: bool = False,
) -> int | None:
    """Normalize the legacy calendar's integral DECIMAL columns strictly."""

    value = row[name]
    if value is None and optional:
        return None
    if type(value) is int:
        return value
    if type(value) is Decimal and value.is_finite() and value == value.to_integral_value():
        return int(value)
    raise V2CanonicalSnapshotInvariantError(
        f"{name} must be an integral Decimal/int"
    )


def _bool_value(row: Mapping[str, Any], name: str) -> bool:
    value = row[name]
    if type(value) is bool:
        return value
    if type(value) is int and value in (0, 1):
        return bool(value)
    raise V2CanonicalSnapshotInvariantError(f"{name} must be bool/0/1")


def _decimal_value(
    row: Mapping[str, Any],
    name: str,
    *,
    optional: bool = False,
    minimum: Decimal | None = None,
) -> Decimal | None:
    value = row[name]
    if value is None and optional:
        return None
    if type(value) is not Decimal or not value.is_finite():
        raise V2CanonicalSnapshotInvariantError(f"{name} must be finite Decimal")
    if minimum is not None and value < minimum:
        raise V2CanonicalSnapshotInvariantError(f"{name} is below minimum")
    return value


def _ratio_value(row: Mapping[str, Any], name: str) -> Decimal:
    value = _decimal_value(row, name, minimum=Decimal("0"))
    assert isinstance(value, Decimal)
    if value > Decimal("1"):
        raise V2CanonicalSnapshotInvariantError(f"{name} must not exceed 1")
    return value


def _receipt_counts(
    row: Mapping[str, Any],
    *,
    prefix: str,
) -> tuple[int, int, Decimal]:
    expected = _integer_value(row, "expected_count", minimum=0)
    observed = _integer_value(row, "observed_count", minimum=0)
    coverage = _ratio_value(row, "coverage")
    if observed > expected:
        raise V2CanonicalSnapshotInvariantError(
            f"{prefix} observed_count exceeds expected_count"
        )
    expected_coverage = Decimal(observed) / Decimal(max(expected, 1))
    if abs(coverage - expected_coverage) > Decimal("0.000000005"):
        raise V2CanonicalSnapshotInvariantError(
            f"{prefix} coverage differs from receipt counts"
        )
    return expected, observed, coverage


def _date_value(row: Mapping[str, Any], name: str, *, optional: bool = False) -> date | None:
    value = row[name]
    if value is None and optional:
        return None
    if type(value) is not date:
        raise V2CanonicalSnapshotInvariantError(f"{name} must be exactly date")
    return value


def _datetime_value(
    row: Mapping[str, Any],
    name: str,
    *,
    optional: bool = False,
) -> datetime | None:
    value = row[name]
    if value is None and optional:
        return None
    if type(value) is not datetime:
        raise V2CanonicalSnapshotInvariantError(f"{name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=V2_MARKET_TIMEZONE)
    return value.astimezone(V2_MARKET_TIMEZONE)


def _canonical_json_text(row: Mapping[str, Any], name: str) -> str:
    raw = _text_value(row, name)
    assert isinstance(raw, str)

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise V2CanonicalSnapshotInvariantError(
                    f"{name} contains duplicate JSON key {key}"
                )
            result[key] = value
        return result

    try:
        parsed = json.loads(raw, object_pairs_hook=unique_object)
        return json.dumps(
            parsed,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except V2CanonicalSnapshotInvariantError:
        raise
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise V2CanonicalSnapshotInvariantError(f"{name} is not strict JSON") from exc


def _exact_columns(
    row: Mapping[str, Any],
    expected: Sequence[str],
    table_name: str,
) -> Mapping[str, Any]:
    actual = frozenset(row.keys())
    wanted = frozenset(expected)
    if actual != wanted:
        raise _SchemaReadBlocked(table_name, "COLUMN_SET_MISMATCH")
    return row


def _not_future(value: datetime | None, knowledge_at: datetime, field_name: str) -> None:
    if value is not None and value > knowledge_at:
        raise V2CanonicalSnapshotInvariantError(
            f"{field_name} is visible only after knowledge_at"
        )


def _rows(
    connection: Connection,
    *,
    table_name: str,
    sql: str,
    parameters: Mapping[str, Any],
    expanding: str | None = None,
) -> tuple[Mapping[str, Any], ...]:
    if not sql.lstrip().upper().startswith("SELECT "):
        raise AssertionError("canonical V2 facade attempted non-SELECT SQL")
    try:
        statement = text(sql)
        if expanding is not None:
            statement = statement.bindparams(bindparam(expanding, expanding=True))
        result = connection.execute(statement, dict(parameters))
        return tuple(result.mappings().all())
    except SQLAlchemyError as exc:
        raise _SchemaReadBlocked(table_name, "SELECT_FAILED") from exc


def _account(row: Mapping[str, Any], knowledge_at: datetime) -> V2TradeAccountRow:
    row = _exact_columns(row, ACCOUNT_COLUMNS, "st_trade_account_v2")
    created_at = _datetime_value(row, "created_at")
    updated_at = _datetime_value(row, "updated_at")
    assert isinstance(created_at, datetime) and isinstance(updated_at, datetime)
    _not_future(created_at, knowledge_at, "account.created_at")
    _not_future(updated_at, knowledge_at, "account.updated_at")
    if updated_at < created_at:
        raise V2CanonicalSnapshotInvariantError(
            "account updated_at precedes created_at"
        )
    status = str(_text_value(row, "status"))
    if status not in ACCOUNT_STATUSES:
        raise V2CanonicalSnapshotInvariantError("account status is unsupported")
    return V2TradeAccountRow(
        account_id=str(_text_value(row, "account_id")),
        account_name=str(_text_value(row, "account_name")),
        status=status,
        initial_cash=_decimal_value(row, "initial_cash", minimum=Decimal("0")),  # type: ignore[arg-type]
        cash_balance=_decimal_value(row, "cash_balance", minimum=Decimal("0")),  # type: ignore[arg-type]
        peak_equity=_decimal_value(row, "peak_equity", minimum=Decimal("0")),  # type: ignore[arg-type]
        policy_version=str(_text_value(row, "policy_version")),
        policy_hash=_sha256(_text_value(row, "policy_hash"), "policy_hash"),
        fee_profile_version=_text_value(row, "fee_profile_version", optional=True),
        instrument_rule_version=_text_value(row, "instrument_rule_version", optional=True),
        real_trading_enabled=_bool_value(row, "real_trading_enabled"),
        created_at=created_at,
        updated_at=updated_at,
    )


def _intent(row: Mapping[str, Any], knowledge_at: datetime) -> V2TradeIntentRow:
    row = _exact_columns(row, INTENT_COLUMNS, "st_trade_intent_v2")
    created_at = _datetime_value(row, "created_at")
    earliest_at = _datetime_value(row, "earliest_at")
    expires_at = _datetime_value(row, "expires_at")
    assert all(isinstance(item, datetime) for item in (created_at, earliest_at, expires_at))
    _not_future(created_at, knowledge_at, "intent.created_at")
    if earliest_at >= expires_at:
        raise V2CanonicalSnapshotInvariantError("intent time window is invalid")
    if created_at >= expires_at:
        raise V2CanonicalSnapshotInvariantError(
            "intent was created after its expiry"
        )
    action = str(_text_value(row, "action"))
    if action not in INTENT_ACTIONS:
        raise V2CanonicalSnapshotInvariantError("intent action is unsupported")
    return V2TradeIntentRow(
        intent_id=str(_text_value(row, "intent_id")),
        account_id=str(_text_value(row, "account_id")),
        decision_run_uid=str(_text_value(row, "decision_run_uid")),
        strategy_version=str(_text_value(row, "strategy_version")),
        stock_code=str(_text_value(row, "stock_code")),
        theme_code=str(_text_value(row, "theme_code", optional=True) or ""),
        action=action,
        current_quantity=_integer_value(row, "current_quantity", minimum=0),
        target_quantity=_integer_value(row, "target_quantity", minimum=0),
        target_weight=_decimal_value(row, "target_weight", minimum=Decimal("0")),  # type: ignore[arg-type]
        earliest_at=earliest_at, expires_at=expires_at,
        limit_price=_decimal_value(row, "limit_price", minimum=Decimal("0")),  # type: ignore[arg-type]
        worst_price=_decimal_value(row, "worst_price", minimum=Decimal("0")),  # type: ignore[arg-type]
        initial_stop=_decimal_value(row, "initial_stop", minimum=Decimal("0")),  # type: ignore[arg-type]
        protective_stop=_decimal_value(row, "protective_stop", minimum=Decimal("0")),  # type: ignore[arg-type]
        invalidation_condition=str(_text_value(row, "invalidation_condition")),
        reason_code=str(_text_value(row, "reason_code")),
        evidence_json=_canonical_json_text(row, "evidence_json"),
        intent_version=_integer_value(row, "intent_version", minimum=1),
        idempotency_key=str(_text_value(row, "idempotency_key")),
        created_at=created_at,
    )


def _order(row: Mapping[str, Any], knowledge_at: datetime) -> V2OrderRow:
    row = _exact_columns(row, ORDER_COLUMNS, "st_order_v2")
    earliest_at = _datetime_value(row, "earliest_at")
    expires_at = _datetime_value(row, "expires_at")
    created_at = _datetime_value(row, "created_at")
    updated_at = _datetime_value(row, "updated_at")
    assert all(isinstance(item, datetime) for item in (earliest_at, expires_at, created_at, updated_at))
    _not_future(created_at, knowledge_at, "order.created_at")
    _not_future(updated_at, knowledge_at, "order.updated_at")
    if (
        earliest_at >= expires_at
        or created_at >= expires_at
        or updated_at < created_at
    ):
        raise V2CanonicalSnapshotInvariantError("order timestamps are invalid")
    side = str(_text_value(row, "side"))
    order_type = str(_text_value(row, "order_type"))
    status = str(_text_value(row, "status"))
    if side not in {"BUY", "SELL"}:
        raise V2CanonicalSnapshotInvariantError("order side is unsupported")
    if order_type != "LIMIT":
        raise V2CanonicalSnapshotInvariantError("order_type must be LIMIT")
    if status not in ORDER_STATUSES:
        raise V2CanonicalSnapshotInvariantError("order status is unsupported")
    return V2OrderRow(
        order_id=str(_text_value(row, "order_id")),
        account_id=str(_text_value(row, "account_id")),
        intent_id=str(_text_value(row, "intent_id")),
        stock_code=str(_text_value(row, "stock_code")),
        side=side,
        order_type=order_type,
        limit_price=_decimal_value(row, "limit_price", minimum=Decimal("0")),  # type: ignore[arg-type]
        quantity=_integer_value(row, "quantity", minimum=1),
        filled_quantity=_integer_value(row, "filled_quantity", minimum=0),
        status=status,
        waiting_reason=_text_value(row, "waiting_reason", optional=True),
        earliest_at=earliest_at, expires_at=expires_at,
        idempotency_key=str(_text_value(row, "idempotency_key")),
        created_at=created_at, updated_at=updated_at,
    )


def _fill(row: Mapping[str, Any], knowledge_at: datetime) -> V2FillRow:
    row = _exact_columns(row, FILL_COLUMNS, "st_fill_v2")
    filled_at = _datetime_value(row, "filled_at")
    created_at = _datetime_value(row, "created_at")
    assert isinstance(filled_at, datetime) and isinstance(created_at, datetime)
    _not_future(filled_at, knowledge_at, "fill.filled_at")
    _not_future(created_at, knowledge_at, "fill.created_at")
    if created_at < filled_at:
        raise V2CanonicalSnapshotInvariantError("fill created_at precedes filled_at")
    side = str(_text_value(row, "side"))
    if side not in {"BUY", "SELL"}:
        raise V2CanonicalSnapshotInvariantError("fill side is unsupported")
    return V2FillRow(
        fill_id=str(_text_value(row, "fill_id")),
        order_id=str(_text_value(row, "order_id")),
        account_id=str(_text_value(row, "account_id")),
        stock_code=str(_text_value(row, "stock_code")),
        side=side,
        quantity=_integer_value(row, "quantity", minimum=1),
        price=_decimal_value(row, "price", minimum=Decimal("0.000001")),  # type: ignore[arg-type]
        gross_amount=_decimal_value(row, "gross_amount", minimum=Decimal("0.01")),  # type: ignore[arg-type]
        fee_amount=_decimal_value(row, "fee_amount", minimum=Decimal("0")),  # type: ignore[arg-type]
        net_cash_amount=_decimal_value(row, "net_cash_amount"),  # type: ignore[arg-type]
        quote_event_id=str(_text_value(row, "quote_event_id")),
        match_event_id=str(_text_value(row, "match_event_id")),
        idempotency_key=str(_text_value(row, "idempotency_key")),
        filled_at=filled_at, created_at=created_at,
    )


def _lot(row: Mapping[str, Any], knowledge_at: datetime) -> V2PositionLotRow:
    row = _exact_columns(row, LOT_COLUMNS, "st_position_lot_v2")
    created_at = _datetime_value(row, "created_at")
    closed_at = _datetime_value(row, "closed_at", optional=True)
    opened_trade_date = _date_value(row, "opened_trade_date")
    settlement_date = _date_value(row, "settlement_date")
    assert isinstance(created_at, datetime)
    assert isinstance(opened_trade_date, date) and isinstance(settlement_date, date)
    _not_future(created_at, knowledge_at, "lot.created_at")
    _not_future(closed_at, knowledge_at, "lot.closed_at")
    if settlement_date < opened_trade_date:
        raise V2CanonicalSnapshotInvariantError("lot settlement precedes open date")
    if closed_at is not None and closed_at < created_at:
        raise V2CanonicalSnapshotInvariantError("lot closed_at precedes created_at")
    position_state = str(_text_value(row, "position_state"))
    if position_state not in POSITION_STATES:
        raise V2CanonicalSnapshotInvariantError("position_state is unsupported")
    return V2PositionLotRow(
        lot_id=str(_text_value(row, "lot_id")), account_id=str(_text_value(row, "account_id")),
        stock_code=str(_text_value(row, "stock_code")),
        theme_code=str(_text_value(row, "theme_code", optional=True) or ""),
        strategy_version=str(_text_value(row, "strategy_version")),
        opened_fill_id=str(_text_value(row, "opened_fill_id")),
        opened_trade_date=opened_trade_date, settlement_date=settlement_date,
        original_quantity=_integer_value(row, "original_quantity", minimum=1),
        remaining_quantity=_integer_value(row, "remaining_quantity", minimum=0),
        cost_price=_decimal_value(row, "cost_price", minimum=Decimal("0.000001")),  # type: ignore[arg-type]
        allocated_buy_fee=_decimal_value(row, "allocated_buy_fee", minimum=Decimal("0")),  # type: ignore[arg-type]
        position_state=position_state,
        approved_target_quantity=_integer_value(row, "approved_target_quantity", minimum=0),
        add_count=_integer_value(row, "add_count", minimum=0),
        initial_stop=_decimal_value(row, "initial_stop", minimum=Decimal("0")),  # type: ignore[arg-type]
        protective_stop=_decimal_value(row, "protective_stop", minimum=Decimal("0")),  # type: ignore[arg-type]
        invalidation_condition=str(_text_value(row, "invalidation_condition")),
        version=_integer_value(row, "version", minimum=1), created_at=created_at, closed_at=closed_at,
    )


def _cash(row: Mapping[str, Any], knowledge_at: datetime) -> V2CashLedgerRow:
    row = _exact_columns(row, CASH_COLUMNS, "st_cash_ledger_v2")
    occurred_at = _datetime_value(row, "occurred_at")
    created_at = _datetime_value(row, "created_at")
    assert isinstance(occurred_at, datetime) and isinstance(created_at, datetime)
    _not_future(occurred_at, knowledge_at, "cash.occurred_at")
    _not_future(created_at, knowledge_at, "cash.created_at")
    if created_at < occurred_at:
        raise V2CanonicalSnapshotInvariantError("cash created_at precedes occurred_at")
    return V2CashLedgerRow(
        cash_event_id=str(_text_value(row, "cash_event_id")),
        account_id=str(_text_value(row, "account_id")),
        business_event_key=str(_text_value(row, "business_event_key")),
        event_type=str(_text_value(row, "event_type")),
        amount=_decimal_value(row, "amount"),  # type: ignore[arg-type]
        balance_after=_decimal_value(row, "balance_after", minimum=Decimal("0")),  # type: ignore[arg-type]
        related_order_id=_text_value(row, "related_order_id", optional=True),
        related_fill_id=_text_value(row, "related_fill_id", optional=True),
        reversal_of=_text_value(row, "reversal_of", optional=True),
        occurred_at=occurred_at, created_at=created_at,
    )


def _other_fee_profile(row: Mapping[str, Any]) -> tuple[str, FeeProfile]:
    canonical = _canonical_json_text(row, "other_fee_json")
    parsed = json.loads(canonical)
    if type(parsed) is not dict:
        raise V2CanonicalSnapshotInvariantError("other_fee_json must be an object")
    allowed = {
        "buy_rate", "sell_rate", "buy_fixed", "sell_fixed",
        "buy_per_share", "sell_per_share",
    }
    if set(parsed) - allowed:
        raise V2CanonicalSnapshotInvariantError("other_fee_json has unsupported fields")

    def other(name: str) -> Decimal:
        value = parsed.get(name, 0)
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            raise V2CanonicalSnapshotInvariantError(f"other fee {name} is invalid")
        converted = Decimal(str(value))
        if not converted.is_finite() or converted < 0:
            raise V2CanonicalSnapshotInvariantError(f"other fee {name} is invalid")
        return converted

    profile = FeeProfile(
        version=str(_text_value(row, "fee_profile_version")),
        buy_commission_rate=_decimal_value(row, "buy_commission_rate", minimum=Decimal("0")),  # type: ignore[arg-type]
        sell_commission_rate=_decimal_value(row, "sell_commission_rate", minimum=Decimal("0")),  # type: ignore[arg-type]
        minimum_commission=_decimal_value(row, "minimum_commission", minimum=Decimal("0")),  # type: ignore[arg-type]
        stamp_tax_sell_rate=_decimal_value(row, "stamp_tax_sell_rate", minimum=Decimal("0")),  # type: ignore[arg-type]
        transfer_fee_buy_rate=_decimal_value(row, "transfer_fee_buy_rate", minimum=Decimal("0")),  # type: ignore[arg-type]
        transfer_fee_sell_rate=_decimal_value(row, "transfer_fee_sell_rate", minimum=Decimal("0")),  # type: ignore[arg-type]
        other_buy_rate=other("buy_rate"), other_sell_rate=other("sell_rate"),
        other_buy_fixed=other("buy_fixed"), other_sell_fixed=other("sell_fixed"),
        other_buy_per_share=other("buy_per_share"), other_sell_per_share=other("sell_per_share"),
    )
    return canonical, profile


def _fee(row: Mapping[str, Any], knowledge_at: datetime) -> V2FeeProfileRow:
    row = _exact_columns(row, FEE_COLUMNS, "st_fee_profile_v2")
    created_at = _datetime_value(row, "created_at")
    effective_from = _date_value(row, "effective_from")
    effective_to = _date_value(row, "effective_to", optional=True)
    assert isinstance(created_at, datetime) and isinstance(effective_from, date)
    _not_future(created_at, knowledge_at, "fee_profile.created_at")
    if effective_to is not None and effective_to < effective_from:
        raise V2CanonicalSnapshotInvariantError("fee profile effective range is invalid")
    other_json, profile = _other_fee_profile(row)
    schedule_hash = fee_schedule_fingerprint(v2_fee_profile_to_neutral_schedule(profile))
    return V2FeeProfileRow(
        fee_profile_version=profile.version, effective_from=effective_from,
        effective_to=effective_to, security_type=str(_text_value(row, "security_type")),
        buy_commission_rate=profile.buy_commission_rate,
        sell_commission_rate=profile.sell_commission_rate,
        minimum_commission=profile.minimum_commission,
        stamp_tax_sell_rate=profile.stamp_tax_sell_rate,
        transfer_fee_buy_rate=profile.transfer_fee_buy_rate,
        transfer_fee_sell_rate=profile.transfer_fee_sell_rate,
        other_fee_json=other_json,
        evidence_hash=_sha256(_text_value(row, "evidence_hash"), "evidence_hash"),
        confirmation_status=str(_text_value(row, "confirmation_status")),
        created_at=created_at, fee_schedule_hash=schedule_hash,
    )


def _rule(row: Mapping[str, Any], knowledge_at: datetime) -> V2InstrumentRuleRow:
    row = _exact_columns(row, RULE_COLUMNS, "st_instrument_rule_v2")
    created_at = _datetime_value(row, "created_at")
    effective_from = _date_value(row, "effective_from")
    effective_to = _date_value(row, "effective_to", optional=True)
    assert isinstance(created_at, datetime) and isinstance(effective_from, date)
    _not_future(created_at, knowledge_at, "instrument_rule.created_at")
    if effective_to is not None and effective_to < effective_from:
        raise V2CanonicalSnapshotInvariantError("instrument rule effective range is invalid")
    instrument = InstrumentRule(
        stock_code=str(_text_value(row, "stock_code")),
        rule_version=str(_text_value(row, "rule_version")),
        security_type=str(_text_value(row, "security_type")),
        exchange=str(_text_value(row, "exchange_code")), effective_from=effective_from,
        effective_to=effective_to, can_buy=_bool_value(row, "can_buy"),
        first_buy_minimum=_integer_value(row, "first_buy_minimum", minimum=1),
        buy_lot_size=_integer_value(row, "buy_lot_size", minimum=1),
        sell_lot_size=_integer_value(row, "sell_lot_size", minimum=1),
        settlement_days=_integer_value(row, "settlement_days", minimum=0),
        tick_size=_decimal_value(row, "tick_size", minimum=Decimal("0.000001")),  # type: ignore[arg-type]
        limit_ratio=_decimal_value(row, "limit_ratio", optional=True, minimum=Decimal("0")),
        is_suspended=_bool_value(row, "suspended"),
        permission_required=str(_text_value(row, "permission_required", optional=True) or ""),
        permission_confirmed=_bool_value(row, "permission_confirmed"),
        fee_profile_version=str(_text_value(row, "fee_profile_version")),
    )
    adapter_instrument_hash = canonical_json_hash(asdict(instrument))
    source_snapshot_hash = _sha256(
        _text_value(row, "source_snapshot_hash"), "source_snapshot_hash"
    )
    instrument_hash = canonical_json_hash(
        {
            **asdict(instrument),
            "special_treatment": _bool_value(row, "special_treatment"),
            "source_snapshot_hash": source_snapshot_hash,
        }
    )
    return V2InstrumentRuleRow(
        stock_code=instrument.stock_code, rule_version=instrument.rule_version,
        effective_from=effective_from, effective_to=effective_to,
        security_type=instrument.security_type, exchange_code=instrument.exchange,
        can_buy=instrument.can_buy, first_buy_minimum=instrument.first_buy_minimum,
        buy_lot_size=instrument.buy_lot_size, sell_lot_size=instrument.sell_lot_size,
        settlement_days=instrument.settlement_days, tick_size=instrument.tick_size,
        limit_ratio=instrument.limit_ratio,
        special_treatment=_bool_value(row, "special_treatment"),
        suspended=instrument.is_suspended,
        permission_required=instrument.permission_required,
        permission_confirmed=instrument.permission_confirmed,
        fee_profile_version=instrument.fee_profile_version,
        source_snapshot_hash=source_snapshot_hash,
        created_at=created_at,
        adapter_instrument_rule_hash=adapter_instrument_hash,
        instrument_rule_hash=instrument_hash,
    )


def _calendar(row: Mapping[str, Any], knowledge_at: datetime) -> V2TradeCalendarRow:
    row = _exact_columns(row, CALENDAR_COLUMNS, "si_trade_calendar")
    trade_date = _date_value(row, "trade_date")
    etl_sync_at = _datetime_value(row, "etl_sync_at", optional=True)
    assert isinstance(trade_date, date)
    _not_future(etl_sync_at, knowledge_at, "trade_calendar.etl_sync_at")
    calendar_year = _calendar_integer_value(row, "calendar_year")
    assert isinstance(calendar_year, int)
    if calendar_year != trade_date.year:
        raise V2CanonicalSnapshotInvariantError("calendar year/date mismatch")
    trade_status = _calendar_integer_value(row, "trade_status")
    assert isinstance(trade_status, int)
    if trade_status not in (0, 1):
        raise V2CanonicalSnapshotInvariantError("trade_status must be 0/1")
    return V2TradeCalendarRow(
        calendar_year=calendar_year, trade_date=trade_date,
        trade_status=trade_status,
        day_week=_calendar_integer_value(row, "day_week", optional=True),
        etl_sync_at=etl_sync_at,
    )


def _qmt_minute_receipt(row: Mapping[str, Any], knowledge_at: datetime) -> V2QmtMinuteReceiptRow:
    row = _exact_columns(row, QMT_MINUTE_RECEIPT_COLUMNS, "st_qmt_minute_sync_receipt_v2")
    first = _datetime_value(row, "first_trade_time")
    last = _datetime_value(row, "last_trade_time")
    created = _datetime_value(row, "created_at")
    trade_date = _date_value(row, "trade_date")
    assert isinstance(first, datetime) and isinstance(last, datetime) and isinstance(created, datetime)
    assert isinstance(trade_date, date)
    for value, name in ((first, "first_trade_time"), (last, "last_trade_time"), (created, "created_at")):
        _not_future(value, knowledge_at, f"qmt_minute_receipt.{name}")
    if first > last or created < last:
        raise V2CanonicalSnapshotInvariantError("minute receipt window is invalid")
    if first.date() != trade_date or last.date() != trade_date:
        raise V2CanonicalSnapshotInvariantError(
            "minute receipt window differs from trade_date"
        )
    expected_count, observed_count, coverage = _receipt_counts(
        row, prefix="minute receipt"
    )
    row_count = _integer_value(row, "row_count", minimum=0)
    if row_count < observed_count:
        raise V2CanonicalSnapshotInvariantError(
            "minute receipt row_count is below observed_count"
        )
    quality_status = str(_text_value(row, "quality_status"))
    if quality_status not in RECEIPT_QUALITY_STATUSES:
        raise V2CanonicalSnapshotInvariantError(
            "minute receipt quality_status is unsupported"
        )
    return V2QmtMinuteReceiptRow(
        receipt_id=str(_text_value(row, "receipt_id")), trade_date=trade_date,
        first_trade_time=first, last_trade_time=last,
        expected_count=expected_count,
        observed_count=observed_count,
        coverage=coverage,
        row_count=row_count,
        source_provider=str(_text_value(row, "source_provider")),
        capture_mode=str(_text_value(row, "capture_mode")),
        forward_eligible=_bool_value(row, "forward_eligible"),
        quality_status=quality_status,
        evidence_json=_canonical_json_text(row, "evidence_json"), created_at=created,
    )


def _public_receipt(row: Mapping[str, Any], knowledge_at: datetime) -> V2PublicQuoteReceiptRow:
    row = _exact_columns(row, PUBLIC_RECEIPT_COLUMNS, "st_public_quote_receipt_v2")
    trade_date = _date_value(row, "trade_date")
    quote_at = _datetime_value(row, "quote_at")
    received_at = _datetime_value(row, "received_at")
    created_at = _datetime_value(row, "created_at")
    assert isinstance(trade_date, date)
    assert all(isinstance(item, datetime) for item in (quote_at, received_at, created_at))
    for value, name in ((quote_at, "quote_at"), (received_at, "received_at"), (created_at, "created_at")):
        _not_future(value, knowledge_at, f"public_quote_receipt.{name}")
    if received_at < quote_at or created_at < received_at:
        raise V2CanonicalSnapshotInvariantError("public receipt timing is invalid")
    if quote_at.date() != trade_date:
        raise V2CanonicalSnapshotInvariantError(
            "public receipt trade_date differs from quote_at"
        )
    expected_count, observed_count, coverage = _receipt_counts(
        row, prefix="public receipt"
    )
    agreement_ratio = _ratio_value(row, "agreement_ratio")
    quality_status = str(_text_value(row, "quality_status"))
    if quality_status not in RECEIPT_QUALITY_STATUSES:
        raise V2CanonicalSnapshotInvariantError(
            "public receipt quality_status is unsupported"
        )
    return V2PublicQuoteReceiptRow(
        batch_id=str(_text_value(row, "batch_id")), trade_date=trade_date,
        quote_at=quote_at, received_at=received_at,
        expected_count=expected_count,
        observed_count=observed_count,
        coverage=coverage,
        provider_count=_integer_value(row, "provider_count", minimum=0),
        minimum_sources_per_symbol=_integer_value(row, "minimum_sources_per_symbol", minimum=0),
        agreement_ratio=agreement_ratio,
        source_provider=str(_text_value(row, "source_provider")),
        maximum_price_deviation_pct=_decimal_value(row, "maximum_price_deviation_pct", minimum=Decimal("0")),  # type: ignore[arg-type]
        maximum_source_latency_seconds=_decimal_value(row, "maximum_source_latency_seconds", minimum=Decimal("0")),  # type: ignore[arg-type]
        quality_status=quality_status,
        provider_status_json=_canonical_json_text(row, "provider_status_json"),
        evidence_json=_canonical_json_text(row, "evidence_json"), created_at=created_at,
    )


def _qmt_realtime_receipt(row: Mapping[str, Any], knowledge_at: datetime) -> V2QmtRealtimeReceiptRow:
    row = _exact_columns(row, QMT_REALTIME_RECEIPT_COLUMNS, "st_qmt_realtime_sync_receipt_v2")
    source_generated_at = _datetime_value(row, "source_generated_at")
    heartbeat_at = _datetime_value(row, "heartbeat_at")
    published_at = _datetime_value(row, "published_at")
    created_at = _datetime_value(row, "created_at")
    assert all(isinstance(item, datetime) for item in (source_generated_at, heartbeat_at, published_at, created_at))
    for value, name in (
        (source_generated_at, "source_generated_at"), (heartbeat_at, "heartbeat_at"),
        (published_at, "published_at"), (created_at, "created_at"),
    ):
        _not_future(value, knowledge_at, f"qmt_realtime_receipt.{name}")
    if not (source_generated_at <= heartbeat_at <= published_at):
        raise V2CanonicalSnapshotInvariantError(
            "realtime receipt source/heartbeat/publish order is invalid"
        )
    if created_at < published_at:
        raise V2CanonicalSnapshotInvariantError("realtime receipt timing is invalid")
    expected_count, observed_count, coverage = _receipt_counts(
        row, prefix="realtime receipt"
    )
    quality_status = str(_text_value(row, "quality_status"))
    if quality_status not in RECEIPT_QUALITY_STATUSES:
        raise V2CanonicalSnapshotInvariantError(
            "realtime receipt quality_status is unsupported"
        )
    return V2QmtRealtimeReceiptRow(
        receipt_id=str(_text_value(row, "receipt_id")),
        source_provider=str(_text_value(row, "source_provider")),
        source_snapshot_token=str(_text_value(row, "source_snapshot_token")),
        source_full_file_token=str(_text_value(row, "source_full_file_token")),
        source_generated_at=source_generated_at, heartbeat_at=heartbeat_at,
        expected_count=expected_count,
        observed_count=observed_count,
        coverage=coverage,
        published_at=published_at, capture_mode=str(_text_value(row, "capture_mode")),
        quality_status=quality_status,
        evidence_json=_canonical_json_text(row, "evidence_json"), created_at=created_at,
    )


def _require_unique(values: Sequence[Any], key, label: str) -> None:
    seen: set[Any] = set()
    for item in values:
        identity = key(item)
        if identity in seen:
            raise V2CanonicalSnapshotInvariantError(f"duplicate {label}: {identity}")
        seen.add(identity)


def _cash_group_has_valid_chain(
    starting_balance: Decimal,
    group: tuple[V2CashLedgerRow, ...],
    terminal_balance: Decimal,
) -> bool:
    """Check whether all equal-time rows admit some balance-chain ordering.

    Each row is an edge from ``balance_after - amount`` to ``balance_after``.
    A chain using every row exactly once is therefore a directed Euler trail.
    Degree balance plus weak connectivity proves that such a trail exists,
    without inventing UUID order or factorial permutation search.
    """

    if not group:
        return starting_balance == terminal_balance
    in_degree: dict[Decimal, int] = {}
    out_degree: dict[Decimal, int] = {}
    neighbours: dict[Decimal, set[Decimal]] = {}
    vertices: set[Decimal] = set()
    for movement in group:
        predecessor = movement.balance_after - movement.amount
        successor = movement.balance_after
        vertices.update((predecessor, successor))
        out_degree[predecessor] = out_degree.get(predecessor, 0) + 1
        in_degree[successor] = in_degree.get(successor, 0) + 1
        neighbours.setdefault(predecessor, set()).add(successor)
        neighbours.setdefault(successor, set()).add(predecessor)

    for vertex in vertices:
        difference = out_degree.get(vertex, 0) - in_degree.get(vertex, 0)
        expected = (
            1
            if vertex == starting_balance and starting_balance != terminal_balance
            else -1
            if vertex == terminal_balance and starting_balance != terminal_balance
            else 0
        )
        if difference != expected:
            return False

    reachable = {starting_balance}
    pending = [starting_balance]
    while pending:
        current = pending.pop()
        for neighbour in neighbours.get(current, ()):
            if neighbour not in reachable:
                reachable.add(neighbour)
                pending.append(neighbour)
    return vertices <= reachable


def _validate_receipt_scope(
    *,
    fills: tuple[V2FillRow, ...],
    minute_receipts: tuple[V2QmtMinuteReceiptRow, ...],
    public_receipts: tuple[V2PublicQuoteReceiptRow, ...],
    realtime_receipts: tuple[V2QmtRealtimeReceiptRow, ...],
) -> None:
    _require_unique(minute_receipts, lambda item: item.receipt_id, "minute receipt_id")
    _require_unique(
        minute_receipts,
        lambda item: (
            item.trade_date,
            item.first_trade_time,
            item.last_trade_time,
            item.source_provider,
            item.capture_mode,
        ),
        "minute receipt window",
    )
    _require_unique(public_receipts, lambda item: item.batch_id, "public batch_id")
    _require_unique(realtime_receipts, lambda item: item.receipt_id, "realtime receipt_id")
    _require_unique(
        realtime_receipts,
        lambda item: (item.source_provider, item.source_snapshot_token),
        "realtime source snapshot",
    )
    all_receipts = (*minute_receipts, *public_receipts, *realtime_receipts)
    if not fills:
        if all_receipts:
            raise V2CanonicalSnapshotInvariantError(
                "quote receipts exist without relevant fills"
            )
        return
    fill_dates = tuple(item.filled_at.date() for item in fills)
    start_date = min(fill_dates)
    end_date = max(fill_dates)
    receipt_dates = (
        *(item.trade_date for item in minute_receipts),
        *(item.trade_date for item in public_receipts),
        *(item.source_generated_at.date() for item in realtime_receipts),
    )
    if any(item < start_date or item > end_date for item in receipt_dates):
        raise V2CanonicalSnapshotInvariantError(
            "quote receipt lies outside the relevant fill-date range"
        )


def _validate_relationships(
    *,
    account: V2TradeAccountRow,
    intents: tuple[V2TradeIntentRow, ...],
    orders: tuple[V2OrderRow, ...],
    fills: tuple[V2FillRow, ...],
    lots: tuple[V2PositionLotRow, ...],
    cash: tuple[V2CashLedgerRow, ...],
    rules: tuple[V2InstrumentRuleRow, ...],
    fees: tuple[V2FeeProfileRow, ...],
    calendar: tuple[V2TradeCalendarRow, ...],
) -> tuple[str, ...]:
    if account.real_trading_enabled:
        raise V2CanonicalSnapshotInvariantError("canonical V2 real trading guard is enabled")
    collections = (intents, orders, fills, lots, cash)
    if any(item.account_id != account.account_id for values in collections for item in values):
        raise V2CanonicalSnapshotInvariantError("row is bound to another account")

    _require_unique(intents, lambda item: item.intent_id, "intent_id")
    _require_unique(intents, lambda item: item.idempotency_key, "intent idempotency key")
    _require_unique(orders, lambda item: item.order_id, "order_id")
    _require_unique(orders, lambda item: item.idempotency_key, "order idempotency key")
    _require_unique(fills, lambda item: item.fill_id, "fill_id")
    _require_unique(fills, lambda item: item.idempotency_key, "fill idempotency key")
    _require_unique(lots, lambda item: item.lot_id, "lot_id")
    _require_unique(lots, lambda item: item.opened_fill_id, "lot opened_fill_id")
    _require_unique(cash, lambda item: item.cash_event_id, "cash_event_id")
    _require_unique(cash, lambda item: item.business_event_key, "cash business key")
    _require_unique(fees, lambda item: (item.fee_profile_version, item.security_type, item.effective_from), "fee profile row")
    _require_unique(fees, lambda item: item.evidence_hash, "fee evidence hash")
    _require_unique(rules, lambda item: (item.stock_code, item.rule_version, item.effective_from), "instrument rule row")
    _require_unique(calendar, lambda item: item.trade_date, "calendar trade_date")

    intents_by_id = {item.intent_id: item for item in intents}
    orders_by_id = {item.order_id: item for item in orders}
    fills_by_id = {item.fill_id: item for item in fills}
    latest_account_event_at = max(
        (
            *(item.filled_at for item in fills),
            *(item.occurred_at for item in cash),
            account.created_at,
        )
    )
    if account.updated_at < latest_account_event_at:
        raise V2CanonicalSnapshotInvariantError(
            "account updated_at precedes its latest logical ledger event"
        )
    action_sides = {
        "BUY": "BUY", "OPEN": "BUY", "ADD": "BUY",
        "SELL": "SELL", "REDUCE": "SELL", "EXIT": "SELL",
    }
    for order in orders:
        intent = intents_by_id.get(order.intent_id)
        if intent is None:
            raise V2CanonicalSnapshotInvariantError("order references missing intent")
        if (intent.account_id, intent.stock_code) != (order.account_id, order.stock_code):
            raise V2CanonicalSnapshotInvariantError("order/intent account or security mismatch")
        expected_side = action_sides.get(intent.action.upper())
        if expected_side is None or order.side.upper() != expected_side:
            raise V2CanonicalSnapshotInvariantError("order/intent side mismatch")
        if expected_side == "BUY":
            intended_delta = intent.target_quantity - intent.current_quantity
        else:
            intended_delta = intent.current_quantity - intent.target_quantity
        if intended_delta <= 0:
            raise V2CanonicalSnapshotInvariantError(
                "intent target direction differs from action"
            )
        if order.quantity > intended_delta:
            raise V2CanonicalSnapshotInvariantError("order quantity exceeds intent delta")
        if order.filled_quantity > order.quantity:
            raise V2CanonicalSnapshotInvariantError("order overfilled")
        if (
            order.earliest_at != intent.earliest_at
            or order.expires_at != intent.expires_at
            or order.limit_price != intent.limit_price
        ):
            raise V2CanonicalSnapshotInvariantError(
                "order execution terms differ from intent"
            )
        if order.created_at < intent.created_at:
            raise V2CanonicalSnapshotInvariantError(
                "order created_at precedes intent created_at"
            )

    fill_quantity_by_order: dict[str, int] = {}
    for fill in fills:
        order = orders_by_id.get(fill.order_id)
        if order is None:
            raise V2CanonicalSnapshotInvariantError("fill references missing order")
        if (fill.account_id, fill.stock_code, fill.side.upper()) != (
            order.account_id, order.stock_code, order.side.upper()
        ):
            raise V2CanonicalSnapshotInvariantError("fill/order identity mismatch")
        executable_from = max(order.created_at, order.earliest_at)
        if not (executable_from <= fill.filled_at < order.expires_at):
            raise V2CanonicalSnapshotInvariantError(
                "fill lies outside the order execution window"
            )
        if order.side.upper() == "BUY" and fill.price > order.limit_price:
            raise V2CanonicalSnapshotInvariantError("BUY fill exceeds limit price")
        if order.side.upper() == "SELL" and fill.price < order.limit_price:
            raise V2CanonicalSnapshotInvariantError("SELL fill is below limit price")
        fill_quantity_by_order[fill.order_id] = fill_quantity_by_order.get(fill.order_id, 0) + fill.quantity
        expected_gross = (fill.price * fill.quantity).quantize(
            Decimal("0.01"), rounding=ROUND_DOWN
        )
        if fill.gross_amount != expected_gross:
            raise V2CanonicalSnapshotInvariantError("fill gross amount is inconsistent")
        expected_net = (
            -(fill.gross_amount + fill.fee_amount)
            if fill.side.upper() == "BUY"
            else fill.gross_amount - fill.fee_amount
        )
        if fill.net_cash_amount != expected_net:
            raise V2CanonicalSnapshotInvariantError("fill net cash amount is inconsistent")
    for order in orders:
        if order.filled_quantity != fill_quantity_by_order.get(order.order_id, 0):
            raise V2CanonicalSnapshotInvariantError("order filled quantity differs from fills")
        if order.filled_quantity == order.quantity and order.status != "FILLED":
            raise V2CanonicalSnapshotInvariantError(
                "fully filled order does not have FILLED status"
            )
        if order.status == "FILLED" and order.filled_quantity != order.quantity:
            raise V2CanonicalSnapshotInvariantError(
                "FILLED order quantity is incomplete"
            )
        if order.status == "PARTIALLY_FILLED" and not (
            0 < order.filled_quantity < order.quantity
        ):
            raise V2CanonicalSnapshotInvariantError(
                "PARTIALLY_FILLED order quantity is invalid"
            )
        if order.status in {
            "CREATED",
            "RISK_APPROVED",
            "QUEUED",
            "REJECTED",
        } and order.filled_quantity != 0:
            raise V2CanonicalSnapshotInvariantError(
                "unfilled order status carries a filled quantity"
            )
        order_fills = tuple(item for item in fills if item.order_id == order.order_id)
        if order_fills and order.updated_at < max(item.filled_at for item in order_fills):
            raise V2CanonicalSnapshotInvariantError(
                "order updated_at precedes its latest fill"
            )

    lots_by_fill = {item.opened_fill_id: item for item in lots}
    for fill in fills:
        lot = lots_by_fill.get(fill.fill_id)
        if fill.side.upper() == "BUY":
            if lot is None:
                raise V2CanonicalSnapshotInvariantError("BUY fill is missing its position lot")
            order = orders_by_id[fill.order_id]
            intent = intents_by_id[order.intent_id]
            if (
                lot.lot_id != f"LOT:{fill.fill_id}"
                or lot.account_id != fill.account_id
                or lot.stock_code != fill.stock_code
                or lot.original_quantity != fill.quantity
                or lot.cost_price != fill.price
                or lot.allocated_buy_fee != fill.fee_amount
                or lot.opened_trade_date != fill.filled_at.date()
                or lot.created_at < fill.filled_at
                or lot.theme_code != intent.theme_code
                or lot.strategy_version != intent.strategy_version
                or lot.initial_stop != intent.initial_stop
                or lot.invalidation_condition != intent.invalidation_condition
            ):
                raise V2CanonicalSnapshotInvariantError("position lot differs from opening fill")
        elif lot is not None:
            raise V2CanonicalSnapshotInvariantError("SELL fill cannot open a lot")
    for lot in lots:
        fill = fills_by_id.get(lot.opened_fill_id)
        if fill is None or fill.side.upper() != "BUY":
            raise V2CanonicalSnapshotInvariantError("lot references missing/non-BUY fill")
        if lot.remaining_quantity > lot.original_quantity:
            raise V2CanonicalSnapshotInvariantError("lot remaining quantity exceeds original")
        if lot.remaining_quantity == 0:
            if lot.position_state != "CLOSED" or lot.closed_at is None:
                raise V2CanonicalSnapshotInvariantError("closed lot state is incomplete")
        elif lot.closed_at is not None or lot.position_state == "CLOSED":
            raise V2CanonicalSnapshotInvariantError("open lot carries closed state")

    replay_remaining = {lot.lot_id: lot.original_quantity for lot in lots}
    lots_in_fifo = tuple(sorted(lots, key=lambda item: (item.opened_trade_date, item.lot_id)))
    for fill in fills:
        if fill.side.upper() != "SELL":
            continue
        remaining = fill.quantity
        trade_date = fill.filled_at.date()
        for lot in lots_in_fifo:
            if (
                remaining == 0
                or lot.stock_code != fill.stock_code
                or lot.settlement_date > trade_date
                or replay_remaining[lot.lot_id] == 0
            ):
                continue
            consumed = min(remaining, replay_remaining[lot.lot_id])
            replay_remaining[lot.lot_id] -= consumed
            remaining -= consumed
        if remaining:
            raise V2CanonicalSnapshotInvariantError("SELL fill has insufficient FIFO lots")
    if any(lot.remaining_quantity != replay_remaining[lot.lot_id] for lot in lots):
        raise V2CanonicalSnapshotInvariantError("materialized lots differ from FIFO fill replay")

    if not cash:
        raise V2CanonicalSnapshotInvariantError("cash ledger is missing initial deposit")
    running = Decimal("0.00")
    fill_cash: dict[str, V2CashLedgerRow] = {}
    cash_by_id = {item.cash_event_id: item for item in cash}
    initial_count = 0
    blockers: set[str] = set()
    reversed_event_ids: set[str] = set()
    for movement in cash:
        if movement.reversal_of is not None:
            reversed_movement = cash_by_id.get(movement.reversal_of)
            if reversed_movement is None:
                raise V2CanonicalSnapshotInvariantError(
                    "cash reversal references missing event"
                )
            if movement.reversal_of in reversed_event_ids:
                raise V2CanonicalSnapshotInvariantError(
                    "cash event was reversed more than once"
                )
            if reversed_movement.occurred_at > movement.occurred_at:
                raise V2CanonicalSnapshotInvariantError(
                    "cash reversal precedes reversed event"
                )
            if movement.amount != -reversed_movement.amount:
                raise V2CanonicalSnapshotInvariantError(
                    "cash reversal amount differs from reversed event"
                )
            reversed_event_ids.add(movement.reversal_of)
        if movement.event_type == "INITIAL_DEPOSIT":
            initial_count += 1
            if (
                movement.amount != account.initial_cash
                or movement.balance_after != account.initial_cash
                or movement.business_event_key
                != f"{account.account_id}:INITIAL_DEPOSIT"
                or movement.related_order_id is not None
                or movement.related_fill_id is not None
                or movement.reversal_of is not None
                or movement.occurred_at != account.created_at
                or movement.created_at != account.created_at
            ):
                raise V2CanonicalSnapshotInvariantError("initial deposit differs from account")
        elif movement.event_type in {"BUY_FILL", "SELL_FILL"}:
            if movement.related_fill_id is None or movement.related_order_id is None:
                raise V2CanonicalSnapshotInvariantError("fill cash event is missing linkage")
            if movement.related_fill_id in fill_cash:
                raise V2CanonicalSnapshotInvariantError("fill has duplicate cash events")
            fill_cash[movement.related_fill_id] = movement
        else:
            blockers.add("UNSUPPORTED_CASH_EVENT_TYPE")

    index = 0
    while index < len(cash):
        group_end = index + 1
        while (
            group_end < len(cash)
            and cash[group_end].occurred_at == cash[index].occurred_at
        ):
            group_end += 1
        group = cash[index:group_end]
        if len(group) == 1:
            running = (running + group[0].amount).quantize(
                Decimal("0.01"), rounding=ROUND_DOWN
            )
            if group[0].balance_after != running:
                raise V2CanonicalSnapshotInvariantError(
                    "cash balance chain is inconsistent"
                )
        else:
            # UUID/hash order is not an event sequence.  Verify only the net
            # group effect and require a row that materializes its terminal
            # balance; exact within-group replay remains explicitly BLOCKED.
            blockers.add("CASH_LEDGER_SEQUENCE_NOT_PERSISTED")
            terminal_balance = (
                running + sum((item.amount for item in group), Decimal("0"))
            ).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            if not _cash_group_has_valid_chain(running, group, terminal_balance):
                raise V2CanonicalSnapshotInvariantError(
                    "equal-time cash group has no valid balance chain"
                )
            running = terminal_balance
        index = group_end
    if initial_count != 1:
        raise V2CanonicalSnapshotInvariantError("cash ledger requires one initial deposit")
    if running != account.cash_balance:
        raise V2CanonicalSnapshotInvariantError("account cash differs from cash ledger")
    for fill in fills:
        movement = fill_cash.get(fill.fill_id)
        if movement is None:
            raise V2CanonicalSnapshotInvariantError("fill is missing cash ledger event")
        if (
            movement.related_order_id != fill.order_id
            or movement.amount != fill.net_cash_amount
            or movement.event_type != f"{fill.side.upper()}_FILL"
            or movement.business_event_key != f"FILL:{fill.idempotency_key}"
            or movement.reversal_of is not None
            or movement.occurred_at != fill.filled_at
            or movement.created_at < fill.created_at
        ):
            raise V2CanonicalSnapshotInvariantError("cash event differs from fill")
    if set(fill_cash) != set(fills_by_id):
        raise V2CanonicalSnapshotInvariantError("cash ledger references missing fill")

    relevant_codes = {item.stock_code for item in (*intents, *orders, *fills, *lots)}
    rules_by_code = {code: [item for item in rules if item.stock_code == code] for code in relevant_codes}
    if any(not rows for rows in rules_by_code.values()):
        raise V2CanonicalSnapshotInvariantError("security is missing instrument rule rows")
    fee_keys = {(item.fee_profile_version, item.security_type) for item in fees}
    if any((rule.fee_profile_version, rule.security_type) not in fee_keys for rule in rules):
        raise V2CanonicalSnapshotInvariantError("instrument rule is missing fee profile rows")

    trading_days = tuple(item.trade_date for item in calendar if item.trade_status == 1)
    trading_day_set = set(trading_days)
    for fill in fills:
        if fill.filled_at.date() not in trading_day_set:
            raise V2CanonicalSnapshotInvariantError("fill date is missing from trade calendar")
    for lot in lots:
        candidates = [
            day for day in trading_days
            if day >= lot.opened_trade_date and day <= lot.settlement_date
        ]
        if not candidates or candidates[0] != lot.opened_trade_date or candidates[-1] != lot.settlement_date:
            raise V2CanonicalSnapshotInvariantError("lot settlement path is missing calendar rows")

    if fills:
        blockers.update(
            {
                "CASH_LEDGER_SEQUENCE_NOT_PERSISTED",
                "FILL_ACCOUNTING_REQUEST_HASH_NOT_PERSISTED",
                "FILL_FEE_SCHEDULE_BINDING_NOT_PERSISTED",
                "FILL_INSTRUMENT_RULE_BINDING_NOT_PERSISTED",
                "FILL_QUOTE_EVENT_SOURCE_BATCH_UNAVAILABLE",
                "FILL_QUOTE_RECEIPT_BINDING_NOT_PERSISTED",
                "FILL_SEQUENCE_NOT_PERSISTED",
                "QUOTE_EVENT_ROW_UNAVAILABLE",
                "TRADE_CALENDAR_SESSION_AUTHORITY_NOT_PERSISTED",
            }
        )
    if orders:
        blockers.update(
            {
                "ORDER_RISK_APPROVAL_BINDING_UNAVAILABLE",
                "ORDER_TRANSITION_HISTORY_NOT_PERSISTED",
            }
        )
    if lots:
        blockers.add("LOT_STATE_HISTORY_NOT_PERSISTED")
    if any(fill.side.upper() == "BUY" for fill in fills):
        blockers.update(
            {
                "FILL_SETTLEMENT_EVIDENCE_HASH_NOT_PERSISTED",
                "TRADE_CALENDAR_VERSION_NOT_PERSISTED",
            }
        )
    if any(fill.side.upper() == "SELL" for fill in fills):
        blockers.add("SELL_LOT_CONSUMPTION_EVENT_NOT_PERSISTED")
    if any(item.etl_sync_at is None for item in calendar):
        blockers.add("TRADE_CALENDAR_VISIBILITY_TIME_NOT_PERSISTED")
    return tuple(sorted(blockers))


TABLE_ROWS = (
    ("st_trade_account_v2", "account"),
    ("st_trade_intent_v2", "intents"),
    ("st_order_v2", "orders"),
    ("st_fill_v2", "fills"),
    ("st_position_lot_v2", "lots"),
    ("st_cash_ledger_v2", "cash_ledger"),
    ("st_fee_profile_v2", "fee_profiles"),
    ("st_instrument_rule_v2", "instrument_rules"),
    ("si_trade_calendar", "trade_calendar"),
    ("st_qmt_minute_sync_receipt_v2", "qmt_minute_receipts"),
    ("st_public_quote_receipt_v2", "public_quote_receipts"),
    ("st_qmt_realtime_sync_receipt_v2", "qmt_realtime_receipts"),
)
SCHEMA_READ_REASON_CODES = frozenset({"SELECT_FAILED", "COLUMN_SET_MISMATCH"})


BLOCKER_DEFINITIONS: Mapping[str, tuple[tuple[str, ...], str]] = {
    "CASH_LEDGER_SEQUENCE_NOT_PERSISTED": (
        ("st_cash_ledger_v2.account_ledger_sequence",),
        "cash rows have no account-local monotonic sequence, so equal-time event order is not authoritative",
    ),
    "FILL_ACCOUNTING_REQUEST_HASH_NOT_PERSISTED": (
        ("st_fill_v2.accounting_request_hash",),
        "the persisted fill cannot be matched to the exact neutral accounting request used for replay",
    ),
    "FILL_FEE_SCHEDULE_BINDING_NOT_PERSISTED": (
        ("st_fill_v2.fee_profile_version", "st_fill_v2.fee_schedule_hash"),
        "account configuration is mutable and a fill does not preserve its historical fee schedule binding",
    ),
    "FILL_INSTRUMENT_RULE_BINDING_NOT_PERSISTED": (
        ("st_fill_v2.instrument_rule_hash", "st_fill_v2.instrument_rule_version"),
        "a fill does not preserve the exact instrument rule used when it executed",
    ),
    "FILL_QUOTE_EVENT_SOURCE_BATCH_UNAVAILABLE": (
        ("st_fill_v2.quote_receipt_hash", "st_fill_v2.source_batch_id"),
        "the allowed facade tables do not provide a unique fill-to-quote-receipt source batch binding",
    ),
    "FILL_QUOTE_RECEIPT_BINDING_NOT_PERSISTED": (
        ("st_fill_v2.quote_receipt_hash", "st_fill_v2.quote_receipt_id"),
        "receipt rows may be present, but no exact immutable receipt identity is persisted on the fill",
    ),
    "FILL_SEQUENCE_NOT_PERSISTED": (
        ("st_fill_v2.order_fill_sequence",),
        "fills have no order-local monotonic sequence, so historical partial-fill fee allocation is not provable",
    ),
    "LOT_STATE_HISTORY_NOT_PERSISTED": (
        ("st_position_lot_transition_v2",),
        "lot state, stops and version are updated in place without a complete immutable transition history",
    ),
    "ORDER_RISK_APPROVAL_BINDING_UNAVAILABLE": (
        ("st_order_v2.risk_decision_hash",),
        "the permitted read scope excludes risk-decision rows and the order does not persist their decision hash",
    ),
    "ORDER_TRANSITION_HISTORY_NOT_PERSISTED": (
        ("st_order_transition_v2",),
        "the order row stores current state without a complete monotonic transition history",
    ),
    "QUOTE_EVENT_ROW_UNAVAILABLE": (
        (
            "st_fill_v2.quote_event_payload_hash",
            "st_fill_v2.quote_event_received_at",
        ),
        "the permitted read scope excludes quote-event rows, so fill quote identity and visibility cannot be replay-verified",
    ),
    "FILL_SETTLEMENT_EVIDENCE_HASH_NOT_PERSISTED": (
        ("st_fill_v2.settlement_evidence_hash",),
        "a BUY fill does not persist the calendar/rule evidence used to derive its settlement date",
    ),
    "QUOTE_RECEIPT_ROWS_MISSING": (
        (
            "st_public_quote_receipt_v2",
            "st_qmt_minute_sync_receipt_v2",
            "st_qmt_realtime_sync_receipt_v2",
        ),
        "fills exist but the transaction returned no quote receipt rows",
    ),
    "SELL_LOT_CONSUMPTION_EVENT_NOT_PERSISTED": (
        ("st_position_lot_consumption_v2",),
        "materialized FIFO quantities can be checked, but sell-to-lot consumption history is not persisted",
    ),
    "TRADE_CALENDAR_SESSION_AUTHORITY_NOT_PERSISTED": (
        ("si_trade_calendar.market_timezone", "si_trade_calendar.session_definition_hash"),
        "calendar date/status rows do not attest market timezone or executable session windows",
    ),
    "TRADE_CALENDAR_VERSION_NOT_PERSISTED": (
        ("st_fill_v2.calendar_hash", "st_fill_v2.calendar_version"),
        "the reloadable calendar is not version-bound to historical fills",
    ),
    "TRADE_CALENDAR_VISIBILITY_TIME_NOT_PERSISTED": (
        ("si_trade_calendar.etl_sync_at",),
        "one or more calendar rows lack a visibility timestamp",
    ),
    "UNSUPPORTED_CASH_EVENT_TYPE": (
        ("st_cash_ledger_v2.event_type",),
        "the current materialized balance is checkable, but an event type has no exact facade replay contract",
    ),
}


def _capability_blocker(code: str) -> V2CapabilityBlocker:
    try:
        missing, reason = BLOCKER_DEFINITIONS[code]
    except KeyError as exc:
        raise AssertionError(f"undefined capability blocker: {code}") from exc
    return V2CapabilityBlocker(
        code=code,
        missing_bindings=tuple(sorted(set(missing))),
        reason=reason,
    )


def _schema_blocker(table_name: str, reason_code: str) -> V2CapabilityBlocker:
    if table_name not in {item[0] for item in TABLE_ROWS}:
        raise AssertionError(f"unexpected canonical V2 table: {table_name}")
    if reason_code not in SCHEMA_READ_REASON_CODES:
        raise AssertionError(f"unexpected schema read reason: {reason_code}")
    return V2CapabilityBlocker(
        code="SCHEMA_READ_BLOCKED",
        missing_bindings=(table_name,),
        reason=(
            f"required typed SELECT for {table_name} failed with {reason_code}; "
            "no partial snapshot was synthesized"
        ),
    )


def _primary_key(table_name: str, row: Any) -> tuple[str, ...]:
    keys = {
        "st_trade_account_v2": ("account_id",),
        "st_trade_intent_v2": ("intent_id",),
        "st_order_v2": ("order_id",),
        "st_fill_v2": ("fill_id",),
        "st_position_lot_v2": ("lot_id",),
        "st_cash_ledger_v2": ("cash_event_id",),
        "st_fee_profile_v2": ("fee_profile_version", "security_type", "effective_from"),
        "st_instrument_rule_v2": ("stock_code", "rule_version", "effective_from"),
        "si_trade_calendar": ("calendar_year", "trade_date"),
        "st_qmt_minute_sync_receipt_v2": ("receipt_id",),
        "st_public_quote_receipt_v2": ("batch_id",),
        "st_qmt_realtime_sync_receipt_v2": ("receipt_id",),
    }[table_name]
    return tuple(str(getattr(row, name)) for name in keys)


def _manifest(
    *,
    account_id: str,
    knowledge_at: datetime,
    rows_by_name: Mapping[str, Any],
) -> V2RowManifest:
    entries: list[V2RowManifestEntry] = []
    for table_name, attribute_name in TABLE_ROWS:
        value = rows_by_name[attribute_name]
        rows = (value,) if attribute_name == "account" else value
        for ordinal, row in enumerate(rows):
            entries.append(
                V2RowManifestEntry(
                    table_name=table_name,
                    ordinal=ordinal,
                    primary_key=_primary_key(table_name, row),
                    row_hash=canonical_json_hash(
                        {"table_name": table_name, "row": asdict(row)}
                    ),
                )
            )
    entry_tuple = tuple(entries)
    root_hash = canonical_json_hash(
        {
            "schema_version": SNAPSHOT_SCHEMA_VERSION,
            "account_id": account_id,
            "knowledge_at": knowledge_at.astimezone(timezone.utc).isoformat(timespec="microseconds"),
            "entries": [asdict(item) for item in entry_tuple],
        }
    )
    return V2RowManifest(
        schema_version=SNAPSHOT_SCHEMA_VERSION,
        entries=entry_tuple,
        row_count=len(entry_tuple),
        root_hash=root_hash,
    )


def _reconstruct_rows(snapshot: V2CanonicalSnapshot) -> None:
    specifications = (
        ((snapshot.account,), _account, ()),
        (snapshot.intents, _intent, ()),
        (snapshot.orders, _order, ()),
        (snapshot.fills, _fill, ()),
        (snapshot.lots, _lot, ()),
        (snapshot.cash_ledger, _cash, ()),
        (snapshot.fee_profiles, _fee, ("fee_schedule_hash",)),
        (
            snapshot.instrument_rules,
            _rule,
            ("adapter_instrument_rule_hash", "instrument_rule_hash"),
        ),
        (snapshot.trade_calendar, _calendar, ()),
        (snapshot.qmt_minute_receipts, _qmt_minute_receipt, ()),
        (snapshot.public_quote_receipts, _public_receipt, ()),
        (snapshot.qmt_realtime_receipts, _qmt_realtime_receipt, ()),
    )
    for values, constructor, derived_fields in specifications:
        for value in values:
            payload = asdict(value)
            for field_name in derived_fields:
                payload.pop(field_name)
            try:
                reconstructed = constructor(payload, snapshot.knowledge_at)
            except _SchemaReadBlocked as exc:
                raise V2CanonicalSnapshotInvariantError(
                    f"snapshot row cannot be reconstructed: {exc.table_name}"
                ) from exc
            if reconstructed != value:
                raise V2CanonicalSnapshotInvariantError(
                    f"{type(value).__name__} differs from canonical reconstruction"
                )


def _snapshot_rows(snapshot: V2CanonicalSnapshot) -> dict[str, Any]:
    return {
        "account": snapshot.account,
        "intents": snapshot.intents,
        "orders": snapshot.orders,
        "fills": snapshot.fills,
        "lots": snapshot.lots,
        "cash_ledger": snapshot.cash_ledger,
        "fee_profiles": snapshot.fee_profiles,
        "instrument_rules": snapshot.instrument_rules,
        "trade_calendar": snapshot.trade_calendar,
        "qmt_minute_receipts": snapshot.qmt_minute_receipts,
        "public_quote_receipts": snapshot.public_quote_receipts,
        "qmt_realtime_receipts": snapshot.qmt_realtime_receipts,
    }


def _snapshot_blocker_codes(snapshot: V2CanonicalSnapshot) -> tuple[str, ...]:
    _validate_receipt_scope(
        fills=snapshot.fills,
        minute_receipts=snapshot.qmt_minute_receipts,
        public_receipts=snapshot.public_quote_receipts,
        realtime_receipts=snapshot.qmt_realtime_receipts,
    )
    blockers = set(
        _validate_relationships(
            account=snapshot.account,
            intents=snapshot.intents,
            orders=snapshot.orders,
            fills=snapshot.fills,
            lots=snapshot.lots,
            cash=snapshot.cash_ledger,
            rules=snapshot.instrument_rules,
            fees=snapshot.fee_profiles,
            calendar=snapshot.trade_calendar,
        )
    )
    if snapshot.fills and not (
        snapshot.qmt_minute_receipts
        or snapshot.public_quote_receipts
        or snapshot.qmt_realtime_receipts
    ):
        blockers.add("QUOTE_RECEIPT_ROWS_MISSING")
    return tuple(sorted(blockers))


def validate_v2_canonical_snapshot(
    snapshot: V2CanonicalSnapshot,
) -> V2CanonicalSnapshot:
    """Reconstruct exact rows and recompute the manifest/content root."""

    if type(snapshot) is not V2CanonicalSnapshot:
        raise TypeError("snapshot must be exactly V2CanonicalSnapshot")
    snapshot.__post_init__()
    _reconstruct_rows(snapshot)

    canonical_orders = (
        (
            snapshot.intents,
            lambda item: (item.created_at, item.intent_id),
            "intents",
        ),
        (
            snapshot.orders,
            lambda item: (item.created_at, item.order_id),
            "orders",
        ),
        (
            snapshot.fills,
            lambda item: (item.filled_at, item.fill_id),
            "fills",
        ),
        (
            snapshot.lots,
            lambda item: (item.opened_trade_date, item.lot_id),
            "lots",
        ),
        (
            snapshot.cash_ledger,
            lambda item: (item.occurred_at, item.cash_event_id),
            "cash_ledger",
        ),
        (
            snapshot.fee_profiles,
            lambda item: (
                item.fee_profile_version,
                item.security_type,
                item.effective_from,
            ),
            "fee_profiles",
        ),
        (
            snapshot.instrument_rules,
            lambda item: (
                item.stock_code,
                item.effective_from,
                item.rule_version,
            ),
            "instrument_rules",
        ),
        (
            snapshot.trade_calendar,
            lambda item: (item.trade_date, item.calendar_year),
            "trade_calendar",
        ),
        (
            snapshot.qmt_minute_receipts,
            lambda item: (
                item.trade_date,
                item.first_trade_time,
                item.last_trade_time,
                item.source_provider,
                item.capture_mode,
                item.receipt_id,
            ),
            "qmt_minute_receipts",
        ),
        (
            snapshot.public_quote_receipts,
            lambda item: (item.quote_at, item.batch_id),
            "public_quote_receipts",
        ),
        (
            snapshot.qmt_realtime_receipts,
            lambda item: (
                item.source_generated_at,
                item.published_at,
                item.receipt_id,
            ),
            "qmt_realtime_receipts",
        ),
    )
    for values, order_key, name in canonical_orders:
        if values != tuple(sorted(values, key=order_key)):
            raise V2CanonicalSnapshotInvariantError(
                f"{name} are not in canonical order"
            )

    _snapshot_blocker_codes(snapshot)
    expected_manifest = _manifest(
        account_id=snapshot.account_id,
        knowledge_at=snapshot.knowledge_at,
        rows_by_name=_snapshot_rows(snapshot),
    )
    if expected_manifest != snapshot.row_manifest:
        raise V2CanonicalSnapshotInvariantError(
            "row manifest/content root differs from reconstructed snapshot"
        )
    return snapshot


def validate_v2_canonical_read_result(
    result: V2CanonicalReadResult,
) -> V2CanonicalReadResult:
    """Recheck snapshot integrity and the exact capability report."""

    if type(result) is not V2CanonicalReadResult:
        raise TypeError("result must be exactly V2CanonicalReadResult")
    result.__post_init__()
    if result.snapshot is None:
        if result.capability_status is not V2CapabilityStatus.SNAPSHOT_READ_BLOCKED:
            raise V2CanonicalSnapshotInvariantError(
                "missing snapshot is not reported as SNAPSHOT_READ_BLOCKED"
            )
        if len(result.blockers) != 1 or result.blockers[0].code != "SCHEMA_READ_BLOCKED":
            raise V2CanonicalSnapshotInvariantError(
                "snapshot read failure must carry one SCHEMA_READ_BLOCKED reason"
            )
        allowed_schema_blockers = {
            _schema_blocker(table_name, reason_code)
            for table_name, _ in TABLE_ROWS
            for reason_code in SCHEMA_READ_REASON_CODES
        }
        if result.blockers[0] not in allowed_schema_blockers:
            raise V2CanonicalSnapshotInvariantError(
                "snapshot read blocker is not a canonical schema failure"
            )
        return result

    snapshot = validate_v2_canonical_snapshot(result.snapshot)
    expected_codes = _snapshot_blocker_codes(snapshot)
    expected_blockers = tuple(_capability_blocker(code) for code in expected_codes)
    if result.blockers != expected_blockers:
        raise V2CanonicalSnapshotInvariantError(
            "capability blockers differ from reconstructed schema gaps"
        )
    expected_status = (
        V2CapabilityStatus.AUTHORITATIVE_REPLAY_BLOCKED
        if expected_blockers
        else V2CapabilityStatus.MATERIALIZED_SNAPSHOT_READY
    )
    if result.capability_status is not expected_status:
        raise V2CanonicalSnapshotInvariantError(
            "capability status differs from reconstructed blocker report"
        )
    return result


def _blocked(*blockers: V2CapabilityBlocker) -> V2CanonicalReadResult:
    return validate_v2_canonical_read_result(
        V2CanonicalReadResult(
            capability_status=V2CapabilityStatus.SNAPSHOT_READ_BLOCKED,
            blockers=tuple(sorted(blockers, key=lambda item: item.code)),
            snapshot=None,
        )
    )


def read_canonical_v2_snapshot(
    connection: Connection,
    *,
    account_id: str,
    knowledge_at: datetime,
) -> V2CanonicalReadResult:
    """Read one deterministic account snapshot from an existing transaction.

    ``transaction_content_root_hash`` authenticates neither the database nor
    the caller.  It becomes authoritative only when an external boundary has
    already established that ``connection`` belongs to the canonical V2
    database.
    """

    if not isinstance(connection, Connection):
        raise TypeError("connection must be a SQLAlchemy Connection")
    if not connection.in_transaction():
        raise V2CanonicalReadError("caller-owned active transaction is required")
    try:
        isolation_level = connection.get_isolation_level()
    except SQLAlchemyError as exc:
        raise V2CanonicalReadError(
            "transaction isolation level could not be verified"
        ) from exc
    if type(isolation_level) is not str:
        raise V2CanonicalReadError(
            "transaction isolation level could not be verified"
        )
    normalized_isolation = " ".join(
        isolation_level.replace("_", " ").replace("-", " ").upper().split()
    )
    if normalized_isolation not in CONSISTENT_SNAPSHOT_ISOLATION_LEVELS:
        raise V2CanonicalReadError(
            "REPEATABLE READ or SERIALIZABLE transaction isolation is required"
        )
    if type(account_id) is not str or not account_id.strip():
        raise TypeError("account_id must be a non-empty str")
    account_id = account_id.strip()
    if type(knowledge_at) is not datetime:
        raise TypeError("knowledge_at must be exactly datetime")
    if knowledge_at.tzinfo is None or knowledge_at.utcoffset() is None:
        raise ValueError("knowledge_at must be timezone-aware")
    knowledge_at = knowledge_at.astimezone(V2_MARKET_TIMEZONE)
    sql_knowledge_at = knowledge_at.replace(tzinfo=None)

    try:
        account_source = _rows(
            connection, table_name="st_trade_account_v2", sql=ACCOUNT_SQL,
            parameters={"account_id": account_id},
        )
        if len(account_source) != 1:
            raise V2CanonicalSnapshotInvariantError(
                "canonical account query must return exactly one row"
            )
        account = _account(account_source[0], knowledge_at)
        if account.account_id != account_id:
            raise V2CanonicalSnapshotInvariantError("account query returned wrong account")

        intents = tuple(
            _intent(row, knowledge_at)
            for row in _rows(connection, table_name="st_trade_intent_v2", sql=INTENT_SQL, parameters={"account_id": account_id})
        )
        orders = tuple(
            _order(row, knowledge_at)
            for row in _rows(connection, table_name="st_order_v2", sql=ORDER_SQL, parameters={"account_id": account_id})
        )
        fills = tuple(
            _fill(row, knowledge_at)
            for row in _rows(connection, table_name="st_fill_v2", sql=FILL_SQL, parameters={"account_id": account_id})
        )
        lots = tuple(
            _lot(row, knowledge_at)
            for row in _rows(connection, table_name="st_position_lot_v2", sql=LOT_SQL, parameters={"account_id": account_id})
        )
        cash = tuple(
            _cash(row, knowledge_at)
            for row in _rows(connection, table_name="st_cash_ledger_v2", sql=CASH_SQL, parameters={"account_id": account_id})
        )

        stock_codes = tuple(
            sorted(
                {
                    *(item.stock_code for item in intents),
                    *(item.stock_code for item in orders),
                    *(item.stock_code for item in fills),
                    *(item.stock_code for item in lots),
                }
            )
        )
        rule_rows = _rows(
            connection, table_name="st_instrument_rule_v2", sql=RULE_SQL,
            parameters={"stock_codes": stock_codes}, expanding="stock_codes",
        )
        rules = tuple(_rule(row, knowledge_at) for row in rule_rows)
        fee_versions = tuple(
            sorted(
                {
                    *(item.fee_profile_version for item in rules),
                    *(
                        (account.fee_profile_version,)
                        if account.fee_profile_version is not None
                        else ()
                    ),
                }
            )
        )
        fee_rows = _rows(
            connection, table_name="st_fee_profile_v2", sql=FEE_SQL,
            parameters={"fee_versions": fee_versions}, expanding="fee_versions",
        )
        fees = tuple(_fee(row, knowledge_at) for row in fee_rows)

        relevant_dates = [item.filled_at.date() for item in fills]
        relevant_dates.extend(item.opened_trade_date for item in lots)
        relevant_dates.extend(item.settlement_date for item in lots)
        calendar_start = min(relevant_dates) if relevant_dates else knowledge_at.date()
        calendar_end = max((*relevant_dates, knowledge_at.date())) if relevant_dates else knowledge_at.date()
        calendar = tuple(
            _calendar(row, knowledge_at)
            for row in _rows(
                connection, table_name="si_trade_calendar", sql=CALENDAR_SQL,
                parameters={"start_date": calendar_start, "end_date": calendar_end},
            )
        )
        minute_receipts: tuple[V2QmtMinuteReceiptRow, ...] = ()
        public_receipts: tuple[V2PublicQuoteReceiptRow, ...] = ()
        realtime_receipts: tuple[V2QmtRealtimeReceiptRow, ...] = ()
        if fills:
            receipt_dates = tuple(item.filled_at.date() for item in fills)
            receipt_start_date = min(receipt_dates)
            receipt_end_date = max(receipt_dates)
            receipt_start_at = datetime.combine(
                receipt_start_date,
                datetime.min.time(),
            )
            receipt_end_exclusive_at = datetime.combine(
                receipt_end_date + timedelta(days=1),
                datetime.min.time(),
            )
            receipt_parameters = {
                "receipt_start_date": receipt_start_date,
                "receipt_end_date": receipt_end_date,
                "receipt_start_at": receipt_start_at,
                "receipt_end_exclusive_at": receipt_end_exclusive_at,
                "knowledge_at": sql_knowledge_at,
            }
            minute_receipts = tuple(
                _qmt_minute_receipt(row, knowledge_at)
                for row in _rows(
                    connection,
                    table_name="st_qmt_minute_sync_receipt_v2",
                    sql=QMT_MINUTE_RECEIPT_SQL,
                    parameters=receipt_parameters,
                )
            )
            public_receipts = tuple(
                _public_receipt(row, knowledge_at)
                for row in _rows(
                    connection,
                    table_name="st_public_quote_receipt_v2",
                    sql=PUBLIC_RECEIPT_SQL,
                    parameters=receipt_parameters,
                )
            )
            realtime_receipts = tuple(
                _qmt_realtime_receipt(row, knowledge_at)
                for row in _rows(
                    connection,
                    table_name="st_qmt_realtime_sync_receipt_v2",
                    sql=QMT_REALTIME_RECEIPT_SQL,
                    parameters=receipt_parameters,
                )
            )
    except _SchemaReadBlocked as exc:
        return _blocked(_schema_blocker(exc.table_name, exc.reason))

    # Normalize independently of database collation and verify no duplicate key
    # was hidden by a non-canonical source order.
    intents = tuple(sorted(intents, key=lambda item: (item.created_at, item.intent_id)))
    orders = tuple(sorted(orders, key=lambda item: (item.created_at, item.order_id)))
    fills = tuple(sorted(fills, key=lambda item: (item.filled_at, item.fill_id)))
    lots = tuple(sorted(lots, key=lambda item: (item.opened_trade_date, item.lot_id)))
    cash = tuple(sorted(cash, key=lambda item: (item.occurred_at, item.cash_event_id)))
    rules = tuple(sorted(rules, key=lambda item: (item.stock_code, item.effective_from, item.rule_version)))
    fees = tuple(sorted(fees, key=lambda item: (item.fee_profile_version, item.security_type, item.effective_from)))
    calendar = tuple(sorted(calendar, key=lambda item: (item.trade_date, item.calendar_year)))
    minute_receipts = tuple(sorted(minute_receipts, key=lambda item: (item.trade_date, item.first_trade_time, item.last_trade_time, item.source_provider, item.capture_mode, item.receipt_id)))
    public_receipts = tuple(sorted(public_receipts, key=lambda item: (item.quote_at, item.batch_id)))
    realtime_receipts = tuple(sorted(realtime_receipts, key=lambda item: (item.source_generated_at, item.published_at, item.receipt_id)))

    blockers = set(
        _validate_relationships(
            account=account, intents=intents, orders=orders, fills=fills,
            lots=lots, cash=cash, rules=rules, fees=fees, calendar=calendar,
        )
    )
    _validate_receipt_scope(
        fills=fills,
        minute_receipts=minute_receipts,
        public_receipts=public_receipts,
        realtime_receipts=realtime_receipts,
    )
    if fills and not (minute_receipts or public_receipts or realtime_receipts):
        blockers.add("QUOTE_RECEIPT_ROWS_MISSING")

    rows_by_name = {
        "account": account, "intents": intents, "orders": orders, "fills": fills,
        "lots": lots, "cash_ledger": cash, "fee_profiles": fees,
        "instrument_rules": rules, "trade_calendar": calendar,
        "qmt_minute_receipts": minute_receipts,
        "public_quote_receipts": public_receipts,
        "qmt_realtime_receipts": realtime_receipts,
    }
    manifest = _manifest(
        account_id=account_id, knowledge_at=knowledge_at, rows_by_name=rows_by_name,
    )
    snapshot = V2CanonicalSnapshot(
        schema_version=SNAPSHOT_SCHEMA_VERSION, account_id=account_id,
        knowledge_at=knowledge_at, account=account, intents=intents, orders=orders,
        fills=fills, lots=lots, cash_ledger=cash, fee_profiles=fees,
        instrument_rules=rules, trade_calendar=calendar,
        qmt_minute_receipts=minute_receipts,
        public_quote_receipts=public_receipts,
        qmt_realtime_receipts=realtime_receipts, row_manifest=manifest,
        transaction_content_root_hash=manifest.root_hash,
    )
    capability_blockers = tuple(
        _capability_blocker(code) for code in sorted(blockers)
    )
    return validate_v2_canonical_read_result(
        V2CanonicalReadResult(
            capability_status=(
                V2CapabilityStatus.AUTHORITATIVE_REPLAY_BLOCKED
                if capability_blockers
                else V2CapabilityStatus.MATERIALIZED_SNAPSHOT_READY
            ),
            blockers=capability_blockers,
            snapshot=snapshot,
        )
    )


__all__ = [
    "V2CanonicalReadError", "V2CanonicalReadResult", "V2CanonicalSnapshot",
    "V2CanonicalSnapshotInvariantError", "V2CapabilityBlocker",
    "V2CapabilityStatus", "V2CashLedgerRow",
    "V2ContentRootSemantics", "V2FeeProfileRow", "V2FillRow",
    "V2InstrumentRuleRow", "V2OrderRow", "V2PositionLotRow",
    "V2PublicQuoteReceiptRow", "V2QmtMinuteReceiptRow",
    "V2QmtRealtimeReceiptRow", "V2RowManifest", "V2RowManifestEntry",
    "V2TradeAccountRow", "V2TradeCalendarRow", "V2TradeIntentRow",
    "read_canonical_v2_snapshot", "validate_v2_canonical_read_result",
    "validate_v2_canonical_snapshot",
]
