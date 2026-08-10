"""Strict, opt-in persistence boundary for V2 execution evidence.

This module accepts only a caller-owned SQLAlchemy-like connection that is
already inside a transaction.  It never creates a connection or changes the
transaction lifecycle.  It is intentionally not wired to a production entry
point.

Content hashes prove equality only.  External authority is accepted solely
through the concrete MySQL registry-backed Ed25519 verifier and is attested in
the same caller-owned transaction; callers cannot supply their own loader or
public key and self-assert a cryptographic trust level.

The caller must use the canonical V2 lock order (order, then account, then
facts/evidence heads) and must not enter this boundary after taking locks in
the opposite order.  Every exception must escape the outer transaction so the
whole transaction is rolled back.  A concurrent unique-key failure is retried
only in a new transaction; this module never queries a failed transaction.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import text

from server.integrations.v2_execution_evidence_authority import (
    AuthorityAttestationConflictError,
    AuthorityNonceReplayError,
    AuthorityReceiptReference,
    AuthorityVerificationError,
    AuthorityVerificationLevel,
    EvidenceAuthorityVerifier,
    MySQLRegistryBackedAuthorityVerifier,
    append_authority_attestation,
    build_authority_claim,
    build_instrument_rule_authority_claim,
    load_authority_attestation,
    require_verified_authority,
    require_verified_instrument_rule_authority,
)
from server.trading_v2.execution_evidence import (
    AuthorityStatus,
    CanonicalJson,
    CashEventBinding,
    FillExecutionEvidence,
    MarketCalendarEvidence,
    OrderTransitionEvidence,
    QuoteReceiptEvidence,
    QuoteReceiptType,
    validate_cash_event_binding,
    validate_fill_execution_evidence,
    validate_market_calendar_evidence,
    validate_order_transition_evidence,
    validate_quote_receipt_evidence,
)
from server.trading_v2.execution_evidence_schema_gate import (
    V2EvidenceMaintenanceFenceError,
    assert_v2_evidence_maintenance_fence_inactive,
)


MARKET_ZONE = ZoneInfo("Asia/Shanghai")


class EvidenceTransactionError(RuntimeError):
    """The supplied object is not an active caller-owned connection."""


class EvidenceCanonicalRowError(RuntimeError):
    """A V2 fact or evidence row differs from its canonical representation."""


class EvidenceAppendConflictError(RuntimeError):
    """An identifier or chain position already carries different content."""


class EvidenceAuthorityUnsupportedError(RuntimeError):
    """The requested source authority cannot be established."""


class EvidenceAuthorityReplayError(EvidenceAuthorityUnsupportedError):
    """An authority nonce is already bound to another claim."""


class EvidenceAuthorityConflictError(EvidenceAuthorityUnsupportedError):
    """An authority identity already carries different immutable content."""


class EvidenceAppendStatus(str, Enum):
    INSERTED = "INSERTED"
    IDEMPOTENT = "IDEMPOTENT"


@dataclass(frozen=True, slots=True)
class EvidenceAppendResult:
    status: EvidenceAppendStatus
    evidence_type: str
    evidence_id: str
    content_hash: str


def _active_connection(connection: Any) -> Any:
    if connection is None or not callable(getattr(connection, "execute", None)):
        raise EvidenceTransactionError("a SQLAlchemy-like connection is required")
    probe = getattr(connection, "in_transaction", None)
    if not callable(probe):
        raise EvidenceTransactionError("connection must expose in_transaction()")
    try:
        active = probe()
    except Exception as exc:  # pragma: no cover - defensive boundary
        raise EvidenceTransactionError("transaction state cannot be inspected") from exc
    if type(active) is not bool or not active:
        raise EvidenceTransactionError("connection must already be in a transaction")
    try:
        assert_v2_evidence_maintenance_fence_inactive(connection)
    except V2EvidenceMaintenanceFenceError as exc:
        raise EvidenceTransactionError(
            "V2 execution-evidence writes are blocked by the maintenance fence"
        ) from exc
    return connection


def _row(result: Any, *, operation: str) -> Mapping[str, Any] | None:
    try:
        value = result.mappings().first()
    except Exception as exc:
        raise EvidenceCanonicalRowError(
            f"{operation} did not return a SQLAlchemy mapping result"
        ) from exc
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise EvidenceCanonicalRowError(f"{operation} returned a non-mapping row")
    return dict(value)


def _query_one(
    connection: Any,
    tag: str,
    sql: str,
    params: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    result = connection.execute(text(f"/* v2e:{tag} */\n{sql}"), dict(params))
    return _row(result, operation=tag)


def _exact_keys(row: Mapping[str, Any], keys: tuple[str, ...], name: str) -> None:
    actual = frozenset(row)
    expected = frozenset(keys)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise EvidenceCanonicalRowError(
            f"{name} columns differ; missing={missing}, extra={extra}"
        )


def _strict_text(value: object, name: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise EvidenceCanonicalRowError(f"{name} must be exact non-blank text")
    return value


def _strict_optional_text(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _strict_text(value, name)


def _strict_int(value: object, name: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise EvidenceCanonicalRowError(f"{name} must be int >= {minimum}")
    return value


def _strict_bool(value: object, name: str) -> bool:
    if type(value) is bool:
        return value
    if type(value) is int and value in {0, 1}:
        return bool(value)
    raise EvidenceCanonicalRowError(f"{name} must be a database boolean")


def _strict_date(value: object, name: str) -> date:
    if type(value) is not date:
        raise EvidenceCanonicalRowError(f"{name} must be exactly date")
    return value


def _aware_db_datetime(value: object, name: str) -> datetime:
    if type(value) is not datetime:
        raise EvidenceCanonicalRowError(f"{name} must be exactly datetime")
    if value.microsecond != 0:
        raise EvidenceCanonicalRowError(
            f"{name} exceeds the V2 DATETIME whole-second precision"
        )
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=MARKET_ZONE)
    return value.astimezone(MARKET_ZONE)


def _db_datetime(value: datetime, name: str) -> datetime:
    aware = _aware_db_datetime(value, name)
    return aware.astimezone(MARKET_ZONE).replace(tzinfo=None)


def _decimal_text(value: object, scale: int, name: str) -> str:
    if type(value) is not Decimal:
        raise EvidenceCanonicalRowError(f"{name} must be exactly Decimal")
    number = value
    if not number.is_finite():
        raise EvidenceCanonicalRowError(f"{name} must be finite")
    quantum = Decimal(1).scaleb(-scale)
    try:
        quantized = number.quantize(quantum)
    except InvalidOperation as exc:
        raise EvidenceCanonicalRowError(f"{name} cannot be quantized") from exc
    if quantized != number:
        raise EvidenceCanonicalRowError(f"{name} exceeds scale {scale}")
    return format(quantized, f".{scale}f")


def _strict_hash(value: object, name: str) -> str:
    result = _strict_text(value, name).lower()
    if len(result) != 64 or any(character not in "0123456789abcdef" for character in result):
        raise EvidenceCanonicalRowError(f"{name} must be SHA-256 text")
    return result


def _strict_canonical_json_text(value: object, name: str) -> Any:
    if type(value) is not str:
        raise EvidenceCanonicalRowError(f"{name} must be canonical JSON text")
    try:
        return CanonicalJson(value).value()
    except (TypeError, ValueError) as exc:
        raise EvidenceCanonicalRowError(f"{name} is not strict canonical JSON") from exc


def _expect_payload(
    actual: CanonicalJson,
    expected_value: Mapping[str, Any],
    name: str,
) -> None:
    expected = CanonicalJson.from_value(dict(expected_value))
    if actual.json_text != expected.json_text or actual.payload_hash != expected.payload_hash:
        raise EvidenceCanonicalRowError(
            f"{name} differs from the exact canonical V2 row projection"
        )


def _expect_payload_keys(
    actual: CanonicalJson,
    expected_keys: frozenset[str],
    name: str,
) -> None:
    value = actual.value()
    if type(value) is not dict or frozenset(value) != expected_keys:
        raise EvidenceCanonicalRowError(f"{name} key set is not exact")


def _verify_external_authority(
    connection: Any,
    *values: Any,
    verifier: EvidenceAuthorityVerifier | None,
) -> None:
    pending = list(values)
    seen: set[int] = set()
    attested_claims: set[str] = set()
    while pending:
        value = pending.pop()
        if value is None or id(value) in seen:
            continue
        seen.add(id(value))
        provenance = getattr(value, "provenance", None)
        if provenance is not None:
            if provenance.authority_status is AuthorityStatus.EXTERNAL_RECEIPT_VERIFIED:
                try:
                    claim = build_authority_claim(value)
                    if claim.claim_hash not in attested_claims:
                        if type(verifier) is not MySQLRegistryBackedAuthorityVerifier:
                            raise AuthorityVerificationError(
                                "writer cryptographic authority requires the "
                                "MySQL registry-backed verifier"
                            )
                        # Lock receipt/key/revocation rows before the attestation
                        # head.  This is the canonical authority lock order for
                        # both first insert and idempotent replay.
                        verifier.require_registered_claim_active(
                            connection, claim
                        )
                        existing = load_authority_attestation(connection, claim)
                        if existing is None:
                            decision = require_verified_authority(
                                connection,
                                value,
                                verifier,
                                minimum_level=(
                                    AuthorityVerificationLevel.CRYPTOGRAPHIC
                                ),
                            )
                            append_authority_attestation(
                                connection, claim, decision
                            )
                        attested_claims.add(claim.claim_hash)
                except AuthorityNonceReplayError as exc:
                    raise EvidenceAuthorityReplayError(
                        "external authority replay nonce conflict"
                    ) from exc
                except AuthorityAttestationConflictError as exc:
                    raise EvidenceAuthorityConflictError(
                        "external authority attestation conflict"
                    ) from exc
                except AuthorityVerificationError as exc:
                    raise EvidenceAuthorityUnsupportedError(
                        "external authority was not cryptographically attested"
                    ) from exc
        for field_name in (
            "quote_evidence",
            "calendar_evidence",
            "fill_execution_evidence",
        ):
            nested = getattr(value, field_name, None)
            if nested is not None:
                pending.append(nested)


def _verify_instrument_rule_authority(
    connection: Any,
    evidence: FillExecutionEvidence,
    reference: AuthorityReceiptReference | None,
    verifier: EvidenceAuthorityVerifier | None,
) -> None:
    if reference is None:
        return
    try:
        claim = build_instrument_rule_authority_claim(evidence, reference)
        if type(verifier) is not MySQLRegistryBackedAuthorityVerifier:
            raise AuthorityVerificationError(
                "writer instrument-rule authority requires the concrete "
                "MySQL registry-backed verifier"
            )
        verifier.require_registered_claim_active(connection, claim)
        existing = load_authority_attestation(connection, claim)
        if existing is None:
            decision = require_verified_instrument_rule_authority(
                connection,
                evidence,
                reference,
                verifier,
                minimum_level=AuthorityVerificationLevel.CRYPTOGRAPHIC,
            )
            append_authority_attestation(connection, claim, decision)
    except AuthorityNonceReplayError as exc:
        raise EvidenceAuthorityReplayError(
            "instrument-rule authority replay nonce conflict"
        ) from exc
    except AuthorityAttestationConflictError as exc:
        raise EvidenceAuthorityConflictError(
            "instrument-rule authority attestation conflict"
        ) from exc
    except AuthorityVerificationError as exc:
        raise EvidenceAuthorityUnsupportedError(
            "instrument rule authority was not cryptographically attested"
        ) from exc


ACCOUNT_COLUMNS = ("account_id", "cash_balance")
ORDER_COLUMNS = (
    "order_id",
    "account_id",
    "intent_id",
    "stock_code",
    "side",
    "order_type",
    "limit_price",
    "quantity",
    "filled_quantity",
    "status",
    "waiting_reason",
    "earliest_at",
    "expires_at",
    "idempotency_key",
    "created_at",
    "updated_at",
)
QUOTE_COLUMNS = (
    "quote_event_id",
    "stock_code",
    "quote_at",
    "received_at",
    "bid1",
    "bid1_volume",
    "ask1",
    "ask1_volume",
    "last_price",
    "pre_close",
    "upper_limit",
    "lower_limit",
    "suspended",
    "source_provider",
    "source_batch_id",
    "payload_hash",
    "created_at",
)
FILL_COLUMNS = (
    "fill_id",
    "order_id",
    "account_id",
    "stock_code",
    "side",
    "quantity",
    "price",
    "gross_amount",
    "fee_amount",
    "net_cash_amount",
    "quote_event_id",
    "match_event_id",
    "idempotency_key",
    "filled_at",
    "created_at",
)
CASH_COLUMNS = (
    "cash_event_id",
    "account_id",
    "business_event_key",
    "event_type",
    "amount",
    "balance_after",
    "related_order_id",
    "related_fill_id",
    "reversal_of",
    "occurred_at",
    "created_at",
)
FEE_COLUMNS = (
    "fee_profile_version",
    "effective_from",
    "effective_to",
    "security_type",
    "buy_commission_rate",
    "sell_commission_rate",
    "minimum_commission",
    "stamp_tax_sell_rate",
    "transfer_fee_buy_rate",
    "transfer_fee_sell_rate",
    "other_fee_json",
    "evidence_hash",
    "confirmation_status",
    "created_at",
)
RULE_COLUMNS = (
    "stock_code",
    "rule_version",
    "effective_from",
    "effective_to",
    "security_type",
    "exchange_code",
    "can_buy",
    "first_buy_minimum",
    "buy_lot_size",
    "sell_lot_size",
    "settlement_days",
    "tick_size",
    "limit_ratio",
    "special_treatment",
    "suspended",
    "permission_required",
    "permission_confirmed",
    "fee_profile_version",
    "source_snapshot_hash",
    "created_at",
)


def _columns_sql(columns: tuple[str, ...]) -> str:
    return ", ".join(columns)


def _lock_account(connection: Any, account_id: str) -> Mapping[str, Any]:
    row = _query_one(
        connection,
        "lock_account",
        f"SELECT {_columns_sql(ACCOUNT_COLUMNS)} FROM st_trade_account_v2 "
        "WHERE account_id = :account_id FOR UPDATE",
        {"account_id": account_id},
    )
    if row is None:
        raise EvidenceCanonicalRowError("canonical V2 account does not exist")
    _exact_keys(row, ACCOUNT_COLUMNS, "account row")
    if _strict_text(row["account_id"], "account_id") != account_id:
        raise EvidenceCanonicalRowError("locked account identity differs")
    _decimal_text(row["cash_balance"], 2, "account.cash_balance")
    return row


def _lock_order(connection: Any, order_id: str, account_id: str) -> Mapping[str, Any]:
    row = _query_one(
        connection,
        "lock_order",
        f"SELECT {_columns_sql(ORDER_COLUMNS)} FROM st_order_v2 "
        "WHERE order_id = :order_id FOR UPDATE",
        {"order_id": order_id},
    )
    if row is None:
        raise EvidenceCanonicalRowError("canonical V2 order does not exist")
    _exact_keys(row, ORDER_COLUMNS, "order row")
    if _strict_text(row["order_id"], "order_id") != order_id:
        raise EvidenceCanonicalRowError("locked order identity differs")
    if _strict_text(row["account_id"], "order.account_id") != account_id:
        raise EvidenceCanonicalRowError("locked order belongs to another account")
    return row


def _select_fact(
    connection: Any,
    tag: str,
    table: str,
    columns: tuple[str, ...],
    where: str,
    params: Mapping[str, Any],
) -> Mapping[str, Any]:
    row = _query_one(
        connection,
        tag,
        f"SELECT {_columns_sql(columns)} FROM {table} WHERE {where} FOR UPDATE",
        params,
    )
    if row is None:
        raise EvidenceCanonicalRowError(f"{tag} canonical row does not exist")
    _exact_keys(row, columns, tag)
    return row


def _canonical_order_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "account_id": _strict_text(row["account_id"], "order.account_id"),
        "created_at": _aware_db_datetime(row["created_at"], "order.created_at"),
        "earliest_at": _aware_db_datetime(row["earliest_at"], "order.earliest_at"),
        "expires_at": _aware_db_datetime(row["expires_at"], "order.expires_at"),
        "idempotency_key": _strict_hash(row["idempotency_key"], "order.idempotency_key"),
        "intent_id": _strict_text(row["intent_id"], "order.intent_id"),
        "limit_price": _decimal_text(row["limit_price"], 6, "order.limit_price"),
        "order_id": _strict_text(row["order_id"], "order.order_id"),
        "order_type": _strict_text(row["order_type"], "order.order_type"),
        "quantity": _strict_int(row["quantity"], "order.quantity", minimum=1),
        "side": _strict_text(row["side"], "order.side"),
        "stock_code": _strict_text(row["stock_code"], "order.stock_code"),
    }


def _canonical_quote_row(row: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "ask1": None if row["ask1"] is None else _decimal_text(row["ask1"], 6, "quote.ask1"),
        "ask1_volume": None if row["ask1_volume"] is None else _strict_int(row["ask1_volume"], "quote.ask1_volume"),
        "bid1": None if row["bid1"] is None else _decimal_text(row["bid1"], 6, "quote.bid1"),
        "bid1_volume": None if row["bid1_volume"] is None else _strict_int(row["bid1_volume"], "quote.bid1_volume"),
        "created_at": _aware_db_datetime(row["created_at"], "quote.created_at"),
        "last_price": None if row["last_price"] is None else _decimal_text(row["last_price"], 6, "quote.last_price"),
        "lower_limit": None if row["lower_limit"] is None else _decimal_text(row["lower_limit"], 6, "quote.lower_limit"),
        "payload_hash": _strict_hash(row["payload_hash"], "quote.payload_hash"),
        "pre_close": None if row["pre_close"] is None else _decimal_text(row["pre_close"], 6, "quote.pre_close"),
        "quote_at": _aware_db_datetime(row["quote_at"], "quote.quote_at"),
        "quote_event_id": _strict_hash(row["quote_event_id"], "quote.quote_event_id"),
        "received_at": _aware_db_datetime(row["received_at"], "quote.received_at"),
        "source_batch_id": _strict_text(row["source_batch_id"], "quote.source_batch_id"),
        "source_provider": _strict_text(row["source_provider"], "quote.source_provider"),
        "stock_code": _strict_text(row["stock_code"], "quote.stock_code"),
        "suspended": _strict_bool(row["suspended"], "quote.suspended"),
        "upper_limit": None if row["upper_limit"] is None else _decimal_text(row["upper_limit"], 6, "quote.upper_limit"),
    }
    if result["quote_event_id"] != result["payload_hash"]:
        raise EvidenceCanonicalRowError("quote legacy identity and payload hash differ")
    return result


def _canonical_fill_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "account_id": _strict_text(row["account_id"], "fill.account_id"),
        "created_at": _aware_db_datetime(row["created_at"], "fill.created_at"),
        "fee_amount": _decimal_text(row["fee_amount"], 2, "fill.fee_amount"),
        "fill_id": _strict_text(row["fill_id"], "fill.fill_id"),
        "filled_at": _aware_db_datetime(row["filled_at"], "fill.filled_at"),
        "gross_amount": _decimal_text(row["gross_amount"], 2, "fill.gross_amount"),
        "idempotency_key": _strict_hash(row["idempotency_key"], "fill.idempotency_key"),
        "match_event_id": _strict_hash(row["match_event_id"], "fill.match_event_id"),
        "net_cash_amount": _decimal_text(row["net_cash_amount"], 2, "fill.net_cash_amount"),
        "order_id": _strict_text(row["order_id"], "fill.order_id"),
        "price": _decimal_text(row["price"], 6, "fill.price"),
        "quantity": _strict_int(row["quantity"], "fill.quantity", minimum=1),
        "quote_event_id": _strict_hash(row["quote_event_id"], "fill.quote_event_id"),
        "side": _strict_text(row["side"], "fill.side"),
        "stock_code": _strict_text(row["stock_code"], "fill.stock_code"),
    }


def _canonical_cash_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "account_id": _strict_text(row["account_id"], "cash.account_id"),
        "amount": _decimal_text(row["amount"], 2, "cash.amount"),
        "balance_after": _decimal_text(row["balance_after"], 2, "cash.balance_after"),
        "business_event_key": _strict_text(row["business_event_key"], "cash.business_event_key"),
        "cash_event_id": _strict_text(row["cash_event_id"], "cash.cash_event_id"),
        "created_at": _aware_db_datetime(row["created_at"], "cash.created_at"),
        "event_type": _strict_text(row["event_type"], "cash.event_type"),
        "occurred_at": _aware_db_datetime(row["occurred_at"], "cash.occurred_at"),
        "related_fill_id": _strict_optional_text(row["related_fill_id"], "cash.related_fill_id"),
        "related_order_id": _strict_optional_text(row["related_order_id"], "cash.related_order_id"),
        "reversal_of": _strict_optional_text(row["reversal_of"], "cash.reversal_of"),
    }


def _canonical_fee_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "buy_commission_rate": _decimal_text(row["buy_commission_rate"], 10, "fee.buy_commission_rate"),
        "confirmation_status": _strict_text(row["confirmation_status"], "fee.confirmation_status"),
        "created_at": _aware_db_datetime(row["created_at"], "fee.created_at"),
        "effective_from": _strict_date(row["effective_from"], "fee.effective_from"),
        "effective_to": None if row["effective_to"] is None else _strict_date(row["effective_to"], "fee.effective_to"),
        "evidence_hash": _strict_hash(row["evidence_hash"], "fee.evidence_hash"),
        "fee_profile_version": _strict_text(row["fee_profile_version"], "fee.fee_profile_version"),
        "minimum_commission": _decimal_text(row["minimum_commission"], 2, "fee.minimum_commission"),
        "other_fee_json": _strict_canonical_json_text(row["other_fee_json"], "fee.other_fee_json"),
        "security_type": _strict_text(row["security_type"], "fee.security_type"),
        "sell_commission_rate": _decimal_text(row["sell_commission_rate"], 10, "fee.sell_commission_rate"),
        "stamp_tax_sell_rate": _decimal_text(row["stamp_tax_sell_rate"], 10, "fee.stamp_tax_sell_rate"),
        "transfer_fee_buy_rate": _decimal_text(row["transfer_fee_buy_rate"], 10, "fee.transfer_fee_buy_rate"),
        "transfer_fee_sell_rate": _decimal_text(row["transfer_fee_sell_rate"], 10, "fee.transfer_fee_sell_rate"),
    }


def _canonical_rule_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "buy_lot_size": _strict_int(row["buy_lot_size"], "rule.buy_lot_size", minimum=1),
        "can_buy": _strict_bool(row["can_buy"], "rule.can_buy"),
        "created_at": _aware_db_datetime(row["created_at"], "rule.created_at"),
        "effective_from": _strict_date(row["effective_from"], "rule.effective_from"),
        "effective_to": None if row["effective_to"] is None else _strict_date(row["effective_to"], "rule.effective_to"),
        "exchange_code": _strict_text(row["exchange_code"], "rule.exchange_code"),
        "fee_profile_version": _strict_text(row["fee_profile_version"], "rule.fee_profile_version"),
        "first_buy_minimum": _strict_int(row["first_buy_minimum"], "rule.first_buy_minimum", minimum=1),
        "limit_ratio": None if row["limit_ratio"] is None else _decimal_text(row["limit_ratio"], 8, "rule.limit_ratio"),
        "permission_confirmed": _strict_bool(row["permission_confirmed"], "rule.permission_confirmed"),
        "permission_required": _strict_text(row["permission_required"], "rule.permission_required"),
        "rule_version": _strict_text(row["rule_version"], "rule.rule_version"),
        "security_type": _strict_text(row["security_type"], "rule.security_type"),
        "sell_lot_size": _strict_int(row["sell_lot_size"], "rule.sell_lot_size", minimum=1),
        "settlement_days": _strict_int(row["settlement_days"], "rule.settlement_days"),
        "source_snapshot_hash": _strict_hash(row["source_snapshot_hash"], "rule.source_snapshot_hash"),
        "special_treatment": _strict_bool(row["special_treatment"], "rule.special_treatment"),
        "stock_code": _strict_text(row["stock_code"], "rule.stock_code"),
        "suspended": _strict_bool(row["suspended"], "rule.suspended"),
        "tick_size": _decimal_text(row["tick_size"], 6, "rule.tick_size"),
    }


def _provenance_columns(value: Any) -> dict[str, Any]:
    provenance = value.provenance
    return {
        "history_origin": provenance.history_origin.value,
        "history_origin_id": provenance.history_origin_id,
        "history_origin_at": None if provenance.history_origin_at is None else _db_datetime(provenance.history_origin_at, "history_origin_at"),
        "authority_status": provenance.authority_status.value,
        "authority_receipt_hash": provenance.authority_receipt_hash,
    }


def _storage_datetime(value: datetime, name: str) -> datetime:
    return _db_datetime(value, name)


def _existing_row(
    connection: Any,
    table: str,
    columns: tuple[str, ...],
    primary_column: str,
    primary_value: str,
) -> Mapping[str, Any] | None:
    return _query_one(
        connection,
        f"existing_{table}",
        f"SELECT {_columns_sql(columns)} FROM {table} "
        f"WHERE {primary_column} = :primary_value FOR UPDATE",
        {"primary_value": primary_value},
    )


def _compare_storage(
    row: Mapping[str, Any],
    expected: Mapping[str, Any],
    name: str,
) -> None:
    _exact_keys(row, tuple(expected), name)
    for key, expected_value in expected.items():
        actual = row[key]
        if type(actual) is not type(expected_value) or actual != expected_value:
            raise EvidenceAppendConflictError(f"{name}.{key} differs")


def _stored_result(
    status: EvidenceAppendStatus,
    evidence_type: str,
    evidence_id: str,
    content_hash: str,
) -> EvidenceAppendResult:
    return EvidenceAppendResult(status, evidence_type, evidence_id, content_hash)


def _check_existing(
    connection: Any,
    *,
    table: str,
    columns: tuple[str, ...],
    primary_column: str,
    primary_value: str,
    expected: Mapping[str, Any],
    evidence_type: str,
    content_hash: str,
) -> EvidenceAppendResult | None:
    existing = _existing_row(
        connection,
        table,
        columns,
        primary_column,
        primary_value,
    )
    if existing is None:
        return None
    _compare_storage(existing, expected, f"existing {table}")
    return _stored_result(
        EvidenceAppendStatus.IDEMPOTENT,
        evidence_type,
        primary_value,
        content_hash,
    )


def _check_natural_existing(
    connection: Any,
    *,
    table: str,
    columns: tuple[str, ...],
    primary_value: str,
    expected: Mapping[str, Any],
    evidence_type: str,
    content_hash: str,
    natural_keys: tuple[
        tuple[str, str, Mapping[str, Any]], ...
    ],
) -> EvidenceAppendResult | None:
    """Classify an already-bound business key before attempting INSERT.

    These reads happen only after the writer's canonical order/account/fact
    locks.  A matching row is an idempotent retry; any storage difference is
    an explicit append conflict.  Database duplicate/deadlock errors may still
    win the first race, but a fresh-transaction retry reaches this classifier.
    """

    for tag, where, params in natural_keys:
        row = _query_one(
            connection,
            tag,
            f"SELECT {_columns_sql(columns)} FROM {table} "
            f"WHERE {where} FOR UPDATE",
            params,
        )
        if row is None:
            continue
        _compare_storage(row, expected, f"natural {table}")
        return _stored_result(
            EvidenceAppendStatus.IDEMPOTENT,
            evidence_type,
            primary_value,
            content_hash,
        )
    return None


def _insert_and_readback(
    connection: Any,
    *,
    table: str,
    columns: tuple[str, ...],
    primary_column: str,
    primary_value: str,
    expected: Mapping[str, Any],
    evidence_type: str,
    content_hash: str,
) -> EvidenceAppendResult:
    placeholders = ", ".join(f":{column}" for column in columns)
    result = connection.execute(
        text(
            f"/* v2e:insert_{table} */\n"
            f"INSERT INTO {table} ({_columns_sql(columns)}) "
            f"VALUES ({placeholders})"
        ),
        dict(expected),
    )
    if int(getattr(result, "rowcount", -1)) != 1:
        raise EvidenceCanonicalRowError(f"{table} insert did not affect exactly one row")
    readback = _existing_row(
        connection,
        table,
        columns,
        primary_column,
        primary_value,
    )
    if readback is None:
        raise EvidenceCanonicalRowError(f"{table} insert cannot be read back")
    _compare_storage(readback, expected, f"readback {table}")
    return _stored_result(
        EvidenceAppendStatus.INSERTED,
        evidence_type,
        primary_value,
        content_hash,
    )


CALENDAR_STORAGE_COLUMNS = (
    "calendar_evidence_id", "market_code", "trade_date", "calendar_version",
    "market_timezone", "calendar_payload_json", "calendar_payload_hash",
    "source_provider", "source_payload_json", "source_payload_hash",
    "source_receipt_id", "source_receipt_hash", "available_at",
    "history_origin", "history_origin_id", "history_origin_at",
    "authority_status", "authority_receipt_hash", "evidence_hash", "created_at",
)
QUOTE_STORAGE_COLUMNS = (
    "quote_evidence_id", "quote_event_id", "stock_code", "trade_date",
    "market_timezone", "quote_at", "received_at", "available_at",
    "source_provider", "source_batch_id", "source_payload_hash",
    "source_receipt_type", "source_receipt_id", "source_receipt_hash",
    "receipt_payload_json", "receipt_payload_hash", "history_origin",
    "history_origin_id", "history_origin_at", "authority_status",
    "authority_receipt_hash", "evidence_hash", "created_at",
)
FILL_STORAGE_COLUMNS = (
    "fill_execution_evidence_id", "fill_id", "order_id", "order_fill_sequence",
    "account_id", "stock_code", "fill_payload_json", "fill_payload_hash",
    "order_payload_json", "order_payload_hash", "quote_event_id",
    "quote_evidence_id", "quote_evidence_hash", "calendar_evidence_id",
    "calendar_evidence_hash", "fee_profile_version", "fee_security_type",
    "fee_effective_from", "fee_effective_to", "fee_created_at",
    "fee_schedule_json", "fee_schedule_hash", "instrument_rule_version",
    "instrument_rule_effective_from", "instrument_rule_effective_to",
    "instrument_rule_created_at", "instrument_rule_json", "instrument_rule_hash",
    "matcher_version", "matcher_request_json", "matcher_request_hash",
    "matcher_response_json", "matcher_output_hash", "accounting_request_json",
    "accounting_request_hash", "settlement_evidence_json",
    "settlement_evidence_hash", "executed_at", "bound_at", "history_origin",
    "history_origin_id", "history_origin_at", "authority_status",
    "authority_receipt_hash", "evidence_hash", "created_at",
)
CASH_STORAGE_COLUMNS = (
    "cash_binding_id", "cash_event_id", "account_id", "account_sequence",
    "cash_event_type", "related_order_id", "related_fill_id", "reversal_of",
    "fill_execution_evidence_id", "fill_execution_evidence_hash",
    "previous_cash_event_id", "previous_binding_id", "previous_binding_hash",
    "cash_event_payload_json", "cash_event_payload_hash", "occurred_at", "bound_at",
    "history_origin", "history_origin_id", "history_origin_at", "authority_status",
    "authority_receipt_hash", "binding_hash", "created_at",
)
ORDER_STORAGE_COLUMNS = (
    "transition_id", "order_id", "account_id", "order_payload_json",
    "order_payload_hash", "transition_sequence", "previous_transition_id",
    "previous_transition_hash", "from_status", "to_status",
    "previous_filled_quantity", "next_filled_quantity", "waiting_reason",
    "transition_kind", "related_fill_id", "fill_execution_evidence_id",
    "fill_execution_evidence_hash", "source_event_type", "source_event_id",
    "source_event_hash", "occurred_at", "recorded_at", "history_origin",
    "history_origin_id", "history_origin_at", "authority_status",
    "authority_receipt_hash", "transition_hash", "created_at",
)


def _calendar_storage(value: MarketCalendarEvidence) -> dict[str, Any]:
    expected = {
        "calendar_evidence_id": value.calendar_evidence_id,
        "market_code": value.market_code,
        "trade_date": value.trade_date,
        "calendar_version": value.calendar_version,
        "market_timezone": value.market_timezone,
        "calendar_payload_json": value.calendar_payload.json_text,
        "calendar_payload_hash": value.calendar_payload.payload_hash,
        "source_provider": value.source_provider,
        "source_payload_json": value.source_payload.json_text,
        "source_payload_hash": value.source_payload.payload_hash,
        "source_receipt_id": value.source_receipt_id,
        "source_receipt_hash": value.source_receipt_hash,
        "available_at": _storage_datetime(value.available_at, "calendar.available_at"),
        **_provenance_columns(value),
        "evidence_hash": value.evidence_hash,
        "created_at": _storage_datetime(value.available_at, "calendar.created_at"),
    }
    return expected


def _quote_storage(value: QuoteReceiptEvidence) -> dict[str, Any]:
    return {
        "quote_evidence_id": value.quote_evidence_id,
        "quote_event_id": value.quote_event_id,
        "stock_code": value.stock_code,
        "trade_date": value.trade_date,
        "market_timezone": value.market_timezone,
        "quote_at": _storage_datetime(value.quote_at, "quote.quote_at"),
        "received_at": _storage_datetime(value.received_at, "quote.received_at"),
        "available_at": _storage_datetime(value.available_at, "quote.available_at"),
        "source_provider": value.source_provider,
        "source_batch_id": value.source_batch_id,
        "source_payload_hash": value.source_payload_hash,
        "source_receipt_type": value.receipt_type.value,
        "source_receipt_id": value.source_receipt_id,
        "source_receipt_hash": value.source_receipt_hash,
        "receipt_payload_json": value.receipt_payload.json_text,
        "receipt_payload_hash": value.receipt_payload.payload_hash,
        **_provenance_columns(value),
        "evidence_hash": value.evidence_hash,
        "created_at": _storage_datetime(value.available_at, "quote.created_at"),
    }


def _fill_storage(value: FillExecutionEvidence) -> dict[str, Any]:
    return {
        "fill_execution_evidence_id": value.fill_execution_evidence_id,
        "fill_id": value.fill_id,
        "order_id": value.order_id,
        "order_fill_sequence": value.order_fill_sequence,
        "account_id": value.account_id,
        "stock_code": value.stock_code,
        "fill_payload_json": value.fill_payload.json_text,
        "fill_payload_hash": value.fill_payload.payload_hash,
        "order_payload_json": value.order_payload.json_text,
        "order_payload_hash": value.order_payload.payload_hash,
        "quote_event_id": value.quote_evidence.quote_event_id,
        "quote_evidence_id": value.quote_evidence.quote_evidence_id,
        "quote_evidence_hash": value.quote_evidence.evidence_hash,
        "calendar_evidence_id": value.calendar_evidence.calendar_evidence_id,
        "calendar_evidence_hash": value.calendar_evidence.evidence_hash,
        "fee_profile_version": value.fee_profile_version,
        "fee_security_type": value.fee_security_type,
        "fee_effective_from": value.fee_effective_from,
        "fee_effective_to": value.fee_effective_to,
        "fee_created_at": _storage_datetime(value.fee_created_at, "fill.fee_created_at"),
        "fee_schedule_json": value.fee_schedule.json_text,
        "fee_schedule_hash": value.fee_schedule.payload_hash,
        "instrument_rule_version": value.instrument_rule_version,
        "instrument_rule_effective_from": value.instrument_rule_effective_from,
        "instrument_rule_effective_to": value.instrument_rule_effective_to,
        "instrument_rule_created_at": _storage_datetime(value.instrument_rule_created_at, "fill.instrument_rule_created_at"),
        "instrument_rule_json": value.instrument_rule.json_text,
        "instrument_rule_hash": value.instrument_rule.payload_hash,
        "matcher_version": value.matcher_version,
        "matcher_request_json": value.matcher_request.json_text,
        "matcher_request_hash": value.matcher_request.payload_hash,
        "matcher_response_json": value.matcher_response.json_text,
        "matcher_output_hash": value.matcher_response.payload_hash,
        "accounting_request_json": value.accounting_request.json_text,
        "accounting_request_hash": value.accounting_request.payload_hash,
        "settlement_evidence_json": value.settlement_evidence.json_text,
        "settlement_evidence_hash": value.settlement_evidence.payload_hash,
        "executed_at": _storage_datetime(value.executed_at, "fill.executed_at"),
        "bound_at": _storage_datetime(value.bound_at, "fill.bound_at"),
        **_provenance_columns(value),
        "evidence_hash": value.evidence_hash,
        "created_at": _storage_datetime(value.bound_at, "fill.created_at"),
    }


def _cash_storage(value: CashEventBinding) -> dict[str, Any]:
    fill = value.fill_execution_evidence
    return {
        "cash_binding_id": value.cash_binding_id,
        "cash_event_id": value.cash_event_id,
        "account_id": value.account_id,
        "account_sequence": value.account_sequence,
        "cash_event_type": value.cash_event_type,
        "related_order_id": value.related_order_id,
        "related_fill_id": value.related_fill_id,
        "reversal_of": value.reversal_of,
        "fill_execution_evidence_id": None if fill is None else fill.fill_execution_evidence_id,
        "fill_execution_evidence_hash": None if fill is None else fill.evidence_hash,
        "previous_cash_event_id": value.previous_cash_event_id,
        "previous_binding_id": value.previous_binding_id,
        "previous_binding_hash": value.previous_binding_hash,
        "cash_event_payload_json": value.cash_event_payload.json_text,
        "cash_event_payload_hash": value.cash_event_payload.payload_hash,
        "occurred_at": _storage_datetime(value.occurred_at, "cash.occurred_at"),
        "bound_at": _storage_datetime(value.bound_at, "cash.bound_at"),
        **_provenance_columns(value),
        "binding_hash": value.binding_hash,
        "created_at": _storage_datetime(value.bound_at, "cash.created_at"),
    }


def _order_storage(value: OrderTransitionEvidence) -> dict[str, Any]:
    fill = value.fill_execution_evidence
    return {
        "transition_id": value.transition_id,
        "order_id": value.order_id,
        "account_id": value.account_id,
        "order_payload_json": value.order_payload.json_text,
        "order_payload_hash": value.order_payload.payload_hash,
        "transition_sequence": value.transition_sequence,
        "previous_transition_id": value.previous_transition_id,
        "previous_transition_hash": value.previous_transition_hash,
        "from_status": value.from_status.value,
        "to_status": value.to_status.value,
        "previous_filled_quantity": value.previous_filled_quantity,
        "next_filled_quantity": value.next_filled_quantity,
        "waiting_reason": value.waiting_reason,
        "transition_kind": value.transition_kind.value,
        "related_fill_id": value.related_fill_id,
        "fill_execution_evidence_id": None if fill is None else fill.fill_execution_evidence_id,
        "fill_execution_evidence_hash": None if fill is None else fill.evidence_hash,
        "source_event_type": value.source_event_type,
        "source_event_id": value.source_event_id,
        "source_event_hash": value.source_event_hash,
        "occurred_at": _storage_datetime(value.occurred_at, "transition.occurred_at"),
        "recorded_at": _storage_datetime(value.recorded_at, "transition.recorded_at"),
        **_provenance_columns(value),
        "transition_hash": value.transition_hash,
        "created_at": _storage_datetime(value.recorded_at, "transition.created_at"),
    }


def _append_storage(
    connection: Any,
    *,
    table: str,
    columns: tuple[str, ...],
    primary_column: str,
    primary_value: str,
    expected: Mapping[str, Any],
    evidence_type: str,
    content_hash: str,
    natural_keys: tuple[
        tuple[str, str, Mapping[str, Any]], ...
    ] = (),
) -> EvidenceAppendResult:
    existing = _check_existing(
        connection,
        table=table,
        columns=columns,
        primary_column=primary_column,
        primary_value=primary_value,
        expected=expected,
        evidence_type=evidence_type,
        content_hash=content_hash,
    )
    if existing is not None:
        return existing
    natural_existing = _check_natural_existing(
        connection,
        table=table,
        columns=columns,
        primary_value=primary_value,
        expected=expected,
        evidence_type=evidence_type,
        content_hash=content_hash,
        natural_keys=natural_keys,
    )
    if natural_existing is not None:
        return natural_existing
    return _insert_and_readback(
        connection,
        table=table,
        columns=columns,
        primary_column=primary_column,
        primary_value=primary_value,
        expected=expected,
        evidence_type=evidence_type,
        content_hash=content_hash,
    )


def append_market_calendar_evidence(
    connection: Any,
    evidence: MarketCalendarEvidence,
    *,
    authority_verifier: EvidenceAuthorityVerifier | None = None,
) -> EvidenceAppendResult:
    connection = _active_connection(connection)
    validate_market_calendar_evidence(evidence)
    _verify_external_authority(
        connection, evidence, verifier=authority_verifier
    )
    if frozenset(evidence.calendar_payload.value()) != frozenset(
        {"coverage_start_at", "coverage_end_at", "sessions", "trading_days"}
    ):
        raise EvidenceCanonicalRowError("calendar payload key set is not exact")
    source = evidence.source_payload.value()
    if type(source) is not dict or frozenset(source) != frozenset(
        {"calendar_version", "market_code", "published_at", "trade_date"}
    ):
        raise EvidenceCanonicalRowError("calendar source payload key set is not exact")
    if source["calendar_version"] != evidence.calendar_version:
        raise EvidenceCanonicalRowError("calendar source version differs")
    if source["market_code"] != evidence.market_code:
        raise EvidenceCanonicalRowError("calendar source market differs")
    if source["trade_date"] != evidence.trade_date.isoformat():
        raise EvidenceCanonicalRowError("calendar source trade_date differs")
    expected = _calendar_storage(evidence)
    return _append_storage(
        connection,
        table="st_market_calendar_evidence_v2",
        columns=CALENDAR_STORAGE_COLUMNS,
        primary_column="calendar_evidence_id",
        primary_value=evidence.calendar_evidence_id,
        expected=expected,
        evidence_type="MARKET_CALENDAR",
        content_hash=evidence.evidence_hash,
        natural_keys=(
            (
                "natural_calendar_identity",
                "market_code = :market_code AND trade_date = :trade_date "
                "AND calendar_version = :calendar_version",
                {
                    "market_code": evidence.market_code,
                    "trade_date": evidence.trade_date,
                    "calendar_version": evidence.calendar_version,
                },
            ),
        ),
    )


def append_quote_receipt_evidence(
    connection: Any,
    evidence: QuoteReceiptEvidence,
    *,
    authority_verifier: EvidenceAuthorityVerifier | None = None,
) -> EvidenceAppendResult:
    connection = _active_connection(connection)
    validate_quote_receipt_evidence(evidence)
    quote = _select_fact(
        connection,
        "select_quote",
        "st_quote_event_v2",
        QUOTE_COLUMNS,
        "quote_event_id = :quote_event_id",
        {"quote_event_id": evidence.quote_event_id},
    )
    _verify_external_authority(
        connection, evidence, verifier=authority_verifier
    )
    quote_payload = _canonical_quote_row(quote)
    expected_receipt = {
        "quote_event_id": evidence.quote_event_id,
        "quote_row": quote_payload,
        "source_batch_id": evidence.source_batch_id,
        "source_payload_hash": evidence.source_payload_hash,
        "source_provider": evidence.source_provider,
    }
    if evidence.receipt_type is not QuoteReceiptType.NONE:
        _expect_payload(
            evidence.receipt_payload,
            expected_receipt,
            "quote receipt_payload",
        )
    if quote_payload["stock_code"] != evidence.stock_code:
        raise EvidenceCanonicalRowError("quote stock_code differs")
    if quote_payload["quote_at"] != evidence.quote_at:
        raise EvidenceCanonicalRowError("quote_at differs")
    if quote_payload["received_at"] != evidence.received_at:
        raise EvidenceCanonicalRowError("received_at differs")
    if quote_payload["source_provider"] != evidence.source_provider:
        raise EvidenceCanonicalRowError("quote provider differs")
    if quote_payload["source_batch_id"] != evidence.source_batch_id:
        raise EvidenceCanonicalRowError("quote batch differs")
    expected = _quote_storage(evidence)
    return _append_storage(
        connection,
        table="st_quote_receipt_evidence_v2",
        columns=QUOTE_STORAGE_COLUMNS,
        primary_column="quote_evidence_id",
        primary_value=evidence.quote_evidence_id,
        expected=expected,
        evidence_type="QUOTE_RECEIPT",
        content_hash=evidence.evidence_hash,
        natural_keys=(
            (
                "natural_quote_event",
                "quote_event_id = :quote_event_id",
                {"quote_event_id": evidence.quote_event_id},
            ),
        ),
    )


def _require_nested_storage(
    connection: Any,
    *,
    table: str,
    columns: tuple[str, ...],
    primary_column: str,
    primary_value: str,
    expected: Mapping[str, Any],
) -> None:
    row = _existing_row(connection, table, columns, primary_column, primary_value)
    if row is None:
        raise EvidenceCanonicalRowError(f"required nested {table} row is absent")
    _compare_storage(row, expected, f"nested {table}")


def append_fill_execution_evidence(
    connection: Any,
    evidence: FillExecutionEvidence,
    *,
    authority_verifier: EvidenceAuthorityVerifier | None = None,
    instrument_rule_authority_reference: (
        AuthorityReceiptReference | None
    ) = None,
) -> EvidenceAppendResult:
    connection = _active_connection(connection)
    validate_fill_execution_evidence(evidence)
    order = _lock_order(connection, evidence.order_id, evidence.account_id)
    _lock_account(connection, evidence.account_id)
    fill = _select_fact(connection, "select_fill", "st_fill_v2", FILL_COLUMNS, "fill_id = :fill_id", {"fill_id": evidence.fill_id})
    quote = _select_fact(connection, "select_quote", "st_quote_event_v2", QUOTE_COLUMNS, "quote_event_id = :quote_event_id", {"quote_event_id": evidence.quote_evidence.quote_event_id})
    fee = _select_fact(
        connection, "select_fee", "st_fee_profile_v2", FEE_COLUMNS,
        "fee_profile_version = :version AND security_type = :security_type AND effective_from = :effective_from",
        {"version": evidence.fee_profile_version, "security_type": evidence.fee_security_type, "effective_from": evidence.fee_effective_from},
    )
    rule = _select_fact(
        connection, "select_rule", "st_instrument_rule_v2", RULE_COLUMNS,
        "stock_code = :stock_code AND rule_version = :version AND effective_from = :effective_from",
        {"stock_code": evidence.stock_code, "version": evidence.instrument_rule_version, "effective_from": evidence.instrument_rule_effective_from},
    )
    _verify_external_authority(
        connection, evidence, verifier=authority_verifier
    )
    _verify_instrument_rule_authority(
        connection,
        evidence,
        instrument_rule_authority_reference,
        authority_verifier,
    )
    order_payload = _canonical_order_payload(order)
    fill_payload = _canonical_fill_payload(fill)
    _expect_payload(evidence.order_payload, order_payload, "fill order_payload")
    _expect_payload(evidence.fill_payload, fill_payload, "fill fill_payload")
    _expect_payload(evidence.fee_schedule, _canonical_fee_payload(fee), "fill fee_schedule")
    _expect_payload(evidence.instrument_rule, _canonical_rule_payload(rule), "fill instrument_rule")
    quote_payload = _canonical_quote_row(quote)
    expected_receipt = {
        "quote_event_id": evidence.quote_evidence.quote_event_id,
        "quote_row": quote_payload,
        "source_batch_id": evidence.quote_evidence.source_batch_id,
        "source_payload_hash": evidence.quote_evidence.source_payload_hash,
        "source_provider": evidence.quote_evidence.source_provider,
    }
    if evidence.quote_evidence.receipt_type is not QuoteReceiptType.NONE:
        _expect_payload(
            evidence.quote_evidence.receipt_payload,
            expected_receipt,
            "fill quote receipt_payload",
        )
    if quote_payload["quote_event_id"] != fill_payload["quote_event_id"]:
        raise EvidenceCanonicalRowError("fill and canonical quote differ")
    if fill_payload["order_id"] != evidence.order_id or fill_payload["account_id"] != evidence.account_id:
        raise EvidenceCanonicalRowError("fill identity differs from locked account/order")
    _expect_payload_keys(
        evidence.matcher_request,
        frozenset(
            {
                "calendar_evidence_hash",
                "matcher_version",
                "order_id",
                "order_payload_hash",
                "quote_event_id",
                "quote_evidence_hash",
            }
        ),
        "matcher_request",
    )
    _expect_payload_keys(
        evidence.matcher_response,
        frozenset(
            {
                "fill_price",
                "fill_quantity",
                "match_event_id",
                "matcher_request_hash",
                "order_id",
                "quote_event_id",
                "side",
                "status",
            }
        ),
        "matcher_response",
    )
    _expect_payload_keys(
        evidence.accounting_request,
        frozenset(
            {
                "account_id",
                "calendar_evidence_hash",
                "fee_amount",
                "fee_schedule_hash",
                "fill_id",
                "gross_amount",
                "instrument_rule_hash",
                "matcher_output_hash",
                "net_cash_amount",
                "order_id",
                "price",
                "quantity",
                "quote_evidence_hash",
                "settlement_evidence_hash",
                "side",
                "stock_code",
            }
        ),
        "accounting_request",
    )
    _expect_payload_keys(
        evidence.settlement_evidence,
        frozenset(
            {
                "calendar_evidence_hash",
                "instrument_rule_hash",
                "settlement_date",
                "settlement_days",
                "stock_code",
                "trade_date",
            }
        ),
        "settlement_evidence",
    )
    _require_nested_storage(
        connection, table="st_quote_receipt_evidence_v2", columns=QUOTE_STORAGE_COLUMNS,
        primary_column="quote_evidence_id", primary_value=evidence.quote_evidence.quote_evidence_id,
        expected=_quote_storage(evidence.quote_evidence),
    )
    _require_nested_storage(
        connection, table="st_market_calendar_evidence_v2", columns=CALENDAR_STORAGE_COLUMNS,
        primary_column="calendar_evidence_id", primary_value=evidence.calendar_evidence.calendar_evidence_id,
        expected=_calendar_storage(evidence.calendar_evidence),
    )
    expected = _fill_storage(evidence)
    existing = _check_existing(
        connection, table="st_fill_execution_evidence_v2", columns=FILL_STORAGE_COLUMNS,
        primary_column="fill_execution_evidence_id", primary_value=evidence.fill_execution_evidence_id,
        expected=expected, evidence_type="FILL_EXECUTION", content_hash=evidence.evidence_hash,
    )
    if existing is not None:
        return existing
    natural_existing = _check_natural_existing(
        connection,
        table="st_fill_execution_evidence_v2",
        columns=FILL_STORAGE_COLUMNS,
        primary_value=evidence.fill_execution_evidence_id,
        expected=expected,
        evidence_type="FILL_EXECUTION",
        content_hash=evidence.evidence_hash,
        natural_keys=(
            (
                "natural_fill_id",
                "fill_id = :fill_id",
                {"fill_id": evidence.fill_id},
            ),
            (
                "natural_fill_sequence",
                "order_id = :order_id AND "
                "order_fill_sequence = :order_fill_sequence",
                {
                    "order_id": evidence.order_id,
                    "order_fill_sequence": evidence.order_fill_sequence,
                },
            ),
        ),
    )
    if natural_existing is not None:
        return natural_existing
    if order["status"] not in {"PARTIALLY_FILLED", "FILLED"}:
        raise EvidenceCanonicalRowError("locked order does not reflect a fill state")
    if _strict_int(order["filled_quantity"], "order.filled_quantity") < fill_payload["quantity"]:
        raise EvidenceCanonicalRowError("locked order fill quantity is below this fill")
    head = _query_one(
        connection, "select_fill_head",
        "SELECT order_fill_sequence FROM st_fill_execution_evidence_v2 "
        "WHERE order_id = :order_id ORDER BY order_fill_sequence DESC LIMIT 1 FOR UPDATE",
        {"order_id": evidence.order_id},
    )
    expected_sequence = 1
    if head is not None:
        _exact_keys(head, ("order_fill_sequence",), "fill head")
        expected_sequence = _strict_int(head["order_fill_sequence"], "fill head sequence", minimum=1) + 1
    if evidence.order_fill_sequence != expected_sequence:
        raise EvidenceAppendConflictError("fill evidence sequence does not extend the locked head")
    return _insert_and_readback(
        connection, table="st_fill_execution_evidence_v2", columns=FILL_STORAGE_COLUMNS,
        primary_column="fill_execution_evidence_id", primary_value=evidence.fill_execution_evidence_id,
        expected=expected, evidence_type="FILL_EXECUTION", content_hash=evidence.evidence_hash,
    )


def append_cash_event_binding(
    connection: Any,
    evidence: CashEventBinding,
    *,
    authority_verifier: EvidenceAuthorityVerifier | None = None,
) -> EvidenceAppendResult:
    connection = _active_connection(connection)
    validate_cash_event_binding(evidence)
    if evidence.related_order_id is not None:
        _lock_order(connection, evidence.related_order_id, evidence.account_id)
    account = _lock_account(connection, evidence.account_id)
    cash = _select_fact(connection, "select_cash", "st_cash_ledger_v2", CASH_COLUMNS, "cash_event_id = :cash_event_id", {"cash_event_id": evidence.cash_event_id})
    _verify_external_authority(
        connection, evidence, verifier=authority_verifier
    )
    _expect_payload(evidence.cash_event_payload, _canonical_cash_payload(cash), "cash_event_payload")
    if evidence.fill_execution_evidence is not None:
        _require_nested_storage(
            connection, table="st_fill_execution_evidence_v2", columns=FILL_STORAGE_COLUMNS,
            primary_column="fill_execution_evidence_id", primary_value=evidence.fill_execution_evidence.fill_execution_evidence_id,
            expected=_fill_storage(evidence.fill_execution_evidence),
        )
    expected = _cash_storage(evidence)
    existing = _check_existing(
        connection, table="st_cash_event_binding_v2", columns=CASH_STORAGE_COLUMNS,
        primary_column="cash_binding_id", primary_value=evidence.cash_binding_id,
        expected=expected, evidence_type="CASH_EVENT", content_hash=evidence.binding_hash,
    )
    if existing is not None:
        return existing
    natural_existing = _check_natural_existing(
        connection,
        table="st_cash_event_binding_v2",
        columns=CASH_STORAGE_COLUMNS,
        primary_value=evidence.cash_binding_id,
        expected=expected,
        evidence_type="CASH_EVENT",
        content_hash=evidence.binding_hash,
        natural_keys=(
            (
                "natural_cash_event",
                "cash_event_id = :cash_event_id",
                {"cash_event_id": evidence.cash_event_id},
            ),
            (
                "natural_cash_sequence",
                "account_id = :account_id AND "
                "account_sequence = :account_sequence",
                {
                    "account_id": evidence.account_id,
                    "account_sequence": evidence.account_sequence,
                },
            ),
        ),
    )
    if natural_existing is not None:
        return natural_existing
    cash_payload = evidence.cash_event_payload.value()
    current_balance = Decimal(_decimal_text(account["cash_balance"], 2, "account.cash_balance"))
    event_amount = Decimal(str(cash_payload["amount"]))
    event_balance = Decimal(str(cash_payload["balance_after"]))
    if current_balance != event_balance:
        raise EvidenceCanonicalRowError(
            "account cash balance differs from the new cash ledger head"
        )
    head = _query_one(
        connection, "select_cash_head",
        "SELECT cash_binding_id, cash_event_id, account_sequence, binding_hash, "
        "cash_event_payload_json, history_origin, history_origin_id, history_origin_at "
        "FROM st_cash_event_binding_v2 WHERE account_id = :account_id "
        "ORDER BY account_sequence DESC LIMIT 1 FOR UPDATE",
        {"account_id": evidence.account_id},
    )
    if head is None:
        if evidence.previous_binding_id is not None:
            raise EvidenceAppendConflictError("cash evidence references a missing head")
        if evidence.account_sequence != 0:
            raise EvidenceAppendConflictError("first cash evidence sequence must be zero")
        if event_amount != event_balance:
            raise EvidenceCanonicalRowError(
                "cash genesis amount must equal its resulting balance"
            )
    else:
        keys = ("cash_binding_id", "cash_event_id", "account_sequence", "binding_hash", "cash_event_payload_json", "history_origin", "history_origin_id", "history_origin_at")
        _exact_keys(head, keys, "cash head")
        if head["cash_binding_id"] != head["binding_hash"]:
            raise EvidenceCanonicalRowError("cash head id and hash differ")
        if evidence.account_sequence != _strict_int(head["account_sequence"], "cash head sequence") + 1:
            raise EvidenceAppendConflictError("cash sequence does not extend the locked head")
        if evidence.previous_binding_id != head["cash_binding_id"] or evidence.previous_binding_hash != head["binding_hash"] or evidence.previous_cash_event_id != head["cash_event_id"]:
            raise EvidenceAppendConflictError("cash previous identifiers differ from locked head")
        provenance = _provenance_columns(evidence)
        for key in ("history_origin", "history_origin_id", "history_origin_at"):
            if provenance[key] != head[key]:
                raise EvidenceAppendConflictError("cash history origin differs from locked head")
        previous_payload = _strict_canonical_json_text(
            head["cash_event_payload_json"],
            "cash head payload",
        )
        if type(previous_payload) is not dict or "balance_after" not in previous_payload:
            raise EvidenceCanonicalRowError("cash head payload lacks balance_after")
        previous_balance = Decimal(str(previous_payload["balance_after"]))
        if previous_balance + event_amount != event_balance:
            raise EvidenceCanonicalRowError("cash balance chain arithmetic differs")
    return _insert_and_readback(
        connection, table="st_cash_event_binding_v2", columns=CASH_STORAGE_COLUMNS,
        primary_column="cash_binding_id", primary_value=evidence.cash_binding_id,
        expected=expected, evidence_type="CASH_EVENT", content_hash=evidence.binding_hash,
    )


def append_order_transition_evidence(
    connection: Any,
    evidence: OrderTransitionEvidence,
    *,
    authority_verifier: EvidenceAuthorityVerifier | None = None,
) -> EvidenceAppendResult:
    connection = _active_connection(connection)
    validate_order_transition_evidence(evidence)
    order = _lock_order(connection, evidence.order_id, evidence.account_id)
    _lock_account(connection, evidence.account_id)
    _verify_external_authority(
        connection, evidence, verifier=authority_verifier
    )
    _expect_payload(evidence.order_payload, _canonical_order_payload(order), "transition order_payload")
    expected = _order_storage(evidence)
    existing = _check_existing(
        connection, table="st_order_transition_v2", columns=ORDER_STORAGE_COLUMNS,
        primary_column="transition_id", primary_value=evidence.transition_id,
        expected=expected, evidence_type="ORDER_TRANSITION", content_hash=evidence.transition_hash,
    )
    if existing is not None:
        return existing
    natural_existing = _check_natural_existing(
        connection,
        table="st_order_transition_v2",
        columns=ORDER_STORAGE_COLUMNS,
        primary_value=evidence.transition_id,
        expected=expected,
        evidence_type="ORDER_TRANSITION",
        content_hash=evidence.transition_hash,
        natural_keys=(
            (
                "natural_order_sequence",
                "order_id = :order_id AND "
                "transition_sequence = :transition_sequence",
                {
                    "order_id": evidence.order_id,
                    "transition_sequence": evidence.transition_sequence,
                },
            ),
            (
                "natural_order_source_event",
                "order_id = :order_id AND "
                "source_event_type = :source_event_type AND "
                "source_event_id = :source_event_id",
                {
                    "order_id": evidence.order_id,
                    "source_event_type": evidence.source_event_type,
                    "source_event_id": evidence.source_event_id,
                },
            ),
        ),
    )
    if natural_existing is not None:
        return natural_existing
    if _strict_text(order["status"], "order.status") != evidence.to_status.value:
        raise EvidenceCanonicalRowError("current order status differs from transition result")
    if _strict_int(order["filled_quantity"], "order.filled_quantity") != evidence.next_filled_quantity:
        raise EvidenceCanonicalRowError("current order fill quantity differs from transition result")
    if _strict_optional_text(order["waiting_reason"], "order.waiting_reason") != evidence.waiting_reason:
        raise EvidenceCanonicalRowError("current order waiting reason differs from transition result")
    if evidence.fill_execution_evidence is not None:
        _require_nested_storage(
            connection, table="st_fill_execution_evidence_v2", columns=FILL_STORAGE_COLUMNS,
            primary_column="fill_execution_evidence_id", primary_value=evidence.fill_execution_evidence.fill_execution_evidence_id,
            expected=_fill_storage(evidence.fill_execution_evidence),
        )
    head = _query_one(
        connection, "select_order_head",
        "SELECT transition_id, transition_sequence, transition_hash, order_payload_hash, "
        "to_status, next_filled_quantity, history_origin, history_origin_id, history_origin_at "
        "FROM st_order_transition_v2 WHERE order_id = :order_id "
        "ORDER BY transition_sequence DESC LIMIT 1 FOR UPDATE",
        {"order_id": evidence.order_id},
    )
    if head is None:
        if evidence.previous_transition_id is not None:
            raise EvidenceAppendConflictError("order transition references a missing head")
        if evidence.transition_sequence != 0:
            raise EvidenceAppendConflictError(
                "first order transition sequence must be zero"
            )
    else:
        keys = ("transition_id", "transition_sequence", "transition_hash", "order_payload_hash", "to_status", "next_filled_quantity", "history_origin", "history_origin_id", "history_origin_at")
        _exact_keys(head, keys, "order head")
        if head["transition_id"] != head["transition_hash"]:
            raise EvidenceCanonicalRowError("order head id and hash differ")
        if evidence.transition_sequence != _strict_int(head["transition_sequence"], "order head sequence") + 1:
            raise EvidenceAppendConflictError("order sequence does not extend the locked head")
        if evidence.previous_transition_id != head["transition_id"] or evidence.previous_transition_hash != head["transition_hash"]:
            raise EvidenceAppendConflictError("order previous identifiers differ from locked head")
        if evidence.order_payload.payload_hash != head["order_payload_hash"]:
            raise EvidenceAppendConflictError("order immutable payload differs from locked head")
        if evidence.from_status.value != head["to_status"] or evidence.previous_filled_quantity != head["next_filled_quantity"]:
            raise EvidenceAppendConflictError("order transition state differs from locked head")
        provenance = _provenance_columns(evidence)
        for key in ("history_origin", "history_origin_id", "history_origin_at"):
            if provenance[key] != head[key]:
                raise EvidenceAppendConflictError("order history origin differs from locked head")
    return _insert_and_readback(
        connection, table="st_order_transition_v2", columns=ORDER_STORAGE_COLUMNS,
        primary_column="transition_id", primary_value=evidence.transition_id,
        expected=expected, evidence_type="ORDER_TRANSITION", content_hash=evidence.transition_hash,
    )


def append_evidence(
    connection: Any,
    evidence: Any,
    *,
    authority_verifier: EvidenceAuthorityVerifier | None = None,
    instrument_rule_authority_reference: (
        AuthorityReceiptReference | None
    ) = None,
) -> EvidenceAppendResult:
    if type(evidence) is MarketCalendarEvidence:
        if instrument_rule_authority_reference is not None:
            raise TypeError(
                "instrument rule authority applies only to fill evidence"
            )
        return append_market_calendar_evidence(
            connection, evidence, authority_verifier=authority_verifier
        )
    if type(evidence) is QuoteReceiptEvidence:
        if instrument_rule_authority_reference is not None:
            raise TypeError(
                "instrument rule authority applies only to fill evidence"
            )
        return append_quote_receipt_evidence(
            connection, evidence, authority_verifier=authority_verifier
        )
    if type(evidence) is FillExecutionEvidence:
        return append_fill_execution_evidence(
            connection,
            evidence,
            authority_verifier=authority_verifier,
            instrument_rule_authority_reference=(
                instrument_rule_authority_reference
            ),
        )
    if type(evidence) is CashEventBinding:
        if instrument_rule_authority_reference is not None:
            raise TypeError(
                "instrument rule authority applies only to fill evidence"
            )
        return append_cash_event_binding(
            connection, evidence, authority_verifier=authority_verifier
        )
    if type(evidence) is OrderTransitionEvidence:
        if instrument_rule_authority_reference is not None:
            raise TypeError(
                "instrument rule authority applies only to fill evidence"
            )
        return append_order_transition_evidence(
            connection, evidence, authority_verifier=authority_verifier
        )
    raise TypeError("unsupported V2 execution evidence type")


__all__ = [
    "EvidenceAppendConflictError", "EvidenceAppendResult", "EvidenceAppendStatus",
    "EvidenceAuthorityUnsupportedError", "EvidenceCanonicalRowError",
    "EvidenceTransactionError", "append_cash_event_binding", "append_evidence",
    "append_fill_execution_evidence", "append_market_calendar_evidence",
    "append_order_transition_evidence", "append_quote_receipt_evidence",
]
