"""Deterministic migration-014/015 behavioral acceptance scenarios.

This module only builds immutable values and canonical seed rows.  It performs
no database I/O and creates no parallel account, order, fill, cash, or lot
ledger.  The accounting scenario deliberately reuses the calendar, quote, fee
schedule, and instrument rule from :mod:`trading_v2_evidence_behavioral_scenario`.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from server.integrations.v2_execution_evidence_authority import (
    AuthorityClaim,
    SignedAuthorityReceipt,
    authority_receipt_signature_message,
    build_authority_claim,
)
from server.trading_v2.accounting_evidence import (
    FillAccountingOutcome,
    LotAccountingEffect,
    LotEffectKind,
    LotSnapshot,
    validate_fill_accounting_outcome,
)
from server.trading_v2.domain import OrderStatus, PositionState
from server.trading_v2.execution_evidence import (
    AuthorityStatus,
    CanonicalJson,
    CashEventBinding,
    EvidenceProvenance,
    FillExecutionEvidence,
    MarketCalendarEvidence,
    OrderTransitionEvidence,
    OrderTransitionKind,
    QuoteReceiptEvidence,
    validate_cash_event_binding,
    validate_cash_event_binding_chain,
    validate_fill_execution_evidence,
    validate_market_calendar_evidence,
    validate_order_transition_evidence,
)
from tools.trading_v2_evidence_behavioral_scenario import (
    CanonicalSeedRow,
    build_behavioral_scenario,
)


UTC = timezone.utc
MARKET_ZONE = ZoneInfo("Asia/Shanghai")

AUTHORITY_KEY_REVOCATION = "KEY"
AUTHORITY_RECEIPT_REVOCATION = "RECEIPT"

ACCOUNTING_ACCOUNT_ID = "mysql57-accounting-sell-account"
ACCOUNTING_ORDER_ID = "mysql57-accounting-sell-order"
ACCOUNTING_FILL_ID = "mysql57-accounting-sell-fill"
ACCOUNTING_CASH_GENESIS_ID = "mysql57-accounting-cash-genesis"
ACCOUNTING_CASH_SELL_ID = "mysql57-accounting-cash-sell"
ACCOUNTING_LOT_IDS = (
    "mysql57-accounting-lot-a",
    "mysql57-accounting-lot-b",
)
ACCOUNTING_OPENED_FILL_IDS = (
    "mysql57-accounting-open-fill-a",
    "mysql57-accounting-open-fill-b",
)
ACCOUNTING_GENESIS_CASH_BALANCE = Decimal("50000.00")
ACCOUNTING_SELL_NET_CASH = Decimal("1573.72")
ACCOUNTING_CASH_AFTER = (
    ACCOUNTING_GENESIS_CASH_BALANCE + ACCOUNTING_SELL_NET_CASH
)


def _require_aware_utc(value: object, name: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    normalized = value.astimezone(UTC)
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be expressed in UTC")
    return normalized


def _require_whole_second_utc(value: object, name: str) -> datetime:
    normalized = _require_aware_utc(value, name)
    if normalized.microsecond != 0:
        raise ValueError(
            f"{name} exceeds the V2 core DATETIME whole-second precision"
        )
    return normalized


def _next_safe_whole_second(now_utc: datetime) -> datetime:
    # Receipt/key triggers stamp UTC_TIMESTAMP(6).  A small deterministic lead
    # leaves room for the concurrent registry/negative probes before the
    # claim's whole-second available_at, including slower shared CI hosts.
    return (now_utc + timedelta(seconds=15)).replace(microsecond=0)


def _db_utc(value: datetime) -> datetime:
    return _require_aware_utc(value, "database UTC datetime").replace(tzinfo=None)


def _utc_iso_microseconds(value: datetime) -> str:
    return _require_aware_utc(value, "hash UTC datetime").isoformat(
        timespec="microseconds"
    )


def _pipe_digest(namespace: str, *values: str) -> str:
    return hashlib.sha256(
        (namespace + "|" + "|".join(values)).encode("utf-8")
    ).hexdigest()


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _private_key(label: str) -> Ed25519PrivateKey:
    seed = hashlib.sha256(
        ("trading-v2-mysql-acceptance-ed25519|" + label).encode("ascii")
    ).digest()
    return Ed25519PrivateKey.from_private_bytes(seed)


def _public_key(private_key: Ed25519PrivateKey) -> bytes:
    return private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )


def _signed_receipt(
    *,
    claim: AuthorityClaim,
    private_key: Ed25519PrivateKey,
    key_id: str,
    key_version: str,
    replay_nonce: str,
    issued_at: datetime,
    expires_at: datetime,
    invalidate_signature: bool = False,
) -> SignedAuthorityReceipt:
    message = authority_receipt_signature_message(
        claim_hash=claim.claim_hash,
        source_provider=claim.source_provider,
        receipt_id=claim.receipt_id,
        receipt_hash=claim.receipt_hash,
        key_id=key_id,
        key_version=key_version,
        replay_nonce=replay_nonce,
        issued_at=issued_at,
        expires_at=expires_at,
    )
    signature = private_key.sign(message)
    if invalidate_signature:
        signature = bytes((signature[0] ^ 1,)) + signature[1:]
    return SignedAuthorityReceipt(
        claim_hash=claim.claim_hash,
        source_provider=claim.source_provider,
        receipt_id=claim.receipt_id,
        receipt_hash=claim.receipt_hash,
        key_id=key_id,
        key_version=key_version,
        replay_nonce=replay_nonce,
        issued_at=issued_at,
        expires_at=expires_at,
        signature=_base64url(signature),
    )


def _trust_key_values(
    *,
    source_provider: str,
    key_id: str,
    key_version: str,
    public_key: bytes,
    issued_at: datetime,
    expires_at: datetime,
    now_utc: datetime,
) -> dict[str, object]:
    return {
        "source_provider": source_provider,
        "key_id": key_id,
        "key_version": key_version,
        "algorithm": "Ed25519",
        "public_key": public_key,
        "public_key_hash": hashlib.sha256(public_key).hexdigest(),
        "valid_from": _db_utc(issued_at - timedelta(days=1)),
        "valid_to": _db_utc(expires_at + timedelta(days=1)),
        # The migration-014 trigger overwrites this with UTC_TIMESTAMP(6).
        "registered_at": _db_utc(now_utc),
    }


def _receipt_values(
    claim: AuthorityClaim,
    receipt: SignedAuthorityReceipt,
    *,
    now_utc: datetime,
) -> dict[str, object]:
    return {
        "receipt_id": receipt.receipt_id,
        "receipt_hash": receipt.receipt_hash,
        "claim_hash": receipt.claim_hash,
        "evidence_type": claim.evidence_type,
        "evidence_id": claim.evidence_id,
        "source_provider": claim.source_provider,
        "source_payload_hash": claim.source_payload_hash,
        "receipt_type": claim.receipt_type,
        "key_id": receipt.key_id,
        "key_version": receipt.key_version,
        "replay_nonce": receipt.replay_nonce,
        "issued_at": _db_utc(receipt.issued_at),
        "expires_at": _db_utc(receipt.expires_at),
        "envelope_json": receipt.envelope_json,
        "envelope_hash": receipt.envelope_hash,
        "status": "ACTIVE",
        "revoked_at": None,
        # The migration-014 trigger overwrites this with UTC_TIMESTAMP(6).
        "created_at": _db_utc(now_utc),
    }


@dataclass(frozen=True, slots=True)
class AuthorityBehavioralCase:
    """One registered, signed calendar and its post-proof revocation probe."""

    evidence: MarketCalendarEvidence
    claim: AuthorityClaim
    receipt: SignedAuthorityReceipt
    public_key: bytes
    trust_key_values: Mapping[str, object]
    receipt_values: Mapping[str, object]
    revocation_kind: str

    @property
    def revocation_table(self) -> str:
        if self.revocation_kind == AUTHORITY_KEY_REVOCATION:
            return "st_execution_authority_key_revocation_v2"
        if self.revocation_kind == AUTHORITY_RECEIPT_REVOCATION:
            return "st_execution_authority_receipt_revocation_v2"
        raise RuntimeError("authority revocation kind drifted")

    def revocation_values(self, revoked_at: datetime) -> Mapping[str, object]:
        """Return exact migration-014 INSERT parameters, preserving micros."""

        revoked = _require_aware_utc(revoked_at, "revoked_at")
        revoked_text = _utc_iso_microseconds(revoked)
        if self.revocation_kind == AUTHORITY_KEY_REVOCATION:
            reason = "KEY_ROTATED"
            digest = _pipe_digest(
                "trading-v2.authority-key-revocation.v1",
                self.claim.source_provider,
                self.receipt.key_id,
                self.receipt.key_version,
                revoked_text,
                reason,
            )
            return {
                "source_provider": self.claim.source_provider,
                "key_id": self.receipt.key_id,
                "key_version": self.receipt.key_version,
                "revoked_at": _db_utc(revoked),
                "reason_code": reason,
                "revocation_hash": digest,
                # The trigger overwrites created_at with UTC_TIMESTAMP(6).
                "created_at": _db_utc(revoked),
            }
        if self.revocation_kind == AUTHORITY_RECEIPT_REVOCATION:
            reason = "SOURCE_RETRACTED"
            digest = _pipe_digest(
                "trading-v2.authority-receipt-revocation.v1",
                self.receipt.receipt_id,
                self.receipt.receipt_hash,
                self.receipt.envelope_hash,
                revoked_text,
                reason,
            )
            return {
                "receipt_id": self.receipt.receipt_id,
                "receipt_hash": self.receipt.receipt_hash,
                "envelope_hash": self.receipt.envelope_hash,
                "revoked_at": _db_utc(revoked),
                "reason_code": reason,
                "revocation_hash": digest,
                # The trigger overwrites created_at with UTC_TIMESTAMP(6).
                "created_at": _db_utc(revoked),
            }
        raise RuntimeError("authority revocation kind drifted")


@dataclass(frozen=True, slots=True)
class AuthorityReceiptCandidate:
    """A legal registry row intended to fail replay or crypto verification."""

    evidence: MarketCalendarEvidence
    claim: AuthorityClaim
    receipt: SignedAuthorityReceipt
    receipt_values: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class AuthorityBehavioralScenario:
    now_utc: datetime
    available_at: datetime
    cases: tuple[AuthorityBehavioralCase, ...]
    nonce_replay: AuthorityReceiptCandidate
    invalid_signature: AuthorityReceiptCandidate

    @property
    def nonce_replay_case(self) -> AuthorityReceiptCandidate:
        return self.nonce_replay

    @property
    def invalid_signature_case(self) -> AuthorityReceiptCandidate:
        return self.invalid_signature


def _authoritative_calendar(
    *,
    base: MarketCalendarEvidence,
    calendar_version: str,
    source_provider: str,
    receipt_id: str,
    published_at: datetime,
    available_at: datetime,
) -> MarketCalendarEvidence:
    source_payload = CanonicalJson.from_value(
        {
            "calendar_version": calendar_version,
            "market_code": base.market_code,
            "published_at": published_at,
            "trade_date": base.trade_date,
        }
    )
    receipt_hash = source_payload.payload_hash
    provenance = EvidenceProvenance(
        history_origin=base.provenance.history_origin,
        history_origin_id=base.provenance.history_origin_id,
        history_origin_at=base.provenance.history_origin_at,
        authority_status=AuthorityStatus.EXTERNAL_RECEIPT_VERIFIED,
        authority_receipt_hash=receipt_hash,
    )
    value = MarketCalendarEvidence(
        market_code=base.market_code,
        trade_date=base.trade_date,
        calendar_version=calendar_version,
        market_timezone=base.market_timezone,
        calendar_payload=base.calendar_payload,
        source_provider=source_provider,
        source_payload=source_payload,
        available_at=available_at,
        provenance=provenance,
        source_receipt_id=receipt_id,
        source_receipt_hash=receipt_hash,
    )
    validate_market_calendar_evidence(value)
    return value


def _authority_case(
    *,
    base: MarketCalendarEvidence,
    label: str,
    now_utc: datetime,
    available_at: datetime,
    revocation_kind: str,
) -> tuple[AuthorityBehavioralCase, Ed25519PrivateKey]:
    provider = f"synthetic-calendar-authority-{label}"
    key_id = f"calendar-key-{label}"
    key_version = "2026-08-v1"
    evidence = _authoritative_calendar(
        base=base,
        calendar_version=f"calendar-authority-{label}-v1",
        source_provider=provider,
        receipt_id=f"calendar-receipt-{label}-v1",
        published_at=now_utc.replace(microsecond=0) - timedelta(seconds=2),
        available_at=available_at,
    )
    claim = build_authority_claim(evidence)
    private_key = _private_key(label)
    public_key = _public_key(private_key)
    issued_at = now_utc.replace(microsecond=0) - timedelta(seconds=2)
    expires_at = available_at + timedelta(hours=1)
    receipt = _signed_receipt(
        claim=claim,
        private_key=private_key,
        key_id=key_id,
        key_version=key_version,
        replay_nonce=f"calendar-nonce-{label}-v1",
        issued_at=issued_at,
        expires_at=expires_at,
    )
    return (
        AuthorityBehavioralCase(
            evidence=evidence,
            claim=claim,
            receipt=receipt,
            public_key=public_key,
            trust_key_values=_trust_key_values(
                source_provider=provider,
                key_id=key_id,
                key_version=key_version,
                public_key=public_key,
                issued_at=issued_at,
                expires_at=expires_at,
                now_utc=now_utc,
            ),
            receipt_values=_receipt_values(claim, receipt, now_utc=now_utc),
            revocation_kind=revocation_kind,
        ),
        private_key,
    )


def _authority_candidate(
    *,
    base: MarketCalendarEvidence,
    label: str,
    now_utc: datetime,
    available_at: datetime,
    template: AuthorityBehavioralCase,
    private_key: Ed25519PrivateKey,
    replay_nonce: str,
    invalid_signature: bool = False,
) -> AuthorityReceiptCandidate:
    evidence = _authoritative_calendar(
        base=base,
        calendar_version=f"calendar-authority-{label}-v1",
        source_provider=template.claim.source_provider,
        receipt_id=f"calendar-receipt-{label}-v1",
        published_at=now_utc.replace(microsecond=0) - timedelta(seconds=1),
        available_at=available_at,
    )
    claim = build_authority_claim(evidence)
    receipt = _signed_receipt(
        claim=claim,
        private_key=private_key,
        key_id=template.receipt.key_id,
        key_version=template.receipt.key_version,
        replay_nonce=replay_nonce,
        issued_at=now_utc.replace(microsecond=0) - timedelta(seconds=1),
        expires_at=available_at + timedelta(hours=1),
        invalidate_signature=invalid_signature,
    )
    return AuthorityReceiptCandidate(
        evidence=evidence,
        claim=claim,
        receipt=receipt,
        receipt_values=_receipt_values(claim, receipt, now_utc=now_utc),
    )


def build_authority_behavioral_scenario(
    now_utc: datetime,
    available_at: datetime | None = None,
) -> AuthorityBehavioralScenario:
    """Build two signed external calendars plus replay/signature negatives.

    ``now_utc`` may retain microseconds because migration-014 uses
    ``DATETIME(6)``.  The evidence/claim ``available_at`` is a V2 core
    ``DATETIME`` value and therefore must be an aware UTC whole second.  When
    omitted it is set a few seconds after ``now_utc`` so registry triggers can
    stamp their rows before the claim becomes available.
    """

    now = _require_aware_utc(now_utc, "now_utc")
    if available_at is None:
        available = _next_safe_whole_second(now)
    else:
        available = _require_whole_second_utc(available_at, "available_at")
        if available <= now:
            raise ValueError("available_at must follow now_utc")

    base_scenario = build_behavioral_scenario()
    calendars = tuple(
        case.evidence
        for case in base_scenario.cases
        if case.evidence_type == "MARKET_CALENDAR"
    )
    if len(calendars) != 1 or type(calendars[0]) is not MarketCalendarEvidence:
        raise RuntimeError("base behavioral calendar contract drifted")
    base = calendars[0]

    key_case, key_private = _authority_case(
        base=base,
        label="key-revocation",
        now_utc=now,
        available_at=available,
        revocation_kind=AUTHORITY_KEY_REVOCATION,
    )
    receipt_case, receipt_private = _authority_case(
        base=base,
        label="receipt-revocation",
        now_utc=now,
        available_at=available,
        revocation_kind=AUTHORITY_RECEIPT_REVOCATION,
    )
    nonce_replay = _authority_candidate(
        base=base,
        label="nonce-replay",
        now_utc=now,
        available_at=available,
        template=key_case,
        private_key=key_private,
        replay_nonce=key_case.receipt.replay_nonce,
    )
    invalid_signature = _authority_candidate(
        base=base,
        label="invalid-signature",
        now_utc=now,
        available_at=available,
        template=receipt_case,
        private_key=receipt_private,
        replay_nonce="calendar-nonce-invalid-signature-v1",
        invalid_signature=True,
    )
    return AuthorityBehavioralScenario(
        now_utc=now,
        available_at=available,
        cases=(key_case, receipt_case),
        nonce_replay=nonce_replay,
        invalid_signature=invalid_signature,
    )


@dataclass(frozen=True, slots=True)
class AccountingBehavioralScenario:
    """One actual SELL whose final V2 state consumes two lots by FIFO."""

    seed_rows: tuple[CanonicalSeedRow, ...]
    account_id: str
    account_cash_before: Decimal
    account_cash_after: Decimal
    fill_evidence: FillExecutionEvidence
    cash_evidence_rows: tuple[CashEventBinding, ...]
    order_transition: OrderTransitionEvidence
    outcome: FillAccountingOutcome
    conflicting_outcome: FillAccountingOutcome


def _market_at(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 8, 3, hour, minute, second, tzinfo=MARKET_ZONE)


def _market_naive(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 8, 3, hour, minute, second)


def _extract_base_execution_values() -> tuple[
    MarketCalendarEvidence,
    QuoteReceiptEvidence,
    FillExecutionEvidence,
]:
    scenario = build_behavioral_scenario()
    by_type = {case.evidence_type: case.evidence for case in scenario.cases}
    calendar = by_type.get("MARKET_CALENDAR")
    quote = by_type.get("QUOTE_RECEIPT")
    fill = by_type.get("FILL_EXECUTION")
    if (
        type(calendar) is not MarketCalendarEvidence
        or type(quote) is not QuoteReceiptEvidence
        or type(fill) is not FillExecutionEvidence
    ):
        raise RuntimeError("base behavioral execution contract drifted")
    return calendar, quote, fill


def _build_sell_fill() -> FillExecutionEvidence:
    calendar, quote, base_fill = _extract_base_execution_values()
    executed_at = _market_at(10, 10)
    fill_created_at = _market_at(10, 10, 1)
    bound_at = _market_at(10, 10, 2)
    match_event_id = hashlib.sha256(b"mysql57-accounting-sell-match").hexdigest()
    fill_idempotency_key = hashlib.sha256(
        f"{ACCOUNTING_ORDER_ID}|{quote.quote_event_id}|{match_event_id}".encode(
            "utf-8"
        )
    ).hexdigest()
    order_idempotency_key = hashlib.sha256(
        b"mysql57-accounting-sell-order-idempotency"
    ).hexdigest()
    order_payload = CanonicalJson.from_value(
        {
            "account_id": ACCOUNTING_ACCOUNT_ID,
            "created_at": _market_at(9),
            "earliest_at": _market_at(9, 30),
            "expires_at": _market_at(15),
            "idempotency_key": order_idempotency_key,
            "intent_id": "mysql57-accounting-sell-intent",
            "limit_price": "10.500000",
            "order_id": ACCOUNTING_ORDER_ID,
            "order_type": "LIMIT",
            "quantity": 150,
            "side": "SELL",
            "stock_code": base_fill.stock_code,
        }
    )
    fill_payload = CanonicalJson.from_value(
        {
            "account_id": ACCOUNTING_ACCOUNT_ID,
            "created_at": fill_created_at,
            "fee_amount": "1.28",
            "fill_id": ACCOUNTING_FILL_ID,
            "filled_at": executed_at,
            "gross_amount": "1575.00",
            "idempotency_key": fill_idempotency_key,
            "match_event_id": match_event_id,
            "net_cash_amount": "1573.72",
            "order_id": ACCOUNTING_ORDER_ID,
            "price": "10.500000",
            "quantity": 150,
            "quote_event_id": quote.quote_event_id,
            "side": "SELL",
            "stock_code": base_fill.stock_code,
        }
    )
    settlement = CanonicalJson.from_value(
        {
            "calendar_evidence_hash": calendar.evidence_hash,
            "instrument_rule_hash": base_fill.instrument_rule.payload_hash,
            "settlement_date": "2026-08-04",
            "settlement_days": 1,
            "stock_code": base_fill.stock_code,
            "trade_date": "2026-08-03",
        }
    )
    matcher_request = CanonicalJson.from_value(
        {
            "calendar_evidence_hash": calendar.evidence_hash,
            "matcher_version": "matcher-mysql57-accounting-sell-v1",
            "order_id": ACCOUNTING_ORDER_ID,
            "order_payload_hash": order_payload.payload_hash,
            "quote_event_id": quote.quote_event_id,
            "quote_evidence_hash": quote.evidence_hash,
        }
    )
    matcher_response = CanonicalJson.from_value(
        {
            "fill_price": "10.500000",
            "fill_quantity": 150,
            "match_event_id": match_event_id,
            "matcher_request_hash": matcher_request.payload_hash,
            "order_id": ACCOUNTING_ORDER_ID,
            "quote_event_id": quote.quote_event_id,
            "side": "SELL",
            "status": "FILLED",
        }
    )
    accounting_request = CanonicalJson.from_value(
        {
            "account_id": ACCOUNTING_ACCOUNT_ID,
            "calendar_evidence_hash": calendar.evidence_hash,
            "fee_amount": "1.28",
            "fee_schedule_hash": base_fill.fee_schedule.payload_hash,
            "fill_id": ACCOUNTING_FILL_ID,
            "gross_amount": "1575.00",
            "instrument_rule_hash": base_fill.instrument_rule.payload_hash,
            "matcher_output_hash": matcher_response.payload_hash,
            "net_cash_amount": "1573.72",
            "order_id": ACCOUNTING_ORDER_ID,
            "price": "10.500000",
            "quantity": 150,
            "quote_evidence_hash": quote.evidence_hash,
            "settlement_evidence_hash": settlement.payload_hash,
            "side": "SELL",
            "stock_code": base_fill.stock_code,
        }
    )
    value = FillExecutionEvidence(
        fill_id=ACCOUNTING_FILL_ID,
        order_id=ACCOUNTING_ORDER_ID,
        order_fill_sequence=1,
        account_id=ACCOUNTING_ACCOUNT_ID,
        stock_code=base_fill.stock_code,
        fill_payload=fill_payload,
        order_payload=order_payload,
        quote_evidence=quote,
        calendar_evidence=calendar,
        fee_profile_version=base_fill.fee_profile_version,
        fee_security_type=base_fill.fee_security_type,
        fee_effective_from=base_fill.fee_effective_from,
        fee_effective_to=base_fill.fee_effective_to,
        fee_created_at=base_fill.fee_created_at,
        fee_schedule=base_fill.fee_schedule,
        instrument_rule_version=base_fill.instrument_rule_version,
        instrument_rule_effective_from=base_fill.instrument_rule_effective_from,
        instrument_rule_effective_to=base_fill.instrument_rule_effective_to,
        instrument_rule_created_at=base_fill.instrument_rule_created_at,
        instrument_rule=base_fill.instrument_rule,
        matcher_version="matcher-mysql57-accounting-sell-v1",
        matcher_request=matcher_request,
        matcher_response=matcher_response,
        accounting_request=accounting_request,
        settlement_evidence=settlement,
        executed_at=executed_at,
        bound_at=bound_at,
        provenance=base_fill.provenance,
    )
    validate_fill_execution_evidence(value)
    return value


def _cash_evidence(fill: FillExecutionEvidence) -> tuple[CashEventBinding, ...]:
    provenance = fill.provenance
    genesis_payload = CanonicalJson.from_value(
        {
            "account_id": ACCOUNTING_ACCOUNT_ID,
            "amount": "50000.00",
            "balance_after": "50000.00",
            "business_event_key": f"{ACCOUNTING_ACCOUNT_ID}:INITIAL_DEPOSIT",
            "cash_event_id": ACCOUNTING_CASH_GENESIS_ID,
            "created_at": _market_at(8),
            "event_type": "INITIAL_DEPOSIT",
            "occurred_at": _market_at(8),
            "related_fill_id": None,
            "related_order_id": None,
            "reversal_of": None,
        }
    )
    genesis = CashEventBinding(
        cash_event_id=ACCOUNTING_CASH_GENESIS_ID,
        account_id=ACCOUNTING_ACCOUNT_ID,
        account_sequence=0,
        cash_event_type="INITIAL_DEPOSIT",
        cash_event_payload=genesis_payload,
        occurred_at=_market_at(8),
        bound_at=_market_at(8, 0, 1),
        provenance=provenance,
    )
    fill_idempotency = str(fill.fill_payload.value()["idempotency_key"])
    sell_payload = CanonicalJson.from_value(
        {
            "account_id": ACCOUNTING_ACCOUNT_ID,
            "amount": "1573.72",
            "balance_after": "51573.72",
            "business_event_key": f"FILL:{fill_idempotency}",
            "cash_event_id": ACCOUNTING_CASH_SELL_ID,
            "created_at": _market_at(10, 10, 1),
            "event_type": "SELL_FILL",
            "occurred_at": fill.executed_at,
            "related_fill_id": fill.fill_id,
            "related_order_id": fill.order_id,
            "reversal_of": None,
        }
    )
    sell = CashEventBinding(
        cash_event_id=ACCOUNTING_CASH_SELL_ID,
        account_id=ACCOUNTING_ACCOUNT_ID,
        account_sequence=1,
        cash_event_type="SELL_FILL",
        cash_event_payload=sell_payload,
        occurred_at=fill.executed_at,
        bound_at=_market_at(10, 10, 3),
        provenance=provenance,
        related_order_id=fill.order_id,
        related_fill_id=fill.fill_id,
        fill_execution_evidence=fill,
        previous_cash_event_id=genesis.cash_event_id,
        previous_binding_id=genesis.cash_binding_id,
        previous_binding_hash=genesis.binding_hash,
    )
    validate_cash_event_binding(genesis)
    validate_cash_event_binding(sell)
    validate_cash_event_binding_chain((genesis, sell))
    return genesis, sell


def _order_transition(fill: FillExecutionEvidence) -> OrderTransitionEvidence:
    value = OrderTransitionEvidence(
        order_id=fill.order_id,
        account_id=fill.account_id,
        order_payload=fill.order_payload,
        transition_sequence=0,
        from_status=OrderStatus.QUEUED,
        to_status=OrderStatus.FILLED,
        previous_filled_quantity=0,
        next_filled_quantity=150,
        transition_kind=OrderTransitionKind.FILL_APPLIED,
        source_event_type="FILL_MATCHED",
        source_event_id=fill.fill_id,
        source_event_hash=fill.evidence_hash,
        occurred_at=fill.executed_at,
        recorded_at=_market_at(10, 10, 3),
        provenance=fill.provenance,
        related_fill_id=fill.fill_id,
        fill_execution_evidence=fill,
    )
    validate_order_transition_evidence(value)
    return value


def _before_lots(fill: FillExecutionEvidence) -> tuple[LotSnapshot, ...]:
    common: dict[str, object] = {
        "account_id": fill.account_id,
        "stock_code": fill.stock_code,
        "theme_code": "banking",
        "strategy_version": "mysql57-accounting-strategy-v1",
        "original_quantity": 100,
        "remaining_quantity": 100,
        "allocated_buy_fee": Decimal("0.30"),
        "position_state": PositionState.VALID,
        "approved_target_quantity": 200,
        "add_count": 0,
        "initial_stop": Decimal("8.000000"),
        "protective_stop": Decimal("8.500000"),
        "invalidation_condition": "synthetic FIFO acceptance lot",
        "version": 1,
        "closed_at": None,
    }
    return (
        LotSnapshot(
            lot_id=ACCOUNTING_LOT_IDS[0],
            opened_fill_id=ACCOUNTING_OPENED_FILL_IDS[0],
            opened_trade_date=date(2026, 7, 30),
            settlement_date=date(2026, 7, 31),
            cost_price=Decimal("9.000000"),
            created_at=datetime(2026, 7, 30, 10, tzinfo=MARKET_ZONE),
            **common,
        ),
        LotSnapshot(
            lot_id=ACCOUNTING_LOT_IDS[1],
            opened_fill_id=ACCOUNTING_OPENED_FILL_IDS[1],
            opened_trade_date=date(2026, 7, 31),
            settlement_date=date(2026, 8, 1),
            cost_price=Decimal("9.500000"),
            created_at=datetime(2026, 7, 31, 10, tzinfo=MARKET_ZONE),
            **common,
        ),
    )


def _lot_effects(fill: FillExecutionEvidence) -> tuple[LotAccountingEffect, ...]:
    first_before, second_before = _before_lots(fill)
    first_after = replace(
        first_before,
        remaining_quantity=0,
        position_state=PositionState.CLOSED,
        version=2,
        closed_at=fill.executed_at,
    )
    second_after = replace(second_before, remaining_quantity=50, version=2)
    first = LotAccountingEffect(
        fill_execution_evidence=fill,
        effect_sequence=0,
        lot_transition_sequence=0,
        effect_kind=LotEffectKind.SELL_FIFO_CONSUME,
        before_lot=first_before,
        after_lot=first_after,
        consumed_quantity=100,
        occurred_at=fill.executed_at,
        bound_at=_market_at(10, 10, 3),
        provenance=fill.provenance,
    )
    second = LotAccountingEffect(
        fill_execution_evidence=fill,
        effect_sequence=1,
        lot_transition_sequence=0,
        effect_kind=LotEffectKind.SELL_FIFO_CONSUME,
        before_lot=second_before,
        after_lot=second_after,
        consumed_quantity=50,
        occurred_at=fill.executed_at,
        bound_at=_market_at(10, 10, 4),
        provenance=fill.provenance,
        previous_effect_id=first.lot_transition_evidence_id,
        previous_effect_hash=first.effect_hash,
    )
    return first, second


def _lot_seed(snapshot: LotSnapshot) -> CanonicalSeedRow:
    return CanonicalSeedRow(
        "st_position_lot_v2",
        {
            "lot_id": snapshot.lot_id,
            "account_id": snapshot.account_id,
            "stock_code": snapshot.stock_code,
            "theme_code": snapshot.theme_code,
            "strategy_version": snapshot.strategy_version,
            "opened_fill_id": snapshot.opened_fill_id,
            "opened_trade_date": snapshot.opened_trade_date,
            "settlement_date": snapshot.settlement_date,
            "original_quantity": snapshot.original_quantity,
            "remaining_quantity": snapshot.remaining_quantity,
            "cost_price": snapshot.cost_price,
            "allocated_buy_fee": snapshot.allocated_buy_fee,
            "position_state": snapshot.position_state.value,
            "approved_target_quantity": snapshot.approved_target_quantity,
            "add_count": snapshot.add_count,
            "initial_stop": snapshot.initial_stop,
            "protective_stop": snapshot.protective_stop,
            "invalidation_condition": snapshot.invalidation_condition,
            "version": snapshot.version,
            "created_at": snapshot.created_at.replace(tzinfo=None),
            "closed_at": (
                None
                if snapshot.closed_at is None
                else snapshot.closed_at.astimezone(MARKET_ZONE).replace(tzinfo=None)
            ),
        },
    )


def _opened_fill_seed(
    *,
    fill_id: str,
    account_id: str,
    stock_code: str,
    number: int,
) -> CanonicalSeedRow:
    opened_day = 30 + number
    filled_at = datetime(2026, 7, opened_day, 10)
    price = Decimal("9.000000") + Decimal("0.500000") * number
    gross = price * 100
    net_cash = -(gross + Decimal("0.30"))
    return CanonicalSeedRow(
        "st_fill_v2",
        {
            "fill_id": fill_id,
            "order_id": f"mysql57-accounting-open-order-{number}",
            "account_id": account_id,
            "stock_code": stock_code,
            "side": "BUY",
            "quantity": 100,
            "price": price,
            "gross_amount": gross,
            "fee_amount": Decimal("0.30"),
            "net_cash_amount": net_cash,
            "quote_event_id": hashlib.sha256(
                f"accounting-open-quote-{number}".encode("ascii")
            ).hexdigest(),
            "match_event_id": hashlib.sha256(
                f"accounting-open-match-{number}".encode("ascii")
            ).hexdigest(),
            "idempotency_key": hashlib.sha256(
                f"accounting-open-fill-{number}".encode("ascii")
            ).hexdigest(),
            "filled_at": filled_at,
            "created_at": filled_at + timedelta(seconds=1),
        },
    )


def _accounting_seed_rows(
    fill: FillExecutionEvidence,
    cash_rows: tuple[CashEventBinding, ...],
    effects: tuple[LotAccountingEffect, ...],
) -> tuple[CanonicalSeedRow, ...]:
    fill_payload = fill.fill_payload.value()
    order_payload = fill.order_payload.value()
    genesis_payload = cash_rows[0].cash_event_payload.value()
    sell_payload = cash_rows[1].cash_event_payload.value()
    return (
        CanonicalSeedRow(
            "st_trade_account_v2",
            {
                "account_id": ACCOUNTING_ACCOUNT_ID,
                "account_name": "MySQL 5.7 accounting SELL account",
                "status": "ACTIVE",
                "initial_cash": ACCOUNTING_GENESIS_CASH_BALANCE,
                # The harness appends genesis evidence first, then updates this
                # one canonical account row to scenario.account_cash_after.
                "cash_balance": ACCOUNTING_GENESIS_CASH_BALANCE,
                "peak_equity": ACCOUNTING_GENESIS_CASH_BALANCE,
                "policy_version": "mysql57-accounting-policy-v1",
                "policy_hash": hashlib.sha256(
                    b"mysql57-accounting-policy-v1"
                ).hexdigest(),
                "fee_profile_version": None,
                "instrument_rule_version": None,
                "real_trading_enabled": 0,
                "created_at": _market_naive(7),
                "updated_at": _market_naive(8),
            },
        ),
        CanonicalSeedRow(
            "st_order_v2",
            {
                "order_id": fill.order_id,
                "account_id": fill.account_id,
                "intent_id": order_payload["intent_id"],
                "stock_code": fill.stock_code,
                "side": "SELL",
                "order_type": "LIMIT",
                "limit_price": Decimal("10.500000"),
                "quantity": 150,
                "filled_quantity": 150,
                "status": "FILLED",
                "waiting_reason": None,
                "earliest_at": _market_naive(9, 30),
                "expires_at": _market_naive(15),
                "idempotency_key": order_payload["idempotency_key"],
                "created_at": _market_naive(9),
                "updated_at": _market_naive(10, 10, 1),
            },
        ),
        _opened_fill_seed(
            fill_id=ACCOUNTING_OPENED_FILL_IDS[0],
            account_id=fill.account_id,
            stock_code=fill.stock_code,
            number=0,
        ),
        _opened_fill_seed(
            fill_id=ACCOUNTING_OPENED_FILL_IDS[1],
            account_id=fill.account_id,
            stock_code=fill.stock_code,
            number=1,
        ),
        CanonicalSeedRow(
            "st_fill_v2",
            {
                "fill_id": fill.fill_id,
                "order_id": fill.order_id,
                "account_id": fill.account_id,
                "stock_code": fill.stock_code,
                "side": "SELL",
                "quantity": 150,
                "price": Decimal("10.500000"),
                "gross_amount": Decimal("1575.00"),
                "fee_amount": Decimal("1.28"),
                "net_cash_amount": ACCOUNTING_SELL_NET_CASH,
                "quote_event_id": fill.quote_evidence.quote_event_id,
                "match_event_id": fill_payload["match_event_id"],
                "idempotency_key": fill_payload["idempotency_key"],
                "filled_at": _market_naive(10, 10),
                "created_at": _market_naive(10, 10, 1),
            },
        ),
        CanonicalSeedRow(
            "st_cash_ledger_v2",
            {
                "cash_event_id": ACCOUNTING_CASH_GENESIS_ID,
                "account_id": fill.account_id,
                "business_event_key": genesis_payload["business_event_key"],
                "event_type": "INITIAL_DEPOSIT",
                "amount": ACCOUNTING_GENESIS_CASH_BALANCE,
                "balance_after": ACCOUNTING_GENESIS_CASH_BALANCE,
                "related_order_id": None,
                "related_fill_id": None,
                "reversal_of": None,
                "occurred_at": _market_naive(8),
                "created_at": _market_naive(8),
            },
        ),
        CanonicalSeedRow(
            "st_cash_ledger_v2",
            {
                "cash_event_id": ACCOUNTING_CASH_SELL_ID,
                "account_id": fill.account_id,
                "business_event_key": sell_payload["business_event_key"],
                "event_type": "SELL_FILL",
                "amount": ACCOUNTING_SELL_NET_CASH,
                "balance_after": ACCOUNTING_CASH_AFTER,
                "related_order_id": fill.order_id,
                "related_fill_id": fill.fill_id,
                "reversal_of": None,
                "occurred_at": _market_naive(10, 10),
                "created_at": _market_naive(10, 10, 1),
            },
        ),
        *(_lot_seed(effect.after_lot) for effect in effects),
    )


def build_accounting_behavioral_scenario() -> AccountingBehavioralScenario:
    """Build a writer-valid SELL FIFO outcome over two canonical V2 lots.

    The base behavioral scenario must already have inserted its calendar,
    quote, fee, and instrument-rule parents and core evidence.  Insert
    ``seed_rows`` next and append ``fill_evidence``.  Append the first
    ``cash_evidence_rows`` item (genesis), update the sole canonical account
    row from ``account_cash_before`` to ``account_cash_after``, then append the
    second (SELL) cash item, ``order_transition``, and finally the accounting
    outcome.
    """

    fill = _build_sell_fill()
    cash_rows = _cash_evidence(fill)
    transition = _order_transition(fill)
    effects = _lot_effects(fill)
    outcome = FillAccountingOutcome(
        fill_execution_evidence=fill,
        cash_binding=cash_rows[1],
        order_transition=transition,
        account_cash_before=ACCOUNTING_GENESIS_CASH_BALANCE,
        account_cash_after=ACCOUNTING_CASH_AFTER,
        lot_effects=effects,
        recorded_at=_market_at(10, 10, 5),
        provenance=fill.provenance,
    )
    validate_fill_accounting_outcome(outcome)
    conflicting_outcome = replace(
        outcome,
        recorded_at=outcome.recorded_at + timedelta(seconds=1),
    )
    validate_fill_accounting_outcome(conflicting_outcome)
    if conflicting_outcome.accounting_outcome_id == outcome.accounting_outcome_id:
        raise RuntimeError("same-fill accounting conflict did not change content")
    return AccountingBehavioralScenario(
        seed_rows=_accounting_seed_rows(fill, cash_rows, effects),
        account_id=ACCOUNTING_ACCOUNT_ID,
        account_cash_before=ACCOUNTING_GENESIS_CASH_BALANCE,
        account_cash_after=ACCOUNTING_CASH_AFTER,
        fill_evidence=fill,
        cash_evidence_rows=cash_rows,
        order_transition=transition,
        outcome=outcome,
        conflicting_outcome=conflicting_outcome,
    )


__all__ = [
    "ACCOUNTING_ACCOUNT_ID",
    "ACCOUNTING_CASH_AFTER",
    "ACCOUNTING_GENESIS_CASH_BALANCE",
    "ACCOUNTING_LOT_IDS",
    "AUTHORITY_KEY_REVOCATION",
    "AUTHORITY_RECEIPT_REVOCATION",
    "AccountingBehavioralScenario",
    "AuthorityBehavioralCase",
    "AuthorityBehavioralScenario",
    "AuthorityReceiptCandidate",
    "build_accounting_behavioral_scenario",
    "build_authority_behavioral_scenario",
]
