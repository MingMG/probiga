from __future__ import annotations

import base64
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
from pathlib import Path
import re
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy.exc import IntegrityError

from server.integrations.v2_execution_evidence_authority import (
    AuthorityTrustKey,
    Ed25519AuthorityVerifier,
    MySQLRegistryBackedAuthorityVerifier,
    SignedAuthorityReceipt,
    authority_receipt_signature_message,
    build_authority_claim,
)

from server.integrations.v2_execution_evidence_writer import (
    EvidenceAppendConflictError,
    EvidenceAppendStatus,
    EvidenceAuthorityUnsupportedError,
    EvidenceCanonicalRowError,
    EvidenceTransactionError,
    append_cash_event_binding,
    append_fill_execution_evidence,
    append_market_calendar_evidence,
    append_order_transition_evidence,
    append_quote_receipt_evidence,
)
from server.integrations.v2_execution_evidence_writer import writer as writer_impl
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
)
from tools.trading_v2_evidence_behavioral_scenario import (
    build_behavioral_scenario,
    build_conflicting_double_writer_scenario,
)


ZONE = ZoneInfo("Asia/Shanghai")
TRADE_DATE = date(2026, 8, 3)


class _Result:
    def __init__(self, row: Mapping[str, Any] | None = None, rowcount: int = -1):
        self._row = None if row is None else dict(row)
        self.rowcount = rowcount

    def mappings(self) -> "_Result":
        return self

    def first(self) -> Mapping[str, Any] | None:
        return None if self._row is None else dict(self._row)

    def all(self) -> tuple[Mapping[str, Any], ...]:
        return () if self._row is None else (dict(self._row),)

    def __iter__(self):
        return iter(self.all())


class ScriptedConnection:
    def __init__(
        self,
        *,
        active: bool = True,
        rows: Mapping[str, Mapping[str, Any] | None] | None = None,
        heads: Mapping[str, Mapping[str, Any] | None] | None = None,
        fence_state: str = "INACTIVE",
    ) -> None:
        self.active = active
        self.rows = {
            key: None if value is None else dict(value)
            for key, value in (rows or {}).items()
        }
        self.heads = {
            key: None if value is None else dict(value)
            for key, value in (heads or {}).items()
        }
        self.tables: dict[str, dict[str, dict[str, Any]]] = {}
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.sql_calls: list[tuple[str, str, dict[str, Any]]] = []
        self.lifecycle_calls: list[str] = []
        self.fence_state = fence_state

    def in_transaction(self) -> bool:
        return self.active

    def begin(self) -> None:  # must never be called
        self.lifecycle_calls.append("begin")
        raise AssertionError("writer changed transaction lifecycle")

    def commit(self) -> None:  # must never be called
        self.lifecycle_calls.append("commit")
        raise AssertionError("writer changed transaction lifecycle")

    def rollback(self) -> None:  # must never be called
        self.lifecycle_calls.append("rollback")
        raise AssertionError("writer changed transaction lifecycle")

    def execute(self, statement: Any, params: Mapping[str, Any]) -> _Result:
        sql = str(statement)
        if "schema_migration_v2_maintenance_fence" in sql:
            payload = dict(params)
            self.calls.append(("maintenance_fence", payload))
            self.sql_calls.append(("maintenance_fence", sql, payload))
            return _Result(
                {
                    "fence_name": "execution_evidence_011_015",
                    "state": self.fence_state,
                }
            )
        match = re.search(r"/\* v2e:([^*]+) \*/", sql)
        assert match, sql
        tag = match.group(1)
        payload = dict(params)
        self.calls.append((tag, payload))
        self.sql_calls.append((tag, sql, payload))
        if tag.startswith("existing_"):
            table = tag.removeprefix("existing_")
            row = self.tables.get(table, {}).get(str(payload["primary_value"]))
            return _Result(row)
        if tag.startswith("insert_"):
            table = tag.removeprefix("insert_")
            primary = {
                "st_market_calendar_evidence_v2": "calendar_evidence_id",
                "st_quote_receipt_evidence_v2": "quote_evidence_id",
                "st_fill_execution_evidence_v2": "fill_execution_evidence_id",
                "st_cash_event_binding_v2": "cash_binding_id",
                "st_order_transition_v2": "transition_id",
            }[table]
            self.tables.setdefault(table, {})[str(payload[primary])] = dict(payload)
            return _Result(rowcount=1)
        if tag in {"select_cash_head", "select_order_head", "select_fill_head"}:
            return _Result(self.heads.get(tag))
        return _Result(self.rows.get(tag))


class AuthorityScriptedConnection(ScriptedConnection):
    def __init__(self, *, registry_row=None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.attestation_row: dict[str, Any] | None = None
        self.registry_row = None if registry_row is None else dict(registry_row)

    def execute(self, statement: Any, params: Mapping[str, Any]) -> _Result:
        sql = " ".join(str(statement).split())
        if "INNER JOIN st_execution_authority_trust_key_v2" in sql:
            self.sql_calls.append(("authority_registry", sql, dict(params)))
            return _Result(self.registry_row)
        if "st_execution_authority_attestation_v2" not in sql:
            return super().execute(statement, params)
        payload = dict(params)
        self.sql_calls.append(("authority_attestation", sql, payload))
        if sql.startswith("SELECT"):
            return _Result(self.attestation_row)
        if sql.startswith("INSERT INTO"):
            self.attestation_row = payload
            return _Result(rowcount=1)
        raise AssertionError(sql)


class _StaticAuthorityReceiptLoader:
    def __init__(self, receipt: SignedAuthorityReceipt) -> None:
        self.receipt = receipt
        self.calls = 0

    def load(self, _connection, _claim):
        self.calls += 1
        return self.receipt


def _naive(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 8, 3, hour, minute, second)


def _aware(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 8, 3, hour, minute, second, tzinfo=ZONE)


def _provenance(
    authority: AuthorityStatus = AuthorityStatus.CONTENT_HASH_ONLY,
    receipt_hash: str | None = None,
) -> EvidenceProvenance:
    return EvidenceProvenance(
        history_origin=HistoryOrigin.START_AFTER_UNKNOWN,
        history_origin_id="writer-cutover",
        history_origin_at=_aware(7),
        authority_status=authority,
        authority_receipt_hash=receipt_hash,
    )


def _account_row() -> dict[str, Any]:
    return {
        "account_id": "paper-main-v2",
        "cash_balance": Decimal("100000.00"),
    }


def _order_row(
    *,
    status: str = "FILLED",
    filled_quantity: int = 100,
    waiting_reason: str | None = None,
) -> dict[str, Any]:
    return {
        "order_id": "order-1",
        "account_id": "paper-main-v2",
        "intent_id": "intent-1",
        "stock_code": "600000.SH",
        "side": "BUY",
        "order_type": "LIMIT",
        "limit_price": Decimal("10.000000"),
        "quantity": 100,
        "filled_quantity": filled_quantity,
        "status": status,
        "waiting_reason": waiting_reason,
        "earliest_at": _naive(9, 30),
        "expires_at": _naive(15),
        "idempotency_key": "1" * 64,
        "created_at": _naive(9),
        "updated_at": _naive(10),
    }


def _quote_row() -> dict[str, Any]:
    return {
        "quote_event_id": "c" * 64,
        "stock_code": "600000.SH",
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
        "source_provider": "qmt",
        "source_batch_id": "batch-1",
        "payload_hash": "c" * 64,
        "created_at": _naive(10),
    }


def _fill_row() -> dict[str, Any]:
    match_event_id = "8" * 64
    quote_event_id = "c" * 64
    idempotency_key = hashlib.sha256(
        f"order-1|{quote_event_id}|{match_event_id}".encode("utf-8")
    ).hexdigest()
    return {
        "fill_id": "fill-1",
        "order_id": "order-1",
        "account_id": "paper-main-v2",
        "stock_code": "600000.SH",
        "side": "BUY",
        "quantity": 100,
        "price": Decimal("10.000000"),
        "gross_amount": Decimal("1000.00"),
        "fee_amount": Decimal("0.30"),
        "net_cash_amount": Decimal("-1000.30"),
        "quote_event_id": quote_event_id,
        "match_event_id": match_event_id,
        "idempotency_key": idempotency_key,
        "filled_at": _naive(10),
        "created_at": _naive(10, 0, 1),
    }


def _fee_row() -> dict[str, Any]:
    return {
        "fee_profile_version": "fee-v1",
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
    }


def _rule_row() -> dict[str, Any]:
    return {
        "stock_code": "600000.SH",
        "rule_version": "rule-v1",
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
        "fee_profile_version": "fee-v1",
        "source_snapshot_hash": "f" * 64,
        "created_at": datetime(2026, 1, 1),
    }


def _cash_row() -> dict[str, Any]:
    return {
        "cash_event_id": "cash-0",
        "account_id": "paper-main-v2",
        "business_event_key": "paper-main-v2:INITIAL_DEPOSIT",
        "event_type": "INITIAL_DEPOSIT",
        "amount": Decimal("100000.00"),
        "balance_after": Decimal("100000.00"),
        "related_order_id": None,
        "related_fill_id": None,
        "reversal_of": None,
        "occurred_at": _naive(8),
        "created_at": _naive(8),
    }


def _calendar() -> MarketCalendarEvidence:
    return MarketCalendarEvidence(
        market_code="SSE",
        trade_date=TRADE_DATE,
        calendar_version="calendar-v1",
        market_timezone="Asia/Shanghai",
        calendar_payload=CanonicalJson.from_value(
            {
                "coverage_end_at": _aware(23, 59, 59),
                "coverage_start_at": _aware(0),
                "sessions": [
                    {"session_id": "MORNING", "opens_at": "09:30:00", "closes_at": "11:30:00"},
                    {"session_id": "AFTERNOON", "opens_at": "13:00:00", "closes_at": "15:00:00"},
                ],
                "trading_days": ["2026-08-03", "2026-08-04"],
            }
        ),
        source_provider="calendar-registry",
        source_payload=CanonicalJson.from_value(
            {
                "calendar_version": "calendar-v1",
                "market_code": "SSE",
                "published_at": _aware(7, 30),
                "trade_date": "2026-08-03",
            }
        ),
        available_at=_aware(8),
        provenance=_provenance(),
    )


def _quote() -> QuoteReceiptEvidence:
    row = _quote_row()
    receipt = CanonicalJson.from_value(
        {
            "quote_event_id": row["quote_event_id"],
            "quote_row": writer_impl._canonical_quote_row(row),
            "source_batch_id": row["source_batch_id"],
            "source_payload_hash": row["payload_hash"],
            "source_provider": row["source_provider"],
        }
    )
    return QuoteReceiptEvidence(
        quote_event_id=row["quote_event_id"],
        stock_code=row["stock_code"],
        trade_date=TRADE_DATE,
        market_timezone="Asia/Shanghai",
        quote_at=_aware(9, 59, 59),
        received_at=_aware(10),
        available_at=_aware(10),
        source_provider=row["source_provider"],
        source_batch_id=row["source_batch_id"],
        source_payload_hash=row["payload_hash"],
        receipt_type=QuoteReceiptType.QMT_REALTIME,
        receipt_payload=receipt,
        provenance=_provenance(),
        source_receipt_id="receipt-1",
        source_receipt_hash="b" * 64,
    )


def _quote_without_receipt() -> QuoteReceiptEvidence:
    return replace(
        _quote(),
        receipt_type=QuoteReceiptType.NONE,
        receipt_payload=CanonicalJson.from_value({}),
        source_receipt_id=None,
        source_receipt_hash=None,
    )


def _fill(calendar: MarketCalendarEvidence, quote: QuoteReceiptEvidence) -> FillExecutionEvidence:
    order_row = _order_row()
    fill_row = _fill_row()
    fee_row = _fee_row()
    rule_row = _rule_row()
    order_payload = CanonicalJson.from_value(writer_impl._canonical_order_payload(order_row))
    fill_payload = CanonicalJson.from_value(writer_impl._canonical_fill_payload(fill_row))
    fee_payload = CanonicalJson.from_value(writer_impl._canonical_fee_payload(fee_row))
    rule_payload = CanonicalJson.from_value(writer_impl._canonical_rule_payload(rule_row))
    settlement = CanonicalJson.from_value(
        {
            "calendar_evidence_hash": calendar.evidence_hash,
            "instrument_rule_hash": rule_payload.payload_hash,
            "settlement_date": "2026-08-04",
            "settlement_days": 1,
            "stock_code": "600000.SH",
            "trade_date": "2026-08-03",
        }
    )
    matcher_request = CanonicalJson.from_value(
        {
            "calendar_evidence_hash": calendar.evidence_hash,
            "matcher_version": "matcher-v1",
            "order_id": "order-1",
            "order_payload_hash": order_payload.payload_hash,
            "quote_event_id": quote.quote_event_id,
            "quote_evidence_hash": quote.evidence_hash,
        }
    )
    matcher_response = CanonicalJson.from_value(
        {
            "fill_price": fill_payload.value()["price"],
            "fill_quantity": 100,
            "match_event_id": fill_payload.value()["match_event_id"],
            "matcher_request_hash": matcher_request.payload_hash,
            "order_id": "order-1",
            "quote_event_id": quote.quote_event_id,
            "side": "BUY",
            "status": "FILLED",
        }
    )
    accounting = CanonicalJson.from_value(
        {
            "account_id": "paper-main-v2",
            "calendar_evidence_hash": calendar.evidence_hash,
            "fee_amount": "0.30",
            "fee_schedule_hash": fee_payload.payload_hash,
            "fill_id": "fill-1",
            "gross_amount": "1000.00",
            "instrument_rule_hash": rule_payload.payload_hash,
            "matcher_output_hash": matcher_response.payload_hash,
            "net_cash_amount": "-1000.30",
            "order_id": "order-1",
            "price": "10.000000",
            "quantity": 100,
            "quote_evidence_hash": quote.evidence_hash,
            "settlement_evidence_hash": settlement.payload_hash,
            "side": "BUY",
            "stock_code": "600000.SH",
        }
    )
    return FillExecutionEvidence(
        fill_id="fill-1",
        order_id="order-1",
        order_fill_sequence=1,
        account_id="paper-main-v2",
        stock_code="600000.SH",
        fill_payload=fill_payload,
        order_payload=order_payload,
        quote_evidence=quote,
        calendar_evidence=calendar,
        fee_profile_version="fee-v1",
        fee_security_type="EQUITY",
        fee_effective_from=date(2026, 1, 1),
        fee_effective_to=None,
        fee_created_at=datetime(2026, 1, 1, tzinfo=ZONE),
        fee_schedule=fee_payload,
        instrument_rule_version="rule-v1",
        instrument_rule_effective_from=date(2026, 1, 1),
        instrument_rule_effective_to=None,
        instrument_rule_created_at=datetime(2026, 1, 1, tzinfo=ZONE),
        instrument_rule=rule_payload,
        matcher_version="matcher-v1",
        matcher_request=matcher_request,
        matcher_response=matcher_response,
        accounting_request=accounting,
        settlement_evidence=settlement,
        executed_at=_aware(10),
        bound_at=_aware(10, 0, 2),
        provenance=_provenance(),
    )


def _facts(*, order: Mapping[str, Any] | None = None) -> dict[str, Mapping[str, Any]]:
    return {
        "lock_account": _account_row(),
        "lock_order": dict(order or _order_row()),
        "select_quote": _quote_row(),
        "select_fill": _fill_row(),
        "select_fee": _fee_row(),
        "select_rule": _rule_row(),
        "select_cash": _cash_row(),
    }


def test_requires_active_caller_owned_connection() -> None:
    connection = ScriptedConnection(active=False)
    with pytest.raises(EvidenceTransactionError, match="already be in a transaction"):
        append_market_calendar_evidence(connection, _calendar())
    assert connection.calls == []
    assert connection.lifecycle_calls == []


def test_active_maintenance_fence_blocks_evidence_before_fact_locks() -> None:
    connection = ScriptedConnection(fence_state="ACTIVE")

    with pytest.raises(EvidenceTransactionError, match="maintenance fence"):
        append_market_calendar_evidence(connection, _calendar())

    assert [tag for tag, _params in connection.calls] == ["maintenance_fence"]
    assert [tag for tag, _sql, _params in connection.sql_calls] == [
        "maintenance_fence"
    ]


def test_calendar_append_exact_retry_and_divergent_retry() -> None:
    connection = ScriptedConnection()
    calendar = _calendar()
    first = append_market_calendar_evidence(connection, calendar)
    assert first.status is EvidenceAppendStatus.INSERTED
    second = append_market_calendar_evidence(connection, calendar)
    assert second.status is EvidenceAppendStatus.IDEMPOTENT
    stored = connection.tables["st_market_calendar_evidence_v2"][calendar.calendar_evidence_id]
    stored["market_code"] = "SZSE"
    with pytest.raises(EvidenceAppendConflictError, match="market_code"):
        append_market_calendar_evidence(connection, calendar)
    assert connection.lifecycle_calls == []


def test_insert_readback_must_match_every_stored_column() -> None:
    class CorruptReadbackConnection(ScriptedConnection):
        def execute(self, statement: Any, params: Mapping[str, Any]) -> _Result:
            result = super().execute(statement, params)
            tag = self.calls[-1][0]
            if tag == "existing_st_market_calendar_evidence_v2" and result.first() is not None:
                row = dict(result.first() or {})
                row["calendar_version"] = "corrupted-after-insert"
                return _Result(row)
            return result

    with pytest.raises(EvidenceAppendConflictError, match="calendar_version"):
        append_market_calendar_evidence(CorruptReadbackConnection(), _calendar())


def test_unique_insert_failure_propagates_without_querying_failed_transaction() -> None:
    class IntegrityFailureConnection(ScriptedConnection):
        def execute(self, statement: Any, params: Mapping[str, Any]) -> _Result:
            sql = str(statement)
            if "/* v2e:insert_" in sql:
                match = re.search(r"/\* v2e:([^*]+) \*/", sql)
                assert match
                tag = match.group(1)
                payload = dict(params)
                self.calls.append((tag, payload))
                self.sql_calls.append((tag, sql, payload))
                raise IntegrityError("duplicate evidence id", params, Exception("duplicate"))
            return super().execute(statement, params)

    connection = IntegrityFailureConnection()
    with pytest.raises(IntegrityError):
        append_market_calendar_evidence(connection, _calendar())
    assert [tag for tag, _ in connection.calls] == [
        "maintenance_fence",
        "existing_st_market_calendar_evidence_v2",
        "natural_calendar_identity",
        "insert_st_market_calendar_evidence_v2",
    ]


def test_all_public_writers_classify_different_content_on_natural_key() -> None:
    base = build_behavioral_scenario()
    conflict = build_conflicting_double_writer_scenario(base)
    base_rows: dict[str, list[dict[str, Any]]] = {}
    conflict_rows: dict[str, list[dict[str, Any]]] = {}
    for seed in base.seed_rows:
        base_rows.setdefault(seed.table, []).append(dict(seed.values))
    for seed in conflict.seed_rows:
        conflict_rows.setdefault(seed.table, []).append(dict(seed.values))
    base_cases = {case.evidence_type: case for case in base.cases}

    storage_builders = {
        "MARKET_CALENDAR": writer_impl._calendar_storage,
        "QUOTE_RECEIPT": writer_impl._quote_storage,
        "FILL_EXECUTION": writer_impl._fill_storage,
        "CASH_EVENT": writer_impl._cash_storage,
        "ORDER_TRANSITION": writer_impl._order_storage,
    }
    natural_tags = {
        "MARKET_CALENDAR": "natural_calendar_identity",
        "QUOTE_RECEIPT": "natural_quote_event",
        "FILL_EXECUTION": "natural_fill_id",
        "CASH_EVENT": "natural_cash_event",
        "ORDER_TRANSITION": "natural_order_sequence",
    }
    writers = {
        "MARKET_CALENDAR": append_market_calendar_evidence,
        "QUOTE_RECEIPT": append_quote_receipt_evidence,
        "FILL_EXECUTION": append_fill_execution_evidence,
        "CASH_EVENT": append_cash_event_binding,
        "ORDER_TRANSITION": append_order_transition_evidence,
    }

    for pair in conflict.pairs:
        left = pair.left.evidence
        right = pair.right.evidence
        rows: dict[str, Mapping[str, Any]] = {
            natural_tags[pair.evidence_type]: storage_builders[
                pair.evidence_type
            ](left),
        }
        if pair.evidence_type == "QUOTE_RECEIPT":
            rows["select_quote"] = conflict_rows["st_quote_event_v2"][0]
        elif pair.evidence_type == "FILL_EXECUTION":
            fill_order = next(
                row
                for row in conflict_rows["st_order_v2"]
                if row["order_id"] == right.order_id
            )
            fill_row = conflict_rows["st_fill_v2"][0]
            rows.update(
                {
                    "lock_order": {
                        key: fill_order[key]
                        for key in writer_impl.ORDER_COLUMNS
                    },
                    "lock_account": {
                        key: base_rows["st_trade_account_v2"][0][key]
                        for key in writer_impl.ACCOUNT_COLUMNS
                    },
                    "select_fill": {
                        key: fill_row[key]
                        for key in writer_impl.FILL_COLUMNS
                    },
                    "select_quote": {
                        key: base_rows["st_quote_event_v2"][0][key]
                        for key in writer_impl.QUOTE_COLUMNS
                    },
                    "select_fee": {
                        key: base_rows["st_fee_profile_v2"][0][key]
                        for key in writer_impl.FEE_COLUMNS
                    },
                    "select_rule": {
                        key: base_rows["st_instrument_rule_v2"][0][key]
                        for key in writer_impl.RULE_COLUMNS
                    },
                }
            )
        elif pair.evidence_type == "CASH_EVENT":
            account = conflict_rows["st_trade_account_v2"][0]
            cash = conflict_rows["st_cash_ledger_v2"][0]
            rows.update(
                {
                    "lock_account": {
                        key: account[key]
                        for key in writer_impl.ACCOUNT_COLUMNS
                    },
                    "select_cash": {
                        key: cash[key]
                        for key in writer_impl.CASH_COLUMNS
                    },
                }
            )
        elif pair.evidence_type == "ORDER_TRANSITION":
            order = next(
                row
                for row in conflict_rows["st_order_v2"]
                if row["order_id"] == right.order_id
            )
            rows.update(
                {
                    "lock_order": {
                        key: order[key]
                        for key in writer_impl.ORDER_COLUMNS
                    },
                    "lock_account": {
                        key: base_rows["st_trade_account_v2"][0][key]
                        for key in writer_impl.ACCOUNT_COLUMNS
                    },
                }
            )

        connection = ScriptedConnection(rows=rows)
        if pair.evidence_type == "FILL_EXECUTION":
            calendar = base_cases["MARKET_CALENDAR"].evidence
            quote = base_cases["QUOTE_RECEIPT"].evidence
            connection.tables.setdefault(
                "st_market_calendar_evidence_v2", {}
            )[calendar.calendar_evidence_id] = writer_impl._calendar_storage(
                calendar
            )
            connection.tables.setdefault(
                "st_quote_receipt_evidence_v2", {}
            )[quote.quote_evidence_id] = writer_impl._quote_storage(quote)

        with pytest.raises(EvidenceAppendConflictError, match="natural"):
            writers[pair.evidence_type](connection, right)
        tags = [tag for tag, _params in connection.calls]
        natural_tag = natural_tags[pair.evidence_type]
        assert natural_tag in tags
        assert not any(tag.startswith("insert_") for tag in tags)
        natural_sql = next(
            sql
            for tag, sql, _params in connection.sql_calls
            if tag == natural_tag
        )
        assert natural_sql.rstrip().endswith("FOR UPDATE")
        natural_index = tags.index(natural_tag)
        if pair.evidence_type == "QUOTE_RECEIPT":
            assert tags.index("select_quote") < natural_index
        elif pair.evidence_type in {"FILL_EXECUTION", "ORDER_TRANSITION"}:
            assert tags.index("lock_order") < tags.index("lock_account")
            assert tags.index("lock_account") < natural_index
        elif pair.evidence_type == "CASH_EVENT":
            assert tags.index("lock_account") < natural_index


def test_external_authority_is_explicitly_unsupported() -> None:
    base = _calendar()
    receipt_hash = base.source_payload.payload_hash
    authoritative = replace(
        base,
        provenance=_provenance(
            AuthorityStatus.EXTERNAL_RECEIPT_VERIFIED,
            receipt_hash,
        ),
        source_receipt_id="calendar-receipt",
        source_receipt_hash=receipt_hash,
    )
    connection = ScriptedConnection()
    with pytest.raises(EvidenceAuthorityUnsupportedError):
        append_market_calendar_evidence(connection, authoritative)
    assert [tag for tag, _params in connection.calls] == ["maintenance_fence"]


def test_external_authority_requires_crypto_and_attests_in_the_same_transaction() -> None:
    base = _calendar()
    receipt_hash = base.source_payload.payload_hash
    authoritative = replace(
        base,
        provenance=_provenance(
            AuthorityStatus.EXTERNAL_RECEIPT_VERIFIED,
            receipt_hash,
        ),
        source_receipt_id="calendar-receipt",
        source_receipt_hash=receipt_hash,
    )
    claim = build_authority_claim(authoritative)
    private_key = Ed25519PrivateKey.generate()
    issued_at = authoritative.available_at - timedelta(seconds=1)
    expires_at = authoritative.available_at + timedelta(hours=1)
    message = authority_receipt_signature_message(
        claim_hash=claim.claim_hash,
        source_provider=claim.source_provider,
        receipt_id=claim.receipt_id,
        receipt_hash=claim.receipt_hash,
        key_id="calendar-key",
        key_version="2026-08",
        replay_nonce="calendar-nonce-1",
        issued_at=issued_at,
        expires_at=expires_at,
    )
    signature = base64.urlsafe_b64encode(private_key.sign(message)).decode(
        "ascii"
    ).rstrip("=")
    receipt = SignedAuthorityReceipt(
        claim_hash=claim.claim_hash,
        source_provider=claim.source_provider,
        receipt_id=claim.receipt_id,
        receipt_hash=claim.receipt_hash,
        key_id="calendar-key",
        key_version="2026-08",
        replay_nonce="calendar-nonce-1",
        issued_at=issued_at,
        expires_at=expires_at,
        signature=signature,
    )
    public_key = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    caller_loader = _StaticAuthorityReceiptLoader(receipt)
    caller_owned_verifier = Ed25519AuthorityVerifier(
        loader=caller_loader,
        trust_keys=(
            AuthorityTrustKey(
                source_provider=claim.source_provider,
                key_id="calendar-key",
                key_version="2026-08",
                public_key=public_key,
                valid_from=issued_at - timedelta(days=1),
                valid_to=expires_at + timedelta(days=1),
            ),
        ),
        clock=lambda: authoritative.available_at + timedelta(seconds=1),
    )
    with pytest.raises(EvidenceAuthorityUnsupportedError) as rejected:
        append_market_calendar_evidence(
            AuthorityScriptedConnection(),
            authoritative,
            authority_verifier=caller_owned_verifier,
        )
    assert "registry-backed" in str(rejected.value.__cause__)
    assert caller_loader.calls == 0

    verification_times = iter(
        (
            authoritative.available_at + timedelta(microseconds=1),
            authoritative.available_at + timedelta(minutes=1),
            authoritative.available_at + timedelta(minutes=10),
            authoritative.available_at + timedelta(minutes=20),
        )
    )
    verifier = MySQLRegistryBackedAuthorityVerifier(
        clock=lambda: next(verification_times),
    )
    def stored(value):
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    connection = AuthorityScriptedConnection(
        registry_row={
            "envelope_json": receipt.envelope_json,
            "envelope_hash": receipt.envelope_hash,
            "public_key": public_key,
            "public_key_hash": hashlib.sha256(public_key).hexdigest(),
            "key_valid_from": stored(issued_at - timedelta(days=1)),
            "key_valid_to": stored(expires_at + timedelta(days=1)),
            "key_revoked_at": None,
            "receipt_revoked_at": None,
        }
    )

    inserted = append_market_calendar_evidence(
        connection,
        authoritative,
        authority_verifier=verifier,
    )
    replayed = append_market_calendar_evidence(
        connection,
        authoritative,
        authority_verifier=verifier,
    )

    assert inserted.status is EvidenceAppendStatus.INSERTED
    assert replayed.status is EvidenceAppendStatus.IDEMPOTENT
    assert connection.attestation_row is not None
    assert connection.attestation_row["claim_hash"] == claim.claim_hash
    assert connection.attestation_row["verification_level"] == "CRYPTOGRAPHIC"
    assert connection.attestation_row["receipt_envelope_hash"] == receipt.envelope_hash
    assert sum(tag == "authority_registry" for tag, _, _ in connection.sql_calls) == 3
    connection.registry_row["receipt_revoked_at"] = stored(
        authoritative.available_at + timedelta(minutes=15)
    )
    with pytest.raises(EvidenceAuthorityUnsupportedError) as revoked:
        append_market_calendar_evidence(
            connection,
            authoritative,
            authority_verifier=verifier,
        )
    assert "AUTHORITY_RECEIPT_REVOKED" in str(revoked.value.__cause__)
    assert connection.lifecycle_calls == []


def test_quote_requires_exact_canonical_row_not_fabricated_payload() -> None:
    connection = ScriptedConnection(rows={"select_quote": _quote_row()})
    quote = _quote()
    result = append_quote_receipt_evidence(connection, quote)
    assert result.status is EvidenceAppendStatus.INSERTED
    stored = connection.tables["st_quote_receipt_evidence_v2"][quote.quote_evidence_id]
    assert stored["source_payload_hash"] == "c" * 64
    assert stored["receipt_payload_hash"] == quote.receipt_payload.payload_hash
    assert stored["receipt_payload_hash"] != stored["source_payload_hash"]
    fabricated = quote.receipt_payload.value()
    fabricated["quote_row"]["ask1"] = "9.000000"
    bad = replace(quote, receipt_payload=CanonicalJson.from_value(fabricated))
    with pytest.raises(EvidenceCanonicalRowError, match="exact canonical"):
        append_quote_receipt_evidence(ScriptedConnection(rows={"select_quote": _quote_row()}), bad)


def test_none_quote_receipt_passes_public_quote_and_fill_writers() -> None:
    connection = ScriptedConnection(rows=_facts())
    calendar = _calendar()
    quote = _quote_without_receipt()

    append_market_calendar_evidence(connection, calendar)
    quote_result = append_quote_receipt_evidence(connection, quote)

    assert quote_result.status is EvidenceAppendStatus.INSERTED
    stored_quote = connection.tables["st_quote_receipt_evidence_v2"][
        quote.quote_evidence_id
    ]
    assert stored_quote["source_receipt_type"] == QuoteReceiptType.NONE.value
    assert stored_quote["source_receipt_id"] is None
    assert stored_quote["source_receipt_hash"] is None
    assert stored_quote["receipt_payload_json"] == "{}"

    fill = _fill(calendar, quote)
    fill_result = append_fill_execution_evidence(connection, fill)

    assert fill_result.status is EvidenceAppendStatus.INSERTED
    stored_fill = connection.tables["st_fill_execution_evidence_v2"][
        fill.fill_execution_evidence_id
    ]
    assert stored_fill["quote_evidence_id"] == quote.quote_evidence_id


def test_quote_rejects_db_precision_not_representable_by_v2_datetime() -> None:
    quote_row = _quote_row()
    quote_row["created_at"] = _naive(10).replace(microsecond=1)
    connection = ScriptedConnection(rows={"select_quote": quote_row})
    with pytest.raises(EvidenceCanonicalRowError, match="whole-second precision"):
        append_quote_receipt_evidence(connection, _quote())


def test_fill_locks_order_then_account_validates_all_rows_and_reads_back() -> None:
    connection = ScriptedConnection(rows=_facts())
    calendar = _calendar()
    quote = _quote()
    append_market_calendar_evidence(connection, calendar)
    append_quote_receipt_evidence(connection, quote)
    fill = _fill(calendar, quote)
    result = append_fill_execution_evidence(connection, fill)
    assert result.status is EvidenceAppendStatus.INSERTED
    tags = [tag for tag, _ in connection.calls]
    account_index = tags.index("lock_account")
    order_index = tags.index("lock_order")
    head_index = tags.index("select_fill_head")
    assert order_index < account_index < head_index
    stored = connection.tables["st_fill_execution_evidence_v2"][fill.fill_execution_evidence_id]
    assert stored["source_payload_hash"] if "source_payload_hash" in stored else True
    assert stored["fill_payload_json"] == fill.fill_payload.json_text
    assert connection.lifecycle_calls == []
    fact_tags = {
        "select_quote",
        "select_fill",
        "select_fee",
        "select_rule",
        "select_calendar_evidence",
        "select_quote_evidence",
    }
    for tag, sql, _ in connection.sql_calls:
        if tag in fact_tags or tag.startswith("existing_"):
            assert sql.rstrip().endswith("FOR UPDATE")


def test_fill_exact_retry_survives_later_terminal_order_state() -> None:
    connection = ScriptedConnection(rows=_facts())
    calendar = _calendar()
    quote = _quote()
    append_market_calendar_evidence(connection, calendar)
    append_quote_receipt_evidence(connection, quote)
    fill = _fill(calendar, quote)
    assert append_fill_execution_evidence(connection, fill).status is EvidenceAppendStatus.INSERTED

    connection.rows["lock_order"]["status"] = "CANCELLED"
    connection.rows["lock_order"]["waiting_reason"] = "USER_CANCELLED"
    assert append_fill_execution_evidence(connection, fill).status is EvidenceAppendStatus.IDEMPOTENT


def test_fill_rejects_payload_extra_key_and_head_gap() -> None:
    calendar = _calendar()
    quote = _quote()
    fill = _fill(calendar, quote)
    connection = ScriptedConnection(rows=_facts())
    append_market_calendar_evidence(connection, calendar)
    append_quote_receipt_evidence(connection, quote)
    extra_payload = {**fill.fill_payload.value(), "untrusted": "field"}
    bad = replace(fill, fill_payload=CanonicalJson.from_value(extra_payload))
    with pytest.raises(EvidenceCanonicalRowError, match="exact canonical"):
        append_fill_execution_evidence(connection, bad)

    split = ScriptedConnection(rows=_facts(), heads={"select_fill_head": {"order_fill_sequence": 7}})
    append_market_calendar_evidence(split, calendar)
    append_quote_receipt_evidence(split, quote)
    with pytest.raises(EvidenceAppendConflictError, match="sequence"):
        append_fill_execution_evidence(split, fill)


def test_cash_genesis_uses_locked_account_and_exact_canonical_cash_row() -> None:
    connection = ScriptedConnection(rows=_facts())
    cash = _cash_row()
    evidence = CashEventBinding(
        cash_event_id="cash-0",
        account_id="paper-main-v2",
        account_sequence=0,
        cash_event_type="INITIAL_DEPOSIT",
        cash_event_payload=CanonicalJson.from_value(writer_impl._canonical_cash_payload(cash)),
        occurred_at=_aware(8),
        bound_at=_aware(8, 0, 1),
        provenance=_provenance(),
    )
    result = append_cash_event_binding(connection, evidence)
    assert result.status is EvidenceAppendStatus.INSERTED
    tags = [tag for tag, _ in connection.calls]
    assert tags.index("lock_account") < tags.index("select_cash_head")


def test_fill_cash_binding_uses_canonical_order_then_account_lock_order() -> None:
    rows = _facts()
    rows["lock_account"] = {
        "account_id": "paper-main-v2",
        "cash_balance": Decimal("98999.70"),
    }
    cash = {
        **_cash_row(),
        "cash_event_id": "cash-buy-1",
        "business_event_key": "FILL:" + _fill_row()["idempotency_key"],
        "event_type": "BUY_FILL",
        "amount": Decimal("-1000.30"),
        "balance_after": Decimal("98999.70"),
        "related_order_id": "order-1",
        "related_fill_id": "fill-1",
        "occurred_at": _naive(10),
        "created_at": _naive(10, 0, 2),
    }
    rows["select_cash"] = cash
    previous_payload = CanonicalJson.from_value(
        {"balance_after": "100000.00"}
    )
    connection = ScriptedConnection(
        rows=rows,
        heads={
            "select_cash_head": {
                "cash_binding_id": "4" * 64,
                "cash_event_id": "cash-0",
                "account_sequence": 0,
                "binding_hash": "4" * 64,
                "cash_event_payload_json": previous_payload.json_text,
                "history_origin": HistoryOrigin.START_AFTER_UNKNOWN.value,
                "history_origin_id": "writer-cutover",
                "history_origin_at": _naive(7),
            }
        },
    )
    calendar = _calendar()
    quote = _quote()
    append_market_calendar_evidence(connection, calendar)
    append_quote_receipt_evidence(connection, quote)
    fill = _fill(calendar, quote)
    append_fill_execution_evidence(connection, fill)
    before_cash = len(connection.calls)
    binding = CashEventBinding(
        cash_event_id="cash-buy-1",
        account_id="paper-main-v2",
        account_sequence=1,
        cash_event_type="BUY_FILL",
        cash_event_payload=CanonicalJson.from_value(
            writer_impl._canonical_cash_payload(cash)
        ),
        occurred_at=_aware(10),
        bound_at=_aware(10, 0, 3),
        provenance=_provenance(),
        related_order_id="order-1",
        related_fill_id="fill-1",
        fill_execution_evidence=fill,
        previous_cash_event_id="cash-0",
        previous_binding_id="4" * 64,
        previous_binding_hash="4" * 64,
    )

    assert append_cash_event_binding(connection, binding).status is EvidenceAppendStatus.INSERTED
    cash_tags = [tag for tag, _ in connection.calls[before_cash:]]
    assert cash_tags.index("lock_order") < cash_tags.index("lock_account")


def test_cash_new_head_must_equal_locked_account_balance() -> None:
    rows = _facts()
    rows["lock_account"] = {
        "account_id": "paper-main-v2",
        "cash_balance": Decimal("99999.99"),
    }
    cash = _cash_row()
    evidence = CashEventBinding(
        cash_event_id="cash-0",
        account_id="paper-main-v2",
        account_sequence=0,
        cash_event_type="INITIAL_DEPOSIT",
        cash_event_payload=CanonicalJson.from_value(writer_impl._canonical_cash_payload(cash)),
        occurred_at=_aware(8),
        bound_at=_aware(8, 0, 1),
        provenance=_provenance(),
    )
    with pytest.raises(EvidenceCanonicalRowError, match="account cash balance"):
        append_cash_event_binding(ScriptedConnection(rows=rows), evidence)


def test_cash_cannot_create_a_second_branch_from_an_existing_head() -> None:
    cash = _cash_row()
    payload = CanonicalJson.from_value(writer_impl._canonical_cash_payload(cash))
    evidence = CashEventBinding(
        cash_event_id="cash-0",
        account_id="paper-main-v2",
        account_sequence=0,
        cash_event_type="INITIAL_DEPOSIT",
        cash_event_payload=payload,
        occurred_at=_aware(8),
        bound_at=_aware(8, 0, 1),
        provenance=_provenance(),
    )
    head = {
        "cash_binding_id": "4" * 64,
        "cash_event_id": "cash-existing",
        "account_sequence": 0,
        "binding_hash": "4" * 64,
        "cash_event_payload_json": payload.json_text,
        "history_origin": HistoryOrigin.START_AFTER_UNKNOWN.value,
        "history_origin_id": "writer-cutover",
        "history_origin_at": _naive(7),
    }
    connection = ScriptedConnection(
        rows=_facts(),
        heads={"select_cash_head": head},
    )
    with pytest.raises(EvidenceAppendConflictError, match="sequence"):
        append_cash_event_binding(connection, evidence)


def test_order_head_fork_is_rejected_and_exact_old_retry_is_idempotent() -> None:
    order = _order_row(status="CREATED", filled_quantity=0)
    payload = CanonicalJson.from_value(writer_impl._canonical_order_payload(order))
    genesis = OrderTransitionEvidence(
        order_id="order-1",
        account_id="paper-main-v2",
        order_payload=payload,
        transition_sequence=0,
        from_status=OrderStatus.CREATED,
        to_status=OrderStatus.CREATED,
        previous_filled_quantity=0,
        next_filled_quantity=0,
        transition_kind=OrderTransitionKind.ORDER_CREATED,
        source_event_type="ORDER_CREATED",
        source_event_id="order-1:created",
        source_event_hash="2" * 64,
        occurred_at=_aware(9),
        recorded_at=_aware(9, 0, 1),
        provenance=_provenance(),
    )
    connection = ScriptedConnection(rows=_facts(order=order))
    inserted = append_order_transition_evidence(connection, genesis)
    assert inserted.status is EvidenceAppendStatus.INSERTED
    tags = [tag for tag, _ in connection.calls]
    assert tags.index("lock_order") < tags.index("lock_account")
    assert append_order_transition_evidence(connection, genesis).status is EvidenceAppendStatus.IDEMPOTENT

    approved_order = _order_row(status="RISK_APPROVED", filled_quantity=0)
    approved = OrderTransitionEvidence(
        order_id="order-1",
        account_id="paper-main-v2",
        order_payload=payload,
        transition_sequence=1,
        from_status=OrderStatus.CREATED,
        to_status=OrderStatus.RISK_APPROVED,
        previous_filled_quantity=0,
        next_filled_quantity=0,
        transition_kind=OrderTransitionKind.STATUS_CHANGE,
        source_event_type="RISK_APPROVED",
        source_event_id="order-1:risk",
        source_event_hash="3" * 64,
        occurred_at=_aware(9, 1),
        recorded_at=_aware(9, 1, 1),
        provenance=_provenance(),
        previous_transition_id=genesis.transition_id,
        previous_transition_hash=genesis.transition_hash,
    )
    forked_head = {
        "transition_id": "4" * 64,
        "transition_sequence": 0,
        "transition_hash": "4" * 64,
        "order_payload_hash": payload.payload_hash,
        "to_status": "CREATED",
        "next_filled_quantity": 0,
        "history_origin": HistoryOrigin.START_AFTER_UNKNOWN.value,
        "history_origin_id": "writer-cutover",
        "history_origin_at": _naive(7),
    }
    fork = ScriptedConnection(
        rows=_facts(order=approved_order),
        heads={"select_order_head": forked_head},
    )
    with pytest.raises(EvidenceAppendConflictError, match="previous identifiers"):
        append_order_transition_evidence(fork, approved)


def test_writer_source_has_no_transaction_lifecycle_or_production_source_dependency() -> None:
    source = Path(writer_impl.__file__).read_text(encoding="utf-8").lower()
    for forbidden in (".begin(", ".commit(", ".rollback(", " engine"):
        assert forbidden not in source
    assert "from server.trading_v2.execution import" not in source
    assert "from server.trading_v3" not in source


def test_mysql_behavioral_scenario_is_accepted_by_public_writer() -> None:
    scenario = build_behavioral_scenario()
    seeds: dict[str, list[dict[str, Any]]] = {}
    for seed in scenario.seed_rows:
        seeds.setdefault(seed.table, []).append(dict(seed.values))
    orders = {
        str(row["order_id"]): row for row in seeds["st_order_v2"]
    }
    account = seeds["st_trade_account_v2"][0]

    class ScenarioConnection(ScriptedConnection):
        def execute(self, statement: Any, params: Mapping[str, Any]) -> _Result:
            if "/* v2e:lock_order */" in str(statement):
                self.rows["lock_order"] = dict(orders[str(params["order_id"])])
            return super().execute(statement, params)

    connection = ScenarioConnection(
        rows={
            "lock_account": {
                "account_id": account["account_id"],
                "cash_balance": account["cash_balance"],
            },
            "select_quote": seeds["st_quote_event_v2"][0],
            "select_fill": seeds["st_fill_v2"][0],
            "select_fee": seeds["st_fee_profile_v2"][0],
            "select_rule": seeds["st_instrument_rule_v2"][0],
            "select_cash": seeds["st_cash_ledger_v2"][0],
        }
    )
    appenders = {
        "MARKET_CALENDAR": append_market_calendar_evidence,
        "QUOTE_RECEIPT": append_quote_receipt_evidence,
        "FILL_EXECUTION": append_fill_execution_evidence,
        "CASH_EVENT": append_cash_event_binding,
        "ORDER_TRANSITION": append_order_transition_evidence,
    }

    for expected_status in (
        EvidenceAppendStatus.INSERTED,
        EvidenceAppendStatus.IDEMPOTENT,
    ):
        for case in scenario.cases:
            result = appenders[case.evidence_type](connection, case.evidence)
            assert result.status is expected_status
