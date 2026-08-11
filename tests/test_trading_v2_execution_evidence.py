from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta
import hashlib
import re
from zoneinfo import ZoneInfo

import pytest

from server.db.migrations_v2 import MIGRATIONS, _checksum
from server.trading_v2.domain import OrderStatus
from server.trading_v2.execution_evidence import (
    AuthorityStatus,
    CanonicalJson,
    CashEventBinding,
    EvidenceProvenance,
    ExecutionEvidenceInvariantError,
    FillExecutionEvidence,
    HistoryOrigin,
    MarketCalendarEvidence,
    OrderTransitionEvidence,
    OrderTransitionKind,
    QuoteReceiptEvidence,
    QuoteReceiptType,
    validate_cash_event_binding,
    validate_cash_event_binding_chain,
    validate_fill_execution_evidence,
    validate_market_calendar_evidence,
    validate_order_transition_chain,
    validate_order_transition_evidence,
    validate_quote_receipt_evidence,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")
TRADE_DATE = date(2026, 8, 3)

LEGACY_MIGRATION_CHECKSUMS = {
    "20260725_001_trading_v2_core": (
        "c21ed007b17ff18604d6f022945db330cc2d8e4aef270104570e5eb60ccd6a40"
    ),
    "20260725_002_trading_v2_jobs_and_lifecycle": (
        "e54264bcb2392b186a1ee3b9b8c478ced6d80bf7d120710ae3a5c1d36663676d"
    ),
    "20260725_003_trading_v2_execution_research_ops": (
        "7be2c5567b0c52c97bdcd80fc17a4eb3d28c303b95acff799e5c5c35705b2a6c"
    ),
    "20260725_004_trading_v2_etf_truth_and_forward": (
        "451b18146a10140b3d7256018fbf885823112ec62239eae1e00e6a6ea5894435"
    ),
    "20260725_005_trading_v2_theme_risk_chain": (
        "2356709db6d2cb906cf4c839873e3b1ab668d9fb74e0be04e4dcd2eb227e3c8b"
    ),
    "20260726_006_real_trading_hard_guard": (
        "16f75c5f0e9e329ebb632cc8cd895c96a626ce76b0c364134e9c54f1b31f9016"
    ),
    "20260726_007_market_regime_transition_state": (
        "e47ac4757eb6990a4c741cd80d74f638f3429a89cf3103ab4f17497602b8b0f1"
    ),
    "20260727_008_intraday_dynamic_activation": (
        "7aa2c2a51f1a69afbbfeb2408520172d8103aa8f8cfa46cb2ae5558ac9e26d63"
    ),
    "20260730_009_public_quote_failover": (
        "d53d1315dd695bb570e1b9058156a3f6a77a86d68fe71d939aec523a4827fb61"
    ),
    "20260730_010_qmt_end_to_end_health": (
        "d4a17a3f04c8d5fb0a51ea99c7cfea271abd6576a2ec829d8e57743d55f4d2b8"
    ),
    "20260803_011_v2_execution_evidence_bindings": (
        "234a2b7a82573b5551b1485dd68598156e26d050d3b2d9b6a6ea76d3c34072d1"
    ),
    "20260803_012_v2_execution_evidence_guards": (
        "cf596bc5157ea5f6d835c07089556164cde9c0fcaf0c3ace10f10b15ba4b6fd1"
    ),
    "20260803_013_v2_execution_evidence_natural_keys": (
        "51addc459d4caae896ee656e901123646deb6a46584ac274092aa65026917eb8"
    ),
    "20260803_014_v2_execution_authority_attestations": (
        "984e2ea7c637c728745b9b21c3b508980cc046c1c434d9851619984918a3823d"
    ),
    "20260803_015_v2_accounting_outcome_evidence": (
        "8e06e57c38f7365fa471a7bde09f5cd4a3ea5aef5fee03c6195fd2930b725a2c"
    ),
}


def _at(hour: int, minute: int = 0, second: int = 0, microsecond: int = 0) -> datetime:
    return datetime(
        2026,
        8,
        3,
        hour,
        minute,
        second,
        microsecond,
        tzinfo=SHANGHAI,
    )


def _provenance(
    *,
    authority: AuthorityStatus = AuthorityStatus.CONTENT_HASH_ONLY,
    receipt_hash: str | None = None,
    history: HistoryOrigin = HistoryOrigin.START_AFTER_UNKNOWN,
    origin_at: datetime | None = None,
) -> EvidenceProvenance:
    return EvidenceProvenance(
        history_origin=history,
        history_origin_id=(None if history is HistoryOrigin.UNKNOWN else "cutover-1"),
        history_origin_at=(
            None
            if history is HistoryOrigin.UNKNOWN
            else (origin_at if origin_at is not None else _at(8))
        ),
        authority_status=authority,
        authority_receipt_hash=receipt_hash,
    )


def _calendar(*, available_at: datetime | None = None) -> MarketCalendarEvidence:
    source_payload = CanonicalJson.from_value(
        {
            "calendar_version": "calendar-2026-v1",
            "published_at": _at(7, 59),
            "revision": 17,
        }
    )
    receipt_hash = source_payload.payload_hash
    return MarketCalendarEvidence(
        market_code="SSE",
        trade_date=TRADE_DATE,
        calendar_version="calendar-2026-v1",
        market_timezone="Asia/Shanghai",
        calendar_payload=CanonicalJson.from_value(
            {
                "coverage_end_at": _at(23, 59, 59),
                "coverage_start_at": _at(0),
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
                "trading_days": ["2026-08-03", "2026-08-04"],
            }
        ),
        source_provider="exchange-calendar-registry",
        source_payload=source_payload,
        source_receipt_id="calendar-receipt-17",
        source_receipt_hash=receipt_hash,
        available_at=available_at or _at(8),
        provenance=_provenance(
            authority=AuthorityStatus.EXTERNAL_RECEIPT_VERIFIED,
            receipt_hash=receipt_hash,
            origin_at=_at(7, 59),
        ),
    )


def _quote() -> QuoteReceiptEvidence:
    quote_event_id = "c" * 64
    receipt_payload = CanonicalJson.from_value(
        {
            "capture_mode": "FORWARD",
            "quality_status": "PASS",
            "quote_event_id": quote_event_id,
            "source_batch_id": "qmt-batch-1",
            "source_payload_hash": quote_event_id,
            "source_provider": "qmt",
        }
    )
    receipt_hash = receipt_payload.payload_hash
    return QuoteReceiptEvidence(
        quote_event_id=quote_event_id,
        stock_code="600000.SH",
        trade_date=TRADE_DATE,
        market_timezone="Asia/Shanghai",
        quote_at=_at(9, 59, 59),
        received_at=_at(10),
        available_at=_at(10),
        source_provider="qmt",
        source_batch_id="qmt-batch-1",
        source_payload_hash=quote_event_id,
        receipt_type=QuoteReceiptType.QMT_REALTIME,
        source_receipt_id="qmt-receipt-1",
        source_receipt_hash=receipt_hash,
        receipt_payload=receipt_payload,
        provenance=_provenance(
            authority=AuthorityStatus.EXTERNAL_RECEIPT_VERIFIED,
            receipt_hash=receipt_hash,
        ),
    )


def _fill(**overrides: object) -> FillExecutionEvidence:
    executed_at = _at(10)
    bound_at = _at(10, 0, 2)
    fill_created_at = _at(10, 0, 1)
    quote = _quote()
    calendar = _calendar()
    fee_created_at = datetime(2026, 1, 1, tzinfo=SHANGHAI)
    rule_created_at = datetime(2026, 1, 1, tzinfo=SHANGHAI)
    match_event_id = "8" * 64
    idempotency_key = hashlib.sha256(
        f"order-1|{quote.quote_event_id}|{match_event_id}".encode("utf-8")
    ).hexdigest()
    fill_payload = CanonicalJson.from_value(
        {
            "account_id": "paper-main-v2",
            "created_at": fill_created_at,
            "fee_amount": "0.30",
            "fill_id": "fill-1",
            "filled_at": executed_at,
            "gross_amount": "1000.00",
            "idempotency_key": idempotency_key,
            "match_event_id": match_event_id,
            "net_cash_amount": "-1000.30",
            "order_id": "order-1",
            "price": "10.00",
            "quantity": 100,
            "quote_event_id": quote.quote_event_id,
            "side": "BUY",
            "stock_code": "600000.SH",
        }
    )
    order_payload = CanonicalJson.from_value(
        {
            "account_id": "paper-main-v2",
            "created_at": _at(9),
            "earliest_at": _at(9, 30),
            "expires_at": _at(15),
            "limit_price": "10.00",
            "order_id": "order-1",
            "order_type": "LIMIT",
            "quantity": 100,
            "side": "BUY",
            "stock_code": "600000.SH",
        }
    )
    fee_schedule = CanonicalJson.from_value(
        {
            "buy_commission_rate": "0.0003",
            "created_at": fee_created_at,
            "effective_from": "2026-01-01",
            "effective_to": None,
            "fee_profile_version": "fee-v1",
            "security_type": "EQUITY",
        }
    )
    instrument_rule = CanonicalJson.from_value(
        {
            "created_at": rule_created_at,
            "effective_from": "2026-01-01",
            "effective_to": None,
            "fee_profile_version": "fee-v1",
            "rule_version": "rule-v1",
            "settlement_days": 1,
            "stock_code": "600000.SH",
            "tick_size": "0.01",
        }
    )
    settlement = CanonicalJson.from_value(
        {
            "calendar_evidence_hash": calendar.evidence_hash,
            "instrument_rule_hash": instrument_rule.payload_hash,
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
            "fill_price": "10.00",
            "fill_quantity": 100,
            "match_event_id": "8" * 64,
            "matcher_request_hash": matcher_request.payload_hash,
            "order_id": "order-1",
            "quote_event_id": quote.quote_event_id,
            "side": "BUY",
            "status": "FILLED",
        }
    )
    accounting_request = CanonicalJson.from_value(
        {
            "account_id": "paper-main-v2",
            "calendar_evidence_hash": calendar.evidence_hash,
            "fee_amount": "0.30",
            "fee_schedule_hash": fee_schedule.payload_hash,
            "fill_id": "fill-1",
            "gross_amount": "1000.00",
            "instrument_rule_hash": instrument_rule.payload_hash,
            "matcher_output_hash": matcher_response.payload_hash,
            "net_cash_amount": "-1000.30",
            "order_id": "order-1",
            "price": "10.00",
            "quantity": 100,
            "quote_evidence_hash": quote.evidence_hash,
            "settlement_evidence_hash": settlement.payload_hash,
            "side": "BUY",
            "stock_code": "600000.SH",
        }
    )
    values: dict[str, object] = {
        "fill_id": "fill-1",
        "order_id": "order-1",
        "order_fill_sequence": 1,
        "account_id": "paper-main-v2",
        "stock_code": "600000.SH",
        "fill_payload": fill_payload,
        "order_payload": order_payload,
        "quote_evidence": quote,
        "calendar_evidence": calendar,
        "fee_profile_version": "fee-v1",
        "fee_security_type": "EQUITY",
        "fee_effective_from": date(2026, 1, 1),
        "fee_effective_to": None,
        "fee_created_at": fee_created_at,
        "fee_schedule": fee_schedule,
        "instrument_rule_version": "rule-v1",
        "instrument_rule_effective_from": date(2026, 1, 1),
        "instrument_rule_effective_to": None,
        "instrument_rule_created_at": rule_created_at,
        "instrument_rule": instrument_rule,
        "matcher_version": "matcher-v1",
        "matcher_request": matcher_request,
        "matcher_response": matcher_response,
        "accounting_request": accounting_request,
        "settlement_evidence": settlement,
        "executed_at": executed_at,
        "bound_at": bound_at,
        "provenance": _provenance(),
    }
    values.update(overrides)
    return FillExecutionEvidence(**values)  # type: ignore[arg-type]


def _cash_payload(
    *,
    cash_event_id: str,
    event_type: str,
    occurred_at: datetime,
    related_order_id: str | None = None,
    related_fill_id: str | None = None,
    reversal_of: str | None = None,
    business_event_key: str | None = None,
) -> CanonicalJson:
    return CanonicalJson.from_value(
        {
            "account_id": "paper-main-v2",
            "amount": "-1000.30" if event_type == "BUY_FILL" else "100000.00",
            "balance_after": "98999.70" if event_type == "BUY_FILL" else "100000.00",
            "business_event_key": business_event_key
            or "paper-main-v2:INITIAL_DEPOSIT",
            "cash_event_id": cash_event_id,
            "created_at": occurred_at,
            "event_type": event_type,
            "occurred_at": occurred_at,
            "related_fill_id": related_fill_id,
            "related_order_id": related_order_id,
            "reversal_of": reversal_of,
        }
    )


def _order_genesis(provenance: EvidenceProvenance) -> OrderTransitionEvidence:
    order_payload = _fill().order_payload
    return OrderTransitionEvidence(
        order_id="order-1",
        account_id="paper-main-v2",
        order_payload=order_payload,
        transition_sequence=0,
        from_status=OrderStatus.CREATED,
        to_status=OrderStatus.CREATED,
        previous_filled_quantity=0,
        next_filled_quantity=0,
        transition_kind=OrderTransitionKind.ORDER_CREATED,
        source_event_type="ORDER_CREATED",
        source_event_id="order-1:created",
        source_event_hash="1" * 64,
        occurred_at=_at(9),
        recorded_at=_at(9, 0, 1),
        provenance=provenance,
    )


def _next_transition(
    previous: OrderTransitionEvidence,
    *,
    to_status: OrderStatus,
    sequence: int,
    occurred_at: datetime,
) -> OrderTransitionEvidence:
    return OrderTransitionEvidence(
        order_id=previous.order_id,
        account_id=previous.account_id,
        order_payload=previous.order_payload,
        transition_sequence=sequence,
        from_status=previous.to_status,
        to_status=to_status,
        previous_filled_quantity=previous.next_filled_quantity,
        next_filled_quantity=previous.next_filled_quantity,
        transition_kind=OrderTransitionKind.STATUS_CHANGE,
        source_event_type="ORDER_STATUS_CHANGED",
        source_event_id=f"order-1:status:{sequence}",
        source_event_hash=f"{sequence + 10:064x}",
        occurred_at=occurred_at,
        recorded_at=occurred_at + timedelta(microseconds=1),
        provenance=previous.provenance,
        previous_transition_id=previous.transition_id,
        previous_transition_hash=previous.transition_hash,
    )


def test_v2_execution_evidence_migration_is_strictly_appended() -> None:
    assert len(MIGRATIONS) == 15
    assert MIGRATIONS[-5]["version"] == "20260803_011_v2_execution_evidence_bindings"
    assert {
        item["version"]: _checksum(tuple(item["statements"]))
        for item in MIGRATIONS
    } == LEGACY_MIGRATION_CHECKSUMS
    statements = tuple(MIGRATIONS[-5]["statements"])
    assert len(statements) == 5
    assert all(statement.lstrip().startswith("CREATE TABLE") for statement in statements)
    combined = "\n".join(statements).upper()
    for forbidden in ("ALTER TABLE", "DROP TABLE", "TRUNCATE", "DELETE FROM"):
        assert forbidden not in combined


def test_v2_execution_evidence_guard_migration_has_exact_trigger_surface() -> None:
    migration = MIGRATIONS[-4]
    assert migration["version"] == "20260803_012_v2_execution_evidence_guards"
    statements = tuple(migration["statements"])
    assert len(statements) == 30
    drops = tuple(
        statement.strip().split()[-1]
        for statement in statements
        if statement.lstrip().upper().startswith("DROP TRIGGER IF EXISTS")
    )
    creates = tuple(
        statement
        for statement in statements
        if statement.lstrip().upper().startswith("CREATE TRIGGER")
    )
    expected = {
        f"trg_{stem}_guard_{suffix}"
        for stem in (
            "market_calendar_evidence_v2",
            "quote_receipt_evidence_v2",
            "fill_execution_evidence_v2",
            "cash_event_binding_v2",
            "order_transition_v2",
        )
        for suffix in ("bi", "bu", "bd")
    }
    created_names = {
        statement.lstrip().split()[2]
        for statement in creates
    }
    assert len(drops) == 15
    assert len(creates) == 15
    assert set(drops) == expected
    assert created_names == expected

    target_tables = {
        "st_market_calendar_evidence_v2",
        "st_quote_receipt_evidence_v2",
        "st_fill_execution_evidence_v2",
        "st_cash_event_binding_v2",
        "st_order_transition_v2",
    }
    for table_name in target_tables:
        assert sum(
            f"BEFORE INSERT ON {table_name}" in statement
            for statement in creates
        ) == 1
        assert sum(
            f"BEFORE UPDATE ON {table_name}" in statement
            for statement in creates
        ) == 1
        assert sum(
            f"BEFORE DELETE ON {table_name}" in statement
            for statement in creates
        ) == 1
    for statement in creates:
        event_target = next(
            line.strip().split()[-1]
            for line in statement.splitlines()
            if line.strip().startswith("BEFORE ")
        )
        assert event_target in target_tables
        assert "SIGNAL SQLSTATE '45000'" in statement
        if "BEFORE UPDATE ON" in statement or "BEFORE DELETE ON" in statement:
            assert "\n                IF " not in statement
            assert "SELECT " not in statement.upper()


def test_v2_execution_evidence_natural_key_migration_is_forward_only() -> None:
    migration = MIGRATIONS[-3]
    assert migration["version"] == (
        "20260803_013_v2_execution_evidence_natural_keys"
    )
    statements = tuple(migration["statements"])
    assert len(statements) == 1
    normalized = " ".join(statements[0].split()).upper()
    assert normalized.startswith(
        "ALTER TABLE ST_MARKET_CALENDAR_EVIDENCE_V2 ADD UNIQUE KEY "
        "UK_CALENDAR_EVIDENCE_V2_NATURAL"
    )
    assert "(MARKET_CODE, TRADE_DATE, CALENDAR_VERSION)" in normalized
    assert _checksum(statements) == (
        "51addc459d4caae896ee656e901123646deb6a46584ac274092aa65026917eb8"
    )


def test_v2_execution_authority_migration_is_forward_only_and_append_only() -> None:
    migration = MIGRATIONS[-2]
    assert migration["version"] == (
        "20260803_014_v2_execution_authority_attestations"
    )
    statements = tuple(migration["statements"])
    assert len(statements) == 39
    combined = "\n".join(statements)
    assert "st_execution_authority_receipt_v2" in combined
    assert "st_execution_authority_attestation_v2" in combined
    assert "Ed25519" in combined  # Algorithm binding is stored in MySQL.
    assert "SIGNATURE_INVALID" not in combined  # Verification stays in Python.
    assert "authority receipt is append only" in combined
    assert "authority attestation is append only" in combined
    assert "uk_authority_receipt_v2_replay" in combined
    assert "uk_authority_attestation_v2_replay" in combined
    attestation_insert_guard = next(
        statement
        for statement in statements
        if "CREATE TRIGGER trg_execution_authority_attestation_v2_guard_bi"
        in statement
    )
    assert "JOIN st_execution_authority_trust_key_v2 k" in attestation_insert_guard
    assert "k.source_provider = r.source_provider" in attestation_insert_guard
    assert _checksum(statements) == (
        "984e2ea7c637c728745b9b21c3b508980cc046c1c434d9851619984918a3823d"
    )


def test_v2_execution_evidence_insert_guards_cover_legacy_mysql_checks() -> None:
    creates = tuple(
        statement
        for statement in MIGRATIONS[-4]["statements"]
        if "BEFORE INSERT ON" in statement
    )
    assert len(creates) == 5
    combined = "\n".join(creates)
    for required in (
        "JSON_VALID",
        "history_origin",
        "history_origin_id",
        "history_origin_at",
        "authority_status",
        "authority_receipt_hash",
        "REGEXP '[^0-9a-f]'",
        "BINARY LOWER(",
        "<> BINARY NEW.evidence_hash",
        "st_quote_event_v2",
        "st_fill_v2",
        "st_order_v2",
        "st_cash_ledger_v2",
        "previous_binding_id",
        "previous_transition_id",
    ):
        assert required in combined
    assert "order_fill_sequence < 1" in combined
    assert "quote_at > NEW.received_at" in combined
    assert "account_sequence < 0" in combined
    assert "next_filled_quantity < NEW.previous_filled_quantity" in combined
    assert "COMPLETE_FROM_DECLARED_ORIGIN" in combined
    by_target = {
        next(
            line.strip().split()[-1]
            for line in statement.splitlines()
            if line.strip().startswith("BEFORE INSERT ON")
        ): statement
        for statement in creates
    }
    for statement in by_target.values():
        for common in (
            "history_origin",
            "history_origin_id",
            "history_origin_at",
            "authority_status",
            "REGEXP '[^0-9a-f]'",
            "BINARY LOWER(",
            "JSON_VALID",
        ):
            assert common in statement
    assert "source_receipt_hash" in by_target["st_market_calendar_evidence_v2"]
    assert "st_qmt_realtime_sync_receipt_v2" in by_target[
        "st_quote_receipt_evidence_v2"
    ]
    assert "st_fee_profile_v2" in by_target["st_fill_execution_evidence_v2"]
    assert "st_instrument_rule_v2" in by_target[
        "st_fill_execution_evidence_v2"
    ]
    assert "previous_binding_id" in by_target["st_cash_event_binding_v2"]
    assert "previous_transition_id" in by_target["st_order_transition_v2"]


def test_v2_execution_evidence_guards_do_not_mutate_canonical_ledgers() -> None:
    combined = "\n".join(MIGRATIONS[-4]["statements"])
    upper = combined.upper()
    canonical_tables = (
        "ST_TRADE_ACCOUNT_V2",
        "ST_TRADE_INTENT_V2",
        "ST_ORDER_V2",
        "ST_FILL_V2",
        "ST_POSITION_LOT_V2",
        "ST_CASH_LEDGER_V2",
    )
    for canonical_table in canonical_tables:
        for verb in ("INSERT INTO", "UPDATE", "DELETE FROM", "REPLACE INTO"):
            assert f"{verb} {canonical_table}" not in upper
    canonical_dml = re.compile(
        r"\b(?:INSERT\s+INTO|REPLACE\s+INTO|UPDATE|DELETE\s+FROM)\s+`?(?:"
        + "|".join(table.lower() for table in canonical_tables)
        + r")`?\b",
        re.IGNORECASE,
    )
    assert canonical_dml.search(combined) is None

    # These protections intentionally remain deployment/privilege concerns:
    # DELETE triggers do not intercept TRUNCATE, SQL cannot reproduce the
    # Python namespaced canonical hashes, and grants depend on user@host.
    for unsupported in ("TRUNCATE", "GRANT ", "REVOKE ", "SHA2("):
        assert unsupported not in upper


def test_execution_evidence_schema_is_a_binding_layer_not_a_second_ledger() -> None:
    ddl = "\n".join(MIGRATIONS[-5]["statements"])
    expected_tables = {
        "st_market_calendar_evidence_v2",
        "st_quote_receipt_evidence_v2",
        "st_fill_execution_evidence_v2",
        "st_cash_event_binding_v2",
        "st_order_transition_v2",
    }
    assert {
        line.split("st_", 1)[1].split(" ", 1)[0].strip()
        for line in ddl.splitlines()
        if "CREATE TABLE IF NOT EXISTS st_" in line
    } == {name.removeprefix("st_") for name in expected_tables}
    for parent in (
        "st_quote_event_v2",
        "st_fill_v2",
        "st_order_v2",
        "st_cash_ledger_v2",
        "st_fee_profile_v2",
        "st_instrument_rule_v2",
    ):
        assert f"REFERENCES {parent}" in ddl
    assert "st_trade_account_v2 (" not in ddl
    assert "st_position_lot_v2 (" not in ddl
    assert "cash_balance" not in ddl
    assert "remaining_quantity" not in ddl


def test_execution_evidence_schema_matches_python_chain_and_payload_contracts() -> None:
    ddl = "\n".join(MIGRATIONS[-5]["statements"])
    for required in (
        "fill_payload_json LONGTEXT NOT NULL",
        "order_payload_json LONGTEXT NOT NULL",
        "fee_effective_to DATE DEFAULT NULL",
        "fee_created_at DATETIME NOT NULL",
        "instrument_rule_effective_to DATE DEFAULT NULL",
        "instrument_rule_created_at DATETIME NOT NULL",
        "cash_event_type VARCHAR(40) NOT NULL",
        "cash_event_payload_json LONGTEXT NOT NULL",
        "transition_kind VARCHAR(40) NOT NULL",
        "fill_execution_evidence_id CHAR(64) DEFAULT NULL",
        "previous_binding_id, previous_cash_event_id",
        "previous_transition_id, previous_transition_hash",
        "JSON_VALID(matcher_response_json)",
        "'ORDER_CREATED', 'STATUS_CHANGE', 'FILL_APPLIED'",
    ):
        assert required in ddl
    assert "DEFAULT 'COMPLETE_FROM_DECLARED_ORIGIN'" not in ddl
    assert "DEFAULT 'EXTERNAL_RECEIPT_VERIFIED'" not in ddl


def test_evidence_hashes_are_deterministic_and_validators_reconstruct() -> None:
    first_calendar = _calendar()
    second_calendar = _calendar()
    first_quote = _quote()
    first_fill = _fill()
    assert first_calendar.evidence_hash == second_calendar.evidence_hash
    assert first_calendar.calendar_evidence_id == first_calendar.evidence_hash
    assert first_quote.quote_evidence_id == first_quote.evidence_hash
    assert first_fill.fill_execution_evidence_id == first_fill.evidence_hash
    assert first_fill.upstream_market_authority_is_verified is True
    validate_market_calendar_evidence(first_calendar)
    validate_quote_receipt_evidence(first_quote)
    validate_fill_execution_evidence(first_fill)


def test_canonical_json_rejects_duplicate_ambiguous_or_noncanonical_values() -> None:
    assert CanonicalJson.from_value({"b": 2, "a": 1}).json_text == '{"a":1,"b":2}'
    for raw in (
        '{"a": 1}',
        ' {"a":1}',
        '{"a":1,"a":1}',
        '{"value":NaN}',
        '{"value":1.0}',
        '{"value":9223372036854775808}',
    ):
        with pytest.raises(ValueError):
            CanonicalJson(raw)
    with pytest.raises(TypeError, match="keys must be exactly str"):
        CanonicalJson.from_value({1: "numeric-key"})
    with pytest.raises(TypeError, match="forbids binary floats"):
        CanonicalJson.from_value({"amount": 1.0})


def test_history_origin_and_authority_are_orthogonal_and_fact_time_is_enforced() -> None:
    complete_content_only = _provenance(
        history=HistoryOrigin.COMPLETE_FROM_DECLARED_ORIGIN
    )
    assert complete_content_only.history_is_complete is True
    assert complete_content_only.source_authority_is_verified is False
    with pytest.raises(ValueError, match="cannot declare an origin"):
        EvidenceProvenance(
            history_origin=HistoryOrigin.UNKNOWN,
            history_origin_id="fake",
            history_origin_at=_at(8),
            authority_status=AuthorityStatus.UNKNOWN,
        )
    with pytest.raises(ValueError, match="requires an id and timestamp"):
        EvidenceProvenance(
            history_origin=HistoryOrigin.START_AFTER_UNKNOWN,
            authority_status=AuthorityStatus.CONTENT_HASH_ONLY,
        )
    late_origin = _provenance(origin_at=_at(10, 0, 1))
    quote = _quote()
    with pytest.raises(ValueError, match="predates"):
        replace(quote, provenance=late_origin)
    fill = _fill()
    with pytest.raises(ValueError, match="predates"):
        replace(fill, provenance=_provenance(origin_at=_at(10, 0, 1)))


def test_fill_rejects_post_execution_calendar_fee_and_rule_evidence() -> None:
    with pytest.raises(ValueError, match="both be available"):
        _fill(calendar_evidence=_calendar(available_at=_at(10, 0, 1)))
    with pytest.raises(ValueError, match="visible before execution"):
        _fill(fee_created_at=_at(10, 0, 1))
    with pytest.raises(ValueError, match="visible before execution"):
        _fill(instrument_rule_created_at=_at(10, 0, 1))
    with pytest.raises(ValueError, match="not effective"):
        _fill(fee_effective_to=date(2026, 8, 2))
    with pytest.raises(ValueError, match="not effective"):
        _fill(instrument_rule_effective_from=date(2026, 8, 4))


def test_fill_rejects_cross_object_hash_or_value_substitution() -> None:
    original = _fill()
    bad_match = CanonicalJson.from_value(
        {
            **original.matcher_response.value(),
            "fill_quantity": 99,
        }
    )
    with pytest.raises(ValueError, match="fill_quantity"):
        replace(original, matcher_response=bad_match)

    bad_accounting = CanonicalJson.from_value(
        {
            **original.accounting_request.value(),
            "matcher_output_hash": "0" * 64,
        }
    )
    with pytest.raises(ValueError, match="matcher_output_hash"):
        replace(original, accounting_request=bad_accounting)

    bad_settlement = CanonicalJson.from_value(
        {
            **original.settlement_evidence.value(),
            "settlement_date": "2026-08-05",
        }
    )
    with pytest.raises(ValueError, match="settlement_date"):
        replace(original, settlement_evidence=bad_settlement)

    order = original.order_payload.value()
    with pytest.raises(ValueError, match="outside the immutable order window"):
        replace(
            original,
            order_payload=CanonicalJson.from_value(
                {**order, "expires_at": original.executed_at}
            ),
        )


def test_low_level_frozen_hash_tampering_is_rejected() -> None:
    calendar = _calendar()
    object.__setattr__(calendar, "evidence_hash", "0" * 64)
    with pytest.raises(ExecutionEvidenceInvariantError):
        validate_market_calendar_evidence(calendar)
    fill = _fill()
    object.__setattr__(fill.quote_evidence, "evidence_hash", "0" * 64)
    with pytest.raises(ExecutionEvidenceInvariantError):
        validate_fill_execution_evidence(fill)


def test_cash_complete_chain_requires_real_genesis_and_exact_fill_binding() -> None:
    complete = _provenance(history=HistoryOrigin.COMPLETE_FROM_DECLARED_ORIGIN)
    genesis_time = _at(8)
    genesis = CashEventBinding(
        cash_event_id="cash-0",
        account_id="paper-main-v2",
        account_sequence=0,
        cash_event_type="INITIAL_DEPOSIT",
        cash_event_payload=_cash_payload(
            cash_event_id="cash-0",
            event_type="INITIAL_DEPOSIT",
            occurred_at=genesis_time,
        ),
        occurred_at=genesis_time,
        bound_at=genesis_time + timedelta(seconds=1),
        provenance=complete,
    )
    fill = _fill()
    fill_idempotency = fill.fill_payload.value()["idempotency_key"]
    cash_fill = CashEventBinding(
        cash_event_id="cash-1",
        account_id="paper-main-v2",
        account_sequence=1,
        cash_event_type="BUY_FILL",
        cash_event_payload=_cash_payload(
            cash_event_id="cash-1",
            event_type="BUY_FILL",
            occurred_at=fill.executed_at,
            related_order_id=fill.order_id,
            related_fill_id=fill.fill_id,
            business_event_key=f"FILL:{fill_idempotency}",
        ),
        occurred_at=fill.executed_at,
        bound_at=fill.bound_at + timedelta(seconds=1),
        provenance=complete,
        related_order_id=fill.order_id,
        related_fill_id=fill.fill_id,
        fill_execution_evidence=fill,
        previous_cash_event_id=genesis.cash_event_id,
        previous_binding_id=genesis.cash_binding_id,
        previous_binding_hash=genesis.binding_hash,
    )
    assert genesis.history_is_complete is True
    assert cash_fill.history_is_complete is False
    assert validate_cash_event_binding_chain((genesis, cash_fill)) is True
    validate_cash_event_binding(cash_fill)
    with pytest.raises(ValueError, match="provided together"):
        replace(cash_fill, previous_binding_hash=None)
    with pytest.raises(ValueError, match="business key"):
        replace(
            cash_fill,
            cash_event_payload=_cash_payload(
                cash_event_id="cash-1",
                event_type="BUY_FILL",
                occurred_at=fill.executed_at,
                related_order_id=fill.order_id,
                related_fill_id=fill.fill_id,
                business_event_key="FILL:wrong",
            ),
        )


def test_cash_cannot_mark_arbitrary_sequence_zero_as_complete_genesis() -> None:
    complete = _provenance(history=HistoryOrigin.COMPLETE_FROM_DECLARED_ORIGIN)
    fill = _fill()
    with pytest.raises(ValueError, match="INITIAL_DEPOSIT"):
        CashEventBinding(
            cash_event_id="cash-fake",
            account_id="paper-main-v2",
            account_sequence=0,
            cash_event_type="BUY_FILL",
            cash_event_payload=_cash_payload(
                cash_event_id="cash-fake",
                event_type="BUY_FILL",
                occurred_at=fill.executed_at,
                related_order_id=fill.order_id,
                related_fill_id=fill.fill_id,
                business_event_key="FILL:" + fill.fill_payload.value()["idempotency_key"],
            ),
            occurred_at=fill.executed_at,
            bound_at=fill.bound_at + timedelta(seconds=1),
            provenance=complete,
            related_order_id=fill.order_id,
            related_fill_id=fill.fill_id,
            fill_execution_evidence=fill,
        )


def test_order_complete_chain_requires_created_genesis_and_binds_fill() -> None:
    complete = _provenance(history=HistoryOrigin.COMPLETE_FROM_DECLARED_ORIGIN)
    genesis = _order_genesis(complete)
    approved = _next_transition(
        genesis,
        to_status=OrderStatus.RISK_APPROVED,
        sequence=1,
        occurred_at=_at(9, 1),
    )
    queued = _next_transition(
        approved,
        to_status=OrderStatus.QUEUED,
        sequence=2,
        occurred_at=_at(9, 2),
    )
    fill = _fill()
    filled = OrderTransitionEvidence(
        order_id="order-1",
        account_id="paper-main-v2",
        order_payload=genesis.order_payload,
        transition_sequence=3,
        from_status=OrderStatus.QUEUED,
        to_status=OrderStatus.FILLED,
        previous_filled_quantity=0,
        next_filled_quantity=100,
        transition_kind=OrderTransitionKind.FILL_APPLIED,
        related_fill_id=fill.fill_id,
        fill_execution_evidence=fill,
        source_event_type="MATCH_APPLIED",
        source_event_id="match-batch-1:order-1:1",
        source_event_hash="f" * 64,
        occurred_at=fill.executed_at,
        recorded_at=fill.bound_at,
        provenance=complete,
        previous_transition_id=queued.transition_id,
        previous_transition_hash=queued.transition_hash,
    )
    assert filled.history_is_complete is False
    assert validate_order_transition_chain((genesis, approved, queued, filled)) is True
    validate_order_transition_evidence(filled)
    with pytest.raises(ValueError, match="ORDER_CREATED genesis"):
        OrderTransitionEvidence(
            order_id="order-1",
            account_id="paper-main-v2",
            order_payload=genesis.order_payload,
            transition_sequence=0,
            from_status=OrderStatus.CREATED,
            to_status=OrderStatus.RISK_APPROVED,
            previous_filled_quantity=0,
            next_filled_quantity=0,
            transition_kind=OrderTransitionKind.STATUS_CHANGE,
            source_event_type="RISK_APPROVED",
            source_event_id="order-1:risk",
            source_event_hash="2" * 64,
            occurred_at=_at(9),
            recorded_at=_at(9, 0, 1),
            provenance=complete,
        )


def test_order_chain_validation_is_iterative_for_long_histories() -> None:
    complete = _provenance(history=HistoryOrigin.COMPLETE_FROM_DECLARED_ORIGIN)
    transitions = [_order_genesis(complete)]
    transitions.append(
        _next_transition(
            transitions[-1],
            to_status=OrderStatus.RISK_APPROVED,
            sequence=1,
            occurred_at=_at(9, 1),
        )
    )
    transitions.append(
        _next_transition(
            transitions[-1],
            to_status=OrderStatus.QUEUED,
            sequence=2,
            occurred_at=_at(9, 2),
        )
    )
    base = _at(9, 3)
    for sequence in range(3, 1203):
        previous = transitions[-1]
        occurred_at = base + timedelta(microseconds=sequence)
        transitions.append(
            OrderTransitionEvidence(
                order_id=previous.order_id,
                account_id=previous.account_id,
                order_payload=previous.order_payload,
                transition_sequence=sequence,
                from_status=OrderStatus.QUEUED,
                to_status=OrderStatus.QUEUED,
                previous_filled_quantity=0,
                next_filled_quantity=0,
                transition_kind=OrderTransitionKind.WAITING_REASON_CHANGED,
                waiting_reason="WAIT_LIQUIDITY",
                source_event_type="WAITING_REASON_CHANGED",
                source_event_id=f"order-1:wait:{sequence}",
                source_event_hash=f"{sequence:064x}",
                occurred_at=occurred_at,
                recorded_at=occurred_at + timedelta(microseconds=1),
                provenance=complete,
                previous_transition_id=previous.transition_id,
                previous_transition_hash=previous.transition_hash,
            )
        )
    assert validate_order_transition_chain(tuple(transitions)) is True
