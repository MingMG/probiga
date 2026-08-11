from __future__ import annotations

import copy
import inspect
import re
from dataclasses import FrozenInstanceError
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

from server.integrations.v2_canonical_read import (
    V2CanonicalReadError,
    V2CanonicalSnapshotInvariantError,
    V2CapabilityStatus,
    V2ContentRootSemantics,
    read_canonical_v2_snapshot,
    validate_v2_canonical_read_result,
    validate_v2_canonical_snapshot,
)
from server.integrations.v2_canonical_read import facade


SHANGHAI = ZoneInfo("Asia/Shanghai")
KNOWLEDGE_AT = datetime(2026, 8, 3, 16, 0, tzinfo=SHANGHAI)
ACCOUNT_ID = "PAPER-A"


class _Mappings:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return _Mappings(self._rows)


def _dt(day: int, hour: int = 0, minute: int = 0, second: int = 0) -> datetime:
    # Canonical V2 DATETIME values are legacy naive Asia/Shanghai wall time.
    return datetime(2026, 8, day, hour, minute, second)


def _rows_fixture() -> dict[str, list[dict[str, object]]]:
    return {
        "st_trade_account_v2": [
            {
                "account_id": ACCOUNT_ID,
                "account_name": "paper",
                "status": "ACTIVE",
                "initial_cash": Decimal("200000.00"),
                "cash_balance": Decimal("198995.00"),
                "peak_equity": Decimal("200000.00"),
                "policy_version": "policy-v1",
                "policy_hash": "a" * 64,
                "fee_profile_version": "fee-v1",
                "instrument_rule_version": "rule-v1",
                "real_trading_enabled": 0,
                "created_at": _dt(1, 8),
                "updated_at": _dt(3, 10, 0, 2),
            }
        ],
        "st_trade_intent_v2": [
            {
                "intent_id": "intent-1",
                "account_id": ACCOUNT_ID,
                "decision_run_uid": "run-1",
                "strategy_version": "strategy-v1",
                "stock_code": "600000",
                "theme_code": "bank",
                "action": "OPEN",
                "current_quantity": 0,
                "target_quantity": 100,
                "target_weight": Decimal("0.10"),
                "earliest_at": _dt(3, 9, 30),
                "expires_at": _dt(3, 15),
                "limit_price": Decimal("10.00"),
                "worst_price": Decimal("10.10"),
                "initial_stop": Decimal("9.00"),
                "protective_stop": Decimal("9.20"),
                "invalidation_condition": "close below stop",
                "reason_code": "TEST",
                "evidence_json": '{"b":2,"a":1}',
                "intent_version": 1,
                "idempotency_key": "intent-key-1",
                "created_at": _dt(3, 9),
            }
        ],
        "st_order_v2": [
            {
                "order_id": "order-1",
                "account_id": ACCOUNT_ID,
                "intent_id": "intent-1",
                "stock_code": "600000",
                "side": "BUY",
                "order_type": "LIMIT",
                "limit_price": Decimal("10.00"),
                "quantity": 100,
                "filled_quantity": 100,
                "status": "FILLED",
                "waiting_reason": None,
                "earliest_at": _dt(3, 9, 30),
                "expires_at": _dt(3, 15),
                "idempotency_key": "order-key-1",
                "created_at": _dt(3, 9, 1),
                "updated_at": _dt(3, 10, 0, 2),
            }
        ],
        "st_fill_v2": [
            {
                "fill_id": "fill-1",
                "order_id": "order-1",
                "account_id": ACCOUNT_ID,
                "stock_code": "600000",
                "side": "BUY",
                "quantity": 100,
                "price": Decimal("10.00"),
                "gross_amount": Decimal("1000.00"),
                "fee_amount": Decimal("5.00"),
                "net_cash_amount": Decimal("-1005.00"),
                "quote_event_id": "quote-1",
                "match_event_id": "match-1",
                "idempotency_key": "fill-key-1",
                "filled_at": _dt(3, 10),
                "created_at": _dt(3, 10, 0, 1),
            }
        ],
        "st_position_lot_v2": [
            {
                "lot_id": "LOT:fill-1",
                "account_id": ACCOUNT_ID,
                "stock_code": "600000",
                "theme_code": "bank",
                "strategy_version": "strategy-v1",
                "opened_fill_id": "fill-1",
                "opened_trade_date": date(2026, 8, 3),
                "settlement_date": date(2026, 8, 4),
                "original_quantity": 100,
                "remaining_quantity": 100,
                "cost_price": Decimal("10.00"),
                "allocated_buy_fee": Decimal("5.00"),
                "position_state": "OPENING",
                "approved_target_quantity": 200,
                "add_count": 0,
                "initial_stop": Decimal("9.00"),
                "protective_stop": Decimal("9.20"),
                "invalidation_condition": "close below stop",
                "version": 1,
                "created_at": _dt(3, 10, 0, 1),
                "closed_at": None,
            }
        ],
        "st_cash_ledger_v2": [
            {
                "cash_event_id": "cash-initial",
                "account_id": ACCOUNT_ID,
                "business_event_key": f"{ACCOUNT_ID}:INITIAL_DEPOSIT",
                "event_type": "INITIAL_DEPOSIT",
                "amount": Decimal("200000.00"),
                "balance_after": Decimal("200000.00"),
                "related_order_id": None,
                "related_fill_id": None,
                "reversal_of": None,
                "occurred_at": _dt(1, 8),
                "created_at": _dt(1, 8),
            },
            {
                "cash_event_id": "cash-fill-1",
                "account_id": ACCOUNT_ID,
                "business_event_key": "FILL:fill-key-1",
                "event_type": "BUY_FILL",
                "amount": Decimal("-1005.00"),
                "balance_after": Decimal("198995.00"),
                "related_order_id": "order-1",
                "related_fill_id": "fill-1",
                "reversal_of": None,
                "occurred_at": _dt(3, 10),
                "created_at": _dt(3, 10, 0, 1),
            },
        ],
        "st_instrument_rule_v2": [
            {
                "stock_code": "600000",
                "rule_version": "rule-v1",
                "effective_from": date(2026, 8, 1),
                "effective_to": None,
                "security_type": "STOCK",
                "exchange_code": "SH",
                "can_buy": 1,
                "first_buy_minimum": 100,
                "buy_lot_size": 100,
                "sell_lot_size": 1,
                "settlement_days": 1,
                "tick_size": Decimal("0.01"),
                "limit_ratio": Decimal("0.10"),
                "special_treatment": 0,
                "suspended": 0,
                "permission_required": "",
                "permission_confirmed": 1,
                "fee_profile_version": "fee-v1",
                "source_snapshot_hash": "b" * 64,
                "created_at": _dt(1, 7),
            }
        ],
        "st_fee_profile_v2": [
            {
                "fee_profile_version": "fee-v1",
                "effective_from": date(2026, 8, 1),
                "effective_to": None,
                "security_type": "STOCK",
                "buy_commission_rate": Decimal("0.0003"),
                "sell_commission_rate": Decimal("0.0003"),
                "minimum_commission": Decimal("5.00"),
                "stamp_tax_sell_rate": Decimal("0.0005"),
                "transfer_fee_buy_rate": Decimal("0"),
                "transfer_fee_sell_rate": Decimal("0"),
                "other_fee_json": "{}",
                "evidence_hash": "c" * 64,
                "confirmation_status": "PAPER_ASSUMPTION",
                "created_at": _dt(1, 7),
            }
        ],
        "si_trade_calendar": [
            {
                "calendar_year": 2026,
                "trade_date": date(2026, 8, 3),
                "trade_status": 1,
                "day_week": 1,
                "etl_sync_at": _dt(1, 6),
            },
            {
                "calendar_year": 2026,
                "trade_date": date(2026, 8, 4),
                "trade_status": 1,
                "day_week": 2,
                "etl_sync_at": _dt(1, 6),
            },
        ],
        "st_qmt_minute_sync_receipt_v2": [],
        "st_public_quote_receipt_v2": [
            {
                "batch_id": "batch-1",
                "trade_date": date(2026, 8, 3),
                "quote_at": _dt(3, 9, 59),
                "received_at": _dt(3, 9, 59, 1),
                "expected_count": 1,
                "observed_count": 1,
                "coverage": Decimal("1"),
                "provider_count": 1,
                "minimum_sources_per_symbol": 1,
                "agreement_ratio": Decimal("1"),
                "source_provider": "test",
                "maximum_price_deviation_pct": Decimal("0"),
                "maximum_source_latency_seconds": Decimal("1"),
                "quality_status": "PASS",
                "provider_status_json": "{}",
                "evidence_json": "{}",
                "created_at": _dt(3, 9, 59, 2),
            }
        ],
        "st_qmt_realtime_sync_receipt_v2": [],
    }


def _connection(
    rows: dict[str, list[dict[str, object]]],
    *,
    active: bool = True,
    isolation_level: str = "REPEATABLE READ",
    fail_table: str | None = None,
):
    connection = MagicMock(spec=Connection)
    connection.in_transaction.return_value = active
    connection.get_isolation_level.return_value = isolation_level
    statements: list[str] = []

    def execute(statement, parameters):
        sql = str(statement)
        statements.append(sql)
        match = re.search(r"\bFROM\s+([A-Za-z0-9_]+)", sql, flags=re.IGNORECASE)
        assert match is not None
        table = match.group(1)
        if table == fail_table:
            raise SQLAlchemyError("simulated unavailable schema")
        return _Result(copy.deepcopy(rows[table]))

    connection.execute.side_effect = execute
    return connection, statements


def _read(rows=None):
    connection, statements = _connection(rows or _rows_fixture())
    result = read_canonical_v2_snapshot(
        connection,
        account_id=ACCOUNT_ID,
        knowledge_at=KNOWLEDGE_AT,
    )
    return result, connection, statements


def test_snapshot_is_frozen_canonical_and_reports_honest_replay_blocks():
    result, connection, statements = _read()

    assert (
        result.capability_status
        is V2CapabilityStatus.AUTHORITATIVE_REPLAY_BLOCKED
    )
    assert result.snapshot is not None
    assert {
        "FILL_FEE_SCHEDULE_BINDING_NOT_PERSISTED",
        "FILL_INSTRUMENT_RULE_BINDING_NOT_PERSISTED",
        "FILL_QUOTE_EVENT_SOURCE_BATCH_UNAVAILABLE",
        "FILL_QUOTE_RECEIPT_BINDING_NOT_PERSISTED",
        "TRADE_CALENDAR_SESSION_AUTHORITY_NOT_PERSISTED",
        "TRADE_CALENDAR_VERSION_NOT_PERSISTED",
        "ORDER_TRANSITION_HISTORY_NOT_PERSISTED",
    } <= set(result.blocker_codes)
    assert all(item.missing_bindings and item.reason for item in result.blockers)
    snapshot = result.snapshot
    assert snapshot.root_semantics is V2ContentRootSemantics.TRANSACTION_CONTENT_ONLY
    assert snapshot.source_authority_verified is False
    assert snapshot.transaction_content_root_hash == snapshot.row_manifest.root_hash
    assert len(snapshot.transaction_content_root_hash) == 64
    assert snapshot.intents[0].evidence_json == '{"a":1,"b":2}'
    assert snapshot.account.created_at.tzinfo is not None
    with pytest.raises(FrozenInstanceError):
        snapshot.account = snapshot.account  # type: ignore[misc]

    assert len(statements) == 12
    assert all(item.lstrip().upper().startswith("SELECT ") for item in statements)
    connection.commit.assert_not_called()
    connection.rollback.assert_not_called()


def test_canonical_order_and_root_do_not_depend_on_driver_row_order():
    first_rows = _rows_fixture()
    second_rows = copy.deepcopy(first_rows)
    for values in second_rows.values():
        values.reverse()

    first, _, _ = _read(first_rows)
    second, _, _ = _read(second_rows)

    assert first.snapshot is not None and second.snapshot is not None
    assert first.snapshot.cash_ledger[0].event_type == "INITIAL_DEPOSIT"
    assert first.snapshot.transaction_content_root_hash == second.snapshot.transaction_content_root_hash
    assert first.snapshot.row_manifest.entries == second.snapshot.row_manifest.entries


def test_empty_account_materialized_snapshot_can_be_ready_without_claiming_authority():
    rows = _rows_fixture()
    rows["st_trade_intent_v2"] = []
    rows["st_order_v2"] = []
    rows["st_fill_v2"] = []
    rows["st_position_lot_v2"] = []
    rows["st_cash_ledger_v2"] = [rows["st_cash_ledger_v2"][0]]
    rows["st_trade_account_v2"][0]["cash_balance"] = Decimal("200000.00")
    rows["st_instrument_rule_v2"] = []

    result, _, _ = _read(rows)

    assert (
        result.capability_status
        is V2CapabilityStatus.MATERIALIZED_SNAPSHOT_READY
    )
    assert result.blockers == ()
    assert result.snapshot is not None
    assert result.snapshot.source_authority_verified is False
    assert result.capability_status.value == "CURRENT_MATERIALIZED_SNAPSHOT_READY"


def test_legacy_calendar_integral_decimals_are_normalized_but_strings_fail():
    rows = _rows_fixture()
    rows["si_trade_calendar"][0]["calendar_year"] = Decimal("2026.000000")
    rows["si_trade_calendar"][0]["trade_status"] = Decimal("1.000000")
    rows["si_trade_calendar"][0]["day_week"] = None
    result, _, _ = _read(rows)
    assert result.snapshot is not None
    assert result.snapshot.trade_calendar[0].calendar_year == 2026
    assert result.snapshot.trade_calendar[0].trade_status == 1
    assert result.snapshot.trade_calendar[0].day_week is None

    rows["si_trade_calendar"][0]["trade_status"] = "1"
    with pytest.raises(V2CanonicalSnapshotInvariantError, match="integral Decimal"):
        _read(rows)


def test_requires_exact_sqlalchemy_connection_and_caller_owned_transaction():
    with pytest.raises(TypeError, match="SQLAlchemy Connection"):
        read_canonical_v2_snapshot(  # type: ignore[arg-type]
            object(), account_id=ACCOUNT_ID, knowledge_at=KNOWLEDGE_AT
        )

    connection, _ = _connection(_rows_fixture(), active=False)
    with pytest.raises(V2CanonicalReadError, match="active transaction"):
        read_canonical_v2_snapshot(
            connection, account_id=ACCOUNT_ID, knowledge_at=KNOWLEDGE_AT
        )
    connection.execute.assert_not_called()


def test_requires_consistent_snapshot_transaction_isolation_before_selects():
    connection, _ = _connection(
        _rows_fixture(), isolation_level="READ COMMITTED"
    )
    with pytest.raises(V2CanonicalReadError, match="REPEATABLE READ"):
        read_canonical_v2_snapshot(
            connection, account_id=ACCOUNT_ID, knowledge_at=KNOWLEDGE_AT
        )
    connection.execute.assert_not_called()

    serializable, _ = _connection(
        _rows_fixture(), isolation_level="serializable"
    )
    result = read_canonical_v2_snapshot(
        serializable, account_id=ACCOUNT_ID, knowledge_at=KNOWLEDGE_AT
    )
    assert result.snapshot is not None


def test_schema_or_receipt_select_failure_returns_blocked_without_partial_snapshot():
    rows = _rows_fixture()
    del rows["si_trade_calendar"][0]["etl_sync_at"]
    missing_column, _, _ = _read(rows)
    assert (
        missing_column.capability_status
        is V2CapabilityStatus.SNAPSHOT_READ_BLOCKED
    )
    assert missing_column.snapshot is None
    assert missing_column.blocker_codes == ("SCHEMA_READ_BLOCKED",)
    assert missing_column.blockers[0].missing_bindings == ("si_trade_calendar",)

    connection, _ = _connection(
        _rows_fixture(), fail_table="st_public_quote_receipt_v2"
    )
    missing_receipt = read_canonical_v2_snapshot(
        connection, account_id=ACCOUNT_ID, knowledge_at=KNOWLEDGE_AT
    )
    assert (
        missing_receipt.capability_status
        is V2CapabilityStatus.SNAPSHOT_READ_BLOCKED
    )
    assert missing_receipt.snapshot is None
    assert missing_receipt.blockers[0].missing_bindings == (
        "st_public_quote_receipt_v2",
    )

    object.__setattr__(missing_receipt.blockers[0], "reason", "forged")
    with pytest.raises(
        V2CanonicalSnapshotInvariantError,
        match="not a canonical schema failure",
    ):
        validate_v2_canonical_read_result(missing_receipt)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda rows: rows["st_fill_v2"][0].__setitem__(
                "account_id", "ANOTHER-ACCOUNT"
            ),
            "another account",
        ),
        (
            lambda rows: rows["st_fill_v2"][0].__setitem__(
                "order_id", "missing-order"
            ),
            "missing order",
        ),
        (
            lambda rows: rows["st_cash_ledger_v2"][1].__setitem__(
                "balance_after", Decimal("1.00")
            ),
            "cash balance chain",
        ),
        (
            lambda rows: rows["st_position_lot_v2"][0].__setitem__(
                "remaining_quantity", 99
            ),
            "FIFO fill replay",
        ),
        (
            lambda rows: rows["st_fill_v2"][0].__setitem__(
                "created_at", _dt(4, 10)
            ),
            "after knowledge_at",
        ),
        (
            lambda rows: rows["st_trade_account_v2"][0].__setitem__(
                "cash_balance", "198995.00"
            ),
            "finite Decimal",
        ),
    ],
)
def test_cross_table_binding_accounting_future_and_types_fail_closed(mutate, message):
    rows = _rows_fixture()
    mutate(rows)
    with pytest.raises(V2CanonicalSnapshotInvariantError, match=message):
        _read(rows)


def test_duplicate_primary_or_idempotency_identity_fails_closed():
    rows = _rows_fixture()
    rows["st_fill_v2"].append(copy.deepcopy(rows["st_fill_v2"][0]))
    with pytest.raises(V2CanonicalSnapshotInvariantError, match="duplicate fill_id"):
        _read(rows)


def test_future_visible_receipt_fails_instead_of_being_silently_attested():
    rows = _rows_fixture()
    rows["st_public_quote_receipt_v2"][0]["received_at"] = _dt(4, 9)
    rows["st_public_quote_receipt_v2"][0]["created_at"] = _dt(4, 9, 0, 1)
    with pytest.raises(V2CanonicalSnapshotInvariantError, match="after knowledge_at"):
        _read(rows)


def test_equal_time_cash_events_do_not_use_hash_id_as_a_fake_sequence():
    rows = _rows_fixture()
    rows["st_cash_ledger_v2"].append(
        {
            "cash_event_id": "aaa-adjustment-sorts-before-fill",
            "account_id": ACCOUNT_ID,
            "business_event_key": "TEST:ADJUSTMENT",
            "event_type": "TEST_ADJUSTMENT",
            "amount": Decimal("1.00"),
            "balance_after": Decimal("198996.00"),
            "related_order_id": None,
            "related_fill_id": None,
            "reversal_of": None,
            "occurred_at": _dt(3, 10),
            "created_at": _dt(3, 10, 0, 2),
        }
    )
    rows["st_trade_account_v2"][0]["cash_balance"] = Decimal("198996.00")
    rows["st_trade_account_v2"][0]["updated_at"] = _dt(3, 10, 0, 3)

    result, _, _ = _read(rows)

    assert result.snapshot is not None
    assert "CASH_LEDGER_SEQUENCE_NOT_PERSISTED" in result.blocker_codes
    assert "UNSUPPORTED_CASH_EVENT_TYPE" in result.blocker_codes


def test_equal_time_cash_requires_every_materialized_balance_to_form_a_chain():
    rows = _rows_fixture()
    rows["st_cash_ledger_v2"].append(
        {
            "cash_event_id": "cash-adjustment",
            "account_id": ACCOUNT_ID,
            "business_event_key": "TEST:ADJUSTMENT",
            "event_type": "TEST_ADJUSTMENT",
            "amount": Decimal("1.00"),
            "balance_after": Decimal("198996.00"),
            "related_order_id": None,
            "related_fill_id": None,
            "reversal_of": None,
            "occurred_at": _dt(3, 10),
            "created_at": _dt(3, 10, 0, 2),
        }
    )
    rows["st_trade_account_v2"][0]["cash_balance"] = Decimal("198996.00")
    # The group terminal balance is still present, but the fill row's
    # materialized balance cannot follow or precede any other group row.
    rows["st_cash_ledger_v2"][1]["balance_after"] = Decimal("42.00")
    with pytest.raises(
        V2CanonicalSnapshotInvariantError,
        match="no valid balance chain",
    ):
        _read(rows)


def test_account_and_position_state_values_are_closed_over_real_v2_writers():
    rows = _rows_fixture()
    rows["st_trade_account_v2"][0]["status"] = "MADE_UP"
    with pytest.raises(V2CanonicalSnapshotInvariantError, match="account status"):
        _read(rows)

    rows = _rows_fixture()
    rows["st_position_lot_v2"][0]["position_state"] = "MADE_UP"
    with pytest.raises(V2CanonicalSnapshotInvariantError, match="position_state"):
        _read(rows)


def test_legal_trailing_stop_evolution_remains_materialized_but_replay_blocked():
    rows = _rows_fixture()
    rows["st_position_lot_v2"][0]["protective_stop"] = Decimal("9.50")
    rows["st_position_lot_v2"][0]["position_state"] = "VALID_STRONG"
    rows["st_position_lot_v2"][0]["version"] = 2

    result, _, _ = _read(rows)

    assert result.snapshot is not None
    assert result.snapshot.lots[0].protective_stop == Decimal("9.50")
    assert result.capability_status is V2CapabilityStatus.AUTHORITATIVE_REPLAY_BLOCKED
    assert "LOT_STATE_HISTORY_NOT_PERSISTED" in result.blocker_codes


def test_legal_non_executable_intent_action_may_exist_without_an_order():
    rows = _rows_fixture()
    rows["st_trade_intent_v2"][0]["action"] = "HOLD"
    rows["st_trade_intent_v2"][0]["current_quantity"] = 100
    rows["st_trade_intent_v2"][0]["target_quantity"] = 100
    rows["st_order_v2"] = []
    rows["st_fill_v2"] = []
    rows["st_position_lot_v2"] = []
    rows["st_cash_ledger_v2"] = [rows["st_cash_ledger_v2"][0]]
    rows["st_trade_account_v2"][0]["cash_balance"] = Decimal("200000.00")

    result, _, statements = _read(rows)

    assert result.snapshot is not None
    assert result.snapshot.intents[0].action == "HOLD"
    assert not any("st_public_quote_receipt_v2" in item for item in statements)


@pytest.mark.parametrize(
    "status", ["APPROVED", "SUBMITTED", "WAITING", "MADE_UP"]
)
def test_order_status_rejects_values_outside_the_v2_oms(status):
    rows = _rows_fixture()
    rows["st_order_v2"][0]["status"] = status
    with pytest.raises(V2CanonicalSnapshotInvariantError, match="order status"):
        _read(rows)


@pytest.mark.parametrize("status", ["QUEUED", "RISK_APPROVED", "REJECTED"])
def test_unfilled_order_status_cannot_carry_partial_fills(status):
    rows = _rows_fixture()
    rows["st_order_v2"][0]["status"] = status
    rows["st_order_v2"][0]["filled_quantity"] = 50
    rows["st_fill_v2"][0]["quantity"] = 50
    rows["st_fill_v2"][0]["gross_amount"] = Decimal("500.00")
    rows["st_fill_v2"][0]["net_cash_amount"] = Decimal("-505.00")
    rows["st_position_lot_v2"][0]["original_quantity"] = 50
    rows["st_position_lot_v2"][0]["remaining_quantity"] = 50
    rows["st_cash_ledger_v2"][1]["amount"] = Decimal("-505.00")
    rows["st_cash_ledger_v2"][1]["balance_after"] = Decimal("199495.00")
    rows["st_trade_account_v2"][0]["cash_balance"] = Decimal("199495.00")
    with pytest.raises(
        V2CanonicalSnapshotInvariantError,
        match="unfilled order status",
    ):
        _read(rows)


def test_account_materialization_time_cannot_precede_latest_ledger_event():
    rows = _rows_fixture()
    rows["st_trade_account_v2"][0]["updated_at"] = _dt(3, 9, 59)
    with pytest.raises(
        V2CanonicalSnapshotInvariantError,
        match="latest logical ledger event",
    ):
        _read(rows)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("observed_count", 2, "observed_count exceeds"),
        ("coverage", Decimal("0.50"), "coverage differs"),
        ("agreement_ratio", Decimal("1.01"), "must not exceed 1"),
        ("quality_status", "UNKNOWN", "quality_status is unsupported"),
    ],
)
def test_public_receipt_counts_ratios_and_status_fail_closed(field, value, message):
    rows = _rows_fixture()
    rows["st_public_quote_receipt_v2"][0][field] = value
    with pytest.raises(V2CanonicalSnapshotInvariantError, match=message):
        _read(rows)


def test_receipts_are_scoped_to_fill_dates_and_realtime_sql_has_upper_bound():
    rows = _rows_fixture()
    receipt = rows["st_public_quote_receipt_v2"][0]
    receipt["trade_date"] = date(2026, 8, 2)
    receipt["quote_at"] = _dt(2, 10)
    receipt["received_at"] = _dt(2, 10, 0, 1)
    receipt["created_at"] = _dt(2, 10, 0, 2)
    with pytest.raises(V2CanonicalSnapshotInvariantError, match="fill-date range"):
        _read(rows)

    result, connection, _ = _read()
    assert result.snapshot is not None
    realtime_call = next(
        call
        for call in connection.execute.call_args_list
        if "st_qmt_realtime_sync_receipt_v2" in str(call.args[0])
    )
    assert "source_generated_at < :receipt_end_exclusive_at" in str(
        realtime_call.args[0]
    )
    assert realtime_call.args[1]["receipt_end_exclusive_at"] == _dt(4)


def test_full_rule_hash_covers_source_evidence_beyond_adapter_fingerprint():
    first, _, _ = _read()
    rows = _rows_fixture()
    rows["st_instrument_rule_v2"][0]["source_snapshot_hash"] = "d" * 64
    second, _, _ = _read(rows)
    assert first.snapshot is not None and second.snapshot is not None
    first_rule = first.snapshot.instrument_rules[0]
    second_rule = second.snapshot.instrument_rules[0]
    assert (
        first_rule.adapter_instrument_rule_hash
        == second_rule.adapter_instrument_rule_hash
    )
    assert first_rule.instrument_rule_hash != second_rule.instrument_rule_hash
    assert first.snapshot.transaction_content_root_hash != second.snapshot.transaction_content_root_hash


def test_snapshot_and_result_validators_recompute_rows_manifest_root_and_blockers():
    result, _, _ = _read()
    assert result.snapshot is not None
    assert validate_v2_canonical_snapshot(result.snapshot) is result.snapshot
    assert validate_v2_canonical_read_result(result) is result

    object.__setattr__(result.snapshot.account, "account_name", "tampered")
    with pytest.raises(V2CanonicalSnapshotInvariantError, match="manifest/content root"):
        validate_v2_canonical_snapshot(result.snapshot)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda rows: rows["st_trade_account_v2"][0].__setitem__(
                "updated_at", _dt(1, 7)
            ),
            "updated_at precedes",
        ),
        (
            lambda rows: rows["st_trade_intent_v2"][0].__setitem__(
                "target_quantity", 0
            ),
            "target direction",
        ),
        (
            lambda rows: rows["st_order_v2"][0].__setitem__(
                "limit_price", Decimal("9.99")
            ),
            "execution terms",
        ),
        (
            lambda rows: rows["st_fill_v2"][0].__setitem__(
                "filled_at", _dt(3, 9)
            ),
            "execution window",
        ),
        (
            lambda rows: rows["st_order_v2"][0].__setitem__(
                "status", "QUEUED"
            ),
            "fully filled order",
        ),
    ],
)
def test_account_order_intent_fill_time_term_and_state_relations_are_strict(
    mutate, message
):
    rows = _rows_fixture()
    mutate(rows)
    with pytest.raises(V2CanonicalSnapshotInvariantError, match=message):
        _read(rows)


def test_module_has_only_static_selects_over_explicit_allowlist_and_no_engine_lifecycle():
    allowed = {
        "st_trade_account_v2",
        "st_trade_intent_v2",
        "st_order_v2",
        "st_fill_v2",
        "st_position_lot_v2",
        "st_cash_ledger_v2",
        "st_fee_profile_v2",
        "st_instrument_rule_v2",
        "si_trade_calendar",
        "st_qmt_minute_sync_receipt_v2",
        "st_public_quote_receipt_v2",
        "st_qmt_realtime_sync_receipt_v2",
    }
    sql_constants = {
        name: value
        for name, value in vars(facade).items()
        if name.endswith("_SQL") and isinstance(value, str)
    }
    assert len(sql_constants) == 12
    referenced: set[str] = set()
    for sql in sql_constants.values():
        assert sql.lstrip().upper().startswith("SELECT ")
        assert not re.search(
            r"\b(?:INSERT|UPDATE|DELETE|REPLACE|ALTER|DROP|TRUNCATE|CREATE)\b",
            sql,
            flags=re.IGNORECASE,
        )
        referenced.update(
            match.group(1).lower()
            for match in re.finditer(
                r"\b(?:FROM|JOIN)\s+([A-Za-z0-9_]+)", sql, flags=re.IGNORECASE
            )
        )
    assert referenced == allowed

    source = Path(inspect.getsourcefile(facade) or "").read_text(encoding="utf-8")
    assert "create_engine" not in source
    assert ".commit(" not in source
    assert ".rollback(" not in source
