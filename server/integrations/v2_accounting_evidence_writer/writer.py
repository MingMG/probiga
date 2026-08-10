"""Caller-transaction writer for immutable V2 accounting-outcome evidence.

The boundary is deliberately opt-in and has no production wiring.  It locks
the existing V2 order, account, fill, cash, and position-lot rows, verifies
that the supplied evidence describes their actual accounting result, and
appends only evidence rows.  Connection and transaction ownership always
remain with the caller.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import text

from server.trading_v2.accounting_evidence import (
    AccountingOutcomeFinalizationStatus,
    FillAccountingFinalization,
    FillAccountingOutcome,
    LotAccountingEffect,
    LotEffectKind,
    LotSnapshot,
    finalize_fill_accounting_outcome,
    validate_fill_accounting_finalization,
    validate_fill_accounting_outcome,
)
from server.trading_v2.domain import PositionState
from server.trading_v2.execution_evidence import CanonicalJson, HistoryOrigin
from server.trading_v2.execution_evidence_schema_gate import (
    V2EvidenceMaintenanceFenceError,
    assert_v2_evidence_maintenance_fence_inactive,
)


MARKET_ZONE = ZoneInfo("Asia/Shanghai")


class AccountingEvidenceTransactionError(RuntimeError):
    """The supplied connection is not inside a caller-owned transaction."""


class AccountingEvidenceCanonicalRowError(RuntimeError):
    """A locked canonical V2 row differs from the supplied outcome."""


class AccountingEvidenceAppendConflictError(RuntimeError):
    """A natural key, identifier, or chain position has different content."""


class AccountingEvidenceAppendStatus(str, Enum):
    INSERTED = "INSERTED"
    FINALIZED = "FINALIZED"
    IDEMPOTENT = "IDEMPOTENT"


@dataclass(frozen=True, slots=True)
class AccountingEvidenceAppendResult:
    status: AccountingEvidenceAppendStatus
    accounting_outcome_id: str
    outcome_hash: str
    lot_effect_count: int
    finalization_id: str
    finalization_hash: str
    finalization_status: AccountingOutcomeFinalizationStatus


def _active_connection(connection: Any) -> Any:
    if connection is None or not callable(getattr(connection, "execute", None)):
        raise AccountingEvidenceTransactionError(
            "a SQLAlchemy-like connection is required"
        )
    probe = getattr(connection, "in_transaction", None)
    if not callable(probe):
        raise AccountingEvidenceTransactionError(
            "connection must expose in_transaction()"
        )
    try:
        active = probe()
    except Exception as exc:  # pragma: no cover - defensive adapter boundary
        raise AccountingEvidenceTransactionError(
            "transaction state cannot be inspected"
        ) from exc
    if type(active) is not bool or not active:
        raise AccountingEvidenceTransactionError(
            "connection must already be in a transaction"
        )
    try:
        assert_v2_evidence_maintenance_fence_inactive(connection)
    except V2EvidenceMaintenanceFenceError as exc:
        raise AccountingEvidenceTransactionError(
            "V2 accounting-evidence writes are blocked by the maintenance fence"
        ) from exc
    return connection


def _mapping_rows(result: Any, *, operation: str) -> tuple[Mapping[str, Any], ...]:
    try:
        values = result.mappings().all()
    except Exception as exc:
        raise AccountingEvidenceCanonicalRowError(
            f"{operation} did not return SQLAlchemy mapping rows"
        ) from exc
    result_rows: list[Mapping[str, Any]] = []
    for value in values:
        if not isinstance(value, Mapping):
            raise AccountingEvidenceCanonicalRowError(
                f"{operation} returned a non-mapping row"
            )
        result_rows.append(dict(value))
    return tuple(result_rows)


def _query_all(
    connection: Any,
    tag: str,
    sql: str,
    params: Mapping[str, Any],
) -> tuple[Mapping[str, Any], ...]:
    result = connection.execute(text(f"/* v2ao:{tag} */\n{sql}"), dict(params))
    return _mapping_rows(result, operation=tag)


def _query_one(
    connection: Any,
    tag: str,
    sql: str,
    params: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    rows = _query_all(connection, tag, sql, params)
    if len(rows) > 1:
        raise AccountingEvidenceCanonicalRowError(
            f"{tag} unexpectedly returned more than one row"
        )
    return None if not rows else rows[0]


def _execute(
    connection: Any,
    tag: str,
    sql: str,
    params: Mapping[str, Any],
) -> None:
    connection.execute(text(f"/* v2ao:{tag} */\n{sql}"), dict(params))


def _columns_sql(columns: tuple[str, ...]) -> str:
    return ", ".join(columns)


def _exact_keys(row: Mapping[str, Any], columns: tuple[str, ...], name: str) -> None:
    actual = frozenset(row)
    expected = frozenset(columns)
    if actual != expected:
        raise AccountingEvidenceCanonicalRowError(
            f"{name} columns differ; "
            f"missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )


def _strict_text(value: object, name: str, *, allow_empty: bool = False) -> str:
    if type(value) is not str or value != value.strip():
        raise AccountingEvidenceCanonicalRowError(f"{name} must be exact text")
    if not value and not allow_empty:
        raise AccountingEvidenceCanonicalRowError(f"{name} cannot be blank")
    return value


def _strict_optional_text(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _strict_text(value, name)


def _strict_int(value: object, name: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise AccountingEvidenceCanonicalRowError(
            f"{name} must be exactly int >= {minimum}"
        )
    return value


def _strict_date(value: object, name: str) -> date:
    if type(value) is not date:
        raise AccountingEvidenceCanonicalRowError(f"{name} must be exactly date")
    return value


def _aware_db_datetime(value: object, name: str) -> datetime:
    if type(value) is not datetime:
        raise AccountingEvidenceCanonicalRowError(
            f"{name} must be exactly datetime"
        )
    if value.microsecond != 0:
        raise AccountingEvidenceCanonicalRowError(
            f"{name} exceeds V2 DATETIME whole-second precision"
        )
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=MARKET_ZONE)
    return value.astimezone(MARKET_ZONE)


def _storage_datetime(value: datetime, name: str) -> datetime:
    return _aware_db_datetime(value, name).replace(tzinfo=None)


def _decimal(value: object, scale: int, name: str) -> Decimal:
    if type(value) is not Decimal or not value.is_finite():
        raise AccountingEvidenceCanonicalRowError(
            f"{name} must be a finite Decimal"
        )
    quantum = Decimal(1).scaleb(-scale)
    try:
        result = value.quantize(quantum)
    except InvalidOperation as exc:
        raise AccountingEvidenceCanonicalRowError(
            f"{name} cannot be represented at scale {scale}"
        ) from exc
    if result != value:
        raise AccountingEvidenceCanonicalRowError(f"{name} exceeds scale {scale}")
    return result


def _decimal_text(value: object, scale: int, name: str) -> str:
    return format(_decimal(value, scale, name), f".{scale}f")


def _canonical_payload_matches(
    supplied: CanonicalJson,
    projection: Mapping[str, Any],
    name: str,
) -> None:
    expected = CanonicalJson.from_value(dict(projection))
    if (
        supplied.json_text != expected.json_text
        or supplied.payload_hash != expected.payload_hash
    ):
        raise AccountingEvidenceCanonicalRowError(
            f"{name} differs from the exact canonical V2 row projection"
        )


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
LOT_COLUMNS = (
    "lot_id",
    "account_id",
    "stock_code",
    "theme_code",
    "strategy_version",
    "opened_fill_id",
    "opened_trade_date",
    "settlement_date",
    "original_quantity",
    "remaining_quantity",
    "cost_price",
    "allocated_buy_fee",
    "position_state",
    "approved_target_quantity",
    "add_count",
    "initial_stop",
    "protective_stop",
    "invalidation_condition",
    "version",
    "created_at",
    "closed_at",
)
FILL_EVIDENCE_REF_COLUMNS = (
    "fill_execution_evidence_id",
    "fill_id",
    "order_id",
    "account_id",
    "stock_code",
    "evidence_hash",
)
CASH_BINDING_REF_COLUMNS = (
    "cash_binding_id",
    "cash_event_id",
    "account_id",
    "related_order_id",
    "related_fill_id",
    "fill_execution_evidence_id",
    "fill_execution_evidence_hash",
    "binding_hash",
)
ORDER_TRANSITION_REF_COLUMNS = (
    "transition_id",
    "order_id",
    "account_id",
    "to_status",
    "next_filled_quantity",
    "transition_kind",
    "related_fill_id",
    "fill_execution_evidence_id",
    "fill_execution_evidence_hash",
    "transition_hash",
)


def _lock_one_fact(
    connection: Any,
    *,
    tag: str,
    table: str,
    columns: tuple[str, ...],
    where: str,
    params: Mapping[str, Any],
) -> Mapping[str, Any]:
    row = _query_one(
        connection,
        tag,
        f"SELECT {_columns_sql(columns)} FROM {table} "
        f"WHERE {where} FOR UPDATE",
        params,
    )
    if row is None:
        raise AccountingEvidenceCanonicalRowError(
            f"{tag} canonical row does not exist"
        )
    _exact_keys(row, columns, tag)
    return row


def _lock_order(connection: Any, outcome: FillAccountingOutcome) -> Mapping[str, Any]:
    return _lock_one_fact(
        connection,
        tag="lock_order",
        table="st_order_v2",
        columns=ORDER_COLUMNS,
        where="order_id = :order_id",
        params={"order_id": outcome.fill_execution_evidence.order_id},
    )


def _lock_account(connection: Any, outcome: FillAccountingOutcome) -> Mapping[str, Any]:
    return _lock_one_fact(
        connection,
        tag="lock_account",
        table="st_trade_account_v2",
        columns=ACCOUNT_COLUMNS,
        where="account_id = :account_id",
        params={"account_id": outcome.fill_execution_evidence.account_id},
    )


def _lock_fill(connection: Any, outcome: FillAccountingOutcome) -> Mapping[str, Any]:
    return _lock_one_fact(
        connection,
        tag="lock_fill",
        table="st_fill_v2",
        columns=FILL_COLUMNS,
        where="fill_id = :fill_id",
        params={"fill_id": outcome.fill_execution_evidence.fill_id},
    )


def _lock_cash(connection: Any, outcome: FillAccountingOutcome) -> Mapping[str, Any]:
    return _lock_one_fact(
        connection,
        tag="lock_cash",
        table="st_cash_ledger_v2",
        columns=CASH_COLUMNS,
        where="cash_event_id = :cash_event_id",
        params={"cash_event_id": outcome.cash_binding.cash_event_id},
    )


def _lock_lots(
    connection: Any,
    outcome: FillAccountingOutcome,
) -> tuple[Mapping[str, Any], ...]:
    fill = outcome.fill_execution_evidence
    side = str(fill.fill_payload.value()["side"])
    lot_params = {
        f"lot_id_{index}": effect.after_lot.lot_id
        for index, effect in enumerate(outcome.lot_effects)
    }
    lot_placeholders = ", ".join(f":{name}" for name in lot_params)
    if side == "BUY":
        lot_scope = f"lot_id IN ({lot_placeholders})"
        scope_params: dict[str, Any] = {}
    else:
        lot_scope = (
            "settlement_date <= :trade_date "
            "AND (remaining_quantity > 0 OR closed_at = :executed_at "
            f"OR lot_id IN ({lot_placeholders}))"
        )
        scope_params = {
            "trade_date": fill.quote_evidence.trade_date,
            "executed_at": _storage_datetime(fill.executed_at, "fill.executed_at"),
        }
    rows = _query_all(
        connection,
        "lock_lots",
        f"SELECT {_columns_sql(LOT_COLUMNS)} FROM st_position_lot_v2 "
        "WHERE account_id = :account_id AND stock_code = :stock_code "
        f"AND ({lot_scope}) "
        "ORDER BY opened_trade_date, lot_id FOR UPDATE",
        {
            "account_id": fill.account_id,
            "stock_code": fill.stock_code,
            **scope_params,
            **lot_params,
        },
    )
    locked_ids: set[str] = set()
    for row in rows:
        _exact_keys(row, LOT_COLUMNS, "position lot row")
        lot_id = _strict_text(row["lot_id"], "lot.lot_id")
        if lot_id in locked_ids:
            raise AccountingEvidenceCanonicalRowError(
                "locked canonical lot ids are not unique"
            )
        locked_ids.add(lot_id)
    expected_ids = {effect.after_lot.lot_id for effect in outcome.lot_effects}
    if not expected_ids <= locked_ids:
        raise AccountingEvidenceCanonicalRowError(
            "one or more affected canonical lots do not exist"
        )
    return rows


def _lock_nested_evidence(connection: Any, outcome: FillAccountingOutcome) -> None:
    fill = outcome.fill_execution_evidence
    cash = outcome.cash_binding
    transition = outcome.order_transition
    fill_row = _lock_one_fact(
        connection,
        tag="lock_fill_evidence",
        table="st_fill_execution_evidence_v2",
        columns=FILL_EVIDENCE_REF_COLUMNS,
        where="fill_execution_evidence_id = :evidence_id",
        params={"evidence_id": fill.fill_execution_evidence_id},
    )
    expected_fill = {
        "fill_execution_evidence_id": fill.fill_execution_evidence_id,
        "fill_id": fill.fill_id,
        "order_id": fill.order_id,
        "account_id": fill.account_id,
        "stock_code": fill.stock_code,
        "evidence_hash": fill.evidence_hash,
    }
    _expect_values(fill_row, expected_fill, "fill execution evidence")
    cash_row = _lock_one_fact(
        connection,
        tag="lock_cash_binding",
        table="st_cash_event_binding_v2",
        columns=CASH_BINDING_REF_COLUMNS,
        where="cash_binding_id = :cash_binding_id",
        params={"cash_binding_id": cash.cash_binding_id},
    )
    expected_cash = {
        "cash_binding_id": cash.cash_binding_id,
        "cash_event_id": cash.cash_event_id,
        "account_id": cash.account_id,
        "related_order_id": cash.related_order_id,
        "related_fill_id": cash.related_fill_id,
        "fill_execution_evidence_id": fill.fill_execution_evidence_id,
        "fill_execution_evidence_hash": fill.evidence_hash,
        "binding_hash": cash.binding_hash,
    }
    _expect_values(cash_row, expected_cash, "cash binding evidence")
    order_row = _lock_one_fact(
        connection,
        tag="lock_order_transition",
        table="st_order_transition_v2",
        columns=ORDER_TRANSITION_REF_COLUMNS,
        where="transition_id = :transition_id",
        params={"transition_id": transition.transition_id},
    )
    expected_order = {
        "transition_id": transition.transition_id,
        "order_id": transition.order_id,
        "account_id": transition.account_id,
        "to_status": transition.to_status.value,
        "next_filled_quantity": transition.next_filled_quantity,
        "transition_kind": transition.transition_kind.value,
        "related_fill_id": transition.related_fill_id,
        "fill_execution_evidence_id": fill.fill_execution_evidence_id,
        "fill_execution_evidence_hash": fill.evidence_hash,
        "transition_hash": transition.transition_hash,
    }
    _expect_values(order_row, expected_order, "order transition evidence")


def _expect_values(
    row: Mapping[str, Any],
    expected: Mapping[str, Any],
    name: str,
) -> None:
    if frozenset(row) != frozenset(expected):
        raise AccountingEvidenceCanonicalRowError(f"{name} columns are not exact")
    for key, value in expected.items():
        if row[key] != value:
            raise AccountingEvidenceCanonicalRowError(
                f"{name}.{key} differs from supplied evidence"
            )


def _canonical_order_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "account_id": _strict_text(row["account_id"], "order.account_id"),
        "created_at": _aware_db_datetime(row["created_at"], "order.created_at"),
        "earliest_at": _aware_db_datetime(row["earliest_at"], "order.earliest_at"),
        "expires_at": _aware_db_datetime(row["expires_at"], "order.expires_at"),
        "idempotency_key": _strict_text(
            row["idempotency_key"], "order.idempotency_key"
        ),
        "intent_id": _strict_text(row["intent_id"], "order.intent_id"),
        "limit_price": _decimal_text(row["limit_price"], 6, "order.limit_price"),
        "order_id": _strict_text(row["order_id"], "order.order_id"),
        "order_type": _strict_text(row["order_type"], "order.order_type"),
        "quantity": _strict_int(row["quantity"], "order.quantity", minimum=1),
        "side": _strict_text(row["side"], "order.side"),
        "stock_code": _strict_text(row["stock_code"], "order.stock_code"),
    }


def _canonical_fill_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "account_id": _strict_text(row["account_id"], "fill.account_id"),
        "created_at": _aware_db_datetime(row["created_at"], "fill.created_at"),
        "fee_amount": _decimal_text(row["fee_amount"], 2, "fill.fee_amount"),
        "fill_id": _strict_text(row["fill_id"], "fill.fill_id"),
        "filled_at": _aware_db_datetime(row["filled_at"], "fill.filled_at"),
        "gross_amount": _decimal_text(row["gross_amount"], 2, "fill.gross_amount"),
        "idempotency_key": _strict_text(
            row["idempotency_key"], "fill.idempotency_key"
        ),
        "match_event_id": _strict_text(row["match_event_id"], "fill.match_event_id"),
        "net_cash_amount": _decimal_text(
            row["net_cash_amount"], 2, "fill.net_cash_amount"
        ),
        "order_id": _strict_text(row["order_id"], "fill.order_id"),
        "price": _decimal_text(row["price"], 6, "fill.price"),
        "quantity": _strict_int(row["quantity"], "fill.quantity", minimum=1),
        "quote_event_id": _strict_text(
            row["quote_event_id"], "fill.quote_event_id"
        ),
        "side": _strict_text(row["side"], "fill.side"),
        "stock_code": _strict_text(row["stock_code"], "fill.stock_code"),
    }


def _canonical_cash_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "account_id": _strict_text(row["account_id"], "cash.account_id"),
        "amount": _decimal_text(row["amount"], 2, "cash.amount"),
        "balance_after": _decimal_text(
            row["balance_after"], 2, "cash.balance_after"
        ),
        "business_event_key": _strict_text(
            row["business_event_key"], "cash.business_event_key"
        ),
        "cash_event_id": _strict_text(row["cash_event_id"], "cash.cash_event_id"),
        "created_at": _aware_db_datetime(row["created_at"], "cash.created_at"),
        "event_type": _strict_text(row["event_type"], "cash.event_type"),
        "occurred_at": _aware_db_datetime(row["occurred_at"], "cash.occurred_at"),
        "related_fill_id": _strict_optional_text(
            row["related_fill_id"], "cash.related_fill_id"
        ),
        "related_order_id": _strict_optional_text(
            row["related_order_id"], "cash.related_order_id"
        ),
        "reversal_of": _strict_optional_text(row["reversal_of"], "cash.reversal_of"),
    }


def _row_to_lot(row: Mapping[str, Any]) -> LotSnapshot:
    try:
        state = PositionState(_strict_text(row["position_state"], "lot.position_state"))
    except ValueError as exc:
        raise AccountingEvidenceCanonicalRowError(
            "lot.position_state is not canonical"
        ) from exc
    return LotSnapshot(
        lot_id=_strict_text(row["lot_id"], "lot.lot_id"),
        account_id=_strict_text(row["account_id"], "lot.account_id"),
        stock_code=_strict_text(row["stock_code"], "lot.stock_code"),
        theme_code=_strict_text(row["theme_code"], "lot.theme_code", allow_empty=True),
        strategy_version=_strict_text(
            row["strategy_version"], "lot.strategy_version"
        ),
        opened_fill_id=_strict_text(row["opened_fill_id"], "lot.opened_fill_id"),
        opened_trade_date=_strict_date(
            row["opened_trade_date"], "lot.opened_trade_date"
        ),
        settlement_date=_strict_date(row["settlement_date"], "lot.settlement_date"),
        original_quantity=_strict_int(
            row["original_quantity"], "lot.original_quantity", minimum=1
        ),
        remaining_quantity=_strict_int(
            row["remaining_quantity"], "lot.remaining_quantity"
        ),
        cost_price=_decimal(row["cost_price"], 6, "lot.cost_price"),
        allocated_buy_fee=_decimal(
            row["allocated_buy_fee"], 2, "lot.allocated_buy_fee"
        ),
        position_state=state,
        approved_target_quantity=_strict_int(
            row["approved_target_quantity"],
            "lot.approved_target_quantity",
            minimum=1,
        ),
        add_count=_strict_int(row["add_count"], "lot.add_count"),
        initial_stop=_decimal(row["initial_stop"], 6, "lot.initial_stop"),
        protective_stop=_decimal(
            row["protective_stop"], 6, "lot.protective_stop"
        ),
        invalidation_condition=_strict_text(
            row["invalidation_condition"], "lot.invalidation_condition"
        ),
        version=_strict_int(row["version"], "lot.version", minimum=1),
        created_at=_aware_db_datetime(row["created_at"], "lot.created_at"),
        closed_at=(
            None
            if row["closed_at"] is None
            else _aware_db_datetime(row["closed_at"], "lot.closed_at")
        ),
    )


def _validate_immutable_facts(
    outcome: FillAccountingOutcome,
    order_row: Mapping[str, Any],
    account_row: Mapping[str, Any],
    fill_row: Mapping[str, Any],
    cash_row: Mapping[str, Any],
) -> None:
    fill = outcome.fill_execution_evidence
    _canonical_payload_matches(
        fill.order_payload,
        _canonical_order_projection(order_row),
        "order payload",
    )
    if _strict_text(account_row["account_id"], "account.account_id") != fill.account_id:
        raise AccountingEvidenceCanonicalRowError(
            "locked account identity differs from fill"
        )
    _decimal(account_row["cash_balance"], 2, "account.cash_balance")
    _canonical_payload_matches(
        fill.fill_payload,
        _canonical_fill_projection(fill_row),
        "fill payload",
    )
    _canonical_payload_matches(
        outcome.cash_binding.cash_event_payload,
        _canonical_cash_projection(cash_row),
        "cash event payload",
    )


def _validate_current_state(
    outcome: FillAccountingOutcome,
    order_row: Mapping[str, Any],
    account_row: Mapping[str, Any],
) -> None:
    fill = outcome.fill_execution_evidence
    transition = outcome.order_transition
    if (
        _strict_text(order_row["order_id"], "order.order_id") != fill.order_id
        or _strict_text(order_row["account_id"], "order.account_id")
        != fill.account_id
        or _strict_text(order_row["stock_code"], "order.stock_code")
        != fill.stock_code
        or _strict_text(order_row["status"], "order.status")
        != transition.to_status.value
        or _strict_int(order_row["filled_quantity"], "order.filled_quantity")
        != transition.next_filled_quantity
        or _strict_optional_text(order_row["waiting_reason"], "order.waiting_reason")
        != transition.waiting_reason
    ):
        raise AccountingEvidenceCanonicalRowError(
            "current order row differs from the FILL_APPLIED outcome"
        )
    _aware_db_datetime(order_row["updated_at"], "order.updated_at")
    _exact_keys(account_row, ACCOUNT_COLUMNS, "account row")
    if (
        _strict_text(account_row["account_id"], "account.account_id")
        != fill.account_id
        or _decimal(account_row["cash_balance"], 2, "account.cash_balance")
        != outcome.account_cash_after
    ):
        raise AccountingEvidenceCanonicalRowError(
            "current account cash differs from accounting outcome"
        )


def _validate_current_lots(
    outcome: FillAccountingOutcome,
    rows: tuple[Mapping[str, Any], ...],
) -> None:
    snapshots = tuple(_row_to_lot(row) for row in rows)
    current: dict[str, LotSnapshot] = {}
    for snapshot in snapshots:
        if snapshot.lot_id in current:
            raise AccountingEvidenceCanonicalRowError(
                "locked canonical lot ids are not unique"
            )
        current[snapshot.lot_id] = snapshot
    for effect in outcome.lot_effects:
        actual = current.get(effect.after_lot.lot_id)
        if actual is None or actual != effect.after_lot:
            raise AccountingEvidenceCanonicalRowError(
                f"current lot {effect.after_lot.lot_id} differs from after row"
            )
    fill = outcome.fill_execution_evidence
    side = fill.fill_payload.value()["side"]
    if side == "BUY":
        created = outcome.lot_effects[0].after_lot
        if created.opened_fill_id != fill.fill_id:
            raise AccountingEvidenceCanonicalRowError(
                "BUY current lot was not opened by the bound fill"
            )
        return
    pre_fill = dict(current)
    effect_ids = {effect.after_lot.lot_id for effect in outcome.lot_effects}
    unbound_same_fill_closures = sorted(
        lot.lot_id
        for lot in pre_fill.values()
        if lot.remaining_quantity == 0
        and lot.closed_at == fill.executed_at
        and lot.lot_id not in effect_ids
    )
    if unbound_same_fill_closures:
        raise AccountingEvidenceCanonicalRowError(
            "SELL effects omit a lot closed at the bound fill time"
        )
    for effect in outcome.lot_effects:
        assert effect.before_lot is not None
        pre_fill[effect.before_lot.lot_id] = effect.before_lot
    trade_date = fill.quote_evidence.trade_date
    candidates = sorted(
        (
            lot
            for lot in pre_fill.values()
            if lot.remaining_quantity > 0
            and lot.settlement_date <= trade_date
            and lot.created_at <= fill.executed_at
        ),
        key=lambda lot: (lot.opened_trade_date, lot.lot_id),
    )
    remaining = int(fill.fill_payload.value()["quantity"])
    expected: list[tuple[str, int]] = []
    for lot in candidates:
        if remaining == 0:
            break
        consumed = min(remaining, lot.remaining_quantity)
        expected.append((lot.lot_id, consumed))
        remaining -= consumed
    if remaining:
        raise AccountingEvidenceCanonicalRowError(
            "locked pre-fill lots cannot cover SELL quantity"
        )
    actual_effects = [
        (effect.after_lot.lot_id, effect.consumed_quantity)
        for effect in outcome.lot_effects
    ]
    if actual_effects != expected:
        raise AccountingEvidenceCanonicalRowError(
            "SELL effects skip or reorder a canonical FIFO lot"
        )


PROVENANCE_COLUMNS = (
    "history_origin",
    "history_origin_id",
    "history_origin_at",
    "authority_status",
    "authority_receipt_hash",
    "provenance_hash",
)
OUTCOME_COLUMNS = (
    "accounting_outcome_id",
    "fill_id",
    "fill_execution_evidence_id",
    "fill_execution_evidence_hash",
    "cash_binding_id",
    "cash_binding_hash",
    "cash_event_id",
    "order_transition_id",
    "order_transition_hash",
    "order_id",
    "account_id",
    "stock_code",
    "side",
    "account_cash_before",
    "account_cash_after",
    "lot_effect_root_hash",
    "lot_effects_hash",
    "lot_effect_count",
    "total_effect_quantity",
    *PROVENANCE_COLUMNS,
    "recorded_at",
    "outcome_hash",
    "created_at",
)
LOT_EFFECT_COLUMNS = (
    "lot_transition_evidence_id",
    "accounting_outcome_id",
    "fill_id",
    "fill_execution_evidence_id",
    "fill_execution_evidence_hash",
    "effect_sequence",
    "lot_transition_sequence",
    "effect_kind",
    "lot_effect_root_hash",
    "previous_effect_id",
    "previous_effect_hash",
    "previous_lot_transition_id",
    "previous_lot_transition_hash",
    "lot_id",
    "consumed_quantity",
    "before_lot_json",
    "before_lot_hash",
    "after_lot_json",
    "after_lot_hash",
    "occurred_at",
    "bound_at",
    *PROVENANCE_COLUMNS,
    "effect_hash",
    "created_at",
)
FINALIZATION_COLUMNS = (
    "finalization_id",
    "accounting_outcome_id",
    "fill_id",
    "outcome_hash",
    "fill_execution_evidence_id",
    "fill_execution_evidence_hash",
    "lot_effect_root_hash",
    "lot_effects_hash",
    "effect_hashes_json",
    "lot_effect_count",
    "total_effect_quantity",
    "finalization_status",
    *PROVENANCE_COLUMNS,
    "finalized_at",
    "finalization_hash",
    "created_at",
)
LOT_HEAD_COLUMNS = (
    "lot_transition_evidence_id",
    "lot_id",
    "lot_transition_sequence",
    "effect_hash",
    *PROVENANCE_COLUMNS,
)


def _provenance_storage(value: Any) -> dict[str, Any]:
    provenance = value.provenance
    return {
        "history_origin": provenance.history_origin.value,
        "history_origin_id": provenance.history_origin_id,
        "history_origin_at": (
            None
            if provenance.history_origin_at is None
            else _storage_datetime(
                provenance.history_origin_at,
                "provenance.history_origin_at",
            )
        ),
        "authority_status": provenance.authority_status.value,
        "authority_receipt_hash": provenance.authority_receipt_hash,
        "provenance_hash": provenance.provenance_hash,
    }


def _outcome_storage(outcome: FillAccountingOutcome) -> dict[str, Any]:
    fill = outcome.fill_execution_evidence
    cash = outcome.cash_binding
    transition = outcome.order_transition
    side = str(fill.fill_payload.value()["side"])
    return {
        "accounting_outcome_id": outcome.accounting_outcome_id,
        "fill_id": fill.fill_id,
        "fill_execution_evidence_id": fill.fill_execution_evidence_id,
        "fill_execution_evidence_hash": fill.evidence_hash,
        "cash_binding_id": cash.cash_binding_id,
        "cash_binding_hash": cash.binding_hash,
        "cash_event_id": cash.cash_event_id,
        "order_transition_id": transition.transition_id,
        "order_transition_hash": transition.transition_hash,
        "order_id": fill.order_id,
        "account_id": fill.account_id,
        "stock_code": fill.stock_code,
        "side": side,
        "account_cash_before": outcome.account_cash_before,
        "account_cash_after": outcome.account_cash_after,
        "lot_effect_root_hash": outcome.lot_effect_root_hash,
        "lot_effects_hash": outcome.lot_effects_hash,
        "lot_effect_count": outcome.lot_effect_count,
        "total_effect_quantity": outcome.total_effect_quantity,
        **_provenance_storage(outcome),
        "recorded_at": _storage_datetime(outcome.recorded_at, "outcome.recorded_at"),
        "outcome_hash": outcome.outcome_hash,
        "created_at": _storage_datetime(outcome.recorded_at, "outcome.created_at"),
    }


def _finalization_storage(
    value: FillAccountingFinalization,
) -> dict[str, Any]:
    validate_fill_accounting_finalization(value)
    outcome = value.accounting_outcome
    fill = outcome.fill_execution_evidence
    return {
        "finalization_id": value.finalization_id,
        "accounting_outcome_id": outcome.accounting_outcome_id,
        "fill_id": fill.fill_id,
        "outcome_hash": outcome.outcome_hash,
        "fill_execution_evidence_id": fill.fill_execution_evidence_id,
        "fill_execution_evidence_hash": fill.evidence_hash,
        "lot_effect_root_hash": outcome.lot_effect_root_hash,
        "lot_effects_hash": outcome.lot_effects_hash,
        "effect_hashes_json": value.effect_hashes_json,
        "lot_effect_count": outcome.lot_effect_count,
        "total_effect_quantity": outcome.total_effect_quantity,
        "finalization_status": value.finalization_status.value,
        **_provenance_storage(outcome),
        "finalized_at": _storage_datetime(
            value.finalized_at,
            "finalization.finalized_at",
        ),
        "finalization_hash": value.finalization_hash,
        "created_at": _storage_datetime(
            value.finalized_at,
            "finalization.created_at",
        ),
    }


def _lot_json(snapshot: LotSnapshot) -> CanonicalJson:
    return CanonicalJson.from_value(snapshot.canonical_payload())


def _effect_storage(
    outcome: FillAccountingOutcome,
    effect: LotAccountingEffect,
) -> dict[str, Any]:
    before = None if effect.before_lot is None else _lot_json(effect.before_lot)
    after = _lot_json(effect.after_lot)
    fill = outcome.fill_execution_evidence
    return {
        "lot_transition_evidence_id": effect.lot_transition_evidence_id,
        "accounting_outcome_id": outcome.accounting_outcome_id,
        "fill_id": fill.fill_id,
        "fill_execution_evidence_id": fill.fill_execution_evidence_id,
        "fill_execution_evidence_hash": fill.evidence_hash,
        "effect_sequence": effect.effect_sequence,
        "lot_transition_sequence": effect.lot_transition_sequence,
        "effect_kind": effect.effect_kind.value,
        "lot_effect_root_hash": effect.lot_effect_root_hash,
        "previous_effect_id": effect.previous_effect_id,
        "previous_effect_hash": effect.previous_effect_hash,
        "previous_lot_transition_id": effect.previous_lot_transition_id,
        "previous_lot_transition_hash": effect.previous_lot_transition_hash,
        "lot_id": effect.after_lot.lot_id,
        "consumed_quantity": effect.consumed_quantity,
        "before_lot_json": None if before is None else before.json_text,
        "before_lot_hash": (
            None if effect.before_lot is None else effect.before_lot.snapshot_hash
        ),
        "after_lot_json": after.json_text,
        "after_lot_hash": effect.after_lot.snapshot_hash,
        "occurred_at": _storage_datetime(effect.occurred_at, "effect.occurred_at"),
        "bound_at": _storage_datetime(effect.bound_at, "effect.bound_at"),
        **_provenance_storage(effect),
        "effect_hash": effect.effect_hash,
        "created_at": _storage_datetime(effect.bound_at, "effect.created_at"),
    }


def _select_existing_outcome(
    connection: Any,
    *,
    tag: str,
    where: str,
    params: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    row = _query_one(
        connection,
        tag,
        f"SELECT {_columns_sql(OUTCOME_COLUMNS)} "
        f"FROM st_fill_accounting_outcome_v2 WHERE {where} FOR UPDATE",
        params,
    )
    if row is not None:
        _exact_keys(row, OUTCOME_COLUMNS, "stored accounting outcome")
    return row


def _select_existing_effects(
    connection: Any,
    outcome_id: str,
) -> tuple[Mapping[str, Any], ...]:
    rows = _query_all(
        connection,
        "select_existing_effects",
        f"SELECT {_columns_sql(LOT_EFFECT_COLUMNS)} "
        "FROM st_lot_transition_evidence_v2 "
        "WHERE accounting_outcome_id = :accounting_outcome_id "
        "ORDER BY effect_sequence FOR UPDATE",
        {"accounting_outcome_id": outcome_id},
    )
    for row in rows:
        _exact_keys(row, LOT_EFFECT_COLUMNS, "stored lot effect")
    return rows


def _select_existing_finalization(
    connection: Any,
    *,
    tag: str,
    where: str,
    params: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    row = _query_one(
        connection,
        tag,
        f"SELECT {_columns_sql(FINALIZATION_COLUMNS)} "
        "FROM st_fill_accounting_outcome_finalization_v2 "
        f"WHERE {where} FOR UPDATE",
        params,
    )
    if row is not None:
        _exact_keys(row, FINALIZATION_COLUMNS, "stored accounting finalization")
    return row


def _exact_stored_row(
    row: Mapping[str, Any],
    expected: Mapping[str, Any],
) -> bool:
    return frozenset(row) == frozenset(expected) and all(
        row[key] == value for key, value in expected.items()
    )


def _stored_replay_result(
    connection: Any,
    outcome: FillAccountingOutcome,
    stored: Mapping[str, Any],
) -> AccountingEvidenceAppendResult:
    expected_outcome = _outcome_storage(outcome)
    if not _exact_stored_row(stored, expected_outcome):
        raise AccountingEvidenceAppendConflictError(
            "accounting outcome identifier or natural fill key has different content"
        )
    actual_effects = _select_existing_effects(
        connection,
        outcome.accounting_outcome_id,
    )
    expected_effects = tuple(
        _effect_storage(outcome, effect) for effect in outcome.lot_effects
    )
    if len(actual_effects) != len(expected_effects) or any(
        not _exact_stored_row(actual, expected)
        for actual, expected in zip(actual_effects, expected_effects)
    ):
        raise AccountingEvidenceAppendConflictError(
            "stored accounting outcome has incomplete or different lot effects"
        )
    return _append_or_replay_finalization(
        connection,
        finalize_fill_accounting_outcome(outcome),
        inserted_status=AccountingEvidenceAppendStatus.FINALIZED,
    )


def _lock_and_validate_lot_heads(
    connection: Any,
    outcome: FillAccountingOutcome,
) -> None:
    for effect in outcome.lot_effects:
        row = _query_one(
            connection,
            "lock_lot_head",
            f"SELECT {_columns_sql(LOT_HEAD_COLUMNS)} "
            "FROM st_lot_transition_evidence_v2 "
            "WHERE lot_id = :lot_id "
            "ORDER BY lot_transition_sequence DESC LIMIT 1 FOR UPDATE",
            {"lot_id": effect.after_lot.lot_id},
        )
        if row is None:
            if (
                effect.lot_transition_sequence != 0
                or effect.previous_lot_transition_id is not None
                or effect.previous_lot_transition_hash is not None
            ):
                raise AccountingEvidenceAppendConflictError(
                    "lot transition predecessor is missing"
                )
            if (
                effect.effect_kind is LotEffectKind.SELL_FIFO_CONSUME
                and outcome.provenance.history_origin
                is HistoryOrigin.COMPLETE_FROM_DECLARED_ORIGIN
            ):
                raise AccountingEvidenceAppendConflictError(
                    "complete SELL lot history cannot start from an unknown head"
                )
            continue
        _exact_keys(row, LOT_HEAD_COLUMNS, "lot transition head")
        expected_sequence = _strict_int(
            row["lot_transition_sequence"],
            "lot head sequence",
        ) + 1
        if (
            _strict_text(row["lot_id"], "lot head id") != effect.after_lot.lot_id
            or effect.lot_transition_sequence != expected_sequence
            or effect.previous_lot_transition_id
            != _strict_text(
                row["lot_transition_evidence_id"],
                "lot head transition id",
            )
            or effect.previous_lot_transition_hash
            != _strict_text(row["effect_hash"], "lot head effect hash")
        ):
            raise AccountingEvidenceAppendConflictError(
                "lot transition chain head differs from supplied predecessor"
            )
        expected_provenance = _provenance_storage(effect)
        for key in PROVENANCE_COLUMNS:
            if row[key] != expected_provenance[key]:
                raise AccountingEvidenceAppendConflictError(
                    "lot transition history origin changes within one lot chain"
                )


def _insert_row(
    connection: Any,
    *,
    tag: str,
    table: str,
    columns: tuple[str, ...],
    storage: Mapping[str, Any],
) -> None:
    _exact_keys(storage, columns, f"{table} storage")
    names = _columns_sql(columns)
    values = ", ".join(f":{name}" for name in columns)
    _execute(
        connection,
        tag,
        f"INSERT INTO {table} ({names}) VALUES ({values})",
        storage,
    )


def _finalization_result(
    status: AccountingEvidenceAppendStatus,
    value: FillAccountingFinalization,
) -> AccountingEvidenceAppendResult:
    outcome = value.accounting_outcome
    return AccountingEvidenceAppendResult(
        status=status,
        accounting_outcome_id=outcome.accounting_outcome_id,
        outcome_hash=outcome.outcome_hash,
        lot_effect_count=outcome.lot_effect_count,
        finalization_id=value.finalization_id,
        finalization_hash=value.finalization_hash,
        finalization_status=value.finalization_status,
    )


def _append_or_replay_finalization(
    connection: Any,
    value: FillAccountingFinalization,
    *,
    inserted_status: AccountingEvidenceAppendStatus,
) -> AccountingEvidenceAppendResult:
    validate_fill_accounting_finalization(value)
    expected = _finalization_storage(value)
    stored_by_id = _select_existing_finalization(
        connection,
        tag="select_finalization_by_id",
        where="finalization_id = :finalization_id",
        params={"finalization_id": value.finalization_id},
    )
    stored = stored_by_id
    if stored is None:
        stored = _select_existing_finalization(
            connection,
            tag="select_finalization_by_outcome",
            where="accounting_outcome_id = :accounting_outcome_id",
            params={
                "accounting_outcome_id": (
                    value.accounting_outcome.accounting_outcome_id
                )
            },
        )
    if stored is not None:
        if not _exact_stored_row(stored, expected):
            raise AccountingEvidenceAppendConflictError(
                "accounting outcome finalization has different content"
            )
        return _finalization_result(
            AccountingEvidenceAppendStatus.IDEMPOTENT,
            value,
        )
    _insert_row(
        connection,
        tag="insert_finalization",
        table="st_fill_accounting_outcome_finalization_v2",
        columns=FINALIZATION_COLUMNS,
        storage=expected,
    )
    read_back = _select_existing_finalization(
        connection,
        tag="read_back_finalization",
        where="finalization_id = :finalization_id",
        params={"finalization_id": value.finalization_id},
    )
    if read_back is None or not _exact_stored_row(read_back, expected):
        raise AccountingEvidenceCanonicalRowError(
            "inserted accounting finalization failed exact read-back"
        )
    return _finalization_result(inserted_status, value)


def append_fill_accounting_outcome(
    connection: Any,
    outcome: FillAccountingOutcome,
) -> AccountingEvidenceAppendResult:
    """Append one fully-bound fill outcome inside the caller's transaction.

    The lock order is order, account, fill, cash, stock/account lots, existing
    nested evidence, outcome natural key, per-lot evidence heads, and the FINAL
    marker.  The parent and all effects remain pending until that append-only
    marker is written.  Database errors escape immediately; the caller owns
    recovery of its transaction.
    """

    validate_fill_accounting_outcome(outcome)
    connection = _active_connection(connection)
    order_row = _lock_order(connection, outcome)
    account_row = _lock_account(connection, outcome)
    fill_row = _lock_fill(connection, outcome)
    cash_row = _lock_cash(connection, outcome)
    lot_rows = _lock_lots(connection, outcome)
    _lock_nested_evidence(connection, outcome)
    _validate_immutable_facts(
        outcome,
        order_row,
        account_row,
        fill_row,
        cash_row,
    )

    stored_by_id = _select_existing_outcome(
        connection,
        tag="select_outcome_by_id",
        where="accounting_outcome_id = :accounting_outcome_id",
        params={"accounting_outcome_id": outcome.accounting_outcome_id},
    )
    if stored_by_id is not None:
        return _stored_replay_result(connection, outcome, stored_by_id)
    stored_by_fill = _select_existing_outcome(
        connection,
        tag="select_outcome_by_fill",
        where="fill_id = :fill_id",
        params={"fill_id": outcome.fill_execution_evidence.fill_id},
    )
    if stored_by_fill is not None:
        return _stored_replay_result(connection, outcome, stored_by_fill)

    _validate_current_state(outcome, order_row, account_row)
    _validate_current_lots(outcome, lot_rows)
    _lock_and_validate_lot_heads(connection, outcome)

    _insert_row(
        connection,
        tag="insert_outcome",
        table="st_fill_accounting_outcome_v2",
        columns=OUTCOME_COLUMNS,
        storage=_outcome_storage(outcome),
    )
    for effect in outcome.lot_effects:
        _insert_row(
            connection,
            tag="insert_lot_effect",
            table="st_lot_transition_evidence_v2",
            columns=LOT_EFFECT_COLUMNS,
            storage=_effect_storage(outcome, effect),
        )

    finalized = _append_or_replay_finalization(
        connection,
        finalize_fill_accounting_outcome(outcome),
        inserted_status=AccountingEvidenceAppendStatus.INSERTED,
    )

    stored = _select_existing_outcome(
        connection,
        tag="read_back_outcome",
        where="accounting_outcome_id = :accounting_outcome_id",
        params={"accounting_outcome_id": outcome.accounting_outcome_id},
    )
    if stored is None or not _exact_stored_row(stored, _outcome_storage(outcome)):
        raise AccountingEvidenceCanonicalRowError(
            "inserted accounting outcome failed exact read-back"
        )
    stored_effects = _select_existing_effects(
        connection,
        outcome.accounting_outcome_id,
    )
    expected_effects = tuple(
        _effect_storage(outcome, effect) for effect in outcome.lot_effects
    )
    if len(stored_effects) != len(expected_effects) or any(
        not _exact_stored_row(actual, expected)
        for actual, expected in zip(stored_effects, expected_effects)
    ):
        raise AccountingEvidenceCanonicalRowError(
            "inserted lot effects failed exact read-back"
        )
    return finalized


__all__ = [
    "AccountingEvidenceAppendConflictError",
    "AccountingEvidenceAppendResult",
    "AccountingEvidenceAppendStatus",
    "AccountingEvidenceCanonicalRowError",
    "AccountingEvidenceTransactionError",
    "append_fill_accounting_outcome",
]
