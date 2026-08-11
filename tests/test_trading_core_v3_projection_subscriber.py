from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import re

import pytest

from server.db.migrations_v3 import MIGRATIONS, _checksum
from server.integrations.v3_execution_projection import (
    ProjectionApplyStatus,
    V3ProjectionSubscriberError,
    apply_v3_execution_projection,
    bind_v3_execution_plan,
    project_execution_result,
)
from server.trading_core.contracts import (
    ExecutionIntent,
    ExecutionResult,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    execution_intent_idempotency_key,
    execution_result_idempotency_key,
)
from server.trading_core.execution import (
    OrderState,
    OrderTransitionReceipt,
    apply_execution_result_with_receipt,
    new_order_state,
)


NOW = datetime(2026, 8, 3, 2, 0, tzinfo=timezone.utc)
EARLIEST = NOW - timedelta(minutes=1)
EXPIRES = NOW + timedelta(hours=1)
APPLIED_AT = NOW + timedelta(minutes=5)
PROJECT_ROOT = Path(__file__).resolve().parents[1]


class _MappingsResult:
    def __init__(self, row=None, *, rowcount: int = 0) -> None:
        self._row = row
        self.rowcount = rowcount

    def mappings(self):
        return self

    def first(self):
        return self._row


class _Dialect:
    name = "sqlite"


class _FakeConnection:
    """Scripted same-connection store; no engine or database is opened."""

    dialect = _Dialect()

    def __init__(self) -> None:
        self.plans = {
            "plan-1": {
                "execution_plan_id": "plan-1",
                "account_id": "account-1",
                "run_uid": "run-1",
                "stock_code": "600001.SH",
                "side": "BUY",
                "quantity": 200,
                "state": "PAPER_QUEUED",
                "real_order_allowed": 0,
                "created_at": EARLIEST.replace(tzinfo=None),
                "updated_at": EARLIEST.replace(tzinfo=None),
            }
        }
        self.intents = {}
        self.orders = {}
        self.bindings = {}
        self.heads = {}
        self.inbox = {}
        self.statements: list[str] = []
        self.add_canonical_order(intent_id="intent-1", order_id="order-1")

    def add_canonical_order(self, *, intent_id: str, order_id: str) -> None:
        self.intents[intent_id] = {
            "intent_id": intent_id,
            "account_id": "account-1",
            "decision_run_uid": "run-1",
            "stock_code": "600001.SH",
            "action": "BUY",
            "current_quantity": 0,
            "target_quantity": 200,
        }
        self.orders[order_id] = {
            "order_id": order_id,
            "intent_id": intent_id,
            "account_id": "account-1",
            "stock_code": "600001.SH",
            "side": "BUY",
            "quantity": 200,
            "created_at": EARLIEST.replace(tzinfo=None),
            "updated_at": EARLIEST.replace(tzinfo=None),
        }

    def execute(self, statement, parameters=None):
        sql = " ".join(str(statement).split())
        params = dict(parameters or {})
        self.statements.append(sql)

        if "FROM st_execution_plan_v3 p" in sql:
            plan = self.plans.get(params["execution_plan_id"])
            intent = self.intents.get(params["source_intent_id"])
            order = self.orders.get(params["source_order_id"])
            if (
                plan is None
                or intent is None
                or order is None
                or order["intent_id"] != intent["intent_id"]
            ):
                return _MappingsResult()
            return _MappingsResult(
                {
                    "execution_plan_id": plan["execution_plan_id"],
                    "plan_account_id": plan["account_id"],
                    "plan_run_uid": plan["run_uid"],
                    "plan_stock_code": plan["stock_code"],
                    "plan_side": plan["side"],
                    "plan_quantity": plan["quantity"],
                    "plan_state": plan["state"],
                    "real_order_allowed": plan["real_order_allowed"],
                    "plan_created_at": plan["created_at"],
                    "intent_id": intent["intent_id"],
                    "intent_account_id": intent["account_id"],
                    "intent_run_uid": intent["decision_run_uid"],
                    "intent_stock_code": intent["stock_code"],
                    "intent_action": intent["action"],
                    "intent_current_quantity": intent["current_quantity"],
                    "intent_target_quantity": intent["target_quantity"],
                    "order_id": order["order_id"],
                    "order_intent_id": order["intent_id"],
                    "order_account_id": order["account_id"],
                    "order_stock_code": order["stock_code"],
                    "order_side": order["side"],
                    "order_quantity": order["quantity"],
                    "order_created_at": order["created_at"],
                    "order_updated_at": order["updated_at"],
                }
            )
        if sql.startswith("SELECT") and "st_execution_plan_binding_v3" in sql:
            if "WHERE execution_plan_id" in sql:
                return _MappingsResult(
                    self.bindings.get(params["execution_plan_id"])
                )
            for binding in self.bindings.values():
                if binding["source_order_id"] == params["source_order_id"]:
                    return _MappingsResult(binding)
            return _MappingsResult()
        if sql.startswith("SELECT") and "st_execution_projection_head_v3" in sql:
            return _MappingsResult(self.heads.get(params["execution_plan_id"]))
        if sql.startswith("SELECT") and "st_execution_projection_inbox_v3" in sql:
            return _MappingsResult(
                self.inbox.get(
                    (params["source_order_id"], params["source_event_id"])
                )
            )
        if sql.startswith("INSERT INTO st_execution_plan_binding_v3"):
            self.bindings[params["execution_plan_id"]] = dict(params)
            return _MappingsResult(rowcount=1)
        if sql.startswith("INSERT INTO st_execution_projection_inbox_v3"):
            self.inbox[(params["source_order_id"], params["source_event_id"])] = {
                **params,
                "plan_state": params["plan_state"],
            }
            return _MappingsResult(rowcount=1)
        if sql.startswith("INSERT INTO st_execution_projection_head_v3"):
            self.heads[params["execution_plan_id"]] = {
                "execution_plan_id": params["execution_plan_id"],
                "binding_id": params["binding_id"],
                "binding_hash": params["binding_hash"],
                "source_order_id": params["source_order_id"],
                "last_source_sequence": params["source_sequence"],
                "last_projection_id": params["projection_id"],
                "last_payload_hash": params["payload_hash"],
                "last_plan_state": params["plan_state"],
                "updated_at": params["updated_at"],
            }
            return _MappingsResult(rowcount=1)
        if sql.startswith("UPDATE st_execution_projection_head_v3"):
            head = self.heads.get(params["execution_plan_id"])
            if (
                head is None
                or head["binding_id"] != params["binding_id"]
                or head["binding_hash"] != params["binding_hash"]
                or head["source_order_id"] != params["source_order_id"]
                or head["last_source_sequence"] != params["previous_sequence"]
            ):
                return _MappingsResult(rowcount=0)
            head.update(
                last_source_sequence=params["source_sequence"],
                last_projection_id=params["projection_id"],
                last_payload_hash=params["payload_hash"],
                last_plan_state=params["plan_state"],
                updated_at=params["updated_at"],
            )
            return _MappingsResult(rowcount=1)
        if sql.startswith("UPDATE st_execution_plan_v3"):
            plan = self.plans.get(params["execution_plan_id"])
            if (
                plan is None
                or plan["state"] != params["previous_state"]
                or plan["real_order_allowed"] != 0
            ):
                return _MappingsResult(rowcount=0)
            plan["state"] = params["next_state"]
            plan["updated_at"] = params["updated_at"]
            return _MappingsResult(rowcount=1)
        raise AssertionError(f"unexpected SQL: {sql}")


def _intent(*, intent_id: str = "intent-1") -> ExecutionIntent:
    semantics = {
        "account_id": "account-1",
        "decision_id": "decision-1",
        "instrument_id": "600001.SH",
        "side": OrderSide.BUY,
        "quantity": 200,
        "order_type": OrderType.LIMIT,
        "time_in_force": TimeInForce.DAY,
        "earliest_at": EARLIEST,
        "expires_at": EXPIRES,
        "limit_price": Decimal("10.01"),
        "rule_version": "rules-v1",
        "fee_profile_version": "fees-v1",
        "execution_policy_version": "execution-v1",
    }
    return ExecutionIntent(
        intent_id=intent_id,
        created_at=EARLIEST,
        idempotency_key=execution_intent_idempotency_key(**semantics),
        **semantics,
    )


def _result(
    state: OrderState,
    status: OrderStatus,
    *,
    event_id: str,
    occurred_at: datetime,
    fill_quantity: int = 0,
    reason_code: str = "",
) -> ExecutionResult:
    return ExecutionResult(
        intent_id=state.intent_id,
        order_id=state.order_id,
        event_id=event_id,
        status=status,
        occurred_at=occurred_at,
        received_at=occurred_at + timedelta(milliseconds=5),
        source_sequence=state.last_source_sequence + 1,
        idempotency_key=execution_result_idempotency_key(
            order_id=state.order_id,
            event_id=event_id,
        ),
        last_fill_quantity=fill_quantity,
        last_fill_price=Decimal("10.01") if fill_quantity else None,
        reason_code=reason_code,
    )


def _receipt(
    state: OrderState,
    status: OrderStatus,
    *,
    event_id: str,
    occurred_at: datetime,
    fill_quantity: int = 0,
    reason_code: str = "",
) -> OrderTransitionReceipt:
    return apply_execution_result_with_receipt(
        state,
        _result(
            state,
            status,
            event_id=event_id,
            occurred_at=occurred_at,
            fill_quantity=fill_quantity,
            reason_code=reason_code,
        ),
    )


def _chain(
    *,
    intent_id: str = "intent-1",
    order_id: str = "order-1",
    plan_id: str = "plan-1",
):
    created = new_order_state(order_id=order_id, intent=_intent(intent_id=intent_id))
    accepted = _receipt(
        created,
        OrderStatus.ACCEPTED,
        event_id=f"{order_id}-accepted",
        occurred_at=NOW,
    )
    partial = _receipt(
        accepted.current_state,
        OrderStatus.PARTIALLY_FILLED,
        event_id=f"{order_id}-partial",
        occurred_at=NOW + timedelta(seconds=1),
        fill_quantity=100,
    )
    pending = _receipt(
        partial.current_state,
        OrderStatus.CANCEL_PENDING,
        event_id=f"{order_id}-cancel-pending",
        occurred_at=NOW + timedelta(seconds=2),
    )
    binding = bind_v3_execution_plan(
        execution_plan_id=plan_id,
        source_intent_id=intent_id,
        source_order_id=order_id,
        bound_at=created.created_at,
    )
    projections = tuple(
        project_execution_result(binding=binding, transition=item)
        for item in (accepted, partial, pending)
    )
    return created, (accepted, partial, pending), projections


def test_subscriber_applies_contiguous_events_and_exact_retry_idempotently():
    connection = _FakeConnection()
    _, _, projections = _chain()

    first = apply_v3_execution_projection(
        connection,
        projections[0],
        applied_at=APPLIED_AT,
    )
    retry = apply_v3_execution_projection(
        connection,
        projections[0],
        applied_at=APPLIED_AT + timedelta(seconds=1),
    )
    second = apply_v3_execution_projection(
        connection,
        projections[1],
        applied_at=APPLIED_AT + timedelta(seconds=2),
    )

    assert first.status == ProjectionApplyStatus.APPLIED
    assert retry.status == ProjectionApplyStatus.IDEMPOTENT
    assert second.status == ProjectionApplyStatus.APPLIED
    assert connection.heads["plan-1"]["last_source_sequence"] == 2
    assert connection.plans["plan-1"]["state"] == "PAPER_PARTIALLY_FILLED"
    assert len(connection.bindings) == 1
    assert len(connection.inbox) == 2


def test_subscriber_rejects_conflicting_payload_for_same_source_event():
    connection = _FakeConnection()
    created, receipts, projections = _chain()
    apply_v3_execution_projection(connection, projections[0], applied_at=APPLIED_AT)
    conflicting_result = replace(
        receipts[0].result,
        reason_code="conflicting-redelivery",
    )
    conflicting_receipt = apply_execution_result_with_receipt(
        created,
        conflicting_result,
    )
    binding = bind_v3_execution_plan(
        execution_plan_id="plan-1",
        source_intent_id="intent-1",
        source_order_id="order-1",
        bound_at=created.created_at,
    )
    conflicting = project_execution_result(
        binding=binding,
        transition=conflicting_receipt,
    )

    assert conflicting.projection_id == projections[0].projection_id
    assert conflicting.payload_hash != projections[0].payload_hash
    with pytest.raises(V3ProjectionSubscriberError, match="different payload"):
        apply_v3_execution_projection(
            connection,
            conflicting,
            applied_at=APPLIED_AT + timedelta(seconds=1),
        )


def test_subscriber_rejects_gap_and_unseen_stale_event_without_plan_regression():
    gap_connection = _FakeConnection()
    _, _, projections = _chain()
    apply_v3_execution_projection(
        gap_connection,
        projections[0],
        applied_at=APPLIED_AT,
    )
    with pytest.raises(V3ProjectionSubscriberError, match="gap"):
        apply_v3_execution_projection(
            gap_connection,
            projections[2],
            applied_at=APPLIED_AT + timedelta(seconds=1),
        )
    assert gap_connection.plans["plan-1"]["state"] == "PAPER_QUEUED"

    stale_connection = _FakeConnection()
    for index, projection in enumerate(projections[:2]):
        apply_v3_execution_projection(
            stale_connection,
            projection,
            applied_at=APPLIED_AT + timedelta(seconds=index),
        )
    stale_receipt = _receipt(
        new_order_state(order_id="order-1", intent=_intent()),
        OrderStatus.ACCEPTED,
        event_id="order-1-unseen-stale",
        occurred_at=NOW,
    )
    stale_binding = bind_v3_execution_plan(
        execution_plan_id="plan-1",
        source_intent_id="intent-1",
        source_order_id="order-1",
        bound_at=EARLIEST,
    )
    stale = project_execution_result(
        binding=stale_binding,
        transition=stale_receipt,
    )
    with pytest.raises(V3ProjectionSubscriberError, match="stale"):
        apply_v3_execution_projection(
            stale_connection,
            stale,
            applied_at=APPLIED_AT + timedelta(seconds=3),
        )
    assert stale_connection.plans["plan-1"]["state"] == (
        "PAPER_PARTIALLY_FILLED"
    )


def test_subscriber_rejects_new_event_with_backwards_apply_time():
    connection = _FakeConnection()
    _, _, projections = _chain()
    apply_v3_execution_projection(
        connection,
        projections[0],
        applied_at=APPLIED_AT,
    )

    with pytest.raises(V3ProjectionSubscriberError, match="head backwards"):
        apply_v3_execution_projection(
            connection,
            projections[1],
            applied_at=APPLIED_AT - timedelta(seconds=1),
        )
    assert connection.heads["plan-1"]["last_source_sequence"] == 1
    assert connection.plans["plan-1"]["state"] == "PAPER_QUEUED"


def test_subscriber_refuses_initial_regression_and_external_plan_drift():
    _, _, projections = _chain()
    terminal = _FakeConnection()
    terminal.plans["plan-1"]["state"] = "CANCELLED"
    with pytest.raises(V3ProjectionSubscriberError, match="non-queued"):
        apply_v3_execution_projection(
            terminal,
            projections[0],
            applied_at=APPLIED_AT,
        )
    assert terminal.plans["plan-1"]["state"] == "CANCELLED"

    drifted = _FakeConnection()
    apply_v3_execution_projection(
        drifted,
        projections[0],
        applied_at=APPLIED_AT,
    )
    drifted.plans["plan-1"]["state"] = "CANCELLED"
    with pytest.raises(V3ProjectionSubscriberError, match="drifted"):
        apply_v3_execution_projection(
            drifted,
            projections[1],
            applied_at=APPLIED_AT + timedelta(seconds=1),
        )
    assert drifted.heads["plan-1"]["last_source_sequence"] == 1


def test_subscriber_rejects_plan_rebinding_to_another_canonical_order():
    connection = _FakeConnection()
    _, _, first_projections = _chain()
    apply_v3_execution_projection(
        connection,
        first_projections[0],
        applied_at=APPLIED_AT,
    )
    connection.add_canonical_order(intent_id="intent-2", order_id="order-2")
    _, _, rebound = _chain(intent_id="intent-2", order_id="order-2")

    with pytest.raises(V3ProjectionSubscriberError, match="binding changed"):
        apply_v3_execution_projection(
            connection,
            rebound[0],
            applied_at=APPLIED_AT + timedelta(seconds=1),
        )


@pytest.mark.parametrize(
    ("table", "field", "value", "message"),
    (
        ("intents", "account_id", "other", "account"),
        ("intents", "decision_run_uid", "other", "run"),
        ("orders", "stock_code", "000001.SZ", "stock"),
        ("orders", "side", "SELL", "side"),
        ("plans", "quantity", 100, "quantity"),
        ("plans", "real_order_allowed", 1, "real-order-enabled"),
        (
            "plans",
            "created_at",
            NOW.replace(tzinfo=None),
            "created after",
        ),
        (
            "orders",
            "created_at",
            NOW.replace(tzinfo=None),
            "creation time differs",
        ),
        (
            "orders",
            "updated_at",
            (EARLIEST - timedelta(seconds=1)).replace(tzinfo=None),
            "timestamp precedes",
        ),
    ),
)
def test_subscriber_fails_closed_on_canonical_mapping_drift(
    table: str,
    field: str,
    value,
    message: str,
):
    connection = _FakeConnection()
    target = getattr(connection, table)
    key = "plan-1" if table == "plans" else (
        "intent-1" if table == "intents" else "order-1"
    )
    target[key][field] = value
    _, _, projections = _chain()

    with pytest.raises(V3ProjectionSubscriberError, match=message):
        apply_v3_execution_projection(
            connection,
            projections[0],
            applied_at=APPLIED_AT,
        )
    assert not connection.bindings
    assert not connection.inbox


def test_subscriber_revalidates_exact_projection_after_frozen_bypass():
    connection = _FakeConnection()
    _, _, projections = _chain()
    object.__setattr__(projections[0], "payload_hash", "0" * 64)

    with pytest.raises(V3ProjectionSubscriberError, match="projection is invalid"):
        apply_v3_execution_projection(
            connection,
            projections[0],
            applied_at=APPLIED_AT,
        )
    assert not connection.statements


def test_projection_subscriber_migration_is_append_only_and_read_model_only():
    migration = next(
        item
        for item in MIGRATIONS
        if item["version"]
        == "20260803_001_v3_execution_projection_subscriber"
    )
    assert migration["version"] == (
        "20260803_001_v3_execution_projection_subscriber"
    )
    predecessor = MIGRATIONS[MIGRATIONS.index(migration) - 1]
    assert _checksum(tuple(predecessor["statements"])) == (
        "3885c522999aa32bf4e74d8d2846a36be5672bef9583b4f685610d3a339c470c"
    )
    sql = "\n".join(migration["statements"])
    for table_name in (
        "st_execution_plan_binding_v3",
        "st_execution_projection_head_v3",
        "st_execution_projection_inbox_v3",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table_name}" in sql
    assert "UNIQUE KEY uk_v3_projection_source_event" in sql
    assert "source_order_id, source_event_id" in sql
    assert "sequence must be contiguous" in sql
    assert not re.search(
        r"\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+"
        r"st_(?:trade_account|trade_intent|order|fill|position_lot|cash|risk)_v2\b",
        sql,
        flags=re.IGNORECASE,
    )


def test_subscriber_source_has_no_engine_or_v2_mechanical_writes():
    source = (
        PROJECT_ROOT
        / "server/integrations/v3_execution_projection/subscriber.py"
    ).read_text(encoding="utf-8")
    assert "create_engine" not in source
    assert ".commit(" not in source
    assert ".rollback(" not in source
    written_tables = {
        match.group(1).lower()
        for match in re.finditer(
            r"\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+([a-z0-9_]+)",
            source,
            flags=re.IGNORECASE,
        )
    }
    assert written_tables <= {
        "st_execution_plan_binding_v3",
        "st_execution_projection_head_v3",
        "st_execution_projection_inbox_v3",
        "st_execution_plan_v3",
    }
