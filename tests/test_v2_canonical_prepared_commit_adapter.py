from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import sqlite3

import pytest
from sqlalchemy import create_engine, event, text

from server.integrations.v2_canonical_commit import coordinator
from server.integrations.v2_canonical_commit import prepared_adapter as adapter
from server.integrations.v3_execution_projection import (
    bind_v3_execution_plan,
    project_execution_result,
)
from server.trading_core.contracts import (
    ExecutionEventKind,
    ExecutionIntent,
    ExecutionResult,
    OrderSide as CoreOrderSide,
    OrderStatus as CoreOrderStatus,
    OrderType,
    TimeInForce,
    execution_intent_idempotency_key,
    execution_result_idempotency_key,
)
from server.trading_core.execution import (
    apply_execution_result,
    apply_execution_result_with_receipt,
    new_order_state,
)
from server.trading_v2 import execution as execution_module
from server.trading_v2.domain import OrderStatus
from server.trading_v2.execution_evidence import (
    AuthorityStatus,
    CanonicalJson,
    EvidenceProvenance,
    HistoryOrigin,
    OrderTransitionEvidence,
    OrderTransitionKind,
)
from tools.trading_v2_evidence_extended_behavioral_scenario import (
    build_accounting_behavioral_scenario,
)


SYSTEM_NOW = datetime(2026, 8, 4, 1, 0, tzinfo=timezone.utc)
REPORT_HASH = "a" * 64
BASELINE_HASH = "b" * 64


class _CallerConnection:
    def __init__(self) -> None:
        self.transaction = object()

    def in_transaction(self):
        return True

    def get_transaction(self):
        return self.transaction

    def execute(self, *_args, **_kwargs):
        raise AssertionError("unexpected SQL")

    def begin(self):
        raise AssertionError("adapter must not open a transaction")

    def commit(self):
        raise AssertionError("adapter must not commit")

    def rollback(self):
        raise AssertionError("adapter must not roll back")


def _issue_cutover(monkeypatch, prepare_commit=lambda _mutation: None):
    monkeypatch.setenv("PROBIGA_RUNTIME_ENVIRONMENT", "TEST")
    monkeypatch.setattr(coordinator, "_system_utc_now", lambda: SYSTEM_NOW)
    return adapter._issue_trusted_test_canonical_execution_cutover(
        token_id="prepared-cutover-test",
        acceptance_report_hash=REPORT_HASH,
        expected_baseline_manifest_hash=BASELINE_HASH,
        verified_baseline_manifest_hash=BASELINE_HASH,
        baseline_verification_report_hash=REPORT_HASH,
        external_trusted_hash_recorded=True,
        prepare_commit=prepare_commit,
    )


def test_cutover_is_private_test_only_and_requires_exact_external_baseline(
    monkeypatch,
):
    cutover = _issue_cutover(monkeypatch)
    with pytest.raises(adapter.CanonicalCommitDisabledError):
        adapter.CanonicalExecutionCutover(
            activation_token=cutover.activation_token,
            baseline_attestation=cutover.baseline_attestation,
            prepare_commit=lambda _mutation: None,
        )
    with pytest.raises(adapter.CanonicalCommitDisabledError):
        adapter._issue_trusted_test_canonical_execution_cutover(
            token_id="baseline-mismatch",
            acceptance_report_hash=REPORT_HASH,
            expected_baseline_manifest_hash=BASELINE_HASH,
            verified_baseline_manifest_hash="c" * 64,
            baseline_verification_report_hash=REPORT_HASH,
            external_trusted_hash_recorded=True,
            prepare_commit=lambda _mutation: None,
        )
    monkeypatch.setenv("PROBIGA_RUNTIME_ENVIRONMENT", "PRODUCTION")
    with pytest.raises(adapter.CanonicalCommitDisabledError):
        adapter._issue_trusted_test_canonical_execution_cutover(
            token_id="production-denied",
            acceptance_report_hash=REPORT_HASH,
            expected_baseline_manifest_hash=BASELINE_HASH,
            verified_baseline_manifest_hash=BASELINE_HASH,
            baseline_verification_report_hash=REPORT_HASH,
            external_trusted_hash_recorded=True,
            prepare_commit=lambda _mutation: None,
        )


def test_preflight_requires_capability_and_locks_fence_in_caller_transaction(
    monkeypatch,
):
    connection = _CallerConnection()
    calls: list[str] = []
    monkeypatch.setattr(
        adapter,
        "assert_v2_evidence_maintenance_fence_inactive",
        lambda observed: calls.append(
            "fence" if observed is connection else "wrong"
        ),
    )
    with pytest.raises(adapter.CanonicalCommitDisabledError):
        adapter.preflight_prepared_commit(
            connection,
            cutover=None,
            now=SYSTEM_NOW,
        )
    assert calls == []
    cutover = _issue_cutover(monkeypatch)
    context = adapter.preflight_prepared_commit(
        connection,
        cutover=cutover,
        now=SYSTEM_NOW,
    )
    assert calls == ["fence"]
    assert context.connection_identity == id(connection)
    assert context.transaction_identity == id(connection.transaction)
    assert context.baseline_manifest_hash == BASELINE_HASH


def _accounting_projection(scenario):
    transition = scenario.order_transition
    order_payload = transition.order_payload.value()
    created_at = datetime.fromisoformat(order_payload["created_at"])
    earliest_at = datetime.fromisoformat(order_payload["earliest_at"])
    expires_at = datetime.fromisoformat(order_payload["expires_at"])
    semantics = {
        "account_id": scenario.account_id,
        "decision_id": "prepared-accounting-decision",
        "instrument_id": order_payload["stock_code"],
        "side": CoreOrderSide.SELL,
        "quantity": int(order_payload["quantity"]),
        "order_type": OrderType.LIMIT,
        "time_in_force": TimeInForce.DAY,
        "earliest_at": earliest_at,
        "expires_at": expires_at,
        "limit_price": Decimal(order_payload["limit_price"]),
        "rule_version": "prepared-rules-v1",
        "fee_profile_version": "prepared-fees-v1",
        "execution_policy_version": "prepared-execution-v1",
    }
    intent = ExecutionIntent(
        intent_id=order_payload["intent_id"],
        created_at=created_at,
        idempotency_key=execution_intent_idempotency_key(**semantics),
        **semantics,
    )
    state = new_order_state(order_id=transition.order_id, intent=intent)
    final_receipt = None
    for sequence, (status, event_id, occurred_at) in enumerate(
        (
            (
                CoreOrderStatus.ACCEPTED,
                f"{transition.order_id}:accepted",
                earliest_at,
            ),
            (
                CoreOrderStatus.QUEUED,
                f"{transition.order_id}:queued",
                earliest_at + timedelta(seconds=1),
            ),
            (
                CoreOrderStatus.FILLED,
                transition.source_event_id,
                transition.occurred_at,
            ),
        ),
        start=1,
    ):
        result = ExecutionResult(
            intent_id=intent.intent_id,
            order_id=state.order_id,
            event_id=event_id,
            status=status,
            occurred_at=occurred_at,
            received_at=occurred_at + timedelta(milliseconds=1),
            source_sequence=sequence,
            idempotency_key=execution_result_idempotency_key(
                order_id=state.order_id,
                event_id=event_id,
            ),
            last_fill_quantity=(
                transition.next_filled_quantity
                - transition.previous_filled_quantity
                if status is CoreOrderStatus.FILLED
                else 0
            ),
            last_fill_price=(
                Decimal(scenario.fill_evidence.fill_payload.value()["price"])
                if status is CoreOrderStatus.FILLED
                else None
            ),
        )
        final_receipt = apply_execution_result_with_receipt(state, result)
        state = final_receipt.current_state
    assert final_receipt is not None
    binding = bind_v3_execution_plan(
        execution_plan_id="prepared-plan-accounting",
        source_intent_id=intent.intent_id,
        source_order_id=transition.order_id,
        bound_at=created_at,
    )
    return (
        project_execution_result(
            binding=binding,
            transition=final_receipt,
        ),
        binding,
        final_receipt,
    )


def test_adapter_writes_evidence_then_accounting_then_outbox_without_lifecycle(
    monkeypatch,
):
    scenario = build_accounting_behavioral_scenario()
    transition = scenario.order_transition
    mutation = adapter.CanonicalMechanicalMutation(
        order_id=transition.order_id,
        account_id=transition.account_id,
        transitions=(
            adapter.CanonicalMechanicalTransition(
                order_id=transition.order_id,
                account_id=transition.account_id,
                from_status=transition.from_status,
                to_status=transition.to_status,
                previous_filled_quantity=transition.previous_filled_quantity,
                next_filled_quantity=transition.next_filled_quantity,
                previous_waiting_reason=None,
                next_waiting_reason=transition.waiting_reason,
                transition_kind=transition.transition_kind,
                source_event_type=transition.source_event_type,
                source_event_id=transition.source_event_id,
                source_event_hash=transition.source_event_hash,
                occurred_at=transition.occurred_at,
                related_fill_id=transition.related_fill_id,
            ),
        ),
        result_status=transition.to_status.value,
        recorded_at=scenario.outcome.recorded_at,
        fill_id=scenario.fill_evidence.fill_id,
    )
    projection, binding, receipt = _accounting_projection(scenario)
    bundle = adapter.PreparedCanonicalCommitBundle(
        mutation_hash=mutation.mutation_hash,
        baseline_manifest_hash=BASELINE_HASH,
        execution_evidence=(
            scenario.fill_evidence.calendar_evidence,
            scenario.fill_evidence.quote_evidence,
            scenario.fill_evidence,
            scenario.cash_evidence_rows[1],
            transition,
        ),
        accounting_outcome=scenario.outcome,
        projections=(projection,),
        projection_bindings=(binding,),
        projection_receipts=(receipt,),
    )
    prepared_inputs: list[tuple[object, ...]] = []

    def _prepare(*args):
        prepared_inputs.append(args)
        return bundle if args == (mutation,) else None

    cutover = _issue_cutover(monkeypatch, prepare_commit=_prepare)
    connection = _CallerConnection()
    monkeypatch.setattr(
        adapter,
        "assert_v2_evidence_maintenance_fence_inactive",
        lambda _connection: None,
    )
    context = adapter.preflight_prepared_commit(
        connection,
        cutover=cutover,
        now=SYSTEM_NOW,
    )
    writes: list[str] = []
    monkeypatch.setattr(
        adapter,
        "append_evidence",
        lambda _connection, evidence, **_kwargs: writes.append(
            f"evidence:{type(evidence).__name__}"
        ),
    )
    monkeypatch.setattr(
        adapter,
        "append_fill_accounting_outcome",
        lambda _connection, _outcome: writes.append("accounting"),
    )
    monkeypatch.setattr(
        adapter,
        "append_v3_transition_outbox",
        lambda _connection, _projection, **_kwargs: writes.append("outbox"),
    )
    receipt = adapter.commit_prepared_canonical_execution(
        connection,
        cutover=cutover,
        preflight=context,
        mutation=mutation,
    )
    assert receipt.production_activation_allowed is False
    assert prepared_inputs == [(mutation,)]
    assert writes == [
        "evidence:MarketCalendarEvidence",
        "evidence:QuoteReceiptEvidence",
        "evidence:FillExecutionEvidence",
        "evidence:CashEventBinding",
        "evidence:OrderTransitionEvidence",
        "accounting",
        "outbox",
    ]


def test_prepare_callback_cannot_replace_the_preflight_transaction(monkeypatch):
    scenario = build_accounting_behavioral_scenario()
    transition = scenario.order_transition
    mutation = adapter.CanonicalMechanicalMutation(
        order_id=transition.order_id,
        account_id=transition.account_id,
        transitions=(
            adapter.CanonicalMechanicalTransition(
                order_id=transition.order_id,
                account_id=transition.account_id,
                from_status=transition.from_status,
                to_status=transition.to_status,
                previous_filled_quantity=transition.previous_filled_quantity,
                next_filled_quantity=transition.next_filled_quantity,
                previous_waiting_reason=None,
                next_waiting_reason=transition.waiting_reason,
                transition_kind=transition.transition_kind,
                source_event_type=transition.source_event_type,
                source_event_id=transition.source_event_id,
                source_event_hash=transition.source_event_hash,
                occurred_at=transition.occurred_at,
                related_fill_id=transition.related_fill_id,
            ),
        ),
        result_status=transition.to_status.value,
        recorded_at=scenario.outcome.recorded_at,
        fill_id=scenario.fill_evidence.fill_id,
    )
    connection = _CallerConnection()

    def _replace_transaction(_mutation):
        connection.transaction = object()
        return None

    cutover = _issue_cutover(monkeypatch, prepare_commit=_replace_transaction)
    monkeypatch.setattr(
        adapter,
        "assert_v2_evidence_maintenance_fence_inactive",
        lambda _connection: None,
    )
    context = adapter.preflight_prepared_commit(
        connection,
        cutover=cutover,
        now=SYSTEM_NOW,
    )
    with pytest.raises(
        adapter.CanonicalCommitInvariantError,
        match="different transaction or gate",
    ):
        adapter.commit_prepared_canonical_execution(
            connection,
            cutover=cutover,
            preflight=context,
            mutation=mutation,
        )


def test_structurally_valid_projection_from_a_different_receipt_is_rejected():
    scenario = build_accounting_behavioral_scenario()
    transition = scenario.order_transition
    mutation = adapter.CanonicalMechanicalMutation(
        order_id=transition.order_id,
        account_id=transition.account_id,
        transitions=(
            adapter.CanonicalMechanicalTransition(
                order_id=transition.order_id,
                account_id=transition.account_id,
                from_status=transition.from_status,
                to_status=transition.to_status,
                previous_filled_quantity=transition.previous_filled_quantity,
                next_filled_quantity=transition.next_filled_quantity,
                previous_waiting_reason=None,
                next_waiting_reason=transition.waiting_reason,
                transition_kind=transition.transition_kind,
                source_event_type=transition.source_event_type,
                source_event_id=transition.source_event_id,
                source_event_hash=transition.source_event_hash,
                occurred_at=transition.occurred_at,
                related_fill_id=transition.related_fill_id,
            ),
        ),
        result_status=transition.to_status.value,
        recorded_at=scenario.outcome.recorded_at,
        fill_id=scenario.fill_evidence.fill_id,
    )
    projection, binding, receipt = _accounting_projection(scenario)
    tampered_result = replace(receipt.result, reason_code="tampered provenance")
    tampered_receipt = apply_execution_result_with_receipt(
        receipt.previous_state,
        tampered_result,
    )
    tampered_projection = project_execution_result(
        binding=binding,
        transition=tampered_receipt,
    )
    assert tampered_projection.source_order_id == projection.source_order_id
    assert tampered_projection.source_event_id == projection.source_event_id
    assert tampered_projection.source_order_status == projection.source_order_status
    assert (
        tampered_projection.source_result_fingerprint
        != projection.source_result_fingerprint
    )
    bundle = adapter.PreparedCanonicalCommitBundle(
        mutation_hash=mutation.mutation_hash,
        baseline_manifest_hash=BASELINE_HASH,
        execution_evidence=(
            scenario.fill_evidence.calendar_evidence,
            scenario.fill_evidence.quote_evidence,
            scenario.fill_evidence,
            scenario.cash_evidence_rows[1],
            transition,
        ),
        accounting_outcome=scenario.outcome,
        projections=(tampered_projection,),
        projection_bindings=(binding,),
        projection_receipts=(receipt,),
    )
    with pytest.raises(
        adapter.CanonicalCommitInvariantError,
        match="exact result",
    ):
        adapter._validate_bundle(mutation, bundle, BASELINE_HASH)


def _execution_engine(*, expires_at: datetime, waiting_reason=None):
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={
            "detect_types": sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        },
    )

    @event.listens_for(engine, "before_cursor_execute", retval=True)
    def _sqlite_for_update(
        _conn, _cursor, statement, parameters, _context, _executemany
    ):
        return statement.replace(" FOR UPDATE", ""), parameters

    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_trade_account_v2 (
                account_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                fee_profile_version TEXT,
                instrument_rule_version TEXT,
                real_trading_enabled INTEGER NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_trade_intent_v2 (
                intent_id TEXT PRIMARY KEY,
                strategy_version TEXT,
                action TEXT,
                theme_code TEXT,
                initial_stop NUMERIC,
                protective_stop NUMERIC,
                invalidation_condition TEXT,
                intent_version INTEGER
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_risk_decision_v2 (
                intent_id TEXT PRIMARY KEY,
                approved_quantity INTEGER NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_order_v2 (
                order_id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL,
                intent_id TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                side TEXT NOT NULL,
                order_type TEXT NOT NULL,
                limit_price NUMERIC NOT NULL,
                quantity INTEGER NOT NULL,
                filled_quantity INTEGER NOT NULL,
                status TEXT NOT NULL,
                waiting_reason TEXT,
                earliest_at TIMESTAMP NOT NULL,
                expires_at TIMESTAMP NOT NULL,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE canonical_side_effect (value TEXT NOT NULL)
        """))
        connection.execute(
            text("""
                INSERT INTO st_trade_account_v2 VALUES
                ('paper-main-v2', 'ACTIVE', 'fees-v1', 'rules-v1', 0)
            """),
        )
        connection.execute(text("""
            INSERT INTO st_trade_intent_v2 VALUES
            ('intent-1', 'strategy-v1', 'OPEN', 'theme-1',
             9, 8, 'stop', 1)
        """))
        connection.execute(text("""
            INSERT INTO st_risk_decision_v2 VALUES ('intent-1', 100)
        """))
        connection.execute(
            text("""
                INSERT INTO st_order_v2 VALUES
                ('order-1', 'paper-main-v2', 'intent-1', '600001.SH',
                 'BUY', 'LIMIT', 10, 100, 0, 'QUEUED', :waiting_reason,
                 :earliest_at, :expires_at, :created_at, :created_at)
            """),
            {
                "waiting_reason": waiting_reason,
                "earliest_at": datetime(2026, 8, 4, 9, 0),
                "expires_at": expires_at,
                "created_at": datetime(2026, 8, 4, 8, 59),
            },
        )
    return engine


def _waiting_bundle(
    mutation,
    *,
    receipt_reason: str | None = None,
):
    mechanical = mutation.transitions[-1]
    created_at = datetime(2026, 8, 4, 0, 59, tzinfo=timezone.utc)
    earliest_at = datetime(2026, 8, 4, 1, 0, tzinfo=timezone.utc)
    expires_at = datetime(2026, 8, 4, 7, 0, tzinfo=timezone.utc)
    semantics = {
        "account_id": mechanical.account_id,
        "decision_id": "waiting-decision-1",
        "instrument_id": "600001.SH",
        "side": CoreOrderSide.BUY,
        "quantity": 100,
        "order_type": OrderType.LIMIT,
        "time_in_force": TimeInForce.DAY,
        "earliest_at": earliest_at,
        "expires_at": expires_at,
        "limit_price": Decimal("10"),
        "rule_version": "rules-v1",
        "fee_profile_version": "fees-v1",
        "execution_policy_version": "execution-v1",
    }
    intent = ExecutionIntent(
        intent_id="intent-1",
        created_at=created_at,
        idempotency_key=execution_intent_idempotency_key(**semantics),
        **semantics,
    )
    state = new_order_state(order_id=mechanical.order_id, intent=intent)
    for sequence, (status, event_id, occurred_at) in enumerate(
        (
            (CoreOrderStatus.ACCEPTED, "order-1:accepted", earliest_at),
            (
                CoreOrderStatus.QUEUED,
                "order-1:queued",
                earliest_at + timedelta(seconds=1),
            ),
        ),
        start=1,
    ):
        state = apply_execution_result(
            state,
            ExecutionResult(
                intent_id=intent.intent_id,
                order_id=mechanical.order_id,
                event_id=event_id,
                status=status,
                occurred_at=occurred_at,
                received_at=occurred_at,
                source_sequence=sequence,
                idempotency_key=execution_result_idempotency_key(
                    order_id=mechanical.order_id,
                    event_id=event_id,
                ),
            ),
        )
    if mechanical.previous_waiting_reason:
        prior_event_id = "order-1:prior-waiting-reason"
        state = apply_execution_result(
            state,
            ExecutionResult(
                intent_id=intent.intent_id,
                order_id=mechanical.order_id,
                event_id=prior_event_id,
                status=CoreOrderStatus.QUEUED,
                occurred_at=mechanical.occurred_at - timedelta(seconds=1),
                received_at=mechanical.occurred_at - timedelta(seconds=1),
                source_sequence=3,
                idempotency_key=execution_result_idempotency_key(
                    order_id=mechanical.order_id,
                    event_id=prior_event_id,
                ),
                reason_code=mechanical.previous_waiting_reason,
                event_kind=ExecutionEventKind.WAITING_REASON_CHANGED,
            ),
        )
    next_reason = receipt_reason or mechanical.next_waiting_reason
    reason_result = ExecutionResult(
        intent_id=intent.intent_id,
        order_id=mechanical.order_id,
        event_id=mechanical.source_event_id,
        status=CoreOrderStatus.QUEUED,
        occurred_at=mechanical.occurred_at,
        received_at=mechanical.occurred_at,
        source_sequence=state.last_source_sequence + 1,
        idempotency_key=execution_result_idempotency_key(
            order_id=mechanical.order_id,
            event_id=mechanical.source_event_id,
        ),
        reason_code=next_reason,
        event_kind=ExecutionEventKind.WAITING_REASON_CHANGED,
    )
    receipt = apply_execution_result_with_receipt(state, reason_result)
    binding = bind_v3_execution_plan(
        execution_plan_id="waiting-plan-1",
        source_intent_id=intent.intent_id,
        source_order_id=mechanical.order_id,
        bound_at=created_at,
    )
    projection = project_execution_result(
        binding=binding,
        transition=receipt,
    )
    evidence = OrderTransitionEvidence(
        order_id=mechanical.order_id,
        account_id=mechanical.account_id,
        order_payload=CanonicalJson.from_value(
            {
                "account_id": mechanical.account_id,
                "created_at": created_at,
                "earliest_at": earliest_at,
                "expires_at": expires_at,
                "idempotency_key": intent.idempotency_key,
                "intent_id": intent.intent_id,
                "limit_price": "10.000000",
                "order_id": mechanical.order_id,
                "order_type": "LIMIT",
                "quantity": 100,
                "side": "BUY",
                "stock_code": "600001.SH",
            }
        ),
        transition_sequence=receipt.result.source_sequence,
        from_status=mechanical.from_status,
        to_status=mechanical.to_status,
        previous_filled_quantity=mechanical.previous_filled_quantity,
        next_filled_quantity=mechanical.next_filled_quantity,
        transition_kind=mechanical.transition_kind,
        waiting_reason=mechanical.next_waiting_reason,
        source_event_type=mechanical.source_event_type,
        source_event_id=mechanical.source_event_id,
        source_event_hash=mechanical.source_event_hash,
        occurred_at=mechanical.occurred_at,
        recorded_at=mutation.recorded_at,
        provenance=EvidenceProvenance(
            history_origin=HistoryOrigin.START_AFTER_UNKNOWN,
            history_origin_id="prepared-waiting-cutover",
            history_origin_at=created_at,
            authority_status=AuthorityStatus.CONTENT_HASH_ONLY,
        ),
    )
    return adapter.PreparedCanonicalCommitBundle(
        mutation_hash=mutation.mutation_hash,
        baseline_manifest_hash=BASELINE_HASH,
        execution_evidence=(evidence,),
        projections=(projection,),
        projection_bindings=(binding,),
        projection_receipts=(receipt,),
    )


def _real_waiting_cutover(
    monkeypatch,
    *,
    receipt_reason: str | None = None,
    fail_outbox: bool = False,
):
    monkeypatch.setattr(
        adapter,
        "assert_v2_evidence_maintenance_fence_inactive",
        lambda _connection: None,
    )
    monkeypatch.setattr(execution_module, "_rule", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        adapter,
        "append_evidence",
        lambda connection, _evidence, **_kwargs: connection.execute(
            text("INSERT INTO canonical_side_effect VALUES ('evidence')")
        ),
    )
    monkeypatch.setattr(
        adapter,
        "append_fill_accounting_outcome",
        lambda *_args, **_kwargs: pytest.fail(
            "waiting transition attempted accounting finalization"
        ),
    )

    def _outbox(connection, _projection, **_kwargs):
        if fail_outbox:
            raise RuntimeError("V3 outbox append failed")
        return connection.execute(
            text("INSERT INTO canonical_side_effect VALUES ('outbox')")
        )

    monkeypatch.setattr(adapter, "append_v3_transition_outbox", _outbox)
    return _issue_cutover(
        monkeypatch,
        prepare_commit=lambda mutation: _waiting_bundle(
            mutation,
            receipt_reason=receipt_reason,
        ),
    )


@pytest.mark.parametrize(
    ("provided_now", "expected_utc"),
    (
        (
            datetime(2026, 8, 4, 10, 0),
            datetime(2026, 8, 4, 2, 0, tzinfo=timezone.utc),
        ),
        (
            datetime(2026, 8, 4, 2, 0, tzinfo=timezone.utc),
            datetime(2026, 8, 4, 2, 0, tzinfo=timezone.utc),
        ),
    ),
)
def test_actual_execute_one_expiry_uses_dual_clock_and_caller_transaction(
    monkeypatch,
    provided_now,
    expected_utc,
):
    engine = _execution_engine(expires_at=datetime(2026, 8, 4, 9, 30))
    calls: list[object] = []
    context = object()

    def _preflight(connection, *, cutover, now):
        assert connection.in_transaction()
        calls.append(("fence", now, cutover))
        return context

    def _commit(connection, *, cutover, preflight, mutation):
        assert connection.in_transaction()
        assert preflight is context
        calls.append(("commit", mutation, cutover))

    monkeypatch.setattr(execution_module, "preflight_prepared_commit", _preflight)
    monkeypatch.setattr(
        execution_module,
        "commit_prepared_canonical_execution",
        _commit,
    )
    capability = object()
    result = execution_module._execute_one(
        engine,
        order_id="order-1",
        account_id="paper-main-v2",
        now=provided_now,
        canonical_cutover=capability,
    )
    assert result["status"] == "EXPIRED"
    assert result["canonical_commit_status"] == "COMMITTED"
    assert calls[0] == ("fence", expected_utc, capability)
    mutation = calls[1][1]
    assert mutation.transitions[0].occurred_at == expected_utc
    assert mutation.transitions[0].to_status is OrderStatus.EXPIRED
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT status FROM st_order_v2 WHERE order_id='order-1'")
        ).scalar_one() == "EXPIRED"


def test_actual_waiting_transition_is_committed_through_same_adapter_seam(
    monkeypatch,
):
    engine = _execution_engine(
        expires_at=datetime(2026, 8, 4, 15, 0),
        waiting_reason="WAIT_NO_QUOTE",
    )
    cutover = _real_waiting_cutover(monkeypatch)
    result = execution_module._execute_one(
        engine,
        order_id="order-1",
        account_id="paper-main-v2",
        now=datetime(2026, 8, 4, 10, 0),
        canonical_cutover=cutover,
    )
    assert result["status"] == "WAITING"
    assert result["canonical_commit_status"] == "COMMITTED"
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT status, waiting_reason FROM st_order_v2")
        ).one() == ("QUEUED", "INSTRUMENT_RULE_BLOCKED")
        assert connection.execute(
            text("SELECT value FROM canonical_side_effect ORDER BY rowid")
        ).scalars().all() == ["evidence", "outbox"]

    replay = execution_module._execute_one(
        engine,
        order_id="order-1",
        account_id="paper-main-v2",
        now=datetime(2026, 8, 4, 10, 0),
        canonical_cutover=cutover,
    )
    assert replay["status"] == "WAITING"
    assert "canonical_commit_status" not in replay
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM canonical_side_effect")
        ).scalar_one() == 2


def test_waiting_receipt_reason_tamper_rolls_back_before_any_append(monkeypatch):
    engine = _execution_engine(
        expires_at=datetime(2026, 8, 4, 15, 0),
        waiting_reason="WAIT_NO_QUOTE",
    )
    cutover = _real_waiting_cutover(
        monkeypatch,
        receipt_reason="WAIT_STALE_QUOTE",
    )
    with pytest.raises(
        adapter.CanonicalCommitInvariantError,
        match="mechanical transition",
    ):
        execution_module._execute_one(
            engine,
            order_id="order-1",
            account_id="paper-main-v2",
            now=datetime(2026, 8, 4, 10, 0),
            canonical_cutover=cutover,
        )
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT status, waiting_reason FROM st_order_v2")
        ).one() == ("QUEUED", "WAIT_NO_QUOTE")
        assert connection.execute(
            text("SELECT COUNT(*) FROM canonical_side_effect")
        ).scalar_one() == 0


def test_outbox_failure_rolls_back_mechanical_and_same_transaction_side_effects(
    monkeypatch,
):
    engine = _execution_engine(
        expires_at=datetime(2026, 8, 4, 15, 0),
        waiting_reason="WAIT_NO_QUOTE",
    )
    cutover = _real_waiting_cutover(monkeypatch, fail_outbox=True)
    with pytest.raises(RuntimeError, match="outbox append failed"):
        execution_module._execute_one(
            engine,
            order_id="order-1",
            account_id="paper-main-v2",
            now=datetime(2026, 8, 4, 10, 0),
            canonical_cutover=cutover,
        )
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT status, waiting_reason FROM st_order_v2")
        ).one() == ("QUEUED", "WAIT_NO_QUOTE")
        assert connection.execute(
            text("SELECT COUNT(*) FROM canonical_side_effect")
        ).scalar_one() == 0


def test_prepare_callback_token_expiry_rolls_back_before_any_append(monkeypatch):
    engine = _execution_engine(
        expires_at=datetime(2026, 8, 4, 15, 0),
        waiting_reason="WAIT_NO_QUOTE",
    )
    clock = [SYSTEM_NOW]
    monkeypatch.setattr(
        adapter,
        "assert_v2_evidence_maintenance_fence_inactive",
        lambda _connection: None,
    )
    monkeypatch.setattr(execution_module, "_rule", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        adapter,
        "append_evidence",
        lambda *_args, **_kwargs: pytest.fail(
            "expired cutover attempted evidence append"
        ),
    )
    monkeypatch.setattr(
        adapter,
        "append_v3_transition_outbox",
        lambda *_args, **_kwargs: pytest.fail(
            "expired cutover attempted outbox append"
        ),
    )

    def _prepare(mutation):
        clock[0] = SYSTEM_NOW + timedelta(minutes=16)
        return _waiting_bundle(mutation)

    cutover = _issue_cutover(monkeypatch, prepare_commit=_prepare)
    monkeypatch.setattr(coordinator, "_system_utc_now", lambda: clock[0])
    with pytest.raises(
        adapter.CanonicalCommitDisabledError,
        match="not currently valid",
    ):
        execution_module._execute_one(
            engine,
            order_id="order-1",
            account_id="paper-main-v2",
            now=datetime(2026, 8, 4, 10, 0),
            canonical_cutover=cutover,
        )
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT status, waiting_reason FROM st_order_v2")
        ).one() == ("QUEUED", "WAIT_NO_QUOTE")
        assert connection.execute(
            text("SELECT COUNT(*) FROM canonical_side_effect")
        ).scalar_one() == 0


def test_default_execute_path_does_not_touch_prepared_adapter(monkeypatch):
    engine = _execution_engine(expires_at=datetime(2026, 8, 4, 9, 30))
    monkeypatch.setattr(
        execution_module,
        "preflight_prepared_commit",
        lambda *_args, **_kwargs: pytest.fail("default path called preflight"),
    )
    monkeypatch.setattr(
        execution_module,
        "commit_prepared_canonical_execution",
        lambda *_args, **_kwargs: pytest.fail("default path called adapter"),
    )
    result = execution_module._execute_one(
        engine,
        order_id="order-1",
        account_id="paper-main-v2",
        now=datetime(2026, 8, 4, 10, 0),
    )
    assert result == {"order_id": "order-1", "status": "EXPIRED"}


def test_tick_cutover_uses_per_order_expiry_and_never_calls_legacy_v3_sync(
    monkeypatch,
):
    engine = _execution_engine(expires_at=datetime(2026, 8, 4, 9, 30))
    capability = object()
    calls = []
    monkeypatch.setattr(execution_module, "is_trade_day", lambda *_args: False)
    monkeypatch.setattr(
        execution_module,
        "_sync_v3_execution_plan_states",
        lambda *_args, **_kwargs: pytest.fail("cutover called legacy V3 sync"),
    )

    def _execute(*_args, **kwargs):
        calls.append(kwargs)
        return {"order_id": kwargs["order_id"], "status": "EXPIRED"}

    monkeypatch.setattr(execution_module, "_execute_one", _execute)
    result = execution_module.run_execution_tick(
        engine,
        now=datetime(2026, 8, 4, 10, 0),
        canonical_cutover=capability,
    )
    assert len(calls) == 1
    assert calls[0]["canonical_cutover"] is capability
    assert result["status"] == "market_closed"
    assert result["expired_orders"] == 1
    assert result["v3_execution_plan_updates"] == 0
    assert result["legacy_v3_direct_sync_suppressed"] is True
    with engine.connect() as connection:
        # The fake per-order caller did not update it; a bulk expiry would.
        assert connection.execute(
            text("SELECT status FROM st_order_v2")
        ).scalar_one() == "QUEUED"
