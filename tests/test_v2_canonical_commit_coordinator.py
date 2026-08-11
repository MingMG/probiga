from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from server.integrations.v2_canonical_commit import (
    AcceptanceActivationToken,
    CanonicalCommitDisabledError,
    CanonicalCommitInvariantError,
    SharedCapacityReservation,
    SharedCapacityReservationResult,
    SharedCapacityReservationStatus,
    coordinate_v2_canonical_commit,
)
from server.integrations.v2_canonical_commit import coordinator as module
from server.trading_core.execution.session_gate import (
    SessionGateReason,
    SessionGatedSnapshotBatchDecision,
)


NOW = datetime(2026, 8, 3, 2, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _trusted_ci_process(monkeypatch):
    monkeypatch.setenv(module._RUNTIME_ENVIRONMENT_VARIABLE, "CI")
    monkeypatch.setattr(module, "_system_utc_now", lambda: NOW)


class _Result:
    def __init__(self, *, rows=(), row=None):
        self._rows = rows
        self._row = row

    def scalars(self):
        return iter(self._rows)

    def mappings(self):
        return self

    def __iter__(self):
        return iter(self._rows)

    def first(self):
        return self._row


class _Connection:
    def __init__(
        self,
        *,
        active=True,
        real_enabled=False,
        fence_state="INACTIVE",
    ):
        self.active = active
        self.real_enabled = real_enabled
        self.fence_state = fence_state
        self.statements: list[str] = []
        self.commit_calls = 0
        self.rollback_calls = 0

    def in_transaction(self):
        return self.active

    def execute(self, statement, parameters=None):
        sql = " ".join(str(statement).split())
        self.statements.append(sql)
        if "schema_migration_v2_maintenance_fence" in sql:
            return _Result(
                rows=(
                    {
                        "fence_name": "execution_evidence_011_015",
                        "state": self.fence_state,
                    },
                )
            )
        if "FROM st_order_v2" in sql:
            ids = sorted(str(value) for value in dict(parameters or {}).values())
            return _Result(
                rows=tuple(
                    {"order_id": order_id, "account_id": "paper-main-v2"}
                    for order_id in ids
                )
            )
        if "FROM st_trade_account_v2" in sql:
            return _Result(
                row={
                    "account_id": parameters["account_id"],
                    "real_trading_enabled": self.real_enabled,
                }
            )
        raise AssertionError(sql)

    def commit(self):
        self.commit_calls += 1

    def rollback(self):
        self.rollback_calls += 1


def _decision(monkeypatch):
    allocations = (
        SimpleNamespace(
            order_id="order-b",
            decision=SimpleNamespace(fill_quantity=30),
        ),
        SimpleNamespace(
            order_id="order-a",
            decision=SimpleNamespace(fill_quantity=20),
        ),
    )
    batch = SimpleNamespace(
        source_receipt_hash="a" * 64,
        instrument_id="600001.SH",
        snapshot_id="snapshot-1",
        shared_liquidity_cap=100,
        total_fill_quantity=50,
        allocations=allocations,
    )
    decision = object.__new__(SessionGatedSnapshotBatchDecision)
    object.__setattr__(
        decision,
        "assessment",
        SimpleNamespace(state=SimpleNamespace(value="ACTIVE")),
    )
    object.__setattr__(decision, "gate_reason", SessionGateReason.NONE)
    object.__setattr__(decision, "batch_result", batch)
    object.__setattr__(decision, "decision_hash", "b" * 64)
    monkeypatch.setattr(
        module,
        "validate_session_gated_snapshot_batch_decision",
        lambda _decision: None,
    )
    return decision


def _reservation():
    return SharedCapacityReservation(
        source_receipt_hash="a" * 64,
        instrument_id="600001.SH",
        snapshot_id="snapshot-1",
        shared_cap_before=100,
        reserved_quantity=50,
        shared_cap_after=50,
        allocations=(("order-b", 30), ("order-a", 20)),
    )


def _token(*, valid_for=timedelta(hours=1)):
    return module._issue_trusted_acceptance_activation_token(
        token_id="acceptance-1",
        acceptance_report_hash="c" * 64,
        valid_for=valid_for,
    )


def _callbacks(events, reservation):
    def reserve(connection, value):
        assert value is reservation
        events.append("capacity")
        return SharedCapacityReservationResult(
            SharedCapacityReservationStatus.RESERVED,
            reservation.reservation_hash,
        )

    def facts(connection, decision, value):
        events.append("facts")
        return {"fill_id": "fill-1"}

    def evidence(connection, decision, value, fact_result):
        events.append("evidence")
        return {"transition_id": "transition-1"}

    def outbox(connection, decision, fact_result, evidence_result):
        events.append("outbox")
        return {"outbox_id": "outbox-1"}

    return reserve, facts, evidence, outbox


def test_default_activation_gate_denies_before_any_lock_or_callback(monkeypatch):
    connection = _Connection()
    decision = _decision(monkeypatch)
    reservation = _reservation()
    events = []
    callbacks = _callbacks(events, reservation)
    with pytest.raises(CanonicalCommitDisabledError, match="runtime-disabled"):
        coordinate_v2_canonical_commit(
            connection,
            account_id="paper-main-v2",
            session_decision=decision,
            reservation=reservation,
            reserve_shared_capacity=callbacks[0],
            mutate_facts=callbacks[1],
            append_evidence=callbacks[2],
            append_transition_outbox=callbacks[3],
        )
    assert connection.statements == []
    assert events == []


def test_activation_token_cannot_be_self_signed() -> None:
    with pytest.raises(CanonicalCommitDisabledError, match="trusted in-process"):
        AcceptanceActivationToken(
            token_id="self-signed",
            acceptance_report_hash="d" * 64,
            environment="CI",
            issued_at=NOW,
            expires_at=NOW + timedelta(minutes=5),
        )


def test_activation_token_rejects_public_field_copy_without_issuer_mac(
    monkeypatch,
) -> None:
    legitimate = _token()
    forged = object.__new__(AcceptanceActivationToken)
    for field_name in (
        "token_id",
        "acceptance_report_hash",
        "environment",
        "issued_at",
        "expires_at",
        "scope",
        "acceptance_status",
        "production_activation_allowed",
        "token_hash",
    ):
        object.__setattr__(forged, field_name, getattr(legitimate, field_name))
    object.__setattr__(forged, "_issuer_mac", "0" * 64)
    connection = _Connection()
    decision = _decision(monkeypatch)
    reservation = _reservation()
    callbacks = _callbacks([], reservation)

    with pytest.raises(CanonicalCommitDisabledError, match="tampered"):
        coordinate_v2_canonical_commit(
            connection,
            account_id="paper-main-v2",
            session_decision=decision,
            reservation=reservation,
            reserve_shared_capacity=callbacks[0],
            mutate_facts=callbacks[1],
            append_evidence=callbacks[2],
            append_transition_outbox=callbacks[3],
            activation_token=forged,
        )

    assert connection.statements == []


def test_activation_token_binds_process_environment(monkeypatch) -> None:
    token = _token()
    monkeypatch.setenv(module._RUNTIME_ENVIRONMENT_VARIABLE, "TEST")
    connection = _Connection()
    decision = _decision(monkeypatch)
    reservation = _reservation()
    callbacks = _callbacks([], reservation)

    with pytest.raises(
        CanonicalCommitDisabledError,
        match="does not match the process environment",
    ):
        coordinate_v2_canonical_commit(
            connection,
            account_id="paper-main-v2",
            session_decision=decision,
            reservation=reservation,
            reserve_shared_capacity=callbacks[0],
            mutate_facts=callbacks[1],
            append_evidence=callbacks[2],
            append_transition_outbox=callbacks[3],
            activation_token=token,
        )

    assert connection.statements == []


def test_activation_token_uses_system_utc_and_rejects_future_issue(
    monkeypatch,
) -> None:
    token = _token()
    monkeypatch.setattr(
        module,
        "_system_utc_now",
        lambda: NOW - timedelta(microseconds=1),
    )
    connection = _Connection()
    decision = _decision(monkeypatch)
    reservation = _reservation()
    callbacks = _callbacks([], reservation)

    with pytest.raises(CanonicalCommitDisabledError, match="not currently valid"):
        coordinate_v2_canonical_commit(
            connection,
            account_id="paper-main-v2",
            session_decision=decision,
            reservation=reservation,
            reserve_shared_capacity=callbacks[0],
            mutate_facts=callbacks[1],
            append_evidence=callbacks[2],
            append_transition_outbox=callbacks[3],
            activation_token=token,
        )

    assert connection.statements == []


def test_missing_outbox_port_fails_before_database_locks_or_mutations(
    monkeypatch,
) -> None:
    connection = _Connection()
    decision = _decision(monkeypatch)
    reservation = _reservation()
    events: list[str] = []
    callbacks = _callbacks(events, reservation)

    with pytest.raises(TypeError, match="append_transition_outbox"):
        coordinate_v2_canonical_commit(
            connection,
            account_id="paper-main-v2",
            session_decision=decision,
            reservation=reservation,
            reserve_shared_capacity=callbacks[0],
            mutate_facts=callbacks[1],
            append_evidence=callbacks[2],
            append_transition_outbox=None,  # type: ignore[arg-type]
            activation_token=_token(),
        )

    assert connection.statements == []
    assert events == []


def test_commit_enforces_lock_and_mutation_order_without_owning_transaction(
    monkeypatch,
):
    connection = _Connection()
    decision = _decision(monkeypatch)
    reservation = _reservation()
    events: list[str] = []
    reserve, facts, evidence, outbox = _callbacks(events, reservation)

    receipt = coordinate_v2_canonical_commit(
        connection,
        account_id="paper-main-v2",
        session_decision=decision,
        reservation=reservation,
        reserve_shared_capacity=reserve,
        mutate_facts=facts,
        append_evidence=evidence,
        append_transition_outbox=outbox,
        activation_token=_token(),
    )

    assert "schema_migration_v2_maintenance_fence" in connection.statements[0]
    assert "LOCK IN SHARE MODE" in connection.statements[0]
    assert "st_order_v2" in connection.statements[1]
    assert "ORDER BY order_id FOR UPDATE" in connection.statements[1]
    assert "st_trade_account_v2" in connection.statements[2]
    assert events == ["capacity", "facts", "evidence", "outbox"]
    assert receipt.order_ids == ("order-a", "order-b")
    assert receipt.production_activation_allowed is False
    assert connection.commit_calls == connection.rollback_calls == 0


def test_active_maintenance_fence_blocks_before_canonical_locks(monkeypatch):
    connection = _Connection(fence_state="ACTIVE")
    decision = _decision(monkeypatch)
    reservation = _reservation()
    events: list[str] = []
    reserve, facts, evidence, outbox = _callbacks(events, reservation)

    with pytest.raises(
        CanonicalCommitInvariantError,
        match="maintenance fence",
    ):
        coordinate_v2_canonical_commit(
            connection,
            account_id="paper-main-v2",
            session_decision=decision,
            reservation=reservation,
            reserve_shared_capacity=reserve,
            mutate_facts=facts,
            append_evidence=evidence,
            append_transition_outbox=outbox,
            activation_token=_token(),
        )

    assert events == []
    assert len(connection.statements) == 1
    assert "schema_migration_v2_maintenance_fence" in connection.statements[0]


def test_outbox_port_must_return_a_durable_append_receipt(monkeypatch) -> None:
    connection = _Connection()
    decision = _decision(monkeypatch)
    reservation = _reservation()
    events: list[str] = []
    reserve, facts, evidence, _outbox = _callbacks(events, reservation)

    def no_receipt_outbox(*_args):
        events.append("outbox")
        return None

    with pytest.raises(
        CanonicalCommitInvariantError,
        match="no durable append receipt",
    ):
        coordinate_v2_canonical_commit(
            connection,
            account_id="paper-main-v2",
            session_decision=decision,
            reservation=reservation,
            reserve_shared_capacity=reserve,
            mutate_facts=facts,
            append_evidence=evidence,
            append_transition_outbox=no_receipt_outbox,
            activation_token=_token(),
        )

    assert events == ["capacity", "facts", "evidence", "outbox"]
    assert connection.commit_calls == connection.rollback_calls == 0


@pytest.mark.parametrize(
    ("missing_stage", "error", "expected_events"),
    (
        ("facts", "no durable mutation receipt", ["capacity", "facts"]),
        (
            "evidence",
            "no durable append receipt",
            ["capacity", "facts", "evidence"],
        ),
    ),
)
def test_fact_and_evidence_ports_must_return_durable_receipts(
    monkeypatch,
    missing_stage,
    error,
    expected_events,
) -> None:
    connection = _Connection()
    decision = _decision(monkeypatch)
    reservation = _reservation()
    events: list[str] = []
    reserve, _facts, _evidence, outbox = _callbacks(events, reservation)

    def facts(*_args):
        events.append("facts")
        return None if missing_stage == "facts" else {"fill_id": "fill-1"}

    def evidence(*_args):
        events.append("evidence")
        return None if missing_stage == "evidence" else {
            "transition_id": "transition-1"
        }

    with pytest.raises(CanonicalCommitInvariantError, match=error):
        coordinate_v2_canonical_commit(
            connection,
            account_id="paper-main-v2",
            session_decision=decision,
            reservation=reservation,
            reserve_shared_capacity=reserve,
            mutate_facts=facts,
            append_evidence=evidence,
            append_transition_outbox=outbox,
            activation_token=_token(),
        )

    assert events == expected_events
    assert connection.commit_calls == connection.rollback_calls == 0


def test_callback_failure_escapes_for_outer_transaction_rollback(monkeypatch):
    connection = _Connection()
    decision = _decision(monkeypatch)
    reservation = _reservation()
    events: list[str] = []

    def reserve(_connection, _reservation):
        events.append("capacity")
        return SharedCapacityReservationResult(
            SharedCapacityReservationStatus.RESERVED,
            reservation.reservation_hash,
        )

    def facts(_connection, _decision, _reservation):
        events.append("facts")
        raise RuntimeError("fact write failed")

    with pytest.raises(RuntimeError, match="fact write failed"):
        coordinate_v2_canonical_commit(
            connection,
            account_id="paper-main-v2",
            session_decision=decision,
            reservation=reservation,
            reserve_shared_capacity=reserve,
            mutate_facts=facts,
            append_evidence=lambda *_args: events.append("evidence"),
            append_transition_outbox=lambda *_args: events.append("outbox"),
            activation_token=_token(),
        )
    assert events == ["capacity", "facts"]
    assert connection.commit_calls == connection.rollback_calls == 0


def test_inactive_transaction_real_account_and_expired_token_fail_closed(
    monkeypatch,
):
    decision = _decision(monkeypatch)
    reservation = _reservation()
    callbacks = _callbacks([], reservation)
    with pytest.raises(CanonicalCommitInvariantError, match="active"):
        coordinate_v2_canonical_commit(
            _Connection(active=False),
            account_id="paper-main-v2",
            session_decision=decision,
            reservation=reservation,
            reserve_shared_capacity=callbacks[0],
            mutate_facts=callbacks[1],
            append_evidence=callbacks[2],
            append_transition_outbox=callbacks[3],
            activation_token=_token(),
        )
    expiring_token = _token(valid_for=timedelta(seconds=1))
    monkeypatch.setattr(
        module,
        "_system_utc_now",
        lambda: NOW + timedelta(seconds=1),
    )
    with pytest.raises(CanonicalCommitDisabledError, match="not currently valid"):
        coordinate_v2_canonical_commit(
            _Connection(),
            account_id="paper-main-v2",
            session_decision=decision,
            reservation=reservation,
            reserve_shared_capacity=callbacks[0],
            mutate_facts=callbacks[1],
            append_evidence=callbacks[2],
            append_transition_outbox=callbacks[3],
            activation_token=expiring_token,
        )
    monkeypatch.setattr(module, "_system_utc_now", lambda: NOW)
    with pytest.raises(CanonicalCommitInvariantError, match="real trading"):
        coordinate_v2_canonical_commit(
            _Connection(real_enabled=True),
            account_id="paper-main-v2",
            session_decision=decision,
            reservation=reservation,
            reserve_shared_capacity=callbacks[0],
            mutate_facts=callbacks[1],
            append_evidence=callbacks[2],
            append_transition_outbox=callbacks[3],
            activation_token=_token(),
        )


def test_reservation_must_exactly_bind_batch_before_database_locks(monkeypatch):
    connection = _Connection()
    decision = _decision(monkeypatch)
    wrong = SharedCapacityReservation(
        source_receipt_hash="d" * 64,
        instrument_id="600001.SH",
        snapshot_id="snapshot-1",
        shared_cap_before=100,
        reserved_quantity=50,
        shared_cap_after=50,
        allocations=(("order-b", 30), ("order-a", 20)),
    )
    with pytest.raises(CanonicalCommitInvariantError, match="does not bind"):
        coordinate_v2_canonical_commit(
            connection,
            account_id="paper-main-v2",
            session_decision=decision,
            reservation=wrong,
            reserve_shared_capacity=lambda *_: None,
            mutate_facts=lambda *_: None,
            append_evidence=lambda *_: None,
            append_transition_outbox=lambda *_: None,
            activation_token=_token(),
        )
    assert connection.statements == []
