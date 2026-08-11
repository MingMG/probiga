from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy.exc import IntegrityError

from server.integrations.v3_execution_projection import (
    ProjectionApplyStatus,
    V3ProjectionApplyResult,
    bind_v3_execution_plan,
    project_execution_result,
)
from server.integrations.v3_execution_projection_outbox import (
    OUTBOX_RUNTIME_ENABLED,
    V3ProjectionBaselineStatus,
    V3ProjectionOutboxAppendStatus,
    V3ProjectionOutboxConflictError,
    V3ProjectionOutboxLease,
    V3ProjectionOutboxSchemaError,
    V3ProjectionWorkerDisabledError,
    V3ProjectionWorkerPorts,
    V3ProjectionWorkerStatus,
    V3_PROJECTION_OUTBOX_DDL,
    append_v3_transition_outbox,
    inspect_legacy_direct_sync,
    lease_v3_projection_outbox,
    projection_from_payload,
    projection_to_payload,
    register_v3_projection_order_baseline,
    requeue_v3_projection_dead_letter,
    require_outbox_replacement_safe,
    run_v3_projection_worker_once,
    validate_v3_projection_outbox_schema,
)
from server.db.migrations_v3 import (
    MIGRATIONS as V3_MIGRATIONS,
    V3_PROJECTION_OUTBOX_MIGRATION_VERSION,
)
from server.integrations.v3_execution_projection_outbox import worker as worker_module
from server.integrations.v3_execution_projection_outbox import schema as schema_module
from server.integrations.v3_execution_projection_outbox.legacy_guard import (
    LegacyDirectSyncStillActiveError,
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
    apply_execution_result_with_receipt,
    new_order_state,
)


NOW = datetime(2026, 8, 3, 2, 0, tzinfo=timezone.utc)


def _runtime_override(monkeypatch):
    monkeypatch.setenv("PROBIGA_RUNTIME_ENVIRONMENT", "TEST")
    return worker_module._issue_trusted_test_runtime_capability(
        test_run_id="v3-outbox-test-run",
        acceptance_report_hash="a" * 64,
    )


def _projection():
    earliest = NOW - timedelta(minutes=1)
    semantics = {
        "account_id": "account-1",
        "decision_id": "decision-1",
        "instrument_id": "600001.SH",
        "side": OrderSide.BUY,
        "quantity": 200,
        "order_type": OrderType.LIMIT,
        "time_in_force": TimeInForce.DAY,
        "earliest_at": earliest,
        "expires_at": NOW + timedelta(hours=1),
        "limit_price": Decimal("10.01"),
        "rule_version": "rules-v1",
        "fee_profile_version": "fees-v1",
        "execution_policy_version": "execution-v1",
    }
    intent = ExecutionIntent(
        intent_id="intent-1",
        created_at=earliest,
        idempotency_key=execution_intent_idempotency_key(**semantics),
        **semantics,
    )
    state = new_order_state(order_id="order-1", intent=intent)
    event_id = "order-1-accepted"
    result = ExecutionResult(
        intent_id=state.intent_id,
        order_id=state.order_id,
        event_id=event_id,
        status=OrderStatus.ACCEPTED,
        occurred_at=NOW,
        received_at=NOW + timedelta(milliseconds=5),
        source_sequence=1,
        idempotency_key=execution_result_idempotency_key(
            order_id=state.order_id,
            event_id=event_id,
        ),
    )
    receipt = apply_execution_result_with_receipt(state, result)
    binding = bind_v3_execution_plan(
        execution_plan_id="plan-1",
        source_intent_id="intent-1",
        source_order_id="order-1",
        bound_at=state.created_at,
    )
    return project_execution_result(binding=binding, transition=receipt)


class _Result:
    def __init__(self, *, row=None, rows=None, rowcount=0, scalar=None):
        self._row = row
        self._rows = rows
        self.rowcount = rowcount
        self._scalar = scalar

    def mappings(self):
        return self

    def first(self):
        return self._row

    def scalar(self):
        return self._scalar

    def __iter__(self):
        if self._rows is not None:
            return iter(self._rows)
        return iter(() if self._row is None else (self._row,))


class _OutboxConnection:
    def __init__(
        self,
        *,
        duplicate_race=False,
        baseline_sequence=None,
        baseline_audit_hash="f" * 64,
        tail_sequence=None,
        insert_rowcount=1,
        corrupt_insert_readback=False,
    ):
        self.stored = None
        self.statements: list[str] = []
        self.duplicate_race = duplicate_race
        self.baseline_sequence = baseline_sequence
        self.baseline_audit_hash = baseline_audit_hash
        self.tail_sequence = tail_sequence
        self.insert_rowcount = insert_rowcount
        self.corrupt_insert_readback = corrupt_insert_readback

    def in_transaction(self):
        return True

    def execute(self, statement, parameters=None):
        sql = " ".join(str(statement).split())
        params = dict(parameters or {})
        self.statements.append(sql)
        if sql.startswith("SELECT baseline_sequence, baseline_audit_hash"):
            return _Result(
                row=(
                    {
                        "baseline_sequence": self.baseline_sequence,
                        "baseline_audit_hash": self.baseline_audit_hash,
                    }
                    if self.baseline_sequence is not None
                    else None
                )
            )
        if sql.startswith("SELECT outbox_id"):
            if isinstance(self.stored, list):
                return _Result(rows=self.stored)
            return _Result(row=self.stored)
        if sql.startswith("SELECT source_sequence FROM"):
            return _Result(
                row=(
                    {"source_sequence": self.tail_sequence}
                    if self.tail_sequence is not None
                    else None
                )
            )
        if sql.startswith("INSERT INTO st_execution_projection_outbox_v2"):
            self.stored = {
                name: params[name]
                for name in (
                    "outbox_id",
                    "projection_id",
                    "projection_payload_hash",
                    "canonical_payload_hash",
                    "payload_json",
                    "source_order_id",
                    "source_transition_id",
                    "source_sequence",
                )
            }
            if self.duplicate_race:
                self.duplicate_race = False
                class _Duplicate(Exception):
                    pass

                duplicate = _Duplicate("duplicate")
                duplicate.args = (1062, "duplicate")
                raise IntegrityError(sql, params, duplicate)
            if self.corrupt_insert_readback:
                self.stored["payload_json"] = "{}"
            return _Result(rowcount=self.insert_rowcount)
        raise AssertionError(sql)


def test_outbox_payload_roundtrip_and_append_are_canonical_and_idempotent():
    projection = _projection()
    payload = projection_to_payload(projection)
    assert payload["state"] == "PAPER_QUEUED"
    connection = _OutboxConnection()
    first = append_v3_transition_outbox(
        connection,
        projection,
        created_at=NOW + timedelta(seconds=1),
    )
    retry = append_v3_transition_outbox(
        connection,
        projection,
        created_at=NOW + timedelta(seconds=2),
    )
    assert first.status == V3ProjectionOutboxAppendStatus.INSERTED
    assert retry.status == V3ProjectionOutboxAppendStatus.IDEMPOTENT
    assert first.outbox_id == retry.outbox_id == projection.projection_id
    assert projection_from_payload(connection.stored["payload_json"]) == projection


def test_outbox_created_at_comparison_normalizes_non_utc_projection_time():
    base = _projection()
    occurred_at = base.occurred_at.astimezone(ZoneInfo("Asia/Shanghai"))
    projection = replace(base, occurred_at=occurred_at)
    connection = _OutboxConnection()

    result = append_v3_transition_outbox(
        connection,
        projection,
        created_at=base.occurred_at + timedelta(seconds=1),
    )

    assert result.status == V3ProjectionOutboxAppendStatus.INSERTED


def test_outbox_append_locks_baseline_first_and_cannot_write_before_it():
    connection = _OutboxConnection(baseline_sequence=5)

    with pytest.raises(
        worker_module.V3ProjectionOutboxError,
        match="does not advance",
    ):
        append_v3_transition_outbox(
            connection,
            _projection(),
            created_at=NOW + timedelta(seconds=1),
        )

    assert connection.statements[0].startswith(
        "SELECT baseline_sequence, baseline_audit_hash"
    )
    assert not any(sql.startswith("INSERT") for sql in connection.statements)


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    (
        ("payload_json", "{}"),
        ("outbox_id", "different-outbox-id"),
        ("source_order_id", "different-order-id"),
        ("source_sequence", 99),
    ),
)
def test_outbox_rejects_any_conflicting_projection_identity(
    field_name,
    replacement,
):
    projection = _projection()
    connection = _OutboxConnection()
    append_v3_transition_outbox(
        connection,
        projection,
        created_at=NOW + timedelta(seconds=1),
    )
    connection.stored[field_name] = replacement
    with pytest.raises(V3ProjectionOutboxConflictError, match="different content"):
        append_v3_transition_outbox(
            connection,
            projection,
            created_at=NOW + timedelta(seconds=2),
        )


def test_outbox_identity_lookup_covers_all_unique_and_natural_keys():
    connection = _OutboxConnection()
    append_v3_transition_outbox(
        connection,
        _projection(),
        created_at=NOW + timedelta(seconds=1),
    )
    lookup = next(
        sql for sql in connection.statements if sql.startswith("SELECT outbox_id")
    )
    assert "outbox_id = :outbox_id" in lookup
    assert "projection_id = :projection_id" in lookup
    assert "source_transition_id = :source_transition_id" in lookup
    assert "source_order_id = :source_order_id" in lookup
    assert "source_sequence = :source_sequence" in lookup


def test_outbox_rejects_split_identity_across_multiple_rows():
    projection = _projection()
    connection = _OutboxConnection()
    append_v3_transition_outbox(
        connection,
        projection,
        created_at=NOW + timedelta(seconds=1),
    )
    winner = dict(connection.stored)
    split_identity = {
        **winner,
        "outbox_id": "different-outbox-id",
        "projection_id": "different-projection-id",
    }
    connection.stored = [winner, split_identity]

    with pytest.raises(
        V3ProjectionOutboxConflictError,
        match="multiple rows",
    ):
        append_v3_transition_outbox(
            connection,
            projection,
            created_at=NOW + timedelta(seconds=2),
        )


def test_concurrent_identical_outbox_insert_reloads_winner_as_idempotent():
    projection = _projection()
    connection = _OutboxConnection(duplicate_race=True)
    result = append_v3_transition_outbox(
        connection,
        projection,
        created_at=NOW + timedelta(seconds=1),
    )
    assert result.status == V3ProjectionOutboxAppendStatus.IDEMPOTENT
    assert sum(
        sql.startswith("SELECT outbox_id") for sql in connection.statements
    ) == 2


@pytest.mark.parametrize(
    ("connection", "error"),
    (
        (_OutboxConnection(insert_rowcount=0), "not durable"),
        (
            _OutboxConnection(corrupt_insert_readback=True),
            "readback differs",
        ),
    ),
)
def test_outbox_insert_requires_exact_durable_readback(connection, error):
    with pytest.raises(worker_module.V3ProjectionOutboxError, match=error):
        append_v3_transition_outbox(
            connection,
            _projection(),
            created_at=NOW + timedelta(seconds=1),
        )


def _lease(projection, *, attempt=1):
    payload_json = "{}"
    return V3ProjectionOutboxLease(
        outbox_sequence=7,
        outbox_id=projection.projection_id,
        projection_id=projection.projection_id,
        projection_payload_hash=projection.payload_hash,
        canonical_payload_hash=hashlib.sha256(payload_json.encode()).hexdigest(),
        payload_json=payload_json,
        source_order_id=projection.source_order_id,
        source_transition_id=projection.source_transition_id,
        source_sequence=projection.source_sequence,
        attempt_count=attempt,
        lease_owner="worker-1",
        lease_token="f" * 64,
        lease_until=NOW.replace(tzinfo=None) + timedelta(seconds=30),
    )


def _ports(events, subscriber):
    @contextmanager
    def outbox_transaction():
        events.append("outbox_begin")
        try:
            yield object()
        except BaseException:
            events.append("outbox_rollback")
            raise
        else:
            events.append("outbox_commit")

    @contextmanager
    def projection_transaction():
        events.append("v3_begin")
        try:
            yield object()
        except BaseException:
            events.append("v3_rollback")
            raise
        else:
            events.append("v3_commit")

    return V3ProjectionWorkerPorts(
        outbox_transaction=outbox_transaction,
        projection_transaction=projection_transaction,
        subscriber=subscriber,
    )


def test_worker_default_runtime_gate_fails_before_opening_a_transaction(
    monkeypatch,
):
    monkeypatch.setenv("PROBIGA_RUNTIME_ENVIRONMENT", "TEST")
    events: list[str] = []

    with pytest.raises(
        V3ProjectionWorkerDisabledError,
        match="trusted TEST/CI capability",
    ):
        run_v3_projection_worker_once(
            _ports(events, lambda *_args, **_kwargs: None),
            worker_id="worker-1",
            now=NOW,
        )

    assert events == []


def test_worker_rejects_production_even_if_runtime_flag_is_misconfigured(
    monkeypatch,
):
    monkeypatch.setenv("PROBIGA_RUNTIME_ENVIRONMENT", "PRODUCTION")
    monkeypatch.setattr(worker_module.legacy_guard, "OUTBOX_RUNTIME_ENABLED", True)
    events: list[str] = []

    with pytest.raises(
        V3ProjectionWorkerDisabledError,
        match="TEST or CI process environment",
    ):
        run_v3_projection_worker_once(
            _ports(events, lambda *_args, **_kwargs: None),
            worker_id="worker-1",
            now=NOW,
        )

    assert events == []


def test_worker_runtime_flag_cannot_bypass_legacy_replacement_gate(monkeypatch):
    monkeypatch.setenv("PROBIGA_RUNTIME_ENVIRONMENT", "TEST")
    monkeypatch.setattr(worker_module.legacy_guard, "OUTBOX_RUNTIME_ENABLED", True)
    events: list[str] = []

    with pytest.raises(
        V3ProjectionWorkerDisabledError,
        match="replacement is not safe",
    ):
        run_v3_projection_worker_once(
            _ports(events, lambda *_args, **_kwargs: None),
            worker_id="worker-1",
            now=NOW,
        )

    assert events == []


def test_worker_test_capability_cannot_be_directly_constructed():
    with pytest.raises(
        V3ProjectionWorkerDisabledError,
        match="trusted in-process issuer",
    ):
        worker_module._V3ProjectionWorkerTestCapability(
            test_run_id="forged",
            acceptance_report_hash="a" * 64,
            environment="TEST",
            issued_at=NOW,
            expires_at=NOW + timedelta(minutes=1),
        )


def test_worker_capability_binds_process_environment_and_system_time(monkeypatch):
    monkeypatch.setattr(worker_module, "_system_utc_now", lambda: NOW)
    capability = _runtime_override(monkeypatch)

    monkeypatch.setenv("PROBIGA_RUNTIME_ENVIRONMENT", "CI")
    with pytest.raises(V3ProjectionWorkerDisabledError, match="does not match"):
        worker_module._require_runtime_enabled(capability)

    monkeypatch.setenv("PROBIGA_RUNTIME_ENVIRONMENT", "TEST")
    monkeypatch.setattr(
        worker_module,
        "_system_utc_now",
        lambda: NOW + timedelta(hours=1),
    )
    with pytest.raises(V3ProjectionWorkerDisabledError, match="not active"):
        worker_module._require_runtime_enabled(capability)


def test_worker_capability_rejects_tampered_issuer_mac(monkeypatch):
    capability = _runtime_override(monkeypatch)
    object.__setattr__(capability, "_issuer_mac", "0" * 64)

    with pytest.raises(V3ProjectionWorkerDisabledError, match="issuer is invalid"):
        worker_module._require_runtime_enabled(capability)


def test_worker_commits_v3_independently_then_acknowledges(monkeypatch):
    projection = _projection()
    lease = _lease(projection)
    events: list[str] = []
    monkeypatch.setattr(worker_module, "lease_v3_projection_outbox", lambda *_a, **_k: lease)
    monkeypatch.setattr(worker_module, "projection_from_payload", lambda _raw: projection)
    monkeypatch.setattr(
        worker_module,
        "_acknowledge_projection",
        lambda *_a, **_k: events.append("ack"),
    )

    def subscriber(_connection, value, *, applied_at):
        events.append("subscriber")
        assert value is projection
        return V3ProjectionApplyResult(
            status=ProjectionApplyStatus.APPLIED,
            projection_id=value.projection_id,
            execution_plan_id=value.execution_plan_id,
            source_sequence=value.source_sequence,
            plan_state=value.state.value,
        )

    result = run_v3_projection_worker_once(
        _ports(events, subscriber),
        worker_id="worker-1",
        now=NOW + timedelta(minutes=1),
        runtime_override=_runtime_override(monkeypatch),
    )
    assert result.status == V3ProjectionWorkerStatus.PUBLISHED
    assert events == [
        "outbox_begin",
        "outbox_commit",
        "v3_begin",
        "subscriber",
        "v3_commit",
        "outbox_begin",
        "ack",
        "outbox_commit",
    ]
    assert result.production_activation_allowed is False


@pytest.mark.parametrize(
    "invalid_receipt",
    (
        "none",
        "status",
        "projection_id",
        "execution_plan_id",
        "source_sequence",
        "plan_state",
    ),
)
def test_worker_rejects_unbound_subscriber_receipt_before_v3_commit_and_ack(
    monkeypatch,
    invalid_receipt,
):
    projection = _projection()
    lease = _lease(projection)
    events: list[str] = []
    monkeypatch.setattr(
        worker_module,
        "lease_v3_projection_outbox",
        lambda *_a, **_k: lease,
    )
    monkeypatch.setattr(
        worker_module,
        "projection_from_payload",
        lambda _raw: projection,
    )
    monkeypatch.setattr(
        worker_module,
        "_acknowledge_projection",
        lambda *_a, **_k: pytest.fail("invalid receipt must never be acknowledged"),
    )
    monkeypatch.setattr(
        worker_module,
        "_record_projection_failure",
        lambda *_a, **_k: (
            events.append("failure_recorded")
            or V3ProjectionWorkerStatus.RETRY_SCHEDULED
        ),
    )
    valid = V3ProjectionApplyResult(
        status=ProjectionApplyStatus.APPLIED,
        projection_id=projection.projection_id,
        execution_plan_id=projection.execution_plan_id,
        source_sequence=projection.source_sequence,
        plan_state=projection.state.value,
    )
    if invalid_receipt == "none":
        receipt = None
    elif invalid_receipt == "status":
        receipt = replace(valid, status="APPLIED")
    elif invalid_receipt == "source_sequence":
        receipt = replace(valid, source_sequence=projection.source_sequence + 1)
    else:
        receipt = replace(valid, **{invalid_receipt: "different"})

    result = run_v3_projection_worker_once(
        _ports(events, lambda *_a, **_k: receipt),
        worker_id="worker-1",
        now=NOW + timedelta(minutes=1),
        runtime_override=_runtime_override(monkeypatch),
    )

    assert result.status == V3ProjectionWorkerStatus.RETRY_SCHEDULED
    assert result.subscriber_result is None
    assert "v3_rollback" in events
    assert "v3_commit" not in events
    assert events[-3:] == ["outbox_begin", "failure_recorded", "outbox_commit"]


class _LeaseConnection:
    def __init__(
        self,
        projection,
        *,
        lease_rowcount=1,
        checkpoint=None,
        baseline=None,
        source_sequence=None,
    ):
        self.projection = projection
        self.lease_rowcount = lease_rowcount
        self.checkpoint = checkpoint
        self.baseline = baseline
        self.source_sequence = source_sequence or projection.source_sequence
        self.statements: list[str] = []

    def in_transaction(self):
        return True

    def execute(self, statement, parameters=None):
        sql = " ".join(str(statement).split())
        self.statements.append(sql)
        if "information_schema.TABLES" in sql:
            return _Result(
                rows=(
                    {
                        "TABLE_NAME": table_name,
                        "ENGINE": "InnoDB",
                        "TABLE_COLLATION": "utf8mb4_bin",
                        "ROW_FORMAT": "Dynamic",
                    }
                    for table_name in schema_module._EXPECTED_COLUMNS
                )
            )
        if "information_schema.COLUMNS" in sql:
            return _Result(
                rows=(
                    {
                        "TABLE_NAME": table_name,
                        "COLUMN_NAME": column_name,
                        "COLUMN_TYPE": details["type"],
                        "IS_NULLABLE": "YES" if details["nullable"] else "NO",
                        "COLUMN_DEFAULT": details["default"],
                        "EXTRA": details["extra"],
                        "COLLATION_NAME": details["collation"] or None,
                    }
                    for table_name, columns in schema_module._EXPECTED_COLUMNS.items()
                    for column_name, details in columns.items()
                )
            )
        if "information_schema.STATISTICS" in sql:
            rows = []
            for table_name, indexes in schema_module._EXPECTED_INDEXES.items():
                for index_name, details in indexes.items():
                    for position, column_name in enumerate(
                        details["columns"],
                        start=1,
                    ):
                        rows.append(
                            {
                                "TABLE_NAME": table_name,
                                "INDEX_NAME": index_name,
                                "NON_UNIQUE": 0 if details["unique"] else 1,
                                "SEQ_IN_INDEX": position,
                                "COLUMN_NAME": column_name,
                                "SUB_PART": None,
                                "INDEX_TYPE": "BTREE",
                                "COLLATION": "A",
                            }
                        )
            return _Result(rows=rows)
        if sql.startswith("SELECT candidate.outbox_sequence"):
            payload_json = "{}"
            baseline = self.baseline or {
                "baseline_sequence": None,
                "baseline_transition_id": None,
                "baseline_order_state_hash": None,
                "reconciliation_evidence_hash": None,
                "baseline_audit_hash": None,
                "baseline_reconciled_by": None,
                "baseline_reconciled_at": None,
            }
            return _Result(
                row={
                    "outbox_sequence": 7,
                    "outbox_id": self.projection.projection_id,
                    "projection_id": self.projection.projection_id,
                    "projection_payload_hash": self.projection.payload_hash,
                    "canonical_payload_hash": hashlib.sha256(
                        payload_json.encode()
                    ).hexdigest(),
                    "payload_json": payload_json,
                    "source_order_id": self.projection.source_order_id,
                    "source_transition_id": self.projection.source_transition_id,
                    "source_sequence": self.source_sequence,
                    "attempt_count": 1,
                    **baseline,
                }
            )
        if sql.startswith("UPDATE st_execution_projection_outbox_v2"):
            return _Result(rowcount=self.lease_rowcount)
        if sql.startswith("SELECT last_outbox_sequence"):
            return _Result(scalar=self.checkpoint)
        if sql.startswith("INSERT INTO st_execution_projection_worker_checkpoint_v3"):
            return _Result(rowcount=1)
        if sql.startswith("UPDATE st_execution_projection_worker_checkpoint_v3"):
            return _Result(rowcount=1)
        raise AssertionError(sql)


def test_lease_reclaims_expired_work_with_cas_and_increments_attempt(monkeypatch):
    projection = _projection()
    connection = _LeaseConnection(projection)
    lease = lease_v3_projection_outbox(
        connection,
        worker_id="worker-1",
        now=NOW + timedelta(minutes=1),
        lease_seconds=30,
        runtime_override=_runtime_override(monkeypatch),
    )
    assert lease is not None
    assert lease.attempt_count == 2
    assert len(lease.lease_token) == 64
    lease_select = next(
        sql
        for sql in connection.statements
        if sql.startswith("SELECT candidate.outbox_sequence")
    )
    lease_update = next(
        sql
        for sql in connection.statements
        if sql.startswith("UPDATE st_execution_projection_outbox_v2")
    )
    assert (
        "candidate.status = 'LEASED' "
        "AND candidate.lease_until <= :now"
    ) in lease_select
    assert "predecessor.status <> 'PUBLISHED'" in lease_select
    assert "predecessor.status IS NULL" in lease_select
    assert "LEFT JOIN st_execution_projection_order_baseline_v3" in lease_select
    assert (
        "candidate.source_sequence = COALESCE(baseline.baseline_sequence, 0) + 1"
        in lease_select
    )
    assert (
        "immediate_predecessor.source_sequence = candidate.source_sequence - 1"
        in lease_select
    )
    assert (
        "immediate_predecessor.status = 'PUBLISHED'"
        in lease_select
    )
    assert "SELECT COUNT(*)" in lease_select
    assert (
        "published_history.source_sequence BETWEEN COALESCE("
        "baseline.baseline_sequence, 0) + 1" in lease_select
    )
    assert "published_history.status = 'PUBLISHED'" in lease_select
    assert (
        ") = candidate.source_sequence - COALESCE("
        "baseline.baseline_sequence, 0) - 1" in lease_select
    )
    assert "DEAD_LETTER" not in lease_select
    assert "lease_token = :lease_token" in lease_update


def _baseline_row(projection, *, audit_hash=None):
    reconciled_at = NOW
    values = {
        "source_order_id": projection.source_order_id,
        "baseline_sequence": 5,
        "baseline_transition_id": "b" * 64,
        "baseline_order_state_hash": "c" * 64,
        "reconciliation_evidence_hash": "d" * 64,
        "reconciled_by": "cutover-auditor",
        "reconciled_at": reconciled_at,
    }
    return {
        "baseline_sequence": values["baseline_sequence"],
        "baseline_transition_id": values["baseline_transition_id"],
        "baseline_order_state_hash": values["baseline_order_state_hash"],
        "reconciliation_evidence_hash": values[
            "reconciliation_evidence_hash"
        ],
        "baseline_audit_hash": audit_hash
        or worker_module._baseline_audit_hash(**values),
        "baseline_reconciled_by": values["reconciled_by"],
        "baseline_reconciled_at": reconciled_at.replace(tzinfo=None),
    }


def test_lease_accepts_exact_audited_baseline_and_never_loosens_order(monkeypatch):
    projection = _projection()
    connection = _LeaseConnection(
        projection,
        baseline=_baseline_row(projection),
        source_sequence=6,
    )

    lease = lease_v3_projection_outbox(
        connection,
        worker_id="worker-1",
        now=NOW + timedelta(minutes=1),
        lease_seconds=30,
        runtime_override=_runtime_override(monkeypatch),
    )

    assert lease is not None and lease.source_sequence == 6
    lease_select = next(
        sql
        for sql in connection.statements
        if sql.startswith("SELECT candidate.outbox_sequence")
    )
    assert "candidate.source_sequence > COALESCE(" in lease_select
    assert (
        "predecessor.source_sequence > COALESCE(baseline.baseline_sequence, 0)"
        in lease_select
    )


def test_lease_rejects_tampered_baseline_before_lease_cas(monkeypatch):
    projection = _projection()
    connection = _LeaseConnection(
        projection,
        baseline=_baseline_row(projection, audit_hash="0" * 64),
        source_sequence=6,
    )

    with pytest.raises(worker_module.V3ProjectionWorkerError, match="audit hash"):
        lease_v3_projection_outbox(
            connection,
            worker_id="worker-1",
            now=NOW + timedelta(minutes=1),
            lease_seconds=30,
            runtime_override=_runtime_override(monkeypatch),
        )

    assert not any(
        sql.startswith("UPDATE st_execution_projection_outbox_v2")
        for sql in connection.statements
    )


def test_lease_fails_closed_on_schema_drift_before_candidate_query(monkeypatch):
    connection = _LeaseConnection(_projection())

    def reject_schema(_connection):
        raise V3ProjectionOutboxSchemaError("schema contract drifted")

    monkeypatch.setattr(
        worker_module,
        "validate_v3_projection_outbox_schema",
        reject_schema,
    )

    with pytest.raises(V3ProjectionOutboxSchemaError, match="drifted"):
        lease_v3_projection_outbox(
            connection,
            worker_id="worker-1",
            now=NOW + timedelta(minutes=1),
            lease_seconds=30,
            runtime_override=_runtime_override(monkeypatch),
        )

    assert not any(
        sql.startswith("SELECT candidate.outbox_sequence")
        for sql in connection.statements
    )


def test_lease_and_acknowledgement_cas_fail_closed(monkeypatch):
    projection = _projection()
    with pytest.raises(worker_module.V3ProjectionWorkerError, match="lease CAS"):
        lease_v3_projection_outbox(
            _LeaseConnection(projection, lease_rowcount=0),
            worker_id="worker-1",
            now=NOW + timedelta(minutes=1),
            lease_seconds=30,
            runtime_override=_runtime_override(monkeypatch),
        )
    lease = _lease(projection)
    connection = _LeaseConnection(projection, lease_rowcount=0)
    with pytest.raises(
        worker_module.V3ProjectionWorkerError,
        match="acknowledgement CAS",
    ):
        worker_module._acknowledge_projection(
            connection,
            lease,
            published_at=NOW + timedelta(minutes=1),
        )


def test_acknowledgement_and_checkpoint_advance_in_one_worker_transaction():
    projection = _projection()
    lease = _lease(projection)
    connection = _LeaseConnection(projection)
    worker_module._acknowledge_projection(
        connection,
        lease,
        published_at=NOW + timedelta(minutes=1),
    )
    assert connection.statements[0].startswith(
        "UPDATE st_execution_projection_outbox_v2"
    )
    assert connection.statements[1].startswith("SELECT last_outbox_sequence")
    assert connection.statements[2].startswith(
        "INSERT INTO st_execution_projection_worker_checkpoint_v3"
    )


@pytest.mark.parametrize(
    ("attempt", "expected"),
    (
        (1, V3ProjectionWorkerStatus.RETRY_SCHEDULED),
        (3, V3ProjectionWorkerStatus.DEAD_LETTER),
    ),
)
def test_v3_failure_rolls_back_only_worker_tx_and_schedules_retry_or_deadletter(
    monkeypatch,
    attempt,
    expected,
):
    projection = _projection()
    lease = _lease(projection, attempt=attempt)
    events: list[str] = []
    monkeypatch.setattr(worker_module, "lease_v3_projection_outbox", lambda *_a, **_k: lease)
    monkeypatch.setattr(worker_module, "projection_from_payload", lambda _raw: projection)
    monkeypatch.setattr(
        worker_module,
        "_record_projection_failure",
        lambda *_a, **_k: events.append("failure_recorded") or expected,
    )

    def subscriber(*_args, **_kwargs):
        events.append("subscriber")
        raise RuntimeError("V3 unavailable")

    result = run_v3_projection_worker_once(
        _ports(events, subscriber),
        worker_id="worker-1",
        now=NOW + timedelta(minutes=1),
        max_attempts=3,
        runtime_override=_runtime_override(monkeypatch),
    )
    assert result.status == expected
    assert "v3_rollback" in events
    assert events[-3:] == ["outbox_begin", "failure_recorded", "outbox_commit"]
    assert "ack" not in events


def test_worker_crash_after_v3_commit_recovers_by_idempotent_replay(monkeypatch):
    projection = _projection()
    leases = iter((_lease(projection, attempt=1), _lease(projection, attempt=2)))
    events: list[str] = []
    monkeypatch.setattr(worker_module, "lease_v3_projection_outbox", lambda *_a, **_k: next(leases))
    monkeypatch.setattr(worker_module, "projection_from_payload", lambda _raw: projection)
    monkeypatch.setattr(
        worker_module,
        "_acknowledge_projection",
        lambda *_a, **_k: events.append("ack"),
    )
    apply_count = 0

    def subscriber(_connection, value, *, applied_at):
        nonlocal apply_count
        apply_count += 1
        status = (
            ProjectionApplyStatus.APPLIED
            if apply_count == 1
            else ProjectionApplyStatus.IDEMPOTENT
        )
        return V3ProjectionApplyResult(
            status=status,
            projection_id=value.projection_id,
            execution_plan_id=value.execution_plan_id,
            source_sequence=value.source_sequence,
            plan_state=value.state.value,
        )

    def crash(_lease):
        events.append("crash")
        raise RuntimeError("worker crashed")

    ports = _ports(events, subscriber)
    with pytest.raises(RuntimeError, match="worker crashed"):
        run_v3_projection_worker_once(
            ports,
            worker_id="worker-1",
            now=NOW + timedelta(minutes=1),
            after_projection_commit=crash,
            runtime_override=_runtime_override(monkeypatch),
        )
    assert events[-2:] == ["v3_commit", "crash"]
    recovered = run_v3_projection_worker_once(
        ports,
        worker_id="worker-1",
        now=NOW + timedelta(minutes=2),
        runtime_override=_runtime_override(monkeypatch),
    )
    assert recovered.status == V3ProjectionWorkerStatus.PUBLISHED
    assert recovered.subscriber_result.status == ProjectionApplyStatus.IDEMPOTENT
    assert apply_count == 2
    assert events[-3:] == ["outbox_begin", "ack", "outbox_commit"]


class _BaselineConnection:
    def __init__(
        self,
        *,
        existing_outbox=False,
        insert_rowcount=1,
        corrupt_readback=False,
    ):
        self.stored = None
        self.existing_outbox = existing_outbox
        self.insert_rowcount = insert_rowcount
        self.corrupt_readback = corrupt_readback
        self.statements: list[str] = []

    def in_transaction(self):
        return True

    def execute(self, statement, parameters=None):
        sql = " ".join(str(statement).split())
        parameters = dict(parameters or {})
        self.statements.append(sql)
        if sql.startswith("SELECT source_order_id, baseline_sequence"):
            return _Result(row=self.stored)
        if sql.startswith("SELECT outbox_id, source_sequence, status"):
            return _Result(
                row=(
                    {
                        "outbox_id": "e" * 64,
                        "source_sequence": 1,
                        "status": "DEAD_LETTER",
                    }
                    if self.existing_outbox
                    else None
                )
            )
        if sql.startswith(
            "INSERT INTO st_execution_projection_order_baseline_v3"
        ):
            if self.insert_rowcount == 1:
                self.stored = dict(parameters)
                if self.corrupt_readback:
                    self.stored["baseline_audit_hash"] = "0" * 64
            return _Result(rowcount=self.insert_rowcount)
        raise AssertionError(sql)


def test_existing_order_baseline_is_audited_immutable_and_idempotent(
    monkeypatch,
):
    monkeypatch.setattr(
        worker_module,
        "validate_v3_projection_outbox_schema",
        lambda _connection: None,
    )
    connection = _BaselineConnection()
    capability = _runtime_override(monkeypatch)
    arguments = {
        "source_order_id": "order-legacy",
        "baseline_sequence": 5,
        "baseline_transition_id": "b" * 64,
        "baseline_order_state_hash": "c" * 64,
        "reconciliation_evidence_hash": "d" * 64,
        "reconciled_by": "cutover-auditor",
        "reconciled_at": NOW,
        "runtime_override": capability,
    }

    inserted = register_v3_projection_order_baseline(connection, **arguments)
    replay = register_v3_projection_order_baseline(connection, **arguments)

    assert inserted.status == V3ProjectionBaselineStatus.INSERTED
    assert replay.status == V3ProjectionBaselineStatus.IDEMPOTENT
    assert inserted.baseline_audit_hash == replay.baseline_audit_hash
    assert len(inserted.baseline_audit_hash) == 64
    assert connection.stored["baseline_audit_hash"] == inserted.baseline_audit_hash
    with pytest.raises(worker_module.V3ProjectionWorkerError, match="different content"):
        register_v3_projection_order_baseline(
            connection,
            **{**arguments, "baseline_sequence": 6},
        )


def test_baseline_and_append_use_the_same_baseline_then_outbox_lock_order(
    monkeypatch,
):
    append_connection = _OutboxConnection()
    append_v3_transition_outbox(
        append_connection,
        _projection(),
        created_at=NOW + timedelta(seconds=1),
    )
    assert append_connection.statements[0].startswith(
        "SELECT baseline_sequence, baseline_audit_hash"
    )
    assert append_connection.statements[0].endswith("FOR UPDATE")
    tail_lock = next(
        sql
        for sql in append_connection.statements
        if sql.startswith("SELECT source_sequence FROM")
    )
    assert "ORDER BY source_sequence DESC LIMIT 1 FOR UPDATE" in tail_lock

    monkeypatch.setattr(
        worker_module,
        "validate_v3_projection_outbox_schema",
        lambda _connection: None,
    )
    baseline_connection = _BaselineConnection()
    register_v3_projection_order_baseline(
        baseline_connection,
        source_order_id="order-legacy",
        baseline_sequence=5,
        baseline_transition_id="b" * 64,
        baseline_order_state_hash="c" * 64,
        reconciliation_evidence_hash="d" * 64,
        reconciled_by="cutover-auditor",
        reconciled_at=NOW,
        runtime_override=_runtime_override(monkeypatch),
    )
    assert baseline_connection.statements[0].startswith(
        "SELECT source_order_id, baseline_sequence"
    )
    assert baseline_connection.statements[0].endswith("FOR UPDATE")
    range_lock = baseline_connection.statements[1]
    assert "WHERE source_order_id = :source_order_id" in range_lock
    assert "ORDER BY source_sequence LIMIT 1 FOR UPDATE" in range_lock


def test_existing_order_baseline_cannot_skip_an_outbox_predecessor(monkeypatch):
    monkeypatch.setattr(
        worker_module,
        "validate_v3_projection_outbox_schema",
        lambda _connection: None,
    )
    connection = _BaselineConnection(existing_outbox=True)
    with pytest.raises(worker_module.V3ProjectionWorkerError, match="cannot skip"):
        register_v3_projection_order_baseline(
            connection,
            source_order_id="order-legacy",
            baseline_sequence=5,
            baseline_transition_id="b" * 64,
            baseline_order_state_hash="c" * 64,
            reconciliation_evidence_hash="d" * 64,
            reconciled_by="cutover-auditor",
            reconciled_at=NOW,
            runtime_override=_runtime_override(monkeypatch),
        )
    assert not any(sql.startswith("INSERT") for sql in connection.statements)


@pytest.mark.parametrize(
    ("connection", "error"),
    (
        (_BaselineConnection(insert_rowcount=0), "not durable"),
        (_BaselineConnection(corrupt_readback=True), "readback differs"),
    ),
)
def test_baseline_requires_exact_durable_insert_receipt(
    monkeypatch,
    connection,
    error,
):
    monkeypatch.setattr(
        worker_module,
        "validate_v3_projection_outbox_schema",
        lambda _connection: None,
    )
    with pytest.raises(worker_module.V3ProjectionWorkerError, match=error):
        register_v3_projection_order_baseline(
            connection,
            source_order_id="order-legacy",
            baseline_sequence=5,
            baseline_transition_id="b" * 64,
            baseline_order_state_hash="c" * 64,
            reconciliation_evidence_hash="d" * 64,
            reconciled_by="cutover-auditor",
            reconciled_at=NOW,
            runtime_override=_runtime_override(monkeypatch),
        )


def test_idempotent_baseline_rejects_later_history_at_or_below_cutover(
    monkeypatch,
):
    monkeypatch.setattr(
        worker_module,
        "validate_v3_projection_outbox_schema",
        lambda _connection: None,
    )
    connection = _BaselineConnection()
    arguments = {
        "source_order_id": "order-legacy",
        "baseline_sequence": 5,
        "baseline_transition_id": "b" * 64,
        "baseline_order_state_hash": "c" * 64,
        "reconciliation_evidence_hash": "d" * 64,
        "reconciled_by": "cutover-auditor",
        "reconciled_at": NOW,
        "runtime_override": _runtime_override(monkeypatch),
    }
    register_v3_projection_order_baseline(connection, **arguments)
    connection.existing_outbox = True
    with pytest.raises(worker_module.V3ProjectionWorkerError, match="polluted"):
        register_v3_projection_order_baseline(connection, **arguments)


class _RequeueConnection:
    def __init__(
        self,
        *,
        status="DEAD_LETTER",
        update_rowcount=1,
        audit_insert_rowcount=1,
        corrupt_audit_readback=False,
    ):
        self.status = status
        self.update_rowcount = update_rowcount
        self.audit_insert_rowcount = audit_insert_rowcount
        self.corrupt_audit_readback = corrupt_audit_readback
        self.statements: list[str] = []
        self.audit_parameters = None

    def in_transaction(self):
        return True

    def execute(self, statement, parameters=None):
        sql = " ".join(str(statement).split())
        parameters = dict(parameters or {})
        self.statements.append(sql)
        if sql.startswith("SELECT outbox_id, source_order_id"):
            return _Result(
                row={
                    "outbox_id": "e" * 64,
                    "source_order_id": "order-1",
                    "source_sequence": 3,
                    "status": self.status,
                    "attempt_count": 5,
                }
            )
        if sql.startswith("UPDATE st_execution_projection_outbox_v2"):
            return _Result(rowcount=self.update_rowcount)
        if "dead_letter_reconciliation_v3" in sql and sql.startswith("INSERT"):
            if self.audit_insert_rowcount == 1:
                self.audit_parameters = dict(parameters)
                if self.corrupt_audit_readback:
                    self.audit_parameters["reason"] = "different"
            return _Result(rowcount=self.audit_insert_rowcount)
        if sql.startswith("SELECT reconciliation_audit_hash"):
            return _Result(row=self.audit_parameters)
        raise AssertionError(sql)


def test_dead_letter_requeue_is_capability_gated_audited_and_does_not_skip(
    monkeypatch,
):
    monkeypatch.setattr(
        worker_module,
        "validate_v3_projection_outbox_schema",
        lambda _connection: None,
    )
    connection = _RequeueConnection()
    monkeypatch.setenv("PROBIGA_RUNTIME_ENVIRONMENT", "TEST")
    with pytest.raises(V3ProjectionWorkerDisabledError, match="trusted TEST/CI"):
        requeue_v3_projection_dead_letter(
            connection,
            outbox_id="e" * 64,
            reason="operator verified V3 was unavailable",
            reconciled_by="operator-1",
            reconciled_at=NOW,
        )
    assert connection.statements == []

    result = requeue_v3_projection_dead_letter(
        connection,
        outbox_id="e" * 64,
        reason="operator verified V3 was unavailable",
        reconciled_by="operator-1",
        reconciled_at=NOW,
        runtime_override=_runtime_override(monkeypatch),
    )

    assert result.previous_attempt_count == 5
    assert result.source_sequence == 3
    assert len(result.reconciliation_audit_hash) == 64
    update = next(
        sql
        for sql in connection.statements
        if sql.startswith("UPDATE st_execution_projection_outbox_v2")
    )
    assert "WHERE outbox_id = :outbox_id AND status = 'DEAD_LETTER'" in update
    assert "attempt_count = :previous_attempt_count" in update
    assert "source_sequence" not in update
    assert "PUBLISHED" not in update
    assert connection.audit_parameters["action"] == "REQUEUE"
    assert (
        connection.audit_parameters["reconciliation_audit_hash"]
        == result.reconciliation_audit_hash
    )


@pytest.mark.parametrize(
    ("status", "rowcount", "error"),
    (
        ("PENDING", 1, "only a DEAD_LETTER"),
        ("DEAD_LETTER", 0, "requeue CAS"),
    ),
)
def test_dead_letter_requeue_fails_closed_on_state_or_cas_drift(
    monkeypatch,
    status,
    rowcount,
    error,
):
    monkeypatch.setattr(
        worker_module,
        "validate_v3_projection_outbox_schema",
        lambda _connection: None,
    )
    connection = _RequeueConnection(status=status, update_rowcount=rowcount)
    with pytest.raises(worker_module.V3ProjectionWorkerError, match=error):
        requeue_v3_projection_dead_letter(
            connection,
            outbox_id="e" * 64,
            reason="retry remains ordered",
            reconciled_by="operator-1",
            reconciled_at=NOW,
            runtime_override=_runtime_override(monkeypatch),
        )
    assert connection.audit_parameters is None


@pytest.mark.parametrize(
    ("connection", "error"),
    (
        (_RequeueConnection(audit_insert_rowcount=0), "not durable"),
        (
            _RequeueConnection(corrupt_audit_readback=True),
            "readback differs",
        ),
    ),
)
def test_dead_letter_requeue_requires_exact_durable_audit_receipt(
    monkeypatch,
    connection,
    error,
):
    monkeypatch.setattr(
        worker_module,
        "validate_v3_projection_outbox_schema",
        lambda _connection: None,
    )
    with pytest.raises(worker_module.V3ProjectionWorkerError, match=error):
        requeue_v3_projection_dead_letter(
            connection,
            outbox_id="e" * 64,
            reason="retry remains ordered",
            reconciled_by="operator-1",
            reconciled_at=NOW,
            runtime_override=_runtime_override(monkeypatch),
        )


class _OutboxSchemaConnection:
    def __init__(
        self,
        *,
        missing_index: str | None = None,
        prefix_index: str | None = None,
        bad_engine_table: str | None = None,
        bad_row_format_table: str | None = None,
        extra_index: bool = False,
        missing_column: tuple[str, str] | None = None,
        column_override: tuple[str, str, str, object] | None = None,
    ):
        self.missing_index = missing_index
        self.prefix_index = prefix_index
        self.bad_engine_table = bad_engine_table
        self.bad_row_format_table = bad_row_format_table
        self.extra_index = extra_index
        self.missing_column = missing_column
        self.column_override = column_override

    def execute(self, statement):
        sql = " ".join(str(statement).split())
        if "information_schema.TABLES" in sql:
            return _Result(
                rows=(
                    {
                        "TABLE_NAME": table_name,
                        "ENGINE": (
                            "MyISAM"
                            if table_name == self.bad_engine_table
                            else "InnoDB"
                        ),
                        "TABLE_COLLATION": "utf8mb4_bin",
                        "ROW_FORMAT": (
                            "Compact"
                            if table_name == self.bad_row_format_table
                            else "Dynamic"
                        ),
                    }
                    for table_name in schema_module._EXPECTED_COLUMNS
                )
            )
        if "information_schema.COLUMNS" in sql:
            rows = []
            for table_name, columns in schema_module._EXPECTED_COLUMNS.items():
                for column_name, details in columns.items():
                    if self.missing_column == (table_name, column_name):
                        continue
                    row = {
                        "TABLE_NAME": table_name,
                        "COLUMN_NAME": column_name,
                        "COLUMN_TYPE": details["type"],
                        "IS_NULLABLE": "YES" if details["nullable"] else "NO",
                        "COLUMN_DEFAULT": details["default"],
                        "EXTRA": details["extra"],
                        "COLLATION_NAME": details["collation"] or None,
                    }
                    if (
                        self.column_override is not None
                        and self.column_override[:2] == (table_name, column_name)
                    ):
                        field_name, value = self.column_override[2:]
                        row[field_name] = value
                    rows.append(row)
            return _Result(rows=rows)
        assert "information_schema.STATISTICS" in sql
        rows = []
        for table_name, indexes in schema_module._EXPECTED_INDEXES.items():
            for index_name, details in indexes.items():
                if index_name == self.missing_index:
                    continue
                for position, column_name in enumerate(
                    details["columns"],
                    start=1,
                ):
                    rows.append(
                        {
                            "TABLE_NAME": table_name,
                            "INDEX_NAME": index_name,
                            "NON_UNIQUE": 0 if details["unique"] else 1,
                            "SEQ_IN_INDEX": position,
                            "COLUMN_NAME": column_name,
                            "SUB_PART": (
                                1
                                if index_name == self.prefix_index
                                and position == 1
                                else None
                            ),
                            "INDEX_TYPE": "BTREE",
                            "COLLATION": "A",
                        }
                    )
        if self.extra_index:
            rows.append(
                {
                    "TABLE_NAME": schema_module._OUTBOX_TABLE,
                    "INDEX_NAME": "idx_unreviewed_outbox",
                    "NON_UNIQUE": 1,
                    "SEQ_IN_INDEX": 1,
                    "COLUMN_NAME": "created_at",
                    "SUB_PART": None,
                    "INDEX_TYPE": "BTREE",
                    "COLLATION": "A",
                }
            )
        return _Result(rows=rows)


def test_outbox_schema_requires_per_order_sequence_uniqueness():
    ddl = "\n".join(V3_PROJECTION_OUTBOX_DDL)
    assert "UNIQUE KEY uk_v3_projection_outbox_order_sequence" in ddl
    assert "(source_order_id, source_sequence)" in ddl

    validate_v3_projection_outbox_schema(_OutboxSchemaConnection())
    with pytest.raises(V3ProjectionOutboxSchemaError, match="contract drifted"):
        validate_v3_projection_outbox_schema(
            _OutboxSchemaConnection(
                missing_index="uk_v3_projection_outbox_order_sequence"
            )
        )


@pytest.mark.parametrize(
    "connection",
    (
        _OutboxSchemaConnection(
            missing_column=(schema_module._OUTBOX_TABLE, "status")
        ),
        _OutboxSchemaConnection(
            column_override=(
                schema_module._OUTBOX_TABLE,
                "status",
                "COLUMN_TYPE",
                "varchar(1)",
            )
        ),
        _OutboxSchemaConnection(
            column_override=(
                schema_module._OUTBOX_TABLE,
                "status",
                "IS_NULLABLE",
                "YES",
            )
        ),
        _OutboxSchemaConnection(
            column_override=(
                schema_module._OUTBOX_TABLE,
                "attempt_count",
                "COLUMN_DEFAULT",
                "7",
            )
        ),
        _OutboxSchemaConnection(
            column_override=(
                schema_module._OUTBOX_TABLE,
                "outbox_sequence",
                "EXTRA",
                "",
            )
        ),
        _OutboxSchemaConnection(
            column_override=(
                schema_module._OUTBOX_TABLE,
                "source_order_id",
                "COLLATION_NAME",
                "utf8mb4_general_ci",
            )
        ),
        _OutboxSchemaConnection(
            column_override=(
                schema_module._OUTBOX_TABLE,
                "created_at",
                "COLUMN_TYPE",
                "datetime",
            )
        ),
        _OutboxSchemaConnection(
            bad_row_format_table=schema_module._BASELINE_TABLE
        ),
    ),
)
def test_outbox_schema_rejects_column_precision_default_and_row_format_drift(
    connection,
):
    with pytest.raises(V3ProjectionOutboxSchemaError, match="drifted"):
        validate_v3_projection_outbox_schema(connection)


@pytest.mark.parametrize(
    "connection",
    (
        _OutboxSchemaConnection(
            prefix_index="uk_v3_projection_outbox_order_sequence"
        ),
        _OutboxSchemaConnection(
            bad_engine_table=schema_module._OUTBOX_TABLE
        ),
        _OutboxSchemaConnection(extra_index=True),
    ),
)
def test_outbox_schema_rejects_prefix_engine_and_extra_index_drift(connection):
    with pytest.raises(V3ProjectionOutboxSchemaError, match="drifted"):
        validate_v3_projection_outbox_schema(connection)


def test_outbox_schema_is_append_only_isolated_and_not_runtime_enabled():
    sql = "\n".join(V3_PROJECTION_OUTBOX_DDL)
    assert "st_execution_projection_outbox_v2" in sql
    assert "st_execution_projection_worker_checkpoint_v3" in sql
    assert "st_execution_projection_order_baseline_v3" in sql
    assert "baseline_sequence BIGINT NOT NULL" in sql
    assert "baseline_audit_hash CHAR(64) NOT NULL" in sql
    assert "reconciliation_evidence_hash CHAR(64) NOT NULL" in sql
    assert "st_execution_projection_dead_letter_reconciliation_v3" in sql
    assert "reconciliation_audit_hash CHAR(64) PRIMARY KEY" in sql
    assert sql.count("ROW_FORMAT=DYNAMIC") == 4
    assert "lease_token" in sql and "DEAD_LETTER" not in sql
    assert "st_trade_account_v2" not in sql
    assert "st_order_v2" not in sql
    assert "st_position_lot_v2" not in sql
    assert OUTBOX_RUNTIME_ENABLED is False


def test_outbox_schema_is_registered_as_one_forward_only_v3_migration():
    migration = next(
        item
        for item in V3_MIGRATIONS
        if item["version"] == V3_PROJECTION_OUTBOX_MIGRATION_VERSION
    )
    assert migration is V3_MIGRATIONS[-1]
    assert tuple(migration["statements"]) == tuple(V3_PROJECTION_OUTBOX_DDL)
    sql = "\n".join(migration["statements"])
    assert sql.count("CREATE TABLE IF NOT EXISTS") == 4
    for forbidden in (
        "st_trade_account_v2",
        "st_trade_intent_v2",
        "st_order_v2",
        "st_fill_v2",
        "st_cash_event_v2",
        "st_position_lot_v2",
        "st_risk_decision_v2",
    ):
        assert forbidden not in sql


def test_legacy_direct_sync_is_detected_and_replacement_activation_blocked():
    status = inspect_legacy_direct_sync()
    assert status.function_definitions == 1
    assert status.import_sites >= 1
    assert status.direct_call_sites >= 3
    assert any(
        path.endswith("server\\trading_v3\\paper_execution.py")
        or path.endswith("server/trading_v3/paper_execution.py")
        for path in status.referenced_files
    )
    assert status.outbox_runtime_enabled is False
    assert status.production_activation_allowed is False
    with pytest.raises(LegacyDirectSyncStillActiveError, match="still present"):
        require_outbox_replacement_safe()


def test_legacy_guard_scans_import_aliases_and_module_attribute_calls(tmp_path):
    (tmp_path / "definition.py").write_text(
        "def _sync_v3_execution_plan_states():\n    return None\n",
        encoding="utf-8",
    )
    (tmp_path / "alias.py").write_text(
        "from pkg import _sync_v3_execution_plan_states as sync\n"
        "sync()\n",
        encoding="utf-8",
    )
    (tmp_path / "attribute.py").write_text(
        "import pkg\npkg._sync_v3_execution_plan_states()\n",
        encoding="utf-8",
    )

    status = inspect_legacy_direct_sync(tmp_path)

    assert status.function_definitions == 1
    assert status.import_sites == 1
    assert status.direct_call_sites == 2
    assert len(status.referenced_files) == 3


def test_new_boundaries_do_not_modify_legacy_execution_or_v2_migrations():
    root = Path(__file__).resolve().parents[1]
    coordinator = (root / "server/integrations/v2_canonical_commit/coordinator.py").read_text(encoding="utf-8")
    worker = (root / "server/integrations/v3_execution_projection_outbox/worker.py").read_text(encoding="utf-8")
    assert "create_engine" not in coordinator
    assert ".commit(" not in coordinator
    assert "run_execution_tick" not in coordinator
    assert "server.db.migrations_v2" not in worker
