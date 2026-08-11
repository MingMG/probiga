from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, event, text

from server.trading_v4.infrastructure import (
    ACTIONABLE_OUTPUT_ALLOWED,
    PAPER_BUY_OUTBOX_STATE,
    PRODUCTION_ACTIVATION_ALLOWED,
    RuntimeControlConflictError,
    RuntimeControlHardGateError,
    RuntimeControlIntegrityError,
    RuntimeControlRepository,
    TradingV4Repository,
)
from server.trading_v4.ports import RunStorePort


NOW = datetime(2026, 8, 4, 1, 2, 3, 456789, tzinfo=timezone.utc)


@pytest.fixture()
def engine():
    value = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with value.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE st_runtime_control_v4 (
                    control_key TEXT PRIMARY KEY,
                    control_value_json TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    updated_by TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE st_runtime_control_transition_v4 (
                    transition_id TEXT PRIMARY KEY,
                    control_key TEXT NOT NULL,
                    previous_value_json TEXT,
                    next_value_json TEXT NOT NULL,
                    next_version INTEGER NOT NULL,
                    changed_by TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    event_hash TEXT NOT NULL UNIQUE,
                    changed_at DATETIME NOT NULL,
                    UNIQUE (control_key, next_version)
                )
                """
            )
        )
    try:
        yield value
    finally:
        value.dispose()


def test_runtime_control_create_update_and_exact_transition_history(engine):
    repository = RuntimeControlRepository(engine)
    created = repository.compare_and_set_control(
        "worker_policy",
        expected_version=0,
        next_value={
            "actionable_output_allowed": False,
            "paper_buy_outbox": "closed",
            "batch_size": 100,
        },
        changed_by="stage2-test",
        reason="create frozen worker policy",
        occurred_at=NOW,
    )

    assert created.changed is True
    assert created.superseded is False
    assert created.control.version == 1
    assert created.control.created_at == NOW
    assert created.control.updated_at == NOW
    assert created.transition.previous_value is None
    assert created.transition.next_value == created.control.value
    assert created.transition.next_version == 1
    assert created.transition.transition_id == created.transition.event_hash

    updated = repository.compare_and_set_control(
        "worker_policy",
        expected_version=1,
        next_value={
            "actionable_output_allowed": False,
            "paper_buy_outbox": "closed",
            "batch_size": 80,
        },
        changed_by="stage2-test",
        reason="reduce bounded batch size",
        occurred_at=NOW + timedelta(seconds=1),
    )

    assert updated.changed is True
    assert updated.superseded is False
    assert updated.control.version == 2
    assert updated.control.created_at == NOW
    assert updated.control.updated_at == NOW + timedelta(seconds=1)
    assert updated.transition.previous_value["batch_size"] == 100
    assert updated.transition.next_value["batch_size"] == 80
    assert updated.transition.next_version == 2
    assert repository.get_control("worker_policy") == updated.control

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                "SELECT next_version, previous_value_json, next_value_json "
                "FROM st_runtime_control_transition_v4 "
                "ORDER BY next_version"
            )
        ).mappings().all()
    assert [int(row["next_version"]) for row in rows] == [1, 2]
    assert rows[0]["previous_value_json"] is None
    assert rows[0]["next_value_json"] == (
        '{"actionable_output_allowed":false,"batch_size":100,'
        '"paper_buy_outbox":"closed"}'
    )


def test_runtime_control_writes_transition_before_authoritative_state(engine):
    statements: list[str] = []

    def record_statement(
        _connection,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ):
        normalized = " ".join(str(statement).casefold().split())
        if normalized.startswith(("insert into", "update ")):
            statements.append(normalized)

    event.listen(engine, "before_cursor_execute", record_statement)
    try:
        repository = RuntimeControlRepository(engine)
        repository.compare_and_set_control(
            "ordered-control",
            expected_version=0,
            next_value={"enabled": False, "batch_size": 20},
            changed_by="ordering-test",
            reason="create in guarded order",
            occurred_at=NOW,
        )
        repository.compare_and_set_control(
            "ordered-control",
            expected_version=1,
            next_value={"enabled": False, "batch_size": 10},
            changed_by="ordering-test",
            reason="update in guarded order",
            occurred_at=NOW + timedelta(seconds=1),
        )
    finally:
        event.remove(engine, "before_cursor_execute", record_statement)

    writes = [
        statement
        for statement in statements
        if "st_runtime_control" in statement
    ]
    assert "insert into st_runtime_control_transition_v4" in writes[0]
    assert "insert into st_runtime_control_v4" in writes[1]
    assert "insert into st_runtime_control_transition_v4" in writes[2]
    assert "update st_runtime_control_v4" in writes[3]


def test_runtime_control_exact_command_retry_is_idempotent(engine):
    repository = RuntimeControlRepository(engine)
    arguments = {
        "expected_version": 0,
        "next_value": {"enabled": False, "status": "blocked"},
        "changed_by": "worker-1",
        "reason": "freeze output",
    }
    first = repository.compare_and_set_control(
        "output_gate",
        **arguments,
        occurred_at=NOW,
    )
    replay = repository.compare_and_set_control(
        "output_gate",
        **arguments,
        occurred_at=NOW + timedelta(minutes=5),
    )

    assert first.changed is True
    assert replay.changed is False
    assert replay.superseded is False
    assert replay.control == first.control
    assert replay.transition == first.transition
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM st_runtime_control_transition_v4")
        ).scalar_one() == 1


def test_old_event_hash_replay_is_explicitly_superseded(engine):
    repository = RuntimeControlRepository(engine)
    first_arguments = {
        "expected_version": 0,
        "next_value": {"scan_seconds": 30},
        "changed_by": "worker-1",
        "reason": "initial scan cadence",
    }
    first = repository.compare_and_set_control(
        "scan_policy",
        **first_arguments,
        occurred_at=NOW,
    )
    latest = repository.compare_and_set_control(
        "scan_policy",
        expected_version=1,
        next_value={"scan_seconds": 15},
        changed_by="worker-1",
        reason="increase scan cadence",
        occurred_at=NOW + timedelta(seconds=1),
    )

    replay = repository.compare_and_set_control(
        "scan_policy",
        **first_arguments,
        occurred_at=NOW + timedelta(minutes=5),
    )

    assert replay.changed is False
    assert replay.superseded is True
    assert replay.control == latest.control
    assert replay.control.version == 2
    assert replay.control.value == {"scan_seconds": 15}
    assert replay.transition == first.transition
    assert replay.transition.next_version == 1
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM st_runtime_control_transition_v4")
        ).scalar_one() == 2


def test_runtime_control_results_are_deeply_immutable(engine):
    repository = RuntimeControlRepository(engine)
    result = repository.compare_and_set_control(
        "immutable_value",
        expected_version=0,
        next_value={"nested": {"values": [1, 2]}},
        changed_by="worker-1",
        reason="freeze caller-visible values",
        occurred_at=NOW,
    )

    with pytest.raises(TypeError):
        result.control.value["nested"] = {}  # type: ignore[index]
    with pytest.raises(TypeError):
        result.control.value["nested"]["values"][0] = 9  # type: ignore[index]
    assert result.control.value["nested"]["values"] == (1, 2)
    assert result.transition.next_value["nested"]["values"] == (1, 2)


def test_runtime_control_stale_cas_noop_and_time_regression_fail(engine):
    repository = RuntimeControlRepository(engine)
    repository.compare_and_set_control(
        "scan_policy",
        expected_version=0,
        next_value={"scan_seconds": 30},
        changed_by="owner",
        reason="initial setting",
        occurred_at=NOW,
    )

    with pytest.raises(RuntimeControlConflictError, match="version changed"):
        repository.compare_and_set_control(
            "scan_policy",
            expected_version=0,
            next_value={"scan_seconds": 15},
            changed_by="other-owner",
            reason="stale write",
            occurred_at=NOW + timedelta(seconds=1),
        )
    with pytest.raises(RuntimeControlConflictError, match="no-op"):
        repository.compare_and_set_control(
            "scan_policy",
            expected_version=1,
            next_value={"scan_seconds": 30},
            changed_by="owner",
            reason="different no-op command",
            occurred_at=NOW + timedelta(seconds=1),
        )
    with pytest.raises(RuntimeControlConflictError, match="backwards"):
        repository.compare_and_set_control(
            "scan_policy",
            expected_version=1,
            next_value={"scan_seconds": 45},
            changed_by="owner",
            reason="late stale clock",
            occurred_at=NOW - timedelta(microseconds=1),
        )

    current = repository.get_control("scan_policy")
    assert current is not None
    assert current.version == 1
    assert current.value == {"scan_seconds": 30}


@pytest.mark.parametrize(
    ("control_key", "value"),
    (
        ("production_activation_allowed", True),
        ("actionable-output-allowed", {"enabled": True}),
        ("v4_paper_outbox_enabled", 1),
        ("paper_buy_outbox", "open"),
        ("safety", {"nested": {"real_trading_enabled": True}}),
        ("safety", {"nested": {"paper_buy_outbox": {"status": "open"}}}),
        ("safety", {"nested": {"production_enabled": "production"}}),
    ),
)
def test_runtime_control_cannot_relax_permanent_gates(
    engine,
    control_key,
    value,
):
    repository = RuntimeControlRepository(engine)
    with pytest.raises(RuntimeControlHardGateError):
        repository.compare_and_set_control(
            control_key,
            expected_version=0,
            next_value=value,
            changed_by="operator",
            reason="attempt forbidden activation",
            occurred_at=NOW,
        )

    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM st_runtime_control_v4")
        ).scalar_one() == 0


@pytest.mark.parametrize(
    ("control_key", "value"),
    (
        ("ProductionActivationAllowed", True),
        ("PRODUCTION-ACTIVATION-ALLOWED", "on"),
        ("production_activation_enabled", 1),
        ("ProductionActivationEnabled", {"status": "open"}),
        ("ActionableOutputAllowed", True),
        ("ACTIONABLE-OUTPUT-ENABLED", {"enabled": True}),
        ("actionable_output_enabled", "enabled"),
        ("PaperBuyOutboxOpen", True),
        ("PAPER-BUY-OUTBOX-OPEN", "open"),
        ("paper_buy_outbox_open", {"value": 1}),
        ("V4ProductionActivationEnabled", True),
        ("V4-PAPER-OUTBOX-OPEN", True),
    ),
)
def test_runtime_control_rejects_every_canonical_top_level_gate_alias(
    engine,
    control_key,
    value,
):
    repository = RuntimeControlRepository(engine)

    with pytest.raises(RuntimeControlHardGateError):
        repository.compare_and_set_control(
            control_key,
            expected_version=0,
            next_value=value,
            changed_by="operator",
            reason="canonical alias bypass attempt",
            occurred_at=NOW,
        )


@pytest.mark.parametrize(
    "value",
    (
        {"nested": {"ProductionActivationAllowed": True}},
        {"nested": {"PRODUCTION-ACTIVATION-ENABLED": "on"}},
        {"nested": {"actionable_output_enabled": 1}},
        {"nested": {"Actionable-Output_Allowed": {"status": "open"}}},
        {"nested": [{"PaperBuyOutboxOpen": True}]},
        {"nested": [{"PAPER-BUY-OUTBOX-OPEN": "open"}]},
    ),
)
def test_runtime_control_rejects_nested_case_and_separator_aliases(engine, value):
    repository = RuntimeControlRepository(engine)

    with pytest.raises(RuntimeControlHardGateError):
        repository.compare_and_set_control(
            "worker_policy",
            expected_version=0,
            next_value=value,
            changed_by="operator",
            reason="nested canonical alias bypass attempt",
            occurred_at=NOW,
        )


def test_runtime_control_closed_alias_matching_does_not_reject_metadata(engine):
    repository = RuntimeControlRepository(engine)
    metadata = {
        "production_activation_allowed_note": True,
        "actionable-output-enabled-description": "enabled",
        "paper_buy_outbox_open_reason": "open",
        "nested": {
            "allowed": True,
            "enabled": True,
            "open": True,
            "state": "production",
        },
    }

    result = repository.compare_and_set_control(
        "strategy_metadata",
        expected_version=0,
        next_value=metadata,
        changed_by="operator",
        reason="persist non-executable descriptive metadata",
        occurred_at=NOW,
    )

    assert result.changed is True
    assert result.superseded is False
    assert result.control.value == metadata


def test_disabled_gate_can_carry_truthy_non_indicator_metadata(engine):
    repository = RuntimeControlRepository(engine)

    result = repository.compare_and_set_control(
        "safety",
        expected_version=0,
        next_value={
            "Production-Activation-Allowed": {
                "value": False,
                "reason": "open incident remains under review",
                "metadata": {"enabled": True, "ticket_count": 2},
            },
            "PaperBuyOutboxOpen": {
                "status": "closed",
                "reason": "open follow-up ticket",
            },
            "ActionableOutputEnabled": {
                "enabled": False,
                "reason": "enabled is a field name in the operator guide",
            },
        },
        changed_by="operator",
        reason="retain closed gates with descriptive metadata",
        occurred_at=NOW,
    )

    assert result.changed is True
    assert result.control.version == 1


@pytest.mark.parametrize(
    ("control_key", "value"),
    (
        ("ProductionActivationEnabled", False),
        ("production-activation-allowed", {"allowed": 0}),
        (
            "ActionableOutputEnabled",
            {"enabled": False, "reason": "live rollout remains prohibited"},
        ),
        (
            "paper_buy_outbox_open",
            {"open": False, "metadata": {"open_ticket_count": 3}},
        ),
    ),
)
def test_canonical_gate_aliases_accept_only_explicit_disabled_values(
    engine,
    control_key,
    value,
):
    repository = RuntimeControlRepository(engine)

    result = repository.compare_and_set_control(
        control_key,
        expected_version=0,
        next_value=value,
        changed_by="operator",
        reason="persist canonical hard gate as disabled",
        occurred_at=NOW,
    )

    assert result.changed is True
    assert result.superseded is False


def test_runtime_control_accepts_only_explicit_closed_gate_values(engine):
    repository = RuntimeControlRepository(engine)
    result = repository.compare_and_set_control(
        "safety",
        expected_version=0,
        next_value={
            "production_activation_allowed": False,
            "actionable_output_allowed": {"value": False, "reason": "frozen"},
            "paper_buy_outbox": {"status": "closed", "reason": "stage gate"},
            "real_trading_enabled": 0,
        },
        changed_by="operator",
        reason="persist permanent gates",
        occurred_at=NOW,
    )
    assert result.control.value["production_activation_allowed"] is False
    assert PRODUCTION_ACTIVATION_ALLOWED is False
    assert ACTIONABLE_OUTPUT_ALLOWED is False
    assert PAPER_BUY_OUTBOX_STATE == "closed"


def test_runtime_control_fails_closed_on_corrupt_or_unsafe_stored_rows(engine):
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO st_runtime_control_v4 ("
                "control_key, control_value_json, version, updated_by, reason, "
                "created_at, updated_at) VALUES ("
                ":key, :value, 1, 'intruder', 'unsafe row', :now, :now)"
            ),
            {
                "key": "production_activation_allowed",
                "value": "true",
                "now": NOW.replace(tzinfo=None),
            },
        )
        connection.execute(
            text(
                "INSERT INTO st_runtime_control_v4 ("
                "control_key, control_value_json, version, updated_by, reason, "
                "created_at, updated_at) VALUES ("
                ":key, :value, 1, 'intruder', 'noncanonical row', :now, :now)"
            ),
            {
                "key": "bad_json",
                "value": '{"b": 2, "a": 1}',
                "now": NOW.replace(tzinfo=None),
            },
        )

    repository = RuntimeControlRepository(engine)
    with pytest.raises(RuntimeControlHardGateError):
        repository.get_control("production_activation_allowed")
    with pytest.raises(RuntimeControlIntegrityError, match="canonical JSON"):
        repository.get_control("bad_json")


@pytest.mark.parametrize(
    ("control_key", "stored_value"),
    (
        ("ProductionActivationEnabled", "true"),
        ("ACTIONABLE-OUTPUT-ALLOWED", '{"status":"open"}'),
        ("PaperBuyOutboxOpen", "1"),
        (
            "worker_policy",
            '{"nested":{"PRODUCTION-ACTIVATION-ALLOWED":true}}',
        ),
    ),
)
def test_runtime_control_fails_closed_on_persisted_canonical_aliases(
    engine,
    control_key,
    stored_value,
):
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO st_runtime_control_v4 ("
                "control_key, control_value_json, version, updated_by, reason, "
                "created_at, updated_at) VALUES ("
                ":key, :value, 1, 'intruder', 'unsafe alias row', :now, :now)"
            ),
            {
                "key": control_key,
                "value": stored_value,
                "now": NOW.replace(tzinfo=None),
            },
        )

    repository = RuntimeControlRepository(engine)
    with pytest.raises(RuntimeControlHardGateError):
        repository.get_control(control_key)


@pytest.mark.parametrize(
    "value",
    (
        {"not": ("a", "json", "array")},
        {1: "non-string-key"},
        {"bad": float("nan")},
        {"bad": float("inf")},
    ),
)
def test_runtime_control_rejects_non_strict_json_before_writing(engine, value):
    repository = RuntimeControlRepository(engine)
    with pytest.raises((TypeError, ValueError)):
        repository.compare_and_set_control(
            "invalid_json",
            expected_version=0,
            next_value=value,
            changed_by="test",
            reason="invalid JSON must fail",
            occurred_at=NOW,
        )


def test_run_store_port_is_control_plane_only(engine):
    repository = TradingV4Repository(engine)
    assert isinstance(repository, RunStorePort)
    assert not hasattr(RunStorePort, "commit")
    assert not hasattr(RunStorePort, "get_bundle")
