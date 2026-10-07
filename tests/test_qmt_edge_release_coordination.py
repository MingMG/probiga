from __future__ import annotations

from datetime import datetime, timedelta
import json

import pytest
from sqlalchemy import create_engine, text

from server.common import qmt_edge_release_receipt as coordination
from server.common import qmt_edge_release_recovery as recovery
from tools import run_qmt_windows_edge_release_bootstrap as bootstrap


BUILD_SHA = "1" * 40
OTHER_BUILD_SHA = "2" * 40
ATTEMPT_A = "a" * 32
ATTEMPT_B = "b" * 32
ATTEMPT_C = "c" * 32
REQUESTED_AT = datetime(2026, 9, 4, 9, 0, 0)
_REAL_ACTIVATION_TRIGGER_SEAL_VALIDATOR = (
    coordination._validate_qmt_edge_release_activation_trigger_seal
)


@pytest.fixture(autouse=True)
def _stub_privileged_grant_connection_attestation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """These SQLite tests exercise ledger semantics, not Linux credentials."""

    monkeypatch.setattr(
        bootstrap,
        "_attest_activation_grant_connection",
        lambda _connection: None,
    )
    monkeypatch.setattr(
        coordination,
        "_validate_qmt_edge_release_activation_trigger_seal",
        lambda _connection, *, expected_build_sha: {
            "attested_build_sha": expected_build_sha,
            "authority": "PRIVILEGED_CUTOVER_TABLE_METADATA_SEAL",
        },
    )


def _engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE st_scheduled_tasks ("
            "id INTEGER PRIMARY KEY, task_type VARCHAR(64) NOT NULL)"
        ))
        connection.execute(text(
            "INSERT INTO st_scheduled_tasks (id, task_type) "
            "VALUES (7, 'qmt_reference_incremental')"
        ))
        connection.execute(text(
            "CREATE TABLE st_scheduled_task_history ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "run_uid VARCHAR(64) NOT NULL UNIQUE, "
            "task_id INTEGER NOT NULL, task_name VARCHAR(255), "
            "task_type VARCHAR(64), run_at DATETIME NOT NULL, "
            "finished_at DATETIME, status VARCHAR(32) NOT NULL, "
            "duration INTEGER NOT NULL, exit_code INTEGER, output TEXT, "
            "host_name VARCHAR(128), scheduler_instance_id VARCHAR(128), "
            "build_sha CHAR(40), trigger_source VARCHAR(32) NOT NULL)"
        ))
    return engine


def _history(engine) -> list[dict[str, object]]:
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(text(
                "SELECT id, run_uid, task_type, status, build_sha, "
                "trigger_source, scheduler_instance_id, output "
                "FROM st_scheduled_task_history ORDER BY id"
            )).mappings().all()
        ]


def test_activation_check_validates_exact_trigger_seal_before_ledger_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from server.engine import strategy_governance

    engine = _engine()
    calls: list[str] = []

    def validate_seal(connection, *, expected_build_sha):
        calls.append("seal")
        assert connection is not None
        assert expected_build_sha == BUILD_SHA
        return {
            "attested_build_sha": BUILD_SHA,
            "authority": "PRIVILEGED_CUTOVER_TABLE_METADATA_SEAL",
        }

    original_reference_task_id = coordination._reference_task_id

    def reference_task_id(connection):
        calls.append("ledger")
        return original_reference_task_id(connection)

    monkeypatch.setattr(
        coordination,
        "_validate_qmt_edge_release_activation_trigger_seal",
        _REAL_ACTIVATION_TRIGGER_SEAL_VALIDATOR,
    )
    monkeypatch.setattr(
        strategy_governance,
        "validate_privileged_trigger_migration_seal",
        validate_seal,
    )
    monkeypatch.setattr(
        coordination,
        "_reference_task_id",
        reference_task_id,
    )

    with engine.connect() as connection:
        ready, detail = coordination.check_qmt_edge_release_activation(
            connection,
            expected_build_sha=BUILD_SHA,
        )

    assert ready is False
    assert detail["status"] == "PENDING"
    assert calls == ["seal", "ledger"]


@pytest.mark.parametrize("case", ("missing", "old", "pending"))
def test_activation_check_rejects_unverified_trigger_seal_before_ledger(
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    from server.engine import strategy_governance

    if case in {"missing", "pending"}:
        def validate_seal(_connection, *, expected_build_sha):
            assert expected_build_sha == BUILD_SHA
            raise RuntimeError(case)
    else:
        def validate_seal(_connection, *, expected_build_sha):
            assert expected_build_sha == BUILD_SHA
            return {
                "attested_build_sha": OTHER_BUILD_SHA,
                "authority": "PRIVILEGED_CUTOVER_TABLE_METADATA_SEAL",
            }

    monkeypatch.setattr(
        coordination,
        "_validate_qmt_edge_release_activation_trigger_seal",
        _REAL_ACTIVATION_TRIGGER_SEAL_VALIDATOR,
    )
    monkeypatch.setattr(
        strategy_governance,
        "validate_privileged_trigger_migration_seal",
        validate_seal,
    )
    monkeypatch.setattr(
        coordination,
        "_reference_task_id",
        lambda _connection: pytest.fail(
            "unverified trigger seal reached the release ledger"
        ),
    )

    with _engine().connect() as connection, pytest.raises(
        coordination.QmtEdgeReleaseReceiptError,
        match="trigger seal",
    ):
        coordination.check_qmt_edge_release_activation(
            connection,
            expected_build_sha=BUILD_SHA,
        )


def test_hold_and_grant_are_canonical_fail_closed_records() -> None:
    hold = coordination.build_qmt_edge_release_quiescence_hold(
        build_sha=BUILD_SHA,
        deployment_attempt_id=ATTEMPT_A,
        requested_at=REQUESTED_AT,
    )
    assert hold["real_order"] is False
    assert hold["hold_run_uid"] == f"qmt-edge-hold-{ATTEMPT_A}"
    assert coordination.validate_qmt_edge_release_quiescence_hold(
        json.dumps(hold, sort_keys=True),
        expected_build_sha=BUILD_SHA,
        expected_deployment_attempt_id=ATTEMPT_A,
    ) == hold

    grant = coordination.build_qmt_edge_release_activation_grant(
        hold=hold,
        granted_at=REQUESTED_AT + timedelta(minutes=1),
    )
    assert grant["schema_cutover_verified"] is True
    assert grant["real_order"] is False
    assert grant["hold_hash"] == hold["hold_hash"]
    assert coordination.validate_qmt_edge_release_activation_grant(
        grant,
        expected_hold=hold,
    ) == grant

    for key, value in (
        ("schema_cutover_verified", False),
        ("real_order", True),
        ("hold_hash", "f" * 64),
        ("build_sha", OTHER_BUILD_SHA),
        ("deployment_attempt_id", ATTEMPT_B),
    ):
        tampered = {**grant, key: value}
        with pytest.raises(
            coordination.QmtEdgeReleaseReceiptError,
            match="content differs",
        ):
            coordination.validate_qmt_edge_release_activation_grant(
                tampered,
                expected_hold=hold,
            )


def test_request_and_hold_are_atomic_hold_first_and_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = _engine()
    first = bootstrap.append_release_request_with_quiescence(
        engine,
        expected_build_sha=BUILD_SHA,
        deployment_attempt_id=ATTEMPT_A,
        now=REQUESTED_AT,
    )
    assert first["status"] == "inserted"
    assert first["quiescence_hold_status"] == "inserted"
    assert first["release_request_status"] == "inserted"
    assert first["activation_granted"] is False
    rows = _history(engine)
    assert [row["trigger_source"] for row in rows] == [
        coordination.QMT_EDGE_RELEASE_QUIESCENCE_TRIGGER_SOURCE,
        coordination.QMT_EDGE_RELEASE_REQUEST_TRIGGER_SOURCE,
    ]
    assert all(
        row["task_type"] == coordination.QMT_EDGE_RELEASE_REQUEST_TASK_TYPE
        for row in rows
    )
    request_row = next(
        row
        for row in rows
        if row["trigger_source"]
        == coordination.QMT_EDGE_RELEASE_REQUEST_TRIGGER_SOURCE
    )
    assert json.loads(request_row["output"])["requested_at"] == (
        REQUESTED_AT.isoformat(timespec="seconds")
    )

    retry = bootstrap.append_release_request_with_quiescence(
        engine,
        expected_build_sha=BUILD_SHA,
        deployment_attempt_id=ATTEMPT_A,
        now=REQUESTED_AT + timedelta(minutes=5),
    )
    assert retry["status"] == "idempotent"
    assert len(_history(engine)) == 2

    rollback_engine = _engine()

    def fail_base(*_args, **_kwargs):
        raise RuntimeError("base insert failed")

    monkeypatch.setattr(bootstrap, "insert_qmt_edge_release_request", fail_base)
    with pytest.raises(RuntimeError, match="base insert failed"):
        bootstrap.append_release_request_with_quiescence(
            rollback_engine,
            expected_build_sha=BUILD_SHA,
            deployment_attempt_id=ATTEMPT_A,
            now=REQUESTED_AT,
        )
    assert _history(rollback_engine) == []


def test_existing_exact_request_is_reused_for_a_new_attempt_hold() -> None:
    engine = _engine()
    first = bootstrap.append_release_request_with_quiescence(
        engine,
        expected_build_sha=BUILD_SHA,
        deployment_attempt_id=ATTEMPT_A,
        now=REQUESTED_AT,
    )
    retried_at = REQUESTED_AT + timedelta(minutes=5)

    retry = bootstrap.append_release_request_with_quiescence(
        engine,
        expected_build_sha=BUILD_SHA,
        deployment_attempt_id=ATTEMPT_B,
        now=retried_at,
    )

    assert retry["status"] == "inserted"
    assert retry["release_request_status"] == "idempotent"
    assert retry["quiescence_hold_status"] == "inserted"
    assert retry["request_run_uid"] == first["request_run_uid"]
    rows = _history(engine)
    request_rows = [
        row
        for row in rows
        if row["trigger_source"]
        == coordination.QMT_EDGE_RELEASE_REQUEST_TRIGGER_SOURCE
    ]
    assert len(request_rows) == 1
    request = json.loads(request_rows[0]["output"])
    assert request["requested_at"] == REQUESTED_AT.isoformat(
        timespec="seconds"
    )
    newest_hold = json.loads(rows[-1]["output"])
    assert newest_hold["deployment_attempt_id"] == ATTEMPT_B
    assert newest_hold["requested_at"] == retried_at.isoformat(
        timespec="seconds"
    )
    assert newest_hold["request_run_uid"] == request["request_run_uid"]


def test_tampered_existing_request_rejects_new_attempt_without_writes() -> None:
    engine = _engine()
    bootstrap.append_release_request_with_quiescence(
        engine,
        expected_build_sha=BUILD_SHA,
        deployment_attempt_id=ATTEMPT_A,
        now=REQUESTED_AT,
    )
    with engine.begin() as connection:
        request_uid = coordination.qmt_edge_release_request_run_uid(BUILD_SHA)
        connection.execute(
            text(
                "UPDATE st_scheduled_task_history SET output=:output "
                "WHERE run_uid=:run_uid"
            ),
            {
                "run_uid": request_uid,
                "output": json.dumps({
                    "schema": coordination.QMT_EDGE_RELEASE_REQUEST_SCHEMA,
                    "build_sha": BUILD_SHA,
                    "request_run_uid": request_uid,
                    "requested_at": "2026-09-04T09:00:01",
                    "request_hash": "f" * 64,
                }),
            },
        )
    rows_before = _history(engine)

    with pytest.raises(
        coordination.QmtEdgeReleaseReceiptError,
        match="content differs",
    ):
        bootstrap.append_release_request_with_quiescence(
            engine,
            expected_build_sha=BUILD_SHA,
            deployment_attempt_id=ATTEMPT_B,
            now=REQUESTED_AT + timedelta(minutes=5),
        )

    assert _history(engine) == rows_before


def test_latest_hold_controls_same_sha_retry_and_rejects_stale_grant() -> None:
    engine = _engine()
    bootstrap.append_release_request_with_quiescence(
        engine,
        expected_build_sha=BUILD_SHA,
        deployment_attempt_id=ATTEMPT_A,
        now=REQUESTED_AT,
    )
    bootstrap.append_release_activation_grant(
        engine,
        expected_build_sha=BUILD_SHA,
        deployment_attempt_id=ATTEMPT_A,
        now=REQUESTED_AT + timedelta(minutes=1),
    )
    with engine.connect() as connection:
        ready, detail = coordination.check_qmt_edge_release_activation(
            connection,
            expected_build_sha=BUILD_SHA,
        )
    assert ready is True
    assert detail["deployment_attempt_id"] == ATTEMPT_A

    bootstrap.append_release_request_with_quiescence(
        engine,
        expected_build_sha=BUILD_SHA,
        deployment_attempt_id=ATTEMPT_B,
        now=REQUESTED_AT + timedelta(minutes=2),
    )
    with engine.connect() as connection:
        ready, detail = coordination.check_qmt_edge_release_activation(
            connection,
            expected_build_sha=BUILD_SHA,
        )
    assert ready is False
    assert detail["status"] == "PENDING"
    assert detail["deployment_attempt_id"] == ATTEMPT_B

    with pytest.raises(
        coordination.QmtEdgeReleaseReceiptError,
        match="content differs",
    ):
        bootstrap.append_release_activation_grant(
            engine,
            expected_build_sha=BUILD_SHA,
            deployment_attempt_id=ATTEMPT_A,
            now=REQUESTED_AT + timedelta(minutes=3),
        )
    bootstrap.append_release_activation_grant(
        engine,
        expected_build_sha=BUILD_SHA,
        deployment_attempt_id=ATTEMPT_B,
        now=REQUESTED_AT + timedelta(minutes=3),
    )
    with engine.connect() as connection:
        ready, detail = coordination.check_qmt_edge_release_activation(
            connection,
            expected_build_sha=BUILD_SHA,
            expected_deployment_attempt_id=ATTEMPT_B,
        )
    assert ready is True
    assert detail["grant"]["hold_hash"] == detail["hold"]["hold_hash"]


def test_activation_read_uses_one_trusted_task_identity_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = _engine()
    bootstrap.append_release_request_with_quiescence(
        engine,
        expected_build_sha=BUILD_SHA,
        deployment_attempt_id=ATTEMPT_A,
        now=REQUESTED_AT,
    )
    bootstrap.append_release_activation_grant(
        engine,
        expected_build_sha=BUILD_SHA,
        deployment_attempt_id=ATTEMPT_A,
        now=REQUESTED_AT + timedelta(minutes=1),
    )
    original = coordination._reference_task_id
    calls = 0

    def counted(connection):
        nonlocal calls
        calls += 1
        return original(connection)

    monkeypatch.setattr(coordination, "_reference_task_id", counted)
    with engine.connect() as connection:
        ready, _detail = coordination.check_qmt_edge_release_activation(
            connection,
            expected_build_sha=BUILD_SHA,
        )

    assert ready is True
    assert calls == 1


@pytest.mark.parametrize(
    ("column", "tampered_value"),
    (
        ("id", 0),
        ("task_id", 0),
        ("task_id", 8),
        ("task_name", "tampered activation task"),
        ("task_type", "tampered_task_type"),
        ("run_at", "2026-09-04 09:01:01"),
        ("finished_at", "2026-09-04 09:01:01"),
        ("status", "pending"),
        ("duration", 1),
        ("exit_code", 1),
        ("output", "{}"),
        ("host_name", "tampered-host"),
        ("scheduler_instance_id", ATTEMPT_B),
        ("build_sha", OTHER_BUILD_SHA),
        ("trigger_source", "release_quiescence"),
    ),
)
def test_activation_grant_rejects_every_tampered_ledger_field(
    column: str,
    tampered_value: object,
) -> None:
    engine = _engine()
    bootstrap.append_release_request_with_quiescence(
        engine,
        expected_build_sha=BUILD_SHA,
        deployment_attempt_id=ATTEMPT_A,
        now=REQUESTED_AT,
    )
    bootstrap.append_release_activation_grant(
        engine,
        expected_build_sha=BUILD_SHA,
        deployment_attempt_id=ATTEMPT_A,
        now=REQUESTED_AT + timedelta(minutes=1),
    )
    with engine.begin() as connection:
        connection.execute(
            text(
                f"UPDATE st_scheduled_task_history SET `{column}`=:value "
                "WHERE trigger_source=:trigger_source"
            ),
            {
                "value": tampered_value,
                "trigger_source": (
                    coordination.QMT_EDGE_RELEASE_ACTIVATION_TRIGGER_SOURCE
                ),
            },
        )

    with engine.connect() as connection, pytest.raises(
        coordination.QmtEdgeReleaseReceiptError
    ):
        coordination.check_qmt_edge_release_activation(
            connection,
            expected_build_sha=BUILD_SHA,
        )


@pytest.mark.parametrize(
    ("trigger_source", "column", "tampered_value"),
    (
        ("release_quiescence", "task_id", 0),
        ("release_quiescence", "task_id", 8),
        ("release_quiescence", "task_name", "tampered hold task"),
        ("release_quiescence", "run_at", "2026-09-04 09:00:01"),
        ("release_quiescence", "finished_at", "2026-09-04 09:00:01"),
        ("release_quiescence", "duration", 1),
        ("release_quiescence", "exit_code", 0),
        ("release_quiescence", "host_name", "tampered-host"),
        ("release_request", "task_id", 0),
        ("release_request", "task_id", 8),
        ("release_request", "task_name", "tampered request task"),
        ("release_request", "run_at", "2026-09-04 09:00:01"),
        ("release_request", "finished_at", "2026-09-04 09:00:01"),
        ("release_request", "duration", 1),
        ("release_request", "exit_code", 0),
        ("release_request", "host_name", "tampered-host"),
    ),
)
def test_hold_and_request_reject_tampered_fixed_ledger_fields(
    trigger_source: str,
    column: str,
    tampered_value: object,
) -> None:
    engine = _engine()
    bootstrap.append_release_request_with_quiescence(
        engine,
        expected_build_sha=BUILD_SHA,
        deployment_attempt_id=ATTEMPT_A,
        now=REQUESTED_AT,
    )
    with engine.begin() as connection:
        connection.execute(
            text(
                f"UPDATE st_scheduled_task_history SET `{column}`=:value "
                "WHERE trigger_source=:trigger_source"
            ),
            {
                "value": tampered_value,
                "trigger_source": trigger_source,
            },
        )

    with engine.connect() as connection, pytest.raises(
        coordination.QmtEdgeReleaseReceiptError
    ):
        coordination.check_qmt_edge_release_activation(
            connection,
            expected_build_sha=BUILD_SHA,
        )


def test_builds_are_isolated_and_attempt_identity_cannot_cross_builds() -> None:
    engine = _engine()
    bootstrap.append_release_request_with_quiescence(
        engine,
        expected_build_sha=BUILD_SHA,
        deployment_attempt_id=ATTEMPT_A,
        now=REQUESTED_AT,
    )
    bootstrap.append_release_activation_grant(
        engine,
        expected_build_sha=BUILD_SHA,
        deployment_attempt_id=ATTEMPT_A,
        now=REQUESTED_AT + timedelta(minutes=1),
    )
    bootstrap.append_release_request_with_quiescence(
        engine,
        expected_build_sha=OTHER_BUILD_SHA,
        deployment_attempt_id=ATTEMPT_B,
        now=REQUESTED_AT + timedelta(minutes=2),
    )
    with engine.connect() as connection:
        first_ready, _ = coordination.check_qmt_edge_release_activation(
            connection,
            expected_build_sha=BUILD_SHA,
        )
        second_ready, second = coordination.check_qmt_edge_release_activation(
            connection,
            expected_build_sha=OTHER_BUILD_SHA,
        )
    assert first_ready is True
    assert second_ready is False
    assert second["deployment_attempt_id"] == ATTEMPT_B

    with pytest.raises(
        coordination.QmtEdgeReleaseReceiptError,
        match="content differs",
    ):
        bootstrap.append_release_request_with_quiescence(
            engine,
            expected_build_sha=OTHER_BUILD_SHA,
            deployment_attempt_id=ATTEMPT_A,
            now=REQUESTED_AT + timedelta(minutes=3),
        )


def test_latest_activation_recovers_lost_attempt_and_commit_output() -> None:
    engine = _engine()
    bootstrap.append_release_request_with_quiescence(
        engine,
        expected_build_sha=BUILD_SHA,
        deployment_attempt_id=ATTEMPT_C,
        now=REQUESTED_AT,
    )
    with engine.connect() as connection:
        ready, pending = coordination.check_qmt_edge_release_activation(
            connection,
            expected_build_sha=BUILD_SHA,
        )
    assert ready is False
    assert pending["deployment_attempt_id"] == ATTEMPT_C

    # Model a successful database commit whose stdout was lost by discarding
    # the first result.  Recovery has only the target SHA, not the attempt ID.
    bootstrap.append_latest_release_activation_grant(
        engine,
        expected_build_sha=BUILD_SHA,
        now=REQUESTED_AT + timedelta(minutes=1),
    )
    retry = bootstrap.append_latest_release_activation_grant(
        engine,
        expected_build_sha=BUILD_SHA,
        now=REQUESTED_AT + timedelta(minutes=2),
    )

    assert retry["mode"] == "activation-grant-latest"
    assert retry["status"] == "idempotent"
    assert retry["deployment_attempt_id"] == ATTEMPT_C
    assert retry["activation_granted"] is True
    grant_rows = [
        row
        for row in _history(engine)
        if row["trigger_source"]
        == coordination.QMT_EDGE_RELEASE_ACTIVATION_TRIGGER_SOURCE
    ]
    assert len(grant_rows) == 1
    with engine.connect() as connection:
        ready, detail = coordination.check_qmt_edge_release_activation(
            connection,
            expected_build_sha=BUILD_SHA,
        )
    assert ready is True
    assert detail["deployment_attempt_id"] == ATTEMPT_C


def test_bootstrap_cli_returns_pending_before_qmt_or_receipt_write(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    engine = _engine()
    bootstrap.append_release_request_with_quiescence(
        engine,
        expected_build_sha=BUILD_SHA,
        deployment_attempt_id=ATTEMPT_C,
        now=REQUESTED_AT,
    )
    before = _history(engine)
    monkeypatch.delenv("PROBIGA_BUILD_COMMIT_SHA", raising=False)
    monkeypatch.setattr("tools.env_config.load_project_env", lambda: None)
    monkeypatch.setattr("tools.env_config.create_tool_engine", lambda: engine)
    monkeypatch.setattr(engine, "dispose", lambda: None)
    monkeypatch.setattr(bootstrap, "run_release_bootstrap", pytest.fail)

    result = bootstrap.main([
        "--bootstrap",
        "--expected-build-sha",
        BUILD_SHA,
        "--expected-scheduler-instance-id",
        "windows-edge-4321",
        "--compact",
    ])

    assert result == 4
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "bootstrap"
    assert payload["status"] == "PENDING"
    assert payload["deployment_attempt_id"] == ATTEMPT_C
    assert payload["activation_granted"] is False
    assert payload["database_writes"] is False
    assert payload["qmt_calls"] is False
    assert before == _history(engine)


PROTECTED_PRIOR_SHA = "f" * 40
PROTECTED_NEXT_SHA = "3" * 40
PROTECTED_PRIOR_ATTEMPT = "d" * 32
PROTECTED_NEW_ATTEMPT = "e" * 32


def _protected_seal(build_sha: str, *, comment: str = "original-compatible-seal"):
    return {
        "attested_build_sha": build_sha,
        "authority": "PRIVILEGED_CUTOVER_TABLE_METADATA_SEAL",
        "trigger_inventory_server_uuid": "00000000-0000-0000-0000-000000000001",
        "trigger_inventory_seal_database": "probiga",
        "trigger_inventory_contract_hash": "4" * 64,
        "trigger_inventory_table_comment": comment,
    }


@pytest.fixture
def protected_handoff(monkeypatch: pytest.MonkeyPatch):
    """Real in-memory ledger; all privileged/native probes are isolated fakes."""
    engine = _engine()
    attested_builds = []
    monkeypatch.delenv("PROBIGA_DEPLOYMENT_MODE", raising=False)
    monkeypatch.setattr(bootstrap, "_require_activation_grant_root", lambda: None)
    monkeypatch.setattr(bootstrap, "_assert_recovery_database_identity", lambda *_: None)
    monkeypatch.setattr(bootstrap, "gethostname", lambda: "protected-win")
    monkeypatch.setattr(
        coordination, "_validate_qmt_edge_release_activation_trigger_seal",
        lambda _connection, *, expected_build_sha: _protected_seal(expected_build_sha),
    )

    def retained_seal(build_sha):
        attested_builds.append(build_sha)
        return _protected_seal(build_sha)

    monkeypatch.setattr(bootstrap, "_read_retained_contract_seal", retained_seal)
    monkeypatch.setattr(bootstrap, "check_qmt_windows_edge_identity", lambda *_args, **_kwargs: (True, {
        "current": {
            "build_sha": PROTECTED_PRIOR_SHA, "host_name": "protected-win",
            "pid": 4321, "instance_id": "protected-win-4321",
        },
    }))
    bootstrap.append_release_request_with_quiescence(
        engine, expected_build_sha=PROTECTED_PRIOR_SHA,
        deployment_attempt_id=PROTECTED_PRIOR_ATTEMPT, now=REQUESTED_AT,
    )
    bootstrap.append_release_activation_grant(
        engine, expected_build_sha=PROTECTED_PRIOR_SHA,
        deployment_attempt_id=PROTECTED_PRIOR_ATTEMPT,
        now=REQUESTED_AT + timedelta(seconds=1),
    )
    root = bootstrap.append_recoverable_release_request(
        engine, engine, expected_build_sha=BUILD_SHA, prior_build_sha=PROTECTED_PRIOR_SHA,
        deployment_attempt_id=ATTEMPT_A, now=REQUESTED_AT + timedelta(seconds=2),
    )
    # A stopped prior cannot provide a later heartbeat. Renewal must preserve
    # the actual originally observed host/PID/instance instead of re-freezing it.
    monkeypatch.setattr(
        bootstrap, "check_qmt_windows_edge_identity",
        lambda *_args, **_kwargs: pytest.fail("renewal re-observed the frozen prior process"),
    )
    yield engine, root, attested_builds
    engine.dispose()


def _protected_forward(engine, *, target=BUILD_SHA, attempt=ATTEMPT_B, seconds=3,
                       prior=PROTECTED_PRIOR_SHA):
    return bootstrap.append_forward_release_request(
        engine, engine, expected_build_sha=target, prior_build_sha=prior,
        deployment_attempt_id=attempt, now=REQUESTED_AT + timedelta(seconds=seconds),
    )


@pytest.mark.parametrize("already_forward", (False, True))
def test_protected_same_target_renewal_keeps_frozen_history_and_requires_latest_grant(
    protected_handoff, already_forward: bool,
) -> None:
    engine, root, attested_builds = protected_handoff
    target = PROTECTED_NEXT_SHA if already_forward else BUILD_SHA
    previous = (_protected_forward(engine, target=target) if already_forward else root)
    attempt = ATTEMPT_C if already_forward else ATTEMPT_B
    before = _history(engine)
    attestations_before = len(attested_builds)

    renewed = _protected_forward(engine, target=target, attempt=attempt, seconds=4)
    context = renewed["context"]
    assert renewed["status"] == "inserted"
    assert renewed["activation_granted"] is False
    assert renewed["database_writes"] is True
    assert len(attested_builds) == attestations_before + 1
    assert attested_builds[-1] == PROTECTED_PRIOR_SHA
    assert _history(engine)[:len(before)] == before
    # The existing SHA request is immutable and reused; only hold/context append.
    assert len(_history(engine)) == len(before) + 2
    assert context["build_sha"] == context["supersedes_build_sha"] == target
    assert context["deployment_attempt_id"] == attempt
    assert context["supersedes_deployment_attempt_id"] == previous["context"]["deployment_attempt_id"]
    assert context["supersedes_hold_run_uid"] == previous["context"]["hold_run_uid"]
    assert context["supersedes_hold_hash"] == previous["context"]["hold_hash"]
    assert context["supersedes_context_run_uid"] == previous["context"]["context_run_uid"]
    assert context["supersedes_context_hash"] == previous["context"]["context_hash"]
    for key in ("build_sha", "host_name", "pid", "instance_id", "seal_hash"):
        assert context["original_prior_" + key] == root["context"]["prior_" + key]
    assert "prior_running" not in context and "captured_at" not in context
    assert context["supersession_depth"] == (2 if already_forward else 1)
    assert coordination.qmt_edge_release_activation_run_uid(attempt) not in {
        row["run_uid"] for row in _history(engine)
    }
    with engine.connect() as connection:
        assert coordination.check_qmt_edge_release_activation(
            connection, expected_build_sha=target,
        )[0] is False
        assert coordination.check_qmt_edge_release_activation(
            connection, expected_build_sha=PROTECTED_PRIOR_SHA,
        )[0] is False

    persisted = _history(engine)
    replay = _protected_forward(engine, target=target, attempt=attempt, seconds=100)
    assert replay["status"] == "idempotent" and replay["database_writes"] is False
    assert replay["context"] == context and _history(engine) == persisted
    assert len(attested_builds) == attestations_before + 2
    with pytest.raises(coordination.QmtEdgeReleaseReceiptError, match="prior identity/schema changed"):
        bootstrap.append_precutover_abort(
            engine, engine, expected_build_sha=PROTECTED_PRIOR_SHA,
            target_build_sha=target, deployment_attempt_id=attempt,
            now=REQUESTED_AT + timedelta(seconds=101),
        )
    assert _history(engine) == persisted
    with pytest.raises(coordination.QmtEdgeReleaseReceiptError):
        bootstrap.append_release_activation_grant(
            engine, expected_build_sha=target,
            deployment_attempt_id=previous["context"]["deployment_attempt_id"],
            now=REQUESTED_AT + timedelta(seconds=101),
        )
    assert _history(engine) == persisted
    bootstrap.append_release_activation_grant(
        engine, expected_build_sha=target, deployment_attempt_id=attempt,
        now=REQUESTED_AT + timedelta(seconds=102),
    )
    with engine.connect() as connection:
        ready, detail = coordination.check_qmt_edge_release_activation(
            connection, expected_build_sha=target, expected_deployment_attempt_id=attempt,
        )
        assert ready is True and detail["deployment_attempt_id"] == attempt
        assert detail["hold"]["hold_hash"] == detail["grant"]["hold_hash"] == context["hold_hash"]
        assert coordination.check_qmt_edge_release_activation(
            connection, expected_build_sha=PROTECTED_PRIOR_SHA,
        )[0] is False
        if already_forward:
            assert coordination.check_qmt_edge_release_activation(
                connection, expected_build_sha=BUILD_SHA,
            )[0] is False
    selected = bootstrap.select_update_target(engine, expected_build_sha=PROTECTED_PRIOR_SHA)
    assert selected["target_build_sha"] == target and selected["context"] == context
    assert selected["writer_authorized"] is False
    transition = bootstrap.read_release_transition(
        engine, expected_build_sha=PROTECTED_PRIOR_SHA, target_build_sha=target,
    )
    assert transition["status"] == "READY_TO_SWITCH" and transition["writer_authorized"] is False


def test_protected_same_target_renewal_allows_verified_compatible_current_seal(
    protected_handoff, monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, root, _ = protected_handoff
    changed = _protected_seal(PROTECTED_PRIOR_SHA, comment="verified-compatible-after-partial-cutover")
    assert recovery.seal_identity_hash(changed) != root["context"]["prior_seal_hash"]
    monkeypatch.setattr(bootstrap, "_read_retained_contract_seal", lambda sha: changed)

    renewed = _protected_forward(engine)

    assert renewed["status"] == "inserted" and renewed["activation_granted"] is False
    assert renewed["context"]["original_prior_seal_hash"] == root["context"]["prior_seal_hash"]


@pytest.mark.parametrize("already_forward", (False, True))
def test_protected_same_target_renewal_failed_actual_seal_attestation_has_no_writes(
    protected_handoff, monkeypatch: pytest.MonkeyPatch, already_forward: bool,
) -> None:
    engine, _root, _ = protected_handoff
    target = PROTECTED_NEXT_SHA if already_forward else BUILD_SHA
    if already_forward:
        _protected_forward(engine, target=target)
    before = _history(engine)

    def unavailable(_build_sha):
        raise RuntimeError("RECOVERY_BLOCKED: retained contract attestation failed")

    monkeypatch.setattr(bootstrap, "_read_retained_contract_seal", unavailable)
    with pytest.raises(RuntimeError, match="retained contract attestation failed"):
        _protected_forward(engine, target=target, attempt=ATTEMPT_C, seconds=4)
    assert _history(engine) == before


@pytest.mark.parametrize("case", ("original_prior", "nonlatest_target", "reused_attempt", "replay_target", "wrong_prior"))
def test_protected_same_target_renewal_never_reopens_prior_or_used_identity(
    protected_handoff, case: str,
) -> None:
    engine, _root, _ = protected_handoff
    _protected_forward(engine, target=PROTECTED_NEXT_SHA)
    _protected_forward(engine, target=PROTECTED_NEXT_SHA, attempt=ATTEMPT_C, seconds=4)
    target, attempt, prior = PROTECTED_NEXT_SHA, PROTECTED_NEW_ATTEMPT, PROTECTED_PRIOR_SHA
    if case == "original_prior":
        target = PROTECTED_PRIOR_SHA
    elif case == "nonlatest_target":
        target = BUILD_SHA
    elif case == "reused_attempt":
        attempt = ATTEMPT_A
    elif case == "replay_target":
        attempt, target = ATTEMPT_C, OTHER_BUILD_SHA
    else:
        prior = OTHER_BUILD_SHA
    before = _history(engine)
    with pytest.raises(RuntimeError, match="RECOVERY_BLOCKED"):
        _protected_forward(engine, target=target, attempt=attempt, prior=prior, seconds=5)
    assert _history(engine) == before


def test_protected_completed_latest_hold_is_not_renewed(protected_handoff) -> None:
    engine, _root, _ = protected_handoff
    renewed = _protected_forward(engine)
    bootstrap.append_release_activation_grant(
        engine, expected_build_sha=BUILD_SHA, deployment_attempt_id=ATTEMPT_B,
        now=REQUESTED_AT + timedelta(seconds=4),
    )
    before = _history(engine)
    stopped = _protected_forward(engine, attempt=ATTEMPT_C, seconds=5)
    assert stopped["status"] == "not_applicable" and stopped["context"] is None
    assert stopped["database_writes"] is False and stopped["activation_granted"] is False
    assert _history(engine) == before
    assert renewed["context"]["deployment_attempt_id"] == ATTEMPT_B


@pytest.mark.parametrize("corrupt_predecessor", (False, True))
def test_protected_same_target_renewal_rejects_corrupted_chain_without_append(
    protected_handoff, corrupt_predecessor: bool,
) -> None:
    engine, _root, _ = protected_handoff
    _protected_forward(engine)
    damaged_attempt = ATTEMPT_A if corrupt_predecessor else ATTEMPT_B
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE st_scheduled_task_history SET output='{}' WHERE run_uid=:uid"
        ), {"uid": recovery.context_uid(damaged_attempt)})
    before = _history(engine)
    with pytest.raises(coordination.QmtEdgeReleaseReceiptError):
        _protected_forward(engine, attempt=ATTEMPT_C, seconds=4)
    assert _history(engine) == before


def test_protected_same_target_renewal_rejects_late_terminal_on_superseded_hold(
    protected_handoff,
) -> None:
    engine, _root, _ = protected_handoff
    _protected_forward(engine, target=PROTECTED_NEXT_SHA)
    with engine.begin() as connection:
        # Simulate ledger inconsistency in isolated SQLite, not an authorized
        # production route. The broker must notice a terminal on an old target.
        superseded = coordination.load_latest_qmt_edge_release_quiescence_hold(
            connection, expected_build_sha=BUILD_SHA,
        )
        coordination.insert_qmt_edge_release_activation_grant(connection,
            coordination.build_qmt_edge_release_activation_grant(
                hold=superseded, granted_at=REQUESTED_AT + timedelta(seconds=4),
            ))
    before = _history(engine)
    with pytest.raises(RuntimeError, match="superseded hold has a terminal"):
        _protected_forward(engine, target=PROTECTED_NEXT_SHA, attempt=ATTEMPT_C, seconds=5)
    assert _history(engine) == before


def test_protected_same_target_renewal_unknown_terminal_is_not_absence(protected_handoff) -> None:
    engine, _root, _ = protected_handoff
    _protected_forward(engine)
    bootstrap.append_release_activation_grant(
        engine, expected_build_sha=BUILD_SHA, deployment_attempt_id=ATTEMPT_B,
        now=REQUESTED_AT + timedelta(seconds=4),
    )
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE st_scheduled_task_history SET output=:output WHERE run_uid=:uid"
        ), {"uid": coordination.qmt_edge_release_activation_run_uid(ATTEMPT_B),
            "output": json.dumps({"schema": "unknown-terminal"})})
    before = _history(engine)
    with pytest.raises(RuntimeError, match="terminal is unknown or malformed"):
        _protected_forward(engine, attempt=ATTEMPT_C, seconds=5)
    assert _history(engine) == before


def test_protected_same_target_renewal_depth_limit_is_atomic_and_chain_stays_readable(
    protected_handoff,
) -> None:
    engine, _root, _ = protected_handoff
    for depth in range(1, recovery.MAX_FORWARD_SUPERSESSION_DEPTH + 1):
        renewed = _protected_forward(
            engine, attempt=f"{depth + 20:032x}", seconds=depth + 2,
        )
        assert renewed["context"]["supersession_depth"] == depth
    with engine.connect() as connection:
        latest = recovery.latest_hold(connection)
        assert len(recovery.load_context_chain(connection, latest)) == (
            recovery.MAX_FORWARD_SUPERSESSION_DEPTH + 1
        )
    before = _history(engine)
    with pytest.raises(coordination.QmtEdgeReleaseReceiptError, match="repeats protected identity"):
        _protected_forward(engine, attempt=PROTECTED_NEW_ATTEMPT, seconds=100)
    assert _history(engine) == before
