"""Real ledger state transitions; host/privileged connection probes are fake.

SQLite exercises terminal uniqueness and state transitions, not MySQL trigger
permissions or distributed lock behavior. Those remain a deployment proof.
"""
from datetime import datetime, timedelta
from contextlib import contextmanager
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

from server.common import qmt_edge_release_receipt as ledger
from server.common import qmt_edge_release_recovery as recovery
from tools import run_qmt_windows_edge_release_bootstrap as bootstrap


OLD = "1" * 40
NEW = "2" * 40
NEXT = "3" * 40
OLD_ATTEMPT = "a" * 32
ATTEMPT = "b" * 32
NEXT_ATTEMPT = "c" * 32
FOURTH = "4" * 40
FOURTH_ATTEMPT = "d" * 32
AT = datetime(2026, 9, 5, 10, 0, 0)


@pytest.mark.parametrize("fault", [None, "lock_denied", "body", "release_lost"])
def test_mysql_lock_session_transaction_and_finally_order(fault):
    """A connection spy proves ordering only, not real MySQL concurrency/ACL."""
    events = []

    class Connection:
        dialect = SimpleNamespace(name="mysql")

        def execute(self, statement, parameters):
            assert parameters == {"name": recovery.CONTROL_LOCK}
            if "GET_LOCK" in str(statement):
                events.append("lock")
                return SimpleNamespace(scalar_one=lambda: 0 if fault == "lock_denied" else 1)
            assert "RELEASE_LOCK" in str(statement)
            events.append("release")
            return SimpleNamespace(scalar_one=lambda: 0 if fault == "release_lost" else 1)

        def commit(self):
            events.append("commit")

        def in_transaction(self):
            return False

        @contextmanager
        def begin(self):
            events.append("begin")
            try:
                yield self
            except RuntimeError:
                events.append("rollback-body")
                raise
            else:
                events.append("commit-body")

    connection = Connection()

    @contextmanager
    def connect():
        events.append("connect")
        try:
            yield connection
        finally:
            events.append("close")

    def run():
        with recovery.release_control_connection(SimpleNamespace(connect=connect)) as actual:
            assert actual is connection
            events.append("body")
            if fault == "body":
                raise RuntimeError("injected body failure")

    if fault:
        with pytest.raises((RuntimeError, ledger.QmtEdgeReleaseReceiptError)):
            run()
    else:
        run()
    if fault == "lock_denied":
        assert events == ["connect", "lock", "commit", "close"]
    else:
        assert events == ["connect", "lock", "commit", "begin", "body",
                          "rollback-body" if fault == "body" else "commit-body",
                          "release", "commit", "close"]


@pytest.mark.parametrize("server, database", [
    ("same-server", "same-database"),
    ("other-server", "same-database"),
    ("same-server", "other-database"),
])
def test_privileged_ledger_and_runtime_probe_must_be_same_database(server, database):
    rows = [{"server_uuid": server, "database_name": database}]
    connection = SimpleNamespace(execute=lambda _statement: SimpleNamespace(
        mappings=lambda: SimpleNamespace(all=lambda: rows)))
    seal = {"trigger_inventory_server_uuid": "same-server",
            "trigger_inventory_seal_database": "same-database"}
    if server == "same-server" and database == "same-database":
        bootstrap._assert_recovery_database_identity(connection, seal)
    else:
        with pytest.raises(RuntimeError, match="databases differ"):
            bootstrap._assert_recovery_database_identity(connection, seal)


def _seal(sha):
    return {
        "attested_build_sha": sha,
        "authority": "PRIVILEGED_CUTOVER_TABLE_METADATA_SEAL",
        "trigger_inventory_server_uuid": "fake-server",
        "trigger_inventory_contract_hash": "a" * 64,
        "trigger_inventory_table_comment": "verified-original-seal",
    }


@pytest.fixture
def engine(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE st_scheduled_tasks (id INTEGER PRIMARY KEY, task_type TEXT)"))
        connection.execute(text("INSERT INTO st_scheduled_tasks VALUES (7, 'qmt_reference_incremental')"))
        connection.execute(text(
            "CREATE TABLE st_scheduled_task_history (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "run_uid TEXT UNIQUE NOT NULL, task_id INTEGER, task_name TEXT, task_type TEXT, "
            "run_at TEXT, finished_at TEXT, status TEXT, duration INTEGER, exit_code INTEGER, "
            "output TEXT, host_name TEXT, scheduler_instance_id TEXT, build_sha TEXT, trigger_source TEXT)"
        ))
    monkeypatch.delenv("PROBIGA_DEPLOYMENT_MODE", raising=False)
    monkeypatch.setattr(bootstrap, "_attest_activation_grant_connection", lambda _connection: None)
    monkeypatch.setattr(bootstrap, "_assert_recovery_database_identity", lambda *_args: None)
    monkeypatch.setattr(ledger, "_validate_qmt_edge_release_activation_trigger_seal",
                        lambda _connection, *, expected_build_sha: _seal(expected_build_sha))
    monkeypatch.setattr(bootstrap, "check_qmt_windows_edge_identity", lambda *_args, **_kwargs: (True, {
        "current": {"build_sha": OLD, "host_name": "WIN", "pid": 41, "instance_id": "WIN-41"}
    }))
    monkeypatch.setattr(bootstrap, "check_qmt_windows_edge_release_receipt", lambda *_args, **_kwargs: (True, {}))
    monkeypatch.setattr(bootstrap, "gethostname", lambda: "WIN")
    bootstrap.append_release_request_with_quiescence(
        engine, expected_build_sha=OLD, deployment_attempt_id=OLD_ATTEMPT, now=AT,
    )
    bootstrap.append_release_activation_grant(
        engine, expected_build_sha=OLD, deployment_attempt_id=OLD_ATTEMPT, now=AT + timedelta(seconds=1),
    )
    yield engine
    engine.dispose()


def _handoff(engine, *, target=NEW, attempt=ATTEMPT, seconds=2):
    return bootstrap.append_recoverable_release_request(
        engine, engine, expected_build_sha=OLD, target_build_sha=target,
        deployment_attempt_id=attempt, now=AT + timedelta(seconds=seconds),
    )


def _abort(engine, *, target=NEW, attempt=ATTEMPT):
    return bootstrap.append_precutover_abort(
        engine, engine, expected_build_sha=OLD, target_build_sha=target,
        deployment_attempt_id=attempt, now=AT + timedelta(seconds=10),
    )


def _ready(engine, sha):
    with engine.connect() as connection:
        return ledger.check_qmt_edge_release_activation(connection, expected_build_sha=sha)[0]


def test_pending_hold_fences_prior_without_authorizing_candidate(engine):
    result = _handoff(engine)
    assert result["context"]["prior_build_sha"] == OLD
    assert result["context"]["prior_instance_id"] == "WIN-41"
    assert not _ready(engine, OLD)
    assert not _ready(engine, NEW)
    hint = bootstrap.read_release_transition(engine, expected_build_sha=OLD, target_build_sha=NEW)
    assert hint["status"] == "PENDING"
    assert hint["writer_authorized"] is False


def test_first_compatibility_install_uses_real_v1_hold_and_still_requires_final_grant(engine):
    result = bootstrap.append_release_request_with_quiescence(
        engine, expected_build_sha=NEW, deployment_attempt_id=ATTEMPT,
        now=AT + timedelta(seconds=2), compatibility_install=True,
    )
    assert result["mode"] == "request-compatibility-quiescence"
    assert result["compatibility_install"] is True
    assert result["activation_granted"] is False
    assert not _ready(engine, NEW)
    with engine.connect() as connection:
        assert not recovery.has_protected_context(connection)
        assert recovery.latest_hold(connection)["deployment_attempt_id"] == ATTEMPT
    bootstrap.append_release_activation_grant(
        engine, expected_build_sha=NEW, deployment_attempt_id=ATTEMPT,
        now=AT + timedelta(seconds=3),
    )
    assert _ready(engine, NEW)


@pytest.mark.parametrize("compatibility", [False, True])
def test_no_legacy_or_compatibility_hold_can_follow_recovery_enablement(engine, compatibility):
    _handoff(engine)
    with pytest.raises(RuntimeError, match="disabled after protected recovery context"):
        bootstrap.append_release_request_with_quiescence(
            engine, expected_build_sha=NEXT, deployment_attempt_id=NEXT_ATTEMPT,
            now=AT + timedelta(seconds=3), compatibility_install=compatibility,
        )
    with engine.connect() as connection:
        assert recovery.latest_hold(connection)["deployment_attempt_id"] == ATTEMPT


def test_compatibility_hold_requires_privileged_identity_before_writing(engine, monkeypatch):
    def reject(_connection):
        raise RuntimeError("migrator identity differs")
    monkeypatch.setattr(bootstrap, "_attest_activation_grant_connection", reject)
    with pytest.raises(RuntimeError, match="migrator identity differs"):
        bootstrap.append_release_request_with_quiescence(
            engine, expected_build_sha=NEW, deployment_attempt_id=ATTEMPT,
            now=AT + timedelta(seconds=2), compatibility_install=True,
        )
    with engine.connect() as connection:
        assert recovery.latest_hold(connection)["deployment_attempt_id"] == OLD_ATTEMPT


def test_abort_restores_only_prior_with_original_real_grant_and_seal(engine):
    _handoff(engine)
    result = _abort(engine)
    assert result["schema"] == recovery.ABORT_SCHEMA
    assert result["terminal_run_uid"] == ledger.qmt_edge_release_activation_run_uid(ATTEMPT)
    assert result["resume_build_sha"] == OLD
    assert _ready(engine, OLD)
    assert not _ready(engine, NEW)
    hint = bootstrap.read_release_transition(engine, expected_build_sha=OLD, target_build_sha=NEXT)
    assert hint["status"] == "RESUME_PRIOR"  # Unreleased newer main cannot strand recovery.
    assert hint["writer_authorized"] is False  # Normal old bootstrap is still mandatory.
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM st_scheduled_task_history WHERE run_uid=:uid"),
                           {"uid": ledger.qmt_edge_release_activation_run_uid(OLD_ATTEMPT)})
    assert not _ready(engine, OLD)  # ABORT is not a substitute activation grant.


def test_abort_replay_is_idempotent_but_commit_after_abort_is_rejected(engine):
    _handoff(engine)
    first = _abort(engine)
    second = _abort(engine)
    assert second["status"] == "idempotent"
    assert second["abort_hash"] == first["abort_hash"]
    with pytest.raises(ledger.QmtEdgeReleaseReceiptError):
        bootstrap.append_release_activation_grant(
            engine, expected_build_sha=NEW, deployment_attempt_id=ATTEMPT,
            now=AT + timedelta(seconds=11),
        )


def test_commit_rejects_abort_and_only_exposes_a_switch_hint(engine):
    _handoff(engine)
    bootstrap.append_release_activation_grant(
        engine, expected_build_sha=NEW, deployment_attempt_id=ATTEMPT,
        now=AT + timedelta(seconds=3),
    )
    with pytest.raises(ledger.QmtEdgeReleaseReceiptError):
        _abort(engine)
    hint = bootstrap.read_release_transition(engine, expected_build_sha=OLD, target_build_sha=NEW)
    assert hint["status"] == "READY_TO_SWITCH"
    assert hint["writer_authorized"] is False
    assert not _ready(engine, OLD)
    assert _ready(engine, NEW)


def test_new_global_attempt_revokes_previous_abort_across_different_builds(engine):
    _handoff(engine)
    _abort(engine)
    assert _ready(engine, OLD)
    _handoff(engine, target=NEXT, attempt=NEXT_ATTEMPT, seconds=12)
    assert not _ready(engine, OLD)
    with pytest.raises(RuntimeError, match="latest target differs"):
        _abort(engine)
    with pytest.raises(RuntimeError, match="globally latest"):
        bootstrap.append_release_activation_grant(
            engine, expected_build_sha=NEW, deployment_attempt_id=ATTEMPT,
            now=AT + timedelta(seconds=13),
        )


def test_schema_drift_blocks_abort_and_leaves_terminal_empty(engine, monkeypatch):
    _handoff(engine)
    monkeypatch.setattr(ledger, "_validate_qmt_edge_release_activation_trigger_seal",
                        lambda _connection, **_kwargs: {**_seal(OLD), "trigger_inventory_table_comment": "CHANGED"})
    with pytest.raises(ledger.QmtEdgeReleaseReceiptError, match="prior identity/schema changed"):
        _abort(engine)
    with engine.connect() as connection:
        assert recovery._row(connection, ledger.qmt_edge_release_activation_run_uid(ATTEMPT)) is None


def test_missing_prior_live_identity_does_not_publish_a_hold(engine, monkeypatch):
    monkeypatch.setattr(bootstrap, "check_qmt_windows_edge_identity", lambda *_args, **_kwargs: (False, {}))
    with pytest.raises(RuntimeError, match="fresh prior Windows"):
        _handoff(engine)
    assert _ready(engine, OLD)
    with engine.connect() as connection:
        assert recovery.latest_hold(connection)["deployment_attempt_id"] == OLD_ATTEMPT


def test_handoff_replay_reuses_original_identity_not_new_capture_time(engine):
    first = _handoff(engine)
    again = _handoff(engine, seconds=100)
    assert again["status"] == "idempotent"
    assert again["context"] == first["context"]


@pytest.mark.parametrize("field,value", [
    ("prior_build_sha", NEXT), ("prior_pid", 42), ("prior_running", 1),
    ("real_order", 0), ("context_hash", "f" * 64), ("extra_field", "x"),
])
def test_protected_context_rejects_field_drift_even_in_fake_database(engine, field, value):
    result = _handoff(engine)
    payload = {**result["context"], field: value}
    with engine.begin() as connection:
        connection.execute(text("UPDATE st_scheduled_task_history SET output=:output WHERE run_uid=:uid"),
                           {"output": json.dumps(payload), "uid": recovery.context_uid(ATTEMPT)})
    with pytest.raises(ledger.QmtEdgeReleaseReceiptError):
        _ready(engine, OLD)


def test_legacy_hold_cannot_be_given_a_synthetic_abort(engine):
    with pytest.raises(ledger.QmtEdgeReleaseReceiptError, match="legacy hold"):
        bootstrap.append_precutover_abort(
            engine, engine, expected_build_sha=NEW, target_build_sha=OLD,
            deployment_attempt_id=OLD_ATTEMPT, now=AT + timedelta(seconds=10),
        )


def test_host_or_replaced_checkout_blocks_resume(engine, monkeypatch):
    _handoff(engine)
    _abort(engine)
    monkeypatch.setattr(bootstrap, "gethostname", lambda: "OTHER-WIN")
    with pytest.raises(RuntimeError, match="host differs"):
        bootstrap.read_release_transition(engine, expected_build_sha=OLD, target_build_sha=NEW)
    monkeypatch.setattr(bootstrap, "gethostname", lambda: "WIN")
    with pytest.raises(RuntimeError, match="checkout was already replaced"):
        bootstrap.read_release_transition(engine, expected_build_sha=NEW, target_build_sha=NEXT)


def test_target_selection_uses_global_authorized_hold_not_unreleased_git_tip(engine):
    _handoff(engine)
    selected = bootstrap.select_update_target(engine, expected_build_sha=OLD)
    assert selected == {"mode": "select-update-target", "status": "SELECTED",
                        "build_sha": OLD, "target_build_sha": NEW,
                        "database_writes": False, "writer_authorized": False}
    assert selected["target_build_sha"] != NEXT
    _abort(engine)
    assert bootstrap.select_update_target(engine, expected_build_sha=OLD)["target_build_sha"] == OLD
    with pytest.raises(RuntimeError, match="outside protected handoff"):
        bootstrap.select_update_target(engine, expected_build_sha=NEXT)


def test_selected_installed_target_can_retry_without_prior_checkout_identity(engine):
    _handoff(engine)
    bootstrap.append_release_activation_grant(
        engine, expected_build_sha=NEW, deployment_attempt_id=ATTEMPT,
        now=AT + timedelta(seconds=3),
    )
    assert bootstrap.select_update_target(engine, expected_build_sha=NEW)["target_build_sha"] == NEW


def test_legacy_compatibility_switch_requires_real_terminal_grant(engine):
    bootstrap.append_release_request_with_quiescence(
        engine, expected_build_sha=NEW, deployment_attempt_id=ATTEMPT,
        now=AT + timedelta(seconds=2), compatibility_install=True,
    )
    assert bootstrap.select_update_target(engine, expected_build_sha=OLD)["target_build_sha"] == NEW
    assert bootstrap.read_release_transition(
        engine, expected_build_sha=OLD, target_build_sha=NEW)["status"] == "LEGACY_PENDING"
    assert bootstrap.read_release_transition(
        engine, expected_build_sha=OLD, target_build_sha=NEXT)["status"] == "NO_REQUEST"
    bootstrap.append_release_activation_grant(
        engine, expected_build_sha=NEW, deployment_attempt_id=ATTEMPT,
        now=AT + timedelta(seconds=3),
    )
    result = bootstrap.read_release_transition(engine, expected_build_sha=OLD, target_build_sha=NEW)
    assert result["status"] == "LEGACY_READY_TO_SWITCH"
    assert result["writer_authorized"] is False


def test_selector_fails_closed_on_missing_context_after_protocol_enabled(engine):
    _handoff(engine)
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM st_scheduled_task_history WHERE run_uid=:uid"),
                           {"uid": ledger.qmt_edge_release_quiescence_run_uid(ATTEMPT)})
    with pytest.raises(RuntimeError, match="legacy intent after protected handoff"):
        bootstrap.select_update_target(engine, expected_build_sha=OLD)


def _forward(engine, *, target=NEXT, attempt=NEXT_ATTEMPT, seconds=4):
    return bootstrap.append_forward_release_request(
        engine, engine, expected_build_sha=target, prior_build_sha=OLD,
        deployment_attempt_id=attempt, now=AT + timedelta(seconds=seconds),
    )


def test_forward_pending_chain_fences_every_old_writer_and_exposes_selector_context(engine):
    failed = _handoff(engine)
    result = _forward(engine)
    context = result["context"]
    assert result["status"] == "inserted"
    assert result["build_sha"] == NEXT
    assert result["prior_build_sha"] == OLD
    assert context["schema"] == recovery.FORWARD_CONTEXT_SCHEMA
    assert context["scope"] == recovery.FORWARD_SCOPE
    assert context["supersedes_hold_hash"] == failed["context"]["hold_hash"]
    assert context["supersedes_context_hash"] == failed["context"]["context_hash"]
    assert context["original_prior_build_sha"] == OLD
    assert "prior_running" not in context
    assert "captured_at" not in context
    assert not _ready(engine, OLD)
    assert not _ready(engine, NEW)
    assert not _ready(engine, NEXT)

    selected = bootstrap.select_update_target(engine, expected_build_sha=OLD)
    assert selected["target_build_sha"] == NEXT
    assert selected["handoff_kind"] == recovery.FORWARD_SCOPE
    assert selected["context"] == context
    with pytest.raises(RuntimeError, match="outside protected handoff"):
        bootstrap.select_update_target(engine, expected_build_sha=NEW)
    transition = bootstrap.read_release_transition(
        engine, expected_build_sha=OLD, target_build_sha=NEXT,
    )
    assert transition["status"] == "PENDING"
    assert transition["context"] == context
    with pytest.raises(RuntimeError, match="forward target differs"):
        bootstrap.read_release_transition(
            engine, expected_build_sha=OLD, target_build_sha=FOURTH,
        )


def test_forward_replay_is_idempotent_then_standard_grant_makes_it_not_applicable(engine):
    _handoff(engine)
    first = _forward(engine)
    replay = _forward(engine, seconds=100)
    assert replay["status"] == "idempotent"
    assert replay["database_writes"] is False
    assert replay["context"] == first["context"]
    with pytest.raises(ledger.QmtEdgeReleaseReceiptError, match="prior identity/schema changed"):
        bootstrap.append_precutover_abort(
            engine, engine, expected_build_sha=OLD, target_build_sha=NEXT,
            deployment_attempt_id=NEXT_ATTEMPT, now=AT + timedelta(seconds=101),
        )
    bootstrap.append_release_activation_grant(
        engine, expected_build_sha=NEXT, deployment_attempt_id=NEXT_ATTEMPT,
        now=AT + timedelta(seconds=102),
    )
    assert _ready(engine, NEXT)
    assert not _ready(engine, OLD)
    assert bootstrap.read_release_transition(
        engine, expected_build_sha=OLD, target_build_sha=NEXT,
    )["status"] == "READY_TO_SWITCH"
    not_applicable = _forward(engine, seconds=103)
    assert not_applicable["status"] == "not_applicable"
    assert not_applicable["activation_granted"] is False
    assert not_applicable["database_writes"] is False


def test_forward_uses_current_full_prior_seal_without_equating_original_hash(engine, monkeypatch):
    _handoff(engine)
    monkeypatch.setattr(
        ledger, "_validate_qmt_edge_release_activation_trigger_seal",
        lambda _connection, *, expected_build_sha: {
            **_seal(expected_build_sha),
            "trigger_inventory_table_comment": "post-cutover-compatible-seal",
        },
    )
    assert _forward(engine)["status"] == "inserted"


def test_forward_rejects_unknown_terminal_and_rolls_back_without_new_hold(engine):
    _handoff(engine)
    with engine.begin() as connection:
        connection.execute(text(
            "INSERT INTO st_scheduled_task_history (run_uid, task_id, task_name, task_type, "
            "run_at, finished_at, status, duration, exit_code, output, host_name, "
            "scheduler_instance_id, build_sha, trigger_source) VALUES "
            "(:uid,7,'unknown','qmt_edge_release_request',:at,:at,'success',0,0,:output,"
            "'linux-release',:attempt,:build,'release_activation')"
        ), {
            "uid": ledger.qmt_edge_release_activation_run_uid(ATTEMPT),
            "at": (AT + timedelta(seconds=3)).isoformat(sep=" "),
            "output": json.dumps({"schema": "unknown"}),
            "attempt": ATTEMPT, "build": NEW,
        })
    with pytest.raises(RuntimeError, match="terminal is unknown or malformed"):
        _forward(engine)
    with engine.connect() as connection:
        assert recovery.latest_hold(connection)["deployment_attempt_id"] == ATTEMPT


def test_repeated_forward_inherits_original_identity_and_rejects_intermediate_writer(engine):
    _handoff(engine)
    first = _forward(engine)
    second = _forward(
        engine, target=FOURTH, attempt=FOURTH_ATTEMPT, seconds=5,
    )
    assert second["context"]["supersession_depth"] == 2
    assert second["context"]["supersedes_context_hash"] == first["context"]["context_hash"]
    assert second["context"]["original_prior_build_sha"] == OLD
    assert not _ready(engine, OLD)
    assert not _ready(engine, NEW)
    assert not _ready(engine, NEXT)
    assert not _ready(engine, FOURTH)
    with pytest.raises(RuntimeError, match="outside protected handoff"):
        bootstrap.select_update_target(engine, expected_build_sha=NEXT)


def test_forward_depth_limit_means_32_edges_and_33_readable_context_nodes(engine):
    root = _handoff(engine)["context"]
    with engine.begin() as connection:
        previous_hold = recovery.latest_hold(connection)
        previous_context = root
        for depth in range(1, recovery.MAX_FORWARD_SUPERSESSION_DEPTH + 1):
            hold = ledger.build_qmt_edge_release_quiescence_hold(
                build_sha=f"{depth + 4:040x}",
                deployment_attempt_id=f"{depth + 4:032x}",
                requested_at=AT + timedelta(seconds=depth + 2),
            )
            ledger.insert_qmt_edge_release_quiescence_hold(connection, hold)
            context = recovery.build_forward_context(
                hold=hold, superseded_hold=previous_hold,
                superseded_context=previous_context,
                superseded_at=AT + timedelta(seconds=depth + 2),
            )
            recovery.insert_forward_context(connection, context)
            previous_hold, previous_context = hold, context
        assert len(recovery.load_context_chain(connection, previous_hold)) == 33
        extra_hold = ledger.build_qmt_edge_release_quiescence_hold(
            build_sha="f" * 40, deployment_attempt_id="f" * 32,
            requested_at=AT + timedelta(seconds=40),
        )
        with pytest.raises(ledger.QmtEdgeReleaseReceiptError, match="repeats protected identity"):
            recovery.build_forward_context(
                hold=extra_hold, superseded_hold=previous_hold,
                superseded_context=previous_context,
                superseded_at=AT + timedelta(seconds=40),
            )
