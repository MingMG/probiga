"""Execute activation/seal SQL, signatures and owner commits against a private DB.

SQLite adapts only MySQL dialect/session metadata; neither the seal validator nor
the hold/context/grant readers are stubbed. A per-statement delay models the
observed cumulative WAN RTT, rather than treating connection reuse as a budget.
This is a boundary regression, not live TLS or business-data acceptance.
"""

from datetime import datetime, timedelta
import re
import threading
import time
from types import SimpleNamespace

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
import pytest
from sqlalchemy import create_engine, event, text

from server.api import scheduler_runtime as runtime
from server.common import component_release as components
from server.common import component_release_ledger as signed_ledger
from server.common import qmt_edge_release_receipt as activation
from server.common import qmt_edge_release_recovery as recovery
from server.engine import strategy_governance as governance


BUILD = "1" * 40
OTHER = "2" * 40
PRIOR = "3" * 40
SERVER_UUID = "12345678-1234-1234-1234-123456789abc"
REQUEST_AT = datetime(2026, 10, 11, 16, 25, 46)


def _component(build=BUILD, *, contract=BUILD, digest="d" * 64):
    return components.build_component_release(
        linux_build_sha=build, windows_build_sha=contract,
        contract_build_sha=contract, contract_sha256=digest,
        parent_linux_build_sha=PRIOR,
        scope="COORDINATED" if build == contract else "LINUX",
        created_at="2026-10-11T08:25:46Z",
    )


@pytest.fixture
def activation_db(tmp_path, monkeypatch):
    case = SimpleNamespace(statements=[], rtt=0.0, actual=BUILD, contract=BUILD)
    engine = create_engine(
        f"sqlite+pysqlite:///{(tmp_path / 'runtime.sqlite').as_posix()}",
        connect_args={"check_same_thread": False, "timeout": 1}, future=True,
    )
    case.engine = engine
    key = Ed25519PrivateKey.generate()
    case.key = key
    case.now = datetime.now().replace(microsecond=0)

    def connection_setup(dbapi, _record):
        dbapi.execute("ATTACH DATABASE ? AS information_schema", (
            str(tmp_path / "metadata.sqlite"),
        ))
        dbapi.execute("ATTACH DATABASE ? AS probiga", (
            str(tmp_path / "component.sqlite"),
        ))
        dbapi.create_function("DATABASE", 0, lambda: "probiga")
        dbapi.create_function("CURRENT_USER", 0, lambda: "probiga_runtime@127.0.0.1")
        dbapi.create_function("USER", 0, lambda: "probiga_runtime@127.0.0.1")
        dbapi.create_function("SERVER_UUID", 0, lambda: SERVER_UUID)
        dbapi.create_function("NOW", 0, lambda: case.now.isoformat(" "))
        dbapi.create_function("TIMESTAMPDIFF", 3, lambda unit, before, after:
            int((datetime.fromisoformat(after) - datetime.fromisoformat(before)).total_seconds())
            if unit == "SECOND" else None)

    def mysql_spelling(_connection, _cursor, statement, params, _context, _many):
        case.statements.append(statement)
        if case.rtt:
            time.sleep(case.rtt)
        statement = statement.replace("@@server_uuid", "SERVER_UUID()")
        statement = re.sub(r"TIMESTAMPDIFF\(\s*SECOND\s*,",
                           "TIMESTAMPDIFF('SECOND',", statement, flags=re.IGNORECASE)
        if statement == "SHOW SESSION STATUS LIKE 'Ssl_cipher'":
            statement = "SELECT Value FROM test_session_status WHERE Variable_name='Ssl_cipher'"
        return statement.replace(" FOR UPDATE", ""), params

    event.listen(engine, "connect", connection_setup)
    event.listen(engine, "before_cursor_execute", mysql_spelling, retval=True)
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE information_schema.TABLES (
            TABLE_SCHEMA TEXT, TABLE_NAME TEXT, TABLE_TYPE TEXT, ENGINE TEXT, TABLE_COMMENT TEXT
        )"""))
        conn.execute(text("""CREATE TABLE information_schema.COLUMNS (
            TABLE_SCHEMA TEXT, TABLE_NAME TEXT, COLUMN_NAME TEXT, COLUMN_TYPE TEXT,
            IS_NULLABLE TEXT, COLUMN_DEFAULT TEXT, EXTRA TEXT, COLLATION_NAME TEXT,
            ORDINAL_POSITION INTEGER
        )"""))
        conn.execute(text("""CREATE TABLE information_schema.STATISTICS (
            TABLE_SCHEMA TEXT, TABLE_NAME TEXT, INDEX_NAME TEXT, NON_UNIQUE INTEGER,
            SEQ_IN_INDEX INTEGER, COLUMN_NAME TEXT, SUB_PART INTEGER, EXPRESSION TEXT
        )"""))
        conn.execute(text("""CREATE TABLE probiga.st_component_release_manifest (
            linux_build_sha TEXT PRIMARY KEY, manifest_json TEXT, signature TEXT
        )"""))
        conn.execute(text("CREATE TABLE test_session_status (Variable_name TEXT, Value TEXT)"))
        conn.execute(text("INSERT INTO test_session_status VALUES ('Ssl_cipher','TEST_CIPHER')"))
        conn.execute(text("CREATE TABLE st_scheduled_tasks (id INTEGER PRIMARY KEY, task_type TEXT)"))
        conn.execute(text("INSERT INTO st_scheduled_tasks VALUES (7,'qmt_reference_incremental')"))
        conn.execute(text("""CREATE TABLE st_scheduled_task_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT, run_uid TEXT UNIQUE, task_id INTEGER,
            task_name TEXT, task_type TEXT, run_at DATETIME, finished_at DATETIME,
            status TEXT, duration INTEGER, exit_code INTEGER, output TEXT, host_name TEXT,
            scheduler_instance_id TEXT, build_sha TEXT, trigger_source TEXT
        )"""))
        conn.execute(text("""CREATE TABLE st_scheduler_runtime (
            instance_id TEXT PRIMARY KEY, mode TEXT, host_name TEXT, pid INTEGER,
            build_sha TEXT, executor_role TEXT, started_at DATETIME, heartbeat_at DATETIME,
            poll_seconds INTEGER, max_concurrent_tasks INTEGER
        )"""))
        conn.execute(text("CREATE TABLE owner_commits (heartbeat_at TEXT)"))
        conn.execute(text("""CREATE TRIGGER observe_owner_renew AFTER UPDATE OF heartbeat_at
            ON st_scheduler_runtime BEGIN
                INSERT INTO owner_commits VALUES (NEW.heartbeat_at);
            END"""))
        conn.execute(text("""INSERT INTO information_schema.TABLES VALUES
            ('probiga',:table,'BASE TABLE','InnoDB',:comment)"""), {
            "table": signed_ledger.TABLE_NAME,
            "comment": signed_ledger.build_component_ledger_metadata(
                signed_ledger.signing_public_key(key)),
        })
        for index, (name, kind, collation) in enumerate((
            ("linux_build_sha", "char(40)", "ascii_bin"),
            ("manifest_json", "text", "utf8mb4_bin"),
            ("signature", "char(88)", "ascii_bin"),
        ), 1):
            conn.execute(text("""INSERT INTO information_schema.COLUMNS VALUES
                ('probiga',:table,:name,:kind,'NO',NULL,'',:collation,:ordinal)"""), {
                "table": signed_ledger.TABLE_NAME, "name": name, "kind": kind,
                "collation": collation, "ordinal": index,
            })
        conn.execute(text("""INSERT INTO information_schema.STATISTICS VALUES
            ('probiga',:table,'PRIMARY',0,1,'linux_build_sha',NULL,NULL)"""), {
            "table": signed_ledger.TABLE_NAME,
        })
        seal = governance.privileged_trigger_inventory_seal_identity(
            BUILD, server_uuid=SERVER_UUID, database_name="probiga")
        conn.execute(text("""INSERT INTO information_schema.TABLES VALUES
            ('probiga',:table,'BASE TABLE','InnoDB',:comment)"""), {
            "table": seal["seal_table"], "comment": seal["table_comment"],
        })

    def put_component(manifest):
        encoded, signature = signed_ledger.sign_component_manifest(manifest, key)
        with engine.begin() as conn:
            conn.execute(text("""INSERT OR REPLACE INTO probiga.st_component_release_manifest
                VALUES (:build,:encoded,:signature)"""), {
                "build": manifest["linux_build_sha"], "encoded": encoded, "signature": signature,
            })

    def hold(attempt="a" * 32, *, build=BUILD, at=REQUEST_AT, granted=True):
        request = activation.build_qmt_edge_release_request(build_sha=build, requested_at=at)
        value = activation.build_qmt_edge_release_quiescence_hold(
            build_sha=build, deployment_attempt_id=attempt, requested_at=at)
        context = recovery.build_context(
            hold=value, prior_build_sha=PRIOR, prior_host_name="old-host",
            prior_pid=123, prior_instance_id="old-host-123", prior_seal_hash="e" * 64,
            captured_at=at,
        )
        with engine.begin() as conn:
            activation.insert_qmt_edge_release_request(conn, request)
            activation.insert_qmt_edge_release_quiescence_hold(conn, value)
            recovery.insert_context(conn, context)
            if granted:
                grant = activation.build_qmt_edge_release_activation_grant(
                    hold=value, granted_at=at + timedelta(seconds=1))
                activation.insert_qmt_edge_release_activation_grant(conn, grant)
        return value

    case.put_component = put_component
    case.hold = hold
    put_component(_component())
    hold()
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", BUILD)
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", BUILD)
    monkeypatch.setenv("PROBIGA_SCHEDULER_EXECUTOR_ROLE", "qmt_windows_edge")
    # OS-local manifest lookup is a separate tested boundary. No DB proof,
    # signature, metadata validator, or activation reader is replaced here.
    monkeypatch.setattr(components, "runtime_contract_build_sha", lambda **_k: case.contract)
    monkeypatch.setattr(components, "load_runtime_component_release", lambda: _component(case.actual))
    case.statements.clear()
    try:
        yield case
    finally:
        engine.dispose()


def _component_reads(case):
    return [sql for sql in case.statements
            if "FROM probiga.st_component_release_manifest" in sql]


def test_same_build_seal_executes_one_complete_signed_component_proof(activation_db):
    case = activation_db
    with case.engine.connect() as conn:
        ready, detail = activation.check_qmt_edge_release_activation(conn, expected_build_sha=BUILD)
    assert ready is True and detail["status"] == "READY"
    assert detail["grant"]["real_order"] is False
    assert len(_component_reads(case)) == 1
    # Full seal (8), task ID (1), latest hold/context/abort (3), exact
    # hold/request/grant (3). No hold, grant, identity or TLS proof is omitted.
    assert len(case.statements) == 15


def test_same_connection_next_activation_reads_new_hold_without_reusing_old_grant(activation_db):
    case = activation_db
    with case.engine.connect() as conn:
        assert activation.check_qmt_edge_release_activation(conn, expected_build_sha=BUILD)[0]
        conn.rollback()
        next_hold = case.hold("b" * 32, at=REQUEST_AT + timedelta(minutes=1), granted=False)
        case.statements.clear()
        ready, detail = activation.check_qmt_edge_release_activation(conn, expected_build_sha=BUILD)
    assert ready is False and detail["status"] == "PENDING"
    assert detail["hold"] == next_hold
    assert detail["deployment_attempt_id"] == "b" * 32
    assert detail["grant"] is None
    assert len(_component_reads(case)) == 1
    assert len(case.statements) == 15


def test_same_connection_next_seal_rejects_new_signature_tamper(activation_db):
    case = activation_db
    with case.engine.connect() as conn:
        assert activation.check_qmt_edge_release_activation(conn, expected_build_sha=BUILD)[0]
        conn.rollback()
        with case.engine.begin() as writer:
            writer.execute(text("""UPDATE probiga.st_component_release_manifest
                SET signature=:signature WHERE linux_build_sha=:build"""), {
                "signature": "A" * 88, "build": BUILD,
            })
        case.statements.clear()
        with pytest.raises(activation.QmtEdgeReleaseReceiptError, match="trigger seal is unavailable"):
            activation.check_qmt_edge_release_activation(conn, expected_build_sha=BUILD)
    assert len(_component_reads(case)) == 1
    assert not any("st_scheduled_task_history" in sql for sql in case.statements)


def test_same_connection_next_seal_rechecks_component_physical_metadata(activation_db):
    case = activation_db
    with case.engine.connect() as conn:
        assert activation.check_qmt_edge_release_activation(conn, expected_build_sha=BUILD)[0]
        conn.rollback()
        with case.engine.begin() as writer:
            writer.execute(text("""UPDATE information_schema.COLUMNS SET EXTRA='GENERATED'
                WHERE TABLE_NAME=:table AND COLUMN_NAME='signature'"""), {
                "table": signed_ledger.TABLE_NAME,
            })
        case.statements.clear()
        with pytest.raises(activation.QmtEdgeReleaseReceiptError, match="trigger seal is unavailable"):
            activation.check_qmt_edge_release_activation(conn, expected_build_sha=BUILD)
    assert not _component_reads(case)  # Drift is rejected before the signed row.
    assert not any("st_scheduled_task_history" in sql for sql in case.statements)


def test_other_build_global_hold_fences_an_existing_exact_grant(activation_db):
    case = activation_db
    with case.engine.connect() as conn:
        assert activation.check_qmt_edge_release_activation(conn, expected_build_sha=BUILD)[0]
        conn.rollback()
        case.hold("c" * 32, build=OTHER, at=REQUEST_AT + timedelta(minutes=2), granted=False)
        case.statements.clear()
        ready, detail = activation.check_qmt_edge_release_activation(conn, expected_build_sha=BUILD)
    assert ready is False and detail["status"] == "PENDING"
    assert detail["activation_granted"] is False
    assert detail["hold"] is None and detail["grant"] is None
    assert len(_component_reads(case)) == 1


@pytest.mark.parametrize("tampered_build", [None, BUILD, OTHER])
def test_different_anchor_build_requires_both_real_component_signatures(
    activation_db, monkeypatch, tampered_build,
):
    case = activation_db
    case.actual = OTHER
    case.put_component(_component(OTHER))
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", OTHER)
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", OTHER)
    monkeypatch.setenv("PROBIGA_SCHEDULER_EXECUTOR_ROLE", "linux_standalone")
    if tampered_build:
        with case.engine.begin() as conn:
            conn.execute(text("""UPDATE probiga.st_component_release_manifest
                SET signature=:signature WHERE linux_build_sha=:build"""), {
                "signature": "A" * 88, "build": tampered_build,
            })
    case.statements.clear()
    with case.engine.connect() as conn:
        if tampered_build:
            with pytest.raises(signed_ledger.ComponentLedgerError, match="signature differs"):
                governance.validate_privileged_trigger_migration_seal(conn, expected_build_sha=BUILD)
        else:
            seal = governance.validate_privileged_trigger_migration_seal(conn, expected_build_sha=BUILD)
            assert seal["attested_build_sha"] == BUILD
            assert seal["runtime_build_sha"] == OTHER
    expected_reads = 1 if tampered_build == OTHER else 2
    assert len(_component_reads(case)) == expected_reads


def test_validly_signed_different_component_contract_is_not_compatible(
    activation_db, monkeypatch,
):
    case = activation_db
    case.actual = OTHER
    case.put_component(_component(OTHER, digest="f" * 64))
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", OTHER)
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", OTHER)
    monkeypatch.setenv("PROBIGA_SCHEDULER_EXECUTOR_ROLE", "linux_standalone")
    monkeypatch.setattr(components, "load_runtime_component_release",
                        lambda: _component(OTHER, digest="f" * 64))
    case.statements.clear()
    with case.engine.connect() as conn, pytest.raises(RuntimeError, match="component producer contract differs"):
        governance.validate_privileged_trigger_migration_seal(conn, expected_build_sha=BUILD)
    assert len(_component_reads(case)) == 2
    assert not any("@@server_uuid" in sql for sql in case.statements)


@pytest.fixture
def sql_owner(activation_db, monkeypatch):
    case = activation_db
    monkeypatch.setattr(runtime, "get_scheduler_runtime_config", lambda: {
        "poll_seconds": 60, "max_concurrent_tasks": 1,
    })
    monkeypatch.setattr(runtime, "_scheduler_executor_role", lambda _mode: "qmt_windows_edge")
    monkeypatch.setattr(runtime, "_scheduler_build_commit_sha", lambda: BUILD)
    monkeypatch.setattr(runtime, "_scheduler_stopping", False)
    monkeypatch.setattr(runtime._WindowsSchedulerLease, "_create_engine", lambda _lease: case.engine)
    lease = runtime._WindowsSchedulerLease(threading.Event())
    # Register once with the real SQL transaction. Subsequent cycles execute
    # the actual renewable-owner CAS and fresh-owner readback unmodified.
    runtime._register_windows_scheduler_heartbeat(case.engine, lease.owner)
    case.statements.clear()
    try:
        yield case, lease
    finally:
        lease.stop()


def test_real_owner_cycle_fits_budget_after_same_call_component_dedup(sql_owner):
    case, lease = sql_owner
    case.rtt = 0.03
    lease.cycle_seconds = 0.60
    began = time.monotonic()
    assert lease._cycle() is True
    elapsed = time.monotonic() - began
    case.rtt = 0.0
    assert len(case.statements) == 17
    assert elapsed < lease.cycle_seconds
    assert lease.verified.is_set()
    # Separate committed readback, not cursor success or a mocked ready bit.
    with case.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM owner_commits")).scalar_one() == 1


def test_over_budget_real_sql_cycle_fails_closed_before_renewal(sql_owner):
    case, lease = sql_owner
    case.rtt = 0.04
    lease.cycle_seconds = 0.14
    began = time.monotonic()
    with pytest.raises(runtime.SchedulerOwnerLeaseLost, match="scheduler_lease_cycle_deadline_exceeded"):
        lease._cycle()
    assert time.monotonic() - began < 1.5
    assert not lease.verified.is_set()
    assert not any("UPDATE st_scheduler_runtime" in sql for sql in case.statements)
    case.rtt = 0.0
    with case.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM owner_commits")).scalar_one() == 0
