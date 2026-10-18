"""Opt-in, private MySQL 8.4/TLS regressions for the Windows owner read cycle.

Set PROBIGA_OWNER_TEST_MYSQLD to an existing mysqld executable. Every test owns
its new pytest tmp datadir and random loopback ports; no service is registered.
The CA, account password and Ed25519 key are generated only for this fixture.
No production credential, signing key, QMT client or business job is used.
PROBIGA_OWNER_TEST_CYCLE_SECONDS optionally sets accelerated rollback/readback
case budgets (0.5 to 7 seconds, default 2). The cumulative activation case uses
the production MAX_CYCLE_SECONDS, irrespective of that setting.

The production engine, driver watchdog, SQL readers, signatures and CAS execute
unchanged. Faults delay actual encrypted server replies in a loopback byte proxy,
not helper return values. This proves guard mechanisms, not the cause of the
2026-10-12 01:35 incident, production data completion, or live trigger inventory.
"""

from __future__ import annotations

from collections import deque
import configparser
from datetime import datetime, timedelta, timezone
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import threading
import time
from types import SimpleNamespace
import uuid

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
import pytest
from sqlalchemy import event, text
from sqlalchemy.engine import URL

from server.api import scheduler_runtime as runtime
from server.common import component_release as components
from server.common import component_release_ledger as signed_ledger
from server.common import engine_factory
from server.common import qmt_edge_release_receipt as activation
from server.common import qmt_edge_release_recovery as recovery
from server.engine import strategy_governance as governance


BUILD = "1" * 40
SUPERSEDED = "2" * 40
ORIGINAL_PRIOR = "3" * 40
FORBIDDEN_PORTS = {3306, 13306, 33085}
pytestmark = pytest.mark.skipif(os.name != "nt", reason="actual Windows owner contract")


def _private_certificates(root: Path) -> Path:
    now = datetime.now(timezone.utc)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "owner-test-private-ca")])
    ca = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
          .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
          .not_valid_before(now - timedelta(hours=1))
          .not_valid_after(now + timedelta(days=2))
          .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
          .sign(ca_key, hashes.SHA256()))
    server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    server_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")])
    cert = (x509.CertificateBuilder().subject_name(server_name).issuer_name(name)
            .public_key(server_key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(hours=1))
            .not_valid_after(now + timedelta(days=2))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.SubjectAlternativeName([
                x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), critical=False)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
            .sign(ca_key, hashes.SHA256()))
    ca_path = root / "test-ca.pem"
    ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    (root / "test-server.pem").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    (root / "test-server-key.pem").write_bytes(server_key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption()))
    return ca_path


def _random_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    assert port not in FORBIDDEN_PORTS
    return port


@pytest.fixture
def native_mysql(tmp_path, monkeypatch):
    configured = os.getenv("PROBIGA_OWNER_TEST_MYSQLD")
    if not configured:
        pytest.skip("explicit private MySQL executable not configured")
    executable = Path(configured).resolve(strict=True)
    assert executable.is_file()
    root = (tmp_path / ("owner-native-" + uuid.uuid4().hex)).resolve()
    root.mkdir()
    data = root / "data"
    data.mkdir()
    ca_path = _private_certificates(root)
    monkeypatch.setattr(engine_factory, "_get_runtime_tls_config", lambda: {
        "required": True, "ssl_ca": str(ca_path),
    })
    port = _random_port()
    common = [str(executable), "--no-defaults", f"--datadir={data}",
              "--innodb-buffer-pool-size=64M", "--innodb-redo-log-capacity=64M"]
    initialized = subprocess.run(common + ["--initialize-insecure"],
                                 capture_output=True, timeout=90,
                                 creationflags=subprocess.CREATE_NO_WINDOW)
    assert initialized.returncode == 0, initialized.stderr.decode(errors="replace")[-2000:]
    auto = configparser.ConfigParser()
    assert auto.read(data / "auto.cnf", encoding="utf-8")
    expected_uuid = str(uuid.UUID(auto["auto"]["server-uuid"]))
    # initialize-insecure creates root@localhost, not a TCP 127.0.0.1
    # principal with skip-name-resolve. Bootstrap only this private native
    # server through its own init-file, never a plaintext client connection.
    admin_name = "owner_admin_" + secrets.token_hex(8)
    admin_password = secrets.token_hex(32)
    init_file = root / "private-admin-init.sql"
    init_file.write_text(
        f"CREATE USER '{admin_name}'@'127.0.0.1' IDENTIFIED BY '{admin_password}' REQUIRE SSL;\n"
        f"GRANT ALL PRIVILEGES ON *.* TO '{admin_name}'@'127.0.0.1' WITH GRANT OPTION;\n",
        encoding="ascii")
    log = (root / "server.log").open("wb")
    process = subprocess.Popen(common + [
        f"--port={port}", "--bind-address=127.0.0.1", "--mysqlx=OFF",
        "--skip-log-bin", "--skip-name-resolve", "--max-connections=12",
        f"--init-file={init_file}",
        f"--ssl-ca={ca_path}", f"--ssl-cert={root / 'test-server.pem'}",
        f"--ssl-key={root / 'test-server-key.pem'}", "--require-secure-transport=ON",
    ], stdout=log, stderr=log, creationflags=subprocess.CREATE_NO_WINDOW)
    try:
        admin = engine_factory.create_pooled_engine(
            URL.create("mysql+pymysql", username=admin_name, password=admin_password,
                       host="127.0.0.1", port=port, database="mysql").render_as_string(hide_password=False),
            pool_pre_ping=False, pool_size=1, max_overflow=0, pool_timeout=1,
            connect_args={"connect_timeout": 2, "read_timeout": 3, "write_timeout": 3})
    except BaseException:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        log.close()
        raise

    def assert_isolated():
        assert process.pid > 0 and process.poll() is None
        assert port not in FORBIDDEN_PORTS
        with admin.connect() as conn:
            facts = conn.execute(text(
                "SELECT @@datadir AS datadir, @@port AS port, @@version AS version, "
                "@@server_uuid AS server_uuid, CURRENT_USER() AS current_identity, "
                "USER() AS session_identity")).mappings().one()
            assert Path(facts["datadir"]).resolve() == data
            assert int(facts["port"]) == port
            assert str(facts["version"]).startswith("8.4.")
            assert facts["server_uuid"] == expected_uuid
            assert facts["current_identity"] == facts["session_identity"] == f"{admin_name}@127.0.0.1"
            assert conn.exec_driver_sql("SHOW SESSION STATUS LIKE 'Ssl_cipher'").one()[1]

    try:
        deadline = time.monotonic() + 45
        for attempt in range(225):
            assert process.poll() is None, f"private mysqld exited; retained log: {root}"
            try:
                assert_isolated()
                break
            except Exception as exc:
                driver_error = getattr(exc, "orig", exc)
                if getattr(driver_error, "args", ())[:1] in ((1130,), (1045,)):
                    raise  # Authentication policy is not a startup transient.
                if attempt == 224 or time.monotonic() >= deadline:
                    raise
                time.sleep(min(0.2, max(0, deadline - time.monotonic())))
        password = secrets.token_urlsafe(24)
        assert_isolated()  # Check physical authority before any fixture DDL.
        with admin.begin() as conn:
            conn.exec_driver_sql("CREATE DATABASE probiga CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
            conn.exec_driver_sql("CREATE USER 'probiga_runtime'@'127.0.0.1' IDENTIFIED BY %s REQUIRE SSL",
                                 (password,))
            conn.exec_driver_sql("GRANT SELECT ON probiga.* TO 'probiga_runtime'@'127.0.0.1'")
        yield SimpleNamespace(root=root, data=data, port=port, process=process,
                              admin=admin, ca=ca_path, uuid=expected_uuid,
                              password=password, assert_isolated=assert_isolated)
    finally:
        try:
            if process.poll() is None:
                try:
                    assert_isolated()
                    with admin.connect() as conn:
                        conn.exec_driver_sql("SHUTDOWN")
                except Exception:
                    pass
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.terminate()  # Only this fixture's held Popen identity.
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
            assert process.poll() is not None, "private mysqld did not become terminal"
        finally:
            admin.dispose()
            log.close()
        # Keep the private datadir, certificates and error log for diagnosis.


class _TlsByteProxy:
    """Forward TLS without decrypting it; delay only bytes actually received."""

    def __init__(self, upstream_port: int):
        self.upstream_port = upstream_port
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.pending = deque()
        self.hits = []
        self.sockets = []
        self.threads = []
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(4)
        self.listener.settimeout(0.1)
        self.port = self.listener.getsockname()[1]
        assert self.port not in FORBIDDEN_PORTS and self.port != upstream_port
        self.acceptor = threading.Thread(target=self._accept, name="test-owner-tls-proxy", daemon=True)
        self.acceptor.start()

    def delay_next_reply(self, stage: str, delay: float):
        with self.lock:
            self.pending.append((stage, delay))

    def _accept(self):
        while not self.stop.is_set():
            try:
                client, _address = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            try:
                upstream = socket.create_connection(("127.0.0.1", self.upstream_port), timeout=1)
            except OSError:
                client.close()
                continue
            if self.stop.is_set():
                client.close()
                upstream.close()
                return
            client.settimeout(0.1)
            upstream.settimeout(0.1)
            with self.lock:
                self.sockets.extend((client, upstream))
            for source, target, downstream in ((client, upstream, False), (upstream, client, True)):
                thread = threading.Thread(target=self._pump, args=(source, target, downstream),
                                          name="test-owner-tls-bytes", daemon=True)
                with self.lock:
                    self.threads.append(thread)
                thread.start()

    def _pump(self, source, target, downstream):
        try:
            while not self.stop.is_set():
                try:
                    payload = source.recv(65536)
                except socket.timeout:
                    continue
                if not payload:
                    return
                if downstream:
                    with self.lock:
                        fault = self.pending.popleft() if self.pending else None
                        if fault:
                            self.hits.append((fault[0], len(payload), time.monotonic()))
                    if fault and self.stop.wait(fault[1]):
                        return
                target.sendall(payload)  # The server's original encrypted bytes.
        except OSError:
            pass  # A real driver/watchdog disconnect is expected in fault cases.
        finally:
            try:
                target.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def close(self):
        self.stop.set()
        self.listener.close()
        deadline = time.monotonic() + 5
        self.acceptor.join(max(0, deadline - time.monotonic()))
        # The joined acceptor can no longer add a connection after this snapshot.
        with self.lock:
            sockets = list(self.sockets)
        for sock in sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
        with self.lock:
            threads = list(self.threads)
        for thread in threads:
            thread.join(max(0, deadline - time.monotonic()))
        assert not self.acceptor.is_alive() and not any(t.is_alive() for t in threads)


def _prepare_signed_forward_ledger(native):
    native.assert_isolated()
    key = ed25519.Ed25519PrivateKey.generate()
    manifest = components.build_component_release(
        linux_build_sha=BUILD, windows_build_sha=BUILD, contract_build_sha=BUILD,
        contract_sha256="d" * 64, parent_linux_build_sha=SUPERSEDED,
        scope="COORDINATED", created_at="2026-10-11T08:25:46Z")
    encoded, signature = signed_ledger.sign_component_manifest(manifest, key)
    seal = governance.privileged_trigger_inventory_seal_identity(
        BUILD, server_uuid=native.uuid, database_name="probiga")
    with native.admin.begin() as conn:
        conn.exec_driver_sql("USE probiga")
        conn.execute(text(
            "CREATE TABLE st_component_release_manifest ("
            "linux_build_sha CHAR(40) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, "
            "manifest_json TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, "
            "signature CHAR(88) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, "
            "PRIMARY KEY (linux_build_sha)) ENGINE=InnoDB "
            "DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT=:comment"),
            {"comment": signed_ledger.build_component_ledger_metadata(signed_ledger.signing_public_key(key))})
        conn.execute(text("INSERT INTO st_component_release_manifest VALUES (:build,:manifest,:signature)"),
                     {"build": BUILD, "manifest": encoded, "signature": signature})
        conn.execute(text(f"CREATE TABLE `{seal['seal_table']}` (id INT PRIMARY KEY) "
                          "ENGINE=InnoDB COMMENT=:comment"), {"comment": seal["table_comment"]})
        conn.exec_driver_sql("CREATE TABLE st_scheduled_tasks (id BIGINT PRIMARY KEY, task_type VARCHAR(128)) ENGINE=InnoDB")
        conn.exec_driver_sql("INSERT INTO st_scheduled_tasks VALUES (7,'qmt_reference_incremental')")
        conn.exec_driver_sql("""CREATE TABLE st_scheduled_task_history (
            id BIGINT AUTO_INCREMENT PRIMARY KEY, run_uid VARCHAR(128) NOT NULL UNIQUE,
            task_id BIGINT, task_name VARCHAR(255), task_type VARCHAR(128),
            run_at DATETIME, finished_at DATETIME, status VARCHAR(32), duration INT NOT NULL DEFAULT 0,
            exit_code INT, output LONGTEXT, host_name VARCHAR(128),
            scheduler_instance_id VARCHAR(128), build_sha CHAR(40), trigger_source VARCHAR(64)
        ) ENGINE=InnoDB""")
        conn.exec_driver_sql("""CREATE TABLE st_scheduler_runtime (
            instance_id VARCHAR(128) PRIMARY KEY, mode VARCHAR(32), host_name VARCHAR(128),
            pid BIGINT, build_sha CHAR(40), executor_role VARCHAR(64), started_at DATETIME,
            heartbeat_at DATETIME, poll_seconds INT, max_concurrent_tasks INT
        ) ENGINE=InnoDB""")
        conn.exec_driver_sql("""CREATE TABLE owner_commits (
            id BIGINT AUTO_INCREMENT PRIMARY KEY, instance_id VARCHAR(128),
            heartbeat_at DATETIME, connection_id BIGINT
        ) ENGINE=InnoDB""")
        conn.exec_driver_sql("""CREATE TRIGGER test_owner_renew AFTER UPDATE ON st_scheduler_runtime
            FOR EACH ROW INSERT INTO owner_commits (instance_id,heartbeat_at,connection_id)
            VALUES (NEW.instance_id,NEW.heartbeat_at,CONNECTION_ID())""")
        conn.exec_driver_sql("GRANT INSERT,UPDATE,DELETE ON probiga.st_scheduler_runtime TO 'probiga_runtime'@'127.0.0.1'")
        now = conn.exec_driver_sql("SELECT NOW()").scalar_one().replace(microsecond=0)
        base_at = now - timedelta(seconds=2)
        base_request = activation.build_qmt_edge_release_request(build_sha=SUPERSEDED, requested_at=base_at)
        base_hold = activation.build_qmt_edge_release_quiescence_hold(
            build_sha=SUPERSEDED, deployment_attempt_id="a" * 32, requested_at=base_at)
        base_context = recovery.build_context(
            hold=base_hold, prior_build_sha=ORIGINAL_PRIOR, prior_host_name="fixture-prior",
            prior_pid=123, prior_instance_id="fixture-prior-123", prior_seal_hash="e" * 64,
            captured_at=base_at)
        activation.insert_qmt_edge_release_request(conn, base_request)
        activation.insert_qmt_edge_release_quiescence_hold(conn, base_hold)
        recovery.insert_context(conn, base_context)
        request = activation.build_qmt_edge_release_request(build_sha=BUILD, requested_at=now)
        hold = activation.build_qmt_edge_release_quiescence_hold(
            build_sha=BUILD, deployment_attempt_id="b" * 32, requested_at=now)
        activation.insert_qmt_edge_release_request(conn, request)
        activation.insert_qmt_edge_release_quiescence_hold(conn, hold)
        forward = recovery.build_forward_context(
            hold=hold, superseded_hold=base_hold, superseded_context=base_context, superseded_at=now)
        recovery.insert_forward_context(conn, forward)
        grant = activation.build_qmt_edge_release_activation_grant(
            hold=hold, granted_at=now + timedelta(seconds=1))
        activation.insert_qmt_edge_release_activation_grant(conn, grant)
    return hold, grant


@pytest.fixture
def native_owner(native_mysql, monkeypatch, request):
    native = native_mysql
    budget = float(os.getenv("PROBIGA_OWNER_TEST_CYCLE_SECONDS", "2"))
    assert 0.5 <= budget <= 7  # Below the unchanged driver socket timeout of 8s.
    hold, grant = _prepare_signed_forward_ledger(native)
    proxy = _TlsByteProxy(native.port)
    url = URL.create("mysql+pymysql", username="probiga_runtime", password=native.password,
                     host="127.0.0.1", port=proxy.port, database="probiga")
    monkeypatch.setattr(runtime, "get_mysql_url", lambda **_kwargs: url.render_as_string(hide_password=False))
    for name in ("PROBIGA_BUILD_COMMIT_SHA", "PROBIGA_EXPECTED_GIT_SHA", "EXPECTED_GIT_SHA"):
        monkeypatch.setenv(name, BUILD)
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_SCHEDULER_EXECUTOR_ROLE", "qmt_windows_edge")
    monkeypatch.setattr(runtime, "_scheduler_stopping", False)
    try:
        lease = runtime._WindowsSchedulerLease(threading.Event())  # No worker/start call.
    except BaseException:
        proxy.close()
        raise
    case = SimpleNamespace(native=native, proxy=proxy, lease=lease, hold=hold, grant=grant,
                           mode=None, in_activation=False, statements=[], completed=[],
                           ready=[], drivers=[], watchdog=threading.Event(), test_node=request.node)
    case.budget = budget

    def driver_seen(dbapi, _record):
        case.drivers.append(dbapi)

    def before_sql(_conn, _cursor, statement, _params, _context, _many):
        if case.in_activation:
            case.statements.append(statement)
            if case.mode == "activation_cumulative":
                proxy.delay_next_reply("activation_sql", case.budget / 8)
        elif (case.mode == "postcommit_readback" and "FROM st_scheduler_runtime" in statement
              and "ORDER BY heartbeat_at DESC" in statement):
            case.fault_statement = statement
            proxy.delay_next_reply("postcommit_readback", case.budget * 2)

    def after_sql(_conn, _cursor, statement, _params, _context, _many):
        if case.in_activation:
            case.completed.append(statement)

    original_check = runtime.check_qmt_edge_release_activation

    def observed_check(*args, **kwargs):
        case.in_activation = True
        try:
            result = original_check(*args, **kwargs)
            if result[0]:
                case.ready.append(result[1])
            return result  # Actual proof result; never substitute READY.
        finally:
            case.in_activation = False

    original_abort = lease._abort_cycle_transport

    def observed_abort():
        case.watchdog.set()
        return original_abort()

    original_rollback = lease.engine.dialect.loaded_dbapi.connections.Connection.rollback

    def observed_rollback(dbapi):
        if (case.mode == "activation_rollback" and case.ready
                and any(dbapi is known for known in case.drivers)):
            case.mode = "rollback_armed"
            proxy.delay_next_reply("activation_rollback", case.budget * 2)
        return original_rollback(dbapi)  # Actual COM_QUERY/ROLLBACK and driver read.

    event.listen(lease.engine, "connect", driver_seen)
    event.listen(lease.engine, "before_cursor_execute", before_sql)
    event.listen(lease.engine, "after_cursor_execute", after_sql)
    monkeypatch.setattr(runtime, "check_qmt_edge_release_activation", observed_check)
    monkeypatch.setattr(lease, "_abort_cycle_transport", observed_abort)
    monkeypatch.setattr(lease.engine.dialect.loaded_dbapi.connections.Connection, "rollback", observed_rollback)
    try:
        native.assert_isolated()
        # Cold setup executes the actual full proof, registration and readback
        # inside the original deadline/watchdog. A newly created lease has no
        # deadline, so directly using its engine would correctly fail closed.
        assert lease._cycle(initial=True) is True
        assert not lease._cycle_dbapis and not lease._cycle_sockets
        case.cold_evidence = lease._last_cycle_evidence
        # Independent direct TLS read verifies private authority and a real
        # authenticated runtime session before any response fault is armed.
        with lease.engine.connect() as conn:
            identity = conn.exec_driver_sql("SELECT @@server_uuid,DATABASE(),CURRENT_USER(),USER()").one()
            assert tuple(identity) == (native.uuid, "probiga", "probiga_runtime@127.0.0.1", "probiga_runtime@127.0.0.1")
            assert conn.exec_driver_sql("SHOW SESSION STATUS LIKE 'Ssl_cipher'").one()[1]
        with native.admin.begin() as conn:
            conn.exec_driver_sql("USE probiga")
            conn.execute(text("UPDATE st_scheduler_runtime SET heartbeat_at=DATE_SUB(NOW(),INTERVAL 2 SECOND) "
                              "WHERE instance_id=:instance"), {"instance": lease.owner["instance_id"]})
            conn.exec_driver_sql("TRUNCATE TABLE owner_commits")
        case.statements.clear()
        case.completed.clear()
        case.ready.clear()
        case.watchdog.clear()
        lease.verified.clear()  # Each tested cycle needs a fresh complete proof.
        case.before = _readback(case)
        yield case
    finally:
        try:
            lease.stop()
        finally:
            proxy.close()
        assert lease.thread is None and not lease.verified.is_set()
        # dispose sends the real COM_QUIT through OwnerDeadlineConnection,
        # which may register references after the final cycle cleanup. The
        # driver's finally closes them; closed references are not live leaks.
        assert all(dbapi._sock is None or dbapi._sock.fileno() < 0
                   for dbapi in lease._cycle_dbapis.values())
        assert all(sock.fileno() < 0 for sock in lease._cycle_sockets.values())
        assert case.drivers and all(dbapi._sock is None or dbapi._sock.fileno() < 0
                                    for dbapi in case.drivers)
        assert not proxy.acceptor.is_alive() and not any(t.is_alive() for t in proxy.threads)
        native.assert_isolated()


def _readback(case):
    case.native.assert_isolated()
    with case.native.admin.connect() as conn:
        conn.exec_driver_sql("USE probiga")
        owner = dict(conn.execute(text("SELECT * FROM st_scheduler_runtime WHERE instance_id=:instance"),
                                  {"instance": case.lease.owner["instance_id"]}).mappings().one())
        commits = list(conn.exec_driver_sql("SELECT instance_id,heartbeat_at,connection_id FROM owner_commits ORDER BY id"))
        control = list(conn.exec_driver_sql("SELECT run_uid,build_sha,status,output FROM st_scheduled_task_history ORDER BY id"))
    return SimpleNamespace(owner=owner, commits=commits, control=control)


def _failed_cycle(case, stage):
    case.native.assert_isolated()
    case.lease.cycle_seconds = case.budget
    began = time.monotonic()
    with pytest.raises(runtime.SchedulerOwnerLeaseLost) as loss:
        case.lease._cycle()
    elapsed = time.monotonic() - began
    reason = str(loss.value)
    evidence = case.lease._last_cycle_evidence
    diagnostic = json.dumps({"canonical_reason": reason, "elapsed_seconds": elapsed,
                             "cycle_evidence": evidence}, sort_keys=True)
    case.test_node.user_properties.extend([
        ("lease_loss_reason", reason), ("lease_cycle_evidence", json.dumps(evidence, sort_keys=True)),
        ("lease_cycle_elapsed_seconds", elapsed),
    ])
    deadline_reason = "scheduler_lease_cycle_deadline_exceeded"
    activation_transport_reason = (
        "scheduler_lease_activation_unavailable:qmt_release_activation_check_failed:OperationalError")
    allowed = {
        "activation_sql": {deadline_reason, activation_transport_reason},
        "activation_rollback": {deadline_reason, activation_transport_reason},
        "postcommit_readback": {
            deadline_reason, "scheduler_lease_identity_unavailable:['heartbeat_read_failed']"},
    }
    # A real OS socket timeout capped to remaining time can wake fractionally
    # before the Timer/absolute postcheck. It still has only the exact enclosing
    # stage's fail-closed reason; never accept a generic error or alter budgets.
    assert reason in allowed[stage], diagnostic
    assert case.budget - 2 < elapsed < case.budget + 2, diagnostic
    assert any(hit[0] == stage and hit[1] > 0 for hit in case.proxy.hits), "fault never held a real TLS reply"
    assert not case.lease.verified.is_set() and not case.lease.authorized()
    assert not case.lease._cycle_dbapis and not case.lease._cycle_sockets
    assert case.drivers and all(driver._sock is None or driver._sock.fileno() < 0 for driver in case.drivers)
    expected_stage = {"activation_sql": "activation_query", "activation_rollback": "activation_rollback",
                      "postcommit_readback": "owner_readback_query"}[stage]
    assert evidence["status"] == "FAILED" and evidence["stage"] == expected_stage, diagnostic
    assert evidence["driver_failure"]["stage"] == expected_stage, diagnostic
    assert evidence["budget_ms"] == 1000 * case.budget
    assert evidence["cycle_uid"] != case.cold_evidence["cycle_uid"]
    assert evidence["last_successful_cycle"]["cycle_uid"] == case.cold_evidence["cycle_uid"]
    assert evidence["statement_sha256"] and evidence["last_success"]
    if evidence["watchdog_activity"] is not None:
        assert evidence["watchdog_abort_completed"]
    if stage == "activation_sql":
        assert evidence["statement_sha256"] in {
            hashlib.sha256(query.encode()).hexdigest() for query in case.statements}
        assert evidence["operation"] in {"SELECT", "SHOW"}
    else:
        statement = "ROLLBACK" if stage == "activation_rollback" else case.fault_statement
        assert evidence["statement_sha256"] == hashlib.sha256(statement.encode()).hexdigest()
        assert evidence["operation"] == ("ROLLBACK" if stage == "activation_rollback" else "SELECT")
    serialized = json.dumps(evidence)
    assert case.native.password not in serialized and str(case.native.root) not in serialized
    assert "mysql+pymysql" not in serialized
    after = _readback(case)
    assert after.control == case.before.control  # No new business/control ledger result.
    return after


def test_native_tls_full_forward_activation_then_committed_owner_readback(native_owner):
    case = native_owner
    assert case.lease._cycle() is True
    assert not case.lease._cycle_dbapis and not case.lease._cycle_sockets
    evidence = case.lease._last_cycle_evidence
    assert evidence["status"] == "COMPLETE"
    assert {"connect", "connect_greeting", "tls_authentication", "session_init_query",
            "session_init_charset", "session_init_autocommit"}.issubset(
        case.cold_evidence["phase_elapsed_ms"])
    assert {"activation_query", "activation_rollback", "renew_query", "renew_commit",
            "owner_readback_query", "owner_readback_rollback"}.issubset(evidence["phase_elapsed_ms"])
    assert case.lease.verified.is_set()
    assert len(case.statements) == len(case.completed) == 16  # Actual one-hop FORWARD trace.
    assert len(case.ready) == 1 and case.ready[0]["status"] == "READY"
    assert case.ready[0]["hold"] == case.hold and case.ready[0]["grant"] == case.grant
    assert any("information_schema.STATISTICS" in query for query in case.completed)
    assert sum("WHERE run_uid=" in query or "WHERE run_uid =" in query for query in case.completed) >= 4
    after = _readback(case)
    assert len(after.commits) == 1
    assert after.commits[0][0] == case.lease.owner["instance_id"]
    assert after.commits[0][1] == after.owner["heartbeat_at"]
    assert after.owner["heartbeat_at"] > case.before.owner["heartbeat_at"]
    assert {k: v for k, v in after.owner.items() if k != "heartbeat_at"} == {
        k: v for k, v in case.before.owner.items() if k != "heartbeat_at"}
    assert after.control == case.before.control and not case.proxy.hits


def test_authenticated_activation_reply_delays_share_one_cycle_budget(native_owner):
    case = native_owner
    case.budget = runtime._WindowsSchedulerLease.MAX_CYCLE_SECONDS
    assert case.budget == runtime._WindowsSchedulerLease.MAX_CYCLE_SECONDS
    case.mode = "activation_cumulative"
    after = _failed_cycle(case, "activation_sql")
    assert len(case.completed) >= 2  # Real prior SQL replies completed, not a dead greeting.
    assert not case.ready
    assert after.owner == case.before.owner and not after.commits
    assert not any("UPDATE st_scheduler_runtime" in query for query in case.statements)
    # Exercise a fresh physical connection and full proof after the old watchdog
    # was cancelled/joined. This is a synchronous private guard test, not a
    # production restart or an assertion that daemon retry behaviour is fixed.
    previous_drivers = tuple(case.drivers)
    case.mode = None
    case.statements.clear()
    case.completed.clear()
    case.lease.cycle_seconds = runtime._WindowsSchedulerLease.MAX_CYCLE_SECONDS
    assert case.lease._cycle() is True
    assert not case.lease._cycle_dbapis and not case.lease._cycle_sockets
    assert len(case.completed) == 16 and len(case.ready) == 1
    assert len(case.drivers) > len(previous_drivers)
    assert case.lease.verified.is_set()
    assert len(_readback(case).commits) == 1


def test_real_activation_close_rollback_stall_cannot_authorize_or_renew(native_owner):
    case = native_owner
    case.mode = "activation_rollback"
    after = _failed_cycle(case, "activation_rollback")
    assert len(case.ready) == 1 and case.ready[0]["status"] == "READY"
    assert len(case.completed) == 16  # All actual seal/signature/forward/grant reads passed.
    assert after.owner == case.before.owner and not after.commits


def test_committed_renewal_without_independent_identity_readback_is_not_authority(native_owner):
    case = native_owner
    case.mode = "postcommit_readback"
    after = _failed_cycle(case, "postcommit_readback")
    assert len(case.ready) == 1 and len(case.completed) == 16
    assert len(after.commits) == 1  # Independent direct connection proves actual COMMIT.
    assert after.owner["heartbeat_at"] == after.commits[0][1]
    assert after.owner["heartbeat_at"] > case.before.owner["heartbeat_at"]
    assert not case.lease.verified.is_set()  # A committed heartbeat alone is insufficient.
