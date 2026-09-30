from pathlib import Path
from types import SimpleNamespace
import hashlib
import json

import pytest

from tools.secondary_edge import target_database as t


BUILD = "a" * 40
SOURCE_UUID = "11111111-1111-1111-1111-111111111111"
TARGET_UUID = "22222222-2222-2222-2222-222222222222"


def make_package(tmp_path):
    package = tmp_path / "package"
    (package / "database").mkdir(parents=True)
    sql = b"-- mock consistent dump, not executed by tests\n"
    (package / "database/app.sql").write_bytes(sql)
    metadata = {"format": t.METADATA_FORMAT, "status": "ready",
                "source": {"server_uuid": SOURCE_UUID, "hostname": "PRIMARY", "version": "8.4.11", "port": 3306},
                "dump": {"file": "app.sql", "bytes": len(sql), "sha256": hashlib.sha256(sql).hexdigest()},
                "schemas": [{"name": name} for name in ("probiga", "probiga_qmt_history", "biga")],
                "tables": [{"schema": "probiga", "name": "sm_stock_current", "type": "BASE TABLE"},
                           {"schema": "probiga", "name": "read_only_view", "type": "VIEW"}],
                "views": [{"schema": "probiga", "name": "read_only_view", "definer": "view_owner@localhost"}],
                "triggers": [{"schema": "probiga", "name": "protect_rows", "definer": "trigger_owner@127.0.0.1"}],
                "routines": [{"schema": "biga", "name": "read_value", "type": "FUNCTION", "definer": "view_owner@localhost"}],
                "events": [{"schema": "biga", "name": "old_job", "definer": "event_owner@localhost"}]}
    manifest = {"format": t.PACKAGE_FORMAT, "build_sha": BUILD, "source_host": "PRIMARY",
                "production_activation": False, "ai_server_url": "https://example.invalid/api/ai"}
    (package / "database/metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
    (package / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return package, manifest, metadata


class Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []
    def __enter__(self):
        return self
    def __exit__(self, *_):
        return False
    def execute(self, sql, params=None):
        db = self.connection
        db.calls.append((sql, params))
        if db.disconnected:
            raise RuntimeError("connection lost PRIVATE_SECRET")
        if sql == "SET GLOBAL log_bin_trust_function_creators=OFF" and db.fail_off:
            raise RuntimeError("reset failed PRIVATE_SECRET")
        self.rows = []
        if sql.startswith("SELECT @@server_uuid"):
            self.rows = [dict(db.state)]
        elif sql.startswith("SHOW SESSION STATUS"):
            self.rows = [{"Variable_name": "Ssl_cipher", "Value": db.cipher}]
        elif sql.startswith("SELECT @@port AS port"):
            self.rows = [{"port": db.state["port"], "database_name": db.database}]
        elif sql.startswith("SELECT @@global.log_bin AS log_bin"):
            self.rows = [{"log_bin": db.state["log_bin"], "trust_creators": db.state["trust_creators"]}]
        elif "information_schema.SCHEMATA" in sql:
            self.rows = [{"name": name} for name in t.SYSTEM | db.schemas]
        elif sql.startswith("SHOW GRANTS FOR "):
            user = params[0]
            self.rows = [{"grant": f"GRANT USAGE ON *.* TO '{user}'@'127.0.0.1'"}]
            self.rows.extend({"grant": statement.split(" TO ")[0] + f" TO '{user}'@'127.0.0.1'"}
                             for statement, arguments in db.calls
                             if statement.startswith("GRANT ") and arguments == (user,))
        elif sql.startswith("SHOW CREATE USER "):
            self.rows = [{"create_user": "CREATE USER `private`@`127.0.0.1` REQUIRE SSL ACCOUNT UNLOCK"}]
        else:
            for kind, table in (("tables", "TABLES"), ("views", "VIEWS"), ("triggers", "TRIGGERS"),
                                ("routines", "ROUTINES"), ("events", "EVENTS")):
                if "FROM information_schema." + table + " " in sql:
                    self.rows = db.metadata[kind] if db.restored and kind != db.missing_kind else []
                    return
        if sql.startswith("SET PERSIST"):
            db.state["default_collation_for_utf8mb4"] = "utf8mb4_general_ci"
        elif sql == "SET GLOBAL log_bin_trust_function_creators=ON":
            db.state["trust_creators"] = 1
            if db.fail_on_response:
                raise RuntimeError("response lost PRIVATE_SECRET")
        elif sql == "SET GLOBAL log_bin_trust_function_creators=OFF":
            if not db.ignore_off:
                db.state["trust_creators"] = 0
        elif sql.startswith("USE "):
            db.database = "probiga_secondary"
        elif sql.startswith("CREATE DATABASE "):
            db.schemas.add("probiga_secondary")
    def fetchall(self):
        return self.rows
    def fetchone(self):
        return self.rows[0] if self.rows else None


class Connection:
    def __init__(self, metadata):
        self.metadata = metadata
        self.calls, self.schemas = [], set()
        self.database = None
        self.restored, self.missing_kind = False, None
        self.closed, self.commits = 0, 0
        self.cipher = "TLS_AES_256_GCM_SHA384"
        self.disconnected, self.fail_off, self.ignore_off, self.fail_on_response = False, False, False, False
        self.state = {"server_uuid": TARGET_UUID, "hostname": "CANDIDATE", "version": "8.4.11",
                      "version_comment": "MySQL Community Server - GPL", "port": 33085,
                      "lower_case_table_names": 1, "require_secure_transport": 1,
                      "event_scheduler": "OFF", "default_collation_for_utf8mb4": "utf8mb4_0900_ai_ci",
                      "log_bin": 1, "trust_creators": 0}
    def cursor(self):
        return Cursor(self)
    def close(self):
        self.closed += 1
    def commit(self):
        self.commits += 1


def fixtures(tmp_path):
    package, manifest, metadata = make_package(tmp_path)
    root = tmp_path / "target"
    (root / "mysql-data").mkdir(parents=True)
    connection = Connection(metadata)
    connections, imports = [], []
    def connector(ca, password):
        connections.append((ca, password))
        return connection
    def runner(command, **kwargs):
        imports.append(command)
        assert "--max-allowed-packet=256M" in command
        assert connection.state["trust_creators"] == 1
        assert json.loads((root / "database-status.json").read_text())["status"] == "IMPORTING"
        assert kwargs["stdin"].read().startswith(b"-- mock consistent")
        assert kwargs["stdout"] == kwargs["stderr"] == t.subprocess.DEVNULL
        assert kwargs["shell"] is False
        assert not any("password=" in part or "PRIVATE_SECRET" in part for part in command)
        connection.restored = True
        connection.schemas.update(row["name"] for row in metadata["schemas"])
        return SimpleNamespace(returncode=0)
    return root, package, manifest, metadata, connection, connector, runner, connections, imports


@pytest.mark.parametrize("changes", [
    {"port": 3306}, {"version": "8.4.10"}, {"version": "8.4.12"},
    {"lower_case_table_names": 0}, {"require_secure_transport": 0}, {"event_scheduler": "ON"},
    {"server_uuid": SOURCE_UUID}, {"hostname": "PRIMARY"},
])
def test_target_identity_contract_refuses_source_or_incompatible_instance(tmp_path, changes):
    _, _, metadata = make_package(tmp_path)
    connection = Connection(metadata)
    connection.state.update(changes)
    with pytest.raises(t.TargetError):
        t.target_state(connection, metadata["source"], "CANDIDATE")
    assert not any(sql.startswith(("CREATE", "ALTER", "GRANT")) for sql, _ in connection.calls)


def test_tls_and_source_windows_hostname_checked(tmp_path):
    _, _, metadata = make_package(tmp_path)
    connection = Connection(metadata)
    connection.cipher = ""
    with pytest.raises(t.TargetError, match="TLS_REQUIRED"):
        t.target_state(connection, metadata["source"], "CANDIDATE")
    connection.cipher = "TLS"
    with pytest.raises(t.TargetError, match="SOURCE_COMPUTER"):
        t.target_state(connection, metadata["source"], "PRIMARY")


def test_hash_failure_before_any_connect(tmp_path):
    root, package, _, _, _, _, _, _, _ = fixtures(tmp_path)
    (package / "database/app.sql").write_bytes(b"changed")
    def forbidden(*_):
        pytest.fail("Should not connect")
    with pytest.raises(t.TargetError, match="HASH_MISMATCH"):
        t.restore_target(root, package, connector=forbidden, local_host="CANDIDATE")


def test_successful_restore_no_source_accounts_and_restricted_runtime(tmp_path):
    root, package, _, _, connection, connector, runner, connections, imports = fixtures(tmp_path)
    receipt = t.restore_target(root, package, connector=connector, runner=runner,
                               local_host="CANDIDATE", chrome_finder=lambda: root / "chrome.exe")
    assert receipt["status"] == "ready"
    assert receipt["production_active"] is False
    assert receipt["source_accounts_imported"] is False
    assert receipt["log_bin_trust_function_creators"] == "OFF"
    assert connection.state["trust_creators"] == 0
    maintenance = [sql for sql, _ in connection.calls if "log_bin_trust_function_creators=" in sql]
    assert maintenance == ["SET GLOBAL log_bin_trust_function_creators=ON",
                           "SET GLOBAL log_bin_trust_function_creators=OFF"]
    assert len(imports) == 1
    assert connections[0][1] == ""
    new_password = t.read_root_password(root / "root-client.ini")
    assert len(new_password) == 64
    assert any(sql.startswith("ALTER USER 'root'") and params == (new_password,)
               for sql, params in connection.calls)
    config = json.loads((root / "config.json").read_text())
    assert config["mysql"]["user"] == t.WRITER
    assert config["source_db"]["user"] == t.READER
    assert config["mysql"]["password"] != config["source_db"]["password"] != new_password
    writer_grants = [(sql, params) for sql, params in connection.calls if sql.startswith("GRANT") and params == (t.WRITER,)]
    assert len(writer_grants) == 4
    assert all("SELECT, INSERT, UPDATE ON `probiga_secondary`." in sql for sql, _ in writer_grants)
    assert not any("ALL PRIVILEGES" in sql or "GRANT OPTION" in sql for sql, _ in connection.calls)
    serialized = (root / "database-status.json").read_text()
    assert new_password not in serialized
    assert config["mysql"]["password"] not in serialized
    assert receipt["objects"] == {"schemas": 3, "tables": 2, "views": 1, "triggers": 1, "routines": 1, "events": 1}


def test_rerun_validates_and_never_imports_again(tmp_path):
    root, package, _, _, connection, connector, runner, connections, imports = fixtures(tmp_path)
    kwargs = dict(connector=connector, runner=runner, local_host="CANDIDATE",
                  chrome_finder=lambda: root / "chrome.exe")
    t.restore_target(root, package, **kwargs)
    count = len(connection.calls)
    repeated = t.restore_target(root, package, **kwargs)
    assert repeated["idempotent"] is True
    assert len(imports) == 1
    assert connections[-1][1] == t.read_root_password(root / "root-client.ini")
    assert not any(sql.startswith(("CREATE", "ALTER", "GRANT", "SET")) for sql, _ in connection.calls[count:])


def test_existing_business_database_not_overwritten(tmp_path):
    root, package, _, _, connection, connector, runner, _, imports = fixtures(tmp_path)
    connection.schemas.add("probiga")
    with pytest.raises(t.TargetError, match="EXISTING_DATABASE"):
        t.restore_target(root, package, connector=connector, runner=runner, local_host="CANDIDATE")
    assert not imports
    assert not (root / "database-status.json").exists()


def test_unowned_private_schema_is_not_reused_or_overwritten(tmp_path):
    root, package, _, _, connection, connector, runner, _, imports = fixtures(tmp_path)
    connection.schemas.add("probiga_secondary")
    with pytest.raises(t.TargetError, match="EXISTING_DATABASE"):
        t.restore_target(root, package, connector=connector, runner=runner, local_host="CANDIDATE")
    assert not imports


def test_rerun_refuses_expanded_runtime_privileges(tmp_path):
    root, package, _, _, connection, connector, runner, _, imports = fixtures(tmp_path)
    kwargs = dict(connector=connector, runner=runner, local_host="CANDIDATE",
                  chrome_finder=lambda: root / "chrome.exe")
    t.restore_target(root, package, **kwargs)
    connection.calls.append(("GRANT ALL PRIVILEGES ON *.* TO %s@'127.0.0.1'", (t.WRITER,)))
    with pytest.raises(t.TargetError, match="PRIVILEGES|DUPLICATE_GRANT"):
        t.restore_target(root, package, **kwargs)
    assert len(imports) == 1


def test_failed_import_fences_every_retry(tmp_path):
    root, package, _, _, connection, connector, _, connections, _ = fixtures(tmp_path)
    def failure(*_, **__):
        return SimpleNamespace(returncode=1)
    with pytest.raises(t.TargetError, match="PARTIAL_IMPORT"):
        t.restore_target(root, package, connector=connector, runner=failure, local_host="CANDIDATE")
    assert connection.state["trust_creators"] == 0
    assert json.loads((root / "database-status.json").read_text())["status"] == "IMPORTING"
    before = len(connections)
    with pytest.raises(t.TargetError, match="PARTIAL_RESTORE"):
        t.restore_target(root, package, connector=connector, runner=failure, local_host="CANDIDATE")
    assert len(connections) == before


@pytest.mark.parametrize("kind", ["tables", "views", "triggers", "routines", "events"])
def test_every_stored_object_kind_is_checked_and_missing_objects_block(tmp_path, kind):
    root, package, _, _, connection, connector, runner, _, _ = fixtures(tmp_path)
    connection.missing_kind = kind
    with pytest.raises(t.TargetError, match="INVENTORY_MISMATCH"):
        t.restore_target(root, package, connector=connector, runner=runner, local_host="CANDIDATE")
    assert json.loads((root / "database-status.json").read_text())["status"] == "IMPORTING"
    assert not (root / "config.json").exists()


def test_definers_are_exact_locked_and_never_copy_a_password_hash(tmp_path):
    _, _, metadata = make_package(tmp_path)
    connection = Connection(metadata)
    result = t.prepare_definers(connection, metadata)
    assert len(result) == 3
    assert all(row["state"] == "LOCKED_READ_ONLY_DEFINER" for row in result)
    creates = [(sql, params) for sql, params in connection.calls if sql.startswith("CREATE USER")]
    assert all("ACCOUNT LOCK" in sql and "IDENTIFIED BY" in sql for sql, _ in creates)
    assert all(len(params[2]) == 64 for _, params in creates)
    assert all("SELECT, EXECUTE" in sql for sql, _ in connection.calls if sql.startswith("GRANT"))


def test_unrepresentable_definer_blocks_instead_of_dropping_object(tmp_path):
    _, _, metadata = make_package(tmp_path)
    metadata["views"][0]["definer"] = "mysql.sys@localhost"
    connection = Connection(metadata)
    with pytest.raises(t.TargetError, match="NOT_REPRESENTABLE"):
        t.prepare_definers(connection, metadata)
    assert not any(sql.startswith("CREATE") for sql, _ in connection.calls)


def test_cli_hides_driver_exceptions_and_passwords(monkeypatch, capsys):
    def failed(*_):
        raise RuntimeError("password=PRIVATE_SECRET; SELECT business_private_data")
    monkeypatch.setattr(t, "restore_target", failed)
    assert t.main(["--root", "target", "--package", "package"]) == 2
    output = capsys.readouterr().out
    assert "PRIVATE_SECRET" not in output
    assert "SELECT business" not in output
    assert json.loads(output)["reason"] == "TARGET_BOOTSTRAP_FAILED"


def maintenance_fixture(tmp_path):
    root, _, _, metadata, connection, *_ = fixtures(tmp_path)
    status_file = root / "database-status.json"
    status_file.write_text(json.dumps({"format": t.STATUS_FORMAT, "status": "IMPORTING",
                                      "target_uuid": TARGET_UUID, "production_active": False}))
    checked_state = dict(connection.state)
    return connection, checked_state, metadata["source"], status_file


@pytest.mark.parametrize("changes", [
    {"server_uuid": SOURCE_UUID}, {"server_uuid": "33333333-3333-3333-3333-333333333333"},
    {"hostname": "PRIMARY"}, {"hostname": "OTHER_CANDIDATE"}, {"port": 3306},
])
def test_restore_window_regates_exact_candidate_before_on(tmp_path, changes):
    connection, checked_state, source, status_file = maintenance_fixture(tmp_path)
    connection.state.update(changes)
    with pytest.raises(t.TargetError):
        with t.stored_function_restore_window(connection, checked_state, source, "CANDIDATE", status_file):
            pytest.fail("Wrong instance must never import")
    assert not any(sql.startswith("SET GLOBAL") for sql, _ in connection.calls)


@pytest.mark.parametrize("fence", [None, {"status": "ready"},
    {"format": t.STATUS_FORMAT, "status": "IMPORTING", "production_active": False, "target_uuid": SOURCE_UUID}])
def test_restore_window_requires_durable_candidate_importing_fence(tmp_path, fence):
    connection, checked_state, source, status_file = maintenance_fixture(tmp_path)
    if fence is None:
        status_file.unlink()
    else:
        status_file.write_text(json.dumps(fence))
    with pytest.raises(t.TargetError, match="FENCE"):
        with t.stored_function_restore_window(connection, checked_state, source, "CANDIDATE", status_file):
            pytest.fail("Unfenced restore must never import")
    assert not any(sql.startswith("SET GLOBAL") for sql, _ in connection.calls)


@pytest.mark.parametrize("changes", [{"log_bin": 0}, {"trust_creators": 1}])
def test_restore_window_refuses_unowned_existing_maintenance_state(tmp_path, changes):
    connection, checked_state, source, status_file = maintenance_fixture(tmp_path)
    connection.state.update(changes)
    with pytest.raises(t.TargetError, match="CONTRACT|NOT_CLOSED"):
        with t.stored_function_restore_window(connection, checked_state, source, "CANDIDATE", status_file):
            pytest.fail("Unexpected initial policy must not be normalized silently")
    assert not any(sql.startswith("SET GLOBAL") for sql, _ in connection.calls)


def test_restore_window_cleans_up_after_on_response_is_lost(tmp_path):
    connection, checked_state, source, status_file = maintenance_fixture(tmp_path)
    connection.fail_on_response = True
    with pytest.raises(t.TargetError, match="MAINTENANCE_FAILED") as raised:
        with t.stored_function_restore_window(connection, checked_state, source, "CANDIDATE", status_file):
            pytest.fail("Import must not proceed without confirmed ON")
    assert "PRIVATE_SECRET" not in str(raised.value)
    assert connection.state["trust_creators"] == 0


@pytest.mark.parametrize("failure", ["reset_error", "reset_not_applied", "connection_lost", "instance_changed"])
def test_failed_restore_maintenance_cleanup_never_marks_ready(tmp_path, failure):
    root, package, _, _, connection, connector, runner, _, imports = fixtures(tmp_path)
    def failed_cleanup(command, **kwargs):
        result = runner(command, **kwargs)
        if failure == "reset_error":
            connection.fail_off = True
        elif failure == "reset_not_applied":
            connection.ignore_off = True
        elif failure == "connection_lost":
            connection.disconnected = True
        else:
            connection.state["server_uuid"] = SOURCE_UUID
        return result
    with pytest.raises(t.TargetError, match="CLEANUP_FAILED") as raised:
        t.restore_target(root, package, connector=connector, runner=failed_cleanup, local_host="CANDIDATE")
    assert "PRIVATE_SECRET" not in str(raised.value)
    assert len(imports) == 1
    assert json.loads((root / "database-status.json").read_text())["status"] == "IMPORTING"
    assert not (root / "config.json").exists()
    if failure == "instance_changed":
        assert not any(sql.endswith("creators=OFF") for sql, _ in connection.calls)


def test_rerun_refuses_reopened_stored_function_policy(tmp_path):
    root, package, _, _, connection, connector, runner, _, imports = fixtures(tmp_path)
    kwargs = dict(connector=connector, runner=runner, local_host="CANDIDATE",
                  chrome_finder=lambda: root / "chrome.exe")
    t.restore_target(root, package, **kwargs)
    connection.state["trust_creators"] = 1
    with pytest.raises(t.TargetError, match="MAINTENANCE_NOT_CLOSED"):
        t.restore_target(root, package, **kwargs)
    assert len(imports) == 1
