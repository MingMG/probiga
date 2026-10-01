from pathlib import Path
from types import SimpleNamespace
import hashlib
import json

import pytest

from tools.secondary_edge import cold_database as c


SOURCE_UUID = "11111111-1111-1111-1111-111111111111"
TARGET_UUID = "22222222-2222-2222-2222-222222222222"


def stopped():
    return {"processes": [], "services": [{"Name": c.SERVICE, "State": "Stopped", "StartMode": "Disabled", "ProcessId": 0}]}


def source_fixture(tmp_path):
    roots = {name: tmp_path / "source" / name for name in c.TARGET_DIRS}
    for path in roots.values():
        path.mkdir(parents=True)
    content = {"auto.cnf": f"[auto]\nserver-uuid={SOURCE_UUID}\n".encode(), "mysql.ibd": b"all mysql users dictionary",
               "ibdata1": b"ibdata", "undo_001": b"undo1", "undo_002": b"undo2",
               "#innodb_redo/#ib_redo7": b"redo", "probiga/a.ibd": b"business records",
               "probiga_qmt_history/history.ibd": b"history", "biga/ai.ibd": b"ai records",
               "mysqld-auto.cnf": json.dumps({"Version": 2, "mysql_dynamic_variables": {"default_collation_for_utf8mb4":
                                        {"Value": "utf8mb4_general_ci", "Metadata": {"User": "root"}}}}).encode()}
    for name, value in content.items():
        path = roots["data"] / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)
    log = roots["logs"] / "mysql-bin.000009"
    log.write_bytes(b"complete binary log")
    (roots["logs"] / "mysql-bin.index").write_text(str(log).replace("\\", "/") + "\n")
    for name in ("ca.pem", "server-cert.pem", "server-key.pem"):
        (roots["certs"] / name).write_bytes(b"mock certificate not used by a real server")
    (roots["config"] / "admin-client.ini").write_text("password=DO_NOT_COPY")
    config = ["[mysqld]", "basedir=D:/MySQL84", "datadir=" + str(roots["data"]), "tmpdir=E:/MySQL84/Tmp",
              "innodb-log-group-home-dir=" + str(roots["data"]), "innodb-buffer-pool-size=8G",
              "log-bin=" + str(roots["logs"] / "mysql-bin"), "log-bin-index=" + str(roots["logs"] / "mysql-bin.index"),
              "ssl-ca=" + str(roots["certs"] / "ca.pem"), "ssl-cert=" + str(roots["certs"] / "server-cert.pem"),
              "ssl-key=" + str(roots["certs"] / "server-key.pem")]
    (roots["config"] / "my.ini").write_text("\n".join(config))
    layout = {"format": c.LAYOUT_FORMAT, "source": {"hostname": "SOURCE", "server_uuid": SOURCE_UUID,
              "version": "8.4.11", "service_name": c.SERVICE, "datadir": str(roots["data"]), "logsdir": str(roots["logs"])},
              "roots": {name: str(path) for name, path in roots.items()}}
    pause = {"format": c.PAUSE_FORMAT, "status": "paused", "source_host": "SOURCE", "source_server_uuid": SOURCE_UUID,
             "source_service_name": c.SERVICE, "source_service_state": "Stopped", "source_service_startup": "Disabled",
             "source_processes_running": False, "shutdown_complete": True, "source_automatically_resume": False,
             "completed_at_utc": "2026-10-01T01:00:00+00:00"}
    return layout, pause, roots


def package_fixture(tmp_path):
    layout, pause, roots = source_fixture(tmp_path)
    package = tmp_path / "package"
    metadata = c.snapshot_source(layout, pause, package / "database", runtime_probe=stopped)
    target = tmp_path / "target"
    target.mkdir()
    return package, target, metadata, roots


def restored_fixture(tmp_path):
    package, target, metadata, roots = package_fixture(tmp_path)
    receipt = c.restore_target(target, package, local_host="TARGET", acl_check=lambda _: None)
    return package, target, metadata, roots, receipt


def state_fixture(target, phase="memory"):
    (target / "mysql-data/auto.cnf").write_text(f"[auto]\nserver-uuid={TARGET_UUID}\n")
    return {"server_uuid": TARGET_UUID, "hostname": "TARGET", "version": "8.4.11", "port": 0 if phase == "memory" else 3306,
            "datadir": str(target / "mysql-data"), "lower_case_table_names": 1, "require_secure_transport": 1,
            "event_scheduler": "OFF", "buffer_pool_size": c.BUFFER_POOL, "redo_capacity": c.REDO_CAPACITY,
            "collation": "utf8mb4_general_ci", "trust_creators": 0, "unlocked_source_accounts": 0,
            "log_bin": 1, "skip_networking": 1 if phase == "memory" else 0,
            "shared_memory": 1 if phase == "memory" else 0, "shared_memory_base_name": c.MEMORY_NAME,
            "current_user": "root@localhost" if phase == "memory" else "root@127.0.0.1",
            "mysql_time_zone": "+08:00", "mysql_system_time_zone": "China Standard Time",
            "mysql_utc_datetime": "2026-10-01T04:00:00Z",
            "schema_names": ["mysql", "probiga", "probiga_qmt_history", "biga"],
            "tablespace_paths": ["./probiga/a.ibd", "./undo_001"], "file_paths": ["./ibdata1", "./undo_002"],
            "ssl_version": "TLSv1.3", "ssl_cipher": "TLS_AES_256_GCM_SHA384"}


def test_snapshot_copies_every_data_log_certificate_file_and_only_formal_config(tmp_path):
    package, _, metadata, roots = package_fixture(tmp_path)
    for row in metadata["files"]:
        source = roots[row["root"]] / row["path"]
        copied = package / "database" / row["root"] / row["path"]
        assert source.read_bytes() == copied.read_bytes()
        assert row["sha256"] == hashlib.sha256(copied.read_bytes()).hexdigest()
    assert not (package / "database/config/admin-client.ini").exists()
    assert metadata["source_automatically_resume"] is False
    assert metadata["pause"]["shutdown_complete"] is True
    assert c.load_snapshot(package)[0] == metadata


@pytest.mark.parametrize("changes", [{"status": "pending"}, {"source_service_state": "Running"},
    {"source_service_startup": "Automatic"}, {"source_automatically_resume": True}, {"shutdown_complete": False}])
def test_snapshot_never_accepts_incomplete_or_resuming_source_pause(tmp_path, changes):
    layout, pause, _ = source_fixture(tmp_path)
    pause.update(changes)
    with pytest.raises(c.ColdError, match="DURABLE_SOURCE_PAUSE"):
        c.snapshot_source(layout, pause, tmp_path / "snapshot", runtime_probe=stopped)
    assert not (tmp_path / "snapshot").exists()


def test_running_process_refused_before_any_source_copy(tmp_path):
    layout, pause, _ = source_fixture(tmp_path)
    def running():
        state = stopped()
        state["processes"] = [{"Name": "mysqld.exe", "ProcessId": 1}]
        return state
    with pytest.raises(c.ColdError, match="NOT_STOPPED"):
        c.snapshot_source(layout, pause, tmp_path / "snapshot", runtime_probe=running)
    assert not (tmp_path / "snapshot").exists()


def test_restart_during_copy_leaves_incomplete_receipt_not_ready(tmp_path):
    layout, pause, _ = source_fixture(tmp_path)
    count = 0
    def changed():
        nonlocal count
        count += 1
        result = stopped()
        if count >= 3:
            result["processes"] = [{"Name": "mysqld.exe", "ProcessId": 9}]
        return result
    destination = tmp_path / "snapshot"
    with pytest.raises(c.ColdError, match="NOT_STOPPED"):
        c.snapshot_source(layout, pause, destination, runtime_probe=changed)
    assert not (destination / "metadata.json").exists()
    assert json.loads((destination / "snapshot-state.json").read_text())["status"] == "COPYING"


def test_symlink_refused_not_followed_or_copied(tmp_path):
    layout, pause, roots = source_fixture(tmp_path)
    try:
        (roots["data"] / "external.ibd").symlink_to(roots["data"] / "mysql.ibd")
    except OSError:
        pytest.skip("Symlink privileges unavailable")
    with pytest.raises(c.ColdError, match="SYMLINK"):
        c.snapshot_source(layout, pause, tmp_path / "snapshot", runtime_probe=stopped)


def test_tampered_or_unlisted_package_file_refused(tmp_path):
    package, target, _, _ = package_fixture(tmp_path)
    (package / "database/logs/mysql-bin.000009").write_bytes(b"changed")
    with pytest.raises(c.ColdError, match="HASH_MISMATCH"):
        c.restore_target(target, package, local_host="TARGET", acl_check=lambda _: None)
    assert not (target / "database-status.json").exists()


def test_restore_preserves_source_snapshot_and_only_regenerates_owned_target_identity(tmp_path):
    package, target, metadata, roots, receipt = restored_fixture(tmp_path)
    assert receipt["status"] == "materialized"
    assert receipt["port"] == 3306 and receipt["service_name"] == "ProBigA-MySQL84"
    assert receipt["production_active"] is False and receipt["pending_production_activation"] is False
    assert not (target / "mysql-data/auto.cnf").exists()
    assert (target / "mysql-source-auto.cnf").read_bytes() == (roots["data"] / "auto.cnf").read_bytes()
    assert (package / "database/data/auto.cnf").read_bytes() == (roots["data"] / "auto.cnf").read_bytes()
    assert (target / "mysql-data/mysqld-auto.cnf").read_bytes() == (roots["data"] / "mysqld-auto.cnf").read_bytes()
    rewritten = (target / "mysql-logs/mysql-bin.index").read_text().strip()
    assert rewritten == str(target / "mysql-logs/mysql-bin.000009").replace("\\", "/")
    my_ini = (target / "my.ini").read_text()
    assert "innodb_buffer_pool_size=" + str(c.BUFFER_POOL) in my_ini
    assert str(roots["data"]) not in my_ini and "8G" not in my_ini
    assert "default_collation_for_utf8mb4=" not in my_ini
    assert "shared_memory=OFF" in my_ini
    assert (target / "mysql-tmp").is_dir()
    assert "E:/MySQL84/Tmp" not in my_ini
    init = (target / "bootstrap-init.sql").read_text()
    password = (target / "root-client.ini").read_text().split("password=")[1].splitlines()[0]
    assert len(password) == 64 and password in init
    assert "RESET PERSIST;" in init and "SET PERSIST_ONLY `default_collation_for_utf8mb4`='utf8mb4_general_ci'" in init
    assert "SET GLOBAL default_collation_for_utf8mb4='utf8mb4_general_ci'" in init
    assert "ACCOUNT LOCK" in init and c.NON_ADMIN_ACCOUNT_PREDICATE in init
    assert "ALTER USER 'root'@'127.0.0.1' IDENTIFIED BY '" + password + "' REQUIRE SSL ACCOUNT UNLOCK" in init
    assert "Host IN ('localhost','127.0.0.1')" in init
    assert "skip-grant-tables" not in init + my_ini
    assert password not in json.dumps(receipt)
    assert metadata["source"]["server_uuid"] == SOURCE_UUID


def test_source_host_and_existing_target_are_never_overwritten(tmp_path):
    package, target, _, _ = package_fixture(tmp_path)
    with pytest.raises(c.ColdError, match="SOURCE_COMPUTER"):
        c.restore_target(target, package, local_host="SOURCE", acl_check=lambda _: None)
    (target / "mysql-data").mkdir()
    with pytest.raises(c.ColdError, match="UNOWNED"):
        c.restore_target(target, package, local_host="TARGET", acl_check=lambda _: None)


def test_idempotent_restore_never_recopies_files_modified_by_bootstrap(tmp_path):
    package, target, _, _, _ = restored_fixture(tmp_path)
    modified = target / "mysql-data/mysql.ibd"
    modified.write_bytes(b"new root applied by orchestrator's mock bootstrap")
    state_fixture(target)
    result = c.restore_target(target, package, local_host="TARGET", acl_check=lambda _: None)
    assert result["idempotent"] is True
    assert modified.read_bytes().startswith(b"new root")
    assert (target / "mysql-data/auto.cnf").exists()


def test_partial_restore_is_fenced_and_does_not_automatically_retry(tmp_path):
    package, target, metadata, _ = package_fixture(tmp_path)
    index = package / "database/logs/mysql-bin.index"
    index.write_text("E:/not-snapshotted/mysql-bin.999999\n")
    row = next(row for row in metadata["files"] if row["path"] == "mysql-bin.index")
    row.update(bytes=index.stat().st_size, sha256=hashlib.sha256(index.read_bytes()).hexdigest())
    identity = dict(metadata)
    identity.pop("snapshot_id")
    metadata["snapshot_id"] = hashlib.sha256(c._json(identity).encode()).hexdigest()
    (package / "database/metadata.json").write_text(json.dumps(metadata))
    with pytest.raises(c.ColdError, match="EXTERNAL_MYSQL_PATH"):
        c.restore_target(target, package, local_host="TARGET", acl_check=lambda _: None)
    assert json.loads((target / "database-status.json").read_text())["status"] == "MATERIALIZING"
    with pytest.raises(c.ColdError, match="PARTIAL_OR_FOREIGN"):
        c.restore_target(target, package, local_host="TARGET", acl_check=lambda _: None)


def test_unrecognized_persisted_settings_block_instead_of_silent_loss(tmp_path):
    _, target, metadata, _, _ = restored_fixture(tmp_path)
    source = (target / "mysql-source-config/my.ini").read_text()
    with pytest.raises(c.ColdError, match="UNRECOGNIZED_PERSISTED"):
        c.target_configuration(metadata, source, target, {"unknown_future_option": "42"})


@pytest.mark.parametrize("changes", [{"server_uuid": SOURCE_UUID}, {"hostname": "SOURCE"}, {"port": 33085},
    {"version": "8.4.10"}, {"event_scheduler": "ON"}, {"require_secure_transport": 0},
    {"buffer_pool_size": 8 * 1024 ** 3}, {"collation": "utf8mb4_0900_ai_ci"}, {"unlocked_source_accounts": 1},
    {"tablespace_paths": ["D:/external/general.ibd"]}, {"file_paths": ["../outside.ibd"]}])
def test_identity_runtime_and_external_tablespaces_fail_closed(tmp_path, changes):
    _, target, metadata, _, receipt = restored_fixture(tmp_path)
    state = state_fixture(target)
    state.update(changes)
    with pytest.raises(c.ColdError):
        c.verify_identity(state, receipt, metadata, target, "memory")


def test_memory_transport_never_claims_tls_and_tcp_separately_verifies_it(tmp_path):
    package, target, _, _, _ = restored_fixture(tmp_path)
    state = state_fixture(target, phase="tcp")
    calls = []
    def runner(command, **kwargs):
        calls.append((command, kwargs))
        assert kwargs["timeout"] == 20 and kwargs["stderr"] == c.subprocess.DEVNULL
        assert kwargs["input"].startswith(b"SELECT JSON_OBJECT")
        assert not any("password" in part for part in command)
        assert command[1].startswith("--defaults-file=")
        observed = dict(state)
        if "--protocol=MEMORY" in command:
            observed.update(ssl_version="", ssl_cipher="", port=0, skip_networking=1, shared_memory=1, current_user="root@localhost")
        return SimpleNamespace(returncode=0, stdout=json.dumps(observed).encode())
    memory = c.verify_target(target, package, runner=runner, acl_check=lambda _: None)
    assert memory["status"] == "bootstrap-verified" and memory["tls_verified"] is False
    tcp = c.verify_target(target, package, phase="tcp", runner=runner, acl_check=lambda _: None)
    assert tcp["status"] == "paused-ready" and tcp["tls_verified"] is True
    assert "--host=127.0.0.1" in calls[-1][0] and "--port=3306" in calls[-1][0]
    assert "--ssl-mode=VERIFY_CA" in calls[-1][0]


def test_only_three_exact_local_system_accounts_are_exempt_from_source_lock():
    sql = c.bootstrap_sql("a" * 64, {})
    predicate = c.NON_ADMIN_ACCOUNT_PREDICATE
    assert "Host='localhost' AND User IN ('mysql.infoschema','mysql.session','mysql.sys')" in predicate
    assert "User='root' AND Host IN ('localhost','127.0.0.1')" in predicate
    assert "LIKE" not in predicate and "mysql.%" not in sql + c.IDENTITY_SQL
    assert predicate in sql and predicate in c.IDENTITY_SQL


def test_reported_timezone_is_evidence_not_a_clock_calibration_claim(tmp_path):
    package, target, _, _, _ = restored_fixture(tmp_path)
    state = state_fixture(target)
    # A differing operating-system zone/date is documented, not silently fixed
    # or used to prevent faithful cold-data preservation while still paused.
    state.update(mysql_system_time_zone="UTC", mysql_utc_datetime="2025-01-01T00:00:00Z")
    def runner(*_, **__):
        return SimpleNamespace(returncode=0, stdout=json.dumps(state).encode())
    receipt = c.verify_target(target, package, runner=runner, acl_check=lambda _: None)
    assert receipt["time_report"] == {"mysql_session_time_zone": "+08:00",
        "mysql_system_time_zone": "UTC", "mysql_reported_utc": "2025-01-01T00:00:00Z",
        "clock_calibration": "NOT_VERIFIED_REQUIRES_PRODUCTION_RESUME_GATE"}
    assert receipt["status"] == "bootstrap-verified" and receipt["production_active"] is False
    assert "@@time_zone" in c.IDENTITY_SQL and "@@system_time_zone" in c.IDENTITY_SQL


def test_cli_masks_native_output_and_configuration_secrets(monkeypatch, capsys):
    def fail(*_, **__):
        raise RuntimeError("password=PRIVATE_SECRET SQL business_content")
    monkeypatch.setattr(c, "restore_target", fail)
    assert c.main(["restore", "--root", "fake", "--package", "fake"]) == 2
    output = capsys.readouterr().out
    assert "PRIVATE_SECRET" not in output and "business_content" not in output
    assert json.loads(output)["reason"] == "COLD_DATABASE_OPERATION_FAILED"


@pytest.mark.parametrize("user_rule,accepted", [
    ({"rights": 131232, "inheritance": 0, "propagation": 0}, True),
    ({"rights": 131233, "inheritance": 0, "propagation": 0}, False),
    ({"rights": 131232, "inheritance": 3, "propagation": 0}, False),
    ({"rights": 2032127, "inheritance": 0, "propagation": 0}, False),
])
def test_acl_allows_original_user_only_noninherited_traverse_not_data(tmp_path, monkeypatch, user_rule, accepted):
    monkeypatch.setattr(c.os, "name", "nt")
    admin = [{"sid": sid, "rights": 2032127, "type": 0, "inherited": False, "inheritance": 3, "propagation": 0}
             for sid in ("S-1-5-18", "S-1-5-32-544")]
    user = {"sid": "S-1-5-21-1-2-3-1001", "type": 0, "inherited": False, **user_rule}
    def runner(command, **kwargs):
        assert kwargs["env"]["PROBIGA_COLD_ACL_PATH"] == str(tmp_path)
        return SimpleNamespace(returncode=0, stdout=json.dumps({"protected": True, "rules": [*admin, user]}).encode())
    if accepted:
        c.require_protected_root(tmp_path, runner=runner)
    else:
        with pytest.raises(c.ColdError, match="ACL_NOT_RESTRICTED"):
            c.require_protected_root(tmp_path, runner=runner)


def test_verify_does_not_rehash_full_package_during_readiness_retries(tmp_path, monkeypatch):
    package, target, _, _, _ = restored_fixture(tmp_path)
    state = state_fixture(target)
    def runner(*_, **__):
        return SimpleNamespace(returncode=0, stdout=json.dumps(state).encode())
    c.verify_target(target, package, runner=runner, acl_check=lambda _: None)
    state.update(port=3306, skip_networking=0, shared_memory=0, current_user="root@127.0.0.1")
    def forbidden(*_):
        pytest.fail("Complete cold-file SHA checks belong before any startup, not readiness retries")
    monkeypatch.setattr(c, "_digest", forbidden)
    assert c.verify_target(target, package, phase="tcp", runner=runner, acl_check=lambda _: None)["status"] == "paused-ready"


def test_source_object_inventory_when_supplied_is_exact_not_row_estimate(tmp_path):
    _, target, metadata, _, receipt = restored_fixture(tmp_path)
    state = state_fixture(target)
    metadata["source"]["inventory"] = {"schemas": state["schema_names"], "tables": [
        {"schema": "probiga", "name": "a", "type": "BASE TABLE", "estimated_rows": 100}]}
    state["table_inventory"] = [{"schema": "probiga", "name": "a", "type": "BASE TABLE", "estimated_rows": 999}]
    c.verify_identity(state, receipt, metadata, target, "memory")
    state["table_inventory"] = []
    with pytest.raises(c.ColdError, match="TABLE_INVENTORY_MISMATCH"):
        c.verify_identity(state, receipt, metadata, target, "memory")


def test_native_verification_failure_never_writes_paused_ready(tmp_path):
    package, target, _, _, _ = restored_fixture(tmp_path)
    def failed(*_, **__):
        return SimpleNamespace(returncode=1, stdout=b"PASSWORD_PRIVATE_SECRET")
    with pytest.raises(c.ColdError, match="READ_ONLY_VERIFICATION_QUERY_FAILED") as exc:
        c.verify_target(target, package, runner=failed, acl_check=lambda _: None)
    assert "SECRET" not in str(exc.value)
    assert json.loads((target / "database-status.json").read_text())["status"] == "materialized"


def test_oracle_8411_real_v2_persisted_format_is_read_without_modifying_source(tmp_path):
    path = tmp_path / "mysqld-auto.cnf"
    original = '{"Version":2,"mysql_dynamic_variables":{"default_collation_for_utf8mb4":{"Value":"utf8mb4_general_ci","Metadata":{"Host":"","User":"probiga_admin","Timestamp":1786108111898337}}}}'
    path.write_text(original)
    assert c._persisted_values(path) == {"default_collation_for_utf8mb4": "utf8mb4_general_ci"}
    assert path.read_text() == original


def test_sensitive_or_unknown_oracle_persisted_categories_fail_closed(tmp_path):
    path = tmp_path / "mysqld-auto.cnf"
    path.write_text(json.dumps({"Version": 2, "mysql_sensitive_variables": {"master_key_id": "secret"}}))
    with pytest.raises(c.ColdError, match="FORMAT_UNSUPPORTED"):
        c._persisted_values(path)


def test_business_only_inventory_matches_same_scope_without_losing_strictness(tmp_path):
    _, target, metadata, _, receipt = restored_fixture(tmp_path)
    state = state_fixture(target)
    metadata["source"]["inventory"] = {"schemas": ["probiga", "probiga_qmt_history", "biga"], "tables": [
        {"schema": "probiga", "name": "a", "type": "BASE TABLE"}]}
    state["table_inventory"] = [{"schema": "probiga", "name": "a", "type": "BASE TABLE"},
                                {"schema": "mysql", "name": "user", "type": "BASE TABLE"}]
    c.verify_identity(state, receipt, metadata, target, "memory")
    state["table_inventory"].append({"schema": "biga", "name": "unexpected", "type": "BASE TABLE"})
    with pytest.raises(c.ColdError, match="TABLE_INVENTORY_MISMATCH"):
        c.verify_identity(state, receipt, metadata, target, "memory")


def test_memory_bootstrap_requires_native_zero_port_and_no_network_listener(tmp_path):
    _, target, metadata, _, receipt = restored_fixture(tmp_path)
    state = state_fixture(target)
    c.verify_identity(state, receipt, metadata, target, "memory")
    state.update(port=3306, skip_networking=0)
    with pytest.raises(c.ColdError, match="IDENTITY_REFUSED"):
        c.verify_identity(state, receipt, metadata, target, "memory")


def test_actual_source_mysql_options_have_closed_paths_and_preserved_business_encoding(tmp_path):
    _, target, metadata, _, _ = restored_fixture(tmp_path)
    metadata["source_roots"] = {"data": "E:/MySQL84/Data", "logs": "E:/MySQL84/Logs",
                                "config": "D:/MySQL84/config", "certs": "D:/MySQL84/certs"}
    source = """[mysqld]
basedir=D:/MySQL84/software/mysql-8.4.11-winx64
datadir=E:/MySQL84/Data
port=3306
bind-address=127.0.0.1
mysqlx=OFF
shared-memory=OFF
named-pipe=OFF
skip-name-resolve=ON
lower_case_table_names=1
character-set-server=utf8mb4
collation-server=utf8mb4_general_ci
default-time-zone=+08:00
transaction-isolation=REPEATABLE-READ
explicit-defaults-for-timestamp=ON
sql-mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ZERO_DATE,NO_ZERO_IN_DATE,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY
require-secure-transport=ON
tls-version=TLSv1.2,TLSv1.3
ssl-ca=D:/MySQL84/certs/ca.pem
ssl-cert=D:/MySQL84/certs/server-cert.pem
ssl-key=D:/MySQL84/certs/server-key.pem
innodb-file-per-table=ON
innodb-buffer-pool-size=8G
innodb-redo-log-capacity=2G
innodb-flush-log-at-trx-commit=1
log-bin=E:/MySQL84/Logs/mysql-bin
log-bin-index=E:/MySQL84/Logs/mysql-bin.index
binlog-format=ROW
binlog-row-image=FULL
sync-binlog=1
max-binlog-size=256M
binlog-expire-logs-seconds=259200
server-id=84011
max-allowed-packet=256M
max-connections=100
event-scheduler=OFF
local-infile=OFF
secure-file-priv=NULL
performance-schema=ON
tmpdir=E:/MySQL84/Tmp
log-error=E:/MySQL84/Logs/mysql84.err
log-error-verbosity=2
pid-file=E:/MySQL84/Logs/mysql84.pid
slow-query-log=ON
slow-query-log-file=E:/MySQL84/Logs/mysql84-slow.log
long-query-time=2
general-log=OFF
[client]
protocol=tcp
host=127.0.0.1
port=3306
default-character-set=utf8mb4
ssl-mode=VERIFY_CA
ssl-ca=D:/MySQL84/certs/ca.pem
"""
    configuration, _, options = c.target_configuration(metadata, source, target, {"default_collation_for_utf8mb4": "utf8mb4_general_ci"})
    assert "D:/MySQL84" not in configuration and "E:/MySQL84" not in configuration
    assert options["max_allowed_packet"] == "256M" and options["collation_server"] == "utf8mb4_general_ci"
    assert options["default_time_zone"] == "+08:00" and options["binlog_format"] == "ROW"
    assert "default_collation_for_utf8mb4=" not in configuration and options["shared_memory"] == "OFF"


def test_small_file_snapshot_does_not_spawn_cim_for_every_file(tmp_path):
    layout, pause, roots = source_fixture(tmp_path)
    for number in range(100):
        (roots["data"] / ("small-%d.data" % number)).write_bytes(b"still included in full SHA inventory")
    probes = []
    def counted():
        probes.append(True)
        return stopped()
    metadata = c.snapshot_source(layout, pause, tmp_path / "snapshot", runtime_probe=counted, clock=lambda: 0)
    assert len(probes) == 4  # initial, after initial inventory, before/after final inventory
    assert len(metadata["files"]) > 100


def test_periodic_guards_and_progress_are_throttled_and_never_expose_paths_or_secrets():
    current = 0
    probes, events = [], []
    def clock():
        return current
    def probe():
        probes.append(current)
        return stopped()
    pulse = c._SnapshotPulse(probe, 1000, 1000000, events.append, clock)
    pulse.copied_files, pulse.copied_bytes = 5, 50000
    for current in range(1, 92):
        pulse()
    assert probes == list(range(5, 91, 5))
    assert [event["elapsed_seconds"] for event in events] == [30, 60, 90]
    assert all(event["copied_files"] == 5 and event["copied_bytes"] == 50000 for event in events)
    assert all(event["production_active"] is False for event in events)
    assert not any("path" in key or "password" in key for event in events for key in event)


def test_large_file_hash_verification_keeps_stop_guard_active(tmp_path):
    path = tmp_path / "large.data"
    path.write_bytes(b"a" * (8 * 1024 ** 2 + 1))
    ticks = iter(range(0, 100, 5))
    probes = []
    def probe():
        probes.append(True)
        return stopped()
    pulse = c._SnapshotPulse(probe, 1, path.stat().st_size, None, lambda: next(ticks))
    assert c._digest(path, pulse=pulse) == hashlib.sha256(path.read_bytes()).hexdigest()
    assert len(probes) == 3  # every digest chunk retains the bounded runtime gate
