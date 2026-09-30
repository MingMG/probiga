import hashlib
import json
from pathlib import Path
import subprocess

import pytest

from tools.secondary_edge import database_export as d


GRANT = "GRANT SELECT, SHOW VIEW, TRIGGER, EVENT, BACKUP_ADMIN ON *.* TO 'backup'@'127.0.0.1'"


class Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.result = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, args=None):
        self.connection.queries.append((query, args))
        if self.connection.fail_query and self.connection.fail_query in query:
            raise RuntimeError("sensitive-password must never appear")
        if query.startswith("SELECT @@"):
            self.result = [self.connection.source]
        elif "Ssl_cipher" in query:
            self.result = [{"Variable_name": "Ssl_cipher", "Value": self.connection.cipher}]
        elif query == "SHOW GRANTS":
            self.result = [{"grant": item} for item in self.connection.grants]
        elif "information_schema." in query:
            table = query.split("information_schema.", 1)[1].split()[0]
            self.result = self.connection.rows.get(table, [])
        else:
            self.result = []

    def fetchall(self):
        return self.result


class Connection:
    def __init__(self):
        self.source = {"server_uuid": "source-uuid", "hostname": "source-host", "version": "8.4.11",
                       "port": 3306, "version_comment": "MySQL Community Server - GPL"}
        self.cipher = "TLS_AES_256_GCM_SHA384"
        self.grants = [GRANT]
        self.queries = []
        self.closed = False
        self.fail_query = None
        self.pings = []
        self.fail_ping = False
        self.rows = {
            "SCHEMATA": [{"name": "probiga", "charset": "utf8mb4", "collation": "utf8mb4_general_ci"},
                         {"name": "unknown_business", "charset": "utf8mb4", "collation": "utf8mb4_general_ci"}],
            "TABLES": [{"schema": "probiga", "name": "data", "type": "BASE TABLE", "engine": "InnoDB",
                        "estimated_bytes": 1000, "estimated_rows": 10},
                       {"schema": "unknown_business", "name": "records", "type": "BASE TABLE",
                        "engine": "InnoDB", "estimated_bytes": 2000, "estimated_rows": 20},
                       {"schema": "probiga", "name": "view_data", "type": "VIEW", "engine": None,
                        "estimated_bytes": 0, "estimated_rows": None}],
            "VIEWS": [{"schema": "probiga", "name": "view_data", "definer": "view_owner@localhost",
                       "definition": "select 1", "check_option": "NONE", "security_type": "DEFINER"}],
            "TRIGGERS": [{"schema": "probiga", "name": "trigger_data", "definer": "trigger_owner@localhost",
                          "definition": "set NEW.v=1", "timing": "BEFORE", "event": "INSERT"}],
            "ROUTINES": [{"schema": "probiga", "name": "stored_data", "type": "PROCEDURE",
                          "definer": "routine_owner@localhost", "definition": "select 1"}],
            "EVENTS": [{"schema": "probiga", "name": "event_data", "definer": "event_owner@localhost",
                        "definition": "select 1", "status": "DISABLED"}],
        }

    def cursor(self):
        return Cursor(self)

    def ping(self, *, reconnect):
        self.pings.append(reconnect)
        if self.fail_ping:
            raise RuntimeError("sensitive-password disconnected")

    def close(self):
        self.closed = True


class Process:
    def __init__(self, command=None, *, code=0, running=False):
        self.returncode = None if running else code
        self.terminated = False
        self.killed = False
        if command:
            output = next(item.split("=", 1)[1] for item in command if item.startswith("--result-file="))
            Path(output).write_bytes(b"-- snapshot\nCREATE DATABASE probiga;\n")

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminated = True
        self.returncode = -15

    def kill(self):
        self.killed = True
        self.returncode = -9

    def wait(self, timeout):
        return self.returncode


def inputs(tmp_path, body=None):
    option = tmp_path / "external-client.ini"
    option.write_text(body or "[client]\nuser=backup\npassword=sensitive-password\nhost=127.0.0.1\nport=3306\n",
                      encoding="utf-8")
    executable = tmp_path / "mysqldump.exe"
    executable.touch()
    ca = tmp_path / "ca.pem"
    ca.touch()
    return option, executable, ca, tmp_path / "output"


def run_export(tmp_path, connection=None, *, factory=None, monitor=d.monitor_dump):
    connection = connection or Connection()
    return d.export_database(*inputs(tmp_path), connector=lambda *_: connection,
                             process_factory=factory or (lambda command, **kwargs: Process(command)),
                             monitor=monitor)


def test_option_password_quotes_escapes_and_percent_are_not_interpolated(tmp_path):
    option, *_ = inputs(tmp_path, "[client]\nuser=backup\npassword=\"p%ss\\\\word\\\"#x\"\n")
    assert d.read_client_options(option)["password"] == 'p%ss\\word"#x'


@pytest.mark.parametrize("body", [
    "[client]\nuser=backup\npassword=x\n[mysqldump]\nwhere=id>10\n",
    "[client]\nuser=backup\npassword=x\nhost=remote.example\n",
    "[client]\nuser=backup\npassword=x\nport=13306\n",
    "[client]\nuser=backup\npassword=x\nssl-mode=DISABLED\n",
    "[client]\nuser=backup\npassword=x\nprotocol=PIPE\n",
    "[client]\nuser=backup\npassword=x\ninit-command=DELETE FROM probiga.data\n",
    "[client]\nuser=backup\n",
    "[DEFAULT]\nwhere=1=0\n[client]\nuser=backup\npassword=x\n",
])
def test_rejects_unsafe_client_file_before_connect(tmp_path, body):
    with pytest.raises(d.ExportError):
        d.read_client_options(inputs(tmp_path, body)[0])


def test_global_privileges_do_not_accept_schema_runtime_or_role_grants():
    connection = Connection()
    for grants in [
        ["GRANT ALL PRIVILEGES ON `probiga`.* TO 'runtime'@'127.0.0.1'"],
        [GRANT.replace(", BACKUP_ADMIN", "")],
        ["GRANT `backup_role`@`%` TO 'runtime'@'127.0.0.1'"],
    ]:
        connection.grants = grants
        with pytest.raises(d.ExportError, match="BACKUP_PRIVILEGES_REQUIRED"):
            d.preflight_source(connection)
    connection.grants = ["GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost'",
                         "GRANT BACKUP_ADMIN ON *.* TO 'root'@'localhost'"]
    assert d.preflight_source(connection)["server_uuid"] == "source-uuid"


@pytest.mark.parametrize("version,port,cipher", [("5.7.44", 3306, "cipher"), ("8.4.11", 33085, "cipher"),
                                                ("8.4.11", 3306, "")])
def test_version_endpoint_and_active_tls_are_required(version, port, cipher):
    connection = Connection()
    connection.source.update(version=version, port=port)
    connection.cipher = cipher
    with pytest.raises(d.ExportError):
        d.preflight_source(connection)


@pytest.mark.parametrize("engine", ["MyISAM", "MEMORY", None])
def test_nontransactional_table_refused(engine):
    connection = Connection()
    connection.rows["TABLES"][0]["engine"] = engine
    with pytest.raises(d.ExportError, match="NON_TRANSACTIONAL"):
        d.inventory_source(connection)


def test_inventory_covers_unknown_schema_and_all_stored_object_names():
    connection = Connection()
    metadata, fingerprint = d.inventory_source(connection)
    assert metadata["schemas"] == [{"name": "probiga", "estimated_bytes": 1000},
                                   {"name": "unknown_business", "estimated_bytes": 2000}]
    for kind in ("views", "triggers", "routines", "events"):
        assert metadata[kind][0]["schema"] == "probiga"
        assert metadata[kind][0]["definer"]
        assert "definition" not in metadata[kind][0]
    assert len(fingerprint) == 64
    assert not any("mysql.user" in query for query, _ in connection.queries)


def test_hidden_definition_is_not_treated_as_an_absent_object():
    connection = Connection()
    connection.rows["ROUTINES"][0]["definition"] = None
    with pytest.raises(d.ExportError, match="METADATA_INCOMPLETE"):
        d.inventory_source(connection)


def test_structure_fingerprint_ignores_dml_estimates_not_definition_changes():
    connection = Connection()
    _, before = d.inventory_source(connection)
    connection.rows["TABLES"][0].update(estimated_rows=5000, estimated_bytes=600000)
    assert d.inventory_source(connection)[1] == before
    connection.rows["ROUTINES"][0]["definition"] = "select 2"
    assert d.inventory_source(connection)[1] != before


def test_dump_command_has_safe_online_flags_and_no_password():
    command = d.build_dump_command(Path("dump.exe"), Path("secret.ini"), Path("ca.pem"),
                                   Path("app.sql.part"), [{"name": "probiga"}, {"name": "unknown_business"}])
    assert command[1] == "--defaults-file=secret.ini"
    assert "--no-login-paths" in command
    assert {"--single-transaction", "--quick", "--no-tablespaces", "--set-gtid-purged=OFF",
            "--routines", "--triggers", "--events", "--hex-blob", "--skip-lock-tables"}.issubset(command)
    assert command[-3:] == ["--databases", "probiga", "unknown_business"]
    assert not any(item.startswith("--password") for item in command)


@pytest.mark.parametrize("name", ["mysql", "--where=1=0", "", "bad\0name"])
def test_unsafe_schema_command_refused(name):
    with pytest.raises(d.ExportError):
        d.build_dump_command(Path("dump"), Path("ini"), Path("ca"), Path("part"), [{"name": name}])


def test_ready_only_after_success_unlock_and_digest(tmp_path, monkeypatch):
    connection = Connection()
    calls = []
    monkeypatch.setenv("MYSQL_PWD", "secret-env-password")

    def factory(command, **kwargs):
        calls.append((command, kwargs))
        return Process(command)

    metadata = run_export(tmp_path, connection, factory=factory)
    output = tmp_path / "output"
    assert (output / "app.sql").is_file()
    assert not (output / "app.sql.part").exists()
    assert json.loads((output / "metadata.json").read_text(encoding="utf-8"))["status"] == "ready"
    assert metadata["format"] == d.FORMAT
    assert metadata["dump"]["sha256"] == hashlib.sha256((output / "app.sql").read_bytes()).hexdigest()
    assert set(metadata["snapshot_window"]) == {"lock_acquired_at", "lock_released_at", "dump_started_at",
                                               "dump_finished_at"}
    assert "sensitive-password" not in json.dumps(metadata)
    assert "secret-env-password" not in str(calls)
    assert calls[0][1]["shell"] is False
    assert calls[0][1]["stderr"] == subprocess.DEVNULL
    assert connection.closed
    queries = [query for query, _ in connection.queries]
    assert queries.index("LOCK INSTANCE FOR BACKUP") < queries.index("UNLOCK INSTANCE")
    assert not any(query.startswith(("GRANT ", "CREATE USER", "UPDATE ", "DELETE ")) for query in queries)


def test_failed_dump_keeps_part_never_ready_and_unlocks(tmp_path):
    connection = Connection()
    with pytest.raises(d.ExportError, match="MYSQLDUMP_FAILED"):
        run_export(tmp_path, connection, factory=lambda command, **kwargs: Process(command, code=2))
    output = tmp_path / "output"
    assert (output / "app.sql.part").exists()
    assert not (output / "app.sql").exists()
    assert not (output / "metadata.json").exists()
    assert ("UNLOCK INSTANCE", None) in connection.queries
    assert connection.closed


def test_metadata_change_rejects_complete_dump(tmp_path):
    connection = Connection()

    def mutate(process, source):
        source.rows["ROUTINES"][0]["definition"] = "select 999"

    with pytest.raises(d.ExportError, match="METADATA_CHANGED"):
        run_export(tmp_path, connection, monitor=mutate)
    assert not (tmp_path / "output" / "metadata.json").exists()
    assert ("UNLOCK INSTANCE", None) in connection.queries


@pytest.mark.parametrize("filename", ["app.sql", "app.sql.part", "metadata.json", "metadata.json.part"])
def test_existing_export_never_overwritten(tmp_path, filename):
    option, executable, ca, output = inputs(tmp_path)
    output.mkdir()
    existing = output / filename
    existing.write_bytes(b"keep me")
    with pytest.raises(d.ExportError, match="OUTPUT_EXISTS"):
        d.export_database(option, executable, ca, output,
                          connector=lambda *_: pytest.fail("must not connect"))
    assert existing.read_bytes() == b"keep me"


def test_lock_connection_loss_terminates_dump_without_reconnect():
    connection = Connection()
    connection.fail_ping = True
    process = Process(running=True)
    times = iter([0, 31])
    with pytest.raises(d.ExportError, match="BACKUP_LOCK_CONNECTION_LOST"):
        d.monitor_dump(process, connection, clock=lambda: next(times), sleep=lambda _: None)
    assert process.terminated
    assert connection.pings == [False]


def test_success_requires_final_connection_ping():
    connection = Connection()
    connection.fail_ping = True
    with pytest.raises(d.ExportError, match="BACKUP_LOCK_CONNECTION_LOST"):
        d.monitor_dump(Process(), connection)
    assert connection.pings == [False]


def test_periodic_connection_heartbeat_does_not_reconnect():
    connection = Connection()
    process = Process(running=True)
    times = iter([0, 31, 31])
    d.monitor_dump(process, connection, clock=lambda: next(times),
                   sleep=lambda _: setattr(process, "returncode", 0))
    assert connection.pings == [False, False]


def test_process_killed_when_interrupted_and_lock_released(tmp_path):
    connection = Connection()
    processes = []

    def factory(command, **kwargs):
        process = Process(command, running=True)
        processes.append(process)
        return process

    def interrupted(*args):
        raise KeyboardInterrupt()

    with pytest.raises(d.ExportError, match="EXPORT_INTERRUPTED"):
        run_export(tmp_path, connection, factory=factory, monitor=interrupted)
    assert processes[0].terminated
    assert ("UNLOCK INSTANCE", None) in connection.queries
    assert not (tmp_path / "output" / "metadata.json").exists()


def test_driver_errors_are_redacted(tmp_path):
    connection = Connection()
    connection.fail_query = "LOCK INSTANCE"
    with pytest.raises(d.ExportError) as error:
        run_export(tmp_path, connection)
    assert "sensitive-password" not in str(error.value)
    assert str(error.value).startswith("EXPORT_FAILED")
    assert connection.closed


def test_unlock_failure_never_publishes_ready(tmp_path):
    connection = Connection()
    connection.fail_query = "UNLOCK INSTANCE"
    with pytest.raises(d.ExportError, match="EXPORT_FAILED"):
        run_export(tmp_path, connection)
    assert not (tmp_path / "output" / "metadata.json").exists()
    assert (tmp_path / "output" / "app.sql.part").exists()
