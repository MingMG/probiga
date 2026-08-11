from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import run_mysql55_consistent_dump as dump


def _source_preflight(
    *, client_sessions: int = 0, active_transactions: int = 0
) -> dump.SourcePreflight:
    return dump.SourcePreflight(
        identity=dump.SourceIdentity(
            version="5.5.20-log",
            version_comment="MySQL Community Server (GPL)",
            port=3306,
            hostname="legacy-db",
            server_id=1,
            datadir="C:\\ProgramData\\MySQL\\MySQL Server 5.5\\Data\\",
            connection_id=91,
            read_only=False,
            log_bin=False,
            binlog_format="STATEMENT",
        ),
        schemas=dump.EXPECTED_SCHEMAS,
        legacy_empty_schema=dump.LegacySchemaObservation(
            schema="test",
            present=True,
            object_counts={"tables": 0, "routines": 0, "events": 0, "triggers": 0},
        ),
        tables=dump.TableInventory(
            total_tables=181,
            tables_by_schema={"biga": 10, "probiga": 168, "probiga_qmt_history": 3},
            canonical_sha256="a" * 64,
        ),
        sessions=dump.SessionObservation(
            client_session_count=client_sessions,
            client_session_sample=(
                {
                    "id": 99,
                    "user": "root",
                    "host": "localhost:50000",
                    "database_name": "probiga",
                    "command": "Sleep",
                    "seconds": 1,
                    "state": "",
                },
            )
            if client_sessions
            else (),
            active_transaction_count=active_transactions,
            active_transaction_sample=(
                {
                    "transaction_id": "123",
                    "thread_id": 99,
                    "state": "RUNNING",
                    "started": "2026-08-05 09:00:00",
                },
            )
            if active_transactions
            else (),
        ),
        observed_at_utc="2026-08-05T01:00:00+00:00",
    )


def _write_client_file(path: Path, extra: str = "") -> None:
    path.write_text(
        "[client]\n"
        "host=127.0.0.1\n"
        "port=3306\n"
        "user=root\n"
        "password=do-not-log-this\n"
        + extra,
        encoding="utf-8",
    )
    path.chmod(0o600)


def _prepare_local_inputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    option_file = tmp_path / "source-client.ini"
    executable = tmp_path / "mysqldump.exe"
    _write_client_file(option_file)
    executable.write_bytes(b"fake")
    monkeypatch.setattr(
        dump,
        "assert_protected_client_option_file",
        lambda path: path.expanduser().resolve(strict=True),
    )
    monkeypatch.setattr(
        dump,
        "inspect_mysqldump_version",
        lambda path: dump.MysqldumpIdentity(
            executable=str(path.resolve()),
            version_output="mysqldump Ver 10.13 Distrib 5.5.20, for Win64 (x86)",
        ),
    )
    return option_file, executable


class _FakePopen:
    return_code = 0
    stderr_payload = b""
    stdout_payload = b""
    dump_payload = b"-- header\n-- Dump completed on 2026-08-05 09:00:00\n"
    calls: list[tuple[tuple[str, ...], dict]] = []

    def __init__(self, command, **kwargs):
        command = tuple(command)
        type(self).calls.append((command, kwargs))
        result_option = next(item for item in command if item.startswith("--result-file="))
        Path(result_option.split("=", 1)[1]).write_bytes(type(self).dump_payload)
        kwargs["stdout"].write(type(self).stdout_payload)
        kwargs["stderr"].write(type(self).stderr_payload)
        kwargs["stdout"].flush()
        kwargs["stderr"].flush()

    def wait(self, timeout=None):
        return type(self).return_code

    def terminate(self):
        return None

    def kill(self):
        return None


def _install_fake_dump_process(monkeypatch: pytest.MonkeyPatch, **overrides):
    fake_type = type("FakePopenForTest", (_FakePopen,), {})
    fake_type.calls = []
    for name, value in overrides.items():
        setattr(fake_type, name, value)
    monkeypatch.setattr(dump.subprocess, "Popen", fake_type)
    return fake_type


def test_build_command_has_fixed_safe_flags_and_defaults_file_first(tmp_path: Path):
    identity = dump.MysqldumpIdentity(
        executable=str(tmp_path / "mysqldump.exe"), version_output="5.5.20"
    )
    command = dump.build_mysqldump_command(
        identity=identity,
        client_option_file=tmp_path / "protected.ini",
        result_file=tmp_path / "partial.sql",
    )

    assert command[1] == f"--defaults-file={tmp_path / 'protected.ini'}"
    assert command[2:5] == ("--protocol=tcp", "--host=127.0.0.1", "--port=3306")
    for required in (
        "--single-transaction",
        "--quick",
        "--skip-lock-tables",
        "--routines",
        "--events",
        "--triggers",
        "--hex-blob",
        "--default-character-set=utf8mb4",
        "--max_allowed_packet=256M",
        "--databases",
    ):
        assert required in command
    assert command[-3:] == dump.EXPECTED_SCHEMAS
    assert not any("password" in part.casefold() for part in command)


def test_build_command_can_capture_comment_only_binlog_coordinates(tmp_path: Path):
    identity = dump.MysqldumpIdentity(
        executable=str(tmp_path / "mysqldump.exe"), version_output="5.5.20"
    )
    command = dump.build_mysqldump_command(
        identity=identity,
        client_option_file=tmp_path / "protected.ini",
        result_file=tmp_path / "partial.sql",
        capture_binlog_coordinates=True,
    )

    assert "--master-data=2" in command
    assert "--master-data=1" not in command


def test_read_dump_binlog_coordinates_requires_one_safe_comment(tmp_path: Path):
    output = tmp_path / "snapshot.sql"
    output.write_bytes(
        b"-- header\n"
        b"-- CHANGE MASTER TO MASTER_LOG_FILE='mysql-bin.000123', MASTER_LOG_POS=4567;\n"
        b"-- Dump completed on 2026-08-07 21:00:00\n"
    )

    assert dump._read_dump_binlog_coordinates(output) == {
        "file": "mysql-bin.000123",
        "position": 4567,
    }


def test_read_client_options_accepts_only_minimal_local_3306_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    option_file = tmp_path / "source.ini"
    _write_client_file(option_file, "protocol=tcp\n")
    monkeypatch.setattr(
        dump,
        "assert_protected_client_option_file",
        lambda path: path.resolve(strict=True),
    )

    options = dump.read_client_options(option_file)

    assert (options.host, options.port, options.user) == ("127.0.0.1", 3306, "root")
    assert "do-not-log-this" not in repr(options)


@pytest.mark.parametrize(
    ("contents", "message"),
    (
        (
            "[client]\nhost=10.0.0.8\nport=3306\nuser=root\npassword=x\n",
            "host must be exactly",
        ),
        (
            "[client]\nhost=127.0.0.1\nport=3307\nuser=root\npassword=x\n",
            "port must be exactly",
        ),
        (
            "[client]\nhost=127.0.0.1\nport=3306\nuser=root\npassword=x\n"
            "[mysqldump]\nignore-table=probiga.orders\n",
            r"only one \[client\] section",
        ),
        (
            "[client]\nhost=127.0.0.1\nport=3306\nuser=root\npassword=x\n"
            "database=probiga\n",
            "unsupported options: database",
        ),
    ),
)
def test_read_client_options_rejects_unsafe_or_ambiguous_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    contents: str,
    message: str,
):
    option_file = tmp_path / "source.ini"
    option_file.write_text(contents, encoding="utf-8")
    monkeypatch.setattr(
        dump,
        "assert_protected_client_option_file",
        lambda path: path.resolve(strict=True),
    )

    with pytest.raises(dump.DumpError, match=message):
        dump.read_client_options(option_file)


def test_windows_acl_rejects_broad_read_principals():
    safe = [
        {"sid": "S-1-5-18", "type": "Allow", "rights": 2032127},
        {"sid": "S-1-5-32-544", "type": "Allow", "rights": 2032127},
    ]
    broad_read = safe + [
        {"sid": "S-1-5-32-545", "type": "Allow", "rights": 131209}
    ]

    assert dump._windows_acl_is_safe(safe)
    assert not dump._windows_acl_is_safe(broad_read)


class _FakeCursor:
    def __init__(self, fixture: dict):
        self.fixture = fixture
        self.key = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql, params=None):
        self.fixture.setdefault("_executed_sql", []).append(sql)
        normalized = " ".join(sql.split()).casefold()
        if "select @@version as version" in normalized:
            self.key = "identity"
        elif "from information_schema.schemata" in normalized:
            self.key = "schemas"
        elif "'tables' as object_type" in normalized and "union all" in normalized:
            self.key = "test_objects"
        elif "from information_schema.tables" in normalized:
            self.key = "tables"
        elif "count(*) as count from information_schema.processlist" in normalized:
            self.key = "client_count"
        elif "from information_schema.processlist" in normalized:
            self.key = "client_sample"
        elif "count(*) as count from information_schema.innodb_trx" in normalized:
            self.key = "transaction_count"
        elif "from information_schema.innodb_trx" in normalized:
            self.key = "transaction_sample"
        else:
            raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self):
        return self.fixture[self.key]

    def fetchall(self):
        return self.fixture[self.key]


class _FakeConnection:
    def __init__(self, fixture: dict):
        self.fixture = fixture
        self.closed = False

    def cursor(self):
        return _FakeCursor(self.fixture)

    def close(self):
        self.closed = True


def _preflight_fixture() -> dict:
    return {
        "identity": {
            "version": "5.5.20-log",
            "version_comment": "MySQL Community Server (GPL)",
            "port": 3306,
            "hostname": "legacy-db",
            "server_id": 1,
            "datadir": "C:\\ProgramData\\MySQL\\MySQL Server 5.5\\Data\\",
            "connection_id": 91,
            "read_only": 0,
            "log_bin": 0,
            "binlog_format": "STATEMENT",
        },
        "schemas": [{"schema_name": name} for name in dump.EXPECTED_SCHEMAS],
        "test_objects": [
            {"object_type": name, "object_count": 0}
            for name in ("tables", "routines", "events", "triggers")
        ],
        "tables": [
            {
                "table_schema": name,
                "table_name": f"table_{index}",
                "table_type": "BASE TABLE",
                "engine": "InnoDB",
            }
            for index, name in enumerate(dump.EXPECTED_SCHEMAS, start=1)
        ],
        "client_count": {"count": 0},
        "client_sample": [],
        "transaction_count": {"count": 0},
        "transaction_sample": [],
    }


def test_source_preflight_verifies_identity_schemas_engines_and_sessions(monkeypatch):
    fixture = _preflight_fixture()
    connection = _FakeConnection(fixture)
    monkeypatch.setattr(dump, "_connect_source", lambda _options: connection)

    report = dump.preflight_source(
        dump.ClientOptions("127.0.0.1", 3306, "root", "secret")
    )

    assert report.identity.version == "5.5.20-log"
    assert report.identity.port == 3306
    assert report.schemas == dump.EXPECTED_SCHEMAS
    assert report.legacy_empty_schema == dump.LegacySchemaObservation(
        schema="test",
        present=False,
        object_counts={"tables": 0, "routines": 0, "events": 0, "triggers": 0},
    )
    assert report.tables.total_tables == 3
    assert report.tables.tables_by_schema == {
        "biga": 1,
        "probiga": 1,
        "probiga_qmt_history": 1,
    }
    assert len(report.tables.canonical_sha256) == 64
    processlist_sql = " ".join(fixture["_executed_sql"]).casefold()
    assert "db as database_name" in processlist_sql
    assert "db as database," not in processlist_sql
    assert connection.closed


def test_source_preflight_allows_only_a_proven_empty_legacy_test_schema(monkeypatch):
    fixture = _preflight_fixture()
    fixture["schemas"].append({"schema_name": "test"})
    connection = _FakeConnection(fixture)
    monkeypatch.setattr(dump, "_connect_source", lambda _options: connection)

    report = dump.preflight_source(
        dump.ClientOptions("127.0.0.1", 3306, "root", "secret")
    )

    assert report.legacy_empty_schema.present is True
    assert report.legacy_empty_schema.object_counts == {
        "tables": 0,
        "routines": 0,
        "events": 0,
        "triggers": 0,
    }
    assert connection.closed


@pytest.mark.parametrize("nonempty_type", ("tables", "routines", "events", "triggers"))
def test_source_preflight_rejects_every_object_type_in_legacy_test_schema(
    monkeypatch, nonempty_type
):
    fixture = _preflight_fixture()
    fixture["schemas"].append({"schema_name": "test"})
    for row in fixture["test_objects"]:
        if row["object_type"] == nonempty_type:
            row["object_count"] = 1
    connection = _FakeConnection(fixture)
    monkeypatch.setattr(dump, "_connect_source", lambda _options: connection)

    with pytest.raises(dump.DumpError, match=f"non-empty types: {nonempty_type}"):
        dump.preflight_source(
            dump.ClientOptions("127.0.0.1", 3306, "root", "secret")
        )
    assert connection.closed


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (lambda f: f["identity"].update(version="5.5.21"), "exact Oracle MySQL 5.5.20"),
        (
            lambda f: f["identity"].update(
                version="5.5.20-MariaDB", version_comment="MariaDB Server"
            ),
            "exact Oracle MySQL 5.5.20",
        ),
        (lambda f: f["identity"].update(port=3307), "port must be exactly 3306"),
            (
                lambda f: f.update(schemas=f["schemas"] + [{"schema_name": "unexpected"}]),
                "exact business set",
            ),
        (lambda f: f["tables"][0].update(engine="MyISAM"), "InnoDB base table"),
        (lambda f: f["tables"][0].update(table_type="VIEW", engine=None), "InnoDB base table"),
    ),
)
def test_source_preflight_rejects_source_drift(monkeypatch, mutation, message):
    fixture = _preflight_fixture()
    mutation(fixture)
    connection = _FakeConnection(fixture)
    monkeypatch.setattr(dump, "_connect_source", lambda _options: connection)

    with pytest.raises(dump.DumpError, match=message):
        dump.preflight_source(
            dump.ClientOptions("127.0.0.1", 3306, "root", "secret")
        )
    assert connection.closed


def test_inspect_mysqldump_requires_exact_5520_and_rejects_forks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    executable = tmp_path / "mysqldump.exe"
    executable.write_bytes(b"fake")

    monkeypatch.setattr(
        dump.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout=b"mysqldump Ver 10.13 Distrib 5.5.20, for Win64 (x86)",
            stderr=b"",
        ),
    )
    assert dump.inspect_mysqldump_version(executable).version_output.startswith("mysqldump")

    monkeypatch.setattr(
        dump.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout=b"mysqldump Distrib 5.5.20-MariaDB",
            stderr=b"",
        ),
    )
    with pytest.raises(dump.DumpError, match="exact Oracle"):
        dump.inspect_mysqldump_version(executable)


def test_final_frozen_requires_exact_attestation_before_local_or_network_work(tmp_path: Path):
    with pytest.raises(dump.DumpError, match="exact explicit"):
        dump.run_consistent_dump(
            mode="final-frozen",
            client_option_file=tmp_path / "missing.ini",
            mysqldump_executable=tmp_path / "missing.exe",
            output=tmp_path / "out.sql",
        )


@pytest.mark.parametrize(
    ("client_sessions", "active_transactions", "message"),
    (
        (1, 0, "other client sessions"),
        (0, 1, "active transactions"),
    ),
)
def test_final_frozen_refuses_sessions_or_transactions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    client_sessions: int,
    active_transactions: int,
    message: str,
):
    option_file, executable = _prepare_local_inputs(tmp_path, monkeypatch)
    monkeypatch.setattr(
        dump,
        "preflight_source",
        lambda _options: _source_preflight(
            client_sessions=client_sessions, active_transactions=active_transactions
        ),
    )
    monkeypatch.setattr(
        dump.subprocess,
        "Popen",
        lambda *args, **kwargs: pytest.fail("mysqldump must not be launched"),
    )

    with pytest.raises(dump.DumpError, match=message):
        dump.run_consistent_dump(
            mode="final-frozen",
            writes_frozen_attestation=dump.FINAL_FROZEN_ATTESTATION,
            client_option_file=option_file,
            mysqldump_executable=executable,
            output=tmp_path / "final.sql",
        )


def test_online_rehearsal_launches_and_waits_for_real_return_code_then_manifests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    option_file, executable = _prepare_local_inputs(tmp_path, monkeypatch)
    monkeypatch.setattr(
        dump,
        "preflight_source",
        lambda _options: _source_preflight(client_sessions=2, active_transactions=1),
    )
    fake_process = _install_fake_dump_process(monkeypatch)
    output = tmp_path / "backup.sql"

    report = dump.run_consistent_dump(
        mode="online-rehearsal",
        client_option_file=option_file,
        mysqldump_executable=executable,
        output=output,
    )

    assert output.read_bytes() == fake_process.dump_payload
    assert report["mysqldump"]["return_code"] == 0
    assert report["source_preflight"]["sessions"]["client_session_count"] == 2
    assert report["artifacts"]["dump"]["sha256"] == hashlib.sha256(
        fake_process.dump_payload
    ).hexdigest()
    assert Path(str(output) + ".stderr.log").stat().st_size == 0
    manifest_path = Path(str(output) + ".manifest.json")
    assert json.loads(manifest_path.read_text(encoding="utf-8"))["status"] == "success"
    assert list(tmp_path.glob(".*.partial")) == []

    command, kwargs = fake_process.calls[0]
    assert command[1] == f"--defaults-file={option_file.resolve()}"
    assert kwargs["shell"] is False
    assert kwargs["stdin"] is dump.subprocess.DEVNULL
    expected_flags = dump._BELOW_NORMAL_PRIORITY_CLASS if dump.os.name == "nt" else 0
    assert kwargs["creationflags"] == expected_flags
    assert "MYSQL_PWD" not in kwargs["env"]


@pytest.mark.parametrize(
    ("overrides", "message"),
    (
        ({"return_code": 7, "stderr_payload": b"dump failed\n"}, "return code 7"),
        ({"return_code": 0, "stderr_payload": b"warning\n"}, "wrote to stderr"),
        ({"return_code": 0, "dump_payload": b"incomplete sql\n"}, "Dump completed"),
        ({"return_code": 0, "dump_payload": b""}, "empty"),
    ),
)
def test_failed_or_untrustworthy_dump_is_never_published_or_manifested(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    overrides: dict,
    message: str,
):
    option_file, executable = _prepare_local_inputs(tmp_path, monkeypatch)
    monkeypatch.setattr(dump, "preflight_source", lambda _options: _source_preflight())
    _install_fake_dump_process(monkeypatch, **overrides)
    output = tmp_path / "backup.sql"

    with pytest.raises(dump.DumpError, match=message):
        dump.run_consistent_dump(
            mode="online-rehearsal",
            client_option_file=option_file,
            mysqldump_executable=executable,
            output=output,
        )

    assert not output.exists()
    assert not Path(str(output) + ".manifest.json").exists()
    assert Path(str(output) + ".stdout.log").exists()
    assert Path(str(output) + ".stderr.log").exists()


def test_existing_artifact_is_rejected_before_credentials_or_process_are_touched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    output = tmp_path / "backup.sql"
    output.write_bytes(b"keep-me")
    monkeypatch.setattr(
        dump,
        "assert_protected_client_option_file",
        lambda _path: pytest.fail("credentials must not be touched"),
    )

    with pytest.raises(dump.DumpError, match="refusing to overwrite"):
        dump.run_consistent_dump(
            mode="online-rehearsal",
            client_option_file=tmp_path / "source.ini",
            mysqldump_executable=tmp_path / "mysqldump.exe",
            output=output,
        )
    assert output.read_bytes() == b"keep-me"


def test_atomic_publish_never_replaces_destination(tmp_path: Path):
    source = tmp_path / "partial"
    destination = tmp_path / "final"
    source.write_bytes(b"new")
    destination.write_bytes(b"old")

    with pytest.raises(dump.DumpError, match="refusing to overwrite"):
        dump._publish_no_replace(source, destination)

    assert source.read_bytes() == b"new"
    assert destination.read_bytes() == b"old"
