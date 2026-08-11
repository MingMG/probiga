from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import run_mysql84_logical_restore as restore


TARGET_UUID = "810354d6-9061-11f1-84ae-74d4dd7f8500"


def _dump_payload(extra: bytes = b"") -> bytes:
    return (
        b"-- MySQL dump 10.13  Distrib 5.5.20, for Win64 (x86)\n"
        b"SET NAMES utf8mb4;\n"
        + extra
        + b"CREATE DATABASE IF NOT EXISTS `biga`;\n"
        b"-- Dump completed on 2026-08-05 10:00:00\n"
    )


def _write_sanitized_pair(
    tmp_path: Path, *, payload: bytes | None = None
) -> tuple[Path, Path, Path]:
    raw = tmp_path / "full-5.5.20.raw.sql"
    dump = tmp_path / "full-5.5.20.mysql84.sql"
    manifest = tmp_path / "full-5.5.20.sanitizer.json"
    raw_payload = _dump_payload(
        b"/*!50003 SET sql_mode = 'NO_AUTO_CREATE_USER,STRICT_TRANS_TABLES' */ ;\n"
        b"/*!50003 SET sql_mode = 'NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION' */ ;\n"
    )
    raw.write_bytes(raw_payload)
    sanitized_payload = _dump_payload() if payload is None else payload
    dump.write_bytes(sanitized_payload)
    report = {
        "source": str(raw.resolve()),
        "output": str(dump.resolve()),
        "source_bytes": len(raw_payload),
        "output_bytes": len(sanitized_payload),
        "lines_scanned": sanitized_payload.count(b"\n"),
        "changed_statements": 2,
        "removed_tokens": 2,
        "source_sha256": hashlib.sha256(raw_payload).hexdigest(),
        "output_sha256": hashlib.sha256(sanitized_payload).hexdigest(),
    }
    manifest.write_text(json.dumps(report), encoding="utf-8")
    return raw, dump, manifest


def _write_option_file(path: Path, *, port: int = 33084, extra: str = "") -> None:
    path.write_text(
        "[client]\n"
        "host=127.0.0.1\n"
        f"port={port}\n"
        "user=restore_admin\n"
        "password=never-log-this\n"
        "protocol=tcp\n"
        + extra,
        encoding="utf-8",
    )
    path.chmod(0o600)


def _observation(
    datadir: Path,
    *,
    port: int = 33084,
    schemas: tuple[str, ...] = (),
    tables: dict[str, int] | None = None,
) -> restore.TargetObservation:
    return restore.TargetObservation(
        version="8.4.11",
        version_comment="MySQL Community Server - GPL",
        server_uuid=TARGET_UUID,
        port=port,
        datadir=str(datadir.resolve()),
        connection_id=42,
        tls_cipher="TLS_AES_256_GCM_SHA384",
        global_log_bin=True,
        global_binlog_format="ROW",
        global_log_bin_trust_function_creators=False,
        business_schemas=schemas,
        tables_by_schema={} if tables is None else tables,
        observed_at_utc="2026-08-05T02:00:00+00:00",
    )


def test_validate_sanitized_dump_binds_path_bytes_hash_count_and_footer(tmp_path: Path):
    raw, dump, manifest = _write_sanitized_pair(tmp_path)

    identity = restore.validate_sanitized_dump(dump, manifest)

    assert identity.path == str(dump.resolve())
    assert identity.source_path == str(raw.resolve())
    assert identity.changed_statements == 2
    assert identity.removed_tokens == 2
    assert identity.sha256 == hashlib.sha256(dump.read_bytes()).hexdigest()
    assert identity.footer.startswith("-- Dump completed on ")


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (lambda report, _dump: report.update(changed_statements=1), "exactly two"),
        (lambda report, _dump: report.update(removed_tokens=3), "exactly two"),
        (lambda report, _dump: report.update(output_bytes=1), "byte count"),
        (lambda report, _dump: report.update(output_sha256="0" * 64), "SHA-256"),
        (
            lambda report, dump: report.update(output=str(dump.with_name("other.sql"))),
            "manifest output",
        ),
    ),
)
def test_validate_sanitized_dump_rejects_manifest_drift(
    tmp_path: Path, mutation, message: str
):
    _raw, dump, manifest = _write_sanitized_pair(tmp_path)
    report = json.loads(manifest.read_text(encoding="utf-8"))
    mutation(report, dump)
    if Path(str(report["output"])).name == "other.sql":
        Path(str(report["output"])).write_bytes(dump.read_bytes())
    manifest.write_text(json.dumps(report), encoding="utf-8")

    with pytest.raises(restore.RestoreError, match=message):
        restore.validate_sanitized_dump(dump, manifest)


def test_validate_sanitized_dump_explicitly_rejects_raw_input(tmp_path: Path):
    raw, _dump, manifest = _write_sanitized_pair(tmp_path)
    report = json.loads(manifest.read_text(encoding="utf-8"))
    report["output"] = str(raw.resolve())
    report["output_bytes"] = raw.stat().st_size
    report["output_sha256"] = hashlib.sha256(raw.read_bytes()).hexdigest()
    manifest.write_text(json.dumps(report), encoding="utf-8")

    with pytest.raises(restore.RestoreError, match="raw MySQL 5.5 dump"):
        restore.validate_sanitized_dump(raw, manifest)


@pytest.mark.parametrize(
    "token",
    (
        b"SET SESSION sql_log_bin=1;\n",
        b"SET GLOBAL max_connections=100;\n",
        b"\\. C:/unsafe/raw.sql\n",
        b"connect other_database\n",
        b"RESET CONNECTION;\n",
        b"SET sql_mode='NO_AUTO_CREATE_USER';\n",
    ),
)
def test_validate_sanitized_dump_rejects_restore_control_tokens(
    tmp_path: Path, token: bytes
):
    _raw, dump, manifest = _write_sanitized_pair(
        tmp_path, payload=_dump_payload(token)
    )

    with pytest.raises(restore.RestoreError, match="forbidden"):
        restore.validate_sanitized_dump(dump, manifest)


def test_validate_sanitized_dump_rejects_truncated_file(tmp_path: Path):
    _raw, dump, manifest = _write_sanitized_pair(
        tmp_path,
        payload=(
            b"-- MySQL dump 10.13  Distrib 5.5.20, for Win64 (x86)\nSELECT 1;\n"
        ),
    )

    with pytest.raises(restore.RestoreError, match="Dump completed"):
        restore.validate_sanitized_dump(dump, manifest)


def test_read_admin_options_is_local_port_bound_and_has_no_tls_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    option_file = tmp_path / "target.ini"
    _write_option_file(option_file)
    monkeypatch.setattr(
        restore,
        "assert_protected_client_option_file",
        lambda path: path.resolve(strict=True),
    )

    options = restore.read_admin_client_options(option_file, expected_port=33084)

    assert (options.host, options.port, options.user) == (
        "127.0.0.1",
        33084,
        "restore_admin",
    )
    assert "never-log-this" not in repr(options)

    _write_option_file(option_file, extra="ssl-mode=DISABLED\n")
    with pytest.raises(restore.RestoreError, match="unsupported options: ssl-mode"):
        restore.read_admin_client_options(option_file, expected_port=33084)


def test_build_mysql_command_has_defaults_first_tls_and_no_secret(tmp_path: Path):
    identity = restore.MySQLClientIdentity(
        executable=str(tmp_path / "mysql.exe"),
        version_output="mysql Ver 8.4.11 for Win64 (MySQL Community Server - GPL)",
    )
    option_file = tmp_path / "admin.ini"
    ca_file = tmp_path / "ca.pem"

    command = restore.build_mysql_command(
        identity=identity,
        client_option_file=option_file,
        ca_file=ca_file,
        expected_port=33084,
    )

    assert command[1] == f"--defaults-file={option_file}"
    assert "--ssl-mode=VERIFY_CA" in command
    assert f"--ssl-ca={ca_file}" in command
    assert "--binary-mode=1" in command
    assert "--skip-reconnect" in command
    assert "--local-infile=0" in command
    assert not any("login-path" in part.casefold() for part in command)
    assert "--port=33084" in command
    assert not any(part.casefold().startswith("--password") for part in command)


def test_inspect_mysql_client_accepts_only_exact_oracle_8411(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    executable = tmp_path / "mysql.exe"
    executable.write_bytes(b"fake")

    monkeypatch.setattr(
        restore.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout=b"mysql Ver 8.4.11 for Win64 on x86_64 (MySQL Community Server - GPL)",
            stderr=b"",
        ),
    )
    assert restore.inspect_mysql_client(executable).version_output.startswith("mysql Ver")

    monkeypatch.setattr(
        restore.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout=b"mysql Ver 8.4.11-MariaDB",
            stderr=b"",
        ),
    )
    with pytest.raises(restore.RestoreError, match="exact Oracle"):
        restore.inspect_mysql_client(executable)

    monkeypatch.setattr(
        restore.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout=b"mysql Ver 8.4.11.1 for Win64 (MySQL Community Server - GPL)",
            stderr=b"",
        ),
    )
    with pytest.raises(restore.RestoreError, match="exact Oracle"):
        restore.inspect_mysql_client(executable)


def test_mysql_environment_is_scrubbed_without_mutating_parent(monkeypatch):
    monkeypatch.setenv("MYSQL_PWD", "secret")
    monkeypatch.setenv("mysql_custom_override", "unsafe")
    monkeypatch.setenv("MARIADB_PLUGIN_DIR", "unsafe")
    monkeypatch.setenv("PROBIGA_SAFE_VALUE", "keep")

    child = restore._scrub_mysql_environment()

    assert not any(name.upper().startswith("MYSQL_") for name in child)
    assert not any(name.upper().startswith("MARIADB_") for name in child)
    assert child["PROBIGA_SAFE_VALUE"] == "keep"
    assert restore.os.environ["MYSQL_PWD"] == "secret"


def test_mode_gates_3306_and_requires_exact_final_attestation():
    with pytest.raises(restore.RestoreError, match="forbidden.*3306"):
        restore._validate_mode("rehearsal", 3306, None)
    with pytest.raises(restore.RestoreError, match="exact offline"):
        restore._validate_mode("final-frozen", 3306, "yes")
    assert (
        restore._validate_mode(
            "final-frozen", 3306, restore.FINAL_FROZEN_ATTESTATION
        )
        is True
    )


def test_target_catalogue_gates_empty_before_and_exact_nonempty_after(tmp_path: Path):
    empty = _observation(tmp_path)
    restore.validate_empty_target(empty)

    with pytest.raises(restore.RestoreError, match="not empty"):
        restore.validate_empty_target(
            _observation(tmp_path, schemas=("probiga",), tables={"probiga": 1})
        )

    restored = _observation(
        tmp_path,
        schemas=tuple(sorted(restore.EXPECTED_SCHEMAS)),
        tables={"biga": 10, "probiga": 168, "probiga_qmt_history": 3},
    )
    restore.validate_restored_target(restored)

    with pytest.raises(restore.RestoreError, match="no tables"):
        restore.validate_restored_target(
            _observation(
                tmp_path,
                schemas=tuple(sorted(restore.EXPECTED_SCHEMAS)),
                tables={"biga": 10, "probiga": 168, "probiga_qmt_history": 0},
            )
        )


class _TargetCursor:
    def __init__(self, fixture: dict):
        self.fixture = fixture
        self.key = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql: str):
        normalized = " ".join(sql.split()).casefold()
        if "select @@version as version" in normalized:
            self.key = "identity"
        elif "show session status" in normalized:
            self.key = "tls"
        elif "select schema_name" in normalized:
            self.key = "schemas"
        elif "count(*) as table_count" in normalized:
            self.key = "tables"
        else:
            raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self):
        return self.fixture[self.key]

    def fetchall(self):
        return self.fixture[self.key]


class _TargetConnection:
    def __init__(self, fixture: dict):
        self.fixture = fixture
        self.closed = False

    def cursor(self):
        return _TargetCursor(self.fixture)

    def close(self):
        self.closed = True


def _target_fixture(datadir: Path) -> dict:
    return {
        "identity": {
            "version": "8.4.11",
            "version_comment": "MySQL Community Server - GPL",
            "server_uuid": TARGET_UUID,
            "port": 33084,
            "datadir": str(datadir.resolve()),
            "connection_id": 19,
            "global_log_bin": 1,
            "global_binlog_format": "ROW",
            "global_log_bin_trust_function_creators": 0,
            "lower_case_table_names": 1,
            "character_set_server": "utf8mb4",
            "collation_server": "utf8mb4_general_ci",
            "default_collation_for_utf8mb4": "utf8mb4_general_ci",
            "global_time_zone": "+08:00",
            "global_sql_mode": (
                "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ZERO_DATE,"
                "NO_ZERO_IN_DATE,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY"
            ),
            "global_require_secure_transport": 1,
            "global_local_infile": 0,
            "global_max_allowed_packet": 268435456,
        },
        "tls": {"Variable_name": "Ssl_cipher", "Value": "TLS_AES_256_GCM_SHA384"},
        "schemas": [
            {"schema_name": name}
            for name in ("information_schema", "mysql", "performance_schema", "sys")
        ],
        "tables": [],
    }


def test_inspect_target_proves_exact_identity_tls_datadir_and_global_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    fixture = _target_fixture(tmp_path)
    connection = _TargetConnection(fixture)
    monkeypatch.setattr(restore, "_connect_target", lambda *_args: connection)

    observation = restore.inspect_target(
        restore.AdminClientOptions("127.0.0.1", 33084, "admin", "secret"),
        tmp_path / "ca.pem",
        expected_server_uuid=TARGET_UUID,
        expected_server_port=33084,
        expected_datadir=tmp_path,
    )

    assert observation.version == "8.4.11"
    assert observation.tls_cipher == "TLS_AES_256_GCM_SHA384"
    assert observation.global_log_bin is True
    assert observation.global_binlog_format == "ROW"
    assert observation.global_log_bin_trust_function_creators is False
    assert observation.business_schemas == ()
    assert connection.closed


def test_inspect_target_accepts_information_schema_uppercase_labels(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    fixture = _target_fixture(tmp_path)
    fixture["schemas"] = [
        {"SCHEMA_NAME": name}
        for name in (
            "biga",
            "information_schema",
            "mysql",
            "performance_schema",
            "probiga",
            "probiga_qmt_history",
            "sys",
        )
    ]
    fixture["tables"] = [
        {"TABLE_SCHEMA": "biga", "TABLE_COUNT": 10},
        {"TABLE_SCHEMA": "probiga", "TABLE_COUNT": 168},
        {"TABLE_SCHEMA": "probiga_qmt_history", "TABLE_COUNT": 3},
    ]
    connection = _TargetConnection(fixture)
    monkeypatch.setattr(restore, "_connect_target", lambda *_args: connection)

    observation = restore.inspect_target(
        restore.AdminClientOptions("127.0.0.1", 33084, "admin", "secret"),
        tmp_path / "ca.pem",
        expected_server_uuid=TARGET_UUID,
        expected_server_port=33084,
        expected_datadir=tmp_path,
    )

    assert observation.business_schemas == (
        "biga",
        "probiga",
        "probiga_qmt_history",
    )
    assert observation.tables_by_schema == {
        "biga": 10,
        "probiga": 168,
        "probiga_qmt_history": 3,
    }


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (lambda f: f["identity"].update(version="8.4.12"), "exact Oracle"),
        (
            lambda f: f["identity"].update(
                version="8.4.11-MariaDB", version_comment="MariaDB Server"
            ),
            "exact Oracle",
        ),
        (lambda f: f["identity"].update(server_uuid="00000000-0000-0000-0000-000000000000"), "UUID"),
        (lambda f: f["identity"].update(port=33085), "port"),
        (lambda f: f["identity"].update(datadir=str(Path.cwd())), "datadir"),
        (lambda f: f.update(tls={"Variable_name": "Ssl_cipher", "Value": ""}), "TLS cipher"),
        (lambda f: f["identity"].update(global_log_bin=0), "log_bin must remain ON"),
        (lambda f: f["identity"].update(global_binlog_format="MIXED"), "must be ROW"),
        (
            lambda f: f["identity"].update(
                global_log_bin_trust_function_creators=1
            ),
            "must be OFF",
        ),
        (
            lambda f: f["identity"].update(lower_case_table_names=0),
            "lower_case_table_names must be 1",
        ),
        (
            lambda f: f["identity"].update(character_set_server="latin1"),
            "character_set_server must be utf8mb4",
        ),
        (
            lambda f: f["identity"].update(collation_server="utf8mb4_0900_ai_ci"),
            "collation_server must be utf8mb4_general_ci",
        ),
        (
            lambda f: f["identity"].update(default_collation_for_utf8mb4="utf8mb4_0900_ai_ci"),
            "default_collation_for_utf8mb4 must be utf8mb4_general_ci",
        ),
        (
            lambda f: f["identity"].update(global_time_zone="SYSTEM"),
            "target global time_zone must be \\+08:00",
        ),
        (
            lambda f: f["identity"].update(global_sql_mode="STRICT_TRANS_TABLES"),
            "strict production policy",
        ),
        (
            lambda f: f["identity"].update(global_require_secure_transport=0),
            "require_secure_transport must be ON",
        ),
        (
            lambda f: f["identity"].update(global_local_infile=1),
            "local_infile must be OFF",
        ),
        (
            lambda f: f["identity"].update(global_max_allowed_packet=1024),
            "max_allowed_packet is below 256 MiB",
        ),
    ),
)
def test_inspect_target_rejects_identity_tls_or_global_policy_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation,
    message: str,
):
    fixture = _target_fixture(tmp_path)
    mutation(fixture)
    connection = _TargetConnection(fixture)
    monkeypatch.setattr(restore, "_connect_target", lambda *_args: connection)

    with pytest.raises(restore.RestoreError, match=message):
        restore.inspect_target(
            restore.AdminClientOptions("127.0.0.1", 33084, "admin", "secret"),
            tmp_path / "ca.pem",
            expected_server_uuid=TARGET_UUID,
            expected_server_port=33084,
            expected_datadir=tmp_path,
        )
    assert connection.closed


class _CaptureStdin:
    def __init__(self):
        self.payload = bytearray()
        self.closed = False

    def write(self, value: bytes):
        self.payload.extend(value)
        return len(value)

    def flush(self):
        return None

    def close(self):
        self.closed = True


def test_restore_stream_can_defer_secondary_indexes(tmp_path: Path):
    dump = tmp_path / "deferred.sql"
    dump.write_bytes(
        b"USE `probiga`;\n"
        b"CREATE TABLE `t` (\n"
        b"  `id` int NOT NULL,\n"
        b"  PRIMARY KEY (`id`),\n"
        b"  KEY `ix_id` (`id`)\n"
        b") ENGINE=InnoDB;\n"
        b"UNLOCK TABLES;\n"
    )
    stdin = _CaptureStdin()
    stats = restore._write_restore_input(
        stdin,
        dump,
        split_insert_bytes=64 * 1024,
        defer_secondary_indexes=("probiga.t",),
    )
    payload = bytes(stdin.payload)
    assert stats is not None
    assert stats.matched_names == {"probiga.t"}
    assert b"PRIMARY KEY (`id`)" in payload
    assert b"ADD KEY `ix_id` (`id`);" in payload


class _FakePopen:
    return_code = 0
    stderr_payload = b""
    include_marker = True
    calls: list[tuple[tuple[str, ...], dict, "_FakePopen"]] = []

    def __init__(self, command, **kwargs):
        self.command = tuple(command)
        self.kwargs = kwargs
        self.stdin = _CaptureStdin()
        type(self).calls.append((self.command, kwargs, self))

    def wait(self, timeout=None):
        if type(self).include_marker:
            self.kwargs["stdout"].write(
                (restore.SESSION_BINLOG_OFF_MARKER + "\n").encode("ascii")
            )
            self.kwargs["stdout"].flush()
        self.kwargs["stderr"].write(type(self).stderr_payload)
        self.kwargs["stderr"].flush()
        return type(self).return_code

    def terminate(self):
        return None

    def kill(self):
        return None


def _install_fake_popen(monkeypatch: pytest.MonkeyPatch, **overrides):
    fake = type("FakePopenForRestore", (_FakePopen,), {})
    fake.calls = []
    for name, value in overrides.items():
        setattr(fake, name, value)
    monkeypatch.setattr(restore.subprocess, "Popen", fake)
    return fake


def _prepare_orchestration(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _raw, dump, manifest = _write_sanitized_pair(tmp_path)
    option_file = tmp_path / "admin.ini"
    ca_file = tmp_path / "ca.pem"
    mysql = tmp_path / "mysql.exe"
    datadir = tmp_path / "data"
    _write_option_file(option_file)
    ca_file.write_text("fake test CA", encoding="utf-8")
    mysql.write_bytes(b"fake")
    datadir.mkdir()
    monkeypatch.setattr(
        restore,
        "assert_protected_client_option_file",
        lambda path: path.resolve(strict=True),
    )
    monkeypatch.setattr(
        restore, "validate_ca_file", lambda path: path.resolve(strict=True)
    )
    monkeypatch.setattr(
        restore,
        "inspect_mysql_client",
        lambda path: restore.MySQLClientIdentity(
            executable=str(path.resolve()),
            version_output="mysql Ver 8.4.11 for Win64 (MySQL Community Server - GPL)",
        ),
    )
    before = _observation(datadir)
    after = _observation(
        datadir,
        schemas=tuple(sorted(restore.EXPECTED_SCHEMAS)),
        tables={"biga": 10, "probiga": 168, "probiga_qmt_history": 3},
    )
    observations = iter((before, after))
    monkeypatch.setattr(
        restore, "inspect_target", lambda *args, **kwargs: next(observations)
    )
    return option_file, ca_file, mysql, datadir, dump, manifest


def test_restore_streams_prelude_and_dump_in_one_waited_tls_client_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    option_file, ca_file, mysql, datadir, dump, manifest = _prepare_orchestration(
        tmp_path, monkeypatch
    )
    fake = _install_fake_popen(monkeypatch)
    evidence = tmp_path / "restore-evidence.json"

    report = restore.run_logical_restore(
        mode="rehearsal",
        client_option_file=option_file,
        ssl_ca=ca_file,
        mysql_executable=mysql,
        dump_path=dump,
        sanitizer_manifest=manifest,
        expected_server_uuid=TARGET_UUID,
        expected_server_port=33084,
        expected_datadir=datadir,
        evidence=evidence,
    )

    assert report["status"] == "success"
    assert report["target_after"]["global_log_bin"] is True
    assert report["target_after"]["global_binlog_format"] == "ROW"
    assert report["target_after"]["global_log_bin_trust_function_creators"] is False
    persisted = json.loads(evidence.read_text(encoding="utf-8"))
    assert persisted["status"] == "success"

    command, kwargs, process = fake.calls[0]
    assert command[1] == f"--defaults-file={option_file.resolve()}"
    assert "--ssl-mode=VERIFY_CA" in command
    assert kwargs["shell"] is False
    assert kwargs["stdin"] is restore.subprocess.PIPE
    assert "MYSQL_PWD" not in kwargs["env"]
    assert not any("never-log-this" in part for part in command)
    assert process.stdin.closed
    payload = bytes(process.stdin.payload)
    assert payload.startswith(b"SET SESSION sql_log_bin=0;\n")
    assert payload.endswith(dump.read_bytes())
    assert payload.count(dump.read_bytes()) == 1
    assert list(tmp_path.glob(".*.partial")) == []


@pytest.mark.parametrize(
    ("overrides", "message"),
    (
        ({"return_code": 9}, "return code 9"),
        ({"stderr_payload": b"ERROR 1064 failed\n"}, "stderr contains an ERROR"),
        ({"include_marker": False}, "did not prove session sql_log_bin"),
    ),
)
def test_restore_failure_is_blocked_and_atomically_attested(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    overrides: dict,
    message: str,
):
    option_file, ca_file, mysql, datadir, dump, manifest = _prepare_orchestration(
        tmp_path, monkeypatch
    )
    # A failed child must not consume the postflight observation.
    calls = 0
    before = _observation(datadir)

    def only_before(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls > 1:
            pytest.fail("postflight must not run after a failed import")
        return before

    monkeypatch.setattr(restore, "inspect_target", only_before)
    _install_fake_popen(monkeypatch, **overrides)
    evidence = tmp_path / "failed-evidence.json"

    with pytest.raises(restore.RestoreError, match=message):
        restore.run_logical_restore(
            mode="rehearsal",
            client_option_file=option_file,
            ssl_ca=ca_file,
            mysql_executable=mysql,
            dump_path=dump,
            sanitizer_manifest=manifest,
            expected_server_uuid=TARGET_UUID,
            expected_server_port=33084,
            expected_datadir=datadir,
            evidence=evidence,
        )

    failed = json.loads(evidence.read_text(encoding="utf-8"))
    assert failed["status"] == "failed"
    assert failed["target_before"]["server_uuid"] == TARGET_UUID
    assert list(tmp_path.glob(".*.partial")) == []


def test_existing_artifact_rejected_before_credentials_network_or_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    datadir = tmp_path / "data"
    datadir.mkdir()
    evidence = tmp_path / "restore.json"
    evidence.write_text("keep", encoding="utf-8")
    monkeypatch.setattr(
        restore,
        "assert_protected_client_option_file",
        lambda _path: pytest.fail("credentials must not be touched"),
    )

    with pytest.raises(restore.RestoreError, match="refusing to overwrite"):
        restore.run_logical_restore(
            mode="rehearsal",
            client_option_file=tmp_path / "missing.ini",
            ssl_ca=tmp_path / "missing.pem",
            mysql_executable=tmp_path / "missing.exe",
            dump_path=tmp_path / "missing.sql",
            sanitizer_manifest=tmp_path / "missing.json",
            expected_server_uuid=TARGET_UUID,
            expected_server_port=33084,
            expected_datadir=datadir,
            evidence=evidence,
        )
    assert evidence.read_text(encoding="utf-8") == "keep"


def test_postflight_policy_or_catalogue_failure_blocks_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    option_file, ca_file, mysql, datadir, dump, manifest = _prepare_orchestration(
        tmp_path, monkeypatch
    )
    before = _observation(datadir)
    bad_after = restore.TargetObservation(
        **{
            **asdict_for_test(_observation(
                datadir,
                schemas=tuple(sorted(restore.EXPECTED_SCHEMAS)),
                tables={"biga": 10, "probiga": 168, "probiga_qmt_history": 3},
            )),
            "global_log_bin_trust_function_creators": True,
        }
    )
    observations = iter((before, bad_after))
    monkeypatch.setattr(
        restore, "inspect_target", lambda *args, **kwargs: next(observations)
    )
    _install_fake_popen(monkeypatch)
    evidence = tmp_path / "policy-failed.json"

    # inspect_target normally raises this policy error itself.  A mocked
    # observation still proves the success gate must reject an invalid result.
    monkeypatch.setattr(
        restore,
        "validate_restored_target",
        lambda observation: (_ for _ in ()).throw(
            restore.RestoreError("trust must remain OFF")
        )
        if observation.global_log_bin_trust_function_creators
        else None,
    )
    with pytest.raises(restore.RestoreError, match="trust must remain OFF"):
        restore.run_logical_restore(
            mode="rehearsal",
            client_option_file=option_file,
            ssl_ca=ca_file,
            mysql_executable=mysql,
            dump_path=dump,
            sanitizer_manifest=manifest,
            expected_server_uuid=TARGET_UUID,
            expected_server_port=33084,
            expected_datadir=datadir,
            evidence=evidence,
        )
    assert json.loads(evidence.read_text(encoding="utf-8"))["status"] == "failed"


def asdict_for_test(value: restore.TargetObservation) -> dict:
    return {
        field: getattr(value, field)
        for field in value.__dataclass_fields__
    }
