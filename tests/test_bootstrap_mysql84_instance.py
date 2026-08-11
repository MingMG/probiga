from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import bootstrap_mysql84_instance as bootstrap


SERVER_UUID = "11111111-2222-3333-4444-555555555555"


def _identity(tmp_path: Path) -> bootstrap.MysqldIdentity:
    executable = tmp_path / "software" / "bin" / "mysqld.exe"
    executable.parent.mkdir(parents=True, exist_ok=True)
    executable.write_bytes(b"fake")
    return bootstrap.MysqldIdentity(
        executable=str(executable),
        basedir=str(executable.parent.parent),
        version_output=(
            "mysqld  Ver 8.4.11 for Win64 on x86_64 "
            "(MySQL Community Server - GPL)"
        ),
    )


def _fresh_layout(datadir: Path) -> None:
    datadir.mkdir(exist_ok=True)
    (datadir / "auto.cnf").write_text(
        f"[auto]\nserver-uuid={SERVER_UUID}\n", encoding="utf-8"
    )
    (datadir / "ibdata1").write_bytes(b"x")
    (datadir / "mysql.ibd").write_bytes(b"x")
    (datadir / "mysql").mkdir(exist_ok=True)
    (datadir / "#innodb_redo").mkdir(exist_ok=True)


def _write_generated_certificates(datadir: Path) -> None:
    certificate = b"-----BEGIN CERTIFICATE-----\n" + b"A" * 200 + b"\n-----END CERTIFICATE-----\n"
    private_key = b"-----BEGIN PRIVATE KEY-----\n" + b"B" * 200 + b"\n-----END PRIVATE KEY-----\n"
    (datadir / "ca.pem").write_bytes(certificate)
    (datadir / "server-cert.pem").write_bytes(certificate)
    (datadir / "server-key.pem").write_bytes(private_key)


def _observation(datadir: Path, port: int = 33085) -> bootstrap.ServerObservation:
    return bootstrap.ServerObservation(
        version="8.4.11",
        version_comment="MySQL Community Server - GPL",
        server_uuid=SERVER_UUID,
        port=port,
        datadir=str(datadir),
        current_user="probiga_admin@127.0.0.1",
        require_secure_transport=True,
        tls_cipher="TLS_AES_256_GCM_SHA384",
        tls_version="TLSv1.3",
        admin_plugin="caching_sha2_password",
        admin_ssl_type="ANY",
        admin_account_locked=False,
        root_plugin="caching_sha2_password",
        root_account_locked=True,
        global_grant_verified=True,
        business_schemas=(),
    )


def test_generated_password_and_init_sql_are_high_entropy_and_complete():
    root_password = bootstrap._generate_password()
    admin_password = bootstrap._generate_password()
    sql = bootstrap._build_init_sql("probiga_admin", root_password, admin_password).decode()

    assert len(root_password) >= 60
    assert len(admin_password) >= 60
    assert root_password != admin_password
    assert "ALTER USER 'root'@'localhost' IDENTIFIED WITH caching_sha2_password" in sql
    assert "CREATE USER 'probiga_admin'@'127.0.0.1'" in sql
    assert "REQUIRE SSL" in sql
    assert "GRANT ALL PRIVILEGES ON *.*" in sql
    assert "WITH GRANT OPTION" in sql
    assert "ALTER USER 'root'@'localhost' ACCOUNT LOCK" in sql
    assert "skip-grant-tables" not in sql.casefold()


def test_first_start_command_is_local_tls_autogeneration_without_auth_bypass(tmp_path):
    identity = _identity(tmp_path)
    command = bootstrap.build_first_start_command(
        identity,
        datadir=tmp_path / "data",
        port=33085,
        init_file=tmp_path / "init.sql",
        error_log=tmp_path / "error.log",
        pid_file=tmp_path / "server.pid",
    )

    lowered = tuple(item.casefold() for item in command)
    assert command[1] == "--no-defaults"
    assert "--bind-address=127.0.0.1" in command
    assert "--skip-name-resolve" in command
    assert "--require-secure-transport=ON" in command
    assert "--auto-generate-certs=ON" in command
    assert "--shared-memory=OFF" in command
    assert "--named-pipe=OFF" in command
    assert not any(item.startswith("--skip-grant-tables") for item in lowered)
    assert not any(
        item.startswith(("--ssl-ca", "--ssl-cert", "--ssl-key")) for item in lowered
    )

    with pytest.raises(bootstrap.BootstrapError, match="non-3306"):
        bootstrap.build_first_start_command(
            identity,
            datadir=tmp_path / "data",
            port=3306,
            init_file=tmp_path / "init.sql",
            error_log=tmp_path / "error.log",
            pid_file=tmp_path / "server.pid",
        )


def test_initialize_command_is_insecure_only_during_new_datadir_initialization(tmp_path):
    identity = _identity(tmp_path)
    command = bootstrap.build_initialize_command(
        identity, datadir=tmp_path / "data", error_log=tmp_path / "initialize.err"
    )

    assert command[1:3] == ("--no-defaults", "--initialize-insecure")
    assert "--lower-case-table-names=1" in command
    assert not any("skip-grant-tables" in item.casefold() for item in command)


def test_mysqld_identity_probe_accepts_only_exact_oracle_8411(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    executable = tmp_path / "mysql-8.4.11" / "bin" / "mysqld.exe"
    executable.parent.mkdir(parents=True)
    executable.write_bytes(b"fake")
    calls = []

    class VersionProcess:
        returncode = 0

        def __init__(self, command, **kwargs):
            calls.append((tuple(command), kwargs))

        def communicate(self, timeout=None):
            return (
                b"mysqld  Ver 8.4.11 for Win64 on x86_64 "
                b"(MySQL Community Server - GPL)",
                b"",
            )

    monkeypatch.setattr(bootstrap.subprocess, "Popen", VersionProcess)
    identity = bootstrap.inspect_mysqld(executable)
    assert identity.basedir == str(executable.parent.parent.resolve())
    assert calls[0][0][1:] == ("--no-defaults", "--version")
    assert calls[0][1]["shell"] is False

    class ForkProcess(VersionProcess):
        def communicate(self, timeout=None):
            return (b"mysqld Ver 8.4.11-MariaDB (MariaDB Server)", b"")

    monkeypatch.setattr(bootstrap.subprocess, "Popen", ForkProcess)
    with pytest.raises(bootstrap.BootstrapError, match="exact Oracle MySQL 8.4.11"):
        bootstrap.inspect_mysqld(executable)


def test_formal_and_rehearsal_drive_policy_is_fail_closed():
    common = {
        "mysqld": Path(r"D:\MySQL84\software\bin\mysqld.exe"),
        "datadir": Path(r"E:\MySQL84\Data"),
        "cert_dir": Path(r"D:\MySQL84\certs"),
        "state_dir": Path(r"D:\MySQL84\bootstrap"),
    }
    bootstrap._validate_path_policy(
        deployment_mode="formal", allow_drive_f_for_rehearsal=False, **common
    )

    with pytest.raises(bootstrap.BootstrapError, match="must not use removable drive F"):
        bootstrap._validate_path_policy(
            deployment_mode="formal",
            allow_drive_f_for_rehearsal=False,
            **{**common, "datadir": Path(r"F:\MySQL84\Data")},
        )
    with pytest.raises(bootstrap.BootstrapError, match="must be on local drive D"):
        bootstrap._validate_path_policy(
            deployment_mode="formal",
            allow_drive_f_for_rehearsal=False,
            **{**common, "cert_dir": Path(r"E:\MySQL84\certs")},
        )
    with pytest.raises(bootstrap.BootstrapError, match="explicit allow flag"):
        bootstrap._validate_path_policy(
            deployment_mode="rehearsal",
            allow_drive_f_for_rehearsal=False,
            **{**common, "datadir": Path(r"F:\MySQL84\Data")},
        )
    bootstrap._validate_path_policy(
        deployment_mode="rehearsal",
        allow_drive_f_for_rehearsal=True,
        **{**common, "datadir": Path(r"F:\MySQL84\Data")},
    )


def test_fresh_datadir_gate_accepts_initialize_output_and_rejects_prior_start_or_schema(
    tmp_path: Path,
):
    datadir = tmp_path / "data"
    _fresh_layout(datadir)
    assert bootstrap.validate_fresh_datadir(datadir) == SERVER_UUID

    business = datadir / "probiga"
    business.mkdir()
    with pytest.raises(bootstrap.BootstrapError, match="possible business schemas"):
        bootstrap.validate_fresh_datadir(datadir)
    business.rmdir()

    (datadir / "ca.pem").write_bytes(b"certificate")
    with pytest.raises(bootstrap.BootstrapError, match="already completed"):
        bootstrap.validate_fresh_datadir(datadir)


class _VerificationCursor:
    def __init__(self, fixture):
        self.fixture = fixture
        self.key = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split()).casefold()
        if "select @@version as version" in normalized:
            self.key = "identity"
        elif "show session status" in normalized:
            self.key = "status"
        elif "from mysql.user" in normalized:
            self.key = "accounts"
        elif "show grants" in normalized:
            self.key = "grants"
        elif "from information_schema.schemata" in normalized:
            self.key = "schemas"
        elif normalized == "shutdown":
            self.key = "shutdown"
        else:
            raise AssertionError(sql)

    def fetchone(self):
        return self.fixture[self.key]

    def fetchall(self):
        return self.fixture[self.key]


class _VerificationConnection:
    def __init__(self, fixture):
        self.fixture = fixture
        self.closed = False

    def cursor(self):
        return _VerificationCursor(self.fixture)

    def close(self):
        self.closed = True


def _verification_fixture(datadir: Path) -> dict:
    return {
        "identity": {
            "version": "8.4.11",
            "version_comment": "MySQL Community Server - GPL",
            "server_uuid": SERVER_UUID,
            "port": 33085,
            "datadir": str(datadir),
            "require_secure_transport": 1,
            "current_user_name": "probiga_admin@127.0.0.1",
        },
        "status": [
            {"Variable_name": "Ssl_cipher", "Value": "TLS_AES_256_GCM_SHA384"},
            {"Variable_name": "Ssl_version", "Value": "TLSv1.3"},
        ],
        "accounts": [
            {
                "account_user": "probiga_admin",
                "account_host": "127.0.0.1",
                "plugin": "caching_sha2_password",
                "ssl_type": "ANY",
                "account_locked": "N",
            },
            {
                "account_user": "root",
                "account_host": "localhost",
                "plugin": "caching_sha2_password",
                "ssl_type": "",
                "account_locked": "Y",
            },
        ],
        "grants": [
            {
                "Grants for probiga_admin@127.0.0.1": (
                    "GRANT ALL PRIVILEGES ON *.* TO `probiga_admin`@`127.0.0.1` "
                    "WITH GRANT OPTION"
                )
            }
        ],
        "schemas": [],
        "shutdown": [],
    }


def test_verify_instance_proves_identity_tls_accounts_lock_and_emptiness(tmp_path: Path):
    datadir = tmp_path / "data"
    fixture = _verification_fixture(datadir)
    observation = bootstrap.verify_instance(
        _VerificationConnection(fixture),
        admin_user="probiga_admin",
        expected_port=33085,
        expected_datadir=datadir,
        expected_uuid=SERVER_UUID,
    )

    assert observation.server_uuid == SERVER_UUID
    assert observation.require_secure_transport
    assert observation.tls_version == "TLSv1.3"
    assert observation.admin_plugin == "caching_sha2_password"
    assert observation.admin_ssl_type == "ANY"
    assert observation.root_account_locked
    assert observation.global_grant_verified
    assert observation.business_schemas == ()


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (lambda f: f["identity"].update(port=33084), "port differs"),
        (lambda f: f["identity"].update(require_secure_transport=0), "not enabled"),
        (lambda f: f["status"][0].update(Value=""), "not using an accepted TLS"),
        (lambda f: f["accounts"][0].update(plugin="mysql_native_password"), "admin authentication"),
        (lambda f: f["accounts"][0].update(ssl_type=""), "does not REQUIRE SSL"),
        (lambda f: f["accounts"][1].update(account_locked="N"), "root@localhost is not locked"),
        (lambda f: f.update(schemas=[{"schema_name": "probiga"}]), "business schemas"),
    ),
)
def test_verify_instance_rejects_security_or_identity_drift(
    tmp_path: Path, mutation, message
):
    datadir = tmp_path / "data"
    fixture = _verification_fixture(datadir)
    mutation(fixture)
    with pytest.raises(bootstrap.BootstrapError, match=message):
        bootstrap.verify_instance(
            _VerificationConnection(fixture),
            admin_user="probiga_admin",
            expected_port=33085,
            expected_datadir=datadir,
            expected_uuid=SERVER_UUID,
        )


class _WaitedProcess:
    def __init__(self, command, **kwargs):
        self.command = tuple(command)
        self.kwargs = kwargs
        self.returncode = 0

    def wait(self, timeout=None):
        return 0


def test_mysql_process_launches_are_direct_and_capture_real_wait_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    identity = _identity(tmp_path)
    datadir = tmp_path / "data"
    cert_dir = tmp_path / "certs"
    state_dir = tmp_path / "state"
    datadir.mkdir()
    cert_dir.mkdir()
    state_dir.mkdir()
    paths = bootstrap._make_paths(datadir, cert_dir, state_dir)
    calls = []

    def fake_popen(command, **kwargs):
        process = _WaitedProcess(command, **kwargs)
        calls.append(process)
        return process

    monkeypatch.setattr(bootstrap.subprocess, "Popen", fake_popen)
    assert bootstrap._initialize_datadir(identity, paths) == 0
    server = bootstrap._start_server(identity, paths, 33085)

    assert server is calls[1]
    assert all(call.kwargs["shell"] is False for call in calls)
    assert all(call.kwargs["stdin"] is bootstrap.subprocess.DEVNULL for call in calls)
    assert all("MYSQL_PWD" not in call.kwargs["env"] for call in calls)
    assert not any(
        "skip-grant-tables" in argument.casefold()
        for call in calls
        for argument in call.command
    )


class _BootstrapProcess:
    def __init__(self):
        self.running = True
        self.terminated = False

    def poll(self):
        return None if self.running else 0

    def wait(self, timeout=None):
        self.running = False
        return 0

    def terminate(self):
        self.terminated = True
        self.running = False

    def kill(self):
        self.running = False


def _install_orchestration_mocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    fail_ready: bool = False,
):
    identity = _identity(tmp_path)
    process = _BootstrapProcess()
    connection = SimpleNamespace(close=lambda: None)
    monkeypatch.setattr(bootstrap, "_assert_port_free", lambda _port: None)
    monkeypatch.setattr(bootstrap, "inspect_mysqld", lambda _path: identity)
    monkeypatch.setattr(bootstrap, "_protect_directory", lambda _path: None)
    monkeypatch.setattr(bootstrap, "_protect_file", lambda _path: None)

    def initialize(_identity, paths):
        _fresh_layout(paths.datadir)
        paths.initialize_stdout.write_bytes(b"")
        paths.initialize_stderr.write_bytes(b"")
        paths.initialize_error.write_bytes(b"initialized")
        return 0

    def start(_identity, paths, _port):
        paths.server_stdout.write_bytes(b"")
        paths.server_stderr.write_bytes(b"")
        paths.server_error.write_bytes(b"ready")
        _write_generated_certificates(paths.datadir)
        return process

    monkeypatch.setattr(bootstrap, "_initialize_datadir", initialize)
    monkeypatch.setattr(bootstrap, "_start_server", start)
    if fail_ready:
        monkeypatch.setattr(
            bootstrap,
            "wait_until_ready",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                bootstrap.BootstrapError("mock ready failure")
            ),
        )
    else:
        monkeypatch.setattr(
            bootstrap, "wait_until_ready", lambda *args, **kwargs: connection
        )
    monkeypatch.setattr(
        bootstrap,
        "verify_instance",
        lambda _connection, **kwargs: _observation(kwargs["expected_datadir"]),
    )

    def shutdown(_connection, target_process, **kwargs):
        target_process.running = False
        return 0

    monkeypatch.setattr(bootstrap, "shutdown_server", shutdown)
    return identity, process


def test_complete_mock_bootstrap_retains_only_admin_option_and_secret_free_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    identity, process = _install_orchestration_mocks(tmp_path, monkeypatch)
    generated = iter(("R" * 64, "A" * 64))
    monkeypatch.setattr(bootstrap, "_generate_password", lambda: next(generated))
    datadir = tmp_path / "data"
    cert_dir = tmp_path / "certs"
    state_dir = tmp_path / "state"

    report = bootstrap.bootstrap_instance(
        deployment_mode="rehearsal",
        operation="initialize-and-first-start",
        mysqld_executable=Path(identity.executable),
        datadir=datadir,
        cert_dir=cert_dir,
        state_dir=state_dir,
        port=33085,
    )

    assert report["status"] == "success"
    assert report["server_observation"]["server_uuid"] == SERVER_UUID
    assert not (state_dir / ".mysql84-first-start-init.sql").exists()
    admin_options = state_dir / "mysql84-admin-client.ini"
    assert admin_options.is_file()
    option_text = admin_options.read_text(encoding="utf-8")
    assert "password=" + "A" * 64 in option_text
    assert "ssl-mode=VERIFY_CA" in option_text
    assert f"ssl-ca={(cert_dir / 'ca.pem').as_posix()}" in option_text
    for name in bootstrap.CERTIFICATE_FILES:
        assert (cert_dir / name).is_file()
    evidence_path = state_dir / "bootstrap-evidence.json"
    evidence_text = evidence_path.read_text(encoding="utf-8")
    assert "R" * 64 not in evidence_text
    assert "A" * 64 not in evidence_text
    assert "ALTER USER" not in evidence_text
    assert '"argv"' not in evidence_text
    assert report["security"]["skip_grant_tables_used"] is False
    assert process.running is False


def test_failure_still_deletes_init_file_and_stops_only_its_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    identity, process = _install_orchestration_mocks(
        tmp_path, monkeypatch, fail_ready=True
    )
    generated = iter(("R" * 64, "A" * 64))
    monkeypatch.setattr(bootstrap, "_generate_password", lambda: next(generated))
    state_dir = tmp_path / "state"

    with pytest.raises(bootstrap.BootstrapError, match="mock ready failure"):
        bootstrap.bootstrap_instance(
            deployment_mode="rehearsal",
            operation="initialize-and-first-start",
            mysqld_executable=Path(identity.executable),
            datadir=tmp_path / "data",
            cert_dir=tmp_path / "certs",
            state_dir=state_dir,
            port=33085,
        )

    assert process.terminated
    assert not (state_dir / ".mysql84-first-start-init.sql").exists()
    assert (state_dir / "mysql84-admin-client.ini").is_file()
    assert not (state_dir / "bootstrap-evidence.json").exists()


def test_first_start_only_requires_exact_attestation_before_port_or_files(
    tmp_path: Path,
):
    with pytest.raises(bootstrap.BootstrapError, match="exact fresh-datadir attestation"):
        bootstrap.bootstrap_instance(
            deployment_mode="rehearsal",
            operation="first-start-only",
            mysqld_executable=tmp_path / "missing.exe",
            datadir=tmp_path / "data",
            cert_dir=tmp_path / "certs",
            state_dir=tmp_path / "state",
            port=33085,
        )


def test_evidence_writer_rejects_password_argv_or_init_sql(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(bootstrap, "_protect_file", lambda _path: None)
    for payload in (
        {"secret": "S" * 64},
        {"argv": ["mysqld"]},
        {"detail": "ALTER USER root"},
    ):
        with pytest.raises(bootstrap.BootstrapError, match="forbidden"):
            bootstrap._write_evidence(
                tmp_path / f"evidence-{len(list(tmp_path.iterdir()))}.json",
                payload,
                ("S" * 64,),
            )


def test_parser_exposes_both_safe_workflows():
    help_text = bootstrap._parser().format_help()
    assert "initialize-and-first-start" in help_text
    assert "first-start-only" in help_text
    assert "--preinitialized-attestation" in help_text
    assert "--allow-drive-f-for-rehearsal" in help_text
