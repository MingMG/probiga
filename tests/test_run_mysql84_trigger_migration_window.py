from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Any

import pytest

from tools import run_mysql84_trigger_migration_window as window
from tools.mysql_acceptance_tls import MySQLAcceptanceTLSConfig


TEST_UUID = "810354d6-9061-11f1-84ae-74d4dd7f8500"


class FakeServer:
    def __init__(self) -> None:
        self.version = "8.4.11"
        self.version_comment = "MySQL Community Server - GPL"
        self.server_uuid = TEST_UUID
        self.port = 33084
        self.ssl_cipher = "TLS_AES_256_GCM_SHA384"
        self.log_bin = 1
        self.binlog_format = "ROW"
        self.trust = 0
        self.lock_available = True
        self.lock_held = False
        self.fail_set_off = False
        self.connect_count = 0
        self.statements: list[tuple[str, object | None]] = []

    def connect(self, _config: window.ValidatedWindowConfig) -> "FakeConnection":
        self.connect_count += 1
        return FakeConnection(self)


class FakeCursor:
    def __init__(self, server: FakeServer) -> None:
        self.server = server
        self.row: tuple[object, ...] | None = None

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, statement: str, params: object | None = None) -> None:
        normalized = " ".join(statement.split())
        self.server.statements.append((normalized, params))
        if normalized.startswith("SELECT @@version"):
            self.row = (
                self.server.version,
                self.server.version_comment,
                self.server.server_uuid,
                self.server.port,
                self.server.log_bin,
                self.server.binlog_format,
                self.server.trust,
            )
        elif normalized == "SHOW SESSION STATUS LIKE 'Ssl_cipher'":
            self.row = ("Ssl_cipher", self.server.ssl_cipher)
        elif normalized.startswith("SELECT GET_LOCK"):
            acquired = self.server.lock_available and not self.server.lock_held
            self.server.lock_held = acquired
            self.row = (1 if acquired else 0,)
        elif normalized.startswith("SELECT RELEASE_LOCK"):
            released = self.server.lock_held
            self.server.lock_held = False
            self.row = (1 if released else None,)
        elif normalized.endswith("= ON"):
            self.server.trust = 1
            self.row = None
        elif normalized.endswith("= OFF"):
            if not self.server.fail_set_off:
                self.server.trust = 0
            self.row = None
        else:  # pragma: no cover - makes new SQL fail loudly
            raise AssertionError(f"unexpected SQL: {normalized}")

    def fetchone(self) -> tuple[object, ...] | None:
        return self.row


class FakeConnection:
    def __init__(self, server: FakeServer) -> None:
        self.server = server
        self.closed = False

    def cursor(self) -> FakeCursor:
        if self.closed:
            raise RuntimeError("connection is closed")
        return FakeCursor(self.server)

    def close(self) -> None:
        self.closed = True


class FakeProcess:
    def __init__(self, return_code: int = 0, *, interrupt: bool = False) -> None:
        self.return_code = return_code
        self.interrupt = interrupt
        self.wait_calls = 0
        self.terminated = False
        self.killed = False

    def wait(self, timeout: int | None = None) -> int:
        self.wait_calls += 1
        if self.interrupt and self.wait_calls == 1 and timeout is None:
            raise KeyboardInterrupt
        return self.return_code

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True


@pytest.fixture
def config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> window.WindowConfig:
    option_file = (tmp_path / "admin-client.ini").resolve()
    option_file.write_text(
        "[client]\nuser=upgrade_admin\npassword=not-read-by-tests\n",
        encoding="utf-8",
    )
    ca_file = (tmp_path / "target-ca.pem").resolve()
    ca_file.write_text("mock CA", encoding="ascii")
    monkeypatch.setattr(
        window,
        "require_mysql_acceptance_ssl_ca",
        lambda value: MySQLAcceptanceTLSConfig(str(Path(value).resolve())),
    )
    return window.WindowConfig(
        admin_option_file=option_file,
        target_ssl_ca=ca_file,
        expected_server_uuid=TEST_UUID,
        expected_server_port=33084,
        business_offline_ack=window.OFFLINE_ACK,
        change_id="MYSQL84-TEST-001",
    )


def _run(
    config: window.WindowConfig,
    server: FakeServer,
    process: FakeProcess | None = None,
    **kwargs: Any,
) -> tuple[window.WindowOutcome, dict[str, object]]:
    observed: dict[str, object] = {}
    child = process or FakeProcess()

    def popen(command: list[str], **options: object) -> FakeProcess:
        observed["command"] = command
        observed["options"] = options
        return child

    child_environ = kwargs.pop(
        "environ", {"V4_TEST_MYSQL_URL": "set-outside-evidence"}
    )
    outcome = window.execute_window(
        config,
        [sys.executable, "tools/fake_migration.py", "--scenario", "serial"],
        environ=child_environ,
        connect_factory=server.connect,
        popen_factory=popen,
        **kwargs,
    )
    return outcome, observed


def test_success_uses_server_lock_temporarily_enables_and_double_verifies_off(
    config: window.WindowConfig,
) -> None:
    server = FakeServer()
    outcome, observed = _run(config, server)

    assert outcome.exit_code == 0
    assert outcome.evidence["outcome"] == "success"
    assert outcome.evidence["production_activation_allowed"] is False
    assert outcome.evidence["business_offline_acknowledged"] is True
    assert outcome.evidence["named_lock_acquired"] is True
    assert outcome.evidence["named_lock_released"] is True
    assert outcome.evidence["trust_transition"] == {
        "enable_attempted": True,
        "enabled_verified": True,
        "restore_attempted": True,
        "restore_primary_verified": True,
        "restore_secondary_verified": True,
    }
    assert outcome.evidence["target"]["server_uuid"] == TEST_UUID
    assert outcome.evidence["target"]["log_bin"] == 1
    assert outcome.evidence["target"]["binlog_format"] == "ROW"
    assert server.trust == 0
    assert server.connect_count == 2  # primary plus fresh secondary verifier
    assert observed["command"][0] == sys.executable
    assert observed["options"]["shell"] is False
    assert observed["options"]["close_fds"] is True
    assert (
        observed["options"]["env"][window.NESTED_WINDOW_ENV] == "1"
    )
    assert "V4_TEST_MYSQL_URL" not in json.dumps(outcome.evidence)
    assert str(config.admin_option_file) not in json.dumps(outcome.evidence)
    assert "fake_migration.py" not in json.dumps(outcome.evidence)

    statements = [statement for statement, _params in server.statements]
    assert "SET GLOBAL log_bin_trust_function_creators = ON" in statements
    assert "SET GLOBAL log_bin_trust_function_creators = OFF" in statements
    assert statements.index(
        "SET GLOBAL log_bin_trust_function_creators = ON"
    ) < statements.index("SET GLOBAL log_bin_trust_function_creators = OFF")


def test_nonzero_child_exit_is_returned_exactly_after_safe_restore(
    config: window.WindowConfig,
) -> None:
    server = FakeServer()
    outcome, _ = _run(config, server, FakeProcess(37))
    assert outcome.exit_code == 37
    assert outcome.evidence["outcome"] == "child_failed"
    assert outcome.evidence["child"]["return_code"] == 37
    assert server.trust == 0


def test_keyboard_interrupt_terminates_child_and_restores_off(
    config: window.WindowConfig,
) -> None:
    server = FakeServer()
    process = FakeProcess(-15, interrupt=True)
    outcome, _ = _run(config, server, process)
    assert outcome.exit_code == window.INTERRUPTED_EXIT_CODE
    assert outcome.evidence["outcome"] == "interrupted"
    assert outcome.evidence["child"]["interrupted"] is True
    assert process.terminated is True
    assert server.trust == 0


def test_child_launch_exception_still_restores_off(
    config: window.WindowConfig,
) -> None:
    server = FakeServer()

    def broken_popen(*_args: object, **_kwargs: object) -> FakeProcess:
        raise OSError(2, "secret-bearing executable text must not be copied")

    outcome = window.execute_window(
        config,
        [sys.executable, "migration.py"],
        environ={},
        connect_factory=server.connect,
        popen_factory=broken_popen,
    )
    assert outcome.exit_code == window.PREFLIGHT_FAILURE_EXIT_CODE
    assert outcome.evidence["outcome"] == "child_launch_failed"
    assert outcome.evidence["failure"]["code"] == "os_operation_failed"
    assert "secret-bearing" not in json.dumps(outcome.evidence)
    assert server.trust == 0


def test_child_wait_exception_terminates_child_before_restoring_off(
    config: window.WindowConfig,
) -> None:
    server = FakeServer()

    class BrokenWaitProcess(FakeProcess):
        def wait(self, timeout: int | None = None) -> int:
            self.wait_calls += 1
            if self.wait_calls == 1 and timeout is None:
                raise RuntimeError("untrusted child error text")
            return -15

    process = BrokenWaitProcess()
    outcome, _ = _run(config, server, process)
    assert outcome.exit_code == window.PREFLIGHT_FAILURE_EXIT_CODE
    assert process.terminated is True
    assert server.trust == 0
    assert "untrusted child" not in json.dumps(outcome.evidence)


def test_restore_failure_overrides_child_exit_with_safety_failure(
    config: window.WindowConfig,
) -> None:
    server = FakeServer()
    server.fail_set_off = True
    outcome, _ = _run(config, server, FakeProcess(23))
    assert outcome.exit_code == window.SAFETY_FAILURE_EXIT_CODE
    assert outcome.evidence["outcome"] == "restoration_failed"
    assert outcome.evidence["child"]["return_code"] == 23
    assert outcome.evidence["failure"]["code"] == (
        "trust_restore_verification_failed"
    )
    assert server.trust == 1


def test_initial_on_is_rejected_without_mutating_a_possibly_active_window(
    config: window.WindowConfig,
) -> None:
    server = FakeServer()
    server.trust = 1
    called = False

    def popen(*_args: object, **_kwargs: object) -> FakeProcess:
        nonlocal called
        called = True
        return FakeProcess()

    outcome = window.execute_window(
        config,
        [sys.executable, "migration.py"],
        environ={},
        connect_factory=server.connect,
        popen_factory=popen,
    )
    assert outcome.exit_code == window.PREFLIGHT_FAILURE_EXIT_CODE
    assert outcome.evidence["failure"]["code"] == "trust_initially_enabled"
    assert called is False
    assert server.trust == 1
    assert not any(
        statement.startswith("SET GLOBAL")
        for statement, _params in server.statements
    )


@pytest.mark.parametrize(
    ("attribute", "value", "failure_code"),
    (
        ("version", "8.4.10", "target_version_mismatch"),
        ("version_comment", "Percona Server", "target_version_mismatch"),
        ("server_uuid", "910354d6-9061-11f1-84ae-74d4dd7f8500", "target_uuid_mismatch"),
        ("port", 33085, "target_port_mismatch"),
        ("ssl_cipher", "", "target_tls_missing"),
        ("log_bin", 0, "binary_log_disabled"),
        ("binlog_format", "STATEMENT", "binary_log_format_mismatch"),
    ),
)
def test_target_preconditions_fail_closed_before_global_mutation(
    config: window.WindowConfig,
    attribute: str,
    value: object,
    failure_code: str,
) -> None:
    server = FakeServer()
    setattr(server, attribute, value)
    outcome, _ = _run(config, server)
    assert outcome.exit_code == window.PREFLIGHT_FAILURE_EXIT_CODE
    assert outcome.evidence["failure"]["code"] == failure_code
    assert server.trust == 0
    assert not any(
        statement.startswith("SET GLOBAL")
        for statement, _params in server.statements
    )


def test_server_lock_rejects_concurrent_window_before_global_mutation(
    config: window.WindowConfig,
) -> None:
    server = FakeServer()
    server.lock_available = False
    outcome, _ = _run(config, server)
    assert outcome.evidence["failure"]["code"] == "window_lock_unavailable"
    assert server.trust == 0
    assert outcome.evidence["named_lock_acquired"] is False


def test_nested_window_offline_and_production_guards_precede_connection(
    config: window.WindowConfig,
) -> None:
    server = FakeServer()
    nested, _ = _run(
        config,
        server,
        environ={window.NESTED_WINDOW_ENV: "1"},
    )
    assert nested.evidence["failure"]["code"] == "nested_window_rejected"

    no_ack = window.WindowConfig(
        **{
            **{
                field: getattr(config, field)
                for field in config.__dataclass_fields__
            },
            "business_offline_ack": "business is probably quiet",
        }
    )
    outcome, _ = _run(no_ack, server)
    assert outcome.evidence["failure"]["code"] == (
        "business_offline_not_acknowledged"
    )

    activation = window.WindowConfig(
        **{
            **{
                field: getattr(config, field)
                for field in config.__dataclass_fields__
            },
            "production_activation_requested": True,
        }
    )
    outcome, _ = _run(activation, server)
    assert outcome.evidence["failure"]["code"] == "production_activation_rejected"
    assert server.connect_count == 0


@pytest.mark.parametrize(
    "command",
    (
        (sys.executable, "migration.py", "--password=hunter2"),
        (sys.executable, "migration.py", "-phunter2"),
        (sys.executable, "mysql://user:secret@127.0.0.1/db"),
        (sys.executable, "migration.py", "--production-activation"),
    ),
)
def test_secret_and_production_arguments_are_rejected_before_connection(
    config: window.WindowConfig, command: tuple[str, ...]
) -> None:
    server = FakeServer()
    outcome = window.execute_window(
        config,
        command,
        environ={},
        connect_factory=server.connect,
        popen_factory=lambda *_args, **_kwargs: FakeProcess(),
    )
    assert outcome.exit_code == window.PREFLIGHT_FAILURE_EXIT_CODE
    assert server.connect_count == 0


def test_admin_option_file_cannot_be_forwarded_to_child(
    config: window.WindowConfig,
) -> None:
    server = FakeServer()
    outcome = window.execute_window(
        config,
        [
            sys.executable,
            "migration.py",
            f"--defaults-extra-file={config.admin_option_file}",
        ],
        environ={},
        connect_factory=server.connect,
        popen_factory=lambda *_args, **_kwargs: FakeProcess(),
    )
    assert outcome.evidence["failure"]["code"] == (
        "admin_credential_forwarding_rejected"
    )
    assert server.connect_count == 0


def test_invalid_change_id_is_not_copied_into_failure_evidence(
    config: window.WindowConfig,
) -> None:
    secret_like_value = "bad change id password=hunter2"
    invalid = window.WindowConfig(
        **{
            **{
                field: getattr(config, field)
                for field in config.__dataclass_fields__
            },
            "change_id": secret_like_value,
        }
    )
    outcome = window.execute_window(
        invalid,
        [sys.executable, "migration.py"],
        environ={},
        connect_factory=FakeServer().connect,
        popen_factory=lambda *_args, **_kwargs: FakeProcess(),
    )
    assert outcome.evidence["change_id"] is None
    assert secret_like_value not in json.dumps(outcome.evidence)


def test_connect_uses_only_option_file_credentials_and_mandatory_tls(
    config: window.WindowConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    validated = window.validate_config(config)
    observed: dict[str, object] = {}
    connection = object()

    def fake_connect(**kwargs: object) -> object:
        observed.update(kwargs)
        return connection

    monkeypatch.setattr(window.pymysql, "connect", fake_connect)
    assert window._connect(validated) is connection
    assert observed["read_default_file"] == str(config.admin_option_file)
    assert observed["read_default_group"] == "client"
    assert observed["ssl_ca"] == str(config.target_ssl_ca)
    assert observed["ssl_verify_cert"] is True
    assert observed["ssl_verify_identity"] is False
    assert observed["local_infile"] is False
    assert "user" not in observed
    assert "password" not in observed


def test_atomic_evidence_write_is_valid_and_refuses_silent_overwrite(
    tmp_path: Path,
) -> None:
    output = (tmp_path / "evidence" / "window.json").resolve()
    evidence = {"outcome": "success", "production_activation_allowed": False}
    assert window.atomic_write_evidence(output, evidence) == output
    assert json.loads(output.read_text(encoding="utf-8")) == evidence
    assert not list(output.parent.glob("*.partial-*"))
    with pytest.raises(FileExistsError):
        window.atomic_write_evidence(output, {"outcome": "replacement"})
    assert json.loads(output.read_text(encoding="utf-8")) == evidence
    window.atomic_write_evidence(
        output, {"outcome": "replacement"}, overwrite=True
    )
    assert json.loads(output.read_text(encoding="utf-8"))["outcome"] == (
        "replacement"
    )


def test_cli_requires_literal_delimiter_and_preserves_child_arguments() -> None:
    with pytest.raises(SystemExit):
        window.parse_cli(["--change-id", "X"])
    args, command = window.parse_cli(
        [
            "--admin-option-file",
            "C:/admin.ini",
            "--target-ssl-ca",
            "C:/ca.pem",
            "--expected-server-uuid",
            TEST_UUID,
            "--expected-server-port",
            "33084",
            "--business-offline-ack",
            window.OFFLINE_ACK,
            "--change-id",
            "CHANGE-1",
            "--evidence",
            "C:/window.json",
            "--",
            sys.executable,
            "migration.py",
            "--",
            "child-positional",
        ]
    )
    assert args.expected_server_port == 33084
    assert command[-2:] == ("--", "child-positional")
