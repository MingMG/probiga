from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest

from tools import mysql_acceptance_tls as tls
from tools import trading_v2_evidence_mysql_acceptance as v2
from tools import trading_v2_evidence_mysql_recovery_acceptance as v2_recovery
from tools import trading_v3_mysql_acceptance as v3
from tools import trading_v4_mysql_acceptance as v4


@pytest.fixture
def ca_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "acceptance-ca.pem"
    path.write_text("unit-test-ca", encoding="ascii")
    monkeypatch.setattr(tls.ssl, "create_default_context", lambda **_kwargs: object())
    return path.resolve()


@pytest.mark.parametrize(
    ("scope", "env_name"),
    (
        ("V2_EVIDENCE", "V2_EVIDENCE_TEST_MYSQL_SSL_CA"),
        ("V2_EVIDENCE", "V2_EVIDENCE_CI_JOB_42_MYSQL_SSL_CA"),
        ("V2_EVIDENCE", "V2_EVIDENCE_TEST_RECOVERY_MYSQL_SSL_CA"),
        ("V3", "V3_TEST_MYSQL_SSL_CA"),
        ("V3", "V3_CI_SERIAL_MYSQL_SSL_CA"),
        ("V4", "V4_TEST_MYSQL_SSL_CA"),
        ("V4", "V4_CI_ACCEPTANCE_MYSQL_SSL_CA"),
    ),
)
def test_tls_ca_resolver_accepts_only_scoped_test_ci_names(
    ca_file: Path,
    scope: str,
    env_name: str,
) -> None:
    config = tls.resolve_mysql_acceptance_tls_config(
        scope,
        env_name,
        environ={env_name: str(ca_file)},
    )
    assert config == tls.MySQLAcceptanceTLSConfig(ssl_ca=str(ca_file))


@pytest.mark.parametrize(
    ("scope", "env_name"),
    (
        ("V4", "MYSQL_SSL_CA"),
        ("V4", "V4_PROD_MYSQL_SSL_CA"),
        ("V4", "V4_TEST_MYSQL_URL"),
        ("V4", "V3_TEST_MYSQL_SSL_CA"),
        ("V3", "V3_test_MYSQL_SSL_CA"),
        ("V2_EVIDENCE", "V2_TEST_MYSQL_SSL_CA"),
    ),
)
def test_tls_ca_resolver_rejects_generic_wrong_scope_and_non_test_names(
    ca_file: Path,
    scope: str,
    env_name: str,
) -> None:
    with pytest.raises(ValueError, match="dedicated TEST/CI"):
        tls.resolve_mysql_acceptance_tls_config(
            scope,
            env_name,
            environ={env_name: str(ca_file)},
        )


def test_tls_ca_resolver_never_falls_back_to_generic_environment(
    ca_file: Path,
) -> None:
    with pytest.raises(ValueError, match="SSL CA file is required"):
        tls.resolve_mysql_acceptance_tls_config(
            "V4",
            "V4_TEST_MYSQL_SSL_CA",
            environ={"MYSQL_SSL_CA": str(ca_file)},
        )


@pytest.mark.parametrize("value", ("", "relative-ca.pem"))
def test_tls_ca_path_fails_closed_before_engine_creation(value: str) -> None:
    with pytest.raises(ValueError):
        tls.require_mysql_acceptance_ssl_ca(value)


def test_tls_engine_injects_one_fixed_pymysql_policy_and_connect_guard(
    ca_file: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}
    engine = object()

    def fake_create(url: str, **kwargs: object) -> object:
        observed["url"] = url
        observed["kwargs"] = kwargs
        return engine

    def fake_listen(target: object, name: str, callback: object) -> None:
        observed["listener"] = (target, name, callback)

    monkeypatch.setattr(tls, "create_tool_engine", fake_create)
    monkeypatch.setattr(tls.event, "listen", fake_listen)
    url = "mysql+pymysql://acceptance:secret@127.0.0.1:33084/db_v4_test"
    config = tls.MySQLAcceptanceTLSConfig(str(ca_file))

    assert (
        tls.create_mysql_acceptance_engine(
            url,
            tls_config=config,
            future=True,
            pool_size=2,
            max_overflow=0,
        )
        is engine
    )
    assert observed["url"] == url
    assert observed["kwargs"] == {
        "connect_args": {
            "ssl_ca": str(ca_file),
            "ssl_verify_cert": True,
        },
        "future": True,
        "pool_size": 2,
        "max_overflow": 0,
    }
    assert observed["listener"] == (
        engine,
        "connect",
        tls._require_negotiated_tls,
    )


@pytest.mark.parametrize("forbidden", ("connect_args", "creator"))
def test_tls_engine_rejects_connection_policy_overrides(
    ca_file: Path,
    forbidden: str,
) -> None:
    with pytest.raises(TypeError, match="may not override"):
        tls.create_mysql_acceptance_engine(
            "mysql+pymysql://u:p@127.0.0.1/db_v4_test",
            tls_config=tls.MySQLAcceptanceTLSConfig(str(ca_file)),
            **{forbidden: object()},
        )


def test_tls_engine_rejects_implicit_or_different_mysql_driver(
    ca_file: Path,
) -> None:
    with pytest.raises(ValueError, match=r"mysql\+pymysql"):
        tls.create_mysql_acceptance_engine(
            "mysql://u:p@127.0.0.1/db_v4_test",
            tls_config=tls.MySQLAcceptanceTLSConfig(str(ca_file)),
        )


def test_explicit_none_preserves_programmatic_non_tls_unit_test_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}
    engine = object()

    def fake_create(url: str, **kwargs: object) -> object:
        observed.update(url=url, kwargs=kwargs)
        return engine

    monkeypatch.setattr(tls, "create_tool_engine", fake_create)
    assert (
        tls.create_mysql_acceptance_engine(
            "mysql+pymysql://u:p@127.0.0.1/db_v4_test",
            tls_config=None,
            future=True,
        )
        is engine
    )
    assert observed["kwargs"] == {"future": True}


class _CipherCursor:
    def __init__(self, cipher: object) -> None:
        self.cipher = cipher
        self.executed = ""
        self.closed = False

    def execute(self, statement: str) -> None:
        self.executed = statement

    def fetchone(self) -> tuple[str, object]:
        return ("Ssl_cipher", self.cipher)

    def close(self) -> None:
        self.closed = True


class _CipherConnection:
    def __init__(self, cipher: object) -> None:
        self.cursor_instance = _CipherCursor(cipher)
        self.closed = False

    def cursor(self) -> _CipherCursor:
        return self.cursor_instance

    def close(self) -> None:
        self.closed = True


def test_connect_guard_requires_an_observed_tls_cipher() -> None:
    secure = _CipherConnection("TLS_AES_128_GCM_SHA256")
    tls._require_negotiated_tls(secure, object())
    assert secure.cursor_instance.executed == (
        "SHOW SESSION STATUS LIKE 'Ssl_cipher'"
    )
    assert secure.cursor_instance.closed is True
    assert secure.closed is False

    plaintext = _CipherConnection("")
    with pytest.raises(RuntimeError, match="no TLS cipher"):
        tls._require_negotiated_tls(plaintext, object())
    assert plaintext.cursor_instance.closed is True
    assert plaintext.closed is True


_RUNNERS = {
    v2: (
        "run_mysql_serial_replay_acceptance",
        "run_mysql_concurrent_initial_acceptance",
        "run_mysql_behavioral_acceptance",
    ),
    v2_recovery: ("run_mysql_recovery_acceptance",),
    v3: ("run_mysql_acceptance",),
    v4: (
        "run_mysql_acceptance",
        "run_mysql_concurrent_initial_acceptance",
        "run_mysql_partial_recovery_acceptance",
        "run_mysql_head_cas_acceptance",
        "run_mysql_transaction_recovery_acceptance",
        "run_mysql_job_lease_behavior_acceptance",
    ),
}


def test_every_formal_runner_has_one_explicit_tls_configuration_parameter() -> None:
    for module, names in _RUNNERS.items():
        for name in names:
            parameter = inspect.signature(getattr(module, name)).parameters[
                "tls_config"
            ]
            assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
            assert parameter.default is None


def test_every_acceptance_engine_call_forwards_the_same_tls_configuration() -> None:
    for module in _RUNNERS:
        source = Path(module.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "create_tool_engine"
        ]
        assert calls
        for call in calls:
            keywords = {item.arg: item.value for item in call.keywords}
            assert "tls_config" in keywords
            assert isinstance(keywords["tls_config"], ast.Name)
            assert keywords["tls_config"].id == "tls_config"


@pytest.mark.parametrize(
    ("module", "expected"),
    (
        (v2, "V2_EVIDENCE_TEST_MYSQL_SSL_CA"),
        (v2_recovery, "V2_EVIDENCE_TEST_RECOVERY_MYSQL_SSL_CA"),
        (v3, "V3_TEST_MYSQL_SSL_CA"),
        (v4, "V4_TEST_MYSQL_SSL_CA"),
    ),
)
def test_formal_cli_has_a_dedicated_default_ssl_ca_environment(
    module: object,
    expected: str,
) -> None:
    argv = ["--scenario", "011-ddl-prefix"] if module is v2_recovery else []
    parsed = module._parser().parse_args(argv)
    assert parsed.ssl_ca_env == expected
