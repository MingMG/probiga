from __future__ import annotations

from pathlib import Path

import pytest

from tools import materialize_mysql84_check_constraints as cli
from tools import mysql_acceptance_tls as tls


TARGET_UUID = "810354d6-9061-11f1-84ae-74d4dd7f8500"
TARGET_PORT = 33084
TARGET_SCHEMA = "probiga"
VALID_URL = (
    "mysql+pymysql://migration:secret@127.0.0.1:33084/probiga"
)


@pytest.fixture
def ca_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "migration-ca.pem"
    path.write_text("unit-test-ca", encoding="ascii")
    monkeypatch.setattr(
        tls.ssl,
        "create_default_context",
        lambda **_kwargs: object(),
    )
    return path.resolve()


def _argv(*extra: str) -> list[str]:
    return [
        "--schema",
        TARGET_SCHEMA,
        "--expected-server-uuid",
        TARGET_UUID,
        "--expected-server-port",
        str(TARGET_PORT),
        *extra,
    ]


def test_migration_url_requires_explicit_driver_schema_and_port() -> None:
    assert (
        cli.require_mysql84_migration_url(
            VALID_URL,
            expected_schema=TARGET_SCHEMA,
            expected_server_port=TARGET_PORT,
        )
        == VALID_URL
    )


@pytest.mark.parametrize(
    ("url", "message"),
    (
        (
            "mysql://migration:secret@127.0.0.1:33084/probiga",
            r"mysql\+pymysql",
        ),
        (
            "mysql+mysqldb://migration:secret@127.0.0.1:33084/probiga",
            r"mysql\+pymysql",
        ),
        (
            "postgresql+pymysql://migration:secret@127.0.0.1:33084/probiga",
            r"mysql\+pymysql",
        ),
        (
            VALID_URL + "?ssl_ca=C%3A%5Cwrong.pem",
            "must not contain URL query",
        ),
        (
            VALID_URL + "?charset=utf8mb4",
            "must not contain URL query",
        ),
        (
            "mysql+pymysql://migration:secret@127.0.0.1/probiga",
            "explicit TCP port",
        ),
        (
            "mysql+pymysql://migration:secret@127.0.0.1:3306/probiga",
            "does not match --expected-server-port",
        ),
        (
            "mysql+pymysql://migration:secret@127.0.0.1:33084/biga",
            "does not match --schema",
        ),
        (
            "mysql+pymysql://migration:secret@:33084/probiga",
            "explicit host",
        ),
    ),
)
def test_migration_url_rejects_driver_query_and_target_drift(
    url: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        cli.require_mysql84_migration_url(
            url,
            expected_schema=TARGET_SCHEMA,
            expected_server_port=TARGET_PORT,
        )


def test_migration_ca_accepts_an_explicit_absolute_path(
    ca_file: Path,
) -> None:
    assert cli.resolve_mysql84_migration_tls_config(
        ssl_ca=str(ca_file),
        ssl_ca_env=None,
        environ={},
    ) == tls.MySQLAcceptanceTLSConfig(ssl_ca=str(ca_file))


def test_migration_ca_uses_only_the_dedicated_environment(
    ca_file: Path,
) -> None:
    assert cli.resolve_mysql84_migration_tls_config(
        ssl_ca=None,
        ssl_ca_env=None,
        environ={cli.MIGRATION_SSL_CA_ENV: str(ca_file)},
    ) == tls.MySQLAcceptanceTLSConfig(ssl_ca=str(ca_file))

    with pytest.raises(ValueError, match="SSL CA file is required"):
        cli.resolve_mysql84_migration_tls_config(
            ssl_ca=None,
            ssl_ca_env=None,
            environ={"MYSQL_SSL_CA": str(ca_file)},
        )
    with pytest.raises(ValueError, match="must name exactly"):
        cli.resolve_mysql84_migration_tls_config(
            ssl_ca=None,
            ssl_ca_env="V4_TEST_MYSQL_SSL_CA",
            environ={"V4_TEST_MYSQL_SSL_CA": str(ca_file)},
        )


def test_migration_ca_rejects_relative_path() -> None:
    with pytest.raises(ValueError, match="must be absolute"):
        cli.resolve_mysql84_migration_tls_config(
            ssl_ca="relative-ca.pem",
            ssl_ca_env=None,
            environ={},
        )


class _ConnectionContext:
    def __init__(self) -> None:
        self.connection = object()

    def __enter__(self) -> object:
        return self.connection

    def __exit__(self, *_args: object) -> None:
        return None


class _Engine:
    def __init__(self) -> None:
        self.context = _ConnectionContext()
        self.disposed = False

    def connect(self) -> _ConnectionContext:
        return self.context

    def dispose(self) -> None:
        self.disposed = True


class _Report:
    complete = True
    applicable_constraint_count = 3
    schema = TARGET_SCHEMA
    added_not_enforced = ("ck_one",)
    enforced_constraints = ("ck_one",)
    violation_counts: tuple[tuple[str, int], ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {"schema": self.schema}


def test_formal_cli_forwards_verified_tls_and_all_existing_safety_gates(
    ca_file: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    engine = _Engine()
    observed: dict[str, object] = {}
    monkeypatch.setattr(cli, "load_project_env", lambda: None)
    monkeypatch.setenv(cli.MIGRATION_URL_ENV, VALID_URL)

    def fake_create_engine(url: str, **kwargs: object) -> _Engine:
        observed["engine"] = (url, kwargs)
        return engine

    def fake_materialize(connection: object, **kwargs: object) -> _Report:
        observed["materialize"] = (connection, kwargs)
        return _Report()

    monkeypatch.setattr(cli, "create_mysql_acceptance_engine", fake_create_engine)
    monkeypatch.setattr(
        cli,
        "materialize_mysql84_check_constraints",
        fake_materialize,
    )

    assert cli.main(
        _argv(
            "--ssl-ca",
            str(ca_file),
            "--apply",
            "--confirm-restored-target-offline",
            "--json",
        )
    ) == 0

    url, engine_kwargs = observed["engine"]
    assert url == VALID_URL
    assert engine_kwargs == {
        "tls_config": tls.MySQLAcceptanceTLSConfig(ssl_ca=str(ca_file)),
        "pool_pre_ping": True,
        "pool_recycle": 900,
        "future": True,
    }
    connection, materialize_kwargs = observed["materialize"]
    assert connection is engine.context.connection
    assert materialize_kwargs == {
        "expected_schema": TARGET_SCHEMA,
        "expected_server_uuid": TARGET_UUID,
        "expected_server_port": TARGET_PORT,
        "apply": True,
        "restored_target_offline": True,
    }
    assert engine.disposed is True
    assert '"status": "ok"' in capsys.readouterr().out


@pytest.mark.parametrize(
    "url",
    (
        VALID_URL + "?ssl_disabled=true",
        "mysql://migration:secret@127.0.0.1:33084/probiga",
        "mysql+pymysql://migration:secret@127.0.0.1:3306/probiga",
    ),
)
def test_formal_cli_rejects_unsafe_url_before_engine_creation(
    url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli, "load_project_env", lambda: None)
    monkeypatch.setenv(cli.MIGRATION_URL_ENV, url)
    monkeypatch.setattr(
        cli,
        "create_mysql_acceptance_engine",
        lambda *_args, **_kwargs: pytest.fail("engine must not be created"),
    )

    with pytest.raises(SystemExit):
        cli.main(_argv())


def test_formal_cli_requires_ca_before_engine_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli, "load_project_env", lambda: None)
    monkeypatch.setenv(cli.MIGRATION_URL_ENV, VALID_URL)
    monkeypatch.delenv(cli.MIGRATION_SSL_CA_ENV, raising=False)
    monkeypatch.setattr(
        cli,
        "create_mysql_acceptance_engine",
        lambda *_args, **_kwargs: pytest.fail("engine must not be created"),
    )

    with pytest.raises(SystemExit, match="SSL CA file is required"):
        cli.main(_argv())
