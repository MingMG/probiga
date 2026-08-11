from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from server.common import engine_factory


class _Cursor:
    def __init__(self, row):
        self.row = row
        self.executed: list[str] = []
        self.closed = False

    def execute(self, statement: str) -> None:
        self.executed.append(statement)

    def fetchone(self):
        return self.row

    def close(self) -> None:
        self.closed = True


class _Connection:
    def __init__(self, row):
        self._cursor = _Cursor(row)
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self) -> None:
        self.closed = True


def _tls_config(ca_file: Path) -> dict[str, str | bool]:
    return {"required": True, "ssl_ca": str(ca_file)}


def test_non_mysql_engine_does_not_consult_mysql_tls_policy():
    expected = object()
    with patch(
        "server.common.engine_factory._get_runtime_tls_config",
        side_effect=AssertionError("SQLite must not consult MySQL TLS settings"),
    ), patch(
        "server.common.engine_factory.create_engine", return_value=expected
    ) as create_engine:
        assert engine_factory.create_pooled_engine("sqlite:///:memory:") is expected

    create_engine.assert_called_once_with(
        "sqlite:///:memory:", pool_pre_ping=True
    )


def test_legacy_mysql_mode_remains_explicitly_compatible():
    expected = object()
    with patch(
        "server.common.engine_factory._get_runtime_tls_config",
        return_value={"required": False, "ssl_ca": None},
    ), patch(
        "server.common.engine_factory.create_engine", return_value=expected
    ) as create_engine:
        assert (
            engine_factory.create_pooled_engine(
                "mysql+pymysql://u:p@localhost/probiga?charset=utf8mb4",
                future=True,
            )
            is expected
        )

    create_engine.assert_called_once_with(
        "mysql+pymysql://u:p@localhost/probiga?charset=utf8mb4",
        pool_pre_ping=True,
        future=True,
    )


def test_mysql_tls_required_needs_ca():
    with patch(
        "server.common.engine_factory._get_runtime_tls_config",
        return_value={"required": True, "ssl_ca": None},
    ), pytest.raises(RuntimeError, match="requires MYSQL_SSL_CA"):
        engine_factory.create_pooled_engine(
            "mysql+pymysql://u:p@localhost/probiga"
        )


def test_mysql_ca_without_required_mode_is_rejected(tmp_path):
    ca_file = tmp_path / "ca.pem"
    ca_file.write_text("test", encoding="ascii")
    with patch(
        "server.common.engine_factory._get_runtime_tls_config",
        return_value={"required": False, "ssl_ca": str(ca_file)},
    ), pytest.raises(RuntimeError, match="ambiguous MySQL TLS policy"):
        engine_factory.create_pooled_engine(
            "mysql+pymysql://u:p@localhost/probiga"
        )


def test_mysql_tls_ca_must_be_absolute(monkeypatch):
    monkeypatch.setattr(Path, "is_absolute", lambda _self: False)
    with patch(
        "server.common.engine_factory._get_runtime_tls_config",
        return_value={"required": True, "ssl_ca": "relative-ca.pem"},
    ), pytest.raises(RuntimeError, match="absolute path"):
        engine_factory.create_pooled_engine(
            "mysql+pymysql://u:p@localhost/probiga"
        )


def test_mysql_tls_requires_explicit_pymysql_driver(tmp_path):
    ca_file = tmp_path / "ca.pem"
    ca_file.write_text("test", encoding="ascii")
    with patch(
        "server.common.engine_factory._get_runtime_tls_config",
        return_value=_tls_config(ca_file),
    ), pytest.raises(RuntimeError, match=r"mysql\+pymysql"):
        engine_factory.create_pooled_engine("mysql://u:p@localhost/probiga")


@pytest.mark.parametrize(
    "url",
    (
        "mysql+pymysql://u:p@localhost/probiga?ssl_ca=C%3A%5Cca.pem",
        "mysql+pymysql://u:p@localhost/probiga?ssl_verify_cert=false",
        "mysql+pymysql://u:p@localhost/probiga?tls_version=TLSv1.2",
    ),
)
def test_mysql_tls_settings_are_forbidden_in_url(url):
    with pytest.raises(RuntimeError, match="must not be embedded"):
        engine_factory.create_pooled_engine(url)


@pytest.mark.parametrize(
    "kwargs",
    (
        {"creator": object()},
        {"module": object()},
    ),
)
def test_mysql_tls_rejects_dbapi_bypass_hooks(tmp_path, kwargs):
    ca_file = tmp_path / "ca.pem"
    ca_file.write_text("test", encoding="ascii")
    with patch(
        "server.common.engine_factory._get_runtime_tls_config",
        return_value=_tls_config(ca_file),
    ), pytest.raises(RuntimeError, match="bypass TLS policy"):
        engine_factory.create_pooled_engine(
            "mysql+pymysql://u:p@localhost/probiga", **kwargs
        )


@pytest.mark.parametrize(
    "connect_args",
    (
        {"ssl_ca": "other.pem"},
        {"ssl_verify_cert": False},
        {"ssl": {"ca": "other.pem"}},
    ),
)
def test_mysql_tls_rejects_caller_tls_connect_args(tmp_path, connect_args):
    ca_file = tmp_path / "ca.pem"
    ca_file.write_text("test", encoding="ascii")
    with patch(
        "server.common.engine_factory._get_runtime_tls_config",
        return_value=_tls_config(ca_file),
    ), pytest.raises(RuntimeError, match="centrally managed"):
        engine_factory.create_pooled_engine(
            "mysql+pymysql://u:p@localhost/probiga",
            connect_args=connect_args,
        )


def test_mysql_tls_preserves_non_tls_connect_args_and_attaches_cipher_gate(tmp_path):
    ca_file = tmp_path / "ca.pem"
    ca_file.write_text("test", encoding="ascii")
    expected_engine = object()
    with patch(
        "server.common.engine_factory._get_runtime_tls_config",
        return_value=_tls_config(ca_file),
    ), patch(
        "server.common.engine_factory.create_engine", return_value=expected_engine
    ) as create_engine, patch("server.common.engine_factory.event.listen") as listen:
        actual = engine_factory.create_pooled_engine(
            "mysql+pymysql://u:p@localhost/probiga?charset=utf8mb4",
            connect_args={"connect_timeout": 12},
            future=True,
        )

    assert actual is expected_engine
    create_engine.assert_called_once_with(
        "mysql+pymysql://u:p@localhost/probiga?charset=utf8mb4",
        pool_pre_ping=True,
        future=True,
        connect_args={
            "connect_timeout": 12,
            "ssl_ca": str(ca_file.resolve()),
            "ssl_verify_cert": True,
        },
    )
    listen.assert_called_once_with(
        expected_engine, "connect", engine_factory._verify_runtime_mysql_tls
    )


@pytest.mark.parametrize(
    "row",
    (
        ("Ssl_cipher", "TLS_AES_256_GCM_SHA384"),
        {"Variable_name": "Ssl_cipher", "Value": "TLS_AES_128_GCM_SHA256"},
    ),
)
def test_runtime_tls_cipher_gate_accepts_negotiated_tls(row):
    connection = _Connection(row)

    engine_factory._verify_runtime_mysql_tls(connection, None)

    assert connection.closed is False
    assert connection._cursor.closed is True
    assert connection._cursor.executed == [
        "SHOW SESSION STATUS LIKE 'Ssl_cipher'"
    ]


def test_runtime_tls_cipher_gate_closes_and_rejects_plain_connection():
    connection = _Connection(("Ssl_cipher", ""))

    with pytest.raises(RuntimeError, match="negotiated no TLS cipher"):
        engine_factory._verify_runtime_mysql_tls(connection, None)

    assert connection.closed is True
    assert connection._cursor.closed is True
