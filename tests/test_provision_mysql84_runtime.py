from __future__ import annotations

from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import pytest

from tools import provision_mysql84_runtime as provision


def test_generate_password_uses_safe_long_alphabet():
    password = provision.generate_password()
    assert provision._PASSWORD_RE.fullmatch(password)


def test_build_runtime_option_file_is_formal_local_and_secret_is_not_repr():
    password = "A" * 64
    payload = provision.build_runtime_option_file(
        user="probiga_runtime", password=password
    ).decode("ascii")
    assert "host=127.0.0.1" in payload
    assert "port=3306" in payload
    assert "user=probiga_runtime" in payload
    assert f"password={password}" in payload
    assert "F:" not in payload


def test_build_staged_env_replaces_url_and_enables_central_tls(tmp_path: Path):
    source = (
        "MYSQL_URL=mysql+pymysql://root:old@localhost:3306/probiga?charset=utf8mb4\n"
        "MYSQL_TLS_REQUIRED=false\n"
        "OTHER=value\n"
    )
    password = "A_B-" * 16
    formal_ca = (tmp_path / "ca.pem").resolve()

    result = provision.build_staged_env(
        source,
        user="probiga_runtime",
        password=password,
        formal_ca=formal_ca,
    )

    values = dict(
        line.split("=", 1) for line in result.splitlines() if line and not line.startswith("#")
    )
    parsed = urlsplit(values["MYSQL_URL"])
    assert parsed.hostname == "127.0.0.1"
    assert parsed.port == 3306
    assert unquote(parsed.username or "") == "probiga_runtime"
    assert unquote(parsed.password or "") == password
    assert parse_qs(parsed.query) == {"charset": ["utf8mb4"]}
    assert values["MYSQL_TLS_REQUIRED"] == "true"
    assert values["MYSQL_SSL_CA"] == formal_ca.as_posix()
    assert values["OTHER"] == "value"


def test_build_staged_env_adds_missing_tls_keys(tmp_path: Path):
    result = provision.build_staged_env(
        "MYSQL_URL=old\n",
        user="probiga_runtime",
        password="A" * 64,
        formal_ca=(tmp_path / "ca.pem").resolve(),
    )
    assert result.count("MYSQL_TLS_REQUIRED=") == 1
    assert result.count("MYSQL_SSL_CA=") == 1


def test_build_staged_env_rejects_duplicate_managed_keys(tmp_path: Path):
    with pytest.raises(provision.ProvisionError, match="duplicate MYSQL_URL"):
        provision.build_staged_env(
            "MYSQL_URL=one\nMYSQL_URL=two\n",
            user="probiga_runtime",
            password="A" * 64,
            formal_ca=(tmp_path / "ca.pem").resolve(),
        )


def test_runtime_option_rejects_unsafe_account_or_password():
    with pytest.raises(provision.ProvisionError):
        provision.build_runtime_option_file(user="bad-user", password="A" * 64)
    with pytest.raises(provision.ProvisionError):
        provision.build_runtime_option_file(user="good_user", password="short")
