"""Restricted TLS wiring for isolated MySQL acceptance commands.

The acceptance URLs deliberately forbid query parameters, so TLS cannot be
smuggled in through a URL.  Formal command-line entry points resolve a CA file
only from their own TEST/CI environment-variable namespace and pass the
validated configuration through this module.

``tls_config=None`` is retained solely for the existing programmatic unit-test
and legacy isolated-harness contract.  The V2/V3/V4 command-line entry points
never use that compatibility path: they always resolve a CA configuration and
fail before engine creation when it is absent or invalid.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os
from pathlib import Path
import re
import ssl
from typing import Any

from sqlalchemy.engine import Engine, make_url

from server.common.engine_factory import create_isolated_mysql_acceptance_engine
from tools.env_config import create_tool_engine


_ALLOWED_SCOPES = frozenset({"V2_EVIDENCE", "V3", "V4"})
_CA_FILE_SUFFIXES = frozenset({".pem", ".crt", ".cer"})
_MAX_CA_FILE_BYTES = 4 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class MySQLAcceptanceTLSConfig:
    """Validated CA-only TLS configuration for one acceptance engine."""

    ssl_ca: str


def _ssl_ca_env_pattern(scope: str) -> re.Pattern[str]:
    normalized = str(scope or "").strip().upper()
    if normalized not in _ALLOWED_SCOPES:
        raise ValueError("unsupported MySQL acceptance TLS scope")
    return re.compile(
        rf"^{re.escape(normalized)}_(?:TEST|CI)"
        rf"(?:_[A-Z0-9]+)*_MYSQL_SSL_CA$"
    )


def require_mysql_acceptance_ssl_ca(value: object) -> MySQLAcceptanceTLSConfig:
    """Validate one absolute, readable CA bundle without accepting fallbacks."""

    if not isinstance(value, str) or not value.strip():
        raise ValueError("a dedicated MySQL acceptance SSL CA file is required")
    raw = value.strip()
    path = Path(raw)
    if not path.is_absolute():
        raise ValueError("MySQL acceptance SSL CA path must be absolute")
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise ValueError("MySQL acceptance SSL CA file does not exist") from exc
    if not resolved.is_file():
        raise ValueError("MySQL acceptance SSL CA path must name a file")
    if resolved.suffix.lower() not in _CA_FILE_SUFFIXES:
        raise ValueError("MySQL acceptance SSL CA must be a PEM/CRT/CER file")
    try:
        size = resolved.stat().st_size
    except OSError as exc:
        raise ValueError("MySQL acceptance SSL CA file is not readable") from exc
    if size < 1 or size > _MAX_CA_FILE_BYTES:
        raise ValueError("MySQL acceptance SSL CA file size is invalid")
    try:
        # Parse the bundle up front.  A malformed or unreadable CA must fail
        # before any network connection is attempted.
        ssl.create_default_context(cafile=str(resolved))
    except (OSError, ssl.SSLError, ValueError) as exc:
        raise ValueError("MySQL acceptance SSL CA bundle is invalid") from exc
    return MySQLAcceptanceTLSConfig(ssl_ca=str(resolved))


def resolve_mysql_acceptance_tls_config(
    scope: str,
    env_name: str,
    *,
    environ: Mapping[str, str] | None = None,
) -> MySQLAcceptanceTLSConfig:
    """Resolve a CA only from an explicitly scoped TEST/CI variable."""

    if not isinstance(env_name, str) or not env_name.strip():
        raise ValueError("SSL CA environment variable name is required")
    normalized = env_name.strip()
    if _ssl_ca_env_pattern(scope).fullmatch(normalized) is None:
        raise ValueError(
            "SSL CA environment variable must be a dedicated TEST/CI "
            "*_MYSQL_SSL_CA name"
        )
    source = os.environ if environ is None else environ
    return require_mysql_acceptance_ssl_ca(source.get(normalized, ""))


def create_mysql_acceptance_engine(
    url: str,
    *,
    tls_config: MySQLAcceptanceTLSConfig | None,
    **engine_options: Any,
) -> Engine:
    """Create an acceptance engine with a fixed, non-extensible TLS policy.

    Callers cannot supply arbitrary ``connect_args`` or a custom DBAPI creator.
    When TLS is configured, only PyMySQL is accepted, certificate-chain
    verification is mandatory, and every new DBAPI connection must report a
    negotiated cipher.  There is no retry without TLS.
    """

    forbidden = frozenset(engine_options) & {
        "connect_args", "creator", "module"
    }
    if forbidden:
        raise TypeError(
            "MySQL acceptance engine options may not override "
            + ", ".join(sorted(forbidden))
        )
    if tls_config is None:
        return create_tool_engine(url, **engine_options)
    if type(tls_config) is not MySQLAcceptanceTLSConfig:
        raise TypeError("tls_config must be MySQLAcceptanceTLSConfig or None")
    verified = require_mysql_acceptance_ssl_ca(tls_config.ssl_ca)
    parsed = make_url(url)
    if parsed.get_backend_name().lower() != "mysql":
        raise ValueError("MySQL acceptance TLS requires the MySQL backend")
    if parsed.get_driver_name().lower() != "pymysql":
        raise ValueError(
            "MySQL acceptance TLS requires an explicit mysql+pymysql URL"
        )
    return create_isolated_mysql_acceptance_engine(
        url,
        ssl_ca=verified.ssl_ca,
        **engine_options,
    )


__all__ = [
    "MySQLAcceptanceTLSConfig",
    "create_mysql_acceptance_engine",
    "require_mysql_acceptance_ssl_ca",
    "resolve_mysql_acceptance_tls_config",
]
