# -*- coding: utf-8 -*-
"""Small SQLAlchemy engine factory helpers.

All production MySQL engines pass through this module.  When
``MYSQL_TLS_REQUIRED=true`` it applies one verified-CA policy to API, batch,
scheduler, worker and routed market-data connections.  TLS settings are kept
out of database URLs and cannot be weakened by individual callers.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine, make_url


_TLS_QUERY_KEYS = {
    "server_public_key",
    "ssl",
    "ssl_ca",
    "ssl_capath",
    "ssl_cert",
    "ssl_cipher",
    "ssl_disabled",
    "ssl_key",
    "ssl_verify_cert",
    "ssl_verify_identity",
    "tls_version",
}


def _get_runtime_tls_config() -> Mapping[str, str | bool | None]:
    # Imported lazily so configuration remains the dependency root and this
    # factory can still be imported by lightweight tooling and unit tests.
    from server.common.config import get_mysql_tls_runtime_config

    return get_mysql_tls_runtime_config()


def _tls_override_keys(values: Mapping[str, Any]) -> set[str]:
    return {
        str(key).strip().lower()
        for key in values
        if str(key).strip().lower() in _TLS_QUERY_KEYS
        or str(key).strip().lower().startswith(("ssl_", "tls_"))
    }


def _validated_runtime_ca(config: Mapping[str, str | bool | None]) -> Path | None:
    required = bool(config.get("required"))
    raw_ca = str(config.get("ssl_ca") or "").strip()
    if not required:
        if raw_ca:
            raise RuntimeError(
                "MYSQL_SSL_CA is configured while MYSQL_TLS_REQUIRED is false; "
                "refusing an ambiguous MySQL TLS policy"
            )
        return None
    if not raw_ca:
        raise RuntimeError(
            "MYSQL_TLS_REQUIRED=true requires MYSQL_SSL_CA to name an absolute CA file"
        )
    path = Path(raw_ca).expanduser()
    if not path.is_absolute():
        raise RuntimeError("MYSQL_SSL_CA must be an absolute path")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise RuntimeError("MYSQL_SSL_CA does not exist or cannot be resolved") from exc
    if not resolved.is_file():
        raise RuntimeError("MYSQL_SSL_CA must identify a regular file")
    return resolved


def _mysql_tls_cipher(dbapi_connection: Any) -> str:
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
        row = cursor.fetchone()
    finally:
        cursor.close()
    if isinstance(row, Mapping):
        value = row.get("Value") or row.get("VALUE") or row.get("value")
    elif row and len(row) >= 2:
        value = row[1]
    else:
        value = ""
    if isinstance(value, bytes):
        value = value.decode("ascii", errors="replace")
    return str(value or "").strip()


def _verify_runtime_mysql_tls(dbapi_connection: Any, _connection_record: Any) -> None:
    """Reject a checkout if the DBAPI connection negotiated no TLS cipher."""
    if not _mysql_tls_cipher(dbapi_connection):
        try:
            dbapi_connection.close()
        finally:
            raise RuntimeError(
                "MySQL runtime TLS was required but the connection negotiated no TLS cipher"
            )


def create_pooled_engine(
    url: str,
    *,
    pool_config: Mapping[str, int] | None = None,
    pool_pre_ping: bool = True,
    **kwargs: Any,
) -> Engine:
    """Create an engine with shared defaults and optional pool settings."""
    parsed_url = make_url(url)
    engine_kwargs: dict[str, Any] = {"pool_pre_ping": pool_pre_ping}
    if pool_config:
        engine_kwargs.update(
            {
                "pool_size": int(pool_config["pool_size"]),
                "max_overflow": int(pool_config["max_overflow"]),
                "pool_recycle": int(pool_config["pool_recycle"]),
            }
        )
    engine_kwargs.update(kwargs)

    if parsed_url.get_backend_name() != "mysql":
        return create_engine(url, **engine_kwargs)

    query_overrides = _tls_override_keys(parsed_url.query)
    if query_overrides:
        raise RuntimeError(
            "MySQL TLS parameters must not be embedded in the database URL: "
            + ", ".join(sorted(query_overrides))
        )

    ca_path = _validated_runtime_ca(_get_runtime_tls_config())
    if ca_path is None:
        return create_engine(url, **engine_kwargs)

    if parsed_url.drivername != "mysql+pymysql":
        raise RuntimeError(
            "MYSQL_TLS_REQUIRED=true requires an explicit mysql+pymysql URL"
        )
    if "creator" in engine_kwargs or "module" in engine_kwargs:
        raise RuntimeError(
            "MYSQL_TLS_REQUIRED=true forbids creator/module overrides that bypass TLS policy"
        )

    caller_connect_args = dict(engine_kwargs.pop("connect_args", {}) or {})
    connect_overrides = _tls_override_keys(caller_connect_args)
    if connect_overrides:
        raise RuntimeError(
            "MySQL TLS connect_args are centrally managed and cannot be overridden: "
            + ", ".join(sorted(connect_overrides))
        )
    caller_connect_args.update(
        {
            "ssl_ca": str(ca_path),
            "ssl_verify_cert": True,
        }
    )
    engine_kwargs["connect_args"] = caller_connect_args
    engine = create_engine(url, **engine_kwargs)
    event.listen(engine, "connect", _verify_runtime_mysql_tls)
    return engine
