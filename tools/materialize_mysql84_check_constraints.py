#!/usr/bin/env python3
"""Audit or materialize V2 CHECK constraints on a restored MySQL 8.4 target."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
import json
import os
from pathlib import Path
import sys

from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.db.mysql84_check_constraints import (  # noqa: E402
    materialize_mysql84_check_constraints,
)
from tools.env_config import load_project_env  # noqa: E402
from tools.mysql_acceptance_tls import (  # noqa: E402
    MySQLAcceptanceTLSConfig,
    create_mysql_acceptance_engine,
    require_mysql_acceptance_ssl_ca,
)


MIGRATION_URL_ENV = "MYSQL84_MIGRATION_URL"
MIGRATION_SSL_CA_ENV = "MYSQL84_MIGRATION_SSL_CA"


def require_mysql84_migration_url(
    value: object,
    *,
    expected_schema: str,
    expected_server_port: int,
) -> str:
    """Validate the isolated migration URL before opening any connection.

    TLS settings are deliberately forbidden in the URL.  The formal CLI
    obtains its CA independently and injects one fixed verified-TLS policy.
    Requiring the expected schema and explicit target port here also prevents
    an accidental identity probe against the source service.
    """

    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"{MIGRATION_URL_ENV} is required; MYSQL_URL is intentionally "
            "ignored"
        )
    raw = value.strip()
    try:
        parsed = make_url(raw)
        url_port = parsed.port
    except (ArgumentError, TypeError, ValueError) as exc:
        raise ValueError(
            f"{MIGRATION_URL_ENV} is not a valid SQLAlchemy URL"
        ) from exc
    if parsed.drivername.lower() != "mysql+pymysql":
        raise ValueError(
            f"{MIGRATION_URL_ENV} must use an explicit mysql+pymysql URL"
        )
    if parsed.query:
        raise ValueError(
            f"{MIGRATION_URL_ENV} must not contain URL query parameters; "
            "TLS is configured independently"
        )
    if not parsed.host:
        raise ValueError(f"{MIGRATION_URL_ENV} must contain an explicit host")
    if url_port is None:
        raise ValueError(f"{MIGRATION_URL_ENV} must contain an explicit TCP port")
    if url_port != expected_server_port:
        raise ValueError(
            f"{MIGRATION_URL_ENV} port does not match --expected-server-port"
        )
    if parsed.database != expected_schema:
        raise ValueError(
            f"{MIGRATION_URL_ENV} database does not match --schema"
        )
    return raw


def resolve_mysql84_migration_tls_config(
    *,
    ssl_ca: str | None,
    ssl_ca_env: str | None,
    environ: Mapping[str, str] | None = None,
) -> MySQLAcceptanceTLSConfig:
    """Resolve a CA from an explicit path or the sole migration CA variable."""

    if ssl_ca and ssl_ca_env:
        raise ValueError("--ssl-ca and --ssl-ca-env are mutually exclusive")
    if ssl_ca:
        return require_mysql_acceptance_ssl_ca(ssl_ca)
    env_name = (ssl_ca_env or MIGRATION_SSL_CA_ENV).strip()
    if env_name != MIGRATION_SSL_CA_ENV:
        raise ValueError(
            f"--ssl-ca-env must name exactly {MIGRATION_SSL_CA_ENV}"
        )
    source = os.environ if environ is None else environ
    return require_mysql_acceptance_ssl_ca(source.get(env_name, ""))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Fail-closed audit/materialization of CHECK constraints omitted by "
            "MySQL 5.5/5.7 logical dumps"
        )
    )
    parser.add_argument(
        "--schema",
        required=True,
        help="exact restored target schema, normally probiga",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="add missing checks and enforce a clean constraint batch",
    )
    parser.add_argument(
        "--confirm-restored-target-offline",
        action="store_true",
        help="assert that business writers cannot reach this restored target",
    )
    parser.add_argument(
        "--expected-server-uuid",
        required=True,
        help="independently verified UUID of the isolated/restored target",
    )
    parser.add_argument(
        "--expected-server-port",
        required=True,
        type=int,
        help="independently verified TCP port of the restored target",
    )
    tls_group = parser.add_mutually_exclusive_group()
    tls_group.add_argument(
        "--ssl-ca",
        help=(
            "absolute PEM/CRT/CER CA file for verified TLS; when omitted, "
            f"{MIGRATION_SSL_CA_ENV} is required"
        ),
    )
    tls_group.add_argument(
        "--ssl-ca-env",
        choices=(MIGRATION_SSL_CA_ENV,),
        help=(
            "restricted environment variable containing the absolute CA "
            f"path (only {MIGRATION_SSL_CA_ENV} is accepted)"
        ),
    )
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.confirm_restored_target_offline and not args.apply:
        raise SystemExit(
            "--confirm-restored-target-offline is meaningful only with --apply"
        )
    load_project_env()
    try:
        database_url = require_mysql84_migration_url(
            os.environ.get(MIGRATION_URL_ENV, ""),
            expected_schema=args.schema,
            expected_server_port=args.expected_server_port,
        )
        tls_config = resolve_mysql84_migration_tls_config(
            ssl_ca=args.ssl_ca,
            ssl_ca_env=args.ssl_ca_env,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    engine = create_mysql_acceptance_engine(
        database_url,
        tls_config=tls_config,
        pool_pre_ping=True,
        pool_recycle=900,
        future=True,
    )
    try:
        with engine.connect() as connection:
            report = materialize_mysql84_check_constraints(
                connection,
                expected_schema=args.schema,
                expected_server_uuid=args.expected_server_uuid,
                expected_server_port=args.expected_server_port,
                apply=args.apply,
                restored_target_offline=args.confirm_restored_target_offline,
            )
    finally:
        engine.dispose()

    payload = report.as_dict()
    payload["status"] = "ok" if report.complete else "blocked"
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(
            f"{payload['status']}: {report.applicable_constraint_count} "
            f"applicable CHECK constraints in {report.schema}; "
            f"added={len(report.added_not_enforced)}, "
            f"enforced={len(report.enforced_constraints)}"
        )
        for name, count in report.violation_counts:
            if count:
                print(f"violation {name}: {count}")
    return 0 if report.complete else 2


if __name__ == "__main__":
    raise SystemExit(main())
