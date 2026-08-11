#!/usr/bin/env python3
"""Audit or repair legacy zero DATETIME defaults on a MySQL 8.4 target."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.db.mysql84_datetime_defaults import (  # noqa: E402
    materialize_mysql84_datetime_defaults,
)
from tools.env_config import load_project_env  # noqa: E402
from tools.materialize_mysql84_check_constraints import (  # noqa: E402
    MIGRATION_SSL_CA_ENV,
    MIGRATION_URL_ENV,
    require_mysql84_migration_url,
    resolve_mysql84_migration_tls_config,
)
from tools.mysql_acceptance_tls import (  # noqa: E402
    create_mysql_acceptance_engine,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Fail-closed audit/repair of eight legacy zero DATETIME defaults "
            "on an isolated Oracle MySQL 8.4.11 target"
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
        help="set clean legacy defaults to CURRENT_TIMESTAMP",
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
            report = materialize_mysql84_datetime_defaults(
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
    if not report.ready_to_apply:
        payload["status"] = "blocked"
    elif report.complete:
        payload["status"] = "ok"
    else:
        payload["status"] = "ready"
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(
            f"{payload['status']}: {report.expected_column_count} DATETIME "
            f"defaults in {report.schema}; "
            f"changed={len(report.changed_columns)}"
        )
        for violation in report.violation_counts:
            if violation.total:
                print(
                    f"violation {violation.column_key}: "
                    f"all_zero={violation.all_zero_count}, "
                    f"partial_zero={violation.partial_zero_count}"
                )
    if not report.ready_to_apply:
        return 2
    if args.apply and not report.complete:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
