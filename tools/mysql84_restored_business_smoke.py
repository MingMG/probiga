#!/usr/bin/env python3
"""Read-only business smoke for a restored local MySQL 8.4 target."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pymysql

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.run_mysql84_logical_restore import (
    RestoreError,
    inspect_target,
    read_admin_client_options,
    validate_ca_file,
)


EXPECTED_SCHEMAS = ("biga", "probiga", "probiga_qmt_history")
EXPECTED_LEDGER_TABLES = (
    "schema_migration_v2",
    "schema_migration_v3",
    "schema_migration_v4",
)
PRODUCTION_SMOKE_ACK = "I_CONFIRM_READ_ONLY_MYSQL84_PRODUCTION_SMOKE"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    if not path.is_absolute() or path.exists():
        raise RestoreError("smoke evidence must be a new absolute path")
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.partial")
    if partial.exists():
        raise RestoreError("smoke evidence partial already exists")
    partial.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    partial.replace(path)


def run(args: argparse.Namespace) -> dict[str, Any]:
    if (
        args.expected_server_port == 3306
        and getattr(args, "production_ack", None) != PRODUCTION_SMOKE_ACK
    ):
        raise RestoreError(
            "exact read-only production smoke acknowledgement is required for port 3306"
        )
    ca_file = validate_ca_file(args.ssl_ca)
    options = read_admin_client_options(
        args.admin_option_file, expected_port=args.expected_server_port
    )
    target = inspect_target(
        options,
        ca_file,
        expected_server_uuid=args.expected_server_uuid,
        expected_server_port=args.expected_server_port,
        expected_datadir=args.expected_datadir,
    )
    schemas: dict[str, dict[str, Any]] = {}
    try:
        for schema in EXPECTED_SCHEMAS:
            connection = pymysql.connect(
                host=options.host,
                port=options.port,
                user=options.user,
                password=options.password,
                database=schema,
                charset="utf8mb4",
                autocommit=False,
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=10,
                read_timeout=60,
                write_timeout=60,
                ssl_ca=str(ca_file),
                ssl_verify_cert=True,
            )
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SET TRANSACTION READ ONLY")
                    cursor.execute("START TRANSACTION READ ONLY")
                    cursor.execute(
                        "SELECT DATABASE() AS database_name, @@server_uuid AS server_uuid, "
                        "@@port AS port"
                    )
                    identity = cursor.fetchone() or {}
                    cursor.execute(
                        "SELECT COUNT(*) AS table_count FROM information_schema.tables "
                        "WHERE table_schema=%s AND table_type='BASE TABLE'",
                        (schema,),
                    )
                    table_count = int((cursor.fetchone() or {}).get("table_count") or 0)
                    ledger_presence: dict[str, bool] = {}
                    for table in EXPECTED_LEDGER_TABLES:
                        cursor.execute(
                            "SELECT COUNT(*) AS n FROM information_schema.tables "
                            "WHERE table_schema=%s AND table_name=%s",
                            (schema, table),
                        )
                        ledger_presence[table] = bool(
                            int((cursor.fetchone() or {}).get("n") or 0)
                        )
                    account_flag_violations = 0
                    if schema == "probiga" and ledger_presence.get("schema_migration_v2"):
                        cursor.execute(
                            "SELECT COUNT(*) AS n FROM st_trade_account_v2 "
                            "WHERE real_trading_enabled <> 0"
                        )
                        account_flag_violations = int(
                            (cursor.fetchone() or {}).get("n") or 0
                        )
                    schemas[schema] = {
                        "database": str(identity.get("database_name") or ""),
                        "server_uuid": str(identity.get("server_uuid") or "").lower(),
                        "port": int(identity.get("port")),
                        "table_count": table_count,
                        "ledger_presence": ledger_presence,
                        "real_trading_enabled_violations": account_flag_violations,
                    }
                connection.rollback()
            finally:
                connection.close()
    except pymysql.MySQLError as exc:
        raise RestoreError("read-only business smoke query failed") from exc

    for schema, observed in schemas.items():
        if observed["database"] != schema:
            raise RestoreError(f"business smoke selected wrong schema: {schema}")
        if observed["server_uuid"] != target.server_uuid or observed["port"] != target.port:
            raise RestoreError(f"business smoke identity drift in schema: {schema}")
        if observed["table_count"] <= 0:
            raise RestoreError(f"business schema has no tables: {schema}")
    if schemas["probiga"]["real_trading_enabled_violations"]:
        raise RestoreError("real_trading_enabled is not zero for every account")

    return {
        "schema_version": 1,
        "status": "ok",
        "observed_at_utc": _utc_now(),
        "target": {
            "server_uuid": target.server_uuid,
            "port": target.port,
            "datadir": target.datadir,
            "tls_cipher": target.tls_cipher,
            "version": target.version,
        },
        "schemas": schemas,
        "read_only_transaction": True,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only restored-target business smoke.")
    parser.add_argument("--admin-option-file", type=Path, required=True)
    parser.add_argument("--ssl-ca", type=Path, required=True)
    parser.add_argument("--expected-server-uuid", required=True)
    parser.add_argument("--expected-server-port", type=int, required=True)
    parser.add_argument("--expected-datadir", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--production-ack")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        evidence = run(args)
    except (RestoreError, OSError, ValueError, pymysql.MySQLError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    _atomic_json(args.evidence, evidence)
    print(json.dumps(evidence, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
