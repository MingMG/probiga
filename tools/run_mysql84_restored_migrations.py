#!/usr/bin/env python3
"""Guarded V2 -> V3 -> V4 migrations on a restored ``probiga`` schema.

This is intentionally separate from the ordinary application migration CLIs.
It can only target an explicitly identified local MySQL 8.4.11 rehearsal (or a
later frozen cutover target), reads credentials from a protected option file,
and requires the trigger-maintenance wrapper's child-process marker.  The
wrapper is responsible for the short-lived ``log_bin_trust_function_creators``
window; this command never changes that global variable itself.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pymysql
from sqlalchemy import inspect, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.db.migrations_v2 import run_v2_migrations
from server.db.migrations_v3 import run_v3_migrations
from server.db.migrations_v4 import run_v4_migrations
from tools.run_mysql84_logical_restore import (
    AdminClientOptions,
    RestoreError,
    inspect_target,
    read_admin_client_options,
    validate_ca_file,
)


SCHEMA = "probiga"
OFFLINE_ACK = "BUSINESS_WRITES_STOPPED"
WINDOW_ENV = "PROBIGA_MYSQL84_TRIGGER_MIGRATION_WINDOW_ACTIVE"
LEDGER_TABLES = (
    "schema_migration_v2",
    "schema_migration_v3",
    "schema_migration_v4",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    if not path.is_absolute():
        raise RestoreError("evidence path must be absolute")
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.partial-{os.getpid()}")
    if partial.exists() or path.exists():
        raise RestoreError("evidence path already exists")
    partial.write_text(
        # SQLAlchemy returns datetime values for migration-ledger timestamps.
        # Evidence must remain JSON while preserving those values losslessly
        # as their ISO-8601 string representation.
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            default=lambda item: item.isoformat()
            if hasattr(item, "isoformat")
            else str(item),
        )
        + "\n",
        encoding="utf-8",
    )
    partial.replace(path)


def _engine_url(options: AdminClientOptions) -> str:
    user = quote(options.user, safe="")
    password = quote(options.password, safe="")
    return f"mysql+pymysql://{user}:{password}@127.0.0.1:{options.port}/{SCHEMA}"


def _schema_identity(options: AdminClientOptions, ca_file: Path) -> dict[str, Any]:
    try:
        connection = pymysql.connect(
            host=options.host,
            port=options.port,
            user=options.user,
            password=options.password,
            database=SCHEMA,
            charset="utf8mb4",
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10,
            read_timeout=60,
            write_timeout=60,
            ssl_ca=str(ca_file),
            ssl_verify_cert=True,
        )
    except pymysql.MySQLError as exc:
        raise RestoreError("restored-target schema TLS connection failed") from exc
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT DATABASE() AS database_name, @@server_uuid AS server_uuid, "
                "@@port AS port"
            )
            row = cursor.fetchone()
            cursor.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
            tls = cursor.fetchone()
    finally:
        connection.close()
    if not isinstance(row, dict) or str(row.get("database_name") or "") != SCHEMA:
        raise RestoreError("restored-target connection did not select probiga")
    cipher = str((tls or {}).get("Value") or "").strip()
    if not cipher:
        raise RestoreError("restored-target migration connection negotiated no TLS")
    return {
        "database": SCHEMA,
        "server_uuid": str(row.get("server_uuid") or "").strip().lower(),
        "port": int(row.get("port")),
        "tls_cipher": cipher,
    }


def _ledger_snapshot(engine) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    inspector = inspect(engine)
    with engine.connect() as connection:
        for table in LEDGER_TABLES:
            if not inspector.has_table(table, schema=SCHEMA):
                result[table] = []
                continue
            rows = connection.execute(
                text(f"SELECT * FROM `{table}` ORDER BY version")
            ).mappings().all()
            result[table] = [dict(row) for row in rows]
    return result


def _result_rows(values: list[Any]) -> list[dict[str, Any]]:
    return [asdict(value) for value in values]


def run(args: argparse.Namespace) -> dict[str, Any]:
    if args.schema != SCHEMA:
        raise RestoreError("restored migration CLI is restricted to schema probiga")
    if args.offline_ack != OFFLINE_ACK:
        raise RestoreError("exact business-offline acknowledgement is required")
    if args.expected_server_port == 3306:
        raise RestoreError("restored migration CLI cannot target production port 3306")
    if os.environ.get(WINDOW_ENV) != "1":
        raise RestoreError(
            "restored migrations must run inside the trigger-maintenance wrapper"
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
        allow_log_bin_trust_function_creators=(os.environ.get(WINDOW_ENV) == "1"),
    )
    schema_identity = _schema_identity(options, ca_file)
    if schema_identity["server_uuid"] != target.server_uuid:
        raise RestoreError("schema connection UUID differs from target preflight")

    previous_tls = os.environ.get("MYSQL_TLS_REQUIRED")
    previous_ca = os.environ.get("MYSQL_SSL_CA")
    os.environ["MYSQL_TLS_REQUIRED"] = "true"
    os.environ["MYSQL_SSL_CA"] = str(ca_file)
    engine = create_batch_engine(_engine_url(options), pool_pre_ping=True)
    started = _utc_now()
    try:
        plan = {
            "v2": _result_rows(
                run_v2_migrations(
                    engine,
                    dry_run=True,
                    allow_execution_evidence=args.allow_execution_evidence,
                )
            ),
            "v3": _result_rows(run_v3_migrations(engine, dry_run=True)),
            "v4": _result_rows(run_v4_migrations(engine, dry_run=True)),
        }
        before = _ledger_snapshot(engine)
        if args.plan_only:
            after = before
            return {
                "schema_version": 1,
                "status": "plan_only",
                "started_at_utc": started,
                "finished_at_utc": _utc_now(),
                "mode": args.mode,
                "target": asdict(target),
                "schema_identity": schema_identity,
                "ledger_before": before,
                "plan": plan,
                "ledger_after": after,
                "replay": None,
            }

        applied = {
            "v2": _result_rows(
                run_v2_migrations(
                    engine,
                    allow_execution_evidence=args.allow_execution_evidence,
                )
            ),
            "v3": _result_rows(run_v3_migrations(engine)),
            "v4": _result_rows(run_v4_migrations(engine)),
        }
        after = _ledger_snapshot(engine)
        replay = {
            "v2": _result_rows(
                run_v2_migrations(
                    engine,
                    allow_execution_evidence=args.allow_execution_evidence,
                )
            ),
            "v3": _result_rows(run_v3_migrations(engine)),
            "v4": _result_rows(run_v4_migrations(engine)),
        }
        replay_after = _ledger_snapshot(engine)
        if after != replay_after:
            raise RestoreError("migration replay changed the ledger")
        return {
            "schema_version": 1,
            "status": "ok",
            "started_at_utc": started,
            "finished_at_utc": _utc_now(),
            "mode": args.mode,
            "target": asdict(target),
            "schema_identity": schema_identity,
            "ledger_before": before,
            "plan": plan,
            "applied": applied,
            "ledger_after": after,
            "replay": replay,
            "replay_ledger_after": replay_after,
        }
    finally:
        engine.dispose()
        if previous_tls is None:
            os.environ.pop("MYSQL_TLS_REQUIRED", None)
        else:
            os.environ["MYSQL_TLS_REQUIRED"] = previous_tls
        if previous_ca is None:
            os.environ.pop("MYSQL_SSL_CA", None)
        else:
            os.environ["MYSQL_SSL_CA"] = previous_ca


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Guarded V2/V3/V4 migrations on restored probiga."
    )
    parser.add_argument("--mode", choices=("rehearsal", "final-frozen"), required=True)
    parser.add_argument("--schema", default=SCHEMA)
    parser.add_argument("--admin-option-file", type=Path, required=True)
    parser.add_argument("--ssl-ca", type=Path, required=True)
    parser.add_argument("--expected-server-uuid", required=True)
    parser.add_argument("--expected-server-port", type=int, required=True)
    parser.add_argument("--expected-datadir", type=Path, required=True)
    parser.add_argument("--offline-ack", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--allow-execution-evidence", action="store_true")
    parser.add_argument("--plan-only", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        evidence = run(args)
    except (RestoreError, RuntimeError, OSError, ValueError, pymysql.MySQLError) as exc:
        failure = {
            "schema_version": 1,
            "status": "failed",
            "finished_at_utc": _utc_now(),
            "failure": str(exc),
        }
        try:
            _atomic_json(args.evidence, failure)
        except Exception:
            pass
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    _atomic_json(args.evidence, evidence)
    print(
        json.dumps(
            evidence,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            default=lambda item: item.isoformat()
            if hasattr(item, "isoformat")
            else str(item),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
