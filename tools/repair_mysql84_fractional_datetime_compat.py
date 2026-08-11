#!/usr/bin/env python3
"""Repair MySQL 5.5/8.4 fractional-DATETIME replay drift on frozen tables.

MySQL 5.5 truncates fractional seconds written into ``DATETIME(0)`` while
MySQL 8.4 rounds them unless ``TIME_TRUNCATE_FRACTIONAL`` is enabled.  A
statement-binlog replay can therefore differ by one second even though the
source accepted the original statement.  This tool repairs only the three
small, reviewed ProBigA tables observed during the upgrade.  It copies their
committed rows from the globally frozen 5.5 source into the isolated 8.4
target inside one target transaction and records hash-only evidence.
"""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence

import pymysql


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.mysql55_to_mysql84_data_manifest import (  # noqa: E402
    ManifestError,
    _connect,
    atomic_write_json,
    canonical_row_bytes,
    inspect_identity,
    seal_document,
)


APPLY_ACK = "I_CONFIRM_SOURCE_WRITES_FROZEN_AND_REPAIR_ISOLATED_MYSQL84_TARGET"
TABLE_PRIMARY_KEYS: Mapping[str, tuple[str, ...]] = {
    "st_etf_forward_observation": ("id",),
    "st_strategy_health_daily_v2": (
        "strategy_version",
        "trade_date",
        "window_days",
    ),
    "st_worker_heartbeat_v2": ("worker_name",),
}
SCHEMA = "probiga"
MAX_ROWS_PER_TABLE = 10_000


class CompatibilityRepairError(RuntimeError):
    """A repair precondition or postcondition failed closed."""


def _quote(identifier: str) -> str:
    if not identifier or not identifier.replace("_", "a").isalnum():
        raise CompatibilityRepairError(f"unsafe identifier: {identifier!r}")
    return f"`{identifier}`"


def _table_sql(table: str) -> str:
    if table not in TABLE_PRIMARY_KEYS:
        raise CompatibilityRepairError("table is outside the reviewed repair manifest")
    return f"{_quote(SCHEMA)}.{_quote(table)}"


def digest_rows(rows: Sequence[Sequence[object]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(canonical_row_bytes(row))
    return digest.hexdigest()


def _identity(
    connection: pymysql.Connection,
    *,
    role: str,
    expected_target_uuid: str,
    expected_target_port: int,
    expected_target_datadir: Path,
) -> dict[str, Any]:
    identity = inspect_identity(connection)
    if role == "source":
        if (
            identity.get("version") != "5.5.20-log"
            or identity.get("port") != 3306
            or identity.get("server_uuid") is not None
        ):
            raise CompatibilityRepairError("legacy source identity mismatch")
        return identity
    if (
        identity.get("version") != "8.4.11"
        or identity.get("port") != expected_target_port
        or identity.get("server_uuid") != expected_target_uuid
        or not identity.get("ssl_cipher")
    ):
        raise CompatibilityRepairError("isolated target identity or TLS mismatch")
    with connection.cursor() as cursor:
        cursor.execute("SELECT @@datadir")
        row = cursor.fetchone()
    if row is None:
        raise CompatibilityRepairError("target datadir query returned no row")
    observed = Path(str(row[0]).replace("/", "\\")).resolve(strict=False)
    expected = expected_target_datadir.resolve(strict=True)
    if str(observed).rstrip("\\").casefold() != str(expected).rstrip("\\").casefold():
        raise CompatibilityRepairError("isolated target datadir mismatch")
    return identity


def _table_metadata(
    connection: pymysql.Connection, table: str
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
            (SCHEMA, table),
        )
        columns = tuple(str(row[0]) for row in cursor.fetchall())
        cursor.execute(
            "SELECT COLUMN_NAME FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND INDEX_NAME='PRIMARY' "
            "ORDER BY SEQ_IN_INDEX",
            (SCHEMA, table),
        )
        primary_key = tuple(str(row[0]) for row in cursor.fetchall())
    if not columns:
        raise CompatibilityRepairError(f"missing reviewed table: {table}")
    if primary_key != TABLE_PRIMARY_KEYS[table]:
        raise CompatibilityRepairError(f"primary key drift on reviewed table: {table}")
    return columns, primary_key


def _assert_no_foreign_keys(connection: pymysql.Connection) -> None:
    tables = tuple(TABLE_PRIMARY_KEYS)
    placeholders = ",".join(["%s"] * len(tables))
    parameters = (SCHEMA, *tables, SCHEMA, *tables)
    query = (
        "SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE "
        "WHERE REFERENCED_TABLE_NAME IS NOT NULL AND "
        f"((TABLE_SCHEMA=%s AND TABLE_NAME IN ({placeholders})) OR "
        f"(REFERENCED_TABLE_SCHEMA=%s AND REFERENCED_TABLE_NAME IN ({placeholders})))"
    )
    with connection.cursor() as cursor:
        cursor.execute(query, parameters)
        count = int(cursor.fetchone()[0])
    if count:
        raise CompatibilityRepairError("reviewed repair tables unexpectedly have foreign keys")


def _read_rows(
    connection: pymysql.Connection,
    *,
    table: str,
    columns: Sequence[str],
    primary_key: Sequence[str],
) -> tuple[tuple[object, ...], ...]:
    projection = ",".join(_quote(column) for column in columns)
    ordering = ",".join(_quote(column) for column in primary_key)
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT {projection} FROM {_table_sql(table)} ORDER BY {ordering}")
        rows = tuple(tuple(row) for row in cursor.fetchall())
    if len(rows) > MAX_ROWS_PER_TABLE:
        raise CompatibilityRepairError(
            f"reviewed table exceeds the bounded repair row limit: {table}"
        )
    return rows


def _replace_rows(
    connection: pymysql.Connection,
    *,
    table: str,
    columns: Sequence[str],
    rows: Sequence[Sequence[object]],
) -> None:
    projection = ",".join(_quote(column) for column in columns)
    placeholders = ",".join(["%s"] * len(columns))
    with connection.cursor() as cursor:
        cursor.execute(f"DELETE FROM {_table_sql(table)}")
        if rows:
            cursor.executemany(
                f"INSERT INTO {_table_sql(table)} ({projection}) VALUES ({placeholders})",
                rows,
            )


def run(args: argparse.Namespace) -> dict[str, Any]:
    if args.apply_ack != APPLY_ACK:
        raise CompatibilityRepairError("exact compatibility-repair acknowledgement is required")
    expected_uuid = str(args.expected_target_uuid).strip().lower()
    target_datadir = args.expected_target_datadir.expanduser().resolve(strict=True)
    source = _connect(
        args.source_option_file,
        ssl_ca=None,
        require_tls=False,
        read_timeout=120,
    )
    target = _connect(
        args.target_option_file,
        ssl_ca=args.target_ssl_ca,
        require_tls=True,
        read_timeout=120,
    )
    committed = False
    try:
        source_identity = _identity(
            source,
            role="source",
            expected_target_uuid=expected_uuid,
            expected_target_port=args.expected_target_port,
            expected_target_datadir=target_datadir,
        )
        target_identity = _identity(
            target,
            role="target",
            expected_target_uuid=expected_uuid,
            expected_target_port=args.expected_target_port,
            expected_target_datadir=target_datadir,
        )
        _assert_no_foreign_keys(source)
        _assert_no_foreign_keys(target)
        source_rows: dict[str, tuple[tuple[object, ...], ...]] = {}
        metadata: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {}
        before: dict[str, dict[str, object]] = {}
        for table in TABLE_PRIMARY_KEYS:
            source_meta = _table_metadata(source, table)
            target_meta = _table_metadata(target, table)
            if source_meta != target_meta:
                raise CompatibilityRepairError(f"source/target column drift on {table}")
            metadata[table] = source_meta
            columns, primary_key = source_meta
            source_table_rows = _read_rows(
                source,
                table=table,
                columns=columns,
                primary_key=primary_key,
            )
            target_table_rows = _read_rows(
                target,
                table=table,
                columns=columns,
                primary_key=primary_key,
            )
            source_rows[table] = source_table_rows
            before[table] = {
                "source_row_count": len(source_table_rows),
                "target_row_count": len(target_table_rows),
                "source_sha256": digest_rows(source_table_rows),
                "target_sha256": digest_rows(target_table_rows),
            }

        with target.cursor() as cursor:
            cursor.execute("SET SESSION sql_mode=CONCAT_WS(',', @@sql_mode, 'TIME_TRUNCATE_FRACTIONAL')")
        for table, rows in source_rows.items():
            columns, _ = metadata[table]
            _replace_rows(target, table=table, columns=columns, rows=rows)
        target.commit()
        committed = True

        tables: dict[str, dict[str, object]] = {}
        for table, rows in source_rows.items():
            columns, primary_key = metadata[table]
            repaired = _read_rows(
                target,
                table=table,
                columns=columns,
                primary_key=primary_key,
            )
            source_hash = digest_rows(rows)
            repaired_hash = digest_rows(repaired)
            if len(rows) != len(repaired) or source_hash != repaired_hash:
                raise CompatibilityRepairError(f"repair postcondition failed for {table}")
            tables[f"{SCHEMA}.{table}"] = {
                **before[table],
                "repaired_row_count": len(repaired),
                "repaired_sha256": repaired_hash,
                "matches_frozen_source": True,
            }
        payload = seal_document(
            {
                "schema_version": 1,
                "tool": "repair_mysql84_fractional_datetime_compat",
                "status": "success",
                "source": {
                    "version": source_identity["version"],
                    "port": source_identity["port"],
                },
                "target": {
                    "version": target_identity["version"],
                    "port": target_identity["port"],
                    "server_uuid": target_identity["server_uuid"],
                    "tls": bool(target_identity["ssl_cipher"]),
                },
                "reason": "mysql55_truncates_mysql84_rounds_fractional_datetime_zero",
                "transaction_committed": True,
                "tables": tables,
                "all_tables_match_frozen_source": True,
                "secrets_in_evidence": False,
            }
        )
        atomic_write_json(args.evidence, payload)
        return payload
    except Exception:
        if not committed:
            target.rollback()
        raise
    finally:
        source.rollback()
        source.close()
        target.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-option-file", type=Path, required=True)
    parser.add_argument("--target-option-file", type=Path, required=True)
    parser.add_argument("--target-ssl-ca", type=Path, required=True)
    parser.add_argument("--expected-target-uuid", required=True)
    parser.add_argument("--expected-target-port", type=int, required=True)
    parser.add_argument("--expected-target-datadir", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--apply-ack", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        run(args)
    except (CompatibilityRepairError, ManifestError, OSError, pymysql.MySQLError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
