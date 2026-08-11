#!/usr/bin/env python3
"""Compare MySQL 5.5 and 8.4 logical schemas by business semantics.

The audit deliberately normalizes only metadata changes that cannot affect
stored values or query behavior:

* integer display widths removed by MySQL 8;
* MySQL 8's informational ``DEFAULT_GENERATED`` marker; and
* ``NO ACTION`` versus ``RESTRICT`` foreign-key labels (equivalent in MySQL).

Defaults are compared as raw bytes (HEX), collations remain exact, and trigger
bodies are compared byte-for-byte after removing only the SQL mode that MySQL
8 removed.  Passwords are read from protected MySQL client option files and
are never included in the JSON report.

An explicit reviewed V2/V3/V4 source-projection mode is also available for a
target on which those forward-only migrations already ran.  In that mode every
source object must still exist with identical semantics, while target-only
objects are reported as allowed additions.  Column ordinals are ignored only
because reviewed migrations insert new columns between existing columns.
"""

from __future__ import annotations

import argparse
import configparser
import hashlib
import json
import os
import re
import sys
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pymysql


DEFAULT_SCHEMAS = ("biga", "probiga", "probiga_qmt_history")
REMOVED_SQL_MODES = frozenset({"NO_AUTO_CREATE_USER"})
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z0-9_]+$")
_INTEGER_DISPLAY_WIDTH_RE = re.compile(
    r"^(tinyint|smallint|mediumint|int|bigint)\([0-9]+\)",
    re.IGNORECASE,
)

_REVIEWED_POST_MIGRATION_SOURCE_REPLACEMENTS = {
    "indexes": frozenset(
        {
            "probiga|st_opportunity_recall_v3|uk_v3_recall|1|trade_date|<NULL>|0|BTREE|A",
            "probiga|st_opportunity_recall_v3|uk_v3_recall|2|horizon_days|<NULL>|0|BTREE|A",
            "probiga|st_opportunity_recall_v3|uk_v3_recall|3|winner_threshold_pct|<NULL>|0|BTREE|A",
        }
    ),
    "table_constraints": frozenset(
        {"probiga|st_opportunity_recall_v3|uk_v3_recall|UNIQUE"}
    ),
}


@dataclass(frozen=True)
class EndpointIdentity:
    host: str
    port: int
    server_version: str
    server_version_comment: str
    server_uuid: str | None
    ssl_cipher: str


@dataclass(frozen=True)
class ObjectComparison:
    name: str
    source_count: int
    target_count: int
    source_sha256: str
    target_sha256: str
    difference_count: int
    allowed_target_only_count: int
    allowed_source_replacement_count: int
    source_only_sample: tuple[str, ...]
    target_only_sample: tuple[str, ...]


@dataclass(frozen=True)
class SchemaAuditReport:
    source: EndpointIdentity
    target: EndpointIdentity
    schemas: tuple[str, ...]
    comparison_mode: str
    comparisons: tuple[ObjectComparison, ...]
    target_check_constraints: tuple[str, ...]
    semantic_match: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _strip_option_value(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def read_client_options(path: Path) -> dict[str, Any]:
    """Read the minimal connection fields from a MySQL client option file."""

    resolved = path.expanduser().resolve(strict=True)
    parser = configparser.RawConfigParser(interpolation=None, strict=False)
    with resolved.open("r", encoding="utf-8-sig") as stream:
        parser.read_file(stream)
    if not parser.has_section("client"):
        raise ValueError(f"missing [client] section: {resolved}")

    def option(name: str, default: str = "") -> str:
        return _strip_option_value(parser.get("client", name, fallback=default))

    host = option("host", "127.0.0.1")
    user = option("user")
    password = option("password")
    try:
        port = int(option("port", "3306"))
    except ValueError as exc:
        raise ValueError(f"invalid client port in {resolved}") from exc
    if not host or not user or not password:
        raise ValueError(f"incomplete client credentials in {resolved}")
    return {"host": host, "port": port, "user": user, "password": password}


def normalize_column_type(value: object) -> str:
    return _INTEGER_DISPLAY_WIDTH_RE.sub(r"\1", str(value or ""), count=1)


def normalize_extra(value: object) -> str:
    tokens = str(value or "").replace("DEFAULT_GENERATED", " ").split()
    return " ".join(tokens).casefold()


def normalize_sql_mode(value: object) -> str:
    modes = [
        token.strip().upper()
        for token in str(value or "").split(",")
        if token.strip() and token.strip().upper() not in REMOVED_SQL_MODES
    ]
    return ",".join(modes)


def normalize_referential_rule(value: object) -> str:
    normalized = str(value or "").strip().upper()
    return "RESTRICT" if normalized == "NO ACTION" else normalized


_REVIEWED_ZERO_DATETIME_DEFAULTS = frozenset(
    {
        ("probiga", "jq_strategy_meta", "created_at"),
        ("probiga", "jq_strategy_meta", "updated_at"),
        ("probiga", "jq_strategy_picks", "created_at"),
        ("probiga", "st_daily_review", "etl_sync_at"),
        ("probiga", "st_portfolio_analysis_log", "created_at"),
        ("probiga", "st_portfolio_trans_log", "created_at"),
        ("probiga", "st_recommended_stocks", "created_at"),
        ("probiga", "st_user_portfolio", "etl_sync_at"),
    }
)
_ZERO_DATETIME_HEX = "0000-00-00 00:00:00".encode("ascii").hex().upper()
_SAFE_CURRENT_TIMESTAMP_HEX = frozenset(
    value.encode("ascii").hex().upper()
    for value in ("CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP()", "now()", "NOW()")
)
_REVIEWED_DATETIME_DEFAULT_TOKEN = "<REVIEWED_MYSQL84_SAFE_DATETIME_DEFAULT>"


def normalize_reviewed_datetime_default(
    row: tuple[object, ...], *, target: bool
) -> tuple[object, ...]:
    """Treat only the eight reviewed forward-only default repairs as equivalent."""

    if len(row) != 11:
        raise ValueError("normalized column row has an unexpected width")
    key = tuple(str(value).casefold() for value in row[:3])
    if key not in _REVIEWED_ZERO_DATETIME_DEFAULTS:
        return row
    default_hex = str(row[7]).upper()
    allowed = {_ZERO_DATETIME_HEX}
    if target:
        allowed.update(_SAFE_CURRENT_TIMESTAMP_HEX)
    if default_hex not in allowed:
        return row
    return (*row[:7], _REVIEWED_DATETIME_DEFAULT_TOKEN, *row[8:])


def _canonical_value(value: object) -> str:
    if value is None:
        return "<NULL>"
    if isinstance(value, bytes):
        return value.hex().upper()
    return str(value)


def _canonical_row(row: Sequence[object]) -> str:
    return "|".join(_canonical_value(value) for value in row)


def _hash_rows(rows: Iterable[str]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(row.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def compare_rows(
    name: str,
    source_rows: Iterable[str],
    target_rows: Iterable[str],
    *,
    allow_target_superset: bool = False,
    allowed_source_replacements: Iterable[str] = (),
) -> ObjectComparison:
    source = tuple(sorted(source_rows))
    target = tuple(sorted(target_rows))
    source_set = set(source)
    target_set = set(target)
    reviewed_replacements = set(allowed_source_replacements)
    if reviewed_replacements - source_set:
        raise ValueError(f"reviewed {name} replacement is absent from the source")
    allowed_source_only = (source_set - target_set) & reviewed_replacements
    source_only = tuple(sorted((source_set - target_set) - allowed_source_only))
    target_only = tuple(sorted(target_set - source_set))
    return ObjectComparison(
        name=name,
        source_count=len(source),
        target_count=len(target),
        source_sha256=_hash_rows(source),
        target_sha256=_hash_rows(target),
        difference_count=len(source_only) + (0 if allow_target_superset else len(target_only)),
        allowed_target_only_count=len(target_only) if allow_target_superset else 0,
        allowed_source_replacement_count=len(allowed_source_only),
        source_only_sample=source_only[:20],
        target_only_sample=target_only[:20],
    )


def _connect(option_file: Path, *, ssl_ca: Path | None) -> pymysql.Connection:
    options = read_client_options(option_file)
    kwargs: dict[str, Any] = {
        **options,
        "charset": "utf8mb4",
        "autocommit": True,
        "connect_timeout": 10,
        "read_timeout": 60,
        "write_timeout": 60,
    }
    if ssl_ca is not None:
        kwargs["ssl"] = {
            "ca": str(ssl_ca.expanduser().resolve(strict=True)),
            "check_hostname": False,
        }
    return pymysql.connect(**kwargs)


def _fetch_rows(
    connection: pymysql.Connection,
    sql: str,
    schemas: Sequence[str],
) -> list[tuple[object, ...]]:
    placeholders = ",".join(["%s"] * len(schemas))
    rendered = sql.format(schema_placeholders=placeholders)
    with connection.cursor() as cursor:
        cursor.execute(rendered, tuple(schemas))
        return [tuple(row) for row in cursor.fetchall()]


def _identity(connection: pymysql.Connection) -> EndpointIdentity:
    with connection.cursor() as cursor:
        cursor.execute("SELECT @@version, @@version_comment, @@port")
        version, version_comment, port = cursor.fetchone()
        try:
            cursor.execute("SELECT @@server_uuid")
            server_uuid = str(cursor.fetchone()[0]).strip().lower()
        except pymysql.MySQLError as exc:
            if exc.args and int(exc.args[0]) == 1193:
                server_uuid = None
            else:
                raise
        cursor.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
        ssl_row = cursor.fetchone()
    return EndpointIdentity(
        host=str(connection.host),
        port=int(port),
        server_version=str(version),
        server_version_comment=str(version_comment),
        server_uuid=server_uuid,
        ssl_cipher=str(ssl_row[1] if ssl_row else ""),
    )


_SCHEMA_QUERIES: tuple[
    tuple[str, str, Callable[[tuple[object, ...]], tuple[object, ...]]], ...
] = (
    (
        "schemas",
        """SELECT schema_name, default_character_set_name, default_collation_name
             FROM information_schema.schemata
            WHERE schema_name IN ({schema_placeholders})""",
        lambda row: row,
    ),
    (
        "tables",
        """SELECT table_schema, table_name, table_type,
                  COALESCE(engine, '<NULL>'), COALESCE(table_collation, '<NULL>')
             FROM information_schema.tables
            WHERE table_schema IN ({schema_placeholders})""",
        lambda row: row,
    ),
    (
        "columns",
        """SELECT table_schema, table_name, column_name, ordinal_position,
                  data_type, column_type, is_nullable,
                  COALESCE(HEX(CAST(column_default AS BINARY)), '<NULL>'),
                  extra, COALESCE(character_set_name, '<NULL>'),
                  COALESCE(collation_name, '<NULL>')
             FROM information_schema.columns
            WHERE table_schema IN ({schema_placeholders})""",
        lambda row: (
            *row[:5],
            normalize_column_type(row[5]),
            row[6],
            row[7],
            normalize_extra(row[8]),
            row[9],
            row[10],
        ),
    ),
    (
        "indexes",
        """SELECT table_schema, table_name, index_name, seq_in_index,
                  column_name, COALESCE(sub_part, '<NULL>'), non_unique,
                  index_type, COALESCE(collation, '<NULL>')
             FROM information_schema.statistics
            WHERE table_schema IN ({schema_placeholders})""",
        lambda row: row,
    ),
    (
        "table_constraints",
        """SELECT constraint_schema, table_name, constraint_name, constraint_type
             FROM information_schema.table_constraints
            WHERE constraint_schema IN ({schema_placeholders})
              AND constraint_type <> 'CHECK'""",
        lambda row: row,
    ),
    (
        "referential_constraints",
        """SELECT constraint_schema, table_name, constraint_name,
                  unique_constraint_schema, unique_constraint_name,
                  match_option, update_rule, delete_rule
             FROM information_schema.referential_constraints
            WHERE constraint_schema IN ({schema_placeholders})""",
        lambda row: (
            *row[:6],
            normalize_referential_rule(row[6]),
            normalize_referential_rule(row[7]),
        ),
    ),
    (
        "triggers",
        """SELECT trigger_schema, trigger_name, event_manipulation,
                  event_object_table, action_orientation, action_timing,
                  HEX(action_statement), sql_mode, definer,
                  character_set_client, collation_connection,
                  database_collation
             FROM information_schema.triggers
            WHERE trigger_schema IN ({schema_placeholders})""",
        lambda row: (*row[:7], normalize_sql_mode(row[7]), *row[8:]),
    ),
)


def _object_rows(
    connection: pymysql.Connection,
    schemas: Sequence[str],
    object_name: str,
    sql: str,
    normalizer: Callable[[tuple[object, ...]], tuple[object, ...]],
    *,
    target: bool,
    ignore_column_ordinal: bool = False,
) -> tuple[str, ...]:
    rows: list[str] = []
    for row in _fetch_rows(connection, sql, schemas):
        normalized = normalizer(row)
        if object_name == "columns":
            normalized = normalize_reviewed_datetime_default(
                normalized, target=target
            )
            if ignore_column_ordinal:
                normalized = (*normalized[:3], "<SOURCE_PROJECTION_ORDINAL>", *normalized[4:])
        rows.append(_canonical_row(normalized))
    return tuple(rows)


def audit_schema(
    source: pymysql.Connection,
    target: pymysql.Connection,
    *,
    schemas: Sequence[str],
    expected_source_version: str,
    expected_target_version: str,
    expected_target_uuid: str,
    comparison_mode: str = "exact",
) -> SchemaAuditReport:
    normalized_schemas = tuple(str(schema).strip() for schema in schemas)
    if not normalized_schemas or any(
        not _IDENTIFIER_RE.fullmatch(schema) for schema in normalized_schemas
    ):
        raise ValueError("schemas must be non-empty simple identifiers")
    if len(set(normalized_schemas)) != len(normalized_schemas):
        raise ValueError("schemas must not contain duplicates")
    if comparison_mode not in {"exact", "reviewed_v2_v3_v4_source_projection"}:
        raise ValueError("unsupported schema comparison mode")
    allow_target_superset = comparison_mode == "reviewed_v2_v3_v4_source_projection"

    source_identity = _identity(source)
    target_identity = _identity(target)
    if source_identity.server_version != expected_source_version:
        raise RuntimeError(
            f"source version mismatch: {source_identity.server_version}"
        )
    if target_identity.server_version != expected_target_version:
        raise RuntimeError(
            f"target version mismatch: {target_identity.server_version}"
        )
    if target_identity.server_uuid != expected_target_uuid.strip().lower():
        raise RuntimeError("target server UUID mismatch")
    if not target_identity.ssl_cipher:
        raise RuntimeError("target schema audit connection is not using TLS")

    comparisons = tuple(
        compare_rows(
            name,
            _object_rows(
                source,
                normalized_schemas,
                name,
                sql,
                normalizer,
                target=False,
                ignore_column_ordinal=allow_target_superset and name == "columns",
            ),
            _object_rows(
                target,
                normalized_schemas,
                name,
                sql,
                normalizer,
                target=True,
                ignore_column_ordinal=allow_target_superset and name == "columns",
            ),
            allow_target_superset=allow_target_superset,
            allowed_source_replacements=(
                _REVIEWED_POST_MIGRATION_SOURCE_REPLACEMENTS.get(name, frozenset())
                if allow_target_superset
                else frozenset()
            ),
        )
        for name, sql, normalizer in _SCHEMA_QUERIES
    )
    check_rows = _fetch_rows(
        target,
        """SELECT constraint_schema, table_name, constraint_name, enforced
             FROM information_schema.table_constraints
            WHERE constraint_schema IN ({schema_placeholders})
              AND constraint_type = 'CHECK'""",
        normalized_schemas,
    )
    target_checks = tuple(sorted(_canonical_row(row) for row in check_rows))
    return SchemaAuditReport(
        source=source_identity,
        target=target_identity,
        schemas=normalized_schemas,
        comparison_mode=comparison_mode,
        comparisons=comparisons,
        target_check_constraints=target_checks,
        semantic_match=all(item.difference_count == 0 for item in comparisons),
    )


def _write_report(path: Path, report: SchemaAuditReport, *, overwrite: bool) -> None:
    resolved = path.expanduser().resolve()
    if resolved.exists() and not overwrite:
        raise FileExistsError(f"output already exists: {resolved}")
    resolved.parent.mkdir(parents=True, exist_ok=True)
    partial = resolved.with_name(f".{resolved.name}.partial-{os.getpid()}")
    if partial.exists():
        raise FileExistsError(f"partial output already exists: {partial}")
    try:
        with partial.open("x", encoding="utf-8", newline="\n") as stream:
            json.dump(report.to_dict(), stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(partial, resolved)
    finally:
        if partial.exists():
            partial.unlink()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-option-file", type=Path, required=True)
    parser.add_argument("--target-option-file", type=Path, required=True)
    parser.add_argument("--target-ssl-ca", type=Path, required=True)
    parser.add_argument("--expected-source-version", default="5.5.20")
    parser.add_argument("--expected-target-version", default="8.4.11")
    parser.add_argument("--expected-target-uuid", required=True)
    parser.add_argument(
        "--comparison-mode",
        choices=("exact", "reviewed_v2_v3_v4_source_projection"),
        default="exact",
    )
    parser.add_argument("--schema", action="append", dest="schemas")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    schemas = tuple(args.schemas or DEFAULT_SCHEMAS)
    try:
        with _connect(args.source_option_file, ssl_ca=None) as source:
            with _connect(args.target_option_file, ssl_ca=args.target_ssl_ca) as target:
                report = audit_schema(
                    source,
                    target,
                    schemas=schemas,
                    expected_source_version=args.expected_source_version,
                    expected_target_version=args.expected_target_version,
                    expected_target_uuid=args.expected_target_uuid,
                    comparison_mode=args.comparison_mode,
                )
        if args.output is not None:
            _write_report(args.output, report, overwrite=args.overwrite)
    except (OSError, ValueError, RuntimeError, pymysql.MySQLError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True))
    return 0 if report.semantic_match else 1


if __name__ == "__main__":
    raise SystemExit(main())
