"""Forward-only repair of legacy zero ``DATETIME`` defaults on MySQL 8.4.

The MySQL 5.5 schema contains eight columns whose declared default is an
all-zero legacy ``DATETIME`` value.  MySQL 8.4 can restore that metadata when strict SQL
mode is relaxed for the import, but the legacy default is not suitable for the
strict production target.  This module changes only those frozen columns to a
``CURRENT_TIMESTAMP`` default; historical migration text is never rewritten.

The apply path is deliberately fail-closed.  It verifies the exact server,
schema, column type, nullability, and existing default before issuing DDL.  It
then audits every target column through ``CAST(... AS CHAR)`` so zero or
partially-zero calendar components are detected without asking MySQL to parse a
zero-date literal.  Any offending row blocks the whole batch without a DDL or
silent data backfill.  Business writers must be offline while applying.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import re
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Connection

from server.common.mysql_version_policy import (
    MYSQL_84_ISOLATED_ACCEPTANCE,
    is_oracle_mysql_distribution,
    isolated_acceptance_version,
)


MYSQL84_DATETIME_TARGET_VERSIONS = frozenset(
    {MYSQL_84_ISOLATED_ACCEPTANCE}
)
LEGACY_ZERO_DATETIME_DEFAULT = "0000-00-00 00:00:00"  # mysql84-zero-date-audit-only
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CANONICAL_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class DateTimeDefaultSpec:
    table_name: str
    column_name: str
    nullable: bool

    @property
    def key(self) -> str:
        return f"{self.table_name}.{self.column_name}"

    def alter_clause(self) -> str:
        return (
            f"ALTER COLUMN {_quote_identifier(self.column_name)} "
            "SET DEFAULT (CURRENT_TIMESTAMP)"
        )


@dataclass(frozen=True, slots=True)
class DateTimeColumnMetadata:
    table_name: str
    column_name: str
    data_type: str
    column_type: str
    nullable: bool
    column_default: object

    @property
    def key(self) -> str:
        return f"{self.table_name}.{self.column_name}"


@dataclass(frozen=True, slots=True)
class DateTimeViolationCount:
    column_key: str
    all_zero_count: int
    partial_zero_count: int

    @property
    def total(self) -> int:
        return self.all_zero_count + self.partial_zero_count

    def as_dict(self) -> dict[str, Any]:
        return {
            "column": self.column_key,
            "all_zero_count": self.all_zero_count,
            "partial_zero_count": self.partial_zero_count,
            "total": self.total,
        }


@dataclass(frozen=True, slots=True)
class DateTimeDefaultMaterializationReport:
    server_version: str
    server_uuid: str
    server_port: int
    schema: str
    manifest_sha256: str
    expected_column_count: int
    legacy_default_count_before: int
    current_timestamp_count_before: int
    changed_columns: tuple[str, ...]
    violation_counts: tuple[DateTimeViolationCount, ...]
    ready_to_apply: bool
    complete: bool
    applied: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "server_version": self.server_version,
            "server_uuid": self.server_uuid,
            "server_port": self.server_port,
            "schema": self.schema,
            "manifest_sha256": self.manifest_sha256,
            "expected_column_count": self.expected_column_count,
            "legacy_default_count_before": self.legacy_default_count_before,
            "current_timestamp_count_before": (
                self.current_timestamp_count_before
            ),
            "changed_columns": list(self.changed_columns),
            "violation_counts": [
                violation.as_dict() for violation in self.violation_counts
            ],
            "ready_to_apply": self.ready_to_apply,
            "complete": self.complete,
            "applied": self.applied,
        }


_FROZEN_DATETIME_DEFAULT_SPECS = (
    DateTimeDefaultSpec("jq_strategy_meta", "created_at", True),
    DateTimeDefaultSpec("jq_strategy_meta", "updated_at", True),
    DateTimeDefaultSpec("jq_strategy_picks", "created_at", True),
    DateTimeDefaultSpec("st_daily_review", "etl_sync_at", False),
    DateTimeDefaultSpec(
        "st_portfolio_analysis_log", "created_at", False
    ),
    DateTimeDefaultSpec("st_portfolio_trans_log", "created_at", False),
    DateTimeDefaultSpec("st_recommended_stocks", "created_at", True),
    DateTimeDefaultSpec("st_user_portfolio", "etl_sync_at", False),
)


def declared_mysql84_datetime_defaults() -> tuple[DateTimeDefaultSpec, ...]:
    """Return the reviewed eight-column compatibility manifest."""

    return _FROZEN_DATETIME_DEFAULT_SPECS


def datetime_default_manifest_sha256(
    specs: Sequence[DateTimeDefaultSpec],
) -> str:
    payload = "\n".join(
        "\x1f".join(
            (
                spec.table_name,
                spec.column_name,
                "YES" if spec.nullable else "NO",
                "datetime",
                LEGACY_ZERO_DATETIME_DEFAULT,
                "CURRENT_TIMESTAMP",
            )
        )
        for spec in specs
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def _quote_identifier(identifier: str) -> str:
    if type(identifier) is not str or not _IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(f"unsafe MySQL identifier: {identifier!r}")
    return f"`{identifier}`"


def _validate_specs(
    specs: Sequence[DateTimeDefaultSpec],
) -> tuple[DateTimeDefaultSpec, ...]:
    normalized = tuple(specs)
    if not normalized:
        raise RuntimeError("no legacy DATETIME default columns were declared")
    keys: set[tuple[str, str]] = set()
    for spec in normalized:
        if type(spec) is not DateTimeDefaultSpec:
            raise TypeError("all DATETIME default specs must be exact specs")
        _quote_identifier(spec.table_name)
        _quote_identifier(spec.column_name)
        if type(spec.nullable) is not bool:
            raise TypeError("DATETIME spec nullable must be bool")
        key = (spec.table_name.casefold(), spec.column_name.casefold())
        if key in keys:
            raise ValueError(f"duplicate DATETIME default spec: {spec.key}")
        keys.add(key)
    return normalized


def _identity(connection: Connection, *, expected_schema: str) -> dict[str, Any]:
    dialect = str(
        getattr(getattr(connection, "dialect", None), "name", "") or ""
    ).lower()
    if dialect != "mysql":
        raise RuntimeError("DATETIME default repair requires Oracle MySQL")
    row = connection.execute(
        text(
            "SELECT @@version AS server_version, "
            "@@version_comment AS version_comment, "
            "@@server_uuid AS server_uuid, @@port AS server_port, "
            "DATABASE() AS current_schema"
        )
    ).mappings().first()
    if row is None:
        raise RuntimeError("MySQL server identity query returned no row")
    raw_version = str(row["server_version"] or "")
    version = isolated_acceptance_version(raw_version)
    if version != MYSQL_84_ISOLATED_ACCEPTANCE:
        allowed = ", ".join(sorted(MYSQL84_DATETIME_TARGET_VERSIONS))
        raise RuntimeError(
            "DATETIME default repair is fail-closed outside validated Oracle "
            f"MySQL {allowed}; observed {raw_version!r}"
        )
    if not is_oracle_mysql_distribution(raw_version, row["version_comment"]):
        raise RuntimeError("DATETIME default repair requires Oracle MySQL identity")
    schema = str(row["current_schema"] or "")
    if schema.casefold() != expected_schema.casefold():
        raise RuntimeError(
            f"connected schema {schema!r} is not expected schema "
            f"{expected_schema!r}"
        )
    return {
        "version": version,
        "uuid": str(row["server_uuid"] or ""),
        "port": int(row["server_port"]),
        "schema": schema,
    }


def _read_column_metadata(
    connection: Connection,
    *,
    schema: str,
    specs: Sequence[DateTimeDefaultSpec],
) -> dict[tuple[str, str], DateTimeColumnMetadata]:
    tables = tuple(dict.fromkeys(spec.table_name for spec in specs))
    parameters: dict[str, object] = {"schema": schema}
    placeholders: list[str] = []
    for index, table_name in enumerate(tables):
        name = f"table_{index:03d}"
        parameters[name] = table_name
        placeholders.append(f":{name}")
    rows = connection.execute(
        text(
            "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, "
            "IS_NULLABLE, COLUMN_DEFAULT FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME IN ("
            + ", ".join(placeholders)
            + ")"
        ),
        parameters,
    ).mappings().all()
    expected_keys = {
        (spec.table_name.casefold(), spec.column_name.casefold())
        for spec in specs
    }
    metadata: dict[tuple[str, str], DateTimeColumnMetadata] = {}
    for row in rows:
        key = (
            str(row["TABLE_NAME"]).casefold(),
            str(row["COLUMN_NAME"]).casefold(),
        )
        if key not in expected_keys:
            continue
        if key in metadata:
            raise RuntimeError("duplicate INFORMATION_SCHEMA column metadata")
        metadata[key] = DateTimeColumnMetadata(
            table_name=str(row["TABLE_NAME"]),
            column_name=str(row["COLUMN_NAME"]),
            data_type=str(row["DATA_TYPE"] or ""),
            column_type=str(row["COLUMN_TYPE"] or ""),
            nullable=str(row["IS_NULLABLE"] or "").upper() == "YES",
            column_default=row["COLUMN_DEFAULT"],
        )
    return metadata


def _default_kind(value: object) -> str:
    if isinstance(value, bytes):
        try:
            value = value.decode("ascii")
        except UnicodeDecodeError:
            return "drift"
    if not isinstance(value, str):
        return "drift"
    normalized = value.strip()
    if normalized == LEGACY_ZERO_DATETIME_DEFAULT:
        return "legacy_zero"
    compact = re.sub(r"\s+", "", normalized).casefold()
    while compact.startswith("(") and compact.endswith(")"):
        compact = compact[1:-1]
    # MySQL 8.4 stores ``SET DEFAULT (CURRENT_TIMESTAMP)`` in
    # INFORMATION_SCHEMA as ``now()``.  ``NOW()`` is the server's exact
    # synonym for CURRENT_TIMESTAMP, not a broader arbitrary expression.
    if compact in {"current_timestamp", "current_timestamp()", "now()"}:
        return "current_timestamp"
    return "drift"


def _validate_metadata(
    specs: Sequence[DateTimeDefaultSpec],
    metadata: Mapping[tuple[str, str], DateTimeColumnMetadata],
    *,
    require_current_timestamp: bool,
    baseline: Mapping[tuple[str, str], DateTimeColumnMetadata] | None = None,
) -> dict[str, str]:
    kinds: dict[str, str] = {}
    for spec in specs:
        key = (spec.table_name.casefold(), spec.column_name.casefold())
        current = metadata.get(key)
        if current is None:
            raise RuntimeError(f"missing target DATETIME column: {spec.key}")
        if current.data_type.casefold() != "datetime":
            raise RuntimeError(
                f"DATETIME column type drift for {spec.key}: "
                f"{current.data_type!r}"
            )
        if current.column_type.casefold() != "datetime":
            raise RuntimeError(
                f"DATETIME column definition drift for {spec.key}: "
                f"{current.column_type!r}"
            )
        if current.nullable is not spec.nullable:
            raise RuntimeError(
                f"DATETIME column nullability drift for {spec.key}"
            )
        kind = _default_kind(current.column_default)
        if require_current_timestamp:
            if kind != "current_timestamp":
                raise RuntimeError(
                    f"DATETIME default postcondition failed for {spec.key}"
                )
        elif kind == "drift":
            raise RuntimeError(
                f"DATETIME default drift for {spec.key}: "
                f"{current.column_default!r}"
            )
        if baseline is not None:
            before = baseline.get(key)
            if before is None:
                raise AssertionError("postcondition baseline is incomplete")
            if (
                current.data_type.casefold() != before.data_type.casefold()
                or current.column_type.casefold()
                != before.column_type.casefold()
                or current.nullable is not before.nullable
            ):
                raise RuntimeError(
                    "DATETIME repair changed type or nullability for "
                    f"{spec.key}"
                )
        kinds[spec.key] = kind
    return kinds


def _zero_component_expression(column_name: str) -> tuple[str, str]:
    column = _quote_identifier(column_name)
    rendered = f"CAST({column} AS CHAR)"
    full_zero = f"LEFT({rendered}, 10) = '0000-00-00'"  # mysql84-zero-date-audit-only
    any_zero = (
        f"(LEFT({rendered}, 4) = '0000' OR "
        f"SUBSTRING({rendered}, 6, 2) = '00' OR "
        f"SUBSTRING({rendered}, 9, 2) = '00')"
    )
    partial_zero = f"(NOT ({full_zero}) AND {any_zero})"
    return full_zero, partial_zero


def _audit_zero_components(
    connection: Connection,
    specs: Sequence[DateTimeDefaultSpec],
) -> tuple[DateTimeViolationCount, ...]:
    by_table: dict[str, list[DateTimeDefaultSpec]] = {}
    for spec in specs:
        by_table.setdefault(spec.table_name, []).append(spec)
    violations: list[DateTimeViolationCount] = []
    for table_name, table_specs in by_table.items():
        projections = ["COUNT(*) AS `row_count`"]
        for index, spec in enumerate(table_specs):
            column = _quote_identifier(spec.column_name)
            full_zero, partial_zero = _zero_component_expression(
                spec.column_name
            )
            projections.extend(
                (
                    "COALESCE(SUM(CASE WHEN "
                    + column
                    + " IS NOT NULL AND "
                    + full_zero
                    + f" THEN 1 ELSE 0 END), 0) AS `all_zero_{index:03d}`",
                    "COALESCE(SUM(CASE WHEN "
                    + column
                    + " IS NOT NULL AND "
                    + partial_zero
                    + f" THEN 1 ELSE 0 END), 0) AS `partial_zero_{index:03d}`",
                )
            )
        row = connection.execute(
            text(
                "SELECT "
                + ", ".join(projections)
                + " FROM "
                + _quote_identifier(table_name)
            )
        ).mappings().first()
        if row is None:
            raise RuntimeError(f"DATETIME audit returned no row for {table_name}")
        for index, spec in enumerate(table_specs):
            all_zero = int(row[f"all_zero_{index:03d}"] or 0)
            partial_zero = int(row[f"partial_zero_{index:03d}"] or 0)
            if all_zero < 0 or partial_zero < 0:
                raise RuntimeError("DATETIME audit returned a negative row count")
            violations.append(
                DateTimeViolationCount(spec.key, all_zero, partial_zero)
            )
    order = {spec.key: index for index, spec in enumerate(specs)}
    return tuple(sorted(violations, key=lambda item: order[item.column_key]))


def _alter_statements(
    specs: Iterable[DateTimeDefaultSpec],
) -> tuple[tuple[str, tuple[DateTimeDefaultSpec, ...]], ...]:
    by_table: dict[str, list[DateTimeDefaultSpec]] = {}
    for spec in specs:
        by_table.setdefault(spec.table_name, []).append(spec)
    statements: list[tuple[str, tuple[DateTimeDefaultSpec, ...]]] = []
    for table_name, table_specs in by_table.items():
        frozen = tuple(table_specs)
        sql = (
            "ALTER TABLE "
            + _quote_identifier(table_name)
            + " "
            + ", ".join(spec.alter_clause() for spec in frozen)
        )
        statements.append((sql, frozen))
    return tuple(statements)


def _entry_transaction_is_clean(connection: Connection) -> bool:
    probe = getattr(connection, "in_transaction", None)
    return not callable(probe) or not bool(probe())


def _commit(connection: Connection) -> None:
    commit = getattr(connection, "commit", None)
    if callable(commit):
        commit()


def _rollback(connection: Connection) -> None:
    rollback = getattr(connection, "rollback", None)
    if callable(rollback):
        rollback()


def materialize_mysql84_datetime_defaults(
    connection: Connection,
    *,
    expected_schema: str,
    expected_server_uuid: str,
    expected_server_port: int,
    apply: bool = False,
    restored_target_offline: bool = False,
    specs: Sequence[DateTimeDefaultSpec] | None = None,
) -> DateTimeDefaultMaterializationReport:
    """Audit or repair the eight legacy DATETIME defaults on MySQL 8.4.11."""

    _quote_identifier(expected_schema)
    if not _entry_transaction_is_clean(connection):
        raise RuntimeError("DATETIME default repair requires a clean connection")
    if apply and restored_target_offline is not True:
        raise RuntimeError(
            "applying DATETIME defaults requires explicit restored-target "
            "offline confirmation"
        )
    normalized_uuid = str(expected_server_uuid or "").strip().lower()
    if not _CANONICAL_UUID_RE.fullmatch(normalized_uuid):
        raise ValueError("expected_server_uuid must be a canonical UUID")
    if (
        type(expected_server_port) is not int
        or not 1 <= expected_server_port <= 65535
    ):
        raise ValueError("expected_server_port must be an integer from 1 to 65535")
    expected = _validate_specs(
        tuple(specs) if specs is not None else declared_mysql84_datetime_defaults()
    )

    identity = _identity(connection, expected_schema=expected_schema)
    if identity["uuid"].strip().lower() != normalized_uuid:
        raise RuntimeError("DATETIME default repair server UUID mismatch")
    if identity["port"] != expected_server_port:
        raise RuntimeError("DATETIME default repair server port mismatch")

    before = _read_column_metadata(
        connection,
        schema=identity["schema"],
        specs=expected,
    )
    kinds_before = _validate_metadata(
        expected,
        before,
        require_current_timestamp=False,
    )
    violations = _audit_zero_components(connection, expected)
    _commit(connection)
    ready = not any(item.total for item in violations)
    changed: list[str] = []

    if apply and ready:
        pending = tuple(
            spec
            for spec in expected
            if kinds_before[spec.key] == "legacy_zero"
        )
        try:
            for statement, statement_specs in _alter_statements(pending):
                connection.execute(text(statement))
                _commit(connection)
                changed.extend(spec.key for spec in statement_specs)
        except Exception:
            _rollback(connection)
            raise

    after = _read_column_metadata(
        connection,
        schema=identity["schema"],
        specs=expected,
    )
    if apply and ready:
        kinds_after = _validate_metadata(
            expected,
            after,
            require_current_timestamp=True,
            baseline=before,
        )
    else:
        kinds_after = _validate_metadata(
            expected,
            after,
            require_current_timestamp=False,
            baseline=before,
        )
    _rollback(connection)
    complete = ready and all(
        kind == "current_timestamp" for kind in kinds_after.values()
    )
    return DateTimeDefaultMaterializationReport(
        server_version=identity["version"],
        server_uuid=identity["uuid"],
        server_port=identity["port"],
        schema=identity["schema"],
        manifest_sha256=datetime_default_manifest_sha256(expected),
        expected_column_count=len(expected),
        legacy_default_count_before=sum(
            kind == "legacy_zero" for kind in kinds_before.values()
        ),
        current_timestamp_count_before=sum(
            kind == "current_timestamp" for kind in kinds_before.values()
        ),
        changed_columns=tuple(changed),
        violation_counts=violations,
        ready_to_apply=ready,
        complete=complete,
        applied=apply,
    )


__all__ = [
    "LEGACY_ZERO_DATETIME_DEFAULT",
    "MYSQL84_DATETIME_TARGET_VERSIONS",
    "DateTimeColumnMetadata",
    "DateTimeDefaultMaterializationReport",
    "DateTimeDefaultSpec",
    "DateTimeViolationCount",
    "datetime_default_manifest_sha256",
    "declared_mysql84_datetime_defaults",
    "materialize_mysql84_datetime_defaults",
]
