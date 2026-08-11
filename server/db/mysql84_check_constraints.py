"""Forward-only materialization of V2 ``CHECK`` constraints on MySQL 8.4.

MySQL 5.5 and 5.7 parse, but do not enforce, table ``CHECK`` constraints.  A
logical dump produced by those servers can therefore omit constraints that are
present in the frozen V2 migration declarations.  This module reconstructs the
declarations without changing their text or checksums and provides an explicit
MySQL-8.4-only materialization boundary.

The write path is deliberately additive:

* missing constraints are added as ``NOT ENFORCED``;
* all stored rows are checked with MySQL's own expression evaluator;
* only a clean batch is switched to ``ENFORCED``;
* existing constraints are never dropped or rewritten.

Callers must use a dedicated connection.  Applying the plan also requires an
explicit assertion that the restored target is offline to business writers.
The final ``ALTER CHECK ... ENFORCED`` remains the authoritative race-safe
validation performed by MySQL.
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


MYSQL84_CHECK_TARGET_VERSIONS = frozenset({MYSQL_84_ISOLATED_ACCEPTANCE})
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CANONICAL_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
_CREATE_TABLE_RE = re.compile(
    r"\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?P<table>`?[A-Za-z_][A-Za-z0-9_]*`?)\s*\(",
    flags=re.IGNORECASE,
)
_CHECK_PREFIX_RE = re.compile(
    r"^\s*(?:CONSTRAINT\s+(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)\s+)?"
    r"CHECK\s*\(",
    flags=re.IGNORECASE | re.DOTALL,
)


@dataclass(frozen=True)
class CheckConstraintSpec:
    migration_version: str
    table_name: str
    ordinal: int
    constraint_name: str
    expression: str

    def add_not_enforced_sql(self) -> str:
        table = _quote_identifier(self.table_name)
        name = _quote_identifier(self.constraint_name)
        return (
            f"ALTER TABLE {table} ADD CONSTRAINT {name} "
            f"CHECK ({self.expression}) NOT ENFORCED"
        )

    def enforce_sql(self, *, constraint_name: str | None = None) -> str:
        table = _quote_identifier(self.table_name)
        name = _quote_identifier(constraint_name or self.constraint_name)
        return f"ALTER TABLE {table} ALTER CHECK {name} ENFORCED"


@dataclass(frozen=True)
class ExistingCheckConstraint:
    table_name: str
    constraint_name: str
    expression: str
    enforced: bool


@dataclass(frozen=True)
class ConstraintAction:
    spec: CheckConstraintSpec
    action: str
    existing_name: str | None = None


@dataclass(frozen=True)
class ConstraintPlan:
    actions: tuple[ConstraintAction, ...]
    missing_tables: tuple[str, ...]

    @property
    def applicable_count(self) -> int:
        return sum(action.action != "skip_missing_table" for action in self.actions)


@dataclass(frozen=True)
class CheckMaterializationReport:
    server_version: str
    server_uuid: str
    server_port: int
    schema: str
    manifest_sha256: str
    expected_constraint_count: int
    applicable_constraint_count: int
    missing_tables: tuple[str, ...]
    covered_before_count: int
    added_not_enforced: tuple[str, ...]
    enforced_constraints: tuple[str, ...]
    violation_counts: tuple[tuple[str, int], ...]
    complete: bool
    applied: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "server_version": self.server_version,
            "server_uuid": self.server_uuid,
            "server_port": self.server_port,
            "schema": self.schema,
            "manifest_sha256": self.manifest_sha256,
            "expected_constraint_count": self.expected_constraint_count,
            "applicable_constraint_count": self.applicable_constraint_count,
            "missing_tables": list(self.missing_tables),
            "covered_before_count": self.covered_before_count,
            "added_not_enforced": list(self.added_not_enforced),
            "enforced_constraints": list(self.enforced_constraints),
            "violation_counts": dict(self.violation_counts),
            "complete": self.complete,
            "applied": self.applied,
        }


def _quote_identifier(identifier: str) -> str:
    if type(identifier) is not str or not _IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(f"unsafe MySQL identifier: {identifier!r}")
    return f"`{identifier}`"


def _matching_parenthesis(sql: str, opening_index: int) -> int:
    """Return the matching close parenthesis while respecting SQL quoting."""

    if opening_index >= len(sql) or sql[opening_index] != "(":
        raise ValueError("opening_index must point to an opening parenthesis")
    depth = 0
    quote: str | None = None
    index = opening_index
    while index < len(sql):
        char = sql[index]
        if quote is not None:
            if char == "\\" and quote in {"'", '"'}:
                index += 2
                continue
            if char == quote:
                if index + 1 < len(sql) and sql[index + 1] == quote:
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char in {"'", '"', "`"}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index
            if depth < 0:
                break
        index += 1
    raise ValueError("unbalanced SQL parentheses")


def _split_top_level(body: str) -> tuple[str, ...]:
    parts: list[str] = []
    start = 0
    depth = 0
    quote: str | None = None
    index = 0
    while index < len(body):
        char = body[index]
        if quote is not None:
            if char == "\\" and quote in {"'", '"'}:
                index += 2
                continue
            if char == quote:
                if index + 1 < len(body) and body[index + 1] == quote:
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char in {"'", '"', "`"}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth < 0:
                raise ValueError("unbalanced SQL table body")
        elif char == "," and depth == 0:
            parts.append(body[start:index])
            start = index + 1
        index += 1
    if quote is not None or depth != 0:
        raise ValueError("unbalanced SQL table body")
    parts.append(body[start:])
    return tuple(parts)


def _constraint_name(table_name: str, ordinal: int, expression: str) -> str:
    digest = sha256(_canonical_expression(expression).encode("utf-8")).hexdigest()[:10]
    suffix = f"_{ordinal:03d}_{digest}"
    table_budget = 64 - len("ck84_") - len(suffix)
    name = f"ck84_{table_name[:table_budget]}{suffix}"
    if len(name) > 64 or not _IDENTIFIER_RE.fullmatch(name):
        raise AssertionError("generated MySQL CHECK name is invalid")
    return name


def extract_check_constraints(
    migrations: Iterable[Mapping[str, Any]],
) -> tuple[CheckConstraintSpec, ...]:
    """Extract stable named checks from frozen ``CREATE TABLE`` statements."""

    specs: list[CheckConstraintSpec] = []
    ordinal_by_table: dict[str, int] = {}
    for migration in migrations:
        version = str(migration["version"])
        for raw_statement in tuple(migration["statements"]):
            statement = str(raw_statement)
            table_match = _CREATE_TABLE_RE.search(statement)
            if table_match is None:
                continue
            table_name = table_match.group("table").strip("`")
            opening_index = table_match.end() - 1
            closing_index = _matching_parenthesis(statement, opening_index)
            body = statement[opening_index + 1 : closing_index]
            for part in _split_top_level(body):
                prefix = _CHECK_PREFIX_RE.match(part)
                if prefix is None:
                    continue
                check_open = part.find("(", prefix.start())
                check_close = _matching_parenthesis(part, check_open)
                if part[check_close + 1 :].strip():
                    raise ValueError(
                        f"unexpected text after CHECK in {version}.{table_name}"
                    )
                expression = part[check_open + 1 : check_close].strip()
                if not expression or ";" in expression or "/*" in expression:
                    raise ValueError(
                        f"unsafe CHECK expression in {version}.{table_name}"
                    )
                ordinal = ordinal_by_table.get(table_name, 0) + 1
                ordinal_by_table[table_name] = ordinal
                specs.append(
                    CheckConstraintSpec(
                        migration_version=version,
                        table_name=table_name,
                        ordinal=ordinal,
                        constraint_name=_constraint_name(
                            table_name, ordinal, expression
                        ),
                        expression=expression,
                    )
                )
    names = [spec.constraint_name.casefold() for spec in specs]
    if len(names) != len(set(names)):
        raise ValueError("generated CHECK constraint names are not unique")
    return tuple(specs)


def declared_v2_check_constraints() -> tuple[CheckConstraintSpec, ...]:
    # Import lazily so this additive compatibility layer cannot participate in
    # construction of the frozen V2 migration tuple.
    from server.db.migrations_v2 import MIGRATIONS

    return extract_check_constraints(MIGRATIONS)


def check_manifest_sha256(specs: Sequence[CheckConstraintSpec]) -> str:
    payload = "\n".join(
        "\x1f".join(
            (
                spec.migration_version,
                spec.table_name,
                str(spec.ordinal),
                spec.constraint_name,
                spec.expression,
            )
        )
        for spec in specs
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def _strip_outer_parentheses(expression: str) -> str:
    value = expression.strip()
    while value.startswith("("):
        try:
            close = _matching_parenthesis(value, 0)
        except ValueError:
            return value
        if close != len(value) - 1:
            return value
        value = value[1:-1].strip()
    return value


def _normalize_information_schema_literals(expression: str) -> str:
    # With the configured SQL mode, MySQL 8.4 may expose a string literal in
    # CHECK_CLAUSE as ``_utf8mb4\'VALUE\'`` or ``_ascii\'VALUE\'`` (the
    # backslashes are part of the
    # returned metadata text).  The frozen declaration contains the
    # equivalent plain ``'VALUE'``.  Constraint literals in this manifest do
    # not contain quote characters, so normalize only that narrow server form.
    return re.sub(
        r"_(?:utf8mb4|ascii)\\'([^']*)\\'",
        r"'\1'",
        expression,
        flags=re.IGNORECASE,
    )


def _canonical_expression(expression: str) -> str:
    """Conservatively normalize formatting emitted by INFORMATION_SCHEMA."""

    value = _normalize_information_schema_literals(expression)
    value = _strip_outer_parentheses(value)
    output: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(value):
        # INFORMATION_SCHEMA rewrites string literals with a charset
        # introducer (for example ``_utf8mb4'UNKNOWN'``), even when the DDL
        # declaration used a plain literal.  The restored schema is fixed to
        # utf8mb4, so this server-generated annotation is not a semantic
        # difference.  Normalize it only outside a quoted literal and only
        # when it immediately introduces that literal.
        introducer = "_utf8mb4"
        if (
            value[index : index + len(introducer)].casefold() == introducer
            and index + len(introducer) < len(value)
            and value[index + len(introducer)] in {"'", '"'}
        ):
            index += len(introducer)
            continue
        char = value[index]
        if quote is not None:
            output.append(char)
            if char == "\\" and quote in {"'", '"'} and index + 1 < len(value):
                output.append(value[index + 1])
                index += 2
                continue
            if char == quote:
                if index + 1 < len(value) and value[index + 1] == quote:
                    output.append(value[index + 1])
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char in {"'", '"'}:
            quote = char
            output.append(char)
        elif char == "`" or char.isspace():
            pass
        else:
            output.append(char.lower())
        index += 1
    return "".join(output)


def _top_level_boolean_parts(value: str, operator: str) -> tuple[str, ...] | None:
    """Split one SQL boolean operator outside strings and parentheses."""

    parts: list[str] = []
    start = 0
    index = 0
    depth = 0
    quote: str | None = None
    lowered = value.casefold()
    while index < len(value):
        char = value[index]
        if quote is not None:
            if char == "\\" and index + 1 < len(value):
                index += 2
                continue
            if char == quote:
                if index + 1 < len(value) and value[index + 1] == quote:
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char in {"'", '"'}:
            quote = char
            index += 1
            continue
        if char == "(":
            depth += 1
            index += 1
            continue
        if char == ")":
            depth = max(0, depth - 1)
            index += 1
            continue
        end = index + len(operator)
        if depth == 0 and lowered[index:end] == operator:
            before = value[index - 1] if index else " "
            after = value[end] if end < len(value) else " "
            if not (before.isalnum() or before == "_") and not (
                after.isalnum() or after == "_"
            ):
                parts.append(value[start:index])
                start = end
                index = end
                continue
        index += 1
    if not parts:
        return None
    parts.append(value[start:])
    return tuple(parts)


def _semantic_expression_ast(expression: str) -> str | tuple[str, tuple[Any, ...]]:
    value = _strip_outer_parentheses(expression.strip())
    for operator in ("or", "and"):
        parts = _top_level_boolean_parts(value, operator)
        if parts is None:
            continue
        children: list[Any] = []
        for part in parts:
            child = _semantic_expression_ast(part)
            if isinstance(child, tuple) and child[0] == operator:
                children.extend(child[1])
            else:
                children.append(child)
        return operator, tuple(children)
    return _canonical_expression(value)


def _semantic_expression_key(expression: str) -> str:
    """Normalize MySQL's redundant boolean grouping for comparisons only."""

    normalized = _normalize_information_schema_literals(expression)

    def render(node: str | tuple[str, tuple[Any, ...]]) -> str:
        if isinstance(node, str):
            return node
        operator, children = node
        return operator + "(" + ",".join(render(child) for child in children) + ")"

    return render(_semantic_expression_ast(normalized))


def plan_check_constraints(
    specs: Sequence[CheckConstraintSpec],
    *,
    present_tables: Iterable[str],
    existing_constraints: Sequence[ExistingCheckConstraint],
) -> ConstraintPlan:
    tables = {table.casefold() for table in present_tables}
    by_name = {
        constraint.constraint_name.casefold(): constraint
        for constraint in existing_constraints
    }
    by_table_expression: dict[tuple[str, str], list[ExistingCheckConstraint]] = {}
    for constraint in existing_constraints:
        key = (
            constraint.table_name.casefold(),
            _semantic_expression_key(constraint.expression),
        )
        by_table_expression.setdefault(key, []).append(constraint)

    actions: list[ConstraintAction] = []
    missing_tables: set[str] = set()
    for spec in specs:
        if spec.table_name.casefold() not in tables:
            missing_tables.add(spec.table_name)
            actions.append(ConstraintAction(spec, "skip_missing_table"))
            continue
        named = by_name.get(spec.constraint_name.casefold())
        if named is not None:
            if (
                named.table_name.casefold() != spec.table_name.casefold()
                or _semantic_expression_key(named.expression)
                != _semantic_expression_key(spec.expression)
            ):
                raise RuntimeError(
                    "MySQL 8.4 CHECK constraint name drift: "
                    f"{spec.constraint_name}"
                )
            if not named.enforced:
                enforced_equivalents = [
                    item
                    for item in by_table_expression.get(
                        (
                            spec.table_name.casefold(),
                            _semantic_expression_key(spec.expression),
                        ),
                        [],
                    )
                    if item.enforced
                ]
                if enforced_equivalents:
                    equivalent = sorted(
                        enforced_equivalents,
                        key=lambda item: item.constraint_name,
                    )[0]
                    actions.append(
                        ConstraintAction(spec, "covered", equivalent.constraint_name)
                    )
                    continue
            actions.append(
                ConstraintAction(
                    spec,
                    "covered" if named.enforced else "enforce_existing",
                    named.constraint_name,
                )
            )
            continue
        equivalents = by_table_expression.get(
            (spec.table_name.casefold(), _semantic_expression_key(spec.expression)),
            [],
        )
        if equivalents:
            equivalent = sorted(
                equivalents,
                key=lambda item: (not item.enforced, item.constraint_name),
            )[0]
            actions.append(
                ConstraintAction(
                    spec,
                    "covered" if equivalent.enforced else "enforce_existing",
                    equivalent.constraint_name,
                )
            )
        else:
            actions.append(ConstraintAction(spec, "add", spec.constraint_name))
    return ConstraintPlan(tuple(actions), tuple(sorted(missing_tables)))


def _identity(connection: Connection, *, expected_schema: str) -> dict[str, Any]:
    dialect = str(
        getattr(getattr(connection, "dialect", None), "name", "") or ""
    ).lower()
    if dialect != "mysql":
        raise RuntimeError("CHECK materialization requires Oracle MySQL")
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
        allowed = ", ".join(sorted(MYSQL84_CHECK_TARGET_VERSIONS))
        raise RuntimeError(
            "CHECK materialization is fail-closed outside validated Oracle "
            f"MySQL {allowed}; observed {raw_version!r}"
        )
    if not is_oracle_mysql_distribution(
        raw_version, row["version_comment"]
    ):
        raise RuntimeError("CHECK materialization requires Oracle MySQL identity")
    schema = str(row["current_schema"] or "")
    if schema.casefold() != expected_schema.casefold():
        raise RuntimeError(
            f"connected schema {schema!r} is not expected schema {expected_schema!r}"
        )
    return {
        "version": version,
        "uuid": str(row["server_uuid"] or ""),
        "port": int(row["server_port"]),
        "schema": schema,
    }


def _read_present_tables(
    connection: Connection, *, schema: str
) -> tuple[str, ...]:
    rows = connection.execute(
        text(
            "SELECT TABLE_NAME FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = :schema AND TABLE_TYPE = 'BASE TABLE'"
        ),
        {"schema": schema},
    ).mappings().all()
    return tuple(str(row["TABLE_NAME"]) for row in rows)


def _read_existing_constraints(
    connection: Connection, *, schema: str
) -> tuple[ExistingCheckConstraint, ...]:
    rows = connection.execute(
        text(
            "SELECT tc.TABLE_NAME, tc.CONSTRAINT_NAME, cc.CHECK_CLAUSE, "
            "tc.ENFORCED FROM information_schema.TABLE_CONSTRAINTS tc "
            "JOIN information_schema.CHECK_CONSTRAINTS cc "
            "ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA "
            "AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME "
            "WHERE tc.CONSTRAINT_SCHEMA = :schema "
            "AND tc.CONSTRAINT_TYPE = 'CHECK'"
        ),
        {"schema": schema},
    ).mappings().all()
    return tuple(
        ExistingCheckConstraint(
            table_name=str(row["TABLE_NAME"]),
            constraint_name=str(row["CONSTRAINT_NAME"]),
            expression=str(row["CHECK_CLAUSE"]),
            enforced=str(row["ENFORCED"]).upper() == "YES",
        )
        for row in rows
    )


def _audit_table_violations(
    connection: Connection,
    specs: Sequence[CheckConstraintSpec],
) -> dict[str, int]:
    by_table: dict[str, list[CheckConstraintSpec]] = {}
    for spec in specs:
        by_table.setdefault(spec.table_name, []).append(spec)
    violations: dict[str, int] = {}
    for table_name, table_specs in sorted(by_table.items()):
        projections = ["COUNT(*) AS `row_count`"]
        for index, spec in enumerate(table_specs):
            projections.append(
                "COALESCE(SUM(("
                + spec.expression
                + f") IS FALSE), 0) AS `violation_{index:03d}`"
            )
        sql = "SELECT " + ", ".join(projections) + " FROM " + _quote_identifier(
            table_name
        )
        row = connection.execute(text(sql)).mappings().first()
        if row is None:
            raise RuntimeError(f"CHECK audit returned no row for {table_name}")
        for index, spec in enumerate(table_specs):
            count = int(row[f"violation_{index:03d}"] or 0)
            if count < 0:
                raise RuntimeError("CHECK audit returned a negative row count")
            violations[spec.constraint_name] = count
    return violations


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


def materialize_mysql84_check_constraints(
    connection: Connection,
    *,
    expected_schema: str,
    expected_server_uuid: str | None = None,
    expected_server_port: int | None = None,
    apply: bool = False,
    restored_target_offline: bool = False,
    specs: Sequence[CheckConstraintSpec] | None = None,
) -> CheckMaterializationReport:
    """Audit or add/enforce V2 CHECK constraints on an exact MySQL 8.4 target."""

    _quote_identifier(expected_schema)
    if not _entry_transaction_is_clean(connection):
        raise RuntimeError("CHECK materialization requires a clean connection")
    if apply and restored_target_offline is not True:
        raise RuntimeError(
            "applying CHECK constraints requires explicit restored-target "
            "offline confirmation"
        )
    normalized_uuid = str(expected_server_uuid or "").strip().lower()
    if expected_server_uuid is not None and not _CANONICAL_UUID_RE.fullmatch(
        normalized_uuid
    ):
        raise ValueError("expected_server_uuid must be a canonical UUID")
    if expected_server_port is not None and (
        type(expected_server_port) is not int
        or not 1 <= expected_server_port <= 65535
    ):
        raise ValueError("expected_server_port must be an integer from 1 to 65535")
    if apply and (not normalized_uuid or expected_server_port is None):
        raise RuntimeError(
            "applying CHECK constraints requires explicit expected server UUID "
            "and port"
        )
    expected = tuple(specs or declared_v2_check_constraints())
    if not expected:
        raise RuntimeError("no declared V2 CHECK constraints were discovered")

    identity = _identity(connection, expected_schema=expected_schema)
    if normalized_uuid and identity["uuid"].strip().lower() != normalized_uuid:
        raise RuntimeError("CHECK materialization server UUID mismatch")
    if (
        expected_server_port is not None
        and identity["port"] != expected_server_port
    ):
        raise RuntimeError("CHECK materialization server port mismatch")
    present_tables = _read_present_tables(connection, schema=identity["schema"])
    existing = _read_existing_constraints(connection, schema=identity["schema"])
    plan = plan_check_constraints(
        expected,
        present_tables=present_tables,
        existing_constraints=existing,
    )
    applicable = tuple(
        action.spec
        for action in plan.actions
        if action.action != "skip_missing_table"
    )
    covered_before = sum(action.action == "covered" for action in plan.actions)
    added: list[str] = []
    enforced: list[str] = []

    if apply:
        _commit(connection)
        try:
            for action in plan.actions:
                if action.action != "add":
                    continue
                connection.execute(text(action.spec.add_not_enforced_sql()))
                _commit(connection)
                added.append(action.spec.constraint_name)
        except Exception:
            _rollback(connection)
            raise

    violations = _audit_table_violations(connection, applicable)
    _commit(connection)
    any_violations = any(count for count in violations.values())

    if apply and not any_violations:
        current = _read_existing_constraints(
            connection, schema=identity["schema"]
        )
        current_plan = plan_check_constraints(
            expected,
            present_tables=present_tables,
            existing_constraints=current,
        )
        _commit(connection)
        try:
            for action in current_plan.actions:
                if action.action != "enforce_existing":
                    continue
                if action.existing_name is None:
                    raise AssertionError("enforcement action has no constraint name")
                connection.execute(
                    text(
                        action.spec.enforce_sql(
                            constraint_name=action.existing_name
                        )
                    )
                )
                _commit(connection)
                enforced.append(action.existing_name)
        except Exception:
            _rollback(connection)
            raise

    final_existing = _read_existing_constraints(
        connection, schema=identity["schema"]
    )
    final_plan = plan_check_constraints(
        expected,
        present_tables=present_tables,
        existing_constraints=final_existing,
    )
    _rollback(connection)
    complete = not any_violations and all(
        action.action in {"covered", "skip_missing_table"}
        for action in final_plan.actions
    )
    return CheckMaterializationReport(
        server_version=identity["version"],
        server_uuid=identity["uuid"],
        server_port=identity["port"],
        schema=identity["schema"],
        manifest_sha256=check_manifest_sha256(expected),
        expected_constraint_count=len(expected),
        applicable_constraint_count=len(applicable),
        missing_tables=plan.missing_tables,
        covered_before_count=covered_before,
        added_not_enforced=tuple(added),
        enforced_constraints=tuple(enforced),
        violation_counts=tuple(sorted(violations.items())),
        complete=complete,
        applied=apply,
    )


__all__ = [
    "MYSQL84_CHECK_TARGET_VERSIONS",
    "CheckConstraintSpec",
    "CheckMaterializationReport",
    "ConstraintAction",
    "ConstraintPlan",
    "ExistingCheckConstraint",
    "check_manifest_sha256",
    "declared_v2_check_constraints",
    "extract_check_constraints",
    "materialize_mysql84_check_constraints",
    "plan_check_constraints",
]
