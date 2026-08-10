"""Read-only structural gate for the V2 execution-evidence extension.

This module never migrates a database and never opens, commits, or rolls back
a transaction.  It inspects the schema visible on a caller-owned connection
and compares it with the frozen V2 migration declarations.  A structurally
correct result is deliberately weaker than production approval: isolated
behavioral MySQL tests, least-privilege attestation, and writer wiring remain
separate fail-closed gates.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

from sqlalchemy import text

from server.common.mysql_metadata_compat import normalize_mysql_referential_rule
from server.common.mysql_version_policy import is_isolated_acceptance_version
from server.db.migrations_v2 import (
    MIGRATIONS,
    V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE,
    V2_EVIDENCE_MAINTENANCE_FENCE_DDL,
    V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE,
    V2_EVIDENCE_MAINTENANCE_FENCE_NAME,
    V2_EVIDENCE_MAINTENANCE_FENCE_TABLE,
    _checksum,
)


EVIDENCE_BINDING_MIGRATION = "20260803_011_v2_execution_evidence_bindings"
EVIDENCE_AUTHORITY_MIGRATION = (
    "20260803_014_v2_execution_authority_attestations"
)
EVIDENCE_ACCOUNTING_MIGRATION = (
    "20260803_015_v2_accounting_outcome_evidence"
)
EVIDENCE_TABLES = frozenset(
    {
        "st_market_calendar_evidence_v2",
        "st_quote_receipt_evidence_v2",
        "st_fill_execution_evidence_v2",
        "st_cash_event_binding_v2",
        "st_order_transition_v2",
    }
)
AUTHORITY_TABLES = frozenset(
    {
        "st_execution_authority_trust_key_v2",
        "st_execution_authority_receipt_v2",
        "st_execution_authority_key_revocation_v2",
        "st_execution_authority_receipt_revocation_v2",
        "st_execution_authority_attestation_v2",
    }
)
ACCOUNTING_EVIDENCE_TABLES = frozenset(
    {
        "st_fill_accounting_outcome_v2",
        "st_lot_transition_evidence_v2",
        "st_fill_accounting_outcome_finalization_v2",
    }
)
_CREATE_TABLE_RE = re.compile(
    r"\bCREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([a-z0-9_]+)",
    flags=re.IGNORECASE,
)
_FULL_COLUMN_RE = re.compile(
    r"^`?([A-Za-z0-9_]+)`?\s+"
    r"([A-Za-z]+(?:\([^)]*\))?(?:\s+UNSIGNED)?)(.*)$",
    flags=re.IGNORECASE | re.DOTALL,
)
_DEFAULT_RE = re.compile(
    r"\bDEFAULT\s+(CURRENT_TIMESTAMP(?:\(\d+\))?|'(?:''|[^'])*'|"
    r'"(?:""|[^"])*"|[^\s]+)',
    flags=re.IGNORECASE,
)
_COLUMN_CHARACTER_SET_RE = re.compile(
    r"\bCHARACTER\s+SET\s+([A-Za-z0-9_]+)",
    flags=re.IGNORECASE,
)
_COLUMN_COLLATION_RE = re.compile(
    r"\bCOLLATE\s+([A-Za-z0-9_]+)",
    flags=re.IGNORECASE,
)
_PRIMARY_KEY_RE = re.compile(
    r"^PRIMARY\s+KEY\s*\(([^)]*)\)$",
    flags=re.IGNORECASE,
)
_INDEX_RE = re.compile(
    r"^(UNIQUE\s+)?KEY\s+`?([A-Za-z0-9_]+)`?\s*\(([^)]*)\)$",
    flags=re.IGNORECASE,
)
_FOREIGN_KEY_RE = re.compile(
    r"^CONSTRAINT\s+`?([A-Za-z0-9_]+)`?\s+"
    r"FOREIGN\s+KEY\s*\(([^)]*)\)\s+"
    r"REFERENCES\s+`?([A-Za-z0-9_]+)`?\s*\(([^)]*)\)"
    r"(?:\s+ON\s+DELETE\s+([A-Za-z]+))?$",
    flags=re.IGNORECASE,
)
_TRIGGER_RE = re.compile(
    r"\bCREATE\s+TRIGGER\s+([a-z0-9_]+)\s+"
    r"BEFORE\s+(INSERT|UPDATE|DELETE)\s+ON\s+([a-z0-9_]+)",
    flags=re.IGNORECASE,
)
_TRIGGER_ORDER_RE = re.compile(
    r"\bFOR\s+EACH\s+ROW\s+(FOLLOWS|PRECEDES)\s+([a-z0-9_]+)\b",
    flags=re.IGNORECASE,
)
_REQUIRED_TRIGGER_SQL_MODES = frozenset(
    {
        "NO_ZERO_DATE",
        "NO_ZERO_IN_DATE",
        "ERROR_FOR_DIVISION_BY_ZERO",
    }
)
_EXPECTED_TRIGGER_FOLLOWS = {
    "trg_market_calendar_evidence_v2_authority_bi": (
        "trg_market_calendar_evidence_v2_guard_bi"
    ),
    "trg_quote_receipt_evidence_v2_authority_bi": (
        "trg_quote_receipt_evidence_v2_guard_bi"
    ),
}


class V2EvidenceSchemaInspectionError(ValueError):
    """Raised when the inspection boundary itself is used incorrectly."""


class V2EvidenceMaintenanceFenceError(RuntimeError):
    """Raised when a writer cannot prove that maintenance is inactive."""


@dataclass(frozen=True, slots=True)
class V2EvidenceSchemaReport:
    database_name: str
    server_version: str
    migration_versions: tuple[str, ...]
    observed_tables: tuple[str, ...]
    observed_triggers: tuple[str, ...]
    structural_blockers: tuple[str, ...]
    activation_blockers: tuple[str, ...]
    guards_checked: bool
    migration_ledger_checked: bool
    activation_checks_included: bool
    canonical_hash_audit_passed: bool
    phase_scoped_migration_replay: bool
    maintenance_fence_checked: bool
    maintenance_fence_active: bool

    @property
    def metadata_preflight_passed(self) -> bool:
        return not self.structural_blockers

    @property
    def schema_ready(self) -> bool:
        return (
            self.metadata_preflight_passed
            and self.guards_checked
            and self.migration_ledger_checked
            and self.activation_checks_included
            and not self.phase_scoped_migration_replay
            and self.maintenance_fence_checked
            and not self.maintenance_fence_active
            and not self.activation_blockers
        )

    @property
    def production_activation_allowed(self) -> bool:
        # This read-only metadata gate is never an activation authority.
        return False

    @property
    def actionable_output_allowed(self) -> bool:
        return False


def _migration(version: str) -> dict[str, Any] | None:
    return next(
        (item for item in MIGRATIONS if str(item["version"]) == version),
        None,
    )


def _normalize_column_type(value: object) -> str:
    normalized = " ".join(str(value).casefold().split())
    integer = re.fullmatch(
        r"(smallint|mediumint|int|integer|bigint)\(\d+\)( unsigned)?",
        normalized,
    )
    if integer is not None:
        return f"{integer.group(1)}{integer.group(2) or ''}"
    return normalized


def _normalize_declared_default(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    quoted = False
    if (
        len(normalized) >= 2
        and normalized[0] == normalized[-1]
        and normalized[0] in {"'", '"'}
    ):
        quoted = True
        quote = normalized[0]
        normalized = normalized[1:-1].replace(quote * 2, quote)
    expression = "".join(normalized.casefold().split())
    if not quoted and expression == "null":
        return None
    if re.fullmatch(r"current_timestamp(?:\(\d+\))?", expression):
        return expression
    return normalized


def _normalize_observed_default(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    expression = "".join(normalized.casefold().split())
    if re.fullmatch(r"current_timestamp(?:\(\d+\))?", expression):
        return expression
    return normalized


def _split_table_definitions(body: str) -> tuple[str, ...]:
    definitions: list[str] = []
    current: list[str] = []
    depth = 0
    quote: str | None = None
    escaped = False
    for character in body:
        if quote is not None:
            current.append(character)
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                quote = None
            continue
        if character in {"'", '"', "`"}:
            quote = character
            current.append(character)
        elif character == "(":
            depth += 1
            current.append(character)
        elif character == ")":
            depth -= 1
            current.append(character)
        elif character == "," and depth == 0:
            definition = " ".join("".join(current).split())
            if definition:
                definitions.append(definition)
            current = []
        else:
            current.append(character)
    definition = " ".join("".join(current).split())
    if definition:
        definitions.append(definition)
    return tuple(definitions)


def _identifier_list(value: str) -> tuple[str, ...]:
    return tuple(
        raw.strip().split()[0].strip("`") for raw in value.split(",")
    )


def _declared_table_schema_signature(
    migration_version: str,
    table_names: frozenset[str],
) -> dict[str, dict[str, Any]]:
    migration = _migration(migration_version)
    if migration is None:
        return {}
    return _declared_table_schema_signature_from_statements(
        tuple(migration["statements"]),
        table_names,
    )


def _declared_table_schema_signature_from_statements(
    statements: tuple[str, ...],
    table_names: frozenset[str],
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for statement in statements:
        match = _CREATE_TABLE_RE.search(statement)
        if match is None:
            continue
        table_name = match.group(1).lower()
        if table_name not in table_names:
            continue
        body = statement[statement.find("(") + 1 : statement.rfind(")")]
        columns: dict[str, dict[str, Any]] = {}
        indexes: dict[str, dict[str, Any]] = {}
        foreign_keys: dict[str, dict[str, Any]] = {}
        for definition in _split_table_definitions(body):
            primary_match = _PRIMARY_KEY_RE.match(definition)
            if primary_match is not None:
                indexes["PRIMARY"] = {
                    "unique": True,
                    "columns": _identifier_list(primary_match.group(1)),
                }
                continue
            index_match = _INDEX_RE.match(definition)
            if index_match is not None:
                indexes[index_match.group(2)] = {
                    "unique": bool(index_match.group(1)),
                    "columns": _identifier_list(index_match.group(3)),
                }
                continue
            foreign_match = _FOREIGN_KEY_RE.match(definition)
            if foreign_match is not None:
                foreign_keys[foreign_match.group(1)] = {
                    "columns": _identifier_list(foreign_match.group(2)),
                    "referenced_table": foreign_match.group(3),
                    "referenced_columns": _identifier_list(
                        foreign_match.group(4)
                    ),
                    "on_delete": normalize_mysql_referential_rule(
                        foreign_match.group(5) or "RESTRICT"
                    ),
                    "on_update": normalize_mysql_referential_rule("RESTRICT"),
                }
                continue
            column_match = _FULL_COLUMN_RE.match(definition)
            if column_match is None:
                continue
            column_name = column_match.group(1)
            remainder = column_match.group(3)
            default_match = _DEFAULT_RE.search(remainder)
            columns[column_name] = {
                "type": _normalize_column_type(column_match.group(2)),
                "nullable": not (
                    "NOT NULL" in remainder.upper()
                    or "PRIMARY KEY" in remainder.upper()
                ),
                "default": _normalize_declared_default(
                    default_match.group(1) if default_match else None
                ),
            }
            if "PRIMARY KEY" in remainder.upper():
                indexes["PRIMARY"] = {
                    "unique": True,
                    "columns": (column_name,),
                }
        result[table_name] = {
            "columns": columns,
            "indexes": indexes,
            "foreign_keys": foreign_keys,
        }
    return result


def _maintenance_fence_schema_signature() -> dict[str, dict[str, Any]]:
    return _declared_table_schema_signature_from_statements(
        (V2_EVIDENCE_MAINTENANCE_FENCE_DDL,),
        frozenset({V2_EVIDENCE_MAINTENANCE_FENCE_TABLE}),
    )


def _declared_column_collation_contracts(
    migration_version: str,
    table_names: frozenset[str],
) -> dict[str, dict[str, tuple[str, str]]]:
    """Return exact or prefix collation contracts from the frozen DDL."""

    migration = _migration(migration_version)
    if migration is None:
        return {}
    result: dict[str, dict[str, tuple[str, str]]] = {}
    for statement in tuple(migration["statements"]):
        match = _CREATE_TABLE_RE.search(statement)
        if match is None:
            continue
        table_name = match.group(1).lower()
        if table_name not in table_names:
            continue
        body = statement[statement.find("(") + 1 : statement.rfind(")")]
        columns: dict[str, tuple[str, str]] = {}
        for definition in _split_table_definitions(body):
            column_match = _FULL_COLUMN_RE.match(definition)
            if column_match is None:
                continue
            column_type = _normalize_column_type(column_match.group(2))
            if not column_type.startswith(("char", "varchar", "longtext")):
                continue
            remainder = column_match.group(3)
            collation_match = _COLUMN_COLLATION_RE.search(remainder)
            if collation_match is not None:
                columns[column_match.group(1)] = (
                    "exact",
                    collation_match.group(1).lower(),
                )
                continue
            character_set_match = _COLUMN_CHARACTER_SET_RE.search(remainder)
            character_set = (
                character_set_match.group(1).lower()
                if character_set_match is not None
                else "utf8mb4"
            )
            columns[column_match.group(1)] = (
                "prefix",
                f"{character_set}_",
            )
        result[table_name] = columns
    return result


def _binding_schema_signature(
    *,
    include_natural_keys: bool = True,
) -> dict[str, dict[str, Any]]:
    result = _declared_table_schema_signature(
        EVIDENCE_BINDING_MIGRATION,
        EVIDENCE_TABLES,
    )
    if include_natural_keys:
        natural_key_migration = _migration(
            "20260803_013_v2_execution_evidence_natural_keys"
        )
        if natural_key_migration is not None:
            for statement in tuple(natural_key_migration["statements"]):
                match = re.search(
                    r"\bALTER\s+TABLE\s+`?([A-Za-z0-9_]+)`?\s+"
                    r"ADD\s+UNIQUE\s+(?:KEY|INDEX)\s+"
                    r"`?([A-Za-z0-9_]+)`?\s*\(([^)]+)\)",
                    statement,
                    flags=re.IGNORECASE | re.DOTALL,
                )
                if match is None:
                    continue
                table_name = match.group(1).lower()
                if table_name not in result:
                    continue
                result[table_name]["indexes"][match.group(2)] = {
                    "unique": True,
                    "columns": _identifier_list(match.group(3)),
                }
    return result


def _authority_schema_signature() -> dict[str, dict[str, Any]]:
    return _declared_table_schema_signature(
        EVIDENCE_AUTHORITY_MIGRATION,
        AUTHORITY_TABLES,
    )


def _accounting_schema_signature() -> dict[str, dict[str, Any]]:
    return _declared_table_schema_signature(
        EVIDENCE_ACCOUNTING_MIGRATION,
        ACCOUNTING_EVIDENCE_TABLES,
    )


def _authority_trigger_contracts() -> dict[str, tuple[str, str]]:
    migration = _migration(EVIDENCE_AUTHORITY_MIGRATION)
    if migration is None:
        return {}
    result: dict[str, tuple[str, str]] = {}
    for statement in tuple(migration["statements"]):
        match = _TRIGGER_RE.search(statement)
        if match is None or match.group(3).lower() not in AUTHORITY_TABLES:
            continue
        name = match.group(1).lower()
        if name in result:
            raise RuntimeError(f"duplicate V2 authority trigger declaration: {name}")
        result[name] = (match.group(2).upper(), match.group(3).lower())
    return result


def _authority_trigger_bodies() -> dict[str, str]:
    migration = _migration(EVIDENCE_AUTHORITY_MIGRATION)
    if migration is None:
        return {}
    result: dict[str, str] = {}
    for statement in tuple(migration["statements"]):
        match = _TRIGGER_RE.search(statement)
        if match is None or match.group(3).lower() not in AUTHORITY_TABLES:
            continue
        body_match = re.search(
            r"\bFOR\s+EACH\s+ROW\s+"
            r"(?:(?:FOLLOWS|PRECEDES)\s+[a-z0-9_]+\s+)?"
            r"(.*)\s*$",
            statement,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if body_match is None:
            raise RuntimeError(
                f"V2 authority trigger has no row body: {match.group(1)}"
            )
        result[match.group(1).lower()] = _normalized_sql(body_match.group(1))
    return result


def _accounting_trigger_contracts() -> dict[str, tuple[str, str]]:
    migration = _migration(EVIDENCE_ACCOUNTING_MIGRATION)
    if migration is None:
        return {}
    result: dict[str, tuple[str, str]] = {}
    for statement in tuple(migration["statements"]):
        match = _TRIGGER_RE.search(statement)
        if match is None or match.group(3).lower() not in ACCOUNTING_EVIDENCE_TABLES:
            continue
        name = match.group(1).lower()
        if name in result:
            raise RuntimeError(
                f"duplicate V2 accounting-evidence trigger declaration: {name}"
            )
        result[name] = (match.group(2).upper(), match.group(3).lower())
    return result


def _accounting_trigger_bodies() -> dict[str, str]:
    migration = _migration(EVIDENCE_ACCOUNTING_MIGRATION)
    if migration is None:
        return {}
    result: dict[str, str] = {}
    for statement in tuple(migration["statements"]):
        match = _TRIGGER_RE.search(statement)
        if match is None or match.group(3).lower() not in ACCOUNTING_EVIDENCE_TABLES:
            continue
        body_match = re.search(
            r"\bFOR\s+EACH\s+ROW\s+"
            r"(?:(?:FOLLOWS|PRECEDES)\s+[a-z0-9_]+\s+)?"
            r"(.*)\s*$",
            statement,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if body_match is None:
            raise RuntimeError(
                f"V2 accounting trigger has no row body: {match.group(1)}"
            )
        result[match.group(1).lower()] = _normalized_sql(body_match.group(1))
    return result


def _all_trigger_contracts() -> dict[str, tuple[str, str]]:
    result = _guard_trigger_contracts(include_authority_attestations=True)
    for contracts in (
        _authority_trigger_contracts(),
        _accounting_trigger_contracts(),
    ):
        duplicate_names = set(result) & set(contracts)
        if duplicate_names:
            raise RuntimeError("duplicate V2 trigger declaration")
        result.update(contracts)
    return result


def _all_trigger_bodies() -> dict[str, str]:
    result = _guard_trigger_bodies(include_authority_attestations=True)
    for bodies in (
        _authority_trigger_bodies(),
        _accounting_trigger_bodies(),
    ):
        duplicate_names = set(result) & set(bodies)
        if duplicate_names:
            raise RuntimeError("duplicate V2 trigger body declaration")
        result.update(bodies)
    return result


def _trigger_action_order_contracts(
    contracts: dict[str, tuple[str, str]] | None = None,
) -> tuple[dict[str, int], dict[str, tuple[str, str]]]:
    """Derive MySQL ``ACTION_ORDER`` from frozen trigger declarations.

    The declaration order is the stable tie-breaker for single-trigger groups;
    explicit FOLLOWS/PRECEDES clauses become graph edges.  Unexpected cycles or
    references fail at code-contract construction rather than being tolerated
    as database drift.
    """

    selected = dict(contracts or _all_trigger_contracts())
    declaration_order: list[str] = []
    order_references: dict[str, tuple[str, str]] = {}
    for migration in MIGRATIONS:
        for statement in tuple(migration["statements"]):
            trigger_match = _TRIGGER_RE.search(statement)
            if trigger_match is None:
                continue
            trigger_name = trigger_match.group(1).lower()
            if trigger_name not in selected:
                continue
            if trigger_name not in declaration_order:
                declaration_order.append(trigger_name)
            order_match = _TRIGGER_ORDER_RE.search(statement)
            if order_match is not None:
                order_references[trigger_name] = (
                    order_match.group(1).upper(),
                    order_match.group(2).lower(),
                )

    if set(declaration_order) != set(selected):
        raise RuntimeError("V2 trigger action-order declaration is incomplete")
    groups: dict[tuple[str, str], list[str]] = {}
    for trigger_name in declaration_order:
        event, table_name = selected[trigger_name]
        groups.setdefault((table_name, event), []).append(trigger_name)

    action_orders: dict[str, int] = {}
    for group_names in groups.values():
        group_set = set(group_names)
        outgoing = {name: set() for name in group_names}
        indegree = {name: 0 for name in group_names}
        for trigger_name in group_names:
            reference = order_references.get(trigger_name)
            if reference is None:
                continue
            direction, other = reference
            if other not in group_set:
                raise RuntimeError(
                    "V2 trigger action-order reference leaves its event group"
                )
            before, after = (
                (other, trigger_name)
                if direction == "FOLLOWS"
                else (trigger_name, other)
            )
            if after not in outgoing[before]:
                outgoing[before].add(after)
                indegree[after] += 1
        remaining = list(group_names)
        ordered: list[str] = []
        while remaining:
            candidate = next(
                (name for name in remaining if indegree[name] == 0),
                None,
            )
            if candidate is None:
                raise RuntimeError("V2 trigger action-order declaration has a cycle")
            remaining.remove(candidate)
            ordered.append(candidate)
            for target in outgoing[candidate]:
                indegree[target] -= 1
        for action_order, trigger_name in enumerate(ordered, start=1):
            action_orders[trigger_name] = action_order
    return action_orders, order_references


def _trigger_context_is_safe(row: dict[str, Any]) -> bool:
    sql_modes = {
        item.strip().upper()
        for item in str(row.get("SQL_MODE") or row.get("sql_mode") or "").split(",")
        if item.strip()
    }
    character_set = str(
        row.get("CHARACTER_SET_CLIENT")
        or row.get("character_set_client")
        or ""
    ).lower()
    connection_collation = str(
        row.get("COLLATION_CONNECTION")
        or row.get("collation_connection")
        or ""
    ).lower()
    database_collation = str(
        row.get("DATABASE_COLLATION")
        or row.get("database_collation")
        or ""
    ).lower()
    return (
        bool(str(row.get("DEFINER") or row.get("definer") or ""))
        and bool({"STRICT_TRANS_TABLES", "STRICT_ALL_TABLES"} & sql_modes)
        and _REQUIRED_TRIGGER_SQL_MODES.issubset(sql_modes)
        and character_set in {"utf8", "utf8mb4"}
        and connection_collation.startswith(("utf8_", "utf8mb4_"))
        and database_collation.startswith("utf8mb4_")
    )


def _trigger_row_matches_contract(
    row: dict[str, Any],
    *,
    trigger_name: str,
    contracts: dict[str, tuple[str, str]],
    bodies: dict[str, str],
    action_orders: dict[str, int],
) -> bool:
    contract = contracts.get(trigger_name)
    if contract is None or trigger_name not in bodies or trigger_name not in action_orders:
        return False
    event, table_name = contract
    try:
        action_order = int(
            row.get("ACTION_ORDER")
            if "ACTION_ORDER" in row
            else row.get("action_order")
        )
    except (TypeError, ValueError):
        return False
    return (
        str(
            row.get("EVENT_OBJECT_TABLE")
            or row.get("event_object_table")
            or ""
        ).lower()
        == table_name
        and str(row.get("ACTION_TIMING") or row.get("action_timing") or "").upper()
        == "BEFORE"
        and str(
            row.get("EVENT_MANIPULATION")
            or row.get("event_manipulation")
            or ""
        ).upper()
        == event
        and _normalized_sql(
            row.get("ACTION_STATEMENT") or row.get("action_statement") or ""
        )
        == bodies[trigger_name]
        and action_order == action_orders[trigger_name]
        and _trigger_context_is_safe(row)
    )


def _required_implicit_fk_index_tuples(
    signature: dict[str, Any],
    expected_indexes: dict[str, dict[str, Any]],
) -> frozenset[tuple[str, ...]]:
    """Return child-FK tuples MySQL must support with implicit indexes."""

    explicit_columns = tuple(
        tuple(details["columns"]) for details in expected_indexes.values()
    )
    required: set[tuple[str, ...]] = set()
    for details in signature["foreign_keys"].values():
        columns = tuple(details["columns"])
        if not any(
            candidate[: len(columns)] == columns
            for candidate in explicit_columns
        ):
            required.add(columns)
    return frozenset(required)


def _guard_trigger_contracts(
    *, include_authority_attestations: bool = True
) -> dict[str, tuple[str, str]]:
    result: dict[str, tuple[str, str]] = {}
    binding_seen = False
    for migration in MIGRATIONS:
        if (
            not include_authority_attestations
            and str(migration["version"]) >= EVIDENCE_AUTHORITY_MIGRATION
        ):
            break
        if str(migration["version"]) == EVIDENCE_BINDING_MIGRATION:
            binding_seen = True
            continue
        if not binding_seen:
            continue
        for statement in tuple(migration["statements"]):
            match = _TRIGGER_RE.search(statement)
            if match is None:
                continue
            trigger_name, event, table_name = (
                match.group(1).lower(),
                match.group(2).upper(),
                match.group(3).lower(),
            )
            if table_name not in EVIDENCE_TABLES:
                continue
            if trigger_name in result:
                raise RuntimeError(
                    f"duplicate V2 evidence trigger declaration: {trigger_name}"
                )
            result[trigger_name] = (event, table_name)
    return result


def _normalized_sql(value: object) -> str:
    return " ".join(str(value or "").split())


def _guard_trigger_bodies(
    *, include_authority_attestations: bool = True
) -> dict[str, str]:
    result: dict[str, str] = {}
    binding_seen = False
    for migration in MIGRATIONS:
        if (
            not include_authority_attestations
            and str(migration["version"]) >= EVIDENCE_AUTHORITY_MIGRATION
        ):
            break
        if str(migration["version"]) == EVIDENCE_BINDING_MIGRATION:
            binding_seen = True
            continue
        if not binding_seen:
            continue
        for statement in tuple(migration["statements"]):
            match = _TRIGGER_RE.search(statement)
            if match is None or match.group(3).lower() not in EVIDENCE_TABLES:
                continue
            body_match = re.search(
                r"\bFOR\s+EACH\s+ROW\s+"
                r"(?:(?:FOLLOWS|PRECEDES)\s+[a-z0-9_]+\s+)?"
                r"(.*)\s*$",
                statement,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if body_match is None:
                raise RuntimeError(
                    f"V2 evidence trigger has no row body: {match.group(1)}"
                )
            result[match.group(1).lower()] = _normalized_sql(
                body_match.group(1)
            )
    return result


def _expected_migrations() -> tuple[dict[str, Any], ...]:
    binding_seen = False
    selected: list[dict[str, Any]] = []
    for migration in MIGRATIONS:
        if str(migration["version"]) == EVIDENCE_BINDING_MIGRATION:
            binding_seen = True
        if binding_seen:
            selected.append(migration)
    return tuple(selected)


def _all_mappings(result: Any) -> tuple[dict[str, Any], ...]:
    return tuple(dict(row) for row in result.mappings())


def _server_is_supported(dialect: str, version: str) -> bool:
    return dialect == "mysql" and is_isolated_acceptance_version(version)


def _inspect_maintenance_fence(
    connection: Any,
    *,
    expected_active: bool | None,
    require_row: bool = True,
) -> tuple[tuple[str, ...], bool]:
    """Validate the bootstrap control table and its single durable row."""

    blockers: list[str] = []
    table_rows = _all_mappings(
        connection.execute(
            text(
                "SELECT TABLE_NAME, ENGINE, TABLE_COLLATION, ROW_FORMAT "
                "FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() "
                f"AND TABLE_NAME = '{V2_EVIDENCE_MAINTENANCE_FENCE_TABLE}'"
            )
        )
    )
    if len(table_rows) != 1:
        return (("MAINTENANCE_FENCE_TABLE_MISSING",), False)
    table_row = table_rows[0]
    if str(table_row.get("ENGINE") or table_row.get("engine") or "").lower() != (
        "innodb"
    ):
        blockers.append("MAINTENANCE_FENCE_ENGINE_INVALID")
    if not str(
        table_row.get("TABLE_COLLATION")
        or table_row.get("table_collation")
        or ""
    ).lower().startswith("utf8mb4_"):
        blockers.append("MAINTENANCE_FENCE_COLLATION_INVALID")
    if str(
        table_row.get("ROW_FORMAT") or table_row.get("row_format") or ""
    ).upper() != "DYNAMIC":
        blockers.append("MAINTENANCE_FENCE_ROW_FORMAT_INVALID")

    signature = _maintenance_fence_schema_signature().get(
        V2_EVIDENCE_MAINTENANCE_FENCE_TABLE,
        {},
    )
    if not signature:
        blockers.append("CODE_MAINTENANCE_FENCE_DECLARATION_INCOMPLETE")
    column_rows = _all_mappings(
        connection.execute(
            text(
                "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, "
                "COLUMN_DEFAULT, COLLATION_NAME "
                "FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() "
                f"AND TABLE_NAME = '{V2_EVIDENCE_MAINTENANCE_FENCE_TABLE}' "
                "ORDER BY ORDINAL_POSITION"
            )
        )
    )
    observed_columns = {
        str(row.get("COLUMN_NAME") or row.get("column_name") or "").lower(): {
            "type": _normalize_column_type(
                row.get("COLUMN_TYPE") or row.get("column_type") or ""
            ),
            "nullable": str(
                row.get("IS_NULLABLE") or row.get("is_nullable") or ""
            ).upper()
            == "YES",
            "default": _normalize_observed_default(
                row.get("COLUMN_DEFAULT")
                if "COLUMN_DEFAULT" in row
                else row.get("column_default")
            ),
            "collation": str(
                row.get("COLLATION_NAME") or row.get("collation_name") or ""
            ).lower(),
        }
        for row in column_rows
    }
    comparable_columns = {
        name: {
            "type": details["type"],
            "nullable": details["nullable"],
            "default": details["default"],
        }
        for name, details in observed_columns.items()
    }
    if comparable_columns != signature.get("columns", {}):
        blockers.append("MAINTENANCE_FENCE_COLUMNS_DRIFTED")
    for column_name in ("fence_name", "state", "target_version"):
        if observed_columns.get(column_name, {}).get("collation") != "ascii_bin":
            blockers.append(
                f"MAINTENANCE_FENCE_COLUMN_COLLATION_INVALID:{column_name}"
            )

    index_rows = _all_mappings(
        connection.execute(
            text(
                "SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, "
                "SUB_PART, INDEX_TYPE, COLLATION "
                "FROM information_schema.STATISTICS "
                "WHERE TABLE_SCHEMA = DATABASE() "
                f"AND TABLE_NAME = '{V2_EVIDENCE_MAINTENANCE_FENCE_TABLE}' "
                "ORDER BY INDEX_NAME, SEQ_IN_INDEX"
            )
        )
    )
    observed_indexes = tuple(
        (
            str(row.get("INDEX_NAME") or row.get("index_name") or ""),
            int(
                row.get("NON_UNIQUE")
                if "NON_UNIQUE" in row
                else row.get("non_unique", 1)
            ),
            int(
                row.get("SEQ_IN_INDEX")
                if "SEQ_IN_INDEX" in row
                else row.get("seq_in_index", 0)
            ),
            str(row.get("COLUMN_NAME") or row.get("column_name") or "").lower(),
            row.get("SUB_PART") if "SUB_PART" in row else row.get("sub_part"),
            str(row.get("INDEX_TYPE") or row.get("index_type") or "").upper(),
            str(row.get("COLLATION") or row.get("collation") or "").upper(),
        )
        for row in index_rows
    )
    if observed_indexes != (("PRIMARY", 0, 1, "fence_name", None, "BTREE", "A"),):
        blockers.append("MAINTENANCE_FENCE_INDEX_DRIFTED")
    foreign_key_count = int(
        connection.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE "
                "WHERE CONSTRAINT_SCHEMA = DATABASE() "
                f"AND TABLE_NAME = '{V2_EVIDENCE_MAINTENANCE_FENCE_TABLE}' "
                "AND REFERENCED_TABLE_NAME IS NOT NULL"
            )
        ).scalar()
        or 0
    )
    if foreign_key_count != 0:
        blockers.append("MAINTENANCE_FENCE_FOREIGN_KEYS_DRIFTED")

    if not require_row:
        return (tuple(blockers), False)
    state_rows = _all_mappings(
        connection.execute(
            text(
                "SELECT fence_name, state, target_version, generation, "
                "activated_at, updated_at "
                f"FROM {V2_EVIDENCE_MAINTENANCE_FENCE_TABLE} "
                "ORDER BY fence_name LOCK IN SHARE MODE"
            )
        )
    )
    active = False
    if len(state_rows) != 1 or str(
        state_rows[0].get("fence_name")
        or state_rows[0].get("FENCE_NAME")
        or ""
    ) != V2_EVIDENCE_MAINTENANCE_FENCE_NAME:
        blockers.append("MAINTENANCE_FENCE_ROW_SET_DRIFTED")
    else:
        row = state_rows[0]
        state = str(row.get("state") or row.get("STATE") or "").upper()
        active = state == V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE
        if state not in {
            V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE,
            V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE,
        }:
            blockers.append("MAINTENANCE_FENCE_STATE_INVALID")
        target_version = str(
            row.get("target_version") or row.get("TARGET_VERSION") or ""
        )
        try:
            generation = int(
                row.get("generation")
                if "generation" in row
                else row.get("GENERATION")
            )
        except (TypeError, ValueError):
            generation = -1
        if (
            target_version not in {str(item["version"]) for item in _expected_migrations()}
            or generation < 0
            or (row.get("activated_at") or row.get("ACTIVATED_AT")) is None
            or (row.get("updated_at") or row.get("UPDATED_AT")) is None
        ):
            blockers.append("MAINTENANCE_FENCE_ROW_DRIFTED")
        if expected_active is True and not active:
            blockers.append("MAINTENANCE_FENCE_NOT_ACTIVE")
        elif expected_active is False and active:
            blockers.append("MAINTENANCE_FENCE_ACTIVE")
    return (tuple(blockers), active)


def assert_v2_evidence_maintenance_fence_inactive(connection: Any) -> None:
    """Acquire the writer-side shared lock and fail unless the fence is INACTIVE.

    Callers must invoke this inside the same transaction that will append V2
    evidence.  The shared row lock is retained until that transaction ends; a
    migration runner waits for it with ``FOR UPDATE`` before publishing ACTIVE.
    """

    if connection is None or not callable(getattr(connection, "execute", None)):
        raise V2EvidenceMaintenanceFenceError(
            "a SQLAlchemy-like connection is required"
        )
    in_transaction = getattr(connection, "in_transaction", None)
    if not callable(in_transaction) or in_transaction() is not True:
        raise V2EvidenceMaintenanceFenceError(
            "maintenance fence must be checked inside the writer transaction"
        )
    try:
        rows = _all_mappings(
            connection.execute(
                text(
                    "SELECT fence_name, state "
                    f"FROM {V2_EVIDENCE_MAINTENANCE_FENCE_TABLE} "
                    "WHERE fence_name = :fence_name LOCK IN SHARE MODE"
                ),
                {"fence_name": V2_EVIDENCE_MAINTENANCE_FENCE_NAME},
            )
        )
    except Exception as exc:
        raise V2EvidenceMaintenanceFenceError(
            "maintenance fence cannot be locked"
        ) from exc
    if (
        len(rows) != 1
        or str(rows[0].get("fence_name") or rows[0].get("FENCE_NAME") or "")
        != V2_EVIDENCE_MAINTENANCE_FENCE_NAME
        or str(rows[0].get("state") or rows[0].get("STATE") or "").upper()
        != V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE
    ):
        raise V2EvidenceMaintenanceFenceError(
            "V2 execution-evidence maintenance fence is active or invalid"
        )


def inspect_v2_execution_evidence_schema(
    connection: Any,
    *,
    require_guards: bool = True,
    require_natural_keys: bool = True,
    require_migration_ledger: bool = True,
    require_authority_attestations: bool = True,
    require_accounting_evidence: bool = True,
    phase_scoped_migration_replay: bool = False,
    maintenance_fence_expected_active: bool = False,
    include_activation_blockers: bool = True,
    canonical_hash_audit_passed: bool = False,
) -> V2EvidenceSchemaReport:
    """Inspect 011-015 evidence schema without mutating external state.

    Authority and accounting declarations are strict by default.  Public
    inspection promotes any observed later-layer table to exact validation.
    The migration runner alone uses ``phase_scoped_migration_replay`` while it
    resumes DDL whose ledger row is intentionally not written yet; that mode
    validates the target layer strictly after all of its DDL is present.
    """

    if not hasattr(connection, "execute"):
        raise TypeError("connection must provide Connection.execute")
    for name, value in (
        ("require_guards", require_guards),
        ("require_natural_keys", require_natural_keys),
        ("require_migration_ledger", require_migration_ledger),
        ("require_authority_attestations", require_authority_attestations),
        ("require_accounting_evidence", require_accounting_evidence),
        ("phase_scoped_migration_replay", phase_scoped_migration_replay),
        (
            "maintenance_fence_expected_active",
            maintenance_fence_expected_active,
        ),
        ("include_activation_blockers", include_activation_blockers),
        ("canonical_hash_audit_passed", canonical_hash_audit_passed),
    ):
        if type(value) is not bool:
            raise TypeError(f"{name} must be bool")
    dialect = str(
        getattr(getattr(connection, "dialect", None), "name", "") or ""
    ).lower()
    if dialect not in {"mysql", "mariadb"}:
        raise V2EvidenceSchemaInspectionError(
            "V2 evidence schema inspection requires MySQL or MariaDB"
        )

    server_version = str(
        connection.execute(text("SELECT VERSION()")).scalar() or ""
    ).strip()
    database_name = str(
        connection.execute(text("SELECT DATABASE()")).scalar() or ""
    ).strip()
    blockers: list[str] = []
    if not database_name:
        blockers.append("DATABASE_NOT_SELECTED")
    if not _server_is_supported(dialect, server_version):
        blockers.append("MYSQL_VERSION_NOT_VALIDATED")

    fence_blockers, maintenance_fence_active = _inspect_maintenance_fence(
        connection,
        expected_active=maintenance_fence_expected_active,
    )
    blockers.extend(fence_blockers)

    binding_schema = _binding_schema_signature(
        include_natural_keys=require_natural_keys
    )
    complete_schema = _binding_schema_signature(include_natural_keys=True)
    authority_schema = _authority_schema_signature()
    accounting_schema = _accounting_schema_signature()
    if set(binding_schema) != set(EVIDENCE_TABLES):
        blockers.append("CODE_BINDING_MIGRATION_INCOMPLETE")

    potential_tables = EVIDENCE_TABLES | AUTHORITY_TABLES | ACCOUNTING_EVIDENCE_TABLES
    potential_table_literals = ", ".join(
        f"'{table_name}'" for table_name in sorted(potential_tables)
    )
    table_rows = _all_mappings(
        connection.execute(
            text(
                "SELECT TABLE_NAME, ENGINE, TABLE_COLLATION, ROW_FORMAT "
                "FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() "
                f"AND TABLE_NAME IN ({potential_table_literals}) "
                "ORDER BY TABLE_NAME"
            )
        )
    )
    observed_tables = {
        str(row.get("TABLE_NAME") or row.get("table_name") or "").lower(): row
        for row in table_rows
    }
    promote_observed_forward_layers = not phase_scoped_migration_replay
    validate_authority = require_authority_attestations or (
        promote_observed_forward_layers
        and bool(AUTHORITY_TABLES & set(observed_tables))
    )
    validate_accounting = require_accounting_evidence or (
        promote_observed_forward_layers
        and bool(ACCOUNTING_EVIDENCE_TABLES & set(observed_tables))
    )
    expected_schema = dict(binding_schema)
    if validate_authority:
        if set(authority_schema) != set(AUTHORITY_TABLES):
            blockers.append("CODE_AUTHORITY_MIGRATION_INCOMPLETE")
        expected_schema.update(authority_schema)
    if validate_accounting:
        if set(accounting_schema) != set(ACCOUNTING_EVIDENCE_TABLES):
            blockers.append("CODE_ACCOUNTING_MIGRATION_INCOMPLETE")
        expected_schema.update(accounting_schema)
    validation_tables = frozenset(expected_schema)
    validation_table_literals = ", ".join(
        f"'{table_name}'" for table_name in sorted(validation_tables)
    )
    missing_tables = sorted(validation_tables - set(observed_tables))
    if missing_tables:
        blockers.extend(f"TABLE_MISSING:{item}" for item in missing_tables)
    for table_name in sorted(validation_tables & set(observed_tables)):
        row = observed_tables[table_name]
        engine = str(row.get("ENGINE") or row.get("engine") or "").lower()
        if engine != "innodb":
            blockers.append(f"TABLE_ENGINE_INVALID:{table_name}")
        collation = str(
            row.get("TABLE_COLLATION") or row.get("table_collation") or ""
        ).lower()
        if not collation.startswith("utf8mb4_"):
            blockers.append(f"TABLE_COLLATION_INVALID:{table_name}")
        if (
            table_name in AUTHORITY_TABLES | ACCOUNTING_EVIDENCE_TABLES
            and str(row.get("ROW_FORMAT") or row.get("row_format") or "").upper()
            != "DYNAMIC"
        ):
            blockers.append(f"TABLE_ROW_FORMAT_INVALID:{table_name}")

    expected_collations = _declared_column_collation_contracts(
        EVIDENCE_BINDING_MIGRATION,
        EVIDENCE_TABLES,
    )
    if validate_authority:
        expected_collations.update(
            _declared_column_collation_contracts(
                EVIDENCE_AUTHORITY_MIGRATION,
                AUTHORITY_TABLES,
            )
        )
    if validate_accounting:
        expected_collations.update(
            _declared_column_collation_contracts(
                EVIDENCE_ACCOUNTING_MIGRATION,
                ACCOUNTING_EVIDENCE_TABLES,
            )
        )

    expected_triggers: dict[str, tuple[str, str]] = {}
    expected_trigger_bodies: dict[str, str] = {}
    expected_trigger_action_orders: dict[str, int] = {}
    if require_guards:
        trigger_contract_groups = [
            _guard_trigger_contracts(
                include_authority_attestations=validate_authority
            )
        ]
        trigger_body_groups = [
            _guard_trigger_bodies(
                include_authority_attestations=validate_authority
            )
        ]
        if validate_authority:
            trigger_contract_groups.append(_authority_trigger_contracts())
            trigger_body_groups.append(_authority_trigger_bodies())
        if validate_accounting:
            trigger_contract_groups.append(_accounting_trigger_contracts())
            trigger_body_groups.append(_accounting_trigger_bodies())
        for contracts, bodies in zip(trigger_contract_groups, trigger_body_groups):
            if set(contracts) != set(bodies):
                blockers.append("CODE_TRIGGER_BODY_DECLARATION_INCOMPLETE")
            duplicate_names = set(expected_triggers) & set(contracts)
            if duplicate_names:
                blockers.append("CODE_TRIGGER_DECLARATION_DUPLICATE")
            expected_triggers.update(contracts)
            expected_trigger_bodies.update(bodies)
        base_shapes = {
            (table_name, event)
            for event, table_name in _guard_trigger_contracts(
                include_authority_attestations=validate_authority
            ).values()
        }
        required_base_shapes = {
            (table_name, event)
            for table_name in EVIDENCE_TABLES
            for event in ("INSERT", "UPDATE", "DELETE")
        }
        if base_shapes != required_base_shapes:
            blockers.append("CODE_GUARD_MIGRATION_INCOMPLETE")
        for enabled, tables, contracts, blocker in (
            (
                validate_authority,
                AUTHORITY_TABLES,
                _authority_trigger_contracts(),
                "CODE_AUTHORITY_GUARD_MIGRATION_INCOMPLETE",
            ),
            (
                validate_accounting,
                ACCOUNTING_EVIDENCE_TABLES,
                _accounting_trigger_contracts(),
                "CODE_ACCOUNTING_GUARD_MIGRATION_INCOMPLETE",
            ),
        ):
            if not enabled:
                continue
            actual_shapes = {
                (table_name, event) for event, table_name in contracts.values()
            }
            required_shapes = {
                (table_name, event)
                for table_name in tables
                for event in ("INSERT", "UPDATE", "DELETE")
            }
            if actual_shapes != required_shapes:
                blockers.append(blocker)
        try:
            (
                expected_trigger_action_orders,
                trigger_order_references,
            ) = _trigger_action_order_contracts(expected_triggers)
        except RuntimeError:
            blockers.append("CODE_TRIGGER_ACTION_ORDER_DECLARATION_INCOMPLETE")
        else:
            if validate_authority and {
                name: reference
                for name, (direction, reference) in trigger_order_references.items()
                if direction == "FOLLOWS"
            } != _EXPECTED_TRIGGER_FOLLOWS:
                blockers.append("CODE_TRIGGER_FOLLOWS_DECLARATION_DRIFTED")
            if any(
                direction != "FOLLOWS"
                for direction, _ in trigger_order_references.values()
            ):
                blockers.append("CODE_TRIGGER_ORDER_DIRECTION_DRIFTED")

    column_rows = _all_mappings(
        connection.execute(
            text(
                "SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, "
                "IS_NULLABLE, COLUMN_DEFAULT, COLLATION_NAME "
                "FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() "
                f"AND TABLE_NAME IN ({validation_table_literals}) "
                "ORDER BY TABLE_NAME, ORDINAL_POSITION"
            )
        )
    )
    observed_columns: dict[str, dict[str, dict[str, Any]]] = {}
    for row in column_rows:
        table_name = str(
            row.get("TABLE_NAME") or row.get("table_name") or ""
        ).lower()
        column_name = str(
            row.get("COLUMN_NAME") or row.get("column_name") or ""
        ).lower()
        observed_columns.setdefault(table_name, {})[column_name] = {
            "type": _normalize_column_type(
                row.get("COLUMN_TYPE") or row.get("column_type") or ""
            ),
            "nullable": str(
                row.get("IS_NULLABLE") or row.get("is_nullable") or ""
            ).upper()
            == "YES",
            "default": _normalize_observed_default(
                row.get("COLUMN_DEFAULT")
                if "COLUMN_DEFAULT" in row
                else row.get("column_default")
            ),
            "collation": str(
                row.get("COLLATION_NAME") or row.get("collation_name") or ""
            ).lower(),
        }
    for table_name, signature in expected_schema.items():
        expected_columns = signature["columns"]
        actual_columns = observed_columns.get(table_name, {})
        comparable_actual = {
            column_name: {
                "type": details["type"],
                "nullable": details["nullable"],
                "default": details["default"],
            }
            for column_name, details in actual_columns.items()
        }
        if comparable_actual != expected_columns:
            blockers.append(f"TABLE_COLUMNS_DRIFTED:{table_name}")
        for column_name, (mode, expected_collation) in expected_collations.get(
            table_name, {}
        ).items():
            actual_collation = actual_columns.get(column_name, {}).get(
                "collation", ""
            )
            if (
                mode == "exact"
                and str(actual_collation) != expected_collation
            ) or (
                mode == "prefix"
                and not str(actual_collation).startswith(expected_collation)
            ):
                blockers.append(
                    f"COLUMN_COLLATION_INVALID:{table_name}.{column_name}"
                )

    index_rows = _all_mappings(
        connection.execute(
            text(
                "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, "
                "SEQ_IN_INDEX, COLUMN_NAME, SUB_PART, INDEX_TYPE, COLLATION "
                "FROM information_schema.STATISTICS "
                "WHERE TABLE_SCHEMA = DATABASE() "
                f"AND TABLE_NAME IN ({validation_table_literals}) "
                "ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX"
            )
        )
    )
    observed_index_parts: dict[str, dict[str, dict[str, Any]]] = {}
    for row in index_rows:
        table_name = str(
            row.get("TABLE_NAME") or row.get("table_name") or ""
        ).lower()
        index_name = str(
            row.get("INDEX_NAME") or row.get("index_name") or ""
        )
        entry = observed_index_parts.setdefault(table_name, {}).setdefault(
            index_name,
            {
                "unique_values": [],
                "columns": [],
                "positions": [],
                "sub_parts": [],
                "index_types": [],
                "collations": [],
            },
        )
        entry["unique_values"].append(
            int(
                row.get("NON_UNIQUE")
                if "NON_UNIQUE" in row
                else row.get("non_unique", 1)
            )
            == 0
        )
        entry["columns"].append(
            str(row.get("COLUMN_NAME") or row.get("column_name") or "")
        )
        entry["positions"].append(
            int(
                row.get("SEQ_IN_INDEX")
                if "SEQ_IN_INDEX" in row
                else row.get("seq_in_index", 0)
            )
        )
        entry["sub_parts"].append(
            row.get("SUB_PART") if "SUB_PART" in row else row.get("sub_part")
        )
        entry["index_types"].append(
            str(row.get("INDEX_TYPE") or row.get("index_type") or "").upper()
        )
        entry["collations"].append(
            str(row.get("COLLATION") or row.get("collation") or "").upper()
        )
    observed_indexes = {
        table_name: {
            index_name: {
                "unique": (
                    next(iter(set(details["unique_values"])))
                    if len(set(details["unique_values"])) == 1
                    else None
                ),
                "columns": tuple(details["columns"]),
                "metadata_valid": (
                    details["positions"]
                    == list(range(1, len(details["columns"]) + 1))
                    and all(value is None for value in details["sub_parts"])
                    and set(details["index_types"]) == {"BTREE"}
                    and set(details["collations"]) == {"A"}
                    and len(set(details["unique_values"])) == 1
                ),
            }
            for index_name, details in indexes.items()
        }
        for table_name, indexes in observed_index_parts.items()
    }
    for table_name, signature in expected_schema.items():
        expected_indexes = dict(signature["indexes"])
        actual_indexes = observed_indexes.get(table_name, {})
        if not require_natural_keys:
            complete_indexes = complete_schema.get(table_name, {}).get(
                "indexes", {}
            )
            for index_name, expected in complete_indexes.items():
                if (
                    index_name not in expected_indexes
                    and index_name in actual_indexes
                ):
                    # During migration replay, 013 may already be installed
                    # while 011/012 are being revalidated.  Treat that exact
                    # forward-only index as optional, but still reject drift.
                    expected_indexes[index_name] = expected
        for index_name, expected in expected_indexes.items():
            actual = actual_indexes.get(index_name)
            if (
                actual is None
                or not actual["metadata_valid"]
                or {
                    "unique": actual["unique"],
                    "columns": actual["columns"],
                }
                != expected
            ):
                blockers.append(
                    f"TABLE_INDEX_DRIFTED:{table_name}.{index_name}"
                )
        expected_unique = {
            name for name, details in expected_indexes.items() if details["unique"]
        }
        actual_unique = {
            name
            for name, details in actual_indexes.items()
            if details["unique"] is True
        }
        if actual_unique != expected_unique:
            blockers.append(f"TABLE_UNIQUE_INDEX_SET_DRIFTED:{table_name}")
        implicit_required = _required_implicit_fk_index_tuples(
            signature,
            expected_indexes,
        )
        extra_indexes = {
            name: details
            for name, details in actual_indexes.items()
            if name not in expected_indexes
        }
        extra_columns = [
            details["columns"] for details in extra_indexes.values()
        ]
        if (
            len(extra_columns) != len(set(extra_columns))
            or set(extra_columns) != set(implicit_required)
            or any(
                details["unique"] is not False
                or not details["metadata_valid"]
                for details in extra_indexes.values()
            )
        ):
            blockers.append(f"TABLE_IMPLICIT_FK_INDEX_SET_DRIFTED:{table_name}")

    foreign_key_rows = _all_mappings(
        connection.execute(
            text(
                "SELECT k.TABLE_NAME, k.CONSTRAINT_NAME, k.COLUMN_NAME, "
                "k.REFERENCED_TABLE_SCHEMA, k.REFERENCED_TABLE_NAME, "
                "k.REFERENCED_COLUMN_NAME, k.ORDINAL_POSITION, "
                "r.DELETE_RULE, r.UPDATE_RULE "
                "FROM information_schema.KEY_COLUMN_USAGE k "
                "JOIN information_schema.REFERENTIAL_CONSTRAINTS r "
                "ON r.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA "
                "AND r.TABLE_NAME = k.TABLE_NAME "
                "AND r.CONSTRAINT_NAME = k.CONSTRAINT_NAME "
                "WHERE k.CONSTRAINT_SCHEMA = DATABASE() "
                f"AND k.TABLE_NAME IN ({validation_table_literals}) "
                "AND k.REFERENCED_TABLE_NAME IS NOT NULL "
                "ORDER BY k.TABLE_NAME, k.CONSTRAINT_NAME, "
                "k.ORDINAL_POSITION"
            )
        )
    )
    observed_fk_parts: dict[str, dict[str, dict[str, Any]]] = {}
    for row in foreign_key_rows:
        table_name = str(
            row.get("TABLE_NAME") or row.get("table_name") or ""
        ).lower()
        constraint_name = str(
            row.get("CONSTRAINT_NAME") or row.get("constraint_name") or ""
        )
        entry = observed_fk_parts.setdefault(table_name, {}).setdefault(
            constraint_name,
            {
                "columns": [],
                "referenced_schema": str(
                    row.get("REFERENCED_TABLE_SCHEMA")
                    or row.get("referenced_table_schema")
                    or ""
                ),
                "referenced_table": str(
                    row.get("REFERENCED_TABLE_NAME")
                    or row.get("referenced_table_name")
                    or ""
                ),
                "referenced_columns": [],
                "on_delete": normalize_mysql_referential_rule(
                    row.get("DELETE_RULE") or row.get("delete_rule") or ""
                ),
                "on_update": normalize_mysql_referential_rule(
                    row.get("UPDATE_RULE") or row.get("update_rule") or ""
                ),
            },
        )
        entry["columns"].append(
            str(row.get("COLUMN_NAME") or row.get("column_name") or "")
        )
        entry["referenced_columns"].append(
            str(
                row.get("REFERENCED_COLUMN_NAME")
                or row.get("referenced_column_name")
                or ""
            )
        )
    observed_foreign_keys = {
        table_name: {
            name: {
                "columns": tuple(details["columns"]),
                "referenced_table": details["referenced_table"],
                "referenced_columns": tuple(details["referenced_columns"]),
                "on_delete": details["on_delete"],
                "on_update": details["on_update"],
            }
            for name, details in constraints.items()
        }
        for table_name, constraints in observed_fk_parts.items()
    }
    for table_name, constraints in observed_fk_parts.items():
        for constraint_name, details in constraints.items():
            if details["referenced_schema"] != database_name:
                blockers.append(
                    "FOREIGN_KEY_SCHEMA_DRIFTED:"
                    f"{table_name}.{constraint_name}"
                )
    for table_name, signature in expected_schema.items():
        if observed_foreign_keys.get(table_name, {}) != signature["foreign_keys"]:
            blockers.append(f"TABLE_FOREIGN_KEYS_DRIFTED:{table_name}")

    trigger_rows = (
        _all_mappings(
            connection.execute(
                text(
                    "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, "
                    "EVENT_MANIPULATION, ACTION_STATEMENT, ACTION_ORDER, SQL_MODE, "
                    "DEFINER, CHARACTER_SET_CLIENT, "
                    "COLLATION_CONNECTION, DATABASE_COLLATION "
                    "FROM information_schema.TRIGGERS "
                    "WHERE TRIGGER_SCHEMA = DATABASE() "
                    f"AND EVENT_OBJECT_TABLE IN ({validation_table_literals}) "
                    "ORDER BY TRIGGER_NAME"
                )
            )
        )
        if require_guards
        else ()
    )
    observed_trigger_rows = {
        str(row.get("TRIGGER_NAME") or row.get("trigger_name") or "").lower(): row
        for row in trigger_rows
    }
    validation_triggers = dict(expected_triggers)
    validation_trigger_bodies = dict(expected_trigger_bodies)
    for trigger_name, (event, table_name) in validation_triggers.items():
        row = observed_trigger_rows.get(trigger_name)
        if row is None:
            blockers.append(f"TRIGGER_MISSING:{trigger_name}")
            continue
        actual_table = str(
            row.get("EVENT_OBJECT_TABLE")
            or row.get("event_object_table")
            or ""
        ).lower()
        timing = str(
            row.get("ACTION_TIMING") or row.get("action_timing") or ""
        ).upper()
        actual_event = str(
            row.get("EVENT_MANIPULATION")
            or row.get("event_manipulation")
            or ""
        ).upper()
        body = str(
            row.get("ACTION_STATEMENT") or row.get("action_statement") or ""
        )
        if actual_table != table_name or timing != "BEFORE" or actual_event != event:
            blockers.append(f"TRIGGER_SHAPE_DRIFTED:{trigger_name}")
        normalized_body = _normalized_sql(body)
        if "SIGNAL SQLSTATE '45000'" not in normalized_body.upper():
            blockers.append(f"TRIGGER_FAIL_CLOSED_GUARD_MISSING:{trigger_name}")
        if normalized_body != validation_trigger_bodies.get(trigger_name, ""):
            blockers.append(f"TRIGGER_BODY_DRIFTED:{trigger_name}")
        try:
            action_order = int(
                row.get("ACTION_ORDER")
                if "ACTION_ORDER" in row
                else row.get("action_order")
            )
        except (TypeError, ValueError):
            action_order = -1
        if action_order != expected_trigger_action_orders.get(trigger_name):
            blockers.append(f"TRIGGER_ACTION_ORDER_DRIFTED:{trigger_name}")
        sql_mode = str(row.get("SQL_MODE") or row.get("sql_mode") or "")
        definer = str(row.get("DEFINER") or row.get("definer") or "")
        character_set = str(
            row.get("CHARACTER_SET_CLIENT")
            or row.get("character_set_client")
            or ""
        ).lower()
        connection_collation = str(
            row.get("COLLATION_CONNECTION")
            or row.get("collation_connection")
            or ""
        ).lower()
        database_collation = str(
            row.get("DATABASE_COLLATION")
            or row.get("database_collation")
            or ""
        ).lower()
        if not sql_mode or not definer:
            blockers.append(f"TRIGGER_EXECUTION_CONTEXT_MISSING:{trigger_name}")
        sql_modes = {
            item.strip().upper() for item in sql_mode.split(",") if item.strip()
        }
        required_modes = {
            "NO_ZERO_DATE",
            "NO_ZERO_IN_DATE",
            "ERROR_FOR_DIVISION_BY_ZERO",
        }
        if (
            not ({"STRICT_TRANS_TABLES", "STRICT_ALL_TABLES"} & sql_modes)
            or not required_modes.issubset(sql_modes)
        ):
            blockers.append(f"TRIGGER_SQL_MODE_UNSAFE:{trigger_name}")
        if character_set not in {"utf8", "utf8mb4"}:
            blockers.append(f"TRIGGER_CHARACTER_SET_INVALID:{trigger_name}")
        if not connection_collation.startswith(("utf8_", "utf8mb4_")):
            blockers.append(f"TRIGGER_CONNECTION_COLLATION_INVALID:{trigger_name}")
        if not database_collation.startswith("utf8mb4_"):
            blockers.append(f"TRIGGER_DATABASE_COLLATION_INVALID:{trigger_name}")
    permitted_phase_forward_triggers: set[str] = set()
    if phase_scoped_migration_replay and not validate_authority:
        permitted_phase_forward_triggers = set(
            _guard_trigger_contracts(include_authority_attestations=True)
        ) - set(_guard_trigger_contracts(include_authority_attestations=False))
    unexpected_triggers = sorted(
        set(observed_trigger_rows)
        - set(validation_triggers)
        - permitted_phase_forward_triggers
    )
    if unexpected_triggers:
        blockers.extend(
            f"UNDECLARED_EVIDENCE_TRIGGER:{item}" for item in unexpected_triggers
        )

    expected_migrations = _expected_migrations()
    expected_checksums = {
        str(item["version"]): _checksum(tuple(item["statements"]))
        for item in expected_migrations
    }
    ledger_rows: tuple[dict[str, Any], ...] = ()
    if require_migration_ledger:
        ledger_exists = bool(
            connection.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'schema_migration_v2'
                    """
                )
            ).scalar()
        )
        if not ledger_exists:
            blockers.append("MIGRATION_LEDGER_TABLE_MISSING")
        else:
            ledger_table_rows = _all_mappings(
                connection.execute(
                    text(
                        """
                        SELECT TABLE_NAME, ENGINE, TABLE_COLLATION
                        FROM information_schema.TABLES
                        WHERE TABLE_SCHEMA = DATABASE()
                          AND TABLE_NAME = 'schema_migration_v2'
                        """
                    )
                )
            )
            if len(ledger_table_rows) != 1:
                blockers.append("MIGRATION_LEDGER_TABLE_DRIFTED")
            else:
                ledger_table = ledger_table_rows[0]
                if str(
                    ledger_table.get("ENGINE")
                    or ledger_table.get("engine")
                    or ""
                ).lower() != "innodb":
                    blockers.append("MIGRATION_LEDGER_ENGINE_INVALID")
                if not str(
                    ledger_table.get("TABLE_COLLATION")
                    or ledger_table.get("table_collation")
                    or ""
                ).lower().startswith("utf8mb4_"):
                    blockers.append("MIGRATION_LEDGER_COLLATION_INVALID")

            ledger_column_rows = _all_mappings(
                connection.execute(
                    text(
                        """
                        SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE,
                               COLUMN_DEFAULT, COLLATION_NAME
                        FROM information_schema.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE()
                          AND TABLE_NAME = 'schema_migration_v2'
                        ORDER BY ORDINAL_POSITION
                        """
                    )
                )
            )
            observed_ledger_columns = {
                str(
                    row.get("COLUMN_NAME") or row.get("column_name") or ""
                ).lower(): {
                    "type": _normalize_column_type(
                        row.get("COLUMN_TYPE") or row.get("column_type") or ""
                    ),
                    "nullable": str(
                        row.get("IS_NULLABLE") or row.get("is_nullable") or ""
                    ).upper()
                    == "YES",
                    "default": _normalize_observed_default(
                        row.get("COLUMN_DEFAULT")
                        if "COLUMN_DEFAULT" in row
                        else row.get("column_default")
                    ),
                }
                for row in ledger_column_rows
            }
            expected_ledger_columns = {
                "version": {
                    "type": "varchar(80)",
                    "nullable": False,
                    "default": None,
                },
                "checksum": {
                    "type": "char(64)",
                    "nullable": False,
                    "default": None,
                },
                "applied_at": {
                    "type": "timestamp",
                    "nullable": False,
                    "default": "current_timestamp",
                },
            }
            if observed_ledger_columns != expected_ledger_columns:
                blockers.append("MIGRATION_LEDGER_COLUMNS_DRIFTED")
            for row in ledger_column_rows:
                column_name = str(
                    row.get("COLUMN_NAME") or row.get("column_name") or ""
                ).lower()
                if column_name not in {"version", "checksum"}:
                    continue
                collation = str(
                    row.get("COLLATION_NAME")
                    or row.get("collation_name")
                    or ""
                ).lower()
                if not collation.startswith("utf8mb4_"):
                    blockers.append(
                        f"MIGRATION_LEDGER_COLUMN_COLLATION_INVALID:{column_name}"
                    )

            ledger_index_rows = _all_mappings(
                connection.execute(
                    text(
                        """
                        SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX,
                               COLUMN_NAME
                        FROM information_schema.STATISTICS
                        WHERE TABLE_SCHEMA = DATABASE()
                          AND TABLE_NAME = 'schema_migration_v2'
                        ORDER BY INDEX_NAME, SEQ_IN_INDEX
                        """
                    )
                )
            )
            ledger_index_parts: dict[str, dict[str, Any]] = {}
            for row in ledger_index_rows:
                index_name = str(
                    row.get("INDEX_NAME") or row.get("index_name") or ""
                )
                entry = ledger_index_parts.setdefault(
                    index_name,
                    {
                        "unique": int(
                            row.get("NON_UNIQUE")
                            if "NON_UNIQUE" in row
                            else row.get("non_unique", 1)
                        )
                        == 0,
                        "columns": [],
                    },
                )
                entry["columns"].append(
                    str(
                        row.get("COLUMN_NAME")
                        or row.get("column_name")
                        or ""
                    )
                )
            observed_ledger_indexes = {
                name: {
                    "unique": details["unique"],
                    "columns": tuple(details["columns"]),
                }
                for name, details in ledger_index_parts.items()
                if details["unique"]
            }
            if observed_ledger_indexes != {
                "PRIMARY": {"unique": True, "columns": ("version",)}
            }:
                blockers.append("MIGRATION_LEDGER_INDEX_DRIFTED")

        if ledger_exists and expected_checksums:
            ledger_rows = _all_mappings(
                connection.execute(
                    text(
                        """
                        SELECT version, checksum
                        FROM schema_migration_v2
                        WHERE version >= :binding_version
                        ORDER BY version
                        """
                    ),
                    {"binding_version": EVIDENCE_BINDING_MIGRATION},
                )
            )
        observed_versions = [
            str(row.get("version") or row.get("VERSION") or "")
            for row in ledger_rows
        ]
        if len(observed_versions) != len(set(observed_versions)):
            blockers.append("MIGRATION_LEDGER_DUPLICATE_VERSION")
        observed_checksums = {
            version: str(row.get("checksum") or row.get("CHECKSUM") or "")
            for version, row in zip(observed_versions, ledger_rows)
        }
        for version, expected in expected_checksums.items():
            actual = observed_checksums.get(version)
            if actual is None:
                blockers.append(f"MIGRATION_LEDGER_MISSING:{version}")
            elif actual != expected:
                blockers.append(f"MIGRATION_CHECKSUM_DRIFTED:{version}")
        unexpected_versions = sorted(
            set(observed_checksums) - set(expected_checksums)
        )
        if unexpected_versions:
            blockers.extend(
                f"UNREVIEWED_EVIDENCE_MIGRATION:{item}"
                for item in unexpected_versions
            )

    activation_blockers = (
        tuple(
            blocker
            for blocker in (
                "ISOLATED_MYSQL_BEHAVIORAL_ACCEPTANCE_MISSING",
                "LEAST_PRIVILEGE_ATTESTATION_MISSING",
                "EVIDENCE_WRITER_NOT_PRODUCTION_WIRED",
                "CANONICAL_HASH_NOT_DATABASE_RECOMPUTABLE",
            )
            if not (
                blocker == "CANONICAL_HASH_NOT_DATABASE_RECOMPUTABLE"
                and canonical_hash_audit_passed
            )
        )
        if include_activation_blockers
        else ()
    )
    return V2EvidenceSchemaReport(
        database_name=database_name,
        server_version=server_version,
        migration_versions=tuple(expected_checksums),
        observed_tables=tuple(sorted(observed_tables)),
        observed_triggers=tuple(sorted(observed_trigger_rows)),
        structural_blockers=tuple(sorted(set(blockers))),
        activation_blockers=activation_blockers,
        guards_checked=require_guards,
        migration_ledger_checked=require_migration_ledger,
        activation_checks_included=include_activation_blockers,
        canonical_hash_audit_passed=canonical_hash_audit_passed,
        phase_scoped_migration_replay=phase_scoped_migration_replay,
        maintenance_fence_checked=True,
        maintenance_fence_active=maintenance_fence_active,
    )


__all__ = [
    "ACCOUNTING_EVIDENCE_TABLES",
    "AUTHORITY_TABLES",
    "EVIDENCE_ACCOUNTING_MIGRATION",
    "EVIDENCE_AUTHORITY_MIGRATION",
    "EVIDENCE_BINDING_MIGRATION",
    "EVIDENCE_TABLES",
    "V2EvidenceMaintenanceFenceError",
    "V2EvidenceSchemaInspectionError",
    "V2EvidenceSchemaReport",
    "assert_v2_evidence_maintenance_fence_inactive",
    "inspect_v2_execution_evidence_schema",
]
