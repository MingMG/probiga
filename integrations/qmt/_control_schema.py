"""Strict MySQL contracts for QMT control tables.

This module deliberately separates two authorities:

* release/setup code may call :func:`privileged_migrate_frozen_tables`;
* scheduled/runtime code may only call :func:`validate_frozen_tables`.

The validator uses ``SELECT`` against ``information_schema`` only.  It checks
the complete ordered column contract, defaults, charset/collation, engine and
the exact named index inventory so a partially prepared control schema cannot
be mistaken for a healthy runtime.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from typing import Any, Mapping

from sqlalchemy import text

from server.common.schema_recovery_evidence import (
    PLAN_ACTION,
    VERIFIED_ACTION,
    EVIDENCE_TABLE,
    ensure_evidence_table,
    load_pending_physical_rewrite_plan,
    make_evidence_record,
    persist_and_verify_evidence,
    plan_sha256,
    table_content_fingerprint,
    validate_recovery_evidence_schema,
)


EXPECTED_ENGINE = "InnoDB"
EXPECTED_CHARSET = "utf8mb4"
EXPECTED_COLLATION = "utf8mb4_unicode_ci"
LEGACY_COLLATION = "utf8mb4_general_ci"
LEGACY_STORAGE_RECOVERY_SCHEMA = (
    "probiga.qmt-control-general-ci-normalization.v1"
)
LEGACY_STORAGE_RECOVERY_VERSION = (
    "qmt-control-general-ci-normalization.v1"
)
FROZEN_TABLE_RECOVERY_PLAN_SCHEMA = (
    "probiga.qmt-frozen-table-recovery-plan.v1"
)
FROZEN_TABLE_RECOVERY_PLAN_VERSION = (
    "qmt-frozen-table-recovery-plan.v1"
)
_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class FrozenColumn:
    column_type: str
    nullable: bool
    default: str | None = None
    character: bool = False
    extra: str = ""


@dataclass(frozen=True)
class FrozenIndex:
    columns: tuple[str, ...]
    unique: bool
    index_type: str = "BTREE"


@dataclass(frozen=True)
class FrozenTable:
    ddl: str
    columns: tuple[tuple[str, FrozenColumn], ...]
    indexes: Mapping[str, FrozenIndex]
    engine: str = EXPECTED_ENGINE
    collation: str = EXPECTED_COLLATION


def character_column(
    column_type: str,
    *,
    nullable: bool,
    default: str | None = None,
) -> FrozenColumn:
    return FrozenColumn(
        column_type=column_type,
        nullable=nullable,
        default=default,
        character=True,
    )


def _normalize_default(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if len(normalized) >= 2 and normalized[0] == normalized[-1] == "'":
        normalized = normalized[1:-1].replace("''", "'")
    if normalized.casefold() in {"current_timestamp", "current_timestamp()"}:
        return "current_timestamp"
    try:
        number = Decimal(normalized)
    except (InvalidOperation, ValueError):
        return normalized
    canonical = format(number, "f")
    if "." in canonical:
        canonical = canonical.rstrip("0").rstrip(".")
    return canonical or "0"


def _normalize_extra(value: Any) -> str:
    # MySQL versions disagree on whether DEFAULT_GENERATED is reported for a
    # literal/current-timestamp default.  It does not change storage semantics.
    tokens = str(value or "").casefold().replace("default_generated", " ")
    return " ".join(tokens.split())


def _row_value(row: Mapping[str, Any], name: str) -> Any:
    for candidate in (name, name.lower(), name.upper()):
        try:
            if candidate in row:
                return row[candidate]
        except TypeError:
            break
    return None


def _validate_contracts(contracts: Mapping[str, FrozenTable]) -> dict[str, FrozenTable]:
    result = dict(contracts)
    if not result:
        raise ValueError("QMT control table contracts cannot be empty")
    for table_name, contract in result.items():
        if not _IDENTIFIER.fullmatch(table_name):
            raise ValueError(f"unsafe QMT control table identifier: {table_name}")
        column_names = tuple(name for name, _ in contract.columns)
        if not column_names or len(set(column_names)) != len(column_names):
            raise ValueError(f"invalid QMT control column contract: {table_name}")
        if any(not _IDENTIFIER.fullmatch(name) for name in column_names):
            raise ValueError(f"unsafe QMT control column identifier: {table_name}")
        for index_name, index in contract.indexes.items():
            if not _IDENTIFIER.fullmatch(index_name):
                raise ValueError(f"unsafe QMT control index identifier: {index_name}")
            if not index.columns or not set(index.columns).issubset(column_names):
                raise ValueError(f"invalid QMT control index: {table_name}.{index_name}")
    return result


def _table_params(contracts: Mapping[str, FrozenTable]) -> tuple[str, dict[str, str]]:
    names = tuple(contracts)
    return (
        ", ".join(f":table_{index}" for index in range(len(names))),
        {f"table_{index}": name for index, name in enumerate(names)},
    )


def _mapping_rows(result) -> list[dict[str, Any]]:
    return [dict(row) for row in result.mappings().all()]


def _contract_payload(contracts: Mapping[str, FrozenTable]) -> dict[str, Any]:
    return {
        table_name: {
            "engine": contract.engine,
            "charset": EXPECTED_CHARSET,
            "collation": contract.collation,
            "columns": [
                {
                    "name": name,
                    "column_type": column.column_type,
                    "nullable": column.nullable,
                    "default": column.default,
                    "character": column.character,
                    "extra": column.extra,
                }
                for name, column in contract.columns
            ],
            "indexes": {
                name: {
                    "columns": list(index.columns),
                    "unique": index.unique,
                    "index_type": index.index_type,
                }
                for name, index in sorted(contract.indexes.items())
            },
        }
        for table_name, contract in sorted(contracts.items())
    }


def frozen_contract_hash(contracts: Mapping[str, FrozenTable]) -> str:
    payload = json.dumps(
        _contract_payload(_validate_contracts(contracts)),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _validate_on_connection(
    connection,
    contracts: Mapping[str, FrozenTable],
    *,
    context: str,
) -> dict[str, Any]:
    contracts = _validate_contracts(contracts)
    placeholders, params = _table_params(contracts)
    table_rows = _mapping_rows(connection.execute(text(
        "SELECT TABLE_NAME, ENGINE, TABLE_COLLATION "
        "FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN "
        f"({placeholders})"
    ), params))
    actual_tables = {
        str(_row_value(row, "TABLE_NAME") or ""): row for row in table_rows
    }
    if len(actual_tables) != len(table_rows) or set(actual_tables) != set(contracts):
        raise RuntimeError(
            f"{context} physical table inventory differs: "
            f"missing={sorted(set(contracts) - set(actual_tables))} "
            f"unexpected={sorted(set(actual_tables) - set(contracts))}"
        )
    for table_name, contract in contracts.items():
        row = actual_tables[table_name]
        engine = str(_row_value(row, "ENGINE") or "")
        collation = str(_row_value(row, "TABLE_COLLATION") or "")
        if engine.casefold() != contract.engine.casefold() or collation != contract.collation:
            raise RuntimeError(
                f"{context} physical storage differs: {table_name} "
                f"engine={engine!r} collation={collation!r}"
            )

    column_rows = _mapping_rows(connection.execute(text(
        "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_TYPE, "
        "IS_NULLABLE, COLUMN_DEFAULT, EXTRA, CHARACTER_SET_NAME, COLLATION_NAME "
        "FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN "
        f"({placeholders}) ORDER BY TABLE_NAME, ORDINAL_POSITION"
    ), params))
    by_table: dict[str, list[dict[str, Any]]] = {name: [] for name in contracts}
    for row in column_rows:
        table_name = str(_row_value(row, "TABLE_NAME") or "")
        if table_name in by_table:
            by_table[table_name].append(row)
    for table_name, contract in contracts.items():
        actual = sorted(
            by_table[table_name],
            key=lambda row: int(_row_value(row, "ORDINAL_POSITION") or 0),
        )
        actual_names = tuple(str(_row_value(row, "COLUMN_NAME") or "") for row in actual)
        expected_names = tuple(name for name, _ in contract.columns)
        if actual_names != expected_names:
            raise RuntimeError(
                f"{context} physical column inventory differs: {table_name} "
                f"expected={expected_names} actual={actual_names}"
            )
        for row, (column_name, expected) in zip(actual, contract.columns):
            actual_type = " ".join(
                str(_row_value(row, "COLUMN_TYPE") or "").casefold().split()
            )
            actual_nullable = str(_row_value(row, "IS_NULLABLE") or "").upper() == "YES"
            actual_default = _normalize_default(_row_value(row, "COLUMN_DEFAULT"))
            actual_extra = _normalize_extra(_row_value(row, "EXTRA"))
            expected_extra = _normalize_extra(expected.extra)
            if (
                actual_type != expected.column_type.casefold()
                or actual_nullable != expected.nullable
                or actual_default != _normalize_default(expected.default)
                or actual_extra != expected_extra
            ):
                raise RuntimeError(
                    f"{context} physical column differs: {table_name}.{column_name} "
                    f"type={actual_type!r} nullable={actual_nullable} "
                    f"default={actual_default!r} extra={actual_extra!r}"
                )
            charset = _row_value(row, "CHARACTER_SET_NAME")
            collation = _row_value(row, "COLLATION_NAME")
            if expected.character:
                if str(charset or "") != EXPECTED_CHARSET or str(collation or "") != contract.collation:
                    raise RuntimeError(
                        f"{context} physical column collation differs: "
                        f"{table_name}.{column_name}"
                    )
            elif charset is not None or collation is not None:
                raise RuntimeError(
                    f"{context} non-character column has collation: "
                    f"{table_name}.{column_name}"
                )

    index_rows = _mapping_rows(connection.execute(text(
        "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, "
        "SUB_PART, INDEX_TYPE FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN "
        f"({placeholders}) ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX"
    ), params))
    index_parts: dict[str, dict[str, list[dict[str, Any]]]] = {
        name: {} for name in contracts
    }
    for row in index_rows:
        table_name = str(_row_value(row, "TABLE_NAME") or "")
        index_name = str(_row_value(row, "INDEX_NAME") or "")
        if table_name in index_parts:
            index_parts[table_name].setdefault(index_name, []).append(row)
    for table_name, contract in contracts.items():
        if set(index_parts[table_name]) != set(contract.indexes):
            raise RuntimeError(
                f"{context} physical index inventory differs: {table_name} "
                f"expected={sorted(contract.indexes)} "
                f"actual={sorted(index_parts[table_name])}"
            )
        for index_name, expected in contract.indexes.items():
            rows = sorted(
                index_parts[table_name][index_name],
                key=lambda row: int(_row_value(row, "SEQ_IN_INDEX") or 0),
            )
            columns = tuple(str(_row_value(row, "COLUMN_NAME") or "") for row in rows)
            unique_values = {int(_row_value(row, "NON_UNIQUE") or 0) == 0 for row in rows}
            index_types = {str(_row_value(row, "INDEX_TYPE") or "").upper() for row in rows}
            prefix_parts = {_row_value(row, "SUB_PART") for row in rows}
            if (
                columns != expected.columns
                or unique_values != {expected.unique}
                or index_types != {expected.index_type.upper()}
                or prefix_parts != {None}
            ):
                raise RuntimeError(
                    f"{context} physical index differs: {table_name}.{index_name}"
                )

    return {
        "table_names": list(contracts),
        "table_count": len(contracts),
        "contract_hash": frozen_contract_hash(contracts),
        "physical_contract_verified": True,
        "read_only": True,
        "runtime_ddl_required": False,
    }


def validate_frozen_tables(
    engine,
    contracts: Mapping[str, FrozenTable],
    *,
    context: str,
    connection=None,
) -> dict[str, Any]:
    """Validate the complete physical contract using SELECT statements only."""

    if connection is not None:
        return _validate_on_connection(connection, contracts, context=context)
    with engine.connect() as bound_connection:
        return _validate_on_connection(bound_connection, contracts, context=context)


def _single_table_contract(
    contract: FrozenTable,
    *,
    collation: str,
) -> FrozenTable:
    return FrozenTable(
        ddl=contract.ddl,
        columns=contract.columns,
        indexes=contract.indexes,
        engine=contract.engine,
        collation=collation,
    )


def _existing_contract_tables(
    connection,
    contracts: Mapping[str, FrozenTable],
) -> set[str]:
    placeholders, params = _table_params(contracts)
    rows = _mapping_rows(connection.execute(text(
        "SELECT TABLE_NAME FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN "
        f"({placeholders})"
    ), params))
    names = {
        str(_row_value(row, "TABLE_NAME") or "")
        for row in rows
    }
    if len(names) != len(rows) or not names <= set(contracts):
        raise RuntimeError("QMT control table inventory is ambiguous")
    return names


def _evidence_table_exists(connection) -> bool:
    rows = _mapping_rows(connection.execute(text(
        "SELECT TABLE_NAME FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name"
    ), {"table_name": EVIDENCE_TABLE}))
    names = {
        str(_row_value(row, "TABLE_NAME") or "")
        for row in rows
    }
    if names not in (set(), {EVIDENCE_TABLE}) or len(names) != len(rows):
        raise RuntimeError("schema recovery evidence table inventory is ambiguous")
    return bool(names)


def _classify_existing_storage(
    connection,
    *,
    table_name: str,
    contract: FrozenTable,
    context: str,
) -> str:
    one = {table_name: contract}
    try:
        _validate_on_connection(connection, one, context=context)
    except RuntimeError:
        legacy_contract = _single_table_contract(
            contract,
            collation=LEGACY_COLLATION,
        )
        try:
            _validate_on_connection(
                connection,
                {table_name: legacy_contract},
                context=context,
            )
        except RuntimeError as legacy_error:
            raise RuntimeError(
                f"{context} existing table has unsupported physical drift: "
                f"{table_name}"
            ) from legacy_error
        return "legacy_general_ci"
    return "target"


def _fingerprint_shape(contract: FrozenTable) -> tuple[tuple[str, ...], tuple[str, ...]]:
    columns = tuple(name for name, _column in contract.columns)
    primary = contract.indexes.get("PRIMARY")
    order_by = (
        tuple(primary.columns)
        if primary is not None and primary.unique
        else columns
    )
    if not order_by or not set(order_by) <= set(columns):
        raise RuntimeError("QMT control fingerprint ordering differs")
    return columns, order_by


def _storage_plan_payload(
    *,
    table_name: str,
    contract: FrozenTable,
    context: str,
    before_fingerprint: Mapping[str, Any],
) -> dict[str, Any]:
    columns, order_by = _fingerprint_shape(contract)
    before = _validated_fingerprint(
        before_fingerprint,
        context=f"{context} QMT storage PLAN fingerprint",
    )
    legacy_contract = _single_table_contract(
        contract,
        collation=LEGACY_COLLATION,
    )
    return {
        "schema": LEGACY_STORAGE_RECOVERY_SCHEMA,
        "context": context,
        "table": table_name,
        "engine": contract.engine,
        "from_collation": LEGACY_COLLATION,
        "to_collation": contract.collation,
        "legacy_contract_hash": frozen_contract_hash(
            {table_name: legacy_contract}
        ),
        "target_contract_hash": frozen_contract_hash(
            {table_name: contract}
        ),
        "before_fingerprint": before,
        "fingerprint_columns": list(columns),
        "order_by": list(order_by),
    }


def _validated_fingerprint(
    value: Mapping[str, Any],
    *,
    context: str,
) -> dict[str, Any]:
    if (
        not isinstance(value, Mapping)
        or set(value) != {"row_count", "content_sha256"}
        or type(value.get("row_count")) is not int
        or int(value["row_count"]) < 0
        or _SHA256.fullmatch(str(value.get("content_sha256") or "")) is None
    ):
        raise RuntimeError(f"{context} differs")
    return {
        "row_count": int(value["row_count"]),
        "content_sha256": str(value["content_sha256"]),
    }


def _validate_pending_storage_plan(
    pending: Mapping[str, Any],
    *,
    table_name: str,
    contract: FrozenTable,
    context: str,
) -> dict[str, Any]:
    record = pending.get("record")
    payload = pending.get("plan_payload")
    source_row = pending.get("source_row")
    if (
        not isinstance(record, Mapping)
        or not isinstance(payload, Mapping)
        or not isinstance(source_row, Mapping)
        or pending.get("business_key") != {"table": table_name}
        or record.get("recovery_version") != LEGACY_STORAGE_RECOVERY_VERSION
        or record.get("source_table") != table_name
        or type(record.get("source_row_id")) is not int
        or record.get("source_row_id") != 0
        or record.get("action") != PLAN_ACTION
        or payload != source_row
    ):
        raise RuntimeError(
            f"{context} pending QMT storage PLAN identity differs: {table_name}"
        )
    expected = _storage_plan_payload(
        table_name=table_name,
        contract=contract,
        context=context,
        before_fingerprint=payload.get("before_fingerprint") or {},
    )
    if dict(payload) != expected:
        raise RuntimeError(
            f"{context} pending QMT storage PLAN contract differs: {table_name}"
        )
    expected_hash = plan_sha256(
        recovery_version=LEGACY_STORAGE_RECOVERY_VERSION,
        payload=expected,
    )
    if (
        str(pending.get("plan_sha256") or "") != expected_hash
        or str(record.get("plan_sha256") or "") != expected_hash
    ):
        raise RuntimeError(
            f"{context} pending QMT storage PLAN hash differs: {table_name}"
        )
    return expected


def _verify_storage_plan_content(
    connection,
    *,
    table_name: str,
    payload: Mapping[str, Any],
    context: str,
) -> dict[str, Any]:
    before = _validated_fingerprint(
        payload.get("before_fingerprint"),
        context=f"{context} QMT storage PLAN fingerprint",
    )
    current = table_content_fingerprint(
        connection,
        table_name,
        order_by=tuple(payload["order_by"]),
        columns=tuple(payload["fingerprint_columns"]),
    )
    if current != before:
        raise RuntimeError(
            f"{context} source content changed after QMT storage PLAN: "
            f"{table_name}"
        )
    return current


def _make_storage_plan(
    connection,
    *,
    table_name: str,
    contract: FrozenTable,
    context: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    columns, order_by = _fingerprint_shape(contract)
    before = table_content_fingerprint(
        connection,
        table_name,
        order_by=order_by,
        columns=columns,
    )
    payload = _storage_plan_payload(
        table_name=table_name,
        contract=contract,
        context=context,
        before_fingerprint=before,
    )
    plan_hash = plan_sha256(
        recovery_version=LEGACY_STORAGE_RECOVERY_VERSION,
        payload=payload,
    )
    record = make_evidence_record(
        recovery_version=LEGACY_STORAGE_RECOVERY_VERSION,
        source_table=table_name,
        source_row_id=0,
        action=PLAN_ACTION,
        business_key={"table": table_name},
        source_row=payload,
        plan_payload=payload,
        plan_hash=plan_hash,
    )
    return payload, record


def _validate_target_storage_contracts(
    contracts: Mapping[str, FrozenTable],
) -> dict[str, FrozenTable]:
    validated = _validate_contracts(contracts)
    if any(
        contract.engine.casefold() != EXPECTED_ENGINE.casefold()
        or contract.collation != EXPECTED_COLLATION
        for contract in validated.values()
    ):
        raise ValueError("QMT frozen target storage differs")
    return validated


def plan_frozen_table_recovery(
    engine,
    contracts: Mapping[str, FrozenTable],
    *,
    context: str,
) -> dict[str, Any]:
    """Return a deterministic, read-only recovery plan for QMT tables."""

    contracts = _validate_target_storage_contracts(contracts)
    with engine.connect() as connection:
        existing = _existing_contract_tables(connection, contracts)
        storage_state = {
            table_name: _classify_existing_storage(
                connection,
                table_name=table_name,
                contract=contracts[table_name],
                context=context,
            )
            for table_name in sorted(existing)
        }
        evidence_available = _evidence_table_exists(connection)
        if evidence_available:
            # Preflight may run before the broker installs missing immutable
            # guards.  The exact table must still be readable; a pending PLAN
            # additionally requires the full append-only trigger boundary.
            validate_recovery_evidence_schema(
                None,
                connection=connection,
                require_triggers=False,
            )

        pending_by_table: dict[str, dict[str, Any]] = {}
        if evidence_available:
            for table_name, contract in contracts.items():
                pending = load_pending_physical_rewrite_plan(
                    connection,
                    recovery_version=LEGACY_STORAGE_RECOVERY_VERSION,
                    source_table=table_name,
                )
                if pending is None:
                    continue
                if table_name not in existing:
                    raise RuntimeError(
                        f"{context} pending QMT storage PLAN source is missing: "
                        f"{table_name}"
                    )
                payload = _validate_pending_storage_plan(
                    pending,
                    table_name=table_name,
                    contract=contract,
                    context=context,
                )
                current = _verify_storage_plan_content(
                    connection,
                    table_name=table_name,
                    payload=payload,
                    context=context,
                )
                pending_by_table[table_name] = {
                    "pending": dict(pending),
                    "payload": payload,
                    "current_fingerprint": current,
                }
        if pending_by_table:
            validate_recovery_evidence_schema(
                None,
                connection=connection,
                require_triggers=True,
            )

        table_plans: dict[str, dict[str, Any]] = {}
        for table_name, contract in sorted(contracts.items()):
            columns, order_by = _fingerprint_shape(contract)
            target_contract_hash = frozen_contract_hash({table_name: contract})
            if table_name not in existing:
                table_plans[table_name] = {
                    "state": "MISSING",
                    "action": "CREATE",
                    "target_contract_hash": target_contract_hash,
                    "fingerprint_columns": list(columns),
                    "order_by": list(order_by),
                    "content_fingerprint": None,
                    "pending_plan": False,
                    "pending_plan_sha256": None,
                    "storage_plan_sha256": None,
                }
                continue

            state = storage_state[table_name]
            pending_detail = pending_by_table.get(table_name)
            if pending_detail is not None:
                fingerprint = dict(pending_detail["current_fingerprint"])
                pending_hash = str(
                    pending_detail["pending"]["plan_sha256"]
                )
                storage_plan_hash = pending_hash
                action = (
                    "RESUME_COLLATION_NORMALIZATION"
                    if state == "legacy_general_ci"
                    else "FINALIZE_PENDING_VERIFICATION"
                )
            else:
                fingerprint = table_content_fingerprint(
                    connection,
                    table_name,
                    order_by=order_by,
                    columns=columns,
                )
                pending_hash = None
                storage_plan_hash = (
                    plan_sha256(
                        recovery_version=LEGACY_STORAGE_RECOVERY_VERSION,
                        payload=_storage_plan_payload(
                            table_name=table_name,
                            contract=contract,
                            context=context,
                            before_fingerprint=fingerprint,
                        ),
                    )
                    if state == "legacy_general_ci" else None
                )
                action = (
                    "NORMALIZE_COLLATION"
                    if state == "legacy_general_ci" else "NONE"
                )
            table_plans[table_name] = {
                "state": (
                    "EXACT_GENERAL_CI"
                    if state == "legacy_general_ci" else "TARGET"
                ),
                "action": action,
                "target_contract_hash": target_contract_hash,
                "fingerprint_columns": list(columns),
                "order_by": list(order_by),
                "content_fingerprint": _validated_fingerprint(
                    fingerprint,
                    context=f"{context} recovery fingerprint",
                ),
                "pending_plan": pending_detail is not None,
                "pending_plan_sha256": pending_hash,
                "storage_plan_sha256": storage_plan_hash,
            }

    state_counts = {
        state: sum(detail["state"] == state for detail in table_plans.values())
        for state in ("MISSING", "TARGET", "EXACT_GENERAL_CI")
    }
    missing_names = sorted(
        name for name, detail in table_plans.items()
        if detail["state"] == "MISSING"
    )
    target_names = sorted(
        name for name, detail in table_plans.items()
        if detail["state"] == "TARGET"
    )
    legacy_names = sorted(
        name for name, detail in table_plans.items()
        if detail["state"] == "EXACT_GENERAL_CI"
    )
    pending_names = sorted(pending_by_table)
    payload = {
        "schema": FROZEN_TABLE_RECOVERY_PLAN_SCHEMA,
        "context": context,
        "target_contract_hash": frozen_contract_hash(contracts),
        "tables": table_plans,
    }
    plan_hash = plan_sha256(
        recovery_version=FROZEN_TABLE_RECOVERY_PLAN_VERSION,
        payload=payload,
    )
    migration_required = bool(
        state_counts["MISSING"]
        or state_counts["EXACT_GENERAL_CI"]
        or pending_names
    )
    return {
        **payload,
        "recovery_version": FROZEN_TABLE_RECOVERY_PLAN_VERSION,
        "table_count": len(contracts),
        "state_counts": state_counts,
        "missing_table_names": missing_names,
        "target_table_names": target_names,
        "legacy_general_ci_table_names": legacy_names,
        "pending_plan_count": len(pending_names),
        "pending_table_names": pending_names,
        "migration_required": migration_required,
        "ready_for_privileged_apply": True,
        "plan_sha256": plan_hash,
        "read_only": True,
        "runtime_ddl_required": False,
    }


def privileged_migrate_frozen_tables(
    engine,
    contracts: Mapping[str, FrozenTable],
    *,
    context: str,
) -> dict[str, Any]:
    """Create frozen QMT tables inside an explicit privileged release window.

    ``CREATE IF NOT EXISTS`` is intentionally followed by the strict validator.
    Existing column/index drift is never silently rewritten or dropped.
    """

    contracts = _validate_target_storage_contracts(contracts)
    normalized_tables: list[str] = []
    resumed_tables: list[str] = []
    recovery_evidence: dict[str, Any] = {}
    with engine.begin() as connection:
        existing = _existing_contract_tables(connection, contracts)
        storage_state = {
            table_name: _classify_existing_storage(
                connection,
                table_name=table_name,
                contract=contracts[table_name],
                context=context,
            )
            for table_name in sorted(existing)
        }

        evidence_available = _evidence_table_exists(connection)
        if (
            any(state == "legacy_general_ci" for state in storage_state.values())
            or evidence_available
        ):
            # Existing evidence triggers are part of the production broker's
            # trust boundary.  A legacy rewrite outside that boundary fails.
            ensure_evidence_table(connection)

        pending_by_table: dict[str, dict[str, Any]] = {}
        if evidence_available:
            for table_name, contract in contracts.items():
                pending = load_pending_physical_rewrite_plan(
                    connection,
                    recovery_version=LEGACY_STORAGE_RECOVERY_VERSION,
                    source_table=table_name,
                )
                if pending is None:
                    continue
                if table_name not in existing:
                    raise RuntimeError(
                        f"{context} pending QMT storage PLAN source is missing: "
                        f"{table_name}"
                    )
                payload = _validate_pending_storage_plan(
                    pending,
                    table_name=table_name,
                    contract=contract,
                    context=context,
                )
                _verify_storage_plan_content(
                    connection,
                    table_name=table_name,
                    payload=payload,
                    context=context,
                )
                pending_by_table[table_name] = dict(pending)

        plan_records: list[dict[str, Any]] = []
        plan_payloads: dict[str, dict[str, Any]] = {}
        for table_name, state in storage_state.items():
            if state != "legacy_general_ci" or table_name in pending_by_table:
                continue
            payload, record = _make_storage_plan(
                connection,
                table_name=table_name,
                contract=contracts[table_name],
                context=context,
            )
            plan_payloads[table_name] = payload
            plan_records.append(record)
        plan_evidence = persist_and_verify_evidence(
            connection,
            plan_records,
        )

        # No table is created or rewritten until every pre-existing table has
        # matched either the exact target or exact legacy-general_ci contract,
        # and every legacy table has a durable content-hashed PLAN.
        for contract in contracts.values():
            connection.execute(text(contract.ddl))

        verified_records: list[dict[str, Any]] = []
        for table_name, contract in contracts.items():
            pending = pending_by_table.get(table_name)
            state = storage_state.get(table_name)
            if state != "legacy_general_ci" and pending is None:
                continue
            payload = (
                _validate_pending_storage_plan(
                    pending,
                    table_name=table_name,
                    contract=contract,
                    context=context,
                )
                if pending is not None
                else plan_payloads[table_name]
            )
            if state == "legacy_general_ci":
                connection.execute(text(
                    f"ALTER TABLE `{table_name}` ENGINE={EXPECTED_ENGINE}, "
                    f"CONVERT TO CHARACTER SET {EXPECTED_CHARSET} "
                    f"COLLATE {EXPECTED_COLLATION}"
                ))
                normalized_tables.append(table_name)
            else:
                resumed_tables.append(table_name)

            _validate_on_connection(
                connection,
                {table_name: contract},
                context=context,
            )
            after = table_content_fingerprint(
                connection,
                table_name,
                order_by=tuple(payload["order_by"]),
                columns=tuple(payload["fingerprint_columns"]),
            )
            before = dict(payload["before_fingerprint"])
            if after != before:
                raise RuntimeError(
                    f"{context} content fingerprint changed during QMT "
                    f"storage normalization: {table_name}"
                )
            plan_hash = plan_sha256(
                recovery_version=LEGACY_STORAGE_RECOVERY_VERSION,
                payload=payload,
            )
            verified_records.append(make_evidence_record(
                recovery_version=LEGACY_STORAGE_RECOVERY_VERSION,
                source_table=table_name,
                source_row_id=0,
                action=VERIFIED_ACTION,
                business_key={"table": table_name},
                source_row={
                    **payload,
                    "after_fingerprint": after,
                    "content_verified": True,
                },
                plan_payload=payload,
                plan_hash=plan_hash,
            ))
            recovery_evidence[table_name] = {
                "plan_sha256": plan_hash,
                "before_fingerprint": before,
                "after_fingerprint": after,
                "content_verified": True,
                "resumed_pending_plan": pending is not None,
            }
        verified_evidence = persist_and_verify_evidence(
            connection,
            verified_records,
        )
        result = _validate_on_connection(connection, contracts, context=context)
    return {
        **result,
        "migrated_table_count": len(contracts),
        "normalized_legacy_table_names": sorted(normalized_tables),
        "normalized_legacy_table_count": len(normalized_tables),
        "resumed_legacy_table_names": sorted(resumed_tables),
        "recovery_plan_evidence": plan_evidence,
        "recovery_verified_evidence": verified_evidence,
        "recovery_evidence": recovery_evidence,
        "privileged_migration": True,
        "read_only": False,
    }


__all__ = [
    "EXPECTED_CHARSET",
    "EXPECTED_COLLATION",
    "EXPECTED_ENGINE",
    "FROZEN_TABLE_RECOVERY_PLAN_SCHEMA",
    "FROZEN_TABLE_RECOVERY_PLAN_VERSION",
    "LEGACY_COLLATION",
    "LEGACY_STORAGE_RECOVERY_SCHEMA",
    "LEGACY_STORAGE_RECOVERY_VERSION",
    "FrozenColumn",
    "FrozenIndex",
    "FrozenTable",
    "character_column",
    "frozen_contract_hash",
    "plan_frozen_table_recovery",
    "privileged_migrate_frozen_tables",
    "validate_frozen_tables",
]
