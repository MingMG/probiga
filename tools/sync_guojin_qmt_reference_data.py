from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.bigqmt import bridge as bigqmt_bridge
from integrations.bigqmt.release_identity import (
    validate_strategy_release_payload,
)
from integrations.qmt import bridge
from integrations.qmt.backend import to_qmt_symbol
from integrations.qmt.info import (
    CORE_INDEXES,
    DEFAULT_STOCK_SECTORS,
    to_qmt_index_symbol,
)
from integrations.qmt.safe_upsert import safe_upsert_rows
from integrations.bigqmt.reference import fetch_sector_datasets
from server.common.batch_db import create_batch_engine, write_frame
from server.common.config import get_mysql_url
from server.common.mysql_metadata_compat import (
    normalize_mysql_referential_rule,
)
from server.common.qmt_stock_catalog import (
    NATIVE_A_SHARE_SECTORS,
    build_catalog_discovery,
    build_catalog_manifest,
    canonical_catalog_members,
    insert_catalog_batch,
    load_stock_catalog,
    privileged_migrate_stock_catalog_schema,
    stock_catalog_migration_statements,
    stock_catalog_table_ddl_statements,
    stock_catalog_trigger_ddl_statements,
    validate_stock_catalog_immutability,
)
from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_trade_calendar import (
    build_calendar_manifest,
    calendar_source_batch_id,
    insert_trade_calendar_receipt,
    privileged_migrate_trade_calendar_schema,
    trade_calendar_migration_statements,
    trade_calendar_table_ddl_statements,
    trade_calendar_trigger_ddl_statements,
    validate_trade_calendar_immutability,
)


PROVIDER_ID = "gj_qmt"
BIGQMT_STRATEGY_SOURCE = (
    ROOT
    / "integrations"
    / "bigqmt"
    / "qmt_strategy"
    / "probiga_big_qmt_bridge.py"
)
INDEX_WEIGHT_PUBLICATION_RECEIPT_SCHEMA = (
    "probiga.qmt-index-weight-publication-receipt.v1"
)
HISTORICAL_INSTRUMENT_ARCHIVE_SCHEMA = (
    "probiga.qmt-historical-instrument-archive.v1"
)
REFERENCE_SCHEMA_CONTRACT_KEY = "qmt_reference_truth_v2"
_REFERENCE_SEAL_TABLE_DDL = ("""
    CREATE TABLE IF NOT EXISTS qmt_reference_schema_contract (
        contract_key VARCHAR(64) NOT NULL PRIMARY KEY,
        contract_hash CHAR(64) NOT NULL,
        table_contract_json MEDIUMTEXT NULL,
        table_contract_hash CHAR(64) NULL,
        trigger_contract_json MEDIUMTEXT NULL,
        trigger_contract_hash CHAR(64) NULL,
        created_at DATETIME NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
""",)
_REFERENCE_SEAL_MIGRATION_DDL = (
    """
    ALTER TABLE qmt_reference_schema_contract
    ADD COLUMN IF NOT EXISTS table_contract_json MEDIUMTEXT NULL
        AFTER contract_hash
    """,
    """
    ALTER TABLE qmt_reference_schema_contract
    ADD COLUMN IF NOT EXISTS table_contract_hash CHAR(64) NULL
        AFTER table_contract_json
    """,
    """
    ALTER TABLE qmt_reference_schema_contract
    ADD COLUMN IF NOT EXISTS trigger_contract_json MEDIUMTEXT NULL
        AFTER table_contract_hash
    """,
    """
    ALTER TABLE qmt_reference_schema_contract
    ADD COLUMN IF NOT EXISTS trigger_contract_hash CHAR(64) NULL
        AFTER trigger_contract_json
    """,
    """
    ALTER TABLE qmt_reference_schema_contract
    CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    """,
)
_REFERENCE_SEAL_TRIGGER_DDL = (
    """
    CREATE TRIGGER IF NOT EXISTS trg_qmt_reference_contract_no_update
    BEFORE UPDATE ON qmt_reference_schema_contract
    FOR EACH ROW SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT='qmt_reference_schema_contract is append-only'
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_qmt_reference_contract_no_delete
    BEFORE DELETE ON qmt_reference_schema_contract
    FOR EACH ROW SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT='qmt_reference_schema_contract is append-only'
    """,
)
_CONDITIONAL_ADD_COLUMN_RE = re.compile(
    r"^\s*ALTER\s+TABLE\s+`?([A-Za-z_][A-Za-z0-9_]*)`?\s+"
    r"ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+"
    r"`?([A-Za-z_][A-Za-z0-9_]*)`?\b",
    flags=re.IGNORECASE | re.DOTALL,
)


def release_bound_reference_batch_id(
    release_build_sha: str, *, captured_at: datetime | None = None,
) -> str:
    """Return one VARCHAR(64)-safe batch identity bound to a full Git SHA."""

    normalized = str(release_build_sha or "").strip().lower()
    if (
        re.fullmatch(r"[0-9a-f]{40}", normalized) is None
        or normalized == "0" * 40
    ):
        raise ValueError("release_build_sha must be one nonzero Git SHA")
    sample = captured_at or datetime.now()
    return f"qmt_rel_{normalized}_{sample.strftime('%Y%m%d%H%M%S')}"


def reference_table_ddl_contracts() -> tuple[str, ...]:
    return (
        *stock_catalog_table_ddl_statements(),
        *trade_calendar_table_ddl_statements(),
        *_REFERENCE_SEAL_TABLE_DDL,
    )


def reference_migration_ddl_contracts() -> tuple[str, ...]:
    return (
        *stock_catalog_migration_statements(),
        *trade_calendar_migration_statements(),
        *_REFERENCE_SEAL_MIGRATION_DDL,
    )


def reference_trigger_ddl_contracts() -> tuple[str, ...]:
    return (
        *stock_catalog_trigger_ddl_statements(),
        *trade_calendar_trigger_ddl_statements(),
        *_REFERENCE_SEAL_TRIGGER_DDL,
    )


def execute_reference_ddl_contracts(
    connection: Any,
    statements: Iterable[str],
) -> None:
    """Execute frozen reference DDL on MySQL with idempotent column adds."""

    for statement in statements:
        matched = _CONDITIONAL_ADD_COLUMN_RE.match(statement)
        if matched is None:
            if "ADD COLUMN IF NOT EXISTS" in statement.upper():
                raise RuntimeError("QMT reference additive DDL contract differs")
            connection.execute(text(statement))
            continue
        table_name, column_name = matched.groups()
        rows = connection.execute(text("""
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=DATABASE()
              AND TABLE_NAME=:table_name
              AND COLUMN_NAME=:column_name
        """), {
            "table_name": table_name,
            "column_name": column_name,
        }).mappings().all()
        if rows:
            continue
        compatible, replacements = re.subn(
            r"ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS",
            "ADD COLUMN",
            statement,
            count=1,
            flags=re.IGNORECASE,
        )
        if replacements != 1:
            raise RuntimeError("QMT reference additive DDL contract differs")
        connection.execute(text(compatible))


def _normalized_ddl(statement: str) -> str:
    return " ".join(str(statement).split())


REFERENCE_SCHEMA_CONTRACT_HASH = canonical_digest({
    "schema": "probiga.qmt-reference-schema-contract.v1",
    "table_ddl": [_normalized_ddl(value)
                  for value in reference_table_ddl_contracts()],
    "migration_ddl": [_normalized_ddl(value)
                      for value in reference_migration_ddl_contracts()],
    "trigger_ddl": [_normalized_ddl(value)
                    for value in reference_trigger_ddl_contracts()],
})

_REFERENCE_TRUTH_TABLES = (
    "qmt_stock_catalog_batch",
    "qmt_stock_catalog_member",
    "qmt_trade_calendar_batch",
    "qmt_trade_calendar_session",
    "qmt_reference_schema_contract",
)
_REFERENCE_COLUMN_CONTRACTS: dict[str, tuple[tuple[Any, ...], ...]] = {
    "qmt_stock_catalog_batch": (
        ("batch_id", "varchar", 64, None, None, "NO", None, ""),
        ("captured_at", "datetime", None, None, None, "NO", None, ""),
        ("history_complete_from", "date", None, None, None, "NO", None, ""),
        ("status", "varchar", 16, None, None, "NO", None, ""),
        ("member_count", "int", None, 10, 0, "NO", None, ""),
        ("member_set_hash", "char", 64, None, None, "NO", None, ""),
        ("manifest_json", "mediumtext", 16777215, None, None, "NO", None, ""),
        ("manifest_hash", "char", 64, None, None, "NO", None, ""),
        ("created_at", "datetime", None, None, None, "NO", None, ""),
    ),
    "qmt_stock_catalog_member": (
        ("batch_id", "varchar", 64, None, None, "NO", None, ""),
        ("qmt_code", "varchar", 64, None, None, "NO", None, ""),
        ("stock_code", "varchar", 16, None, None, "NO", None, ""),
        ("list_date", "date", None, None, None, "NO", None, ""),
        ("expire_date", "date", None, None, None, "YES", None, ""),
        ("instrument_batch_id", "varchar", 64, None, None, "NO", None, ""),
        ("instrument_type", "varchar", 32, None, None, "NO", None, ""),
        ("created_at", "datetime", None, None, None, "NO", None, ""),
    ),
    "qmt_trade_calendar_batch": (
        ("batch_id", "varchar", 64, None, None, "NO", None, ""),
        ("source_batch_id", "varchar", 64, None, None, "NO", None, ""),
        ("known_at", "datetime", None, None, None, "NO", None, ""),
        ("start_date", "date", None, None, None, "NO", None, ""),
        ("end_date", "date", None, None, None, "NO", None, ""),
        ("status", "varchar", 16, None, None, "NO", None, ""),
        ("session_count", "int", None, 10, 0, "NO", None, ""),
        ("session_set_hash", "char", 64, None, None, "NO", None, ""),
        ("manifest_json", "mediumtext", 16777215, None, None, "NO", None, ""),
        ("manifest_hash", "char", 64, None, None, "NO", None, ""),
        ("created_at", "datetime", None, None, None, "NO", None, ""),
    ),
    "qmt_trade_calendar_session": (
        ("batch_id", "varchar", 64, None, None, "NO", None, ""),
        ("trade_date", "date", None, None, None, "NO", None, ""),
        ("created_at", "datetime", None, None, None, "NO", None, ""),
    ),
    "qmt_reference_schema_contract": (
        ("contract_key", "varchar", 64, None, None, "NO", None, ""),
        ("contract_hash", "char", 64, None, None, "NO", None, ""),
        ("table_contract_json", "mediumtext", 16777215, None, None, "YES", None, ""),
        ("table_contract_hash", "char", 64, None, None, "YES", None, ""),
        ("trigger_contract_json", "mediumtext", 16777215, None, None, "YES", None, ""),
        ("trigger_contract_hash", "char", 64, None, None, "YES", None, ""),
        ("created_at", "datetime", None, None, None, "NO", None, ""),
    ),
}
_REFERENCE_INDEX_CONTRACTS: dict[str, dict[str, tuple[int, tuple[str, ...]]]] = {
    "qmt_stock_catalog_batch": {
        "PRIMARY": (0, ("batch_id",)),
        "idx_qmt_stock_catalog_complete": (1, ("status", "captured_at")),
    },
    "qmt_stock_catalog_member": {
        "PRIMARY": (0, ("batch_id", "qmt_code")),
        "uk_qmt_stock_catalog_stock": (0, ("batch_id", "stock_code")),
        "idx_qmt_stock_catalog_target": (
            1, ("batch_id", "list_date", "expire_date", "stock_code"),
        ),
    },
    "qmt_trade_calendar_batch": {
        "PRIMARY": (0, ("batch_id",)),
        "idx_qmt_calendar_complete": (
            1, ("status", "start_date", "end_date", "known_at"),
        ),
    },
    "qmt_trade_calendar_session": {
        "PRIMARY": (0, ("batch_id", "trade_date")),
        "idx_qmt_calendar_session_date": (1, ("trade_date", "batch_id")),
    },
    "qmt_reference_schema_contract": {
        "PRIMARY": (0, ("contract_key",)),
    },
}
_REFERENCE_FOREIGN_KEY_CONTRACTS = {
    "qmt_stock_catalog_member": {
        "fk_qmt_stock_catalog_member_batch": (
            ("batch_id",), "qmt_stock_catalog_batch", ("batch_id",),
            "RESTRICT", "RESTRICT",
        ),
    },
    "qmt_trade_calendar_session": {
        "fk_qmt_calendar_session_batch": (
            ("batch_id",), "qmt_trade_calendar_batch", ("batch_id",),
            "RESTRICT", "RESTRICT",
        ),
    },
}
_REFERENCE_TRIGGER_CONTRACTS = {
    "trg_qmt_stock_catalog_batch_no_update": (
        "qmt_stock_catalog_batch", "UPDATE",
        "qmt_stock_catalog_batch is append-only",
    ),
    "trg_qmt_stock_catalog_batch_no_delete": (
        "qmt_stock_catalog_batch", "DELETE",
        "qmt_stock_catalog_batch is append-only",
    ),
    "trg_qmt_stock_catalog_member_no_update": (
        "qmt_stock_catalog_member", "UPDATE",
        "qmt_stock_catalog_member is append-only",
    ),
    "trg_qmt_stock_catalog_member_no_delete": (
        "qmt_stock_catalog_member", "DELETE",
        "qmt_stock_catalog_member is append-only",
    ),
    "trg_qmt_calendar_batch_no_update": (
        "qmt_trade_calendar_batch", "UPDATE",
        "qmt_trade_calendar_batch is append-only",
    ),
    "trg_qmt_calendar_batch_no_delete": (
        "qmt_trade_calendar_batch", "DELETE",
        "qmt_trade_calendar_batch is append-only",
    ),
    "trg_qmt_calendar_session_no_update": (
        "qmt_trade_calendar_session", "UPDATE",
        "qmt_trade_calendar_session is append-only",
    ),
    "trg_qmt_calendar_session_no_delete": (
        "qmt_trade_calendar_session", "DELETE",
        "qmt_trade_calendar_session is append-only",
    ),
    "trg_qmt_reference_contract_no_update": (
        "qmt_reference_schema_contract", "UPDATE",
        "qmt_reference_schema_contract is append-only",
    ),
    "trg_qmt_reference_contract_no_delete": (
        "qmt_reference_schema_contract", "DELETE",
        "qmt_reference_schema_contract is append-only",
    ),
}
REFERENCE_TABLE_NAMES = _REFERENCE_TRUTH_TABLES
REFERENCE_TRIGGER_NAMES = tuple(sorted(_REFERENCE_TRIGGER_CONTRACTS))


def _inspector_data_type(value: Any) -> str:
    raw = str(value or "").strip().lower().split("(", 1)[0].split()[0]
    return {"integer": "int"}.get(raw, raw)


def preflight_reference_tables(engine: Engine) -> dict[str, Any]:
    """Read-only first-install/upgrade preflight for the privileged broker."""

    inspector = inspect(engine)
    present: list[str] = []
    absent: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    conflicts: list[str] = []
    migratable_columns = {
        ("qmt_stock_catalog_batch", "history_complete_from"),
        ("qmt_stock_catalog_member", "instrument_type"),
    }
    for table_name in REFERENCE_TABLE_NAMES:
        if not inspector.has_table(table_name):
            absent.append(table_name)
            continue
        present.append(table_name)
        observed_rows = list(inspector.get_columns(table_name))
        observed_names = [str(row.get("name") or "") for row in observed_rows]
        expected = _REFERENCE_COLUMN_CONTRACTS[table_name]
        expected_by_name = {str(row[0]): row for row in expected}
        unknown = sorted(set(observed_names) - set(expected_by_name))
        if unknown:
            conflicts.append(f"{table_name}:unknown_columns={unknown!r}")
        expected_order = [str(row[0]) for row in expected]
        relative_expected = [
            name for name in expected_order if name in set(observed_names)
        ]
        if observed_names != relative_expected:
            conflicts.append(f"{table_name}:column_order_conflict")
        missing = [name for name in expected_order if name not in observed_names]
        if missing:
            missing_columns[table_name] = missing
        for row in observed_rows:
            name = str(row.get("name") or "")
            if name not in expected_by_name:
                continue
            expected_row = expected_by_name[name]
            actual_type = _inspector_data_type(row.get("type"))
            actual_nullable = "YES" if bool(row.get("nullable")) else "NO"
            actual_length = getattr(row.get("type"), "length", None)
            if actual_type != expected_row[1]:
                conflicts.append(
                    f"{table_name}.{name}:type={actual_type!r}"
                )
            if (
                expected_row[2] is not None
                and actual_length is not None
                and int(actual_length) != int(expected_row[2])
            ):
                conflicts.append(
                    f"{table_name}.{name}:length={actual_length!r}"
                )
            if (
                actual_nullable != expected_row[5]
                and (table_name, name) not in migratable_columns
            ):
                conflicts.append(
                    f"{table_name}.{name}:nullable={actual_nullable}"
                )
    if conflicts:
        raise RuntimeError(
            "QMT reference preflight found unsafe schema conflicts: "
            + "; ".join(conflicts)
        )
    state = (
        "EMPTY" if not present
        else "MIGRATION_REQUIRED" if absent or missing_columns
        else "READY_FOR_PHYSICAL_ATTESTATION"
    )
    return {
        "status": state,
        "read_only": True,
        "contract_key": REFERENCE_SCHEMA_CONTRACT_KEY,
        "contract_hash": REFERENCE_SCHEMA_CONTRACT_HASH,
        "table_names": list(REFERENCE_TABLE_NAMES),
        "trigger_names": list(REFERENCE_TRIGGER_NAMES),
        "present_tables": present,
        "absent_tables": absent,
        "missing_columns": missing_columns,
        "table_ddl_count": len(reference_table_ddl_contracts()),
        "migration_ddl_count": len(reference_migration_ddl_contracts()),
        "trigger_ddl_count": len(reference_trigger_ddl_contracts()),
    }


def _normalized_schema_default(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value)
    if len(raw) >= 2 and raw[0] == raw[-1] == "'":
        raw = raw[1:-1]
    return raw


def _reference_table_physical_snapshot(connection: Any) -> dict[str, Any]:
    names_sql = ", ".join(f"'{name}'" for name in _REFERENCE_TRUTH_TABLES)
    table_rows = connection.execute(text(
        "SELECT TABLE_NAME AS table_name, ENGINE AS engine, "
        "TABLE_COLLATION AS table_collation FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN (" + names_sql + ") "
        "ORDER BY BINARY TABLE_NAME"
    )).mappings().all()
    column_rows = connection.execute(text(
        "SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name, "
        "ORDINAL_POSITION AS ordinal_position, DATA_TYPE AS data_type, "
        "CHARACTER_MAXIMUM_LENGTH AS character_maximum_length, "
        "NUMERIC_PRECISION AS numeric_precision, NUMERIC_SCALE AS numeric_scale, "
        "IS_NULLABLE AS is_nullable, COLUMN_DEFAULT AS column_default, "
        "EXTRA AS extra, CHARACTER_SET_NAME AS character_set_name, "
        "COLLATION_NAME AS collation_name FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN (" + names_sql + ") "
        "ORDER BY BINARY TABLE_NAME, ORDINAL_POSITION"
    )).mappings().all()
    index_rows = connection.execute(text(
        "SELECT TABLE_NAME AS table_name, INDEX_NAME AS index_name, "
        "NON_UNIQUE AS non_unique, SEQ_IN_INDEX AS seq_in_index, "
        "COLUMN_NAME AS column_name, SUB_PART AS sub_part, INDEX_TYPE AS index_type "
        "FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() "
        "AND TABLE_NAME IN (" + names_sql + ") "
        "ORDER BY BINARY TABLE_NAME, BINARY INDEX_NAME, SEQ_IN_INDEX"
    )).mappings().all()
    foreign_rows = connection.execute(text(
        "SELECT k.TABLE_NAME AS table_name, k.CONSTRAINT_NAME AS constraint_name, "
        "k.ORDINAL_POSITION AS ordinal_position, k.COLUMN_NAME AS column_name, "
        "k.REFERENCED_TABLE_NAME AS referenced_table_name, "
        "k.REFERENCED_COLUMN_NAME AS referenced_column_name, "
        "r.UPDATE_RULE AS update_rule, r.DELETE_RULE AS delete_rule "
        "FROM information_schema.KEY_COLUMN_USAGE k "
        "JOIN information_schema.REFERENTIAL_CONSTRAINTS r "
        "ON r.CONSTRAINT_SCHEMA=k.CONSTRAINT_SCHEMA "
        "AND r.CONSTRAINT_NAME=k.CONSTRAINT_NAME "
        "AND r.TABLE_NAME=k.TABLE_NAME "
        "WHERE k.TABLE_SCHEMA=DATABASE() AND k.TABLE_NAME IN (" + names_sql + ") "
        "AND k.REFERENCED_TABLE_NAME IS NOT NULL "
        "ORDER BY BINARY k.TABLE_NAME, BINARY k.CONSTRAINT_NAME, k.ORDINAL_POSITION"
    )).mappings().all()
    check_rows = connection.execute(text(
        "SELECT tc.TABLE_NAME AS table_name, tc.CONSTRAINT_NAME AS constraint_name, "
        "cc.CHECK_CLAUSE AS check_clause FROM information_schema.TABLE_CONSTRAINTS tc "
        "JOIN information_schema.CHECK_CONSTRAINTS cc "
        "ON cc.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA "
        "AND cc.CONSTRAINT_NAME=tc.CONSTRAINT_NAME "
        "WHERE tc.TABLE_SCHEMA=DATABASE() AND tc.TABLE_NAME IN (" + names_sql + ") "
        "AND tc.CONSTRAINT_TYPE='CHECK' "
        "ORDER BY BINARY tc.TABLE_NAME, BINARY tc.CONSTRAINT_NAME"
    )).mappings().all()
    snapshot: dict[str, Any] = {
        "schema": "probiga.qmt-reference-table-physical.v1",
        "tables": {},
    }
    for name in _REFERENCE_TRUTH_TABLES:
        snapshot["tables"][name] = {
            "engine": "",
            "table_collation": "",
            "columns": [],
            "indexes": {},
            "foreign_keys": {},
            "checks": {},
        }
    for row in table_rows:
        name = str(row.get("table_name") or "")
        if name in snapshot["tables"]:
            snapshot["tables"][name]["engine"] = str(
                row.get("engine") or ""
            ).lower()
            snapshot["tables"][name]["table_collation"] = str(
                row.get("table_collation") or ""
            ).lower()
    for row in column_rows:
        name = str(row.get("table_name") or "")
        if name not in snapshot["tables"]:
            continue
        snapshot["tables"][name]["columns"].append({
            "name": str(row.get("column_name") or ""),
            "data_type": str(row.get("data_type") or "").lower(),
            "character_maximum_length": (
                int(row["character_maximum_length"])
                if row.get("character_maximum_length") is not None else None
            ),
            "numeric_precision": (
                int(row["numeric_precision"])
                if row.get("numeric_precision") is not None else None
            ),
            "numeric_scale": (
                int(row["numeric_scale"])
                if row.get("numeric_scale") is not None else None
            ),
            "is_nullable": str(row.get("is_nullable") or "").upper(),
            "column_default": _normalized_schema_default(
                row.get("column_default")
            ),
            "extra": str(row.get("extra") or "").lower(),
            "character_set_name": str(
                row.get("character_set_name") or ""
            ).lower() or None,
            "collation_name": str(
                row.get("collation_name") or ""
            ).lower() or None,
        })
    index_parts: dict[tuple[str, str], dict[str, Any]] = {}
    for row in index_rows:
        key = (str(row.get("table_name") or ""), str(row.get("index_name") or ""))
        if key[0] not in snapshot["tables"]:
            continue
        part = index_parts.setdefault(key, {
            "non_unique": int(row.get("non_unique") or 0),
            "columns": [], "sub_parts": [], "index_type": "",
        })
        part["columns"].append(str(row.get("column_name") or ""))
        part["sub_parts"].append(
            int(row["sub_part"]) if row.get("sub_part") is not None else None
        )
        part["index_type"] = str(row.get("index_type") or "").upper()
    for (table_name, index_name), value in index_parts.items():
        snapshot["tables"][table_name]["indexes"][index_name] = value
    foreign_parts: dict[tuple[str, str], dict[str, Any]] = {}
    for row in foreign_rows:
        key = (
            str(row.get("table_name") or ""),
            str(row.get("constraint_name") or ""),
        )
        if key[0] not in snapshot["tables"]:
            continue
        part = foreign_parts.setdefault(key, {
            "columns": [],
            "referenced_table": str(row.get("referenced_table_name") or ""),
            "referenced_columns": [],
            "update_rule": normalize_mysql_referential_rule(
                row.get("update_rule")
            ),
            "delete_rule": normalize_mysql_referential_rule(
                row.get("delete_rule")
            ),
        })
        part["columns"].append(str(row.get("column_name") or ""))
        part["referenced_columns"].append(
            str(row.get("referenced_column_name") or "")
        )
    for (table_name, constraint_name), value in foreign_parts.items():
        snapshot["tables"][table_name]["foreign_keys"][constraint_name] = value
    for row in check_rows:
        name = str(row.get("table_name") or "")
        if name in snapshot["tables"]:
            snapshot["tables"][name]["checks"][
                str(row.get("constraint_name") or "")
            ] = " ".join(str(row.get("check_clause") or "").split())
    return snapshot


def _validate_reference_table_snapshot(snapshot: Mapping[str, Any]) -> None:
    tables = snapshot.get("tables") if isinstance(snapshot, Mapping) else None
    errors: list[str] = []
    if not isinstance(tables, Mapping) or set(tables) != set(_REFERENCE_TRUTH_TABLES):
        raise RuntimeError("QMT reference physical table inventory differs")
    character_types = {"char", "varchar", "text", "mediumtext"}
    for table_name in _REFERENCE_TRUTH_TABLES:
        table = tables[table_name]
        if (
            str(table.get("engine") or "").lower() != "innodb"
            or str(table.get("table_collation") or "").lower()
            != "utf8mb4_unicode_ci"
        ):
            errors.append(f"{table_name} engine/collation differs")
        observed_columns = []
        for column in table.get("columns") or []:
            data_type = str(column.get("data_type") or "").lower()
            if data_type in character_types:
                if (
                    column.get("character_set_name") != "utf8mb4"
                    or column.get("collation_name") != "utf8mb4_unicode_ci"
                ):
                    errors.append(
                        f"{table_name}.{column.get('name')} collation differs"
                    )
            elif (
                column.get("character_set_name") is not None
                or column.get("collation_name") is not None
            ):
                errors.append(
                    f"{table_name}.{column.get('name')} character metadata differs"
                )
            observed_columns.append((
                column.get("name"), data_type,
                column.get("character_maximum_length"),
                column.get("numeric_precision"), column.get("numeric_scale"),
                column.get("is_nullable"), column.get("column_default"),
                column.get("extra"),
            ))
        if tuple(observed_columns) != _REFERENCE_COLUMN_CONTRACTS[table_name]:
            errors.append(f"{table_name} column contract differs")
        observed_indexes = {
            name: (
                int(value.get("non_unique") or 0),
                tuple(value.get("columns") or ()),
            )
            for name, value in (table.get("indexes") or {}).items()
            if (
                all(part is None for part in value.get("sub_parts") or ())
                and str(value.get("index_type") or "").upper() == "BTREE"
            )
        }
        if observed_indexes != _REFERENCE_INDEX_CONTRACTS[table_name]:
            errors.append(f"{table_name} index contract differs")
        expected_fks = _REFERENCE_FOREIGN_KEY_CONTRACTS.get(table_name, {})
        observed_fks = {
            name: (
                tuple(value.get("columns") or ()),
                value.get("referenced_table"),
                tuple(value.get("referenced_columns") or ()),
                value.get("update_rule"), value.get("delete_rule"),
            )
            for name, value in (table.get("foreign_keys") or {}).items()
        }
        if observed_fks != expected_fks:
            errors.append(f"{table_name} foreign-key contract differs")
        if table.get("checks"):
            errors.append(f"{table_name} unexpected check constraint")
    if errors:
        raise RuntimeError("QMT reference physical schema differs: " + "; ".join(errors))


def _reference_trigger_physical_snapshot(connection: Any) -> dict[str, Any]:
    names_sql = ", ".join(f"'{name}'" for name in _REFERENCE_TRIGGER_CONTRACTS)
    rows = connection.execute(text(
        "SELECT TRIGGER_NAME AS trigger_name, EVENT_OBJECT_TABLE AS event_object_table, "
        "EVENT_MANIPULATION AS event_manipulation, ACTION_TIMING AS action_timing, "
        "ACTION_ORIENTATION AS action_orientation, ACTION_STATEMENT AS action_statement, "
        "SQL_MODE AS sql_mode, DEFINER AS definer, "
        "CHARACTER_SET_CLIENT AS character_set_client, "
        "COLLATION_CONNECTION AS collation_connection, "
        "DATABASE_COLLATION AS database_collation "
        "FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=DATABASE() "
        "AND TRIGGER_NAME IN (" + names_sql + ") ORDER BY BINARY TRIGGER_NAME"
    )).mappings().all()
    return {
        "schema": "probiga.qmt-reference-trigger-physical.v1",
        "triggers": [
            {
                key: str(row.get(key) or "")
                for key in (
                    "trigger_name", "event_object_table", "event_manipulation",
                    "action_timing", "action_orientation", "action_statement",
                    "sql_mode", "definer", "character_set_client",
                    "collation_connection", "database_collation",
                )
            }
            for row in rows
        ],
    }


def _normalize_trigger_action(value: Any) -> str:
    return "".join(str(value or "").upper().split()).replace(
        "SQLSTATEVALUE", "SQLSTATE"
    )


def _validate_reference_trigger_snapshot(snapshot: Mapping[str, Any]) -> None:
    rows = snapshot.get("triggers") if isinstance(snapshot, Mapping) else None
    if not isinstance(rows, list):
        raise RuntimeError("QMT reference trigger snapshot is invalid")
    observed = {str(row.get("trigger_name") or ""): row for row in rows}
    if set(observed) != set(_REFERENCE_TRIGGER_CONTRACTS):
        raise RuntimeError("QMT reference trigger inventory differs")
    for name, (table_name, event, message) in _REFERENCE_TRIGGER_CONTRACTS.items():
        row = observed[name]
        expected_action = _normalize_trigger_action(
            "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='" + message + "'"
        )
        if (
            str(row.get("event_object_table") or "").lower() != table_name
            or str(row.get("event_manipulation") or "").upper() != event
            or str(row.get("action_timing") or "").upper() != "BEFORE"
            or str(row.get("action_orientation") or "").upper() != "ROW"
            or _normalize_trigger_action(row.get("action_statement"))
            != expected_action
        ):
            raise RuntimeError(f"QMT reference trigger differs: {name}")


def _load_historical_instrument_archive(
    archive_path: str,
) -> tuple[str, list[dict[str, Any]]]:
    if not str(archive_path or "").strip():
        return "", []
    path = Path(archive_path).expanduser().resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected_keys = {
        "schema",
        "source_export_id",
        "history_complete_from",
        "members",
        "payload_hash",
    }
    if type(payload) is not dict or set(payload) != expected_keys:
        raise RuntimeError("QMT historical instrument archive fields differ")
    unsigned = {key: payload[key] for key in expected_keys - {"payload_hash"}}
    payload_hash = str(payload.get("payload_hash") or "")
    if (
        payload.get("schema") != HISTORICAL_INSTRUMENT_ARCHIVE_SCHEMA
        or not str(payload.get("source_export_id") or "").strip()
        or canonical_digest(unsigned) != payload_hash
    ):
        raise RuntimeError("QMT historical instrument archive root differs")
    history_complete_from = str(payload.get("history_complete_from") or "")
    raw_members = payload.get("members")
    member_keys = {
        "qmt_code", "stock_code", "list_date", "expire_date",
        "instrument_type",
    }
    if (
        type(raw_members) is not list
        or any(type(member) is not dict or set(member) != member_keys
               for member in raw_members)
    ):
        raise RuntimeError("QMT historical instrument archive member differs")
    members = canonical_catalog_members([
        {
            **dict(member),
            "instrument_batch_id": payload_hash,
        }
        for member in raw_members
    ])
    try:
        if date.fromisoformat(history_complete_from).isoformat() != history_complete_from:
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            "QMT historical instrument archive boundary is invalid"
        ) from exc
    return history_complete_from, members


def _quote(identifier: str) -> str:
    value = str(identifier or "").strip()
    if not value.replace("_", "").isalnum():
        raise ValueError(f"unsafe identifier: {identifier!r}")
    return f"`{value}`"


def _table_columns(engine: Engine, table_name: str) -> set[str]:
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        return set()
    return {str(column["name"]) for column in inspector.get_columns(table_name)}


def _privileged_migrate_column(
    engine: Engine,
    table_name: str,
    column_name: str,
    ddl: str,
) -> None:
    if column_name in _table_columns(engine, table_name):
        return
    with engine.begin() as conn:
        conn.execute(text(f"ALTER TABLE {_quote(table_name)} ADD COLUMN {_quote(column_name)} {ddl}"))


def privileged_migrate_reference_schema(
    engine: Engine,
    *,
    install_triggers: bool = True,
    trigger_ddl_executor: Any | None = None,
    attest_schema: bool = True,
) -> dict[str, Any]:
    """Install persistent QMT reference storage in a fenced release window.

    Production uses ``install_triggers=False, attest_schema=False`` here, then
    installs the exact trigger contract with its allow-listed broker and calls
    :func:`attest_prepared_reference_schema`.  The standalone schema-only CLI
    uses the defaults and performs the complete privileged migration.
    """

    if type(install_triggers) is not bool:
        raise TypeError("install_triggers must be bool")
    if type(attest_schema) is not bool:
        raise TypeError("attest_schema must be bool")
    if trigger_ddl_executor is not None and not callable(trigger_ddl_executor):
        raise TypeError("trigger_ddl_executor must be callable")
    if attest_schema and not install_triggers:
        raise ValueError("attest_schema requires install_triggers=True")

    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS qmt_sector_list (
            sector_name VARCHAR(191) NOT NULL PRIMARY KEY,
            source VARCHAR(32) NOT NULL DEFAULT 'qmt',
            etl_sync_at DATETIME NULL,
            data_source VARCHAR(32) NULL,
            received_at DATETIME NULL,
            batch_id VARCHAR(64) NULL,
            quality_status VARCHAR(32) NULL,
            permission_status VARCHAR(32) NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS qmt_sector_member (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            sector_name VARCHAR(191) NOT NULL,
            stock_code VARCHAR(64) NOT NULL,
            qmt_code VARCHAR(64) NOT NULL DEFAULT '',
            exchange VARCHAR(16) NOT NULL DEFAULT '',
            etl_sync_at DATETIME NULL,
            data_source VARCHAR(32) NULL,
            received_at DATETIME NULL,
            batch_id VARCHAR(64) NULL,
            quality_status VARCHAR(32) NULL,
            permission_status VARCHAR(32) NULL,
            UNIQUE KEY uk_qmt_sector_member (sector_name, stock_code),
            KEY idx_qmt_sector_member_stock (stock_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS qmt_instrument_detail (
            qmt_code VARCHAR(64) NOT NULL PRIMARY KEY,
            stock_code VARCHAR(64) NOT NULL,
            exchange VARCHAR(16) NULL,
            short_name VARCHAR(128) NULL,
            list_date DATE NULL,
            expire_date DATE NULL,
            pre_close DECIMAL(20, 6) NULL,
            up_stop_price DECIMAL(20, 6) NULL,
            down_stop_price DECIMAL(20, 6) NULL,
            float_volume DECIMAL(24, 4) NULL,
            total_volume DECIMAL(24, 4) NULL,
            instrument_type VARCHAR(32) NULL,
            etl_sync_at DATETIME NULL,
            data_source VARCHAR(32) NULL,
            received_at DATETIME NULL,
            batch_id VARCHAR(64) NULL,
            quality_status VARCHAR(32) NULL,
            permission_status VARCHAR(32) NULL,
            KEY idx_qmt_instrument_stock (stock_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS qmt_index_weight (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            index_qmt_code VARCHAR(64) NOT NULL,
            index_code VARCHAR(32) NOT NULL,
            qmt_code VARCHAR(64) NOT NULL,
            stock_code VARCHAR(64) NOT NULL,
            exchange VARCHAR(16) NULL,
            weight DECIMAL(20, 8) NULL,
            etl_sync_at DATETIME NULL,
            data_source VARCHAR(32) NULL,
            received_at DATETIME NULL,
            batch_id VARCHAR(64) NULL,
            quality_status VARCHAR(32) NULL,
            permission_status VARCHAR(32) NULL,
            UNIQUE KEY uk_qmt_index_weight (index_code, stock_code),
            KEY idx_qmt_index_weight_stock (stock_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
    ]
    with engine.begin() as conn:
        for ddl in ddl_statements:
            conn.execute(text(ddl))

    for table_name, column_name, ddl in [
        ("qmt_sector_member", "stock_code", "VARCHAR(64) NOT NULL"),
        ("qmt_sector_member", "qmt_code", "VARCHAR(64) NOT NULL DEFAULT ''"),
        ("qmt_instrument_detail", "qmt_code", "VARCHAR(64) NOT NULL"),
        ("qmt_instrument_detail", "stock_code", "VARCHAR(64) NOT NULL"),
        ("qmt_index_weight", "index_qmt_code", "VARCHAR(64) NOT NULL"),
        ("qmt_index_weight", "qmt_code", "VARCHAR(64) NOT NULL"),
        ("qmt_index_weight", "stock_code", "VARCHAR(64) NOT NULL"),
    ]:
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {_quote(table_name)} MODIFY COLUMN {_quote(column_name)} {ddl}"))

    if _table_columns(engine, "si_index_constituent"):
        _privileged_migrate_column(
            engine, "si_index_constituent", "index_qmt_code", "VARCHAR(32) NULL"
        )
        _privileged_migrate_column(
            engine, "si_index_constituent", "exchange", "VARCHAR(16) NULL"
        )
        _privileged_migrate_column(
            engine, "si_index_constituent", "weight", "DECIMAL(20, 8) NULL"
        )
    catalog_result = privileged_migrate_stock_catalog_schema(
        engine,
        install_triggers=False,
    )
    calendar_result = privileged_migrate_trade_calendar_schema(
        engine,
        install_triggers=False,
    )
    with engine.begin() as connection:
        execute_reference_ddl_contracts(connection, (
            *_REFERENCE_SEAL_TABLE_DDL,
            *_REFERENCE_SEAL_MIGRATION_DDL,
        ))
    if install_triggers:
        if trigger_ddl_executor is None:
            with engine.begin() as connection:
                for statement in reference_trigger_ddl_contracts():
                    connection.execute(text(statement))
        else:
            for statement in reference_trigger_ddl_contracts():
                trigger_ddl_executor(statement)
    seal = attest_prepared_reference_schema(engine) if attest_schema else None
    return {
        "status": "MIGRATED",
        "privileged_migration": True,
        "runtime_ddl_required": False,
        "working_table_count": len(ddl_statements),
        "truth_tables": tuple(REFERENCE_TABLE_NAMES),
        "trigger_names": tuple(REFERENCE_TRIGGER_NAMES),
        "triggers_installed": install_triggers,
        "schema_attested": attest_schema,
        "catalog": catalog_result,
        "calendar": calendar_result,
        "seal": seal,
    }


def prepare_reference_tables(engine: Engine) -> dict[str, Any]:
    """Backward-compatible, read-only runtime validation alias."""

    validate_reference_tables(engine)
    return {
        "status": "READY",
        "read_only": True,
        "runtime_ddl_required": False,
        "table_names": tuple(REFERENCE_TABLE_NAMES),
    }


def attest_prepared_reference_schema(engine: Engine) -> dict[str, str]:
    """Seal an exact schema after privileged table/trigger migration.

    Production calls this only after the trigger broker has installed all ten
    append-only guards.  Scheduled capture never calls this function.
    """

    with engine.begin() as connection:
        table_snapshot = _reference_table_physical_snapshot(connection)
        _validate_reference_table_snapshot(table_snapshot)
        trigger_snapshot = _reference_trigger_physical_snapshot(connection)
        _validate_reference_trigger_snapshot(trigger_snapshot)
        table_json = json.dumps(
            table_snapshot, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"),
        )
        trigger_json = json.dumps(
            trigger_snapshot, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"),
        )
        table_hash = canonical_digest(table_snapshot)
        trigger_hash = canonical_digest(trigger_snapshot)
        connection.execute(text("""
            INSERT INTO qmt_reference_schema_contract
            (contract_key, contract_hash, table_contract_json,
             table_contract_hash, trigger_contract_json,
             trigger_contract_hash, created_at)
            SELECT :contract_key, :contract_hash, :table_contract_json,
                   :table_contract_hash, :trigger_contract_json,
                   :trigger_contract_hash, NOW()
            WHERE NOT EXISTS (
                SELECT 1 FROM qmt_reference_schema_contract
                WHERE contract_key=:contract_key
            )
        """), {
            "contract_key": REFERENCE_SCHEMA_CONTRACT_KEY,
            "contract_hash": REFERENCE_SCHEMA_CONTRACT_HASH,
            "table_contract_json": table_json,
            "table_contract_hash": table_hash,
            "trigger_contract_json": trigger_json,
            "trigger_contract_hash": trigger_hash,
        })
    validate_reference_tables(engine, verify_triggers=True)
    return {
        "contract_key": REFERENCE_SCHEMA_CONTRACT_KEY,
        "contract_hash": REFERENCE_SCHEMA_CONTRACT_HASH,
        "table_contract_hash": table_hash,
        "trigger_contract_hash": trigger_hash,
    }


def validate_reference_tables(
    engine: Engine,
    *,
    verify_triggers: bool = False,
) -> None:
    required = {
        "qmt_sector_list": {"sector_name"},
        "qmt_sector_member": {"sector_name", "stock_code", "qmt_code"},
        "qmt_instrument_detail": {
            "qmt_code", "stock_code", "list_date", "expire_date", "batch_id",
        },
        "qmt_index_weight": {
            "index_qmt_code", "index_code", "qmt_code", "stock_code",
        },
        "qmt_reference_schema_contract": {
            "contract_key", "contract_hash", "table_contract_json",
            "table_contract_hash", "trigger_contract_json",
            "trigger_contract_hash", "created_at",
        },
    }
    missing = {
        table_name: sorted(columns - _table_columns(engine, table_name))
        for table_name, columns in required.items()
        if columns - _table_columns(engine, table_name)
    }
    if missing:
        raise RuntimeError(f"QMT reference schema is not prepared: {missing}")
    with engine.connect() as connection:
        seal_rows = connection.execute(text("""
            SELECT contract_hash, table_contract_json, table_contract_hash,
                   trigger_contract_json, trigger_contract_hash
            FROM qmt_reference_schema_contract
            WHERE contract_key=:contract_key
        """), {
            "contract_key": REFERENCE_SCHEMA_CONTRACT_KEY,
        }).mappings().all()
        if len(seal_rows) != 1:
            raise RuntimeError("QMT reference schema contract seal differs")
        seal = dict(seal_rows[0])
        try:
            sealed_tables = json.loads(str(seal.get("table_contract_json") or ""))
            sealed_triggers = json.loads(str(seal.get("trigger_contract_json") or ""))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError("QMT reference physical contract seal is invalid") from exc
        if (
            str(seal.get("contract_hash") or "")
            != REFERENCE_SCHEMA_CONTRACT_HASH
            or str(seal.get("table_contract_hash") or "")
            != canonical_digest(sealed_tables)
            or str(seal.get("trigger_contract_hash") or "")
            != canonical_digest(sealed_triggers)
        ):
            raise RuntimeError("QMT reference schema contract seal differs")
        _validate_reference_table_snapshot(sealed_tables)
        _validate_reference_trigger_snapshot(sealed_triggers)
        live_tables = _reference_table_physical_snapshot(connection)
        _validate_reference_table_snapshot(live_tables)
        if live_tables != sealed_tables:
            raise RuntimeError("QMT reference live physical schema differs from seal")
        if verify_triggers:
            live_triggers = _reference_trigger_physical_snapshot(connection)
            _validate_reference_trigger_snapshot(live_triggers)
            if live_triggers != sealed_triggers:
                raise RuntimeError("QMT reference live triggers differ from seal")


def _fmt_date(value: Any) -> str | None:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(digits) != 8:
        return None
    formatted = f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    try:
        datetime.strptime(formatted, "%Y-%m-%d")
    except ValueError:
        return None
    return formatted


def _stamp(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    out = df.copy()
    now = datetime.now().replace(microsecond=0)
    out["etl_sync_at"] = now
    out["data_source"] = PROVIDER_ID
    out["received_at"] = now
    out["batch_id"] = batch_id
    out["quality_status"] = "PENDING"
    out["permission_status"] = "SUPPORTED"
    return out


def _records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    return df.astype(object).where(pd.notna(df), None).to_dict(orient="records")


def _chunks(items: Sequence[Mapping[str, Any]], size: int) -> Iterable[list[Mapping[str, Any]]]:
    chunk_size = max(1, int(size))
    for offset in range(0, len(items), chunk_size):
        yield list(items[offset : offset + chunk_size])


def _safe_upsert_frame(
    engine: Engine,
    *,
    table_name: str,
    frame: pd.DataFrame,
    key_columns: Sequence[str],
    batch_id: str,
    chunk_size: int = 50000,
) -> dict[str, Any]:
    rows = _records(frame)
    if not rows:
        return {"status": "EMPTY", "source_rows": 0, "accepted_rows": 0, "duplicate_rows": 0}
    total_source = 0
    total_accepted = 0
    total_duplicates = 0
    statuses: set[str] = set()
    for chunk in _chunks(rows, chunk_size):
        result = safe_upsert_rows(
            engine,
            table_name=table_name,
            rows=chunk,
            key_columns=key_columns,
            batch_id=batch_id,
            permission_status="SUPPORTED",
            quality_status="PENDING",
        )
        total_source += result.source_rows
        total_accepted += result.accepted_rows
        total_duplicates += result.duplicate_rows
        statuses.add(result.status)
    return {
        "status": "UPSERTED" if "UPSERTED" in statuses else sorted(statuses)[0],
        "source_rows": total_source,
        "accepted_rows": total_accepted,
        "duplicate_rows": total_duplicates,
    }


def _read_stock_qmt_codes(engine: Engine) -> list[str]:
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT stock_code FROM si_all_code WHERE stock_code REGEXP '^(0|3|4|6|8|9)' ORDER BY stock_code")
        ).fetchall()
    result: list[str] = []
    seen: set[str] = set()
    for row in rows:
        symbol = to_qmt_symbol(str(row[0] or ""))
        if symbol and symbol not in seen:
            seen.add(symbol)
            result.append(symbol)
    return result


def _discover_native_stock_members(*, source_bridge: Any = bridge) -> pd.DataFrame:
    """Discover the stock catalog from QMT, never from a local stock table."""

    sector_names = list(dict.fromkeys(
        [*DEFAULT_STOCK_SECTORS.values(), "沪深A股"]
    ))
    frame = source_bridge.sector_members_many(sector_names, timeout=900)
    if frame is None or frame.empty:
        raise RuntimeError("QMT native A-share sector membership is empty")
    required_columns = {"sector_name", "qmt_code", "stock_code"}
    if not required_columns.issubset(frame.columns):
        raise RuntimeError("QMT native A-share membership fields are incomplete")
    normalized = frame.copy()
    normalized["sector_name"] = normalized["sector_name"].astype(str).str.strip()
    normalized["qmt_code"] = normalized["qmt_code"].astype(str).str.strip().str.upper()
    normalized["stock_code"] = normalized["stock_code"].astype(str).str.strip().str.zfill(6)
    missing_sectors = [
        sector
        for sector in DEFAULT_STOCK_SECTORS.values()
        if normalized.loc[normalized["sector_name"] == sector].empty
    ]
    if missing_sectors:
        raise RuntimeError(
            "QMT native A-share sector membership is incomplete: "
            + ",".join(missing_sectors)
        )
    result: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in normalized.itertuples(index=False):
        qmt_code = str(row.qmt_code)
        stock_code = str(row.stock_code)
        if (
            len(stock_code) != 6
            or not stock_code.isdigit()
            or stock_code[0] not in "034689"
            or qmt_code not in {
                f"{stock_code}.SH",
                f"{stock_code}.SZ",
                f"{stock_code}.BJ",
            }
        ):
            raise RuntimeError(f"QMT native A-share member is invalid: {qmt_code}")
        key = (str(row.sector_name), qmt_code)
        if key not in seen:
            seen.add(key)
            result.append({
                "sector_name": str(row.sector_name),
                "qmt_code": qmt_code,
                "stock_code": stock_code,
            })
    if not result:
        raise RuntimeError("QMT native A-share sector membership has no valid codes")
    return pd.DataFrame(result).sort_values(
        ["sector_name", "qmt_code"]
    ).reset_index(drop=True)


def _discover_native_stock_qmt_codes(*, source_bridge: Any = bridge) -> list[str]:
    members = _discover_native_stock_members(source_bridge=source_bridge)
    return sorted(set(members["qmt_code"].astype(str)))


def _read_index_qmt_codes(engine: Engine) -> list[str]:
    symbols = list(CORE_INDEXES)
    queries = [
        "SELECT index_code FROM si_all_index_code",
        "SELECT DISTINCT index_code FROM si_index_constituent",
    ]
    with engine.begin() as conn:
        for sql in queries:
            try:
                rows = conn.execute(text(sql)).fetchall()
            except Exception:
                continue
            for row in rows:
                symbol = to_qmt_index_symbol(str(row[0] or ""))
                if symbol:
                    symbols.append(symbol)
    result: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        value = str(symbol or "").strip().upper()
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _fetch_instrument_details(qmt_codes: Sequence[str], *, iscomplete: bool, batch_size: int, timeout: int, source_bridge: Any = bridge) -> pd.DataFrame:
    if not qmt_codes:
        return pd.DataFrame()
    df = source_bridge.instrument_details(qmt_codes, iscomplete=iscomplete, batch_size=batch_size, timeout=timeout)
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out["qmt_code"] = out["qmt_code"].astype(str).str.upper()
    out["stock_code"] = out["stock_code"].astype(str).str.zfill(6)
    if "product_type" not in out.columns:
        out["product_type"] = None
    if "list_date" not in out.columns:
        out["list_date"] = None
    if "expire_date" not in out.columns:
        out["expire_date"] = None
    out["list_date"] = out["list_date"].map(_fmt_date)
    out["expire_date"] = out["expire_date"].map(_fmt_date)
    return out.drop_duplicates(subset=["qmt_code"], keep="first").reset_index(drop=True)


def _instrument_detail_source_batch_id(details: pd.DataFrame) -> str:
    required = {
        "qmt_code", "stock_code", "exchange", "product_type",
        "list_date", "expire_date",
    }
    if details is None or details.empty or not required.issubset(details.columns):
        raise RuntimeError("QMT instrument-detail source payload is incomplete")
    rows = [
        {
            "qmt_code": str(row.get("qmt_code") or "").strip().upper(),
            "stock_code": str(row.get("stock_code") or "").strip().zfill(6),
            "exchange": str(row.get("exchange") or "").strip().upper(),
            "product_type": str(row.get("product_type") or "").strip(),
            "list_date": str(row.get("list_date") or "")[:10],
            "expire_date": (
                str(row.get("expire_date"))[:10]
                if row.get("expire_date") not in (None, "", "NaT") else None
            ),
        }
        for row in _records(details)
    ]
    rows.sort(key=lambda item: item["qmt_code"])
    if (
        any(not row["qmt_code"] for row in rows)
        or len({row["qmt_code"] for row in rows}) != len(rows)
    ):
        raise RuntimeError("QMT instrument-detail source identity differs")
    return canonical_digest({
        "schema": "probiga.qmt-instrument-detail-source.v1",
        "provider": "GUOJIN_QMT",
        "method": "get_instrument_detail_list/get_instrument_detail",
        "rows": rows,
    })


def _business_stock_info_rows(details: pd.DataFrame) -> pd.DataFrame:
    if details.empty:
        return pd.DataFrame(columns=["stock_code", "short_name", "exchange", "list_date", "qmt_code"])
    out = details[["stock_code", "short_name", "exchange", "list_date", "qmt_code"]].copy()
    out["short_name"] = out["short_name"].fillna("").astype(str).str.strip().str[:128]
    return out[out["short_name"] != ""].drop_duplicates(subset=["stock_code"], keep="first")


def _business_index_rows(details: pd.DataFrame) -> pd.DataFrame:
    if details.empty:
        return pd.DataFrame(columns=["index_code", "concept_code", "name", "source"])
    out = pd.DataFrame()
    out["index_code"] = details["stock_code"].astype(str).str.zfill(6)
    out["concept_code"] = ""
    out["name"] = details["short_name"].fillna("").astype(str).str.strip()
    out["source"] = "qmt"
    return out[(out["index_code"] != "") & (out["name"] != "")].drop_duplicates(subset=["index_code"], keep="first")


def _fetch_trading_calendar(
    start_year: int,
    end_year: int,
    *,
    expected_build_sha: str,
    as_of_date: date | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Capture native calendar partitions through the loaded BigQMT model."""

    release_proof = validate_strategy_release_payload(
        bigqmt_bridge.capabilities(timeout=180),
        expected_build_sha=expected_build_sha,
        root=ROOT,
        source_path=BIGQMT_STRATEGY_SOURCE,
    )
    observed_as_of = as_of_date or date.today()
    if start_year > observed_as_of.year:
        raise RuntimeError("BigQMT requested calendar range is entirely future")
    frames: list[pd.DataFrame] = []
    partitions: list[dict[str, Any]] = []
    capture_end_year = min(end_year, observed_as_of.year)
    for year in range(start_year, capture_end_year + 1):
        requested_start = f"{year:04d}-01-01"
        requested_end = (
            observed_as_of.isoformat()
            if year == observed_as_of.year
            else f"{year:04d}-12-31"
        )
        try:
            capture = bigqmt_bridge.trading_calendar_capture(
                "SH",
                start_date=requested_start,
                end_date=requested_end,
                timeout=120,
            )
        except Exception as exc:
            # 000001.SH has no authoritative ContextInfo sessions before its
            # own available history. Empty leading years are outside the
            # proven range, not missing partitions inside that range.
            if not frames and "returned no sessions" in str(exc):
                partitions.append({
                    "calendar_year": year,
                    "requested_start_date": requested_start,
                    "requested_end_date": requested_end,
                    "status": "UNPROVEN_BEFORE_SOURCE_HISTORY",
                    "session_count": 0,
                })
                continue
            raise
        for key in (
            "strategy_release_protocol",
            "strategy_identity_protocol",
            "strategy_identity_frozen",
            "strategy_identity_status",
            "strategy_build_sha",
            "strategy_git_blob",
            "strategy_source_sha256",
            "strategy_artifact_sha256",
            "strategy_loaded_identity_sha256",
        ):
            if capture.get(key) != release_proof.get(key):
                raise RuntimeError(
                    "BigQMT calendar response release identity differs"
                )
        rows = capture.get("rows")
        if (
            capture.get("status") != "ok"
            or capture.get("source") != "gj_big_qmt_inner"
            or capture.get("action") != "trading_calendar"
            or capture.get("source_method")
            != "ContextInfo.get_trading_dates"
            or capture.get("source_stock_code") != "000001.SH"
            or capture.get("requested_start_date") != requested_start
            or capture.get("requested_end_date") != requested_end
            or not isinstance(rows, list)
            or not rows
        ):
            raise RuntimeError(
                f"BigQMT native trading-calendar partition {year} is incomplete"
            )
        df = pd.DataFrame(rows)
        required = {
            "market", "calendar_year", "trade_date", "trade_status", "day_week",
        }
        if not required.issubset(df.columns):
            raise RuntimeError(
                f"BigQMT native trading-calendar partition {year} lacks columns"
            )
        normalized_dates = pd.to_datetime(
            df["trade_date"], errors="coerce"
        )
        if (
            normalized_dates.isna().any()
            or df["trade_date"].astype(str).duplicated().any()
            or set(df["market"].astype(str)) != {"SH"}
            or set(pd.to_numeric(df["calendar_year"], errors="coerce"))
            != {year}
            or set(pd.to_numeric(df["trade_status"], errors="coerce"))
            != {1}
            or not (
                pd.to_numeric(df["day_week"], errors="coerce").astype("Int64")
                == normalized_dates.dt.isocalendar().day.astype("Int64")
            ).all()
            or str(capture.get("observed_start_date") or "")
            != normalized_dates.min().date().isoformat()
            or str(capture.get("observed_end_date") or "")
            != normalized_dates.max().date().isoformat()
        ):
            raise RuntimeError(
                f"BigQMT native trading-calendar partition {year} differs"
            )
        frames.append(df)
        partitions.append({
            "calendar_year": year,
            "requested_start_date": requested_start,
            "requested_end_date": requested_end,
            "observed_start_date": capture["observed_start_date"],
            "observed_end_date": capture["observed_end_date"],
            "session_count": int(len(df)),
            "session_set_hash": canonical_digest({
                "schema": "probiga.bigqmt-calendar-partition.v1",
                "calendar_year": year,
                "sessions": sorted(df["trade_date"].astype(str).tolist()),
            }),
        })
    if not frames:
        raise RuntimeError("BigQMT native trading calendar returned no partitions")
    out = pd.concat(frames, ignore_index=True)
    out["calendar_year"] = pd.to_numeric(out["calendar_year"], errors="coerce").astype("Int64")
    out["trade_status"] = 1
    out["day_week"] = pd.to_numeric(out["day_week"], errors="coerce").astype("Int64")
    out = out[["calendar_year", "trade_date", "trade_status", "day_week"]].drop_duplicates(
        subset=["calendar_year", "trade_date"],
        keep="first",
    )
    proven_start = str(out["trade_date"].min())
    proven_end = str(out["trade_date"].max())
    requested_start = f"{start_year:04d}-01-01"
    requested_end = f"{end_year:04d}-12-31"
    unproven_after = (
        (date.fromisoformat(proven_end) + timedelta(days=1)).isoformat()
        if proven_end < requested_end
        else None
    )
    evidence = {
        "schema": "probiga.bigqmt-calendar-capture.v1",
        "provider": "gj_big_qmt_inner",
        "source_method": "ContextInfo.get_trading_dates",
        "strategy_release": release_proof,
        "requested_start_date": requested_start,
        "requested_end_date": requested_end,
        "proven_start_date": proven_start,
        "proven_end_date": proven_end,
        "unproven_after_date": unproven_after,
        "future_range_status": (
            "NOT_COVERED_NO_AUTHORITATIVE_FUTURE_CALENDAR"
            if unproven_after else "NONE"
        ),
        "partitions": partitions,
        "partition_manifest_hash": canonical_digest({
            "schema": "probiga.bigqmt-calendar-partition-manifest.v1",
            "partitions": partitions,
        }),
    }
    return out, evidence


def _build_proven_calendar_manifest(
    *,
    batch_id: str,
    captured_at: datetime,
    calendar: pd.DataFrame,
    capture_evidence: Mapping[str, Any],
) -> tuple[dict[str, Any], str]:
    """Build a receipt only for the native source's observed interval."""

    proven_start_date = str(capture_evidence.get("proven_start_date") or "")
    proven_end_date = str(capture_evidence.get("proven_end_date") or "")
    if calendar.empty or not proven_start_date or not proven_end_date:
        raise RuntimeError("BigQMT calendar proven range is unavailable")
    sessions = calendar["trade_date"].astype(str).tolist()
    if min(sessions) != proven_start_date or max(sessions) != proven_end_date:
        raise RuntimeError("BigQMT calendar sessions differ from proven range")
    source_id = calendar_source_batch_id(
        start_date=proven_start_date,
        end_date=proven_end_date,
        sessions=sessions,
    )
    manifest, _ = build_calendar_manifest(
        batch_id=batch_id,
        source_batch_id=source_id,
        known_at=captured_at,
        start_date=proven_start_date,
        end_date=proven_end_date,
        sessions=sessions,
    )
    return manifest, source_id


def _append_replace_source(engine: Engine, table_name: str, df: pd.DataFrame, *, source_column: str, source_value: str) -> int:
    if df.empty or not _table_columns(engine, table_name):
        return 0
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {_quote(table_name)} WHERE {_quote(source_column)} = :source"), {"source": source_value})
        write_frame(
            df,
            table_name,
            conn,
            if_exists="append",
            index=False,
            chunksize=2000,
            method="multi",
        )
    return int(len(df))


def _normalize_expected_index_symbols(
    index_symbols: Sequence[str],
) -> list[str]:
    expected: list[str] = []
    seen: set[str] = set()
    for item in index_symbols:
        symbol = str(item or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        expected.append(symbol)
    if not expected:
        raise RuntimeError("QMT index-weight expected index set is empty")
    return expected


def _prove_index_weight_coverage(
    index_weight: pd.DataFrame,
    *,
    expected_index_symbols: Sequence[str],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Prove one non-empty response partition for every expected index.

    ``get_index_weight`` represents an empty index map by returning no rows and
    the current worker does not distinguish that outcome from an unavailable
    partition.  Missing partitions therefore remain explicitly incomplete and
    may never authorize a full-source replacement.
    """

    expected = _normalize_expected_index_symbols(expected_index_symbols)
    frame = (
        index_weight.copy()
        if index_weight is not None
        else pd.DataFrame()
    )
    if frame.empty:
        normalized = pd.DataFrame()
        row_counts: dict[str, int] = {}
    else:
        required = {
            "index_qmt_code", "index_code", "qmt_code", "stock_code",
        }
        missing_columns = sorted(required - set(frame.columns))
        if missing_columns:
            raise RuntimeError(
                "QMT index-weight response fields are incomplete: "
                + ",".join(missing_columns)
            )
        normalized = frame.copy()
        normalized["index_qmt_code"] = (
            normalized["index_qmt_code"].astype(str).str.strip().str.upper()
        )
        normalized["index_code"] = (
            normalized["index_code"].astype(str).str.strip().str.zfill(6)
        )
        normalized["qmt_code"] = (
            normalized["qmt_code"].astype(str).str.strip().str.upper()
        )
        normalized["stock_code"] = (
            normalized["stock_code"].astype(str).str.strip().str.zfill(6)
        )
        invalid_identity = normalized[
            (normalized["index_qmt_code"] == "")
            | (normalized["index_code"] == "")
            | (normalized["qmt_code"] == "")
            | (normalized["stock_code"] == "")
        ]
        if not invalid_identity.empty:
            raise RuntimeError("QMT index-weight response contains empty identities")
        derived_index_codes = (
            normalized["index_qmt_code"].str.split(".", n=1).str[0].str.zfill(6)
        )
        if not (
            normalized["index_code"].to_numpy()
            == derived_index_codes.to_numpy()
        ).all():
            raise RuntimeError(
                "QMT index-weight response index identities are inconsistent"
            )
        outside_expected = sorted(
            set(normalized["index_qmt_code"].tolist()) - set(expected)
        )
        if outside_expected:
            raise RuntimeError(
                "QMT index-weight response contains unexpected indexes: "
                + ",".join(outside_expected)
            )
        normalized = normalized.drop_duplicates(
            subset=["index_qmt_code", "stock_code"], keep="last"
        ).reset_index(drop=True)
        row_counts = {
            str(symbol): int(count)
            for symbol, count in normalized.groupby(
                "index_qmt_code", sort=False
            ).size().items()
        }

    successful = [symbol for symbol in expected if row_counts.get(symbol, 0) > 0]
    successful_set = set(successful)
    preserved = [symbol for symbol in expected if symbol not in successful_set]
    complete = len(successful) == len(expected)
    coverage_status = (
        "COMPLETE" if complete
        else "EMPTY" if not successful
        else "PARTIAL"
    )
    per_index = [
        {
            "index_qmt_code": symbol,
            "index_code": symbol.split(".", 1)[0].zfill(6),
            "returned_rows": int(row_counts.get(symbol, 0)),
            "source_status": (
                "NON_EMPTY" if row_counts.get(symbol, 0) > 0
                else "EMPTY_OR_FAILED"
            ),
            "publication_action": (
                "REPLACE_PARTITION" if row_counts.get(symbol, 0) > 0
                else "PRESERVE_PREVIOUS_PARTITION"
            ),
        }
        for symbol in expected
    ]
    receipt = {
        "schema": INDEX_WEIGHT_PUBLICATION_RECEIPT_SCHEMA,
        "coverage_status": coverage_status,
        "coverage_complete": complete,
        "expected_index_count": len(expected),
        "successful_index_count": len(successful),
        "preserved_index_count": len(preserved),
        "coverage_ratio": len(successful) / len(expected),
        "expected_index_qmt_codes": expected,
        "successful_index_qmt_codes": successful,
        "preserved_index_qmt_codes": preserved,
        "per_index": per_index,
        "returned_rows": int(len(normalized)),
    }
    return normalized, receipt


def _index_weight_business_frame(
    index_weight: pd.DataFrame,
    *,
    detail_names: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    if index_weight is None or index_weight.empty:
        return pd.DataFrame()
    columns = [
        "index_code", "stock_code", "qmt_code", "index_qmt_code",
        "exchange", "weight", "etl_sync_at", "data_source", "received_at",
        "batch_id", "quality_status", "permission_status",
    ]
    missing = sorted(set(columns) - set(index_weight.columns))
    if missing:
        raise RuntimeError(
            "QMT index-weight publication fields are incomplete: "
            + ",".join(missing)
        )
    frame = index_weight[columns].copy()
    frame["short_name"] = frame["stock_code"].map(
        dict(detail_names or {})
    ).fillna("")
    return frame


def _provider_source_identity(columns: set[str]) -> tuple[str, str]:
    if "data_source" in columns:
        return "data_source", PROVIDER_ID
    if "source" in columns:
        return "source", "qmt"
    raise RuntimeError(
        "QMT index-weight target lacks a source identity column"
    )


def _frame_for_index_target(
    frame: pd.DataFrame,
    *,
    table_name: str,
    table_columns: set[str],
) -> pd.DataFrame:
    required = {"index_code", "stock_code"}
    if table_name == "qmt_index_weight":
        required.update({"index_qmt_code", "qmt_code"})
    missing = sorted(required - table_columns)
    if missing:
        raise RuntimeError(
            f"{table_name} lacks index-weight publication columns: {missing}"
        )
    source_column, source_value = _provider_source_identity(table_columns)
    prepared = frame.copy()
    prepared[source_column] = source_value
    insert_columns = [
        column for column in prepared.columns if column in table_columns
    ]
    return prepared[insert_columns]


def _delete_index_weight_scope(
    connection: Any,
    *,
    table_name: str,
    table_columns: set[str],
    successful_index_symbols: Sequence[str],
    coverage_complete: bool,
    expected_index_symbols: Sequence[str],
) -> None:
    source_column, source_value = _provider_source_identity(table_columns)
    params: dict[str, Any] = {"provider_source": source_value}
    predicate = f"{_quote(source_column)}=:provider_source"
    if not coverage_complete:
        if "index_qmt_code" in table_columns:
            partition_values = list(successful_index_symbols)
            partition_column = "index_qmt_code"
        else:
            expected_codes = [
                symbol.split(".", 1)[0].zfill(6)
                for symbol in expected_index_symbols
            ]
            if len(set(expected_codes)) != len(expected_codes):
                raise RuntimeError(
                    f"{table_name} cannot safely partition indexes without "
                    "index_qmt_code"
                )
            partition_values = [
                symbol.split(".", 1)[0].zfill(6)
                for symbol in successful_index_symbols
            ]
            partition_column = "index_code"
        placeholders: list[str] = []
        for index, value in enumerate(partition_values):
            key = f"published_index_{index}"
            placeholders.append(f":{key}")
            params[key] = value
        if not placeholders:
            raise RuntimeError("QMT partial index publication scope is empty")
        predicate += (
            f" AND {_quote(partition_column)} IN ({', '.join(placeholders)})"
        )
    connection.execute(
        text(f"DELETE FROM {_quote(table_name)} WHERE {predicate}"),
        params,
    )


def publish_index_weight_snapshot(
    engine: Engine,
    *,
    expected_index_symbols: Sequence[str],
    index_weight: pd.DataFrame,
    detail_names: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Publish raw/business index weights with one coverage-bound transaction.

    A complete response replaces all QMT-owned rows.  A partial response only
    replaces the proven non-empty index partitions, and an empty response does
    no DML.  Both target tables share the same transaction so an insert failure
    restores every previous partition.
    """

    normalized, receipt = _prove_index_weight_coverage(
        index_weight,
        expected_index_symbols=expected_index_symbols,
    )
    expected = list(receipt["expected_index_qmt_codes"])
    successful = list(receipt["successful_index_qmt_codes"])
    raw_columns = _table_columns(engine, "qmt_index_weight")
    business_columns = _table_columns(engine, "si_index_constituent")
    if not successful:
        receipt.update({
            "publication_status": "NO_CHANGE_EMPTY_SOURCE",
            "publication_scope": "NONE",
            "atomic": True,
            "tables": {
                "qmt_index_weight": {
                    "status": "PRESERVED_EMPTY_SOURCE",
                    "accepted_rows": 0,
                },
                "si_index_constituent": {
                    "status": (
                        "PRESERVED_EMPTY_SOURCE"
                        if business_columns else "TARGET_NOT_PRESENT"
                    ),
                    "accepted_rows": 0,
                },
            },
        })
        return receipt

    if not raw_columns:
        raise RuntimeError("qmt_index_weight target is unavailable")
    if not business_columns:
        raise RuntimeError(
            "si_index_constituent target is unavailable; refusing an "
            "unpaired index-weight publication"
        )
    raw_frame = _frame_for_index_target(
        normalized,
        table_name="qmt_index_weight",
        table_columns=raw_columns,
    )
    business_frame = _frame_for_index_target(
        _index_weight_business_frame(
            normalized,
            detail_names=detail_names,
        ),
        table_name="si_index_constituent",
        table_columns=business_columns,
    )
    complete = bool(receipt["coverage_complete"])
    try:
        with engine.begin() as connection:
            _delete_index_weight_scope(
                connection,
                table_name="qmt_index_weight",
                table_columns=raw_columns,
                successful_index_symbols=successful,
                coverage_complete=complete,
                expected_index_symbols=expected,
            )
            write_frame(
                raw_frame,
                "qmt_index_weight",
                connection,
                if_exists="append",
                index=False,
                chunksize=2000,
                method="multi",
            )
            _delete_index_weight_scope(
                connection,
                table_name="si_index_constituent",
                table_columns=business_columns,
                successful_index_symbols=successful,
                coverage_complete=complete,
                expected_index_symbols=expected,
            )
            write_frame(
                business_frame,
                "si_index_constituent",
                connection,
                if_exists="append",
                index=False,
                chunksize=2000,
                method="multi",
            )
    except Exception as exc:
        raise RuntimeError(
            "QMT index-weight publication failed; all index partitions were "
            "rolled back"
        ) from exc

    table_status = (
        "REPLACED_QMT_ROWS_COMPLETE"
        if complete else "REPLACED_SUCCESSFUL_PARTITIONS"
    )
    receipt.update({
        "publication_status": (
            "FULL_ATOMIC_REPLACE"
            if complete else "PARTIAL_ATOMIC_PARTITION_REPLACE"
        ),
        "publication_scope": "ALL_QMT_INDEXES" if complete else "SUCCESSFUL_INDEXES",
        "atomic": True,
        "tables": {
            "qmt_index_weight": {
                "status": table_status,
                "accepted_rows": int(len(raw_frame)),
            },
            "si_index_constituent": {
                "status": table_status,
                "accepted_rows": int(len(business_frame)),
            },
        },
    })
    return receipt


def sync_reference_data(
    *,
    start_year: int,
    end_year: int,
    iscomplete: bool,
    refresh_timeout: int,
    skip_refresh: bool = False,
    skip_calendar: bool = True,
    dry_run: bool = False,
    historical_instrument_archive: str = "",
    release_build_sha: str = "",
) -> dict[str, Any]:
    normalized_release_sha = str(release_build_sha or "").strip().lower()
    if not skip_calendar and not normalized_release_sha:
        normalized_release_sha = os.environ.get(
            "PROBIGA_BUILD_COMMIT_SHA", ""
        ).strip().lower()
    if not skip_calendar and not normalized_release_sha:
        raise RuntimeError(
            "BigQMT formal calendar capture requires a release build SHA"
        )
    engine = create_batch_engine(
        get_mysql_url(required=True), pool_pre_ping=True, future=True
    )
    # The scheduled runtime account is DML-only.  DDL/triggers are installed
    # once by the privileged schema migration path.
    validate_reference_tables(engine)
    captured_at = datetime.now().replace(microsecond=0)
    if normalized_release_sha:
        # The full release identity is committed into both append-only
        # manifests while keeping the shared VARCHAR(64) batch contract.
        batch_id = release_bound_reference_batch_id(
            normalized_release_sha,
            captured_at=captured_at,
        )
    else:
        batch_id = (
            "qmt_reference_"
            f"{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
        )
    archive_history_from, archive_members = (
        _load_historical_instrument_archive(historical_instrument_archive)
    )
    previous_catalog = None
    try:
        with engine.connect() as connection:
            previous_catalog = load_stock_catalog(
                connection,
                decision_known_at=captured_at,
            )
    except RuntimeError as exc:
        if "no complete independent QMT stock catalog batch" not in str(exc):
            raise

    refresh_result: dict[str, Any] = {"status": "skipped"} if skip_refresh else {}
    if not skip_refresh:
        try:
            refresh_result = bridge.refresh_reference_data(
                ["download_sector_data", "download_index_weight", "download_holiday_data"],
                timeout=refresh_timeout,
            )
        except Exception as exc:
            refresh_result = {"status": "warning", "error": str(exc)}

    sector_df = bigqmt_bridge.sector_list(timeout=180)
    sector_df = sector_df if sector_df is not None else pd.DataFrame()
    sector_df = _stamp(sector_df.drop_duplicates(subset=["sector_name"], keep="first"), batch_id) if not sector_df.empty else sector_df

    sector_members = bigqmt_bridge.sector_members_many(
        sector_df["sector_name"].astype(str).tolist() if not sector_df.empty else [],
        timeout=1800,
    )
    sector_members = sector_members if sector_members is not None else pd.DataFrame()
    if not sector_members.empty:
        sector_members = sector_members.copy()
        sector_members["stock_code"] = sector_members["stock_code"].astype(str).str.zfill(6)
        sector_members["qmt_code"] = sector_members["qmt_code"].astype(str).str.upper()
        sector_members["exchange"] = sector_members["qmt_code"].str.split(".", n=1).str[1].fillna("")
        sector_members = _stamp(
            sector_members.drop_duplicates(subset=["sector_name", "stock_code"], keep="first"),
            batch_id,
        )

    sector_datasets = fetch_sector_datasets()

    native_stock_members = _discover_native_stock_members(
        source_bridge=bigqmt_bridge
    )
    native_stock_qmt_codes = sorted(set(
        native_stock_members["qmt_code"].astype(str)
    ))
    archive_discovery_sector = "QMT历史证券归档"
    carried_members = list(archive_members)
    if previous_catalog is not None:
        carried_members.extend(dict(member) for member in previous_catalog.members)
    requested_stock_codes = sorted(
        set(native_stock_qmt_codes)
    )
    stock_details = _fetch_instrument_details(
        requested_stock_codes,
        iscomplete=iscomplete,
        batch_size=400,
        timeout=900,
        source_bridge=bigqmt_bridge,
    )
    observed_detail_codes = set(
        stock_details.get("qmt_code", pd.Series(dtype=str)).astype(str).str.upper()
    )
    if observed_detail_codes != set(requested_stock_codes):
        missing = sorted(set(requested_stock_codes) - observed_detail_codes)
        extra = sorted(observed_detail_codes - set(requested_stock_codes))
        raise RuntimeError(
            "BigQMT instrument detail does not exactly cover current "
            f"A-share discovery: missing={missing[:10]}, extra={extra[:10]}"
        )
    instrument_source_batch_id = _instrument_detail_source_batch_id(
        stock_details
    )
    catalog_by_qmt: dict[str, dict[str, Any]] = {}
    for member in carried_members:
        code = str(member["qmt_code"])
        previous = catalog_by_qmt.get(code)
        if previous is not None and (
            previous["stock_code"] != member["stock_code"]
            or previous["list_date"] != member["list_date"]
            or (
                previous.get("expire_date") not in (None, "")
                and member.get("expire_date") not in (None, "")
                and previous["expire_date"] != member["expire_date"]
            )
        ):
            raise RuntimeError("QMT historical instrument archives conflict")
        catalog_by_qmt[code] = {
            **(previous or {}),
            **member,
            "expire_date": (
                member.get("expire_date")
                if member.get("expire_date") not in (None, "")
                else (previous or {}).get("expire_date")
            ),
        }
    for detail in _records(stock_details):
        code = str(detail.get("qmt_code") or "").upper()
        previous = catalog_by_qmt.get(code, {})
        list_date = str(detail.get("list_date") or "")[:10]
        if previous and list_date and list_date != previous["list_date"]:
            raise RuntimeError("QMT instrument list_date changed across batches")
        catalog_by_qmt[code] = {
            "qmt_code": code,
            "stock_code": str(detail.get("stock_code") or "").zfill(6),
            "list_date": list_date or previous.get("list_date"),
            "expire_date": (
                str(detail.get("expire_date"))[:10]
                if detail.get("expire_date") not in (None, "", "NaT")
                else previous.get("expire_date")
            ),
            "instrument_batch_id": instrument_source_batch_id,
            "instrument_type": "STOCK",
        }
    captured_day = captured_at.date().isoformat()
    unresolved_removed = sorted(
        code for code, member in catalog_by_qmt.items()
        if code not in set(native_stock_qmt_codes)
        and (
            member.get("expire_date") in (None, "")
            or captured_day <= str(member.get("expire_date"))[:10]
        )
    )
    if unresolved_removed:
        raise RuntimeError(
            "QMT current membership omitted instruments without an independent "
            "expire_date fact: " + ",".join(unresolved_removed[:10])
        )
    catalog_members = pd.DataFrame(list(catalog_by_qmt.values()))
    archived_codes = sorted(set(catalog_by_qmt) - set(native_stock_qmt_codes))
    discovery = build_catalog_discovery(
        current_sectors=NATIVE_A_SHARE_SECTORS,
        expired_sectors=[archive_discovery_sector] if archived_codes else [],
        sector_members=[
            *_records(native_stock_members[["sector_name", "qmt_code"]]),
            *[
                {"sector_name": archive_discovery_sector, "qmt_code": code}
                for code in archived_codes
            ],
        ],
    )
    official_codes = set(catalog_by_qmt)
    official_list_dates = [
        str(catalog_by_qmt[code].get("list_date") or "")[:10]
        for code in official_codes
    ]
    if not official_list_dates or any(not value for value in official_list_dates):
        raise RuntimeError("QMT historical contract instrument dates are incomplete")
    history_candidates = [min(official_list_dates)]
    if previous_catalog is not None:
        history_candidates.append(previous_catalog.history_complete_from)
    if archive_history_from:
        history_candidates.append(archive_history_from)
    history_complete_from = min(history_candidates)
    catalog_manifest, _ = build_catalog_manifest(
        batch_id=batch_id,
        captured_at=captured_at,
        history_complete_from=history_complete_from,
        members=_records(catalog_members),
        discovery=discovery,
        native_sectors=NATIVE_A_SHARE_SECTORS,
    )
    index_details = _fetch_instrument_details(
        _read_index_qmt_codes(engine),
        iscomplete=iscomplete,
        batch_size=300,
        timeout=600,
        source_bridge=bigqmt_bridge,
    )
    all_details = pd.concat([stock_details, index_details], ignore_index=True).drop_duplicates(
        subset=["qmt_code"],
        keep="first",
    )
    if not all_details.empty:
        all_details["instrument_type"] = all_details.get(
            "product_type", pd.Series("", index=all_details.index)
        ).fillna("").astype(str)
        all_details = _stamp(all_details, batch_id)

    index_symbols = _read_index_qmt_codes(engine)
    index_weight = bigqmt_bridge.index_weight_many(index_symbols, timeout=1200)
    index_weight = index_weight if index_weight is not None else pd.DataFrame()
    if not index_weight.empty:
        index_weight = index_weight.copy()
        index_weight["index_code"] = index_weight["index_code"].astype(str).str.zfill(6)
        index_weight["stock_code"] = index_weight["stock_code"].astype(str).str.zfill(6)
        index_weight = _stamp(
            index_weight.drop_duplicates(
                subset=["index_qmt_code", "stock_code"], keep="first"
            ),
            batch_id,
        )
    index_weight, index_weight_coverage = _prove_index_weight_coverage(
        index_weight,
        expected_index_symbols=index_symbols,
    )

    calendar_error = ""
    calendar_capture_evidence: dict[str, Any] | None = None
    if skip_calendar:
        calendar = pd.DataFrame(columns=["calendar_year", "trade_date", "trade_status", "day_week"])
    else:
        try:
            calendar, calendar_capture_evidence = _fetch_trading_calendar(
                start_year,
                end_year,
                expected_build_sha=normalized_release_sha,
                as_of_date=captured_at.date(),
            )
            calendar = _stamp(calendar, batch_id)
        except Exception as exc:
            calendar_error = str(exc)
            calendar = pd.DataFrame(columns=["calendar_year", "trade_date", "trade_status", "day_week"])
    calendar_manifest: dict[str, Any] | None = None
    calendar_source_id: str | None = None
    if not skip_calendar:
        if calendar.empty or calendar_error:
            raise RuntimeError(
                "QMT trade calendar receipt is incomplete: "
                + (calendar_error or "empty source")
            )
        calendar_manifest, calendar_source_id = _build_proven_calendar_manifest(
            batch_id=batch_id,
            captured_at=datetime.now().replace(microsecond=0),
            calendar=calendar,
            capture_evidence=calendar_capture_evidence or {},
        )

    if dry_run:
        return {
            "status": "dry_run",
            "batch_id": batch_id,
            "refresh": refresh_result,
            "rows": {
                "qmt_sector_list": int(len(sector_df)),
                "qmt_sector_member": int(len(sector_members)),
                "qmt_instrument_detail": int(len(all_details)),
                "qmt_index_weight": int(len(index_weight)),
                "si_trade_calendar": int(len(calendar)),
                "si_concept_code_east": int(len(sector_datasets.get("concept_catalog", pd.DataFrame()))),
                "si_concept_constituent_east": int(len(sector_datasets.get("concept_constituents", pd.DataFrame()))),
                "si_industry_sw": int(len(sector_datasets.get("industry_sw", pd.DataFrame()))),
                "si_all_code": int(len(stock_details)),
                "si_all_index_code": int(len(index_details)),
                "qmt_stock_catalog_member": int(
                    catalog_manifest["member_count"]
                ),
            },
            "stock_catalog": catalog_manifest,
            "trade_calendar": calendar_manifest,
            "trade_calendar_capture": calendar_capture_evidence,
            "index_weight_publication": {
                **index_weight_coverage,
                "publication_status": "DRY_RUN_NOT_PUBLISHED",
                "publication_scope": "NONE",
                "atomic": True,
            },
            "calendar_error": calendar_error,
        }

    results: dict[str, Any] = {
        "refresh": refresh_result,
        "batch_id": batch_id,
        "release_build_sha": normalized_release_sha or None,
        "stock_catalog": catalog_manifest,
        "trade_calendar": calendar_manifest,
        "trade_calendar_capture": calendar_capture_evidence,
        "index_weight_publication": index_weight_coverage,
        "tables": {},
    }

    write_plan: list[tuple[str, pd.DataFrame, Sequence[str]]] = [
        ("qmt_sector_list", sector_df, ["sector_name"]),
        ("qmt_sector_member", sector_members, ["sector_name", "stock_code"]),
        ("qmt_instrument_detail", all_details, ["qmt_code"]),
        ("si_all_code", _stamp(_business_stock_info_rows(stock_details), batch_id), ["stock_code"]),
        ("si_all_index_code", _stamp(_business_index_rows(index_details), batch_id), ["index_code"]),
        ("si_concept_code_east", _stamp(sector_datasets.get("concept_catalog", pd.DataFrame()), batch_id), ["concept_code"]),
        (
            "si_concept_constituent_east",
            _stamp(sector_datasets.get("concept_constituents", pd.DataFrame()), batch_id),
            ["concept_code", "stock_code"],
        ),
    ]
    if not calendar.empty:
        write_plan.append(("si_trade_calendar", calendar, ["calendar_year", "trade_date"]))
    for table_name, frame, keys in write_plan:
        results["tables"][table_name] = _safe_upsert_frame(
            engine,
            table_name=table_name,
            key_columns=keys,
            batch_id=batch_id,
            frame=frame,
        )

    with engine.begin() as connection:
        results["tables"]["qmt_stock_catalog_batch"] = {
            "status": "INSERTED",
            **insert_catalog_batch(
                connection,
                batch_id=batch_id,
                captured_at=catalog_manifest["captured_at"],
                history_complete_from=catalog_manifest[
                    "history_complete_from"
                ],
                members=_records(catalog_members),
                discovery=catalog_manifest["discovery"],
                native_sectors=NATIVE_A_SHARE_SECTORS,
            ),
        }
        if calendar_manifest is not None:
            results["tables"]["qmt_trade_calendar_batch"] = {
                "status": "INSERTED",
                **insert_trade_calendar_receipt(
                    connection,
                    batch_id=batch_id,
                    source_batch_id=str(calendar_source_id),
                    known_at=calendar_manifest["known_at"],
                    start_date=calendar_manifest["start_date"],
                    end_date=calendar_manifest["end_date"],
                    sessions=calendar["trade_date"].tolist(),
                ),
            }

    industry_sw = sector_datasets.get("industry_sw", pd.DataFrame())
    if industry_sw is not None and not industry_sw.empty:
        industry_sw = industry_sw.copy()
        industry_sw["etl_sync_at"] = datetime.now().replace(microsecond=0)
        results["tables"]["si_industry_sw"] = {
            "status": "REPLACED_SOURCE",
            "accepted_rows": _append_replace_source(engine, "si_industry_sw", industry_sw, source_column="source", source_value="qmt"),
        }

    detail_names: dict[str, str] = {}
    if not all_details.empty:
        detail_names = (
            all_details.drop_duplicates(subset=["stock_code"], keep="first")
            .set_index("stock_code")["short_name"]
            .fillna("")
            .astype(str)
            .to_dict()
        )
    index_publication = publish_index_weight_snapshot(
        engine,
        expected_index_symbols=index_symbols,
        index_weight=index_weight,
        detail_names=detail_names,
    )
    results["index_weight_publication"] = {
        key: value
        for key, value in index_publication.items()
        if key != "tables"
    }
    results["tables"].update(index_publication["tables"])

    results["status"] = (
        "success"
        if index_publication["coverage_complete"] else "partial"
    )
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync Guojin QMT reference data into local business tables.")
    parser.add_argument("--start-year", type=int, default=1990)
    parser.add_argument("--end-year", type=int, default=date.today().year + 1)
    parser.add_argument("--iscomplete", action="store_true", help="Request complete instrument details when QMT supports it.")
    parser.add_argument("--refresh-timeout", type=int, default=900)
    parser.add_argument("--skip-refresh", action="store_true")
    parser.add_argument("--include-calendar", action="store_true", help="Also call QMT get_trading_calendar. Disabled by default.")
    parser.add_argument(
        "--historical-instrument-archive",
        default="",
        help=(
            "Content-addressed native-QMT historical instrument export. "
            "Required once to prove dates before the first catalog capture."
        ),
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--release-build-sha",
        default="",
        help=(
            "Bind the append-only catalog/calendar batch to one release SHA. "
            "Used only by the controlled Windows edge bootstrap."
        ),
    )
    parser.add_argument(
        "--prepare-schema-only",
        action="store_true",
        help="Privileged migration mode: create/verify tables and immutable triggers only.",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    if args.prepare_schema_only:
        engine = create_batch_engine(
            get_mysql_url(required=True), pool_pre_ping=True, future=True
        )
        migration = privileged_migrate_reference_schema(engine)
        result = {
            "status": "success",
            "mode": "prepare_schema_only",
            "runtime_ddl_required": False,
            "migration": migration,
        }
        print(
            json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
            if args.json else result
        )
        return 0

    result = sync_reference_data(
        start_year=args.start_year,
        end_year=args.end_year,
        iscomplete=args.iscomplete,
        refresh_timeout=max(1, args.refresh_timeout),
        skip_refresh=args.skip_refresh,
        skip_calendar=not args.include_calendar,
        dry_run=args.dry_run,
        historical_instrument_archive=args.historical_instrument_archive,
        release_build_sha=args.release_build_sha,
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    else:
        print(result)
    return 0 if result.get("status") in {"success", "dry_run"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
