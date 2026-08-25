#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only QMT collation audit with a fail-closed maintenance entrypoint.

The audit path is deliberately DDL/DML free.  ``apply`` is disabled until the
tool can independently prove a root-owned maintenance guard, real systemd
writer/process fencing, an isolated trusted artifact, pinned TLS, and the
audited database identity at every DDL boundary.  There is no caller-supplied
``writers_fenced`` assertion.
"""
from __future__ import annotations

import argparse
import configparser
import hashlib
import importlib
import json
import os
import re
import stat
import sys
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping, Protocol, Sequence

# This maintenance command is intentionally self-contained.  In particular it
# must be executable as ``python -I /opt/ProBigA-releases/<sha>/tools/...``
# without adding a checkout/worktree to sys.path before isolation is proved.
QMT_ATTESTATION_COLLATION = "utf8mb4_unicode_ci"
QMT_ATTESTATION_LEGACY_COLLATION = "utf8mb4_general_ci"
QMT_ATTESTATION_TRIGGER_DEFINER = "probiga_migrator@127.0.0.1"
QMT_ATTESTATION_TRIGGER_SQL_MODE = (
    "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,"
    "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
)
QMT_ATTESTATION_COLUMN_SPECS = {
    "qmt_kline_attestation_schema_migration": (
        ("migration_key", "varchar", 100, None, None, "NO", None, ""),
        ("migration_hash", "char", 64, None, None, "NO", None, ""),
        ("completed_at", "datetime", None, None, None, "NO", None, ""),
    ),
    "qmt_kline_attestation_run": (
        ("run_id", "varchar", 64, None, None, "NO", None, ""),
        ("provider", "varchar", 32, None, None, "NO", None, ""),
        ("start_date", "date", None, None, None, "NO", None, ""),
        ("end_date", "date", None, None, None, "NO", None, ""),
        ("status", "varchar", 40, None, None, "NO", None, ""),
        ("target_rows", "bigint", None, 19, 0, "NO", "0", ""),
        ("qmt_rows", "bigint", None, 19, 0, "NO", "0", ""),
        ("matched_rows", "bigint", None, 19, 0, "NO", "0", ""),
        ("missing_qmt_rows", "bigint", None, 19, 0, "NO", "0", ""),
        ("mismatched_rows", "bigint", None, 19, 0, "NO", "0", ""),
        ("already_attested_rows", "bigint", None, 19, 0, "NO", "0", ""),
        ("updated_rows", "bigint", None, 19, 0, "NO", "0", ""),
        ("tolerance_json", "mediumtext", 16777215, None, None, "NO", None, ""),
        ("started_at", "datetime", None, None, None, "NO", None, ""),
        ("finished_at", "datetime", None, None, None, "YES", None, ""),
        ("error_message", "text", 65535, None, None, "YES", None, ""),
    ),
    "qmt_kline_attestation_mismatch": (
        ("id", "bigint", None, 19, 0, "NO", None, "auto_increment"),
        ("run_id", "varchar", 64, None, None, "NO", None, ""),
        ("trade_date", "date", None, None, None, "NO", None, ""),
        ("stock_code", "varchar", 16, None, None, "NO", None, ""),
        ("reason", "varchar", 40, None, None, "NO", None, ""),
        ("target_close", "decimal", None, 20, 6, "YES", None, ""),
        ("qmt_close", "decimal", None, 20, 6, "YES", None, ""),
        ("target_volume", "decimal", None, 24, 6, "YES", None, ""),
        ("qmt_volume", "decimal", None, 24, 6, "YES", None, ""),
        ("target_amount", "decimal", None, 24, 6, "YES", None, ""),
        ("qmt_amount", "decimal", None, 24, 6, "YES", None, ""),
        ("created_at", "datetime", None, None, None, "NO", None, ""),
    ),
    "qmt_kline_attestation_row": (
        ("attestation_id", "char", 64, None, None, "NO", None, ""),
        ("run_id", "varchar", 64, None, None, "NO", None, ""),
        ("target_id", "bigint", None, 19, 0, "NO", None, ""),
        ("qmt_id", "bigint", None, 19, 0, "NO", None, ""),
        ("trade_date", "date", None, None, None, "NO", None, ""),
        ("stock_code", "varchar", 16, None, None, "NO", None, ""),
        ("protocol_version", "varchar", 64, None, None, "NO", None, ""),
        ("source_data_version", "varchar", 64, None, None, "NO", None, ""),
        ("source_pre_close_origin", "varchar", 32, None, None, "NO", None, ""),
        ("source_pre_close", "decimal", None, 20, 6, "NO", None, ""),
        ("attested_open", "decimal", None, 20, 6, "NO", None, ""),
        ("attested_close", "decimal", None, 20, 6, "NO", None, ""),
        ("attested_high", "decimal", None, 20, 6, "NO", None, ""),
        ("attested_low", "decimal", None, 20, 6, "NO", None, ""),
        ("attested_volume", "decimal", None, 24, 6, "NO", None, ""),
        ("attested_amount", "decimal", None, 24, 6, "NO", None, ""),
        ("created_at", "datetime", None, None, None, "NO", None, ""),
    ),
}
QMT_ATTESTATION_INDEX_SPECS = {
    "qmt_kline_attestation_schema_migration": {
        "PRIMARY": (0, ("migration_key",)),
    },
    "qmt_kline_attestation_run": {
        "PRIMARY": (0, ("run_id",)),
        "idx_qmt_kline_attestation_range": (
            1,
            ("start_date", "end_date", "status"),
        ),
    },
    "qmt_kline_attestation_mismatch": {
        "PRIMARY": (0, ("id",)),
        "uk_qmt_kline_attestation_mismatch": (
            0,
            ("run_id", "trade_date", "stock_code"),
        ),
        "idx_qmt_kline_mismatch_lookup": (1, ("trade_date", "stock_code")),
    },
    "qmt_kline_attestation_row": {
        "PRIMARY": (0, ("attestation_id",)),
        "uk_qmt_kline_attestation_row_source": (
            0,
            ("target_id", "protocol_version", "source_data_version"),
        ),
        "idx_qmt_kline_attestation_row_date": (
            1,
            ("trade_date", "protocol_version", "stock_code"),
        ),
        "idx_qmt_kline_attestation_row_run": (1, ("run_id",)),
    },
}
QMT_ATTESTATION_TRIGGER_SPECS = {
    "trg_qmt_kline_attestation_run_completed_bu": (
        "BEFORE",
        "UPDATE",
        "qmt_kline_attestation_run",
        "BEGIN IF BINARY OLD.status = BINARY 'COMPLETED' THEN "
        "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = "
        "'Completed QMT attestation run is immutable'; END IF; END",
    ),
    "trg_qmt_kline_attestation_run_completed_bd": (
        "BEFORE",
        "DELETE",
        "qmt_kline_attestation_run",
        "BEGIN IF BINARY OLD.status = BINARY 'COMPLETED' THEN "
        "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = "
        "'Completed QMT attestation run cannot be deleted'; END IF; END",
    ),
    "trg_qmt_kline_attestation_row_immutable_bu": (
        "BEFORE",
        "UPDATE",
        "qmt_kline_attestation_row",
        "BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = "
        "'QMT row attestation is append only'; END",
    ),
    "trg_qmt_kline_attestation_row_immutable_bd": (
        "BEFORE",
        "DELETE",
        "qmt_kline_attestation_row",
        "BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = "
        "'QMT row attestation cannot be deleted'; END",
    ),
    "trg_qmt_attestation_schema_migration_immutable_bu": (
        "BEFORE",
        "UPDATE",
        "qmt_kline_attestation_schema_migration",
        "BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = "
        "'QMT schema migration marker is append only'; END",
    ),
    "trg_qmt_attestation_schema_migration_immutable_bd": (
        "BEFORE",
        "DELETE",
        "qmt_kline_attestation_schema_migration",
        "BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = "
        "'QMT schema migration marker cannot be deleted'; END",
    ),
}

DATABASE_NAME = "probiga"
EXPECTED_MYSQL_VERSION = "8.4.11"
EXPECTED_SERVER_UUID = "f40c3202-9260-11f1-86ae-74d4dd7f8500"
EXPECTED_SERVER_PORT = 3306
EXPECTED_SERVER_HOSTNAME = "WIN-20260322RGF"
EXPECTED_AUDITOR_USER = "probiga_qmt_auditor@127.0.0.1"
EXPECTED_MIGRATOR_USER = "probiga_migrator@127.0.0.1"
AUDITOR_OPTION_FILE = Path("/etc/probiga/mysql-qmt-auditor.ini")
MIGRATOR_OPTION_FILE = Path("/etc/probiga/mysql-migrator.ini")
FIXED_TLS_CA_FILE = Path("/etc/probiga/mysql84-ca.pem")
FIXED_TLS_CLIENT_CERT_FILE = Path("/etc/probiga/mysql-qmt-client-cert.pem")
FIXED_TLS_CLIENT_KEY_FILE = Path("/etc/probiga/mysql-qmt-client-key.pem")
FIXED_TLS_SERVER_PIN_FILE = Path("/etc/probiga/mysql84-server-cert.sha256")
SOURCE_COLLATION = QMT_ATTESTATION_LEGACY_COLLATION
TARGET_COLLATION = QMT_ATTESTATION_COLLATION
EXPECTED_SQL_MODE = QMT_ATTESTATION_TRIGGER_SQL_MODE
PLAN_SCHEMA = "probiga.qmt-attestation-collation-plan.v2"
RESULT_SCHEMA = "probiga.qmt-attestation-collation-result.v2"

QMT_TABLES = (
    "qmt_kline_attestation_run",
    "qmt_kline_attestation_mismatch",
    "qmt_kline_attestation_row",
    "qmt_kline_attestation_schema_migration",
)
GOVERNANCE_TABLES = (
    "st_strategy_governance_schema_migration",
    "st_strategy_registry",
    "st_strategy_version",
    "st_strategy_lifecycle_event",
    "st_strategy_metric_input",
    "st_strategy_health_snapshot",
    "st_strategy_combination",
    "st_strategy_combination_version",
    "st_strategy_combination_health_snapshot",
    "st_strategy_governance_run",
    "st_strategy_pool_snapshot",
    "st_strategy_allocation_snapshot",
    "st_strategy_governance_audit",
)
ALTER_DDL = {
    table: (
        f"ALTER TABLE `{table}` CONVERT TO CHARACTER SET utf8mb4 "
        "COLLATE utf8mb4_unicode_ci"
    )
    for table in QMT_TABLES
}
ROW_PROOF_SQL = {
    "qmt_kline_attestation_run": (
        "SELECT `run_id`,`provider`,`start_date`,`end_date`,`status`,"
        "`target_rows`,`qmt_rows`,`matched_rows`,`missing_qmt_rows`,"
        "`mismatched_rows`,`already_attested_rows`,`updated_rows`,"
        "`tolerance_json`,`started_at`,`finished_at`,`error_message` "
        "FROM `qmt_kline_attestation_run` ORDER BY BINARY `run_id`"
    ),
    "qmt_kline_attestation_mismatch": (
        "SELECT `id`,`run_id`,`trade_date`,`stock_code`,`reason`,"
        "`target_close`,`qmt_close`,`target_volume`,`qmt_volume`,"
        "`target_amount`,`qmt_amount`,`created_at` "
        "FROM `qmt_kline_attestation_mismatch` ORDER BY `id`"
    ),
    "qmt_kline_attestation_row": (
        "SELECT `attestation_id`,`run_id`,`target_id`,`qmt_id`,`trade_date`,"
        "`stock_code`,`protocol_version`,`source_data_version`,"
        "`source_pre_close_origin`,`source_pre_close`,`attested_open`,"
        "`attested_close`,`attested_high`,`attested_low`,`attested_volume`,"
        "`attested_amount`,`created_at` FROM `qmt_kline_attestation_row` "
        "ORDER BY BINARY `attestation_id`"
    ),
    "qmt_kline_attestation_schema_migration": (
        "SELECT `migration_key`,`migration_hash`,`completed_at` "
        "FROM `qmt_kline_attestation_schema_migration` "
        "ORDER BY BINARY `migration_key`"
    ),
}
CHECKSUM_SQL = {
    table: f"CHECKSUM TABLE `{table}` EXTENDED" for table in QMT_TABLES
}
_LOWER_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_OPTION_PASSWORD_RE = re.compile(r"^[A-Za-z0-9_-]{48,160}$")


@dataclass(frozen=True, slots=True)
class OptionCredential:
    host: str
    port: int
    user: str
    password: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class TlsMaterial:
    ca_file: Path
    client_cert_file: Path
    client_key_file: Path
    server_cert_sha256: str


class CollationMigrationError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        evidence: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.evidence = dict(evidence or {})


class ReadProofDatabase(Protocol):
    def target_state(self) -> Mapping[str, Any]: ...
    def grants(self) -> Sequence[str]: ...
    def table_inventory(self, names: Sequence[str]) -> set[str]: ...
    def snapshot(self, table_name: str) -> Mapping[str, Any]: ...


def _canonical_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _json_value(value: Any) -> Any:
    if value is None or type(value) in {bool, int, float, str}:
        return value
    if isinstance(value, Decimal):
        return {"decimal": format(value, "f")}
    if isinstance(value, bytes):
        return {"bytes_hex": value.hex()}
    if hasattr(value, "isoformat"):
        return {"iso8601": value.isoformat()}
    raise CollationMigrationError(
        "ROW_PROOF_TYPE_INVALID", "row proof contains an unsupported type"
    )


def _digest(payload: Any) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _deep_freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze(item) for item in value)
    return value


def _deep_thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _deep_thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_deep_thaw(item) for item in value]
    return value


def _require_table(table_name: str) -> str:
    if table_name not in QMT_TABLES or table_name not in ALTER_DDL:
        raise CollationMigrationError(
            "TABLE_NOT_ALLOWED", "only the four frozen QMT tables are allowed"
        )
    return table_name


def _column_type(
    data_type: str,
    character_length: int | None,
    numeric_precision: int | None,
    numeric_scale: int | None,
) -> str:
    if data_type in {"varchar", "char"}:
        return f"{data_type}({character_length})"
    if data_type == "decimal":
        return f"decimal({numeric_precision},{numeric_scale})"
    return data_type


def _expected_structure(table_name: str, collation: str) -> dict[str, Any]:
    """Expand the compact code-owned contract; never use live metadata."""

    table_name = _require_table(table_name)
    character_types = {"varchar", "char", "text", "mediumtext"}
    nullable: dict[str, str] = {}
    columns = []
    for ordinal, spec in enumerate(QMT_ATTESTATION_COLUMN_SPECS[table_name], start=1):
        (
            name,
            data_type,
            character_length,
            numeric_precision,
            numeric_scale,
            is_nullable,
            default,
            extra,
        ) = spec
        nullable[name] = is_nullable
        character = data_type in character_types
        columns.append({
            "ordinal_position": ordinal,
            "column_name": name,
            "column_type": _column_type(
                data_type, character_length, numeric_precision, numeric_scale
            ),
            "data_type": data_type,
            "character_maximum_length": character_length,
            "numeric_precision": numeric_precision,
            "numeric_scale": numeric_scale,
            "is_nullable": is_nullable,
            "column_default": default,
            "extra": extra,
            "character_set_name": "utf8mb4" if character else None,
            "collation_name": collation if character else None,
            "column_comment": "",
            "generation_expression": "",
        })

    indexes = []
    constraints = []
    for index_name in sorted(QMT_ATTESTATION_INDEX_SPECS[table_name]):
        non_unique, index_columns = QMT_ATTESTATION_INDEX_SPECS[table_name][index_name]
        for sequence, column_name in enumerate(index_columns, start=1):
            indexes.append({
                "index_name": index_name,
                "non_unique": non_unique,
                "seq_in_index": sequence,
                "column_name": column_name,
                "collation": "A",
                "sub_part": None,
                "packed": None,
                "nullable": "YES" if nullable[column_name] == "YES" else "",
                "index_type": "BTREE",
                "comment": "",
                "index_comment": "",
                "is_visible": "YES",
                "expression": None,
            })
        if non_unique == 0:
            constraint_type = "PRIMARY KEY" if index_name == "PRIMARY" else "UNIQUE"
            for sequence, column_name in enumerate(index_columns, start=1):
                constraints.append({
                    "constraint_name": index_name,
                    "constraint_type": constraint_type,
                    "column_name": column_name,
                    "ordinal_position": sequence,
                    "position_in_unique_constraint": None,
                    "referenced_table_schema": None,
                    "referenced_table_name": None,
                    "referenced_column_name": None,
                    "update_rule": None,
                    "delete_rule": None,
                    "match_option": None,
                    "check_clause": None,
                })

    triggers = []
    for trigger_name in sorted(QMT_ATTESTATION_TRIGGER_SPECS):
        timing, event, event_table, statement = QMT_ATTESTATION_TRIGGER_SPECS[
            trigger_name
        ]
        if event_table == table_name:
            triggers.append({
                "trigger_name": trigger_name,
                "action_timing": timing,
                "event_manipulation": event,
                "action_order": 1,
                "action_condition": None,
                "action_statement": statement,
                "action_orientation": "ROW",
                "action_reference_old_row": "OLD",
                "action_reference_new_row": "NEW",
                "sql_mode": EXPECTED_SQL_MODE,
                "definer": QMT_ATTESTATION_TRIGGER_DEFINER,
                "character_set_client": "utf8mb4",
                "collation_connection": SOURCE_COLLATION,
                "database_collation": TARGET_COLLATION,
            })
    return {
        "table": {
            "table_name": table_name,
            "engine": "InnoDB",
            "row_format": "Dynamic",
            "table_collation": collation,
            "create_options": "",
            "table_comment": "",
        },
        "columns": columns,
        "indexes": indexes,
        "constraints": constraints,
        "triggers": triggers,
    }


FROZEN_SCHEMA_MANIFESTS = _deep_freeze({
    table: {
        "source": _expected_structure(table, SOURCE_COLLATION),
        "target": _expected_structure(table, TARGET_COLLATION),
    }
    for table in QMT_TABLES
})
# Independent reviewed digest of the literal contract above.  Never derive
# this expected value from FROZEN_SCHEMA_MANIFESTS at runtime.
FROZEN_SCHEMA_CONTRACT_SHA256 = (
    "fac7f2b32602bb677d9f0e5e33be6a833d11fa0c8842a67f0ba6f85b16b1f369"
)


def _assert_frozen_schema_integrity() -> None:
    reviewed_digest = (
        "fac7f2b32602bb677d9f0e5e33be6a833d11fa0c8842a67f0ba6f85b16b1f369"
    )
    if (
        FROZEN_SCHEMA_CONTRACT_SHA256 != reviewed_digest
        or _digest(_deep_thaw(FROZEN_SCHEMA_MANIFESTS)) != reviewed_digest
    ):
        raise CollationMigrationError(
            "FROZEN_SCHEMA_CONTRACT_TAMPERED",
            "code-owned QMT schema contract digest differs",
        )


def _frozen_manifest(table_name: str, status: str) -> dict[str, Any]:
    _require_table(table_name)
    if status not in {"source", "target"}:
        raise AssertionError("unknown frozen schema status")
    return _deep_thaw(FROZEN_SCHEMA_MANIFESTS[table_name][status])


def _normalized_default(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="strict")
    return str(value)


def _normalize_structure(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """Project live rows onto the exact frozen metadata fields."""

    try:
        table = snapshot["table"]
        columns = snapshot["columns"]
        indexes = snapshot["indexes"]
        constraints = snapshot["constraints"]
        triggers = snapshot["triggers"]
    except (KeyError, TypeError) as exc:
        raise CollationMigrationError(
            "TABLE_SCHEMA_DRIFT", "table metadata proof is incomplete"
        ) from exc
    normalized_columns = [{
        "ordinal_position": int(row.get("ordinal_position") or 0),
        "column_name": str(row.get("column_name") or ""),
        "column_type": str(row.get("column_type") or "").lower(),
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
        "column_default": _normalized_default(row.get("column_default")),
        "extra": str(row.get("extra") or "").lower(),
        "character_set_name": (
            str(row["character_set_name"]).lower()
            if row.get("character_set_name") is not None else None
        ),
        "collation_name": (
            str(row["collation_name"]).lower()
            if row.get("collation_name") is not None else None
        ),
        "column_comment": str(row.get("column_comment") or ""),
        "generation_expression": str(row.get("generation_expression") or ""),
    } for row in columns]
    normalized_indexes = [{
        "index_name": str(row.get("index_name") or ""),
        "non_unique": int(row.get("non_unique") or 0),
        "seq_in_index": int(row.get("seq_in_index") or 0),
        "column_name": str(row.get("column_name") or ""),
        "collation": str(row.get("collation") or ""),
        "sub_part": int(row["sub_part"]) if row.get("sub_part") is not None else None,
        "packed": row.get("packed"),
        "nullable": str(row.get("nullable") or ""),
        "index_type": str(row.get("index_type") or "").upper(),
        "comment": str(row.get("comment") or ""),
        "index_comment": str(row.get("index_comment") or ""),
        "is_visible": str(row.get("is_visible") or "").upper(),
        "expression": row.get("expression"),
    } for row in indexes]
    normalized_constraints = [{
        "constraint_name": str(row.get("constraint_name") or ""),
        "constraint_type": str(row.get("constraint_type") or "").upper(),
        "column_name": str(row.get("column_name") or ""),
        "ordinal_position": int(row.get("ordinal_position") or 0),
        "position_in_unique_constraint": row.get("position_in_unique_constraint"),
        "referenced_table_schema": row.get("referenced_table_schema"),
        "referenced_table_name": row.get("referenced_table_name"),
        "referenced_column_name": row.get("referenced_column_name"),
        "update_rule": row.get("update_rule"),
        "delete_rule": row.get("delete_rule"),
        "match_option": row.get("match_option"),
        "check_clause": row.get("check_clause"),
    } for row in constraints]
    normalized_triggers = [{
        "trigger_name": str(row.get("trigger_name") or ""),
        "action_timing": str(row.get("action_timing") or "").upper(),
        "event_manipulation": str(row.get("event_manipulation") or "").upper(),
        "action_order": int(row.get("action_order") or 0),
        "action_condition": row.get("action_condition"),
        "action_statement": " ".join(str(row.get("action_statement") or "").split()),
        "action_orientation": str(row.get("action_orientation") or "").upper(),
        "action_reference_old_row": str(row.get("action_reference_old_row") or ""),
        "action_reference_new_row": str(row.get("action_reference_new_row") or ""),
        "sql_mode": str(row.get("sql_mode") or ""),
        "definer": str(row.get("definer") or ""),
        "character_set_client": str(row.get("character_set_client") or "").lower(),
        "collation_connection": str(row.get("collation_connection") or "").lower(),
        "database_collation": str(row.get("database_collation") or "").lower(),
    } for row in triggers]
    return {
        "table": {
            "table_name": str(table.get("table_name") or ""),
            "engine": str(table.get("engine") or ""),
            "row_format": str(table.get("row_format") or ""),
            "table_collation": str(table.get("table_collation") or "").lower(),
            "create_options": str(table.get("create_options") or ""),
            "table_comment": str(table.get("table_comment") or ""),
        },
        "columns": normalized_columns,
        "indexes": normalized_indexes,
        "constraints": normalized_constraints,
        "triggers": normalized_triggers,
    }


def _schema_status(table_name: str, snapshot: Mapping[str, Any]) -> str:
    observed = _normalize_structure(snapshot)
    if observed == _frozen_manifest(table_name, "source"):
        return "source"
    if observed == _frozen_manifest(table_name, "target"):
        return "target"
    raise CollationMigrationError(
        "TABLE_SCHEMA_DRIFT",
        "engine, columns, indexes, constraints, triggers, definer or SQL mode differs",
        evidence={"table_name": table_name},
    )


def _binary_value(value: Any, *, field_name: str) -> int:
    if type(value) is bool:
        return int(value)
    if type(value) is int and value in {0, 1}:
        return value
    normalized = str(value or "").strip().upper()
    if normalized in {"0", "OFF"}:
        return 0
    if normalized in {"1", "ON"}:
        return 1
    raise CollationMigrationError(
        "TARGET_STATE_INVALID", f"invalid binary state for {field_name}"
    )


def _validate_target_state(
    state: Mapping[str, Any], *, expected_user: str
) -> dict[str, Any]:
    normalized = {
        "mysql_version": str(state.get("mysql_version") or "").strip(),
        "version_comment": str(state.get("version_comment") or "").strip(),
        "database_name": str(state.get("database_name") or "").strip(),
        "authenticated_user": str(state.get("authenticated_user") or "").strip(),
        "active_roles": str(state.get("active_roles") or "").strip().upper(),
        "server_uuid": str(state.get("server_uuid") or "").strip().lower(),
        "server_port": int(state.get("server_port") or 0),
        "server_hostname": str(state.get("server_hostname") or "").strip(),
        "log_bin": _binary_value(state.get("log_bin"), field_name="log_bin"),
        "binlog_format": str(state.get("binlog_format") or "").strip().upper(),
        "trust_creators": _binary_value(
            state.get("trust_creators"), field_name="log_bin_trust_function_creators"
        ),
        "session_sql_mode": str(state.get("session_sql_mode") or "").strip(),
        "character_set_client": str(state.get("character_set_client") or "").strip(),
        "collation_connection": str(state.get("collation_connection") or "").strip(),
        "database_collation": str(state.get("database_collation") or "").strip(),
        "tls_cipher": str(state.get("tls_cipher") or "").strip(),
        "tls_version": str(state.get("tls_version") or "").strip(),
        "tls_peer_cert_sha256": str(state.get("tls_peer_cert_sha256") or "").lower(),
        "expected_tls_peer_cert_sha256": str(
            state.get("expected_tls_peer_cert_sha256") or ""
        ).lower(),
    }
    if (
        normalized["mysql_version"] != EXPECTED_MYSQL_VERSION
        or "MYSQL" not in normalized["version_comment"].upper()
        or normalized["database_name"] != DATABASE_NAME
        or normalized["authenticated_user"] != expected_user
        or normalized["active_roles"] != "NONE"
        or normalized["server_uuid"] != EXPECTED_SERVER_UUID
        or normalized["server_port"] != EXPECTED_SERVER_PORT
        or normalized["server_hostname"] != EXPECTED_SERVER_HOSTNAME
        or normalized["log_bin"] != 1
        or normalized["binlog_format"] != "ROW"
        or normalized["trust_creators"] != 0
        or normalized["session_sql_mode"] != EXPECTED_SQL_MODE
        or normalized["character_set_client"] != "utf8mb4"
        or normalized["collation_connection"] != SOURCE_COLLATION
        or normalized["database_collation"] != TARGET_COLLATION
        or normalized["tls_version"] != "TLSv1.3"
        or not normalized["tls_cipher"]
        or not _LOWER_SHA256_RE.fullmatch(normalized["tls_peer_cert_sha256"])
        or normalized["tls_peer_cert_sha256"]
        != normalized["expected_tls_peer_cert_sha256"]
    ):
        raise CollationMigrationError(
            "TARGET_BOUNDARY_MISMATCH",
            "MySQL 8.4.11 identity, TLS, trust or collation boundary differs",
        )
    return normalized


def _validate_auditor_grants(grants: Sequence[str]) -> None:
    account = "`PROBIGA_QMT_AUDITOR`@`127.0.0.1`"
    expected = {
        (frozenset({"USAGE"}), "*.*", True),
        (frozenset({"SELECT"}), "PROBIGA.*", False),
    }
    observed = set()
    for raw in grants:
        grant = " ".join(str(raw).upper().split())
        if " WITH GRANT OPTION" in grant or grant.startswith("GRANT PROXY "):
            raise CollationMigrationError(
                "AUDITOR_GRANTS_INVALID", "delegation or proxy privilege is forbidden"
            )
        match = re.fullmatch(r"GRANT (.+?) ON (.+?) TO (.+)", grant)
        if match is None:
            raise CollationMigrationError(
                "AUDITOR_GRANTS_INVALID", "auditor grant syntax differs"
            )
        privilege_text, scope, principal = match.groups()
        require_ssl = principal.endswith(" REQUIRE SSL")
        principal = principal.removesuffix(" REQUIRE SSL")
        if principal != account:
            raise CollationMigrationError(
                "AUDITOR_GRANTS_INVALID", "auditor account identity differs"
            )
        observed.add((
            frozenset(item.strip() for item in privilege_text.split(",")),
            scope.replace("`", ""),
            require_ssl,
        ))
    if observed != expected or len(grants) != len(expected):
        raise CollationMigrationError(
            "AUDITOR_GRANTS_INVALID",
            "auditor must have only USAGE and schema-wide read inventory proof",
        )


def _validate_migrator_grants(grants: Sequence[str]) -> None:
    """Accept only SELECT+ALTER on each frozen table, never schema ALL/DML."""

    account = "`PROBIGA_MIGRATOR`@`127.0.0.1`"
    expected = {(frozenset({"USAGE"}), "*.*", True)}
    expected.update(
        (frozenset({"SELECT", "ALTER"}), f"PROBIGA.{table.upper()}", False)
        for table in QMT_TABLES
    )
    observed = set()
    for raw in grants:
        grant = " ".join(str(raw).upper().split())
        if (
            " WITH GRANT OPTION" in grant
            or grant.startswith("GRANT PROXY ")
            or any(
                forbidden in grant
                for forbidden in (
                    "ALL PRIVILEGES",
                    "INSERT",
                    "UPDATE",
                    "DELETE",
                    "DROP",
                    "TRIGGER",
                    "EVENT",
                    "ROUTINE",
                    "EXECUTE",
                )
            )
        ):
            raise CollationMigrationError(
                "MIGRATOR_GRANTS_INVALID", "write/routine/delegation privilege is forbidden"
            )
        match = re.fullmatch(r"GRANT (.+?) ON (.+?) TO (.+)", grant)
        if match is None:
            raise CollationMigrationError(
                "MIGRATOR_GRANTS_INVALID", "migrator grant syntax differs"
            )
        privilege_text, scope, principal = match.groups()
        require_ssl = principal.endswith(" REQUIRE SSL")
        principal = principal.removesuffix(" REQUIRE SSL")
        if principal != account:
            raise CollationMigrationError(
                "MIGRATOR_GRANTS_INVALID", "migrator identity differs"
            )
        observed.add((
            frozenset(item.strip() for item in privilege_text.split(",")),
            scope.replace("`", ""),
            require_ssl,
        ))
    if observed != expected or len(grants) != len(expected):
        raise CollationMigrationError(
            "MIGRATOR_GRANTS_INVALID",
            "migrator must have only four-table SELECT and ALTER",
        )


def _snapshot_proof(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    proof = {
        "exact_row_count": int(snapshot["exact_row_count"]),
        "canonical_row_sha256": str(snapshot["canonical_row_sha256"]),
        "row_checksum": int(snapshot["row_checksum"]),
        "data_bytes": int(snapshot["data_bytes"]),
        "index_bytes": int(snapshot["index_bytes"]),
        "allocated_bytes": int(snapshot["allocated_bytes"]),
    }
    if (
        proof["exact_row_count"] < 0
        or proof["row_checksum"] < 0
        or min(proof["data_bytes"], proof["index_bytes"], proof["allocated_bytes"])
        < 0
        or proof["allocated_bytes"] != proof["data_bytes"] + proof["index_bytes"]
        or not _LOWER_SHA256_RE.fullmatch(proof["canonical_row_sha256"])
    ):
        raise CollationMigrationError(
            "TABLE_PROOF_INVALID", "canonical row or storage proof is malformed"
        )
    return proof


def _protected_root_file(path: Path, *, exact_mode: int) -> Path:
    if not path.is_absolute() or not os.path.lexists(path) or path.is_symlink():
        raise CollationMigrationError(
            "PROTECTED_FILE_INVALID", "required root-owned file is missing or unsafe"
        )
    try:
        link_state = path.lstat()
        resolved = path.resolve(strict=True)
        state = resolved.stat()
        parent_state = resolved.parent.stat()
    except OSError as exc:
        raise CollationMigrationError(
            "PROTECTED_FILE_INVALID", "protected file metadata is unavailable"
        ) from exc
    if (
        not stat.S_ISREG(link_state.st_mode)
        or not stat.S_ISREG(state.st_mode)
        or link_state.st_uid != 0
        or state.st_uid != 0
        or stat.S_IMODE(link_state.st_mode) != exact_mode
        or stat.S_IMODE(state.st_mode) != exact_mode
        or parent_state.st_uid != 0
        or stat.S_IMODE(parent_state.st_mode) & 0o022
    ):
        raise CollationMigrationError(
            "PROTECTED_FILE_INVALID", "protected file ownership or mode differs"
        )
    return resolved


def _read_credential(path: Path, *, expected_user: str) -> OptionCredential:
    resolved = _protected_root_file(path, exact_mode=0o600)
    parser = configparser.RawConfigParser(interpolation=None, strict=True)
    try:
        with resolved.open("r", encoding="utf-8") as stream:
            parser.read_file(stream)
    except (OSError, UnicodeError, configparser.Error) as exc:
        raise CollationMigrationError(
            "MYSQL_OPTION_INVALID", "MySQL option file cannot be parsed"
        ) from exc
    expected_keys = {"protocol", "host", "port", "user", "password"}
    if parser.sections() != ["client"] or set(parser.options("client")) != expected_keys:
        raise CollationMigrationError("MYSQL_OPTION_INVALID", "option file keys differ")
    values = {key: parser.get("client", key, raw=True).strip() for key in expected_keys}
    if (
        values["protocol"].casefold() != "tcp"
        or values["host"] != "127.0.0.1"
        or values["port"] != str(EXPECTED_SERVER_PORT)
        or values["user"] != expected_user.split("@", 1)[0]
        or _OPTION_PASSWORD_RE.fullmatch(values["password"]) is None
    ):
        raise CollationMigrationError("MYSQL_OPTION_INVALID", "option target differs")
    return OptionCredential(
        host=values["host"],
        port=int(values["port"]),
        user=values["user"],
        password=values["password"],
    )


def _tls_material() -> TlsMaterial:
    ca = _protected_root_file(FIXED_TLS_CA_FILE, exact_mode=0o644)
    cert = _protected_root_file(FIXED_TLS_CLIENT_CERT_FILE, exact_mode=0o644)
    key = _protected_root_file(FIXED_TLS_CLIENT_KEY_FILE, exact_mode=0o600)
    pin = _protected_root_file(FIXED_TLS_SERVER_PIN_FILE, exact_mode=0o644)
    for protected in (ca, cert, key, pin):
        for parent in protected.parents:
            state = parent.stat()
            if state.st_uid != 0 or stat.S_IMODE(state.st_mode) & 0o022:
                raise CollationMigrationError(
                    "TLS_MATERIAL_INVALID", "TLS ancestor permissions are unsafe"
                )
    try:
        ca_bytes = ca.read_bytes()
        cert_bytes = cert.read_bytes()
        key_bytes = key.read_bytes()
        pin_text = pin.read_text(encoding="ascii")
    except (OSError, UnicodeError) as exc:
        raise CollationMigrationError(
            "TLS_MATERIAL_INVALID", "TLS material cannot be read"
        ) from exc
    if (
        max(len(ca_bytes), len(cert_bytes), len(key_bytes)) > 262_144
        or b"-----BEGIN CERTIFICATE-----" not in ca_bytes
        or b"-----BEGIN CERTIFICATE-----" not in cert_bytes
        or b"PRIVATE KEY-----" not in key_bytes
        or pin_text != pin_text.strip() + "\n"
        or not _LOWER_SHA256_RE.fullmatch(pin_text.strip())
    ):
        raise CollationMigrationError(
            "TLS_MATERIAL_INVALID", "TLS PEM or peer pin boundary differs"
        )
    return TlsMaterial(ca, cert, key, pin_text.strip())


def _audit_boundaries(
    database: ReadProofDatabase,
    migrator_database: ReadProofDatabase,
) -> dict[str, Any]:
    state = _validate_target_state(
        database.target_state(), expected_user=EXPECTED_AUDITOR_USER
    )
    migrator_state = _validate_target_state(
        migrator_database.target_state(), expected_user=EXPECTED_MIGRATOR_USER
    )
    _validate_auditor_grants(database.grants())
    _validate_migrator_grants(migrator_database.grants())
    identity_fields = (
        "database_name",
        "server_uuid",
        "server_port",
        "server_hostname",
        "tls_peer_cert_sha256",
    )
    if any(state[field] != migrator_state[field] for field in identity_fields):
        raise CollationMigrationError(
            "INDEPENDENT_CONNECTION_IDENTITY_MISMATCH",
            "auditor and migrator proof connections reached different servers",
        )
    qmt_inventory = database.table_inventory(QMT_TABLES)
    governance_inventory = database.table_inventory(GOVERNANCE_TABLES)
    if qmt_inventory != set(QMT_TABLES):
        raise CollationMigrationError(
            "QMT_TABLE_INVENTORY_MISMATCH", "exactly four QMT tables must exist"
        )
    if governance_inventory:
        raise CollationMigrationError(
            "GOVERNANCE_SCHEMA_ALREADY_PRESENT",
            "governance tables must be absent during this maintenance",
        )
    return state


def audit_database(
    database: ReadProofDatabase,
    migrator_database: ReadProofDatabase | None = None,
) -> dict[str, Any]:
    """Build a deterministic, double-read plan without DDL or DML."""

    _assert_frozen_schema_integrity()
    if migrator_database is None:
        raise CollationMigrationError(
            "MIGRATOR_PROOF_CONNECTION_REQUIRED",
            "audit requires an independent migrator credential/grant proof connection",
        )
    state = _audit_boundaries(database, migrator_database)
    first_snapshots = {
        table_name: database.snapshot(table_name) for table_name in QMT_TABLES
    }
    second_snapshots = {
        table_name: database.snapshot(table_name) for table_name in QMT_TABLES
    }
    second_state = _audit_boundaries(database, migrator_database)
    if state != second_state:
        raise CollationMigrationError(
            "AUDIT_BOUNDARY_CHANGED", "database boundary changed during audit"
        )
    for table_name in QMT_TABLES:
        first = first_snapshots[table_name]
        second = second_snapshots[table_name]
        if (
            _normalize_structure(first) != _normalize_structure(second)
            or _snapshot_proof(first) != _snapshot_proof(second)
        ):
            raise CollationMigrationError(
                "AUDIT_SNAPSHOT_UNSTABLE",
                "full-table schema, rows, checksum or storage changed between reads",
                evidence={"table_name": table_name},
            )
    operations = []
    source_count = 0
    target_count = 0
    total_rows = 0
    total_bytes = 0
    for ordinal, table_name in enumerate(QMT_TABLES, start=1):
        snapshot = second_snapshots[table_name]
        status = _schema_status(table_name, snapshot)
        proof = _snapshot_proof(snapshot)
        source_count += int(status == "source")
        target_count += int(status == "target")
        total_rows += proof["exact_row_count"]
        total_bytes += proof["allocated_bytes"]
        operations.append({
            "ordinal": ordinal,
            "table_name": table_name,
            "action": "convert" if status == "source" else "already_target",
            "from_collation": SOURCE_COLLATION,
            "to_collation": TARGET_COLLATION,
            "ddl": ALTER_DDL[table_name],
            "ddl_sha256": _digest(ALTER_DDL[table_name]),
            "source_structure_sha256": _digest(
                _frozen_manifest(table_name, "source")
            ),
            "target_structure_sha256": _digest(
                _frozen_manifest(table_name, "target")
            ),
            **proof,
        })
    plan = {
        "schema": PLAN_SCHEMA,
        "database_identity": {
            "database_name": state["database_name"],
            "server_uuid": state["server_uuid"],
            "server_port": state["server_port"],
            "server_hostname": state["server_hostname"],
        },
        "authenticated_user": state["authenticated_user"],
        "connection_collation": state["collation_connection"],
        "database_collation": state["database_collation"],
        "global_trust_function_creators": "OFF",
        "allowed_tables": list(QMT_TABLES),
        "required_absent_governance_tables": list(GOVERNANCE_TABLES),
        "frozen_schema_contract_sha256": FROZEN_SCHEMA_CONTRACT_SHA256,
        "operations": operations,
        "totals": {
            "table_count": len(QMT_TABLES),
            "source_table_count": source_count,
            "target_table_count": target_count,
            "exact_row_count": total_rows,
            "allocated_bytes": total_bytes,
        },
        "row_count_measurement": "CANONICAL_STREAM_ROW_COUNT_V1",
        "row_content_measurement": "CANONICAL_SHA256_PRIMARY_KEY_ORDER_V1",
        "secondary_row_measurement": "CHECKSUM_TABLE_EXTENDED",
        "byte_measurement": "INFORMATION_SCHEMA_DATA_LENGTH_PLUS_INDEX_LENGTH",
        "stability_measurement": "FULL_DOUBLE_READ_EXACT_V1",
        "apply_disabled_reason": "INDEPENDENT_SAFETY_PROOF_NOT_YET_COMPLETE",
        "zero_business_row_mutations": True,
        "ordinary_deploy_integration": False,
    }
    return {
        "schema": RESULT_SCHEMA,
        "status": "AUDIT_ONLY",
        "success": True,
        "ddl_executed": False,
        "zero_ddl": True,
        "apply_eligible": False,
        "schema_eligible": source_count == len(QMT_TABLES),
        "plan_sha256": _digest(plan),
        "plan": plan,
        "automatic_real_order_submission": False,
    }


_STATE_SQL = (
    "SELECT VERSION() AS mysql_version, @@version_comment AS version_comment, "
    "DATABASE() AS database_name, CURRENT_USER() AS authenticated_user, "
    "CURRENT_ROLE() AS active_roles, @@server_uuid AS server_uuid, "
    "@@port AS server_port, @@hostname AS server_hostname, "
    "@@GLOBAL.log_bin AS log_bin, @@GLOBAL.binlog_format AS binlog_format, "
    "@@GLOBAL.log_bin_trust_function_creators AS trust_creators, "
    "@@SESSION.sql_mode AS session_sql_mode, "
    "@@SESSION.character_set_client AS character_set_client, "
    "@@SESSION.collation_connection AS collation_connection, "
    "@@collation_database AS database_collation"
)


class PymysqlReadProofDatabase:
    """Read-only production adapter; PyMySQL is imported only by the opener."""

    def __init__(
        self,
        connection: Any,
        *,
        expected_peer_cert_sha256: str,
        observed_peer_cert_sha256: str,
    ) -> None:
        self.connection = connection
        self.expected_peer_cert_sha256 = expected_peer_cert_sha256
        self.observed_peer_cert_sha256 = observed_peer_cert_sha256

    def _one(self, sql: str, params: Sequence[Any] = ()) -> Mapping[str, Any]:
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            row = cursor.fetchone()
        if not isinstance(row, Mapping):
            raise CollationMigrationError(
                "DATABASE_PROOF_INCOMPLETE", "database proof is incomplete"
            )
        return row

    def _all(self, sql: str, params: Sequence[Any] = ()) -> list[Mapping[str, Any]]:
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
        if not isinstance(rows, Sequence) or any(
            not isinstance(row, Mapping) for row in rows
        ):
            raise CollationMigrationError(
                "DATABASE_PROOF_INCOMPLETE", "database proof is incomplete"
            )
        return list(rows)

    def target_state(self) -> Mapping[str, Any]:
        state = dict(self._one(_STATE_SQL))
        cipher = self._one("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
        version = self._one("SHOW SESSION STATUS LIKE 'Ssl_version'")
        state["tls_cipher"] = cipher.get("Value") or cipher.get("VALUE") or ""
        state["tls_version"] = version.get("Value") or version.get("VALUE") or ""
        state["tls_peer_cert_sha256"] = self.observed_peer_cert_sha256
        state["expected_tls_peer_cert_sha256"] = self.expected_peer_cert_sha256
        return state

    def grants(self) -> Sequence[str]:
        rows = self._all("SHOW GRANTS FOR CURRENT_USER()")
        if any(len(row) != 1 for row in rows):
            raise CollationMigrationError(
                "AUDITOR_GRANTS_INVALID", "grant rows are malformed"
            )
        return [str(next(iter(row.values())) or "") for row in rows]

    def table_inventory(self, names: Sequence[str]) -> set[str]:
        if tuple(names) not in {QMT_TABLES, GOVERNANCE_TABLES}:
            raise CollationMigrationError("TABLE_NOT_ALLOWED", "inventory not allowed")
        placeholders = ",".join(["%s"] * len(names))
        rows = self._all(
            "SELECT TABLE_NAME AS table_name FROM information_schema.TABLES "
            f"WHERE TABLE_SCHEMA=%s AND TABLE_NAME IN ({placeholders}) "
            "ORDER BY BINARY TABLE_NAME",
            (DATABASE_NAME, *names),
        )
        return {str(row["table_name"]) for row in rows}

    def _canonical_row_proof(self, table_name: str) -> tuple[int, str]:
        columns = [spec[0] for spec in QMT_ATTESTATION_COLUMN_SPECS[table_name]]
        hasher = hashlib.sha256()
        hasher.update(b"probiga.qmt-canonical-row-proof.v1\x00")
        hasher.update(table_name.encode("ascii") + b"\x00")
        count = 0
        with self.connection.cursor() as cursor:
            cursor.execute(ROW_PROOF_SQL[table_name])
            while True:
                rows = cursor.fetchmany(512)
                if not rows:
                    break
                for row in rows:
                    if not isinstance(row, Mapping) or set(row) != set(columns):
                        raise CollationMigrationError(
                            "ROW_PROOF_INVALID", "row proof columns differ"
                        )
                    encoded = _canonical_bytes([
                        [name, _json_value(row[name])] for name in columns
                    ])
                    hasher.update(len(encoded).to_bytes(8, "big"))
                    hasher.update(encoded)
                    count += 1
        return count, hasher.hexdigest()

    def snapshot(self, table_name: str) -> Mapping[str, Any]:
        table_name = _require_table(table_name)
        table = dict(self._one(
            "SELECT TABLE_NAME AS table_name, ENGINE AS engine, "
            "ROW_FORMAT AS row_format, TABLE_COLLATION AS table_collation, "
            "CREATE_OPTIONS AS create_options, TABLE_COMMENT AS table_comment, "
            "COALESCE(DATA_LENGTH,0) AS data_bytes, "
            "COALESCE(INDEX_LENGTH,0) AS index_bytes "
            "FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s "
            "AND BINARY TABLE_NAME=BINARY %s",
            (DATABASE_NAME, table_name),
        ))
        columns = self._all(
            "SELECT ORDINAL_POSITION AS ordinal_position, COLUMN_NAME AS column_name, "
            "COLUMN_TYPE AS column_type, DATA_TYPE AS data_type, "
            "CHARACTER_MAXIMUM_LENGTH AS character_maximum_length, "
            "NUMERIC_PRECISION AS numeric_precision, NUMERIC_SCALE AS numeric_scale, "
            "IS_NULLABLE AS is_nullable, COLUMN_DEFAULT AS column_default, "
            "EXTRA AS extra, CHARACTER_SET_NAME AS character_set_name, "
            "COLLATION_NAME AS collation_name, COLUMN_COMMENT AS column_comment, "
            "GENERATION_EXPRESSION AS generation_expression "
            "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s "
            "AND BINARY TABLE_NAME=BINARY %s ORDER BY ORDINAL_POSITION",
            (DATABASE_NAME, table_name),
        )
        indexes = self._all(
            "SELECT INDEX_NAME AS index_name, NON_UNIQUE AS non_unique, "
            "SEQ_IN_INDEX AS seq_in_index, COLUMN_NAME AS column_name, "
            "COLLATION AS collation, SUB_PART AS sub_part, PACKED AS packed, "
            "NULLABLE AS nullable, INDEX_TYPE AS index_type, COMMENT AS comment, "
            "INDEX_COMMENT AS index_comment, IS_VISIBLE AS is_visible, "
            "EXPRESSION AS expression FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA=%s AND BINARY TABLE_NAME=BINARY %s "
            "ORDER BY BINARY INDEX_NAME, SEQ_IN_INDEX",
            (DATABASE_NAME, table_name),
        )
        constraints = self._all(
            "SELECT tc.CONSTRAINT_NAME AS constraint_name, "
            "tc.CONSTRAINT_TYPE AS constraint_type, kcu.COLUMN_NAME AS column_name, "
            "kcu.ORDINAL_POSITION AS ordinal_position, "
            "kcu.POSITION_IN_UNIQUE_CONSTRAINT AS position_in_unique_constraint, "
            "kcu.REFERENCED_TABLE_SCHEMA AS referenced_table_schema, "
            "kcu.REFERENCED_TABLE_NAME AS referenced_table_name, "
            "kcu.REFERENCED_COLUMN_NAME AS referenced_column_name, "
            "rc.UPDATE_RULE AS update_rule, rc.DELETE_RULE AS delete_rule, "
            "rc.MATCH_OPTION AS match_option, cc.CHECK_CLAUSE AS check_clause "
            "FROM information_schema.TABLE_CONSTRAINTS tc "
            "LEFT JOIN information_schema.KEY_COLUMN_USAGE kcu "
            "ON kcu.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA "
            "AND kcu.TABLE_NAME=tc.TABLE_NAME "
            "AND kcu.CONSTRAINT_NAME=tc.CONSTRAINT_NAME "
            "LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS rc "
            "ON rc.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA "
            "AND rc.TABLE_NAME=tc.TABLE_NAME "
            "AND rc.CONSTRAINT_NAME=tc.CONSTRAINT_NAME "
            "LEFT JOIN information_schema.CHECK_CONSTRAINTS cc "
            "ON cc.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA "
            "AND cc.CONSTRAINT_NAME=tc.CONSTRAINT_NAME "
            "WHERE tc.CONSTRAINT_SCHEMA=%s AND BINARY tc.TABLE_NAME=BINARY %s "
            "ORDER BY BINARY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION",
            (DATABASE_NAME, table_name),
        )
        triggers = self._all(
            "SELECT TRIGGER_NAME AS trigger_name, ACTION_TIMING AS action_timing, "
            "EVENT_MANIPULATION AS event_manipulation, ACTION_ORDER AS action_order, "
            "ACTION_CONDITION AS action_condition, ACTION_STATEMENT AS action_statement, "
            "ACTION_ORIENTATION AS action_orientation, "
            "ACTION_REFERENCE_OLD_ROW AS action_reference_old_row, "
            "ACTION_REFERENCE_NEW_ROW AS action_reference_new_row, SQL_MODE AS sql_mode, "
            "DEFINER AS definer, CHARACTER_SET_CLIENT AS character_set_client, "
            "COLLATION_CONNECTION AS collation_connection, "
            "DATABASE_COLLATION AS database_collation "
            "FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=%s "
            "AND BINARY EVENT_OBJECT_TABLE=BINARY %s ORDER BY BINARY TRIGGER_NAME",
            (DATABASE_NAME, table_name),
        )
        exact_count, row_sha = self._canonical_row_proof(table_name)
        checksum = self._one(CHECKSUM_SQL[table_name])
        checksum_value = checksum.get("Checksum")
        if checksum_value is None:
            checksum_value = checksum.get("CHECKSUM")
        if checksum_value is None:
            raise CollationMigrationError(
                "TABLE_CHECKSUM_UNAVAILABLE", "secondary checksum is unavailable"
            )
        data_bytes = int(table.pop("data_bytes") or 0)
        index_bytes = int(table.pop("index_bytes") or 0)
        return {
            "table": table,
            "columns": columns,
            "indexes": indexes,
            "constraints": constraints,
            "triggers": triggers,
            "exact_row_count": exact_count,
            "canonical_row_sha256": row_sha,
            "row_checksum": int(checksum_value),
            "data_bytes": data_bytes,
            "index_bytes": index_bytes,
            "allocated_bytes": data_bytes + index_bytes,
        }


def _open_read_database(*, role: str) -> PymysqlReadProofDatabase:
    """Open one fixed proof account; PyMySQL is deliberately lazy."""

    _require_isolated_interpreter()
    if role == "auditor":
        option_file = AUDITOR_OPTION_FILE
        expected_user = EXPECTED_AUDITOR_USER
    elif role == "migrator":
        option_file = MIGRATOR_OPTION_FILE
        expected_user = EXPECTED_MIGRATOR_USER
    else:
        raise AssertionError("unknown proof database role")
    credential = _read_credential(
        option_file, expected_user=expected_user
    )
    tls = _tls_material()
    pymysql = importlib.import_module("pymysql")
    cursors = importlib.import_module("pymysql.cursors")
    connection = pymysql.connect(
        host=credential.host,
        port=credential.port,
        user=credential.user,
        password=credential.password,
        database=DATABASE_NAME,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=cursors.SSDictCursor,
        connect_timeout=10,
        read_timeout=900,
        write_timeout=30,
        local_infile=False,
        ssl_ca=str(tls.ca_file),
        ssl_cert=str(tls.client_cert_file),
        ssl_key=str(tls.client_key_file),
        ssl_verify_cert=True,
        ssl_verify_identity=True,
        program_name=f"probiga-qmt-collation-{role}-proof-v2",
    )
    try:
        peer = connection._sock.getpeercert(binary_form=True)
        if not isinstance(peer, bytes) or not peer:
            raise CollationMigrationError(
                "TLS_PEER_CERT_INVALID", "TLS peer certificate is unavailable"
            )
        observed_peer_sha = hashlib.sha256(peer).hexdigest()
        if observed_peer_sha != tls.server_cert_sha256:
            raise CollationMigrationError(
                "TLS_PEER_CERT_INVALID", "TLS peer certificate pin differs"
            )
    except BaseException:
        connection.close()
        raise
    return PymysqlReadProofDatabase(
        connection,
        expected_peer_cert_sha256=tls.server_cert_sha256,
        observed_peer_cert_sha256=observed_peer_sha,
    )


def _open_auditor_database() -> PymysqlReadProofDatabase:
    return _open_read_database(role="auditor")


def _open_migrator_proof_database() -> PymysqlReadProofDatabase:
    return _open_read_database(role="migrator")


def apply_migration(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
    """Hard blocked: no boolean/caller text can authorize production DDL."""

    raise CollationMigrationError(
        "APPLY_SAFETY_PROOF_UNAVAILABLE",
        "apply is disabled until OS writer, artifact, TLS and boundary proofs are complete",
    )


def _require_root_execution() -> None:
    if os.name != "posix" or not hasattr(os, "geteuid") or os.geteuid() != 0:
        raise CollationMigrationError(
            "ROOT_EXECUTION_REQUIRED", "maintenance audit must run as root on POSIX"
        )


def _require_isolated_interpreter() -> None:
    if sys.flags.isolated != 1 or sys.flags.no_user_site != 1:
        raise CollationMigrationError(
            "ISOLATED_INTERPRETER_REQUIRED",
            "the executable audit boundary requires Python -I",
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit QMT collation maintenance")
    parser.add_argument(
        "command",
        nargs="?",
        choices=("audit", "apply", "startup-check"),
        default="audit",
    )
    parser.add_argument("--expected-plan-sha", default="")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    databases: list[PymysqlReadProofDatabase] = []
    try:
        if args.command == "apply":
            apply_migration(expected_plan_sha256=args.expected_plan_sha)
        _require_isolated_interpreter()
        if args.command == "startup-check":
            if args.expected_plan_sha:
                raise CollationMigrationError(
                    "STARTUP_ARGUMENT_INVALID",
                    "startup check accepts no apply arguments",
                )
            print(json.dumps({
                "schema": RESULT_SCHEMA,
                "status": "STARTUP_BOUNDARY_VERIFIED",
                "success": True,
                "isolated_interpreter": True,
                "project_module_imported": False,
                "automatic_real_order_submission": False,
            }, ensure_ascii=False, sort_keys=True))
            return 0
        _require_root_execution()
        if args.expected_plan_sha:
            raise CollationMigrationError(
                "AUDIT_ARGUMENT_INVALID", "apply-only arguments are forbidden in audit"
            )
        database = _open_auditor_database()
        databases.append(database)
        migrator_database = _open_migrator_proof_database()
        databases.append(migrator_database)
        result = audit_database(database, migrator_database)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    except CollationMigrationError as exc:
        print(json.dumps({
            "schema": RESULT_SCHEMA,
            "status": "FAILED",
            "success": False,
            "error_code": exc.code,
            "message": str(exc),
            "completed": False,
            "automatic_real_order_submission": False,
        }, ensure_ascii=False, sort_keys=True))
        return 2
    except Exception:
        print(json.dumps({
            "schema": RESULT_SCHEMA,
            "status": "FAILED",
            "success": False,
            "error_code": "UNEXPECTED_AUDIT_FAILURE",
            "message": "read-only audit failed before complete verification",
            "completed": False,
            "automatic_real_order_submission": False,
        }, ensure_ascii=False, sort_keys=True))
        return 2
    finally:
        for database in databases:
            database.connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
