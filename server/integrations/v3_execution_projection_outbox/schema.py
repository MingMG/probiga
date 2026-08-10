"""Forward-only candidate DDL; intentionally not wired into V2 migrations."""

from __future__ import annotations

import re
from typing import Any

from sqlalchemy import text


V3_PROJECTION_OUTBOX_DDL = (
    """
    CREATE TABLE IF NOT EXISTS st_execution_projection_outbox_v2 (
        outbox_sequence BIGINT NOT NULL AUTO_INCREMENT,
        outbox_id CHAR(64) NOT NULL,
        projection_id CHAR(64) NOT NULL,
        projection_payload_hash CHAR(64) NOT NULL,
        canonical_payload_hash CHAR(64) NOT NULL,
        payload_json LONGTEXT NOT NULL,
        source_order_id VARCHAR(64) NOT NULL,
        source_transition_id CHAR(64) NOT NULL,
        source_sequence BIGINT NOT NULL,
        status VARCHAR(24) NOT NULL,
        attempt_count INT NOT NULL DEFAULT 0,
        available_at DATETIME(6) NOT NULL,
        lease_owner VARCHAR(120) NULL,
        lease_token CHAR(64) NULL,
        lease_until DATETIME(6) NULL,
        last_error VARCHAR(1000) NULL,
        published_at DATETIME(6) NULL,
        created_at DATETIME(6) NOT NULL,
        updated_at DATETIME(6) NOT NULL,
        PRIMARY KEY (outbox_id),
        UNIQUE KEY uk_v3_projection_outbox_sequence (outbox_sequence),
        UNIQUE KEY uk_v3_projection_outbox_projection (projection_id),
        UNIQUE KEY uk_v3_projection_outbox_transition (source_transition_id),
        UNIQUE KEY uk_v3_projection_outbox_order_sequence
            (source_order_id, source_sequence),
        KEY idx_v3_projection_outbox_lease
            (status, available_at, lease_until, outbox_sequence)
    ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC
      DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    """,
    """
    CREATE TABLE IF NOT EXISTS st_execution_projection_worker_checkpoint_v3 (
        worker_id VARCHAR(120) PRIMARY KEY,
        last_outbox_sequence BIGINT NOT NULL,
        last_outbox_id CHAR(64) NOT NULL,
        last_projection_id CHAR(64) NOT NULL,
        updated_at DATETIME(6) NOT NULL
    ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC
      DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    """,
    """
    CREATE TABLE IF NOT EXISTS st_execution_projection_order_baseline_v3 (
        source_order_id VARCHAR(64) PRIMARY KEY,
        baseline_sequence BIGINT NOT NULL,
        baseline_transition_id CHAR(64) NOT NULL,
        baseline_order_state_hash CHAR(64) NOT NULL,
        reconciliation_evidence_hash CHAR(64) NOT NULL,
        baseline_audit_hash CHAR(64) NOT NULL,
        reconciled_by VARCHAR(120) NOT NULL,
        reconciled_at DATETIME(6) NOT NULL,
        UNIQUE KEY uk_v3_projection_baseline_audit (baseline_audit_hash),
        UNIQUE KEY uk_v3_projection_baseline_evidence
            (reconciliation_evidence_hash)
    ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC
      DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    """,
    """
    CREATE TABLE IF NOT EXISTS
        st_execution_projection_dead_letter_reconciliation_v3 (
        reconciliation_audit_hash CHAR(64) PRIMARY KEY,
        outbox_id CHAR(64) NOT NULL,
        source_order_id VARCHAR(64) NOT NULL,
        source_sequence BIGINT NOT NULL,
        previous_attempt_count INT NOT NULL,
        action VARCHAR(24) NOT NULL,
        reason VARCHAR(1000) NOT NULL,
        reconciled_by VARCHAR(120) NOT NULL,
        capability_hash CHAR(64) NOT NULL,
        reconciled_at DATETIME(6) NOT NULL,
        KEY idx_v3_projection_reconciliation_attempt
            (outbox_id, previous_attempt_count),
        KEY idx_v3_projection_reconciliation_order
            (source_order_id, source_sequence, reconciled_at)
    ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC
      DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    """,
)

PRODUCTION_ACTIVATION_ALLOWED = False


class V3ProjectionOutboxSchemaError(RuntimeError):
    pass


_OUTBOX_TABLE = "st_execution_projection_outbox_v2"
_CHECKPOINT_TABLE = "st_execution_projection_worker_checkpoint_v3"
_BASELINE_TABLE = "st_execution_projection_order_baseline_v3"
_RECONCILIATION_TABLE = (
    "st_execution_projection_dead_letter_reconciliation_v3"
)
_TABLES = (
    _OUTBOX_TABLE,
    _CHECKPOINT_TABLE,
    _BASELINE_TABLE,
    _RECONCILIATION_TABLE,
)


def _column(
    column_type: str,
    *,
    nullable: bool = False,
    default: str | None = None,
    extra: str = "",
    collation: str = "",
) -> dict[str, Any]:
    return {
        "type": column_type,
        "nullable": nullable,
        "default": default,
        "extra": extra,
        "collation": collation,
    }


_BIN = "utf8mb4_bin"
_EXPECTED_COLUMNS = {
    _OUTBOX_TABLE: {
        "outbox_sequence": _column("bigint", extra="auto_increment"),
        "outbox_id": _column("char(64)", collation=_BIN),
        "projection_id": _column("char(64)", collation=_BIN),
        "projection_payload_hash": _column("char(64)", collation=_BIN),
        "canonical_payload_hash": _column("char(64)", collation=_BIN),
        "payload_json": _column("longtext", collation=_BIN),
        "source_order_id": _column("varchar(64)", collation=_BIN),
        "source_transition_id": _column("char(64)", collation=_BIN),
        "source_sequence": _column("bigint"),
        "status": _column("varchar(24)", collation=_BIN),
        "attempt_count": _column("int", default="0"),
        "available_at": _column("datetime(6)"),
        "lease_owner": _column("varchar(120)", nullable=True, collation=_BIN),
        "lease_token": _column("char(64)", nullable=True, collation=_BIN),
        "lease_until": _column("datetime(6)", nullable=True),
        "last_error": _column("varchar(1000)", nullable=True, collation=_BIN),
        "published_at": _column("datetime(6)", nullable=True),
        "created_at": _column("datetime(6)"),
        "updated_at": _column("datetime(6)"),
    },
    _CHECKPOINT_TABLE: {
        "worker_id": _column("varchar(120)", collation=_BIN),
        "last_outbox_sequence": _column("bigint"),
        "last_outbox_id": _column("char(64)", collation=_BIN),
        "last_projection_id": _column("char(64)", collation=_BIN),
        "updated_at": _column("datetime(6)"),
    },
    _BASELINE_TABLE: {
        "source_order_id": _column("varchar(64)", collation=_BIN),
        "baseline_sequence": _column("bigint"),
        "baseline_transition_id": _column("char(64)", collation=_BIN),
        "baseline_order_state_hash": _column("char(64)", collation=_BIN),
        "reconciliation_evidence_hash": _column("char(64)", collation=_BIN),
        "baseline_audit_hash": _column("char(64)", collation=_BIN),
        "reconciled_by": _column("varchar(120)", collation=_BIN),
        "reconciled_at": _column("datetime(6)"),
    },
    _RECONCILIATION_TABLE: {
        "reconciliation_audit_hash": _column("char(64)", collation=_BIN),
        "outbox_id": _column("char(64)", collation=_BIN),
        "source_order_id": _column("varchar(64)", collation=_BIN),
        "source_sequence": _column("bigint"),
        "previous_attempt_count": _column("int"),
        "action": _column("varchar(24)", collation=_BIN),
        "reason": _column("varchar(1000)", collation=_BIN),
        "reconciled_by": _column("varchar(120)", collation=_BIN),
        "capability_hash": _column("char(64)", collation=_BIN),
        "reconciled_at": _column("datetime(6)"),
    },
}

_EXPECTED_OUTBOX_UNIQUE_INDEXES = {
    "PRIMARY": ("outbox_id",),
    "uk_v3_projection_outbox_sequence": ("outbox_sequence",),
    "uk_v3_projection_outbox_projection": ("projection_id",),
    "uk_v3_projection_outbox_transition": ("source_transition_id",),
    "uk_v3_projection_outbox_order_sequence": (
        "source_order_id",
        "source_sequence",
    ),
}
_EXPECTED_INDEXES = {
    _OUTBOX_TABLE: {
        **{
            name: {"unique": True, "columns": columns}
            for name, columns in _EXPECTED_OUTBOX_UNIQUE_INDEXES.items()
        },
        "idx_v3_projection_outbox_lease": {
            "unique": False,
            "columns": (
                "status",
                "available_at",
                "lease_until",
                "outbox_sequence",
            ),
        },
    },
    _CHECKPOINT_TABLE: {
        "PRIMARY": {"unique": True, "columns": ("worker_id",)},
    },
    _BASELINE_TABLE: {
        "PRIMARY": {"unique": True, "columns": ("source_order_id",)},
        "uk_v3_projection_baseline_audit": {
            "unique": True,
            "columns": ("baseline_audit_hash",),
        },
        "uk_v3_projection_baseline_evidence": {
            "unique": True,
            "columns": ("reconciliation_evidence_hash",),
        },
    },
    _RECONCILIATION_TABLE: {
        "PRIMARY": {
            "unique": True,
            "columns": ("reconciliation_audit_hash",),
        },
        "idx_v3_projection_reconciliation_attempt": {
            "unique": False,
            "columns": ("outbox_id", "previous_attempt_count"),
        },
        "idx_v3_projection_reconciliation_order": {
            "unique": False,
            "columns": ("source_order_id", "source_sequence", "reconciled_at"),
        },
    },
}


def _normalize_column_type(value: object) -> str:
    normalized = " ".join(str(value or "").casefold().split())
    integer = re.fullmatch(
        r"(smallint|mediumint|int|integer|bigint)\(\d+\)( unsigned)?",
        normalized,
    )
    if integer is not None:
        return f"{integer.group(1)}{integer.group(2) or ''}"
    return normalized


def _normalize_default(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    expression = "".join(normalized.casefold().split())
    if re.fullmatch(r"current_timestamp(?:\(\d+\))?", expression):
        return expression
    return normalized


def _mappings(result: Any) -> tuple[dict[str, Any], ...]:
    return tuple(dict(row) for row in result.mappings())


def validate_v3_projection_outbox_schema(connection: Any) -> None:
    """Validate the complete disabled-candidate schema contract."""

    if connection is None or not callable(getattr(connection, "execute", None)):
        raise TypeError("connection must provide Connection.execute")
    literals = ", ".join(f"'{name}'" for name in _TABLES)
    table_rows = _mappings(
        connection.execute(
            text(
                "SELECT TABLE_NAME, ENGINE, TABLE_COLLATION, ROW_FORMAT "
                "FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() "
                f"AND TABLE_NAME IN ({literals}) ORDER BY TABLE_NAME"
            )
        )
    )
    observed_tables = {
        str(row.get("TABLE_NAME") or row.get("table_name") or ""): row
        for row in table_rows
    }
    if set(observed_tables) != set(_EXPECTED_COLUMNS):
        raise V3ProjectionOutboxSchemaError(
            "V3 projection outbox table inventory drifted"
        )
    for table_name, row in observed_tables.items():
        engine = str(row.get("ENGINE") or row.get("engine") or "").upper()
        collation = str(
            row.get("TABLE_COLLATION") or row.get("table_collation") or ""
        ).lower()
        row_format = str(
            row.get("ROW_FORMAT") or row.get("row_format") or ""
        ).upper()
        if (
            engine != "INNODB"
            or collation != _BIN
            or row_format != "DYNAMIC"
        ):
            raise V3ProjectionOutboxSchemaError(
                f"V3 projection outbox table contract drifted: {table_name}"
            )

    column_rows = _mappings(
        connection.execute(
            text(
                "SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, "
                "COLUMN_DEFAULT, EXTRA, COLLATION_NAME "
                "FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() "
                f"AND TABLE_NAME IN ({literals}) "
                "ORDER BY TABLE_NAME, ORDINAL_POSITION"
            )
        )
    )
    observed_columns: dict[str, dict[str, dict[str, Any]]] = {}
    for row in column_rows:
        table_name = str(
            row.get("TABLE_NAME") or row.get("table_name") or ""
        )
        column_name = str(
            row.get("COLUMN_NAME") or row.get("column_name") or ""
        )
        observed_columns.setdefault(table_name, {})[column_name] = {
            "type": _normalize_column_type(
                row.get("COLUMN_TYPE") or row.get("column_type") or ""
            ),
            "nullable": str(
                row.get("IS_NULLABLE") or row.get("is_nullable") or ""
            ).upper()
            == "YES",
            "default": _normalize_default(
                row.get("COLUMN_DEFAULT")
                if "COLUMN_DEFAULT" in row
                else row.get("column_default")
            ),
            "extra": " ".join(
                str(row.get("EXTRA") or row.get("extra") or "")
                .casefold()
                .split()
            ),
            "collation": str(
                row.get("COLLATION_NAME") or row.get("collation_name") or ""
            ).lower(),
        }
    for table_name, expected in _EXPECTED_COLUMNS.items():
        if observed_columns.get(table_name, {}) != expected:
            raise V3ProjectionOutboxSchemaError(
                f"V3 projection outbox column contract drifted: {table_name}"
            )

    rows = _mappings(
        connection.execute(
            text(
                "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, "
                "COLUMN_NAME, SUB_PART, INDEX_TYPE, COLLATION "
                "FROM information_schema.STATISTICS "
                "WHERE TABLE_SCHEMA = DATABASE() "
                f"AND TABLE_NAME IN ({literals}) "
                "ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX"
            )
        )
    )
    parts: dict[str, dict[str, dict[str, Any]]] = {}
    for row in rows:
        table_name = str(row.get("TABLE_NAME") or row.get("table_name") or "")
        index_name = str(row.get("INDEX_NAME") or row.get("index_name") or "")
        entry = parts.setdefault(table_name, {}).setdefault(
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
    if any(
        not all(value is None for value in details["sub_parts"])
        or set(details["index_types"]) != {"BTREE"}
        or set(details["collations"]) != {"A"}
        or len(set(details["unique_values"])) != 1
        or details["positions"]
        != list(range(1, len(details["columns"]) + 1))
        for indexes in parts.values()
        for details in indexes.values()
    ):
        raise V3ProjectionOutboxSchemaError(
            "V3 projection outbox index metadata drifted"
        )
    observed_indexes = {
        table_name: {
            name: {
                "unique": next(iter(set(details["unique_values"]))),
                "columns": tuple(details["columns"]),
            }
            for name, details in indexes.items()
        }
        for table_name, indexes in parts.items()
    }
    if observed_indexes != _EXPECTED_INDEXES:
        raise V3ProjectionOutboxSchemaError(
            "V3 projection outbox index contract drifted"
        )


__all__ = [
    "PRODUCTION_ACTIVATION_ALLOWED",
    "V3ProjectionOutboxSchemaError",
    "V3_PROJECTION_OUTBOX_DDL",
    "validate_v3_projection_outbox_schema",
]
