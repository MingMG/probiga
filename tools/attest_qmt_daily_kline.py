#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Attest existing raw A-share daily bars against row-level BigQMT history.

Only rows whose OHLC, volume and amount match within frozen tolerances and
whose native QMT ``preClose`` is present receive QMT provenance.  The native
reference price is copied to the target and bound in an append-only row-level
attestation so corporate-action discontinuities cannot be hidden by
``shift(close)``.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine, make_url

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.bigqmt.spool import PROVIDER_ID
from integrations.qmt.local_history import (
    get_local_history_engine,
    validate_local_history_provenance_schema,
)
from server.common.batch_db import (
    create_batch_engine,
    qualified_table_name,
)
from server.common.qmt_attestation_contract import (
    AMOUNT_REL_TOLERANCE,
    ATTESTATION_PROTOCOL_VERSION,
    EXPECTED_STOCK_SET_SCHEMA,
    PRICE_TOLERANCE,
    QMT_ATTESTATION_COLLATION,
    QMT_ATTESTATION_LEGACY_COLLATION,
    QMT_V2_TOLERANCE_VALUES,
    VOLUME_ABSOLUTE_TOLERANCE,
    VOLUME_REL_TOLERANCE,
    build_qmt_v2_manifest,
    bound_stock_set_contract,
    canonical_digest,
    daily_market_source_batch_id,
    expected_stock_set_contract,
    validated_universe_manifest,
)
from server.common.qmt_stock_catalog import (
    a_share_stock_code_sql,
    load_stock_catalog,
)
from server.common.qmt_trade_calendar import load_trade_calendar_receipt
from tools.env_config import load_project_env

_LOWER_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

ATTESTATION_TABLE_NAMES = (
    "qmt_kline_attestation_run",
    "qmt_kline_attestation_mismatch",
    "qmt_kline_attestation_row",
    "qmt_kline_attestation_schema_migration",
)
ATTESTATION_SCHEMA_MIGRATION_TABLE = (
    "qmt_kline_attestation_schema_migration"
)
TOLERANCE_MEDIUMTEXT_MIGRATION_KEY = (
    "20260822_001_qmt_attestation_tolerance_mediumtext"
)
COLLATION_MIGRATION_MARKER_PREFIX = (
    "20260823_001_qmt_attestation_unicode_ci:"
)
COLLATION_MIGRATION_PROTOCOL = (
    "probiga.qmt-attestation-collation-migration.v1"
)
LEGACY_MANIFEST_GRANDFATHER_MIGRATION_KEY = (
    "20260822_002_qmt_legacy_v1_ineligible_manifest_binding"
)
LEGACY_MANIFEST_BINDING_SCHEMA = (
    "probiga.qmt-legacy-ineligible-manifest-binding.v1"
)
LEGACY_MANIFEST_DISPOSITION = (
    "LEGACY_V1_INELIGIBLE_NO_UNIVERSE_MANIFEST"
)
EXPECTED_LEGACY_MANIFEST_GRANDFATHER_RUN_COUNT = 11
EXPECTED_LEGACY_MANIFEST_GRANDFATHER_PLAN_HASH = (
    "fc8328550615413445edf1055b3b88d70fa8b37e45ee331f682cf8f654779b54"
)
ATTESTATION_SESSION_CHUNK_SIZE = 10
# The deployed pre-V2 writer serialized this exact four-key object with
# ``json.dumps(..., sort_keys=True)``.  No alternate whitespace, key order,
# tolerance or additional field is grandfathered.
LEGACY_TOLERANCE_JSON = (
    '{"amount_relative": 0.001, "price_absolute": 0.0001, '
    '"volume_absolute": 100.0, "volume_relative": 0.0001}'
)
LEGACY_TOLERANCE_JSON_SHA256 = (
    "707a02cceef18d45ed7f689ae460a3abbf78079a3ebb37c761ecb17839007bde"
)
_TOLERANCE_MEDIUMTEXT_MIGRATION_PAYLOAD = {
    "schema": "probiga.qmt-attestation-schema-migration.v1",
    "migration_key": TOLERANCE_MEDIUMTEXT_MIGRATION_KEY,
    "table": "qmt_kline_attestation_run",
    "column": "tolerance_json",
    "from": "TEXT NOT NULL",
    "to": "MEDIUMTEXT NOT NULL",
}

# This contract is deliberately independent from CREATE TABLE IF NOT EXISTS.
# Setup may create a missing table, but it must never silently bless or mutate
# an older, drifted structure.  DATA_TYPE/length/precision are used instead of
# COLUMN_TYPE so the same contract works on Oracle MySQL 5.7 and 8.x (which
# differ in integer display-width metadata).
_ATTESTATION_COLUMN_CONTRACTS: dict[
    str,
    tuple[
        tuple[
            str,
            str,
            int | None,
            int | None,
            int | None,
            str,
            str | None,
            str,
        ],
        ...,
    ],
] = {
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
        (
            "tolerance_json",
            "mediumtext",
            16777215,
            None,
            None,
            "NO",
            None,
            "",
        ),
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

_ATTESTATION_INDEX_CONTRACTS: dict[
    str, dict[str, tuple[int, tuple[str, ...]]]
] = {
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
        "idx_qmt_kline_mismatch_lookup": (
            1,
            ("trade_date", "stock_code"),
        ),
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

ATTESTATION_TRIGGER_STATEMENTS = {
    "trg_qmt_kline_attestation_run_completed_bu": """
        CREATE TRIGGER trg_qmt_kline_attestation_run_completed_bu
        BEFORE UPDATE ON qmt_kline_attestation_run
        FOR EACH ROW
        BEGIN
            IF BINARY OLD.status = BINARY 'COMPLETED' THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'Completed QMT attestation run is immutable';
            END IF;
        END
    """,
    "trg_qmt_kline_attestation_run_completed_bd": """
        CREATE TRIGGER trg_qmt_kline_attestation_run_completed_bd
        BEFORE DELETE ON qmt_kline_attestation_run
        FOR EACH ROW
        BEGIN
            IF BINARY OLD.status = BINARY 'COMPLETED' THEN
                SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'Completed QMT attestation run cannot be deleted';
            END IF;
        END
    """,
    "trg_qmt_kline_attestation_row_immutable_bu": """
        CREATE TRIGGER trg_qmt_kline_attestation_row_immutable_bu
        BEFORE UPDATE ON qmt_kline_attestation_row
        FOR EACH ROW
        BEGIN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'QMT row attestation is append only';
        END
    """,
    "trg_qmt_kline_attestation_row_immutable_bd": """
        CREATE TRIGGER trg_qmt_kline_attestation_row_immutable_bd
        BEFORE DELETE ON qmt_kline_attestation_row
        FOR EACH ROW
        BEGIN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'QMT row attestation cannot be deleted';
        END
    """,
    "trg_qmt_attestation_schema_migration_immutable_bu": """
        CREATE TRIGGER trg_qmt_attestation_schema_migration_immutable_bu
        BEFORE UPDATE ON qmt_kline_attestation_schema_migration
        FOR EACH ROW
        BEGIN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'QMT schema migration marker is append only';
        END
    """,
    "trg_qmt_attestation_schema_migration_immutable_bd": """
        CREATE TRIGGER trg_qmt_attestation_schema_migration_immutable_bd
        BEFORE DELETE ON qmt_kline_attestation_schema_migration
        FOR EACH ROW
        BEGIN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'QMT schema migration marker cannot be deleted';
        END
    """,
}

_ATTESTATION_TRIGGER_CONTRACTS = {
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
        "BEGIN SIGNAL SQLSTATE '45000' "
        "SET MESSAGE_TEXT = 'QMT row attestation is append only'; END",
    ),
    "trg_qmt_kline_attestation_row_immutable_bd": (
        "BEFORE",
        "DELETE",
        "qmt_kline_attestation_row",
        "BEGIN SIGNAL SQLSTATE '45000' "
        "SET MESSAGE_TEXT = 'QMT row attestation cannot be deleted'; END",
    ),
    "trg_qmt_attestation_schema_migration_immutable_bu": (
        "BEFORE",
        "UPDATE",
        "qmt_kline_attestation_schema_migration",
        "BEGIN SIGNAL SQLSTATE '45000' "
        "SET MESSAGE_TEXT = 'QMT schema migration marker is append only'; END",
    ),
    "trg_qmt_attestation_schema_migration_immutable_bd": (
        "BEFORE",
        "DELETE",
        "qmt_kline_attestation_schema_migration",
        "BEGIN SIGNAL SQLSTATE '45000' "
        "SET MESSAGE_TEXT = 'QMT schema migration marker cannot be deleted'; END",
    ),
}

# Runtime attestation remains DDL-free.  These deterministic statements are
# consumed by the privileged production trigger broker; the scheduled writer
# never executes them directly.


class QmtAttestationSchemaError(RuntimeError):
    """Raised when the frozen QMT attestation schema has drifted."""

    def __init__(self, detail: dict[str, Any]):
        self.detail = detail
        errors = detail.get("errors") or ["unknown schema drift"]
        super().__init__(
            "QMT attestation frozen schema validation failed: "
            + "; ".join(str(error) for error in errors[:20])
        )


_DDL_STATEMENT_RE = re.compile(
    r"^\s*(?:/\*.*?\*/\s*)*(CREATE|ALTER|DROP)\b",
    re.IGNORECASE | re.DOTALL,
)
_SESSION_LOCAL_TEMPORARY_DDL_RE = re.compile(
    r"^\s*(?:/\*.*?\*/\s*)*(?:CREATE|DROP)\s+TEMPORARY\s+TABLE\b",
    re.IGNORECASE | re.DOTALL,
)


def assert_schema_prepared_statement_is_session_local(statement: Any) -> None:
    """Reject permanent or ALTER DDL in the production history path."""

    sql = str(statement)
    if _DDL_STATEMENT_RE.match(sql) and not (
        _SESSION_LOCAL_TEMPORARY_DDL_RE.match(sql)
    ):
        raise RuntimeError(
            "schema_prepared history path permits only CREATE/DROP "
            "TEMPORARY TABLE DDL"
        )


class _SessionLocalDdlConnection:
    def __init__(self, connection: Connection):
        self._connection = connection

    def execute(self, statement, *args, **kwargs):
        assert_schema_prepared_statement_is_session_local(statement)
        return self._connection.execute(statement, *args, **kwargs)

    def __getattr__(self, name: str):
        return getattr(self._connection, name)


@contextmanager
def _attestation_transaction(engine: Engine, *, schema_prepared: bool):
    with engine.begin() as connection:
        # Every execution path is runtime code.  ``schema_prepared`` remains
        # part of the public compatibility contract, but no value may relax
        # the session-local-only DDL guard.
        yield _SessionLocalDdlConnection(connection)


TOLERANCE_MEDIUMTEXT_MIGRATION_HASH = canonical_digest(
    _TOLERANCE_MEDIUMTEXT_MIGRATION_PAYLOAD
)


def _tolerance_json_column_contract(
    connection: Connection,
) -> tuple[str, int | None, str, str, str, str | None, str]:
    rows = connection.execute(
        text(
            "SELECT DATA_TYPE AS data_type, "
            "CHARACTER_MAXIMUM_LENGTH AS character_maximum_length, "
            "IS_NULLABLE AS is_nullable, "
            "CHARACTER_SET_NAME AS character_set_name, "
            "COLLATION_NAME AS collation_name, "
            "COLUMN_DEFAULT AS column_default, EXTRA AS extra "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() "
            "AND BINARY TABLE_NAME=BINARY "
            "'qmt_kline_attestation_run' "
            "AND BINARY COLUMN_NAME=BINARY 'tolerance_json'"
        )
    ).mappings().all()
    if len(rows) != 1:
        raise QmtAttestationSchemaError(
            {
                "protocol_version": ATTESTATION_PROTOCOL_VERSION,
                "errors": [
                    "qmt_kline_attestation_run.tolerance_json "
                    "column identity differs"
                ],
            }
        )
    row = rows[0]
    return (
        str(row.get("data_type") or "").lower(),
        (
            int(row["character_maximum_length"])
            if row.get("character_maximum_length") is not None
            else None
        ),
        str(row.get("is_nullable") or "").upper(),
        str(row.get("character_set_name") or "").lower(),
        str(row.get("collation_name") or "").lower(),
        _normalized_schema_default(row.get("column_default")),
        str(row.get("extra") or "").lower(),
    )


def _privileged_migrate_tolerance_json_mediumtext(
    connection: Connection,
) -> None:
    """Apply only the one frozen legacy TEXT -> MEDIUMTEXT upgrade."""

    marker_rows = connection.execute(
        text(
            "SELECT migration_hash FROM "
            "qmt_kline_attestation_schema_migration "
            "WHERE migration_key=:migration_key"
        ),
        {"migration_key": TOLERANCE_MEDIUMTEXT_MIGRATION_KEY},
    ).mappings().all()
    column_contract = _tolerance_json_column_contract(connection)
    legacy_contract = ("text", 65535, "NO", "utf8mb4")
    target_contract = ("mediumtext", 16777215, "NO", "utf8mb4")
    column_prefix = column_contract[:4]
    metadata_valid = bool(
        column_contract[4] == QMT_ATTESTATION_COLLATION
        and column_contract[5] is None
        and column_contract[6] == ""
    )
    if marker_rows:
        if (
            len(marker_rows) != 1
            or str(marker_rows[0].get("migration_hash") or "")
            != TOLERANCE_MEDIUMTEXT_MIGRATION_HASH
            or column_prefix != target_contract
            or not metadata_valid
        ):
            raise QmtAttestationSchemaError(
                {
                    "protocol_version": ATTESTATION_PROTOCOL_VERSION,
                    "errors": [
                        "tolerance_json MEDIUMTEXT migration marker or "
                        "post-migration contract differs"
                    ],
                }
            )
        return
    if not metadata_valid or column_prefix not in {
        legacy_contract,
        target_contract,
    }:
        raise QmtAttestationSchemaError(
            {
                "protocol_version": ATTESTATION_PROTOCOL_VERSION,
                "errors": [
                    "tolerance_json is neither the exact legacy TEXT "
                    "contract nor the frozen MEDIUMTEXT contract"
                ],
            }
        )
    if column_prefix == legacy_contract:
        connection.execute(
            text(
                "ALTER TABLE qmt_kline_attestation_run "
                "MODIFY COLUMN tolerance_json MEDIUMTEXT NOT NULL"
            )
        )
        migrated_contract = _tolerance_json_column_contract(connection)
        if not (
            migrated_contract[:4] == target_contract
            and migrated_contract[4] == QMT_ATTESTATION_COLLATION
            and migrated_contract[5] is None
            and migrated_contract[6] == ""
        ):
            raise QmtAttestationSchemaError(
                {
                    "protocol_version": ATTESTATION_PROTOCOL_VERSION,
                    "errors": [
                        "tolerance_json MEDIUMTEXT migration did not "
                        "produce the frozen contract"
                    ],
                }
            )
    connection.execute(
        text(
            "INSERT INTO qmt_kline_attestation_schema_migration "
            "(migration_key, migration_hash, completed_at) "
            "VALUES (:migration_key, :migration_hash, NOW())"
        ),
        {
            "migration_key": TOLERANCE_MEDIUMTEXT_MIGRATION_KEY,
            "migration_hash": TOLERANCE_MEDIUMTEXT_MIGRATION_HASH,
        },
    )


def _legacy_completed_run_binding(row: dict[str, Any]) -> dict[str, Any]:
    """Freeze the one known legacy COMPLETED row without upgrading evidence.

    The old protocol did not persist an immutable daily universe, so these
    rows can never become V2 funding evidence.  We only bind their exact
    historical identity and counters into an append-only migration marker.
    """

    run_id = str(row.get("run_id") or "").strip()
    start_date = str(row.get("start_date") or "")[:10]
    end_date = str(row.get("end_date") or "")[:10]
    try:
        parsed_start = datetime.strptime(start_date, "%Y-%m-%d").date()
        parsed_end = datetime.strptime(end_date, "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise ValueError("legacy completed run date is invalid") from exc
    if (
        not run_id
        or len(run_id) > 64
        or parsed_start.isoformat() != start_date
        or parsed_end.isoformat() != end_date
        or parsed_start > parsed_end
        or str(row.get("provider") or "") != PROVIDER_ID
    ):
        raise ValueError("legacy completed run identity differs")

    raw_tolerance = row.get("tolerance_json")
    if not isinstance(raw_tolerance, str):
        raise ValueError("legacy tolerance JSON is not the exact stored text")
    raw_hash = hashlib.sha256(raw_tolerance.encode("utf-8")).hexdigest()
    if (
        raw_tolerance != LEGACY_TOLERANCE_JSON
        or raw_hash != LEGACY_TOLERANCE_JSON_SHA256
    ):
        raise ValueError("legacy tolerance JSON contract differs")
    try:
        parsed_tolerance = json.loads(raw_tolerance)
    except json.JSONDecodeError as exc:  # pragma: no cover - hash is stronger
        raise ValueError("legacy tolerance JSON is invalid") from exc
    if parsed_tolerance != {
        "amount_relative": AMOUNT_REL_TOLERANCE,
        "price_absolute": PRICE_TOLERANCE,
        "volume_absolute": VOLUME_ABSOLUTE_TOLERANCE,
        "volume_relative": VOLUME_REL_TOLERANCE,
    }:
        raise ValueError("legacy tolerance values differ")

    counter_names = (
        "target_rows",
        "qmt_rows",
        "matched_rows",
        "missing_qmt_rows",
        "mismatched_rows",
        "already_attested_rows",
        "updated_rows",
    )
    try:
        counters = {
            name: int(row.get(name) or 0) for name in counter_names
        }
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("legacy completed run counters are invalid") from exc
    target_rows = counters["target_rows"]
    if (
        target_rows <= 0
        or counters["qmt_rows"] < target_rows
        or counters["matched_rows"] != target_rows
        or counters["missing_qmt_rows"] != 0
        or counters["mismatched_rows"] != 0
        or counters["already_attested_rows"] < 0
        or counters["already_attested_rows"] > target_rows
        or counters["updated_rows"] < 0
        or counters["updated_rows"] > target_rows
    ):
        raise ValueError("legacy completed run counters are not self-consistent")
    return {
        "run_id": run_id,
        "provider": PROVIDER_ID,
        "start_date": start_date,
        "end_date": end_date,
        **counters,
        "tolerance_json_sha256": raw_hash,
        "disposition": LEGACY_MANIFEST_DISPOSITION,
    }


def legacy_completed_run_binding_plan(
    rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> dict[str, Any]:
    bindings = sorted(
        (_legacy_completed_run_binding(dict(row)) for row in rows),
        key=lambda item: (
            item["start_date"],
            item["end_date"],
            item["run_id"],
        ),
    )
    run_ids = [str(item["run_id"]) for item in bindings]
    if len(run_ids) != len(set(run_ids)):
        raise ValueError("legacy completed run identity is duplicated")
    payload = {
        "schema": LEGACY_MANIFEST_BINDING_SCHEMA,
        "disposition": LEGACY_MANIFEST_DISPOSITION,
        "legacy_run_count": len(bindings),
        "runs": bindings,
    }
    return {
        **payload,
        "plan_hash": canonical_digest(payload),
    }


def validate_legacy_completed_run_release_contract(
    plan: dict[str, Any],
    *,
    expected_run_count: int,
    expected_plan_hash: str,
) -> None:
    if (
        type(expected_run_count) is not int
        or expected_run_count <= 0
        or not _LOWER_SHA256_RE.fullmatch(str(expected_plan_hash or ""))
    ):
        raise ValueError("legacy release expectation is invalid")
    if (
        int(plan.get("legacy_run_count") or 0) != expected_run_count
        or str(plan.get("plan_hash") or "") != expected_plan_hash
    ):
        raise ValueError("legacy completed run release contract differs")


def values_match(
    target: dict[str, Any],
    source: dict[str, Any],
    *,
    price_tolerance: float = PRICE_TOLERANCE,
    volume_rel_tolerance: float = VOLUME_REL_TOLERANCE,
    amount_rel_tolerance: float = AMOUNT_REL_TOLERANCE,
) -> bool:
    def close_enough(left: Any, right: Any, absolute: float, relative: float = 0.0) -> bool:
        if left is None or right is None:
            return False
        left_value = float(left)
        right_value = float(right)
        tolerance = max(absolute, abs(right_value) * relative)
        return math.isfinite(left_value) and math.isfinite(right_value) and abs(left_value - right_value) <= tolerance

    prices_are_positive = all(
        target.get(field) is not None
        and source.get(field) is not None
        and float(target[field]) > 0
        and float(source[field]) > 0
        for field in ("open", "close", "high", "low")
    )
    non_price_values_are_nonnegative = bool(
        target.get("volume") is not None
        and source.get("volume") is not None
        and target.get("amount") is not None
        and source.get("amount") is not None
        and float(target["volume"]) >= 0
        and float(source["volume"]) >= 0
        and float(target["amount"]) >= 0
        and float(source["amount"]) >= 0
    )
    base_matches = prices_are_positive and non_price_values_are_nonnegative and all(
        close_enough(target.get(field), source.get(field), price_tolerance)
        for field in ("open", "close", "high", "low")
    ) and close_enough(
        target.get("volume"),
        source.get("volume"),
        VOLUME_ABSOLUTE_TOLERANCE,
        volume_rel_tolerance,
    ) and close_enough(
        target.get("amount"),
        source.get("amount"),
        1.0,
        amount_rel_tolerance,
    )
    return bool(
        base_matches
        and str(source.get("pre_close_origin") or "") == "NATIVE_QMT"
        and float(source.get("pre_close") or 0) > 0
    )


def _table_names(
    engine: Engine,
    *,
    local_history_engine: Engine | None = None,
) -> tuple[str, str]:
    target_url = make_url(str(engine.url))
    local_engine = local_history_engine or get_local_history_engine()
    local_url = make_url(str(local_engine.url))
    target_host = (target_url.host or "localhost").lower()
    local_host = (local_url.host or "localhost").lower()
    localhost_aliases = {"localhost", "127.0.0.1"}
    hosts_match = target_host == local_host or {
        target_host,
        local_host,
    }.issubset(localhost_aliases)
    if (
        not hosts_match
        or int(target_url.port or 3306) != int(local_url.port or 3306)
        or str(target_url.username or "") != str(local_url.username or "")
    ):
        raise RuntimeError(
            "QMT attestation must run on the Windows local MySQL boundary; "
            "target and QMT history schemas are not on the same server"
        )
    if not target_url.database or not local_url.database:
        raise RuntimeError("target and QMT history database names are required")
    # This SELECT-only contract check must run before attest_range inserts its
    # RUNNING ledger row.  A legacy source table is never interpreted through
    # a synthetic NATIVE_QMT fallback.
    validate_local_history_provenance_schema(
        engine,
        database=local_url.database,
    )
    return (
        qualified_table_name(target_url.database, "sm_stock_kline"),
        qualified_table_name(local_url.database, "qmt_local_stock_kline"),
    )


def _normalized_trigger_body(value: Any) -> str:
    normalized = str(value or "").replace("`", "")
    normalized = re.sub(
        r"\bSQLSTATE\s+VALUE\b",
        "SQLSTATE",
        normalized,
        flags=re.IGNORECASE,
    )
    normalized = re.sub(r"\s+", " ", normalized).strip().lower()
    normalized = re.sub(r"\s*=\s*", "=", normalized)
    normalized = re.sub(r"\s*;\s*", ";", normalized)
    return normalized


def _missing_attestation_trigger_statements(
    connection: Connection,
) -> tuple[str, ...]:
    """Compatibility shim: QMT schema setup no longer manages triggers."""

    del connection
    return ()


_ATTESTATION_ROW_ORDER = {
    "qmt_kline_attestation_run": "BINARY run_id",
    "qmt_kline_attestation_mismatch": "id",
    "qmt_kline_attestation_row": "BINARY attestation_id",
    "qmt_kline_attestation_schema_migration": "BINARY migration_key",
}


def _collation_proof_value(value: Any) -> Any:
    if value is None or type(value) in {bool, int, float, str}:
        return value
    if isinstance(value, Decimal):
        return {"decimal": format(value, "f")}
    if isinstance(value, bytes):
        return {"bytes_hex": value.hex()}
    if hasattr(value, "isoformat"):
        return {"iso8601": value.isoformat()}
    raise TypeError(f"unsupported QMT row-proof value: {type(value).__name__}")


def _attestation_table_row_proof(
    connection: Connection,
    table_name: str,
    *,
    exclude_migration_keys: tuple[str, ...] = (),
) -> dict[str, Any]:
    if table_name not in ATTESTATION_TABLE_NAMES:
        raise ValueError("unknown QMT attestation table")
    if exclude_migration_keys and (
        table_name != ATTESTATION_SCHEMA_MIGRATION_TABLE
        or len(set(exclude_migration_keys)) != len(exclude_migration_keys)
        or any(not key for key in exclude_migration_keys)
    ):
        raise ValueError("migration-key exclusion is only valid for the marker table")
    columns = tuple(
        contract[0] for contract in _ATTESTATION_COLUMN_CONTRACTS[table_name]
    )
    select_columns = ",".join(f"`{column}`" for column in columns)
    where = ""
    params: dict[str, Any] = {}
    if exclude_migration_keys:
        predicates = []
        for index, migration_key in enumerate(exclude_migration_keys):
            parameter = f"excluded_migration_key_{index}"
            predicates.append(f"BINARY migration_key<>BINARY :{parameter}")
            params[parameter] = migration_key
        where = " WHERE " + " AND ".join(predicates)
    statement = text(
        f"SELECT {select_columns} FROM `{table_name}`{where} "
        f"ORDER BY {_ATTESTATION_ROW_ORDER[table_name]}"
    )
    stream = connection.execution_options(
        stream_results=True, max_row_buffer=512
    ).execute(statement, params).mappings()
    hasher = hashlib.sha256()
    hasher.update(b"probiga.qmt-attestation-row-proof.v1\x00")
    hasher.update(table_name.encode("ascii") + b"\x00")
    count = 0
    try:
        while True:
            rows = stream.fetchmany(512)
            if not rows:
                break
            for row in rows:
                if set(row) != set(columns):
                    raise RuntimeError("QMT row-proof columns differ")
                encoded = json.dumps(
                    [
                        [column, _collation_proof_value(row[column])]
                        for column in columns
                    ],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
                hasher.update(len(encoded).to_bytes(8, "big"))
                hasher.update(encoded)
                count += 1
    finally:
        close = getattr(stream, "close", None)
        if callable(close):
            close()
    return {"row_count": count, "row_sha256": hasher.hexdigest()}


def _collation_marker_contract(
    table_name: str,
) -> tuple[str, str, str]:
    pending_key = COLLATION_MIGRATION_MARKER_PREFIX + "pending:" + table_name
    complete_key = COLLATION_MIGRATION_MARKER_PREFIX + "complete:" + table_name
    if max(len(pending_key), len(complete_key)) > 100:
        raise AssertionError("QMT collation marker key is too long")
    contract_hash = canonical_digest(
        {
            "schema": COLLATION_MIGRATION_PROTOCOL,
            "table": table_name,
            "from_collation": QMT_ATTESTATION_LEGACY_COLLATION,
            "to_collation": QMT_ATTESTATION_COLLATION,
            "ddl": (
                f"ALTER TABLE `{table_name}` CONVERT TO CHARACTER SET utf8mb4 "
                f"COLLATE {QMT_ATTESTATION_COLLATION}"
            ),
        }
    )
    return pending_key, complete_key, contract_hash


def _collation_pending_hash(
    table_name: str,
    contract_hash: str,
    proof: dict[str, Any],
) -> str:
    return canonical_digest(
        {
            "schema": COLLATION_MIGRATION_PROTOCOL + ".pending-proof.v1",
            "table": table_name,
            "contract_hash": contract_hash,
            **proof,
        }
    )


def _collation_complete_hash(
    table_name: str,
    contract_hash: str,
    pending_hash: str,
) -> str:
    return canonical_digest(
        {
            "schema": COLLATION_MIGRATION_PROTOCOL + ".complete-proof.v1",
            "table": table_name,
            "contract_hash": contract_hash,
            "pending_hash": pending_hash,
        }
    )


def _read_collation_marker(
    connection: Connection,
    migration_key: str,
) -> str | None:
    rows = connection.execute(
        text(
            "SELECT migration_hash FROM "
            "qmt_kline_attestation_schema_migration "
            "WHERE migration_key=:migration_key"
        ),
        {"migration_key": migration_key},
    ).mappings().all()
    if len(rows) > 1:
        raise QmtAttestationSchemaError(
            {"errors": [f"duplicate QMT migration marker: {migration_key}"]}
        )
    if not rows:
        return None
    marker_hash = str(rows[0].get("migration_hash") or "")
    if not _LOWER_SHA256_RE.fullmatch(marker_hash):
        raise QmtAttestationSchemaError(
            {"errors": [f"invalid QMT migration marker: {migration_key}"]}
        )
    return marker_hash


def _insert_collation_marker(
    connection: Connection,
    migration_key: str,
    migration_hash: str,
) -> None:
    connection.execute(
        text(
            "INSERT INTO qmt_kline_attestation_schema_migration "
            "(migration_key, migration_hash, completed_at) "
            "VALUES (:migration_key, :migration_hash, NOW())"
        ),
        {"migration_key": migration_key, "migration_hash": migration_hash},
    )


def privileged_migrate_legacy_attestation_collation(
    engine: Engine,
    *,
    writers_fenced: bool,
) -> dict[str, Any]:
    """Convert only an exact legacy QMT schema under an explicit writer fence."""

    if writers_fenced is not True:
        raise ValueError("QMT collation migration requires writers_fenced=True")
    operations: list[dict[str, Any]] = []
    with engine.connect() as connection:
        _validate_attestation_schema_connection(
            connection,
            allow_legacy_collation=True,
            require_current_manifests=False,
        )
    for table_name in ATTESTATION_TABLE_NAMES:
        pending_key, complete_key, contract_hash = (
            _collation_marker_contract(table_name)
        )
        proof_exclusions = (
            (pending_key, complete_key)
            if table_name == ATTESTATION_SCHEMA_MIGRATION_TABLE
            else ()
        )
        with engine.begin() as connection:
            detail = _validate_attestation_schema_connection(
                connection,
                allow_legacy_collation=True,
                require_current_manifests=False,
            )
            observed = str(detail["table_collations"].get(table_name) or "")
            proof = _attestation_table_row_proof(
                connection,
                table_name,
                exclude_migration_keys=proof_exclusions,
            )
            pending_hash = _collation_pending_hash(
                table_name,
                contract_hash,
                proof,
            )
            observed_pending = _read_collation_marker(connection, pending_key)
            observed_complete = _read_collation_marker(connection, complete_key)
            if observed_complete is not None:
                if observed_pending is None or observed_complete != (
                    _collation_complete_hash(
                        table_name,
                        contract_hash,
                        observed_pending,
                    )
                ):
                    raise QmtAttestationSchemaError(
                        {"errors": [f"{table_name} completion marker differs"]}
                    )
            if observed == QMT_ATTESTATION_COLLATION:
                if observed_pending is None:
                    raise QmtAttestationSchemaError(
                        {
                            "errors": [
                                f"{table_name} target collation lacks migration proof"
                            ]
                        }
                    )
                action = "already_target"
                if observed_complete is None:
                    if observed_pending != pending_hash:
                        raise QmtAttestationSchemaError(
                            {"errors": [f"{table_name} pending row proof differs"]}
                        )
                    _insert_collation_marker(
                        connection,
                        complete_key,
                        _collation_complete_hash(
                            table_name,
                            contract_hash,
                            pending_hash,
                        ),
                    )
                    action = "finalized_after_interrupt"
                operations.append(
                    {"table_name": table_name, "action": action, **proof}
                )
                continue
            if observed != QMT_ATTESTATION_LEGACY_COLLATION:
                raise QmtAttestationSchemaError(
                    {"errors": [f"{table_name} legacy collation differs"]}
                )
            if observed_complete is not None:
                raise QmtAttestationSchemaError(
                    {
                        "errors": [
                            f"{table_name} completion marker precedes conversion"
                        ]
                    }
                )
            if observed_pending is not None and observed_pending != pending_hash:
                raise QmtAttestationSchemaError(
                    {"errors": [f"{table_name} pending row proof differs"]}
                )
            if observed_pending is None:
                _insert_collation_marker(
                    connection,
                    pending_key,
                    pending_hash,
                )
        # Each stage has its own committed transaction.  MySQL DDL commits
        # implicitly; a process interruption therefore leaves a durable
        # pending proof that the next fenced deployment can verify and resume.
        with engine.begin() as connection:
            detail = _validate_attestation_schema_connection(
                connection,
                allow_legacy_collation=True,
                require_current_manifests=False,
            )
            if detail["table_collations"].get(table_name) != (
                QMT_ATTESTATION_LEGACY_COLLATION
            ):
                raise QmtAttestationSchemaError(
                    {"errors": [f"{table_name} source collation changed"]}
                )
            current_proof = _attestation_table_row_proof(
                connection,
                table_name,
                exclude_migration_keys=proof_exclusions,
            )
            if current_proof != proof or (
                _read_collation_marker(connection, pending_key) != pending_hash
            ):
                raise QmtAttestationSchemaError(
                    {"errors": [f"{table_name} pending row proof changed"]}
                )
            if _read_collation_marker(connection, complete_key) is not None:
                raise QmtAttestationSchemaError(
                    {"errors": [f"{table_name} completed before conversion"]}
                )
            connection.execute(
                text(
                    f"ALTER TABLE `{table_name}` CONVERT TO CHARACTER SET "
                    f"utf8mb4 COLLATE {QMT_ATTESTATION_COLLATION}"
                )
            )
        with engine.begin() as connection:
            detail = _validate_attestation_schema_connection(
                connection,
                allow_legacy_collation=True,
                require_current_manifests=False,
            )
            if detail["table_collations"].get(table_name) != (
                QMT_ATTESTATION_COLLATION
            ):
                raise QmtAttestationSchemaError(
                    {"errors": [f"{table_name} collation conversion failed"]}
                )
            post_proof = _attestation_table_row_proof(
                connection,
                table_name,
                exclude_migration_keys=proof_exclusions,
            )
            if post_proof != proof:
                raise QmtAttestationSchemaError(
                    {"errors": [f"{table_name} row proof changed during conversion"]}
                )
            if _read_collation_marker(connection, pending_key) != pending_hash:
                raise QmtAttestationSchemaError(
                    {"errors": [f"{table_name} pending marker changed"]}
                )
            observed_complete = _read_collation_marker(connection, complete_key)
            if observed_complete is None:
                _insert_collation_marker(
                    connection,
                    complete_key,
                    _collation_complete_hash(
                        table_name,
                        contract_hash,
                        pending_hash,
                    ),
                )
            elif observed_complete != _collation_complete_hash(
                table_name,
                contract_hash,
                pending_hash,
            ):
                raise QmtAttestationSchemaError(
                    {"errors": [f"{table_name} completion marker differs"]}
                )
            operations.append(
                {"table_name": table_name, "action": "converted", **proof}
            )
    with engine.connect() as connection:
        final_schema = _validate_attestation_schema_connection(
            connection,
            require_current_manifests=False,
        )
    return {
        "status": "ok",
        "writers_fenced": True,
        "source_collation": QMT_ATTESTATION_LEGACY_COLLATION,
        "target_collation": QMT_ATTESTATION_COLLATION,
        "operations": operations,
        "schema": final_schema,
        "database_triggers_required": False,
    }


def migrate_legacy_attestation_collation(
    engine: Engine,
    *,
    writers_fenced: bool,
) -> dict[str, Any]:
    """Compatibility alias for the explicit privileged migration."""

    return privileged_migrate_legacy_attestation_collation(
        engine,
        writers_fenced=writers_fenced,
    )


def attestation_table_ddl_statements() -> tuple[str, ...]:
    """Return the frozen persistent table DDL for the privileged release."""

    return (
        """
        CREATE TABLE IF NOT EXISTS qmt_kline_attestation_run (
            run_id VARCHAR(64) PRIMARY KEY,
            provider VARCHAR(32) NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            status VARCHAR(40) NOT NULL,
            target_rows BIGINT NOT NULL DEFAULT 0,
            qmt_rows BIGINT NOT NULL DEFAULT 0,
            matched_rows BIGINT NOT NULL DEFAULT 0,
            missing_qmt_rows BIGINT NOT NULL DEFAULT 0,
            mismatched_rows BIGINT NOT NULL DEFAULT 0,
            already_attested_rows BIGINT NOT NULL DEFAULT 0,
            updated_rows BIGINT NOT NULL DEFAULT 0,
            tolerance_json MEDIUMTEXT NOT NULL,
            started_at DATETIME NOT NULL,
            finished_at DATETIME NULL,
            error_message TEXT NULL,
            KEY idx_qmt_kline_attestation_range (start_date, end_date, status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
          COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS qmt_kline_attestation_mismatch (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            run_id VARCHAR(64) NOT NULL,
            trade_date DATE NOT NULL,
            stock_code VARCHAR(16) NOT NULL,
            reason VARCHAR(40) NOT NULL,
            target_close DECIMAL(20,6) NULL,
            qmt_close DECIMAL(20,6) NULL,
            target_volume DECIMAL(24,6) NULL,
            qmt_volume DECIMAL(24,6) NULL,
            target_amount DECIMAL(24,6) NULL,
            qmt_amount DECIMAL(24,6) NULL,
            created_at DATETIME NOT NULL,
            UNIQUE KEY uk_qmt_kline_attestation_mismatch
                (run_id, trade_date, stock_code),
            KEY idx_qmt_kline_mismatch_lookup (trade_date, stock_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
          COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS qmt_kline_attestation_row (
            attestation_id CHAR(64) NOT NULL PRIMARY KEY,
            run_id VARCHAR(64) NOT NULL,
            target_id BIGINT NOT NULL,
            qmt_id BIGINT NOT NULL,
            trade_date DATE NOT NULL,
            stock_code VARCHAR(16) NOT NULL,
            protocol_version VARCHAR(64) NOT NULL,
            source_data_version VARCHAR(64) NOT NULL,
            source_pre_close_origin VARCHAR(32) NOT NULL,
            source_pre_close DECIMAL(20,6) NOT NULL,
            attested_open DECIMAL(20,6) NOT NULL,
            attested_close DECIMAL(20,6) NOT NULL,
            attested_high DECIMAL(20,6) NOT NULL,
            attested_low DECIMAL(20,6) NOT NULL,
            attested_volume DECIMAL(24,6) NOT NULL,
            attested_amount DECIMAL(24,6) NOT NULL,
            created_at DATETIME NOT NULL,
            UNIQUE KEY uk_qmt_kline_attestation_row_source
                (target_id, protocol_version, source_data_version),
            KEY idx_qmt_kline_attestation_row_date
                (trade_date, protocol_version, stock_code),
            KEY idx_qmt_kline_attestation_row_run (run_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
          COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS qmt_kline_attestation_schema_migration (
            migration_key VARCHAR(100) NOT NULL PRIMARY KEY,
            migration_hash CHAR(64) NOT NULL,
            completed_at DATETIME NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
          COLLATE=utf8mb4_unicode_ci
        """,
    )


def privileged_migrate_attestation_tables(
    engine: Engine,
    *,
    trigger_ddl_executor: Callable[[str], None] | None = None,
    allow_legacy_manifest_candidates: bool = False,
) -> dict[str, Any]:
    """Install the attestation table contract in a fenced release window.

    Attestation triggers remain owned by the production allow-listed broker;
    the compatibility callback is validated but deliberately not invoked.
    """

    if trigger_ddl_executor is not None and not callable(
        trigger_ddl_executor
    ):
        raise TypeError("trigger_ddl_executor must be callable")
    if type(allow_legacy_manifest_candidates) is not bool:
        raise TypeError("allow_legacy_manifest_candidates must be bool")
    with engine.begin() as connection:
        for statement in attestation_table_ddl_statements():
            connection.execute(text(statement))
        _privileged_migrate_tolerance_json_mediumtext(connection)
    detail = validate_attestation_schema(
        engine,
        require_current_manifests=not allow_legacy_manifest_candidates,
    )
    return {
        **detail,
        "privileged_migration": True,
        "runtime_ddl_required": False,
    }


def ensure_attestation_tables(
    engine: Engine,
    *,
    trigger_ddl_executor: Callable[[str], None] | None = None,
    allow_legacy_manifest_candidates: bool = False,
) -> dict[str, Any]:
    """Backward-compatible, read-only runtime schema validation alias."""

    if trigger_ddl_executor is not None and not callable(
        trigger_ddl_executor
    ):
        raise TypeError("trigger_ddl_executor must be callable")
    if type(allow_legacy_manifest_candidates) is not bool:
        raise TypeError("allow_legacy_manifest_candidates must be bool")
    return validate_attestation_schema(
        engine,
        require_current_manifests=not allow_legacy_manifest_candidates,
    )


def _normalized_schema_default(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="strict")
    return str(value)


def _validate_attestation_schema_connection(
    connection: Connection,
    *,
    require_triggers: bool = True,
    require_current_manifests: bool = True,
    allow_legacy_collation: bool = False,
    expected_legacy_run_count: int = (
        EXPECTED_LEGACY_MANIFEST_GRANDFATHER_RUN_COUNT
    ),
    expected_legacy_plan_hash: str = (
        EXPECTED_LEGACY_MANIFEST_GRANDFATHER_PLAN_HASH
    ),
) -> dict[str, Any]:
    if type(allow_legacy_collation) is not bool:
        raise TypeError("allow_legacy_collation must be bool")
    errors: list[str] = []
    migration_rows = connection.execute(
        text(
            "SELECT migration_hash FROM "
            "qmt_kline_attestation_schema_migration "
            "WHERE migration_key=:migration_key"
        ),
        {"migration_key": TOLERANCE_MEDIUMTEXT_MIGRATION_KEY},
    ).mappings().all()
    if (
        len(migration_rows) != 1
        or str(migration_rows[0].get("migration_hash") or "")
        != TOLERANCE_MEDIUMTEXT_MIGRATION_HASH
    ):
        errors.append(
            "tolerance_json MEDIUMTEXT migration marker/hash differs"
        )
    table_names_sql = ", ".join(
        f"'{name}'" for name in ATTESTATION_TABLE_NAMES
    )
    table_rows = connection.execute(
        text(
            "SELECT TABLE_NAME AS table_name, ENGINE AS engine, "
            "TABLE_COLLATION AS table_collation "
            "FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA=DATABASE() "
            f"AND TABLE_NAME IN ({table_names_sql}) "
            "ORDER BY BINARY TABLE_NAME"
        )
    ).mappings().all()
    observed_tables = {
        str(row.get("table_name") or ""): dict(row) for row in table_rows
    }
    allowed_collations = {QMT_ATTESTATION_COLLATION}
    if allow_legacy_collation:
        allowed_collations.add(QMT_ATTESTATION_LEGACY_COLLATION)
    observed_table_collations: dict[str, str] = {}
    if set(observed_tables) != set(ATTESTATION_TABLE_NAMES):
        errors.append(
            "table inventory differs: expected="
            f"{sorted(ATTESTATION_TABLE_NAMES)!r}, "
            f"observed={sorted(observed_tables)!r}"
        )
    for table_name in ATTESTATION_TABLE_NAMES:
        row = observed_tables.get(table_name, {})
        if str(row.get("engine") or "").lower() != "innodb":
            errors.append(f"{table_name} engine is not InnoDB")
        collation = str(row.get("table_collation") or "").lower()
        observed_table_collations[table_name] = collation
        if collation not in allowed_collations:
            errors.append(f"{table_name} table collation differs")

    column_rows = connection.execute(
        text(
            "SELECT TABLE_NAME AS table_name, "
            "COLUMN_NAME AS column_name, "
            "ORDINAL_POSITION AS ordinal_position, "
            "DATA_TYPE AS data_type, "
            "CHARACTER_MAXIMUM_LENGTH AS character_maximum_length, "
            "NUMERIC_PRECISION AS numeric_precision, "
            "NUMERIC_SCALE AS numeric_scale, "
            "IS_NULLABLE AS is_nullable, "
            "COLUMN_DEFAULT AS column_default, EXTRA AS extra, "
            "CHARACTER_SET_NAME AS character_set_name, "
            "COLLATION_NAME AS collation_name "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() "
            f"AND TABLE_NAME IN ({table_names_sql}) "
            "ORDER BY BINARY TABLE_NAME, ORDINAL_POSITION"
        )
    ).mappings().all()
    observed_columns: dict[str, list[tuple[Any, ...]]] = {
        name: [] for name in ATTESTATION_TABLE_NAMES
    }
    for row in column_rows:
        table_name = str(row.get("table_name") or "")
        if table_name not in observed_columns:
            errors.append(f"unexpected column table: {table_name}")
            continue
        data_type = str(row.get("data_type") or "").lower()
        character_type = data_type in {
            "char",
            "varchar",
            "text",
            "mediumtext",
        }
        character_set = str(row.get("character_set_name") or "").lower()
        column_collation = str(row.get("collation_name") or "").lower()
        expected_column_collation = observed_table_collations.get(
            table_name, QMT_ATTESTATION_COLLATION
        )
        if character_type and (
            character_set != "utf8mb4"
            or column_collation != expected_column_collation
        ):
            errors.append(
                f"{table_name}.{row.get('column_name')} "
                "character set/collation differs"
            )
        if not character_type and (
            row.get("character_set_name") is not None
            or row.get("collation_name") is not None
        ):
            errors.append(
                f"{table_name}.{row.get('column_name')} "
                "unexpected character metadata"
            )
        observed_columns[table_name].append(
            (
                str(row.get("column_name") or ""),
                data_type,
                (
                    int(row["character_maximum_length"])
                    if row.get("character_maximum_length") is not None
                    else None
                ),
                (
                    int(row["numeric_precision"])
                    if row.get("numeric_precision") is not None
                    else None
                ),
                (
                    int(row["numeric_scale"])
                    if row.get("numeric_scale") is not None
                    else None
                ),
                str(row.get("is_nullable") or "").upper(),
                _normalized_schema_default(row.get("column_default")),
                str(row.get("extra") or "").lower(),
            )
        )
    for table_name, expected in _ATTESTATION_COLUMN_CONTRACTS.items():
        observed = tuple(observed_columns.get(table_name, []))
        if observed != expected:
            errors.append(
                f"{table_name} column contract differs: "
                f"expected={expected!r}, observed={observed!r}"
            )

    index_rows = connection.execute(
        text(
            "SELECT TABLE_NAME AS table_name, INDEX_NAME AS index_name, "
            "NON_UNIQUE AS non_unique, SEQ_IN_INDEX AS seq_in_index, "
            "COLUMN_NAME AS column_name, SUB_PART AS sub_part, "
            "INDEX_TYPE AS index_type "
            "FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA=DATABASE() "
            f"AND TABLE_NAME IN ({table_names_sql}) "
            "ORDER BY BINARY TABLE_NAME, BINARY INDEX_NAME, SEQ_IN_INDEX"
        )
    ).mappings().all()
    observed_indexes: dict[str, dict[str, dict[str, Any]]] = {
        name: {} for name in ATTESTATION_TABLE_NAMES
    }
    for row in index_rows:
        table_name = str(row.get("table_name") or "")
        index_name = str(row.get("index_name") or "")
        if table_name not in observed_indexes:
            errors.append(f"unexpected index table: {table_name}")
            continue
        entry = observed_indexes[table_name].setdefault(
            index_name,
            {
                "non_unique": int(row.get("non_unique") or 0),
                "columns": [],
                "valid": True,
            },
        )
        entry["valid"] = bool(
            entry["valid"]
            and int(row.get("non_unique") or 0) == entry["non_unique"]
            and row.get("sub_part") is None
            and str(row.get("index_type") or "").upper() == "BTREE"
            and int(row.get("seq_in_index") or 0)
            == len(entry["columns"]) + 1
        )
        entry["columns"].append(str(row.get("column_name") or ""))
    normalized_indexes = {
        table_name: {
            index_name: (
                int(entry["non_unique"]),
                tuple(entry["columns"]),
            )
            for index_name, entry in table_indexes.items()
            if entry["valid"]
        }
        for table_name, table_indexes in observed_indexes.items()
    }
    for table_name, expected in _ATTESTATION_INDEX_CONTRACTS.items():
        observed = normalized_indexes.get(table_name, {})
        all_valid = all(
            entry["valid"]
            for entry in observed_indexes.get(table_name, {}).values()
        )
        if not all_valid or observed != expected:
            errors.append(
                f"{table_name} index contract differs: "
                f"expected={expected!r}, observed={observed!r}"
            )

    completed_run_rows = connection.execute(
        text(
            "SELECT run_id, provider, start_date, end_date, "
            "target_rows, qmt_rows, matched_rows, missing_qmt_rows, "
            "mismatched_rows, already_attested_rows, updated_rows, "
            "tolerance_json "
            "FROM qmt_kline_attestation_run "
            "WHERE status='COMPLETED' ORDER BY start_date, end_date, run_id"
        )
    ).mappings().all()
    completed_manifest_entry_count = 0
    current_manifest_run_count = 0
    legacy_rows: list[dict[str, Any]] = []
    for row in completed_run_rows:
        run_id = str(row.get("run_id") or "")
        start_date = str(row.get("start_date") or "")[:10]
        end_date = str(row.get("end_date") or "")[:10]
        try:
            manifest = validated_universe_manifest(
                row.get("tolerance_json"),
                start_date=start_date,
                end_date=end_date,
            )
            completed_manifest_entry_count += len(manifest)
            current_manifest_run_count += 1
        except Exception as exc:
            try:
                _legacy_completed_run_binding(dict(row))
                legacy_rows.append(dict(row))
            except Exception as legacy_exc:
                errors.append(
                    f"completed run universe manifest invalid: "
                    f"{run_id or '<missing-run-id>'}: "
                    f"{type(exc).__name__}: {exc}; legacy contract: "
                    f"{type(legacy_exc).__name__}: {legacy_exc}"
                )

    legacy_plan: dict[str, Any] | None = None
    try:
        legacy_plan = legacy_completed_run_binding_plan(legacy_rows)
        if legacy_rows:
            validate_legacy_completed_run_release_contract(
                legacy_plan,
                expected_run_count=expected_legacy_run_count,
                expected_plan_hash=expected_legacy_plan_hash,
            )
    except Exception as exc:
        errors.append(
            "legacy completed run binding plan invalid: "
            f"{type(exc).__name__}: {exc}"
        )
    legacy_marker_rows = connection.execute(
        text(
            "SELECT migration_hash FROM "
            "qmt_kline_attestation_schema_migration "
            "WHERE migration_key=:migration_key"
        ),
        {"migration_key": LEGACY_MANIFEST_GRANDFATHER_MIGRATION_KEY},
    ).mappings().all()
    legacy_marker_verified = False
    expected_legacy_hash = str(
        (legacy_plan or {}).get("plan_hash") or ""
    )
    if legacy_rows:
        if not legacy_marker_rows:
            if require_current_manifests:
                errors.append(
                    "legacy completed runs lack their ineligible binding marker"
                )
        elif (
            len(legacy_marker_rows) != 1
            or str(legacy_marker_rows[0].get("migration_hash") or "")
            != expected_legacy_hash
        ):
            errors.append(
                "legacy completed run binding marker/hash differs"
            )
        else:
            legacy_marker_verified = True
    elif legacy_marker_rows:
        errors.append(
            "legacy completed run binding marker exists without bound rows"
        )

    detail = {
        "protocol_version": ATTESTATION_PROTOCOL_VERSION,
        "table_names": sorted(observed_tables),
        "table_count": len(observed_tables),
        "column_count": sum(len(rows) for rows in observed_columns.values()),
        "index_names": {
            name: sorted(indexes) for name, indexes in observed_indexes.items()
        },
        "trigger_names": [],
        "trigger_count": 0,
        "database_triggers_required": False,
        "immutability_enforcement": (
            "application_transaction_unique_identity_and_evidence_hash"
        ),
        "table_collations": observed_table_collations,
        "completed_run_count": len(completed_run_rows),
        "completed_current_manifest_run_count": current_manifest_run_count,
        "completed_manifest_entry_count": completed_manifest_entry_count,
        "legacy_ineligible_run_count": len(legacy_rows),
        "legacy_binding_plan_hash": expected_legacy_hash,
        "legacy_binding_marker_verified": legacy_marker_verified,
        "legacy_binding_pending": bool(
            legacy_rows and not legacy_marker_rows
        ),
        "errors": errors,
    }
    if errors:
        raise QmtAttestationSchemaError(detail)
    return detail


def validate_attestation_schema(
    bind: Engine | Connection,
    *,
    require_triggers: bool = True,
    require_current_manifests: bool = True,
    expected_legacy_run_count: int = (
        EXPECTED_LEGACY_MANIFEST_GRANDFATHER_RUN_COUNT
    ),
    expected_legacy_plan_hash: str = (
        EXPECTED_LEGACY_MANIFEST_GRANDFATHER_PLAN_HASH
    ),
) -> dict[str, Any]:
    """Fail closed unless the complete V2 table/index contract is frozen.

    This is read-only.  In particular, it never repairs an old nullable row
    ledger.  ``require_triggers`` is retained only for caller compatibility;
    managed-database deployments do not require or inspect database triggers.
    """

    try:
        if hasattr(bind, "execute"):
            return _validate_attestation_schema_connection(  # type: ignore[arg-type]
                bind,
                require_triggers=require_triggers,
                require_current_manifests=require_current_manifests,
                expected_legacy_run_count=expected_legacy_run_count,
                expected_legacy_plan_hash=expected_legacy_plan_hash,
            )
        with bind.connect() as connection:
            return _validate_attestation_schema_connection(
                connection,
                require_triggers=require_triggers,
                require_current_manifests=require_current_manifests,
                expected_legacy_run_count=expected_legacy_run_count,
                expected_legacy_plan_hash=expected_legacy_plan_hash,
            )
    except QmtAttestationSchemaError:
        raise
    except Exception as exc:
        raise QmtAttestationSchemaError(
            {
                "protocol_version": ATTESTATION_PROTOCOL_VERSION,
                "errors": [f"{type(exc).__name__}: {exc}"],
            }
        ) from exc


def _match_sql(target_alias: str = "t", source_alias: str = "q") -> str:
    price_checks = " AND ".join(
        (
            f"({target_alias}.`{field}` IS NOT NULL "
            f"AND {source_alias}.`{field}` IS NOT NULL "
            f"AND {target_alias}.`{field}` > 0 "
            f"AND {source_alias}.`{field}` > 0 "
            f"AND ABS({target_alias}.`{field}` - {source_alias}.`{field}`) "
            f"<= :price_tolerance)"
        )
        for field in ("open", "close", "high", "low")
    )
    volume_check = (
        f"({target_alias}.volume IS NOT NULL "
        f"AND {source_alias}.volume IS NOT NULL "
        f"AND {target_alias}.volume >= 0 AND {source_alias}.volume >= 0 "
        f"AND ABS({target_alias}.volume - {source_alias}.volume) <= "
        f"GREATEST(:volume_absolute_tolerance, "
        f"ABS({source_alias}.volume) * :volume_rel_tolerance))"
    )
    amount_check = (
        f"({target_alias}.amount IS NOT NULL "
        f"AND {source_alias}.amount IS NOT NULL "
        f"AND {target_alias}.amount >= 0 AND {source_alias}.amount >= 0 "
        f"AND ABS({target_alias}.amount - {source_alias}.amount) <= "
        f"GREATEST(1.0, ABS({source_alias}.amount) * :amount_rel_tolerance))"
    )
    native_pre_close_check = (
        f"{source_alias}.pre_close IS NOT NULL "
        f"AND {source_alias}.pre_close > 0 "
        f"AND BINARY {source_alias}.pre_close_origin="
        "BINARY 'NATIVE_QMT'"
    )
    return (
        f"{price_checks} AND {volume_check} AND {amount_check} "
        f"AND {native_pre_close_check}"
    )


def attest_range(
    engine: Engine,
    *,
    start_date: str,
    end_date: str,
    apply: bool,
    provider: str = PROVIDER_ID,
    mismatch_sample_limit: int = 5000,
    schema_prepared: bool = False,
    local_history_engine: Engine | None = None,
) -> dict[str, Any]:
    if type(schema_prepared) is not bool:
        raise TypeError("schema_prepared must be bool")
    validate_attestation_schema(engine)
    target_table, source_table = _table_names(
        engine,
        local_history_engine=local_history_engine,
    )
    run_id = f"qmt_attest_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    tolerances = {
        "attestation_protocol": ATTESTATION_PROTOCOL_VERSION,
        **dict(QMT_V2_TOLERANCE_VALUES),
    }
    params = {
        "run_id": run_id,
        "provider": provider,
        "start_date": start_date,
        "end_date": end_date,
        "price_tolerance": PRICE_TOLERANCE,
        "volume_absolute_tolerance": VOLUME_ABSOLUTE_TOLERANCE,
        "volume_rel_tolerance": VOLUME_REL_TOLERANCE,
        "amount_rel_tolerance": AMOUNT_REL_TOLERANCE,
        "attestation_protocol": ATTESTATION_PROTOCOL_VERSION,
    }
    with _attestation_transaction(
        engine, schema_prepared=schema_prepared,
    ) as connection:
        connection.execute(
            text(
                """
                INSERT INTO qmt_kline_attestation_run
                (run_id, provider, start_date, end_date, status,
                 tolerance_json, started_at)
                VALUES
                (:run_id, :provider, :start_date, :end_date, 'RUNNING',
                 :tolerance_json, NOW())
                """
            ),
            {**params, "tolerance_json": json.dumps(tolerances, sort_keys=True)},
        )
    match_sql = _match_sql()
    unqualified_a_share = a_share_stock_code_sql("stock_code")
    raw_a_share = a_share_stock_code_sql("raw.stock_code")
    target_temp = "tmp_qmt_attest_target"
    source_temp = "tmp_qmt_attest_source"
    source_batch_temp = "tmp_qmt_attest_source_batch"
    calendar_temp = "tmp_qmt_attest_calendar"
    expected_temp = "tmp_qmt_attest_expected"
    compare_temp = "tmp_qmt_attest_compare"
    try:
        with _attestation_transaction(
            engine, schema_prepared=schema_prepared,
        ) as connection:
            # Pull each indexed date range sequentially once.  Comparing the
            # two compact temporary tables is substantially faster than
            # repeating random cross-schema lookups for counters, mismatch
            # samples, and the final update.
            catalog = load_stock_catalog(
                connection,
                decision_known_at=datetime.now().replace(microsecond=0),
            )
            calendar_receipt = load_trade_calendar_receipt(
                connection,
                start_date=start_date,
                end_date=end_date,
                decision_known_at=datetime.now().replace(microsecond=0),
            )
            catalog_sessions = calendar_receipt.sessions_between(
                start_date, end_date
            )
            if any(not day for day in catalog_sessions):
                raise RuntimeError("QMT catalog target session is invalid")
            catalog_daily_codes = {
                day: catalog.eligible_codes(day) for day in catalog_sessions
            }
            if any(not codes for codes in catalog_daily_codes.values()):
                raise RuntimeError(
                    "independent QMT catalog has an empty target-date universe"
                )
            for temporary in (
                compare_temp, source_temp, source_batch_temp, calendar_temp,
                target_temp, expected_temp,
            ):
                connection.execute(text(f"DROP TEMPORARY TABLE IF EXISTS `{temporary}`"))
            connection.execute(text(f"""
                CREATE TEMPORARY TABLE `{calendar_temp}` (
                    trade_date DATE NOT NULL PRIMARY KEY
                ) ENGINE=InnoDB
            """))
            if catalog_sessions:
                connection.execute(text(f"""
                    INSERT INTO `{calendar_temp}` (trade_date)
                    VALUES (:trade_date)
                """), [
                    {"trade_date": trade_date}
                    for trade_date in catalog_sessions
                ])
            connection.execute(
                text(
                    f"""
                    CREATE TEMPORARY TABLE `{target_temp}` (
                        target_id BIGINT NOT NULL,
                        stock_code VARCHAR(16) CHARACTER SET utf8mb4
                            COLLATE utf8mb4_unicode_ci NOT NULL,
                        trade_date DATE NOT NULL,
                        `open` DECIMAL(20,6) NULL,
                        `close` DECIMAL(20,6) NULL,
                        `high` DECIMAL(20,6) NULL,
                        `low` DECIMAL(20,6) NULL,
                        volume DECIMAL(24,6) NULL,
                        amount DECIMAL(24,6) NULL,
                        pre_close DECIMAL(20,6) NULL,
                        PRIMARY KEY (target_id),
                        UNIQUE KEY uk_tmp_qmt_attest_target
                            (stock_code, trade_date)
                    ) ENGINE=InnoDB AS
                    SELECT id AS target_id,
                           stock_code,
                           trade_date, `open`, `close`, `high`, `low`,
                           volume, amount, pre_close
                    FROM {target_table}
                    WHERE trade_date BETWEEN :start_date AND :end_date
                      AND k_type=1 AND adjust_type=0
                      AND {unqualified_a_share}
                    """
                ),
                params,
            )
            connection.execute(
                text(
                    f"""
                    CREATE TEMPORARY TABLE `{source_batch_temp}` (
                        trade_date DATE NOT NULL PRIMARY KEY,
                        batch_id VARCHAR(64) NOT NULL
                    ) ENGINE=InnoDB
                    """
                )
            )
            source_batch_rows = connection.execute(text(f"""
                SELECT trade_date, batch_id, MAX(received_at) AS latest_received_at
                FROM {source_table}
                WHERE trade_date BETWEEN :start_date AND :end_date
                  AND period='1d' AND k_type=1 AND adjust_type=0
                  AND provider=:provider
                  AND {unqualified_a_share}
                GROUP BY trade_date, batch_id
                ORDER BY trade_date, latest_received_at DESC, BINARY batch_id DESC
            """), params).mappings().all()
            source_batch_by_date: dict[str, str] = {}
            for row in source_batch_rows:
                day = str(row.get("trade_date") or "")[:10]
                source_batch_id = str(row.get("batch_id") or "").strip()
                if day and source_batch_id and day not in source_batch_by_date:
                    source_batch_by_date[day] = source_batch_id
            if source_batch_by_date:
                connection.execute(text(f"""
                    INSERT INTO `{source_batch_temp}` (trade_date, batch_id)
                    VALUES (:trade_date, :batch_id)
                """), [
                    {"trade_date": day, "batch_id": batch_id}
                    for day, batch_id in sorted(source_batch_by_date.items())
                ])
            connection.execute(
                text(
                    f"""
                    CREATE TEMPORARY TABLE `{source_temp}` (
                        qmt_id BIGINT NOT NULL,
                        stock_code VARCHAR(16) CHARACTER SET utf8mb4
                            COLLATE utf8mb4_unicode_ci NOT NULL,
                        trade_date DATE NOT NULL,
                        `open` DECIMAL(20,6) NULL,
                        `close` DECIMAL(20,6) NULL,
                        `high` DECIMAL(20,6) NULL,
                        `low` DECIMAL(20,6) NULL,
                        volume DECIMAL(24,6) NULL,
                        amount DECIMAL(24,6) NULL,
                        pre_close DECIMAL(20,6) NULL,
                        pre_close_origin VARCHAR(32) NULL,
                        qmt_code VARCHAR(32) NULL,
                        provider VARCHAR(32) NULL,
                        source_time DATETIME NULL,
                        received_at DATETIME NULL,
                        batch_id VARCHAR(64) NULL,
                        data_version VARCHAR(64) NULL,
                        permission_status VARCHAR(32) NULL,
                        PRIMARY KEY (qmt_id),
                        UNIQUE KEY uk_tmp_qmt_attest_source
                            (stock_code, trade_date)
                    ) ENGINE=InnoDB AS
                    SELECT raw.id AS qmt_id, raw.stock_code, raw.trade_date,
                           raw.`open`, raw.`close`, raw.`high`, raw.`low`,
                           raw.volume, raw.amount,
                           raw.pre_close, raw.pre_close_origin,
                           raw.qmt_code, raw.provider, raw.source_time,
                           raw.received_at, raw.batch_id, raw.data_version,
                           raw.permission_status
                    FROM {source_table} AS raw
                    JOIN `{source_batch_temp}` AS selected_batch
                      ON selected_batch.trade_date=raw.trade_date
                     AND BINARY selected_batch.batch_id=BINARY raw.batch_id
                    WHERE raw.trade_date BETWEEN :start_date AND :end_date
                      AND raw.period='1d' AND raw.k_type=1
                      AND raw.adjust_type=0
                      AND raw.provider=:provider
                      AND {raw_a_share}
                    """
                ),
                params,
            )
            connection.execute(text(f"""
                CREATE TEMPORARY TABLE `{expected_temp}` (
                    trade_date DATE NOT NULL,
                    stock_code VARCHAR(16) CHARACTER SET utf8mb4
                        COLLATE utf8mb4_unicode_ci NOT NULL,
                    PRIMARY KEY (trade_date, stock_code)
                ) ENGINE=InnoDB AS
                SELECT calendar.trade_date, member.stock_code
                FROM `{calendar_temp}` AS calendar
                JOIN qmt_stock_catalog_member AS member
                  ON member.batch_id=:catalog_batch_id
                 AND member.list_date <= calendar.trade_date
                 AND (
                     member.expire_date IS NULL
                     OR member.expire_date > calendar.trade_date
                 )
                WHERE calendar.trade_date BETWEEN :start_date AND :end_date
            """), {
                **params,
                "catalog_batch_id": catalog.batch_id,
            })
            expected_rows = int(connection.execute(
                text(f"SELECT COUNT(*) FROM `{expected_temp}`")
            ).scalar() or 0)
            expected_contract_rows = sum(
                len(codes) for codes in catalog_daily_codes.values()
            )
            if expected_rows != expected_contract_rows:
                raise RuntimeError(
                    "QMT catalog SQL/Python target-set proof differs"
                )
            qmt_rows = int(
                connection.execute(
                    text(f"SELECT COUNT(*) FROM `{source_temp}`")
                ).scalar()
                or 0
            )
            source_only_rows = int(
                connection.execute(text(
                    f"""
                    SELECT COUNT(*)
                    FROM `{source_temp}` q
                    LEFT JOIN `{target_temp}` t
                      ON t.stock_code=q.stock_code
                     AND t.trade_date=q.trade_date
                    WHERE t.target_id IS NULL
                    """
                )).scalar()
                or 0
            )
            catalog_missing_target_rows = int(connection.execute(text(f"""
                SELECT COUNT(*)
                FROM `{expected_temp}` expected
                LEFT JOIN `{target_temp}` target
                  ON target.trade_date=expected.trade_date
                 AND target.stock_code=expected.stock_code
                WHERE target.target_id IS NULL
            """)).scalar() or 0)
            target_not_catalog_rows = int(connection.execute(text(f"""
                SELECT COUNT(*)
                FROM `{target_temp}` target
                LEFT JOIN `{expected_temp}` expected
                  ON expected.trade_date=target.trade_date
                 AND expected.stock_code=target.stock_code
                WHERE expected.stock_code IS NULL
            """)).scalar() or 0)
            catalog_missing_source_rows = int(connection.execute(text(f"""
                SELECT COUNT(*)
                FROM `{expected_temp}` expected
                LEFT JOIN `{source_temp}` source
                  ON source.trade_date=expected.trade_date
                 AND source.stock_code=expected.stock_code
                WHERE source.qmt_id IS NULL
            """)).scalar() or 0)
            source_not_catalog_rows = int(connection.execute(text(f"""
                SELECT COUNT(*)
                FROM `{source_temp}` source
                LEFT JOIN `{expected_temp}` expected
                  ON expected.trade_date=source.trade_date
                 AND expected.stock_code=source.stock_code
                WHERE expected.stock_code IS NULL
            """)).scalar() or 0)
            universe_gap_rows = sum((
                catalog_missing_target_rows,
                target_not_catalog_rows,
                catalog_missing_source_rows,
                source_not_catalog_rows,
            ))
            connection.execute(
                text(
                    f"""
                    CREATE TEMPORARY TABLE `{compare_temp}` (
                        target_id BIGINT NOT NULL,
                        qmt_id BIGINT NULL,
                        trade_date DATE NOT NULL,
                        stock_code VARCHAR(16) CHARACTER SET utf8mb4
                            COLLATE utf8mb4_unicode_ci NOT NULL,
                        is_match TINYINT NULL,
                        provenance_already TINYINT NOT NULL,
                        target_close DECIMAL(20,6) NULL,
                        qmt_close DECIMAL(20,6) NULL,
                        target_pre_close DECIMAL(20,6) NULL,
                        qmt_pre_close DECIMAL(20,6) NULL,
                        target_volume DECIMAL(24,6) NULL,
                        qmt_volume DECIMAL(24,6) NULL,
                        target_amount DECIMAL(24,6) NULL,
                        qmt_amount DECIMAL(24,6) NULL,
                        qmt_code VARCHAR(32) NULL,
                        provider VARCHAR(32) NULL,
                        source_time DATETIME NULL,
                        received_at DATETIME NULL,
                        batch_id VARCHAR(64) NULL,
                        data_version VARCHAR(64) NULL,
                        permission_status VARCHAR(32) NULL,
                        pre_close_origin VARCHAR(32) NULL,
                        qmt_open DECIMAL(20,6) NULL,
                        qmt_high DECIMAL(20,6) NULL,
                        qmt_low DECIMAL(20,6) NULL,
                        PRIMARY KEY (target_id),
                        KEY idx_tmp_qmt_attest_match (is_match),
                        KEY idx_tmp_qmt_attest_source (qmt_id),
                        KEY idx_tmp_qmt_attest_pending
                            (is_match, provenance_already, trade_date)
                    ) ENGINE=InnoDB
                    """
                ),
            )
            # Keep every individual comparison statement below the same
            # ten-session size proven by the production backfill batches.
            # A single 120-session correlated provenance lookup can exceed a
            # defensive 30-second DB read timeout even when every row is
            # valid; chunking changes only execution shape, not the snapshot
            # or the final all-or-nothing transaction.
            compare_sessions = list(catalog_sessions)
            if any(not value for value in compare_sessions):
                raise RuntimeError("QMT attestation target session is invalid")
            for offset in range(
                0,
                len(compare_sessions),
                ATTESTATION_SESSION_CHUNK_SIZE,
            ):
                session_chunk = compare_sessions[
                    offset:offset + ATTESTATION_SESSION_CHUNK_SIZE
                ]
                chunk_params = {
                    **params,
                    "chunk_start_date": session_chunk[0],
                    "chunk_end_date": session_chunk[-1],
                }
                connection.execute(
                    text(
                        f"""
                        INSERT INTO `{compare_temp}`
                        SELECT t.target_id, q.qmt_id,
                               t.trade_date, t.stock_code,
                               ({match_sql}) AS is_match,
                               COALESCE(EXISTS(
                                   SELECT 1
                                   FROM qmt_kline_attestation_row a
                                   WHERE a.target_id=t.target_id
                                     AND a.qmt_id=q.qmt_id
                                     AND BINARY a.protocol_version=
                                         BINARY :attestation_protocol
                                     AND BINARY a.source_data_version=
                                         BINARY q.data_version
                                     AND BINARY a.source_pre_close_origin=
                                         BINARY q.pre_close_origin
                                     AND a.source_pre_close=q.pre_close
                                     AND a.attested_open=q.`open`
                                     AND a.attested_close=q.`close`
                                     AND a.attested_high=q.`high`
                                     AND a.attested_low=q.`low`
                                     AND a.attested_volume=q.volume
                                     AND a.attested_amount=q.amount
                                     AND t.pre_close=a.source_pre_close
                                     AND t.`open`=a.attested_open
                                     AND t.`close`=a.attested_close
                                     AND t.`high`=a.attested_high
                                     AND t.`low`=a.attested_low
                                     AND t.volume=a.attested_volume
                                     AND t.amount=a.attested_amount
                               ), 0) AS provenance_already,
                               t.close AS target_close, q.close AS qmt_close,
                               t.pre_close AS target_pre_close,
                               q.pre_close AS qmt_pre_close,
                               t.volume AS target_volume, q.volume AS qmt_volume,
                               t.amount AS target_amount, q.amount AS qmt_amount,
                               q.qmt_code, q.provider, q.source_time,
                               q.received_at, q.batch_id, q.data_version,
                               q.permission_status, q.pre_close_origin,
                               q.`open` AS qmt_open,
                               q.`high` AS qmt_high,
                               q.`low` AS qmt_low
                        FROM `{target_temp}` t
                        LEFT JOIN `{source_temp}` q
                          ON q.stock_code=t.stock_code
                         AND q.trade_date=t.trade_date
                        WHERE t.trade_date BETWEEN :chunk_start_date
                                               AND :chunk_end_date
                        """
                    ),
                    chunk_params,
                )
            aggregate = connection.execute(
                text(
                    f"""
                    SELECT COUNT(*) AS target_rows,
                           COALESCE(SUM(is_match), 0) AS matched_rows,
                           COALESCE(SUM(qmt_id IS NULL), 0)
                               AS missing_qmt_rows,
                           COALESCE(SUM(qmt_id IS NOT NULL), 0)
                               AS joined_rows,
                           COALESCE(SUM(
                               is_match AND provenance_already
                           ), 0) AS already_attested_rows
                    FROM `{compare_temp}`
                    """
                )
            ).mappings().one()
            target_rows = int(aggregate["target_rows"] or 0)
            matched_rows = int(aggregate["matched_rows"] or 0)
            missing_qmt_rows = int(aggregate["missing_qmt_rows"] or 0)
            joined_rows = int(aggregate["joined_rows"] or 0)
            mismatched_rows = max(
                0,
                joined_rows - matched_rows + source_only_rows
                + universe_gap_rows,
            )
            already_attested_rows = int(aggregate["already_attested_rows"] or 0)
            sample_limit = max(0, min(50000, int(mismatch_sample_limit)))
            if sample_limit:
                mismatch_rows = []
                if target_rows:
                    mismatch_rows = connection.execute(
                        text(
                            f"""
                            SELECT t.trade_date, t.stock_code,
                                   CASE WHEN t.qmt_id IS NULL THEN 'MISSING_QMT'
                                        ELSE 'VALUE_MISMATCH' END AS reason,
                                   t.target_close, t.qmt_close,
                                   t.target_volume, t.qmt_volume,
                                   t.target_amount, t.qmt_amount
                            FROM `{compare_temp}` t
                            WHERE t.qmt_id IS NULL OR NOT t.is_match
                            ORDER BY t.trade_date, t.stock_code
                            LIMIT {sample_limit}
                            """
                        ),
                    ).mappings().all()
                if mismatch_rows:
                    connection.execute(
                        text(
                            """
                            INSERT INTO qmt_kline_attestation_mismatch
                            (run_id, trade_date, stock_code, reason,
                             target_close, qmt_close, target_volume, qmt_volume,
                             target_amount, qmt_amount, created_at)
                            VALUES
                            (:run_id, :trade_date, :stock_code, :reason,
                             :target_close, :qmt_close, :target_volume, :qmt_volume,
                             :target_amount, :qmt_amount, NOW())
                            """
                        ),
                        [{"run_id": run_id, **dict(row)} for row in mismatch_rows],
                    )
                source_only_samples = connection.execute(
                    text(
                        f"""
                        SELECT q.trade_date, q.stock_code,
                               'SOURCE_ONLY' AS reason,
                               NULL AS target_close,
                               q.`close` AS qmt_close,
                               NULL AS target_volume,
                               q.volume AS qmt_volume,
                               NULL AS target_amount,
                               q.amount AS qmt_amount
                        FROM `{source_temp}` q
                        LEFT JOIN `{target_temp}` t
                          ON t.stock_code=q.stock_code
                         AND t.trade_date=q.trade_date
                        WHERE t.target_id IS NULL
                        ORDER BY q.trade_date, q.stock_code
                        LIMIT {sample_limit}
                        """
                    )
                ).mappings().all()
                if source_only_samples:
                    connection.execute(
                        text(
                            """
                            INSERT INTO qmt_kline_attestation_mismatch
                            (run_id, trade_date, stock_code, reason,
                             target_close, qmt_close, target_volume, qmt_volume,
                             target_amount, qmt_amount, created_at)
                            VALUES
                            (:run_id, :trade_date, :stock_code, :reason,
                             :target_close, :qmt_close, :target_volume, :qmt_volume,
                             :target_amount, :qmt_amount, NOW())
                            """
                        ),
                        [
                            {"run_id": run_id, **dict(row)}
                            for row in source_only_samples
                        ],
                    )
            universe_sets_exact = bool(
                catalog_sessions
                and set(source_batch_by_date) == set(catalog_sessions)
                and all(
                    source_batch_by_date[day]
                    == daily_market_source_batch_id(
                        catalog_manifest_hash=catalog.manifest_hash,
                        calendar_manifest_hash=calendar_receipt.manifest_hash,
                    )
                    for day in catalog_sessions
                )
                and universe_gap_rows == 0
                and target_rows == expected_rows
                and qmt_rows == expected_rows
            )
            if universe_sets_exact:
                daily_universe = {
                    day: bound_stock_set_contract(
                        day,
                        codes,
                        catalog_batch_id=catalog.batch_id,
                        catalog_member_count=catalog.member_count,
                        catalog_member_set_hash=catalog.member_set_hash,
                        catalog_manifest_hash=catalog.manifest_hash,
                        source_batch_id=source_batch_by_date[day],
                        calendar_batch_id=calendar_receipt.batch_id,
                        calendar_session_set_hash=(
                            calendar_receipt.session_set_hash
                        ),
                        calendar_manifest_hash=calendar_receipt.manifest_hash,
                        calendar_known_at=calendar_receipt.known_at,
                    )
                    for day, codes in sorted(catalog_daily_codes.items())
                }
            else:
                # A failed/partial run records only its planned catalog set.
                # It must never claim catalog/source/target equality.
                daily_universe = {
                    day: expected_stock_set_contract(day, codes)
                    for day, codes in sorted(catalog_daily_codes.items())
                }
            run_tolerances = build_qmt_v2_manifest(daily_universe)
            manifest_row_count = sum(
                int(contract["stock_count"])
                for contract in daily_universe.values()
            )
            manifest_complete = bool(
                daily_universe
                and manifest_row_count == target_rows
                and universe_sets_exact
                and validated_universe_manifest(
                    run_tolerances,
                    start_date=start_date,
                    end_date=end_date,
                )
                == daily_universe
            )
            universe_complete = bool(
                target_rows > 0
                and matched_rows == target_rows
                and source_only_rows == 0
                and universe_sets_exact
                and manifest_complete
            )
            updated_rows = 0
            # Never stamp a partial universe.  Otherwise rows from a PARTIAL
            # run can survive through the append-only uniqueness key and make
            # a later complete run appear bound to incomplete evidence.
            if apply and universe_complete:
                exact_readback_rows = already_attested_rows
                for offset in range(
                    0,
                    len(compare_sessions),
                    ATTESTATION_SESSION_CHUNK_SIZE,
                ):
                    session_chunk = compare_sessions[
                        offset:offset + ATTESTATION_SESSION_CHUNK_SIZE
                    ]
                    chunk_params = {
                        **params,
                        "chunk_start_date": session_chunk[0],
                        "chunk_end_date": session_chunk[-1],
                    }
                    updated_rows += int(
                        connection.execute(
                            text(
                                f"""
                                UPDATE {target_table} t
                                INNER JOIN `{compare_temp}` q
                                  ON q.target_id=t.id
                                SET t.qmt_code=q.qmt_code,
                                    t.data_source=q.provider,
                                    t.`open`=q.qmt_open,
                                    t.`close`=q.qmt_close,
                                    t.`high`=q.qmt_high,
                                    t.`low`=q.qmt_low,
                                    t.volume=q.qmt_volume,
                                    t.amount=q.qmt_amount,
                                    t.pre_close=q.qmt_pre_close,
                                    t.`change`=q.qmt_close-q.qmt_pre_close,
                                    t.change_pct=(q.qmt_close-q.qmt_pre_close)
                                        / q.qmt_pre_close * 100,
                                    t.source_time=q.source_time,
                                    t.received_at=q.received_at,
                                    t.batch_id=q.batch_id,
                                    t.data_version=q.data_version,
                                    t.quality_status='QMT_ATTESTED',
                                    t.permission_status=q.permission_status,
                                    t.etl_sync_at=NOW()
                                WHERE q.is_match
                                  AND NOT COALESCE(q.provenance_already, 0)
                                  AND q.trade_date BETWEEN :chunk_start_date
                                                       AND :chunk_end_date
                                """
                            ),
                            chunk_params,
                        ).rowcount
                        or 0
                    )
                    connection.execute(
                        text(
                            f"""
                            INSERT IGNORE INTO qmt_kline_attestation_row (
                                attestation_id, run_id, target_id, qmt_id,
                                trade_date, stock_code, protocol_version,
                                source_data_version, source_pre_close_origin,
                                source_pre_close, attested_open, attested_close,
                                attested_high, attested_low, attested_volume,
                                attested_amount, created_at
                            )
                            SELECT SHA2(CONCAT_WS('|',
                                       :attestation_protocol,
                                       q.target_id, q.qmt_id,
                                       q.data_version,
                                       q.qmt_pre_close,
                                       q.qmt_open, q.qmt_close,
                                       q.qmt_high, q.qmt_low,
                                       q.qmt_volume, q.qmt_amount), 256),
                                   :run_id, q.target_id, q.qmt_id,
                                   q.trade_date, q.stock_code,
                                   :attestation_protocol,
                                   q.data_version, q.pre_close_origin,
                                   q.qmt_pre_close, q.qmt_open, q.qmt_close,
                                   q.qmt_high, q.qmt_low,
                                   q.qmt_volume, q.qmt_amount, NOW()
                            FROM `{compare_temp}` q
                            WHERE q.is_match
                              AND NOT COALESCE(q.provenance_already, 0)
                              AND q.trade_date BETWEEN :chunk_start_date
                                                   AND :chunk_end_date
                            """
                        ),
                        chunk_params,
                    )
                    exact_readback_rows += int(
                        connection.execute(
                            text(
                                f"""
                                SELECT COUNT(DISTINCT q.target_id)
                                FROM `{compare_temp}` q
                                JOIN {target_table} t ON t.id=q.target_id
                                JOIN qmt_kline_attestation_row a
                                  ON a.target_id=t.id
                                 AND BINARY a.protocol_version=
                                     BINARY :attestation_protocol
                                 AND BINARY a.source_data_version=
                                     BINARY t.data_version
                                 AND BINARY a.source_pre_close_origin=
                                     BINARY 'NATIVE_QMT'
                                 AND a.source_pre_close=t.pre_close
                                 AND a.attested_open=t.`open`
                                 AND a.attested_close=t.`close`
                                 AND a.attested_high=t.`high`
                                 AND a.attested_low=t.`low`
                                 AND a.attested_volume=t.volume
                                 AND a.attested_amount=t.amount
                                WHERE q.is_match
                                  AND NOT COALESCE(q.provenance_already, 0)
                                  AND q.trade_date BETWEEN :chunk_start_date
                                                       AND :chunk_end_date
                                """
                            ),
                            chunk_params,
                        ).scalar()
                        or 0
                    )
                if exact_readback_rows != matched_rows:
                    raise RuntimeError(
                        "QMT attestation exact readback mismatch: "
                        f"{exact_readback_rows}/{matched_rows}"
                    )
            if target_rows == 0 and qmt_rows == 0:
                status = "EMPTY_TARGET"
            elif universe_complete:
                status = "COMPLETED" if apply else "DRY_RUN_COMPLETE"
            elif target_rows == 0:
                status = "BLOCKED_TARGET_INCOMPLETE"
            elif matched_rows == 0 and missing_qmt_rows == target_rows:
                status = "BLOCKED_SOURCE_INCOMPLETE"
            else:
                status = "PARTIAL" if apply else "DRY_RUN_PARTIAL"
            connection.execute(
                text(
                    """
                    UPDATE qmt_kline_attestation_run
                    SET status=:status, target_rows=:target_rows,
                        qmt_rows=:qmt_rows, matched_rows=:matched_rows,
                        missing_qmt_rows=:missing_qmt_rows,
                        mismatched_rows=:mismatched_rows,
                        already_attested_rows=:already_attested_rows,
                        updated_rows=:updated_rows,
                        tolerance_json=:tolerance_json, finished_at=NOW()
                    WHERE run_id=:run_id
                    """
                ),
                {
                    **params,
                    "status": status,
                    "target_rows": target_rows,
                    "qmt_rows": qmt_rows,
                    "matched_rows": matched_rows,
                    "missing_qmt_rows": missing_qmt_rows,
                    "mismatched_rows": mismatched_rows,
                    "already_attested_rows": already_attested_rows,
                    "updated_rows": updated_rows,
                    "tolerance_json": json.dumps(
                        run_tolerances,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                },
            )
            for temporary in (
                compare_temp, source_temp, source_batch_temp, calendar_temp,
                target_temp, expected_temp,
            ):
                connection.execute(text(f"DROP TEMPORARY TABLE IF EXISTS `{temporary}`"))
        return {
            "run_id": run_id,
            "status": status,
            "apply": apply,
            "provider": provider,
            "start_date": start_date,
            "end_date": end_date,
            "target_rows": target_rows,
            "qmt_rows": qmt_rows,
            "matched_rows": matched_rows,
            "missing_qmt_rows": missing_qmt_rows,
            "source_only_rows": source_only_rows,
            "catalog_batch_id": catalog.batch_id,
            "catalog_manifest_hash": catalog.manifest_hash,
            "catalog_member_count": catalog.member_count,
            "catalog_member_set_hash": catalog.member_set_hash,
            "source_batch_by_date": dict(sorted(source_batch_by_date.items())),
            "calendar_batch_id": calendar_receipt.batch_id,
            "calendar_session_set_hash": calendar_receipt.session_set_hash,
            "calendar_manifest_hash": calendar_receipt.manifest_hash,
            "calendar_known_at": calendar_receipt.known_at,
            "catalog_missing_target_rows": catalog_missing_target_rows,
            "target_not_catalog_rows": target_not_catalog_rows,
            "catalog_missing_source_rows": catalog_missing_source_rows,
            "source_not_catalog_rows": source_not_catalog_rows,
            "mismatched_rows": mismatched_rows,
            "already_attested_rows": already_attested_rows,
            "updated_rows": updated_rows,
            "attestation_protocol": ATTESTATION_PROTOCOL_VERSION,
            "tolerances": run_tolerances,
            "universe_manifest_schema": run_tolerances[
                "universe_manifest_schema"
            ],
            "daily_universe": daily_universe,
        }
    except Exception as exc:
        with _attestation_transaction(
            engine, schema_prepared=schema_prepared,
        ) as connection:
            connection.execute(
                text(
                    """
                    UPDATE qmt_kline_attestation_run
                    SET status='FAILED', error_message=:error,
                        finished_at=NOW()
                    WHERE run_id=:run_id
                    """
                ),
                {"run_id": run_id, "error": str(exc)[:4000]},
            )
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--provider", default=PROVIDER_ID)
    parser.add_argument("--mismatch-sample-limit", type=int, default=5000)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--windows-local-option-file",
        action="store_true",
        help=(
            "Use the fixed protected Windows MySQL option file for both "
            "the local primary and QMT history schemas."
        ),
    )
    args = parser.parse_args()
    load_project_env()
    local_history_engine = None
    if args.windows_local_option_file:
        from tools.backfill_guojin_qmt_local_history import (
            _windows_local_engines,
        )

        engine, local_history_engine = _windows_local_engines()
    else:
        engine = create_batch_engine(future=True)
    try:
        result = attest_range(
            engine,
            start_date=args.start_date,
            end_date=args.end_date,
            apply=args.apply,
            provider=args.provider,
            mismatch_sample_limit=args.mismatch_sample_limit,
            local_history_engine=local_history_engine,
        )
    finally:
        if args.windows_local_option_file:
            engine.dispose()
            assert local_history_engine is not None
            local_history_engine.dispose()
    print(
        json.dumps(result, ensure_ascii=False, default=str)
        if args.json
        else result
    )
    return 0 if result["status"] in {"COMPLETED", "DRY_RUN_COMPLETE", "EMPTY_TARGET"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
