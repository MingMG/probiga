"""Frozen, shared contract for funding-eligible QMT V2 run manifests.

The persisted ``tolerance_json`` of a COMPLETED run is evidence, not an
extensible configuration object.  Every producer and consumer therefore uses
this module so a permissive parser cannot silently admit a different protocol.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from types import MappingProxyType
from typing import Any, Iterable


ATTESTATION_PROTOCOL_VERSION = "QMT_DAILY_UNADJUSTED_PRECLOSE_V2"
UNIVERSE_MANIFEST_SCHEMA = "probiga.qmt-daily-universe.v1"
EXPECTED_STOCK_SET_SCHEMA = "probiga.qmt-expected-stock-set.v1"
QMT_ATTESTATION_COLLATION = "utf8mb4_unicode_ci"
QMT_ATTESTATION_LEGACY_COLLATION = "utf8mb4_general_ci"
QMT_ATTESTATION_TRIGGER_DEFINER = "probiga_migrator@127.0.0.1"
QMT_ATTESTATION_TRIGGER_SQL_MODE = (
    "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,"
    "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
)

# These compact tuples are the code-owned QMT attestation schema contract.
# Collation maintenance must compare live metadata to these values; it must
# never turn a live SHOW CREATE TABLE result into its own expected manifest.
# Tuple fields are: name, data type, character length, numeric precision,
# numeric scale, nullable, default, extra.
QMT_ATTESTATION_COLUMN_SPECS = MappingProxyType({
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
})

# Index fields are: uniqueness (0 means unique), then the ordered columns.
QMT_ATTESTATION_INDEX_SPECS = MappingProxyType({
    "qmt_kline_attestation_schema_migration": MappingProxyType({
        "PRIMARY": (0, ("migration_key",)),
    }),
    "qmt_kline_attestation_run": MappingProxyType({
        "PRIMARY": (0, ("run_id",)),
        "idx_qmt_kline_attestation_range": (
            1,
            ("start_date", "end_date", "status"),
        ),
    }),
    "qmt_kline_attestation_mismatch": MappingProxyType({
        "PRIMARY": (0, ("id",)),
        "uk_qmt_kline_attestation_mismatch": (
            0,
            ("run_id", "trade_date", "stock_code"),
        ),
        "idx_qmt_kline_mismatch_lookup": (1, ("trade_date", "stock_code")),
    }),
    "qmt_kline_attestation_row": MappingProxyType({
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
    }),
})

# Trigger fields are: timing, event, table and normalized ACTION_STATEMENT.
QMT_ATTESTATION_TRIGGER_SPECS = MappingProxyType({
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
})

PRICE_TOLERANCE = 0.0001
PRE_CLOSE_ABSOLUTE_TOLERANCE = 0.0001
VOLUME_ABSOLUTE_TOLERANCE = 100.0
VOLUME_REL_TOLERANCE = 0.0001
AMOUNT_REL_TOLERANCE = 0.001

QMT_V2_TOLERANCE_VALUES = MappingProxyType({
    "price_absolute": PRICE_TOLERANCE,
    "pre_close_absolute": PRE_CLOSE_ABSOLUTE_TOLERANCE,
    "volume_absolute": VOLUME_ABSOLUTE_TOLERANCE,
    "volume_relative": VOLUME_REL_TOLERANCE,
    "amount_relative": AMOUNT_REL_TOLERANCE,
})
QMT_V2_MANIFEST_KEYS = frozenset({
    "attestation_protocol",
    *QMT_V2_TOLERANCE_VALUES,
    "universe_manifest_schema",
    "daily_universe",
})
QMT_V2_DAILY_ENTRY_KEYS = frozenset({"stock_count", "stock_set_hash"})
_LOWER_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def canonical_digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def expected_stock_set_contract(
    trade_date: str,
    stock_codes: Iterable[str],
) -> dict[str, Any]:
    if type(trade_date) is not str:
        raise ValueError("QMT daily universe date must be an exact string")
    try:
        parsed_day = datetime.strptime(trade_date, "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise ValueError("QMT daily universe date is invalid") from exc
    if parsed_day != trade_date:
        raise ValueError("QMT daily universe date is invalid")
    normalized_codes = sorted({str(code).strip() for code in stock_codes})
    if not normalized_codes or any(not code for code in normalized_codes):
        raise ValueError("QMT daily universe must be non-empty")
    payload = {
        "schema": EXPECTED_STOCK_SET_SCHEMA,
        "trade_date": trade_date,
        "stock_codes": normalized_codes,
    }
    return {
        "stock_count": len(normalized_codes),
        "stock_set_hash": canonical_digest(payload),
    }


def build_qmt_v2_manifest(
    daily_universe: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    return {
        "attestation_protocol": ATTESTATION_PROTOCOL_VERSION,
        **dict(QMT_V2_TOLERANCE_VALUES),
        "universe_manifest_schema": UNIVERSE_MANIFEST_SCHEMA,
        "daily_universe": daily_universe,
    }


def validated_universe_manifest(
    tolerance_json: Any,
    *,
    start_date: str,
    end_date: str,
) -> dict[str, dict[str, Any]]:
    """Validate the exact immutable manifest of one COMPLETED V2 run."""

    if isinstance(tolerance_json, dict):
        payload = tolerance_json
    else:
        try:
            payload = json.loads(str(tolerance_json or ""))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("tolerance_json is not valid JSON") from exc
    if type(payload) is not dict:
        raise ValueError("tolerance_json must be an object")
    if set(payload) != QMT_V2_MANIFEST_KEYS:
        raise ValueError("QMT V2 manifest top-level fields differ")
    if (
        type(payload["attestation_protocol"]) is not str
        or payload["attestation_protocol"] != ATTESTATION_PROTOCOL_VERSION
    ):
        raise ValueError("attestation protocol differs")
    if (
        type(payload["universe_manifest_schema"]) is not str
        or payload["universe_manifest_schema"] != UNIVERSE_MANIFEST_SCHEMA
    ):
        raise ValueError("universe manifest schema differs")
    for key, expected in QMT_V2_TOLERANCE_VALUES.items():
        observed = payload[key]
        if type(observed) is not float or observed != expected:
            raise ValueError(f"QMT V2 tolerance differs: {key}")

    daily = payload["daily_universe"]
    if type(daily) is not dict or not daily:
        raise ValueError("daily universe manifest must be non-empty")
    normalized: dict[str, dict[str, Any]] = {}
    for raw_day, raw_contract in daily.items():
        if type(raw_day) is not str:
            raise ValueError("daily universe date must be an exact string")
        try:
            parsed_day = datetime.strptime(raw_day, "%Y-%m-%d").date().isoformat()
        except ValueError as exc:
            raise ValueError("daily universe date is invalid") from exc
        if parsed_day != raw_day or not (start_date <= raw_day <= end_date):
            raise ValueError("daily universe date is outside run range")
        if type(raw_contract) is not dict or set(raw_contract) != QMT_V2_DAILY_ENTRY_KEYS:
            raise ValueError("daily universe entry fields differ")
        stock_count = raw_contract["stock_count"]
        stock_set_hash = raw_contract["stock_set_hash"]
        if (
            type(stock_count) is not int
            or stock_count <= 0
            or type(stock_set_hash) is not str
            or not _LOWER_SHA256_RE.fullmatch(stock_set_hash)
        ):
            raise ValueError("daily universe count/hash is invalid")
        normalized[raw_day] = {
            "stock_count": stock_count,
            "stock_set_hash": stock_set_hash,
        }
    return dict(sorted(normalized.items()))
