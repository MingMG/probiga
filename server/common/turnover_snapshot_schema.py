# -*- coding: utf-8 -*-
"""Privileged schema for immutable point-in-time market-field evidence.

Runtime collectors and analysis readers only validate/read these tables.  The
two append-only tables are created by the production schema bundle.  Trigger
DDL is exported as a frozen source contract and is installed only by the
release trigger broker; table migration never opens or bypasses that boundary.
"""
from __future__ import annotations

import re

from sqlalchemy import text

from server.common.runtime_table_schema import (
    RuntimeColumn,
    RuntimeIndex,
    RuntimeTable,
    privileged_normalize_mysql_storage,
    validate_runtime_tables,
)


FIELD_CAPTURE_RUN_TABLE = "st_market_field_capture_run"
FIELD_CAPTURE_ROW_TABLE = "st_market_field_capture_row"
# Compatibility names for the first producer implemented on the generic
# ledger.  Future upper-limit and other PIT field publishers use the same two
# physical tables rather than introducing provider-specific ledgers.
TURNOVER_SNAPSHOT_RUN_TABLE = FIELD_CAPTURE_RUN_TABLE
TURNOVER_SNAPSHOT_ROW_TABLE = FIELD_CAPTURE_ROW_TABLE


TURNOVER_SNAPSHOT_SCHEMA = {
    TURNOVER_SNAPSHOT_RUN_TABLE: RuntimeTable(
        columns={
            "run_id": RuntimeColumn("char", False, character_length=32),
            "schema_version": RuntimeColumn("varchar", False, character_length=64),
            "collector_build_sha": RuntimeColumn("char", False, character_length=40),
            "capture_kind": RuntimeColumn("varchar", False, character_length=40),
            "target_date": RuntimeColumn("date", False),
            "window_start_date": RuntimeColumn("date", False),
            "window_end_date": RuntimeColumn("date", False),
            "decision_at": RuntimeColumn("datetime", False, datetime_precision=6),
            "provider": RuntimeColumn("varchar", False, character_length=64),
            "api_path": RuntimeColumn("varchar", False, character_length=160),
            "transport_contract": RuntimeColumn("varchar", False, character_length=64),
            "resolved_endpoint": RuntimeColumn("varchar", False, character_length=160),
            "source_field": RuntimeColumn("varchar", False, character_length=16),
            "unit": RuntimeColumn("varchar", False, character_length=16),
            "match_policy": RuntimeColumn("varchar", False, character_length=64),
            "promotion_mode": RuntimeColumn("varchar", False, character_length=32),
            "promotion_table": RuntimeColumn("varchar", False, character_length=64),
            "promotion_column": RuntimeColumn("varchar", False, character_length=64),
            "k_type": RuntimeColumn("smallint", False),
            "adjust_type": RuntimeColumn("smallint", False),
            "subject_kind": RuntimeColumn("varchar", False, character_length=40),
            "subject_identity": RuntimeColumn("varchar", False, character_length=128),
            "subject_sha256": RuntimeColumn("char", False, character_length=64),
            "subject_payload": RuntimeColumn("mediumblob", True),
            "subject_payload_sha256": RuntimeColumn("char", True, character_length=64),
            "authority_proof_kind": RuntimeColumn("varchar", True, character_length=40),
            "authority_proof_identity": RuntimeColumn("varchar", True, character_length=128),
            "authority_proof_sha256": RuntimeColumn("char", True, character_length=64),
            "authority_set_sha256": RuntimeColumn("char", True, character_length=64),
            "expected_count": RuntimeColumn("int", False, unsigned=True),
            "fetched_count": RuntimeColumn("int", False, unsigned=True),
            "valid_count": RuntimeColumn("int", False, unsigned=True),
            "matched_count": RuntimeColumn("int", False, unsigned=True),
            "promoted_count": RuntimeColumn("int", False, unsigned=True),
            "expected_keyset_sha256": RuntimeColumn("char", False, character_length=64),
            "provider_request_payload": RuntimeColumn("mediumblob", True),
            "provider_request_sha256": RuntimeColumn("char", True, character_length=64),
            "provider_response_payload": RuntimeColumn("mediumblob", False),
            "provider_response_sha256": RuntimeColumn("char", False, character_length=64),
            "collector_binary_sha256": RuntimeColumn("char", True, character_length=64),
            "provider_sdk_version": RuntimeColumn("varchar", True, character_length=32),
            "collector_runtime_version": RuntimeColumn("varchar", True, character_length=32),
            "source_timezone": RuntimeColumn("varchar", True, character_length=40),
            "entitlement_status": RuntimeColumn("varchar", True, character_length=20),
            "raw_payload_root_sha256": RuntimeColumn("char", False, character_length=64),
            "field_value_root_sha256": RuntimeColumn("char", False, character_length=64),
            "target_fingerprint_root_sha256": RuntimeColumn("char", False, character_length=64),
            "semantic_sha256": RuntimeColumn("char", False, character_length=64),
            "request_started_at": RuntimeColumn("datetime", False, datetime_precision=6),
            "captured_max_at": RuntimeColumn("datetime", False, datetime_precision=6),
            "provider_observed_max_at": RuntimeColumn("datetime", False, datetime_precision=6),
            "published_at": RuntimeColumn("datetime", False, datetime_precision=6),
            "status": RuntimeColumn("varchar", False, character_length=16),
            "error_message": RuntimeColumn("varchar", False, character_length=500),
            "created_at": RuntimeColumn("datetime", False, datetime_precision=6),
        },
        indexes=(
            RuntimeIndex(("run_id",), unique=True),
            RuntimeIndex(("target_date", "status", "decision_at")),
            RuntimeIndex(
                ("capture_kind", "target_date", "subject_sha256", "decision_at"),
                unique=True,
            ),
            RuntimeIndex(("semantic_sha256",)),
        ),
    ),
    TURNOVER_SNAPSHOT_ROW_TABLE: RuntimeTable(
        columns={
            "run_id": RuntimeColumn("char", False, character_length=32),
            "stock_code": RuntimeColumn("varchar", False, character_length=10),
            "trade_date": RuntimeColumn("date", False),
            "k_type": RuntimeColumn("smallint", False),
            "adjust_type": RuntimeColumn("smallint", False),
            "target_row_id": RuntimeColumn("bigint", True),
            "field_value_decimal": RuntimeColumn("decimal", False, numeric_precision=20, numeric_scale=6),
            "source_pre_close": RuntimeColumn("decimal", True, numeric_precision=20, numeric_scale=6),
            "source_lower_limit": RuntimeColumn("decimal", True, numeric_precision=20, numeric_scale=6),
            "source_is_suspended": RuntimeColumn("smallint", True),
            "source_open": RuntimeColumn("decimal", True, numeric_precision=12, numeric_scale=4),
            "source_high": RuntimeColumn("decimal", True, numeric_precision=12, numeric_scale=4),
            "source_low": RuntimeColumn("decimal", True, numeric_precision=12, numeric_scale=4),
            "source_close": RuntimeColumn("decimal", True, numeric_precision=12, numeric_scale=4),
            "source_volume_shares": RuntimeColumn("decimal", True, numeric_precision=24, numeric_scale=4),
            "source_amount": RuntimeColumn("decimal", True, numeric_precision=24, numeric_scale=4),
            "raw_row_text": RuntimeColumn("varchar", False, character_length=512),
            "raw_payload": RuntimeColumn("mediumblob", False),
            "raw_payload_sha256": RuntimeColumn("char", False, character_length=64),
            "snapshot_row_sha256": RuntimeColumn("char", False, character_length=64),
            "captured_at": RuntimeColumn("datetime", False, datetime_precision=6),
            "provider_observed_at_text": RuntimeColumn("varchar", False, character_length=128),
            "provider_observed_at": RuntimeColumn("datetime", False, datetime_precision=6),
            "qmt_open": RuntimeColumn("decimal", True, numeric_precision=12, numeric_scale=4),
            "qmt_high": RuntimeColumn("decimal", True, numeric_precision=12, numeric_scale=4),
            "qmt_low": RuntimeColumn("decimal", True, numeric_precision=12, numeric_scale=4),
            "qmt_close": RuntimeColumn("decimal", True, numeric_precision=12, numeric_scale=4),
            "qmt_volume_shares": RuntimeColumn("decimal", True, numeric_precision=24, numeric_scale=4),
            "qmt_amount": RuntimeColumn("decimal", True, numeric_precision=24, numeric_scale=4),
            "qmt_received_at": RuntimeColumn("datetime", True, datetime_precision=6),
            "qmt_data_source": RuntimeColumn("varchar", True, character_length=64),
            "qmt_batch_id": RuntimeColumn("varchar", True, character_length=128),
            "qmt_data_version": RuntimeColumn("varchar", True, character_length=128),
            "qmt_quality_status": RuntimeColumn("varchar", True, character_length=32),
            "qmt_permission_status": RuntimeColumn("varchar", True, character_length=32),
            "target_prewrite_sha256": RuntimeColumn("char", False, character_length=64),
            "target_fact_sha256": RuntimeColumn("char", False, character_length=64),
            "validation_status": RuntimeColumn("varchar", False, character_length=16),
            "validation_error": RuntimeColumn("varchar", False, character_length=500),
            "promoted_at": RuntimeColumn("datetime", False, datetime_precision=6),
        },
        indexes=(
            RuntimeIndex(("run_id", "stock_code", "trade_date", "k_type", "adjust_type"), unique=True),
            RuntimeIndex(("run_id", "target_row_id"), unique=True),
            RuntimeIndex(("stock_code", "trade_date", "validation_status")),
        ),
    ),
}


TURNOVER_SNAPSHOT_DDL = (
    f"""
    CREATE TABLE IF NOT EXISTS `{TURNOVER_SNAPSHOT_RUN_TABLE}` (
      `run_id` CHAR(32) NOT NULL,
      `schema_version` VARCHAR(64) NOT NULL,
      `collector_build_sha` CHAR(40) NOT NULL,
      `capture_kind` VARCHAR(40) NOT NULL,
      `target_date` DATE NOT NULL,
      `window_start_date` DATE NOT NULL,
      `window_end_date` DATE NOT NULL,
      `decision_at` DATETIME(6) NOT NULL,
      `provider` VARCHAR(64) NOT NULL,
      `api_path` VARCHAR(160) NOT NULL,
      `transport_contract` VARCHAR(64) NOT NULL,
      `resolved_endpoint` VARCHAR(160) NOT NULL,
      `source_field` VARCHAR(16) NOT NULL,
      `unit` VARCHAR(16) NOT NULL,
      `match_policy` VARCHAR(64) NOT NULL,
      `promotion_mode` VARCHAR(32) NOT NULL,
      `promotion_table` VARCHAR(64) NOT NULL,
      `promotion_column` VARCHAR(64) NOT NULL,
      `k_type` SMALLINT NOT NULL,
      `adjust_type` SMALLINT NOT NULL,
      `subject_kind` VARCHAR(40) NOT NULL,
      `subject_identity` VARCHAR(128) NOT NULL,
      `subject_sha256` CHAR(64) NOT NULL,
      `subject_payload` MEDIUMBLOB DEFAULT NULL,
      `subject_payload_sha256` CHAR(64) DEFAULT NULL,
      `authority_proof_kind` VARCHAR(40) DEFAULT NULL,
      `authority_proof_identity` VARCHAR(128) DEFAULT NULL,
      `authority_proof_sha256` CHAR(64) DEFAULT NULL,
      `authority_set_sha256` CHAR(64) DEFAULT NULL,
      `expected_count` INT UNSIGNED NOT NULL,
      `fetched_count` INT UNSIGNED NOT NULL,
      `valid_count` INT UNSIGNED NOT NULL,
      `matched_count` INT UNSIGNED NOT NULL,
      `promoted_count` INT UNSIGNED NOT NULL,
      `expected_keyset_sha256` CHAR(64) NOT NULL,
      `provider_request_payload` MEDIUMBLOB DEFAULT NULL,
      `provider_request_sha256` CHAR(64) DEFAULT NULL,
      `provider_response_payload` MEDIUMBLOB NOT NULL,
      `provider_response_sha256` CHAR(64) NOT NULL,
      `collector_binary_sha256` CHAR(64) DEFAULT NULL,
      `provider_sdk_version` VARCHAR(32) DEFAULT NULL,
      `collector_runtime_version` VARCHAR(32) DEFAULT NULL,
      `source_timezone` VARCHAR(40) DEFAULT NULL,
      `entitlement_status` VARCHAR(20) DEFAULT NULL,
      `raw_payload_root_sha256` CHAR(64) NOT NULL,
      `field_value_root_sha256` CHAR(64) NOT NULL,
      `target_fingerprint_root_sha256` CHAR(64) NOT NULL,
      `semantic_sha256` CHAR(64) NOT NULL,
      `request_started_at` DATETIME(6) NOT NULL,
      `captured_max_at` DATETIME(6) NOT NULL,
      `provider_observed_max_at` DATETIME(6) NOT NULL,
      `published_at` DATETIME(6) NOT NULL,
      `status` VARCHAR(16) NOT NULL,
      `error_message` VARCHAR(500) NOT NULL DEFAULT '',
      `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
      PRIMARY KEY (`run_id`),
      KEY `idx_field_capture_run_target` (`target_date`, `status`, `decision_at`),
      UNIQUE KEY `uk_field_capture_logical_run`
        (`capture_kind`, `target_date`, `subject_sha256`, `decision_at`),
      KEY `idx_field_capture_run_semantic` (`semantic_sha256`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    f"""
    CREATE TABLE IF NOT EXISTS `{TURNOVER_SNAPSHOT_ROW_TABLE}` (
      `run_id` CHAR(32) NOT NULL,
      `stock_code` VARCHAR(10) NOT NULL,
      `trade_date` DATE NOT NULL,
      `k_type` SMALLINT NOT NULL,
      `adjust_type` SMALLINT NOT NULL,
      `target_row_id` BIGINT DEFAULT NULL,
      `field_value_decimal` DECIMAL(20,6) NOT NULL,
      `source_pre_close` DECIMAL(20,6) DEFAULT NULL,
      `source_lower_limit` DECIMAL(20,6) DEFAULT NULL,
      `source_is_suspended` SMALLINT DEFAULT NULL,
      `source_open` DECIMAL(12,4) DEFAULT NULL,
      `source_high` DECIMAL(12,4) DEFAULT NULL,
      `source_low` DECIMAL(12,4) DEFAULT NULL,
      `source_close` DECIMAL(12,4) DEFAULT NULL,
      `source_volume_shares` DECIMAL(24,4) DEFAULT NULL,
      `source_amount` DECIMAL(24,4) DEFAULT NULL,
      `raw_row_text` VARCHAR(512) NOT NULL,
      `raw_payload` MEDIUMBLOB NOT NULL,
      `raw_payload_sha256` CHAR(64) NOT NULL,
      `snapshot_row_sha256` CHAR(64) NOT NULL,
      `captured_at` DATETIME(6) NOT NULL,
      `provider_observed_at_text` VARCHAR(128) NOT NULL,
      `provider_observed_at` DATETIME(6) NOT NULL,
      `qmt_open` DECIMAL(12,4) DEFAULT NULL,
      `qmt_high` DECIMAL(12,4) DEFAULT NULL,
      `qmt_low` DECIMAL(12,4) DEFAULT NULL,
      `qmt_close` DECIMAL(12,4) DEFAULT NULL,
      `qmt_volume_shares` DECIMAL(24,4) DEFAULT NULL,
      `qmt_amount` DECIMAL(24,4) DEFAULT NULL,
      `qmt_received_at` DATETIME(6) DEFAULT NULL,
      `qmt_data_source` VARCHAR(64) DEFAULT NULL,
      `qmt_batch_id` VARCHAR(128) DEFAULT NULL,
      `qmt_data_version` VARCHAR(128) DEFAULT NULL,
      `qmt_quality_status` VARCHAR(32) DEFAULT NULL,
      `qmt_permission_status` VARCHAR(32) DEFAULT NULL,
      `target_prewrite_sha256` CHAR(64) NOT NULL,
      `target_fact_sha256` CHAR(64) NOT NULL,
      `validation_status` VARCHAR(16) NOT NULL,
      `validation_error` VARCHAR(500) NOT NULL DEFAULT '',
      `promoted_at` DATETIME(6) NOT NULL,
      PRIMARY KEY (`run_id`, `stock_code`, `trade_date`, `k_type`, `adjust_type`),
      UNIQUE KEY `uk_field_capture_run_target_row` (`run_id`, `target_row_id`),
      KEY `idx_field_capture_row_stock_date` (`stock_code`, `trade_date`, `validation_status`),
      CONSTRAINT `fk_field_capture_run`
        FOREIGN KEY (`run_id`) REFERENCES `{TURNOVER_SNAPSHOT_RUN_TABLE}` (`run_id`)
        ON UPDATE RESTRICT ON DELETE RESTRICT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
)

_RUN_IMMUTABLE_COLUMNS = tuple(
    column
    for column in TURNOVER_SNAPSHOT_SCHEMA[TURNOVER_SNAPSHOT_RUN_TABLE].columns
    if column != "status"
)
_RUN_TRANSITION_GUARD = " AND ".join(
    f"(OLD.`{column}` <=> NEW.`{column}`)"
    for column in _RUN_IMMUTABLE_COLUMNS
)

_TRIGGERS = (
    (
        "trg_field_capture_run_no_update",
        f"CREATE TRIGGER `trg_field_capture_run_no_update` BEFORE UPDATE ON "
        f"`{TURNOVER_SNAPSHOT_RUN_TABLE}` FOR EACH ROW BEGIN "
        "IF NOT (OLD.`status`='BUILDING' AND NEW.`status`='COMPLETED' AND "
        f"{_RUN_TRANSITION_GUARD}) THEN SIGNAL SQLSTATE '45000' "
        "SET MESSAGE_TEXT='field capture run is immutable after terminal state'; "
        "END IF; END",
    ),
    (
        "trg_field_capture_run_no_delete",
        f"CREATE TRIGGER `trg_field_capture_run_no_delete` BEFORE DELETE ON "
        f"`{TURNOVER_SNAPSHOT_RUN_TABLE}` FOR EACH ROW SIGNAL SQLSTATE '45000' "
        "SET MESSAGE_TEXT='turnover snapshot run is immutable'",
    ),
    (
        "trg_field_capture_row_insert_open_run",
        f"CREATE TRIGGER `trg_field_capture_row_insert_open_run` BEFORE INSERT ON "
        f"`{TURNOVER_SNAPSHOT_ROW_TABLE}` FOR EACH ROW BEGIN "
        f"IF COALESCE((SELECT `status` FROM `{TURNOVER_SNAPSHOT_RUN_TABLE}` "
        "WHERE `run_id`=NEW.`run_id`), '') <> 'BUILDING' THEN "
        "SIGNAL SQLSTATE '45000' "
        "SET MESSAGE_TEXT='field capture rows require one BUILDING run'; "
        "END IF; END",
    ),
    (
        "trg_field_capture_row_no_update",
        f"CREATE TRIGGER `trg_field_capture_row_no_update` BEFORE UPDATE ON "
        f"`{TURNOVER_SNAPSHOT_ROW_TABLE}` FOR EACH ROW SIGNAL SQLSTATE '45000' "
        "SET MESSAGE_TEXT='turnover snapshot row is immutable'",
    ),
    (
        "trg_field_capture_row_no_delete",
        f"CREATE TRIGGER `trg_field_capture_row_no_delete` BEFORE DELETE ON "
        f"`{TURNOVER_SNAPSHOT_ROW_TABLE}` FOR EACH ROW SIGNAL SQLSTATE '45000' "
        "SET MESSAGE_TEXT='turnover snapshot row is immutable'",
    ),
)


def market_field_capture_trigger_ddl_statements() -> tuple[str, ...]:
    """Return the exact five CREATE statements consumed by the release broker."""

    return tuple(statement for _name, statement in _TRIGGERS)


def _normalized_trigger_body(value: object) -> str:
    return re.sub(r"\s+", "", str(value or "")).replace("`", "").lower()


def _expected_trigger_contracts() -> dict[str, tuple[str, str, str]]:
    contracts: dict[str, tuple[str, str, str]] = {}
    pattern = re.compile(
        r"^CREATE TRIGGER `[^`]+` BEFORE (INSERT|UPDATE|DELETE) ON "
        r"`([^`]+)` FOR EACH ROW (.*)$",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for name, statement in _TRIGGERS:
        matched = pattern.match(statement.strip())
        if matched is None:
            raise RuntimeError(f"field capture trigger DDL is unparsable: {name}")
        event, table_name, body = matched.groups()
        contracts[name] = (
            table_name.lower(),
            event.upper(),
            _normalized_trigger_body(body),
        )
    return contracts


def validate_market_field_capture_immutability(connection) -> None:
    """Attest the exact five-trigger append-only contract on MySQL."""

    expected = _expected_trigger_contracts()
    names = tuple(expected)
    placeholders = ",".join(f":trigger_{index}" for index in range(len(names)))
    rows = connection.execute(text(
        "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, EVENT_MANIPULATION, "
        "ACTION_TIMING, ACTION_STATEMENT FROM information_schema.TRIGGERS "
        "WHERE TRIGGER_SCHEMA=DATABASE() AND TRIGGER_NAME IN "
        f"({placeholders})"
    ), {
        f"trigger_{index}": name for index, name in enumerate(names)
    }).mappings().all()
    observed = {
        str(row.get("TRIGGER_NAME") or row.get("trigger_name") or ""): row
        for row in rows
    }
    if set(observed) != set(expected):
        raise RuntimeError("market field capture trigger inventory differs")
    for name, (table_name, event, body) in expected.items():
        row = observed[name]
        if (
            str(
                row.get("EVENT_OBJECT_TABLE")
                or row.get("event_object_table")
                or ""
            ).lower()
            != table_name
            or str(
                row.get("EVENT_MANIPULATION")
                or row.get("event_manipulation")
                or ""
            ).upper()
            != event
            or str(
                row.get("ACTION_TIMING") or row.get("action_timing") or ""
            ).upper()
            != "BEFORE"
            or _normalized_trigger_body(
                row.get("ACTION_STATEMENT")
                or row.get("action_statement")
                or ""
            )
            != body
        ):
            raise RuntimeError(
                f"market field capture trigger contract differs: {name}"
            )


def privileged_migrate_market_field_capture_schema(
    engine,
    *,
    install_triggers: bool = False,
) -> dict[str, object]:
    """Create ledger tables while leaving all trigger DDL to the broker."""

    if type(install_triggers) is not bool:
        raise TypeError("install_triggers must be bool")
    if install_triggers:
        raise RuntimeError(
            "market field capture triggers require the frozen release broker"
        )

    with engine.begin() as connection:
        for statement in TURNOVER_SNAPSHOT_DDL:
            connection.execute(text(statement))
        existing_columns = {
            str(row.get("COLUMN_NAME") or row.get("column_name") or "")
            for row in connection.execute(text(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name"
            ), {"table_name": TURNOVER_SNAPSHOT_RUN_TABLE}).mappings().all()
        }
        for column_name, definition in (
            ("subject_payload", "MEDIUMBLOB DEFAULT NULL"),
            ("subject_payload_sha256", "CHAR(64) DEFAULT NULL"),
        ):
            if column_name not in existing_columns:
                connection.execute(text(
                    f"ALTER TABLE `{TURNOVER_SNAPSHOT_RUN_TABLE}` "
                    f"ADD COLUMN `{column_name}` {definition}"
                ))
        privileged_normalize_mysql_storage(connection, TURNOVER_SNAPSHOT_SCHEMA)
        validate_market_field_capture_runtime(
            engine,
            connection=connection,
            require_triggers=False,
        )
    return {
        "tables": tuple(sorted(TURNOVER_SNAPSHOT_SCHEMA)),
        "trigger_names": tuple(sorted(_expected_trigger_contracts())),
        "triggers_installed": False,
        "trigger_installation": "FROZEN_RELEASE_BROKER_REQUIRED",
        "runtime_ddl_required": False,
        "privileged_migration": True,
    }


def validate_market_field_capture_runtime(
    engine,
    *,
    connection=None,
    require_triggers: bool = True,
) -> None:
    """Validate the complete ledger surface using SELECT-only metadata reads."""

    if type(require_triggers) is not bool:
        raise TypeError("require_triggers must be bool")

    validate_runtime_tables(
        engine,
        TURNOVER_SNAPSHOT_SCHEMA,
        context="market_field_capture",
        connection=connection,
    )
    if not require_triggers:
        return
    if str(getattr(getattr(engine, "dialect", None), "name", "")).lower() != "mysql":
        return
    if connection is not None:
        validate_market_field_capture_immutability(connection)
    else:
        with engine.connect() as runtime_connection:
            validate_market_field_capture_immutability(runtime_connection)


# Compatibility aliases for callers developed with the first f61 producer.
privileged_migrate_turnover_snapshot_schema = (
    privileged_migrate_market_field_capture_schema
)
validate_turnover_snapshot_runtime = validate_market_field_capture_runtime


__all__ = [
    "FIELD_CAPTURE_ROW_TABLE",
    "FIELD_CAPTURE_RUN_TABLE",
    "TURNOVER_SNAPSHOT_DDL",
    "TURNOVER_SNAPSHOT_ROW_TABLE",
    "TURNOVER_SNAPSHOT_RUN_TABLE",
    "TURNOVER_SNAPSHOT_SCHEMA",
    "market_field_capture_trigger_ddl_statements",
    "privileged_migrate_market_field_capture_schema",
    "privileged_migrate_turnover_snapshot_schema",
    "validate_market_field_capture_runtime",
    "validate_market_field_capture_immutability",
    "validate_turnover_snapshot_runtime",
]
