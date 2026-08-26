"""Privileged schema migration and read-only runtime contract for sim trading.

The simulation engine is a normal application writer, not a schema owner.
Persistent DDL is therefore exposed only through
``privileged_migrate_sim_trade_schema`` for the fenced release window.  Every
runtime/API entry point calls ``validate_sim_trade_runtime_schema`` and fails
closed before reading or writing if the installed physical contract drifts.
"""
from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from sqlalchemy import text

from server.common.schema_recovery_evidence import (
    ensure_evidence_table,
    load_pending_physical_rewrite_plan,
    make_evidence_record,
    persist_and_verify_evidence,
    plan_sha256,
    sha256_json,
    table_content_fingerprint,
    verify_pending_plan_content,
)


EXPECTED_ENGINE = "InnoDB"
EXPECTED_COLLATION = "utf8mb4_unicode_ci"
RECOVERY_VERSION = "sim-trade-legacy-physical-normalization.v1"
WATCHLIST_MANUAL_RECOVERY_VERSION = (
    "sim-trade-watchlist-manual-bookkeeping.v1"
)
WATCHLIST_MANUAL_PLAN_ACTION = "WATCHLIST_MANUAL_PLAN"
WATCHLIST_MANUAL_VERIFIED_ACTION = "WATCHLIST_MANUAL_VERIFIED"
WATCHLIST_MANUAL_RECEIPT_ACTION = "WATCHLIST_MANUAL_RECEIPT"
WATCHLIST_MANUAL_TARGET_MODE = "manual_bookkeeping"
_LEGACY_UTF8MB4_COLLATIONS = frozenset(
    {"utf8mb4_general_ci", EXPECTED_COLLATION}
)
_EMPTY_CONTENT_FINGERPRINT = {
    "row_count": 0,
    "content_sha256": (
        "e3b0c44298fc1c149afbf4c8996fb924"
        "27ae41e4649b934ca495991b7852b855"
    ),
}


# One production incident was created by the old watchlist writer relying on
# the st_trade_flow default ``trade_mode='live'``.  The repository freezes only
# the exact ids and irreversible canonical row hashes; customer symbols,
# quantities, prices and timestamps are read under ``FOR UPDATE`` and may exist
# only in the database's append-only recovery evidence.
_KNOWN_WATCHLIST_MANUAL_IDS = (187, 188, 189, 190)
_KNOWN_WATCHLIST_MANUAL_ROW_SHA256: Mapping[int, str] = {
    187: "4f08297a26386f6dbecff6f71ab18bd3c074a8185bf9b637a5dbc22f9ccc8ade",
    188: "7e858a8ea3147740ce0ea0a63f65d5ff996b6bd3a0f7b6db00fd9eb88acba65b",
    189: "69c453bf8509840e2ef31f8810dd75b808d4f233a41d3f629634b4b8f15628b2",
    190: "d15f29003b00a99055d6a65248c1ba429a4b6c58cc7598f7c4ff923a5b25bed6",
}
_KNOWN_WATCHLIST_MANUAL_AGGREGATE_SHA256 = (
    "45414b2cd7d25105056fdbb632351dd2b09f0bc1cc95acc90be590fb9fe518a9"
)
_WATCHLIST_FLOW_COLUMNS = (
    "id",
    "order_id",
    "stock_code",
    "short_name",
    "flow_type",
    "source",
    "strategy_type",
    "trade_mode",
    "trans_type",
    "price",
    "shares",
    "amount",
    "fee",
    "reason",
    "ai_score",
    "trans_date",
    "trans_time",
    "created_at",
)
_WATCHLIST_FACT_COLUMNS = tuple(
    column for column in _WATCHLIST_FLOW_COLUMNS if column != "trade_mode"
)


TABLE_DDL: Mapping[str, str] = {
    "st_sim_position": """
        CREATE TABLE IF NOT EXISTS `st_sim_position` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `signal_id` BIGINT DEFAULT NULL,
            `entry_order_id` BIGINT DEFAULT NULL,
            `exit_order_id` BIGINT DEFAULT NULL,
            `stock_code` VARCHAR(10) NOT NULL,
            `short_name` VARCHAR(20) DEFAULT '',
            `strategy_type` VARCHAR(20) NOT NULL,
            `trade_mode` VARCHAR(20) DEFAULT 'live',
            `buy_price` DECIMAL(12,4) NOT NULL,
            `buy_amount` DECIMAL(14,2) NOT NULL,
            `buy_shares` INT NOT NULL,
            `buy_date` DATE NOT NULL,
            `buy_time` VARCHAR(20) DEFAULT '',
            `buy_reason` TEXT,
            `ai_score` DECIMAL(5,2) DEFAULT 0,
            `short_score` DECIMAL(5,2) DEFAULT 0,
            `long_score` DECIMAL(5,2) DEFAULT 0,
            `capital_score` DECIMAL(5,2) DEFAULT 0,
            `technical_score` DECIMAL(5,2) DEFAULT 0,
            `fundamental_score` DECIMAL(5,2) DEFAULT 0,
            `event_risk_level` VARCHAR(10) DEFAULT 'LOW',
            `status` VARCHAR(20) DEFAULT 'holding',
            `sell_price` DECIMAL(12,4) DEFAULT NULL,
            `sell_date` DATE DEFAULT NULL,
            `sell_time` VARCHAR(20) DEFAULT '',
            `sell_reason` TEXT,
            `profit` DECIMAL(14,2) DEFAULT 0,
            `profit_rate` DECIMAL(8,4) DEFAULT 0,
            `holding_days` INT DEFAULT 0,
            `fee_total` DECIMAL(10,2) DEFAULT 0,
            `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `updated_at` DATETIME DEFAULT NULL,
            PRIMARY KEY (`id`),
            KEY `idx_strategy_status` (`strategy_type`, `status`),
            KEY `idx_trade_mode` (`trade_mode`, `strategy_type`, `status`),
            KEY `idx_stock_code` (`stock_code`),
            KEY `idx_buy_date` (`buy_date`),
            KEY `idx_status` (`status`),
            KEY `idx_sim_position_signal` (`signal_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    "st_trade_flow": """
        CREATE TABLE IF NOT EXISTS `st_trade_flow` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `order_id` BIGINT DEFAULT NULL,
            `stock_code` VARCHAR(10) NOT NULL,
            `short_name` VARCHAR(20) DEFAULT '',
            `flow_type` VARCHAR(20) NOT NULL,
            `source` VARCHAR(20) NOT NULL,
            `strategy_type` VARCHAR(20) DEFAULT '',
            `trade_mode` VARCHAR(20) DEFAULT 'live',
            `trans_type` VARCHAR(10) NOT NULL,
            `price` DECIMAL(12,4) NOT NULL,
            `shares` INT NOT NULL,
            `amount` DECIMAL(14,2) NOT NULL,
            `fee` DECIMAL(10,2) DEFAULT 0,
            `reason` TEXT,
            `ai_score` DECIMAL(5,2) DEFAULT 0,
            `trans_date` DATE NOT NULL,
            `trans_time` VARCHAR(20) DEFAULT '',
            `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            KEY `idx_flow_type` (`flow_type`),
            KEY `idx_source` (`source`),
            KEY `idx_stock_date` (`stock_code`, `trans_date`),
            KEY `idx_trans_date` (`trans_date`),
            KEY `idx_strategy` (`strategy_type`, `trans_date`),
            KEY `idx_trade_mode` (`trade_mode`, `strategy_type`, `trans_date`),
            KEY `idx_trade_flow_order` (`order_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    "st_sim_signal": """
        CREATE TABLE IF NOT EXISTS `st_sim_signal` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `trade_mode` VARCHAR(20) DEFAULT 'live',
            `signal_date` DATE NOT NULL,
            `trade_date` DATE NOT NULL,
            `stock_code` VARCHAR(10) NOT NULL,
            `short_name` VARCHAR(20) DEFAULT '',
            `strategy_type` VARCHAR(20) NOT NULL,
            `status` VARCHAR(20) DEFAULT 'NEW',
            `reason` TEXT,
            `last_check_reason` TEXT,
            `ai_score` DECIMAL(5,2) DEFAULT 0,
            `quality_score` DECIMAL(5,2) DEFAULT 0,
            `entry_score` DECIMAL(5,2) DEFAULT 0,
            `final_trade_score` DECIMAL(5,2) DEFAULT 0,
            `expected_return_pct` DECIMAL(8,4) DEFAULT 0,
            `short_score` DECIMAL(5,2) DEFAULT 0,
            `long_score` DECIMAL(5,2) DEFAULT 0,
            `capital_score` DECIMAL(5,2) DEFAULT 0,
            `technical_score` DECIMAL(5,2) DEFAULT 0,
            `fundamental_score` DECIMAL(5,2) DEFAULT 0,
            `main_wave_score` DECIMAL(5,2) DEFAULT 0,
            `trend_hold_score` DECIMAL(5,2) DEFAULT 0,
            `event_risk_level` VARCHAR(10) DEFAULT 'LOW',
            `entry_price_low` DECIMAL(12,4) DEFAULT NULL,
            `entry_price_high` DECIMAL(12,4) DEFAULT NULL,
            `stop_loss_price` DECIMAL(12,4) DEFAULT NULL,
            `take_profit_1` DECIMAL(12,4) DEFAULT NULL,
            `take_profit_2` DECIMAL(12,4) DEFAULT NULL,
            `intended_amount` DECIMAL(14,2) DEFAULT 0,
            `intended_shares` INT DEFAULT 0,
            `risk_budget_amount` DECIMAL(14,2) DEFAULT 0,
            `risk_budget_note` TEXT,
            `filled_order_id` BIGINT DEFAULT NULL,
            `filled_position_id` BIGINT DEFAULT NULL,
            `pending_order_id` BIGINT DEFAULT NULL,
            `last_check_at` DATETIME DEFAULT NULL,
            `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `updated_at` DATETIME DEFAULT NULL,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uk_sim_signal` (`trade_mode`, `signal_date`, `trade_date`, `stock_code`, `strategy_type`),
            KEY `idx_sim_signal_status` (`trade_mode`, `trade_date`, `status`),
            KEY `idx_sim_signal_stock` (`stock_code`, `trade_date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    "st_sim_order": """
        CREATE TABLE IF NOT EXISTS `st_sim_order` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `signal_id` BIGINT DEFAULT NULL,
            `trade_mode` VARCHAR(20) DEFAULT 'live',
            `order_date` DATE NOT NULL,
            `order_time` VARCHAR(20) DEFAULT '',
            `stock_code` VARCHAR(10) NOT NULL,
            `short_name` VARCHAR(20) DEFAULT '',
            `strategy_type` VARCHAR(20) NOT NULL,
            `side` VARCHAR(10) NOT NULL,
            `order_type` VARCHAR(20) DEFAULT 'SIM_LIMIT',
            `limit_price` DECIMAL(12,4) DEFAULT NULL,
            `target_price` DECIMAL(12,4) DEFAULT NULL,
            `requested_shares` INT DEFAULT 0,
            `remaining_shares` INT DEFAULT 0,
            `status` VARCHAR(20) DEFAULT 'PENDING',
            `filled_price` DECIMAL(12,4) DEFAULT NULL,
            `filled_shares` INT DEFAULT 0,
            `filled_amount` DECIMAL(14,2) DEFAULT 0,
            `fee` DECIMAL(10,2) DEFAULT 0,
            `position_id` BIGINT DEFAULT NULL,
            `source_event` VARCHAR(40) DEFAULT '',
            `price_source` VARCHAR(40) DEFAULT '',
            `risk_budget_amount` DECIMAL(14,2) DEFAULT 0,
            `risk_budget_note` TEXT,
            `match_count` INT DEFAULT 0,
            `reason` TEXT,
            `reject_reason` TEXT,
            `last_match_reason` TEXT,
            `cancel_reason` TEXT,
            `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `updated_at` DATETIME DEFAULT NULL,
            `filled_at` DATETIME DEFAULT NULL,
            PRIMARY KEY (`id`),
            KEY `idx_sim_order_signal` (`signal_id`),
            KEY `idx_sim_order_status` (`trade_mode`, `order_date`, `status`),
            KEY `idx_sim_order_stock` (`stock_code`, `order_date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    "st_sim_event": """
        CREATE TABLE IF NOT EXISTS `st_sim_event` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `trade_mode` VARCHAR(20) DEFAULT 'live',
            `event_date` DATE NOT NULL,
            `event_time` VARCHAR(20) DEFAULT '',
            `event_type` VARCHAR(40) NOT NULL,
            `signal_id` BIGINT DEFAULT NULL,
            `order_id` BIGINT DEFAULT NULL,
            `position_id` BIGINT DEFAULT NULL,
            `stock_code` VARCHAR(10) DEFAULT '',
            `strategy_type` VARCHAR(20) DEFAULT '',
            `severity` VARCHAR(20) DEFAULT 'INFO',
            `message` TEXT,
            `payload` LONGTEXT,
            `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            KEY `idx_sim_event_date` (`trade_mode`, `event_date`, `event_type`),
            KEY `idx_sim_event_stock` (`stock_code`, `event_date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    "st_sim_risk_budget": """
        CREATE TABLE IF NOT EXISTS `st_sim_risk_budget` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `trade_mode` VARCHAR(20) DEFAULT 'live',
            `budget_date` DATE NOT NULL,
            `strategy_type` VARCHAR(20) NOT NULL,
            `initial_capital` DECIMAL(14,2) DEFAULT 0,
            `total_equity` DECIMAL(14,2) DEFAULT 0,
            `cash_available` DECIMAL(14,2) DEFAULT 0,
            `max_total_position_amount` DECIMAL(14,2) DEFAULT 0,
            `max_strategy_amount` DECIMAL(14,2) DEFAULT 0,
            `used_strategy_amount` DECIMAL(14,2) DEFAULT 0,
            `pending_strategy_amount` DECIMAL(14,2) DEFAULT 0,
            `available_strategy_amount` DECIMAL(14,2) DEFAULT 0,
            `risk_budget_note` TEXT,
            `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            `updated_at` DATETIME DEFAULT NULL,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uk_sim_risk_budget` (`trade_mode`, `budget_date`, `strategy_type`),
            KEY `idx_sim_risk_budget_date` (`trade_mode`, `budget_date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    "st_strategy_snapshot": """
        CREATE TABLE IF NOT EXISTS `st_strategy_snapshot` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `snapshot_date` DATE NOT NULL,
            `strategy_type` VARCHAR(20) NOT NULL,
            `total_trades` INT DEFAULT 0,
            `win_count` INT DEFAULT 0,
            `lose_count` INT DEFAULT 0,
            `win_rate` DECIMAL(6,2) DEFAULT 0,
            `total_profit` DECIMAL(14,2) DEFAULT 0,
            `total_fee` DECIMAL(10,2) DEFAULT 0,
            `avg_profit_rate` DECIMAL(8,4) DEFAULT 0,
            `max_profit_rate` DECIMAL(8,4) DEFAULT 0,
            `max_loss_rate` DECIMAL(8,4) DEFAULT 0,
            `holding_count` INT DEFAULT 0,
            `holding_amount` DECIMAL(14,2) DEFAULT 0,
            `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            UNIQUE KEY `uk_date_strategy` (`snapshot_date`, `strategy_type`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
}


# These columns were historically added by application traffic.  They are the
# only missing columns an upgrade may add to an already populated legacy table.
_SAFE_ADDITIVE_COLUMNS = {
    "st_sim_position": {"signal_id", "entry_order_id", "exit_order_id", "trade_mode"},
    "st_trade_flow": {"order_id", "trade_mode"},
    "st_sim_signal": {
        "intended_amount", "intended_shares", "risk_budget_amount",
        "risk_budget_note", "pending_order_id",
    },
    "st_sim_order": {
        "requested_shares", "remaining_shares", "source_event", "price_source",
        "risk_budget_amount", "risk_budget_note", "match_count",
        "last_match_reason", "cancel_reason",
    },
    "st_sim_event": set(),
    "st_sim_risk_budget": set(),
    "st_strategy_snapshot": set(),
}


_COLUMN_RE = re.compile(r"^`(?P<name>[a-z0-9_]+)`\s+(?P<type>[a-z]+(?:\([0-9,]+\))?)(?P<tail>.*)$", re.I)
_INDEX_RE = re.compile(
    r"^(?:(?P<primary>PRIMARY)\s+KEY|(?P<unique>UNIQUE)\s+KEY\s+`[^`]+`|KEY\s+`[^`]+`)\s*\((?P<columns>[^)]+)\)",
    re.I,
)
_DEFAULT_RE = re.compile(
    r"\bDEFAULT\s+(?P<value>NULL|CURRENT_TIMESTAMP(?:\([0-9]+\))?|'(?:''|[^'])*'|-?[0-9]+(?:\.[0-9]+)?)",
    re.I,
)


def _normalize_default(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if normalized.upper() == "NULL":
        return None
    if len(normalized) >= 2 and normalized[0] == normalized[-1] == "'":
        normalized = normalized[1:-1].replace("''", "'")
    timestamp_match = re.fullmatch(
        r"CURRENT_TIMESTAMP(?:\((?P<precision>[0-9]*)\))?",
        normalized,
        re.I,
    )
    if timestamp_match:
        precision = timestamp_match.group("precision")
        return f"current_timestamp({precision})" if precision else "current_timestamp"
    if re.fullmatch(r"-?[0-9]+(?:\.[0-9]+)?", normalized):
        try:
            number = format(Decimal(normalized), "f")
            return number.rstrip("0").rstrip(".") if "." in number else number
        except InvalidOperation:
            pass
    return normalized


def _parse_contract(ddl: str) -> dict[str, Any]:
    columns: dict[str, dict[str, Any]] = {}
    indexes: set[tuple[bool, tuple[str, ...]]] = set()
    for raw_line in ddl.splitlines():
        line = raw_line.strip().rstrip(",")
        column_match = _COLUMN_RE.match(line)
        if column_match:
            name = column_match.group("name")
            tail = column_match.group("tail")
            default_match = _DEFAULT_RE.search(tail)
            base_type = column_match.group("type").split("(", 1)[0].casefold()
            character = base_type in {"char", "varchar", "text", "longtext"}
            columns[name] = {
                "ordinal_position": len(columns) + 1,
                "column_type": column_match.group("type").casefold(),
                "is_nullable": "NO" if re.search(r"\bNOT\s+NULL\b", tail, re.I) else "YES",
                "column_default": _normalize_default(
                    default_match.group("value") if default_match else None
                ),
                "extra": "auto_increment" if re.search(r"\bAUTO_INCREMENT\b", tail, re.I) else "",
                "character_set_name": "utf8mb4" if character else None,
                "collation_name": EXPECTED_COLLATION if character else None,
                "ddl": line,
            }
            continue
        index_match = _INDEX_RE.match(line)
        if index_match:
            index_columns = tuple(
                item.strip().strip("`").casefold()
                for item in index_match.group("columns").split(",")
            )
            indexes.add((bool(index_match.group("primary") or index_match.group("unique")), index_columns))
    if not columns or not indexes:
        raise RuntimeError("sim trade schema source contract is invalid")
    return {"columns": columns, "indexes": indexes}


EXPECTED_CONTRACTS = {
    table_name: _parse_contract(ddl)
    for table_name, ddl in TABLE_DDL.items()
}


def _mapping_value(row: Mapping[str, Any], name: str) -> Any:
    return row.get(name) if name in row else row.get(name.casefold())


def _load_inventory(connection) -> dict[str, Any]:
    table_rows = connection.execute(
        text(
            "SELECT TABLE_NAME, ENGINE, TABLE_COLLATION "
            "FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() "
            "AND TABLE_NAME IN (" + ",".join(f"'{name}'" for name in TABLE_DDL) + ")"
        )
    ).mappings().all()
    tables = {
        str(_mapping_value(row, "TABLE_NAME")): {
            "engine": str(_mapping_value(row, "ENGINE") or ""),
            "collation": str(_mapping_value(row, "TABLE_COLLATION") or ""),
        }
        for row in table_rows
    }
    column_rows = connection.execute(
        text(
            "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_TYPE, IS_NULLABLE, "
            "COLUMN_DEFAULT, EXTRA, CHARACTER_SET_NAME, COLLATION_NAME "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN ("
            + ",".join(f"'{name}'" for name in TABLE_DDL) + ")"
        )
    ).mappings().all()
    columns: dict[str, dict[str, dict[str, Any]]] = {name: {} for name in TABLE_DDL}
    for row in column_rows:
        table_name = str(_mapping_value(row, "TABLE_NAME"))
        column_name = str(_mapping_value(row, "COLUMN_NAME"))
        columns.setdefault(table_name, {})[column_name] = {
            "ordinal_position": int(_mapping_value(row, "ORDINAL_POSITION") or 0),
            "column_type": str(_mapping_value(row, "COLUMN_TYPE") or "").casefold(),
            "is_nullable": str(_mapping_value(row, "IS_NULLABLE") or "").upper(),
            "column_default": _normalize_default(_mapping_value(row, "COLUMN_DEFAULT")),
            "extra": re.sub(
                r"\bdefault_generated\b",
                "",
                str(_mapping_value(row, "EXTRA") or "").casefold(),
            ).strip(),
            "character_set_name": (
                str(_mapping_value(row, "CHARACTER_SET_NAME") or "").casefold()
                or None
            ),
            "collation_name": (
                str(_mapping_value(row, "COLLATION_NAME") or "").casefold()
                or None
            ),
        }
    index_rows = connection.execute(
        text(
            "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME "
            "FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() "
            "AND TABLE_NAME IN (" + ",".join(f"'{name}'" for name in TABLE_DDL) + ") "
            "ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX"
        )
    ).mappings().all()
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in index_rows:
        table_name = str(_mapping_value(row, "TABLE_NAME"))
        index_name = str(_mapping_value(row, "INDEX_NAME"))
        item = grouped.setdefault(
            (table_name, index_name),
            {"unique": int(_mapping_value(row, "NON_UNIQUE") or 0) == 0, "columns": []},
        )
        item["columns"].append(
            (int(_mapping_value(row, "SEQ_IN_INDEX") or 0), str(_mapping_value(row, "COLUMN_NAME")).casefold())
        )
    indexes: dict[str, set[tuple[bool, tuple[str, ...]]]] = {name: set() for name in TABLE_DDL}
    for (table_name, _index_name), item in grouped.items():
        indexes.setdefault(table_name, set()).add(
            (bool(item["unique"]), tuple(column for _seq, column in sorted(item["columns"])))
        )
    return {"tables": tables, "columns": columns, "indexes": indexes}


def validate_sim_trade_runtime_schema(engine, *, connection=None) -> dict[str, Any]:
    """Validate all seven tables without issuing persistent DDL or DML."""

    owns_connection = connection is None
    bound_connection = connection or engine.connect()
    try:
        inventory = _load_inventory(bound_connection)
    finally:
        if owns_connection:
            bound_connection.close()

    errors: list[str] = []
    for table_name, expected in EXPECTED_CONTRACTS.items():
        table = inventory["tables"].get(table_name)
        if table is None:
            errors.append(f"{table_name}:missing-table")
            continue
        if str(table.get("engine") or "").casefold() != EXPECTED_ENGINE.casefold():
            errors.append(f"{table_name}:engine")
        if str(table.get("collation") or "") != EXPECTED_COLLATION:
            errors.append(f"{table_name}:collation")
        actual_columns = inventory["columns"].get(table_name, {})
        # The immutable DDL is a required minimum surface.  Newer execution
        # evidence columns are legitimate and must not be dropped or reordered
        # merely to make a legacy table resemble the old source text.
        for column_name, actual in actual_columns.items():
            if actual.get("character_set_name") is not None and (
                actual.get("character_set_name") != "utf8mb4"
                or actual.get("collation_name") != EXPECTED_COLLATION
            ):
                errors.append(f"{table_name}.{column_name}:character_encoding")
        for column_name, expected_column in expected["columns"].items():
            actual = actual_columns.get(column_name)
            if actual is None:
                errors.append(f"{table_name}.{column_name}:missing")
                continue
            for field in (
                "column_type", "is_nullable",
                "column_default", "extra", "character_set_name", "collation_name",
            ):
                if actual.get(field) != expected_column.get(field):
                    errors.append(f"{table_name}.{column_name}:{field}")
        actual_indexes = inventory["indexes"].get(table_name, set())
        for index_shape in expected["indexes"]:
            if index_shape not in actual_indexes:
                errors.append(f"{table_name}:index:{index_shape}")
    if errors:
        raise RuntimeError("sim trade physical schema differs: " + ",".join(sorted(errors)))
    return {
        "schema": "probiga.sim-trade-physical-contract.v1",
        "status": "HEALTHY",
        "table_count": len(TABLE_DDL),
        "tables": sorted(TABLE_DDL),
        "physical_schema_verified": True,
        "runtime_ddl_required": False,
        "read_only": True,
    }


def _actual_index_shapes(connection, table_name: str) -> set[tuple[bool, tuple[str, ...]]]:
    inventory = _load_inventory(connection)
    return set(inventory["indexes"].get(table_name, set()))


def _row_count(connection, table_name: str) -> int:
    return int(connection.execute(
        text(f"SELECT COUNT(*) FROM `{table_name}`")
    ).scalar() or 0)


def _null_count(connection, table_name: str, column_name: str) -> int:
    return int(connection.execute(text(
        f"SELECT SUM(`{column_name}` IS NULL) FROM `{table_name}`"
    )).scalar() or 0)


def _duplicate_key_exists(
    connection, table_name: str, columns: tuple[str, ...]
) -> bool:
    column_sql = ", ".join(f"`{column}`" for column in columns)
    return connection.execute(text(
        f"SELECT 1 FROM `{table_name}` GROUP BY {column_sql} "
        "HAVING COUNT(*) > 1 LIMIT 1"
    )).first() is not None


def _column_drift_fields(
    actual: Mapping[str, Any], expected: Mapping[str, Any]
) -> tuple[str, ...]:
    return tuple(
        field for field in (
            "column_type", "is_nullable", "column_default", "extra",
            "character_set_name", "collation_name",
        )
        if actual.get(field) != expected.get(field)
    )


def _safe_legacy_column_drift(
    table_name: str,
    column_name: str,
    actual: Mapping[str, Any],
    expected: Mapping[str, Any],
    *,
    created_at_null_count: int,
) -> bool:
    fields = set(_column_drift_fields(actual, expected))
    if not fields:
        return True
    if fields <= {"collation_name"}:
        return (
            actual.get("character_set_name") == "utf8mb4"
            and actual.get("collation_name") in _LEGACY_UTF8MB4_COLLATIONS
        )
    if column_name == "created_at" and fields <= {"is_nullable"}:
        return (
            actual.get("is_nullable") == "YES"
            and expected.get("is_nullable") == "NO"
            and created_at_null_count == 0
        )
    historic_text_widen = (
        (table_name, column_name)
        in {("st_sim_position", "sell_reason"), ("st_trade_flow", "reason")}
        and str(actual.get("column_type") or "").startswith("varchar(")
        and expected.get("column_type") == "text"
    )
    if historic_text_widen:
        remaining = fields - {"column_type", "collation_name"}
        return (
            not remaining
            and actual.get("character_set_name") == "utf8mb4"
            and actual.get("collation_name") in _LEGACY_UTF8MB4_COLLATIONS
        )
    return False


def _build_sim_trade_recovery_plan(connection) -> dict[str, Any]:
    inventory = _load_inventory(connection)
    table_plans: dict[str, dict[str, Any]] = {}
    for table_name, expected in EXPECTED_CONTRACTS.items():
        table = inventory["tables"].get(table_name)
        actual_columns = inventory["columns"].get(table_name, {})
        actual_indexes = set(inventory["indexes"].get(table_name, set()))
        if table is None:
            table_plans[table_name] = {
                "table_exists": False,
                "create_table": True,
                "row_count": 0,
                "extra_columns": [],
                "missing_columns": sorted(expected["columns"]),
                "missing_indexes": sorted(expected["indexes"]),
                "column_drift": {},
                "created_at_null_count": 0,
                "rewrite_required": True,
                "safe_automatic_rewrite": True,
                "before_fingerprint": dict(_EMPTY_CONTENT_FINGERPRINT),
                "fingerprint_columns": [],
            }
            continue

        row_count = _row_count(connection, table_name)
        missing = sorted(set(expected["columns"]) - set(actual_columns))
        unsafe_missing = set(missing) - _SAFE_ADDITIVE_COLUMNS[table_name]
        created_at_null_count = (
            _null_count(connection, table_name, "created_at")
            if "created_at" in actual_columns else 0
        )
        column_drift: dict[str, list[str]] = {}
        unsafe_drift: list[str] = []
        for column_name, expected_column in expected["columns"].items():
            actual = actual_columns.get(column_name)
            if actual is None:
                continue
            fields = list(_column_drift_fields(actual, expected_column))
            if fields:
                column_drift[column_name] = fields
                if not _safe_legacy_column_drift(
                    table_name,
                    column_name,
                    actual,
                    expected_column,
                    created_at_null_count=created_at_null_count,
                ):
                    unsafe_drift.append(column_name)

        encoding_safe = all(
            column.get("character_set_name") is None
            or (
                column.get("character_set_name") == "utf8mb4"
                and column.get("collation_name") in _LEGACY_UTF8MB4_COLLATIONS
            )
            for column in actual_columns.values()
        )
        engine_safe = str(table.get("engine") or "").casefold() == (
            EXPECTED_ENGINE.casefold()
        )
        table_collation_safe = str(table.get("collation") or "") in (
            _LEGACY_UTF8MB4_COLLATIONS
        )
        missing_indexes = sorted(expected["indexes"] - actual_indexes)
        duplicate_unique_indexes = [
            columns for unique, columns in missing_indexes
            if unique and columns != ("id",)
            and _duplicate_key_exists(connection, table_name, columns)
        ]
        primary_missing = (True, ("id",)) in missing_indexes
        safe = not (
            unsafe_missing
            or unsafe_drift
            or not encoding_safe
            or (row_count and not engine_safe)
            or (row_count and not table_collation_safe)
            or duplicate_unique_indexes
            or primary_missing
        )
        rewrite_required = bool(
            missing or missing_indexes or column_drift
            or not engine_safe
            or str(table.get("collation") or "") != EXPECTED_COLLATION
            or any(
                column.get("character_set_name") is not None
                and column.get("collation_name") != EXPECTED_COLLATION
                for column in actual_columns.values()
            )
        )
        fingerprint_columns = sorted(
            actual_columns,
            key=lambda name: int(actual_columns[name].get("ordinal_position") or 0),
        )
        before = (
            table_content_fingerprint(
                connection,
                table_name,
                columns=fingerprint_columns,
            )
            if rewrite_required and safe else None
        )
        table_plans[table_name] = {
            "table_exists": True,
            "create_table": False,
            "row_count": row_count,
            "engine": table.get("engine"),
            "table_collation": table.get("collation"),
            "extra_columns": sorted(set(actual_columns) - set(expected["columns"])),
            "missing_columns": missing,
            "missing_indexes": missing_indexes,
            "column_drift": column_drift,
            "created_at_null_count": created_at_null_count,
            "duplicate_unique_indexes": [list(item) for item in duplicate_unique_indexes],
            "rewrite_required": rewrite_required,
            "safe_automatic_rewrite": safe,
            "before_fingerprint": before,
            "fingerprint_columns": fingerprint_columns,
        }

    public_tables = {
        table: {
            key: value for key, value in detail.items()
            if not key.startswith("_")
        }
        for table, detail in table_plans.items()
    }
    public_payload = {"tables": public_tables}
    return {
        "schema": "probiga.sim-trade-legacy-recovery-plan.v1",
        "recovery_version": RECOVERY_VERSION,
        "table_count": len(table_plans),
        "rewrite_table_count": sum(
            bool(detail["rewrite_required"]) for detail in table_plans.values()
        ),
        "safe_automatic_rewrite": all(
            bool(detail["safe_automatic_rewrite"])
            for detail in table_plans.values()
        ),
        "tables": public_tables,
        "plan_sha256": plan_sha256(
            recovery_version=RECOVERY_VERSION,
            payload=public_payload,
        ),
        "read_only": True,
        "_plan_payload": public_payload,
        "_tables": table_plans,
    }


def _public_sim_trade_plan(plan: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in plan.items() if not key.startswith("_")}


def plan_sim_trade_legacy_recovery(engine) -> dict[str, Any]:
    """Return the deterministic, read-only seven-table recovery plan."""

    with engine.connect() as connection:
        return _public_sim_trade_plan(_build_sim_trade_recovery_plan(connection))


def _decimal_text(value: Any, places: int) -> str:
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise RuntimeError("watchlist off-hours numeric fact differs") from exc
    quantum = Decimal(1).scaleb(-places)
    try:
        return format(decimal_value.quantize(quantum), "f")
    except InvalidOperation as exc:
        raise RuntimeError("watchlist off-hours numeric fact differs") from exc


def _date_text(value: Any) -> str:
    raw = value.isoformat() if hasattr(value, "isoformat") else str(value or "")
    normalized = raw[:10]
    if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", normalized) is None:
        raise RuntimeError("watchlist off-hours date fact differs")
    return normalized


def _datetime_text(value: Any) -> str:
    raw = value.isoformat(sep=" ") if hasattr(value, "isoformat") else str(value or "")
    normalized = raw.replace("T", " ").split(".", 1)[0]
    if re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}",
        normalized,
    ) is None:
        raise RuntimeError("watchlist off-hours created_at fact differs")
    return normalized


def _normalize_watchlist_flow_row(row: Mapping[str, Any]) -> dict[str, Any]:
    missing = set(_WATCHLIST_FLOW_COLUMNS) - set(row)
    if missing:
        raise RuntimeError(
            "watchlist off-hours source row misses fields: "
            + ",".join(sorted(missing))
        )
    order_id = row.get("order_id")
    return {
        "id": int(row.get("id") or 0),
        "order_id": None if order_id is None else int(order_id),
        "stock_code": str(row.get("stock_code") or ""),
        "short_name": str(row.get("short_name") or ""),
        "flow_type": str(row.get("flow_type") or ""),
        "source": str(row.get("source") or ""),
        "strategy_type": str(row.get("strategy_type") or ""),
        "trade_mode": str(row.get("trade_mode") or ""),
        "trans_type": str(row.get("trans_type") or ""),
        "price": _decimal_text(row.get("price"), 4),
        "shares": int(row.get("shares") or 0),
        "amount": _decimal_text(row.get("amount"), 2),
        "fee": _decimal_text(row.get("fee"), 2),
        "reason": None if row.get("reason") is None else str(row.get("reason")),
        "ai_score": _decimal_text(row.get("ai_score"), 2),
        "trans_date": _date_text(row.get("trans_date")),
        "trans_time": str(row.get("trans_time") or ""),
        "created_at": _datetime_text(row.get("created_at")),
    }


def _watchlist_fact_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {column: row[column] for column in _WATCHLIST_FACT_COLUMNS}


def _validate_known_watchlist_manual_hash_manifest() -> None:
    expected_ids = list(_KNOWN_WATCHLIST_MANUAL_IDS)
    if sorted(_KNOWN_WATCHLIST_MANUAL_ROW_SHA256) != expected_ids:
        raise RuntimeError("watchlist manual hash manifest ids differ")
    hash_manifest = {
        str(row_id): _KNOWN_WATCHLIST_MANUAL_ROW_SHA256[row_id]
        for row_id in expected_ids
    }
    if sha256_json(hash_manifest) != _KNOWN_WATCHLIST_MANUAL_AGGREGATE_SHA256:
        raise RuntimeError("watchlist manual hash manifest aggregate differs")


def _is_watchlist_manual_incident_row(row: Mapping[str, Any]) -> bool:
    row_id = int(row.get("id") or 0)
    if row_id not in _KNOWN_WATCHLIST_MANUAL_IDS:
        return False
    trans_type = str(row.get("trans_type") or "")
    expected_flow_type = {
        "buy": "watch_buy",
        "sell": "watch_sell",
    }.get(trans_type)
    if (
        row.get("order_id") is not None
        or str(row.get("source") or "") != "watchlist"
        or str(row.get("strategy_type") or "") != ""
        or str(row.get("flow_type") or "") != expected_flow_type
        or str(row.get("trade_mode") or "")
        not in {"live", WATCHLIST_MANUAL_TARGET_MODE}
    ):
        return False

    match = re.fullmatch(
        r"([0-9]{2}):([0-9]{2})(?::([0-9]{2}))?",
        str(row.get("trans_time") or ""),
    )
    if match is None:
        return False
    hour, minute, second = (int(part or 0) for part in match.groups())
    if hour > 23 or minute > 59 or second > 59:
        return False
    seconds = hour * 3600 + minute * 60 + second
    in_session = (
        9 * 3600 + 25 * 60 <= seconds <= 11 * 3600 + 31 * 60
        or 12 * 3600 + 59 * 60 <= seconds <= 15 * 3600 + 60
    )
    return not in_session


def _load_known_watchlist_manual_rows(
    connection,
    *,
    lock_rows: bool,
) -> list[dict[str, Any]]:
    columns = ", ".join(f"`{column}`" for column in _WATCHLIST_FLOW_COLUMNS)
    lock_sql = " FOR UPDATE" if lock_rows else ""
    id_list = ",".join(str(row_id) for row_id in _KNOWN_WATCHLIST_MANUAL_IDS)
    rows = connection.execute(text(
        f"SELECT {columns} FROM `st_trade_flow` "
        f"WHERE `id` IN ({id_list}) ORDER BY `id`" + lock_sql
    )).mappings().all()
    return [_normalize_watchlist_flow_row(row) for row in rows]


def _count_live_offhours_flows(connection) -> int:
    return int(connection.execute(text(
        "SELECT COUNT(*) FROM `st_trade_flow` "
        "WHERE COALESCE(`trade_mode`, 'live') = 'live' "
        "AND `trans_time` <> '' AND NOT ("
        "(`trans_time` >= '09:25:00' AND `trans_time` <= '11:31:00') OR "
        "(`trans_time` >= '12:59:00' AND `trans_time` <= '15:01:00'))"
    )).scalar() or 0)


def _watchlist_update_bindings(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **{column: row[column] for column in _WATCHLIST_FACT_COLUMNS},
        "source_mode": "live",
        "target_mode": WATCHLIST_MANUAL_TARGET_MODE,
    }


_WATCHLIST_EXACT_UPDATE_SQL = """
UPDATE `st_trade_flow`
SET `trade_mode`=:target_mode
WHERE `id`=:id
  AND `order_id` IS NULL
  AND `stock_code`=:stock_code
  AND `short_name`=:short_name
  AND `flow_type`=:flow_type
  AND `source`=:source
  AND `strategy_type`=:strategy_type
  AND `trade_mode`=:source_mode
  AND `trans_type`=:trans_type
  AND `price`=:price
  AND `shares`=:shares
  AND `amount`=:amount
  AND `fee`=:fee
  AND `reason`=:reason
  AND `ai_score`=:ai_score
  AND `trans_date`=:trans_date
  AND `trans_time`=:trans_time
  AND `created_at`=:created_at
"""


def _repair_known_watchlist_manual_bookkeeping_flows(
    connection,
    *,
    lock_rows: bool = True,
) -> dict[str, Any]:
    """Reclassify only the four hash-bound watchlist bookkeeping rows.

    The caller owns the transaction.  PLAN evidence is persisted and verified
    before the guarded update; VERIFIED evidence and one deterministic receipt
    are persisted after an exact readback.  A second invocation performs no
    update and verifies the same evidence records.
    """

    _validate_known_watchlist_manual_hash_manifest()
    target_ids = list(_KNOWN_WATCHLIST_MANUAL_IDS)
    observed = _load_known_watchlist_manual_rows(
        connection,
        lock_rows=lock_rows,
    )
    matched: dict[int, dict[str, Any]] = {}
    original_by_id: dict[int, dict[str, Any]] = {}
    for row in observed:
        if not _is_watchlist_manual_incident_row(row):
            continue
        row_id = int(row["id"])
        original = {**row, "trade_mode": "live"}
        if sha256_json(original) != _KNOWN_WATCHLIST_MANUAL_ROW_SHA256[row_id]:
            raise RuntimeError(
                f"watchlist manual row {row_id} canonical hash differs"
            )
        matched[row_id] = row
        original_by_id[row_id] = original

    if not matched:
        return {
            "schema": "probiga.sim-trade-watchlist-manual-recovery.v1",
            "status": "NOT_APPLICABLE",
            "target_ids": target_ids,
            "matched_count": 0,
            "reclassified_count": 0,
            "already_reclassified_count": 0,
            "facts_preserved": True,
            "evidence_verified": True,
            "receipt_sha256": None,
        }
    if sorted(matched) != target_ids:
        raise RuntimeError(
            "watchlist manual incident is incomplete; refusing partial repair"
        )

    plan_payload = {
        "schema": "probiga.sim-trade-watchlist-manual-plan.v1",
        "target_ids": target_ids,
        "target_trade_mode": WATCHLIST_MANUAL_TARGET_MODE,
        "mutable_columns": ["trade_mode"],
        "source_row_sha256": {
            str(row_id): _KNOWN_WATCHLIST_MANUAL_ROW_SHA256[row_id]
            for row_id in target_ids
        },
        "source_manifest_sha256": _KNOWN_WATCHLIST_MANUAL_AGGREGATE_SHA256,
        "immutable_fact_sha256": {
            str(row_id): sha256_json(
                _watchlist_fact_payload(original_by_id[row_id])
            )
            for row_id in target_ids
        },
    }
    recovery_plan_hash = plan_sha256(
        recovery_version=WATCHLIST_MANUAL_RECOVERY_VERSION,
        payload=plan_payload,
    )
    plan_records = [
        make_evidence_record(
            recovery_version=WATCHLIST_MANUAL_RECOVERY_VERSION,
            source_table="st_trade_flow",
            source_row_id=row_id,
            action=WATCHLIST_MANUAL_PLAN_ACTION,
            business_key={
                "incident": "watchlist-manual-hash-manifest.v1",
                "id": row_id,
            },
            source_row=original_by_id[row_id],
            plan_payload=plan_payload,
            plan_hash=recovery_plan_hash,
        )
        for row_id in target_ids
    ]
    plan_evidence = persist_and_verify_evidence(connection, plan_records)

    before_fact_hashes = {
        row_id: sha256_json(_watchlist_fact_payload(row))
        for row_id, row in matched.items()
    }
    reclassified_ids: list[int] = []
    for row_id in target_ids:
        row = matched[row_id]
        if row["trade_mode"] == WATCHLIST_MANUAL_TARGET_MODE:
            continue
        result = connection.execute(
            text(_WATCHLIST_EXACT_UPDATE_SQL),
            _watchlist_update_bindings(original_by_id[row_id]),
        )
        if int(result.rowcount or 0) != 1:
            raise RuntimeError(
                f"watchlist manual row {row_id} guarded update missed"
            )
        reclassified_ids.append(row_id)

    after_rows = _load_known_watchlist_manual_rows(
        connection,
        lock_rows=lock_rows,
    )
    after_by_id = {int(row["id"]): row for row in after_rows}
    if sorted(after_by_id) != target_ids:
        raise RuntimeError("watchlist manual repaired row set differs")

    after_fact_hashes: dict[int, str] = {}
    for row_id in target_ids:
        expected_after = {
            **original_by_id[row_id],
            "trade_mode": WATCHLIST_MANUAL_TARGET_MODE,
        }
        actual_after = after_by_id.get(row_id)
        if actual_after != expected_after:
            raise RuntimeError(
                f"watchlist manual row {row_id} readback differs"
            )
        after_fact_hashes[row_id] = sha256_json(
            _watchlist_fact_payload(actual_after)
        )
    if before_fact_hashes != after_fact_hashes:
        raise RuntimeError("watchlist manual immutable facts changed")

    remaining_offhours_live = _count_live_offhours_flows(connection)
    if remaining_offhours_live != 0:
        raise RuntimeError(
            "live off-hours flows remain after exact watchlist manual recovery"
        )

    verified_records = [
        make_evidence_record(
            recovery_version=WATCHLIST_MANUAL_RECOVERY_VERSION,
            source_table="st_trade_flow",
            source_row_id=row_id,
            action=WATCHLIST_MANUAL_VERIFIED_ACTION,
            business_key={
                "incident": "watchlist-manual-hash-manifest.v1",
                "id": row_id,
            },
            source_row=after_by_id[row_id],
            plan_payload=plan_payload,
            plan_hash=recovery_plan_hash,
        )
        for row_id in target_ids
    ]
    receipt_payload = {
        "schema": "probiga.sim-trade-watchlist-manual-receipt.v1",
        "target_ids": target_ids,
        "target_trade_mode": WATCHLIST_MANUAL_TARGET_MODE,
        "source_manifest_sha256": _KNOWN_WATCHLIST_MANUAL_AGGREGATE_SHA256,
        "immutable_fact_sha256": {
            str(row_id): after_fact_hashes[row_id] for row_id in target_ids
        },
        "post_row_sha256": {
            str(row_id): sha256_json(after_by_id[row_id])
            for row_id in target_ids
        },
        "remaining_live_offhours_count": remaining_offhours_live,
        "facts_preserved": True,
    }
    receipt_record = make_evidence_record(
        recovery_version=WATCHLIST_MANUAL_RECOVERY_VERSION,
        source_table="st_trade_flow",
        source_row_id=0,
        action=WATCHLIST_MANUAL_RECEIPT_ACTION,
        business_key={"incident": "watchlist-manual-hash-manifest.v1"},
        source_row=receipt_payload,
        plan_payload=plan_payload,
        plan_hash=recovery_plan_hash,
    )
    verified_evidence = persist_and_verify_evidence(
        connection,
        [*verified_records, receipt_record],
    )
    return {
        "schema": "probiga.sim-trade-watchlist-manual-recovery.v1",
        "status": "VERIFIED",
        "target_ids": target_ids,
        "matched_count": len(target_ids),
        "reclassified_count": len(reclassified_ids),
        "already_reclassified_count": len(target_ids) - len(reclassified_ids),
        "reclassified_ids": reclassified_ids,
        "facts_preserved": True,
        "remaining_live_offhours_count": remaining_offhours_live,
        "plan_sha256": recovery_plan_hash,
        "plan_evidence": plan_evidence,
        "verified_evidence": verified_evidence,
        "receipt_recovery_key": receipt_record["recovery_key"],
        "receipt_sha256": sha256_json(receipt_payload),
    }


def privileged_migrate_sim_trade_schema(engine) -> dict[str, Any]:
    """Create or safely upgrade sim-trade tables in a writer-fenced window."""

    added_columns: list[str] = []
    added_indexes: list[str] = []
    normalized_tables: list[str] = []
    physical_evidence: dict[str, Any] = {}
    with engine.begin() as connection:
        ensure_evidence_table(connection)
        pending_by_table: dict[str, dict[str, Any]] = {}
        initial_inventory = _load_inventory(connection)
        for table_name in TABLE_DDL:
            pending = load_pending_physical_rewrite_plan(
                connection,
                recovery_version=RECOVERY_VERSION,
                source_table=table_name,
            )
            if pending is None:
                continue
            payload_tables = pending["plan_payload"].get("tables")
            if (
                pending["business_key"] != {"table": table_name}
                or int(pending["record"]["source_row_id"]) != 0
                or not isinstance(payload_tables, dict)
                or payload_tables.get(table_name) != pending["source_row"]
            ):
                raise RuntimeError(f"{table_name} pending physical PLAN differs")
            if table_name in initial_inventory["tables"]:
                verify_pending_plan_content(connection, pending)
            elif not (
                pending["source_row"].get("table_exists") is False
                and pending["source_row"].get("before_fingerprint")
                == _EMPTY_CONTENT_FINGERPRINT
            ):
                raise RuntimeError(
                    f"{table_name} disappeared after physical PLAN"
                )
            pending_by_table[table_name] = pending

        recovery_plan = _build_sim_trade_recovery_plan(connection)
        if not recovery_plan["safe_automatic_rewrite"]:
            unsafe = sorted(
                table for table, detail in recovery_plan["_tables"].items()
                if not detail["safe_automatic_rewrite"]
            )
            raise RuntimeError(
                "legacy sim trade recovery plan is unsafe: " + ",".join(unsafe)
            )

        plan_records = []
        physical_contexts = dict(pending_by_table)
        for table_name, detail in recovery_plan["_tables"].items():
            if not detail["rewrite_required"] or table_name in pending_by_table:
                continue
            public_detail = {
                key: value for key, value in detail.items()
                if not key.startswith("_")
            }
            plan_record = make_evidence_record(
                recovery_version=RECOVERY_VERSION,
                source_table=table_name,
                source_row_id=0,
                action="PHYSICAL_REWRITE_PLAN",
                business_key={"table": table_name},
                source_row=public_detail,
                plan_payload=recovery_plan["_plan_payload"],
                plan_hash=recovery_plan["plan_sha256"],
            )
            plan_records.append(plan_record)
            physical_contexts[table_name] = {
                "record": plan_record,
                "business_key": {"table": table_name},
                "source_row": public_detail,
                "plan_payload": recovery_plan["_plan_payload"],
                "plan_sha256": recovery_plan["plan_sha256"],
            }
        plan_evidence = persist_and_verify_evidence(connection, plan_records)

        for table_name, ddl in TABLE_DDL.items():
            connection.execute(text(ddl))
            inventory = _load_inventory(connection)
            actual_columns = inventory["columns"].get(table_name, {})
            expected_columns = EXPECTED_CONTRACTS[table_name]["columns"]
            missing = set(expected_columns) - set(actual_columns)
            unsafe_missing = missing - _SAFE_ADDITIVE_COLUMNS[table_name]
            if unsafe_missing:
                raise RuntimeError(
                    f"legacy sim trade table {table_name} misses non-additive columns: "
                    + ",".join(sorted(unsafe_missing))
                )
            ordered_names = list(expected_columns)
            for column_name in (name for name in ordered_names if name in missing):
                definition = expected_columns[column_name]["ddl"]
                previous_name = ordered_names[ordered_names.index(column_name) - 1]
                connection.execute(
                    text(
                        f"ALTER TABLE `{table_name}` ADD COLUMN {definition} "
                        f"AFTER `{previous_name}`"
                    )
                )
                added_columns.append(f"{table_name}.{column_name}")

            inventory = _load_inventory(connection)
            table = inventory["tables"].get(table_name)
            actual_columns = inventory["columns"].get(table_name, {})
            if table is None:
                raise RuntimeError(f"sim trade table {table_name} is unavailable")
            storage_rewrite = (
                str(table.get("engine") or "").casefold()
                != EXPECTED_ENGINE.casefold()
                or str(table.get("collation") or "") != EXPECTED_COLLATION
                or any(
                    column.get("character_set_name") is not None
                    and column.get("collation_name") != EXPECTED_COLLATION
                    for column in actual_columns.values()
                )
            )
            if storage_rewrite:
                connection.execute(text(
                    f"ALTER TABLE `{table_name}` ENGINE={EXPECTED_ENGINE}, "
                    "CONVERT TO CHARACTER SET utf8mb4 "
                    f"COLLATE {EXPECTED_COLLATION}"
                ))

            inventory = _load_inventory(connection)
            actual_columns = inventory["columns"].get(table_name, {})
            for column_name, expected_column in expected_columns.items():
                actual = actual_columns.get(column_name)
                if actual is None:
                    raise RuntimeError(
                        f"sim trade table {table_name} misses {column_name}"
                    )
                fields = _column_drift_fields(actual, expected_column)
                if not fields:
                    continue
                created_at_null_count = (
                    _null_count(connection, table_name, "created_at")
                    if column_name == "created_at" else 0
                )
                if not _safe_legacy_column_drift(
                    table_name,
                    column_name,
                    actual,
                    expected_column,
                    created_at_null_count=created_at_null_count,
                ):
                    raise RuntimeError(
                        f"unsupported sim trade column drift: "
                        f"{table_name}.{column_name}:{fields}"
                    )
                connection.execute(text(
                    f"ALTER TABLE `{table_name}` MODIFY COLUMN "
                    f"{expected_column['ddl']}"
                ))

            inventory = _load_inventory(connection)
            actual_shapes = set(inventory["indexes"].get(table_name, set()))
            for unique, columns in sorted(EXPECTED_CONTRACTS[table_name]["indexes"]):
                shape = (unique, columns)
                if shape in actual_shapes:
                    continue
                if columns == ("id",) and unique:
                    raise RuntimeError(f"legacy sim trade table {table_name} has no primary id")
                base_name = "uk" if unique else "idx"
                generated_name = f"{base_name}_{table_name[3:]}_{'_'.join(columns)}"[:64]
                kind = "UNIQUE INDEX" if unique else "INDEX"
                quoted_columns = ", ".join(f"`{column}`" for column in columns)
                connection.execute(
                    text(f"ALTER TABLE `{table_name}` ADD {kind} `{generated_name}` ({quoted_columns})")
                )
                actual_shapes.add(shape)
                added_indexes.append(f"{table_name}.{generated_name}")

            context = physical_contexts.get(table_name)
            if context is not None:
                source_detail = context["source_row"]
                before = source_detail["before_fingerprint"]
                after = table_content_fingerprint(
                    connection,
                    table_name,
                    columns=source_detail.get("fingerprint_columns") or None,
                )
                if after != before:
                    raise RuntimeError(
                        f"{table_name} content fingerprint changed during "
                        "legacy physical normalization"
                    )
                verified_payload = {
                    "before_fingerprint": before,
                    "after_fingerprint": after,
                    "fingerprint_columns": source_detail.get(
                        "fingerprint_columns"
                    ),
                    "preserved_extra_columns": source_detail["extra_columns"],
                }
                physical_evidence[table_name] = {
                    **verified_payload,
                    "content_verified": after == before,
                    "plan_sha256": str(context["plan_sha256"]),
                    "resumed_pending_plan": table_name in pending_by_table,
                }
                normalized_tables.append(table_name)

        validate_sim_trade_runtime_schema(None, connection=connection)
        verified_records = []
        for table_name, context in physical_contexts.items():
            verified_records.append(make_evidence_record(
                recovery_version=RECOVERY_VERSION,
                source_table=table_name,
                source_row_id=0,
                action="PHYSICAL_REWRITE_VERIFIED",
                business_key={"table": table_name},
                source_row={
                    key: physical_evidence[table_name][key]
                    for key in (
                        "before_fingerprint",
                        "after_fingerprint",
                        "fingerprint_columns",
                        "preserved_extra_columns",
                    )
                },
                plan_payload=context["plan_payload"],
                plan_hash=str(context["plan_sha256"]),
            ))
        persist_and_verify_evidence(connection, verified_records)
        watchlist_manual_recovery = _repair_known_watchlist_manual_bookkeeping_flows(
            connection
        )

    validated = validate_sim_trade_runtime_schema(engine)
    return {
        **validated,
        "migration_status": "ok",
        "added_columns": added_columns,
        "added_indexes": added_indexes,
        "normalized_tables": sorted(normalized_tables),
        "recovery_plan": _public_sim_trade_plan(recovery_plan),
        "plan_evidence": plan_evidence,
        "physical_rewrite_evidence": physical_evidence,
        "watchlist_manual_bookkeeping_recovery": watchlist_manual_recovery,
    }


__all__ = [
    "EXPECTED_COLLATION",
    "EXPECTED_CONTRACTS",
    "EXPECTED_ENGINE",
    "TABLE_DDL",
    "WATCHLIST_MANUAL_RECOVERY_VERSION",
    "WATCHLIST_MANUAL_TARGET_MODE",
    "_repair_known_watchlist_manual_bookkeeping_flows",
    "plan_sim_trade_legacy_recovery",
    "privileged_migrate_sim_trade_schema",
    "validate_sim_trade_runtime_schema",
]
