"""Build conservative, versioned instrument rules from audited QMT catalogs."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .config import canonical_json_hash


RULE_VERSION = "instrument_rules_qmt_v2.0.0"


def _catalog_rows(
    engine: Engine,
    *,
    table_name: str,
    required_columns: tuple[str, ...],
    optional_columns: tuple[str, ...],
    where_sql: str = "",
    order_column: str,
) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        available = {
            str(row[0])
            for row in connection.execute(
                text(
                    """
                    SELECT COLUMN_NAME FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = :table_name
                    """
                ),
                {"table_name": table_name},
            ).fetchall()
        }
        missing = sorted(set(required_columns) - available)
        if missing:
            raise RuntimeError(
                f"{table_name} missing required catalog columns: {missing}"
            )
        selected = [
            *required_columns,
            *(column for column in optional_columns if column in available),
        ]
        sql = (
            f"SELECT {', '.join(selected)} FROM {table_name} "
            f"{where_sql} ORDER BY {order_column}"
        )
        return [
            dict(row)
            for row in connection.execute(text(sql)).mappings().all()
        ]


def _stock_rule(row: dict[str, Any], effective_from: date) -> dict[str, Any]:
    code = str(row["stock_code"]).zfill(6)
    short_name = str(row.get("short_name") or "")
    exchange = str(row.get("exchange") or "")
    special = "ST" in short_name.upper()
    if code.startswith("68"):
        permission = "A_SHARE_STAR"
        first_buy_minimum, buy_lot_size = 200, 1
        limit_ratio = Decimal("0.20")
    elif code.startswith("30"):
        permission = "A_SHARE_GEM"
        first_buy_minimum, buy_lot_size = 100, 100
        limit_ratio = Decimal("0.20")
    elif code.startswith("92") or exchange.upper() in {"BJ", "BSE"}:
        permission = "A_SHARE_BSE"
        first_buy_minimum, buy_lot_size = 100, 1
        limit_ratio = Decimal("0.30")
    else:
        permission = "A_SHARE_MAIN"
        first_buy_minimum, buy_lot_size = 100, 100
        limit_ratio = Decimal("0.10")
    if special:
        # Current ST price-limit details require an effective-dated status
        # source. Until that source is registered, prohibit buys rather than
        # infer one universal ST rule.
        limit_ratio = None
    source_hash = canonical_json_hash(
        {
            "source": "si_all_code",
            "source_row": row,
            "rule_version": RULE_VERSION,
            "effective_from": effective_from,
        }
    )
    return {
        "stock_code": code,
        "rule_version": RULE_VERSION,
        "effective_from": effective_from,
        "security_type": "A_SHARE",
        "exchange_code": exchange or ("SH" if code.startswith("6") else "SZ"),
        "can_buy": not special,
        "first_buy_minimum": first_buy_minimum,
        "buy_lot_size": buy_lot_size,
        "sell_lot_size": 1,
        "settlement_days": 1,
        "tick_size": Decimal("0.01"),
        "limit_ratio": limit_ratio,
        "special_treatment": special,
        "permission_required": permission,
        "fee_profile_version": "UNCONFIRMED",
        "source_snapshot_hash": source_hash,
    }


def _etf_rule(row: dict[str, Any], effective_from: date) -> dict[str, Any]:
    code = str(row["etf_code"]).zfill(6)
    exchange = str(row.get("exchange") or "")
    source_hash = canonical_json_hash(
        {
            "source": "si_etf_code",
            "source_row": row,
            "rule_version": RULE_VERSION,
            "effective_from": effective_from,
        }
    )
    return {
        "stock_code": code,
        "rule_version": RULE_VERSION,
        "effective_from": effective_from,
        "security_type": "ETF",
        "exchange_code": exchange or ("SH" if code.startswith("5") else "SZ"),
        "can_buy": str(row.get("status") or "").lower()
        not in {"delisted", "inactive"},
        "first_buy_minimum": 100,
        "buy_lot_size": 100,
        "sell_lot_size": 1,
        # T+1 is deliberately conservative for a mixed ETF universe. A
        # confirmed instrument-specific T+0 classification may reduce it.
        "settlement_days": 1,
        "tick_size": Decimal("0.001"),
        # ETF limit ratios vary by product and must not be guessed.
        "limit_ratio": None,
        "special_treatment": False,
        "permission_required": "ETF",
        "fee_profile_version": "UNCONFIRMED",
        "source_snapshot_hash": source_hash,
    }


def sync_instrument_registry(
    engine: Engine,
    *,
    effective_from: date,
) -> dict[str, Any]:
    stocks = _catalog_rows(
        engine,
        table_name="si_all_code",
        required_columns=("stock_code", "short_name"),
        optional_columns=(
            "exchange",
            "list_date",
            "data_source",
            "source_time",
            "received_at",
            "data_version",
            "quality_status",
            "permission_status",
        ),
        where_sql=(
            "WHERE stock_code REGEXP "
            "'^(00|30|60|68|92)[0-9]{4}$'"
        ),
        order_column="stock_code",
    )
    etfs = _catalog_rows(
        engine,
        table_name="si_etf_code",
        required_columns=("etf_code", "short_name"),
        optional_columns=(
            "exchange",
            "asset_class",
            "list_date",
            "status",
            "primary_source",
            "validation_source",
            "sync_status",
            "updated_at",
        ),
        order_column="etf_code",
    )
    rules = [
        *(_stock_rule(row, effective_from) for row in stocks),
        *(_etf_rule(row, effective_from) for row in etfs),
    ]
    inserted = 0
    now = datetime.now()
    with engine.begin() as connection:
        for rule in rules:
            result = connection.execute(
                text(
                    """
                    INSERT IGNORE INTO st_instrument_rule_v2
                    (stock_code, rule_version, effective_from, effective_to,
                     security_type, exchange_code, can_buy,
                     first_buy_minimum, buy_lot_size, sell_lot_size,
                     settlement_days, tick_size, limit_ratio,
                     special_treatment, suspended, permission_required,
                     permission_confirmed, fee_profile_version,
                     source_snapshot_hash, created_at)
                    VALUES
                    (:stock_code, :rule_version, :effective_from, NULL,
                     :security_type, :exchange_code, :can_buy,
                     :first_buy_minimum, :buy_lot_size, :sell_lot_size,
                     :settlement_days, :tick_size, :limit_ratio,
                     :special_treatment, 0, :permission_required,
                     0, :fee_profile_version, :source_snapshot_hash,
                     :created_at)
                    """
                ),
                {**rule, "created_at": now},
            )
            inserted += max(0, int(result.rowcount or 0))
    return {
        "status": "ok",
        "rule_version": RULE_VERSION,
        "effective_from": effective_from.isoformat(),
        "stock_rules": len(stocks),
        "etf_rules": len(etfs),
        "inserted": inserted,
        "permission_confirmed": False,
        "fee_profile_confirmed": False,
    }
