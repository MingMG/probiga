"""Database reconciliation for the isolated V2 paper account."""
from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .config import canonical_json_hash
from .domain import money


def _account_open_date(created_at: datetime | date | str) -> date:
    """Return the account's first valid reconciliation date."""
    if isinstance(created_at, datetime):
        return created_at.date()
    if isinstance(created_at, date):
        return created_at
    return datetime.fromisoformat(str(created_at).replace(" ", "T")).date()


def reconcile_account(
    engine: Engine,
    *,
    account_id: str,
    trade_date: date,
) -> dict[str, Any]:
    with engine.begin() as connection:
        account = connection.execute(
            text(
                """
                SELECT initial_cash, cash_balance, peak_equity, status,
                       fee_profile_version, instrument_rule_version, created_at
                FROM st_trade_account_v2
                WHERE account_id = :account_id FOR UPDATE
                """
            ),
            {"account_id": account_id},
        ).mappings().first()
        if not account:
            raise ValueError("V2 account not found")
        account_open_date = _account_open_date(account["created_at"])
        if trade_date < account_open_date:
            # A backdated run before the account existed has no opening
            # balance, orders, fills or positions to reconcile.  It is not a
            # mismatch and, most importantly, must never change the current
            # account status.
            status = "SKIPPED_BEFORE_ACCOUNT_OPEN"
            checks = {
                "account_open_date": account_open_date.isoformat(),
                "cash_difference": "0.00",
                "position_difference": 0,
                "order_difference": 0,
                "duplicate_fill_count": 0,
                "open_quantity": 0,
                "price_snapshot_required": False,
                "current_account_status_unchanged": True,
            }
            version = int(
                connection.execute(
                    text(
                        """
                        SELECT COALESCE(MAX(version), 0) + 1
                        FROM st_reconciliation_v2
                        WHERE account_id = :account_id
                          AND trade_date = :trade_date
                        """
                    ),
                    {"account_id": account_id, "trade_date": trade_date},
                ).scalar()
                or 1
            )
            payload = {
                "account_id": account_id,
                "trade_date": trade_date.isoformat(),
                "version": version,
                "status": status,
                "checks": checks,
            }
            reconciliation_hash = canonical_json_hash(payload)
            connection.execute(
                text(
                    """
                    INSERT INTO st_reconciliation_v2
                    (account_id, trade_date, version, status, cash_difference,
                     equity_difference, position_difference, order_difference,
                     fill_difference, checks_json, reconciliation_hash,
                     created_at)
                    VALUES
                    (:account_id, :trade_date, :version, :status, 0, 0, 0, 0,
                     0, :checks, :hash, :created_at)
                    """
                ),
                {
                    "account_id": account_id,
                    "trade_date": trade_date,
                    "version": version,
                    "status": status,
                    "checks": json.dumps(
                        checks, ensure_ascii=False, sort_keys=True
                    ),
                    "hash": reconciliation_hash,
                    "created_at": datetime.now(),
                },
            )
            return {
                "status": status,
                "account_id": account_id,
                "trade_date": trade_date.isoformat(),
                "version": version,
                "checks": checks,
                "reconciliation_hash": reconciliation_hash,
            }
        cash_rows = connection.execute(
            text(
                """
                SELECT amount, balance_after, occurred_at, cash_event_id
                FROM st_cash_ledger_v2
                WHERE account_id = :account_id
                  AND occurred_at < DATE_ADD(:trade_date, INTERVAL 1 DAY)
                ORDER BY occurred_at, cash_event_id
                """
            ),
            {"account_id": account_id, "trade_date": trade_date},
        ).mappings().all()
        running_cash = Decimal("0")
        cash_chain_difference = Decimal("0")
        for row in cash_rows:
            running_cash = money(
                running_cash + Decimal(str(row["amount"]))
            )
            cash_chain_difference += abs(
                money(
                    Decimal(str(row["balance_after"])) - running_cash
                )
            )
        cash_as_of = (
            money(cash_rows[-1]["balance_after"])
            if cash_rows
            else Decimal("0.00")
        )
        future_cash_events = int(
            connection.execute(
                text(
                    """
                    SELECT COUNT(*) FROM st_cash_ledger_v2
                    WHERE account_id = :account_id
                      AND occurred_at >=
                          DATE_ADD(:trade_date, INTERVAL 1 DAY)
                    """
                ),
                {"account_id": account_id, "trade_date": trade_date},
            ).scalar()
            or 0
        )
        current_cash_difference = (
            money(
                Decimal(str(account["cash_balance"])) - cash_as_of
            )
            if future_cash_events == 0
            else Decimal("0.00")
        )
        cash_difference = money(
            cash_chain_difference + abs(current_cash_difference)
        )
        positions = connection.execute(
            text(
                """
                SELECT stock_code, COALESCE(SUM(remaining_quantity), 0) AS lot_qty
                FROM st_position_lot_v2
                WHERE account_id = :account_id
                GROUP BY stock_code
                """
            ),
            {"account_id": account_id},
        ).mappings().all()
        fill_positions = connection.execute(
            text(
                """
                SELECT stock_code,
                       SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) AS fill_qty
                FROM st_fill_v2
                WHERE account_id = :account_id
                  AND filled_at < DATE_ADD(:trade_date, INTERVAL 1 DAY)
                GROUP BY stock_code
                """
            ),
            {"account_id": account_id, "trade_date": trade_date},
        ).mappings().all()
        lot_map = {str(row["stock_code"]): int(row["lot_qty"] or 0) for row in positions}
        fill_map = {str(row["stock_code"]): int(row["fill_qty"] or 0) for row in fill_positions}
        future_fill_count = int(
            connection.execute(
                text(
                    """
                    SELECT COUNT(*) FROM st_fill_v2
                    WHERE account_id = :account_id
                      AND filled_at >=
                          DATE_ADD(:trade_date, INTERVAL 1 DAY)
                    """
                ),
                {"account_id": account_id, "trade_date": trade_date},
            ).scalar()
            or 0
        )
        materialized_position_check_applied = future_fill_count == 0
        position_difference = (
            sum(
                abs(lot_map.get(code, 0) - fill_map.get(code, 0))
                for code in set(lot_map) | set(fill_map)
            )
            if materialized_position_check_applied
            else 0
        )
        order_difference = int(
            connection.execute(
                text(
                    """
                    SELECT COALESCE(SUM(ABS(o.filled_quantity - COALESCE(f.fill_qty, 0))), 0)
                    FROM st_order_v2 o
                    LEFT JOIN (
                        SELECT order_id, SUM(quantity) AS fill_qty
                        FROM st_fill_v2
                        WHERE filled_at <
                            DATE_ADD(:trade_date, INTERVAL 1 DAY)
                        GROUP BY order_id
                    ) f ON f.order_id = o.order_id
                    WHERE o.account_id = :account_id
                      AND o.created_at <
                          DATE_ADD(:trade_date, INTERVAL 1 DAY)
                    """
                ),
                {
                    "account_id": account_id,
                    "trade_date": trade_date,
                },
            ).scalar()
            or 0
        )
        duplicate_fill_count = int(
            connection.execute(
                text(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT idempotency_key
                        FROM st_fill_v2
                        WHERE account_id = :account_id
                          AND filled_at <
                              DATE_ADD(:trade_date, INTERVAL 1 DAY)
                        GROUP BY idempotency_key HAVING COUNT(*) > 1
                    ) d
                    """
                ),
                {
                    "account_id": account_id,
                    "trade_date": trade_date,
                },
            ).scalar()
            or 0
        )
        open_holdings = {
            code: quantity
            for code, quantity in fill_map.items()
            if quantity > 0
        }
        open_quantity = sum(open_holdings.values())
        price_rows: list[dict[str, Any]] = []
        missing_prices: list[str] = []
        invalid_price_sources: list[str] = []
        market_value = Decimal("0")
        for stock_code, quantity in sorted(open_holdings.items()):
            security_type = str(
                connection.execute(
                    text(
                        """
                        SELECT security_type
                        FROM st_instrument_rule_v2
                        WHERE stock_code = :stock_code
                          AND effective_from <= :trade_date
                          AND (effective_to IS NULL
                               OR effective_to >= :trade_date)
                        ORDER BY effective_from DESC, rule_version DESC
                        LIMIT 1
                        """
                    ),
                    {
                        "stock_code": stock_code,
                        "trade_date": trade_date,
                    },
                ).scalar()
                or ""
            )
            if security_type == "ETF":
                price = connection.execute(
                    text(
                        """
                        SELECT close, data_source, data_version,
                               quality_status, received_at
                        FROM sm_etf_kline
                        WHERE etf_code = :stock_code
                          AND trade_date = :trade_date
                          AND k_type = 1 AND adjust_type = 0
                        LIMIT 1
                        """
                    ),
                    {
                        "stock_code": stock_code,
                        "trade_date": trade_date,
                    },
                ).mappings().first()
                accepted_sources = {"gj_big_qmt_inner"}
            else:
                price = connection.execute(
                    text(
                        """
                        SELECT close, data_source, data_version,
                               quality_status, received_at
                        FROM sm_stock_kline
                        WHERE stock_code = :stock_code
                          AND trade_date = :trade_date
                          AND k_type = 1 AND adjust_type = 0
                        LIMIT 1
                        """
                    ),
                    {
                        "stock_code": stock_code,
                        "trade_date": trade_date,
                    },
                ).mappings().first()
                accepted_sources = {"gj_qmt", "gj_big_qmt_inner"}
            if not price or Decimal(str(price["close"] or 0)) <= 0:
                missing_prices.append(stock_code)
                continue
            source = str(price["data_source"] or "")
            version_value = str(price["data_version"] or "")
            quality = str(price["quality_status"] or "").upper()
            if (
                source not in accepted_sources
                or not version_value
                or quality not in {"PASS", "VALIDATED"}
            ):
                invalid_price_sources.append(stock_code)
            close = Decimal(str(price["close"]))
            position_value = close * quantity
            market_value += position_value
            price_rows.append(
                {
                    "stock_code": stock_code,
                    "quantity": quantity,
                    "close": str(close),
                    "position_value": str(position_value),
                    "data_source": source,
                    "data_version": version_value,
                    "quality_status": quality,
                    "received_at": str(price["received_at"] or ""),
                }
            )
        market_value = money(market_value)
        price_snapshot_complete = (
            not missing_prices and not invalid_price_sources
        )
        price_snapshot_hash = canonical_json_hash(
            {
                "trade_date": trade_date.isoformat(),
                "positions": price_rows,
            }
        )
        equity = money(cash_as_of + market_value)
        existing_equity = connection.execute(
            text(
                """
                SELECT total_equity, price_snapshot_hash
                FROM st_equity_daily_v2
                WHERE account_id = :account_id
                  AND trade_date = :trade_date
                """
            ),
            {"account_id": account_id, "trade_date": trade_date},
        ).mappings().first()
        equity_difference = (
            money(
                equity - Decimal(str(existing_equity["total_equity"]))
            )
            if existing_equity
            else Decimal("0.00")
        )
        current_state_date = (
            future_cash_events == 0 and future_fill_count == 0
        )
        checks = {
            "cash_difference": str(cash_difference),
            "cash_chain_difference": str(
                money(cash_chain_difference)
            ),
            "current_cash_difference": str(current_cash_difference),
            "future_cash_event_count": future_cash_events,
            "position_difference": position_difference,
            "materialized_position_check_applied": (
                materialized_position_check_applied
            ),
            "future_fill_count": future_fill_count,
            "order_difference": order_difference,
            "duplicate_fill_count": duplicate_fill_count,
            "open_quantity": open_quantity,
            "price_snapshot_required": open_quantity > 0,
            "price_snapshot_complete": price_snapshot_complete,
            "missing_close_prices": missing_prices,
            "invalid_close_price_sources": invalid_price_sources,
            "price_snapshot_hash": price_snapshot_hash,
            "market_value": str(market_value),
            "cash_as_of": str(cash_as_of),
            "calculated_equity": str(equity),
            "equity_difference": str(equity_difference),
            "current_account_status_updated": current_state_date,
        }
        status = (
            "PASS"
            if abs(cash_difference) <= Decimal("0.01")
            and position_difference == 0
            and order_difference == 0
            and duplicate_fill_count == 0
            and price_snapshot_complete
            and abs(equity_difference) <= Decimal("0.01")
            else "RECONCILIATION_BLOCKED"
        )
        version = int(
            connection.execute(
                text(
                    """
                    SELECT COALESCE(MAX(version), 0) + 1
                    FROM st_reconciliation_v2
                    WHERE account_id = :account_id AND trade_date = :trade_date
                    """
                ),
                {"account_id": account_id, "trade_date": trade_date},
            ).scalar()
            or 1
        )
        payload = {
            "account_id": account_id,
            "trade_date": trade_date.isoformat(),
            "version": version,
            "status": status,
            "checks": checks,
        }
        reconciliation_hash = canonical_json_hash(payload)
        now = datetime.now()
        connection.execute(
            text(
                """
                INSERT INTO st_reconciliation_v2
                (account_id, trade_date, version, status, cash_difference,
                 equity_difference, position_difference, order_difference,
                 fill_difference, checks_json, reconciliation_hash, created_at)
                VALUES
                (:account_id, :trade_date, :version, :status, :cash_difference,
                 :equity_difference, :position_difference, :order_difference,
                 :fill_difference, :checks, :hash, :created_at)
                """
            ),
            {
                "account_id": account_id,
                "trade_date": trade_date,
                "version": version,
                "status": status,
                "cash_difference": cash_difference,
                "equity_difference": equity_difference,
                "position_difference": position_difference,
                "order_difference": order_difference,
                "fill_difference": duplicate_fill_count,
                "checks": json.dumps(checks, ensure_ascii=False, sort_keys=True),
                "hash": reconciliation_hash,
                "created_at": now,
            },
        )
        if status == "PASS" and not existing_equity:
            peak = max(equity, money(account["peak_equity"]))
            drawdown = (
                (peak - equity) / peak if peak > 0 else Decimal("0")
            )
            connection.execute(
                text(
                    """
                    INSERT INTO st_equity_daily_v2
                    (account_id, trade_date, cash_balance, market_value,
                     receivables, payables, total_equity, peak_equity,
                     drawdown, price_snapshot_hash, created_at)
                    VALUES
                    (:account_id, :trade_date, :cash, :market_value,
                     0, 0, :equity, :peak,
                     :drawdown, :price_hash, :created_at)
                    """
                ),
                {
                    "account_id": account_id,
                    "trade_date": trade_date,
                    "cash": cash_as_of,
                    "market_value": market_value,
                    "equity": equity,
                    "peak": peak,
                    "drawdown": drawdown,
                    "price_hash": price_snapshot_hash,
                    "created_at": now,
                },
            )
        if status != "PASS":
            account_status = "RECONCILIATION_BLOCKED"
        elif (
            not account["fee_profile_version"]
            or not account["instrument_rule_version"]
        ):
            account_status = "CONFIG_BLOCKED"
        else:
            account_status = "ACTIVE"
        if current_state_date:
            connection.execute(
                text(
                    """
                    UPDATE st_trade_account_v2
                    SET status = :status,
                        peak_equity = CASE
                            WHEN :status = 'ACTIVE'
                                 OR :status = 'CONFIG_BLOCKED'
                            THEN GREATEST(peak_equity, :equity)
                            ELSE peak_equity END,
                        updated_at = :updated_at
                    WHERE account_id = :account_id
                    """
                ),
                {
                    "status": account_status,
                    "equity": equity,
                    "updated_at": now,
                    "account_id": account_id,
                },
            )
    return {
        "status": status,
        "account_id": account_id,
        "trade_date": trade_date.isoformat(),
        "version": version,
        "checks": checks,
        "reconciliation_hash": reconciliation_hash,
    }
