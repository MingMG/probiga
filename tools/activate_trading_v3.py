#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.env_config import create_tool_engine, load_project_env


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    load_project_env()
    engine = create_tool_engine()
    try:
        with engine.begin() as connection:
            legacy_strategies = connection.execute(
                text(
                    """
                    SELECT strategy_id, version, lifecycle_status
                    FROM st_strategy_version_v2
                    WHERE strategy_id IN (
                        'sector_preheat',
                        'intraday_dynamic_activation'
                    )
                      AND lifecycle_status IN (
                        'PAPER_TRIAL', 'PAPER_ACTIVE'
                      )
                    """
                )
            ).mappings().all()
            legacy_buy_orders = connection.execute(
                text(
                    """
                    SELECT o.order_id, o.stock_code, o.status,
                           i.strategy_version
                    FROM st_order_v2 o
                    JOIN st_trade_intent_v2 i
                      ON i.intent_id = o.intent_id
                    WHERE o.side = 'BUY'
                      AND o.filled_quantity = 0
                      AND o.status IN (
                          'CREATED', 'RISK_APPROVED', 'QUEUED'
                      )
                      AND (
                          i.strategy_version LIKE 'stock_strategy_v2.%%'
                          OR i.strategy_version LIKE 'sector_preheat_%%'
                          OR i.strategy_version
                              LIKE 'intraday_dynamic_activation_%%'
                      )
                    """
                )
            ).mappings().all()
            real_switch = connection.execute(
                text(
                    """
                    SELECT real_trading_enabled
                    FROM st_trade_account_v2
                    WHERE account_id = 'paper-main-v2'
                    """
                )
            ).scalar()
            if int(real_switch or 0) != 0:
                raise RuntimeError(
                    "paper-main-v2 real_trading_enabled 必须为 0"
                )
            if args.apply:
                connection.execute(
                    text(
                        """
                        UPDATE st_strategy_version_v2
                        SET lifecycle_status = 'SUSPENDED',
                            suspended_at = :updated_at
                        WHERE strategy_id IN (
                            'sector_preheat',
                            'intraday_dynamic_activation'
                        )
                          AND lifecycle_status IN (
                            'PAPER_TRIAL', 'PAPER_ACTIVE'
                          )
                        """
                    ),
                    {
                        "updated_at": datetime.now().replace(
                            microsecond=0
                        )
                    },
                )
                connection.execute(
                    text(
                        """
                        UPDATE st_order_v2 o
                        JOIN st_trade_intent_v2 i
                          ON i.intent_id = o.intent_id
                        SET o.status = 'CANCELLED',
                            o.waiting_reason =
                                'V3_PROFIT_GATE_MIGRATION',
                            o.updated_at = :updated_at
                        WHERE o.side = 'BUY'
                          AND o.filled_quantity = 0
                          AND o.status IN (
                              'CREATED', 'RISK_APPROVED', 'QUEUED'
                          )
                          AND (
                              i.strategy_version
                                  LIKE 'stock_strategy_v2.%%'
                              OR i.strategy_version
                                  LIKE 'sector_preheat_%%'
                              OR i.strategy_version
                                  LIKE 'intraday_dynamic_activation_%%'
                          )
                        """
                    ),
                    {
                        "updated_at": datetime.now().replace(
                            microsecond=0
                        )
                    },
                )
    finally:
        engine.dispose()
    print(json.dumps(
        {
            "status": "applied" if args.apply else "dry_run",
            "legacy_strategies_to_suspend": [
                dict(row) for row in legacy_strategies
            ],
            "legacy_buy_orders_to_cancel": [
                dict(row) for row in legacy_buy_orders
            ],
            "real_trading_enabled": False,
        },
        ensure_ascii=False,
        indent=2,
        default=str,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
