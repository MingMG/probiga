#!/usr/bin/env python3
"""Compact read-only audit of current V2 stock-strategy routing."""
from __future__ import annotations

import json
import sys
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.trading_v2.repository import TradingV2ReadRepository


def _load_json(value, fallback):
    if value in (None, ""):
        return fallback
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return fallback


def main() -> int:
    engine = create_batch_engine()
    try:
        with engine.connect() as connection:
            latest = connection.execute(
                text(
                    """
                    SELECT run_uid, trade_date, decision_at, market_regime,
                           status, error_code
                    FROM st_decision_run_v2
                    ORDER BY decision_at DESC, started_at DESC
                    LIMIT 1
                    """
                )
            ).mappings().first()
            if not latest:
                print(json.dumps({"latest_decision": None}, indent=2))
                return 0
            run_uid = str(latest["run_uid"])
            distribution = [
                dict(row)
                for row in connection.execute(
                    text(
                        """
                        SELECT strategy_version, action, competition_status,
                               COALESCE(rejection_code, '') AS rejection_code,
                               COUNT(*) AS signal_count
                        FROM st_strategy_signal_v2
                        WHERE run_uid = :run_uid
                        GROUP BY strategy_version, action,
                                 competition_status,
                                 COALESCE(rejection_code, '')
                        ORDER BY strategy_version, action,
                                 competition_status, rejection_code
                        """
                    ),
                    {"run_uid": run_uid},
                ).mappings()
            ]
            rows = connection.execute(
                text(
                    """
                    SELECT s.strategy_version, s.stock_code,
                           COALESCE(a.short_name, '') AS stock_name,
                           s.action, s.raw_score, s.risk_reward_ratio,
                           s.competition_status,
                           COALESCE(s.rejection_code, '') AS rejection_code,
                           s.raw_features_json
                    FROM st_strategy_signal_v2 s
                    LEFT JOIN si_all_code a
                      ON a.stock_code COLLATE utf8mb4_unicode_ci =
                         s.stock_code COLLATE utf8mb4_unicode_ci
                    WHERE s.run_uid = :run_uid
                      AND (
                        s.action = 'BUY'
                        OR s.competition_status IN (
                            'SELECTED','ELIGIBLE','PAPER_TRIAL_ELIGIBLE'
                        )
                      )
                    ORDER BY s.strategy_version, s.raw_score DESC,
                             s.stock_code
                    """
                ),
                {"run_uid": run_uid},
            ).mappings().all()
            buy_signals = []
            for source in rows:
                row = dict(source)
                raw = _load_json(row.pop("raw_features_json", None), {})
                route = raw.get("paper_trial_route") or {}
                basis = raw.get("signal_basis") or {}
                buy_signals.append(
                    {
                        **row,
                        "strategy_key": raw.get("strategy_key"),
                        "signal_status": raw.get("signal_status"),
                        "gate_status": raw.get("gate_status"),
                        "risk_level": raw.get("risk_level"),
                        "data_quality_score": raw.get(
                            "data_quality_score"
                        ),
                        "effective_weight": raw.get("effective_weight"),
                        "market_only_downgrade": raw.get(
                            "market_only_downgrade"
                        ),
                        "route_reason": route.get("route_reason"),
                        "route_score": route.get("competition_score"),
                        "opening_target_fraction": route.get(
                            "opening_target_fraction"
                        ),
                        "hard_block": basis.get("hard_block"),
                        "strategy_block_hits": basis.get(
                            "strategy_block_hits"
                        ),
                        "signal_reason": basis.get("reason"),
                    }
                )
            pending = [
                dict(row)
                for row in connection.execute(
                    text(
                        """
                        SELECT o.order_id, o.stock_code,
                               COALESCE(a.short_name, '') AS stock_name,
                               o.side, o.status, o.quantity,
                               o.filled_quantity, o.limit_price,
                               o.earliest_at, o.expires_at,
                               i.strategy_version, i.theme_code,
                               i.initial_stop
                        FROM st_order_v2 o
                        JOIN st_trade_intent_v2 i
                          ON i.intent_id = o.intent_id
                        LEFT JOIN si_all_code a
                          ON a.stock_code COLLATE utf8mb4_unicode_ci =
                             o.stock_code COLLATE utf8mb4_unicode_ci
                        WHERE o.status IN (
                            'CREATED','APPROVED','RISK_APPROVED','QUEUED',
                            'SUBMITTED','PARTIALLY_FILLED','WAITING'
                        )
                        ORDER BY o.created_at
                        """
                    )
                ).mappings()
            ]
            tomorrow = TradingV2ReadRepository(engine).tomorrow_action(
                "paper-main-v2"
            )
            tomorrow["watch_candidate_count"] = len(
                tomorrow.get("watch_candidates") or []
            )
            tomorrow.pop("watch_candidates", None)
            output = {
                "latest_decision": dict(latest),
                "distribution": distribution,
                "buy_signals": buy_signals,
                "pending_paper_orders": pending,
                "tomorrow_action": tomorrow,
            }
    finally:
        engine.dispose()
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
