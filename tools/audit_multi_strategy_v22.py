#!/usr/bin/env python3
"""Read-only production acceptance summary for the current V2 stock routing."""
from __future__ import annotations

import json
import sys
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine


def _rows(connection, sql: str) -> list[dict]:
    return [
        dict(row)
        for row in connection.execute(text(sql)).mappings().all()
    ]


def _json_value(value, fallback):
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
            result = {
                "database": connection.execute(
                    text("SELECT DATABASE()")
                ).scalar(),
                "notice_validation": _rows(
                    connection,
                    """
                    SELECT COUNT(*) AS total_rows,
                           SUM(association_validated = 1) AS validated_rows,
                           COUNT(DISTINCT CASE
                               WHEN association_validated = 1
                               THEN stock_code END) AS validated_stocks,
                           MIN(CASE WHEN association_validated = 1
                               THEN notice_date END) AS validated_min_date,
                           MAX(CASE WHEN association_validated = 1
                               THEN notice_date END) AS validated_max_date
                    FROM si_notice_eastmoney
                    """,
                ),
                "notice_recent_runs": _rows(
                    connection,
                    """
                    SELECT status, COUNT(*) AS stock_count,
                           SUM(validated_rows) AS validated_rows
                    FROM si_notice_sync_run
                    WHERE completed_at >= '2026-07-28 01:53:00'
                    GROUP BY status
                    ORDER BY status
                    """,
                ),
                "notice_failed_stocks": _rows(
                    connection,
                    """
                    SELECT stock_code, error_text, completed_at
                    FROM si_notice_sync_run
                    WHERE completed_at >= '2026-07-28 01:53:00'
                      AND status = 'FAILED'
                    ORDER BY completed_at
                    """,
                ),
                "account": _rows(
                    connection,
                    """
                    SELECT account_id, status, initial_cash, cash_balance,
                           fee_profile_version, instrument_rule_version,
                           real_trading_enabled
                    FROM st_trade_account_v2
                    WHERE account_id = 'paper-main-v2'
                    """,
                ),
                "current_stock_strategies": _rows(
                    connection,
                    """
                    SELECT strategy_id, version, lifecycle_status,
                           validation_json, created_at
                    FROM st_strategy_version_v2
                    WHERE version LIKE 'stock_strategy_v2.3.0:%'
                    ORDER BY strategy_id
                    """,
                ),
                "analysis_refresh": _rows(
                    connection,
                    """
                    SELECT pick_date, COUNT(*) AS recommendation_rows,
                           COUNT(DISTINCT stock_code) AS recommendation_stocks,
                           MIN(created_at) AS first_created_at,
                           MAX(created_at) AS last_created_at,
                           SUM(industry_name IS NOT NULL
                               AND industry_name <> '') AS named_industry_rows
                    FROM st_recommended_stocks
                    WHERE pick_date = (
                        SELECT MAX(pick_date) FROM st_recommended_stocks
                    )
                    GROUP BY pick_date
                    """,
                ),
                "recommendation_risk_distribution": _rows(
                    connection,
                    """
                    SELECT event_risk_level, recommend_status, signal_status,
                           COUNT(*) AS stock_count
                    FROM st_recommended_stocks
                    WHERE pick_date = '2026-07-27'
                    GROUP BY event_risk_level, recommend_status, signal_status
                    ORDER BY stock_count DESC
                    """,
                ),
                "latest_decision": _rows(
                    connection,
                    """
                    SELECT run_uid, trade_date, decision_at, market_regime,
                           status, error_code, error_message, finished_at
                    FROM st_decision_run_v2
                    ORDER BY decision_at DESC, started_at DESC
                    LIMIT 1
                    """,
                ),
                "pending_paper_orders": _rows(
                    connection,
                    """
                    SELECT order_id, stock_code, side, status,
                           quantity, filled_quantity, limit_price, created_at
                    FROM st_order_v2
                    WHERE status IN (
                        'CREATED','APPROVED','RISK_APPROVED','QUEUED',
                        'SUBMITTED','PARTIALLY_FILLED','WAITING'
                    )
                    ORDER BY created_at
                    """,
                ),
                "all_paper_order_count": _rows(
                    connection,
                    """
                    SELECT COUNT(*) AS order_count
                    FROM st_order_v2
                    """,
                ),
            }

            latest = result["latest_decision"]
            if latest:
                run_uid = str(latest[0]["run_uid"])
                result["latest_signal_distribution"] = _rows(
                    connection,
                    f"""
                    SELECT strategy_version, action, competition_status,
                           COALESCE(rejection_code, '') AS rejection_code,
                           COUNT(*) AS signal_count
                    FROM st_strategy_signal_v2
                    WHERE run_uid = '{run_uid}'
                    GROUP BY strategy_version, action, competition_status,
                             COALESCE(rejection_code, '')
                    ORDER BY strategy_version, signal_count DESC
                    """,
                )
                signal_rows = _rows(
                    connection,
                    f"""
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
                    WHERE s.run_uid = '{run_uid}'
                    ORDER BY s.strategy_version, s.raw_score DESC,
                             s.stock_code
                    """,
                )
                compact_signals = []
                strategy_sample_counts: dict[str, int] = {}
                for row in signal_rows:
                    strategy_version = str(row.get("strategy_version") or "")
                    competition_status = str(
                        row.get("competition_status") or ""
                    )
                    always_include = competition_status in {
                        "SELECTED",
                        "ELIGIBLE",
                        "PAPER_TRIAL_ELIGIBLE",
                    } or str(row.get("action") or "") == "BUY"
                    sample_count = strategy_sample_counts.get(
                        strategy_version, 0
                    )
                    if not always_include and sample_count >= 5:
                        continue
                    if not always_include:
                        strategy_sample_counts[strategy_version] = (
                            sample_count + 1
                        )
                    raw = _json_value(row.pop("raw_features_json", None), {})
                    route = raw.get("paper_trial_route") or {}
                    basis = raw.get("signal_basis") or {}
                    compact_signals.append(
                        {
                            **row,
                            "strategy_key": raw.get("strategy_key"),
                            "theme_name": raw.get("theme_name"),
                            "route_reason": route.get("reason"),
                            "competition_score": route.get("competition_score"),
                            "opening_target_fraction": route.get(
                                "opening_target_fraction"
                            ),
                            "source_signal_status": basis.get(
                                "source_signal_status"
                            ),
                            "market_only_downgrade": basis.get(
                                "market_only_downgrade"
                            ),
                            "strategy_block_hits": basis.get(
                                "strategy_block_hits"
                            ),
                        }
                    )
                result["latest_signals"] = compact_signals

                plans = _rows(
                    connection,
                    f"""
                    SELECT market_regime, target_cash,
                           target_risk_asset_weight, positions_json,
                           rejected_candidates_json, worst_case_loss,
                           theme_exposure_json, created_at
                    FROM st_portfolio_plan_v2
                    WHERE run_uid = '{run_uid}'
                    ORDER BY created_at DESC
                    """,
                )
                for plan in plans:
                    plan["positions"] = _json_value(
                        plan.pop("positions_json", None), []
                    )
                    rejected = _json_value(
                        plan.pop("rejected_candidates_json", None), []
                    )
                    plan["rejected_candidate_count"] = len(rejected)
                    plan["rejected_candidates_sample"] = rejected[:20]
                    plan["theme_exposure"] = _json_value(
                        plan.pop("theme_exposure_json", None), {}
                    )
                result["latest_plans"] = plans
    finally:
        engine.dispose()
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
