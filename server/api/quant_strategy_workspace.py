"""Read existing version-bound strategy results without running selection."""
from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import text

from server.common.authoritative_market_clock import PRODUCTION_TIMEZONE, authoritative_closed_trade_date


def _json(value, default):
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value or "")
    except (TypeError, ValueError):
        return default


def read_strategy_workspace(engine, trade_date: str = "") -> dict:
    from server.engine.qmt_strategy_simulation import strategy_catalog
    day = trade_date or authoritative_closed_trade_date(engine, now=datetime.now(PRODUCTION_TIMEZONE))
    if not day or datetime.strptime(day, "%Y-%m-%d").date().isoformat() != day:
        raise ValueError("invalid strategy observation date")
    with engine.connect() as connection:
        def rows(sql, params=None):
            return [dict(row) for row in connection.execute(text(sql.replace("SELECT ", "SELECT /*+ MAX_EXECUTION_TIME(4000) */ ", 1)), params or {}).mappings()]
        count = int(rows("SELECT COUNT(*) AS n FROM st_strategy_registry")[0]["n"])
        strategies = rows("""
            SELECT strategy_key, strategy_name, category, family_key, description,
                   current_version, current_status, enabled, status_reason
            FROM st_strategy_registry ORDER BY strategy_key LIMIT :limit
        """, {"limit": count + 1})
        if len(strategies) != count:
            raise ValueError("strategy registry changed during read; refresh required")
        runs = rows("""
            SELECT run_uid, trade_date, market_state, input_ready, status,
                   strategy_count, combination_count, finished_at
            FROM st_strategy_governance_run
            WHERE is_canonical=1 AND status='COMPLETED' AND trade_date<=:day
            ORDER BY trade_date DESC, created_at DESC, run_uid DESC LIMIT 1
        """, {"day": day})
        run = runs[0] if runs else {}
        health, picks = [], []
        if run:
            health = rows("""
                SELECT h.strategy_key, h.strategy_version, h.trade_date, h.window_days,
                       h.completed_trades, h.coverage_days, h.net_expectancy_pct,
                       h.profit_factor, h.max_drawdown_pct, h.win_rate_pct,
                       h.profit_gate_passed, h.gate_reason, h.market_match_score
                FROM st_strategy_health_snapshot h JOIN st_strategy_registry r
                  ON r.strategy_key=h.strategy_key AND r.current_version=h.strategy_version
                WHERE h.run_uid=:run_uid ORDER BY h.strategy_key,h.window_days
                LIMIT :limit
            """, {"run_uid": run["run_uid"], "limit": count * 3 + 1})
            if len(health) > count * 3:
                raise ValueError("strategy health window inventory differs")
            picks = rows("""
                SELECT pool_level, stock_code, stock_name, dominant_strategy,
                       opportunity_score, execution_score, gate_status, reason_json
                FROM st_strategy_pool_snapshot WHERE run_uid=:run_uid
                ORDER BY pool_level,rank_no,stock_code
            """, {"run_uid": run["run_uid"]})
        tasks = rows("""
            SELECT id,task_type,enabled,last_run_status,last_run_at,last_run_output
            FROM st_scheduled_tasks WHERE task_type IN
              ('strategy_governance_daily','qmt_strategy_simulation_daily','trading_v2_job_worker')
            ORDER BY id
        """)
        external = rows("""
            SELECT snapshot_id,captured_at,context_date
            FROM st_external_market_context
            WHERE context_date=(SELECT MAX(context_date) FROM st_external_market_context)
            ORDER BY captured_at DESC,snapshot_id DESC LIMIT 1
        """)
    by_key = {row["strategy_key"]: row for row in strategies}
    for row in strategies:
        row.update(enabled=bool(row["enabled"]), windows={}, picks=[])
    for item in health:
        row = by_key.get(item["strategy_key"])
        if row is None or item["strategy_version"] != row["current_version"]:
            continue
        window = str(item["window_days"])
        if window in row["windows"]:
            raise ValueError("duplicate strategy health window")
        if not item["completed_trades"]:
            for field in ("net_expectancy_pct", "profit_factor", "max_drawdown_pct", "win_rate_pct"):
                item[field] = None
        row["windows"][window] = item
    for pick in picks:
        row = by_key.get(pick["dominant_strategy"])
        # A pool's family key alone cannot prove the current strategy version.
        if row and row["windows"]:
            row["picks"].append({**{k: v for k, v in pick.items() if k != "reason_json"},
                                 "reasons": _json(pick.get("reason_json"), [])})
    for task in tasks:
        output = str(task.pop("last_run_output") or "")
        receipts = [_json(line, {}) for line in output.splitlines() if line.startswith("{")]
        task["reason"] = next((r["reason"] for r in reversed(receipts)
                               if isinstance(r, dict) and r.get("reason")), "") or next(
            (r["reason_code"] for r in reversed(receipts) if isinstance(r, dict) and r.get("reason_code")), "")
    return {
        "trade_date": day, "registry_observed_at": datetime.now(PRODUCTION_TIMEZONE).isoformat(timespec="seconds"),
        "strategy_count": count, "strategies": strategies, "governance": run,
        "governance_current": bool(run and str(run["trade_date"])[:10] == day and run["input_ready"]),
        "tasks": tasks, "external_observation": external[0] if external else None,
        "simulation_catalog": strategy_catalog(),
        "metric_basis": "扣费后单笔收益均值与已完成交易样本；不是账户累计收益。缺少当前版本证据时保留未知。",
        "automatic_real_order_submission": False, "real_order_authority": False,
    }
