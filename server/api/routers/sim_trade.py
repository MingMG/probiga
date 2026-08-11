# -*- coding: utf-8 -*-
"""
模拟交易 API 路由

提供模拟交易的查询、执行、统计和流水接口。
"""

import json
import logging
import re
from datetime import date, datetime, timedelta

from fastapi import APIRouter, Query
from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.common.kline_data import get_kline_engine, should_use_kline_engine
from server.common.sql_reader import read_sql_rows
from server.api.scheduler_runtime import read_scheduler_heartbeat, scheduler_runtime_info
from server.common.minute_data import get_latest_stock_minute_price, minute_source_info
from server.engine.sim_trade_engine import (
    SimTradeEngine,
    STRATEGY_CONFIG,
    SIM_RISK_CONFIG,
    EXCLUDED_RECOMMEND_PREFIXES,
    MIN_EXECUTABLE_RISK_REWARD,
    SNAPSHOT_FALLBACK_MAX_AGE_MINUTES,
    build_buy_decision,
    build_sell_decision,
    fetch_recommended_candidates,
    _get_current_price,
    _ensure_tables,
    _is_trading_time,
    _intraday_action_window,
    _is_near_limit_down,
)

logger = logging.getLogger(__name__)

router = APIRouter()

SIM_INITIAL_CAPITAL = 1_000_000.0


def _runtime_config_snapshot() -> dict:
    window = _intraday_action_window()
    return {
        "status": "ok",
        "strategy_config": {
            key: dict(value)
            for key, value in STRATEGY_CONFIG.items()
        },
        "risk_config": {
            key: (dict(value) if isinstance(value, dict) else value)
            for key, value in SIM_RISK_CONFIG.items()
        },
        "global_rules": {
            "excluded_recommend_prefixes": list(EXCLUDED_RECOMMEND_PREFIXES),
            "min_executable_risk_reward": MIN_EXECUTABLE_RISK_REWARD,
            "snapshot_fallback_max_age_minutes": SNAPSHOT_FALLBACK_MAX_AGE_MINUTES,
            "buy_trigger_mode": "continuous_market",
            "buy_trigger_label": "交易时段持续根据实时盘面判断",
            "holding_count_limit_enabled": False,
            "same_strategy_duplicate_block": True,
        },
        "intraday_windows": {
            "entry_windows": window.get("entry_windows") or [],
            "exit_windows": window.get("exit_windows") or [],
            "t_windows": window.get("t_windows") or [],
        },
    }


def _read_sql(sql: str, params: dict = None) -> list[dict]:
    engine = get_kline_engine() if should_use_kline_engine(sql) else get_engine()
    return read_sql_rows(engine, sql, params, context="sim_trade_api")


def _exec_sql(sql: str, params: dict = None):
    e = get_engine()
    with e.begin() as c:
        c.execute(text(sql), params)


def _safe_float(v, default=0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def _safe_int(v, default=0) -> int:
    try:
        return int(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def _normalize_trade_mode(trade_mode: str) -> str:
    trade_mode = (trade_mode or "live").strip().lower()
    return trade_mode if trade_mode in ("live", "backtest", "forward") else "live"


def _position_exit_decision(
    engine: SimTradeEngine,
    row: dict,
    current_price: float,
    price_info: dict | None = None,
) -> dict:
    strategy_type = str(row.get("strategy_type") or "")
    cfg = STRATEGY_CONFIG.get(strategy_type) or {}
    buy_price = _safe_float(row.get("buy_price"))
    max_rate = None
    if cfg.get("trailing_activate") is not None and row.get("id") and buy_price > 0:
        max_rate = engine._get_max_profit_rate(
            _safe_int(row.get("id")),
            str(row.get("stock_code") or ""),
            buy_price,
        )
    return build_sell_decision(
        strategy_type,
        buy_price,
        current_price,
        row.get("buy_date"),
        max_profit_rate=max_rate,
        near_limit_down=bool(price_info and _is_near_limit_down(price_info)),
    )


def _sim_trade_task_status(row: dict | None) -> dict:
    if not row:
        return {
            "exists": False,
            "enabled": False,
            "status": "missing",
            "status_label": "任务缺失",
        }
    enabled = int(row.get("enabled") or 0) == 1
    last_status = str(row.get("last_run_status") or "").lower()
    if not enabled:
        status, label = "disabled", "已停用"
    elif last_status in {"failed", "timeout", "stopped"}:
        status, label = "error", "最近失败"
    elif last_status == "running":
        status, label = "running", "运行中"
    elif last_status == "success":
        status, label = "ok", "正常"
    else:
        status, label = "idle", "待运行"
    return {
        "exists": True,
        "enabled": enabled,
        "status": status,
        "status_label": label,
        "task_name": row.get("task_name") or "",
        "task_type": row.get("task_type") or "",
        "cron_time": row.get("cron_time") or "",
        "interval_minutes": _safe_int(row.get("interval_minutes")),
        "last_run_status": row.get("last_run_status") or "",
        "last_run_at": str(row.get("last_run_at") or "")[:19],
        "last_triggered_at": str(row.get("last_triggered_at") or "")[:19],
        "last_run_output_tail": str(row.get("last_run_output") or "")[-300:],
    }


def _scheduler_online_status() -> dict:
    info = scheduler_runtime_info()
    heartbeat = read_scheduler_heartbeat()
    poll_seconds = int(info.get("scheduler_poll_seconds") or 60)
    heartbeat_age = None
    if heartbeat and heartbeat.get("heartbeat_age_seconds") is not None:
        try:
            heartbeat_age = int(heartbeat["heartbeat_age_seconds"])
        except Exception:
            heartbeat_age = None
    standalone_online = bool(heartbeat and heartbeat_age is not None and heartbeat_age <= max(180, poll_seconds * 3))
    embedded_running = bool(info.get("embedded_scheduler_running"))
    return {
        "embedded_enabled": bool(info.get("embedded_scheduler_enabled")),
        "embedded_running": embedded_running,
        "standalone_online": standalone_online,
        "api_restart_safe": bool((not bool(info.get("embedded_scheduler_enabled"))) and standalone_online),
        "heartbeat_age_seconds": heartbeat_age,
        "poll_seconds": poll_seconds,
    }


def _extract_signal_date_from_reason(reason: str) -> str:
    text = str(reason or "")
    m = re.search(r"信号日\s*(\d{4}-\d{2}-\d{2})", text)
    return m.group(1) if m else ""


def _infer_position_signal_date(row: dict, trade_mode: str) -> str:
    if row.get("signal_date"):
        return str(row.get("signal_date"))[:10]
    inferred = _extract_signal_date_from_reason(row.get("buy_reason") or row.get("reason") or "")
    if inferred:
        return inferred
    if trade_mode == "live":
        buy_date = row.get("buy_date")
        return str(buy_date)[:10] if buy_date else ""
    return ""


def _candidate_action_rows(signal_date: str, limit: int = 80) -> list[dict]:
    recs = fetch_recommended_candidates(signal_date)
    rows = []
    max_rows = max(1, min(int(limit or 80), 500))
    for rec in recs[:max_rows]:
        code = str(rec.get("stock_code") or "").zfill(6)
        primary = str(rec.get("primary_strategy") or "").strip()
        evaluations = []
        allowed = []
        rejected = []
        for stype, cfg in STRATEGY_CONFIG.items():
            decision = build_buy_decision(stype, rec)
            item = {
                "strategy_type": stype,
                "strategy_name": cfg["name"],
                "allowed": bool(decision.get("allowed")),
                "reason": decision.get("reason", ""),
            }
            evaluations.append(item)
            if item["allowed"]:
                allowed.append(item)
            elif len(rejected) < 2:
                rejected.append(item)

        preferred = None
        if primary:
            preferred = next((x for x in allowed if x["strategy_type"] == primary), None)
        if not preferred and allowed:
            preferred = allowed[0]

        signal_status = str(rec.get("signal_status") or "").upper()
        main_wave_signal = str(rec.get("main_wave_signal") or "").upper()
        if preferred:
            action = "BUY_READY"
            action_label = "可模拟买入"
            action_reason = preferred["reason"]
        elif signal_status == "SELL_ALERT" or main_wave_signal in {"REDUCE", "SELL_ALERT"}:
            action = "SELL_ALERT"
            action_label = "卖点提醒"
            action_reason = rec.get("main_wave_reason") or rec.get("reason") or "AI提示卖点/减仓"
        else:
            action = "WAIT"
            action_label = "等待买点"
            action_reason = (rejected[0]["reason"] if rejected else rec.get("reason") or "")

        rows.append({
            "stock_code": code,
            "short_name": rec.get("short_name") or code,
            "primary_strategy": primary,
            "preferred_strategy": (preferred or {}).get("strategy_type", ""),
            "preferred_strategy_name": (preferred or {}).get("strategy_name", ""),
            "allowed_strategies": allowed,
            "rejected_samples": rejected,
            "action": action,
            "action_label": action_label,
            "action_reason": action_reason,
            "ai_score": round(_safe_float(rec.get("ai_score")), 2),
            "quality_score": round(_safe_float(rec.get("quality_score")), 2),
            "entry_score": round(_safe_float(rec.get("entry_score")), 2),
            "final_trade_score": round(_safe_float(rec.get("final_trade_score")), 2),
            "expected_return_pct": round(_safe_float(rec.get("expected_return_pct")), 2),
            "risk_reward_ratio": round(_safe_float(rec.get("risk_reward_ratio")), 2),
            "sector_gate_status": rec.get("sector_gate_status") or "WATCH",
            "sector_gate_reason": rec.get("sector_gate_reason") or "",
            "evidence_chain_json": rec.get("evidence_chain_json") or "[]",
            "failure_tags_json": rec.get("failure_tags_json") or "[]",
            "main_wave_score": round(_safe_float(rec.get("main_wave_score")), 2),
            "trend_hold_score": round(_safe_float(rec.get("trend_hold_score")), 2),
            "signal_status": signal_status or "-",
            "main_wave_signal": main_wave_signal or "-",
            "entry_price_low": rec.get("entry_price_low"),
            "entry_price_high": rec.get("entry_price_high"),
            "stop_loss_price": rec.get("stop_loss_price"),
            "take_profit_1": rec.get("take_profit_1"),
            "take_profit_2": rec.get("take_profit_2"),
            "trend_stop_price": rec.get("trend_stop_price"),
            "trend_reduce_price": rec.get("trend_reduce_price"),
            "evaluations": evaluations,
        })

    rows.sort(key=lambda r: (
        0 if r["action"] == "BUY_READY" else 1 if r["action"] == "WAIT" else 2,
        -max(_safe_float(r.get("final_trade_score")), _safe_float(r.get("main_wave_score"))),
    ))
    return rows


def _recommendation_window_returns(signal_date: str, candidate_rows: list[dict]) -> dict:
    codes = sorted({str(r.get("stock_code") or "").zfill(6) for r in candidate_rows if r.get("stock_code")})
    if not signal_date or not codes:
        return {"windows": {}, "sample_count": 0}
    placeholders = ", ".join(f":c{i}" for i in range(len(codes)))
    params = {"signal_date": signal_date, **{f"c{i}": code for i, code in enumerate(codes)}}
    rows = _read_sql(f"""
        SELECT stock_code, trade_date, close
        FROM sm_stock_kline
        WHERE k_type = 1
          AND adjust_type = 0
          AND stock_code IN ({placeholders})
          AND trade_date >= :signal_date
          AND trade_date <= DATE_ADD(:signal_date, INTERVAL 30 DAY)
        ORDER BY stock_code, trade_date
    """, params)
    if not rows:
        return {"windows": {}, "sample_count": 0}

    by_code: dict[str, list[dict]] = {}
    for row in rows:
        code = str(row.get("stock_code") or "").zfill(6)
        by_code.setdefault(code, []).append(row)

    windows = {1: [], 3: [], 5: [], 10: []}
    for code, items in by_code.items():
        ordered = sorted(items, key=lambda x: str(x.get("trade_date") or ""))
        base = _safe_float(ordered[0].get("close")) if ordered else 0.0
        if base <= 0:
            continue
        for window in windows:
            if len(ordered) > window:
                close = _safe_float(ordered[window].get("close"))
                if close > 0:
                    windows[window].append((close / base - 1.0) * 100.0)

    summary = {}
    for window, values in windows.items():
        summary[f"{window}d"] = {
            "avg_return_pct": round(sum(values) / len(values), 2) if values else None,
            "win_rate": round(sum(1 for v in values if v > 0) / len(values) * 100, 1) if values else None,
            "sample_count": len(values),
        }
    return {"windows": summary, "sample_count": len(by_code)}


def _summarize_recommendation_outcomes(signal_date: str, trade_mode: str) -> dict:
    candidate_rows = _candidate_action_rows(signal_date, limit=500)
    window_returns = _recommendation_window_returns(signal_date, candidate_rows)
    positions = _read_sql("""
        SELECT stock_code, short_name, strategy_type, status, buy_date, buy_reason,
               sell_date, profit, profit_rate
        FROM st_sim_position
        WHERE COALESCE(trade_mode, 'live') = :mode
        ORDER BY id DESC
    """, {"mode": trade_mode})

    matched_positions = []
    for row in positions:
        if _infer_position_signal_date(row, trade_mode) == signal_date:
            matched_positions.append(row)

    win_count = 0
    closed_count = 0
    total_profit = 0.0
    closed_rates = []
    bought_codes = set()
    bought_pairs = set()
    by_strategy = {}

    for row in matched_positions:
        stype = str(row.get("strategy_type") or "")
        code = str(row.get("stock_code") or "").zfill(6)
        bought_codes.add(code)
        bought_pairs.add((code, stype))
        info = by_strategy.setdefault(stype, {
            "strategy_type": stype,
            "bought_count": 0,
            "closed_count": 0,
            "win_count": 0,
            "win_rate": 0.0,
            "avg_profit_rate": 0.0,
            "total_profit": 0.0,
        })
        info["bought_count"] += 1
        if row.get("status") == "sold":
            profit = _safe_float(row.get("profit"))
            profit_rate = _safe_float(row.get("profit_rate"))
            total_profit += profit
            closed_count += 1
            closed_rates.append(profit_rate)
            info["closed_count"] += 1
            info["total_profit"] += profit
            if profit > 0:
                win_count += 1
                info["win_count"] += 1
            if profit_rate or profit_rate == 0:
                info.setdefault("_rates", []).append(profit_rate)

    for stype, info in by_strategy.items():
        cnt = info["closed_count"]
        rates = info.pop("_rates", [])
        info["win_rate"] = round(info["win_count"] / cnt * 100, 1) if cnt else 0.0
        info["avg_profit_rate"] = round(sum(rates) / len(rates), 2) if rates else 0.0
        info["total_profit"] = round(info["total_profit"], 2)

    buy_ready_rows = [r for r in candidate_rows if r["action"] == "BUY_READY"]
    wait_rows = [r for r in candidate_rows if r["action"] == "WAIT"]
    sell_alert_rows = [r for r in candidate_rows if r["action"] == "SELL_ALERT"]

    return {
        "signal_date": signal_date,
        "trade_mode": trade_mode,
        "total_recommendations": len(candidate_rows),
        "buy_ready_count": len(buy_ready_rows),
        "wait_count": len(wait_rows),
        "sell_alert_count": len(sell_alert_rows),
        "bought_count": len(matched_positions),
        "bought_stock_count": len(bought_codes),
        "closed_count": closed_count,
        "win_count": win_count,
        "win_rate": round(win_count / closed_count * 100, 1) if closed_count else 0.0,
        "avg_profit_rate": round(sum(closed_rates) / len(closed_rates), 2) if closed_rates else 0.0,
        "total_profit": round(total_profit, 2),
        "buy_ready_ratio": round(len(buy_ready_rows) / len(candidate_rows) * 100, 1) if candidate_rows else 0.0,
        "buy_ready_rows": buy_ready_rows[:5],
        "wait_rows": wait_rows[:3],
        "sell_alert_rows": sell_alert_rows[:3],
        "by_strategy": sorted(by_strategy.values(), key=lambda x: (-x["bought_count"], x["strategy_type"])),
        "bought_pairs": [{"stock_code": code, "strategy_type": stype} for code, stype in sorted(bought_pairs)],
        "window_returns": window_returns,
    }


def _recent_closed_trade_rows(trade_mode: str, strategy_type: str = "", days: int = 90) -> list[dict]:
    trade_mode = _normalize_trade_mode(trade_mode)
    since_date = (date.today() - timedelta(days=days)).isoformat()
    where = """
        WHERE status = 'sold'
          AND COALESCE(trade_mode, 'live') = :mode
          AND sell_date >= :since_date
    """
    params = {"mode": trade_mode, "since_date": since_date}
    if strategy_type:
        where += " AND strategy_type = :strategy_type"
        params["strategy_type"] = strategy_type
    return _read_sql(f"""
        SELECT id, strategy_type, profit, profit_rate, sell_date, sell_time
        FROM st_sim_position
        {where}
        ORDER BY sell_date ASC, sell_time ASC, id ASC
    """, params)


def _return_metrics(rows: list[dict]) -> dict:
    rates = [_safe_float(r.get("profit_rate")) for r in rows if r.get("profit_rate") is not None]
    profits = [_safe_float(r.get("profit")) for r in rows if r.get("profit") is not None]
    wins = [r for r in rates if r > 0]
    losses = [r for r in rates if r < 0]
    gross_profit = sum(p for p in profits if p > 0)
    gross_loss = abs(sum(p for p in profits if p < 0))

    avg_return = sum(rates) / len(rates) if rates else 0.0
    avg_win = sum(wins) / len(wins) if wins else 0.0
    avg_loss = abs(sum(losses) / len(losses)) if losses else 0.0
    profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else (avg_win if avg_win > 0 else 0.0)
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else (gross_profit if gross_profit > 0 else 0.0)

    max_drawdown = 0.0
    equity = 1.0
    peak = 1.0
    for rate in rates:
        equity *= max(0.0, 1.0 + rate / 100.0)
        peak = max(peak, equity)
        if peak > 0:
            max_drawdown = max(max_drawdown, (peak - equity) / peak * 100.0)

    sharpe = 0.0
    if len(rates) > 1:
        mean = avg_return
        variance = sum((r - mean) ** 2 for r in rates) / (len(rates) - 1)
        std = variance ** 0.5
        sharpe = (mean / std) * (len(rates) ** 0.5) if std > 0 else 0.0

    return {
        "trades_3m": len(rates),
        "avg_return_3m": round(avg_return, 2),
        "max_drawdown_3m": round(max_drawdown, 2),
        "profit_loss_ratio_3m": round(profit_loss_ratio, 2),
        "sharpe_ratio_3m": round(sharpe, 2),
        "profit_factor_3m": round(profit_factor, 2),
    }


def _previous_trade_date(trade_date: str) -> str:
    rows = _read_sql("""
        SELECT trade_date
        FROM si_trade_calendar
        WHERE trade_status = 1 AND trade_date < :d
        ORDER BY trade_date DESC
        LIMIT 1
    """, {"d": trade_date})
    return str(rows[0]["trade_date"])[:10] if rows else ""


def _next_trade_date(trade_date: str) -> str:
    rows = _read_sql("""
        SELECT trade_date
        FROM si_trade_calendar
        WHERE trade_status = 1 AND trade_date > :d
        ORDER BY trade_date ASC
        LIMIT 1
    """, {"d": trade_date})
    return str(rows[0]["trade_date"])[:10] if rows else ""


def _trade_dates_between(start_date: str, end_date: str) -> list[str]:
    rows = _read_sql("""
        SELECT trade_date
        FROM si_trade_calendar
        WHERE trade_status = 1
          AND trade_date >= :s
          AND trade_date <= :e
        ORDER BY trade_date
    """, {"s": start_date, "e": end_date})
    return [str(r["trade_date"])[:10] for r in rows]


def _trade_mode_stats(trade_mode: str) -> dict:
    trade_mode = _normalize_trade_mode(trade_mode)
    by_strategy = {}
    summary = {
        "total_trades": 0,
        "total_win": 0,
        "win_rate": 0,
        "total_profit": 0.0,
        "total_holding": 0,
    }

    for stype, cfg in STRATEGY_CONFIG.items():
        closed = _read_sql("""
            SELECT COUNT(*) AS cnt,
                   SUM(CASE WHEN profit > 0 THEN 1 ELSE 0 END) AS win_cnt,
                   SUM(CASE WHEN profit <= 0 THEN 1 ELSE 0 END) AS lose_cnt,
                   SUM(profit) AS total_profit,
                   SUM(fee_total) AS total_fee,
                   AVG(profit_rate) AS avg_rate,
                   MAX(profit_rate) AS max_rate,
                   MIN(profit_rate) AS min_rate
            FROM st_sim_position
            WHERE strategy_type = :st AND status = 'sold'
              AND COALESCE(trade_mode, 'live') = :mode
        """, {"st": stype, "mode": trade_mode})
        holdings = _read_sql("""
            SELECT COUNT(*) AS cnt
            FROM st_sim_position
            WHERE strategy_type = :st AND status = 'holding'
              AND COALESCE(trade_mode, 'live') = :mode
        """, {"st": stype, "mode": trade_mode})

        c = closed[0] if closed else {}
        cnt = _safe_int(c.get("cnt"))
        win_cnt = _safe_int(c.get("win_cnt"))
        lose_cnt = _safe_int(c.get("lose_cnt"))
        total_profit = _safe_float(c.get("total_profit"))
        holding_count = _safe_int((holdings[0] if holdings else {}).get("cnt"))

        by_strategy[stype] = {
            "name": cfg["name"],
            "total_trades": cnt,
            "win_count": win_cnt,
            "lose_count": lose_cnt,
            "win_rate": round(win_cnt / cnt * 100, 1) if cnt else 0,
            "total_profit": round(total_profit, 2),
            "total_fee": round(_safe_float(c.get("total_fee")), 2),
            "avg_profit_rate": round(_safe_float(c.get("avg_rate")), 2),
            "max_profit_rate": round(_safe_float(c.get("max_rate")), 2),
            "max_loss_rate": round(_safe_float(c.get("min_rate")), 2),
            "holding_count": holding_count,
            **_return_metrics(_recent_closed_trade_rows(trade_mode, stype)),
        }
        summary["total_trades"] += cnt
        summary["total_win"] += win_cnt
        summary["total_profit"] += total_profit
        summary["total_holding"] += holding_count

    total = summary["total_trades"]
    summary["win_rate"] = round(summary["total_win"] / total * 100, 1) if total else 0
    summary["total_profit"] = round(summary["total_profit"], 2)
    summary.update(_return_metrics(_recent_closed_trade_rows(trade_mode)))
    return {"mode": trade_mode, "summary": summary, "by_strategy": by_strategy}


# ═══════════════════════════════════════════
# 总览仪表盘
# ═══════════════════════════════════════════

def _normalize_strategy_filter(strategy_types: str = "") -> list[str]:
    tokens = [x.strip() for x in re.split(r"[,;|，、\s]+", strategy_types or "") if x.strip()]
    selected = []
    for token in tokens:
        if token in STRATEGY_CONFIG and token not in selected:
            selected.append(token)
    return selected or list(STRATEGY_CONFIG.keys())


def _strategy_filter_sql(selected: list[str]) -> tuple[str, dict]:
    selected = [s for s in selected if s in STRATEGY_CONFIG]
    if not selected or len(selected) == len(STRATEGY_CONFIG):
        return "", {}
    params = {}
    placeholders = []
    for i, stype in enumerate(selected):
        key = f"strategy_{i}"
        params[key] = stype
        placeholders.append(f":{key}")
    return f" AND strategy_type IN ({', '.join(placeholders)})", params


def _date_text(v) -> str:
    if not v:
        return ""
    if hasattr(v, "isoformat"):
        return v.isoformat()[:10]
    return str(v)[:10]


def _profit_distribution(rates: list[float]) -> list[dict]:
    buckets = {"<-10": 0, "-10~-5": 0, "-5~0": 0, "0~5": 0, "5~10": 0, ">10": 0}
    for rate in rates:
        if rate < -10:
            buckets["<-10"] += 1
        elif rate < -5:
            buckets["-10~-5"] += 1
        elif rate < 0:
            buckets["-5~0"] += 1
        elif rate < 5:
            buckets["0~5"] += 1
        elif rate < 10:
            buckets["5~10"] += 1
        else:
            buckets[">10"] += 1
    return [{"range": k, "count": v} for k, v in buckets.items()]


def _calc_trade_metrics(rows: list[dict], initial_capital: float = SIM_INITIAL_CAPITAL) -> dict:
    profits = [_safe_float(r.get("profit")) for r in rows]
    rates = [_safe_float(r.get("profit_rate")) for r in rows]
    wins = [p for p in profits if p > 0]
    losses = [p for p in profits if p < 0]
    win_rates = [r for r in rates if r > 0]
    loss_rates = [r for r in rates if r < 0]

    closed_count = len(rows)
    win_count = len(wins)
    lose_count = closed_count - win_count
    total_profit = sum(profits)
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    avg_win_rate = sum(win_rates) / len(win_rates) if win_rates else 0.0
    avg_loss_rate = abs(sum(loss_rates) / len(loss_rates)) if loss_rates else 0.0

    return {
        "closed_count": closed_count,
        "win_count": win_count,
        "lose_count": lose_count,
        "win_rate": round(win_count / closed_count * 100, 2) if closed_count else 0,
        "total_profit": round(total_profit, 2),
        "total_return_rate": round(total_profit / initial_capital * 100, 2) if initial_capital else 0,
        "avg_profit": round(total_profit / closed_count, 2) if closed_count else 0,
        "avg_profit_rate": round(sum(rates) / closed_count, 2) if closed_count else 0,
        "max_profit_rate": round(max(rates), 2) if rates else 0,
        "max_loss_rate": round(min(rates), 2) if rates else 0,
        "gross_profit": round(gross_profit, 2),
        "gross_loss": round(gross_loss, 2),
        "profit_factor": round(gross_profit / gross_loss, 2) if gross_loss > 0 else (round(gross_profit, 2) if gross_profit > 0 else 0),
        "profit_loss_ratio": round(avg_win_rate / avg_loss_rate, 2) if avg_loss_rate > 0 else (round(avg_win_rate, 2) if avg_win_rate > 0 else 0),
        "avg_holding_days": round(sum(_safe_float(r.get("holding_days")) for r in rows) / closed_count, 1) if closed_count else 0,
        "total_fee": round(sum(_safe_float(r.get("fee_total")) for r in rows), 2),
    }


def _equity_curve(rows: list[dict], initial_capital: float = SIM_INITIAL_CAPITAL) -> tuple[list[dict], list[dict], float, list[dict]]:
    daily = {}
    for row in rows:
        sell_date = _date_text(row.get("sell_date"))
        if not sell_date:
            continue
        if sell_date not in daily:
            daily[sell_date] = {"date": sell_date, "pnl": 0.0, "count": 0}
        daily[sell_date]["pnl"] += _safe_float(row.get("profit"))
        daily[sell_date]["count"] += 1

    daily_pnl = []
    equity_curve = []
    drawdown_curve = []
    equity = float(initial_capital or SIM_INITIAL_CAPITAL)
    peak = equity
    max_drawdown = 0.0

    for day in sorted(daily):
        pnl = round(daily[day]["pnl"], 2)
        equity += pnl
        peak = max(peak, equity)
        drawdown = ((peak - equity) / peak * 100) if peak > 0 else 0.0
        max_drawdown = max(max_drawdown, drawdown)
        daily_pnl.append({"date": day, "pnl": pnl, "count": daily[day]["count"]})
        equity_curve.append({"date": day, "equity": round(equity, 2), "pnl": pnl})
        drawdown_curve.append({"date": day, "drawdown": round(drawdown, 2)})

    return daily_pnl, equity_curve, drawdown_curve, round(max_drawdown, 2)


def _format_backtest_trade(row: dict) -> dict:
    stype = row.get("strategy_type") or ""
    return {
        "id": row.get("id"),
        "stock_code": str(row.get("stock_code") or "").zfill(6),
        "short_name": row.get("short_name") or row.get("stock_code") or "",
        "strategy_type": stype,
        "strategy_name": STRATEGY_CONFIG.get(stype, {}).get("name", stype),
        "status": row.get("status") or "",
        "buy_price": round(_safe_float(row.get("buy_price")), 4),
        "buy_amount": round(_safe_float(row.get("buy_amount")), 2),
        "buy_shares": _safe_int(row.get("buy_shares")),
        "buy_date": _date_text(row.get("buy_date")),
        "sell_price": round(_safe_float(row.get("sell_price")), 4) if row.get("sell_price") is not None else None,
        "sell_date": _date_text(row.get("sell_date")),
        "profit": round(_safe_float(row.get("profit")), 2),
        "profit_rate": round(_safe_float(row.get("profit_rate")), 2),
        "holding_days": _safe_int(row.get("holding_days")),
        "fee_total": round(_safe_float(row.get("fee_total")), 2),
        "ai_score": round(_safe_float(row.get("ai_score")), 2),
        "signal_date": _infer_position_signal_date(row, "backtest"),
        "buy_reason": row.get("buy_reason") or "",
        "sell_reason": row.get("sell_reason") or "",
    }


def _sim_backtest_report(strategy_types: str = "", initial_capital: float = SIM_INITIAL_CAPITAL) -> dict:
    selected = _normalize_strategy_filter(strategy_types)
    filter_sql, filter_params = _strategy_filter_sql(selected)
    params = {"mode": "backtest", **filter_params}

    closed_rows = _read_sql(f"""
        SELECT id, stock_code, short_name, strategy_type, status,
               buy_price, buy_amount, buy_shares, buy_date, buy_reason,
               sell_price, sell_date, sell_reason, profit, profit_rate,
               holding_days, fee_total, ai_score
        FROM st_sim_position
        WHERE status = 'sold'
          AND COALESCE(trade_mode, 'live') = :mode
          {filter_sql}
        ORDER BY sell_date ASC, id ASC
    """, params)

    open_rows = _read_sql(f"""
        SELECT id, stock_code, short_name, strategy_type, status,
               buy_price, buy_amount, buy_shares, buy_date, buy_reason,
               sell_price, sell_date, sell_reason, profit, profit_rate,
               holding_days, fee_total, ai_score
        FROM st_sim_position
        WHERE status = 'holding'
          AND COALESCE(trade_mode, 'live') = :mode
          {filter_sql}
        ORDER BY buy_date DESC, id DESC
        LIMIT 200
    """, params)

    summary = _calc_trade_metrics(closed_rows, initial_capital)
    daily_pnl, curve, drawdowns, max_drawdown = _equity_curve(closed_rows, initial_capital)
    holding_amount = sum(_safe_float(r.get("buy_amount")) for r in open_rows)

    by_strategy = {}
    for stype in selected:
        strategy_rows = [r for r in closed_rows if r.get("strategy_type") == stype]
        strategy_holding = [r for r in open_rows if r.get("strategy_type") == stype]
        item = _calc_trade_metrics(strategy_rows, initial_capital)
        item.update({
            "name": STRATEGY_CONFIG.get(stype, {}).get("name", stype),
            "holding_count": len(strategy_holding),
            "holding_amount": round(sum(_safe_float(r.get("buy_amount")) for r in strategy_holding), 2),
        })
        by_strategy[stype] = item

    summary.update({
        "initial_capital": round(float(initial_capital or SIM_INITIAL_CAPITAL), 2),
        "ending_equity": round(float(initial_capital or SIM_INITIAL_CAPITAL) + _safe_float(summary.get("total_profit")), 2),
        "holding_count": len(open_rows),
        "holding_amount": round(holding_amount, 2),
        "max_drawdown": max_drawdown,
    })

    rates = [_safe_float(r.get("profit_rate")) for r in closed_rows]
    return {
        "status": "ok",
        "mode": "backtest",
        "strategy_types": selected,
        "summary": summary,
        "by_strategy": by_strategy,
        "profit_distribution": _profit_distribution(rates),
        "daily_pnl": daily_pnl,
        "equity_curve": curve,
        "drawdown_curve": drawdowns,
        "recent_trades": [_format_backtest_trade(r) for r in reversed(closed_rows[-80:])],
        "open_positions": [_format_backtest_trade(r) for r in open_rows],
    }


@router.get("/sim-trade/runtime-config")
def sim_trade_runtime_config():
    """Return the effective simulated-trading rule snapshot."""
    return _runtime_config_snapshot()


@router.get("/sim-trade/dashboard")
def sim_trade_dashboard(trade_mode: str = Query(default="live")):
    """模拟交易总览数据"""
    try:
        from server.engine.sim_trade_engine import _ensure_tables
        _ensure_tables()
        trade_mode = _normalize_trade_mode(trade_mode)
        engine = SimTradeEngine()

        result = {"mode": trade_mode, "strategies": {}, "summary": {}}
        total_trades = 0
        total_win = 0
        total_profit = 0.0
        total_holding = 0
        total_holding_amount = 0.0

        for stype, cfg in STRATEGY_CONFIG.items():
            # 已平仓统计
            closed = _read_sql("""
                SELECT COUNT(*) AS cnt,
                       SUM(CASE WHEN profit > 0 THEN 1 ELSE 0 END) AS win_cnt,
                       SUM(CASE WHEN profit <= 0 THEN 1 ELSE 0 END) AS lose_cnt,
                       SUM(profit) AS total_profit,
                       SUM(fee_total) AS total_fee,
                       AVG(profit_rate) AS avg_rate,
                       MAX(profit_rate) AS max_rate,
                       MIN(profit_rate) AS min_rate
                FROM st_sim_position
                WHERE strategy_type = :st AND status = 'sold'
                  AND COALESCE(trade_mode, 'live') = :mode
            """, {"st": stype, "mode": trade_mode})

            # 当前持仓
            holdings = _read_sql("""
                SELECT id, stock_code, short_name, buy_price, buy_shares, buy_date, ai_score
                FROM st_sim_position
                WHERE strategy_type = :st AND status = 'holding'
                  AND COALESCE(trade_mode, 'live') = :mode
                ORDER BY buy_date DESC
            """, {"st": stype, "mode": trade_mode})

            c = closed[0] if closed else {}
            cnt = _safe_int(c.get("cnt"))
            win_cnt = _safe_int(c.get("win_cnt"))
            lose_cnt = _safe_int(c.get("lose_cnt"))
            win_rate = round(win_cnt / cnt * 100, 1) if cnt > 0 else 0
            tp = _safe_float(c.get("total_profit"))
            tf = _safe_float(c.get("total_fee"))
            avg_rate = _safe_float(c.get("avg_rate"))
            max_rate = _safe_float(c.get("max_rate"))
            min_rate = _safe_float(c.get("min_rate"))

            # 计算持仓市值 — 盘中批量拉实时行情
            holding_amount = 0.0
            holding_details = []
            live_prices = {}
            if holdings:
                from server.engine.sim_trade_engine import _is_trading_time, _fetch_live_prices_batch
                if _is_trading_time():
                    live_prices = _fetch_live_prices_batch([h["stock_code"] for h in holdings])

            for h in holdings:
                code = h["stock_code"]
                bp = _safe_float(h["buy_price"])
                bs = _safe_int(h["buy_shares"])
                cost = bp * bs
                bd = h.get("buy_date")
                if isinstance(bd, str):
                    bd_str = bd[:10]
                elif hasattr(bd, 'isoformat'):
                    bd_str = bd.isoformat()
                else:
                    bd_str = str(bd)[:10]

                # 优先用实时行情，降级到日K收盘价
                lp = live_prices.get(code)
                if lp and lp.get("price", 0) > 0:
                    cur_price = lp["price"]
                elif trade_mode == "forward":
                    try:
                        cur_row = get_latest_stock_minute_price(code, bd_str)
                        cur_price = _safe_float(cur_row.get("price")) if cur_row else bp
                    except Exception:
                        cur_price = bp
                else:
                    try:
                        cur_rows = _read_sql("""
                            SELECT close FROM sm_stock_kline
                            WHERE stock_code = :c AND k_type = 1
                            ORDER BY trade_date DESC LIMIT 1
                        """, {"c": code})
                        cur_price = _safe_float(cur_rows[0]["close"]) if cur_rows else bp
                    except Exception:
                        cur_price = bp

                pnl = (cur_price - bp) * bs if bp > 0 else 0
                pnl_rate = ((cur_price - bp) / bp * 100) if bp > 0 else 0
                holding_amount += cur_price * bs
                exit_decision = _position_exit_decision(
                    engine,
                    {**h, "strategy_type": stype},
                    cur_price,
                    lp,
                )

                holding_details.append({
                    "id": h.get("id"),
                    "stock_code": code,
                    "short_name": h.get("short_name", ""),
                    "buy_price": bp,
                    "buy_shares": bs,
                    "buy_date": bd_str,
                    "cur_price": round(cur_price, 2),
                    "pnl": round(pnl, 2),
                    "pnl_rate": round(pnl_rate, 2),
                    "holding_days": (date.today() - datetime.strptime(bd_str, "%Y-%m-%d").date()).days
                        if bd_str else 0,
                    "ai_score": _safe_float(h.get("ai_score")),
                    "exit_action": exit_decision.get("action"),
                    "exit_reason": exit_decision.get("reason"),
                    "exit_reason_detail": exit_decision.get("reason_detail"),
                    "exit_thresholds": {
                        "take_profit_pct": exit_decision.get("take_profit_pct"),
                        "stop_loss_pct": exit_decision.get("stop_loss_pct"),
                        "max_holding_days": exit_decision.get("max_holding_days"),
                        "trailing_activate_pct": exit_decision.get("trailing_activate_pct"),
                        "trailing_drawdown_pct": exit_decision.get("trailing_drawdown_pct"),
                    },
                })

            result["strategies"][stype] = {
                "name": cfg["name"],
                "total_trades": cnt,
                "win_count": win_cnt,
                "lose_count": lose_cnt,
                "win_rate": win_rate,
                "total_profit": round(tp, 2),
                "total_fee": round(tf, 2),
                "avg_profit_rate": round(avg_rate, 2),
                "max_profit_rate": round(max_rate, 2),
                "max_loss_rate": round(min_rate, 2),
                "holding_count": len(holdings),
                "holding_amount": round(holding_amount, 2),
                "holdings": holding_details,
                **_return_metrics(_recent_closed_trade_rows(trade_mode, stype)),
            }

            total_trades += cnt
            total_win += win_cnt
            total_profit += tp
            total_holding += len(holdings)
            total_holding_amount += holding_amount

        result["summary"] = {
            "total_trades": total_trades,
            "total_win": total_win,
            "win_rate": round(total_win / total_trades * 100, 1) if total_trades > 0 else 0,
            "total_profit": round(total_profit, 2),
            "total_holding": total_holding,
            "total_holding_amount": round(total_holding_amount, 2),
            **_return_metrics(_recent_closed_trade_rows(trade_mode)),
        }
        result["summary"]["initial_capital"] = round(SIM_INITIAL_CAPITAL, 2)
        result["summary"]["cash_available"] = round(SIM_INITIAL_CAPITAL + total_profit - total_holding_amount, 2)
        result["summary"]["total_equity"] = round(SIM_INITIAL_CAPITAL + total_profit, 2)
        result["summary"]["total_return_rate"] = round(total_profit / SIM_INITIAL_CAPITAL * 100, 2) if SIM_INITIAL_CAPITAL else 0
        result["summary"]["position_usage_rate"] = round(total_holding_amount / SIM_INITIAL_CAPITAL * 100, 2) if SIM_INITIAL_CAPITAL else 0
        if trade_mode == "live":
            result["summary"]["signal_counts"] = engine.signal_pool_counts(date.today().isoformat())
            result["summary"]["order_counts"] = engine.order_counts(date.today().isoformat())
        try:
            result["portfolio_state"] = engine.portfolio_state(trade_mode)
            result["summary"]["risk_budget"] = {
                "cash_buffer_amount": result["portfolio_state"].get("cash_buffer_amount", 0),
                "cash_available_after_buffer": result["portfolio_state"].get("cash_available_after_buffer", 0),
                "max_total_position_amount": result["portfolio_state"].get("max_total_position_amount", 0),
                "pending_buy_amount": result["portfolio_state"].get("pending_buy_amount", 0),
            }
        except Exception:
            result["portfolio_state"] = {}

        return result
    except Exception as e:
        logger.error(f"模拟交易总览失败: {e}", exc_info=True)
        return {"error": str(e)}


@router.get("/sim-trade/automation-status")
def sim_trade_automation_status():
    """Return whether paper-trading automation is wired and visible."""
    try:
        _ensure_tables()
        today = date.today().isoformat()
        engine = SimTradeEngine()
        task_rows = _read_sql(
            """
            SELECT task_name, task_type, script_path, script_args, cron_time, interval_minutes,
                   enabled, last_run_status, last_run_at, last_triggered_at, last_run_output
            FROM st_scheduled_tasks
            WHERE task_type IN ('sim_trade', 'sim_trade_signal_prepare')
               OR script_path = 'biz/analysis/sync_sim_trade.py'
            ORDER BY task_type, id
            """,
            {},
        )
        task_by_type = {}
        for row in task_rows:
            task_type = str(row.get("task_type") or "")
            if task_type and task_type not in task_by_type:
                task_by_type[task_type] = row

        scheduler = _scheduler_online_status()
        tick_task = _sim_trade_task_status(task_by_type.get("sim_trade"))
        prepare_task = _sim_trade_task_status(task_by_type.get("sim_trade_signal_prepare"))
        intraday_window = _intraday_action_window()
        signal_counts = engine.signal_pool_counts(today)
        order_counts = engine.order_counts(today)
        latest_events = _read_sql(
            """
            SELECT e.event_time, e.event_type, e.stock_code,
                   COALESCE(NULLIF(s.short_name, ''), e.stock_code) AS short_name,
                   e.strategy_type, e.message
            FROM st_sim_event e
            LEFT JOIN si_all_code s ON BINARY e.stock_code = BINARY s.stock_code
            WHERE BINARY COALESCE(e.trade_mode, 'live') = BINARY 'live'
            ORDER BY e.event_time DESC, e.id DESC
            LIMIT 5
            """,
            {},
        )
        sim_auto_ready = bool(
            tick_task.get("enabled")
            and prepare_task.get("enabled")
            and (scheduler.get("standalone_online") or scheduler.get("embedded_running"))
        )
        return {
            "date": today,
            "sim_auto_ready": sim_auto_ready,
            "sim_auto_status": "ready" if sim_auto_ready else "not_ready",
            "sim_auto_label": "模拟自动交易已就绪" if sim_auto_ready else "模拟自动交易未就绪",
            "real_trading_enabled": False,
            "real_trading_label": "真实自动买卖未启用",
            "real_trading_reason": "当前系统只做模拟交易和风控提示，真实账户下单需要单独的券商权限、人工确认和风控闸门。",
            "intraday_window": intraday_window,
            "scheduler": scheduler,
            "tasks": {
                "signal_prepare": prepare_task,
                "intraday_tick": tick_task,
            },
            "signal_counts": signal_counts,
            "order_counts": order_counts,
            "latest_events": latest_events,
        }
    except Exception as e:
        logger.error("模拟交易自动化状态查询失败: %s", e, exc_info=True)
        return {
            "sim_auto_ready": False,
            "sim_auto_status": "error",
            "sim_auto_label": "模拟自动交易状态异常",
            "real_trading_enabled": False,
            "real_trading_label": "真实自动买卖未启用",
            "error": str(e),
        }


# ═══════════════════════════════════════════
# AI推荐模拟池
# ═══════════════════════════════════════════

@router.get("/sim-trade/candidates")
def sim_trade_candidates(
    signal_date: str = Query(default=""),
    trade_mode: str = Query(default="live"),
    limit: int = Query(default=80),
):
    """返回最新AI推荐在模拟交易规则下的可买/等待判断。"""
    try:
        _ensure_tables()
        trade_mode = _normalize_trade_mode(trade_mode)
        engine = SimTradeEngine()

        if not signal_date:
            latest = _read_sql("SELECT MAX(pick_date) AS d FROM st_recommended_stocks", {})
            signal_date = str((latest[0] if latest else {}).get("d") or "")[:10]
        signal_date = (signal_date or "")[:10]
        if not signal_date:
            return {"date": "", "mode": trade_mode, "data": [], "total": 0}

        recs = fetch_recommended_candidates(signal_date)
        rows = []
        max_rows = max(1, min(int(limit or 80), 200))
        for rec in recs[:max_rows]:
            code = str(rec.get("stock_code") or "").zfill(6)
            primary = str(rec.get("primary_strategy") or "").strip()
            evaluations = []
            allowed = []
            rejected = []
            for stype, cfg in STRATEGY_CONFIG.items():
                decision = build_buy_decision(stype, rec)
                item = {
                    "strategy_type": stype,
                    "strategy_name": cfg["name"],
                    "allowed": bool(decision.get("allowed")),
                    "reason": decision.get("reason", ""),
                }
                evaluations.append(item)
                if item["allowed"]:
                    allowed.append(item)
                elif len(rejected) < 2:
                    rejected.append(item)

            preferred = None
            if primary:
                preferred = next((x for x in allowed if x["strategy_type"] == primary), None)
            if not preferred and allowed:
                preferred = allowed[0]

            signal_status = str(rec.get("signal_status") or "").upper()
            main_wave_signal = str(rec.get("main_wave_signal") or "").upper()
            if preferred:
                action = "BUY_READY"
                action_label = "可模拟买入"
                action_reason = preferred["reason"]
            elif signal_status == "SELL_ALERT" or main_wave_signal in {"REDUCE", "SELL_ALERT"}:
                action = "SELL_ALERT"
                action_label = "卖点提醒"
                action_reason = rec.get("main_wave_reason") or rec.get("reason") or "AI提示卖点/减仓"
            else:
                action = "WAIT"
                action_label = "等待买点"
                action_reason = (rejected[0]["reason"] if rejected else rec.get("reason") or "")

            rows.append({
                "stock_code": code,
                "short_name": rec.get("short_name") or code,
                "primary_strategy": primary,
                "preferred_strategy": (preferred or {}).get("strategy_type", ""),
                "preferred_strategy_name": (preferred or {}).get("strategy_name", ""),
                "allowed_strategies": allowed,
                "rejected_samples": rejected,
                "action": action,
                "action_label": action_label,
                "action_reason": action_reason,
                "ai_score": round(_safe_float(rec.get("ai_score")), 2),
                "quality_score": round(_safe_float(rec.get("quality_score")), 2),
                "entry_score": round(_safe_float(rec.get("entry_score")), 2),
                "final_trade_score": round(_safe_float(rec.get("final_trade_score")), 2),
                "expected_return_pct": round(_safe_float(rec.get("expected_return_pct")), 2),
                "risk_reward_ratio": round(_safe_float(rec.get("risk_reward_ratio")), 2),
                "sector_gate_status": rec.get("sector_gate_status") or "WATCH",
                "sector_gate_reason": rec.get("sector_gate_reason") or "",
                "evidence_chain_json": rec.get("evidence_chain_json") or "[]",
                "failure_tags_json": rec.get("failure_tags_json") or "[]",
                "main_wave_score": round(_safe_float(rec.get("main_wave_score")), 2),
                "trend_hold_score": round(_safe_float(rec.get("trend_hold_score")), 2),
                "signal_status": signal_status or "-",
                "main_wave_signal": main_wave_signal or "-",
                "entry_price_low": rec.get("entry_price_low"),
                "entry_price_high": rec.get("entry_price_high"),
                "stop_loss_price": rec.get("stop_loss_price"),
                "take_profit_1": rec.get("take_profit_1"),
                "take_profit_2": rec.get("take_profit_2"),
                "trend_stop_price": rec.get("trend_stop_price"),
                "trend_reduce_price": rec.get("trend_reduce_price"),
                "evaluations": evaluations,
            })

        rows.sort(key=lambda r: (
            0 if r["action"] == "BUY_READY" else 1 if r["action"] == "WAIT" else 2,
            -max(_safe_float(r.get("final_trade_score")), _safe_float(r.get("main_wave_score"))),
        ))
        return {"date": signal_date, "mode": trade_mode, "data": rows, "total": len(rows)}
    except Exception as e:
        logger.error(f"AI推荐模拟池读取失败: {e}", exc_info=True)
        return {"date": signal_date, "mode": trade_mode, "data": [], "total": 0, "error": str(e)}


# ═══════════════════════════════════════════
# 当前持仓
# ═══════════════════════════════════════════

@router.get("/sim-trade/recommendation-summary")
def sim_trade_recommendation_summary(
    signal_date: str = Query(default=""),
    trade_mode: str = Query(default="backtest"),
    days: int = Query(default=20),
):
    """按 AI 推荐日汇总：可买数量、实际买入数量、以及买后胜率。"""
    try:
        _ensure_tables()
        trade_mode = _normalize_trade_mode(trade_mode)
        days = max(1, min(int(days or 20), 120))

        latest_row = _read_sql("SELECT MAX(pick_date) AS d FROM st_recommended_stocks", {})
        latest_signal_date = str((latest_row[0] if latest_row else {}).get("d") or "")[:10]
        signal_date = (signal_date or latest_signal_date or "")[:10]

        if not signal_date:
            return {
                "mode": trade_mode,
                "signal_date": "",
                "latest": {},
                "recent": [],
                "error": "暂无 AI 推荐数据",
            }

        recent_dates = _read_sql(f"""
            SELECT DISTINCT pick_date
            FROM st_recommended_stocks
            WHERE pick_date <= :d
            ORDER BY pick_date DESC
            LIMIT {days}
        """, {"d": signal_date})
        ordered_dates = [str(r.get("pick_date"))[:10] for r in recent_dates if r.get("pick_date")]
        if signal_date not in ordered_dates:
            ordered_dates.insert(0, signal_date)

        latest_summary = _summarize_recommendation_outcomes(signal_date, trade_mode)
        recent = []
        for d in ordered_dates:
            try:
                recent.append(_summarize_recommendation_outcomes(d, trade_mode))
            except Exception:
                logger.warning("summary skipped for %s", d, exc_info=True)

        return {
            "mode": trade_mode,
            "signal_date": signal_date,
            "latest": latest_summary,
            "recent": recent,
        }
    except Exception as e:
        logger.error("AI推荐决策摘要失败: %s", e, exc_info=True)
        return {"mode": trade_mode, "signal_date": signal_date, "latest": {}, "recent": [], "error": str(e)}


@router.get("/sim-trade/positions")
def sim_trade_positions(
    strategy_type: str = Query(default=""),
    trade_mode: str = Query(default="live"),
):
    """获取当前持仓列表"""
    try:
        _ensure_tables()
        trade_mode = _normalize_trade_mode(trade_mode)
        engine = SimTradeEngine()
        where = "WHERE status = 'holding'"
        params = {"mode": trade_mode}
        where += " AND COALESCE(trade_mode, 'live') = :mode"
        if strategy_type:
            where += " AND strategy_type = :st"
            params["st"] = strategy_type

        rows = _read_sql(f"""
            SELECT id, stock_code, short_name, strategy_type, trade_mode,
                   buy_price, buy_shares, buy_date, buy_time, buy_amount,
                   ai_score, short_score, long_score, capital_score,
                   technical_score, fundamental_score, event_risk_level,
                   TIMESTAMPDIFF(DAY, buy_date, CURDATE()) AS holding_days
            FROM st_sim_position
            {where}
            ORDER BY buy_date DESC
        """, params)

        # 盘中批量拉实时行情
        live_prices = {}
        if rows:
            from server.engine.sim_trade_engine import _is_trading_time, _fetch_live_prices_batch
            if _is_trading_time():
                live_prices = _fetch_live_prices_batch([r["stock_code"] for r in rows])

        # 关联最新价格
        for r in rows:
            code = r["stock_code"]
            bp = _safe_float(r["buy_price"])
            lp = live_prices.get(code)
            if lp and lp.get("price", 0) > 0:
                cp = lp["price"]
            elif trade_mode == "forward":
                try:
                    bd = r.get("buy_date")
                    bd_str = str(bd)[:10] if bd else date.today().isoformat()
                    cur = get_latest_stock_minute_price(code, bd_str)
                    cp = _safe_float(cur.get("price")) if cur else bp
                except Exception:
                    cp = bp
            else:
                try:
                    cur = _read_sql("""
                        SELECT close FROM sm_stock_kline
                        WHERE stock_code = :c AND k_type = 1
                        ORDER BY trade_date DESC LIMIT 1
                    """, {"c": code})
                    cp = _safe_float(cur[0]["close"]) if cur else bp
                except Exception:
                    cp = bp
            r["cur_price"] = round(cp, 2)
            r["pnl"] = round((cp - bp) * _safe_int(r["buy_shares"]), 2)
            r["pnl_rate"] = round(((cp - bp) / bp * 100), 2) if bp > 0 else 0
            exit_decision = _position_exit_decision(engine, r, cp, lp)
            r["exit_action"] = exit_decision.get("action")
            r["exit_reason"] = exit_decision.get("reason")
            r["exit_reason_detail"] = exit_decision.get("reason_detail")
            r["exit_thresholds"] = {
                "take_profit_pct": exit_decision.get("take_profit_pct"),
                "stop_loss_pct": exit_decision.get("stop_loss_pct"),
                "max_holding_days": exit_decision.get("max_holding_days"),
                "trailing_activate_pct": exit_decision.get("trailing_activate_pct"),
                "trailing_drawdown_pct": exit_decision.get("trailing_drawdown_pct"),
            }

            bd = r.get("buy_date")
            if isinstance(bd, str):
                r["buy_date"] = bd[:10]
            elif hasattr(bd, 'isoformat'):
                r["buy_date"] = bd.isoformat()

        return {"data": rows, "total": len(rows)}
    except Exception as e:
        return {"data": [], "total": 0, "error": str(e)}


# ═══════════════════════════════════════════
# 历史交易记录
# ═══════════════════════════════════════════

@router.get("/sim-trade/history")
def sim_trade_history(
    strategy_type: str = Query(default=""),
    status: str = Query(default=""),
    trade_mode: str = Query(default="live"),
    limit: int = Query(default=200),
):
    """获取历史交易记录"""
    try:
        _ensure_tables()
        trade_mode = _normalize_trade_mode(trade_mode)
        conditions = ["COALESCE(trade_mode, 'live') = :mode"]
        params = {"mode": trade_mode}
        if strategy_type:
            conditions.append("strategy_type = :st")
            params["st"] = strategy_type
        if status:
            conditions.append("status = :status")
            params["status"] = status

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        rows = _read_sql(f"""
            SELECT id, stock_code, short_name, strategy_type, trade_mode,
                   buy_price, buy_shares, buy_date, buy_time, buy_amount,
                   buy_reason, ai_score, short_score, long_score,
                   status, sell_price, sell_date, sell_time, sell_reason,
                   profit, profit_rate, holding_days, fee_total,
                   event_risk_level
            FROM st_sim_position
            {where}
            ORDER BY COALESCE(sell_date, buy_date) DESC, id DESC
            LIMIT {int(limit)}
        """, params)

        for r in rows:
            for d_field in ("buy_date", "sell_date"):
                v = r.get(d_field)
                if v and hasattr(v, 'isoformat'):
                    r[d_field] = v.isoformat()
                elif v:
                    r[d_field] = str(v)[:10]

        return {"data": rows, "total": len(rows)}
    except Exception as e:
        return {"data": [], "total": 0, "error": str(e)}


# ═══════════════════════════════════════════
# 操作流水表
# ═══════════════════════════════════════════

@router.get("/sim-trade/flow")
def sim_trade_flow(
    source: str = Query(default=""),
    strategy_type: str = Query(default=""),
    stock_code: str = Query(default=""),
    trade_mode: str = Query(default="live"),
    limit: int = Query(default=500),
):
    """获取操作流水(模拟交易 + 自选股操作)"""
    try:
        _ensure_tables()
        trade_mode = _normalize_trade_mode(trade_mode)
        conditions = ["COALESCE(trade_mode, 'live') = :mode"]
        params = {"mode": trade_mode}
        if source:
            conditions.append("source = :src")
            params["src"] = source
        if strategy_type:
            conditions.append("strategy_type = :st")
            params["st"] = strategy_type
        if stock_code:
            conditions.append("stock_code = :code")
            params["code"] = stock_code.strip().zfill(6)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        rows = _read_sql(f"""
            SELECT id, stock_code, short_name, flow_type, source,
                   strategy_type, trade_mode, trans_type, price, shares, amount,
                   fee, reason, ai_score, trans_date, trans_time
            FROM st_trade_flow
            {where}
            ORDER BY trans_date DESC, trans_time DESC, id DESC
            LIMIT {int(limit)}
        """, params)

        for r in rows:
            v = r.get("trans_date")
            if v and hasattr(v, 'isoformat'):
                r["trans_date"] = v.isoformat()
            elif v:
                r["trans_date"] = str(v)[:10]

        return {"data": rows, "total": len(rows)}
    except Exception as e:
        return {"data": [], "total": 0, "error": str(e)}


# ═══════════════════════════════════════════
# 策略统计(胜率曲线)
# ═══════════════════════════════════════════

@router.get("/sim-trade/stats")
def sim_trade_stats(trade_mode: str = Query(default="live")):
    """获取详细统计数据"""
    try:
        _ensure_tables()
        trade_mode = _normalize_trade_mode(trade_mode)
        result = {
            "by_strategy": {},
            "profit_distribution": [],
            "daily_pnl": [],
            "performance_3m": _return_metrics(_recent_closed_trade_rows(trade_mode)),
        }

        for stype in STRATEGY_CONFIG:
            rows = _read_sql("""
                SELECT profit_rate, profit, sell_date, holding_days
                FROM st_sim_position
                WHERE strategy_type = :st AND status = 'sold'
                  AND COALESCE(trade_mode, 'live') = :mode
                ORDER BY sell_date
            """, {"st": stype, "mode": trade_mode})

            if rows:
                rates = [_safe_float(r["profit_rate"]) for r in rows]
                result["by_strategy"][stype] = {
                    "count": len(rates),
                    "win": sum(1 for r in rates if r > 0),
                    "lose": sum(1 for r in rates if r <= 0),
                    "avg_rate": round(sum(rates) / len(rates), 2) if rates else 0,
                    "median_rate": round(sorted(rates)[len(rates) // 2], 2) if rates else 0,
                    "max_rate": round(max(rates), 2) if rates else 0,
                    "min_rate": round(min(rates), 2) if rates else 0,
                    **_return_metrics(_recent_closed_trade_rows(trade_mode, stype)),
                }

                # 盈亏分布
                buckets = {"<-10": 0, "-10~-5": 0, "-5~0": 0, "0~5": 0, "5~10": 0, ">10": 0}
                for r in rates:
                    if r < -10:
                        buckets["<-10"] += 1
                    elif r < -5:
                        buckets["-10~-5"] += 1
                    elif r < 0:
                        buckets["-5~0"] += 1
                    elif r < 5:
                        buckets["0~5"] += 1
                    elif r < 10:
                        buckets["5~10"] += 1
                    else:
                        buckets[">10"] += 1
                result["profit_distribution"] = [{"range": k, "count": v} for k, v in buckets.items()]

                # 每日盈亏
                daily = {}
                for r in rows:
                    sd = r.get("sell_date")
                    if sd:
                        d_str = str(sd)[:10]
                        if d_str not in daily:
                            daily[d_str] = {"date": d_str, "pnl": 0, "count": 0}
                        daily[d_str]["pnl"] += _safe_float(r["profit"])
                        daily[d_str]["count"] += 1
                result["daily_pnl"] = sorted(daily.values(), key=lambda x: x["date"])

        return result
    except Exception as e:
        return {"error": str(e)}


# ═══════════════════════════════════════════
# 手动触发扫描
# ═══════════════════════════════════════════

@router.post("/sim-trade/scan")
def sim_trade_scan():
    """手动触发一次事件驱动模拟交易 tick"""
    try:
        engine = SimTradeEngine()
        results = engine.run_event_tick(auto_prepare=True, strict=True)
        return {"status": "ok", "results": results}
    except Exception as e:
        logger.error(f"模拟交易扫描失败: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}


# ═══════════════════════════════════════════
# 盘中验证
# ═══════════════════════════════════════════

@router.get("/sim-trade/orders")
def sim_trade_orders(
    trade_mode: str = Query(default="live"),
    status: str = Query(default=""),
    side: str = Query(default=""),
    limit: int = Query(default=100),
):
    """List simulated orders created by the event-driven trading loop."""
    try:
        _ensure_tables()
        trade_mode = _normalize_trade_mode(trade_mode)
        limit = max(1, min(int(limit or 100), 500))
        where = "WHERE COALESCE(trade_mode, 'live') = :mode"
        params = {"mode": trade_mode, "limit": limit}
        if status:
            where += " AND status = :status"
            params["status"] = status.strip().upper()
        if side:
            where += " AND side = :side"
            params["side"] = side.strip().upper()
        rows = _read_sql(f"""
            SELECT id, signal_id, trade_mode, order_date, order_time, stock_code, short_name,
                   strategy_type, side, order_type, limit_price, target_price,
                   requested_shares, remaining_shares, status, filled_price, filled_shares,
                   filled_amount, fee, position_id, source_event, price_source,
                   risk_budget_amount, risk_budget_note, reason, reject_reason,
                   last_match_reason, cancel_reason, created_at, updated_at, filled_at
            FROM st_sim_order
            {where}
            ORDER BY id DESC
            LIMIT :limit
        """, params)
        return {"status": "ok", "mode": trade_mode, "data": rows}
    except Exception as e:
        logger.error("模拟订单查询失败: %s", e, exc_info=True)
        return {"status": "error", "error": str(e)}


@router.get("/sim-trade/risk-budget")
def sim_trade_risk_budget(
    trade_mode: str = Query(default="live"),
    trade_date: str = Query(default=""),
):
    """Return portfolio state and latest strategy risk budgets."""
    try:
        _ensure_tables()
        trade_mode = _normalize_trade_mode(trade_mode)
        trade_date = (trade_date or date.today().isoformat())[:10]
        engine = SimTradeEngine()
        state = engine.portfolio_state(trade_mode, trade_date)
        if trade_mode == "live":
            engine._save_risk_budget_snapshot(state, trade_date)
        rows = _read_sql("""
            SELECT strategy_type, initial_capital, total_equity, cash_available,
                   max_total_position_amount, max_strategy_amount,
                   used_strategy_amount, pending_strategy_amount,
                   available_strategy_amount, risk_budget_note, updated_at
            FROM st_sim_risk_budget
            WHERE trade_mode = :mode AND budget_date = :trade_date
            ORDER BY FIELD(strategy_type, 'ultra_short', 'short_term', 'swing', 'main_wave'), strategy_type
        """, {"mode": trade_mode, "trade_date": trade_date})
        return {"status": "ok", "mode": trade_mode, "trade_date": trade_date, "portfolio_state": state, "budgets": rows}
    except Exception as e:
        logger.error("模拟交易风险预算查询失败: %s", e, exc_info=True)
        return {"status": "error", "error": str(e)}


@router.get("/sim-trade/events")
def sim_trade_events(
    trade_mode: str = Query(default="live"),
    limit: int = Query(default=100),
):
    """Recent event-driven simulated trading log."""
    try:
        _ensure_tables()
        trade_mode = _normalize_trade_mode(trade_mode)
        limit = max(1, min(int(limit or 100), 500))
        rows = _read_sql("""
            SELECT id, trade_mode, event_date, event_time, event_type, signal_id,
                   order_id, position_id, stock_code, strategy_type, severity,
                   message, payload, created_at
            FROM st_sim_event
            WHERE trade_mode = :mode
            ORDER BY id DESC
            LIMIT :limit
        """, {"mode": trade_mode, "limit": limit})
        for row in rows:
            payload = row.get("payload")
            if isinstance(payload, str) and payload:
                try:
                    row["payload"] = json.loads(payload)
                except Exception:
                    logger.debug("Failed to decode simulated trade event payload.", exc_info=True)
        return {"status": "ok", "mode": trade_mode, "data": rows}
    except Exception as e:
        logger.error("模拟交易事件查询失败: %s", e, exc_info=True)
        return {"status": "error", "error": str(e)}


@router.post("/sim-trade/forward/start")
def sim_trade_forward_start(
    signal_date: str = Query(default=""),
    trade_date: str = Query(default=""),
    end_date: str = Query(default=""),
    reset: bool = Query(default=False),
):
    """Start forward validation: signal day T, validation buy day T+1."""
    try:
        _ensure_tables()
        engine = SimTradeEngine()

        if not trade_date:
            trade_date = date.today().isoformat()
        trade_date = trade_date[:10]

        try:
            datetime.strptime(trade_date, "%Y-%m-%d")
        except ValueError:
            return {"status": "error", "error": "验证日格式错误，应为YYYY-MM-DD"}

        if not signal_date:
            signal_date = _previous_trade_date(trade_date)
        signal_date = (signal_date or "")[:10]
        if not signal_date:
            return {"status": "error", "error": "找不到验证日前一个交易日，无法确定信号日"}

        try:
            datetime.strptime(signal_date, "%Y-%m-%d")
        except ValueError:
            return {"status": "error", "error": "信号日格式错误，应为YYYY-MM-DD"}

        expected_trade_date = _next_trade_date(signal_date)
        if expected_trade_date and expected_trade_date != trade_date:
            return {
                "status": "error",
                "error": f"验证日必须是信号日的下一个交易日：{signal_date} 的T+1是 {expected_trade_date}，不是 {trade_date}",
            }

        if reset:
            _exec_sql("DELETE FROM st_trade_flow WHERE COALESCE(trade_mode, 'live') = 'forward'", {})
            _exec_sql("DELETE FROM st_sim_position WHERE COALESCE(trade_mode, 'live') = 'forward'", {})

        recs = fetch_recommended_candidates(signal_date)
        if not recs:
            return {
                "status": "error",
                "error": f"{signal_date} 无AI推荐数据，不能开始盘中验证",
                "signal_date": signal_date,
                "trade_date": trade_date,
            }

        total_recommendations = 0
        total_allowed_signals = 0
        total_rejected_signals = 0
        total_bought = 0
        total_sold = 0
        skipped = {}
        bought = []
        rejected_samples = []

        for rec in recs:
            total_recommendations += 1
            code = str(rec.get("stock_code", "")).zfill(6)
            for stype in STRATEGY_CONFIG:
                decision = build_buy_decision(stype, rec)
                if not decision["allowed"]:
                    total_rejected_signals += 1
                    if len(rejected_samples) < 12:
                        rejected_samples.append({
                            "stock_code": code,
                            "short_name": rec.get("short_name") or code,
                            "strategy_type": stype,
                            "reason": decision["reason"],
                        })
                    continue

                total_allowed_signals += 1
                analysis = decision["analysis"]
                analysis["short_name"] = rec.get("short_name", "")
                analysis["reason"] = decision["reason"]
                ret = engine.forward_buy(
                    code,
                    stype,
                    trade_date,
                    analysis,
                    signal_date=signal_date,
                    allow_live_price=True,
                )
                if ret and ret.get("status") == "ok":
                    total_bought += 1
                    bought.append({
                        "stock_code": code,
                        "short_name": rec.get("short_name") or code,
                        "strategy_type": stype,
                        "price": ret.get("price"),
                        "shares": ret.get("shares"),
                        "buy_time": ret.get("buy_time"),
                    })
                else:
                    reason = (ret or {}).get("reason") or "unknown"
                    skipped[reason] = skipped.get(reason, 0) + 1

        replay_dates = []
        if end_date:
            end_date = end_date[:10]
            try:
                datetime.strptime(end_date, "%Y-%m-%d")
            except ValueError:
                return {"status": "error", "error": "验证结束日格式错误，应为YYYY-MM-DD"}
            if end_date < trade_date:
                return {"status": "error", "error": "验证结束日不能早于验证买入日"}
            replay_dates = _trade_dates_between(trade_date, end_date)
            for td in replay_dates:
                sell_signals = engine.forward_check_sell_by_minute(td)
                for sig in sell_signals:
                    engine.execute_sell(sig)
                    total_sold += 1

        stats = _trade_mode_stats("forward")
        minute_source = minute_source_info()
        no_minute_note = ""
        if skipped.get("no_minute_price"):
            no_minute_note = "验证日缺少分钟线；请确认当前分钟数据源可访问且有对应日期数据。"

        return {
            "status": "ok",
            "mode": "forward",
            "signal_date": signal_date,
            "trade_date": trade_date,
            "end_date": end_date,
            "replay_dates": replay_dates,
            "is_trading_time": _is_trading_time(),
            "buy_rule": "信号日T只读取AI推荐；验证日T+1按09:30后首条分钟价买入，当天不能卖出",
            "sell_rule": "T+2起按分钟线/实时价顺序触发止损、止盈、动态止盈、持仓到期",
            "total_recommendations": total_recommendations,
            "total_allowed_signals": total_allowed_signals,
            "total_rejected_signals": total_rejected_signals,
            "total_bought": total_bought,
            "total_sold": total_sold,
            "skipped": skipped,
            "bought": bought,
            "rejected_samples": rejected_samples,
            "minute_source": minute_source,
            "data_note": no_minute_note,
            "stats": stats,
        }
    except Exception as e:
        logger.error(f"盘中验证启动失败: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}


@router.post("/sim-trade/forward/scan")
def sim_trade_forward_scan():
    """Scan forward-validation holdings for real-time sell signals."""
    try:
        _ensure_tables()
        engine = SimTradeEngine()
        sell_signals = engine.check_forward_sell_signals()
        executed = []
        for sig in sell_signals:
            ret = engine.execute_sell(sig)
            executed.append({**sig, **ret})

        stats = _trade_mode_stats("forward")
        return {
            "status": "ok",
            "mode": "forward",
            "is_trading_time": _is_trading_time(),
            "minute_source": minute_source_info(),
            "sell_count": len(executed),
            "sell_signals": executed,
            "stats": stats,
        }
    except Exception as e:
        logger.error(f"盘中验证卖点扫描失败: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}


# ═══════════════════════════════════════════
# 回测
# ═══════════════════════════════════════════

@router.post("/sim-trade/backtest")
def sim_trade_backtest(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    strategy_types: str = Query(default=""),
    initial_capital: float = Query(default=SIM_INITIAL_CAPITAL),
):
    """基于历史数据回测。

    口径：推荐日 T 只产生信号，实际买入发生在下一交易日 T+1 的开盘价，
    卖出按止损、止盈、持仓到期、动态止盈规则触发。
    """
    try:
        _ensure_tables()
        engine = SimTradeEngine()
        selected_strategies = _normalize_strategy_filter(strategy_types)

        if not end_date:
            end_date = date.today().isoformat()
        if not start_date:
            # 默认从最近的推荐数据开始
            rows = _read_sql("SELECT MIN(pick_date) AS d FROM st_recommended_stocks", {})
            if rows and rows[0].get("d"):
                start_date = str(rows[0]["d"])[:10]
            else:
                return {"status": "error", "error": "无推荐数据，无法回测"}

        max_holding_days = max(STRATEGY_CONFIG[stype].get("max_days", 0) for stype in selected_strategies)
        run_end_date = (
            datetime.strptime(end_date[:10], "%Y-%m-%d").date()
            + timedelta(days=max_holding_days + 10)
        ).isoformat()

        # 清理上一次历史回测结果，只清 backtest 模式，不影响实时模拟仓位。
        _exec_sql("DELETE FROM st_trade_flow WHERE COALESCE(trade_mode, 'live') = 'backtest'", {})
        _exec_sql("DELETE FROM st_sim_position WHERE COALESCE(trade_mode, 'live') = 'backtest'", {})

        # 信号日期只取用户要求的推荐窗口；执行日期延后，用来等待仓位自然卖出。
        signal_rows = _read_sql("""
            SELECT trade_date FROM si_trade_calendar
            WHERE trade_status = 1 AND trade_date >= :s AND trade_date <= :e
            ORDER BY trade_date
        """, {"s": start_date, "e": end_date})

        if not signal_rows:
            return {"status": "error", "error": "该时间段内无交易日"}

        run_rows = _read_sql("""
            SELECT trade_date FROM si_trade_calendar
            WHERE trade_status = 1 AND trade_date >= :s AND trade_date <= :e
            ORDER BY trade_date
        """, {"s": start_date, "e": run_end_date})

        dates = [str(r["trade_date"])[:10] for r in run_rows]
        signal_dates = [str(r["trade_date"])[:10] for r in signal_rows]
        if len(dates) < 2:
            return {"status": "error", "error": "交易日不足，无法按T+1回测"}

        next_trade_date = {dates[i]: dates[i + 1] for i in range(len(dates) - 1)}
        recs_by_buy_date = {}
        skipped_no_next_day = 0
        for signal_date in signal_dates:
            buy_date = next_trade_date.get(signal_date)
            if not buy_date:
                skipped_no_next_day += 1
                continue
            recs_by_buy_date.setdefault(buy_date, []).append(signal_date)

        total_bought = 0
        total_sold = 0
        total_recommendations = 0
        total_allowed_signals = 0
        total_rejected_signals = 0

        for td in dates:
            # 先检查卖出
            sell_signals = engine.backtest_check_sell(td)
            for sig in sell_signals:
                engine.execute_sell(sig)
                total_sold += 1

            # T日推荐，T+1开盘买入。
            for signal_date in recs_by_buy_date.get(td, []):
                recs = fetch_recommended_candidates(signal_date)

                if not recs:
                    continue

                for rec in recs:
                    total_recommendations += 1
                    code = str(rec["stock_code"]).zfill(6)

                    for stype in selected_strategies:
                        decision = build_buy_decision(stype, rec)
                        if not decision["allowed"]:
                            total_rejected_signals += 1
                            continue

                        total_allowed_signals += 1
                        analysis = decision["analysis"]
                        analysis["short_name"] = rec.get("short_name", "")
                        analysis["reason"] = decision["reason"]

                        ret = engine.backtest_buy(
                            code,
                            stype,
                            td,
                            analysis,
                            signal_date=signal_date,
                        )
                        if ret and ret.get("status") == "ok":
                            total_bought += 1

        stats = _trade_mode_stats("backtest")

        return {
            "status": "ok",
            "strategy_types": selected_strategies,
            "signal_start_date": signal_dates[0] if signal_dates else "",
            "signal_end_date": signal_dates[-1] if signal_dates else "",
            "run_start_date": dates[0] if dates else "",
            "run_end_date": dates[-1] if dates else "",
            "trade_days": len(signal_dates),
            "run_trade_days": len(dates),
            "buy_rule": "AI评分>=70 + 推荐资格ALLOW + 策略确认项；T日推荐，T+1开盘买入",
            "sell_rule": "T+1后按止损、止盈、持仓到期、动态止盈卖出；日K同日触发止盈止损时按保守止损",
            "total_recommendations": total_recommendations,
            "total_allowed_signals": total_allowed_signals,
            "total_rejected_signals": total_rejected_signals,
            "total_bought": total_bought,
            "total_sold": total_sold,
            "skipped_no_next_day": skipped_no_next_day,
            "stats": stats,
            "report": _sim_backtest_report(
                strategy_types=",".join(selected_strategies),
                initial_capital=initial_capital,
            ),
        }
    except Exception as e:
        logger.error(f"回测失败: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}


@router.get("/sim-trade/backtest/report")
def sim_trade_backtest_report(
    strategy_types: str = Query(default=""),
    initial_capital: float = Query(default=SIM_INITIAL_CAPITAL),
):
    """Return the latest strategy-backtest report without mutating positions."""
    try:
        _ensure_tables()
        return _sim_backtest_report(strategy_types=strategy_types, initial_capital=initial_capital)
    except Exception as e:
        logger.error("策略回测报告查询失败: %s", e, exc_info=True)
        return {"status": "error", "error": str(e)}


RISK_ORDER = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}


# ═══════════════════════════════════════════
# 手动平仓
# ═══════════════════════════════════════════

@router.post("/sim-trade/close/{position_id}")
def sim_trade_close(position_id: int):
    """手动平仓指定持仓"""
    try:
        _ensure_tables()
        engine = SimTradeEngine()
        rows = _read_sql("""
            SELECT id, stock_code, short_name, strategy_type,
                   buy_price, buy_shares, buy_date, ai_score
            FROM st_sim_position
            WHERE id = :id AND status = 'holding'
              AND COALESCE(trade_mode, 'live') = 'live'
        """, {"id": position_id})

        if not rows:
            return {"status": "error", "error": "持仓不存在或已平仓"}

        h = rows[0]
        code = h["stock_code"]

        from server.api.routers.portfolio_math import portfolio_trade_fee

        # 获取当前价
        price_info = _get_current_price(code)
        if not price_info:
            return {"status": "error", "error": "无法获取当前价格"}

        current_price = price_info["price"]
        buy_price = _safe_float(h["buy_price"])
        shares = _safe_int(h["buy_shares"])
        profit_rate = ((current_price - buy_price) / buy_price) * 100 if buy_price > 0 else 0
        sell_fee = portfolio_trade_fee("sell", current_price, shares)
        buy_fee = portfolio_trade_fee("buy", buy_price, shares)
        total_fee = round(buy_fee + sell_fee, 2)
        profit = round((current_price - buy_price) * shares - total_fee, 2)

        bd = h.get("buy_date")
        if isinstance(bd, str):
            buy_d = datetime.strptime(bd[:10], "%Y-%m-%d").date()
        elif hasattr(bd, 'isoformat'):
            buy_d = bd
        else:
            buy_d = date.today()
        holding_days = (date.today() - buy_d).days

        sig = {
            "position_id": h["id"],
            "stock_code": code,
            "short_name": h.get("short_name") or code,
            "strategy_type": h["strategy_type"],
            "buy_price": buy_price,
            "sell_price": current_price,
            "shares": shares,
            "profit_rate": round(profit_rate, 2),
            "profit": profit,
            "fee": total_fee,
            "holding_days": holding_days,
            "reason": "manual_close",
            "reason_label": "手动平仓",
            "ai_score": _safe_float(h.get("ai_score")),
        }

        ret = engine.execute_sell(sig)
        return ret
    except Exception as e:
        return {"status": "error", "error": str(e)}
