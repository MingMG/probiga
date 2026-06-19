# -*- coding: utf-8 -*-
"""
模拟交易 API 路由

提供模拟交易的查询、执行、统计和流水接口。
"""

import json
import logging
from datetime import date, datetime, timedelta

import pandas as pd
from fastapi import APIRouter, Query
from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.common.minute_data import get_latest_stock_minute_price, minute_source_info
from server.engine.sim_trade_engine import (
    SimTradeEngine,
    STRATEGY_CONFIG,
    build_buy_decision,
    fetch_recommended_candidates,
    _get_current_price,
    _ensure_tables,
    _is_trading_time,
)

logger = logging.getLogger(__name__)

router = APIRouter()


def _read_sql(sql: str, params: dict = None) -> list[dict]:
    import numpy as np
    df = pd.read_sql(text(sql), get_engine(), params=params)
    if df.empty:
        return []
    df = df.replace({np.nan: None, pd.NA: None, pd.NaT: None})
    for c in df.columns:
        if df[c].dtype == "datetime64[ns]":
            df[c] = df[c].astype(str)
    return df.to_dict(orient="records")


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

        return result
    except Exception as e:
        logger.error(f"模拟交易总览失败: {e}", exc_info=True)
        return {"error": str(e)}


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

@router.get("/sim-trade/positions")
def sim_trade_positions(
    strategy_type: str = Query(default=""),
    trade_mode: str = Query(default="live"),
):
    """获取当前持仓列表"""
    try:
        _ensure_tables()
        trade_mode = _normalize_trade_mode(trade_mode)
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
    """手动触发一次模拟交易信号扫描"""
    try:
        engine = SimTradeEngine()
        results = {"sell_signals": [], "buy_signals": {}}

        # 先检查卖出
        sell_signals = engine.check_sell_signals()
        for sig in sell_signals:
            ret = engine.execute_sell(sig)
            results["sell_signals"].append({**sig, **ret})

        forward_sell_signals = engine.check_forward_sell_signals()
        results["forward_sell_signals"] = []
        for sig in forward_sell_signals:
            ret = engine.execute_sell(sig)
            results["forward_sell_signals"].append({**sig, **ret})

        # 再检查买入(三种策略)
        for stype in STRATEGY_CONFIG:
            buy_signals = engine.check_buy_signals(stype)
            executed = []
            for sig in buy_signals:
                ret = engine.execute_buy(sig)
                executed.append({**sig, **ret})
            results["buy_signals"][stype] = executed

        return {"status": "ok", "results": results}
    except Exception as e:
        logger.error(f"模拟交易扫描失败: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}


# ═══════════════════════════════════════════
# 盘中验证
# ═══════════════════════════════════════════

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
):
    """基于历史数据回测。

    口径：推荐日 T 只产生信号，实际买入发生在下一交易日 T+1 的开盘价，
    卖出按止损、止盈、持仓到期、动态止盈规则触发。
    """
    try:
        _ensure_tables()
        engine = SimTradeEngine()

        if not end_date:
            end_date = date.today().isoformat()
        if not start_date:
            # 默认从最近的推荐数据开始
            rows = _read_sql("SELECT MIN(pick_date) AS d FROM st_recommended_stocks", {})
            if rows and rows[0].get("d"):
                start_date = str(rows[0]["d"])[:10]
            else:
                return {"status": "error", "error": "无推荐数据，无法回测"}

        max_holding_days = max(cfg.get("max_days", 0) for cfg in STRATEGY_CONFIG.values())
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

                    for stype in STRATEGY_CONFIG:
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
        }
    except Exception as e:
        logger.error(f"回测失败: {e}", exc_info=True)
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
