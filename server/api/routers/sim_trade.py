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
from server.engine.sim_trade_engine import SimTradeEngine, STRATEGY_CONFIG

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


# ═══════════════════════════════════════════
# 总览仪表盘
# ═══════════════════════════════════════════

@router.get("/sim-trade/dashboard")
def sim_trade_dashboard():
    """模拟交易总览数据"""
    try:
        from server.engine.sim_trade_engine import _ensure_tables
        _ensure_tables()
        engine = SimTradeEngine()

        result = {"strategies": {}, "summary": {}}
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
            """, {"st": stype})

            # 当前持仓
            holdings = _read_sql("""
                SELECT id, stock_code, short_name, buy_price, buy_shares, buy_date, ai_score
                FROM st_sim_position
                WHERE strategy_type = :st AND status = 'holding'
                ORDER BY buy_date DESC
            """, {"st": stype})

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

                # 优先用实时行情，降级到日K收盘价
                lp = live_prices.get(code)
                if lp and lp.get("price", 0) > 0:
                    cur_price = lp["price"]
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

                bd = h.get("buy_date")
                if isinstance(bd, str):
                    bd_str = bd[:10]
                elif hasattr(bd, 'isoformat'):
                    bd_str = bd.isoformat()
                else:
                    bd_str = str(bd)[:10]

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
        }

        return result
    except Exception as e:
        logger.error(f"模拟交易总览失败: {e}", exc_info=True)
        return {"error": str(e)}


# ═══════════════════════════════════════════
# 当前持仓
# ═══════════════════════════════════════════

@router.get("/sim-trade/positions")
def sim_trade_positions(strategy_type: str = Query(default="")):
    """获取当前持仓列表"""
    try:
        where = "WHERE status = 'holding'"
        params = {}
        if strategy_type:
            where += " AND strategy_type = :st"
            params["st"] = strategy_type

        rows = _read_sql(f"""
            SELECT id, stock_code, short_name, strategy_type,
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
    limit: int = Query(default=200),
):
    """获取历史交易记录"""
    try:
        conditions = []
        params = {}
        if strategy_type:
            conditions.append("strategy_type = :st")
            params["st"] = strategy_type
        if status:
            conditions.append("status = :status")
            params["status"] = status

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        rows = _read_sql(f"""
            SELECT id, stock_code, short_name, strategy_type,
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
    limit: int = Query(default=500),
):
    """获取操作流水(模拟交易 + 自选股操作)"""
    try:
        conditions = []
        params = {}
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
                   strategy_type, trans_type, price, shares, amount,
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
def sim_trade_stats():
    """获取详细统计数据"""
    try:
        result = {"by_strategy": {}, "profit_distribution": [], "daily_pnl": []}

        for stype in STRATEGY_CONFIG:
            rows = _read_sql("""
                SELECT profit_rate, profit, sell_date, holding_days
                FROM st_sim_position
                WHERE strategy_type = :st AND status = 'sold'
                ORDER BY sell_date
            """, {"st": stype})

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
# 回测
# ═══════════════════════════════════════════

@router.post("/sim-trade/backtest")
def sim_trade_backtest(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
):
    """基于历史数据回测"""
    try:
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

        # 获取交易日列表
        trade_dates = _read_sql("""
            SELECT trade_date FROM si_trade_calendar
            WHERE trade_status = 1 AND trade_date >= :s AND trade_date <= :e
            ORDER BY trade_date
        """, {"s": start_date, "e": end_date})

        if not trade_dates:
            return {"status": "error", "error": "该时间段内无交易日"}

        dates = [str(r["trade_date"])[:10] for r in trade_dates]
        total_bought = 0
        total_sold = 0

        for td in dates:
            # 先检查卖出
            sell_signals = engine.backtest_check_sell(td)
            for sig in sell_signals:
                engine.execute_sell(sig)
                total_sold += 1

            # 获取当日推荐
            recs = _read_sql("""
                SELECT stock_code, short_name, ai_score, long_term_score, short_term_score,
                       fundamental, capital_score, technical, event_risk_level, reason
                FROM st_recommended_stocks
                WHERE pick_date = :d AND recommend_status = 'ALLOW'
                ORDER BY ai_score DESC
            """, {"d": td})

            if not recs:
                continue

            for rec in recs:
                ai_score = _safe_float(rec.get("ai_score") or rec.get("short_term_score"))
                if ai_score < 70:
                    continue

                analysis = {
                    "short_name": rec.get("short_name", ""),
                    "ai_score": ai_score,
                    "short_score": _safe_float(rec.get("short_term_score")),
                    "long_score": _safe_float(rec.get("long_term_score")),
                    "capital_score": _safe_float(rec.get("capital_score")),
                    "technical_score": _safe_float(rec.get("technical")),
                    "fundamental_score": _safe_float(rec.get("fundamental")),
                    "event_risk_level": rec.get("event_risk_level", "LOW"),
                    "reason": rec.get("reason", ""),
                }

                code = str(rec["stock_code"]).zfill(6)

                # 按策略尝试买入
                for stype in ["ultra_short", "short_term", "swing"]:
                    cfg = STRATEGY_CONFIG[stype]
                    # 快速前置检查
                    if ai_score < cfg["min_ai_score"]:
                        continue
                    risk = analysis.get("event_risk_level", "LOW")
                    if not (RISK_ORDER.get(risk, 99) <= RISK_ORDER.get(cfg["max_risk_level"], 99)):
                        continue

                    ret = engine.backtest_buy(code, stype, td, analysis)
                    if ret and ret.get("status") == "ok":
                        total_bought += 1

        return {
            "status": "ok",
            "start_date": dates[0] if dates else "",
            "end_date": dates[-1] if dates else "",
            "trade_days": len(dates),
            "total_bought": total_bought,
            "total_sold": total_sold,
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
        engine = SimTradeEngine()
        rows = _read_sql("""
            SELECT id, stock_code, short_name, strategy_type,
                   buy_price, buy_shares, buy_date, ai_score
            FROM st_sim_position
            WHERE id = :id AND status = 'holding'
        """, {"id": position_id})

        if not rows:
            return {"status": "error", "error": "持仓不存在或已平仓"}

        h = rows[0]
        code = h["stock_code"]

        from server.api.routers.portfolio_math import portfolio_trade_fee

        # 获取当前价
        price_info = engine._get_current_price(code)
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
