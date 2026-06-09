# -*- coding: utf-8 -*-
"""
模拟交易引擎

三种策略的买入/卖出信号检测，以及交易执行逻辑。
盘中实时检测信号，以当前价模拟交易。
"""

import json
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal

import pandas as pd
from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.api.routers.portfolio_math import portfolio_trade_fee

logger = logging.getLogger(__name__)

# ── 策略配置 ──
STRATEGY_CONFIG = {
    "ultra_short": {
        "name": "超短",
        "max_holding": 3,          # 最大同时持仓数
        "max_days": 3,             # 最长持仓天数
        "take_profit": 5.0,        # 止盈(%)
        "stop_loss": -3.0,         # 止损(%)
        "trailing_activate": None, # 动态止盈激活点(无)
        "trailing_drawdown": None, # 动态止盈回撤(无)
        "buy_amount": 100000,      # 每笔买入金额(元)
        "min_ai_score": 70,        # 最低AI评分
        "min_short_score": 75,     # 最低短期评分
        "min_capital_score": 70,   # 最低资金面评分
        "min_technical_score": 0,  # 最低技术面评分
        "max_risk_level": "LOW",   # 最高风险等级
    },
    "short_term": {
        "name": "短线",
        "max_holding": 3,
        "max_days": 10,
        "take_profit": 10.0,
        "stop_loss": -5.0,
        "trailing_activate": 7.0,
        "trailing_drawdown": 2.0,
        "buy_amount": 100000,
        "min_ai_score": 70,
        "min_short_score": 65,
        "min_capital_score": 0,
        "min_technical_score": 65,
        "max_risk_level": "MEDIUM",
    },
    "swing": {
        "name": "波段",
        "max_holding": 2,
        "max_days": 30,
        "take_profit": 20.0,
        "stop_loss": -8.0,
        "trailing_activate": 15.0,
        "trailing_drawdown": 5.0,
        "buy_amount": 100000,
        "min_ai_score": 70,
        "min_short_score": 0,
        "min_capital_score": 0,
        "min_technical_score": 0,
        "min_long_score": 60,
        "min_fundamental_score": 60,
        "max_risk_level": "LOW",
    },
}

RISK_ORDER = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}


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


def _ensure_tables():
    """确保模拟交易相关表存在"""
    try:
        _exec_sql("""
            CREATE TABLE IF NOT EXISTS `st_sim_position` (
                `id`              BIGINT AUTO_INCREMENT PRIMARY KEY,
                `stock_code`      VARCHAR(10)  NOT NULL,
                `short_name`      VARCHAR(20)  DEFAULT '',
                `strategy_type`   VARCHAR(20)  NOT NULL,
                `buy_price`       DECIMAL(12,4) NOT NULL,
                `buy_amount`      DECIMAL(14,2) NOT NULL,
                `buy_shares`      INT          NOT NULL,
                `buy_date`        DATE         NOT NULL,
                `buy_time`        VARCHAR(20)  DEFAULT '',
                `buy_reason`      TEXT,
                `ai_score`        DECIMAL(5,2) DEFAULT 0,
                `short_score`     DECIMAL(5,2) DEFAULT 0,
                `long_score`      DECIMAL(5,2) DEFAULT 0,
                `capital_score`   DECIMAL(5,2) DEFAULT 0,
                `technical_score` DECIMAL(5,2) DEFAULT 0,
                `fundamental_score` DECIMAL(5,2) DEFAULT 0,
                `event_risk_level` VARCHAR(10) DEFAULT 'LOW',
                `status`          VARCHAR(20)  DEFAULT 'holding',
                `sell_price`      DECIMAL(12,4) DEFAULT NULL,
                `sell_date`       DATE         DEFAULT NULL,
                `sell_time`       VARCHAR(20)  DEFAULT '',
                `sell_reason`     VARCHAR(100) DEFAULT '',
                `profit`          DECIMAL(14,2) DEFAULT 0,
                `profit_rate`     DECIMAL(8,4) DEFAULT 0,
                `holding_days`    INT          DEFAULT 0,
                `fee_total`       DECIMAL(10,2) DEFAULT 0,
                `created_at`      DATETIME     DEFAULT CURRENT_TIMESTAMP,
                `updated_at`      DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX `idx_strategy_status` (`strategy_type`, `status`),
                INDEX `idx_stock_code` (`stock_code`),
                INDEX `idx_buy_date` (`buy_date`),
                INDEX `idx_status` (`status`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        _exec_sql("""
            CREATE TABLE IF NOT EXISTS `st_trade_flow` (
                `id`              BIGINT AUTO_INCREMENT PRIMARY KEY,
                `stock_code`      VARCHAR(10)  NOT NULL,
                `short_name`      VARCHAR(20)  DEFAULT '',
                `flow_type`       VARCHAR(20)  NOT NULL,
                `source`          VARCHAR(20)  NOT NULL,
                `strategy_type`   VARCHAR(20)  DEFAULT '',
                `trans_type`      VARCHAR(10)  NOT NULL,
                `price`           DECIMAL(12,4) NOT NULL,
                `shares`          INT          NOT NULL,
                `amount`          DECIMAL(14,2) NOT NULL,
                `fee`             DECIMAL(10,2) DEFAULT 0,
                `reason`          VARCHAR(200) DEFAULT '',
                `ai_score`        DECIMAL(5,2) DEFAULT 0,
                `trans_date`      DATE         NOT NULL,
                `trans_time`      VARCHAR(20)  DEFAULT '',
                `created_at`      DATETIME     DEFAULT CURRENT_TIMESTAMP,
                INDEX `idx_flow_type` (`flow_type`),
                INDEX `idx_source` (`source`),
                INDEX `idx_stock_date` (`stock_code`, `trans_date`),
                INDEX `idx_trans_date` (`trans_date`),
                INDEX `idx_strategy` (`strategy_type`, `trans_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        _exec_sql("""
            CREATE TABLE IF NOT EXISTS `st_strategy_snapshot` (
                `id`              BIGINT AUTO_INCREMENT PRIMARY KEY,
                `snapshot_date`   DATE         NOT NULL,
                `strategy_type`   VARCHAR(20)  NOT NULL,
                `total_trades`    INT          DEFAULT 0,
                `win_count`       INT          DEFAULT 0,
                `lose_count`      INT          DEFAULT 0,
                `win_rate`        DECIMAL(6,2) DEFAULT 0,
                `total_profit`    DECIMAL(14,2) DEFAULT 0,
                `total_fee`       DECIMAL(10,2) DEFAULT 0,
                `avg_profit_rate` DECIMAL(8,4) DEFAULT 0,
                `max_profit_rate` DECIMAL(8,4) DEFAULT 0,
                `max_loss_rate`   DECIMAL(8,4) DEFAULT 0,
                `holding_count`   INT          DEFAULT 0,
                `holding_amount`  DECIMAL(14,2) DEFAULT 0,
                `created_at`      DATETIME     DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY `uk_date_strategy` (`snapshot_date`, `strategy_type`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
    except Exception as e:
        logger.warning(f"确保模拟交易表存在失败: {e}")


def _is_trading_time(now=None) -> bool:
    """判断当前是否为A股交易时间(9:25-11:30 / 13:00-15:00)"""
    from datetime import datetime as _dt, time as _t
    now = now or _dt.now()
    if now.weekday() >= 5:
        return False
    t = now.time()
    return (_t(9, 25) <= t <= _t(11, 31)) or (_t(12, 59) <= t <= _t(15, 1))


def _fetch_live_price_sina(stock_code: str) -> dict | None:
    """通过新浪财经接口获取单只股票实时价格"""
    try:
        import requests
        code = str(stock_code).strip().zfill(6)
        prefix = "sh" if code.startswith("6") else "sz"
        url = f"https://hq.sinajs.cn/list={prefix}{code}"
        resp = requests.get(url, headers={
            "User-Agent": "Mozilla/5.0 ProBigA",
            "Referer": "https://finance.sina.com.cn",
        }, timeout=10)
        text = resp.text.strip()
        if '""' in text or "=" not in text:
            return None
        _, val_part = text.split("=", 1)
        fields = val_part.strip('";\r ').split(",")
        if len(fields) < 4:
            return None
        price = float(fields[3] or 0)
        pre_close = float(fields[2] or 0)
        if price <= 0:
            price = pre_close
        if price <= 0:
            return None
        return {"price": price, "source": "sina_live",
                "short_name": fields[0],
                "change_pct": round((price - pre_close) / pre_close * 100, 2) if pre_close > 0 else 0}
    except Exception as e:
        logger.warning(f"新浪行情获取 {stock_code} 失败: {e}")
        return None


def _fetch_live_prices_batch(stock_codes: list[str]) -> dict[str, dict]:
    """批量获取实时价格(新浪接口，一次请求拿多只)"""
    try:
        import requests
        clean = [str(c).strip().zfill(6) for c in stock_codes if c]
        if not clean:
            return {}
        symbols = ",".join([("sh" + c if c.startswith("6") else "sz" + c) for c in clean])
        resp = requests.get(
            f"https://hq.sinajs.cn/list={symbols}",
            headers={"User-Agent": "Mozilla/5.0 ProBigA", "Referer": "https://finance.sina.com.cn"},
            timeout=15,
        )
        result = {}
        for line in resp.text.strip().split("\n"):
            if "=" not in line or '""' in line:
                continue
            var_part, val_part = line.split("=", 1)
            code = var_part.split("_")[-1][2:]
            fields = val_part.strip('";\r ').split(",")
            if len(fields) < 4:
                continue
            try:
                price = float(fields[3] or 0)
                pre_close = float(fields[2] or 0)
                if price <= 0:
                    price = pre_close
                if price > 0:
                    result[code] = {
                        "price": price, "source": "sina_live",
                        "short_name": fields[0],
                        "change_pct": round((price - pre_close) / pre_close * 100, 2) if pre_close > 0 else 0,
                    }
            except (ValueError, IndexError):
                continue
        return result
    except Exception as e:
        logger.warning(f"批量新浪行情获取失败: {e}")
        return {}


def _get_current_price(stock_code: str) -> dict | None:
    """获取股票当前价格：盘中走实时行情接口，盘后走数据库"""
    code = str(stock_code).strip().zfill(6)

    # 盘中：直接调新浪实时接口
    if _is_trading_time():
        live = _fetch_live_price_sina(code)
        if live and live.get("price", 0) > 0:
            return live
        # 接口失败时降级到数据库快照
        try:
            rows = _read_sql("""
                SELECT price, snapshot_at FROM sm_rt_quote_snapshot
                WHERE stock_code = :c LIMIT 1
            """, {"c": code})
            if rows and rows[0].get("price") and float(rows[0]["price"]) > 0:
                snap = str(rows[0].get("snapshot_at") or "")
                if snap[:10] == date.today().isoformat():
                    return {"price": float(rows[0]["price"]), "source": "snapshot_fallback"}
        except Exception:
            pass

    # 盘后/非交易日：从数据库取最新收盘价
    try:
        rows = _read_sql("""
            SELECT close AS price, trade_date FROM sm_stock_kline
            WHERE stock_code = :c AND k_type = 1
            ORDER BY trade_date DESC LIMIT 1
        """, {"c": code})
        if rows and rows[0].get("price") and float(rows[0]["price"]) > 0:
            return {"price": float(rows[0]["price"]), "source": "kline_close",
                    "trade_date": str(rows[0].get("trade_date", ""))[:10]}
    except Exception as e:
        logger.warning(f"获取 {code} 收盘价失败: {e}")

    return None


def _calc_shares(price: float, amount: int) -> int:
    """计算可买股数(100的整数倍)"""
    if price <= 0:
        return 0
    shares = int(amount / price / 100) * 100
    return max(shares, 100)


def _check_risk_level(stock_risk: str, max_risk: str) -> bool:
    """检查风险等级是否在允许范围内"""
    r = RISK_ORDER.get(stock_risk, 99)
    m = RISK_ORDER.get(max_risk, 99)
    return r <= m


class SimTradeEngine:
    """模拟交易引擎"""

    def __init__(self):
        _ensure_tables()

    # ────────────────────────────────────────
    # 买入信号检测
    # ────────────────────────────────────────

    def check_buy_signals(self, strategy_type: str) -> list[dict]:
        """
        检查指定策略的买入信号。
        1. 从 st_recommended_stocks 获取最新推荐(评分>70)
        2. 从 stock_analysis_result 获取详细评分
        3. 按策略条件过滤
        4. 排除已持仓的股票
        5. 检查持仓数量上限
        """
        cfg = STRATEGY_CONFIG.get(strategy_type)
        if not cfg:
            return []

        _ensure_tables()

        # 检查当前持仓数
        holding_count = self._get_holding_count(strategy_type)
        if holding_count >= cfg["max_holding"]:
            return []

        # 获取已持仓的股票代码
        holding_codes = self._get_holding_codes(strategy_type)

        # 从推荐表获取最新数据
        latest_pick = _read_sql("SELECT MAX(pick_date) AS d FROM st_recommended_stocks", {})
        if not latest_pick or not latest_pick[0].get("d"):
            return []
        pick_date = str(latest_pick[0]["d"])[:10]

        candidates = _read_sql("""
            SELECT stock_code, short_name, ai_score, long_term_score, short_term_score,
                   fundamental, capital_score, valuation, technical, reason,
                   recommend_status, event_risk_level, sentiment_score, event_score
            FROM st_recommended_stocks
            WHERE pick_date = :d
              AND recommend_status = 'ALLOW'
            ORDER BY ai_score DESC
        """, {"d": pick_date})

        if not candidates:
            return []

        # 盘中：批量拉候选股实时行情
        live_prices = {}
        if _is_trading_time():
            candidate_codes = [str(c.get("stock_code", "")).zfill(6) for c in candidates]
            live_prices = _fetch_live_prices_batch(candidate_codes)

        signals = []
        for c in candidates:
            code = str(c.get("stock_code", "")).zfill(6)
            if code in holding_codes:
                continue

            ai_score = float(c.get("ai_score") or c.get("short_term_score") or 0)
            short_score = float(c.get("short_term_score") or 0)
            long_score = float(c.get("long_term_score") or 0)
            capital_score = float(c.get("capital_score") or 0)
            technical_score = float(c.get("technical") or 0)
            fundamental_score = float(c.get("fundamental") or 0)
            risk_level = str(c.get("event_risk_level") or "LOW")

            # 基础门槛: AI评分
            if ai_score < cfg["min_ai_score"]:
                continue

            # 策略特定条件
            if strategy_type == "ultra_short":
                if short_score < cfg["min_short_score"]:
                    continue
                if capital_score < cfg["min_capital_score"]:
                    continue

            elif strategy_type == "short_term":
                if short_score < cfg["min_short_score"]:
                    continue
                if technical_score < cfg["min_technical_score"]:
                    continue

            elif strategy_type == "swing":
                if long_score < cfg.get("min_long_score", 0):
                    continue
                if fundamental_score < cfg.get("min_fundamental_score", 0):
                    continue

            # 风险等级过滤
            if not _check_risk_level(risk_level, cfg["max_risk_level"]):
                continue

            # 获取当前价格：优先用批量行情
            price_info = live_prices.get(code)
            if not price_info or price_info.get("price", 0) <= 0:
                price_info = _get_current_price(code)
            if not price_info or price_info["price"] <= 0:
                continue

            price = price_info["price"]
            shares = _calc_shares(price, cfg["buy_amount"])
            if shares <= 0:
                continue

            signals.append({
                "stock_code": code,
                "short_name": c.get("short_name") or code,
                "strategy_type": strategy_type,
                "price": price,
                "shares": shares,
                "amount": round(price * shares, 2),
                "ai_score": ai_score,
                "short_score": short_score,
                "long_score": long_score,
                "capital_score": capital_score,
                "technical_score": technical_score,
                "fundamental_score": fundamental_score,
                "event_risk_level": risk_level,
                "reason": c.get("reason") or "",
                "price_source": price_info.get("source", ""),
                # 保留剩余可买数量
                "slots_left": cfg["max_holding"] - holding_count - len(signals),
            })

            # 检查是否达到持仓上限
            if holding_count + len(signals) >= cfg["max_holding"]:
                break

        return signals

    # ────────────────────────────────────────
    # 卖出信号检测
    # ────────────────────────────────────────

    def check_sell_signals(self) -> list[dict]:
        """
        检查所有持仓的卖出信号。
        盘中批量拉实时行情，一次请求拿所有持仓价格。
        """
        _ensure_tables()
        holdings = _read_sql("""
            SELECT id, stock_code, short_name, strategy_type,
                   buy_price, buy_shares, buy_date, buy_amount,
                   ai_score, profit_rate
            FROM st_sim_position
            WHERE status = 'holding'
        """)
        if not holdings:
            return []

        # 盘中：批量拉实时行情，避免逐只请求
        live_prices = {}
        if _is_trading_time():
            codes = [h["stock_code"] for h in holdings]
            live_prices = _fetch_live_prices_batch(codes)

        signals = []
        for h in holdings:
            code = h["stock_code"]
            cfg = STRATEGY_CONFIG.get(h["strategy_type"])
            if not cfg:
                continue

            # 优先用批量行情，降级到逐只查询
            price_info = live_prices.get(code)
            if not price_info or price_info.get("price", 0) <= 0:
                price_info = _get_current_price(code)
            if not price_info or price_info["price"] <= 0:
                continue

            current_price = price_info["price"]
            buy_price = float(h["buy_price"])

            # 计算收益率
            profit_rate = ((current_price - buy_price) / buy_price) * 100 if buy_price > 0 else 0

            # 计算持仓天数
            buy_date = h["buy_date"]
            if isinstance(buy_date, str):
                buy_d = datetime.strptime(buy_date[:10], "%Y-%m-%d").date()
            else:
                buy_d = buy_date
            holding_days = (date.today() - buy_d).days

            reason = ""
            should_sell = False

            # 1. 止盈检查
            if profit_rate >= cfg["take_profit"]:
                reason = "take_profit"
                should_sell = True

            # 2. 止损检查
            elif profit_rate <= cfg["stop_loss"]:
                reason = "stop_loss"
                should_sell = True

            # 3. 时间止损
            elif holding_days >= cfg["max_days"]:
                reason = "time_limit"
                should_sell = True

            # 4. 动态止盈(如果配置了)
            elif cfg["trailing_activate"] and profit_rate >= cfg["trailing_activate"]:
                # 从最高点回撤超过阈值
                max_rate = self._get_max_profit_rate(h["id"], code, buy_price)
                if max_rate is not None and (max_rate - profit_rate) >= cfg["trailing_drawdown"]:
                    reason = "trailing_stop"
                    should_sell = True

            if should_sell:
                shares = int(h["buy_shares"])
                sell_fee = portfolio_trade_fee("sell", current_price, shares)
                buy_fee = portfolio_trade_fee("buy", buy_price, shares)
                total_fee = round(buy_fee + sell_fee, 2)
                gross_profit = round((current_price - buy_price) * shares, 2)
                net_profit = round(gross_profit - total_fee, 2)

                signals.append({
                    "position_id": h["id"],
                    "stock_code": code,
                    "short_name": h.get("short_name") or code,
                    "strategy_type": h["strategy_type"],
                    "buy_price": buy_price,
                    "sell_price": current_price,
                    "shares": shares,
                    "profit_rate": round(profit_rate, 2),
                    "profit": net_profit,
                    "fee": total_fee,
                    "holding_days": holding_days,
                    "reason": reason,
                    "reason_label": self._reason_label(reason),
                    "ai_score": float(h.get("ai_score") or 0),
                })

        return signals

    def _get_max_profit_rate(self, position_id: int, stock_code: str, buy_price: float) -> float | None:
        """获取持仓期间的最高收益率(基于K线数据)"""
        try:
            # 获取持仓以来的K线最高价
            rows = _read_sql("""
                SELECT MAX(high) AS max_high
                FROM sm_stock_kline
                WHERE stock_code = :c AND k_type = 1
                  AND trade_date >= (SELECT buy_date FROM st_sim_position WHERE id = :id)
            """, {"c": stock_code, "id": position_id})
            if rows and rows[0].get("max_high") and buy_price > 0:
                return ((float(rows[0]["max_high"]) - buy_price) / buy_price) * 100
        except Exception:
            pass
        return None

    @staticmethod
    def _reason_label(reason: str) -> str:
        labels = {
            "take_profit": "止盈",
            "stop_loss": "止损",
            "time_limit": "时间止损",
            "trailing_stop": "动态止盈",
        }
        return labels.get(reason, reason)

    # ────────────────────────────────────────
    # 交易执行
    # ────────────────────────────────────────

    def execute_buy(self, signal: dict) -> dict:
        """执行模拟买入"""
        _ensure_tables()
        now = datetime.now()
        price = signal["price"]
        shares = signal["shares"]
        amount = round(price * shares, 2)
        fee = portfolio_trade_fee("buy", price, shares)

        # 写入持仓表
        _exec_sql("""
            INSERT INTO st_sim_position
            (stock_code, short_name, strategy_type, buy_price, buy_amount, buy_shares,
             buy_date, buy_time, buy_reason, ai_score, short_score, long_score,
             capital_score, technical_score, fundamental_score, event_risk_level, status)
            VALUES (:code, :name, :st, :price, :amount, :shares,
                    :date, :time, :reason, :ai, :ss, :ls,
                    :cs, :ts, :fs, :risk, 'holding')
        """, {
            "code": signal["stock_code"],
            "name": signal.get("short_name", ""),
            "st": signal["strategy_type"],
            "price": price,
            "amount": amount,
            "shares": shares,
            "date": now.date(),
            "time": now.strftime("%H:%M"),
            "reason": json.dumps({"reason": signal.get("reason", ""), "price_source": signal.get("price_source", "")},
                                 ensure_ascii=False),
            "ai": signal.get("ai_score", 0),
            "ss": signal.get("short_score", 0),
            "ls": signal.get("long_score", 0),
            "cs": signal.get("capital_score", 0),
            "ts": signal.get("technical_score", 0),
            "fs": signal.get("fundamental_score", 0),
            "risk": signal.get("event_risk_level", "LOW"),
        })

        # 写入流水表
        _exec_sql("""
            INSERT INTO st_trade_flow
            (stock_code, short_name, flow_type, source, strategy_type, trans_type,
             price, shares, amount, fee, reason, ai_score, trans_date, trans_time)
            VALUES (:code, :name, :ft, 'simulation', :st, 'buy',
                    :price, :shares, :amount, :fee, :reason, :ai, :date, :time)
        """, {
            "code": signal["stock_code"],
            "name": signal.get("short_name", ""),
            "ft": "sim_buy",
            "st": signal["strategy_type"],
            "price": price,
            "shares": shares,
            "amount": amount,
            "fee": round(fee, 2),
            "reason": f"AI评分{signal.get('ai_score', 0)}",
            "ai": signal.get("ai_score", 0),
            "date": now.date(),
            "time": now.strftime("%H:%M"),
        })

        return {"status": "ok", "stock_code": signal["stock_code"], "price": price,
                "shares": shares, "amount": amount, "fee": round(fee, 2)}

    def execute_sell(self, sell_signal: dict) -> dict:
        """执行模拟卖出"""
        _ensure_tables()
        now = datetime.now()
        position_id = sell_signal["position_id"]
        price = sell_signal["sell_price"]
        shares = sell_signal["shares"]
        amount = round(price * shares, 2)
        fee = sell_signal.get("fee", 0)
        profit = sell_signal["profit"]
        profit_rate = sell_signal["profit_rate"]
        reason = sell_signal["reason"]

        # 更新持仓表
        _exec_sql("""
            UPDATE st_sim_position
            SET status = 'sold', sell_price = :price, sell_date = :date,
                sell_time = :time, sell_reason = :reason,
                profit = :profit, profit_rate = :rate,
                holding_days = :days, fee_total = :fee,
                updated_at = NOW()
            WHERE id = :id
        """, {
            "price": price,
            "date": now.date(),
            "time": now.strftime("%H:%M"),
            "reason": reason,
            "profit": profit,
            "rate": profit_rate,
            "days": sell_signal.get("holding_days", 0),
            "fee": fee,
            "id": position_id,
        })

        # 写入流水表
        _exec_sql("""
            INSERT INTO st_trade_flow
            (stock_code, short_name, flow_type, source, strategy_type, trans_type,
             price, shares, amount, fee, reason, ai_score, trans_date, trans_time)
            VALUES (:code, :name, :ft, 'simulation', :st, 'sell',
                    :price, :shares, :amount, :fee, :reason, :ai, :date, :time)
        """, {
            "code": sell_signal["stock_code"],
            "name": sell_signal.get("short_name", ""),
            "ft": "sim_sell",
            "st": sell_signal["strategy_type"],
            "price": price,
            "shares": shares,
            "amount": amount,
            "fee": round(fee, 2),
            "reason": SimTradeEngine._reason_label(reason),
            "ai": sell_signal.get("ai_score", 0),
            "date": now.date(),
            "time": now.strftime("%H:%M"),
        })

        return {"status": "ok", "stock_code": sell_signal["stock_code"], "profit": profit,
                "profit_rate": profit_rate, "reason": reason}

    # ────────────────────────────────────────
    # 辅助方法
    # ────────────────────────────────────────

    def _get_holding_count(self, strategy_type: str) -> int:
        rows = _read_sql("""
            SELECT COUNT(*) AS cnt FROM st_sim_position
            WHERE strategy_type = :st AND status = 'holding'
        """, {"st": strategy_type})
        return int(rows[0]["cnt"]) if rows else 0

    def _get_holding_codes(self, strategy_type: str) -> set:
        rows = _read_sql("""
            SELECT stock_code FROM st_sim_position
            WHERE strategy_type = :st AND status = 'holding'
        """, {"st": strategy_type})
        return {r["stock_code"] for r in rows}

    # ────────────────────────────────────────
    # 回测执行(使用K线历史数据)
    # ────────────────────────────────────────

    def backtest_buy(self, stock_code: str, strategy_type: str, trade_date: str,
                     analysis: dict = None) -> dict | None:
        """
        回测模式买入：使用指定日期的开盘价买入。
        analysis: 可选的评分数据 dict
        """
        cfg = STRATEGY_CONFIG.get(strategy_type)
        if not cfg:
            return None

        # 检查持仓上限
        holding_count = self._get_holding_count(strategy_type)
        if holding_count >= cfg["max_holding"]:
            return None

        # 检查是否已持仓
        holding_codes = self._get_holding_codes(strategy_type)
        if stock_code in holding_codes:
            return None

        # 获取开盘价
        rows = _read_sql("""
            SELECT open, close, high, low FROM sm_stock_kline
            WHERE stock_code = :c AND k_type = 1 AND trade_date = :d
        """, {"c": stock_code, "d": trade_date})
        if not rows or not rows[0].get("open"):
            return None

        open_price = float(rows[0]["open"])
        if open_price <= 0:
            return None

        shares = _calc_shares(open_price, cfg["buy_amount"])
        if shares <= 0:
            return None

        analysis = analysis or {}
        signal = {
            "stock_code": stock_code,
            "short_name": analysis.get("short_name", stock_code),
            "strategy_type": strategy_type,
            "price": open_price,
            "shares": shares,
            "amount": round(open_price * shares, 2),
            "ai_score": analysis.get("ai_score", 0),
            "short_score": analysis.get("short_score", 0),
            "long_score": analysis.get("long_score", 0),
            "capital_score": analysis.get("capital_score", 0),
            "technical_score": analysis.get("technical_score", 0),
            "fundamental_score": analysis.get("fundamental_score", 0),
            "event_risk_level": analysis.get("event_risk_level", "LOW"),
            "reason": analysis.get("reason", ""),
            "price_source": "backtest_open",
        }

        # 直接写入(回测模式)
        now = datetime.now()
        fee = portfolio_trade_fee("buy", open_price, shares)

        _exec_sql("""
            INSERT INTO st_sim_position
            (stock_code, short_name, strategy_type, buy_price, buy_amount, buy_shares,
             buy_date, buy_time, buy_reason, ai_score, short_score, long_score,
             capital_score, technical_score, fundamental_score, event_risk_level, status)
            VALUES (:code, :name, :st, :price, :amount, :shares,
                    :date, :time, :reason, :ai, :ss, :ls,
                    :cs, :ts, :fs, :risk, 'holding')
        """, {
            "code": stock_code,
            "name": analysis.get("short_name", ""),
            "st": strategy_type,
            "price": open_price,
            "amount": round(open_price * shares, 2),
            "shares": shares,
            "date": trade_date,
            "time": "09:30",
            "reason": json.dumps({"reason": analysis.get("reason", ""), "mode": "backtest"}, ensure_ascii=False),
            "ai": analysis.get("ai_score", 0),
            "ss": analysis.get("short_score", 0),
            "ls": analysis.get("long_score", 0),
            "cs": analysis.get("capital_score", 0),
            "ts": analysis.get("technical_score", 0),
            "fs": analysis.get("fundamental_score", 0),
            "risk": analysis.get("event_risk_level", "LOW"),
        })

        _exec_sql("""
            INSERT INTO st_trade_flow
            (stock_code, short_name, flow_type, source, strategy_type, trans_type,
             price, shares, amount, fee, reason, ai_score, trans_date, trans_time)
            VALUES (:code, :name, 'sim_buy', 'simulation', :st, 'buy',
                    :price, :shares, :amount, :fee, :reason, :ai, :date, '09:30')
        """, {
            "code": stock_code,
            "name": analysis.get("short_name", ""),
            "st": strategy_type,
            "price": open_price,
            "shares": shares,
            "amount": round(open_price * shares, 2),
            "fee": round(fee, 2),
            "reason": f"回测买入 AI评分{analysis.get('ai_score', 0)}",
            "ai": analysis.get("ai_score", 0),
            "date": trade_date,
        })

        return {"status": "ok", "stock_code": stock_code, "price": open_price,
                "shares": shares, "trade_date": trade_date}

    def backtest_check_sell(self, trade_date: str) -> list[dict]:
        """
        回测模式检查卖出：基于指定日期的收盘价检查卖出信号。
        """
        _ensure_tables()
        holdings = _read_sql("""
            SELECT id, stock_code, short_name, strategy_type,
                   buy_price, buy_shares, buy_date, buy_amount, ai_score
            FROM st_sim_position
            WHERE status = 'holding'
        """)

        signals = []
        for h in holdings:
            code = h["stock_code"]
            cfg = STRATEGY_CONFIG.get(h["strategy_type"])
            if not cfg:
                continue

            # 获取当日K线
            rows = _read_sql("""
                SELECT open, close, high, low FROM sm_stock_kline
                WHERE stock_code = :c AND k_type = 1 AND trade_date = :d
            """, {"c": code, "d": trade_date})
            if not rows or not rows[0].get("close"):
                continue

            close_price = float(rows[0]["close"])
            high_price = float(rows[0].get("high") or close_price)
            buy_price = float(h["buy_price"])

            if buy_price <= 0:
                continue

            # 当日收益率
            profit_rate = ((close_price - buy_price) / buy_price) * 100
            # 当日最高收益率
            high_rate = ((high_price - buy_price) / buy_price) * 100

            buy_date = h["buy_date"]
            if isinstance(buy_date, str):
                buy_d = datetime.strptime(buy_date[:10], "%Y-%m-%d").date()
            else:
                buy_d = buy_date
            td = datetime.strptime(trade_date[:10], "%Y-%m-%d").date()
            holding_days = (td - buy_d).days

            reason = ""
            should_sell = False

            # 止盈: 当日最高价达到止盈线(假设盘中触发)
            if high_rate >= cfg["take_profit"]:
                reason = "take_profit"
                should_sell = True
                # 卖出价按止盈价计算
                sell_price = round(buy_price * (1 + cfg["take_profit"] / 100), 2)
            # 止损: 当日最低价触及止损线
            elif float(rows[0].get("low") or 0) > 0:
                low_rate = ((float(rows[0]["low"]) - buy_price) / buy_price) * 100
                if low_rate <= cfg["stop_loss"]:
                    reason = "stop_loss"
                    should_sell = True
                    sell_price = round(buy_price * (1 + cfg["stop_loss"] / 100), 2)
                elif holding_days >= cfg["max_days"]:
                    reason = "time_limit"
                    should_sell = True
                    sell_price = close_price
                elif cfg["trailing_activate"] and high_rate >= cfg["trailing_activate"]:
                    if (high_rate - profit_rate) >= cfg["trailing_drawdown"]:
                        reason = "trailing_stop"
                        should_sell = True
                        sell_price = close_price
            elif holding_days >= cfg["max_days"]:
                reason = "time_limit"
                should_sell = True
                sell_price = close_price

            if should_sell:
                shares = int(h["buy_shares"])
                sell_fee = portfolio_trade_fee("sell", sell_price, shares)
                buy_fee = portfolio_trade_fee("buy", buy_price, shares)
                total_fee = round(buy_fee + sell_fee, 2)
                gross_profit = round((sell_price - buy_price) * shares, 2)
                net_profit = round(gross_profit - total_fee, 2)
                final_rate = ((sell_price - buy_price) / buy_price) * 100

                signals.append({
                    "position_id": h["id"],
                    "stock_code": code,
                    "short_name": h.get("short_name") or code,
                    "strategy_type": h["strategy_type"],
                    "buy_price": buy_price,
                    "sell_price": sell_price,
                    "shares": shares,
                    "profit_rate": round(final_rate, 2),
                    "profit": net_profit,
                    "fee": total_fee,
                    "holding_days": holding_days,
                    "reason": reason,
                    "reason_label": self._reason_label(reason),
                    "ai_score": float(h.get("ai_score") or 0),
                })

        return signals
