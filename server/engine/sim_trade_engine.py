# -*- coding: utf-8 -*-
"""
模拟交易引擎

多策略的买入/卖出信号检测，以及交易执行逻辑。
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
from server.common.minute_data import (
    get_first_stock_minute_price,
    get_max_stock_minute_price,
    get_stock_minute_prices,
)

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
    "main_wave": {
        "name": "主升浪",
        "max_holding": 2,
        "max_days": 60,
        "take_profit": 80.0,
        "stop_loss": -10.0,
        "trailing_activate": 35.0,
        "trailing_drawdown": 8.0,
        "buy_amount": 100000,
        "min_ai_score": 74,
        "min_short_score": 0,
        "min_capital_score": 0,
        "min_technical_score": 0,
        "min_main_wave_score": 74,
        "min_trend_hold_score": 58,
        "max_risk_level": "LOW",
    },
}

RISK_ORDER = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
VALID_TRADE_MODES = {"live", "backtest", "forward"}
SNAPSHOT_FALLBACK_MAX_AGE_MINUTES = 5


def _safe_float(v, default=0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def _sina_symbol(stock_code: str) -> str:
    code = str(stock_code).strip().zfill(6)
    if code.startswith(("43", "83", "87", "92")):
        return "bj" + code
    if code.startswith(("6", "9")):
        return "sh" + code
    return "sz" + code


def _score_value(data: dict, *keys: str, default=0.0) -> float:
    for key in keys:
        if key in data and data.get(key) is not None:
            return _safe_float(data.get(key), default)
    return default


def build_buy_decision(strategy_type: str, candidate: dict) -> dict:
    """
    将 AI 推荐记录转换成明确的买入判断。

    规则口径：
    - 推荐资格必须是 ALLOW
    - AI 综合评分达到策略阈值，当前默认为 >=70
    - 风险等级不超过策略允许上限
    - 再按超短/短线/波段分别检查资金、技术、基本面等确认项
    """
    cfg = STRATEGY_CONFIG.get(strategy_type)
    if not cfg:
        return {"allowed": False, "analysis": {}, "reason": f"未知策略: {strategy_type}"}

    short_score = _score_value(candidate, "short_score", "short_term_score")
    ai_score = _score_value(candidate, "ai_score", default=short_score)
    if ai_score <= 0:
        ai_score = short_score
    final_trade_score = _score_value(candidate, "final_trade_score", default=ai_score)
    if final_trade_score <= 0:
        final_trade_score = ai_score
    quality_score = _score_value(candidate, "quality_score", default=ai_score)
    entry_score = _score_value(candidate, "entry_score", default=final_trade_score)
    expected_return_pct = _score_value(candidate, "expected_return_pct", default=0)
    heat_overload_score = _score_value(candidate, "heat_overload_score", default=60)
    confidence_score = _score_value(candidate, "confidence_score", default=62)
    main_wave_score = _score_value(candidate, "main_wave_score", default=0)
    trend_hold_score = _score_value(candidate, "trend_hold_score", default=0)
    main_wave_signal = str(candidate.get("main_wave_signal") or "NONE").upper()
    if strategy_type == "main_wave" and main_wave_score > 0:
        final_trade_score = main_wave_score
    long_score = _score_value(candidate, "long_score", "long_term_score")
    capital_score = _score_value(candidate, "capital_score")
    technical_score = _score_value(candidate, "technical_score", "technical")
    fundamental_score = _score_value(candidate, "fundamental_score", "fundamental")
    risk_level = str(candidate.get("event_risk_level") or "LOW").upper()
    recommend_status = str(candidate.get("recommend_status") or "ALLOW").upper()
    signal_status = str(candidate.get("signal_status") or "CONFIRM").upper()

    analysis = {
        "ai_score": ai_score,
        "quality_score": quality_score,
        "entry_score": entry_score,
        "final_trade_score": final_trade_score,
        "expected_return_pct": expected_return_pct,
        "heat_overload_score": heat_overload_score,
        "confidence_score": confidence_score,
        "main_wave_score": main_wave_score,
        "trend_hold_score": trend_hold_score,
        "main_wave_signal": main_wave_signal,
        "short_score": short_score,
        "long_score": long_score,
        "capital_score": capital_score,
        "technical_score": technical_score,
        "fundamental_score": fundamental_score,
        "event_risk_level": risk_level,
        "signal_status": signal_status,
        "short_name": candidate.get("short_name") or candidate.get("stock_name") or "",
        "orig_reason": candidate.get("reason") or candidate.get("summary") or "",
        "sources": candidate.get("sources") or "",
    }

    blockers = []
    reason_parts = []

    if recommend_status != "ALLOW":
        blockers.append(f"推荐资格为{recommend_status}，不是ALLOW")
    else:
        reason_parts.append("推荐资格ALLOW")

    if signal_status in {"BLOCK", "WATCH", "SELL_ALERT"}:
        blockers.append(f"AI signal status is {signal_status}; waiting for confirmation")
    elif signal_status:
        reason_parts.append(f"AI signal status {signal_status}")

    if final_trade_score < cfg["min_ai_score"]:
        blockers.append(f"最终交易评分{final_trade_score:.0f}分低于{cfg['min_ai_score']}分")
    else:
        reason_parts.append(f"最终交易评分{final_trade_score:.0f}分>={cfg['min_ai_score']}分")

    if entry_score < 55:
        blockers.append(f"买点评分{entry_score:.0f}分不足")
    else:
        reason_parts.append(f"买点评分{entry_score:.0f}分确认")

    if strategy_type != "main_wave" and expected_return_pct and expected_return_pct < 5:
        blockers.append(f"预期上涨空间{expected_return_pct:.1f}%不足5%")
    if heat_overload_score < 50:
        blockers.append(f"热度拥挤度健康分{heat_overload_score:.0f}过低")
    if confidence_score < 45:
        blockers.append(f"推荐一致性{confidence_score:.0f}分过低")

    if not _check_risk_level(risk_level, cfg["max_risk_level"]):
        blockers.append(f"风险等级{risk_level}超过允许上限{cfg['max_risk_level']}")
    else:
        reason_parts.append(f"风险等级{risk_level}(允许{cfg['max_risk_level']})")

    if strategy_type == "ultra_short":
        if short_score < cfg["min_short_score"]:
            blockers.append(f"短期评分{short_score:.0f}分低于{cfg['min_short_score']}分")
        else:
            reason_parts.append(f"短期评分{short_score:.0f}分>={cfg['min_short_score']}分")
        if capital_score < cfg["min_capital_score"]:
            blockers.append(f"资金面{capital_score:.0f}分低于{cfg['min_capital_score']}分")
        else:
            reason_parts.append(f"资金面{capital_score:.0f}分>={cfg['min_capital_score']}分")
    elif strategy_type == "short_term":
        if short_score < cfg["min_short_score"]:
            blockers.append(f"短期评分{short_score:.0f}分低于{cfg['min_short_score']}分")
        else:
            reason_parts.append(f"短期评分{short_score:.0f}分>={cfg['min_short_score']}分")
        if technical_score < cfg["min_technical_score"]:
            blockers.append(f"技术面{technical_score:.0f}分低于{cfg['min_technical_score']}分")
        else:
            reason_parts.append(f"技术面{technical_score:.0f}分>={cfg['min_technical_score']}分")
    elif strategy_type == "swing":
        min_long = cfg.get("min_long_score", 0)
        min_fundamental = cfg.get("min_fundamental_score", 0)
        if long_score < min_long:
            blockers.append(f"长期评分{long_score:.0f}分低于{min_long}分")
        else:
            reason_parts.append(f"长期评分{long_score:.0f}分>={min_long}分")
        if fundamental_score < min_fundamental:
            blockers.append(f"基本面{fundamental_score:.0f}分低于{min_fundamental}分")
        else:
            reason_parts.append(f"基本面{fundamental_score:.0f}分>={min_fundamental}分")
    elif strategy_type == "main_wave":
        min_main_wave = cfg.get("min_main_wave_score", 0)
        min_hold = cfg.get("min_trend_hold_score", 0)
        if main_wave_score < min_main_wave:
            blockers.append(f"主升浪评分{main_wave_score:.0f}分低于{min_main_wave}分")
        else:
            reason_parts.append(f"主升浪评分{main_wave_score:.0f}分>={min_main_wave}分")
        if trend_hold_score < min_hold:
            blockers.append(f"趋势持有评分{trend_hold_score:.0f}分低于{min_hold}分")
        else:
            reason_parts.append(f"趋势持有评分{trend_hold_score:.0f}分>={min_hold}分")
        if main_wave_signal not in {"BUY_READY", "WATCH"}:
            blockers.append(f"主升浪信号为{main_wave_signal}，不是买点")
        elif main_wave_signal == "BUY_READY":
            reason_parts.append("主升浪买点就绪")

    if analysis["sources"]:
        reason_parts.append(f"来源:{analysis['sources']}")
    if analysis["orig_reason"]:
        reason_parts.append(f"AI摘要:{analysis['orig_reason']}")

    if blockers:
        return {"allowed": False, "analysis": analysis, "reason": "；".join(blockers)}
    return {"allowed": True, "analysis": analysis, "reason": "；".join(reason_parts)}


def _random_intraday_time(seed: str, session: str = "any") -> str:
    """
    生成一个随机的盘中时间(HH:MM:SS)。
    seed: 用于确定性随机的种子(如股票代码+日期)
    session: 'am'=上午 9:30-11:30, 'pm'=下午 13:00-15:00, 'any'=任意盘中
    """
    import random
    r = random.Random(seed)
    if session == "am":
        minutes = r.randint(0, 120)  # 9:30 开始的分钟偏移
        h, m = 9 + (30 + minutes) // 60, (30 + minutes) % 60
    elif session == "pm":
        minutes = r.randint(0, 120)  # 13:00 开始的分钟偏移
        h, m = 13 + minutes // 60, minutes % 60
    else:
        minutes = r.randint(0, 330)  # 全天 9:25-15:00 共335分钟
        total = 9 * 60 + 25 + minutes
        h, m = total // 60, total % 60
        if h >= 11 and m > 30 and h < 13:  # 跳过午休
            h, m = 13, m - 30
    s = r.randint(0, 59)
    return f"{h:02d}:{m:02d}:{s:02d}"


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


def _table_columns(table_name: str) -> set[str]:
    rows = _read_sql("""
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = :table_name
    """, {"table_name": table_name})
    return {str(r.get("COLUMN_NAME")) for r in rows}


def _select_expr(columns: set[str], column: str, default: str, alias: str = "") -> str:
    alias = alias or column
    if column in columns:
        return f"`{column}` AS `{alias}`"
    return f"{default} AS `{alias}`"


def fetch_recommended_candidates(pick_date: str) -> list[dict]:
    """读取推荐表，兼容旧版 st_recommended_stocks 缺少新评分列的情况。"""
    columns = _table_columns("st_recommended_stocks")
    if not columns:
        return []

    long_expr = (
        "`long_term_score` AS `long_term_score`"
        if "long_term_score" in columns else
        "`fundamental` AS `long_term_score`"
        if "fundamental" in columns else
        "0 AS `long_term_score`"
    )
    short_expr = (
        "`short_term_score` AS `short_term_score`"
        if "short_term_score" in columns else
        "`ai_score` AS `short_term_score`"
        if "ai_score" in columns else
        "0 AS `short_term_score`"
    )
    status_filter = "AND (recommend_status IS NULL OR recommend_status = 'ALLOW')" if "recommend_status" in columns else ""

    return _read_sql(f"""
        SELECT
            {_select_expr(columns, "stock_code", "''")},
            {_select_expr(columns, "short_name", "''")},
            {_select_expr(columns, "ai_score", "0")},
            {long_expr},
            {short_expr},
            {_select_expr(columns, "fundamental", "0")},
            {_select_expr(columns, "capital_score", "0")},
            {_select_expr(columns, "valuation", "0")},
            {_select_expr(columns, "technical", "0")},
            {_select_expr(columns, "reason", "''")},
            {_select_expr(columns, "sources", "''")},
            {_select_expr(columns, "recommend_status", "'ALLOW'")},
            {_select_expr(columns, "recommend_reason", "''")},
            {_select_expr(columns, "event_risk_level", "'LOW'")},
            {_select_expr(columns, "sentiment_score", "0")},
            {_select_expr(columns, "market_mood_score", "0")},
            {_select_expr(columns, "event_score", "0")},
            {_select_expr(columns, "signal_status", "'CONFIRM'")},
            {_select_expr(columns, "primary_strategy", "''")},
            {_select_expr(columns, "entry_price_low", "NULL")},
            {_select_expr(columns, "entry_price_high", "NULL")},
            {_select_expr(columns, "stop_loss_price", "NULL")},
            {_select_expr(columns, "take_profit_1", "NULL")},
            {_select_expr(columns, "take_profit_2", "NULL")},
            {_select_expr(columns, "position_weight", "NULL")},
            {_select_expr(columns, "max_holding_days", "NULL")},
            {_select_expr(columns, "quality_score", "0")},
            {_select_expr(columns, "entry_score", "0")},
            {_select_expr(columns, "final_trade_score", "0")},
            {_select_expr(columns, "expected_return_pct", "0")},
            {_select_expr(columns, "heat_overload_score", "0")},
            {_select_expr(columns, "confidence_score", "0")},
            {_select_expr(columns, "main_wave_score", "0")},
            {_select_expr(columns, "trend_hold_score", "0")},
            {_select_expr(columns, "main_wave_signal", "'NONE'")},
            {_select_expr(columns, "main_wave_reason", "''")},
            {_select_expr(columns, "trend_stop_price", "NULL")},
            {_select_expr(columns, "trend_reduce_price", "NULL")},
            {_select_expr(columns, "suitable_strategies", "''")}
        FROM st_recommended_stocks
        WHERE pick_date = :d
          {status_filter}
        ORDER BY ai_score DESC
    """, {"d": pick_date})


def _ensure_tables():
    """确保模拟交易相关表存在"""
    try:
        _exec_sql("""
            CREATE TABLE IF NOT EXISTS `st_sim_position` (
                `id`              BIGINT AUTO_INCREMENT PRIMARY KEY,
                `stock_code`      VARCHAR(10)  NOT NULL,
                `short_name`      VARCHAR(20)  DEFAULT '',
                `strategy_type`   VARCHAR(20)  NOT NULL,
                `trade_mode`      VARCHAR(20)  DEFAULT 'live',
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
                `created_at`      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
                `updated_at`      DATETIME     DEFAULT NULL,
                INDEX `idx_strategy_status` (`strategy_type`, `status`),
                INDEX `idx_trade_mode` (`trade_mode`, `strategy_type`, `status`),
                INDEX `idx_stock_code` (`stock_code`),
                INDEX `idx_buy_date` (`buy_date`),
                INDEX `idx_status` (`status`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        try:
            _exec_sql("ALTER TABLE `st_sim_position` ADD COLUMN `trade_mode` VARCHAR(20) DEFAULT 'live' AFTER `strategy_type`")
        except Exception:
            pass
        try:
            _exec_sql("ALTER TABLE `st_sim_position` ADD INDEX `idx_trade_mode` (`trade_mode`, `strategy_type`, `status`)")
        except Exception:
            pass
        try:
            _exec_sql("ALTER TABLE `st_sim_position` MODIFY COLUMN `sell_reason` TEXT")
        except Exception:
            pass
        _exec_sql("""
            CREATE TABLE IF NOT EXISTS `st_trade_flow` (
                `id`              BIGINT AUTO_INCREMENT PRIMARY KEY,
                `stock_code`      VARCHAR(10)  NOT NULL,
                `short_name`      VARCHAR(20)  DEFAULT '',
                `flow_type`       VARCHAR(20)  NOT NULL,
                `source`          VARCHAR(20)  NOT NULL,
                `strategy_type`   VARCHAR(20)  DEFAULT '',
                `trade_mode`      VARCHAR(20)  DEFAULT 'live',
                `trans_type`      VARCHAR(10)  NOT NULL,
                `price`           DECIMAL(12,4) NOT NULL,
                `shares`          INT          NOT NULL,
                `amount`          DECIMAL(14,2) NOT NULL,
                `fee`             DECIMAL(10,2) DEFAULT 0,
                `reason`          VARCHAR(200) DEFAULT '',
                `ai_score`        DECIMAL(5,2) DEFAULT 0,
                `trans_date`      DATE         NOT NULL,
                `trans_time`      VARCHAR(20)  DEFAULT '',
                `created_at`      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
                INDEX `idx_flow_type` (`flow_type`),
                INDEX `idx_source` (`source`),
                INDEX `idx_stock_date` (`stock_code`, `trans_date`),
                INDEX `idx_trans_date` (`trans_date`),
                INDEX `idx_strategy` (`strategy_type`, `trans_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        try:
            _exec_sql("ALTER TABLE `st_trade_flow` ADD COLUMN `trade_mode` VARCHAR(20) DEFAULT 'live' AFTER `strategy_type`")
        except Exception:
            pass
        try:
            _exec_sql("ALTER TABLE `st_trade_flow` ADD INDEX `idx_trade_mode` (`trade_mode`, `strategy_type`, `trans_date`)")
        except Exception:
            pass
        try:
            _exec_sql("ALTER TABLE `st_trade_flow` MODIFY COLUMN `reason` TEXT")
        except Exception:
            pass
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
                `created_at`      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
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
        url = f"https://hq.sinajs.cn/list={_sina_symbol(code)}"
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
        symbols = ",".join(_sina_symbol(c) for c in clean)
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
            rows = _read_sql(f"""
                SELECT price, snapshot_at FROM sm_rt_quote_snapshot
                WHERE stock_code = :c
                  AND snapshot_at >= NOW() - INTERVAL {SNAPSHOT_FALLBACK_MAX_AGE_MINUTES} MINUTE
                ORDER BY snapshot_at DESC
                LIMIT 1
            """, {"c": code})
            if rows and rows[0].get("price") and float(rows[0]["price"]) > 0:
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


def _minute_time_text(value) -> str:
    """Return HH:MM:SS from a DB datetime/time value."""
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%H:%M:%S")
    s = str(value)
    if " " in s:
        s = s.split(" ", 1)[1]
    if len(s) == 5:
        return s + ":00"
    return s[:8]


def get_first_minute_price(stock_code: str, trade_date: str) -> dict | None:
    """Get the first available intraday minute price on a validation date."""
    code = str(stock_code).strip().zfill(6)
    row = get_first_stock_minute_price(code, trade_date)
    if not row:
        return None
    return {
        "price": float(row["price"]),
        "change_pct": _safe_float(row.get("change_pct")),
        "time": _minute_time_text(row.get("trade_time")),
        "source": row.get("source") or "minute_open",
        "trade_time": str(row.get("trade_time") or ""),
    }


def get_minute_prices(stock_code: str, start_date: str, end_date: str = "") -> list[dict]:
    """Read ordered minute prices for a stock over a date range."""
    code = str(stock_code).strip().zfill(6)
    end_date = end_date or start_date
    return get_stock_minute_prices(code, start_date, end_date)


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


def _is_near_limit_up(price_info: dict | None) -> bool:
    """A股模拟撮合：接近涨停时不模拟追价买入。"""
    if not price_info:
        return False
    return _safe_float(price_info.get("change_pct")) >= 9.7


def _is_near_limit_down(price_info: dict | None) -> bool:
    """A股模拟撮合：接近跌停时认为卖出流动性不足。"""
    if not price_info:
        return False
    return _safe_float(price_info.get("change_pct")) <= -9.7


def _open_change_pct(row: dict) -> float | None:
    open_price = _safe_float(row.get("open"))
    pre_close = _safe_float(row.get("pre_close"))
    if open_price > 0 and pre_close > 0:
        return (open_price - pre_close) / pre_close * 100
    return None


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
        if not _is_trading_time():
            logger.info("非交易时间，跳过实时模拟买入扫描: %s", strategy_type)
            return []

        _ensure_tables()

        # 检查当前持仓数
        holding_count = self._get_holding_count(strategy_type, trade_mode="live")
        if holding_count >= cfg["max_holding"]:
            return []

        # 获取已持仓的股票代码
        holding_codes = self._get_holding_codes(strategy_type, trade_mode="live")

        # 从推荐表获取最新数据
        latest_pick = _read_sql("SELECT MAX(pick_date) AS d FROM st_recommended_stocks", {})
        if not latest_pick or not latest_pick[0].get("d"):
            return []
        pick_date = str(latest_pick[0]["d"])[:10]

        candidates = fetch_recommended_candidates(pick_date)

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

            decision = build_buy_decision(strategy_type, c)
            if not decision["allowed"]:
                continue
            analysis = decision["analysis"]

            # 获取当前价格：优先用批量行情
            price_info = live_prices.get(code)
            if not price_info or price_info.get("price", 0) <= 0:
                price_info = _get_current_price(code)
            if not price_info or price_info["price"] <= 0:
                continue

            price = price_info["price"]
            if _is_near_limit_up(price_info):
                continue
            entry_low = _safe_float(c.get("entry_price_low"), 0.0)
            entry_high = _safe_float(c.get("entry_price_high"), 0.0)
            if entry_low > 0 and entry_high > 0 and not (entry_low <= price <= entry_high):
                continue
            shares = _calc_shares(price, cfg["buy_amount"])
            if shares <= 0:
                continue

            signals.append({
                "stock_code": code,
                "short_name": c.get("short_name") or code,
                "strategy_type": strategy_type,
                "trade_mode": "live",
                "price": price,
                "shares": shares,
                "amount": round(price * shares, 2),
                "ai_score": analysis["ai_score"],
                "quality_score": analysis.get("quality_score"),
                "entry_score": analysis.get("entry_score"),
                "final_trade_score": analysis.get("final_trade_score"),
                "expected_return_pct": analysis.get("expected_return_pct"),
                "short_score": analysis["short_score"],
                "long_score": analysis["long_score"],
                "capital_score": analysis["capital_score"],
                "technical_score": analysis["technical_score"],
                "fundamental_score": analysis["fundamental_score"],
                "event_risk_level": analysis["event_risk_level"],
                "signal_status": analysis.get("signal_status"),
                "entry_price_low": c.get("entry_price_low"),
                "entry_price_high": c.get("entry_price_high"),
                "stop_loss_price": c.get("stop_loss_price"),
                "take_profit_1": c.get("take_profit_1"),
                "take_profit_2": c.get("take_profit_2"),
                "reason": decision["reason"],
                "orig_reason": analysis["orig_reason"],
                "price_source": price_info.get("source", ""),
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
        if not _is_trading_time():
            logger.info("非交易时间，跳过实时模拟卖出扫描")
            return []
        _ensure_tables()
        holdings = _read_sql("""
            SELECT id, stock_code, short_name, strategy_type,
                   buy_price, buy_shares, buy_date, buy_amount,
                   ai_score, profit_rate
            FROM st_sim_position
            WHERE status = 'holding'
              AND COALESCE(trade_mode, 'live') = 'live'
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
            if _is_near_limit_down(price_info):
                continue

            # 计算持仓天数
            buy_date = h["buy_date"]
            if isinstance(buy_date, str):
                buy_d = datetime.strptime(buy_date[:10], "%Y-%m-%d").date()
            else:
                buy_d = buy_date
            holding_days = (date.today() - buy_d).days

            # A股 T+1：当天买入的仓位不能当天卖出。
            if holding_days < 1:
                continue

            reason = ""
            reason_detail = ""
            should_sell = False

            # 1. 止盈检查
            if profit_rate >= cfg["take_profit"]:
                reason = "take_profit"
                reason_detail = f"盈利{profit_rate:.2f}%已达止盈线{cfg['take_profit']}%, 买入价{buy_price:.2f}→现价{current_price:.2f}, 持仓{holding_days}天"
                should_sell = True

            # 2. 止损检查
            elif profit_rate <= cfg["stop_loss"]:
                reason = "stop_loss"
                reason_detail = f"亏损{profit_rate:.2f}%已触及止损线{cfg['stop_loss']}%, 买入价{buy_price:.2f}→现价{current_price:.2f}, 持仓{holding_days}天, 及时止损避免更大亏损"
                should_sell = True

            # 3. 时间止损
            elif holding_days >= cfg["max_days"]:
                reason = "time_limit"
                reason_detail = f"持仓已达{holding_days}天, 超过{cfg['name']}策略最大持仓{cfg['max_days']}天, 收益率{profit_rate:.2f}%, 释放资金寻找新机会"
                should_sell = True

            # 4. 动态止盈(如果配置了)
            elif cfg["trailing_activate"] and profit_rate >= cfg["trailing_activate"]:
                max_rate = self._get_max_profit_rate(h["id"], code, buy_price)
                if max_rate is not None and (max_rate - profit_rate) >= cfg["trailing_drawdown"]:
                    reason = "trailing_stop"
                    reason_detail = f"最高盈利{max_rate:.2f}%回撤至{profit_rate:.2f}%, 回撤幅度{max_rate - profit_rate:.2f}%超过阈值{cfg['trailing_drawdown']}%, 保护利润离场"
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
                    "trade_mode": "live",
                    "buy_price": buy_price,
                    "sell_price": current_price,
                    "shares": shares,
                    "profit_rate": round(profit_rate, 2),
                    "profit": net_profit,
                    "fee": total_fee,
                    "holding_days": holding_days,
                    "reason": reason,
                    "reason_detail": reason_detail,
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
        trade_mode = signal.get("trade_mode") or "live"
        if trade_mode not in VALID_TRADE_MODES:
            raise ValueError(f"不允许写入未知模拟交易模式: {trade_mode}")
        buy_date = signal.get("buy_date") or now.date()
        buy_time = signal.get("buy_time") or now.strftime("%H:%M:%S")
        price = signal["price"]
        shares = signal["shares"]
        amount = round(price * shares, 2)
        fee = portfolio_trade_fee("buy", price, shares)

        # 写入持仓表
        _exec_sql("""
            INSERT INTO st_sim_position
            (stock_code, short_name, strategy_type, trade_mode, buy_price, buy_amount, buy_shares,
             buy_date, buy_time, buy_reason, ai_score, short_score, long_score,
             capital_score, technical_score, fundamental_score, event_risk_level, status)
            VALUES (:code, :name, :st, :mode, :price, :amount, :shares,
                    :date, :time, :reason, :ai, :ss, :ls,
                    :cs, :ts, :fs, :risk, 'holding')
        """, {
            "code": signal["stock_code"],
            "name": signal.get("short_name", ""),
            "st": signal["strategy_type"],
            "mode": trade_mode,
            "price": price,
            "amount": amount,
            "shares": shares,
            "date": buy_date,
            "time": buy_time,
            "reason": signal.get("reason", ""),
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
            (stock_code, short_name, flow_type, source, strategy_type, trade_mode, trans_type,
             price, shares, amount, fee, reason, ai_score, trans_date, trans_time)
            VALUES (:code, :name, :ft, 'simulation', :st, :mode, 'buy',
                    :price, :shares, :amount, :fee, :reason, :ai, :date, :time)
        """, {
            "code": signal["stock_code"],
            "name": signal.get("short_name", ""),
            "ft": "sim_buy",
            "st": signal["strategy_type"],
            "mode": trade_mode,
            "price": price,
            "shares": shares,
            "amount": amount,
            "fee": round(fee, 2),
            "reason": signal.get("reason", ""),
            "ai": signal.get("ai_score", 0),
            "date": buy_date,
            "time": buy_time,
        })

        return {"status": "ok", "stock_code": signal["stock_code"], "price": price,
                "shares": shares, "amount": amount, "fee": round(fee, 2)}

    def execute_sell(self, sell_signal: dict) -> dict:
        """执行模拟卖出"""
        _ensure_tables()
        now = datetime.now()
        trade_mode = sell_signal.get("trade_mode") or "live"
        if trade_mode not in VALID_TRADE_MODES:
            raise ValueError(f"不允许写入未知模拟交易模式: {trade_mode}")
        sell_date = sell_signal.get("sell_date") or now.date()
        position_id = sell_signal["position_id"]
        price = sell_signal["sell_price"]
        shares = sell_signal["shares"]
        amount = round(price * shares, 2)
        fee = sell_signal.get("fee", 0)
        profit = sell_signal["profit"]
        profit_rate = sell_signal["profit_rate"]
        reason = sell_signal["reason"]
        reason_detail = sell_signal.get("reason_detail") or self._reason_label(reason)
        sell_time = sell_signal.get("sell_time") or now.strftime("%H:%M:%S")

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
            "date": sell_date,
            "time": sell_time,
            "reason": reason_detail,
            "profit": profit,
            "rate": profit_rate,
            "days": sell_signal.get("holding_days", 0),
            "fee": fee,
            "id": position_id,
        })

        # 写入流水表
        _exec_sql("""
            INSERT INTO st_trade_flow
            (stock_code, short_name, flow_type, source, strategy_type, trade_mode, trans_type,
             price, shares, amount, fee, reason, ai_score, trans_date, trans_time)
            VALUES (:code, :name, :ft, 'simulation', :st, :mode, 'sell',
                    :price, :shares, :amount, :fee, :reason, :ai, :date, :time)
        """, {
            "code": sell_signal["stock_code"],
            "name": sell_signal.get("short_name", ""),
            "ft": "sim_sell",
            "st": sell_signal["strategy_type"],
            "mode": trade_mode,
            "price": price,
            "shares": shares,
            "amount": amount,
            "fee": round(fee, 2),
            "reason": reason_detail,
            "ai": sell_signal.get("ai_score", 0),
            "date": sell_date,
            "time": sell_time,
        })

        return {"status": "ok", "stock_code": sell_signal["stock_code"], "profit": profit,
                "profit_rate": profit_rate, "reason": reason}

    # ────────────────────────────────────────
    # 辅助方法
    # ────────────────────────────────────────

    def _get_holding_count(self, strategy_type: str, trade_mode: str = "live") -> int:
        rows = _read_sql("""
            SELECT COUNT(*) AS cnt FROM st_sim_position
            WHERE strategy_type = :st AND status = 'holding'
              AND COALESCE(trade_mode, 'live') = :mode
        """, {"st": strategy_type, "mode": trade_mode})
        return int(rows[0]["cnt"]) if rows else 0

    def _get_holding_codes(self, strategy_type: str, trade_mode: str = "live") -> set:
        rows = _read_sql("""
            SELECT stock_code FROM st_sim_position
            WHERE strategy_type = :st AND status = 'holding'
              AND COALESCE(trade_mode, 'live') = :mode
        """, {"st": strategy_type, "mode": trade_mode})
        return {r["stock_code"] for r in rows}

    # ────────────────────────────────────────
    # 盘中验证执行(推荐日T -> 验证日T+1分时)
    # ────────────────────────────────────────

    def forward_buy(self, stock_code: str, strategy_type: str, trade_date: str,
                    analysis: dict = None, signal_date: str = "",
                    allow_live_price: bool = True) -> dict | None:
        """Forward validation buy: use first minute price on validation date."""
        cfg = STRATEGY_CONFIG.get(strategy_type)
        if not cfg:
            return None

        code = str(stock_code).strip().zfill(6)
        duplicate = _read_sql("""
            SELECT id
            FROM st_sim_position
            WHERE stock_code = :c
              AND strategy_type = :st
              AND buy_date = :d
              AND COALESCE(trade_mode, 'live') = 'forward'
              AND buy_reason LIKE :needle
            LIMIT 1
        """, {
            "c": code,
            "st": strategy_type,
            "d": trade_date,
            "needle": f"%信号日{signal_date}%",
        })
        if duplicate:
            return {"status": "skip", "reason": "duplicate_session", "stock_code": code}

        holding_count = self._get_holding_count(strategy_type, trade_mode="forward")
        if holding_count >= cfg["max_holding"]:
            return {"status": "skip", "reason": "max_holding", "stock_code": code}

        holding_codes = self._get_holding_codes(strategy_type, trade_mode="forward")
        if code in holding_codes:
            return {"status": "skip", "reason": "already_holding", "stock_code": code}

        price_info = get_first_minute_price(code, trade_date)
        if not price_info and allow_live_price and trade_date[:10] == date.today().isoformat() and _is_trading_time():
            live = _get_current_price(code)
            if live and live.get("price", 0) > 0 and live.get("source") != "kline_close":
                price_info = {
                    "price": float(live["price"]),
                    "change_pct": _safe_float(live.get("change_pct")),
                    "time": datetime.now().strftime("%H:%M:%S"),
                    "source": live.get("source", "live"),
                    "trade_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

        if not price_info:
            return {"status": "skip", "reason": "no_minute_price", "stock_code": code}

        buy_price = float(price_info["price"])
        if buy_price <= 0:
            return {"status": "skip", "reason": "bad_price", "stock_code": code}
        if _is_near_limit_up(price_info):
            return {"status": "skip", "reason": "limit_up_not_buyable", "stock_code": code}

        shares = _calc_shares(buy_price, cfg["buy_amount"])
        if shares <= 0:
            return {"status": "skip", "reason": "bad_shares", "stock_code": code}

        analysis = analysis or {}
        buy_time = price_info.get("time") or "09:30:00"
        base_reason = analysis.get("reason", "")
        reason = (
            f"盘中验证：信号日{signal_date}，验证日{trade_date}，"
            f"{buy_time}首条分时/实时价买入；价格来源{price_info.get('source', '')}；{base_reason}"
        )
        signal = {
            "stock_code": code,
            "short_name": analysis.get("short_name", code),
            "strategy_type": strategy_type,
            "trade_mode": "forward",
            "buy_date": trade_date,
            "buy_time": buy_time,
            "price": buy_price,
            "shares": shares,
            "amount": round(buy_price * shares, 2),
            "ai_score": analysis.get("ai_score", 0),
            "short_score": analysis.get("short_score", 0),
            "long_score": analysis.get("long_score", 0),
            "capital_score": analysis.get("capital_score", 0),
            "technical_score": analysis.get("technical_score", 0),
            "fundamental_score": analysis.get("fundamental_score", 0),
            "event_risk_level": analysis.get("event_risk_level", "LOW"),
            "reason": reason,
            "price_source": price_info.get("source", ""),
        }
        ret = self.execute_buy(signal)
        return {**ret, "trade_date": trade_date, "signal_date": signal_date, "buy_time": buy_time}

    def check_forward_sell_signals(self) -> list[dict]:
        """
        Check forward-validation holdings with current intraday price.
        This is for real-time paper validation after forward_buy has created positions.
        """
        _ensure_tables()
        holdings = _read_sql("""
            SELECT id, stock_code, short_name, strategy_type,
                   buy_price, buy_shares, buy_date, buy_amount, ai_score
            FROM st_sim_position
            WHERE status = 'holding'
              AND COALESCE(trade_mode, 'live') = 'forward'
        """)
        if not holdings:
            return []

        if not _is_trading_time():
            return []

        live_prices = _fetch_live_prices_batch([h["stock_code"] for h in holdings])

        signals = []
        today = date.today()
        for h in holdings:
            code = h["stock_code"]
            cfg = STRATEGY_CONFIG.get(h["strategy_type"])
            if not cfg:
                continue

            price_info = live_prices.get(code)
            if not price_info or price_info.get("price", 0) <= 0:
                price_info = _get_current_price(code)
            if not price_info or price_info.get("price", 0) <= 0 or price_info.get("source") == "kline_close":
                continue

            buy_date = h["buy_date"]
            if isinstance(buy_date, str):
                buy_d = datetime.strptime(buy_date[:10], "%Y-%m-%d").date()
            else:
                buy_d = buy_date
            holding_days = (today - buy_d).days
            if holding_days < 1:
                continue

            current_price = float(price_info["price"])
            buy_price = float(h["buy_price"])
            if buy_price <= 0:
                continue
            if _is_near_limit_down(price_info):
                continue
            profit_rate = ((current_price - buy_price) / buy_price) * 100

            reason = ""
            reason_detail = ""
            should_sell = False

            if profit_rate >= cfg["take_profit"]:
                reason = "take_profit"
                reason_detail = (
                    f"盘中验证卖出：实时收益{profit_rate:.2f}%达到止盈线{cfg['take_profit']}%, "
                    f"买入价{buy_price:.2f}→现价{current_price:.2f}, 持仓{holding_days}天"
                )
                should_sell = True
            elif profit_rate <= cfg["stop_loss"]:
                reason = "stop_loss"
                reason_detail = (
                    f"盘中验证卖出：实时收益{profit_rate:.2f}%触及止损线{cfg['stop_loss']}%, "
                    f"买入价{buy_price:.2f}→现价{current_price:.2f}, 持仓{holding_days}天"
                )
                should_sell = True
            elif holding_days >= cfg["max_days"]:
                reason = "time_limit"
                reason_detail = (
                    f"盘中验证卖出：持仓{holding_days}天达到{cfg['name']}策略最大持仓{cfg['max_days']}天, "
                    f"当前收益{profit_rate:.2f}%"
                )
                should_sell = True
            elif cfg["trailing_activate"] and profit_rate >= cfg["trailing_activate"]:
                max_rate = self._get_max_profit_rate(h["id"], code, buy_price)
                if max_rate is not None and (max_rate - profit_rate) >= cfg["trailing_drawdown"]:
                    reason = "trailing_stop"
                    reason_detail = (
                        f"盘中验证卖出：最高盈利{max_rate:.2f}%回撤至{profit_rate:.2f}%, "
                        f"回撤{max_rate - profit_rate:.2f}%超过阈值{cfg['trailing_drawdown']}%"
                    )
                    should_sell = True

            if should_sell:
                shares = int(h["buy_shares"])
                sell_fee = portfolio_trade_fee("sell", current_price, shares)
                buy_fee = portfolio_trade_fee("buy", buy_price, shares)
                total_fee = round(buy_fee + sell_fee, 2)
                net_profit = round((current_price - buy_price) * shares - total_fee, 2)
                signals.append({
                    "position_id": h["id"],
                    "stock_code": code,
                    "short_name": h.get("short_name") or code,
                    "strategy_type": h["strategy_type"],
                    "trade_mode": "forward",
                    "buy_price": buy_price,
                    "sell_price": current_price,
                    "shares": shares,
                    "profit_rate": round(profit_rate, 2),
                    "profit": net_profit,
                    "fee": total_fee,
                    "holding_days": holding_days,
                    "reason": reason,
                    "reason_detail": reason_detail,
                    "sell_date": today.isoformat(),
                    "sell_time": datetime.now().strftime("%H:%M:%S"),
                    "reason_label": self._reason_label(reason),
                    "ai_score": float(h.get("ai_score") or 0),
                })

        return signals

    def forward_check_sell_by_minute(self, trade_date: str) -> list[dict]:
        """Replay forward-validation sell rules with ordered minute prices."""
        _ensure_tables()
        holdings = _read_sql("""
            SELECT id, stock_code, short_name, strategy_type,
                   buy_price, buy_shares, buy_date, buy_time, buy_amount, ai_score
            FROM st_sim_position
            WHERE status = 'holding'
              AND COALESCE(trade_mode, 'live') = 'forward'
        """)

        signals = []
        td = datetime.strptime(trade_date[:10], "%Y-%m-%d").date()
        for h in holdings:
            code = h["stock_code"]
            cfg = STRATEGY_CONFIG.get(h["strategy_type"])
            if not cfg:
                continue

            buy_date = h["buy_date"]
            if isinstance(buy_date, str):
                buy_d = datetime.strptime(buy_date[:10], "%Y-%m-%d").date()
            else:
                buy_d = buy_date
            holding_days = (td - buy_d).days
            if holding_days < 1:
                continue

            rows = get_minute_prices(code, trade_date, trade_date)
            if not rows:
                continue

            buy_price = float(h["buy_price"])
            if buy_price <= 0:
                continue

            prior_max_rate = 0.0
            if cfg["trailing_activate"]:
                try:
                    max_price = get_max_stock_minute_price(code, str(buy_d), trade_date)
                    if max_price > 0:
                        prior_max_rate = ((max_price - buy_price) / buy_price) * 100
                except Exception:
                    prior_max_rate = 0.0

            max_rate = prior_max_rate
            latest_row = None
            for row in rows:
                latest_row = row
                price = float(row["price"])
                minute_time = _minute_time_text(row.get("trade_time"))
                if _is_near_limit_down(row):
                    continue
                profit_rate = ((price - buy_price) / buy_price) * 100
                max_rate = max(max_rate, profit_rate)

                reason = ""
                reason_detail = ""
                should_sell = False

                if profit_rate <= cfg["stop_loss"]:
                    reason = "stop_loss"
                    reason_detail = (
                        f"分钟线验证卖出：{trade_date} {minute_time} 分时收益{profit_rate:.2f}%"
                        f"触及止损线{cfg['stop_loss']}%, 买入价{buy_price:.2f}→分时价{price:.2f}, "
                        f"持仓{holding_days}天"
                    )
                    should_sell = True
                elif profit_rate >= cfg["take_profit"]:
                    reason = "take_profit"
                    reason_detail = (
                        f"分钟线验证卖出：{trade_date} {minute_time} 分时收益{profit_rate:.2f}%"
                        f"达到止盈线{cfg['take_profit']}%, 买入价{buy_price:.2f}→分时价{price:.2f}, "
                        f"持仓{holding_days}天"
                    )
                    should_sell = True
                elif cfg["trailing_activate"] and max_rate >= cfg["trailing_activate"]:
                    drawdown = max_rate - profit_rate
                    if drawdown >= cfg["trailing_drawdown"]:
                        reason = "trailing_stop"
                        reason_detail = (
                            f"分钟线验证卖出：最高盈利{max_rate:.2f}%回撤到{profit_rate:.2f}%, "
                            f"回撤{drawdown:.2f}%超过阈值{cfg['trailing_drawdown']}%, "
                            f"{trade_date} {minute_time} 离场"
                        )
                        should_sell = True

                if should_sell:
                    signals.append(self._build_forward_minute_sell_signal(
                        h, price, profit_rate, holding_days, reason, reason_detail,
                        trade_date, minute_time,
                    ))
                    break

            if not any(sig["position_id"] == h["id"] for sig in signals):
                if latest_row is not None and holding_days >= cfg["max_days"]:
                    price = float(latest_row["price"])
                    minute_time = _minute_time_text(latest_row.get("trade_time"))
                    profit_rate = ((price - buy_price) / buy_price) * 100
                    reason = "time_limit"
                    reason_detail = (
                        f"分钟线验证卖出：持仓{holding_days}天达到{cfg['name']}策略最大持仓"
                        f"{cfg['max_days']}天，按{trade_date} {minute_time}分时价{price:.2f}离场，"
                        f"收益{profit_rate:.2f}%"
                    )
                    signals.append(self._build_forward_minute_sell_signal(
                        h, price, profit_rate, holding_days, reason, reason_detail,
                        trade_date, minute_time,
                    ))

        return signals

    def _build_forward_minute_sell_signal(self, holding: dict, sell_price: float,
                                          profit_rate: float, holding_days: int,
                                          reason: str, reason_detail: str,
                                          sell_date: str, sell_time: str) -> dict:
        buy_price = float(holding["buy_price"])
        shares = int(holding["buy_shares"])
        sell_fee = portfolio_trade_fee("sell", sell_price, shares)
        buy_fee = portfolio_trade_fee("buy", buy_price, shares)
        total_fee = round(buy_fee + sell_fee, 2)
        net_profit = round((sell_price - buy_price) * shares - total_fee, 2)
        return {
            "position_id": holding["id"],
            "stock_code": holding["stock_code"],
            "short_name": holding.get("short_name") or holding["stock_code"],
            "strategy_type": holding["strategy_type"],
            "trade_mode": "forward",
            "buy_price": buy_price,
            "sell_price": sell_price,
            "shares": shares,
            "profit_rate": round(profit_rate, 2),
            "profit": net_profit,
            "fee": total_fee,
            "holding_days": holding_days,
            "reason": reason,
            "reason_detail": reason_detail,
            "sell_date": sell_date,
            "sell_time": sell_time,
            "reason_label": self._reason_label(reason),
            "ai_score": float(holding.get("ai_score") or 0),
        }

    # ────────────────────────────────────────
    # 回测执行(使用K线历史数据)
    # ────────────────────────────────────────

    def backtest_buy(self, stock_code: str, strategy_type: str, trade_date: str,
                     analysis: dict = None, signal_date: str = "") -> dict | None:
        """
        回测模式买入：使用指定日期的开盘价买入。
        analysis: 可选的评分数据 dict
        """
        cfg = STRATEGY_CONFIG.get(strategy_type)
        if not cfg:
            return None

        # 检查持仓上限
        holding_count = self._get_holding_count(strategy_type, trade_mode="backtest")
        if holding_count >= cfg["max_holding"]:
            return None

        # 检查是否已持仓
        holding_codes = self._get_holding_codes(strategy_type, trade_mode="backtest")
        if stock_code in holding_codes:
            return None

        # 获取开盘价
        rows = _read_sql("""
            SELECT open, close, high, low, change_pct, pre_close FROM sm_stock_kline
            WHERE stock_code = :c AND k_type = 1 AND trade_date = :d
        """, {"c": stock_code, "d": trade_date})
        if not rows or not rows[0].get("open"):
            return None

        open_price = float(rows[0]["open"])
        if open_price <= 0:
            return None
        open_chg = _open_change_pct(rows[0])
        if open_chg is not None and open_chg >= 9.7:
            return None

        shares = _calc_shares(open_price, cfg["buy_amount"])
        if shares <= 0:
            return None

        analysis = analysis or {}
        buy_time = _random_intraday_time(stock_code + trade_date + "buy", "am")
        reason = analysis.get("reason", "")
        if signal_date:
            reason = f"信号日{signal_date}，次交易日开盘买入；{reason}"
        signal = {
            "stock_code": stock_code,
            "short_name": analysis.get("short_name", stock_code),
            "strategy_type": strategy_type,
            "trade_mode": "backtest",
            "buy_date": trade_date,
            "buy_time": buy_time,
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
            "reason": reason,
            "price_source": "backtest_open",
        }

        self.execute_buy(signal)

        return {"status": "ok", "stock_code": stock_code, "price": open_price,
                "shares": shares, "trade_date": trade_date, "signal_date": signal_date}

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
              AND COALESCE(trade_mode, 'live') = 'backtest'
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
            low_price = float(rows[0].get("low") or 0)
            low_rate = ((low_price - buy_price) / buy_price) * 100 if low_price > 0 else None

            buy_date = h["buy_date"]
            if isinstance(buy_date, str):
                buy_d = datetime.strptime(buy_date[:10], "%Y-%m-%d").date()
            else:
                buy_d = buy_date
            td = datetime.strptime(trade_date[:10], "%Y-%m-%d").date()
            holding_days = (td - buy_d).days

            # T+1: 当天买入不能当天卖出
            if holding_days < 1:
                continue

            reason = ""
            reason_detail = ""
            should_sell = False

            sell_time = _random_intraday_time(code + trade_date + "sell", "any")

            # 日K无法知道高低点先后；同日同时触发止盈/止损时按保守口径先止损。
            if low_rate is not None and low_rate <= cfg["stop_loss"]:
                reason = "stop_loss"
                detail_suffix = ""
                if high_rate >= cfg["take_profit"]:
                    detail_suffix = "；同日也触及止盈线，因日K无法判断先后，按保守口径先止损"
                reason_detail = (
                    f"亏损{low_rate:.2f}%已触及止损线{cfg['stop_loss']}%, "
                    f"买入价{buy_price:.2f}→盘中低点{low_price:.2f}, "
                    f"持仓{holding_days}天, 及时止损{detail_suffix}"
                )
                should_sell = True
                sell_price = round(buy_price * (1 + cfg["stop_loss"] / 100), 2)
                sell_time = _random_intraday_time(code + trade_date + "sl", "am")
            # 止盈: 当日最高价达到止盈线(假设盘中触发)
            elif high_rate >= cfg["take_profit"]:
                reason = "take_profit"
                reason_detail = f"盈利{high_rate:.2f}%已达止盈线{cfg['take_profit']}%, 买入价{buy_price:.2f}→盘中高点{high_price:.2f}, 持仓{holding_days}天"
                should_sell = True
                sell_price = round(buy_price * (1 + cfg["take_profit"] / 100), 2)
                sell_time = _random_intraday_time(code + trade_date + "tp", "am")
            elif holding_days >= cfg["max_days"]:
                reason = "time_limit"
                reason_detail = f"持仓已达{holding_days}天, 超过{cfg['name']}策略最大持仓{cfg['max_days']}天, 收益率{profit_rate:.2f}%, 尾盘清仓"
                should_sell = True
                sell_price = close_price
                sell_time = _random_intraday_time(code + trade_date + "tl2", "pm")
            elif cfg["trailing_activate"] and high_rate >= cfg["trailing_activate"]:
                if (high_rate - profit_rate) >= cfg["trailing_drawdown"]:
                    reason = "trailing_stop"
                    reason_detail = f"最高盈利{high_rate:.2f}%回撤至{profit_rate:.2f}%, 回撤{high_rate-profit_rate:.2f}%超阈值{cfg['trailing_drawdown']}%, 保护利润"
                    should_sell = True
                    sell_price = close_price
                    sell_time = _random_intraday_time(code + trade_date + "ts", "pm")

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
                    "trade_mode": "backtest",
                    "buy_price": buy_price,
                    "sell_price": sell_price,
                    "shares": shares,
                    "profit_rate": round(final_rate, 2),
                    "profit": net_profit,
                    "fee": total_fee,
                    "holding_days": holding_days,
                    "reason": reason,
                    "reason_detail": reason_detail,
                    "sell_date": trade_date,
                    "sell_time": sell_time,
                    "reason_label": self._reason_label(reason),
                    "ai_score": float(h.get("ai_score") or 0),
                })

        return signals
