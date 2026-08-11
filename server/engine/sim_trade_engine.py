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
from zoneinfo import ZoneInfo

from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.common.kline_data import get_kline_engine, should_use_kline_engine
from server.common.batch_db import quote_identifier
from server.common.sql_reader import read_sql_rows
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
        "max_holding": 0,          # 0=不限制持仓数量，仍受组合/单票资金风控约束
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
        "max_holding": 0,
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
        "max_holding": 0,
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
        "max_holding": 0,
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
STATUS_LABELS = {
    "ALLOW": "可跟踪",
    "BLOCK": "回避",
    "WATCH": "观察",
    "CONFIRM": "确认",
    "BUY_READY": "买入就绪",
    "SELL_ALERT": "卖出提醒",
    "SUSPENDED": "暂停",
    "LOW": "低",
    "MEDIUM": "中",
    "HIGH": "高",
    "CRITICAL": "极高",
    "REDUCE": "减仓",
    "NONE": "无信号",
    "DATA_BLOCKED": "数据阻断",
    "EXECUTION_BLOCKED": "执行阻断",
}
VALID_TRADE_MODES = {"live", "backtest", "forward"}
SNAPSHOT_FALLBACK_MAX_AGE_MINUTES = 5
SIM_INITIAL_CAPITAL = 1_000_000.0
EXCLUDED_RECOMMEND_PREFIXES = ("688",)
MIN_EXECUTABLE_RISK_REWARD = 3.0
ACTIONABLE_SIGNAL_STATUSES = frozenset({"CONFIRM", "BUY_READY"})
REQUIRED_CHASE_GATE_COLUMNS = frozenset(
    {"recommend_status", "signal_status", "chase_risk_status", "ordinary_buy_eligible"}
)
SIM_MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")

# Matching is allowed to mutate the existing simulator ledger only when every
# column used by the transactional boundary is present.  This is deliberately
# explicit: silently falling back to a legacy shape would split one fill across
# multiple autocommit operations and make rollback/idempotency impossible.
SIM_EXECUTION_REQUIRED_COLUMNS = {
    "st_sim_order": frozenset({
        "id", "signal_id", "trade_mode", "order_date", "stock_code",
        "strategy_type", "side", "status", "requested_shares",
        "remaining_shares", "filled_shares", "filled_amount", "fee",
        "position_id", "source_event", "match_count", "last_match_reason",
        "cancel_reason", "execution_gate_status", "execution_gate_hash",
        "execution_gate_checked_at", "execution_gate_valid_until",
        "execution_gate_evidence",
    }),
    "st_sim_signal": frozenset({
        "id", "trade_mode", "trade_date", "stock_code", "strategy_type",
        "status", "pending_order_id", "filled_order_id",
        "filled_position_id", "execution_gate_status", "execution_gate_hash",
        "execution_gate_checked_at", "execution_gate_valid_until",
        "execution_gate_evidence",
    }),
    "st_sim_position": frozenset({
        "id", "signal_id", "entry_order_id", "exit_order_id", "stock_code",
        "strategy_type", "trade_mode", "buy_price", "buy_amount",
        "buy_shares", "buy_date", "status", "sell_price", "sell_date",
        "profit", "profit_rate", "fee_total",
    }),
    "st_trade_flow": frozenset({
        "id", "order_id", "stock_code", "strategy_type", "trade_mode",
        "trans_type", "price", "shares", "amount", "fee", "trans_date",
    }),
    "st_sim_event": frozenset({
        "id", "trade_mode", "event_date", "event_type", "signal_id",
        "order_id", "position_id", "stock_code", "strategy_type", "payload",
    }),
    "st_sim_risk_budget": frozenset({
        "id", "trade_mode", "budget_date", "strategy_type", "initial_capital",
        "total_equity", "cash_available", "risk_budget_note", "updated_at",
    }),
}

SIM_RISK_CONFIG = {
    "max_total_position_pct": 0.80,
    "cash_buffer_pct": 0.20,
    "max_single_stock_pct": 0.10,
    "per_trade_risk_pct": 0.012,
    "min_order_amount": 8_000,
    "slippage_buy_pct": 0.05,
    "slippage_sell_pct": 0.05,
    "liquidity_volume_pct": 0.02,
    "strategy_budget_pct": {
        "ultra_short": 0.25,
        "short_term": 0.30,
        "swing": 0.25,
        "main_wave": 0.35,
    },
    "risk_multiplier": {
        "LOW": 1.00,
        "MEDIUM": 0.70,
        "HIGH": 0.35,
        "CRITICAL": 0.0,
    },
}


def _safe_float(v, default=0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def _status_label(value: str) -> str:
    key = str(value or "").strip().upper()
    return STATUS_LABELS.get(key, key or "-")


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


def _is_explicit_database_true(value) -> bool:
    """Accept only an actual boolean or MySQL TINYINT(1), never truthy text/float."""
    return value is True or (type(value) is int and value == 1)


def _market_aware_datetime(value=None) -> datetime:
    """Normalize a clock value for execution gates; naive values are China local."""
    if value is None:
        return datetime.now(SIM_MARKET_TIMEZONE)
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, datetime.min.time())
    if not isinstance(value, datetime):
        value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if value.tzinfo is None:
        return value.replace(tzinfo=SIM_MARKET_TIMEZONE)
    return value.astimezone(SIM_MARKET_TIMEZONE)


def _parse_gate_datetime(value) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return _market_aware_datetime(value)
    except (TypeError, ValueError):
        return None


def _blocked_execution_gate(reason: str, *, stock_code: str = "", trade_date: str = "") -> dict:
    now = _market_aware_datetime()
    return {
        "status": "DATA_BLOCKED",
        "eligible": False,
        "ordinary_buy_eligible": False,
        "reason": reason,
        "evidence": {},
        "context_hash": "",
        "evaluated_at": now.isoformat(),
        "valid_until": now.isoformat(),
        "stock_code": str(stock_code or "").zfill(6),
        "trade_date": str(trade_date or "")[:10],
    }


def evaluate_sim_buy_execution_gate(
    stock_code: str,
    trade_date: str,
    *,
    knowledge_cutoff=None,
) -> dict:
    """Read-only, fail-closed adapter around the exact-cutoff legacy gate."""
    cutoff = _market_aware_datetime(knowledge_cutoff)
    try:
        from biz.analysis.sync_analysis_fast import evaluate_stock_buy_gate_at_cutoff

        raw = evaluate_stock_buy_gate_at_cutoff(
            get_engine(),
            str(stock_code or "").zfill(6),
            str(trade_date or cutoff.date().isoformat())[:10],
            cutoff,
        )
    except Exception as exc:
        logger.warning("sim buy execution gate failed closed for %s: %s", stock_code, exc)
        return _blocked_execution_gate(
            f"execution-time gate unavailable: {exc}",
            stock_code=stock_code,
            trade_date=trade_date,
        )

    gate = dict(raw or {})
    status = str(gate.get("status") or "DATA_BLOCKED").upper()
    eligible = gate.get("eligible") is True
    ordinary_eligible = gate.get("ordinary_buy_eligible") is True
    context_hash = str(gate.get("context_hash") or "").strip()
    evaluated_at = _parse_gate_datetime(gate.get("evaluated_at"))
    valid_until = _parse_gate_datetime(gate.get("valid_until"))
    identity_ok = (
        str(gate.get("stock_code") or "").zfill(6) == str(stock_code or "").zfill(6)
        and str(gate.get("trade_date") or "")[:10] == str(trade_date or "")[:10]
    )
    time_ok = bool(
        evaluated_at
        and valid_until
        and evaluated_at <= cutoff <= valid_until
    )
    allowed = bool(
        status == "ALLOW"
        and eligible
        and ordinary_eligible
        and context_hash
        and identity_ok
        and time_ok
    )
    if not allowed:
        status = "DATA_BLOCKED" if status == "ALLOW" else status
    gate.update({
        "status": status,
        "eligible": allowed,
        "ordinary_buy_eligible": allowed,
        "context_hash": context_hash,
        "evaluated_at": evaluated_at.isoformat() if evaluated_at else "",
        "valid_until": valid_until.isoformat() if valid_until else "",
        "reason": str(gate.get("reason") or "execution-time gate did not prove eligibility"),
    })
    return gate


def evaluate_sim_holding_exit_gate(
    stock_code: str,
    trade_date: str,
    *,
    knowledge_cutoff=None,
) -> dict:
    """Evaluate current holding risk without requiring the stock in a candidate pool."""
    cutoff = _market_aware_datetime(knowledge_cutoff)
    base = {
        "exit_intent": "WAIT_DATA",
        "reason": "holding exit evidence is unavailable",
        "evidence": {},
        "context_hash": "",
        "evaluated_at": cutoff.isoformat(),
        "valid_until": cutoff.isoformat(),
        "stock_code": str(stock_code or "").zfill(6),
        "trade_date": str(trade_date or cutoff.date().isoformat())[:10],
    }
    try:
        from biz.analysis.sync_analysis_fast import evaluate_stock_holding_exit_at_cutoff

        raw = evaluate_stock_holding_exit_at_cutoff(
            get_engine(),
            base["stock_code"],
            base["trade_date"],
            cutoff,
        )
    except Exception as exc:
        logger.warning("sim holding exit gate unavailable for %s: %s", stock_code, exc)
        return {**base, "reason": f"holding exit gate unavailable: {exc}"}

    result = {**base, **dict(raw or {})}
    intent = str(result.get("exit_intent") or "WAIT_DATA").upper()
    evaluated_at = _parse_gate_datetime(result.get("evaluated_at"))
    valid_until = _parse_gate_datetime(result.get("valid_until"))
    identity_ok = (
        str(result.get("stock_code") or "").zfill(6) == base["stock_code"]
        and str(result.get("trade_date") or "")[:10] == base["trade_date"]
    )
    if (
        intent not in {"HOLD", "REDUCE", "SELL", "WAIT_DATA"}
        or not evaluated_at
        or not valid_until
        or not (evaluated_at <= cutoff <= valid_until)
        or not identity_ok
        or not str(result.get("context_hash") or "").strip()
    ):
        intent = "WAIT_DATA"
        result["reason"] = "holding exit gate returned invalid/stale evidence"
    result.update({
        "exit_intent": intent,
        "evaluated_at": evaluated_at.isoformat() if evaluated_at else "",
        "valid_until": valid_until.isoformat() if valid_until else "",
    })
    return result


def _execution_gate_allows_buy(gate: dict | None, *, now=None) -> bool:
    gate = gate or {}
    check_at = _market_aware_datetime(now)
    valid_until = _parse_gate_datetime(gate.get("valid_until"))
    evaluated_at = _parse_gate_datetime(gate.get("evaluated_at"))
    return bool(
        str(gate.get("status") or "").upper() == "ALLOW"
        and gate.get("eligible") is True
        and gate.get("ordinary_buy_eligible") is True
        and str(gate.get("context_hash") or "").strip()
        and evaluated_at
        and valid_until
        and evaluated_at <= check_at <= valid_until
    )


def _execution_gate_allows_expected_buy(
    gate: dict | None,
    stock_code: str,
    trade_date: str,
    *,
    now=None,
) -> bool:
    """Require freshness and exact instrument/session identity."""
    gate = gate or {}
    expected_code = str(stock_code or "").zfill(6)
    expected_date = str(trade_date or "")[:10]
    return bool(
        _execution_gate_allows_buy(gate, now=now)
        and str(gate.get("stock_code") or "").zfill(6) == expected_code
        and str(gate.get("trade_date") or "")[:10] == expected_date
    )


def _gate_sql_datetime(value) -> str | None:
    parsed = _parse_gate_datetime(value)
    return parsed.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S") if parsed else None


def _gate_evidence_json(gate: dict | None) -> str:
    payload = (gate or {}).get("evidence")
    if not isinstance(payload, (dict, list)):
        payload = {"raw": str(payload or "")}
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


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

    code = str(candidate.get("stock_code") or "").strip().zfill(6)
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
    risk_reward_ratio = _score_value(candidate, "risk_reward_ratio", default=0)
    if risk_reward_ratio <= 0 and expected_return_pct > 0:
        downside_pct = abs(_safe_float(cfg.get("stop_loss"), 0.0)) or 1.0
        risk_reward_ratio = round(expected_return_pct / downside_pct, 2)
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
    risk_level = str(candidate.get("event_risk_level") or "DATA_BLOCKED").upper()
    recommend_status = str(candidate.get("recommend_status") or "DATA_BLOCKED").upper()
    signal_status = str(candidate.get("signal_status") or "WATCH").upper()
    chase_risk_status = str(
        candidate.get("chase_risk_status") or "DATA_BLOCKED"
    ).upper()
    ordinary_buy_eligible = _is_explicit_database_true(
        candidate.get("ordinary_buy_eligible")
    )
    sector_gate_status = str(candidate.get("sector_gate_status") or "WATCH").upper()

    analysis = {
        "ai_score": ai_score,
        "quality_score": quality_score,
        "entry_score": entry_score,
        "final_trade_score": final_trade_score,
        "expected_return_pct": expected_return_pct,
        "risk_reward_ratio": risk_reward_ratio,
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
        "chase_risk_status": chase_risk_status,
        "ordinary_buy_eligible": ordinary_buy_eligible,
        "sector_gate_status": sector_gate_status,
        "sector_gate_reason": candidate.get("sector_gate_reason") or "",
        "short_name": candidate.get("short_name") or candidate.get("stock_name") or "",
        "orig_reason": candidate.get("reason") or candidate.get("summary") or "",
        "sources": candidate.get("sources") or "",
    }

    blockers = []
    reason_parts = []

    if not code.startswith(("0", "3", "6")):
        blockers.append("非沪深A股主代码按策略要求过滤")
    if code.startswith(EXCLUDED_RECOMMEND_PREFIXES):
        blockers.append("688开头科创板标的按策略要求过滤")
    if sector_gate_status == "BLOCK":
        blockers.append("板块资金或延续性不合格，板块先行规则未通过")

    if recommend_status != "ALLOW":
        blockers.append(f"推荐资格为{_status_label(recommend_status)}，未达到可跟踪")
    else:
        reason_parts.append("推荐资格可跟踪")

    if chase_risk_status != "ALLOW" or not ordinary_buy_eligible:
        blockers.append(
            "追高与成交能力硬门未显式通过，禁止新增买入"
        )
    else:
        reason_parts.append("追高与成交能力硬门通过")

    if signal_status not in ACTIONABLE_SIGNAL_STATUSES:
        blockers.append(f"AI信号状态为{_status_label(signal_status)}，仍需等待买点确认")
    elif signal_status:
        reason_parts.append(f"AI信号状态{_status_label(signal_status)}")

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
    if strategy_type != "main_wave" and risk_reward_ratio and risk_reward_ratio < MIN_EXECUTABLE_RISK_REWARD:
        blockers.append(f"盈亏比{risk_reward_ratio:.2f}:1低于{MIN_EXECUTABLE_RISK_REWARD:.0f}:1执行底线")
    if heat_overload_score < 50:
        blockers.append(f"热度拥挤度健康分{heat_overload_score:.0f}过低")
    if confidence_score < 45:
        blockers.append(f"推荐一致性{confidence_score:.0f}分过低")

    if not _check_risk_level(risk_level, cfg["max_risk_level"]):
        blockers.append(f"风险等级{_status_label(risk_level)}超过允许上限{_status_label(cfg['max_risk_level'])}")
    else:
        reason_parts.append(f"风险等级{_status_label(risk_level)}(允许{_status_label(cfg['max_risk_level'])})")

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
            blockers.append(f"主升浪信号为{_status_label(main_wave_signal)}，不是买点")
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
    engine = get_kline_engine() if should_use_kline_engine(sql) else get_engine()
    return read_sql_rows(engine, sql, params, context="sim_trade_engine")


def _exec_sql(sql: str, params: dict = None):
    e = get_engine()
    with e.begin() as c:
        c.execute(text(sql), params)


def _exec_insert_get_id(sql: str, params: dict = None) -> int:
    e = get_engine()
    with e.begin() as c:
        c.execute(text(sql), params or {})
        return int(c.execute(text("SELECT LAST_INSERT_ID()")).scalar() or 0)


def _connection_rows(connection, sql: str, params: dict = None) -> list[dict]:
    """Read rows through an already-open transaction connection."""
    result = connection.execute(text(sql), params or {})
    rows = []
    for row in result:
        mapping = getattr(row, "_mapping", row)
        rows.append(dict(mapping))
    return rows


def _connection_write(connection, sql: str, params: dict = None):
    """Write through ``connection`` or use the legacy one-shot helper."""
    if connection is None:
        return _exec_sql(sql, params)
    return connection.execute(text(sql), params or {})


def _connection_insert_get_id(connection, sql: str, params: dict = None) -> int:
    """Insert and resolve LAST_INSERT_ID without leaving the transaction."""
    if connection is None:
        return _exec_insert_get_id(sql, params)
    connection.execute(text(sql), params or {})
    return int(connection.execute(text("SELECT LAST_INSERT_ID()")).scalar() or 0)


def _normalize_trade_date(value, *, field_name: str = "trade_date") -> str:
    normalized = str(value or "")[:10]
    try:
        datetime.strptime(normalized, "%Y-%m-%d")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {field_name}: {value!r}") from exc
    return normalized


def _require_sim_execution_schema(connection=None) -> None:
    """Fail closed unless the existing simulator ledger is transactional-ready."""
    if connection is None:
        columns_by_table = {
            table_name: _table_columns(table_name)
            for table_name in SIM_EXECUTION_REQUIRED_COLUMNS
        }
        engine_rows = _read_sql("""
            SELECT TABLE_NAME, ENGINE
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME IN (
                'st_sim_order', 'st_sim_signal', 'st_sim_position',
                'st_trade_flow', 'st_sim_event', 'st_sim_risk_budget'
              )
        """)
    else:
        column_rows = _connection_rows(connection, """
            SELECT TABLE_NAME, COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME IN (
                'st_sim_order', 'st_sim_signal', 'st_sim_position',
                'st_trade_flow', 'st_sim_event', 'st_sim_risk_budget'
              )
        """)
        columns_by_table = {name: set() for name in SIM_EXECUTION_REQUIRED_COLUMNS}
        for row in column_rows:
            columns_by_table.setdefault(str(row.get("TABLE_NAME") or ""), set()).add(
                str(row.get("COLUMN_NAME") or "")
            )
        engine_rows = _connection_rows(connection, """
            SELECT TABLE_NAME, ENGINE
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME IN (
                'st_sim_order', 'st_sim_signal', 'st_sim_position',
                'st_trade_flow', 'st_sim_event', 'st_sim_risk_budget'
              )
        """)

    missing = {
        table_name: sorted(required - set(columns_by_table.get(table_name) or set()))
        for table_name, required in SIM_EXECUTION_REQUIRED_COLUMNS.items()
        if required - set(columns_by_table.get(table_name) or set())
    }
    if missing:
        detail = "; ".join(
            f"{table_name}: {','.join(columns)}"
            for table_name, columns in sorted(missing.items())
        )
        raise RuntimeError(f"sim execution schema is incomplete; matching disabled: {detail}")

    storage_engines = {
        str(row.get("TABLE_NAME") or ""): str(row.get("ENGINE") or "").upper()
        for row in engine_rows
    }
    non_transactional = sorted(
        table_name
        for table_name in SIM_EXECUTION_REQUIRED_COLUMNS
        if storage_engines.get(table_name) != "INNODB"
    )
    if non_transactional:
        raise RuntimeError(
            "sim execution tables must use InnoDB; matching disabled: "
            + ",".join(non_transactional)
        )


def _table_columns(table_name: str) -> set[str]:
    rows = _read_sql("""
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = :table_name
    """, {"table_name": table_name})
    return {str(r.get("COLUMN_NAME")) for r in rows}


def _ensure_column(table_name: str, column_name: str, ddl: str) -> None:
    columns = _table_columns(table_name)
    if column_name not in columns:
        _exec_sql(f"ALTER TABLE {quote_identifier(table_name)} ADD COLUMN {quote_identifier(column_name)} {ddl}")


def _ensure_index(table_name: str, index_name: str, ddl: str) -> None:
    rows = _read_sql("""
        SELECT INDEX_NAME
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = :table_name
          AND INDEX_NAME = :index_name
        LIMIT 1
    """, {"table_name": table_name, "index_name": index_name})
    if not rows:
        _exec_sql(f"ALTER TABLE {quote_identifier(table_name)} ADD {ddl}")


def _ensure_text_column(table_name: str, column_name: str) -> None:
    rows = _read_sql("""
        SELECT DATA_TYPE
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = :table_name
          AND COLUMN_NAME = :column_name
        LIMIT 1
    """, {"table_name": table_name, "column_name": column_name})
    if not rows:
        raise RuntimeError(f"missing migration column: {table_name}.{column_name}")
    data_type = str(rows[0].get("DATA_TYPE") or "").lower()
    if data_type != "text":
        _exec_sql(
            f"ALTER TABLE {quote_identifier(table_name)} "
            f"MODIFY COLUMN {quote_identifier(column_name)} TEXT"
        )


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

    missing_gate_columns = REQUIRED_CHASE_GATE_COLUMNS - columns
    if missing_gate_columns:
        logger.warning(
            "st_recommended_stocks missing mandatory new-buy gate columns: %s",
            ",".join(sorted(missing_gate_columns)),
        )
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
    status_filter = """
        AND recommend_status = 'ALLOW'
        AND signal_status IN ('CONFIRM', 'BUY_READY')
        AND chase_risk_status = 'ALLOW'
        AND ordinary_buy_eligible = 1
    """

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
            {_select_expr(columns, "recommend_status", "'DATA_BLOCKED'")},
            {_select_expr(columns, "recommend_reason", "''")},
            {_select_expr(columns, "event_risk_level", "'DATA_BLOCKED'")},
            {_select_expr(columns, "sentiment_score", "0")},
            {_select_expr(columns, "market_mood_score", "0")},
            {_select_expr(columns, "event_score", "0")},
            {_select_expr(columns, "signal_status", "'WATCH'")},
            {_select_expr(columns, "chase_risk_status", "'DATA_BLOCKED'")},
            {_select_expr(columns, "ordinary_buy_eligible", "0")},
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
            {_select_expr(columns, "risk_reward_ratio", "0")},
            {_select_expr(columns, "sector_gate_status", "'WATCH'")},
            {_select_expr(columns, "sector_gate_reason", "''")},
            {_select_expr(columns, "evidence_chain_json", "'[]'")},
            {_select_expr(columns, "failure_tags_json", "'[]'")},
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


def recommended_candidate_summary(pick_date: str) -> dict:
    columns = _table_columns("st_recommended_stocks")
    if not columns:
        return {"total": 0, "allow_count": 0, "status_breakdown": []}

    recommend_expr = "COALESCE(recommend_status, 'DATA_BLOCKED')" if "recommend_status" in columns else "'DATA_BLOCKED'"
    signal_expr = "COALESCE(signal_status, 'WATCH')" if "signal_status" in columns else "'WATCH'"
    chase_expr = "COALESCE(chase_risk_status, 'DATA_BLOCKED')" if "chase_risk_status" in columns else "'DATA_BLOCKED'"
    eligible_expr = "COALESCE(ordinary_buy_eligible, 0)" if "ordinary_buy_eligible" in columns else "0"
    rows = _read_sql(f"""
        SELECT
            {recommend_expr} AS recommend_status,
            {signal_expr} AS signal_status,
            {chase_expr} AS chase_risk_status,
            {eligible_expr} AS ordinary_buy_eligible,
            COUNT(*) AS cnt
        FROM st_recommended_stocks
        WHERE pick_date = :d
        GROUP BY recommend_status, signal_status, chase_risk_status, ordinary_buy_eligible
        ORDER BY cnt DESC
    """, {"d": pick_date})

    total = 0
    allow_count = 0
    breakdown = []
    for row in rows:
        recommend_status = str(row.get("recommend_status") or "").upper()
        signal_status = str(row.get("signal_status") or "").upper()
        chase_risk_status = str(row.get("chase_risk_status") or "").upper()
        ordinary_buy_eligible = _is_explicit_database_true(
            row.get("ordinary_buy_eligible")
        )
        count = int(row.get("cnt") or 0)
        total += count
        if (
            recommend_status == "ALLOW"
            and signal_status in ACTIONABLE_SIGNAL_STATUSES
            and chase_risk_status == "ALLOW"
            and ordinary_buy_eligible
        ):
            allow_count += count
        breakdown.append({
            "recommend_status": recommend_status,
            "signal_status": signal_status,
            "chase_risk_status": chase_risk_status,
            "ordinary_buy_eligible": ordinary_buy_eligible,
            "count": count,
        })

    return {
        "total": total,
        "allow_count": allow_count,
        "status_breakdown": breakdown,
    }


def migrate_sim_trade_schema(*, allow_schema_change: bool = False) -> None:
    """Create or upgrade simulator tables through an explicit operator action."""
    if not allow_schema_change:
        raise PermissionError(
            "simulator schema migration requires allow_schema_change=True"
        )
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
                `sell_reason`     TEXT,
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
        _ensure_column(
            "st_sim_position",
            "trade_mode",
            "VARCHAR(20) DEFAULT 'live' AFTER `strategy_type`",
        )
        _ensure_index(
            "st_sim_position",
            "idx_trade_mode",
            "INDEX `idx_trade_mode` (`trade_mode`, `strategy_type`, `status`)",
        )
        _ensure_text_column("st_sim_position", "sell_reason")
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
                `reason`          TEXT,
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
        _ensure_column(
            "st_trade_flow",
            "trade_mode",
            "VARCHAR(20) DEFAULT 'live' AFTER `strategy_type`",
        )
        _ensure_index(
            "st_trade_flow",
            "idx_trade_mode",
            "INDEX `idx_trade_mode` (`trade_mode`, `strategy_type`, `trans_date`)",
        )
        _ensure_text_column("st_trade_flow", "reason")
        _exec_sql("""
            CREATE TABLE IF NOT EXISTS `st_sim_signal` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `trade_mode` VARCHAR(20) DEFAULT 'live',
                `signal_date` DATE NOT NULL,
                `trade_date` DATE NOT NULL,
                `stock_code` VARCHAR(10) NOT NULL,
                `short_name` VARCHAR(20) DEFAULT '',
                `strategy_type` VARCHAR(20) NOT NULL,
                `status` VARCHAR(20) DEFAULT 'NEW',
                `reason` TEXT,
                `last_check_reason` TEXT,
                `ai_score` DECIMAL(5,2) DEFAULT 0,
                `quality_score` DECIMAL(5,2) DEFAULT 0,
                `entry_score` DECIMAL(5,2) DEFAULT 0,
                `final_trade_score` DECIMAL(5,2) DEFAULT 0,
                `expected_return_pct` DECIMAL(8,4) DEFAULT 0,
                `risk_reward_ratio` DECIMAL(8,4) DEFAULT 0,
                `short_score` DECIMAL(5,2) DEFAULT 0,
                `long_score` DECIMAL(5,2) DEFAULT 0,
                `capital_score` DECIMAL(5,2) DEFAULT 0,
                `technical_score` DECIMAL(5,2) DEFAULT 0,
                `fundamental_score` DECIMAL(5,2) DEFAULT 0,
                `main_wave_score` DECIMAL(5,2) DEFAULT 0,
                `trend_hold_score` DECIMAL(5,2) DEFAULT 0,
                `event_risk_level` VARCHAR(10) DEFAULT 'LOW',
                `entry_price_low` DECIMAL(12,4) DEFAULT NULL,
                `entry_price_high` DECIMAL(12,4) DEFAULT NULL,
                `stop_loss_price` DECIMAL(12,4) DEFAULT NULL,
                `take_profit_1` DECIMAL(12,4) DEFAULT NULL,
                `take_profit_2` DECIMAL(12,4) DEFAULT NULL,
                `filled_order_id` BIGINT DEFAULT NULL,
                `filled_position_id` BIGINT DEFAULT NULL,
                `execution_gate_status` VARCHAR(30) DEFAULT 'DATA_BLOCKED',
                `execution_gate_hash` VARCHAR(128) DEFAULT '',
                `execution_gate_checked_at` DATETIME DEFAULT NULL,
                `execution_gate_valid_until` DATETIME DEFAULT NULL,
                `execution_gate_evidence` TEXT,
                `last_check_at` DATETIME DEFAULT NULL,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                `updated_at` DATETIME DEFAULT NULL,
                UNIQUE KEY `uk_sim_signal` (`trade_mode`, `signal_date`, `trade_date`, `stock_code`, `strategy_type`),
                INDEX `idx_sim_signal_status` (`trade_mode`, `trade_date`, `status`),
                INDEX `idx_sim_signal_stock` (`stock_code`, `trade_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        _ensure_column("st_sim_signal", "risk_reward_ratio", "DECIMAL(8,4) DEFAULT 0 AFTER `expected_return_pct`")
        _exec_sql("""
            CREATE TABLE IF NOT EXISTS `st_sim_order` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `signal_id` BIGINT DEFAULT NULL,
                `trade_mode` VARCHAR(20) DEFAULT 'live',
                `order_date` DATE NOT NULL,
                `order_time` VARCHAR(20) DEFAULT '',
                `stock_code` VARCHAR(10) NOT NULL,
                `short_name` VARCHAR(20) DEFAULT '',
                `strategy_type` VARCHAR(20) NOT NULL,
                `side` VARCHAR(10) NOT NULL,
                `order_type` VARCHAR(20) DEFAULT 'SIM_LIMIT',
                `limit_price` DECIMAL(12,4) DEFAULT NULL,
                `target_price` DECIMAL(12,4) DEFAULT NULL,
                `requested_shares` INT DEFAULT 0,
                `remaining_shares` INT DEFAULT 0,
                `status` VARCHAR(20) DEFAULT 'PENDING',
                `filled_price` DECIMAL(12,4) DEFAULT NULL,
                `filled_shares` INT DEFAULT 0,
                `filled_amount` DECIMAL(14,2) DEFAULT 0,
                `fee` DECIMAL(10,2) DEFAULT 0,
                `position_id` BIGINT DEFAULT NULL,
                `source_event` VARCHAR(40) DEFAULT '',
                `price_source` VARCHAR(40) DEFAULT '',
                `risk_budget_amount` DECIMAL(14,2) DEFAULT 0,
                `risk_budget_note` TEXT,
                `match_count` INT DEFAULT 0,
                `reason` TEXT,
                `reject_reason` TEXT,
                `execution_gate_status` VARCHAR(30) DEFAULT 'DATA_BLOCKED',
                `execution_gate_hash` VARCHAR(128) DEFAULT '',
                `execution_gate_checked_at` DATETIME DEFAULT NULL,
                `execution_gate_valid_until` DATETIME DEFAULT NULL,
                `execution_gate_evidence` TEXT,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                `updated_at` DATETIME DEFAULT NULL,
                `filled_at` DATETIME DEFAULT NULL,
                INDEX `idx_sim_order_signal` (`signal_id`),
                INDEX `idx_sim_order_status` (`trade_mode`, `order_date`, `status`),
                INDEX `idx_sim_order_stock` (`stock_code`, `order_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        for column, ddl in {
            "requested_shares": "INT DEFAULT 0 AFTER `target_price`",
            "remaining_shares": "INT DEFAULT 0 AFTER `requested_shares`",
            "source_event": "VARCHAR(40) DEFAULT '' AFTER `position_id`",
            "price_source": "VARCHAR(40) DEFAULT '' AFTER `source_event`",
            "risk_budget_amount": "DECIMAL(14,2) DEFAULT 0 AFTER `price_source`",
            "risk_budget_note": "TEXT AFTER `risk_budget_amount`",
            "match_count": "INT DEFAULT 0 AFTER `risk_budget_note`",
            "last_match_reason": "TEXT AFTER `reject_reason`",
            "cancel_reason": "TEXT AFTER `last_match_reason`",
            "execution_gate_status": "VARCHAR(30) DEFAULT 'DATA_BLOCKED' AFTER `cancel_reason`",
            "execution_gate_hash": "VARCHAR(128) DEFAULT '' AFTER `execution_gate_status`",
            "execution_gate_checked_at": "DATETIME DEFAULT NULL AFTER `execution_gate_hash`",
            "execution_gate_valid_until": "DATETIME DEFAULT NULL AFTER `execution_gate_checked_at`",
            "execution_gate_evidence": "TEXT AFTER `execution_gate_valid_until`",
        }.items():
            _ensure_column("st_sim_order", column, ddl)
        for column, ddl in {
            "pending_order_id": "BIGINT DEFAULT NULL AFTER `filled_position_id`",
            "intended_amount": "DECIMAL(14,2) DEFAULT 0 AFTER `take_profit_2`",
            "intended_shares": "INT DEFAULT 0 AFTER `intended_amount`",
            "risk_budget_amount": "DECIMAL(14,2) DEFAULT 0 AFTER `intended_shares`",
            "risk_budget_note": "TEXT AFTER `risk_budget_amount`",
            "execution_gate_status": "VARCHAR(30) DEFAULT 'DATA_BLOCKED' AFTER `risk_budget_note`",
            "execution_gate_hash": "VARCHAR(128) DEFAULT '' AFTER `execution_gate_status`",
            "execution_gate_checked_at": "DATETIME DEFAULT NULL AFTER `execution_gate_hash`",
            "execution_gate_valid_until": "DATETIME DEFAULT NULL AFTER `execution_gate_checked_at`",
            "execution_gate_evidence": "TEXT AFTER `execution_gate_valid_until`",
        }.items():
            _ensure_column("st_sim_signal", column, ddl)
        for column, ddl in {
            "signal_id": "BIGINT DEFAULT NULL AFTER `id`",
            "entry_order_id": "BIGINT DEFAULT NULL AFTER `signal_id`",
            "exit_order_id": "BIGINT DEFAULT NULL AFTER `entry_order_id`",
        }.items():
            _ensure_column("st_sim_position", column, ddl)
        _ensure_column("st_trade_flow", "order_id", "BIGINT DEFAULT NULL AFTER `id`")
        _ensure_index("st_sim_position", "idx_sim_position_signal", "INDEX `idx_sim_position_signal` (`signal_id`)")
        _ensure_index("st_trade_flow", "idx_trade_flow_order", "INDEX `idx_trade_flow_order` (`order_id`)")
        _exec_sql("""
            CREATE TABLE IF NOT EXISTS `st_sim_event` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `trade_mode` VARCHAR(20) DEFAULT 'live',
                `event_date` DATE NOT NULL,
                `event_time` VARCHAR(20) DEFAULT '',
                `event_type` VARCHAR(40) NOT NULL,
                `signal_id` BIGINT DEFAULT NULL,
                `order_id` BIGINT DEFAULT NULL,
                `position_id` BIGINT DEFAULT NULL,
                `stock_code` VARCHAR(10) DEFAULT '',
                `short_name` VARCHAR(20) DEFAULT '',
                `strategy_type` VARCHAR(20) DEFAULT '',
                `severity` VARCHAR(20) DEFAULT 'INFO',
                `message` TEXT,
                `payload` LONGTEXT,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX `idx_sim_event_date` (`trade_mode`, `event_date`, `event_type`),
                INDEX `idx_sim_event_stock` (`stock_code`, `event_date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        _ensure_column("st_sim_event", "short_name", "VARCHAR(20) DEFAULT '' AFTER `stock_code`")
        _exec_sql("""
            CREATE TABLE IF NOT EXISTS `st_sim_risk_budget` (
                `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                `trade_mode` VARCHAR(20) DEFAULT 'live',
                `budget_date` DATE NOT NULL,
                `strategy_type` VARCHAR(20) NOT NULL,
                `initial_capital` DECIMAL(14,2) DEFAULT 0,
                `total_equity` DECIMAL(14,2) DEFAULT 0,
                `cash_available` DECIMAL(14,2) DEFAULT 0,
                `max_total_position_amount` DECIMAL(14,2) DEFAULT 0,
                `max_strategy_amount` DECIMAL(14,2) DEFAULT 0,
                `used_strategy_amount` DECIMAL(14,2) DEFAULT 0,
                `pending_strategy_amount` DECIMAL(14,2) DEFAULT 0,
                `available_strategy_amount` DECIMAL(14,2) DEFAULT 0,
                `risk_budget_note` TEXT,
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                `updated_at` DATETIME DEFAULT NULL,
                UNIQUE KEY `uk_sim_risk_budget` (`trade_mode`, `budget_date`, `strategy_type`),
                INDEX `idx_sim_risk_budget_date` (`trade_mode`, `budget_date`)
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
                `created_at`      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY `uk_date_strategy` (`snapshot_date`, `strategy_type`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        _require_sim_execution_schema()
    except Exception as e:
        logger.warning(f"确保模拟交易表存在失败: {e}")
        raise


def _ensure_tables() -> None:
    """Validate the runtime simulator schema without creating or altering it."""
    _require_sim_execution_schema()


def _is_trading_time(now=None) -> bool:
    """判断当前是否为A股交易时间(9:25-11:30 / 13:00-15:00)"""
    from datetime import datetime as _dt, time as _t
    now = now or _dt.now()
    if now.weekday() >= 5:
        return False
    t = now.time()
    return (_t(9, 25) <= t <= _t(11, 31)) or (_t(12, 59) <= t <= _t(15, 1))


def _intraday_action_window(now=None) -> dict:
    """Classify intraday context while allowing market-driven buys all session."""
    from datetime import datetime as _dt, time as _t
    configured_windows = {
        "entry_windows": ["09:37-09:45", "14:26-14:28"],
        "exit_windows": ["09:30-09:37", "13:15-13:30"],
        "t_windows": ["14:00-14:05", "14:57-15:00"],
    }
    now = now or _dt.now()
    if now.weekday() >= 5:
        return {
            "action": "closed",
            "label": "非交易日",
            "action_label": "非交易日",
            "is_trading_time": False,
            "is_entry_window": False,
            "is_preferred_entry_window": False,
            "is_buy_allowed": False,
            "buy_mode": "continuous_market",
            "is_exit_window": False,
            "is_t_window": False,
            **configured_windows,
        }
    t = now.time()
    trading = _is_trading_time(now)
    entry = (_t(9, 37) <= t <= _t(9, 45)) or (_t(14, 26) <= t <= _t(14, 28))
    exit_window = (_t(9, 30) <= t < _t(9, 37)) or (_t(13, 15) <= t <= _t(13, 30))
    t_window = (_t(14, 0) <= t <= _t(14, 5)) or (_t(14, 57) <= t <= _t(15, 0))
    if entry:
        action = "entry"
        label = "入场窗口"
    elif exit_window:
        action = "exit"
        label = "出场窗口"
    elif t_window:
        action = "t"
        label = "T窗口"
    elif trading:
        action = "observe"
        label = "观察窗口"
    else:
        action = "closed"
        label = "非交易时间"
    return {
        "action": action,
        "label": label,
        "is_trading_time": trading,
        "is_entry_window": entry,
        "is_preferred_entry_window": entry,
        "is_buy_allowed": trading,
        "buy_mode": "continuous_market",
        "is_exit_window": exit_window,
        "is_t_window": t_window,
        **configured_windows,
        "action_label": label,
    }


def _is_buy_execution_time(now=None) -> bool:
    """Allow new simulated positions whenever the A-share market is trading."""
    return bool(_intraday_action_window(now).get("is_buy_allowed"))


def _is_signal_expire_time(now=None) -> bool:
    from datetime import datetime as _dt, time as _t
    now = now or _dt.now()
    return now.time() > _t(15, 1)


def _holding_limit_reached(config: dict | None, holding_count: int) -> bool:
    """A non-positive max_holding means no count cap."""
    limit = int((config or {}).get("max_holding") or 0)
    return limit > 0 and int(holding_count or 0) >= limit


def build_sell_decision(
    strategy_type: str,
    buy_price,
    current_price,
    buy_date,
    *,
    as_of_date=None,
    max_profit_rate=None,
    near_limit_down: bool = False,
    holding_assessment: dict | None = None,
) -> dict:
    """Return the executable sell decision and a user-readable explanation."""
    cfg = STRATEGY_CONFIG.get(strategy_type)
    if not cfg:
        return {
            "should_sell": False,
            "action": "WAIT",
            "reason": "unknown_strategy",
            "reason_detail": f"暂不处理：未知策略 {strategy_type}",
        }

    buy_price = _safe_float(buy_price, 0.0)
    current_price = _safe_float(current_price, 0.0)
    if buy_price <= 0 or current_price <= 0:
        return {
            "should_sell": False,
            "action": "WAIT",
            "reason": "price_unavailable",
            "reason_detail": "暂不卖出：买入价或实时价不可用，等待有效行情。",
        }

    if isinstance(buy_date, datetime):
        buy_d = buy_date.date()
    elif isinstance(buy_date, date):
        buy_d = buy_date
    else:
        try:
            buy_d = datetime.strptime(str(buy_date)[:10], "%Y-%m-%d").date()
        except (TypeError, ValueError):
            buy_d = date.today()

    if isinstance(as_of_date, datetime):
        today = as_of_date.date()
    elif isinstance(as_of_date, date):
        today = as_of_date
    elif as_of_date:
        try:
            today = datetime.strptime(str(as_of_date)[:10], "%Y-%m-%d").date()
        except (TypeError, ValueError):
            today = date.today()
    else:
        today = date.today()

    holding_days = max(0, (today - buy_d).days)
    profit_rate = ((current_price - buy_price) / buy_price) * 100
    take_profit = _safe_float(cfg.get("take_profit"), 0.0)
    stop_loss = _safe_float(cfg.get("stop_loss"), 0.0)
    max_days = int(cfg.get("max_days") or 0)
    trailing_activate = cfg.get("trailing_activate")
    trailing_drawdown = cfg.get("trailing_drawdown")
    max_rate = None if max_profit_rate is None else _safe_float(max_profit_rate)

    base = {
        "should_sell": False,
        "action": "HOLD",
        "reason": "hold",
        "profit_rate": round(profit_rate, 2),
        "holding_days": holding_days,
        "take_profit_pct": take_profit,
        "stop_loss_pct": stop_loss,
        "max_holding_days": max_days,
        "distance_to_take_profit_pct": round(max(0.0, take_profit - profit_rate), 2),
        "distance_to_stop_loss_pct": round(max(0.0, profit_rate - stop_loss), 2),
        "remaining_days": max(0, max_days - holding_days),
        "trailing_activate_pct": trailing_activate,
        "trailing_drawdown_pct": trailing_drawdown,
        "max_profit_rate": None if max_rate is None else round(max_rate, 2),
        "holding_assessment": dict(holding_assessment or {}),
    }
    trigger: dict | None = None
    assessment = holding_assessment if isinstance(holding_assessment, dict) else {}
    exit_intent = str(assessment.get("exit_intent") or "HOLD").upper()
    if exit_intent in {"SELL", "REDUCE"}:
        reason = "dynamic_sell" if exit_intent == "SELL" else "dynamic_reduce"
        trigger = {
            "reason": reason,
            "requested_exit_intent": exit_intent,
            "reason_detail": (
                f"动态风险触发{('卖出' if exit_intent == 'SELL' else '减仓')}："
                f"{assessment.get('reason') or '最新个股/事件/趋势证据要求降低风险'}。"
            ),
        }
    elif profit_rate <= stop_loss:
        trigger = {
            "reason": "stop_loss",
            "reason_detail": (
                f"触发卖出：当前收益{profit_rate:.2f}%已触及止损线{stop_loss:.2f}%，"
                f"买入价{buy_price:.2f}→现价{current_price:.2f}，持有{holding_days}天。"
            ),
        }

    trailing_note = ""
    if trailing_activate is not None and trailing_drawdown is not None:
        activate = _safe_float(trailing_activate)
        drawdown_limit = _safe_float(trailing_drawdown)
        if max_rate is not None and max_rate >= activate:
            drawdown = max_rate - profit_rate
            if trigger is None and drawdown >= drawdown_limit:
                trigger = {
                    "reason": "trailing_stop",
                    "trailing_drawdown_now_pct": round(drawdown, 2),
                    "reason_detail": (
                        f"触发卖出：最高收益{max_rate:.2f}%已激活动态止盈，"
                        f"现回撤{drawdown:.2f}个百分点，达到{drawdown_limit:.2f}个百分点阈值。"
                    ),
                }
            trailing_note = (
                f"动态止盈已激活，最高收益{max_rate:.2f}%，当前回撤{drawdown:.2f}/"
                f"{drawdown_limit:.2f}个百分点"
            )
        elif max_rate is not None:
            trailing_note = f"最高收益{max_rate:.2f}%，尚未达到动态止盈激活线+{activate:.2f}%"
        else:
            trailing_note = f"动态止盈需最高收益达到+{activate:.2f}%后激活"

    if trigger is None and profit_rate >= take_profit:
        trigger = {
            "reason": "take_profit",
            "reason_detail": (
                f"触发卖出：当前收益{profit_rate:.2f}%已达到止盈线+{take_profit:.2f}%，"
                f"买入价{buy_price:.2f}→现价{current_price:.2f}，持有{holding_days}天。"
            ),
        }
    if trigger is None and max_days > 0 and holding_days >= max_days:
        trigger = {
            "reason": "time_limit",
            "reason_detail": (
                f"触发卖出：已持有{holding_days}天，达到{cfg['name']}策略最长{max_days}天，"
                f"当前收益{profit_rate:.2f}%。"
            ),
        }

    if holding_days < 1:
        pending = trigger or {}
        return {
            **base,
            "action": "WAIT_EXECUTION" if trigger else "WAIT",
            "reason": "t_plus_one",
            "exit_intent": bool(trigger),
            "pending_exit_reason": pending.get("reason"),
            "reason_detail": (
                (pending.get("reason_detail", "") + " ") if trigger else ""
            ) + (
                f"当前受A股T+1限制，收益{profit_rate:.2f}%，最早下一交易日执行；"
                "退出意图会保留并在下一次扫描继续核验。"
                if trigger
                else f"继续持有：今日买入受A股T+1限制，当前收益{profit_rate:.2f}%，最早下一交易日才能卖出。"
            ),
        }

    if near_limit_down:
        pending = trigger or {
            "reason": "liquidity_risk",
            "reason_detail": "接近跌停导致模拟成交条件不足",
        }
        return {
            **base,
            "action": "WAIT_EXECUTION",
            "reason": "near_limit_down",
            "exit_intent": True,
            "pending_exit_reason": pending.get("reason"),
            "reason_detail": (
                f"{pending.get('reason_detail')}；当前收益{profit_rate:.2f}%。"
                "退出意图保留，下一次行情刷新继续尝试。"
            ),
        }

    if trigger:
        return {
            **base,
            **trigger,
            "should_sell": True,
            "action": "SELL",
            "exit_intent": True,
        }

    hold_detail = (
        f"继续持有：当前收益{profit_rate:.2f}%，未到止盈+{take_profit:.2f}%"
        f"（还差{max(0.0, take_profit - profit_rate):.2f}个百分点），未到止损{stop_loss:.2f}%"
        f"（距止损{max(0.0, profit_rate - stop_loss):.2f}个百分点），"
        f"已持有{holding_days}/{max_days}天（剩{max(0, max_days - holding_days)}天）"
    )
    if trailing_note:
        hold_detail += f"；{trailing_note}"
    if exit_intent == "WAIT_DATA":
        hold_detail += f"；动态退出数据暂不可用（{assessment.get('reason') or '待恢复'}），静态止损仍持续生效"
    return {**base, "reason_detail": hold_detail + "。"}


def _previous_trade_date(trade_date: str) -> str:
    trade_date = str(trade_date or "")[:10]
    if not trade_date:
        return ""
    try:
        rows = _read_sql("""
            SELECT trade_date
            FROM si_trade_calendar
            WHERE trade_status = 1 AND trade_date < :d
            ORDER BY trade_date DESC
            LIMIT 1
        """, {"d": trade_date})
        if rows:
            return str(rows[0]["trade_date"])[:10]
    except Exception as exc:
        logger.debug("trade calendar previous date lookup failed: %s", exc)
    try:
        rows = _read_sql("""
            SELECT MAX(trade_date) AS trade_date
            FROM sm_stock_kline
            WHERE k_type = 1
              AND trade_date < :d
        """, {"d": trade_date})
        if rows and rows[0].get("trade_date"):
            return str(rows[0]["trade_date"])[:10]
    except Exception as exc:
        logger.debug("kline previous date fallback lookup failed: %s", exc)
    try:
        d = datetime.strptime(trade_date, "%Y-%m-%d").date()
        d -= timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d.isoformat()
    except Exception as exc:
        logger.debug("weekday previous date fallback failed for %s: %s", trade_date, exc)
        return ""


def _is_trade_date(trade_date: str) -> bool:
    trade_date = str(trade_date or "")[:10]
    if not trade_date:
        return False
    try:
        rows = _read_sql("""
            SELECT trade_status
            FROM si_trade_calendar
            WHERE trade_date = :d
            LIMIT 1
        """, {"d": trade_date})
        if rows:
            return int(rows[0].get("trade_status") or 0) == 1
    except Exception as exc:
        logger.debug("trade calendar status lookup failed for %s: %s", trade_date, exc)
    try:
        rows = _read_sql("""
            SELECT 1 AS ok
            FROM sm_stock_kline
            WHERE k_type = 1
              AND trade_date = :d
            LIMIT 1
        """, {"d": trade_date})
        if rows:
            return True
    except Exception as exc:
        logger.debug("kline trade-date fallback lookup failed for %s: %s", trade_date, exc)
    try:
        d = datetime.strptime(trade_date, "%Y-%m-%d").date()
        return d.weekday() < 5
    except Exception as exc:
        logger.debug("weekday trade-date fallback failed for %s: %s", trade_date, exc)
        return False


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
                "change_pct": round((price - pre_close) / pre_close * 100, 2) if pre_close > 0 else 0,
                "volume": _safe_float(fields[8]) if len(fields) > 8 else 0,
                "turnover": _safe_float(fields[9]) if len(fields) > 9 else 0}
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
                        "volume": _safe_float(fields[8]) if len(fields) > 8 else 0,
                        "turnover": _safe_float(fields[9]) if len(fields) > 9 else 0,
                    }
            except (ValueError, IndexError):
                continue
        return result
    except Exception as e:
        logger.warning(f"批量新浪行情获取失败: {e}")
        return {}


def _fetch_qmt_snapshot_prices(stock_codes: list[str]) -> dict[str, dict]:
    clean = [str(c).strip().zfill(6) for c in stock_codes if c]
    if not clean:
        return {}
    try:
        placeholders = ",".join(f":c{i}" for i in range(len(clean)))
        params = {f"c{i}": code for i, code in enumerate(clean)}
        rows = _read_sql(f"""
            SELECT stock_code, short_name, price, change_pct, volume, amount, snapshot_at
            FROM sm_rt_quote_snapshot
            WHERE stock_code IN ({placeholders})
              AND snapshot_at >= NOW() - INTERVAL {SNAPSHOT_FALLBACK_MAX_AGE_MINUTES} MINUTE
            ORDER BY snapshot_at DESC
        """, params)
        result: dict[str, dict] = {}
        for row in rows:
            code = str(row.get("stock_code") or "").zfill(6)
            if code in result:
                continue
            price = _safe_float(row.get("price"))
            if price <= 0:
                continue
            result[code] = {
                "price": price,
                "source": "qmt_snapshot",
                "short_name": row.get("short_name") or code,
                "change_pct": _safe_float(row.get("change_pct")),
                "volume": _safe_float(row.get("volume")),
                "turnover": _safe_float(row.get("amount")),
                "snapshot_at": str(row.get("snapshot_at") or ""),
            }
        return result
    except Exception as e:
        logger.warning("QMT realtime snapshot read failed: %s", e)
        return {}


def _fetch_live_prices_batch(stock_codes: list[str]) -> dict[str, dict]:
    """Batch quote reader: local QMT snapshot first, external realtime fallback for gaps."""
    clean = [str(c).strip().zfill(6) for c in stock_codes if c]
    if not clean:
        return {}
    result = _fetch_qmt_snapshot_prices(clean)
    missing = [code for code in clean if code not in result]
    if not missing:
        return result
    try:
        import requests
        symbols = ",".join(_sina_symbol(c) for c in missing)
        resp = requests.get(
            f"https://hq.sinajs.cn/list={symbols}",
            headers={"User-Agent": "Mozilla/5.0 ProBigA", "Referer": "https://finance.sina.com.cn"},
            timeout=15,
        )
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
                        "price": price,
                        "source": "sina_live_fallback",
                        "short_name": fields[0],
                        "change_pct": round((price - pre_close) / pre_close * 100, 2) if pre_close > 0 else 0,
                        "volume": _safe_float(fields[8]) if len(fields) > 8 else 0,
                        "turnover": _safe_float(fields[9]) if len(fields) > 9 else 0,
                    }
            except (ValueError, IndexError):
                continue
        return result
    except Exception as e:
        logger.warning("Realtime fallback batch quote failed: %s", e)
        return result


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
        except Exception as exc:
            logger.debug("snapshot fallback price lookup failed for %s: %s", code, exc)

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


def _get_current_price(stock_code: str) -> dict | None:
    """Single quote reader used by simulated order matching.

    Priority: local Guojin QMT snapshot -> external realtime fallback during session ->
    latest daily close only outside realtime execution.
    """
    code = str(stock_code).strip().zfill(6)
    qmt = _fetch_qmt_snapshot_prices([code]).get(code)
    if qmt and qmt.get("price", 0) > 0:
        return qmt

    if _is_trading_time():
        live = _fetch_live_price_sina(code)
        if live and live.get("price", 0) > 0:
            live["source"] = "sina_live_fallback"
            return live

    try:
        rows = _read_sql("""
            SELECT close AS price, trade_date FROM sm_stock_kline
            WHERE stock_code = :c AND k_type = 1
            ORDER BY trade_date DESC LIMIT 1
        """, {"c": code})
        if rows and rows[0].get("price") and float(rows[0]["price"]) > 0:
            return {
                "price": float(rows[0]["price"]),
                "source": "kline_close",
                "trade_date": str(rows[0].get("trade_date", ""))[:10],
            }
    except Exception as e:
        logger.warning("daily close fallback failed for %s: %s", code, e)
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
    # 事件驱动模拟交易：信号池 → 模拟订单 → 持仓
    # ────────────────────────────────────────

    def prepare_signal_pool(
        self,
        trade_date: str = "",
        signal_date: str = "",
        *,
        strict: bool = True,
        reset: bool = False,
    ) -> dict:
        """把 AI 推荐转换成今日可执行信号池。

        strict=True 时，信号日必须是交易日 trade_date 的上一交易日；
        否则只生成候选，不允许自动新开仓。
        """
        _ensure_tables()
        trade_date = (trade_date or date.today().isoformat())[:10]
        if not _is_trade_date(trade_date):
            return {
                "status": "skipped",
                "reason": f"{trade_date} 不是交易日，不生成模拟交易信号池",
                "trade_date": trade_date,
                "signal_date": signal_date[:10] if signal_date else "",
                "counts": self.signal_pool_counts(trade_date),
            }
        expected_signal_date = _previous_trade_date(trade_date)
        signal_date = (signal_date or expected_signal_date)[:10]

        if strict and expected_signal_date and signal_date != expected_signal_date:
            return {
                "status": "error",
                "error": f"信号日必须是交易日上一交易日: {trade_date} 对应 {expected_signal_date}, 当前 {signal_date}",
                "trade_date": trade_date,
                "signal_date": signal_date,
                "expected_signal_date": expected_signal_date,
            }
        if not signal_date:
            return {
                "status": "error",
                "error": "无法确定信号日，不能生成模拟交易信号池",
                "trade_date": trade_date,
                "signal_date": "",
                "expected_signal_date": expected_signal_date,
            }

        if reset:
            _exec_sql("""
                DELETE FROM st_sim_signal
                WHERE trade_mode = 'live'
                  AND signal_date = :signal_date
                  AND trade_date = :trade_date
                  AND status NOT IN ('FILLED')
            """, {"signal_date": signal_date, "trade_date": trade_date})

        recs = fetch_recommended_candidates(signal_date)
        if not recs:
            recommendation_summary = recommended_candidate_summary(signal_date)
            if recommendation_summary.get("total", 0) > 0:
                error = (
                    f"{signal_date} 有 {recommendation_summary['total']} 条 AI 推荐，"
                    f"但可交易 ALLOW 候选为 {recommendation_summary.get('allow_count', 0)} 条，"
                    "今日禁止自动新开仓"
                )
            else:
                error = f"{signal_date} 没有 AI 推荐数据，今日禁止自动新开仓"
            return {
                "status": "error",
                "error": error,
                "trade_date": trade_date,
                "signal_date": signal_date,
                "expected_signal_date": expected_signal_date,
                "recommendation_summary": recommendation_summary,
                "counts": self.signal_pool_counts(trade_date),
            }

        total_recommendations = 0
        allowed_count = 0
        rejected_count = 0
        for rec in recs:
            total_recommendations += 1
            code = str(rec.get("stock_code") or "").zfill(6)
            if not code:
                continue
            for stype in STRATEGY_CONFIG:
                decision = build_buy_decision(stype, rec)
                if not decision.get("allowed"):
                    rejected_count += 1
                    continue

                allowed_count += 1
                analysis = decision.get("analysis") or {}
                _exec_sql("""
                    INSERT INTO st_sim_signal
                    (trade_mode, signal_date, trade_date, stock_code, short_name, strategy_type,
                     status, reason, ai_score, quality_score, entry_score, final_trade_score,
                     expected_return_pct, risk_reward_ratio, short_score, long_score, capital_score, technical_score,
                     fundamental_score, main_wave_score, trend_hold_score, event_risk_level,
                     entry_price_low, entry_price_high, stop_loss_price, take_profit_1, take_profit_2,
                     updated_at)
                    VALUES
                    ('live', :signal_date, :trade_date, :code, :name, :strategy_type,
                     'NEW', :reason, :ai_score, :quality_score, :entry_score, :final_trade_score,
                     :expected_return_pct, :risk_reward_ratio, :short_score, :long_score, :capital_score, :technical_score,
                     :fundamental_score, :main_wave_score, :trend_hold_score, :risk_level,
                     :entry_low, :entry_high, :stop_loss, :take_profit_1, :take_profit_2,
                     NOW())
                    ON DUPLICATE KEY UPDATE
                     short_name = VALUES(short_name),
                     reason = VALUES(reason),
                     ai_score = VALUES(ai_score),
                     quality_score = VALUES(quality_score),
                     entry_score = VALUES(entry_score),
                     final_trade_score = VALUES(final_trade_score),
                     expected_return_pct = VALUES(expected_return_pct),
                     risk_reward_ratio = VALUES(risk_reward_ratio),
                     short_score = VALUES(short_score),
                     long_score = VALUES(long_score),
                     capital_score = VALUES(capital_score),
                     technical_score = VALUES(technical_score),
                     fundamental_score = VALUES(fundamental_score),
                     main_wave_score = VALUES(main_wave_score),
                     trend_hold_score = VALUES(trend_hold_score),
                     event_risk_level = VALUES(event_risk_level),
                     entry_price_low = VALUES(entry_price_low),
                     entry_price_high = VALUES(entry_price_high),
                     stop_loss_price = VALUES(stop_loss_price),
                     take_profit_1 = VALUES(take_profit_1),
                     take_profit_2 = VALUES(take_profit_2),
                     updated_at = NOW()
                """, {
                    "signal_date": signal_date,
                    "trade_date": trade_date,
                    "code": code,
                    "name": rec.get("short_name") or code,
                    "strategy_type": stype,
                    "reason": decision.get("reason", ""),
                    "ai_score": analysis.get("ai_score", 0),
                    "quality_score": analysis.get("quality_score", 0),
                    "entry_score": analysis.get("entry_score", 0),
                    "final_trade_score": analysis.get("final_trade_score", 0),
                    "expected_return_pct": analysis.get("expected_return_pct", 0),
                    "risk_reward_ratio": analysis.get("risk_reward_ratio", 0),
                    "short_score": analysis.get("short_score", 0),
                    "long_score": analysis.get("long_score", 0),
                    "capital_score": analysis.get("capital_score", 0),
                    "technical_score": analysis.get("technical_score", 0),
                    "fundamental_score": analysis.get("fundamental_score", 0),
                    "main_wave_score": analysis.get("main_wave_score", 0),
                    "trend_hold_score": analysis.get("trend_hold_score", 0),
                    "risk_level": analysis.get("event_risk_level", "LOW"),
                    "entry_low": rec.get("entry_price_low"),
                    "entry_high": rec.get("entry_price_high"),
                    "stop_loss": rec.get("stop_loss_price"),
                    "take_profit_1": rec.get("take_profit_1"),
                    "take_profit_2": rec.get("take_profit_2"),
                })

        return {
            "status": "ok",
            "trade_date": trade_date,
            "signal_date": signal_date,
            "expected_signal_date": expected_signal_date,
            "total_recommendations": total_recommendations,
            "allowed_count": allowed_count,
            "rejected_count": rejected_count,
            "counts": self.signal_pool_counts(trade_date),
        }

    def signal_pool_counts(self, trade_date: str = "", trade_mode: str = "live") -> dict:
        trade_date = (trade_date or date.today().isoformat())[:10]
        rows = _read_sql("""
            SELECT status, COUNT(*) AS cnt
            FROM st_sim_signal
            WHERE trade_mode = :mode AND trade_date = :trade_date
            GROUP BY status
        """, {"mode": trade_mode, "trade_date": trade_date})
        counts = {str(r.get("status") or ""): int(r.get("cnt") or 0) for r in rows}
        counts["total"] = sum(counts.values())
        return counts

    def order_counts(self, trade_date: str = "", trade_mode: str = "live") -> dict:
        trade_date = (trade_date or date.today().isoformat())[:10]
        rows = _read_sql("""
            SELECT status, side, COUNT(*) AS cnt
            FROM st_sim_order
            WHERE trade_mode = :mode AND order_date = :trade_date
            GROUP BY status, side
        """, {"mode": trade_mode, "trade_date": trade_date})
        counts: dict[str, int] = {"total": 0}
        for r in rows:
            status = str(r.get("status") or "")
            side = str(r.get("side") or "")
            cnt = int(r.get("cnt") or 0)
            counts[status] = counts.get(status, 0) + cnt
            counts[f"{side}_{status}"] = counts.get(f"{side}_{status}", 0) + cnt
            counts["total"] += cnt
        return counts

    def _log_event(
        self,
        event_type: str,
        *,
        trade_mode: str = "live",
        signal_id: int | None = None,
        order_id: int | None = None,
        position_id: int | None = None,
        stock_code: str = "",
        strategy_type: str = "",
        severity: str = "INFO",
        message: str = "",
        payload: dict | None = None,
        event_date: str = "",
        _connection=None,
    ) -> None:
        now = datetime.now()
        payload_text = None
        if payload is not None:
            try:
                payload_text = json.dumps(payload, ensure_ascii=False, default=str)
            except Exception:
                payload_text = json.dumps({"raw": str(payload)}, ensure_ascii=False)
        _connection_write(_connection, """
            INSERT INTO st_sim_event
            (trade_mode, event_date, event_time, event_type, signal_id, order_id, position_id,
             stock_code, strategy_type, severity, message, payload, created_at)
            VALUES
            (:mode, :event_date, :event_time, :event_type, :signal_id, :order_id, :position_id,
             :stock_code, :strategy_type, :severity, :message, :payload, NOW())
        """, {
            "mode": trade_mode,
            "event_date": (event_date or now.date().isoformat())[:10],
            "event_time": now.strftime("%H:%M:%S"),
            "event_type": event_type,
            "signal_id": signal_id,
            "order_id": order_id,
            "position_id": position_id,
            "stock_code": stock_code,
            "strategy_type": strategy_type,
            "severity": severity,
            "message": message,
            "payload": payload_text,
        })

    def portfolio_state(
        self,
        trade_mode: str = "live",
        trade_date: str = "",
        price_map: dict[str, dict] | None = None,
    ) -> dict:
        """Return current simulated portfolio state used by risk budgeting."""
        _ensure_tables()
        trade_mode = trade_mode or "live"
        trade_date = (trade_date or date.today().isoformat())[:10]
        price_map = price_map or {}

        holdings = _read_sql("""
            SELECT id, stock_code, short_name, strategy_type, buy_price, buy_amount,
                   buy_shares, buy_date, ai_score
            FROM st_sim_position
            WHERE status = 'holding'
              AND COALESCE(trade_mode, 'live') = :mode
        """, {"mode": trade_mode})
        closed_rows = _read_sql("""
            SELECT SUM(profit) AS realized_profit, SUM(fee_total) AS realized_fee
            FROM st_sim_position
            WHERE COALESCE(trade_mode, 'live') = :mode
        """, {"mode": trade_mode})
        order_rows = _read_sql("""
            SELECT side, strategy_type, stock_code, limit_price, target_price,
                   requested_shares, remaining_shares, filled_shares
            FROM st_sim_order
            WHERE COALESCE(trade_mode, 'live') = :mode
              AND status IN ('PENDING', 'PARTIAL', 'MATCHING')
        """, {"mode": trade_mode})

        realized_profit = _safe_float((closed_rows[0] if closed_rows else {}).get("realized_profit"))
        realized_fee = _safe_float((closed_rows[0] if closed_rows else {}).get("realized_fee"))

        used_by_strategy = {stype: 0.0 for stype in STRATEGY_CONFIG}
        used_by_stock: dict[str, float] = {}
        holding_value = 0.0
        holding_cost = 0.0
        holding_details = []

        missing_prices = []
        for h in holdings:
            code = str(h.get("stock_code") or "").zfill(6)
            buy_price = _safe_float(h.get("buy_price"))
            shares = int(h.get("buy_shares") or 0)
            price_info = price_map.get(code)
            cur_price = _safe_float((price_info or {}).get("price"), buy_price)
            if cur_price <= 0:
                missing_prices.append(code)
                cur_price = buy_price
            value = round(cur_price * shares, 2)
            cost = round(buy_price * shares, 2)
            holding_value += value
            holding_cost += cost
            stype = str(h.get("strategy_type") or "")
            used_by_strategy[stype] = used_by_strategy.get(stype, 0.0) + value
            used_by_stock[code] = used_by_stock.get(code, 0.0) + value
            holding_details.append({
                "id": h.get("id"),
                "stock_code": code,
                "short_name": h.get("short_name") or code,
                "strategy_type": stype,
                "buy_price": buy_price,
                "cur_price": cur_price,
                "shares": shares,
                "market_value": value,
                "cost": cost,
                "unrealized_profit": round(value - cost, 2),
            })

        pending_buy_amount = 0.0
        pending_by_strategy = {stype: 0.0 for stype in STRATEGY_CONFIG}
        pending_by_stock: dict[str, float] = {}
        for o in order_rows:
            if str(o.get("side") or "").upper() != "BUY":
                continue
            remain = int(o.get("remaining_shares") or 0)
            if remain <= 0:
                requested = int(o.get("requested_shares") or 0)
                filled = int(o.get("filled_shares") or 0)
                remain = max(0, requested - filled)
            px = _safe_float(o.get("limit_price"), _safe_float(o.get("target_price")))
            amount = round(px * remain, 2) if px > 0 and remain > 0 else 0.0
            pending_buy_amount += amount
            stype = str(o.get("strategy_type") or "")
            code = str(o.get("stock_code") or "").zfill(6)
            pending_by_strategy[stype] = pending_by_strategy.get(stype, 0.0) + amount
            pending_by_stock[code] = pending_by_stock.get(code, 0.0) + amount

        unrealized_profit = round(holding_value - holding_cost, 2)
        total_equity = round(SIM_INITIAL_CAPITAL + realized_profit + unrealized_profit, 2)
        cash_before_pending = round(SIM_INITIAL_CAPITAL + realized_profit - holding_cost, 2)
        cash_available = round(cash_before_pending - pending_buy_amount, 2)
        cash_buffer_amount = round(total_equity * SIM_RISK_CONFIG["cash_buffer_pct"], 2)
        max_total_position_amount = round(total_equity * SIM_RISK_CONFIG["max_total_position_pct"], 2)
        total_available_for_position = max(
            0.0,
            max_total_position_amount - holding_value - pending_buy_amount,
            0.0,
        )
        cash_available_after_buffer = max(0.0, cash_available - cash_buffer_amount)

        return {
            "trade_mode": trade_mode,
            "trade_date": trade_date,
            "initial_capital": round(SIM_INITIAL_CAPITAL, 2),
            "realized_profit": round(realized_profit, 2),
            "realized_fee": round(realized_fee, 2),
            "unrealized_profit": unrealized_profit,
            "total_equity": total_equity,
            "holding_value": round(holding_value, 2),
            "holding_cost": round(holding_cost, 2),
            "pending_buy_amount": round(pending_buy_amount, 2),
            "cash_before_pending": cash_before_pending,
            "cash_available": cash_available,
            "cash_buffer_amount": cash_buffer_amount,
            "cash_available_after_buffer": round(cash_available_after_buffer, 2),
            "max_total_position_amount": max_total_position_amount,
            "total_available_for_position": round(total_available_for_position, 2),
            "position_usage_rate": round(holding_value / total_equity * 100, 2) if total_equity > 0 else 0.0,
            "used_by_strategy": {k: round(v, 2) for k, v in used_by_strategy.items()},
            "pending_by_strategy": {k: round(v, 2) for k, v in pending_by_strategy.items()},
            "used_by_stock": {k: round(v, 2) for k, v in used_by_stock.items()},
            "pending_by_stock": {k: round(v, 2) for k, v in pending_by_stock.items()},
            "holdings": holding_details,
            "missing_prices": missing_prices,
        }

    def _save_risk_budget_snapshot(self, state: dict, trade_date: str) -> None:
        for stype in STRATEGY_CONFIG:
            equity = _safe_float(state.get("total_equity"), SIM_INITIAL_CAPITAL)
            max_strategy_amount = equity * SIM_RISK_CONFIG["strategy_budget_pct"].get(stype, 0.25)
            used = _safe_float((state.get("used_by_strategy") or {}).get(stype))
            pending = _safe_float((state.get("pending_by_strategy") or {}).get(stype))
            available = max(
                0.0,
                max_strategy_amount - used - pending,
                _safe_float(state.get("total_available_for_position")),
                0.0,
            )
            available = min(
                max_strategy_amount - used - pending,
                _safe_float(state.get("total_available_for_position")),
                _safe_float(state.get("cash_available_after_buffer")),
            )
            available = max(0.0, available)
            _exec_sql("""
                INSERT INTO st_sim_risk_budget
                (trade_mode, budget_date, strategy_type, initial_capital, total_equity,
                 cash_available, max_total_position_amount, max_strategy_amount,
                 used_strategy_amount, pending_strategy_amount, available_strategy_amount,
                 risk_budget_note, created_at, updated_at)
                VALUES
                (:mode, :budget_date, :strategy_type, :initial_capital, :total_equity,
                 :cash_available, :max_total_position_amount, :max_strategy_amount,
                 :used_strategy_amount, :pending_strategy_amount, :available_strategy_amount,
                 :risk_budget_note, NOW(), NOW())
                ON DUPLICATE KEY UPDATE
                 initial_capital = VALUES(initial_capital),
                 total_equity = VALUES(total_equity),
                 cash_available = VALUES(cash_available),
                 max_total_position_amount = VALUES(max_total_position_amount),
                 max_strategy_amount = VALUES(max_strategy_amount),
                 used_strategy_amount = VALUES(used_strategy_amount),
                 pending_strategy_amount = VALUES(pending_strategy_amount),
                 available_strategy_amount = VALUES(available_strategy_amount),
                 risk_budget_note = VALUES(risk_budget_note),
                 updated_at = NOW()
            """, {
                "mode": state.get("trade_mode") or "live",
                "budget_date": trade_date,
                "strategy_type": stype,
                "initial_capital": state.get("initial_capital"),
                "total_equity": state.get("total_equity"),
                "cash_available": state.get("cash_available"),
                "max_total_position_amount": state.get("max_total_position_amount"),
                "max_strategy_amount": round(max_strategy_amount, 2),
                "used_strategy_amount": used,
                "pending_strategy_amount": pending,
                "available_strategy_amount": round(available, 2),
                "risk_budget_note": (
                    f"总仓位上限{SIM_RISK_CONFIG['max_total_position_pct']:.0%}，"
                    f"现金缓冲{SIM_RISK_CONFIG['cash_buffer_pct']:.0%}，"
                    f"单票上限{SIM_RISK_CONFIG['max_single_stock_pct']:.0%}"
                ),
            })

    def _risk_budget_for_signal(self, signal: dict, price: float, state: dict) -> dict:
        stype = str(signal.get("strategy_type") or "")
        cfg = STRATEGY_CONFIG.get(stype) or {}
        code = str(signal.get("stock_code") or "").zfill(6)
        equity = max(_safe_float(state.get("total_equity")), SIM_INITIAL_CAPITAL)
        risk_level = str(signal.get("event_risk_level") or "LOW").upper()
        risk_multiplier = SIM_RISK_CONFIG["risk_multiplier"].get(risk_level, 0.5)
        if risk_multiplier <= 0:
            return {"allowed": False, "reason": f"风险等级{risk_level}禁止新开仓", "shares": 0, "amount": 0}

        stop_price = _safe_float(signal.get("stop_loss_price"))
        if stop_price <= 0 and cfg.get("stop_loss") is not None:
            stop_price = price * (1 + float(cfg["stop_loss"]) / 100.0)
        if stop_price <= 0 or stop_price >= price:
            stop_price = price * 0.97
        per_share_risk = max(price - stop_price, price * 0.015)

        final_score = _safe_float(signal.get("final_trade_score"), _safe_float(signal.get("ai_score"), 70))
        quality_multiplier = max(0.60, min(1.25, final_score / 80.0 if final_score > 0 else 0.85))
        raw_amount = min(_safe_float(cfg.get("buy_amount"), 100000), equity * SIM_RISK_CONFIG["max_single_stock_pct"])
        strategy_cap = equity * SIM_RISK_CONFIG["strategy_budget_pct"].get(stype, 0.25)
        used_strategy = _safe_float((state.get("used_by_strategy") or {}).get(stype))
        pending_strategy = _safe_float((state.get("pending_by_strategy") or {}).get(stype))
        strategy_available = max(0.0, strategy_cap - used_strategy - pending_strategy)

        single_cap = equity * SIM_RISK_CONFIG["max_single_stock_pct"]
        used_stock = _safe_float((state.get("used_by_stock") or {}).get(code))
        pending_stock = _safe_float((state.get("pending_by_stock") or {}).get(code))
        single_available = max(0.0, single_cap - used_stock - pending_stock)

        total_available = _safe_float(state.get("total_available_for_position"))
        cash_available = _safe_float(state.get("cash_available_after_buffer"))
        risk_cash = equity * SIM_RISK_CONFIG["per_trade_risk_pct"] * risk_multiplier * quality_multiplier
        risk_sized_amount = max(0.0, (risk_cash / per_share_risk) * price) if per_share_risk > 0 else 0.0

        max_amount = min(raw_amount, strategy_available, single_available, total_available, cash_available, risk_sized_amount)
        shares = int(max_amount / price / 100) * 100 if price > 0 else 0
        amount = round(shares * price, 2)

        note = (
            f"raw={raw_amount:.0f}; strategy_avail={strategy_available:.0f}; "
            f"single_avail={single_available:.0f}; total_avail={total_available:.0f}; "
            f"cash_after_buffer={cash_available:.0f}; risk_sized={risk_sized_amount:.0f}; "
            f"risk_level={risk_level}; score={final_score:.0f}; stop={stop_price:.2f}"
        )
        if amount < SIM_RISK_CONFIG["min_order_amount"] or shares < 100:
            return {
                "allowed": False,
                "reason": "组合风险预算不足，未达到最小下单金额/100股整手",
                "shares": 0,
                "amount": 0,
                "budget_amount": round(max_amount, 2),
                "note": note,
                "stop_price": round(stop_price, 4),
                "per_share_risk": round(per_share_risk, 4),
            }
        return {
            "allowed": True,
            "shares": shares,
            "amount": amount,
            "budget_amount": round(max_amount, 2),
            "note": note,
            "stop_price": round(stop_price, 4),
            "per_share_risk": round(per_share_risk, 4),
        }

    def _mark_signal_check(self, signal_id: int, reason: str, status: str = "") -> None:
        if status:
            _exec_sql("""
                UPDATE st_sim_signal
                SET status = :status, last_check_reason = :reason,
                    last_check_at = NOW(), updated_at = NOW()
                WHERE id = :id
            """, {"id": signal_id, "status": status, "reason": reason})
        else:
            _exec_sql("""
                UPDATE st_sim_signal
                SET last_check_reason = :reason, last_check_at = NOW(), updated_at = NOW()
                WHERE id = :id
            """, {"id": signal_id, "reason": reason})

    def _expire_unfilled_signals(self, trade_date: str) -> int:
        e = get_engine()
        with e.begin() as c:
            result = c.execute(text("""
                UPDATE st_sim_signal
                SET status = 'EXPIRED',
                    last_check_reason = '收盘后未触发买入，信号过期',
                    last_check_at = NOW(),
                    updated_at = NOW()
                WHERE trade_mode = 'live'
                  AND trade_date = :trade_date
                  AND status = 'NEW'
            """), {"trade_date": trade_date})
            return int(result.rowcount or 0)

    def _create_order(
        self,
        signal: dict,
        price: float,
        shares: int,
        reason: str,
        *,
        side: str = "BUY",
        trade_mode: str = "live",
        order_type: str = "SIM_LIMIT",
        position_id: int | None = None,
        risk_budget: dict | None = None,
        price_source: str = "",
        execution_gate: dict | None = None,
        source_event: str = "",
    ) -> int:
        now = datetime.now()
        side = str(side or "BUY").upper()
        risk_budget = risk_budget or {}
        execution_gate = dict(execution_gate or {})
        order_date = str(signal.get("trade_date") or date.today().isoformat())[:10]
        if shares <= 0:
            raise ValueError("sim order requested_shares must be positive")
        if side == "BUY" and not _execution_gate_allows_expected_buy(
            execution_gate,
            signal.get("stock_code") or "",
            order_date,
        ):
            raise ValueError(
                "execution-time buy gate blocked order creation: "
                + str(execution_gate.get("reason") or "missing valid gate evidence")
            )
        limit_price = signal.get("entry_price_high") or price
        if side == "SELL" and order_type == "SIM_MARKET":
            limit_price = None
        return _exec_insert_get_id("""
            INSERT INTO st_sim_order
            (signal_id, trade_mode, order_date, order_time, stock_code, short_name,
             strategy_type, side, order_type, limit_price, target_price, requested_shares,
             remaining_shares, status, position_id, source_event, price_source,
             risk_budget_amount, risk_budget_note, reason,
             execution_gate_status, execution_gate_hash, execution_gate_checked_at,
             execution_gate_valid_until, execution_gate_evidence, created_at, updated_at)
            VALUES
            (:signal_id, :trade_mode, :order_date, :order_time, :code, :name,
             :strategy_type, :side, :order_type, :limit_price, :target_price, :requested_shares,
             :remaining_shares, 'PENDING', :position_id, :source_event, :price_source,
             :risk_budget_amount, :risk_budget_note, :reason,
             :execution_gate_status, :execution_gate_hash, :execution_gate_checked_at,
             :execution_gate_valid_until, :execution_gate_evidence, NOW(), NOW())
        """, {
            "signal_id": signal.get("id"),
            "trade_mode": trade_mode,
            "order_date": order_date,
            "order_time": now.strftime("%H:%M:%S"),
            "code": signal["stock_code"],
            "name": signal.get("short_name") or signal["stock_code"],
            "strategy_type": signal["strategy_type"],
            "side": side,
            "order_type": order_type,
            "limit_price": limit_price,
            "target_price": price,
            "requested_shares": shares,
            "remaining_shares": shares,
            "position_id": position_id,
            "source_event": (
                source_event
                or signal.get("source_event")
                or ("SIGNAL_BUY" if side == "BUY" else "RISK_SELL_GTC")
            ),
            "price_source": price_source,
            "risk_budget_amount": risk_budget.get("budget_amount", 0),
            "risk_budget_note": risk_budget.get("note", ""),
            "reason": reason,
            "execution_gate_status": str(execution_gate.get("status") or "DATA_BLOCKED"),
            "execution_gate_hash": str(execution_gate.get("context_hash") or ""),
            "execution_gate_checked_at": _gate_sql_datetime(execution_gate.get("evaluated_at")),
            "execution_gate_valid_until": _gate_sql_datetime(execution_gate.get("valid_until")),
            "execution_gate_evidence": _gate_evidence_json(execution_gate),
        })

    def _mark_signal_ordered(
        self,
        signal_id: int,
        order_id: int,
        shares: int,
        amount: float,
        budget: dict,
        reason: str,
        execution_gate: dict,
    ) -> None:
        _exec_sql("""
            UPDATE st_sim_signal
            SET status = 'ORDERED',
                pending_order_id = :order_id,
                intended_shares = :shares,
                intended_amount = :amount,
                risk_budget_amount = :budget_amount,
                risk_budget_note = :risk_budget_note,
                execution_gate_status = :execution_gate_status,
                execution_gate_hash = :execution_gate_hash,
                execution_gate_checked_at = :execution_gate_checked_at,
                execution_gate_valid_until = :execution_gate_valid_until,
                execution_gate_evidence = :execution_gate_evidence,
                last_check_reason = :reason,
                last_check_at = NOW(),
                updated_at = NOW()
            WHERE id = :id
        """, {
            "id": signal_id,
            "order_id": order_id,
            "shares": shares,
            "amount": amount,
            "budget_amount": budget.get("budget_amount", amount),
            "risk_budget_note": budget.get("note", ""),
            "reason": reason,
            "execution_gate_status": str(execution_gate.get("status") or "DATA_BLOCKED"),
            "execution_gate_hash": str(execution_gate.get("context_hash") or ""),
            "execution_gate_checked_at": _gate_sql_datetime(execution_gate.get("evaluated_at")),
            "execution_gate_valid_until": _gate_sql_datetime(execution_gate.get("valid_until")),
            "execution_gate_evidence": _gate_evidence_json(execution_gate),
        })

    def _fill_order(
        self,
        order_id: int,
        price: float,
        shares: int,
        amount: float,
        fee: float,
        position_id: int,
        *,
        _connection=None,
    ) -> None:
        result = _connection_write(_connection, """
            UPDATE st_sim_order
            SET status = CASE
                    WHEN GREATEST(remaining_shares - :shares, 0) = 0 THEN 'FILLED'
                    ELSE 'PARTIAL'
                END,
                filled_price = CASE
                    WHEN filled_shares + :shares > 0
                    THEN ((COALESCE(filled_price, 0) * filled_shares) + (:price * :shares)) / (filled_shares + :shares)
                    ELSE :price
                END,
                filled_shares = filled_shares + :shares,
                remaining_shares = GREATEST(remaining_shares - :shares, 0),
                filled_amount = filled_amount + :amount,
                fee = fee + :fee,
                position_id = :position_id,
                filled_at = CASE WHEN GREATEST(remaining_shares - :shares, 0) = 0 THEN NOW() ELSE filled_at END,
                match_count = match_count + 1,
                last_match_reason = 'matched',
                updated_at = NOW()
            WHERE id = :id
              AND status IN ('MATCHING', 'PENDING', 'PARTIAL')
        """, {
            "id": order_id,
            "price": price,
            "shares": shares,
            "amount": amount,
            "fee": fee,
            "position_id": position_id,
        })
        if _connection is not None and int(result.rowcount or 0) != 1:
            raise RuntimeError(f"claimed sim order {order_id} could not be filled")

    def _mark_order_waiting(self, order_id: int, reason: str, *, _connection=None) -> None:
        _connection_write(_connection, """
            UPDATE st_sim_order
            SET status = CASE
                    WHEN status = 'MATCHING' AND filled_shares > 0 THEN 'PARTIAL'
                    WHEN status = 'MATCHING' THEN 'PENDING'
                    ELSE status
                END,
                last_match_reason = :reason,
                updated_at = NOW()
            WHERE id = :id
              AND status IN ('MATCHING', 'PENDING', 'PARTIAL')
        """, {"id": order_id, "reason": reason})

    def _mark_order_closed(
        self,
        order_id: int,
        status: str,
        reason: str,
        *,
        _connection=None,
    ) -> None:
        status = str(status or "CANCELLED").upper()
        _connection_write(_connection, """
            UPDATE st_sim_order
            SET status = CASE
                    WHEN filled_shares > 0 AND :status = 'CANCELLED' THEN 'PARTIAL_CANCELLED'
                    WHEN filled_shares > 0 AND :status = 'EXPIRED' THEN 'PARTIAL_EXPIRED'
                    ELSE :status
                END,
                remaining_shares = CASE
                    WHEN :status IN ('CANCELLED', 'EXPIRED', 'REJECTED') THEN 0
                    ELSE remaining_shares
                END,
                reject_reason = CASE WHEN :status = 'REJECTED' THEN :reason ELSE reject_reason END,
                cancel_reason = CASE WHEN :status IN ('CANCELLED', 'EXPIRED') THEN :reason ELSE cancel_reason END,
                last_match_reason = :reason,
                updated_at = NOW()
            WHERE id = :id
        """, {"id": order_id, "status": status, "reason": reason})

    def _refresh_order_execution_gate(self, order_id: int, gate: dict, *, _connection=None) -> None:
        """Bind the exact gate used by the immediately following fill attempt."""
        _connection_write(_connection, """
            UPDATE st_sim_order
            SET execution_gate_status = :status,
                execution_gate_hash = :context_hash,
                execution_gate_checked_at = :checked_at,
                execution_gate_valid_until = :valid_until,
                execution_gate_evidence = :evidence,
                updated_at = NOW()
            WHERE id = :id
              AND side = 'BUY'
              AND status IN ('MATCHING', 'PENDING', 'PARTIAL')
        """, {
            "id": order_id,
            "status": str(gate.get("status") or "DATA_BLOCKED"),
            "context_hash": str(gate.get("context_hash") or ""),
            "checked_at": _gate_sql_datetime(gate.get("evaluated_at")),
            "valid_until": _gate_sql_datetime(gate.get("valid_until")),
            "evidence": _gate_evidence_json(gate),
        })

    def _cancel_buy_order_for_execution_gate(
        self,
        order: dict,
        gate: dict,
        reason: str,
        *,
        requeue_if_unfilled: bool = False,
        _connection=None,
    ) -> dict:
        filled_shares = int(order.get("filled_shares") or 0)
        order_status = "PARTIAL_CANCELLED" if filled_shares > 0 else "CANCELLED"
        _connection_write(_connection, """
            UPDATE st_sim_order
            SET status = :status,
                remaining_shares = 0,
                cancel_reason = :reason,
                last_match_reason = :reason,
                execution_gate_status = :gate_status,
                execution_gate_hash = :context_hash,
                execution_gate_checked_at = :checked_at,
                execution_gate_valid_until = :valid_until,
                execution_gate_evidence = :evidence,
                updated_at = NOW()
            WHERE id = :id
              AND side = 'BUY'
              AND status IN ('MATCHING', 'PENDING', 'PARTIAL')
        """, {
            "id": order["id"],
            "status": order_status,
            "reason": reason,
            "gate_status": str(gate.get("status") or "DATA_BLOCKED"),
            "context_hash": str(gate.get("context_hash") or ""),
            "checked_at": _gate_sql_datetime(gate.get("evaluated_at")),
            "valid_until": _gate_sql_datetime(gate.get("valid_until")),
            "evidence": _gate_evidence_json(gate),
        })
        signal_id = order.get("signal_id")
        if signal_id:
            signal_status = (
                "PARTIAL_CANCELLED"
                if filled_shares > 0
                else "NEW"
                if requeue_if_unfilled
                else "EXECUTION_BLOCKED"
            )
            _connection_write(_connection, """
                UPDATE st_sim_signal
                SET status = :status,
                    pending_order_id = NULL,
                    execution_gate_status = :gate_status,
                    execution_gate_hash = :context_hash,
                    execution_gate_checked_at = :checked_at,
                    execution_gate_valid_until = :valid_until,
                    execution_gate_evidence = :evidence,
                    last_check_reason = :reason,
                    last_check_at = NOW(),
                    updated_at = NOW()
                WHERE id = :signal_id
            """, {
                "signal_id": signal_id,
                "status": signal_status,
                "reason": reason,
                "gate_status": str(gate.get("status") or "DATA_BLOCKED"),
                "context_hash": str(gate.get("context_hash") or ""),
                "checked_at": _gate_sql_datetime(gate.get("evaluated_at")),
                "valid_until": _gate_sql_datetime(gate.get("valid_until")),
                "evidence": _gate_evidence_json(gate),
            })
        self._log_event(
            "BUY_EXECUTION_GATE_CANCELLED",
            trade_mode=order.get("trade_mode") or "live",
            signal_id=signal_id,
            order_id=order["id"],
            stock_code=order.get("stock_code") or "",
            strategy_type=order.get("strategy_type") or "",
            severity="WARN",
            message=reason,
            payload={"gate": gate, "filled_shares": filled_shares},
            event_date=str(order.get("order_date") or date.today().isoformat())[:10],
            _connection=_connection,
        )
        return {
            "order_id": order["id"],
            "status": "partial_cancelled" if filled_shares > 0 else "cancelled",
            "reason": "execution_gate",
            "detail": reason,
        }

    def _expire_open_orders(self, trade_date: str) -> int:
        now = datetime.now()
        buy_expired = 0
        e = get_engine()
        with e.begin() as c:
            if _is_signal_expire_time(now):
                result = c.execute(text("""
                    UPDATE st_sim_order
                    SET status = CASE WHEN filled_shares > 0 THEN 'PARTIAL_EXPIRED' ELSE 'EXPIRED' END,
                        remaining_shares = 0,
                        cancel_reason = '收盘后买入订单未成交，自动过期',
                        last_match_reason = '收盘后买入订单未成交，自动过期',
                        updated_at = NOW()
                    WHERE trade_mode = 'live'
                      AND order_date = :trade_date
                      AND side = 'BUY'
                      AND status IN ('PENDING', 'PARTIAL')
                """), {"trade_date": trade_date})
                buy_expired += int(result.rowcount or 0)
            if now.time() > datetime.strptime("15:01:00", "%H:%M:%S").time():
                result = c.execute(text("""
                    UPDATE st_sim_order
                    SET status = CASE WHEN filled_shares > 0 THEN 'PARTIAL_EXPIRED' ELSE 'EXPIRED' END,
                        remaining_shares = 0,
                        cancel_reason = '收盘后订单未成交，自动过期',
                        last_match_reason = '收盘后订单未成交，自动过期',
                        updated_at = NOW()
                    WHERE trade_mode = 'live'
                      AND order_date = :trade_date
                      AND status IN ('PENDING', 'PARTIAL')
                      AND NOT (side = 'SELL' AND source_event LIKE 'RISK_SELL_GTC%')
                """), {"trade_date": trade_date})
                buy_expired += int(result.rowcount or 0)
            c.execute(text("""
                UPDATE st_sim_signal s
                JOIN st_sim_order o ON o.id = s.pending_order_id
                SET s.status = CASE
                        WHEN o.status = 'PARTIAL_EXPIRED' THEN 'PARTIAL_EXPIRED'
                        ELSE 'EXPIRED'
                    END,
                    s.last_check_reason = COALESCE(o.cancel_reason, o.last_match_reason, '订单过期'),
                    s.last_check_at = NOW(),
                    s.updated_at = NOW()
                WHERE s.trade_mode = 'live'
                  AND s.trade_date = :trade_date
                  AND s.status = 'ORDERED'
                  AND o.status IN ('EXPIRED', 'PARTIAL_EXPIRED')
            """), {"trade_date": trade_date})
        return buy_expired

    def _slipped_price(self, side: str, price: float) -> float:
        side = str(side or "").upper()
        pct = SIM_RISK_CONFIG["slippage_buy_pct"] if side == "BUY" else SIM_RISK_CONFIG["slippage_sell_pct"]
        sign = 1 if side == "BUY" else -1
        return round(price * (1 + sign * pct / 100.0), 4)

    def _liquidity_shares(self, order: dict, price_info: dict | None) -> int:
        remaining = int(order.get("remaining_shares") or 0)
        if remaining <= 0:
            requested = int(order.get("requested_shares") or 0)
            filled = int(order.get("filled_shares") or 0)
            remaining = max(0, requested - filled)
        volume = _safe_float((price_info or {}).get("volume"))
        if volume <= 0:
            return remaining
        max_shares = int(volume * SIM_RISK_CONFIG["liquidity_volume_pct"] / 100) * 100
        return min(remaining, max(0, max_shares))

    def _create_sell_order_from_signal(self, sell_signal: dict, trade_date: str) -> dict | None:
        position_id = int(sell_signal.get("position_id") or 0)
        if position_id <= 0:
            return None
        trade_date = _normalize_trade_date(trade_date)
        assessment = sell_signal.get("holding_assessment")
        assessment = assessment if isinstance(assessment, dict) else {}
        exit_intent = str(
            assessment.get("exit_intent")
            or sell_signal.get("requested_exit_intent")
            or ("REDUCE" if sell_signal.get("reason") == "dynamic_reduce" else "SELL")
        ).upper()
        if exit_intent not in {"SELL", "REDUCE"}:
            exit_intent = "SELL"

        available_shares = int(sell_signal.get("shares") or 0)
        requested_shares = available_shares
        reduce_ratio = 1.0
        if exit_intent == "REDUCE":
            reduce_ratio = _safe_float(
                sell_signal.get("reduce_ratio"),
                _safe_float(assessment.get("reduce_ratio"), 0.50),
            )
            if not (0 < reduce_ratio < 1):
                reduce_ratio = 0.50
            requested_shares = int((available_shares * reduce_ratio) / 100) * 100
            if requested_shares < 100:
                return {
                    "status": "waiting",
                    "reason": "reduce_below_board_lot",
                    "position_id": position_id,
                    "exit_intent": exit_intent,
                    "available_shares": available_shares,
                    "requested_shares": 0,
                }
        if requested_shares <= 0 or requested_shares > available_shares:
            return {
                "status": "waiting",
                "reason": "no_sellable_shares",
                "position_id": position_id,
                "exit_intent": exit_intent,
                "available_shares": available_shares,
                "requested_shares": 0,
            }

        source_event = f"RISK_SELL_GTC_{exit_intent}"
        existing = _read_sql("""
            SELECT id, status
            FROM st_sim_order
            WHERE trade_mode = :mode
              AND side = 'SELL'
              AND position_id = :position_id
              AND (
                    status IN ('PENDING', 'PARTIAL', 'MATCHING')
                    OR (
                        :exit_intent = 'REDUCE'
                        AND order_date = :trade_date
                        AND status = 'FILLED'
                        AND source_event = :source_event
                    )
                  )
            ORDER BY id DESC
            LIMIT 1
        """, {
            "mode": sell_signal.get("trade_mode") or "live",
            "position_id": position_id,
            "exit_intent": exit_intent,
            "trade_date": trade_date,
            "source_event": source_event,
        })
        if existing:
            return {
                "status": "exists",
                "order_id": existing[0]["id"],
                "position_id": position_id,
                "exit_intent": exit_intent,
            }

        order_signal = {
            "id": None,
            "trade_date": trade_date,
            "stock_code": sell_signal["stock_code"],
            "short_name": sell_signal.get("short_name") or sell_signal["stock_code"],
            "strategy_type": sell_signal["strategy_type"],
            "source_event": source_event,
        }
        reason = sell_signal.get("reason_detail") or sell_signal.get("reason") or "risk_sell"
        if exit_intent == "REDUCE":
            reason += f"；风险减仓比例{reduce_ratio:.0%}，整手委托{requested_shares}股"
        order_id = self._create_order(
            order_signal,
            _safe_float(sell_signal.get("sell_price")),
            requested_shares,
            reason,
            side="SELL",
            trade_mode=sell_signal.get("trade_mode") or "live",
            order_type="SIM_MARKET",
            position_id=position_id,
            price_source=sell_signal.get("price_source", ""),
            source_event=source_event,
        )
        self._log_event(
            "SELL_ORDER_CREATED",
            trade_mode=sell_signal.get("trade_mode") or "live",
            order_id=order_id,
            position_id=position_id,
            stock_code=sell_signal["stock_code"],
            strategy_type=sell_signal["strategy_type"],
            message=reason,
            payload=sell_signal,
            event_date=trade_date,
        )
        return {
            "status": "created",
            "order_id": order_id,
            "position_id": position_id,
            "exit_intent": exit_intent,
            "requested_shares": requested_shares,
            "reduce_ratio": reduce_ratio,
            "time_in_force": "GTC",
        }

    def _match_buy_order(
        self,
        order: dict,
        price_info: dict,
        state: dict,
        *,
        effective_trade_date: str = "",
        _connection=None,
    ) -> dict:
        effective_trade_date = _normalize_trade_date(
            effective_trade_date or order.get("order_date") or date.today().isoformat(),
            field_name="effective_trade_date",
        )
        order_date = _normalize_trade_date(order.get("order_date"), field_name="order.order_date")
        if order_date != effective_trade_date:
            self._mark_order_closed(
                order["id"],
                "EXPIRED",
                "BUY is DAY-only and cannot be filled on another trade date",
                _connection=_connection,
            )
            return {"order_id": order["id"], "status": "expired", "reason": "buy_day_only"}
        price = _safe_float(price_info.get("price"))
        if price <= 0:
            self._mark_order_waiting(order["id"], "未获取到可用实时价格", _connection=_connection)
            return {"order_id": order["id"], "status": "waiting", "reason": "no_price"}
        if price_info.get("source") == "kline_close":
            self._mark_order_waiting(order["id"], "实时撮合禁止使用日K收盘价", _connection=_connection)
            return {"order_id": order["id"], "status": "waiting", "reason": "stale_price"}
        if _is_near_limit_up(price_info):
            self._mark_order_waiting(order["id"], "接近涨停，买入订单等待", _connection=_connection)
            return {"order_id": order["id"], "status": "waiting", "reason": "limit_up"}
        limit_price = _safe_float(order.get("limit_price"), _safe_float(order.get("target_price")))
        if limit_price > 0 and price > limit_price:
            self._mark_order_waiting(
                order["id"],
                f"当前价{price:.2f}高于限价{limit_price:.2f}",
                _connection=_connection,
            )
            return {"order_id": order["id"], "status": "waiting", "reason": "above_limit"}

        liquidity_shares = self._liquidity_shares(order, price_info)
        if liquidity_shares < 100:
            self._mark_order_waiting(order["id"], "可模拟流动性不足100股", _connection=_connection)
            return {"order_id": order["id"], "status": "waiting", "reason": "liquidity"}

        fill_price = self._slipped_price("BUY", price)
        shares = int(liquidity_shares / 100) * 100
        amount = round(fill_price * shares, 2)
        if amount > _safe_float(state.get("cash_available_after_buffer")):
            affordable = int(_safe_float(state.get("cash_available_after_buffer")) / fill_price / 100) * 100
            shares = max(0, min(shares, affordable))
            amount = round(fill_price * shares, 2)
        if shares < 100:
            self._mark_order_waiting(order["id"], "现金缓冲后可用资金不足", _connection=_connection)
            return {"order_id": order["id"], "status": "waiting", "reason": "cash"}

        gate_trade_date = effective_trade_date
        current_gate = evaluate_sim_buy_execution_gate(
            order.get("stock_code") or "",
            gate_trade_date,
        )
        if not _execution_gate_allows_expected_buy(
            current_gate,
            order.get("stock_code") or "",
            gate_trade_date,
        ):
            return self._cancel_buy_order_for_execution_gate(
                order,
                current_gate,
                "成交前买入门已阻断：" + str(current_gate.get("reason") or "资格已撤销/数据失效"),
                _connection=_connection,
            )
        bound_hash = str(order.get("execution_gate_hash") or "").strip()
        current_hash = str(current_gate.get("context_hash") or "").strip()
        if not bound_hash:
            return self._cancel_buy_order_for_execution_gate(
                order,
                current_gate,
                "历史买单没有绑定执行门证据，禁止补成交",
                _connection=_connection,
            )
        if current_hash != bound_hash:
            return self._cancel_buy_order_for_execution_gate(
                order,
                current_gate,
                "下单后事实上下文已变化，撤销原买单并按新事实重新决策",
                requeue_if_unfilled=True,
                _connection=_connection,
            )
        self._refresh_order_execution_gate(order["id"], current_gate, _connection=_connection)

        buy_signal = {
            "stock_code": order["stock_code"],
            "short_name": order.get("short_name") or order["stock_code"],
            "strategy_type": order["strategy_type"],
            "trade_mode": order.get("trade_mode") or "live",
            "buy_date": effective_trade_date,
            "buy_time": datetime.now().strftime("%H:%M:%S"),
            "price": fill_price,
            "shares": shares,
            "amount": amount,
            "ai_score": _safe_float(order.get("ai_score")),
            "reason": (order.get("reason") or "") + f"；撮合成交价{fill_price:.2f}",
            "price_source": price_info.get("source", ""),
            "signal_id": order.get("signal_id"),
            "order_id": order["id"],
        }
        if _connection is None:
            raise RuntimeError("BUY matching requires one claimed transaction connection")
        ret = self._execute_buy_in_transaction(
            _connection,
            buy_signal,
            expected_stock_code=str(order.get("stock_code") or "").zfill(6),
            expected_trade_date=effective_trade_date,
            expected_context_hash=bound_hash,
        )
        self._fill_order(
            order["id"],
            fill_price,
            shares,
            ret.get("amount", amount),
            ret.get("fee", 0),
            ret.get("position_id", 0),
            _connection=_connection,
        )
        if order.get("signal_id"):
            final_status = (
                "FILLED"
                if max(0, int(order.get("remaining_shares") or 0) - shares) == 0
                else "PARTIAL"
            )
            _connection_write(_connection, """
                UPDATE st_sim_signal
                SET status = CASE WHEN :order_status = 'FILLED' THEN 'FILLED' ELSE 'ORDERED' END,
                    filled_order_id = :order_id,
                    filled_position_id = :position_id,
                    last_check_reason = :reason,
                    last_check_at = NOW(),
                    updated_at = NOW()
                WHERE id = :signal_id
            """, {
                "order_status": final_status,
                "order_id": order["id"],
                "position_id": ret.get("position_id", 0),
                "reason": f"模拟订单撮合{final_status}",
                "signal_id": order.get("signal_id"),
            })
        self._log_event(
            "ORDER_FILLED",
            trade_mode=order.get("trade_mode") or "live",
            signal_id=order.get("signal_id"),
            order_id=order["id"],
            position_id=ret.get("position_id", 0),
            stock_code=order["stock_code"],
            strategy_type=order["strategy_type"],
            message=f"BUY {shares} @ {fill_price:.2f}",
            payload={**ret, "price_source": price_info.get("source", "")},
            event_date=effective_trade_date,
            _connection=_connection,
        )
        return {**ret, "order_id": order["id"], "status": "filled", "side": "BUY"}

    def _match_sell_order(
        self,
        order: dict,
        price_info: dict,
        *,
        effective_trade_date: str = "",
        _connection=None,
    ) -> dict:
        if _connection is None:
            raise RuntimeError("SELL matching requires one claimed transaction connection")
        effective_trade_date = _normalize_trade_date(
            effective_trade_date or date.today().isoformat(),
            field_name="effective_trade_date",
        )
        position_id = int(order.get("position_id") or 0)
        rows = _connection_rows(_connection, """
            SELECT id, stock_code, short_name, strategy_type, trade_mode,
                   buy_price, buy_shares, buy_date, buy_amount, ai_score, status
            FROM st_sim_position
            WHERE id = :id
            LIMIT 1
            FOR UPDATE
        """, {"id": position_id})
        if not rows:
            self._mark_order_closed(
                order["id"], "CANCELLED", "持仓不存在", _connection=_connection
            )
            return {"order_id": order["id"], "status": "cancelled", "reason": "position_missing"}
        h = rows[0]
        if str(h.get("status") or "").lower() == "sold":
            self._mark_order_closed(
                order["id"], "CANCELLED", "持仓已经卖出", _connection=_connection
            )
            return {"order_id": order["id"], "status": "cancelled", "reason": "position_closed"}
        order_code = str(order.get("stock_code") or "").zfill(6)
        holding_code = str(h.get("stock_code") or "").zfill(6)
        order_mode = str(order.get("trade_mode") or "live")
        holding_mode = str(h.get("trade_mode") or "live")
        if (
            str(h.get("status") or "").lower() != "holding"
            or order_code != holding_code
            or order_mode != holding_mode
            or str(order.get("strategy_type") or "") != str(h.get("strategy_type") or "")
        ):
            self._mark_order_closed(
                order["id"],
                "REJECTED",
                "SELL order identity does not match the locked holding",
                _connection=_connection,
            )
            return {"order_id": order["id"], "status": "rejected", "reason": "holding_identity"}
        price = _safe_float(price_info.get("price"))
        if price <= 0:
            self._mark_order_waiting(order["id"], "未获取到可用实时价格", _connection=_connection)
            return {"order_id": order["id"], "status": "waiting", "reason": "no_price"}
        if _is_near_limit_down(price_info):
            self._mark_order_waiting(order["id"], "接近跌停，卖出订单等待", _connection=_connection)
            return {"order_id": order["id"], "status": "waiting", "reason": "limit_down"}

        buy_date = h.get("buy_date")
        if isinstance(buy_date, str):
            buy_d = datetime.strptime(buy_date[:10], "%Y-%m-%d").date()
        else:
            buy_d = buy_date
        td = datetime.strptime(effective_trade_date, "%Y-%m-%d").date()
        holding_days = (td - buy_d).days if buy_d else 0
        if holding_days < 1:
            self._mark_order_waiting(
                order["id"],
                "A股T+1约束：当日买入不能当日卖出；GTC退出意图继续保留",
                _connection=_connection,
            )
            return {"order_id": order["id"], "status": "waiting", "reason": "t_plus_1"}

        liquidity_shares = self._liquidity_shares(order, price_info)
        shares = min(int(h.get("buy_shares") or 0), int(order.get("remaining_shares") or 0), liquidity_shares)
        shares = int(shares / 100) * 100
        required_shares = int(order.get("remaining_shares") or 0)
        if shares < required_shares or shares <= 0:
            self._mark_order_waiting(
                order["id"],
                "流动性不足以一次性完成本笔卖单，GTC订单等待下一tick",
                _connection=_connection,
            )
            return {"order_id": order["id"], "status": "waiting", "reason": "liquidity"}

        fill_price = self._slipped_price("SELL", price)
        buy_price = _safe_float(h.get("buy_price"))
        sell_fee = portfolio_trade_fee("sell", fill_price, shares)
        buy_fee = portfolio_trade_fee("buy", buy_price, shares)
        total_fee = round(buy_fee + sell_fee, 2)
        profit = round((fill_price - buy_price) * shares - total_fee, 2)
        profit_rate = round(((fill_price - buy_price) / buy_price * 100) if buy_price > 0 else 0, 2)
        sell_signal = {
            "position_id": position_id,
            "stock_code": h["stock_code"],
            "short_name": h.get("short_name") or h["stock_code"],
            "strategy_type": h["strategy_type"],
            "trade_mode": h.get("trade_mode") or order.get("trade_mode") or "live",
            "buy_price": buy_price,
            "sell_price": fill_price,
            "shares": shares,
            "profit_rate": profit_rate,
            "profit": profit,
            "fee": total_fee,
            "holding_days": holding_days,
            "reason": "sim_order_match",
            "reason_detail": (order.get("reason") or "") + f"；撮合成交价{fill_price:.2f}",
            "sell_date": effective_trade_date,
            "sell_time": datetime.now().strftime("%H:%M:%S"),
            "ai_score": _safe_float(h.get("ai_score")),
            "order_id": order["id"],
        }
        ret = self._execute_sell_in_transaction(
            _connection,
            sell_signal,
            expected_stock_code=order_code,
            expected_trade_date=effective_trade_date,
            expected_trade_mode=order_mode,
        )
        self._fill_order(
            order["id"],
            fill_price,
            shares,
            ret.get("amount", round(fill_price * shares, 2)),
            ret.get("fee", total_fee),
            position_id,
            _connection=_connection,
        )
        self._log_event(
            "ORDER_FILLED",
            trade_mode=sell_signal["trade_mode"],
            order_id=order["id"],
            position_id=position_id,
            stock_code=h["stock_code"],
            strategy_type=h["strategy_type"],
            message=f"SELL {shares} @ {fill_price:.2f}",
            payload={**ret, "price_source": price_info.get("source", "")},
            event_date=effective_trade_date,
            _connection=_connection,
        )
        return {**ret, "order_id": order["id"], "status": "filled", "side": "SELL"}

    def _acquire_buy_execution_lock(
        self,
        connection,
        *,
        trade_mode: str,
        trade_date: str,
    ) -> None:
        """Serialize account-wide BUY fills using the existing risk-budget table."""
        connection.execute(text("""
            INSERT INTO st_sim_risk_budget
            (trade_mode, budget_date, strategy_type, initial_capital, total_equity,
             cash_available, max_total_position_amount, max_strategy_amount,
             used_strategy_amount, pending_strategy_amount, available_strategy_amount,
             risk_budget_note, created_at, updated_at)
            VALUES
            (:mode, :trade_date, '__EXECUTION_LOCK__', 0, 0, 0, 0, 0, 0, 0, 0,
             'serializes simulator BUY fills; not an account or position ledger', NOW(), NOW())
            ON DUPLICATE KEY UPDATE
                id = LAST_INSERT_ID(id),
                updated_at = updated_at
        """), {"mode": trade_mode, "trade_date": trade_date})
        locked = _connection_rows(connection, """
            SELECT id
            FROM st_sim_risk_budget
            WHERE trade_mode = :mode
              AND budget_date = :trade_date
              AND strategy_type = '__EXECUTION_LOCK__'
            LIMIT 1
            FOR UPDATE
        """, {"mode": trade_mode, "trade_date": trade_date})
        if len(locked) != 1:
            raise RuntimeError("sim BUY serialization lock is unavailable")

    def _transactional_buy_cash_state(self, connection, trade_mode: str) -> dict:
        """Conservative cash view read after the account-wide BUY lock."""
        rows = _connection_rows(connection, """
            SELECT
                COALESCE(SUM(COALESCE(profit, 0)), 0) AS realized_profit,
                COALESCE(SUM(
                    CASE WHEN status = 'holding' THEN buy_price * buy_shares ELSE 0 END
                ), 0) AS holding_cost
            FROM st_sim_position
            WHERE COALESCE(trade_mode, 'live') = :mode
        """, {"mode": trade_mode})
        position_state = rows[0] if rows else {}
        pending_rows = _connection_rows(connection, """
            SELECT COALESCE(SUM(
                COALESCE(NULLIF(limit_price, 0), target_price, 0)
                * GREATEST(remaining_shares, 0)
            ), 0) AS pending_buy_amount
            FROM st_sim_order
            WHERE COALESCE(trade_mode, 'live') = :mode
              AND side = 'BUY'
              AND status IN ('PENDING', 'PARTIAL', 'MATCHING')
        """, {"mode": trade_mode})
        realized_profit = _safe_float(position_state.get("realized_profit"))
        holding_cost = _safe_float(position_state.get("holding_cost"))
        pending_buy_amount = _safe_float(
            (pending_rows[0] if pending_rows else {}).get("pending_buy_amount")
        )
        total_equity = max(0.0, SIM_INITIAL_CAPITAL + realized_profit)
        cash_available = SIM_INITIAL_CAPITAL + realized_profit - holding_cost - pending_buy_amount
        cash_buffer = total_equity * SIM_RISK_CONFIG["cash_buffer_pct"]
        return {
            "total_equity": round(total_equity, 2),
            "holding_cost": round(holding_cost, 2),
            "pending_buy_amount": round(pending_buy_amount, 2),
            "cash_available_after_buffer": round(max(0.0, cash_available - cash_buffer), 2),
        }

    def _claim_and_match_order(
        self,
        order_id: int,
        *,
        trade_mode: str,
        effective_trade_date: str,
        price_info: dict,
        state: dict,
    ) -> dict:
        """MySQL-5.5-safe compare-and-set claim plus one atomic fill transaction."""
        with get_engine().begin() as connection:
            claim = connection.execute(text("""
                UPDATE st_sim_order
                SET status = 'MATCHING',
                    last_match_reason = 'claimed_for_match',
                    updated_at = NOW()
                WHERE id = :id
                  AND COALESCE(trade_mode, 'live') = :mode
                  AND order_date <= :trade_date
                  AND status IN ('PENDING', 'PARTIAL')
            """), {
                "id": int(order_id),
                "mode": trade_mode,
                "trade_date": effective_trade_date,
            })
            if int(claim.rowcount or 0) != 1:
                return {
                    "order_id": int(order_id),
                    "status": "skipped",
                    "reason": "already_claimed_or_terminal",
                }

            rows = _connection_rows(connection, """
                SELECT o.*, r.ai_score, r.short_score, r.long_score,
                       r.capital_score, r.technical_score, r.fundamental_score,
                       r.event_risk_level
                FROM st_sim_order o
                LEFT JOIN st_sim_signal r ON r.id = o.signal_id
                WHERE o.id = :id
                LIMIT 1
                FOR UPDATE
            """, {"id": int(order_id)})
            if not rows:
                raise RuntimeError(f"claimed sim order disappeared: {order_id}")
            order = rows[0]
            if str(order.get("status") or "") != "MATCHING":
                raise RuntimeError(f"sim order {order_id} lost MATCHING ownership")
            if str(order.get("trade_mode") or "live") != trade_mode:
                raise RuntimeError(f"sim order {order_id} changed trade_mode after claim")

            side = str(order.get("side") or "").upper()
            if side == "SELL":
                return self._match_sell_order(
                    order,
                    price_info,
                    effective_trade_date=effective_trade_date,
                    _connection=connection,
                )
            if side == "BUY":
                self._acquire_buy_execution_lock(
                    connection,
                    trade_mode=trade_mode,
                    trade_date=effective_trade_date,
                )
                # This read happens only after the global BUY row is locked, so
                # a different order cannot size/fill against the same stale cash.
                state = self._transactional_buy_cash_state(connection, trade_mode)
                return self._match_buy_order(
                    order,
                    price_info,
                    state,
                    effective_trade_date=effective_trade_date,
                    _connection=connection,
                )
            self._mark_order_closed(
                order_id,
                "REJECTED",
                f"unknown order side: {side}",
                _connection=connection,
            )
            return {"order_id": order_id, "status": "rejected", "reason": "unknown_side"}

    def match_open_orders(self, trade_date: str = "", trade_mode: str = "live") -> list[dict]:
        _ensure_tables()
        trade_date = _normalize_trade_date(
            trade_date or date.today().isoformat(),
            field_name="trade_date",
        )
        trade_mode = str(trade_mode or "live")
        if trade_mode not in VALID_TRADE_MODES:
            raise ValueError(f"unknown sim trade_mode: {trade_mode}")
        if not _is_trading_time():
            return []
        _require_sim_execution_schema()
        orders = _read_sql("""
            SELECT o.*, r.ai_score, r.short_score, r.long_score, r.capital_score,
                   r.technical_score, r.fundamental_score, r.event_risk_level
            FROM st_sim_order o
            LEFT JOIN st_sim_signal r ON r.id = o.signal_id
            WHERE COALESCE(o.trade_mode, 'live') = :mode
              AND o.order_date <= :trade_date
              AND o.status IN ('PENDING', 'PARTIAL')
            ORDER BY CASE WHEN o.side = 'SELL' THEN 0 ELSE 1 END, o.id ASC
            LIMIT 300
        """, {"mode": trade_mode, "trade_date": trade_date})
        if not orders:
            return []
        price_map = _fetch_live_prices_batch([o["stock_code"] for o in orders])
        state = self.portfolio_state(trade_mode, trade_date, price_map)
        matched = []
        for order in orders:
            code = str(order.get("stock_code") or "").zfill(6)
            price_info = price_map.get(code)
            if not price_info:
                price_info = _get_current_price(code)
            if not price_info:
                self._mark_order_waiting(order["id"], "未获取到行情，等待下一tick")
                matched.append({"order_id": order["id"], "status": "waiting", "reason": "no_quote"})
                continue
            try:
                ret = self._claim_and_match_order(
                    int(order["id"]),
                    trade_mode=trade_mode,
                    effective_trade_date=trade_date,
                    price_info=price_info,
                    state=state,
                )
            except Exception as exc:
                logger.exception("atomic sim order match rolled back for order=%s", order.get("id"))
                ret = {
                    "order_id": order.get("id"),
                    "status": "error",
                    "reason": "transaction_rolled_back",
                    "detail": str(exc),
                }
            if ret.get("status") == "filled":
                state = self.portfolio_state(trade_mode, trade_date, price_map)
            matched.append(ret)
        return matched

    # The former one-phase run_event_tick implementation was removed.
    # All live fills now pass through order claim + transactional matching.

    def run_event_tick(self, trade_date: str = "", *, auto_prepare: bool = True, strict: bool = True) -> dict:
        """Three-stage event-driven simulated trading tick.

        Stage 1 creates/updates signals and trading intents.
        Stage 2 creates simulated orders and matches open orders.
        Stage 3 sizes every new buy order through portfolio/risk budget.
        """
        _ensure_tables()
        trade_date = (trade_date or date.today().isoformat())[:10]
        intraday_window = _intraday_action_window()
        result = {
            "status": "ok",
            "mode": "live",
            "trade_date": trade_date,
            "is_trade_date": _is_trade_date(trade_date),
            "is_trading_time": bool(intraday_window.get("is_trading_time")),
            "is_buy_window": bool(intraday_window.get("is_buy_allowed")),
            "is_preferred_entry_window": bool(intraday_window.get("is_preferred_entry_window")),
            "intraday_action": intraday_window.get("action"),
            "intraday_window": intraday_window,
            "prepare": None,
            "sell_signals": [],
            "sell_orders": [],
            "forward_sell_signals": [],
            "buy_signals": {stype: [] for stype in STRATEGY_CONFIG},
            "buy_orders": [],
            "match_results": [],
            "expired_count": 0,
            "expired_order_count": 0,
            "signal_counts": self.signal_pool_counts(trade_date),
            "order_counts": self.order_counts(trade_date),
            "portfolio_state": self.portfolio_state("live", trade_date),
        }

        if not result["is_trade_date"]:
            result["status"] = "skipped"
            result["reason"] = f"{trade_date} 不是交易日，模拟交易tick跳过"
            return result

        if auto_prepare:
            result["prepare"] = self.prepare_signal_pool(trade_date=trade_date, strict=strict)

        sell_signals = self.check_sell_signals()
        for sig in sell_signals:
            created = self._create_sell_order_from_signal(sig, trade_date)
            if created:
                result["sell_orders"].append({**sig, **created})
            result["sell_signals"].append(sig)

        forward_sell_signals = self.check_forward_sell_signals()
        for sig in forward_sell_signals:
            ret = self.execute_sell(sig)
            result["forward_sell_signals"].append({**sig, **ret})

        if _is_signal_expire_time():
            result["expired_count"] = self._expire_unfilled_signals(trade_date)
            result["expired_order_count"] = self._expire_open_orders(trade_date)
        elif result["is_buy_window"]:
            rows = _read_sql("""
                SELECT *
                FROM st_sim_signal
                WHERE trade_mode = 'live'
                  AND trade_date = :trade_date
                  AND status = 'NEW'
                ORDER BY final_trade_score DESC, ai_score DESC, id ASC
                LIMIT 200
            """, {"trade_date": trade_date})
            live_prices = _fetch_live_prices_batch([r["stock_code"] for r in rows]) if rows else {}
            state = self.portfolio_state("live", trade_date, live_prices)
            self._save_risk_budget_snapshot(state, trade_date)
            holding_count_cache: dict[str, int] = {}
            holding_codes_cache: dict[str, set] = {}

            for row in rows:
                stype = row["strategy_type"]
                cfg = STRATEGY_CONFIG.get(stype)
                if not cfg:
                    self._mark_signal_check(row["id"], "未知策略", "REJECTED")
                    continue

                if stype not in holding_count_cache:
                    holding_count_cache[stype] = self._get_holding_count(stype, trade_mode="live")
                    holding_codes_cache[stype] = self._get_holding_codes(stype, trade_mode="live")
                if _holding_limit_reached(cfg, holding_count_cache[stype]):
                    self._mark_signal_check(row["id"], f"{cfg['name']}策略已满仓", "RISK_BLOCKED")
                    continue
                if row["stock_code"] in holding_codes_cache[stype]:
                    self._mark_signal_check(row["id"], "同策略已持仓，信号不重复成交", "REJECTED")
                    continue

                price_info = live_prices.get(row["stock_code"])
                if not price_info or price_info.get("price", 0) <= 0:
                    price_info = _get_current_price(row["stock_code"])
                if not price_info or price_info.get("price", 0) <= 0:
                    self._mark_signal_check(row["id"], "未获取到可用实时价格")
                    continue
                if price_info.get("source") == "kline_close":
                    self._mark_signal_check(row["id"], "当前无实时价，不能用收盘价自动买入")
                    continue
                if _is_near_limit_up(price_info):
                    self._mark_signal_check(row["id"], "接近涨停，模拟订单等待下一tick")
                    continue

                price = float(price_info["price"])
                entry_low = _safe_float(row.get("entry_price_low"), 0.0)
                entry_high = _safe_float(row.get("entry_price_high"), 0.0)
                if entry_low > 0 and entry_high > 0 and not (entry_low <= price <= entry_high):
                    self._mark_signal_check(row["id"], f"当前价{price:.2f}不在买入区间{entry_low:.2f}~{entry_high:.2f}")
                    continue

                budget = self._risk_budget_for_signal(row, price, state)
                if not budget.get("allowed"):
                    self._mark_signal_check(row["id"], budget.get("reason", "组合风险预算不足"), "RISK_BLOCKED")
                    self._log_event(
                        "RISK_BLOCKED",
                        signal_id=row["id"],
                        stock_code=row["stock_code"],
                        strategy_type=stype,
                        severity="WARN",
                        message=budget.get("reason", "组合风险预算不足"),
                        payload=budget,
                        event_date=trade_date,
                    )
                    continue

                execution_gate = evaluate_sim_buy_execution_gate(
                    row["stock_code"],
                    trade_date,
                )
                if not _execution_gate_allows_buy(execution_gate):
                    gate_reason = "下单前买入门阻断：" + str(
                        execution_gate.get("reason") or "资格或事实证据不可用"
                    )
                    self._mark_signal_check(row["id"], gate_reason, "EXECUTION_BLOCKED")
                    self._log_event(
                        "BUY_EXECUTION_GATE_BLOCKED",
                        signal_id=row["id"],
                        stock_code=row["stock_code"],
                        strategy_type=stype,
                        severity="WARN",
                        message=gate_reason,
                        payload=execution_gate,
                        event_date=trade_date,
                    )
                    continue

                order_reason = (
                    f"信号日{str(row['signal_date'])[:10]}，交易日{trade_date}；"
                    f"事件驱动生成模拟买单；价格来源{price_info.get('source', '')}；"
                    f"风险预算：{budget.get('note', '')}；{row.get('reason') or ''}"
                )
                try:
                    order_id = self._create_order(
                        row,
                        price,
                        int(budget["shares"]),
                        order_reason,
                        side="BUY",
                        trade_mode="live",
                        order_type="SIM_LIMIT",
                        risk_budget=budget,
                        price_source=price_info.get("source", ""),
                        execution_gate=execution_gate,
                    )
                    self._mark_signal_ordered(
                        row["id"],
                        order_id,
                        int(budget["shares"]),
                        budget["amount"],
                        budget,
                        "已生成模拟买入订单，等待撮合",
                        execution_gate,
                    )
                    self._log_event(
                        "BUY_ORDER_CREATED",
                        signal_id=row["id"],
                        order_id=order_id,
                        stock_code=row["stock_code"],
                        strategy_type=stype,
                        message=f"BUY order {budget['shares']} shares @ target {price:.2f}",
                        payload={"budget": budget, "price_info": price_info},
                        event_date=trade_date,
                    )
                    result["buy_orders"].append({
                        "order_id": order_id,
                        "signal_id": row["id"],
                        "stock_code": row["stock_code"],
                        "short_name": row.get("short_name") or row["stock_code"],
                        "strategy_type": stype,
                        "price": price,
                        "shares": int(budget["shares"]),
                        "amount": budget["amount"],
                        "risk_budget": budget,
                    })
                    state = self.portfolio_state("live", trade_date, live_prices)
                except Exception as exc:
                    self._mark_signal_check(row["id"], f"模拟订单创建失败: {exc}")

        result["match_results"] = self.match_open_orders(trade_date, "live")
        result["signal_counts"] = self.signal_pool_counts(trade_date)
        result["order_counts"] = self.order_counts(trade_date)
        result["portfolio_state"] = self.portfolio_state("live", trade_date)
        self._save_risk_budget_snapshot(result["portfolio_state"], trade_date)
        return result

    def check_buy_signals(self, strategy_type: str) -> list[dict]:
        """
        检查指定策略的买入信号。
        1. 从 st_recommended_stocks 获取最新推荐(评分>70)
        2. 从 stock_analysis_result 获取详细评分
        3. 按策略条件过滤
        4. 排除已持仓的股票
        5. 同一策略不重复持有同一只股票；数量不设硬上限
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
        if _holding_limit_reached(cfg, holding_count):
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
                "slots_left": None,
            })

            # 检查是否达到持仓上限
            if _holding_limit_reached(cfg, holding_count + len(signals)):
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
        scan_cutoff = _market_aware_datetime()
        scan_trade_date = scan_cutoff.date().isoformat()
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
            max_rate = None
            if cfg.get("trailing_activate") is not None:
                max_rate = self._get_max_profit_rate(h["id"], code, buy_price)
            holding_assessment = evaluate_sim_holding_exit_gate(
                code,
                scan_trade_date,
                knowledge_cutoff=scan_cutoff,
            )
            decision = build_sell_decision(
                h["strategy_type"],
                buy_price,
                current_price,
                h["buy_date"],
                max_profit_rate=max_rate,
                near_limit_down=_is_near_limit_down(price_info),
                holding_assessment=holding_assessment,
            )

            if decision["should_sell"] or decision.get("exit_intent"):
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
                    "profit_rate": decision["profit_rate"],
                    "profit": net_profit,
                    "fee": total_fee,
                    "holding_days": decision["holding_days"],
                    "reason": decision["reason"],
                    "reason_detail": decision["reason_detail"],
                    "reason_label": self._reason_label(decision["reason"]),
                    "ai_score": float(h.get("ai_score") or 0),
                    "execution_status": (
                        "READY" if decision["should_sell"] else "WAIT_EXECUTION"
                    ),
                    "pending_exit_reason": decision.get("pending_exit_reason"),
                    "holding_assessment": holding_assessment,
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
        except Exception as exc:
            logger.debug("max gain fallback lookup failed for position=%s stock=%s: %s", position_id, stock_code, exc)
        return None

    @staticmethod
    def _reason_label(reason: str) -> str:
        labels = {
            "take_profit": "止盈",
            "stop_loss": "止损",
            "time_limit": "时间止损",
            "trailing_stop": "动态止盈",
            "dynamic_sell": "动态风险卖出",
            "dynamic_reduce": "动态风险减仓",
            "t_plus_one": "T+1待执行",
            "near_limit_down": "跌停流动性待执行",
        }
        return labels.get(reason, reason)

    # ────────────────────────────────────────
    # 交易执行
    # ────────────────────────────────────────

    def _execute_buy_in_transaction(
        self,
        connection,
        signal: dict,
        *,
        expected_stock_code: str,
        expected_trade_date: str,
        expected_context_hash: str = "",
    ) -> dict:
        """Final BUY boundary. Never accepts a caller-provided gate as authority."""
        now = datetime.now()
        cutoff = _market_aware_datetime(now)
        trade_mode = str(signal.get("trade_mode") or "live")
        if trade_mode not in VALID_TRADE_MODES:
            raise ValueError(f"不允许写入未知模拟交易模式: {trade_mode}")

        expected_code = str(expected_stock_code or "").zfill(6)
        signal_code = str(signal.get("stock_code") or "").zfill(6)
        expected_date = _normalize_trade_date(expected_trade_date, field_name="expected_trade_date")
        signal_date = _normalize_trade_date(
            signal.get("buy_date") or expected_date,
            field_name="buy_date",
        )
        if signal_code != expected_code or signal_date != expected_date:
            raise ValueError("BUY identity/date differs from the claimed order")

        if trade_mode in {"live", "forward"}:
            final_gate = evaluate_sim_buy_execution_gate(
                expected_code,
                expected_date,
                knowledge_cutoff=cutoff,
            )
            if not _execution_gate_allows_expected_buy(
                final_gate,
                expected_code,
                expected_date,
                now=cutoff,
            ):
                raise ValueError(
                    "execution-time gate blocked final BUY: "
                    + str(final_gate.get("reason") or "fresh expected-identity evidence unavailable")
                )
            final_context_hash = str(final_gate.get("context_hash") or "").strip().lower()
            if (
                not str(expected_context_hash or "").strip()
                or final_context_hash
                != str(expected_context_hash or "").strip().lower()
            ):
                raise ValueError(
                    "execution-time gate context changed at final BUY boundary"
                )

        order_id = int(signal.get("order_id") or 0)
        if order_id:
            existing = _connection_rows(connection, """
                SELECT p.id AS position_id, f.id AS flow_id, f.price, f.shares, f.amount, f.fee
                FROM st_sim_position p
                JOIN st_trade_flow f
                  ON f.order_id = p.entry_order_id AND f.trans_type = 'buy'
                WHERE p.entry_order_id = :order_id
                  AND p.stock_code = :code
                  AND p.trade_mode = :mode
                LIMIT 1
                FOR UPDATE
            """, {"order_id": order_id, "code": expected_code, "mode": trade_mode})
            if existing:
                row = existing[0]
                return {
                    "status": "idempotent",
                    "stock_code": expected_code,
                    "price": _safe_float(row.get("price")),
                    "shares": int(row.get("shares") or 0),
                    "amount": _safe_float(row.get("amount")),
                    "fee": _safe_float(row.get("fee")),
                    "position_id": int(row.get("position_id") or 0),
                    "flow_id": int(row.get("flow_id") or 0),
                }

        price = _safe_float(signal.get("price"))
        shares = int(signal.get("shares") or 0)
        if price <= 0 or shares <= 0 or shares % 100 != 0:
            raise ValueError("BUY price/shares must be positive and shares must be a board lot")
        amount = round(price * shares, 2)
        fee = round(portfolio_trade_fee("buy", price, shares), 2)
        buy_time = signal.get("buy_time") or now.strftime("%H:%M:%S")

        position_id = _connection_insert_get_id(connection, """
            INSERT INTO st_sim_position
            (signal_id, entry_order_id, stock_code, short_name, strategy_type, trade_mode,
             buy_price, buy_amount, buy_shares, buy_date, buy_time, buy_reason,
             ai_score, short_score, long_score, capital_score, technical_score,
             fundamental_score, event_risk_level, status)
            VALUES (:signal_id, :order_id, :code, :name, :st, :mode,
                    :price, :amount, :shares, :date, :time, :reason,
                    :ai, :ss, :ls, :cs, :ts, :fs, :risk, 'holding')
        """, {
            "signal_id": signal.get("signal_id"),
            "order_id": order_id or None,
            "code": expected_code,
            "name": signal.get("short_name", ""),
            "st": signal["strategy_type"],
            "mode": trade_mode,
            "price": price,
            "amount": amount,
            "shares": shares,
            "date": expected_date,
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
        flow_id = _connection_insert_get_id(connection, """
            INSERT INTO st_trade_flow
            (order_id, stock_code, short_name, flow_type, source, strategy_type,
             trade_mode, trans_type, price, shares, amount, fee, reason, ai_score,
             trans_date, trans_time)
            VALUES (:order_id, :code, :name, 'sim_buy', 'simulation', :st,
                    :mode, 'buy', :price, :shares, :amount, :fee, :reason, :ai,
                    :date, :time)
        """, {
            "order_id": order_id or None,
            "code": expected_code,
            "name": signal.get("short_name", ""),
            "st": signal["strategy_type"],
            "mode": trade_mode,
            "price": price,
            "shares": shares,
            "amount": amount,
            "fee": fee,
            "reason": signal.get("reason", ""),
            "ai": signal.get("ai_score", 0),
            "date": expected_date,
            "time": buy_time,
        })
        return {
            "status": "ok",
            "stock_code": expected_code,
            "price": price,
            "shares": shares,
            "amount": amount,
            "fee": fee,
            "position_id": position_id,
            "flow_id": flow_id,
        }

    def execute_buy(self, signal: dict) -> dict:
        """Execute a BUY atomically; live/forward authorization is recomputed inside."""
        _ensure_tables()
        trade_mode = str(signal.get("trade_mode") or "live")
        expected_code = str(signal.get("stock_code") or "").zfill(6)
        expected_date = _normalize_trade_date(
            signal.get("buy_date") or date.today().isoformat(),
            field_name="buy_date",
        )
        # Preserve fail-closed gate behaviour even if schema bootstrap failed.
        if trade_mode in {"live", "forward"}:
            probe_cutoff = _market_aware_datetime()
            probe = evaluate_sim_buy_execution_gate(
                expected_code,
                expected_date,
                knowledge_cutoff=probe_cutoff,
            )
            if not _execution_gate_allows_expected_buy(
                probe,
                expected_code,
                expected_date,
                now=probe_cutoff,
            ):
                raise ValueError(
                    "execution-time gate blocked BUY: "
                    + str(probe.get("reason") or "fresh expected-identity evidence unavailable")
                )
            expected_context_hash = str(probe.get("context_hash") or "")
        else:
            expected_context_hash = ""
        _require_sim_execution_schema()
        with get_engine().begin() as connection:
            return self._execute_buy_in_transaction(
                connection,
                signal,
                expected_stock_code=expected_code,
                expected_trade_date=expected_date,
                expected_context_hash=expected_context_hash,
            )

    def _execute_sell_in_transaction(
        self,
        connection,
        sell_signal: dict,
        *,
        expected_stock_code: str,
        expected_trade_date: str,
        expected_trade_mode: str,
    ) -> dict:
        """Lock and validate the canonical holding before any SELL mutation."""
        now = datetime.now()
        trade_mode = str(expected_trade_mode or "")
        if trade_mode not in VALID_TRADE_MODES:
            raise ValueError(f"不允许写入未知模拟交易模式: {trade_mode}")
        signal_mode = str(sell_signal.get("trade_mode") or "live")
        expected_code = str(expected_stock_code or "").zfill(6)
        signal_code = str(sell_signal.get("stock_code") or "").zfill(6)
        effective_date = _normalize_trade_date(
            expected_trade_date,
            field_name="effective_trade_date",
        )
        signal_date = _normalize_trade_date(
            sell_signal.get("sell_date") or effective_date,
            field_name="sell_date",
        )
        if signal_mode != trade_mode or signal_code != expected_code or signal_date != effective_date:
            raise ValueError("SELL identity/mode/date differs from the claimed order")

        position_id = int(sell_signal.get("position_id") or 0)
        if position_id <= 0:
            raise ValueError("SELL position_id must be positive")
        rows = _connection_rows(connection, """
            SELECT id, stock_code, short_name, strategy_type, trade_mode, buy_price,
                   buy_amount, buy_shares, buy_date, status, profit, profit_rate,
                   fee_total, exit_order_id, ai_score
            FROM st_sim_position
            WHERE id = :id
            LIMIT 1
            FOR UPDATE
        """, {"id": position_id})
        if not rows:
            raise ValueError(f"SELL position does not exist: {position_id}")
        holding = rows[0]
        holding_code = str(holding.get("stock_code") or "").zfill(6)
        holding_mode = str(holding.get("trade_mode") or "live")
        holding_strategy = str(holding.get("strategy_type") or "")
        signal_strategy = str(sell_signal.get("strategy_type") or "")
        if holding_code != expected_code or holding_mode != trade_mode:
            raise ValueError("SELL stock/mode does not match the locked holding")
        if signal_strategy and signal_strategy != holding_strategy:
            raise ValueError("SELL strategy does not match the locked holding")

        order_id = int(sell_signal.get("order_id") or 0)
        if order_id:
            prior_flows = _connection_rows(connection, """
                SELECT id, stock_code, trade_mode, price, shares, amount, fee
                FROM st_trade_flow
                WHERE order_id = :order_id AND trans_type = 'sell'
                LIMIT 1
                FOR UPDATE
            """, {"order_id": order_id})
            if prior_flows:
                prior = prior_flows[0]
                if (
                    str(prior.get("stock_code") or "").zfill(6) != expected_code
                    or str(prior.get("trade_mode") or "live") != trade_mode
                ):
                    raise ValueError("SELL order_id is already bound to another holding identity")
                return {
                    "status": "idempotent",
                    "stock_code": expected_code,
                    "position_id": position_id,
                    "flow_id": int(prior.get("id") or 0),
                    "price": _safe_float(prior.get("price")),
                    "shares": int(prior.get("shares") or 0),
                    "amount": _safe_float(prior.get("amount")),
                    "fee": _safe_float(prior.get("fee")),
                }

        holding_status = str(holding.get("status") or "").lower()
        if holding_status == "sold":
            return {
                "status": "idempotent",
                "stock_code": expected_code,
                "position_id": position_id,
                "profit": _safe_float(holding.get("profit")),
                "profit_rate": _safe_float(holding.get("profit_rate")),
                "reason": sell_signal.get("reason") or "already_sold",
            }
        if holding_status != "holding":
            raise ValueError(f"SELL holding status is not executable: {holding_status}")

        holding_shares = int(holding.get("buy_shares") or 0)
        shares = int(sell_signal.get("shares") or 0)
        if shares <= 0 or shares > holding_shares:
            raise ValueError(
                f"SELL shares exceed locked holding: requested={shares}, holding={holding_shares}"
            )
        if trade_mode in {"live", "forward"} and shares < holding_shares and not order_id:
            raise ValueError(
                "partial live/forward SELL requires a stable order_id for idempotency"
            )
        buy_date = _normalize_trade_date(holding.get("buy_date"), field_name="position.buy_date")
        if trade_mode in {"live", "forward"} and effective_date <= buy_date:
            raise ValueError(
                f"SELL violates T+1: buy_date={buy_date}, effective_trade_date={effective_date}"
            )

        price = _safe_float(sell_signal.get("sell_price"))
        buy_price = _safe_float(holding.get("buy_price"))
        if price <= 0 or buy_price <= 0:
            raise ValueError("SELL price and locked buy_price must be positive")
        amount = round(price * shares, 2)
        sell_fee = portfolio_trade_fee("sell", price, shares)
        allocated_buy_fee = portfolio_trade_fee("buy", buy_price, shares)
        fee = round(sell_fee + allocated_buy_fee, 2)
        profit = round((price - buy_price) * shares - fee, 2)
        profit_rate = round(((price - buy_price) / buy_price * 100), 2)
        holding_days = (
            datetime.strptime(effective_date, "%Y-%m-%d").date()
            - datetime.strptime(buy_date, "%Y-%m-%d").date()
        ).days
        remaining_shares = holding_shares - shares
        reason = str(sell_signal.get("reason") or "risk_sell")
        reason_detail = sell_signal.get("reason_detail") or self._reason_label(reason)
        sell_time = sell_signal.get("sell_time") or now.strftime("%H:%M:%S")

        result = connection.execute(text("""
            UPDATE st_sim_position
            SET status = CASE WHEN :remaining_shares = 0 THEN 'sold' ELSE 'holding' END,
                buy_shares = CASE WHEN :remaining_shares > 0 THEN :remaining_shares ELSE buy_shares END,
                buy_amount = CASE
                    WHEN :remaining_shares > 0 THEN ROUND(buy_price * :remaining_shares, 2)
                    ELSE buy_amount
                END,
                sell_price = :price,
                sell_date = :date,
                sell_time = :time,
                sell_reason = :reason,
                profit = COALESCE(profit, 0) + :profit,
                profit_rate = :rate,
                holding_days = :days,
                fee_total = COALESCE(fee_total, 0) + :fee,
                exit_order_id = COALESCE(:order_id, exit_order_id),
                updated_at = NOW()
            WHERE id = :id
              AND status = 'holding'
              AND stock_code = :code
              AND trade_mode = :mode
              AND buy_shares = :locked_shares
        """), {
            "remaining_shares": remaining_shares,
            "price": price,
            "date": effective_date,
            "time": sell_time,
            "reason": reason_detail,
            "profit": profit,
            "rate": profit_rate,
            "days": holding_days,
            "fee": fee,
            "order_id": order_id or None,
            "id": position_id,
            "code": expected_code,
            "mode": trade_mode,
            "locked_shares": holding_shares,
        })
        if int(result.rowcount or 0) != 1:
            raise RuntimeError("locked SELL holding changed before mutation")

        flow_id = _connection_insert_get_id(connection, """
            INSERT INTO st_trade_flow
            (order_id, stock_code, short_name, flow_type, source, strategy_type,
             trade_mode, trans_type, price, shares, amount, fee, reason, ai_score,
             trans_date, trans_time)
            VALUES (:order_id, :code, :name, 'sim_sell', 'simulation', :st,
                    :mode, 'sell', :price, :shares, :amount, :fee, :reason, :ai,
                    :date, :time)
        """, {
            "order_id": order_id or None,
            "code": expected_code,
            "name": holding.get("short_name") or sell_signal.get("short_name", ""),
            "st": holding_strategy,
            "mode": trade_mode,
            "price": price,
            "shares": shares,
            "amount": amount,
            "fee": fee,
            "reason": reason_detail,
            "ai": holding.get("ai_score") or sell_signal.get("ai_score", 0),
            "date": effective_date,
            "time": sell_time,
        })
        return {
            "status": "ok",
            "stock_code": expected_code,
            "position_id": position_id,
            "flow_id": flow_id,
            "price": price,
            "shares": shares,
            "remaining_shares": remaining_shares,
            "amount": amount,
            "fee": fee,
            "profit": profit,
            "profit_rate": profit_rate,
            "holding_days": holding_days,
            "reason": reason,
        }

    def execute_sell(self, sell_signal: dict) -> dict:
        """Execute SELL atomically after locking and validating the holding."""
        _ensure_tables()
        trade_mode = str(sell_signal.get("trade_mode") or "live")
        expected_code = str(sell_signal.get("stock_code") or "").zfill(6)
        expected_date = _normalize_trade_date(
            sell_signal.get("sell_date") or date.today().isoformat(),
            field_name="sell_date",
        )
        _require_sim_execution_schema()
        with get_engine().begin() as connection:
            return self._execute_sell_in_transaction(
                connection,
                sell_signal,
                expected_stock_code=expected_code,
                expected_trade_date=expected_date,
                expected_trade_mode=trade_mode,
            )

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
        if _holding_limit_reached(cfg, holding_count):
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
        execution_gate = evaluate_sim_buy_execution_gate(code, trade_date)
        if not _execution_gate_allows_buy(execution_gate):
            return {
                "status": "skip",
                "reason": "execution_gate_blocked",
                "detail": execution_gate.get("reason"),
                "stock_code": code,
                "trade_date": trade_date,
            }
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
        if _holding_limit_reached(cfg, holding_count):
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
