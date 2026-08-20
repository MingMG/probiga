# -*- coding: utf-8 -*-
"""Point-in-time strategy projection for holdings recorded in the watchlist."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine


_STOCK_CODE_RE = re.compile(r"^\d{6}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SHANGHAI_TZ = timezone(timedelta(hours=8))


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _range(low: Any, high: Any) -> dict[str, float] | None:
    left = _number(low)
    right = _number(high)
    if left is None and right is None:
        return None
    if left is None:
        left = right
    if right is None:
        right = left
    assert left is not None and right is not None
    return {"low": round(min(left, right), 4), "high": round(max(left, right), 4)}


def _first_text(*values: Any) -> str:
    for value in values:
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    return ""


def _cutoff_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        normalized = str(value or "").strip().replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(_SHANGHAI_TZ).replace(tzinfo=None)
    return parsed


def _table_columns(engine: Engine, table_name: str) -> set[str]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT COLUMN_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name
                """
            ),
            {"table_name": table_name},
        ).fetchall()
    return {str(row[0]) for row in rows}


def _acquisition_clause(columns: set[str]) -> tuple[str, str]:
    timestamps = [
        column
        for column in ("updated_at", "created_at", "etl_sync_at", "received_at")
        if column in columns
    ]
    if not timestamps:
        return "", ""
    coalesced = ", ".join(
        f"COALESCE(`{column}`, '1000-01-01 00:00:00')" for column in timestamps
    )
    any_timestamp = " OR ".join(f"`{column}` IS NOT NULL" for column in timestamps)
    effective = f"GREATEST({coalesced})" if len(timestamps) > 1 else coalesced
    return f"({any_timestamp}) AND {effective} <= :knowledge_cutoff", effective


def _latest_pit_row(
    engine: Engine,
    *,
    table_name: str,
    date_column: str,
    stock_code: str,
    trade_date: str,
    cutoff: datetime,
    fields: tuple[str, ...],
) -> tuple[dict[str, Any], str]:
    try:
        columns = _table_columns(engine, table_name)
        if not {"stock_code", date_column}.issubset(columns):
            return {}, f"{table_name} is missing required fields"
        cutoff_clause, acquisition_order = _acquisition_clause(columns)
        if not cutoff_clause:
            return {}, f"{table_name} has no acquisition timestamp"
        selected = [
            column
            for column in dict.fromkeys(("stock_code", date_column, *fields))
            if column in columns
        ]
        select_sql = ", ".join(f"`{column}`" for column in selected)
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    f"""
                    SELECT {select_sql}
                    FROM `{table_name}`
                    WHERE stock_code = :stock_code
                      AND `{date_column}` <= :trade_date
                      AND {cutoff_clause}
                    ORDER BY `{date_column}` DESC, {acquisition_order} DESC
                    LIMIT 1
                    """
                ),
                {
                    "stock_code": stock_code,
                    "trade_date": trade_date,
                    "knowledge_cutoff": cutoff,
                },
            ).mappings().first()
    except Exception as error:
        return {}, str(error)
    if row is None:
        return {}, f"no cutoff-eligible {table_name} row"
    return dict(row), ""


def _daily_price_context(
    engine: Engine,
    *,
    stock_code: str,
    trade_date: str,
    cutoff: datetime,
    current_price: float | None,
) -> tuple[dict[str, Any], str]:
    try:
        columns = _table_columns(engine, "sm_stock_kline")
        required = {"stock_code", "trade_date", "close"}
        if not required.issubset(columns):
            return {}, "sm_stock_kline is missing required fields"
        cutoff_clause, acquisition_order = _acquisition_clause(columns)
        if not cutoff_clause:
            return {}, "sm_stock_kline has no acquisition timestamp"
        filters = [
            "stock_code = :stock_code",
            "trade_date <= :trade_date",
            cutoff_clause,
        ]
        if "k_type" in columns:
            filters.append("k_type = 1")
        if "adjust_type" in columns:
            filters.append("adjust_type = 0")
        with engine.connect() as connection:
            rows = connection.execute(
                text(
                    f"""
                    SELECT trade_date, close
                    FROM sm_stock_kline
                    WHERE {' AND '.join(filters)}
                    ORDER BY trade_date DESC, {acquisition_order} DESC
                    LIMIT 40
                    """
                ),
                {
                    "stock_code": stock_code,
                    "trade_date": trade_date,
                    "knowledge_cutoff": cutoff,
                },
            ).mappings().all()
    except Exception as error:
        return {}, str(error)

    closes_by_date: dict[str, float] = {}
    for row in rows:
        key = str(row.get("trade_date") or "")[:10]
        close = _number(row.get("close"))
        if key and close is not None and key not in closes_by_date:
            closes_by_date[key] = close
    ordered = [closes_by_date[key] for key in sorted(closes_by_date)]
    daily_price = ordered[-1] if ordered else None
    ma20 = sum(ordered[-20:]) / 20.0 if len(ordered) >= 20 else None
    live_price = _number(current_price)
    latest = live_price or daily_price
    return {
        "latest_price": round(latest, 6) if latest is not None else None,
        "price_source": "portfolio_live_snapshot" if live_price is not None else "daily_close",
        "ma20": round(ma20, 6) if ma20 is not None else None,
        "daily_session_count": len(ordered),
    }, ""


def evaluate_watchlist_holding_exit_at_cutoff(
    engine: Engine,
    stock_code: str,
    trade_date: str,
    knowledge_cutoff: str | datetime,
    *,
    current_price: float | None = None,
) -> dict[str, Any]:
    """Evaluate one holding using only records acquired by ``knowledge_cutoff``."""
    code = str(stock_code or "").strip().zfill(6)
    target_date = str(trade_date or "")[:10]
    if not _STOCK_CODE_RE.fullmatch(code):
        raise ValueError("stock_code must be a six-digit A-share code")
    if not _DATE_RE.fullmatch(target_date):
        raise ValueError("trade_date must be YYYY-MM-DD")
    cutoff = _cutoff_datetime(knowledge_cutoff)
    if cutoff.date() < date.fromisoformat(target_date):
        raise ValueError("knowledge_cutoff cannot precede trade_date")

    recommendation, recommendation_error = _latest_pit_row(
        engine,
        table_name="st_recommended_stocks",
        date_column="pick_date",
        stock_code=code,
        trade_date=target_date,
        cutoff=cutoff,
        fields=(
            "event_risk_level",
            "signal_status",
            "signal_reason",
            "main_wave_signal",
            "main_wave_reason",
            "entry_price_low",
            "entry_price_high",
            "stop_loss_price",
            "take_profit_1",
            "take_profit_2",
            "resistance_price",
            "trend_stop_price",
            "trend_reduce_price",
            "sell_rules_json",
            "invalidation_reason",
            "recommend_status",
            "model_version",
        ),
    )
    analysis, analysis_error = _latest_pit_row(
        engine,
        table_name="stock_analysis_result",
        date_column="analysis_date",
        stock_code=code,
        trade_date=target_date,
        cutoff=cutoff,
        fields=(
            "event_risk_level",
            "event_risk_detail",
            "recommend_status",
            "recommend_reason",
            "model_version",
        ),
    )
    price, price_error = _daily_price_context(
        engine,
        stock_code=code,
        trade_date=target_date,
        cutoff=cutoff,
        current_price=current_price,
    )

    signals = {
        str(recommendation.get("signal_status") or "").upper(),
        str(recommendation.get("main_wave_signal") or "").upper(),
    }
    risks = {
        str(recommendation.get("event_risk_level") or "").upper(),
        str(analysis.get("event_risk_level") or "").upper(),
    }
    latest_price = _number(price.get("latest_price")) or 0.0
    stop_loss = _number(recommendation.get("stop_loss_price")) or 0.0
    trend_stop = _number(recommendation.get("trend_stop_price")) or 0.0
    trend_reduce = _number(recommendation.get("trend_reduce_price")) or 0.0
    ma20 = _number(price.get("ma20")) or 0.0
    computed_trend_stop = ma20 * 0.97 if ma20 > 0 else 0.0

    intent = "HOLD"
    reason = "cutoff-visible analysis and price do not require an exit"
    explicit = False
    if "SELL_ALERT" in signals:
        intent, reason, explicit = "SELL", "persisted strategy signal is SELL_ALERT", True
    elif "CRITICAL" in risks:
        intent, reason, explicit = "SELL", "cutoff-visible critical event risk requires exit", True
    elif latest_price > 0 and stop_loss > 0 and latest_price <= stop_loss:
        intent, reason, explicit = "SELL", f"latest price {latest_price:.4f} breached stop loss {stop_loss:.4f}", True
    elif latest_price > 0 and trend_stop > 0 and latest_price <= trend_stop:
        intent, reason, explicit = "SELL", f"latest price {latest_price:.4f} breached trend stop {trend_stop:.4f}", True
    elif latest_price > 0 and computed_trend_stop > 0 and latest_price <= computed_trend_stop:
        intent, reason, explicit = "SELL", f"latest price {latest_price:.4f} invalidated MA20 trend stop {computed_trend_stop:.4f}", True
    elif "REDUCE" in signals:
        intent, reason, explicit = "REDUCE", "persisted strategy signal is REDUCE", True
    elif "HIGH" in risks:
        intent, reason, explicit = "REDUCE", "cutoff-visible high event risk requires reduction", True
    elif latest_price > 0 and trend_reduce > 0 and latest_price <= trend_reduce:
        intent, reason, explicit = "REDUCE", f"latest price {latest_price:.4f} breached reduction line {trend_reduce:.4f}", True

    if not explicit:
        if not recommendation:
            intent, reason = "WAIT_DATA", recommendation_error or "no cutoff-eligible recommendation row"
        elif not analysis:
            intent, reason = "WAIT_DATA", analysis_error or "no cutoff-eligible analysis row"
        elif latest_price <= 0:
            intent, reason = "WAIT_DATA", price_error or "no cutoff-eligible holding price"

    evidence = {
        "analysis": analysis,
        "analysis_error": analysis_error or None,
        "recommendation": recommendation,
        "recommendation_error": recommendation_error or None,
        "price": price,
        "price_error": price_error or None,
        "thresholds": {
            "stop_loss_price": stop_loss or None,
            "trend_stop_price": trend_stop or None,
            "trend_reduce_price": trend_reduce or None,
            "computed_ma20_trend_stop": computed_trend_stop or None,
        },
    }
    hash_payload = json.dumps(
        {"stock_code": code, "trade_date": target_date, "intent": intent, "evidence": evidence},
        ensure_ascii=False,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )
    evidence_hash = hashlib.sha256(hash_payload.encode("utf-8")).hexdigest()
    evaluated_at = cutoff.isoformat()
    return {
        "stock_code": code,
        "trade_date": target_date,
        "knowledge_cutoff": evaluated_at,
        "exit_intent": intent,
        "reason": reason,
        "evidence": evidence,
        "evaluated_at": evaluated_at,
        "valid_until": (cutoff + timedelta(seconds=60)).isoformat(),
        "context_hash": evidence_hash,
        "evidence_hash": evidence_hash,
    }


def build_watchlist_holding_strategy(
    portfolio_row: dict[str, Any],
    exit_decision: dict[str, Any],
) -> dict[str, Any]:
    """Turn an exit decision into one concise, advisory-only action row."""
    evidence = dict(exit_decision.get("evidence") or {})
    recommendation = dict(evidence.get("recommendation") or {})
    thresholds = dict(evidence.get("thresholds") or {})
    price_evidence = dict(evidence.get("price") or {})
    intent = str(exit_decision.get("exit_intent") or "WAIT_DATA").upper()
    if intent not in {"SELL", "REDUCE", "HOLD", "WAIT_DATA"}:
        intent = "WAIT_DATA"

    latest_price = _number(
        price_evidence.get("latest_price")
        or portfolio_row.get("cur_price")
        or portfolio_row.get("price")
    )
    cost_price = _number(portfolio_row.get("cost_price"))
    shares = max(0, int(portfolio_row.get("shares") or 0))
    sellable_shares = max(0, int(portfolio_row.get("sellable_shares") or shares))
    position_date = str(portfolio_row.get("position_date") or "")[:10]
    trade_date = str(exit_decision.get("trade_date") or "")[:10]
    t1_blocked = bool(position_date and trade_date and position_date == trade_date)
    entry_range = _range(recommendation.get("entry_price_low"), recommendation.get("entry_price_high"))
    take_profit_range = _range(
        recommendation.get("take_profit_1") or recommendation.get("trend_reduce_price"),
        recommendation.get("take_profit_2")
        or recommendation.get("resistance_price")
        or recommendation.get("take_profit_1")
        or recommendation.get("trend_reduce_price"),
    )
    protective_values = [
        _number(thresholds.get("stop_loss_price")),
        _number(thresholds.get("trend_stop_price")),
        _number(thresholds.get("computed_ma20_trend_stop")),
    ]
    protective_values = [value for value in protective_values if value is not None]
    emergency_exit_price = round(max(protective_values), 4) if protective_values else None
    signal_status = str(recommendation.get("signal_status") or "").upper()
    main_wave_signal = str(recommendation.get("main_wave_signal") or "").upper()
    decision_reason = _first_text(
        recommendation.get("signal_reason"),
        recommendation.get("main_wave_reason"),
        exit_decision.get("reason"),
    )

    if intent == "SELL" and t1_blocked:
        action, priority, urgency = "明日优先卖出（T+1）", 0, "NEXT_SESSION"
        sell_plan = {"mode": "EXIT_PENDING_T1", "range": None, "label": "今日买入不可卖；下一交易日开盘优先退出"}
        next_plan = "下一交易日开盘优先退出，不再等待反弹。"
    elif intent == "SELL":
        action, priority, urgency = "立即卖出", 0, "IMMEDIATE"
        sell_plan = {"mode": "IMMEDIATE", "range": _range(latest_price, latest_price), "label": "按当前可成交价尽快退出，不等待目标区间"}
        next_plan = "若今天未完成卖出，下一交易日优先退出，不再等待反弹。"
    elif intent == "REDUCE" and t1_blocked:
        action, priority, urgency = "明日优先减仓（T+1）", 1, "NEXT_SESSION"
        sell_plan = {"mode": "REDUCE_PENDING_T1", "range": None, "label": "今日买入不可卖；下一交易日开盘优先减仓"}
        next_plan = "下一交易日开盘优先降低仓位；若风险升级则直接退出。"
    elif intent == "REDUCE":
        action, priority, urgency = "分批减仓", 1, "HIGH"
        sell_plan = {"mode": "REDUCE_NOW", "range": _range(latest_price, latest_price) or take_profit_range, "label": "先降低仓位；若盘中触发退出线，剩余仓位直接退出"}
        next_plan = "若今天未完成减仓，下一交易日开盘优先处理剩余计划。"
    elif intent == "HOLD":
        action, priority, urgency = "继续持有", 2, "NORMAL"
        sell_plan = {"mode": "PLANNED_RANGE" if take_profit_range else "RECALCULATE_AFTER_CLOSE", "range": take_profit_range, "label": "到达计划区间后分批止盈" if take_profit_range else "收盘后重新计算卖出范围"}
        next_plan = "收盘后按最新趋势、事件与保护位重新生成下一交易日预案。"
    else:
        action, priority, urgency = "冻结加仓，等待数据", 3, "DATA_BLOCKED"
        sell_plan = {"mode": "RISK_ONLY", "range": None, "label": "证据不完整时不生成正常卖出区间；已知硬止损仍优先"}
        next_plan = "数据恢复前禁止加仓；仅执行已经明确的风险退出条件。"

    pnl = pnl_pct = None
    if latest_price is not None and cost_price is not None and shares > 0:
        pnl = round((latest_price - cost_price) * shares, 2)
        pnl_pct = round((latest_price / cost_price - 1.0) * 100.0, 2)
    direct_exit = (intent == "SELL" or signal_status == "SELL_ALERT") and not t1_blocked
    return {
        "stock_code": str(portfolio_row.get("stock_code") or "").zfill(6),
        "short_name": _first_text(portfolio_row.get("display_name"), portfolio_row.get("short_name"), portfolio_row.get("current_name")),
        "source": "WATCHLIST_HOLDING",
        "position_source": str(portfolio_row.get("position_source") or "manual"),
        "position_date": position_date or None,
        "cost_price": round(cost_price, 4) if cost_price is not None else None,
        "latest_price": round(latest_price, 4) if latest_price is not None else None,
        "shares": shares,
        "sellable_shares": 0 if t1_blocked else sellable_shares,
        "t1_blocked": t1_blocked,
        "pnl": pnl,
        "pnl_pct": pnl_pct,
        "exit_intent": intent,
        "action": action,
        "action_priority": priority,
        "urgency": urgency,
        "direct_exit": direct_exit,
        "reason": decision_reason or "当前没有足够证据形成持仓动作",
        "signal_status": signal_status or None,
        "main_wave_signal": main_wave_signal or None,
        "buy_plan": {"allowed": False, "range": entry_range, "label": "持仓页默认不加仓；买入范围仅供策略池复核"},
        "sell_plan": sell_plan,
        "emergency_exit": {
            "price": emergency_exit_price,
            "direct": direct_exit,
            "label": "今日买入受 T+1 限制；下一交易日直接退出" if t1_blocked and intent == "SELL" else "策略已触发 SELL_ALERT，盘中直接退出" if direct_exit else "跌破保护位后直接退出，不等待收盘" if emergency_exit_price is not None else "尚无可信保护位，禁止加仓并等待数据",
        },
        "next_session_plan": next_plan,
        "trade_date": trade_date,
        "knowledge_cutoff": exit_decision.get("knowledge_cutoff"),
        "evaluated_at": exit_decision.get("evaluated_at"),
        "valid_until": exit_decision.get("valid_until"),
        "context_hash": exit_decision.get("context_hash"),
        "execution_authority": "ADVISORY_ONLY",
    }


def summarize_watchlist_holding_strategies(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = {"SELL": 0, "REDUCE": 0, "HOLD": 0, "WAIT_DATA": 0}
    for row in rows:
        intent = str(row.get("exit_intent") or "WAIT_DATA").upper()
        counts[intent if intent in counts else "WAIT_DATA"] += 1
    return {
        "holding_count": len(rows),
        "sell_count": counts["SELL"],
        "reduce_count": counts["REDUCE"],
        "hold_count": counts["HOLD"],
        "wait_data_count": counts["WAIT_DATA"],
        "urgent_count": counts["SELL"] + counts["REDUCE"],
    }
