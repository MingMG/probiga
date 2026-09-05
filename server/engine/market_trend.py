# -*- coding: utf-8 -*-
"""Transparent multi-period market trend projection.

The module is deliberately independent from the database and schedulers so the
same calculation can be reused by APIs, briefings and reviews.  It only accepts
daily index closes; it does not attempt to reproduce an indicator from a chart
whose formula is unknown.
"""
from __future__ import annotations

import calendar
import math
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Iterable


SCHEMA_VERSION = "market-trend-v1"
OBSERVATION_SCHEMA_VERSION = "market-trend-observation-v1"
DEFAULT_INDEX_NAMES = {
    "000016": "上证50",
    "000300": "沪深300",
    "000905": "中证500",
    "000852": "中证1000",
    "399303": "国证2000",
    "399006": "创业板指",
    "000688": "科创50",
}
METHOD = {
    "source_frequency": "日线收盘价",
    "resample": "周线/月线取对应自然周期内最后一个已有收盘价",
    "indicators": [
        {"name": "SMA", "parameters": {"fast": 20, "slow": 60}, "formula": "最近N根收盘价的算术平均"},
        {"name": "SMA20斜率", "parameters": {"lookback_bars": 5}, "formula": "SMA20相对5根前的百分比变化"},
        {"name": "RSI", "parameters": {"period": 14}, "formula": "100-100/(1+平均上涨幅度/平均下跌幅度)，简单移动平均"},
        {"name": "位置分位", "parameters": {"lookback_bars": 252}, "formula": "当前收盘价在回看窗口收盘价中的百分位"},
    ],
    "thresholds": {
        "trend_slope_pct": 0.5,
        "low_position_percentile_lte": 20,
        "high_position_percentile_gte": 80,
        "bottoming_rsi_improvement_gte": 3,
        "strengthening_rsi_gte": 50,
    },
    "reference_chart_indicator": {
        "status": "unknown",
        "note": "截图指标名称和公式未知，仅参考展示目标，不仿造其数值。",
    },
}


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value or "")[:10])
    except (TypeError, ValueError):
        return None


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) and result > 0 else None


def _sma(values: list[float], period: int, index: int) -> float | None:
    if index + 1 < period:
        return None
    window = values[index + 1 - period : index + 1]
    return sum(window) / period


def _rsi(values: list[float], period: int, index: int) -> float | None:
    if index < period:
        return None
    changes = [values[pos] - values[pos - 1] for pos in range(index - period + 1, index + 1)]
    gains = sum(max(change, 0.0) for change in changes) / period
    losses = sum(max(-change, 0.0) for change in changes) / period
    if losses == 0:
        return 100.0 if gains > 0 else 50.0
    return 100.0 - 100.0 / (1.0 + gains / losses)


def _percentile(values: list[float], index: int, lookback: int = 252) -> float:
    window = values[max(0, index + 1 - lookback) : index + 1]
    current = values[index]
    below = sum(value < current for value in window)
    equal = sum(value == current for value in window)
    return (below + max(0, equal - 1) / 2) / max(len(window) - 1, 1) * 100.0


def _period_end(value: date, period: str) -> date:
    if period == "daily":
        return value
    if period == "weekly":
        return value + timedelta(days=4 - value.weekday())
    return date(value.year, value.month, calendar.monthrange(value.year, value.month)[1])


def _resample(rows: list[tuple[date, float]], period: str) -> list[tuple[date, float, date]]:
    if period == "daily":
        return [(row_date, close, row_date) for row_date, close in rows]
    grouped: dict[Any, list[tuple[date, float]]] = defaultdict(list)
    for row_date, close in rows:
        key = row_date.isocalendar()[:2] if period == "weekly" else (row_date.year, row_date.month)
        grouped[key].append((row_date, close))
    result = []
    for values in grouped.values():
        row_date, close = max(values, key=lambda item: item[0])
        result.append((row_date, close, _period_end(row_date, period)))
    return sorted(result, key=lambda item: item[0])


def _state_at(bars: list[tuple[date, float, date]], index: int) -> dict[str, Any]:
    closes = [bar[1] for bar in bars]
    close = closes[index]
    fast = _sma(closes, 20, index)
    slow = _sma(closes, 60, index)
    fast_then = _sma(closes, 20, index - 5) if index >= 5 else None
    slope = ((fast / fast_then - 1.0) * 100.0) if fast and fast_then else None
    rsi = _rsi(closes, 14, index)
    prior_rsi = _rsi(closes, 14, index - 3) if index >= 3 else None
    location = _percentile(closes, index)
    low = location <= 20.0
    high = location >= 80.0

    if fast is None or slope is None:
        direction = "unavailable"
    elif close > fast and slope >= 0.5:
        direction = "up"
    elif close < fast and slope <= -0.5:
        direction = "down"
    else:
        direction = "range"

    previous_window = closes[max(0, index - 20) : index]
    new_low = bool(previous_window and close < min(previous_window))
    five_return = ((close / closes[index - 5] - 1.0) * 100.0) if index >= 5 else None
    rsi_improvement = (rsi - prior_rsi) if rsi is not None and prior_rsi is not None else None
    bottoming = bool(
        low
        and rsi_improvement is not None
        and rsi_improvement >= 3.0
        and five_return is not None
        and five_return > 0
        and not new_low
    )
    strengthening = bool(
        direction == "up"
        and rsi is not None
        and rsi >= 50.0
        and slow is not None
        and close > slow
    )
    return {
        "direction": direction,
        "position": "low" if low else "high" if high else "middle",
        "bottoming": "observing" if bottoming else "not_seen",
        "strengthening": "confirmed" if strengthening else "not_confirmed",
        "metrics": {
            "close": round(close, 4),
            "sma20": round(fast, 4) if fast is not None else None,
            "sma60": round(slow, 4) if slow is not None else None,
            "sma20_slope_5_pct": round(slope, 3) if slope is not None else None,
            "rsi14": round(rsi, 2) if rsi is not None else None,
            "location_252_pct": round(location, 1),
            "return_5_bars_pct": round(five_return, 2) if five_return is not None else None,
            "new_low_20": new_low,
        },
    }


def _direction_text(direction: str) -> str:
    return {"up": "上行", "down": "下行", "range": "反复震荡", "unavailable": "数据不足"}.get(direction, "数据不足")


def _period_result(
    rows: list[tuple[date, float]],
    period: str,
    requested: date,
    *,
    daily_closed: bool,
    next_trade_date: date | None,
) -> dict[str, Any]:
    bars = _resample(rows, period)
    if not bars:
        return {"status": "unavailable", "reason": "没有可用收盘价"}
    current_index = len(bars) - 1
    current = _state_at(bars, current_index)
    latest_date, _close, end_date = bars[current_index]
    if period == "daily":
        is_final = latest_date < requested or daily_closed
        closure_basis = "daily_close_fact"
    elif next_trade_date is not None:
        is_final = daily_closed and _period_end(next_trade_date, period) != end_date
        closure_basis = (
            f"next_effective_trade_date:{next_trade_date.isoformat()}"
            if daily_closed
            else f"daily_close_unconfirmed;next_effective_trade_date:{next_trade_date.isoformat()}"
        )
    else:
        # Without an effective trading calendar, a natural Friday/month-end
        # rule would be wrong around holidays.  Fail closed as provisional.
        is_final = False
        closure_basis = "next_effective_trade_date_unavailable"
    confirmation = "final" if is_final else "provisional"
    confirmed_index = current_index if is_final else current_index - 1
    confirmed = _state_at(bars, confirmed_index) if confirmed_index >= 0 else None

    start_index = current_index
    while start_index > 0 and _state_at(bars, start_index - 1)["direction"] == current["direction"]:
        start_index -= 1

    transitions: list[dict[str, Any]] = []
    previous = None
    history_last_index = current_index if is_final else current_index - 1
    for index in range(max(0, history_last_index + 1)):
        state = _state_at(bars, index)
        key = (state["direction"], state["position"], state["bottoming"], state["strengthening"])
        if previous is not None and key != previous[0]:
            changed = []
            labels = ("趋势", "位置", "止跌", "转强")
            for pos, label in enumerate(labels):
                if key[pos] != previous[0][pos]:
                    changed.append(f"{label}:{previous[0][pos]}→{key[pos]}")
            transitions.append({"changed_at": bars[index][0].isoformat(), "reason": "；".join(changed)})
        previous = (key, bars[index][0])

    evidence = []
    metrics = current["metrics"]
    if metrics["sma20"] is not None:
        evidence.append(f"收盘价{metrics['close']:.2f}，SMA20为{metrics['sma20']:.2f}")
    if metrics["sma20_slope_5_pct"] is not None:
        evidence.append(f"SMA20近5根斜率{metrics['sma20_slope_5_pct']:+.2f}%")
    evidence.append(f"位置分位P{metrics['location_252_pct']:.0f}")
    if metrics["rsi14"] is not None:
        evidence.append(f"RSI14为{metrics['rsi14']:.1f}")

    return {
        "status": "ok" if len(bars) >= 20 else "insufficient_history",
        "period": period,
        "confirmation_status": confirmation,
        "data_cutoff": latest_date.isoformat(),
        "natural_period_end": end_date.isoformat(),
        "closure_basis": closure_basis,
        "bar_count": len(bars),
        **current,
        "confirmed_state": (
            {"data_cutoff": bars[confirmed_index][0].isoformat(), **confirmed}
            if confirmed is not None
            else None
        ),
        "trend_started_at": bars[start_index][0].isoformat(),
        "trend_duration_bars": current_index - start_index + 1,
        "evidence": evidence,
        "history": transitions[-12:],
        "history_kind": "derived_from_price_history",
        "explanation": (
            f"当前为{_direction_text(current['direction'])}；"
            f"位置{'偏低' if current['position'] == 'low' else '偏高' if current['position'] == 'high' else '居中'}；"
            f"{'出现止跌观察信号' if current['bottoming'] == 'observing' else '尚未出现止跌信号'}；"
            f"{'已满足转强条件' if current['strengthening'] == 'confirmed' else '尚未满足转强条件'}。"
        ),
    }


def _summary(periods: dict[str, dict[str, Any]]) -> dict[str, str]:
    daily = periods.get("daily") or {}
    weekly = periods.get("weekly") or {}
    monthly = periods.get("monthly") or {}
    daily_direction = daily.get("direction", "unavailable")
    weekly_direction = weekly.get("direction", "unavailable")
    position = weekly.get("position") if weekly.get("status") != "unavailable" else daily.get("position")
    conflict = daily_direction not in {"unavailable", weekly_direction} and weekly_direction != "unavailable"
    weekly_bottoming = weekly.get("bottoming") == "observing"
    daily_bottoming = daily.get("bottoming") == "observing"
    bottoming = weekly_bottoming or daily_bottoming
    strengthening = weekly.get("strengthening") == "confirmed"
    weekly_provisional = weekly.get("confirmation_status") == "provisional"
    bottoming_provisional = (
        weekly_bottoming and weekly_provisional
    ) or (
        not weekly_bottoming
        and daily_bottoming
        and daily.get("confirmation_status") == "provisional"
    )
    return {
        "daily": f"日线：当前{_direction_text(daily_direction)}。",
        "weekly": (
            f"周线：当前{_direction_text(weekly_direction)}"
            + ("（本周尚未结束，属于暂时变化）。" if weekly.get("confirmation_status") == "provisional" else "。")
        ),
        "monthly": (
            f"月线背景：{_direction_text(monthly.get('direction', 'unavailable'))}"
            + ("（本月尚未结束，属于暂时变化）。" if monthly.get("confirmation_status") == "provisional" else "。")
        ),
        "position": "所处位置：指标进入历史偏低区域，但低位不等于底部。" if position == "low" else "所处位置：当前不在历史低位区。",
        "overall": (
            "综合判断：日线与周线方向存在分歧，短期变化尚未改变中期趋势。"
            if conflict
            else f"综合判断：日线与周线方向大体一致，当前以{_direction_text(weekly_direction)}为主。"
        ),
        "watch": (
            "后续观察：本周暂时满足转强条件，需等待周线收盘确认。"
            if strengthening and weekly_provisional
            else "后续观察：周线已满足转强条件，继续观察能否维持。"
            if strengthening
            else "后续观察：本周暂时出现止跌迹象，需等待周线收盘确认。"
            if bottoming and bottoming_provisional
            else "后续观察：已有止跌迹象，等待周线转强条件确认。"
            if bottoming
            else "后续观察：周线是否停止创新低、下跌力度是否减弱，以及价格是否重新站上趋势线。"
        ),
    }


def build_market_trend(
    index_rows: Iterable[dict[str, Any]],
    *,
    requested_date: str | date | None = None,
    generated_at: str | datetime | None = None,
    daily_closed: bool = True,
    next_trade_date: str | date | None = None,
    index_names: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build reproducible daily/weekly/monthly index trend states."""
    requested = _as_date(requested_date) or date.today()
    next_trade = _as_date(next_trade_date)
    names = {**DEFAULT_INDEX_NAMES, **(index_names or {})}
    grouped: dict[str, dict[date, float]] = defaultdict(dict)
    for row in index_rows:
        raw_code = str(row.get("index_code") or row.get("code") or "").split(".")[0]
        code = raw_code.zfill(6) if raw_code.isdigit() and len(raw_code) <= 6 else ""
        row_date = _as_date(row.get("trade_date") or row.get("date"))
        close = _number(row.get("close"))
        if code in names and row_date and close is not None and row_date <= requested:
            grouped[code][row_date] = close

    generated = generated_at.isoformat(sep=" ", timespec="seconds") if isinstance(generated_at, datetime) else str(generated_at or datetime.now().isoformat(sep=" ", timespec="seconds"))
    indices = []
    overall_latest = max((max(values) for values in grouped.values() if values), default=None)
    for code in sorted(grouped, key=lambda item: list(names).index(item) if item in names else len(names)):
        rows = sorted(grouped[code].items())
        cutoff = rows[-1][0]
        gap_days = max(0, (requested - cutoff).days)
        if overall_latest is not None and cutoff < overall_latest:
            source_status = "stale"
        elif next_trade is not None:
            source_status = "fresh" if next_trade > requested else "stale"
        else:
            source_status = "fresh" if gap_days <= 3 else "stale"
        index_daily_closed = daily_closed and (
            overall_latest is None or cutoff == overall_latest
        )
        periods = {
            period: _period_result(
                rows,
                period,
                requested,
                daily_closed=index_daily_closed,
                next_trade_date=next_trade,
            )
            for period in ("daily", "weekly", "monthly")
        }
        indices.append(
            {
                "index_code": code,
                "index_name": names.get(code, code),
                "source_status": source_status,
                "data_cutoff": cutoff.isoformat(),
                "gap_calendar_days": gap_days,
                "periods": periods,
                "summary": _summary(periods),
            }
        )

    cutoffs = [item["data_cutoff"] for item in indices]
    all_requested_available = all(code in grouped and grouped[code] for code in names)
    all_fresh = bool(indices) and all(item["source_status"] == "fresh" for item in indices)
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "ok" if all_requested_available and all_fresh else "partial" if indices else "unavailable",
        "requested_date": requested.isoformat(),
        "generated_at": generated,
        "data_cutoff": max(cutoffs) if cutoffs else None,
        "source": {"table": "sm_index_kline", "frequency": "daily_close", "provider": "existing_market_store"},
        "methodology": METHOD,
        "indices": indices,
        "coverage": {
            "requested_index_count": len(names),
            "available_index_count": len(indices),
            "missing_indices": [
                {"index_code": code, "index_name": name, "source_status": "missing"}
                for code, name in names.items()
                if code not in grouped
            ],
        },
    }


def compact_market_trend_observation(trend: dict[str, Any]) -> dict[str, Any]:
    """Keep the original daily conclusion without copying derived history."""

    indices = []
    for item in trend.get("indices") or []:
        periods = {}
        for key in ("daily", "weekly", "monthly"):
            source = (item.get("periods") or {}).get(key) or {}
            periods[key] = {
                field: source.get(field)
                for field in (
                    "status",
                    "confirmation_status",
                    "data_cutoff",
                    "direction",
                    "position",
                    "bottoming",
                    "strengthening",
                    "metrics",
                    "evidence",
                    "explanation",
                )
            }
        indices.append(
            {
                "index_code": item.get("index_code"),
                "index_name": item.get("index_name"),
                "source_status": item.get("source_status"),
                "data_cutoff": item.get("data_cutoff"),
                "periods": periods,
                "summary": item.get("summary") or {},
            }
        )
    return {
        "evidence_type": "market_trend_snapshot",
        "schema_version": OBSERVATION_SCHEMA_VERSION,
        "calculation_schema_version": trend.get("schema_version"),
        "requested_date": trend.get("requested_date"),
        "generated_at": trend.get("generated_at"),
        "status": trend.get("status"),
        "data_cutoff": trend.get("data_cutoff"),
        "methodology": trend.get("methodology") or {},
        "coverage": trend.get("coverage") or {},
        "indices": indices,
    }


__all__ = [
    "DEFAULT_INDEX_NAMES",
    "METHOD",
    "OBSERVATION_SCHEMA_VERSION",
    "SCHEMA_VERSION",
    "build_market_trend",
    "compact_market_trend_observation",
]
