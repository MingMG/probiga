"""Point-in-time, explainable market-stage rules for the broad index."""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from itertools import accumulate
import math
from statistics import mean, stdev
from typing import Any

from server.common.pit_facts import normalize_decision_at


MODEL_VERSION = "probiga.market-stage.rules.v1"
_MARKET_CLOSE = time(15)
_PERIODS = {
    # Each tuple is the trend window, fast window, material move, drawdown.
    "daily": (20, 5, 0.8, 4.0),
    "weekly": (12, 3, 1.5, 7.0),
    "monthly": (8, 2, 2.5, 12.0),
}
_DOWN_STATES = {"DOWNTREND", "ACCELERATING_DECLINE"}
_BREADTH_KEYS = (
    "above_ma20_pct", "above_ma60_pct", "new_low_20d_pct", "new_low_60d_pct", "advancing_pct",
)


@dataclass(frozen=True)
class _Bar:
    day: date
    open: float
    high: float
    low: float
    close: float
    amount: float | None


def _date(value: date | str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("market stage session must be YYYY-MM-DD")
    return parsed


def _closed_by(day: date, cutoff: datetime) -> bool:
    return datetime.combine(day, _MARKET_CLOSE) <= cutoff


def _market_bar(day: date, row: Mapping[str, Any]) -> _Bar:
    try:
        prices = {key: float(row[key]) for key in ("open", "high", "low", "close")}
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"invalid market stage OHLC for {day}") from exc
    if (not all(math.isfinite(value) and value > 0 for value in prices.values())
            or not prices["low"] <= min(prices["open"], prices["close"])
            or not prices["high"] >= max(prices["open"], prices["close"])):
        raise ValueError(f"invalid market stage OHLC for {day}")
    try:
        amount = float(row["amount"]) if row.get("amount") is not None else None
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"invalid market stage turnover for {day}") from exc
    if amount is not None and (not math.isfinite(amount) or amount < 0):
        raise ValueError(f"invalid market stage turnover for {day}")
    return _Bar(day=day, amount=amount, **prices)


def valid_market_bar(row: Mapping[str, Any]) -> bool:
    """Allow adapters to isolate a bad session before computing coverage."""
    try:
        _market_bar(_date(row["trade_date"]), row)
    except (KeyError, TypeError, ValueError):
        return False
    return True


def _known_bars(rows: Iterable[Mapping[str, Any]], cutoff: datetime) -> dict[date, _Bar]:
    latest: dict[date, tuple[datetime, list[Mapping[str, Any]]]] = {}
    for row in rows:
        day = _date(row["trade_date"])
        available = normalize_decision_at(row["available_at"])
        if available > cutoff or not _closed_by(day, available):
            continue
        prior = latest.get(day)
        if prior is None or available > prior[0]:
            latest[day] = available, [row]
        elif available == prior[0]:
            prior[1].append(row)
    known = {}
    for day, (_, versions) in sorted(latest.items()):
        bars = {_market_bar(day, row) for row in versions}
        if len(bars) != 1:
            raise ValueError(f"conflicting market stage bars for {day}")
        known[day] = bars.pop()
    return known


def _period_start(day: date, period: str) -> date:
    if period == "weekly":
        return day - timedelta(days=day.weekday())
    return day.replace(day=1)


def _aggregate(bars: Sequence[_Bar]) -> _Bar:
    amounts = [bar.amount for bar in bars if bar.amount is not None]
    return _Bar(
        day=bars[-1].day, open=bars[0].open, high=max(bar.high for bar in bars),
        low=min(bar.low for bar in bars), close=bars[-1].close,
        amount=sum(amounts) if len(amounts) == len(bars) else None,
    )


def _completed_bars(
    known: Mapping[date, _Bar], calendar: Sequence[date] | None, period: str, cutoff: datetime,
) -> list[_Bar]:
    days = tuple(known) if calendar is None else calendar
    groups: dict[date, list[date]] = {}
    for day in days:
        groups.setdefault(_period_start(day, period), []).append(day)
    current = _period_start(cutoff.date(), period)
    complete = []
    for key, sessions in groups.items():
        if key > current:
            break
        if calendar is not None and next(reversed(groups)) == key:
            # The last retained calendar group has no next period to prove closure.
            break
        if key == current and (calendar is None or not _closed_by(sessions[-1], cutoff)):
            break
        # The start of a supplied calendar can fall inside the first period.
        # Omit that group unless the calendar itself begins at its boundary.
        if key < days[0] and key == next(iter(groups)):
            continue
        if not all(day in known for day in sessions):
            complete.clear()
            continue
        complete.append(_aggregate([known[day] for day in sessions]))
    return complete


def _move(value: float, base: float) -> float:
    return (value / base - 1.0) * 100


def _retest(recent: Sequence[_Bar], resistance: float, move: float) -> bool:
    breakout = next((i for i, bar in enumerate(recent) if _move(bar.close, resistance) >= move), None)
    if breakout is None:
        return False
    after_break = recent[breakout + 1:]
    return (any(abs(_move(bar.low, resistance)) <= move for bar in after_break)
            and all(_move(bar.close, resistance) >= -move / 2 for bar in after_break)
            and recent[-1].close >= resistance)


def _period_state(bars: Sequence[_Bar], period: str) -> dict[str, Any]:
    window, fast, material_move, drawdown = _PERIODS[period]
    result = {"state": None, "end_date": bars[-1].day.isoformat() if bars else None,
              "observation_count": len(bars), "evidence": [], "features": {}}
    if len(bars) < window:
        result["evidence"] = [f"连续有效周期不足，至少需要 {window} 根已收盘行情。"]
        return result
    tail = bars[-window:]
    recent, previous = tail[-fast:], tail[-2 * fast:-fast]
    closes = [bar.close for bar in tail]
    close = closes[-1]
    fast_return = _move(close, closes[-fast - 1])
    prior_return = _move(closes[-fast - 1], closes[-2 * fast - 1])
    trend_return = _move(close, closes[0])
    moving_average = mean(closes)
    fast_average = mean(bar.close for bar in recent)
    slope = _move(fast_average, mean(bar.close for bar in previous))
    recent_low, prior_low = min(bar.low for bar in recent), min(bar.low for bar in previous)
    recent_high, prior_high = max(bar.high for bar in recent), max(bar.high for bar in previous)
    support = prior_low
    resistance = max(bar.high for bar in tail[-3 * fast:-2 * fast])
    higher_lows = _move(recent_low, prior_low) >= material_move / 2
    higher_highs = _move(recent_high, prior_high) >= material_move / 2
    tested_breakout = _retest(recent, resistance, material_move)
    prior_closes = closes[:-fast]
    peak = max(prior_closes)
    prior_drawdown = min(_move(value, high) for value, high in zip(prior_closes, accumulate(prior_closes, max)))
    current_drawdown = _move(close, max(closes))
    weak_background = prior_drawdown <= -drawdown or _move(close, peak) <= -drawdown
    range_now = mean(_move(bar.high, bar.low) for bar in recent)
    range_prior = mean(_move(bar.high, bar.low) for bar in previous)
    range_ratio = range_now / range_prior if range_prior > 0 else None
    recent_amounts = [bar.amount for bar in recent if bar.amount is not None]
    prior_amounts = [bar.amount for bar in previous if bar.amount is not None]
    amount_ratio = (sum(recent_amounts) / sum(prior_amounts)
                    if len(recent_amounts) == len(prior_amounts) == fast and sum(prior_amounts) > 0 else None)
    new_low = _move(recent_low, support) <= -material_move / 2
    support_tests = sum(abs(_move(bar.low, support)) <= material_move for bar in recent)
    bounced = fast_return >= material_move and _move(close, recent_low) >= material_move
    structural = (higher_lows and higher_highs and close > fast_average and slope > 0) or tested_breakout
    downside = (close < moving_average and slope < 0 and fast_return < -material_move) or (
        new_low and close < fast_average and fast_return < 0
    )

    state, reason = None, "当前量价结构未达到阶段判定阈值。"
    if new_low and fast_return < min(-2 * material_move, prior_return - material_move):
        state, reason = "ACCELERATING_DECLINE", "近期低点下移，跌幅较上一窗口扩大。"
    elif downside:
        state, reason = "DOWNTREND", "价格结构及均线仍偏弱，需观察前低能否守住。"
    elif weak_background and bounced and structural:
        state, reason = "REVERSAL_CANDIDATE", "下跌背景中出现结构改善，进入反转核验。"
    elif weak_background and bounced:
        state, reason = "TECHNICAL_REBOUND", "低点后回升，尚缺持续抬升的高低点或突破回踩证据。"
    elif weak_background and not new_low and support_tests >= 2 and range_ratio is not None and range_ratio < 0.9:
        state, reason = "BOTTOMING_CANDIDATE", "前低附近至少两次获得支撑，近期价格振幅收敛。"
    elif weak_background and not new_low:
        state, reason = "STABILIZATION_ATTEMPT", "前期跌幅较大，近期低点暂未明显下移。"
    elif (trend_return >= drawdown and current_drawdown >= -drawdown and not higher_highs
          and fast_return <= material_move and fast_return < prior_return - material_move):
        state, reason = "TOP_CANDIDATE", "仍处近段高位，但近期动量较前一窗口回落。"
    elif trend_return >= material_move and close > moving_average and slope > 0:
        state, reason = "UPTREND", "价格高于趋势均线，短均线斜率为正。"

    features = dict(close=close, trend_ma=moving_average, fast_ma=fast_average,
                    fast_ma_slope_pct=slope, fast_return_pct=fast_return,
                    prior_fast_return_pct=prior_return, trend_return_pct=trend_return,
                    drawdown_pct=current_drawdown, support=support, resistance=resistance,
                    higher_lows=higher_lows, higher_highs=higher_highs, breakout_retested=tested_breakout,
                    realized_volatility_pct=stdev(_move(v, p) for p, v in zip(closes, closes[1:])),
                    range_ratio=range_ratio, turnover_ratio=amount_ratio, support_tests=support_tests)
    result.update(state=state, evidence=[reason], features={
        key: round(value, 6) if isinstance(value, float) else value for key, value in features.items()
    })
    return result


def _current_breadth(breadth: Mapping[str, Any] | None, day: date | None, cutoff: datetime) -> dict | None:
    if breadth is None or day is None or breadth.get("quality_status") != "VERIFIED":
        return None
    try:
        if _date(breadth["trade_date"]) != day:
            return None
        available = normalize_decision_at(breadth["available_at"])
        if available > cutoff or not _closed_by(day, available):
            return None
        values = {key: float(breadth[key]) for key in _BREADTH_KEYS}
        for key in ("above_ma20_change_5d_pp", "limit_down_pct"):
            if breadth.get(key) is not None:
                values[key] = float(breadth[key])
        if any(not (-100 if key.endswith("_pp") else 0) <= value <= 100 for key, value in values.items()):
            return None
    except (KeyError, TypeError, ValueError):
        return None
    result = {"trade_date": day.isoformat(), "available_at": available.isoformat(sep=" "),
              "quality_status": "VERIFIED", **values}
    if isinstance(breadth.get("source"), str) and breadth["source"]:
        result["source"] = breadth["source"]
    return result


def evaluate_market_stage(
    bars: Iterable[Mapping[str, Any]], *, as_of: datetime | str,
    trading_sessions: Sequence[date | str] | None = None,
    breadth: Mapping[str, Any] | None = None,
    target_session: date | str | None = None,
) -> dict[str, Any]:
    """Evaluate one index at a knowledge cutoff with an optional verified exchange calendar.

    The caller supplies exact availability timestamps. A known calendar can prove
    the last session of a week or month; without it only prior calendar periods
    are used and no reversal confirmation is issued. Thresholds are research rules.
    """
    cutoff = normalize_decision_at(as_of)
    period_cutoff = (min(cutoff, datetime.combine(_date(target_session), time.max))
                     if target_session is not None else cutoff)
    known = _known_bars(bars, cutoff)
    known = {day: bar for day, bar in known.items() if _closed_by(day, period_cutoff)}
    calendar = tuple(_date(day) for day in trading_sessions) if trading_sessions is not None else None
    if calendar is not None and (not calendar or calendar != tuple(sorted(set(calendar)))):
        raise ValueError("market stage calendar must contain unique increasing sessions")
    if calendar is not None and not set(known).issubset(calendar):
        raise ValueError("market stage bars differ from the supplied exchange calendar")
    daily = list(known.values())
    expected = [day for day in calendar if _closed_by(day, period_cutoff)] if calendar is not None else []
    if calendar is not None:
        daily = []
        for day in reversed(expected):
            if day not in known:
                break
            daily.append(known[day])
        daily.reverse()
    periods = {"daily": _period_state(daily, "daily")}
    for period in ("weekly", "monthly"):
        periods[period] = _period_state(_completed_bars(known, calendar, period, period_cutoff), period)
    target = expected[-1] if expected else next(reversed(known), None)
    calendar_continues = calendar is not None and target is not None and calendar[-1] > target
    market_breadth = _current_breadth(breadth, target, cutoff)
    daily_state = periods["daily"]["state"]
    weekly_state = periods["weekly"]["state"]
    monthly_state = periods["monthly"]["state"]
    features = periods["daily"]["features"]
    state = daily_state
    evidence = list(periods["daily"]["evidence"])
    uncertainties = []
    if calendar is None:
        uncertainties.append("缺少交易日历证据，周月只采用上一个日历周期及更早行情。")
    elif not calendar_continues:
        uncertainties.append("当前日历未覆盖后续交易日，无法确认当前周月的收盘边界。")
    if not all(periods[period]["features"] for period in ("weekly", "monthly")):
        uncertainties.append("部分高周期缺少连续已收盘样本。")
    if market_breadth is None:
        uncertainties.append("缺少同交易日已核验广度，指数信号无法代表全市场。")
    if features and features["turnover_ratio"] is None:
        uncertainties.append("成交额历史不完整，无法验证成交持续性。")
    if daily_state in {"TECHNICAL_REBOUND", "REVERSAL_CANDIDATE"} and (
        weekly_state in _DOWN_STATES or monthly_state in _DOWN_STATES
    ):
        uncertainties.append("日线改善与更长周期下跌结构同时存在。")
    if daily_state == "REVERSAL_CANDIDATE" and weekly_state in _DOWN_STATES:
        state = "TECHNICAL_REBOUND"
        evidence.append("日线结构已有改善，但周线仍偏弱，综合状态暂按技术性反弹识别。")
    breadth_confirmed = False
    if market_breadth is not None and features:
        broad_recovery = market_breadth["above_ma20_pct"] >= 55 and market_breadth["above_ma60_pct"] >= 50
        if broad_recovery and daily_state in _DOWN_STATES:
            evidence.append("指数仍弱，但过半数股票位于中期均线上方。")
        if market_breadth["above_ma20_pct"] < 45 and features["fast_return_pct"] > 0:
            evidence.append("指数回升时，站上 20 日均线的股票比例仍低于 45%。")
        breadth_confirmed = (
            broad_recovery and market_breadth["advancing_pct"] >= 50
            and market_breadth["new_low_20d_pct"] <= 8 and market_breadth["new_low_60d_pct"] <= 5
            and market_breadth.get("above_ma20_change_5d_pp", -1) >= 0
        )
    if (state == "REVERSAL_CANDIDATE" and calendar_continues and features["breakout_retested"]
            and weekly_state in {"REVERSAL_CANDIDATE", "UPTREND"}
            and monthly_state in {"STABILIZATION_ATTEMPT", "BOTTOMING_CANDIDATE", "TECHNICAL_REBOUND", "REVERSAL_CANDIDATE", "UPTREND"}
            and breadth_confirmed and (features["turnover_ratio"] or 0) >= 1.05):
        state = "REVERSAL_CONFIRMED"
        evidence.append("突破回踩、周月结构、成交额及已验证市场广度共同支持反转。")
    confirmation = ["核对周线结构、月线弱势是否缓解，以及市场广度与成交额能否持续改善。"]
    invalidation = ["广度转弱、创新低扩散，或短期成交改善衰减。"]
    if features:
        support, resistance = features["support"], features["resistance"]
        confirmation.insert(0, f"观察前低 {support:.2f} 能否守住并形成更高低点，突破 {resistance:.2f} 后等待回踩。")
        retest_floor = resistance * (1 - _PERIODS["daily"][2] / 200)
        invalidation.insert(0, f"跌破支撑 {support:.2f}，或回踩期间收在 {retest_floor:.2f} 下方。"
                              f"回踩完成时收盘需恢复至 {resistance:.2f} 或以上。")
    scenarios = ["跌破支撑时，反弹或磨底假设失效，应重新识别下跌阶段。",
                 "回踩守住且更多股票同步修复时，再评估中期趋势。"]
    if state in _DOWN_STATES:
        scenarios[0] = "继续破低并伴随波动或跌停扩散时，维持下跌风险情景。"
    return dict(model_version=MODEL_VERSION, as_of=cutoff.isoformat(sep=" "),
                trade_date=target.isoformat() if target else None,
                status="READY" if features else "INSUFFICIENT", state=state, periods=periods,
                candidate_signals={"technical_rebound": state == "TECHNICAL_REBOUND",
                                   "reversal": daily_state == "REVERSAL_CANDIDATE"},
                evidence=evidence, confirmation_conditions=confirmation,
                invalidation_conditions=invalidation, risk_scenarios=scenarios,
                uncertainties=uncertainties, breadth=market_breadth)
