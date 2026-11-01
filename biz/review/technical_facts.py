"""Pure, closed-session technical measurements from one unadjusted series.

No quote loader, database, realtime defaults or corporate-action repair is used.
All availability decisions describe the supplied rows and optional calendar.
"""
from __future__ import annotations

from datetime import date, datetime
import math
from typing import Any, Iterable, Mapping

WARMUP = 120
MA_WINDOWS = (5, 10, 20, 60)


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _day(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(str(value)).isoformat()


def _cross(previous_left, previous_right, last_left, last_right) -> str:
    if any(value is None for value in (previous_left, previous_right, last_left, last_right)):
        return "UNKNOWN"
    def difference(left, right):
        return 0.0 if math.isclose(left, right, rel_tol=1e-10, abs_tol=1e-12) else left - right
    before, after = difference(previous_left, previous_right), difference(last_left, last_right)
    if before <= 0 < after:
        return "BULLISH"
    if before >= 0 > after:
        return "BEARISH"
    return "NONE"


def _ema(values: list[float], span: int) -> list[float]:
    alpha = 2.0 / (span + 1)
    result = [values[0]]
    for value in values[1:]:
        result.append(alpha * value + (1 - alpha) * result[-1])
    return result


def _mean(values: list[float]) -> float | None:
    try:
        anchor = values[0]
        return _number(anchor + math.fsum((value - anchor) / len(values) for value in values))
    except (OverflowError, ValueError):
        return None


def _unknown(reason: str) -> dict:
    return {"status": "UNKNOWN", "last": None, "previous": None, "cross": "UNKNOWN", "reason": reason}


def compute_technical_facts(rows: Iterable[Mapping[str, Any]], *, trade_date,
                            expected_dates=None, volume_quality: Mapping[str, Any] | None = None) -> dict:
    """Calculate MA, MACD, KDJ and prior-complete-session volume ratios.

    MACD/KDJ need 120 consecutive supplied daily bars for the last value and
    121 for a warmed previous value/cross. ``expected_dates`` proves calendar
    coverage; without it, continuity is asserted only over supplied dates.
    Volume ratios additionally require an explicit VERIFIED unit/amount-price
    receipt covering this series, never merely a provider's quality label.
    """
    target = _day(trade_date)
    if hasattr(rows, "to_dict") and not isinstance(rows, Mapping):
        records = rows.to_dict(orient="records")
    else:
        records = [dict(row) for row in rows] if rows is not None else []
    issues: list[dict] = []
    result = {"status": "UNKNOWN", "trade_date": target, "coverage": {
        "provided_rows": len(records), "first_date": None, "last_date": None,
        "calendar_verified": expected_dates is not None, "expected_rows": None,
        "missing_dates_count": 0, "missing_dates_sample": [], "contiguous_tail_rows": 0,
        "price_status": "UNKNOWN", "volume_missing_or_invalid_rows": 0},
        "metadata": {"price_basis": "adjust_type=0, closed daily bars only", "ma_windows": list(MA_WINDOWS),
            "macd": {"fast": 12, "slow": 26, "signal": 9, "ema_adjust": False,
                     "ema_seed": "first close of the contiguous tail", "hist_multiplier": 2, "warmup_rows": WARMUP},
            "kdj": {"window": 9, "k_smoothing": 3, "d_smoothing": 3, "initial_k": 50,
                    "initial_d": 50, "first_update": "first complete 9-bar window", "flat_range_rsv": 50,
                    "flat_range_cross": "UNKNOWN", "warmup_rows": WARMUP},
            "volume_definition": "current daily volume / mean of preceding 5 or 20 complete daily bars; current bar excluded; not intraday same-time volume ratio",
            "continuity_tolerance": {"relative": 1e-8, "absolute": 1e-8},
            "cross_series": {"ma": "close minus same-session MA", "macd": "DIF minus DEA", "kdj": "K minus D"},
            "cross_definition": "previous <= zero and last > zero is BULLISH; previous >= zero and last < zero is BEARISH"},
        "ma": {str(period): {**_unknown("insufficient_complete_window"), "last_deviation_pct": None,
                             "previous_deviation_pct": None} for period in MA_WINDOWS},
        "macd": _unknown("insufficient_warmup"), "kdj": _unknown("insufficient_warmup"),
        "volume": {"status": "UNKNOWN", "unit": None, "ratio5": None, "ratio20": None,
                   "previous_ratio5": None, "previous_ratio20": None, "last_volume": None,
                   "previous_volume": None, "reason": "volume_unit_and_amount_price_not_verified"}, "issues": issues}
    coverage = result["coverage"]
    def issue(code, **details):
        prior = next((item for item in issues if item["code"] == code), None)
        if prior is not None:
            prior["occurrences"] = prior.get("occurrences", 1) + 1
            return
        issues.append({"code": code, **details})
    if not records:
        issue("empty_history")
        return result
    parsed = []
    invalid_dates = False
    for row in records:
        try:
            parsed.append(_day(row.get("trade_date")))
        except (TypeError, ValueError):
            invalid_dates = True
    if invalid_dates:
        issue("invalid_trade_date")
        return result
    coverage.update(first_date=parsed[0], last_date=parsed[-1])
    blocking = False
    if len(set(parsed)) != len(parsed) or parsed != sorted(parsed):
        issue("duplicate_or_unordered_dates")
        blocking = True
    if any(day > target for day in parsed):
        issue("future_bar_after_cutoff")
        blocking = True
    if parsed[-1] != target:
        issue("target_session_missing")
        blocking = True
    if any(_number(row.get("adjust_type")) != 0 for row in records):
        issue("mixed_missing_or_nonzero_adjust_type")
        blocking = True
    if any(row.get("k_type") is not None and _number(row["k_type"]) != 1 for row in records):
        issue("not_daily_bars")
        blocking = True
    identifiers = {(key, str(row[key])) for row in records for key in ("stock_code", "index_code") if row.get(key)}
    if len(identifiers) > 1:
        issue("multiple_instruments")
        blocking = True
    calendar = None
    if expected_dates is not None:
        try:
            calendar = [_day(day) for day in expected_dates]
        except (TypeError, ValueError):
            calendar = []
        if not calendar or calendar != sorted(set(calendar)) or calendar[-1] != target:
            issue("invalid_expected_calendar")
            blocking = True
        else:
            coverage["expected_rows"] = len(calendar)
            missing = sorted(set(calendar) - set(parsed))
            coverage.update(missing_dates_count=len(missing), missing_dates_sample=missing[:5])
            if missing:
                issue("history_dates_missing", count=len(missing))
            if set(parsed) - set(calendar):
                issue("unexpected_history_dates")
                blocking = True
    prices = []
    for index, row in enumerate(records):
        values = {key: _number(row.get(key)) for key in ("open", "high", "low", "close", "pre_close")}
        valid = all(value is not None and value > 0 for value in values.values())
        if not valid or not (values["low"] <= min(values["open"], values["close"]) <= max(values["open"], values["close"]) <= values["high"]):
            issue("invalid_ohlc_or_preclose", row=index, trade_date=parsed[index])
            blocking = True
        prices.append(values)
    positions = {day: index for index, day in enumerate(calendar or [])}
    for index in range(1, len(records)):
        if calendar and positions.get(parsed[index], -999) != positions.get(parsed[index - 1], -999) + 1:
            continue  # A known missing session is a coverage gap, not an ex-dividend diagnosis.
        previous, current = prices[index - 1]["close"], prices[index]["pre_close"]
        if previous is not None and current is not None and not math.isclose(previous, current, rel_tol=1e-8, abs_tol=1e-8):
            issue("previous_close_discontinuity_pending_verification", trade_date=parsed[index], previous_close=previous, pre_close=current)
            blocking = True
    volumes = [_number(row.get("volume")) for row in records]
    coverage["volume_missing_or_invalid_rows"] = sum(value is None or value < 0 for value in volumes)
    if blocking:
        return result
    coverage["price_status"] = "OBSERVED"
    count = len(records)
    if calendar:
        count = 0
        observed = set(parsed)
        for day in reversed(calendar):
            if day not in observed:
                break
            count += 1
    coverage["contiguous_tail_rows"] = count
    coverage["contiguous_tail_start_date"] = parsed[-count]
    prices, volumes = prices[-count:], volumes[-count:]
    closes = [row["close"] for row in prices]
    for period in MA_WINDOWS:
        last = _mean(closes[-period:]) if count >= period else None
        previous = _mean(closes[-period - 1:-1]) if count >= period + 1 else None
        last = last if last is not None and last > 0 else None
        previous = previous if previous is not None and previous > 0 else None
        result["ma"][str(period)] = {"status": "OBSERVED" if last is not None else "UNKNOWN",
            "last": last, "previous": previous,
            "last_deviation_pct": (closes[-1] / last - 1) * 100 if last is not None and last > 0 else None,
            "previous_deviation_pct": (closes[-2] / previous - 1) * 100 if previous is not None and previous > 0 else None,
            "cross": _cross(closes[-2] if count > 1 else None, previous, closes[-1], last),
            "reason": None if previous is not None else ("previous_window_incomplete" if last is not None else "insufficient_complete_window")}
    if count >= WARMUP:
        fast, slow = _ema(closes, 12), _ema(closes, 26)
        dif = [left - right for left, right in zip(fast, slow)]
        dea = _ema(dif, 9)
        macd_values = [{"dif": left, "dea": right, "hist": 2 * (left - right)} for left, right in zip(dif, dea)]
        last, previous = macd_values[-1], macd_values[-2] if count >= WARMUP + 1 else None
        result["macd"] = {"status": "OBSERVED", "last": last, "previous": previous,
            "cross": _cross(previous["dif"] if previous else None, previous["dea"] if previous else None, last["dif"], last["dea"]),
            "reason": None if previous else "previous_warmup_incomplete"}
        if any(_number(value) is None for item in (last, previous) if item for value in item.values()):
            result["macd"] = _unknown("nonfinite_arithmetic")
            issue("nonfinite_macd_arithmetic")
        k = d = 50.0
        kdj_values = []
        neutral_windows = 0
        for index in range(count):
            if index < 8:
                kdj_values.append(None)
                continue
            window = prices[index - 8:index + 1]
            low, high = min(row["low"] for row in window), max(row["high"] for row in window)
            neutral = high == low
            if neutral:
                neutral_windows += 1
            rsv = 50.0 if neutral else (closes[index] - low) / (high - low) * 100
            k = (2 * k + rsv) / 3
            d = (2 * d + k) / 3
            kdj_values.append({"k": k, "d": d, "j": 3 * k - 2 * d, "rsv": rsv,
                               "range_status": "NEUTRAL_FLAT_RANGE" if neutral else "OBSERVED"})
        last, previous = kdj_values[-1], kdj_values[-2] if count >= WARMUP + 1 else None
        cross = _cross(previous["k"] if previous else None, previous["d"] if previous else None, last["k"], last["d"])
        if last["range_status"] != "OBSERVED" or previous and previous["range_status"] != "OBSERVED":
            cross = "UNKNOWN"
        result["kdj"] = {"status": "OBSERVED", "last": last, "previous": previous, "cross": cross,
                         "neutral_range_windows": neutral_windows, "reason": None if previous else "previous_warmup_incomplete"}
    else:
        issue("insufficient_macd_kdj_warmup", required=WARMUP, observed=count)
    quality = dict(volume_quality or {})
    result["volume"]["verification"] = {key: quality.get(key) if isinstance(quality.get(key), str) else None
        for key in ("status", "unit", "amount_price_status")}
    result["volume"]["verification"]["sample_count"] = _number(quality.get("sample_count"))
    verified = (quality.get("status") == "VERIFIED" and quality.get("amount_price_status") == "VERIFIED"
                and isinstance(quality.get("unit"), str) and 0 < len(quality["unit"].strip()) <= 40
                and (quality.get("sample_count") is None or quality["sample_count"] == len(records))
                and (quality.get("trade_date") is None or str(quality["trade_date"])[:10] == target))
    if verified and coverage["volume_missing_or_invalid_rows"] == 0:
        volume = result["volume"]
        volume.update(unit=quality["unit"], last_volume=volumes[-1], previous_volume=volumes[-2] if count > 1 else None)
        def ratio(index, period):
            if index < period:
                return None
            mean = _mean(volumes[index - period:index])
            return _number(volumes[index] / mean) if mean is not None and mean > 0 else None
        for period in (5, 20):
            volume["ratio" + str(period)] = ratio(count - 1, period)
            volume["previous_ratio" + str(period)] = ratio(count - 2, period) if count > 1 else None
        available = any(volume[key] is not None for key in ("ratio5", "ratio20"))
        volume.update(status="OBSERVED" if available else "UNKNOWN",
                      reason=None if volume["ratio20"] is not None else "incomplete_window_or_zero_reference_volume")
    else:
        result["volume"]["reason"] = "missing_or_invalid_volume" if coverage["volume_missing_or_invalid_rows"] else "volume_unit_and_amount_price_not_verified"
        issue("volume_not_usable", reason=result["volume"]["reason"])
    if any(item["last"] is not None for item in result["ma"].values()) or result["volume"]["status"] == "OBSERVED":
        result["status"] = "OBSERVED"
    return result
