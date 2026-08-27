"""Production projection of the frozen V4 chase-risk policy.

The V4 release module is integrity-pinned and cannot import production code.
Production code is likewise forbidden from importing the research release.
This module therefore owns a small, I/O-free projection of the frozen daily
bar calculation.  Explicit parity tests prevent both its thresholds and its
assessment results from drifting from the integrity-pinned V4 implementation.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping


_ZERO = Decimal("0")
_HUNDRED = Decimal("100")
_PCT_QUANTUM = Decimal("0.000001")
_QUALITY_VALUES = frozenset({"PASS", "WARN", "FAIL"})
_CANDIDATE_VALUES = frozenset(
    {
        "DATA_BLOCKED",
        "RESEARCH_ONLY",
        "WATCH",
        "CONDITIONAL",
        "EXECUTION_BLOCKED",
    }
)


def _as_decimal(value: Any, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be decimal-compatible")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise TypeError(f"{field_name} must be decimal-compatible") from exc
    if not result.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return result


@dataclass(frozen=True)
class ChaseRiskPolicy:
    """Versioned thresholds for the production analysis projection."""

    policy_version: str = "EXTREME_EXTENSION_POLICY_V2"
    feature_set_version: str = "v4:daily-bar-chase-risk-v2"
    feature_builder_version: str = "v4:daily-bar-chase-risk-builder-v2"
    surge_daily_return_pct: Decimal = Decimal("9.5")
    surge_close_to_high_ratio: Decimal = Decimal("0.995")
    extreme_return_5d_pct: Decimal = Decimal("35")
    extreme_gap_pct: Decimal = Decimal("5")
    crowded_turnover_pct: Decimal = Decimal("20")
    peak_rebase_drawdown_pct: Decimal = Decimal("12")
    extreme_ma20_extension_pct: Decimal = Decimal("15")
    extreme_ma5_extension_atr: Decimal = Decimal("3")
    peak_cooldown_sessions: int = 10

    def __post_init__(self) -> None:
        for name in (
            "policy_version",
            "feature_set_version",
            "feature_builder_version",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be non-empty text")
            object.__setattr__(self, name, value.strip())
        for name in (
            "surge_daily_return_pct",
            "surge_close_to_high_ratio",
            "extreme_return_5d_pct",
            "extreme_gap_pct",
            "crowded_turnover_pct",
            "peak_rebase_drawdown_pct",
            "extreme_ma20_extension_pct",
            "extreme_ma5_extension_atr",
        ):
            value = _as_decimal(getattr(self, name), name)
            if value <= _ZERO:
                raise ValueError(f"{name} must be positive")
            if name == "surge_close_to_high_ratio" and value > Decimal("1"):
                raise ValueError("surge_close_to_high_ratio must not exceed one")
            object.__setattr__(self, name, value)
        if (
            type(self.peak_cooldown_sessions) is not int
            or self.peak_cooldown_sessions < 1
        ):
            raise ValueError("peak_cooldown_sessions must be a positive integer")


@dataclass(frozen=True)
class CanonicalChaseBar:
    """One already-authorized PIT daily bar consumed by production analysis."""

    record_id: str
    instrument: str
    session: date
    knowledge_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    previous_close: Decimal | None
    volume: Decimal
    amount: Decimal | None
    upper_limit: Decimal | None
    turnover_pct: Decimal | None = None
    explicit_capacity: bool | None = None
    suspended: bool = False
    quality_status: str = "PASS"

    def __post_init__(self) -> None:
        for field_name in ("record_id", "instrument"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be non-empty text")
            object.__setattr__(self, field_name, value.strip())
        if not isinstance(self.session, date) or isinstance(self.session, datetime):
            raise TypeError("session must be exactly a date")
        _require_aware(self.knowledge_time, "knowledge_time")
        for field_name in ("open", "high", "low", "close", "volume"):
            object.__setattr__(
                self,
                field_name,
                _as_decimal(getattr(self, field_name), field_name),
            )
        for field_name in (
            "previous_close",
            "amount",
            "upper_limit",
            "turnover_pct",
        ):
            value = getattr(self, field_name)
            object.__setattr__(
                self,
                field_name,
                None if value is None else _as_decimal(value, field_name),
            )
        if min(self.open, self.high, self.low, self.close) <= _ZERO:
            raise ValueError("daily bar prices must be positive")
        if self.volume < _ZERO:
            raise ValueError("daily bar volume must not be negative")
        if (
            self.high < max(self.open, self.close, self.low)
            or self.low > min(self.open, self.close, self.high)
        ):
            raise ValueError("daily bar OHLC range is inconsistent")
        if self.previous_close is not None and self.previous_close <= _ZERO:
            object.__setattr__(self, "previous_close", None)
        if self.amount is not None and self.amount < _ZERO:
            raise ValueError("amount must not be negative")
        if self.upper_limit is not None and self.upper_limit <= _ZERO:
            raise ValueError("upper_limit must be positive")
        if self.turnover_pct is not None and self.turnover_pct < _ZERO:
            raise ValueError("turnover_pct must not be negative")
        if self.explicit_capacity is not None and type(self.explicit_capacity) is not bool:
            raise TypeError("explicit_capacity must be a bool or None")
        if type(self.suspended) is not bool:
            raise TypeError("suspended must be a bool")
        quality = _quality_value(self.quality_status)
        object.__setattr__(self, "quality_status", quality)

    @property
    def exact_limit_up(self) -> bool | None:
        if self.upper_limit is None:
            return None
        return self.close == self.upper_limit

    @property
    def one_price_limit_up(self) -> bool:
        return bool(
            self.exact_limit_up
            and self.open == self.high == self.low == self.close
        )

    @property
    def has_price_discovery(self) -> bool:
        return not self.suspended and self.volume > _ZERO


@dataclass(frozen=True)
class ChaseRiskAssessment:
    """Frozen-policy-compatible result without a research-package type."""

    instrument: str
    cutoff: datetime
    bar_count: int
    surge_streak: int
    limit_streak: int | None
    peak_streak: int
    recent_peak_streak: int
    sessions_since_peak: int | None
    drawdown_from_peak_pct: Decimal | None
    cooldown_active: bool
    zero_volume: bool
    one_price_limit_up: bool
    has_verified_capacity: bool
    no_capacity: bool
    return_1d_pct: Decimal | None
    return_5d_pct: Decimal | None
    return_20d_pct: Decimal | None
    ma5: Decimal | None
    ma20: Decimal | None
    atr14: Decimal | None
    ma5_extension_pct: Decimal | None
    ma20_extension_pct: Decimal | None
    atr14_pct: Decimal | None
    ma5_extension_atr: Decimal | None
    ma20_extension_atr: Decimal | None
    gap_pct: Decimal | None
    crowding_detected: bool | None
    extreme_extension: bool
    ordinary_buy_eligible: bool
    candidate_status: str
    quality_status: str
    missing_fields: tuple[str, ...]
    reason_codes: tuple[str, ...]
    source_bars: tuple[CanonicalChaseBar, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, str) or not self.instrument.strip():
            raise ValueError("instrument must be non-empty text")
        _require_aware(self.cutoff, "cutoff")
        if self.bar_count < 1 or self.bar_count != len(self.source_bars):
            raise ValueError("bar_count must equal the non-empty source bar set")
        if self.surge_streak < 0:
            raise ValueError("surge_streak must not be negative")
        if self.limit_streak is not None and self.limit_streak < 0:
            raise ValueError("limit_streak must not be negative")
        if self.peak_streak < 0 or self.recent_peak_streak < 0:
            raise ValueError("peak streaks must not be negative")
        if self.sessions_since_peak is not None and self.sessions_since_peak < 0:
            raise ValueError("sessions_since_peak must not be negative")
        if self.no_capacity == self.has_verified_capacity:
            raise ValueError("no_capacity must negate has_verified_capacity")
        candidate = str(self.candidate_status).strip()
        if candidate not in _CANDIDATE_VALUES:
            raise ValueError("candidate_status is invalid")
        object.__setattr__(self, "instrument", self.instrument.strip())
        object.__setattr__(self, "candidate_status", candidate)
        object.__setattr__(self, "quality_status", _quality_value(self.quality_status))
        object.__setattr__(
            self,
            "missing_fields",
            tuple(sorted(set(self.missing_fields))),
        )
        object.__setattr__(
            self,
            "reason_codes",
            tuple(sorted(set(self.reason_codes))),
        )

    def feature_values(self, policy: ChaseRiskPolicy) -> Mapping[str, Any]:
        return {
            "policy_version": policy.policy_version,
            "bar_count": self.bar_count,
            "surge_streak": self.surge_streak,
            "limit_streak": self.limit_streak,
            "peak_streak": self.peak_streak,
            "recent_peak_streak": self.recent_peak_streak,
            "sessions_since_peak": self.sessions_since_peak,
            "drawdown_from_peak_pct": self.drawdown_from_peak_pct,
            "cooldown_active": self.cooldown_active,
            "zero_volume": self.zero_volume,
            "one_price_limit_up": self.one_price_limit_up,
            "has_verified_capacity": self.has_verified_capacity,
            "no_capacity": self.no_capacity,
            "return_1d_pct": self.return_1d_pct,
            "return_5d_pct": self.return_5d_pct,
            "return_20d_pct": self.return_20d_pct,
            "ma5": self.ma5,
            "ma20": self.ma20,
            "atr14": self.atr14,
            "ma5_extension_pct": self.ma5_extension_pct,
            "ma20_extension_pct": self.ma20_extension_pct,
            "atr14_pct": self.atr14_pct,
            "ma5_extension_atr": self.ma5_extension_atr,
            "ma20_extension_atr": self.ma20_extension_atr,
            "gap_pct": self.gap_pct,
            "crowding_detected": self.crowding_detected,
            "extreme_extension": self.extreme_extension,
            "ordinary_buy_eligible": self.ordinary_buy_eligible,
            "candidate_status": self.candidate_status,
        }


def assess_chase_risk(
    source_bars: tuple[CanonicalChaseBar, ...],
    *,
    instrument: str,
    cutoff: datetime,
    policy: ChaseRiskPolicy | None = None,
    dataset_quality: str = "PASS",
) -> ChaseRiskAssessment:
    """Evaluate canonical bars with the frozen V4 calculation."""

    if not isinstance(source_bars, tuple) or any(
        type(item) is not CanonicalChaseBar for item in source_bars
    ):
        raise TypeError("source_bars must be a tuple of CanonicalChaseBar")
    if not isinstance(instrument, str) or not instrument.strip():
        raise ValueError("instrument must be non-empty text")
    normalized_instrument = instrument.strip()
    _require_aware(cutoff, "cutoff")
    effective_policy = ChaseRiskPolicy() if policy is None else policy
    if type(effective_policy) is not ChaseRiskPolicy:
        raise TypeError("policy must be exactly ChaseRiskPolicy")
    normalized_dataset_quality = _quality_value(dataset_quality)
    bars = _select_bars(source_bars, normalized_instrument, cutoff)
    if not bars:
        raise ValueError("no point-in-time daily bars found for instrument")

    closes = tuple(bar.close for bar in bars)
    latest = bars[-1]
    if latest.previous_close is None:
        return_1d = return_5d = return_20d = None
    else:
        return_1d = _relative_pct(latest.close, latest.previous_close)
        return_5d = _period_return(closes, 5)
        return_20d = _period_return(closes, 20)
    ma5 = _moving_average(closes, 5)
    ma20 = _moving_average(closes, 20)
    atr14 = _average_true_range(bars, 14)
    ma5_extension = _relative_pct(latest.close, ma5)
    ma20_extension = _relative_pct(latest.close, ma20)
    atr14_pct = _ratio_pct(atr14, latest.close)
    ma5_extension_atr = _atr_extension(latest.close, ma5, atr14)
    ma20_extension_atr = _atr_extension(latest.close, ma20, atr14)
    gap_pct = _relative_pct(latest.open, latest.previous_close)

    surge_streak = _surge_streak(
        bars,
        effective_policy.surge_daily_return_pct,
        effective_policy.surge_close_to_high_ratio,
    )
    limit_streak = _exact_limit_streak(bars)
    (
        peak_streak,
        recent_peak_streak,
        sessions_since_peak,
        drawdown_from_peak,
    ) = _peak_risk_state(
        bars,
        effective_policy.surge_daily_return_pct,
        effective_policy.surge_close_to_high_ratio,
    )
    cooldown_active = bool(
        recent_peak_streak >= 3
        and sessions_since_peak is not None
        and sessions_since_peak < effective_policy.peak_cooldown_sessions
        and drawdown_from_peak is not None
        and drawdown_from_peak < effective_policy.peak_rebase_drawdown_pct
    )
    effective_streak = max(
        surge_streak,
        limit_streak or 0,
        recent_peak_streak if cooldown_active else 0,
    )
    zero_volume = latest.volume == _ZERO
    one_price_limit_up = latest.one_price_limit_up
    hard_capacity = bool(
        latest.volume > _ZERO
        and latest.amount is not None
        and latest.amount > _ZERO
        and not latest.suspended
        and not one_price_limit_up
    )
    has_capacity = bool(
        hard_capacity and latest.explicit_capacity is not False
    )
    no_capacity = not has_capacity
    crowding = (
        None
        if latest.turnover_pct is None
        else latest.turnover_pct >= effective_policy.crowded_turnover_pct
    )
    compound_extension = bool(
        return_5d is not None
        and return_5d >= effective_policy.extreme_return_5d_pct
        and gap_pct is not None
        and gap_pct >= effective_policy.extreme_gap_pct
        and crowding is True
    )
    ma_atr_extension = bool(
        (
            ma20_extension is not None
            and ma20_extension >= effective_policy.extreme_ma20_extension_pct
        )
        or (
            ma5_extension_atr is not None
            and ma5_extension_atr >= effective_policy.extreme_ma5_extension_atr
        )
    )
    extreme_extension = bool(
        effective_streak >= 4 or compound_extension or ma_atr_extension
    )

    missing_fields = [
        name
        for name, value in (
            ("return_1d_pct", return_1d),
            ("return_5d_pct", return_5d),
            ("return_20d_pct", return_20d),
            ("ma5", ma5),
            ("ma20", ma20),
            ("atr14", atr14),
            ("gap_pct", gap_pct),
            ("crowding_detected", crowding),
        )
        if value is None
    ]
    if limit_streak is None:
        missing_fields.append("limit_streak")
    if latest.previous_close is None:
        missing_fields.append("previous_close")
    if latest.amount is None:
        missing_fields.append("amount")

    reasons: list[str] = []
    if zero_volume:
        reasons.append("ZERO_VOLUME")
    if latest.suspended:
        reasons.append("SUSPENDED")
    if one_price_limit_up:
        reasons.append("ONE_PRICE_LIMIT_UP")
    if no_capacity:
        reasons.append("NO_VERIFIED_CAPACITY")
    if latest.previous_close is None:
        reasons.append("PREVIOUS_CLOSE_MISSING")
    if latest.amount is None:
        reasons.append("AMOUNT_MISSING")
    if limit_streak is None:
        reasons.append("LIMIT_RULE_MISSING")
    if effective_streak >= 4:
        reasons.append("FOUR_PLUS_LIMIT_OR_SURGE_STREAK")
    elif effective_streak == 3:
        reasons.append("THREE_LIMIT_OR_SURGE_STREAK")
    if compound_extension:
        reasons.append("RETURN_GAP_CROWDING_EXTENSION")
    if cooldown_active:
        reasons.append("PEAK_STREAK_COOLDOWN")
    if ma_atr_extension:
        reasons.append("MA_ATR_EXTREME_EXTENSION")
    if extreme_extension:
        reasons.append("EXTREME_EXTENSION")
    if missing_fields:
        reasons.append("INCOMPLETE_DAILY_BAR_WINDOW")
    if normalized_dataset_quality != "PASS":
        reasons.append("SOURCE_DATASET_NOT_PASS")
    if any(bar.quality_status != "PASS" for bar in bars):
        reasons.append("SOURCE_RECORD_NOT_PASS")

    if latest.amount is None:
        candidate_status = "DATA_BLOCKED"
    elif no_capacity:
        candidate_status = "EXECUTION_BLOCKED"
    elif extreme_extension or effective_streak >= 4:
        candidate_status = "WATCH"
    elif effective_streak == 3:
        candidate_status = "CONDITIONAL"
    elif limit_streak is None:
        candidate_status = "DATA_BLOCKED"
    else:
        candidate_status = "RESEARCH_ONLY"
    quality = _assessment_quality(
        normalized_dataset_quality,
        bars,
        missing_fields,
    )
    ordinary_buy_eligible = bool(
        candidate_status == "RESEARCH_ONLY"
        and quality == "PASS"
        and not extreme_extension
        and has_capacity
    )
    return ChaseRiskAssessment(
        instrument=normalized_instrument,
        cutoff=cutoff,
        bar_count=len(bars),
        surge_streak=surge_streak,
        limit_streak=limit_streak,
        peak_streak=peak_streak,
        recent_peak_streak=recent_peak_streak,
        sessions_since_peak=sessions_since_peak,
        drawdown_from_peak_pct=drawdown_from_peak,
        cooldown_active=cooldown_active,
        zero_volume=zero_volume,
        one_price_limit_up=one_price_limit_up,
        has_verified_capacity=has_capacity,
        no_capacity=no_capacity,
        return_1d_pct=return_1d,
        return_5d_pct=return_5d,
        return_20d_pct=return_20d,
        ma5=ma5,
        ma20=ma20,
        atr14=atr14,
        ma5_extension_pct=ma5_extension,
        ma20_extension_pct=ma20_extension,
        atr14_pct=atr14_pct,
        ma5_extension_atr=ma5_extension_atr,
        ma20_extension_atr=ma20_extension_atr,
        gap_pct=gap_pct,
        crowding_detected=crowding,
        extreme_extension=extreme_extension,
        ordinary_buy_eligible=ordinary_buy_eligible,
        candidate_status=candidate_status,
        quality_status=quality,
        missing_fields=tuple(missing_fields),
        reason_codes=tuple(reasons),
        source_bars=bars,
    )


def _select_bars(
    source_bars: tuple[CanonicalChaseBar, ...],
    instrument: str,
    cutoff: datetime,
) -> tuple[CanonicalChaseBar, ...]:
    selected: dict[date, CanonicalChaseBar] = {}
    for bar in source_bars:
        if (
            bar.instrument != instrument
            or bar.knowledge_time > cutoff
            or bar.session > cutoff.date()
        ):
            continue
        current = selected.get(bar.session)
        if current is None or bar.knowledge_time > current.knowledge_time:
            selected[bar.session] = bar
            continue
        if bar.knowledge_time < current.knowledge_time:
            continue
        if _bar_economic_payload(bar) != _bar_economic_payload(current):
            raise ValueError(
                "conflicting daily bars share one session and knowledge authority"
            )
        if bar.record_id < current.record_id:
            selected[bar.session] = bar
    return tuple(selected[session] for session in sorted(selected))


def _bar_economic_payload(bar: CanonicalChaseBar) -> tuple[object, ...]:
    return (
        bar.open,
        bar.high,
        bar.low,
        bar.close,
        bar.previous_close,
        bar.volume,
        bar.amount,
        bar.upper_limit,
        bar.turnover_pct,
        bar.explicit_capacity,
        bar.suspended,
    )


def _period_return(closes: tuple[Decimal, ...], sessions: int) -> Decimal | None:
    if len(closes) <= sessions:
        return None
    return _relative_pct(closes[-1], closes[-1 - sessions])


def _moving_average(closes: tuple[Decimal, ...], window: int) -> Decimal | None:
    if len(closes) < window:
        return None
    return sum(closes[-window:], _ZERO) / Decimal(window)


def _average_true_range(
    bars: tuple[CanonicalChaseBar, ...],
    window: int,
) -> Decimal | None:
    if len(bars) < window:
        return None
    ranges: list[Decimal] = []
    for bar in bars:
        candidates = [bar.high - bar.low]
        if bar.previous_close is not None:
            candidates.extend(
                (
                    abs(bar.high - bar.previous_close),
                    abs(bar.low - bar.previous_close),
                )
            )
        ranges.append(max(candidates))
    return sum(ranges[-window:], _ZERO) / Decimal(window)


def _relative_pct(current: Decimal, base: Decimal | None) -> Decimal | None:
    if base is None or base <= _ZERO:
        return None
    return (((current / base) - Decimal("1")) * _HUNDRED).quantize(
        _PCT_QUANTUM
    )


def _ratio_pct(
    numerator: Decimal | None,
    denominator: Decimal,
) -> Decimal | None:
    if numerator is None or denominator <= _ZERO:
        return None
    return ((numerator / denominator) * _HUNDRED).quantize(_PCT_QUANTUM)


def _atr_extension(
    close: Decimal,
    moving_average: Decimal | None,
    atr: Decimal | None,
) -> Decimal | None:
    if moving_average is None or atr is None or atr <= _ZERO:
        return None
    return ((close - moving_average) / atr).quantize(_PCT_QUANTUM)


def _bar_return(
    bars: tuple[CanonicalChaseBar, ...],
    index: int,
) -> Decimal | None:
    bar = bars[index]
    return _relative_pct(bar.close, bar.previous_close)


def _surge_streak(
    bars: tuple[CanonicalChaseBar, ...],
    threshold_pct: Decimal,
    close_to_high_ratio: Decimal,
) -> int:
    streak = 0
    for index in range(len(bars) - 1, -1, -1):
        bar = bars[index]
        if not bar.has_price_discovery:
            continue
        daily_return = _bar_return(bars, index)
        if (
            daily_return is None
            or daily_return < threshold_pct
            or bar.close / bar.high < close_to_high_ratio
        ):
            break
        streak += 1
    return streak


def _exact_limit_streak(bars: tuple[CanonicalChaseBar, ...]) -> int | None:
    streak = 0
    saw_price_discovery = False
    for bar in reversed(bars):
        if not bar.has_price_discovery:
            continue
        saw_price_discovery = True
        if bar.exact_limit_up is None:
            return None
        if not bar.exact_limit_up:
            return streak
        streak += 1
    return streak if saw_price_discovery else None


def _peak_risk_state(
    bars: tuple[CanonicalChaseBar, ...],
    threshold_pct: Decimal,
    close_to_high_ratio: Decimal,
) -> tuple[int, int, int | None, Decimal | None]:
    current = 0
    peak = 0
    recent_peak = 0
    recent_peak_end: int | None = None
    for index, bar in enumerate(bars):
        if not bar.has_price_discovery:
            continue
        daily_return = _bar_return(bars, index)
        conservative_surge = bool(
            daily_return is not None
            and daily_return >= threshold_pct
            and bar.close / bar.high >= close_to_high_ratio
        )
        if bar.exact_limit_up is True or conservative_surge:
            current += 1
            peak = max(peak, current)
            if current >= 3:
                recent_peak = current
                recent_peak_end = index
        else:
            current = 0
    if recent_peak_end is None:
        return peak, 0, None, None
    peak_close = bars[recent_peak_end].close
    drawdown = max(
        _ZERO,
        (
            (peak_close - bars[-1].close)
            / peak_close
            * _HUNDRED
        ).quantize(_PCT_QUANTUM),
    )
    return peak, recent_peak, len(bars) - 1 - recent_peak_end, drawdown


def _assessment_quality(
    dataset_quality: str,
    bars: tuple[CanonicalChaseBar, ...],
    missing_fields: list[str],
) -> str:
    if dataset_quality == "FAIL" or any(
        bar.quality_status == "FAIL" for bar in bars
    ):
        return "FAIL"
    if (
        dataset_quality == "WARN"
        or any(bar.quality_status == "WARN" for bar in bars)
        or missing_fields
    ):
        return "WARN"
    return "PASS"


def _quality_value(value: Any) -> str:
    normalized = str(value).strip()
    if normalized not in _QUALITY_VALUES:
        raise ValueError("quality_status is invalid")
    return normalized


def _require_aware(value: datetime, field_name: str) -> None:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


__all__ = [
    "CanonicalChaseBar",
    "ChaseRiskAssessment",
    "ChaseRiskPolicy",
    "assess_chase_risk",
]
