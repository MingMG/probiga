"""Point-in-time daily-bar and chase-risk factors.

This module is deliberately a pure function boundary.  It receives an
immutable :class:`AsOfDataset`, an explicit cutoff and an explicit validity
boundary.  It performs no I/O, reads no ambient clock and has no dependency on
the V2/V3 runtimes.

``limit_streak`` is exact only when an exchange-derived upper-limit price is
present for every bar needed to prove the trailing streak.  Missing rules are
reported as ``None``; they are never silently converted to zero.  The lower
``surge_streak`` threshold is intentionally conservative and is used only as a
risk backstop, never as a claim that a security actually closed limit-up.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from server.trading_v4.domain import (
    AsOfDataset,
    AsOfRecord,
    CandidateStatus,
    DataManifest,
    FeatureVector,
    QualityStatus,
    ScopeRef,
    ScopeType,
)


_ZERO = Decimal("0")
_HUNDRED = Decimal("100")
_PCT_QUANTUM = Decimal("0.000001")


@dataclass(frozen=True)
class ChaseRiskPolicy:
    """Versioned, deterministic thresholds for the first risk-only policy."""

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
class ChaseRiskAssessment:
    """Immutable intermediate result, independent of persistence contracts."""

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
    candidate_status: CandidateStatus
    quality_status: QualityStatus
    missing_fields: tuple[str, ...]
    reason_codes: tuple[str, ...]
    source_records: tuple[AsOfRecord, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, str) or not self.instrument.strip():
            raise ValueError("instrument must be non-empty text")
        _require_aware(self.cutoff, "cutoff")
        if self.bar_count < 1 or self.bar_count != len(self.source_records):
            raise ValueError("bar_count must equal the non-empty source record set")
        if self.surge_streak < 0:
            raise ValueError("surge_streak must not be negative")
        if self.limit_streak is not None and self.limit_streak < 0:
            raise ValueError("limit_streak must not be negative")
        if self.peak_streak < 0:
            raise ValueError("peak_streak must not be negative")
        if self.recent_peak_streak < 0:
            raise ValueError("recent_peak_streak must not be negative")
        if self.sessions_since_peak is not None and self.sessions_since_peak < 0:
            raise ValueError("sessions_since_peak must not be negative")
        if self.no_capacity == self.has_verified_capacity:
            raise ValueError("no_capacity must negate has_verified_capacity")
        object.__setattr__(self, "instrument", self.instrument.strip())
        object.__setattr__(self, "candidate_status", CandidateStatus(self.candidate_status))
        object.__setattr__(self, "quality_status", QualityStatus(self.quality_status))
        object.__setattr__(self, "missing_fields", tuple(sorted(set(self.missing_fields))))
        object.__setattr__(self, "reason_codes", tuple(sorted(set(self.reason_codes))))

    def feature_values(self, policy: ChaseRiskPolicy) -> Mapping[str, Any]:
        """Return the JSON-safe values carried by the domain FeatureVector."""

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
            "candidate_status": self.candidate_status.value,
        }


@dataclass(frozen=True)
class _DailyBar:
    record: AsOfRecord
    session: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    previous_close: Decimal | None
    volume: Decimal
    amount: Decimal | None
    upper_limit: Decimal | None
    turnover_pct: Decimal | None
    explicit_capacity: bool | None
    suspended: bool

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


def assess_chase_risk(
    dataset: AsOfDataset,
    *,
    instrument: str,
    cutoff: datetime | None = None,
    policy: ChaseRiskPolicy | None = None,
) -> ChaseRiskAssessment:
    """Compute daily-bar and chase-risk facts at an explicit PIT cutoff."""

    if type(dataset) is not AsOfDataset:
        raise TypeError("dataset must be exactly AsOfDataset")
    if not isinstance(instrument, str) or not instrument.strip():
        raise ValueError("instrument must be non-empty text")
    normalized_instrument = instrument.strip()
    effective_cutoff = dataset.as_of if cutoff is None else cutoff
    _require_aware(effective_cutoff, "cutoff")
    if effective_cutoff > dataset.as_of:
        raise ValueError("cutoff cannot exceed dataset.as_of")
    if policy is None:
        policy = ChaseRiskPolicy()
    elif type(policy) is not ChaseRiskPolicy:
        raise TypeError("policy must be exactly ChaseRiskPolicy")

    bars = _select_bars(dataset, normalized_instrument, effective_cutoff)
    if not bars:
        raise ValueError("no point-in-time daily bars found for instrument")

    closes = tuple(bar.close for bar in bars)
    latest = bars[-1]
    if latest.previous_close is None:
        # A zero/missing source pre-close is an explicit evidence gap (common
        # on no-trade placeholder rows).  Do not manufacture returns by
        # substituting another physical row's close.
        return_1d = None
        return_5d = None
        return_20d = None
    else:
        # The source row's previous_close is the authoritative previous
        # trading-session close.  The preceding physical record can be older
        # when a session is missing, suspended, or intentionally omitted.
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
    gap_pct = _latest_gap_pct(bars)

    surge_streak = _surge_streak(
        bars,
        policy.surge_daily_return_pct,
        policy.surge_close_to_high_ratio,
    )
    limit_streak = _exact_limit_streak(bars)
    (
        peak_streak,
        recent_peak_streak,
        sessions_since_peak,
        drawdown_from_peak,
    ) = _peak_risk_state(
        bars,
        policy.surge_daily_return_pct,
        policy.surge_close_to_high_ratio,
    )
    cooldown_active = bool(
        recent_peak_streak >= 3
        and sessions_since_peak is not None
        and sessions_since_peak < policy.peak_cooldown_sessions
        and drawdown_from_peak is not None
        and drawdown_from_peak < policy.peak_rebase_drawdown_pct
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
        else latest.turnover_pct >= policy.crowded_turnover_pct
    )
    compound_extension = bool(
        return_5d is not None
        and return_5d >= policy.extreme_return_5d_pct
        and gap_pct is not None
        and gap_pct >= policy.extreme_gap_pct
        and crowding is True
    )
    ma_atr_extension = bool(
        (
            ma20_extension is not None
            and ma20_extension >= policy.extreme_ma20_extension_pct
        )
        or (
            ma5_extension_atr is not None
            and ma5_extension_atr >= policy.extreme_ma5_extension_atr
        )
    )
    extreme_extension = (
        effective_streak >= 4 or compound_extension or ma_atr_extension
    )

    missing_fields: list[str] = []
    for name, value in (
        ("return_1d_pct", return_1d),
        ("return_5d_pct", return_5d),
        ("return_20d_pct", return_20d),
        ("ma5", ma5),
        ("ma20", ma20),
        ("atr14", atr14),
        ("gap_pct", gap_pct),
        ("crowding_detected", crowding),
    ):
        if value is None:
            missing_fields.append(name)
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
    if dataset.quality_status != QualityStatus.PASS:
        reasons.append("SOURCE_DATASET_NOT_PASS")
    if any(bar.record.quality_status != QualityStatus.PASS for bar in bars):
        reasons.append("SOURCE_RECORD_NOT_PASS")

    if latest.amount is None:
        candidate_status = CandidateStatus.DATA_BLOCKED
    elif no_capacity:
        candidate_status = CandidateStatus.EXECUTION_BLOCKED
    elif extreme_extension or effective_streak >= 4:
        candidate_status = CandidateStatus.WATCH
    elif effective_streak == 3:
        candidate_status = CandidateStatus.CONDITIONAL
    elif limit_streak is None:
        candidate_status = CandidateStatus.DATA_BLOCKED
    else:
        candidate_status = CandidateStatus.RESEARCH_ONLY

    quality = _assessment_quality(dataset, bars, missing_fields)
    ordinary_buy_eligible = bool(
        candidate_status == CandidateStatus.RESEARCH_ONLY
        and quality == QualityStatus.PASS
        and not extreme_extension
        and has_capacity
    )

    return ChaseRiskAssessment(
        instrument=normalized_instrument,
        cutoff=effective_cutoff,
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
        source_records=tuple(bar.record for bar in bars),
    )


def build_chase_risk_feature_vector(
    dataset: AsOfDataset,
    *,
    instrument: str,
    valid_until: datetime,
    cutoff: datetime | None = None,
    policy: ChaseRiskPolicy | None = None,
    data_manifest: DataManifest | None = None,
) -> FeatureVector:
    """Build a traceable V4 FeatureVector from a chase-risk assessment."""

    effective_policy = ChaseRiskPolicy() if policy is None else policy
    if type(effective_policy) is not ChaseRiskPolicy:
        raise TypeError("policy must be exactly ChaseRiskPolicy")
    assessment = assess_chase_risk(
        dataset,
        instrument=instrument,
        cutoff=cutoff,
        policy=effective_policy,
    )
    _require_aware(valid_until, "valid_until")
    if valid_until < assessment.cutoff:
        raise ValueError("valid_until cannot precede cutoff")

    source_hashes: dict[str, str] = {}
    for record in assessment.source_records:
        existing = source_hashes.get(record.record_id)
        if existing is not None and existing != record.record_hash:
            raise ValueError("selected source records reuse a record_id")
        source_hashes[record.record_id] = record.record_hash
    # Feature vectors and DecisionContext share the canonical DataManifest
    # identity.  The selected source hashes remain an exact subset proof.
    if data_manifest is None:
        # Convenience path only when this selected dataset is the complete
        # context manifest.  Multi-source callers must pass the full manifest.
        effective_manifest = DataManifest(source_hashes)
    else:
        if type(data_manifest) is not DataManifest:
            raise TypeError("data_manifest must be exactly DataManifest")
        if not data_manifest.contains_exact_subset(source_hashes):
            raise ValueError(
                "selected chase-risk records are absent from data_manifest"
            )
        effective_manifest = data_manifest
    return FeatureVector(
        scope=ScopeRef(ScopeType.INSTRUMENT, assessment.instrument),
        feature_set_version=effective_policy.feature_set_version,
        feature_builder_version=effective_policy.feature_builder_version,
        capability_name="daily_bar_chase_risk",
        source_manifest_hash=effective_manifest.manifest_hash,
        knowledge_time=assessment.cutoff,
        valid_until=valid_until,
        values=assessment.feature_values(effective_policy),
        source_record_ids=tuple(source_hashes),
        source_record_hashes=source_hashes,
        quality_status=assessment.quality_status,
        missing_fields=assessment.missing_fields,
        reason_codes=assessment.reason_codes,
    )


def _select_bars(
    dataset: AsOfDataset,
    instrument: str,
    cutoff: datetime,
) -> tuple[_DailyBar, ...]:
    selected: dict[date, AsOfRecord] = {}
    for record in dataset.records:
        if record.knowledge_time > cutoff:
            continue
        raw_instrument = record.payload.get("instrument")
        if raw_instrument != instrument:
            continue
        session = _session_date(record)
        if session > cutoff.date():
            continue
        if record.event_time is not None and record.event_time > cutoff:
            continue
        current = selected.get(session)
        if current is None:
            selected[session] = record
            continue
        # revision_id is opaque provenance, not a validated monotonic sequence.
        # Only knowledge_time carries ordering authority.
        authority = record.knowledge_time
        current_authority = current.knowledge_time
        if authority > current_authority:
            selected[session] = record
            continue
        if authority != current_authority:
            continue
        if _bar_economic_payload(record, session) != _bar_economic_payload(
            current,
            session,
        ):
            raise ValueError(
                "conflicting daily bars share one session and knowledge authority"
            )
        if (record.record_hash, record.record_id) < (
            current.record_hash,
            current.record_id,
        ):
            selected[session] = record
    return tuple(
        _parse_bar(selected[session], session)
        for session in sorted(selected)
    )


def _bar_economic_payload(record: AsOfRecord, session: date) -> tuple[object, ...]:
    bar = _parse_bar(record, session)
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


def _session_date(record: AsOfRecord) -> date:
    if record.event_time is not None:
        return record.event_time.date()
    raw = record.payload.get("trade_date")
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    if isinstance(raw, str):
        try:
            return date.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError("trade_date must be an ISO date") from exc
    raise ValueError("daily bar requires event_time or trade_date")


def _parse_bar(record: AsOfRecord, session: date) -> _DailyBar:
    payload = record.payload
    open_price = _required_decimal(payload, "open")
    high = _required_decimal(payload, "high")
    low = _required_decimal(payload, "low")
    close = _required_decimal(payload, "close")
    volume = _required_decimal(payload, "volume")
    if min(open_price, high, low, close) <= _ZERO:
        raise ValueError("daily bar prices must be positive")
    if volume < _ZERO:
        raise ValueError("daily bar volume must not be negative")
    if high < max(open_price, close, low) or low > min(open_price, close, high):
        raise ValueError("daily bar OHLC range is inconsistent")

    previous_close = _optional_decimal(payload, ("previous_close", "prev_close"))
    amount = _optional_decimal(payload, ("amount",))
    upper_limit = _optional_decimal(payload, ("upper_limit", "limit_up_price"))
    turnover = _optional_decimal(payload, ("turnover_pct", "turnover_rate"))
    if previous_close is not None and previous_close <= _ZERO:
        # Legacy no-trade placeholder rows use zero.  Preserve the row while
        # treating the unavailable comparison base as missing evidence.
        previous_close = None
    if amount is not None and amount < _ZERO:
        raise ValueError("amount must not be negative")
    if upper_limit is not None and upper_limit <= _ZERO:
        raise ValueError("upper_limit must be positive")
    if turnover is not None and turnover < _ZERO:
        raise ValueError("turnover_pct must not be negative")

    suspended = _optional_bool(payload, "is_suspended", default=False)
    explicit_capacity = _capacity_signal(payload)
    return _DailyBar(
        record=record,
        session=session,
        open=open_price,
        high=high,
        low=low,
        close=close,
        previous_close=previous_close,
        volume=volume,
        amount=amount,
        upper_limit=upper_limit,
        turnover_pct=turnover,
        explicit_capacity=explicit_capacity,
        suspended=suspended,
    )


def _required_decimal(payload: Mapping[str, Any], key: str) -> Decimal:
    if key not in payload:
        raise ValueError(f"daily bar requires {key}")
    return _as_decimal(payload[key], key)


def _optional_decimal(
    payload: Mapping[str, Any],
    keys: tuple[str, ...],
) -> Decimal | None:
    for key in keys:
        if key in payload and payload[key] is not None:
            return _as_decimal(payload[key], key)
    return None


def _as_decimal(value: Any, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be a decimal, not bool")
    try:
        converted = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"{field_name} must be a decimal") from exc
    if not converted.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return converted


def _optional_bool(
    payload: Mapping[str, Any],
    key: str,
    *,
    default: bool,
) -> bool:
    value = payload.get(key, default)
    if not isinstance(value, bool):
        raise TypeError(f"{key} must be a bool")
    return value


def _capacity_signal(payload: Mapping[str, Any]) -> bool | None:
    for key in ("verified_capacity", "tradable_capacity"):
        if key not in payload or payload[key] is None:
            continue
        value = payload[key]
        if isinstance(value, bool):
            return value
        return _as_decimal(value, key) > _ZERO
    return None


def _period_return(closes: tuple[Decimal, ...], sessions: int) -> Decimal | None:
    if len(closes) <= sessions:
        return None
    return _relative_pct(closes[-1], closes[-1 - sessions])


def _moving_average(closes: tuple[Decimal, ...], window: int) -> Decimal | None:
    if len(closes) < window:
        return None
    return sum(closes[-window:], _ZERO) / Decimal(window)


def _average_true_range(
    bars: tuple[_DailyBar, ...],
    window: int,
) -> Decimal | None:
    if len(bars) < window:
        return None
    ranges: list[Decimal] = []
    for bar in bars:
        previous_close = bar.previous_close
        candidates = [bar.high - bar.low]
        if previous_close is not None:
            candidates.extend(
                (abs(bar.high - previous_close), abs(bar.low - previous_close))
            )
        ranges.append(max(candidates))
    return sum(ranges[-window:], _ZERO) / Decimal(window)


def _relative_pct(current: Decimal, base: Decimal | None) -> Decimal | None:
    if base is None or base <= _ZERO:
        return None
    return (((current / base) - Decimal("1")) * _HUNDRED).quantize(
        _PCT_QUANTUM
    )


def _ratio_pct(numerator: Decimal | None, denominator: Decimal) -> Decimal | None:
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


def _bar_return(bars: tuple[_DailyBar, ...], index: int) -> Decimal | None:
    bar = bars[index]
    previous_close = bar.previous_close
    return _relative_pct(bar.close, previous_close)


def _surge_streak(
    bars: tuple[_DailyBar, ...],
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


def _exact_limit_streak(bars: tuple[_DailyBar, ...]) -> int | None:
    streak = 0
    saw_price_discovery = False
    for bar in reversed(bars):
        if not bar.has_price_discovery:
            continue
        saw_price_discovery = True
        exact = bar.exact_limit_up
        if exact is None:
            return None
        if not exact:
            return streak
        streak += 1
    return streak if saw_price_discovery else None


def _peak_risk_state(
    bars: tuple[_DailyBar, ...],
    threshold_pct: Decimal,
    close_to_high_ratio: Decimal,
) -> tuple[int, int, int | None, Decimal | None]:
    """Return global peak plus the most recent dangerous episode state."""

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
            if current >= peak:
                peak = current
            if current >= 3:
                recent_peak = current
                recent_peak_end = index
        else:
            current = 0
    if recent_peak_end is None:
        return peak, 0, None, None
    peak_close = bars[recent_peak_end].close
    latest_close = bars[-1].close
    drawdown = max(
        _ZERO,
        ((peak_close - latest_close) / peak_close * _HUNDRED).quantize(
            _PCT_QUANTUM
        ),
    )
    return peak, recent_peak, len(bars) - 1 - recent_peak_end, drawdown


def _latest_gap_pct(bars: tuple[_DailyBar, ...]) -> Decimal | None:
    latest = bars[-1]
    previous_close = latest.previous_close
    return _relative_pct(latest.open, previous_close)


def _assessment_quality(
    dataset: AsOfDataset,
    bars: tuple[_DailyBar, ...],
    missing_fields: list[str],
) -> QualityStatus:
    if dataset.quality_status == QualityStatus.FAIL or any(
        bar.record.quality_status == QualityStatus.FAIL for bar in bars
    ):
        return QualityStatus.FAIL
    if (
        dataset.quality_status == QualityStatus.WARN
        or any(bar.record.quality_status == QualityStatus.WARN for bar in bars)
        or missing_fields
    ):
        return QualityStatus.WARN
    return QualityStatus.PASS


def _require_aware(value: datetime, field_name: str) -> None:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
