"""Auditable intraday activation for V2 paper trading.

The normal lane promotes immutable daily watch candidates.  A separately
versioned market-wide reversal radar may also discover an A-share outside that
pool, but it can only create a small ProBigA paper probe after complete QMT
minute history, a deep intraday washout, price/volume recovery, theme breadth
and live risk/reward all confirm.  Real-order submission remains prohibited.
"""
from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from statistics import median
from typing import Any

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

from server.common.kline_data import get_kline_engine
from server.common.current_data import get_current_engine
from server.common.minute_data import (
    get_minute_engine,
    get_minute_stock_table,
    minute_source_info,
)

from .bootstrap import ACCOUNT_ID
from .calendar import is_trade_day
from .config import canonical_json_hash, load_frozen_json
from .domain import decimal_value
from .planner import persist_portfolio_competition
from .position_monitor import monitor_positions
from .public_quote_failover import (
    collect_public_quote_failover,
    load_latest_public_quote_snapshot,
    qmt_primary_health,
)


CONFIG_PATH = "strategies/intraday_activation_v2.json"
logger = logging.getLogger(__name__)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError):
        return default


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _parse_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _hhmm(value: str) -> int:
    hour, minute = str(value).split(":", 1)
    return int(hour) * 100 + int(minute)


def _expected_session_minutes(now: datetime) -> int:
    """Return elapsed A-share minute bars, including each session's first bar."""
    intervals = (
        (
            datetime.combine(now.date(), time(9, 30)),
            datetime.combine(now.date(), time(11, 30)),
        ),
        (
            datetime.combine(now.date(), time(13, 0)),
            datetime.combine(now.date(), time(15, 0)),
        ),
    )
    count = 0
    for start, end in intervals:
        if now < start:
            continue
        observed_end = min(now.replace(second=0, microsecond=0), end)
        if observed_end >= start:
            count += int((observed_end - start).total_seconds() // 60) + 1
    return count


@dataclass(frozen=True)
class MarketPoint:
    observed_at: datetime
    observed_count: int
    expected_count: int
    coverage: float
    positive_breadth_pct: float
    equal_weight_return_pct: float
    median_return_pct: float
    source: str


@dataclass(frozen=True)
class MarketAssessment:
    state: str
    quality_status: str
    actionable: bool
    execution_regime: str
    confirming_points: int
    evidence: tuple[str, ...]


@dataclass(frozen=True)
class CandidateAssessment:
    stock_code: str
    short_name: str
    theme_code: str
    theme_name: str
    source_strategy_version: str
    role: str
    action: str
    status: str
    reason_code: str
    current_price: float
    current_return_pct: float
    relative_strength_pct: float
    intraday_amount_ratio: float
    theme_positive_breadth_pct: float
    theme_average_return_pct: float
    raw_score: float
    risk_reward_ratio: float
    leader_code: str
    leader_state: str
    opening_target_fraction: float
    evidence: tuple[str, ...]


def _assess_market_primary_legacy(
    points: list[MarketPoint],
    *,
    previous_regime: str,
    now: datetime,
    config: dict[str, Any],
) -> MarketAssessment:
    """Confirm recovery from multiple fresh, sufficiently complete QMT points."""
    quality = config["data_quality"]
    thresholds = config["market_confirmation"]
    required_points = int(quality["confirmation_points"])
    ordered = sorted(points, key=lambda item: item.observed_at)
    evidence: list[str] = []
    if len(ordered) < required_points:
        return MarketAssessment(
            state="DATA_BLOCKED",
            quality_status="BLOCK",
            actionable=False,
            execution_regime="DATA_BLOCKED",
            confirming_points=0,
            evidence=(f"分钟确认点不足：{len(ordered)}/{required_points}",),
        )
    ordered = ordered[-required_points:]
    latest = ordered[-1]
    age_seconds = max(0.0, (now - latest.observed_at).total_seconds())
    gaps = [
        (right.observed_at - left.observed_at).total_seconds()
        for left, right in zip(ordered, ordered[1:])
    ]
    if latest.observed_count < int(quality["minimum_observed_stocks"]):
        evidence.append(
            f"QMT有效股票数不足：{latest.observed_count}/"
            f"{quality['minimum_observed_stocks']}"
        )
    if latest.coverage < float(quality["minimum_universe_coverage"]):
        evidence.append(
            f"QMT覆盖率不足：{latest.coverage:.1%}/"
            f"{float(quality['minimum_universe_coverage']):.1%}"
        )
    if age_seconds > float(quality["maximum_minute_age_seconds"]):
        evidence.append(
            f"最新分钟数据过期：{age_seconds:.0f}秒"
        )
    if gaps and max(gaps) > float(quality["maximum_point_gap_seconds"]):
        evidence.append(
            f"分钟确认点不连续：最大间隔{max(gaps):.0f}秒"
        )
    required_provider = str(quality["required_provider"]).upper()
    if str(latest.source or "").upper() != required_provider:
        evidence.append(
            f"分钟来源未补证：{latest.source or 'UNKNOWN'}，"
            f"要求{required_provider}"
        )
    average_breadth = sum(
        item.positive_breadth_pct for item in ordered
    ) / len(ordered)
    confirming_points = sum(
        item.positive_breadth_pct
        >= float(thresholds["confirming_point_breadth_pct_gte"])
        for item in ordered
    )
    broad_confirmation = (
        latest.positive_breadth_pct
        >= float(thresholds["latest_positive_breadth_pct_gte"])
        and average_breadth
        >= float(thresholds["average_positive_breadth_pct_gte"])
        and latest.equal_weight_return_pct
        >= float(thresholds["latest_equal_weight_return_pct_gte"])
        and confirming_points
        >= int(thresholds["minimum_confirming_points"])
    )
    evidence.extend(
        [
            f"最新上涨家数占比{latest.positive_breadth_pct:.2f}%",
            f"{len(ordered)}点平均上涨家数占比{average_breadth:.2f}%",
            f"等权平均涨幅{latest.equal_weight_return_pct:.3f}%",
            f"确认点{confirming_points}/{len(ordered)}",
        ]
    )
    if len(evidence) > 4:
        # A data-quality failure is fail-closed even if the partial sample
        # visibly resembles a broad rally.
        if broad_confirmation:
            evidence.append("部分样本显示普涨，但数据门槛未通过，只观察")
        return MarketAssessment(
            state="DATA_BLOCKED",
            quality_status="BLOCK",
            actionable=False,
            execution_regime="DATA_BLOCKED",
            confirming_points=confirming_points,
            evidence=tuple(evidence),
        )
    if not broad_confirmation:
        return MarketAssessment(
            state="OBSERVING",
            quality_status="PASS",
            actionable=False,
            execution_regime="RANGE",
            confirming_points=confirming_points,
            evidence=tuple(evidence + ["普涨确认条件尚未同时成立"]),
        )
    previous = str(previous_regime or "").upper()
    panic_regimes = {
        str(item).upper()
        for item in thresholds["panic_recovery_previous_regimes"]
    }
    if previous in panic_regimes:
        state = "PANIC_RECOVERY_CONFIRMED"
        execution_regime = "PANIC_RECOVERY"
        evidence.append(f"前一市场状态为{previous}，盘中确认恐慌修复")
    else:
        state = "BROAD_RALLY_CONFIRMED"
        execution_regime = "TREND_UP"
        evidence.append("盘中多点确认广度与等权收益同步转强")
    return MarketAssessment(
        state=state,
        quality_status="PASS",
        actionable=True,
        execution_regime=execution_regime,
        confirming_points=confirming_points,
        evidence=tuple(evidence),
    )


def assess_market(
    points: list[MarketPoint],
    *,
    previous_regime: str,
    now: datetime,
    config: dict[str, Any],
) -> MarketAssessment:
    """Assess QMT first and an independently attested public quorum second."""
    primary_quality = dict(config["data_quality"])
    failover = dict(config.get("public_quote_failover") or {})
    failover_provider = str(
        failover.get("source_provider") or "PUBLIC_QUOTE_QUORUM_V1"
    ).upper()
    ordered = sorted(points, key=lambda item: item.observed_at)
    latest_source = (
        str(ordered[-1].source or "").upper() if ordered else ""
    )
    using_public_failover = bool(
        failover.get("enabled")
        and latest_source == failover_provider
    )
    quality = (
        {
            **primary_quality,
            "minimum_observed_stocks": int(
                failover.get("minimum_observed_stocks") or 5000
            ),
            "minimum_universe_coverage": float(
                failover.get("minimum_universe_coverage") or 0.95
            ),
            "maximum_minute_age_seconds": float(
                failover.get("maximum_snapshot_age_seconds") or 45
            ),
            "maximum_point_gap_seconds": float(
                failover.get("maximum_point_gap_seconds") or 90
            ),
        }
        if using_public_failover
        else primary_quality
    )
    thresholds = config["market_confirmation"]
    required_points = int(primary_quality["confirmation_points"])
    evidence: list[str] = []
    if len(ordered) < required_points:
        return MarketAssessment(
            state="DATA_BLOCKED",
            quality_status="BLOCK",
            actionable=False,
            execution_regime="DATA_BLOCKED",
            confirming_points=0,
            evidence=(
                f"分钟确认点不足：{len(ordered)}/{required_points}",
            ),
        )
    ordered = ordered[-required_points:]
    latest = ordered[-1]
    age_seconds = max(
        0.0,
        (now - latest.observed_at).total_seconds(),
    )
    gaps = [
        (right.observed_at - left.observed_at).total_seconds()
        for left, right in zip(ordered, ordered[1:])
    ]
    source_label = (
        "公共双源替补" if using_public_failover else "QMT"
    )
    if latest.observed_count < int(quality["minimum_observed_stocks"]):
        evidence.append(
            f"{source_label}有效股票数不足：{latest.observed_count}/"
            f"{quality['minimum_observed_stocks']}"
        )
    if latest.coverage < float(quality["minimum_universe_coverage"]):
        evidence.append(
            f"{source_label}覆盖率不足：{latest.coverage:.1%}/"
            f"{float(quality['minimum_universe_coverage']):.1%}"
        )
    if age_seconds > float(quality["maximum_minute_age_seconds"]):
        evidence.append(
            f"最新行情数据过期：{age_seconds:.0f}秒"
        )
    if gaps and max(gaps) > float(quality["maximum_point_gap_seconds"]):
        evidence.append(
            f"分钟确认点不连续：最大间隔{max(gaps):.0f}秒"
        )
    required_provider = str(
        primary_quality["required_provider"]
    ).upper()
    accepted_providers = {required_provider}
    if bool(failover.get("enabled")):
        accepted_providers.add(failover_provider)
    if latest_source not in accepted_providers:
        evidence.append(
            f"分钟来源未补证：{latest.source or 'UNKNOWN'}，"
            f"要求{required_provider}或{failover_provider}"
        )
    if using_public_failover:
        evidence.append(
            "QMT不可用，当前采用新浪与腾讯双源一致性替补；"
            "仅用于ProBigA模拟盘降仓试错，Level-1能力保持关闭"
        )

    average_breadth = sum(
        item.positive_breadth_pct for item in ordered
    ) / len(ordered)
    confirming_points = sum(
        item.positive_breadth_pct
        >= float(thresholds["confirming_point_breadth_pct_gte"])
        for item in ordered
    )
    broad_confirmation = (
        latest.positive_breadth_pct
        >= float(thresholds["latest_positive_breadth_pct_gte"])
        and average_breadth
        >= float(thresholds["average_positive_breadth_pct_gte"])
        and latest.equal_weight_return_pct
        >= float(thresholds["latest_equal_weight_return_pct_gte"])
        and confirming_points
        >= int(thresholds["minimum_confirming_points"])
    )
    market_evidence = [
        f"最新上涨家数占比{latest.positive_breadth_pct:.2f}%",
        f"{len(ordered)}点平均上涨家数占比{average_breadth:.2f}%",
        f"等权平均涨幅{latest.equal_weight_return_pct:.3f}%",
        f"确认点{confirming_points}/{len(ordered)}",
    ]
    quality_failed = any(
        (
            latest.observed_count
            < int(quality["minimum_observed_stocks"]),
            latest.coverage
            < float(quality["minimum_universe_coverage"]),
            age_seconds
            > float(quality["maximum_minute_age_seconds"]),
            bool(
                gaps
                and max(gaps)
                > float(quality["maximum_point_gap_seconds"])
            ),
            latest_source not in accepted_providers,
        )
    )
    evidence.extend(market_evidence)
    if quality_failed:
        if broad_confirmation:
            evidence.append(
                "部分样本显示普涨，但数据门禁未通过，只观察不买"
            )
        return MarketAssessment(
            state="DATA_BLOCKED",
            quality_status="BLOCK",
            actionable=False,
            execution_regime="DATA_BLOCKED",
            confirming_points=confirming_points,
            evidence=tuple(evidence),
        )
    if not broad_confirmation:
        return MarketAssessment(
            state="OBSERVING",
            quality_status="PASS",
            actionable=False,
            execution_regime="RANGE",
            confirming_points=confirming_points,
            evidence=tuple(
                evidence
                + ["普涨确认条件尚未同时成立"]
            ),
        )
    previous = str(previous_regime or "").upper()
    panic_regimes = {
        str(item).upper()
        for item in thresholds["panic_recovery_previous_regimes"]
    }
    if previous in panic_regimes:
        state = "PANIC_RECOVERY_CONFIRMED"
        execution_regime = "PANIC_RECOVERY"
        evidence.append(
            f"前一市场状态为{previous}，盘中确认恐慌修复"
        )
    else:
        state = "BROAD_RALLY_CONFIRMED"
        execution_regime = "TREND_UP"
        evidence.append(
            "盘中多点确认宽度与等权收益同步转强"
        )
    return MarketAssessment(
        state=state,
        quality_status="PASS",
        actionable=True,
        execution_regime=execution_regime,
        confirming_points=confirming_points,
        evidence=tuple(evidence),
    )


def assess_candidate(
    candidate: dict[str, Any],
    *,
    market: MarketAssessment,
    market_return_pct: float,
    quote: dict[str, Any] | None,
    amount_ratio: float,
    theme_metrics: dict[str, float],
    leader_code: str,
    leader_score: float,
    leader_unavailable: bool,
    config: dict[str, Any],
) -> CandidateAssessment:
    """Evaluate one watch candidate without mutating portfolio state."""
    raw = _parse_json_object(candidate.get("raw_features_json"))
    code = str(candidate.get("stock_code") or "").zfill(6)
    short_name = str(
        raw.get("stock_name")
        or candidate.get("short_name")
        or code
    )
    theme_code = str(candidate.get("theme_code") or "")
    theme_name = str(raw.get("theme_name") or theme_code or "未分类")
    role = str(raw.get("sector_role") or "")
    score = _float(candidate.get("raw_score"))
    thresholds = config["candidate_activation"]
    substitute = config["leader_substitution"]
    price = _float((quote or {}).get("price"))
    pre_close = _float((quote or {}).get("pre_close"))
    current_return = (
        (price / pre_close - 1.0) * 100.0
        if price > 0 and pre_close > 0
        else 0.0
    )
    relative_strength = current_return - market_return_pct
    stop = _float(candidate.get("initial_stop") or raw.get("stop_loss"))
    target = _float(raw.get("take_profit_2"))
    risk_reward = (
        (target - price) / max(price - stop, 1e-9)
        if target > price > stop > 0
        else 0.0
    )
    reference = _float(raw.get("db_close") or raw.get("entry_low"))
    distance = (
        (price / reference - 1.0) * 100.0
        if price > 0 and reference > 0
        else 999.0
    )
    near_limit = bool((quote or {}).get("near_limit_up"))
    allowed_substitute_roles = set(substitute["allowed_roles"])
    is_substitute_candidate = (
        leader_unavailable
        and code != leader_code
        and role in allowed_substitute_roles
    )
    reasons: list[tuple[str, str]] = []
    if not market.actionable:
        reasons.append(
            ("MARKET_NOT_CONFIRMED", "市场盘中修复尚未通过完整数据确认")
        )
    if not quote or price <= 0 or pre_close <= 0:
        reasons.append(("QMT_QUOTE_MISSING", "没有可用的QMT分钟价格"))
    if near_limit:
        reasons.append(("LEADER_LIMIT_LOCKED", "价格接近涨停，当前不追板"))
    if "ST" in short_name.upper() or "退" in short_name:
        reasons.append(("SPECIAL_TREATMENT_BLOCKED", "ST或退市整理股票禁止试仓"))
    if score < float(thresholds["minimum_raw_score"]):
        reasons.append(("RAW_SCORE_TOO_LOW", "盘前候选分不足"))
    if current_return < float(thresholds["minimum_current_return_pct"]):
        reasons.append(("INTRADAY_STRENGTH_TOO_LOW", "个股盘中强度不足"))
    if current_return > float(thresholds["maximum_current_return_pct"]):
        reasons.append(("INTRADAY_TOO_EXTENDED", "个股盘中涨幅过大，避免追高"))
    if relative_strength < float(
        thresholds["minimum_relative_strength_pct"]
    ):
        reasons.append(("RELATIVE_STRENGTH_TOO_LOW", "个股没有明显跑赢市场"))
    if leader_unavailable and code != leader_code:
        if role not in allowed_substitute_roles:
            reasons.append(
                (
                    "SUBSTITUTE_ROLE_NOT_ALLOWED",
                    "龙一不可成交，但该股票不是允许递补的龙二、中军或低位替补",
                )
            )
        elif score - leader_score < float(
            substitute["minimum_score_gap_from_leader"]
        ):
            reasons.append(
                (
                    "SUBSTITUTE_SCORE_GAP_TOO_LARGE",
                    "递补股票与龙一的盘前评分差距过大",
                )
            )
        elif relative_strength < float(
            substitute["minimum_relative_strength_pct"]
        ):
            reasons.append(
                (
                    "SUBSTITUTE_RELATIVE_STRENGTH_TOO_LOW",
                    "递补股票盘中强度不足，不能只因龙一买不到就套利",
                )
            )
    if amount_ratio < float(thresholds["minimum_intraday_amount_ratio"]):
        reasons.append(("INTRADAY_VOLUME_NOT_CONFIRMED", "当前分钟量能没有放大"))
    if risk_reward < float(thresholds["minimum_risk_reward_ratio"]):
        reasons.append(("RISK_REWARD_TOO_LOW", "按当前价格计算的盈亏比不足"))
    if distance > float(thresholds["maximum_distance_above_reference_pct"]):
        reasons.append(("REFERENCE_PRICE_TOO_EXTENDED", "距离盘前参考价过远"))
    theme_count = int(theme_metrics.get("observed_count") or 0)
    theme_breadth = _float(theme_metrics.get("positive_breadth_pct"))
    theme_return = _float(theme_metrics.get("average_return_pct"))
    if theme_count < int(thresholds["minimum_theme_member_observations"]):
        reasons.append(("THEME_SAMPLE_TOO_SMALL", "板块可观测成员不足"))
    if theme_breadth < float(
        thresholds["minimum_theme_positive_breadth_pct"]
    ):
        reasons.append(("THEME_BREADTH_NOT_CONFIRMED", "板块上涨宽度不足"))
    if theme_return < float(thresholds["minimum_theme_average_return_pct"]):
        reasons.append(("THEME_RETURN_NOT_CONFIRMED", "板块平均涨幅不足"))

    action = "WATCH"
    status = "WATCHING"
    reason_code = reasons[0][0] if reasons else "ALL_INTRADAY_GATES_PASSED"
    if not reasons:
        if leader_unavailable and code == leader_code:
            action = "REJECT"
            status = "REJECTED"
            reason_code = "LEADER_UNAVAILABLE"
        elif is_substitute_candidate:
            action = "ACTIVATE_SUBSTITUTE"
            status = "ACTIVATABLE"
            reason_code = "LEADER_UNAVAILABLE_CORE_SUBSTITUTE"
        else:
            action = "ACTIVATE_PROBE"
            status = "ACTIVATABLE"
            reason_code = "WATCH_STOCK_INTRADAY_OUTPERFORMANCE"
    opening_fraction = float(
        thresholds[
            "substitute_opening_target_fraction"
            if action == "ACTIVATE_SUBSTITUTE"
            else "normal_opening_target_fraction"
        ]
    )
    evidence = [
        f"个股涨幅{current_return:.2f}%，相对市场{relative_strength:.2f}%",
        f"分钟量能倍率{amount_ratio:.2f}",
        f"板块上涨宽度{theme_breadth:.2f}%，平均涨幅{theme_return:.2f}%",
        f"动态盈亏比{risk_reward:.2f}",
    ]
    if reasons:
        evidence.append(reasons[0][1])
    return CandidateAssessment(
        stock_code=code,
        short_name=short_name,
        theme_code=theme_code,
        theme_name=theme_name,
        source_strategy_version=str(candidate.get("strategy_version") or ""),
        role=role,
        action=action,
        status=status,
        reason_code=reason_code,
        current_price=price,
        current_return_pct=current_return,
        relative_strength_pct=relative_strength,
        intraday_amount_ratio=amount_ratio,
        theme_positive_breadth_pct=theme_breadth,
        theme_average_return_pct=theme_return,
        raw_score=score,
        risk_reward_ratio=risk_reward,
        leader_code=leader_code,
        leader_state=(
            "UNAVAILABLE_LIMIT_LOCKED"
            if leader_unavailable
            else "TRADEABLE"
        ),
        opening_target_fraction=opening_fraction,
        evidence=tuple(evidence),
    )


def assess_reversal_candidate(
    candidate: dict[str, Any],
    *,
    market: MarketAssessment,
    market_return_pct: float,
    market_breadth_pct: float,
    quote: dict[str, Any] | None,
    theme_metrics: dict[str, float],
    config: dict[str, Any],
) -> CandidateAssessment:
    """Evaluate one market-wide waterline reversal without portfolio writes."""
    raw = _parse_json_object(candidate.get("raw_features_json"))
    thresholds = config["market_wide_reversal_radar"]
    code = str(candidate.get("stock_code") or "").zfill(6)
    short_name = str(
        raw.get("stock_name")
        or candidate.get("short_name")
        or code
    )
    theme_code = str(candidate.get("theme_code") or "")
    theme_name = str(raw.get("theme_name") or theme_code or "未分类")
    price = _float((quote or {}).get("price"))
    pre_close = _float((quote or {}).get("pre_close"))
    current_return = (
        (price / pre_close - 1.0) * 100.0
        if price > 0 and pre_close > 0
        else 0.0
    )
    relative_strength = current_return - market_return_pct
    session_low_return = _float(raw.get("session_low_return_pct"))
    rebound = _float(raw.get("rebound_from_low_pct"))
    momentum_10m = _float(raw.get("momentum_10m_pct"))
    momentum_5m = _float(raw.get("momentum_5m_pct"))
    amount_ratio = _float(raw.get("intraday_amount_ratio"))
    history_coverage = _float(raw.get("minute_history_coverage"))
    history_age = _float(raw.get("minute_history_age_seconds"), 999999.0)
    stop = _float(candidate.get("initial_stop"))
    target = _float(raw.get("take_profit_2"))
    risk_reward = (
        (target - price) / max(price - stop, 1e-9)
        if target > price > stop > 0
        else 0.0
    )
    theme_count = int(theme_metrics.get("observed_count") or 0)
    theme_breadth = _float(theme_metrics.get("positive_breadth_pct"))
    theme_return = _float(theme_metrics.get("average_return_pct"))
    theme_relative = theme_return - market_return_pct
    discovery_lane = str(raw.get("discovery_lane") or "")
    if discovery_lane == "MARKET_WIDE_LEADER_SUBSTITUTE":
        rule = dict(config.get("leader_follower_radar") or {})
        leader_code = str(raw.get("leader_code") or "").zfill(6)
        leader_name = str(raw.get("leader_name") or leader_code)
        near_limit = bool((quote or {}).get("near_limit_up"))
        reasons: list[tuple[str, str]] = []
        if market.quality_status != "PASS":
            reasons.append(
                ("SUBSTITUTE_MARKET_DATA_BLOCKED", "市场数据质量未通过")
            )
        if market_breadth_pct < float(
            rule.get("minimum_market_positive_breadth_pct", 50.0)
        ):
            reasons.append(
                ("SUBSTITUTE_MARKET_BREADTH_LOW", "市场上涨宽度不足")
            )
        if near_limit:
            reasons.append(("SUBSTITUTE_LIMIT_LOCKED", "龙二也接近涨停，禁止追板"))
        if current_return < float(
            rule.get("minimum_current_return_pct", 0.3)
        ):
            reasons.append(("SUBSTITUTE_STRENGTH_LOW", "龙二盘中强度不足"))
        if current_return > float(
            rule.get("maximum_current_return_pct", 7.5)
        ):
            reasons.append(("SUBSTITUTE_TOO_EXTENDED", "龙二涨幅过高，套利空间不足"))
        if theme_count < int(
            rule.get("minimum_theme_member_observations", 5)
        ):
            reasons.append(("SUBSTITUTE_THEME_SAMPLE_SMALL", "板块可观测成员不足"))
        if theme_breadth < float(
            rule.get("minimum_theme_positive_breadth_pct", 60.0)
        ):
            reasons.append(("SUBSTITUTE_THEME_BREADTH_LOW", "板块扩散宽度不足"))
        if theme_return < float(
            rule.get("minimum_theme_average_return_pct", 0.3)
        ):
            reasons.append(("SUBSTITUTE_THEME_RETURN_LOW", "板块平均涨幅不足"))
        if relative_strength < float(
            rule.get("minimum_relative_strength_pct", 0.2)
        ):
            reasons.append(("SUBSTITUTE_RELATIVE_STRENGTH_LOW", "龙二没有跑赢市场"))
        if risk_reward < float(rule.get("minimum_risk_reward_ratio", 1.3)):
            reasons.append(("SUBSTITUTE_RISK_REWARD_LOW", "龙二剩余盈亏比不足"))
        action = "WATCH" if reasons else "ACTIVATE_SUBSTITUTE"
        status = "WATCHING" if reasons else "ACTIVATABLE"
        reason_code = (
            reasons[0][0]
            if reasons
            else "LOCKED_LEADER_FOLLOWER_CONFIRMED"
        )
        return CandidateAssessment(
            stock_code=code,
            short_name=short_name,
            theme_code=theme_code,
            theme_name=theme_name,
            source_strategy_version=str(
                candidate.get("strategy_version") or ""
            ),
            role=str(raw.get("sector_role") or "龙二"),
            action=action,
            status=status,
            reason_code=reason_code,
            current_price=price,
            current_return_pct=current_return,
            relative_strength_pct=relative_strength,
            intraday_amount_ratio=0.0,
            theme_positive_breadth_pct=theme_breadth,
            theme_average_return_pct=theme_return,
            raw_score=_float(candidate.get("raw_score")),
            risk_reward_ratio=risk_reward,
            leader_code=leader_code,
            leader_state="UNAVAILABLE_LIMIT_LOCKED",
            opening_target_fraction=float(
                rule.get("opening_target_fraction", 0.08)
            ),
            evidence=(
                f"龙一{leader_name}（{leader_code}）已封板不可成交",
                f"龙二现涨{current_return:.2f}%，相对市场{relative_strength:.2f}%",
                f"{theme_name}上涨宽度{theme_breadth:.2f}%，平均涨幅{theme_return:.2f}%",
                f"动态盈亏比{risk_reward:.2f}",
                (
                    reasons[0][1]
                    if reasons
                    else "龙一封板、板块扩散和龙二强度同时确认，仅模拟小仓套利"
                ),
            ),
        )
    if discovery_lane == "MARKET_WIDE_MOMENTUM_ALERT":
        interval_return = _float(raw.get("reference_interval_return_pct"))
        interval_minutes = _float(raw.get("reference_interval_minutes"))
        near_limit = bool((quote or {}).get("near_limit_up"))
        reason_code = (
            "MARKET_WIDE_LIMIT_ATTACK_ALERT"
            if near_limit
            else "MARKET_WIDE_ROCKET_ALERT"
        )
        description = (
            "快速冲击涨停，立即检查同板块龙二和中军；本票不追板"
            if near_limit
            else "短时快速拉升，已进入全市场异动观察"
        )
        return CandidateAssessment(
            stock_code=code,
            short_name=short_name,
            theme_code=theme_code,
            theme_name=theme_name,
            source_strategy_version=str(
                candidate.get("strategy_version") or ""
            ),
            role="盘中极速拉升",
            action="WATCH",
            status="WATCHING",
            reason_code=reason_code,
            current_price=price,
            current_return_pct=current_return,
            relative_strength_pct=relative_strength,
            intraday_amount_ratio=0.0,
            theme_positive_breadth_pct=theme_breadth,
            theme_average_return_pct=theme_return,
            raw_score=_float(candidate.get("raw_score")),
            risk_reward_ratio=0.0,
            leader_code=code,
            leader_state=(
                "UNAVAILABLE_LIMIT_LOCKED"
                if near_limit
                else "MOMENTUM_ALERT"
            ),
            opening_target_fraction=0.0,
            evidence=(
                f"当前涨幅{current_return:.2f}%",
                (
                    f"约{interval_minutes:.0f}分钟拉升"
                    f"{interval_return:.2f}%"
                    if interval_minutes > 0
                    else "没有可靠前值，按当前冲板状态先报警"
                ),
                f"{theme_name}上涨宽度{theme_breadth:.2f}%",
                description,
            ),
        )
    if discovery_lane == "MARKET_WIDE_VOLUME_BURST":
        interval_return = _float(raw.get("reference_interval_return_pct"))
        interval_seconds = _float(raw.get("reference_interval_seconds"))
        amount_delta = _float(raw.get("interval_amount_delta"))
        snapshot_count = int(raw.get("live_snapshot_count") or 0)
        sector_rotation_safe = (
            market.quality_status == "PASS"
            and market_return_pct
            >= float(thresholds["minimum_market_average_return_pct"])
            and market_breadth_pct
            >= float(thresholds["minimum_market_positive_breadth_pct"])
        )
        burst_reasons: list[tuple[str, str]] = []
        if not market.actionable and not sector_rotation_safe:
            burst_reasons.append(
                (
                    "VOLUME_BURST_MARKET_NOT_SAFE",
                    "市场环境不足以支持爆量跟随，只报警观察",
                )
            )
        if snapshot_count < int(
            thresholds["burst_minimum_live_snapshots"]
        ):
            burst_reasons.append(
                (
                    "VOLUME_BURST_CONFIRMATION_POINTS_MISSING",
                    "连续实时快照不足，只能看到放量，不能确认持续性",
                )
            )
        if amount_ratio < float(thresholds["burst_amount_ratio_gte"]):
            burst_reasons.append(
                (
                    "VOLUME_BURST_RATIO_TOO_LOW",
                    "单位时间成交额放大倍数不足",
                )
            )
        if amount_delta < float(
            thresholds["burst_minimum_interval_amount_cny"]
        ):
            burst_reasons.append(
                (
                    "VOLUME_BURST_AMOUNT_TOO_SMALL",
                    "爆量区间的实际成交额太小",
                )
            )
        if interval_return < float(
            thresholds["burst_minimum_price_return_pct"]
        ):
            burst_reasons.append(
                (
                    "VOLUME_BURST_PRICE_NOT_UP",
                    "有成交额但价格没有同步向上，不能当成主动买盘",
                )
            )
        if interval_return > float(
            thresholds["burst_maximum_price_return_pct"]
        ):
            burst_reasons.append(
                (
                    "VOLUME_BURST_PRICE_TOO_FAST",
                    "价格瞬间拉升过快，只报警、不追脉冲",
                )
            )
        if current_return < float(
            thresholds["burst_minimum_current_return_pct"]
        ):
            burst_reasons.append(
                (
                    "VOLUME_BURST_STILL_WEAK",
                    "爆量后个股仍偏弱，先观察资金承接",
                )
            )
        if current_return > float(
            thresholds["burst_maximum_current_return_pct"]
        ):
            burst_reasons.append(
                (
                    "VOLUME_BURST_TOO_EXTENDED",
                    "爆量后涨幅已经过高，剩余空间不足",
                )
            )
        if bool((quote or {}).get("near_limit_up")):
            burst_reasons.append(
                ("LEADER_LIMIT_LOCKED", "价格接近涨停，当前不追板")
            )
        if theme_count < int(
            thresholds["minimum_theme_member_observations"]
        ):
            burst_reasons.append(("THEME_SAMPLE_TOO_SMALL", "板块可观测成员不足"))
        if theme_breadth < float(
            thresholds["burst_minimum_theme_positive_breadth_pct"]
        ):
            burst_reasons.append(
                ("THEME_BREADTH_NOT_CONFIRMED", "板块上涨宽度不足")
            )
        if theme_return < float(
            thresholds["burst_minimum_theme_average_return_pct"]
        ):
            burst_reasons.append(
                ("THEME_RETURN_NOT_CONFIRMED", "板块平均涨幅不足")
            )
        if theme_relative < float(
            thresholds["burst_minimum_theme_relative_strength_pct"]
        ):
            burst_reasons.append(
                (
                    "REVERSAL_THEME_RELATIVE_STRENGTH_LOW",
                    "板块没有明显跑赢市场，可能只是单票爆量",
                )
            )
        if risk_reward < float(
            thresholds["standard_minimum_risk_reward_ratio"]
        ):
            burst_reasons.append(
                (
                    "REVERSAL_RISK_REWARD_TOO_LOW",
                    "爆量后的现价剩余盈亏比不足",
                )
            )
        action = "WATCH"
        status = "WATCHING"
        reason_code = (
            burst_reasons[0][0]
            if burst_reasons
            else "MARKET_WIDE_VOLUME_BURST_CONFIRMED"
        )
        if not burst_reasons:
            action = "ACTIVATE_VOLUME_PROBE"
            status = "ACTIVATABLE"
        return CandidateAssessment(
            stock_code=code,
            short_name=short_name,
            theme_code=theme_code,
            theme_name=theme_name,
            source_strategy_version=str(
                candidate.get("strategy_version") or ""
            ),
            role="盘中爆量上攻",
            action=action,
            status=status,
            reason_code=reason_code,
            current_price=price,
            current_return_pct=current_return,
            relative_strength_pct=relative_strength,
            intraday_amount_ratio=amount_ratio,
            theme_positive_breadth_pct=theme_breadth,
            theme_average_return_pct=theme_return,
            raw_score=_float(candidate.get("raw_score")),
            risk_reward_ratio=risk_reward,
            leader_code=code,
            leader_state="VOLUME_BURST",
            opening_target_fraction=float(
                thresholds["burst_opening_target_fraction"]
            ),
            evidence=(
                f"约{interval_seconds:.0f}秒成交额"
                f"{amount_delta / 10000:.0f}万元",
                f"单位时间成交额放大{amount_ratio:.2f}倍",
                f"同期价格上涨{interval_return:.2f}%，当前涨幅{current_return:.2f}%",
                f"{theme_name}上涨宽度{theme_breadth:.2f}%",
                (
                    burst_reasons[0][1]
                    if burst_reasons
                    else "疑似主动买盘持续增强，仅在模拟盘小仓验证"
                ),
            ),
        )
    is_deep_reversal = (
        session_low_return
        <= float(thresholds["deep_reversal_low_return_pct_lte"])
    )
    minimum_rebound = float(
        thresholds[
            "minimum_rebound_from_low_pct"
            if is_deep_reversal
            else "standard_minimum_rebound_from_low_pct"
        ]
    )
    minimum_current_return = float(
        thresholds[
            "minimum_current_return_pct"
            if is_deep_reversal
            else "standard_minimum_current_return_pct"
        ]
    )
    minimum_momentum_10m = float(
        thresholds[
            "minimum_10m_return_pct"
            if is_deep_reversal
            else "standard_minimum_10m_return_pct"
        ]
    )
    minimum_amount_ratio = float(
        thresholds[
            "minimum_intraday_amount_ratio"
            if is_deep_reversal
            else "standard_minimum_intraday_amount_ratio"
        ]
    )
    minimum_risk_reward = float(
        thresholds[
            "minimum_risk_reward_ratio"
            if is_deep_reversal
            else "standard_minimum_risk_reward_ratio"
        ]
    )
    sector_rotation_safe = (
        market.quality_status == "PASS"
        and market_return_pct
        >= float(thresholds["minimum_market_average_return_pct"])
        and market_breadth_pct
        >= float(thresholds["minimum_market_positive_breadth_pct"])
    )

    reasons: list[tuple[str, str]] = []
    if not market.actionable and not sector_rotation_safe:
        reasons.append(
            (
                "REVERSAL_MARKET_NOT_SAFE",
                "大盘并未普涨，且当前广度不足以支持板块逆势试仓",
            )
        )
    if not quote or price <= 0 or pre_close <= 0:
        reasons.append(("QMT_QUOTE_MISSING", "没有新鲜的QMT全市场价格"))
    if history_coverage < float(
        thresholds["minimum_minute_history_coverage"]
    ):
        reasons.append(
            (
                "REVERSAL_MINUTE_HISTORY_INCOMPLETE",
                "当日分钟路径不完整，先报警但不根据不完整路径买入",
            )
        )
    if history_age > float(thresholds["maximum_minute_history_age_seconds"]):
        reasons.append(
            (
                "REVERSAL_MINUTE_HISTORY_STALE",
                "分钟K线更新过慢，暂不根据旧走势试仓",
            )
        )
    if session_low_return > float(
        thresholds["maximum_waterline_low_return_pct"]
    ):
        reasons.append(
            (
                "REVERSAL_WATERLINE_PATTERN_MISSING",
                "日内没有形成可确认的水下修复形态",
            )
        )
    if rebound < minimum_rebound:
        reasons.append(
            (
                "REVERSAL_REBOUND_NOT_CONFIRMED",
                "从日内低点反弹的幅度还不够",
            )
        )
    if current_return < minimum_current_return:
        reasons.append(
            (
                "REVERSAL_WATERLINE_NOT_RECLAIMED",
                "尚未有效翻红，先报警观察，不接仍在水下的反抽",
            )
        )
    if current_return > float(thresholds["maximum_current_return_pct"]):
        reasons.append(
            (
                "REVERSAL_TOO_EXTENDED_TO_CHASE",
                "已经拉得过高，错过低风险区后只提示、不追涨",
            )
        )
    if momentum_10m < minimum_momentum_10m:
        reasons.append(
            (
                "REVERSAL_MOMENTUM_NOT_CONFIRMED",
                "最近10分钟上攻速度不足",
            )
        )
    if momentum_5m > float(thresholds["maximum_5m_return_pct"]):
        reasons.append(
            (
                "REVERSAL_PULSE_TOO_FAST",
                "最近5分钟拉升过急，等待回踩而不是追脉冲",
            )
        )
    if amount_ratio < minimum_amount_ratio:
        reasons.append(
            (
                "REVERSAL_VOLUME_NOT_CONFIRMED",
                "反转时段没有出现足够的成交额放大",
            )
        )
    if bool((quote or {}).get("near_limit_up")):
        reasons.append(("LEADER_LIMIT_LOCKED", "价格接近涨停，当前不追板"))
    if "ST" in short_name.upper() or "退" in short_name:
        reasons.append(("SPECIAL_TREATMENT_BLOCKED", "ST或退市整理股票禁止试仓"))
    if theme_count < int(thresholds["minimum_theme_member_observations"]):
        reasons.append(("THEME_SAMPLE_TOO_SMALL", "板块可观测成员不足"))
    if theme_breadth < float(
        thresholds["minimum_theme_positive_breadth_pct"]
    ):
        reasons.append(("THEME_BREADTH_NOT_CONFIRMED", "板块上涨宽度不足"))
    if theme_return < float(thresholds["minimum_theme_average_return_pct"]):
        reasons.append(("THEME_RETURN_NOT_CONFIRMED", "板块平均涨幅不足"))
    if theme_relative < float(
        thresholds["minimum_theme_relative_strength_pct"]
    ):
        reasons.append(
            (
                "REVERSAL_THEME_RELATIVE_STRENGTH_LOW",
                "板块没有明显跑赢全市场，可能只是单票脉冲",
            )
        )
    if risk_reward < minimum_risk_reward:
        reasons.append(
            (
                "REVERSAL_RISK_REWARD_TOO_LOW",
                "按翻红后的现价、保护位和冻结目标价计算，剩余盈亏比不足",
            )
        )

    action = "WATCH"
    status = "WATCHING"
    confirmed_code = (
        "MARKET_WIDE_DEEP_REVERSAL_CONFIRMED"
        if is_deep_reversal
        else "MARKET_WIDE_WATERLINE_RECOVERY_CONFIRMED"
    )
    reason_code = reasons[0][0] if reasons else confirmed_code
    if not reasons:
        action = "ACTIVATE_REVERSAL_PROBE"
        status = "ACTIVATABLE"
    evidence = (
        f"日内最低{session_low_return:.2f}%，现价{current_return:.2f}%",
        f"低点反弹{rebound:.2f}%，近10分钟{momentum_10m:.2f}%",
        f"反转量能{amount_ratio:.2f}倍，分钟覆盖{history_coverage:.1%}",
        f"{theme_name}上涨宽度{theme_breadth:.2f}%，平均涨幅{theme_return:.2f}%",
        f"动态盈亏比{risk_reward:.2f}",
        *(
            (reasons[0][1],)
            if reasons
            else (
                (
                    "深水反转、放量和板块共振同时确认，仅模拟小仓试错"
                    if is_deep_reversal
                    else "水下修复、爆量和板块共振确认，仅模拟更小仓试错"
                ),
            )
        ),
    )
    return CandidateAssessment(
        stock_code=code,
        short_name=short_name,
        theme_code=theme_code,
        theme_name=theme_name,
        source_strategy_version=str(candidate.get("strategy_version") or ""),
        role=(
            "盘中深水反转"
            if is_deep_reversal
            else "盘中水下修复"
        ),
        action=action,
        status=status,
        reason_code=reason_code,
        current_price=price,
        current_return_pct=current_return,
        relative_strength_pct=relative_strength,
        intraday_amount_ratio=amount_ratio,
        theme_positive_breadth_pct=theme_breadth,
        theme_average_return_pct=theme_return,
        raw_score=_float(candidate.get("raw_score")),
        risk_reward_ratio=risk_reward,
        leader_code=code,
        leader_state="REVERSAL_CANDIDATE",
        opening_target_fraction=float(
            thresholds[
                "opening_target_fraction"
                if is_deep_reversal
                else "standard_opening_target_fraction"
            ]
        ),
        evidence=evidence,
    )


def select_reversal_activations(
    candidates: list[dict[str, Any]],
    *,
    market: MarketAssessment,
    market_return_pct: float,
    market_breadth_pct: float,
    quotes: dict[str, dict[str, Any]],
    theme_metrics: dict[str, dict[str, float]],
    config: dict[str, Any],
) -> list[CandidateAssessment]:
    """Rank market-wide reversals and allow only a tiny number of probes."""
    assessed = [
        assess_reversal_candidate(
            row,
            market=market,
            market_return_pct=market_return_pct,
            market_breadth_pct=market_breadth_pct,
            quote=quotes.get(str(row.get("stock_code") or "").zfill(6)),
            theme_metrics=theme_metrics.get(
                str(row.get("theme_code") or "")
            )
            or {},
            config=config,
        )
        for row in candidates
    ]
    actionable = sorted(
        (
            item
            for item in assessed
            if item.action
            in {
                "ACTIVATE_REVERSAL_PROBE",
                "ACTIVATE_VOLUME_PROBE",
                "ACTIVATE_SUBSTITUTE",
            }
        ),
        key=lambda item: (
            -item.risk_reward_ratio,
            -item.raw_score,
            -item.relative_strength_pct,
            item.stock_code,
        ),
    )
    maximum = int(
        config["market_wide_reversal_radar"][
            "maximum_activations_per_tick"
        ]
    )
    selected = {item.stock_code for item in actionable[:maximum]}
    return [
        (
            item
            if item.stock_code in selected
            or item.action
            not in {
                "ACTIVATE_REVERSAL_PROBE",
                "ACTIVATE_VOLUME_PROBE",
                "ACTIVATE_SUBSTITUTE",
            }
            else CandidateAssessment(
                **{
                    **item.__dict__,
                    "action": "WATCH",
                    "status": "WATCHING",
                    "reason_code": "LOWER_RANKED_REVERSAL_CANDIDATE",
                }
            )
        )
        for item in assessed
    ]


def select_theme_activations(
    candidates: list[dict[str, Any]],
    *,
    market: MarketAssessment,
    market_return_pct: float,
    quotes: dict[str, dict[str, Any]],
    amount_ratios: dict[str, float],
    theme_metrics: dict[str, dict[str, float]],
    config: dict[str, Any],
) -> list[CandidateAssessment]:
    """Select at most one candidate per theme, with a guarded leader fallback."""
    by_theme: dict[str, list[dict[str, Any]]] = {}
    for row in candidates:
        by_theme.setdefault(str(row.get("theme_code") or ""), []).append(row)
    results: list[CandidateAssessment] = []
    for theme_code, rows in sorted(by_theme.items()):
        ordered = sorted(
            rows,
            key=lambda row: (
                int(
                    _parse_json_object(row.get("raw_features_json")).get(
                        "sector_rank"
                    )
                    or 999
                ),
                -_float(row.get("raw_score")),
                str(row.get("stock_code") or ""),
            ),
        )
        leader = next(
            (
                row
                for row in ordered
                if str(
                    _parse_json_object(row.get("raw_features_json")).get(
                        "sector_role"
                    )
                    or ""
                )
                in {"龙头", "观察龙头"}
            ),
            ordered[0],
        )
        leader_code = str(leader.get("stock_code") or "").zfill(6)
        leader_score = _float(leader.get("raw_score"))
        leader_quote = quotes.get(leader_code) or {}
        leader_unavailable = (
            not leader_quote
            or bool(leader_quote.get("near_limit_up"))
        )
        assessed = [
            assess_candidate(
                row,
                market=market,
                market_return_pct=market_return_pct,
                quote=quotes.get(
                    str(row.get("stock_code") or "").zfill(6)
                ),
                amount_ratio=amount_ratios.get(
                    str(row.get("stock_code") or "").zfill(6),
                    0.0,
                ),
                theme_metrics=theme_metrics.get(theme_code) or {},
                leader_code=leader_code,
                leader_score=leader_score,
                leader_unavailable=leader_unavailable,
                config=config,
            )
            for row in ordered
        ]
        actionable = [
            item
            for item in assessed
            if item.action in {
                "ACTIVATE_PROBE",
                "ACTIVATE_SUBSTITUTE",
            }
        ]
        if actionable:
            actionable.sort(
                key=lambda item: (
                    item.action != "ACTIVATE_SUBSTITUTE"
                    if leader_unavailable
                    else item.stock_code != leader_code,
                    -item.raw_score,
                    -item.relative_strength_pct,
                    item.stock_code,
                )
            )
            selected_code = actionable[0].stock_code
            results.extend(
                item
                if item.stock_code == selected_code
                else CandidateAssessment(
                    **{
                        **item.__dict__,
                        "action": "WATCH",
                        "status": "WATCHING",
                        "reason_code": "LOWER_RANKED_INTRADAY_CANDIDATE",
                    }
                )
                for item in assessed
            )
        else:
            results.extend(assessed)
    return results


def _expected_universe_count(engine: Engine) -> int:
    # Match the collector's current tradable universe. ``si_all_code`` also
    # retains historical/delisted instruments, which inflated production's
    # denominator and made healthy QMT coverage look lower than it was.
    try:
        with get_kline_engine().connect() as connection:
            value = connection.execute(
                text(
                    """
                    SELECT COUNT(DISTINCT stock_code)
                    FROM sm_stock_kline
                    WHERE k_type = 1
                      AND adjust_type = 0
                      AND trade_date = (
                          SELECT MAX(trade_date)
                          FROM sm_stock_kline
                          WHERE k_type = 1
                            AND adjust_type = 0
                      )
                    """
                )
            ).scalar()
        if int(value or 0) > 0:
            return int(value)
    except Exception as exc:
        logger.debug(
            "latest tradable K-line universe count unavailable; "
            "falling back to security master: %s",
            exc,
        )
    with engine.connect() as connection:
        value = connection.execute(
            text(
                """
                SELECT COUNT(DISTINCT stock_code)
                FROM si_all_code
                WHERE stock_code REGEXP '^[0-9]{6}$'
                """
            )
        ).scalar()
    return int(value or 0)


def _previous_close_map(
    engine: Engine,
    *,
    trade_date: date,
) -> dict[str, float]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT k.stock_code, k.close
                FROM sm_stock_kline k
                INNER JOIN (
                    SELECT stock_code, MAX(trade_date) AS trade_date
                    FROM sm_stock_kline
                    WHERE k_type = 1 AND adjust_type = 0
                      AND trade_date < :trade_date
                    GROUP BY stock_code
                ) latest
                  ON latest.stock_code = k.stock_code
                 AND latest.trade_date = k.trade_date
                WHERE k.k_type = 1 AND k.adjust_type = 0
                  AND k.close > 0
                """
            ),
            {"trade_date": trade_date},
        ).mappings().all()
    return {
        str(row["stock_code"]).zfill(6): _float(row["close"])
        for row in rows
        if _float(row["close"]) > 0
    }


def _load_current_market(
    primary_engine: Engine,
    *,
    now: datetime,
    config: dict[str, Any],
) -> tuple[MarketPoint | None, dict[str, dict[str, Any]]]:
    """Load one row-level-provenance BigQMT full-market snapshot."""
    provider = str(
        config["data_quality"]["required_provider"]
    ).lower()
    maximum_age = int(
        config["data_quality"]["maximum_minute_age_seconds"]
    )
    current_engine = get_current_engine()
    try:
        with current_engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT stock_code, short_name, price,
                           `change` AS price_change,
                           change_pct, volume, amount, snapshot_at, source_time,
                           data_source
                    FROM sm_stock_current
                    WHERE LOWER(data_source) = :provider
                      AND COALESCE(source_time, snapshot_at)
                          >= :cutoff
                      AND COALESCE(source_time, snapshot_at)
                          <= :now
                      AND price > 0
                    """
                ),
                {
                    "provider": provider,
                    "cutoff": now - timedelta(seconds=maximum_age),
                    "now": now,
                },
            ).mappings().all()
    except Exception:
        return None, {}
    latest_by_code: dict[str, dict[str, Any]] = {}
    for raw_row in rows:
        row = dict(raw_row)
        code = str(row.get("stock_code") or "").zfill(6)
        observed_at = row.get("source_time") or row.get("snapshot_at")
        if not isinstance(observed_at, datetime):
            try:
                observed_at = datetime.fromisoformat(
                    str(observed_at).replace(" ", "T")
                )
            except ValueError:
                continue
        previous = latest_by_code.get(code)
        if previous and previous["observed_at"] >= observed_at:
            continue
        price = _float(row.get("price"))
        change = _float(row.get("price_change"))
        pre_close = price - change
        if pre_close <= 0:
            change_pct = _float(row.get("change_pct"))
            if change_pct > -99.0:
                pre_close = price / (1.0 + change_pct / 100.0)
        latest_by_code[code] = {
            **row,
            "stock_code": code,
            "observed_at": observed_at,
            "price": price,
            "pre_close": pre_close,
            "return_pct": (price / pre_close - 1.0) * 100.0,
        }
    if not latest_by_code:
        return None, {}
    expected = _expected_universe_count(primary_engine)
    returns = [row["return_pct"] for row in latest_by_code.values()]
    observed_at = max(
        row["observed_at"] for row in latest_by_code.values()
    )
    point = MarketPoint(
        observed_at=observed_at,
        observed_count=len(latest_by_code),
        expected_count=expected,
        coverage=len(latest_by_code) / max(expected, 1),
        positive_breadth_pct=(
            sum(value > 0 for value in returns)
            / len(returns)
            * 100.0
        ),
        equal_weight_return_pct=sum(returns) / len(returns),
        median_return_pct=median(returns),
        source=provider.upper(),
    )
    return point, latest_by_code


def _market_point_from_public_snapshot(
    receipt: dict[str, Any] | None,
    quotes: dict[str, dict[str, Any]],
) -> MarketPoint | None:
    if not receipt or not quotes:
        return None
    observed_at = receipt.get("quote_at")
    if not isinstance(observed_at, datetime):
        try:
            observed_at = datetime.fromisoformat(
                str(observed_at).replace(" ", "T")
            )
        except (TypeError, ValueError):
            return None
    returns = [
        _float(row.get("return_pct"))
        for row in quotes.values()
        if _float(row.get("price")) > 0
        and _float(row.get("pre_close")) > 0
    ]
    if not returns:
        return None
    expected_count = int(receipt.get("expected_count") or 0)
    observed_count = len(returns)
    return MarketPoint(
        observed_at=observed_at,
        observed_count=observed_count,
        expected_count=expected_count,
        coverage=observed_count / max(expected_count, 1),
        positive_breadth_pct=(
            sum(value > 0 for value in returns)
            / len(returns)
            * 100.0
        ),
        equal_weight_return_pct=sum(returns) / len(returns),
        median_return_pct=median(returns),
        source=str(
            receipt.get("source_provider")
            or "PUBLIC_QUOTE_QUORUM_V1"
        ).upper(),
    )


def _point_meets_quality(
    point: MarketPoint | None,
    *,
    now: datetime,
    expected_provider: str,
    minimum_observed_stocks: int,
    minimum_universe_coverage: float,
    maximum_age_seconds: float,
) -> bool:
    if point is None:
        return False
    age_seconds = (now - point.observed_at).total_seconds()
    return bool(
        str(point.source or "").upper()
        == str(expected_provider or "").upper()
        and point.observed_count >= int(minimum_observed_stocks)
        and point.coverage >= float(minimum_universe_coverage)
        and 0 <= age_seconds <= float(maximum_age_seconds)
    )


def _load_public_failover_market(
    engine: Engine,
    *,
    now: datetime,
    config: dict[str, Any],
    collect_if_missing: bool,
) -> tuple[
    MarketPoint | None,
    dict[str, dict[str, Any]],
    dict[str, Any] | None,
]:
    failover = dict(config.get("public_quote_failover") or {})
    if not bool(failover.get("enabled")):
        return None, {}, None

    def load(reference_now: datetime) -> tuple[
        MarketPoint | None,
        dict[str, dict[str, Any]],
    ]:
        receipt, quotes = load_latest_public_quote_snapshot(
            engine,
            now=reference_now,
            config=failover,
        )
        return _market_point_from_public_snapshot(receipt, quotes), quotes

    try:
        point, quotes = load(now)
    except Exception as exc:
        logger.warning(
            "public quote failover snapshot read failed: %s",
            exc,
        )
        point, quotes = None, {}
    collection: dict[str, Any] | None = None
    if point is None and collect_if_missing:
        try:
            collection = collect_public_quote_failover(
                engine,
                now=now,
                config=failover,
                lock_timeout_seconds=int(
                    failover.get("activation_lock_wait_seconds") or 0
                ),
            )
        except Exception as exc:
            logger.exception("public quote failover collection failed")
            collection = {
                "status": "error",
                "reason": (
                    f"PUBLIC_QUOTE_FAILOVER_ERROR:"
                    f"{type(exc).__name__}"
                ),
            }
        if collection.get("status") in {
            "success",
            "already_running",
            "existing_fresh",
        }:
            try:
                reload_at = now
                if collection.get("quote_at"):
                    reload_at = datetime.fromisoformat(
                        str(collection["quote_at"]).replace(" ", "T")
                    )
                elif collection.get("status") == "already_running":
                    reload_at = max(now, datetime.now())
                point, quotes = load(reload_at)
            except Exception as exc:
                logger.warning(
                    "public quote failover reload failed: %s",
                    exc,
                )
    return point, quotes, collection


def _failover_opening_fraction(
    value: float,
    *,
    config: dict[str, Any],
) -> float:
    failover = dict(config.get("public_quote_failover") or {})
    multiplier = max(
        0.0,
        min(1.0, _float(failover.get("risk_fraction_multiplier"), 0.5)),
    )
    maximum = max(
        0.0,
        _float(failover.get("maximum_opening_target_fraction"), 0.05),
    )
    return min(max(0.0, float(value)) * multiplier, maximum)


def _recent_persisted_market_points(
    engine: Engine,
    *,
    before: datetime,
    provider: str,
    limit: int,
) -> list[MarketPoint]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT observed_at, observed_count, expected_count,
                       coverage, positive_breadth_pct,
                       equal_weight_return_pct, median_return_pct,
                       source_provider
                FROM st_intraday_market_state_v2
                WHERE observed_at < :before
                  AND UPPER(source_provider) = :provider
                ORDER BY observed_at DESC
                LIMIT :limit
                """
            ),
            {
                "before": before,
                "provider": provider.upper(),
                "limit": int(limit),
            },
        ).mappings().all()
    result = []
    for row in reversed(rows):
        observed_at = row["observed_at"]
        if not isinstance(observed_at, datetime):
            observed_at = datetime.fromisoformat(
                str(observed_at).replace(" ", "T")
            )
        result.append(
            MarketPoint(
                observed_at=observed_at,
                observed_count=int(row["observed_count"] or 0),
                expected_count=int(row["expected_count"] or 0),
                coverage=_float(row["coverage"]),
                positive_breadth_pct=_float(
                    row["positive_breadth_pct"]
                ),
                equal_weight_return_pct=_float(
                    row["equal_weight_return_pct"]
                ),
                median_return_pct=_float(row["median_return_pct"]),
                source=str(row["source_provider"] or ""),
            )
        )
    return result


def _load_minute_market(
    primary_engine: Engine,
    *,
    trade_date: date,
    now: datetime,
    config: dict[str, Any],
) -> tuple[
    list[MarketPoint],
    dict[str, dict[str, Any]],
    dict[str, float],
]:
    minute_engine = get_minute_engine()
    kline_engine = get_kline_engine()
    table = get_minute_stock_table()
    source_info = minute_source_info()
    price_column = (
        "close" if source_info["kind"] == "ohlc" else "price"
    )
    required_points = int(config["data_quality"]["confirmation_points"])
    with minute_engine.connect() as connection:
        time_rows = connection.execute(
            text(
                f"""
                SELECT trade_time, COUNT(DISTINCT stock_code) AS row_count
                FROM `{table}`
                WHERE trade_date = :trade_date
                  AND trade_time <= :now
                  AND {price_column} > 0
                GROUP BY trade_time
                ORDER BY trade_time DESC
                LIMIT {required_points}
                """
            ),
            {"trade_date": trade_date, "now": now},
        ).mappings().all()
        times = sorted(
            [
                row["trade_time"]
                if isinstance(row["trade_time"], datetime)
                else datetime.fromisoformat(
                    str(row["trade_time"]).replace(" ", "T")
                )
                for row in time_rows
            ]
        )
        if not times:
            return [], {}, {}
        statement = text(
            f"""
            SELECT stock_code, trade_time,
                   MAX({price_column}) AS price,
                   MAX(volume) AS volume,
                   MAX(amount) AS amount
            FROM `{table}`
            WHERE trade_date = :trade_date
              AND trade_time IN :trade_times
              AND {price_column} > 0
            GROUP BY stock_code, trade_time
            """
        ).bindparams(bindparam("trade_times", expanding=True))
        rows = connection.execute(
            statement,
            {
                "trade_date": trade_date,
                "trade_times": times,
            },
        ).mappings().all()
    previous_close = _previous_close_map(
        kline_engine,
        trade_date=trade_date,
    )
    expected = _expected_universe_count(primary_engine)
    source = "UNATTESTED_MINUTE_SOURCE"
    try:
        # The source receipt is stored beside the minute bars. Production may
        # reach both over the Windows reverse-MySQL route, while V2 account and
        # order state stays in the primary production database.
        with minute_engine.connect() as connection:
            receipt = connection.execute(
                text(
                    """
                    SELECT source_provider, coverage, observed_count,
                           expected_count, last_trade_time, quality_status,
                           capture_mode, forward_eligible
                    FROM st_qmt_minute_sync_receipt_v2
                    WHERE trade_date = :trade_date
                      AND last_trade_time >= :latest_trade_time
                      AND quality_status = 'PASS'
                      AND capture_mode = 'LIVE_FORWARD'
                      AND forward_eligible = 1
                    ORDER BY last_trade_time DESC, created_at DESC
                    LIMIT 1
                    """
                ),
                {
                    "trade_date": trade_date,
                    "latest_trade_time": times[-1],
                },
            ).mappings().first()
        if receipt:
            source = str(receipt["source_provider"] or "").upper()
    except Exception:
        source = "UNATTESTED_MINUTE_SOURCE"
    by_time: dict[datetime, list[dict[str, Any]]] = {
        item: [] for item in times
    }
    for raw_row in rows:
        row = dict(raw_row)
        observed_at = row["trade_time"]
        if not isinstance(observed_at, datetime):
            observed_at = datetime.fromisoformat(
                str(observed_at).replace(" ", "T")
            )
        code = str(row["stock_code"]).zfill(6)
        pre_close = previous_close.get(code, 0.0)
        price = _float(row["price"])
        if pre_close <= 0 or price <= 0:
            continue
        by_time.setdefault(observed_at, []).append(
            {
                **row,
                "stock_code": code,
                "trade_time": observed_at,
                "price": price,
                "pre_close": pre_close,
                "return_pct": (price / pre_close - 1.0) * 100.0,
            }
        )
    points: list[MarketPoint] = []
    for observed_at in times:
        point_rows = by_time.get(observed_at) or []
        if not point_rows:
            continue
        returns = [row["return_pct"] for row in point_rows]
        points.append(
            MarketPoint(
                observed_at=observed_at,
                observed_count=len(point_rows),
                expected_count=expected,
                coverage=len(point_rows) / max(expected, 1),
                positive_breadth_pct=(
                    sum(value > 0 for value in returns)
                    / len(returns)
                    * 100.0
                ),
                equal_weight_return_pct=sum(returns) / len(returns),
                median_return_pct=median(returns),
                source=source,
            )
        )
    latest_rows = by_time.get(points[-1].observed_at, []) if points else []
    latest = {
        str(row["stock_code"]): dict(row)
        for row in latest_rows
    }
    amount_ratios: dict[str, float] = {}
    series_by_code: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        code = str(row["stock_code"]).zfill(6)
        series_by_code.setdefault(code, []).append(dict(row))
    for code, series in series_by_code.items():
        series.sort(key=lambda item: str(item.get("trade_time") or ""))
        amounts = [_float(item.get("amount")) for item in series]
        if len(amounts) >= 2:
            baseline = sum(amounts[:-1]) / max(1, len(amounts) - 1)
            amount_ratios[code] = (
                amounts[-1] / baseline if baseline > 0 else 0.0
            )
        else:
            amount_ratios[code] = 0.0
    return points, latest, amount_ratios


def _load_watch_candidates(
    engine: Engine,
    *,
    now: datetime,
    source_versions: list[str],
) -> tuple[str, str, list[dict[str, Any]]]:
    with engine.connect() as connection:
        run = connection.execute(
            text(
                """
                SELECT run_uid, market_regime
                FROM st_decision_run_v2
                WHERE status = 'COMPLETED'
                ORDER BY decision_at DESC, started_at DESC, run_uid DESC
                LIMIT 1
                """
            )
        ).mappings().first()
        if not run:
            return "", "DATA_BLOCKED", []
        statement = text(
            """
            SELECT s.*
            FROM st_strategy_signal_v2 s
            WHERE s.run_uid = :run_uid
              AND s.strategy_version IN :versions
              AND s.valid_from <= :now
              AND s.valid_until >= :now
              AND s.action IN ('BUY','HOLD')
              AND s.competition_status IN (
                    'ELIGIBLE', 'PAPER_TRIAL_ELIGIBLE'
                  )
              AND s.rejection_code IS NULL
            ORDER BY s.raw_score DESC, s.stock_code
            """
        ).bindparams(bindparam("versions", expanding=True))
        rows = connection.execute(
            statement,
            {
                "run_uid": run["run_uid"],
                "versions": source_versions,
                "now": now,
            },
        ).mappings().all()
    return (
        str(run["run_uid"]),
        str(run["market_regime"]),
        [dict(row) for row in rows],
    )


def _load_theme_memberships(
    engine: Engine,
    themes: set[str],
) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {theme: set() for theme in themes}
    by_kind = {
        "CONCEPT": {
            theme.split(":", 1)[1]: theme
            for theme in themes
            if theme.startswith("CONCEPT:")
        },
        "INDUSTRY": {
            theme.split(":", 1)[1]: theme
            for theme in themes
            if theme.startswith("INDUSTRY:")
        },
    }
    with engine.connect() as connection:
        for kind, mapping in by_kind.items():
            if not mapping:
                continue
            table = (
                "qmt_concept_member_snapshot"
                if kind == "CONCEPT"
                else "qmt_industry_member_snapshot"
            )
            code_column = (
                "concept_code" if kind == "CONCEPT" else "industry_code"
            )
            statement = text(
                f"""
                SELECT {code_column} AS theme_key, stock_code
                FROM `{table}`
                WHERE snapshot_date = (
                    SELECT MAX(snapshot_date) FROM `{table}`
                    WHERE quality_status = 'QMT_VALIDATED'
                )
                  AND quality_status = 'QMT_VALIDATED'
                  AND {code_column} IN :theme_keys
                """
            ).bindparams(bindparam("theme_keys", expanding=True))
            rows = connection.execute(
                statement,
                {"theme_keys": list(mapping)},
            ).mappings().all()
            for row in rows:
                theme = mapping.get(str(row["theme_key"]))
                if theme:
                    result[theme].add(
                        str(row["stock_code"]).zfill(6)
                    )
    return result


def _theme_metrics(
    memberships: dict[str, set[str]],
    quotes: dict[str, dict[str, Any]],
) -> dict[str, dict[str, float]]:
    output: dict[str, dict[str, float]] = {}
    for theme, members in memberships.items():
        returns = [
            _float(quotes[code].get("return_pct"))
            for code in members
            if code in quotes
        ]
        output[theme] = {
            "observed_count": float(len(returns)),
            "positive_breadth_pct": (
                sum(value > 0 for value in returns)
                / len(returns)
                * 100.0
                if returns
                else 0.0
            ),
            "average_return_pct": (
                sum(returns) / len(returns) if returns else 0.0
            ),
        }
    return output


def _load_primary_industries(
    engine: Engine,
    *,
    stock_codes: set[str],
    trade_date: date,
) -> dict[str, dict[str, str]]:
    """Resolve each stock to its point-in-time SW2 industry, then SW1."""
    if not stock_codes:
        return {}
    rows: list[dict[str, Any]] = []
    ordered_codes = sorted(stock_codes)
    with engine.connect() as connection:
        for offset in range(0, len(ordered_codes), 500):
            batch = ordered_codes[offset : offset + 500]
            statement = text(
                """
                SELECT stock_code, industry_code, industry_name,
                       industry_type, short_name
                FROM qmt_industry_member_snapshot
                WHERE snapshot_date = (
                    SELECT MAX(snapshot_date)
                    FROM qmt_industry_member_snapshot
                    WHERE snapshot_date <= :trade_date
                      AND quality_status = 'QMT_VALIDATED'
                )
                  AND quality_status = 'QMT_VALIDATED'
                  AND stock_code IN :codes
                """
            ).bindparams(bindparam("codes", expanding=True))
            rows.extend(
                dict(row)
                for row in connection.execute(
                    statement,
                    {"trade_date": trade_date, "codes": batch},
                ).mappings().all()
            )
    by_code: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_code.setdefault(
            str(row.get("stock_code") or "").zfill(6),
            [],
        ).append(row)

    def priority(row: dict[str, Any]) -> tuple[int, str]:
        kind = str(row.get("industry_type") or "")
        code = str(row.get("industry_code") or "")
        if "二级" in kind or code.startswith("SW2"):
            return 0, code
        if "一级" in kind or code.startswith("SW1"):
            return 1, code
        return 2, code

    result: dict[str, dict[str, str]] = {}
    for code, candidates in by_code.items():
        selected = sorted(candidates, key=priority)[0]
        result[code] = {
            "theme_code": (
                f"INDUSTRY:{selected.get('industry_code') or ''}"
            ),
            "theme_name": str(
                selected.get("industry_name") or "未分类"
            ),
            "short_name": str(selected.get("short_name") or ""),
        }
    return result


def _discover_market_wide_reversals(
    *,
    trade_date: date,
    now: datetime,
    quotes: dict[str, dict[str, Any]],
    live_quote_metrics: dict[str, dict[str, float]],
    excluded_codes: set[str],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    """Discover deep-water reversals from accumulated canonical QMT minutes."""
    thresholds = config["market_wide_reversal_radar"]
    prefiltered: list[str] = []
    for code, quote in quotes.items():
        price = _float(quote.get("price"))
        pre_close = _float(quote.get("pre_close"))
        current_return = _float(quote.get("return_pct"))
        short_name = str(quote.get("short_name") or "")
        if (
            code not in excluded_codes
            and len(code) == 6
            and code[0] in {"0", "3", "4", "6", "8", "9"}
            and price > 0
            and pre_close > 0
            and float(thresholds["alert_current_return_pct_min"])
            <= current_return
            <= float(thresholds["alert_current_return_pct_max"])
            and _float(quote.get("amount"))
            >= float(thresholds["minimum_cumulative_amount_cny"])
            and "ST" not in short_name.upper()
            and "退" not in short_name
        ):
            prefiltered.append(code)
    if not prefiltered:
        return []

    minute_engine = get_minute_engine()
    table = get_minute_stock_table()
    source_info = minute_source_info()
    price_column = (
        "close" if source_info["kind"] == "ohlc" else "price"
    )
    aggregates: dict[str, dict[str, Any]] = {}
    with minute_engine.connect() as connection:
        for offset in range(0, len(prefiltered), 500):
            batch = prefiltered[offset : offset + 500]
            statement = text(
                f"""
                SELECT stock_code, MIN({price_column}) AS session_low_price,
                       MAX(trade_time) AS latest_trade_time,
                       COUNT(DISTINCT trade_time) AS minute_count,
                       SUM(amount) AS session_amount
                FROM `{table}`
                WHERE trade_date = :trade_date
                  AND trade_time <= :now
                  AND {price_column} > 0
                  AND stock_code IN :codes
                GROUP BY stock_code
                """
            ).bindparams(bindparam("codes", expanding=True))
            for row in connection.execute(
                statement,
                {
                    "trade_date": trade_date,
                    "now": now,
                    "codes": batch,
                },
            ).mappings().all():
                aggregates[str(row["stock_code"]).zfill(6)] = dict(row)

    rough_codes: list[str] = []
    for code, aggregate in aggregates.items():
        quote = quotes.get(code) or {}
        price = _float(quote.get("price"))
        pre_close = _float(quote.get("pre_close"))
        low_price = _float(aggregate.get("session_low_price"))
        if not (price > 0 and pre_close > 0 and low_price > 0):
            continue
        low_return = (low_price / pre_close - 1.0) * 100.0
        rebound = (price / low_price - 1.0) * 100.0
        if (
            low_return
            <= float(thresholds["alert_session_low_return_pct_max"])
            and rebound
            >= float(thresholds["alert_rebound_from_low_pct_min"])
        ):
            rough_codes.append(code)
    if not rough_codes:
        return []

    recent: dict[str, list[dict[str, Any]]] = {}
    recent_start = now - timedelta(
        minutes=int(thresholds["recent_history_minutes"])
    )
    with minute_engine.connect() as connection:
        for offset in range(0, len(rough_codes), 500):
            batch = rough_codes[offset : offset + 500]
            statement = text(
                f"""
                SELECT stock_code, trade_time,
                       MAX({price_column}) AS price,
                       MAX(amount) AS amount
                FROM `{table}`
                WHERE trade_date = :trade_date
                  AND trade_time >= :recent_start
                  AND trade_time <= :now
                  AND {price_column} > 0
                  AND stock_code IN :codes
                GROUP BY stock_code, trade_time
                ORDER BY stock_code, trade_time
                """
            ).bindparams(bindparam("codes", expanding=True))
            for raw_row in connection.execute(
                statement,
                {
                    "trade_date": trade_date,
                    "recent_start": recent_start,
                    "now": now,
                    "codes": batch,
                },
            ).mappings().all():
                row = dict(raw_row)
                if not isinstance(row.get("trade_time"), datetime):
                    row["trade_time"] = datetime.fromisoformat(
                        str(row["trade_time"]).replace(" ", "T")
                    )
                recent.setdefault(
                    str(row["stock_code"]).zfill(6),
                    [],
                ).append(row)

    industries = _load_primary_industries(
        get_kline_engine(),
        stock_codes=set(rough_codes),
        trade_date=trade_date,
    )
    expected_minutes = max(1, _expected_session_minutes(now))
    candidates: list[dict[str, Any]] = []
    for code in rough_codes:
        quote = quotes.get(code) or {}
        aggregate = aggregates[code]
        series = recent.get(code) or []
        price = _float(quote.get("price"))
        pre_close = _float(quote.get("pre_close"))
        low_price = _float(aggregate.get("session_low_price"))
        low_return = (low_price / pre_close - 1.0) * 100.0
        rebound = (price / low_price - 1.0) * 100.0
        observed_at = quote.get("observed_at") or now
        if not isinstance(observed_at, datetime):
            observed_at = datetime.fromisoformat(
                str(observed_at).replace(" ", "T")
            )
        latest_trade_time = aggregate.get("latest_trade_time")
        if not isinstance(latest_trade_time, datetime):
            latest_trade_time = datetime.fromisoformat(
                str(latest_trade_time).replace(" ", "T")
            )

        def price_at_or_before(moment: datetime) -> float:
            eligible = [
                _float(row.get("price"))
                for row in series
                if row.get("trade_time") <= moment
                and _float(row.get("price")) > 0
            ]
            return eligible[-1] if eligible else 0.0

        price_10m = price_at_or_before(
            observed_at - timedelta(minutes=10)
        )
        price_5m = price_at_or_before(
            observed_at - timedelta(minutes=5)
        )
        momentum_10m = (
            (price / price_10m - 1.0) * 100.0
            if price_10m > 0
            else 0.0
        )
        momentum_5m = (
            (price / price_5m - 1.0) * 100.0
            if price_5m > 0
            else 0.0
        )
        amounts = [
            _float(row.get("amount"))
            for row in series
            if _float(row.get("amount")) >= 0
        ]
        recent_count = int(thresholds["amount_recent_minutes"])
        baseline_count = int(thresholds["amount_baseline_minutes"])
        recent_amounts = amounts[-recent_count:]
        baseline_amounts = amounts[
            -recent_count - baseline_count : -recent_count
        ]
        recent_average = (
            sum(recent_amounts) / len(recent_amounts)
            if recent_amounts
            else 0.0
        )
        baseline_average = (
            sum(baseline_amounts) / len(baseline_amounts)
            if baseline_amounts
            else 0.0
        )
        amount_ratio = (
            recent_average / baseline_average
            if baseline_average > 0
            else 0.0
        )
        amount_ratio = max(
            amount_ratio,
            _float(
                (live_quote_metrics.get(code) or {}).get(
                    "amount_ratio"
                )
            ),
        )
        current_cumulative_amount = _float(quote.get("amount"))
        stored_session_amount = _float(aggregate.get("session_amount"))
        live_gap_minutes = max(
            0.0,
            (observed_at - latest_trade_time).total_seconds() / 60.0,
        )
        if (
            live_gap_minutes >= 0.5
            and live_gap_minutes
            <= float(thresholds["maximum_live_amount_gap_minutes"])
            and current_cumulative_amount > stored_session_amount
            and baseline_average > 0
        ):
            live_average = (
                current_cumulative_amount - stored_session_amount
            ) / max(1.0, live_gap_minutes)
            amount_ratio = max(
                amount_ratio,
                live_average / baseline_average,
            )
        minute_count = int(aggregate.get("minute_count") or 0)
        history_coverage = min(1.0, minute_count / expected_minutes)
        history_age = max(
            0.0,
            (observed_at - latest_trade_time).total_seconds(),
        )
        limit_ratio = _float(quote.get("limit_ratio"), 0.10)
        stop = pre_close * (
            1.0 - float(thresholds["protective_stop_below_preclose_pct"]) / 100.0
        )
        target = pre_close * (
            1.0
            + min(
                limit_ratio,
                float(thresholds["target_return_pct_above_preclose"])
                / 100.0,
            )
        )
        current_return = (price / pre_close - 1.0) * 100.0
        score = max(
            0.0,
            min(
                99.0,
                55.0
                + max(0.0, -low_return - 3.0) * 2.0
                + rebound * 1.5
                + max(0.0, momentum_10m) * 2.0
                + min(max(amount_ratio, 0.0), 3.0) * 3.0
                - max(0.0, current_return - 4.0) * 4.0,
            ),
        )
        industry = industries.get(code) or {}
        short_name = str(
            quote.get("short_name")
            or industry.get("short_name")
            or code
        )
        raw = {
            "stock_name": short_name,
            "theme_name": industry.get("theme_name") or "未分类",
            "sector_role": "盘中深水反转",
            "session_low_price": low_price,
            "session_low_return_pct": low_return,
            "rebound_from_low_pct": rebound,
            "momentum_10m_pct": momentum_10m,
            "momentum_5m_pct": momentum_5m,
            "intraday_amount_ratio": amount_ratio,
            "minute_history_coverage": history_coverage,
            "minute_history_age_seconds": history_age,
            "db_close": pre_close,
            "stop_loss": stop,
            "take_profit_2": target,
            "limit_ratio": limit_ratio,
            "discovery_lane": "MARKET_WIDE_REVERSAL_RADAR",
        }
        candidates.append(
            {
                "stock_code": code,
                "short_name": short_name,
                "theme_code": industry.get("theme_code") or "",
                "strategy_version": config["strategy_version"],
                "raw_score": score,
                "initial_stop": stop,
                "raw_features_json": raw,
            }
        )
    candidates.sort(
        key=lambda row: (
            -_float(row.get("raw_score")),
            str(row.get("stock_code") or ""),
        )
    )
    return candidates[: int(thresholds["maximum_alerts_per_tick"])]


def _discover_market_wide_momentum_alerts(
    *,
    trade_date: date,
    now: datetime,
    quotes: dict[str, dict[str, Any]],
    reference_quotes: dict[str, dict[str, Any]],
    excluded_codes: set[str],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    """Alert on rockets/limit attacks even when full-session minutes are absent."""
    thresholds = config["market_wide_reversal_radar"]
    rough: list[dict[str, Any]] = []
    for code, quote in quotes.items():
        if (
            code in excluded_codes
            or len(code) != 6
            or code[0] not in {"0", "3", "4", "6", "8", "9"}
        ):
            continue
        price = _float(quote.get("price"))
        pre_close = _float(quote.get("pre_close"))
        current_return = _float(quote.get("return_pct"))
        short_name = str(quote.get("short_name") or "")
        near_limit = bool(quote.get("near_limit_up"))
        minimum_amount = float(
            thresholds[
                (
                    "minimum_limit_alert_cumulative_amount_cny"
                    if near_limit
                    else "minimum_cumulative_amount_cny"
                )
            ]
        )
        if (
            price <= 0
            or pre_close <= 0
            or _float(quote.get("amount"))
            < minimum_amount
            or "ST" in short_name.upper()
            or "退" in short_name
            or (
                current_return
                < float(thresholds["rocket_alert_return_pct_gte"])
                and not near_limit
            )
        ):
            continue
        reference = reference_quotes.get(code) or {}
        reference_price = _float(reference.get("price"))
        reference_at = (
            reference.get("trade_time")
            or reference.get("observed_at")
        )
        if reference_at is not None and not isinstance(
            reference_at,
            datetime,
        ):
            reference_at = datetime.fromisoformat(
                str(reference_at).replace(" ", "T")
            )
        interval_minutes = (
            max(0.0, (now - reference_at).total_seconds() / 60.0)
            if isinstance(reference_at, datetime)
            else 0.0
        )
        interval_return = (
            (price / reference_price - 1.0) * 100.0
            if reference_price > 0
            else 0.0
        )
        has_fresh_reference = (
            reference_price > 0
            and interval_minutes > 0
            and interval_minutes * 60.0
            <= float(
                thresholds["rocket_alert_max_reference_age_seconds"]
            )
        )
        if (
            not near_limit
            and (
                not has_fresh_reference
                or interval_return
                < float(
                    thresholds[
                        "rocket_alert_interval_return_pct_gte"
                    ]
                )
            )
        ):
            continue
        rough.append(
            {
                "stock_code": code,
                "short_name": short_name,
                "price": price,
                "pre_close": pre_close,
                "current_return_pct": current_return,
                "near_limit_up": near_limit,
                "reference_interval_return_pct": interval_return,
                "reference_interval_minutes": interval_minutes,
            }
        )
    if not rough:
        return []
    industries = _load_primary_industries(
        get_kline_engine(),
        stock_codes={item["stock_code"] for item in rough},
        trade_date=trade_date,
    )
    candidates: list[dict[str, Any]] = []
    for item in rough:
        code = str(item["stock_code"])
        industry = industries.get(code) or {}
        score = min(
            99.0,
            65.0
            + _float(item["current_return_pct"]) * 2.0
            + max(
                0.0,
                _float(item["reference_interval_return_pct"]),
            )
            * 3.0,
        )
        raw = {
            "stock_name": (
                item["short_name"]
                or industry.get("short_name")
                or code
            ),
            "theme_name": industry.get("theme_name") or "未分类",
            "sector_role": "盘中极速拉升",
            "discovery_lane": "MARKET_WIDE_MOMENTUM_ALERT",
            "reference_interval_return_pct": item[
                "reference_interval_return_pct"
            ],
            "reference_interval_minutes": item[
                "reference_interval_minutes"
            ],
            "db_close": item["pre_close"],
            "stop_loss": item["pre_close"] * 0.985,
            "take_profit_2": item["pre_close"] * 1.08,
        }
        candidates.append(
            {
                "stock_code": code,
                "short_name": raw["stock_name"],
                "theme_code": industry.get("theme_code") or "",
                "strategy_version": config["strategy_version"],
                "raw_score": score,
                "initial_stop": raw["stop_loss"],
                "raw_features_json": raw,
            }
        )
    candidates.sort(
        key=lambda row: (
            -_float(row.get("raw_score")),
            str(row.get("stock_code") or ""),
        )
    )
    return candidates[
        : int(thresholds["maximum_rocket_alerts_per_tick"])
    ]


def _discover_locked_leader_substitutes(
    *,
    trade_date: date,
    quotes: dict[str, dict[str, Any]],
    momentum_candidates: list[dict[str, Any]],
    excluded_codes: set[str],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    """Discover tradeable followers when a same-industry leader is locked.

    The leader itself remains a WATCH alert.  Followers are discovered from
    the point-in-time QMT industry snapshot and still pass independent market,
    breadth and risk/reward gates before they can become a paper order.
    """
    rule = dict(config.get("leader_follower_radar") or {})
    if not bool(rule.get("enabled", True)):
        return []
    locked_by_theme: dict[str, dict[str, Any]] = {}
    for candidate in momentum_candidates:
        code = str(candidate.get("stock_code") or "").zfill(6)
        theme_code = str(candidate.get("theme_code") or "")
        if (
            theme_code
            and bool((quotes.get(code) or {}).get("near_limit_up"))
        ):
            current = locked_by_theme.get(theme_code)
            if (
                current is None
                or _float(candidate.get("raw_score"))
                > _float(current.get("raw_score"))
            ):
                locked_by_theme[theme_code] = candidate
    if not locked_by_theme:
        return []

    raw_industry_codes = sorted(
        {
            theme.split(":", 1)[1]
            for theme in locked_by_theme
            if theme.startswith("INDUSTRY:") and ":" in theme
        }
    )
    if not raw_industry_codes:
        return []
    statement = text(
        """
        SELECT stock_code, short_name, industry_code, industry_name
        FROM qmt_industry_member_snapshot
        WHERE snapshot_date = (
            SELECT MAX(snapshot_date)
            FROM qmt_industry_member_snapshot
            WHERE snapshot_date <= :trade_date
              AND quality_status = 'QMT_VALIDATED'
        )
          AND quality_status = 'QMT_VALIDATED'
          AND industry_code IN :industry_codes
        """
    ).bindparams(bindparam("industry_codes", expanding=True))
    with get_kline_engine().connect() as connection:
        members = [
            dict(row)
            for row in connection.execute(
                statement,
                {
                    "trade_date": trade_date,
                    "industry_codes": raw_industry_codes,
                },
            ).mappings().all()
        ]

    by_theme: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in members:
        by_theme[
            f"INDUSTRY:{row.get('industry_code') or ''}"
        ].append(row)
    output: list[dict[str, Any]] = []
    for theme_code, leader in locked_by_theme.items():
        leader_code = str(leader.get("stock_code") or "").zfill(6)
        leader_quote = quotes.get(leader_code) or {}
        leader_raw = _parse_json_object(leader.get("raw_features_json"))
        eligible: list[dict[str, Any]] = []
        for member in by_theme.get(theme_code) or []:
            code = str(member.get("stock_code") or "").zfill(6)
            quote = quotes.get(code) or {}
            current_return = _float(quote.get("return_pct"))
            amount = _float(quote.get("amount"))
            short_name = str(
                quote.get("short_name")
                or member.get("short_name")
                or code
            )
            if (
                code == leader_code
                or code in excluded_codes
                or _float(quote.get("price")) <= 0
                or bool(quote.get("near_limit_up"))
                or amount
                < float(rule.get("minimum_amount_cny", 80_000_000.0))
                or current_return
                < float(rule.get("minimum_current_return_pct", 0.3))
                or current_return
                > float(rule.get("maximum_current_return_pct", 7.5))
                or "ST" in short_name.upper()
                or "退" in short_name
            ):
                continue
            eligible.append(
                {
                    "stock_code": code,
                    "short_name": short_name,
                    "quote": quote,
                    "current_return_pct": current_return,
                    "amount": amount,
                }
            )
        eligible.sort(
            key=lambda item: (
                -_float(item["current_return_pct"]),
                -_float(item["amount"]),
                str(item["stock_code"]),
            )
        )
        for follower_rank, item in enumerate(
            eligible[
                : int(rule.get("maximum_followers_per_leader", 3))
            ],
            start=1,
        ):
            quote = item["quote"]
            price = _float(quote.get("price"))
            pre_close = _float(quote.get("pre_close"))
            stop = max(pre_close * 0.995, price * 0.97)
            target = pre_close * 1.095
            score = min(
                99.0,
                62.0
                + _float(item["current_return_pct"]) * 3.0
                + min(
                    8.0,
                    math.log10(
                        max(_float(item["amount"]), 1.0)
                        / 80_000_000.0
                    )
                    * 8.0,
                ),
            )
            raw = {
                "stock_name": item["short_name"],
                "theme_name": (
                    (by_theme.get(theme_code) or [{}])[0].get(
                        "industry_name"
                    )
                    or leader_raw.get("theme_name")
                    or theme_code
                ),
                "sector_role": "龙二" if follower_rank == 1 else "中军",
                "sector_rank": follower_rank + 1,
                "discovery_lane": "MARKET_WIDE_LEADER_SUBSTITUTE",
                "leader_code": leader_code,
                "leader_name": (
                    leader_raw.get("stock_name")
                    or leader.get("short_name")
                    or leader_code
                ),
                "leader_return_pct": _float(
                    leader_quote.get("return_pct")
                ),
                "db_close": pre_close,
                "stop_loss": stop,
                "take_profit_2": target,
            }
            output.append(
                {
                    "stock_code": item["stock_code"],
                    "short_name": item["short_name"],
                    "theme_code": theme_code,
                    "strategy_version": config["strategy_version"],
                    "raw_score": score,
                    "initial_stop": stop,
                    "raw_features_json": raw,
                }
            )
    output.sort(
        key=lambda row: (
            -_float(row.get("raw_score")),
            str(row.get("stock_code") or ""),
        )
    )
    return output[: int(rule.get("maximum_candidates_per_tick", 20))]


def _discover_market_wide_volume_bursts(
    *,
    trade_date: date,
    quotes: dict[str, dict[str, Any]],
    live_quote_metrics: dict[str, dict[str, float]],
    excluded_codes: set[str],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    """Find positive price/amount bursts from consecutive full-market quotes."""
    thresholds = config["market_wide_reversal_radar"]
    rough: list[dict[str, Any]] = []
    for code, metrics in live_quote_metrics.items():
        quote = quotes.get(code) or {}
        short_name = str(quote.get("short_name") or "")
        current_return = _float(quote.get("return_pct"))
        if (
            code in excluded_codes
            or len(code) != 6
            or code[0] not in {"0", "3", "4", "6", "8", "9"}
            or _float(quote.get("price")) <= 0
            or _float(quote.get("pre_close")) <= 0
            or "ST" in short_name.upper()
            or "退" in short_name
            or current_return
            < float(thresholds["burst_alert_current_return_pct_min"])
            or current_return
            > float(thresholds["burst_alert_current_return_pct_max"])
            or _float(metrics.get("amount_ratio"))
            < float(thresholds["burst_alert_amount_ratio_gte"])
            or _float(metrics.get("amount_delta"))
            < float(thresholds["burst_alert_interval_amount_cny"])
            or _float(metrics.get("price_return_pct"))
            < float(thresholds["burst_alert_price_return_pct_gte"])
        ):
            continue
        rough.append(
            {
                "stock_code": code,
                "short_name": short_name,
                "current_return_pct": current_return,
                **metrics,
            }
        )
    if not rough:
        return []
    industries = _load_primary_industries(
        get_kline_engine(),
        stock_codes={str(item["stock_code"]) for item in rough},
        trade_date=trade_date,
    )
    candidates: list[dict[str, Any]] = []
    for item in rough:
        code = str(item["stock_code"])
        quote = quotes[code]
        pre_close = _float(quote.get("pre_close"))
        industry = industries.get(code) or {}
        score = min(
            99.0,
            58.0
            + min(_float(item.get("amount_ratio")), 5.0) * 5.0
            + max(0.0, _float(item.get("price_return_pct"))) * 4.0
            + max(0.0, _float(item.get("current_return_pct"))) * 2.0,
        )
        raw = {
            "stock_name": (
                item.get("short_name")
                or industry.get("short_name")
                or code
            ),
            "theme_name": industry.get("theme_name") or "未分类",
            "sector_role": "盘中爆量上攻",
            "discovery_lane": "MARKET_WIDE_VOLUME_BURST",
            "reference_interval_return_pct": item.get(
                "price_return_pct"
            ),
            "reference_interval_seconds": item.get(
                "interval_seconds"
            ),
            "interval_amount_delta": item.get("amount_delta"),
            "live_snapshot_count": item.get("snapshot_count"),
            "intraday_amount_ratio": item.get("amount_ratio"),
            "db_close": pre_close,
            "stop_loss": pre_close * 0.99,
            "take_profit_2": pre_close * 1.08,
        }
        candidates.append(
            {
                "stock_code": code,
                "short_name": raw["stock_name"],
                "theme_code": industry.get("theme_code") or "",
                "strategy_version": config["strategy_version"],
                "raw_score": score,
                "initial_stop": raw["stop_loss"],
                "raw_features_json": raw,
            }
        )
    candidates.sort(
        key=lambda row: (
            -_float(row.get("raw_score")),
            str(row.get("stock_code") or ""),
        )
    )
    return candidates[
        : int(thresholds["maximum_volume_burst_alerts_per_tick"])
    ]


def _apply_limit_flags(
    engine: Engine,
    *,
    trade_date: date,
    quotes: dict[str, dict[str, Any]],
) -> None:
    if not quotes:
        return
    statement = text(
        """
        SELECT stock_code, limit_ratio
        FROM st_instrument_rule_v2
        WHERE stock_code IN :codes
          AND effective_from <= :trade_date
          AND (effective_to IS NULL OR effective_to >= :trade_date)
        ORDER BY effective_from DESC, rule_version DESC
        """
    ).bindparams(bindparam("codes", expanding=True))
    with engine.connect() as connection:
        rows = connection.execute(
            statement,
            {
                "codes": list(quotes),
                "trade_date": trade_date,
            },
        ).mappings().all()
    limits: dict[str, float] = {}
    for row in rows:
        code = str(row["stock_code"]).zfill(6)
        limits.setdefault(code, _float(row.get("limit_ratio"), 0.10))
    for code, quote in quotes.items():
        limit_ratio = limits.get(code, 0.10)
        return_pct = _float(quote.get("return_pct"))
        quote["limit_ratio"] = limit_ratio
        quote["near_limit_up"] = (
            limit_ratio > 0
            and return_pct >= limit_ratio * 100.0 - 0.5
        )


def _existing_entry_codes(
    engine: Engine,
    *,
    trade_date: date,
) -> set[str]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT DISTINCT stock_code
                FROM st_position_lot_v2
                WHERE account_id = :account_id
                  AND remaining_quantity > 0
                UNION
                SELECT DISTINCT stock_code
                FROM st_order_v2
                WHERE account_id = :account_id
                  AND side = 'BUY'
                  AND DATE(created_at) = :trade_date
                  AND status NOT IN ('CANCELLED','EXPIRED','REJECTED')
                UNION
                SELECT DISTINCT stock_code
                FROM st_intraday_activation_v2
                WHERE account_id = :account_id
                  AND trade_date = :trade_date
                  AND status IN
                      ('ORDER_CREATED','FILLED','PARTIALLY_FILLED')
                """
            ),
            {
                "account_id": ACCOUNT_ID,
                "trade_date": trade_date,
            },
        ).all()
    return {str(row[0]).zfill(6) for row in rows}


def _persist_watch_quotes(
    engine: Engine,
    *,
    state_id: str,
    trade_date: date,
    observed_at: datetime,
    candidate_codes: set[str],
    quotes: dict[str, dict[str, Any]],
    now: datetime,
) -> None:
    payloads = []
    for code in sorted(candidate_codes):
        quote = quotes.get(code)
        if not quote:
            continue
        source_quote_at = (
            quote.get("observed_at")
            or quote.get("trade_time")
            or observed_at
        )
        if not isinstance(source_quote_at, datetime):
            source_quote_at = datetime.fromisoformat(
                str(source_quote_at).replace(" ", "T")
            )
        source = str(
            quote.get("data_source")
            or (
                "gj_big_qmt_inner"
                if str(quote.get("source") or "").lower().startswith(
                    "gj_big_qmt"
                )
                else "qmt_minute_receipt"
            )
        )
        payloads.append(
            {
                "observation_id": canonical_json_hash(
                    {
                        "state_id": state_id,
                        "observed_at": observed_at,
                        "stock_code": code,
                    }
                )[:32],
                "state_id": state_id,
                "trade_date": trade_date,
                "observed_at": observed_at,
                "source_quote_at": source_quote_at,
                "stock_code": code,
                "price": _float(quote.get("price")),
                "pre_close": _float(quote.get("pre_close")),
                "volume": max(0.0, _float(quote.get("volume"))),
                "amount": max(0.0, _float(quote.get("amount"))),
                "source_provider": source[:80],
                "created_at": now,
            }
        )
    if not payloads:
        return
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT IGNORE INTO st_intraday_watch_quote_v2
                (observation_id, state_id, trade_date, observed_at,
                 source_quote_at, stock_code, price, pre_close, volume,
                 amount, source_provider, created_at)
                VALUES
                (:observation_id, :state_id, :trade_date, :observed_at,
                 :source_quote_at, :stock_code, :price, :pre_close,
                 :volume, :amount, :source_provider, :created_at)
                """
            ),
            payloads,
        )


def _watch_quote_amount_ratios(
    engine: Engine,
    *,
    trade_date: date,
    candidate_codes: set[str],
    now: datetime | None = None,
) -> dict[str, float]:
    metrics = _watch_quote_change_metrics(
        engine,
        trade_date=trade_date,
        candidate_codes=candidate_codes,
        now=now,
    )
    return {
        code: _float(item.get("amount_ratio"))
        for code, item in metrics.items()
    }


def _watch_quote_change_metrics(
    engine: Engine,
    *,
    trade_date: date,
    candidate_codes: set[str],
    now: datetime | None = None,
) -> dict[str, dict[str, float]]:
    if not candidate_codes:
        return {}
    statement = text(
        """
        SELECT stock_code, observed_at, price, amount
        FROM st_intraday_watch_quote_v2
        WHERE trade_date = :trade_date
          AND stock_code IN :codes
          AND observed_at >= :cutoff
        ORDER BY stock_code, observed_at DESC
        """
    ).bindparams(bindparam("codes", expanding=True))
    observed_now = now or datetime.combine(trade_date, time(15, 0))
    with engine.connect() as connection:
        rows = connection.execute(
            statement,
            {
                "trade_date": trade_date,
                "codes": list(candidate_codes),
                "cutoff": observed_now - timedelta(minutes=10),
            },
        ).mappings().all()
    series: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        code = str(row["stock_code"]).zfill(6)
        if len(series.setdefault(code, [])) < 3:
            series[code].append(dict(row))
    metrics: dict[str, dict[str, float]] = {}
    for code, latest_first in series.items():
        if len(latest_first) < 3:
            metrics[code] = {
                "amount_ratio": 0.0,
                "price_return_pct": 0.0,
                "interval_seconds": 0.0,
                "amount_delta": 0.0,
                "snapshot_count": float(len(latest_first)),
            }
            continue
        ordered = list(reversed(latest_first))
        intervals = []
        amount_deltas = []
        interval_seconds = []
        for left, right in zip(ordered, ordered[1:]):
            left_at = left["observed_at"]
            right_at = right["observed_at"]
            if not isinstance(left_at, datetime):
                left_at = datetime.fromisoformat(
                    str(left_at).replace(" ", "T")
                )
            if not isinstance(right_at, datetime):
                right_at = datetime.fromisoformat(
                    str(right_at).replace(" ", "T")
                )
            seconds = max(1.0, (right_at - left_at).total_seconds())
            amount_delta = max(
                0.0,
                _float(right["amount"]) - _float(left["amount"]),
            )
            intervals.append(amount_delta / seconds)
            amount_deltas.append(amount_delta)
            interval_seconds.append(seconds)
        baseline, latest = intervals[0], intervals[-1]
        first_price = _float(ordered[-2].get("price"))
        latest_price = _float(ordered[-1].get("price"))
        metrics[code] = {
            "amount_ratio": latest / baseline if baseline > 0 else 0.0,
            "price_return_pct": (
                (latest_price / first_price - 1.0) * 100.0
                if latest_price > 0 and first_price > 0
                else 0.0
            ),
            "interval_seconds": interval_seconds[-1],
            "amount_delta": amount_deltas[-1],
            "snapshot_count": float(len(latest_first)),
        }
    return metrics


def _persist_market_state(
    engine: Engine,
    *,
    trade_date: date,
    run_uid: str,
    previous_regime: str,
    point: MarketPoint | None,
    assessment: MarketAssessment,
    config: dict[str, Any],
    config_hash: str,
    now: datetime,
) -> str:
    observed_at = point.observed_at if point else now.replace(second=0, microsecond=0)
    state_id = canonical_json_hash(
        {
            "trade_date": trade_date,
            "observed_at": observed_at,
            "config_hash": config_hash,
        }
    )[:32]
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT IGNORE INTO st_intraday_market_state_v2
                (state_id, trade_date, observed_at, decision_run_uid,
                 previous_market_regime, state, quality_status, actionable,
                 observed_count, expected_count, coverage,
                 positive_breadth_pct, equal_weight_return_pct,
                 median_return_pct, confirming_points, source_provider,
                 config_version, config_hash, evidence_json, created_at)
                VALUES
                (:state_id, :trade_date, :observed_at, :run_uid,
                 :previous_regime, :state, :quality_status, :actionable,
                 :observed_count, :expected_count, :coverage,
                 :breadth, :average_return, :median_return,
                 :confirming_points, :source_provider, :config_version,
                 :config_hash, :evidence, :created_at)
                """
            ),
            {
                "state_id": state_id,
                "trade_date": trade_date,
                "observed_at": observed_at,
                "run_uid": run_uid,
                "previous_regime": previous_regime,
                "state": assessment.state,
                "quality_status": assessment.quality_status,
                "actionable": int(assessment.actionable),
                "observed_count": point.observed_count if point else 0,
                "expected_count": point.expected_count if point else 0,
                "coverage": point.coverage if point else 0,
                "breadth": point.positive_breadth_pct if point else 0,
                "average_return": (
                    point.equal_weight_return_pct if point else 0
                ),
                "median_return": point.median_return_pct if point else 0,
                "confirming_points": assessment.confirming_points,
                "source_provider": (
                    point.source if point else "qmt_minute_unavailable"
                ),
                "config_version": config["strategy_version"],
                "config_hash": config_hash,
                "evidence": _json(list(assessment.evidence)),
                "created_at": now,
            },
        )
    return state_id


def _persist_candidate_assessments(
    engine: Engine,
    *,
    state_id: str,
    run_uid: str,
    trade_date: date,
    observed_at: datetime,
    decisions: list[CandidateAssessment],
    now: datetime,
) -> None:
    if not decisions:
        return
    statement = text(
        """
        INSERT IGNORE INTO st_intraday_activation_v2
        (activation_id, state_id, account_id, decision_run_uid,
         trade_date, observed_at, stock_code, short_name, theme_code,
         theme_name, source_strategy_version, role, action, status,
         reason_code, current_price, current_return_pct,
         relative_strength_pct, intraday_amount_ratio,
         theme_positive_breadth_pct, theme_average_return_pct,
         raw_score, risk_reward_ratio, leader_code, leader_state,
         opening_target_fraction, evidence_json, intent_id, order_id,
         created_at, updated_at)
        VALUES
        (:activation_id, :state_id, :account_id, :run_uid,
         :trade_date, :observed_at, :stock_code, :short_name,
         :theme_code, :theme_name, :source_strategy_version, :role,
         :action, :status, :reason_code, :current_price,
         :current_return_pct, :relative_strength_pct,
         :intraday_amount_ratio, :theme_positive_breadth_pct,
         :theme_average_return_pct, :raw_score, :risk_reward_ratio,
         :leader_code, :leader_state, :opening_target_fraction,
         :evidence, NULL, NULL, :created_at, :created_at)
        """
    )
    payloads = []
    for item in decisions:
        payloads.append(
            {
                "activation_id": canonical_json_hash(
                    {
                        "state_id": state_id,
                        "stock_code": item.stock_code,
                    }
                )[:32],
                "state_id": state_id,
                "account_id": ACCOUNT_ID,
                "run_uid": run_uid,
                "trade_date": trade_date,
                "observed_at": observed_at,
                "stock_code": item.stock_code,
                "short_name": item.short_name[:128],
                "theme_code": item.theme_code[:80],
                "theme_name": item.theme_name[:160],
                "source_strategy_version": (
                    item.source_strategy_version[:80]
                ),
                "role": item.role[:40],
                "action": item.action,
                "status": item.status,
                "reason_code": item.reason_code,
                "current_price": item.current_price,
                "current_return_pct": item.current_return_pct,
                "relative_strength_pct": item.relative_strength_pct,
                "intraday_amount_ratio": item.intraday_amount_ratio,
                "theme_positive_breadth_pct": (
                    item.theme_positive_breadth_pct
                ),
                "theme_average_return_pct": (
                    item.theme_average_return_pct
                ),
                "raw_score": item.raw_score,
                "risk_reward_ratio": item.risk_reward_ratio,
                "leader_code": item.leader_code,
                "leader_state": item.leader_state,
                "opening_target_fraction": (
                    item.opening_target_fraction
                ),
                "evidence": _json(list(item.evidence)),
                "created_at": now,
            }
        )
    with engine.begin() as connection:
        connection.execute(statement, payloads)


def _update_activation_orders(
    engine: Engine,
    *,
    state_id: str,
    competition: dict[str, Any],
    now: datetime,
) -> None:
    selected = {
        str(item["stock_code"]): item
        for item in competition.get("selected") or []
    }
    rejected = {
        str(item["stock_code"]): item
        for item in competition.get("rejected") or []
    }
    with engine.begin() as connection:
        for code, item in selected.items():
            connection.execute(
                text(
                    """
                    UPDATE st_intraday_activation_v2
                    SET status = 'ORDER_CREATED',
                        intent_id = :intent_id,
                        order_id = :order_id,
                        updated_at = :updated_at
                    WHERE state_id = :state_id
                      AND stock_code = :stock_code
                    """
                ),
                {
                    "intent_id": item.get("intent_id"),
                    "order_id": item.get("order_id"),
                    "updated_at": now,
                    "state_id": state_id,
                    "stock_code": code,
                },
            )
        for code, item in rejected.items():
            connection.execute(
                text(
                    """
                    UPDATE st_intraday_activation_v2
                    SET status = 'RISK_REJECTED',
                        reason_code = :reason_code,
                        updated_at = :updated_at
                    WHERE state_id = :state_id
                      AND stock_code = :stock_code
                      AND status = 'ACTIVATABLE'
                    """
                ),
                {
                    "reason_code": str(
                        item.get("rejection_code") or "RISK_REJECTED"
                    )[:100],
                    "updated_at": now,
                    "state_id": state_id,
                    "stock_code": code,
                },
            )


def run_intraday_activation(
    engine: Engine,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Run one deterministic, paper-only intraday decision tick."""
    now = now or datetime.now()
    config, config_hash = load_frozen_json(CONFIG_PATH)
    session = config["session"]
    radar_config = config.get("market_wide_reversal_radar") or {}
    hhmm = now.hour * 100 + now.minute
    if not is_trade_day(engine, now.date()):
        return {
            "status": "skipped",
            "reason": "NON_TRADING_DAY",
            "trade_date": now.date().isoformat(),
            "real_order_count": 0,
        }
    latest_new_entry = max(
        _hhmm(session["last_new_entry"]),
        _hhmm(
            str(
                radar_config.get(
                    "last_new_entry",
                    session["last_new_entry"],
                )
            )
        ),
    )
    if hhmm < _hhmm(session["start"]) or hhmm > latest_new_entry:
        return {
            "status": "skipped",
            "reason": "OUTSIDE_NEW_ENTRY_SESSION",
            "trade_date": now.date().isoformat(),
            "real_order_count": 0,
        }
    run_uid, previous_regime, candidates = _load_watch_candidates(
        engine,
        now=now,
        source_versions=list(config["source_strategy_scope"]),
    )
    minute_points, minute_quotes, amount_ratios = _load_minute_market(
        engine,
        trade_date=now.date(),
        now=now,
        config=config,
    )
    current_point, current_quotes = _load_current_market(
        engine,
        now=now,
        config=config,
    )
    primary_quality = dict(config["data_quality"])
    primary_end_to_end = qmt_primary_health(
        engine,
        now=now,
        config=primary_quality,
    )
    primary_usable = bool(
        primary_end_to_end.get("healthy")
    ) and _point_meets_quality(
        current_point,
        now=now,
        expected_provider=str(primary_quality["required_provider"]),
        minimum_observed_stocks=int(
            primary_quality["minimum_observed_stocks"]
        ),
        minimum_universe_coverage=float(
            primary_quality["minimum_universe_coverage"]
        ),
        maximum_age_seconds=float(
            primary_quality["maximum_minute_age_seconds"]
        ),
    )
    if current_point is not None and not bool(
        primary_end_to_end.get("healthy")
    ):
        current_point = replace(
            current_point,
            source="QMT_END_TO_END_BLOCKED",
        )
    public_point: MarketPoint | None = None
    public_quotes: dict[str, dict[str, Any]] = {}
    failover_collection: dict[str, Any] | None = None
    if not primary_usable:
        (
            public_point,
            public_quotes,
            failover_collection,
        ) = _load_public_failover_market(
            engine,
            now=now,
            config=config,
            collect_if_missing=True,
        )
        if failover_collection is not None:
            refresh_now = max(now, datetime.now())
            refreshed_point, refreshed_quotes = _load_current_market(
                engine,
                now=refresh_now,
                config=config,
            )
            refreshed_health = qmt_primary_health(
                engine,
                now=refresh_now,
                config=primary_quality,
            )
            if bool(
                refreshed_health.get("healthy")
            ) and _point_meets_quality(
                refreshed_point,
                now=refresh_now,
                expected_provider=str(
                    primary_quality["required_provider"]
                ),
                minimum_observed_stocks=int(
                    primary_quality["minimum_observed_stocks"]
                ),
                minimum_universe_coverage=float(
                    primary_quality["minimum_universe_coverage"]
                ),
                maximum_age_seconds=float(
                    primary_quality["maximum_minute_age_seconds"]
                ),
            ):
                current_point = refreshed_point
                current_quotes = refreshed_quotes
                primary_usable = True
    failover = dict(config.get("public_quote_failover") or {})
    public_quality_now = (
        max(now, public_point.observed_at)
        if public_point is not None
        else now
    )
    public_usable = _point_meets_quality(
        public_point,
        now=public_quality_now,
        expected_provider=str(
            failover.get("source_provider")
            or "PUBLIC_QUOTE_QUORUM_V1"
        ),
        minimum_observed_stocks=int(
            failover.get("minimum_observed_stocks") or 5000
        ),
        minimum_universe_coverage=float(
            failover.get("minimum_universe_coverage") or 0.95
        ),
        maximum_age_seconds=float(
            failover.get("maximum_snapshot_age_seconds") or 45
        ),
    )
    if primary_usable:
        selected_point = current_point
    elif public_usable:
        selected_point = public_point
    elif current_point is not None:
        # Retain the partial QMT point only to explain why the gate is closed.
        selected_point = current_point
    elif minute_points:
        selected_point = minute_points[-1]
    else:
        selected_point = public_point

    if selected_point is not None and (
        primary_usable or public_usable
    ):
        prior_points = _recent_persisted_market_points(
            engine,
            before=selected_point.observed_at,
            provider=selected_point.source,
            limit=max(
                0,
                int(config["data_quality"]["confirmation_points"]) - 1,
            ),
        )
        points = prior_points + [selected_point]
    else:
        points = minute_points
        if selected_point is not None and (
            not points
            or points[-1].observed_at < selected_point.observed_at
        ):
            points = points + [selected_point]
    # Market confirmation chooses the more complete source, while stock-level
    # radar detection always overlays every fresh current quote on the latest
    # canonical minute. Source priority is minute < public quorum < QMT, so a
    # healthy QMT row always takes over immediately when the terminal recovers.
    quotes = {
        **minute_quotes,
        **public_quotes,
        **(current_quotes if primary_usable else {}),
    }
    market = assess_market(
        points,
        previous_regime=previous_regime,
        now=(
            max(now, points[-1].observed_at)
            if points
            else now
        ),
        config=config,
    )
    latest_point = points[-1] if points else None
    latest_source = str(
        latest_point.source if latest_point else ""
    ).upper()
    failover_provider = str(
        failover.get("source_provider")
        or "PUBLIC_QUOTE_QUORUM_V1"
    ).upper()
    using_public_failover = bool(
        public_usable and latest_source == failover_provider
    )
    state_id = _persist_market_state(
        engine,
        trade_date=now.date(),
        run_uid=run_uid,
        previous_regime=previous_regime,
        point=latest_point,
        assessment=market,
        config=config,
        config_hash=config_hash,
        now=now,
    )
    if not run_uid:
        return {
            "status": "blocked",
            "reason": "DECISION_WATCH_POOL_MISSING",
            "market_state": market.state,
            "state_id": state_id,
            "real_order_count": 0,
        }

    _apply_limit_flags(
        engine,
        trade_date=now.date(),
        quotes=quotes,
    )
    # Existing positions are managed on every tick even when new entries are
    # blocked. This is what makes exits independent of a fixed holding period.
    position_result = monitor_positions(
        engine,
        trade_date=now.date(),
        run_uid=run_uid,
        as_of=now,
        market_price_overrides={
            code: decimal_value(row.get("price"))
            for code, row in quotes.items()
        },
    )
    observed_at = latest_point.observed_at if latest_point else now
    daily_candidate_codes = {
        str(row.get("stock_code") or "").zfill(6)
        for row in candidates
    }
    tracking_codes = {
        code
        for code, quote in quotes.items()
        if _float(quote.get("price")) > 0
        and _float(quote.get("pre_close")) > 0
        and _float(quote.get("amount"))
        >= float(radar_config.get("tracking_minimum_amount_cny", 50000000))
        and float(radar_config.get("tracking_return_pct_min", -3.0))
        <= _float(quote.get("return_pct"))
        <= float(radar_config.get("tracking_return_pct_max", 10.5))
    }
    tracking_observed_at = (
        latest_point.observed_at
        if latest_point is not None
        else observed_at
    )
    if radar_config.get("enabled") and tracking_codes:
        _persist_watch_quotes(
            engine,
            state_id=state_id,
            trade_date=now.date(),
            observed_at=tracking_observed_at,
            candidate_codes=tracking_codes,
            quotes=quotes,
            now=now,
        )
    live_quote_metrics = _watch_quote_change_metrics(
        engine,
        trade_date=now.date(),
        candidate_codes=tracking_codes,
        now=tracking_observed_at,
    )
    radar_candidates: list[dict[str, Any]] = []
    reversal_candidates: list[dict[str, Any]] = []
    momentum_candidates: list[dict[str, Any]] = []
    leader_substitute_candidates: list[dict[str, Any]] = []
    volume_burst_candidates: list[dict[str, Any]] = []
    radar_session_active = (
        bool(radar_config.get("enabled"))
        and hhmm >= _hhmm(str(radar_config.get("start", session["start"])))
        and hhmm <= _hhmm(
            str(
                radar_config.get(
                    "last_new_entry",
                    session["last_new_entry"],
                )
            )
        )
    )
    if radar_session_active:
        momentum_candidates = _discover_market_wide_momentum_alerts(
            trade_date=now.date(),
            now=now,
            quotes=quotes,
            reference_quotes=minute_quotes,
            excluded_codes=daily_candidate_codes,
            config=config,
        )
        momentum_codes = {
            str(row.get("stock_code") or "").zfill(6)
            for row in momentum_candidates
        }
        leader_substitute_candidates = (
            _discover_locked_leader_substitutes(
                trade_date=now.date(),
                quotes=quotes,
                momentum_candidates=momentum_candidates,
                excluded_codes=daily_candidate_codes | momentum_codes,
                config=config,
            )
        )
        leader_substitute_codes = {
            str(row.get("stock_code") or "").zfill(6)
            for row in leader_substitute_candidates
        }
        volume_burst_candidates = _discover_market_wide_volume_bursts(
            trade_date=now.date(),
            quotes=quotes,
            live_quote_metrics=live_quote_metrics,
            excluded_codes=(
                daily_candidate_codes
                | momentum_codes
                | leader_substitute_codes
            ),
            config=config,
        )
        volume_burst_codes = {
            str(row.get("stock_code") or "").zfill(6)
            for row in volume_burst_candidates
        }
        reversal_candidates = _discover_market_wide_reversals(
            trade_date=now.date(),
            now=now,
            quotes=quotes,
            live_quote_metrics=live_quote_metrics,
            excluded_codes=(
                daily_candidate_codes
                | momentum_codes
                | leader_substitute_codes
                | volume_burst_codes
            ),
            config=config,
        )
        radar_candidates = (
            reversal_candidates
            + momentum_candidates
            + leader_substitute_candidates
            + volume_burst_candidates
        )
    all_candidates = candidates + radar_candidates
    if not all_candidates:
        return {
            "status": "blocked",
            "reason": "VALID_INTRADAY_CANDIDATE_EMPTY",
            "market_state": market.state,
            "state_id": state_id,
            "position_monitor": position_result,
            "watch_candidate_count": 0,
            "radar_candidate_count": 0,
            "real_order_count": 0,
        }
    candidate_codes = {
        str(row.get("stock_code") or "").zfill(6)
        for row in all_candidates
    }
    _persist_watch_quotes(
        engine,
        state_id=state_id,
        trade_date=now.date(),
        observed_at=observed_at,
        candidate_codes=candidate_codes,
        quotes=quotes,
        now=now,
    )
    if latest_source in {
        str(primary_quality["required_provider"]).upper(),
        failover_provider,
    }:
        amount_ratios = _watch_quote_amount_ratios(
            engine,
            trade_date=now.date(),
            candidate_codes=candidate_codes,
            now=tracking_observed_at,
        )
        amount_ratios = {
            **amount_ratios,
            **{
                code: _float(metric.get("amount_ratio"))
                for code, metric in live_quote_metrics.items()
                if code in candidate_codes
                and _float(metric.get("amount_ratio")) > 0
            },
        }
    themes = {
        str(row.get("theme_code") or "")
        for row in all_candidates
        if str(row.get("theme_code") or "")
    }
    memberships = _load_theme_memberships(
        get_kline_engine(),
        themes,
    )
    theme_state = _theme_metrics(memberships, quotes)
    market_return = (
        latest_point.equal_weight_return_pct if latest_point else 0.0
    )
    decisions = (
        select_theme_activations(
            candidates,
            market=market,
            market_return_pct=market_return,
            quotes=quotes,
            amount_ratios=amount_ratios,
            theme_metrics=theme_state,
            config=config,
        )
        if candidates
        else []
    )
    if hhmm > _hhmm(session["last_new_entry"]):
        decisions = [
            (
                CandidateAssessment(
                    **{
                        **item.__dict__,
                        "action": "WATCH",
                        "status": "WATCHING",
                        "reason_code": "OUTSIDE_DAILY_ENTRY_WINDOW",
                    }
                )
                if item.action in {
                    "ACTIVATE_PROBE",
                    "ACTIVATE_SUBSTITUTE",
                }
                else item
            )
            for item in decisions
        ]
    radar_decisions = (
        select_reversal_activations(
            radar_candidates,
            market=market,
            market_return_pct=market_return,
            market_breadth_pct=(
                latest_point.positive_breadth_pct
                if latest_point
                else 0.0
            ),
            quotes=quotes,
            theme_metrics=theme_state,
            config=config,
        )
        if radar_candidates
        else []
    )
    decisions.extend(radar_decisions)
    existing_codes = _existing_entry_codes(
        engine,
        trade_date=now.date(),
    )
    decisions = [
        (
            CandidateAssessment(
                **{
                    **item.__dict__,
                    "action": "WATCH",
                    "status": "WATCHING",
                    "reason_code": "DUPLICATE_ENTRY_SAME_DAY_BLOCKED",
                }
            )
            if item.stock_code in existing_codes
            else item
        )
        for item in decisions
    ]
    if using_public_failover:
        decisions = [
            CandidateAssessment(
                **{
                    **item.__dict__,
                    "opening_target_fraction": (
                        _failover_opening_fraction(
                            item.opening_target_fraction,
                            config=config,
                        )
                    ),
                    "evidence": tuple(
                        list(item.evidence)
                        + [
                            "QMT主源不可用，公共多源替补仅用于模拟盘；"
                            "开仓比例已自动减半且单票不超过5%"
                        ]
                    ),
                }
            )
            for item in decisions
        ]
    _persist_candidate_assessments(
        engine,
        state_id=state_id,
        run_uid=run_uid,
        trade_date=now.date(),
        observed_at=observed_at,
        decisions=decisions,
        now=now,
    )
    activatable = [
        item
        for item in decisions
        if item.action
        in {
            "ACTIVATE_PROBE",
            "ACTIVATE_SUBSTITUTE",
            "ACTIVATE_REVERSAL_PROBE",
            "ACTIVATE_VOLUME_PROBE",
        }
    ]
    activatable.sort(
        key=lambda item: (
            -item.raw_score,
            -item.relative_strength_pct,
            item.stock_code,
        )
    )
    activatable = activatable[
        : int(config["candidate_activation"]["maximum_entries_per_tick"])
    ]
    competition: dict[str, Any] = {
        "selected": [],
        "rejected": [],
        "intent_count": 0,
        "order_count": 0,
    }
    if activatable:
        with engine.begin() as connection:
            account = connection.execute(
                text(
                    """
                    SELECT * FROM st_trade_account_v2
                    WHERE account_id = :account_id
                    FOR UPDATE
                    """
                ),
                {"account_id": ACCOUNT_ID},
            ).mappings().first()
            if not account:
                raise RuntimeError("V2 paper account is missing")
            by_code = {
                str(row.get("stock_code") or "").zfill(6): row
                for row in all_candidates
            }
            planner_candidates = []
            for item in activatable:
                source = by_code[item.stock_code]
                raw = _parse_json_object(
                    source.get("raw_features_json")
                )
                current = Decimal(str(item.current_price))
                limit_price = current * Decimal("1.003")
                stop = decimal_value(
                    source.get("initial_stop")
                    or raw.get("stop_loss")
                )
                planner_candidates.append(
                    {
                        "stock_code": item.stock_code,
                        "strategy_version": (
                            item.source_strategy_version
                        ),
                        "expected_return_lower_bound": None,
                        "raw_score": item.raw_score,
                        "risk_reward_ratio": item.risk_reward_ratio,
                        "entry_price": limit_price,
                        "initial_stop": stop,
                        "invalidation_condition": (
                            "盘中强势或板块广度失效、保护位触发时退出"
                        ),
                        "evidence": list(item.evidence),
                        "theme_code": item.theme_code,
                        "opening_target_fraction": (
                            item.opening_target_fraction
                        ),
                        "allow_minimum_board_lot": (
                            item.action
                            in {
                                "ACTIVATE_REVERSAL_PROBE",
                                "ACTIVATE_VOLUME_PROBE",
                            }
                        ),
                        "minimum_board_lot_max_weight": (
                            radar_config.get(
                                "minimum_board_lot_max_account_weight",
                                0.08,
                            )
                        ),
                        "reason_code": item.reason_code,
                    }
                )
            competition = persist_portfolio_competition(
                connection,
                run_uid=run_uid,
                trade_date=now.date(),
                account=dict(account),
                market_regime=market.execution_regime,
                candidates=planner_candidates,
                execution_at=now,
                execution_expires_at=datetime.combine(
                    now.date(),
                    time(
                        max(
                            _hhmm(session["order_expiry"]),
                            _hhmm(
                                str(
                                    radar_config.get(
                                        "order_expiry",
                                        session["order_expiry"],
                                    )
                                )
                            ),
                        )
                        // 100,
                        max(
                            _hhmm(session["order_expiry"]),
                            _hhmm(
                                str(
                                    radar_config.get(
                                        "order_expiry",
                                        session["order_expiry"],
                                    )
                                )
                            ),
                        )
                        % 100,
                    ),
                ),
                reason_code="WATCH_STOCK_INTRADAY_OUTPERFORMANCE",
            )
        _update_activation_orders(
            engine,
            state_id=state_id,
            competition=competition,
            now=now,
        )
    return {
        "status": (
            "orders_created"
            if competition.get("order_count")
            else "observing"
            if market.quality_status == "PASS"
            else "blocked"
        ),
        "trade_date": now.date().isoformat(),
        "observed_at": observed_at.isoformat(sep=" "),
        "state_id": state_id,
        "market_state": market.state,
        "market_actionable": market.actionable,
        "market_source_provider": latest_source,
        "public_failover_active": using_public_failover,
        "public_failover_collection": failover_collection,
        "market_evidence": list(market.evidence),
        "watch_candidate_count": len(candidates),
        "radar_candidate_count": len(radar_candidates),
        "waterline_candidate_count": len(reversal_candidates),
        "momentum_alert_count": len(momentum_candidates),
        "leader_substitute_candidate_count": len(
            leader_substitute_candidates
        ),
        "volume_burst_candidate_count": len(volume_burst_candidates),
        "evaluated_candidate_count": len(decisions),
        "activatable_count": len(activatable),
        "selected": competition.get("selected") or [],
        "rejected": competition.get("rejected") or [],
        "position_monitor": position_result,
        "real_order_count": 0,
    }
