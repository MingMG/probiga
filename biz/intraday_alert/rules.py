# -*- coding: utf-8 -*-
"""Pure, evidence-first rules for event-driven intraday commentary.

The public entry point is :func:`evaluate_events`.  It deliberately accepts
plain dictionaries so the collector and state machine can evolve without
pulling database or delivery concerns into the rule layer.

Canonical observation shape (common aliases are accepted)::

    {
        "snapshot_at": "2026-08-13 10:42:00",
        "coverage": 0.96,
        "source_provider": "qmt_full_tick",
        "market": {
            "median_return_pct": -0.35,
            "positive_breadth_pct": 38,
            "equal_weight_return_pct": -0.28,
            "amount_delta": 1.2e10,
            "cap_weighted_return_pct": 0.10,
        },
        "sectors": [{"code": "SW1:电子", "name": "电子", ...}],
        "key_stocks": [{"code": "000001", "name": "示例", ...}],
        "styles": [{"code": "large", "name": "大盘", ...}],
        "benchmarks": [{"code": "510300", "instrument_type": "ETF", ...}],
    }

All return values are percentage points (``0.35`` means 0.35%), while breadth
and coverage accept either fractions or percentages.  Rules never identify a
participant from quote data and never infer trade direction from turnover.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Callable


SUSPECTED = "SUSPECTED"
ENHANCED = "ENHANCED"
CONFIRMED = "CONFIRMED"
INVALIDATED = "INVALIDATED"

MARKET_REVERSAL = "market_reversal"
SECTOR_SPREAD = "sector_spread"
SECTOR_EBB = "sector_ebb"
KEY_STOCK = "key_stock"
STYLE_SEESAW = "style_seesaw"
BROAD_INDEX_SUPPORT = "broad_index_support"


_MISSING = object()


def _number(value: Any, default: float | None = None) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    if result != result or result in (float("inf"), float("-inf")):
        return default
    return result


def _first_number(row: Mapping[str, Any] | None, *keys: str, default: float | None = None) -> float | None:
    if not isinstance(row, Mapping):
        return default
    for key in keys:
        if key in row and row[key] is not None:
            value = _number(row[key])
            if value is not None:
                return value
    return default


def _first_text(row: Mapping[str, Any] | None, *keys: str, default: str = "") -> str:
    if not isinstance(row, Mapping):
        return default
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return default


def _bool(row: Mapping[str, Any] | None, *keys: str) -> bool | None:
    if not isinstance(row, Mapping):
        return None
    for key in keys:
        if key not in row or row[key] is None:
            continue
        value = row[key]
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        normalized = str(value).strip().lower()
        if normalized in {"1", "true", "yes", "y", "up", "above", "是"}:
            return True
        if normalized in {"0", "false", "no", "n", "down", "below", "否"}:
            return False
    return None


def _breadth(row: Mapping[str, Any] | None) -> float | None:
    value = _first_number(
        row,
        "positive_breadth_pct",
        "breadth_pct",
        "breadth",
        "up_ratio_pct",
        "positive_ratio_pct",
        "advance_ratio",
    )
    if value is not None and 0 <= value <= 1:
        return value * 100.0
    return value


def _coverage(observation: Mapping[str, Any]) -> float:
    value = _first_number(observation, "coverage", "coverage_ratio", "data_coverage")
    if value is None:
        expected = _first_number(observation, "expected_count")
        observed = _first_number(observation, "observed_count")
        value = observed / expected if expected and observed is not None else 1.0
    if value > 1:
        value /= 100.0
    return max(0.0, min(1.0, value))


def _market(observation: Mapping[str, Any]) -> dict[str, Any]:
    value = observation.get("market")
    return dict(value) if isinstance(value, Mapping) else dict(observation)


def _records(observation: Mapping[str, Any], *keys: str) -> list[dict[str, Any]]:
    raw: Any = None
    for key in keys:
        if key in observation:
            raw = observation.get(key)
            break
    if raw is None:
        return []
    if isinstance(raw, Mapping):
        # Collector containers use ``{"items": [...]}``.  Key-stock containers
        # intentionally expose three overlapping ranked lists; merge and dedupe.
        if isinstance(raw.get("items"), Sequence) and not isinstance(raw.get("items"), (str, bytes)):
            return [dict(item) for item in raw["items"] if isinstance(item, Mapping)]
        ranked_keys = ("top_turnover", "leaders", "laggards")
        if any(isinstance(raw.get(key), Sequence) for key in ranked_keys):
            result: list[dict[str, Any]] = []
            seen: set[str] = set()
            for ranked_key in ranked_keys:
                values = raw.get(ranked_key)
                if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
                    continue
                for value in values:
                    if not isinstance(value, Mapping):
                        continue
                    item = dict(value)
                    identity = _first_text(item, "code", "stock_code", "name", "short_name")
                    if identity and identity in seen:
                        continue
                    if identity:
                        seen.add(identity)
                    result.append(item)
            return result
        # A mapping keyed by code/name is convenient for collectors and tests.
        metric_keys = {
            "code", "name", "return_pct", "change_pct", "median_return_pct",
            "breadth", "breadth_pct", "amount_delta", "instrument_type",
        }
        if metric_keys.intersection(raw):
            return [dict(raw)]
        result: list[dict[str, Any]] = []
        for key, value in raw.items():
            if not isinstance(value, Mapping):
                continue
            item = dict(value)
            item.setdefault("code", str(key))
            item.setdefault("name", str(key))
            result.append(item)
        return result
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        return [dict(item) for item in raw if isinstance(item, Mapping)]
    return []


def _identity(row: Mapping[str, Any], default_type: str) -> tuple[str, str, str]:
    code = _first_text(
        row, "code", "stock_code", "sector_code", "industry_code", "instrument_code", "style_code"
    )
    name = _first_text(
        row,
        "name",
        "short_name",
        "sector_name",
        "industry_name",
        "instrument_name",
        "style_name",
        default=code,
    )
    kind = _first_text(row, "type", "instrument_type", "sector_type", default=default_type)
    if not code:
        code = name
    if not name:
        name = code or "未命名对象"
    return code, name, kind


def _return(row: Mapping[str, Any] | None) -> float | None:
    return _first_number(
        row,
        "median_return_pct",
        "return_pct",
        "change_pct",
        "avg_change_pct",
        "equal_weight_return_pct",
    )


def _equal_weight(row: Mapping[str, Any] | None) -> float | None:
    return _first_number(row, "equal_weight_return_pct", "equal_weight_pct", "equal_weight")


def _amount(row: Mapping[str, Any] | None) -> float | None:
    return _first_number(row, "amount_delta", "turnover_delta", "incremental_amount", "amount")


def _amount_ratio(current: Mapping[str, Any], previous: Mapping[str, Any] | None) -> float | None:
    explicit = _first_number(
        current,
        "amount_ratio",
        "amount_ratio_5m",
        "historical_amount_ratio",
        "short_amount_ratio",
        "baseline_ratio",
        "amount_delta_ratio",
        "volume_ratio",
        "same_time_amount_ratio",
        "turnover_ratio",
    )
    if explicit is not None:
        return explicit
    now = _amount(current)
    before = _amount(previous)
    if now is None or before is None or before <= 0:
        return None
    return now / before


def _metric_delta(
    current: Mapping[str, Any] | None,
    previous: Mapping[str, Any] | None,
    getter: Callable[[Mapping[str, Any] | None], float | None],
) -> float | None:
    now = getter(current)
    before = getter(previous)
    return now - before if now is not None and before is not None else None


def _find_record(rows: Iterable[Mapping[str, Any]], code: str, name: str = "") -> dict[str, Any] | None:
    for row in rows:
        row_code, row_name, _ = _identity(row, "")
        if code and row_code == code:
            return dict(row)
        if name and row_name == name:
            return dict(row)
    return None


def _source_time(observation: Mapping[str, Any]) -> str:
    return _first_text(
        observation,
        "source_snapshot_at",
        "snapshot_at",
        "observed_at",
        "data_time",
        default="",
    )


def _state(support_count: int, evidence_count: int, coverage: float) -> str:
    """Translate persistence and independent evidence into a conservative state."""

    if support_count >= 4 and evidence_count >= 3 and coverage >= 0.80:
        return CONFIRMED
    if support_count >= 2 and evidence_count >= 2 and coverage >= 0.65:
        return ENHANCED
    return SUSPECTED


def _severity(state: str, magnitude: float = 0.0) -> int:
    base = {SUSPECTED: 1, ENHANCED: 3, CONFIRMED: 4, INVALIDATED: 0}.get(state, 1)
    return min(5, base + (1 if state != INVALIDATED and magnitude >= 1.5 else 0))


def _event(
    *,
    event_key: str,
    event_type: str,
    state: str,
    subject: Mapping[str, str],
    direction: str,
    magnitude: float,
    facts: list[str],
    metrics: Mapping[str, Any],
    support_count: int,
    inference: str,
    boundaries: list[str],
    upgrade_condition: str,
    invalidation_condition: str,
    observation: Mapping[str, Any],
) -> dict[str, Any]:
    subject_dict = dict(subject)
    code = str(subject_dict.get("code") or subject_dict.get("name") or "market")
    name = str(subject_dict.get("name") or code)
    evidence = {
        "facts": list(facts),
        "metrics": dict(metrics),
        "evidence_count": len(facts),
        "support_count": support_count,
        "source_time": _source_time(observation),
    }
    return {
        "event_key": event_key,
        "event_type": event_type,
        "state": state,
        "target_state": state,
        "subject": subject_dict,
        "subject_code": code,
        "subject_name": name,
        "direction": direction,
        "severity": _severity(state, magnitude),
        "detected_at": _source_time(observation),
        "evidence": evidence,
        # Top-level copies make state/outbox serialization and ad-hoc clients simple.
        "facts": list(facts),
        "inference": inference,
        "boundaries": list(boundaries),
        "upgrade_condition": upgrade_condition,
        "invalidation_condition": invalidation_condition,
    }


def _market_values(observation: Mapping[str, Any]) -> tuple[float | None, float | None, float | None]:
    market = _market(observation)
    return _return(market), _breadth(market), _equal_weight(market)


def _market_step(previous: Mapping[str, Any], current: Mapping[str, Any], direction: int) -> tuple[bool, int]:
    before_median, before_breadth, before_equal = _market_values(previous)
    now_median, now_breadth, now_equal = _market_values(current)
    changes = (
        (now_median - before_median) * direction if now_median is not None and before_median is not None else None,
        (now_breadth - before_breadth) * direction if now_breadth is not None and before_breadth is not None else None,
        (now_equal - before_equal) * direction if now_equal is not None and before_equal is not None else None,
    )
    hits = sum(
        value is not None and value >= threshold
        for value, threshold in zip(changes, (0.22, 7.0, 0.18))
    )
    return hits >= 2, hits


def _consecutive_pair_hits(
    observations: Sequence[Mapping[str, Any]],
    predicate: Callable[[Mapping[str, Any], Mapping[str, Any]], bool],
) -> int:
    hits = 0
    for index in range(len(observations) - 1, 0, -1):
        if not predicate(observations[index - 1], observations[index]):
            break
        hits += 1
    return hits


def _market_reversal_events(
    current: Mapping[str, Any], history: Sequence[Mapping[str, Any]], coverage: float
) -> list[dict[str, Any]]:
    if not history:
        return []
    previous = history[-1]
    events: list[dict[str, Any]] = []
    now_median, now_breadth, now_equal = _market_values(current)
    old_median, old_breadth, old_equal = _market_values(previous)
    for direction, label in ((1, "UP"), (-1, "DOWN")):
        hit, evidence_count = _market_step(previous, current, direction)
        if not hit:
            continue
        sequence = [*history, current]
        support = _consecutive_pair_hits(
            sequence,
            lambda left, right, sign=direction: _market_step(left, right, sign)[0],
        )
        state = _state(support, evidence_count, coverage)
        verb = "回升" if direction > 0 else "回落"
        inference = (
            "市场内部多数指标同步改善，盘面出现向上转折特征。"
            if direction > 0
            else "市场内部多数指标同步走弱，盘面出现向下转折特征。"
        )
        facts: list[str] = []
        metrics: dict[str, Any] = {}
        if now_median is not None and old_median is not None:
            facts.append(f"全市场收益中位数由{old_median:.2f}%{verb}至{now_median:.2f}%")
            metrics.update(median_before=old_median, median_current=now_median)
        if now_breadth is not None and old_breadth is not None:
            facts.append(f"上涨覆盖率由{old_breadth:.1f}%{verb}至{now_breadth:.1f}%")
            metrics.update(breadth_before=old_breadth, breadth_current=now_breadth)
        if now_equal is not None and old_equal is not None:
            facts.append(f"等权收益由{old_equal:.2f}%{verb}至{now_equal:.2f}%")
            metrics.update(equal_weight_before=old_equal, equal_weight_current=now_equal)
        amount = _amount(_market(current))
        if amount is not None:
            facts.append(f"本观察窗成交增量为{amount / 100_000_000:.2f}亿元")
            metrics["amount_delta"] = amount
        magnitude = max(
            abs((now_median or 0) - (old_median or 0)),
            abs((now_equal or 0) - (old_equal or 0)),
            abs((now_breadth or 0) - (old_breadth or 0)) / 10,
        )
        events.append(
            _event(
                event_key=f"market_reversal:{label.lower()}",
                event_type=MARKET_REVERSAL,
                state=state,
                subject={"code": "ALL_A", "name": "全A市场", "type": "market"},
                direction=label,
                magnitude=magnitude,
                facts=facts,
                metrics=metrics,
                support_count=support,
                inference=inference,
                boundaries=["转折描述的是市场内部量价与广度变化，不等同于趋势已经反转。"],
                upgrade_condition=(
                    "若收益中位数、上涨覆盖率和等权收益继续同步回升并保持两个观察窗，则升级。"
                    if direction > 0
                    else "若收益中位数、上涨覆盖率和等权收益继续同步走弱并保持两个观察窗，则升级。"
                ),
                invalidation_condition=(
                    "若上述三项中至少两项回落至触发前水平，则该向上转折失效。"
                    if direction > 0
                    else "若上述三项中至少两项恢复至触发前水平，则该向下转折失效。"
                ),
                observation=current,
            )
        )
    return events


def _sector_step(previous: Mapping[str, Any], current: Mapping[str, Any], direction: int) -> tuple[bool, int]:
    return_delta = _metric_delta(current, previous, _return)
    breadth_delta = _metric_delta(current, previous, _breadth)
    ratio = _amount_ratio(current, previous)
    if direction > 0:
        checks = (
            return_delta is not None and return_delta >= 0.30,
            breadth_delta is not None and breadth_delta >= 10.0,
            ratio is not None and ratio >= 1.15,
        )
        breadth_gate = _breadth(current)
        hit = sum(checks) >= 2 and (breadth_gate is None or breadth_gate >= 50)
    else:
        checks = (
            return_delta is not None and return_delta <= -0.30,
            breadth_delta is not None and breadth_delta <= -10.0,
            ratio is not None and ratio <= 0.85,
        )
        hit = sum(checks) >= 2
    return hit, sum(checks)


def _sector_events(
    current: Mapping[str, Any], history: Sequence[Mapping[str, Any]], coverage: float
) -> list[dict[str, Any]]:
    if not history:
        return []
    current_rows = _records(current, "sectors", "sector_metrics", "sector")
    previous_rows = _records(history[-1], "sectors", "sector_metrics", "sector")
    events: list[dict[str, Any]] = []
    for row in current_rows:
        code, name, kind = _identity(row, "sector")
        previous = _find_record(previous_rows, code, name)
        if previous is None:
            continue
        for direction, event_type in ((1, SECTOR_SPREAD), (-1, SECTOR_EBB)):
            hit, evidence_count = _sector_step(previous, row, direction)
            if not hit:
                continue

            def pair_hit(left_obs: Mapping[str, Any], right_obs: Mapping[str, Any], sign: int = direction) -> bool:
                left = _find_record(_records(left_obs, "sectors", "sector_metrics", "sector"), code, name)
                right = _find_record(_records(right_obs, "sectors", "sector_metrics", "sector"), code, name)
                return bool(left and right and _sector_step(left, right, sign)[0])

            support = _consecutive_pair_hits([*history, current], pair_hit)
            state = _state(support, evidence_count, coverage)
            now_return, old_return = _return(row), _return(previous)
            now_breadth, old_breadth = _breadth(row), _breadth(previous)
            ratio = _amount_ratio(row, previous)
            facts: list[str] = []
            metrics: dict[str, Any] = {}
            if now_return is not None and old_return is not None:
                facts.append(f"{name}收益中位数由{old_return:.2f}%变为{now_return:.2f}%")
                metrics.update(return_before=old_return, return_current=now_return)
            if now_breadth is not None and old_breadth is not None:
                facts.append(f"板块上涨覆盖率由{old_breadth:.1f}%变为{now_breadth:.1f}%")
                metrics.update(breadth_before=old_breadth, breadth_current=now_breadth)
            if ratio is not None:
                facts.append(f"板块观察窗成交增量为上一观察窗的{ratio:.2f}倍")
                metrics["amount_ratio"] = ratio
            is_spread = direction > 0
            events.append(
                _event(
                    event_key=f"sector:{code}:{'spread' if is_spread else 'ebb'}",
                    event_type=event_type,
                    state=state,
                    subject={"code": code, "name": name, "type": kind or "sector"},
                    direction="UP" if is_spread else "DOWN",
                    magnitude=max(
                        abs((now_return or 0) - (old_return or 0)),
                        abs((now_breadth or 0) - (old_breadth or 0)) / 10,
                    ),
                    facts=facts,
                    metrics=metrics,
                    support_count=support,
                    inference=(
                        f"{name}的上涨正在由局部向更多成分扩散。"
                        if is_spread
                        else f"{name}的强势覆盖面正在收缩，出现板块退潮特征。"
                    ),
                    boundaries=["板块成交变化只能说明活跃度变化，不能据此识别资金主体或成交方向。"],
                    upgrade_condition=(
                        "若上涨覆盖率继续提高、板块收益增强且成交活跃保持，则升级。"
                        if is_spread
                        else "若上涨覆盖率继续下降、核心与后排同步转弱，则升级。"
                    ),
                    invalidation_condition=(
                        "若上涨覆盖率回落且板块收益降至触发前水平，则扩散判断失效。"
                        if is_spread
                        else "若上涨覆盖率与板块收益同步恢复至触发前水平，则退潮判断失效。"
                    ),
                    observation=current,
                )
            )
    return events


def _stock_step(previous: Mapping[str, Any], current: Mapping[str, Any], direction: int) -> tuple[bool, int]:
    change = _metric_delta(current, previous, _return)
    speed = _first_number(current, "price_speed_pct", "price_speed", "return_speed_pct")
    ratio = _amount_ratio(current, previous)
    above_now = _bool(current, "above_vwap", "above_intraday_average")
    above_before = _bool(previous, "above_vwap", "above_intraday_average")
    crossed = above_now is not None and above_before is not None and above_now != above_before
    directional = (
        change is not None and change * direction >= 0.60,
        speed is not None and speed * direction >= 0.15,
        crossed and above_now == (direction > 0),
    )
    activity = ratio is not None and ratio >= 1.30
    checks = sum(directional) + int(activity)
    return any(directional) and checks >= 2, checks


def _key_stock_events(
    current: Mapping[str, Any], history: Sequence[Mapping[str, Any]], coverage: float
) -> list[dict[str, Any]]:
    if not history:
        return []
    current_rows = _records(current, "key_stocks", "key_stock_metrics", "stocks")
    previous_rows = _records(history[-1], "key_stocks", "key_stock_metrics", "stocks")
    events: list[dict[str, Any]] = []
    for row in current_rows:
        code, name, kind = _identity(row, "stock")
        previous = _find_record(previous_rows, code, name)
        if previous is None:
            continue
        for direction in (1, -1):
            hit, evidence_count = _stock_step(previous, row, direction)
            if not hit:
                continue

            def pair_hit(left_obs: Mapping[str, Any], right_obs: Mapping[str, Any], sign: int = direction) -> bool:
                left = _find_record(_records(left_obs, "key_stocks", "key_stock_metrics", "stocks"), code, name)
                right = _find_record(_records(right_obs, "key_stocks", "key_stock_metrics", "stocks"), code, name)
                return bool(left and right and _stock_step(left, right, sign)[0])

            support = _consecutive_pair_hits([*history, current], pair_hit)
            state = _state(support, evidence_count, coverage)
            now_return, old_return = _return(row), _return(previous)
            speed = _first_number(row, "price_speed_pct", "price_speed", "return_speed_pct")
            ratio = _amount_ratio(row, previous)
            breadth = _first_number(row, "sector_breadth_pct")
            if breadth is not None and 0 <= breadth <= 1:
                breadth *= 100
            facts: list[str] = []
            metrics: dict[str, Any] = {}
            if now_return is not None and old_return is not None:
                facts.append(f"{name}涨跌幅由{old_return:.2f}%变为{now_return:.2f}%")
                metrics.update(return_before=old_return, return_current=now_return)
            if speed is not None:
                facts.append(f"最近观察窗价格速度为{speed:+.2f}个百分点")
                metrics["price_speed_pct"] = speed
            if ratio is not None:
                facts.append(f"成交增量为上一观察窗的{ratio:.2f}倍")
                metrics["amount_ratio"] = ratio
            if breadth is not None:
                facts.append(f"所属板块上涨覆盖率为{breadth:.1f}%")
                metrics["sector_breadth_pct"] = breadth
            is_up = direction > 0
            events.append(
                _event(
                    event_key=f"stock:{code}:{'up' if is_up else 'down'}",
                    event_type=KEY_STOCK,
                    state=state,
                    subject={"code": code, "name": name, "type": kind or "stock"},
                    direction="UP" if is_up else "DOWN",
                    magnitude=abs((now_return or 0) - (old_return or 0)),
                    facts=facts,
                    metrics=metrics,
                    support_count=support,
                    inference=(
                        f"关键个股{name}正在增强，可能成为板块强度的同步验证点。"
                        if is_up
                        else f"关键个股{name}正在转弱，可能成为板块风险的同步验证点。"
                    ),
                    boundaries=["个股与板块的同步变化不证明两者之间存在单向因果关系。"],
                    upgrade_condition=(
                        "若价格强度、成交活跃度与板块覆盖率继续同向改善，则升级。"
                        if is_up
                        else "若价格继续走弱且板块覆盖率同步下降，则升级。"
                    ),
                    invalidation_condition=(
                        "若价格回到触发前水平且成交活跃度回落，则增强节点失效。"
                        if is_up
                        else "若价格收复触发前水平且板块覆盖率恢复，则转弱节点失效。"
                    ),
                    observation=current,
                )
            )
    return events


_STYLE_PAIR_ALIASES: tuple[tuple[set[str], set[str]], ...] = (
    (
        {"large", "large_cap", "big", "hs300", "000300", "沪深300", "上证50", "大盘", "权重"},
        {"small", "small_cap", "micro", "zz2000", "932000", "中证2000", "小盘", "微盘"},
    ),
    (
        {"growth", "科技成长", "成长", "technology", "tech"},
        {"dividend", "红利", "高股息", "defensive", "防御"},
    ),
    (
        {"cyclical", "周期", "资源"},
        {"consumer", "消费", "manufacturing", "制造"},
    ),
)


def _style_token(row: Mapping[str, Any]) -> str:
    code, name, _ = _identity(row, "style")
    return f"{code} {name}".lower()


def _matches_alias(row: Mapping[str, Any], aliases: set[str]) -> bool:
    token = _style_token(row)
    return any(alias.lower() == token or alias.lower() in token for alias in aliases)


def _style_pairs(rows: Sequence[Mapping[str, Any]]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    by_code = {_identity(row, "style")[0]: dict(row) for row in rows}
    for row in rows:
        target = _first_text(row, "opposite_code", "opposite", "pair_with")
        if target and target in by_code:
            pairs.append((dict(row), by_code[target]))
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        group = _first_text(row, "pair_group", "group")
        if group:
            groups.setdefault(group, []).append(dict(row))
    pairs.extend((items[0], items[1]) for items in groups.values() if len(items) == 2)
    for left_aliases, right_aliases in _STYLE_PAIR_ALIASES:
        left = next((dict(row) for row in rows if _matches_alias(row, left_aliases)), None)
        right = next((dict(row) for row in rows if _matches_alias(row, right_aliases)), None)
        if left and right:
            pairs.append((left, right))
    if not pairs and len(rows) == 2:
        pairs.append((dict(rows[0]), dict(rows[1])))
    unique: list[tuple[dict[str, Any], dict[str, Any]]] = []
    seen: set[tuple[str, str]] = set()
    for left, right in pairs:
        left_code, _, _ = _identity(left, "style")
        right_code, _, _ = _identity(right, "style")
        key = tuple(sorted((left_code, right_code)))
        if left_code and right_code and left_code != right_code and key not in seen:
            seen.add(key)
            unique.append((left, right))
    return unique


def _style_amount_share(row: Mapping[str, Any] | None) -> float | None:
    value = _first_number(row, "amount_share_pct", "turnover_share_pct", "amount_share")
    if value is not None and 0 <= value <= 1:
        value *= 100
    return value


def _seesaw_step(
    previous_left: Mapping[str, Any],
    previous_right: Mapping[str, Any],
    current_left: Mapping[str, Any],
    current_right: Mapping[str, Any],
) -> tuple[bool, str, int, dict[str, float]]:
    returns = (_return(previous_left), _return(previous_right), _return(current_left), _return(current_right))
    if any(value is None for value in returns):
        return False, "", 0, {}
    old_left, old_right, now_left, now_right = (float(value) for value in returns)
    delta_left = now_left - old_left
    delta_right = now_right - old_right
    gap_delta = (now_left - now_right) - (old_left - old_right)
    leader = "left" if gap_delta > 0 else "right"
    signed_gap = abs(gap_delta)
    opposite_move = delta_left >= 0.15 and delta_right <= -0.15
    if leader == "right":
        opposite_move = delta_right >= 0.15 and delta_left <= -0.15
    opposite_level = (now_left > 0 > now_right) if leader == "left" else (now_right > 0 > now_left)
    old_left_breadth, old_right_breadth = _breadth(previous_left), _breadth(previous_right)
    now_left_breadth, now_right_breadth = _breadth(current_left), _breadth(current_right)
    breadth_opposite = False
    if None not in (old_left_breadth, old_right_breadth, now_left_breadth, now_right_breadth):
        left_change = float(now_left_breadth) - float(old_left_breadth)
        right_change = float(now_right_breadth) - float(old_right_breadth)
        breadth_opposite = (
            left_change >= 5 and right_change <= -5 if leader == "left" else right_change >= 5 and left_change <= -5
        )
    old_left_share, old_right_share = _style_amount_share(previous_left), _style_amount_share(previous_right)
    now_left_share, now_right_share = _style_amount_share(current_left), _style_amount_share(current_right)
    share_opposite = False
    if None not in (old_left_share, old_right_share, now_left_share, now_right_share):
        share_opposite = (
            now_left_share > old_left_share and now_right_share < old_right_share
            if leader == "left"
            else now_right_share > old_right_share and now_left_share < old_left_share
        )
    checks = int(signed_gap >= 0.45) + int(opposite_move or opposite_level) + int(breadth_opposite) + int(share_opposite)
    return checks >= 2 and signed_gap >= 0.45, leader, checks, {
        "gap_before": old_left - old_right,
        "gap_current": now_left - now_right,
        "gap_delta": gap_delta,
        "left_return": now_left,
        "right_return": now_right,
    }


def _style_events(
    current: Mapping[str, Any], history: Sequence[Mapping[str, Any]], coverage: float
) -> list[dict[str, Any]]:
    if not history:
        return []
    current_rows = _records(current, "styles", "style_metrics", "style")
    previous_rows = _records(history[-1], "styles", "style_metrics", "style")
    events: list[dict[str, Any]] = []
    for left, right in _style_pairs(current_rows):
        left_code, left_name, _ = _identity(left, "style")
        right_code, right_name, _ = _identity(right, "style")
        old_left = _find_record(previous_rows, left_code, left_name)
        old_right = _find_record(previous_rows, right_code, right_name)
        if old_left is None or old_right is None:
            continue
        hit, leader_side, evidence_count, metrics = _seesaw_step(old_left, old_right, left, right)
        if not hit:
            continue
        leader, laggard = (left, right) if leader_side == "left" else (right, left)
        leader_code, leader_name, _ = _identity(leader, "style")
        laggard_code, laggard_name, _ = _identity(laggard, "style")
        pair_codes = tuple(sorted((left_code, right_code)))

        def pair_hit(left_obs: Mapping[str, Any], right_obs: Mapping[str, Any]) -> bool:
            older_rows = _records(left_obs, "styles", "style_metrics", "style")
            newer_rows = _records(right_obs, "styles", "style_metrics", "style")
            older_left = _find_record(older_rows, left_code, left_name)
            older_right = _find_record(older_rows, right_code, right_name)
            newer_left = _find_record(newer_rows, left_code, left_name)
            newer_right = _find_record(newer_rows, right_code, right_name)
            if not all((older_left, older_right, newer_left, newer_right)):
                return False
            step_hit, step_leader, _, _ = _seesaw_step(older_left, older_right, newer_left, newer_right)
            return step_hit and step_leader == leader_side

        support = _consecutive_pair_hits([*history, current], pair_hit)
        state = _state(support, evidence_count, coverage)
        gap_before = metrics["gap_before"] if leader_side == "left" else -metrics["gap_before"]
        gap_current = metrics["gap_current"] if leader_side == "left" else -metrics["gap_current"]
        facts = [
            f"{leader_name}相对{laggard_name}的收益差由{gap_before:+.2f}个百分点扩大至{gap_current:+.2f}个百分点",
            f"当前{leader_name}为{_return(leader):+.2f}%，{laggard_name}为{_return(laggard):+.2f}%",
        ]
        leader_breadth, laggard_breadth = _breadth(leader), _breadth(laggard)
        if leader_breadth is not None and laggard_breadth is not None:
            facts.append(f"两侧上涨覆盖率分别为{leader_breadth:.1f}%与{laggard_breadth:.1f}%")
            metrics.update(leader_breadth=leader_breadth, laggard_breadth=laggard_breadth)
        metrics.update(counterpart_code=laggard_code, leader_code=leader_code)
        events.append(
            _event(
                event_key=f"style_seesaw:{pair_codes[0]}:{pair_codes[1]}:{leader_code}",
                event_type=STYLE_SEESAW,
                state=state,
                subject={"code": leader_code, "name": leader_name, "type": "style"},
                direction="ROTATE_TO",
                magnitude=abs(metrics["gap_delta"]),
                facts=facts,
                metrics=metrics,
                support_count=support,
                inference=f"盘面呈现由{laggard_name}相对切换至{leader_name}的跷跷板特征。",
                boundaries=["相对强弱变化不等于资金从一侧直接、等额流向另一侧。"],
                upgrade_condition="若相对收益差继续扩大，且成交占比与上涨覆盖率保持反向变化，则升级。",
                invalidation_condition=f"若相对收益差明显收窄，或{laggard_name}的覆盖率与成交占比恢复，则判断失效。",
                observation=current,
            )
        )
    return events


def _benchmark_return(row: Mapping[str, Any] | None) -> float | None:
    return _first_number(row, "return_pct", "change_pct", "index_return_pct")


def _is_etf(row: Mapping[str, Any]) -> bool:
    kind = _first_text(row, "instrument_type", "type", "kind").upper()
    return "ETF" in kind or _bool(row, "is_etf") is True


def _is_index(row: Mapping[str, Any]) -> bool:
    kind = _first_text(row, "instrument_type", "type", "kind").upper()
    return "INDEX" in kind or "指数" in kind or _bool(row, "is_index") is True


def _is_broad_etf(row: Mapping[str, Any]) -> bool:
    explicit = _bool(row, "is_broad", "broad_based")
    if explicit is not None:
        return explicit and _is_etf(row)
    category = _first_text(row, "category", "benchmark_type", "scope").lower()
    if category and any(token in category for token in ("sector", "industry", "行业", "主题")):
        return False
    return _is_etf(row)


def _broad_support_signal(
    previous: Mapping[str, Any] | None, current: Mapping[str, Any]
) -> tuple[bool, int, dict[str, Any]]:
    benchmarks = _records(current, "benchmarks", "benchmark_metrics", "benchmark")
    previous_benchmarks = _records(previous or {}, "benchmarks", "benchmark_metrics", "benchmark")
    hot_etfs: list[tuple[dict[str, Any], float]] = []
    for row in benchmarks:
        if not _is_broad_etf(row):
            continue
        code, name, _ = _identity(row, "ETF")
        old = _find_record(previous_benchmarks, code, name)
        ratio = _amount_ratio(row, old)
        zscore = _first_number(row, "amount_z", "turnover_z")
        if (ratio is not None and ratio >= 1.60) or (zscore is not None and zscore >= 2.0):
            hot_etfs.append((row, ratio if ratio is not None else 1.0 + float(zscore or 0) / 2))
    market = _market(current)
    previous_market = _market(previous or {})
    index_delta = _first_number(market, "index_return_delta_pct", "cap_weighted_return_delta_pct")
    index_name = "权重指数"
    index_return = _first_number(market, "cap_weighted_return_pct", "index_return_pct", "benchmark_return_pct")
    if index_delta is None and previous is not None:
        old_index_return = _first_number(
            previous_market, "cap_weighted_return_pct", "index_return_pct", "benchmark_return_pct"
        )
        if index_return is not None and old_index_return is not None:
            index_delta = index_return - old_index_return
    index_rows = [row for row in benchmarks if _is_index(row)]
    for row in index_rows:
        code, name, _ = _identity(row, "index")
        old = _find_record(previous_benchmarks, code, name)
        current_return = _benchmark_return(row)
        explicit_delta = _first_number(row, "return_delta_pct", "change_delta_pct")
        delta = explicit_delta
        if delta is None and old is not None and current_return is not None and _benchmark_return(old) is not None:
            delta = current_return - float(_benchmark_return(old))
        if delta is not None and (index_delta is None or delta > index_delta):
            index_delta, index_return, index_name = delta, current_return, name
    stabilized = _bool(market, "index_stabilized", "benchmark_stabilized") is True
    stabilized = stabilized or (index_delta is not None and index_delta >= 0.20)
    median, breadth, equal_weight = _market_values(current)
    weighted_gap = None
    if index_return is not None and equal_weight is not None:
        weighted_gap = index_return - equal_weight
    weak_internals = (median is not None and median < 0) or (breadth is not None and breadth < 45)
    divergence = weighted_gap is not None and weighted_gap >= 0.25 and weak_internals
    evidence_count = int(bool(hot_etfs)) + int(stabilized) + int(divergence) + int(len(hot_etfs) >= 2)
    return len(hot_etfs) >= 2 and stabilized and (weak_internals or divergence), evidence_count, {
        "hot_etfs": hot_etfs,
        "index_delta": index_delta,
        "index_return": index_return,
        "index_name": index_name,
        "median": median,
        "breadth": breadth,
        "equal_weight": equal_weight,
        "weighted_gap": weighted_gap,
    }


def _broad_support_events(
    current: Mapping[str, Any], history: Sequence[Mapping[str, Any]], coverage: float
) -> list[dict[str, Any]]:
    previous = history[-1] if history else None
    hit, evidence_count, details = _broad_support_signal(previous, current)
    if not hit:
        return []

    def pair_hit(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
        return _broad_support_signal(left, right)[0]

    support = _consecutive_pair_hits([*history, current], pair_hit) if history else 1
    state = _state(support, evidence_count, coverage)
    # Confirmation needs breadth across three independently quoted products;
    # two products may strengthen a signal but cannot confirm it.
    if state == CONFIRMED and len(details["hot_etfs"]) < 3:
        state = ENHANCED
    facts: list[str] = []
    etf_metrics: list[dict[str, Any]] = []
    for row, ratio in details["hot_etfs"]:
        code, name, _ = _identity(row, "ETF")
        facts.append(f"{name or code}观察窗成交为基准的{ratio:.2f}倍")
        etf_metrics.append({"code": code, "name": name, "amount_ratio": ratio})
    if details["index_delta"] is not None:
        facts.append(f"{details['index_name']}较上一观察窗改善{details['index_delta']:.2f}个百分点")
    if details["median"] is not None or details["breadth"] is not None:
        fragments: list[str] = []
        if details["median"] is not None:
            fragments.append(f"全A中位数{details['median']:.2f}%")
        if details["breadth"] is not None:
            fragments.append(f"上涨覆盖率{details['breadth']:.1f}%")
        facts.append("、".join(fragments))
    metrics = {
        "broad_etfs": etf_metrics,
        "index_delta_pct": details["index_delta"],
        "index_return_pct": details["index_return"],
        "market_median_pct": details["median"],
        "market_breadth_pct": details["breadth"],
        "equal_weight_return_pct": details["equal_weight"],
        "weighted_equal_gap_pct": details["weighted_gap"],
    }
    magnitude = max([ratio for _, ratio in details["hot_etfs"]] + [0.0])
    return [
        _event(
            event_key="broad_index_support:market",
            event_type=BROAD_INDEX_SUPPORT,
            state=state,
            subject={"code": "BROAD_INDEX", "name": "宽基ETF与权重指数", "type": "benchmark"},
            direction="STABILIZE",
            magnitude=magnitude,
            facts=facts,
            metrics=metrics,
            support_count=support,
            inference="宽基成交放大、指数改善而市场内部仍弱，盘面呈现稳定指数的托底特征。",
            boundaries=[
                "盘中行情只能支持“疑似宽基托底”的行为判断，不能确认具体资金身份。",
                "成交放大本身不提供成交方向，也不代表修复已经扩散至多数个股。",
            ],
            upgrade_condition="若多个宽基ETF持续放量、指数保持稳定且市场广度随后恢复，则升级为托底向修复扩散。",
            invalidation_condition="若宽基成交活跃度回落后指数再度走弱，或量能仅为单点异常，则托底判断失效。",
            observation=current,
        )
    ]


def _family(event_type: str) -> str:
    if event_type in {SECTOR_SPREAD, SECTOR_EBB}:
        return "sector"
    return event_type


def _explicit_invalidations(
    active: Sequence[Mapping[str, Any]],
    previous_events: Sequence[Mapping[str, Any]] | Mapping[str, Mapping[str, Any]] | None,
    current: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Invalidate only when a new candidate supplies direct opposite evidence.

    Ordinary event absence is intentionally left to the persistent state machine,
    which can apply miss counts and cooldowns across process restarts.
    """

    if not previous_events:
        return []
    rows = list(previous_events.values()) if isinstance(previous_events, Mapping) else list(previous_events)
    active_keys = {str(item.get("event_key")) for item in active}
    result: list[dict[str, Any]] = []
    for previous in rows:
        key = str(previous.get("event_key") or "")
        if not key or key in active_keys or previous.get("state") == INVALIDATED:
            continue
        previous_type = str(previous.get("event_type") or "")
        previous_code = str(
            previous.get("subject_code")
            or (previous.get("subject") or {}).get("code")
            or ""
        )
        previous_direction = str(previous.get("direction") or "")
        opposite = next(
            (
                item
                for item in active
                if _family(str(item.get("event_type"))) == _family(previous_type)
                and str(item.get("subject_code") or "") == previous_code
                and str(item.get("direction") or "") != previous_direction
            ),
            None,
        )
        if opposite is None:
            continue
        facts = list(opposite.get("facts") or opposite.get("evidence", {}).get("facts") or [])
        facts.append("当前反向证据已替代此前触发条件")
        result.append(
            _event(
                event_key=key,
                event_type=previous_type,
                state=INVALIDATED,
                subject=previous.get("subject")
                or {"code": previous_code, "name": previous.get("subject_name") or previous_code, "type": ""},
                direction=previous_direction,
                magnitude=0,
                facts=facts,
                metrics=opposite.get("evidence", {}).get("metrics") or {},
                support_count=0,
                inference="此前判断已被明确的反向量价与广度证据推翻。",
                boundaries=list(previous.get("boundaries") or []),
                upgrade_condition="如需重新建立该判断，须重新满足初始触发与持续性条件。",
                invalidation_condition="已失效。",
                observation=current,
            )
        )
    return result


def evaluate_events(
    current: Mapping[str, Any],
    history: Sequence[Mapping[str, Any]] = (),
    previous_events: Sequence[Mapping[str, Any]] | Mapping[str, Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Return deterministic event candidates for one observation.

    ``history`` must be oldest-to-newest and should not include ``current``.
    Missing fields simply reduce the available evidence; no I/O, clock access or
    mutation is performed.  A persistent state machine may invalidate events that
    disappear.  Passing ``previous_events`` additionally emits ``INVALIDATED``
    only when the current sample contains an explicit opposite candidate.
    """

    if not isinstance(current, Mapping):
        raise TypeError("current observation must be a mapping")
    clean_history = [item for item in history if isinstance(item, Mapping)]
    coverage = _coverage(current)
    active: list[dict[str, Any]] = []
    active.extend(_market_reversal_events(current, clean_history, coverage))
    active.extend(_sector_events(current, clean_history, coverage))
    active.extend(_key_stock_events(current, clean_history, coverage))
    active.extend(_style_events(current, clean_history, coverage))
    active.extend(_broad_support_events(current, clean_history, coverage))
    active.extend(_explicit_invalidations(active, previous_events, current))
    return sorted(active, key=lambda item: (-int(item["severity"]), item["event_key"]))


__all__ = [
    "BROAD_INDEX_SUPPORT",
    "CONFIRMED",
    "ENHANCED",
    "INVALIDATED",
    "KEY_STOCK",
    "MARKET_REVERSAL",
    "SECTOR_EBB",
    "SECTOR_SPREAD",
    "STYLE_SEESAW",
    "SUSPECTED",
    "evaluate_events",
]
