# -*- coding: utf-8 -*-
"""Stable WeCom Markdown rendering for intraday event candidates."""
from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from .rules import (
    BROAD_INDEX_SUPPORT,
    CONFIRMED,
    ENHANCED,
    INVALIDATED,
    KEY_STOCK,
    MARKET_REVERSAL,
    SECTOR_EBB,
    SECTOR_SPREAD,
    STYLE_SEESAW,
    SUSPECTED,
)


_STATE_LABELS = {
    SUSPECTED: "疑似",
    ENHANCED: "证据增强",
    CONFIRMED: "形态确认",
    INVALIDATED: "判断失效",
}

_TYPE_LABELS = {
    MARKET_REVERSAL: "市场转折",
    SECTOR_SPREAD: "板块扩散",
    SECTOR_EBB: "板块退潮",
    KEY_STOCK: "关键个股节点",
    STYLE_SEESAW: "风格跷跷板",
    BROAD_INDEX_SUPPORT: "宽基托底特征",
}

_UNSUPPORTED_ASSERTIONS = (
    (re.compile(r"国家队(?:已经|已)?(?:进场|入场|买入|护盘|托底)"), "宽基成交出现托底特征"),
    (re.compile(r"(?:确认|证实|确定)(?:为|是)?国家队"), "无法确认具体资金身份"),
    (re.compile(r"主动买入|大单买入|净买入"), "成交放大"),
)


def _safe_text(value: Any) -> str:
    text = str(value or "").strip()
    for pattern, replacement in _UNSUPPORTED_ASSERTIONS:
        text = pattern.sub(replacement, text)
    return text.replace("\r", " ").replace("\n", " ")


def _as_lines(value: Any) -> list[str]:
    if isinstance(value, str):
        return [_safe_text(value)] if value.strip() else []
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [_safe_text(item) for item in value if _safe_text(item)]
    return []


def _first_text(row: Mapping[str, Any], *keys: str, default: str = "") -> str:
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip():
            return _safe_text(value)
    return default


def _coverage(observation: Mapping[str, Any]) -> tuple[str, str]:
    observed = observation.get("observed_count")
    expected = observation.get("expected_count")
    try:
        ratio = float(observation.get("coverage", observation.get("coverage_ratio")))
        if ratio <= 1:
            ratio *= 100
        ratio_text = f"{ratio:.1f}%"
    except (TypeError, ValueError):
        try:
            ratio_text = f"{float(observed) / float(expected) * 100:.1f}%"
        except (TypeError, ValueError, ZeroDivisionError):
            ratio_text = "未标注"
    count_text = f"{observed}/{expected}" if observed is not None and expected is not None else "数量未标注"
    return count_text, ratio_text


def _source(observation: Mapping[str, Any]) -> str:
    raw = (
        observation.get("sources")
        or observation.get("source_provider")
        or observation.get("data_source")
        or observation.get("source")
    )
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        return "、".join(_safe_text(item) for item in raw if _safe_text(item)) or "未标注"
    return _safe_text(raw) or "未标注"


def _clock(value: str) -> str:
    match = re.search(r"(?:T|\s)(\d{2}:\d{2})(?::\d{2})?", value)
    return match.group(1) if match else value or "--:--"


def render_event(event: Mapping[str, Any], observation: Mapping[str, Any] | None = None) -> str:
    """Render one candidate using a fixed, auditable six-section layout.

    The renderer accepts state-machine overrides of ``facts``, ``inference`` and
    ``boundaries``.  Unsupported participant or order-flow assertions are
    defensively neutralized, including on ``INVALIDATED`` messages.
    """

    if not isinstance(event, Mapping):
        raise TypeError("event must be a mapping")
    observation = observation or {}
    if not isinstance(observation, Mapping):
        raise TypeError("observation must be a mapping")
    state = _first_text(event, "state", default=SUSPECTED)
    event_type = _first_text(event, "event_type")
    subject = event.get("subject") if isinstance(event.get("subject"), Mapping) else {}
    subject_name = _first_text(event, "subject_name") or _first_text(subject, "name", "code", default="市场")
    timestamp = _first_text(
        observation,
        "source_snapshot_at",
        "snapshot_at",
        "observed_at",
        default=_first_text(event, "detected_at", default="--:--"),
    )
    type_label = _TYPE_LABELS.get(event_type, event_type or "盘中事件")
    state_label = _STATE_LABELS.get(state, state or "观察")
    evidence = event.get("evidence") if isinstance(event.get("evidence"), Mapping) else {}
    facts = _as_lines(event.get("facts")) or _as_lines(evidence.get("facts")) or ["暂无可展示事实"]
    inference = _safe_text(event.get("inference") or event.get("judgment") or "证据不足，保持观察。")
    boundaries = _as_lines(event.get("boundaries") or event.get("boundary"))
    upgrade = _safe_text(event.get("upgrade_condition") or "需更多连续观察窗与独立指标同向验证。")
    invalidation = _safe_text(event.get("invalidation_condition") or "若核心触发指标恢复至触发前水平，则失效。")
    count_text, coverage_text = _coverage(observation)
    source = _source(observation)

    fact_text = "；".join(facts)
    boundary_text = "；".join(boundaries) if boundaries else "行情证据仅支持盘面行为判断，不识别具体资金身份或成交方向。"
    # This fixed boundary is mandatory for the especially sensitive inference.
    if event_type == BROAD_INDEX_SUPPORT:
        mandatory = "仅凭盘中行情无法确认具体资金身份，也无法判定成交方向。"
        if mandatory not in boundary_text:
            boundary_text = f"{boundary_text}；{mandatory}"

    return "\n".join(
        [
            f"**【{_clock(timestamp)}｜{state_label}·{type_label}｜{subject_name}】**",
            f"> 事实：{fact_text}",
            f"> 判断：{inference}",
            f"> 边界：{boundary_text}",
            f"> 升级条件：{upgrade}",
            f"> 失效条件：{invalidation}",
            f"> 数据：截止 {timestamp}｜覆盖 {count_text}（{coverage_text}）｜来源 {source}",
        ]
    )


__all__ = ["render_event"]
