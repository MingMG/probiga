# -*- coding: utf-8 -*-
"""Production-safe unified stock screener.

Trading V4/V5/V6 are deliberately kept outside the production import graph.
This router exposes their governance state, but never imports or executes the
research packages.  Candidate generation uses the existing production data
reader and always returns non-actionable research observations.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from server.api.routers import hot_data


router = APIRouter()


PRESETS: tuple[dict[str, Any], ...] = (
    {
        "key": "trend_breakout",
        "name": "趋势突破",
        "mode": "trend_strong",
        "category": "趋势",
        "description": "四线多头、连续站稳均线并接近阶段高点。",
        "defaults": {"trend_days": 5, "ma_slope_min": 0.2, "vol_ratio_min": 0.5, "vol_ratio_max": 3.0, "new_high_pct": 0.90},
    },
    {
        "key": "startup_breakout",
        "name": "启动突破",
        "mode": "startup",
        "category": "启动",
        "description": "盘整区间突破并温和放量，保留趋势尚可控的候选。",
        "defaults": {"vol_boost": 1.3, "min_change": 2.0, "max_change": 12.0},
    },
    {
        "key": "oversold_reversal",
        "name": "低位反转",
        "mode": "low_start",
        "category": "反转",
        "description": "靠近阶段低点并出现放量止跌迹象。",
        "defaults": {"low_lookback": 20, "max_from_low": 0.05, "vol_boost": 1.5, "min_change": 3.0, "max_change": 20.0},
    },
    {
        "key": "capital_support",
        "name": "资金承接",
        "mode": "flow",
        "category": "资金",
        "description": "按主力净流入筛选，再由风险与事件证据确认。",
        "defaults": {"min_main_flow": 5_000_000.0},
    },
    {
        "key": "momentum_ladder",
        "name": "情绪连板",
        "mode": "ladder",
        "category": "情绪",
        "description": "识别连续涨停附近的强势股，仅作为短线研究池。",
        "defaults": {"min_boards": 2, "max_boards": 5, "limit_pct": 9.5},
    },
    {
        "key": "dragon_tiger",
        "name": "龙虎榜事件",
        "mode": "lhb",
        "category": "事件",
        "description": "把龙虎榜作为事件和资金证据，不直接等同买入信号。",
        "defaults": {},
    },
    {
        "key": "technical_cross",
        "name": "技术交叉",
        "mode": "macd",
        "category": "技术",
        "description": "使用现有技术指标筛出多头交叉候选。",
        "defaults": {},
    },
)
PRESET_MAP = {item["key"]: item for item in PRESETS}


VERSION_MATRIX: tuple[dict[str, Any], ...] = (
    {
        "version": "V2",
        "role": "策略配置与模拟兼容",
        "lifecycle": "PAPER_TRIAL",
        "decision": "OBSERVE",
        "production_selector": False,
        "reason": "保留历史策略与模拟兼容，不作为当前统一排序器。",
    },
    {
        "version": "V3",
        "role": "现有 AI 分析与推荐兼容",
        "lifecycle": "COMPATIBILITY",
        "decision": "DATA_GATED",
        "production_selector": True,
        "reason": "只有公告、新闻、行情和质量门禁全部通过才允许升级信号。",
    },
    {
        "version": "V4",
        "role": "净室决策核心研究",
        "lifecycle": "RESEARCH_ONLY",
        "decision": "BLOCK",
        "production_selector": False,
        "reason": "尚无可激活的前向证据，生产只展示治理状态。",
    },
    {
        "version": "V5",
        "role": "市场状态专家与统一路由研究",
        "lifecycle": "RESEARCH_ONLY",
        "decision": "BLOCK",
        "production_selector": False,
        "reason": "历史候选未通过，禁止调度、下单和冒充生产模型。",
    },
    {
        "version": "V6",
        "role": "PIT 财务与收益门槛模型研究",
        "lifecycle": "RESEARCH_ONLY",
        "decision": "BLOCK",
        "production_selector": False,
        "reason": "历史候选通过数为零，压力矩阵和前向证据尚未完成。",
    },
)


class ScreenerRunRequest(BaseModel):
    preset: str = "trend_breakout"
    as_of_date: str = ""
    top: int = Field(default=50, ge=1, le=200)
    filters: dict[str, Any] = Field(default_factory=dict)


def _clean_date(value: Any) -> str:
    raw = str(value or "").strip()[:10]
    if not raw:
        return ""
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError:
        return ""


def _number(value: Any, default: float | None = None) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if number == number and abs(number) != float("inf") else default


def _safe_max_date(table: str, column: str) -> str:
    allowed = {
        ("sm_stock_kline", "trade_date"),
        ("sm_stock_current", "snapshot_at"),
        ("sm_stock_capital_flow_daily", "trade_date"),
        ("stock_analysis_result", "analysis_date"),
        ("st_recommended_stocks", "pick_date"),
        ("st_news_flash", "publish_time"),
        ("si_notice_eastmoney", "notice_date"),
    }
    if (table, column) not in allowed:
        raise ValueError("unsupported freshness source")
    try:
        rows = hot_data._read_sql(f"SELECT MAX(`{column}`) AS value FROM `{table}`", {})
    except Exception:
        return ""
    return _clean_date(rows[0].get("value")) if rows else ""


def _expected_completed_session(now: datetime | None = None) -> date:
    current = now or datetime.now()
    candidate = current.date()
    if current.time() < time(16, 0):
        candidate -= timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate


def _freshness_state(latest: str, expected: date) -> dict[str, Any]:
    parsed = date.fromisoformat(latest) if latest else None
    lag_days = (expected - parsed).days if parsed else None
    return {
        "latest_date": latest or None,
        "expected_completed_session": expected.isoformat(),
        "fresh": bool(parsed and parsed >= expected),
        "lag_days": max(0, lag_days) if lag_days is not None else None,
    }


def _runtime_status() -> dict[str, Any]:
    expected = _expected_completed_session()
    dates = {
        "daily_kline": _safe_max_date("sm_stock_kline", "trade_date"),
        "current_quote": _safe_max_date("sm_stock_current", "snapshot_at"),
        "capital_flow": _safe_max_date("sm_stock_capital_flow_daily", "trade_date"),
        "analysis": _safe_max_date("stock_analysis_result", "analysis_date"),
        "recommendation": _safe_max_date("st_recommended_stocks", "pick_date"),
        "news": _safe_max_date("st_news_flash", "publish_time"),
        "notice": _safe_max_date("si_notice_eastmoney", "notice_date"),
    }
    required = {
        key: _freshness_state(dates[key], expected)
        for key in ("daily_kline", "capital_flow")
    }
    selection_ready = all(item["fresh"] for item in required.values())
    recommendation_required = {
        key: _freshness_state(dates[key], expected)
        for key in (
            "daily_kline",
            "current_quote",
            "capital_flow",
            "analysis",
            "recommendation",
            "news",
            "notice",
        )
    }
    recommendation_ready = all(item["fresh"] for item in recommendation_required.values())
    if not selection_ready:
        status = "blocked"
        gate = "DATA_STALE"
        message = "基础行情或资金数据未到最近完整交易日，筛选结果被数据门禁阻断。"
    elif not recommendation_ready:
        status = "degraded"
        gate = "RESEARCH_WATCH_ONLY_RECOMMENDATION_BLOCKED"
        message = "基础规则选股数据可用；新闻、公告、分析或推荐数据仍有滞后，AI 推荐继续封锁，结果仅供观察。"
    else:
        status = "ok"
        gate = "RESEARCH_WATCH_ONLY"
        message = "基础筛选和推荐证据均已到最近完整交易日；结果仍仅供观察。"
    return {
        "status": status,
        "selection_ready": selection_ready,
        "recommendation_ready": recommendation_ready,
        "actionable_output_allowed": False,
        "expected_completed_session": expected.isoformat(),
        "data_dates": dates,
        "required_sources": required,
        "recommendation_sources": recommendation_required,
        "gate": gate,
        "message": message,
    }


def _normalize_rows(rows: list[dict[str, Any]], top: int) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        code = str(row.get("stock_code") or "").strip().zfill(6)
        name = str(row.get("short_name") or row.get("stock_name") or code).strip()
        if len(code) != 6 or not code.isdigit() or not code.startswith(("00", "30", "60", "68", "92")):
            continue
        compact_name = name.lower().replace(" ", "")
        if "st" in compact_name or "退" in name:
            continue
        change = _number(row.get("change_pct"), 0.0) or 0.0
        flow = _number(row.get("main_net_inflow"), 0.0) or 0.0
        volume_ratio = _number(row.get("vol_ratio"), 0.0) or 0.0
        score = 50.0 + max(-10.0, min(15.0, change * 1.2))
        if flow > 0:
            score += min(12.0, 4.0 + flow / 10_000_000.0)
        if volume_ratio >= 1.2:
            score += min(8.0, volume_ratio * 2.0)
        if row.get("above_ma5_days") is not None:
            score += min(12.0, _number(row.get("above_ma5_days"), 0.0) or 0.0)
        if row.get("boards") is not None:
            score += min(15.0, (_number(row.get("boards"), 0.0) or 0.0) * 3.0)
        row.update(
            {
                "stock_code": code,
                "stock_name": name,
                "score": round(max(0.0, min(100.0, score)), 1),
                "decision_scope": "RESEARCH_ONLY",
                "action": "WATCH",
                "actionable": False,
            }
        )
        normalized.append(row)
    normalized.sort(key=lambda item: (-float(item["score"]), item["stock_code"]))
    for rank, row in enumerate(normalized[:top], 1):
        row["rank"] = rank
    return normalized[:top]


def _legacy_run(request: ScreenerRunRequest, preset: dict[str, Any]) -> dict[str, Any]:
    filters = dict(preset.get("defaults") or {})
    filters.update(request.filters or {})
    target = _clean_date(request.as_of_date) or date.today().isoformat()
    return hot_data.screen_stocks(
        mode=preset["mode"],
        trade_date=target,
        top=min(200, max(request.top * 3, request.top)),
        min_change=float(filters.get("min_change", 0.0)),
        max_change=float(filters.get("max_change", 20.0)),
        min_turnover=float(filters.get("min_turnover", 0.0)),
        min_main_flow=float(filters.get("min_main_flow", 1_000_000.0)),
        min_boards=int(filters.get("min_boards", 2)),
        max_boards=int(filters.get("max_boards", 5)),
        vol_boost=float(filters.get("vol_boost", 1.2)),
        max_from_low=float(filters.get("max_from_low", 0.08)),
        low_lookback=int(filters.get("low_lookback", 20)),
        min_chg_trend=float(filters.get("min_chg_trend", -1.0)),
        limit_pct=float(filters.get("limit_pct", 9.5)),
        trend_days=int(filters.get("trend_days", 5)),
        ma_slope_min=float(filters.get("ma_slope_min", 0.2)),
        vol_ratio_min=float(filters.get("vol_ratio_min", 0.5)),
        vol_ratio_max=float(filters.get("vol_ratio_max", 3.0)),
        max_60d_gain=float(filters.get("max_60d_gain", 200.0)),
        new_high_pct=float(filters.get("new_high_pct", 0.90)),
    )


@router.get("/screener/catalog")
def screener_catalog() -> dict[str, Any]:
    return {
        "status": "ok",
        "presets": list(PRESETS),
        "versions": list(VERSION_MATRIX),
        "execution_boundary": {
            "research_only": True,
            "paper_orders_allowed": False,
            "real_orders_allowed": False,
        },
    }


@router.get("/screener/status")
def screener_status() -> dict[str, Any]:
    result = _runtime_status()
    result["versions"] = list(VERSION_MATRIX)
    return result


@router.post("/screener/run")
def screener_run(request: ScreenerRunRequest) -> dict[str, Any]:
    preset = PRESET_MAP.get(request.preset)
    if preset is None:
        raise HTTPException(status_code=400, detail=f"未知选股预设: {request.preset}")
    runtime = _runtime_status()
    raw = _legacy_run(request, preset)
    rows = _normalize_rows(list(raw.get("data") or []), request.top)
    data_date = _clean_date(raw.get("date"))
    status = "ok" if runtime["selection_ready"] and not raw.get("error") else "blocked"
    return {
        "status": status,
        "preset": preset,
        "requested_date": _clean_date(request.as_of_date) or None,
        "data_date": data_date or None,
        "data_gate": runtime,
        "versions": list(VERSION_MATRIX),
        "decision_scope": "RESEARCH_ONLY",
        "actionable_output_allowed": False,
        "data": rows,
        "total": len(rows),
        "source": "production_rule_screener",
        "error": str(raw.get("error") or "")[:300],
    }


__all__ = [
    "PRESETS",
    "VERSION_MATRIX",
    "ScreenerRunRequest",
    "router",
    "screener_catalog",
    "screener_run",
    "screener_status",
]
