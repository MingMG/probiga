# -*- coding: utf-8 -*-
"""Unified stock screener and candidate-pool API.

The legacy ``/api/hot-data/screen-stocks`` endpoint remains available for
compatibility, while this router provides one stable contract for the new
workbench: presets, composable filters, saved screens and a research-only
candidate pool.  It deliberately never places orders.
"""
from __future__ import annotations

import base64
import json
import hashlib
import logging
import math
import re
import uuid
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.common.manual_scheduler_launch import launch_registered_scheduler_task
from server.common.pit_facts import (
    EVENT_REVISION_TABLE,
    FINANCE_REVISION_TABLE,
    PIT_AVAILABLE,
    PIT_DATA_BLOCKED,
    load_event_facts,
    load_finance_facts,
    normalize_decision_at,
    resolve_common_fact_cutoff,
)
from server.common.sql_reader import read_sql_rows
from server.common.screener_schema import ensure_screener_tables
from server.engine.production_selector import (
    board_limit_trigger_pct,
    rank_production_candidates,
    selector_contract,
    selector_run_summary,
)
from tools.manual_long_task_contracts import SCREENER_TASKS_BY_TYPE

logger = logging.getLogger(__name__)
router = APIRouter()
_ROOT = Path(__file__).resolve().parents[3]
_SCREENER_SCRIPT_PATH = "tools/run_screener_delivery.py"
_SCREENER_FILTER_KEYS = frozenset(
    {
        "exclude_st",
        "keyword",
        "limit_pct",
        "low_lookback",
        "ma_slope_min",
        "max_60d_gain",
        "max_boards",
        "max_change",
        "max_from_low",
        "min_amount",
        "min_boards",
        "min_change",
        "min_flow",
        "min_score",
        "min_turnover",
        "minimum_active_members",
        "new_high_pct",
        "trend_days",
        "vol_boost",
        "vol_ratio_max",
        "vol_ratio_min",
    }
)


PRESETS: tuple[dict[str, Any], ...] = (
    {
        "key": "intraday_sector",
        "name": "盘中主线",
        "mode": "intraday_sector",
        "category": "盘中",
        "description": "使用新鲜全市场行情识别同一概念内的联动上涨，避免用昨日日线替代今天盘中主线。",
        "defaults": {"min_change": 1.0, "minimum_active_members": 2},
    },
    {
        "key": "trend_breakout",
        "name": "趋势突破",
        "mode": "trend_strong",
        "category": "趋势",
        "description": "四线多头、持续站上均线、接近阶段高点，适合顺势研究。",
        "defaults": {"trend_days": 5, "ma_slope_min": 0.2, "vol_ratio_min": 0.5, "vol_ratio_max": 3.0, "new_high_pct": 0.90},
    },
    {
        "key": "startup_breakout",
        "name": "启动突破",
        "mode": "startup",
        "category": "启动",
        "description": "盘整区间突破、温和放量、价格仍在趋势可控区间。",
        "defaults": {"vol_boost": 1.3, "min_change": 2, "max_change": 12},
    },
    {
        "key": "oversold_reversal",
        "name": "低位反转",
        "mode": "low_start",
        "category": "反转",
        "description": "靠近阶段低位、出现放量和止跌反弹迹象。",
        "defaults": {"low_lookback": 20, "max_from_low": 0.05, "vol_boost": 1.5, "min_change": 3, "max_change": 20},
    },
    {
        "key": "capital_support",
        "name": "资金承接",
        "mode": "flow",
        "category": "资金",
        "description": "按主力净流入初筛，再结合技术、板块和风险证据确认。",
        "defaults": {"min_main_flow": 5000000},
    },
    {
        "key": "momentum_ladder",
        "name": "情绪连板",
        "mode": "ladder",
        "category": "情绪",
        "description": "识别连续涨停附近的情绪强势股，仅作为短线研究池。",
        "defaults": {"min_boards": 2, "max_boards": 5, "limit_pct": 9.5},
    },
    {
        "key": "dragon_tiger",
        "name": "龙虎榜事件",
        "mode": "lhb",
        "category": "事件",
        "description": "把龙虎榜作为事件和资金证据，不直接等同于买入信号。",
        "defaults": {},
    },
    {
        "key": "technical_cross",
        "name": "MACD 金叉",
        "mode": "macd",
        "category": "技术",
        "description": "使用真实 EMA/MACD/KDJ 计算，只保留当日 DIF 上穿 DEA。",
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
        "role": "生产融合选股基础分",
        "lifecycle": "PRODUCTION_BASE",
        "decision": "ACTIVE_BASE",
        "production_selector": True,
        "order_authority": False,
        "reason": "保留至少 70% 排名权重；数据闸门不通过时只显示观察结果。",
    },
    {
        "version": "V4",
        "role": "入场、追高、事件、流动性与可执行性硬门禁",
        "lifecycle": "PRODUCTION_ADVISORY",
        "decision": "ACTIVE_BOUNDED",
        "production_selector": True,
        "order_authority": False,
        "research_release_gate": "BLOCK_ORDER_AUTHORITY",
        "reason": "硬拒绝不可被其他版本覆盖；通过后以最大 12% 权重参与排序。",
    },
    {
        "version": "V5",
        "role": "全市场状态与个股策略适配分离修正",
        "lifecycle": "PRODUCTION_ADVISORY",
        "decision": "ACTIVE_BOUNDED",
        "production_selector": True,
        "order_authority": False,
        "research_release_gate": "BLOCK_ORDER_AUTHORITY",
        "reason": "全局市场状态与个股特征分离，最大权重 10%；缺证据回退 V3。",
    },
    {
        "version": "V6",
        "role": "PIT 财务质量、现金流、成长与估值修正",
        "lifecycle": "PRODUCTION_ADVISORY",
        "decision": "ACTIVE_BOUNDED",
        "production_selector": True,
        "order_authority": False,
        "research_release_gate": "BLOCK_ORDER_AUTHORITY",
        "reason": "仅接收决策日当时已知的财务证据，最大权重 8%；未来数据直接回退。",
    },
)

_A_SHARE_CODE_RE = re.compile(r"^(?:00|30|60|68|92)[0-9]{4}$")
_ACTIONABLE_SIGNAL_STATUSES = frozenset({"CONFIRM", "BUY_READY"})
_FULL_MARKET_SCAN_LIMIT = 6000
_CORRELATION_CANDIDATE_LIMIT = 800
_CORRELATION_THRESHOLD = 0.85


def _explicit_db_true(value: Any) -> bool:
    return value is True or (type(value) is int and value == 1)


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    try:
        decoded = json.loads(str(value or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return decoded if isinstance(decoded, list) else []


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        decoded = json.loads(str(value or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _analysis_pit_binding(row: dict[str, Any]) -> tuple[bool, str]:
    """Prove persisted scores used the same immutable facts selected now."""

    revision_id = str(row.get("finance_revision_id") or "")
    content_hash = str(row.get("finance_content_hash") or "")
    flags = {str(item) for item in _json_list(row.get("analysis_data_quality_flags"))}
    if revision_id and content_hash:
        if (
            f"finance_revision_id={revision_id}" not in flags
            or f"finance_content_hash={content_hash}" not in flags
        ):
            return False, "PIT_SCORE_BINDING_FINANCE_REVISION_MISMATCH"
    else:
        coverage_id = str(row.get("finance_coverage_id") or "")
        response_hash = str(row.get("finance_coverage_response_hash") or "")
        watermark_hash = str(row.get("finance_coverage_watermark_hash") or "")
        if not (
            row.get("finance_authoritative_empty") is True
            and coverage_id
            and response_hash
            and watermark_hash
            and f"finance_coverage_id={coverage_id}" in flags
            and f"finance_coverage_response_hash={response_hash}" in flags
            and f"finance_coverage_watermark_hash={watermark_hash}" in flags
        ):
            return False, "PIT_SCORE_BINDING_FINANCE_COVERAGE_MISMATCH"

    event_detail = _json_dict(row.get("analysis_event_risk_detail"))
    stored_ids = [str(value) for value in event_detail.get("event_revision_ids") or []]
    stored_hashes = [str(value) for value in event_detail.get("event_content_hashes") or []]
    selected_ids = [str(value) for value in row.get("event_revision_ids") or []]
    selected_hashes = [str(value) for value in row.get("event_content_hashes") or []]
    if selected_ids or stored_ids:
        if (
            len(stored_ids) != len(stored_hashes)
            or sorted(zip(stored_ids, stored_hashes))
            != sorted(zip(selected_ids, selected_hashes))
        ):
            return False, "PIT_SCORE_BINDING_EVENT_REVISION_MISMATCH"
    else:
        coverage_id = str(row.get("event_coverage_id") or "")
        response_hash = str(row.get("event_coverage_response_hash") or "")
        watermark_hash = str(row.get("event_coverage_watermark_hash") or "")
        if not (
            row.get("event_authoritative_empty") is True
            and event_detail.get("event_authoritative_empty") is True
            and coverage_id
            and response_hash
            and watermark_hash
            and str(event_detail.get("event_coverage_id") or "") == coverage_id
            and str(event_detail.get("event_coverage_response_hash") or "")
            == response_hash
            and str(event_detail.get("event_coverage_watermark_hash") or "")
            == watermark_hash
        ):
            return False, "PIT_SCORE_BINDING_EVENT_COVERAGE_MISMATCH"
    return True, ""


def _candidate_new_buy_action(row: dict[str, Any]) -> tuple[str, bool, str]:
    """Return display action without ever upgrading missing gates to a buy."""
    recommend = str(row.get("recommend_status") or "DATA_BLOCKED").upper()
    signal = str(row.get("signal_status") or "WATCH").upper()
    chase = str(row.get("chase_risk_status") or "DATA_BLOCKED").upper()
    ordinary = _explicit_db_true(row.get("ordinary_buy_eligible"))
    main_wave = str(row.get("main_wave_signal") or "").upper()
    if signal in {"SELL", "SELL_ALERT", "EXIT", "REDUCE"}:
        return signal, False, "当前为持仓退出/减仓信号，不是新买资格"
    if main_wave in {"SELL", "SELL_ALERT", "EXIT", "REDUCE"}:
        return main_wave, False, "主升浪/趋势模块发出退出或减仓信号"
    if (
        recommend == "ALLOW"
        and signal in _ACTIONABLE_SIGNAL_STATUSES
        and chase == "ALLOW"
        and ordinary
    ):
        return signal, True, "推荐、信号、追高风险和普通买入资格均明确通过"
    missing = [
        name
        for name, value in (
            ("recommend_status", row.get("recommend_status")),
            ("signal_status", row.get("signal_status")),
            ("chase_risk_status", row.get("chase_risk_status")),
            ("ordinary_buy_eligible", row.get("ordinary_buy_eligible")),
        )
        if value is None
    ]
    if missing:
        return "DATA_BLOCKED", False, "缺少强制执行门字段: " + ",".join(missing)
    if chase != "ALLOW" or not ordinary:
        return chase if chase != "ALLOW" else "EXECUTION_BLOCKED", False, "追高/可成交性门未通过"
    if recommend != "ALLOW":
        return recommend, False, "推荐资格未通过"
    return "WATCH", False, "信号尚未达到 CONFIRM/BUY_READY"


class ScreenerRunRequest(BaseModel):
    preset: str = "trend_breakout"
    as_of_date: str = ""
    universe: str = "market"
    concept_code: str = ""
    top: int = Field(default=50, ge=1, le=200)
    filters: dict[str, Any] = Field(default_factory=dict)
    notify: bool = False


class SavedScreenRequest(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    definition: dict[str, Any] = Field(default_factory=dict)


class CandidateSaveRequest(BaseModel):
    stock_code: str = Field(min_length=1, max_length=12)
    stock_name: str = ""
    source: str = "screener"
    screen_name: str = ""
    score: float | None = None
    as_of_date: str = ""
    reason: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)


def _clean_date(value: Any) -> str:
    raw = str(value or "").strip()[:10]
    if not raw:
        return ""
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError:
        return ""


def _validated_screener_task_payload(request: ScreenerRunRequest) -> dict[str, Any]:
    raw = request.model_dump() if hasattr(request, "model_dump") else request.dict()
    preset = str(raw.get("preset") or "").strip()
    if preset not in PRESET_MAP:
        raise HTTPException(status_code=422, detail="unknown screener preset")
    as_of_raw = str(raw.get("as_of_date") or "").strip()
    as_of_date = _clean_date(as_of_raw)
    if as_of_raw and as_of_date != as_of_raw:
        raise HTTPException(status_code=422, detail="as_of_date must be YYYY-MM-DD")
    universe = str(raw.get("universe") or "").strip()
    if universe not in {"market", "portfolio", "concept"}:
        raise HTTPException(status_code=422, detail="unknown screener universe")
    concept_code = str(raw.get("concept_code") or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_.:-]{0,32}", concept_code):
        raise HTTPException(status_code=422, detail="invalid concept_code")
    if universe == "concept" and not concept_code:
        raise HTTPException(status_code=422, detail="concept_code is required")

    filters = dict(raw.get("filters") or {})
    unknown = sorted(set(filters) - _SCREENER_FILTER_KEYS)
    if unknown:
        raise HTTPException(status_code=422, detail="unknown screener filters")
    normalized_filters: dict[str, Any] = {}
    for key, value in filters.items():
        if key == "exclude_st":
            if type(value) is not bool:
                raise HTTPException(status_code=422, detail="exclude_st must be boolean")
            normalized_filters[key] = value
        elif key == "keyword":
            keyword = str(value or "").strip()
            if len(keyword) > 120:
                raise HTTPException(status_code=422, detail="keyword is too long")
            normalized_filters[key] = keyword
        else:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise HTTPException(status_code=422, detail="numeric screener filter required")
            number = float(value)
            if not math.isfinite(number) or abs(number) > 1_000_000_000_000:
                raise HTTPException(status_code=422, detail="screener filter is out of range")
            normalized_filters[key] = value

    return {
        "preset": preset,
        "as_of_date": as_of_date,
        "universe": universe,
        "concept_code": concept_code,
        "top": int(request.top),
        "filters": normalized_filters,
        "notify": bool(request.notify),
    }


def _encode_screener_task_request(request: ScreenerRunRequest) -> str:
    payload = _validated_screener_task_payload(request)
    encoded = base64.urlsafe_b64encode(
        json.dumps(
            payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).decode("ascii")
    if len(encoded) > 450:
        raise HTTPException(status_code=422, detail="screener request is too large")
    return encoded


def decode_screener_task_request(token: str) -> ScreenerRunRequest:
    """Task-side strict inverse of the API's URL-safe request token."""

    raw_token = str(token or "").strip()
    if not raw_token or len(raw_token) > 450 or not re.fullmatch(r"[A-Za-z0-9_=-]+", raw_token):
        raise ValueError("invalid screener request token")
    try:
        decoded = base64.b64decode(raw_token, altchars=b"-_", validate=True)
        payload = json.loads(decoded.decode("utf-8"))
    except Exception as exc:
        raise ValueError("invalid screener request token") from exc
    if not isinstance(payload, dict):
        raise ValueError("invalid screener request token")
    request = (
        ScreenerRunRequest.model_validate(payload)
        if hasattr(ScreenerRunRequest, "model_validate")
        else ScreenerRunRequest.parse_obj(payload)
    )
    # Re-run the same semantic allow-list before task execution.
    normalized = _validated_screener_task_payload(request)
    return (
        ScreenerRunRequest.model_validate(normalized)
        if hasattr(ScreenerRunRequest, "model_validate")
        else ScreenerRunRequest.parse_obj(normalized)
    )


def _screener_task_type(preset: str) -> str:
    return (
        "screener_intraday_delivery"
        if str(preset) == "intraday_sector"
        else "screener_premarket_delivery"
    )


def _engine_rows(sql: str, params: dict[str, Any] | None = None, context: str = "screener") -> list[dict]:
    return read_sql_rows(get_engine(), sql, params or {}, context=context, stringify_datetime=True)


def _latest_date(table: str, requested: str = "", column: str = "trade_date") -> str:
    requested = _clean_date(requested)
    where = f" WHERE `{column}` <= :d" if requested else ""
    params = {"d": requested} if requested else {}
    rows = _engine_rows(
        f"SELECT MAX(`{column}`) AS value FROM `{table}`{where}",
        params,
        context=f"screener_latest_{table}",
    )
    return _clean_date(rows[0].get("value")) if rows else ""


_FRESHNESS_SOURCES: dict[str, tuple[str, str]] = {
    "daily_kline": ("sm_stock_kline", "trade_date"),
    "current_quote": ("sm_stock_current", "snapshot_at"),
    "capital_flow": ("sm_stock_capital_flow_daily", "trade_date"),
    "analysis": ("stock_analysis_result", "analysis_date"),
    "recommendation": ("st_recommended_stocks", "pick_date"),
    "news": ("st_news_flash", "publish_time"),
    "notice": ("si_notice_eastmoney", "notice_date"),
}


def _safe_runtime_latest_date(source: str) -> str:
    table, column = _FRESHNESS_SOURCES[source]
    try:
        return _latest_date(table, column=column)
    except Exception as exc:  # pragma: no cover - defensive production fallback
        logger.warning("Unable to read screener freshness source %s: %s", source, exc)
        return ""


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


def _runtime_status(now: datetime | None = None) -> dict[str, Any]:
    expected = _expected_completed_session(now)
    dates = {
        source: _safe_runtime_latest_date(source)
        for source in _FRESHNESS_SOURCES
    }
    required = {
        source: _freshness_state(dates[source], expected)
        for source in ("daily_kline", "capital_flow")
    }
    selection_ready = all(item["fresh"] for item in required.values())
    recommendation_required = {
        source: _freshness_state(dates[source], expected)
        for source in _FRESHNESS_SOURCES
    }
    recommendation_ready = all(
        item["fresh"] for item in recommendation_required.values()
    )
    if not selection_ready:
        status = "blocked"
        gate = "DATA_STALE"
        message = "基础行情或资金数据未到最近完整交易日；结果仅可回看研究，禁止作为交易输入。"
    elif not recommendation_ready:
        status = "degraded"
        gate = "RESEARCH_WATCH_ONLY_RECOMMENDATION_BLOCKED"
        message = "基础规则选股可用，但新闻、公告、分析或推荐证据仍有滞后；AI 推荐继续封锁。"
    else:
        status = "ok"
        gate = "RESEARCH_WATCH_ONLY"
        message = "选股与推荐证据均已到最近完整交易日；结果仍只允许研究观察。"
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


def _ensure_tables() -> None:
    ensure_screener_tables(get_engine())


def _number(value: Any, default: float | None = None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _request_payload(request: ScreenerRunRequest) -> dict[str, Any]:
    payload = request.model_dump() if hasattr(request, "model_dump") else request.dict()
    payload.pop("notify", None)
    return payload


def _screener_run_key(request: ScreenerRunRequest, result: dict[str, Any]) -> str:
    rows = result.get("data") or []
    signature = {
        "request": _request_payload(request),
        "data_date": result.get("data_date"),
        "evidence_date": result.get("evidence_date"),
        "observed_at": result.get("observed_at"),
        "freshness": result.get("freshness"),
        "selector": (result.get("selector") or {}).get("model_fingerprint"),
        "results": [
            [
                row.get("rank"),
                row.get("stock_code"),
                row.get("ensemble_score", row.get("score")),
            ]
            for row in rows
        ],
    }
    raw = json.dumps(signature, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _persist_screener_run(request: ScreenerRunRequest, result: dict[str, Any]) -> dict[str, Any]:
    """Persist the immutable ranking before it is returned to a page or notifier."""
    _ensure_tables()
    run_key = _screener_run_key(request, result)
    generated_at = datetime.now().replace(microsecond=0)
    run_uid = uuid.uuid4().hex
    rows = result.get("data") or []
    request_json = json.dumps(_request_payload(request), ensure_ascii=False, default=str, separators=(",", ":"))
    summary_json = json.dumps(result.get("stats") or {}, ensure_ascii=False, default=str, separators=(",", ":"))
    selector_json = json.dumps(result.get("selector") or {}, ensure_ascii=False, default=str, separators=(",", ":"))
    observed_date = _clean_date(str(result.get("observed_at") or "")[:10])
    session_date = observed_date or (_clean_date(result.get("requested_date")) if request.as_of_date else "") or generated_at.date().isoformat()
    with get_engine().begin() as conn:
        existing = conn.execute(
            text("""
                SELECT run_uid, session_date, data_date, evidence_date,
                       observed_at, generated_at, push_status
                FROM st_screener_run_history
                WHERE run_key = :run_key
                LIMIT 1
            """),
            {"run_key": run_key},
        ).mappings().first()
        if existing:
            return {
                "run_uid": str(existing["run_uid"]),
                "run_key": run_key,
                "session_date": str(existing.get("session_date") or ""),
                "data_date": str(existing.get("data_date") or ""),
                "evidence_date": str(existing.get("evidence_date") or ""),
                "observed_at": str(existing.get("observed_at") or ""),
                "generated_at": str(existing["generated_at"]),
                "persisted": True,
                "is_new": False,
                "push_status": str(existing.get("push_status") or ""),
            }
        conn.execute(
            text("""
                INSERT INTO st_screener_run_history
                  (run_uid, run_key, preset, requested_date, session_date, data_date, evidence_date,
                   observed_at, generated_at, freshness, status, source, universe,
                   concept_code, result_count, request_json, summary_json, selector_json, push_status)
                VALUES
                  (:run_uid, :run_key, :preset, :requested_date, :session_date, :data_date, :evidence_date,
                   :observed_at, :generated_at, :freshness, :status, :source, :universe,
                   :concept_code, :result_count, :request_json, :summary_json, :selector_json, :push_status)
            """),
            {
                "run_uid": run_uid,
                "run_key": run_key,
                "preset": request.preset,
                "requested_date": _clean_date(result.get("requested_date")) or None,
                "session_date": session_date,
                "data_date": _clean_date(result.get("data_date")) or None,
                "evidence_date": _clean_date(result.get("evidence_date")) or None,
                "observed_at": str(result.get("observed_at") or "").replace("T", " ")[:19] or None,
                "generated_at": generated_at,
                "freshness": str(result.get("freshness") or "")[:32],
                "status": str(result.get("status") or "")[:32],
                "source": str(result.get("source") or "")[:255],
                "universe": request.universe,
                "concept_code": request.concept_code,
                "result_count": len(rows),
                "request_json": request_json,
                "summary_json": summary_json,
                "selector_json": selector_json,
                "push_status": "PENDING" if request.notify else "NOT_REQUESTED",
            },
        )
        for index, row in enumerate(rows, 1):
            stock_code = str(row.get("stock_code") or "").strip().split(".")[0].zfill(6)
            if not stock_code:
                continue
            conn.execute(
                text("""
                    INSERT INTO st_screener_run_result
                      (run_uid, rank_no, selector_rank, stock_code, stock_name, score,
                       ensemble_score, candidate_grade, action_status, primary_concept,
                       change_pct, price, payload_json)
                    VALUES
                      (:run_uid, :rank_no, :selector_rank, :stock_code, :stock_name, :score,
                       :ensemble_score, :candidate_grade, :action_status, :primary_concept,
                       :change_pct, :price, :payload_json)
                """),
                {
                    "run_uid": run_uid,
                    "rank_no": int(row.get("rank") or index),
                    "selector_rank": int(row.get("selector_rank") or 0) or None,
                    "stock_code": stock_code,
                    "stock_name": str(row.get("stock_name") or row.get("short_name") or "")[:120],
                    "score": _number(row.get("score")),
                    "ensemble_score": _number(row.get("ensemble_score")),
                    "candidate_grade": str(row.get("candidate_grade") or "")[:20],
                    "action_status": str((row.get("decision_readiness") or {}).get("recommend_status") or row.get("signal_status") or "")[:40],
                    "primary_concept": str(row.get("primary_concept") or row.get("concept_name") or "")[:120],
                    "change_pct": _number(row.get("change_pct")),
                    "price": _number(row.get("price", row.get("close"))),
                    "payload_json": json.dumps(row, ensure_ascii=False, default=str, separators=(",", ":")),
                },
            )
    return {
        "run_uid": run_uid,
        "run_key": run_key,
        "session_date": session_date,
        "data_date": _clean_date(result.get("data_date")),
        "evidence_date": _clean_date(result.get("evidence_date")),
        "observed_at": str(result.get("observed_at") or "").replace("T", " ")[:19],
        "generated_at": generated_at.isoformat(sep=" "),
        "persisted": True,
        "is_new": True,
        "push_status": "PENDING" if request.notify else "NOT_REQUESTED",
    }


def _update_screener_push(run_uid: str, notification: dict[str, Any]) -> None:
    status = str(notification.get("status") or "error").upper()[:32]
    error = str(notification.get("error") or notification.get("reason") or "")[:500] or None
    with get_engine().begin() as conn:
        conn.execute(
            text("""
                UPDATE st_screener_run_history
                SET push_status = :status,
                    push_error = :error,
                    pushed_at = CASE WHEN :status = 'SENT' THEN CURRENT_TIMESTAMP ELSE pushed_at END
                WHERE run_uid = :run_uid
            """),
            {"status": status, "error": error, "run_uid": run_uid},
        )


def _codes_for_universe(universe: str, concept_code: str = "") -> set[str] | None:
    universe = str(universe or "market").strip().lower()
    if universe in {"market", "all", ""}:
        return None
    if universe in {"portfolio", "watchlist"}:
        rows = _engine_rows("SELECT stock_code FROM st_user_portfolio", context="screener_universe_portfolio")
        return {str(row.get("stock_code") or "").zfill(6) for row in rows if row.get("stock_code")}
    if universe == "concept":
        code = str(concept_code or "").strip()
        if not code:
            return set()
        rows = _engine_rows(
            "SELECT DISTINCT stock_code FROM si_stock_concept_east WHERE concept_code = :code",
            {"code": code},
            context="screener_universe_concept",
        )
        return {str(row.get("stock_code") or "").zfill(6) for row in rows if row.get("stock_code")}
    raise HTTPException(status_code=400, detail=f"不支持的股票范围: {universe}")


def _listed_codes(as_of_date: str) -> set[str] | None:
    target = _clean_date(as_of_date)
    if not target:
        return None
    try:
        rows = _engine_rows(
            """
            SELECT stock_code
            FROM si_all_code
            WHERE stock_code REGEXP '^(00|30|60|68|92)[0-9]{4}$'
              AND list_date IS NOT NULL
              AND list_date <= :target_date
            """,
            {"target_date": target},
            context="screener_listed_universe",
        )
        return {
            str(row.get("stock_code") or "").strip().zfill(6)
            for row in rows
            if row.get("stock_code")
        }
    except Exception as exc:  # pragma: no cover - defensive DB fallback
        logger.warning("Unable to load listed screener universe: %s", exc)
        return None


def _apply_filters(
    rows: list[dict],
    request: ScreenerRunRequest,
    *,
    listed_codes: set[str] | None = None,
) -> tuple[list[dict], dict[str, Any]]:
    filters = request.filters or {}
    codes = _codes_for_universe(request.universe, request.concept_code)
    min_change = _number(filters.get("min_change"))
    max_change = _number(filters.get("max_change"))
    min_turnover = _number(filters.get("min_turnover"))
    min_amount = _number(filters.get("min_amount"))
    min_flow = _number(filters.get("min_flow"))
    min_score = _number(filters.get("min_score"))
    keyword = str(filters.get("keyword") or "").strip().lower()
    exclude_st = bool(filters.get("exclude_st", True))
    filtered: list[dict] = []
    for row in rows:
        code = str(row.get("stock_code") or "").zfill(6)
        name = str(row.get("short_name") or row.get("stock_name") or code)
        if not _A_SHARE_CODE_RE.fullmatch(code):
            continue
        if listed_codes is not None and code not in listed_codes:
            continue
        if codes is not None and code not in codes:
            continue
        normalized_name = name.lower().replace(" ", "")
        if exclude_st and (
            "st" in normalized_name
            or "退" in name
        ):
            continue
        if keyword and keyword not in f"{code} {name}".lower():
            continue
        change = _number(row.get("change_pct"))
        turnover = _number(row.get("turnover_ratio", row.get("turnover_rate")))
        amount = _number(row.get("amount"))
        flow = _number(row.get("main_net_inflow"))
        if min_change is not None and (change is None or change < min_change):
            continue
        if max_change is not None and (change is None or change > max_change):
            continue
        if min_turnover is not None and (turnover is None or turnover < min_turnover):
            continue
        if min_amount is not None and (amount is None or amount < min_amount):
            continue
        if min_flow is not None and (flow is None or flow < min_flow):
            continue

        source_score = _number(row.get("final_trade_score", row.get("ai_score")))
        if source_score is None:
            source_score = 50.0
            if change is not None:
                source_score += max(-10.0, min(15.0, change * 1.2))
            if flow is not None and flow > 0:
                source_score += 8.0
            if (_number(row.get("vol_ratio"), 0) or 0) >= 1.2:
                source_score += 4.0
            if row.get("above_ma5_days") is not None:
                source_score += min(12.0, _number(row.get("above_ma5_days"), 0) or 0)
            if row.get("boards") is not None:
                source_score += min(15.0, (_number(row.get("boards"), 0) or 0) * 3.0)
        source_score = round(max(0.0, min(100.0, source_score)), 1)
        if min_score is not None and source_score < min_score:
            continue

        matched: list[str] = []
        for key, label in (
            ("change_pct", "涨跌幅"), ("turnover_ratio", "换手率"),
            ("main_net_inflow", "主力净流入"), ("vol_ratio", "量能"),
            ("above_ma5_days", "趋势持续"), ("boards", "连板"),
            ("golden_cross", "MACD金叉"),
        ):
            value = row.get(key)
            if value is not None and value is not False:
                matched.append(label)
        if not matched:
            matched.append("预设条件")
        row = dict(row)
        row.update({
            "stock_code": code,
            "stock_name": name,
            "score": source_score,
            "matched_conditions": matched,
            "explanation": "、".join(matched),
            "signal_status": row.get("signal_status") or row.get("recommend_status") or "WATCH",
            "risk_level": row.get("event_risk_level") or "UNKNOWN",
            "decision_scope": "PRODUCTION_SELECTION_ADVISORY",
            "action": "WATCH",
            "actionable": False,
        })
        filtered.append(row)
    ranked = rank_production_candidates(filtered)
    return ranked[: request.top], {
        "universe": request.universe,
        "filter_count": len(filters),
        "input_count": len(rows),
        "result_count": len(ranked),
    }


def _enrich_selector_evidence(
    rows: list[dict],
    target_date: str,
    *,
    decision_at: datetime | str | None = None,
) -> list[dict]:
    """Attach same-day analysis/recommendation evidence used by V4/V5/V6.

    Legacy preset queries intentionally return a light market row.  The
    production selector needs the frozen same-day analysis snapshot as well;
    this bounded join prevents the seven presets from silently falling back to
    V3 merely because their original SQL predates the ensemble.
    """
    codes = sorted(
        {
            str(row.get("stock_code") or "").zfill(6)
            for row in rows
            if _A_SHARE_CODE_RE.fullmatch(str(row.get("stock_code") or "").zfill(6))
        }
    )
    if not codes:
        return [dict(row) for row in rows]
    if decision_at is None and target_date == date.today().isoformat():
        decision_at = datetime.now().replace(microsecond=0)
    try:
        exact_decision_at = (
            normalize_decision_at(decision_at)
            if decision_at is not None
            else None
        )
    except (TypeError, ValueError):
        # An invalid or date-only decision timestamp is a data-quality failure,
        # not permission to fall back to a mutable current-state table.
        exact_decision_at = None
    common_cutoff: dict[str, Any] = {
        "status": PIT_DATA_BLOCKED,
        "reason": "PIT_COMMON_CUTOFF_EXACT_DECISION_TIME_REQUIRED",
        "fact_cutoff_at": "",
        "receipt_root_hash": "",
    }
    if exact_decision_at is not None:
        common_cutoff = resolve_common_fact_cutoff(
            get_engine(),
            codes=codes,
            decision_at=exact_decision_at,
            finance_start_date="1900-01-01",
            finance_end_date=target_date,
            event_start_date=(date.fromisoformat(target_date) - timedelta(days=14)),
            event_end_date=target_date,
            require_qmt_event_batch=True,
        )
    pit_reader_decision_at = (
        exact_decision_at
        if common_cutoff.get("status") == PIT_AVAILABLE
        else None
    )
    fact_cutoff_at = common_cutoff.get("fact_cutoff_at") or None
    params: dict[str, Any] = {"target_date": target_date}
    placeholders: list[str] = []
    for index, code in enumerate(codes):
        key = f"code_{index}"
        params[key] = code
        placeholders.append(f":{key}")
    code_sql = ",".join(placeholders)
    evidence_by_code: dict[str, dict[str, Any]] = {code: {} for code in codes}

    def _safe_selector_rows(sql: str, query_params=None, *, context: str):
        try:
            return _engine_rows(sql, query_params, context=context)
        except Exception as exc:
            logger.warning(
                "selector evidence source %s unavailable for %s: %s",
                context,
                target_date,
                exc,
            )
            return []

    market_mood = None
    try:
        market_rows = _safe_selector_rows(
            f"""
            SELECT stock_code, short_name, close, high, low, volume, amount,
                   turnover_ratio, change_pct
            FROM sm_stock_kline
            WHERE trade_date = :target_date
              AND k_type = 1 AND adjust_type = 0
              AND stock_code IN ({code_sql})
            """,
            params,
            context="screener_selector_market_evidence",
        )
        for evidence in market_rows:
            code = str(evidence.get("stock_code") or "").zfill(6)
            evidence_by_code.setdefault(code, {}).update(
                {key: value for key, value in evidence.items() if key != "stock_code" and value is not None}
            )

        analysis_rows = _safe_selector_rows(
            f"""
            SELECT stock_code,
                   long_term_score, fundamental_score AS fundamental,
                   growth_score, valuation_score AS valuation,
                   risk_score, short_term_score, capital_score,
                   technical_score AS entry_score,
                   sentiment_score, event_score, event_risk_level,
                   recommend_status, data_quality_score AS quality_score,
                   ordinary_buy_eligible, chase_risk_status,
                   data_quality_flags AS analysis_data_quality_flags,
                   event_risk_detail AS analysis_event_risk_detail
            FROM stock_analysis_result
            WHERE analysis_date = :target_date
              AND stock_code IN ({code_sql})
            """,
            params,
            context="screener_selector_analysis_evidence",
        )
        for evidence in analysis_rows:
            code = str(evidence.get("stock_code") or "").zfill(6)
            evidence_by_code.setdefault(code, {}).update(
                {key: value for key, value in evidence.items() if key != "stock_code" and value is not None}
            )

        recommendation_rows = _safe_selector_rows(
            f"""
            SELECT stock_code, final_trade_score, ai_score,
                   fundamental, valuation, long_term_score, short_term_score,
                   capital_score, sentiment_score, market_mood_score,
                   ultra_short_score, swing_score, main_wave_score,
                   quality_score, entry_score, risk_reward_ratio,
                   heat_overload_score, failure_penalty_score,
                   sector_rotation_score, chip_capital_score,
                   expected_return_score, event_risk_level,
                   chase_risk_status, ordinary_buy_eligible
            FROM st_recommended_stocks
            WHERE pick_date = :target_date
              AND stock_code IN ({code_sql})
            """,
            params,
            context="screener_selector_recommendation_evidence",
        )
        for evidence in recommendation_rows:
            code = str(evidence.get("stock_code") or "").zfill(6)
            evidence_by_code.setdefault(code, {}).update(
                {key: value for key, value in evidence.items() if key != "stock_code" and value is not None}
            )

        mood_rows = _safe_selector_rows(
            """
            SELECT AVG(NULLIF(market_mood_score, 0)) AS market_mood_score
            FROM st_recommended_stocks WHERE pick_date = :target_date
            """,
            {"target_date": target_date},
            context="screener_selector_market_mood",
        )
        market_mood = mood_rows[0].get("market_mood_score") if mood_rows else None

        if pit_reader_decision_at is None:
            for code in codes:
                evidence_by_code.setdefault(code, {}).update(
                    {
                        "finance_pit_verified": False,
                        "finance_pit_status": PIT_DATA_BLOCKED,
                        "finance_pit_reason": (
                            common_cutoff.get("reason")
                            or "PIT_FINANCE_EXACT_DECISION_TIME_REQUIRED"
                        ),
                        "finance_source": FINANCE_REVISION_TABLE,
                        "event_pit_verified": False,
                        "event_pit_status": PIT_DATA_BLOCKED,
                        "event_pit_reason": (
                            common_cutoff.get("reason")
                            or "PIT_EVENT_EXACT_DECISION_TIME_REQUIRED"
                        ),
                        "event_source": EVENT_REVISION_TABLE,
                    }
                )
        else:
            finance_batch = load_finance_facts(
                get_engine(),
                codes=codes,
                decision_at=pit_reader_decision_at,
                fact_cutoff_at=fact_cutoff_at,
                as_of_date=target_date,
            )
            for code in codes:
                raw = dict(finance_batch.facts.get(code) or {})
                coverage = dict(
                    finance_batch.coverage_by_code.get(code) or {}
                )
                status = finance_batch.status_for(code)
                payload = {
                    key: value
                    for key, value in raw.items()
                    if value is not None
                }
                payload.update(
                    {
                        "finance_pit_verified": status == PIT_AVAILABLE,
                        "finance_pit_status": (
                            PIT_AVAILABLE
                            if status == PIT_AVAILABLE
                            else PIT_DATA_BLOCKED
                        ),
                        "finance_pit_reason": (
                            finance_batch.reason_for(code)
                            or (
                                ""
                                if status == PIT_AVAILABLE
                                else "PIT_FINANCE_COVERAGE_UNPROVEN"
                            )
                        ),
                        "finance_manifest_hash": finance_batch.manifest_hash,
                        "finance_source": FINANCE_REVISION_TABLE,
                        "finance_authoritative_empty": bool(
                            status == PIT_AVAILABLE and not raw and coverage
                        ),
                        "finance_coverage_id": coverage.get("coverage_id"),
                        "finance_coverage_response_hash": coverage.get(
                            "coverage_response_hash"
                        ),
                        "finance_coverage_watermark_hash": coverage.get(
                            "coverage_watermark_hash"
                        ),
                    }
                )
                evidence_by_code.setdefault(code, {}).update(payload)
            event_batch = load_event_facts(
                get_engine(),
                codes=codes,
                decision_at=pit_reader_decision_at,
                fact_cutoff_at=fact_cutoff_at,
                start_date=(
                    date.fromisoformat(target_date) - timedelta(days=14)
                ),
                end_date=target_date,
                require_qmt_complete_batch=True,
            )
            for code in codes:
                status = event_batch.status_for(code)
                event_rows = list(event_batch.facts.get(code) or [])
                coverage = dict(event_batch.coverage_by_code.get(code) or {})
                evidence_by_code.setdefault(code, {}).update(
                    {
                        "event_pit_verified": status == PIT_AVAILABLE,
                        "event_pit_status": (
                            PIT_AVAILABLE
                            if status == PIT_AVAILABLE
                            else PIT_DATA_BLOCKED
                        ),
                        "event_pit_reason": (
                            event_batch.reason_for(code)
                            or (
                                ""
                                if status == PIT_AVAILABLE
                                else "PIT_EVENT_COVERAGE_UNPROVEN"
                            )
                        ),
                        "event_manifest_hash": event_batch.manifest_hash,
                        "event_revision_ids": [
                            row.get("event_revision_id") for row in event_rows
                        ],
                        "event_content_hashes": [
                            row.get("event_content_hash") for row in event_rows
                        ],
                        "event_source": EVENT_REVISION_TABLE,
                        "event_authoritative_empty": bool(
                            status == PIT_AVAILABLE
                            and not event_rows
                            and coverage
                        ),
                        "event_coverage_id": coverage.get("coverage_id"),
                        "event_coverage_response_hash": coverage.get(
                            "coverage_response_hash"
                        ),
                        "event_coverage_watermark_hash": coverage.get(
                            "coverage_watermark_hash"
                        ),
                    }
                )
    except Exception as exc:
        logger.warning("selector evidence enrichment failed for %s: %s", target_date, exc)
        for code in codes:
            evidence_by_code.setdefault(code, {}).update(
                {
                    "finance_pit_verified": False,
                    "finance_pit_status": PIT_DATA_BLOCKED,
                    "finance_pit_reason": (
                        f"PIT_FINANCE_READER_FAILED:{type(exc).__name__}"
                    ),
                    "finance_source": FINANCE_REVISION_TABLE,
                    "event_pit_verified": False,
                    "event_pit_status": PIT_DATA_BLOCKED,
                    "event_pit_reason": (
                        f"PIT_EVENT_READER_FAILED:{type(exc).__name__}"
                    ),
                    "event_source": EVENT_REVISION_TABLE,
                }
            )

    enriched: list[dict] = []
    for row in rows:
        code = str(row.get("stock_code") or "").zfill(6)
        # Frozen same-day/PIT evidence is authoritative over light preset rows;
        # otherwise a caller-provided legacy field could spoof verification.
        item = dict(row)
        item.update(evidence_by_code.get(code) or {})
        item["stock_code"] = code
        item["pit_fact_cutoff_at"] = common_cutoff.get("fact_cutoff_at") or ""
        item["pit_decision_at"] = common_cutoff.get("decision_at") or ""
        item["pit_common_receipt_root_hash"] = (
            common_cutoff.get("receipt_root_hash") or ""
        )
        item["pit_common_cutoff_status"] = common_cutoff.get("status")
        item["pit_common_cutoff_reason"] = common_cutoff.get("reason") or ""
        item["data_date"] = target_date
        item["global_market_regime_score"] = market_mood
        close = _number(item.get("close"))
        high = _number(item.get("high"))
        low = _number(item.get("low"))
        change = _number(item.get("change_pct"))
        trigger = board_limit_trigger_pct(code, item.get("short_name"))
        item["limit_trigger_pct"] = trigger
        item["limit_up_locked"] = bool(
            close is not None
            and high is not None
            and low is not None
            and change is not None
            and abs(high - low) <= 1e-9
            and abs(close - high) <= 1e-9
            and change >= trigger
        )
        if item.get("market_mood_score") in (None, 0, 0.0, "") and market_mood is not None:
            item["market_mood_score"] = market_mood
        pit_score_binding, pit_score_binding_reason = _analysis_pit_binding(
            item
        )
        item["pit_score_binding_verified"] = pit_score_binding
        item["pit_score_binding_reason"] = pit_score_binding_reason
        pit_ready = bool(
            item.get("finance_pit_verified") is True
            and item.get("event_pit_verified") is True
            and pit_score_binding
        )
        item["pit_strategy_status"] = (
            PIT_AVAILABLE if pit_ready else PIT_DATA_BLOCKED
        )
        if not pit_ready:
            item["ordinary_buy_eligible"] = False
            item["signal_status"] = "DATA_BLOCKED"
            item["recommend_status"] = "SUSPENDED"
            item["pit_strategy_reason"] = (
                "PIT_DATA_BLOCKED：财务/公告缺少决策时点可验证修订，"
                "或持久化评分未绑定相同修订证据"
            )
        enriched.append(item)
    return enriched


def _pearson_overlap(left: dict[str, float], right: dict[str, float]) -> float | None:
    dates = sorted(set(left).intersection(right))
    if len(dates) < 10:
        return None
    xs = [left[value] for value in dates]
    ys = [right[value] for value in dates]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    covariance = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    variance_x = sum((x - mean_x) ** 2 for x in xs)
    variance_y = sum((y - mean_y) ** 2 for y in ys)
    denominator = math.sqrt(variance_x * variance_y)
    return covariance / denominator if denominator > 0 else None


def _attach_correlation_clusters(rows: list[dict], target_date: str) -> list[dict]:
    """Attach deterministic 60-day return-correlation clusters to the shortlist.

    The legacy strategy SQL already orders the most relevant names first.  We
    calculate exact pairwise correlation only for a bounded 800-name shortlist
    and fail closed for rows without ten overlapping official return bars.
    """
    items = [dict(row) for row in rows]
    codes = []
    for row in items[:_CORRELATION_CANDIDATE_LIMIT]:
        code = str(row.get("stock_code") or "").zfill(6)
        if _A_SHARE_CODE_RE.fullmatch(code) and code not in codes:
            codes.append(code)
    for row in items:
        row["correlation_cluster_status"] = "DATA_BLOCKED"
    if not codes:
        return items
    params: dict[str, Any] = {"target_date": target_date}
    placeholders = []
    for index, code in enumerate(codes):
        key = f"corr_code_{index}"
        params[key] = code
        placeholders.append(f":{key}")
    try:
        price_rows = _engine_rows(
            f"""
            SELECT stock_code, trade_date, close, pre_close
            FROM sm_stock_kline
            WHERE k_type = 1 AND adjust_type = 0
              AND trade_date <= :target_date
              AND trade_date >= DATE_SUB(:target_date, INTERVAL 60 DAY)
              AND stock_code IN ({','.join(placeholders)})
            ORDER BY stock_code, trade_date
            """,
            params,
            context="screener_selector_correlation_history",
        )
    except Exception as exc:
        logger.warning("selector correlation evidence unavailable for %s: %s", target_date, exc)
        return items
    returns: dict[str, dict[str, float]] = {}
    for row in price_rows:
        code = str(row.get("stock_code") or "").zfill(6)
        close = _number(row.get("close"))
        pre_close = _number(row.get("pre_close"))
        trade_date = _clean_date(row.get("trade_date"))
        if close is None or pre_close is None or pre_close <= 0 or not trade_date:
            continue
        value = close / pre_close - 1.0
        if math.isfinite(value):
            returns.setdefault(code, {})[trade_date] = value

    representatives: list[str] = []
    assignments: dict[str, tuple[str, float]] = {}
    for code in codes:
        series = returns.get(code) or {}
        if len(series) < 10:
            continue
        selected = ""
        selected_corr = 0.0
        for representative in representatives:
            correlation = _pearson_overlap(series, returns[representative])
            if correlation is not None and correlation >= _CORRELATION_THRESHOLD:
                selected = representative
                selected_corr = correlation
                break
        if not selected:
            selected = code
            selected_corr = 1.0
            representatives.append(code)
        assignments[code] = (f"CORR:{selected}", selected_corr)

    for row in items:
        code = str(row.get("stock_code") or "").zfill(6)
        assignment = assignments.get(code)
        if assignment:
            row["correlation_cluster"] = assignment[0]
            row["correlation_cluster_status"] = "VERIFIED_60D"
            row["correlation_to_cluster_representative"] = round(assignment[1], 6)
            row["correlation_observation_count"] = len(returns.get(code) or {})
    return items


def _run_preset(request: ScreenerRunRequest, target_date: str) -> dict[str, Any]:
    preset = PRESET_MAP.get(request.preset)
    if not preset:
        raise HTTPException(status_code=400, detail=f"未知选股预设: {request.preset}")
    if preset["mode"] == "intraday_sector":
        return _run_intraday_sector(request, target_date, preset)
    from server.api.routers.hot_data import screen_stocks as legacy_screen_stocks

    defaults = dict(preset.get("defaults") or {})
    filters = request.filters or {}
    scan_limit = max(_FULL_MARKET_SCAN_LIMIT, request.top * 20)
    result = legacy_screen_stocks(
        mode=preset["mode"],
        trade_date=target_date,
        top=scan_limit,
        min_change=float(filters.get("min_change", defaults.get("min_change", 0))),
        max_change=float(filters.get("max_change", defaults.get("max_change", 20))),
        min_turnover=float(filters.get("min_turnover", 0)),
        min_main_flow=float(filters.get("min_flow", defaults.get("min_main_flow", 1_000_000))),
        min_boards=int(filters.get("min_boards", defaults.get("min_boards", 2))),
        max_boards=int(filters.get("max_boards", defaults.get("max_boards", 5))),
        vol_boost=float(filters.get("vol_boost", defaults.get("vol_boost", 1.2))),
        max_from_low=float(filters.get("max_from_low", defaults.get("max_from_low", 0.08))),
        low_lookback=int(filters.get("low_lookback", defaults.get("low_lookback", 20))),
        min_chg_trend=float(filters.get("min_change", defaults.get("min_chg_trend", -1))),
        limit_pct=float(filters.get("limit_pct", defaults.get("limit_pct", 9.5))),
        trend_days=int(filters.get("trend_days", defaults.get("trend_days", 5))),
        ma_slope_min=float(filters.get("ma_slope_min", defaults.get("ma_slope_min", 0.2))),
        vol_ratio_min=float(filters.get("vol_ratio_min", defaults.get("vol_ratio_min", 0.5))),
        vol_ratio_max=float(filters.get("vol_ratio_max", defaults.get("vol_ratio_max", 3.0))),
        max_60d_gain=float(filters.get("max_60d_gain", defaults.get("max_60d_gain", 200.0))),
        new_high_pct=float(filters.get("new_high_pct", defaults.get("new_high_pct", 0.90))),
    )
    data_date = _clean_date(result.get("data_date") or result.get("date") or target_date)
    rows = _enrich_selector_evidence(
        result.get("data") or [],
        data_date or target_date,
        decision_at=(
            datetime.now().replace(microsecond=0)
            if not _clean_date(request.as_of_date)
            else None
        ),
    )
    rows = _attach_correlation_clusters(rows, data_date or target_date)
    for row in rows:
        row.setdefault("data_date", data_date)
    listed_codes = _listed_codes(data_date or target_date)
    normalized, stats = _apply_filters(
        rows,
        request,
        listed_codes=listed_codes,
    )
    freshness = result.get("freshness") or ("exact" if data_date == target_date else "fallback")
    return {
        "status": "degraded" if result.get("error") or freshness in {"error", "unavailable"} else "ok",
        "preset": preset,
        "requested_date": _clean_date(request.as_of_date) or target_date,
        "data_date": data_date,
        "freshness": freshness,
        "source": "local_screen_engine",
        "decision_scope": "PRODUCTION_SELECTION_ADVISORY",
        "actionable_output_allowed": False,
        "selector": selector_contract(),
        "stats": {
            **stats,
            "scan_limit": scan_limit,
            "listed_universe_count": len(listed_codes or ()),
            "correlation_verified_count": sum(
                row.get("correlation_cluster_status") == "VERIFIED_60D"
                for row in normalized
            ),
            "selector_summary": selector_run_summary(normalized),
        },
        "data": normalized,
        "total": len(normalized),
        "error": result.get("error", ""),
    }


_GENERIC_CONCEPT_MARKERS = (
    "融资融券", "沪股通", "深股通", "转融券", "MSCI", "富时罗素",
    "标准普尔", "基金重仓", "社保重仓", "QFII", "预盈预增",
)

_INTRADAY_NAME_THEME_MARKERS = (
    "\u9502", "\u7a00\u571f", "\u9ec4\u91d1", "\u94dc", "\u94dd",
    "\u82af\u7247", "\u534a\u5bfc\u4f53", "\u5149\u4f0f", "\u98ce\u7535",
    "\u50a8\u80fd", "\u673a\u5668\u4eba",
)


def _intraday_quota_shortlist(
    rows: list[dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    """Keep a bounded main board while reserving space for small live themes."""
    capacity = max(1, int(limit))
    theme_groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if row.get("intraday_theme_source") != "name_keyword":
            continue
        theme_groups.setdefault(str(row.get("primary_concept") or ""), []).append(row)
    mandatory: list[dict[str, Any]] = []
    for theme_name in sorted(theme_groups):
        mandatory.extend(theme_groups[theme_name][:4])
    mandatory = mandatory[:capacity]
    mandatory_codes = {
        str(row.get("stock_code") or "") for row in mandatory
    }
    base_capacity = capacity - len(mandatory)
    base = [
        row for row in rows
        if str(row.get("stock_code") or "") not in mandatory_codes
    ][:base_capacity]
    selected = [*base, *mandatory]
    selected.sort(
        key=lambda row: (
            int(row.get("intraday_discovery_rank") or 10**9),
            str(row.get("stock_code") or ""),
        )
    )
    return selected


def _intraday_market_day_active(now: datetime | None = None) -> bool:
    current = now or datetime.now()
    current_time = current.time()
    return (
        current.weekday() < 5
        and time(9, 20) <= current_time <= time(15, 10)
    )


def _run_intraday_sector(
    request: ScreenerRunRequest,
    target_date: str,
    preset: dict[str, Any],
) -> dict[str, Any]:
    """Rank concept-linked leaders without ever granting order authority.

    During the session only a fresh ten-minute quote window is accepted.  Once
    the session has ended, the latest complete market snapshot is exposed as a
    clearly-labelled, read-only review instead of returning an empty page.
    """
    filters = request.filters or {}
    min_change = float(filters.get("min_change", 1.0))
    minimum_active = max(2, int(filters.get("minimum_active_members", 2)))
    live_rows = _engine_rows(
        """
        SELECT c.stock_code, c.short_name, c.price, c.change_pct,
               c.volume, c.amount, c.snapshot_at,
               k.close, k.high, k.low, k.turnover_ratio
        FROM sm_stock_current c
        LEFT JOIN sm_stock_kline k
          ON k.stock_code = c.stock_code
         AND k.k_type = 1 AND k.adjust_type = 0
         AND k.trade_date = :target_date
        WHERE c.snapshot_at >= DATE_SUB(NOW(), INTERVAL 10 MINUTE)
          AND c.price > 0 AND c.change_pct >= :min_change
          AND c.stock_code REGEXP '^(00|30|60|68|92)[0-9]{4}$'
        """,
        {"target_date": target_date, "min_change": min_change},
        context="screener_intraday_live_quotes",
    )
    freshness = "live"
    quote_date = date.today().isoformat()
    quote_filter_sql = "c.snapshot_at >= DATE_SUB(NOW(), INTERVAL 10 MINUTE)"
    quote_filter_params: dict[str, Any] = {}
    if not live_rows and not _intraday_market_day_active():
        latest_rows = _engine_rows(
            """
            SELECT MAX(snapshot_at) AS snapshot_at
            FROM sm_stock_current
            WHERE price > 0
            """,
            context="screener_intraday_latest_snapshot",
        )
        latest_snapshot = str((latest_rows[0] if latest_rows else {}).get("snapshot_at") or "")
        quote_date = latest_snapshot[:10]
        if quote_date:
            freshness = "historical_close"
            quote_filter_sql = "DATE(c.snapshot_at) = :quote_date"
            quote_filter_params = {"quote_date": quote_date}
            live_rows = _engine_rows(
                """
                SELECT c.stock_code, c.short_name, c.price, c.change_pct,
                       c.volume, c.amount, c.snapshot_at,
                       k.close, k.high, k.low, k.turnover_ratio
                FROM sm_stock_current c
                LEFT JOIN sm_stock_kline k
                  ON k.stock_code = c.stock_code
                 AND k.k_type = 1 AND k.adjust_type = 0
                 AND k.trade_date = :target_date
                WHERE DATE(c.snapshot_at) = :quote_date
                  AND c.price > 0 AND c.change_pct >= :min_change
                  AND c.stock_code REGEXP '^(00|30|60|68|92)[0-9]{4}$'
                """,
                {
                    "target_date": target_date,
                    "quote_date": quote_date,
                    "min_change": min_change,
                },
                context="screener_intraday_review_quotes",
            )
    observed_at = max(
        (str(row.get("snapshot_at") or "") for row in live_rows),
        default="",
    )
    if not live_rows:
        return {
            "status": "degraded",
            "preset": preset,
            "requested_date": _clean_date(request.as_of_date) or date.today().isoformat(),
            "data_date": quote_date or date.today().isoformat(),
            "freshness": "unavailable",
            "source": "sm_stock_current+si_stock_concept_east",
            "decision_scope": "PRODUCTION_SELECTION_ADVISORY",
            "actionable_output_allowed": False,
            "selector": selector_contract(),
            "stats": {"input_count": 0, "result_count": 0, "selector_summary": selector_run_summary([])},
            "data": [],
            "total": 0,
            "error": "没有可用的全市场行情快照；盘中机会暂时无法生成。",
        }

    theme_rows = _engine_rows(
        f"""
        SELECT m.theme_code AS concept_code, MAX(m.theme_name) AS concept_name,
               MAX(m.theme_source) AS theme_source,
               COUNT(DISTINCT m.stock_code) AS total_members,
               COUNT(DISTINCT CASE WHEN c.change_pct >= :active_change THEN m.stock_code END) AS active_members,
               SUM(CASE WHEN c.change_pct > 0 THEN 1 ELSE 0 END) AS positive_members,
               AVG(c.change_pct) AS average_change_pct,
               MAX(c.change_pct) AS leader_change_pct
        FROM (
            SELECT stock_code, CONCAT('CONCEPT:', concept_code) AS theme_code,
                   name AS theme_name, 'concept' AS theme_source
            FROM si_stock_concept_east
        ) m
        JOIN sm_stock_current c ON c.stock_code = m.stock_code
        WHERE {quote_filter_sql}
          AND c.price > 0
        GROUP BY m.theme_code
        HAVING active_members >= :minimum_active
           AND leader_change_pct >= 4.0
           AND average_change_pct >= 0.8
        ORDER BY average_change_pct DESC, active_members DESC, leader_change_pct DESC
        LIMIT 500
        """,
        {
            "active_change": max(2.0, min_change),
            "minimum_active": minimum_active,
            **quote_filter_params,
        },
        context="screener_intraday_theme_strength",
    )
    name_theme_members: list[dict[str, Any]] = []
    active_change = max(2.0, min_change)
    for marker in _INTRADAY_NAME_THEME_MARKERS:
        marker_members = [
            row for row in live_rows
            if marker in str(row.get("short_name") or "")
        ]
        active_members = sum(
            (_number(row.get("change_pct"), 0.0) or 0.0) >= active_change
            for row in marker_members
        )
        if active_members < minimum_active:
            continue
        changes = [
            _number(row.get("change_pct"), 0.0) or 0.0
            for row in marker_members
        ]
        average_change = sum(changes) / max(1, len(changes))
        leader_change = max(changes, default=0.0)
        if average_change < 0.8 or leader_change < 4.0:
            continue
        theme_code = f"NAME:{marker}"
        theme_rows.append({
            "concept_code": theme_code,
            "concept_name": f"{marker}\u4ea7\u4e1a\u94fe",
            "theme_source": "name_keyword",
            "total_members": len(marker_members),
            "active_members": active_members,
            "positive_members": sum(change > 0 for change in changes),
            "average_change_pct": average_change,
            "leader_change_pct": leader_change,
        })
        name_theme_members.extend(
            {
                "stock_code": row.get("stock_code"),
                "concept_code": theme_code,
                "name": f"{marker}\u4ea7\u4e1a\u94fe",
                "theme_source": "name_keyword",
            }
            for row in marker_members
        )
    theme_rows = [
        row for row in theme_rows
        if not any(marker.lower() in str(row.get("concept_name") or "").lower() for marker in _GENERIC_CONCEPT_MARKERS)
    ]
    theme_by_code: dict[str, dict[str, Any]] = {}
    for row in theme_rows:
        total = max(1.0, _number(row.get("total_members"), 1.0) or 1.0)
        active = _number(row.get("active_members"), 0.0) or 0.0
        positive = _number(row.get("positive_members"), 0.0) or 0.0
        average = _number(row.get("average_change_pct"), 0.0) or 0.0
        leader = _number(row.get("leader_change_pct"), 0.0) or 0.0
        row = dict(row)
        row["positive_breadth"] = positive / total
        row["theme_strength"] = round(
            average * 5.0 + leader * 2.0 + min(active, 12.0) * 1.5 + min(positive / total, 1.0) * 12.0,
            2,
        )
        if row.get("theme_source") == "name_keyword":
            row["theme_strength"] = round(row["theme_strength"] + 15.0, 2)
        theme_by_code[str(row.get("concept_code") or "")] = row

    database_theme_codes = [
        concept_code for concept_code in theme_by_code
        if not concept_code.startswith("NAME:")
    ]
    if not database_theme_codes:
        member_rows: list[dict[str, Any]] = []
    else:
        params: dict[str, Any] = {}
        placeholders: list[str] = []
        for index, concept_code in enumerate(database_theme_codes):
            key = f"theme_{index}"
            params[key] = concept_code
            placeholders.append(f":{key}")
        member_rows = _engine_rows(
            f"""
            SELECT stock_code, theme_code AS concept_code, theme_name AS name,
                   theme_source
            FROM (
                SELECT stock_code, CONCAT('CONCEPT:', concept_code) AS theme_code,
                       name AS theme_name, 'concept' AS theme_source
                FROM si_stock_concept_east
            ) membership
            WHERE theme_code IN ({','.join(placeholders)})
            """,
            params,
            context="screener_intraday_theme_members",
        )
    member_rows.extend(name_theme_members)

    themes_for_stock: dict[str, list[dict[str, Any]]] = {}
    for member in member_rows:
        theme = theme_by_code.get(str(member.get("concept_code") or ""))
        if theme:
            themes_for_stock.setdefault(str(member.get("stock_code") or "").zfill(6), []).append(theme)

    raw: list[dict[str, Any]] = []
    for quote in live_rows:
        code = str(quote.get("stock_code") or "").zfill(6)
        themes = sorted(
            themes_for_stock.get(code) or [],
            key=lambda item: (
                0 if item.get("theme_source") == "name_keyword" else 1,
                -float(item.get("theme_strength") or 0),
                str(item.get("concept_code") or ""),
            ),
        )
        if not themes:
            continue
        best = themes[0]
        change = _number(quote.get("change_pct"), 0.0) or 0.0
        base_score = max(0.0, min(100.0, 42.0 + change * 3.0 + float(best.get("theme_strength") or 0) * 0.35))
        raw.append({
            **quote,
            "stock_code": code,
            "score": round(base_score, 2),
            "final_trade_score": round(base_score, 2),
            "data_date": target_date,
            "intraday_observed_at": observed_at,
            "primary_concept": best.get("concept_name") or best.get("concept_code"),
            "concept_name": best.get("concept_name") or best.get("concept_code"),
            "theme_name": best.get("concept_name") or best.get("concept_code"),
            "intraday_theme_strength": best.get("theme_strength"),
            "intraday_theme_source": best.get("theme_source") or "concept",
            "intraday_theme_active_members": int(best.get("active_members") or 0),
            "intraday_theme_positive_breadth": best.get("positive_breadth"),
            "intraday_theme_names": [str(item.get("concept_name") or item.get("concept_code")) for item in themes[:5]],
            "industry_evidence_status": "DATA_BLOCKED",
            "industry_evidence_reason": (
                "PIT_EXACT_DATE_OR_VALID_INTERVAL_INDUSTRY_REQUIRED"
            ),
            "legacy_theme_evidence_status": "LEGACY_UNVERIFIED",
            "strategy_theme_eligible": False,
            "funding_eligible": False,
            "order_authority": False,
            "matched_conditions": [
                f"盘中涨幅{change:.2f}%",
                f"{best.get('concept_name') or best.get('concept_code')}联动{int(best.get('active_members') or 0)}只",
                f"板块均涨{float(best.get('average_change_pct') or 0):.2f}%",
            ],
        })
    raw.sort(key=lambda row: (-float(row.get("score") or 0), str(row.get("stock_code") or "")))
    for discovery_rank, row in enumerate(raw, 1):
        row["intraday_discovery_rank"] = discovery_rank
    # This endpoint is a live discovery list.  V4/V5/V6 still annotate every
    # row with gates and buy readiness, but cannot hide an already discovered
    # sector leader merely because its previous-close evidence ranks lower.
    shortlist = _intraday_quota_shortlist(raw, request.top)
    enriched = _enrich_selector_evidence(
        shortlist,
        target_date,
        decision_at=observed_at,
    )
    enriched = _attach_correlation_clusters(enriched, target_date)
    normalized, stats = _apply_filters(
        enriched,
        request,
        listed_codes=_listed_codes(target_date),
    )
    normalized.sort(
        key=lambda row: (
            int(row.get("intraday_discovery_rank") or 10**9),
            str(row.get("stock_code") or ""),
        )
    )
    for rank, row in enumerate(normalized, 1):
        row["selector_rank"] = row.get("rank")
        row["rank"] = rank
        row["intraday_rank"] = rank
        row["industry_evidence_status"] = "DATA_BLOCKED"
        row["industry_evidence_reason"] = (
            "PIT_EXACT_DATE_OR_VALID_INTERVAL_INDUSTRY_REQUIRED"
        )
        row["legacy_theme_evidence_status"] = "LEGACY_UNVERIFIED"
        row["strategy_theme_eligible"] = False
        row["funding_eligible"] = False
        row["order_authority"] = False
    return {
        "status": "ok",
        "preset": preset,
        "requested_date": _clean_date(request.as_of_date) or date.today().isoformat(),
        "data_date": quote_date,
        "evidence_date": target_date,
        "observed_at": observed_at,
        "freshness": freshness,
        "review_only": freshness != "live",
        "source": "sm_stock_current+si_stock_concept_east+previous_close_evidence",
        "decision_scope": "PRODUCTION_SELECTION_ADVISORY",
        "actionable_output_allowed": False,
        "industry_evidence_status": "DATA_BLOCKED",
        "industry_evidence_reason": (
            "PIT_EXACT_DATE_OR_VALID_INTERVAL_INDUSTRY_REQUIRED"
        ),
        "legacy_theme_evidence_status": "LEGACY_UNVERIFIED",
        "funding_eligible": False,
        "order_authority": False,
        "selector": selector_contract(),
        "stats": {
            **stats,
            "live_quote_count": len(live_rows),
            "strong_theme_count": len(theme_by_code),
            "selector_summary": selector_run_summary(normalized),
        },
        "data": normalized,
        "total": len(normalized),
        "error": "",
    }


@router.get("/screener/catalog")
def screener_catalog():
    return {
        "status": "ok",
        "presets": list(PRESETS),
        "versions": list(VERSION_MATRIX),
        "execution_boundary": {
            "production_ranking_active": True,
            "research_models_order_blocked": True,
            "paper_orders_allowed": False,
            "real_orders_allowed": False,
        },
        "selector": selector_contract(),
        "universes": [
            {"key": "market", "name": "全市场"},
            {"key": "portfolio", "name": "我的自选"},
            {"key": "concept", "name": "概念成分"},
        ],
        "filters": [
            {"key": "min_change", "name": "最低涨幅", "type": "number"},
            {"key": "max_change", "name": "最高涨幅", "type": "number"},
            {"key": "min_turnover", "name": "最低换手", "type": "number"},
            {"key": "min_amount", "name": "最低成交额", "type": "number"},
            {"key": "min_flow", "name": "最低主力净流入", "type": "number"},
            {"key": "min_score", "name": "最低综合分", "type": "number"},
            {"key": "keyword", "name": "代码/名称搜索", "type": "text"},
            {"key": "exclude_st", "name": "排除 ST", "type": "boolean"},
        ],
    }


@router.get("/screener/status")
def screener_status():
    result = _runtime_status()
    result["versions"] = list(VERSION_MATRIX)
    result["selector"] = selector_contract()
    return result


def execute_screener_task(request: ScreenerRunRequest) -> dict[str, Any]:
    """Generate, persist and optionally deliver from a scheduler task only."""

    requested = _clean_date(request.as_of_date)
    target = requested or _latest_date("sm_stock_kline") or date.today().isoformat()
    result = _run_preset(request, target)
    result["data_gate"] = _runtime_status()
    result["versions"] = list(VERSION_MATRIX)
    result["selector"] = selector_contract()
    result["view_mode"] = "latest"
    run_meta = {
        "run_type": "scheduler_task",
        "run_id": f"screen-{target}-{request.preset}",
        "generated_at": datetime.now().replace(microsecond=0).isoformat(sep=" "),
        "model_fingerprint": result["selector"]["model_fingerprint"],
        "rollback_target": result["selector"]["rollback"]["target"],
    }
    try:
        persisted = _persist_screener_run(request, result)
        run_meta.update(persisted)
    except Exception as exc:  # A database ledger failure must be visible without hiding the ranking.
        logger.exception("Unable to persist screener run: %s", exc)
        run_meta.update({
            "persisted": False,
            "is_new": False,
            "persistence_error": "SCREENER_PERSISTENCE_FAILED",
            "persistence_error_type": type(exc).__name__,
        })
    result["run"] = run_meta
    if request.notify:
        if not run_meta.get("persisted"):
            result["notification"] = {"status": "error", "error": "候选榜未落库，禁止发送无追溯结果"}
        elif not run_meta.get("is_new") and str(run_meta.get("push_status") or "").upper() == "SENT":
            result["notification"] = {"status": "skipped", "reason": "same_snapshot_already_sent"}
        else:
            from biz.analysis.trading_wecom import notify_screener_result

            result["notification"] = notify_screener_result(result)
            try:
                _update_screener_push(str(run_meta.get("run_uid") or ""), result["notification"])
            except Exception as exc:  # The result remains queryable even if push status bookkeeping fails.
                logger.warning("Unable to update screener push status: %s", exc)
    logger.info(
        "production_screener_run %s",
        json.dumps(
            {
                "run": result["run"],
                "requested_date": result.get("requested_date"),
                "data_date": result.get("data_date"),
                "freshness": result.get("freshness"),
                "preset": request.preset,
                "summary": (result.get("stats") or {}).get("selector_summary"),
            },
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        ),
    )
    return result


@router.post("/screener/run")
def screener_run(request: ScreenerRunRequest):
    """Queue one strict registered task; never screen or deliver in the API."""

    token = _encode_screener_task_request(request)
    task_type = _screener_task_type(request.preset)
    contract = SCREENER_TASKS_BY_TYPE.get(task_type)
    if not contract or str(contract.get("script_path") or "") != _SCREENER_SCRIPT_PATH:
        return {
            "accepted": False,
            "queued": False,
            "status": "task_contract_unavailable",
            "task_type": task_type,
            "job_id": "",
            "error": "生产筛选任务合同不可用，已拒绝执行",
        }
    result = launch_registered_scheduler_task(
        get_engine(),
        task_type=task_type,
        expected_script_path=_SCREENER_SCRIPT_PATH,
        script_args=f"--request-token {token} --json",
        root=_ROOT,
    )
    return {
        **result,
        "queued": bool(result.get("accepted")),
        "preset": request.preset,
    }


@router.get("/screener/history")
def screener_history(
    data_date: str = Query(default=""),
    preset: str = Query(default="intraday_sector"),
    q: str = Query(default="", max_length=120),
    run_uid: str = Query(default="", max_length=32),
    limit: int = Query(default=200, ge=1, le=500),
):
    """Read a persisted ranking; historical dates are never recomputed with today's data."""
    try:
        _ensure_tables()
        target = _clean_date(data_date)
        params: dict[str, Any] = {"preset": preset, "data_date": target or None, "run_uid": run_uid}
        runs = _engine_rows(
            """
            SELECT run_uid, preset, requested_date, session_date, data_date, evidence_date, observed_at,
                   generated_at, freshness, status, source, result_count, summary_json,
                   selector_json, push_status, pushed_at
            FROM st_screener_run_history
            WHERE (:preset = '' OR preset = :preset)
              AND (:data_date IS NULL OR session_date = :data_date)
              AND (:run_uid = '' OR run_uid = :run_uid)
            ORDER BY COALESCE(observed_at, generated_at) DESC, id DESC
            LIMIT 50
            """,
            params,
            context="screener_history_runs",
        )
        if not runs:
            return {"status": "not_found", "data": [], "total": 0, "available_runs": []}
        selected = runs[0]
        selected_uid = str(selected.get("run_uid") or "")
        keyword = str(q or "").strip()
        result_rows = _engine_rows(
            """
            SELECT rank_no, selector_rank, stock_code, stock_name, payload_json
            FROM st_screener_run_result
            WHERE run_uid = :run_uid
              AND (:keyword = '' OR stock_code LIKE :pattern OR stock_name LIKE :pattern)
            ORDER BY rank_no ASC
            LIMIT :limit
            """,
            {"run_uid": selected_uid, "keyword": keyword, "pattern": f"%{keyword}%", "limit": limit},
            context="screener_history_results",
        )
        data: list[dict[str, Any]] = []
        for row in result_rows:
            try:
                payload = json.loads(row.get("payload_json") or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                payload = {}
            payload.setdefault("rank", row.get("rank_no"))
            payload.setdefault("selector_rank", row.get("selector_rank"))
            payload.setdefault("stock_code", row.get("stock_code"))
            payload.setdefault("stock_name", row.get("stock_name"))
            data.append(payload)
        try:
            stats = json.loads(selected.get("summary_json") or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            stats = {}
        try:
            selector = json.loads(selected.get("selector_json") or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            selector = {}
        available = [
            {
                "run_uid": row.get("run_uid"),
                "data_date": row.get("data_date"),
                "session_date": row.get("session_date"),
                "observed_at": row.get("observed_at"),
                "generated_at": row.get("generated_at"),
                "freshness": row.get("freshness"),
                "result_count": row.get("result_count"),
                "push_status": row.get("push_status"),
            }
            for row in runs
        ]
        return {
            "status": selected.get("status") or "ok",
            "preset": selected.get("preset"),
            "requested_date": selected.get("requested_date"),
            "session_date": selected.get("session_date"),
            "data_date": selected.get("data_date"),
            "evidence_date": selected.get("evidence_date"),
            "observed_at": selected.get("observed_at"),
            "generated_at": selected.get("generated_at"),
            "freshness": selected.get("freshness"),
            "source": selected.get("source"),
            "stats": stats,
            "selector": selector,
            "run": {"run_uid": selected_uid, "persisted": True, "push_status": selected.get("push_status"), "pushed_at": selected.get("pushed_at")},
            "view_mode": "historical",
            "data": data,
            "total": len(data),
            "available_runs": available,
        }
    except Exception as exc:
        logger.exception("Unable to read screener history: %s", exc)
        return {
            "status": "error",
            "data": [],
            "total": 0,
            "error": "SCREENER_HISTORY_UNAVAILABLE",
            "available_runs": [],
        }


@router.get("/screener/saved")
def screener_saved():
    try:
        _ensure_tables()
        rows = _engine_rows(
            "SELECT id, name, definition_json, created_at, updated_at FROM st_screener_saved ORDER BY updated_at DESC",
            context="screener_saved_list",
        )
        for row in rows:
            try:
                row["definition"] = json.loads(row.pop("definition_json") or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                row["definition"] = {}
        return {"status": "ok", "data": rows, "total": len(rows)}
    except Exception as exc:
        logger.warning("Unable to list saved screens: %s", exc)
        return {
            "status": "degraded",
            "data": [],
            "total": 0,
            "error": "SCREENER_SAVED_LIST_UNAVAILABLE",
        }


@router.post("/screener/saved")
def screener_saved_create(request: SavedScreenRequest):
    _ensure_tables()
    definition_json = json.dumps(request.definition or {}, ensure_ascii=False, separators=(",", ":"))
    with get_engine().begin() as conn:
        conn.execute(text("""
            INSERT INTO st_screener_saved (name, definition_json)
            VALUES (:name, :definition_json)
            ON DUPLICATE KEY UPDATE definition_json = VALUES(definition_json), updated_at = CURRENT_TIMESTAMP
        """), {"name": request.name.strip(), "definition_json": definition_json})
    return {"status": "ok", "name": request.name.strip()}


@router.delete("/screener/saved/{screen_id}")
def screener_saved_delete(screen_id: int):
    _ensure_tables()
    with get_engine().begin() as conn:
        result = conn.execute(text("DELETE FROM st_screener_saved WHERE id = :id"), {"id": screen_id})
    return {"status": "ok", "deleted": int(result.rowcount or 0)}


@router.get("/screener/candidates")
def screener_candidates(status: str = Query(default="ACTIVE"), limit: int = Query(default=100, ge=1, le=500)):
    try:
        _ensure_tables()
        rows = _engine_rows(
            """
            SELECT id, stock_code, stock_name, source, screen_name, score, as_of_date, status, reason, payload_json, created_at, updated_at
            FROM st_screener_candidate_pool
            WHERE (:status = '' OR status = :status)
            ORDER BY updated_at DESC, score DESC
            LIMIT :limit
            """,
            {"status": str(status or ""), "limit": limit},
            context="screener_candidates_list",
        )
        for row in rows:
            try:
                row["payload"] = json.loads(row.pop("payload_json") or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                row["payload"] = {}
            row["decision_scope"] = "RESEARCH_ONLY"
            row["action"] = "WATCH"
            row["actionable"] = False
        return {"status": "ok", "data": rows, "total": len(rows)}
    except Exception:
        return {
            "status": "degraded",
            "data": [],
            "total": 0,
            "error": "SCREENER_CANDIDATE_LIST_UNAVAILABLE",
        }


@router.post("/screener/candidates")
def screener_candidate_save(request: CandidateSaveRequest):
    _ensure_tables()
    code = str(request.stock_code or "").strip().zfill(6)
    as_of = _clean_date(request.as_of_date) or None
    payload = dict(request.payload or {})
    payload.update({
        "decision_scope": "RESEARCH_ONLY",
        "action": "WATCH",
        "actionable": False,
    })
    with get_engine().begin() as conn:
        conn.execute(text("""
            INSERT INTO st_screener_candidate_pool
              (stock_code, stock_name, source, screen_name, score, as_of_date, status, reason, payload_json)
            VALUES (:stock_code, :stock_name, :source, :screen_name, :score, :as_of_date, 'ACTIVE', :reason, :payload_json)
            ON DUPLICATE KEY UPDATE
              stock_name = VALUES(stock_name), screen_name = VALUES(screen_name), score = VALUES(score),
              status = 'ACTIVE', reason = VALUES(reason), payload_json = VALUES(payload_json), updated_at = CURRENT_TIMESTAMP
        """), {
            "stock_code": code,
            "stock_name": request.stock_name,
            "source": request.source,
            "screen_name": request.screen_name,
            "score": request.score,
            "as_of_date": as_of,
            "reason": request.reason,
            "payload_json": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        })
    return {
        "status": "ok",
        "stock_code": code,
        "as_of_date": as_of,
        "decision_scope": "RESEARCH_ONLY",
        "actionable": False,
    }


@router.delete("/screener/candidates/{stock_code}")
def screener_candidate_remove(stock_code: str):
    _ensure_tables()
    code = str(stock_code or "").strip().zfill(6)
    with get_engine().begin() as conn:
        result = conn.execute(
            text("UPDATE st_screener_candidate_pool SET status = 'REMOVED', updated_at = CURRENT_TIMESTAMP WHERE stock_code = :code AND status = 'ACTIVE'"),
            {"code": code},
        )
    return {"status": "ok", "stock_code": code, "removed": int(result.rowcount or 0)}


@router.get("/screener/candidate-center")
def screener_candidate_center(trade_date: str = Query(default=""), limit: int = Query(default=100, ge=1, le=300)):
    """One read model for AI recommendations, strategy labels and external picks."""
    target = _clean_date(trade_date) or _latest_date("st_recommended_stocks", column="pick_date") or date.today().isoformat()
    try:
        from server.api.routers.hot_data import recommended_stocks

        # This is a direct Python call rather than a FastAPI-dispatched call.
        # Pass every Query-backed argument explicitly so FastAPI ``Query``
        # objects never leak into the recommendation implementation.
        rec = recommended_stocks(
            trade_date=target,
            strategy="",
            signal_status="",
            start_date="",
            end_date="",
            prefer_latest=True,
        )
    except Exception as exc:
        logger.warning("candidate center recommendation source failed: %s", exc)
        rec = {"data": [], "error": "RECOMMENDATION_SOURCE_UNAVAILABLE"}
    candidates: list[dict[str, Any]] = []
    for row in (rec.get("data") or []):
        item = dict(row)
        item["stock_code"] = str(item.get("stock_code") or "").zfill(6)
        item["stock_name"] = item.get("stock_name") or item.get("short_name") or item["stock_code"]
        item["score"] = _number(item.get("final_trade_score", item.get("ai_score")), 0) or 0
        item["source"] = "AI推荐"
        item["source_signal_status"] = str(item.get("signal_status") or "")
        item["source_recommend_status"] = str(item.get("recommend_status") or "")
        action, new_buy_eligible, action_reason = _candidate_new_buy_action(item)
        item["action"] = action
        item["new_buy_eligible"] = new_buy_eligible
        item["action_reason"] = action_reason
        item["decision_scope"] = "PRODUCTION_SELECTION_ADVISORY"
        item["actionable"] = False
        candidates.append(item)
    candidates = _enrich_selector_evidence(
        candidates,
        target,
        decision_at=(
            datetime.now().replace(microsecond=0) if not trade_date else None
        ),
    )
    candidates = _attach_correlation_clusters(candidates, target)
    candidates = rank_production_candidates(candidates)[:limit]
    strategy_counts: dict[str, int] = {}
    for row in candidates:
        key = str(row.get("primary_strategy") or row.get("strategy_profile") or "综合")
        strategy_counts[key] = strategy_counts.get(key, 0) + 1
    jq = []
    try:
        jq = _engine_rows(
            """
            SELECT strategy_name, MAX(pick_date) AS latest_date, COUNT(*) AS rows_count
            FROM jq_strategy_picks GROUP BY strategy_name ORDER BY latest_date DESC
            """,
            context="candidate_center_jq",
        )
    except Exception as exc:
        logger.debug("candidate center JQ source unavailable: %s", exc)
    latest_jq = max((_clean_date(row.get("latest_date")) for row in jq), default="")
    return {
        "status": "ok" if candidates else "degraded",
        "requested_date": _clean_date(trade_date) or target,
        "data_date": _clean_date(rec.get("date")) or target,
        "freshness": rec.get("freshness") or {},
        "summary": {
            **selector_run_summary(candidates),
            "candidate_count": len(candidates),
            "buy_ready_count": sum(1 for row in candidates if row.get("new_buy_eligible") is True),
            "watch_count": sum(1 for row in candidates if "WATCH" in str(row.get("action")).upper()),
            "risk_count": sum(1 for row in candidates if any(x in str(row.get("action")).upper() for x in ("BLOCK", "SELL", "RISK"))),
            "strategy_counts": strategy_counts,
        },
        "candidates": candidates,
        "decision_scope": "PRODUCTION_SELECTION_ADVISORY",
        "actionable_output_allowed": False,
        "selector": selector_contract(),
        "sources": {
            "ai_recommendation": {"count": len(candidates), "date": _clean_date(rec.get("date")) or target, "error": rec.get("error", "")},
            "jq": {"strategies": jq, "latest_date": latest_jq, "stale": bool(latest_jq and latest_jq < target)},
        },
    }
