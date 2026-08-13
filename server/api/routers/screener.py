# -*- coding: utf-8 -*-
"""Unified stock screener and candidate-pool API.

The legacy ``/api/hot-data/screen-stocks`` endpoint remains available for
compatibility, while this router provides one stable contract for the new
workbench: presets, composable filters, saved screens and a research-only
candidate pool.  It deliberately never places orders.
"""
from __future__ import annotations

import json
import hashlib
import logging
import math
import re
import uuid
from datetime import date, datetime, time, timedelta
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.common.sql_reader import read_sql_rows
from server.engine.production_selector import (
    board_limit_trigger_pct,
    rank_production_candidates,
    selector_contract,
    selector_run_summary,
)

logger = logging.getLogger(__name__)
router = APIRouter()


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
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS st_screener_saved (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(120) NOT NULL,
                definition_json LONGTEXT NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_st_screener_saved_name (name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS st_screener_candidate_pool (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                stock_code VARCHAR(12) NOT NULL,
                stock_name VARCHAR(80) NOT NULL DEFAULT '',
                source VARCHAR(40) NOT NULL DEFAULT 'screener',
                screen_name VARCHAR(120) NOT NULL DEFAULT '',
                score DECIMAL(10,2) NULL,
                as_of_date DATE NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
                reason TEXT NULL,
                payload_json LONGTEXT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_st_screener_candidate (stock_code, source, as_of_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS st_screener_run_history (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                run_uid CHAR(32) NOT NULL,
                run_key CHAR(64) NOT NULL,
                preset VARCHAR(64) NOT NULL,
                requested_date DATE NULL,
                session_date DATE NULL,
                data_date DATE NULL,
                evidence_date DATE NULL,
                observed_at DATETIME NULL,
                generated_at DATETIME NOT NULL,
                freshness VARCHAR(32) NOT NULL DEFAULT '',
                status VARCHAR(32) NOT NULL DEFAULT '',
                source VARCHAR(255) NOT NULL DEFAULT '',
                universe VARCHAR(32) NOT NULL DEFAULT 'market',
                concept_code VARCHAR(32) NOT NULL DEFAULT '',
                result_count INT NOT NULL DEFAULT 0,
                request_json LONGTEXT NOT NULL,
                summary_json LONGTEXT NULL,
                selector_json LONGTEXT NULL,
                push_status VARCHAR(32) NOT NULL DEFAULT 'NOT_REQUESTED',
                push_error VARCHAR(500) NULL,
                pushed_at DATETIME NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_st_screener_run_uid (run_uid),
                UNIQUE KEY uq_st_screener_run_key (run_key),
                KEY idx_st_screener_run_date (session_date, preset, generated_at),
                KEY idx_st_screener_data_date (data_date, preset)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS st_screener_run_result (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                run_uid CHAR(32) NOT NULL,
                rank_no INT NOT NULL,
                selector_rank INT NULL,
                stock_code VARCHAR(12) NOT NULL,
                stock_name VARCHAR(120) NOT NULL DEFAULT '',
                score DECIMAL(12,4) NULL,
                ensemble_score DECIMAL(12,4) NULL,
                candidate_grade VARCHAR(20) NOT NULL DEFAULT '',
                action_status VARCHAR(40) NOT NULL DEFAULT '',
                primary_concept VARCHAR(120) NOT NULL DEFAULT '',
                change_pct DECIMAL(12,4) NULL,
                price DECIMAL(18,4) NULL,
                payload_json LONGTEXT NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_st_screener_result_rank (run_uid, rank_no),
                UNIQUE KEY uq_st_screener_result_stock (run_uid, stock_code),
                KEY idx_st_screener_result_lookup (stock_code, run_uid)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))


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
            text("SELECT run_uid, generated_at, push_status FROM st_screener_run_history WHERE run_key = :run_key LIMIT 1"),
            {"run_key": run_key},
        ).mappings().first()
        if existing:
            return {
                "run_uid": str(existing["run_uid"]),
                "run_key": run_key,
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


def _enrich_selector_evidence(rows: list[dict], target_date: str) -> list[dict]:
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
                   ordinary_buy_eligible, chase_risk_status
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

        finance_rows = _safe_selector_rows(
            f"""
            SELECT f.stock_code, f.report_date AS finance_report_date,
                   f.notice_date AS finance_notice_date,
                   f.etl_sync_at AS finance_knowledge_at,
                   f.net_asset_ps, f.oper_cf_ps, f.total_rev_yoy_gr,
                   f.net_profit_yoy_gr, f.roe_wtd, f.gross_margin,
                   f.net_margin, f.cash_flow_ratio, f.asset_liab_ratio
            FROM si_stock_finance f
            JOIN (
                SELECT stock_code, MAX(report_date) AS report_date
                FROM si_stock_finance
                WHERE stock_code IN ({code_sql})
                  AND report_date <= :target_date
                  AND notice_date <= :target_date
                  AND notice_date >= report_date
                  AND etl_sync_at IS NOT NULL
                  AND etl_sync_at < DATE_ADD(:target_date, INTERVAL 1 DAY)
                GROUP BY stock_code
            ) latest
              ON latest.stock_code = f.stock_code
             AND latest.report_date = f.report_date
            WHERE f.stock_code IN ({code_sql})
              AND f.report_date <= :target_date
              AND f.notice_date <= :target_date
              AND f.notice_date >= f.report_date
              AND f.etl_sync_at IS NOT NULL
              AND f.etl_sync_at < DATE_ADD(:target_date, INTERVAL 1 DAY)
            ORDER BY f.stock_code, f.notice_date DESC, f.etl_sync_at DESC, f.id DESC
            """,
            params,
            context="screener_selector_pit_finance",
        )
        finance_seen: set[str] = set()
        for evidence in finance_rows:
            code = str(evidence.get("stock_code") or "").zfill(6)
            if code in finance_seen:
                continue
            finance_seen.add(code)
            payload = {
                key: value
                for key, value in evidence.items()
                if key != "stock_code" and value is not None
            }
            payload.update(
                {
                    "finance_pit_verified": True,
                    "finance_source": "si_stock_finance",
                }
            )
            evidence_by_code.setdefault(code, {}).update(payload)
    except Exception as exc:
        logger.warning("selector evidence enrichment failed for %s: %s", target_date, exc)
        return [dict(row) for row in rows]

    enriched: list[dict] = []
    for row in rows:
        code = str(row.get("stock_code") or "").zfill(6)
        item = dict(evidence_by_code.get(code) or {})
        item.update(row)
        item["stock_code"] = code
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
    rows = _enrich_selector_evidence(result.get("data") or [], data_date or target_date)
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
            UNION
            SELECT stock_code, CONCAT('INDUSTRY:', sw_code) AS theme_code,
                   industry_name AS theme_name, 'industry' AS theme_source
            FROM si_industry_sw
            WHERE industry_name IS NOT NULL AND industry_name <> ''
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
                UNION
                SELECT stock_code, CONCAT('INDUSTRY:', sw_code) AS theme_code,
                       industry_name AS theme_name, 'industry' AS theme_source
                FROM si_industry_sw
                WHERE industry_name IS NOT NULL AND industry_name <> ''
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
    enriched = _enrich_selector_evidence(shortlist, target_date)
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


@router.post("/screener/run")
def screener_run(request: ScreenerRunRequest):
    requested = _clean_date(request.as_of_date)
    target = requested or _latest_date("sm_stock_kline") or date.today().isoformat()
    result = _run_preset(request, target)
    result["data_gate"] = _runtime_status()
    result["versions"] = list(VERSION_MATRIX)
    result["selector"] = selector_contract()
    run_meta = {
        "run_type": "sync",
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
        run_meta.update({"persisted": False, "is_new": False, "persistence_error": str(exc)[:300]})
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
            "data": data,
            "total": len(data),
            "available_runs": available,
        }
    except Exception as exc:
        logger.exception("Unable to read screener history: %s", exc)
        return {"status": "error", "data": [], "total": 0, "error": str(exc)[:300], "available_runs": []}


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
        return {"status": "degraded", "data": [], "total": 0, "error": str(exc)[:300]}


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
    except Exception as exc:
        return {"status": "degraded", "data": [], "total": 0, "error": str(exc)[:300]}


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
        rec = {"data": [], "error": str(exc)}
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
    candidates = _enrich_selector_evidence(candidates, target)
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
