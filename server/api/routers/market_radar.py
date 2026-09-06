# -*- coding: utf-8 -*-
"""API endpoints for the QMT intraday anomaly radar."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text

from biz.market_radar.core import (
    EVENT_TABLE,
    SECTOR_TABLE,
    STOCK_TABLE,
    annotate_radar_relations,
    build_radar_relation_index,
    ensure_radar_tables,
    get_shared_radar_engine,
)
from server.api.routers._engine import get_engine
from server.common.authoritative_market_clock import (
    PRODUCTION_TIMEZONE,
    authoritative_closed_trade_date,
)
from server.common.canonical_decision_bridge import canonical_governance_decision

router = APIRouter(prefix="/market-radar", tags=["market-radar"])
RADAR_SNAPSHOT_FRESH_SECONDS = 180
RADAR_EVENT_FRESH_SECONDS = 15 * 60


def _radar_now() -> datetime:
    return datetime.now(PRODUCTION_TIMEZONE)


def _radar_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip().replace("Z", "+00:00")
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace(" ", "T", 1))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=PRODUCTION_TIMEZONE)
    return parsed.astimezone(PRODUCTION_TIMEZONE)


def _radar_authoritative_trade_date(engine: Any, now: datetime) -> str:
    # Probe after the daily close-ready time so the exchange calendar can say
    # whether today is an actual trading day, including weekday holidays.
    probe = now.astimezone(PRODUCTION_TIMEZONE).replace(
        hour=23, minute=59, second=59, microsecond=0
    )
    try:
        return str(authoritative_closed_trade_date(engine, now=probe) or "")[:10]
    except TypeError:
        # Compatibility for simple one-argument test doubles.
        try:
            return str(authoritative_closed_trade_date(engine) or "")[:10]
        except Exception:
            return ""
    except Exception:
        return ""


def _apply_radar_freshness(
    engine: Any,
    rows: list[dict[str, Any]],
    *,
    event: bool = False,
) -> dict[str, Any]:
    """Recompute read-time freshness instead of trusting persisted flags."""

    now = _radar_now()
    expected_date = _radar_authoritative_trade_date(engine, now)
    max_age = RADAR_EVENT_FRESH_SECONDS if event else RADAR_SNAPSHOT_FRESH_SECONDS
    cutoffs: list[datetime] = []
    fresh_count = 0
    for row in rows:
        snapshot = _radar_datetime(row.get("snapshot_at"))
        if snapshot is not None:
            cutoffs.append(snapshot)
            age_seconds = (now - snapshot).total_seconds()
            snapshot_date = snapshot.date().isoformat()
        else:
            age_seconds = None
            snapshot_date = ""
        fresh = bool(
            expected_date
            and snapshot_date == expected_date
            and age_seconds is not None
            and 0 <= age_seconds <= max_age
        )
        if fresh:
            fresh_count += 1
        row["stale"] = not fresh
        row["freshness_status"] = (
            "fresh"
            if fresh
            else "unavailable"
            if not expected_date or snapshot is None
            else "stale"
        )
        row["age_seconds"] = round(age_seconds, 1) if age_seconds is not None else None
        row["expected_trade_date"] = expected_date or None
        row["freshness_reason"] = (
            None
            if fresh
            else "RADAR_TRADE_DATE_AUTHORITY_UNAVAILABLE"
            if not expected_date
            else "RADAR_TRADE_DATE_MISMATCH"
            if snapshot_date and expected_date and snapshot_date != expected_date
            else "RADAR_SNAPSHOT_EXPIRED"
            if age_seconds is not None and age_seconds > max_age
            else "RADAR_SNAPSHOT_TIME_INVALID"
        )

    row_count = len(rows)
    missing_count = row_count - len(cutoffs)
    if not expected_date or not row_count or not cutoffs:
        data_status = "unavailable"
    elif missing_count:
        data_status = "partial"
    elif fresh_count == row_count:
        data_status = "fresh"
    elif fresh_count:
        data_status = "partial"
    else:
        data_status = "stale"
    cutoff = max(cutoffs).strftime("%Y-%m-%d %H:%M:%S") if cutoffs else None
    return {
        "data_status": data_status,
        "data_cutoff": cutoff,
        "actual_cutoff": cutoff,
        "expected_trade_date": expected_date or None,
        "fresh_rows": fresh_count,
        "stale_rows": row_count - fresh_count,
        "freshness_threshold_seconds": max_age,
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
    }


def _read_rows(engine, sql: str, params: dict[str, Any] | None = None, **_: Any) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        return [dict(row) for row in result.mappings().all()]


def _json_column(rows: list[dict[str, Any]], *columns: str) -> list[dict[str, Any]]:
    for row in rows:
        for column in columns:
            value = row.get(column)
            if not value:
                row[column] = [] if column.endswith("json") else None
                continue
            try:
                row[column] = json.loads(value)
            except (TypeError, ValueError):
                pass
    return rows


def _load_relation_index(engine) -> dict[str, Any]:
    portfolio_rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    portfolio_status = "available"
    candidate_status = "available"
    candidate_date = ""
    candidate_run_uid = ""
    try:
        portfolio_rows = _read_rows(
            engine,
            "SELECT stock_code, shares FROM st_user_portfolio",
            context="market_radar_portfolio_relations",
        )
    except Exception:
        portfolio_status = "unavailable"
    try:
        canonical = canonical_governance_decision()
        context = canonical.get("context") if isinstance(canonical, dict) else None
        pool = canonical.get("pool") if isinstance(canonical, dict) else None
        if not (
            isinstance(context, dict)
            and context.get("decision_integrity_verified") is True
            and context.get("run_status") == "COMPLETED"
            and context.get("run_uid")
            and isinstance(pool, dict)
            and pool.get("pool_readable") is True
            and isinstance(pool.get("items"), list)
        ):
            raise ValueError("canonical governance pool unavailable")
        candidate_date = str(context.get("decision_date") or "")[:10]
        candidate_run_uid = str(context.get("run_uid") or "")
        authoritative_date = authoritative_closed_trade_date(engine)
        if not authoritative_date:
            candidate_status = "unavailable"
        elif candidate_date != authoritative_date:
            candidate_status = "stale"
        else:
            candidate_rows = [
                {
                    "stock_code": item.get("stock_code"),
                    "primary_strategy": item.get("primary_strategy_key")
                    or (item.get("strategy_keys") or [""])[0],
                }
                for item in pool["items"]
                if isinstance(item, dict) and item.get("is_strategy_candidate") is True
            ]
    except Exception:
        candidate_status = "unavailable"
    return build_radar_relation_index(
        portfolio_rows,
        candidate_rows,
        portfolio_status=portfolio_status,
        candidate_status=candidate_status,
        candidate_date=candidate_date,
        candidate_run_uid=candidate_run_uid,
    )


def _public_relation_context(index: dict[str, Any]) -> dict[str, Any]:
    members = index.get("members") or {}
    return {
        "portfolio_status": index.get("portfolio_status"),
        "candidate_status": index.get("candidate_status"),
        "candidate_date": index.get("candidate_date"),
        "candidate_run_uid": index.get("candidate_run_uid"),
        "sources": index.get("sources"),
        "watchlist_count": sum(bool(item.get("watchlist")) for item in members.values()),
        "holding_count": sum(bool(item.get("holding")) for item in members.values()),
        "strategy_candidate_count": sum(bool(item.get("strategy_candidate")) for item in members.values()),
    }


def _scope_available(scope: str, index: dict[str, Any]) -> bool:
    if scope in {"watchlist", "holding"}:
        return index.get("portfolio_status") == "available"
    if scope == "strategy_candidate":
        return index.get("candidate_status") == "available"
    return True


def _attach_sector_relation_members(
    engine: Any,
    rows: list[dict[str, Any]],
    relation_index: dict[str, Any],
) -> str:
    """Attach ordinary concept members needed for display-only relations."""

    codes = sorted((relation_index.get("members") or {}).keys())
    if not rows or not codes:
        return "not_needed"
    placeholders = ",".join(f":relation_code_{index}" for index in range(len(codes)))
    params = {f"relation_code_{index}": code for index, code in enumerate(codes)}
    try:
        membership_rows = _read_rows(
            engine,
            f"""
            SELECT concept_code, stock_code
            FROM si_concept_constituent_east
            WHERE stock_code IN ({placeholders})
              AND concept_code IS NOT NULL AND concept_code <> ''
            """,
            params,
            context="market_radar_sector_relation_members",
        )
    except Exception:
        return "unavailable"
    members_by_sector: dict[str, set[str]] = {}
    for item in membership_rows:
        concept_code = str(item.get("concept_code") or "").strip()
        stock_code = str(item.get("stock_code") or "").split(".")[0].zfill(6)
        if concept_code and stock_code in codes:
            members_by_sector.setdefault(f"CONCEPT:{concept_code}", set()).add(stock_code)
    for row in rows:
        related_codes = sorted(members_by_sector.get(str(row.get("sector_code") or ""), set()))
        row["relation_member_codes"] = [
            {"stock_code": code} for code in related_codes
        ]
    return "available"


@router.get("/status")
def radar_status() -> dict[str, Any]:
    engine = get_engine()
    ensure_radar_tables(engine)
    rows = _read_rows(
        engine,
        f"""
        SELECT
            (SELECT COUNT(*) FROM {STOCK_TABLE}) AS stock_rows,
            (SELECT COUNT(*) FROM {SECTOR_TABLE}) AS sector_rows,
            (SELECT COUNT(*) FROM {EVENT_TABLE}) AS event_rows,
            (SELECT MAX(snapshot_at) FROM {STOCK_TABLE}) AS latest_stock_at,
            (SELECT MAX(snapshot_at) FROM {SECTOR_TABLE}) AS latest_sector_at,
            (SELECT MAX(snapshot_at) FROM {EVENT_TABLE}) AS latest_event_at
        """,
        context="market_radar_status",
        stringify_datetime=True,
    )
    latest = rows[0] if rows else {}
    freshness_rows = [
        {"kind": kind, "snapshot_at": latest.get(column)}
        for kind, column in (
            ("stock", "latest_stock_at"),
            ("sector", "latest_sector_at"),
        )
    ]
    freshness = _apply_radar_freshness(engine, freshness_rows)
    event_freshness_rows = (
        [{"kind": "event", "snapshot_at": latest.get("latest_event_at")}]
        if latest.get("latest_event_at")
        else []
    )
    _apply_radar_freshness(engine, event_freshness_rows, event=True)
    for item in freshness_rows:
        latest[f"latest_{item['kind']}_status"] = item["freshness_status"]
    channel_status = {
        item["kind"]: {
            "data_status": item["freshness_status"],
            "data_cutoff": item.get("snapshot_at"),
            "freshness_reason": item.get("freshness_reason"),
        }
        for item in freshness_rows
    }
    event_row = event_freshness_rows[0] if event_freshness_rows else None
    if event_row and event_row.get("freshness_status") == "fresh":
        event_channel = {
            "data_status": "fresh",
            "data_cutoff": event_row.get("snapshot_at"),
            "freshness_reason": None,
        }
    elif freshness.get("data_status") == "fresh":
        # Events are emitted only when a threshold is crossed. A current stock
        # and sector scan with no current event is a valid empty event channel,
        # not evidence that the whole radar is stale.
        event_channel = {
            "data_status": "fresh_empty",
            "data_cutoff": freshness.get("data_cutoff"),
            "freshness_reason": "NO_FRESH_EVENTS_IN_CURRENT_RADAR_WINDOW",
        }
    elif event_row:
        event_channel = {
            "data_status": event_row.get("freshness_status") or "unavailable",
            "data_cutoff": event_row.get("snapshot_at"),
            "freshness_reason": event_row.get("freshness_reason"),
        }
    else:
        event_channel = {
            "data_status": "unavailable",
            "data_cutoff": None,
            "freshness_reason": "RADAR_EVENT_SCAN_STATUS_UNAVAILABLE",
        }
    channel_status["event"] = event_channel
    latest["latest_event_status"] = event_channel["data_status"]
    return {
        "status": "ok",
        "data_source": "qmt_full_tick_5level",
        "l2_available": False,
        "decision_scope": "RESEARCH_DISPLAY_ONLY",
        "actionable_output_allowed": False,
        "industry_evidence_status": "DATA_BLOCKED",
        "membership_evidence_status": "LEGACY_UNVERIFIED",
        "funding_eligible": False,
        "order_authority": False,
        "quote_fields": ["price", "change_pct", "amount", "amount_delta", "bid/ask five levels"],
        "flow_note": "QMT 当前环境无 VIP/L2；资金强弱使用成交额增量与五档压力代理",
        "latest": latest,
        "channel_status": channel_status,
        **freshness,
    }


@router.post("/scan")
def scan_radar() -> dict[str, Any]:
    try:
        return get_shared_radar_engine(get_engine()).scan_once()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"market radar scan failed: {exc}") from exc


@router.get("/stocks")
def radar_stocks(
    direction: str = Query("", pattern="^(|UP|DOWN|NEUTRAL)$"),
    scope: str = Query("all", pattern="^(all|watchlist|holding|strategy_candidate)$"),
    limit: int = Query(50, ge=1, le=500),
) -> dict[str, Any]:
    engine = get_engine()
    ensure_radar_tables(engine)
    where = "WHERE 1=1"
    relation_index = _load_relation_index(engine)
    if not _scope_available(scope, relation_index):
        return {
            "status": "unavailable",
            "reason": "RELATION_SOURCE_UNAVAILABLE",
            "requested_scope": scope,
            "relation_context": _public_relation_context(relation_index),
            "rows": [],
            **_apply_radar_freshness(engine, []),
        }
    params: dict[str, Any] = {"limit": limit if scope == "all" else 5000}
    if direction:
        where += " AND direction = :direction"
        params["direction"] = direction
    order = "score DESC" if direction != "DOWN" else "score ASC"
    rows = _read_rows(
        engine,
        f"""
        SELECT stock_code, short_name, snapshot_at, price, change_pct, amount, amount_delta,
               five_pressure, amount_score, price_score, pressure_score, score, direction,
               stale, signal_tags, data_source
        FROM {STOCK_TABLE} {where}
        ORDER BY {order}
        LIMIT :limit
        """,
        params,
        context="market_radar_stocks",
        stringify_datetime=True,
    )
    rows = _json_column(rows, "signal_tags")
    freshness = _apply_radar_freshness(engine, rows)
    rows = annotate_radar_relations(rows, relation_index, scope=scope)[:limit]
    return {
        "status": "ok",
        "data_source": "qmt_full_tick_5level",
        "decision_scope": "RESEARCH_DISPLAY_ONLY",
        "actionable_output_allowed": False,
        "funding_eligible": False,
        "order_authority": False,
        "requested_scope": scope,
        "relation_context": _public_relation_context(relation_index),
        "rows": rows,
        **freshness,
    }


@router.get("/sectors")
def radar_sectors(
    direction: str = Query("", pattern="^(|UP|DOWN|NEUTRAL)$"),
    sector_type: str = Query("", pattern="^(|industry|concept)$"),
    scope: str = Query("all", pattern="^(all|watchlist|holding|strategy_candidate)$"),
    limit: int = Query(50, ge=1, le=500),
) -> dict[str, Any]:
    engine = get_engine()
    ensure_radar_tables(engine)
    where = "WHERE 1=1"
    relation_index = _load_relation_index(engine)
    if not _scope_available(scope, relation_index):
        return {
            "status": "unavailable",
            "reason": "RELATION_SOURCE_UNAVAILABLE",
            "requested_scope": scope,
            "relation_context": _public_relation_context(relation_index),
            "rows": [],
            **_apply_radar_freshness(engine, []),
        }
    params: dict[str, Any] = {"limit": limit if scope == "all" else 500}
    if direction:
        where += " AND direction = :direction"
        params["direction"] = direction
    if sector_type:
        where += " AND sector_type = :sector_type"
        params["sector_type"] = sector_type
    order = "score DESC" if direction != "DOWN" else "score ASC"
    rows = _read_rows(
        engine,
        f"""
        SELECT sector_code, sector_name, sector_type, snapshot_at, member_count,
               positive_count, negative_count, breadth_pct, avg_change_pct,
               amount_delta, score, direction, dragon_json, core_json, follower_json, data_source
        FROM {SECTOR_TABLE} {where}
        ORDER BY {order}
        LIMIT :limit
        """,
        params,
        context="market_radar_sectors",
        stringify_datetime=True,
    )
    rows = _json_column(rows, "dragon_json", "core_json", "follower_json")
    freshness = _apply_radar_freshness(engine, rows)
    membership_status = _attach_sector_relation_members(engine, rows, relation_index)
    rows = annotate_radar_relations(rows, relation_index, scope=scope)[:limit]
    return {
        "status": "ok",
        "data_source": "qmt_full_tick_5level+local_membership",
        "decision_scope": "RESEARCH_DISPLAY_ONLY",
        "actionable_output_allowed": False,
        "industry_evidence_status": "DATA_BLOCKED",
        "membership_evidence_status": "LEGACY_UNVERIFIED",
        "sector_relation_membership_status": membership_status,
        "sector_relation_basis": "current_display_only_concept_membership",
        "funding_eligible": False,
        "order_authority": False,
        "requested_scope": scope,
        "relation_context": _public_relation_context(relation_index),
        "rows": rows,
        **freshness,
    }


@router.get("/events")
def radar_events(
    scope: str = Query("all", pattern="^(all|watchlist|holding|strategy_candidate)$"),
    limit: int = Query(50, ge=1, le=500),
) -> dict[str, Any]:
    engine = get_engine()
    ensure_radar_tables(engine)
    relation_index = _load_relation_index(engine)
    if not _scope_available(scope, relation_index):
        return {
            "status": "unavailable",
            "reason": "RELATION_SOURCE_UNAVAILABLE",
            "requested_scope": scope,
            "relation_context": _public_relation_context(relation_index),
            "rows": [],
            **_apply_radar_freshness(engine, [], event=True),
        }
    rows = _read_rows(
        engine,
        f"""
        SELECT event_id, event_key, event_type, direction, sector_code, sector_name,
               stock_code, snapshot_at, score, detail_json, data_source, created_at
        FROM {EVENT_TABLE}
        ORDER BY event_id DESC
        LIMIT :limit
        """,
        {"limit": limit if scope == "all" else 500},
        context="market_radar_events",
        stringify_datetime=True,
    )
    rows = _json_column(rows, "detail_json")
    freshness = _apply_radar_freshness(engine, rows, event=True)
    membership_status = _attach_sector_relation_members(engine, rows, relation_index)
    rows = annotate_radar_relations(rows, relation_index, scope=scope)[:limit]
    return {
        "status": "ok",
        "data_source": "qmt_full_tick_5level+local_membership",
        "decision_scope": "RESEARCH_DISPLAY_ONLY",
        "actionable_output_allowed": False,
        "membership_evidence_status": "LEGACY_UNVERIFIED",
        "sector_relation_membership_status": membership_status,
        "sector_relation_basis": "current_display_only_concept_membership",
        "funding_eligible": False,
        "order_authority": False,
        "requested_scope": scope,
        "relation_context": _public_relation_context(relation_index),
        "rows": rows,
        **freshness,
    }
