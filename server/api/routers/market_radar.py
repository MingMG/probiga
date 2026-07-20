# -*- coding: utf-8 -*-
"""API endpoints for the QMT intraday anomaly radar."""
from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text

from biz.market_radar.core import EVENT_TABLE, SECTOR_TABLE, STOCK_TABLE, ensure_radar_tables, get_shared_radar_engine
from server.api.routers._engine import get_engine

router = APIRouter(prefix="/market-radar", tags=["market-radar"])


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
            (SELECT MAX(updated_at) FROM {STOCK_TABLE}) AS latest_stock_at,
            (SELECT MAX(updated_at) FROM {SECTOR_TABLE}) AS latest_sector_at
        """,
        context="market_radar_status",
        stringify_datetime=True,
    )
    return {
        "status": "ok",
        "data_source": "qmt_full_tick_5level",
        "l2_available": False,
        "quote_fields": ["price", "change_pct", "amount", "amount_delta", "bid/ask five levels"],
        "flow_note": "QMT 当前环境无 VIP/L2；资金强弱使用成交额增量与五档压力代理",
        "latest": rows[0] if rows else {},
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
    limit: int = Query(50, ge=1, le=500),
) -> dict[str, Any]:
    engine = get_engine()
    ensure_radar_tables(engine)
    where = "WHERE 1=1"
    params: dict[str, Any] = {"limit": limit}
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
    return {"status": "ok", "data_source": "qmt_full_tick_5level", "rows": _json_column(rows, "signal_tags")}


@router.get("/sectors")
def radar_sectors(
    direction: str = Query("", pattern="^(|UP|DOWN|NEUTRAL)$"),
    sector_type: str = Query("", pattern="^(|industry|concept)$"),
    limit: int = Query(50, ge=1, le=500),
) -> dict[str, Any]:
    engine = get_engine()
    ensure_radar_tables(engine)
    where = "WHERE 1=1"
    params: dict[str, Any] = {"limit": limit}
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
    return {
        "status": "ok",
        "data_source": "qmt_full_tick_5level+local_membership",
        "rows": _json_column(rows, "dragon_json", "core_json", "follower_json"),
    }


@router.get("/events")
def radar_events(limit: int = Query(50, ge=1, le=500)) -> dict[str, Any]:
    engine = get_engine()
    ensure_radar_tables(engine)
    rows = _read_rows(
        engine,
        f"""
        SELECT event_id, event_key, event_type, direction, sector_code, sector_name,
               stock_code, snapshot_at, score, detail_json, data_source, created_at
        FROM {EVENT_TABLE}
        ORDER BY event_id DESC
        LIMIT :limit
        """,
        {"limit": limit},
        context="market_radar_events",
        stringify_datetime=True,
    )
    return {"status": "ok", "data_source": "qmt_full_tick_5level", "rows": _json_column(rows, "detail_json")}
