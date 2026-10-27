# -*- coding: utf-8 -*-
"""Ten bounded tools consumed by the A-share briefing assistant."""
from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import date
import json
import logging
import time
import uuid
from typing import Any, Callable, TypeVar

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.exc import SQLAlchemyError
from starlette.concurrency import run_in_threadpool

from server.briefing_tools.auth import require_briefing_tools_access
from server.briefing_tools.contracts import TOOL_SCHEMA_VERSION, ToolResult
from server.briefing_tools.repository import BriefingDataReader
from server.common.config import get_briefing_tools_config


logger = logging.getLogger("briefing_tools")
router = APIRouter(
    prefix="/briefing-tools",
    tags=["a-share-briefing-tools"],
)
_TOOL_DEPENDENCIES = [Depends(require_briefing_tools_access)]

_T = TypeVar("_T", bound=ToolResult)


def get_reader(request: Request) -> BriefingDataReader:
    reader = getattr(request.app.state, "briefing_reader", None)
    if reader is None:
        reader = BriefingDataReader()
        request.app.state.briefing_reader = reader
    return reader


def _request_id(request: Request) -> str:
    supplied = request.headers.get("X-Request-ID", "").strip()
    if supplied and len(supplied) <= 96 and all(
        character.isalnum() or character in "-_.:" for character in supplied
    ):
        return supplied
    return uuid.uuid4().hex


async def _execute(
    request: Request,
    *,
    tool: str,
    function: Callable[[], _T],
) -> dict[str, Any]:
    request_id = _request_id(request)
    started = time.monotonic()
    timeout = int(get_briefing_tools_config()["timeout_seconds"])
    status = "RUNNING"
    row_count = 0
    try:
        result = await asyncio.wait_for(
            run_in_threadpool(function),
            timeout=float(timeout),
        )
        row_count = len(result.data)
        status = result.data_status
        payload = {
            "schema_version": TOOL_SCHEMA_VERSION,
            "tool": tool,
            "request_id": request_id,
            "as_of": result.as_of,
            "data_status": result.data_status,
            "missing_fields": list(result.missing_fields),
            "data": result.data,
            "meta": result.meta,
        }
        return payload
    except asyncio.TimeoutError as exc:
        status = "TIMEOUT"
        raise HTTPException(
            status_code=504,
            detail={
                "error": "tool_timeout",
                "message": f"{tool} exceeded its {timeout}-second deadline.",
                "request_id": request_id,
            },
        ) from exc
    except ValueError as exc:
        status = "INVALID_ARGUMENT"
        raise HTTPException(
            status_code=422,
            detail={
                "error": "invalid_tool_argument",
                "message": str(exc),
                "request_id": request_id,
            },
        ) from exc
    except SQLAlchemyError as exc:
        status = "DATABASE_ERROR"
        logger.warning(
            "briefing tool database failure request_id=%s tool=%s "
            "exception_type=%s",
            request_id,
            tool,
            type(exc).__name__,
        )
        raise HTTPException(
            status_code=503,
            detail={
                "error": "database_unavailable",
                "message": "The read-only market database is unavailable.",
                "request_id": request_id,
            },
        ) from exc
    finally:
        elapsed_ms = round((time.monotonic() - started) * 1000.0, 3)
        logger.info(
            "%s",
            json.dumps(
                {
                    "event": "briefing_tool_call",
                    "request_id": request_id,
                    "tool": tool,
                    "status": status,
                    "elapsed_ms": elapsed_ms,
                    "row_count": row_count,
                },
                ensure_ascii=True,
                separators=(",", ":"),
            ),
        )


@router.get(
    "/index-snapshot",
    operation_id="get_index_snapshot",
    summary="Read the five core A-share index closes for one trade date.",
    dependencies=_TOOL_DEPENDENCIES,
)
async def get_index_snapshot(
    request: Request,
    trade_date: date = Query(..., alias="trade_date"),
    reader: BriefingDataReader = Depends(get_reader),
):
    return await _execute(
        request,
        tool="get_index_snapshot",
        function=lambda: reader.index_snapshot(trade_date),
    )


@router.get(
    "/sector-rank",
    operation_id="get_sector_rank",
    summary="Read the bounded sector ranking for one trade date.",
    dependencies=_TOOL_DEPENDENCIES,
)
async def get_sector_rank(
    request: Request,
    trade_date: date = Query(..., alias="trade_date"),
    top_n: int = Query(default=10, ge=1, le=200),
    reader: BriefingDataReader = Depends(get_reader),
):
    return await _execute(
        request,
        tool="get_sector_rank",
        function=lambda: reader.sector_rank(trade_date, top_n),
    )


@router.get(
    "/stock-rank",
    operation_id="get_stock_rank",
    summary="Read ranked stocks belonging to one verified sector.",
    dependencies=_TOOL_DEPENDENCIES,
)
async def get_stock_rank(
    request: Request,
    trade_date: date = Query(..., alias="trade_date"),
    sector_name: str = Query(..., min_length=1, max_length=160),
    top_n: int = Query(default=5, ge=1, le=200),
    reader: BriefingDataReader = Depends(get_reader),
):
    return await _execute(
        request,
        tool="get_stock_rank",
        function=lambda: reader.stock_rank(
            trade_date,
            sector_name,
            top_n,
        ),
    )


@router.get(
    "/watchlist",
    operation_id="get_watchlist",
    summary="Read the operator watchlist and holdings without recommendations.",
    dependencies=_TOOL_DEPENDENCIES,
)
async def get_watchlist(
    request: Request,
    trade_date: date = Query(..., alias="trade_date"),
    reader: BriefingDataReader = Depends(get_reader),
):
    return await _execute(
        request,
        tool="get_watchlist",
        function=lambda: reader.watchlist(trade_date),
    )


@router.get(
    "/announcements",
    operation_id="get_announcements",
    summary="Read validated announcement titles for one publication date.",
    dependencies=_TOOL_DEPENDENCIES,
)
async def get_announcements(
    request: Request,
    trade_date: date = Query(..., alias="trade_date"),
    limit: int = Query(default=200, ge=1, le=200),
    reader: BriefingDataReader = Depends(get_reader),
):
    return await _execute(
        request,
        tool="get_announcements",
        function=lambda: reader.announcements(trade_date, limit),
    )


@router.get(
    "/market-breadth",
    operation_id="get_market_breadth",
    summary="Read daily breadth, limit structure and sentiment for one date.",
    dependencies=_TOOL_DEPENDENCIES,
)
async def get_market_breadth(
    request: Request,
    trade_date: date = Query(..., alias="trade_date"),
    reader: BriefingDataReader = Depends(get_reader),
):
    return await _execute(
        request,
        tool="get_market_breadth",
        function=lambda: reader.market_breadth(trade_date),
    )


@router.get(
    "/intraday-snapshot",
    operation_id="get_intraday_snapshot",
    summary="Read a bounded stock, sector or market intraday snapshot.",
    dependencies=_TOOL_DEPENDENCIES,
)
async def get_intraday_snapshot(
    request: Request,
    trade_date: date = Query(..., alias="trade_date"),
    time_from: str = Query(..., pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d$"),
    time_to: str = Query(..., pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d$"),
    scope: str = Query(default="market", max_length=160),
    limit: int = Query(default=200, ge=1, le=200),
    reader: BriefingDataReader = Depends(get_reader),
):
    return await _execute(
        request,
        tool="get_intraday_snapshot",
        function=lambda: reader.intraday_snapshot(
            trade_date,
            time_from,
            time_to,
            scope,
            limit,
        ),
    )


@router.get(
    "/limit-up-events",
    operation_id="get_limit_up_events",
    summary="Read append-only touch, seal, break and reseal events.",
    dependencies=_TOOL_DEPENDENCIES,
)
async def get_limit_up_events(
    request: Request,
    trade_date: date = Query(..., alias="trade_date"),
    stock_code: str = Query(default="", max_length=16),
    time_from: str = Query(default="09:15", pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d$"),
    time_to: str = Query(default="15:10", pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d$"),
    limit: int = Query(default=200, ge=1, le=200),
    reader: BriefingDataReader = Depends(get_reader),
):
    return await _execute(
        request,
        tool="get_limit_up_events",
        function=lambda: reader.limit_up_events(
            trade_date,
            stock_code,
            time_from,
            time_to,
            limit,
        ),
    )


@router.get(
    "/sector-linkage",
    operation_id="get_sector_linkage",
    summary="Read sector and member movement with explicit time-lag evidence.",
    dependencies=_TOOL_DEPENDENCIES,
)
async def get_sector_linkage(
    request: Request,
    trade_date: date = Query(..., alias="trade_date"),
    time_window: str = Query(
        ...,
        pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d-(?:[01]\d|2[0-3]):[0-5]\d$",
    ),
    sector: str = Query(default="", max_length=160),
    limit: int = Query(default=20, ge=1, le=200),
    reader: BriefingDataReader = Depends(get_reader),
):
    return await _execute(
        request,
        tool="get_sector_linkage",
        function=lambda: reader.sector_linkage(
            trade_date,
            time_window,
            sector,
            limit,
        ),
    )


@router.get(
    "/market-breadth-intraday",
    operation_id="get_market_breadth_intraday",
    summary="Read bucketed intraday breadth and event-backed break rates.",
    dependencies=_TOOL_DEPENDENCIES,
)
async def get_market_breadth_intraday(
    request: Request,
    trade_date: date = Query(..., alias="trade_date"),
    time_from: str = Query(default="09:30", pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d$"),
    time_to: str = Query(default="15:00", pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d$"),
    interval_minutes: int = Query(default=5),
    reader: BriefingDataReader = Depends(get_reader),
):
    return await _execute(
        request,
        tool="get_market_breadth_intraday",
        function=lambda: reader.market_breadth_intraday(
            trade_date,
            time_from,
            time_to,
            interval_minutes,
        ),
    )


@router.get(
    "/openapi.json",
    include_in_schema=False,
    dependencies=[],
)
def briefing_openapi(request: Request) -> dict[str, Any]:
    """Return only the ten AI tools, never the application's mutation routes."""

    schema = deepcopy(request.app.openapi())
    schema["info"] = {
        "title": "ProBigA A-share Briefing Tools",
        "version": TOOL_SCHEMA_VERSION,
        "description": (
            "Read-only, bounded market-data tools. Output is information "
            "organization and is not investment advice."
        ),
    }
    prefix = "/api/briefing-tools/"
    schema["paths"] = {
        path: operations
        for path, operations in schema.get("paths", {}).items()
        if path.startswith(prefix) and path != f"{prefix}openapi.json"
    }
    return schema


__all__ = ["router"]
