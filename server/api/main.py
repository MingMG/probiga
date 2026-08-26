# -*- coding: utf-8 -*-
from contextlib import asynccontextmanager
import logging
import os
from pathlib import Path
import time
import uuid

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.exc import (
    DBAPIError,
    InterfaceError,
    OperationalError,
    SQLAlchemyError,
    TimeoutError as SQLAlchemyTimeoutError,
)
from starlette.concurrency import run_in_threadpool

from server.api.admin_auth import validate_admin_request
from server.api.qmt_live_runtime import start_qmt_live_runtime, stop_qmt_live_runtime
from server.api.market_radar_runtime import start_market_radar_runtime, stop_market_radar_runtime
from server.api.routers._engine import dispose_engine as dispose_api_engine
from server.api.routers import ai_bridge, auth, broad_etf_flow, commentary, datasource, deploy, health, hot_data, jq_minute, notify, scheduler, sim_trade, strategy_center, screener, trading_v2, trading_v3
from server.api.routers import market_radar
from server.api.scheduler_runtime import start_embedded_scheduler, stop_embedded_scheduler
from server.common.config import get_api_lifespan_config, get_api_observability_config
from server.common.kline_data import dispose_kline_engine
from server.common.minute_data import dispose_minute_engine
from server.common.strategy_governance_mode import (
    strategy_governance_database_deferred,
    get_strategy_governance_mode,
)
from server.engine.strategy_execution_adapters import (
    bootstrap_strategy_execution_adapter_registry,
)
from server.api.strategy_evidence_request_limit import (
    StrategyEvidenceRequestSizeMiddleware,
    StrategyEvidenceRequestTooLarge,
    StrategyGovernanceRequestTooLarge,
    strategy_evidence_too_large_response,
    strategy_governance_too_large_response,
)

logger = logging.getLogger(__name__)

_MYSQL_CONNECTION_ERROR_CODES = frozenset(
    {
        1040,  # too many connections
        1042,  # unable to resolve database host
        1043,  # bad handshake
        1045,  # access denied
        1049,  # unknown database
        1053,  # server shutdown in progress
        1077,  # server shutdown
        1129,  # host blocked
        1130,  # host not allowed
        1152,
        1153,
        1154,
        1155,
        1156,
        1157,
        1158,
        1159,
        1160,
        1161,
        1203,  # per-user connection limit
        1226,  # resource connection limit
        2002,
        2003,
        2006,
        2012,
        2013,
        2026,
        2055,
    }
)


def _dbapi_error_code(exc: SQLAlchemyError) -> int | None:
    original = getattr(exc, "orig", None)
    arguments = getattr(original, "args", ())
    if arguments and isinstance(arguments[0], int):
        return int(arguments[0])
    return None


def _database_error_response(
    exc: SQLAlchemyError,
) -> tuple[int, dict[str, object]]:
    """Return a safe, accurate public response for a SQLAlchemy failure."""

    error_code = _dbapi_error_code(exc)
    unavailable = isinstance(
        exc,
        (InterfaceError, SQLAlchemyTimeoutError),
    ) or (
        isinstance(exc, OperationalError)
        and (
            error_code is None
            or error_code in _MYSQL_CONNECTION_ERROR_CODES
        )
    ) or (isinstance(exc, DBAPIError) and bool(exc.connection_invalidated))
    if unavailable:
        return 503, {
            "status": "error",
            "error": "database_unavailable",
            "message": "数据库连接暂时不可用，请稍后重试。",
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    return 500, {
        "status": "error",
        "error": "database_operation_failed",
        "message": "数据库操作失败，服务版本或数据结构可能不一致。",
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def _desktop_runtimes_allowed() -> bool:
    """Return whether this host may start Windows/QMT desktop runtimes."""
    return os.name == "nt"


_STRATEGY_CENTER_API_ROOT = "/api/strategy-center"
_STRATEGY_GOVERNANCE_OVERVIEW_PATH = (
    f"{_STRATEGY_CENTER_API_ROOT}/governance"
)
_SAFE_HTTP_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


def _deferred_governance_request_blocked(method: str, path: str) -> bool:
    """Identify governance access that cannot run without its DB contract."""

    normalized_method = str(method or "").upper()
    normalized_path = str(path or "")
    in_strategy_center = (
        normalized_path == _STRATEGY_CENTER_API_ROOT
        or normalized_path.startswith(f"{_STRATEGY_CENTER_API_ROOT}/")
    )
    if not in_strategy_center:
        return False
    if normalized_method not in _SAFE_HTTP_METHODS:
        return True
    return normalized_path.startswith(
        f"{_STRATEGY_GOVERNANCE_OVERVIEW_PATH}/"
    )


def dispose_shared_engines() -> None:
    for label, dispose in (
        ("api", dispose_api_engine),
        ("minute", dispose_minute_engine),
        ("kline", dispose_kline_engine),
    ):
        try:
            dispose()
        except Exception as exc:
            logger.warning(
                "Engine dispose failed: engine=%s exception_type=%s",
                label,
                type(exc).__name__,
            )


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Unknown mode values must stop startup before any worker or DB writer is
    # allowed to run.
    get_strategy_governance_mode()
    # Fail before starting any background worker if the production code-owned
    # adapter manifest is absent, unexpected, or does not match the release seal.
    bootstrap_strategy_execution_adapter_registry()
    start_embedded_scheduler()
    api_lifespan = get_api_lifespan_config()
    qmt_live_runtime_started = False
    market_radar_runtime_started = False
    if _desktop_runtimes_allowed() and api_lifespan["qmt_live_runtime_enabled"]:
        start_qmt_live_runtime()
        qmt_live_runtime_started = True
    if _desktop_runtimes_allowed() and start_market_radar_runtime() is not None:
        market_radar_runtime_started = True
    try:
        yield
    finally:
        stop_embedded_scheduler()
        if qmt_live_runtime_started:
            stop_qmt_live_runtime()
        if market_radar_runtime_started:
            stop_market_radar_runtime()
        dispose_shared_engines()


app = FastAPI(
    title="ProBigA",
    description="数据分析平台 API",
    lifespan=lifespan,
)
app.add_middleware(StrategyEvidenceRequestSizeMiddleware)


@app.exception_handler(StrategyEvidenceRequestTooLarge)
async def strategy_evidence_request_too_large_handler(
    _request: Request, _exc: StrategyEvidenceRequestTooLarge,
):
    return strategy_evidence_too_large_response()


@app.exception_handler(StrategyGovernanceRequestTooLarge)
async def strategy_governance_request_too_large_handler(
    _request: Request, _exc: StrategyGovernanceRequestTooLarge,
):
    return strategy_governance_too_large_response()

app.include_router(health.router, prefix="/api")
app.include_router(auth.router, prefix="/api")
app.include_router(ai_bridge.router, prefix="/api")
app.include_router(notify.router, prefix="/api")
app.include_router(hot_data.router, prefix="/api")
app.include_router(broad_etf_flow.router, prefix="/api")
app.include_router(jq_minute.router, prefix="/api")
app.include_router(scheduler.router, prefix="/api")
app.include_router(sim_trade.router, prefix="/api")
app.include_router(strategy_center.router, prefix="/api")
app.include_router(screener.router, prefix="/api")
app.include_router(datasource.router, prefix="/api")
app.include_router(commentary.router, prefix="/api")
app.include_router(deploy.router, prefix="/api")
app.include_router(market_radar.router, prefix="/api")
app.include_router(trading_v2.router, prefix="/api")
app.include_router(trading_v3.router, prefix="/api")


@app.middleware("http")
async def enforce_deferred_governance_boundary(
    request: Request, call_next,
):
    if (
        strategy_governance_database_deferred()
        and _deferred_governance_request_blocked(
            request.method, request.url.path,
        )
    ):
        return JSONResponse(
            status_code=503,
            content={
                "status": "blocked",
                "error": "governance_database_deferred",
                "message": "治理数据库迁移待完成，当前接口暂不可用。",
                "strategy_governance_mode": "DEFERRED_DB",
                "schema_ready": False,
                "governance_ready": False,
                "activation_enabled": False,
                "automatic_real_order_submission": False,
                "real_order_authority": False,
            },
        )
    return await call_next(request)


@app.middleware("http")
async def require_admin_auth(request: Request, call_next):
    blocked = await run_in_threadpool(validate_admin_request, request)
    if blocked is not None:
        return blocked
    return await call_next(request)


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    embedded_same_origin_page = (
        request.url.path in {
            "/static/trading-v2.html",
            "/static/trading-v3.html",
            "/ai-stock",
            "/ai-general",
        }
        and request.query_params.get("embedded") == "1"
    )
    if embedded_same_origin_page:
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        response.headers["Content-Security-Policy"] = "frame-ancestors 'self'"
    else:
        response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "same-origin")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
    return response


@app.middleware("http")
async def add_timing_headers(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
    response.headers["X-ProBigA-Elapsed-Ms"] = str(elapsed_ms)
    slow_ms = int(get_api_observability_config()["slow_request_ms"])
    if slow_ms and elapsed_ms >= slow_ms:
        logger.warning(
            "Slow request %s %s completed in %.2fms status=%s",
            request.method,
            request.url.path,
            elapsed_ms,
            response.status_code,
        )
    return response


@app.exception_handler(SQLAlchemyError)
async def sqlalchemy_exception_handler(request: Request, exc: SQLAlchemyError):
    logger.error(
        "Database error while handling %s %s: type=%s code=%s invalidated=%s",
        request.method,
        request.url.path,
        type(exc).__name__,
        _dbapi_error_code(exc),
        bool(getattr(exc, "connection_invalidated", False)),
    )
    status_code, content = _database_error_response(exc)
    return JSONResponse(
        status_code=status_code,
        content=content,
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    incident_id = uuid.uuid4().hex
    logger.error(
        "Unhandled request error: incident_id=%s exception_type=%s method=%s path=%s",
        incident_id,
        type(exc).__name__,
        request.method,
        request.url.path,
    )
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "error": "internal_server_error",
            "message": "服务内部错误，请稍后重试。",
            "incident_id": incident_id,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        },
    )

static_dir = Path(__file__).resolve().parent.parent / "static"


def _index_html() -> HTMLResponse:
    index_path = static_dir / "index.html"
    if index_path.is_file():
        return HTMLResponse(
            content=index_path.read_text(encoding="utf-8"),
            headers={"Cache-Control": "no-store"},
        )
    return HTMLResponse(
        content="<h1>ProBigA</h1>",
        headers={"Cache-Control": "no-store"},
    )


@app.get("/", response_class=HTMLResponse)
def index():
    return _index_html()


@app.get("/login", response_class=HTMLResponse)
def login_page():
    page_path = static_dir / "login.html"
    if page_path.is_file():
        return HTMLResponse(
            content=page_path.read_text(encoding="utf-8"),
            headers={"Cache-Control": "no-store"},
        )
    return HTMLResponse(content="<h1>ProBigA Login</h1>")


@app.get("/intraday-battle", response_class=HTMLResponse)
def intraday_battle():
    return _index_html()


@app.get("/battle", response_class=HTMLResponse)
def battle_shortcut():
    return _index_html()


@app.get("/deploy", response_class=HTMLResponse)
def deploy_console():
    if not deploy._in_app_deploy_enabled():
        raise HTTPException(status_code=404, detail="In-app deployment is disabled")
    page_path = static_dir / "deploy.html"
    if page_path.is_file():
        return HTMLResponse(content=page_path.read_text(encoding="utf-8"))
    return HTMLResponse(content="<h1>ProBigA Deploy</h1>")


@app.get("/market-radar", response_class=HTMLResponse)
def market_radar_page():
    page_path = static_dir / "market_radar.html"
    if page_path.is_file():
        return HTMLResponse(content=page_path.read_text(encoding="utf-8"))
    return HTMLResponse(content="<h1>Market Radar</h1>")


def _ai_page(filename: str, fallback_title: str) -> HTMLResponse:
    page_path = static_dir / filename
    if page_path.is_file():
        return HTMLResponse(
            content=page_path.read_text(encoding="utf-8"),
            headers={"Cache-Control": "no-store"},
        )
    return HTMLResponse(content=f"<h1>{fallback_title}</h1>")


@app.get("/ai-stock", response_class=HTMLResponse)
def ai_stock_page():
    return _ai_page("ai-stock.html", "Stock AI")


@app.get("/ai-general", response_class=HTMLResponse)
def ai_general_page():
    return _ai_page("ai-general.html", "General AI")


@app.get("/trading-v2", response_class=RedirectResponse)
def trading_v2_page():
    return RedirectResponse(url="/?tab=trading", status_code=307)


@app.get("/trading-v3", response_class=RedirectResponse)
def trading_v3_page():
    return RedirectResponse(url="/?tab=trading", status_code=307)


if static_dir.is_dir():
    app.mount("/static", StaticFiles(directory=str(static_dir), html=False), name="static")
