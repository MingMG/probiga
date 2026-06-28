# -*- coding: utf-8 -*-
from datetime import datetime

from fastapi import APIRouter
from sqlalchemy import text
from sqlalchemy.engine import make_url

from server.api.scheduler_runtime import scheduler_runtime_info
from server.api.routers._engine import get_engine
from server.common.config import get_gj_qmt_config, get_minute_mysql_pool_config, get_mysql_url

try:
    from server.common.config import get_qmt_live_runtime_config as _get_qmt_live_runtime_config
except ImportError:
    def _get_qmt_live_runtime_config() -> dict[str, int | bool]:
        return {
            "enabled": False,
            "poll_seconds": 5,
            "idle_sleep_seconds": 30,
            "trading_hours_only": True,
            "candidate_limit": 60,
        }

router = APIRouter(tags=["health"])


def _format_mysql_target() -> dict[str, str | int | None]:
    url = make_url(get_mysql_url(required=True))
    return {
        "drivername": url.drivername,
        "host": url.host,
        "port": url.port,
        "database": url.database,
    }


def _serialize_ts(value) -> str | None:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return None


def _is_trading_time(now: datetime | None = None) -> bool:
    current = now or datetime.now()
    if current.weekday() >= 5:
        return False
    hhmm = current.hour * 100 + current.minute
    return (925 <= hhmm <= 1135) or (1255 <= hhmm <= 1505)


def _table_freshness(table_name: str, code_column: str, *, fresh_window_seconds: int) -> dict[str, object]:
    engine = get_engine()
    sql = text(
        f"""
        SELECT
            MAX(snapshot_at) AS latest_snapshot_at,
            COUNT(*) AS total_rows,
            SUM(CASE WHEN DATE(snapshot_at) = CURDATE() THEN 1 ELSE 0 END) AS today_rows,
            COUNT(DISTINCT CASE WHEN DATE(snapshot_at) = CURDATE() THEN {code_column} END) AS today_symbols
        FROM {table_name}
        """
    )
    now = datetime.now()
    try:
        with engine.connect() as conn:
            row = conn.execute(sql).mappings().first() or {}
    except Exception as exc:
        return {
            "table": table_name,
            "status": "error",
            "error": str(exc),
        }

    latest_snapshot_at = row.get("latest_snapshot_at")
    age_seconds = None
    if isinstance(latest_snapshot_at, datetime):
        age_seconds = max(0, int((now - latest_snapshot_at).total_seconds()))

    trading_now = _is_trading_time(now)
    intraday_fresh = None
    if trading_now:
        intraday_fresh = bool(age_seconds is not None and age_seconds <= fresh_window_seconds)

    status = "ok"
    if trading_now and not intraday_fresh:
        status = "warn"

    return {
        "table": table_name,
        "status": status,
        "latest_snapshot_at": _serialize_ts(latest_snapshot_at),
        "age_seconds": age_seconds,
        "today_rows": int(row.get("today_rows") or 0),
        "today_symbols": int(row.get("today_symbols") or 0),
        "total_rows": int(row.get("total_rows") or 0),
        "intraday_fresh": intraday_fresh,
        "fresh_window_seconds": int(fresh_window_seconds),
    }


@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("/health/runtime")
def health_runtime():
    return {
        "status": "ok",
        **scheduler_runtime_info(),
        "mysql_target": _format_mysql_target(),
        "minute_mysql_pool": get_minute_mysql_pool_config(),
        "gj_qmt": get_gj_qmt_config(),
        "qmt_live_runtime": _get_qmt_live_runtime_config(),
    }


@router.get("/health/intraday-readiness")
def health_intraday_readiness():
    from tools.data_quality_check import intraday_readiness

    return intraday_readiness(get_engine())


@router.get("/health/qmt-bridge")
def health_qmt_bridge():
    from integrations.qmt.diagnostics import diagnostics

    runtime = _get_qmt_live_runtime_config()
    qmt = diagnostics(timeout=int(get_gj_qmt_config()["ping_timeout"] or 8))
    stock_current = _table_freshness(
        "sm_stock_current",
        "stock_code",
        fresh_window_seconds=max(20, int(runtime["poll_seconds"]) * 4),
    )
    index_current = _table_freshness(
        "sm_index_current",
        "index_code",
        fresh_window_seconds=max(30, int(runtime["poll_seconds"]) * 6),
    )
    overall_status = str(qmt.get("status") or "error")
    for item in (stock_current, index_current):
        if item.get("status") == "error":
            overall_status = "error"
            break
        if item.get("status") == "warn" and overall_status == "ok":
            overall_status = item["status"]
    return {
        "status": overall_status,
        "trading_now": _is_trading_time(),
        "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mysql_target": _format_mysql_target(),
        "gj_qmt": qmt,
        "qmt_live_runtime": runtime,
        "stock_current": stock_current,
        "index_current": index_current,
    }


@router.get("/health/qmt-capabilities")
def health_qmt_capabilities(force: bool = False):
    from integrations.qmt.diagnostics import capabilities

    timeout = max(2, int(get_gj_qmt_config()["ping_timeout"] or 8) + 4)
    return capabilities(timeout=timeout, force=force)


@router.get("/health/qmt-core-probe")
def health_qmt_core_probe(force: bool = False):
    from integrations.qmt.diagnostics import core_probe

    timeout = max(15, int(get_gj_qmt_config()["ping_timeout"] or 8) + 22)
    return core_probe(timeout=timeout, force=force)
