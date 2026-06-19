# -*- coding: utf-8 -*-
"""JoinQuant live minute data automation API."""
from __future__ import annotations

from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text

from server.api.routers._engine import get_engine
from tools.sync_jq_minute_gml import TABLE_NAME, _run_ddl, is_trading_time, sync_jq_minute_gml

router = APIRouter(tags=["jq-minute"])

TASK_NAME = "盘中聚宽分钟GML同步"
TASK_TYPE = "jq_minute_gml"
SCRIPT_PATH = "tools/sync_jq_minute_gml.py"

SCHEDULER_COLUMNS = {
    "task_type": "VARCHAR(50) DEFAULT 'python'",
    "group_name": "VARCHAR(32) DEFAULT 'system'",
    "script_args": "VARCHAR(500) DEFAULT ''",
    "date_param": "VARCHAR(100) DEFAULT ''",
    "interval_minutes": "INT DEFAULT 0",
    "sort_order": "INT DEFAULT 0",
    "last_triggered_at": "DATETIME DEFAULT NULL",
    "last_run_output": "TEXT DEFAULT NULL",
    "last_run_duration": "INT DEFAULT 0",
    "etl_sync_at": "DATETIME DEFAULT NULL",
    "updated_at": "DATETIME DEFAULT NULL",
}
NOW_COLUMNS = {"created_at", "updated_at", "etl_sync_at"}


def _read_sql(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    df = pd.read_sql(text(sql), get_engine(), params=params)
    if df.empty:
        return []
    df = df.replace({np.nan: None, pd.NaT: None})
    for column in df.columns:
        if str(df[column].dtype).startswith("datetime"):
            df[column] = df[column].astype(str)
    return df.to_dict(orient="records")


def _table_exists(table_name: str) -> bool:
    with get_engine().connect() as conn:
        count = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = :table_name
                """
            ),
            {"table_name": table_name},
        ).scalar()
    return bool(count)


def _table_columns(table_name: str) -> set[str]:
    with get_engine().connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT COLUMN_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = :table_name
                """
            ),
            {"table_name": table_name},
        ).fetchall()
    return {str(row[0]) for row in rows}


def _ensure_scheduler_columns() -> set[str]:
    columns = _table_columns("st_scheduled_tasks")
    if not columns:
        raise RuntimeError("st_scheduled_tasks does not exist")

    with get_engine().begin() as conn:
        for column, ddl in SCHEDULER_COLUMNS.items():
            if column not in columns:
                conn.execute(text(f"ALTER TABLE st_scheduled_tasks ADD COLUMN `{column}` {ddl}"))
                columns.add(column)
    return columns


def _scheduler_where(columns: set[str]) -> tuple[str, dict[str, Any]]:
    params = {"task_name": TASK_NAME, "task_type": TASK_TYPE, "script_path": SCRIPT_PATH}
    clauses = ["task_name = :task_name", "script_path = :script_path"]
    if "task_type" in columns:
        clauses.append("task_type = :task_type")
    return " OR ".join(clauses), params


def _build_script_args(
    *,
    universe: str,
    codes: str,
    limit: int,
    count: int,
    batch_size: int,
    min_coverage: float,
    include_now: bool,
    include_paused: bool,
    include_bj: bool,
) -> str:
    args = [
        "--universe", universe,
        "--count", str(max(1, count)),
        "--batch-size", str(max(1, batch_size)),
        "--min-coverage", str(max(0.0, min_coverage)),
        "--skip-closed",
        "--json",
    ]
    cleaned_codes = ",".join(item.strip() for item in codes.replace(";", ",").split(",") if item.strip())
    if cleaned_codes:
        args.extend(["--codes", cleaned_codes])
    if limit > 0:
        args.extend(["--limit", str(limit)])
    if not include_now:
        args.append("--complete-only")
    if include_paused:
        args.append("--include-paused")
    if include_bj:
        args.append("--include-bj")
    return " ".join(args)


def _upsert_scheduler_task(payload: dict[str, Any]) -> dict[str, Any]:
    columns = _ensure_scheduler_columns()
    allowed = {
        "task_name", "task_type", "group_name", "script_path", "script_args",
        "cron_time", "interval_minutes", "enabled", "description",
        "sort_order", "date_param",
    }
    compatible = {key: value for key, value in payload.items() if key in allowed and key in columns}
    if not compatible:
        raise RuntimeError("no compatible scheduler columns found")

    where_sql, params = _scheduler_where(columns)
    with get_engine().begin() as conn:
        task_id = conn.execute(
            text(f"SELECT id FROM st_scheduled_tasks WHERE {where_sql} LIMIT 1"),
            params,
        ).scalar()
        if task_id:
            assignments = ", ".join(f"`{key}` = :{key}" for key in compatible if key != "task_name")
            if "updated_at" in columns:
                assignments += ", `updated_at` = NOW()"
            conn.execute(
                text(f"UPDATE st_scheduled_tasks SET {assignments} WHERE id = :id"),
                {**compatible, "id": task_id},
            )
            action = "updated"
        else:
            insert_payload = dict(compatible)
            for column in NOW_COLUMNS:
                if column in columns:
                    insert_payload[column] = None
            names = ", ".join(f"`{key}`" for key in insert_payload)
            values = ", ".join("NOW()" if key in NOW_COLUMNS else f":{key}" for key in insert_payload)
            bind_payload = {key: value for key, value in insert_payload.items() if key not in NOW_COLUMNS}
            conn.execute(text(f"INSERT INTO st_scheduled_tasks ({names}) VALUES ({values})"), bind_payload)
            task_id = conn.execute(text("SELECT LAST_INSERT_ID()")).scalar()
            action = "inserted"
    return {"id": int(task_id), "action": action}


def _scheduler_row() -> dict[str, Any] | None:
    columns = _table_columns("st_scheduled_tasks")
    if not columns:
        return None
    where_sql, params = _scheduler_where(columns)
    rows = _read_sql(f"SELECT * FROM st_scheduled_tasks WHERE {where_sql} LIMIT 1", params)
    return rows[0] if rows else None


def _jq_runtime_status() -> dict[str, Any]:
    from tools.jq_config import get_jq_client

    try:
        get_jq_client(required=True)
    except RuntimeError as exc:
        return {"available": False, "reason": str(exc)}
    return {"available": True, "reason": ""}


def _raise_jq_unavailable() -> None:
    status = _jq_runtime_status()
    if not status["available"]:
        raise HTTPException(status_code=503, detail=status["reason"])


@router.post("/jq/minute/table/ensure")
def ensure_jq_minute_table():
    _run_ddl(get_engine())
    return {"success": True, "table": TABLE_NAME}


@router.get("/jq/minute/status")
def jq_minute_status(include_quota: bool = False):
    engine = get_engine()
    jq_status = _jq_runtime_status()
    table_exists = _table_exists(TABLE_NAME)
    latest = None
    latest_day = None
    if table_exists:
        latest_rows = _read_sql(
            f"""
            SELECT MAX(trade_time) AS latest_trade_time,
                   MAX(trade_date) AS latest_trade_date,
                   COUNT(*) AS total_rows
            FROM `{TABLE_NAME}`
            """
        )
        latest = latest_rows[0] if latest_rows else None
        day_rows = _read_sql(
            f"""
            SELECT trade_date,
                   COUNT(DISTINCT stock_code) AS stock_count,
                   COUNT(*) AS row_count,
                   MAX(trade_time) AS latest_trade_time
            FROM `{TABLE_NAME}`
            WHERE trade_date = (SELECT MAX(trade_date) FROM `{TABLE_NAME}`)
            GROUP BY trade_date
            """
        )
        latest_day = day_rows[0] if day_rows else None

    quota = None
    quota_error = ""
    if include_quota and jq_status["available"]:
        from tools.jq_config import jq_auth

        try:
            jq = jq_auth()
            quota = jq.get_query_count()
        except RuntimeError as exc:
            quota_error = str(exc)
            jq_status = {"available": False, "reason": quota_error}

    return {
        "table": TABLE_NAME,
        "table_exists": table_exists,
        "is_trading_time": is_trading_time(engine, datetime.now()),
        "latest": latest,
        "latest_day": latest_day,
        "scheduler_task": _scheduler_row(),
        "jq_query_count": quota,
        "jq_status": jq_status,
        "jq_quota_error": quota_error,
    }


@router.post("/jq/minute/sync")
def sync_jq_minute_once(
    universe: str = Query(default="latest-kline", pattern="^(latest-kline|si-all)$"),
    codes: str = Query(default=""),
    limit: int = Query(default=0, ge=0),
    count: int = Query(default=3, ge=1, le=30),
    batch_size: int = Query(default=200, ge=1, le=1000),
    include_now: bool = True,
    include_paused: bool = False,
    include_bj: bool = False,
    skip_closed: bool = True,
    min_coverage: float = Query(default=0.0, ge=0.0, le=1.0),
    dry_run: bool = False,
):
    _raise_jq_unavailable()
    return sync_jq_minute_gml(
        get_engine(),
        universe=universe,
        codes=codes,
        limit=limit,
        count=count,
        batch_size=batch_size,
        include_now=include_now,
        skip_paused=not include_paused,
        skip_closed=skip_closed,
        min_coverage=min_coverage,
        include_bj=include_bj,
        dry_run=dry_run,
    )


@router.post("/jq/minute/automation/enable")
def enable_jq_minute_automation(
    universe: str = Query(default="latest-kline", pattern="^(latest-kline|si-all)$"),
    codes: str = Query(default=""),
    limit: int = Query(default=0, ge=0),
    count: int = Query(default=3, ge=1, le=30),
    batch_size: int = Query(default=200, ge=1, le=1000),
    interval_minutes: int = Query(default=1, ge=1, le=30),
    cron_time: str = Query(default="09:30"),
    min_coverage: float = Query(default=0.0, ge=0.0, le=1.0),
    include_now: bool = True,
    include_paused: bool = False,
    include_bj: bool = False,
):
    _raise_jq_unavailable()
    _run_ddl(get_engine())
    script_args = _build_script_args(
        universe=universe,
        codes=codes,
        limit=limit,
        count=count,
        batch_size=batch_size,
        min_coverage=min_coverage,
        include_now=include_now,
        include_paused=include_paused,
        include_bj=include_bj,
    )
    task = {
        "task_name": TASK_NAME,
        "task_type": TASK_TYPE,
        "group_name": "盘中交易",
        "script_path": SCRIPT_PATH,
        "script_args": script_args,
        "cron_time": cron_time,
        "interval_minutes": interval_minutes,
        "enabled": 1,
        "sort_order": 71,
        "date_param": "",
        "description": "交易时段每分钟从聚宽同步最新1分钟K线，写入sm_stock_minute_gml。",
    }
    upsert = _upsert_scheduler_task(task)
    return {"success": True, "task": {**task, **upsert}, "table": TABLE_NAME}


@router.post("/jq/minute/automation/disable")
def disable_jq_minute_automation():
    columns = _table_columns("st_scheduled_tasks")
    if not columns:
        return {"success": False, "error": "st_scheduled_tasks does not exist"}
    where_sql, params = _scheduler_where(columns)
    with get_engine().begin() as conn:
        result = conn.execute(
            text(f"UPDATE st_scheduled_tasks SET enabled = 0 WHERE {where_sql}"),
            params,
        )
    return {"success": True, "disabled": int(result.rowcount or 0), "task_name": TASK_NAME}
