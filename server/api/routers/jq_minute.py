# -*- coding: utf-8 -*-
"""JoinQuant live minute data automation API.

All writes run as registered scheduler tasks. API workers only inspect status,
toggle one exact registration, or submit strict arguments to the audited
launcher.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
from typing import Any

import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.common.jq_minute_schema import JQ_MINUTE_TABLE, validate_jq_minute_runtime
from server.common.manual_scheduler_launch import (
    launch_registered_scheduler_task,
    validate_scheduler_launch_surface,
)
from tools.manual_long_task_contracts import JQ_MINUTE_TASK
from tools.sync_jq_minute_gml import is_trading_time


router = APIRouter(tags=["jq-minute"])

TABLE_NAME = JQ_MINUTE_TABLE
TASK_NAME = str(JQ_MINUTE_TASK["task_name"])
TASK_TYPE = str(JQ_MINUTE_TASK["task_type"])
SCRIPT_PATH = str(JQ_MINUTE_TASK["script_path"])
_ROOT = Path(__file__).resolve().parents[3]


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
    with get_engine().connect() as connection:
        count = connection.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:table_name"
            ),
            {"table_name": table_name},
        ).scalar()
    return bool(count)


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
    skip_closed: bool = True,
    dry_run: bool = False,
) -> str:
    """Build a shell-free, allow-listed scheduler argument string."""

    if universe not in {"latest-kline", "si-all"}:
        raise ValueError("invalid JQ minute universe")
    if not 0 <= int(limit) <= 6000:
        raise ValueError("invalid JQ minute limit")
    if not 1 <= int(count) <= 30 or not 1 <= int(batch_size) <= 1000:
        raise ValueError("invalid JQ minute batch parameters")
    if not 0.0 <= float(min_coverage) <= 1.0:
        raise ValueError("invalid JQ minute coverage")
    raw_codes = [
        item.strip()
        for item in str(codes or "").replace(";", ",").split(",")
        if item.strip()
    ]
    if len(raw_codes) > 200 or any(
        not re.fullmatch(r"[0-9]{6}", item) for item in raw_codes
    ):
        raise ValueError("invalid JQ minute stock codes")

    args = [
        "--universe", universe,
        "--count", str(int(count)),
        "--batch-size", str(int(batch_size)),
        "--min-coverage", str(float(min_coverage)),
    ]
    cleaned_codes = ",".join(dict.fromkeys(raw_codes))
    if cleaned_codes:
        args.extend(["--codes", cleaned_codes])
    if limit > 0:
        args.extend(["--limit", str(int(limit))])
    if not include_now:
        args.append("--complete-only")
    if include_paused:
        args.append("--include-paused")
    if include_bj:
        args.append("--include-bj")
    if skip_closed:
        args.append("--skip-closed")
    if dry_run:
        args.append("--dry-run")
    args.append("--json")
    return " ".join(args)


def _scheduler_rows() -> list[dict[str, Any]]:
    validate_scheduler_launch_surface(get_engine())
    return _read_sql(
        "SELECT * FROM st_scheduled_tasks "
        "WHERE task_type=:task_type ORDER BY id LIMIT 2",
        {"task_type": TASK_TYPE},
    )


def _scheduler_row() -> dict[str, Any] | None:
    try:
        rows = _scheduler_rows()
    except Exception:
        return {"status": "scheduler_registry_unavailable"}
    if len(rows) != 1:
        return {
            "status": "task_registration_missing" if not rows else "task_registration_ambiguous"
        }
    row = dict(rows[0])
    if str(row.get("script_path") or "").replace("\\", "/") != SCRIPT_PATH:
        return {"status": "task_contract_mismatch"}
    return row


def _jq_runtime_status() -> dict[str, Any]:
    from tools.jq_config import get_jq_client

    try:
        get_jq_client(required=True)
    except RuntimeError:
        return {"available": False, "reason": "JQ_RUNTIME_UNAVAILABLE"}
    return {"available": True, "reason": ""}


@router.post("/jq/minute/table/ensure")
def ensure_jq_minute_table():
    raise HTTPException(
        status_code=410,
        detail="运行时建表已禁用；请在受控发布窗口执行 privileged_migrate_jq_minute_tables",
    )


@router.get("/jq/minute/status")
def jq_minute_status(include_quota: bool = False):
    engine = get_engine()
    jq_status = _jq_runtime_status()
    table_exists = _table_exists(TABLE_NAME)
    schema_status = "missing"
    latest = None
    latest_day = None
    if table_exists:
        try:
            validate_jq_minute_runtime(engine)
            schema_status = "valid"
            latest_rows = _read_sql(
                f"SELECT MAX(trade_time) AS latest_trade_time, "
                f"MAX(trade_date) AS latest_trade_date, COUNT(*) AS total_rows "
                f"FROM `{TABLE_NAME}`"
            )
            latest = latest_rows[0] if latest_rows else None
            day_rows = _read_sql(
                f"SELECT trade_date, COUNT(DISTINCT stock_code) AS stock_count, "
                f"COUNT(*) AS row_count, MAX(trade_time) AS latest_trade_time "
                f"FROM `{TABLE_NAME}` "
                f"WHERE trade_date=(SELECT MAX(trade_date) FROM `{TABLE_NAME}`) "
                f"GROUP BY trade_date"
            )
            latest_day = day_rows[0] if day_rows else None
        except Exception:
            schema_status = "invalid"

    quota = None
    quota_error = ""
    if include_quota and jq_status["available"]:
        from tools.jq_config import jq_auth

        try:
            quota = jq_auth().get_query_count()
        except RuntimeError:
            quota_error = "JQ_QUOTA_UNAVAILABLE"
            jq_status = {"available": False, "reason": quota_error}

    return {
        "table": TABLE_NAME,
        "table_exists": table_exists,
        "schema_status": schema_status,
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
    codes: str = Query(default="", max_length=1400),
    limit: int = Query(default=0, ge=0, le=6000),
    count: int = Query(default=3, ge=1, le=30),
    batch_size: int = Query(default=200, ge=1, le=1000),
    include_now: bool = True,
    include_paused: bool = False,
    include_bj: bool = False,
    skip_closed: bool = True,
    min_coverage: float = Query(default=0.0, ge=0.0, le=1.0),
    dry_run: bool = False,
):
    try:
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
            skip_closed=skip_closed,
            dry_run=dry_run,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="invalid JQ minute sync parameters") from exc
    result = launch_registered_scheduler_task(
        get_engine(),
        task_type=TASK_TYPE,
        expected_script_path=SCRIPT_PATH,
        script_args=script_args,
        root=_ROOT,
    )
    return {**result, "queued": bool(result.get("accepted")), "table": TABLE_NAME}


def _registered_task_for_toggle() -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    try:
        rows = _scheduler_rows()
    except Exception:
        return None, {
            "success": False,
            "status": "scheduler_registry_unavailable",
            "error": "生产任务注册表不可用",
        }
    if len(rows) != 1:
        return None, {
            "success": False,
            "status": "task_registration_missing" if not rows else "task_registration_ambiguous",
            "error": "生产任务未唯一注册",
        }
    row = dict(rows[0])
    if str(row.get("script_path") or "").replace("\\", "/") != SCRIPT_PATH:
        return None, {
            "success": False,
            "status": "task_contract_mismatch",
            "error": "生产任务脚本合同不匹配",
        }
    return row, None


@router.post("/jq/minute/automation/enable")
def enable_jq_minute_automation():
    row, error = _registered_task_for_toggle()
    if error:
        return error
    if str(row.get("script_args") or "").strip() != str(JQ_MINUTE_TASK["script_args"]):
        return {
            "success": False,
            "status": "task_contract_mismatch",
            "error": "生产任务参数合同不匹配",
        }
    with get_engine().begin() as connection:
        result = connection.execute(
            text("UPDATE st_scheduled_tasks SET enabled=1 WHERE id=:id AND task_type=:task_type"),
            {"id": int(row["id"]), "task_type": TASK_TYPE},
        )
    changed = int(result.rowcount or 0)
    if changed != 1:
        return {"success": False, "status": "task_toggle_conflict", "error": "任务状态更新冲突"}
    return {"success": True, "enabled": True, "task_id": int(row["id"]), "task_type": TASK_TYPE}


@router.post("/jq/minute/automation/disable")
def disable_jq_minute_automation():
    row, error = _registered_task_for_toggle()
    if error:
        return error
    with get_engine().begin() as connection:
        result = connection.execute(
            text("UPDATE st_scheduled_tasks SET enabled=0 WHERE id=:id AND task_type=:task_type"),
            {"id": int(row["id"]), "task_type": TASK_TYPE},
        )
    changed = int(result.rowcount or 0)
    if changed != 1:
        return {"success": False, "status": "task_toggle_conflict", "error": "任务状态更新冲突"}
    return {"success": True, "enabled": False, "task_id": int(row["id"]), "task_type": TASK_TYPE}
