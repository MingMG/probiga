#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ensure reliability-focused scheduled tasks exist.

This script is intentionally non-destructive: it only adds missing scheduler
columns and upserts the quality-gate tasks used to catch stale data before the
dashboard or paper-trading workflows trust it.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.config import get_mysql_url


SCHEDULER_COLUMNS = {
    "task_type": "VARCHAR(50) DEFAULT 'python'",
    "group_name": "VARCHAR(32) DEFAULT 'system'",
    "script_args": "VARCHAR(500) DEFAULT ''",
    "date_param": "VARCHAR(100) DEFAULT ''",
    "date_param_desc": "VARCHAR(200) DEFAULT ''",
    "interval_minutes": "INT DEFAULT 0",
    "sort_order": "INT DEFAULT 0",
    "last_triggered_at": "DATETIME DEFAULT NULL",
    "last_run_output": "TEXT DEFAULT NULL",
    "last_run_duration": "INT DEFAULT 0",
    "etl_sync_at": "DATETIME DEFAULT NULL",
    "updated_at": "DATETIME DEFAULT NULL",
}

NOW_COLUMNS = {"created_at", "updated_at", "etl_sync_at"}


TASKS = [
    {
        "task_name": "盘中实时行情同步",
        "task_type": "intraday_realtime",
        "group_name": "盘中交易",
        "script_path": "tools/crawl_realtime_batch.py",
        "script_args": "--only snapshot --min-coverage 0.70 --archive-snapshot --skip-closed --json",
        "cron_time": "09:25",
        "interval_minutes": 1,
        "enabled": 1,
        "sort_order": 70,
        "date_param": "",
        "description": "交易时段每分钟刷新 sm_stock_current，并归档到 sm_rt_quote_snapshot；覆盖率不足时失败。",
    },
    {
        "task_name": "盘中分钟K线同步",
        "task_type": "intraday_minute_kline",
        "group_name": "盘中交易",
        "script_path": "tools/crawl_minute_kline.py",
        "script_args": "--type stock --min-coverage 0.70 --skip-closed",
        "cron_time": "09:35",
        "interval_minutes": 15,
        "enabled": 1,
        "sort_order": 72,
        "date_param": "",
        "description": "交易时段周期性同步全市场分钟K线；覆盖率不足时失败。",
    },
    {
        "task_name": "盘中分钟资金流同步",
        "task_type": "intraday_minute_flow",
        "group_name": "盘中交易",
        "script_path": "tools/crawl_minute_kline.py",
        "script_args": "--type flow --min-coverage 0.50 --skip-closed",
        "cron_time": "09:40",
        "interval_minutes": 15,
        "enabled": 1,
        "sort_order": 74,
        "date_param": "",
        "description": "交易时段周期性同步分钟资金流；覆盖率不足时失败。",
    },
    {
        "task_name": "盘中实时质量体检",
        "task_type": "intraday_quality_check",
        "group_name": "盘中交易",
        "script_path": "tools/data_quality_check.py",
        "script_args": "--json --include-realtime --fail-on-warn --skip-closed",
        "cron_time": "09:45",
        "interval_minutes": 5,
        "enabled": 1,
        "sort_order": 76,
        "date_param": "",
        "description": "交易时段严格检查实时、分钟、资金流基础数据；非交易时段跳过。",
    },
    {
        "task_name": "盘中模拟交易扫描",
        "task_type": "sim_trade",
        "group_name": "盘中交易",
        "script_path": "biz/analysis/sync_sim_trade.py",
        "script_args": "",
        "cron_time": "09:31",
        "interval_minutes": 1,
        "enabled": 1,
        "sort_order": 78,
        "date_param": "",
        "description": "交易时段每分钟扫描模拟买卖信号；非交易时段由策略引擎跳过。",
    },
    {
        "task_name": "盘后快速资金流同步",
        "task_type": "capital_flow_batch_fast",
        "group_name": "系统管理",
        "script_path": "tools/crawl_realtime_batch.py",
        "script_args": "--only flow --min-coverage 0.70 --json",
        "cron_time": "15:20",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 84,
        "date_param": "",
        "description": "盘后用东财全市场批量接口快速补齐最新交易日资金流，作为逐股慢任务前置保障。",
    },
    {
        "task_name": "盘前数据质量体检",
        "task_type": "quality_check_pre",
        "group_name": "系统管理",
        "script_path": "tools/data_quality_check.py",
        "script_args": "--json --fail-on-warn",
        "cron_time": "08:45",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 88,
        "date_param": "",
        "description": "盘前只读体检；有 WARN/FAIL 时任务失败，提醒不要信任过期推荐。",
    },
    {
        "task_name": "盘后快速分析推荐",
        "task_type": "analysis_fast",
        "group_name": "系统管理",
        "script_path": "biz/analysis/sync_analysis_fast.py",
        "script_args": "--top-n 80 --min-score 62",
        "cron_time": "18:50",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 90,
        "date_param": "",
        "description": "基于最新日K批量生成 stock_analysis_result 与 st_recommended_stocks。",
    },
    {
        "task_name": "盘后数据质量体检",
        "task_type": "quality_check_post",
        "group_name": "系统管理",
        "script_path": "tools/data_quality_check.py",
        "script_args": "--json --fail-on-warn",
        "cron_time": "19:30",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 92,
        "date_param": "",
        "description": "盘后只读体检；确认采集、分析、推荐和调度链路是否跟上最新交易日。",
    },
]


def _table_columns(engine: Engine, table_name: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = :table_name
        """), {"table_name": table_name}).fetchall()
    return {str(row[0]) for row in rows}


def ensure_scheduler_columns(engine: Engine) -> None:
    columns = _table_columns(engine, "st_scheduled_tasks")
    if not columns:
        raise RuntimeError("st_scheduled_tasks does not exist")

    with engine.begin() as conn:
        for column, ddl in SCHEDULER_COLUMNS.items():
            if column not in columns:
                conn.execute(text(f"ALTER TABLE st_scheduled_tasks ADD COLUMN `{column}` {ddl}"))


def _task_payload(task: dict[str, Any], columns: set[str]) -> dict[str, Any]:
    allowed = {
        "task_name", "task_type", "group_name", "script_path", "script_args",
        "cron_time", "interval_minutes", "enabled", "description",
        "sort_order", "date_param",
    }
    return {k: v for k, v in task.items() if k in allowed and k in columns}


def upsert_task(engine: Engine, task: dict[str, Any]) -> str:
    columns = _table_columns(engine, "st_scheduled_tasks")
    payload = _task_payload(task, columns)
    if not payload:
        raise RuntimeError("no compatible scheduler columns found")

    with engine.begin() as conn:
        existing_id = conn.execute(
            text("""
                SELECT id
                FROM st_scheduled_tasks
                WHERE task_name = :task_name
                   OR task_type = :task_type
                LIMIT 1
            """),
            {"task_name": task["task_name"], "task_type": task["task_type"]},
        ).scalar()

        if existing_id:
            assignments = ", ".join(f"`{key}` = :{key}" for key in payload if key != "task_name")
            if "updated_at" in columns:
                assignments += ", `updated_at` = NOW()"
            conn.execute(
                text(f"UPDATE st_scheduled_tasks SET {assignments} WHERE id = :id"),
                {**payload, "id": existing_id},
            )
            return "updated"

        insert_payload = dict(payload)
        for column in NOW_COLUMNS:
            if column in columns:
                insert_payload[column] = None
        names = ", ".join(f"`{key}`" for key in insert_payload)
        values = ", ".join("NOW()" if key in NOW_COLUMNS else f":{key}" for key in insert_payload)
        bind_payload = {k: v for k, v in insert_payload.items() if k not in NOW_COLUMNS}
        conn.execute(text(f"INSERT INTO st_scheduled_tasks ({names}) VALUES ({values})"), bind_payload)
        return "inserted"


def run(engine: Engine) -> dict[str, str]:
    ensure_scheduler_columns(engine)
    result: dict[str, str] = {}
    for task in TASKS:
        result[task["task_name"]] = upsert_task(engine, task)
    return result


def main() -> int:
    engine = create_engine(get_mysql_url(required=True), pool_pre_ping=True)
    result = run(engine)
    for task_name, action in result.items():
        print(f"{action}: {task_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
