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
        "task_name": "Guojin QMT API catalog capability refresh",
        "task_type": "qmt_catalog_capability_refresh",
        "group_name": "Guojin QMT",
        "script_path": "tools/setup_guojin_qmt_catalog.py",
        "script_args": "",
        "cron_time": "01:10",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 86,
        "date_param": "",
        "description": "Refresh official QMT API registry and capability ledger every night; unverified sample probes are recorded explicitly.",
    },
    {
        "task_name": "Guojin QMT local history gap repair execute",
        "task_type": "qmt_local_gap_repair_execute",
        "group_name": "Guojin QMT",
        "script_path": "tools/backfill_guojin_qmt_local_history.py",
        "script_args": "from-gaps --gap-limit 2 --apply --json",
        "cron_time": "07:05",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 90,
        "date_param": "",
        "description": "After the 00:00-07:00 bulk local-history window, repair a small number of registered QMT history gaps into the local history DB and update sys_data_gap status.",
    },
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
        "task_name": "国金QMT盘中实时行情同步",
        "task_type": "qmt_intraday_realtime",
        "group_name": "国金QMT",
        "script_path": "tools/sync_qmt_realtime.py",
        "script_args": "--min-coverage 0.60 --no-archive-snapshot --json",
        "cron_time": "09:25",
        "interval_minutes": 1,
        "enabled": 1,
        "sort_order": 71,
        "date_param": "",
        "description": "国金QMT独立实时行情通道；写入 sm_stock_current 使用安全 Upsert，不清空正式表。",
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
        "task_name": "盘中模拟交易执行Tick",
        "task_type": "sim_trade",
        "group_name": "盘中交易",
        "script_path": "biz/analysis/sync_sim_trade.py",
        "script_args": "--tick --skip-outside-intraday --json",
        "cron_time": "09:31",
        "interval_minutes": 1,
        "enabled": 1,
        "sort_order": 78,
        "date_param": "",
        "description": "事件驱动模拟交易tick：买入只执行信号池NEW信号，卖出做风控检查；非盘中时段快速跳过。",
    },
    {
        "task_name": "盘前模拟交易信号池准备",
        "task_type": "sim_trade_signal_prepare",
        "group_name": "盘中交易",
        "script_path": "biz/analysis/sync_sim_trade.py",
        "script_args": "--prepare-signals --ensure-recommendations --json",
        "cron_time": "09:20",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 77,
        "date_param": "",
        "description": "开盘前将上一交易日AI推荐转换为今日模拟交易信号池；若上一交易日推荐缺失则先严格补生成，日期不匹配则禁止自动新开仓。",
    },
    {
        "task_name": "盘前生产候选榜自动交付",
        "task_type": "screener_premarket_delivery",
        "group_name": "交易决策",
        "script_path": "tools/run_screener_delivery.py",
        "script_args": "--preset capital_support --top 100 --json",
        "cron_time": "09:08",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 78,
        "date_param": "",
        "description": "09:08使用开盘前已知数据生成盘前生产候选榜；结果先固定落库，再将前五名主动发送到早报机器人。错过执行时间会在当日上午自动补跑。",
    },
    {
        "task_name": "开盘生产融合候选榜自动交付",
        "task_type": "screener_intraday_delivery",
        "group_name": "交易决策",
        "script_path": "tools/run_screener_delivery.py",
        "script_args": "--preset intraday_sector --top 100 --json",
        "cron_time": "09:32",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 79,
        "date_param": "",
        "description": "09:32基于当日全市场快照生成V3/V4/V5/V6生产融合榜；结果先固定落库，再将前五名主动发送到早报机器人。错过执行时间会在当日上午自动补跑。",
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
        "task_name": "国金QMT凌晨缺口扫描",
        "task_type": "qmt_nightly_reconciliation",
        "group_name": "国金QMT",
        "script_path": "tools/nightly_guojin_qmt_reconciliation.py",
        "script_args": "--scan-days 20 --json",
        "cron_time": "01:30",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 87,
        "date_param": "",
        "description": "每天凌晨扫描国金QMT待写队列、最近20个交易日覆盖率和质量规则；历史缺口登记到 sys_data_gap 后续补。",
    },
    {
        "task_name": "国金QMT本地历史补数(2026)",
        "task_type": "qmt_local_history_2026",
        "group_name": "国金QMT",
        "script_path": "tools/run_guojin_qmt_full_market_history.py",
        "script_args": (
            "--start-date 2026-01-01 --mode all --daily-batch-size 120 "
            "--minute-batch-size 80 --sleep-seconds 0.2 --stop-at 07:00 "
            "--log-path data/logs/qmt_full_market_history_2026.jsonl --json"
        ),
        "cron_time": "00:00",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 88,
        "date_param": "",
        "description": "每天00:00启动国金QMT本地历史补数，只补2026年至最新交易日；07:00自然停止，次日按本地覆盖率续跑。",
    },
    {
        "task_name": "国金QMT基础目录增量同步",
        "task_type": "qmt_reference_incremental",
        "group_name": "国金QMT",
        "script_path": "tools/sync_guojin_qmt_reference_data.py",
        "script_args": "--skip-refresh --json",
        "cron_time": "03:20",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 89,
        "date_param": "",
        "description": "每天凌晨同步国金QMT板块列表、板块成分、股票/指数基础信息、指数权重；通过安全Upsert只更新新增或变化数据，交易日历不处理。",
    },
    {
        "task_name": "国金QMT历史缺口修复队列",
        "task_type": "qmt_gap_repair_plan",
        "group_name": "国金QMT",
        "script_path": "tools/repair_guojin_qmt_gaps.py",
        "script_args": "--limit 50 --json",
        "cron_time": "02:00",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 89,
        "date_param": "",
        "description": "每天凌晨列出待修复历史缺口；当前仅计划不自动拉取，避免在QMT历史下载未完全验收前误写。",
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
        "task_name": "AI推荐盘前严格生成",
        "task_type": "analysis_morning_strict",
        "group_name": "AI推荐",
        "script_path": "biz/analysis/sync_analysis_fast.py",
        "script_args": "--strict-prev-trade-day --top-n 80 --min-score 62 --min-kline-coverage 0.80 --auto-repair-missing-kline",
        "cron_time": "08:30",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 91,
        "date_param": "",
        "description": "每天08:30按执行日上一交易日严格生成AI推荐；上一交易日K线不足时先用国金QMT补目标日，仍不足则失败，不回退更早日期。",
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
            assignments = ", ".join(f"`{key}` = :{key}" for key in payload)
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
