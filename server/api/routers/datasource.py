# -*- coding: utf-8 -*-
"""数据源管理 API"""
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from fastapi import APIRouter, Query
from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.api.scheduler_runtime import (
    launch_scheduler_task,
    strategy_governance_task_block_reason,
)

router = APIRouter(tags=["datasource"])


def _read_sql(sql: str, params: dict = None) -> list[dict]:
    df = pd.read_sql(text(sql), get_engine(), params=params)
    if df.empty:
        return []
    df = df.replace({np.nan: None, pd.NaT: None})
    for c in df.columns:
        if df[c].dtype == "datetime64[ns]":
            df[c] = df[c].astype(str)
    return df.to_dict(orient="records")


def _execute_sql(sql: str, params: dict = None):
    with get_engine().begin() as conn:
        conn.execute(text(sql), params or {})


# 数据源分组配置
# key: 提供商标识
# value: 提供商配置，包含名称、图标、业务类型分组
DATASOURCE_CONFIG = {
    "adata": {
        "name": "adata",
        "icon": "📊",
        "types": {
            "行情数据": ["stock_kline", "stock_minute", "stock_current", "dividend"],
            "基础数据": ["all_code", "index_constituent", "concept_code_east"],
            "龙虎榜": ["alist_daily", "alist_info"],
            "指数行情": ["index_current", "index_minute"],
        }
    },
    "同花顺": {
        "name": "同花顺",
        "icon": "🏆",
        "types": {
            "热门数据": ["hot_rank_ths", "hot_concept", "sync_concept_ths"],
            "概念行情": ["concept_ths_current", "concept_ths_minute"],
        }
    },
    "东财": {
        "name": "东财",
        "icon": "✨",
        "types": {
            "资金数据": ["capital_flow_batch_fast"],
            "热门数据": ["hot_pop_east"],
            "概念行情": ["concept_east_current", "concept_east_minute", "concept_flow"],
            "板块热度": ["sector_heat_east"],
        }
    },
    "新浪": {
        "name": "新浪",
        "icon": "🌐",
        "types": {
            "热门数据": ["hot_rank_sina"],
        }
    },
    "雪球": {
        "name": "雪球",
        "icon": "❄️",
        "types": {
            "热门数据": ["fetch_hot_rank_xq"],
        }
    },
    "融合数据": {
        "name": "融合数据",
        "icon": "🔗",
        "types": {
            "融合榜单": ["hot_fused", "hot_fused_3", "hot_fused_5"],
        }
    },
    "聚宽": {
        "name": "聚宽",
        "icon": "📈",
        "types": {
            "分钟数据": ["jq_minute_gml"],
        }
    },
    "内部": {
        "name": "内部",
        "icon": "🤖",
        "types": {
            "分析数据": ["analysis_fast", "sim_trade"],
            "复盘报告": ["daily_review", "evening_review"],
            "资讯数据": ["news_daily", "news_sync", "news_weekly"],
            "盘中监控": ["intraday_realtime", "intraday_minute_kline", "intraday_minute_flow", "intraday_quality_check"],
            "数据质量": ["quality_check_pre", "quality_check_post"],
        }
    },
}


def _classify_task(task_type: str) -> tuple[str, str]:
    """根据 task_type 分类到提供商和业务类型"""
    for provider, config in DATASOURCE_CONFIG.items():
        for biz_type, types in config["types"].items():
            if task_type in types:
                return provider, biz_type
    return "其他", "其他"


@router.get("/datasource/list")
def list_datasources():
    """获取数据源列表，按提供商分组"""
    rows = _read_sql("""
        SELECT id, task_name, task_type, script_path, script_args,
               cron_time, interval_minutes, date_param, enabled,
               last_run_status, last_run_at, last_run_duration,
               last_run_output, group_name
        FROM st_scheduled_tasks
        ORDER BY sort_order
    """)

    now = datetime.now()
    grouped = {}

    for row in rows:
        task_type = row.get("task_type") or ""
        provider, biz_type = _classify_task(task_type)

        if provider not in grouped:
            grouped[provider] = {
                "provider": provider,
                "icon": DATASOURCE_CONFIG.get(provider, {}).get("icon", "📌"),
                "types": {}
            }

        if biz_type not in grouped[provider]["types"]:
            grouped[provider]["types"][biz_type] = []

        # 计算下次执行时间
        interval_minutes = int(row.get("interval_minutes") or 0)
        if interval_minutes > 0:
            ref_time = row.get("last_run_at")
            if ref_time:
                try:
                    ref_dt = datetime.strptime(str(ref_time)[:19], "%Y-%m-%d %H:%M:%S")
                    next_dt = ref_dt + timedelta(minutes=interval_minutes)
                    if next_dt <= now:
                        next_dt = now
                    row["next_run_at"] = next_dt.strftime("%Y-%m-%d %H:%M")
                except Exception:
                    row["next_run_at"] = ""
            else:
                row["next_run_at"] = now.strftime("%Y-%m-%d %H:%M")
        else:
            cron_time = str(row.get("cron_time") or "17:10").strip()
            try:
                h, m = cron_time.split(":")
                next_dt = datetime(now.year, now.month, now.day, int(h), int(m))
                if next_dt <= now:
                    next_dt += timedelta(days=1)
                row["next_run_at"] = next_dt.strftime("%Y-%m-%d %H:%M")
            except Exception:
                row["next_run_at"] = ""

        grouped[provider]["types"][biz_type].append(row)

    # 转换为列表格式（按 DATASOURCE_CONFIG 的顺序）
    result = []
    for provider in list(DATASOURCE_CONFIG.keys()) + ["其他"]:
        if provider in grouped:
            result.append(grouped[provider])

    return {"data": result, "total": len(rows)}


@router.get("/datasource/stats")
def get_stats():
    """获取数据源统计信息"""
    rows = _read_sql("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN last_run_status = 'success' THEN 1 ELSE 0 END) as success,
            SUM(CASE WHEN last_run_status = 'failed' THEN 1 ELSE 0 END) as failed,
            SUM(CASE WHEN last_run_status = 'running' THEN 1 ELSE 0 END) as running,
            SUM(CASE WHEN last_run_status IS NULL OR last_run_status = '' THEN 1 ELSE 0 END) as pending,
            SUM(CASE WHEN enabled = 1 THEN 1 ELSE 0 END) as enabled,
            SUM(CASE WHEN enabled = 0 THEN 1 ELSE 0 END) as disabled
        FROM st_scheduled_tasks
    """)

    stats = rows[0] if rows else {}
    return {
        "total": int(stats.get("total") or 0),
        "success": int(stats.get("success") or 0),
        "failed": int(stats.get("failed") or 0),
        "running": int(stats.get("running") or 0),
        "pending": int(stats.get("pending") or 0),
        "enabled": int(stats.get("enabled") or 0),
        "disabled": int(stats.get("disabled") or 0),
    }


@router.get("/datasource/{task_id}/history")
def get_history(task_id: int, limit: int = Query(default=10)):
    """获取数据源运行历史"""
    # 先从 st_scheduled_task_history 表读取（如果有）
    try:
        rows = _read_sql("""
            SELECT id, run_at, status, duration, output
            FROM st_scheduled_task_history
            WHERE task_id = :task_id
            ORDER BY run_at DESC
            LIMIT :limit
        """, {"task_id": task_id, "limit": limit})
        if rows:
            return {"data": rows}
    except Exception:
        pass

    # 如果没有历史表，从主表读取最近一次运行信息
    row = _read_sql("""
        SELECT id, last_run_at as run_at, last_run_status as status,
               last_run_duration as duration, last_run_output as output
        FROM st_scheduled_tasks
        WHERE id = :task_id
    """, {"task_id": task_id})

    return {"data": row}


@router.get("/datasource/{task_id}/log")
def get_log(task_id: int):
    """获取数据源最新运行日志"""
    row = _read_sql("""
        SELECT last_run_output, last_run_status, last_run_at
        FROM st_scheduled_tasks
        WHERE id = :task_id
    """, {"task_id": task_id})

    if not row:
        return {"error": "任务不存在"}

    return {
        "data": {
            "output": row[0].get("last_run_output") or "",
            "status": row[0].get("last_run_status") or "",
            "run_at": row[0].get("last_run_at") or "",
        }
    }


@router.post("/datasource/{task_id}/run")
def run_task(task_id: int):
    """通过共享的抢占、归属和审计链路异步执行数据源任务。"""
    row = _read_sql("SELECT * FROM st_scheduled_tasks WHERE id = :id", {"id": task_id})
    if not row:
        return {"error": "任务不存在"}
    engine = get_engine()
    result = launch_scheduler_task(
        row[0],
        root=Path(__file__).resolve().parents[3],
        engine=engine,
    )
    # Preserve the fields consumed by the existing datasource page while also
    # exposing the launcher's accepted/task_id/job_id decision contract.
    return {
        "id": task_id,
        "duration": 0,
        "output": "任务已提交后台执行" if result.get("accepted") else "",
        **result,
    }


@router.post("/datasource/{task_id}/toggle")
def toggle_task(task_id: int):
    """启用/禁用数据源"""
    row = _read_sql(
        "SELECT id, task_type, script_path, enabled "
        "FROM st_scheduled_tasks WHERE id = :id",
        {"id": task_id},
    )
    if not row:
        return {"error": "任务不存在"}
    new_enabled = 0 if row[0]["enabled"] == 1 else 1
    governance_block_reason = strategy_governance_task_block_reason(row[0])
    if new_enabled == 1 and governance_block_reason:
        return {
            "id": task_id,
            "enabled": 0,
            "error": "治理数据库延迟模式下禁止启用策略治理任务",
            "status": governance_block_reason,
        }
    _execute_sql("UPDATE st_scheduled_tasks SET enabled = :e, updated_at = NOW() WHERE id = :id", {"e": new_enabled, "id": task_id})
    return {"id": task_id, "enabled": new_enabled}
