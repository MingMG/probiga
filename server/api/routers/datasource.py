# -*- coding: utf-8 -*-
"""数据源管理 API"""
import copy
import logging
import threading
import time
from datetime import datetime, timedelta

from fastapi import APIRouter, Query
from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.common.batch_db import quote_identifier
from server.common.scheduler_runner import run_scheduler_task_sync
from server.common.scheduler_tasks import update_scheduler_task
from server.common.sql_reader import read_sql_rows

router = APIRouter(tags=["datasource"])
logger = logging.getLogger(__name__)
_REQUIRED_HEALTH_CACHE_TTL_SECONDS = 120
_required_health_cache_lock = threading.Lock()
_required_health_cache: tuple[float, list[dict]] | None = None

def _read_sql(sql: str, params: dict = None) -> list[dict]:
    return read_sql_rows(get_engine(), sql, params, context="datasource")


REQUIRED_TASK_HEALTH = [
    {
        "key": "hot_rank_sina",
        "label": "新浪热股",
        "task_type": "hot_rank_sina",
        "task_types": ["hot_rank_sina"],
        "table": "st_hot_rank_sina",
        "date_col": "snapshot_date",
        "min_rows": 1,
        "max_stale_days": 4,
        "require_latest_trade_date": True,
        "target_ready_time": "17:16",
    },
    {
        "key": "capital_flow",
        "label": "个股资金流向(全量)",
        "task_type": "capital_flow",
        "task_types": ["capital_flow", "capital_flow_batch_fast"],
        "table": "sm_stock_capital_flow_daily",
        "date_col": "trade_date",
        "min_rows": 100,
        "max_stale_days": 4,
        "require_latest_trade_date": True,
        "target_ready_time": "15:20",
    },
    {
        "key": "concept_flow",
        "label": "概念资金流向",
        "task_type": "concept_flow",
        "task_types": ["concept_flow"],
        "table": "sm_concept_capital_flow_east",
        "date_col": "snapshot_at",
        "min_rows": 1,
        "max_stale_days": 4,
        "require_latest_trade_date": True,
        "target_ready_time": "19:30",
    },
]


def _parse_dt(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    text_value = str(value).strip()
    if not text_value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text_value[:len(fmt)], fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(text_value.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _time_reached(now: datetime, hhmm: str) -> bool:
    try:
        hour, minute = str(hhmm or "00:00").split(":", 1)
        return now.hour * 60 + now.minute >= int(hour) * 60 + int(minute)
    except Exception:
        return True


def _latest_required_trade_date(now: datetime, *, ready_time: str = "00:00") -> datetime.date:
    today = now.date().isoformat()
    comparator = "<=" if _time_reached(now, ready_time) else "<"
    try:
        rows = _read_sql(
            f"""
            SELECT MAX(trade_date) AS latest_trade_date
            FROM si_trade_calendar
            WHERE trade_status = 1 AND trade_date {comparator} :today
            """,
            {"today": today},
        )
        value = rows[0].get("latest_trade_date") if rows else None
        parsed = _parse_dt(value)
        if parsed:
            return parsed.date()
    except Exception:
        logger.debug("Failed to resolve latest required trade date", exc_info=True)
    return now.date()


def _task_types_for_config(cfg: dict) -> list[str]:
    raw = cfg.get("task_types") or [cfg.get("task_type")]
    out = []
    for item in raw:
        value = str(item or "").strip()
        if value and value not in out:
            out.append(value)
    return out


def _select_required_task(candidates: list[dict], primary_task_type: str) -> dict | None:
    if not candidates:
        return None

    def score(row: dict) -> tuple:
        status = str(row.get("last_run_status") or "").lower()
        run_at = _parse_dt(row.get("last_run_at")) or datetime.min
        return (
            1 if int(row.get("enabled") or 0) == 1 else 0,
            2 if status in ("success", "running") else 1 if status not in ("failed", "timeout", "stopped") else 0,
            run_at,
            1 if str(row.get("task_type") or "") == primary_task_type else 0,
        )

    return max(candidates, key=score)


def _table_freshness(table: str, date_col: str) -> dict:
    exists = _read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table
        """,
        {"table": table},
    )
    if not exists or int(exists[0].get("cnt") or 0) <= 0:
        return {"table_exists": False, "max_data_time": None, "row_count_latest": 0}

    quoted_table = quote_identifier(table)
    quoted_date_col = quote_identifier(date_col)
    latest = _read_sql(f"SELECT MAX({quoted_date_col}) AS max_data_time FROM {quoted_table}")
    max_data_time = latest[0].get("max_data_time") if latest else None
    if not max_data_time:
        return {"table_exists": True, "max_data_time": None, "row_count_latest": 0}

    count_rows = _read_sql(
        f"SELECT COUNT(*) AS cnt FROM {quoted_table} WHERE {quoted_date_col} = :max_data_time",
        {"max_data_time": max_data_time},
    )
    return {
        "table_exists": True,
        "max_data_time": str(max_data_time),
        "row_count_latest": int(count_rows[0].get("cnt") or 0) if count_rows else 0,
    }


def _required_task_health(now: datetime | None = None) -> list[dict]:
    now = now or datetime.now()
    all_task_types = []
    for cfg in REQUIRED_TASK_HEALTH:
        for task_type in _task_types_for_config(cfg):
            if task_type not in all_task_types:
                all_task_types.append(task_type)
    placeholders = ", ".join(f":task_type_{idx}" for idx, _ in enumerate(all_task_types))
    params = {f"task_type_{idx}": task_type for idx, task_type in enumerate(all_task_types)}
    task_rows = _read_sql(
        f"""
        SELECT id, task_name, task_type, script_path, script_args, enabled,
               last_run_status, last_run_at, last_run_duration, last_run_output
        FROM st_scheduled_tasks
        WHERE task_type IN ({placeholders})
        ORDER BY id
        """,
        params,
    )
    tasks_by_type: dict[str, list[dict]] = {}
    for row in task_rows:
        task_type = row.get("task_type") or ""
        if task_type:
            tasks_by_type.setdefault(task_type, []).append(row)

    results = []
    for cfg in REQUIRED_TASK_HEALTH:
        latest_trade_date = _latest_required_trade_date(
            now,
            ready_time=str(cfg.get("target_ready_time") or "00:00"),
        )
        task_types = _task_types_for_config(cfg)
        candidates = [row for task_type in task_types for row in tasks_by_type.get(task_type, [])]
        task = _select_required_task(candidates, cfg["task_type"])
        item = {
            "key": cfg["key"],
            "label": cfg["label"],
            "task_type": cfg["task_type"],
            "candidate_task_types": task_types,
            "table": cfg["table"],
            "date_col": cfg["date_col"],
            "min_rows": cfg["min_rows"],
            "target_trade_date": latest_trade_date.isoformat(),
            "configured": bool(task),
            "status": "ok",
            "message": "正常",
        }

        if task:
            item.update({
                "task_id": task.get("id"),
                "task_name": task.get("task_name"),
                "enabled": int(task.get("enabled") or 0),
                "last_run_status": task.get("last_run_status") or "",
                "last_run_at": task.get("last_run_at") or "",
                "last_run_duration": task.get("last_run_duration"),
                "selected_task_type": task.get("task_type") or "",
                "script_path": task.get("script_path") or "",
                "script_args": task.get("script_args") or "",
                "last_run_output_tail": (task.get("last_run_output") or "")[-500:],
            })
        else:
            item.update({"status": "missing_task", "message": "调度任务未配置"})
            results.append(item)
            continue

        try:
            item.update(_table_freshness(cfg["table"], cfg["date_col"]))
        except Exception as exc:
            item.update({
                "table_exists": None,
                "max_data_time": None,
                "row_count_latest": 0,
                "status": "table_error",
                "message": f"数据表检查失败: {exc}",
            })
            results.append(item)
            continue

        last_status = str(item.get("last_run_status") or "").lower()
        max_dt = _parse_dt(item.get("max_data_time"))
        row_count = int(item.get("row_count_latest") or 0)
        enabled = int(item.get("enabled") or 0) == 1

        if not enabled:
            item.update({"status": "disabled", "message": "任务已停用"})
        elif last_status == "running":
            item.update({"status": "running", "message": "任务正在运行"})
        elif last_status == "failed":
            item.update({"status": "failed", "message": "最近一次执行失败"})
        elif not item.get("last_run_at") and not last_status:
            item.update({"status": "never_run", "message": "任务尚未执行"})
        elif item.get("table_exists") is False:
            item.update({"status": "missing_table", "message": "目标表不存在"})
        elif not max_dt:
            item.update({"status": "empty", "message": "目标表暂无数据"})
        elif row_count < int(cfg["min_rows"]):
            item.update({"status": "too_few_rows", "message": f"最新批次只有 {row_count} 条"})
        elif cfg.get("require_latest_trade_date") and max_dt.date() < latest_trade_date:
            item.update({
                "status": "stale_target_date",
                "message": f"目标交易日 {latest_trade_date.isoformat()} 数据未补齐",
            })
        elif (now - max_dt).days > int(cfg.get("max_stale_days") or 4):
            item.update({"status": "stale", "message": "目标表数据过旧"})
        else:
            item.update({"status": "ok", "message": "正常"})

        results.append(item)

    return results


def _required_task_health_cached(force: bool = False) -> tuple[list[dict], bool, int]:
    global _required_health_cache
    now = time.monotonic()
    if not force:
        with _required_health_cache_lock:
            cached = _required_health_cache
        if cached and now - cached[0] < _REQUIRED_HEALTH_CACHE_TTL_SECONDS:
            return copy.deepcopy(cached[1]), True, int(now - cached[0])

    health = _required_task_health()
    with _required_health_cache_lock:
        _required_health_cache = (time.monotonic(), copy.deepcopy(health))
    return health, False, 0


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
            "资金数据": ["capital_flow", "capital_flow_batch_fast"],
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
    required_health, _, _ = _required_task_health_cached()
    return {
        "total": int(stats.get("total") or 0),
        "success": int(stats.get("success") or 0),
        "failed": int(stats.get("failed") or 0),
        "running": int(stats.get("running") or 0),
        "pending": int(stats.get("pending") or 0),
        "enabled": int(stats.get("enabled") or 0),
        "disabled": int(stats.get("disabled") or 0),
        "required_health": required_health,
    }


@router.get("/datasource/required-health")
def get_required_health(force: bool = False):
    """获取关键数据任务健康状态"""
    health, cached, cache_age_seconds = _required_task_health_cached(force=force)
    bad = [item for item in health if item.get("status") not in ("ok", "running")]
    return {
        "data": health,
        "ok": len(bad) == 0,
        "bad_count": len(bad),
        "cached": cached,
        "cache_age_seconds": cache_age_seconds,
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
        logger.debug("Failed to read scheduler run history; falling back to task row.", exc_info=True)

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
    """手动执行数据源"""
    from pathlib import Path

    row = _read_sql("SELECT * FROM st_scheduled_tasks WHERE id = :id", {"id": task_id})
    if not row:
        return {"error": "任务不存在"}
    root = Path(__file__).resolve().parents[3]
    return run_scheduler_task_sync(row[0], root=root, engine=get_engine())


@router.post("/datasource/{task_id}/toggle")
def toggle_task(task_id: int):
    """启用/禁用数据源"""
    row = _read_sql("SELECT id, enabled FROM st_scheduled_tasks WHERE id = :id", {"id": task_id})
    if not row:
        return {"error": "任务不存在"}
    new_enabled = 0 if row[0]["enabled"] == 1 else 1
    update_scheduler_task(get_engine(), task_id, {"enabled": new_enabled})
    return {"id": task_id, "enabled": new_enabled}
