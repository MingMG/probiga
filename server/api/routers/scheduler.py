# -*- coding: utf-8 -*-
"""定时任务管理 API"""
import copy
import threading
import time
from datetime import date, datetime, timedelta

from fastapi import APIRouter, Query
from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.common.scheduler_tasks import update_scheduler_task
from server.common.sql_reader import read_sql_rows
from server.api.scheduler_runtime import (
    launch_scheduler_task,
    read_scheduler_heartbeat,
    request_stop_owned_scheduler_task,
    scheduler_task_owned_by_current_host,
    scheduler_runtime_info,
    strategy_governance_task_block_reason,
)

router = APIRouter(tags=["scheduler"])
_QUALITY_CACHE_TTL_SECONDS = 300
_QUALITY_REALTIME_CACHE_TTL_SECONDS = 60
_quality_cache_lock = threading.Lock()
_quality_cache: dict[tuple[str, bool, bool], tuple[float, dict]] = {}

def _read_sql(sql: str, params: dict = None) -> list[dict]:
    return read_sql_rows(get_engine(), sql, params, context="scheduler")


def _coerce_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.strptime(str(value)[:19], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _next_run_at(row: dict, now: datetime) -> str:
    interval_minutes = int(row.get("interval_minutes") or 0)
    if interval_minutes > 0:
        ref_time = _coerce_datetime(row.get("last_triggered_at")) or _coerce_datetime(row.get("last_run_at"))
        if not ref_time:
            return now.strftime("%Y-%m-%d %H:%M")
        next_dt = ref_time + timedelta(minutes=interval_minutes)
        if next_dt <= now:
            next_dt = now
        return next_dt.strftime("%Y-%m-%d %H:%M")

    cron_time = str(row.get("cron_time") or "17:10").strip()
    try:
        h, m = cron_time.split(":")
        next_dt = datetime(now.year, now.month, now.day, int(h), int(m))
        if next_dt <= now:
            next_dt += timedelta(days=1)
        return next_dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return ""


def _runtime_payload() -> dict:
    info = scheduler_runtime_info()
    heartbeat = read_scheduler_heartbeat()
    poll_seconds = int(info.get("scheduler_poll_seconds") or 60)
    heartbeat_age = None
    if heartbeat and heartbeat.get("heartbeat_age_seconds") is not None:
        try:
            heartbeat_age = int(heartbeat["heartbeat_age_seconds"])
        except Exception:
            heartbeat_age = None
    daemon_online = bool(heartbeat and heartbeat_age is not None and heartbeat_age <= max(180, poll_seconds * 3))
    embedded_enabled = bool(info.get("embedded_scheduler_enabled"))
    embedded_running = bool(info.get("embedded_scheduler_running"))

    if embedded_enabled and embedded_running:
        status_text = "内嵌调度运行中：重启 API 会中断定时任务，建议改用独立调度进程。"
    elif daemon_online:
        status_text = "独立调度进程在线：重启 API 服务不会中断定时任务。"
    elif embedded_enabled:
        status_text = "内嵌调度已开启但未检测到运行线程，请检查 API 启动状态。"
    else:
        status_text = "内嵌调度已关闭，但未检测到独立调度心跳，请单独启动调度进程。"

    return {
        **info,
        "standalone_scheduler_online": daemon_online,
        "api_restart_safe": bool((not embedded_enabled) and daemon_online),
        "scheduler_daemon_command": "python tools/run_scheduler_daemon.py",
        "status_text": status_text,
        "heartbeat": heartbeat,
    }


def _table_exists_conn(conn, table_name: str) -> bool:
    value = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name
            """
        ),
        {"table_name": table_name},
    ).scalar()
    return bool(value)


def _quality_overall(checks: list[dict]) -> str:
    statuses = [str(item.get("status") or "").upper() for item in checks]
    return "FAIL" if "FAIL" in statuses else "WARN" if "WARN" in statuses else "PASS"


def _scheduler_quality_fast(engine, trade_date: str | None = None, include_realtime: bool = False) -> dict:
    generated_at = datetime.now().isoformat(timespec="seconds")
    checks: list[dict] = []
    with engine.connect() as conn:
        latest_trade_date = conn.execute(
            text(
                """
                SELECT trade_date
                FROM sm_stock_kline
                WHERE k_type = 1 AND adjust_type = 0
                ORDER BY trade_date DESC
                LIMIT 1
                """
            )
        ).scalar()
        resolved_trade_date = (trade_date or str(latest_trade_date or "")[:10]).strip()
        checks.append(
            {
                "name": "latest_trade_date",
                "status": "PASS" if resolved_trade_date else "FAIL",
                "message": f"最新日K交易日 {resolved_trade_date or '-'}",
                "details": {"trade_date": resolved_trade_date},
            }
        )

        scheduler_rows = conn.execute(
            text(
                """
                SELECT task_name, task_type, last_run_status, last_run_at,
                       TIMESTAMPDIFF(MINUTE, last_run_at, NOW()) AS age_minutes
                FROM st_scheduled_tasks
                WHERE enabled = 1
                  AND (
                    COALESCE(last_run_status, '') IN ('failed', 'timeout', 'stopped')
                    OR (last_run_status = 'running'
                        AND (last_run_at IS NULL OR last_run_at < NOW() - INTERVAL 30 MINUTE))
                  )
                ORDER BY last_run_at DESC
                LIMIT 10
                """
            )
        ).mappings().all()
        bad_tasks = [dict(row) for row in scheduler_rows]
        checks.append(
            {
                "name": "scheduler_health",
                "status": "WARN" if bad_tasks else "PASS",
                "message": "调度任务状态正常" if not bad_tasks else f"调度异常任务 {len(bad_tasks)} 个",
                "details": {"bad_tasks": bad_tasks},
            }
        )

        coverage_rows: list[dict] = []
        if _table_exists_conn(conn, "sys_data_coverage"):
            coverage_rows = [
                dict(row)
                for row in conn.execute(
                    text(
                        """
                        SELECT c.dataset, c.trade_date, c.expected_count, c.actual_count,
                               c.missing_count, c.coverage_ratio, c.status, c.checked_at
                        FROM sys_data_coverage c
                        JOIN (
                            SELECT dataset, MAX(trade_date) AS trade_date
                            FROM sys_data_coverage
                            WHERE provider = 'gj_qmt'
                            GROUP BY dataset
                        ) latest
                          ON latest.dataset = c.dataset AND latest.trade_date = c.trade_date
                        WHERE c.provider = 'gj_qmt'
                        ORDER BY c.trade_date DESC, c.dataset
                        """
                    )
                ).mappings().all()
            ]
        coverage_statuses = {str(row.get("status") or "").upper() for row in coverage_rows}
        coverage_status = (
            "FAIL" if "FAIL" in coverage_statuses else "WARN" if "WARN" in coverage_statuses else "PASS"
        ) if coverage_rows else "WARN"

        gap_rows: list[dict] = []
        deferred_gap_rows: list[dict] = []
        if _table_exists_conn(conn, "sys_data_gap"):
            gap_rows = [
                dict(row)
                for row in conn.execute(
                    text(
                        """
                        SELECT dataset, COUNT(*) AS pending_count, MAX(gap_start) AS latest_gap_start
                        FROM sys_data_gap
                        WHERE provider = 'gj_qmt'
                          AND status IN ('PENDING', 'RETRYING')
                          AND COALESCE(reason, '') <> 'coverage_below_threshold_history_backfill_deferred'
                        GROUP BY dataset
                        ORDER BY latest_gap_start DESC
                        """
                    )
                ).mappings().all()
            ]
            deferred_gap_rows = [
                dict(row)
                for row in conn.execute(
                    text(
                        """
                        SELECT dataset, COUNT(*) AS pending_count, MAX(gap_start) AS latest_gap_start
                        FROM sys_data_gap
                        WHERE provider = 'gj_qmt'
                          AND status IN ('PENDING', 'RETRYING')
                          AND COALESCE(reason, '') = 'coverage_below_threshold_history_backfill_deferred'
                        GROUP BY dataset
                        ORDER BY latest_gap_start DESC
                        """
                    )
                ).mappings().all()
            ]
        if gap_rows and coverage_status == "PASS":
            coverage_status = "WARN"
        checks.append(
            {
                "name": "qmt_coverage",
                "status": coverage_status,
                "message": "国金QMT覆盖率正常" if coverage_status == "PASS" else "国金QMT覆盖率存在缺口",
                "details": {
                    "coverage": coverage_rows,
                    "open_gaps": gap_rows,
                    "deferred_history_gaps": deferred_gap_rows,
                },
            }
        )

        quality_rows: list[dict] = []
        if _table_exists_conn(conn, "sys_data_quality_result"):
            quality_rows = [
                dict(row)
                for row in conn.execute(
                    text(
                        """
                        SELECT dataset, rule_name, status, checked_rows, failed_rows, checked_at
                        FROM sys_data_quality_result
                        WHERE provider = 'gj_qmt'
                          AND checked_at = (
                            SELECT MAX(checked_at)
                            FROM sys_data_quality_result
                            WHERE provider = 'gj_qmt'
                          )
                        ORDER BY dataset, rule_name
                        """
                    )
                ).mappings().all()
            ]
        non_blocking_quality_rows = [
            row
            for row in quality_rows
            if str(row.get("status") or "").upper() == "WARN"
            and str(row.get("dataset") or "") == "sm_stock_current"
            and str(row.get("rule_name") or "") == "current_daily_close_consistency"
            and int(row.get("checked_rows") or 0) == 0
            and int(row.get("failed_rows") or 0) == 0
        ]
        blocking_quality_rows = [row for row in quality_rows if row not in non_blocking_quality_rows]
        quality_statuses = {str(row.get("status") or "").upper() for row in blocking_quality_rows}
        quality_status = (
            "FAIL" if "FAIL" in quality_statuses else "WARN" if "WARN" in quality_statuses else "PASS"
        ) if quality_rows else "WARN"
        checks.append(
            {
                "name": "qmt_quality_result",
                "status": quality_status,
                "message": "国金QMT质量规则正常" if quality_status == "PASS" else "国金QMT质量规则存在告警或暂无结果",
                "details": {
                    "latest_results": quality_rows,
                    "non_blocking_warnings": non_blocking_quality_rows,
                },
            }
        )

        if include_realtime:
            realtime_data = {"latest_snapshot": None, "stock_count": 0}
            if _table_exists_conn(conn, "sm_stock_current"):
                realtime = conn.execute(
                    text(
                        """
                        SELECT MAX(snapshot_at) AS latest_snapshot,
                               COUNT(DISTINCT stock_code) AS stock_count,
                               COUNT(*) AS row_count
                        FROM sm_stock_current
                        """
                    )
                ).mappings().first()
                realtime_data = dict(realtime or {})
            checks.append(
                {
                    "name": "realtime_snapshot",
                    "status": "PASS" if int(realtime_data.get("stock_count") or 0) >= 1000 else "WARN",
                    "message": f"实时快照覆盖 {int(realtime_data.get('stock_count') or 0)} 只",
                    "details": realtime_data,
                }
            )

    return {
        "status": _quality_overall(checks),
        "trade_date": resolved_trade_date,
        "generated_at": generated_at,
        "mode": "fast",
        "checks": checks,
    }


@router.get("/scheduler/tasks")
def list_tasks():
    rows = _read_sql("SELECT * FROM st_scheduled_tasks ORDER BY sort_order")
    now = datetime.now()
    for r in rows:
        r["next_run_at"] = _next_run_at(r, now)
    return {"data": rows, "total": len(rows), "runtime": _runtime_payload()}


@router.get("/scheduler/quality")
def scheduler_quality(
    trade_date: str = "",
    include_realtime: bool = False,
    force: bool = False,
    fast: bool = True,
):
    normalized_date = trade_date.strip()
    cache_key = (normalized_date, bool(include_realtime), bool(fast))
    ttl_seconds = _QUALITY_REALTIME_CACHE_TTL_SECONDS if include_realtime else _QUALITY_CACHE_TTL_SECONDS
    now = time.monotonic()
    if not force:
        with _quality_cache_lock:
            cached = _quality_cache.get(cache_key)
        if cached and now - cached[0] < ttl_seconds:
            payload = copy.deepcopy(cached[1])
            payload["cached"] = True
            payload["cache_age_seconds"] = int(now - cached[0])
            return payload

    engine = get_engine()
    if fast:
        payload = _scheduler_quality_fast(engine, normalized_date or None, include_realtime=include_realtime)
    else:
        from tools.data_quality_check import run_checks

        payload = run_checks(
            engine,
            normalized_date or None,
            include_realtime=include_realtime,
        )
        payload["mode"] = "full"
    payload["cached"] = False
    payload["cache_age_seconds"] = 0
    with _quality_cache_lock:
        _quality_cache[cache_key] = (time.monotonic(), copy.deepcopy(payload))
    return payload


@router.post("/scheduler/tasks/{task_id}/toggle")
def toggle_task(task_id: int):
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
    update_scheduler_task(get_engine(), task_id, {"enabled": new_enabled})
    return {"id": task_id, "enabled": new_enabled}


@router.post("/scheduler/tasks/{task_id}/cron")
def update_cron(task_id: int, cron_time: str = Query()):
    update_scheduler_task(get_engine(), task_id, {"cron_time": cron_time})
    return {"id": task_id, "cron_time": cron_time}


@router.post("/scheduler/tasks/{task_id}/date-param")
def update_date_param(task_id: int, date_param: str = Query(default="")):
    update_scheduler_task(get_engine(), task_id, {"date_param": date_param})
    return {"id": task_id, "date_param": date_param}


@router.post("/scheduler/tasks/{task_id}/run")
def run_task_now(task_id: int):
    from pathlib import Path

    row = _read_sql("SELECT * FROM st_scheduled_tasks WHERE id = :id", {"id": task_id})
    if not row:
        return {"error": "任务不存在"}
    root = Path(__file__).resolve().parents[3]
    return launch_scheduler_task(row[0], root=root, engine=get_engine())


@router.post("/scheduler/tasks/{task_id}/stop")
def stop_task(task_id: int):
    row = _read_sql(
        "SELECT id, task_name, task_type, script_path, last_run_status "
        "FROM st_scheduled_tasks WHERE id = :id",
        {"id": task_id},
    )
    if not row:
        return {"error": "任务不存在"}
    task = row[0]
    if not scheduler_task_owned_by_current_host(task):
        return {
            "id": task_id,
            "status": "delegated_to_other_host",
            "process_killed": False,
        }
    if task["last_run_status"] != "running":
        return {"message": f"任务当前状态: {task['last_run_status']}，无需停止"}
    result = request_stop_owned_scheduler_task(task_id)
    return {"id": task_id, **result}
