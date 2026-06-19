# -*- coding: utf-8 -*-
"""定时任务管理 API"""
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
from fastapi import APIRouter, Query
from sqlalchemy import text

from server.api.routers._engine import get_engine

router = APIRouter(tags=["scheduler"])


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


@router.get("/scheduler/tasks")
def list_tasks():
    rows = _read_sql("SELECT * FROM st_scheduled_tasks ORDER BY sort_order")
    now = datetime.now()
    for r in rows:
        r["next_run_at"] = _next_run_at(r, now)
    return {"data": rows, "total": len(rows)}


@router.get("/scheduler/quality")
def scheduler_quality(
    trade_date: str = "",
    include_realtime: bool = False,
):
    from tools.data_quality_check import run_checks

    return run_checks(
        get_engine(),
        trade_date.strip() or None,
        include_realtime=include_realtime,
    )


@router.post("/scheduler/tasks/{task_id}/toggle")
def toggle_task(task_id: int):
    row = _read_sql("SELECT id, enabled FROM st_scheduled_tasks WHERE id = :id", {"id": task_id})
    if not row:
        return {"error": "任务不存在"}
    new_enabled = 0 if row[0]["enabled"] == 1 else 1
    _execute_sql("UPDATE st_scheduled_tasks SET enabled = :e, updated_at = NOW() WHERE id = :id", {"e": new_enabled, "id": task_id})
    return {"id": task_id, "enabled": new_enabled}


@router.post("/scheduler/tasks/{task_id}/cron")
def update_cron(task_id: int, cron_time: str = Query()):
    _execute_sql("UPDATE st_scheduled_tasks SET cron_time = :c, updated_at = NOW() WHERE id = :id", {"c": cron_time, "id": task_id})
    return {"id": task_id, "cron_time": cron_time}


@router.post("/scheduler/tasks/{task_id}/date-param")
def update_date_param(task_id: int, date_param: str = Query(default="")):
    _execute_sql("UPDATE st_scheduled_tasks SET date_param = :d, updated_at = NOW() WHERE id = :id", {"d": date_param, "id": task_id})
    return {"id": task_id, "date_param": date_param}


@router.post("/scheduler/tasks/{task_id}/run")
def run_task_now(task_id: int):
    import subprocess
    import sys
    from pathlib import Path

    row = _read_sql("SELECT * FROM st_scheduled_tasks WHERE id = :id", {"id": task_id})
    if not row:
        return {"error": "任务不存在"}
    task = row[0]

    _execute_sql(
        "UPDATE st_scheduled_tasks SET last_run_status = 'running', last_run_at = NOW(), last_triggered_at = NOW() WHERE id = :id",
        {"id": task_id},
    )

    root = Path(__file__).resolve().parents[3]
    script = root / task["script_path"]

    script_args_raw = (task["script_args"] or "").strip()
    date_param = (task["date_param"] or "").strip()

    if script_args_raw:
        args = script_args_raw.split()
        if date_param:
            args.append(date_param)
    elif date_param:
        args = date_param.split()
    else:
        args = [datetime.now().strftime("%Y-%m-%d")]

    if "run_single_table" in (task["script_path"] or "") and len(args) == 1:
        args.append(datetime.now().strftime("%Y-%m-%d"))

    cmd = [sys.executable, str(script)] + args

    import os
    child_env = os.environ.copy()
    child_env["MYSQL_URL"] = get_engine().url.render_as_string(hide_password=False)
    child_env.setdefault("PYTHONPATH", str(root))

    try:
        start = datetime.now()
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=None, cwd=str(root), env=child_env)
        duration = int((datetime.now() - start).total_seconds())
        if r.returncode == 0:
            status = "success"
        else:
            status = "failed"
        output = (r.stdout or "")[-2000:] + (r.stderr or "")[-2000:]
    except Exception as e:
        status = "failed"
        duration = 0
        output = str(e)

    _execute_sql(
        "UPDATE st_scheduled_tasks SET last_run_status = :s, last_run_output = :o, last_run_duration = :d, updated_at = NOW() WHERE id = :id",
        {"s": status, "o": output, "d": duration, "id": task_id}
    )

    return {"id": task_id, "status": status, "duration": duration, "output": output}


@router.post("/scheduler/tasks/{task_id}/stop")
def stop_task(task_id: int):
    import os
    import signal
    from server.api.scheduler_runtime import _running_lock, _running_procs, _running_task_ids

    row = _read_sql("SELECT id, task_name, last_run_status FROM st_scheduled_tasks WHERE id = :id", {"id": task_id})
    if not row:
        return {"error": "任务不存在"}
    task = row[0]
    if task["last_run_status"] != "running":
        return {"message": f"任务当前状态: {task['last_run_status']}，无需停止"}

    proc = _running_procs.pop(task_id, None)
    if proc and proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            killed = True
        except Exception:
            try:
                proc.kill()
                killed = True
            except Exception:
                killed = False
    else:
        killed = False

    _execute_sql(
        "UPDATE st_scheduled_tasks SET last_run_status = 'stopped', last_run_output = '用户手动停止', updated_at = NOW() WHERE id = :id",
        {"id": task_id},
    )
    with _running_lock:
        _running_task_ids.discard(int(task_id))
    return {"id": task_id, "status": "stopped", "process_killed": killed}
