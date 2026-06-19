# -*- coding: utf-8 -*-
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

from server.api.routers._engine import get_engine
from server.common.config import get_api_mysql_pool_config, get_scheduler_runtime_config

logger = logging.getLogger("scheduler_daemon")
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)

ZOMBIE_TIMEOUT_HOURS = 12

_running_procs: dict[int, subprocess.Popen] = {}
_running_task_ids: set[int] = set()
_running_lock = threading.Lock()
_task_semaphore: threading.Semaphore | None = None
_scheduler_thread: threading.Thread | None = None


def _get_task_semaphore() -> threading.Semaphore:
    global _task_semaphore
    if _task_semaphore is None:
        limit = int(get_scheduler_runtime_config()["max_concurrent_tasks"])
        _task_semaphore = threading.Semaphore(limit)
    return _task_semaphore


def scheduler_runtime_info() -> dict[str, int | bool]:
    scheduler = get_scheduler_runtime_config()
    api_pool = get_api_mysql_pool_config()
    return {
        "embedded_scheduler_enabled": bool(scheduler["embedded_enabled"]),
        "embedded_scheduler_running": bool(_scheduler_thread and _scheduler_thread.is_alive()),
        "scheduler_max_concurrent_tasks": int(scheduler["max_concurrent_tasks"]),
        "scheduler_poll_seconds": int(scheduler["poll_seconds"]),
        "api_mysql_pool_size": int(api_pool["pool_size"]),
        "api_mysql_max_overflow": int(api_pool["max_overflow"]),
        "api_mysql_pool_recycle": int(api_pool["pool_recycle"]),
    }


def _run_task(row: dict, root: Path, engine) -> None:
    """执行单个定时任务"""
    task_id = row["id"]
    task_name = row["task_name"]
    script_path = row["script_path"] or ""
    script_args_raw = str(row["script_args"] or "").strip()
    date_param_raw = str(row["date_param"] or "").strip()
    today = datetime.now().strftime("%Y-%m-%d")

    script = root / script_path
    if not script.exists():
        logger.warning("脚本不存在: %s", script)
        return

    if script_args_raw:
        args = script_args_raw.split()
    elif date_param_raw:
        if ":" in date_param_raw:
            parts = date_param_raw.split(":")
            args = [parts[0], parts[1]] if len(parts) >= 2 else [today]
        else:
            args = [date_param_raw]
    else:
        args = [today]

    if "run_single_table" in script_path and len(args) == 1:
        args.append(today)

    cmd = [sys.executable, str(script)] + args

    child_env = os.environ.copy()
    child_env["MYSQL_URL"] = engine.url.render_as_string(hide_password=False)
    child_env.setdefault("PYTHONPATH", str(root))

    with engine.begin() as conn:
        conn.execute(
            text("UPDATE st_scheduled_tasks SET last_run_status='running', last_run_at=NOW(), last_triggered_at=NOW() WHERE id=:id"),
            {"id": task_id},
        )

    start_t = datetime.now()
    try:
        popen_kwargs = dict(
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(root),
            env=child_env,
        )
        if os.name == "nt":
            popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            popen_kwargs["preexec_fn"] = os.setsid
        proc = subprocess.Popen(cmd, **popen_kwargs)
        _running_procs[task_id] = proc
        stdout, stderr = proc.communicate()
        _running_procs.pop(task_id, None)

        duration = int((datetime.now() - start_t).total_seconds())
        status = "success" if proc.returncode == 0 else "failed"
        output = (stdout or "")[-3000:] + "\n---STDERR---\n" + (stderr or "")[-2000:]
    except Exception as exc:
        _running_procs.pop(task_id, None)
        status = "failed"
        duration = 0
        output = str(exc)

    with engine.begin() as conn:
        conn.execute(
            text("UPDATE st_scheduled_tasks SET last_run_status=:s, last_run_output=:o, last_run_duration=:d, updated_at=NOW() WHERE id=:id"),
            {"s": status, "o": output, "d": duration, "id": task_id},
        )
    logger.info("任务 %s 完成: %s (%ds)", task_name, status, duration)


def _run_task_async(row: dict, root: Path, engine) -> None:
    with _get_task_semaphore():
        try:
            _run_task(row, root, engine)
        finally:
            with _running_lock:
                _running_task_ids.discard(int(row["id"]))


def _catchup_on_startup() -> None:
    """启动时将卡在 running 的任务重置"""
    try:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE st_scheduled_tasks SET last_run_status='', last_run_output=CONCAT(IFNULL(last_run_output,''), ' [启动重置]') "
                     "WHERE last_run_status='running' AND last_triggered_at < NOW() - INTERVAL 30 MINUTE")
            )
        logger.info("启动重置已完成")
    except Exception as exc:
        logger.error("启动补跑异常: %s", exc)


def _check_and_run_tasks() -> None:
    """后台调度线程：每分钟检查一次，到点执行任务"""
    root = Path(__file__).resolve().parents[2]
    _catchup_on_startup()
    startup_time = datetime.now()
    poll_seconds = int(get_scheduler_runtime_config()["poll_seconds"])
    while True:
        try:
            engine = get_engine()

            try:
                with engine.connect() as conn:
                    stale = conn.execute(
                        text("SELECT id, task_name, last_run_at FROM st_scheduled_tasks "
                             "WHERE last_run_status = 'running' AND last_run_at < NOW() - INTERVAL :h HOUR"),
                        {"h": ZOMBIE_TIMEOUT_HOURS},
                    ).fetchall()
                for row in stale:
                    task_id = row[0]
                    task_name = row[1]
                    logger.warning("检测到僵尸任务: %s (id=%d, started=%s), 强制清理", task_name, task_id, row[2])
                    proc = _running_procs.pop(task_id, None)
                    if proc and proc.poll() is None:
                        try:
                            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                        except Exception:
                            try:
                                proc.kill()
                            except Exception:
                                pass
                    with engine.begin() as conn:
                        conn.execute(
                            text("UPDATE st_scheduled_tasks SET last_run_status='timeout', last_run_output='任务超时自动清理(>%dh)', updated_at=NOW() WHERE id=:id"),
                            {"h": ZOMBIE_TIMEOUT_HOURS, "id": task_id},
                        )
                    with _running_lock:
                        _running_task_ids.discard(int(task_id))
            except Exception as exc:
                logger.warning("僵尸检测异常: %s", exc)

            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT id, task_name, script_path, script_args, cron_time, interval_minutes, "
                         "enabled, date_param, last_run_at, last_triggered_at "
                         "FROM st_scheduled_tasks WHERE enabled = 1 ORDER BY sort_order")
                )
                rows = [dict(zip(result.keys(), row)) for row in result.fetchall()]

            now = datetime.now()
            time_str = now.strftime("%H:%M")

            for row in rows:
                task_id = row["id"]
                task_name = row["task_name"]
                cron_time = str(row["cron_time"] or "17:10")
                interval_minutes = int(row.get("interval_minutes") or 0)
                last_triggered = row.get("last_triggered_at")

                if interval_minutes > 0:
                    ref_time = last_triggered or row.get("last_run_at")
                    if ref_time and isinstance(ref_time, str):
                        try:
                            ref_time = datetime.strptime(ref_time[:19], "%Y-%m-%d %H:%M:%S")
                        except Exception:
                            ref_time = None
                    if ref_time:
                        elapsed = (now - ref_time).total_seconds() / 60
                        if elapsed < interval_minutes:
                            continue
                else:
                    if time_str != cron_time:
                        # 补漏逻辑：启动后检查今天是否有未执行的任务
                        try:
                            cron_parts = cron_time.split(":")
                            cur_parts = time_str.split(":")
                            cron_min = int(cron_parts[0]) * 60 + int(cron_parts[1])
                            cur_min = int(cur_parts[0]) * 60 + int(cur_parts[1])
                            # 当前时间还没到 cron 时间，跳过
                            if cur_min <= cron_min:
                                continue
                        except Exception:
                            continue
                        # 今天已经触发过，跳过
                        if str(last_triggered or "")[:10] == now.strftime("%Y-%m-%d"):
                            continue
                        catchup_window = (now - startup_time).total_seconds() <= 120
                        logger.info("补漏触发(启动后%.0fs): %s (cron=%s, now=%s)", (now - startup_time).total_seconds(), task_name, cron_time, time_str)

                with _running_lock:
                    task_running = task_id in _running_task_ids
                    if not task_running:
                        proc = _running_procs.get(task_id)
                        task_running = bool(proc and proc.poll() is None)
                    if task_running:
                        logger.warning("任务 %s 仍在运行，跳过本次触发", task_name)
                        continue
                    _running_task_ids.add(int(task_id))

                logger.info("执行定时任务: %s (cron=%s, now=%s)", task_name, cron_time, time_str)
                threading.Thread(
                    target=_run_task_async,
                    args=(row, root, engine),
                    daemon=True,
                    name=f"scheduler-task-{task_id}",
                ).start()

        except Exception as exc:
            logger.error("调度线程异常: %s", exc)

        time.sleep(poll_seconds)


def start_embedded_scheduler() -> threading.Thread | None:
    global _scheduler_thread
    runtime = get_scheduler_runtime_config()
    if not runtime["embedded_enabled"]:
        logger.info("内嵌调度已禁用；如需独立调度进程，请运行 tools/run_scheduler_daemon.py")
        return None
    if _scheduler_thread and _scheduler_thread.is_alive():
        return _scheduler_thread
    _scheduler_thread = threading.Thread(target=_check_and_run_tasks, daemon=True, name="scheduler-daemon")
    _scheduler_thread.start()
    logger.info(
        "定时调度线程已启动 (max_concurrent_tasks=%s, poll=%ss)",
        runtime["max_concurrent_tasks"],
        runtime["poll_seconds"],
    )
    return _scheduler_thread


def run_scheduler_forever() -> None:
    """Run the scheduler loop as a standalone process."""
    runtime = get_scheduler_runtime_config()
    logger.info(
        "独立调度进程启动 (max_concurrent_tasks=%s, poll=%ss)",
        runtime["max_concurrent_tasks"],
        runtime["poll_seconds"],
    )
    _check_and_run_tasks()
