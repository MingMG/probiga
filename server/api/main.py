# -*- coding: utf-8 -*-
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text

from server.api.routers import health, hot_data, notify, scheduler
from server.api.routers._engine import get_engine

logger = logging.getLogger("scheduler_daemon")
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)

ZOMBIE_TIMEOUT_HOURS = 12

_running_procs: dict[int, subprocess.Popen] = {}


def _run_task(row: dict, ROOT: Path, engine) -> None:
    """执行单个定时任务"""
    task_id = row["id"]
    task_name = row["task_name"]
    script_path = row["script_path"] or ""
    script_args_raw = str(row["script_args"] or "").strip()
    cron_time = str(row["cron_time"] or "17:10")
    date_param_raw = str(row["date_param"] or "").strip()
    interval_minutes = int(row.get("interval_minutes") or 0)
    today = datetime.now().strftime("%Y-%m-%d")

    script = ROOT / script_path
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
    # 使用引擎实际连接的URL，确保子进程能正确连接数据库
    child_env["MYSQL_URL"] = engine.url.render_as_string(hide_password=False)
    child_env.setdefault("PYTHONPATH", str(ROOT))

    with engine.begin() as u:
        u.execute(
            text("UPDATE st_scheduled_tasks SET last_run_status='running', last_run_at=NOW(), last_triggered_at=NOW() WHERE id=:id"),
            {"id": task_id},
        )

    start_t = datetime.now()
    try:
        popen_kwargs = dict(
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, cwd=str(ROOT), env=child_env,
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
    except Exception as e:
        _running_procs.pop(task_id, None)
        status = "failed"
        duration = 0
        output = str(e)

    with engine.begin() as u:
        u.execute(
            text("UPDATE st_scheduled_tasks SET last_run_status=:s, last_run_output=:o, last_run_duration=:d, updated_at=NOW() WHERE id=:id"),
            {"s": status, "o": output, "d": duration, "id": task_id},
        )
    logger.info("任务 %s 完成: %s (%ds)", task_name, status, duration)


def _catchup_on_startup(ROOT: Path):
    """启动时将卡在 running 的任务重置"""
    try:
        engine = get_engine()
        with engine.begin() as u:
            u.execute(
                text("UPDATE st_scheduled_tasks SET last_run_status='', last_run_output=CONCAT(IFNULL(last_run_output,''), ' [启动重置]') "
                     "WHERE last_run_status='running' AND last_triggered_at < NOW() - INTERVAL 30 MINUTE")
            )
        logger.info("启动重置已完成")
    except Exception as e:
        logger.error("启动补跑异常: %s", e)


def _check_and_run_tasks():
    """后台调度线程：每分钟检查一次，到点执行任务"""
    ROOT = Path(__file__).resolve().parents[2]
    _catchup_on_startup(ROOT)
    _startup_time = datetime.now()
    _catchup_enabled = True
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
                    tid = row[0]
                    tname = row[1]
                    logger.warning("检测到僵尸任务: %s (id=%d, started=%s), 强制清理", tname, tid, row[2])
                    proc = _running_procs.pop(tid, None)
                    if proc and proc.poll() is None:
                        try:
                            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                        except Exception:
                            try:
                                proc.kill()
                            except Exception:
                                pass
                    with engine.begin() as u:
                        u.execute(
                            text("UPDATE st_scheduled_tasks SET last_run_status='timeout', last_run_output='任务超时自动清理(>%dh)', updated_at=NOW() WHERE id=:id"),
                            {"h": ZOMBIE_TIMEOUT_HOURS, "id": tid},
                        )
            except Exception as e:
                logger.warning("僵尸检测异常: %s", e)

            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT id, task_name, script_path, script_args, cron_time, interval_minutes, "
                         "enabled, date_param, last_triggered_at FROM st_scheduled_tasks WHERE enabled = 1 ORDER BY sort_order")
                )
                rows = [dict(zip(result.keys(), row)) for row in result.fetchall()]
            now = datetime.now()
            today = now.strftime("%Y-%m-%d")
            time_str = now.strftime("%H:%M")

            for row in rows:
                task_id = row["id"]
                task_name = row["task_name"]
                cron_time = str(row["cron_time"] or "17:10")
                interval_minutes = int(row.get("interval_minutes") or 0)
                last_triggered = row.get("last_triggered_at")

                if interval_minutes > 0:
                    ref_time = last_triggered or row.get("last_run_at")
                    if ref_time:
                        if isinstance(ref_time, str):
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
                        # 补漏触发：启动后2分钟内，cron时间已过且今天还没跑过
                        catchup_expired = (_startup_time and (now - _startup_time).total_seconds() > 120)
                        if catchup_expired:
                            continue
                        try:
                            cron_parts = cron_time.split(":")
                            cur_parts = time_str.split(":")
                            cron_min = int(cron_parts[0]) * 60 + int(cron_parts[1])
                            cur_min = int(cur_parts[0]) * 60 + int(cur_parts[1])
                            if cur_min <= cron_min:
                                continue
                        except Exception:
                            continue
                        trig_str = str(last_triggered or "")[:10]
                        if trig_str == now.strftime("%Y-%m-%d"):
                            continue
                        logger.info("补漏触发(启动后%.0fs): %s (cron=%s, now=%s)", (now - _startup_time).total_seconds(), task_name, cron_time, time_str)

                if task_id in _running_procs and _running_procs[task_id].poll() is None:
                    logger.warning("任务 %s 仍在运行，跳过本次触发", task_name)
                    continue

                logger.info("执行定时任务: %s (cron=%s, now=%s)", task_name, cron_time, time_str)
                _run_task(row, ROOT, engine)

        except Exception as e:
            logger.error("调度线程异常: %s", e)

        time.sleep(60)


_scheduler_thread: threading.Thread | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    global _scheduler_thread
    _scheduler_thread = threading.Thread(target=_check_and_run_tasks, daemon=True, name="scheduler-daemon")
    _scheduler_thread.start()
    logger.info("定时调度线程已启动")
    yield


app = FastAPI(
    title="ProBigA",
    description="数据分析平台 API",
    lifespan=lifespan,
)

app.include_router(health.router, prefix="/api")
app.include_router(notify.router, prefix="/api")
app.include_router(hot_data.router, prefix="/api")
app.include_router(scheduler.router, prefix="/api")

static_dir = Path(__file__).resolve().parent.parent / "static"


@app.get("/", response_class=HTMLResponse)
def index():
    index_path = static_dir / "index.html"
    if index_path.is_file():
        return HTMLResponse(content=index_path.read_text(encoding="utf-8"))
    return HTMLResponse(content="<h1>ProBigA</h1>")


if static_dir.is_dir():
    app.mount("/static", StaticFiles(directory=str(static_dir), html=False), name="static")
