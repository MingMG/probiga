# -*- coding: utf-8 -*-
"""Low-priority worker for queued AI recommendation jobs.

The dashboard only enqueues jobs.  This worker claims one queued job at a time
and runs the heavy recommendation batch outside the API process.
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import subprocess
import sys
import time as time_module
from datetime import datetime, time
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.common.process_env import build_child_env

logger = logging.getLogger(__name__)


def _ensure_history_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS `st_recommended_run_history` (
                `id` BIGINT NOT NULL AUTO_INCREMENT,
                `run_uid` VARCHAR(40) NOT NULL,
                `trade_date` DATE NULL,
                `status` VARCHAR(20) NOT NULL DEFAULT 'running',
                `min_score` DECIMAL(8,2) NULL,
                `top_n` INT NULL,
                `strict_prev_trade_day` TINYINT(1) NOT NULL DEFAULT 0,
                `execution_time` DATETIME NULL,
                `started_at` DATETIME NULL,
                `finished_at` DATETIME NULL,
                `duration_seconds` INT NULL,
                `progress_percent` INT NULL,
                `done_count` INT NULL,
                `total` INT NULL,
                `passed` INT NULL,
                `flow_date` VARCHAR(20) NULL,
                `hot_date` VARCHAR(20) NULL,
                `market_mood_score` DECIMAL(8,2) NULL,
                `message` VARCHAR(500) NULL,
                `error` VARCHAR(500) NULL,
                `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_rec_run_uid` (`run_uid`),
                KEY `idx_rec_run_date` (`trade_date`, `started_at`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
    _ensure_history_column(
        engine,
        "progress_percent",
        "ALTER TABLE `st_recommended_run_history` ADD COLUMN `progress_percent` INT NULL AFTER `duration_seconds`",
    )
    _ensure_history_column(
        engine,
        "done_count",
        "ALTER TABLE `st_recommended_run_history` ADD COLUMN `done_count` INT NULL AFTER `progress_percent`",
    )


def _ensure_history_column(engine: Engine, column: str, ddl: str) -> None:
    with engine.begin() as conn:
        count = conn.execute(text("""
            SELECT COUNT(*) AS cnt
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'st_recommended_run_history'
              AND column_name = :column
        """), {"column": column}).scalar()
        if int(count or 0) == 0:
            conn.execute(text(ddl))


def _expire_stale(engine: Engine, *, running_minutes: int = 180, queued_hours: int = 24) -> None:
    running_minutes = max(5, min(1440, int(running_minutes)))
    queued_hours = max(1, min(168, int(queued_hours)))
    with engine.begin() as conn:
        conn.execute(text(f"""
            UPDATE st_recommended_run_history
            SET status = 'error',
                finished_at = NOW(),
                duration_seconds = TIMESTAMPDIFF(SECOND, started_at, NOW()),
                progress_percent = 0,
                message = 'AI recommendation worker expired stale running job',
                error = 'stale running recommendation job expired by worker'
            WHERE status = 'running'
              AND started_at < DATE_SUB(NOW(), INTERVAL {running_minutes} MINUTE)
        """))
        conn.execute(text(f"""
            UPDATE st_recommended_run_history
            SET status = 'error',
                finished_at = NOW(),
                duration_seconds = TIMESTAMPDIFF(SECOND, started_at, NOW()),
                progress_percent = 0,
                message = 'AI recommendation queued job expired',
                error = 'queued recommendation job expired by worker'
            WHERE status = 'queued'
              AND started_at < DATE_SUB(NOW(), INTERVAL {queued_hours} HOUR)
        """))


def _claim_next(engine: Engine) -> dict[str, Any] | None:
    with engine.begin() as conn:
        running = conn.execute(text("""
            SELECT run_uid
            FROM st_recommended_run_history
            WHERE status = 'running'
            ORDER BY started_at DESC, id DESC
            LIMIT 1
        """)).first()
        if running:
            logger.info("Existing running recommendation job detected; skip claim.")
            return None

        row = conn.execute(text("""
            SELECT id, run_uid, trade_date, min_score, top_n, strict_prev_trade_day,
                   execution_time
            FROM st_recommended_run_history
            WHERE status = 'queued'
            ORDER BY started_at ASC, id ASC
            LIMIT 1
        """)).mappings().first()
        if not row:
            return None

        result = conn.execute(text("""
            UPDATE st_recommended_run_history
            SET status = 'running',
                started_at = NOW(),
                finished_at = NULL,
                duration_seconds = NULL,
                progress_percent = 10,
                done_count = 0,
                message = 'AI recommendation worker started',
                error = ''
            WHERE run_uid = :run_uid
              AND status = 'queued'
        """), {"run_uid": row["run_uid"]})
        if result.rowcount != 1:
            logger.info("Queued recommendation job was claimed by another worker.")
            return None
        return dict(row)


def _mark_error(engine: Engine, run_uid: str, message: str, error: str) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE st_recommended_run_history
            SET status = 'error',
                finished_at = NOW(),
                duration_seconds = TIMESTAMPDIFF(SECOND, started_at, NOW()),
                progress_percent = 0,
                message = :message,
                error = :error
            WHERE run_uid = :run_uid
              AND status = 'running'
        """), {
            "run_uid": run_uid,
            "message": message[:500],
            "error": error[:500],
        })


def _available_memory_mb() -> int:
    try:
        values: dict[str, int] = {}
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            key, raw = line.split(":", 1)
            values[key] = int(raw.strip().split()[0]) // 1024
        return int(values.get("MemAvailable", 0))
    except Exception:
        return 0


def _process_parent_pid(pid: int) -> int | None:
    try:
        raw = Path(f"/proc/{int(pid)}/stat").read_text(encoding="utf-8", errors="ignore")
        after_name = raw.rsplit(") ", 1)[1]
        return int(after_name.split()[1])
    except Exception:
        return None


def _process_tree_pids(root_pid: int) -> list[int]:
    root_pid = int(root_pid)
    pids = {root_pid}
    changed = True
    while changed:
        changed = False
        try:
            candidates = [int(path.name) for path in Path("/proc").iterdir() if path.name.isdigit()]
        except Exception:
            break
        for pid in candidates:
            if pid in pids:
                continue
            parent = _process_parent_pid(pid)
            if parent in pids:
                pids.add(pid)
                changed = True
    return sorted(pids)


def _process_rss_mb(pid: int) -> float:
    try:
        for line in Path(f"/proc/{int(pid)}/status").read_text(encoding="utf-8", errors="ignore").splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024.0
    except Exception:
        return 0.0
    return 0.0


def _process_tree_rss_mb(root_pid: int) -> float:
    return sum(_process_rss_mb(pid) for pid in _process_tree_pids(root_pid))


def _terminate_process_group(proc: subprocess.Popen[Any]) -> None:
    if proc.poll() is not None:
        return
    try:
        if os.name == "nt":
            proc.terminate()
        else:
            os.killpg(proc.pid, signal.SIGTERM)
    except Exception:
        try:
            proc.terminate()
        except Exception:
            logger.debug("Failed to terminate AI child process", exc_info=True)
    try:
        proc.wait(timeout=30)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        if os.name == "nt":
            proc.kill()
        else:
            os.killpg(proc.pid, signal.SIGKILL)
    except Exception:
        try:
            proc.kill()
        except Exception:
            logger.debug("Failed to kill AI child process", exc_info=True)
    try:
        proc.wait(timeout=10)
    except Exception:
        logger.debug("AI child process did not exit cleanly after kill", exc_info=True)


def _run_child_with_limits(
    cmd: list[str],
    *,
    env: dict[str, str],
    timeout_seconds: int,
    child_rss_limit_mb: int,
) -> tuple[int, str]:
    popen_kwargs: dict[str, Any] = {
        "cwd": str(ROOT),
        "env": env,
        "text": True,
    }
    if os.name != "nt":
        popen_kwargs["start_new_session"] = True
    proc = subprocess.Popen(cmd, **popen_kwargs)
    started = time_module.monotonic()
    rss_limit = max(0, int(child_rss_limit_mb))
    timeout_seconds = max(1, int(timeout_seconds))
    last_logged_rss = 0.0
    while True:
        returncode = proc.poll()
        if returncode is not None:
            return int(returncode), ""
        elapsed = time_module.monotonic() - started
        if elapsed > timeout_seconds:
            _terminate_process_group(proc)
            return 124, f"timeout after {timeout_seconds}s"
        if rss_limit:
            rss_mb = _process_tree_rss_mb(proc.pid)
            if rss_mb > rss_limit:
                _terminate_process_group(proc)
                return 137, f"child rss limit exceeded: {rss_mb:.0f}MB > {rss_limit}MB"
            if rss_mb - last_logged_rss >= 256:
                logger.info("AI recommendation child rss=%.0fMB limit=%sMB", rss_mb, rss_limit)
                last_logged_rss = rss_mb
        time_module.sleep(5)


def _safe_to_run(*, allow_intraday: bool, max_load: float, min_memory_mb: int) -> tuple[bool, str]:
    now = datetime.now().time()
    if not allow_intraday and time(9, 0) <= now <= time(15, 30):
        return False, "inside intraday guard window"
    try:
        load1 = os.getloadavg()[0]
    except (AttributeError, OSError):
        load1 = 0.0
    if load1 > float(max_load):
        return False, f"load too high: {load1:.2f} > {max_load:.2f}"
    available_mb = _available_memory_mb()
    if available_mb and available_mb < int(min_memory_mb):
        return False, f"memory too low: {available_mb}MB < {min_memory_mb}MB"
    return True, "ok"


def _run_claimed_job(
    engine: Engine,
    job: dict[str, Any],
    *,
    refresh_realtime: bool,
    timeout_seconds: int,
    child_rss_limit_mb: int,
) -> int:
    run_uid = str(job["run_uid"])
    trade_date = str(job.get("trade_date") or "")[:10]
    execution_time = str(job.get("execution_time") or "")[:19] or datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    cmd = [
        sys.executable,
        str(ROOT / "tools" / "run_ai_recommendation_premarket.py"),
        "--date",
        trade_date,
        "--top-n",
        str(int(job.get("top_n") or 80)),
        "--min-score",
        str(float(job.get("min_score") or 50.0)),
        "--execution-time",
        execution_time,
        "--min-kline-coverage",
        "0.80",
        "--run-uid",
        run_uid,
        "--json",
        "--auto-repair-missing-kline",
    ]
    if bool(job.get("strict_prev_trade_day")):
        cmd.append("--strict-prev-trade-day")
    if refresh_realtime:
        cmd.append("--refresh-realtime")

    env = build_child_env(ROOT, engine=engine)
    env.update({
        "OMP_NUM_THREADS": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1",
        "NUMEXPR_NUM_THREADS": "1",
        "PROBIGA_KLINE_FEATURE_SQL_MODE": os.environ.get("PROBIGA_KLINE_FEATURE_SQL_MODE", "pandas"),
        "PROBIGA_KLINE_FEATURE_BATCH_SIZE": os.environ.get("PROBIGA_KLINE_FEATURE_BATCH_SIZE", "80"),
        "PROBIGA_KLINE_FEATURE_STREAM_BATCHES": os.environ.get("PROBIGA_KLINE_FEATURE_STREAM_BATCHES", "1"),
        "PROBIGA_BATCH_DB_READ_RETRIES": os.environ.get("PROBIGA_BATCH_DB_READ_RETRIES", "12"),
    })
    logger.info("Running queued AI recommendation job run_uid=%s date=%s", run_uid, trade_date)
    try:
        returncode, failure_reason = _run_child_with_limits(
            cmd,
            env=env,
            timeout_seconds=int(timeout_seconds),
            child_rss_limit_mb=int(child_rss_limit_mb),
        )
    except Exception as exc:
        _mark_error(engine, run_uid, "AI recommendation worker failed to start", str(exc))
        raise
    if returncode != 0:
        _mark_error(
            engine,
            run_uid,
            "AI recommendation worker failed",
            failure_reason or f"returncode={returncode}",
        )
    return int(returncode)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run queued AI recommendation jobs safely.")
    parser.add_argument("--once", action="store_true", help="Process at most one queued job.")
    parser.add_argument("--force", action="store_true", help="Run even when worker env flag is disabled.")
    parser.add_argument("--allow-intraday", action="store_true", help="Allow running during 09:00-15:30.")
    parser.add_argument("--refresh-realtime", action="store_true", help="Refresh realtime data before analysis.")
    parser.add_argument("--max-load", type=float, default=float(os.environ.get("PROBIGA_AI_WORKER_MAX_LOAD", "1.20")))
    parser.add_argument("--min-memory-mb", type=int, default=int(os.environ.get("PROBIGA_AI_WORKER_MIN_MEMORY_MB", "1200")))
    parser.add_argument("--timeout-seconds", type=int, default=int(os.environ.get("PROBIGA_AI_WORKER_TIMEOUT_SECONDS", "7200")))
    parser.add_argument("--child-rss-limit-mb", type=int, default=int(os.environ.get("PROBIGA_AI_WORKER_CHILD_RSS_LIMIT_MB", "1800")))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    if not args.force and os.environ.get("PROBIGA_AI_RECOMMEND_WORKER_ENABLED", "").strip() != "1":
        logger.info("AI recommendation worker disabled; set PROBIGA_AI_RECOMMEND_WORKER_ENABLED=1 or pass --force.")
        return 0

    ok, reason = _safe_to_run(
        allow_intraday=bool(args.allow_intraday),
        max_load=float(args.max_load),
        min_memory_mb=int(args.min_memory_mb),
    )
    if not ok:
        logger.info("AI recommendation worker guard skipped run: %s", reason)
        return 0

    engine = create_batch_engine()
    _ensure_history_table(engine)
    _expire_stale(engine)
    job = _claim_next(engine)
    if not job:
        logger.info("No queued AI recommendation job.")
        return 0
    return _run_claimed_job(
        engine,
        job,
        refresh_realtime=bool(args.refresh_realtime),
        timeout_seconds=int(args.timeout_seconds),
        child_rss_limit_mb=int(args.child_rss_limit_mb),
    )


if __name__ == "__main__":
    raise SystemExit(main())
