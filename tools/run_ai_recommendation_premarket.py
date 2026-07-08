# -*- coding: utf-8 -*-
"""Run premarket AI recommendations and write execution history.

This wrapper keeps scheduled morning runs visible in the same history panel as
manual runs from the AI recommendation page.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.analysis.sync_analysis_fast import latest_trade_date, previous_trade_date, run_batch
from server.api.routers.hot_data import (
    _recommended_run_history_finish,
    _recommended_run_history_start,
    _recommended_run_history_update,
)
from server.common.batch_db import create_batch_engine


def _wait_for_db(engine, *, attempts: int = 12) -> None:
    if not hasattr(engine, "connect"):
        return
    attempts = max(1, int(attempts))
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1")).scalar()
            return
        except Exception as exc:
            last_error = exc
            delay = min(5.0, 0.5 * (2 ** (attempt - 1)))
            logging.warning(
                "MySQL not ready before AI recommendation attempt=%s/%s; retrying in %.1fs: %s",
                attempt,
                attempts,
                delay,
                exc,
            )
            try:
                engine.dispose()
            except Exception:
                logging.debug("Failed to dispose engine while waiting for MySQL", exc_info=True)
            time.sleep(delay)
    raise RuntimeError(f"MySQL not ready before AI recommendation: {last_error}")


def _resolve_target_trade_date(engine, *, strict_prev_trade_day: bool, execution_time: str, trade_date: str) -> str:
    if trade_date:
        return str(trade_date)[:10]
    if strict_prev_trade_day:
        return previous_trade_date(engine, execution_time or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return latest_trade_date(engine)


def main() -> int:
    parser = argparse.ArgumentParser(description="Premarket AI recommendation batch with run history")
    parser.add_argument("--date", default="", help="Analysis trade date; default latest/previous trading day")
    parser.add_argument("--top-n", type=int, default=80)
    parser.add_argument("--min-score", type=float, default=62.0)
    parser.add_argument("--run-uid", default="", help="Existing st_recommended_run_history.run_uid to update")
    parser.add_argument("--refresh-realtime", action="store_true", help="Refresh realtime quote/flow/index data before screening")
    parser.add_argument("--strict-prev-trade-day", action="store_true")
    parser.add_argument("--execution-time", default="")
    parser.add_argument("--min-kline-coverage", type=float, default=0.80)
    parser.add_argument("--auto-repair-missing-kline", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    os.environ.setdefault("PROBIGA_KLINE_FEATURE_SQL_MODE", "pandas")
    os.environ.setdefault("PROBIGA_BATCH_DB_READ_RETRIES", "8")
    os.environ.setdefault("PROBIGA_KLINE_FEATURE_BATCH_SIZE", "200")
    execution_time = args.execution_time.strip() or datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    engine = create_batch_engine()
    target_trade_date = _resolve_target_trade_date(
        engine,
        strict_prev_trade_day=bool(args.strict_prev_trade_day),
        execution_time=execution_time,
        trade_date=args.date.strip(),
    )

    run_uid = args.run_uid.strip() or _recommended_run_history_start(
        trade_date=target_trade_date,
        min_score=float(args.min_score),
        top_n=int(args.top_n),
        strict_prev_trade_day=bool(args.strict_prev_trade_day),
        execution_time=execution_time,
        message="AI推荐盘前自动预演启动",
    )

    try:
        if args.refresh_realtime:
            _recommended_run_history_update(run_uid, status="running", payload={
                "progress_percent": 8,
                "done_count": 0,
                "message": "正在刷新点击时实时数据...",
                "error": "",
            })
            refresh_cmd = [
                sys.executable,
                str(ROOT / "tools" / "crawl_realtime_batch.py"),
                "--only",
                "all",
                "--min-coverage",
                "0.70",
                "--archive-snapshot",
                "--skip-closed",
                "--json",
            ]
            completed = subprocess.run(
                refresh_cmd,
                cwd=str(ROOT),
                text=True,
                capture_output=True,
                timeout=180,
            )
            if completed.returncode != 0:
                raise RuntimeError(
                    "realtime refresh failed before AI recommendation: "
                    + ((completed.stderr or completed.stdout or "")[-800:])
                )
            _recommended_run_history_update(run_uid, status="running", payload={
                "progress_percent": 12,
                "done_count": 0,
                "message": "实时数据已刷新，正在准备 AI 推荐筛选...",
                "error": "",
            })
        try:
            wait_attempts = int(os.environ.get("PROBIGA_AI_RECOMMEND_DB_WAIT_ATTEMPTS", "12"))
        except (TypeError, ValueError):
            wait_attempts = 12
        _wait_for_db(engine, attempts=wait_attempts)

        def _progress_callback(event: dict) -> None:
            try:
                stage = str(event.get("stage") or "")
                if stage == "done":
                    return
                percent = int(event.get("percent") or 0)
                _recommended_run_history_update(run_uid, status="running", payload={
                    "progress_percent": max(1, min(99, percent)),
                    "done_count": int(event.get("done") or 0),
                    "total": int(event.get("analysis_count") or 0) or None,
                    "passed": int(event.get("recommendation_count") or 0),
                    "flow_date": event.get("flow_date") or "",
                    "hot_date": event.get("hot_date") or "",
                    "market_mood_score": event.get("market_mood_score"),
                    "message": str(event.get("step") or "AI 推荐筛选运行中...")[:500],
                    "error": "",
                })
            except Exception:
                logging.debug("Failed to update AI recommendation progress", exc_info=True)

        stats = run_batch(
            engine=engine,
            trade_date=target_trade_date,
            top_n=int(args.top_n),
            min_score=float(args.min_score),
            strict_prev_trade_day=bool(args.strict_prev_trade_day),
            execution_time=execution_time,
            min_kline_coverage=float(args.min_kline_coverage),
            auto_repair_missing_kline=bool(args.auto_repair_missing_kline),
            progress_callback=_progress_callback,
        )
        payload = {
            "trade_date": stats.trade_date,
            "analysis_count": stats.analysis_count,
            "recommendation_count": stats.recommendation_count,
            "market_mood_score": stats.market_mood_score,
            "flow_date": stats.flow_date,
            "hot_date": stats.hot_date,
            "run_uid": run_uid,
        }
        _recommended_run_history_finish(run_uid, status="done", payload={
            "trade_date": stats.trade_date,
            "total": stats.analysis_count,
            "passed": stats.recommendation_count,
            "flow_date": stats.flow_date,
            "hot_date": stats.hot_date,
            "market_mood_score": stats.market_mood_score,
            "message": f"盘前预演完成，通过 {stats.recommendation_count} 只",
        })
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print(
                f"premarket AI recommendation done: date={stats.trade_date}, "
                f"analysis={stats.analysis_count}, recommendations={stats.recommendation_count}"
            )
        return 0
    except Exception as exc:
        _recommended_run_history_finish(run_uid, status="error", payload={
            "trade_date": target_trade_date,
            "message": "盘前预演失败",
            "error": str(exc)[:500],
        })
        raise


if __name__ == "__main__":
    raise SystemExit(main())
