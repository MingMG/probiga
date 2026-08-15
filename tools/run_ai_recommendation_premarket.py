# -*- coding: utf-8 -*-
"""Run premarket AI recommendations and write execution history.

This wrapper keeps scheduled morning runs visible in the same history panel as
manual runs from the AI recommendation page.
"""
from __future__ import annotations

import argparse
import inspect
import json
import logging
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, time as datetime_time
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.analysis.sync_analysis_fast import latest_trade_date, previous_trade_date, run_batch
from biz.market_context.external_market import fetch_external_market_snapshot, store_external_market_snapshot
from biz.premarket.theme_forecast import (
    format_forecast_markdown,
    mark_forecast_delivery,
    run_premarket_theme_forecast,
)
from integrations.wecom.webhook import send_markdown
from server.api.routers.hot_data import (
    _recommended_run_history_finish,
    _recommended_run_history_start,
    _recommended_run_history_update,
)
from server.common.batch_db import create_batch_engine
from server.common.config import get_wecom_webhook
from tools.repair_recommendation_data import repair_target_data


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


def _fetch_external_market_snapshot_with_retries(
    *,
    attempts: int = 3,
    retry_delay_seconds: float = 5.0,
) -> dict:
    """Prefer a complete external snapshot without blocking the daily run forever."""
    attempts = max(1, int(attempts))
    best_snapshot: dict | None = None
    best_available = -1
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            snapshot = fetch_external_market_snapshot()
            available = int(snapshot.get("available_count") or 0)
            expected = int(snapshot.get("expected_count") or 0)
            if best_snapshot is None or available > best_available:
                best_snapshot = snapshot
                best_available = available
            if expected > 0 and available >= expected:
                return snapshot
            logging.warning(
                "External market snapshot incomplete attempt=%s/%s available=%s expected=%s",
                attempt,
                attempts,
                available,
                expected,
            )
        except Exception as exc:
            last_error = exc
            logging.warning(
                "External market snapshot fetch failed attempt=%s/%s: %s",
                attempt,
                attempts,
                exc,
            )
        if attempt < attempts:
            time.sleep(max(0.0, float(retry_delay_seconds)))

    if best_snapshot is None:
        raise RuntimeError(f"External market snapshot unavailable after {attempts} attempts: {last_error}")
    best_snapshot = dict(best_snapshot)
    warnings = list(best_snapshot.get("source_warnings") or [])
    warnings.append(
        "external snapshot remained incomplete after retries: "
        f"{best_available}/{int(best_snapshot.get('expected_count') or 0)}"
    )
    best_snapshot["source_warnings"] = warnings
    return best_snapshot


def _resolve_target_trade_date(engine, *, strict_prev_trade_day: bool, execution_time: str, trade_date: str) -> str:
    if trade_date:
        return str(trade_date)[:10]
    if strict_prev_trade_day:
        return previous_trade_date(engine, execution_time or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return latest_trade_date(engine)


def _premarket_theme_cutoff(execution_time: str) -> datetime:
    execution_dt = datetime.fromisoformat(str(execution_time).replace("Z", "+00:00"))
    if execution_dt.tzinfo is not None:
        execution_dt = execution_dt.replace(tzinfo=None)
    now = datetime.now().replace(microsecond=0)
    hard_cutoff = datetime.combine(execution_dt.date(), datetime_time(9, 7, 59))
    if now.date() == execution_dt.date():
        return min(now, hard_cutoff)
    return min(execution_dt.replace(microsecond=0), hard_cutoff)


def _split_theme_markdown(content: str, max_bytes: int = 3800) -> list[str]:
    """Split Markdown on line boundaries while keeping every UTF-8 character."""
    if not content:
        raise ValueError("09:08盘前主线预判内容为空")
    chunks: list[str] = []
    current = ""
    for line in content.splitlines(keepends=True):
        if len(line.encode("utf-8")) > max_bytes:
            if current:
                chunks.append(current)
                current = ""
            remaining = line
            while remaining:
                end = min(len(remaining), max_bytes)
                while end > 0 and len(remaining[:end].encode("utf-8")) > max_bytes:
                    end -= 1
                if end <= 0:
                    raise ValueError("企业微信消息中存在无法分段的字符")
                chunks.append(remaining[:end])
                remaining = remaining[end:]
            continue
        candidate = current + line
        if current and len(candidate.encode("utf-8")) > max_bytes:
            chunks.append(current)
            current = line
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def _send_theme_forecast_markdown(webhook_url: str, content: str) -> dict:
    if not str(webhook_url or "").strip():
        raise RuntimeError("未配置企业微信早报机器人 webhook")
    segments = _split_theme_markdown(content)
    delivery_id = str(uuid.uuid4())
    for index, segment in enumerate(segments, start=1):
        prefix = f"## 🧭 09:08盘前主线预判 ({index}/{len(segments)})\n\n" if len(segments) > 1 else ""
        response = send_markdown(webhook_url, prefix + segment)
        if not isinstance(response, dict) or response.get("errcode") not in (None, 0):
            raise RuntimeError(f"企业微信第{index}/{len(segments)}段未返回成功回执")
        if index < len(segments):
            time.sleep(2)
    return {"success": True, "delivery_id": delivery_id, "segments": len(segments)}


def _deliver_theme_forecast(engine, forecast: dict) -> dict:
    run_uid = str(forecast.get("run_uid") or "")
    if str(forecast.get("delivery_status") or "").upper() == "SUCCESS":
        return {
            "success": True,
            "delivery_id": str(forecast.get("delivery_id") or ""),
            "skipped": True,
        }
    try:
        result = _send_theme_forecast_markdown(
            get_wecom_webhook("briefing", required=False),
            format_forecast_markdown(forecast),
        )
        if not result.get("success"):
            raise RuntimeError("09:08盘前主线预判未获得企业微信成功回执")
        if run_uid:
            mark_forecast_delivery(
                engine,
                run_uid,
                status="SUCCESS",
                delivery_id=str(result.get("delivery_id") or ""),
            )
        return {
            "success": True,
            "delivery_id": str(result.get("delivery_id") or ""),
            "segments": int(result.get("segments") or 0),
            "skipped": False,
        }
    except Exception as exc:
        if run_uid:
            mark_forecast_delivery(engine, run_uid, status="FAILED", error=str(exc))
        raise


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
    parser.add_argument("--auto-repair-missing-data", action="store_true", help="Repair K-line, historical capital flow and snapshot coverage before screening")
    parser.add_argument("--use-intraday-current", action="store_true", help="Use current intraday quote snapshots as target-day K-line inputs")
    parser.add_argument("--external-market", action="store_true", help="Capture global market data before recommendations")
    parser.add_argument("--theme-forecast", action="store_true", help="Build and freeze the theme-first 09:08 forecast")
    parser.add_argument("--push-theme-forecast", action="store_true", help="Push the frozen 09:08 forecast to the briefing WeCom bot")
    parser.add_argument("--theme-top-n", type=int, default=12)
    parser.add_argument("--theme-stocks-per-theme", type=int, default=5)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    os.environ.setdefault("PROBIGA_KLINE_FEATURE_SQL_MODE", "pandas")
    os.environ.setdefault("PROBIGA_BATCH_DB_READ_RETRIES", "8")
    os.environ.setdefault("PROBIGA_KLINE_FEATURE_BATCH_SIZE", "200")
    execution_time = args.execution_time.strip() or datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    engine = create_batch_engine()
    try:
        wait_attempts = int(os.environ.get("PROBIGA_AI_RECOMMEND_DB_WAIT_ATTEMPTS", "12"))
    except (TypeError, ValueError):
        wait_attempts = 12
    _wait_for_db(engine, attempts=wait_attempts)
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
        repair_report = None
        external_report = None
        external_snapshot = None
        theme_forecast_report = None
        theme_delivery_report = None
        batch_execution_time = execution_time
        if args.external_market:
            _recommended_run_history_update(run_uid, status="running", payload={
                "progress_percent": 3,
                "done_count": 0,
                "message": "正在抓取美股、日韩、港股、期货、外汇和美债外围数据...",
                "error": "",
            })
            try:
                external_attempts = int(os.environ.get("PROBIGA_EXTERNAL_MARKET_FETCH_ATTEMPTS", "3"))
            except (TypeError, ValueError):
                external_attempts = 3
            try:
                external_retry_delay = float(os.environ.get("PROBIGA_EXTERNAL_MARKET_RETRY_DELAY_SECONDS", "5"))
            except (TypeError, ValueError):
                external_retry_delay = 5.0
            snapshot = _fetch_external_market_snapshot_with_retries(
                attempts=external_attempts,
                retry_delay_seconds=external_retry_delay,
            )
            external_snapshot = snapshot
            try:
                external_report = store_external_market_snapshot(engine, snapshot)
            except Exception as exc:
                logging.warning("External market snapshot storage failed; continuing base recommendation: %s", exc)
                external_report = {
                    "external_market_status": snapshot.get("external_market_status") or "UNKNOWN",
                    "external_market_score": snapshot.get("external_market_score"),
                    "external_market_data_quality": "NOT_STORED",
                    "available_count": int(snapshot.get("available_count") or 0),
                    "expected_count": int(snapshot.get("expected_count") or 0),
                    "source_warnings": [f"storage failed: {exc}"],
                }
            # Use a completion-side cutoff so the snapshot captured during this
            # run is always eligible for the model context.
            batch_execution_time = datetime.now().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
            _recommended_run_history_update(run_uid, status="running", payload={
                "progress_percent": 7,
                "done_count": 0,
                "message": "外围数据已落库，正在执行 AI 推荐筛选..." if external_report.get("external_market_data_quality") != "NOT_STORED" else "外围数据抓取完成但未落库，继续执行基础 AI 推荐...",
                "error": "",
            })
        if args.theme_forecast:
            _recommended_run_history_update(run_uid, status="running", payload={
                "progress_percent": 10,
                "done_count": 0,
                "message": "正在按外盘、隔夜催化和上一完整交易日数据生成09:08主线预判...",
                "error": "",
            })
            theme_cutoff = _premarket_theme_cutoff(execution_time)
            snapshot_for_theme = external_snapshot
            snapshot_time = None
            if snapshot_for_theme is not None:
                captured_value = snapshot_for_theme.get("captured_at")
                if isinstance(captured_value, datetime):
                    snapshot_time = captured_value.replace(tzinfo=None) if captured_value.tzinfo else captured_value
                elif captured_value:
                    try:
                        snapshot_time = datetime.fromisoformat(str(captured_value).replace("Z", "+00:00")).replace(tzinfo=None)
                    except ValueError:
                        snapshot_time = None
            if snapshot_time is not None and snapshot_time > theme_cutoff:
                snapshot_for_theme = None
            theme_forecast_report = run_premarket_theme_forecast(
                engine,
                session_date=theme_cutoff.date().isoformat(),
                source_trade_date=target_trade_date,
                cutoff_at=theme_cutoff,
                external_snapshot=snapshot_for_theme,
                theme_limit=max(1, int(args.theme_top_n)),
                stocks_per_theme=max(1, int(args.theme_stocks_per_theme)),
                persist=True,
            )
            if args.push_theme_forecast:
                _recommended_run_history_update(run_uid, status="running", payload={
                    "progress_percent": 14,
                    "done_count": 0,
                    "message": "09:08主线预判已冻结，正在推送到企业微信早报机器人...",
                    "error": "",
                })
                theme_delivery_report = _deliver_theme_forecast(engine, theme_forecast_report)
            _recommended_run_history_update(run_uid, status="running", payload={
                "progress_percent": 16,
                "done_count": len(theme_forecast_report.get("stock_candidates") or []),
                "message": "09:08主线预判已落库并冻结，继续执行基础AI推荐。",
                "error": "",
            })
        if args.auto_repair_missing_data:
            _recommended_run_history_update(run_uid, status="running", payload={
                "progress_percent": 4,
                "done_count": 0,
                "message": "正在检查并补齐推荐所需的交易日数据覆盖...",
                "error": "",
            })

            def _repair_progress(stage: str, _payload: dict) -> None:
                _recommended_run_history_update(run_uid, status="running", payload={
                    "progress_percent": 5,
                    "done_count": 0,
                    "message": f"正在补齐推荐数据：{stage}",
                    "error": "",
                })

            repair_report = repair_target_data(
                target_trade_date,
                min_kline_coverage=float(args.min_kline_coverage),
                callback=_repair_progress,
            )
            _recommended_run_history_update(run_uid, status="running", payload={
                "progress_percent": 8,
                "done_count": 0,
                "message": "推荐所需数据已补齐，准备运行策略筛选...",
                "error": "",
            })
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
                "--trade-date",
                target_trade_date,
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
        def _progress_callback(event: dict) -> None:
            try:
                stage = str(event.get("stage") or "")
                if stage == "done":
                    return
                percent = int(event.get("percent") or 0)
                _recommended_run_history_update(run_uid, status="running", payload={
                    "progress_percent": max(1, min(99, percent)),
                    "done_count": int(event.get("done") or 0),
                    "total": int(event.get("analysis_count") or event.get("total") or 0) or None,
                    "passed": int(event.get("recommendation_count") or 0),
                    "flow_date": event.get("flow_date") or "",
                    "hot_date": event.get("hot_date") or "",
                    "market_mood_score": event.get("market_mood_score"),
                    "message": str(event.get("step") or "AI 推荐筛选运行中...")[:500],
                    "error": "",
                })
            except Exception:
                logging.debug("Failed to update AI recommendation progress", exc_info=True)

        batch_kwargs = {
            "engine": engine,
            "trade_date": target_trade_date,
            "top_n": int(args.top_n),
            "min_score": float(args.min_score),
            "strict_prev_trade_day": bool(args.strict_prev_trade_day),
            "execution_time": batch_execution_time,
            "min_kline_coverage": float(args.min_kline_coverage),
            "auto_repair_missing_kline": bool(args.auto_repair_missing_kline),
            "progress_callback": _progress_callback,
        }
        if args.use_intraday_current:
            if "use_intraday_current" not in inspect.signature(run_batch).parameters:
                raise RuntimeError(
                    "--use-intraday-current requires a screening engine that supports intraday inputs"
                )
            batch_kwargs["use_intraday_current"] = True
        stats = run_batch(
            **batch_kwargs,
        )
        payload = {
            "trade_date": stats.trade_date,
            "analysis_count": stats.analysis_count,
            "recommendation_count": stats.recommendation_count,
            "market_mood_score": stats.market_mood_score,
            "external_market": external_report,
            "premarket_theme_forecast": {
                "run_uid": (theme_forecast_report or {}).get("run_uid"),
                "session_date": (theme_forecast_report or {}).get("session_date"),
                "summary": (theme_forecast_report or {}).get("summary"),
                "data_quality": (theme_forecast_report or {}).get("data_quality"),
                "theme_count": len((theme_forecast_report or {}).get("themes") or []),
                "candidate_count": len((theme_forecast_report or {}).get("stock_candidates") or []),
                "delivery": theme_delivery_report,
            } if theme_forecast_report is not None else None,
            "flow_date": stats.flow_date,
            "hot_date": stats.hot_date,
            "run_uid": run_uid,
            "repair": repair_report,
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
