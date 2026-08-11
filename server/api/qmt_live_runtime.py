from __future__ import annotations

import logging
import threading
import time
from datetime import datetime

from biz.stock_market.sync_stock_market import read_index_codes, step_index_current
from server.api.routers._engine import get_engine
from server.common.current_data import get_current_engine
from server.common.config import get_qmt_live_runtime_config
from server.common.sql_reader import read_sql_rows
from tools.sync_market_realtime import sync_market_realtime

logger = logging.getLogger("live_quote_runtime")

CORE_INDEX_CODES = ["000001", "399001", "399006", "000300", "000905", "000852"]

_live_thread: threading.Thread | None = None
_stop_event: threading.Event | None = None
_run_lock = threading.Lock()
_last_index_poll = 0.0


def _is_trading_time(now: datetime | None = None) -> bool:
    current = now or datetime.now()
    if current.weekday() >= 5:
        return False
    hhmm = current.hour * 100 + current.minute
    return (925 <= hhmm <= 1135) or (1255 <= hhmm <= 1505)


def _sleep_with_stop(wait_seconds: float, stop_event: threading.Event) -> bool:
    return stop_event.wait(max(0.5, float(wait_seconds)))


def _load_index_codes(engine) -> list[str]:
    try:
        codes = [str(code).strip() for code in read_index_codes(engine) if str(code).strip()]
    except Exception as exc:
        logger.warning("load index codes failed, fallback to core indexes: %s", exc)
        codes = []
    if codes:
        return codes
    return list(CORE_INDEX_CODES)


def _load_tracked_stock_codes(engine, candidate_limit: int) -> list[str]:
    codes: list[str] = []
    queries: list[tuple[str, dict]] = [
        ("SELECT DISTINCT stock_code FROM st_user_portfolio WHERE stock_code IS NOT NULL AND stock_code <> ''", {}),
        ("SELECT DISTINCT stock_code FROM st_sim_position WHERE status = 'holding' AND stock_code IS NOT NULL AND stock_code <> ''", {}),
        (
            """
            SELECT stock_code
            FROM st_recommended_stocks
            WHERE pick_date = (SELECT MAX(pick_date) FROM st_recommended_stocks)
              AND stock_code IS NOT NULL AND stock_code <> ''
            LIMIT :limit
            """,
            {"limit": max(20, int(candidate_limit or 60))},
        ),
    ]
    for sql, params in queries:
        try:
            rows = read_sql_rows(engine, sql, params, context="qmt_live_runtime")
        except Exception as exc:
            logger.warning("load tracked stocks failed for query: %s", exc)
            continue
        codes.extend(
            str(row.get("stock_code")).strip().zfill(6)
            for row in rows
            if str(row.get("stock_code") or "").strip()
        )

    deduped: list[str] = []
    seen: set[str] = set()
    for code in codes:
        if code in seen:
            continue
        seen.add(code)
        deduped.append(code)
    return deduped


def _run_once(engine) -> None:
    global _last_index_poll
    with _run_lock:
        config = get_qmt_live_runtime_config()
        # Business universes live in the primary application database.  The
        # dedicated current-quote database can contain stale compatibility
        # copies of those tables and must only be used for quote reads/writes.
        tracked_codes = _load_tracked_stock_codes(get_engine(), int(config["candidate_limit"]))
        if tracked_codes:
            placeholders = ",".join(f":code_{idx}" for idx, _ in enumerate(tracked_codes))
            params = {f"code_{idx}": code for idx, code in enumerate(tracked_codes)}
            fresh_codes = set()
            try:
                rows = read_sql_rows(
                    engine,
                    f"SELECT stock_code, snapshot_at FROM sm_stock_current WHERE stock_code IN ({placeholders})",
                    params,
                    context="qmt_live_runtime_current",
                )
                now = datetime.now()
                for row in rows:
                    snapshot_at = row.get("snapshot_at")
                    if not snapshot_at:
                        continue
                    if not isinstance(snapshot_at, datetime):
                        snapshot_at = datetime.strptime(str(snapshot_at)[:19], "%Y-%m-%d %H:%M:%S")
                    if (now - snapshot_at).total_seconds() <= max(15, int(config["poll_seconds"]) * 2):
                        fresh_codes.add(str(row.get("stock_code") or "").strip().zfill(6))
            except Exception as exc:
                logger.warning("read current QMT snapshot failed; using Sina fallback: %s", exc)

            stale_codes = [code for code in tracked_codes if code not in fresh_codes]
            if stale_codes:
                result = sync_market_realtime(
                    engine=engine,
                    codes=stale_codes,
                    source="sina",
                    archive_snapshot=True,
                    run_rt_ddl=False,
                    skip_closed=False,
                    min_coverage=0.60,
                    replace_scope="subset",
                )
                logger.info(
                    "Sina fallback synced stale=%s qmt_fresh=%s current=%s snapshot=%s coverage=%s generated_at=%s",
                    len(stale_codes),
                    len(fresh_codes),
                    result.get("current_rows"),
                    result.get("snapshot_rows"),
                    result.get("coverage"),
                    result.get("generated_at"),
                )
            else:
                logger.info("Big QMT current snapshots are fresh tracked=%s; skipped Sina fallback", len(fresh_codes))
        else:
            logger.info("public realtime skipped stock sync because tracked universe is empty")

        now_monotonic = time.monotonic()
        if now_monotonic - _last_index_poll >= float(config.get("index_poll_seconds", 60)):
            step_index_current(engine, _load_index_codes(engine))
            _last_index_poll = now_monotonic


def _worker(stop_event: threading.Event) -> None:
    config = get_qmt_live_runtime_config()
    interval_seconds = float(config["poll_seconds"])
    idle_seconds = float(config["idle_sleep_seconds"])
    engine = get_current_engine()
    logger.info(
        "Public live quote runtime started interval=%ss idle=%ss trading_only=%s",
        interval_seconds,
        idle_seconds,
        bool(config["trading_hours_only"]),
    )
    while not stop_event.is_set():
        try:
            trading_now = _is_trading_time()
            if bool(config["trading_hours_only"]) and not trading_now:
                if _sleep_with_stop(idle_seconds, stop_event):
                    break
                continue
            _run_once(engine)
        except Exception as exc:
            logger.exception("Public live quote runtime sync failed: %s", exc)
        if _sleep_with_stop(interval_seconds, stop_event):
            break
    logger.info("Public live quote runtime stopped")


def start_qmt_live_runtime() -> threading.Thread | None:
    global _live_thread, _stop_event
    config = get_qmt_live_runtime_config()
    if not bool(config["enabled"]):
        logger.info("Public live quote runtime disabled")
        return None
    if _live_thread and _live_thread.is_alive():
        return _live_thread
    _stop_event = threading.Event()
    _live_thread = threading.Thread(
        target=_worker,
        args=(_stop_event,),
        daemon=True,
        name="live-quote-runtime",
    )
    _live_thread.start()
    return _live_thread


def stop_qmt_live_runtime(timeout_seconds: float = 5.0) -> None:
    global _live_thread, _stop_event
    thread = _live_thread
    stop_event = _stop_event
    if stop_event is not None:
        stop_event.set()
    if thread is not None and thread.is_alive() and thread is not threading.current_thread():
        thread.join(timeout=max(0.0, float(timeout_seconds)))
        if thread.is_alive():
            logger.warning("Public live quote runtime did not stop within %.1fs", float(timeout_seconds))
            return
    _live_thread = None
    _stop_event = None

