from __future__ import annotations

import logging
import threading
from datetime import datetime

from sqlalchemy import create_engine, text

from biz.stock_market.sync_stock_market import read_index_codes, step_index_current
from integrations.qmt.info import CORE_INDEXES
from server.common.config import get_mysql_url, get_qmt_live_runtime_config
from tools.sync_qmt_realtime import sync_qmt_realtime

logger = logging.getLogger("qmt_live_runtime")

_live_thread: threading.Thread | None = None
_stop_event: threading.Event | None = None
_run_lock = threading.Lock()


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
    return [symbol.split(".", 1)[0] for symbol in CORE_INDEXES]


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
    with engine.connect() as conn:
        for sql, params in queries:
            try:
                rows = conn.execute(text(sql), params).fetchall()
            except Exception as exc:
                logger.warning("load tracked stocks failed for query: %s", exc)
                continue
            codes.extend(str(row[0]).strip().zfill(6) for row in rows if str(row[0] or "").strip())

    deduped: list[str] = []
    seen: set[str] = set()
    for code in codes:
        if code in seen:
            continue
        seen.add(code)
        deduped.append(code)
    return deduped


def _run_once(engine) -> None:
    with _run_lock:
        config = get_qmt_live_runtime_config()
        tracked_codes = _load_tracked_stock_codes(engine, int(config["candidate_limit"]))
        if tracked_codes:
            result = sync_qmt_realtime(
                engine=engine,
                codes=tracked_codes,
                archive_snapshot=True,
                run_rt_ddl=False,
                skip_closed=False,
                min_coverage=0.0,
                replace_scope="subset",
            )
            logger.info(
                "qmt realtime synced tracked=%s current=%s snapshot=%s coverage=%s generated_at=%s",
                len(tracked_codes),
                result.get("current_rows"),
                result.get("snapshot_rows"),
                result.get("coverage"),
                result.get("generated_at"),
            )
        else:
            logger.info("qmt realtime skipped stock sync because tracked universe is empty")
        step_index_current(engine, _load_index_codes(engine))


def _worker(stop_event: threading.Event) -> None:
    config = get_qmt_live_runtime_config()
    interval_seconds = float(config["poll_seconds"])
    idle_seconds = float(config["idle_sleep_seconds"])
    engine = create_engine(get_mysql_url(required=True), pool_pre_ping=True, future=True)
    logger.info(
        "QMT live runtime started interval=%ss idle=%ss trading_only=%s",
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
            logger.exception("QMT live runtime sync failed: %s", exc)
        if _sleep_with_stop(interval_seconds, stop_event):
            break
    engine.dispose()
    logger.info("QMT live runtime stopped")


def start_qmt_live_runtime() -> threading.Thread | None:
    global _live_thread, _stop_event
    config = get_qmt_live_runtime_config()
    if not bool(config["enabled"]):
        logger.info("QMT live runtime disabled")
        return None
    if _live_thread and _live_thread.is_alive():
        return _live_thread
    _stop_event = threading.Event()
    _live_thread = threading.Thread(
        target=_worker,
        args=(_stop_event,),
        daemon=True,
        name="qmt-live-runtime",
    )
    _live_thread.start()
    return _live_thread


def stop_qmt_live_runtime() -> None:
    global _live_thread, _stop_event
    if _stop_event is not None:
        _stop_event.set()
    _live_thread = None
    _stop_event = None
