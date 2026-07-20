# -*- coding: utf-8 -*-
"""Optional FastAPI-owned background worker for the full-market radar."""
from __future__ import annotations

import logging
import threading
from datetime import datetime

from biz.market_radar.core import get_shared_radar_engine, market_phase
from server.api.routers._engine import get_engine as get_api_engine
from server.common.config import get_market_radar_runtime_config

logger = logging.getLogger("market_radar_runtime")

_thread: threading.Thread | None = None
_stop_event: threading.Event | None = None


def _sleep(stop_event: threading.Event, seconds: float) -> bool:
    return stop_event.wait(max(0.5, float(seconds)))


def _worker(stop_event: threading.Event) -> None:
    config = get_market_radar_runtime_config()
    engine = get_shared_radar_engine(get_api_engine())
    interval = float(config["poll_seconds"])
    logger.info("market radar runtime started interval=%ss trading_only=%s", interval, config["trading_hours_only"])
    while not stop_event.is_set():
        phase = market_phase(datetime.now())
        if bool(config["trading_hours_only"]) and phase == "closed":
            if _sleep(stop_event, min(interval * 6, 30)):
                break
            continue
        try:
            result = engine.scan_once()
            logger.info(
                "market radar scanned phase=%s quotes=%s sectors=%s events=%s",
                result.get("phase"), result.get("quote_rows"), result.get("sector_rows"), result.get("event_rows"),
            )
        except Exception as exc:
            logger.exception("market radar scan failed: %s", exc)
        if _sleep(stop_event, interval):
            break
    logger.info("market radar runtime stopped")


def start_market_radar_runtime() -> threading.Thread | None:
    global _thread, _stop_event
    config = get_market_radar_runtime_config()
    if not bool(config["enabled"]):
        logger.info("market radar runtime disabled")
        return None
    if _thread and _thread.is_alive():
        return _thread
    _stop_event = threading.Event()
    _thread = threading.Thread(target=_worker, args=(_stop_event,), daemon=True, name="market-radar-runtime")
    _thread.start()
    return _thread


def stop_market_radar_runtime(timeout_seconds: float = 5.0) -> None:
    global _thread, _stop_event
    thread = _thread
    stop_event = _stop_event
    if stop_event is not None:
        stop_event.set()
    if thread is not None and thread.is_alive() and thread is not threading.current_thread():
        thread.join(timeout=max(0.0, float(timeout_seconds)))
        if thread.is_alive():
            logger.warning("market radar runtime did not stop within %.1fs", float(timeout_seconds))
            return
    _thread = None
    _stop_event = None
