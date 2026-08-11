from __future__ import annotations

import logging
import signal
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.api.qmt_live_runtime import start_qmt_live_runtime, stop_qmt_live_runtime
from server.api.market_radar_runtime import start_market_radar_runtime, stop_market_radar_runtime


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main() -> int:
    _configure_logging()
    qmt_thread = start_qmt_live_runtime()
    radar_thread = start_market_radar_runtime()
    if qmt_thread is None and radar_thread is None:
        print("Live quote runtime and market radar are disabled by configuration.")
        return 1

    stop_requested = False

    def _handle_stop(_signum, _frame) -> None:
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGINT, _handle_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_stop)

    try:
        while not stop_requested and any(thread and thread.is_alive() for thread in (qmt_thread, radar_thread)):
            time.sleep(1)
    finally:
        stop_qmt_live_runtime()
        stop_market_radar_runtime()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
