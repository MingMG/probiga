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


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def main() -> int:
    _configure_logging()
    thread = start_qmt_live_runtime()
    if thread is None:
        print("QMT live runtime is disabled by configuration.")
        return 1

    stop_requested = False

    def _handle_stop(_signum, _frame) -> None:
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGINT, _handle_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_stop)

    try:
        while not stop_requested and thread.is_alive():
            time.sleep(1)
    finally:
        stop_qmt_live_runtime()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
