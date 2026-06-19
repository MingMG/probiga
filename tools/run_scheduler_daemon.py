# -*- coding: utf-8 -*-
"""Run the ProBigA scheduler as a standalone process."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.api.scheduler_runtime import run_scheduler_forever, scheduler_runtime_info


def main() -> int:
    info = scheduler_runtime_info()
    print(
        "ProBigA scheduler daemon starting "
        f"(max_concurrent_tasks={info['scheduler_max_concurrent_tasks']}, "
        f"poll_seconds={info['scheduler_poll_seconds']})"
    )
    run_scheduler_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
