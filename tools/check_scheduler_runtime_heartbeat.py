#!/usr/bin/env python3
"""Check the current Linux standalone scheduler heartbeat identity."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.scheduler_runtime_health import (
    check_linux_standalone_scheduler_heartbeat,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-build-sha", required=True)
    parser.add_argument("--expected-scheduler-pid", required=True, type=int)
    args = parser.parse_args()

    from tools.env_config import create_tool_engine, load_project_env

    load_project_env()
    try:
        engine = create_tool_engine()
        with engine.connect() as connection:
            passed, detail = check_linux_standalone_scheduler_heartbeat(
                connection,
                expected_build_sha=args.expected_build_sha,
                expected_pid=args.expected_scheduler_pid,
            )
        payload = {
            "status": "PASS" if passed else "FAIL",
            "check": "linux_standalone_scheduler_heartbeat_current",
            "detail": detail,
        }
        print(json.dumps(payload, sort_keys=True, default=str))
        return 0 if passed else 1
    except Exception:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "check": "linux_standalone_scheduler_heartbeat_current",
                    "error_code": "scheduler_heartbeat_probe_failed",
                },
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
