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

from tools.linux_release_activation import Services


def probe_scheduler_heartbeat(expected_build_sha: str, expected_pid: int, *, services=None) -> dict:
    """Use the running API's database view, as Linux activation does."""
    transport = services or Services()
    health = transport.health()
    revision = health.get("release_revision") or {}
    scheduler = health.get("standalone_scheduler") or {}
    heartbeat = health.get("standalone_scheduler_heartbeat") or {}
    detail = heartbeat.get("detail") or {}
    current = detail.get("current") or {}
    errors = list(detail.get("errors") or [])
    if health.get("status") != "ok" or revision.get("actual_git_sha") != expected_build_sha or revision.get("matches_expected") is not True:
        errors.append("api_release_not_ready")
    if scheduler.get("verified") is not True or scheduler.get("active") is not True or scheduler.get("pid") != expected_pid:
        errors.append("scheduler_instance_differs")
    if heartbeat.get("ready") is not True or detail.get("fresh_row_count") != 1 or detail.get("future_row_count") != 0:
        errors.append("scheduler_heartbeat_not_ready")
    if current.get("pid") != expected_pid or current.get("build_sha") != expected_build_sha or current.get("executor_role") != "linux_standalone":
        errors.append("heartbeat_identity_differs")
    return {"status": "FAIL" if errors else "PASS",
            "check": "linux_standalone_scheduler_heartbeat_current",
            "source": "LIVE_LOCAL_API_HEALTH",
            "detail": {**detail, "errors": sorted(set(errors))}}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-build-sha", required=True)
    parser.add_argument("--expected-scheduler-pid", required=True, type=int)
    args = parser.parse_args()

    try:
        payload = probe_scheduler_heartbeat(args.expected_build_sha, args.expected_scheduler_pid)
        print(json.dumps(payload, sort_keys=True, default=str))
        return 0 if payload["status"] == "PASS" else 1
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "check": "linux_standalone_scheduler_heartbeat_current",
                    "error_code": "scheduler_heartbeat_probe_failed",
                    "error_type": type(exc).__name__,
                },
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
