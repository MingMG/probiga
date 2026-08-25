#!/usr/bin/env python3
"""Read-only release and execution proof for the Windows QMT edge."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.scheduler_runtime_health import (
    check_qmt_windows_edge_executor,
    check_qmt_windows_edge_identity,
    check_qmt_windows_edge_release_receipt,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-build-sha", required=True)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--identity-only", action="store_true")
    modes.add_argument("--release-bootstrap-only", action="store_true")
    parser.add_argument("--expected-poll-seconds", type=int, default=60)
    parser.add_argument("--compact", action="store_true")
    args = parser.parse_args(argv)
    from tools.env_config import create_tool_engine, load_project_env

    load_project_env()
    engine = create_tool_engine()
    try:
        with engine.connect() as connection:
            checker = check_qmt_windows_edge_executor
            mode = "executor"
            if args.identity_only:
                checker = check_qmt_windows_edge_identity
                mode = "identity"
            elif args.release_bootstrap_only:
                checker = check_qmt_windows_edge_release_receipt
                mode = "release-bootstrap"
            passed, detail = checker(
                connection,
                expected_build_sha=args.expected_build_sha,
                expected_poll_seconds=args.expected_poll_seconds,
            )
    finally:
        engine.dispose()
    payload = {
        "schema": "probiga.qmt-windows-edge-health.v1",
        "status": "AVAILABLE" if passed else "UNAVAILABLE",
        "mode": mode,
        "strategy_eligible": passed,
        "detail": detail,
        "database_writes": False,
        "automatic_real_order_submission": False,
    }
    print(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            indent=None if args.compact else 2,
            default=str,
        )
    )
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
