#!/usr/bin/env python3
"""Fetch and persist one point-in-time external-market snapshot."""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.market_context.external_market import (
    fetch_external_market_snapshot,
    store_external_market_snapshot,
)
from tools.env_config import create_tool_engine, load_project_env


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--retry-delay-seconds", type=float, default=5.0)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    load_project_env()
    attempts = max(1, int(args.attempts))
    snapshot = None
    errors: list[str] = []
    for attempt in range(1, attempts + 1):
        try:
            snapshot = fetch_external_market_snapshot()
            if int(snapshot.get("available_count") or 0) > 0:
                break
            errors.append(f"attempt {attempt}: no available items")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"attempt {attempt}: {type(exc).__name__}: {exc}")
        if attempt < attempts:
            time.sleep(max(0.0, float(args.retry_delay_seconds)))
    if snapshot is None:
        raise RuntimeError("; ".join(errors) or "external snapshot unavailable")
    engine = create_tool_engine()
    try:
        report = store_external_market_snapshot(engine, snapshot)
    finally:
        engine.dispose()
    report["fetch_errors"] = errors
    if args.json:
        print(json.dumps(report, ensure_ascii=False, default=str))
    else:
        print(report)
    return 0 if int(report.get("available_count") or 0) > 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
