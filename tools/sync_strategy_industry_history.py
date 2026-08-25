#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Validate/copy one immutable exact-date QMT industry snapshot."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.engine.strategy_industry_history import (
    IndustrySnapshotIntegrityError,
    IndustrySnapshotNotReady,
    _digest,
    build_history_rows,
    capture_industry_history,
    prepare_industry_history,
)
from tools.env_config import create_tool_engine, load_project_env


_SHANGHAI = ZoneInfo("Asia/Shanghai")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--trade-date",
        default=datetime.now(_SHANGHAI).date().isoformat(),
    )
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    load_project_env(ROOT)
    engine = create_tool_engine()
    try:
        if args.apply:
            report = capture_industry_history(
                engine, trade_date=args.trade_date,
            )
        else:
            report, _rows = prepare_industry_history(
                engine, trade_date=args.trade_date,
            )
            report = {**report, "status": "PREVIEW"}
    except IndustrySnapshotNotReady as exc:
        report = {
            "status": "NOT_READY",
            "retryable": True,
            "trade_date": args.trade_date,
            "reason": str(exc),
        }
    except IndustrySnapshotIntegrityError as exc:
        report = {
            "status": "INTEGRITY_ERROR",
            "retryable": False,
            "trade_date": args.trade_date,
            "reason": str(exc),
        }
    finally:
        engine.dispose()
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    if report["status"] in {"COMPLETED", "PREVIEW"}:
        return 0
    return 2 if report["status"] == "NOT_READY" else 3


__all__ = [
    "IndustrySnapshotIntegrityError",
    "IndustrySnapshotNotReady",
    "_digest",
    "build_history_rows",
    "capture_industry_history",
    "prepare_industry_history",
]


if __name__ == "__main__":
    raise SystemExit(main())
