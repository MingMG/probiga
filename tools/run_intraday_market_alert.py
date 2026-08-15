#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run one evidence-backed intraday market alert scan."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.intraday_alert.core import IntradayAlertError, run_intraday_scan  # noqa: E402
from server.common.batch_db import create_batch_engine  # noqa: E402
from server.common.current_data import get_current_engine  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="盘中关键节点事件扫描与播报")
    parser.add_argument(
        "--mode",
        choices=("shadow", "live"),
        default="shadow",
        help="shadow 只留痕不发消息；live 显式发送（默认 shadow）",
    )
    parser.add_argument("--json", action="store_true", help="输出机器可读结果")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main_engine = create_batch_engine()
    current_engine = get_current_engine()
    try:
        result = run_intraday_scan(main_engine, current_engine, mode=args.mode)
    except IntradayAlertError as exc:
        payload = {"status": "failed", "error_type": type(exc).__name__, "message": str(exc)}
        print(json.dumps(payload, ensure_ascii=False) if args.json else f"FAILED: {exc}")
        return 1
    finally:
        main_engine.dispose()
    print(json.dumps(result, ensure_ascii=False, default=str) if args.json else result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
