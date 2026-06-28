#!/usr/bin/env python
from __future__ import annotations

import argparse
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _qmt_python_hint() -> str:
    return os.environ.get("QMT_PYTHON", "")


def _print_result(label: str, fn: Callable[[], Any]) -> None:
    print(f"\n### {label}", flush=True)
    try:
        data = fn()
        print(f"type={type(data)!r}", flush=True)
        print(f"repr={repr(data)[:3000]}", flush=True)
        if isinstance(data, dict):
            for key, value in list(data.items())[:3]:
                print(
                    f"item {key}: type={type(value)!r} shape={getattr(value, 'shape', None)} repr={repr(value)[:1200]}",
                    flush=True,
                )
    except Exception as exc:
        print(f"ERROR {type(exc).__name__}: {exc}", flush=True)
        traceback.print_exc(limit=2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose native QMT capital-flow availability.")
    parser.add_argument("--code", default="600519.SH", help="QMT stock code, e.g. 600519.SH")
    parser.add_argument("--date", default=time.strftime("%Y%m%d"), help="Trade date in YYYYMMDD")
    parser.add_argument("--port", type=int, default=int(os.environ.get("QMT_PORT", "58610") or "58610"))
    args = parser.parse_args()

    print(f"python={sys.executable}", flush=True)
    print(f"QMT_PYTHON={_qmt_python_hint()}", flush=True)

    from xtquant import xtdata

    try:
        xtdata.enable_hello = False
    except Exception:
        pass

    print(f"connecting port={args.port}", flush=True)
    xtdata.connect(port=args.port, remember_if_success=False)
    print("connected", flush=True)

    code = args.code
    day = "".join(ch for ch in args.date if ch.isdigit())[:8]
    start = f"{day}093000"
    end = f"{day}150000"

    _print_result("get_full_tick", lambda: xtdata.get_full_tick([code]))
    _print_result("transactioncount1d", lambda: xtdata.get_market_data_ex([], [code], "transactioncount1d", day, day, 0, "none", False))
    _print_result("transactioncount1m", lambda: xtdata.get_market_data_ex([], [code], "transactioncount1m", start, end, 0, "none", False))
    _print_result("get_transactioncount", lambda: xtdata.get_transactioncount([code]))
    _print_result("get_l2_quote", lambda: xtdata.get_l2_quote([], code, "", "", 5))
    _print_result("get_l2_transaction", lambda: xtdata.get_l2_transaction([], code, "", "", 5))

    for period in ("l2quote", "l2transaction", "l2transactioncount", "transactioncount1m"):
        _print_result(
            f"subscribe_quote {period}",
            lambda period=period: xtdata.subscribe_quote(code, period=period, count=-1),
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
