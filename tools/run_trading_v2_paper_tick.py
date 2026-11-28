#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
from contextlib import contextmanager
import signal
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.trading_v2.paper_tick import run_paper_tick
from tools.env_config import load_project_env


@contextmanager
def study_deadline():
    def expired(signum, frame):
        raise TimeoutError("intraday pair research stage exceeded 25 seconds")

    if not hasattr(signal, "SIGALRM"):
        yield
        return
    previous = signal.signal(signal.SIGALRM, expired)
    signal.alarm(25)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def main() -> int:
    parser = argparse.ArgumentParser()
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--register-study", action="store_true")
    modes.add_argument("--study-report", action="store_true")
    args = parser.parse_args()
    load_project_env()
    engine = create_batch_engine()
    from server.trading_v3.intraday_pair import run_study

    if args.study_report or args.register_study:
        with study_deadline():
            if args.study_report:
                from server.trading_v3.intraday_pair_report import report

                with engine.connect() as connection:
                    result = report(connection)
            else:
                result = run_study(engine, registration_only=True)
        print(json.dumps(result, ensure_ascii=False, default=str))
        return 0 if result.get("status") not in ("BUSY", "NOT_REGISTERED") else 2
    result = run_paper_tick(engine)
    with study_deadline():
        result["intraday_entry_pair"] = run_study(engine)
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0 if result["status"] in {"idle", "market_closed", "completed"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
