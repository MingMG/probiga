#!/usr/bin/env python3
"""Read one canonical QMT session and emit a distinct, post-close replay report."""
from __future__ import annotations

import argparse
from datetime import date, datetime
import json
from pathlib import Path
import sys
from time import monotonic

from sqlalchemy.exc import SQLAlchemyError


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.intraday_alert.minute_replay import MinuteReplayBlocked, TZ, replay_local_minute_anomalies, replay_minute_anomalies
from server.common.config import get_kline_mysql_url
from server.common.engine_factory import create_pooled_engine
from tools.env_config import load_project_env


class _Progress:
    def __init__(self):
        self.total = 0
        self.receipt = None
        self.last_line = 0.0
        self.last_progress_at = None

    def emit(self, stage, *, status="RUNNING", completed_units=0, current_unit=None,
             total_units=None, receipt_run_id=None, **metrics):
        if receipt_run_id is not None:
            self.receipt, self.total = receipt_run_id, total_units
        now = datetime.now(TZ).isoformat(timespec="seconds")
        if completed_units:
            self.last_progress_at = now
        current = monotonic()
        if status == "RUNNING" and current - self.last_line < 20:
            return
        self.last_line = current
        print(json.dumps({"stage": stage, "status": status, "completed_units": completed_units,
                          "total_units": self.total, "current_unit": current_unit,
                          "last_progress_at": self.last_progress_at, "source_receipt_run_id": self.receipt,
                          **metrics}, ensure_ascii=True), file=sys.stderr, flush=True)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", required=True, type=date.fromisoformat)
    parser.add_argument("--cutoff", type=datetime.fromisoformat, help="Latest allowed evidence time")
    parser.add_argument("--local-datadir", type=Path, help="Use the local QMT cache, never query the remote minute table")
    args = parser.parse_args(argv)
    load_project_env()
    engine = None
    try:
        engine = create_pooled_engine(get_kline_mysql_url(), pool_size=1, max_overflow=0, pool_timeout=5,
                                      connect_args={"connect_timeout": 6, "read_timeout": 20, "write_timeout": 5})
        if args.local_datadir is not None:
            report = replay_local_minute_anomalies(engine, args.trade_date, args.local_datadir.resolve(strict=True),
                                                   cutoff=args.cutoff, progress=_Progress())
        else:
            report = replay_minute_anomalies(engine, args.trade_date, cutoff=args.cutoff, progress=_Progress())
    except (MinuteReplayBlocked, OSError, SQLAlchemyError, ValueError, RuntimeError, TimeoutError) as exc:
        reason = str(exc) if isinstance(exc, (MinuteReplayBlocked, TimeoutError)) else type(exc).__name__
        print(json.dumps({"status": "BLOCKED", "trade_date": args.trade_date.isoformat(), "reason": reason}),
              file=sys.stderr)
        return 2
    finally:
        if engine is not None:
            engine.dispose()
    print(json.dumps(report, ensure_ascii=True, separators=(",", ":"), allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
