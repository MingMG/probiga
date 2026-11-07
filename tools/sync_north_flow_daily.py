#!/usr/bin/env python3
"""Record the daily disclosure disposition of Northbound net flow."""
from __future__ import annotations

import argparse
from datetime import date
import json

from sqlalchemy import text

from server.common.authoritative_market_clock import authoritative_closed_trade_date
from server.common.northbound_disclosure import (
    NORTHBOUND_CLOSE_READY_TIME,
    NORTHBOUND_DAILY_NET_DISCLOSURE_END,
    NORTHBOUND_DAILY_RESULT_SCHEMA,
    NORTHBOUND_NET_NOT_DISCLOSED,
)
from tools.env_config import create_tool_engine, load_project_env


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default="latest")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    load_project_env()
    engine = None
    result = {
        "schema": NORTHBOUND_DAILY_RESULT_SCHEMA,
        "database_writes": False,
    }
    try:
        engine = create_tool_engine(connect_args={"connect_timeout": 5, "read_timeout": 8})
        closed = authoritative_closed_trade_date(engine, close_ready_time=NORTHBOUND_CLOSE_READY_TIME)
        target = closed if args.date == "latest" else date.fromisoformat(args.date).isoformat()
        if not closed or target > closed or (args.date != "latest" and target != args.date):
            raise ValueError("northbound disclosure target must be a closed session date")
        if target != closed and target >= NORTHBOUND_DAILY_NET_DISCLOSURE_END:
            with engine.connect() as connection:
                is_open = connection.execute(text(
                    "SELECT trade_status FROM si_trade_calendar WHERE trade_date=:target"
                ), {"target": target}).scalar_one_or_none()
            if is_open != 1:
                raise ValueError("northbound disclosure target is not an exchange session")
        result["target_date"] = target
        if target < NORTHBOUND_DAILY_NET_DISCLOSURE_END:
            raise RuntimeError("historical Northbound net flow requires a dated historical source")
        result.update(status="NOT_DISCLOSED", reason_code=NORTHBOUND_NET_NOT_DISCLOSED)
    except Exception as exc:
        result.update(status="DATA_BLOCKED", error_type=type(exc).__name__, error=str(exc)[:300])
    finally:
        if engine is not None:
            engine.dispose()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "NOT_DISCLOSED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
