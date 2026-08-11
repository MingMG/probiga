# -*- coding: utf-8 -*-
"""Run the validated ETF close sync, then append the frozen forward ledger.

The job is read-only with respect to brokerage orders. It writes only
validated market data and append-only research observations.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.env_config import create_tool_engine, load_project_env


def _is_trade_day(engine: Any, day_text: str) -> bool:
    with engine.connect() as connection:
        count = connection.execute(
            text(
                """
                SELECT COUNT(*)
                  FROM si_trade_calendar
                 WHERE trade_date = :trade_date
                   AND trade_status = 1
                """
            ),
            {"trade_date": day_text},
        ).scalar()
    return bool(int(count or 0))


def _run(command: list[str], timeout: int) -> dict[str, Any]:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )
    stdout = completed.stdout.strip()
    stderr = completed.stderr.strip()
    return {
        "command": command,
        "returncode": completed.returncode,
        "stdout_tail": stdout[-4000:],
        "stderr_tail": stderr[-2000:],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Run the validated ETF sync and append the forward ledger.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=(
            ROOT
            / "artifacts"
            / "etf_forward"
            / "daily_job_latest.json"
        ),
    )
    parser.add_argument(
        "--sync-timeout-seconds",
        type=int,
        default=900,
    )
    args = parser.parse_args()
    now = datetime.now()
    day_text = now.date().isoformat()
    result: dict[str, Any] = {
        "generated_at": now.isoformat(timespec="seconds"),
        "trade_date": day_text,
        "automatic_order_submission": False,
    }
    if not args.execute:
        result["status"] = "dry_run"
    else:
        load_project_env()
        engine = create_tool_engine()
        try:
            trade_day = _is_trade_day(engine, day_text)
        finally:
            engine.dispose()
        if not trade_day:
            result["status"] = "skipped_non_trade_day"
        else:
            if now.hour * 100 + now.minute < 1500:
                raise RuntimeError(
                    "ETF forward daily job must run after 15:00"
                )
            sync_result = _run(
                [
                    sys.executable,
                    str(ROOT / "tools" / "sync_etf_history.py"),
                    "--start",
                    day_text,
                    "--end",
                    day_text,
                    "--write",
                    "--pause",
                    "0.05",
                ],
                timeout=max(60, args.sync_timeout_seconds),
            )
            result["etf_sync"] = sync_result
            if sync_result["returncode"] != 0:
                result["status"] = "etf_sync_failed"
            else:
                forward_result = _run(
                    [
                        sys.executable,
                        str(
                            ROOT
                            / "tools"
                            / "run_etf_forward_simulation.py"
                        ),
                        "--write",
                    ],
                    timeout=300,
                )
                result["forward_ledger"] = forward_result
                result["status"] = (
                    "success"
                    if forward_result["returncode"] == 0
                    else "forward_ledger_failed"
                )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["status"] in {
        "dry_run",
        "skipped_non_trade_day",
        "success",
    } else 1


if __name__ == "__main__":
    raise SystemExit(main())
