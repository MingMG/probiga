#!/usr/bin/env python3
"""Run one configured scheduler task synchronously by its task type."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.scheduler_runner import run_scheduler_task_sync
from tools.env_config import create_tool_engine, load_project_env


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-type", required=True)
    parser.add_argument("--timeout-seconds", type=int, default=900)
    args = parser.parse_args()

    load_project_env()
    engine = create_tool_engine()
    try:
        with engine.connect() as connection:
            rows = connection.execute(
                text(
                    "SELECT * FROM st_scheduled_tasks "
                    "WHERE task_type = :task_type ORDER BY id"
                ),
                {"task_type": args.task_type},
            ).mappings().all()
        if len(rows) != 1:
            raise RuntimeError(
                f"expected exactly one scheduler task for {args.task_type!r}; "
                f"found {len(rows)}"
            )
        result = run_scheduler_task_sync(
            dict(rows[0]),
            root=ROOT,
            engine=engine,
            timeout_seconds=args.timeout_seconds,
        )
    finally:
        engine.dispose()
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result["status"] in {"success", "blocked"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
