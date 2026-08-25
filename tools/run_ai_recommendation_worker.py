"""Retired legacy AI recommendation queue worker.

Recommendation execution is owned exclusively by the registered production
scheduler task and its audit ledger.  This compatibility entry point performs
no database access, claims no rows and starts no subprocesses.
"""
from __future__ import annotations

import argparse
import json


RETIRED_EXIT_CODE = 78


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Retired: use the scheduler-managed "
            "analysis_premarket_external task"
        )
    )
    parser.add_argument("--json", action="store_true")
    # Parse known compatibility flags without granting them any behavior.
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--allow-intraday", action="store_true")
    parser.add_argument("--refresh-realtime", action="store_true")
    args, _unknown = parser.parse_known_args()

    payload = {
        "status": "retired",
        "exit_code": RETIRED_EXIT_CODE,
        "replacement": "scheduler:analysis_premarket_external",
        "database_access": False,
        "subprocess_started": False,
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    else:
        print(
            "AI recommendation queue worker is retired; use the "
            "scheduler-managed analysis_premarket_external task."
        )
    return RETIRED_EXIT_CODE


if __name__ == "__main__":
    raise SystemExit(main())
