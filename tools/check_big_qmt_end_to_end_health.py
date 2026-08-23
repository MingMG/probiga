from __future__ import annotations

"""Read-only end-to-end health probe for the local standard-QMT bridge."""

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.bigqmt.health import evaluate_spool_health
from integrations.bigqmt.spool import resolve_big_qmt_home


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--heartbeat-max-age", type=float, default=30)
    parser.add_argument("--full-max-age", type=float, default=75)
    parser.add_argument("--receipt-max-age", type=float, default=75)
    parser.add_argument("--level1-max-age", type=float, default=15)
    level1 = parser.add_mutually_exclusive_group()
    level1.add_argument("--require-level1", action="store_true")
    level1.add_argument("--skip-level1", action="store_true")
    args = parser.parse_args()

    home = resolve_big_qmt_home(required=False)
    if home is None:
        result = {
            "healthy": False,
            "status": "BLOCK",
            "reason": "QMT_HOME_NOT_FOUND",
            "checks": {
                "strategy_heartbeat": False,
                "full_market_snapshot": False,
                "sync_receipt": False,
                "level1_callback": False,
            },
            "failed_checks": [
                "strategy_heartbeat",
                "full_market_snapshot",
                "sync_receipt",
                "level1_callback",
            ],
        }
    else:
        result = evaluate_spool_health(
            home,
            heartbeat_max_age_seconds=args.heartbeat_max_age,
            full_snapshot_max_age_seconds=args.full_max_age,
            sync_receipt_max_age_seconds=args.receipt_max_age,
            level1_callback_max_age_seconds=args.level1_max_age,
            require_level1_callback=(
                True
                if args.require_level1
                else False if args.skip_level1 else None
            ),
        )
        result["qmt_home"] = str(home)
    print(
        json.dumps(result, ensure_ascii=False, default=str)
        if args.json
        else result
    )
    return 0 if result["healthy"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
