from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sqlalchemy import create_engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.gap_repair import plan_gap_repairs, result_dict
from server.common.config import get_mysql_url


def main() -> int:
    parser = argparse.ArgumentParser(description="Plan or lock Guojin QMT historical data gaps for later repair.")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--apply", action="store_true", help="Mark selected gaps as RETRYING. Default is dry-run.")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    engine = create_engine(get_mysql_url(required=True), pool_pre_ping=True, future=True)
    plan = plan_gap_repairs(engine, limit=args.limit, apply=args.apply)
    payload = result_dict(plan)
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    else:
        print(f"{plan.mode}: selected={plan.selected}, locked={plan.locked}")
        for item in plan.items:
            print(f"- #{item.id} {item.dataset} {item.gap_start}~{item.gap_end}: {item.action}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
