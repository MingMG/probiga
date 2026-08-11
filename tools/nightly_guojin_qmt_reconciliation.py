from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.reconciliation import result_dict, run_nightly_reconciliation
from server.common.batch_db import create_batch_engine


def main() -> int:
    parser = argparse.ArgumentParser(description="Nightly Guojin QMT data coverage scan and gap registration.")
    parser.add_argument("--scan-days", type=int, default=20, help="Number of recent trading days to scan.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    args = parser.parse_args()

    engine = create_batch_engine(future=True)
    result = run_nightly_reconciliation(engine, scan_days=max(1, args.scan_days))
    payload = result_dict(result)
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    else:
        print(
            f"{result.status}: run_id={result.run_id}, target_trade_date={result.target_trade_date}, "
            f"coverage_rows={len(result.coverage)}, quality_rows={len(result.quality)}, "
            f"gaps={result.gaps_created_or_open}"
        )
    return 0 if result.status in {"SUCCESS", "WARN"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
