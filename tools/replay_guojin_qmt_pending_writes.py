from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import create_engine

from server.common.config import get_mysql_url

from integrations.qmt.pending_write import replay_pending_writes, result_dict


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay local Guojin QMT pending database writes.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of pending writes to replay.")
    parser.add_argument("--pending-root", default=None, help="Override pending write root directory.")
    args = parser.parse_args()

    engine = create_engine(get_mysql_url(required=True), pool_pre_ping=True, future=True)
    result = replay_pending_writes(engine, pending_root=args.pending_root, limit=args.limit)
    print(json.dumps(result_dict(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result.failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
