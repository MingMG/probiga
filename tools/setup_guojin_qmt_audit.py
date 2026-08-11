#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.audit import ensure_audit_tables
from server.common.batch_db import create_batch_engine


def main() -> int:
    engine = create_batch_engine(future=True)
    table_count = ensure_audit_tables(engine)
    print(json.dumps({"status": "ok", "tables_ensured": table_count}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
