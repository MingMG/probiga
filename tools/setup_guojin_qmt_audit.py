#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
import sys

from sqlalchemy import create_engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.audit import ensure_audit_tables
from server.common.config import get_mysql_url


def main() -> int:
    engine = create_engine(get_mysql_url(required=True), pool_pre_ping=True, future=True)
    table_count = ensure_audit_tables(engine)
    print(json.dumps({"status": "ok", "tables_ensured": table_count}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
