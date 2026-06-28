#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
import sys

from sqlalchemy import create_engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.catalog import complete_capability_ledger, ensure_catalog_tables, save_capabilities, seed_registry
from integrations.qmt.diagnostics import capabilities, core_probe
from server.common.config import get_mysql_url


def main() -> int:
    engine = create_engine(get_mysql_url(required=True), pool_pre_ping=True, future=True)
    ensure_catalog_tables(engine)
    registry_count = seed_registry(engine)
    capability_result = capabilities(timeout=30, force=True)
    core_result = core_probe(timeout=45, force=True)
    capability_count = save_capabilities(engine, capability_result, core_result)
    pending_count = complete_capability_ledger(engine)
    result = {
        "status": "ok",
        "registry_rows": registry_count,
        "capability_rows": capability_count,
        "pending_capability_rows": pending_count,
        "core_status": core_result.get("status"),
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
