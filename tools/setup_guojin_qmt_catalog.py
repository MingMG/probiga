#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.catalog import (
    complete_capability_ledger,
    save_capabilities,
    validate_catalog_registry_seed,
    validate_catalog_schema,
)
from integrations.bigqmt.diagnostics import probe_capabilities
from server.common.config import get_mysql_url
from server.common.engine_factory import create_pooled_engine as create_engine


def main() -> int:
    engine = create_engine(get_mysql_url(required=True), pool_pre_ping=True, future=True)
    schema_result = validate_catalog_schema(engine)
    seed_result = validate_catalog_registry_seed(engine)
    capability_result, core_result = probe_capabilities(timeout=240, engine=engine)
    capability_count = save_capabilities(engine, capability_result, core_result)
    pending_count = complete_capability_ledger(engine)
    result = {
        "status": "error" if core_result.get("status") == "error" else "ok",
        "catalog_schema": schema_result,
        "catalog_seed": seed_result,
        "registry_rows": seed_result["active_registry_rows"],
        "capability_rows": capability_count,
        "pending_capability_rows": pending_count,
        "core_status": core_result.get("status"),
        "source": core_result.get("source"),
        "model_instance_id": core_result.get("model_instance_id"),
    }
    print(json.dumps(result, ensure_ascii=False))
    return 1 if result["status"] == "error" else 0


if __name__ == "__main__":
    raise SystemExit(main())
