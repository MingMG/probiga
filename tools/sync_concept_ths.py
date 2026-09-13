#!/usr/bin/env python3
"""Refresh native THS directory and independently verified member partitions."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from biz.stock_info.sync_stock_info import sync_concept_code_ths
from biz.stock_info.ths_members import sync_member_partitions

def run_sync(engine):
    return sync_member_partitions(engine, sync_concept_code_ths(engine, None))

def main():
    engine = create_batch_engine()
    try:
        result = run_sync(engine)
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")), flush=True)
        return 0 if result["status"] == "COMPLETE" else 2
    except Exception as exc:
        print(f"THS directory acquisition failed: {type(exc).__name__}", file=sys.stderr, flush=True)
        return 2
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
