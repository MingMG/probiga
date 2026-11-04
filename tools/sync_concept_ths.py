#!/usr/bin/env python3
"""Refresh native THS directory and independently verified member partitions."""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from biz.stock_info.sync_stock_info import sync_concept_code_ths
from biz.stock_info.ths_members import RESULT_SCHEMA, result_hash, sync_member_partitions
from server.common.batch_db import read_frame
from server.common.authoritative_market_clock import authoritative_closed_trade_date


def resume_catalog(engine):
    """Reuse the catalog already bound by this trade date's member run."""
    as_of = authoritative_closed_trade_date(engine)
    catalog = read_frame(text(
        "SELECT index_code,concept_code,name,etl_sync_at FROM si_concept_code_ths ORDER BY index_code"
    ), engine)
    if not as_of or catalog.empty:
        return None
    records = catalog.to_dict("records")
    now = datetime.now().replace(microsecond=0)
    try:
        stamps = [datetime.fromisoformat(str(row["etl_sync_at"])) for row in records]
        if any(stamp.tzinfo or not as_of <= stamp.date().isoformat() or stamp > now for stamp in stamps):
            return None
        identities = [[str(row["index_code"]), str(row["concept_code"])] for row in records]
        if len({row[0] for row in identities}) != len(records) or len({row[1] for row in identities}) != len(records):
            return None
        digest = result_hash(identities)
        expected = {row[0] for row in identities}
        with engine.connect() as connection:
            task_ids = connection.execute(text(
                "SELECT id FROM st_scheduled_tasks WHERE script_path='tools/sync_concept_ths.py' LIMIT 2"
            )).scalars().all()
            if len(task_ids) != 1:
                return None
            outputs = connection.execute(text(
                "SELECT output FROM st_scheduled_task_history WHERE task_id=:task_id "
                "AND task_type='sync_concept_ths' AND finished_at IS NOT NULL "
                "ORDER BY id DESC LIMIT 10"
            ), {"task_id": task_ids[0]}).scalars().all()
        for output in outputs:
            for line in str(output or "").splitlines():
                try:
                    receipt = json.loads(line)
                    supplied = receipt.pop("result_sha256", None)
                    if (receipt.get("schema") == RESULT_SCHEMA and supplied == result_hash(receipt)
                            and receipt.get("provider") == "ths_native_members"
                            and receipt.get("status") in {"PARTIAL", "COMPLETE"}
                            and receipt.get("source_trade_date") == as_of
                            and receipt.get("catalog_count") == len(records)
                            and receipt.get("catalog_sha256") == digest
                            and set(receipt["completed_indices"]) | {r["index_code"] for r in receipt["failed_indices"]} == expected):
                        return catalog
                except (ValueError, TypeError, KeyError, AttributeError):
                    continue
    except (ValueError, TypeError):
        return None
    return None

def run_sync(engine):
    catalog = resume_catalog(engine)
    if catalog is None:
        catalog = sync_concept_code_ths(engine, None)
    return sync_member_partitions(engine, catalog)

def main():
    engine = create_batch_engine()
    try:
        result = run_sync(engine)
        print(json.dumps(result, ensure_ascii=False, separators=(",", ":")), flush=True)
        return 0 if result["status"] == "COMPLETE" else 2
    except Exception as exc:
        reason = str(exc)[:1200] if isinstance(exc, RuntimeError) and str(exc).startswith("THS ") else type(exc).__name__
        print(f"THS acquisition failed: {reason}", file=sys.stderr, flush=True)
        return 2
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
