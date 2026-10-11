#!/usr/bin/env python3
"""Validate and append one research-only quantitative study batch."""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.intelligence.quant_research import (  # noqa: E402
    MAX_BATCH_BYTES,
    persist_quant_research_batch,
)
from server.common.batch_db import create_batch_engine  # noqa: E402
from tools._bounded_intelligence_job import (  # noqa: E402
    IntelligenceJobReporter,
    run_bounded_intelligence_job,
)


IMPORT_DEADLINE_SECONDS = 300


def _import_worker(payload: dict, reporter: IntelligenceJobReporter) -> dict:
    reporter.stage("input_validation")
    data = json.loads(payload["raw"].decode("utf-8"))
    reporter.stage("database_engine")
    engine = create_batch_engine(
        connect_args={"connect_timeout": 10, "read_timeout": 30, "write_timeout": 30},
        pool_timeout=10,
    )
    try:
        return persist_quant_research_batch(
            engine, data, deadline_seconds=IMPORT_DEADLINE_SECONDS, stage_fn=reporter.stage,
        )
    finally:
        reporter.stage("database_engine_cleanup")
        engine.dispose()


def _accept_receipt(receipt: dict) -> bool:
    try:
        count = int(receipt.get("candidate_count") or 0)
    except (TypeError, ValueError):
        return False
    return bool(
        receipt.get("status") == "COMPLETE"
        and 3 <= count <= 5
        and re.fullmatch(r"[0-9a-f]{64}", str(receipt.get("batch_id") or ""))
        and re.fullmatch(r"[0-9a-f]{64}", str(receipt.get("payload_hash") or ""))
        and receipt.get("readback_verified") is True
        and receipt.get("research_only") is True
        and receipt.get("automatic_real_order_submission") is False
        and receipt.get("real_order_authority") is False
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", type=Path, help="UTF-8 JSON input path")
    source.add_argument("--stdin", action="store_true", help="read JSON from stdin")
    parser.add_argument("--json", action="store_true", help="emit a JSON receipt")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    def read_input() -> dict:
        if args.stdin:
            raw = sys.stdin.buffer.read(MAX_BATCH_BYTES + 1)
        else:
            with args.input.open("rb") as source:
                raw = source.read(MAX_BATCH_BYTES + 1)
        if len(raw) > MAX_BATCH_BYTES:
            raise ValueError("quant research input exceeds MAX_BATCH_BYTES")
        return {"raw": raw}

    outcome = run_bounded_intelligence_job(
        _import_worker,
        timeout_seconds=IMPORT_DEADLINE_SECONDS,
        receipt_acceptor=_accept_receipt,
        prepare_payload=read_input,
    )
    print(json.dumps(outcome.receipt, ensure_ascii=False) if args.json else outcome.receipt, flush=True)
    return outcome.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
