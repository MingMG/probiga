#!/usr/bin/env python3
"""Run the bounded official-announcement watch for current stock exposure."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.intelligence.exposure_risk_watch import (  # noqa: E402
    DEFAULT_CHECKPOINT_ROOT,
    ExposureRiskWatchError,
    run_exposure_event_risk_watch,
)
from server.common.batch_db import create_batch_engine  # noqa: E402
from tools._bounded_intelligence_job import (  # noqa: E402
    IntelligenceJobReporter,
    run_bounded_intelligence_job,
)


def _risk_worker(payload: dict, reporter: IntelligenceJobReporter) -> dict:
    reporter.stage("input_validation")
    if not 1 <= payload["lookback_days"] <= 14:
        raise ValueError("lookback_days must be 1..14")
    if not 5 <= payload["deadline_minutes"] <= 30:
        raise ValueError("deadline_minutes must be 5..30")
    if not 1 <= payload["batch_size"] <= 4:
        raise ValueError("batch_size must be 1..4")
    reporter.stage("database_engine")
    engine = create_batch_engine(
        connect_args={"connect_timeout": 10, "read_timeout": 30, "write_timeout": 30},
        pool_timeout=10,
    )
    try:
        return run_exposure_event_risk_watch(
            engine,
            checkpoint_root=Path(payload["checkpoint_root"]),
            lookback_days=payload["lookback_days"],
            deadline_minutes=payload["deadline_minutes"],
            batch_size=payload["batch_size"],
            progress_fn=reporter.progress,
            stage_fn=reporter.stage,
        )
    finally:
        reporter.stage("database_engine_cleanup")
        engine.dispose()


def _accept_receipt(receipt: dict) -> bool:
    try:
        total = int(receipt.get("total_units") or 0)
        completed = int(receipt.get("completed_units") or 0)
        coverage = int(receipt.get("coverage_count") or 0)
    except (TypeError, ValueError):
        return False
    return bool(
        receipt.get("status") == "COMPLETE"
        and receipt.get("coverage_readback") is True
        and receipt.get("automatic_real_order_submission") is False
        and receipt.get("real_order_authority") is False
        and total == completed
        and (
            total == 0 and receipt.get("reason") == "NO_CURRENT_EXPOSURE"
            or total > 0 and coverage == total
        )
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--checkpoint-root",
        type=Path,
        default=DEFAULT_CHECKPOINT_ROOT,
    )
    parser.add_argument("--lookback-days", type=int, default=3)
    parser.add_argument("--deadline-minutes", type=int, default=20)
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    def progress(payload: dict) -> None:
        if args.json:
            print(json.dumps(payload, ensure_ascii=False), flush=True)

    outcome = run_bounded_intelligence_job(
        _risk_worker,
        {
            "checkpoint_root": str(args.checkpoint_root),
            "lookback_days": args.lookback_days,
            "deadline_minutes": args.deadline_minutes,
            "batch_size": args.batch_size,
        },
        timeout_seconds=args.deadline_minutes * 60 if 5 <= args.deadline_minutes <= 30 else 1,
        receipt_acceptor=_accept_receipt,
        progress_fn=progress,
        exception_exit_codes={ExposureRiskWatchError.__name__: 2},
    )
    print(json.dumps(outcome.receipt, ensure_ascii=False) if args.json else outcome.receipt, flush=True)
    return outcome.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
