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
    engine = create_batch_engine()

    def progress(payload: dict) -> None:
        if args.json:
            print(json.dumps(payload, ensure_ascii=False), flush=True)

    try:
        result = run_exposure_event_risk_watch(
            engine,
            checkpoint_root=args.checkpoint_root,
            lookback_days=args.lookback_days,
            deadline_minutes=args.deadline_minutes,
            batch_size=args.batch_size,
            progress_fn=progress,
        )
    except Exception as exc:
        result = {
            "status": "FAILED",
            "stage": getattr(exc, "stage", "exposure_event_risk_watch"),
            "error_type": type(exc).__name__,
            "message": str(exc),
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
        print(json.dumps(result, ensure_ascii=False) if args.json else result)
        return 2 if isinstance(exc, ExposureRiskWatchError) else 1
    finally:
        engine.dispose()
    print(json.dumps(result, ensure_ascii=False) if args.json else result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
