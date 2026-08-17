#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.kline_data import get_kline_engine
from server.trading_v3.continuous_calibration import (
    ContinuousCalibrationAlreadyRunning,
    FilesystemHorizonModelAdapter,
    ImmutableEvidenceStore,
)
from server.trading_v3.shadow_intelligence_worker import (
    run_continuous_model_lifecycle_cycle,
)
from tools.env_config import create_tool_engine, load_project_env


def _evaluated_at(value: str) -> datetime:
    if not value:
        return datetime.now(ZoneInfo("Asia/Shanghai"))
    result = datetime.fromisoformat(
        value[:-1] + "+00:00" if value.endswith("Z") else value
    )
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("--evaluated-at 必须包含时区")
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--evaluated-at",
        default="",
        help="带时区 ISO-8601；默认当前 Asia/Shanghai 时点",
    )
    parser.add_argument("--artifact-root", default="")
    parser.add_argument("--calibration-evidence-root", default="")
    parser.add_argument("--lock-timeout-seconds", type=int, default=0)
    parser.add_argument(
        "--training-timeout-seconds",
        type=int,
        default=19800,
        help="真实全市场训练 CLI 的有界超时；默认 330 分钟",
    )
    args = parser.parse_args()
    if args.lock_timeout_seconds < 0:
        parser.error("--lock-timeout-seconds must not be negative")
    if not 1 <= args.training_timeout_seconds <= 19800:
        parser.error("--training-timeout-seconds must be between 1 and 19800")
    load_project_env()
    primary = create_tool_engine()
    market = get_kline_engine()
    try:
        try:
            result = run_continuous_model_lifecycle_cycle(
                primary,
                market,
                evaluated_at=_evaluated_at(args.evaluated_at),
                lifecycle_adapter=FilesystemHorizonModelAdapter(
                    args.artifact_root or None,
                    training_timeout_seconds=args.training_timeout_seconds,
                ),
                evidence_store=ImmutableEvidenceStore(
                    args.calibration_evidence_root or None
                ),
                lock_timeout_seconds=args.lock_timeout_seconds,
            )
        except ContinuousCalibrationAlreadyRunning as exc:
            print(json.dumps({
                "status": "ALREADY_RUNNING",
                "error_code": str(exc),
                "order_authority": False,
                "real_order_allowed": False,
            }, ensure_ascii=False))
            return 75
        except Exception as exc:
            print(json.dumps({
                "status": "ERROR",
                "error_type": type(exc).__name__,
                "error_code": str(exc),
                "order_authority": False,
                "real_order_allowed": False,
            }, ensure_ascii=False), file=sys.stderr)
            return 1
        print(json.dumps(
            result,
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
            default=str,
        ))
        if result.get("status") == "READY":
            return 0
        if (
            result.get("status") == "COLLECTING"
            and result.get("forward_evidence_progress")
            == "VERIFIED_PROGRESS"
        ):
            return 0
        # A syntactically successful cycle with no artifact-bound, QMT-attested
        # forward outcomes is not scheduler readiness.  Keep it distinct from
        # an exception (1) and a policy/integrity BLOCK (2).
        return 3 if result.get("status") == "COLLECTING" else 2
    finally:
        primary.dispose()
        market.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
