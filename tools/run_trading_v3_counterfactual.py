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
from server.trading_v3.counterfactual_worker import (
    drain_counterfactual_backlog,
)
from server.trading_v3.continuous_calibration import (
    ContinuousCalibrationAlreadyRunning,
    FilesystemHorizonModelAdapter,
    ImmutableEvidenceStore,
)
from server.trading_v3.shadow_intelligence_worker import (
    run_shadow_intelligence_cycle,
)
from tools.env_config import create_tool_engine, load_project_env


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10000)
    parser.add_argument("--max-batches", type=int, default=10)
    parser.add_argument(
        "--skip-rebuild-recall",
        action="store_true",
    )
    parser.add_argument(
        "--evaluated-at",
        default="",
        help="带时区 ISO-8601；默认当前 Asia/Shanghai 时点",
    )
    parser.add_argument("--artifact-root", default="")
    parser.add_argument("--calibration-evidence-root", default="")
    parser.add_argument("--lock-timeout-seconds", type=int, default=0)
    args = parser.parse_args()
    load_project_env()
    primary = create_tool_engine()
    kline = get_kline_engine()
    try:
        legacy = drain_counterfactual_backlog(
            primary,
            kline,
            batch_size=args.limit,
            max_batches=args.max_batches,
            rebuild_recall=not args.skip_rebuild_recall,
        )
        if args.evaluated_at:
            evaluated_at = datetime.fromisoformat(
                args.evaluated_at[:-1] + "+00:00"
                if args.evaluated_at.endswith("Z")
                else args.evaluated_at
            )
            if evaluated_at.tzinfo is None:
                raise ValueError("--evaluated-at 必须包含时区")
        else:
            evaluated_at = datetime.now(ZoneInfo("Asia/Shanghai"))
        try:
            shadow = run_shadow_intelligence_cycle(
                primary,
                kline,
                evaluated_at=evaluated_at,
                lifecycle_adapter=FilesystemHorizonModelAdapter(
                    args.artifact_root or None
                ),
                evidence_store=ImmutableEvidenceStore(
                    args.calibration_evidence_root or None
                ),
                lock_timeout_seconds=max(0, args.lock_timeout_seconds),
                run_continuous_calibration=False,
            )
        except ContinuousCalibrationAlreadyRunning as exc:
            print(json.dumps({
                "status": "ALREADY_RUNNING",
                "error_code": str(exc),
                "order_authority": False,
                "real_order_allowed": False,
            }, ensure_ascii=False))
            return 75
        empty_shadow_cycle = str(shadow.get("status") or "") == "EMPTY"
        result = {
            "status": "EMPTY" if empty_shadow_cycle else "ok",
            "legacy_counterfactual": legacy,
            "shadow_intelligence": shadow,
            "forward_evidence_progress": (
                "EMPTY" if empty_shadow_cycle else "OBSERVED"
            ),
            "order_authority": False,
            "real_order_allowed": False,
        }
    finally:
        primary.dispose()
        kline.dispose()
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 3 if result["status"] == "EMPTY" else 0


if __name__ == "__main__":
    raise SystemExit(main())
