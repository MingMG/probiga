#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.kline_data import get_kline_engine
from server.trading_v3.backtest import (
    run_right_side_walk_forward,
    write_backtest_artifact,
)
from server.trading_v3.config import load_v3_config
from server.trading_v3.repository import TradingV3Repository
from tools.env_config import create_tool_engine, load_project_env
from tools.remote_support import remote_root


def _assert_resource_bounded_production() -> None:
    """Refuse an unbounded historical scan on the online application host."""
    raw = str(ROOT).replace("\\", "/").rstrip("/")
    normalized = str(ROOT.resolve()).replace("\\", "/").rstrip("/")
    production_root = remote_root()
    if raw != production_root and normalized != production_root:
        return
    if os.environ.get("INVOCATION_ID"):
        return
    raise RuntimeError(
        "production backtests must run through "
        "tools/run_production_acceptance_job.py or be imported with "
        "tools/register_trading_v3_artifact.py"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--training-start", default="2024-01-01")
    parser.add_argument("--training-end", default="2025-06-30")
    parser.add_argument("--validation-start", default="2025-07-01")
    parser.add_argument("--validation-end", default="2026-07-27")
    parser.add_argument(
        "--model-version",
        default="right_side_trend.v3.4.1-censor-safe-oos-20260801",
    )
    parser.add_argument(
        "--output",
        default=(
            "artifacts/trading_v3/"
            "right_side_trend_censor_safe_walk_forward_20260801.json"
        ),
    )
    parser.add_argument("--register-if-pass", action="store_true")
    args = parser.parse_args()
    _assert_resource_bounded_production()
    load_project_env()
    kline = get_kline_engine()
    primary = create_tool_engine()
    try:
        result = run_right_side_walk_forward(
            kline,
            training_start=date.fromisoformat(args.training_start),
            training_end=date.fromisoformat(args.training_end),
            validation_start=date.fromisoformat(args.validation_start),
            validation_end=date.fromisoformat(args.validation_end),
            model_version=args.model_version,
        )
        output_path = ROOT / args.output
        write_backtest_artifact(result, output_path)
        registered_model_id = ""
        repository = TradingV3Repository(primary)
        if args.register_if_pass and result.gate_status == "PASS":
            registered_model_id = repository.register_model(
                calibration=result.calibration,
                lifecycle_status="PAPER_ACTIVE",
                training_start=date.fromisoformat(
                    result.periods["final_calibration_start"]
                ),
                training_end=date.fromisoformat(
                    result.periods["final_calibration_end"]
                ),
                validation_start=date.fromisoformat(
                    args.validation_start
                ),
                validation_end=date.fromisoformat(args.validation_end),
                feature_schema_hash=result.feature_schema_hash,
                metrics={
                    "training": result.training_metrics,
                    "validation": result.validation_metrics,
                    "portfolio": result.portfolio_metrics,
                },
                config=load_v3_config(),
                activated_at=datetime.now().replace(microsecond=0),
            )
        with primary.begin() as connection:
            connection.execute(
                    text(
                        """
                        INSERT INTO st_validation_result_v3 (
                            validation_id, model_version,
                            validation_type, period_start, period_end,
                            sample_count, net_expectancy_pct,
                            payoff_ratio, profit_factor,
                            maximum_drawdown_pct, cost_total_cny,
                            opportunity_recall_at_20,
                            result_status, block_reasons_json,
                            evidence_json, created_at
                        ) VALUES (
                            :validation_id, :model_version,
                            'WALK_FORWARD_OOS', :period_start, :period_end,
                            :sample_count, :net_expectancy_pct,
                            :payoff_ratio, :profit_factor,
                            :maximum_drawdown_pct, :cost_total_cny,
                            NULL, :result_status, :block_reasons_json,
                            :evidence_json, :created_at
                        )
                        ON DUPLICATE KEY UPDATE
                            sample_count = VALUES(sample_count),
                            net_expectancy_pct =
                                VALUES(net_expectancy_pct),
                            payoff_ratio = VALUES(payoff_ratio),
                            profit_factor = VALUES(profit_factor),
                            maximum_drawdown_pct =
                                VALUES(maximum_drawdown_pct),
                            result_status = VALUES(result_status),
                            block_reasons_json =
                                VALUES(block_reasons_json),
                            evidence_json = VALUES(evidence_json),
                            created_at = VALUES(created_at)
                        """
                    ),
                    {
                        "validation_id": uuid.uuid4().hex,
                        "model_version": args.model_version,
                        "period_start": date.fromisoformat(
                            args.validation_start
                        ),
                        "period_end": date.fromisoformat(
                            args.validation_end
                        ),
                        "sample_count": result.validation_metrics[
                            "sample_count"
                        ],
                        "net_expectancy_pct": result.validation_metrics[
                            "net_expectancy_pct"
                        ],
                        "payoff_ratio": result.validation_metrics[
                            "payoff_ratio"
                        ],
                        "profit_factor": result.validation_metrics[
                            "profit_factor"
                        ],
                        "maximum_drawdown_pct": result.portfolio_metrics[
                            "maximum_drawdown_pct"
                        ],
                        "cost_total_cny": result.portfolio_metrics.get(
                            "total_cost_cny",
                            0,
                        ),
                        "result_status": result.gate_status,
                        "block_reasons_json": json.dumps(
                            result.block_reasons,
                            ensure_ascii=False,
                        ),
                        "evidence_json": json.dumps(
                            {
                                "artifact": str(output_path),
                                "portfolio": result.portfolio_metrics,
                            },
                            ensure_ascii=False,
                            default=str,
                        ),
                        "created_at": datetime.now().replace(
                            microsecond=0
                        ),
                    },
            )
    finally:
        kline.dispose()
        primary.dispose()
    summary = {
        "status": "ok",
        "gate_status": result.gate_status,
        "block_reasons": result.block_reasons,
        "training_metrics": result.training_metrics,
        "validation_metrics": result.validation_metrics,
        "portfolio_metrics": result.portfolio_metrics,
        "artifact": str(output_path),
        "registered_model_id": registered_model_id,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
