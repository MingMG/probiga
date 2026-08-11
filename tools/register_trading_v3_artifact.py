#!/usr/bin/env python3
"""Register a verified V3 walk-forward artifact without rerunning it online.

Historical walk-forward tests are intentionally executed outside the online
application host.  Production verifies the exact artifact hash, rechecks every
profit/risk gate, and only then registers the frozen calibration table.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.trading_v3.calibration import CalibrationTable
from server.trading_v3.config import load_v3_config
from server.trading_v3.repository import TradingV3Repository
from server.trading_v3.validation import model_gate_failures
from tools.env_config import create_tool_engine, load_project_env


def _require_number(
    mapping: dict[str, Any],
    key: str,
) -> float:
    value = mapping.get(key)
    if not isinstance(value, (int, float)):
        raise RuntimeError(f"artifact metric is missing or invalid: {key}")
    return float(value)


def _verify_profit_gate(
    payload: dict[str, Any],
    config: dict[str, Any],
) -> None:
    if payload.get("gate_status") != "PASS":
        raise RuntimeError("artifact did not pass its walk-forward gate")
    if list(payload.get("block_reasons") or []):
        raise RuntimeError("artifact still contains blocking reasons")

    failures = list(model_gate_failures(
        validation=dict(payload.get("validation_metrics") or {}),
        portfolio=dict(payload.get("portfolio_metrics") or {}),
        config=config,
    ))
    if failures:
        raise RuntimeError(
            "artifact fails the current production gate: "
            + ",".join(failures)
        )


def _verify_blocked_gate(
    payload: dict[str, Any],
    config: dict[str, Any],
) -> tuple[str, ...]:
    if payload.get("gate_status") != "BLOCK":
        raise RuntimeError("blocked-validation mode requires a BLOCK artifact")
    failures = model_gate_failures(
        validation=dict(payload.get("validation_metrics") or {}),
        portfolio=dict(payload.get("portfolio_metrics") or {}),
        config=config,
    )
    artifact_failures = {
        str(item) for item in payload.get("block_reasons") or []
    }
    missing = set(failures) - artifact_failures
    if not failures or missing:
        raise RuntimeError(
            "artifact BLOCK reasons do not match the current production gate: "
            + ",".join(sorted(missing))
        )
    return tuple(str(item) for item in payload.get("block_reasons") or [])


def _nullable_float(value: Any) -> float | None:
    if not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--training-start", default="2024-01-01")
    parser.add_argument("--training-end", default="2025-06-30")
    parser.add_argument("--validation-start", default="2025-07-01")
    parser.add_argument("--validation-end", default="2026-07-27")
    parser.add_argument(
        "--record-blocked-validation",
        action="store_true",
        help=(
            "Record a verified BLOCK result without registering or "
            "activating its model."
        ),
    )
    args = parser.parse_args()

    load_project_env()
    path = Path(args.artifact).resolve()
    raw = path.read_bytes()
    artifact_hash = hashlib.sha256(raw).hexdigest()
    if artifact_hash != args.expected_sha256.strip().lower():
        raise RuntimeError(
            "artifact SHA-256 mismatch; production registration refused"
        )

    payload = json.loads(raw.decode("utf-8"))
    config = load_v3_config()
    passed = payload.get("gate_status") == "PASS"
    if passed:
        _verify_profit_gate(payload, config)
        block_reasons: tuple[str, ...] = ()
    elif args.record_blocked_validation:
        block_reasons = _verify_blocked_gate(payload, config)
    else:
        _verify_profit_gate(payload, config)
    calibration = CalibrationTable.from_dict(
        dict(payload.get("calibration") or {})
    )
    feature_schema_hash = str(
        payload.get("feature_schema_hash") or ""
    ).strip()
    if not feature_schema_hash or not calibration.dataset_hash:
        raise RuntimeError(
            "artifact provenance is incomplete; production registration refused"
        )

    engine = create_tool_engine()
    now = datetime.now().replace(microsecond=0)
    if passed:
        repository = TradingV3Repository(engine)
        with engine.connect() as connection:
            existing_model_id = connection.execute(
                text(
                    """
                    SELECT model_id
                    FROM st_model_registry_v3
                    WHERE strategy_key = :strategy_key
                      AND model_version = :model_version
                      AND dataset_hash = :dataset_hash
                      AND feature_schema_hash = :feature_schema_hash
                      AND lifecycle_status = 'PAPER_ACTIVE'
                    ORDER BY activated_at DESC, created_at DESC
                    LIMIT 1
                    """
                ),
                {
                    "strategy_key": calibration.strategy_key,
                    "model_version": calibration.model_version,
                    "dataset_hash": calibration.dataset_hash,
                    "feature_schema_hash": feature_schema_hash,
                },
            ).scalar()
        if existing_model_id:
            model_id = str(existing_model_id)
            registration = "already_active"
        else:
            model_id = repository.register_model(
                calibration=calibration,
                lifecycle_status="PAPER_ACTIVE",
                training_start=date.fromisoformat(args.training_start),
                training_end=date.fromisoformat(args.training_end),
                validation_start=date.fromisoformat(args.validation_start),
                validation_end=date.fromisoformat(args.validation_end),
                feature_schema_hash=feature_schema_hash,
                metrics={
                    "training": payload["training_metrics"],
                    "validation": payload["validation_metrics"],
                    "portfolio": payload["portfolio_metrics"],
                },
                config=config,
                activated_at=now,
            )
            registration = "registered"
    else:
        model_id = ""
        registration = "blocked_validation_recorded_only"

    validation = dict(payload["validation_metrics"])
    portfolio = dict(payload["portfolio_metrics"])
    validation_id = hashlib.sha256(
        (
            calibration.model_version
            + ":"
            + artifact_hash
        ).encode("utf-8")
    ).hexdigest()[:32]
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO st_validation_result_v3 (
                    validation_id, model_version, validation_type,
                    period_start, period_end, sample_count,
                    net_expectancy_pct, payoff_ratio, profit_factor,
                    maximum_drawdown_pct, cost_total_cny,
                    opportunity_recall_at_20, result_status,
                    block_reasons_json, evidence_json, created_at
                ) VALUES (
                    :validation_id, :model_version,
                    'VERIFIED_WALK_FORWARD_ARTIFACT',
                    :period_start, :period_end, :sample_count,
                    :net_expectancy_pct, :payoff_ratio, :profit_factor,
                    :maximum_drawdown_pct, :cost_total_cny,
                    NULL, :result_status, :block_reasons_json,
                    :evidence_json, :created_at
                )
                ON DUPLICATE KEY UPDATE
                    sample_count = VALUES(sample_count),
                    net_expectancy_pct = VALUES(net_expectancy_pct),
                    payoff_ratio = VALUES(payoff_ratio),
                    profit_factor = VALUES(profit_factor),
                    maximum_drawdown_pct =
                        VALUES(maximum_drawdown_pct),
                    cost_total_cny = VALUES(cost_total_cny),
                    result_status = VALUES(result_status),
                    block_reasons_json = VALUES(block_reasons_json),
                    evidence_json = VALUES(evidence_json),
                    created_at = VALUES(created_at)
                """
            ),
            {
                "validation_id": validation_id,
                "model_version": calibration.model_version,
                "period_start": date.fromisoformat(
                    args.validation_start
                ),
                "period_end": date.fromisoformat(args.validation_end),
                "sample_count": int(validation["sample_count"]),
                "net_expectancy_pct": _nullable_float(
                    validation.get("net_expectancy_pct")
                ),
                "payoff_ratio": _nullable_float(
                    validation.get("payoff_ratio")
                ),
                "profit_factor": _nullable_float(
                    validation.get("profit_factor")
                ),
                "maximum_drawdown_pct": _nullable_float(
                    portfolio.get("maximum_drawdown_pct")
                ),
                "cost_total_cny": float(
                    portfolio.get("total_cost_cny") or 0
                ),
                "evidence_json": json.dumps(
                    {
                        "artifact": str(path),
                        "artifact_sha256": artifact_hash,
                        "registration_mode": (
                            "offline_compute_online_verify"
                        ),
                        "portfolio": portfolio,
                    },
                    ensure_ascii=False,
                    default=str,
                ),
                "result_status": "PASS" if passed else "BLOCK",
                "block_reasons_json": json.dumps(
                    block_reasons,
                    ensure_ascii=False,
                ),
                "created_at": now,
            },
        )
    engine.dispose()
    print(
        json.dumps(
            {
                "status": "ok",
                "registration": registration,
                "model_id": model_id,
                "model_version": calibration.model_version,
                "artifact_sha256": artifact_hash,
                "validation_status": "PASS" if passed else "BLOCK",
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
