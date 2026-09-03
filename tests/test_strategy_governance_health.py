from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal
import hashlib
import inspect
import json
import math
import re
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

from tools import check_strategy_governance_health as health
from tools import attest_qmt_daily_kline as qmt_attester
from tools.strategy_governance_task_contract import TASK
from tools.qmt_announcement_task_contract import (
    ANALYSIS_FAST_CRON,
    ANALYSIS_UPPER_EVIDENCE_CRON,
    TASK as QMT_ANNOUNCEMENT_TASK,
)
from tools.qmt_operations_task_contract import TASKS as QMT_OPERATIONS_TASKS
from server.common.qmt_stock_catalog import A_SHARE_STOCK_CODE_SQL_REGEXP


BUILD_SHA = "a" * 40
TRADE_DATE = "2026-08-21"
MARKET_STATE = "trend_bullish"
MARKET_CONFIG_VERSION, MARKET_CONFIG_HASH = (
    health._market_router_config_contract()
)
ROUTER_POLICY = {
    "trend_bullish": 1.0,
    "high_range": 0.5,
    "risk_declining": 0.2,
    "extreme_event": 0.0,
}
_REAL_FUNDING_MANIFEST_PERSISTENCE_CHECK = (
    health._funding_manifest_persistence_check
)
_REAL_QMT_REFERENCE_FROZEN_SCHEMA_CHECK = (
    health._qmt_reference_frozen_schema_check
)
_REAL_QMT_HISTORY_COVERAGE_FROZEN_SCHEMA_CHECK = (
    health._qmt_history_coverage_frozen_schema_check
)
_REAL_QMT_WINDOWS_EDGE_EXECUTOR_CHECK = (
    health.check_qmt_windows_edge_executor
)


def test_governance_health_retries_one_operational_error_with_fresh_engine(
    monkeypatch,
):
    class _Engine:
        def __init__(self):
            self.disposed = False

        def dispose(self):
            self.disposed = True

    engines = [_Engine(), _Engine()]
    sleeps = []
    calls = []

    def collect(engine, **kwargs):
        calls.append((engine, kwargs))
        if len(calls) == 1:
            raise OperationalError("SELECT 1", {}, RuntimeError("disconnect"))
        return {"status": "PASS"}

    monkeypatch.setattr(health, "collect_governance_health", collect)
    result, current = health._collect_governance_health_with_operational_retry(
        lambda: engines.pop(0),
        expected_build_sha=BUILD_SHA,
        expected_trade_date=TRADE_DATE,
        allow_input_not_ready=True,
        expected_scheduler_pid=123,
        sleep=sleeps.append,
    )

    assert result == {"status": "PASS"}
    assert current is calls[1][0]
    assert calls[0][0].disposed is True
    assert calls[1][0].disposed is False
    assert sleeps == [1.0]
_REAL_QMT_HISTORY_CAPABILITY_MATRIX_CHECK = (
    health._qmt_history_capability_matrix_check
)
_REAL_SCHEDULER_TASK_HISTORY_FROZEN_SCHEMA_CHECK = (
    health._scheduler_task_history_frozen_schema_check
)
_REAL_PIT_FACT_FROZEN_SCHEMA_CHECK = health._pit_fact_frozen_schema_check
_REAL_LATEST_QMT_ANNOUNCEMENT_BATCH_CHECK = (
    health._latest_qmt_announcement_batch_check
)


@pytest.fixture(autouse=True)
def _exact_funding_schema_contract(monkeypatch):
    from server.db import migrations_v4

    # Most fixtures predate the production cutover and exercise the generic
    # replay algorithm.  Cutover-specific tests override this explicitly.
    monkeypatch.setattr(
        health,
        "STRATEGY_INDUSTRY_HISTORY_PRODUCTION_CUTOVER_DATE",
        TRADE_DATE,
    )
    monkeypatch.setattr(
        migrations_v4,
        "run_v4_migrations",
        lambda _engine, *, dry_run: [
            SimpleNamespace(
                version=str(migration["version"]),
                status="would_apply",
            )
            for migration in migrations_v4.MIGRATIONS
        ]
        if dry_run
        else pytest.fail("health fixture must not apply V4 migrations"),
    )
    monkeypatch.setattr(
        health,
        "validate_strategy_funding_checkpoint_schema",
        lambda _connection: {
            "table_count": 2,
            "tables": {
                **health.EXPECTED_FUNDING_TABLE_COUNTS,
            },
            "trigger_count": len(
                health.FUNDING_CHECKPOINT_TRIGGER_CONTRACTS
            ),
            "contract_hash": health.FUNDING_CHECKPOINT_SCHEMA_CONTRACT_HASH,
            "rolling_history_storage": (
                "ADDRESSABLE_APPEND_ONLY_DAILY_FACT_CHAIN"
            ),
            "checkpoint_target_average_bytes": (
                health.FUNDING_CHECKPOINT_TARGET_AVG_BYTES
            ),
            "checkpoint_total_target_bytes": (
                health.FUNDING_CHECKPOINT_TOTAL_TARGET_BYTES
            ),
            "checkpoint_total_hard_bytes": (
                health.FUNDING_CHECKPOINT_TOTAL_HARD_BYTES
            ),
            "batch_max_rows": health.FUNDING_CHECKPOINT_BATCH_MAX_ROWS,
            "batch_max_bytes": health.FUNDING_CHECKPOINT_BATCH_MAX_BYTES,
            "manifest_max_bytes": health.FUNDING_CHECKPOINT_MANIFEST_MAX_BYTES,
            "audit_max_bytes": health.FUNDING_CHECKPOINT_AUDIT_MAX_BYTES,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        },
    )
    monkeypatch.setattr(
        health,
        "_funding_manifest_persistence_check",
        lambda _connection, *, run, result, trade_date: (
            True,
            {
                "run_uid": run.get("run_uid"),
                "manifest_hash": (
                    (result.get("funding_checkpoint_manifest") or {}).get(
                        "manifest_hash", "fixture"
                    )
                ),
                "trade_date": trade_date,
                "invalid_count": 0,
                "errors": [],
            },
        ),
    )
    monkeypatch.setattr(
        health,
        "_qmt_reference_frozen_schema_check",
        lambda _engine: (
            True,
            {
                "contract_hash": "b" * 64,
                "trigger_count": 10,
                "expected_trigger_count": 10,
                "physical_schema_verified": True,
                "physical_seal_verified": True,
            },
        ),
    )
    monkeypatch.setattr(
        health,
        "_qmt_history_coverage_frozen_schema_check",
        lambda _connection: (
            True,
            {
                "valid": True,
                "table_count": 2,
                "trigger_count": 4,
                "expected_trigger_count": 4,
                "physical_schema_verified": True,
            },
        ),
    )
    monkeypatch.setattr(
        health,
        "check_qmt_windows_edge_executor",
        lambda _connection, *, expected_build_sha: (
            True,
            {
                "status": "AVAILABLE",
                "strategy_eligible": True,
                "build_sha": expected_build_sha,
                "executor_role": "qmt_windows_edge",
                "task_last_success_count": 3,
            },
        ),
    )
    monkeypatch.setattr(
        health,
        "check_qmt_windows_edge_release_receipt",
        lambda _connection, *, expected_build_sha: (
            True,
            {
                "status": "AVAILABLE",
                "strategy_eligible": True,
                "expected_build_sha": expected_build_sha,
                "expected_poll_seconds": 60,
                "receipt_count": 1,
                "immutable_reference_verified": True,
                "identity": {
                    "current": {
                        "instance_id": "win-qmt-edge-01-9191",
                        "host_name": "win-qmt-edge-01",
                        "pid": 9191,
                        "build_sha": expected_build_sha,
                        "executor_role": "qmt_windows_edge",
                    }
                },
                "receipt": {
                    "build_sha": expected_build_sha,
                    "request_run_uid": (
                        f"qmt-edge-request-{expected_build_sha}"
                    ),
                    "host_name": "win-qmt-edge-01",
                    "scheduler_instance_id": "win-qmt-edge-01-9191",
                    "catalog_batch_id": (
                        f"qmt_rel_{expected_build_sha}_20260825120000"
                    ),
                    "calendar_batch_id": (
                        f"qmt_rel_{expected_build_sha}_20260825120000"
                    ),
                    "receipt_hash": "d" * 64,
                },
                "errors": [],
            },
        ),
    )
    monkeypatch.setattr(
        health,
        "_qmt_history_capability_matrix_check",
        lambda _connection: (
            True,
            {
                "status": "HEALTHY",
                "dataset_count": 19,
                "strategy_eligible_dataset_count": 0,
                "fail_closed_verified": True,
                "automatic_real_order_submission": False,
                "real_order_authority": False,
            },
        ),
    )
    monkeypatch.setattr(
        health,
        "_scheduler_task_history_frozen_schema_check",
        lambda _engine: (
            True,
            {
                "physical_contract_verified": True,
                "runtime_ddl_required": False,
                "read_only": True,
            },
        ),
    )
    monkeypatch.setattr(
        health,
        "_pit_fact_frozen_schema_check",
        lambda _engine: (
            True,
            {
                "valid": True,
                "table_count": 3,
                "trigger_count": 6,
                "expected_trigger_count": 6,
                "physical_schema_verified": True,
            },
        ),
    )
    monkeypatch.setattr(
        health,
        "_latest_qmt_announcement_batch_check",
        lambda _engine, trade_date: (
            bool(trade_date),
            {
                "status": "COMPLETE" if trade_date else "DATA_BLOCKED",
                "trade_date": trade_date,
                "batch_id": "qmt-announcement-health-fixture",
                "batch_root_hash": "c" * 64,
                "catalog_member_count": 2,
                "coverage_row_count": 2,
                "funding_eligible": bool(trade_date),
                "automatic_real_order_submission": False,
                "real_order_authority": False,
            },
        ),
    )


def _portfolio_risk_metrics(seed: int, stock_code: str) -> dict:
    start = date(2026, 1, 1)
    records = [
        {
            "trade_date": (start + timedelta(days=index)).isoformat(),
            "return_pct": round(
                math.sin(
                    2 * math.pi * (seed + 1) * (index + 1) / 60
                ),
                8,
            ),
        }
        for index in range(60)
    ]
    return {
        "internal_daily_records": records,
        "internal_daily_stock_market_values": [
            {
                "trade_date": row["trade_date"],
                "stock_risk_exposure": {stock_code: "10000"},
            }
            for row in records
        ],
    }


def _attested_session_window(window_days: int) -> dict:
    sessions = []
    cursor = date.fromisoformat(TRADE_DATE)
    while len(sessions) < window_days:
        if cursor.weekday() < 5:
            sessions.append(cursor.isoformat())
        cursor -= timedelta(days=1)
    sessions.reverse()
    stock_codes = ["000001", "600000"]
    calendar_payload = {
        "schema": "probiga.governance-calendar-receipt-binding.v1",
        "batch_id": "health-calendar-fixture",
        "known_at": f"{TRADE_DATE} 15:30:00",
        "start_date": sessions[0],
        "end_date": sessions[-1],
        "session_count": len(sessions),
        "session_set_hash": health._canonical_digest({
            "schema": "probiga.governance-calendar-session-set.v1",
            "sessions": sessions,
        }),
        "manifest_hash": health._canonical_digest({
            "schema": "probiga.governance-calendar-manifest.v1",
            "sessions": sessions,
        }),
    }
    calendar_receipt = {
        **calendar_payload,
        "binding_hash": health._canonical_digest(calendar_payload),
    }
    payload = {
        "schema": "probiga.authoritative-session-window.v1",
        "window_days": window_days,
        "start_date": sessions[0],
        "end_date": sessions[-1],
        "session_count": len(sessions),
        "sessions": sessions,
        "calendar_receipt": calendar_receipt,
        "calendar_receipt_binding_hash": calendar_receipt["binding_hash"],
        "calendar_manifest_hash": calendar_receipt["manifest_hash"],
        "calendar_session_set_hash": calendar_receipt["session_set_hash"],
        "session_attestations": [
            {
                "trade_date": day,
                "attested_bar_count": len(stock_codes),
                "expected_stock_count": (
                    qmt_attester.expected_stock_set_contract(
                        day,
                        stock_codes,
                    )["stock_count"]
                ),
                "expected_stock_set_hash": (
                    qmt_attester.expected_stock_set_contract(
                        day,
                        stock_codes,
                    )["stock_set_hash"]
                ),
                "batch_count": 1,
                "min_data_version": "qmt-close-v1",
                "max_data_version": "qmt-close-v1",
                "latest_received_at": f"{day}T15:05:00",
                "pre_close_attestation_protocol": (
                    health.QMT_PRECLOSE_ATTESTATION_PROTOCOL
                ),
            }
            for day in sessions
        ],
    }
    return {
        **payload,
        "session_hash": health._canonical_digest(payload),
    }


def _row_binding_proof(window: dict) -> dict:
    sessions = []
    for attestation in window["session_attestations"]:
        stock_count = attestation["expected_stock_count"]
        stock_hash = attestation["expected_stock_set_hash"]
        sessions.append({
            "trade_date": attestation["trade_date"],
            "target_stock_count": stock_count,
            "target_stock_set_hash": stock_hash,
            "completed_attestation_stock_count": stock_count,
            "completed_attestation_stock_set_hash": stock_hash,
            "exact_attestation_stock_count": stock_count,
            "exact_attestation_stock_set_hash": stock_hash,
            "attested_bar_count": stock_count,
            "matching_completed_manifest_run_count": 1,
        })
    payload = {
        "schema": "probiga.qmt-row-binding-proof.v1",
        "as_of_date": TRADE_DATE,
        "start_date": window["start_date"],
        "end_date": TRADE_DATE,
        "session_count": len(sessions),
        "protocol_version": health.QMT_PRECLOSE_ATTESTATION_PROTOCOL,
        "source_pre_close_origin": "NATIVE_QMT",
        "row_run_binding": "SAME_COMPLETED_RUN_ID",
        "sessions": sessions,
    }
    return {**payload, "proof_hash": health._canonical_digest(payload)}


def _valid_external_artifact_fixture():
    from server.engine import strategy_governance as governance_module

    session_window = _attested_session_window(60)
    sessions = session_window["sessions"]
    candidate_positions = [
        index for index, day in enumerate(sessions)
        if day >= "2026-07-06" and index + 2 < len(sessions)
    ][::4][:5]
    test_days = [date.fromisoformat(sessions[index]) for index in candidate_positions]
    label_days = [sessions[index + 2] for index in candidate_positions]
    net_returns = [1.0, 1.0, 1.0, 1.0, -0.5]
    trades = [
        {
            "evidence_id": health._canonical_digest({
                "schema": "probiga.validation-sample-id.v1",
                "source_key": f"health-trade-{index}",
            }),
            "trade_date": day.isoformat(),
            "label_available_at": label_day + "T15:00:00",
            "observed_at": label_day + "T15:00:00",
            "net_return_pct": net_return,
            "cost_pct": 0.1,
        }
        for index, (day, label_day, net_return) in enumerate(
            zip(test_days, label_days, net_returns), 1
        )
    ]
    equity_curve = governance_module._rebuild_equity_curve(trades)
    peak = 100.0
    max_drawdown = 0.0
    for point in equity_curve:
        peak = max(peak, point["equity"])
        max_drawdown = max(
            max_drawdown,
            (peak - point["equity"]) / peak * 100.0,
        )
    wins = [value for value in net_returns if value > 0]
    losses = [value for value in net_returns if value < 0]
    net_profit = sum(net_returns)
    gross_values = [value + 0.1 for value in net_returns]
    metrics = governance_module._validated_metric_evidence(
        {
            "completed_trades": len(trades),
            "coverage_days": len(trades),
            "win_rate_pct": len(wins) / len(trades) * 100.0,
            "average_win_pct": sum(wins) / len(wins),
            "average_loss_pct": abs(sum(losses) / len(losses)),
            "payoff_ratio": (
                sum(wins) / len(wins) / abs(sum(losses) / len(losses))
            ),
            "gross_expectancy_pct": sum(gross_values) / len(trades),
            "estimated_cost_pct": 0.1,
            "net_expectancy_pct": net_profit / len(trades),
            "profit_factor": sum(wins) / abs(sum(losses)),
            "max_drawdown_pct": max_drawdown,
            "walk_forward_segments": 5,
            "positive_segments": 4,
            "cost_stress_expectancy_pct": (
                sum(gross_values) / len(trades)
                - 0.1
                * governance_module.PROFIT_GATE_POLICY[
                    "cost_stress_multiple"
                ]
            ),
            "top5_profit_contribution_pct": (
                sum(sorted(wins, reverse=True)[:5]) / net_profit * 100.0
            ),
            "market_match_score": 100.0,
            "walk_forward_verified": True,
            "independent_oos": True,
            "drawdown_basis": "sequential_trade_compounded_equity",
            "cost_basis": "validated_fee_model_v1",
        }
    )
    segments = []
    for index, (test_day, trade) in enumerate(zip(test_days, trades), 1):
        train_start = "2026-06-01"
        train_end = (test_day - timedelta(days=3)).isoformat()
        train_dataset = [
            {
                "observation_id": health._canonical_digest({
                    "schema": "probiga.validation-sample-id.v1",
                    "source_key": f"health-train-{index}",
                }),
                "observed_at": "2026-06-01T15:00:00",
                "label_available_at": "2026-06-03T15:00:00",
                "feature_snapshot_hash": health._canonical_digest(
                    {"segment": index, "kind": "feature"}
                ),
                "label_snapshot_hash": health._canonical_digest(
                    {"segment": index, "kind": "label"}
                ),
            }
        ]
        segments.append(
            {
                "train_start": train_start,
                "train_end": train_end,
                "test_start": test_day.isoformat(),
                "test_end": test_day.isoformat(),
                "completed_trades": 1,
                "net_expectancy_pct": trade["net_return_pct"],
                "train_dataset": train_dataset,
                "train_dataset_hash": health._canonical_digest(
                    {
                        "segment_index": index,
                        "train_start": train_start,
                        "train_end": train_end,
                        "observations": train_dataset,
                    }
                ),
                "test_dataset_hash": health._canonical_digest(
                    {
                        "segment_index": index,
                        "test_start": test_day.isoformat(),
                        "test_end": test_day.isoformat(),
                        "trades": [trade],
                    }
                ),
            }
        )
    revision_at = trades[-1]["observed_at"]
    artifact = {
        "schema_version": "probiga.strategy-validation-artifact.v3",
        "entity_type": "STRATEGY",
        "entity_key": "strategy_a",
        "entity_version": "v1",
        "as_of_date": TRADE_DATE,
        "window_days": 60,
        "evidence_protocol": "PURGED_WALK_FORWARD_V2",
        "evidence_revision_at": revision_at,
        "metrics_hash": health._canonical_digest(metrics),
        "window_session_start": session_window["start_date"],
        "window_session_end": session_window["end_date"],
        "window_session_count": session_window["session_count"],
        "window_session_hash": session_window["session_hash"],
        "trades": trades,
        "equity_curve": equity_curve,
        "segments": segments,
        "validation_protocol": {
            "label_horizon_days": 2,
            "max_holding_days": 2,
            "purge_days": 2,
            "embargo_days": 2,
        },
    }
    artifact["source_dataset_hash"] = health._canonical_digest(
        {"trades": trades, "equity_curve": equity_curve}
    )
    return (
        metrics,
        artifact,
        health._canonical_digest(artifact),
        revision_at,
        session_window,
    )


def _strategy_evaluator_config() -> dict:
    return {
        "market_regime_multipliers": ROUTER_POLICY,
        "market_router_policy_version": health.ROUTER_POLICY_VERSION,
        "market_state_config_version": MARKET_CONFIG_VERSION,
        "market_state_config_hash": MARKET_CONFIG_HASH,
    }


def _strategy_version_hash(strategy_key: str) -> str:
    return health._canonical_digest(
        {
            "schema": "probiga.strategy-version.v1",
            "strategy_key": strategy_key,
            "version": "v1",
            "evaluator_type": "external_evidence",
            "evaluator_config": _strategy_evaluator_config(),
            "parameters": {},
            "source_kind": "runtime_registry",
        }
    )


def _strategy_content_hash(strategy_key: str) -> str:
    return health._canonical_digest(
        {
            "schema": "probiga.strategy-content.v1",
            "strategy_key": strategy_key,
            "evaluator_type": "external_evidence",
            "evaluator_config": _strategy_evaluator_config(),
            "parameters": {},
            "source_kind": "runtime_registry",
        }
    )


def _strategy_route(strategy_key: str) -> dict:
    version_hash = _strategy_version_hash(strategy_key)
    payload = {
        "schema": "probiga.strategy-market-route.v1",
        "policy_version": health.ROUTER_POLICY_VERSION,
        "strategy_key": strategy_key,
        "strategy_version": "v1",
        "trade_date": TRADE_DATE,
        "data_date": TRADE_DATE,
        "market_state": MARKET_STATE,
        "market_state_config_hash": MARKET_CONFIG_HASH,
        "route_source": "immutable_strategy_version",
        "source_binding": {
            "version_hash": version_hash,
            "policy": ROUTER_POLICY,
            "policy_version": health.ROUTER_POLICY_VERSION,
            "market_state_config_version": MARKET_CONFIG_VERSION,
            "market_state_config_hash": MARKET_CONFIG_HASH,
        },
        "multiplier": 1.0,
        "market_match_score": 100.0,
        "eligible": True,
        "reason": "适配当前市场状态",
    }
    return {
        **payload,
        "router_decision_hash": health._canonical_digest(payload),
    }


STRATEGY_ROUTES = {
    key: _strategy_route(key) for key in ("strategy_a", "strategy_b")
}
STRATEGY_WINDOW_EVIDENCE = {
    key: {
        str(window): health._canonical_digest(
            {"strategy_key": key, "window_days": window}
        )
        for window in health.EXPECTED_WINDOWS
    }
    for key in STRATEGY_ROUTES
}
STRATEGY_PRE_GATE_HASHES = {
    key: health._canonical_digest(
        {
            "strategy_key": key,
            "strategy_version": "v1",
            "window_evidence": STRATEGY_WINDOW_EVIDENCE[key],
            "router_decision_hash": route["router_decision_hash"],
            "overall_gate_passed": True,
        }
    )
    for key, route in STRATEGY_ROUTES.items()
}


def _fixture_statistical_decision(
    *, entity_type: str, entity_key: str, passed: bool = True,
) -> dict:
    from server.engine import strategy_governance as governance_module

    payload = {
        "schema": "probiga.strategy-family-by-decision-compact.v1",
        "entity_type": entity_type,
        "entity_key": entity_key,
        "entity_version": "v1",
        "family_id": f"fixture-{entity_type.lower()}",
        "trial_key": f"{entity_type}:{entity_key}:fixture",
        "valid": True,
        "passed": bool(passed),
        "reason": "fixture BY decision",
        "q": 0.05,
        "total_hypotheses": 3,
        "candidate_p_value": 0.000001 if passed else 1.0,
        "rank": 1 if passed else 3,
        "critical_value": 0.001 if passed else 0.0,
        "trial_inventory_hash": "1" * 64,
        "trial_inventory_state_hash": "2" * 64,
        "statistical_policy_hash": governance_module.STATISTICAL_POLICY_HASH,
        "source_by_result_hash": "3" * 64,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    return {**payload, "decision_hash": health._canonical_digest(payload)}


def _fixture_confirmation(*, passed: bool) -> dict:
    from server.engine import strategy_governance as governance_module

    payload = {
        "schema": "probiga.strategy-spaced-confirmation-compact.v1",
        "valid": bool(passed),
        "passed": bool(passed),
        "reason": "fixture confirmation" if passed else "fixture shadow",
        "minimum_new_sessions": 20,
        "required_total_confirmations": 3,
        "prior_confirmation_count": 2 if passed else 0,
        "total_confirmation_count": 3 if passed else 0,
        "continuous_session_count": 41 if passed else 0,
        "milestone_count": 3 if passed else 0,
        "milestone_set_hash": "4" * 64,
        "milestone_dates": (
            ["2026-06-01", "2026-07-01", TRADE_DATE] if passed else []
        ),
        "full_input_hash": "5" * 64 if passed else "",
        "full_parameter_hash": "6" * 64 if passed else "",
        "full_result_hash": "7" * 64 if passed else "",
        "statistical_policy_hash": governance_module.STATISTICAL_POLICY_HASH,
        "calendar_manifest_hash": "8" * 64 if passed else "",
        "calendar_session_set_hash": "9" * 64 if passed else "",
        "calendar_receipt_binding_hash": "a" * 64 if passed else "",
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    return {**payload, "compact_hash": health._canonical_digest(payload)}


STRATEGY_STATISTICAL_DECISIONS = {
    key: _fixture_statistical_decision(
        entity_type="STRATEGY", entity_key=key,
    )
    for key in STRATEGY_ROUTES
}
STRATEGY_CONFIRMATIONS = {
    key: _fixture_confirmation(passed=key == "strategy_a")
    for key in STRATEGY_ROUTES
}


def _final_funding_hash(
    *, entity_type: str, entity_key: str, pre_gate_hash: str,
    decision: dict, confirmation: dict, projected_status: str,
    paper_eligible: bool,
) -> str:
    return health._canonical_digest({
        "schema": "probiga.strategy-final-funding-gate.v1",
        "entity_type": entity_type,
        "entity_key": entity_key,
        "entity_version": "v1",
        "pre_confirmation_funding_gate_hash": pre_gate_hash,
        "statistical_family_decision_hash": decision["decision_hash"],
        "confirmation_guard_hash": confirmation["compact_hash"],
        "confirmation_passed": confirmation["passed"] is True,
        "projected_status": projected_status,
        "paper_allocation_eligible": bool(paper_eligible),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    })


STRATEGY_GATE_HASHES = {
    key: _final_funding_hash(
        entity_type="STRATEGY", entity_key=key,
        pre_gate_hash=STRATEGY_PRE_GATE_HASHES[key],
        decision=STRATEGY_STATISTICAL_DECISIONS[key],
        confirmation=STRATEGY_CONFIRMATIONS[key],
        projected_status="ACTIVE" if key == "strategy_a" else "SHADOW",
        paper_eligible=key == "strategy_a",
    )
    for key in STRATEGY_ROUTES
}


def _valid_funding_window_metrics(
    *, window: int, evidence_hash: str, route_hash: str,
) -> dict:
    from server.engine import strategy_governance as governance_module

    session_window = _attested_session_window(window)
    records = [
        {
            "trade_date": day,
            "return_pct": 1.5 if index % 10 < 7 else -0.5,
            "actual_cost_pct": 0.1,
            "is_net_return": True,
            "evidence_revision_at": f"{day}T15:00:00",
        }
        for index, day in enumerate(session_window["sessions"])
    ]
    metrics = governance_module.calculate_return_metrics(
        records,
        window_days=window,
        market_match_score=100.0,
        version_bound_evidence=True,
        independent_oos=True,
    )
    metrics.update({
        "internal_daily_records": records,
        "completed_trades": max(window, 80) if window != 20 else 20,
        "funding_provenance": (
            "INTERNAL_PORTFOLIO_CHECKPOINT_FACT_LEDGER_V3"
        ),
        "internal_ledger_hash": health._canonical_digest(
            {"window": window, "kind": "ledger"}
        ),
        "internal_ledger_schema": (
            "probiga.internal-strategy-portfolio-ledger.v3"
        ),
        "drawdown_basis": "internal_version_bound_portfolio_equity",
        "cost_basis": "actual_ledger_fees",
        "portfolio_coverage_days": window,
        "evidence_fresh": True,
        "evidence_revision_at": f"{TRADE_DATE}T15:00:00",
        "evidence_hash": evidence_hash,
        "market_route_hash": route_hash,
        "session_window_valid": True,
        "session_window_start": session_window["start_date"],
        "session_window_end": session_window["end_date"],
        "session_window_count": session_window["session_count"],
        "session_window_hash": session_window["session_hash"],
        "selection_validation": {
            "status": "UNAVAILABLE",
            "status_label": "fixture",
            "window_days": window,
            "evidence_ready": False,
            "funding_authority": False,
            "real_order_authority": False,
        },
    })
    if window == 60:
        metrics["internal_daily_stock_market_values"] = [
            {
                "trade_date": row["trade_date"],
                "stock_risk_exposure": {"000001": "10000"},
            }
            for row in records
        ]
    governance_module._apply_statistical_health(
        metrics, session_window=session_window,
    )
    metrics["profit_gate"] = governance_module.evaluate_window_gate(metrics)
    return metrics


STRATEGY_RANKING_SCORES = {
    key: round(
        sum(
            _valid_funding_window_metrics(
                window=window,
                evidence_hash=STRATEGY_WINDOW_EVIDENCE[key][str(window)],
                route_hash=STRATEGY_ROUTES[key]["router_decision_hash"],
            )["health_score"] * weight
            for window, weight in ((20, 0.25), (60, 0.50), (120, 0.25))
        ),
        2,
    )
    for key in STRATEGY_ROUTES
}


def _valid_strategy_router_rows() -> list[dict]:
    rows = []
    for key, route in STRATEGY_ROUTES.items():
        for window in health.EXPECTED_WINDOWS:
            metrics = _valid_funding_window_metrics(
                window=window,
                evidence_hash=STRATEGY_WINDOW_EVIDENCE[key][str(window)],
                route_hash=route["router_decision_hash"],
            )
            from server.engine import strategy_governance as governance_module

            stored_metrics = governance_module._canonical_metric_window_summary(
                metrics,
                entity_type="STRATEGY",
                entity_key=key,
                entity_version="v1",
                window_days=window,
            )
            payload = {
                "strategy_key": key,
                "strategy_version": "v1",
                "trade_date": TRADE_DATE,
                "window_days": window,
                "metrics": stored_metrics,
                "gate": metrics["profit_gate"],
                "overall_profit_gate_passed": True,
                "decision_contract_version": (
                    governance_module.STATISTICAL_DECISION_CONTRACT
                ),
                "statistical_policy_hash": (
                    governance_module.STATISTICAL_POLICY_HASH
                ),
                "statistical_family_decision": (
                    STRATEGY_STATISTICAL_DECISIONS[key]
                ),
                "statistical_family_passed": True,
                "pre_confirmation_funding_gate_hash": (
                    STRATEGY_PRE_GATE_HASHES[key]
                ),
                "confirmation_guard": STRATEGY_CONFIRMATIONS[key],
                "market_route": route,
                "paper_allocation_eligible": key == "strategy_a",
                "funding_gate_hash": STRATEGY_GATE_HASHES[key],
                "funding_evidence_revision_at": f"{TRADE_DATE}T15:00:00",
            }
            rows.append(
                {
                    "strategy_key": key,
                    "strategy_version": "v1",
                    "trade_date": TRADE_DATE,
                    "window_days": window,
                    "recommended_status": (
                        "ACTIVE" if key == "strategy_a" else "SHADOW"
                    ),
                    "market_match_score": Decimal("100.0000"),
                    "health_score": Decimal(str(metrics["health_score"])),
                    "profit_gate_passed": 1,
                    "evidence_json": payload,
                    "result_hash": health._canonical_digest(payload),
                    "version_hash": route["source_binding"]["version_hash"],
                    "evaluator_type": "external_evidence",
                    "evaluator_config_json": json.dumps(
                        _strategy_evaluator_config()
                    ),
                    "parameters_json": "{}",
                    "source_kind": "runtime_registry",
                    "registry_name": key,
                    "registry_current_version": "v1",
                    "registry_current_status": (
                        "ACTIVE" if key == "strategy_a" else "SHADOW"
                    ),
                    "registry_enabled": 1,
                }
            )
    return rows


def _combination_route() -> dict:
    payload = {
        "schema": "probiga.combination-market-route.v1",
        "policy_version": health.ROUTER_POLICY_VERSION,
        "combination_key": "combo_a",
        "combination_version": "v1",
        "trade_date": TRADE_DATE,
        "market_state": MARKET_STATE,
        "member_route_hashes": {
            key: route["router_decision_hash"]
            for key, route in sorted(STRATEGY_ROUTES.items())
        },
        "market_match_score": 100.0,
        "eligible": True,
        "reason": "全部成员适配当前市场状态",
    }
    return {
        **payload,
        "router_decision_hash": health._canonical_digest(payload),
    }


COMBINATION_ROUTE = _combination_route()
COMBINATION_WINDOW_EVIDENCE = {
    str(window): health._canonical_digest(
        {"combination_key": "combo_a", "window_days": window}
    )
    for window in health.EXPECTED_WINDOWS
}
COMBINATION_MEMBER_DETAILS = [
    {
        "strategy_key": key,
        "strategy_name": key,
        "strategy_version": "v1",
        "current_strategy_version": "v1",
        "version_match": True,
        "weight": 0.5,
        "status_label": (
            "正常运行" if key == "strategy_a" else "影子观察"
        ),
        "lifecycle_status": (
            "ACTIVE" if key == "strategy_a" else "SHADOW"
        ),
        "lifecycle_risk_multiplier": (
            1.0 if key == "strategy_a" else 0.0
        ),
        "effective_weight_after_lifecycle": (
            0.5 if key == "strategy_a" else 0.0
        ),
        "contribution_score": round(
            STRATEGY_RANKING_SCORES[key] * 0.5, 2,
        ),
    }
    for key in sorted(STRATEGY_ROUTES)
]
_COMBINATION_CONSTRAINT_PAYLOAD = {
    "schema": "probiga.combination-constraint-evaluation.v1",
    "combination_key": "combo_a",
    "combination_version": "v1",
    "constraints": {},
    "checks": [{"name": "fixture", "passed": True}],
    "pairwise_correlations": [],
    "pairwise_stock_overlaps": [],
    "industry_weights_pct": {},
}
COMBINATION_CONSTRAINT_EVALUATION = {
    **_COMBINATION_CONSTRAINT_PAYLOAD,
    "passed": True,
    "evaluation_hash": health._canonical_digest(
        _COMBINATION_CONSTRAINT_PAYLOAD
    ),
}
COMBINATION_PRE_GATE_HASH = health._canonical_digest(
    {
        "combination_key": "combo_a",
        "combination_version": "v1",
        "window_evidence": COMBINATION_WINDOW_EVIDENCE,
        "member_versions": {
            item["strategy_key"]: {
                "frozen": item["strategy_version"],
                "current": item["current_strategy_version"],
                "lifecycle_status": item["lifecycle_status"],
                "lifecycle_risk_multiplier": item[
                    "lifecycle_risk_multiplier"
                ],
            }
            for item in COMBINATION_MEMBER_DETAILS
        },
        "member_sleeve_risk_multiplier": 0.5,
        "router_decision_hash": COMBINATION_ROUTE["router_decision_hash"],
        "constraint_evaluation_hash": COMBINATION_CONSTRAINT_EVALUATION[
            "evaluation_hash"
        ],
        "profit_gate_passed": False,
    }
)
COMBINATION_STATISTICAL_DECISION = _fixture_statistical_decision(
    entity_type="COMBINATION", entity_key="combo_a",
)
COMBINATION_CONFIRMATION = _fixture_confirmation(passed=False)
COMBINATION_GATE_HASH = _final_funding_hash(
    entity_type="COMBINATION",
    entity_key="combo_a",
    pre_gate_hash=COMBINATION_PRE_GATE_HASH,
    decision=COMBINATION_STATISTICAL_DECISION,
    confirmation=COMBINATION_CONFIRMATION,
    projected_status="SHADOW",
    paper_eligible=False,
)
COMBINATION_RANKING_SCORE = round(
    sum(
        _valid_funding_window_metrics(
            window=window,
            evidence_hash=COMBINATION_WINDOW_EVIDENCE[str(window)],
            route_hash=COMBINATION_ROUTE["router_decision_hash"],
        )["health_score"] * weight
        for window, weight in ((20, 0.25), (60, 0.50), (120, 0.25))
    ),
    2,
)


def _valid_combination_router_rows() -> list[dict]:
    from server.engine import strategy_governance as governance_module

    raw_metrics = {
        str(window): _valid_funding_window_metrics(
                window=window,
                evidence_hash=COMBINATION_WINDOW_EVIDENCE[str(window)],
                route_hash=COMBINATION_ROUTE["router_decision_hash"],
            )
        for window in health.EXPECTED_WINDOWS
    }
    metrics = {
        str(window): governance_module._canonical_metric_window_summary(
            raw_metrics[str(window)],
            entity_type="COMBINATION",
            entity_key="combo_a",
            entity_version="v1",
            window_days=window,
        )
        for window in health.EXPECTED_WINDOWS
    }
    payload = {
        "combination_key": "combo_a",
        "combination_version": "v1",
        "trade_date": TRADE_DATE,
        "metrics": metrics,
        "multi_window_gate": {
            str(window): raw_metrics[str(window)]["profit_gate"]
            for window in health.EXPECTED_WINDOWS
        },
        "decision_contract_version": (
            governance_module.STATISTICAL_DECISION_CONTRACT
        ),
        "statistical_policy_hash": governance_module.STATISTICAL_POLICY_HASH,
        "statistical_family_decision": COMBINATION_STATISTICAL_DECISION,
        "statistical_family_passed": True,
        "pre_confirmation_funding_gate_hash": COMBINATION_PRE_GATE_HASH,
        "confirmation_guard": COMBINATION_CONFIRMATION,
        "funding_gate_hash": COMBINATION_GATE_HASH,
        "funding_evidence_revision_at": f"{TRADE_DATE}T15:00:00",
        "overall_profit_gate_passed": False,
        "market_route": COMBINATION_ROUTE,
        "paper_allocation_eligible": False,
        "member_details": COMBINATION_MEMBER_DETAILS,
        "constraint_evaluation": COMBINATION_CONSTRAINT_EVALUATION,
    }
    members = [
        {
            "strategy_key": key,
            "strategy_version": "v1",
            "weight": 0.5,
        }
        for key in sorted(STRATEGY_ROUTES)
    ]
    constraints = {}
    return [
        {
            "combination_key": "combo_a",
            "combination_version": "v1",
            "trade_date": TRADE_DATE,
            "recommended_status": "SHADOW",
            "ranking_score": Decimal(str(COMBINATION_RANKING_SCORE)),
            "profit_gate_passed": 0,
            "evidence_json": payload,
            "result_hash": health._canonical_digest(payload),
            "members_json": json.dumps(members),
            "constraints_json": json.dumps(constraints),
            "config_hash": health._canonical_digest(
                {"members": members, "constraints": constraints}
            ),
            "registry_name": "combo_a",
            "registry_current_version": "v1",
            "registry_current_status": "SHADOW",
            "registry_enabled": 1,
        }
    ]


ROUTER_SNAPSHOT_HASH = health._canonical_digest(
    {
        "schema": "probiga.strategy-market-router-snapshot.v1",
        "policy_version": health.ROUTER_POLICY_VERSION,
        "trade_date": TRADE_DATE,
        "market_state": MARKET_STATE,
        "market_state_config_hash": MARKET_CONFIG_HASH,
        "strategy_routes": {
            key: route["router_decision_hash"]
            for key, route in sorted(STRATEGY_ROUTES.items())
        },
        "combination_routes": {
            "combo_a": COMBINATION_ROUTE["router_decision_hash"]
        },
    }
)


def _fixture_industry_contract() -> dict:
    from server.engine.strategy_industry_history import build_history_rows

    source = "QMT_TEST"
    captured_at = f"{TRADE_DATE}T15:05:00"
    members = [
        {
            "snapshot_date": TRADE_DATE,
            "source": source,
            "industry_code": "801780",
            "industry_name": "银行",
            "industry_type": "L1",
            "stock_code": "000001",
            "short_name": "平安银行",
            "quality_status": health.QMT_VALIDATED,
            "captured_at": captured_at,
        },
        {
            "snapshot_date": TRADE_DATE,
            "source": source,
            "industry_code": "801780",
            "industry_name": "银行",
            "industry_type": "L1",
            "stock_code": "600000",
            "short_name": "浦发银行",
            "quality_status": health.QMT_VALIDATED,
            "captured_at": captured_at,
        },
        {
            "snapshot_date": TRADE_DATE,
            "source": source,
            "industry_code": "851911",
            "industry_name": "股份制银行",
            "industry_type": "L2",
            "stock_code": "000001",
            "short_name": "平安银行",
            "quality_status": health.QMT_VALIDATED,
            "captured_at": captured_at,
        },
    ]
    source_hash = health._canonical_qmt_industry_hash(members)
    snapshot_id, history_rows = build_history_rows(
        members,
        trade_date=TRADE_DATE,
        source=source,
        industry_hash=source_hash,
        captured_at=captured_at,
    )
    selected = [
        deepcopy(row) for row in history_rows
        if row["stock_code"] == "000001"
    ]
    wrapper_payload = {
        "schema": health.INDUSTRY_SNAPSHOT_SCHEMA,
        "snapshot_id": snapshot_id,
        "trade_date": TRADE_DATE,
        "as_of_exclusive": "2026-08-22T00:00:00",
        "status": "COMPLETED",
        "requested_stock_codes": ["000001"],
        "rows": selected,
        "reason": "append-only行业历史已按治理交易日冻结",
    }
    wrapper = {
        **wrapper_payload,
        "snapshot_hash": health._canonical_digest(wrapper_payload),
    }
    selected_row = selected[0]
    binding = {
        "schema": health.INDUSTRY_BINDING_SCHEMA,
        "snapshot_id": snapshot_id,
        "snapshot_hash": wrapper["snapshot_hash"],
        "row_hash": selected_row["row_hash"],
        "trade_date": TRADE_DATE,
        "as_of_exclusive": selected_row["as_of_exclusive"],
        "stock_code": "000001",
        "industry_name": selected_row["industry_name"],
        "industry_type": selected_row["industry_type"],
        "source_system": selected_row["source_system"],
        "source_fact_id": selected_row["source_fact_id"],
        "source_effective_at": selected_row["source_effective_at"],
        "source_etl_sync_at": selected_row["source_etl_sync_at"],
    }
    return {
        "run": {
            "snapshot_date": TRADE_DATE,
            "source": source,
            "quality_status": health.QMT_VALIDATED,
            "capture_mode": "qmt_close_full_refresh",
            "industry_count": 2,
            "industry_relation_count": len(members),
            "industry_hash": source_hash,
            "captured_at": captured_at,
        },
        "members": members,
        "history_rows": history_rows,
        "wrapper": wrapper,
        "binding": binding,
    }


def _fixture_pool_rows() -> list[dict]:
    industry = _fixture_industry_contract()
    binding = industry["binding"]
    rows = []
    for level, gate_status in (
        ("OBSERVATION", "观察"),
        ("CONFIRMATION", "研究确认"),
        ("TRADABLE", "模拟资金候选"),
    ):
        reason = {"reason": "fixture", "blocking_reasons": []}
        source_evidence = {
            "data_date": TRADE_DATE,
            "risk_level": "LOW",
            "source_status": "fresh",
            "industry_snapshot_status": "COMPLETED",
            "industry_snapshot_reason": (
                "append-only行业历史已按治理交易日冻结"
            ),
            "industry_binding": binding,
        }
        payload = {
            "schema": health.POOL_ROW_SCHEMA,
            "trade_date": TRADE_DATE,
            "pool_level": level,
            "stock_code": "000001",
            "stock_name": "平安银行",
            "rank_no": 1,
            "opportunity_score": "90.0000",
            "execution_score": "80.0000",
            "dominant_strategy": "strategy_a",
            "strategies": ["strategy_a"],
            "industry_name": "银行",
            "industry_type": "L1",
            "industry_snapshot_id": binding["snapshot_id"],
            "industry_snapshot_hash": binding["snapshot_hash"],
            "industry_row_hash": binding["row_hash"],
            "industry_source_system": binding["source_system"],
            "industry_source_fact_id": binding["source_fact_id"],
            "industry_binding": binding,
            "industry_names": ["银行"],
            "industry_by_strategy": {"strategy_a": "银行"},
            "gate_status": gate_status,
            "reason": reason,
            "evidence": source_evidence,
        }
        rows.append(
            {
                "trade_date": TRADE_DATE,
                "pool_level": level,
                "stock_code": "000001",
                "stock_name": "平安银行",
                "rank_no": 1,
                "opportunity_score": Decimal("90.0000"),
                "execution_score": Decimal("80.0000"),
                "dominant_strategy": "strategy_a",
                "strategies_json": ["strategy_a"],
                "industry_name": "银行",
                "gate_status": gate_status,
                "reason_json": reason,
                "evidence_json": {
                    "schema": health.POOL_ROW_EVIDENCE_SCHEMA,
                    "source_evidence": source_evidence,
                    "industry_names": ["银行"],
                    "industry_by_strategy": {"strategy_a": "银行"},
                    "industry_binding": binding,
                    "pool_row_hash": health._canonical_digest(payload),
                },
            }
        )
    return rows


def _fixture_allocation_rows() -> list[dict]:
    return [
        {
            "target_type": "CASH",
            "target_key": "cash",
            "target_version": "",
            "funding_gate_hash": "",
            "market_state": MARKET_STATE,
            "market_match_score": Decimal("0.0000"),
            "router_decision_hash": "",
            "lifecycle_status": "",
            "lifecycle_status_label": "",
            "lifecycle_risk_multiplier": Decimal("0.0000"),
            "base_competitive_weight_pct": Decimal("0.0000"),
            "simulated_weight_pct": Decimal("15.0000"),
            "member_sleeves_json": [],
            "member_sleeve_hash": "",
            "cash_discount_bp": 0,
            "real_order_authority": 0,
        },
        {
            "target_type": "STRATEGY",
            "target_key": "strategy_a",
            "target_version": "v1",
            "funding_gate_hash": STRATEGY_GATE_HASHES["strategy_a"],
            "market_state": MARKET_STATE,
            "market_match_score": Decimal("100.0000"),
            "router_decision_hash": STRATEGY_ROUTES["strategy_a"][
                "router_decision_hash"
            ],
            "lifecycle_status": "ACTIVE",
            "lifecycle_status_label": "正常运行",
            "lifecycle_risk_multiplier": Decimal("1.0000"),
            "base_competitive_weight_pct": Decimal("85.0000"),
            "simulated_weight_pct": Decimal("85.0000"),
            "member_sleeves_json": [],
            "member_sleeve_hash": "",
            "cash_discount_bp": 0,
            "real_order_authority": 0,
        },
    ]


def _fixture_pool_snapshot_hash() -> str:
    contracts = [
        {
            "pool_level": row["pool_level"],
            "rank_no": row["rank_no"],
            "stock_code": row["stock_code"],
            "pool_row_hash": row["evidence_json"]["pool_row_hash"],
        }
        for row in _fixture_pool_rows()
    ]
    contracts.sort(
        key=lambda row: (
            row["pool_level"], row["rank_no"], row["stock_code"]
        )
    )
    return health._canonical_digest(
        {
            "schema": health.POOL_SNAPSHOT_SCHEMA,
            "trade_date": TRADE_DATE,
            "row_count": len(contracts),
            "rows": contracts,
        }
    )


def _fixture_paper_plan(allocations: list[dict]) -> dict:
    industry = _fixture_industry_contract()
    binding = industry["binding"]
    allocation = next(
        row for row in allocations if row["target_type"] == "STRATEGY"
    )
    budget_bp = int(
        Decimal(str(allocation["simulated_weight_pct"])) * 100
    )
    source_hash = health._canonical_digest({"fixture": "candidate"})
    target_payload = {
        "stock_code": "000001",
        "stock_name": "平安银行",
        "industry_name": "银行",
        "industry_type": "L1",
        "industry_snapshot_id": binding["snapshot_id"],
        "industry_snapshot_hash": binding["snapshot_hash"],
        "industry_row_hash": binding["row_hash"],
        "industry_source_system": binding["source_system"],
        "industry_source_fact_id": binding["source_fact_id"],
        "industry_binding": binding,
        "strategy_key": "strategy_a",
        "strategy_version": "v1",
        "allocation_target_type": "STRATEGY",
        "allocation_target_key": "strategy_a",
        "allocation_target_version": "v1",
        "target_bp": 500,
        "target_weight_pct": 5.0,
        "previous_target_bp": 0,
        "new_buy_delta_bp": 500,
        "reference_capital_cny": 1_000_000.0,
        "reference_price": 10.0,
        "reference_board_lot_quantity": 5000,
        "quantity_semantics": "REFERENCE_ONLY_INTERNAL_PAPER_OMS_RECALCULATES",
        "opportunity_score": 90.0,
        "execution_score": 80.0,
        "planned_risk_reward_ratio": 2.0,
        "stop_loss_price": 9.5,
        "take_profit_1": 11.0,
        "take_profit_2": 12.0,
        "candidate_source_hash": source_hash,
        "allocation_backed": True,
        "new_buy_allowed": True,
        "exit_always_allowed": True,
        "real_order_authority": False,
    }
    target = {
        **target_payload,
        "target_hash": health._canonical_digest({
            "schema": "probiga.governance-paper-target.v1",
            **target_payload,
        }),
    }
    payload = {
        "schema": "probiga.governance-paper-execution-plan.v1",
        "trade_date": TRADE_DATE,
        "industry_snapshot_id": binding["snapshot_id"],
        "industry_snapshot_hash": binding["snapshot_hash"],
        "industry_snapshot_status": "COMPLETED",
        "policy": health.GLOBAL_PORTFOLIO_POLICY,
        "funded_sleeves": [{
            "strategy_key": "strategy_a",
            "strategy_version": "v1",
            "allocation_target_type": "STRATEGY",
            "allocation_target_key": "strategy_a",
            "allocation_target_version": "v1",
            "budget_bp": budget_bp,
        }],
        "portfolio_risk": {
            "valid": True,
            "observations": 60,
            "annualized_volatility_pct": 10.0,
            "expected_shortfall_95_pct": 1.0,
            "risk_multiplier": 1.0,
            "reason": "fixture",
        },
        "requested_new_buy_turnover_bp": 500,
        "new_buy_turnover_multiplier": 1.0,
        "actual_new_buy_turnover_bp": 500,
        "targets": [target],
        "exit_targets": [],
        "target_count": 1,
        "invested_bp": 500,
        "cash_bp": 9500,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    return {**payload, "plan_hash": health._canonical_digest(payload)}


def _fixture_transition_plan_hash(
    transitions: list[dict] | None = None,
) -> str:
    rows = [deepcopy(row) for row in (transitions or [])]
    for row in rows:
        evidence = row.get("evidence") or {}
        row["evidence"] = {
            str(key): value
            for key, value in evidence.items()
            if str(key) != "run_uid"
        }
    rows.sort(
        key=lambda row: (
            row["entity_type"],
            row["entity_key"],
            row["entity_version"],
            row["previous_status"],
            row["next_status"],
            health._canonical_digest(row),
        )
    )
    return health._canonical_digest(
        {
            "schema": health.AUTOMATIC_TRANSITION_PLAN_SCHEMA,
            "trade_date": TRADE_DATE,
            "transition_count": len(rows),
            "transitions": rows,
        }
    )


def _fixture_v7_statistical_contract() -> dict:
    from server.engine import strategy_governance as governance_module

    strategy_inventory = {
        "trial_inventory_hash": "1" * 64,
        "trial_inventory_state_hash": "2" * 64,
    }
    combination_inventory = deepcopy(strategy_inventory)
    inventory_payload = {
        "schema": "probiga.strategy-statistical-inventory-compact.v1",
        "strategy": strategy_inventory,
        "combination": combination_inventory,
        "source_inventory_result_hash": "3" * 64,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    inventory = {
        **inventory_payload,
        "compact_hash": health._canonical_digest(inventory_payload),
    }

    def summary(kind: str, inventory_row: dict) -> dict:
        payload = {
            "schema": "probiga.strategy-family-by-summary.v1",
            "family_id": f"fixture-{kind.lower()}",
            "valid": True,
            "passed_candidate_count": 2 if kind == "STRATEGY" else 1,
            "current_candidate_count": 2 if kind == "STRATEGY" else 1,
            "total_hypotheses": 3,
            "trial_inventory_hash": inventory_row["trial_inventory_hash"],
            "trial_inventory_state_hash": inventory_row[
                "trial_inventory_state_hash"
            ],
            "q": 0.05,
            "source_by_result_hash": "3" * 64,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
        return {**payload, "summary_hash": health._canonical_digest(payload)}

    strategy_summary = summary("STRATEGY", strategy_inventory)
    combination_summary = summary("COMBINATION", combination_inventory)
    input_payload = {
        "schema": "probiga.strategy-statistical-input-binding.v1",
        "decision_contract_version": (
            governance_module.STATISTICAL_DECISION_CONTRACT
        ),
        "statistical_policy_hash": governance_module.STATISTICAL_POLICY_HASH,
        "inventory_compact_hash": inventory["compact_hash"],
        "inventory_result_hash": inventory["source_inventory_result_hash"],
        "strategy_fdr_summary_hash": strategy_summary["summary_hash"],
        "combination_fdr_summary_hash": combination_summary["summary_hash"],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    input_binding = {
        **input_payload,
        "binding_hash": health._canonical_digest(input_payload),
    }
    return {
        "decision_contract_version": (
            governance_module.STATISTICAL_DECISION_CONTRACT
        ),
        "statistical_policy": governance_module.STATISTICAL_GUARD_POLICY,
        "statistical_policy_hash": governance_module.STATISTICAL_POLICY_HASH,
        "statistical_inventory": inventory,
        "strategy_fdr_summary": strategy_summary,
        "combination_fdr_summary": combination_summary,
        "statistical_input_binding": input_binding,
        "statistical_inventory_compact_hash": inventory["compact_hash"],
        "strategy_fdr_summary_hash": strategy_summary["summary_hash"],
        "combination_fdr_summary_hash": combination_summary["summary_hash"],
        "statistical_input_binding_hash": input_binding["binding_hash"],
    }


def _fixture_canonical_v7_entities() -> tuple[list[dict], list[dict]]:
    from server.engine import strategy_governance as governance_module

    strategy_rows = _valid_strategy_router_rows()
    by_strategy: dict[str, dict[str, dict]] = {}
    for row in strategy_rows:
        payload = row["evidence_json"]
        by_strategy.setdefault(row["strategy_key"], {})[
            str(row["window_days"])
        ] = payload["metrics"]
    strategies = []
    for key in ("strategy_a", "strategy_b"):
        decision = STRATEGY_STATISTICAL_DECISIONS[key]
        confirmation = STRATEGY_CONFIRMATIONS[key]
        strategies.append({
            "strategy_key": key,
            "current_version": "v1",
            "current_status": (
                "ACTIVE" if key == "strategy_a" else "SHADOW"
            ),
            "projected_status": (
                "ACTIVE" if key == "strategy_a" else "SHADOW"
            ),
            "enabled": True,
            "paper_allocation_eligible": key == "strategy_a",
            "pre_confirmation_funding_gate_hash": (
                STRATEGY_PRE_GATE_HASHES[key]
            ),
            "funding_gate_hash": STRATEGY_GATE_HASHES[key],
            "statistical_family_decision": {
                "valid": decision["valid"],
                "passed": decision["passed"],
                "p_value": decision["candidate_p_value"],
                "rank": decision["rank"],
                "critical": decision["critical_value"],
                "trials": decision["total_hypotheses"],
                "source_hash": decision["decision_hash"],
            },
            "confirmation_guard": {
                "valid": confirmation["valid"],
                "passed": confirmation["passed"],
                "gap_sessions": confirmation["minimum_new_sessions"],
                "confirmations": confirmation["total_confirmation_count"],
                "continuous_sessions": confirmation[
                    "continuous_session_count"
                ],
                "source_hash": confirmation["compact_hash"],
            },
            "metrics": by_strategy[key],
        })
    combination_payload = _valid_combination_router_rows()[0][
        "evidence_json"
    ]
    decision = COMBINATION_STATISTICAL_DECISION
    confirmation = COMBINATION_CONFIRMATION
    combinations = [{
        "combination_key": "combo_a",
        "current_version": "v1",
        "current_status": "SHADOW",
        "projected_status": "SHADOW",
        "enabled": True,
        "paper_allocation_eligible": False,
        "pre_confirmation_funding_gate_hash": COMBINATION_PRE_GATE_HASH,
        "funding_gate_hash": COMBINATION_GATE_HASH,
        "statistical_family_decision": {
            "valid": decision["valid"],
            "passed": decision["passed"],
            "p_value": decision["candidate_p_value"],
            "rank": decision["rank"],
            "critical": decision["critical_value"],
            "trials": decision["total_hypotheses"],
            "source_hash": decision["decision_hash"],
        },
        "confirmation_guard": {
            "valid": confirmation["valid"],
            "passed": confirmation["passed"],
            "gap_sessions": confirmation["minimum_new_sessions"],
            "confirmations": confirmation["total_confirmation_count"],
            "continuous_sessions": confirmation["continuous_session_count"],
            "source_hash": confirmation["compact_hash"],
        },
        "metrics": combination_payload["metrics"],
    }]
    return strategies, combinations


def _fixture_allocation_contract(
    *, router_snapshot_hash: str = ROUTER_SNAPSHOT_HASH,
    combination_route: dict | None = None,
    combination_gate_hash: str = COMBINATION_GATE_HASH,
    combination_pre_gate_hash: str = COMBINATION_PRE_GATE_HASH,
    combination_statistical_decision: dict | None = None,
    combination_confirmation: dict | None = None,
    combination_canonical_metrics: dict | None = None,
    combination_profit_gate_passed: bool = False,
    combination_member_details: list[dict] | None = None,
    combination_risk_metrics: dict | None = None,
    strategy_a_status: str = "ACTIVE",
    automatic_transition_plan_hash: str | None = None,
) -> dict:
    combination_route = combination_route or COMBINATION_ROUTE
    combination_statistical_decision = (
        combination_statistical_decision
        or COMBINATION_STATISTICAL_DECISION
    )
    combination_confirmation = (
        combination_confirmation or COMBINATION_CONFIRMATION
    )
    combination_member_details = (
        combination_member_details or COMBINATION_MEMBER_DETAILS
    )
    if combination_risk_metrics is None:
        combination_risk_metrics = _portfolio_risk_metrics(0, "000001")
    bindings = {}
    for key, route in STRATEGY_ROUTES.items():
        active = key == "strategy_a"
        projected_status = strategy_a_status if active else "SHADOW"
        strategy_gate_hash = _final_funding_hash(
            entity_type="STRATEGY",
            entity_key=key,
            pre_gate_hash=STRATEGY_PRE_GATE_HASHES[key],
            decision=STRATEGY_STATISTICAL_DECISIONS[key],
            confirmation=STRATEGY_CONFIRMATIONS[key],
            projected_status=projected_status,
            paper_eligible=active,
        )
        bindings[("STRATEGY", key, "v1")] = {
            "router_decision_hash": route["router_decision_hash"],
            "market_match_score": Decimal("100.0000"),
            "market_state": MARKET_STATE,
            "eligible": True,
            "paper_allocation_eligible": active,
            "funding_gate_hash": strategy_gate_hash,
            "members": frozenset({key}),
            "ranking_score": Decimal(str(STRATEGY_RANKING_SCORES[key])),
            "target_name": key,
            "enabled": True,
            "lifecycle_status": strategy_a_status if active else "SHADOW",
            "profit_gate_passed": True,
            "constraint_passed": True,
            "statistical_confirmation_passed": active,
            "portfolio_risk_metrics": _portfolio_risk_metrics(0, "000001"),
        }
    bindings[("COMBINATION", "combo_a", "v1")] = {
        "router_decision_hash": combination_route["router_decision_hash"],
        "market_match_score": Decimal("100.0000"),
        "market_state": MARKET_STATE,
        "eligible": combination_route.get("eligible") is True,
        "paper_allocation_eligible": False,
        "funding_gate_hash": combination_gate_hash,
        "members": frozenset(STRATEGY_ROUTES),
        "ranking_score": Decimal(str(COMBINATION_RANKING_SCORE)),
        "target_name": "combo_a",
        "enabled": True,
        "lifecycle_status": "SHADOW",
        "profit_gate_passed": combination_profit_gate_passed,
        "constraint_passed": True,
        "statistical_confirmation_passed": False,
        "member_sleeve_risk_multiplier": Decimal(str(round(
            sum(
                float(item["weight"])
                * float(item["lifecycle_risk_multiplier"])
                for item in combination_member_details
            ),
            8,
        ))),
        "member_sleeves_source": [
            {
                "strategy_key": item["strategy_key"],
                "strategy_version": item["strategy_version"],
                "current_strategy_version": item[
                    "current_strategy_version"
                ],
                "version_match": item["version_match"],
                "original_weight": item["weight"],
                "member_lifecycle_status": item["lifecycle_status"],
                "member_lifecycle_multiplier": item[
                    "lifecycle_risk_multiplier"
                ],
            }
            for item in combination_member_details
        ],
        "portfolio_risk_metrics": combination_risk_metrics,
    }
    candidates, errors = health._allocation_candidate_contract(bindings)
    assert not errors
    candidate_hash = health._canonical_digest(
        {
            "schema": "probiga.strategy-allocation-candidate-set.v1",
            "allocation_policy_version": health.ALLOCATION_POLICY_VERSION,
            "trade_date": TRADE_DATE,
            "market_state": MARKET_STATE,
            "candidates": candidates,
        }
    )
    allocations = health._expected_allocation_snapshot(
        candidates,
        market_state=MARKET_STATE,
        trading_gate_passed=True,
    )
    allocation_hash = health._canonical_digest(
        {
            "schema": "probiga.strategy-allocation-snapshot.v1",
            "allocation_policy_version": health.ALLOCATION_POLICY_VERSION,
            "trade_date": TRADE_DATE,
            "market_state": MARKET_STATE,
            "market_risk_cap_pct": 85.0,
            "trading_gate_passed": True,
            "candidate_set_hash": candidate_hash,
            "allocations": allocations,
        }
    )
    strategies = sorted(
        (row for row in candidates if row["target_type"] == "STRATEGY"),
        key=lambda row: (
            {"ACTIVE": 0, "REDUCE": 0, "SHADOW": 1,
             "SUSPENDED": 2, "RETIRED": 3}.get(
                row["lifecycle_status"], 9
            ),
            -float(row["ranking_score"]),
            row["target_key"],
        ),
    )
    combinations = sorted(
        (row for row in candidates if row["target_type"] == "COMBINATION"),
        key=lambda row: (-float(row["ranking_score"]), row["target_key"]),
    )
    transition_plan_hash = (
        automatic_transition_plan_hash or _fixture_transition_plan_hash()
    )
    industry = _fixture_industry_contract()
    industry_snapshot_hash = industry["wrapper"]["snapshot_hash"]
    paper_plan = _fixture_paper_plan(allocations)
    statistical = _fixture_v7_statistical_contract()
    canonical_strategies, canonical_combinations = (
        _fixture_canonical_v7_entities()
    )
    for row in canonical_strategies:
        if row["strategy_key"] != "strategy_a":
            continue
        row["current_status"] = strategy_a_status
        row["projected_status"] = strategy_a_status
        row["funding_gate_hash"] = next(
            item["funding_gate_hash"]
            for item in strategies
            if item["target_key"] == "strategy_a"
        )
    canonical_combinations[0]["funding_gate_hash"] = combination_gate_hash
    canonical_combinations[0]["pre_confirmation_funding_gate_hash"] = (
        combination_pre_gate_hash
    )
    canonical_combinations[0]["statistical_family_decision"] = {
        "valid": combination_statistical_decision["valid"],
        "passed": combination_statistical_decision["passed"],
        "p_value": combination_statistical_decision["candidate_p_value"],
        "rank": combination_statistical_decision["rank"],
        "critical": combination_statistical_decision["critical_value"],
        "trials": combination_statistical_decision["total_hypotheses"],
        "source_hash": combination_statistical_decision["decision_hash"],
    }
    canonical_combinations[0]["confirmation_guard"] = {
        "valid": combination_confirmation["valid"],
        "passed": combination_confirmation["passed"],
        "gap_sessions": combination_confirmation["minimum_new_sessions"],
        "confirmations": combination_confirmation[
            "total_confirmation_count"
        ],
        "continuous_sessions": combination_confirmation[
            "continuous_session_count"
        ],
        "source_hash": combination_confirmation["compact_hash"],
    }
    if combination_canonical_metrics is not None:
        canonical_combinations[0]["metrics"] = deepcopy(
            combination_canonical_metrics
        )
    funding_manifest = _funding_manifest_persistence_fixture()["result"][
        "funding_checkpoint_manifest"
    ]
    strategy_decision_fields = {
        row["strategy_key"]: {
            "pre_confirmation_funding_gate_hash": row[
                "pre_confirmation_funding_gate_hash"
            ],
            "statistical_family_decision_hash": row[
                "statistical_family_decision"
            ]["source_hash"],
            "confirmation_guard_hash": row["confirmation_guard"][
                "source_hash"
            ],
        }
        for row in canonical_strategies
    }
    combination_decision_fields = {
        "combo_a": {
            "pre_confirmation_funding_gate_hash": canonical_combinations[0][
                "pre_confirmation_funding_gate_hash"
            ],
            "statistical_family_decision_hash": canonical_combinations[0][
                "statistical_family_decision"
            ]["source_hash"],
            "confirmation_guard_hash": canonical_combinations[0][
                "confirmation_guard"
            ]["source_hash"],
        }
    }
    decision_hash = health._canonical_digest(
        {
            "schema": statistical["decision_contract_version"],
            "trade_date": TRADE_DATE,
            "build_commit_sha": BUILD_SHA,
            "input_hash": "c" * 64,
            "router_snapshot_hash": router_snapshot_hash,
            "allocation_policy_version": health.ALLOCATION_POLICY_VERSION,
            "trading_gate_passed": True,
            "market_risk_cap_pct": 85.0,
            "allocation_candidate_count": len(candidates),
            "eligible_candidate_count": 1,
            "candidate_set_hash": candidate_hash,
            "allocation_snapshot_hash": allocation_hash,
            "paper_execution_plan_hash": paper_plan["plan_hash"],
            "pool_snapshot_hash": _fixture_pool_snapshot_hash(),
            "candidate_industry_snapshot_hash": industry_snapshot_hash,
            "funding_checkpoint_manifest_hash": funding_manifest[
                "manifest_hash"
            ],
            "statistical_policy_hash": statistical[
                "statistical_policy_hash"
            ],
            "statistical_inventory_compact_hash": statistical[
                "statistical_inventory_compact_hash"
            ],
            "strategy_fdr_summary_hash": statistical[
                "strategy_fdr_summary_hash"
            ],
            "combination_fdr_summary_hash": statistical[
                "combination_fdr_summary_hash"
            ],
            "statistical_input_binding_hash": statistical[
                "statistical_input_binding_hash"
            ],
            "strategies": [
                {
                    "strategy_key": row["target_key"],
                    "strategy_version": row["target_version"],
                    "enabled": row["enabled"],
                    "projected_status": row["lifecycle_status"],
                    **strategy_decision_fields[row["target_key"]],
                    "funding_gate_hash": row["funding_gate_hash"],
                }
                for row in strategies
            ],
            "combinations": [
                {
                    "combination_key": row["target_key"],
                    "combination_version": row["target_version"],
                    "enabled": row["enabled"],
                    "projected_status": row["lifecycle_status"],
                    **combination_decision_fields[row["target_key"]],
                    "funding_gate_hash": row["funding_gate_hash"],
                }
                for row in combinations
            ],
        }
    )
    result_summary = {
        "paper_execution_plan_hash": paper_plan["plan_hash"],
        "candidate_industry_snapshot_id": industry["wrapper"]["snapshot_id"],
        "candidate_industry_snapshot_hash": industry_snapshot_hash,
        "candidate_industry_snapshot_status": "COMPLETED",
        "decision_contract_version": statistical[
            "decision_contract_version"
        ],
        "statistical_policy_hash": statistical["statistical_policy_hash"],
        "statistical_inventory_compact_hash": statistical[
            "statistical_inventory_compact_hash"
        ],
        "strategy_fdr_summary_hash": statistical[
            "strategy_fdr_summary_hash"
        ],
        "combination_fdr_summary_hash": statistical[
            "combination_fdr_summary_hash"
        ],
        "statistical_input_binding_hash": statistical[
            "statistical_input_binding_hash"
        ],
        "funding_checkpoint_manifest_hash": funding_manifest["manifest_hash"],
    }
    result_json = json.dumps(
        {
            "trade_date": TRADE_DATE,
            "decision_contract_version": statistical[
                "decision_contract_version"
            ],
            "statistical_funding_eligible": True,
            "legacy_statistical_display_only": False,
            "statistical_policy": statistical["statistical_policy"],
            "statistical_policy_hash": statistical[
                "statistical_policy_hash"
            ],
            "statistical_inventory": statistical["statistical_inventory"],
            "strategy_fdr_summary": statistical["strategy_fdr_summary"],
            "combination_fdr_summary": statistical[
                "combination_fdr_summary"
            ],
            "statistical_input_binding": statistical[
                "statistical_input_binding"
            ],
            "allocation_candidate_set": candidates,
            "strategies": canonical_strategies,
            "combinations": canonical_combinations,
            "funding_checkpoint_manifest": funding_manifest,
            "paper_execution_plan": paper_plan,
            "paper_execution_plan_hash": paper_plan["plan_hash"],
            "candidate_industry_snapshot": industry["wrapper"],
            "candidate_industry_snapshot_hash": industry_snapshot_hash,
            "summary": result_summary,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return {
        "allocation_policy_version": health.ALLOCATION_POLICY_VERSION,
        "trading_gate_passed": True,
        "market_risk_cap_pct": 85.0,
        "allocation_candidate_count": len(candidates),
        "eligible_candidate_count": 1,
        "candidate_set_hash": candidate_hash,
        "allocation_snapshot_hash": allocation_hash,
        "paper_execution_plan_hash": paper_plan["plan_hash"],
        "candidate_industry_snapshot_id": industry["wrapper"]["snapshot_id"],
        "candidate_industry_snapshot_hash": industry_snapshot_hash,
        "candidate_industry_snapshot_status": "COMPLETED",
        "paper_target_count": 1,
        "paper_invested_weight_pct": 5.0,
        "pool_row_count": 3,
        "pool_snapshot_hash": _fixture_pool_snapshot_hash(),
        "automatic_transition_count": 0,
        "automatic_transition_plan_hash": transition_plan_hash,
        "decision_contract_version": statistical[
            "decision_contract_version"
        ],
        "statistical_policy_hash": statistical["statistical_policy_hash"],
        "statistical_inventory_compact_hash": statistical[
            "statistical_inventory_compact_hash"
        ],
        "strategy_fdr_summary_hash": statistical[
            "strategy_fdr_summary_hash"
        ],
        "combination_fdr_summary_hash": statistical[
            "combination_fdr_summary_hash"
        ],
        "statistical_input_binding_hash": statistical[
            "statistical_input_binding_hash"
        ],
        "funding_checkpoint_manifest_hash": funding_manifest["manifest_hash"],
        "cash_weight_pct": next(
            row["simulated_weight_pct"]
            for row in allocations
            if row["target_type"] == "CASH"
        ),
        "decision_hash": decision_hash,
        "_result_json": result_json,
        "_result_hash": hashlib.sha256(result_json.encode("utf-8")).hexdigest(),
    }


class _ScalarResult:
    def __init__(self, values):
        self._values = list(values)

    def all(self):
        return list(self._values)


class _Result:
    def __init__(self, rows=(), *, scalar_values=()):
        self._rows = [dict(row) for row in rows]
        self._scalar_values = list(scalar_values)

    def mappings(self):
        return self

    def scalars(self):
        return _ScalarResult(self._scalar_values)

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar_one(self):
        if self._scalar_values:
            return self._scalar_values[0]
        if len(self._rows) == 1 and len(self._rows[0]) == 1:
            return next(iter(self._rows[0].values()))
        raise RuntimeError("fixture scalar cardinality differs")


def _lifecycle_projection_fixture():
    registry_rows = [
        {
            "entity_type": "STRATEGY",
            "entity_key": "strategy_a",
            "entity_version": "v1",
            "current_status": "ACTIVE",
            "status_reason": "fixture active",
            "enabled": 1,
        },
        {
            "entity_type": "STRATEGY",
            "entity_key": "strategy_b",
            "entity_version": "v1",
            "current_status": "SHADOW",
            "status_reason": "fixture strategy genesis",
            "enabled": 1,
        },
        {
            "entity_type": "COMBINATION",
            "entity_key": "combo_a",
            "entity_version": "v1",
            "current_status": "SHADOW",
            "status_reason": "fixture combination genesis",
            "enabled": 1,
        },
    ]
    event_rows = []
    for index, (entity_type, entity_key, reason) in enumerate((
        ("STRATEGY", "strategy_a", "fixture strategy genesis"),
        ("STRATEGY", "strategy_b", "fixture strategy genesis"),
        ("COMBINATION", "combo_a", "fixture combination genesis"),
    ), 1):
        payload = {
            "entity_type": entity_type,
            "entity_key": entity_key,
            "old_version": "",
            "new_version": "v1",
            "previous_status": "SHADOW",
            "next_status": "SHADOW",
            "reason": reason,
        }
        event_rows.append({
            "event_id": format(index, "032x"),
            "entity_type": entity_type,
            "entity_key": entity_key,
            "entity_version": "v1",
            "previous_status": "SHADOW",
            "next_status": "SHADOW",
            "reason": reason,
            "trigger_type": "VERSION_REGISTRATION",
            "payload_json": payload,
            "event_hash": health._canonical_digest(payload),
            "occurred_at": f"{TRADE_DATE} 09:00:0{index}",
        })
    transition_payload = {
        "entity_type": "STRATEGY",
        "entity_key": "strategy_a",
        "entity_version": "v1",
        "previous_status": "SHADOW",
        "next_status": "ACTIVE",
        "reason": "fixture active",
        "evidence": {},
        "nonce": "f" * 32,
    }
    event_rows.append({
        "event_id": "f" * 32,
        "entity_type": "STRATEGY",
        "entity_key": "strategy_a",
        "entity_version": "v1",
        "previous_status": "SHADOW",
        "next_status": "ACTIVE",
        "reason": "fixture active",
        "trigger_type": "AUTOMATIC_GATE",
        "payload_json": transition_payload,
        "event_hash": health._canonical_digest(transition_payload),
        "occurred_at": f"{TRADE_DATE} 09:01:00",
    })
    return registry_rows, event_rows


def _funding_manifest_persistence_fixture():
    run_uid = "b" * 32
    strategy_key = "strategy_a"
    strategy_version = "v1"
    account_id = "paper-main-v2"
    version_hash = "1" * 64
    checkpoint_id = health.checkpoint_identity(
        strategy_key=strategy_key,
        strategy_version=strategy_version,
        account_id=account_id,
        trade_date=TRADE_DATE,
        anchor_run_uid=run_uid,
    )
    fact_id = health.funding_daily_fact_identity(
        entity_type="STRATEGY",
        entity_key=strategy_key,
        entity_version=strategy_version,
        account_id=account_id,
        trade_date=TRADE_DATE,
        anchor_run_uid=run_uid,
    )
    fact = {
        "schema": health.FUNDING_DAILY_FACT_SCHEMA,
        "entity_type": "STRATEGY",
        "entity_key": strategy_key,
        "entity_version": strategy_version,
        "entity_version_hash": version_hash,
        "execution_binding_hash": "",
        "account_id": account_id,
        "trade_date": TRADE_DATE,
        "origin_checkpoint_id": checkpoint_id,
        "previous_fact_id": "",
        "previous_fact_hash": "",
        "opening_cash_cny": "1000000.000000",
        "closing_cash_cny": "1000000.000000",
        "opening_equity_cny": "1000000.000000",
        "closing_equity_cny": "1000000.000000",
        "daily_return_pct": "0.000000000000",
        "cumulative_fee_cny": "0.000000",
        "high_watermark_equity_cny": "1000000.000000",
        "normalized_opening_equity": "100.00000000",
        "normalized_closing_equity": "100.00000000",
        "actual_cost_pct": "0.000000000000",
        "stock_risk_exposure": {},
        "closed_evidence_ids": [],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    fact_json = health._funding_canonical_json(fact)
    fact_hash = health.funding_daily_fact_hash(fact)
    fact_members = [{"fact_id": fact_id, "fact_hash": fact_hash}]
    fact_set_hash = health.ordered_funding_fact_set_hash(fact_members)
    state = {
        "schema": health.FUNDING_CHECKPOINT_SCHEMA,
        "strategy_key": strategy_key,
        "strategy_version": strategy_version,
        "strategy_version_hash": version_hash,
        "execution_binding_hash": "",
        "account_id": account_id,
        "trade_date": TRADE_DATE,
        "replay_mode": "FULL_BOOTSTRAP",
        "replay_start_date": TRADE_DATE,
        "replay_session_count": 1,
        "max_holding_days": 10,
        "holdings": [],
        "history_fact_count": 1,
        "history_fact_set_hash": fact_set_hash,
        "history_tip_fact_id": fact_id,
        "history_tip_fact_hash": fact_hash,
        "new_fact_count": 1,
        "new_fact_set_hash": fact_set_hash,
        "new_fact_first_id": fact_id,
        "new_fact_tip_id": fact_id,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    state_json = health._funding_canonical_json(state)
    checkpoint_hash = health.checkpoint_state_hash(state)
    chain = health.checkpoint_chain_payload(
        checkpoint_hash=checkpoint_hash,
        previous_checkpoint_id="",
        previous_checkpoint_hash="",
        previous_chain_hash="",
    )
    chain_json = health._funding_canonical_json(chain)
    chain_hash = health._funding_canonical_hash(chain)
    checkpoint_storage_row = {
        "checkpoint_id": checkpoint_id,
        "holdings_json": "[]",
        "state_json": state_json,
        "checkpoint_hash": checkpoint_hash,
        "chain_payload_json": chain_json,
        "chain_hash": chain_hash,
        "previous_checkpoint_id": None,
        "previous_checkpoint_hash": None,
        "previous_chain_hash": None,
    }
    checkpoint_storage_bytes = len(health._funding_canonical_json(
        health._funding_checkpoint_storage_projection(
            checkpoint_storage_row, state, run_uid=run_uid,
        )
    ).encode("utf-8"))
    checkpoint_entry = {
        "entity_type": "STRATEGY",
        "entity_key": strategy_key,
        "entity_version": strategy_version,
        "checkpoint_id": checkpoint_id,
        "strategy_key": strategy_key,
        "strategy_version": strategy_version,
        "account_id": account_id,
        "trade_date": TRADE_DATE,
        "replay_mode": "FULL_BOOTSTRAP",
        "replay_session_count": 1,
        "max_holding_days": 10,
        "checkpoint_hash": checkpoint_hash,
        "chain_hash": chain_hash,
        "history_fact_count": 1,
        "history_fact_set_hash": fact_set_hash,
        "history_tip_fact_id": fact_id,
        "history_tip_fact_hash": fact_hash,
        "new_fact_count": 1,
        "new_fact_set_hash": fact_set_hash,
        "new_fact_first_id": fact_id,
        "new_fact_tip_id": fact_id,
        "bootstrap_full_history_scan": True,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    ineligible = [{
        "entity_type": "COMBINATION",
        "entity_key": "combo_a",
        "entity_version": "v1",
        "reason_code": "COMBINATION_RECIPE_NOT_MATERIALIZED",
        "reason": "fixture combination has no independently materialized NAV",
    }]
    current_entities = [
        {
            "entity_type": "COMBINATION",
            "entity_key": "combo_a",
            "entity_version": "v1",
        },
        {
            "entity_type": "STRATEGY",
            "entity_key": strategy_key,
            "entity_version": strategy_version,
        },
    ]
    checkpoint_entities = [current_entities[1]]
    ineligible_entities = [current_entities[0]]
    fact_storage_row = {
        "fact_id": fact_id,
        "fact_json": fact_json,
        "fact_hash": fact_hash,
    }
    fact_storage_bytes = len(health._funding_canonical_json(
        health._funding_fact_storage_projection(
            fact_storage_row, fact, run_uid=run_uid,
        )
    ).encode("utf-8"))
    manifest_payload = {
        "schema": "probiga.strategy-funding-checkpoint-manifest.v2",
        "run_uid": run_uid,
        "trade_date": TRADE_DATE,
        "coverage": {
            "current_entity_count": 2,
            "funding_ready_count": 1,
            "eligible_count": 1,
            "strategy_checkpoint_count": 1,
            "combination_recipe_count": 0,
            "checkpointed_count": 1,
            "ineligible_count": 1,
            "current_entity_set_hash": health._funding_entity_set_hash(
                current_entities
            ),
            "checkpointed_set_hash": health._funding_entity_set_hash(
                checkpoint_entities
            ),
            "combination_recipe_set_hash": health._funding_entity_set_hash([]),
            "funding_ready_set_hash": health._funding_entity_set_hash(
                checkpoint_entities
            ),
            "ineligible_set_hash": health._funding_entity_set_hash(
                ineligible_entities
            ),
            "eligible_persistence_coverage_pct": 100.0,
        },
        "checkpoint_root": health._funding_manifest_batch_root(
            [checkpoint_entry], kind="CHECKPOINT"
        ),
        "combination_recipe_root": health._funding_manifest_batch_root(
            [], kind="COMBINATION_RECIPE"
        ),
        "ineligible_root": health._funding_manifest_batch_root(
            ineligible, kind="INELIGIBLE"
        ),
        "ineligible_reason_code_counts": {
            "COMBINATION_RECIPE_NOT_MATERIALIZED": 1,
        },
        "checkpoint_storage_bytes": checkpoint_storage_bytes,
        "fact_storage_bytes": fact_storage_bytes,
        "total_storage_bytes": (
            checkpoint_storage_bytes + fact_storage_bytes
        ),
        "target_total_bytes": health.FUNDING_CHECKPOINT_TOTAL_TARGET_BYTES,
        "hard_total_bytes": health.FUNDING_CHECKPOINT_TOTAL_HARD_BYTES,
        "target_total_met": True,
        "bootstrap_is_daily_bounded": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    manifest = {
        **manifest_payload,
        "manifest_hash": health._funding_canonical_hash(manifest_payload),
    }
    result = {
        "run_uid": run_uid,
        "trade_date": TRADE_DATE,
        "is_canonical": True,
        "strategies": [{
            "strategy_key": strategy_key,
            "current_version": strategy_version,
            "funding_checkpoint_ready": True,
            "funding_checkpoint_ref": checkpoint_entry,
        }],
        "combinations": [{
            "combination_key": "combo_a",
            "current_version": "v1",
            "funding_recipe_ready": False,
            "paper_allocation_eligible": False,
            "funding_manifest_ineligible": ineligible[0],
        }],
        "summary": {
            "funding_checkpoint_manifest_hash": manifest["manifest_hash"],
            "funding_checkpoint_eligible_count": 1,
            "funding_checkpointed_count": 1,
            "funding_strategy_checkpoint_count": 1,
            "funding_combination_recipe_count": 0,
            "funding_ready_count": 1,
            "funding_checkpoint_ineligible_count": 1,
        },
        "funding_checkpoint_manifest": manifest,
    }
    result_json = health._funding_canonical_json(result)
    result_hash = hashlib.sha256(result_json.encode("utf-8")).hexdigest()
    run = {
        "run_uid": run_uid,
        "trade_date": TRADE_DATE,
        "result_json": result_json,
        "result_hash": result_hash,
    }
    audit_id = "d" * 32
    audit_evidence = {
        "schema": health.FUNDING_CHECKPOINT_AUDIT_SCHEMA,
        "run_uid": run_uid,
        "canonical_result_hash": result_hash,
        "checkpoint_manifest_hash": manifest["manifest_hash"],
        "coverage": manifest["coverage"],
        "checkpoint_root": manifest["checkpoint_root"],
        "combination_recipe_root": manifest["combination_recipe_root"],
        "ineligible_root": manifest["ineligible_root"],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    audit_after = {
        "run_uid": run_uid,
        "manifest_hash": manifest["manifest_hash"],
        "checkpoint_count": 1,
    }
    audit_payload = {
        "entity_type": "SYSTEM",
        "entity_key": "strategy_funding_checkpoint_manifest",
        "action": "ANCHOR_FUNDING_CHECKPOINT_MANIFEST",
        "reason": "fixture funding anchor",
        "operator": "fixture",
        "before": {},
        "after": audit_after,
        "evidence": audit_evidence,
        "nonce": "e" * 32,
    }
    audit_hash = health._funding_canonical_hash(audit_payload)
    checkpoint_row = {
        **{
            key: value for key, value in checkpoint_entry.items()
            if key not in {
                "entity_type", "entity_key", "entity_version",
                "bootstrap_full_history_scan",
            }
        },
        "strategy_version_hash": version_hash,
        "execution_binding_hash": None,
        "holdings_json": "[]",
        "state_json": state_json,
        "chain_payload_json": chain_json,
        "previous_checkpoint_id": None,
        "previous_checkpoint_hash": None,
        "previous_chain_hash": None,
        "referenced_previous_hash": None,
        "referenced_previous_chain_hash": None,
        "referenced_previous_tip_id": None,
        "referenced_previous_tip_hash": None,
        "canonical_result_hash": result_hash,
        "anchor_audit_id": audit_id,
        "anchor_audit_hash": audit_hash,
        "automatic_real_order_submission": 0,
        "real_order_authority": 0,
    }
    fact_row = {
        "fact_id": fact_id,
        "entity_type": "STRATEGY",
        "entity_key": strategy_key,
        "entity_version": strategy_version,
        "entity_version_hash": version_hash,
        "execution_binding_hash": None,
        "account_id": account_id,
        "trade_date": TRADE_DATE,
        "origin_checkpoint_id": checkpoint_id,
        "previous_fact_id": None,
        "previous_fact_hash": None,
        "opening_cash_cny": Decimal(fact["opening_cash_cny"]),
        "closing_cash_cny": Decimal(fact["closing_cash_cny"]),
        "opening_equity_cny": Decimal(fact["opening_equity_cny"]),
        "closing_equity_cny": Decimal(fact["closing_equity_cny"]),
        "daily_return_pct": Decimal(fact["daily_return_pct"]),
        "cumulative_fee_cny": Decimal(fact["cumulative_fee_cny"]),
        "high_watermark_equity_cny": Decimal(
            fact["high_watermark_equity_cny"]
        ),
        "stock_exposure_json": "{}",
        "closed_evidence_ids_json": "[]",
        "fact_json": fact_json,
        "fact_hash": fact_hash,
        "anchor_run_uid": run_uid,
        "canonical_result_hash": result_hash,
        "anchor_audit_id": audit_id,
        "anchor_audit_hash": audit_hash,
        "automatic_real_order_submission": 0,
        "real_order_authority": 0,
    }
    audit_row = {
        "audit_id": audit_id,
        "entity_type": "SYSTEM",
        "entity_key": "strategy_funding_checkpoint_manifest",
        "action": "ANCHOR_FUNDING_CHECKPOINT_MANIFEST",
        "reason": audit_payload["reason"],
        "operator_name": audit_payload["operator"],
        "before_json": {},
        "after_json": audit_after,
        "evidence_json": audit_evidence,
        "payload_json": health._funding_canonical_json(audit_payload),
        "audit_hash": audit_hash,
        "created_at": f"{TRADE_DATE} 15:30:00",
    }
    return {
        "run": run,
        "result": result,
        "registry_rows": current_entities,
        "checkpoint_rows": [checkpoint_row],
        "fact_rows": [fact_row],
        "audit_rows": [audit_row],
    }


class _Connection:
    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, params=None):
        return self.engine.execute(str(statement), params or {})


class _FundingManifestEngine:
    def __init__(self, fixture):
        self.fixture = fixture
        self.calls = []

    def execute(self, sql, _params):
        self.calls.append((sql, deepcopy(_params)))
        if "funding_registry" in sql:
            return _Result(self.fixture["registry_rows"])
        if (
            f"FROM {health.FUNDING_CHECKPOINT_TABLE_NAME} cp" in sql
            and "referenced_previous_hash" in sql
        ):
            return _Result(self.fixture["checkpoint_rows"])
        if f"SELECT * FROM {health.FUNDING_DAILY_FACT_TABLE_NAME}" in sql:
            return _Result(self.fixture["fact_rows"])
        if "ANCHOR_FUNDING_CHECKPOINT_MANIFEST" in sql:
            return _Result(self.fixture["audit_rows"])
        raise AssertionError(f"unexpected funding manifest SQL: {sql}")


class _GovernanceHealthEngine:
    def __init__(
        self,
        runs=None,
        tasks=None,
        strategy_evidence_rows=None,
        combination_evidence_rows=None,
        metric_rows=None,
        raw_fill_rows=None,
        forward_forecast_rows=None,
        exit_allocation_rows=None,
        lifecycle_rows=None,
        audit_rows=None,
        industry_contract=None,
        dynamic_fk_rows=None,
        dynamic_check_rows=None,
        projection_registry_rows=None,
        projection_event_rows=None,
    ):
        self.runs = list(runs or [])
        self.tasks = list(
            tasks
            or [
                self._valid_task(),
                self._valid_qmt_announcement_task(),
                *self._valid_daily_pipeline_tasks(),
                *self._valid_qmt_operations_tasks(),
            ]
        )
        self.strategy_evidence_rows = list(strategy_evidence_rows or [])
        self.combination_evidence_rows = list(
            combination_evidence_rows or []
        )
        self.metric_rows = [deepcopy(row) for row in (metric_rows or [])]
        for row in self.metric_rows:
            row.setdefault("created_at", f"{TRADE_DATE} 12:00:00")
        self.raw_fill_rows = list(raw_fill_rows or [])
        self.forward_forecast_rows = list(forward_forecast_rows or [])
        self.exit_allocation_rows = list(exit_allocation_rows or [])
        self.lifecycle_rows = list(lifecycle_rows or [])
        (
            default_projection_registry_rows,
            default_projection_event_rows,
        ) = _lifecycle_projection_fixture()
        self.projection_registry_rows = deepcopy(
            default_projection_registry_rows
            if projection_registry_rows is None
            else projection_registry_rows
        )
        self.projection_event_rows = deepcopy(
            default_projection_event_rows
            if projection_event_rows is None
            else projection_event_rows
        )
        self.audit_rows = (
            None if audit_rows is None else list(audit_rows)
        )
        self.industry_contract = deepcopy(
            industry_contract or _fixture_industry_contract()
        )
        from server.engine import dynamic_shadow_ledger_schema as dynamic_schema

        if dynamic_fk_rows is None:
            self.dynamic_fk_rows = [
                {
                    "table_name": table_name,
                    "constraint_name": constraint_name,
                    "ordinal_position": ordinal,
                    "column_name": column_name,
                    "referenced_table_name": parent_table,
                    "referenced_column_name": parent_columns[ordinal - 1],
                    "update_rule": update_rule,
                    "delete_rule": delete_rule,
                }
                for constraint_name, (
                    table_name, columns, parent_table, parent_columns,
                    update_rule, delete_rule,
                ) in dynamic_schema._DYNAMIC_FOREIGN_KEY_CONTRACTS.items()
                for ordinal, column_name in enumerate(columns, 1)
            ]
        else:
            self.dynamic_fk_rows = deepcopy(dynamic_fk_rows)
        if dynamic_check_rows is None:
            self.dynamic_check_rows = [
                {
                    "table_name": table_name,
                    "constraint_name": constraint_name,
                    "check_clause": clause,
                }
                for constraint_name, (table_name, clause) in (
                    dynamic_schema._DYNAMIC_CHECK_CONTRACTS.items()
                )
            ]
        else:
            self.dynamic_check_rows = deepcopy(dynamic_check_rows)

    @staticmethod
    def _metric_audits(metric_rows):
        from server.engine import strategy_governance as governance_module

        rows = []
        for index, metric in enumerate(metric_rows, 1):
            evidence_id = str(metric.get("evidence_id") or "")
            submission = governance_module._metric_submission_contract(metric)
            if not evidence_id or submission is None:
                continue
            submitted_by = str(metric.get("submitted_by") or "")
            add_evidence = {
                "evidence_id": evidence_id,
                "evidence_hash": str(metric.get("evidence_hash") or ""),
                "artifact_hash": str(metric.get("artifact_hash") or ""),
                "source_dataset_hash": str(
                    metric.get("source_dataset_hash") or ""
                ),
                "verification_status": "PENDING",
            }
            add_payload = {
                "entity_type": metric.get("entity_type"),
                "entity_key": metric.get("strategy_key"),
                "action": "ADD_METRIC_EVIDENCE",
                "reason": "fixture submission",
                "operator": submitted_by,
                "before": {},
                "after": submission,
                "evidence": add_evidence,
                "nonce": format(1000 + index * 2, "032x"),
            }
            rows.append({
                "audit_id": format(1000 + index * 2, "032x"),
                "entity_type": metric.get("entity_type"),
                "entity_key": metric.get("strategy_key"),
                "action": "ADD_METRIC_EVIDENCE",
                "reason": add_payload["reason"],
                "operator_name": submitted_by,
                "before_json": {},
                "after_json": submission,
                "evidence_json": add_evidence,
                "payload_json": add_payload,
                "audit_hash": health._canonical_digest(add_payload),
                "created_at": metric.get("created_at"),
            })
            status = str(metric.get("verification_status") or "")
            if status not in {"CONFIRMED", "REJECTED"}:
                continue
            reviewed_by = str(metric.get("reviewed_by") or "")
            reviewed_at = governance_module._normalize_evidence_revision(
                metric.get("reviewed_at")
            )
            action = (
                "CONFIRM_METRIC_EVIDENCE"
                if status == "CONFIRMED"
                else "REJECT_METRIC_EVIDENCE"
            )
            review_evidence = {
                "evidence_id": evidence_id,
                "evidence_hash": str(metric.get("evidence_hash") or ""),
                "artifact_hash": str(metric.get("artifact_hash") or ""),
                "submitted_by": submitted_by,
                "reviewed_by": reviewed_by,
                "reviewed_at": reviewed_at,
            }
            review_after = {
                "verification_status": status,
                "reviewed_by": reviewed_by,
                "reviewed_at": reviewed_at,
            }
            review_payload = {
                "entity_type": metric.get("entity_type"),
                "entity_key": metric.get("strategy_key"),
                "action": action,
                "reason": "fixture review",
                "operator": reviewed_by,
                "before": {"verification_status": "PENDING"},
                "after": review_after,
                "evidence": review_evidence,
                "nonce": format(1001 + index * 2, "032x"),
            }
            rows.append({
                "audit_id": format(1001 + index * 2, "032x"),
                "entity_type": metric.get("entity_type"),
                "entity_key": metric.get("strategy_key"),
                "action": action,
                "reason": review_payload["reason"],
                "operator_name": reviewed_by,
                "before_json": {"verification_status": "PENDING"},
                "after_json": review_after,
                "evidence_json": review_evidence,
                "payload_json": review_payload,
                "audit_hash": health._canonical_digest(review_payload),
                "created_at": reviewed_at,
            })
        return rows

    @staticmethod
    def _valid_task():
        return {
            "id": 218,
            "task_name": TASK["task_name"],
            "task_type": TASK["task_type"],
            "group_name": TASK["group_name"],
            "script_path": TASK["script_path"],
            "script_args": TASK["script_args"],
            "cron_time": TASK["cron_time"],
            "interval_minutes": TASK["interval_minutes"],
            "date_param": TASK["date_param"],
            "enabled": TASK["enabled"],
        }

    @staticmethod
    def _valid_qmt_announcement_task():
        return {
            "id": 89,
            "task_name": QMT_ANNOUNCEMENT_TASK["task_name"],
            "task_type": QMT_ANNOUNCEMENT_TASK["task_type"],
            "group_name": QMT_ANNOUNCEMENT_TASK["group_name"],
            "script_path": QMT_ANNOUNCEMENT_TASK["script_path"],
            "script_args": QMT_ANNOUNCEMENT_TASK["script_args"],
            "cron_time": QMT_ANNOUNCEMENT_TASK["cron_time"],
            "interval_minutes": QMT_ANNOUNCEMENT_TASK[
                "interval_minutes"
            ],
            "date_param": QMT_ANNOUNCEMENT_TASK["date_param"],
            "enabled": QMT_ANNOUNCEMENT_TASK["enabled"],
        }

    @staticmethod
    def _valid_daily_pipeline_tasks():
        return [
            {
                "id": 90,
                "task_type": "analysis_upper_evidence_prepare",
                "cron_time": ANALYSIS_UPPER_EVIDENCE_CRON,
                "enabled": 1,
            },
            {
                "id": 91,
                "task_type": "analysis_fast",
                "cron_time": ANALYSIS_FAST_CRON,
                "enabled": 1,
            },
        ]

    @staticmethod
    def _valid_qmt_operations_tasks():
        return [
            {
                "id": 300 + index,
                **{
                    key: task[key]
                    for key in (
                        "task_name",
                        "task_type",
                        "group_name",
                        "script_path",
                        "script_args",
                        "cron_time",
                        "interval_minutes",
                        "date_param",
                        "enabled",
                    )
                },
            }
            for index, task in enumerate(QMT_OPERATIONS_TASKS, 1)
        ]

    def connect(self):
        return _Connection(self)

    def _strategy_governance_raw_replay_fixture(
        self,
        *,
        snapshot_contract,
        run_uid,
        trade_date,
        authoritative_windows,
    ):
        """Represent a successful independent raw replay in health unit tests."""

        replay = deepcopy(snapshot_contract)
        strategy_baseline = {
            "|".join((
                "STRATEGY", row["strategy_key"], row["strategy_version"],
                str(row["window_days"]),
            )): {
                "metrics": row["evidence_json"]["metrics"],
                "gate": row["evidence_json"]["gate"],
                "overall_profit_gate_passed": row["evidence_json"][
                    "overall_profit_gate_passed"
                ],
                "funding_gate_hash": row["evidence_json"][
                    "pre_confirmation_funding_gate_hash"
                ],
            }
            for row in _valid_strategy_router_rows()
        }
        combination_row = _valid_combination_router_rows()[0]
        combination_baseline = {
            "COMBINATION|combo_a|v1": {
                "metrics": combination_row["evidence_json"]["metrics"],
                "multi_window_gate": combination_row["evidence_json"][
                    "multi_window_gate"
                ],
                "overall_profit_gate_passed": combination_row[
                    "evidence_json"
                ]["overall_profit_gate_passed"],
                "funding_gate_hash": combination_row["evidence_json"][
                    "pre_confirmation_funding_gate_hash"
                ],
            }
        }
        for key, baseline in strategy_baseline.items():
            if key in replay["strategies"]:
                replay["strategies"][key].update(deepcopy(baseline))
        for key, baseline in combination_baseline.items():
            if key in replay["combinations"]:
                replay["combinations"][key].update(deepcopy(baseline))
        if any(row.get("tamper_raw_metric") for row in self.raw_fill_rows):
            first_key = sorted(replay["strategies"])[0]
            replay["strategies"][first_key]["metrics"][
                "net_expectancy_pct"
            ] = -99.0
        return replay

    def _release_trigger_rows_fixture(self, rows, _sql, _params):
        """Allow focused tests to adversarially drift frozen trigger metadata."""

        return rows

    def execute(self, sql, params):
        from server.engine import dynamic_shadow_ledger_schema as dynamic_schema
        from server.engine import strategy_funding_checkpoint as funding_schema

        if (
            "information_schema.TRIGGERS" in sql
            and "WHERE TRIGGER_SCHEMA=DATABASE() ORDER BY" in sql
        ):
            from tools import prepare_strategy_governance_schema as schema

            managed = {
                **schema._final_v3_trigger_contracts(),
                **schema._frozen_non_v3_release_trigger_contracts(
                    schema._non_v3_trigger_contracts()
                ),
            }
            v2_contracts, v2_bodies, v2_orders = (
                schema._v2_release_trigger_contract()
            )
            rows = [
                {
                    "trigger_schema": schema.DATABASE_NAME,
                    "trigger_name": name,
                    "definer": schema.EXPECTED_MIGRATOR_USER,
                    "event_object_schema": schema.DATABASE_NAME,
                    "action_timing": contract.timing,
                    "event_manipulation": contract.event,
                    "event_object_table": contract.table,
                    "action_orientation": "ROW",
                    "action_statement": contract.body,
                    "action_order": 1,
                    "sql_mode": schema.EXPECTED_SQL_MODE,
                    "character_set_client": (
                        schema.EXPECTED_CHARACTER_SET_CLIENT
                    ),
                    "collation_connection": (
                        schema.EXPECTED_COLLATION_CONNECTION
                    ),
                    "database_collation": (
                        schema.EXPECTED_DATABASE_COLLATION
                    ),
                }
                for name, contract in sorted(managed.items())
            ]
            rows.extend({
                "trigger_schema": schema.DATABASE_NAME,
                "trigger_name": name,
                "definer": schema.EXPECTED_MIGRATOR_USER,
                "event_object_schema": schema.DATABASE_NAME,
                "action_timing": "BEFORE",
                "event_manipulation": event,
                "event_object_table": table_name,
                "action_orientation": "ROW",
                "action_statement": v2_bodies[name],
                "action_order": v2_orders[name],
                "sql_mode": schema.EXPECTED_SQL_MODE,
                "character_set_client": schema.EXPECTED_CHARACTER_SET_CLIENT,
                "collation_connection": schema.EXPECTED_COLLATION_CONNECTION,
                "database_collation": schema.EXPECTED_DATABASE_COLLATION,
            } for name, (event, table_name) in sorted(v2_contracts.items()))
            return _Result(
                self._release_trigger_rows_fixture(rows, sql, params)
            )

        if (
            "information_schema.TRIGGERS" in sql
            and "DEFINER AS definer" in sql
            and any(
                str(key).startswith(("trigger_name_", "trigger_table_"))
                for key in params
            )
        ):
            from tools import prepare_strategy_governance_schema as schema

            contracts = {
                **schema._final_v3_trigger_contracts(),
                **schema._non_v3_trigger_contracts(),
            }
            requested_names = {
                str(value)
                for key, value in params.items()
                if str(key).startswith("trigger_name_")
            }
            controlled_tables = {
                str(value)
                for key, value in params.items()
                if str(key).startswith("trigger_table_")
            }
            selected = {
                name: contract
                for name, contract in contracts.items()
                if name in requested_names
                or contract.table in controlled_tables
            }
            rows = [
                {
                    "trigger_name": name,
                    "definer": schema.EXPECTED_MIGRATOR_USER,
                    "action_timing": contract.timing,
                    "event_manipulation": contract.event,
                    "event_object_table": contract.table,
                    "action_orientation": "ROW",
                    "action_statement": contract.body,
                    "sql_mode": schema.EXPECTED_SQL_MODE,
                    "character_set_client": (
                        schema.EXPECTED_CHARACTER_SET_CLIENT
                    ),
                    "collation_connection": (
                        schema.EXPECTED_COLLATION_CONNECTION
                    ),
                    "database_collation": schema.EXPECTED_DATABASE_COLLATION,
                }
                for name, contract in sorted(selected.items())
            ]
            return _Result(
                self._release_trigger_rows_fixture(rows, sql, params)
            )

        funding_tables = set(funding_schema._TABLE_CONTRACTS)
        requested_funding_table = next(
            (table_name for table_name in funding_tables if table_name in sql),
            None,
        )
        if requested_funding_table and "information_schema.TABLES" in sql:
            return _Result([{
                "engine": "InnoDB",
                "table_collation": "utf8mb4_unicode_ci",
            }])
        if requested_funding_table and "information_schema.COLUMNS" in sql:
            contract = funding_schema._TABLE_CONTRACTS[
                requested_funding_table
            ]
            rows = []
            for ordinal, (name, column_type, nullable, default) in enumerate(
                contract["columns"], 1
            ):
                character = column_type.split("(", 1)[0] in {
                    "char", "varchar", "text", "longtext",
                }
                rows.append({
                    "column_name": name,
                    "ordinal_position": ordinal,
                    "column_type": column_type,
                    "is_nullable": nullable,
                    "column_default": default,
                    "extra": "",
                    "character_set_name": "utf8mb4" if character else None,
                    "collation_name": (
                        "utf8mb4_unicode_ci" if character else None
                    ),
                })
            return _Result(rows)
        if requested_funding_table and "information_schema.STATISTICS" in sql:
            indexes = funding_schema._TABLE_CONTRACTS[
                requested_funding_table
            ]["indexes"]
            return _Result([
                {
                    "index_name": name,
                    "non_unique": non_unique,
                    "seq_in_index": ordinal,
                    "column_name": column,
                    "sub_part": None,
                    "index_type": "BTREE",
                }
                for name, (non_unique, columns) in indexes.items()
                for ordinal, column in enumerate(columns, 1)
            ])
        if (
            requested_funding_table
            and "information_schema.REFERENTIAL_CONSTRAINTS" in sql
        ):
            foreign_keys = funding_schema._TABLE_CONTRACTS[
                requested_funding_table
            ]["foreign_keys"]
            return _Result([
                {
                    "constraint_name": name,
                    "ordinal_position": ordinal,
                    "column_name": column,
                    "referenced_table_name": parent_table,
                    "referenced_column_name": parent_columns[ordinal - 1],
                    "update_rule": update_rule,
                    "delete_rule": delete_rule,
                }
                for name, (
                    columns,
                    parent_table,
                    parent_columns,
                    update_rule,
                    delete_rule,
                ) in foreign_keys.items()
                for ordinal, column in enumerate(columns, 1)
            ])
        if (
            requested_funding_table
            and "information_schema.CHECK_CONSTRAINTS" in sql
        ):
            checks = funding_schema._TABLE_CONTRACTS[
                requested_funding_table
            ]["checks"]
            return _Result([
                {"constraint_name": name, "check_clause": clause}
                for name, clause in checks.items()
            ])
        if (
            "information_schema.TRIGGERS" in sql
            and all(table_name in sql for table_name in funding_tables)
            and "DEFINER AS definer" in sql
        ):
            return _Result([
                {
                    "trigger_name": name,
                    "event": event,
                    "timing": timing,
                    "table_name": table_name,
                    "orientation": "ROW",
                    "body": body,
                    "definer": "probiga_migrator@127.0.0.1",
                }
                for name, (
                    timing,
                    event,
                    table_name,
                    body,
                ) in funding_schema.FUNDING_CHECKPOINT_TRIGGER_CONTRACTS.items()
            ])

        if sql.strip() == (
            "SELECT COUNT(*) FROM st_strategy_adapter_candidate_fact"
        ):
            return _Result([{"count": 0}])
        if sql.startswith(
            "SELECT * FROM st_strategy_adapter_candidate_fact "
        ):
            return _Result([])
        if sql.startswith(
            "SELECT COUNT(*) FROM st_strategy_adapter_run_receipt r "
        ) and "st_strategy_adapter_candidate_fact cf" in sql:
            return _Result([{"count": 0}])
        if sql.startswith(
            "SELECT r.* FROM st_strategy_adapter_run_receipt r "
        ) and "st_strategy_adapter_candidate_fact cf" in sql:
            return _Result([])
        if sql.strip() == "SELECT COUNT(*) FROM st_dynamic_shadow_trial_plan":
            return _Result([{"count": 0}])
        if sql.startswith(
            "SELECT strategy_key, strategy_version, strategy_version_hash, "
        ) and "FROM st_dynamic_shadow_trial_plan" in sql:
            return _Result([])
        if "FROM information_schema.TABLES" in sql and (
            "st_dynamic_shadow_trial_plan" in sql
        ):
            return _Result([{
                "table_name": table_name,
                "engine": "InnoDB",
                "table_collation": "utf8mb4_unicode_ci",
            } for table_name in dynamic_schema.DYNAMIC_SHADOW_LEDGER_TABLE_NAMES])
        if "FROM information_schema.COLUMNS" in sql and (
            "st_dynamic_shadow_trial_plan" in sql
        ):
            return _Result([
                {
                    "table_name": table_name,
                    "column_name": column["name"],
                    "ordinal_position": ordinal,
                    "column_type": column["column_type"],
                    "is_nullable": column["is_nullable"],
                    "column_default": column["column_default"],
                    "extra": column["extra"],
                    "character_set_name": column["character_set_name"],
                    "collation_name": column["collation_name"],
                }
                for table_name in dynamic_schema.DYNAMIC_SHADOW_LEDGER_TABLE_NAMES
                for ordinal, column in enumerate(
                    dynamic_schema._DYNAMIC_COLUMN_CONTRACTS[table_name], 1
                )
            ])
        if "FROM information_schema.STATISTICS" in sql and (
            "st_dynamic_shadow_trial_plan" in sql
        ):
            return _Result([
                {
                    "table_name": table_name,
                    "index_name": index_name,
                    "non_unique": 0 if unique else 1,
                    "seq_in_index": ordinal,
                    "column_name": column_name,
                    "sub_part": None,
                    "index_type": "BTREE",
                }
                for table_name in dynamic_schema.DYNAMIC_SHADOW_LEDGER_TABLE_NAMES
                for index_name, (unique, columns, _ddl) in (
                    dynamic_schema._DYNAMIC_INDEX_CONTRACTS[table_name].items()
                )
                for ordinal, column_name in enumerate(columns, 1)
            ])
        if (
            "information_schema.table_constraints" in sql.lower()
            and "information_schema.referential_constraints" in sql.lower()
        ):
            return _Result(deepcopy(self.dynamic_fk_rows))
        if (
            "information_schema.table_constraints" in sql.lower()
            and "information_schema.check_constraints" in sql.lower()
        ):
            return _Result(deepcopy(self.dynamic_check_rows))
        if (
            "FROM st_strategy_industry_history" in sql
            and "source_fact_id" in sql
            and "row_hash" in sql
        ):
            return _Result(self.industry_contract["history_rows"])
        if (
            "FROM qmt_membership_snapshot_run" in sql
            and "fallback_target_date" in params
        ):
            return _Result(self.industry_contract.get("target_runs", []))
        if "FROM si_trade_calendar" in sql and "MAX(trade_date)" in sql:
            return _Result([{
                "trade_date": self.industry_contract.get(
                    "previous_trade_date"
                )
            }])
        if (
            "FROM qmt_membership_snapshot_run" in sql
            and "industry_relation_count" in sql
        ):
            return _Result(self.industry_contract.get(
                "runs", [self.industry_contract["run"]],
            ))
        if (
            "FROM qmt_industry_member_snapshot" in sql
            and "industry_code" in sql
        ):
            return _Result(self.industry_contract.get(
                "all_members", self.industry_contract["members"],
            ))
        if "FROM qmt_kline_attestation_schema_migration" in sql:
            if params.get("migration_key") == (
                qmt_attester.LEGACY_MANIFEST_GRANDFATHER_MIGRATION_KEY
            ):
                return _Result([])
            return _Result(
                [{
                    "migration_hash": (
                        qmt_attester
                        .TOLERANCE_MEDIUMTEXT_MIGRATION_HASH
                    )
                }]
            )
        if (
            "FROM qmt_kline_attestation_run" in sql
            and "WHERE status='COMPLETED'" in sql
        ):
            return _Result([])
        if (
            "ENGINE AS engine" in sql
            and "qmt_kline_attestation_run" in sql
        ):
            return _Result(
                [
                    {
                        "table_name": table_name,
                        "engine": "InnoDB",
                        "table_collation": (
                            qmt_attester.QMT_ATTESTATION_COLLATION
                        ),
                    }
                    for table_name in qmt_attester.ATTESTATION_TABLE_NAMES
                ]
            )
        if (
            "ORDINAL_POSITION AS ordinal_position" in sql
            and "qmt_kline_attestation_run" in sql
        ):
            return _Result(
                [
                    {
                        "table_name": table_name,
                        "column_name": column[0],
                        "ordinal_position": ordinal,
                        "data_type": column[1],
                        "character_maximum_length": column[2],
                        "numeric_precision": column[3],
                        "numeric_scale": column[4],
                        "is_nullable": column[5],
                        "column_default": column[6],
                        "extra": column[7],
                        "character_set_name": (
                            "utf8mb4"
                            if column[1]
                            in {"char", "varchar", "text", "mediumtext"}
                            else None
                        ),
                        "collation_name": (
                            qmt_attester.QMT_ATTESTATION_COLLATION
                            if column[1]
                            in {"char", "varchar", "text", "mediumtext"}
                            else None
                        ),
                    }
                    for table_name, columns in (
                        qmt_attester._ATTESTATION_COLUMN_CONTRACTS.items()
                    )
                    for ordinal, column in enumerate(columns, 1)
                ]
            )
        if (
            "INDEX_TYPE AS index_type" in sql
            and "qmt_kline_attestation_run" in sql
        ):
            return _Result(
                [
                    {
                        "table_name": table_name,
                        "index_name": index_name,
                        "non_unique": non_unique,
                        "seq_in_index": sequence,
                        "column_name": column_name,
                        "sub_part": None,
                        "index_type": "BTREE",
                    }
                    for table_name, indexes in (
                        qmt_attester._ATTESTATION_INDEX_CONTRACTS.items()
                    )
                    for index_name, (non_unique, columns) in indexes.items()
                    for sequence, column_name in enumerate(columns, 1)
                ]
            )
        if (
            "information_schema.TRIGGERS" in sql
            and "qmt_kline_attestation_row" in sql
        ):
            return _Result(
                [
                    {
                        "trigger_name": trigger_name,
                        "action_timing": timing,
                        "event_manipulation": event,
                        "event_object_table": table_name,
                        "action_orientation": "ROW",
                        "action_statement": body,
                    }
                    for trigger_name, (
                        timing,
                        event,
                        table_name,
                        body,
                    ) in qmt_attester._ATTESTATION_TRIGGER_CONTRACTS.items()
                ]
            )
        if (
            "information_schema.TRIGGERS" in sql
            and "st_strategy_metric_input" in sql
        ):
            from server.engine import strategy_governance as governance_module

            return _Result(
                [
                    {
                        "trigger_name": trigger_name,
                        "action_timing": contract["timing"],
                        "event_manipulation": contract["event"],
                        "event_object_table": contract["table"],
                        "action_orientation": "ROW",
                        "action_statement": contract["body"],
                    }
                    for trigger_name, contract in (
                        governance_module
                        .METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS.items()
                    )
                ]
            )
        if (
            "information_schema.TRIGGERS" in sql
            and "st_strategy_lifecycle_event" in sql
            and "st_strategy_governance_audit" in sql
        ):
            from server.engine import strategy_governance as governance_module

            return _Result(
                [
                    {
                        "trigger_name": trigger_name,
                        "action_timing": timing,
                        "event_manipulation": event,
                        "event_object_table": table_name,
                        "action_orientation": "ROW",
                        "action_statement": body,
                    }
                    for trigger_name, (
                        timing,
                        event,
                        table_name,
                        body,
                    ) in (
                        governance_module
                        .GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACTS.items()
                    )
                ]
            )
        if "FROM st_strategy_governance_schema_migration" in sql:
            from server.engine.strategy_governance import (
                RUN_REVISION_MIGRATION_HASH,
                RUN_REVISION_MIGRATION_KEY,
                STRATEGY_CONTENT_HASH_MIGRATION_HASH,
                STRATEGY_CONTENT_HASH_MIGRATION_KEY,
            )

            return _Result(
                [
                    {
                        "migration_key": RUN_REVISION_MIGRATION_KEY,
                        "migration_hash": RUN_REVISION_MIGRATION_HASH,
                        "completed_at": f"{TRADE_DATE} 12:00:00",
                    },
                    {
                        "migration_key": STRATEGY_CONTENT_HASH_MIGRATION_KEY,
                        "migration_hash": STRATEGY_CONTENT_HASH_MIGRATION_HASH,
                        "completed_at": f"{TRADE_DATE} 12:00:00",
                    },
                    {
                        "migration_key": health.FUNDING_CHECKPOINT_MIGRATION_KEY,
                        "migration_hash": health.FUNDING_CHECKPOINT_MIGRATION_HASH,
                        "completed_at": f"{TRADE_DATE} 12:00:00",
                    },
                ]
            )
        if (
            "COUNT(*) AS total_count" in sql
            and "FROM schema_migration_v3" in sql
        ):
            from server.db.migrations_v3 import MIGRATIONS

            return _Result([{"total_count": len(MIGRATIONS)}])
        if "FROM schema_migration_v3" in sql:
            from server.db.migrations_v3 import MIGRATIONS, _checksum

            migration = next(
                item
                for item in MIGRATIONS
                if item["version"] == params["version"]
            )
            statements = tuple(migration["statements"])
            return _Result(
                [
                    {
                        "version": migration["version"],
                        "checksum": _checksum(statements),
                        "statement_count": len(statements),
                    }
                ]
            )
        if (
            "ENGINE AS engine" in sql
            and "st_forward_exit_allocation_v3" in sql
        ):
            return _Result(
                [
                    {
                        "engine": "InnoDB",
                        "table_collation": "utf8mb4_unicode_ci",
                    }
                ]
            )
        if (
            "CHARACTER_MAXIMUM_LENGTH AS character_maximum_length"
            in sql
            and "st_forward_exit_allocation_v3" in sql
        ):
            contracts = (
                ("allocation_id", "char", 64, None, None, "NO"),
                ("evidence_id", "char", 64, None, None, "YES"),
                ("attribution_status", "varchar", 32, None, None, "NO"),
                ("account_id", "varchar", 64, None, None, "NO"),
                ("stock_code", "varchar", 16, None, None, "NO"),
                ("entry_fill_id", "varchar", 64, None, None, "NO"),
                ("exit_fill_id", "varchar", 64, None, None, "NO"),
                ("exit_order_id", "varchar", 64, None, None, "NO"),
                ("allocation_sequence", "bigint", None, 19, 0, "NO"),
                ("allocated_quantity", "bigint", None, 19, 0, "NO"),
                ("allocated_gross_cny", "decimal", None, 20, 6, "NO"),
                ("allocated_fee_cny", "decimal", None, 20, 6, "NO"),
                ("exit_filled_at", "datetime", None, None, None, "NO"),
                (
                    "allocation_protocol_version",
                    "varchar",
                    80,
                    None,
                    None,
                    "NO",
                ),
                ("created_at", "datetime", None, None, None, "NO"),
            )
            return _Result(
                [
                    {
                        "column_name": name,
                        "data_type": data_type,
                        "character_maximum_length": char_length,
                        "numeric_precision": precision,
                        "numeric_scale": scale,
                        "is_nullable": nullable,
                        "column_default": None,
                        "extra": "",
                    }
                    for (
                        name,
                        data_type,
                        char_length,
                        precision,
                        scale,
                        nullable,
                    ) in contracts
                ]
            )
        if (
            "TABLE_NAME='st_forward_exit_allocation_v3'" in sql
            and "referential_constraints" in sql
        ):
            return _Result(
                [
                    {
                        "constraint_name": name,
                        "column_name": column,
                        "referenced_table_name": table,
                        "referenced_column_name": "evidence_id"
                        if table == "st_forward_trade_evidence_v3"
                        else "fill_id",
                        "update_rule": "RESTRICT",
                        "delete_rule": "RESTRICT",
                    }
                    for name, column, table in (
                        (
                            "fk_v3_forward_exit_allocation_evidence",
                            "evidence_id",
                            "st_forward_trade_evidence_v3",
                        ),
                        (
                            "fk_v3_forward_exit_allocation_entry_fill",
                            "entry_fill_id",
                            "st_fill_v2",
                        ),
                        (
                            "fk_v3_forward_exit_allocation_fill",
                            "exit_fill_id",
                            "st_fill_v2",
                        ),
                    )
                ]
            )
        if (
            "table_name='st_forward_exit_allocation_v3'" in sql
            and "FROM information_schema.statistics" in sql
        ):
            contracts = {
                "PRIMARY": (0, ("allocation_id",)),
                "uk_v3_forward_exit_evidence_fill": (
                    0,
                    ("evidence_id", "exit_fill_id"),
                ),
                "uk_v3_forward_exit_fill_sequence": (
                    0,
                    ("exit_fill_id", "allocation_sequence"),
                ),
                "uk_v3_forward_exit_fill_entry": (
                    0,
                    ("exit_fill_id", "entry_fill_id"),
                ),
                "idx_v3_forward_exit_evidence": (
                    1,
                    ("evidence_id", "exit_filled_at"),
                ),
                "idx_v3_forward_exit_entry": (1, ("entry_fill_id",)),
                "idx_v3_forward_exit_account": (
                    1,
                    ("account_id", "stock_code", "exit_filled_at"),
                ),
            }
            return _Result(
                [
                    {
                        "index_name": name,
                        "non_unique": non_unique,
                        "seq_in_index": sequence,
                        "column_name": column,
                        "sub_part": None,
                    }
                    for name, (non_unique, columns) in contracts.items()
                    for sequence, column in enumerate(columns, 1)
                ]
            )
        if (
            "table_name='st_forward_trade_evidence_v3'" in sql
            and "column_name='strategy_version'" in sql
        ):
            return _Result(
                [
                    {
                        "column_type": "varchar(160)",
                        "is_nullable": "NO",
                        "column_default": "",
                    }
                ]
            )
        if "index_name='idx_v3_forward_strategy_version'" in sql:
            return _Result(
                [
                    {
                        "non_unique": 1,
                        "seq_in_index": index,
                        "column_name": column,
                        "sub_part": None,
                    }
                    for index, column in enumerate(
                        (
                            "strategy_key",
                            "strategy_version",
                            "evidence_status",
                            "exit_at",
                        ),
                        1,
                    )
                ]
            )
        if "FROM information_schema.triggers" in sql:
            from server.db.migrations_v3 import (
                FORWARD_EXIT_ALLOCATION_DDL,
                FORWARD_STRATEGY_VERSION_DDL,
                V2_RAW_LEDGER_IMMUTABILITY_DDL,
                _CREATE_TRIGGER_RE,
            )

            rows = []
            statements = (
                FORWARD_EXIT_ALLOCATION_DDL
                if "st_forward_exit_allocation_v3" in sql
                else (
                    V2_RAW_LEDGER_IMMUTABILITY_DDL
                    if "st_cash_ledger_v2" in sql
                    else FORWARD_STRATEGY_VERSION_DDL
                )
            )
            for statement in statements:
                match = _CREATE_TRIGGER_RE.match(str(statement))
                if match is None:
                    continue
                name, timing, event, table_name, body = match.groups()
                rows.append(
                    {
                        "trigger_name": name,
                        "event_object_table": table_name,
                        "action_timing": timing,
                        "event_manipulation": event,
                        "action_statement": body,
                    }
                )
            return _Result(rows)
        if (
            "AS eligible_empty_count" in sql
            and "FROM st_forward_trade_evidence_v3 e" in sql
        ):
            return _Result(
                [
                    {
                        "total_count": 0,
                        "versioned_count": 0,
                        "quarantined_count": 0,
                        "eligible_empty_count": 0,
                        "invalid_nonempty_count": 0,
                        "current_version_evidence_count": 0,
                    }
                ]
            )
        if (
            "COUNT(*) AS raw_sell_count" in sql
            and "FROM st_fill_v2" in sql
        ):
            return _Result(
                [
                    {
                        "raw_sell_count": sum(
                            str(row.get("side") or "").upper() == "SELL"
                            for row in self.raw_fill_rows
                        )
                    }
                ]
            )
        if "SELECT f.fill_id, f.order_id, f.account_id" in sql:
            return _Result(self.raw_fill_rows)
        if (
            "SELECT f.forecast_id, f.run_uid, f.stock_code" in sql
            and "run_model_version" in sql
        ):
            return _Result(self.forward_forecast_rows)
        if (
            "FROM st_forward_exit_allocation_v3 a" in sql
            and "bound_evidence_id" in sql
        ):
            return _Result(self.exit_allocation_rows)
        if "FROM information_schema.tables" in sql:
            return _Result(
                scalar_values=(
                    set(health.GOVERNANCE_TABLES) | {"st_scheduled_tasks"}
                )
            )
        if "FROM information_schema.columns" in sql:
            return _Result(
                scalar_values=health.REQUIRED_COLUMNS.get(
                    params["table_name"], frozenset()
                )
            )
        if "FROM information_schema.statistics" in sql:
            if "non_unique" in sql:
                rows = []
                for index_name, (columns, unique) in (
                    health.REQUIRED_INDEX_CONTRACTS.get(
                        params["table_name"], {}
                    ).items()
                ):
                    rows.extend(
                        {
                            "index_name": index_name,
                            "non_unique": 0 if unique else 1,
                            "seq_in_index": index,
                            "column_name": column,
                            "sub_part": None,
                        }
                        for index, column in enumerate(columns, 1)
                    )
                return _Result(rows)
            return _Result(
                scalar_values=health.REQUIRED_INDEXES.get(
                    params["table_name"], frozenset()
                )
            )
        if "FROM st_strategy_adapter_candidate_fact" in sql:
            return _Result([])
        if "FROM st_dynamic_shadow_trial_plan" in sql:
            return _Result([])
        if "FROM st_scheduled_tasks" in sql:
            task_types = {
                str(value or "")
                for key, value in params.items()
                if str(key).startswith("task_type")
            }
            script_paths = {
                str(value or "")
                for key, value in params.items()
                if str(key).startswith("script_path")
            }
            return _Result([
                row
                for row in self.tasks
                if str(row.get("task_type") or "") in task_types
                or str(row.get("script_path") or "") in script_paths
            ])
        if (
            "FROM st_strategy_lifecycle_event" in sql
            and "trigger_type, payload_json" in sql
            and "evidence_json" not in sql
        ):
            return _Result(deepcopy(self.projection_event_rows))
        if "FROM st_strategy_lifecycle_event" in sql:
            return _Result(deepcopy(self.lifecycle_rows))
        if "FROM st_strategy_governance_audit" in sql:
            if self.audit_rows is not None:
                return _Result(deepcopy(self.audit_rows))
            completed = [
                row
                for row in self.runs
                if row.get("status") == "COMPLETED"
            ]
            completed.sort(
                key=lambda row: (
                    row.get("trade_date") or "",
                    row.get("run_revision") or 0,
                    row.get("finished_at") or "",
                    row.get("created_at") or "",
                    row.get("run_uid") or "",
                ),
                reverse=True,
            )
            audit_rows = []
            for index, run in enumerate(completed, 1):
                before = {}
                after = {
                    "status": "COMPLETED",
                    "trade_date": run["trade_date"],
                    "run_revision": run["run_revision"],
                    "supersedes_run_uid": (
                        run.get("supersedes_run_uid") or ""
                    ),
                    "is_canonical": True,
                    "summary": run["summary_json"],
                }
                evidence = {
                    "run_uid": run["run_uid"],
                    "run_revision": run["run_revision"],
                    "supersedes_run_uid": (
                        run.get("supersedes_run_uid") or ""
                    ),
                    "input_hash": run["input_hash"],
                    "decision_hash": run["decision_hash"],
                    "build_commit_sha": run["build_commit_sha"],
                    "router_policy_version": run["router_policy_version"],
                    "router_snapshot_hash": run["router_snapshot_hash"],
                    "automatic_real_order_submission": False,
                }
                payload = {
                    "entity_type": "SYSTEM",
                    "entity_key": "strategy_governance_daily",
                    "action": "RUN_GOVERNANCE",
                    "reason": "completed test governance",
                    "operator": "scheduled_daily_governance",
                    "before": before,
                    "after": after,
                    "evidence": evidence,
                    "nonce": format(index, "032x"),
                }
                audit_rows.append(
                    {
                        "audit_id": format(index, "032x"),
                        "entity_type": "SYSTEM",
                        "entity_key": "strategy_governance_daily",
                        "action": "RUN_GOVERNANCE",
                        "reason": payload["reason"],
                        "operator_name": payload["operator"],
                        "before_json": before,
                        "after_json": after,
                        "evidence_json": evidence,
                        "payload_json": payload,
                        "audit_hash": health._canonical_digest(payload),
                        "created_at": run["finished_at"],
                    }
                )
            audit_rows.extend(self._metric_audits(self.metric_rows))
            return _Result(audit_rows)
        if "SELECT i.evidence_id" in sql:
            requested = set(params.values())
            return _Result(
                [
                    row
                    for row in self.metric_rows
                    if row["evidence_hash"] in requested
                ]
            )
        if "SELECT h.strategy_key, h.strategy_version" in sql:
            return _Result(_valid_strategy_router_rows())
        if "SELECT h.combination_key, h.combination_version" in sql:
            return _Result(_valid_combination_router_rows())
        if "SELECT strategy_key, version, version_hash, content_hash" in sql:
            return _Result(
                [
                    {
                        "strategy_key": key,
                        "version": "v1",
                        "version_hash": _strategy_version_hash(key),
                        "content_hash": _strategy_content_hash(key),
                        "evaluator_type": "external_evidence",
                        "evaluator_config_json": json.dumps(
                            _strategy_evaluator_config()
                        ),
                        "parameters_json": "{}",
                        "source_kind": "runtime_registry",
                    }
                    for key in sorted(STRATEGY_ROUTES)
                ]
            )
        if "SELECT combination_key, version, members_json" in sql:
            rows = _valid_combination_router_rows()
            return _Result(
                [
                    {
                        "combination_key": row["combination_key"],
                        "version": row["combination_version"],
                        "members_json": row["members_json"],
                        "constraints_json": row["constraints_json"],
                        "config_hash": row["config_hash"],
                    }
                    for row in rows
                ]
            )
        if "SELECT r.strategy_key, r.current_version" in sql:
            return _Result(
                [
                    {
                        "strategy_key": key,
                        "current_version": "v1",
                        "version_hash": _strategy_version_hash(key),
                        "evaluator_type": "external_evidence",
                        "evaluator_config_json": json.dumps(
                            _strategy_evaluator_config()
                        ),
                        "parameters_json": "{}",
                        "source_kind": "runtime_registry",
                    }
                    for key in sorted(STRATEGY_ROUTES)
                ]
            )
        if "SELECT c.combination_key, c.current_version" in sql:
            rows = _valid_combination_router_rows()
            return _Result(
                [
                    {
                        "combination_key": row["combination_key"],
                        "current_version": row["combination_version"],
                        "members_json": row["members_json"],
                        "constraints_json": row["constraints_json"],
                        "config_hash": row["config_hash"],
                    }
                    for row in rows
                ]
            )
        if (
            "SELECT entity_type, entity_key, entity_version, current_status"
            in sql
            and "current_registry" in sql
        ):
            return _Result(deepcopy(self.projection_registry_rows))
        if "LEFT JOIN st_strategy_version" in sql:
            return _Result(
                [
                    {
                        "registry_count": 2,
                        "missing_current_version_count": 0,
                        "invalid_version_hash_count": 0,
                    }
                ]
            )
        if "LEFT JOIN st_strategy_combination_version" in sql:
            return _Result(
                [
                    {
                        "registry_count": 1,
                        "missing_current_version_count": 0,
                        "invalid_config_hash_count": 0,
                    }
                ]
            )
        if (
            "FROM st_strategy_registry" in sql
            and "LEFT JOIN" not in sql
        ):
            return _Result([{"total": 2, "invalid_status_count": 0}])
        if (
            "FROM st_strategy_combination" in sql
            and "LEFT JOIN" not in sql
            and "health_snapshot" not in sql
        ):
            return _Result([{"total": 1, "invalid_status_count": 0}])
        if (
            "FROM st_strategy_metric_input ORDER BY created_at, evidence_id"
            in sql
        ):
            return _Result(deepcopy(self.metric_rows))
        if (
            "SELECT evidence_id, artifact_hash, source_dataset_hash" in sql
            and "FROM st_strategy_metric_input" in sql
        ):
            return _Result(deepcopy(self.metric_rows))
        if "FROM st_strategy_metric_input" in sql:
            return _Result(
                [
                    {
                        "total": 0,
                        "invalid_status_count": 0,
                        "invalid_provenance_count": 0,
                        "invalid_contract_count": 0,
                        "invalid_review_count": 0,
                        "mutated_pending_count": 0,
                        "confirmed_missing_artifact_count": 0,
                        "invalid_confirmed_protocol_count": 0,
                    }
                ]
            )
        if (
            "AS canonical_count" in sql
            and "FROM st_strategy_governance_run WHERE is_canonical=1"
            in sql
        ):
            canonical = [
                row
                for row in self.runs
                if row.get("is_canonical") == 1
            ]
            return _Result(
                [
                    {
                        "canonical_count": len(canonical),
                        "invalid_status_count": sum(
                            row.get("status") != "COMPLETED"
                            for row in canonical
                        ),
                        "completed_v5_canonical_count": sum(
                            row.get("status") == "COMPLETED"
                            and (
                                row.get("summary_json") or {}
                            ).get("allocation_policy_version")
                            == health.ALLOCATION_POLICY_VERSION
                            for row in canonical
                        ),
                    }
                ]
            )
        if (
            "SELECT run_uid, trade_date, run_revision" in sql
            and "WHERE trade_date=:trade_date" in sql
        ):
            rows = [
                row
                for row in self.runs
                if row.get("trade_date") == params["trade_date"]
            ]
            rows.sort(
                key=lambda row: (
                    row.get("run_revision") or 0,
                    row.get("created_at") or "",
                    row.get("run_uid") or "",
                )
            )
            return _Result(rows)
        if "SELECT * FROM st_strategy_governance_run" in sql:
            return _Result(
                [
                    row
                    for row in self.runs
                    if row["build_commit_sha"] == params["build_commit_sha"]
                ]
            )
        if (
            "FROM st_strategy_governance_run WHERE status='COMPLETED'"
            in sql
        ):
            if (
                "LIMIT 1" not in sql
                and (
                    "SELECT run_uid, trade_date, run_revision, supersedes_run_uid"
                    in sql
                    or "SELECT run_uid, trade_date, market_state, status"
                    in sql
                )
            ):
                completed = [
                    row for row in self.runs
                    if row.get("status") == "COMPLETED"
                ]
                completed.sort(key=lambda row: row.get("run_uid") or "")
                return _Result(completed)
            completed = [
                row
                for row in self.runs
                if row.get("status") == "COMPLETED"
                and row.get("is_canonical") == 1
            ]
            completed.sort(
                key=lambda row: (
                    row.get("trade_date") or "",
                    row.get("run_revision") or 0,
                    row.get("finished_at") or "",
                    row.get("created_at") or "",
                    row.get("run_uid") or "",
                ),
                reverse=True,
            )
            return _Result(completed[:1])
        if "HAVING COUNT(*)<>3" in sql:
            return _Result([{"incomplete_count": 0}])
        if (
            "SELECT run_uid, strategy_key, strategy_version, trade_date"
            in sql
            and "FROM st_strategy_health_snapshot" in sql
        ):
            return _Result([
                {
                    **row,
                    "run_uid": run["run_uid"],
                    "profit_gate_passed": int(
                        row["evidence_json"]["gate"]["passed"] is True
                    ),
                    "recommended_status": (
                        "ACTIVE"
                        if row["strategy_key"] == "strategy_a"
                        else "SHADOW"
                    ),
                }
                for run in self.runs
                if run.get("status") == "COMPLETED"
                for row in _valid_strategy_router_rows()
            ])
        if (
            "LEFT JOIN st_strategy_registry" in sql
            and "st_strategy_allocation_snapshot" not in sql
        ):
            return _Result([{"version_mismatch_count": 0}])
        if (
            "SELECT strategy_key AS entity_key" in sql
            and "FROM st_strategy_health_snapshot" in sql
        ):
            return _Result(self.strategy_evidence_rows)
        if (
            "SELECT strategy_key, strategy_version, evidence_json" in sql
            and "FROM st_strategy_health_snapshot" in sql
        ):
            payload = {
                "overall_profit_gate_passed": True,
                "funding_gate_hash": STRATEGY_GATE_HASHES["strategy_a"],
            }
            return _Result(
                [
                    {
                        "strategy_key": "strategy_a",
                        "strategy_version": "v1",
                        "evidence_json": payload,
                        "window_days": _window,
                        "health_score": Decimal("80.0000"),
                    }
                    for _window in (20, 60, 120)
                ]
            )
        if (
            "FROM st_strategy_health_snapshot WHERE" in sql
            and "LEFT JOIN" not in sql
        ):
            return _Result(
                [
                    {
                        "row_count": 6,
                        "strategy_count": 2,
                        "window_count": 3,
                        "invalid_window_count": 0,
                        "invalid_gate_count": 0,
                        "invalid_recommended_status_count": 0,
                        "wrong_trade_date_count": 0,
                        "invalid_result_hash_count": 0,
                    }
                ]
            )
        if (
            "LEFT JOIN st_strategy_combination c" in sql
            and "st_strategy_allocation_snapshot" not in sql
        ):
            return _Result([{"version_mismatch_count": 0}])
        if (
            "SELECT run_uid, combination_key, combination_version"
            in sql
            and "FROM st_strategy_combination_health_snapshot" in sql
        ):
            return _Result([
                {
                    **row,
                    "run_uid": run["run_uid"],
                    "profit_gate_passed": int(
                        row["evidence_json"].get(
                            "overall_profit_gate_passed"
                        ) is True
                    ),
                    "recommended_status": "SHADOW",
                }
                for run in self.runs
                if run.get("status") == "COMPLETED"
                for row in _valid_combination_router_rows()
            ])
        if (
            "SELECT combination_key AS entity_key" in sql
            and "FROM st_strategy_combination_health_snapshot" in sql
        ):
            return _Result(self.combination_evidence_rows)
        if (
            "SELECT combination_key, combination_version, evidence_json"
            in sql
        ):
            return _Result(
                [
                    {
                        "combination_key": "combo_a",
                        "combination_version": "v1",
                        "ranking_score": Decimal("80.0000"),
                        "evidence_json": {
                            "overall_profit_gate_passed": False,
                            "funding_gate_hash": COMBINATION_GATE_HASH,
                        },
                    }
                ]
            )
        if "FROM st_strategy_combination_health_snapshot" in sql:
            return _Result(
                [
                    {
                        "row_count": 1,
                        "combination_count": 1,
                        "invalid_gate_count": 0,
                        "invalid_recommended_status_count": 0,
                        "wrong_trade_date_count": 0,
                        "invalid_result_hash_count": 0,
                    }
                ]
            )
        if (
            "SELECT run_uid, trade_date, pool_level, stock_code" in sql
            and "FROM st_strategy_pool_snapshot" in sql
        ):
            return _Result([
                {**row, "run_uid": run["run_uid"]}
                for run in self.runs
                if run.get("status") == "COMPLETED"
                for row in _fixture_pool_rows()
            ])
        if (
            "SELECT trade_date, pool_level, stock_code" in sql
            and "FROM st_strategy_pool_snapshot" in sql
        ):
            return _Result(_fixture_pool_rows())
        if "FROM st_strategy_pool_snapshot" in sql:
            return _Result(
                [
                    {
                        "row_count": 3,
                        "observation_count": 1,
                        "confirmation_count": 1,
                        "tradable_count": 1,
                        "invalid_pool_level_count": 0,
                        "wrong_trade_date_count": 0,
                    }
                ]
            )
        if (
            "FROM st_strategy_allocation_snapshot ORDER BY" in sql
        ):
            return _Result([
                {**row, "run_uid": run["run_uid"]}
                for run in self.runs
                if run.get("status") == "COMPLETED"
                for row in _fixture_allocation_rows()
            ])
        if "FROM st_strategy_allocation_snapshot a" in sql:
            return _Result(
                [
                    {
                        "target_type": "CASH",
                        "target_key": "cash",
                        "target_version": "",
                        "funding_gate_hash": "",
                        "market_state": MARKET_STATE,
                        "market_match_score": Decimal("0.0000"),
                        "router_decision_hash": "",
                        "lifecycle_status": "",
                        "lifecycle_status_label": "",
                        "lifecycle_risk_multiplier": Decimal("0.0000"),
                        "base_competitive_weight_pct": Decimal("0.0000"),
                        "simulated_weight_pct": Decimal("15.0000"),
                        "member_sleeves_json": [],
                        "member_sleeve_hash": "",
                        "cash_discount_bp": 0,
                        "real_order_authority": 0,
                        "strategy_registry_key": None,
                        "strategy_current_version": None,
                        "strategy_current_status": None,
                        "strategy_enabled": None,
                        "combination_registry_key": None,
                        "combination_current_version": None,
                        "combination_current_status": None,
                        "combination_enabled": None,
                    },
                    {
                        "target_type": "STRATEGY",
                        "target_key": "strategy_a",
                        "target_version": "v1",
                        "funding_gate_hash": STRATEGY_GATE_HASHES[
                            "strategy_a"
                        ],
                        "market_state": MARKET_STATE,
                        "market_match_score": Decimal("100.0000"),
                        "router_decision_hash": STRATEGY_ROUTES[
                            "strategy_a"
                        ]["router_decision_hash"],
                        "lifecycle_status": "ACTIVE",
                        "lifecycle_status_label": "正常运行",
                        "lifecycle_risk_multiplier": Decimal("1.0000"),
                        "base_competitive_weight_pct": Decimal("85.0000"),
                        "simulated_weight_pct": Decimal("85.0000"),
                        "member_sleeves_json": [],
                        "member_sleeve_hash": "",
                        "cash_discount_bp": 0,
                        "real_order_authority": 0,
                        "strategy_registry_key": "strategy_a",
                        "strategy_current_version": "v1",
                        "strategy_current_status": "ACTIVE",
                        "strategy_enabled": 1,
                        "combination_registry_key": None,
                        "combination_current_version": None,
                        "combination_current_status": None,
                        "combination_enabled": None,
                    },
                ]
            )
        if "FROM st_strategy_allocation_snapshot" in sql:
            return _Result(
                [
                    {
                        "row_count": 2,
                        "weight_sum": Decimal("100.0000"),
                        "forbidden_authority_count": 0,
                        "negative_weight_count": 0,
                        "invalid_target_type_count": 0,
                        "cash_rows": 1,
                    }
                ]
            )
        raise AssertionError(f"unexpected health SQL: {sql}")


def _completed_run():
    allocation_contract = _fixture_allocation_contract()
    return {
        "run_uid": "b" * 32,
        "trade_date": TRADE_DATE,
        "run_revision": 1,
        "supersedes_run_uid": "",
        "is_canonical": 1,
        "market_state": MARKET_STATE,
        "source_status": "fresh",
        "input_ready": 1,
        "input_hash": "c" * 64,
        "build_commit_sha": BUILD_SHA,
        "router_policy_version": health.ROUTER_POLICY_VERSION,
        "router_snapshot_hash": ROUTER_SNAPSHOT_HASH,
        "decision_hash": allocation_contract["decision_hash"],
        "result_json": allocation_contract["_result_json"],
        "result_hash": allocation_contract["_result_hash"],
        "status": "COMPLETED",
        "strategy_count": 2,
        "combination_count": 1,
        "observation_count": 1,
        "confirmation_count": 1,
        "tradable_count": 1,
        "allocation_count": 1,
        "summary_json": {
            "strategy_count": 2,
            "combination_count": 1,
            "allocation_count": 1,
            "strategy_route_eligible_count": 2,
            "combination_route_eligible_count": 1,
            "router_policy_version": health.ROUTER_POLICY_VERSION,
            "router_snapshot_hash": ROUTER_SNAPSHOT_HASH,
            "allocation_policy_version": allocation_contract[
                "allocation_policy_version"
            ],
            "trading_gate_passed": allocation_contract[
                "trading_gate_passed"
            ],
            "market_risk_cap_pct": allocation_contract[
                "market_risk_cap_pct"
            ],
            "allocation_candidate_count": allocation_contract[
                "allocation_candidate_count"
            ],
            "eligible_candidate_count": allocation_contract[
                "eligible_candidate_count"
            ],
            "candidate_set_hash": allocation_contract[
                "candidate_set_hash"
            ],
            "allocation_snapshot_hash": allocation_contract[
                "allocation_snapshot_hash"
            ],
            "paper_execution_plan_hash": allocation_contract[
                "paper_execution_plan_hash"
            ],
            "candidate_industry_snapshot_id": allocation_contract[
                "candidate_industry_snapshot_id"
            ],
            "candidate_industry_snapshot_hash": allocation_contract[
                "candidate_industry_snapshot_hash"
            ],
            "candidate_industry_snapshot_status": allocation_contract[
                "candidate_industry_snapshot_status"
            ],
            "paper_target_count": allocation_contract[
                "paper_target_count"
            ],
            "paper_invested_weight_pct": allocation_contract[
                "paper_invested_weight_pct"
            ],
            "pool_row_count": allocation_contract["pool_row_count"],
            "pool_snapshot_hash": allocation_contract[
                "pool_snapshot_hash"
            ],
            "automatic_transition_count": allocation_contract[
                "automatic_transition_count"
            ],
            "automatic_transition_plan_hash": allocation_contract[
                "automatic_transition_plan_hash"
            ],
            "decision_contract_version": allocation_contract[
                "decision_contract_version"
            ],
            "statistical_policy_hash": allocation_contract[
                "statistical_policy_hash"
            ],
            "statistical_inventory_compact_hash": allocation_contract[
                "statistical_inventory_compact_hash"
            ],
            "strategy_fdr_summary_hash": allocation_contract[
                "strategy_fdr_summary_hash"
            ],
            "combination_fdr_summary_hash": allocation_contract[
                "combination_fdr_summary_hash"
            ],
            "statistical_input_binding_hash": allocation_contract[
                "statistical_input_binding_hash"
            ],
            "funding_checkpoint_manifest_hash": allocation_contract[
                "funding_checkpoint_manifest_hash"
            ],
            "cash_weight_pct": allocation_contract["cash_weight_pct"],
        },
        "created_at": "2026-08-21 22:35:00",
        "finished_at": "2026-08-21 22:35:10",
    }


def test_independent_health_replays_stock_level_paper_plan_contract():
    run = _completed_run()

    passed, detail, plan_hash = (
        health._paper_execution_plan_contract_check(
            run,
            trade_date=TRADE_DATE,
            pool_rows=_fixture_pool_rows(),
            allocation_rows=_fixture_allocation_rows(),
            industry_bindings={
                "000001": _fixture_industry_contract()["binding"]
            },
        )
    )

    assert passed is True
    assert detail["errors"] == []
    assert plan_hash == run["summary_json"]["paper_execution_plan_hash"]


def test_independent_health_rejects_rehashed_single_stock_cap_forgery():
    run = _completed_run()
    result = json.loads(run["result_json"])
    plan = result["paper_execution_plan"]
    target = plan["targets"][0]
    target.update({
        "target_bp": 600,
        "target_weight_pct": 6.0,
        "new_buy_delta_bp": 600,
    })
    target["target_hash"] = health._canonical_digest({
        "schema": "probiga.governance-paper-target.v1",
        **{
            key: value for key, value in target.items()
            if key != "target_hash"
        },
    })
    plan.update({
        "requested_new_buy_turnover_bp": 600,
        "actual_new_buy_turnover_bp": 600,
        "invested_bp": 600,
        "cash_bp": 9400,
    })
    plan["plan_hash"] = health._canonical_digest({
        key: value for key, value in plan.items() if key != "plan_hash"
    })
    result["paper_execution_plan_hash"] = plan["plan_hash"]
    run["summary_json"].update({
        "paper_execution_plan_hash": plan["plan_hash"],
        "paper_invested_weight_pct": 6.0,
    })
    run["result_json"] = json.dumps(
        result, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    run["result_hash"] = hashlib.sha256(
        run["result_json"].encode("utf-8")
    ).hexdigest()

    passed, detail, _plan_hash = (
        health._paper_execution_plan_contract_check(
            run,
            trade_date=TRADE_DATE,
            pool_rows=_fixture_pool_rows(),
            allocation_rows=_fixture_allocation_rows(),
            industry_bindings={
                "000001": _fixture_industry_contract()["binding"]
            },
        )
    )

    assert passed is False
    assert any(
        error["reason"] == "paper target identity/cap/hash differs"
        for error in detail["errors"]
    )


def test_independent_health_replays_authoritative_multi_stock_industry_cap():
    from server.engine.strategy_industry_history import build_history_rows

    codes = [f"{index:06d}" for index in range(1, 6)]
    source_hash = health._canonical_digest({
        "schema": "test.multi-stock-industry-source.v1",
        "codes": codes,
    })
    snapshot_id, history_rows = build_history_rows(
        [
            {
                "industry_code": "801780",
                "industry_name": "银行",
                "industry_type": "L1",
                "stock_code": code,
            }
            for code in codes
        ],
        trade_date=TRADE_DATE,
        source="QMT_TEST",
        industry_hash=source_hash,
        captured_at=f"{TRADE_DATE}T15:05:00",
    )
    wrapper_payload = {
        "schema": health.INDUSTRY_SNAPSHOT_SCHEMA,
        "snapshot_id": snapshot_id,
        "trade_date": TRADE_DATE,
        "as_of_exclusive": "2026-08-22T00:00:00",
        "status": "COMPLETED",
        "requested_stock_codes": codes,
        "rows": history_rows,
        "reason": "测试多股票权威一级行业",
    }
    wrapper = {
        **wrapper_payload,
        "snapshot_hash": health._canonical_digest(wrapper_payload),
    }
    bindings = {
        row["stock_code"]: {
            "schema": health.INDUSTRY_BINDING_SCHEMA,
            "snapshot_id": snapshot_id,
            "snapshot_hash": wrapper["snapshot_hash"],
            "row_hash": row["row_hash"],
            "trade_date": TRADE_DATE,
            "as_of_exclusive": row["as_of_exclusive"],
            "stock_code": row["stock_code"],
            "industry_name": row["industry_name"],
            "industry_type": row["industry_type"],
            "source_system": row["source_system"],
            "source_fact_id": row["source_fact_id"],
            "source_effective_at": row["source_effective_at"],
            "source_etl_sync_at": row["source_etl_sync_at"],
        }
        for row in history_rows
    }
    run = _completed_run()
    result = json.loads(run["result_json"])
    plan = result["paper_execution_plan"]
    template = {
        key: value for key, value in plan["targets"][0].items()
        if key != "target_hash"
    }
    targets = []
    for code in codes:
        binding = bindings[code]
        payload = {
            **template,
            "stock_code": code,
            "stock_name": f"银行股{code}",
            "industry_name": binding["industry_name"],
            "industry_type": binding["industry_type"],
            "industry_snapshot_id": binding["snapshot_id"],
            "industry_snapshot_hash": binding["snapshot_hash"],
            "industry_row_hash": binding["row_hash"],
            "industry_source_system": binding["source_system"],
            "industry_source_fact_id": binding["source_fact_id"],
            "industry_binding": binding,
        }
        targets.append({
            **payload,
            "target_hash": health._canonical_digest({
                "schema": "probiga.governance-paper-target.v1",
                **payload,
            }),
        })
    plan.update({
        "industry_snapshot_id": snapshot_id,
        "industry_snapshot_hash": wrapper["snapshot_hash"],
        "industry_snapshot_status": "COMPLETED",
        "requested_new_buy_turnover_bp": 2500,
        "new_buy_turnover_multiplier": 1.0,
        "actual_new_buy_turnover_bp": 2500,
        "targets": targets,
        "target_count": 5,
        "invested_bp": 2500,
        "cash_bp": 7500,
    })
    plan["plan_hash"] = health._canonical_digest({
        key: value for key, value in plan.items() if key != "plan_hash"
    })
    result.update({
        "candidate_industry_snapshot": wrapper,
        "candidate_industry_snapshot_hash": wrapper["snapshot_hash"],
        "paper_execution_plan_hash": plan["plan_hash"],
    })
    result["summary"].update({
        "candidate_industry_snapshot_id": snapshot_id,
        "candidate_industry_snapshot_hash": wrapper["snapshot_hash"],
        "candidate_industry_snapshot_status": "COMPLETED",
        "paper_execution_plan_hash": plan["plan_hash"],
    })
    run["summary_json"].update({
        "candidate_industry_snapshot_id": snapshot_id,
        "candidate_industry_snapshot_hash": wrapper["snapshot_hash"],
        "candidate_industry_snapshot_status": "COMPLETED",
        "paper_execution_plan_hash": plan["plan_hash"],
        "paper_target_count": 5,
        "paper_invested_weight_pct": 25.0,
    })
    run["result_json"] = json.dumps(
        result, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    run["result_hash"] = hashlib.sha256(
        run["result_json"].encode("utf-8")
    ).hexdigest()

    passed, detail, _plan_hash = (
        health._paper_execution_plan_contract_check(
            run,
            trade_date=TRADE_DATE,
            pool_rows=None,
            allocation_rows=_fixture_allocation_rows(),
            industry_bindings=bindings,
        )
    )

    assert passed is False
    assert any(
        error["reason"] == "paper target aggregate limits differ"
        for error in detail["errors"]
    )


def _automatic_lifecycle_history_fixture():
    run = _completed_run()
    evidence = {
        "run_uid": run["run_uid"],
        "trade_date": TRADE_DATE,
        "funding_gate_hash": "9" * 64,
    }
    event_payload = {
        "entity_type": "STRATEGY",
        "entity_key": "strategy_a",
        "entity_version": "v1",
        "previous_status": "SHADOW",
        "next_status": "ACTIVE",
        "reason": "连续确认后自动开放",
        "evidence": evidence,
        "nonce": "1" * 32,
    }
    event = {
        "event_id": "2" * 32,
        "entity_type": "STRATEGY",
        "entity_key": "strategy_a",
        "entity_version": "v1",
        "previous_status": "SHADOW",
        "next_status": "ACTIVE",
        "reason": event_payload["reason"],
        "trigger_type": "AUTOMATIC_GATE",
        "evidence_json": evidence,
        "payload_json": event_payload,
        "event_hash": health._canonical_digest(event_payload),
        "operator_name": "scheduled_daily_governance",
        "occurred_at": run["finished_at"],
    }
    transition_entry = {
        "entity_type": "STRATEGY",
        "entity_key": "strategy_a",
        "entity_version": "v1",
        "previous_status": "SHADOW",
        "next_status": "ACTIVE",
        "reason": event_payload["reason"],
        "evidence": evidence,
    }
    run["summary_json"]["automatic_transition_count"] = 1
    run["summary_json"]["automatic_transition_plan_hash"] = (
        _fixture_transition_plan_hash([transition_entry])
    )

    transition_audit_payload = {
        "entity_type": "STRATEGY",
        "entity_key": "strategy_a",
        "action": "LIFECYCLE_TRANSITION",
        "reason": event_payload["reason"],
        "operator": "scheduled_daily_governance",
        "before": {"status": "SHADOW", "version": "v1", "enabled": True},
        "after": {"status": "ACTIVE", "version": "v1", "enabled": True},
        "evidence": evidence,
        "nonce": "3" * 32,
    }
    transition_audit = {
        "audit_id": "4" * 32,
        "entity_type": "STRATEGY",
        "entity_key": "strategy_a",
        "action": "LIFECYCLE_TRANSITION",
        "reason": event_payload["reason"],
        "operator_name": "scheduled_daily_governance",
        "before_json": transition_audit_payload["before"],
        "after_json": transition_audit_payload["after"],
        "evidence_json": evidence,
        "payload_json": transition_audit_payload,
        "audit_hash": health._canonical_digest(transition_audit_payload),
        "created_at": run["finished_at"],
    }
    default_engine = _GovernanceHealthEngine(runs=[run])
    run_audit = default_engine.execute(
        "FROM st_strategy_governance_audit", {}
    )._rows[0]
    return run, event, transition_audit, run_audit


def _unattributed_fifo_fixture():
    from server.trading_v3.forward_evidence import (
        EXIT_ALLOCATION_PROTOCOL,
        _exit_allocation_id,
    )

    account_id = "paper-main-v2"
    stock_code = "600000.SH"
    buy = {
        "fill_id": "buy-fill-1",
        "order_id": "buy-order-1",
        "account_id": account_id,
        "stock_code": stock_code,
        "side": "BUY",
        "quantity": 100,
        "price": Decimal("10.000000"),
        "gross_amount": Decimal("1000.000000"),
        "fee_amount": Decimal("1.000000"),
        "filled_at": "2026-08-20 10:00:00",
        "intent_id": "buy-intent-1",
        "decision_run_uid": "",
        "intent_reason_code": "LEGACY_TEST",
        "evidence_json": "{}",
    }
    sell = {
        "fill_id": "sell-fill-1",
        "order_id": "sell-order-1",
        "account_id": account_id,
        "stock_code": stock_code,
        "side": "SELL",
        "quantity": 40,
        "price": Decimal("11.000000"),
        "gross_amount": Decimal("440.000000"),
        "fee_amount": Decimal("0.500000"),
        "filled_at": "2026-08-21 14:00:00",
        "intent_id": "sell-intent-1",
        "decision_run_uid": "",
        "intent_reason_code": "PAPER_EXIT",
        "evidence_json": "{}",
    }
    allocation = {
        "allocation_id": _exit_allocation_id(
            sell["fill_id"],
            0,
            buy["fill_id"],
        ),
        "evidence_id": None,
        "attribution_status": "UNATTRIBUTED",
        "account_id": account_id,
        "stock_code": stock_code,
        "entry_fill_id": buy["fill_id"],
        "exit_fill_id": sell["fill_id"],
        "exit_order_id": sell["order_id"],
        "allocation_sequence": 0,
        "allocated_quantity": sell["quantity"],
        "allocated_gross_cny": sell["gross_amount"],
        "allocated_fee_cny": sell["fee_amount"],
        "exit_filled_at": sell["filled_at"],
        "allocation_protocol_version": EXIT_ALLOCATION_PROTOCOL,
        "bound_evidence_id": None,
        "evidence_account_id": None,
        "evidence_stock_code": None,
        "evidence_entry_fill_id": None,
    }
    return [buy, sell], allocation


def _fixed_trade_date(monkeypatch):
    monkeypatch.setattr(
        health,
        "_authoritative_trade_date",
        lambda _engine, _explicit="": (
            TRADE_DATE,
            "latest_completed_daily_kline",
        ),
    )
    monkeypatch.setattr(
        health,
        "_authoritative_session_window_attestation_check",
        lambda trade_date: (
            trade_date == TRADE_DATE,
            {
                "windows": {
                    str(value): {
                        key: _attested_session_window(value)[key]
                        for key in (
                            "start_date",
                            "end_date",
                            "session_count",
                            "session_hash",
                        )
                    }
                    for value in health.EXPECTED_WINDOWS
                }
            },
        ),
    )
    monkeypatch.setattr(
        health,
        "_qmt_row_attestation_binding_check",
        lambda trade_date, _detail: (
            trade_date == TRADE_DATE,
            {
                "table_exists": True,
                "protocol_version": (
                    health.QMT_PRECLOSE_ATTESTATION_PROTOCOL
                ),
                "source_pre_close_origin": "NATIVE_QMT",
            },
        ),
    )


def _blocked_after_historical_run(monkeypatch):
    monkeypatch.setattr(
        health,
        "_authoritative_trade_date",
        lambda _engine, _explicit="": (
            "2026-08-22",
            "latest_completed_daily_kline",
        ),
    )
    monkeypatch.setattr(
        health,
        "_authoritative_session_window_attestation_check",
        lambda trade_date: (
            trade_date == TRADE_DATE,
            {
                "windows": {
                    str(value): {
                        key: _attested_session_window(value)[key]
                        for key in (
                            "start_date",
                            "end_date",
                            "session_count",
                            "session_hash",
                        )
                    }
                    for value in health.EXPECTED_WINDOWS
                }
            },
        ),
    )
    monkeypatch.setattr(
        health,
        "_qmt_row_attestation_binding_check",
        lambda trade_date, _detail: (
            trade_date == TRADE_DATE,
            {
                "table_exists": True,
                "protocol_version": (
                    health.QMT_PRECLOSE_ATTESTATION_PROTOCOL
                ),
                "source_pre_close_origin": "NATIVE_QMT",
            },
        ),
    )


def test_health_accepts_one_complete_exact_build_date_run(monkeypatch):
    _fixed_trade_date(monkeypatch)
    result = health.collect_governance_health(
        _GovernanceHealthEngine(runs=[_completed_run()]),
        expected_build_sha=BUILD_SHA,
    )
    assert result["status"] == "PASS"
    assert result["run_disposition"] == "completed"
    assert all(check["passed"] for check in result["checks"])
    assert {check["name"] for check in result["checks"]} == set(
        health.governance_health_required_check_names("completed")
    )


def test_industry_history_replay_accepts_non_l1_source_rows_but_selects_l1():
    engine = _GovernanceHealthEngine()

    passed, detail, bindings, snapshots = (
        health._strategy_industry_history_contract_check(
            _Connection(engine)
        )
    )

    assert passed is True
    assert detail["qmt_member_count"] == 3
    assert detail["replayed_binding_count"] == 2
    assert set(bindings) == {
        (TRADE_DATE, "000001"), (TRADE_DATE, "600000")
    }
    assert bindings[(TRADE_DATE, "000001")]["industry_type"] == "L1"
    assert snapshots[TRADE_DATE]["row_count"] == 2


def test_industry_history_replay_accepts_audited_one_session_fallback():
    from server.engine.strategy_industry_history import build_history_rows

    target = "2026-08-28"
    source_date = "2026-08-27"
    source = "QMT_TEST"
    captured_at = f"{source_date}T15:12:00"
    members = [{
        "snapshot_date": source_date,
        "source": source,
        "industry_code": "801780",
        "industry_name": "银行",
        "industry_type": "L1",
        "stock_code": "000001",
        "short_name": "平安银行",
        "quality_status": health.QMT_VALIDATED,
        "captured_at": captured_at,
    }]
    source_hash = health._canonical_qmt_industry_hash(members)
    _snapshot_id, history_rows = build_history_rows(
        members,
        trade_date=target,
        source=source,
        industry_hash=source_hash,
        captured_at=captured_at,
        source_snapshot_date=source_date,
        capture_mode="qmt_close_full_refresh",
        fallback_reason="QMT_HISTORICAL_SECTOR_API_UNAVAILABLE",
    )
    contract = {
        "previous_trade_date": source_date,
        "history_rows": history_rows,
        "members": members,
        "run": {
            "snapshot_date": source_date,
            "source": source,
            "quality_status": health.QMT_VALIDATED,
            "capture_mode": "qmt_close_full_refresh",
            "industry_count": 1,
            "industry_relation_count": 1,
            "industry_hash": source_hash,
            "captured_at": captured_at,
        },
    }

    passed, detail, bindings, snapshots = (
        health._strategy_industry_history_contract_check(
            _Connection(_GovernanceHealthEngine(industry_contract=contract))
        )
    )

    assert passed is True
    assert detail["invalid_count"] == 0
    assert set(bindings) == {(target, "000001")}
    assert snapshots[target]["source_snapshot_date"] == source_date
    assert snapshots[target]["capture_mode"] == "qmt_close_full_refresh"
    assert snapshots[target]["fallback_reason"] == (
        "QMT_HISTORICAL_SECTOR_API_UNAVAILABLE"
    )


def test_industry_history_cutover_isolates_legacy_but_replays_production(
    monkeypatch,
):
    from server.engine.strategy_industry_history import build_history_rows

    cutover = "2026-08-28"
    source_date = "2026-08-27"
    source = "QMT_TEST"
    captured_at = f"{source_date}T15:12:00"
    members = [{
        "snapshot_date": source_date,
        "source": source,
        "industry_code": "801780",
        "industry_name": "银行",
        "industry_type": "L1",
        "stock_code": "000001",
        "short_name": "平安银行",
        "quality_status": health.QMT_VALIDATED,
        "captured_at": captured_at,
    }]
    source_hash = health._canonical_qmt_industry_hash(members)
    _snapshot_id, production_rows = build_history_rows(
        members,
        trade_date=cutover,
        source=source,
        industry_hash=source_hash,
        captured_at=captured_at,
        source_snapshot_date=source_date,
        capture_mode="qmt_close_full_refresh",
        fallback_reason="QMT_HISTORICAL_SECTOR_API_UNAVAILABLE",
    )
    legacy_rows = []
    for legacy_date, effective_date in (
        ("2026-08-24", "2026-08-20"),
        ("2026-08-25", "2026-08-25"),
    ):
        legacy = deepcopy(production_rows[0])
        legacy.update({
            "trade_date": legacy_date,
            "source_effective_at": f"{effective_date}T15:21:40",
            "source_etl_sync_at": f"{effective_date}T15:21:40",
            # Deliberately leave hashes inconsistent.  Isolation is by the
            # immutable partition date, not by accepting legacy provenance.
        })
        legacy_rows.append(legacy)
    run = {
        "snapshot_date": source_date,
        "source": source,
        "quality_status": health.QMT_VALIDATED,
        "capture_mode": "qmt_close_full_refresh",
        "industry_count": 1,
        "industry_relation_count": 1,
        "industry_hash": source_hash,
        "captured_at": captured_at,
    }
    contract = {
        "previous_trade_date": source_date,
        "history_rows": [*legacy_rows, *production_rows],
        "members": members,
        "all_members": members,
        "run": run,
        "runs": [run],
    }
    monkeypatch.setattr(
        health,
        "STRATEGY_INDUSTRY_HISTORY_PRODUCTION_CUTOVER_DATE",
        cutover,
    )

    passed, detail, bindings, snapshots = (
        health._strategy_industry_history_contract_check(
            _Connection(_GovernanceHealthEngine(industry_contract=contract))
        )
    )

    assert passed is True
    assert detail["legacy_isolation_status"] == "LEGACY_RESEARCH_ONLY"
    assert detail["legacy_isolated"] is True
    assert detail["legacy_isolated_trade_dates"] == [
        "2026-08-24", "2026-08-25",
    ]
    assert detail["legacy_isolated_row_count"] == 2
    assert detail["production_history_row_count"] == 1
    assert set(bindings) == {(cutover, "000001")}
    assert set(snapshots) == {cutover}

    broken_contract = deepcopy(contract)
    broken_contract["history_rows"][-1]["row_hash"] = "f" * 64
    broken_passed, broken_detail, _bindings, _snapshots = (
        health._strategy_industry_history_contract_check(
            _Connection(_GovernanceHealthEngine(
                industry_contract=broken_contract,
            ))
        )
    )
    assert broken_passed is False
    assert broken_detail["invalid_count"] == 1


@pytest.mark.parametrize(
    "target_run",
    (
        {
            "snapshot_date": "2026-08-28",
            "source": "QMT_TEST",
            "quality_status": "PARTIAL",
            "capture_mode": "qmt_close_full_refresh",
            "captured_at": "2026-08-28T15:12:00",
        },
        {
            "snapshot_date": "2026-08-28",
            "source": "QMT_LATE",
            "quality_status": "QMT_VALIDATED",
            "capture_mode": "qmt_close_full_refresh",
            "captured_at": "2026-08-29T00:01:00",
        },
    ),
)
def test_industry_health_rejects_fallback_when_any_target_run_exists(
    monkeypatch, target_run,
):
    from server.engine.strategy_industry_history import build_history_rows

    target = "2026-08-28"
    source_date = "2026-08-27"
    source = "QMT_TEST"
    captured_at = f"{source_date}T15:12:00"
    members = [{
        "snapshot_date": source_date,
        "source": source,
        "industry_code": "801780",
        "industry_name": "银行",
        "industry_type": "L1",
        "stock_code": "000001",
        "short_name": "平安银行",
        "quality_status": health.QMT_VALIDATED,
        "captured_at": captured_at,
    }]
    source_hash = health._canonical_qmt_industry_hash(members)
    _snapshot_id, history_rows = build_history_rows(
        members,
        trade_date=target,
        source=source,
        industry_hash=source_hash,
        captured_at=captured_at,
        source_snapshot_date=source_date,
        fallback_reason="QMT_HISTORICAL_SECTOR_API_UNAVAILABLE",
    )
    run = {
        "snapshot_date": source_date,
        "source": source,
        "quality_status": health.QMT_VALIDATED,
        "capture_mode": "qmt_close_full_refresh",
        "industry_count": 1,
        "industry_relation_count": 1,
        "industry_hash": source_hash,
        "captured_at": captured_at,
    }
    contract = {
        "previous_trade_date": source_date,
        "history_rows": history_rows,
        "members": members,
        "run": run,
        "target_runs": [target_run],
    }
    monkeypatch.setattr(
        health,
        "STRATEGY_INDUSTRY_HISTORY_PRODUCTION_CUTOVER_DATE",
        target,
    )

    passed, detail, bindings, snapshots = (
        health._strategy_industry_history_contract_check(
            _Connection(_GovernanceHealthEngine(industry_contract=contract))
        )
    )

    assert passed is False
    assert detail["invalid_count"] == 1
    assert bindings == {}
    assert snapshots == {}


def test_downstream_snapshot_history_isolates_pre_cutover_only(monkeypatch):
    legacy_uid = "1" * 32

    def legacy_rows(_connection, sql, _params=None):
        if "FROM st_strategy_governance_run" in sql:
            return [{
                "run_uid": legacy_uid,
                "trade_date": "2026-08-25",
                "market_state": "legacy",
                "status": "COMPLETED",
                "summary_json": "not-canonical-legacy",
            }]
        if "FROM st_strategy_health_snapshot" in sql:
            return [{"run_uid": legacy_uid}]
        if "FROM st_strategy_combination_health_snapshot" in sql:
            return [{"run_uid": legacy_uid}]
        if "FROM st_strategy_pool_snapshot" in sql:
            return [{"run_uid": legacy_uid}]
        if "FROM st_strategy_allocation_snapshot" in sql:
            return [{"run_uid": legacy_uid}]
        raise AssertionError(sql)

    monkeypatch.setattr(
        health,
        "STRATEGY_INDUSTRY_HISTORY_PRODUCTION_CUTOVER_DATE",
        "2026-08-28",
    )
    monkeypatch.setattr(health, "_rows", legacy_rows)

    passed, detail = health._all_governance_snapshot_history_check(object())

    assert passed is True
    assert detail["legacy_isolation_status"] == "LEGACY_RESEARCH_ONLY"
    assert detail["legacy_completed_run_count"] == 1
    assert detail["legacy_strategy_health_row_count"] == 1
    assert detail["legacy_combination_health_row_count"] == 1
    assert detail["legacy_pool_row_count"] == 1
    assert detail["legacy_allocation_row_count"] == 1
    assert detail["completed_run_count"] == 0
    assert detail["invalid_count"] == 0

    production_uid = "2" * 32

    def production_rows(_connection, sql, _params=None):
        if "FROM st_strategy_governance_run" in sql:
            return [{
                "run_uid": production_uid,
                "trade_date": "2026-08-28",
                "market_state": "trend_bullish",
                "status": "COMPLETED",
                "summary_json": {},
            }]
        return []

    monkeypatch.setattr(health, "_rows", production_rows)
    strict_passed, strict_detail = (
        health._all_governance_snapshot_history_check(object())
    )
    assert strict_passed is False
    assert strict_detail["legacy_completed_run_count"] == 0
    assert strict_detail["completed_run_count"] == 1
    assert strict_detail["invalid_count"] >= 1


def test_empty_candidate_industry_wrapper_is_reproducible_not_missing():
    from server.engine import strategy_governance as governance_module

    wrapper = governance_module._frozen_industry_snapshot(TRADE_DATE, [])
    industry = _fixture_industry_contract()
    history_bindings = {
        (TRADE_DATE, row["stock_code"]): row
        for row in industry["history_rows"]
    }
    result = {
        "candidate_industry_snapshot": wrapper,
        "candidate_industry_snapshot_hash": wrapper["snapshot_hash"],
        "summary": {
            "candidate_industry_snapshot_id": "",
            "candidate_industry_snapshot_hash": wrapper["snapshot_hash"],
            "candidate_industry_snapshot_status": "INCOMPLETE",
        },
    }

    passed, detail, bindings = health._candidate_industry_snapshot_contract(
        result,
        trade_date=TRADE_DATE,
        history_bindings=history_bindings,
        history_snapshots={
            TRADE_DATE: {
                "snapshot_id": industry["history_rows"][0]["snapshot_id"]
            }
        },
    )

    assert passed is True
    assert detail["requested_count"] == 0
    assert bindings == {}
    assert wrapper["reason"] == "当日候选为空，无需行业绑定；模拟资金保持现金"


@pytest.mark.parametrize(
    "drift_kind",
    ("tampered", "missing", "wrong_date", "stale", "multi_l1"),
)
def test_industry_history_full_replay_rejects_every_source_drift(drift_kind):
    contract = _fixture_industry_contract()
    if drift_kind == "tampered":
        row = contract["history_rows"][0]
        row["industry_name"] = "电子"
        row["row_hash"] = health._canonical_digest({
            key: value for key, value in row.items() if key != "row_hash"
        })
    elif drift_kind == "missing":
        contract["history_rows"] = contract["history_rows"][:-1]
    elif drift_kind == "wrong_date":
        row = contract["history_rows"][0]
        row["trade_date"] = "2026-08-20"
        row["as_of_exclusive"] = "2026-08-21T00:00:00"
        row["row_hash"] = health._canonical_digest({
            key: value for key, value in row.items() if key != "row_hash"
        })
    elif drift_kind == "stale":
        contract["run"]["captured_at"] = f"{TRADE_DATE}T14:59:59"
        for member in contract["members"]:
            member["captured_at"] = f"{TRADE_DATE}T14:59:59"
    else:
        duplicate = deepcopy(contract["members"][0])
        duplicate.update({
            "industry_code": "801080",
            "industry_name": "电子",
            "industry_type": "L1",
        })
        contract["members"].append(duplicate)
        contract["run"]["industry_relation_count"] = len(
            contract["members"]
        )
        contract["run"]["industry_count"] = len({
            row["industry_code"] for row in contract["members"]
        })
        contract["run"]["industry_hash"] = (
            health._canonical_qmt_industry_hash(contract["members"])
        )
    engine = _GovernanceHealthEngine(industry_contract=contract)

    passed, detail, _bindings, _snapshots = (
        health._strategy_industry_history_contract_check(
            _Connection(engine)
        )
    )

    assert passed is False
    assert detail["invalid_count"] >= 1


def test_allow_input_not_ready_matches_strict_for_complete_exact_run(
    monkeypatch,
):
    _fixed_trade_date(monkeypatch)
    strict_result = health.collect_governance_health(
        _GovernanceHealthEngine(runs=[_completed_run()]),
        expected_build_sha=BUILD_SHA,
    )
    allowed_result = health.collect_governance_health(
        _GovernanceHealthEngine(runs=[_completed_run()]),
        expected_build_sha=BUILD_SHA,
        allow_input_not_ready=True,
    )

    assert allowed_result == strict_result
    assert allowed_result["status"] == "PASS"
    assert allowed_result["run_disposition"] == "completed"
    assert not any(check["waived"] for check in allowed_result["checks"])


def test_automatic_lifecycle_history_is_exactly_bound_to_run_and_audit():
    run, event, transition_audit, run_audit = (
        _automatic_lifecycle_history_fixture()
    )
    engine = _GovernanceHealthEngine(
        runs=[run],
        lifecycle_rows=[event],
        audit_rows=[transition_audit, run_audit],
    )

    passed, detail, hashes = (
        health._immutable_lifecycle_and_audit_history_check(
            _Connection(engine)
        )
    )

    assert passed is True
    assert detail["invalid_count"] == 0
    assert hashes[run["run_uid"]] == run["summary_json"][
        "automatic_transition_plan_hash"
    ]


@pytest.mark.parametrize(
    "drift_kind",
    ["missing_audit", "duplicate_event", "tampered_event", "wrong_run_uid"],
)
def test_automatic_lifecycle_history_rejects_every_binding_drift(
    drift_kind,
):
    run, event, transition_audit, run_audit = (
        _automatic_lifecycle_history_fixture()
    )
    events = [event]
    audits = [transition_audit, run_audit]
    if drift_kind == "missing_audit":
        audits = [run_audit]
    elif drift_kind == "duplicate_event":
        duplicate = deepcopy(event)
        duplicate["event_id"] = "5" * 32
        duplicate["payload_json"] = deepcopy(event["payload_json"])
        duplicate["payload_json"]["nonce"] = "6" * 32
        duplicate["event_hash"] = health._canonical_digest(
            duplicate["payload_json"]
        )
        events.append(duplicate)
    elif drift_kind == "tampered_event":
        events[0] = deepcopy(event)
        events[0]["reason"] = "tampered"
    else:
        events[0] = deepcopy(event)
        events[0]["evidence_json"] = deepcopy(event["evidence_json"])
        events[0]["evidence_json"]["run_uid"] = "f" * 32
        events[0]["payload_json"] = deepcopy(event["payload_json"])
        events[0]["payload_json"]["evidence"] = events[0]["evidence_json"]
        events[0]["event_hash"] = health._canonical_digest(
            events[0]["payload_json"]
        )

    passed, detail, _hashes = (
        health._immutable_lifecycle_and_audit_history_check(
            _Connection(_GovernanceHealthEngine(
                runs=[run], lifecycle_rows=events, audit_rows=audits,
            ))
        )
    )

    assert passed is False
    assert detail["invalid_count"] > 0


def test_registry_lifecycle_projection_matches_exact_event_replay():
    registry_rows, event_rows = _lifecycle_projection_fixture()
    engine = _GovernanceHealthEngine(
        projection_registry_rows=registry_rows,
        projection_event_rows=event_rows,
    )

    passed, detail = health._lifecycle_registry_projection_check(
        _Connection(engine)
    )

    assert passed is True
    assert detail["registry_count"] == 3
    assert detail["projected_count"] == 3
    assert detail["invalid_count"] == 0


@pytest.mark.parametrize(
    "drift_kind",
    ("entity_version", "current_status", "status_reason", "event_hash"),
)
def test_registry_lifecycle_projection_rejects_pointer_or_event_drift(
    drift_kind,
):
    registry_rows, event_rows = _lifecycle_projection_fixture()
    if drift_kind == "event_hash":
        event_rows[-1]["event_hash"] = "0" * 64
    else:
        registry_rows[0][drift_kind] = {
            "entity_version": "v2",
            "current_status": "REDUCE",
            "status_reason": "rewritten reason",
        }[drift_kind]
    engine = _GovernanceHealthEngine(
        projection_registry_rows=registry_rows,
        projection_event_rows=event_rows,
    )

    passed, detail = health._lifecycle_registry_projection_check(
        _Connection(engine)
    )

    assert passed is False
    assert detail["invalid_count"] > 0


def test_registry_lifecycle_projection_makes_retired_terminal_per_version():
    registry_rows, event_rows = _lifecycle_projection_fixture()
    strategy = registry_rows[0]
    strategy.update({
        "current_status": "ACTIVE",
        "status_reason": "illegal revival",
    })
    previous = event_rows[-1]
    for offset, (old_status, new_status, reason) in enumerate((
        ("ACTIVE", "RETIRED", "retired"),
        ("RETIRED", "ACTIVE", "illegal revival"),
    ), 1):
        payload = {
            "entity_type": "STRATEGY",
            "entity_key": "strategy_a",
            "entity_version": "v1",
            "previous_status": old_status,
            "next_status": new_status,
            "reason": reason,
            "evidence": {},
            "nonce": format(100 + offset, "032x"),
        }
        previous = {
            "event_id": format(100 + offset, "032x"),
            "entity_type": "STRATEGY",
            "entity_key": "strategy_a",
            "entity_version": "v1",
            "previous_status": old_status,
            "next_status": new_status,
            "reason": reason,
            "trigger_type": "AUTOMATIC_GATE",
            "payload_json": payload,
            "event_hash": health._canonical_digest(payload),
            "occurred_at": f"{TRADE_DATE} 10:00:0{offset}",
        }
        event_rows.append(previous)
    engine = _GovernanceHealthEngine(
        projection_registry_rows=registry_rows,
        projection_event_rows=event_rows,
    )

    passed, detail = health._lifecycle_registry_projection_check(
        _Connection(engine)
    )

    assert passed is False
    assert any(
        "does not continue" in error["reason"]
        for error in detail["errors"]
    )


def test_funding_manifest_partition_and_persistence_replay_is_exact():
    fixture = _funding_manifest_persistence_fixture()
    engine = _FundingManifestEngine(fixture)

    passed, detail = _REAL_FUNDING_MANIFEST_PERSISTENCE_CHECK(
        _Connection(engine),
        run=fixture["run"],
        result=fixture["result"],
        trade_date=TRADE_DATE,
    )

    assert passed is True
    assert detail["current_entity_count"] == 2
    assert detail["checkpoint_count"] == 1
    assert detail["daily_fact_count"] == 1
    assert detail["invalid_count"] == 0
    calls = {next(
        fragment for fragment in (
            "funding_registry", "referenced_previous_hash",
            f"SELECT * FROM {health.FUNDING_DAILY_FACT_TABLE_NAME}",
            "ANCHOR_FUNDING_CHECKPOINT_MANIFEST",
        ) if fragment in sql
    ): (sql, params) for sql, params in engine.calls}
    assert calls["funding_registry"][1]["registry_limit"] == 3
    assert calls["referenced_previous_hash"][1]["checkpoint_limit"] == 2
    assert calls[
        f"SELECT * FROM {health.FUNDING_DAILY_FACT_TABLE_NAME}"
    ][1]["fact_limit"] == 2
    assert "LIMIT 2" in calls["ANCHOR_FUNDING_CHECKPOINT_MANIFEST"][0]


def test_funding_health_has_no_fixed_dynamic_entity_count_ceiling():
    fixture = _funding_manifest_persistence_fixture()
    result = fixture["result"]
    added_ineligible = []
    for index in range(1000):
        key = f"dynamic_strategy_{index:04d}"
        ineligible = {
            "entity_type": "STRATEGY",
            "entity_key": key,
            "entity_version": "v1",
            "reason_code": "NO_VERIFIED_INTERNAL_LEDGER",
            "reason": "fixture has no independently verified forward ledger",
        }
        added_ineligible.append(ineligible)
        result["strategies"].append({
            "strategy_key": key,
            "current_version": "v1",
            "funding_checkpoint_ready": False,
            "funding_checkpoint_ref": None,
            "funding_manifest_ineligible": ineligible,
        })
        fixture["registry_rows"].append({
            "entity_type": "STRATEGY",
            "entity_key": key,
            "entity_version": "v1",
        })

    current_entities = sorted(
        fixture["registry_rows"],
        key=lambda row: (
            row["entity_type"], row["entity_key"], row["entity_version"],
        ),
    )
    checkpoint_entities = [{
        "entity_type": "STRATEGY",
        "entity_key": "strategy_a",
        "entity_version": "v1",
    }]
    ineligible_rows = [
        result["combinations"][0]["funding_manifest_ineligible"],
        *added_ineligible,
    ]
    ineligible_entities = sorted(
        [{
            "entity_type": row["entity_type"],
            "entity_key": row["entity_key"],
            "entity_version": row["entity_version"],
        } for row in ineligible_rows],
        key=lambda row: (
            row["entity_type"], row["entity_key"], row["entity_version"],
        ),
    )
    coverage = {
        "current_entity_count": len(current_entities),
        "funding_ready_count": 1,
        "eligible_count": 1,
        "strategy_checkpoint_count": 1,
        "combination_recipe_count": 0,
        "checkpointed_count": 1,
        "ineligible_count": len(ineligible_entities),
        "current_entity_set_hash": health._funding_entity_set_hash(
            current_entities
        ),
        "checkpointed_set_hash": health._funding_entity_set_hash(
            checkpoint_entities
        ),
        "combination_recipe_set_hash": health._funding_entity_set_hash([]),
        "funding_ready_set_hash": health._funding_entity_set_hash(
            checkpoint_entities
        ),
        "ineligible_set_hash": health._funding_entity_set_hash(
            ineligible_entities
        ),
        "eligible_persistence_coverage_pct": 100.0,
    }
    old_manifest = result["funding_checkpoint_manifest"]
    manifest_payload = {
        key: deepcopy(value)
        for key, value in old_manifest.items()
        if key != "manifest_hash"
    }
    manifest_payload.update({
        "coverage": coverage,
        "ineligible_root": health._funding_manifest_batch_root(
            ineligible_rows, kind="INELIGIBLE"
        ),
        "ineligible_reason_code_counts": {
            "COMBINATION_RECIPE_NOT_MATERIALIZED": 1,
            "NO_VERIFIED_INTERNAL_LEDGER": 1000,
        },
    })
    manifest = {
        **manifest_payload,
        "manifest_hash": health._funding_canonical_hash(manifest_payload),
    }
    result["funding_checkpoint_manifest"] = manifest
    result["summary"].update({
        "funding_checkpoint_manifest_hash": manifest["manifest_hash"],
        "funding_checkpoint_ineligible_count": len(ineligible_entities),
    })
    result_json = health._funding_canonical_json(result)
    result_hash = hashlib.sha256(result_json.encode("utf-8")).hexdigest()
    fixture["run"].update({
        "result_json": result_json,
        "result_hash": result_hash,
    })

    audit = fixture["audit_rows"][0]
    evidence = deepcopy(audit["evidence_json"])
    evidence.update({
        "canonical_result_hash": result_hash,
        "checkpoint_manifest_hash": manifest["manifest_hash"],
        "coverage": coverage,
        "checkpoint_root": manifest["checkpoint_root"],
        "combination_recipe_root": manifest["combination_recipe_root"],
        "ineligible_root": manifest["ineligible_root"],
    })
    after = {
        "run_uid": result["run_uid"],
        "manifest_hash": manifest["manifest_hash"],
        "checkpoint_count": 1,
    }
    audit_payload = {
        "entity_type": "SYSTEM",
        "entity_key": "strategy_funding_checkpoint_manifest",
        "action": "ANCHOR_FUNDING_CHECKPOINT_MANIFEST",
        "reason": audit["reason"],
        "operator": audit["operator_name"],
        "before": {},
        "after": after,
        "evidence": evidence,
        "nonce": "e" * 32,
    }
    audit_hash = health._funding_canonical_hash(audit_payload)
    audit.update({
        "after_json": after,
        "evidence_json": evidence,
        "payload_json": health._funding_canonical_json(audit_payload),
        "audit_hash": audit_hash,
    })
    for row in fixture["checkpoint_rows"] + fixture["fact_rows"]:
        row["canonical_result_hash"] = result_hash
        row["anchor_audit_hash"] = audit_hash

    engine = _FundingManifestEngine(fixture)
    passed, detail = _REAL_FUNDING_MANIFEST_PERSISTENCE_CHECK(
        _Connection(engine),
        run=fixture["run"],
        result=result,
        trade_date=TRADE_DATE,
    )

    assert passed is True
    assert detail["current_entity_count"] == 1002
    registry_call = next(
        params for sql, params in engine.calls if "funding_registry" in sql
    )
    assert registry_call["registry_limit"] == 1003


@pytest.mark.parametrize(
    ("row_count", "batch_counts"),
    [
        (0, []),
        (1, [1]),
        (100, [100]),
        (101, [100, 1]),
        (750, [100, 100, 100, 100, 100, 100, 100, 50]),
        (1001, [100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 1]),
    ],
)
def test_compact_funding_manifest_root_matches_core_at_scale(
    row_count,
    batch_counts,
):
    from server.engine import strategy_governance as governance

    rows = [{
        "entity_type": "STRATEGY",
        "entity_key": f"strategy_{index:04d}",
        "entity_version": "v1",
        "reason_code": "NO_VERIFIED_INTERNAL_LEDGER",
        "reason": "fixture",
    } for index in range(row_count)]

    root = health._funding_manifest_batch_root(rows, kind="INELIGIBLE")

    assert root == governance._funding_manifest_batch_root(
        rows, kind="INELIGIBLE"
    )
    assert root["count"] == row_count
    assert [batch["count"] for batch in root["batches"]] == batch_counts
    assert len(health._funding_canonical_json(root).encode("utf-8")) < 8192


def test_combination_recipe_root_uses_member_checkpoints_without_cash_facts():
    from server.engine import strategy_governance as governance

    fixture = _funding_manifest_persistence_fixture()
    checkpoint = fixture["result"]["strategies"][0]["funding_checkpoint_ref"]
    members = [{
        "strategy_key": checkpoint["strategy_key"],
        "strategy_version": checkpoint["strategy_version"],
        "weight": 1.0,
        "checkpoint_id": checkpoint["checkpoint_id"],
        "account_id": checkpoint["account_id"],
        "checkpoint_hash": checkpoint["checkpoint_hash"],
        "chain_hash": checkpoint["chain_hash"],
        "history_fact_set_hash": checkpoint["history_fact_set_hash"],
        "checkpoint_trade_date": TRADE_DATE,
    }]
    risk_binding_payload = {
        "schema": "probiga.combination-drift-risk-binding.v2",
        "window_days": 60,
        "risk_path_hash": "6" * 64,
        "constraint_evaluation_hash": "7" * 64,
        "constraint_passed": True,
        "peak_member_weight": 1.0,
        "current_member_weight": 1.0,
        "peak_pairwise_stock_overlap_pct": 0.0,
        "current_pairwise_stock_overlap_pct": 0.0,
        "peak_industry_weight_pct": 100.0,
        "current_industry_weight_pct": 100.0,
        "industry_snapshot_path_hash": "8" * 64,
        "industry_trade_dates_hash": "9" * 64,
        "industry_stock_code_sets_hash": "a" * 64,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    risk_binding = {
        **risk_binding_payload,
        "binding_hash": health._funding_canonical_hash(
            risk_binding_payload
        ),
    }
    recipe_payload = {
        "schema": "probiga.combination-member-fact-recipe.v1",
        "combination_key": "combo_a",
        "combination_version": "v1",
        "trade_date": TRADE_DATE,
        "members": members,
        "risk_constraint_binding": risk_binding,
        "cash_fact_materialized": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    recipe_hash = health._funding_canonical_hash(recipe_payload)
    pre_recipe_hash = "9" * 64
    gate_payload = {
        "schema": "probiga.combination-recipe-funding-gate.v1",
        "combination_key": "combo_a",
        "combination_version": "v1",
        "trade_date": TRADE_DATE,
        "pre_recipe_funding_gate_hash": pre_recipe_hash,
        "recipe_hash": recipe_hash,
        "recipe_ready": True,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    recipe_gate_hash = health._funding_canonical_hash(gate_payload)
    decision = _fixture_statistical_decision(
        entity_type="COMBINATION", entity_key="combo_a",
    )
    confirmation = _fixture_confirmation(passed=True)
    final_gate_hash = _final_funding_hash(
        entity_type="COMBINATION",
        entity_key="combo_a",
        pre_gate_hash=recipe_gate_hash,
        decision=decision,
        confirmation=confirmation,
        projected_status="ACTIVE",
        paper_eligible=True,
    )
    combination = {
        "combination_key": "combo_a",
        "current_version": "v1",
        "current_status": "ACTIVE",
        "projected_status": "ACTIVE",
        "paper_allocation_eligible": True,
        "statistical_family_decision": decision,
        "confirmation_guard": confirmation,
        "pre_confirmation_funding_gate_hash": recipe_gate_hash,
        "funding_gate_hash": final_gate_hash,
        "combination_recipe_ref": {
            **recipe_payload,
            "member_fact_sets_ready": True,
            "pre_recipe_funding_gate_hash": pre_recipe_hash,
            "recipe_hash": recipe_hash,
            "recipe_gate_hash": recipe_gate_hash,
        },
    }

    entry = health._combination_recipe_manifest_entry(
        combination, trade_date=TRADE_DATE,
    )

    assert entry == governance._combination_recipe_manifest_entry(
        combination, trade_date=TRADE_DATE,
    )
    assert entry["cash_fact_materialized"] is False
    assert health._funding_manifest_batch_root(
        [entry], kind="COMBINATION_RECIPE"
    )["count"] == 1
    tampered = deepcopy(combination)
    tampered["combination_recipe_ref"]["cash_fact_materialized"] = True
    with pytest.raises(ValueError):
        health._combination_recipe_manifest_entry(
            tampered, trade_date=TRADE_DATE,
        )


@pytest.mark.parametrize(
    "drift_kind",
    ("partition", "checkpoint", "fact", "audit"),
)
def test_funding_manifest_replay_rejects_every_anchor_drift(drift_kind):
    fixture = _funding_manifest_persistence_fixture()
    if drift_kind == "partition":
        fixture["registry_rows"][0]["entity_version"] = "v2"
    elif drift_kind == "checkpoint":
        fixture["checkpoint_rows"][0]["chain_hash"] = "0" * 64
    elif drift_kind == "fact":
        fixture["fact_rows"][0]["fact_hash"] = "0" * 64
    else:
        fixture["audit_rows"][0]["audit_hash"] = "0" * 64

    passed, detail = _REAL_FUNDING_MANIFEST_PERSISTENCE_CHECK(
        _Connection(_FundingManifestEngine(fixture)),
        run=fixture["run"],
        result=fixture["result"],
        trade_date=TRADE_DATE,
    )

    assert passed is False
    assert detail["invalid_count"] > 0


@pytest.mark.parametrize(
    ("query_fragment", "field", "value"),
    [
        ("SELECT run_uid, strategy_key", "result_hash", "0" * 64),
        ("SELECT run_uid, combination_key", "result_hash", "0" * 64),
        ("SELECT run_uid, trade_date, pool_level", "rank_no", 2),
        ("FROM st_strategy_allocation_snapshot ORDER BY", "simulated_weight_pct", Decimal("14.0000")),
    ],
)
def test_all_historical_snapshot_rows_fail_on_single_field_tamper(
    query_fragment,
    field,
    value,
):
    class _TamperedHistoryEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if query_fragment in sql and result._rows:
                result._rows[0][field] = value
            return result

    passed, detail = health._all_governance_snapshot_history_check(
        _Connection(_TamperedHistoryEngine(runs=[_completed_run()]))
    )

    assert passed is False
    assert detail["invalid_count"] > 0


@pytest.mark.parametrize(
    ("field", "tampered_value"),
    [
        ("stock_code", "000002"),
        ("rank_no", 2),
        ("gate_status", "研究确认"),
        ("strategies_json", ["combo_a"]),
    ],
)
def test_health_rejects_any_tampered_pool_row_field(
    monkeypatch,
    field,
    tampered_value,
):
    _fixed_trade_date(monkeypatch)

    class _TamperedPoolEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if "SELECT trade_date, pool_level, stock_code" in sql:
                tradable = next(
                    row
                    for row in result._rows
                    if row["pool_level"] == "TRADABLE"
                )
                tradable[field] = deepcopy(tampered_value)
            return result

    result = health.collect_governance_health(
        _TamperedPoolEngine(runs=[_completed_run()]),
        expected_build_sha=BUILD_SHA,
    )

    assert result["status"] == "FAIL"
    checks = {item["name"]: item for item in result["checks"]}
    assert checks[
        "pool_rows_snapshot_hash_and_funding_references"
    ]["passed"] is False
    assert checks[
        "allocation_candidate_snapshot_and_decision_hashes"
    ]["passed"] is False
    assert checks[
        "pool_rows_snapshot_hash_and_funding_references"
    ]["detail"]["errors"]


def test_health_rejects_rehashed_pool_row_when_bound_snapshot_is_unchanged(
    monkeypatch,
):
    _fixed_trade_date(monkeypatch)

    class _RehashedTamperedPoolEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if "SELECT trade_date, pool_level, stock_code" in sql:
                row = next(
                    item
                    for item in result._rows
                    if item["pool_level"] == "TRADABLE"
                )
                row["stock_code"] = "000002"
                envelope = deepcopy(row["evidence_json"])
                payload = {
                    "schema": health.POOL_ROW_SCHEMA,
                    "trade_date": row["trade_date"],
                    "pool_level": row["pool_level"],
                    "stock_code": row["stock_code"],
                    "stock_name": row["stock_name"],
                    "rank_no": row["rank_no"],
                    "opportunity_score": health._pool_score_text(
                        row["opportunity_score"]
                    ),
                    "execution_score": health._pool_score_text(
                        row["execution_score"]
                    ),
                    "dominant_strategy": row["dominant_strategy"],
                    "strategies": row["strategies_json"],
                    "industry_name": row["industry_name"],
                    "gate_status": row["gate_status"],
                    "reason": row["reason_json"],
                    "evidence": envelope["source_evidence"],
                }
                envelope["pool_row_hash"] = health._canonical_digest(
                    payload
                )
                row["evidence_json"] = envelope
            return result

    result = health.collect_governance_health(
        _RehashedTamperedPoolEngine(runs=[_completed_run()]),
        expected_build_sha=BUILD_SHA,
    )

    assert result["status"] == "FAIL"
    checks = {item["name"]: item for item in result["checks"]}
    pool_check = checks[
        "pool_rows_snapshot_hash_and_funding_references"
    ]
    assert pool_check["passed"] is False
    assert any(
        error["reason"] == "pool snapshot summary hash/count differs"
        for error in pool_check["detail"]["errors"]
    )
    assert checks[
        "allocation_candidate_snapshot_and_decision_hashes"
    ]["passed"] is False


def test_combination_member_version_drift_uses_current_member_ranking_and_fails_closed(
    monkeypatch,
):
    _fixed_trade_date(monkeypatch)
    members = [
        {
            "strategy_key": key,
            "strategy_version": "v0",
            "weight": 0.5,
        }
        for key in sorted(STRATEGY_ROUTES)
    ]
    constraints = {}
    route_payload = {
        **{
            key: value
            for key, value in COMBINATION_ROUTE.items()
            if key != "router_decision_hash"
        },
        "eligible": False,
        "reason": "组合冻结成员版本与当前版本不一致",
    }
    route = {
        **route_payload,
        "router_decision_hash": health._canonical_digest(route_payload),
    }
    window_evidence = {
        str(window): health._canonical_digest(
            {"combination": "combo_a", "drift_window": window}
        )
        for window in health.EXPECTED_WINDOWS
    }
    member_details = [
        {
            "strategy_key": key,
            "strategy_name": key,
            "strategy_version": "v0",
            "current_strategy_version": "v1",
            "version_match": False,
            "weight": 0.5,
            "status_label": (
                "正常运行" if key == "strategy_a" else "影子观察"
            ),
            "lifecycle_status": (
                "ACTIVE" if key == "strategy_a" else "SHADOW"
            ),
            "lifecycle_risk_multiplier": (
                1.0 if key == "strategy_a" else 0.0
            ),
            "effective_weight_after_lifecycle": (
                0.5 if key == "strategy_a" else 0.0
            ),
            "contribution_score": round(
                STRATEGY_RANKING_SCORES[key] * 0.5, 2
            ),
        }
        for key in sorted(STRATEGY_ROUTES)
    ]
    pre_gate_hash = health._canonical_digest(
        {
            "combination_key": "combo_a",
            "combination_version": "v1",
            "window_evidence": window_evidence,
            "member_versions": {
                item["strategy_key"]: {
                    "frozen": "v0",
                    "current": "v1",
                    "lifecycle_status": item["lifecycle_status"],
                    "lifecycle_risk_multiplier": item[
                        "lifecycle_risk_multiplier"
                    ],
                }
                for item in member_details
            },
            "member_sleeve_risk_multiplier": 0.5,
            "router_decision_hash": route["router_decision_hash"],
            "constraint_evaluation_hash": COMBINATION_CONSTRAINT_EVALUATION[
                "evaluation_hash"
            ],
            "profit_gate_passed": False,
        }
    )
    from server.engine import strategy_governance as governance_module

    raw_metrics = {}
    metrics = {}
    for window in health.EXPECTED_WINDOWS:
        raw = _valid_funding_window_metrics(
            window=window,
            evidence_hash=window_evidence[str(window)],
            route_hash=route["router_decision_hash"],
        )
        raw_metrics[str(window)] = raw
        metrics[str(window)] = (
            governance_module._canonical_metric_window_summary(
                raw,
                entity_type="COMBINATION",
                entity_key="combo_a",
                entity_version="v1",
                window_days=window,
            )
        )
    gate_hash = _final_funding_hash(
        entity_type="COMBINATION",
        entity_key="combo_a",
        pre_gate_hash=pre_gate_hash,
        decision=COMBINATION_STATISTICAL_DECISION,
        confirmation=COMBINATION_CONFIRMATION,
        projected_status="SHADOW",
        paper_eligible=False,
    )
    payload = {
        "combination_key": "combo_a",
        "combination_version": "v1",
        "trade_date": TRADE_DATE,
        "metrics": metrics,
        "multi_window_gate": {
            str(window): raw_metrics[str(window)]["profit_gate"]
            for window in health.EXPECTED_WINDOWS
        },
        "decision_contract_version": (
            governance_module.STATISTICAL_DECISION_CONTRACT
        ),
        "statistical_policy_hash": governance_module.STATISTICAL_POLICY_HASH,
        "statistical_family_decision": COMBINATION_STATISTICAL_DECISION,
        "statistical_family_passed": True,
        "pre_confirmation_funding_gate_hash": pre_gate_hash,
        "confirmation_guard": COMBINATION_CONFIRMATION,
        "funding_gate_hash": gate_hash,
        "funding_evidence_revision_at": f"{TRADE_DATE}T15:00:00",
        "overall_profit_gate_passed": False,
        "market_route": route,
        "paper_allocation_eligible": False,
        "member_details": member_details,
        "constraint_evaluation": COMBINATION_CONSTRAINT_EVALUATION,
    }
    combo_rows = [
        {
            "combination_key": "combo_a",
            "combination_version": "v1",
            "trade_date": TRADE_DATE,
            "recommended_status": "SHADOW",
            "ranking_score": Decimal(str(COMBINATION_RANKING_SCORE)),
            "profit_gate_passed": 0,
            "evidence_json": payload,
            "result_hash": health._canonical_digest(payload),
            "members_json": json.dumps(members),
            "constraints_json": json.dumps(constraints),
            "config_hash": health._canonical_digest(
                {"members": members, "constraints": constraints}
            ),
            "registry_name": "combo_a",
            "registry_current_version": "v1",
            "registry_current_status": "SHADOW",
            "registry_enabled": 1,
        }
    ]
    router_snapshot_hash = health._canonical_digest(
        {
            "schema": "probiga.strategy-market-router-snapshot.v1",
            "policy_version": health.ROUTER_POLICY_VERSION,
            "trade_date": TRADE_DATE,
            "market_state": MARKET_STATE,
            "market_state_config_hash": MARKET_CONFIG_HASH,
            "strategy_routes": {
                key: value["router_decision_hash"]
                for key, value in sorted(STRATEGY_ROUTES.items())
            },
            "combination_routes": {
                "combo_a": route["router_decision_hash"]
            },
        }
    )
    run = _completed_run()
    allocation_contract = _fixture_allocation_contract(
        router_snapshot_hash=router_snapshot_hash,
        combination_route=route,
        combination_gate_hash=gate_hash,
        combination_pre_gate_hash=pre_gate_hash,
        combination_canonical_metrics=metrics,
        combination_profit_gate_passed=False,
        combination_member_details=member_details,
        combination_risk_metrics={},
    )
    run["router_snapshot_hash"] = router_snapshot_hash
    run["decision_hash"] = allocation_contract["decision_hash"]
    run["result_json"] = allocation_contract["_result_json"]
    run["result_hash"] = allocation_contract["_result_hash"]
    run["summary_json"] = {
        **run["summary_json"],
        "combination_route_eligible_count": 0,
        "router_snapshot_hash": router_snapshot_hash,
        **{
            key: value
            for key, value in allocation_contract.items()
            if key != "decision_hash" and not key.startswith("_")
        },
    }

    class _DriftEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            if "SELECT h.combination_key, h.combination_version" in sql:
                return _Result(combo_rows)
            if "SELECT c.combination_key, c.current_version" in sql:
                return _Result(
                    [
                        {
                            "combination_key": "combo_a",
                            "current_version": "v1",
                            "members_json": combo_rows[0]["members_json"],
                            "constraints_json": combo_rows[0][
                                "constraints_json"
                            ],
                            "config_hash": combo_rows[0]["config_hash"],
                        }
                    ]
                )
            if (
                "SELECT combination_key, combination_version, evidence_json"
                in sql
            ):
                return _Result(
                    [
                        {
                            "combination_key": "combo_a",
                            "combination_version": "v1",
                            "evidence_json": payload,
                        }
                    ]
                )
            return super().execute(sql, params)

    result = health.collect_governance_health(
        _DriftEngine(runs=[run]), expected_build_sha=BUILD_SHA
    )

    assert result["status"] == "PASS"
    router_check = next(
        item
        for item in result["checks"]
        if item["name"] == "market_router_snapshot_is_reproducible"
    )
    assert router_check["passed"] is True


def test_forward_version_relations_allow_only_true_quarantine():
    class _ForwardCountsEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            if "AS eligible_empty_count" in sql:
                assert " LIMIT " not in f" {sql.upper()} "
                assert sql.count("'LEGACY_VERSION_DERIVED'") == 1
                assert sql.count("e2.source_run_uid<>''") == 2
                assert (
                    "ON BINARY current_strategy.strategy_key="
                    "\n             BINARY e.strategy_key"
                ) in sql
                assert (
                    "AND BINARY current_strategy.current_version="
                    "\n             BINARY e.strategy_version"
                ) in sql
                return _Result(
                    [
                        {
                            "total_count": 5,
                            "versioned_count": 3,
                            "quarantined_count": 2,
                            "eligible_empty_count": 0,
                            "invalid_nonempty_count": 0,
                            "current_version_evidence_count": 3,
                        }
                    ]
                )
            return super().execute(sql, params)

    engine = _ForwardCountsEngine()
    with engine.connect() as connection:
        passed, detail = health._forward_strategy_version_data_check(
            connection
        )

    assert passed is True
    assert detail["total_count"] == (
        detail["versioned_count"] + detail["quarantined_count"]
    )
    assert detail["eligible_empty_count"] == 0
    assert detail["invalid_nonempty_count"] == 0
    assert detail["current_version_query_has_limit"] is False


def test_forward_version_relations_reject_eligible_empty_and_invalid_nonempty():
    class _ForwardCountsEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            if "AS eligible_empty_count" in sql:
                return _Result(
                    [
                        {
                            "total_count": 5,
                            "versioned_count": 3,
                            "quarantined_count": 2,
                            "eligible_empty_count": 1,
                            "invalid_nonempty_count": 1,
                            "current_version_evidence_count": 2,
                        }
                    ]
                )
            return super().execute(sql, params)

    engine = _ForwardCountsEngine()
    with engine.connect() as connection:
        passed, detail = health._forward_strategy_version_data_check(
            connection
        )

    assert passed is False
    assert detail["eligible_empty_count"] == 1
    assert detail["invalid_nonempty_count"] == 1


def test_health_invokes_frozen_exit_allocation_schema_and_fifo_replay(
    monkeypatch,
):
    _fixed_trade_date(monkeypatch)
    fills, allocation = _unattributed_fifo_fixture()
    result = health.collect_governance_health(
        _GovernanceHealthEngine(
            runs=[_completed_run()],
            raw_fill_rows=fills,
            exit_allocation_rows=[allocation],
        ),
        expected_build_sha=BUILD_SHA,
    )

    assert result["status"] == "PASS"
    checks = {check["name"]: check for check in result["checks"]}
    schema = checks["forward_exit_allocation_v3_frozen_schema"]
    replay = checks["forward_exit_allocation_v3_fifo_conservation"]
    assert schema["passed"] is True
    assert schema["detail"]["frozen_checksum"] == (
        "f2e99ea79df11e578e17298ebd9a829cc0715d334708ca760bd99970a6a5d460"
    )
    assert schema["detail"]["frozen_statement_count"] == 1
    assert schema["detail"]["frozen_migration_count"] == 27
    assert schema["detail"]["database_triggers_required"] is False
    assert schema["detail"]["database_trigger_inventory_checked"] is False
    assert replay["passed"] is True
    assert replay["detail"]["raw_sell_count"] == 1
    assert replay["detail"]["expected_allocation_count"] == 1
    assert replay["detail"]["observed_allocation_count"] == 1


def test_health_fails_closed_when_exit_allocation_003_ledger_drifts(
    monkeypatch,
):
    _fixed_trade_date(monkeypatch)

    class _Drifted003LedgerEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if (
                "FROM schema_migration_v3 WHERE version=:version" in sql
                and params.get("version")
                == "20260822_003_forward_exit_allocation_ledger"
            ):
                result._rows[0]["checksum"] = "0" * 64
            return result

    result = health.collect_governance_health(
        _Drifted003LedgerEngine(runs=[]),
        expected_build_sha=BUILD_SHA,
        allow_input_not_ready=True,
    )

    assert result["status"] == "FAIL"
    assert result["run_disposition"] == "schema_invalid"
    check = next(
        item
        for item in result["checks"]
        if item["name"] == "forward_exit_allocation_v3_frozen_schema"
    )
    assert check["passed"] is False
    assert "003 migration ledger row differs" in check["detail"]["errors"]


def test_exit_allocation_replay_rejects_missing_extra_and_mutated_rows():
    fills, allocation = _unattributed_fifo_fixture()
    mutations = []
    mutations.append([])
    extra = deepcopy(allocation)
    extra["allocation_id"] = "f" * 64
    extra["allocation_sequence"] = 1
    mutations.append([allocation, extra])
    wrong_quantity = deepcopy(allocation)
    wrong_quantity["allocated_quantity"] = 39
    mutations.append([wrong_quantity])
    wrong_attribution = deepcopy(allocation)
    wrong_attribution["attribution_status"] = "ATTRIBUTED"
    mutations.append([wrong_attribution])

    for observed in mutations:
        engine = _GovernanceHealthEngine(
            raw_fill_rows=fills,
            exit_allocation_rows=observed,
        )
        with engine.connect() as connection:
            passed, detail = health._forward_exit_allocation_data_check(
                connection
            )
        assert passed is False
        assert detail["errors"]
        assert detail["full_replay_has_limit"] is False


def test_current_canonical_metric_replay_rejects_raw_ledger_tampering(
    monkeypatch,
):
    _fixed_trade_date(monkeypatch)
    fills, allocation = _unattributed_fifo_fixture()
    fills[0]["tamper_raw_metric"] = True
    result = health.collect_governance_health(
        _GovernanceHealthEngine(
            runs=[_completed_run()],
            raw_fill_rows=fills,
            exit_allocation_rows=[allocation],
        ),
        expected_build_sha=BUILD_SHA,
    )

    check = next(
        item
        for item in result["checks"]
        if item["name"]
        == "current_canonical_metrics_replay_from_raw_ledgers"
    )
    assert result["status"] == "FAIL"
    assert check["passed"] is False
    assert any(
        error["path"].endswith("metrics.net_expectancy_pct")
        for error in check["detail"]["errors"]
    )
    assert "st_trade_intent_v2" in check["detail"]["source_contract"]
    assert "QMT-attested" in check["detail"]["source_contract"]


def test_qmt_session_window_attestation_is_exact_and_hash_bound(monkeypatch):
    from server.engine import strategy_governance as governance_module

    windows = {
        value: _attested_session_window(value)
        for value in health.EXPECTED_WINDOWS
    }
    row_binding_proof = _row_binding_proof(windows[120])
    monkeypatch.setattr(
        governance_module,
        "_authoritative_session_windows_with_proof",
        lambda _trade_date: (
            deepcopy(windows),
            deepcopy(row_binding_proof),
        ),
    )
    passed, first_detail = health._authoritative_session_window_attestation_check(
        TRADE_DATE
    )
    assert passed is True
    monkeypatch.setattr(
        governance_module,
        "_db_read",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("health must reuse its authoritative row proof")
        ),
    )
    row_passed, row_detail = health._qmt_row_attestation_binding_check(
        TRADE_DATE,
        first_detail,
    )
    assert row_passed is True
    assert row_detail["proof_reused"] is True
    assert row_detail["database_query_count"] == 0

    tampered = deepcopy(windows)
    tampered[20]["session_attestations"][0]["latest_received_at"] = ""
    payload = {
        key: value
        for key, value in tampered[20].items()
        if key != "session_hash"
    }
    tampered[20]["session_hash"] = health._canonical_digest(payload)
    monkeypatch.setattr(
        governance_module,
        "_authoritative_session_windows_with_proof",
        lambda _trade_date: (tampered, deepcopy(row_binding_proof)),
    )
    passed, detail = health._authoritative_session_window_attestation_check(
        TRADE_DATE
    )
    assert passed is False
    assert detail["errors"][0]["window_days"] == 20

    wrong_universe = deepcopy(windows)
    wrong_universe[20]["session_attestations"][0][
        "expected_stock_set_hash"
    ] = "0" * 64
    payload = {
        key: value
        for key, value in wrong_universe[20].items()
        if key != "session_hash"
    }
    wrong_universe[20]["session_hash"] = health._canonical_digest(payload)
    monkeypatch.setattr(
        governance_module,
        "_authoritative_session_windows_with_proof",
        lambda _trade_date: (
            wrong_universe,
            deepcopy(row_binding_proof),
        ),
    )
    passed, detail = health._authoritative_session_window_attestation_check(
        TRADE_DATE
    )
    # The window helper proves shape/hash binding.  The separately hash-bound
    # row proof remains authoritative for three-set membership.
    assert passed is True
    assert detail["windows"]["20"]["expected_stock_sets"][
        wrong_universe[20]["sessions"][0]
    ]["expected_stock_set_hash"] == "0" * 64

    legacy = deepcopy(windows)
    legacy[20]["session_attestations"][0].pop(
        "pre_close_attestation_protocol"
    )
    payload = {
        key: value
        for key, value in legacy[20].items()
        if key != "session_hash"
    }
    legacy[20]["session_hash"] = health._canonical_digest(payload)
    monkeypatch.setattr(
        governance_module,
        "_authoritative_session_windows_with_proof",
        lambda _trade_date: (legacy, deepcopy(row_binding_proof)),
    )
    passed, detail = health._authoritative_session_window_attestation_check(
        TRADE_DATE
    )
    assert passed is False
    assert detail["errors"][0]["window_days"] == 20


def test_qmt_pre_close_v2_requires_row_table_and_current_snapshot_binding(
    monkeypatch,
):
    from server.engine import strategy_governance as governance_module

    window = _attested_session_window(120)
    sessions = window["sessions"]
    expected_stock_sets = {
        row["trade_date"]: {
            "expected_stock_count": row["expected_stock_count"],
            "expected_stock_set_hash": row["expected_stock_set_hash"],
        }
        for row in window["session_attestations"]
    }
    detail = {
        "windows": {
            "120": {
                "sessions": sessions,
                "expected_stock_sets": expected_stock_sets,
            }
        }
    }
    observed_sql = []
    tolerance_json = qmt_attester.build_qmt_v2_manifest(
        {
            day: qmt_attester.expected_stock_set_contract(
                day,
                ["000001", "600000"],
            )
            for day in sessions
        }
    )

    def valid_read(sql, params=None):
        observed_sql.append(sql)
        if "information_schema.tables" in sql:
            return [{"cnt": 2}]
        assert params == {
            "protocol_version": health.QMT_PRECLOSE_ATTESTATION_PROTOCOL,
            "start_date": sessions[0],
            "trade_date": TRADE_DATE,
        }
        if "SELECT run_id, start_date, end_date, tolerance_json" in sql:
            return [
                {
                    "run_id": "qmt-complete-v2",
                    "start_date": sessions[0],
                    "end_date": sessions[-1],
                    "tolerance_json": tolerance_json,
                }
            ]
        for token in (
            "qmt_kline_attestation_row a",
            "a.source_pre_close_origin=BINARY 'NATIVE_QMT'",
            "a.source_pre_close=k.pre_close",
            "a.attested_open=k.`open`",
            "a.attested_close=k.`close`",
            "a.attested_high=k.`high`",
            "a.attested_low=k.`low`",
            "a.attested_volume=k.volume",
            "a.attested_amount=k.amount",
            "a.attestation_id=BINARY SHA2",
            "r.status='COMPLETED'",
            "$.universe_manifest_schema",
            "$.daily_universe",
        ):
            assert token in sql
        return [
            {
                "trade_date": day,
                "stock_code": stock_code,
                "in_target": 1,
                "in_completed_attestation": 1,
                "in_exact_attestation": 1,
            }
            for day in sessions
            for stock_code in ("000001", "600000")
        ]

    monkeypatch.setattr(governance_module, "_db_read", valid_read)
    passed, result = health._qmt_row_attestation_binding_check(
        TRADE_DATE,
        detail,
    )
    assert passed is True
    assert result["table_exists"] is True
    assert result["expected_session_count"] == 120
    assert result["target_stock_count"] == 240
    assert result["completed_attestation_stock_count"] == 240
    assert result["exact_attestation_stock_count"] == 240
    assert len(observed_sql) == 3
    universe_sql = next(
        sql for sql in observed_sql if "MAX(u.in_target)" in sql
    )
    assert "EXISTS (" not in universe_sql
    assert universe_sql.count("JOIN qmt_kline_attestation_run r") == 2
    assert universe_sql.count("r.run_id=a.run_id") == 2
    assert universe_sql.count("BINARY r.run_id=BINARY a.run_id") == 2
    assert universe_sql.count(A_SHARE_STOCK_CODE_SQL_REGEXP) == 3
    assert "REGEXP '^(0|3|6)'" not in universe_sql

    wrong_contract_detail = deepcopy(detail)
    wrong_contract_detail["windows"]["120"]["expected_stock_sets"][
        sessions[0]
    ]["expected_stock_set_hash"] = "0" * 64
    passed, result = health._qmt_row_attestation_binding_check(
        TRADE_DATE,
        wrong_contract_detail,
    )
    assert passed is False
    assert result["errors"][0]["trade_date"] == sessions[0]
    assert result["errors"][0]["contract_stock_set_hash"] == "0" * 64

    monkeypatch.setattr(
        governance_module,
        "_db_read",
        lambda sql, params=None: [{"cnt": 0}],
    )
    passed, result = health._qmt_row_attestation_binding_check(
        TRADE_DATE,
        detail,
    )
    assert passed is False
    assert result["table_exists"] is False


def test_qmt_pre_close_v2_rejects_batch_only_or_stale_row_coverage(
    monkeypatch,
):
    from server.engine import strategy_governance as governance_module

    window = _attested_session_window(120)
    sessions = window["sessions"]
    detail = {
        "windows": {
            "120": {
                "sessions": sessions,
                "expected_stock_sets": {
                    row["trade_date"]: {
                        "expected_stock_count": row[
                            "expected_stock_count"
                        ],
                        "expected_stock_set_hash": row[
                            "expected_stock_set_hash"
                        ],
                    }
                    for row in window["session_attestations"]
                },
            }
        }
    }
    tolerance_json = qmt_attester.build_qmt_v2_manifest(
        {
            day: qmt_attester.expected_stock_set_contract(
                day,
                ["000001", "600000"],
            )
            for day in sessions
        }
    )

    def stale_read(sql, params=None):
        if "information_schema.tables" in sql:
            return [{"cnt": 2}]
        if "SELECT run_id, start_date, end_date, tolerance_json" in sql:
            return [
                {
                    "run_id": "qmt-complete-v2",
                    "start_date": sessions[0],
                    "end_date": sessions[-1],
                    "tolerance_json": tolerance_json,
                }
            ]
        return [
            {
                "trade_date": day,
                "stock_code": stock_code,
                "in_target": 1,
                "in_completed_attestation": 1,
                "in_exact_attestation": (
                    0
                    if index == 0 and stock_code == "600000"
                    else 1
                ),
            }
            for index, day in enumerate(sessions)
            for stock_code in ("000001", "600000")
        ]

    monkeypatch.setattr(governance_module, "_db_read", stale_read)
    passed, result = health._qmt_row_attestation_binding_check(
        TRADE_DATE,
        detail,
    )
    assert passed is False
    assert result["errors"][0]["trade_date"] == sessions[0]
    assert "stock sets" in result["errors"][0]["reason"]


def test_qmt_row_binding_reuses_one_authoritative_hash_bound_proof(
    monkeypatch,
):
    from server.engine import strategy_governance as governance_module

    window = _attested_session_window(120)
    detail = {
        "windows": {
            "120": {
                "sessions": window["sessions"],
                "expected_stock_sets": {
                    row["trade_date"]: {
                        "expected_stock_count": row[
                            "expected_stock_count"
                        ],
                        "expected_stock_set_hash": row[
                            "expected_stock_set_hash"
                        ],
                    }
                    for row in window["session_attestations"]
                },
            }
        },
        "row_binding_proof": _row_binding_proof(window),
    }

    def unexpected_read(_sql, _params=None):
        raise AssertionError("reused authoritative proof must not rescan QMT")

    monkeypatch.setattr(governance_module, "_db_read", unexpected_read)
    passed, result = health._qmt_row_attestation_binding_check(
        TRADE_DATE,
        detail,
    )
    assert passed is True
    assert result["proof_reused"] is True
    assert result["database_query_count"] == 0
    assert result["row_run_binding"] == "SAME_COMPLETED_RUN_ID"

    tampered = deepcopy(detail)
    tampered_proof = tampered["row_binding_proof"]
    tampered_proof["sessions"][0][
        "exact_attestation_stock_count"
    ] = 1
    proof_payload = {
        key: value
        for key, value in tampered_proof.items()
        if key != "proof_hash"
    }
    tampered_proof["proof_hash"] = health._canonical_digest(proof_payload)
    passed, result = health._qmt_row_attestation_binding_check(
        TRADE_DATE,
        tampered,
    )
    assert passed is False
    assert result["proof_reused"] is True
    assert result["errors"][0]["trade_date"] == window["sessions"][0]


def test_health_recomputes_reduce_discount_and_keeps_budget_in_cash(
    monkeypatch,
):
    _fixed_trade_date(monkeypatch)
    reduced_member_details = deepcopy(COMBINATION_MEMBER_DETAILS)
    reduced_member_details[0].update(
        {
            "status_label": "降权运行",
            "lifecycle_status": "REDUCE",
            "lifecycle_risk_multiplier": 0.5,
            "effective_weight_after_lifecycle": 0.25,
        }
    )
    reduced_combination_pre_gate_hash = health._canonical_digest(
        {
            "combination_key": "combo_a",
            "combination_version": "v1",
            "window_evidence": COMBINATION_WINDOW_EVIDENCE,
            "member_versions": {
                item["strategy_key"]: {
                    "frozen": item["strategy_version"],
                    "current": item["current_strategy_version"],
                    "lifecycle_status": item["lifecycle_status"],
                    "lifecycle_risk_multiplier": item[
                        "lifecycle_risk_multiplier"
                    ],
                }
                for item in reduced_member_details
            },
            "member_sleeve_risk_multiplier": 0.25,
            "router_decision_hash": COMBINATION_ROUTE[
                "router_decision_hash"
            ],
            "constraint_evaluation_hash": (
                COMBINATION_CONSTRAINT_EVALUATION["evaluation_hash"]
            ),
            "profit_gate_passed": False,
        }
    )
    reduced_combination_gate_hash = _final_funding_hash(
        entity_type="COMBINATION",
        entity_key="combo_a",
        pre_gate_hash=reduced_combination_pre_gate_hash,
        decision=COMBINATION_STATISTICAL_DECISION,
        confirmation=COMBINATION_CONFIRMATION,
        projected_status="SHADOW",
        paper_eligible=False,
    )
    reduced_strategy_gate_hash = _final_funding_hash(
        entity_type="STRATEGY",
        entity_key="strategy_a",
        pre_gate_hash=STRATEGY_PRE_GATE_HASHES["strategy_a"],
        decision=STRATEGY_STATISTICAL_DECISIONS["strategy_a"],
        confirmation=STRATEGY_CONFIRMATIONS["strategy_a"],
        projected_status="REDUCE",
        paper_eligible=True,
    )
    reduced_combination_rows = _valid_combination_router_rows()
    for combo_row in reduced_combination_rows:
        payload = combo_row["evidence_json"]
        payload["member_details"] = reduced_member_details
        payload["pre_confirmation_funding_gate_hash"] = (
            reduced_combination_pre_gate_hash
        )
        payload["funding_gate_hash"] = reduced_combination_gate_hash
        combo_row["result_hash"] = health._canonical_digest(payload)

    class _ReduceAllocationEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            if "SELECT h.combination_key, h.combination_version" in sql:
                return _Result(deepcopy(reduced_combination_rows))
            if (
                "SELECT combination_key, combination_version, evidence_json"
                in sql
            ):
                return _Result(
                    [
                        {
                            "combination_key": "combo_a",
                            "combination_version": "v1",
                            "ranking_score": Decimal("80.0000"),
                            "evidence_json": {
                                "overall_profit_gate_passed": False,
                                "funding_gate_hash": (
                                    reduced_combination_gate_hash
                                ),
                            },
                        }
                    ]
                )
            result = super().execute(sql, params)
            if (
                "SELECT strategy_key, strategy_version, evidence_json"
                in sql
                and "FROM st_strategy_health_snapshot" in sql
            ):
                for row in result._rows:
                    if row["strategy_key"] == "strategy_a":
                        row["evidence_json"]["funding_gate_hash"] = (
                            reduced_strategy_gate_hash
                        )
            if "SELECT h.strategy_key, h.strategy_version" in sql:
                for row in result._rows:
                    if row["strategy_key"] == "strategy_a":
                        row["registry_current_status"] = "REDUCE"
                        row["recommended_status"] = "REDUCE"
                        payload = row["evidence_json"]
                        payload["funding_gate_hash"] = (
                            reduced_strategy_gate_hash
                        )
                        row["result_hash"] = health._canonical_digest(
                            payload
                        )
            if (
                "FROM st_strategy_allocation_snapshot" in sql
                and len(result._rows) == 2
                and all("target_type" in row for row in result._rows)
            ):
                cash, strategy = result._rows
                cash["simulated_weight_pct"] = Decimal("57.5000")
                strategy.update(
                    {
                        "funding_gate_hash": reduced_strategy_gate_hash,
                        "strategy_current_status": "REDUCE",
                        "lifecycle_status": "REDUCE",
                        "lifecycle_status_label": "降权运行",
                        "lifecycle_risk_multiplier": Decimal("0.5000"),
                        "base_competitive_weight_pct": Decimal("85.0000"),
                        "simulated_weight_pct": Decimal("42.5000"),
                        "cash_discount_bp": 4250,
                    }
                )
            return result

    run = _completed_run()
    allocation_contract = _fixture_allocation_contract(
        strategy_a_status="REDUCE",
        combination_gate_hash=reduced_combination_gate_hash,
        combination_pre_gate_hash=reduced_combination_pre_gate_hash,
        combination_member_details=reduced_member_details,
    )
    run["decision_hash"] = allocation_contract["decision_hash"]
    run["result_json"] = allocation_contract["_result_json"]
    run["result_hash"] = allocation_contract["_result_hash"]
    run["summary_json"].update(
        {
            key: value
            for key, value in allocation_contract.items()
            if key != "decision_hash" and not key.startswith("_")
        }
    )
    result = health.collect_governance_health(
        _ReduceAllocationEngine(runs=[run]),
        expected_build_sha=BUILD_SHA,
    )

    assert result["status"] == "PASS"
    lifecycle_budget = next(
        check
        for check in result["checks"]
        if check["name"] == "allocation_lifecycle_budget_exact"
    )
    assert lifecycle_budget["passed"] is True
    assert lifecycle_budget["detail"]["expected_actual_weight_pct"] == (
        "42.5"
    )
    assert lifecycle_budget["detail"]["expected_cash_weight_pct"] == "57.5"


def test_allocation_replay_keeps_zero_bp_candidate_in_denominator():
    bindings = {}
    for key, score, status in (
        ("large", Decimal("80.0000"), "ACTIVE"),
        ("tiny", Decimal("0.0100"), "REDUCE"),
    ):
        bindings[("STRATEGY", key, "v1")] = {
            "router_decision_hash": health._canonical_digest(
                {"route": key}
            ),
            "market_match_score": Decimal("100.0000"),
            "market_state": MARKET_STATE,
            "eligible": True,
            "paper_allocation_eligible": True,
            "funding_gate_hash": health._canonical_digest(
                {"gate": key}
            ),
            "members": frozenset({key}),
            "ranking_score": score,
            "target_name": key,
            "enabled": True,
            "lifecycle_status": status,
            "profit_gate_passed": True,
            "constraint_passed": True,
            "portfolio_risk_metrics": _portfolio_risk_metrics(
                0 if key == "large" else 1,
                "000001" if key == "large" else "000002",
            ),
        }
    candidates, errors = health._allocation_candidate_contract(bindings)
    assert not errors
    expected = health._expected_allocation_snapshot(
        candidates,
        market_state=MARKET_STATE,
        trading_gate_passed=True,
    )
    assert not any(row["target_key"] == "tiny" for row in expected)
    assert next(
        row for row in expected if row["target_key"] == "large"
    )["base_competitive_weight_pct"] == 84.99
    run = {
        "trade_date": TRADE_DATE,
        "market_state": MARKET_STATE,
        "build_commit_sha": BUILD_SHA,
        "input_hash": "c" * 64,
        "router_snapshot_hash": "d" * 64,
        "decision_hash": "",
        "summary_json": {
            "allocation_policy_version": health.ALLOCATION_POLICY_VERSION,
            "trading_gate_passed": True,
            "market_risk_cap_pct": 85.0,
            "allocation_candidate_count": 2,
            "eligible_candidate_count": 2,
            "allocation_count": 1,
            "pool_row_count": 3,
            "pool_snapshot_hash": _fixture_pool_snapshot_hash(),
            "automatic_transition_count": 0,
            "automatic_transition_plan_hash": (
                _fixture_transition_plan_hash()
            ),
            "cash_weight_pct": next(
                row["simulated_weight_pct"]
                for row in expected
                if row["target_type"] == "CASH"
            ),
        },
    }
    _passed, detail, _rows = health._allocation_decision_contract_check(
        run, bindings, expected, TRADE_DATE
    )
    run["summary_json"].update(
        {
            "candidate_set_hash": detail["expected_candidate_set_hash"],
            "allocation_snapshot_hash": detail[
                "expected_allocation_snapshot_hash"
            ],
        }
    )
    run["decision_hash"] = detail["expected_decision_hash"]

    passed, detail, replayed = health._allocation_decision_contract_check(
        run, bindings, expected, TRADE_DATE
    )

    assert passed is True
    assert replayed == expected
    assert detail["errors"] == []


def test_health_replay_fails_closed_when_frozen_pair_evidence_is_tampered():
    bindings = {}
    for index, key in enumerate(("preferred", "fallback")):
        bindings[("STRATEGY", key, "v1")] = {
            "router_decision_hash": health._canonical_digest({"route": key}),
            "market_match_score": Decimal("100.0000"),
            "market_state": MARKET_STATE,
            "eligible": True,
            "paper_allocation_eligible": True,
            "funding_gate_hash": health._canonical_digest({"gate": key}),
            "members": frozenset({key}),
            "ranking_score": Decimal(str(90 - index)),
            "target_name": key,
            "enabled": True,
            "lifecycle_status": "ACTIVE",
            "profit_gate_passed": True,
            "constraint_passed": True,
            "portfolio_risk_metrics": _portfolio_risk_metrics(
                index, f"{index + 1:06d}"
            ),
        }
    candidates, errors = health._allocation_candidate_contract(bindings)
    assert errors == []
    preferred = next(
        row for row in candidates if row["target_key"] == "preferred"
    )
    preferred["portfolio_risk_evidence"]["daily_returns"][0][
        "return_pct"
    ] = "99.00000000"

    selected, rejected, gate_hash = health._replay_global_portfolio_gate(
        [sorted(candidates, key=lambda row: -row["ranking_score"])]
    )

    assert [row["target_key"] for row in selected] == ["fallback"]
    assert rejected[0]["target_key"] == "preferred"
    assert "缺少有效" in rejected[0]["reason"]
    assert health.RESULT_HASH_RE.fullmatch(gate_hash)


def test_health_replay_counts_unique_expanded_combination_sleeves():
    members = frozenset(f"member_{index}" for index in range(9))
    bindings = {
        ("COMBINATION", "nine_member_combo", "v1"): {
            "router_decision_hash": health._canonical_digest({
                "route": "combo"
            }),
            "market_match_score": Decimal("100.0000"),
            "market_state": MARKET_STATE,
            "eligible": True,
            "paper_allocation_eligible": True,
            "funding_gate_hash": health._canonical_digest({"gate": "combo"}),
            "members": members,
            "ranking_score": Decimal("90.0000"),
            "target_name": "nine_member_combo",
            "enabled": True,
            "lifecycle_status": "ACTIVE",
            "profit_gate_passed": True,
            "constraint_passed": True,
            "member_sleeve_risk_multiplier": Decimal("1.00000000"),
            "member_sleeves_source": [
                {
                    "strategy_key": key,
                    "strategy_version": "v1",
                    "current_strategy_version": "v1",
                    "version_match": True,
                    "original_weight": 1 / 9,
                    "member_lifecycle_status": "ACTIVE",
                    "member_lifecycle_multiplier": 1.0,
                }
                for key in sorted(members)
            ],
            "portfolio_risk_metrics": _portfolio_risk_metrics(0, "000001"),
        }
    }
    candidates, errors = health._allocation_candidate_contract(bindings)
    assert errors == []

    selected, rejected, _gate_hash = health._replay_global_portfolio_gate(
        [candidates]
    )
    allocations = health._expected_allocation_snapshot(
        candidates,
        market_state=MARKET_STATE,
        trading_gate_passed=True,
    )

    assert selected == []
    assert rejected[0]["funded_sleeve_count_after_admission"] == 9
    assert "超过单日上限" in rejected[0]["reason"]
    assert allocations == [{
        "target_type": "CASH",
        "target_key": "cash",
        "target_version": "",
        "funding_gate_hash": "",
        "market_state": MARKET_STATE,
        "market_match_score": 0.0,
        "router_decision_hash": "",
        "lifecycle_status": "",
        "lifecycle_status_label": "",
        "lifecycle_risk_multiplier": 0.0,
        "base_competitive_weight_pct": 0.0,
        "simulated_weight_pct": 100.0,
        "member_sleeves": [],
        "member_sleeve_hash": "",
        "cash_discount_bp": 0,
        "real_order_authority": False,
    }]


def _v3_combination_allocation_fixture():
    member_specs = (
        ("member_active", "ACTIVE", Decimal("0.60000000")),
        ("member_reduce", "REDUCE", Decimal("0.40000000")),
    )
    bindings = {}
    for key, status, _weight in member_specs:
        bindings[("STRATEGY", key, "v1")] = {
            "router_decision_hash": health._canonical_digest(
                {"route": key}
            ),
            "market_match_score": Decimal("100.0000"),
            "market_state": MARKET_STATE,
            "eligible": True,
            "paper_allocation_eligible": True,
            "funding_gate_hash": health._canonical_digest({"gate": key}),
            "members": frozenset({key}),
            "ranking_score": Decimal("95.0000"),
            "target_name": key,
            "enabled": True,
            "lifecycle_status": status,
            "profit_gate_passed": True,
            "constraint_passed": True,
            "portfolio_risk_metrics": _portfolio_risk_metrics(
                0 if key == "member_active" else 1,
                "000001" if key == "member_active" else "000002",
            ),
        }
    bindings[("STRATEGY", "standalone", "v1")] = {
        "router_decision_hash": health._canonical_digest(
            {"route": "standalone"}
        ),
        "market_match_score": Decimal("100.0000"),
        "market_state": MARKET_STATE,
        "eligible": True,
        "paper_allocation_eligible": True,
        "funding_gate_hash": health._canonical_digest(
            {"gate": "standalone"}
        ),
        "members": frozenset({"standalone"}),
        "ranking_score": Decimal("5.0000"),
        "target_name": "standalone",
        "enabled": True,
        "lifecycle_status": "ACTIVE",
        "profit_gate_passed": True,
        "constraint_passed": True,
        "portfolio_risk_metrics": _portfolio_risk_metrics(3, "000003"),
    }
    bindings[("COMBINATION", "combo_reduce_member", "v1")] = {
        "router_decision_hash": health._canonical_digest(
            {"route": "combo_reduce_member"}
        ),
        "market_match_score": Decimal("100.0000"),
        "market_state": MARKET_STATE,
        "eligible": True,
        "paper_allocation_eligible": True,
        "funding_gate_hash": health._canonical_digest(
            {"gate": "combo_reduce_member"}
        ),
        "members": frozenset(key for key, _status, _weight in member_specs),
        "ranking_score": Decimal("100.0000"),
        "target_name": "combo_reduce_member",
        "enabled": True,
        "lifecycle_status": "ACTIVE",
        "profit_gate_passed": True,
        "constraint_passed": True,
        "member_sleeve_risk_multiplier": Decimal("0.80000000"),
        "member_sleeves_source": [
            {
                "strategy_key": key,
                "strategy_version": "v1",
                "current_strategy_version": "v1",
                "version_match": True,
                "original_weight": float(weight),
                "member_lifecycle_status": status,
                "member_lifecycle_multiplier": (
                    1.0 if status == "ACTIVE" else 0.5
                ),
            }
            for key, status, weight in member_specs
        ],
        "portfolio_risk_metrics": _portfolio_risk_metrics(10, "000010"),
    }
    candidates, errors = health._allocation_candidate_contract(bindings)
    assert errors == []
    allocations = health._expected_allocation_snapshot(
        candidates,
        market_state=MARKET_STATE,
        trading_gate_passed=True,
    )
    run = {
        "trade_date": TRADE_DATE,
        "market_state": MARKET_STATE,
        "build_commit_sha": BUILD_SHA,
        "input_hash": "c" * 64,
        "router_snapshot_hash": "d" * 64,
        "decision_hash": "",
        "summary_json": {
            "allocation_policy_version": health.ALLOCATION_POLICY_VERSION,
            "trading_gate_passed": True,
            "market_risk_cap_pct": 85.0,
            "allocation_candidate_count": len(candidates),
            "eligible_candidate_count": len(candidates),
            "allocation_count": 2,
            "pool_row_count": 0,
            "pool_snapshot_hash": "e" * 64,
            "automatic_transition_count": 0,
            "automatic_transition_plan_hash": "f" * 64,
            "cash_weight_pct": next(
                row["simulated_weight_pct"]
                for row in allocations
                if row["target_type"] == "CASH"
            ),
        },
    }
    _passed, detail, _rows = health._allocation_decision_contract_check(
        run, bindings, allocations, TRADE_DATE
    )
    run["summary_json"].update(
        {
            "candidate_set_hash": detail["expected_candidate_set_hash"],
            "allocation_snapshot_hash": detail[
                "expected_allocation_snapshot_hash"
            ],
        }
    )
    run["decision_hash"] = detail["expected_decision_hash"]
    passed, detail, replayed = health._allocation_decision_contract_check(
        run, bindings, allocations, TRADE_DATE
    )
    assert passed is True
    assert detail["errors"] == []
    assert replayed == allocations
    return bindings, run, allocations


def test_v5_allocation_uses_unified_quality_lane_and_reduce_member_sleeves():
    _bindings, _run, allocations = _v3_combination_allocation_fixture()

    combination = next(
        row for row in allocations if row["target_type"] == "COMBINATION"
    )
    standalone = next(
        row for row in allocations if row["target_key"] == "standalone"
    )
    cash = next(row for row in allocations if row["target_type"] == "CASH")

    assert combination["base_competitive_weight_pct"] == 80.95
    assert standalone["base_competitive_weight_pct"] == 4.05
    assert combination["simulated_weight_pct"] == 64.76
    assert combination["cash_discount_bp"] == 1619
    assert cash["simulated_weight_pct"] == 31.19
    assert [
        (row["strategy_key"], row["base_bp"], row["effective_bp"])
        for row in combination["member_sleeves"]
    ] == [
        ("member_active", 4857, 4857),
        ("member_reduce", 3238, 1619),
    ]


def _legacy_v4_allocation_contract_fixture():
    bindings, _v5_run, _v5_allocations = (
        _v3_combination_allocation_fixture()
    )
    candidates, errors = health._allocation_candidate_contract(bindings)
    assert errors == []
    allocations = health._expected_allocation_snapshot(
        candidates,
        market_state=MARKET_STATE,
        trading_gate_passed=True,
        allocation_policy_version=health.LEGACY_ALLOCATION_POLICY_VERSION,
    )
    run = {
        "trade_date": TRADE_DATE,
        "market_state": MARKET_STATE,
        "build_commit_sha": BUILD_SHA,
        "finished_at": f"{TRADE_DATE} 12:00:00",
        "input_hash": "c" * 64,
        "router_snapshot_hash": "d" * 64,
        "decision_hash": "",
        "summary_json": {
            "allocation_policy_version": (
                health.LEGACY_ALLOCATION_POLICY_VERSION
            ),
            "trading_gate_passed": True,
            "market_risk_cap_pct": 85.0,
            "allocation_candidate_count": len(candidates),
            "eligible_candidate_count": len(candidates),
            "allocation_count": sum(
                row["target_type"] != "CASH" for row in allocations
            ),
            "pool_row_count": 0,
            "pool_snapshot_hash": "e" * 64,
            "automatic_transition_count": 0,
            "automatic_transition_plan_hash": "f" * 64,
            "cash_weight_pct": next(
                row["simulated_weight_pct"]
                for row in allocations
                if row["target_type"] == "CASH"
            ),
        },
    }
    replay_kwargs = {
        "current_build_commit_sha": "b" * 40,
        "completed_v5_canonical_count": 0,
    }
    _passed, detail, _rows = health._allocation_decision_contract_check(
        run, bindings, allocations, TRADE_DATE, **replay_kwargs
    )
    run["summary_json"].update({
        "candidate_set_hash": detail["expected_candidate_set_hash"],
        "allocation_snapshot_hash": detail[
            "expected_allocation_snapshot_hash"
        ],
    })
    run["decision_hash"] = detail["expected_decision_hash"]
    return bindings, run, allocations, replay_kwargs


def test_legacy_v4_canonical_replay_is_bounded_to_pre_v5_build():
    bindings, run, allocations, replay_kwargs = (
        _legacy_v4_allocation_contract_fixture()
    )

    passed, detail, replayed = health._allocation_decision_contract_check(
        run, bindings, allocations, TRADE_DATE, **replay_kwargs
    )

    assert passed is True
    assert detail["legacy_v4_allowed"] is True
    assert replayed == allocations
    assert detail["errors"] == []


@pytest.mark.parametrize(
    ("override", "run_override"),
    (
        ({"current_build_commit_sha": BUILD_SHA}, {}),
        ({"completed_v5_canonical_count": 1}, {}),
        (
            {},
            {
                "trade_date": health.ALLOCATION_POLICY_V5_EFFECTIVE_TRADE_DATE,
                "finished_at": (
                    health.ALLOCATION_POLICY_V5_EFFECTIVE_TRADE_DATE
                    + " 00:00:00"
                ),
            },
        ),
        (
            {},
            {
                "finished_at": (
                    health.ALLOCATION_POLICY_V5_EFFECTIVE_TRADE_DATE
                    + " 00:00:00"
                ),
            },
        ),
    ),
    ids=("current_build", "v5_already_canonical", "new_date", "late_rebuild"),
)
def test_legacy_v4_canonical_cannot_be_reintroduced(
    override, run_override,
):
    bindings, run, allocations, replay_kwargs = (
        _legacy_v4_allocation_contract_fixture()
    )
    run.update(run_override)
    replay_kwargs.update(override)

    passed, detail, _rows = health._allocation_decision_contract_check(
        run,
        bindings,
        allocations,
        str(run.get("trade_date") or TRADE_DATE),
        **replay_kwargs,
    )

    assert passed is False
    assert detail["legacy_v4_allowed"] is False
    assert any(
        error["reason"] == "allocation summary contract differs"
        for error in detail["errors"]
    )


def test_health_v3_allocation_replay_matches_runtime_normative_contract():
    from server.engine import strategy_governance as governance_module

    bindings, _run, expected = _v3_combination_allocation_fixture()
    candidates, errors = health._allocation_candidate_contract(bindings)
    assert errors == []
    runtime_allocations = governance_module._allocation(
        [],
        [],
        MARKET_STATE,
        trading_allowed=True,
        candidate_contract=deepcopy(candidates),
        trading_gate={"status": "OPEN", "reason": "fixture"},
    )
    runtime_snapshot = governance_module._allocation_snapshot_contract(
        runtime_allocations
    )
    stored_rows = [
        {
            **{
                key: value
                for key, value in row.items()
                if key != "member_sleeves"
            },
            "member_sleeves_json": json.dumps(
                row["member_sleeves"],
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
        }
        for row in runtime_snapshot
    ]

    assert health.ALLOCATION_POLICY_VERSION == (
        governance_module.ALLOCATION_POLICY_VERSION
    )
    assert health.DAILY_NAV_RANKING_BASIS == (
        governance_module.DAILY_NAV_RANKING_BASIS
    )
    assert health.ALLOCATION_TYPE_LANE_POLICY == (
        governance_module.ALLOCATION_TYPE_LANE_POLICY
    )
    assert runtime_snapshot == expected
    assert health._stored_allocation_snapshot(stored_rows) == expected


_MEMBER_SLEEVE_TAMPERS = {
    "strategy_key": "tampered_key",
    "strategy_version": "tampered_version",
    "current_strategy_version": "tampered_current_version",
    "original_weight": "0.61000000",
    "configured_weight_pct": 61.0,
    "base_bp": 2549,
    "base_weight_pct": 25.49,
    "member_lifecycle_status": "REDUCE",
    "member_lifecycle_multiplier": "0.50000000",
    "member_multiplier": 0.5,
    "combination_lifecycle_status": "REDUCE",
    "combination_lifecycle_multiplier": "0.50000000",
    "combination_multiplier": 0.5,
    "effective_bp": 2549,
    "effective_weight_pct": 25.49,
    "cash_discount_bp": 1,
    "discount_to_cash_pct": 0.01,
    "sleeve_row_hash": "0" * 64,
}


@pytest.mark.parametrize(
    ("field", "tampered_value"),
    list(_MEMBER_SLEEVE_TAMPERS.items()),
    ids=list(_MEMBER_SLEEVE_TAMPERS),
)
def test_v3_allocation_replay_rejects_every_tampered_member_sleeve_field(
    field,
    tampered_value,
):
    bindings, run, allocations = _v3_combination_allocation_fixture()
    tampered = deepcopy(allocations)
    combination = next(
        row for row in tampered if row["target_type"] == "COMBINATION"
    )
    combination["member_sleeves"][0][field] = tampered_value

    passed, detail, _replayed = health._allocation_decision_contract_check(
        run, bindings, tampered, TRADE_DATE
    )

    assert passed is False
    assert any(
        error["reason"] == "persisted allocation replay differs"
        for error in detail["errors"]
    )


@pytest.mark.parametrize(
    ("field", "tampered_value"),
    (("member_sleeve_hash", "0" * 64), ("cash_discount_bp", 849)),
)
def test_v3_allocation_replay_rejects_tampered_sleeve_aggregate_fields(
    field,
    tampered_value,
):
    bindings, run, allocations = _v3_combination_allocation_fixture()
    tampered = deepcopy(allocations)
    combination = next(
        row for row in tampered if row["target_type"] == "COMBINATION"
    )
    combination[field] = tampered_value

    passed, detail, _replayed = health._allocation_decision_contract_check(
        run, bindings, tampered, TRADE_DATE
    )

    assert passed is False
    assert any(
        error["reason"] == "persisted allocation replay differs"
        for error in detail["errors"]
    )


def test_v3_replay_rejects_self_consistent_rehashed_member_sleeve_forgery():
    bindings, run, allocations = _v3_combination_allocation_fixture()
    tampered = deepcopy(allocations)
    combination = next(
        row for row in tampered if row["target_type"] == "COMBINATION"
    )
    sleeve = combination["member_sleeves"][0]
    sleeve.update(
        {
            "member_lifecycle_status": "REDUCE",
            "member_lifecycle_multiplier": "0.50000000",
            "member_multiplier": 0.5,
            "effective_bp": 1275,
            "effective_weight_pct": 12.75,
            "cash_discount_bp": 1275,
            "discount_to_cash_pct": 12.75,
        }
    )
    sleeve["sleeve_row_hash"] = health._canonical_digest(
        {
            "schema": "probiga.strategy-combination-member-sleeve-row.v1",
            **{
                key: value
                for key, value in sleeve.items()
                if key != "sleeve_row_hash"
            },
        }
    )
    combination["simulated_weight_pct"] = 21.25
    combination["cash_discount_bp"] = 2125
    combination["member_sleeve_hash"] = health._canonical_digest(
        {
            "schema": "probiga.strategy-combination-member-sleeves.v1",
            "combination_key": combination["target_key"],
            "combination_version": combination["target_version"],
            "base_bp": 4250,
            "effective_bp": 2125,
            "cash_discount_bp": 2125,
            "members": combination["member_sleeves"],
        }
    )
    cash = next(row for row in tampered if row["target_type"] == "CASH")
    cash["simulated_weight_pct"] = 36.25

    forged_run = deepcopy(run)
    normalized_tampered = health._stored_allocation_snapshot(tampered)
    forged_allocation_hash = health._canonical_digest(
        {
            "schema": "probiga.strategy-allocation-snapshot.v1",
            "allocation_policy_version": health.ALLOCATION_POLICY_VERSION,
            "trade_date": TRADE_DATE,
            "market_state": MARKET_STATE,
            "market_risk_cap_pct": 85.0,
            "trading_gate_passed": True,
            "candidate_set_hash": forged_run["summary_json"][
                "candidate_set_hash"
            ],
            "allocations": normalized_tampered,
        }
    )
    forged_run["summary_json"].update(
        {
            "allocation_snapshot_hash": forged_allocation_hash,
            "cash_weight_pct": 36.25,
        }
    )
    _passed, detail, _rows = health._allocation_decision_contract_check(
        forged_run, bindings, tampered, TRADE_DATE
    )
    forged_run["decision_hash"] = detail["expected_decision_hash"]

    passed, detail, _rows = health._allocation_decision_contract_check(
        forged_run, bindings, tampered, TRADE_DATE
    )

    assert passed is False
    assert detail["stored_allocation_snapshot_hash"] == (
        forged_allocation_hash
    )
    assert any(
        error["reason"] == "persisted allocation replay differs"
        for error in detail["errors"]
    )


def test_health_rejects_reduce_weight_redistributed_back_to_risk_cap(
    monkeypatch,
):
    _fixed_trade_date(monkeypatch)

    class _RedistributedReduceEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if "FROM st_strategy_allocation_snapshot a" in sql:
                _cash, strategy = result._rows
                strategy.update(
                    {
                        "strategy_current_status": "REDUCE",
                        "lifecycle_status": "REDUCE",
                        "lifecycle_status_label": "降权运行",
                        "lifecycle_risk_multiplier": Decimal("0.5000"),
                    }
                )
            return result

    result = health.collect_governance_health(
        _RedistributedReduceEngine(runs=[_completed_run()]),
        expected_build_sha=BUILD_SHA,
    )

    assert result["status"] == "FAIL"
    lifecycle_budget = next(
        check
        for check in result["checks"]
        if check["name"] == "allocation_lifecycle_budget_exact"
    )
    assert lifecycle_budget["passed"] is False
    assert lifecycle_budget["detail"]["errors"]


def test_competitive_score_columns_must_match_hash_bound_snapshots(
    monkeypatch,
):
    _fixed_trade_date(monkeypatch)

    for target in ("strategy", "combination"):
        class _DriftedScoreEngine(_GovernanceHealthEngine):
            def execute(self, sql, params):
                result = super().execute(sql, params)
                if (
                    target == "strategy"
                    and "SELECT h.strategy_key" in sql
                ):
                    result._rows[0]["health_score"] = Decimal("81.0000")
                if (
                    target == "combination"
                    and "SELECT h.combination_key" in sql
                ):
                    result._rows[0]["ranking_score"] = Decimal("81.0000")
                return result

        result = health.collect_governance_health(
            _DriftedScoreEngine(runs=[_completed_run()]),
            expected_build_sha=BUILD_SHA,
        )

        assert result["status"] == "FAIL"
        router = next(
            check
            for check in result["checks"]
            if check["name"] == "market_router_snapshot_is_reproducible"
        )
        assert router["passed"] is False
        assert any(
            "score column differs" in error["reason"]
            for error in router["detail"]["errors"]
        )


def test_input_not_ready_waives_only_missing_run(monkeypatch):
    _fixed_trade_date(monkeypatch)
    result = health.collect_governance_health(
        _GovernanceHealthEngine(runs=[]),
        expected_build_sha=BUILD_SHA,
        allow_input_not_ready=True,
    )
    assert result["status"] == "PASS"
    assert result["run_disposition"] == "input_not_ready"
    assert {check["name"] for check in result["checks"]} == set(
        health.governance_health_required_check_names("input_not_ready")
    )
    waived = [check for check in result["checks"] if check["waived"]]
    assert [check["name"] for check in waived] == [
        "authoritative_date_has_one_canonical_revision",
        "expected_build_date_run"
    ]
    assert all(
        check["passed"]
        for check in result["checks"]
        if check["name"].startswith("schema_")
        or check["name"].startswith("daily_scheduler_")
        or check["name"] == "required_tables"
    )


def test_input_not_ready_without_authoritative_date_still_fails_closed(
    monkeypatch,
):
    monkeypatch.setattr(
        health,
        "_authoritative_trade_date",
        lambda _engine, _explicit="": (
            "",
            "authoritative_closed_trading_calendar_day",
        ),
    )

    result = health.collect_governance_health(
        _GovernanceHealthEngine(runs=[]),
        expected_build_sha=BUILD_SHA,
        allow_input_not_ready=True,
    )

    assert result["status"] == "FAIL"
    assert result["run_disposition"] == "input_not_ready"
    checks = {check["name"]: check for check in result["checks"]}
    assert checks["authoritative_trade_date"]["passed"] is True
    assert checks["authoritative_trade_date"]["waived"] is True
    assert [
        check["name"] for check in result["checks"] if check["waived"]
    ] == [
        "authoritative_trade_date",
        "authoritative_date_has_one_canonical_revision",
        "expected_build_date_run",
    ]
    for name in (
        "authoritative_session_windows_qmt_close_attested",
        "qmt_pre_close_v2_rows_bind_current_kline",
    ):
        assert checks[name]["passed"] is False
        assert checks[name]["waived"] is False


def test_input_not_ready_cannot_waive_missing_qmt_row_attestations(
    monkeypatch,
):
    _fixed_trade_date(monkeypatch)
    monkeypatch.setattr(
        health,
        "_qmt_row_attestation_binding_check",
        lambda _trade_date, _detail: (
            False,
            {
                "table_exists": False,
                "error": "batch-only legacy attestation",
            },
        ),
    )

    result = health.collect_governance_health(
        _GovernanceHealthEngine(runs=[]),
        expected_build_sha=BUILD_SHA,
        allow_input_not_ready=True,
    )

    assert result["status"] == "FAIL"
    check = next(
        item
        for item in result["checks"]
        if item["name"] == "qmt_pre_close_v2_rows_bind_current_kline"
    )
    assert check["passed"] is False
    assert check["waived"] is False


def test_input_not_ready_cannot_waive_qmt_frozen_schema_drift(
    monkeypatch,
):
    _fixed_trade_date(monkeypatch)
    monkeypatch.setattr(
        health,
        "_qmt_attestation_frozen_schema_check",
        lambda _connection: (
            False,
            {"errors": ["immutable unique-index inventory differs"]},
        ),
    )

    result = health.collect_governance_health(
        _GovernanceHealthEngine(runs=[]),
        expected_build_sha=BUILD_SHA,
        allow_input_not_ready=True,
    )

    assert result["status"] == "FAIL"
    assert result["run_disposition"] == "schema_invalid"
    check = next(
        item
        for item in result["checks"]
        if item["name"] == "qmt_pre_close_v2_frozen_schema"
    )
    assert check["passed"] is False
    assert check["waived"] is False


def test_blocked_with_history_fails_missing_run_but_fully_replays_baseline(
    monkeypatch,
):
    _blocked_after_historical_run(monkeypatch)
    result = health.collect_governance_health(
        _GovernanceHealthEngine(runs=[_completed_run()]),
        expected_build_sha="d" * 40,
        allow_input_not_ready=True,
    )

    assert result["status"] == "FAIL"
    assert (
        result["run_disposition"]
        == "required_run_missing_historical_baseline"
    )
    checks = {check["name"]: check for check in result["checks"]}
    assert checks["expected_build_date_run"]["passed"] is False
    assert checks["expected_build_date_run"]["waived"] is False
    assert checks["historical_baseline_run_identity"]["passed"] is True
    for name in (
        "latest_completed_run_has_hash_valid_audit",
        "completed_run_has_hash_valid_audit",
        "strategy_health_three_windows",
        "combination_health_one_snapshot_each",
        "market_router_snapshot_is_reproducible",
        "allocation_candidate_snapshot_and_decision_hashes",
        "paper_allocation_exactly_closed",
        "global_real_order_authority_closed",
    ):
        assert checks[name]["passed"] is True


def test_blocked_with_history_rejects_tampered_old_health_snapshot(
    monkeypatch,
):
    _blocked_after_historical_run(monkeypatch)

    class _TamperedHistoricalHealthEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if "SELECT h.strategy_key, h.strategy_version" in sql:
                result._rows[0]["result_hash"] = "0" * 64
            return result

    result = health.collect_governance_health(
        _TamperedHistoricalHealthEngine(runs=[_completed_run()]),
        expected_build_sha="d" * 40,
        allow_input_not_ready=True,
    )

    checks = {check["name"]: check for check in result["checks"]}
    assert result["status"] == "FAIL"
    assert checks["market_router_snapshot_is_reproducible"]["passed"] is False
    assert any(
        "snapshot payload/result hash/route is invalid"
        in item["reason"]
        for item in checks["market_router_snapshot_is_reproducible"][
            "detail"
        ]["errors"]
    )


@pytest.mark.parametrize(
    ("entity_type", "expected_path"),
    (
        (
            "STRATEGY",
            "strategies.STRATEGY|strategy_a|v1|20.metrics.completed_trades",
        ),
        (
            "COMBINATION",
            "combinations.COMBINATION|combo_a|v1.metrics.20.completed_trades",
        ),
    ),
)
def test_health_recomputes_window_gate_instead_of_trusting_rehashed_payload(
    monkeypatch, entity_type, expected_path,
):
    _fixed_trade_date(monkeypatch)

    class _ForgedWindowGateEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if (
                entity_type == "STRATEGY"
                and "FROM st_strategy_health_snapshot" in sql
            ):
                for row in result._rows:
                    payload = row.get("evidence_json")
                    if (
                        row.get("strategy_key") == "strategy_a"
                        and row.get("window_days") == 20
                        and isinstance(payload, dict)
                        and isinstance(payload.get("metrics"), dict)
                    ):
                        payload["metrics"]["completed_trades"] = 19
                        row["result_hash"] = health._canonical_digest(
                            payload
                        )
            if (
                entity_type == "COMBINATION"
                and "FROM st_strategy_combination_health_snapshot" in sql
            ):
                for row in result._rows:
                    payload = row.get("evidence_json")
                    if (
                        row.get("combination_key") == "combo_a"
                        and isinstance(payload, dict)
                        and isinstance(payload.get("metrics"), dict)
                        and isinstance(payload["metrics"].get("20"), dict)
                    ):
                        payload["metrics"]["20"][
                            "completed_trades"
                        ] = 19
                        row["result_hash"] = health._canonical_digest(
                            payload
                        )
            return result

    result = health.collect_governance_health(
        _ForgedWindowGateEngine(runs=[_completed_run()]),
        expected_build_sha=BUILD_SHA,
    )

    checks = {check["name"]: check for check in result["checks"]}
    assert result["status"] == "FAIL"
    replay_check = checks[
        "current_canonical_metrics_replay_from_raw_ledgers"
    ]
    assert replay_check["passed"] is False
    assert any(
        item["path"] == expected_path
        and item["reason"] == "raw-ledger replay value differs"
        for item in replay_check["detail"]["errors"]
    )


def test_health_rejects_window_gate_database_column_drift(monkeypatch):
    _fixed_trade_date(monkeypatch)

    class _WindowGateColumnDriftEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if "SELECT h.strategy_key, h.strategy_version" in sql:
                row = next(
                    item
                    for item in result._rows
                    if item["strategy_key"] == "strategy_a"
                    and item["window_days"] == 20
                )
                row["profit_gate_passed"] = 0
            return result

    result = health.collect_governance_health(
        _WindowGateColumnDriftEngine(runs=[_completed_run()]),
        expected_build_sha=BUILD_SHA,
    )

    router_check = next(
        check
        for check in result["checks"]
        if check["name"] == "market_router_snapshot_is_reproducible"
    )
    assert result["status"] == "FAIL"
    assert router_check["passed"] is False
    assert any(
        item["reason"]
        == "persisted v7 compact window/full gate binding differs"
        for item in router_check["detail"]["errors"]
    )


def test_health_recomputes_strategy_overall_gate_from_all_three_windows(
    monkeypatch,
):
    _fixed_trade_date(monkeypatch)
    forged_pre_gate_hash = health._canonical_digest(
        {
            "strategy_key": "strategy_a",
            "strategy_version": "v1",
            "window_evidence": STRATEGY_WINDOW_EVIDENCE["strategy_a"],
            "router_decision_hash": STRATEGY_ROUTES["strategy_a"][
                "router_decision_hash"
            ],
            "overall_gate_passed": False,
        }
    )
    forged_final_gate_hash = _final_funding_hash(
        entity_type="STRATEGY",
        entity_key="strategy_a",
        pre_gate_hash=forged_pre_gate_hash,
        decision=STRATEGY_STATISTICAL_DECISIONS["strategy_a"],
        confirmation=STRATEGY_CONFIRMATIONS["strategy_a"],
        projected_status="SHADOW",
        paper_eligible=False,
    )

    class _ForgedOverallGateEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if "FROM st_strategy_health_snapshot" in sql:
                for row in result._rows:
                    if row.get("strategy_key") != "strategy_a":
                        continue
                    payload = row.get("evidence_json")
                    if not isinstance(payload, dict):
                        continue
                    payload["overall_profit_gate_passed"] = False
                    payload["paper_allocation_eligible"] = False
                    payload["pre_confirmation_funding_gate_hash"] = (
                        forged_pre_gate_hash
                    )
                    payload["funding_gate_hash"] = forged_final_gate_hash
                    if "metrics" in payload:
                        row["recommended_status"] = "SHADOW"
                        row["result_hash"] = health._canonical_digest(
                            payload
                        )
            return result

    result = health.collect_governance_health(
        _ForgedOverallGateEngine(runs=[_completed_run()]),
        expected_build_sha=BUILD_SHA,
    )

    replay_check = next(
        check for check in result["checks"]
        if check["name"]
        == "current_canonical_metrics_replay_from_raw_ledgers"
    )
    assert result["status"] == "FAIL"
    assert replay_check["passed"] is False
    assert any(
        item["path"].endswith("overall_profit_gate_passed")
        and item["reason"] == "raw-ledger replay value differs"
        for item in replay_check["detail"]["errors"]
    )


def test_blocked_with_history_rejects_tampered_old_allocation(
    monkeypatch,
):
    _blocked_after_historical_run(monkeypatch)

    class _TamperedHistoricalAllocationEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if "FROM st_strategy_allocation_snapshot a" in sql:
                result._rows[1]["simulated_weight_pct"] = Decimal(
                    "84.0000"
                )
            elif "FROM st_strategy_allocation_snapshot" in sql:
                result._rows[0]["weight_sum"] = Decimal("99.0000")
            return result

    result = health.collect_governance_health(
        _TamperedHistoricalAllocationEngine(runs=[_completed_run()]),
        expected_build_sha="d" * 40,
        allow_input_not_ready=True,
    )

    checks = {check["name"]: check for check in result["checks"]}
    assert result["status"] == "FAIL"
    assert (
        checks["allocation_candidate_snapshot_and_decision_hashes"][
            "passed"
        ]
        is False
    )
    assert checks["paper_allocation_exactly_closed"]["passed"] is False


def test_blocked_with_history_rejects_tampered_old_decision_hash(
    monkeypatch,
):
    _blocked_after_historical_run(monkeypatch)
    historical = _completed_run()
    historical["decision_hash"] = "d" * 64

    result = health.collect_governance_health(
        _GovernanceHealthEngine(runs=[historical]),
        expected_build_sha="e" * 40,
        allow_input_not_ready=True,
    )

    checks = {check["name"]: check for check in result["checks"]}
    assert result["status"] == "FAIL"
    assert checks["expected_run_completed"]["passed"] is True
    assert checks["completed_run_has_hash_valid_audit"]["passed"] is True
    assert (
        checks["allocation_candidate_snapshot_and_decision_hashes"][
            "passed"
        ]
        is False
    )
    assert checks["allocation_candidate_snapshot_and_decision_hashes"][
        "detail"
    ]["stored_decision_hash"] == "d" * 64


def test_blocked_with_history_rejects_current_registry_version_drift(
    monkeypatch,
):
    _blocked_after_historical_run(monkeypatch)

    class _HistoricalRegistryDriftEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if "SELECT h.strategy_key, h.strategy_version" in sql:
                result._rows[0]["registry_current_version"] = "v2"
            return result

    result = health.collect_governance_health(
        _HistoricalRegistryDriftEngine(runs=[_completed_run()]),
        expected_build_sha="d" * 40,
        allow_input_not_ready=True,
    )

    checks = {check["name"]: check for check in result["checks"]}
    assert result["status"] == "FAIL"
    assert checks["market_router_snapshot_is_reproducible"]["passed"] is False
    assert any(
        item["reason"] == "current strategy registry binding is invalid"
        for item in checks["market_router_snapshot_is_reproducible"][
            "detail"
        ]["errors"]
    )


def test_input_not_ready_cannot_waive_calendar_permission_failure(
    monkeypatch,
):
    def denied(_engine, _explicit=""):
        raise PermissionError("calendar read denied")

    monkeypatch.setattr(health, "_authoritative_trade_date", denied)
    result = health.collect_governance_health(
        _GovernanceHealthEngine(runs=[]),
        expected_build_sha=BUILD_SHA,
        allow_input_not_ready=True,
    )

    assert result["status"] == "FAIL"
    authoritative = next(
        check
        for check in result["checks"]
        if check["name"] == "authoritative_trade_date"
    )
    assert authoritative["passed"] is False
    assert authoritative["waived"] is False
    assert "PermissionError" in authoritative["detail"]["error"]


def test_input_not_ready_flag_cannot_hide_malformed_existing_run(monkeypatch):
    _fixed_trade_date(monkeypatch)
    malformed = deepcopy(_completed_run())
    malformed.update({"source_status": "stale", "input_ready": 0})
    result = health.collect_governance_health(
        _GovernanceHealthEngine(runs=[malformed]),
        expected_build_sha=BUILD_SHA,
        allow_input_not_ready=True,
    )
    assert result["status"] == "FAIL"
    source_check = next(
        check
        for check in result["checks"]
        if check["name"] == "expected_run_input_fresh"
    )
    assert source_check["passed"] is False
    assert source_check["waived"] is False


def test_input_not_ready_cannot_hide_noncanonical_row_from_same_build(
    monkeypatch,
):
    _fixed_trade_date(monkeypatch)
    stale_build_revision = deepcopy(_completed_run())
    stale_build_revision.update(
        {
            "is_canonical": 0,
            "source_status": "stale",
            "input_ready": 0,
        }
    )
    canonical_other_build = deepcopy(_completed_run())
    canonical_other_build.update(
        {
            "run_uid": "e" * 32,
            "run_revision": 2,
            "supersedes_run_uid": stale_build_revision["run_uid"],
            "build_commit_sha": "f" * 40,
            "created_at": "2026-08-21 22:36:00",
            "finished_at": "2026-08-21 22:36:10",
        }
    )

    result = health.collect_governance_health(
        _GovernanceHealthEngine(
            runs=[stale_build_revision, canonical_other_build]
        ),
        expected_build_sha=BUILD_SHA,
        allow_input_not_ready=True,
    )

    assert result["status"] == "FAIL"
    expected_run = next(
        check
        for check in result["checks"]
        if check["name"] == "expected_build_date_run"
    )
    assert expected_run["passed"] is False
    assert expected_run["waived"] is False
    assert expected_run["detail"]["matching_build_date_run_count"] == 1


def test_input_not_ready_requires_latest_successful_run_audit(monkeypatch):
    _fixed_trade_date(monkeypatch)
    previous_build_run = deepcopy(_completed_run())
    previous_build_run["build_commit_sha"] = "f" * 40

    class _MissingLatestAuditEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            if "FROM st_strategy_governance_audit" in sql:
                return _Result([])
            return super().execute(sql, params)

    result = health.collect_governance_health(
        _MissingLatestAuditEngine(runs=[previous_build_run]),
        expected_build_sha=BUILD_SHA,
        allow_input_not_ready=True,
    )

    assert result["status"] == "FAIL"
    audit = next(
        check
        for check in result["checks"]
        if check["name"] == "latest_completed_run_has_hash_valid_audit"
    )
    assert audit["passed"] is False
    assert audit["waived"] is False


def test_exact_run_must_also_be_latest_completed_run(monkeypatch):
    _fixed_trade_date(monkeypatch)
    later = deepcopy(_completed_run())
    later.update(
        {
            "run_uid": "f" * 32,
            "trade_date": "2026-08-22",
            "build_commit_sha": "e" * 40,
            "created_at": "2026-08-22 22:35:00",
            "finished_at": "2026-08-22 22:35:10",
        }
    )
    result = health.collect_governance_health(
        _GovernanceHealthEngine(runs=[_completed_run(), later]),
        expected_build_sha=BUILD_SHA,
    )
    assert result["status"] == "FAIL"
    latest = next(
        check
        for check in result["checks"]
        if check["name"] == "latest_completed_run_identity"
    )
    assert latest["passed"] is False


def test_historical_revision_is_allowed_when_latest_is_only_canonical(
    monkeypatch,
):
    _fixed_trade_date(monkeypatch)
    previous = deepcopy(_completed_run())
    previous.update(
        {
            "run_uid": "e" * 32,
            "run_revision": 1,
            "is_canonical": 0,
            "build_commit_sha": "f" * 40,
            "created_at": "2026-08-21 22:34:00",
            "finished_at": "2026-08-21 22:34:10",
        }
    )
    current = deepcopy(_completed_run())
    current.update(
        {
            "run_revision": 2,
            "supersedes_run_uid": previous["run_uid"],
        }
    )
    result = health.collect_governance_health(
        _GovernanceHealthEngine(runs=[previous, current]),
        expected_build_sha=BUILD_SHA,
    )
    assert result["status"] == "PASS"
    revisions = next(
        check
        for check in result["checks"]
        if check["name"]
        == "authoritative_date_has_one_canonical_revision"
    )
    assert revisions["detail"]["revisions"] == [1, 2]


def test_two_canonical_revisions_for_one_day_fail(monkeypatch):
    _fixed_trade_date(monkeypatch)
    first = deepcopy(_completed_run())
    second = deepcopy(_completed_run())
    second.update(
        {
            "run_uid": "e" * 32,
            "run_revision": 2,
            "supersedes_run_uid": first["run_uid"],
        }
    )
    result = health.collect_governance_health(
        _GovernanceHealthEngine(runs=[first, second]),
        expected_build_sha=BUILD_SHA,
    )
    assert result["status"] == "FAIL"
    revisions = next(
        check
        for check in result["checks"]
        if check["name"]
        == "authoritative_date_has_one_canonical_revision"
    )
    assert revisions["passed"] is False


def test_scheduler_duplicates_fail_even_when_input_is_not_ready(monkeypatch):
    _fixed_trade_date(monkeypatch)
    duplicate = _GovernanceHealthEngine._valid_task()
    duplicate["id"] = 219
    result = health.collect_governance_health(
        _GovernanceHealthEngine(
            runs=[],
            tasks=[
                _GovernanceHealthEngine._valid_task(),
                duplicate,
            ],
        ),
        expected_build_sha=BUILD_SHA,
        allow_input_not_ready=True,
    )
    assert result["status"] == "FAIL"
    unique = next(
        check
        for check in result["checks"]
        if check["name"] == "daily_scheduler_task_unique"
    )
    assert unique["passed"] is False


def test_allow_input_not_ready_does_not_waive_hash_or_authority_failures(
    monkeypatch,
):
    _fixed_trade_date(monkeypatch)

    class _UnsafeBlockedEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if "FROM st_strategy_metric_input" in sql:
                result._rows[0]["invalid_contract_count"] = 1
            if "total_snapshot_count" in sql:
                result._rows[0]["forbidden_authority_count"] = 1
            return result

    result = health.collect_governance_health(
        _UnsafeBlockedEngine(runs=[]),
        expected_build_sha=BUILD_SHA,
        allow_input_not_ready=True,
    )
    assert result["status"] == "FAIL"
    checks = {check["name"]: check for check in result["checks"]}
    assert checks["expected_build_date_run"]["waived"] is True
    assert checks["metric_evidence_state_domain"]["passed"] is False
    assert checks["global_real_order_authority_closed"]["passed"] is False


def test_allow_input_not_ready_recomputes_all_immutable_hashes(
    monkeypatch,
):
    _fixed_trade_date(monkeypatch)

    for target in ("strategy", "combination"):
        class _DriftedCurrentVersionEngine(_GovernanceHealthEngine):
            def execute(self, sql, params):
                result = super().execute(sql, params)
                if (
                    target == "strategy"
                    and "SELECT strategy_key, version, version_hash, content_hash"
                    in sql
                ):
                    result._rows[0]["parameters_json"] = json.dumps(
                        {"unhashed_mutation": True}
                    )
                if (
                    target == "combination"
                    and "SELECT combination_key, version, members_json"
                    in sql
                ):
                    result._rows[0]["constraints_json"] = json.dumps(
                        {"unhashed_mutation": True}
                    )
                return result

        result = health.collect_governance_health(
            _DriftedCurrentVersionEngine(runs=[]),
            expected_build_sha=BUILD_SHA,
            allow_input_not_ready=True,
        )

        assert result["status"] == "FAIL"
        immutable = next(
            check
            for check in result["checks"]
            if check["name"] == "all_immutable_version_hashes"
        )
        assert immutable["passed"] is False
        assert immutable["detail"]["invalid_count"] == 1


def test_allow_input_not_ready_treats_null_authority_as_forbidden(
    monkeypatch,
):
    _fixed_trade_date(monkeypatch)

    class _NullAuthorityEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if "total_snapshot_count" in sql:
                assert "real_order_authority IS NULL" in sql
                result._rows[0]["forbidden_authority_count"] = 1
            return result

    result = health.collect_governance_health(
        _NullAuthorityEngine(runs=[]),
        expected_build_sha=BUILD_SHA,
        allow_input_not_ready=True,
    )

    assert result["status"] == "FAIL"
    authority = next(
        check
        for check in result["checks"]
        if check["name"] == "global_real_order_authority_closed"
    )
    assert authority["passed"] is False


def test_rds_schema_health_fails_closed_without_trigger_inventory(monkeypatch):
    _fixed_trade_date(monkeypatch)

    class _NoTriggerInventoryEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            assert "information_schema.triggers" not in str(sql).casefold()
            return super().execute(sql, params)

    result = health.collect_governance_health(
        _NoTriggerInventoryEngine(runs=[]),
        expected_build_sha=BUILD_SHA,
        allow_input_not_ready=True,
    )
    assert result["status"] == "FAIL"
    checks = {item["name"]: item for item in result["checks"]}
    assert checks[
        "strategy_metric_input_application_state_machine"
    ]["passed"] is False
    assert checks[
        "governance_append_only_application_integrity"
    ]["passed"] is False
    for name in (
        "forward_strategy_version_schema",
        "v2_raw_fill_cash_ledgers_are_immutable",
        "forward_exit_allocation_v3_frozen_schema",
    ):
        assert checks[name]["passed"] is True
        assert checks[name]["detail"]["database_triggers_required"] is False


def test_passing_snapshot_cannot_claim_unconfirmed_evidence(monkeypatch):
    _fixed_trade_date(monkeypatch)
    evidence_payload = {
        "strategy_key": "strategy_a",
        "strategy_version": "v1",
        "trade_date": TRADE_DATE,
        "window_days": 60,
        "metrics": {
            "verification_status": "PENDING",
            "evidence_hash": "e" * 64,
        },
        "gate": {"passed": True},
        "overall_profit_gate_passed": True,
        "funding_gate_hash": "7" * 64,
    }
    snapshot = {
        "entity_key": "strategy_a",
        "entity_version": "v1",
        "window_days": 60,
        "trade_date": TRADE_DATE,
        "profit_gate_passed": 1,
        "recommended_status": "ACTIVE",
        "evidence_json": evidence_payload,
        "result_hash": health._canonical_digest(evidence_payload),
    }
    result = health.collect_governance_health(
        _GovernanceHealthEngine(
            runs=[_completed_run()],
            strategy_evidence_rows=[snapshot],
        ),
        expected_build_sha=BUILD_SHA,
    )
    assert result["status"] == "FAIL"
    evidence_check = next(
        check
        for check in result["checks"]
        if check["name"] == "funding_snapshots_use_confirmed_evidence"
    )
    assert evidence_check["passed"] is False
    assert evidence_check["detail"]["invalid_count"] == 1


def test_health_rejects_forged_confirmed_review_fields_without_audit():
    from server.engine import strategy_governance as governance_module

    metric = {
        "evidence_id": "d" * 32,
        "entity_type": "STRATEGY",
        "strategy_key": "strategy_a",
        "strategy_version": "v1",
        "as_of_date": TRADE_DATE,
        "window_days": 60,
        "metrics_json": {},
        "source": "forged_direct_review",
        "evidence_protocol": "PURGED_WALK_FORWARD_V2",
        "artifact_hash": "a" * 64,
        "source_dataset_hash": "b" * 64,
        "evidence_revision_at": f"{TRADE_DATE}T15:00:00",
        "verification_status": "CONFIRMED",
        "funding_provenance": "EXTERNAL_SUBMITTED",
        "submitted_by": "user-id:1",
        "reviewed_by": "user-id:2",
        "reviewed_at": f"{TRADE_DATE}T16:00:00",
        "created_at": f"{TRADE_DATE}T15:01:00",
    }
    metric["evidence_hash"] = governance_module._digest(
        governance_module._metric_submission_contract(metric)
    )
    engine = _GovernanceHealthEngine(
        metric_rows=[metric], audit_rows=[],
    )

    ok, detail = health._metric_evidence_audit_history_check(
        engine.connect()
    )

    assert ok is False
    assert detail["invalid_count"] == 1
    assert detail["errors"][0]["detail"]["confirm_audit_count"] == 0


def test_passing_snapshot_binds_internal_and_confirmed_selection_hashes(
    monkeypatch,
):
    from server.engine import strategy_governance as governance_module

    stored_metrics, artifact, artifact_hash, revision_at, session_window = (
        _valid_external_artifact_fixture()
    )
    internal_trade_hash = "1" * 64
    internal_ledger_hash = "2" * 64
    dataset_hash = artifact["source_dataset_hash"]
    pending_payload = {
        "strategy_key": "strategy_a",
        "entity_type": "STRATEGY",
        "strategy_version": "v1",
        "as_of_date": TRADE_DATE,
        "window_days": 60,
        "metrics": stored_metrics,
        "source": "reviewed_selection_validation",
        "evidence_protocol": "PURGED_WALK_FORWARD_V2",
        "artifact_hash": artifact_hash,
        "source_dataset_hash": dataset_hash,
        "evidence_revision_at": revision_at,
        "verification_status": "PENDING",
        "funding_provenance": "EXTERNAL_SUBMITTED",
    }
    selection_hash = health._canonical_digest(pending_payload)
    composite_hash = health._canonical_digest(
        {
            "internal_trade_evidence_hash": internal_trade_hash,
            "internal_ledger_hash": internal_ledger_hash,
            "selection_evidence_hash": selection_hash,
            "selection_artifact_hash": artifact_hash,
            "strategy_key": "strategy_a",
            "strategy_version": "v1",
            "window_days": 60,
        }
    )
    embedded_metrics = {
        "window_days": 60,
        "evidence_hash": composite_hash,
        "internal_trade_evidence_hash": internal_trade_hash,
        "internal_ledger_hash": internal_ledger_hash,
        "selection_evidence_hash": selection_hash,
        "artifact_hash": artifact_hash,
        "source_dataset_hash": dataset_hash,
        "evidence_protocol": "PURGED_WALK_FORWARD_V2",
        "verification_status": "CONFIRMED",
        "funding_provenance": (
            "INTERNAL_PORTFOLIO_CHECKPOINT_FACT_LEDGER_V3"
        ),
        "drawdown_basis": "internal_version_bound_portfolio_equity",
        "cost_basis": "actual_ledger_fees",
        "submitted_by": "submitter",
        "reviewed_by": "reviewer",
        "reviewed_at": f"{TRADE_DATE}T16:00:00",
        "walk_forward_verified": True,
        "walk_forward_segments": 5,
        "positive_segments": 4,
        "selection_validation_completed_trades": 5,
        "selection_validation_coverage_days": 5,
        "selection_validation_revision_at": revision_at,
        "selection_validation_independent_oos": True,
        "selection_validation_scope": "VERSION_SELECTION_ONLY",
    }
    evidence_payload = {
        "strategy_key": "strategy_a",
        "strategy_version": "v1",
        "trade_date": TRADE_DATE,
        "window_days": 60,
        "metrics": embedded_metrics,
        "gate": {"passed": True},
        "overall_profit_gate_passed": True,
        "funding_gate_hash": "5" * 64,
    }
    snapshot = {
        "entity_key": "strategy_a",
        "entity_version": "v1",
        "window_days": 60,
        "trade_date": TRADE_DATE,
        "profit_gate_passed": 1,
        "recommended_status": "ACTIVE",
        "evidence_json": evidence_payload,
        "result_hash": health._canonical_digest(evidence_payload),
    }
    metric = {
        "evidence_id": "6" * 32,
        "entity_type": "STRATEGY",
        "strategy_key": "strategy_a",
        "strategy_version": "v1",
        "as_of_date": TRADE_DATE,
        "window_days": 60,
        "metrics_json": stored_metrics,
        "source": pending_payload["source"],
        "evidence_protocol": pending_payload["evidence_protocol"],
        "artifact_hash": artifact_hash,
        "artifact_json": artifact,
        "source_dataset_hash": dataset_hash,
        "evidence_revision_at": revision_at,
        "verification_status": "CONFIRMED",
        "funding_provenance": "EXTERNAL_SUBMITTED",
        "submitted_by": "submitter",
        "reviewed_by": "reviewer",
        "reviewed_at": f"{TRADE_DATE}T16:00:00",
        "evidence_hash": selection_hash,
        "version_frozen_at": "2026-07-01T00:00:00",
        "evidence_after_version_freeze": 1,
    }
    class _VersionAwareEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            if "SELECT parameters_json FROM st_strategy_version" in sql:
                return _Result(
                    [
                        {
                            "parameters_json": json.dumps(
                                {
                                    "max_holding_days": 2,
                                    "label_horizon_days": 2,
                                }
                            )
                        }
                    ]
                )
            return super().execute(sql, params)

    engine = _VersionAwareEngine(
        strategy_evidence_rows=[snapshot], metric_rows=[metric]
    )
    monkeypatch.setattr(
        governance_module,
        "_trading_sessions_between",
        lambda start, end: max(
            0,
            (date.fromisoformat(end) - date.fromisoformat(start)).days - 1,
        ),
    )
    monkeypatch.setattr(
        governance_module,
        "_authoritative_session_windows",
        lambda as_of_date: (
            {60: session_window} if as_of_date == TRADE_DATE else {}
        ),
    )

    ok, detail = health._confirmed_funding_evidence(
        engine.connect(), "b" * 32, TRADE_DATE
    )
    assert ok is True, detail["errors"][0]["reason"]
    assert detail["passing_snapshot_evidence_references"] == 1
    assert detail["distinct_evidence_rows"] == 1

    embedded_metrics["evidence_hash"] = "7" * 64
    evidence_payload["metrics"] = embedded_metrics
    snapshot["result_hash"] = health._canonical_digest(evidence_payload)
    ok, detail = health._confirmed_funding_evidence(
        engine.connect(), "b" * 32, TRADE_DATE
    )
    assert ok is False
    assert detail["invalid_count"] == 1


def test_confirmed_v1_walk_forward_protocol_fails_health(monkeypatch):
    _fixed_trade_date(monkeypatch)

    class _LegacyConfirmedProtocolEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if "FROM st_strategy_metric_input" in sql:
                result._rows[0]["invalid_confirmed_protocol_count"] = 1
            return result

    result = health.collect_governance_health(
        _LegacyConfirmedProtocolEngine(runs=[_completed_run()]),
        expected_build_sha=BUILD_SHA,
    )
    assert result["status"] == "FAIL"
    evidence_domain = next(
        check
        for check in result["checks"]
        if check["name"] == "metric_evidence_state_domain"
    )
    assert evidence_domain["passed"] is False


def test_confirmed_evidence_must_follow_version_freeze(monkeypatch):
    _fixed_trade_date(monkeypatch)
    evidence_hash = "e" * 64
    evidence_payload = {
        "strategy_key": "strategy_a",
        "strategy_version": "v1",
        "trade_date": TRADE_DATE,
        "window_days": 60,
        "metrics": {
            "verification_status": "CONFIRMED",
            "evidence_hash": evidence_hash,
        },
        "gate": {"passed": True},
        "overall_profit_gate_passed": True,
        "funding_gate_hash": "7" * 64,
    }
    snapshot = {
        "entity_key": "strategy_a",
        "entity_version": "v1",
        "window_days": 60,
        "trade_date": TRADE_DATE,
        "profit_gate_passed": 1,
        "recommended_status": "ACTIVE",
        "evidence_json": evidence_payload,
        "result_hash": health._canonical_digest(evidence_payload),
    }
    metric = {
        "evidence_id": "f" * 32,
        "entity_type": "STRATEGY",
        "strategy_key": "strategy_a",
        "strategy_version": "v1",
        "as_of_date": TRADE_DATE,
        "window_days": 60,
        "metrics_json": "{}",
        "source": "manual_evidence",
        "evidence_protocol": "PURGED_WALK_FORWARD_V2",
        "artifact_hash": "1" * 64,
        "artifact_json": "{}",
        "source_dataset_hash": "2" * 64,
        "evidence_revision_at": f"{TRADE_DATE} 15:00:00",
        "verification_status": "CONFIRMED",
        "submitted_by": "submitter",
        "reviewed_by": "reviewer",
        "reviewed_at": f"{TRADE_DATE} 16:00:00",
        "evidence_hash": evidence_hash,
        "version_frozen_at": f"{TRADE_DATE} 16:00:00",
        "evidence_after_version_freeze": 0,
    }
    result = health.collect_governance_health(
        _GovernanceHealthEngine(
            runs=[_completed_run()],
            strategy_evidence_rows=[snapshot],
            metric_rows=[metric],
        ),
        expected_build_sha=BUILD_SHA,
    )
    assert result["status"] == "FAIL"
    evidence_check = next(
        check
        for check in result["checks"]
        if check["name"] == "funding_snapshots_use_confirmed_evidence"
    )
    assert evidence_check["detail"]["invalid_count"] == 1


def test_named_but_nonunique_decision_index_fails_schema(monkeypatch):
    _fixed_trade_date(monkeypatch)

    class _BadIndexEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if (
                "non_unique" in sql
                and params.get("table_name")
                == "st_strategy_governance_run"
            ):
                for row in result._rows:
                    if row["index_name"] == "uk_strategy_governance_decision":
                        row["non_unique"] = 1
            return result

    result = health.collect_governance_health(
        _BadIndexEngine(runs=[]),
        expected_build_sha=BUILD_SHA,
        allow_input_not_ready=True,
    )
    assert result["status"] == "FAIL"
    contract = next(
        check
        for check in result["checks"]
        if check["name"]
        == "schema_index_contracts:st_strategy_governance_run"
    )
    assert contract["passed"] is False


def test_snapshot_result_hash_is_recomputed(monkeypatch):
    _fixed_trade_date(monkeypatch)
    payload = {
        "strategy_key": "strategy_a",
        "strategy_version": "v1",
        "trade_date": TRADE_DATE,
        "window_days": 20,
        "metrics": {},
        "gate": {"passed": False},
        "overall_profit_gate_passed": False,
        "funding_gate_hash": "7" * 64,
    }
    snapshot = {
        "entity_key": "strategy_a",
        "entity_version": "v1",
        "window_days": 20,
        "trade_date": TRADE_DATE,
        "profit_gate_passed": 0,
        "recommended_status": "SHADOW",
        "evidence_json": payload,
        "result_hash": "f" * 64,
    }
    result = health.collect_governance_health(
        _GovernanceHealthEngine(
            runs=[_completed_run()], strategy_evidence_rows=[snapshot]
        ),
        expected_build_sha=BUILD_SHA,
    )
    assert result["status"] == "FAIL"
    evidence = next(
        check
        for check in result["checks"]
        if check["name"] == "funding_snapshots_use_confirmed_evidence"
    )
    assert evidence["passed"] is False


def test_allocation_target_version_and_gate_hash_must_match(monkeypatch):
    _fixed_trade_date(monkeypatch)

    class _IneligibleAllocationEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if "FROM st_strategy_allocation_snapshot a" in sql:
                for row in result._rows:
                    if row.get("target_type") == "STRATEGY":
                        row["funding_gate_hash"] = "9" * 64
            return result

    result = health.collect_governance_health(
        _IneligibleAllocationEngine(runs=[_completed_run()]),
        expected_build_sha=BUILD_SHA,
    )
    assert result["status"] == "FAIL"
    eligibility = next(
        check
        for check in result["checks"]
        if check["name"] == "allocation_targets_are_funding_eligible"
    )
    assert eligibility["passed"] is False


def test_run_audit_revision_binding_must_match_canonical_run(monkeypatch):
    _fixed_trade_date(monkeypatch)

    class _WrongAuditRevisionEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if "FROM st_strategy_governance_audit" in sql:
                row = result._rows[0]
                row["after_json"]["run_revision"] = 2
                row["evidence_json"]["run_revision"] = 2
                row["payload_json"]["after"] = row["after_json"]
                row["payload_json"]["evidence"] = row["evidence_json"]
                row["audit_hash"] = health._canonical_digest(
                    row["payload_json"]
                )
            return result

    result = health.collect_governance_health(
        _WrongAuditRevisionEngine(runs=[_completed_run()]),
        expected_build_sha=BUILD_SHA,
    )
    assert result["status"] == "FAIL"
    audit = next(
        check
        for check in result["checks"]
        if check["name"] == "completed_run_has_hash_valid_audit"
    )
    assert audit["passed"] is False


def test_market_router_snapshot_hash_is_recomputed(monkeypatch):
    _fixed_trade_date(monkeypatch)

    class _TamperedRouterEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if "SELECT h.strategy_key, h.strategy_version" in sql:
                payload = result._rows[0]["evidence_json"]
                payload["market_route"]["market_match_score"] = 99.0
                result._rows[0]["market_match_score"] = Decimal("99.0000")
            return result

    result = health.collect_governance_health(
        _TamperedRouterEngine(runs=[_completed_run()]),
        expected_build_sha=BUILD_SHA,
    )
    assert result["status"] == "FAIL"
    router = next(
        check
        for check in result["checks"]
        if check["name"] == "market_router_snapshot_is_reproducible"
    )
    assert router["passed"] is False


def test_market_router_risk_cap_is_exact(monkeypatch):
    _fixed_trade_date(monkeypatch)

    class _WrongRiskCapEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if "FROM st_strategy_allocation_snapshot a" in sql:
                for row in result._rows:
                    row["simulated_weight_pct"] = Decimal(
                        "20.0000" if row["target_type"] == "CASH" else "80.0000"
                    )
            return result

    result = health.collect_governance_health(
        _WrongRiskCapEngine(runs=[_completed_run()]),
        expected_build_sha=BUILD_SHA,
    )
    assert result["status"] == "FAIL"
    budget = next(
        check
        for check in result["checks"]
        if check["name"] == "allocation_obeys_market_router_risk_budget"
    )
    assert budget["passed"] is False


def test_application_integrity_health_requires_exact_database_triggers(
    monkeypatch,
):
    from server.db import migrations_v4

    monkeypatch.setattr(
        migrations_v4,
        "run_v4_migrations",
        lambda _engine, *, dry_run=False: tuple(
            SimpleNamespace(
                version=str(migration["version"]),
                status="would_apply",
            )
            for migration in migrations_v4.MIGRATIONS
        ),
    )
    connection = _GovernanceHealthEngine().connect()
    metric_passed, metric = health._metric_input_review_trigger_check(connection)
    ledger_passed, ledger = health._governance_append_only_trigger_check(
        connection
    )
    supporting_passed, supporting = (
        health._supporting_release_trigger_inventory_check(connection)
    )
    full_passed, full = health._full_database_trigger_inventory_check(
        connection
    )

    assert (
        metric_passed
        is ledger_passed
        is supporting_passed
        is full_passed
        is True
    )
    assert metric["trigger_count"] == 2
    assert ledger["trigger_count"] == 38
    assert ledger["total_governance_trigger_count"] == 40
    assert metric["database_triggers_required"] is True
    assert ledger["database_triggers_required"] is True
    assert metric["metadata_frozen"] is True
    assert ledger["metadata_frozen"] is True
    assert metric["definer"] == "probiga_migrator@127.0.0.1"
    assert ledger["definer"] == "probiga_migrator@127.0.0.1"
    assert metric["contract_hash"] == (
        "c217a42eb6c2a5f7bed592bb7c7e724499546f997061c4daad1db957317bdf28"
    )
    assert ledger["contract_hash"] == (
        "bf537f9ed5fb1d31195092ae6a24262511de6f45bf9addacefebc88e25b6b9d8"
    )
    assert metric["source_contract_hash"] == ledger["source_contract_hash"] == (
        "5a1a19e0664c715ae0cac7cfa8dd87c47da1b63b1d2df869561cecf3c995f01f"
    )
    assert ledger["core_contract_hash"] == (
        "1fcde61ce5a5ea0cc16f1910d94da431d044c667383fafd2224217709f555943"
    )
    assert metric["core_append_only_contract_hash"] == (
        ledger["core_contract_hash"]
    )
    assert metric["core_metric_review_contract_hash"] == (
        ledger["core_metric_review_contract_hash"]
    ) == (
        "0dbaa644427139c472bab0c3f719d78bd292bb6a7726a0f0ef195adc2e37fa84"
    )
    assert ledger["funding_contract_hash"] == (
        "47b44f4c1e5201b4ea7cd51f61073fdb4229c245214685c338e24809435a7bde"
    )
    assert supporting["observed_count"] == 81
    assert supporting["expected_trigger_count"] == 81
    assert supporting["owner_counts"]["market_field_capture"] == 5
    assert supporting["owner_counts"]["qmt_membership"] == 6
    assert supporting["owner_counts"]["schema_recovery_evidence"] == 2
    assert supporting["source_contract_hash"] == (
        "076a2b84c15b9dbb54901c63f980c2f85ab17f7652d9334ab661d89ad990d0bc"
    )
    assert full["expected_count"] == full["observed_count"] == 142
    assert full["v2_count"] == 41
    assert full["managed_count"] == 101
    assert full["nameset_sha256"] == (
        "a1c6aa0e9f241a419bbb87c101fbac7d8dd1404aa9f95493afbd604370644a87"
    )


def _privileged_runtime_trigger_seal() -> dict:
    from server.engine import strategy_governance as governance

    inventory = governance.privileged_trigger_inventory_seal_identity(
        BUILD_SHA,
        server_uuid="11111111-2222-4333-8444-555555555555",
        database_name="probiga",
    )
    return {
        "schema": "probiga.privileged-trigger-migration-seal.v1",
        "authority": "PRIVILEGED_CUTOVER_TABLE_METADATA_SEAL",
        "attested_build_sha": BUILD_SHA,
        "trigger_inventory_seal_schema": inventory["schema"],
        "trigger_inventory_seal_database": inventory["database_name"],
        "trigger_inventory_seal_table": inventory["seal_table"],
        "trigger_inventory_server_uuid": inventory["server_uuid"],
        "trigger_inventory_contract_hash": inventory["contract_hash"],
        "trigger_inventory_table_comment": inventory["table_comment"],
        "trigger_inventory_candidate_build_sha": BUILD_SHA,
        "trigger_inventory_rollback_build_sha": "",
        "trigger_inventory_entry_count": 1,
        "runtime_current_user": "probiga_runtime@127.0.0.1",
        "runtime_session_user": "probiga_runtime@127.0.0.1",
        "runtime_tls_verified": True,
        "permission_audit_status": "SKIPPED_BY_USER_AUTHORIZATION",
        "permission_audit_verified": False,
        "runtime_grant_count": None,
        "grant_contract_hash": "",
        "routine_inventory_audit_status": "SKIPPED_BY_USER_AUTHORIZATION",
        "runtime_self_definer_routine_count": None,
        "migrator_self_definer_routine_count": None,
        "runtime_definer_routine_count": None,
        "runtime_definer_routine_inventory_verified": False,
        "runtime_definer_routine_inventory_complete": False,
        "runtime_definer_routine_inventory_authority": "",
        "runtime_definer_routine_inventory_schemas": [],
        "live_trigger_metadata_checked": False,
        "runtime_least_privilege_verified": False,
        "runtime_trigger_metadata_visible": None,
        "runtime_trigger_ddl_authority": None,
        "runtime_database_identity": "probiga_runtime@127.0.0.1",
        "funding_trigger_count": 4,
        "governance_append_only_trigger_count": 38,
        "metric_review_trigger_count": 2,
        "governance_trigger_count": 40,
        "supporting_trigger_count": 81,
        "supporting_trigger_source_contract_hash": (
            governance.PRIVILEGED_SUPPORTING_TRIGGER_SOURCE_CONTRACT_HASH
        ),
        "supporting_trigger_nameset_hash": (
            governance.PRIVILEGED_SUPPORTING_TRIGGER_NAMESET_HASH
        ),
        "managed_trigger_count": 101,
        "managed_trigger_source_contract_hash": (
            governance.PRIVILEGED_MANAGED_TRIGGER_SOURCE_CONTRACT_HASH
        ),
        "managed_trigger_nameset_hash": (
            governance.PRIVILEGED_MANAGED_TRIGGER_NAMESET_HASH
        ),
        "v2_trigger_source_contract_hash": (
            governance.PRIVILEGED_V2_TRIGGER_SOURCE_CONTRACT_HASH
        ),
        "v2_trigger_count": 41,
        "optional_v4_trigger_count": 32,
        "full_trigger_count": 174,
        "full_trigger_nameset_hash": (
            "6cb393a3b7e8471d2e9a382dea51dded58de3662eb87f944886574831567eec0"
        ),
        "pit_fact_table_count": 3,
        "pit_fact_trigger_count": 6,
        "pit_fact_schema_contract_hash": (
            governance.PRIVILEGED_PIT_FACT_SCHEMA_CONTRACT_HASH
        ),
        "base_trigger_nameset_hash": (
            "a1c6aa0e9f241a419bbb87c101fbac7d8dd1404aa9f95493afbd604370644a87"
        ),
        "funding_contract_hash": health.FUNDING_CHECKPOINT_MIGRATION_HASH,
        "governance_append_only_contract_hash": (
            governance.GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACT_HASH
        ),
        "metric_review_contract_hash": (
            governance.METRIC_INPUT_REVIEW_TRIGGER_CONTRACT_HASH
        ),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


class _NoTriggerMetadataConnection:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, _params=None):
        sql = str(statement)
        assert "information_schema.TRIGGERS" not in sql
        raise AssertionError(f"unexpected live-schema query: {sql}")


class _NoTriggerMetadataEngine:
    def __init__(self):
        self.connection = _NoTriggerMetadataConnection()

    def connect(self):
        return self.connection


def _enable_privileged_runtime_trigger_seal(monkeypatch):
    from server.engine import strategy_governance as governance

    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", BUILD_SHA)
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", BUILD_SHA)
    monkeypatch.setattr(
        governance,
        "validate_privileged_trigger_migration_seal",
        lambda _connection: _privileged_runtime_trigger_seal(),
    )


def test_production_runtime_trigger_seal_is_revalidated_on_pooled_connection(
    monkeypatch,
):
    from server.engine import strategy_governance as governance

    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", BUILD_SHA)
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", BUILD_SHA)
    valid = _privileged_runtime_trigger_seal()
    drifted = {**valid, "attested_build_sha": "f" * 40}
    observed = iter((valid, drifted))
    calls = {"count": 0}

    def validate(_connection):
        calls["count"] += 1
        return next(observed)

    monkeypatch.setattr(
        governance,
        "validate_privileged_trigger_migration_seal",
        validate,
    )

    class PooledConnection(_NoTriggerMetadataConnection):
        info = {}

    connection = PooledConnection()
    assert health._production_runtime_trigger_seal(connection)[
        "attested_build_sha"
    ] == BUILD_SHA
    with pytest.raises(RuntimeError, match="migration seal differs"):
        health._production_runtime_trigger_seal(connection)
    assert calls["count"] == 2
    assert connection.info == {}


def test_production_trigger_integrity_uses_privileged_seal_without_metadata(
    monkeypatch,
):
    _enable_privileged_runtime_trigger_seal(monkeypatch)
    connection = _NoTriggerMetadataConnection()
    monkeypatch.setattr(
        qmt_attester,
        "validate_attestation_schema",
        lambda _connection, *, require_triggers: {
            "physical_schema_verified": True,
            "require_triggers": require_triggers,
        },
    )
    monkeypatch.setattr(
        health,
        "validate_strategy_funding_checkpoint_base_schema",
        lambda _connection: {
            "table_count": 2,
            "tables": deepcopy(health.EXPECTED_FUNDING_TABLE_COUNTS),
            "contract_hash": "3" * 64,
            "trigger_installation_asserted": False,
        },
    )

    results = [
        health._qmt_attestation_frozen_schema_check(connection),
        health._supporting_release_trigger_inventory_check(connection),
        health._full_database_trigger_inventory_check(connection),
        health._metric_input_review_trigger_check(connection),
        health._strategy_funding_schema_check(connection),
        health._governance_append_only_trigger_check(connection),
    ]

    assert all(passed for passed, _detail in results)
    assert all(
        detail["live_trigger_metadata_checked"] is False
        and detail["trigger_evidence_authority"]
        == "PRIVILEGED_CUTOVER_TABLE_METADATA_SEAL"
        for _passed, detail in results
    )
    assert results[2][1]["observed_count"] == 174
    assert results[4][1]["trigger_count"] == 4


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("trigger_inventory_seal_database", "other"),
        ("trigger_inventory_server_uuid", "bad-server"),
        ("trigger_inventory_contract_hash", "f" * 64),
        ("trigger_inventory_table_comment", "old-build"),
        ("runtime_database_identity", "probiga_runtime@localhost"),
        ("runtime_current_user", "other@%"),
        ("runtime_session_user", "other@%"),
        ("runtime_tls_verified", False),
        ("permission_audit_status", "VERIFIED"),
        ("permission_audit_verified", True),
        ("runtime_grant_count", 4),
        ("grant_contract_hash", "f" * 64),
        ("routine_inventory_audit_status", "VERIFIED"),
        ("runtime_self_definer_routine_count", 0),
        ("migrator_self_definer_routine_count", 0),
        ("runtime_definer_routine_count", 0),
        ("runtime_definer_routine_inventory_verified", True),
        ("supporting_trigger_nameset_hash", "f" * 64),
        ("managed_trigger_source_contract_hash", "f" * 64),
        ("managed_trigger_nameset_hash", "f" * 64),
        ("v2_trigger_source_contract_hash", "f" * 64),
        ("full_trigger_nameset_hash", "f" * 64),
        ("pit_fact_schema_contract_hash", "f" * 64),
    ),
)
def test_production_trigger_integrity_rejects_drifted_build_seal(
    monkeypatch,
    field,
    value,
):
    from server.engine import strategy_governance as governance

    drifted = _privileged_runtime_trigger_seal()
    drifted[field] = value
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", BUILD_SHA)
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", BUILD_SHA)
    monkeypatch.setattr(
        governance,
        "validate_privileged_trigger_migration_seal",
        lambda _connection: drifted,
    )

    with pytest.raises(
        RuntimeError,
        match="production trigger migration seal differs",
    ):
        health._production_runtime_trigger_seal(
            _NoTriggerMetadataConnection()
        )


def test_production_physical_schema_checks_keep_live_tables_but_use_seal(
    monkeypatch,
):
    import sqlalchemy
    from server.common import pit_facts, qmt_history_coverage
    from tools import sync_guojin_qmt_reference_data as reference

    _enable_privileged_runtime_trigger_seal(monkeypatch)
    calls = {}
    monkeypatch.setattr(
        reference,
        "validate_reference_tables",
        lambda _engine, *, verify_triggers: calls.setdefault(
            "reference_verify_triggers", verify_triggers
        ),
    )
    monkeypatch.setattr(
        qmt_history_coverage,
        "validate_coverage_schema",
        lambda _connection, *, require_triggers: {
            "database": "probiga",
            "table_names": list(qmt_history_coverage.COVERAGE_TABLE_NAMES),
            "table_count": 2,
            "foreign_key_count": 3,
            "trigger_names": list(qmt_history_coverage.COVERAGE_TRIGGER_NAMES),
            "trigger_count": 0,
            "runtime_ddl_required": False,
            "physical_schema_verified": True,
            "physical_seal_verified": False,
            **calls.setdefault(
                "coverage_require_triggers", {
                    "value": require_triggers,
                }
            ),
        },
    )

    class _Inspector:
        @staticmethod
        def get_table_names():
            return list(pit_facts.PIT_FACT_TABLE_NAMES)

        @staticmethod
        def get_columns(table_name):
            return [
                {"name": name}
                for name in pit_facts._REQUIRED_COLUMNS[table_name]
            ]

    monkeypatch.setattr(sqlalchemy, "inspect", lambda _engine: _Inspector())
    monkeypatch.setattr(
        health,
        "validate_strategy_funding_checkpoint_base_schema",
        lambda _connection: {
            "table_count": 2,
            "tables": deepcopy(health.EXPECTED_FUNDING_TABLE_COUNTS),
            "contract_hash": "3" * 64,
            "trigger_installation_asserted": False,
        },
    )
    engine = _NoTriggerMetadataEngine()
    results = [
        _REAL_QMT_REFERENCE_FROZEN_SCHEMA_CHECK(engine),
        _REAL_QMT_HISTORY_COVERAGE_FROZEN_SCHEMA_CHECK(engine.connection),
        _REAL_PIT_FACT_FROZEN_SCHEMA_CHECK(engine),
        health._strategy_funding_schema_check(engine.connection),
    ]

    assert all(passed for passed, _detail in results)
    assert calls["reference_verify_triggers"] is False
    assert calls["coverage_require_triggers"]["value"] is False
    assert all(
        detail["live_trigger_metadata_checked"] is False
        for _passed, detail in results
    )


@pytest.mark.parametrize(
    "case",
    ("missing", "unexpected", "renamed", "body", "definer"),
)
def test_append_only_trigger_health_rejects_exact_inventory_or_metadata_drift(
    case,
):
    class _DriftedTriggerEngine(_GovernanceHealthEngine):
        def _release_trigger_rows_fixture(self, rows, _sql, _params):
            rows = deepcopy(rows)
            target_index = next(
                index
                for index, row in enumerate(rows)
                if row["trigger_name"]
                == "trg_strategy_version_immutable_bu"
            )
            if case == "missing":
                rows.pop(target_index)
            elif case == "unexpected":
                unexpected = deepcopy(rows[target_index])
                unexpected["trigger_name"] = "trg_unexpected_governance_bu"
                rows.append(unexpected)
            elif case == "renamed":
                rows[target_index]["trigger_name"] = (
                    "trg_strategy_version_rewritten_bu"
                )
            elif case == "body":
                rows[target_index]["action_statement"] = (
                    "BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT="
                    "'rewritten trigger body'; END"
                )
            else:
                rows[target_index]["definer"] = "runtime@127.0.0.1"
            return rows

    passed, detail = health._governance_append_only_trigger_check(
        _DriftedTriggerEngine().connect()
    )

    assert passed is False
    assert detail["trigger_count"] == 0
    assert detail["expected_trigger_count"] == 38
    assert detail["errors"] == ["PrivilegedSchemaPreparationError"]


@pytest.mark.parametrize("field", ("action_statement", "definer", "sql_mode"))
def test_metric_review_trigger_health_rejects_physical_metadata_drift(field):
    class _DriftedMetricTriggerEngine(_GovernanceHealthEngine):
        def _release_trigger_rows_fixture(self, rows, _sql, _params):
            rows = deepcopy(rows)
            target = next(
                row
                for row in rows
                if row["trigger_name"]
                == "trg_strategy_metric_input_review_bu"
            )
            target[field] = {
                "action_statement": (
                    "BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT="
                    "'rewritten metric trigger'; END"
                ),
                "definer": "runtime@127.0.0.1",
                "sql_mode": "STRICT_TRANS_TABLES",
            }[field]
            return rows

    passed, detail = health._metric_input_review_trigger_check(
        _DriftedMetricTriggerEngine().connect()
    )

    assert passed is False
    assert detail["trigger_count"] == 0
    assert detail["expected_trigger_count"] == 2
    assert detail["errors"] == ["PrivilegedSchemaPreparationError"]


def test_qmt_attestation_health_rejects_missing_completed_run_guard():
    class _MissingCompletedRunTriggerEngine(_GovernanceHealthEngine):
        def _release_trigger_rows_fixture(self, rows, _sql, _params):
            return [
                deepcopy(row)
                for row in rows
                if row["trigger_name"]
                != "trg_qmt_kline_attestation_run_completed_bu"
            ]

    passed, detail = health._qmt_attestation_frozen_schema_check(
        _MissingCompletedRunTriggerEngine().connect()
    )

    assert passed is False
    assert detail["trigger_count"] == 0
    assert detail["expected_trigger_count"] == 6
    assert detail["database_triggers_required"] is True
    assert len(detail["errors"]) == 1
    assert (
        "exception_type=PrivilegedSchemaPreparationError"
        in detail["errors"][0]
    )


@pytest.mark.parametrize(
    "trigger_name,owner",
    (
        ("trg_qmt_reference_contract_no_delete", "qmt_reference"),
        ("trg_pit_source_coverage_immutable_bd", "pit_facts"),
        ("trg_qmt_history_coverage_no_delete", "qmt_history_coverage"),
        (
            "trg_scheduler_history_qmt_release_no_delete",
            "scheduler_task_history",
        ),
        (
            "trg_privileged_schema_recovery_evidence_immutable_bd",
            "schema_recovery_evidence",
        ),
        ("trg_field_capture_row_no_delete", "market_field_capture"),
        ("trg_qmt_membership_run_no_delete", "qmt_membership"),
    ),
)
def test_supporting_release_inventory_rejects_missing_guard(
    trigger_name,
    owner,
):
    class _MissingSupportingTriggerEngine(_GovernanceHealthEngine):
        def _release_trigger_rows_fixture(self, rows, _sql, _params):
            return [
                deepcopy(row)
                for row in rows
                if row["trigger_name"] != trigger_name
            ]

    passed, detail = health._supporting_release_trigger_inventory_check(
        _MissingSupportingTriggerEngine().connect()
    )

    assert passed is False
    assert detail["trigger_count"] == 0
    assert detail["expected_trigger_count"] == 81
    assert detail["expected_owner_counts"][owner] == {
        "qmt_reference": 10,
        "pit_facts": 6,
        "qmt_history_coverage": 4,
        "scheduler_task_history": 2,
        "schema_recovery_evidence": 2,
        "market_field_capture": 5,
        "qmt_membership": 6,
    }[owner]
    assert detail["database_triggers_required"] is True


def test_full_database_trigger_health_rejects_unrelated_trigger(monkeypatch):
    from server.db import migrations_v4

    monkeypatch.setattr(
        migrations_v4,
        "run_v4_migrations",
        lambda _engine, *, dry_run=False: tuple(
            SimpleNamespace(
                version=str(migration["version"]),
                status="would_apply",
            )
            for migration in migrations_v4.MIGRATIONS
        ),
    )

    class _UnexpectedGlobalTriggerEngine(_GovernanceHealthEngine):
        def _release_trigger_rows_fixture(self, rows, sql, _params):
            rows = deepcopy(rows)
            if "WHERE TRIGGER_SCHEMA=DATABASE() ORDER BY" in sql:
                rows.append({
                    **rows[0],
                    "trigger_name": "trg_unapproved_global_trigger",
                })
            return rows

    passed, detail = health._full_database_trigger_inventory_check(
        _UnexpectedGlobalTriggerEngine().connect()
    )

    assert passed is False
    assert detail["expected_count"] == 142
    assert detail["observed_count"] == 0
    assert detail["metadata_frozen"] is False
    assert len(detail["errors"]) == 1
    assert "exception_type=PrivilegedSchemaPreparationError" in detail["errors"][0]


def test_qmt_reference_health_fails_closed_on_physical_seal_drift(
    monkeypatch,
):
    from tools import sync_guojin_qmt_reference_data as reference

    monkeypatch.setattr(
        reference,
        "validate_reference_tables",
        lambda _engine, verify_triggers=False: (_ for _ in ()).throw(
            RuntimeError("live physical schema differs from seal")
        ),
    )

    passed, detail = _REAL_QMT_REFERENCE_FROZEN_SCHEMA_CHECK(object())

    assert passed is False
    assert detail["expected_trigger_count"] == 10
    assert detail["physical_schema_verified"] is False
    assert detail["physical_seal_verified"] is False
    assert len(detail["errors"]) == 1
    assert "exception_type=RuntimeError" in detail["errors"][0]
    assert "live physical schema" not in detail["errors"][0]


def test_pit_fact_health_fails_closed_on_incomplete_physical_schema(
    monkeypatch,
):
    from server.common import pit_facts

    monkeypatch.setattr(
        pit_facts,
        "pit_fact_schema_health",
        lambda _engine: {
            "schema": "probiga.pit-fact-schema-health.v1",
            "status": "NOT_READY",
            "valid": False,
            "table_count": 3,
            "trigger_count": 5,
            "missing_tables": [],
            "missing_columns": {},
            "missing_triggers": [
                "trg_pit_source_coverage_immutable_bd"
            ],
            "contract_hash": "d" * 64,
        },
    )

    passed, detail = _REAL_PIT_FACT_FROZEN_SCHEMA_CHECK(object())

    assert passed is False
    assert detail["expected_trigger_count"] == 6
    assert detail["physical_schema_verified"] is False
    assert detail["missing_triggers"] == [
        "trg_pit_source_coverage_immutable_bd"
    ]


def test_qmt_announcement_scheduler_bad_contract_fails_production_health(
    monkeypatch,
):
    _fixed_trade_date(monkeypatch)
    bad_event_task = _GovernanceHealthEngine._valid_qmt_announcement_task()
    bad_event_task["script_args"] = "--window-days 1 --batch-size 500"
    engine = _GovernanceHealthEngine(
        runs=[_completed_run()],
        tasks=[
            _GovernanceHealthEngine._valid_task(),
            bad_event_task,
            *_GovernanceHealthEngine._valid_qmt_operations_tasks(),
        ],
    )

    result = health.collect_governance_health(
        engine,
        expected_build_sha=BUILD_SHA,
    )

    assert result["status"] == "FAIL"
    check = next(
        item
        for item in result["checks"]
        if item["name"] == "qmt_announcement_scheduler_task_contract"
    )
    assert check["passed"] is False
    assert check["detail"]["actual"]["script_args"] != (
        check["detail"]["expected"]["script_args"]
    )


def test_qmt_operations_scheduler_drift_fails_production_health(monkeypatch):
    _fixed_trade_date(monkeypatch)
    operation_tasks = _GovernanceHealthEngine._valid_qmt_operations_tasks()
    full_history = next(
        task
        for task in operation_tasks
        if task["task_type"] == "qmt_local_history_2024"
    )
    full_history["script_args"] = (
        "--start-date 2026-01-01 --log-path data/logs/unsafe.jsonl"
    )
    engine = _GovernanceHealthEngine(
        runs=[_completed_run()],
        tasks=[
            _GovernanceHealthEngine._valid_task(),
            _GovernanceHealthEngine._valid_qmt_announcement_task(),
            *operation_tasks,
        ],
    )

    result = health.collect_governance_health(
        engine,
        expected_build_sha=BUILD_SHA,
    )

    assert result["status"] == "FAIL"
    check = next(
        item
        for item in result["checks"]
        if item["name"] == "qmt_operations_scheduler_tasks_contract"
    )
    assert check["passed"] is False
    assert check["detail"]["actual"]["qmt_local_history_2024"] != (
        check["detail"]["expected"]["qmt_local_history_2024"]
    )


def test_incomplete_latest_qmt_announcement_batch_is_data_blocked(
    monkeypatch,
):
    from tools import sync_qmt_announcement_pit

    monkeypatch.setattr(
        sync_qmt_announcement_pit,
        "validate_existing_complete_qmt_announcement_batch",
        lambda _engine, **_kwargs: {
            "status": "COMPLETE",
            "reason_code": "QMT_ANNOUNCEMENT_EXISTING_FULL_MARKET_COMPLETE",
            "trade_date": TRADE_DATE,
            "source": "qmt.announcement",
            "batch_id": "incomplete-event-batch",
            "batch_root_hash": "e" * 64,
            "stock_count": 2,
            "coverage_count": 1,
            "funding_eligible": True,
            "database_writes": False,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        },
    )
    monkeypatch.setattr(
        sync_qmt_announcement_pit,
        "validate_existing_task_result",
        lambda *_args, **_kwargs: "complete",
    )

    passed, detail = _REAL_LATEST_QMT_ANNOUNCEMENT_BATCH_CHECK(
        object(),
        TRADE_DATE,
    )

    assert passed is False
    assert detail["funding_eligible"] is False
    assert detail["coverage_row_count"] == 1
    assert detail["catalog_member_count"] == 2
    assert detail["automatic_real_order_submission"] is False
    assert detail["real_order_authority"] is False


@pytest.mark.parametrize(
    ("source", "reason_code", "primary_source", "fallback_reason"),
    (
        (
            "qmt.announcement",
            "QMT_ANNOUNCEMENT_EXISTING_FULL_MARKET_COMPLETE",
            "",
            "",
        ),
        (
            "cninfo.announcement",
            "ANNOUNCEMENT_FALLBACK_EXISTING_FULL_MARKET_COMPLETE",
            "qmt.announcement",
            "QMT_ANNOUNCEMENT_NO_PERMISSION_OR_QUERY_FAILED",
        ),
        (
            "eastmoney.notice",
            "ANNOUNCEMENT_FALLBACK_EXISTING_FULL_MARKET_COMPLETE",
            "qmt.announcement",
            "QMT_ANNOUNCEMENT_SDK_UNAVAILABLE",
        ),
        (
            "cninfo.announcement",
            "ANNOUNCEMENT_FALLBACK_EXISTING_FULL_MARKET_COMPLETE",
            "qmt.announcement",
            "QMT_ANNOUNCEMENT_TERMINAL_DEPENDENCY_UNAVAILABLE",
        ),
    ),
)
def test_latest_announcement_health_accepts_only_strict_authoritative_batches(
    monkeypatch,
    source,
    reason_code,
    primary_source,
    fallback_reason,
):
    from tools import sync_qmt_announcement_pit

    calls = []
    proof = {
        "status": "COMPLETE",
        "reason_code": reason_code,
        "trade_date": TRADE_DATE,
        "source": source,
        "batch_id": "authoritative-event-batch",
        "batch_root_hash": "e" * 64,
        "stock_count": 5555,
        "coverage_count": 5555,
        "funding_eligible": True,
        "database_writes": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    if source != "qmt.announcement":
        proof.update({
            "primary_source": primary_source,
            "fallback_reason": fallback_reason,
        })

    def validate_existing(_engine, **kwargs):
        calls.append(kwargs)
        return dict(proof)

    monkeypatch.setattr(
        sync_qmt_announcement_pit,
        "validate_existing_complete_qmt_announcement_batch",
        validate_existing,
    )
    monkeypatch.setattr(
        sync_qmt_announcement_pit,
        "validate_existing_task_result",
        lambda payload, process_exit, **kwargs: (
            "complete"
            if payload == proof
            and process_exit == 0
            and kwargs == {"expected_trade_date": TRADE_DATE}
            else "invalid"
        ),
    )

    passed, detail = _REAL_LATEST_QMT_ANNOUNCEMENT_BATCH_CHECK(
        object(), TRADE_DATE
    )

    assert passed is True
    assert calls == [{"expected_trade_date": TRADE_DATE}]
    assert detail["source"] == source
    assert detail["catalog_member_count"] == 5555
    assert detail["coverage_row_count"] == 5555
    assert detail["funding_eligible"] is True


@pytest.mark.parametrize(
    ("primary_source", "fallback_reason"),
    (
        ("cninfo.announcement", "QMT_ANNOUNCEMENT_SDK_UNAVAILABLE"),
        ("qmt.announcement", "LOCAL_DATABASE_ERROR"),
        ("qmt.announcement", "ModuleNotFoundError"),
    ),
)
def test_latest_announcement_health_rejects_unfrozen_fallback_evidence(
    monkeypatch,
    primary_source,
    fallback_reason,
):
    from tools import sync_qmt_announcement_pit

    proof = {
        "status": "COMPLETE",
        "reason_code": "ANNOUNCEMENT_FALLBACK_EXISTING_FULL_MARKET_COMPLETE",
        "trade_date": TRADE_DATE,
        "source": "cninfo.announcement",
        "primary_source": primary_source,
        "fallback_reason": fallback_reason,
        "batch_id": "invalid-fallback-event-batch",
        "batch_root_hash": "e" * 64,
        "stock_count": 5555,
        "coverage_count": 5555,
        "funding_eligible": True,
        "database_writes": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    monkeypatch.setattr(
        sync_qmt_announcement_pit,
        "validate_existing_complete_qmt_announcement_batch",
        lambda *_args, **_kwargs: dict(proof),
    )
    # Even if the shared result validator were accidentally weakened, health
    # independently requires the frozen QMT-primary fallback evidence.
    monkeypatch.setattr(
        sync_qmt_announcement_pit,
        "validate_existing_task_result",
        lambda *_args, **_kwargs: "complete",
    )

    passed, detail = _REAL_LATEST_QMT_ANNOUNCEMENT_BATCH_CHECK(
        object(), TRADE_DATE
    )

    assert passed is False
    assert detail["source"] == "cninfo.announcement"
    assert detail["funding_eligible"] is False


def test_missing_qmt_announcement_batch_cannot_be_waived_for_funding(
    monkeypatch,
):
    _fixed_trade_date(monkeypatch)
    monkeypatch.setattr(
        health,
        "_latest_qmt_announcement_batch_check",
        lambda _engine, trade_date: (
            False,
            {
                "status": "DATA_BLOCKED",
                "trade_date": trade_date,
                "reason_code": (
                    "QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND"
                ),
                "funding_eligible": False,
                "automatic_real_order_submission": False,
                "real_order_authority": False,
            },
        ),
    )

    result = health.collect_governance_health(
        _GovernanceHealthEngine(runs=[]),
        expected_build_sha=BUILD_SHA,
        allow_input_not_ready=True,
    )

    assert result["status"] == "FAIL"
    check = next(
        item
        for item in result["checks"]
        if item["name"] == "latest_qmt_announcement_full_market_batch"
    )
    assert check["passed"] is False
    assert check["waived"] is False
    assert check["detail"]["status"] == "DATA_BLOCKED"
    assert check["detail"]["funding_eligible"] is False


def test_funding_schema_health_fails_closed_on_validator_error(monkeypatch):
    monkeypatch.setattr(
        health,
        "validate_strategy_funding_checkpoint_schema",
        lambda _connection: (_ for _ in ()).throw(
            RuntimeError("private-host/secret")
        ),
    )

    passed, detail = health._strategy_funding_schema_check(object())

    assert passed is False
    assert detail["errors"] == ["RuntimeError"]
    assert "private-host" not in str(detail)
    assert "secret" not in str(detail)


def test_governance_health_exception_metadata_never_reflects_credentials(
    monkeypatch,
):
    from server.engine import strategy_governance as governance

    credential = (
        "mysql://" + "private_user" + ":" + "private_password"
        + "@10.9.8.7:3306/probiga C:/private/schema.sql"
    )
    monkeypatch.setattr(
        governance,
        "_authoritative_session_windows_with_proof",
        lambda _trade_date: (_ for _ in ()).throw(RuntimeError(credential)),
    )

    passed, detail = health._authoritative_session_window_attestation_check(
        TRADE_DATE
    )

    assert passed is False
    assert credential not in str(detail)
    assert "private_password" not in str(detail)
    assert "error_code=governance_health_check_failed" in detail["error"]
    assert "exception_type=RuntimeError" in detail["error"]
    assert re.search(r"incident_id=[0-9a-f]{16}", detail["error"])


def _valid_challenger_evidence_audit(
    *,
    challenger_id="1" * 32,
    artifact_hash="c" * 64,
    source_dataset_hash="d" * 64,
    nonce="2" * 32,
):
    evidence = {
        "schema": "probiga.strategy-challenger-evidence-submission.v1",
        "challenger_id": challenger_id,
        "proposal_hash": "3" * 64,
        "proposed_version_hash": "4" * 64,
        "proposal_submitted_at": "2026-08-20T10:00:00",
        "submitted_by": "user-id:challenger-submitter",
        "as_of_date": "2026-08-21",
        "window_days": 120,
        "evidence_protocol": "PURGED_WALK_FORWARD_V2",
        "evidence_revision_at": "2026-08-21T15:00:00",
        "metrics": {"net_expectancy_pct": 1.0},
        "artifact_manifest": {
            "source_dataset_hash": source_dataset_hash,
        },
        "artifact_hash": artifact_hash,
        "source_dataset_hash": source_dataset_hash,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
        "server_replay_validation_hash": "5" * 64,
    }
    evidence["evidence_submission_hash"] = health._canonical_digest(evidence)
    before = {"challenger_id": challenger_id, "status": "VALIDATING"}
    after = {
        "challenger_id": challenger_id,
        "status": "REVIEW_PENDING",
        "evidence_submission_hash": evidence["evidence_submission_hash"],
        "artifact_hash": artifact_hash,
        "source_dataset_hash": source_dataset_hash,
    }
    payload = {
        "entity_type": "STRATEGY",
        "entity_key": "challenger_strategy",
        "action": "SUBMIT_CHALLENGER_EVIDENCE",
        "reason": "fixture challenger evidence",
        "operator": evidence["submitted_by"],
        "before": before,
        "after": after,
        "evidence": evidence,
        "nonce": nonce,
    }
    return {
        "audit_id": nonce,
        "entity_type": payload["entity_type"],
        "entity_key": payload["entity_key"],
        "action": payload["action"],
        "reason": payload["reason"],
        "operator_name": payload["operator"],
        "before_json": before,
        "after_json": after,
        "evidence_json": evidence,
        "payload_json": payload,
        "audit_hash": health._canonical_digest(payload),
        "created_at": "2026-08-21T15:01:00",
    }


def test_dynamic_shadow_health_reuses_the_authoritative_exact_schema_validator(
    monkeypatch,
):
    engine = _GovernanceHealthEngine()
    from server.engine import dynamic_shadow_ledger_schema as schema

    expected = {
        "scope": "dynamic_shadow_ledger",
        "table_count": 4,
        "column_count": 57,
        "index_count": 22,
        "foreign_key_count": 15,
        "check_count": 10,
        "contract_hash": schema.DYNAMIC_SHADOW_LEDGER_SCHEMA_CONTRACT_HASH,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    calls = []
    monkeypatch.setattr(
        schema,
        "validate_dynamic_shadow_ledger_schema",
        lambda connection: calls.append(connection) or expected,
    )

    passed, detail = health._dynamic_shadow_schema_constraints_check(
        _Connection(engine)
    )

    assert passed is True
    assert detail == expected
    assert detail["index_count"] == 22
    assert len(calls) == 1


@pytest.mark.parametrize(
    "drift_kind",
    (
        "missing_fk",
        "wrong_parent_table",
        "wrong_parent_column",
        "wrong_update_rule",
        "wrong_delete_rule",
        "missing_check",
        "changed_check_clause",
    ),
)
def test_dynamic_shadow_schema_constraint_drift_fails_closed_on_empty_ledgers(
    drift_kind, monkeypatch,
):
    from server.engine import dynamic_shadow_ledger_schema as schema

    monkeypatch.setattr(
        schema,
        "validate_dynamic_shadow_ledger_schema",
        lambda _connection: (_ for _ in ()).throw(RuntimeError(drift_kind)),
    )

    with pytest.raises(RuntimeError, match=drift_kind):
        health._dynamic_shadow_schema_constraints_check(
            _Connection(_GovernanceHealthEngine())
        )


def test_health_source_does_not_implement_a_second_dynamic_schema_validator():
    source = inspect.getsource(health._dynamic_shadow_schema_constraints_check)

    assert "validate_dynamic_shadow_ledger_schema" in source
    assert "information_schema" not in source
    assert "DYNAMIC_SHADOW_FOREIGN_KEY_CONTRACTS" not in source


def test_global_evidence_namespace_accepts_distinct_hash_verified_claims():
    engine = _GovernanceHealthEngine(metric_rows=[{
        "evidence_id": "a" * 32,
        "artifact_hash": "a" * 64,
        "source_dataset_hash": "b" * 64,
    }], audit_rows=[_valid_challenger_evidence_audit()])

    passed, detail = health._global_evidence_claim_uniqueness_check(
        _Connection(engine)
    )

    assert passed is True, detail
    assert detail["metric_claim_count"] == 1
    assert detail["challenger_claim_count"] == 1


@pytest.mark.parametrize("duplicate_namespace", ("artifact", "dataset"))
def test_global_evidence_namespace_rejects_metric_challenger_reuse(
    duplicate_namespace,
):
    artifact_hash = "a" * 64
    dataset_hash = "b" * 64
    challenger = _valid_challenger_evidence_audit(
        artifact_hash=(artifact_hash if duplicate_namespace == "artifact" else "c" * 64),
        source_dataset_hash=(
            dataset_hash if duplicate_namespace == "dataset" else "d" * 64
        ),
    )
    engine = _GovernanceHealthEngine(metric_rows=[{
        "evidence_id": "e" * 32,
        "artifact_hash": artifact_hash,
        "source_dataset_hash": dataset_hash,
    }], audit_rows=[challenger])

    passed, detail = health._global_evidence_claim_uniqueness_check(
        _Connection(engine)
    )

    assert passed is False
    assert any(
        error.get("namespace")
        == (
            "artifact_hash"
            if duplicate_namespace == "artifact"
            else "source_dataset_hash"
        )
        for error in detail["errors"]
    )


def test_global_evidence_namespace_rejects_tampered_challenger_audit():
    challenger = _valid_challenger_evidence_audit()
    challenger["evidence_json"]["artifact_hash"] = "f" * 64
    engine = _GovernanceHealthEngine(audit_rows=[challenger])

    passed, detail = health._global_evidence_claim_uniqueness_check(
        _Connection(engine)
    )

    assert passed is False
    assert detail["valid_challenger_claim_count"] == 0
    assert "error_code=governance_health_check_failed" in (
        detail["errors"][0]["reason"]
    )
    assert "exception_type=ValueError" in detail["errors"][0]["reason"]
