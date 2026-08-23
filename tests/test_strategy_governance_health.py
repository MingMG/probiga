from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal
import json

import pytest

from tools import check_strategy_governance_health as health
from tools import attest_qmt_daily_kline as qmt_attester
from tools.strategy_governance_task_contract import TASK


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


def _attested_session_window(window_days: int) -> dict:
    sessions = []
    cursor = date.fromisoformat(TRADE_DATE)
    while len(sessions) < window_days:
        if cursor.weekday() < 5:
            sessions.append(cursor.isoformat())
        cursor -= timedelta(days=1)
    sessions.reverse()
    stock_codes = ["000001", "600000"]
    payload = {
        "schema": "probiga.authoritative-session-window.v1",
        "window_days": window_days,
        "start_date": sessions[0],
        "end_date": sessions[-1],
        "session_count": len(sessions),
        "sessions": sessions,
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
STRATEGY_GATE_HASHES = {
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


def _valid_funding_window_metrics(
    *, window: int, evidence_hash: str, route_hash: str,
) -> dict:
    from server.engine import strategy_governance as governance_module

    session_window = _attested_session_window(window)
    is_decay_window = window == 20
    completed_trades = 20 if is_decay_window else 80
    coverage_days = 20 if is_decay_window else 60
    metrics = {
        "window_days": window,
        "completed_trades": completed_trades,
        "coverage_days": coverage_days,
        "portfolio_coverage_days": coverage_days,
        "gross_expectancy_pct": 0.8,
        "estimated_cost_pct": 0.2,
        "net_expectancy_pct": 0.6,
        "payoff_ratio": 1.2,
        "profit_factor": 1.5,
        "max_drawdown_pct": 5.0,
        "walk_forward_segments": 5,
        "positive_segments": 4,
        "cost_stress_expectancy_pct": 0.5,
        "top5_profit_contribution_pct": 50.0,
        "market_match_score": 100.0,
        "market_route_hash": route_hash,
        "health_score": 80.0,
        "version_bound_evidence": True,
        "independent_oos": True,
        "evidence_protocol": "PURGED_WALK_FORWARD_V2",
        "artifact_hash": health._canonical_digest(
            {"window": window, "kind": "artifact"}
        ),
        "source_dataset_hash": health._canonical_digest(
            {"window": window, "kind": "source"}
        ),
        "evidence_revision_at": f"{TRADE_DATE}T15:00:00",
        "funding_provenance": "INTERNAL_PORTFOLIO_LEDGER_V1",
        "internal_ledger_hash": health._canonical_digest(
            {"window": window, "kind": "ledger"}
        ),
        "verification_status": "CONFIRMED",
        "submitted_by": "fixture_submitter",
        "reviewed_by": "fixture_reviewer",
        "reviewed_at": f"{TRADE_DATE}T16:00:00",
        "review_audit_valid": True,
        "drawdown_basis": "internal_version_bound_portfolio_equity",
        "cost_basis": "actual_ledger_fees",
        "evidence_fresh": True,
        "selection_validation_fresh": True,
        "walk_forward_verified": True,
        "selection_validation_independent_oos": True,
        "selection_validation_scope": "VERSION_SELECTION_ONLY",
        "selection_validation_completed_trades": completed_trades,
        "selection_validation_coverage_days": coverage_days,
        "evidence_hash": evidence_hash,
        "session_window_valid": True,
        "session_window_start": session_window["start_date"],
        "session_window_end": session_window["end_date"],
        "session_window_count": session_window["session_count"],
        "session_window_hash": session_window["session_hash"],
    }
    metrics["profit_gate"] = governance_module.evaluate_window_gate(metrics)
    return metrics


def _valid_strategy_router_rows() -> list[dict]:
    rows = []
    for key, route in STRATEGY_ROUTES.items():
        for window in health.EXPECTED_WINDOWS:
            metrics = _valid_funding_window_metrics(
                window=window,
                evidence_hash=STRATEGY_WINDOW_EVIDENCE[key][str(window)],
                route_hash=route["router_decision_hash"],
            )
            payload = {
                "strategy_key": key,
                "strategy_version": "v1",
                "trade_date": TRADE_DATE,
                "window_days": window,
                "metrics": metrics,
                "gate": metrics["profit_gate"],
                "overall_profit_gate_passed": True,
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
                    "market_match_score": Decimal("100.0000"),
                    "health_score": Decimal("80.0000"),
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
        "contribution_score": 40.0,
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
COMBINATION_GATE_HASH = health._canonical_digest(
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


def _valid_combination_router_rows() -> list[dict]:
    metrics = {
        str(window): {
            **_valid_funding_window_metrics(
                window=window,
                evidence_hash=COMBINATION_WINDOW_EVIDENCE[str(window)],
                route_hash=COMBINATION_ROUTE["router_decision_hash"],
            ),
            "selection_evidence_hash": health._canonical_digest(
                {
                    "combination_key": "combo_a",
                    "window": window,
                    "kind": "selection",
                }
            ),
        }
        for window in health.EXPECTED_WINDOWS
    }
    payload = {
        "combination_key": "combo_a",
        "combination_version": "v1",
        "trade_date": TRADE_DATE,
        "metrics": metrics,
        "multi_window_gate": {
            str(window): metrics[str(window)]["profit_gate"]
            for window in health.EXPECTED_WINDOWS
        },
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
            "ranking_score": Decimal("80.0000"),
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


def _fixture_pool_rows() -> list[dict]:
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


def _fixture_allocation_contract(
    *, router_snapshot_hash: str = ROUTER_SNAPSHOT_HASH,
    combination_route: dict | None = None,
    combination_gate_hash: str = COMBINATION_GATE_HASH,
    combination_profit_gate_passed: bool = False,
    combination_member_details: list[dict] | None = None,
    strategy_a_status: str = "ACTIVE",
    automatic_transition_plan_hash: str | None = None,
) -> dict:
    combination_route = combination_route or COMBINATION_ROUTE
    combination_member_details = (
        combination_member_details or COMBINATION_MEMBER_DETAILS
    )
    bindings = {}
    for key, route in STRATEGY_ROUTES.items():
        active = key == "strategy_a"
        bindings[("STRATEGY", key, "v1")] = {
            "router_decision_hash": route["router_decision_hash"],
            "market_match_score": Decimal("100.0000"),
            "market_state": MARKET_STATE,
            "eligible": True,
            "paper_allocation_eligible": active,
            "funding_gate_hash": STRATEGY_GATE_HASHES[key],
            "members": frozenset({key}),
            "ranking_score": Decimal("80.0000"),
            "target_name": key,
            "enabled": True,
            "lifecycle_status": strategy_a_status if active else "SHADOW",
            "profit_gate_passed": True,
            "constraint_passed": True,
        }
    bindings[("COMBINATION", "combo_a", "v1")] = {
        "router_decision_hash": combination_route["router_decision_hash"],
        "market_match_score": Decimal("100.0000"),
        "market_state": MARKET_STATE,
        "eligible": combination_route.get("eligible") is True,
        "paper_allocation_eligible": False,
        "funding_gate_hash": combination_gate_hash,
        "members": frozenset(STRATEGY_ROUTES),
        "ranking_score": Decimal("80.0000"),
        "target_name": "combo_a",
        "enabled": True,
        "lifecycle_status": "SHADOW",
        "profit_gate_passed": combination_profit_gate_passed,
        "constraint_passed": True,
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
    decision_hash = health._canonical_digest(
        {
            "schema": "strategy-governance-decision.v6",
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
            "pool_snapshot_hash": _fixture_pool_snapshot_hash(),
            "strategies": [
                {
                    "strategy_key": row["target_key"],
                    "strategy_version": row["target_version"],
                    "enabled": row["enabled"],
                    "projected_status": row["lifecycle_status"],
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
                    "funding_gate_hash": row["funding_gate_hash"],
                }
                for row in combinations
            ],
        }
    )
    return {
        "allocation_policy_version": health.ALLOCATION_POLICY_VERSION,
        "trading_gate_passed": True,
        "market_risk_cap_pct": 85.0,
        "allocation_candidate_count": len(candidates),
        "eligible_candidate_count": 1,
        "candidate_set_hash": candidate_hash,
        "allocation_snapshot_hash": allocation_hash,
        "pool_row_count": 3,
        "pool_snapshot_hash": _fixture_pool_snapshot_hash(),
        "automatic_transition_count": 0,
        "automatic_transition_plan_hash": transition_plan_hash,
        "cash_weight_pct": next(
            row["simulated_weight_pct"]
            for row in allocations
            if row["target_type"] == "CASH"
        ),
        "decision_hash": decision_hash,
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


class _Connection:
    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, params=None):
        return self.engine.execute(str(statement), params or {})


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
    ):
        self.runs = list(runs or [])
        self.tasks = list(tasks or [self._valid_task()])
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
        self.audit_rows = (
            None if audit_rows is None else list(audit_rows)
        )

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
        if any(row.get("tamper_raw_metric") for row in self.raw_fill_rows):
            first_key = sorted(replay["strategies"])[0]
            replay["strategies"][first_key]["metrics"][
                "net_expectancy_pct"
            ] = -99.0
        return replay

    def execute(self, sql, params):
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
        if "FROM st_scheduled_tasks" in sql:
            return _Result(self.tasks)
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
            "cash_weight_pct": allocation_contract["cash_weight_pct"],
        },
        "created_at": "2026-08-21 22:35:00",
        "finished_at": "2026-08-21 22:35:10",
    }


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
            "contribution_score": 40.0,
        }
        for key in sorted(STRATEGY_ROUTES)
    ]
    gate_hash = health._canonical_digest(
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
    metrics = {}
    for window in health.EXPECTED_WINDOWS:
        session = _attested_session_window(window)
        metrics[str(window)] = {
            "window_days": window,
            "evidence_hash": window_evidence[str(window)],
            "market_match_score": 100.0,
            "market_route_hash": route["router_decision_hash"],
            "health_score": 0.0,
            "funding_provenance": "EXTERNAL_SUBMITTED",
            "verification_status": "CONFIRMED",
            "selection_validation_scope": "VERSION_SELECTION_ONLY",
            "session_window_valid": True,
            "session_window_start": session["start_date"],
            "session_window_end": session["end_date"],
            "session_window_count": session["session_count"],
            "session_window_hash": session["session_hash"],
        }
        metrics[str(window)]["profit_gate"] = (
            health._canonical_window_gate(metrics[str(window)])
        )
    payload = {
        "combination_key": "combo_a",
        "combination_version": "v1",
        "trade_date": TRADE_DATE,
        "metrics": metrics,
        "multi_window_gate": {
            str(window): metrics[str(window)]["profit_gate"]
            for window in health.EXPECTED_WINDOWS
        },
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
            "ranking_score": Decimal("80.0000"),
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
        combination_profit_gate_passed=False,
        combination_member_details=member_details,
    )
    run["router_snapshot_hash"] = router_snapshot_hash
    run["decision_hash"] = allocation_contract["decision_hash"]
    run["summary_json"] = {
        **run["summary_json"],
        "combination_route_eligible_count": 0,
        "router_snapshot_hash": router_snapshot_hash,
        **{
            key: value
            for key, value in allocation_contract.items()
            if key != "decision_hash"
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
    monkeypatch.setattr(
        governance_module,
        "_authoritative_session_windows",
        lambda _trade_date: deepcopy(windows),
    )
    passed, _detail = health._authoritative_session_window_attestation_check(
        TRADE_DATE
    )
    assert passed is True

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
        "_authoritative_session_windows",
        lambda _trade_date: tampered,
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
        "_authoritative_session_windows",
        lambda _trade_date: wrong_universe,
    )
    passed, detail = health._authoritative_session_window_attestation_check(
        TRADE_DATE
    )
    # The window helper proves shape/hash binding.  The independently queried
    # three-set check recomputes and rejects this syntactically valid lie.
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
        "_authoritative_session_windows",
        lambda _trade_date: legacy,
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
    reduced_combination_gate_hash = health._canonical_digest(
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
    reduced_combination_rows = _valid_combination_router_rows()
    for combo_row in reduced_combination_rows:
        payload = combo_row["evidence_json"]
        payload["member_details"] = reduced_member_details
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
            if "SELECT h.strategy_key, h.strategy_version" in sql:
                for row in result._rows:
                    if row["strategy_key"] == "strategy_a":
                        row["registry_current_status"] = "REDUCE"
            if (
                "FROM st_strategy_allocation_snapshot" in sql
                and len(result._rows) == 2
                and all("target_type" in row for row in result._rows)
            ):
                cash, strategy = result._rows
                cash["simulated_weight_pct"] = Decimal("57.5000")
                strategy.update(
                    {
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
        combination_member_details=reduced_member_details,
    )
    run["decision_hash"] = allocation_contract["decision_hash"]
    run["summary_json"].update(
        {
            key: value
            for key, value in allocation_contract.items()
            if key != "decision_hash"
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
        "ranking_score": Decimal("10.0000"),
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


def test_v3_allocation_uses_fixed_type_lanes_and_reduce_member_sleeves():
    _bindings, _run, allocations = _v3_combination_allocation_fixture()

    combination = next(
        row for row in allocations if row["target_type"] == "COMBINATION"
    )
    standalone = next(
        row for row in allocations if row["target_key"] == "standalone"
    )
    cash = next(row for row in allocations if row["target_type"] == "CASH")

    assert combination["base_competitive_weight_pct"] == 42.5
    assert standalone["base_competitive_weight_pct"] == 42.5
    assert combination["simulated_weight_pct"] == 34.0
    assert combination["cash_discount_bp"] == 850
    assert cash["simulated_weight_pct"] == 23.5
    assert [
        (row["strategy_key"], row["base_bp"], row["effective_bp"])
        for row in combination["member_sleeves"]
    ] == [
        ("member_active", 2550, 2550),
        ("member_reduce", 1700, 850),
    ]


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
    ("entity_type", "expected_reason"),
    (
        (
            "STRATEGY",
            "persisted window gate differs from canonical metrics gate",
        ),
        (
            "COMBINATION",
            "combination window gate differs from canonical metrics gate",
        ),
    ),
)
def test_health_recomputes_window_gate_instead_of_trusting_rehashed_payload(
    monkeypatch, entity_type, expected_reason,
):
    _fixed_trade_date(monkeypatch)

    class _ForgedWindowGateEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if (
                entity_type == "STRATEGY"
                and "SELECT h.strategy_key, h.strategy_version" in sql
            ):
                row = next(
                    item
                    for item in result._rows
                    if item["strategy_key"] == "strategy_a"
                    and item["window_days"] == 20
                )
                payload = row["evidence_json"]
                payload["metrics"]["completed_trades"] = 19
                row["result_hash"] = health._canonical_digest(payload)
            if (
                entity_type == "COMBINATION"
                and "SELECT h.combination_key, h.combination_version" in sql
            ):
                row = result._rows[0]
                payload = row["evidence_json"]
                payload["metrics"]["20"]["completed_trades"] = 19
                row["result_hash"] = health._canonical_digest(payload)
            return result

    result = health.collect_governance_health(
        _ForgedWindowGateEngine(runs=[_completed_run()]),
        expected_build_sha=BUILD_SHA,
    )

    checks = {check["name"]: check for check in result["checks"]}
    assert result["status"] == "FAIL"
    router_check = checks["market_router_snapshot_is_reproducible"]
    assert router_check["passed"] is False
    assert any(
        item["entity_type"] == entity_type
        and item["reason"] == expected_reason
        for item in router_check["detail"]["errors"]
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
        == "persisted window gate differs from canonical metrics gate"
        for item in router_check["detail"]["errors"]
    )


def test_health_recomputes_strategy_overall_gate_from_all_three_windows(
    monkeypatch,
):
    _fixed_trade_date(monkeypatch)

    class _ForgedOverallGateEngine(_GovernanceHealthEngine):
        def execute(self, sql, params):
            result = super().execute(sql, params)
            if "SELECT h.strategy_key, h.strategy_version" in sql:
                for row in result._rows:
                    if row["strategy_key"] != "strategy_a":
                        continue
                    payload = row["evidence_json"]
                    payload["overall_profit_gate_passed"] = False
                    payload["paper_allocation_eligible"] = False
                    payload["funding_gate_hash"] = health._canonical_digest(
                        {
                            "strategy_key": "strategy_a",
                            "strategy_version": "v1",
                            "window_evidence": STRATEGY_WINDOW_EVIDENCE[
                                "strategy_a"
                            ],
                            "router_decision_hash": STRATEGY_ROUTES[
                                "strategy_a"
                            ]["router_decision_hash"],
                            "overall_gate_passed": False,
                        }
                    )
                    row["result_hash"] = health._canonical_digest(payload)
            return result

    result = health.collect_governance_health(
        _ForgedOverallGateEngine(runs=[_completed_run()]),
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
        == "strategy overall gate differs from canonical three-window gates"
        for item in router_check["detail"]["errors"]
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


def test_rds_schema_health_never_inventories_database_triggers(monkeypatch):
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
    assert result["status"] == "PASS"
    checks = {check["name"]: check for check in result["checks"]}
    for name in (
        "strategy_metric_input_application_state_machine",
        "governance_append_only_application_integrity",
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
        "funding_provenance": "INTERNAL_PORTFOLIO_LEDGER_V1",
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


def test_application_integrity_health_contracts_need_no_trigger_inventory():
    class _NoDatabaseAccess:
        def execute(self, *_args, **_kwargs):
            raise AssertionError("database trigger inventory must not be queried")

    connection = _NoDatabaseAccess()
    metric_passed, metric = health._metric_input_review_trigger_check(connection)
    ledger_passed, ledger = health._governance_append_only_trigger_check(
        connection
    )

    assert metric_passed is ledger_passed is True
    assert metric["trigger_count"] == ledger["trigger_count"] == 0
    assert metric["database_triggers_required"] is False
    assert ledger["database_triggers_required"] is False
    assert "state_machine" in metric["enforcement"]
    assert "hash_replay" in ledger["enforcement"]
