from __future__ import annotations

import json
from copy import deepcopy
from datetime import date, timedelta

import pytest

from server.engine import strategy_governance as governance


def _session_window(records: list[dict], window: int) -> dict:
    sessions = [str(item["trade_date"]) for item in records]
    calendar = _calendar_binding(sessions)
    payload = {
        "schema": "probiga.authoritative-session-window.v1",
        "window_days": window,
        "start_date": sessions[0],
        "end_date": sessions[-1],
        "session_count": window,
        "sessions": sessions,
        "calendar_manifest_hash": calendar["manifest_hash"],
        "calendar_session_set_hash": calendar["session_set_hash"],
        "calendar_receipt_binding_hash": calendar["binding_hash"],
        "calendar_receipt": calendar,
    }
    return {**payload, "session_hash": governance._digest(payload)}


def _calendar_binding(sessions: list[str]) -> dict:
    payload = {
        "schema": "probiga.governance-calendar-receipt-binding.v1",
        "batch_id": "calendar-test",
        "known_at": f"{sessions[0]} 15:01:00",
        "start_date": min(sessions),
        "end_date": max(sessions),
        "session_count": len(sessions),
        "session_set_hash": "a" * 64,
        "manifest_hash": "b" * 64,
    }
    return {**payload, "binding_hash": governance._digest(payload)}


def _internal_metrics(
    window: int = 120, *, weak_segments: bool = False,
) -> tuple[dict, dict]:
    start = date(2026, 1, 1)
    records = []
    for index in range(window):
        segment = index * 5 // window
        if weak_segments and segment >= 3:
            value = -1.2 if index % 10 < 7 else 0.2
        else:
            value = 1.5 if index % 10 < 7 else -0.5
        records.append({
            "trade_date": (start + timedelta(days=index)).isoformat(),
            "return_pct": value,
            "actual_cost_pct": 0.1,
            "is_net_return": True,
            "evidence_revision_at": (
                start + timedelta(days=index)
            ).isoformat() + "T15:00:00",
        })
    metrics = governance.calculate_return_metrics(
        records,
        window_days=window,
        market_match_score=100.0,
        version_bound_evidence=True,
        independent_oos=True,
    )
    session_window = _session_window(records, window)
    metrics.update({
        "internal_daily_records": records,
        "funding_provenance": governance.CANONICAL_FUNDING_PROVENANCE,
        "internal_ledger_hash": "c" * 64,
        "internal_ledger_schema": (
            "probiga.internal-strategy-portfolio-ledger.v3"
        ),
        "drawdown_basis": "internal_version_bound_portfolio_equity",
        "cost_basis": "actual_ledger_fees",
        "portfolio_coverage_days": window,
        "session_window_valid": True,
        "session_window_count": window,
        "session_window_hash": session_window["session_hash"],
        "evidence_revision_at": records[-1]["evidence_revision_at"],
        "evidence_fresh": True,
        "selection_validation": {
            "status": "UNAVAILABLE",
            "status_label": "独立信号证据待补齐",
            "evidence_ready": False,
            "funding_authority": False,
            "real_order_authority": False,
        },
    })
    governance._apply_statistical_health(
        metrics, session_window=session_window,
    )
    metrics["profit_gate"] = governance.evaluate_window_gate(metrics)
    return metrics, session_window


def test_client_reported_walk_forward_cannot_create_funding_authority():
    metrics = governance.calculate_return_metrics(
        [], window_days=120, version_bound_evidence=True,
        independent_oos=True, walk_forward_verified=True,
    )
    metrics.update({
        "walk_forward_verified": True,
        "walk_forward_segments": 999,
        "positive_segments": 999,
        "selection_validation_fresh": True,
        "selection_validation_independent_oos": True,
        "selection_validation_scope": "VERSION_SELECTION_ONLY",
        "selection_validation_completed_trades": 999999,
        "selection_validation_coverage_days": 999999,
        "evidence_fresh": True,
    })
    governance._apply_statistical_health(metrics, session_window=None)

    gate = governance.evaluate_profit_gate(metrics)

    assert metrics["walk_forward_verified"] is False
    assert metrics["walk_forward_segments"] == 0
    assert metrics["positive_segments"] == 0
    assert gate["passed"] is False
    assert "内部时序分段合同" in gate["failed_checks"]
    assert "内部时序稳健性" in gate["failed_checks"]


def test_internal_negative_or_insufficient_segments_block_funding():
    metrics, _window = _internal_metrics(120, weak_segments=True)

    assert metrics["internal_forward_stability"][
        "positive_segment_count"
    ] == 3
    assert metrics["internal_forward_stability"]["passed"] is False
    assert metrics["profit_gate"]["passed"] is False
    assert "内部时序稳健性" in metrics["profit_gate"]["failed_checks"]


def test_internal_daily_nav_segments_and_hac_can_pass_without_external_artifact():
    metrics, _window = _internal_metrics(120)

    assert metrics["selection_validation"]["funding_authority"] is False
    assert metrics["internal_forward_stability"]["segment_count"] == 5
    assert metrics["internal_forward_stability"][
        "positive_segment_count"
    ] == 5
    assert metrics["internal_forward_stability"]["passed"] is True
    assert metrics["statistical_guard"]["threshold_passed"] is True
    assert metrics["profit_gate"]["passed"] is True


def test_point_profit_factor_above_one_does_not_override_hac_lcb():
    start = date(2026, 1, 1)
    records = [{
        "trade_date": (start + timedelta(days=index)).isoformat(),
        "return_pct": 1.05 if index % 2 == 0 else -1.0,
        "actual_cost_pct": 0.0,
        "is_net_return": True,
    } for index in range(120)]
    metrics = governance.calculate_return_metrics(
        records, window_days=120, version_bound_evidence=True,
        independent_oos=True,
    )
    window = _session_window(records, 120)
    metrics.update({
        "internal_daily_records": records,
        "funding_provenance": governance.CANONICAL_FUNDING_PROVENANCE,
        "internal_ledger_hash": "d" * 64,
        "session_window_hash": window["session_hash"],
    })
    governance._apply_statistical_health(metrics, session_window=window)

    assert metrics["profit_factor"] > 1
    assert metrics["statistical_guard"][
        "profit_factor_one_sided_95_lcb"
    ] < 1
    assert metrics["statistical_guard"]["threshold_passed"] is False


def _formal_version(
    key: str, version: str, *, holding: int, label: int,
) -> dict:
    evaluator_config = {
        "market_regime_multipliers": {
            state: 1.0 for state in governance.MARKET_REGIME_STATES
        },
        "market_router_policy_version": (
            governance.MARKET_ROUTER_POLICY_VERSION
        ),
        "market_state_config_version": "test",
        "market_state_config_hash": "f" * 64,
    }
    parameters = {
        "max_holding_days": holding,
        "label_horizon_days": label,
    }
    version_hash = governance._strategy_version_digest(
        strategy_key=key,
        version=version,
        evaluator_type="external_evidence",
        evaluator_config=evaluator_config,
        parameters=parameters,
        source_kind="runtime_registry",
    )
    return {
        "entity_key": key,
        "version": version,
        "version_hash": version_hash,
        "content_hash": governance._strategy_content_digest(
            strategy_key=key,
            evaluator_type="external_evidence",
            evaluator_config=evaluator_config,
            parameters=parameters,
            source_kind="runtime_registry",
        ),
        "evaluator_type": "external_evidence",
        "evaluator_config_json": json.dumps(evaluator_config),
        "parameters_json": json.dumps(parameters),
        "source_kind": "runtime_registry",
        "created_at": "2026-01-01T00:00:00",
    }


def test_server_inventory_counts_all_versions_and_deduplicates_promotion():
    champion = _formal_version(
        "alpha_strategy", "v1", holding=10, label=20,
    )
    rejected = _formal_version(
        "alpha_strategy", "v2", holding=12, label=25,
    )
    family = governance._build_statistical_trial_inventory(
        strategy_versions=[champion],
        combination_versions=[],
        challengers=[
            {
                "strategy_key": "alpha_strategy",
                "proposed_version": "v1",
                "proposed_version_hash": champion["version_hash"],
                "challenger_id": "1" * 32,
                "proposal_hash": "2" * 64,
                "status": "PROMOTED",
                "registration_payload": {
                    "parameters": json.loads(champion["parameters_json"]),
                },
            },
            {
                "strategy_key": "alpha_strategy",
                "proposed_version": "v2",
                "proposed_version_hash": rejected["version_hash"],
                "challenger_id": "3" * 32,
                "proposal_hash": "4" * 64,
                "status": "REJECTED",
                "registration_payload": {
                    "parameters": json.loads(rejected["parameters_json"]),
                },
            },
        ],
    )

    assert family["strategy"]["valid"] is True
    assert family["strategy"]["total_hypotheses"] == 2
    states = family["strategy"]["trial_state_rows"]
    assert {item["entity_version"] for item in states} == {"v1", "v2"}
    promoted = next(item for item in states if item["entity_version"] == "v1")
    assert promoted["source_kinds"] == ["CHALLENGER", "FORMAL_VERSION"]


def test_family_fdr_binds_every_missing_historical_trial_as_p_one(monkeypatch):
    current = _formal_version(
        "alpha_strategy", "v2", holding=10, label=20,
    )
    historical = _formal_version(
        "alpha_strategy", "v1", holding=10, label=20,
    )
    family = governance._build_statistical_trial_inventory(
        strategy_versions=[historical, current],
        combination_versions=[], challengers=[],
    )["strategy"]
    metrics, _window = _internal_metrics(120)
    metrics_60, _window_60 = _internal_metrics(60)
    captured = {}
    original = governance.benjamini_yekutieli_fdr

    def capture(p_values, **kwargs):
        captured.update(p_values)
        return original(p_values, **kwargs)

    monkeypatch.setattr(governance, "benjamini_yekutieli_fdr", capture)
    decisions, summary = governance._statistical_family_fdr_decisions(
        entity_type="STRATEGY",
        current_rows=[{
            "strategy_key": "alpha_strategy",
            "current_version": "v2",
            "version_hash": current["version_hash"],
        }],
        metrics_by_key={
            "alpha_strategy": {60: metrics_60, 120: metrics},
        },
        family=family,
    )

    historical_key = (
        f"STRATEGY:alpha_strategy:{historical['version_hash']}"
    )
    current_key = f"STRATEGY:alpha_strategy:{current['version_hash']}"
    assert set(captured) == set(family["trial_keys"])
    assert captured[historical_key] == 1.0
    assert 0.0 <= captured[current_key] < 1.0
    assert decisions["alpha_strategy"]["valid"] is True
    assert summary["total_hypotheses"] == 2


def test_bad_strategy_inventory_chain_fails_only_that_global_family_closed():
    invalid = _formal_version("broken_strategy", "v1", holding=5, label=5)
    invalid["version_hash"] = "0" * 64
    combination_members = [
        {"strategy_key": "a_strategy", "strategy_version": "v1", "weight": 0.5},
        {"strategy_key": "b_strategy", "strategy_version": "v1", "weight": 0.5},
    ]
    constraints = {}
    inventory = governance._build_statistical_trial_inventory(
        strategy_versions=[invalid],
        combination_versions=[{
            "entity_key": "stable_combo",
            "version": "v1",
            "config_hash": governance._digest({
                "members": combination_members, "constraints": constraints,
            }),
            "members_json": json.dumps(combination_members),
            "constraints_json": json.dumps(constraints),
            "created_at": "2026-01-01T00:00:00",
        }],
        challengers=[],
    )

    assert inventory["strategy"]["valid"] is False
    assert inventory["strategy"]["total_hypotheses"] == 0
    assert inventory["combination"]["valid"] is True
    assert inventory["combination"]["total_hypotheses"] == 1


def test_locked_inventory_drift_fails_the_persistence_transaction_closed():
    initial = governance._build_statistical_trial_inventory(
        strategy_versions=[
            _formal_version("alpha_strategy", "v1", holding=5, label=5),
        ],
        combination_versions=[], challengers=[],
    )
    locked = governance._build_statistical_trial_inventory(
        strategy_versions=[
            _formal_version("alpha_strategy", "v1", holding=5, label=5),
            _formal_version("alpha_strategy", "v2", holding=5, label=5),
        ],
        combination_versions=[], challengers=[],
    )

    governance._require_stable_statistical_inventory(initial, deepcopy(initial))
    with pytest.raises(RuntimeError, match="完整试验库存已改变"):
        governance._require_stable_statistical_inventory(initial, locked)


def test_persist_entry_holds_named_writer_lock_across_recursive_run(monkeypatch):
    from server.engine import strategy_center

    events = []

    class Result:
        def __init__(self, scalar):
            self._scalar = scalar

        def scalar(self):
            return self._scalar

    class Transaction:
        def __init__(self, connection):
            self.connection = connection

        def __enter__(self):
            self.connection.transaction = True
            events.append("BEGIN")
            return self.connection

        def __exit__(self, exc_type, _exc, _tb):
            events.append("ROLLBACK" if exc_type else "COMMIT_SCOPE")
            self.connection.transaction = False
            return False

    class Connection:
        def __init__(self):
            self.transaction = False

        def execute(self, statement, _params=None):
            sql = str(statement)
            if "GET_LOCK" in sql:
                events.append("GET_LOCK")
                return Result(1)
            if "RELEASE_LOCK" in sql:
                events.append("RELEASE_LOCK")
                return Result(1)
            raise AssertionError(sql)

        def commit(self):
            events.append("COMMIT")

        def rollback(self):
            events.append("ROLLBACK_EXPLICIT")
            self.transaction = False

        def in_transaction(self):
            return self.transaction

        def execution_options(self, **_kwargs):
            return self

        def begin(self):
            return Transaction(self)

        def close(self):
            events.append("CLOSE")

    connection = Connection()

    class Engine:
        def connect(self):
            return connection

    original = governance.governance_snapshot

    def recursive(**kwargs):
        events.append("INNER_RUN")
        assert kwargs["persist"] is True
        assert kwargs["_governance_connection"] is connection
        return {"ok": True}

    monkeypatch.setattr(governance, "ensure_and_seed_governance", lambda: None)
    monkeypatch.setattr(
        strategy_center, "ensure_strategy_center_tables", lambda: None,
    )
    monkeypatch.setattr(governance, "get_engine", lambda: Engine())
    monkeypatch.setattr(governance, "governance_snapshot", recursive)

    result = original(persist=True, strategy_snapshot={})

    assert result == {"ok": True}
    assert events.index("GET_LOCK") < events.index("INNER_RUN")
    assert events.index("INNER_RUN") < events.index("RELEASE_LOCK")
    assert events[-1] == "CLOSE"


def test_confirmation_gap_uses_maximum_holding_label_and_all_combo_members():
    first = _formal_version("first_strategy", "v1", holding=8, label=13)
    second = _formal_version("second_strategy", "v1", holding=30, label=20)
    inventory = governance._build_statistical_trial_inventory(
        strategy_versions=[first, second],
        combination_versions=[], challengers=[],
    )

    assert governance._inventory_confirmation_gap_sessions(
        "STRATEGY",
        {"strategy_key": "first_strategy", "current_version": "v1"},
        inventory,
    ) == 13
    assert governance._inventory_confirmation_gap_sessions(
        "COMBINATION",
        {
            "members": [
                {"strategy_key": "first_strategy", "strategy_version": "v1"},
                {"strategy_key": "second_strategy", "strategy_version": "v1"},
            ],
        },
        inventory,
    ) == 30


def _decision_and_confirmation() -> tuple[dict, dict]:
    decision_payload = {
        "schema": "probiga.strategy-family-by-decision-compact.v1",
        "valid": True,
        "passed": True,
        "candidate_p_value": 0.000001,
        "rank": 1,
        "critical_value": 0.000002,
        "total_hypotheses": 1000,
        "trial_inventory_hash": "a" * 64,
        "trial_inventory_state_hash": "b" * 64,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    confirmation_payload = {
        "schema": "probiga.strategy-spaced-confirmation-compact.v1",
        "valid": True,
        "passed": True,
        "reason": "通过",
        "minimum_new_sessions": 20,
        "required_total_confirmations": 3,
        "prior_confirmation_count": 2,
        "total_confirmation_count": 3,
        "continuous_session_count": 41,
        "milestone_count": 3,
        "milestone_set_hash": "c" * 64,
        "milestone_dates": ["2026-06-01", "2026-07-01", "2026-08-01"],
        "full_input_hash": "d" * 64,
        "full_parameter_hash": "e" * 64,
        "full_result_hash": "f" * 64,
        "statistical_policy_hash": governance.STATISTICAL_POLICY_HASH,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    return (
        {**decision_payload, "decision_hash": governance._digest(decision_payload)},
        {
            **confirmation_payload,
            "compact_hash": governance._digest(confirmation_payload),
        },
    )


def test_thousand_statistical_entities_stay_below_canonical_four_mib():
    metrics = {}
    for window in governance.WINDOWS:
        current, _session = _internal_metrics(window)
        metrics[str(window)] = current
    decision, confirmation = _decision_and_confirmation()
    template = {
        "strategy_key": "strategy_0000",
        "strategy_name": "容量策略",
        "current_version": "v1",
        "current_status": "ACTIVE",
        "projected_status": "ACTIVE",
        "enabled": True,
        "ranking_score": 88.0,
        "metrics": metrics,
        "primary_metrics": metrics["60"],
        "profit_gate_passed": True,
        "paper_allocation_eligible": True,
        "statistical_family_decision": decision,
        "statistical_family_passed": True,
        "confirmation_guard": confirmation,
        "pre_confirmation_funding_gate_hash": "8" * 64,
        "funding_gate_hash": "9" * 64,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    projected = governance._canonical_competition_rows(
        [
            {**template, "strategy_key": f"strategy_{index:04d}"}
            for index in range(1000)
        ],
        entity_type="STRATEGY",
    )

    assert len(governance._json_text({
        "strategies": projected,
    }).encode("utf-8")) < 4 * 1024 * 1024
    assert "internal_daily_records" not in governance._json_text(projected)


def test_spaced_confirmation_rejects_v6_history_and_accepts_exact_v7_gap():
    latest = date(2026, 8, 24)
    sessions = [
        (latest - timedelta(days=index)).isoformat() for index in range(41)
    ]
    history = []
    for index, day in enumerate(sessions[1:], 1):
        evidence = {
            "decision_contract_version": governance.STATISTICAL_DECISION_CONTRACT,
            "overall_profit_gate_passed": True,
            "statistical_family_passed": True,
            "pre_confirmation_funding_gate_hash": f"{index + 1:064x}",
            "funding_evidence_revision_at": f"{day}T15:00:00",
        }
        history.append({
            "trade_date": day,
            "profit_gate_passed": 1,
            "evidence_json": json.dumps(evidence),
        })
    accepted = governance._spaced_confirmation_guard(
        trade_date=sessions[0],
        pre_confirmation_funding_gate_hash=f"{1:064x}",
        funding_evidence_revision_at=f"{sessions[0]}T15:00:00",
        history_rows=history,
        authoritative_sessions_desc=sessions,
        minimum_new_sessions=20,
        suspension_boundary=None,
        calendar_receipt_binding=_calendar_binding(sessions),
    )
    legacy = deepcopy(history)
    for item in legacy:
        evidence = json.loads(item["evidence_json"])
        evidence["decision_contract_version"] = "strategy-governance-decision.v6"
        item["evidence_json"] = json.dumps(evidence)
    rejected = governance._spaced_confirmation_guard(
        trade_date=sessions[0],
        pre_confirmation_funding_gate_hash=f"{1:064x}",
        funding_evidence_revision_at=f"{sessions[0]}T15:00:00",
        history_rows=legacy,
        authoritative_sessions_desc=sessions,
        minimum_new_sessions=20,
        suspension_boundary=None,
        calendar_receipt_binding=_calendar_binding(sessions),
    )

    assert accepted["valid"] is True
    assert accepted["passed"] is True
    assert accepted["total_confirmation_count"] == 3
    assert rejected["valid"] is False
    assert rejected["passed"] is False


def test_spaced_confirmation_fails_closed_without_immutable_calendar_receipt():
    sessions = [
        (date(2026, 8, 24) - timedelta(days=index)).isoformat()
        for index in range(41)
    ]
    result = governance._spaced_confirmation_guard(
        trade_date=sessions[0],
        pre_confirmation_funding_gate_hash="1" * 64,
        funding_evidence_revision_at=f"{sessions[0]}T15:00:00",
        history_rows=[],
        authoritative_sessions_desc=sessions,
        minimum_new_sessions=20,
        suspension_boundary=None,
        calendar_receipt_binding=None,
    )

    assert result["valid"] is False
    assert result["passed"] is False
    assert "不可变QMT交易日历回执" in result["reason"]


def test_final_funding_hash_binds_pre_gate_family_and_confirmation():
    decision, confirmation = _decision_and_confirmation()
    row = {
        "strategy_key": "alpha_strategy",
        "current_version": "v1",
        "current_status": "ACTIVE",
        "projected_status": "ACTIVE",
        "pre_confirmation_funding_gate_hash": "1" * 64,
        "statistical_family_decision": decision,
        "confirmation_guard": confirmation,
        "paper_allocation_eligible": True,
    }
    original = governance._finalize_funding_gate_hash(
        row, entity_type="STRATEGY",
    )
    changed = deepcopy(row)
    changed["confirmation_guard"]["compact_hash"] = "2" * 64

    assert original != governance._finalize_funding_gate_hash(
        changed, entity_type="STRATEGY",
    )
    assert len(original) == 64


def test_canonical_selection_summary_is_explicitly_non_funding():
    summary = governance._selection_validation_summary({
        "independent_oos": True,
        "walk_forward_verified": True,
        "evidence_protocol": "PURGED_WALK_FORWARD_V2",
        "verification_status": "CONFIRMED",
        "review_audit_valid": True,
        "artifact_hash": "a" * 64,
        "source_dataset_hash": "b" * 64,
        "completed_trades": 100,
        "coverage_days": 120,
    })

    assert summary["evidence_ready"] is True
    assert summary["evidence_scope"] == "VERSION_SELECTION_ONLY"
    assert summary["funding_authority"] is False
    assert summary["real_order_authority"] is False


def test_canonical_window_source_root_binds_nested_statistical_proofs():
    metrics, _window = _internal_metrics(120)
    original = governance._canonical_metric_window_summary(
        metrics,
        entity_type="STRATEGY",
        entity_key="alpha_strategy",
        entity_version="v1",
        window_days=120,
    )
    changed = deepcopy(metrics)
    changed["statistical_guard"]["compact_hash"] = "f" * 64
    revised = governance._canonical_metric_window_summary(
        changed,
        entity_type="STRATEGY",
        entity_key="alpha_strategy",
        entity_version="v1",
        window_days=120,
    )

    assert len(original["source_root"]) == 64
    assert original["source_root"] != revised["source_root"]
