from __future__ import annotations

from copy import deepcopy
import json

import pytest

from server.engine import strategy_challenger_factory as factory
from server.engine import strategy_governance as governance


CHALLENGER_ID = "c" * 32
ARTIFACT_HASH = "a" * 64
DATASET_HASH = "b" * 64


def _metrics() -> dict:
    return {
        "completed_trades": 120,
        "coverage_days": 80,
        "win_rate_pct": 75.0,
        "average_win_pct": 1.0,
        "average_loss_pct": 0.5,
        "payoff_ratio": 2.0,
        "gross_expectancy_pct": 0.6,
        "estimated_cost_pct": 0.1,
        "net_expectancy_pct": 0.5,
        "profit_factor": 3.0,
        "max_drawdown_pct": 5.0,
        "walk_forward_segments": 5,
        "positive_segments": 4,
        "cost_stress_expectancy_pct": 0.45,
        "top5_profit_contribution_pct": 40.0,
        "market_match_score": 100.0,
        "walk_forward_verified": True,
        "independent_oos": True,
        "drawdown_basis": "sequential_trade_compounded_equity",
        "cost_basis": "validated_fee_model_v1",
    }


def _selected() -> dict:
    registration = {
        "strategy_key": "artifact_test_strategy",
        "strategy_name": "挑战者测试策略",
        "version": "v2",
        "category": "测试",
        "family_key": "artifact_test_strategy",
        "description": "fixture",
        "evaluator_type": "external_evidence",
        "evaluator_config": {"market_regime_multipliers": {
            "trend_bullish": 1.0,
            "high_range": 0.5,
            "risk_declining": 0.2,
            "extreme_event": 0.0,
        }},
        "parameters": {
            "max_holding_days": 2,
            "label_horizon_days": 2,
        },
        "reason": "fixture",
        "owner_name": "user-id:1",
    }
    proposal_hash = governance._digest({
        "strategy_key": registration["strategy_key"],
        "parent_version": "v1",
        "proposed_version_hash": "d" * 64,
        "registration_payload": registration,
    })
    return {
        "challenger_id": CHALLENGER_ID,
        "strategy_key": registration["strategy_key"],
        "parent_version": "v1",
        "proposed_version": "v2",
        "proposed_version_hash": "d" * 64,
        "proposal_hash": proposal_hash,
        "registration_payload": registration,
        "submitted_by": "user-id:1",
        "submitted_at": "2026-01-01T10:00:00",
        "status": "VALIDATING",
    }


def _frozen_evidence(selected: dict | None = None) -> dict:
    selected = selected or _selected()
    artifact = {
        "source_dataset_hash": DATASET_HASH,
        "trades": [{"trade_date": "2026-01-02"}],
    }
    return {
        "schema": "probiga.strategy-challenger-evidence-submission.v1",
        "challenger_id": CHALLENGER_ID,
        "proposal_hash": selected["proposal_hash"],
        "proposed_version_hash": selected["proposed_version_hash"],
        "proposal_submitted_at": selected["submitted_at"],
        "submitted_by": "user-id:1",
        "as_of_date": "2026-08-21",
        "window_days": 120,
        "evidence_protocol": "PURGED_WALK_FORWARD_V2",
        "evidence_revision_at": "2026-08-21T15:00:00",
        "metrics": _metrics(),
        "artifact_manifest": artifact,
        "artifact_hash": ARTIFACT_HASH,
        "source_dataset_hash": DATASET_HASH,
        "server_replay_validation_hash": "e" * 64,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def _audit_row(
    *, action: str, after: dict, evidence: dict, operator: str,
    created_at: str,
) -> dict:
    _sql, params = governance._audit_record(
        entity_type="STRATEGY",
        entity_key="artifact_test_strategy",
        action=action,
        reason="test",
        operator=operator,
        before={},
        after=after,
        evidence=evidence,
    )
    return {
        "audit_id": params["audit_id"],
        "action": action,
        "operator_name": operator,
        "payload_json": params["payload_json"],
        "audit_hash": params["audit_hash"],
        "created_at": created_at,
    }


def _proposal_row() -> tuple[dict, dict]:
    selected = _selected()
    proposal = {
        key: selected[key] for key in (
            "challenger_id", "strategy_key", "parent_version",
            "proposed_version", "proposed_version_hash", "proposal_hash",
            "registration_payload",
        )
    }
    proposal.update({
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    })
    return selected, _audit_row(
        action="REGISTER_CHALLENGER",
        after=proposal,
        evidence={"challenger_id": CHALLENGER_ID},
        operator="user-id:1",
        created_at=selected["submitted_at"],
    )


def _submission_row(selected: dict) -> tuple[dict, dict]:
    base = _frozen_evidence(selected)
    submission = {
        **base,
        "evidence_submission_hash": governance._digest(base),
    }
    return submission, _audit_row(
        action="SUBMIT_CHALLENGER_EVIDENCE",
        after={
            "challenger_id": CHALLENGER_ID,
            "status": "REVIEW_PENDING",
            "evidence_submission_hash": submission[
                "evidence_submission_hash"
            ],
            "artifact_hash": ARTIFACT_HASH,
            "source_dataset_hash": DATASET_HASH,
        },
        evidence=submission,
        operator="user-id:1",
        created_at="2026-08-21T16:00:00",
    )


def _review_row(
    selected: dict, submission: dict, *, reviewer: str = "user-id:2",
    decision: str = "CONFIRM",
) -> tuple[dict, dict]:
    gate = factory._validate_challenger_evidence(
        _metrics(),
        evidence_protocol="PURGED_WALK_FORWARD_V2",
        window_days=120,
        artifact_hash=ARTIFACT_HASH,
        source_dataset_hash=DATASET_HASH,
        artifact_replayed=True,
    )
    payload = {
        "schema": "probiga.strategy-challenger-review.v2",
        "challenger_id": CHALLENGER_ID,
        "proposal_hash": selected["proposal_hash"],
        "proposed_version_hash": selected["proposed_version_hash"],
        "evidence_submission_hash": submission["evidence_submission_hash"],
        "artifact_hash": ARTIFACT_HASH,
        "source_dataset_hash": DATASET_HASH,
        "reviewer": reviewer,
        "decision": decision,
        "review_reason": "fixture",
        "gate_validation": gate,
        "passed": decision == "CONFIRM" and gate["passed"] is True,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    review = {**payload, "validation_hash": governance._digest(payload)}
    return review, _audit_row(
        action="REVIEW_CHALLENGER",
        after={
            "challenger_id": CHALLENGER_ID,
            "status": "READY" if review["passed"] else "REJECTED",
            "validation_hash": review["validation_hash"],
        },
        evidence=review,
        operator=reviewer,
        created_at="2026-08-21T17:00:00",
    )


def test_self_reported_metrics_and_deprecated_claims_cannot_pass():
    metrics = _metrics()
    metrics.update({
        "deflated_sharpe_probability": 1.0,
        "probability_of_backtest_overfitting": 0.0,
        "false_discovery_rate_q": 0.0,
    })
    validation = factory._validate_challenger_evidence(metrics)
    assert validation["passed"] is False
    assert validation["checks"]["server_replayed_artifact"] is False
    assert "deflated_sharpe" not in validation["checks"]
    assert "pbo" not in validation["checks"]
    assert "false_discovery_rate" not in validation["checks"]
    assert not any(
        key in factory.CHALLENGER_POLICY
        for key in (
            "minimum_deflated_sharpe_probability",
            "maximum_probability_of_backtest_overfitting",
            "maximum_false_discovery_rate_q",
        )
    )


def test_frozen_artifact_replay_binds_proposal_time_version_and_horizons(
    monkeypatch,
):
    selected = _selected()
    evidence = _frozen_evidence(selected)
    calls = []

    def validate(raw, **kwargs):
        calls.append((deepcopy(raw), kwargs))
        assert kwargs["version_created_at"] == selected["submitted_at"]
        assert kwargs["entity_key"] == selected["strategy_key"]
        assert kwargs["entity_version"] == selected["proposed_version"]
        assert kwargs["expected_max_holding_days"] == 2
        assert kwargs["expected_label_horizon_days"] == 2
        assert kwargs["metrics"] == governance._validated_metric_evidence(
            _metrics()
        )
        if raw != evidence["artifact_manifest"]:
            raise ValueError("artifact tampered")
        if raw["trades"][0]["trade_date"] <= kwargs[
            "version_created_at"
        ][:10]:
            raise ValueError("time travel")
        if kwargs["artifact_hash"] != ARTIFACT_HASH:
            raise ValueError("artifact hash differs")
        return raw

    monkeypatch.setattr(factory, "_validate_metric_artifact", validate)
    gate = factory._replay_frozen_challenger_evidence(selected, evidence)
    assert gate["passed"] is True
    assert len(calls) == 1
    assert gate["artifact_hash"] == ARTIFACT_HASH
    assert gate["source_dataset_hash"] == DATASET_HASH


@pytest.mark.parametrize("tamper", ("artifact", "artifact_hash", "time_travel"))
def test_frozen_artifact_tampering_and_time_travel_fail_closed(
    monkeypatch, tamper,
):
    selected = _selected()
    evidence = _frozen_evidence(selected)
    if tamper == "artifact":
        evidence["artifact_manifest"] = {
            **evidence["artifact_manifest"], "x": 1,
        }
    elif tamper == "artifact_hash":
        evidence["artifact_hash"] = "f" * 64
    else:
        evidence["artifact_manifest"]["trades"][0]["trade_date"] = (
            selected["submitted_at"][:10]
        )

    def validate(raw, **kwargs):
        if raw != _frozen_evidence(selected)["artifact_manifest"]:
            raise ValueError("artifact tampered")
        if kwargs["artifact_hash"] != ARTIFACT_HASH:
            raise ValueError("artifact hash differs")
        if raw["trades"][0]["trade_date"] <= kwargs[
            "version_created_at"
        ][:10]:
            raise ValueError("time travel")
        return raw

    monkeypatch.setattr(factory, "_validate_metric_artifact", validate)
    with pytest.raises(ValueError):
        factory._replay_frozen_challenger_evidence(selected, evidence)


def test_two_stage_audit_replay_requires_frozen_evidence_and_separate_reviewer():
    selected, proposal_row = _proposal_row()
    submission, submission_row = _submission_row(selected)
    review, review_row = _review_row(selected, submission)
    replayed = factory._challengers_from_events([
        proposal_row, submission_row, review_row,
    ])
    assert replayed[0]["status"] == "READY"
    assert replayed[0]["evidence_submission"]["artifact_hash"] == ARTIFACT_HASH
    assert replayed[0]["latest_validation"]["validation_hash"] == (
        review["validation_hash"]
    )
    assert replayed[0]["reviewed_by"] == "user-id:2"

    _same_review, same_reviewer_row = _review_row(
        selected, submission, reviewer="user-id:1"
    )
    with pytest.raises(RuntimeError, match="复核审计合同无效"):
        factory._challengers_from_events([
            proposal_row, submission_row, same_reviewer_row,
        ])
    with pytest.raises(RuntimeError, match="审计合同无效"):
        factory._challengers_from_events([proposal_row, review_row])


def test_audit_tamper_and_promotion_validation_hash_tamper_fail_closed():
    selected, proposal_row = _proposal_row()
    submission, submission_row = _submission_row(selected)
    review, review_row = _review_row(selected, submission)
    promotion = _audit_row(
        action="PROMOTE_CHALLENGER",
        after={
            "challenger_id": CHALLENGER_ID,
            "new_version": "v2",
            "new_status": "SHADOW",
        },
        evidence={
            "challenger_id": CHALLENGER_ID,
            "proposal_hash": selected["proposal_hash"],
            "validation_hash": review["validation_hash"],
            "evidence_submission_hash": submission[
                "evidence_submission_hash"
            ],
            "artifact_hash": ARTIFACT_HASH,
            "source_dataset_hash": DATASET_HASH,
            "promoted_version_hash": selected["proposed_version_hash"],
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        },
        operator="user-id:1",
        created_at="2026-08-21T18:00:00",
    )
    replayed = factory._challengers_from_events([
        proposal_row, submission_row, review_row, promotion,
    ])
    assert replayed[0]["status"] == "PROMOTED"

    forged = dict(promotion)
    envelope = json.loads(forged["payload_json"])
    envelope["evidence"]["validation_hash"] = "0" * 64
    forged["payload_json"] = json.dumps(envelope, ensure_ascii=False)
    with pytest.raises(RuntimeError, match="哈希漂移"):
        factory._challengers_from_events([
            proposal_row, submission_row, review_row, forged,
        ])


class _Result:
    def __init__(self, rows=None, scalar_value=None):
        self.rows = list(rows or [])
        self.scalar_value = scalar_value

    def mappings(self):
        return self

    def first(self):
        return self.rows[0] if self.rows else None

    def all(self):
        return self.rows

    def scalar(self):
        return self.scalar_value


class _Context:
    def __init__(self, value):
        self.value = value

    def __enter__(self):
        return self.value

    def __exit__(self, *_args):
        return False


class _RegistryConnection:
    def __init__(self, existing):
        self.existing = existing
        self.sql = []

    def execute(self, statement, _params=None):
        sql = str(statement)
        self.sql.append(sql)
        if "st_strategy_governance_schema_migration" in sql:
            return _Result([{"migration_key": "lock"}])
        if "FROM st_strategy_registry" in sql:
            return _Result(
                [{"current_version": "v1"}] if self.existing else []
            )
        raise AssertionError(sql)


class _RegistryEngine:
    def __init__(self, connection):
        self.connection = connection

    def begin(self):
        return _Context(self.connection)


class _AuditConnection:
    def __init__(self):
        self.events = []
        self.sql = []

    def execute(self, statement, _params=None):
        sql = str(statement)
        self.sql.append(sql)
        if "st_strategy_governance_schema_migration" in sql:
            return _Result([{"migration_key": "lock"}])
        if "FROM st_strategy_registry" in sql:
            return _Result([{"current_version": "v1"}])
        if "SELECT COUNT(*) FROM st_strategy_version" in sql:
            return _Result(scalar_value=0)
        if "FROM st_strategy_metric_input" in sql:
            return _Result([])
        if "SELECT COUNT(*) FROM st_strategy_governance_audit" in sql:
            return _Result(scalar_value=len(self.events))
        if "action='SUBMIT_CHALLENGER_EVIDENCE'" in sql:
            return _Result([
                row for row in self.events
                if row["action"] == "SUBMIT_CHALLENGER_EVIDENCE"
            ])
        if "AND action IN" in sql:
            return _Result(self.events)
        raise AssertionError(sql)


class _AuditEngine:
    def __init__(self, connection):
        self.connection = connection

    def begin(self):
        return _Context(self.connection)

    def connect(self):
        return _Context(self.connection)


def test_existing_key_registry_bypass_is_blocked_under_database_lock(
    monkeypatch,
):
    connection = _RegistryConnection(existing=True)
    called = []
    monkeypatch.setattr(factory, "ensure_and_seed_governance", lambda: None)
    monkeypatch.setattr(factory, "get_engine", lambda: _RegistryEngine(connection))
    monkeypatch.setattr(
        factory, "register_strategy",
        lambda *_args, **_kwargs: called.append(True),
    )
    with pytest.raises(factory.StrategyAlreadyRegisteredError):
        factory.register_new_strategy(
            {"strategy_key": "artifact_test_strategy"}, operator="user-id:1"
        )
    assert called == []
    assert "FOR UPDATE" in connection.sql[0]
    assert "FOR UPDATE" in connection.sql[1]


def test_brand_new_key_registry_path_still_registers(monkeypatch):
    connection = _RegistryConnection(existing=False)
    observed = {}

    def _register(payload, *, operator, _global_inventory_lock_held=False):
        observed["global_inventory_lock_held"] = _global_inventory_lock_held
        return {
            "strategy_key": payload["strategy_key"], "operator": operator,
        }

    monkeypatch.setattr(factory, "ensure_and_seed_governance", lambda: None)
    monkeypatch.setattr(factory, "get_engine", lambda: _RegistryEngine(connection))
    monkeypatch.setattr(factory, "register_strategy", _register)
    result = factory.register_new_strategy(
        {"strategy_key": "brand_new_strategy"}, operator="user-id:1"
    )
    assert result == {
        "strategy_key": "brand_new_strategy", "operator": "user-id:1"
    }
    assert observed == {"global_inventory_lock_held": True}


def test_artifact_and_dataset_cannot_be_reused_across_challengers():
    selected, _proposal = _proposal_row()
    submission, submission_row = _submission_row(selected)

    class Connection:
        def execute(self, statement, _params=None):
            if "FROM st_strategy_metric_input" in str(statement):
                return _Result([])
            return _Result([submission_row])

    with pytest.raises(ValueError, match="验证产物"):
        factory._assert_challenger_evidence_unclaimed(
            Connection(),
            challenger_id="f" * 32,
            artifact_hash=ARTIFACT_HASH,
            source_dataset_hash="9" * 64,
        )
    with pytest.raises(ValueError, match="底层样本集"):
        factory._assert_challenger_evidence_unclaimed(
            Connection(),
            challenger_id="f" * 32,
            artifact_hash="9" * 64,
            source_dataset_hash=DATASET_HASH,
        )
    assert submission["artifact_hash"] == ARTIFACT_HASH


@pytest.mark.parametrize(
    ("metric_row", "message"),
    [
        ({
            "evidence_id": "metric-artifact",
            "artifact_hash": ARTIFACT_HASH,
            "source_dataset_hash": "8" * 64,
        }, "验证产物已经被普通指标证据占用"),
        ({
            "evidence_id": "metric-dataset",
            "artifact_hash": "8" * 64,
            "source_dataset_hash": DATASET_HASH,
        }, "底层样本集已经被普通指标证据占用"),
    ],
)
def test_challenger_cannot_reuse_regular_metric_evidence(
    metric_row, message,
):
    class Connection:
        def __init__(self):
            self.sql = []

        def execute(self, statement, _params=None):
            self.sql.append(str(statement))
            return _Result([metric_row])

    connection = Connection()
    with pytest.raises(ValueError, match=message):
        factory._assert_challenger_evidence_unclaimed(
            connection,
            challenger_id="f" * 32,
            artifact_hash=ARTIFACT_HASH,
            source_dataset_hash=DATASET_HASH,
        )
    assert "st_strategy_metric_input" in connection.sql[0]
    assert "FOR UPDATE" in connection.sql[0]


def test_submit_then_review_replays_the_same_frozen_artifact_twice(monkeypatch):
    connection = _AuditConnection()
    engine = _AuditEngine(connection)
    replay_calls = []
    timestamps = {
        "REGISTER_CHALLENGER": "2026-01-01T10:00:00",
        "SUBMIT_CHALLENGER_EVIDENCE": "2026-08-21T16:00:00",
        "REVIEW_CHALLENGER": "2026-08-21T17:00:00",
    }

    def append(
        _connection, *, entity_type, entity_key, action, reason, operator,
        before, after, evidence,
    ):
        _sql, params = governance._audit_record(
            entity_type=entity_type,
            entity_key=entity_key,
            action=action,
            reason=reason,
            operator=operator,
            before=before,
            after=after,
            evidence=evidence,
        )
        connection.events.append({
            "audit_id": params["audit_id"],
            "entity_key": entity_key,
            "action": action,
            "operator_name": operator,
            "payload_json": params["payload_json"],
            "audit_hash": params["audit_hash"],
            "created_at": timestamps[action],
        })

    def validate(raw, **kwargs):
        replay_calls.append({
            "artifact": deepcopy(raw),
            "version_created_at": kwargs["version_created_at"],
            "metrics": deepcopy(kwargs["metrics"]),
        })
        return raw

    monkeypatch.setattr(factory, "ensure_and_seed_governance", lambda: None)
    monkeypatch.setattr(factory, "get_engine", lambda: engine)
    monkeypatch.setattr(factory, "_append_audit_connection", append)
    monkeypatch.setattr(factory, "_validate_metric_artifact", validate)
    monkeypatch.setattr(factory, "load_registry", lambda: [{
        "strategy_key": "artifact_test_strategy",
        "current_version": "v1",
    }])
    challenger = factory.register_strategy_challenger({
        "strategy_key": "artifact_test_strategy",
        "strategy_name": "挑战者测试策略",
        "version": "v2",
        "evaluator_type": "external_evidence",
        "evaluator_config": {"market_regime_multipliers": {
            "trend_bullish": 1.0,
            "high_range": 0.5,
            "risk_declining": 0.2,
            "extreme_event": 0.0,
        }},
        "parameters": {
            "max_holding_days": 2,
            "label_horizon_days": 2,
        },
        "reason": "fixture",
    }, operator="user-id:1")
    artifact = _frozen_evidence()["artifact_manifest"]
    submitted = factory.submit_strategy_challenger_evidence(
        challenger["challenger_id"],
        {
            "as_of_date": "2026-08-21",
            "window_days": 120,
            "evidence_protocol": "PURGED_WALK_FORWARD_V2",
            "evidence_revision_at": "2026-08-21T15:00:00",
            "metrics": _metrics(),
            "artifact_manifest": artifact,
            "artifact_hash": ARTIFACT_HASH,
        },
        operator="user-id:1",
        reason="冻结产物",
    )
    reviewed = factory.review_strategy_challenger(
        challenger["challenger_id"],
        "CONFIRM",
        operator="user-id:2",
        reason="独立重放",
    )

    assert submitted["status"] == "REVIEW_PENDING"
    assert reviewed["status"] == "READY"
    assert len(replay_calls) == 2
    assert replay_calls[0] == replay_calls[1]
    assert replay_calls[0]["version_created_at"] == "2026-01-01T10:00:00"
    assert reviewed["latest_validation"]["reviewer"] == "user-id:2"


def test_unfiltered_challenger_listing_uses_two_queries_not_registry_n_plus_one(
    monkeypatch,
):
    connection = _AuditConnection()
    engine = _AuditEngine(connection)
    monkeypatch.setattr(factory, "ensure_and_seed_governance", lambda: None)
    monkeypatch.setattr(factory, "get_engine", lambda: engine)
    monkeypatch.setattr(
        factory,
        "load_registry",
        lambda: (_ for _ in ()).throw(
            AssertionError("challenger listing must not enumerate registry rows")
        ),
    )

    result = factory.list_strategy_challengers()

    assert result["challengers"] == []
    selects = [
        sql for sql in connection.sql if sql.lstrip().upper().startswith("SELECT")
    ]
    assert len(selects) == 2
    assert "COUNT(*)" in selects[0]
    assert "LIMIT" in selects[1]


def test_challenger_policy_never_claims_order_authority():
    assert factory.CHALLENGER_POLICY["automatic_real_order_submission"] is False
    assert factory.CHALLENGER_POLICY["real_order_authority"] is False
    assert factory.CHALLENGER_POLICY["minimum_walk_forward_segments"] >= 5
