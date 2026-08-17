from __future__ import annotations

import hashlib
import json
import sys
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

from server.trading_v3.config import load_v3_config
from server.trading_v3.horizon_contracts import HorizonOutcomeEvidence
from server.trading_v3.learning_intelligence import counterfactual_learning_metrics
from server.trading_v3.release_governance import (
    CalibrationGateDecision,
    ContinuousCalibrationEvidence,
)
from server.trading_v3.shadow_intelligence_repository import (
    ShadowIntelligenceRepository,
    _hash,
    _validate_outcome_evidence,
)
from server.trading_v3.shadow_intelligence_schema import (
    SHADOW_INTELLIGENCE_DDL,
    _normalize_mysql84_check_clause,
)
from server.trading_v3.shadow_intelligence_worker import (
    _model_specs,
    materialize_horizon_outcomes,
    score_proxy_model,
)


UTC = timezone.utc


def _digest(value) -> str:
    return hashlib.sha256(json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")).hexdigest()


def test_three_proxy_artifacts_and_feature_protocols_are_independent():
    specs = _model_specs(load_v3_config())
    assert {item["horizon_days"] for item in specs} == {1, 5, 20}
    assert len({item["feature_protocol_hash"] for item in specs}) == 3
    assert len({item["model_artifact_hash"] for item in specs}) == 3
    assert len({item["scorer_source_hash"] for item in specs}) == 1
    assert len(specs[0]["scorer_source_hash"]) == 64

    features = {
        "intraday_amount_surprise_z": 2.0,
        "price_vs_vwap_pct": 1.0,
        "interval_return_pct": 1.0,
        "sector_breadth_pct": 60.0,
        "fill_probability": 0.7,
        "spread_bps": 20.0,
        "return_5d_pct": 3.0,
        "latest_change_pct": 1.0,
        "amount_ratio_5_20": 1.2,
        "sector_relative_return_pct": 2.0,
        "distance_ma20_pct": 1.0,
        "quality_percentile": 0.8,
        "growth_percentile": 0.7,
        "cashflow_quality_percentile": 0.75,
        "valuation_percentile": 0.6,
        "momentum_60d_percentile": 0.8,
        "volatility_20d_percentile": 0.2,
    }
    baseline = {
        item["horizon_days"]: score_proxy_model(features, spec=item)["score"]
        for item in specs
    }
    changed = {**features, "intraday_amount_surprise_z": 5.0}
    observed = {
        item["horizon_days"]: score_proxy_model(changed, spec=item)["score"]
        for item in specs
    }
    assert observed[1] != baseline[1]
    assert observed[5] == baseline[5]
    assert observed[20] == baseline[20]


class _OutcomeRepository:
    def __init__(self, contracts):
        self.contracts = contracts
        self.saved = []

    def mature_horizon_contracts(self, *, evaluation_date, limit=10000):
        assert evaluation_date == date(2026, 9, 15)
        return list(self.contracts)

    def save_horizon_outcomes(self, rows, *, created_at):
        self.saved = list(rows)
        return {
            "status": "ok",
            "inserted_count": len(self.saved),
            "existing_count": 0,
            "outcome_ids": [item.contract_id for item, _ in self.saved],
        }


def _market_engine(*, corporate_action=False, qmt_attested=True):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE si_trade_calendar (trade_date DATE, trade_status INT)"
        ))
        connection.execute(text(
            "CREATE TABLE sm_stock_kline ("
            "stock_code TEXT, trade_date DATE, k_type INT, adjust_type INT, "
            "open REAL, high REAL, low REAL, close REAL, pre_close REAL, "
            "amount REAL, etl_sync_at TEXT, data_source TEXT, "
            "quality_status TEXT, source_time TEXT, received_at TEXT, "
            "batch_id TEXT, data_version TEXT, permission_status TEXT)"
        ))
        start = date(2026, 8, 17)
        sessions = []
        cursor = start
        while len(sessions) < 21:
            if cursor.weekday() < 5:
                sessions.append(cursor)
            cursor += timedelta(days=1)
        previous_close = 100.0
        for index, session in enumerate(sessions):
            close = 100.0 + index
            pre_close = previous_close
            if corporate_action and index == 3:
                pre_close = previous_close * 0.8
            connection.execute(
                text(
                    "INSERT INTO si_trade_calendar VALUES (:d, 1)"
                ),
                {"d": session},
            )
            connection.execute(
                text(
                    "INSERT INTO sm_stock_kline VALUES ("
                    "'600001', :d, 1, 0, :o, :h, :l, :c, :p, 1000000, :etl, "
                    "'gj_big_qmt_inner', :quality_status, :source_time, "
                    ":received_at, :batch_id, 'bigqmt-v1', 'AUTHORIZED')"
                ),
                {
                    "d": session,
                    "o": previous_close,
                    "h": max(previous_close, close) + 1,
                    "l": min(previous_close, close) - 1,
                    "c": close,
                    "p": pre_close,
                    "etl": datetime.combine(
                        session + timedelta(days=1), datetime.min.time()
                    ).replace(tzinfo=UTC).isoformat(),
                    "source_time": datetime.combine(
                        session, datetime.min.time()
                    ).replace(tzinfo=UTC).isoformat(),
                    "received_at": datetime.combine(
                        session + timedelta(days=1), datetime.min.time()
                    ).replace(tzinfo=UTC).isoformat(),
                    "batch_id": f"qmt-{session.isoformat()}",
                    "quality_status": (
                        "QMT_ATTESTED" if qmt_attested else "RAW"
                    ),
                },
            )
            previous_close = close
    return engine, sessions


def _contracts(sessions):
    rows = []
    for horizon in (1, 5, 20):
        rows.append({
            "contract_id": _digest({"kind": "contract", "horizon": horizon}),
            "contract_hash": _digest({"kind": "contract_hash", "horizon": horizon}),
            "stock_code": "600001",
            "horizon_days": horizon,
            "entry_trade_date": sessions[0],
            "earliest_exit_trade_date": sessions[1],
            "outcome_matures_on": sessions[horizon],
            "entry_session_sequence": 1,
            "earliest_exit_session_sequence": 2,
            "outcome_maturity_session_sequence": horizon + 1,
            "cost_assumption_pct": 0.2,
            "cost_model_version": "ROUNDTRIP_COST_ASSUMPTION_V1",
        })
    return rows


def test_contract_id_outcomes_use_distinct_frozen_horizon_exits_and_costs():
    engine, sessions = _market_engine()
    repository = _OutcomeRepository(_contracts(sessions))
    evaluated_at = datetime(2026, 9, 16, 9, tzinfo=UTC)

    result = materialize_horizon_outcomes(
        repository,
        engine,
        evaluated_at=evaluated_at,
    )

    assert result["status"] == "READY"
    assert len(repository.saved) == 3
    returns = {}
    for outcome, evidence in repository.saved:
        _validate_outcome_evidence(outcome, evidence)
        returns[outcome.horizon_days] = outcome.realized_net_return_pct
        assert outcome.exit_trade_date == sessions[outcome.horizon_days]
        assert outcome.realized_cost_pct == 0.2
        assert outcome.execution_feasibility == "UNVERIFIED_RESEARCH"
    assert len(set(returns.values())) == 3
    engine.dispose()


def test_outcome_materialization_rejects_ambiguous_naive_clock():
    engine, sessions = _market_engine()
    repository = _OutcomeRepository(_contracts(sessions))
    with pytest.raises(ValueError, match="must include a timezone"):
        materialize_horizon_outcomes(
            repository,
            engine,
            evaluated_at=datetime(2026, 9, 16, 9),
        )
    assert repository.saved == []
    engine.dispose()


def test_corporate_action_gap_is_quarantined_not_learned():
    engine, sessions = _market_engine(corporate_action=True)
    repository = _OutcomeRepository([_contracts(sessions)[1]])
    result = materialize_horizon_outcomes(
        repository,
        engine,
        evaluated_at=datetime(2026, 9, 16, 9, tzinfo=UTC),
    )
    outcome, evidence = repository.saved[0]
    assert result["status"] == "QUARANTINED"
    assert outcome.outcome_status == "QUARANTINED"
    _validate_outcome_evidence(outcome, evidence)
    engine.dispose()


def test_unattested_qmt_bars_do_not_materialize_forward_outcome():
    engine, sessions = _market_engine(qmt_attested=False)
    repository = _OutcomeRepository([_contracts(sessions)[0]])
    result = materialize_horizon_outcomes(
        repository,
        engine,
        evaluated_at=datetime(2026, 9, 16, 9, tzinfo=UTC),
    )

    assert result["status"] == "COLLECTING"
    assert result["materialized_outcome_count"] == 0
    assert repository.saved == []
    assert result["unresolved"] == [{
        "contract_id": _contracts(sessions)[0]["contract_id"],
        "reason_code": "QMT_DAILY_BAR_ATTESTATION_REQUIRED",
    }]
    engine.dispose()


def test_outcome_repository_validator_rejects_forged_bar_derived_return():
    qmt_bars = [
        {
            "trade_date": day,
            "data_source": "gj_big_qmt_inner",
            "quality_status": "QMT_ATTESTED",
            "source_time": f"{day}T07:00:00+00:00",
            "received_at": f"{day}T08:00:00+00:00",
            "batch_id": f"qmt-{day}",
            "data_version": "bigqmt-v1",
        }
        for day in ("2026-08-17", "2026-08-18")
    ]
    evidence = {
        "contract_id": "a" * 64,
        "contract_hash": "b" * 64,
        "stock_code": "600001",
        "horizon_days": 1,
        "entry_trade_date": "2026-08-17",
        "exit_trade_date": "2026-08-18",
        "market_data_source": "sm_stock_kline.daily.unadjusted",
        "cost_model_version": "cost-v1",
        "realized_cost_pct": 0.2,
        "execution_feasibility": "UNVERIFIED_RESEARCH",
        "qmt_attestation": {
            "protocol": "probiga.trading-v3.qmt-attested-outcome-bars.v1",
            "status": "QMT_ATTESTED",
            "provider": "gj_big_qmt_inner",
            "attested_bar_count": 2,
            "attestation_hash": _digest(qmt_bars),
        },
        "knowledge_cutoff": "2026-08-19T08:00:00+00:00",
        "corporate_action_guard": {
            "method": "PRE_CLOSE_VS_PRIOR_UNADJUSTED_CLOSE",
            "tolerance_pct": 0.05,
            "detected": False,
        },
        "bars": [
            {**qmt_bars[0], "trade_date": "2026-08-17", "open": 100, "high": 102,
             "low": 99, "close": 101, "pre_close": 100, "amount": 1,
             "etl_sync_at": "2026-08-18T00:00:00+00:00"},
            {**qmt_bars[1], "trade_date": "2026-08-18", "open": 101, "high": 103,
             "low": 100, "close": 102, "pre_close": 101, "amount": 1,
             "etl_sync_at": "2026-08-19T00:00:00+00:00"},
        ],
    }
    outcome = HorizonOutcomeEvidence(
        contract_id="a" * 64,
        contract_hash="b" * 64,
        stock_code="600001",
        horizon_days=1,
        entry_trade_date=date(2026, 8, 17),
        exit_trade_date=date(2026, 8, 18),
        entry_price=100,
        exit_price=102,
        gross_return_pct=2,
        realized_cost_pct=0.2,
        realized_net_return_pct=1.8,
        realized_mae_pct=-99.0,
        realized_mfe_pct=3.0,
        bar_count=2,
        cost_model_version="cost-v1",
        market_data_source="sm_stock_kline.daily.unadjusted",
        market_evidence_hash=_digest(evidence),
        execution_feasibility="UNVERIFIED_RESEARCH",
        outcome_status="MATURED_VERIFIED",
        observed_at=datetime(2026, 8, 19, 8, tzinfo=UTC),
    )
    with pytest.raises(RuntimeError, match="NOT_DERIVED_FROM_BARS"):
        _validate_outcome_evidence(outcome, evidence)


def test_verified_learning_run_rebuilds_ledger_and_rejects_forged_ready(monkeypatch):
    import server.trading_v3.shadow_intelligence_repository as module

    policy = {
        "minimum_mature_samples": {"1": 160, "5": 120, "20": 80},
        "maximum_evidence_age_days": 30,
    }
    runtime = {
        "config_hash": "c" * 64,
        "continuous_policy": policy,
        "model_artifact_hashes": {
            "t1:v:T+1": "1" * 64,
            "t5:v:T+5": "5" * 64,
            "t20:v:T+20": "2" * 64,
        },
        "code_version": "d" * 64,
        "code_version_kind": "source_artifact_sha256",
    }
    monkeypatch.setattr(module, "_runtime_provenance", lambda: runtime)
    metrics = counterfactual_learning_metrics(
        [], minimum_mature_samples=80,
        minimum_mature_samples_by_horizon=policy["minimum_mature_samples"],
    )
    metrics["status"] = "EVIDENCE_READY"
    metrics["horizon_readiness"] = {
        key: {"required": 1, "observed": 1, "ready": True}
        for key in ("T+1", "T+5", "T+20")
    }
    metrics["provenance"] = {
        "learning_run_id": "f" * 64,
        "learning_evidence_hash": _hash([]),
        "learning_policy_hash": _hash(policy),
        "evidence_source": "HORIZON_CONTRACT_OUTCOME_LEDGER",
        "config_hash": runtime["config_hash"],
        "code_version": runtime["code_version"],
        "code_version_kind": runtime["code_version_kind"],
        "model_artifact_hashes": runtime["model_artifact_hashes"],
    }
    row = {
        "learning_run_id": "f" * 64,
        "evaluation_date": date.today(),
        "learning_status": "EVIDENCE_READY",
        "evidence_source": "HORIZON_CONTRACT_OUTCOME_LEDGER",
        "order_authority": 0,
        "can_activate_model": 0,
        "config_hash": runtime["config_hash"],
        "code_version": runtime["code_version"],
        "policy_hash": _hash(policy),
        "evidence_hash": _hash([]),
        "metrics_json": json.dumps(metrics, sort_keys=True, separators=(",", ":")),
        "learning_result_hash": _hash(metrics),
        "model_artifact_hashes_json": json.dumps(runtime["model_artifact_hashes"]),
        "provenance_hash": _hash(metrics["provenance"]),
        "evaluated_at": datetime.now(UTC),
    }
    repository = ShadowIntelligenceRepository(create_engine("sqlite+pysqlite:///:memory:"))
    monkeypatch.setattr(repository, "learning_run", lambda _learning_id: row)
    monkeypatch.setattr(
        repository, "counterfactual_learning_samples", lambda **_kwargs: []
    )
    assert repository.verified_learning_run("f" * 64) is None


def test_verified_learning_run_rejects_forged_ready_sql_projection(monkeypatch):
    import server.trading_v3.shadow_intelligence_repository as module

    policy = {
        "minimum_mature_samples": {"1": 160, "5": 120, "20": 80},
        "maximum_evidence_age_days": 30,
    }
    runtime = {
        "config_hash": "c" * 64,
        "continuous_policy": policy,
        "model_artifact_hashes": {
            "t1:v:T+1": "1" * 64,
            "t5:v:T+5": "5" * 64,
            "t20:v:T+20": "2" * 64,
        },
        "code_version": "d" * 64,
        "code_version_kind": "source_artifact_sha256",
    }
    monkeypatch.setattr(module, "_runtime_provenance", lambda: runtime)
    evaluation_date = datetime.now().date()
    evidence_hash = _hash([])
    policy_hash = _hash(policy)
    learning_id = _hash({
        "evaluation_date": evaluation_date.isoformat(),
        "evidence_hash": evidence_hash,
        "policy_hash": policy_hash,
        "config_hash": runtime["config_hash"],
        "code_version": runtime["code_version"],
        "model_artifact_hashes": runtime["model_artifact_hashes"],
    })
    metrics = counterfactual_learning_metrics(
        [],
        minimum_mature_samples=80,
        minimum_mature_samples_by_horizon=policy["minimum_mature_samples"],
    )
    metrics["provenance"] = {
        "learning_run_id": learning_id,
        "learning_evidence_hash": evidence_hash,
        "learning_policy_hash": policy_hash,
        "evidence_source": "HORIZON_CONTRACT_OUTCOME_LEDGER",
        "config_hash": runtime["config_hash"],
        "code_version": runtime["code_version"],
        "code_version_kind": runtime["code_version_kind"],
        "model_artifact_hashes": runtime["model_artifact_hashes"],
    }
    # The JSON/identity are authoritative COLLECTING evidence, while the SQL
    # projection is forged to satisfy the table's EVIDENCE_READY count checks.
    row = {
        "learning_run_id": learning_id,
        "evaluation_date": evaluation_date,
        "learning_status": "EVIDENCE_READY",
        "sample_count": 360,
        "selected_win_count": 360,
        "selected_loss_count": 0,
        "rejected_win_count": 0,
        "rejected_correct_count": 0,
        "selection_precision": 1.0,
        "winner_recall": 1.0,
        "mean_absolute_forecast_error_pct": 0.0,
        "mean_brier_score": 0.0,
        "total_opportunity_cost_pct": 0.0,
        "t1_sample_count": 160,
        "t5_sample_count": 120,
        "t20_sample_count": 80,
        "t1_evidence_ready": 1,
        "t5_evidence_ready": 1,
        "t20_evidence_ready": 1,
        "evidence_source": "HORIZON_CONTRACT_OUTCOME_LEDGER",
        "order_authority": 0,
        "can_activate_model": 0,
        "config_hash": runtime["config_hash"],
        "code_version": runtime["code_version"],
        "policy_hash": policy_hash,
        "evidence_hash": evidence_hash,
        "metrics_json": json.dumps(metrics, sort_keys=True, separators=(",", ":")),
        "learning_result_hash": _hash(metrics),
        "model_artifact_hashes_json": json.dumps(runtime["model_artifact_hashes"]),
        "provenance_hash": _hash(metrics["provenance"]),
        "evaluated_at": datetime.now(UTC),
    }
    repository = ShadowIntelligenceRepository(
        create_engine("sqlite+pysqlite:///:memory:")
    )
    monkeypatch.setattr(repository, "learning_run", lambda _learning_id: row)
    monkeypatch.setattr(
        repository, "counterfactual_learning_samples", lambda **_kwargs: []
    )

    assert repository.verified_learning_run(learning_id) is None


def test_learning_samples_reset_when_model_artifact_changes(monkeypatch):
    import server.trading_v3.shadow_intelligence_repository as module

    release_id = ShadowIntelligenceRepository.release_id(
        model_key="t1-model",
        model_version="v1",
        horizon_days=1,
    )
    monkeypatch.setattr(
        module,
        "_runtime_provenance",
        lambda: {
            "model_artifact_hashes": {release_id: "a" * 64},
        },
    )
    base = {
        "outcome_id": "o" * 64,
        "outcome_hash": "h" * 64,
        "run_uid": "run-1",
        "stock_code": "600001",
        "model_key": "t1-model",
        "model_version": "v1",
        "horizon_days": 1,
        "prediction_kind": "PROXY_SCORE",
        "source_strategy_key": "microstructure",
        "selection_status": "SELECTED",
        "reason_code": "TARGET_PORTFOLIO_SELECTED",
        "source_forecast_hash": "s" * 64,
        "selection_evidence_hash": "e" * 64,
        "realized_net_return_pct": 1.0,
        "realized_mae_pct": -1.0,
        "realized_mfe_pct": 2.0,
        "realized_cost_pct": 0.2,
        "market_evidence_hash": "m" * 64,
        "execution_feasibility": "UNVERIFIED_RESEARCH",
        "expected_return_net_pct": None,
        "probability_positive": None,
    }
    rows = [
        {
            **base,
            "forecast_id": "1" * 64,
            "contract_id": "1" * 64,
            "model_artifact_hash": "a" * 64,
        },
        {
            **base,
            "forecast_id": "2" * 64,
            "contract_id": "2" * 64,
            "model_artifact_hash": "b" * 64,
        },
    ]

    class _Result:
        def mappings(self):
            return self

        def all(self):
            return rows

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args, **_kwargs):
            return _Result()

    class _Engine:
        def connect(self):
            return _Connection()

    repository = ShadowIntelligenceRepository(_Engine())  # type: ignore[arg-type]
    samples = repository.counterfactual_learning_samples()

    assert [item["contract_id"] for item in samples] == ["1" * 64]
    assert samples[0]["release_id"] == release_id
    assert samples[0]["model_artifact_hash"] == "a" * 64


def test_real_oos_learning_sample_uses_persisted_shadow_suite(monkeypatch):
    release_id = "suite-real:t1-model:v1:T+1"
    artifact_hash = "c" * 64
    row = {
        "forecast_id": "1" * 64,
        "contract_id": "1" * 64,
        "outcome_id": "o" * 64,
        "outcome_hash": "h" * 64,
        "run_uid": "run-real",
        "stock_code": "600001",
        "model_key": "t1-model",
        "model_version": "v1",
        "horizon_days": 1,
        "model_artifact_hash": artifact_hash,
        "prediction_kind": "CALIBRATED_OOS",
        "source_strategy_key": "microstructure",
        "selection_status": "SELECTED",
        "reason_code": "TARGET_PORTFOLIO_SELECTED",
        "source_forecast_hash": "s" * 64,
        "selection_evidence_hash": "e" * 64,
        "realized_net_return_pct": 1.0,
        "realized_mae_pct": -1.0,
        "realized_mfe_pct": 2.0,
        "realized_cost_pct": 0.2,
        "market_evidence_hash": "m" * 64,
        "execution_feasibility": "UNVERIFIED_RESEARCH",
        "expected_return_net_pct": 0.5,
        "probability_positive": 0.6,
    }

    class _Result:
        def mappings(self):
            return self

        def all(self):
            return [row]

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args, **_kwargs):
            return _Result()

    class _Engine:
        def connect(self):
            return _Connection()

    repository = ShadowIntelligenceRepository(_Engine())  # type: ignore[arg-type]
    monkeypatch.setattr(
        repository,
        "effective_runtime_provenance",
        lambda **_kwargs: {
            "model_artifact_hashes": {release_id: artifact_hash},
        },
    )

    samples = repository.counterfactual_learning_samples(
        evaluation_date=date(2026, 8, 16),
    )

    assert len(samples) == 1
    assert samples[0]["release_id"] == release_id
    assert samples[0]["model_artifact_hash"] == artifact_hash
    assert samples[0]["prediction_kind"] == "CALIBRATED_OOS"


def test_repository_rejects_handmade_pass_decision_before_insert(monkeypatch):
    import server.trading_v3.shadow_intelligence_repository as module

    release_id = "model:v:T+1"
    policy = {
        "minimum_mature_samples": {"1": 160, "5": 120, "20": 80},
        "minimum_oos_samples": {"1": 100, "5": 100, "20": 80},
        "minimum_walk_forward_folds": 3,
        "minimum_direction_rank_correlation": 0.05,
        "maximum_calibration_mae": 0.15,
        "maximum_brier_score": 0.24,
        "maximum_population_stability_index": 0.2,
        "minimum_net_expectancy_after_cost_pct": 0.0,
        "minimum_profit_factor": 1.3,
        "minimum_cost_coverage_ratio": 1.0,
        "maximum_evidence_age_days": 30,
    }
    runtime = {
        "config_hash": "c" * 64,
        "continuous_policy": policy,
        "model_artifact_hashes": {release_id: "a" * 64},
        "code_version": "d" * 64,
        "code_version_kind": "source_artifact_sha256",
    }
    monkeypatch.setattr(module, "_runtime_provenance", lambda: runtime)
    repository = ShadowIntelligenceRepository(create_engine("sqlite+pysqlite:///:memory:"))
    monkeypatch.setattr(
        repository,
        "effective_runtime_provenance",
        lambda **_kwargs: {
            **runtime,
            "artifact_registry_status": "AVAILABLE",
        },
    )
    learning = {
        "learning_run_id": "l" * 64,
        "learning_result_hash": "r" * 64,
        "evidence_hash": "e" * 64,
        "policy_hash": _hash(policy),
    }
    monkeypatch.setattr(repository, "verified_learning_run", lambda _id: learning)
    observed = datetime.now(UTC) - timedelta(days=1)
    evidence = ContinuousCalibrationEvidence(
        release_id=release_id,
        model_key="model",
        model_version="v",
        horizon_days=1,
        prediction_kind="CALIBRATED_OOS",
        matured_sample_count=1,
        oos_sample_count=1,
        walk_forward_fold_count=1,
        direction_rank_correlation=-1,
        calibration_mae=0.9,
        brier_score=0.9,
        population_stability_index=0.9,
        net_expectancy_after_cost_pct=-1,
        profit_factor=0,
        cost_coverage_ratio=0,
        observed_at=observed,
        valid_until=observed + timedelta(days=2),
    )
    raw = {
        key: getattr(evidence, key)
        for key in (
            "release_id", "model_key", "model_version", "horizon_days",
            "matured_sample_count", "oos_sample_count",
            "walk_forward_fold_count", "direction_rank_correlation",
            "calibration_mae", "brier_score",
            "population_stability_index", "net_expectancy_after_cost_pct",
            "profit_factor", "cost_coverage_ratio", "observed_at", "valid_until",
        )
    }
    raw.update({
        "learning_run_id": learning["learning_run_id"],
        "learning_result_hash": learning["learning_result_hash"],
        "learning_evidence_hash": learning["evidence_hash"],
        "learning_policy_hash": learning["policy_hash"],
    })
    forged = CalibrationGateDecision(
        status="PASS",
        failure_codes=(),
        release_id=release_id,
        horizon_days=1,
        evidence_observed_at=observed.isoformat(),
        evidence_valid_until=(observed + timedelta(days=2)).isoformat(),
        evaluated_at=datetime.now(UTC).isoformat(),
        recommended_stage="PAPER_ELIGIBLE",
        evidence_provenance_status="PERSISTED_VERIFIED",
    )
    with pytest.raises(RuntimeError, match="NOT_RECOMPUTED"):
        repository.save_calibration_gate(
            release_id=release_id,
            model_key="model",
            model_version="v",
            horizon_days=1,
            prediction_kind="CALIBRATED_OOS",
            decision=forged,
            evidence=evidence,
            raw_evidence=raw,
            policy=policy,
            evaluated_at=datetime.now(UTC),
        )


def test_mysql84_check_clause_normalization_is_literal_preserving_and_narrow():
    real_mysql84 = (
        r"((`decision_scope` = _utf8mb4\'RESEARCH_ONLY\') "
        r"and (`order_authority` = 0))"
    )
    normalized = _normalize_mysql84_check_clause(real_mysql84)
    assert normalized == (
        "((decision_scope = 'RESEARCH_ONLY') and (order_authority = 0))"
    )
    assert "'RESEARCH_ONLY'" in normalized

    drifted = _normalize_mysql84_check_clause(
        real_mysql84.replace("RESEARCH_ONLY", "RESEARCH_LIVE")
    )
    assert "'RESEARCH_ONLY'" not in drifted
    assert "'RESEARCH_LIVE'" in drifted

    wrong_introducer = _normalize_mysql84_check_clause(
        real_mysql84.replace("_utf8mb4", "_latin1")
    )
    assert "_latin1\\'RESEARCH_ONLY\\'" in wrong_introducer
    assert "'RESEARCH_ONLY'" not in wrong_introducer

    regex_clause = _normalize_mysql84_check_clause(
        r"regexp_like(`artifact_id`,_utf8mb4\'^[0-9a-f]{64}$\')"
    )
    assert regex_clause == "regexp_like(artifact_id,'^[0-9a-f]{64}$')"


def test_shadow_schema_has_requested_clock_outcome_and_insert_guards():
    ddl = "\n".join(SHADOW_INTELLIGENCE_DDL)
    assert "ADD COLUMN requested_as_of DATE" in ddl
    assert "CREATE TABLE IF NOT EXISTS st_horizon_outcome_v3" in ddl
    assert "fk_v3_calibration_gate_learning_run" in ddl
    assert "trg_v3_horizon_outcome_guard_bi" in ddl
    assert "chk_v3_horizon_outcome_research_execution" in ddl
    assert "NEW.prediction_kind <> 'PROXY_SCORE'" in ddl
    assert "NEW.execution_feasibility <> 'UNVERIFIED_RESEARCH'" in ddl
    assert "trg_v3_learning_run_guard_bi" in ddl
    assert "trg_v3_calibration_gate_guard_bi" in ddl
    assert "external signed attestation pipeline" in ddl
    assert "order_authority = 0" in ddl


class _DisposableEngine:
    def __init__(self):
        self.disposed = False

    def dispose(self):
        self.disposed = True


def test_counterfactual_runner_executes_legacy_then_shadow(monkeypatch, capsys):
    import tools.run_trading_v3_counterfactual as runner

    primary = _DisposableEngine()
    kline = _DisposableEngine()
    calls = []
    monkeypatch.setattr(runner, "load_project_env", lambda: None)
    monkeypatch.setattr(runner, "create_tool_engine", lambda: primary)
    monkeypatch.setattr(runner, "get_kline_engine", lambda: kline)
    monkeypatch.setattr(
        runner,
        "drain_counterfactual_backlog",
        lambda *args, **kwargs: calls.append("legacy") or {"status": "ok"},
    )
    monkeypatch.setattr(
        runner,
        "run_shadow_intelligence_cycle",
        lambda *args, **kwargs: calls.append("shadow") or {"status": "ok"},
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_trading_v3_counterfactual.py",
            "--evaluated-at",
            "2026-08-16T16:00:00+08:00",
        ],
    )

    assert runner.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert calls == ["legacy", "shadow"]
    assert payload["order_authority"] is False
    assert payload["real_order_allowed"] is False
    assert primary.disposed is True
    assert kline.disposed is True


def test_counterfactual_runner_shadow_failure_is_nonzero(monkeypatch):
    import tools.run_trading_v3_counterfactual as runner

    primary = _DisposableEngine()
    kline = _DisposableEngine()
    monkeypatch.setattr(runner, "load_project_env", lambda: None)
    monkeypatch.setattr(runner, "create_tool_engine", lambda: primary)
    monkeypatch.setattr(runner, "get_kline_engine", lambda: kline)
    monkeypatch.setattr(
        runner,
        "drain_counterfactual_backlog",
        lambda *args, **kwargs: {"status": "ok"},
    )

    def _fail(*args, **kwargs):
        raise RuntimeError("SHADOW_CYCLE_FAILED")

    monkeypatch.setattr(runner, "run_shadow_intelligence_cycle", _fail)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_trading_v3_counterfactual.py",
            "--evaluated-at",
            "2026-08-16T16:00:00+08:00",
        ],
    )

    with pytest.raises(RuntimeError, match="SHADOW_CYCLE_FAILED"):
        runner.main()
    assert primary.disposed is True
    assert kline.disposed is True


def test_counterfactual_runner_empty_forward_cycle_is_nonzero(
    monkeypatch,
    capsys,
):
    import tools.run_trading_v3_counterfactual as runner

    primary = _DisposableEngine()
    kline = _DisposableEngine()
    monkeypatch.setattr(runner, "load_project_env", lambda: None)
    monkeypatch.setattr(runner, "create_tool_engine", lambda: primary)
    monkeypatch.setattr(runner, "get_kline_engine", lambda: kline)
    monkeypatch.setattr(
        runner,
        "drain_counterfactual_backlog",
        lambda *args, **kwargs: {"status": "ok"},
    )
    monkeypatch.setattr(
        runner,
        "run_shadow_intelligence_cycle",
        lambda *args, **kwargs: {"status": "EMPTY"},
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_trading_v3_counterfactual.py",
            "--evaluated-at",
            "2026-08-16T16:00:00+08:00",
        ],
    )

    assert runner.main() == 3
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "EMPTY"
    assert payload["forward_evidence_progress"] == "EMPTY"
    assert primary.disposed is True
    assert kline.disposed is True
