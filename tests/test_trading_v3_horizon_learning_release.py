from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone

import pytest

from server.trading_v3.config import load_v3_config
from server.trading_v3.horizon_contracts import (
    CalibrationEvidence,
    HorizonContractError,
    HorizonForecastContract,
    PredictionKind,
    validate_independent_horizon_suite,
)
from server.trading_v3.learning_intelligence import (
    build_counterfactual_samples,
    counterfactual_learning_metrics,
)
from server.trading_v3.release_governance import (
    ContinuousCalibrationEvidence,
    ReleaseEvent,
    ReleaseStage,
    enforce_continuous_gate,
    evaluate_continuous_calibration,
    transition_shadow_release,
)


UTC = timezone.utc
HASH_A = "a" * 64
HASH_B = "b" * 64


def _digest(value) -> str:
    return hashlib.sha256(json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")).hexdigest()


def _contract_metadata(horizon: int) -> dict:
    source_evidence = {
        "forecast_id": f"source-{horizon}",
        "strategy_key": f"sleeve-{horizon}",
        "features": {f"feature-{horizon}": 0.82},
    }
    selection_evidence = {
        "source_forecast_id": f"source-{horizon}",
        "selection_status": "REJECTED",
        "reason_code": "RESEARCH_ONLY",
    }
    return {
        "source_strategy_key": f"sleeve-{horizon}",
        "source_forecast_hash": _digest(source_evidence),
        "source_evidence": source_evidence,
        "decision_result_hash": _digest({"decision": "frozen"}),
        "feature_protocol_hash": _digest({"protocol": horizon}),
        "model_artifact_hash": _digest({"artifact": horizon}),
        "model_inputs": {f"feature-{horizon}": 0.82},
        "selection_status": "REJECTED",
        "selection_reason_code": "RESEARCH_ONLY",
        "selection_evidence_hash": _digest(selection_evidence),
        "selection_evidence": selection_evidence,
        "cost_model_version": "cn-a-share-cost-v1",
    }


def test_v370_intelligence_defaults_remain_shadow_and_order_blocked():
    config = load_v3_config()

    assert config["strategy_version"] == "trading_v3.11.0-paper"
    intelligence = config["decision_intelligence"]
    horizons = config["multi_horizon_forecasts"]
    release = config["shadow_release"]
    calibration = config["continuous_calibration"]
    assert intelligence["lifecycle"] == "SHADOW_RESEARCH_ONLY"
    assert intelligence["order_allowed"] is False
    assert horizons["order_allowed"] is False
    assert horizons["can_activate_model"] is False
    assert {
        item["model_key"] for item in horizons["models"].values()
    } == {
        "trading_v3_t1_independent_proxy_v1",
        "trading_v3_t5_independent_proxy_v1",
        "trading_v3_t20_independent_proxy_v1",
    }
    assert all(
        item["prediction_kind"] == "PROXY_SCORE"
        and item["order_allowed"] is False
        for item in horizons["models"].values()
    )
    assert release["automatic_promotion_allowed"] is False
    assert release["order_allowed"] is False
    assert release["real_order_allowed"] is False
    assert calibration["automatic_promotion_allowed"] is False
    assert calibration["failed_gate_stage"] == "BLOCKED"
    assert calibration["order_allowed"] is False


def _calibration(horizon: int, model_key: str) -> CalibrationEvidence:
    return CalibrationEvidence(
        evidence_id=f"evidence-{horizon}",
        model_key=model_key,
        model_version="1.0",
        horizon_days=horizon,
        dataset_hash=HASH_A,
        feature_protocol_hash=_digest({"protocol": horizon}),
        cost_model_version="cn-a-share-cost-v1",
        cost_assumption_pct=0.25,
        matured_sample_count=200,
        oos_sample_count=120,
        walk_forward_fold_count=4,
        outcomes_include_costs=True,
        score_direction_valid=True,
        calibration_mae=0.10,
        brier_score=0.20,
        generated_at=datetime(2026, 8, 1, tzinfo=UTC),
        valid_until=datetime(2026, 9, 1, tzinfo=UTC),
    )


def _forecast(
    horizon: int,
    *,
    selected_kind: PredictionKind = PredictionKind.CALIBRATED_OOS,
    forecast_id: str | None = None,
) -> HorizonForecastContract:
    model_key = f"independent-t{horizon}"
    is_calibrated = selected_kind is PredictionKind.CALIBRATED_OOS
    maturity = {1: date(2026, 8, 18), 5: date(2026, 8, 24), 20: date(2026, 9, 14)}[
        horizon
    ]
    return HorizonForecastContract(
        forecast_id=forecast_id or f"forecast-{horizon}",
        run_uid="run-frozen",
        stock_code="600001",
        model_key=model_key,
        model_version="1.0",
        **_contract_metadata(horizon),
        horizon_days=horizon,
        prediction_kind=selected_kind,
        decision_as_of=datetime(2026, 8, 14, 7, tzinfo=UTC),
        feature_as_of=datetime(2026, 8, 14, 6, 59, tzinfo=UTC),
        decision_session_date=date(2026, 8, 14),
        entry_trade_date=date(2026, 8, 17),
        earliest_exit_trade_date=date(2026, 8, 18),
        outcome_matures_on=maturity,
        entry_session_sequence=1,
        earliest_exit_session_sequence=2,
        outcome_maturity_session_sequence=1 + horizon,
        score=0.82,
        expected_return_net_pct=1.0 if is_calibrated else None,
        probability_positive=0.62 if is_calibrated else None,
        cost_assumption_pct=0.25,
        calibration_evidence=(
            _calibration(horizon, model_key) if is_calibrated else None
        ),
    )


def test_proxy_contract_cannot_claim_calibrated_probability_or_return():
    with pytest.raises(HorizonContractError, match="must not claim"):
        HorizonForecastContract(
            forecast_id="proxy",
            run_uid="run",
            stock_code="600001",
            model_key="t1-proxy",
            model_version="1",
            **_contract_metadata(1),
            horizon_days=1,
            prediction_kind=PredictionKind.PROXY_SCORE,
            decision_as_of=datetime(2026, 8, 14, tzinfo=UTC),
            feature_as_of=datetime(2026, 8, 14, tzinfo=UTC),
            decision_session_date=date(2026, 8, 14),
            entry_trade_date=date(2026, 8, 17),
            earliest_exit_trade_date=date(2026, 8, 18),
            outcome_matures_on=date(2026, 8, 18),
            entry_session_sequence=1,
            earliest_exit_session_sequence=2,
            outcome_maturity_session_sequence=2,
            score=0.8,
            expected_return_net_pct=1.2,
            probability_positive=None,
            cost_assumption_pct=0.25,
        )


def test_horizon_contract_forbids_same_close_entry_and_t0_exit():
    with pytest.raises(HorizonContractError, match="same-close"):
        HorizonForecastContract(
            forecast_id="bad-clock",
            run_uid="run",
            stock_code="600001",
            model_key="t1",
            model_version="1",
            **_contract_metadata(1),
            horizon_days=1,
            prediction_kind=PredictionKind.PROXY_SCORE,
            decision_as_of=datetime(2026, 8, 14, tzinfo=UTC),
            feature_as_of=datetime(2026, 8, 14, tzinfo=UTC),
            decision_session_date=date(2026, 8, 14),
            entry_trade_date=date(2026, 8, 14),
            earliest_exit_trade_date=date(2026, 8, 15),
            outcome_matures_on=date(2026, 8, 15),
            entry_session_sequence=1,
            earliest_exit_session_sequence=2,
            outcome_maturity_session_sequence=2,
            score=0.8,
            expected_return_net_pct=None,
            probability_positive=None,
            cost_assumption_pct=0.25,
        )

    with pytest.raises(HorizonContractError, match=r"for T\+1"):
        HorizonForecastContract(
            forecast_id="bad-t0",
            run_uid="run",
            stock_code="600001",
            model_key="t1",
            model_version="1",
            **_contract_metadata(1),
            horizon_days=1,
            prediction_kind=PredictionKind.PROXY_SCORE,
            decision_as_of=datetime(2026, 8, 14, tzinfo=UTC),
            feature_as_of=datetime(2026, 8, 14, tzinfo=UTC),
            decision_session_date=date(2026, 8, 14),
            entry_trade_date=date(2026, 8, 17),
            earliest_exit_trade_date=date(2026, 8, 17),
            outcome_matures_on=date(2026, 8, 18),
            entry_session_sequence=1,
            earliest_exit_session_sequence=2,
            outcome_maturity_session_sequence=2,
            score=0.8,
            expected_return_net_pct=None,
            probability_positive=None,
            cost_assumption_pct=0.25,
        )

    with pytest.raises(HorizonContractError, match="too early"):
        HorizonForecastContract(
            forecast_id="immature-t20",
            run_uid="run",
            stock_code="600001",
            model_key="t20",
            model_version="1",
            **_contract_metadata(20),
            horizon_days=20,
            prediction_kind=PredictionKind.PROXY_SCORE,
            decision_as_of=datetime(2026, 8, 14, tzinfo=UTC),
            feature_as_of=datetime(2026, 8, 14, tzinfo=UTC),
            decision_session_date=date(2026, 8, 14),
            entry_trade_date=date(2026, 8, 17),
            earliest_exit_trade_date=date(2026, 8, 18),
            outcome_matures_on=date(2026, 8, 25),
            entry_session_sequence=1,
            earliest_exit_session_sequence=2,
            outcome_maturity_session_sequence=21,
            score=0.8,
            expected_return_net_pct=None,
            probability_positive=None,
            cost_assumption_pct=0.25,
        )


def test_t1_t5_t20_suite_requires_independent_models_and_evidence():
    result = validate_independent_horizon_suite(
        [_forecast(1), _forecast(5), _forecast(20)]
    )

    assert list(result["horizons"]) == ["T+1", "T+5", "T+20"]
    assert result["independent_model_keys"] is True
    assert result["order_authority"] is False

    reused_model = _forecast(5)
    object.__setattr__(reused_model, "model_key", "independent-t1")
    with pytest.raises(HorizonContractError, match="independent model_key"):
        validate_independent_horizon_suite(
            [_forecast(1), reused_model, _forecast(20)]
        )

    mixed_cutoff = _forecast(5)
    object.__setattr__(
        mixed_cutoff,
        "feature_as_of",
        datetime(2026, 8, 14, 6, 58, tzinfo=UTC),
    )
    with pytest.raises(HorizonContractError, match="feature cutoff"):
        validate_independent_horizon_suite(
            [_forecast(1), mixed_cutoff, _forecast(20)]
        )


def test_horizon_contract_uses_shanghai_date_and_exchange_session_sequence():
    with pytest.raises(HorizonContractError, match="Asia/Shanghai"):
        HorizonForecastContract(
            forecast_id="wrong-exchange-date",
            run_uid="run",
            stock_code="600001",
            model_key="t1",
            model_version="1",
            **_contract_metadata(1),
            horizon_days=1,
            prediction_kind=PredictionKind.PROXY_SCORE,
            decision_as_of=datetime(2026, 8, 14, 16, 30, tzinfo=UTC),
            feature_as_of=datetime(2026, 8, 14, 15, 0, tzinfo=UTC),
            decision_session_date=date(2026, 8, 14),
            entry_trade_date=date(2026, 8, 17),
            earliest_exit_trade_date=date(2026, 8, 18),
            outcome_matures_on=date(2026, 8, 18),
            entry_session_sequence=1,
            earliest_exit_session_sequence=2,
            outcome_maturity_session_sequence=2,
            score=0.8,
            expected_return_net_pct=None,
            probability_positive=None,
            cost_assumption_pct=0.25,
        )

    with pytest.raises(HorizonContractError, match=r"must enforce T\+1"):
        HorizonForecastContract(
            forecast_id="wrong-session-sequence",
            run_uid="run",
            stock_code="600001",
            model_key="t5",
            model_version="1",
            **_contract_metadata(1),
            horizon_days=5,
            prediction_kind=PredictionKind.PROXY_SCORE,
            decision_as_of=datetime(2026, 8, 14, 7, tzinfo=UTC),
            feature_as_of=datetime(2026, 8, 14, 6, tzinfo=UTC),
            decision_session_date=date(2026, 8, 14),
            entry_trade_date=date(2026, 8, 17),
            earliest_exit_trade_date=date(2026, 8, 18),
            outcome_matures_on=date(2026, 8, 24),
            entry_session_sequence=1,
            earliest_exit_session_sequence=3,
            outcome_maturity_session_sequence=6,
            score=0.8,
            expected_return_net_pct=None,
            probability_positive=None,
            cost_assumption_pct=0.25,
        )


def test_counterfactual_samples_cover_four_quadrants_and_never_activate():
    forecasts = [
        _forecast(1, forecast_id="selected-win"),
        _forecast(1, forecast_id="selected-loss"),
        _forecast(1, forecast_id="rejected-win"),
        _forecast(1, forecast_id="rejected-correct"),
    ]
    selections = {
        "selected-win": {"selected": True, "reason_code": "PASS"},
        "selected-loss": {"selected": True, "reason_code": "PASS"},
        "rejected-win": {"selected": False, "reason_code": "THEME_CAP"},
        "rejected-correct": {"selected": False, "reason_code": "EDGE_LOW"},
    }

    def outcome(gross: float) -> dict:
        return {
            "entry_trade_date": "2026-08-17",
            "exit_trade_date": "2026-08-18",
            "gross_return_pct": gross,
            "realized_cost_pct": 0.25,
        }

    outcomes = {
        "selected-win": outcome(2.25),
        "selected-loss": outcome(-0.75),
        "rejected-win": outcome(3.25),
        "rejected-correct": outcome(0.0),
    }
    built = build_counterfactual_samples(
        forecasts,
        selections,
        outcomes,
        evaluation_date="2026-08-18",
    )

    assert {item["quadrant"] for item in built["samples"]} == {
        "SELECTED_WIN",
        "SELECTED_LOSS",
        "REJECTED_WIN",
        "REJECTED_CORRECT",
    }
    assert all(item["can_activate_model"] is False for item in built["samples"])
    metrics = counterfactual_learning_metrics(
        built["samples"], minimum_mature_samples=4
    )
    assert metrics["status"] == "EVIDENCE_READY"
    assert metrics["overall"]["selection_precision"] == 0.5
    assert metrics["overall"]["winner_recall"] == 0.5
    assert metrics["overall"]["rejection_reason_regret_pct"] == {
        "THEME_CAP": 3.0
    }
    assert metrics["can_activate_model"] is False


def test_counterfactual_readiness_requires_each_independent_horizon():
    rows = []
    for horizon in (1, 5):
        row = {
            **build_counterfactual_samples(
                [_forecast(horizon)],
                {f"forecast-{horizon}": {"selected": True}},
                {
                    f"forecast-{horizon}": {
                        "entry_trade_date": "2026-08-17",
                        "exit_trade_date": (
                            "2026-08-18" if horizon == 1 else "2026-08-24"
                        ),
                        "gross_return_pct": 1.0,
                        "realized_cost_pct": 0.2,
                    }
                },
                evaluation_date=(
                    "2026-08-18" if horizon == 1 else "2026-08-24"
                ),
            )
        }
        rows.extend(row["samples"])

    metrics = counterfactual_learning_metrics(
        rows,
        minimum_mature_samples=3,
        minimum_mature_samples_by_horizon={"1": 1, "5": 1, "20": 1},
    )

    assert metrics["status"] == "COLLECTING"
    assert metrics["horizon_readiness"]["T+1"]["ready"] is True
    assert metrics["horizon_readiness"]["T+5"]["ready"] is True
    assert metrics["horizon_readiness"]["T+20"]["ready"] is False


def test_counterfactual_keeps_immature_pending_and_quarantines_t0_outcome():
    t20 = _forecast(20)
    pending = build_counterfactual_samples(
        [t20],
        {t20.forecast_id: {"selected": False}},
        {},
        evaluation_date="2026-08-30",
    )
    assert pending["pending"][0]["reason_code"] == "OUTCOME_NOT_MATURE"

    t1 = _forecast(1)
    invalid = build_counterfactual_samples(
        [t1],
        {t1.forecast_id: {"selected": True}},
        {
            t1.forecast_id: {
                "entry_trade_date": "2026-08-17",
                "exit_trade_date": "2026-08-17",
                "gross_return_pct": 1.0,
                "realized_cost_pct": 0.2,
            }
        },
        evaluation_date="2026-08-18",
    )
    assert invalid["quarantined"][0]["reason_code"] == (
        "OUTCOME_CONTRACT_VIOLATION"
    )


def test_counterfactual_rejects_string_boolean_and_future_or_reversed_outcomes():
    forecast = _forecast(1)
    outcome = {
        "entry_trade_date": "2026-08-17",
        "exit_trade_date": "2026-08-18",
        "gross_return_pct": 1.0,
        "realized_cost_pct": 0.2,
    }
    bad_selection = build_counterfactual_samples(
        [forecast],
        {forecast.forecast_id: {"selected": "false"}},
        {forecast.forecast_id: outcome},
        evaluation_date="2026-08-18",
    )
    assert bad_selection["quarantined"][0]["reason_code"] == (
        "SELECTION_CONTRACT_VIOLATION"
    )

    future_exit = build_counterfactual_samples(
        [forecast],
        {forecast.forecast_id: {"selected": False}},
        {
            forecast.forecast_id: {
                **outcome,
                "exit_trade_date": "2026-08-19",
            }
        },
        evaluation_date="2026-08-18",
    )
    assert future_exit["quarantined"][0]["reason_code"] == (
        "OUTCOME_CONTRACT_VIOLATION"
    )

    reversed_exit = build_counterfactual_samples(
        [forecast],
        {forecast.forecast_id: {"selected": False}},
        {
            forecast.forecast_id: {
                **outcome,
                "entry_trade_date": "2026-08-19",
                "exit_trade_date": "2026-08-18",
            }
        },
        evaluation_date="2026-08-19",
    )
    assert reversed_exit["quarantined"][0]["reason_code"] == (
        "OUTCOME_CONTRACT_VIOLATION"
    )


def _gate_policy() -> dict:
    return {
        "minimum_mature_samples": {"1": 160, "5": 120, "20": 80},
        "minimum_oos_samples": {"1": 100, "5": 100, "20": 80},
        "minimum_walk_forward_folds": 3,
        "minimum_direction_rank_correlation": 0.05,
        "maximum_calibration_mae": 0.15,
        "maximum_brier_score": 0.24,
        "maximum_population_stability_index": 0.20,
        "minimum_net_expectancy_after_cost_pct": 0.0,
        "minimum_profit_factor": 1.30,
        "minimum_cost_coverage_ratio": 1.0,
        "maximum_evidence_age_days": 30,
    }


def _gate_evidence(
    *,
    prediction_kind: PredictionKind = PredictionKind.CALIBRATED_OOS,
    psi: float = 0.10,
) -> ContinuousCalibrationEvidence:
    return ContinuousCalibrationEvidence(
        release_id="release-t1",
        model_key="independent-t1",
        model_version="1.0",
        horizon_days=1,
        prediction_kind=prediction_kind,
        matured_sample_count=200,
        oos_sample_count=120,
        walk_forward_fold_count=4,
        direction_rank_correlation=0.20,
        calibration_mae=0.10,
        brier_score=0.18,
        population_stability_index=psi,
        net_expectancy_after_cost_pct=0.30,
        profit_factor=1.50,
        cost_coverage_ratio=1.20,
        observed_at=datetime(2026, 8, 10, tzinfo=UTC),
        valid_until=datetime(2026, 9, 10, tzinfo=UTC),
    )


def test_continuous_calibration_gate_passes_only_calibrated_fresh_evidence():
    passed = evaluate_continuous_calibration(
        _gate_evidence(),
        policy=_gate_policy(),
        evaluated_at=datetime(2026, 8, 16, tzinfo=UTC),
    )
    assert passed.passed is True
    assert passed.recommended_stage == "PAPER_ELIGIBLE"
    assert passed.order_authority is False

    blocked = evaluate_continuous_calibration(
        _gate_evidence(
            prediction_kind=PredictionKind.PROXY_SCORE,
            psi=0.30,
        ),
        policy=_gate_policy(),
        evaluated_at=datetime(2026, 8, 16, tzinfo=UTC),
    )
    assert blocked.status == "BLOCK"
    assert "PREDICTION_IS_PROXY_NOT_CALIBRATED" in blocked.failure_codes
    assert "FEATURE_DRIFT_TOO_HIGH" in blocked.failure_codes
    assert blocked.recommended_stage == "BLOCKED"


def test_shadow_release_requires_review_and_never_grants_order_authority():
    direct = transition_shadow_release(
        ReleaseStage.SHADOW,
        ReleaseEvent.APPROVE_PAPER_ELIGIBILITY,
    )
    assert direct.accepted is False
    assert direct.next_stage == "SHADOW"

    authority = transition_shadow_release(
        ReleaseStage.PAPER_ELIGIBLE,
        ReleaseEvent.REQUEST_ORDER_AUTHORITY,
    )
    assert authority.accepted is False
    assert authority.reason_code == "ORDER_AUTHORITY_OUTSIDE_RELEASE_STATE_MACHINE"
    assert authority.order_authority is False

    gate = evaluate_continuous_calibration(
        _gate_evidence(),
        policy=_gate_policy(),
        evaluated_at=datetime(2026, 8, 16, tzinfo=UTC),
    )
    reviewed = transition_shadow_release(
        ReleaseStage.CALIBRATION_REVIEW,
        ReleaseEvent.APPROVE_PAPER_ELIGIBILITY,
        calibration_gate=gate,
    )
    assert reviewed.next_stage == "PAPER_ELIGIBLE"
    assert reviewed.order_authority is False


def test_failed_continuous_gate_demotes_paper_eligible_to_blocked():
    failed = evaluate_continuous_calibration(
        _gate_evidence(psi=0.50),
        policy=_gate_policy(),
        evaluated_at=datetime(2026, 8, 16, tzinfo=UTC),
    )
    demoted = enforce_continuous_gate(ReleaseStage.PAPER_ELIGIBLE, failed)
    assert demoted.next_stage == "BLOCKED"
    assert demoted.reason_code == "FAIL_CLOSED_DEMOTION"
    assert demoted.order_authority is False
