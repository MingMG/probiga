from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from server.api.routers import trading_v3
from server.trading_v3 import daily_features
from server.trading_v3.config import config_hash, load_v3_config
from server.trading_v3.shadow_intelligence_worker import (
    SOURCE_ATTRIBUTION_POLICY,
    materialize_proxy_horizon_contracts,
)


UTC = timezone.utc
ROOT = Path(__file__).resolve().parents[1]


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _calendar_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE si_trade_calendar "
            "(trade_date DATE, trade_status INTEGER)"
        ))
        cursor = date(2026, 8, 15)
        inserted = 0
        while inserted < 21:
            if cursor.weekday() < 5:
                connection.execute(
                    text("INSERT INTO si_trade_calendar VALUES (:day, 1)"),
                    {"day": cursor},
                )
                inserted += 1
            cursor += timedelta(days=1)
    return engine


def _history_features(
    session_count: int = 70,
    *,
    consecutive: bool = True,
) -> dict:
    calendar = [
        item.date()
        for item in pd.bdate_range("2026-05-11", periods=70)
    ]
    observed = list(calendar[-session_count:])
    if not consecutive:
        observed[-2] = calendar[0]
    return daily_features._history_session_evidence(
        observed,
        calendar,
    )


def _all_proxy_features(
    *,
    history_sessions: int = 70,
    history_consecutive: bool = True,
) -> dict[str, object]:
    return {
        **_history_features(
            history_sessions,
            consecutive=history_consecutive,
        ),
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
        "return_1d_pct": 1.25,
    }


def _source_row(
    strategy_key: str,
    index: int,
    *,
    source_hash: str,
    features: dict | None = None,
) -> dict:
    return {
        "forecast_id": f"source-{index}",
        "run_uid": "runtime-run-20260814",
        "stock_code": f"60000{index}",
        "strategy_key": strategy_key,
        "raw_score": 0.8,
        "source_model_version": "source-v1",
        "dataset_hash": _digest(f"dataset-{index}"),
        "forecast_status": "ACTIVE",
        "feature_time": "2026-08-14T14:59:00+08:00",
        "valid_until": "2026-08-14T16:00:00+08:00",
        "features": features or _all_proxy_features(),
        "reasons_json": "[]",
        "decision_result_hash": _digest("decision"),
        "data_snapshot_hash": _digest("snapshot"),
        "config_hash": source_hash,
        "requested_as_of": date(2026, 8, 14),
        "decision_at": "2026-08-14T15:00:00+08:00",
        "selection_status": "REJECTED",
        "selection_reason_code": "NOT_SELECTED_IN_FROZEN_TARGET",
        "selection_snapshot": {
            "target_id": None,
            "target_status": None,
            "target_strategy_keys": [],
            "attribution_snapshot_hash": None,
            "selection_status": "REJECTED",
            "selection_reason_code": "NOT_SELECTED_IN_FROZEN_TARGET",
        },
    }


def _real_artifact(horizon: int) -> dict:
    suite = load_v3_config()["multi_horizon_forecasts"]
    spec = suite["trainable_models"][f"T+{horizon}"]
    release_id = (
        f"runtime-suite:{spec['model_key']}:{spec['model_version']}:"
        f"T+{horizon}"
    )
    return {
        "release_id": release_id,
        "suite_release_id": "runtime-suite",
        "artifact_hash": _digest(f"real-t{horizon}-artifact"),
        "artifact_status": "OOS_VERIFIED",
        "model_key": spec["model_key"],
        "model_version": spec["model_version"],
        "horizon_days": horizon,
        "config_hash": config_hash(),
        "feature_protocol_hash": _digest(f"real-t{horizon}-features"),
        "oos_evidence_hash": _digest(f"real-t{horizon}-oos"),
        "dataset_hash": _digest(f"real-t{horizon}-dataset"),
        "created_at": "2026-08-01T00:00:00+00:00",
        "valid_until": "2026-08-31",
        "contract_eligible": True,
        "order_authority": False,
        "gate": {"status": "PASS", "order_authority": False},
        "feature_protocol": {
            "history_sessions_required": {
                1: 20,
                5: 20,
                20: 70,
            }[horizon],
        },
        "final_model": {
            "features": ["return_1d_pct", "overnight_gap_pct"],
            "medians": [0.0, 0.5],
        },
        "oos_evidence": {
            "matured_sample_count": 200,
            "oos_sample_count": 120,
            "walk_forward_fold_count": 3,
            "outcomes_include_costs": True,
            "calibration_mae": 0.1,
            "brier_score": 0.2,
        },
        "execution_feasibility": {
            "cost_model_version": "ROUNDTRIP_COST_ASSUMPTION_V1",
            "cost_assumption_pct": 0.2,
        },
    }


class _RuntimeRepository:
    def __init__(self, source_hash: str, *, suite_available: bool = True):
        self.rows = [
            _source_row("intraday_surprise", 1, source_hash=source_hash),
            _source_row("theme_diffusion", 2, source_hash=source_hash),
            _source_row("quality_momentum", 3, source_hash=source_hash),
        ]
        self.saved = []
        self.lookups = []
        self.suite_available = suite_available

    def latest_forecast_rows(self):
        return self.rows

    def latest_verified_horizon_suite(self, **kwargs):
        self.lookups.append(kwargs)
        if not self.suite_available:
            return None
        artifacts = {
            horizon: _real_artifact(horizon) for horizon in (1, 5, 20)
        }
        return {
            "suite_release_id": "runtime-suite",
            "artifacts_by_horizon": {
                horizon: {
                    "artifact_status": "OOS_VERIFIED",
                    "artifact": artifact,
                }
                for horizon, artifact in artifacts.items()
            },
            "release_states_by_horizon": {
                horizon: {
                    "release_id": artifact["release_id"],
                    "current_stage": "SHADOW",
                    "order_authority": False,
                }
                for horizon, artifact in artifacts.items()
            },
            "order_authority": False,
        }

    def save_horizon_contracts(self, rows, *, created_at):
        self.saved = list(rows)
        return {
            "status": "ok",
            "inserted_count": len(self.saved),
            "existing_count": 0,
            "contract_ids": [item.forecast_id for item, _ in self.saved],
            "order_authority": False,
        }


class _ResearchRuntimeRepository(_RuntimeRepository):
    def __init__(self, source_hash: str):
        super().__init__(source_hash, suite_available=False)
        for row in self.rows:
            row["features"] = {
                **dict(row["features"]),
                "overnight_gap_pct": 0.25,
            }

    def latest_forward_shadow_research_suite(self, **kwargs):
        self.lookups.append(kwargs)
        artifacts = {}
        for horizon in (1, 5, 20):
            artifact = _real_artifact(horizon)
            artifact["gate"] = {
                "status": "BLOCK",
                "block_reasons": ["SELECTED_NET_EXPECTANCY_BELOW_MINIMUM"],
                "order_authority": False,
            }
            artifact["contract_eligible"] = False
            artifacts[horizon] = artifact
        return {
            "binding_protocol": (
                "probiga.trading-v3.forward-shadow-artifact-binding.v1"
            ),
            "suite_release_id": "runtime-suite",
            "artifacts_by_horizon": {
                horizon: {
                    "artifact_status": "BLOCKED",
                    "training_receipt_status": "PROCESS_VERIFIED",
                    "artifact": artifact,
                    "order_authority": False,
                }
                for horizon, artifact in artifacts.items()
            },
            "promotion_eligible": False,
            "order_authority": False,
        }


def test_worker_selects_only_a_complete_released_real_oos_suite(
    monkeypatch,
):
    import server.trading_v3.shadow_intelligence_worker as worker

    artifact = _real_artifact(1)
    monkeypatch.setattr(worker, "verify_horizon_artifact", lambda value: dict(value))
    monkeypatch.setattr(
        worker,
        "predict_horizon_artifact",
        lambda value, features: SimpleNamespace(
            score=0.73,
            expected_return_net_pct=1.4,
            probability_positive=0.73,
            model_artifact_hash=value["artifact_hash"],
            feature_protocol_hash=value["feature_protocol_hash"],
        ),
    )
    repository = _RuntimeRepository(config_hash())
    calendar = _calendar_engine()
    try:
        result = materialize_proxy_horizon_contracts(
            repository,
            calendar,
            config=load_v3_config(),
            evaluated_at=datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
        )
    finally:
        calendar.dispose()

    contracts = {item.horizon_days: item for item, _ in repository.saved}
    assert contracts[1].prediction_kind.value == "CALIBRATED_OOS"
    assert contracts[1].model_artifact_hash == artifact["artifact_hash"]
    assert contracts[1].model_inputs["overnight_gap_pct"] == 0.5
    assert contracts[1].imputed_feature_keys == ("overnight_gap_pct",)
    assert contracts[1].as_dict()["imputed_feature_keys"] == [
        "overnight_gap_pct"
    ]
    assert contracts[5].prediction_kind.value == "CALIBRATED_OOS"
    assert contracts[20].prediction_kind.value == "CALIBRATED_OOS"
    assert result["runtime_model_selection"]["T+1"]["status"] == (
        "REAL_OOS_MODEL"
    )
    assert result["runtime_model_selection"]["T+5"]["status"] == (
        "REAL_OOS_MODEL"
    )
    assert result["runtime_model_selection"]["T+20"]["status"] == (
        "REAL_OOS_MODEL"
    )
    assert result["runtime_suite_release_id"] == "runtime-suite"
    assert {
        item["suite_release_id"]
        for item in result["runtime_model_selection"].values()
    } == {"runtime-suite"}
    assert result["order_authority"] is False


def test_worker_scores_process_verified_block_suite_for_forward_research_only(
    monkeypatch,
):
    import server.trading_v3.shadow_intelligence_worker as worker

    monkeypatch.setattr(
        worker, "verify_horizon_artifact", lambda value: dict(value)
    )
    monkeypatch.setattr(
        worker,
        "predict_horizon_artifact",
        lambda value, features: SimpleNamespace(
            score=0.62,
            expected_return_net_pct=-0.1,
            probability_positive=0.48,
            model_artifact_hash=value["artifact_hash"],
            feature_protocol_hash=value["feature_protocol_hash"],
            contract_eligible=False,
        ),
    )
    repository = _ResearchRuntimeRepository(config_hash())
    calendar = _calendar_engine()
    try:
        result = materialize_proxy_horizon_contracts(
            repository,
            calendar,
            config=load_v3_config(),
            evaluated_at=datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
        )
    finally:
        calendar.dispose()

    assert repository.saved
    assert all(
        contract.prediction_kind.value == "PROXY_SCORE"
        and contract.expected_return_net_pct is None
        and contract.probability_positive is None
        and contract.calibration_evidence is None
        and contract.imputed_feature_keys == ()
        and contract.order_authority is False
        for contract, _ in repository.saved
    )
    assert all(
        item["status"] == "BLOCKED_GATE_FORWARD_SHADOW"
        and item["suite_authority"] == "BLOCKED_GATE_FORWARD_RESEARCH"
        and item["promotion_eligible"] is False
        and item["order_authority"] is False
        for item in result["runtime_model_selection"].values()
    )
    assert result["runtime_suite_release_id"] == "runtime-suite"
    assert result["runtime_suite_authority"] == (
        "BLOCKED_GATE_FORWARD_RESEARCH"
    )
    assert result["blocked_gate_forward_research_contract_count"] == len(
        repository.saved
    )
    assert result["real_oos_contract_count"] == 0
    assert result["order_authority"] is False


def test_runtime_drawdown_matches_training_close_max_definition():
    from server.trading_v3.horizon_models import (
        _add_point_in_time_features,
        _normalize_bars,
    )

    dates = pd.bdate_range("2026-01-05", periods=70)
    closes = [10.0 + index * 0.02 for index in range(70)]
    closes[-10] = 14.0
    closes[-1] = 11.0
    rows = []
    previous = closes[0]
    for index, (trade_date, close) in enumerate(zip(dates, closes)):
        pre_close = previous if index else close
        rows.append({
            "stock_code": "600001",
            "trade_date": trade_date,
            "open": close,
            "high": 20.0 if index == 65 else close + 0.5,
            "low": close - 0.5,
            "close": close,
            "pre_close": pre_close,
            "amount": 100_000_000,
            "change_pct": (close / pre_close - 1.0) * 100.0,
        })
        previous = close
    normalized, _ = _normalize_bars(pd.DataFrame(rows), dates)
    trained = _add_point_in_time_features(normalized).iloc[-1]

    runtime_20 = daily_features._rolling_close_drawdown_pct(
        normalized["_adjusted_close"],
        window=20,
    )
    runtime_60 = daily_features._rolling_close_drawdown_pct(
        normalized["_adjusted_close"],
        window=60,
    )

    assert runtime_20 == pytest.approx(trained["drawdown_20d_pct"])
    assert runtime_60 == pytest.approx(trained["drawdown_60d_pct"])
    assert runtime_20 != pytest.approx(
        (11.0 / 20.0 - 1.0) * 100.0
    )


def test_history_evidence_distinguishes_69_of_70_and_nonconsecutive():
    evidence = _history_features(69)
    assert evidence["observed_history_sessions"] == 69
    assert evidence["history_sessions_consecutive"] is True
    assert (
        evidence["history_session_dates_hash"]
        == evidence["expected_history_session_dates_hash"]
    )

    broken = _history_features(70, consecutive=False)
    assert broken["observed_history_sessions"] == 70
    assert broken["history_sessions_consecutive"] is False
    assert (
        broken["history_session_dates_hash"]
        != broken["expected_history_session_dates_hash"]
    )


@pytest.mark.parametrize(
    ("invalid_features", "reason_code", "observed_sessions"),
    (
        (
            _all_proxy_features(history_sessions=69),
            "FEATURE_HISTORY_INSUFFICIENT",
            69,
        ),
        (
            _all_proxy_features(history_consecutive=False),
            "FEATURE_HISTORY_NON_CONSECUTIVE",
            70,
        ),
    ),
)
def test_one_invalid_history_candidate_is_blocked_without_suite_fallback(
    monkeypatch,
    invalid_features,
    reason_code,
    observed_sessions,
):
    import server.trading_v3.shadow_intelligence_worker as worker

    monkeypatch.setattr(worker, "verify_horizon_artifact", lambda value: dict(value))
    monkeypatch.setattr(
        worker,
        "predict_horizon_artifact",
        lambda value, features: SimpleNamespace(
            score=0.73,
            expected_return_net_pct=1.4,
            probability_positive=0.73,
            model_artifact_hash=value["artifact_hash"],
            feature_protocol_hash=value["feature_protocol_hash"],
        ),
    )
    repository = _RuntimeRepository(config_hash())
    repository.rows.append(_source_row(
        "quality_momentum",
        4,
        source_hash=config_hash(),
        features=invalid_features,
    ))
    calendar = _calendar_engine()
    try:
        result = materialize_proxy_horizon_contracts(
            repository,
            calendar,
            config=load_v3_config(),
            evaluated_at=datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
        )
    finally:
        calendar.dispose()

    contracts = {
        (item.horizon_days, item.stock_code): item
        for item, _ in repository.saved
    }
    assert (20, "600003") in contracts
    assert (20, "600004") not in contracts
    assert all(
        item["status"] == "REAL_OOS_MODEL"
        for item in result["runtime_model_selection"].values()
    )
    assert result["runtime_suite_release_id"] == "runtime-suite"
    assert result["proxy_fallback_contract_count"] == 0
    t20 = result["runtime_model_selection"]["T+20"]
    assert t20["blocked_candidate_count"] == 1
    assert t20["blocked_candidates"] == [{
        "stock_code": "600004",
        "source_strategy_key": "quality_momentum",
        "reason_code": reason_code,
        "observed_history_sessions": observed_sessions,
        "required_history_sessions": 70,
    }]
    assert (
        f"T+20:600004:{reason_code}"
        in result["blockers"]
    )
    assert contracts[(1, "600001")].imputed_feature_keys == (
        "overnight_gap_pct",
    )


def test_nonconsecutive_history_is_candidate_local_block():
    import server.trading_v3.shadow_intelligence_worker as worker

    with pytest.raises(worker._CandidateFeatureBlocked) as error:
        worker._artifact_model_inputs(
            _real_artifact(20),
            _all_proxy_features(history_consecutive=False),
        )
    assert error.value.reason_code == "FEATURE_HISTORY_NON_CONSECUTIVE"
    assert error.value.observed_history_sessions == 70
    assert error.value.required_history_sessions == 70


def test_worker_falls_back_all_horizons_when_complete_suite_is_unavailable(
    monkeypatch,
):
    import server.trading_v3.shadow_intelligence_worker as worker

    monkeypatch.setattr(worker, "verify_horizon_artifact", lambda value: dict(value))
    repository = _RuntimeRepository(config_hash(), suite_available=False)
    calendar = _calendar_engine()
    try:
        result = materialize_proxy_horizon_contracts(
            repository,
            calendar,
            config=load_v3_config(),
            evaluated_at=datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
        )
    finally:
        calendar.dispose()

    assert all(
        contract.prediction_kind.value == "PROXY_SCORE"
        for contract, _ in repository.saved
    )
    assert all(
        item["status"] == "PROXY_FALLBACK"
        and item["reason_codes"]
        == [
            "NO_COMPLETE_RELEASED_OOS_VERIFIED_SUITE",
            "RESEARCH_ARTIFACT_REGISTRY_UNAVAILABLE",
        ]
        for item in result["runtime_model_selection"].values()
    )
    assert result["runtime_suite_release_id"] is None


@pytest.mark.parametrize("suite_available", [False, True])
def test_duplicate_stock_source_attribution_prefers_selected_sleeve(
    monkeypatch,
    suite_available,
):
    """Equal stock predictions bind to the strategy the portfolio selected."""

    import server.trading_v3.shadow_intelligence_worker as worker

    monkeypatch.setattr(
        worker, "verify_horizon_artifact", lambda value: dict(value)
    )
    if suite_available:
        monkeypatch.setattr(
            worker,
            "predict_horizon_artifact",
            lambda value, features: SimpleNamespace(
                score=0.73,
                expected_return_net_pct=1.4,
                probability_positive=0.73,
                model_artifact_hash=value["artifact_hash"],
                feature_protocol_hash=value["feature_protocol_hash"],
            ),
        )
    repository = _RuntimeRepository(
        config_hash(), suite_available=suite_available
    )
    theme = _source_row(
        "theme_diffusion", 4, source_hash=config_hash()
    )
    reversal = _source_row(
        "oversold_reversal", 5, source_hash=config_hash()
    )
    for row in (theme, reversal):
        row["stock_code"] = "600099"
    theme["raw_score"] = 0.91
    reversal["raw_score"] = 0.80
    reversal["selection_status"] = "SELECTED"
    reversal["selection_reason_code"] = "TARGET_PORTFOLIO_SELECTED"
    reversal["selection_snapshot"] = {
        **reversal["selection_snapshot"],
        "target_id": "target-600099",
        "target_strategy_keys": ["oversold_reversal"],
        "selection_status": "SELECTED",
        "selection_reason_code": "TARGET_PORTFOLIO_SELECTED",
    }
    # Put the rejected, higher raw-score row first to reproduce the old
    # row-order-dependent attribution.
    repository.rows.extend((theme, reversal))
    calendar = _calendar_engine()
    try:
        result = materialize_proxy_horizon_contracts(
            repository,
            calendar,
            config=load_v3_config(),
            evaluated_at=datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
        )
    finally:
        calendar.dispose()

    contract = next(
        item
        for item, _source_id in repository.saved
        if item.horizon_days == 5 and item.stock_code == "600099"
    )
    assert contract.source_strategy_key == "oversold_reversal"
    assert contract.selection_status == "SELECTED"
    assert result["source_attribution_policy"] == SOURCE_ATTRIBUTION_POLICY
    assert result["runtime_model_selection"]["T+5"][
        "source_attribution_policy"
    ] == SOURCE_ATTRIBUTION_POLICY


def test_duplicate_rejected_sources_prefer_strongest_raw_sleeve():
    repository = _RuntimeRepository(config_hash(), suite_available=False)
    lower = _source_row(
        "theme_diffusion", 4, source_hash=config_hash()
    )
    higher = _source_row(
        "oversold_reversal", 5, source_hash=config_hash()
    )
    for row in (lower, higher):
        row["stock_code"] = "600098"
    lower["raw_score"] = 0.79
    higher["raw_score"] = 0.88
    repository.rows.extend((lower, higher))
    calendar = _calendar_engine()
    try:
        materialize_proxy_horizon_contracts(
            repository,
            calendar,
            config=load_v3_config(),
            evaluated_at=datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
        )
    finally:
        calendar.dispose()

    contract = next(
        item
        for item, _source_id in repository.saved
        if item.horizon_days == 5 and item.stock_code == "600098"
    )
    assert contract.source_strategy_key == "oversold_reversal"
    assert contract.selection_status == "REJECTED"


def test_worker_never_attaches_current_artifact_to_old_config_run(monkeypatch):
    import server.trading_v3.shadow_intelligence_worker as worker

    monkeypatch.setattr(worker, "verify_horizon_artifact", lambda value: dict(value))
    repository = _RuntimeRepository(_digest("old-config"))
    calendar = _calendar_engine()
    try:
        result = materialize_proxy_horizon_contracts(
            repository,
            calendar,
            config=load_v3_config(),
            evaluated_at=datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
        )
    finally:
        calendar.dispose()

    assert repository.lookups == []
    assert all(
        contract.prediction_kind.value == "PROXY_SCORE"
        for contract, _ in repository.saved
    )
    assert all(
        row["reason_codes"] == ["SOURCE_RUN_CONFIG_NOT_CURRENT"]
        for row in result["runtime_model_selection"].values()
    )


def _api_artifact() -> dict:
    ledger_content_sha256 = _digest("api-candidate-ledger-content")
    ledger_canonical_sha256 = _digest("api-candidate-ledger-canonical")
    ledger_reference_hash = _digest("api-candidate-ledger-reference")
    feature_names = [
        "return_1d_pct",
        "overnight_gap_pct",
        "intraday_return_pct",
        "amount_ratio_1_20",
        "range_1d_pct",
        "close_location_value",
        "volatility_5d_pct",
        "relative_return_1d_pct",
    ]
    return {
        "artifact_id": _digest("artifact-id"),
        "artifact_status": "OOS_VERIFIED",
        "protocol_status": "CURRENT_V3_LEDGER_VERIFIED",
        "runtime_eligible": True,
        "training_receipt_status": "PROCESS_VERIFIED",
        "candidate_ledger_schema_version": (
            trading_v3.HORIZON_CANDIDATE_LEDGER_SCHEMA
        ),
        "candidate_ledger_content_sha256": ledger_content_sha256,
        "candidate_ledger_row_count": 120,
        "ledger_registration_evidence_hash": _digest(
            "api-ledger-registration-evidence"
        ),
        "registration_verification_hash": _digest(
            "api-ledger-registration-verification"
        ),
        "registration_evidence_hash": _digest(
            "api-artifact-registration-evidence"
        ),
        "artifact": {
            "schema_version": trading_v3.HORIZON_ARTIFACT_SCHEMA,
            "release_id": "suite-1:real-t1:1.0:T+1",
            "suite_release_id": "suite-1",
            "model_key": "real-t1",
            "model_version": "1.0",
            "model_protocol": trading_v3.HORIZON_MODEL_PROTOCOL,
            "horizon_days": 1,
            "prediction_kind": "CALIBRATED_OOS",
            "contract_eligible": True,
            "contract_eligibility_scope": (
                trading_v3.HORIZON_CONTRACT_ELIGIBILITY_SCOPE
            ),
            "paper_eligible": False,
            "production_eligible": False,
            "candidate_ledger_registration_required": True,
            "artifact_hash": _digest("api-artifact"),
            "feature_protocol_hash": _digest("api-features"),
            "oos_evidence_hash": _digest("api-oos"),
            "config_hash": config_hash(),
            "code_version": "code-v1",
            "created_at": "2026-08-01T00:00:00+00:00",
            "valid_until": "2026-08-31",
            "order_authority": False,
            "model_spec": {"features": feature_names},
            "candidate_evaluation_ledger": {
                "schema_version": (
                    trading_v3.HORIZON_CANDIDATE_LEDGER_SCHEMA
                ),
                "binding_protocol": (
                    trading_v3.HORIZON_CANDIDATE_LEDGER_BINDING_PROTOCOL
                ),
                "encoding": trading_v3.HORIZON_CANDIDATE_LEDGER_ENCODING,
                "content_sha256": ledger_content_sha256,
                "canonical_records_sha256": ledger_canonical_sha256,
                "reference_hash": ledger_reference_hash,
                "row_count": 120,
                "session_count": 50,
                "evaluation_row_count": 100,
                "evaluation_session_count": 10,
                "fold_count": 3,
                "registration_verification_required": True,
            },
            "selection_policy": {
                "protocol": trading_v3.HORIZON_SELECTION_PROTOCOL,
                "candidate_domain": (
                    "FULL_UNIVERSE_TOP12_RESEARCH_DIAGNOSTIC"
                ),
                "order_authority": False,
            },
            "calibration": {
                "protocol": trading_v3.HORIZON_CALIBRATION_PROTOCOL,
            },
            "walk_forward": {
                "protocol": (
                    "EXPANDING_SESSION_SPLIT_PURGED_BY_LABEL_MATURITY_V2"
                ),
            },
            "gate": {
                "status": "PASS",
                "block_reasons": [],
                "gate_scope": "FULL_UNIVERSE_TOP12_RESEARCH_DIAGNOSTIC",
                "deployment_gate": False,
                "contract_eligible": True,
                "contract_eligibility_scope": (
                    trading_v3.HORIZON_CONTRACT_ELIGIBILITY_SCOPE
                ),
                "paper_eligible": False,
                "production_eligible": False,
                "order_authority": False,
            },
            "oos_evidence": {
                "distinct_train_sessions": 180,
                "distinct_oos_sessions": 50,
                "oos_sample_count": 120,
                "walk_forward_fold_count": 3,
                "selection_evidence": {
                    "protocol": trading_v3.HORIZON_SELECTION_PROTOCOL,
                    "economic_evaluation_scope": (
                        "FULL_UNIVERSE_TOP12_RESEARCH_DIAGNOSTIC"
                    ),
                    "deployment_candidate_domain_verified": False,
                    "candidate_sample_count": 120,
                    "eligible_candidate_count": 48,
                    "candidate_ledger_schema": (
                        trading_v3.HORIZON_CANDIDATE_LEDGER_SCHEMA
                    ),
                    "candidate_ledger_content_sha256": (
                        ledger_content_sha256
                    ),
                    "candidate_ledger_canonical_records_sha256": (
                        ledger_canonical_sha256
                    ),
                    "candidate_ledger_reference_hash": ledger_reference_hash,
                    "order_authority": False,
                },
                "selected_oos_sample_count": 36,
                "selected_oos_session_count": 12,
                "net_expectancy_after_cost_pct": 0.42,
                "profit_factor": 1.45,
                "cost_coverage_ratio": 2.2,
                "unconditional_baseline_net_expectancy_after_cost_pct": -0.08,
                "unconditional_baseline_profit_factor": 0.91,
                "unconditional_baseline_cost_coverage_ratio": 0.6,
                "direction_evidence": {
                    "protocol": "PREQUENTIAL_RUNTIME_SESSION_FOLD_EQUAL_IC_V2",
                    "session_count": 12,
                    "valid_session_count": 10,
                    "expected_return_rank_ic": 0.09,
                    "probability_rank_ic": 0.08,
                    "gate_direction_rank_ic": 0.08,
                },
                "calibration_is_oos_only": True,
                "calibration_evaluation_is_prequential": True,
                "calibration_labels_purged_by_maturity": True,
                "economic_metrics_use_frozen_selection_ledger": True,
                "calibration_evaluation_sample_count": 100,
                "distinct_calibration_evaluation_sessions": 10,
                "candidate_evaluation_ledger_reference_hash": (
                    ledger_reference_hash
                ),
            },
        },
    }


class _ApiRepository:
    def horizon_contracts(self, *, run_uid, limit):
        artifact = _api_artifact()["artifact"]
        return [
            {
                "contract_id": _digest("contract-t1"),
                "run_uid": run_uid,
                "stock_code": "600001",
                "model_key": "real-t1",
                "model_version": "1.0",
                "model_artifact_hash": artifact["artifact_hash"],
                "feature_protocol_hash": artifact["feature_protocol_hash"],
                "horizon_days": 1,
                "prediction_kind": "CALIBRATED_OOS",
                "decision_as_of": "2026-08-14T07:00:00+00:00",
                "score": 0.7,
                "expected_return_net_pct": 1.2,
                "probability_positive": 0.7,
                "calibration_evidence_hash": artifact["oos_evidence_hash"],
                "contract_hash": _digest("contract-hash-t1"),
                "derived_contract_status": "OPEN",
                "contract_json": json.dumps({
                    "imputed_feature_keys": ["overnight_gap_pct"]
                }),
            },
            {
                "contract_id": _digest("contract-t5"),
                "run_uid": run_uid,
                "stock_code": "600002",
                "model_key": "proxy-t5",
                "model_version": "proxy-v1",
                "model_artifact_hash": _digest("proxy-t5-artifact"),
                "horizon_days": 5,
                "prediction_kind": "PROXY_SCORE",
                "contract_hash": _digest("contract-hash-t5"),
                "derived_contract_status": "OPEN",
                "contract_json": json.dumps({"imputed_feature_keys": []}),
            },
        ]

    def horizon_outcomes(self, *, contract_ids, limit):
        return []

    def horizon_model_artifacts(self, *, limit):
        assert limit == 1000
        return [_api_artifact()]


def _strip_api_candidate_ledger(row: dict) -> None:
    for field in (
        "candidate_ledger_schema_version",
        "candidate_ledger_content_sha256",
        "candidate_ledger_row_count",
        "ledger_registration_evidence_hash",
        "registration_verification_hash",
    ):
        row.pop(field, None)
    artifact = row["artifact"]
    artifact.pop("candidate_ledger_registration_required", None)
    artifact.pop("candidate_evaluation_ledger", None)
    selection = artifact["oos_evidence"]["selection_evidence"]
    for field in (
        "candidate_ledger_schema",
        "candidate_ledger_content_sha256",
        "candidate_ledger_canonical_records_sha256",
        "candidate_ledger_reference_hash",
    ):
        selection.pop(field, None)
    artifact["oos_evidence"].pop(
        "candidate_evaluation_ledger_reference_hash",
        None,
    )


def test_horizon_api_projects_registry_runtime_state_without_raw_artifact(
    monkeypatch,
):
    monkeypatch.setattr(trading_v3, "_shadow_repo", _ApiRepository)
    result = trading_v3.latest_horizon_runtime(
        run_uid="runtime-run-20260814",
        limit=10,
    )
    data = result["data"]

    assert data["artifact_registry"]["status"] == "AVAILABLE"
    assert data["artifact_registry"]["current_v3_artifact_count"] == 1
    assert data["artifact_registry"]["pre_ledger_v2_artifact_count"] == 0
    assert data["artifact_registry"]["historical_v1_artifact_count"] == 0
    artifact = data["artifact_registry"]["artifacts"][0]
    assert artifact["artifact_hash"] == _digest("api-artifact")
    assert "artifact" not in artifact and "artifact_json" not in artifact
    assert artifact["candidate_ledger_schema_version"] == (
        trading_v3.HORIZON_CANDIDATE_LEDGER_SCHEMA
    )
    assert artifact["candidate_ledger_content_sha256"] == _digest(
        "api-candidate-ledger-content"
    )
    assert artifact["candidate_ledger_row_count"] == 120
    assert artifact["registration_verified"] is True
    assert data["runtime_model_selection"]["T+1"]["status"] == (
        "REAL_OOS_MODEL"
    )
    assert artifact["protocols"] == {
        "artifact": trading_v3.HORIZON_ARTIFACT_SCHEMA,
        "suite": trading_v3.HORIZON_SUITE_SCHEMA,
        "model": trading_v3.HORIZON_MODEL_PROTOCOL,
        "selection": trading_v3.HORIZON_SELECTION_PROTOCOL,
        "calibration": trading_v3.HORIZON_CALIBRATION_PROTOCOL,
    }
    selection = data["runtime_model_selection"]["T+1"]
    assert selection["candidate_economic_scope"][
        "economic_evaluation_scope"
    ] == "FULL_UNIVERSE_TOP12_RESEARCH_DIAGNOSTIC"
    assert selection["candidate_economic_scope"]["gate_scope"] == (
        "FULL_UNIVERSE_TOP12_RESEARCH_DIAGNOSTIC"
    )
    assert selection["candidate_economic_scope"]["deployment_gate"] is False
    assert selection["eligibility_boundary"] == {
        "contract_eligibility_scope": (
            trading_v3.HORIZON_CONTRACT_ELIGIBILITY_SCOPE
        ),
        "contract_eligible": True,
        "paper_eligible": False,
        "production_eligible": False,
        "evidence_status": "VERIFIED_SHADOW_BOUNDARY",
    }
    assert selection["candidate_ledger"] == {
        "schema_version": trading_v3.HORIZON_CANDIDATE_LEDGER_SCHEMA,
        "binding_protocol": (
            trading_v3.HORIZON_CANDIDATE_LEDGER_BINDING_PROTOCOL
        ),
        "encoding": trading_v3.HORIZON_CANDIDATE_LEDGER_ENCODING,
        "content_sha256": _digest("api-candidate-ledger-content"),
        "canonical_records_sha256": _digest(
            "api-candidate-ledger-canonical"
        ),
        "reference_hash": _digest("api-candidate-ledger-reference"),
        "row_count": 120,
        "session_count": 50,
        "evaluation_row_count": 100,
        "evaluation_session_count": 10,
        "fold_count": 3,
        "ledger_registration_evidence_hash": _digest(
            "api-ledger-registration-evidence"
        ),
        "registration_verification_hash": _digest(
            "api-ledger-registration-verification"
        ),
        "artifact_registration_evidence_hash": _digest(
            "api-artifact-registration-evidence"
        ),
        "registration_verified": True,
        "evidence_status": "REGISTERED_CONTENT_VERIFIED",
    }
    assert selection["candidate_ledger_schema_version"] == (
        trading_v3.HORIZON_CANDIDATE_LEDGER_SCHEMA
    )
    assert selection["candidate_ledger_content_sha256"] == _digest(
        "api-candidate-ledger-content"
    )
    assert selection["candidate_ledger_row_count"] == 120
    assert selection["ledger_registration_evidence_hash"] == _digest(
        "api-ledger-registration-evidence"
    )
    assert selection["registration_verification_hash"] == _digest(
        "api-ledger-registration-verification"
    )
    assert selection["registration_verified"] is True
    assert selection["selected_economics"] == {
        "selected_oos_sample_count": 36,
        "selected_oos_session_count": 12,
        "net_expectancy_after_cost_pct": 0.42,
        "profit_factor": 1.45,
        "cost_coverage_ratio": 2.2,
    }
    assert selection["unconditional_baseline"] == {
        "net_expectancy_after_cost_pct": -0.08,
        "profit_factor": 0.91,
        "cost_coverage_ratio": 0.6,
    }
    assert selection["session_direction"]["gate_direction_rank_ic"] == 0.08
    assert selection["calibration_evidence"][
        "labels_purged_by_maturity"
    ] is True
    assert "PASS" not in json.dumps(data, ensure_ascii=False)
    assert data["runtime_model_selection"]["T+1"][
        "imputed_feature_keys"
    ] == ["overnight_gap_pct"]
    assert data["runtime_model_selection"]["T+1"][
        "contract_artifact_binding_status"
    ] == "VERIFIED"
    assert data["runtime_model_selection"]["T+5"]["status"] == (
        "PROXY_FALLBACK"
    )
    assert data["runtime_model_selection"]["T+20"]["status"] == "COLLECTING"
    assert all(item["order_authority"] is False for item in data["contracts"])


def test_horizon_api_registry_failure_is_sanitized_and_fail_closed(monkeypatch):
    class Repository(_ApiRepository):
        def horizon_model_artifacts(self, *, limit):
            raise RuntimeError(
                "SELECT secret FROM mysql.user password=hunter2"
            )

    monkeypatch.setattr(trading_v3, "_shadow_repo", Repository)
    result = trading_v3.latest_horizon_runtime(
        run_uid="runtime-run-20260814",
        limit=10,
    )
    payload = json.dumps(result, ensure_ascii=False)

    assert result["data"]["artifact_registry"]["status"] == "UNAVAILABLE"
    assert all(
        item["status"] == "REGISTRY_UNAVAILABLE"
        for item in result["data"]["runtime_model_selection"].values()
    )
    assert "hunter2" not in payload
    assert "mysql.user" not in payload


def test_horizon_api_v1_is_historical_audit_only(monkeypatch):
    class Repository(_ApiRepository):
        def horizon_contracts(self, *, run_uid, limit):
            rows = super().horizon_contracts(run_uid=run_uid, limit=limit)
            rows[0]["contract_json"] = json.dumps({})
            return rows

        def horizon_model_artifacts(self, *, limit):
            row = _api_artifact()
            row["artifact"]["schema_version"] = (
                "probiga.trading-v3.independent-horizon-model-artifact.v1"
            )
            row["protocol_status"] = "HISTORICAL_AUDIT_ONLY"
            row["runtime_eligible"] = False
            _strip_api_candidate_ledger(row)
            return [row]

    monkeypatch.setattr(trading_v3, "_shadow_repo", Repository)
    result = trading_v3.latest_horizon_runtime(
        run_uid="runtime-run-20260814",
        limit=10,
    )
    data = result["data"]
    artifact = data["artifact_registry"]["artifacts"][0]

    assert data["artifact_registry"]["status"] == "HISTORICAL_AUDIT_ONLY"
    assert artifact["artifact_status"] == "HISTORICAL_AUDIT_ONLY"
    assert artifact["gate_status"] == "HISTORICAL_AUDIT_ONLY"
    assert artifact["current_runtime_eligible"] is False
    assert "selected_economics" not in artifact
    assert data["runtime_model_selection"]["T+1"]["status"] == (
        "HISTORICAL_AUDIT_ONLY"
    )
    assert data["runtime_model_selection"]["T+1"]["imputation"][
        "evidence_status"
    ] == "HISTORICAL_AUDIT_ONLY"
    assert data["contracts"][0]["imputation_evidence_status"] == (
        "HISTORICAL_FIELD_UNAVAILABLE"
    )
    assert "PASS" not in json.dumps(data, ensure_ascii=False)


def test_horizon_api_v2_is_pre_ledger_audit_only(monkeypatch):
    class Repository(_ApiRepository):
        def horizon_model_artifacts(self, *, limit):
            row = _api_artifact()
            row["artifact"]["schema_version"] = (
                trading_v3.HISTORICAL_HORIZON_ARTIFACT_SCHEMA_V2
            )
            row["protocol_status"] = "PRE_LEDGER_V2_AUDIT_ONLY"
            row["runtime_eligible"] = False
            _strip_api_candidate_ledger(row)
            return [row]

    monkeypatch.setattr(trading_v3, "_shadow_repo", Repository)
    result = trading_v3.latest_horizon_runtime(
        run_uid="runtime-run-20260814",
        limit=10,
    )
    data = result["data"]
    artifact = data["artifact_registry"]["artifacts"][0]
    selection = data["runtime_model_selection"]["T+1"]

    assert data["artifact_registry"]["status"] == "HISTORICAL_AUDIT_ONLY"
    assert data["artifact_registry"]["pre_ledger_v2_artifact_count"] == 1
    assert artifact["evidence_status"] == "PRE_LEDGER_V2_AUDIT_ONLY"
    assert artifact["protocols"]["suite"] == (
        trading_v3.HISTORICAL_HORIZON_SUITE_SCHEMA_V2
    )
    assert artifact["candidate_ledger"] == {
        "schema_version": "UNAVAILABLE_IN_PRE_LEDGER_V2",
        "registration_verified": False,
        "evidence_status": "PRE_LEDGER_V2_AUDIT_ONLY",
    }
    assert selection["status"] == "PRE_LEDGER_V2_AUDIT_ONLY"
    assert selection["order_authority"] is False
    assert "PASS" not in json.dumps(data, ensure_ascii=False)


def test_horizon_api_missing_v3_economics_is_unavailable(monkeypatch):
    class Repository(_ApiRepository):
        def horizon_model_artifacts(self, *, limit):
            row = _api_artifact()
            del row["artifact"]["oos_evidence"][
                "selected_oos_session_count"
            ]
            return [row]

    monkeypatch.setattr(trading_v3, "_shadow_repo", Repository)
    result = trading_v3.latest_horizon_runtime(
        run_uid="runtime-run-20260814",
        limit=10,
    )
    data = result["data"]
    artifact = data["artifact_registry"]["artifacts"][0]

    assert data["artifact_registry"]["status"] == (
        "AVAILABLE_WITH_UNAVAILABLE_ARTIFACTS"
    )
    assert artifact["evidence_status"] == "UNAVAILABLE"
    assert artifact["reason_codes"] == [
        "V3_ARTIFACT_OR_LEDGER_EVIDENCE_MISSING_OR_INVALID"
    ]
    assert data["runtime_model_selection"]["T+1"]["status"] == (
        "REGISTRY_UNAVAILABLE"
    )
    assert data["runtime_model_selection"]["T+1"][
        "selected_economics"
    ] is None


def test_horizon_api_missing_v3_validity_is_unavailable(monkeypatch):
    class Repository(_ApiRepository):
        def horizon_model_artifacts(self, *, limit):
            row = _api_artifact()
            del row["artifact"]["valid_until"]
            return [row]

    monkeypatch.setattr(trading_v3, "_shadow_repo", Repository)
    result = trading_v3.latest_horizon_runtime(
        run_uid="runtime-run-20260814",
        limit=10,
    )
    data = result["data"]

    assert data["artifact_registry"]["artifacts"][0][
        "evidence_status"
    ] == "UNAVAILABLE"
    assert data["runtime_model_selection"]["T+1"]["status"] == (
        "REGISTRY_UNAVAILABLE"
    )
    assert data["runtime_model_selection"]["T+1"][
        "artifact_valid_until"
    ] is None


@pytest.mark.parametrize(
    "ledger_failure",
    ["MISSING_LEDGER", "UNREGISTERED_LEDGER", "COUNT_MISMATCH"],
)
def test_horizon_api_v3_candidate_ledger_failure_is_unavailable(
    monkeypatch,
    ledger_failure,
):
    class Repository(_ApiRepository):
        def horizon_model_artifacts(self, *, limit):
            row = _api_artifact()
            if ledger_failure == "MISSING_LEDGER":
                del row["artifact"]["candidate_evaluation_ledger"]
            elif ledger_failure == "UNREGISTERED_LEDGER":
                row["protocol_status"] = "CURRENT_V3_LEDGER_UNVERIFIED"
                row["runtime_eligible"] = False
                row["registration_verification_hash"] = None
            else:
                row["candidate_ledger_row_count"] = 119
            return [row]

    monkeypatch.setattr(trading_v3, "_shadow_repo", Repository)
    result = trading_v3.latest_horizon_runtime(
        run_uid="runtime-run-20260814",
        limit=10,
    )
    data = result["data"]
    artifact = data["artifact_registry"]["artifacts"][0]
    selection = data["runtime_model_selection"]["T+1"]

    assert artifact["evidence_status"] == "UNAVAILABLE"
    assert artifact["reason_codes"] == [
        "V3_ARTIFACT_OR_LEDGER_EVIDENCE_MISSING_OR_INVALID"
    ]
    assert artifact["candidate_ledger"] == {
        "schema_version": "UNAVAILABLE",
        "registration_verified": False,
        "evidence_status": "UNAVAILABLE",
    }
    assert selection["status"] == "REGISTRY_UNAVAILABLE"
    assert selection["candidate_ledger"]["registration_verified"] is False
    assert selection["registration_verified"] is False
    assert selection["order_authority"] is False
    assert "PASS" not in json.dumps(data, ensure_ascii=False)


@pytest.mark.parametrize(
    ("field_path", "invalid_value"),
    [
        (("contract_eligibility_scope",), None),
        (("gate", "paper_eligible"), True),
        (("gate", "production_eligible"), True),
        (("gate", "contract_eligible"), False),
    ],
)
def test_horizon_api_invalid_shadow_eligibility_boundary_is_unavailable(
    monkeypatch,
    field_path,
    invalid_value,
):
    class Repository(_ApiRepository):
        def horizon_model_artifacts(self, *, limit):
            row = _api_artifact()
            target = row["artifact"]
            for key in field_path[:-1]:
                target = target[key]
            if invalid_value is None:
                del target[field_path[-1]]
            else:
                target[field_path[-1]] = invalid_value
            return [row]

    monkeypatch.setattr(trading_v3, "_shadow_repo", Repository)
    result = trading_v3.latest_horizon_runtime(
        run_uid="runtime-run-20260814",
        limit=10,
    )
    data = result["data"]
    artifact = data["artifact_registry"]["artifacts"][0]
    selection = data["runtime_model_selection"]["T+1"]

    assert artifact["evidence_status"] == "UNAVAILABLE"
    assert artifact["eligibility_boundary"] == {
        "contract_eligibility_scope": "UNAVAILABLE",
        "contract_eligible": False,
        "paper_eligible": False,
        "production_eligible": False,
        "evidence_status": "UNAVAILABLE",
    }
    assert selection["status"] == "REGISTRY_UNAVAILABLE"
    assert selection["eligibility_boundary"] == artifact[
        "eligibility_boundary"
    ]
    assert "PASS" not in json.dumps(data, ensure_ascii=False)
    assert selection["order_authority"] is False


@pytest.mark.parametrize("missing_field", ["protocol_status", "runtime_eligible"])
def test_horizon_api_missing_repository_protocol_metadata_is_unavailable(
    monkeypatch,
    missing_field,
):
    class Repository(_ApiRepository):
        def horizon_model_artifacts(self, *, limit):
            row = _api_artifact()
            del row[missing_field]
            return [row]

    monkeypatch.setattr(trading_v3, "_shadow_repo", Repository)
    result = trading_v3.latest_horizon_runtime(
        run_uid="runtime-run-20260814",
        limit=10,
    )
    data = result["data"]

    assert data["artifact_registry"]["status"] == (
        "AVAILABLE_WITH_UNAVAILABLE_ARTIFACTS"
    )
    assert data["artifact_registry"]["artifacts"][0][
        "evidence_status"
    ] == "UNAVAILABLE"
    assert data["runtime_model_selection"]["T+1"]["status"] == (
        "REGISTRY_UNAVAILABLE"
    )
    assert "PASS" not in json.dumps(data, ensure_ascii=False)


def test_horizon_api_contract_artifact_binding_mismatch_is_blocked(
    monkeypatch,
):
    class Repository(_ApiRepository):
        def horizon_contracts(self, *, run_uid, limit):
            rows = super().horizon_contracts(run_uid=run_uid, limit=limit)
            rows[0]["model_version"] = "stale-model-version"
            return rows

    monkeypatch.setattr(trading_v3, "_shadow_repo", Repository)
    result = trading_v3.latest_horizon_runtime(
        run_uid="runtime-run-20260814",
        limit=10,
    )
    selection = result["data"]["runtime_model_selection"]["T+1"]

    assert selection["status"] == "MIXED_MODEL_EVIDENCE_BLOCKED"
    assert selection["reason_codes"] == [
        "CONTRACT_ARTIFACT_BINDING_MISMATCH"
    ]
    assert selection["contract_artifact_binding_status"] == "UNAVAILABLE"
    assert selection["model_version"] == "1.0"
    assert selection["order_authority"] is False


def test_horizon_api_missing_v3_imputation_disclosure_is_unavailable(
    monkeypatch,
):
    class Repository(_ApiRepository):
        def horizon_contracts(self, *, run_uid, limit):
            rows = super().horizon_contracts(run_uid=run_uid, limit=limit)
            rows[0]["contract_json"] = json.dumps({})
            return rows

    monkeypatch.setattr(trading_v3, "_shadow_repo", Repository)
    result = trading_v3.latest_horizon_runtime(
        run_uid="runtime-run-20260814",
        limit=10,
    )
    data = result["data"]
    selection = data["runtime_model_selection"]["T+1"]

    assert selection["status"] == "REGISTRY_UNAVAILABLE"
    assert selection["reason_codes"] == [
        "V3_CONTRACT_IMPUTATION_EVIDENCE_UNAVAILABLE"
    ]
    assert selection["imputation"]["evidence_status"] == "UNAVAILABLE"
    assert data["contracts"][0]["imputation_evidence_status"] == (
        "HISTORICAL_FIELD_UNAVAILABLE"
    )
    assert selection["order_authority"] is False


def test_horizon_api_malformed_contract_json_is_sanitized_and_fail_closed(
    monkeypatch,
):
    class Repository(_ApiRepository):
        def horizon_contracts(self, *, run_uid, limit):
            rows = super().horizon_contracts(run_uid=run_uid, limit=limit)
            rows[0]["contract_json"] = json.dumps(["imputed_feature_keys"])
            return rows

    monkeypatch.setattr(trading_v3, "_shadow_repo", Repository)
    result = trading_v3.latest_horizon_runtime(
        run_uid="runtime-run-20260814",
        limit=10,
    )
    data = result["data"]

    assert data["status"] == "UNAVAILABLE"
    assert data["error_code"] == "HORIZON_CONTRACT_EVIDENCE_INVALID"
    assert data["contracts"] == []
    assert data["outcomes"] == []
    assert data["artifact_registry"]["status"] == "UNAVAILABLE"
    assert data["order_authority"] is False


def test_horizon_api_full_median_imputation_blocks_green_state(monkeypatch):
    class Repository(_ApiRepository):
        def horizon_contracts(self, *, run_uid, limit):
            rows = super().horizon_contracts(run_uid=run_uid, limit=limit)
            features = _api_artifact()["artifact"]["model_spec"]["features"]
            rows[0]["contract_json"] = json.dumps({
                "imputed_feature_keys": features
            })
            return rows

    monkeypatch.setattr(trading_v3, "_shadow_repo", Repository)
    result = trading_v3.latest_horizon_runtime(
        run_uid="runtime-run-20260814",
        limit=10,
    )
    selection = result["data"]["runtime_model_selection"]["T+1"]

    assert selection["status"] == "MODEL_INPUT_EVIDENCE_BLOCKED"
    assert selection["imputation"]["imputed_feature_ratio"] == 1.0
    assert selection["reason_codes"] == [
        "FULL_MEDIAN_IMPUTATION_OBSERVED"
    ]


def test_horizon_api_cross_suite_models_are_blocked(monkeypatch):
    def artifact_for(horizon: int, suite_id: str) -> dict:
        row = json.loads(json.dumps(_api_artifact()))
        artifact = row["artifact"]
        model_key = f"real-t{horizon}"
        artifact_hash = _digest(f"api-artifact-t{horizon}")
        row["artifact_id"] = artifact_hash
        artifact.update({
            "release_id": (
                f"{suite_id}:{model_key}:1.0:T+{horizon}"
            ),
            "suite_release_id": suite_id,
            "model_key": model_key,
            "horizon_days": horizon,
            "artifact_hash": artifact_hash,
            "feature_protocol_hash": _digest(f"api-features-t{horizon}"),
            "oos_evidence_hash": _digest(f"api-oos-t{horizon}"),
        })
        return row

    artifacts = [
        artifact_for(1, "suite-a"),
        artifact_for(5, "suite-a"),
        artifact_for(20, "suite-b"),
    ]

    class Repository:
        def horizon_contracts(self, *, run_uid, limit):
            return [
                {
                    "contract_id": _digest(f"contract-t{horizon}"),
                    "run_uid": run_uid,
                    "stock_code": f"6000{horizon:02d}",
                    "model_key": row["artifact"]["model_key"],
                    "model_version": "1.0",
                    "model_artifact_hash": row["artifact"]["artifact_hash"],
                    "feature_protocol_hash": row["artifact"][
                        "feature_protocol_hash"
                    ],
                    "horizon_days": horizon,
                    "prediction_kind": "CALIBRATED_OOS",
                    "contract_hash": _digest(f"contract-hash-t{horizon}"),
                    "derived_contract_status": "OPEN",
                    "contract_json": json.dumps({
                        "imputed_feature_keys": []
                    }),
                }
                for horizon, row in zip((1, 5, 20), artifacts, strict=True)
            ]

        def horizon_outcomes(self, *, contract_ids, limit):
            return []

        def horizon_model_artifacts(self, *, limit):
            return artifacts

    monkeypatch.setattr(trading_v3, "_shadow_repo", Repository)
    result = trading_v3.latest_horizon_runtime(
        run_uid="runtime-run-20260814",
        limit=10,
    )
    data = result["data"]

    assert data["model_suite_runtime"]["status"] == (
        "CROSS_SUITE_MODEL_EVIDENCE_BLOCKED"
    )
    assert data["model_suite_runtime"]["single_suite"] is False
    assert set(data["model_suite_runtime"]["suite_release_ids"]) == {
        "suite-a", "suite-b"
    }
    assert all(
        item["status"] == "CROSS_SUITE_MODEL_EVIDENCE_BLOCKED"
        for item in data["runtime_model_selection"].values()
    )


def test_horizon_page_uses_server_runtime_evidence_labels():
    javascript = (ROOT / "server/static/js/trading-v3.js").read_text(
        encoding="utf-8"
    )
    html = (ROOT / "server/static/trading-v3.html").read_text(
        encoding="utf-8"
    )

    assert "validation.runtime_model_selection" in javascript
    assert "validation.artifact_registry" in javascript
    assert "真实 OOS 模型" in javascript
    assert "代理回退" in javascript
    assert "注册表不可用" in javascript
    assert "imputed_feature_keys" in javascript
    assert "artifact_valid_until" in javascript
    assert "artifact_gate_status" in javascript
    assert "contract_artifact_binding_status" in javascript
    assert "contract_eligibility_scope" in javascript
    assert "paper_eligible" in javascript
    assert "production_eligible" in javascript
    assert "Shadow contract 资格不等于 PAPER 或生产资格" in javascript
    assert "candidate_ledger" in javascript
    assert "content_sha256" in javascript
    assert "registration_verified" in javascript
    assert "rows/sessions/folds" in javascript
    assert "PRE_LEDGER_V2_AUDIT_ONLY" in javascript
    assert "selected_economics" in javascript
    assert "unconditional_baseline" in javascript
    assert "session_direction" in javascript
    assert "labels_purged_by_maturity" in javascript
    assert "HISTORICAL_AUDIT_ONLY" in javascript
    assert "order=UNAVAILABLE" in javascript
    assert "/static/js/trading-v3.js?v=29" in html
    assert "/static/css/trading-v3.css?v=13" in html
