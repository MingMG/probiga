from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from server.api.routers import trading_v3


def _proxy_forecast(horizon: int) -> dict:
    maturity = {
        1: "2026-08-18",
        5: "2026-08-24",
        20: "2026-09-14",
    }[horizon]
    source_evidence = {
        "forecast_id": f"source-t{horizon}",
        "strategy_key": f"source-t{horizon}",
        "raw_score": 0.82,
    }
    selection_evidence = {
        "selection_status": "REJECTED",
        "reason_code": "SHADOW_RESEARCH_ONLY",
    }

    def evidence_hash(value):
        return hashlib.sha256(json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")).hexdigest()

    return {
        "forecast_id": f"forecast-t{horizon}",
        "run_uid": "frozen-run",
        "stock_code": "600001",
        "model_key": f"independent-t{horizon}",
        "model_version": "proxy-v1",
        "source_strategy_key": f"source-t{horizon}",
        "source_forecast_hash": evidence_hash(source_evidence),
        "source_evidence": source_evidence,
        "decision_result_hash": "b" * 64,
        "feature_protocol_hash": f"{horizon % 10}" * 64,
        "model_artifact_hash": f"{(horizon + 1) % 10}" * 64,
        "model_inputs": {f"t{horizon}_signal": 0.82},
        "selection_status": "REJECTED",
        "selection_reason_code": "SHADOW_RESEARCH_ONLY",
        "selection_evidence_hash": evidence_hash(selection_evidence),
        "selection_evidence": selection_evidence,
        "horizon_days": horizon,
        "prediction_kind": "PROXY_SCORE",
        "decision_as_of": "2026-08-14T07:00:00+00:00",
        "feature_as_of": "2026-08-14T06:59:00+00:00",
        "decision_session_date": "2026-08-14",
        "entry_trade_date": "2026-08-17",
        "earliest_exit_trade_date": "2026-08-18",
        "outcome_matures_on": maturity,
        "entry_session_sequence": 1,
        "earliest_exit_session_sequence": 2,
        "outcome_maturity_session_sequence": 1 + horizon,
        "score": 0.82,
        "expected_return_net_pct": None,
        "probability_positive": None,
        "cost_assumption_pct": 0.25,
        "cost_model_version": "v3-frozen-cost-v1",
    }


def _replacement_request() -> dict:
    return {
        "policy": {
            "equity_cny": 100_000,
            "maximum_participation_rate": 0.1,
            "capacity_sessions": 1,
            "maximum_theme_weight": 1.0,
            "minimum_incremental_net_edge_pct": 0.0,
        },
        "candidates": [{
            "stock_code": "600001",
            "expected_return_gross_pct": 10.0,
            "entry_cost_pct": 0.1,
            "exit_cost_pct": 0.1,
            "average_daily_value_cny": 1_000_000,
        }],
        "holdings": [{
            "stock_code": "600002",
            "current_weight": 0.1,
            "expected_return_gross_pct": 1.0,
            "exit_cost_pct": 0.1,
        }],
    }


def _optimization_request() -> dict:
    return {
        "policy": {
            "equity_cny": 100_000,
            "risk_asset_cap": 1.0,
            "maximum_positions": 10,
            "maximum_single_weight": 0.2,
            "maximum_theme_weight": 1.0,
            "maximum_cluster_weight": 1.0,
            "maximum_turnover_weight": 1.0,
            "maximum_participation_rate": 1.0,
            "capacity_sessions": 1,
            "minimum_order_cny": 100,
            "minimum_edge_to_cost_multiple": 0.0,
            "standard_trade_risk": 0.01,
            "board_lot": 100,
            "fees": {
                "commission_rate": 0.0,
                "minimum_commission_cny": 0.0,
                "transfer_fee_rate": 0.0,
                "sell_stamp_duty_rate": 0.0,
                "default_slippage_rate": 0.0,
            },
        },
        "candidates": [{
            "stock_code": "600001",
            "selection_score": 1.0,
            "conservative_return_gross_pct": 10.0,
            "price": 10.0,
            "average_daily_value_cny": 1_000_000,
            "initial_stop_pct": -5.0,
            "desired_weight": 0.1,
        }],
        "current_positions": [],
    }


def test_research_governance_exposes_shadow_configuration_without_authority():
    result = trading_v3.research_governance()

    assert result["data"]["strategy_version"] == "trading_v3.11.0-paper"
    assert result["data"]["release_mode"] == "SHADOW_RESEARCH_ONLY"
    assert result["data"]["order_authority"] is False
    assert result["data"]["multi_horizon_forecasts"]["order_allowed"] is False
    assert result["real_trading_enabled"] is False


def test_client_calculators_are_unverified_at_both_envelope_levels():
    replacement = trading_v3.replacement_analysis(_replacement_request())
    optimization = trading_v3.portfolio_optimization(
        _optimization_request()
    )

    for result in (replacement, optimization):
        assert result["status"] == "UNVERIFIED_PREVIEW"
        assert result["persisted"] is False
        assert result["order_authority"] is False
        assert result["data"]["status"] == "UNVERIFIED_PREVIEW"
        assert result["data"]["persisted"] is False
        assert result["data"]["persisted_evidence_verified"] is False
        assert result["data"]["order_authority"] is False
        assert result["data"]["reason_codes"] == [
            "CLIENT_SUPPLIED_INPUT_UNVERIFIED"
        ]

    assert replacement["data"]["eligible_count"] == 1
    assert optimization["data"]["targets"][0]["target_quantity"] == 1000


def test_client_calculators_reject_malformed_nested_types_with_422():
    app = FastAPI()
    app.include_router(trading_v3.router)
    client = TestClient(app, raise_server_exceptions=False)
    cases = (
        ("/v3/research/replacement-analysis", {"policy": "bad"}),
        ("/v3/research/replacement-analysis", {"candidates": {}}),
        ("/v3/research/replacement-analysis", {"holdings": ["bad"]}),
        ("/v3/research/portfolio-optimization", {"policy": []}),
        ("/v3/research/portfolio-optimization", {"candidates": "bad"}),
        (
            "/v3/research/portfolio-optimization",
            {"current_positions": {}},
        ),
        (
            "/v3/research/portfolio-optimization",
            {
                **_optimization_request(),
                "policy": {
                    **_optimization_request()["policy"],
                    "fees": "bad",
                },
            },
        ),
    )

    for path, payload in cases:
        response = client.post(path, json=payload)
        assert response.status_code == 422, (path, response.text)
        assert response.json().get("detail")


def test_persisted_shadow_runtime_gets_never_grant_order_authority(monkeypatch):
    class Repository:
        def horizon_contracts(self, *, run_uid, limit):
            assert run_uid == "frozen-run"
            assert limit == 11
            return [{
                "contract_id": "contract-1",
                "run_uid": run_uid,
                "stock_code": "600001",
                "model_key": "t1",
                "model_version": "proxy-v1",
                "horizon_days": 1,
                "prediction_kind": "PROXY_SCORE",
                "derived_contract_status": "MATURED_VERIFIED",
                "contract_hash": "c" * 64,
            }]

        def horizon_outcomes(self, *, contract_ids, limit):
            assert contract_ids == {"contract-1"}
            assert limit == 1
            return [{
                "outcome_id": "outcome-1",
                "contract_id": "contract-1",
                "horizon_days": 1,
                "outcome_status": "MATURED_VERIFIED",
                "outcome_hash": "o" * 64,
            }]

        def latest_learning_run(self):
            return {
                "learning_run_id": "learning-1",
                "learning_status": "EVIDENCE_READY",
            }

        def verified_learning_run(self, learning_run_id):
            assert learning_run_id == "learning-1"
            return {
                "learning_run_id": learning_run_id,
                "learning_status": "EVIDENCE_READY",
                "sample_count": 360,
                "evaluated_at": datetime.now(timezone.utc),
                "evidence_source": "HORIZON_CONTRACT_OUTCOME_LEDGER",
                "evidence_hash": "e" * 64,
                "policy_hash": (
                    trading_v3._current_calibration_policy_hash()
                ),
                "learning_result_hash": "r" * 64,
                "metrics": {
                    "horizon_readiness": {"T+1": {"ready": True}},
                    "overall": {"sample_count": 360},
                    "provenance": {
                        "decision_config_hash": trading_v3.config_hash(),
                        "code_commit_sha": trading_v3.code_version()[0],
                    },
                },
            }

        def release_audit(self):
            return {
                "status": "READY",
                "releases": [{
                    "release_id": "release-1",
                    "current_stage": "PAPER_ELIGIBLE",
                    "effective_stage": "PAPER_ELIGIBLE",
                    "effective_blockers": [],
                    "config_hash": trading_v3.config_hash(),
                }],
            }

        def latest_calibration_gate(self, release_id):
            assert release_id == "release-1"
            return {
                "gate_evaluation_id": "gate-1",
                "gate_status": "PASS",
                "learning_run_id": "learning-1",
                "evidence_provenance_status": "PERSISTED_VERIFIED",
                "failure_codes_json": "[]",
                "policy_hash": (
                    trading_v3._current_calibration_policy_hash()
                ),
                "config_hash": trading_v3.config_hash(),
                "code_version": trading_v3.code_version()[0],
                "model_artifact_hash": "m" * 64,
                "gate_result_hash": "g" * 64,
                "evidence_observed_at": datetime.now(timezone.utc),
                "evidence_valid_until": (
                    datetime.now(timezone.utc) + timedelta(days=5)
                ),
            }

    monkeypatch.setattr(trading_v3, "_shadow_repo", Repository)

    horizons = trading_v3.latest_horizon_runtime(
        run_uid="frozen-run",
        limit=10,
    )
    learning = trading_v3.latest_counterfactual_learning_runtime()
    shadow = trading_v3.shadow_release_runtime_status()

    assert horizons["data"]["summary"]["verified_outcome_count"] == 1
    assert horizons["data"]["order_authority"] is False
    assert learning["data"]["evidence_verified"] is True
    assert learning["data"]["order_authority"] is False
    assert shadow["data"]["paper_eligible_count"] == 1
    assert shadow["data"]["releases"][0]["latest_gate"][
        "policy_hash"
    ] == trading_v3._current_calibration_policy_hash()
    assert shadow["data"]["order_authority"] is False


def test_horizon_runtime_filters_outcomes_before_applying_limit(monkeypatch):
    class Repository:
        def horizon_contracts(self, *, run_uid, limit):
            assert run_uid == "frozen-run"
            assert limit == 3
            return [
                {
                    "contract_id": f"contract-{index}",
                    "run_uid": run_uid,
                    "horizon_days": 1,
                    "derived_contract_status": "MATURED_VERIFIED",
                }
                for index in (1, 2, 3)
            ]

        def horizon_outcomes(self, *, contract_ids, limit):
            assert contract_ids == {"contract-1", "contract-2"}
            assert limit == 2
            return [
                {
                    "outcome_id": f"outcome-{index}",
                    "contract_id": f"contract-{index}",
                    "horizon_days": 1,
                    "outcome_status": "MATURED_VERIFIED",
                }
                for index in (1, 2)
            ]

    monkeypatch.setattr(trading_v3, "_shadow_repo", Repository)

    result = trading_v3.latest_horizon_runtime(
        run_uid="frozen-run",
        limit=2,
    )

    assert result["status"] == "partial"
    assert result["data"]["status"] == "TRUNCATED"
    assert result["data"]["pagination"] == {
        "limit": 2,
        "returned_contract_count": 2,
        "returned_outcome_count": 2,
        "contract_limit_reached": True,
        "truncated": True,
        "truncation_unknown": False,
    }
    assert {
        item["contract_id"] for item in result["data"]["outcomes"]
    } == {"contract-1", "contract-2"}


def _current_learning_runtime():
    return {
        "verified": {
            "learning_run_id": "learning-1",
            "learning_status": "EVIDENCE_READY",
        },
        "evidence_verified": True,
    }


def _current_gate(**overrides):
    gate = {
        "gate_evaluation_id": "gate-1",
        "gate_status": "PASS",
        "learning_run_id": "learning-1",
        "evidence_provenance_status": "PERSISTED_VERIFIED",
        "failure_codes_json": "[]",
        "policy_hash": trading_v3._current_calibration_policy_hash(),
        "config_hash": trading_v3.config_hash(),
        "code_version": trading_v3.code_version()[0],
        "model_artifact_hash": "m" * 64,
        "gate_result_hash": "g" * 64,
        "evidence_observed_at": datetime.now(timezone.utc),
        "evidence_valid_until": datetime.now(timezone.utc)
        + timedelta(days=5),
    }
    gate.update(overrides)
    return gate


def test_shadow_status_preserves_repository_effective_blockers(monkeypatch):
    class Repository:
        def release_audit(self):
            return {
                "status": "STALE_OR_BLOCKED",
                "releases": [{
                    "release_id": "release-1",
                    "current_stage": "PAPER_ELIGIBLE",
                    "effective_stage": "BLOCKED",
                    "effective_blockers": ["GATE_MODEL_ARTIFACT_STALE"],
                    "config_hash": trading_v3.config_hash(),
                }],
            }

        def latest_calibration_gate(self, release_id):
            assert release_id == "release-1"
            return _current_gate()

    monkeypatch.setattr(trading_v3, "_shadow_repo", Repository)
    monkeypatch.setattr(
        trading_v3,
        "_learning_runtime_truth",
        lambda _repository: _current_learning_runtime(),
    )

    result = trading_v3.shadow_release_runtime_status()

    release = result["data"]["releases"][0]
    assert result["status"] == "blocked"
    assert result["data"]["status"] == "BLOCKED"
    assert result["data"]["paper_eligible_count"] == 0
    assert release["effective_stage"] == "BLOCKED"
    assert "GATE_MODEL_ARTIFACT_STALE" in release["effective_blockers"]
    assert release["latest_gate"]["effective_pass"] is False


def test_shadow_status_rechecks_gate_code_currentness(monkeypatch):
    class Repository:
        def release_audit(self):
            return {
                "status": "READY",
                "releases": [{
                    "release_id": "release-1",
                    "current_stage": "PAPER_ELIGIBLE",
                    "effective_stage": "PAPER_ELIGIBLE",
                    "effective_blockers": [],
                    "config_hash": trading_v3.config_hash(),
                }],
            }

        def latest_calibration_gate(self, release_id):
            assert release_id == "release-1"
            return _current_gate(code_version="stale-code-version")

    monkeypatch.setattr(trading_v3, "_shadow_repo", Repository)
    monkeypatch.setattr(
        trading_v3,
        "_learning_runtime_truth",
        lambda _repository: _current_learning_runtime(),
    )

    result = trading_v3.shadow_release_runtime_status()

    release = result["data"]["releases"][0]
    assert result["status"] == "blocked"
    assert result["data"]["paper_eligible_count"] == 0
    assert release["effective_stage"] == "BLOCKED"
    assert "GATE_CODE_VERSION_STALE" in release["effective_blockers"]
    assert release["latest_gate"]["code_current"] is False
    assert release["latest_gate"]["effective_pass"] is False


def test_horizon_api_validates_independent_proxy_suite_but_grants_no_orders():
    result = trading_v3.validate_horizon_contracts(
        {"forecasts": [_proxy_forecast(day) for day in (1, 5, 20)]}
    )

    assert result["data"]["status"] == "UNVERIFIED_PREVIEW"
    assert result["data"]["diagnostic_status"] == "VALID"
    assert result["data"]["persisted_evidence_verified"] is False
    assert result["data"]["independent_model_keys"] is True
    assert list(result["data"]["horizons"]) == ["T+1", "T+5", "T+20"]
    assert result["data"]["order_authority"] is False


def test_horizon_api_contract_parser_round_trips_server_projection():
    contract = trading_v3._horizon_contract(_proxy_forecast(1))

    restored = trading_v3._horizon_contract(contract.as_dict())

    assert restored.forecast_id == contract.forecast_id
    assert restored.decision_session_date == contract.decision_session_date
    assert restored.order_authority is False


def test_horizon_api_contract_parser_rejects_read_only_authority_claim():
    payload = trading_v3._horizon_contract(_proxy_forecast(1)).as_dict()
    payload["order_authority"] = True

    try:
        trading_v3._horizon_contract(payload)
    except Exception as exc:
        assert "server-derived read-only field" in str(exc)
    else:
        raise AssertionError("client authority claim must be rejected")


def test_shadow_api_marks_client_evidence_unverified_and_grants_no_authority():
    evidence = {
        "release_id": "release-t1",
        "model_key": "independent-t1",
        "model_version": "1.0",
        "horizon_days": 1,
        "prediction_kind": "CALIBRATED_OOS",
        "matured_sample_count": 200,
        "oos_sample_count": 120,
        "walk_forward_fold_count": 4,
        "direction_rank_correlation": 0.20,
        "calibration_mae": 0.10,
        "brier_score": 0.18,
        "population_stability_index": 0.10,
        "net_expectancy_after_cost_pct": 0.30,
        "profit_factor": 1.50,
        "cost_coverage_ratio": 1.20,
        "observed_at": "2026-08-10T00:00:00+00:00",
        "valid_until": "2026-09-10T00:00:00+00:00",
    }
    gate = trading_v3.shadow_calibration_gate(
        {
            "evidence": evidence,
            "evaluated_at": "2026-08-16T00:00:00+00:00",
        }
    )
    assert gate["data"]["status"] == "UNVERIFIED_PREVIEW"
    assert gate["data"]["provisional_passed"] is True
    assert gate["data"]["passed"] is False
    assert "PERSISTED_EVIDENCE_REQUIRED" in gate["data"]["failure_codes"]
    assert gate["data"]["order_authority"] is False

    transition = trading_v3.shadow_transition_preview(
        {
            "current_stage": "PAPER_ELIGIBLE",
            "event": "REQUEST_ORDER_AUTHORITY",
        }
    )

    assert transition["data"]["accepted"] is False
    assert transition["data"]["reason_code"] == (
        "ORDER_AUTHORITY_OUTSIDE_RELEASE_STATE_MACHINE"
    )
    assert transition["data"]["order_authority"] is False


def test_shadow_transition_rejects_client_fabricated_gate():
    try:
        trading_v3.shadow_transition_preview(
            {
                "current_stage": "CALIBRATION_REVIEW",
                "event": "APPROVE_PAPER_ELIGIBILITY",
                "calibration_gate": {
                    "status": "PASS",
                    "horizon_days": 999,
                },
            }
        )
    except Exception as exc:
        assert getattr(exc, "status_code", None) == 422
        assert "not trusted" in str(getattr(exc, "detail", exc))
    else:
        raise AssertionError("fabricated calibration gate must be rejected")


def test_shadow_transition_rejects_client_raw_evidence():
    try:
        trading_v3.shadow_transition_preview(
            {
                "current_stage": "CALIBRATION_REVIEW",
                "event": "APPROVE_PAPER_ELIGIBILITY",
                "evidence": {"release_id": "client-claim"},
            }
        )
    except Exception as exc:
        assert getattr(exc, "status_code", None) == 422
        assert "persisted server-evaluated gate_id" in str(
            getattr(exc, "detail", exc)
        )
    else:
        raise AssertionError("raw client evidence must not transition release")


def test_calibration_gate_rejects_client_weakened_policy():
    try:
        trading_v3.shadow_calibration_gate(
            {
                "evidence": {},
                "policy": {"minimum_mature_samples": {"1": 1}},
                "evaluated_at": "2026-08-16T00:00:00+00:00",
            }
        )
    except Exception as exc:
        assert getattr(exc, "status_code", None) == 422
        assert "not accepted" in str(getattr(exc, "detail", exc))
    else:
        raise AssertionError("client policy override must be rejected")


def test_run_diff_api_compares_frozen_batches_and_remains_advisory(monkeypatch):
    batches = {
        "previous-run": {
            "run_uid": "previous-run",
            "decision_as_of": "2026-08-14T07:00:00+00:00",
            "items": [
                {
                    "forecast_id": "forecast-1",
                    "stock_code": "600001",
                    "strategy_key": "t1",
                    "horizon_days": 1,
                    "selection_status": "WATCH",
                }
            ],
        },
        "current-run": {
            "run_uid": "current-run",
            "decision_as_of": "2026-08-15T07:00:00+00:00",
            "items": [
                {
                    "forecast_id": "forecast-1",
                    "stock_code": "600001",
                    "strategy_key": "t1",
                    "horizon_days": 1,
                    "selection_status": "TARGET",
                }
            ],
        },
    }
    engine = object()
    monkeypatch.setattr(
        trading_v3,
        "_repo",
        lambda: SimpleNamespace(engine=engine),
    )
    monkeypatch.setattr(
        trading_v3,
        "_load_run_batch",
        lambda current_engine, run_uid: batches[run_uid],
    )

    result = trading_v3.decision_run_diff(
        "current-run",
        previous_run_uid="previous-run",
    )

    assert result["data"]["status"] == "CHANGED"
    assert result["data"]["summary"]["changed_count"] == 1
    assert result["data"]["order_authority"] is False


def test_run_batch_diff_uses_terminal_runs_and_strategy_level_selection():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE st_decision_run_v3 (run_uid TEXT, "
            "requested_as_of TEXT, trade_date TEXT, decision_at TEXT, "
            "status TEXT)"
        ))
        connection.execute(text(
            "CREATE TABLE st_alpha_forecast_v3 (forecast_id TEXT, "
            "run_uid TEXT, rank_no INTEGER, stock_code TEXT, "
            "strategy_key TEXT, horizon_days INTEGER, forecast_status TEXT, "
            "expected_return_net_pct REAL, model_version TEXT, "
            "feature_time TEXT, valid_until TEXT, reasons_json TEXT, "
            "features_json TEXT, theme_code TEXT)"
        ))
        connection.execute(text(
            "CREATE TABLE st_target_portfolio_v3 (run_uid TEXT, "
            "stock_code TEXT, target_weight REAL, "
            "conservative_return_pct REAL, status TEXT, "
            "theme_codes_json TEXT, strategy_keys_json TEXT)"
        ))
        connection.execute(text(
            "INSERT INTO st_decision_run_v3 VALUES "
            "('completed-run','2026-08-17','2026-08-14',"
            "'2026-08-17 09:15:00','COMPLETED'),"
            "('newer-run','2026-08-18','2026-08-15',"
            "'2026-08-18 09:15:00','COMPLETED'),"
            "('processing-run','2026-08-17','2026-08-14',"
            "'2026-08-17 09:16:00','PROCESSING')"
        ))
        connection.execute(text(
            "INSERT INTO st_alpha_forecast_v3 VALUES "
            "('f1','completed-run',1,'600001','t1',1,'VALIDATED_POSITIVE',"
            "1.2,'m1','2026-08-14 15:00:00','2026-08-20','[]','{}','AI'),"
            "('f5','completed-run',2,'600001','t5',5,'REJECTED',"
            "0.2,'m5','2026-08-14 15:00:00','2026-08-25','[]','{}','AI'),"
            "('f1-new','newer-run',1,'600001','t1',1,"
            "'VALIDATED_POSITIVE',1.2,'m1','2026-08-14 15:00:00',"
            "'2026-08-20','[]','{}','AI'),"
            "('f5-new','newer-run',2,'600001','t5',5,'REJECTED',"
            "0.2,'m5','2026-08-14 15:00:00','2026-08-25','[]','{}','AI')"
        ))
        connection.execute(text(
            "INSERT INTO st_target_portfolio_v3 VALUES "
            "('completed-run','600001',0.1,0.8,'PLANNED','[\"AI\"]',"
            "'[\"t1\"]'),"
            "('newer-run','600001',0.1,0.9,'PLANNED','[\"AI\"]',"
            "'[\"t1\"]')"
        ))

    batch = trading_v3._load_run_batch(engine, "completed-run")
    by_strategy = {item["strategy_key"]: item for item in batch["items"]}
    assert batch["decision_session_date"] == "2026-08-17"
    assert batch["data_date"] == "2026-08-14"
    assert by_strategy["t1"]["portfolio_selected"] is True
    assert by_strategy["t1"]["selection_status"] == "PLANNED"
    assert by_strategy["t1"]["target_weight"] == 0.1
    assert by_strategy["t1"]["conservative_return_pct"] == 0.8
    assert by_strategy["t5"]["portfolio_selected"] is False
    assert by_strategy["t5"]["selection_status"] == "REJECTED"
    assert by_strategy["t5"]["target_weight"] is None
    assert by_strategy["t5"]["conservative_return_pct"] is None

    newer = trading_v3._load_run_batch(engine, "newer-run")
    difference = trading_v3.diff_run_batches(batch, newer)
    assert difference["summary"]["changed_count"] == 1
    assert difference["changed"][0]["changes"] == [{
        "field": "conservative_return_pct",
        "before": 0.8,
        "after": 0.9,
    }]

    try:
        trading_v3._load_run_batch(engine, "processing-run")
    except Exception as exc:
        assert getattr(exc, "status_code", None) == 409
    else:
        raise AssertionError("partial run must not participate in batch diff")
