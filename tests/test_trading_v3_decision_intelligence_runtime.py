from __future__ import annotations

import json

from sqlalchemy import create_engine, text

from server.api.routers import trading_v3
from server.trading_v3.decision_truth import canonical_hash


def _decision_manifest() -> dict:
    manifest = {
        "schema_version": "probiga.trading-v3.decision-snapshot.v1",
        "requested_as_of": "2026-08-17",
        "trade_date": "2026-08-14",
        "decision_at": "2026-08-17 09:15:00",
        "knowledge_cutoff_at": "2026-08-17 09:15:00",
        "feature_time": "2026-08-14 15:00:00",
        "data_source": "TEST_FIXTURE",
        "data_snapshot_hash": "d" * 64,
        "account": {"account_id": "paper-v2", "status": "ACTIVE"},
        "equity": {
            "account_id": "paper-v2",
            "trade_date": "2026-08-17",
            "total_equity": 200_000,
        },
        "reconciliation": {
            "account_id": "paper-v2",
            "trade_date": "2026-08-17",
            "status": "PASS",
        },
        "positions": [],
        "open_orders": [],
        "valuation_prices": {},
        "derived_risk_state": {},
        "section_hashes": {},
    }
    manifest["manifest_hash"] = canonical_hash(manifest)
    return manifest


def _runtime_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    manifest = _decision_manifest()
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE st_decision_run_v3 ("
            "run_uid TEXT, status TEXT, requested_as_of TEXT, "
            "trade_date TEXT, decision_at TEXT, portfolio_json TEXT, "
            "result_hash TEXT, risk_asset_cap REAL)"
        ))
        connection.execute(text(
            "CREATE TABLE st_alpha_forecast_v3 ("
            "forecast_id TEXT, run_uid TEXT, stock_code TEXT, "
            "short_name TEXT, strategy_key TEXT, horizon_days INTEGER, "
            "raw_score REAL, expected_return_net_pct REAL, "
            "return_q10_pct REAL, expected_mae_pct REAL, "
            "initial_stop_pct REAL, forecast_status TEXT, theme_code TEXT, "
            "features_json TEXT, feature_time TEXT, valid_until TEXT)"
        ))
        connection.execute(text(
            "CREATE TABLE st_target_portfolio_v3 ("
            "run_uid TEXT, stock_code TEXT, target_weight REAL, "
            "estimated_roundtrip_cost_pct REAL, "
            "conservative_return_pct REAL, primary_strategy_key TEXT, "
            "strategy_keys_json TEXT, theme_codes_json TEXT)"
        ))
        connection.execute(text(
            "INSERT INTO st_decision_run_v3 VALUES "
            "(:run_uid, 'COMPLETED', '2026-08-17', '2026-08-14', "
            "'2026-08-17 09:15:00', :portfolio_json, :result_hash, 0.6)"
        ), {
            "run_uid": "runtime-run",
            "portfolio_json": json.dumps({"decision_snapshot": manifest}),
            "result_hash": "r" * 64,
        })
        connection.execute(text(
            "INSERT INTO st_alpha_forecast_v3 VALUES ("
            "'forecast-t5', 'runtime-run', '600001', '候选一', 't5', 5, "
            "0.84, 2.4, 0.8, -4.0, -5.0, 'VALIDATED_POSITIVE', 'AI', "
            ":features, '2026-08-14 15:00:00', '2026-08-25 15:00:00')"
        ), {
            "features": json.dumps({
                "price": 10.0,
                "average_amount_20d": 2_000_000,
                "theme_codes": ["AI"],
                "theme_cluster_keys": ["TECH"],
            }),
        })
        connection.execute(text(
            "INSERT INTO st_target_portfolio_v3 VALUES ("
            "'runtime-run', '600001', 0.1, 0.2, 0.8, 't5', "
            "'[\"t5\"]', '[\"AI\"]')"
        ))
    return engine


def test_server_decision_intelligence_uses_verified_persisted_snapshot():
    result = trading_v3._server_decision_intelligence_snapshot(
        _runtime_engine(),
        run_uid="runtime-run",
    )

    assert result["status"] == "READY"
    assert result["run"]["decision_session_date"] == "2026-08-17"
    assert result["run"]["data_date"] == "2026-08-14"
    assert result["input_summary"]["equity_cny"] == 200_000
    assert result["input_summary"]["candidate_count"] == 1
    assert result["portfolio_optimization"]["order_authority"] is False
    assert result["replacement_analysis"]["order_authority"] is False
    assert result["execution_revalidation_required"] is True
    assert result["order_authority"] is False


def test_latest_decision_intelligence_fails_closed_without_verified_input(
    monkeypatch,
):
    monkeypatch.setattr(
        trading_v3,
        "_repo",
        lambda: type("Repository", (), {"engine": _runtime_engine()})(),
    )

    result = trading_v3.latest_decision_intelligence_runtime(
        run_uid="missing-run",
    )

    assert result["status"] == "unavailable"
    assert result["data"]["status"] == "UNAVAILABLE"
    assert result["data"]["portfolio_optimization"]["status"] == (
        "UNAVAILABLE"
    )
    assert result["data"]["order_authority"] is False
