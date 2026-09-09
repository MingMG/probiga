from __future__ import annotations

import json

import pytest
from sqlalchemy import create_engine, text

from server.engine import strategy_shadow_trials as native
from server.engine.shadow_trial_policy import (
    frozen_shadow_execution_contract, shadow_exit_reason, shadow_initial_stop,
    validate_shadow_execution_contract, SHADOW_EXECUTION_POLICY,
)
from server.engine.strategy_governance import _default_governance_seed_contract
from server.engine.strategy_execution_adapters import persist_strategy_adapter_run_receipt
from server.engine.dynamic_shadow_ledger import (
    create_dynamic_shadow_trial_plans_from_candidate_facts,
    persist_strategy_adapter_candidate_facts, verify_dynamic_shadow_bootstrap_authorization,
    bind_dynamic_shadow_trial_to_existing_paper_evidence, verify_dynamic_shadow_trial,
)
from server.engine.strategy_execution_adapters import batch_dynamic_shadow_ledger_readiness
from server.trading_v3.config import load_v3_config
from server.trading_v3.paper_execution import materialize_dynamic_shadow_bootstrap_orders
from test_dynamic_shadow_ledger import _schema, _insert_bootstrap_prerequisites, _persist_capacity_run


def _strategy(key="ultra_short"):
    contract = _default_governance_seed_contract()["strategies"][key]
    return {**contract, "version_integrity_valid": True, "current_status": "SHADOW", "enabled": True}


def _analysis_row():
    return {
        "stock_code": "600036", "short_name": "招商银行", "ultra_short_score": 99.0,
        "recommend_status": "ALLOW", "signal_status": "CONFIRM",
        "chase_risk_status": "ALLOW", "ordinary_buy_eligible": True,
        "data_quality_flags": [], "event_risk_level": "LOW", "confidence_score": 77.0,
        "entry_price_low": 10.0, "entry_price_high": 10.0, "stop_loss_price": 9.5,
        "risk_reward_ratio": 3.0,
    }


def _complete_native_fills(connection, created, strategy, auth, receipt):
    """Real fixture ledger rows: entry, cost-inclusive FIFO exit and ownership."""
    connection.execute(text("UPDATE st_order_v2 SET status='FILLED',filled_quantity=100 WHERE order_id=:order_id"), created)
    connection.execute(text("""INSERT INTO st_fill_v2 VALUES (
        'native-buy-fill', :order_id,'paper-main-v2','600036','BUY',100,10,1000,1,-1001,
        'native-buy-quote','native-buy-match','native-buy-key','2026-08-24 09:31:00','2026-08-24 09:31:00')"""), created)
    connection.execute(text("""INSERT INTO st_order_v2 VALUES (
        'native-sell-order','paper-main-v2','native-exit','600036','SELL','LIMIT',11,100,100,'FILLED',NULL,
        '2026-08-26 09:30:00','2026-08-26 14:45:00','native-sell-key','2026-08-25 16:00:00','2026-08-26 09:31:00')"""))
    connection.execute(text("""INSERT INTO st_fill_v2 VALUES (
        'native-sell-fill','native-sell-order','paper-main-v2','600036','SELL',100,11,1100,1.5,1098.5,
        'native-sell-quote','native-sell-match','native-sell-fill-key','2026-08-26 09:31:00','2026-08-26 09:31:00')"""))
    intent_evidence = json.loads(connection.execute(text("SELECT evidence_json FROM st_trade_intent_v2 WHERE intent_id=:intent_id"), created).scalar_one())
    connection.execute(text("""INSERT INTO st_forward_trade_evidence_v3 VALUES (
        'native-evidence','paper-main-v2',:run_uid,:forecast_id,:intent_id,'600036',:key,:version,
        'PRIMARY','VERIFIED_SNAPSHOT','V3_PRIMARY_FORECAST_SNAPSHOT_V1',:keys,:ownership_hash,'EXECUTED_PAPER',
        'PAPER_EXECUTED_LEDGER_V1',:order_id,'native-buy-fill','2026-08-24','2026-08-24 09:31:00',
        100,10,1000,1,100,'["native-sell-fill"]','["native-sell-order"]','2026-08-26 09:31:00',
        11,1100,1.5,97.5,9.74025974,-2,12,'SHADOW_MAXIMUM_HOLDING_SESSIONS','MATURED')"""), {
            **created, "run_uid": receipt["run_uid"], "forecast_id": auth["shadow_forecast_id"],
            "key": strategy["strategy_key"], "version": strategy["current_version"],
            "keys": json.dumps([strategy["strategy_key"]]), "ownership_hash": intent_evidence["ownership_hash"],
        })
    connection.execute(text("""INSERT INTO st_forward_exit_allocation_v3 VALUES (
        'native-allocation','native-evidence','ATTRIBUTED','paper-main-v2','600036','native-buy-fill',
        'native-sell-fill','native-sell-order',0,100,1100,1.5,'2026-08-26 09:31:00','PAPER_FIFO_EXIT_ALLOCATION_V1')"""))


def test_native_analysis_batch_reaches_bounded_paper_intent_with_frozen_contract(monkeypatch):
    strategy = _strategy()
    source_calls = []
    receipt = {"identity": "immutable-publication"}
    snapshot = {"run_uid": "analysis-original", "scored_rows": [_analysis_row()]}

    def source(connection, target, run_uid=None):
        source_calls.append(run_uid)
        assert target == "2026-08-21"
        assert run_uid in (None, "analysis-original")
        return receipt, snapshot

    monkeypatch.setattr(native, "_analysis_source", source)
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        _schema(connection)
        _insert_bootstrap_prerequisites(connection)
        connection.exec_driver_sql("ALTER TABLE st_strategy_version ADD COLUMN evaluator_config_json TEXT")
        connection.execute(text("UPDATE st_strategy_registry SET strategy_key=:key,current_version=:version"),
                           {"key": strategy["strategy_key"], "version": strategy["current_version"]})
        connection.execute(text("""UPDATE st_strategy_version SET strategy_key=:key,version=:version,
            version_hash=:hash,source_kind=:kind,parameters_json=:parameters,evaluator_config_json=:config"""), {
                "key": strategy["strategy_key"], "version": strategy["current_version"],
                "hash": strategy["version_hash"], "kind": strategy["source_kind"],
                "parameters": json.dumps(strategy["parameters"]), "config": json.dumps(strategy["evaluator_config"]),
            })
        execution = native.native_trial_candidate_batch(connection, strategy, {
            "trade_date": "2026-08-21", "market": {"market_state": "trend_bullish"},
            "recommendation_rows": (), "configs": {}, "metrics": {},
        })
        assert len(execution["candidate_facts"]) == 1
        persist_strategy_adapter_run_receipt(connection, execution["receipt"])
        persist_strategy_adapter_candidate_facts(connection, candidate_receipt=execution["receipt"],
                                                 candidates=execution["candidate_facts"])
        plans = create_dynamic_shadow_trial_plans_from_candidate_facts(connection, strategy=strategy,
                    candidate_receipt=execution["receipt"], maximum_target_bp=100)
        assert plans["plan_count"] == 1
        result = materialize_dynamic_shadow_bootstrap_orders(connection, plan_ids=plans["plan_ids"],
                                                             governance_run_uid=_persist_capacity_run(connection, plans["plan_ids"], strategy))
        assert result["paper_order_count"] == 1, result
        assert result["real_order_count"] == 0
        intent = connection.execute(text("SELECT * FROM st_trade_intent_v2")).mappings().one()
        auth = json.loads(intent["evidence_json"])["dynamic_shadow_bootstrap"]
        verified = verify_dynamic_shadow_bootstrap_authorization(connection, auth, require_current_shadow=True)
        assert verified["execution_contract"]["strategy_version"] == strategy["current_version"]
        assert verified["execution_contract"]["maximum_holding_sessions"] == strategy["parameters"]["max_holding_days"]
        assert float(intent["initial_stop"]) == 9.5
        assert source_calls.count("analysis-original") >= 2
        _complete_native_fills(connection, result["created"][0], strategy, auth, execution["receipt"])
        bind_dynamic_shadow_trial_to_existing_paper_evidence(connection, plan_id=plans["plan_ids"][0],
                                                            forward_evidence_id="native-evidence")
        assert verify_dynamic_shadow_trial(connection, plans["plan_ids"][0])["status"] == "VERIFIED_MATURED_INTERNAL_PAPER_CHAIN"
        identity = (strategy["strategy_key"], strategy["current_version"], strategy["version_hash"], strategy["version_hash"])
        ready = batch_dynamic_shadow_ledger_readiness(connection, [identity])[identity]
        assert ready["funding_pipeline_ready"] is True, ready
        assert ready["verified_chain_count"] == 1
        # Source corruption cannot be laundered by an intact candidate/plan hash.
        snapshot["scored_rows"][0]["ordinary_buy_eligible"] = False
        with pytest.raises(ValueError, match="SOURCE_ROOT_MISMATCH"):
            verify_dynamic_shadow_bootstrap_authorization(connection, auth, require_current_shadow=True)


@pytest.mark.parametrize("change", [
    {"recommend_status": "WATCH"}, {"signal_status": "WATCH"},
    {"chase_risk_status": "BLOCK"}, {"ordinary_buy_eligible": None},
    {"data_quality_flags": ["missing_finance"]}, {"event_risk_level": "HIGH"},
    {"ultra_short_score": None},
])
def test_manifest_trial_never_manufactures_upstream_buy(change):
    row = {**_analysis_row(), **change}
    signal = native._manifest_signal(_strategy(), row, target="2026-08-21", market={"market_state": "trend_bullish"})
    assert signal["signal_status"] != "READY"


def test_native_source_replay_uses_frozen_version_rules(monkeypatch):
    from server.engine import strategy_center
    strategy = _strategy()
    monkeypatch.setattr(strategy_center, "load_stock_manifest", lambda: pytest.fail("must use frozen version"))
    assert native._manifest_signal(strategy, _analysis_row(), target="2026-08-21",
                                  market={"market_state": "trend_bullish"})["signal_status"] == "READY"


@pytest.mark.parametrize("key", sorted(load_v3_config()["sleeves"]))
def test_all_eight_v3_native_sleeves_have_explicit_source_and_regime_policy(key):
    strategy = _strategy(key)
    policy = strategy["evaluator_config"]["shadow_trial_routing"]
    assert set(policy["regime_weights"]) == set(load_v3_config()["regime"]["states"])
    forecast = {
        "forecast_id": "native-forecast", "stock_code": "600036", "strategy_key": key,
        "forecast_status": "RESEARCH_ONLY_UNCALIBRATED", "raw_score": 0.95,
        "confidence": 0.0, "initial_stop_pct": -5, "horizon_days": strategy["parameters"]["horizon_days"],
        "features_json": json.dumps({"price": 10, "entry_eligible": 1, "latest_tradable": 1}),
    }
    assert native._v3_signal(strategy, forecast, "2026-08-21")["signal_status"] == "READY"
    assert native._v3_signal(strategy, {**forecast, "forecast_status": "LEFT_SIDE_PREPARE"}, "2026-08-21") is None
    assert native._v3_signal(strategy, {**forecast, "features_json": '{"price":10}'}, "2026-08-21") is None
    regime = {"probabilities": {state: (1.0 if state == "RANGE" else 0.0) for state in policy["regime_weights"]}, "risk_asset_cap": 0.45}
    run = {"trade_date": "2026-08-21", "run_uid": "original", "result_hash": "a"*64,
           "regime_json": json.dumps(regime), "risk_asset_cap": 0.45}
    route = native._v3_market_route(strategy, run)
    assert route["multiplier"] == policy["regime_weights"]["RANGE"]
    del regime["probabilities"]["RISK_OFF"]
    with pytest.raises(ValueError, match="PROBABILITIES_INVALID"):
        native._v3_market_route(strategy, {**run, "regime_json": json.dumps(regime)})


def test_frozen_stop_horizon_and_tamper_boundary():
    contract = frozen_shadow_execution_contract(strategy_key="alpha", strategy_version="v1",
        version_hash="a"*64, source_kind="runtime_registry", parameters={"horizon_days": 5, "shadow_execution_policy": SHADOW_EXECUTION_POLICY},
        candidate={"stop_loss": 9.5})
    assert shadow_initial_stop(contract, 10) == 9.5
    assert shadow_exit_reason(contract, holding_sessions=3, session_low=9.6, protective_stop=9.5) is None
    assert shadow_exit_reason(contract, holding_sessions=4, session_low=None, protective_stop=9.5) == "SHADOW_MAXIMUM_HOLDING_SESSIONS"
    assert shadow_exit_reason(contract, holding_sessions=1, session_low=9.4, protective_stop=9.5) == "HARD_STOP"
    with pytest.raises(ValueError, match="CONTRACT_INVALID"):
        validate_shadow_execution_contract({**contract, "maximum_holding_sessions": 99})
    with pytest.raises(ValueError, match="HORIZON_MISSING"):
        frozen_shadow_execution_contract(strategy_key="alpha", strategy_version="v1", version_hash="a"*64,
                                         source_kind="runtime_registry", parameters={"shadow_execution_policy": SHADOW_EXECUTION_POLICY}, candidate={})


def test_historical_contract_does_not_follow_future_global_policy(monkeypatch):
    from server.engine import shadow_trial_policy as policy_module
    frozen_parameters = {"horizon_days": 5, "shadow_execution_policy": json.loads(json.dumps(SHADOW_EXECUTION_POLICY))}
    contract = frozen_shadow_execution_contract(strategy_key="alpha", strategy_version="old-v1",
        version_hash="a"*64, source_kind="runtime_registry", parameters=frozen_parameters, candidate={})
    future_policy = {**SHADOW_EXECUTION_POLICY, "maximum_initial_stop_pct": 4.0}
    monkeypatch.setattr(policy_module, "SHADOW_EXECUTION_POLICY", future_policy)
    replay = frozen_shadow_execution_contract(strategy_key="alpha", strategy_version="old-v1",
        version_hash="a"*64, source_kind="runtime_registry", parameters=frozen_parameters, candidate={})
    assert replay == contract
    assert shadow_initial_stop(replay, 10) == 9.2
    assert shadow_exit_reason(replay, holding_sessions=4, session_low=None, protective_stop=9.2) == "SHADOW_MAXIMUM_HOLDING_SESSIONS"
