"""Native strategy sources for the common, bounded SHADOW trial ledger.

Each native signal is reproducible from its original published analysis run or
verified V3 decision ledger. A date partition or a caller's score is never a
substitute for that source. The same CandidateBatch and trial controller are
used for native and dynamically registered strategies.
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime
from types import SimpleNamespace
from typing import Any, Mapping

from sqlalchemy import text

from server.common.analysis_pool_receipt import canonical_sha256, decode_score_snapshot
from server.common.daily_delivery_control import load_published_analysis_receipt
from server.engine.strategy_execution_adapters import verified_native_candidate_batch, _stable_candidate_runtime_value
from server.engine.shadow_trial_policy import SHADOW_EXECUTION_POLICY


NATIVE_SOURCE_KINDS = frozenset({"immutable_manifest", "immutable_v3_sleeve"})
V3_ROUTE_POLICY = "VERIFIED_V3_FORECAST_REGIME_V1"


def manifest_shadow_policy(manifest: Mapping[str, Any]) -> dict:
    return {**manifest["paper_trial_routing"], "execution_policy": dict(SHADOW_EXECUTION_POLICY)}


def v3_shadow_policy(config: Mapping[str, Any], strategy_key: str) -> dict:
    """Freeze the existing V3 five-state/eight-sleeve map, never an ALLOW default."""
    from server.trading_v3.hypotheses import strategy_weights_for_regime
    from server.trading_v3.sleeves import SLEEVE_BUILDERS

    if set(config["sleeves"]) != set(SLEEVE_BUILDERS) or strategy_key not in SLEEVE_BUILDERS:
        raise ValueError("SHADOW_V3_NATIVE_BUILDER_COVERAGE_INVALID")
    routing = config["shadow_trial_routing"]
    thresholds = routing["minimum_raw_score_by_strategy"]
    if set(thresholds) != set(SLEEVE_BUILDERS):
        raise ValueError("SHADOW_V3_ENTRY_THRESHOLD_COVERAGE_INVALID")
    weights = {
        state: strategy_weights_for_regime(SimpleNamespace(probabilities={state: 1.0}))[strategy_key]
        for state in config["regime"]["states"]
    }
    return {
        "policy": V3_ROUTE_POLICY,
        "regime_weights": weights,
        "minimum_raw_score": float(thresholds[strategy_key]),
        "allowed_forecast_statuses": list(routing["allowed_forecast_statuses"]),
        "execution_policy": dict(SHADOW_EXECUTION_POLICY),
    }
_SIGNAL_FIELDS = (
    "stock_code", "stock_name", "strategy_key", "signal_direction", "signal_status",
    "model_confidence", "effective_score", "risk_reward_ratio", "entry_low",
    "entry_high", "stop_loss", "initial_stop_pct", "today_signal", "trade_date", "data_date",
    "effective_weight", "gate_status",
)


def _projection(signal: Mapping[str, Any]) -> dict[str, Any]:
    return {field: signal.get(field) for field in _SIGNAL_FIELDS}


def _analysis_source(connection: Any, target: str, run_uid: str | None = None) -> tuple[dict, dict]:
    receipt = load_published_analysis_receipt(connection.engine, target, run_uid=run_uid)
    snapshot = decode_score_snapshot(receipt["score_snapshot"], trade_date=target)
    return receipt, snapshot


def _manifest_signal(
    strategy: Mapping[str, Any], raw: Mapping[str, Any], *, target: str,
    market: Mapping[str, Any],
) -> dict[str, Any]:
    from server.engine.strategy_center import _strategy_signal_basis

    evaluator = strategy["evaluator_config"]
    policy = evaluator["shadow_trial_routing"]
    if policy["signal_policy"] != "FROZEN_INDEPENDENT_SCORE_V1":
        raise ValueError("SHADOW_MANIFEST_SIGNAL_POLICY_INVALID")
    key = str(strategy["strategy_key"])
    row = {**dict(raw), "publication_status": "ACTIVE"}
    raw_score = raw.get(str(evaluator["score_field"]))
    score = float(raw_score) if raw_score is not None else None
    if score is not None and (not math.isfinite(score) or not 0 <= score <= 100):
        raise ValueError("SHADOW_MANIFEST_SCORE_INVALID")
    basis = _strategy_signal_basis(
        row, key, score, frozen_manifest={
            "paper_trial_routing": policy,
            "strategies": [{"key": key, "parameters": strategy["parameters"]}],
        },
    )
    state = str(market.get("market_state") or "")
    multiplier = evaluator["market_regime_multipliers"].get(state)
    ready = (basis["direction"] == "BUY" and not basis["hard_block"]
             and multiplier is not None and float(multiplier) > 0 and state != "extreme_event")
    return {
        "stock_code": str(raw["stock_code"]).zfill(6),
        "stock_name": str(raw.get("short_name") or raw.get("stock_name") or ""),
        "strategy_key": key, "signal_direction": "BUY" if ready else "HOLD",
        "signal_status": "READY" if ready else "WATCH",
        "model_confidence": raw.get("confidence_score"), "effective_score": score,
        "effective_weight": float(multiplier) if multiplier is not None else 0.0,
        "gate_status": "PASS" if ready else "BLOCK",
        "risk_reward_ratio": raw.get("risk_reward_ratio"),
        "entry_low": raw.get("entry_price_low"), "entry_high": raw.get("entry_price_high"),
        "stop_loss": raw.get("stop_loss_price") or raw.get("trend_stop_price"),
        "initial_stop_pct": None, "today_signal": str(basis["reason"]),
        "trade_date": target, "data_date": target,
    }


def _v3_source(connection: Any, strategy: Mapping[str, Any], target: str, run_uid: str | None = None) -> dict:
    from server.trading_v3.paper_execution import _verify_persisted_decision_truth

    model_version = str((strategy.get("evaluator_config") or {}).get("model_version") or "")
    if not model_version or str(strategy["current_version"]) != f"{model_version}:{strategy['strategy_key']}":
        raise ValueError("SHADOW_V3_VERSION_BINDING_INVALID")
    rows = connection.execute(text("""
        SELECT * FROM st_decision_run_v3
        WHERE trade_date=:trade_date AND model_version=:model_version
          AND status='COMPLETED' AND mode='close'
          AND (:run_uid IS NULL OR run_uid=:run_uid)
        ORDER BY decision_at DESC, created_at DESC LIMIT 1
    """), {"trade_date": target, "model_version": model_version, "run_uid": run_uid}).mappings().all()
    if len(rows) != 1:
        raise ValueError("SHADOW_V3_COMPLETED_SOURCE_MISSING")
    run = dict(rows[0])
    targets = [dict(row) for row in connection.execute(text(
        "SELECT * FROM st_target_portfolio_v3 WHERE run_uid=:run_uid ORDER BY rank_no,stock_code"
    ), {"run_uid": run["run_uid"]}).mappings()]
    account = connection.execute(text(
        "SELECT * FROM st_trade_account_v2 WHERE account_id='paper-main-v2'"
    )).mappings().one()
    _portfolio, _equity, verified, reason = _verify_persisted_decision_truth(
        connection, account=dict(account), run=run, targets=targets,
        account_id="paper-main-v2", now=datetime.now().replace(microsecond=0),
        enforce_current_account=False,
    )
    if not verified or reason:
        raise ValueError(reason or "SHADOW_V3_SOURCE_UNVERIFIED")
    forecasts = {str(row["forecast_id"]): dict(row) for row in connection.execute(text(
        "SELECT * FROM st_alpha_forecast_v3 WHERE run_uid=:run_uid"
    ), {"run_uid": run["run_uid"]}).mappings()}
    return {"run": run, "targets": targets, "forecasts": forecasts}


def _v3_market_route(strategy: Mapping[str, Any], run: Mapping[str, Any]) -> dict:
    policy = strategy["evaluator_config"]["shadow_trial_routing"]
    if policy["policy"] != V3_ROUTE_POLICY:
        raise ValueError("SHADOW_V3_MARKET_POLICY_INVALID")
    regime = json.loads(str(run["regime_json"]))
    probabilities, weights = regime["probabilities"], policy["regime_weights"]
    if (set(probabilities) != set(weights)
        or any(not math.isfinite(float(p)) or not 0 <= float(p) <= 1 for p in probabilities.values())
        or abs(sum(float(p) for p in probabilities.values()) - 1) > 0.0001):
        raise ValueError("SHADOW_V3_REGIME_PROBABILITIES_INVALID")
    cap = float(regime["risk_asset_cap"])
    if not math.isfinite(cap) or not 0 <= cap <= 1 or abs(cap-float(run["risk_asset_cap"])) > 0.000001:
        raise ValueError("SHADOW_V3_REGIME_RISK_CAP_INVALID")
    payload = {
        "policy": V3_ROUTE_POLICY, "strategy_key": strategy["strategy_key"],
        "strategy_version": strategy["current_version"], "version_hash": strategy["version_hash"],
        "trade_date": str(run["trade_date"])[:10], "source_run_uid": str(run["run_uid"]),
        "result_hash": str(run["result_hash"]), "regime_sha256": canonical_sha256(regime),
        "probabilities": probabilities, "risk_asset_cap": cap,
        "regime_weights": weights,
        "multiplier": round(sum(float(probabilities[s])*float(weights[s]) for s in weights), 6) if cap > 0 else 0.0,
    }
    return {**payload, "route_hash": canonical_sha256(payload)}


def _v3_signal(strategy: Mapping[str, Any], forecast: Mapping[str, Any], target: str) -> dict | None:
    policy = strategy["evaluator_config"]["shadow_trial_routing"]
    if forecast["strategy_key"] != strategy["strategy_key"]:
        raise ValueError("SHADOW_V3_FORECAST_OWNER_MISMATCH")
    # The forecast engine only assigns these statuses after the native builder
    # returns SCORED. Missing facts, preparation states and hard blocks cannot
    # enter this trial. Calibration failure is observed, never called profit.
    if str(forecast["forecast_status"]) not in policy["allowed_forecast_statuses"]:
        return None
    score = float(forecast.get("raw_score") or 0)
    if not math.isfinite(score) or not float(policy["minimum_raw_score"]) <= score <= 1:
        return None
    features = json.loads(str(forecast.get("features_json") or "{}"))
    if any(features.get(key) not in (1, True, 1.0) for key in ("entry_eligible", "latest_tradable")):
        return None
    price = float(features.get("price") or 0)
    stop_pct = abs(float(forecast.get("initial_stop_pct") or 0))
    if not math.isfinite(price) or price <= 0 or not 0 < stop_pct < 100:
        raise ValueError("SHADOW_V3_TARGET_EXECUTION_FACTS_INVALID")
    if int(forecast["horizon_days"]) != int(strategy["parameters"]["horizon_days"]):
        raise ValueError("SHADOW_V3_FORECAST_HORIZON_MISMATCH")
    expected = forecast.get("expected_return_net_pct")
    return {
        "stock_code": str(forecast["stock_code"]), "stock_name": str(forecast.get("short_name") or ""),
        "strategy_key": str(strategy["strategy_key"]), "signal_direction": "BUY", "signal_status": "READY",
        "model_confidence": float(forecast.get("confidence") or 0) * 100,
        "effective_score": score * 100,
        "effective_weight": float(strategy["parameters"]["default_risk_weight"]),
        "gate_status": "PASS",
        "risk_reward_ratio": float(expected) / stop_pct if expected is not None else None,
        "entry_low": price, "entry_high": price, "stop_loss": round(price * (1-stop_pct/100), 3),
        "initial_stop_pct": stop_pct, "today_signal": str(forecast.get("reasons_json") or ""),
        "trade_date": target, "data_date": target,
    }


def native_trial_candidate_batch(
    connection: Any, strategy: Mapping[str, Any], context: Mapping[str, Any], *, source_cache: dict | None = None,
) -> dict[str, Any]:
    source_kind, target = str(strategy.get("source_kind") or ""), str(context["trade_date"])
    key = str(strategy["strategy_key"])
    rows = []
    market_route = None
    cache = source_cache if source_cache is not None else {}
    if source_kind == "immutable_manifest":
        if "analysis" not in cache:
            cache["analysis"] = _analysis_source(connection, target)
        receipt, snapshot = cache["analysis"]
        for raw in snapshot["scored_rows"]:
            signal = _manifest_signal(
                strategy, raw, target=target, market=context["market"],
            )
            if signal["signal_direction"] != "BUY" or signal["signal_status"] != "READY":
                continue
            signal["native_source_binding"] = {
                "source_kind": source_kind, "trade_date": target, "source_run_uid": snapshot["run_uid"],
                "receipt_sha256": canonical_sha256(receipt),
                "source_row_sha256": canonical_sha256(raw),
                "market": _stable_candidate_runtime_value(dict(context["market"])),
                "signal_sha256": canonical_sha256(_projection(signal)),
            }
            rows.append(signal)
    elif source_kind == "immutable_v3_sleeve":
        source_key = ("v3", strategy["evaluator_config"]["model_version"], target)
        if source_key not in cache:
            cache[source_key] = _v3_source(connection, strategy, target)
        source = cache[source_key]
        market_route = _v3_market_route(strategy, source["run"])
        for forecast in source["forecasts"].values():
            if forecast["strategy_key"] != key:
                continue
            signal = _v3_signal(strategy, forecast, target)
            if signal is None or market_route["multiplier"] <= 0:
                continue
            signal["native_source_binding"] = {
                "source_kind": source_kind, "trade_date": target, "source_run_uid": str(source["run"]["run_uid"]),
                "result_hash": str(source["run"]["result_hash"]),
                "forecast_id": str(forecast["forecast_id"]), "market_route": market_route,
                "signal_sha256": canonical_sha256(_projection(signal)),
            }
            rows.append(signal)
    else:
        raise ValueError("SHADOW_NATIVE_SOURCE_KIND_INVALID")
    rows.sort(key=lambda row: (-float(row.get("effective_score") or 0), row["stock_code"]))
    result = verified_native_candidate_batch(strategy, context, rows)
    result["native_market_route"] = market_route
    return result


def verify_native_trial_source(connection: Any, *, plan: Mapping[str, Any], candidate: Mapping[str, Any]) -> None:
    binding = candidate.get("native_source_binding")
    if not isinstance(binding, dict) or binding.get("trade_date") != plan["trade_date"]:
        raise ValueError("SHADOW_NATIVE_SOURCE_BINDING_MISSING")
    version = connection.execute(text("""
        SELECT strategy_key, version AS current_version, version_hash,
               source_kind, evaluator_config_json, parameters_json
        FROM st_strategy_version WHERE strategy_key=:key AND version=:version
    """), {"key": plan["strategy_key"], "version": plan["strategy_version"]}).mappings().one()
    strategy = {**dict(version), "evaluator_config": json.loads(str(version["evaluator_config_json"])),
                "parameters": json.loads(str(version["parameters_json"]))}
    if binding.get("source_kind") != version["source_kind"] or version["version_hash"] != plan["strategy_version_hash"]:
        raise ValueError("SHADOW_NATIVE_VERSION_SOURCE_MISMATCH")
    if version["source_kind"] == "immutable_manifest":
        receipt, snapshot = _analysis_source(connection, plan["trade_date"], str(binding["source_run_uid"]))
        matches = [row for row in snapshot["scored_rows"] if str(row["stock_code"]) == plan["stock_code"]]
        if len(matches) != 1 or canonical_sha256(receipt) != binding.get("receipt_sha256") or canonical_sha256(matches[0]) != binding.get("source_row_sha256"):
            raise ValueError("SHADOW_ANALYSIS_SOURCE_ROOT_MISMATCH")
        expected = _manifest_signal(strategy, matches[0], target=plan["trade_date"], market=binding["market"])
    elif version["source_kind"] == "immutable_v3_sleeve":
        source = _v3_source(connection, strategy, plan["trade_date"], str(binding["source_run_uid"]))
        forecast = source["forecasts"].get(str(binding.get("forecast_id") or ""))
        route = _v3_market_route(strategy, source["run"])
        if forecast is None or str(source["run"]["result_hash"]) != binding.get("result_hash") or route != binding.get("market_route") or route["multiplier"] <= 0:
            raise ValueError("SHADOW_V3_SOURCE_ROOT_MISMATCH")
        expected = _v3_signal(strategy, forecast, plan["trade_date"])
    else:
        raise ValueError("SHADOW_NATIVE_SOURCE_KIND_INVALID")
    if expected is None or expected["signal_status"] != "READY" or canonical_sha256(_projection(candidate)) != binding.get("signal_sha256") or _projection(candidate) != _projection(expected):
        raise ValueError("SHADOW_NATIVE_SIGNAL_SOURCE_MISMATCH")
