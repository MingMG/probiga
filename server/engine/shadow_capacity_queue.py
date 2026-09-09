"""Finite, persistent research capacity, independent of daily price rankings."""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Mapping

from sqlalchemy import text

from server.common.analysis_pool_receipt import canonical_sha256
from server.engine.shadow_trial_policy import SHADOW_CAPACITY_POLICY


def validate_capacity_selection(decision: Mapping[str, Any] | None, authorization: Mapping[str, Any]) -> dict:
    value = dict(decision or {})
    payload = {k: v for k, v in value.items() if k != "queue_hash"}
    selected = value.get("selected") or {}
    frozen = authorization["execution_contract"]
    policy = frozen["policy"]["capacity_policy"]
    horizon = int(frozen["maximum_holding_sessions"])
    needed = math.ceil(int(policy["required_mature_trades"]) * horizon /
                       (int(policy["rolling_window_sessions"]) * float(policy["planning_utilization"])))
    if (value.get("queue_hash") != canonical_sha256(payload) or value.get("policy") != policy
        or value.get("trade_date") != authorization["trade_date"]
        or selected.get("strategy_key") != authorization["strategy_key"]
        or selected.get("strategy_version") != authorization["strategy_version"]
        or selected.get("strategy_version_hash") != authorization["strategy_version_hash"]
        or not int(selected.get("start_session_ordinal") or 0) <= int(value.get("session_ordinal") or 0) <= int(selected.get("end_session_ordinal") or 0)
        or selected.get("end_session_ordinal", 0) - selected.get("start_session_ordinal", 0) + 1 != policy["evaluation_sessions"]
        or selected.get("start_session_ordinal", 0) < 1
        or not 0 < float(value.get("account_equity_cny") or 0)
        or not needed <= int(selected.get("initial_available_position_slots") or 0) <= int(policy["maximum_positions"])
        or float(selected.get("initial_account_equity_cny") or 0) <= 0
        or int(selected.get("initial_affordable_candidate_count") or 0) < math.ceil(policy["required_mature_trades"] / (policy["rolling_window_sessions"] * policy["planning_utilization"]))
        or needed > policy["maximum_positions"]
        or value.get("real_order_authority") is not False):
        raise ValueError("SHADOW_CAPACITY_SELECTION_INVALID")
    return value


def load_capacity_source(connection: Any, source: Mapping[str, Any], *, plan_id: str,
                         authorization: Mapping[str, Any], require_current: bool) -> dict:
    if not isinstance(source, Mapping) or set(source) != {"governance_run_uid", "governance_result_hash", "queue_hash"}:
        raise ValueError("SHADOW_CAPACITY_CANONICAL_SOURCE_MISSING")
    row = connection.execute(text("SELECT result_json,result_hash FROM st_strategy_governance_run WHERE run_uid=:run_uid AND status='COMPLETED'"),
                             {"run_uid": source["governance_run_uid"]}).mappings().one()
    body = str(row["result_json"])
    if hashlib.sha256(body.encode()).hexdigest() != row["result_hash"] or row["result_hash"] != source["governance_result_hash"]:
        raise ValueError("SHADOW_CAPACITY_CANONICAL_ROOT_INVALID")
    result = json.loads(body)
    queue = result.get("shadow_capacity_queue") or {}
    if (result.get("run_uid") != source["governance_run_uid"] or result.get("is_canonical") is not True
        or queue.get("queue_hash") != source["queue_hash"] or plan_id not in queue.get("authorized_plan_ids", [])):
        raise ValueError("SHADOW_CAPACITY_PLAN_SOURCE_MISMATCH")
    validate_capacity_selection(queue, authorization)
    if require_current:
        current = connection.execute(text("SELECT result_json,result_hash FROM st_strategy_governance_run WHERE trade_date=:trade_date AND status='COMPLETED' AND is_canonical=1"),
                                     {"trade_date": authorization["trade_date"]}).mappings().one()
        current_body = str(current["result_json"])
        if (hashlib.sha256(current_body.encode()).hexdigest() != current["result_hash"]
            or json.loads(current_body).get("shadow_capacity_queue", {}).get("queue_hash") != source["queue_hash"]):
            raise ValueError("SHADOW_CAPACITY_CURRENT_QUEUE_CHANGED")
    return dict(source)


def select_capacity_queue(*, previous: Mapping[str, Any] | None, inventories: list[dict],
                          trade_date: str, session_ordinal: int, available_slots: int,
                          equity_cny: float) -> dict:
    policy = SHADOW_CAPACITY_POLICY
    prior = dict(previous or {})
    if prior:
        payload = {k: v for k, v in prior.items() if k != "queue_hash"}
        if prior.get("queue_hash") != canonical_sha256(payload) or prior.get("policy") != policy:
            raise ValueError("SHADOW_CAPACITY_PREVIOUS_QUEUE_INVALID")
    completed = list(prior.get("completed_versions") or [])
    current_by_identity = {(row["strategy_key"], row["strategy_version"]): row for row in inventories}
    selected = dict(prior.get("selected") or {})
    end_reason = ""
    if selected:
        identity = (selected["strategy_key"], selected["strategy_version"])
        current = current_by_identity.get(identity)
        if current is None or current.get("enabled") is not True:
            end_reason = "VERSION_OR_ENABLEMENT_CHANGED"
        elif current["lifecycle"] in policy["early_exit_statuses"]:
            end_reason = "GOVERNANCE_" + current["lifecycle"]
        elif session_ordinal > selected["end_session_ordinal"]:
            end_reason = "EVALUATION_PERIOD_ENDED"
        if end_reason:
            completed.append({**selected, "ended_on": trade_date, "end_reason": end_reason})
            selected = {}
    completed_ids = {(row["strategy_key"], row["strategy_version"]) for row in completed}
    diagnostics = []
    for raw in inventories:
        row = dict(raw)
        horizon = int(row.get("maximum_holding_sessions") or 0)
        required_slots = math.ceil(policy["required_mature_trades"] * horizon /
                                   (policy["rolling_window_sessions"] * policy["planning_utilization"])) if horizon > 0 else 10**9
        row["required_position_slots"] = required_slots
        row["required_daily_affordable_candidates"] = math.ceil(policy["required_mature_trades"] /
            (policy["rolling_window_sessions"] * policy["planning_utilization"]))
        row["theoretical_60_session_completions"] = math.floor(policy["maximum_positions"] * 60 / horizon) if horizon > 0 else 0
        identity = (row["strategy_key"], row["strategy_version"])
        if row.get("enabled") is not True or row["lifecycle"] != "SHADOW":
            status = "NOT_SHADOW"
        elif identity in completed_ids:
            status = "EVALUATION_FINISHED_REQUIRES_NEW_VERSION"
        elif required_slots > policy["maximum_positions"]:
            status = "CAPACITY_INSUFFICIENT"
        elif row["affordable_candidate_count"] < row["required_daily_affordable_candidates"]:
            status = "CANDIDATE_SUPPLY_INSUFFICIENT"
        elif required_slots > available_slots:
            status = "CAPACITY_WAITING"
        else:
            status = "ELIGIBLE_FOR_EVALUATION"
        row["capacity_status"] = status
        diagnostics.append(row)
    if not selected:
        eligible = sorted((r for r in diagnostics if r["capacity_status"] == "ELIGIBLE_FOR_EVALUATION"),
                          key=lambda r: (r["maximum_holding_sessions"], r["version_created_at"], r["strategy_key"]))
        if eligible:
            chosen = eligible[0]
            selected = {
                "strategy_key": chosen["strategy_key"], "strategy_version": chosen["strategy_version"],
                "strategy_version_hash": chosen["strategy_version_hash"],
                "started_on": trade_date, "start_session_ordinal": session_ordinal,
                "end_session_ordinal": session_ordinal + policy["evaluation_sessions"] - 1,
                "initial_inventory_hash": canonical_sha256(chosen),
                "initial_available_position_slots": available_slots,
                "initial_account_equity_cny": equity_cny,
                "initial_affordable_candidate_count": chosen["affordable_candidate_count"],
            }
    for row in diagnostics:
        if (row["strategy_key"], row["strategy_version"]) == (selected.get("strategy_key"), selected.get("strategy_version")):
            row["capacity_status"] = "ACTIVE_EVALUATION"
        elif row["capacity_status"] == "ELIGIBLE_FOR_EVALUATION":
            row["capacity_status"] = "CAPACITY_WAITING"
    payload = {
        "schema": "probiga.shadow-capacity-queue.v1", "policy": policy,
        "trade_date": trade_date, "session_ordinal": session_ordinal,
        "previous_queue_hash": prior.get("queue_hash") or "", "selected": selected,
        "completed_versions": completed, "last_end_reason": end_reason,
        "available_position_slots": available_slots, "account_equity_cny": equity_cny,
        "inventories": diagnostics, "automatic_real_order_submission": False, "real_order_authority": False,
    }
    return {**payload, "queue_hash": canonical_sha256(payload)}


def capacity_queue_for_run(connection: Any, *, registry: list[dict], groups: list[dict],
                           trade_date: str, session_ordinal: int) -> dict:
    prior_row = connection.execute(text("""
        SELECT result_json,result_hash FROM st_strategy_governance_run
        WHERE trade_date<=:trade_date AND is_canonical=1 AND status='COMPLETED'
        ORDER BY trade_date DESC,run_revision DESC LIMIT 1
    """), {"trade_date": trade_date}).mappings().first()
    previous = None
    if prior_row:
        body = str(prior_row["result_json"])
        if hashlib.sha256(body.encode()).hexdigest() != prior_row["result_hash"]:
            raise ValueError("SHADOW_CAPACITY_PREVIOUS_RUN_HASH_INVALID")
        previous = json.loads(body).get("shadow_capacity_queue")
        # One trading date has one frozen capacity decision. A revised market
        # run may refresh alpha facts; it cannot restart or reassign the cohort.
        if previous and previous.get("trade_date") == trade_date:
            payload = {k: v for k, v in previous.items() if k != "queue_hash"}
            if previous.get("queue_hash") != canonical_sha256(payload) or previous.get("policy") != SHADOW_CAPACITY_POLICY:
                raise ValueError("SHADOW_CAPACITY_PREVIOUS_QUEUE_INVALID")
            return previous
    from server.trading_v3.config import load_v3_config
    config = load_v3_config()
    equity = float(connection.execute(text("""
        SELECT total_equity FROM st_equity_daily_v2
        WHERE account_id='paper-main-v2' AND trade_date=:trade_date
    """), {"trade_date": trade_date}).scalar_one())
    if not math.isfinite(equity) or equity <= 0:
        raise ValueError("SHADOW_CAPACITY_EQUITY_INVALID")
    occupied = int(connection.execute(text("""
        SELECT COUNT(DISTINCT stock_code) FROM (
            SELECT stock_code FROM st_position_lot_v2 WHERE account_id='paper-main-v2' AND remaining_quantity>0
            UNION SELECT stock_code FROM st_order_v2 WHERE account_id='paper-main-v2'
              AND side='BUY' AND status IN ('CREATED','RISK_APPROVED','QUEUED','PARTIALLY_FILLED')
        ) occupied
    """)).scalar_one())
    group_by_key = {group["strategy_key"]: group for group in groups}
    reference_prices = {str(row["stock_code"]): float(row["close"]) for row in connection.execute(text("""
        SELECT stock_code,close FROM sm_stock_kline
        WHERE trade_date=:trade_date AND k_type=1 AND adjust_type=0 AND close>0
    """), {"trade_date": trade_date}).mappings()}
    inventories = []
    for strategy in registry:
        key = strategy["strategy_key"]
        group = group_by_key.get(key, {})
        candidates = group.get("candidate_facts") or []
        affordable = []
        for row in candidates:
            price = float(row.get("entry_high") or row.get("entry_low") or reference_prices.get(str(row["stock_code"]), 0))
            if not math.isfinite(price) or price <= 0:
                continue
            gross = 100 * price * (1 + float(config["paper_execution"]["worst_price_premium_pct"]) / 100)
            fee = max(float(config["account"]["minimum_commission_cny"]), gross * float(config["account"]["commission_rate"])) + gross * float(config["account"]["transfer_fee_rate"])
            if gross + fee <= equity * 0.01:
                affordable.append(str(row["stock_code"]))
        params = strategy.get("parameters") or {}
        inventories.append({
            "strategy_key": key, "strategy_version": strategy["current_version"],
            "strategy_version_hash": strategy["version_hash"], "lifecycle": strategy["current_status"],
            "enabled": strategy.get("enabled") is True,
            "version_created_at": str(strategy.get("version_created_at") or strategy.get("created_at") or ""),
            "maximum_holding_sessions": int(params.get("max_holding_days", params.get("horizon_days", 0))),
            "candidate_count": len(candidates), "affordable_candidate_count": len(set(affordable)),
            "affordable_candidate_codes_hash": canonical_sha256(sorted(set(affordable))),
            "maximum_order_cny": round(equity * 0.01, 2), "minimum_board_lot": 100,
        })
    return select_capacity_queue(previous=previous, inventories=inventories, trade_date=trade_date,
        session_ordinal=session_ordinal, available_slots=max(0, min(12, int(config["paper_execution"]["maximum_live_positions"]))-occupied), equity_cny=equity)
