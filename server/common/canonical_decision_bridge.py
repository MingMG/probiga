"""Read-only bridge from canonical strategy governance to trading decision UI.

The strategy governance batch is the production strategy truth.  Trading V3
remains an execution/research ledger, so a day can have a completed canonical
governance result without a V3 decision row.  This module projects only an
already persisted, hash-validated canonical result; it never recomputes a
strategy or grants order authority.
"""
from __future__ import annotations

from datetime import date
from threading import Lock
from time import monotonic
from typing import Any

from server.engine.strategy_governance import (
    load_canonical_governance_snapshot,
)
from server.trading_v3.candidate_dynamics import enrich_candidate_dynamics


_SNAPSHOT_CACHE_SECONDS = 30.0
_SNAPSHOT_CACHE: dict[
    tuple[int, str], tuple[float, dict[str, Any] | None]
] = {}
_SNAPSHOT_CACHE_LOCK = Lock()


def _day(value: Any) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return str(value or "").strip()[:10]


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_snapshot(requested: str) -> dict[str, Any] | None:
    key = (id(load_canonical_governance_snapshot), requested)
    now = monotonic()
    with _SNAPSHOT_CACHE_LOCK:
        cached = _SNAPSHOT_CACHE.get(key)
        if cached and now - cached[0] < _SNAPSHOT_CACHE_SECONDS:
            return cached[1]
        try:
            snapshot = load_canonical_governance_snapshot(
                trade_date=requested
            )
        except Exception:
            snapshot = None
        snapshot = snapshot if isinstance(snapshot, dict) else None
        _SNAPSHOT_CACHE[key] = (monotonic(), snapshot)
        return snapshot


def _canonical_snapshot(trade_date: date | str | None) -> dict[str, Any] | None:
    requested = _day(trade_date)
    snapshot = _load_snapshot(requested)
    if not isinstance(snapshot, dict):
        return None
    result_day = _day(snapshot.get("trade_date"))
    plan = snapshot.get("paper_execution_plan")
    targets = plan.get("targets") if isinstance(plan, dict) else None
    exits = plan.get("exit_targets") if isinstance(plan, dict) else None
    checks = (
        snapshot.get("status") == "ok",
        snapshot.get("is_canonical") is True,
        snapshot.get("result_mode") == "CANONICAL_PERSISTED",
        snapshot.get("input_ready") is True,
        len(str(snapshot.get("run_uid") or "")) == 32,
        len(str(snapshot.get("canonical_result_hash") or "")) == 64,
        bool(result_day),
        not requested or result_day == requested,
        snapshot.get("automatic_real_order_submission") is False,
        snapshot.get("real_order_authority") is False,
        isinstance(plan, dict),
        isinstance(targets, list),
        isinstance(exits, list),
        plan.get("schema") == "probiga.governance-paper-execution-plan.v1",
        _day(plan.get("trade_date")) == result_day,
        str(plan.get("plan_hash") or "")
        == str(snapshot.get("paper_execution_plan_hash") or ""),
        plan.get("automatic_real_order_submission") is False,
        plan.get("real_order_authority") is False,
        int(plan.get("target_count") or 0) == len(targets or []),
    )
    if not all(checks):
        return None
    latest = snapshot if not requested else _load_snapshot("")
    is_latest = bool(
        isinstance(latest, dict)
        and str(latest.get("run_uid") or "")
        == str(snapshot.get("run_uid") or "")
    )
    return {**snapshot, "_bridge_is_latest": is_latest}


def _target_rows(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    plan = dict(snapshot.get("paper_execution_plan") or {})
    result: list[dict[str, Any]] = []
    for index, raw in enumerate(plan.get("targets") or [], 1):
        if not isinstance(raw, dict):
            continue
        target_bp = int(raw.get("target_bp") or 0)
        reference_capital = _number(raw.get("reference_capital_cny"))
        strategy_key = str(raw.get("strategy_key") or "")
        result.append({
            **raw,
            "run_uid": snapshot.get("run_uid"),
            "trade_date": snapshot.get("trade_date"),
            "rank_no": index,
            "short_name": raw.get("stock_name") or raw.get("stock_code"),
            "target_weight": target_bp / 10_000.0,
            "target_value": reference_capital * target_bp / 10_000.0,
            "target_quantity": int(
                raw.get("reference_board_lot_quantity") or 0
            ),
            "primary_strategy_key": strategy_key,
            "strategy_keys": [strategy_key] if strategy_key else [],
            "theme_codes": [
                str(raw.get("industry_name"))
            ] if raw.get("industry_name") else [],
            "reason": "规范治理批次资金计划（只读，订单权限关闭）",
            "decision_scope": "CANONICAL_GOVERNANCE",
            "new_buy_eligible": False,
            "display_action": "WATCH",
        })
    return result


def _pool_items(
    snapshot: dict[str, Any], targets: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    target_by_code = {
        str(row.get("stock_code") or "").zfill(6): row for row in targets
    }
    merged: dict[str, dict[str, Any]] = {}
    pools = snapshot.get("pools") or {}
    for level in ("tradable", "confirmation", "observation"):
        rows = pools.get(level) if isinstance(pools, dict) else []
        for raw in rows or []:
            if not isinstance(raw, dict):
                continue
            code = str(raw.get("stock_code") or "").strip().zfill(6)
            if not code.strip("0"):
                continue
            item = merged.setdefault(code, {
                "stock_code": code,
                "stock_name": raw.get("stock_name") or code,
                "rank_no": raw.get("rank"),
                "strategy_keys": [],
                "theme_codes": [],
                "forecast_statuses": [],
                "raw_score": raw.get("opportunity_score"),
                "expected_return_net_pct": None,
                "probability_positive": None,
                "confidence": None,
                "valid_until": None,
                "reasons": [],
                "sample_count": 0,
                "profit_factor": None,
                "payoff_ratio": None,
                "is_strategy_candidate": True,
                "governance_pool_levels": [],
                "target": target_by_code.get(code),
                "rejection": None,
            })
            if level not in item["governance_pool_levels"]:
                item["governance_pool_levels"].append(level)
            strategy_values = raw.get("strategies") or [
                raw.get("dominant_strategy")
            ]
            for value in strategy_values:
                normalized = str(value or "").strip()
                if normalized and normalized not in item["strategy_keys"]:
                    item["strategy_keys"].append(normalized)
            industry = str(raw.get("industry_name") or "").strip()
            if industry and industry not in item["theme_codes"]:
                item["theme_codes"].append(industry)
            for value in (
                raw.get("reason"), raw.get("pool_reason"),
                raw.get("blocking_reason"),
            ):
                normalized = str(value or "").strip()
                if normalized and normalized not in item["reasons"]:
                    item["reasons"].append(normalized)
            if raw.get("opportunity_score") is not None:
                item["raw_score"] = max(
                    _number(item.get("raw_score")),
                    _number(raw.get("opportunity_score")),
                )

    for code, target in target_by_code.items():
        item = merged.setdefault(code, {
            "stock_code": code,
            "stock_name": target.get("short_name") or code,
            "rank_no": target.get("rank_no"),
            "strategy_keys": list(target.get("strategy_keys") or []),
            "theme_codes": list(target.get("theme_codes") or []),
            "forecast_statuses": [],
            "raw_score": target.get("opportunity_score"),
            "expected_return_net_pct": None,
            "probability_positive": None,
            "confidence": None,
            "valid_until": None,
            "reasons": [],
            "sample_count": 0,
            "profit_factor": None,
            "payoff_ratio": target.get("planned_risk_reward_ratio"),
            "is_strategy_candidate": True,
            "governance_pool_levels": ["tradable"],
            "target": target,
            "rejection": None,
        })
        item["target"] = target

    items = list(merged.values())
    for index, item in enumerate(items, 1):
        target = item.get("target") or {}
        has_target = bool(target)
        item["rank_no"] = int(item.get("rank_no") or index)
        item["actionability"] = "PAPER_ONLY" if has_target else "RESEARCH_ONLY"
        item["forecast_statuses"] = [
            "RESEARCH_TARGET" if has_target else "RESEARCH_SAMPLE"
        ]
        item["action_plan"] = {
            "actionability": item["actionability"],
            "label": (
                "规范治理模拟目标；成交前仍需独立复验"
                if has_target else "规范治理观察候选；只读研究"
            ),
            "buy_range": (
                {
                    "low": target.get("reference_price"),
                    "high": target.get("reference_price"),
                }
                if target.get("reference_price") is not None else None
            ),
            "sell_range": (
                {
                    "low": target.get("take_profit_1"),
                    "high": target.get("take_profit_2"),
                }
                if target.get("take_profit_1") is not None
                or target.get("take_profit_2") is not None else None
            ),
            "protective_stop": target.get("stop_loss_price"),
            "execution_authority": "NONE",
        }
        item["reasons"] = item["reasons"][:8] or [
            "来自已验证的规范治理批次"
        ]
    items.sort(key=lambda row: (int(row.get("rank_no") or 999999), row["stock_code"]))
    return items


def _strategy_execution_projection(
    snapshot: dict[str, Any],
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for item in items:
        for strategy_key in item.get("strategy_keys") or []:
            key = str(strategy_key or "").strip()
            if key:
                counts[key] = counts.get(key, 0) + 1
    rows = []
    for raw in snapshot.get("strategies") or []:
        if not isinstance(raw, dict):
            continue
        key = str(raw.get("strategy_key") or "").strip()
        if not key:
            continue
        lifecycle = str(raw.get("current_status") or "UNKNOWN")
        candidate_count = counts.get(key, 0)
        rows.append({
            "strategy_key": key,
            "strategy_name": str(raw.get("strategy_name") or key),
            "lifecycle_status": lifecycle,
            "candidate_count": candidate_count,
            "forecast_count": candidate_count,
            "status": (
                "SKIPPED_LIFECYCLE"
                if lifecycle in {"SUSPENDED", "RETIRED"}
                else "COMPLETED_WITH_CANDIDATES"
                if candidate_count
                else "COMPLETED_NO_CANDIDATE"
            ),
            "ranking_score": raw.get("ranking_score"),
            "today_reason": str(
                raw.get("reason")
                or raw.get("status_reason")
                or "当日治理批次已完成"
            ),
        })
    rows.sort(key=lambda row: row["strategy_key"])
    return {
        "strategy_count": len(rows),
        "completed_count": sum(
            row["status"].startswith("COMPLETED") for row in rows
        ),
        "blocked_count": 0,
        "candidate_strategy_count": sum(
            row["candidate_count"] > 0 for row in rows
        ),
        "strategies": rows,
    }


def canonical_governance_decision(
    trade_date: date | str | None = None,
    *,
    latest_as_of: bool = False,
) -> dict[str, Any] | None:
    """Return V3-shaped, read-only projections for one canonical batch."""

    requested_day = _day(trade_date)
    snapshot = _canonical_snapshot(trade_date)
    is_as_of_fallback = False
    if snapshot is None and latest_as_of and requested_day:
        latest = _canonical_snapshot(None)
        latest_day = _day((latest or {}).get("trade_date"))
        if latest is not None and latest_day and latest_day <= requested_day:
            snapshot = latest
            is_as_of_fallback = latest_day != requested_day
    if snapshot is None:
        return None
    result_day = _day(snapshot.get("trade_date"))
    requested_session = requested_day or result_day
    plan = dict(snapshot.get("paper_execution_plan") or {})
    targets = _target_rows(snapshot)
    pool_items = _pool_items(snapshot, targets)
    pool_items, daily_change = enrich_candidate_dynamics(pool_items)
    strategy_execution = _strategy_execution_projection(snapshot, pool_items)
    summary = dict(snapshot.get("summary") or {})
    gate = dict(snapshot.get("trading_gate") or {})
    decision_at = (
        snapshot.get("finished_at")
        or snapshot.get("completed_at")
        or snapshot.get("created_at")
    )
    raw_market_state = gate.get("market_state") or snapshot.get("market_state")
    if isinstance(raw_market_state, dict):
        raw_market_state = raw_market_state.get("key")
    market_state = str(raw_market_state or "unknown")
    risk_cap = _number(
        gate.get("market_risk_cap_pct", summary.get("market_risk_cap_pct"))
    ) / 100.0
    target_count = len(targets)
    decision_status = "CANDIDATE_AVAILABLE" if pool_items else "EMPTY"
    run_uid = str(snapshot.get("run_uid") or "")
    result_hash = str(snapshot.get("canonical_result_hash") or "")
    build_commit_sha = str(snapshot.get("build_commit_sha") or "").lower()
    context = {
        "requested_date": requested_session,
        "decision_date": result_day,
        "decision_session_date": result_day,
        "data_date": result_day,
        "expected_data_date": result_day,
        "context_mode": (
            "CANONICAL_GOVERNANCE_LATEST_AS_OF"
            if is_as_of_fallback else "CANONICAL_GOVERNANCE"
        ),
        "context_date_matches": True,
        "run_uid": run_uid,
        "decision_at": decision_at,
        "knowledge_cutoff_at": decision_at,
        "evidence_as_of": decision_at,
        "valid_until": None,
        "run_status": "COMPLETED",
        "data_status": "READY",
        "decision_status": decision_status,
        "decision_scope": "CANONICAL_GOVERNANCE",
        "ranking_authority": "STRATEGY_GOVERNANCE_CANONICAL",
        "execution_authority": "NONE",
        "paper_order_authority": "NONE",
        "order_authority": False,
        "real_order_authority": "DISABLED",
        "real_order_allowed": False,
        "actionable_output_allowed": False,
        "actionable_status": "READ_ONLY_CANONICAL_PLAN",
        "decision_integrity_verified": True,
        "decision_integrity_reason": "",
        "snapshot_manifest_hash": result_hash,
        "historical_read_only": snapshot.get("_bridge_is_latest") is not True,
        "target_count": target_count,
        "reason_codes": [
            "CANONICAL_GOVERNANCE_BRIDGE",
            *(
                ["LATEST_COMPLETED_DECISION_AS_OF"]
                if is_as_of_fallback else []
            ),
        ],
        "source_system": "STRATEGY_GOVERNANCE",
        "canonical_result_hash": result_hash,
        "build_commit_sha": build_commit_sha,
        "is_as_of_fallback": is_as_of_fallback,
        "decision_data_date": result_day,
    }
    portfolio = {
        "targets": targets,
        "rejected": [],
        "target_risk_asset_weight": int(plan.get("invested_bp") or 0)
        / 10_000.0,
        "target_cash": _number(plan.get("cash_bp")) / 10_000.0
        * _number((plan.get("policy") or {}).get("reference_capital_cny"), 1_000_000.0),
        "status": "CANONICAL_GOVERNANCE",
    }
    run = {
        "run_uid": run_uid,
        "trade_date": result_day,
        "requested_as_of": requested_session,
        "decision_date": result_day,
        "decision_session_date": result_day,
        "decision_at": decision_at,
        "completed_at": decision_at,
        "status": "COMPLETED",
        "mode": "CANONICAL_GOVERNANCE",
        "dominant_regime": market_state,
        "regime": {
            "dominant_state": market_state,
            "quality_status": "PASS",
            "risk_asset_cap": risk_cap,
        },
        "risk_asset_cap": risk_cap,
        "target_count": target_count,
        "validated_count": int(summary.get("tradable_count") or 0),
        "forecast_count": len(pool_items),
        "lifecycle_status": "RESEARCH_ONLY",
        "decision_scope": "CANONICAL_GOVERNANCE",
        "decision_integrity_verified": True,
        "decision_integrity_reason": "",
        "result_hash": result_hash,
        "portfolio": portfolio,
        "source_system": "STRATEGY_GOVERNANCE",
        "build_commit_sha": build_commit_sha,
        "is_as_of_fallback": is_as_of_fallback,
    }
    pool_summary = {
        "stock_count": len(pool_items),
        "forecast_count": len(pool_items),
        "strategy_candidate_count": len(pool_items),
        "target_count": target_count,
        "rejected_count": 0,
        "display_count": len(pool_items),
        "buy_zone_count": 0,
        "wait_trigger_count": 0,
        "paper_only_count": target_count,
        "visible_wait_limit": 20,
        "daily_new_count": int(daily_change.get("new_count") or 0),
        "daily_retained_count": int(
            daily_change.get("retained_count") or 0
        ),
        "daily_removed_count": int(daily_change.get("removed_count") or 0),
        "executed_strategy_count": int(
            strategy_execution.get("strategy_count") or 0
        ),
    }
    pool = {
        "run_uid": run_uid,
        "trade_date": result_day,
        "data_date": result_day,
        "decision_date": result_day,
        "decision_session_date": result_day,
        "requested_trade_date": requested_session,
        "before_session_date": None,
        "is_historical_fallback": False,
        "historical_read_only": snapshot.get("_bridge_is_latest") is not True,
        "historical_fallback_status": None,
        "decision_at": decision_at,
        "generated_at": decision_at,
        "pool_status": "READY" if pool_items else "EMPTY",
        "pool_readable": True,
        "run_status": "COMPLETED",
        "decision_integrity_verified": True,
        "reason_codes": ["CANONICAL_GOVERNANCE_BRIDGE"],
        "items": pool_items,
        "summary": pool_summary,
        "daily_change": {
            **daily_change,
            "status": "NO_PREVIOUS_CANONICAL_BATCH",
            "previous_run_uid": None,
            "previous_session_date": None,
        },
        "strategy_execution": strategy_execution,
        "decision_scope": "CANONICAL_GOVERNANCE",
        "actionable_output_allowed": False,
        "source_system": "STRATEGY_GOVERNANCE",
        "canonical_result_hash": result_hash,
        "build_commit_sha": build_commit_sha,
        "is_as_of_fallback": is_as_of_fallback,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    lineage = {
        "run": {
            "run_uid": run_uid,
            "trade_date": result_day,
            "decision_at": decision_at,
            "status": "COMPLETED",
            "dominant_regime": market_state,
            "target_count": target_count,
            "result_hash": result_hash,
        },
        "targets": targets,
        "intents": [],
        "orders": [],
        "fills": [],
        "lots": [],
        "summary": {
            "target_count": target_count,
            "intent_count": 0,
            "order_count": 0,
            "fill_count": 0,
            "lot_count": 0,
            "lot_close_evidence_status": "NO_SELL_FILL",
        },
        "execution_ledger_status": "NO_V3_ORDER_AUTHORITY",
        "source_system": "STRATEGY_GOVERNANCE",
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    return {
        "snapshot": snapshot,
        "context": context,
        "run": run,
        "pool": pool,
        "targets": targets,
        "lineage": lineage,
    }


def canonical_governance_decision_for_run(
    run_uid: str,
) -> dict[str, Any] | None:
    """Resolve the latest canonical bridge only when its identity matches."""

    projected = canonical_governance_decision()
    if projected is None:
        return None
    return projected if str(projected["context"].get("run_uid")) == str(run_uid) else None
