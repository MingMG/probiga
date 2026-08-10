"""Explicit V2 decision worker; API GET handlers never call this module."""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, time
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from server.engine.strategy_center import (
    build_strategy_center_snapshot,
    load_qmt_kline_attestation_status,
)

from .bootstrap import ACCOUNT_ID
from .config import canonical_json_hash
from .policy import load_portfolio_policy
from .market_regime import classify_market_regime
from .multi_strategy_router import evaluate_signal_route
from .planner import persist_portfolio_competition
from .domain import decimal_value
from .position_monitor import monitor_positions
from .sector_preheat import (
    build_sector_preheat_snapshot,
    merge_sector_preheat_candidates,
)
from .versioning import code_version


def _json(payload: Any) -> str:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _explicit_gate_true(value: Any) -> bool:
    return value is True or (type(value) is int and value == 1)


def _candidate_has_exit(candidate: dict[str, Any]) -> bool:
    if str(candidate.get("final_direction") or "").upper() in {
        "SELL",
        "REDUCE",
        "EXIT",
    }:
        return True
    if str(candidate.get("final_status") or "").upper() in {
        "SELL_ALERT",
        "REDUCE",
        "EXIT",
    }:
        return True
    return any(
        str(item.get("signal_direction") or "").upper()
        in {"SELL", "REDUCE", "EXIT"}
        or str(item.get("signal_status") or "").upper()
        in {"SELL_ALERT", "REDUCE", "EXIT"}
        for item in candidate.get("strategy_signals") or []
    )


def _canonical_new_buy_rejection(
    signal: dict[str, Any],
    *,
    candidate_has_exit: bool = False,
) -> str | None:
    requested_direction = str(
        signal.get("requested_signal_direction")
        or signal.get("signal_direction")
        or "HOLD"
    ).upper()
    if requested_direction not in {"BUY"}:
        return None
    if candidate_has_exit:
        return "CONFLICTING_EXIT_SIGNAL"
    explicit = str(
        signal.get("canonical_new_buy_rejection_code") or ""
    ).upper()
    if explicit:
        return explicit
    recommend_status = str(
        signal.get("source_recommend_status")
        or signal.get("recommend_status")
        or "DATA_BLOCKED"
    ).upper()
    if recommend_status != "ALLOW":
        return "CANONICAL_RECOMMEND_GATE_NOT_ALLOWED"
    source_signal_status = str(
        signal.get("source_signal_status") or "WATCH"
    ).upper()
    if source_signal_status not in {"CONFIRM", "BUY_READY"}:
        return "CANONICAL_SIGNAL_NOT_CONFIRMED"
    chase_status = str(
        signal.get("source_chase_risk_status")
        or signal.get("chase_risk_status")
        or "DATA_BLOCKED"
    ).upper()
    ordinary_buy_eligible = _explicit_gate_true(
        signal.get(
            "ordinary_buy_eligible",
            signal.get("source_ordinary_buy_eligible"),
        )
    )
    if chase_status != "ALLOW" or not ordinary_buy_eligible:
        return "CANONICAL_CHASE_GATE_NOT_ALLOWED"
    return None


def _account_payload(connection) -> dict[str, Any]:
    row = connection.execute(
        text(
            """
            SELECT account_id, status, initial_cash, cash_balance, peak_equity,
                   policy_version, policy_hash, fee_profile_version,
                   instrument_rule_version, real_trading_enabled
            FROM st_trade_account_v2 WHERE account_id = :account_id
            """
        ),
        {"account_id": ACCOUNT_ID},
    ).mappings().first()
    if not row:
        raise RuntimeError("V2 account is not initialized")
    return dict(row)


def _previous_market_regime(
    engine: Engine,
    *,
    trade_date: date,
) -> dict[str, Any]:
    """Load one prior trade-day state; same-day reruns never advance it."""
    try:
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT market_regime,
                           market_regime_candidate,
                           market_regime_candidate_streak,
                           market_regime_state_days,
                           market_regime_cooldown_remaining
                    FROM st_decision_run_v2
                    WHERE trade_date < :trade_date
                      AND status = 'COMPLETED'
                    ORDER BY trade_date DESC, decision_at DESC,
                             started_at DESC
                    LIMIT 1
                    """
                ),
                {"trade_date": trade_date},
            ).mappings().first()
    except Exception:
        row = None
    if not row:
        return {
            "previous_state": "",
            "previous_state_days": 0,
            "previous_candidate_state": "",
            "previous_candidate_streak": 0,
            "extreme_cooldown_remaining": 0,
        }
    return {
        "previous_state": str(row.get("market_regime") or ""),
        "previous_state_days": int(
            row.get("market_regime_state_days") or 1
        ),
        "previous_candidate_state": str(
            row.get("market_regime_candidate")
            or row.get("market_regime")
            or ""
        ),
        "previous_candidate_streak": int(
            row.get("market_regime_candidate_streak") or 1
        ),
        "extreme_cooldown_remaining": int(
            row.get("market_regime_cooldown_remaining") or 0
        ),
    }


def _data_quality(
    snapshot: dict[str, Any],
    qmt_attestation: dict[str, Any],
    code_commit_sha: str,
    target_date: date,
    sector_snapshot: dict[str, Any] | None = None,
) -> tuple[str, list[str]]:
    blocks: list[str] = []
    candidate_source_ready = snapshot.get("source_status") in {
        "fresh",
        "reference_verified",
    } or (sector_snapshot or {}).get("source_status") == "fresh"
    if not candidate_source_ready:
        blocks.append("CANDIDATE_SOURCE_NOT_FRESH")
    completed_for_target = None
    for run in qmt_attestation.get("runs") or []:
        try:
            starts_before = (
                date.fromisoformat(str(run.get("start_date"))[:10])
                <= target_date
            )
            ends_after = (
                date.fromisoformat(str(run.get("end_date"))[:10])
                >= target_date
            )
        except (TypeError, ValueError):
            continue
        if (
            str(run.get("status") or "") == "COMPLETED"
            and starts_before
            and ends_after
        ):
            completed_for_target = run
            break
    if not completed_for_target:
        blocks.append("QMT_DAILY_KLINE_NOT_ATTESTED")
    elif float(completed_for_target.get("coverage_pct") or 0) < 100:
        blocks.append("QMT_DAILY_KLINE_ATTESTATION_INCOMPLETE")
    return ("PASS" if not blocks else "BLOCK"), blocks


def run_daily_decision(
    engine: Engine,
    *,
    trade_date: str,
    decision_at: datetime | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    target_date = date.fromisoformat(str(trade_date)[:10])
    decision_at = decision_at or datetime.combine(target_date, time(15, 20))
    policy = load_portfolio_policy()
    code_sha, code_version_kind = code_version()

    # Heavy computation occurs here in the worker process, never in API GET.
    legacy_snapshot = build_strategy_center_snapshot(target_date.isoformat(), limit)
    state = legacy_snapshot.get("market_state") or {}
    state_inputs = state.get("input") or {
        key: state.get(key)
        for key in (
            "risk_score",
            "market_change_pct",
            "breadth_pct",
            "trend_score",
            "switch_score",
        )
    }
    previous_regime = _previous_market_regime(
        engine,
        trade_date=target_date,
    )
    regime_decision = classify_market_regime(
        state_inputs,
        **previous_regime,
    )
    regime = regime_decision.final_state
    regime_candidate_streak = (
        previous_regime["previous_candidate_streak"] + 1
        if regime_decision.candidate_state
        == previous_regime["previous_candidate_state"]
        else 1
    )
    regime_state_days = (
        previous_regime["previous_state_days"] + 1
        if regime_decision.final_state == previous_regime["previous_state"]
        else 1
    )
    sector_snapshot = build_sector_preheat_snapshot(
        trade_date=target_date.isoformat(),
        decision_at=decision_at,
        market_regime=regime,
        context_engine=engine,
    )
    legacy_snapshot = merge_sector_preheat_candidates(
        legacy_snapshot,
        sector_snapshot,
    )
    qmt_attestation = load_qmt_kline_attestation_status(limit=30)
    quality_status, blocked_capabilities = _data_quality(
        legacy_snapshot,
        qmt_attestation,
        code_sha,
        target_date,
        sector_snapshot,
    )
    source_manifest = {
        "decision_at": decision_at.isoformat(sep=" ", timespec="seconds"),
        "available_at_rule": "available_at <= decision_at",
        "candidate_source": {
            "source_status": legacy_snapshot.get("source_status"),
            "data_date": legacy_snapshot.get("data_date"),
            "configuration": legacy_snapshot.get("configuration"),
            "candidate_count": len(legacy_snapshot.get("candidates") or []),
        },
        "sector_preheat": {
            "strategy_version": sector_snapshot.get("strategy_version"),
            "config_hash": sector_snapshot.get("config_hash"),
            "snapshot_hash": sector_snapshot.get("snapshot_hash"),
            "source_status": sector_snapshot.get("source_status"),
            "industry_snapshot_date": sector_snapshot.get(
                "industry_snapshot_date"
            ),
            "concept_snapshot_date": sector_snapshot.get(
                "concept_snapshot_date"
            ),
            "sector_count": sector_snapshot.get("sector_count"),
            "hot_sector_count": sector_snapshot.get("hot_sector_count"),
            "discovery_hot_sector_count": sector_snapshot.get(
                "discovery_hot_sector_count"
            ),
            "execution_candidate_count": sector_snapshot.get(
                "execution_candidate_count"
            ),
            "discovery_candidate_count": sector_snapshot.get(
                "discovery_candidate_count"
            ),
            "candidate_count": sector_snapshot.get("candidate_count"),
            "ready_count": sector_snapshot.get("ready_count"),
            "available_at_rule": sector_snapshot.get("available_at_rule"),
            "context_hash": sector_snapshot.get("context_hash"),
            "context_applied_count": sector_snapshot.get(
                "context_applied_count"
            ),
            "context_sources": sector_snapshot.get("context_sources") or {},
            "error": sector_snapshot.get("error"),
        },
        "market_regime_transition": {
            "candidate_state": regime_decision.candidate_state,
            "final_state": regime_decision.final_state,
            "candidate_streak": regime_candidate_streak,
            "state_days": regime_state_days,
            "cooldown_remaining": regime_decision.cooldown_remaining,
            "previous": previous_regime,
            "evidence": list(regime_decision.evidence),
        },
        "qmt_attestation": qmt_attestation,
        "quality_status": quality_status,
        "blocked_capabilities": blocked_capabilities,
        "code_version_kind": code_version_kind,
    }
    snapshot_hash = canonical_json_hash(source_manifest)
    snapshot_id = snapshot_hash[:32]
    if quality_status != "PASS":
        regime = "DATA_BLOCKED"

    with engine.begin() as connection:
        account = _account_payload(connection)
        account_hash = canonical_json_hash(account)
        run_inputs = {
            "code_commit_sha": code_sha,
            "strategy_version": legacy_snapshot.get("configuration", {}).get(
                "stock_manifest_version"
            ),
            "portfolio_policy_version": policy.version,
            "data_snapshot_hash": snapshot_hash,
            "instrument_rule_version": account.get("instrument_rule_version"),
            "fee_profile_version": account.get("fee_profile_version"),
            "account_state_hash": account_hash,
        }
        idempotency_key = canonical_json_hash(
            {
                "trade_date": target_date.isoformat(),
                "account_id": ACCOUNT_ID,
                **run_inputs,
            }
        )
        existing = connection.execute(
            text(
                """
                SELECT run_uid, status, result_hash FROM st_decision_run_v2
                WHERE run_idempotency_key = :key
                """
            ),
            {"key": idempotency_key},
        ).mappings().first()
        if existing:
            return {
                "status": "idempotent_hit",
                "run_uid": existing["run_uid"],
                "run_status": existing["status"],
                "result_hash": existing["result_hash"],
                "data_snapshot_hash": snapshot_hash,
            }
        existing_snapshot = connection.execute(
            text(
                """
                SELECT snapshot_id FROM st_data_snapshot_v2
                WHERE data_snapshot_hash = :hash
                """
            ),
            {"hash": snapshot_hash},
        ).mappings().first()
        if existing_snapshot:
            snapshot_id = str(existing_snapshot["snapshot_id"])
        else:
            connection.execute(
                text(
                    """
                    INSERT INTO st_data_snapshot_v2
                    (snapshot_id, trade_date, decision_at, source_manifest_json,
                     data_snapshot_hash, quality_status,
                     blocked_capabilities_json, code_commit_sha, created_at)
                    VALUES
                    (:snapshot_id, :trade_date, :decision_at, :manifest,
                     :snapshot_hash, :quality_status, :blocked, :code_sha, :now)
                    """
                ),
                {
                    "snapshot_id": snapshot_id,
                    "trade_date": target_date,
                    "decision_at": decision_at,
                    "manifest": _json(source_manifest),
                    "snapshot_hash": snapshot_hash,
                    "quality_status": quality_status,
                    "blocked": _json(blocked_capabilities),
                    "code_sha": code_sha,
                    "now": datetime.now(),
                },
            )
        run_uid = idempotency_key[:32]
        now = datetime.now()
        run_status = "BLOCKED" if quality_status != "PASS" else "COMPLETED"
        connection.execute(
            text(
                """
                INSERT INTO st_decision_run_v2
                (run_uid, run_idempotency_key, trade_date, decision_at,
                 account_id, snapshot_id, market_regime, market_regime_version,
                 market_regime_candidate,
                 market_regime_candidate_streak,
                 market_regime_state_days,
                 market_regime_cooldown_remaining,
                 market_regime_evidence_json,
                 portfolio_policy_version, config_version, code_commit_sha,
                 account_state_hash, random_seed, status, started_at)
                VALUES
                (:run_uid, :idempotency_key, :trade_date, :decision_at,
                 :account_id, :snapshot_id, :market_regime,
                 :market_regime_version, :market_regime_candidate,
                 :market_regime_candidate_streak,
                 :market_regime_state_days,
                 :market_regime_cooldown_remaining,
                 :market_regime_evidence_json,
                 :policy_version, :config_version,
                 :code_sha, :account_hash, 20260725, :status, :started_at)
                """
            ),
            {
                "run_uid": run_uid,
                "idempotency_key": idempotency_key,
                "trade_date": target_date,
                "decision_at": decision_at,
                "account_id": ACCOUNT_ID,
                "snapshot_id": snapshot_id,
                "market_regime": regime,
                "market_regime_version": regime_decision.config_version,
                "market_regime_candidate": (
                    regime_decision.candidate_state
                ),
                "market_regime_candidate_streak": (
                    regime_candidate_streak
                ),
                "market_regime_state_days": regime_state_days,
                "market_regime_cooldown_remaining": (
                    regime_decision.cooldown_remaining
                ),
                "market_regime_evidence_json": _json(
                    list(regime_decision.evidence)
                ),
                "policy_version": policy.version,
                "config_version": legacy_snapshot.get("configuration", {}).get(
                    "stock_manifest_version", ""
                ),
                "code_sha": code_sha,
                "account_hash": account_hash,
                "status": run_status,
                "started_at": now,
            },
        )

        rejected: list[dict[str, Any]] = []
        eligible_for_competition: list[dict[str, Any]] = []
        signal_rows = 0
        registry_rows = connection.execute(
            text(
                """
                SELECT version, lifecycle_status, validation_json
                FROM st_strategy_version_v2
                """
            )
        ).mappings().all()
        registry = {
            str(row["version"]): {
                "lifecycle_status": str(row["lifecycle_status"]),
                "validation": json.loads(
                    str(row["validation_json"] or "{}")
                ),
            }
            for row in registry_rows
        }
        execution_trade_date = connection.execute(
            text(
                """
                SELECT MIN(trade_date)
                FROM si_trade_calendar
                WHERE trade_status = 1
                  AND trade_date > :trade_date
                """
            ),
            {"trade_date": target_date},
        ).scalar()
        if execution_trade_date is None:
            raise RuntimeError(
                "trade calendar has no execution day for signal"
            )
        if not isinstance(execution_trade_date, date):
            execution_trade_date = date.fromisoformat(
                str(execution_trade_date)[:10]
            )
        signal_valid_until = datetime.combine(
            execution_trade_date,
            time(15, 0),
        )
        for candidate in sorted(
            legacy_snapshot.get("candidates") or [],
            key=lambda item: str(item.get("stock_code") or ""),
        ):
            candidate_has_exit = _candidate_has_exit(candidate)
            for signal in sorted(
                candidate.get("strategy_signals") or [],
                key=lambda item: str(item.get("strategy_key") or ""),
            ):
                strategy_key = str(signal.get("strategy_key") or "")
                strategy_version = str(
                    signal.get("strategy_version")
                    or (
                        f"{legacy_snapshot.get('configuration', {}).get('stock_manifest_version', '')}"
                        f":{strategy_key}"
                    )
                )
                registered = registry.get(strategy_version) or {
                    "lifecycle_status": "DRAFT_BLOCKED",
                    "validation": {},
                }
                lifecycle = str(registered["lifecycle_status"])
                validation = dict(registered["validation"])
                expected_net = validation.get("expected_return_net")
                expected_lower = validation.get(
                    "expected_return_lower_bound"
                )
                expected_source = validation.get(
                    "expected_return_source"
                )
                if lifecycle == "PAPER_TRIAL" and not expected_source:
                    expected_source = "paper_forward_trial_unproven"
                route = evaluate_signal_route(signal, regime)
                canonical_new_buy_rejection = _canonical_new_buy_rejection(
                    signal,
                    candidate_has_exit=candidate_has_exit,
                )
                if canonical_new_buy_rejection:
                    route = {
                        **route,
                        "eligible": False,
                        "reason_code": canonical_new_buy_rejection,
                        "route_reason": (
                            "canonical stock-level new-buy gate rejected signal"
                        ),
                    }
                signal["paper_trial_route"] = route
                if blocked_capabilities:
                    rejection = blocked_capabilities[0]
                    competition = "BLOCKED"
                elif canonical_new_buy_rejection:
                    rejection = canonical_new_buy_rejection
                    competition = "REJECTED"
                elif lifecycle not in {"PAPER_TRIAL", "PAPER_ACTIVE"}:
                    rejection = f"STRATEGY_LIFECYCLE_{lifecycle}"
                    competition = "RESEARCH_ONLY"
                elif (
                    lifecycle == "PAPER_ACTIVE"
                    and (
                        expected_lower is None
                        or decimal_value(expected_lower) <= 0
                    )
                ):
                    rejection = (
                        "OOS_EXPECTED_RETURN_LOWER_BOUND_MISSING"
                    )
                    competition = "RESEARCH_ONLY"
                elif not bool(route.get("eligible")):
                    rejection = str(
                        route.get("reason_code")
                        or "SIGNAL_NOT_BUY_READY"
                    )
                    competition = "REJECTED"
                elif lifecycle == "PAPER_TRIAL":
                    rejection = None
                    competition = "PAPER_TRIAL_ELIGIBLE"
                elif expected_lower is None or decimal_value(
                    expected_lower
                ) <= 0:
                    rejection = (
                        "OOS_EXPECTED_RETURN_LOWER_BOUND_MISSING"
                    )
                    competition = "RESEARCH_ONLY"
                else:
                    rejection = None
                    competition = "ELIGIBLE"
                invalidation = str(
                    signal.get("gate_reason")
                    or "strategy-specific frozen exit formula"
                )[:1000]
                valid_from = decision_at
                valid_until = signal_valid_until
                connection.execute(
                    text(
                        """
                        INSERT INTO st_strategy_signal_v2
                        (run_uid, strategy_version, stock_code, theme_code,
                         action,
                         lifecycle_status, raw_features_json, raw_score,
                         expected_return_net, expected_return_lower_bound,
                         expected_return_source, initial_stop,
                         invalidation_condition, risk_reward_ratio,
                         valid_from, valid_until, data_snapshot_hash,
                         config_hash, evidence_json, competition_status,
                         rejection_code, created_at)
                        VALUES
                        (:run_uid, :strategy_version, :stock_code, :theme_code,
                         :action,
                         :lifecycle_status, :raw_features, :raw_score,
                         :expected_net, :expected_lower, :expected_source,
                         :initial_stop,
                         :invalidation, :risk_reward_ratio,
                         :valid_from, :valid_until, :snapshot_hash,
                         :config_hash, :evidence, :competition_status,
                         :rejection_code, :created_at)
                        """
                    ),
                    {
                        "run_uid": run_uid,
                        "strategy_version": strategy_version,
                        "stock_code": str(signal.get("stock_code") or ""),
                        "theme_code": str(
                            signal.get("theme_code") or ""
                        )[:80],
                        "action": str(signal.get("signal_direction") or "HOLD"),
                        "lifecycle_status": lifecycle,
                        "raw_features": _json(signal),
                        "raw_score": signal.get("raw_score"),
                        "expected_net": expected_net,
                        "expected_lower": expected_lower,
                        "expected_source": expected_source,
                        "initial_stop": signal.get("stop_loss"),
                        "invalidation": invalidation,
                        "risk_reward_ratio": signal.get("risk_reward_ratio"),
                        "valid_from": valid_from,
                        "valid_until": valid_until,
                        "snapshot_hash": snapshot_hash,
                        "config_hash": legacy_snapshot.get(
                            "configuration", {}
                        ).get("stock_manifest_hash", "")
                        if strategy_key != "sector_preheat"
                        else sector_snapshot.get("config_hash", ""),
                        "evidence": _json(signal.get("evidence_chain") or []),
                        "competition_status": competition,
                        "rejection_code": rejection,
                        "created_at": now,
                    },
                )
                if competition in {
                    "ELIGIBLE",
                    "PAPER_TRIAL_ELIGIBLE",
                }:
                    eligible_for_competition.append(
                        {
                            "stock_code": str(
                                signal.get("stock_code") or ""
                            ),
                            "strategy_version": strategy_version,
                            "expected_return_lower_bound": expected_lower,
                            "raw_score": signal.get("raw_score"),
                            "competition_score": route.get(
                                "competition_score"
                            ),
                            "risk_reward_ratio": signal.get(
                                "risk_reward_ratio"
                            ),
                            "entry_price": (
                                signal.get("entry_high")
                                or signal.get("entry_low")
                                or signal.get("db_close")
                            ),
                            "initial_stop": signal.get("stop_loss"),
                            "invalidation_condition": invalidation,
                            "evidence": signal.get("evidence_chain")
                            or [],
                            "theme_code": signal.get("theme_code") or "",
                            "opening_target_fraction": route.get(
                                "opening_target_fraction"
                            ),
                            "route_reason": route.get("route_reason") or "",
                        }
                    )
                else:
                    rejected.append(
                        {
                            "stock_code": str(
                                signal.get("stock_code") or ""
                            ),
                            "strategy_version": strategy_version,
                            "rejection_code": rejection,
                        }
                    )
                signal_rows += 1

        competition_result = persist_portfolio_competition(
            connection,
            run_uid=run_uid,
            trade_date=target_date,
            account=account,
            market_regime=regime,
            candidates=eligible_for_competition,
        )
        rejected.extend(competition_result["rejected"])
        plan_payload = {
            "run_uid": run_uid,
            "account_id": ACCOUNT_ID,
            "market_regime": regime,
            "positions": competition_result["selected"],
            "rejected_candidates": rejected,
            "target_cash": competition_result["target_cash"],
            "target_risk_asset_weight": competition_result[
                "target_risk_asset_weight"
            ],
            "worst_case_loss": competition_result["worst_case_loss"],
            "theme_exposure": competition_result["theme_exposure"],
        }
        result_hash = canonical_json_hash(
            {"inputs": run_inputs, "signals": rejected, "plan": plan_payload}
        )
        connection.execute(
            text(
                """
                INSERT INTO st_portfolio_plan_v2
                (run_uid, account_id, plan_version, market_regime,
                 target_cash, target_risk_asset_weight, positions_json,
                 rejected_candidates_json, worst_case_loss,
                 theme_exposure_json, result_hash, created_at)
                VALUES
                (:run_uid, :account_id, 1, :regime, :target_cash,
                 :target_weight, :positions, :rejected,
                 :worst_case_loss, :theme_exposure, :result_hash, :created_at)
                """
            ),
            {
                "run_uid": run_uid,
                "account_id": ACCOUNT_ID,
                "regime": regime,
                "target_cash": competition_result["target_cash"],
                "target_weight": competition_result[
                    "target_risk_asset_weight"
                ],
                "positions": _json(competition_result["selected"]),
                "rejected": _json(rejected),
                "worst_case_loss": competition_result[
                    "worst_case_loss"
                ],
                "theme_exposure": _json(
                    competition_result["theme_exposure"]
                ),
                "result_hash": result_hash,
                "created_at": now,
            },
        )
        connection.execute(
            text(
                """
                UPDATE st_decision_run_v2
                SET result_hash = :result_hash, finished_at = :finished_at,
                    error_code = :error_code, error_message = :error_message
                WHERE run_uid = :run_uid
                """
            ),
            {
                "result_hash": result_hash,
                "finished_at": datetime.now(),
                "error_code": blocked_capabilities[0]
                if blocked_capabilities
                else None,
                "error_message": ",".join(blocked_capabilities)[:500],
                "run_uid": run_uid,
            },
        )
    position_monitor_result = monitor_positions(
        engine,
        trade_date=target_date,
        run_uid=run_uid,
        account_id=ACCOUNT_ID,
    )
    return {
        "status": "ok",
        "run_uid": run_uid,
        "run_status": run_status,
        "market_regime": regime,
        "data_quality_status": quality_status,
        "blocked_capabilities": blocked_capabilities,
        "signal_rows": signal_rows,
        "eligible_candidates": len(eligible_for_competition),
        "intent_count": competition_result["intent_count"],
        "paper_order_count": competition_result["order_count"],
        "position_monitor": position_monitor_result,
        "real_order_count": 0,
        "result_hash": result_hash,
    }
