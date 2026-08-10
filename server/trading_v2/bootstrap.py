"""Explicit bootstrap for the V2 account and immutable strategy registry."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .config import PROJECT_ROOT, canonical_json_hash
from .policy import load_portfolio_policy
from .versioning import code_version


ACCOUNT_ID = "paper-main-v2"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _code_commit_sha() -> str:
    return code_version()[0]


def _strategy_manifests() -> list[dict[str, Any]]:
    stock = _load_json(PROJECT_ROOT / "strategies" / "stock_strategy_v2.json")
    etf = _load_json(PROJECT_ROOT / "strategies" / "etf_trend_risk_v2.json")
    sector_preheat = _load_json(
        PROJECT_ROOT / "strategies" / "sector_preheat_v1.json"
    )
    intraday_activation = _load_json(
        PROJECT_ROOT / "strategies" / "intraday_activation_v2.json"
    )
    result: list[dict[str, Any]] = []
    for strategy in stock["strategies"]:
        strategy_id = str(strategy["key"])
        normalized = {
            "strategy_id": strategy_id,
            "strategy_version": f"{stock['manifest_version']}:{strategy_id}",
            "instrument_scope": "A_SHARE",
            "universe_definition": "audited A-share universe available at decision_at",
            "required_features": sorted(
                set(strategy.get("score_formula", {}).get("weights", {}).keys())
            ),
            "feature_available_time": "T_CLOSE_COMPLETE",
            "signal_schedule": "T close after data audit; earliest execution T+1",
            "entry_formula": strategy.get("score_formula", {}),
            "add_formula": {
                "expression": "position_state == VALID_STRONG and unrealized_pnl > 0 and add_count == 0"
            },
            "reduce_formula": strategy.get("exit_rules", {}).get("immediate", []),
            "exit_formula": strategy.get("exit_rules", {}),
            "initial_stop_formula": strategy.get("parameters", {}).get("stop_loss_pct"),
            "position_sizing_policy": "portfolio_policy_v2.0.0",
            "market_regime_mapping": "market_state_v2.1.0",
            "fee_profile_version": None,
            "validation_protocol": stock.get(
                "paper_trial_validation",
                {
                    "status": "RESEARCH",
                    "reason": "No qualifying OOS expected-return lower bound is registered",
                },
            ),
            "code_commit_sha": _code_commit_sha(),
            "source_manifest_hash": canonical_json_hash(stock),
            "frozen_parameters": strategy.get("parameters", {}),
        }
        normalized["config_hash"] = canonical_json_hash(
            {key: value for key, value in normalized.items() if key != "code_commit_sha"}
        )
        result.append(normalized)
    etf_normalized = {
        "strategy_id": "etf_trend_risk",
        "strategy_version": str(etf["strategy_version"]),
        "instrument_scope": "EXCHANGE_TRADED_FUND",
        "universe_definition": etf["universe"],
        "required_features": [
            "daily_kline",
            "adjustment_factor",
            "instrument_rule",
            "trade_calendar",
        ],
        "feature_available_time": "T_CLOSE_COMPLETE",
        "signal_schedule": etf["monthly_signal"]["schedule"],
        "entry_formula": etf["monthly_signal"],
        "add_formula": {"expression": "monthly rebalance target increases"},
        "reduce_formula": etf["risk_overlay"],
        "exit_formula": etf["risk_overlay"],
        "initial_stop_formula": etf["risk_overlay"],
        "position_sizing_policy": "portfolio_policy_v2.0.0",
        "market_regime_mapping": "independent defensive asset module",
        "fee_profile_version": None,
        "validation_protocol": {
            "status": "SHADOW",
            "forward_start_date": etf["forward_start_date"],
            "backfill": "prohibited",
            "research_acceptance": etf["research_acceptance"],
        },
        "code_commit_sha": _code_commit_sha(),
        "source_manifest_hash": canonical_json_hash(etf),
    }
    etf_normalized["config_hash"] = canonical_json_hash(
        {
            key: value
            for key, value in etf_normalized.items()
            if key != "code_commit_sha"
        }
    )
    result.append(etf_normalized)
    sector_normalized = {
        "strategy_id": str(sector_preheat["strategy_id"]),
        "strategy_version": str(sector_preheat["strategy_version"]),
        "instrument_scope": str(sector_preheat["instrument_scope"]),
        "universe_definition": sector_preheat["universe_definition"],
        "required_features": [
            "qmt_industry_member_snapshot",
            "qmt_concept_member_snapshot",
            "qmt_daily_kline",
            "trade_calendar",
        ],
        "feature_available_time": sector_preheat[
            "feature_available_time"
        ],
        "signal_schedule": sector_preheat["signal_schedule"],
        "entry_formula": {
            "factor_weights": sector_preheat["factor_weights"],
            "sector_thresholds": sector_preheat["sector_thresholds"],
            "candidate_thresholds": sector_preheat[
                "candidate_thresholds"
            ],
            "daily_market_discovery": sector_preheat[
                "daily_market_discovery"
            ],
            "right_side_startup": sector_preheat[
                "right_side_startup"
            ],
            "execution_confirmation": sector_preheat[
                "execution_confirmation"
            ],
            "entry_rules": sector_preheat["entry_rules"],
        },
        "add_formula": {
            "expression": (
                "sector_stage remains CONFIRMED and the position trend "
                "state is VALID_STRONG"
            )
        },
        "reduce_formula": sector_preheat["exit_rules"]["immediate"],
        "exit_formula": sector_preheat["exit_rules"],
        "initial_stop_formula": sector_preheat[
            "candidate_thresholds"
        ]["initial_stop_pct"],
        "position_sizing_policy": "portfolio_policy_v2.1.0-paper",
        "market_regime_mapping": "market_regime_v2.1.0",
        "fee_profile_version": None,
        "validation_protocol": sector_preheat["validation_protocol"],
        "code_commit_sha": _code_commit_sha(),
        "source_manifest_hash": canonical_json_hash(sector_preheat),
        "frozen_parameters": {
            key: sector_preheat[key]
            for key in (
                "history_sessions",
                "minimum_sector_members",
                "minimum_member_coverage",
                "maximum_hot_sectors",
                "maximum_candidates_per_sector",
                "maximum_candidates",
                "maximum_discovery_candidates",
                "factor_weights",
                "sector_thresholds",
                "candidate_thresholds",
                "daily_market_discovery",
                "right_side_startup",
                "execution_confirmation",
            )
        },
    }
    sector_normalized["config_hash"] = canonical_json_hash(
        {
            key: value
            for key, value in sector_normalized.items()
            if key != "code_commit_sha"
        }
    )
    result.append(sector_normalized)
    intraday_normalized = {
        "strategy_id": str(intraday_activation["strategy_id"]),
        "strategy_version": str(
            intraday_activation["strategy_version"]
        ),
        "instrument_scope": str(
            intraday_activation["instrument_scope"]
        ),
        "universe_definition": (
            "immutable daily sector-preheat watch pool plus a separately "
            "guarded market-wide QMT momentum, volume, reversal and "
            "locked-leader follower radar"
        ),
        "required_features": [
            "qmt_minute_price",
            "qmt_daily_previous_close",
            "qmt_point_in_time_membership",
            "daily_watch_pool",
            "full_market_current_quote",
            "accumulated_full_session_minutes",
            "instrument_rule",
        ],
        "feature_available_time": "INTRADAY_POINT_IN_TIME",
        "signal_schedule": (
            "every minute from 09:31 through 14:50; ordinary watch-pool "
            "entries stop at 14:45 and guarded reversal probes stop at 14:50"
        ),
        "entry_formula": {
            "data_quality": intraday_activation["data_quality"],
            "market_confirmation": intraday_activation[
                "market_confirmation"
            ],
            "candidate_activation": intraday_activation[
                "candidate_activation"
            ],
            "leader_substitution": intraday_activation[
                "leader_substitution"
            ],
            "leader_follower_radar": intraday_activation[
                "leader_follower_radar"
            ],
            "market_wide_reversal_radar": intraday_activation[
                "market_wide_reversal_radar"
            ],
        },
        "add_formula": {
            "expression": "existing V2 position state machine only"
        },
        "reduce_formula": intraday_activation["risk_controls"],
        "exit_formula": {
            "expression": (
                "protective stop, trend invalidation, sector invalidation "
                "or market risk event; no fixed holding period"
            )
        },
        "initial_stop_formula": (
            "daily pool reuses immutable source stop; market-wide reversal "
            "uses frozen pre-close protection and may only move upward"
        ),
        "position_sizing_policy": "portfolio_policy_v2.1.0-paper",
        "market_regime_mapping": (
            "intraday multi-point confirmation overlay on "
            "market_regime_v2.1.0"
        ),
        "fee_profile_version": None,
        "validation_protocol": {
            "status": "PAPER_TRIAL",
            "real_trading": "prohibited",
            "minimum_forward_trades": 30,
            "minimum_profit_factor": 1.2,
            "lookahead_data": "prohibited",
        },
        "code_commit_sha": _code_commit_sha(),
        "source_manifest_hash": canonical_json_hash(
            intraday_activation
        ),
        "frozen_parameters": {
            key: intraday_activation[key]
            for key in (
                "session",
                "data_quality",
                "market_confirmation",
                "candidate_activation",
                "leader_substitution",
                "leader_follower_radar",
                "market_wide_reversal_radar",
                "risk_controls",
            )
        },
    }
    intraday_normalized["config_hash"] = canonical_json_hash(
        {
            key: value
            for key, value in intraday_normalized.items()
            if key != "code_commit_sha"
        }
    )
    result.append(intraday_normalized)
    return result


def bootstrap_v2(engine: Engine) -> dict[str, Any]:
    """Create deterministic initial records after migrations were applied."""
    policy = load_portfolio_policy()
    now = datetime.now()
    created_account = False
    registered = 0
    corrected = 0
    superseded = 0
    capability_rows_initialized = 0
    with engine.begin() as connection:
        account = connection.execute(
            text(
                "SELECT account_id FROM st_trade_account_v2 "
                "WHERE account_id = :account_id"
            ),
            {"account_id": ACCOUNT_ID},
        ).first()
        if not account:
            connection.execute(
                text(
                    """
                    INSERT INTO st_trade_account_v2
                    (account_id, account_name, status, initial_cash, cash_balance,
                     peak_equity, policy_version, policy_hash,
                     fee_profile_version, instrument_rule_version,
                     real_trading_enabled, created_at, updated_at)
                    VALUES
                    (:account_id, :account_name, 'CONFIG_BLOCKED',
                     :cash, :cash, :cash, :policy_version, :policy_hash,
                     NULL, NULL, 0, :now, :now)
                    """
                ),
                {
                    "account_id": ACCOUNT_ID,
                    "account_name": "20万元V2主模拟账户",
                    "cash": policy.initial_cash,
                    "policy_version": policy.version,
                    "policy_hash": policy.config_hash,
                    "now": now,
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO st_cash_ledger_v2
                    (cash_event_id, account_id, business_event_key, event_type,
                     amount, balance_after, occurred_at, created_at)
                    VALUES
                    (:event_id, :account_id, :business_key, 'INITIAL_DEPOSIT',
                     :cash, :cash, :now, :now)
                    """
                ),
                {
                    "event_id": canonical_json_hash(
                        {"account_id": ACCOUNT_ID, "event": "INITIAL_DEPOSIT"}
                    )[:32],
                    "account_id": ACCOUNT_ID,
                    "business_key": f"{ACCOUNT_ID}:INITIAL_DEPOSIT",
                    "cash": policy.initial_cash,
                    "now": now,
                },
            )
            created_account = True
        manifests = _strategy_manifests()
        for manifest in manifests:
            strategy_id = str(manifest["strategy_id"])
            version = str(manifest["strategy_version"])
            existing = connection.execute(
                text(
                    """
                    SELECT config_hash, manifest_json
                    FROM st_strategy_version_v2
                    WHERE strategy_id = :strategy_id AND version = :version
                    """
                ),
                {"strategy_id": strategy_id, "version": version},
            ).mappings().first()
            if existing:
                if str(existing["config_hash"]) != str(manifest["config_hash"]):
                    existing_manifest = json.loads(
                        str(existing["manifest_json"] or "{}")
                    )
                    semantic_existing_hash = canonical_json_hash(
                        {
                            key: value
                            for key, value in existing_manifest.items()
                            if key not in {"code_commit_sha", "config_hash"}
                        }
                    )
                    if semantic_existing_hash != str(manifest["config_hash"]):
                        raise RuntimeError(
                            f"published strategy manifest changed: {strategy_id}/{version}"
                        )
                    # One-time correction for registry rows created before the
                    # code-version field was separated from config_hash.
                    connection.execute(
                        text(
                            """
                            UPDATE st_strategy_version_v2
                            SET manifest_json = :manifest_json,
                                config_hash = :config_hash,
                                code_commit_sha = :code_commit_sha
                            WHERE strategy_id = :strategy_id AND version = :version
                            """
                        ),
                        {
                            "manifest_json": json.dumps(
                                manifest, ensure_ascii=False, sort_keys=True
                            ),
                            "config_hash": manifest["config_hash"],
                            "code_commit_sha": manifest["code_commit_sha"],
                            "strategy_id": strategy_id,
                            "version": version,
                        },
                    )
                    corrected += 1
                continue
            lifecycle = {
                "etf_trend_risk": "SHADOW",
                "sector_preheat": "RESEARCH",
                "intraday_dynamic_activation": "RESEARCH",
            }.get(
                strategy_id,
                (
                    "PAPER_TRIAL"
                    if str(
                        manifest.get("validation_protocol", {}).get("status")
                        or ""
                    ).upper()
                    == "PAPER_TRIAL"
                    else "RESEARCH"
                ),
            )
            connection.execute(
                text(
                    """
                    INSERT INTO st_strategy_version_v2
                    (strategy_id, version, lifecycle_status, instrument_scope,
                     manifest_json, config_hash, code_commit_sha,
                     validation_json, created_at)
                    VALUES
                    (:strategy_id, :version, :lifecycle_status, :instrument_scope,
                     :manifest_json, :config_hash, :code_commit_sha,
                     :validation_json, :created_at)
                    """
                ),
                {
                    "strategy_id": strategy_id,
                    "version": version,
                    "lifecycle_status": lifecycle,
                    "instrument_scope": manifest["instrument_scope"],
                    "manifest_json": json.dumps(
                        manifest, ensure_ascii=False, sort_keys=True
                    ),
                    "config_hash": manifest["config_hash"],
                    "code_commit_sha": manifest["code_commit_sha"],
                    "validation_json": json.dumps(
                        manifest["validation_protocol"], ensure_ascii=False
                    ),
                    "created_at": now,
                },
            )
            registered += 1
        superseded = 0
        # Exactly one paper/shadow version per strategy may remain current.
        # Keeping an older sector-preheat version in PAPER_TRIAL made the UI
        # and downstream source selection ambiguous after a frozen upgrade.
        for manifest in manifests:
            superseded_result = connection.execute(
                text(
                    """
                    UPDATE st_strategy_version_v2
                    SET lifecycle_status = 'SUSPENDED',
                        suspended_at = COALESCE(suspended_at, :now)
                    WHERE strategy_id = :strategy_id
                      AND version <> :current_version
                      AND lifecycle_status IN (
                          'RESEARCH','PAPER_TRIAL','PAPER_ACTIVE','SHADOW'
                      )
                    """
                ),
                {
                    "strategy_id": manifest["strategy_id"],
                    "current_version": manifest["strategy_version"],
                    "now": now,
                },
            )
            superseded += int(superseded_result.rowcount or 0)
        capability = connection.execute(
            text(
                """
                SELECT capability_code
                FROM st_execution_capability_v2
                WHERE capability_code = 'B-003_RELIABLE_LEVEL1_BID_ASK'
                """
            )
        ).first()
        if not capability:
            connection.execute(
                text(
                    """
                    INSERT INTO st_execution_capability_v2
                    (capability_code, status, protocol_version,
                     consecutive_trade_days, evidence_json, checked_at,
                     passed_at, updated_at)
                    VALUES
                    ('B-003_RELIABLE_LEVEL1_BID_ASK', 'BLOCK',
                     'level1_continuity_v2.0.0', 0, :evidence,
                     :now, NULL, :now)
                    """
                ),
                {
                    "evidence": json.dumps(
                        {
                            "reason": "NO_COMPLETED_CONTINUITY_AUDIT",
                            "minimum_complete_trade_days": 5,
                            "maximum_quote_age_seconds": 15,
                            "minimum_interval_coverage": 0.95,
                            "last_price_fallback": "PROHIBITED",
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    "now": now,
                },
            )
            capability_rows_initialized += 1
    return {
        "status": "ok",
        "account_id": ACCOUNT_ID,
        "account_created": created_account,
        "strategy_versions_registered": registered,
        "strategy_registry_metadata_corrected": corrected,
        "strategy_versions_superseded": superseded,
        "capability_rows_initialized": capability_rows_initialized,
        "real_trading_enabled": False,
        "configuration_blocks": [
            "B-001_ACTUAL_BROKER_FEES",
            "B-002_ACCOUNT_INSTRUMENT_PERMISSIONS",
            "B-003_RELIABLE_LEVEL1_BID_ASK",
        ],
    }
