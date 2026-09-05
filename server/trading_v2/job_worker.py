"""Single-owner asynchronous worker for V2 decision and research jobs."""
from __future__ import annotations

import json
import math
import os
import socket
import uuid
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .config import canonical_json_hash
from .decision_worker import run_daily_decision
from .versioning import code_version


WORKER_NAME = "trading-v2-job-worker"
RESEARCH_PROTOCOL_VERSION = "v2_research_protocol_20260725_4"
ETF_RESEARCH_DATA_START = "2019-01-01"
ETF_RESEARCH_MINIMUM_START = "2021-01-04"
ETF_UNIVERSE_CUTOFF = "2020-12-31"
ETF_MUTABLE_INPUT_BLOCKERS = (
    "ETF_PIT_CLASSIFICATION_LEDGER_UNAVAILABLE",
    "ETF_RAW_BAR_REVISION_LEDGER_UNAVAILABLE",
)


def _json(payload: Any) -> str:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _heartbeat(
    engine: Engine,
    *,
    status: str,
    current_job_id: str | None = None,
    success: bool = False,
    error_code: str | None = None,
    error_message: str | None = None,
) -> None:
    now = datetime.now()
    instance = f"{socket.gethostname()}:{os.getpid()}"
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO st_worker_heartbeat_v2
                (worker_name, worker_instance, status, current_job_id,
                 last_success_at, last_error_code, last_error_message,
                 heartbeat_at, updated_at)
                VALUES
                (:worker_name, :instance, :status, :job_id,
                 :success_at, :error_code, :error_message, :now, :now)
                ON DUPLICATE KEY UPDATE
                    worker_instance = VALUES(worker_instance),
                    status = VALUES(status),
                    current_job_id = VALUES(current_job_id),
                    last_success_at = CASE
                        WHEN VALUES(last_success_at) IS NOT NULL
                        THEN VALUES(last_success_at)
                        ELSE last_success_at END,
                    last_error_code = VALUES(last_error_code),
                    last_error_message = VALUES(last_error_message),
                    heartbeat_at = VALUES(heartbeat_at),
                    updated_at = VALUES(updated_at)
                """
            ),
            {
                "worker_name": WORKER_NAME,
                "instance": instance,
                "status": status,
                "job_id": current_job_id,
                "success_at": now if success else None,
                "error_code": error_code,
                "error_message": (error_message or "")[:500] or None,
                "now": now,
            },
        )


def _claim_job(engine: Engine) -> dict[str, Any] | None:
    with engine.begin() as connection:
        lock = connection.execute(
            text("SELECT GET_LOCK('probiga:trading_v2_job_worker', 0)")
        ).scalar()
        if int(lock or 0) != 1:
            return None
        try:
            row = connection.execute(
                text(
                    """
                    SELECT * FROM st_job_v2
                    WHERE status = 'PENDING'
                    ORDER BY requested_at, job_id
                    LIMIT 1 FOR UPDATE
                    """
                )
            ).mappings().first()
            if not row:
                return None
            connection.execute(
                text(
                    """
                    UPDATE st_job_v2
                    SET status = 'RUNNING', started_at = :now,
                        error_code = NULL, error_message = NULL
                    WHERE job_id = :job_id AND status = 'PENDING'
                    """
                ),
                {"now": datetime.now(), "job_id": row["job_id"]},
            )
            return dict(row)
        finally:
            connection.execute(
                text("SELECT RELEASE_LOCK('probiga:trading_v2_job_worker')")
            )


def research_backtest_adapter(
    *,
    strategy_id: str,
    strategy_version: str,
    instrument_scope: str,
) -> dict[str, Any]:
    """Return the existing reproducible adapter for one registered version.

    A strategy registration alone does not mean that the generic screener or
    ETF replay can reproduce it. Keep that distinction explicit so an
    unsupported strategy can never inherit another strategy's report.
    """

    strategy_id = str(strategy_id or "")
    strategy_version = str(strategy_version or "")
    instrument_scope = str(instrument_scope or "").upper()
    if instrument_scope == "EXCHANGE_TRADED_FUND":
        supported = (
            strategy_id == "etf_trend_risk"
            and strategy_version == "etf_trend_risk_v2.0.0"
        )
        return {
            "supported": supported,
            "status": "AVAILABLE" if supported else "UNAVAILABLE",
            "adapter": "etf_trade_level_replay_v2" if supported else None,
            "minimum_start_date": (
                ETF_RESEARCH_MINIMUM_START if supported else None
            ),
            "reason": (
                "已绑定 ETF 成交级回放"
                if supported
                else "该 ETF 策略没有可复算回测适配器"
            ),
        }

    return {
        "supported": False,
        "status": "UNAVAILABLE",
        "adapter": None,
        "reason": "当前登记股票策略暂无可复算历史回测适配器",
    }


def _etf_dependency_start(start_date: str) -> str:
    """Return the frozen adapter's declared history window.

    Recent evaluation windows still need the same pre-cutoff observations used
    to freeze the ETF universe and calculate the first signals.  Loading only
    ``start_date - 550 days`` would put a 2025 request entirely after the 2020
    cutoff and make every product ineligible.
    """

    parsed_start = datetime.fromisoformat(str(start_date)).date().isoformat()
    if parsed_start < ETF_RESEARCH_MINIMUM_START:
        raise ValueError(
            "ETF formal backtest start_date must be on or after "
            f"{ETF_RESEARCH_MINIMUM_START}"
        )
    return ETF_RESEARCH_DATA_START


def _resolved_execution_inputs(
    request: dict[str, Any],
    *,
    instrument_scope: str,
) -> tuple[float, float]:
    is_etf = str(instrument_scope).upper() == "EXCHANGE_TRADED_FUND"
    capital_raw = request.get("initial_capital")
    cost_raw = request.get("round_trip_cost")
    initial_capital = float(
        (200_000.0 if is_etf else 1_000_000.0)
        if capital_raw is None
        else capital_raw
    )
    round_trip_cost = float(
        (0.001 if is_etf else 0.002) if cost_raw is None else cost_raw
    )
    if not math.isfinite(initial_capital) or initial_capital <= 0:
        raise ValueError("initial_capital must be finite and positive")
    if not math.isfinite(round_trip_cost) or round_trip_cost < 0:
        raise ValueError("round_trip_cost must be finite and non-negative")
    return initial_capital, round_trip_cost


def _etf_research_truth_contract(
    snapshot_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Quarantine mutable ETF reference inputs from promotion authority."""

    price_rows: list[dict[str, Any]] = []
    classifications: dict[str, dict[str, Any]] = {}
    for raw in snapshot_rows:
        row = dict(raw)
        code = str(row.get("etf_code") or "")
        data_version = str(row.get("data_version") or "")
        if not code or not data_version:
            raise RuntimeError("ETF native snapshot row is unversioned")
        raw_adjust_type = row.get("adjust_type")
        if raw_adjust_type is None or int(raw_adjust_type) != 0:
            raise RuntimeError("ETF research must use native adjust_type=0 rows")
        received_at = str(row.get("received_at") or "")
        if not received_at:
            raise RuntimeError("ETF native snapshot row lacks received_at")
        numeric_values: dict[str, float] = {}
        for field in ("open", "close", "pre_close", "amount"):
            try:
                value = float(row.get(field))
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"ETF native snapshot row has invalid {field}"
                ) from exc
            if not math.isfinite(value) or (
                field != "amount" and value <= 0
            ) or (field == "amount" and value < 0):
                raise RuntimeError(
                    f"ETF native snapshot row has invalid {field}"
                )
            numeric_values[field] = value
        asset_class = str(row.get("asset_class") or "")
        classification_updated_at = str(
            row.get("classification_updated_at") or ""
        )
        if not asset_class or not classification_updated_at:
            raise RuntimeError(
                "ETF current classification snapshot lacks provenance"
            )
        price_rows.append({
            "etf_code": code,
            "trade_date": str(row.get("trade_date") or ""),
            "adjust_type": 0,
            "data_version": data_version,
            "validation_status": str(row.get("validation_status") or ""),
            "quality_status": str(row.get("quality_status") or ""),
            "received_at": received_at,
            **numeric_values,
        })
        classification = {
            "etf_code": code,
            "asset_class": asset_class,
            "classification_updated_at": classification_updated_at,
        }
        previous = classifications.setdefault(code, classification)
        if previous != classification:
            raise RuntimeError(
                "ETF current classification changed within one snapshot"
            )
    price_rows.sort(key=lambda item: (item["trade_date"], item["etf_code"]))
    classification_rows = [
        classifications[key] for key in sorted(classifications)
    ]
    price_hash = canonical_json_hash(price_rows)
    classification_hash = canonical_json_hash(classification_rows)
    payload = {
        "schema": "probiga.etf-research-input-truth.v1",
        "status": "MUTABLE_INPUTS_QUARANTINED_RESEARCH_ONLY",
        "native_unadjusted_prices_only": True,
        "adjusted_history_rows_consumed": False,
        "derived_price_protocol": (
            "NATIVE_UNADJUSTED_CLOSE_PRE_CLOSE_RETURN_CHAIN_AND_OPEN_RATIO_V1"
        ),
        "native_price_snapshot_hash": price_hash,
        "native_price_row_count": len(price_rows),
        "native_price_revision_ledger_available": False,
        "current_classification_snapshot_hash": classification_hash,
        "current_classification_count": len(classification_rows),
        "historical_classification_verified": False,
        "current_classification_can_authorize_promotion": False,
        "activation_eligible": False,
        "promotion_blockers": list(ETF_MUTABLE_INPUT_BLOCKERS),
    }
    return {**payload, "contract_hash": canonical_json_hash(payload)}


def _etf_backtest(engine: Engine, request: dict[str, Any]) -> dict[str, Any]:
    from decimal import Decimal

    from server.trading_v2.research import (
        evaluate_oos_gate,
        nav_records_from_equity,
    )
    from server.trading_v2.research_replay import (
        annual_trade_metrics,
        fifo_completed_trade_rows,
        metrics_for_trade_rows,
        remove_best_n_net_pnl,
        remove_largest_profit_security_net_pnl,
    )
    from tools.backtest_etf_ensemble import (
        build_target_schedule,
        load_market_data,
        performance_metrics,
    )
    from tools.backtest_etf_robust import (
        ExecutionAssumptions,
        build_fast_risk_schedule,
        freeze_universe,
        moving_block_bootstrap,
        simulate_realistic,
    )

    start = str(request["start_date"])
    end = str(request["end_date"])
    seed = int(request["random_seed"])
    initial_capital, resolved_round_trip_cost = _resolved_execution_inputs(
        request,
        instrument_scope="EXCHANGE_TRADED_FUND",
    )
    requested_round_trip_cost = request.get("round_trip_cost")
    # The ETF simulator models commission, spread and impact separately.  Its
    # frozen base round-trip rate is 0.10%; scale every explicit trading cost
    # from that base when a request supplies a different rate.
    base_round_trip_cost = 0.001
    cost_multiplier = (
        1.0
        if requested_round_trip_cost is None
        else resolved_round_trip_cost / base_round_trip_cost
    )
    dependency_start = _etf_dependency_start(start)
    source_data = load_market_data(engine, dependency_start, end)
    with engine.connect() as connection:
        snapshot_rows = connection.execute(
            text(
                """
                SELECT k.etf_code, k.trade_date, k.adjust_type,
                       k.data_version, k.received_at,
                       k.open, k.close, k.pre_close, k.amount,
                       k.validation_status, k.quality_status,
                       c.asset_class,
                       c.updated_at AS classification_updated_at
                FROM sm_etf_kline k
                JOIN si_etf_code c ON c.etf_code = k.etf_code
                WHERE k.adjust_type = 0
                  AND k.k_type = 1
                  AND k.validation_status = 'passed'
                  AND k.quality_status = 'validated'
                  AND k.trade_date BETWEEN :start_date AND :end_date
                ORDER BY k.trade_date, k.etf_code
                """
            ),
            {"start_date": dependency_start, "end_date": end},
        ).mappings().all()
    if not snapshot_rows:
        raise RuntimeError("ETF data snapshot has no validated source rows")
    research_truth = _etf_research_truth_contract(
        [dict(row) for row in snapshot_rows]
    )
    data_snapshot_hash = str(research_truth["contract_hash"])
    data, universe_audit = freeze_universe(
        source_data,
        cutoff_date=ETF_UNIVERSE_CUTOFF,
    )
    monthly_targets, target_records = build_target_schedule(
        data,
        backtest_start=start,
        end_date=end,
        mode="trend_risk",
        execution_lag=1,
    )

    def run_case(
        assumptions: ExecutionAssumptions,
        *,
        risk_mode: str = "daily_vol_stop",
        volatility_multiplier: float = 3.0,
        minimum_stop: float = 0.06,
        maximum_stop: float = 0.15,
        reentry_mode: str = "trend_resume",
        reentry_cooldown_days: int = 3,
    ) -> dict[str, Any]:
        schedule, contexts, exits = build_fast_risk_schedule(
            data,
            monthly_targets,
            end_date=end,
            risk_mode=risk_mode,
            volatility_multiplier=volatility_multiplier,
            minimum_stop=minimum_stop,
            maximum_stop=maximum_stop,
            reentry_mode=reentry_mode,
            reentry_cooldown_days=reentry_cooldown_days,
        )
        equity, rebalances, fills = simulate_realistic(
            data,
            schedule,
            contexts=contexts,
            end_date=end,
            assumptions=assumptions,
        )
        rows = fifo_completed_trade_rows(
            fills,
            data,
            volatility_multiplier=Decimal(
                str(volatility_multiplier)
            ),
            minimum_stop=Decimal(str(minimum_stop)),
            maximum_stop=Decimal(str(maximum_stop)),
        )
        open_position_count = 0
        if (
            not fills.empty
            and {"etf_code", "side", "filled_units"}.issubset(fills.columns)
        ):
            signed_units = fills["filled_units"].fillna(0).astype(float).where(
                fills["side"].eq("BUY"),
                -fills["filled_units"].fillna(0).astype(float),
            )
            open_position_count = int(
                (signed_units.groupby(fills["etf_code"]).sum() > 0).sum()
            )
        return {
            "equity": equity,
            "rebalances": rebalances,
            "fills": fills,
            "trades": rows,
            "metrics": metrics_for_trade_rows(rows, equity=equity),
            "performance": performance_metrics(
                equity / assumptions.initial_capital
            ),
            "risk_exit_events": int(len(exits)),
            "risk_reentry_events": (
                int(
                    rebalances["event_type"]
                    .isin(
                        [
                            "fast_risk_reentry",
                            "fast_risk_exit_and_reentry",
                        ]
                    )
                    .sum()
                )
                if not rebalances.empty
                else 0
            ),
            "blocked_orders": (
                int(rebalances["blocked_orders"].sum())
                if not rebalances.empty
                else 0
            ),
            "partial_orders": (
                int(rebalances["partial_orders"].sum())
                if not rebalances.empty
                else 0
            ),
            "open_position_count": open_position_count,
        }

    base_assumptions = ExecutionAssumptions(
        initial_capital=initial_capital,
        cost_multiplier=cost_multiplier,
    )
    proposed = run_case(base_assumptions)
    no_overlay = run_case(
        base_assumptions,
        risk_mode="none",
        reentry_mode="none",
    )
    doubled_cost = run_case(
        ExecutionAssumptions(
            initial_capital=initial_capital,
            cost_multiplier=cost_multiplier * 2.0,
        )
    )
    half_capacity = run_case(
        ExecutionAssumptions(
            initial_capital=initial_capital,
            cost_multiplier=cost_multiplier,
            max_adv_participation=0.01,
        )
    )
    adverse_gap = run_case(
        ExecutionAssumptions(
            initial_capital=initial_capital,
            cost_multiplier=cost_multiplier,
            adverse_open_gap_rate=0.005,
        )
    )
    neighborhood_results: list[dict[str, Any]] = []
    for multiplier in (2.7, 3.0, 3.3):
        for minimum_stop in (0.055, 0.060, 0.065):
            case = run_case(
                base_assumptions,
                volatility_multiplier=multiplier,
                minimum_stop=minimum_stop,
            )
            pnl = Decimal(
                str(case["metrics"]["cumulative_net_pnl"])
            )
            neighborhood_results.append(
                {
                    "volatility_multiplier": multiplier,
                    "minimum_stop": minimum_stop,
                    "cumulative_net_pnl": str(pnl),
                    "positive": pnl > 0,
                }
            )
    positive_neighborhood_ratio = (
        Decimal(
            sum(
                1
                for item in neighborhood_results
                if item["positive"]
            )
        )
        / Decimal(len(neighborhood_results))
    )
    bootstrap = moving_block_bootstrap(
        proposed["equity"],
        simulations=2000,
        block_days=20,
        seed=seed,
    )
    future_violations = sum(
        1
        for row in target_records
        if str(row["execution_date"]) <= str(row["signal_date"])
    )
    robustness = {
        "complete": True,
        "block_bootstrap_paths": int(bootstrap["simulations"]),
        "positive_parameter_neighborhood_ratio": str(
            positive_neighborhood_ratio
        ),
        "fee_and_slippage_2x": doubled_cost["metrics"],
        "capacity_half": half_capacity["metrics"],
        "adverse_next_open_gap_0_5pct": adverse_gap["metrics"],
        "remove_best_1_net_pnl": str(
            remove_best_n_net_pnl(proposed["trades"], 1)
        ),
        "remove_best_3_net_pnl": str(
            remove_best_n_net_pnl(proposed["trades"], 3)
        ),
        "remove_best_5_net_pnl": str(
            remove_best_n_net_pnl(proposed["trades"], 5)
        ),
        "remove_largest_security_net_pnl": str(
            remove_largest_profit_security_net_pnl(
                proposed["trades"]
            )
        ),
        "annual_trade_metrics": annual_trade_metrics(
            proposed["trades"]
        ),
        "parameter_neighborhood": neighborhood_results,
        "bootstrap": bootstrap,
    }
    statistical_gate = evaluate_oos_gate(
        security_scope="ETF",
        trading_days=int(len(proposed["equity"])),
        oos_windows=int(len(target_records)),
        metrics=proposed["metrics"],
        doubled_cost_metrics=doubled_cost["metrics"],
        remove_best_three_net_pnl=remove_best_n_net_pnl(
            proposed["trades"], 3
        ),
        robustness=robustness,
        future_data_violations=future_violations,
        impossible_fill_profit=Decimal("0"),
        nav_records=nav_records_from_equity(proposed["equity"]),
        doubled_cost_nav_records=nav_records_from_equity(
            doubled_cost["equity"]
        ),
    )
    proposed_risk_adjusted = (
        float(proposed["performance"]["total_return"])
        / max(
            abs(float(proposed["performance"]["max_drawdown"])),
            1e-12,
        )
    )
    baseline_risk_adjusted = (
        float(no_overlay["performance"]["total_return"])
        / max(
            abs(float(no_overlay["performance"]["max_drawdown"])),
            1e-12,
        )
    )
    baseline_comparison = {
        "same_data_account_cost_execution": True,
        "dynamic_performance": proposed["performance"],
        "no_overlay_performance": no_overlay["performance"],
        "dynamic_trade_metrics": proposed["metrics"],
        "no_overlay_trade_metrics": no_overlay["metrics"],
        "dynamic_risk_adjusted_return": proposed_risk_adjusted,
        "no_overlay_risk_adjusted_return": baseline_risk_adjusted,
        "dynamic_improves_risk_adjusted_return": (
            proposed_risk_adjusted > baseline_risk_adjusted
        ),
        "dynamic_max_drawdown_not_worse": (
            float(proposed["performance"]["max_drawdown"])
            >= float(no_overlay["performance"]["max_drawdown"])
        ),
    }
    with engine.connect() as connection:
        confirmed_fee_rows = int(
            connection.execute(
                text(
                    """
                    SELECT COUNT(*) FROM st_fee_profile_v2
                    WHERE security_type = 'ETF'
                      AND confirmation_status = 'CONFIRMED'
                    """
                )
            ).scalar()
            or 0
        )
    promotion_blockers: list[str] = []
    if statistical_gate["status"] != "PASS":
        promotion_blockers.append("STATISTICAL_GATE_BLOCKED")
    if not (
        baseline_comparison["same_data_account_cost_execution"]
        and baseline_comparison[
            "dynamic_improves_risk_adjusted_return"
        ]
        and baseline_comparison["dynamic_max_drawdown_not_worse"]
    ):
        promotion_blockers.append("BASELINE_COMPARISON_BLOCKED")
    if confirmed_fee_rows == 0:
        promotion_blockers.append("B-001_ACTUAL_BROKER_FEES")
    promotion_blockers.extend(ETF_MUTABLE_INPUT_BLOCKERS)
    # The frozen holdout has already been inspected during strategy
    # development, so it cannot honestly be relabelled as a one-shot,
    # untouched test set after the fact.
    promotion_blockers.append("FROZEN_HOLDOUT_NOT_PRISTINE")
    return {
        "adapter": "etf_trade_level_replay_v2",
        "strategy_key": "etf_trend_risk",
        "data_dependency_start": dependency_start,
        "data_snapshot": {
            "hash": data_snapshot_hash,
            "row_count": len(snapshot_rows),
            "start_date": dependency_start,
            "end_date": end,
            "source": "gj_big_qmt_inner",
            "validation_status": "passed",
            "quality_status": "validated",
            "adjust_type": 0,
            "stored_adjusted_history_consumed": False,
            "row_identity": (
                "etf_code+trade_date+adjust_type+data_version+received_at"
            ),
        },
        "research_input_truth": research_truth,
        "account_initial_cash": f"{initial_capital:.2f}",
        "execution_assumptions": {
            "initial_capital_cny": initial_capital,
            "round_trip_cost": (
                base_round_trip_cost
                if requested_round_trip_cost is None
                else resolved_round_trip_cost
            ),
            "base_round_trip_cost": base_round_trip_cost,
            "cost_multiplier": cost_multiplier,
        },
        "data_source": "gj_big_qmt_inner",
        "universe_cutoff": ETF_UNIVERSE_CUTOFF,
        "eligible_universe": universe_audit.loc[
            universe_audit["eligible"], "etf_code"
        ].tolist(),
        "trade_dates": int(len(proposed["equity"])),
        "rebalance_windows": int(len(target_records)),
        "completed_trade_count": len(proposed["trades"]),
        "open_position_count": proposed["open_position_count"],
        "final_equity_cny": (
            float(proposed["equity"].iloc[-1])
            if not proposed["equity"].empty
            else initial_capital
        ),
        "metrics": proposed["metrics"],
        "performance": proposed["performance"],
        "blocked_orders": proposed["blocked_orders"],
        "partial_orders": proposed["partial_orders"],
        "risk_exit_events": proposed["risk_exit_events"],
        "risk_reentry_events": proposed["risk_reentry_events"],
        "dynamic_exit_policy": {
            "exit": "volatility_scaled_trailing_stop",
            "execution": "next_trading_day_open",
            "reentry": "ma20_and_return20_recovery",
            "reentry_cooldown_trading_days": 3,
            "fixed_holding_days": False,
        },
        "robustness": robustness,
        "baseline_comparison": baseline_comparison,
        "statistical_gate": statistical_gate,
        "promotion_protocol": {
            "status": (
                "PASS" if not promotion_blockers else "BLOCK"
            ),
            "blockers": promotion_blockers,
            "research_assumption_fees": confirmed_fee_rows == 0,
            "oos_passed": False,
            "mutable_etf_inputs_quarantined": True,
        },
        "_data_snapshot_hash": data_snapshot_hash,
        "_trade_rows": proposed["trades"],
    }


def _run_backtest_job_impl(
    engine: Engine,
    request: dict[str, Any],
) -> dict[str, Any]:
    strategy_id = str(request.get("strategy_id") or "")
    strategy_version = str(request["strategy_version"])
    start_date = str(request["start_date"])
    end_date = str(request["end_date"])
    random_seed = int(request["random_seed"])
    with engine.connect() as connection:
        if strategy_id:
            strategy_rows = connection.execute(
                text(
                    """
                    SELECT strategy_id, version, instrument_scope, config_hash
                    FROM st_strategy_version_v2
                    WHERE BINARY strategy_id = BINARY :strategy_id
                      AND BINARY version = BINARY :version
                    """
                ),
                {"strategy_id": strategy_id, "version": strategy_version},
            ).mappings().all()
        else:
            # Compatibility for jobs queued before strategy_id became part of
            # the request.  Ambiguous versions fail closed rather than binding
            # to whichever row the database happens to return first.
            strategy_rows = connection.execute(
                text(
                    """
                    SELECT strategy_id, version, instrument_scope, config_hash
                    FROM st_strategy_version_v2
                    WHERE BINARY version = BINARY :version
                    """
                ),
                {"version": strategy_version},
            ).mappings().all()
    if not strategy_rows:
        raise ValueError("exact strategy version is not registered")
    if len(strategy_rows) != 1:
        raise ValueError("strategy version is ambiguous; strategy_id is required")
    strategy = strategy_rows[0]
    strategy_id = str(strategy["strategy_id"])
    request["strategy_id"] = strategy_id
    adapter = research_backtest_adapter(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        instrument_scope=str(strategy["instrument_scope"]),
    )
    if not adapter["supported"]:
        raise ValueError(str(adapter["reason"]))
    request["_backtest_adapter"] = str(adapter["adapter"])
    code_sha = code_version()[0]
    strategy_binding = {
        "strategy_id": strategy_id,
        "strategy_version": strategy_version,
        "config_hash": str(strategy["config_hash"]),
        "code_commit_sha": code_sha,
        "protocol_version": RESEARCH_PROTOCOL_VERSION,
    }
    initial_capital, round_trip_cost = _resolved_execution_inputs(
        request,
        instrument_scope=str(strategy["instrument_scope"]),
    )
    running_evidence = {
        "adapter": str(adapter["adapter"]),
        "strategy_binding": strategy_binding,
        "run_request_uid": str(request.get("run_request_uid") or ""),
        "execution_assumptions": {
            "initial_capital_cny": initial_capital,
            "round_trip_cost": round_trip_cost,
        },
    }
    request_hash = canonical_json_hash(
        {
            "run_request_uid": str(request.get("run_request_uid") or ""),
            "strategy_id": strategy_id,
            "strategy_version": strategy_version,
            "start_date": start_date,
            "end_date": end_date,
            "random_seed": random_seed,
            "initial_capital": request.get("initial_capital"),
            "round_trip_cost": request.get("round_trip_cost"),
            "top_per_day": int(
                request.get("top_per_day") or request.get("top") or 10
            ),
            "protocol_version": RESEARCH_PROTOCOL_VERSION,
            "code_commit_sha": code_sha,
            "config_hash": strategy["config_hash"],
        }
    )
    backtest_uid = request_hash[:32]
    request["_backtest_uid"] = backtest_uid
    now = datetime.now()
    with engine.begin() as connection:
        existing = connection.execute(
            text(
                """
                SELECT status, result_hash FROM st_backtest_run_v2
                WHERE request_hash = :request_hash
                """
            ),
            {"request_hash": request_hash},
        ).mappings().first()
        if existing and existing["status"] == "COMPLETED":
            return {
                "backtest_uid": backtest_uid,
                "status": "idempotent_hit",
                "result_hash": existing["result_hash"],
            }
        connection.execute(
            text(
                """
                INSERT INTO st_backtest_run_v2
                (backtest_uid, strategy_version, start_date, end_date,
                 random_seed, status, request_hash, data_snapshot_hash,
                 code_commit_sha, config_hash, protocol_version,
                 result_json, gate_status, started_at)
                VALUES
                (:uid, :version, :start_date, :end_date, :seed, 'RUNNING',
                 :request_hash, :empty_hash, :code_sha, :config_hash,
                 :protocol_version, :result_json, 'BLOCK', :started_at)
                ON DUPLICATE KEY UPDATE
                    status = 'RUNNING', started_at = VALUES(started_at),
                    result_json = VALUES(result_json),
                    result_hash = NULL,
                    data_snapshot_hash = VALUES(data_snapshot_hash),
                    gate_status = 'BLOCK', finished_at = NULL,
                    error_code = NULL, error_message = NULL
                """
            ),
            {
                "uid": backtest_uid,
                "version": strategy_version,
                "start_date": start_date,
                "end_date": end_date,
                "seed": random_seed,
                "request_hash": request_hash,
                "empty_hash": "0" * 64,
                "code_sha": code_sha,
                "config_hash": strategy["config_hash"],
                "protocol_version": RESEARCH_PROTOCOL_VERSION,
                "result_json": _json(running_evidence),
                "started_at": now,
            },
        )
    if adapter["adapter"] != "etf_trade_level_replay_v2":
        raise ValueError("registered strategy has no reproducible backtest adapter")
    report = _etf_backtest(engine, request)
    report["strategy_binding"] = strategy_binding
    report["run_request_uid"] = str(request.get("run_request_uid") or "")
    trade_rows = list(report.pop("_trade_rows", []))
    data_hash = str(report.pop("_data_snapshot_hash", "") or "")
    data_hash = data_hash or canonical_json_hash(
        {
            "data_audit": report.get("data_audit"),
            "data_dependency_start": report.get(
                "data_dependency_start"
            ),
            "trade_dates": report.get("trade_dates"),
        }
    )
    result_hash = canonical_json_hash(report)
    gate_status = str(
        (report.get("promotion_protocol") or {}).get("status") or "BLOCK"
    )
    with engine.begin() as connection:
        for row in trade_rows:
            connection.execute(
                text(
                    """
                    INSERT INTO st_backtest_trade_v2
                    (backtest_uid, trade_id, stock_code, entry_date,
                     exit_date, quantity, buy_fill_amount,
                     sell_fill_amount, buy_fees, sell_fees,
                     initial_risk_amount, trade_net_pnl,
                     evidence_json, created_at)
                    VALUES
                    (:backtest_uid, :trade_id, :stock_code, :entry_date,
                     :exit_date, :quantity, :buy_fill_amount,
                     :sell_fill_amount, :buy_fees, :sell_fees,
                     :initial_risk_amount, :trade_net_pnl,
                     :evidence_json, :created_at)
                    """
                ),
                {
                    "backtest_uid": backtest_uid,
                    "trade_id": row["trade_id"],
                    "stock_code": row["stock_code"],
                    "entry_date": row["entry_date"],
                    "exit_date": row["exit_date"],
                    "quantity": row["quantity"],
                    "buy_fill_amount": row["buy_fill_amount"],
                    "sell_fill_amount": row["sell_fill_amount"],
                    "buy_fees": row["buy_fees"],
                    "sell_fees": row["sell_fees"],
                    "initial_risk_amount": row[
                        "initial_risk_amount"
                    ],
                    "trade_net_pnl": row["trade_net_pnl"],
                    "evidence_json": _json(row["evidence"]),
                    "created_at": datetime.now(),
                },
            )
        connection.execute(
            text(
                """
                UPDATE st_backtest_run_v2
                SET status = 'COMPLETED',
                    data_snapshot_hash = :data_hash,
                    result_json = :result_json,
                    result_hash = :result_hash,
                    gate_status = :gate_status,
                    finished_at = :finished_at
                WHERE backtest_uid = :uid
                """
            ),
            {
                "data_hash": data_hash,
                "result_json": _json(report),
                "result_hash": result_hash,
                "gate_status": gate_status,
                "finished_at": datetime.now(),
                "uid": backtest_uid,
            },
        )
    return {
        "backtest_uid": backtest_uid,
        "status": "COMPLETED",
        "gate_status": gate_status,
        "result_hash": result_hash,
    }


def _mark_latest_matching_backtest_failed(
    engine: Engine,
    request: dict[str, Any],
    error: Exception,
) -> int:
    backtest_uid = str(request.get("_backtest_uid") or "")
    if backtest_uid:
        with engine.begin() as connection:
            result = connection.execute(
                text(
                    """
                    UPDATE st_backtest_run_v2
                    SET status = 'FAILED',
                        error_code = :error_code,
                        error_message = :error_message,
                        finished_at = :finished_at
                    WHERE backtest_uid = :backtest_uid
                      AND status = 'RUNNING'
                    """
                ),
                {
                    "error_code": type(error).__name__.upper()[:80],
                    "error_message": str(error)[:500],
                    "finished_at": datetime.now(),
                    "backtest_uid": backtest_uid,
                },
            )
        return int(result.rowcount or 0)
    if request.get("run_request_uid"):
        # New jobs identify their own run before inserting it. If validation
        # fails earlier, never mark an older run with matching dates as failed.
        return 0
    required = (
        "strategy_version",
        "start_date",
        "end_date",
        "random_seed",
    )
    if any(key not in request for key in required):
        return 0
    with engine.begin() as connection:
        result = connection.execute(
            text(
                """
                UPDATE st_backtest_run_v2
                SET status = 'FAILED',
                    error_code = :error_code,
                    error_message = :error_message,
                    finished_at = :finished_at
                WHERE strategy_version = :strategy_version
                  AND start_date = :start_date
                  AND end_date = :end_date
                  AND random_seed = :random_seed
                  AND status = 'RUNNING'
                ORDER BY started_at DESC
                LIMIT 1
                """
            ),
            {
                "error_code": type(error).__name__.upper()[:80],
                "error_message": str(error)[:500],
                "finished_at": datetime.now(),
                "strategy_version": str(request["strategy_version"]),
                "start_date": str(request["start_date"]),
                "end_date": str(request["end_date"]),
                "random_seed": int(request["random_seed"]),
            },
        )
    return int(result.rowcount or 0)


def _run_backtest_job(
    engine: Engine,
    request: dict[str, Any],
) -> dict[str, Any]:
    try:
        return _run_backtest_job_impl(engine, request)
    except Exception as exc:
        _mark_latest_matching_backtest_failed(engine, request, exc)
        raise


def repair_orphaned_backtests(
    engine: Engine,
    *,
    stale_after_minutes: int = 15,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    cutoff = (now or datetime.now()) - timedelta(
        minutes=max(1, stale_after_minutes)
    )
    with engine.connect() as connection:
        running = connection.execute(
            text(
                """
                SELECT backtest_uid, strategy_version, start_date,
                       end_date, random_seed, started_at
                FROM st_backtest_run_v2
                WHERE status = 'RUNNING'
                  AND started_at <= :cutoff
                ORDER BY started_at
                """
            ),
            {"cutoff": cutoff},
        ).mappings().all()
        failed_jobs = connection.execute(
            text(
                """
                SELECT job_id, request_json, error_code, error_message,
                       finished_at
                FROM st_job_v2
                WHERE job_type = 'BACKTEST'
                  AND status = 'FAILED'
                ORDER BY finished_at DESC
                """
            )
        ).mappings().all()
    failed_by_request: dict[
        tuple[str, str, str, int], dict[str, Any]
    ] = {}
    for job in failed_jobs:
        try:
            request = json.loads(str(job["request_json"]))
            key = (
                str(request["strategy_version"]),
                str(request["start_date"]),
                str(request["end_date"]),
                int(request["random_seed"]),
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            continue
        failed_by_request.setdefault(key, dict(job))

    repaired: list[dict[str, Any]] = []
    for row in running:
        key = (
            str(row["strategy_version"]),
            str(row["start_date"]),
            str(row["end_date"]),
            int(row["random_seed"]),
        )
        job = failed_by_request.get(key)
        if not job:
            continue
        finished_at = job.get("finished_at") or datetime.now()
        with engine.begin() as connection:
            result = connection.execute(
                text(
                    """
                    UPDATE st_backtest_run_v2
                    SET status = 'FAILED',
                        error_code = :error_code,
                        error_message = :error_message,
                        finished_at = :finished_at
                    WHERE backtest_uid = :backtest_uid
                      AND status = 'RUNNING'
                    """
                ),
                {
                    "error_code": str(
                        job.get("error_code") or "ORPHANED_BACKTEST"
                    )[:80],
                    "error_message": str(
                        job.get("error_message")
                        or "backtest worker failed before finalization"
                    )[:500],
                    "finished_at": finished_at,
                    "backtest_uid": str(row["backtest_uid"]),
                },
            )
        if int(result.rowcount or 0) == 1:
            repaired.append(
                {
                    "backtest_uid": str(row["backtest_uid"]),
                    "job_id": str(job["job_id"]),
                    "status": "FAILED",
                }
            )
    return repaired


def run_one_job(engine: Engine) -> dict[str, Any]:
    job = _claim_job(engine)
    if not job:
        _heartbeat(engine, status="IDLE")
        return {"status": "idle"}
    job_id = str(job["job_id"])
    _heartbeat(
        engine,
        status="RUNNING",
        current_job_id=job_id,
    )
    try:
        request = json.loads(str(job["request_json"]))
        job_type = str(job["job_type"])
        if job_type == "DECISION_RUN":
            result = run_daily_decision(
                engine,
                trade_date=str(request["trade_date"]),
            )
            result_ref = str(result["run_uid"])
        elif job_type == "BACKTEST":
            result = _run_backtest_job(engine, request)
            result_ref = str(result["backtest_uid"])
        else:
            raise ValueError(f"unsupported V2 job type: {job_type}")
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE st_job_v2
                    SET status = 'COMPLETED', result_ref = :result_ref,
                        finished_at = :finished_at
                    WHERE job_id = :job_id
                    """
                ),
                {
                    "result_ref": result_ref,
                    "finished_at": datetime.now(),
                    "job_id": job_id,
                },
            )
        _heartbeat(engine, status="IDLE", success=True)
        return {
            "status": "COMPLETED",
            "job_id": job_id,
            "job_type": job_type,
            "result_ref": result_ref,
            "result": result,
        }
    except Exception as exc:
        error_code = type(exc).__name__.upper()
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE st_job_v2
                    SET status = 'FAILED', error_code = :error_code,
                        error_message = :error_message,
                        finished_at = :finished_at
                    WHERE job_id = :job_id
                    """
                ),
                {
                    "error_code": error_code[:80],
                    "error_message": str(exc)[:500],
                    "finished_at": datetime.now(),
                    "job_id": job_id,
                },
            )
        _heartbeat(
            engine,
            status="ERROR",
            error_code=error_code,
            error_message=str(exc),
        )
        return {
            "status": "FAILED",
            "job_id": job_id,
            "error_code": error_code,
            "error_message": str(exc),
        }
