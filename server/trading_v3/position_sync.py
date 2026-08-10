from __future__ import annotations

import json
import uuid
from datetime import date, datetime
from typing import Any, Iterable

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .config import load_v3_config
from .domain import AlphaForecast, TradeHypothesis
from .exit_policy import daily_exit_reason
from .positions import decide_position_transition


def sync_position_states(
    engine: Engine,
    *,
    trade_date: date,
    equity: float,
    stocks: Iterable[dict[str, Any]],
    forecasts: Iterable[AlphaForecast],
    targets: Iterable[dict[str, Any]],
    hypotheses: Iterable[TradeHypothesis] = (),
    account_id: str = "paper-main-v2",
    decision_quality_status: str = "PASS",
) -> dict[str, Any]:
    config = load_v3_config()
    maximum_add_count = int(config["portfolio"]["maximum_add_count"])
    paper_signal_strategies = set(
        config.get("paper_discovery", {}).get(
            "allowed_strategy_keys",
            (),
        )
    )
    features = {
        str(item["stock_code"]): item for item in stocks
    }
    best_forecast: dict[str, AlphaForecast] = {}
    forecasts_by_stock_strategy: dict[
        tuple[str, str], AlphaForecast
    ] = {}
    for forecast in forecasts:
        forecasts_by_stock_strategy[
            (forecast.stock_code, forecast.strategy_key)
        ] = forecast
        current = best_forecast.get(forecast.stock_code)
        if current is None or (
            float(forecast.expected_return_net_pct or -10**6),
            float(forecast.raw_score or 0),
        ) > (
            float(current.expected_return_net_pct or -10**6),
            float(current.raw_score or 0),
        ):
            best_forecast[forecast.stock_code] = forecast
    target_rows = list(targets)
    target_map = {
        str(item["stock_code"]): float(item["target_weight"])
        for item in target_rows
    }
    paper_discovery_codes = {
        str(item["stock_code"])
        for item in target_rows
        if "paper_discovery" in set(item.get("strategy_keys") or [])
    }
    hypothesis_map = {
        item.scope_code: item
        for item in hypotheses
        if item.scope_type == "STOCK"
    }
    with engine.begin() as connection:
        rows = connection.execute(
            text(
                """
                SELECT l.stock_code,
                       MIN(l.opened_trade_date) AS entry_date,
                       SUM(l.remaining_quantity) AS quantity,
                       SUM(
                           CASE
                             WHEN l.settlement_date <= :trade_date
                             THEN l.remaining_quantity
                             ELSE 0
                           END
                       ) AS sellable_quantity,
                       SUM(l.remaining_quantity * l.cost_price)
                           / NULLIF(SUM(l.remaining_quantity), 0)
                           AS average_cost,
                       MAX(l.add_count) AS add_count,
                       MIN(l.protective_stop) AS protective_stop,
                       MAX(l.strategy_version) AS strategy_version,
                       MAX(
                            CASE
                              WHEN i.reason_code = 'V3_PAPER_DISCOVERY'
                              THEN 1 ELSE 0
                            END
                        ) AS is_paper_discovery,
                       MAX(
                            CASE
                              WHEN i.reason_code = 'V3_PAPER_DISCOVERY'
                              THEN i.evidence_json ELSE NULL
                            END
                        ) AS paper_evidence_json
                FROM st_position_lot_v2 l
                LEFT JOIN st_fill_v2 f
                  ON f.fill_id = l.opened_fill_id
                LEFT JOIN st_order_v2 o
                  ON o.order_id = f.order_id
                LEFT JOIN st_trade_intent_v2 i
                  ON i.intent_id = o.intent_id
                WHERE l.account_id = :account_id
                  AND l.remaining_quantity > 0
                GROUP BY l.stock_code
                """
            ),
            {"trade_date": trade_date, "account_id": account_id},
        ).mappings().all()
        updated = []
        for row in rows:
            code = str(row["stock_code"])
            item = features.get(code, {})
            forecast = best_forecast.get(code)
            hypothesis = hypothesis_map.get(code)
            previous = connection.execute(
                text(
                    """
                    SELECT state
                    FROM st_position_state_v3
                    WHERE account_id = :account_id
                      AND stock_code = :stock_code
                    LIMIT 1
                    """
                ),
                {"account_id": account_id, "stock_code": code},
            ).scalar()
            quantity = int(row["quantity"] or 0)
            current_price = float(
                item.get("price")
                or row["average_cost"]
                or 0
            )
            protective_stop = float(row["protective_stop"] or 0)
            paper_position = bool(
                int(row.get("is_paper_discovery") or 0)
            )
            paper_position_strategies: set[str] = set()
            if paper_position:
                try:
                    paper_evidence = json.loads(
                        str(row.get("paper_evidence_json") or "{}")
                    )
                    paper_position_strategies = {
                        str(value)
                        for value in (
                            paper_evidence.get("signal_strategy_keys") or ()
                        )
                        if str(value)
                    }
                except (TypeError, ValueError, json.JSONDecodeError):
                    paper_position_strategies = set()
                if not paper_position_strategies:
                    # Orders produced before strategy-level provenance was
                    # added were exclusively oversold-reversal probes.
                    paper_position_strategies = {"oversold_reversal"}
            raw_latest_trade_date = item.get("latest_trade_date")
            bar_fresh = bool(item) and (
                raw_latest_trade_date is None
                or str(raw_latest_trade_date)[:10]
                == trade_date.isoformat()
            )
            paper_signal_rows = [
                forecasts_by_stock_strategy[(code, strategy_key)]
                for strategy_key in (
                    paper_position_strategies
                    if paper_position
                    else paper_signal_strategies
                )
                if (code, strategy_key) in forecasts_by_stock_strategy
            ]
            signal_evaluation_valid = bool(
                decision_quality_status == "PASS"
                and bar_fresh
                and paper_signal_rows
                and any(
                    item.status != "INSUFFICIENT_DATA"
                    for item in paper_signal_rows
                )
            )
            hypothesis_invalidated = bool(
                hypothesis is not None
                and hypothesis.state == "INVALIDATED"
            )
            exit_reason = (
                daily_exit_reason(
                    protective_stop=protective_stop,
                    session_low=item.get("latest_low"),
                    close_above_ma20=item.get("close_above_ma20"),
                    ma20_above_ma60=item.get("ma20_above_ma60"),
                    hypothesis_invalidated=hypothesis_invalidated,
                    require_trend_alignment=not paper_position,
                )
                if bar_fresh
                else (
                    "HYPOTHESIS_INVALIDATED"
                    if hypothesis_invalidated
                    else None
                )
            )
            hard_stop = exit_reason == "HARD_STOP"
            trend_valid = exit_reason is None
            forecast_improving = bool(
                hypothesis is not None
                and hypothesis.state in {"ACTIVE", "TRIGGER_READY"}
                and hypothesis.probability
                >= hypothesis.prior_probability + 0.03
            )
            transition = decide_position_transition(
                stock_code=code,
                previous_state=str(
                    previous
                    or (
                        "PAPER_DISCOVERY"
                        if paper_position
                        else "PROBE"
                    )
                ),
                current_quantity=quantity,
                sellable_quantity=int(row["sellable_quantity"] or 0),
                current_weight=(
                    quantity * current_price / max(equity, 1.0)
                ),
                target_weight=target_map.get(code, 0.0),
                entry_date=row["entry_date"],
                trade_date=trade_date,
                trend_valid=trend_valid,
                hard_stop_triggered=hard_stop,
                forecast_status=(
                    "PAPER_DISCOVERY_ACTIVE"
                    if code in paper_discovery_codes
                    else (
                        forecast.status
                        if forecast
                        else "RESEARCH_ONLY_UNCALIBRATED"
                    )
                ),
                forecast_improving=forecast_improving,
                add_count=int(row["add_count"] or 0),
                maximum_add_count=maximum_add_count,
                signal_evaluation_valid=(
                    signal_evaluation_valid
                    if paper_position
                    else True
                ),
                explicit_exit_reason=exit_reason,
            )
            now = datetime.now().replace(microsecond=0)
            connection.execute(
                text(
                    """
                    INSERT INTO st_position_state_v3 (
                        position_state_id, account_id, stock_code,
                        short_name, state, quantity, sellable_quantity,
                        average_cost, current_weight, target_weight,
                        entry_date, add_count, thesis_version,
                        invalidation_json, last_action,
                        last_reason_code, last_reason, updated_at
                    ) VALUES (
                        :position_state_id, :account_id, :stock_code,
                        :short_name, :state, :quantity,
                        :sellable_quantity, :average_cost,
                        :current_weight, :target_weight, :entry_date,
                        :add_count, :thesis_version,
                        :invalidation_json, :last_action,
                        :last_reason_code, :last_reason, :updated_at
                    )
                    ON DUPLICATE KEY UPDATE
                        short_name = VALUES(short_name),
                        state = VALUES(state),
                        quantity = VALUES(quantity),
                        sellable_quantity = VALUES(sellable_quantity),
                        average_cost = VALUES(average_cost),
                        current_weight = VALUES(current_weight),
                        target_weight = VALUES(target_weight),
                        entry_date = VALUES(entry_date),
                        add_count = VALUES(add_count),
                        thesis_version = VALUES(thesis_version),
                        invalidation_json = VALUES(invalidation_json),
                        last_action = VALUES(last_action),
                        last_reason_code = VALUES(last_reason_code),
                        last_reason = VALUES(last_reason),
                        updated_at = VALUES(updated_at)
                    """
                ),
                {
                    "position_state_id": uuid.uuid4().hex,
                    "account_id": account_id,
                    "stock_code": code,
                    "short_name": item.get("stock_name") or code,
                    "state": transition.next_state,
                    "quantity": quantity,
                    "sellable_quantity": transition.sellable_quantity,
                    "average_cost": float(row["average_cost"] or 0),
                    "current_weight": (
                        quantity * current_price / max(equity, 1.0)
                    ),
                    "target_weight": transition.target_fraction,
                    "entry_date": row["entry_date"],
                    "add_count": transition.add_count,
                    "thesis_version": (
                        hypothesis.hypothesis_key
                        if hypothesis is not None
                        else forecast.model_version
                        if forecast is not None
                        else str(row["strategy_version"] or "")
                    ),
                    "invalidation_json": json.dumps(
                        {
                            "trend_valid": trend_valid,
                            "hard_stop_triggered": hard_stop,
                            "protective_stop": protective_stop,
                            "latest_price": current_price,
                            "latest_low": item.get(
                                "latest_low",
                                current_price,
                            ),
                            "exit_reason": exit_reason,
                            "hypothesis_state": (
                                hypothesis.state
                                if hypothesis is not None
                                else None
                            ),
                            "hypothesis_probability": (
                                hypothesis.probability
                                if hypothesis is not None
                                else None
                            ),
                            "hypothesis_invalidations": (
                                list(hypothesis.invalidations)
                                if hypothesis is not None
                                else []
                            ),
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    "last_action": transition.action,
                    "last_reason_code": transition.reason_code,
                    "last_reason": transition.reason,
                    "updated_at": now,
                },
            )
            updated.append(transition.as_dict())
    return {
        "status": "ok",
        "updated_count": len(updated),
        "transitions": updated,
    }
