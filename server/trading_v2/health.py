"""Daily strategy-health calculation and fail-closed automatic suspension."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .config import canonical_json_hash
from .jobs import transition_strategy
from .research import (
    CompletedTrade,
    trade_metrics,
    v2_nav_statistical_guard,
)
from .domain import decimal_value


WINDOWS = (20, 60, 120)


@dataclass
class _OpenLot:
    stock_code: str
    quantity: int
    unit_cost_with_fee: Decimal
    initial_risk_per_share: Decimal
    opened_at: datetime


def _trade_dates(
    engine: Engine,
    *,
    end_date: date,
    limit: int,
) -> list[date]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT trade_date FROM si_trade_calendar
                WHERE trade_status = 1 AND trade_date <= :end_date
                ORDER BY trade_date DESC LIMIT :limit
                """
            ),
            {"end_date": end_date, "limit": int(limit)},
        ).fetchall()
    return sorted(
        value if isinstance(value, date) else date.fromisoformat(str(value)[:10])
        for (value,) in rows
    )


def _completed_trades(
    engine: Engine,
    *,
    strategy_version: str,
    start_date: date,
    end_date: date,
) -> list[CompletedTrade]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT f.fill_id, f.stock_code, f.side, f.quantity,
                       f.price, f.fee_amount, f.filled_at,
                       i.initial_stop
                FROM st_fill_v2 f
                JOIN st_order_v2 o ON o.order_id = f.order_id
                JOIN st_trade_intent_v2 i ON i.intent_id = o.intent_id
                WHERE i.strategy_version = :strategy_version
                  AND DATE(f.filled_at) <= :end_date
                ORDER BY f.filled_at, f.fill_id
                """
            ),
            {
                "strategy_version": strategy_version,
                "end_date": end_date,
            },
        ).mappings().all()
    lots: dict[str, list[_OpenLot]] = {}
    completed: list[CompletedTrade] = []
    for row in rows:
        code = str(row["stock_code"])
        quantity = int(row["quantity"])
        price = decimal_value(row["price"])
        fee = decimal_value(row["fee_amount"])
        if str(row["side"]) == "BUY":
            initial_stop = decimal_value(row["initial_stop"])
            lots.setdefault(code, []).append(
                _OpenLot(
                    stock_code=code,
                    quantity=quantity,
                    unit_cost_with_fee=(
                        price + fee / Decimal(quantity)
                    ),
                    initial_risk_per_share=max(
                        Decimal("0"),
                        price - initial_stop,
                    ),
                    opened_at=row["filled_at"],
                )
            )
            continue
        remaining = quantity
        sell_fee_per_share = fee / Decimal(quantity)
        for lot in lots.get(code, []):
            if remaining <= 0:
                break
            if lot.quantity <= 0:
                continue
            consumed = min(remaining, lot.quantity)
            pnl = (
                price
                - sell_fee_per_share
                - lot.unit_cost_with_fee
            ) * Decimal(consumed)
            exit_date = row["filled_at"].date()
            if exit_date >= start_date:
                trade_id = canonical_json_hash(
                    {
                        "sell_fill_id": row["fill_id"],
                        "stock_code": code,
                        "opened_at": lot.opened_at,
                        "consumed": consumed,
                    }
                )[:32]
                completed.append(
                    CompletedTrade(
                        trade_id=trade_id,
                        stock_code=code,
                        trade_net_pnl=pnl,
                        initial_risk_amount=(
                            lot.initial_risk_per_share
                            * Decimal(consumed)
                        ),
                    )
                )
            lot.quantity -= consumed
            remaining -= consumed
        if remaining:
            raise RuntimeError(
                f"health reconstruction found unmatched sell: {code}"
            )
    return completed


def _window_drawdown(
    engine: Engine,
    *,
    start_date: date,
    end_date: date,
) -> Decimal:
    with engine.connect() as connection:
        values = [
            decimal_value(row[0])
            for row in connection.execute(
                text(
                    """
                    SELECT total_equity FROM st_equity_daily_v2
                    WHERE account_id = 'paper-main-v2'
                      AND trade_date BETWEEN :start_date AND :end_date
                    ORDER BY trade_date
                    """
                ),
                {"start_date": start_date, "end_date": end_date},
            ).fetchall()
        ]
    peak = Decimal("0")
    maximum = Decimal("0")
    for equity in values:
        peak = max(peak, equity)
        if peak > 0:
            maximum = max(maximum, (peak - equity) / peak)
    return maximum


def _health_state(
    metrics: dict[str, Any],
    *,
    drawdown_limit: Decimal,
    nav_statistical_guard: dict[str, Any] | None = None,
) -> tuple[str, str]:
    count = int(metrics.get("completed_trade_count") or 0)
    expectancy = (
        decimal_value(metrics["expectancy_cny"])
        if metrics.get("expectancy_cny") is not None
        else None
    )
    pf_raw = metrics.get("profit_factor")
    profit_factor = (
        decimal_value(pf_raw)
        if pf_raw is not None
        and pf_raw != "INF"
        else None
    )
    drawdown = decimal_value(metrics.get("max_drawdown") or 0)
    if (
        (count >= 20 and profit_factor is not None and profit_factor < Decimal("0.90"))
        or (expectancy is not None and expectancy < 0)
        or drawdown > drawdown_limit
    ):
        return "RED", "SUSPEND_NEW_ENTRY"
    if (
        count >= 20
        and isinstance(nav_statistical_guard, dict)
        and nav_statistical_guard.get("passed") is True
        and profit_factor is not None
        and profit_factor >= Decimal("1.10")
        and expectancy is not None
        and expectancy >= 0
    ):
        return "GREEN", "NORMAL"
    return "YELLOW", "WEIGHT_CAP_0_50"


def run_strategy_health(
    engine: Engine,
    *,
    trade_date: date,
) -> dict[str, Any]:
    dates = _trade_dates(engine, end_date=trade_date, limit=max(WINDOWS))
    if not dates:
        raise RuntimeError("trade calendar has no health window")
    with engine.connect() as connection:
        strategies = connection.execute(
            text(
                """
                SELECT strategy_id, version, validation_json
                FROM st_strategy_version_v2
                WHERE lifecycle_status IN ('PAPER_TRIAL','PAPER_ACTIVE')
                ORDER BY strategy_id, version
                """
            )
        ).mappings().all()
    results: list[dict[str, Any]] = []
    suspend: list[tuple[str, str, dict[str, Any]]] = []
    for strategy in strategies:
        validation = json.loads(str(strategy["validation_json"] or "{}"))
        drawdown_limit = decimal_value(
            validation.get("paper_drawdown_limit") or "0.10"
        )
        by_window: dict[int, dict[str, Any]] = {}
        for window in WINDOWS:
            window_dates = dates[-window:]
            start_date = window_dates[0]
            trades = _completed_trades(
                engine,
                strategy_version=str(strategy["version"]),
                start_date=start_date,
                end_date=trade_date,
            )
            drawdown = _window_drawdown(
                engine,
                start_date=start_date,
                end_date=trade_date,
            )
            metrics = trade_metrics(trades, max_drawdown=drawdown)
            # V2 currently has no immutable per-strategy daily NAV ledger.
            # Account-wide equity is not a substitute, so health remains
            # fail-closed (YELLOW) until an exact strategy NAV path is bound.
            nav_guard = v2_nav_statistical_guard(
                None,
                minimum_observations=max(20, window),
                minimum_effective_sample_size=max(20, window // 2),
                minimum_profit_factor_lcb=Decimal("1.10"),
            )
            health_status, action_code = _health_state(
                metrics,
                drawdown_limit=drawdown_limit,
                nav_statistical_guard=nav_guard,
            )
            evidence = {
                "strategy_id": strategy["strategy_id"],
                "strategy_version": strategy["version"],
                "window_days": window,
                "start_date": start_date.isoformat(),
                "end_date": trade_date.isoformat(),
                "metrics": metrics,
                "drawdown_limit": str(drawdown_limit),
                "nav_statistical_guard": nav_guard,
            }
            result_hash = canonical_json_hash(evidence)
            pf = metrics.get("profit_factor")
            with engine.begin() as connection:
                connection.execute(
                    text(
                        """
                        INSERT INTO st_strategy_health_daily_v2
                        (strategy_version, trade_date, window_days,
                         completed_trades, expectancy_cny, profit_factor,
                         max_drawdown, health_status, action_code,
                         evidence_json, result_hash, created_at)
                        VALUES
                        (:version, :trade_date, :window_days,
                         :completed_trades, :expectancy_cny, :profit_factor,
                         :max_drawdown, :health_status, :action_code,
                         :evidence, :result_hash, :created_at)
                        ON DUPLICATE KEY UPDATE
                            completed_trades = VALUES(completed_trades),
                            expectancy_cny = VALUES(expectancy_cny),
                            profit_factor = VALUES(profit_factor),
                            max_drawdown = VALUES(max_drawdown),
                            health_status = VALUES(health_status),
                            action_code = VALUES(action_code),
                            evidence_json = VALUES(evidence_json),
                            result_hash = VALUES(result_hash),
                            created_at = VALUES(created_at)
                        """
                    ),
                    {
                        "version": strategy["version"],
                        "trade_date": trade_date,
                        "window_days": window,
                        "completed_trades": metrics["completed_trade_count"],
                        "expectancy_cny": metrics.get("expectancy_cny"),
                        "profit_factor": None if pf == "INF" else pf,
                        "max_drawdown": drawdown,
                        "health_status": health_status,
                        "action_code": action_code,
                        "evidence": json.dumps(
                            evidence,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                            default=str,
                        ),
                        "result_hash": result_hash,
                        "created_at": datetime.now(),
                    },
                )
            by_window[window] = {
                "health_status": health_status,
                "action_code": action_code,
                "metrics": metrics,
                "result_hash": result_hash,
            }
        sixty = by_window[60]
        if sixty["health_status"] == "RED":
            suspend.append(
                (
                    str(strategy["strategy_id"]),
                    str(strategy["version"]),
                    sixty,
                )
            )
        results.append(
            {
                "strategy_id": strategy["strategy_id"],
                "strategy_version": strategy["version"],
                "windows": by_window,
            }
        )
    suspension_events = []
    for strategy_id, version, evidence in suspend:
        suspension_events.append(
            transition_strategy(
                engine,
                strategy_id=strategy_id,
                strategy_version=version,
                next_status="SUSPENDED",
                reason=(
                    "automatic suspension: 60-session health status RED"
                ),
                operator="trading-v2-health-worker",
            )
        )
    return {
        "status": "ok",
        "trade_date": trade_date.isoformat(),
        "strategy_count": len(results),
        "strategies": results,
        "suspension_events": suspension_events,
    }
