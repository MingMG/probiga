"""Deterministic trade-level reconstruction for V2 research reports."""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

import numpy as np
import pandas as pd

from .config import canonical_json_hash
from .research import CompletedTrade, trade_metrics


CENT = Decimal("0.01")
PRICE = Decimal("0.000001")


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def _money(value: Any) -> Decimal:
    return _decimal(value).quantize(CENT, rounding=ROUND_HALF_UP)


@dataclass
class _OpenLot:
    entry_date: date
    quantity: int
    remaining_quantity: int
    execution_price: Decimal
    remaining_buy_fee: Decimal
    initial_risk_per_share: Decimal


def _initial_risk_per_share(
    data,
    *,
    stock_code: str,
    entry_day: pd.Timestamp,
    entry_price: Decimal,
    volatility_multiplier: Decimal = Decimal("3"),
    minimum_stop: Decimal = Decimal("0.06"),
    maximum_stop: Decimal = Decimal("0.15"),
) -> Decimal:
    history = data.close.loc[data.close.index < entry_day, stock_code]
    returns = history.dropna().astype(float).pct_change().dropna().tail(20)
    if len(returns) >= 10:
        volatility = Decimal(str(float(returns.std(ddof=1))))
        stop_fraction = max(
            minimum_stop,
            min(maximum_stop, volatility_multiplier * volatility),
        )
    else:
        stop_fraction = maximum_stop
    return (entry_price * stop_fraction).quantize(
        PRICE,
        rounding=ROUND_HALF_UP,
    )


def fifo_completed_trade_rows(
    trades: pd.DataFrame,
    data,
    *,
    volatility_multiplier: Decimal = Decimal("3"),
    minimum_stop: Decimal = Decimal("0.06"),
    maximum_stop: Decimal = Decimal("0.15"),
) -> list[dict[str, Any]]:
    """Pair realistic fill events into immutable FIFO completed trades."""
    if trades.empty:
        return []
    open_lots: dict[str, deque[_OpenLot]] = defaultdict(deque)
    completed: list[dict[str, Any]] = []
    indexed = trades.copy()
    indexed["_row_order"] = np.arange(len(indexed))
    fill_rows = indexed.loc[
        trades["side"].isin(["BUY", "SELL"])
        & trades["filled_units"].fillna(0).gt(0)
        & trades["execution_price"].fillna(0).gt(0)
    ].copy()
    fill_rows = fill_rows.sort_values(
        ["trade_date", "_row_order"],
        kind="stable",
    )
    for row_number, row in enumerate(
        fill_rows.to_dict(orient="records"),
        start=1,
    ):
        code = str(row["etf_code"])
        side = str(row["side"])
        quantity = int(row["filled_units"])
        execution_price = _decimal(row["execution_price"]).quantize(
            PRICE,
            rounding=ROUND_HALF_UP,
        )
        fee = _money(row.get("commission") or 0)
        trade_day = pd.Timestamp(row["trade_date"])
        if side == "BUY":
            open_lots[code].append(
                _OpenLot(
                    entry_date=trade_day.date(),
                    quantity=quantity,
                    remaining_quantity=quantity,
                    execution_price=execution_price,
                    remaining_buy_fee=fee,
                    initial_risk_per_share=_initial_risk_per_share(
                        data,
                        stock_code=code,
                        entry_day=trade_day,
                        entry_price=execution_price,
                        volatility_multiplier=volatility_multiplier,
                        minimum_stop=minimum_stop,
                        maximum_stop=maximum_stop,
                    ),
                )
            )
            continue
        remaining_sell = quantity
        remaining_sell_fee = fee
        while remaining_sell > 0:
            if not open_lots[code]:
                raise RuntimeError(
                    f"research replay sell exceeds FIFO lots: {code}"
                )
            lot = open_lots[code][0]
            consumed = min(remaining_sell, lot.remaining_quantity)
            buy_fee = (
                lot.remaining_buy_fee
                if consumed == lot.remaining_quantity
                else _money(
                    lot.remaining_buy_fee
                    * Decimal(consumed)
                    / Decimal(lot.remaining_quantity)
                )
            )
            sell_fee = (
                remaining_sell_fee
                if consumed == remaining_sell
                else _money(
                    remaining_sell_fee
                    * Decimal(consumed)
                    / Decimal(remaining_sell)
                )
            )
            buy_amount = _money(lot.execution_price * consumed)
            sell_amount = _money(execution_price * consumed)
            initial_risk = _money(
                lot.initial_risk_per_share * consumed
            )
            net_pnl = _money(
                sell_amount - buy_amount - buy_fee - sell_fee
            )
            identity = {
                "stock_code": code,
                "entry_date": lot.entry_date.isoformat(),
                "exit_date": trade_day.date().isoformat(),
                "quantity": consumed,
                "buy_fill_amount": str(buy_amount),
                "sell_fill_amount": str(sell_amount),
                "buy_fees": str(buy_fee),
                "sell_fees": str(sell_fee),
                "source_row": row_number,
                "sequence": len(completed) + 1,
            }
            completed.append(
                {
                    "trade_id": canonical_json_hash(identity)[:32],
                    "stock_code": code,
                    "entry_date": lot.entry_date,
                    "exit_date": trade_day.date(),
                    "quantity": consumed,
                    "buy_fill_amount": buy_amount,
                    "sell_fill_amount": sell_amount,
                    "buy_fees": buy_fee,
                    "sell_fees": sell_fee,
                    "initial_risk_amount": initial_risk,
                    "trade_net_pnl": net_pnl,
                    "evidence": {
                        "matching": "FIFO",
                        "entry_execution_price": str(
                            lot.execution_price
                        ),
                        "exit_execution_price": str(execution_price),
                        "initial_risk_per_share": str(
                            lot.initial_risk_per_share
                        ),
                        "fees_included_once": True,
                    },
                }
            )
            lot.remaining_quantity -= consumed
            lot.remaining_buy_fee = _money(
                lot.remaining_buy_fee - buy_fee
            )
            remaining_sell -= consumed
            remaining_sell_fee = _money(
                remaining_sell_fee - sell_fee
            )
            if lot.remaining_quantity == 0:
                open_lots[code].popleft()
    return completed


def equity_max_drawdown(equity: pd.Series) -> Decimal:
    if equity.empty:
        return Decimal("0")
    values = equity.astype(float)
    drawdown = values / values.cummax() - 1.0
    return Decimal(str(float(drawdown.min())))


def metrics_for_trade_rows(
    rows: list[dict[str, Any]],
    *,
    equity: pd.Series,
) -> dict[str, Any]:
    return trade_metrics(
        [
            CompletedTrade(
                trade_id=str(row["trade_id"]),
                stock_code=str(row["stock_code"]),
                trade_net_pnl=_decimal(row["trade_net_pnl"]),
                initial_risk_amount=_decimal(
                    row["initial_risk_amount"]
                ),
            )
            for row in rows
        ],
        max_drawdown=equity_max_drawdown(equity),
    )


def remove_best_n_net_pnl(
    rows: list[dict[str, Any]],
    count: int,
) -> Decimal:
    ordered = sorted(
        (_decimal(row["trade_net_pnl"]) for row in rows),
        reverse=True,
    )
    return sum(ordered[max(0, count) :], Decimal("0"))


def remove_largest_profit_security_net_pnl(
    rows: list[dict[str, Any]],
) -> Decimal | None:
    by_security: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for row in rows:
        by_security[str(row["stock_code"])] += _decimal(
            row["trade_net_pnl"]
        )
    if not by_security:
        return None
    largest = max(
        by_security,
        key=lambda code: (by_security[code], code),
    )
    return sum(
        (
            _decimal(row["trade_net_pnl"])
            for row in rows
            if str(row["stock_code"]) != largest
        ),
        Decimal("0"),
    )


def annual_trade_metrics(
    rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    years = sorted({row["exit_date"].year for row in rows})
    result: dict[str, dict[str, Any]] = {}
    for year in years:
        selected = [
            row for row in rows if row["exit_date"].year == year
        ]
        total = sum(
            (_decimal(row["trade_net_pnl"]) for row in selected),
            Decimal("0"),
        )
        result[str(year)] = {
            "completed_trade_count": len(selected),
            "cumulative_net_pnl": str(total),
            "expectancy_cny": (
                str(total / len(selected)) if selected else None
            ),
        }
    return result
