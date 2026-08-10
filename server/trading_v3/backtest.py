from __future__ import annotations

import gc
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from server.common.batch_db import read_frame_chunks

from .calibration import CalibrationTable, fit_calibration
from .config import load_v3_config
from .exit_policy import daily_exit_reason
from .metrics import trade_metrics
from .right_side_policy import (
    AMOUNT_RATIO_5_20_RANGE,
    DISTANCE_MA20_RANGE,
    MA20_SLOPE_5D_RANGE,
    MAXIMUM_LATEST_CHANGE_PCT,
    MINIMUM_MARKET_RETURN_20D_PCT,
    RETURN_20D_RANGE,
    RETURN_60D_RANGE,
    RIGHT_SIDE_FEATURE_COLUMNS,
    RIGHT_SIDE_LABEL_PROTOCOL,
    right_side_model_contract_hash,
)
from .validation import model_gate_failures


@dataclass(frozen=True)
class BacktestResult:
    calibration: CalibrationTable
    training_metrics: dict[str, Any]
    validation_metrics: dict[str, Any]
    portfolio_metrics: dict[str, Any]
    validation_trades: list[dict[str, Any]]
    equity_curve: list[dict[str, Any]]
    gate_status: str
    block_reasons: tuple[str, ...]
    feature_schema_hash: str
    periods: dict[str, str]
    diagnostics: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "calibration": self.calibration.as_dict(),
            "training_metrics": self.training_metrics,
            "validation_metrics": self.validation_metrics,
            "portfolio_metrics": self.portfolio_metrics,
            "validation_trades": self.validation_trades,
            "equity_curve": self.equity_curve,
            "gate_status": self.gate_status,
            "block_reasons": list(self.block_reasons),
            "feature_schema_hash": self.feature_schema_hash,
            "periods": self.periods,
            "diagnostics": self.diagnostics,
            "validation_protocol": {
                "name": "PURGED_MONTHLY_WALK_FORWARD",
                "calibration_window": "TRAILING_ONE_YEAR",
                "signal_embargo_calendar_days": 35,
                "label_protocol": RIGHT_SIDE_LABEL_PROTOCOL,
                "right_censored_candidates_in_portfolio": True,
            },
        }


FEATURE_COLUMNS = RIGHT_SIDE_FEATURE_COLUMNS


def _load_history(
    engine: Engine,
    *,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    chunks: list[pd.DataFrame] = []
    with engine.connect() as connection:
        statement = text(
            """
            SELECT
                CAST(LEFT(stock_code, 6) AS UNSIGNED)
                    AS stock_code_number,
                trade_date,
                open, close, high, low, pre_close, amount, change_pct,
                CASE
                    WHEN UPPER(COALESCE(short_name, '')) LIKE :st_pattern
                      OR COALESCE(short_name, '') LIKE :delist_pattern
                    THEN 1
                    ELSE 0
                END AS name_excluded
            FROM sm_stock_kline
            WHERE k_type = 1
              AND trade_date BETWEEN :start_date AND :end_date
              AND (
                  stock_code LIKE '00%%'
                  OR stock_code LIKE '30%%'
                  OR stock_code LIKE '60%%'
                  OR stock_code LIKE '68%%'
                  OR stock_code LIKE '92%%'
              )
            ORDER BY stock_code, trade_date
            """
        )
        iterator = read_frame_chunks(
            statement,
            connection.execution_options(stream_results=True),
            params={
                "start_date": start_date - timedelta(days=150),
                "end_date": end_date + timedelta(days=60),
                "st_pattern": "%ST%",
                "delist_pattern": "%退%",
            },
            chunksize=100_000,
        )
        for chunk in iterator:
            chunk["trade_date"] = pd.to_datetime(
                chunk["trade_date"],
                errors="coerce",
            )
            chunk["stock_code_number"] = pd.to_numeric(
                chunk["stock_code_number"],
                errors="coerce",
                downcast="unsigned",
            )
            for column in (
                "open",
                "close",
                "high",
                "low",
                "pre_close",
                "amount",
                "change_pct",
            ):
                chunk[column] = pd.to_numeric(
                    chunk[column],
                    errors="coerce",
                ).astype("float32")
            chunk["name_excluded"] = pd.to_numeric(
                chunk["name_excluded"],
                errors="coerce",
            ).fillna(0).astype("uint8")
            chunk = chunk.dropna(
                subset=[
                    "stock_code_number",
                    "trade_date",
                    "open",
                    "close",
                    "high",
                    "low",
                    "amount",
                ]
            )
            chunks.append(chunk)
    # Whole-DataFrame concat keeps every source chunk and another full-size
    # result alive at the same time.  Move one typed column at a time so each
    # source column can be released as its final array is allocated.
    column_order = list(chunks[0].columns) if chunks else []
    combined_columns: dict[str, np.ndarray] = {}
    for column in column_order:
        values = [
            chunk.pop(column).to_numpy(copy=False)
            for chunk in chunks
        ]
        combined_columns[column] = np.concatenate(values)
        del values
    chunks.clear()
    frame = pd.DataFrame(combined_columns, copy=False)
    del combined_columns
    if frame.empty:
        raise RuntimeError("QMT 日 K 历史为空")
    category_codes, unique_codes = pd.factorize(
        frame.pop("stock_code_number"),
        sort=True,
    )
    frame["stock_code"] = pd.Categorical.from_codes(
        category_codes,
        categories=[
            f"{int(value):06d}"
            for value in unique_codes
        ],
    )
    # SQL ORDER BY plus ordered factorization establishes the rolling-feature
    # order.  Mark trusted loader output to avoid a second full-history copy.
    frame.attrs["probiga_sorted_stock_trade_date"] = True
    return frame


def _clamp(series: pd.Series) -> pd.Series:
    return series.clip(lower=0.0, upper=1.0)


def _scaled(
    series: pd.Series,
    lower: float,
    upper: float,
) -> pd.Series:
    return _clamp((series - lower) / (upper - lower))


def _band_series(
    series: pd.Series,
    lower: float,
    upper: float,
    shoulder: float,
) -> pd.Series:
    below = (series - (lower - shoulder)) / shoulder
    above = ((upper + shoulder) - series) / shoulder
    return pd.Series(
        1.0,
        index=series.index,
    ).where(series.between(lower, upper), below.where(
        series < lower,
        above,
    )).clip(lower=0.0, upper=1.0)


def _build_features(frame: pd.DataFrame) -> pd.DataFrame:
    if not frame.attrs.get("probiga_sorted_stock_trade_date", False):
        frame = frame.sort_values(["stock_code", "trade_date"]).copy()
    # MySQL DECIMAL values otherwise keep object dtype in pandas.  Arithmetic
    # on those objects can raise decimal.DivisionByZero for legitimate source
    # rows such as a zero pre-close or amount, instead of producing a value we
    # can validate and discard.  Normalize at this boundary so every feature
    # calculation uses NumPy floating-point semantics.
    numeric_columns = (
        "open",
        "close",
        "high",
        "low",
        "pre_close",
        "amount",
        "change_pct",
    )
    for column in numeric_columns:
        frame[column] = pd.to_numeric(
            frame[column],
            errors="coerce",
        ).astype("float32")
    valid_price_row = frame[[
        "open",
        "close",
        "high",
        "low",
    ]].gt(0).all(axis=1)
    if not bool(valid_price_row.all()):
        frame = frame.loc[valid_price_row].copy()
    frame["amount"] = frame["amount"].fillna(0.0).clip(lower=0.0)
    if "name_excluded" not in frame:
        names = frame.get(
            "short_name",
            pd.Series("", index=frame.index),
        ).fillna("").astype(str)
        frame["name_excluded"] = (
            names.str.upper().str.contains("ST", regex=False)
            | names.str.contains("退", regex=False)
        ).astype("uint8")
    for column in ("open", "close", "high", "low"):
        frame["raw_" + column] = frame[column]
    daily_return = (
        pd.to_numeric(frame["change_pct"], errors="coerce") / 100.0
    )
    fallback_return = frame["close"] / frame["pre_close"] - 1.0
    daily_return = daily_return.where(
        daily_return.notna(),
        fallback_return,
    ).replace([math.inf, -math.inf], math.nan).fillna(0.0)
    frame["_daily_growth"] = (1.0 + daily_return).clip(lower=0.01)
    frame["_adj_close"] = frame.groupby(
        "stock_code",
        sort=False,
        observed=True,
    )["_daily_growth"].cumprod()
    first_close = frame.groupby(
        "stock_code",
        sort=False,
        observed=True,
    )["raw_close"].transform("first")
    frame["_adj_close"] = frame["_adj_close"] * first_close
    previous_adj_close = frame.groupby(
        "stock_code",
        sort=False,
        observed=True,
    )["_adj_close"].shift(1)
    price_base = frame["pre_close"].where(
        frame["pre_close"] > 0,
        frame["raw_close"],
    )
    for column in ("open", "high", "low"):
        ratio = frame["raw_" + column] / price_base
        frame[column] = (
            previous_adj_close * ratio
        ).where(previous_adj_close.notna(), frame["_adj_close"] * (
            frame["raw_" + column] / frame["raw_close"]
        ))
    frame["close"] = frame["_adj_close"]
    del daily_return, fallback_return, first_close
    del previous_adj_close, price_base, ratio
    del frame["_daily_growth"], frame["_adj_close"], frame["pre_close"]
    groups = frame.groupby(
        "stock_code",
        sort=False,
        group_keys=False,
        observed=True,
    )
    frame["return_2d_pct"] = (
        groups["close"].pct_change(2) * 100
    ).astype("float32")
    frame["return_5d_pct"] = (
        groups["close"].pct_change(5) * 100
    ).astype("float32")
    frame["return_20d_pct"] = (
        groups["close"].pct_change(20) * 100
    ).astype("float32")
    frame["return_60d_pct"] = (
        groups["close"].pct_change(60) * 100
    ).astype("float32")
    frame["ma20"] = groups["close"].transform(
        lambda value: value.rolling(20).mean()
    ).astype("float32")
    frame["ma5"] = groups["close"].transform(
        lambda value: value.rolling(5).mean()
    ).astype("float32")
    frame["ma60"] = groups["close"].transform(
        lambda value: value.rolling(60).mean()
    ).astype("float32")
    frame["ma20_slope_5d_pct"] = (
        frame["ma20"] / groups["ma20"].shift(5) - 1.0
    ).mul(100).astype("float32")
    frame["high20"] = groups["high"].transform(
        lambda value: value.rolling(20).max()
    ).astype("float32")
    frame["breakout_20d_proximity"] = (
        frame["close"] / frame["high20"]
    ).clip(upper=1.0).astype("float32")
    frame["amount5"] = groups["amount"].transform(
        lambda value: value.rolling(5).mean()
    ).astype("float32")
    frame["amount20"] = groups["amount"].transform(
        lambda value: value.rolling(20).mean()
    ).astype("float32")
    frame["amount_ratio_5_20"] = (
        frame["amount5"] / frame["amount20"]
    ).astype("float32")
    frame["amount_ratio_1_20"] = (
        frame["amount"] / frame["amount20"]
    ).astype("float32")
    del frame["amount5"]
    market20 = frame.groupby("trade_date")[
        "return_20d_pct"
    ].median()
    market_latest = frame.groupby("trade_date")[
        "change_pct"
    ].median()
    frame["market_return_20d_pct"] = (
        frame["trade_date"].map(market20).astype("float32")
    )
    frame["market_latest_change_pct"] = frame[
        "trade_date"
    ].map(market_latest).astype("float32")
    frame["latest_relative_to_market_pct"] = (
        frame["change_pct"] - frame["market_latest_change_pct"]
    ).astype("float32")
    frame["relative_strength_20d_pct"] = (
        frame["return_20d_pct"] - frame["market_return_20d_pct"]
    ).astype("float32")
    del market20, market_latest
    frame["distance_ma20_pct"] = (
        frame["close"] / frame["ma20"] - 1.0
    ).mul(100).astype("float32")
    frame["distance_ma5_pct"] = (
        frame["close"] / frame["ma5"] - 1.0
    ).mul(100).astype("float32")
    frame["drawdown_20d_pct"] = (
        frame["close"] / frame["high20"] - 1.0
    ).mul(100).astype("float32")
    frame["rebound_from_low_pct"] = (
        frame["close"] / frame["low"] - 1.0
    ).mul(100).astype("float32")
    frame["previous_change_pct"] = groups["change_pct"].shift(1).astype(
        "float32"
    )
    frame["close_above_ma20"] = (
        frame["close"] > frame["ma20"]
    ).astype("uint8")
    frame["ma20_above_ma60"] = (
        frame["ma20"] > frame["ma60"]
    ).astype("uint8")
    previous_close = groups["close"].shift(1)
    high_values = frame["high"].to_numpy(copy=False)
    low_values = frame["low"].to_numpy(copy=False)
    previous_values = previous_close.to_numpy(copy=False)
    true_range_values = np.subtract(
        high_values,
        low_values,
    )
    np.maximum(
        true_range_values,
        np.abs(high_values - previous_values),
        out=true_range_values,
    )
    np.maximum(
        true_range_values,
        np.abs(low_values - previous_values),
        out=true_range_values,
    )
    true_range = pd.Series(true_range_values, index=frame.index)
    frame["atr14"] = true_range.groupby(
        frame["stock_code"],
        observed=True,
    ).transform(
        lambda value: value.rolling(14).mean()
    ).astype("float32")
    frame["atr_14d_pct"] = (
        frame["atr14"] / frame["close"] * 100
    ).astype("float32")
    del previous_close, previous_values, true_range, true_range_values
    del high_values, low_values
    del frame["open"], frame["high"], frame["low"]
    del frame["ma5"], frame["ma20"], frame["ma60"]
    del frame["high20"], frame["atr14"]
    trend_alignment = (
        (frame["close_above_ma20"] >= 1)
        & (frame["ma20_above_ma60"] >= 1)
    ).astype("float32")
    frame["score"] = (
        0.24 * trend_alignment
        + 0.16 * _scaled(
            frame["return_20d_pct"], 2, 22
        )
        + 0.14 * _scaled(
            frame["return_60d_pct"], 12, 55
        )
        + 0.15 * _scaled(
            frame["ma20_slope_5d_pct"], 0.2, 4
        )
        + 0.12 * _scaled(
            frame["relative_strength_20d_pct"], 2, 22
        )
        + 0.08 * _scaled(
            frame["amount_ratio_5_20"], 0.9, 1.8
        )
        + 0.07 * (
            1.0 - _scaled(
                (frame["distance_ma20_pct"] - 4.0).abs(),
                0,
                8,
            )
        )
        + 0.04 * (
            1.0 - _scaled(
                frame["atr_14d_pct"],
                1,
                5,
            )
        )
    ).clip(lower=0, upper=1).astype("float32")
    frame["initial_stop_pct"] = -(
        frame["atr_14d_pct"] * 2.2
    ).clip(lower=3.5, upper=8.0).astype("float32")
    runtime_columns = [
        "stock_code",
        "trade_date",
        "name_excluded",
        "raw_open",
        "raw_close",
        "raw_high",
        "raw_low",
        "amount",
        "change_pct",
        "close",
        "amount20",
        "market_return_20d_pct",
        "score",
        "initial_stop_pct",
        *FEATURE_COLUMNS,
    ]
    if "short_name" in frame:
        runtime_columns.insert(1, "short_name")
    keep_columns = set(runtime_columns)
    for column in tuple(frame.columns):
        if column not in keep_columns:
            del frame[column]
    for column in frame.select_dtypes(include=["float64"]).columns:
        frame[column] = frame[column].astype("float32")
    return frame


def _execution_fee(
    value: float,
    *,
    account: dict[str, Any],
    sell: bool,
) -> float:
    result = max(
        float(account["minimum_commission_cny"]),
        value * float(account["commission_rate"]),
    )
    result += value * float(account["transfer_fee_rate"])
    if sell:
        result += value * float(account["sell_stamp_duty_rate"])
    result += value * float(account["default_slippage_rate"])
    return result


def _bar_value(
    row: Any,
    raw_column: str,
    adjusted_column: str,
) -> float:
    raw_value = row.get(raw_column)
    if raw_value is not None and not pd.isna(raw_value):
        return float(raw_value)
    return float(row[adjusted_column])


def _one_price_limit_bar(
    row: Any,
    previous_row: Any,
    *,
    direction: str,
) -> bool:
    high = _bar_value(row, "raw_high", "high")
    low = _bar_value(row, "raw_low", "low")
    current_open = _bar_value(row, "raw_open", "open")
    previous_close = _bar_value(
        previous_row,
        "raw_close",
        "close",
    )
    if previous_close <= 0 or abs(high - low) > max(1e-8, previous_close * 1e-6):
        return False
    ratio = current_open / previous_close - 1.0
    return ratio >= 0.095 if direction == "up" else ratio <= -0.095


def _advance_dynamic_position(
    position: dict[str, Any],
    row: Any,
) -> dict[str, Any] | None:
    """Apply the canonical production close-decision exit rule."""

    position["holding_days"] += 1
    reason = daily_exit_reason(
        protective_stop=position["initial_stop"],
        session_low=_bar_value(row, "raw_low", "low"),
        close_above_ma20=row.get("close_above_ma20"),
        ma20_above_ma60=row.get("ma20_above_ma60"),
    )
    if reason is not None:
        return {
            "timing": "NEXT_OPEN",
            "price": None,
            "reason": reason,
        }
    return None


def _dynamic_signal_outcome(
    group: pd.DataFrame,
    *,
    signal_index: int,
    config: dict[str, Any],
    initial_stop_pct: float | None = None,
    include_censored: bool = False,
    paper_discovery: bool = False,
) -> dict[str, Any] | None:
    entry_index = signal_index + 1
    if entry_index >= len(group):
        return None
    candidate = group.iloc[entry_index]
    if float(candidate["amount"] or 0.0) <= 0:
        return None
    if _one_price_limit_bar(
        candidate,
        group.iloc[entry_index - 1],
        direction="up",
    ):
        return None
    entry = group.iloc[entry_index]
    entry_price = _bar_value(entry, "raw_open", "open")
    signal = group.iloc[signal_index]
    signal_close = _bar_value(signal, "raw_close", "close")
    execution_policy = dict(config.get("paper_execution") or {})
    maximum_entry_premium = float(
        execution_policy.get("maximum_entry_premium_pct", 0.5)
    ) / 100.0
    if (
        float(entry["amount"] or 0.0) <= 0
        or entry_price <= 0
        or entry_price > signal_close * (1.0 + maximum_entry_premium)
    ):
        return None
    account = config["account"]
    policy = config["portfolio"]
    if paper_discovery:
        # The shadow replay uses the exact same board-lot sizing policy as the
        # production paper order.  It remains shadow evidence, but commission
        # drag and economic-lot eligibility must not be calculated on a
        # fictitious 5%/10,000 CNY position.
        from .portfolio import _paper_probe_quantity

        discovery = dict(config.get("paper_discovery") or {})
        equity = float(account["initial_cash_cny"])
        desired_weight = float(discovery.get("position_weight", 0.025))
        maximum_weight = desired_weight + float(
            discovery.get("maximum_lot_rounding_overweight", 0.0)
        )
        quantity, _lot_reason = _paper_probe_quantity(
            equity=equity,
            price=entry_price,
            desired_weight=desired_weight,
            maximum_weight=maximum_weight,
            preferred_minimum_order_cny=float(
                discovery.get("minimum_order_cny", 5_000.0)
            ),
            absolute_minimum_lot_order_cny=float(
                discovery.get(
                    "absolute_minimum_lot_order_cny",
                    discovery.get("minimum_order_cny", 5_000.0),
                )
            ),
        )
        minimum_order_cny = float(
            discovery.get("absolute_minimum_lot_order_cny", 0.0)
        )
    else:
        desired = float(account["initial_cash_cny"]) * float(
            policy.get(
                "initial_probe_position_weight",
                policy["normal_position_weight"],
            )
        )
        quantity = math.floor(desired / entry_price / 100) * 100
        minimum_order_cny = float(policy["minimum_economic_order_cny"])
    entry_value = quantity * entry_price
    if (
        quantity <= 0
        or entry_value
        < minimum_order_cny
    ):
        return None
    buy_fee = _execution_fee(
        entry_value,
        account=account,
        sell=False,
    )
    position = {
        "entry_price": entry_price,
        "initial_stop": entry_price
        * (
            1.0
            + float(
                initial_stop_pct
                if initial_stop_pct is not None
                else signal["initial_stop_pct"]
            )
            / 100.0
        ),
        "holding_days": 0,
    }
    minimum_low = entry_price
    maximum_high = entry_price
    pending_reason: str | None = None
    for row_index in range(entry_index, len(group)):
        row = group.iloc[row_index]
        if float(row["amount"] or 0.0) <= 0:
            continue
        minimum_low = min(
            minimum_low,
            _bar_value(row, "raw_low", "low"),
        )
        maximum_high = max(
            maximum_high,
            _bar_value(row, "raw_high", "high"),
        )
        if pending_reason is not None:
            if _one_price_limit_bar(
                row,
                group.iloc[row_index - 1],
                direction="down",
            ):
                continue
            exit_price = _bar_value(row, "raw_open", "open")
            exit_reason = pending_reason
        else:
            directive = _advance_dynamic_position(position, row)
            if directive is None:
                continue
            if directive["timing"] == "NEXT_OPEN":
                pending_reason = str(directive["reason"])
                continue
            exit_price = float(directive["price"])
            exit_reason = str(directive["reason"])
        exit_value = quantity * exit_price
        sell_fee = _execution_fee(
            exit_value,
            account=account,
            sell=True,
        )
        net_pnl = (
            (exit_price - entry_price) * quantity
            - buy_fee
            - sell_fee
        )
        return {
            "entry_open": entry_price,
            "exit_close": exit_price,
            "entry_date": pd.Timestamp(entry["trade_date"]),
            "exit_date": pd.Timestamp(row["trade_date"]),
            "exit_reason": exit_reason,
            "holding_days": int(position["holding_days"]),
            "label_order_value_cny": entry_value,
            "net_return_pct": net_pnl / entry_value * 100.0,
            "mae_pct": (
                minimum_low / entry_price - 1.0
            )
            * 100.0,
            "mfe_pct": (
                maximum_high / entry_price - 1.0
            )
            * 100.0,
            "label_mature": True,
        }
    # An open trend is right-censored, not force-labelled with a convenient
    # end-of-window close.  It must still remain an entry candidate during
    # portfolio replay; otherwise future knowledge of whether it eventually
    # exits changes today's simulated decision.
    if not include_censored:
        return None
    return {
        "entry_open": entry_price,
        "exit_close": math.nan,
        "entry_date": pd.Timestamp(entry["trade_date"]),
        "exit_date": pd.NaT,
        "exit_reason": "RIGHT_CENSORED",
        "holding_days": int(position["holding_days"]),
        "label_order_value_cny": entry_value,
        "net_return_pct": math.nan,
        "mae_pct": math.nan,
        "mfe_pct": math.nan,
        "label_mature": False,
    }


def _signal_samples(
    frame: pd.DataFrame,
    *,
    start_date: date,
    end_date: date,
    horizon_days: int,
    top_per_day: int,
) -> pd.DataFrame:
    if horizon_days <= 0:
        raise ValueError("horizon_days must be positive")
    excluded_names = frame["name_excluded"].fillna(0).astype(bool)
    if "short_name" in frame:
        names = frame["short_name"].fillna("").astype(str)
        excluded_names = (
            excluded_names
            | names.str.upper().str.contains("ST", regex=False)
            | names.str.contains("退", regex=False)
        )
    eligible = frame[
        (frame["trade_date"].dt.date >= start_date)
        & (frame["trade_date"].dt.date <= end_date)
        & (frame["amount20"] >= 50_000_000)
        & (frame["amount"] > 0)
        & (frame["close"] >= 2)
        & (frame["close_above_ma20"] == 1)
        & (frame["ma20_above_ma60"] == 1)
        & (
            frame["market_return_20d_pct"]
            >= MINIMUM_MARKET_RETURN_20D_PCT
        )
        & frame["return_60d_pct"].between(*RETURN_60D_RANGE)
        & frame["return_20d_pct"].between(*RETURN_20D_RANGE)
        & frame["ma20_slope_5d_pct"].between(
            *MA20_SLOPE_5D_RANGE
        )
        & frame["distance_ma20_pct"].between(*DISTANCE_MA20_RANGE)
        & frame["amount_ratio_5_20"].between(
            *AMOUNT_RATIO_5_20_RANGE
        )
        & (frame["change_pct"] < MAXIMUM_LATEST_CHANGE_PCT)
        & (~excluded_names)
    ].copy()
    eligible["daily_rank"] = eligible.groupby("trade_date")[
        "score"
    ].rank(method="first", ascending=False)
    selected = eligible[
        eligible["daily_rank"] <= top_per_day
    ].copy()
    if "short_name" not in selected:
        selected["short_name"] = selected["stock_code"].astype(str)
    selected_by_code = {
        str(code): group.to_dict("records")
        for code, group in selected.groupby(
            "stock_code",
            sort=False,
            observed=True,
        )
    }
    config = load_v3_config()
    records: list[dict[str, Any]] = []
    for code_value, code_group in frame.groupby(
        "stock_code",
        sort=False,
        observed=True,
    ):
        code = str(code_value)
        candidates = selected_by_code.get(code)
        if not candidates:
            continue
        group = code_group.reset_index(drop=True)
        locations = {
            pd.Timestamp(trade_date): index
            for index, trade_date in enumerate(group["trade_date"])
        }
        for item in candidates:
            signal_date = pd.Timestamp(item["trade_date"])
            signal_index = locations.get(signal_date)
            if signal_index is None:
                continue
            outcome = _dynamic_signal_outcome(
                group,
                signal_index=signal_index,
                config=config,
                include_censored=True,
            )
            if outcome is None:
                continue
            records.append({**item, **outcome})
    if not records:
        return selected.iloc[0:0].assign(
            net_return_pct=pd.Series(dtype="float64"),
            mae_pct=pd.Series(dtype="float64"),
            mfe_pct=pd.Series(dtype="float64"),
            exit_date=pd.Series(dtype="datetime64[ns]"),
            label_mature=pd.Series(dtype="bool"),
        )
    return pd.DataFrame(records)


def _signal_eligibility_funnel(
    frame: pd.DataFrame,
    *,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    """Explain every cumulative drop before daily Top-N selection."""

    excluded_names = frame["name_excluded"].fillna(0).astype(bool)
    if "short_name" in frame:
        names = frame["short_name"].fillna("").astype(str)
        excluded_names = (
            excluded_names
            | names.str.upper().str.contains("ST", regex=False)
            | names.str.contains("\u9000", regex=False)
        )
    stages = [
        (
            "period",
            (frame["trade_date"].dt.date >= start_date)
            & (frame["trade_date"].dt.date <= end_date),
        ),
        (
            "liquidity",
            (frame["amount20"] >= 50_000_000)
            & (frame["amount"] > 0),
        ),
        ("minimum_price", frame["close"] >= 2),
        ("close_above_ma20", frame["close_above_ma20"] == 1),
        ("ma20_above_ma60", frame["ma20_above_ma60"] == 1),
        (
            "market_regime",
            frame["market_return_20d_pct"]
            >= MINIMUM_MARKET_RETURN_20D_PCT,
        ),
        (
            "return_60d_range",
            frame["return_60d_pct"].between(*RETURN_60D_RANGE),
        ),
        (
            "return_20d_range",
            frame["return_20d_pct"].between(*RETURN_20D_RANGE),
        ),
        (
            "ma20_slope_range",
            frame["ma20_slope_5d_pct"].between(
                *MA20_SLOPE_5D_RANGE
            ),
        ),
        (
            "distance_ma20_range",
            frame["distance_ma20_pct"].between(
                *DISTANCE_MA20_RANGE
            ),
        ),
        (
            "amount_ratio_range",
            frame["amount_ratio_5_20"].between(
                *AMOUNT_RATIO_5_20_RANGE
            ),
        ),
        (
            "latest_change_cap",
            frame["change_pct"] < MAXIMUM_LATEST_CHANGE_PCT,
        ),
        ("name_eligible", ~excluded_names),
    ]
    mask = pd.Series(True, index=frame.index, dtype=bool)
    previous_count = len(frame)
    funnel: list[dict[str, Any]] = []
    for stage, condition in stages:
        mask &= condition.fillna(False).astype(bool)
        selected = frame.loc[mask, ["stock_code", "trade_date"]]
        row_count = len(selected)
        funnel.append({
            "stage": stage,
            "row_count": row_count,
            "dropped_count": previous_count - row_count,
            "stock_count": int(selected["stock_code"].nunique()),
            "trade_day_count": int(selected["trade_date"].nunique()),
        })
        previous_count = row_count
    return funnel


def _validated_bucket(
    table: CalibrationTable,
    score: float,
) -> bool:
    bucket = table.bucket_for(score)
    if not bucket:
        return False
    gate = load_v3_config()["profit_gate"]
    return (
        bucket.sample_count >= int(gate["minimum_oos_samples"])
        and bucket.expected_return_net_pct > 0
        and bucket.profit_factor is not None
        and (
            math.isinf(bucket.profit_factor)
            or bucket.profit_factor
            >= float(gate["minimum_profit_factor"])
        )
        and bucket.payoff_ratio is not None
        and (
            math.isinf(bucket.payoff_ratio)
            or bucket.payoff_ratio
            >= float(gate["minimum_payoff_ratio"])
        )
    )


def _metrics(values: list[float]) -> dict[str, Any]:
    result = trade_metrics(values)
    result["sample_count"] = len(values)
    result["win_rate"] = (
        sum(value > 0 for value in values) / len(values)
        if values
        else None
    )
    result["total_net_return_pct"] = sum(values)
    return result


def _walk_forward_validation(
    samples: pd.DataFrame,
    *,
    training_start: date,
    validation_start: date,
    validation_end: date,
    model_version: str,
) -> tuple[
    CalibrationTable,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    """Use each trailing year to calibrate only the following month."""

    if "label_mature" not in samples:
        samples = samples.copy()
        samples["label_mature"] = samples["net_return_pct"].notna()
    accepted_candidate_parts = []
    accepted_outcome_parts = []
    bucket_count = int(
        load_v3_config().get("calibration", {}).get(
            "bucket_count",
            5,
        )
    )
    month_start = pd.Timestamp(validation_start).replace(day=1)
    final_month = pd.Timestamp(validation_end).replace(day=1)
    while month_start <= final_month:
        next_month = month_start + pd.offsets.MonthBegin(1)
        # A signal can remain open for an unbounded number of sessions.  Only
        # outcomes already completed before this cutoff enter calibration;
        # the 35-day signal embargo adds another conservative purge gap.
        train_end = month_start - pd.Timedelta(days=35)
        train_start = max(
            pd.Timestamp(training_start),
            train_end - pd.DateOffset(years=1),
        )
        train = samples[
            samples["label_mature"].fillna(False)
            & samples["net_return_pct"].notna()
            & (samples["trade_date"] >= train_start)
            & (samples["trade_date"] <= train_end)
            & (samples["exit_date"] <= train_end)
        ]
        table = fit_calibration(
            "right_side_trend",
            train[
                ["score", "net_return_pct", "mae_pct", "mfe_pct"]
            ].to_dict("records"),
            model_version=(
                f"{model_version}-calibrated-through-"
                f"{train_end.date()}"
            ),
            bucket_count=bucket_count,
        )
        # Production refuses a calibration whose higher scores rank worse.
        # Apply the identical rule during every walk-forward month so the
        # backtest cannot admit signals that the runtime model would reject.
        if not table.has_valid_score_direction():
            month_start = next_month
            continue
        month = samples[
            (samples["trade_date"] >= month_start)
            & (samples["trade_date"] < next_month)
            & (
                samples["trade_date"].dt.date
                <= validation_end
            )
        ].copy()
        if not month.empty:
            month = month[
                month["score"].map(
                    lambda value: _validated_bucket(
                        table,
                        float(value),
                    )
                )
            ]
            if not month.empty:
                accepted_candidate_parts.append(month)
                mature_month = month[
                    month["label_mature"].fillna(False)
                    & month["net_return_pct"].notna()
                ]
                if not mature_month.empty:
                    accepted_outcome_parts.append(mature_month)
        month_start = next_month
    final_cutoff = pd.Timestamp(validation_end) - pd.Timedelta(days=35)
    final_train_start = max(
        pd.Timestamp(training_start),
        final_cutoff - pd.DateOffset(years=1),
    )
    final_train = samples[
        samples["label_mature"].fillna(False)
        & samples["net_return_pct"].notna()
        & (samples["trade_date"] >= final_train_start)
        & (samples["trade_date"] <= final_cutoff)
        & (samples["exit_date"] <= final_cutoff)
    ].copy()
    final_calibration = fit_calibration(
        "right_side_trend",
        final_train[
            ["score", "net_return_pct", "mae_pct", "mfe_pct"]
        ].to_dict("records"),
        model_version=model_version,
        bucket_count=bucket_count,
    )
    if final_calibration.has_valid_score_direction():
        final_train_valid = final_train[
            final_train["score"].map(
                lambda value: _validated_bucket(
                    final_calibration,
                    float(value),
                )
            )
        ]
    else:
        final_train_valid = final_train.iloc[0:0].copy()
    accepted_candidates = (
        pd.concat(accepted_candidate_parts, ignore_index=True)
        if accepted_candidate_parts
        else samples.iloc[0:0].copy()
    )
    accepted_outcomes = (
        pd.concat(accepted_outcome_parts, ignore_index=True)
        if accepted_outcome_parts
        else samples.iloc[0:0].copy()
    )
    return (
        final_calibration,
        final_train_valid,
        accepted_outcomes,
        accepted_candidates,
    )


def _simulate_portfolio(
    features: pd.DataFrame,
    validation_signals: pd.DataFrame,
    calibration: CalibrationTable,
    *,
    start_date: date,
    end_date: date,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    config = load_v3_config()
    account = config["account"]
    policy = config["portfolio"]
    initial_cash = float(account["initial_cash_cny"])
    cash = initial_cash
    positions: dict[str, dict[str, Any]] = {}
    pending_buys: dict[pd.Timestamp, list[dict[str, Any]]] = defaultdict(list)
    pending_sells: dict[pd.Timestamp, list[str]] = defaultdict(list)
    bars = features[
        (features["trade_date"].dt.date >= start_date)
        & (features["trade_date"].dt.date <= end_date)
    ].copy()
    trading_days = [
        pd.Timestamp(value)
        for value in sorted(bars["trade_date"].unique())
    ]
    next_day = {
        trading_days[index]: trading_days[index + 1]
        for index in range(len(trading_days) - 1)
    }
    signal_by_day = {
        day: group.sort_values(
            ["score", "stock_code"],
            ascending=[False, True],
        )
        for day, group in validation_signals.groupby("trade_date")
    }
    trades: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    total_cost_cny = 0.0

    def close_position(
        code: str,
        position: dict[str, Any],
        day: pd.Timestamp,
        price: float,
        reason: str,
    ) -> None:
        nonlocal cash, total_cost_cny
        value = price * position["quantity"]
        sell_fee = _execution_fee(
            value,
            account=account,
            sell=True,
        )
        total_cost_cny += sell_fee
        cash += value - sell_fee
        net = (
            (price - position["entry_price"])
            * position["quantity"]
            - position["buy_fee"]
            - sell_fee
        )
        trades.append({
            "stock_code": code,
            "stock_name": position["stock_name"],
            "entry_date": position["entry_date"].date().isoformat(),
            "exit_date": day.date().isoformat(),
            "entry_price": round(position["entry_price"], 4),
            "exit_price": round(price, 4),
            "quantity": position["quantity"],
            "net_pnl_cny": round(net, 2),
            "net_return_pct": round(
                net
                / (
                    position["entry_price"]
                    * position["quantity"]
                )
                * 100.0,
                6,
            ),
            "exit_reason": reason,
            "holding_days": position["holding_days"],
        })
        del positions[code]

    for day, day_group in bars.groupby(
        "trade_date",
        sort=True,
        observed=True,
    ):
        day = pd.Timestamp(day)
        day_bars = day_group.set_index("stock_code")
        for code in pending_sells.pop(day, []):
            position = positions.get(code)
            if not position or code not in day_bars.index:
                continue
            if float(day_bars.loc[code]["amount"] or 0) <= 0:
                execution_day = next_day.get(day)
                if execution_day:
                    pending_sells[execution_day].append(code)
                continue
            price = _bar_value(
                day_bars.loc[code],
                "raw_open",
                "open",
            )
            close_position(
                code,
                position,
                day,
                price,
                position.get("exit_reason", "TREND_INVALIDATED"),
            )
        for candidate in pending_buys.pop(day, []):
            code = candidate["stock_code"]
            if (
                code in positions
                or code not in day_bars.index
                or len(positions) >= int(policy["maximum_positions"])
            ):
                continue
            if float(day_bars.loc[code]["amount"] or 0) <= 0:
                continue
            price = _bar_value(
                day_bars.loc[code],
                "raw_open",
                "open",
            )
            desired = initial_cash * float(
                policy.get(
                    "initial_probe_position_weight",
                    policy["normal_position_weight"],
                )
            )
            quantity = math.floor(desired / price / 100) * 100
            value = quantity * price
            buy_fee = _execution_fee(
                value,
                account=account,
                sell=False,
            )
            if (
                quantity <= 0
                or value < float(policy["minimum_economic_order_cny"])
                or value + buy_fee > cash
            ):
                continue
            cash -= value + buy_fee
            total_cost_cny += buy_fee
            positions[code] = {
                "stock_name": candidate["short_name"],
                "quantity": quantity,
                "entry_price": price,
                "entry_date": day,
                "buy_fee": buy_fee,
                "initial_stop": price
                * (1.0 + candidate["initial_stop_pct"] / 100.0),
                "holding_days": 0,
            }
        for code, position in list(positions.items()):
            if code not in day_bars.index:
                continue
            row = day_bars.loc[code]
            if float(row["amount"] or 0) <= 0:
                continue
            directive = _advance_dynamic_position(position, row)
            if directive is not None:
                execution_day = next_day.get(day)
                if execution_day:
                    pending_sells[execution_day].append(code)
                    position["exit_reason"] = str(
                        directive["reason"]
                    )
        execution_day = next_day.get(day)
        if execution_day:
            candidates = signal_by_day.get(day)
            if candidates is not None:
                available = max(
                    0,
                    int(policy["maximum_positions"]) - len(positions),
                )
                for row in candidates.itertuples():
                    if available <= 0:
                        break
                    if row.stock_code in positions:
                        continue
                    pending_buys[execution_day].append({
                        "stock_code": row.stock_code,
                        "short_name": row.short_name,
                        "score": float(row.score),
                        "initial_stop_pct": float(row.initial_stop_pct),
                    })
                    available -= 1
        marked = cash
        for code, position in positions.items():
            if code in day_bars.index:
                marked += (
                    position["quantity"]
                    * _bar_value(
                        day_bars.loc[code],
                        "raw_close",
                        "close",
                    )
                )
        equity_curve.append({
            "trade_date": day.date().isoformat(),
            "equity": round(marked, 2),
            "cash": round(cash, 2),
            "position_count": len(positions),
        })
    final_equity = (
        equity_curve[-1]["equity"] if equity_curve else initial_cash
    )
    peak = initial_cash
    max_drawdown = 0.0
    for item in equity_curve:
        peak = max(peak, float(item["equity"]))
        drawdown = (peak - float(item["equity"])) / peak * 100.0
        max_drawdown = max(max_drawdown, drawdown)
    values = [float(item["net_return_pct"]) for item in trades]
    metrics = _metrics(values)
    metrics.update({
        "initial_cash_cny": initial_cash,
        "final_equity_cny": round(final_equity, 2),
        "net_profit_cny": round(final_equity - initial_cash, 2),
        "total_return_pct": round(
            (final_equity / initial_cash - 1.0) * 100.0,
            6,
        ),
        "maximum_drawdown_pct": round(max_drawdown, 6),
        "trade_count": len(trades),
        "total_cost_cny": round(total_cost_cny, 2),
    })
    return metrics, trades, equity_curve


def run_right_side_walk_forward(
    kline_engine: Engine,
    *,
    training_start: date,
    training_end: date,
    validation_start: date,
    validation_end: date,
    model_version: str,
) -> BacktestResult:
    if training_start > training_end:
        raise ValueError("training_start must not exceed training_end")
    if training_end >= validation_start:
        raise ValueError("training_end must precede validation_start")
    if validation_start > validation_end:
        raise ValueError("validation_start must not exceed validation_end")
    first_validation_month = pd.Timestamp(validation_start).replace(day=1)
    earliest_train_end = first_validation_month - pd.Timedelta(days=35)
    earliest_sample_start = max(
        pd.Timestamp(training_start),
        earliest_train_end - pd.DateOffset(years=1),
    ).date()
    history = _load_history(
        kline_engine,
        start_date=earliest_sample_start,
        end_date=validation_end,
    )
    features = _build_features(history)
    del history
    gc.collect()
    config = load_v3_config()
    signal_funnel = _signal_eligibility_funnel(
        features,
        start_date=earliest_sample_start,
        end_date=validation_end,
    )
    samples = _signal_samples(
        features,
        start_date=earliest_sample_start,
        end_date=validation_end,
        horizon_days=10,
        top_per_day=int(
            config.get("calibration", {}).get("top_per_day", 10)
        ),
    )
    portfolio_features = features[
        [
            "stock_code",
            "trade_date",
            "amount",
            "raw_open",
            "raw_close",
            "raw_low",
            "close_above_ma20",
            "ma20_above_ma60",
        ]
    ].copy()
    del features
    gc.collect()
    (
        calibration,
        training_valid,
        validation_valid,
        validation_candidates,
    ) = (
        _walk_forward_validation(
            samples,
            training_start=training_start,
            validation_start=validation_start,
            validation_end=validation_end,
            model_version=model_version,
        )
    )
    training_metrics = _metrics(
        training_valid["net_return_pct"].astype(float).tolist()
    )
    validation_metrics = _metrics(
        validation_valid["net_return_pct"].astype(float).tolist()
    )
    portfolio_metrics, portfolio_trades, equity_curve = (
        _simulate_portfolio(
            portfolio_features,
            validation_candidates,
            calibration,
            start_date=validation_start,
            end_date=validation_end,
        )
    )
    blocks = list(model_gate_failures(
        validation=validation_metrics,
        portfolio=portfolio_metrics,
        config=load_v3_config(),
    ))
    if not calibration.has_valid_score_direction():
        blocks.append("CALIBRATION_DIRECTION_FAILED")
    blocks = list(dict.fromkeys(blocks))
    feature_schema_hash = right_side_model_contract_hash(config)
    final_calibration_end = pd.Timestamp(
        validation_end
    ) - pd.Timedelta(days=35)
    final_calibration_start = max(
        pd.Timestamp(training_start),
        final_calibration_end - pd.DateOffset(years=1),
    )
    diagnostics = {
        "signal_funnel": signal_funnel,
        "eligible_before_daily_top_n_count": (
            signal_funnel[-1]["row_count"] if signal_funnel else 0
        ),
        "candidate_count": len(samples),
        "mature_candidate_count": int(
            samples["label_mature"].fillna(False).sum()
        ),
        "right_censored_candidate_count": int(
            (~samples["label_mature"].fillna(False)).sum()
        ),
        "final_calibration_bucket_count": len(calibration.buckets),
        "final_calibration_direction_valid": (
            calibration.has_valid_score_direction()
        ),
        "training_accepted_count": len(training_valid),
        "validation_accepted_candidate_count": len(
            validation_candidates
        ),
        "validation_mature_outcome_count": len(validation_valid),
        "portfolio_trade_count": int(
            portfolio_metrics.get("trade_count") or 0
        ),
    }
    return BacktestResult(
        calibration=calibration,
        training_metrics=training_metrics,
        validation_metrics=validation_metrics,
        portfolio_metrics=portfolio_metrics,
        validation_trades=portfolio_trades,
        equity_curve=equity_curve,
        gate_status="PASS" if not blocks else "BLOCK",
        block_reasons=tuple(blocks),
        feature_schema_hash=feature_schema_hash,
        periods={
            "declared_training_start": training_start.isoformat(),
            "declared_training_end": training_end.isoformat(),
            "effective_sample_start": earliest_sample_start.isoformat(),
            "validation_start": validation_start.isoformat(),
            "validation_end": validation_end.isoformat(),
            "final_calibration_start": (
                final_calibration_start.date().isoformat()
            ),
            "final_calibration_end": (
                final_calibration_end.date().isoformat()
            ),
        },
        diagnostics=diagnostics,
    )


def write_backtest_artifact(
    result: BacktestResult,
    path: Path,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(
        result.as_dict(),
        ensure_ascii=False,
        indent=2,
        default=str,
    )
    if path.exists():
        existing = path.read_text(encoding="utf-8")
        if existing != content:
            raise FileExistsError(
                f"immutable backtest artifact already exists: {path}"
            )
        return
    path.write_text(content, encoding="utf-8")
