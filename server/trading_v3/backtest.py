from __future__ import annotations

import gc
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from server.common.batch_db import read_frame_chunks
from server.common.qmt_attestation_contract import (
    ATTESTATION_PROTOCOL_VERSION,
    canonical_digest,
)
from server.common.qmt_daily_market_truth import load_qmt_daily_market_truth
from server.common.pit_execution_guard import (
    daily_bar_execution_disposition,
    nonlinear_impact_rate,
    participation_capped_quantity,
)

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

HISTORICAL_EXECUTION_PROTOCOL = (
    "PIT_DAILY_BAR_CAPACITY_IMPACT_FAIL_CLOSED_V1"
)
DERIVED_CHANGE_PCT_PROTOCOL = (
    "ATTESTED_NATIVE_CLOSE_DIV_ATTESTED_NATIVE_PRE_CLOSE_MINUS_ONE_X100_V1"
)


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
    market_data_truth: dict[str, Any]
    validation_hash: str
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
            "market_data_truth": self.market_data_truth,
            "validation_hash": self.validation_hash,
            "periods": self.periods,
            "diagnostics": self.diagnostics,
            "validation_protocol": {
                "name": "PURGED_MONTHLY_WALK_FORWARD",
                "calibration_window": "TRAILING_ONE_YEAR",
                "signal_embargo_calendar_days": 35,
                "same_stock_calibration_cooldown_sessions": 5,
                "overlapping_labels_count_toward_profit_gate": False,
                "label_protocol": RIGHT_SIDE_LABEL_PROTOCOL,
                "right_censored_candidates_in_portfolio": True,
                "execution_protocol": HISTORICAL_EXECUTION_PROTOCOL,
                "execution_evidence_required": True,
            },
        }


FEATURE_COLUMNS = RIGHT_SIDE_FEATURE_COLUMNS


def _derive_attested_change_pct(frame: pd.DataFrame) -> pd.Series:
    """Derive returns only from the two row-attested native QMT prices."""

    if not {"close", "pre_close"}.issubset(frame.columns):
        raise RuntimeError(
            "QMT attested close/pre_close cannot derive finite change_pct"
        )
    close = pd.to_numeric(frame.get("close"), errors="coerce")
    pre_close = pd.to_numeric(frame.get("pre_close"), errors="coerce")
    invalid = (
        close.isna()
        | pre_close.isna()
        | ~np.isfinite(close)
        | ~np.isfinite(pre_close)
        | (close <= 0)
        | (pre_close <= 0)
    )
    if bool(invalid.any()):
        raise RuntimeError(
            "QMT attested close/pre_close cannot derive finite change_pct"
        )
    result = (close / pre_close - 1.0) * 100.0
    if not bool(np.isfinite(result).all()):
        raise RuntimeError("derived QMT change_pct is non-finite")
    return result


def _bind_derived_change_pct_truth(
    market_truth: dict[str, Any],
    *,
    row_count: int,
) -> dict[str, Any]:
    """Extend immutable market truth with the deterministic return formula."""

    source_truth_hash = str(market_truth.get("truth_hash") or "")
    if len(source_truth_hash) != 64:
        raise RuntimeError("QMT source truth hash is missing")
    binding_payload = {
        "schema": "probiga.v3-derived-change-pct-binding.v1",
        "protocol": DERIVED_CHANGE_PCT_PROTOCOL,
        "source_fields": [
            "qmt_kline_attestation_row.attested_close",
            "qmt_kline_attestation_row.source_pre_close",
        ],
        "stored_change_pct_consumed": False,
        "source_market_truth_hash": source_truth_hash,
        "row_count": int(row_count),
    }
    binding = {
        **binding_payload,
        "binding_hash": canonical_digest(binding_payload),
    }
    consumer_payload = {
        "schema": "probiga.v3-market-consumer-truth.v1",
        "source_market_truth_hash": source_truth_hash,
        "derived_change_pct_binding_hash": binding["binding_hash"],
    }
    return {
        **market_truth,
        "derived_change_pct_binding": binding,
        "consumer_truth_hash": canonical_digest(consumer_payload),
    }


def _load_history(
    engine: Engine,
    *,
    start_date: date,
    end_date: date,
    decision_known_at: datetime,
) -> pd.DataFrame:
    chunks: list[pd.DataFrame] = []
    history_start = start_date - timedelta(days=150)
    history_end = end_date + timedelta(days=60)
    with engine.connect() as connection:
        market_truth = load_qmt_daily_market_truth(
            connection,
            start_date=history_start.isoformat(),
            end_date=history_end.isoformat(),
            decision_known_at=decision_known_at,
        )
        statement = text(
            """
            SELECT
                CAST(LEFT(k.stock_code, 6) AS UNSIGNED)
                    AS stock_code_number,
                k.trade_date,
                k.open, k.close, k.high, k.low, k.pre_close,
                k.volume, k.amount,
                CAST(0 AS UNSIGNED) AS name_excluded
            FROM sm_stock_kline AS k
            JOIN qmt_stock_catalog_member AS member
              ON member.batch_id=:catalog_batch_id
             AND member.stock_code=LEFT(k.stock_code, 6)
             AND member.instrument_type='STOCK'
             AND member.list_date<=k.trade_date
             AND (member.expire_date IS NULL OR member.expire_date>k.trade_date)
            WHERE k.k_type = 1 AND k.adjust_type=0
              AND k.trade_date BETWEEN :start_date AND :end_date
              AND EXISTS (
                  SELECT 1 FROM qmt_kline_attestation_row AS attestation
                  WHERE attestation.target_id=k.id
                    AND BINARY attestation.run_id=BINARY :selected_run_id
                    AND BINARY attestation.protocol_version=
                        BINARY :protocol_version
                    AND attestation.created_at<=:run_finished_at
                    AND BINARY attestation.source_data_version=
                        BINARY k.data_version
                    AND BINARY attestation.source_pre_close_origin=
                        BINARY 'NATIVE_QMT'
                    AND attestation.trade_date=k.trade_date
                    AND attestation.stock_code=LEFT(k.stock_code, 6)
                    AND attestation.source_pre_close=k.pre_close
                    AND attestation.attested_open=k.open
                    AND attestation.attested_close=k.close
                    AND attestation.attested_high=k.high
                    AND attestation.attested_low=k.low
                    AND attestation.attested_volume=k.volume
                    AND attestation.attested_amount=k.amount
              )
            ORDER BY k.stock_code, k.trade_date
            """
        )
        iterator = read_frame_chunks(
            statement,
            connection.execution_options(stream_results=True),
            params={
                "start_date": history_start,
                "end_date": history_end,
                "catalog_batch_id": market_truth.catalog_batch_id,
                "protocol_version": ATTESTATION_PROTOCOL_VERSION,
                "selected_run_id": market_truth.run_id,
                "run_finished_at": market_truth.run_finished_at,
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
                "volume",
                "amount",
            ):
                chunk[column] = pd.to_numeric(
                    chunk[column],
                    errors="coerce",
                ).astype("float32")
            # ``sm_stock_kline.change_pct`` is outside the immutable row
            # attestation contract.  Never select or trust it: close and
            # native pre_close are attested exactly above, so the return is a
            # deterministic consumer-side derivation.
            chunk["change_pct"] = _derive_attested_change_pct(chunk).astype(
                "float32"
            )
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
                    "pre_close",
                    "volume",
                    "amount",
                    "change_pct",
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
    frame.attrs["qmt_daily_market_truth"] = _bind_derived_change_pct_truth(
        market_truth.as_dict(),
        row_count=len(frame),
    )
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
        "volume",
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
    for column in ("open", "close", "high", "low", "pre_close"):
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
        "raw_pre_close",
        "volume",
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


def _raw_execution_bar(row: Any) -> dict[str, Any]:
    """Project one feature row back to its unadjusted daily execution facts."""

    return {
        "open": row.get("raw_open"),
        "high": row.get("raw_high"),
        "low": row.get("raw_low"),
        "close": row.get("raw_close"),
        "pre_close": row.get("raw_pre_close"),
        "volume": row.get("volume"),
        "amount": row.get("amount"),
    }


def _execution_fee_with_impact(
    value: float,
    *,
    account: dict[str, Any],
    sell: bool,
    participation_rate: float,
    maximum_participation_rate: float,
) -> tuple[float, float]:
    """Return configured fees plus a nonlinear turnover-impact surcharge."""

    impact_rate = nonlinear_impact_rate(
        participation_rate=participation_rate,
        maximum_participation_rate=maximum_participation_rate,
        base_slippage_rate=float(account["default_slippage_rate"]),
    )
    impact_cny = value * impact_rate
    return (
        _execution_fee(value, account=account, sell=sell) + impact_cny,
        impact_cny,
    )


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
    entry_disposition = daily_bar_execution_disposition(
        _raw_execution_bar(candidate),
        side="BUY",
    )
    if entry_disposition["executable"] is not True:
        return None
    entry = group.iloc[entry_index]
    entry_price = float(entry_disposition["open_price"])
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
    optimizer = dict(
        (config.get("decision_intelligence") or {}).get(
            "portfolio_optimizer"
        ) or {}
    )
    maximum_participation_rate = float(
        optimizer.get("maximum_participation_rate", 0.05)
    )
    board_lot = int(optimizer.get("board_lot", 100))
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
        quantity = math.floor(desired / entry_price / board_lot) * board_lot
        minimum_order_cny = float(policy["minimum_economic_order_cny"])
    capacity = participation_capped_quantity(
        desired_notional_cny=quantity * entry_price,
        price=entry_price,
        daily_amount_cny=float(entry_disposition["daily_amount_cny"]),
        maximum_participation_rate=maximum_participation_rate,
        board_lot=board_lot,
    )
    if capacity.get("valid") is not True:
        return None
    if int(capacity.get("quantity") or 0) < quantity:
        # The requested quantity was fixed from signal-time information.  Do
        # not resize it with next-session full-day turnover known only later.
        return None
    entry_value = quantity * entry_price
    if (
        quantity <= 0
        or entry_value
        < minimum_order_cny
    ):
        return None
    entry_participation_rate = (
        entry_value / float(entry_disposition["daily_amount_cny"])
        if entry_value > 0 else 0.0
    )
    buy_fee, _buy_impact = _execution_fee_with_impact(
        entry_value,
        account=account,
        sell=False,
        participation_rate=entry_participation_rate,
        maximum_participation_rate=maximum_participation_rate,
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
        holding_truth = daily_bar_execution_disposition(
            _raw_execution_bar(row),
            side="SELL",
        )
        if holding_truth["status"] == "DATA_BLOCKED":
            if not include_censored:
                return None
            return {
                "entry_open": entry_price,
                "exit_close": math.nan,
                "entry_date": pd.Timestamp(entry["trade_date"]),
                "exit_date": pd.NaT,
                "exit_reason": str(holding_truth["reason"]),
                "holding_days": int(position["holding_days"]),
                "label_order_value_cny": entry_value,
                "net_return_pct": math.nan,
                "mae_pct": math.nan,
                "mfe_pct": math.nan,
                "label_mature": False,
                "execution_status": "DATA_BLOCKED",
                "execution_evidence_valid": False,
            }
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
            exit_disposition = daily_bar_execution_disposition(
                _raw_execution_bar(row),
                side="SELL",
            )
            if exit_disposition["executable"] is not True:
                if exit_disposition["status"] == "DATA_BLOCKED":
                    if not include_censored:
                        return None
                    return {
                        "entry_open": entry_price,
                        "exit_close": math.nan,
                        "entry_date": pd.Timestamp(entry["trade_date"]),
                        "exit_date": pd.NaT,
                        "exit_reason": str(exit_disposition["reason"]),
                        "holding_days": int(position["holding_days"]),
                        "label_order_value_cny": entry_value,
                        "net_return_pct": math.nan,
                        "mae_pct": math.nan,
                        "mfe_pct": math.nan,
                        "label_mature": False,
                        "execution_status": "DATA_BLOCKED",
                        "execution_evidence_valid": False,
                    }
                continue
            exit_price = float(exit_disposition["open_price"])
            exit_daily_amount = float(
                exit_disposition["daily_amount_cny"]
            )
            exit_reason = pending_reason
        else:
            directive = _advance_dynamic_position(position, row)
            if directive is None:
                continue
            if directive["timing"] == "NEXT_OPEN":
                pending_reason = str(directive["reason"])
                continue
            exit_price = float(directive["price"])
            exit_daily_amount = float(row["amount"])
            exit_reason = str(directive["reason"])
        exit_value = quantity * exit_price
        exit_participation_rate = (
            exit_value / exit_daily_amount
        )
        if exit_participation_rate > maximum_participation_rate + 1e-12:
            continue
        sell_fee, _sell_impact = _execution_fee_with_impact(
            exit_value,
            account=account,
            sell=True,
            participation_rate=exit_participation_rate,
            maximum_participation_rate=maximum_participation_rate,
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
            "execution_status": "FILLED",
            "execution_evidence_valid": True,
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
        "execution_status": "RIGHT_CENSORED",
        "execution_evidence_valid": True,
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
        str(code): group.sort_values("trade_date").to_dict("records")
        for code, group in selected.groupby(
            "stock_code",
            sort=False,
            observed=True,
        )
    }
    market_dates = [
        pd.Timestamp(value)
        for value in sorted(frame["trade_date"].unique())
    ]
    next_market_date = {
        market_dates[index]: market_dates[index + 1]
        for index in range(len(market_dates) - 1)
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
        calibration_blocked_through_index = -1
        for item in candidates:
            signal_date = pd.Timestamp(item["trade_date"])
            signal_index = locations.get(signal_date)
            if signal_index is None:
                continue
            expected_entry_date = next_market_date.get(signal_date)
            if expected_entry_date is None:
                records.append({
                    **item,
                    "entry_open": math.nan,
                    "exit_close": math.nan,
                    "entry_date": pd.NaT,
                    "exit_date": pd.NaT,
                    "exit_reason": "NO_NEXT_SESSION_FOR_ENTRY",
                    "holding_days": 0,
                    "label_order_value_cny": math.nan,
                    "net_return_pct": math.nan,
                    "mae_pct": math.nan,
                    "mfe_pct": math.nan,
                    "label_mature": False,
                    "execution_status": "RIGHT_CENSORED",
                    "execution_evidence_valid": True,
                    "calibration_eligible": False,
                })
                continue
            expected_entry_index = locations.get(expected_entry_date)
            if expected_entry_index != signal_index + 1:
                records.append({
                    **item,
                    "entry_open": math.nan,
                    "exit_close": math.nan,
                    "entry_date": expected_entry_date,
                    "exit_date": pd.NaT,
                    "exit_reason": "MISSING_EXPECTED_ENTRY_BAR",
                    "holding_days": 0,
                    "label_order_value_cny": math.nan,
                    "net_return_pct": math.nan,
                    "mae_pct": math.nan,
                    "mfe_pct": math.nan,
                    "label_mature": False,
                    "execution_status": "DATA_BLOCKED",
                    "execution_evidence_valid": False,
                    "calibration_eligible": False,
                })
                continue
            outcome = _dynamic_signal_outcome(
                group,
                signal_index=signal_index,
                config=config,
                include_censored=True,
            )
            if outcome is None:
                entry_row = group.iloc[expected_entry_index]
                entry_disposition = daily_bar_execution_disposition(
                    _raw_execution_bar(entry_row),
                    side="BUY",
                )
                status = str(entry_disposition["status"])
                reason = str(entry_disposition["reason"])
                if entry_disposition["executable"] is True:
                    status = "KNOWN_UNFILLED"
                    reason = "ENTRY_POLICY_OR_CAPACITY_REJECTED"
                records.append({
                    **item,
                    "entry_open": math.nan,
                    "exit_close": math.nan,
                    "entry_date": expected_entry_date,
                    "exit_date": pd.NaT,
                    "exit_reason": reason,
                    "holding_days": 0,
                    "label_order_value_cny": math.nan,
                    "net_return_pct": math.nan,
                    "mae_pct": math.nan,
                    "mfe_pct": math.nan,
                    "label_mature": False,
                    "execution_status": status,
                    "execution_evidence_valid": status != "DATA_BLOCKED",
                    "calibration_eligible": False,
                })
                continue
            entry_day = pd.Timestamp(outcome.get("entry_date"))
            outcome_end = (
                pd.Timestamp(outcome.get("exit_date"))
                if bool(outcome.get("label_mature"))
                else market_dates[-1]
            )
            if not pd.isna(entry_day) and not pd.isna(outcome_end):
                expected_holding_dates = {
                    value for value in market_dates
                    if entry_day <= value <= outcome_end
                }
                missing_holding_dates = sorted(
                    expected_holding_dates - set(locations)
                )
                if missing_holding_dates:
                    outcome = {
                        **outcome,
                        "exit_close": math.nan,
                        "exit_date": pd.NaT,
                        "exit_reason": "MISSING_HOLDING_BAR",
                        "net_return_pct": math.nan,
                        "mae_pct": math.nan,
                        "mfe_pct": math.nan,
                        "label_mature": False,
                        "execution_status": "DATA_BLOCKED",
                        "execution_evidence_valid": False,
                        "missing_holding_session_count": len(
                            missing_holding_dates
                        ),
                        "first_missing_holding_session": (
                            missing_holding_dates[0].date().isoformat()
                        ),
                    }
            calibration_eligible = bool(
                outcome.get("label_mature")
                and signal_index > calibration_blocked_through_index
            )
            outcome["calibration_eligible"] = calibration_eligible
            if calibration_eligible:
                exit_index = locations.get(
                    pd.Timestamp(outcome["exit_date"]), signal_index
                )
                # Prevent one trend episode from contributing many highly
                # dependent daily signals to sample count and calibration.
                calibration_blocked_through_index = min(
                    len(group) - 1,
                    exit_index + 5,
                )
            records.append({**item, **outcome})
    if not records:
        return selected.iloc[0:0].assign(
            net_return_pct=pd.Series(dtype="float64"),
            mae_pct=pd.Series(dtype="float64"),
            mfe_pct=pd.Series(dtype="float64"),
            exit_date=pd.Series(dtype="datetime64[ns]"),
            label_mature=pd.Series(dtype="bool"),
            execution_status=pd.Series(dtype="object"),
            execution_evidence_valid=pd.Series(dtype="bool"),
            calibration_eligible=pd.Series(dtype="bool"),
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
        and math.isfinite(float(bucket.profit_factor))
        and bucket.profit_factor >= float(gate["minimum_profit_factor"])
        and bucket.payoff_ratio is not None
        and math.isfinite(float(bucket.payoff_ratio))
        and bucket.payoff_ratio >= float(gate["minimum_payoff_ratio"])
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
    if "calibration_eligible" not in samples:
        samples = samples.copy()
        samples["calibration_eligible"] = True
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
            & samples["calibration_eligible"].fillna(False)
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
                    & month["calibration_eligible"].fillna(False)
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
        & samples["calibration_eligible"].fillna(False)
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
    """Replay the portfolio with complete, fail-closed order dispositions."""

    config = load_v3_config()
    account = config["account"]
    policy = config["portfolio"]
    optimizer = dict(
        (config.get("decision_intelligence") or {}).get(
            "portfolio_optimizer"
        ) or {}
    )
    maximum_participation_rate = float(
        optimizer.get("maximum_participation_rate", 0.05)
    )
    board_lot = int(optimizer.get("board_lot", 100))
    if not 0 < maximum_participation_rate <= 1:
        raise ValueError("maximum_participation_rate must be in (0, 1]")
    if board_lot < 1:
        raise ValueError("board_lot must be positive")
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
    ordered_signals = validation_signals.copy().reset_index(drop=True)
    ordered_signals["_signal_id"] = [
        f"{index}:{pd.Timestamp(row.trade_date).date().isoformat()}:"
        f"{row.stock_code}"
        for index, row in enumerate(ordered_signals.itertuples())
    ]
    signal_by_day = {
        day: group.sort_values(
            ["score", "stock_code"],
            ascending=[False, True],
        )
        for day, group in ordered_signals.groupby("trade_date")
    }
    trades: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    total_cost_cny = 0.0
    total_impact_cny = 0.0
    status_counts: dict[str, int] = defaultdict(int)
    reason_counts: dict[str, int] = defaultdict(int)
    disposition_examples: list[dict[str, Any]] = []
    resolved_signal_ids: set[str] = set()
    data_gap_keys: set[tuple[str, str, str]] = set()
    duplicate_bar_count = int(
        bars.duplicated(["stock_code", "trade_date"]).sum()
    )

    def record_disposition(
        *,
        status: str,
        reason: str,
        side: str,
        code: str,
        day: pd.Timestamp | None,
        signal_id: str | None = None,
    ) -> None:
        status_counts[status] += 1
        reason_counts[reason] += 1
        if len(disposition_examples) < 25:
            disposition_examples.append({
                "status": status,
                "reason": reason,
                "side": side,
                "stock_code": code,
                "trade_date": (
                    day.date().isoformat() if day is not None else None
                ),
                "signal_id": signal_id,
            })

    def resolve_signal(
        candidate: dict[str, Any],
        *,
        status: str,
        reason: str,
        day: pd.Timestamp | None,
    ) -> None:
        signal_id = str(candidate["signal_id"])
        if signal_id in resolved_signal_ids:
            return
        resolved_signal_ids.add(signal_id)
        record_disposition(
            status=status,
            reason=reason,
            side="BUY",
            code=str(candidate["stock_code"]),
            day=day,
            signal_id=signal_id,
        )

    def record_data_gap(
        *,
        code: str,
        day: pd.Timestamp,
        reason: str,
        side: str,
    ) -> None:
        key = (code, day.date().isoformat(), reason)
        if key in data_gap_keys:
            return
        data_gap_keys.add(key)
        record_disposition(
            status="DATA_BLOCKED",
            reason=reason,
            side=side,
            code=code,
            day=day,
        )

    def day_bar(day_bars: pd.DataFrame, code: str) -> Any | None:
        if code not in day_bars.index:
            return None
        row = day_bars.loc[code]
        if isinstance(row, pd.DataFrame):
            return None
        return row

    def close_position(
        code: str,
        position: dict[str, Any],
        day: pd.Timestamp,
        price: float,
        reason: str,
        participation_rate: float,
    ) -> None:
        nonlocal cash, total_cost_cny, total_impact_cny
        value = price * position["quantity"]
        sell_fee, sell_impact = _execution_fee_with_impact(
            value,
            account=account,
            sell=True,
            participation_rate=participation_rate,
            maximum_participation_rate=maximum_participation_rate,
        )
        total_cost_cny += sell_fee
        total_impact_cny += sell_impact
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
            "entry_participation_rate": round(
                float(position["entry_participation_rate"]), 8
            ),
            "exit_participation_rate": round(
                participation_rate, 8
            ),
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
            if not position:
                continue
            row = day_bar(day_bars, code)
            disposition = daily_bar_execution_disposition(
                _raw_execution_bar(row) if row is not None else None,
                side="SELL",
            )
            if disposition["executable"] is not True:
                if disposition["status"] == "DATA_BLOCKED":
                    record_data_gap(
                        code=code,
                        day=day,
                        reason=str(disposition["reason"]),
                        side="SELL",
                    )
                else:
                    record_disposition(
                        status=str(disposition["status"]),
                        reason=str(disposition["reason"]),
                        side="SELL",
                        code=code,
                        day=day,
                    )
                execution_day = next_day.get(day)
                if execution_day:
                    pending_sells[execution_day].append(code)
                continue
            price = float(disposition["open_price"])
            value = price * int(position["quantity"])
            participation_rate = value / float(
                disposition["daily_amount_cny"]
            )
            if participation_rate > maximum_participation_rate + 1e-12:
                record_disposition(
                    status="KNOWN_UNFILLED",
                    reason="EXIT_CAPACITY_EXCEEDED",
                    side="SELL",
                    code=code,
                    day=day,
                )
                execution_day = next_day.get(day)
                if execution_day:
                    pending_sells[execution_day].append(code)
                continue
            close_position(
                code,
                position,
                day,
                price,
                position.get("exit_reason", "TREND_INVALIDATED"),
                participation_rate,
            )
            record_disposition(
                status="FILLED",
                reason="SELL_FILLED",
                side="SELL",
                code=code,
                day=day,
            )
        for candidate in pending_buys.pop(day, []):
            code = str(candidate["stock_code"])
            if code in positions:
                resolve_signal(
                    candidate,
                    status="PORTFOLIO_REJECTED",
                    reason="ALREADY_HELD",
                    day=day,
                )
                continue
            if len(positions) >= int(policy["maximum_positions"]):
                resolve_signal(
                    candidate,
                    status="PORTFOLIO_REJECTED",
                    reason="MAXIMUM_POSITIONS_REACHED",
                    day=day,
                )
                continue
            row = day_bar(day_bars, code)
            disposition = daily_bar_execution_disposition(
                _raw_execution_bar(row) if row is not None else None,
                side="BUY",
            )
            if disposition["executable"] is not True:
                resolve_signal(
                    candidate,
                    status=str(disposition["status"]),
                    reason=str(disposition["reason"]),
                    day=day,
                )
                if disposition["status"] == "DATA_BLOCKED":
                    data_gap_keys.add(
                        (code, day.date().isoformat(), str(disposition["reason"]))
                    )
                continue
            price = float(disposition["open_price"])
            maximum_entry_premium = float(
                (config.get("paper_execution") or {}).get(
                    "maximum_entry_premium_pct", 0.5
                )
            ) / 100.0
            signal_close = float(candidate["signal_close"])
            if price > signal_close * (1.0 + maximum_entry_premium):
                resolve_signal(
                    candidate,
                    status="KNOWN_UNFILLED",
                    reason="ENTRY_PREMIUM_LIMIT_EXCEEDED",
                    day=day,
                )
                continue
            desired = initial_cash * float(
                policy.get(
                    "initial_probe_position_weight",
                    policy["normal_position_weight"],
                )
            )
            desired_quantity = (
                math.floor(desired / price / board_lot) * board_lot
            )
            capacity = participation_capped_quantity(
                desired_notional_cny=desired,
                price=price,
                daily_amount_cny=float(disposition["daily_amount_cny"]),
                maximum_participation_rate=maximum_participation_rate,
                board_lot=board_lot,
            )
            if capacity.get("valid") is not True:
                resolve_signal(
                    candidate,
                    status="DATA_BLOCKED",
                    reason="INVALID_ENTRY_CAPACITY",
                    day=day,
                )
                data_gap_keys.add(
                    (code, day.date().isoformat(), "INVALID_ENTRY_CAPACITY")
                )
                continue
            if int(capacity.get("quantity") or 0) < desired_quantity:
                resolve_signal(
                    candidate,
                    status="KNOWN_UNFILLED",
                    reason="ENTRY_CAPACITY_EXCEEDED",
                    day=day,
                )
                continue
            quantity = desired_quantity
            value = float(quantity * price)
            participation_rate = value / float(
                disposition["daily_amount_cny"]
            )
            buy_fee, buy_impact = _execution_fee_with_impact(
                value,
                account=account,
                sell=False,
                participation_rate=participation_rate,
                maximum_participation_rate=maximum_participation_rate,
            )
            if (
                quantity <= 0
                or value < float(policy["minimum_economic_order_cny"])
            ):
                resolve_signal(
                    candidate,
                    status="KNOWN_UNFILLED",
                    reason="ENTRY_CAPACITY_BELOW_ECONOMIC_LOT",
                    day=day,
                )
                continue
            if value + buy_fee > cash:
                resolve_signal(
                    candidate,
                    status="PORTFOLIO_REJECTED",
                    reason="INSUFFICIENT_CASH",
                    day=day,
                )
                continue
            cash -= value + buy_fee
            total_cost_cny += buy_fee
            total_impact_cny += buy_impact
            positions[code] = {
                "stock_name": candidate["short_name"],
                "quantity": quantity,
                "entry_price": price,
                "entry_date": day,
                "buy_fee": buy_fee,
                "initial_stop": price
                * (1.0 + candidate["initial_stop_pct"] / 100.0),
                "holding_days": 0,
                "last_mark_price": price,
                "entry_participation_rate": participation_rate,
                "pending_exit": False,
            }
            resolve_signal(
                candidate,
                status="FILLED",
                reason=(
                    "BUY_FILLED"
                    if capacity.get("reason") == "DESIRED_NOTIONAL_ACCEPTED"
                    else "BUY_FILLED_BOARD_LOT_ROUNDED"
                ),
                day=day,
            )
        for code, position in list(positions.items()):
            row = day_bar(day_bars, code)
            if row is None:
                record_data_gap(
                    code=code,
                    day=day,
                    reason="MISSING_HELD_POSITION_BAR",
                    side="MARK",
                )
                continue
            raw_close = row.get("raw_close")
            if raw_close is None or pd.isna(raw_close) or float(raw_close) <= 0:
                record_data_gap(
                    code=code,
                    day=day,
                    reason="INVALID_HELD_POSITION_MARK",
                    side="MARK",
                )
                continue
            position["last_mark_price"] = float(raw_close)
            if float(row["amount"] or 0) <= 0:
                continue
            if position.get("pending_exit"):
                continue
            directive = _advance_dynamic_position(position, row)
            if directive is not None:
                execution_day = next_day.get(day)
                if execution_day:
                    pending_sells[execution_day].append(code)
                    position["pending_exit"] = True
                    position["exit_reason"] = str(
                        directive["reason"]
                    )
        execution_day = next_day.get(day)
        candidates = signal_by_day.get(day)
        if candidates is not None:
            available = max(
                0,
                int(policy["maximum_positions"]) - len(positions),
            )
            for row in candidates.to_dict("records"):
                candidate = {
                    "signal_id": str(row["_signal_id"]),
                    "stock_code": str(row["stock_code"]),
                    "short_name": str(row["short_name"]),
                    "score": float(row["score"]),
                    "initial_stop_pct": float(row["initial_stop_pct"]),
                    "signal_close": float(row["raw_close"]),
                }
                if execution_day is None:
                    resolve_signal(
                        candidate,
                        status="RIGHT_CENSORED",
                        reason="NO_NEXT_SESSION_FOR_ENTRY",
                        day=day,
                    )
                    continue
                if candidate["stock_code"] in positions:
                    resolve_signal(
                        candidate,
                        status="PORTFOLIO_REJECTED",
                        reason="ALREADY_HELD",
                        day=day,
                    )
                    continue
                if available <= 0:
                    resolve_signal(
                        candidate,
                        status="PORTFOLIO_REJECTED",
                        reason="MAXIMUM_POSITIONS_REACHED",
                        day=day,
                    )
                    continue
                pending_buys[execution_day].append(candidate)
                available -= 1
        marked = cash
        for code, position in positions.items():
            marked += position["quantity"] * float(
                position["last_mark_price"]
            )
        equity_curve.append({
            "trade_date": day.date().isoformat(),
            "equity": round(marked, 2),
            "cash": round(cash, 2),
            "position_count": len(positions),
        })
    for signal_id in set(ordered_signals["_signal_id"].astype(str)) - resolved_signal_ids:
        row = ordered_signals[
            ordered_signals["_signal_id"].astype(str) == signal_id
        ].iloc[0]
        candidate = {
            "signal_id": signal_id,
            "stock_code": str(row["stock_code"]),
        }
        resolve_signal(
            candidate,
            status="DATA_BLOCKED",
            reason="SIGNAL_DATE_NOT_IN_REPLAY_CALENDAR",
            day=pd.Timestamp(row["trade_date"]),
        )
        data_gap_keys.add((
            str(row["stock_code"]),
            pd.Timestamp(row["trade_date"]).date().isoformat(),
            "SIGNAL_DATE_NOT_IN_REPLAY_CALENDAR",
        ))
    unresolved_position_count = len(positions)
    for code, position in positions.items():
        record_disposition(
            status="UNRESOLVED_EXIT",
            reason=(
                "PENDING_EXIT_AT_VALIDATION_END"
                if position.get("pending_exit")
                else "OPEN_POSITION_AT_VALIDATION_END"
            ),
            side="SELL",
            code=code,
            day=(trading_days[-1] if trading_days else None),
        )
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
    provisional_trade_metrics = dict(metrics)
    expected_signal_count = len(ordered_signals)
    signal_disposition_coverage = (
        len(resolved_signal_ids) / expected_signal_count
        if expected_signal_count else 1.0
    )
    execution_evidence_valid = bool(
        duplicate_bar_count == 0
        and not data_gap_keys
        and signal_disposition_coverage == 1.0
        and unresolved_position_count == 0
        and not any(pending_buys.values())
        and not any(pending_sells.values())
    )
    if not execution_evidence_valid:
        for field in (
            "net_expectancy_pct",
            "profit_factor",
            "payoff_ratio",
            "win_rate",
            "total_net_return_pct",
        ):
            metrics[field] = None
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
        "total_nonlinear_impact_cny": round(total_impact_cny, 2),
        "maximum_participation_rate": maximum_participation_rate,
        "board_lot": board_lot,
        "expected_signal_count": expected_signal_count,
        "resolved_signal_count": len(resolved_signal_ids),
        "signal_disposition_coverage": round(
            signal_disposition_coverage, 8
        ),
        "execution_evidence_valid": execution_evidence_valid,
        "execution_status_counts": dict(sorted(status_counts.items())),
        "execution_reason_counts": dict(sorted(reason_counts.items())),
        "execution_disposition_examples": disposition_examples,
        "data_gap_count": len(data_gap_keys) + duplicate_bar_count,
        "duplicate_bar_count": duplicate_bar_count,
        "unresolved_position_count": unresolved_position_count,
        "provisional_trade_metrics": provisional_trade_metrics,
        "order_authority": False,
        "automatic_real_order_submission": False,
        "execution_protocol": HISTORICAL_EXECUTION_PROTOCOL,
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
    decision_known_at = datetime.now().replace(microsecond=0)
    history = _load_history(
        kline_engine,
        start_date=earliest_sample_start,
        end_date=validation_end,
        decision_known_at=decision_known_at,
    )
    market_data_truth = dict(history.attrs.get("qmt_daily_market_truth") or {})
    if (
        market_data_truth.get("schema")
        != "probiga.qmt-daily-market-consumer-truth.v1"
        or len(str(market_data_truth.get("truth_hash") or "")) != 64
        or len(
            str(market_data_truth.get("consumer_truth_hash") or "")
        ) != 64
    ):
        raise RuntimeError("V3 backtest lacks immutable QMT daily market truth")
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
    sample_execution_data_gap_count = int(
        (
            samples.get(
                "execution_status",
                pd.Series(index=samples.index, dtype="object"),
            ).astype(str)
            == "DATA_BLOCKED"
        ).sum()
    )
    portfolio_features = features[
        [
            "stock_code",
            "trade_date",
            "amount",
            "raw_open",
            "raw_close",
            "raw_high",
            "raw_low",
            "raw_pre_close",
            "volume",
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
    validation_metrics["execution_evidence_valid"] = (
        sample_execution_data_gap_count == 0
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
    if portfolio_metrics.get("execution_evidence_valid") is not True:
        blocks.append("PORTFOLIO_EXECUTION_EVIDENCE_INVALID")
    if sample_execution_data_gap_count > 0:
        blocks.append("SIGNAL_EXECUTION_DATA_GAPS")
    if not calibration.has_valid_score_direction():
        blocks.append("CALIBRATION_DIRECTION_FAILED")
    # Historical names in sm_stock_kline are mutable current labels, not
    # point-in-time ST/delisting facts.  Metrics remain research-only until a
    # separately immutable QMT historical status/stop-price receipt is bound.
    blocks.extend([
        "HISTORICAL_ST_STATUS_EVIDENCE_UNAVAILABLE",
        "HISTORICAL_DAILY_LIMIT_EVIDENCE_UNAVAILABLE",
    ])
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
        "non_overlapping_calibration_sample_count": int(
            (
                samples["label_mature"].fillna(False)
                & samples["calibration_eligible"].fillna(False)
            ).sum()
        ),
        "overlapping_mature_sample_excluded_count": int(
            (
                samples["label_mature"].fillna(False)
                & ~samples["calibration_eligible"].fillna(False)
            ).sum()
        ),
        "right_censored_candidate_count": int(
            (~samples["label_mature"].fillna(False)).sum()
        ),
        "signal_execution_data_gap_count": (
            sample_execution_data_gap_count
        ),
        "signal_execution_status_counts": {
            str(key): int(value)
            for key, value in samples.get(
                "execution_status",
                pd.Series(index=samples.index, dtype="object"),
            ).fillna("UNSPECIFIED").value_counts().sort_index().items()
        },
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
        "portfolio_execution_evidence_valid": (
            portfolio_metrics.get("execution_evidence_valid") is True
        ),
        "portfolio_execution_status_counts": dict(
            portfolio_metrics.get("execution_status_counts") or {}
        ),
        "historical_status_evidence": "FAIL_CLOSED_UNAVAILABLE",
        "historical_limit_price_evidence": "FAIL_CLOSED_UNAVAILABLE",
    }
    periods = {
        "declared_training_start": training_start.isoformat(),
        "declared_training_end": training_end.isoformat(),
        "effective_sample_start": earliest_sample_start.isoformat(),
        "validation_start": validation_start.isoformat(),
        "validation_end": validation_end.isoformat(),
        "final_calibration_start": final_calibration_start.date().isoformat(),
        "final_calibration_end": final_calibration_end.date().isoformat(),
    }
    validation_hash = canonical_digest({
        "schema": "probiga.v3-backtest-validation-binding.v1",
        "model_version": model_version,
        "feature_schema_hash": feature_schema_hash,
        "periods": periods,
        "market_data_truth_hash": market_data_truth[
            "consumer_truth_hash"
        ],
        "historical_status_evidence": diagnostics[
            "historical_status_evidence"
        ],
        "historical_limit_price_evidence": diagnostics[
            "historical_limit_price_evidence"
        ],
    })
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
        market_data_truth=market_data_truth,
        validation_hash=validation_hash,
        periods=periods,
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
