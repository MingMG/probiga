#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Retail-sized, event-segmented A-share backtest on the QMT history store.

The harness is deliberately explicit:

* theme membership comes from the frozen QMT sector-member snapshot;
* signals use only the current close and older observations;
* all orders execute at the next session open;
* buys respect board lots, T+1 is automatic, and limit-locked opens reject;
* commission, minimum commission, transfer fee, stamp duty and slippage apply;
* the dynamic strategy uses half/full positions and has no fixed holding limit;
* the two rigid controls reuse the current simulator's short/main-wave exits.

This is a conditional theme-capture study, not proof that a live model discovered
the theme before it became known.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.env_config import create_tool_engine, load_project_env
from server.common.kline_data import get_kline_engine
from server.common.sql_reader import read_sql_rows


INITIAL_CAPITAL = 200_000.0
LOT_SIZE = 100
COMMISSION_RATE = 0.00025
MIN_COMMISSION = 5.0
STAMP_DUTY_RATE = 0.0005
TRANSFER_FEE_RATE = 0.00001
SLIPPAGE_RATE = 0.0005
RISK_PER_TRADE = 0.006
MAX_POSITION_WEIGHT = 0.22
MAX_POSITIONS = 4
MIN_AVG_AMOUNT_20 = 50_000_000.0
ENTRY_SCORE_MIN = 75.0
ADD_SCORE_MIN = 80.0
REDUCE_SCORE = 55.0
EXIT_SCORE = 40.0


EPISODES = {
    "commercial_space": {
        "name": "商业航天",
        "start": "2026-01-05",
        "end": "2026-04-30",
        # QMT's individual commercial-space concept lists are broad.  A stock
        # is included only when it is in one of these candidate lists and is
        # independently confirmed by at least three aerospace/satellite lists.
        # SW2 aerospace equipment members enter directly.
        "direct_sectors": [
            "SW2航天装备",
        ],
        "candidate_sectors": [
            "GN商业航天",
            "GN商业航天（航天航空）",
            "TDGN商业航天",
            "TGN商业航天",
        ],
        "confirmation_sectors": [
            "GN卫星互联网",
            "GN卫星导航",
            "GN航天",
            "GN航天系",
            "GN航天航空",
            "GN航空航天",
            "GN航天军工",
        ],
        "confirmation_min": 3,
        "universe_rule": "商业航天候选且至少命中3个航天/卫星交叉板块，申万航天装备直接纳入",
    },
    "precious_metals": {
        "name": "贵金属",
        "start": "2026-01-05",
        "end": "2026-04-30",
        # Use industry classification only.  QMT's GICS4 gold list contains
        # stale/reused symbols and the generic gold concepts contain retail
        # jewellers and tangential names, so neither is admitted here.
        "direct_sectors": [
            "SW2贵金属",
            "THY2贵金属",
        ],
        "candidate_sectors": [],
        "confirmation_sectors": [],
        "confirmation_min": 0,
        "universe_rule": "仅申万/同花顺贵金属二级行业，剔除泛黄金概念和代码污染",
    },
    "technology_may_june": {
        "name": "5—6月科技",
        "start": "2026-05-06",
        "end": "2026-06-30",
        # The observed May/June technology trade was concentrated in hard
        # technology.  Restricting the pool avoids treating property, toys and
        # generic software names as interchangeable "technology".
        "direct_sectors": [
            "GN光通信设备",
            "GN半导体设备",
            "GN半导体材料",
            "SW2半导体",
        ],
        "candidate_sectors": [],
        "confirmation_sectors": [],
        "confirmation_min": 0,
        "universe_rule": "半导体行业/设备/材料与光通信设备核心硬科技池",
    },
    "july_market": {
        "name": "7月行情",
        "start": "2026-07-01",
        "end": "2026-07-24",
        "direct_sectors": [],
        "candidate_sectors": [],
        "confirmation_sectors": [],
        "confirmation_min": 0,
        "universe_rule": "仅使用6月已知流动性：6月日均成交额前1000且至少交易10日",
    },
}


MODES = {
    "dynamic": "个人动态持仓",
    "rigid_short": "现有固定短线",
    "rigid_main": "现有固定主升",
}


@dataclass
class Position:
    code: str
    name: str
    adj_units: float
    avg_entry_adj: float
    initial_stop_adj: float
    high_adj: float
    entry_date: pd.Timestamp
    stage: float
    invested_cash: float = 0.0
    returned_cash: float = 0.0
    entry_reasons: list[str] = field(default_factory=list)
    holding_sessions: int = 0
    max_profit_rate: float = 0.0


def _json_default(value: Any) -> Any:
    if isinstance(value, (pd.Timestamp, pd.Period)):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    raise TypeError(type(value).__name__)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _dataset_hash(frame: pd.DataFrame) -> str:
    columns = [
        "stock_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "volume",
        "amount",
    ]
    payload = frame[columns].to_json(
        orient="records",
        date_format="iso",
        double_precision=8,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _is_a_share(code: str) -> bool:
    return (
        len(code) == 6
        and code.isdigit()
        and code.startswith(("00", "30", "60", "68"))
    )


def _limit_ratio(code: str, name: str) -> float:
    if "ST" in str(name).upper():
        return 0.05
    if code.startswith(("30", "68")):
        return 0.20
    return 0.10


def _trade_fee(notional: float, side: str) -> float:
    if notional <= 0:
        return 0.0
    commission = max(MIN_COMMISSION, notional * COMMISSION_RATE)
    transfer = notional * TRANSFER_FEE_RATE
    stamp = notional * STAMP_DUTY_RATE if side == "sell" else 0.0
    return commission + transfer + stamp


def _load_universes(
    engine,
    kline_engine,
) -> tuple[dict[str, set[str]], dict[str, Any]]:
    sector_names = sorted({
        sector
        for episode in EPISODES.values()
        for field in (
            "direct_sectors",
            "candidate_sectors",
            "confirmation_sectors",
        )
        for sector in episode[field]
    })
    stmt = text("""
        SELECT sector_name, stock_code, batch_id, received_at, data_source
        FROM qmt_sector_member
        WHERE sector_name IN :sectors
    """).bindparams(bindparam("sectors", expanding=True))
    with engine.connect() as conn:
        rows = conn.execute(stmt, {"sectors": sector_names}).mappings().all()
    with kline_engine.connect() as conn:
        july_rows = conn.execute(text("""
            SELECT stock_code, AVG(amount) AS avg_amount, COUNT(*) AS sessions
            FROM sm_stock_kline
            WHERE trade_date BETWEEN '2026-06-01' AND '2026-06-30'
              AND k_type = 1
              AND adjust_type = 0
              AND stock_code REGEXP '^(00|30|60|68)[0-9]{4}$'
              AND amount > 0
            GROUP BY stock_code
            HAVING COUNT(*) >= 10
            ORDER BY avg_amount DESC
            LIMIT 1000
        """)).mappings().all()

    by_sector: dict[str, set[str]] = {}
    batches: set[str] = set()
    received: list[pd.Timestamp] = []
    sources: set[str] = set()
    for row in rows:
        code = str(row["stock_code"] or "").strip().zfill(6)
        if not _is_a_share(code):
            continue
        by_sector.setdefault(str(row["sector_name"]), set()).add(code)
        if row.get("batch_id"):
            batches.add(str(row["batch_id"]))
        if row.get("received_at"):
            received.append(pd.Timestamp(row["received_at"]))
        if row.get("data_source"):
            sources.add(str(row["data_source"]))

    universes: dict[str, set[str]] = {}
    for key, episode in EPISODES.items():
        if key == "july_market":
            universes[key] = {
                str(row["stock_code"]).zfill(6)
                for row in july_rows
                if _is_a_share(str(row["stock_code"]).zfill(6))
            }
        else:
            direct = set().union(
                *(
                    by_sector.get(sector, set())
                    for sector in episode["direct_sectors"]
                ),
            )
            candidates = set().union(
                *(
                    by_sector.get(sector, set())
                    for sector in episode["candidate_sectors"]
                ),
            )
            confirmation_min = int(episode["confirmation_min"])
            confirmed = {
                code
                for code in candidates
                if sum(
                    code in by_sector.get(sector, set())
                    for sector in episode["confirmation_sectors"]
                ) >= confirmation_min
            }
            universes[key] = direct | confirmed

    audit = {
        "qmt_sector_sources": sorted(sources),
        "qmt_sector_batches": sorted(batches),
        "qmt_sector_snapshot_min": (
            min(received).isoformat() if received else None
        ),
        "qmt_sector_snapshot_max": (
            max(received).isoformat() if received else None
        ),
        "universe_sizes": {
            key: len(value)
            for key, value in universes.items()
        },
        "universe_rules": {
            key: episode["universe_rule"]
            for key, episode in EPISODES.items()
        },
        "july_universe_rule": "2026-06月日均成交额前1000且至少10个交易日",
    }
    return universes, audit


def _load_prices(
    engine,
    codes: set[str],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    ordered_codes = sorted(codes)
    if not ordered_codes:
        raise RuntimeError("theme universe contains no stock codes")
    code_params = {
        f"code_{index}": stock_code
        for index, stock_code in enumerate(ordered_codes)
    }
    code_placeholders = ", ".join(f":code_{index}" for index in range(len(ordered_codes)))
    sql = f"""
        SELECT stock_code, short_name, trade_date,
               `open`, high, low, `close`, pre_close,
               volume, amount, change_pct, turnover_ratio,
               data_source, batch_id, data_version,
               quality_status, permission_status
        FROM sm_stock_kline
        WHERE stock_code IN ({code_placeholders})
          AND trade_date BETWEEN :start_date AND :end_date
          AND k_type = 1
          AND adjust_type = 0
        ORDER BY stock_code, trade_date
    """
    frame = pd.DataFrame(
        read_sql_rows(
            engine,
            sql,
            params={
                **code_params,
                "start_date": start_date,
                "end_date": end_date,
            },
            context="retail_dynamic_theme_prices",
        )
    )
    if frame.empty:
        raise RuntimeError("QMT history store returned no stock daily bars")
    frame["stock_code"] = frame["stock_code"].astype(str).str.zfill(6)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    for column in [
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "volume",
        "amount",
        "change_pct",
        "turnover_ratio",
    ]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.sort_values(["stock_code", "trade_date"]).reset_index(drop=True)
    return frame


def _prepare_features(frame: pd.DataFrame) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for _, raw in frame.groupby("stock_code", sort=False):
        group = raw.sort_values("trade_date").copy()
        previous_close = group["close"].shift(1)
        action_ratio = previous_close / group["pre_close"]
        action_ratio = action_ratio.where(
            previous_close.gt(0) & group["pre_close"].gt(0),
            1.0,
        ).fillna(1.0)
        group["adjust_factor"] = action_ratio.cumprod()
        for column in ("open", "high", "low", "close", "pre_close"):
            group[f"adj_{column}"] = group[column] * group["adjust_factor"]

        close = group["adj_close"]
        previous_adj_close = close.shift(1)
        true_range = pd.concat(
            [
                group["adj_high"] - group["adj_low"],
                (group["adj_high"] - previous_adj_close).abs(),
                (group["adj_low"] - previous_adj_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        group["ma10"] = close.rolling(10, min_periods=10).mean()
        group["ma20"] = close.rolling(20, min_periods=20).mean()
        group["ma60"] = close.rolling(60, min_periods=60).mean()
        group["ma20_slope5"] = group["ma20"] / group["ma20"].shift(5) - 1.0
        group["ret5"] = close / close.shift(5) - 1.0
        group["ret20"] = close / close.shift(20) - 1.0
        group["ret60"] = close / close.shift(60) - 1.0
        group["atr14"] = true_range.rolling(14, min_periods=14).mean()
        group["high20_prev"] = (
            group["adj_high"].rolling(20, min_periods=20).max().shift(1)
        )
        group["low10"] = group["adj_low"].rolling(10, min_periods=10).min()
        group["avg_amount20"] = group["amount"].rolling(20, min_periods=15).mean()
        group["volume_ratio"] = (
            group["volume"].rolling(5, min_periods=5).mean()
            / group["volume"].rolling(20, min_periods=15).mean()
        )
        parts.append(group)
    return pd.concat(parts, ignore_index=True)


def _episode_features(
    frame: pd.DataFrame,
    codes: set[str],
) -> pd.DataFrame:
    out = frame[frame["stock_code"].isin(codes)].copy()
    out["ret20_pct"] = out.groupby("trade_date")["ret20"].rank(
        pct=True,
        method="average",
    )
    out["ret60_pct"] = out.groupby("trade_date")["ret60"].rank(
        pct=True,
        method="average",
    )
    out["above_ma20"] = out["adj_close"] > out["ma20"]
    breadth = out.groupby("trade_date")["above_ma20"].mean()
    out["breadth"] = out["trade_date"].map(breadth)

    score = pd.Series(0.0, index=out.index)
    score += (out["adj_close"] > out["ma20"]).astype(float) * 25
    score += (out["ma20"] > out["ma60"]).astype(float) * 20
    score += (out["ma20_slope5"] > 0).astype(float) * 15
    score += (out["ret20_pct"] >= 0.70).astype(float) * 15
    score += (out["ret60_pct"] >= 0.60).astype(float) * 10
    score += (
        out["adj_close"].ge(out["high20_prev"] * 0.97)
        & out["high20_prev"].gt(0)
    ).astype(float) * 10
    score += out["volume_ratio"].between(0.80, 2.50).astype(float) * 5
    out["entry_score"] = score

    hold = pd.Series(0.0, index=out.index)
    hold += (out["adj_close"] > out["ma20"]).astype(float) * 30
    hold += (out["ma20"] > out["ma60"]).astype(float) * 20
    hold += (out["ma20_slope5"] > 0).astype(float) * 15
    hold += (out["ret20_pct"] >= 0.50).astype(float) * 15
    hold += (out["ret5"] > 0).astype(float) * 10
    hold += (out["breadth"] >= 0.45).astype(float) * 10
    out["hold_score"] = hold
    out["selection_rank"] = (
        out["entry_score"]
        + out["ret20_pct"].fillna(0) * 10
        + out["ret60_pct"].fillna(0) * 5
    )
    return out


def _episode_benchmark(
    frame: pd.DataFrame,
    episode_key: str,
) -> dict[str, Any]:
    """Build a transparent, no-cost backdrop for the frozen episode pool.

    The equal-weight reference is rebalanced daily and is not presented as an
    executable strategy.  It answers whether the chosen pool itself was rising
    while the strategy was active.
    """
    episode = EPISODES[episode_key]
    start = pd.Timestamp(episode["start"])
    end = pd.Timestamp(episode["end"])
    sample = frame.loc[
        frame["trade_date"].between(start, end),
        [
            "stock_code",
            "trade_date",
            "adj_open",
            "adj_close",
        ],
    ].copy()
    sample = sample.dropna(subset=["adj_open", "adj_close"])
    sample = sample.loc[
        sample["adj_open"].gt(0) & sample["adj_close"].gt(0)
    ].sort_values(["stock_code", "trade_date"])
    if sample.empty:
        return {
            "pool_size": 0,
            "benchmark_equal_weight_pct": None,
            "benchmark_max_drawdown_pct": None,
            "pool_median_stock_return_pct": None,
            "pool_positive_stock_pct": None,
        }

    previous = sample.groupby("stock_code")["adj_close"].shift(1)
    daily_return = sample["adj_close"] / previous - 1.0
    first_rows = previous.isna()
    daily_return.loc[first_rows] = (
        sample.loc[first_rows, "adj_close"]
        / sample.loc[first_rows, "adj_open"]
        - 1.0
    )
    sample["daily_return"] = daily_return.replace([np.inf, -np.inf], np.nan)
    daily = sample.groupby("trade_date")["daily_return"].mean().dropna()
    benchmark_equity = (1.0 + daily).cumprod()
    benchmark_drawdown = (
        benchmark_equity / benchmark_equity.cummax() - 1.0
    )

    stock_returns: list[float] = []
    for _, group in sample.groupby("stock_code"):
        if len(group) < 2:
            continue
        first_open = _safe_float(group.iloc[0]["adj_open"])
        last_close = _safe_float(group.iloc[-1]["adj_close"])
        if first_open > 0 and last_close > 0:
            stock_returns.append(last_close / first_open - 1.0)
    stock_series = pd.Series(stock_returns, dtype=float)
    return {
        "pool_size": int(sample["stock_code"].nunique()),
        "benchmark_equal_weight_pct": (
            float((benchmark_equity.iloc[-1] - 1.0) * 100.0)
            if not benchmark_equity.empty
            else None
        ),
        "benchmark_max_drawdown_pct": (
            float(benchmark_drawdown.min() * 100.0)
            if not benchmark_drawdown.empty
            else None
        ),
        "pool_median_stock_return_pct": (
            float(stock_series.median() * 100.0)
            if not stock_series.empty
            else None
        ),
        "pool_positive_stock_pct": (
            float(stock_series.gt(0).mean() * 100.0)
            if not stock_series.empty
            else None
        ),
    }


def _data_audit(
    frame: pd.DataFrame,
    expected_end: str,
) -> dict[str, Any]:
    duplicate_count = int(
        frame.duplicated(["stock_code", "trade_date"]).sum()
    )
    invalid = frame[
        frame[["open", "high", "low", "close"]]
        .isna()
        .any(axis=1)
        | frame[["open", "high", "low", "close"]]
        .le(0)
        .any(axis=1)
    ]
    ordered = frame.sort_values(["stock_code", "trade_date"]).copy()
    ordered["stock_session_number"] = ordered.groupby("stock_code").cumcount()
    missing_reference = ordered[
        ordered["pre_close"].isna() | ordered["pre_close"].le(0)
    ]
    first_dataset_date = ordered["trade_date"].min()
    noninitial_missing_reference = missing_reference[
        (missing_reference["stock_session_number"] > 0)
        | (missing_reference["trade_date"] <= first_dataset_date)
    ]
    bad_ohlc = frame[
        frame["high"].lt(frame[["open", "low", "close"]].max(axis=1))
        | frame["low"].gt(frame[["open", "high", "close"]].min(axis=1))
    ]
    latest = frame["trade_date"].max()
    source_counts = (
        frame["data_source"]
        .fillna("(legacy_untagged)")
        .replace("", "(legacy_untagged)")
        .value_counts()
        .to_dict()
    )
    quality_counts = (
        frame["quality_status"]
        .fillna("(legacy_untagged)")
        .replace("", "(legacy_untagged)")
        .value_counts()
        .to_dict()
    )
    return {
        "row_count": int(len(frame)),
        "stock_count": int(frame["stock_code"].nunique()),
        "min_trade_date": frame["trade_date"].min().date().isoformat(),
        "max_trade_date": latest.date().isoformat(),
        "expected_end_date": expected_end,
        "latest_date_matches": latest.date().isoformat() == expected_end,
        "duplicate_business_keys": duplicate_count,
        "invalid_price_rows": int(len(invalid)),
        "bad_ohlc_rows": int(len(bad_ohlc)),
        "missing_pre_close_rows": int(len(missing_reference)),
        "noninitial_missing_pre_close_rows": int(
            len(noninitial_missing_reference)
        ),
        "missing_pre_close_samples": missing_reference[
            ["stock_code", "short_name", "trade_date", "open", "close"]
        ].head(50).to_dict(orient="records"),
        "data_source_counts": {
            str(key): int(value)
            for key, value in source_counts.items()
        },
        "quality_status_counts": {
            str(key): int(value)
            for key, value in quality_counts.items()
        },
        "row_level_provenance_complete": (
            "(legacy_untagged)" not in source_counts
        ),
        "dataset_sha256": _dataset_hash(frame),
    }


def _entry_eligible(row: pd.Series) -> bool:
    name = str(row.get("short_name") or "")
    if "ST" in name.upper() or "退" in name:
        return False
    required = [
        "adj_close",
        "ma20",
        "ma60",
        "atr14",
        "ret20",
        "ret60",
        "entry_score",
        "breadth",
        "avg_amount20",
    ]
    if any(pd.isna(row.get(column)) for column in required):
        return False
    return bool(
        row["entry_score"] >= ENTRY_SCORE_MIN
        and row["breadth"] >= 0.45
        and row["avg_amount20"] >= MIN_AVG_AMOUNT_20
        and 3.0 <= row["close"]
        and 0.03 <= row["ret20"] <= 0.60
        and row["change_pct"] < 9.5
        and row["adj_close"] > row["ma20"] > row["ma60"]
    )


def _stop_and_size(row: pd.Series, equity: float) -> tuple[float, float, float]:
    close = _safe_float(row["adj_close"])
    atr = _safe_float(row["atr14"])
    natural_stop = max(
        _safe_float(row["low10"], close * 0.94),
        close - 2.5 * atr,
    )
    natural_distance = max(0.0, (close - natural_stop) / close)
    stop_distance = min(0.08, max(0.03, natural_distance))
    stop = close * (1.0 - stop_distance)
    risk_budget = equity * RISK_PER_TRADE
    max_value = min(
        equity * MAX_POSITION_WEIGHT,
        risk_budget / stop_distance,
    )
    return stop, stop_distance, max_value


def _row_map(frame: pd.DataFrame) -> dict[tuple[pd.Timestamp, str], pd.Series]:
    return {
        (pd.Timestamp(row["trade_date"]), str(row["stock_code"])): row
        for _, row in frame.iterrows()
    }


def _position_value(position: Position, row: pd.Series) -> float:
    return position.adj_units * _safe_float(row["adj_close"])


def _portfolio_equity(
    cash: float,
    positions: dict[str, Position],
    day: pd.Timestamp,
    rows: dict[tuple[pd.Timestamp, str], pd.Series],
) -> float:
    value = cash
    for code, position in positions.items():
        row = rows.get((day, code))
        if row is not None:
            value += _position_value(position, row)
    return value


def _execute_buy(
    *,
    order: dict[str, Any],
    row: pd.Series,
    cash: float,
    positions: dict[str, Position],
    mode: str,
    day: pd.Timestamp,
    fills: list[dict[str, Any]],
) -> float:
    code = order["code"]
    name = str(row.get("short_name") or code)
    raw_open = _safe_float(row.get("open"))
    pre_close = _safe_float(row.get("pre_close"))
    factor = _safe_float(row.get("adjust_factor"), 1.0)
    if raw_open <= 0 or pre_close <= 0 or factor <= 0:
        return cash
    open_return = raw_open / pre_close - 1.0
    if open_return >= _limit_ratio(code, name) - 0.002:
        fills.append({
            "date": day,
            "code": code,
            "name": name,
            "action": "REJECT_BUY",
            "reason": "开盘接近涨停，按不可成交处理",
            "mode": mode,
        })
        return cash
    signal_close = _safe_float(order.get("signal_raw_close"))
    if signal_close > 0 and raw_open / signal_close - 1.0 > 0.03:
        fills.append({
            "date": day,
            "code": code,
            "name": name,
            "action": "REJECT_BUY",
            "reason": "次日高开超过3%，执行不追高",
            "mode": mode,
        })
        return cash

    execution_price = raw_open * (1.0 + SLIPPAGE_RATE)
    requested_value = min(_safe_float(order["requested_value"]), cash)
    shares = int(requested_value / execution_price / LOT_SIZE) * LOT_SIZE
    while shares >= LOT_SIZE:
        notional = shares * execution_price
        fee = _trade_fee(notional, "buy")
        if notional + fee <= cash + 1e-8:
            break
        shares -= LOT_SIZE
    if shares < LOT_SIZE:
        fills.append({
            "date": day,
            "code": code,
            "name": name,
            "action": "REJECT_BUY",
            "reason": "现金或整手数量不足",
            "mode": mode,
        })
        return cash

    notional = shares * execution_price
    fee = _trade_fee(notional, "buy")
    adj_price = execution_price * factor
    added_units = shares / factor
    position = positions.get(code)
    if position is None:
        position = Position(
            code=code,
            name=name,
            adj_units=0.0,
            avg_entry_adj=0.0,
            initial_stop_adj=_safe_float(order["initial_stop_adj"]),
            high_adj=adj_price,
            entry_date=day,
            stage=float(order["target_stage"]),
            entry_reasons=[str(order["reason"])],
        )
        positions[code] = position
    old_units = position.adj_units
    total_units = old_units + added_units
    position.avg_entry_adj = (
        (
            position.avg_entry_adj * old_units
            + adj_price * added_units
        )
        / total_units
    )
    position.adj_units = total_units
    position.stage = max(position.stage, float(order["target_stage"]))
    position.invested_cash += notional + fee
    position.high_adj = max(position.high_adj, adj_price)
    cash -= notional + fee
    fills.append({
        "date": day,
        "signal_date": order["signal_date"],
        "code": code,
        "name": name,
        "action": order["action"],
        "price": execution_price,
        "shares": shares,
        "notional": notional,
        "fee": fee,
        "stage_after": position.stage,
        "reason": order["reason"],
        "entry_score": order.get("entry_score"),
        "hold_score": order.get("hold_score"),
        "mode": mode,
    })
    return cash


def _execute_sell(
    *,
    order: dict[str, Any],
    row: pd.Series,
    cash: float,
    positions: dict[str, Position],
    mode: str,
    day: pd.Timestamp,
    fills: list[dict[str, Any]],
    cycles: list[dict[str, Any]],
) -> float:
    code = order["code"]
    position = positions.get(code)
    if position is None:
        return cash
    raw_open = _safe_float(row.get("open"))
    pre_close = _safe_float(row.get("pre_close"))
    factor = _safe_float(row.get("adjust_factor"), 1.0)
    if raw_open <= 0 or pre_close <= 0 or factor <= 0:
        return cash
    open_return = raw_open / pre_close - 1.0
    if open_return <= -_limit_ratio(code, position.name) + 0.002:
        fills.append({
            "date": day,
            "code": code,
            "name": position.name,
            "action": "REJECT_SELL",
            "reason": "开盘接近跌停，按不可成交处理并等待下一日",
            "mode": mode,
        })
        return cash

    equivalent_shares = max(1, int(round(position.adj_units * factor)))
    target_stage = float(order["target_stage"])
    if target_stage <= 0:
        shares = equivalent_shares
    else:
        desired_sell = equivalent_shares * (
            max(0.0, position.stage - target_stage) / max(position.stage, 1e-9)
        )
        shares = int(desired_sell / LOT_SIZE) * LOT_SIZE
        if shares < LOT_SIZE:
            fills.append({
                "date": day,
                "code": code,
                "name": position.name,
                "action": "REJECT_REDUCE",
                "reason": "持仓只有一手或不足以分批减仓",
                "mode": mode,
            })
            return cash

    execution_price = raw_open * (1.0 - SLIPPAGE_RATE)
    notional = shares * execution_price
    fee = _trade_fee(notional, "sell")
    returned = notional - fee
    sold_units = min(position.adj_units, shares / factor)
    position.adj_units -= sold_units
    position.returned_cash += returned
    cash += returned
    is_exit = target_stage <= 0 or position.adj_units <= 1e-8
    position.stage = 0.0 if is_exit else target_stage
    fills.append({
        "date": day,
        "signal_date": order["signal_date"],
        "code": code,
        "name": position.name,
        "action": order["action"],
        "price": execution_price,
        "shares": shares,
        "notional": notional,
        "fee": fee,
        "stage_after": position.stage,
        "reason": order["reason"],
        "profit_rate_signal": order.get("profit_rate"),
        "hold_score": order.get("hold_score"),
        "mode": mode,
    })
    if is_exit:
        pnl = position.returned_cash - position.invested_cash
        cycles.append({
            "code": code,
            "name": position.name,
            "entry_date": position.entry_date,
            "exit_date": day,
            "holding_sessions": position.holding_sessions,
            "invested_cash": position.invested_cash,
            "returned_cash": position.returned_cash,
            "pnl": pnl,
            "return_pct": (
                pnl / position.invested_cash * 100
                if position.invested_cash > 0
                else 0.0
            ),
            "max_profit_pct": position.max_profit_rate * 100,
            "exit_reason": order["reason"],
            "mode": mode,
            "marked_at_end": False,
        })
        positions.pop(code, None)
    return cash


def _entry_orders(
    *,
    day_rows: pd.DataFrame,
    positions: dict[str, Position],
    cooldown: dict[str, int],
    equity: float,
    slots: int,
    mode: str,
    day: pd.Timestamp,
) -> list[dict[str, Any]]:
    if slots <= 0:
        return []
    candidates = day_rows[
        day_rows.apply(_entry_eligible, axis=1)
        & ~day_rows["stock_code"].isin(positions)
        & day_rows["stock_code"].map(lambda code: cooldown.get(code, 0) <= 0)
    ].sort_values(
        ["selection_rank", "avg_amount20"],
        ascending=[False, False],
    )
    orders: list[dict[str, Any]] = []
    for _, row in candidates.head(slots).iterrows():
        stop, stop_distance, max_value = _stop_and_size(row, equity)
        target_stage = 0.5 if mode == "dynamic" else 1.0
        orders.append({
            "code": str(row["stock_code"]),
            "action": "BUY_HALF" if mode == "dynamic" else "BUY_FULL",
            "target_stage": target_stage,
            "requested_value": max_value * target_stage,
            "max_value": max_value,
            "initial_stop_adj": stop,
            "stop_distance_pct": stop_distance * 100,
            "signal_date": day,
            "signal_raw_close": _safe_float(row["close"]),
            "entry_score": _safe_float(row["entry_score"]),
            "hold_score": _safe_float(row["hold_score"]),
            "reason": (
                f"趋势入场：买入分{row['entry_score']:.0f}，"
                f"20日强度分位{row['ret20_pct']:.0%}，"
                f"板块宽度{row['breadth']:.0%}，"
                f"初始风险距离{stop_distance:.1%}"
            ),
        })
    return orders


def _dynamic_position_order(
    position: Position,
    row: pd.Series,
    day: pd.Timestamp,
    current_value: float,
) -> dict[str, Any] | None:
    close = _safe_float(row["adj_close"])
    atr = _safe_float(row["atr14"])
    hold_score = _safe_float(row["hold_score"])
    profit_rate = close / position.avg_entry_adj - 1.0
    risk_distance = max(
        1e-9,
        position.avg_entry_adj - position.initial_stop_adj,
    )
    profit_r = (close - position.avg_entry_adj) / risk_distance
    position.high_adj = max(position.high_adj, _safe_float(row["adj_high"], close))
    position.max_profit_rate = max(
        position.max_profit_rate,
        position.high_adj / position.avg_entry_adj - 1.0,
    )

    dynamic_stop = position.initial_stop_adj
    if profit_r >= 1.0:
        dynamic_stop = max(dynamic_stop, position.avg_entry_adj)
    if profit_r >= 2.0 and atr > 0:
        dynamic_stop = max(dynamic_stop, position.high_adj - 2.5 * atr)

    trend_broken = (
        close < _safe_float(row["ma20"])
        and _safe_float(row["ma20_slope5"]) <= 0
    )
    efficiency_exit = (
        position.holding_sessions >= 7
        and profit_rate < 0.01
        and hold_score < 65
    )
    if close <= dynamic_stop:
        return {
            "code": position.code,
            "action": "EXIT",
            "target_stage": 0.0,
            "signal_date": day,
            "profit_rate": profit_rate,
            "hold_score": hold_score,
            "reason": (
                f"动态保护退出：收盘{close:.3f}跌破保护位"
                f"{dynamic_stop:.3f}，当前{profit_rate:+.2%}"
            ),
        }
    if trend_broken or hold_score < EXIT_SCORE:
        return {
            "code": position.code,
            "action": "EXIT",
            "target_stage": 0.0,
            "signal_date": day,
            "profit_rate": profit_rate,
            "hold_score": hold_score,
            "reason": (
                f"趋势失效退出：持有分{hold_score:.0f}，"
                f"收盘{'跌破20日线且斜率转负' if trend_broken else '综合状态失效'}"
            ),
        }
    if efficiency_exit:
        return {
            "code": position.code,
            "action": "EXIT",
            "target_stage": 0.0,
            "signal_date": day,
            "profit_rate": profit_rate,
            "hold_score": hold_score,
            "reason": (
                f"资金效率退出：持有{position.holding_sessions}个交易日，"
                f"收益{profit_rate:+.2%}，持有分{hold_score:.0f}"
            ),
        }
    reduce_condition = (
        position.stage >= 0.99
        and (
            hold_score < REDUCE_SCORE
            or (
                profit_r >= 2.0
                and close < _safe_float(row["ma10"])
            )
            or _safe_float(row["breadth"]) < 0.35
        )
    )
    if reduce_condition:
        return {
            "code": position.code,
            "action": "REDUCE_HALF",
            "target_stage": 0.5,
            "signal_date": day,
            "profit_rate": profit_rate,
            "hold_score": hold_score,
            "reason": (
                f"趋势转弱减半：持有分{hold_score:.0f}，"
                f"板块宽度{_safe_float(row['breadth']):.0%}，"
                f"当前{profit_rate:+.2%}"
            ),
        }
    if (
        position.stage <= 0.51
        and position.holding_sessions >= 1
        and hold_score >= ADD_SCORE_MIN
        and close > position.avg_entry_adj
        and _safe_float(row["breadth"]) >= 0.50
    ):
        return {
            "code": position.code,
            "action": "ADD_HALF",
            "target_stage": 1.0,
            "requested_value": current_value,
            "signal_date": day,
            "signal_raw_close": _safe_float(row["close"]),
            "initial_stop_adj": position.initial_stop_adj,
            "entry_score": _safe_float(row["entry_score"]),
            "hold_score": hold_score,
            "reason": (
                f"趋势确认加仓：持有分{hold_score:.0f}，"
                f"板块宽度{_safe_float(row['breadth']):.0%}，"
                f"浮盈{profit_rate:+.2%}"
            ),
        }
    return None


def _rigid_position_order(
    position: Position,
    row: pd.Series,
    day: pd.Timestamp,
    mode: str,
) -> dict[str, Any] | None:
    profit_rate = _safe_float(row["adj_close"]) / position.avg_entry_adj - 1.0
    high_profit = position.high_adj / position.avg_entry_adj - 1.0
    position.high_adj = max(position.high_adj, _safe_float(row["adj_high"]))
    position.max_profit_rate = max(position.max_profit_rate, high_profit)
    if mode == "rigid_short":
        take_profit, stop_loss, max_days = 0.10, -0.05, 10
        trail_activate, trail_drawdown = 0.07, 0.02
    else:
        take_profit, stop_loss, max_days = 0.80, -0.10, 60
        trail_activate, trail_drawdown = 0.35, 0.08

    reason = None
    if profit_rate >= take_profit:
        reason = f"固定止盈：达到{take_profit:+.0%}"
    elif profit_rate <= stop_loss:
        reason = f"固定止损：达到{stop_loss:+.0%}"
    elif position.holding_sessions >= max_days:
        reason = f"固定期限：达到{max_days}个交易日"
    elif high_profit >= trail_activate and high_profit - profit_rate >= trail_drawdown:
        reason = (
            f"固定回撤止盈：最高{high_profit:+.2%}，"
            f"回撤{high_profit - profit_rate:.2%}"
        )
    if reason is None:
        return None
    return {
        "code": position.code,
        "action": "EXIT",
        "target_stage": 0.0,
        "signal_date": day,
        "profit_rate": profit_rate,
        "hold_score": _safe_float(row["hold_score"]),
        "reason": reason,
    }


def run_episode(
    *,
    feature_frame: pd.DataFrame,
    episode_key: str,
    mode: str,
    initial_capital: float,
) -> dict[str, Any]:
    episode = EPISODES[episode_key]
    start = pd.Timestamp(episode["start"])
    end = pd.Timestamp(episode["end"])
    calendar = sorted(
        feature_frame.loc[
            feature_frame["trade_date"].between(start, end),
            "trade_date",
        ].unique()
    )
    calendar = [pd.Timestamp(day) for day in calendar]
    if not calendar:
        raise RuntimeError(f"{episode_key} has no trading sessions")
    rows = _row_map(feature_frame)
    frames_by_day = {
        pd.Timestamp(day): day_frame.copy()
        for day, day_frame in feature_frame[
            feature_frame["trade_date"].between(start, end)
        ].groupby("trade_date")
    }

    cash = float(initial_capital)
    positions: dict[str, Position] = {}
    pending: list[dict[str, Any]] = []
    fills: list[dict[str, Any]] = []
    cycles: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    cooldown: dict[str, int] = {}
    total_turnover = 0.0

    for day in calendar:
        for code in list(cooldown):
            cooldown[code] -= 1
            if cooldown[code] <= 0:
                cooldown.pop(code, None)

        todays_orders = pending
        pending = []
        sell_orders = [
            order for order in todays_orders
            if order["action"] in {"REDUCE_HALF", "EXIT"}
        ]
        buy_orders = [
            order for order in todays_orders
            if order["action"] in {"BUY_HALF", "BUY_FULL", "ADD_HALF"}
        ]

        for order in sell_orders:
            row = rows.get((day, order["code"]))
            if row is None:
                pending.append(order)
                continue
            before_cash = cash
            cash = _execute_sell(
                order=order,
                row=row,
                cash=cash,
                positions=positions,
                mode=mode,
                day=day,
                fills=fills,
                cycles=cycles,
            )
            total_turnover += max(0.0, cash - before_cash)
            if order["code"] not in positions:
                cooldown[order["code"]] = 3

        for order in buy_orders:
            row = rows.get((day, order["code"]))
            if row is None:
                continue
            before_cash = cash
            cash = _execute_buy(
                order=order,
                row=row,
                cash=cash,
                positions=positions,
                mode=mode,
                day=day,
                fills=fills,
            )
            total_turnover += max(0.0, before_cash - cash)

        for position in positions.values():
            position.holding_sessions += 1

        equity = _portfolio_equity(cash, positions, day, rows)
        day_frame = frames_by_day.get(day, pd.DataFrame())
        for code, position in list(positions.items()):
            row = rows.get((day, code))
            if row is None:
                continue
            current_value = _position_value(position, row)
            if mode == "dynamic":
                order = _dynamic_position_order(
                    position,
                    row,
                    day,
                    current_value,
                )
            else:
                order = _rigid_position_order(position, row, day, mode)
            if order is not None:
                pending.append(order)

        occupied_next = len(positions) + sum(
            order["action"] in {"BUY_HALF", "BUY_FULL"}
            for order in pending
        )
        slots = max(0, MAX_POSITIONS - occupied_next)
        if slots > 0 and not day_frame.empty and day < calendar[-1]:
            pending.extend(_entry_orders(
                day_rows=day_frame,
                positions=positions,
                cooldown=cooldown,
                equity=equity,
                slots=slots,
                mode=mode,
                day=day,
            ))

        equity_rows.append({
            "date": day,
            "equity": equity,
            "cash": cash,
            "position_count": len(positions),
            "exposure_pct": (
                max(0.0, equity - cash) / equity * 100
                if equity > 0
                else 0.0
            ),
        })

    final_day = calendar[-1]
    marked_equity = _portfolio_equity(cash, positions, final_day, rows)
    hypothetical_exit_fees = 0.0
    open_marks: list[dict[str, Any]] = []
    for code, position in positions.items():
        row = rows.get((final_day, code))
        if row is None:
            continue
        value = _position_value(position, row)
        hypothetical_fee = _trade_fee(value, "sell")
        hypothetical_exit_fees += hypothetical_fee
        pnl = (
            position.returned_cash
            + value
            - hypothetical_fee
            - position.invested_cash
        )
        mark = {
            "code": code,
            "name": position.name,
            "entry_date": position.entry_date,
            "exit_date": final_day,
            "holding_sessions": position.holding_sessions,
            "invested_cash": position.invested_cash,
            "returned_cash": position.returned_cash + value - hypothetical_fee,
            "pnl": pnl,
            "return_pct": (
                pnl / position.invested_cash * 100
                if position.invested_cash > 0
                else 0.0
            ),
            "max_profit_pct": position.max_profit_rate * 100,
            "exit_reason": "期末按收盘价估值，非实际卖出",
            "mode": mode,
            "marked_at_end": True,
        }
        cycles.append(mark)
        open_marks.append(mark)
    final_equity = marked_equity - hypothetical_exit_fees

    equity_frame = pd.DataFrame(equity_rows).set_index("date")
    peak = equity_frame["equity"].cummax()
    drawdown = equity_frame["equity"] / peak - 1.0
    completed = pd.DataFrame(cycles)
    if completed.empty:
        wins = pd.Series(dtype=float)
        losses = pd.Series(dtype=float)
    else:
        wins = completed.loc[completed["pnl"] > 0, "pnl"]
        losses = completed.loc[completed["pnl"] < 0, "pnl"]
    avg_win = _safe_float(wins.mean()) if not wins.empty else 0.0
    avg_loss = abs(_safe_float(losses.mean())) if not losses.empty else 0.0
    profit_factor = (
        _safe_float(wins.sum()) / abs(_safe_float(losses.sum()))
        if not losses.empty and abs(_safe_float(losses.sum())) > 0
        else None
    )
    payoff = avg_win / avg_loss if avg_loss > 0 else None
    fee_total = (
        sum(_safe_float(fill.get("fee")) for fill in fills)
        + hypothetical_exit_fees
    )
    summary = {
        "episode": episode_key,
        "episode_name": episode["name"],
        "mode": mode,
        "mode_name": MODES[mode],
        "start_date": calendar[0].date().isoformat(),
        "end_date": calendar[-1].date().isoformat(),
        "sessions": len(calendar),
        "initial_capital": initial_capital,
        "ending_equity": final_equity,
        "net_profit": final_equity - initial_capital,
        "total_return_pct": (final_equity / initial_capital - 1.0) * 100,
        "max_drawdown_pct": _safe_float(drawdown.min()) * 100,
        "trade_cycles": int(len(completed)),
        "closed_cycles": int(
            0 if completed.empty else (~completed["marked_at_end"]).sum()
        ),
        "open_marks": len(open_marks),
        "win_rate_pct": (
            float((completed["pnl"] > 0).mean() * 100)
            if not completed.empty
            else None
        ),
        "average_win": avg_win,
        "average_loss": avg_loss,
        "payoff_ratio": payoff,
        "profit_factor": profit_factor,
        "fee_total": fee_total,
        "turnover": total_turnover,
        "turnover_multiple": total_turnover / initial_capital,
        "max_positions": int(equity_frame["position_count"].max()),
        "average_exposure_pct": float(equity_frame["exposure_pct"].mean()),
        "rejected_orders": sum(
            str(fill.get("action", "")).startswith("REJECT")
            for fill in fills
        ),
    }
    return {
        "summary": summary,
        "fills": fills,
        "cycles": cycles,
        "equity": equity_rows,
    }


def _round_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    numeric = out.select_dtypes(include=[np.number]).columns
    out.loc[:, numeric] = out.loc[:, numeric].round(6)
    return out


def _execution_audit(
    prices: pd.DataFrame,
    results: dict[str, Any],
) -> dict[str, Any]:
    raw_rows = {
        (pd.Timestamp(row["trade_date"]), str(row["stock_code"])): row
        for _, row in prices.iterrows()
    }
    checked_fills = 0
    missing_price_rows = 0
    execution_price_mismatches = 0
    notional_mismatches = 0
    fee_mismatches = 0
    lot_violations = 0
    signal_timing_violations = 0
    negative_cash_days = 0
    pnl_reconciliation_mismatches = 0
    samples: list[dict[str, Any]] = []

    for episode_key in EPISODES:
        for mode in MODES:
            result = results[episode_key][mode]
            for equity_row in result["equity"]:
                if _safe_float(equity_row.get("cash")) < -1e-6:
                    negative_cash_days += 1
            cycle_pnl = sum(
                _safe_float(cycle.get("pnl"))
                for cycle in result["cycles"]
            )
            net_profit = _safe_float(result["summary"].get("net_profit"))
            if abs(cycle_pnl - net_profit) > 0.02:
                pnl_reconciliation_mismatches += 1
                if len(samples) < 30:
                    samples.append({
                        "episode": episode_key,
                        "mode": mode,
                        "issues": ["pnl_reconciliation"],
                        "cycle_pnl": cycle_pnl,
                        "net_profit": net_profit,
                    })
            for fill in result["fills"]:
                action = str(fill.get("action") or "")
                if action.startswith("REJECT"):
                    continue
                checked_fills += 1
                key = (
                    pd.Timestamp(fill["date"]),
                    str(fill["code"]).zfill(6),
                )
                row = raw_rows.get(key)
                issues: list[str] = []
                if row is None:
                    missing_price_rows += 1
                    issues.append("missing_price_row")
                else:
                    side = (
                        "buy"
                        if action in {"BUY_HALF", "BUY_FULL", "ADD_HALF"}
                        else "sell"
                    )
                    multiplier = (
                        1.0 + SLIPPAGE_RATE
                        if side == "buy"
                        else 1.0 - SLIPPAGE_RATE
                    )
                    expected_price = _safe_float(row["open"]) * multiplier
                    actual_price = _safe_float(fill.get("price"))
                    if abs(expected_price - actual_price) > 1e-6:
                        execution_price_mismatches += 1
                        issues.append("execution_price")

                shares = int(round(_safe_float(fill.get("shares"))))
                if shares <= 0 or shares % LOT_SIZE != 0:
                    lot_violations += 1
                    issues.append("lot_size")
                expected_notional = shares * _safe_float(fill.get("price"))
                actual_notional = _safe_float(fill.get("notional"))
                if abs(expected_notional - actual_notional) > 0.01:
                    notional_mismatches += 1
                    issues.append("notional")
                expected_fee = _trade_fee(
                    actual_notional,
                    (
                        "buy"
                        if action in {"BUY_HALF", "BUY_FULL", "ADD_HALF"}
                        else "sell"
                    ),
                )
                if abs(expected_fee - _safe_float(fill.get("fee"))) > 0.01:
                    fee_mismatches += 1
                    issues.append("fee")
                signal_date = pd.Timestamp(fill.get("signal_date"))
                if pd.isna(signal_date) or signal_date >= pd.Timestamp(fill["date"]):
                    signal_timing_violations += 1
                    issues.append("signal_timing")
                if issues and len(samples) < 30:
                    samples.append({
                        "episode": episode_key,
                        "mode": mode,
                        "date": fill.get("date"),
                        "code": fill.get("code"),
                        "action": action,
                        "issues": issues,
                    })

    violations = (
        missing_price_rows
        + execution_price_mismatches
        + notional_mismatches
        + fee_mismatches
        + lot_violations
        + signal_timing_violations
        + negative_cash_days
        + pnl_reconciliation_mismatches
    )
    return {
        "status": "PASS" if violations == 0 else "FAIL",
        "checked_fills": checked_fills,
        "missing_price_rows": missing_price_rows,
        "execution_price_mismatches": execution_price_mismatches,
        "notional_mismatches": notional_mismatches,
        "fee_mismatches": fee_mismatches,
        "lot_violations": lot_violations,
        "signal_timing_violations": signal_timing_violations,
        "negative_cash_days": negative_cash_days,
        "pnl_reconciliation_mismatches": (
            pnl_reconciliation_mismatches
        ),
        "samples": samples,
    }


def _write_report(
    output_dir: Path,
    summaries: pd.DataFrame,
    universe_audit: dict[str, Any],
    data_audit: dict[str, Any],
    execution_audit: dict[str, Any],
    results: dict[str, Any],
) -> Path:
    dynamic = summaries[summaries["mode"] == "dynamic"].copy()
    dynamic_by_name = {
        str(row["episode_name"]): row
        for _, row in dynamic.iterrows()
    }
    comparison = summaries.pivot(
        index="episode_name",
        columns="mode_name",
        values="total_return_pct",
    )
    lines = [
        "# 2026主题行情个人动态策略回测报告",
        "",
        "> 生成日期：2026-07-25  ",
        "> 数据截止：2026-07-24  ",
        "> 初始资金：20万元  ",
        "> 行情：生产系统实际读取的独立日K历史库  ",
        "> 成分：国金QMT板块快照；价格逐行来源与历史成分限制见“边界”部分  ",
        "> 验收结论：条件通过——计算与执行审计通过，旧价格行的QMT逐行溯源未通过",
        "",
        "## 一、结果总览",
        "",
        "| 行情 | 动态策略 | 固定短线 | 固定主升 | 股票池等权参照¹ | 动态最大回撤 | 动态交易段 | 动态胜率 | 动态盈亏比 | 动态PF |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in dynamic.iterrows():
        episode_name = row["episode_name"]
        comp = comparison.loc[episode_name]
        lines.append(
            "| {name} | {dynamic:+.2f}% | {short:+.2f}% | {main:+.2f}% | "
            "{benchmark:+.2f}% | "
            "{dd:.2f}% | {trades} | {win} | {payoff} | {pf} |".format(
                name=episode_name,
                dynamic=row["total_return_pct"],
                short=_safe_float(comp.get("现有固定短线")),
                main=_safe_float(comp.get("现有固定主升")),
                benchmark=_safe_float(row["benchmark_equal_weight_pct"]),
                dd=row["max_drawdown_pct"],
                trades=int(row["trade_cycles"]),
                win=(
                    f"{row['win_rate_pct']:.1f}%"
                    if pd.notna(row["win_rate_pct"])
                    else "-"
                ),
                payoff=(
                    f"{row['payoff_ratio']:.2f}"
                    if pd.notna(row["payoff_ratio"])
                    else "-"
                ),
                pf=(
                    f"{row['profit_factor']:.2f}"
                    if pd.notna(row["profit_factor"])
                    else "-"
                ),
            )
        )
    lines.extend([
        "",
        "¹ 股票池等权参照为每日等权、零成本的行情背景，不是可直接照搬的交易策略。",
        "",
        "**这四行不能直接相加或复利。** 商业航天与贵金属的测试日期重叠，"
        "每一行都是“单独给这个主题20万元”的独立情景。若要回答同一账户全年实际收益，"
        "必须再加主题识别、跨主题抢仓和统一的四仓上限，不能拿重叠账户重复计算。",
        "",
        "## 二、人话结论",
        "",
        f"- 商业航天：赚{dynamic_by_name['商业航天']['net_profit']:+,.2f}元，"
        f"但最大回撤{dynamic_by_name['商业航天']['max_drawdown_pct']:.2f}%，"
        "只能算勉强赚到，且不如固定主升对照，动态退出仍丢掉了不少已出现的浮盈。",
        f"- 贵金属：赚{dynamic_by_name['贵金属']['net_profit']:+,.2f}元，"
        f"最大回撤{dynamic_by_name['贵金属']['max_drawdown_pct']:.2f}%。"
        "主要利润来自晓程科技、山金国际、招金黄金几笔大趋势；胜率不高，"
        "靠的是少数大赚覆盖多次小亏。",
        f"- 5—6月科技：赚{dynamic_by_name['5—6月科技']['net_profit']:+,.2f}元，"
        f"最大回撤{dynamic_by_name['5—6月科技']['max_drawdown_pct']:.2f}%。"
        "风险控制较好，但相对核心科技池的大涨明显吃得太少，说明入场和持仓容量偏保守。",
        f"- 7月行情：亏{abs(dynamic_by_name['7月行情']['net_profit']):,.2f}元，"
        f"最大回撤{dynamic_by_name['7月行情']['max_drawdown_pct']:.2f}%。"
        "5个交易段全部亏损；好的一面是股票池等权跌幅更大，系统很快降到低仓位，"
        "坏的一面是市场转弱时仍连续试错，缺一个更强的总闸门。",
        "",
        "结论不是“策略已经能稳定赚钱”，而是：**趋势持有和动态退出方向有效，"
        "但商业航天吃盈能力、科技行情参与度、7月弱市试错次数仍需要继续优化。**",
        "",
        "## 三、策略如何操作",
        "",
        "- 收盘产生信号，下一交易日开盘成交，不在信号日收盘偷看后成交。",
        "- 新机会先买半仓；持有分达到80、价格确认盈利、板块宽度不弱时再加到完整仓。",
        "- 单笔风险预算为账户的0.6%，单股最高22%，最多同时4只。",
        "- 不设3天、10天、60天强制卖出；趋势破坏、动态保护位、资金效率决定退出。",
        "- 佣金0.025%且最低5元，卖出印花税0.05%，过户费0.001%，单边滑点0.05%。",
        "- 涨停开盘不追、跌停开盘不假设能够卖出、买入按100股整数手。",
        "",
        "## 四、动态策略逐笔交易段",
        "",
    ])
    for episode_key, episode in EPISODES.items():
        cycles = pd.DataFrame(results[episode_key]["dynamic"]["cycles"])
        fills = pd.DataFrame(results[episode_key]["dynamic"]["fills"])
        lines.extend([
            f"### {episode['name']}",
            "",
        ])
        if cycles.empty:
            lines.append("本段没有形成可交易持仓。")
        else:
            lines.extend([
                "| 股票 | 建仓 | 退出/估值 | 持有交易日 | 净盈亏 | 收益率 | 最高浮盈 | 退出原因 |",
                "|---|---|---|---:|---:|---:|---:|---|",
            ])
            for _, row in cycles.sort_values(["entry_date", "code"]).iterrows():
                lines.append(
                    "| {code} {name} | {entry} | {exit} | {days} | "
                    "{pnl:+,.2f} | {ret:+.2f}% | {maxp:+.2f}% | {reason} |".format(
                        code=row["code"],
                        name=row["name"],
                        entry=str(row["entry_date"])[:10],
                        exit=str(row["exit_date"])[:10],
                        days=int(row["holding_sessions"]),
                        pnl=row["pnl"],
                        ret=row["return_pct"],
                        maxp=row["max_profit_pct"],
                        reason=row["exit_reason"],
                    )
                )
        lines.extend(["", "操作流水：", ""])
        if fills.empty:
            lines.append("- 无成交。")
        else:
            for _, fill in fills.iterrows():
                action = str(fill.get("action", ""))
                if action.startswith("REJECT"):
                    lines.append(
                        f"- {str(fill['date'])[:10]} {fill['code']} "
                        f"{fill.get('name', '')}：未成交，{fill.get('reason', '')}。"
                    )
                    continue
                lines.append(
                    f"- {str(fill['date'])[:10]} {fill['code']} "
                    f"{fill.get('name', '')}：{action}，"
                    f"{int(_safe_float(fill.get('shares')))}股，"
                    f"成交价{_safe_float(fill.get('price')):.3f}，"
                    f"费用{_safe_float(fill.get('fee')):.2f}元；"
                    f"{fill.get('reason', '')}。"
                )
        lines.append("")

    lines.extend([
        "## 五、数据审计",
        "",
        f"- 日K行数：{data_audit['row_count']:,}，股票数：{data_audit['stock_count']:,}。",
        f"- 日期：{data_audit['min_trade_date']} 至 {data_audit['max_trade_date']}。",
        f"- 重复业务键：{data_audit['duplicate_business_keys']}。",
        f"- 非法价格：{data_audit['invalid_price_rows']}；OHLC异常：{data_audit['bad_ohlc_rows']}。",
        f"- 缺失昨收：{data_audit['missing_pre_close_rows']}；"
        f"非首个可用交易日缺失：{data_audit['noninitial_missing_pre_close_rows']}。",
        f"- 行级来源分布：`{json.dumps(data_audit['data_source_counts'], ensure_ascii=False)}`。",
        f"- 行级质量标签分布：`{json.dumps(data_audit['quality_status_counts'], ensure_ascii=False)}`。",
        "- 国金QMT直连核价门禁：未标记通过。本报告没有把“价格结构审计通过”"
        "偷换成“每条旧日K均已由QMT直接复核”。",
        f"- 数据哈希：`{data_audit['dataset_sha256']}`。",
        f"- QMT板块来源：{', '.join(universe_audit['qmt_sector_sources']) or '-'}。",
        f"- QMT板块批次：{', '.join(universe_audit['qmt_sector_batches']) or '-'}。",
        f"- 执行审计：{execution_audit['status']}；逐笔核对"
        f"{execution_audit['checked_fills']}笔实际成交，成交价、费用、整手、"
        "信号时序、现金均按规则复算。",
        "",
        "主题池规则：",
        "",
    ])
    for episode_key, episode in EPISODES.items():
        lines.append(
            f"- {episode['name']}：{universe_audit['universe_sizes'][episode_key]}只；"
            f"{universe_audit['universe_rules'][episode_key]}。"
        )
    lines.extend([
        "",
        "## 六、边界与不能夸大的地方",
        "",
        "1. 这是用户指定主题后的条件回测，不等于系统在年初已经自动识别出商业航天和贵金属。",
        "2. QMT板块表当前只有7月20日快照，本报告据此冻结主题池；"
        "股票日K没有未来数据，但主题成员存在历史成分幸存者偏差。",
        "3. 历史库是生产系统当前实际读取的数据，但绝大多数旧行的 `data_source`/`quality_status` "
        "为空；因此本报告能证明价格结构完整，不能把每一条旧日K都宣称为已完成QMT行级溯源。",
        "4. 7月使用6月份成交额前1000只作为事前流动性池，没有使用7月末涨幅倒选股票。",
        "5. 日线无法知道涨跌停板内的真实排队位置；本报告对涨跌停开盘采取保守拒单。",
        "6. 期末尚未卖出的股票按最后收盘价估值，并扣除假设卖出费用，但不是一笔真实可成交卖单。",
        "7. 四个片段样本仍小，只能回答“这些行情里规则怎样表现”，不能证明未来长期盈利。",
        "",
        "## 七、文件",
        "",
        "- `summary.csv`：全部策略与行情汇总。",
        "- `fills_dynamic.csv`：动态策略逐笔成交和拒单。",
        "- `cycles_dynamic.csv`：动态策略完整持仓段。",
        "- `equity_dynamic.csv`：动态策略每日净值。",
        "- `report.json`：全部机器可读结果和审计信息。",
    ])
    report_path = output_dir / "2026主题行情个人动态策略回测报告.md"
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Backtest retail dynamic holding across 2026 A-share themes",
    )
    parser.add_argument(
        "--output-dir",
        default="artifacts/theme_dynamic_backtest_20260725",
    )
    parser.add_argument(
        "--initial-capital",
        type=float,
        default=INITIAL_CAPITAL,
    )
    args = parser.parse_args(argv)

    load_project_env()
    engine = create_tool_engine()
    kline_engine = get_kline_engine()
    universes, universe_audit = _load_universes(engine, kline_engine)
    all_codes = set().union(*universes.values())
    prices = _load_prices(
        kline_engine,
        all_codes,
        "2025-08-01",
        "2026-07-24",
    )
    data_audit = _data_audit(prices, "2026-07-24")
    if (
        not data_audit["latest_date_matches"]
        or data_audit["duplicate_business_keys"] != 0
        or data_audit["invalid_price_rows"] != 0
        or data_audit["bad_ohlc_rows"] != 0
        or data_audit["noninitial_missing_pre_close_rows"] != 0
    ):
        raise RuntimeError(f"QMT data audit failed: {data_audit}")
    features = _prepare_features(prices)

    output_dir = ROOT / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    summaries: list[dict[str, Any]] = []
    results: dict[str, dict[str, Any]] = {}
    for episode_key in EPISODES:
        episode_frame = _episode_features(
            features,
            universes[episode_key],
        )
        benchmark = _episode_benchmark(episode_frame, episode_key)
        results[episode_key] = {}
        for mode in MODES:
            result = run_episode(
                feature_frame=episode_frame,
                episode_key=episode_key,
                mode=mode,
                initial_capital=args.initial_capital,
            )
            result["benchmark"] = benchmark
            result["summary"].update(benchmark)
            results[episode_key][mode] = result
            summaries.append(result["summary"])

    execution_audit = _execution_audit(prices, results)
    if execution_audit["status"] != "PASS":
        raise RuntimeError(
            f"Backtest execution audit failed: {execution_audit}"
        )
    summary_frame = _round_frame(pd.DataFrame(summaries))
    summary_frame.to_csv(
        output_dir / "summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    for mode in MODES:
        fills = pd.concat(
            [
                pd.DataFrame(results[key][mode]["fills"]).assign(
                    episode=key,
                    episode_name=EPISODES[key]["name"],
                )
                for key in EPISODES
            ],
            ignore_index=True,
        )
        cycles = pd.concat(
            [
                pd.DataFrame(results[key][mode]["cycles"]).assign(
                    episode=key,
                    episode_name=EPISODES[key]["name"],
                )
                for key in EPISODES
            ],
            ignore_index=True,
        )
        equity = pd.concat(
            [
                pd.DataFrame(results[key][mode]["equity"]).assign(
                    episode=key,
                    episode_name=EPISODES[key]["name"],
                )
                for key in EPISODES
            ],
            ignore_index=True,
        )
        _round_frame(fills).to_csv(
            output_dir / f"fills_{mode}.csv",
            index=False,
            encoding="utf-8-sig",
        )
        _round_frame(cycles).to_csv(
            output_dir / f"cycles_{mode}.csv",
            index=False,
            encoding="utf-8-sig",
        )
        _round_frame(equity).to_csv(
            output_dir / f"equity_{mode}.csv",
            index=False,
            encoding="utf-8-sig",
        )

    report_payload = {
        "generated_at": pd.Timestamp.now(),
        "assumptions": {
            "initial_capital": args.initial_capital,
            "lot_size": LOT_SIZE,
            "commission_rate": COMMISSION_RATE,
            "minimum_commission": MIN_COMMISSION,
            "stamp_duty_rate": STAMP_DUTY_RATE,
            "transfer_fee_rate": TRANSFER_FEE_RATE,
            "slippage_rate": SLIPPAGE_RATE,
            "risk_per_trade": RISK_PER_TRADE,
            "max_position_weight": MAX_POSITION_WEIGHT,
            "max_positions": MAX_POSITIONS,
            "minimum_average_amount_20": MIN_AVG_AMOUNT_20,
        },
        "episodes": EPISODES,
        "universe_audit": universe_audit,
        "data_audit": data_audit,
        "execution_audit": execution_audit,
        "summaries": summaries,
        "results": results,
    }
    (output_dir / "report.json").write_text(
        json.dumps(
            report_payload,
            ensure_ascii=False,
            indent=2,
            default=_json_default,
        ),
        encoding="utf-8",
    )
    report_path = _write_report(
        output_dir,
        summary_frame,
        universe_audit,
        data_audit,
        execution_audit,
        results,
    )
    print(summary_frame.to_string(index=False))
    print(f"report={report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
