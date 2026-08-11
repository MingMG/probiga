# -*- coding: utf-8 -*-
"""Register and append observations for the frozen ETF forward experiment.

This tool never submits orders. It refuses retrospective observations, keeps
the strategy configuration immutable, and records each post-freeze market
close at most once.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.sql_reader import read_sql_rows
from tools.backtest_etf_ensemble import (
    CASH_CODE,
    MarketData,
    load_market_data,
    target_weights,
)
from tools.backtest_etf_robust import freeze_universe
from tools.env_config import create_tool_engine, load_project_env

SHANGHAI = ZoneInfo("Asia/Shanghai")
DEFAULT_CONFIG = ROOT / "strategies" / "etf_trend_risk_v2.json"


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def stable_hash(value: Any) -> str:
    return hashlib.sha256(
        canonical_json(value).encode("utf-8")
    ).hexdigest()


def load_config(path: Path) -> tuple[dict[str, Any], str]:
    config = json.loads(path.read_text(encoding="utf-8"))
    required = {
        "schema_version",
        "strategy_version",
        "frozen_at",
        "forward_start_date",
        "data",
        "universe",
        "monthly_signal",
        "risk_overlay",
        "execution",
        "forward_protocol",
    }
    missing = sorted(required - set(config))
    if missing:
        raise ValueError(f"forward config missing fields: {missing}")
    if config["forward_protocol"].get("backfill") != "prohibited":
        raise ValueError("forward config must prohibit backfill")
    if config["forward_protocol"].get("automatic_order_submission"):
        raise ValueError("forward tool must remain read-only")
    return config, stable_hash(config)


def ensure_tables(engine: Any) -> None:
    required = {
        "st_etf_forward_strategy",
        "st_etf_forward_observation",
    }
    with engine.connect() as connection:
        existing = {
            str(row[0])
            for row in connection.execute(
                text(
                    """
                    SELECT TABLE_NAME FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME IN
                          ('st_etf_forward_strategy',
                           'st_etf_forward_observation')
                    """
                )
            ).fetchall()
        }
    missing = sorted(required - existing)
    if missing:
        raise RuntimeError(
            "ETF forward schema migration is required: "
            + ",".join(missing)
        )


def register_strategy(
    engine: Any,
    config: dict[str, Any],
    config_hash: str,
) -> dict[str, Any]:
    version = str(config["strategy_version"])
    rows = read_sql_rows(
        engine,
        """
        SELECT strategy_version, config_hash, frozen_at,
               forward_start_date, mode, status, registered_at
          FROM st_etf_forward_strategy
         WHERE strategy_version = :version
        """,
        {"version": version},
        context="etf_forward_registry",
        stringify_datetime=True,
    )
    if rows:
        existing = rows[0]
        if existing["config_hash"] != config_hash:
            raise RuntimeError(
                "strategy version is immutable; use a new version "
                "for parameter changes"
            )
        return {**existing, "registration": "already_registered"}

    frozen_at = datetime.fromisoformat(
        str(config["frozen_at"])
    ).replace(tzinfo=None)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO st_etf_forward_strategy (
                    strategy_version, config_hash, frozen_at,
                    forward_start_date, mode, status, config_json
                ) VALUES (
                    :strategy_version, :config_hash, :frozen_at,
                    :forward_start_date, :mode, 'registered',
                    :config_json
                )
                """
            ),
            {
                "strategy_version": version,
                "config_hash": config_hash,
                "frozen_at": frozen_at,
                "forward_start_date": config["forward_start_date"],
                "mode": config["mode"],
                "config_json": canonical_json(config),
            },
        )
    return {
        "strategy_version": version,
        "config_hash": config_hash,
        "frozen_at": str(config["frozen_at"]),
        "forward_start_date": config["forward_start_date"],
        "mode": config["mode"],
        "status": "registered",
        "registered_at": datetime.now(SHANGHAI).isoformat(),
        "registration": "created",
    }


def latest_validated_data_date(
    engine: Any,
    *,
    data_source: str,
) -> date | None:
    rows = read_sql_rows(
        engine,
        """
        SELECT MAX(trade_date) AS data_date
          FROM sm_etf_kline
         WHERE adjust_type = 1
           AND k_type = 1
           AND validation_status = 'passed'
           AND quality_status = 'validated'
           AND data_source = :data_source
        """,
        {"data_source": data_source},
        context="etf_forward_latest_date",
    )
    value = rows[0].get("data_date") if rows else None
    if value is None:
        return None
    return pd.Timestamp(value).date()


def next_trading_date(engine: Any, data_date: date) -> date | None:
    rows = read_sql_rows(
        engine,
        """
        SELECT MIN(trade_date) AS next_date
          FROM si_trade_calendar
         WHERE trade_status = 1
           AND trade_date > :data_date
        """,
        {"data_date": data_date},
        context="etf_forward_next_trade_date",
    )
    value = rows[0].get("next_date") if rows else None
    return pd.Timestamp(value).date() if value is not None else None


def validate_observation_date(
    *,
    data_date: date,
    forward_start_date: date,
    registered_at: datetime,
    local_today: date,
) -> None:
    minimum_date = max(forward_start_date, registered_at.date())
    if data_date < minimum_date:
        raise ValueError(
            f"backfill prohibited: {data_date} < {minimum_date}"
        )
    if data_date > local_today:
        raise ValueError(
            f"future observation prohibited: {data_date} > {local_today}"
        )


def is_month_end_close(
    data_date: date,
    next_trade_date: date,
) -> bool:
    return (
        data_date.year,
        data_date.month,
    ) != (
        next_trade_date.year,
        next_trade_date.month,
    )


def _latest_observation(
    engine: Any,
    version: str,
) -> dict[str, Any] | None:
    rows = read_sql_rows(
        engine,
        """
        SELECT data_date, signal_type, execution_date,
               target_json, context_json, input_hash
          FROM st_etf_forward_observation
         WHERE strategy_version = :version
         ORDER BY data_date DESC
         LIMIT 1
        """,
        {"version": version},
        context="etf_forward_latest_observation",
        stringify_datetime=True,
    )
    return rows[0] if rows else None


def _latest_monthly_signal(
    engine: Any,
    version: str,
) -> dict[str, Any] | None:
    rows = read_sql_rows(
        engine,
        """
        SELECT data_date, execution_date, target_json
          FROM st_etf_forward_observation
         WHERE strategy_version = :version
           AND signal_type = 'monthly_rebalance'
         ORDER BY data_date DESC
         LIMIT 1
        """,
        {"version": version},
        context="etf_forward_latest_monthly_signal",
        stringify_datetime=True,
    )
    return rows[0] if rows else None


def _close_input_hash(
    data: MarketData,
    data_date: pd.Timestamp,
    eligible_codes: list[str],
) -> str:
    rows = []
    for code in eligible_codes:
        close = data.close.at[data_date, code]
        amount = data.amount.at[data_date, code]
        rows.append(
            {
                "code": code,
                "close": (
                    None if pd.isna(close) else round(float(close), 6)
                ),
                "amount": (
                    None if pd.isna(amount) else round(float(amount), 4)
                ),
            }
        )
    return stable_hash({"data_date": str(data_date.date()), "rows": rows})


def _daily_stop_target(
    data: MarketData,
    *,
    data_date: pd.Timestamp,
    current_target: dict[str, float],
    monthly_signal_date: pd.Timestamp,
    config: dict[str, Any],
) -> tuple[dict[str, float], list[dict[str, Any]]]:
    risk = config["risk_overlay"]
    revised = dict(current_target)
    exits: list[dict[str, Any]] = []
    for code, weight in current_target.items():
        if code == CASH_CODE or float(weight) <= 0:
            continue
        series = data.close.loc[
            monthly_signal_date:data_date,
            code,
        ].dropna()
        full = data.close.loc[:data_date, code].dropna()
        if series.empty or len(full) < 21:
            continue
        close = float(series.iloc[-1])
        peak = float(series.max())
        daily_vol = float(
            full.pct_change().dropna().tail(
                int(risk["volatility_lookback_days"])
            ).std(ddof=1)
        )
        threshold = float(
            np.clip(
                float(risk["volatility_multiplier"]) * daily_vol,
                float(risk["minimum_stop"]),
                float(risk["maximum_stop"]),
            )
        )
        drawdown = close / peak - 1.0
        if not math.isfinite(threshold) or drawdown > -threshold:
            continue
        exited_weight = float(revised.get(code, 0.0))
        revised[code] = 0.0
        revised[CASH_CODE] = (
            float(revised.get(CASH_CODE, 0.0)) + exited_weight
        )
        exits.append(
            {
                "etf_code": code,
                "close": close,
                "peak_close": peak,
                "drawdown_from_peak": drawdown,
                "stop_threshold": threshold,
            }
        )
    revised = {
        code: weight
        for code, weight in revised.items()
        if float(weight) > 1e-8
    }
    total = sum(float(value) for value in revised.values())
    if total > 0:
        revised = {
            code: float(value) / total
            for code, value in revised.items()
        }
    return revised, exits


def build_observation(
    engine: Any,
    config: dict[str, Any],
    config_hash: str,
    data_date: date,
) -> dict[str, Any]:
    version = str(config["strategy_version"])
    start_date = "2019-01-01"
    source_data = load_market_data(
        engine,
        start_date,
        data_date.isoformat(),
    )
    data, audit = freeze_universe(
        source_data,
        cutoff_date=config["universe"]["cutoff_date"],
        minimum_history_days=int(
            config["universe"]["minimum_history_days_at_cutoff"]
        ),
        minimum_average_amount=float(
            config["universe"][
                "minimum_average_amount_20_at_cutoff"
            ]
        ),
    )
    eligible = audit.loc[audit["eligible"], "etf_code"].tolist()
    if eligible != sorted(config["universe"]["eligible_codes"]):
        raise RuntimeError(
            "current frozen-universe evidence differs from registered config"
        )
    data_ts = pd.Timestamp(data_date)
    if data_ts not in data.calendar:
        raise RuntimeError(f"calendar proxy has no close for {data_date}")
    next_date = next_trading_date(engine, data_date)
    if next_date is None:
        raise RuntimeError("trade calendar has no next trading date")

    previous = _latest_observation(engine, version)
    current_target = (
        json.loads(previous["target_json"])
        if previous
        else {CASH_CODE: 1.0}
    )
    signal_type = "carry"
    context: dict[str, Any] = {
        "cold_start": previous is None,
        "risk_exits": [],
        "automatic_order_submission": False,
    }
    execution_date: date | None = None

    if is_month_end_close(data_date, next_date):
        weights, signal_context = target_weights(
            data,
            data_ts,
            "trend_risk",
        )
        current_target = {
            str(code): float(weight)
            for code, weight in weights.items()
        }
        signal_type = "monthly_rebalance"
        execution_date = next_date
        context.update(signal_context)
    else:
        monthly = _latest_monthly_signal(engine, version)
        if monthly:
            current_target, exits = _daily_stop_target(
                data,
                data_date=data_ts,
                current_target=current_target,
                monthly_signal_date=pd.Timestamp(monthly["data_date"]),
                config=config,
            )
            if exits:
                signal_type = "daily_vol_stop"
                execution_date = next_date
                context["risk_exits"] = exits

    return {
        "strategy_version": version,
        "config_hash": config_hash,
        "data_date": data_date.isoformat(),
        "observed_at": datetime.now(SHANGHAI).isoformat(),
        "data_source": config["data"]["primary_source"],
        "input_hash": _close_input_hash(data, data_ts, eligible),
        "signal_type": signal_type,
        "execution_date": (
            execution_date.isoformat() if execution_date else None
        ),
        "target": current_target,
        "context": context,
    }


def append_observation(
    engine: Any,
    observation: dict[str, Any],
) -> str:
    existing = read_sql_rows(
        engine,
        """
        SELECT input_hash, config_hash
          FROM st_etf_forward_observation
         WHERE strategy_version = :version
           AND data_date = :data_date
        """,
        {
            "version": observation["strategy_version"],
            "data_date": observation["data_date"],
        },
        context="etf_forward_existing_observation",
    )
    if existing:
        if (
            existing[0]["input_hash"] != observation["input_hash"]
            or existing[0]["config_hash"] != observation["config_hash"]
        ):
            raise RuntimeError(
                "immutable observation already exists with different evidence"
            )
        return "already_recorded"
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO st_etf_forward_observation (
                    strategy_version, config_hash, data_date,
                    observed_at, data_source, input_hash,
                    signal_type, execution_date, target_json,
                    context_json
                ) VALUES (
                    :strategy_version, :config_hash, :data_date,
                    :observed_at, :data_source, :input_hash,
                    :signal_type, :execution_date, :target_json,
                    :context_json
                )
                """
            ),
            {
                **observation,
                "observed_at": datetime.fromisoformat(
                    observation["observed_at"]
                ).replace(tzinfo=None),
                "target_json": canonical_json(observation["target"]),
                "context_json": canonical_json(observation["context"]),
            },
        )
    return "created"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Register the immutable config and append eligible observations.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=(
            ROOT
            / "artifacts"
            / "etf_forward"
            / "latest_result.json"
        ),
    )
    args = parser.parse_args()
    config, config_hash = load_config(args.config)
    if not args.write:
        result = {
            "status": "dry_run",
            "strategy_version": config["strategy_version"],
            "config_hash": config_hash,
            "forward_start_date": config["forward_start_date"],
            "automatic_order_submission": False,
        }
    else:
        load_project_env()
        engine = create_tool_engine()
        try:
            ensure_tables(engine)
            registry = register_strategy(engine, config, config_hash)
            latest_date = latest_validated_data_date(
                engine,
                data_source=config["data"]["primary_source"],
            )
            start = date.fromisoformat(config["forward_start_date"])
            registered_at = datetime.fromisoformat(
                registry["registered_at"]
            )
            today = datetime.now(SHANGHAI).date()
            if latest_date is None or latest_date < start:
                result = {
                    "status": "waiting_for_first_forward_data",
                    "registry": registry,
                    "latest_validated_data_date": (
                        latest_date.isoformat() if latest_date else None
                    ),
                    "first_eligible_data_date": start.isoformat(),
                    "automatic_order_submission": False,
                }
            else:
                validate_observation_date(
                    data_date=latest_date,
                    forward_start_date=start,
                    registered_at=registered_at,
                    local_today=today,
                )
                observation = build_observation(
                    engine,
                    config,
                    config_hash,
                    latest_date,
                )
                write_status = append_observation(engine, observation)
                result = {
                    "status": write_status,
                    "registry": registry,
                    "observation": observation,
                    "automatic_order_submission": False,
                }
        finally:
            engine.dispose()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
