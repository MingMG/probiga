#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Append one current-close observation to the frozen ETF research ledger.

The ledger is research-only.  Configuration rows are immutable, historical
observations are never reconstructed, and an already-recorded current date is
accepted only when every deterministic identity matches.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
import hashlib
import json
import math
from pathlib import Path
import sys
from typing import Any, Mapping
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
from tools.sync_etf_bigqmt_daily import ETF_CODES, PROVIDER_ID, code_set_hash


SHANGHAI = ZoneInfo("Asia/Shanghai")
DEFAULT_CONFIG = ROOT / "strategies" / "etf_trend_risk_v2.json"
RECEIPT_SCHEMA = "probiga.etf-forward-observation-receipt.v1"


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _receipt(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result["receipt_id"] = stable_hash(result)
    return result


def load_config(path: Path) -> tuple[dict[str, Any], str]:
    config = json.loads(path.read_text(encoding="utf-8"))
    required = {
        "schema_version",
        "strategy_version",
        "frozen_at",
        "forward_start_date",
        "mode",
        "data",
        "universe",
        "monthly_signal",
        "risk_overlay",
        "execution",
        "forward_protocol",
    }
    missing = sorted(required - set(config))
    if missing:
        raise RuntimeError(f"ETF forward config missing fields: {missing}")
    if config["forward_protocol"].get("backfill") != "prohibited":
        raise RuntimeError("ETF forward config must prohibit backfill")
    if config["forward_protocol"].get("automatic_order_submission") is not False:
        raise RuntimeError("ETF forward config must disable automatic orders")
    if config.get("mode") != "research_paper_read_only":
        raise RuntimeError("ETF forward config is not research-only")
    eligible = sorted(str(code) for code in config["universe"].get("eligible_codes", ()))
    if not eligible or not set(eligible).issubset(ETF_CODES):
        raise RuntimeError("ETF forward eligible universe is outside the frozen data set")
    if config["data"].get("primary_source") != PROVIDER_ID:
        raise RuntimeError("ETF forward config provider differs")
    if int(config["data"].get("adjust_type", -1)) != 1:
        raise RuntimeError("ETF forward config must use forward-adjusted bars")
    return config, stable_hash(config)


_FORWARD_COLUMNS: dict[str, frozenset[str]] = {
    "st_etf_forward_strategy": frozenset(
        {
            "strategy_version",
            "config_hash",
            "frozen_at",
            "forward_start_date",
            "mode",
            "status",
            "config_json",
            "registered_at",
        }
    ),
    "st_etf_forward_observation": frozenset(
        {
            "id",
            "strategy_version",
            "config_hash",
            "data_date",
            "observed_at",
            "data_source",
            "input_hash",
            "signal_type",
            "execution_date",
            "target_json",
            "context_json",
            "created_at",
        }
    ),
    "sm_etf_kline": frozenset(
        {
            "etf_code",
            "trade_date",
            "k_type",
            "adjust_type",
            "data_source",
            "validation_status",
            "validation_source",
            "quality_status",
            "data_version",
            "batch_id",
            "permission_status",
        }
    ),
    "si_trade_calendar": frozenset({"trade_date", "trade_status"}),
}


def validate_runtime_schema(engine: Any) -> dict[str, Any]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT TABLE_NAME, COLUMN_NAME
                  FROM information_schema.COLUMNS
                 WHERE TABLE_SCHEMA=DATABASE()
                   AND TABLE_NAME IN
                       ('st_etf_forward_strategy','st_etf_forward_observation',
                        'sm_etf_kline','si_trade_calendar')
                """
            )
        ).fetchall()
        indexes = connection.execute(
            text(
                """
                SELECT INDEX_NAME,NON_UNIQUE,SEQ_IN_INDEX,COLUMN_NAME
                  FROM information_schema.STATISTICS
                 WHERE TABLE_SCHEMA=DATABASE()
                   AND TABLE_NAME='st_etf_forward_observation'
                 ORDER BY INDEX_NAME,SEQ_IN_INDEX
                """
            )
        ).mappings().all()
    observed = {table: set() for table in _FORWARD_COLUMNS}
    for table_name, column_name in rows:
        observed.setdefault(str(table_name), set()).add(str(column_name))
    missing = {
        table: sorted(columns - observed.get(table, set()))
        for table, columns in _FORWARD_COLUMNS.items()
        if columns - observed.get(table, set())
    }
    if missing:
        raise RuntimeError(
            "ETF forward schema migration is missing: " + canonical_json(missing)
        )
    by_index: dict[str, list[tuple[int, str, int]]] = {}
    for row in indexes:
        by_index.setdefault(str(row["INDEX_NAME"]), []).append(
            (
                int(row["SEQ_IN_INDEX"]),
                str(row["COLUMN_NAME"]),
                int(row["NON_UNIQUE"]),
            )
        )
    unique_keys = {
        tuple(item[1] for item in sorted(values))
        for values in by_index.values()
        if values and all(item[2] == 0 for item in values)
    }
    if ("strategy_version", "data_date") not in unique_keys:
        raise RuntimeError("ETF forward observation unique identity is missing")
    return {
        "status": "PASS",
        "schema_hash": stable_hash(
            {table: sorted(observed[table]) for table in sorted(observed)}
        ),
    }


def _load_strategy(engine: Any, version: str) -> dict[str, Any] | None:
    rows = read_sql_rows(
        engine,
        """
        SELECT strategy_version,config_hash,frozen_at,forward_start_date,
               mode,status,config_json,registered_at
          FROM st_etf_forward_strategy
         WHERE strategy_version=:version
        """,
        {"version": version},
        context="etf_forward_registry",
        stringify_datetime=True,
    )
    return rows[0] if rows else None


def register_strategy(
    engine: Any,
    config: dict[str, Any],
    config_hash: str,
) -> dict[str, Any]:
    version = str(config["strategy_version"])
    existing = _load_strategy(engine, version)
    if existing is not None:
        if (
            existing["config_hash"] != config_hash
            or json.loads(str(existing["config_json"])) != config
            or str(existing["mode"]) != str(config["mode"])
            or str(existing["status"]) != "registered"
        ):
            raise RuntimeError(
                "ETF strategy version is immutable; use a new version for changes"
            )
        return {**existing, "registration": "ALREADY_REGISTERED"}
    frozen_at = datetime.fromisoformat(str(config["frozen_at"])).replace(tzinfo=None)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO st_etf_forward_strategy
                  (strategy_version,config_hash,frozen_at,forward_start_date,
                   mode,status,config_json)
                VALUES
                  (:strategy_version,:config_hash,:frozen_at,:forward_start_date,
                   :mode,'registered',:config_json)
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
    created = _load_strategy(engine, version)
    if created is None or created["config_hash"] != config_hash:
        raise RuntimeError("ETF strategy registry readback differs")
    return {**created, "registration": "CREATED"}


def latest_validated_data_date(engine: Any) -> date | None:
    with engine.connect() as connection:
        latest = connection.execute(
            text(
                """
                SELECT MAX(trade_date)
                  FROM sm_etf_kline
                 WHERE adjust_type=1 AND k_type=1
                   AND validation_status='passed'
                   AND quality_status='validated'
                   AND data_source=:data_source
                """
            ),
            {"data_source": PROVIDER_ID},
        ).scalar()
    return pd.Timestamp(latest).date() if latest is not None else None


def validate_latest_etf_partition(engine: Any, data_date: date) -> dict[str, Any]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT etf_code,data_version,batch_id,permission_status,
                       validation_source
                  FROM sm_etf_kline
                 WHERE trade_date=:trade_date
                   AND adjust_type=1 AND k_type=1
                   AND validation_status='passed'
                   AND quality_status='validated'
                   AND data_source=:data_source
                 ORDER BY etf_code
                """
            ),
            {"trade_date": data_date, "data_source": PROVIDER_ID},
        ).mappings().all()
    codes = [str(row["etf_code"]).strip().zfill(6) for row in rows]
    if codes != list(ETF_CODES) or len(set(codes)) != len(codes):
        raise RuntimeError("ETF latest forward-adjusted partition is incomplete")
    identities = [
        {
            "etf_code": code,
            "data_version": str(row["data_version"]),
            "batch_id": str(row["batch_id"]),
            "permission_status": str(row["permission_status"]),
            "validation_source": str(row["validation_source"]),
        }
        for code, row in zip(codes, rows)
    ]
    if (
        any(
            len(row["data_version"]) != 64
            or not row["batch_id"]
            or row["permission_status"] != "SUPPORTED"
            or row["validation_source"] != "bigqmt_identity_and_set"
            for row in identities
        )
        or len({row["batch_id"] for row in identities}) != 1
    ):
        raise RuntimeError("ETF latest partition provenance is incomplete")
    return {
        "trade_date": data_date.isoformat(),
        "code_count": len(codes),
        "code_set_hash": code_set_hash(codes),
        "identity_hash": stable_hash(identities),
    }


def next_trading_date(engine: Any, data_date: date) -> date | None:
    with engine.connect() as connection:
        value = connection.execute(
            text(
                """
                SELECT MIN(trade_date)
                  FROM si_trade_calendar
                 WHERE trade_status=1 AND trade_date>:trade_date
                """
            ),
            {"trade_date": data_date},
        ).scalar()
    return pd.Timestamp(value).date() if value is not None else None


def validate_observation_date(
    *,
    data_date: date,
    forward_start_date: date,
    registered_at: datetime,
    local_today: date,
) -> None:
    minimum_date = max(forward_start_date, registered_at.date())
    if data_date != local_today:
        raise RuntimeError(
            "ETF forward backfill prohibited: latest validated close must equal today"
        )
    if data_date < minimum_date:
        raise RuntimeError(
            f"ETF forward backfill prohibited: {data_date} < {minimum_date}"
        )


def is_month_end_close(data_date: date, next_trade_date: date) -> bool:
    return (data_date.year, data_date.month) != (
        next_trade_date.year,
        next_trade_date.month,
    )


def _latest_observation(
    engine: Any,
    version: str,
    *,
    before_date: date,
) -> dict[str, Any] | None:
    rows = read_sql_rows(
        engine,
        """
        SELECT data_date,signal_type,execution_date,target_json,context_json,
               input_hash,config_hash
          FROM st_etf_forward_observation
         WHERE strategy_version=:version AND data_date<:before_date
         ORDER BY data_date DESC
         LIMIT 1
        """,
        {"version": version, "before_date": before_date},
        context="etf_forward_latest_observation",
        stringify_datetime=True,
    )
    return rows[0] if rows else None


def _latest_monthly_signal(
    engine: Any,
    version: str,
    *,
    before_date: date,
) -> dict[str, Any] | None:
    rows = read_sql_rows(
        engine,
        """
        SELECT data_date,execution_date,target_json
          FROM st_etf_forward_observation
         WHERE strategy_version=:version AND signal_type='monthly_rebalance'
           AND data_date<:before_date
         ORDER BY data_date DESC
         LIMIT 1
        """,
        {"version": version, "before_date": before_date},
        context="etf_forward_latest_monthly_signal",
        stringify_datetime=True,
    )
    return rows[0] if rows else None


def validate_observation_history(
    engine: Any,
    *,
    version: str,
    current_date: date,
) -> None:
    with engine.connect() as connection:
        latest = connection.execute(
            text(
                """
                SELECT MAX(data_date)
                  FROM st_etf_forward_observation
                 WHERE strategy_version=:version
                """
            ),
            {"version": version},
        ).scalar()
    if latest is not None and pd.Timestamp(latest).date() > current_date:
        raise RuntimeError("ETF forward ledger contains a future observation")


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
                "close": None if pd.isna(close) else round(float(close), 6),
                "amount": None if pd.isna(amount) else round(float(amount), 4),
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
        series = data.close.loc[monthly_signal_date:data_date, code].dropna()
        full = data.close.loc[:data_date, code].dropna()
        if series.empty or len(full) < 21:
            continue
        close = float(series.iloc[-1])
        peak = float(series.max())
        daily_vol = float(
            full.pct_change()
            .dropna()
            .tail(int(risk["volatility_lookback_days"]))
            .std(ddof=1)
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
        revised[CASH_CODE] = float(revised.get(CASH_CODE, 0.0)) + exited_weight
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
        code: weight for code, weight in revised.items() if float(weight) > 1e-8
    }
    total = sum(float(value) for value in revised.values())
    if total > 0:
        revised = {code: float(value) / total for code, value in revised.items()}
    return revised, exits


def build_observation(
    engine: Any,
    config: dict[str, Any],
    config_hash: str,
    data_date: date,
    *,
    observed_at: datetime,
) -> dict[str, Any]:
    version = str(config["strategy_version"])
    source_data = load_market_data(engine, "2019-01-01", data_date.isoformat())
    data, audit = freeze_universe(
        source_data,
        cutoff_date=config["universe"]["cutoff_date"],
        minimum_history_days=int(
            config["universe"]["minimum_history_days_at_cutoff"]
        ),
        minimum_average_amount=float(
            config["universe"]["minimum_average_amount_20_at_cutoff"]
        ),
    )
    eligible = sorted(audit.loc[audit["eligible"], "etf_code"].tolist())
    expected_eligible = sorted(config["universe"]["eligible_codes"])
    if eligible != expected_eligible:
        raise RuntimeError("ETF frozen-universe evidence differs from config")
    data_ts = pd.Timestamp(data_date)
    if data_ts not in data.calendar:
        raise RuntimeError(f"ETF calendar proxy has no close for {data_date}")
    next_date = next_trading_date(engine, data_date)
    if next_date is None:
        raise RuntimeError("ETF trade calendar has no next trading date")

    previous = _latest_observation(
        engine,
        version,
        before_date=data_date,
    )
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
        weights, signal_context = target_weights(data, data_ts, "trend_risk")
        current_target = {str(code): float(weight) for code, weight in weights.items()}
        signal_type = "monthly_rebalance"
        execution_date = next_date
        context.update(signal_context)
    else:
        monthly = _latest_monthly_signal(
            engine,
            version,
            before_date=data_date,
        )
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
        "observed_at": observed_at.replace(tzinfo=None),
        "data_source": PROVIDER_ID,
        "input_hash": _close_input_hash(data, data_ts, eligible),
        "signal_type": signal_type,
        "execution_date": execution_date.isoformat() if execution_date else None,
        "target": current_target,
        "context": context,
    }


def append_observation(engine: Any, observation: dict[str, Any]) -> str:
    existing = read_sql_rows(
        engine,
        """
        SELECT input_hash,config_hash,signal_type,execution_date,
               target_json,context_json
          FROM st_etf_forward_observation
         WHERE strategy_version=:version AND data_date=:data_date
        """,
        {
            "version": observation["strategy_version"],
            "data_date": observation["data_date"],
        },
        context="etf_forward_existing_observation",
        stringify_datetime=True,
    )
    target_json = canonical_json(observation["target"])
    context_json = canonical_json(observation["context"])
    if existing:
        row = existing[0]
        if (
            row["input_hash"] != observation["input_hash"]
            or row["config_hash"] != observation["config_hash"]
            or row["signal_type"] != observation["signal_type"]
            or (str(row.get("execution_date") or "")[:10] or None)
            != observation["execution_date"]
            or json.loads(row["target_json"]) != observation["target"]
            or json.loads(row["context_json"]) != observation["context"]
        ):
            raise RuntimeError(
                "ETF immutable current observation exists with different evidence"
            )
        return "ALREADY_RECORDED"
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO st_etf_forward_observation
                  (strategy_version,config_hash,data_date,observed_at,data_source,
                   input_hash,signal_type,execution_date,target_json,context_json)
                VALUES
                  (:strategy_version,:config_hash,:data_date,:observed_at,:data_source,
                   :input_hash,:signal_type,:execution_date,:target_json,:context_json)
                """
            ),
            {
                **observation,
                "target_json": target_json,
                "context_json": context_json,
            },
        )
    return "CREATED"


def run_forward(
    engine: Any,
    *,
    config_path: Path = DEFAULT_CONFIG,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = now or datetime.now(SHANGHAI)
    if current.tzinfo is None:
        current = current.replace(tzinfo=SHANGHAI)
    current = current.replace(microsecond=0)
    if current.hour * 100 + current.minute < 1505:
        raise RuntimeError("ETF forward observation may append only after 15:05")
    schema = validate_runtime_schema(engine)
    config, config_hash = load_config(config_path)
    latest_date = latest_validated_data_date(engine)
    if latest_date is None:
        raise RuntimeError("ETF forward has no validated market-data date")
    if latest_date != current.date():
        raise RuntimeError(
            "ETF forward backfill prohibited: latest validated close must equal today"
        )
    partition = validate_latest_etf_partition(engine, latest_date)
    registry = register_strategy(engine, config, config_hash)
    registered_at = pd.Timestamp(registry["registered_at"]).to_pydatetime()
    validate_observation_date(
        data_date=latest_date,
        forward_start_date=date.fromisoformat(config["forward_start_date"]),
        registered_at=registered_at,
        local_today=current.date(),
    )
    validate_observation_history(
        engine,
        version=str(config["strategy_version"]),
        current_date=latest_date,
    )
    observation = build_observation(
        engine,
        config,
        config_hash,
        latest_date,
        observed_at=current,
    )
    status = append_observation(engine, observation)
    return _receipt(
        {
            "schema": RECEIPT_SCHEMA,
            "status": "PASS",
            "write_status": status,
            "data_date": latest_date.isoformat(),
            "strategy_version": config["strategy_version"],
            "config_hash": config_hash,
            "input_hash": observation["input_hash"],
            "signal_type": observation["signal_type"],
            "partition": partition,
            "registry": {
                "registration": registry["registration"],
                "registered_at": str(registry["registered_at"]),
            },
            "schema_hash": schema["schema_hash"],
            "automatic_order_submission": False,
        }
    )


def _failure_receipt(*, data_date: str, error: BaseException) -> dict[str, Any]:
    return _receipt(
        {
            "schema": RECEIPT_SCHEMA,
            "status": "DATA_BLOCKED",
            "data_date": data_date,
            "error_type": type(error).__name__,
            "error": str(error)[:1000],
            "automatic_order_submission": False,
        }
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    args = parser.parse_args(argv)
    now = datetime.now(SHANGHAI).replace(microsecond=0)
    if not args.execute:
        config, config_hash = load_config(args.config)
        result = _receipt(
            {
                "schema": RECEIPT_SCHEMA,
                "status": "DRY_RUN",
                "data_date": now.date().isoformat(),
                "strategy_version": config["strategy_version"],
                "config_hash": config_hash,
                "automatic_order_submission": False,
            }
        )
        print(canonical_json(result), flush=True)
        return 0
    try:
        load_project_env()
        engine = create_tool_engine()
        try:
            result = run_forward(engine, config_path=args.config, now=now)
        finally:
            engine.dispose()
    except Exception as exc:
        result = _failure_receipt(data_date=now.date().isoformat(), error=exc)
        print(canonical_json(result), flush=True)
        return 1
    print(canonical_json(result), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
