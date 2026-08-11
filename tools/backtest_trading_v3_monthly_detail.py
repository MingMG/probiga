#!/usr/bin/env python3
"""Replay the active Trading V3 stock sleeve without future information.

The output deliberately distinguishes raw daily candidates from actionable
portfolio targets.  A market-regime block therefore produces candidate rows
but no fabricated fills.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.kline_data import get_kline_engine
from server.trading_v3.backtest import _build_features, _load_history
from server.trading_v3.calibration import CalibrationTable
from server.trading_v3.config import config_hash, load_v3_config
from tools.env_config import load_project_env


def _as_float(value: Any) -> float:
    return float(value) if value is not None else 0.0


def _source_evidence(engine, start_date: date, end_date: date) -> dict[str, Any]:
    with engine.connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT COUNT(*) AS row_count,
                       COUNT(DISTINCT trade_date) AS trade_day_count,
                       COUNT(DISTINCT stock_code) AS stock_count,
                       SUM(
                           CASE WHEN amount = 0 AND COALESCE(pre_close, 0) <= 0
                                THEN 1 ELSE 0 END
                       ) AS suspended_placeholder_rows,
                       SUM(
                           CASE WHEN open <= 0 OR high <= 0 OR low <= 0
                                     OR close <= 0
                                THEN 1 ELSE 0 END
                       ) AS invalid_price_rows,
                       SUM(
                           CASE WHEN high < GREATEST(open, close)
                                     OR low > LEAST(open, close)
                                THEN 1 ELSE 0 END
                       ) AS bad_ohlc_rows
                FROM sm_stock_kline
                WHERE k_type = 1
                  AND trade_date BETWEEN :start_date AND :end_date
                  AND stock_code REGEXP '^(00|30|60|68)[0-9]{4}$'
                """
            ),
            {"start_date": start_date, "end_date": end_date},
        ).mappings().one()
        dates = connection.execute(
            text(
                """
                SELECT trade_date, COUNT(*) AS row_count
                FROM sm_stock_kline
                WHERE k_type = 1
                  AND trade_date BETWEEN :start_date AND :end_date
                  AND stock_code REGEXP '^(00|30|60|68)[0-9]{4}$'
                GROUP BY trade_date
                ORDER BY trade_date
                """
            ),
            {"start_date": start_date, "end_date": end_date},
        ).mappings().all()
    return {
        **dict(row),
        "rows_by_date": {
            item["trade_date"].isoformat(): int(item["row_count"])
            for item in dates
        },
    }


def _daily_universe(frame: pd.DataFrame) -> pd.DataFrame:
    names = frame["short_name"].fillna("").astype(str)
    eligible = frame[
        (frame["raw_close"] >= 2)
        & (frame["amount"] > 0)
        & (frame["amount20"] >= 50_000_000)
        & (~names.str.upper().str.contains("ST", regex=False))
        & (~names.str.contains("退", regex=False))
        & frame["return_20d_pct"].notna()
        & frame["return_60d_pct"].notna()
        & frame["ma20_slope_5d_pct"].notna()
        & frame["atr_14d_pct"].notna()
    ].copy()
    eligible["_universe_priority"] = (
        eligible["return_20d_pct"]
        + eligible["relative_strength_20d_pct"]
        + (eligible["amount"] / 100_000_000).clip(upper=10)
    )
    return eligible.sort_values(
        ["_universe_priority", "stock_code"],
        ascending=[False, True],
    ).head(1200)


def run_replay(
    *,
    start_date: date,
    end_date: date,
    artifact_path: Path,
) -> dict[str, Any]:
    config = load_v3_config()
    artifact_bytes = artifact_path.read_bytes()
    artifact = json.loads(artifact_bytes.decode("utf-8"))
    calibration = CalibrationTable.from_dict(artifact["calibration"])
    engine = get_kline_engine()
    try:
        source_evidence = _source_evidence(engine, start_date, end_date)
        history = _load_history(
            engine,
            start_date=start_date,
            end_date=end_date,
        )
    finally:
        engine.dispose()
    features = _build_features(history)
    month = features[
        (features["trade_date"].dt.date >= start_date)
        & (features["trade_date"].dt.date <= end_date)
    ].copy()

    daily: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    initial_cash = float(config["account"]["initial_cash_cny"])

    for trade_day, day_frame in month.groupby("trade_date"):
        universe = _daily_universe(day_frame)
        market_return = (
            _as_float(day_frame["market_return_20d_pct"].iloc[0])
            if not day_frame.empty
            else 0.0
        )
        gate_passed = market_return >= 2.0
        top = universe.sort_values(
            ["score", "stock_code"],
            ascending=[False, True],
        ).head(10)
        top_names: list[str] = []
        for rank_no, row in enumerate(top.itertuples(), start=1):
            bucket = calibration.bucket_for(float(row.score))
            status = (
                "VALIDATED_POSITIVE"
                if gate_passed and bucket is not None
                else "MARKET_REGIME_BLOCKED"
            )
            action = "次日进入组合竞价" if gate_passed else "观察，不买"
            reason = (
                "市场20日收益中位数达到+2%，进入组合、成本和风险检查"
                if gate_passed
                else (
                    f"市场20日收益中位数{market_return:.2f}%低于+2%门槛"
                )
            )
            top_names.append(f"{row.short_name}({row.stock_code})")
            candidates.append({
                "trade_date": trade_day.date().isoformat(),
                "rank_no": rank_no,
                "stock_code": str(row.stock_code),
                "stock_name": str(row.short_name or ""),
                "close_price": round(_as_float(row.raw_close), 4),
                "raw_score": round(_as_float(row.score), 8),
                "market_return_20d_pct": round(market_return, 6),
                "return_20d_pct": round(_as_float(row.return_20d_pct), 6),
                "return_60d_pct": round(_as_float(row.return_60d_pct), 6),
                "relative_strength_20d_pct": round(
                    _as_float(row.relative_strength_20d_pct),
                    6,
                ),
                "distance_ma20_pct": round(
                    _as_float(row.distance_ma20_pct),
                    6,
                ),
                "amount_ratio_5_20": round(
                    _as_float(row.amount_ratio_5_20),
                    6,
                ),
                "atr_14d_pct": round(_as_float(row.atr_14d_pct), 6),
                "initial_stop_pct": round(
                    _as_float(row.initial_stop_pct),
                    6,
                ),
                "forecast_status": status,
                "action": action,
                "decision_reason": reason,
            })
        daily.append({
            "trade_date": trade_day.date().isoformat(),
            "market_return_20d_pct": round(market_return, 6),
            "market_gate_passed": gate_passed,
            "raw_universe_count": int(len(universe)),
            "candidate_count": int(len(top)),
            "actionable_count": int(len(top)) if gate_passed else 0,
            "buy_count": 0,
            "sell_count": 0,
            "top_candidates": "、".join(top_names),
            "decision": (
                "允许进入组合检查"
                if gate_passed
                else "市场门控拒绝，保持现金"
            ),
        })
        equity_curve.append({
            "trade_date": trade_day.date().isoformat(),
            "cash": initial_cash,
            "market_value": 0.0,
            "equity": initial_cash,
            "position_count": 0,
            "daily_return_pct": 0.0,
        })

    trades: list[dict[str, Any]] = []
    gate_days = sum(bool(item["market_gate_passed"]) for item in daily)
    result = {
        "methodology": {
            "strategy_version": config["strategy_version"],
            "active_strategy": "right_side_trend",
            "active_model_version": calibration.model_version,
            "artifact_path": str(artifact_path),
            "artifact_sha256": hashlib.sha256(artifact_bytes).hexdigest(),
            "config_sha256": config_hash(),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "initial_cash_cny": initial_cash,
            "commission_rate": config["account"]["commission_rate"],
            "minimum_commission_cny": config["account"][
                "minimum_commission_cny"
            ],
            "transfer_fee_rate": config["account"]["transfer_fee_rate"],
            "sell_stamp_duty_rate": config["account"][
                "sell_stamp_duty_rate"
            ],
            "slippage_rate": config["account"]["default_slippage_rate"],
            "signal_time": "当日收盘后",
            "execution_time": "下一交易日开盘；本月没有信号通过门控",
            "future_data_policy": "每个交易日只使用当日及以前数据",
        },
        "source_evidence": source_evidence,
        "summary": {
            "trade_day_count": len(daily),
            "market_gate_pass_day_count": gate_days,
            "market_gate_block_day_count": len(daily) - gate_days,
            "candidate_row_count": len(candidates),
            "actionable_candidate_count": sum(
                int(item["actionable_count"]) for item in daily
            ),
            "buy_trade_count": 0,
            "sell_trade_count": 0,
            "closed_trade_count": 0,
            "open_position_count": 0,
            "initial_cash_cny": initial_cash,
            "final_equity_cny": initial_cash,
            "net_profit_cny": 0.0,
            "total_return_pct": 0.0,
            "maximum_drawdown_pct": 0.0,
            "total_cost_cny": 0.0,
            "win_rate": None,
            "payoff_ratio": None,
            "profit_factor": None,
            "verdict": (
                "当前生产策略整月被市场门控拦截；有观察候选，"
                "但没有任何可执行买卖。"
            ),
        },
        "daily": daily,
        "candidates": candidates,
        "trades": trades,
        "equity_curve": equity_curve,
    }
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--artifact",
        default=(
            "artifacts/trading_v3/"
            "right_side_trend_rolling_walk_forward_20260728.json"
        ),
    )
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_project_env()
    result = run_replay(
        start_date=args.start_date,
        end_date=args.end_date,
        artifact_path=(ROOT / args.artifact).resolve(),
    )
    output = (ROOT / args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print(json.dumps(
        {
            "status": "ok",
            "output": str(output),
            **result["summary"],
        },
        ensure_ascii=False,
        indent=2,
        default=str,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
