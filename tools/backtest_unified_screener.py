#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only forward-return backtest for the unified screener presets.

Returns are chained with each session's official ``pre_close`` reference
instead of raw cross-day close ratios. This avoids false jumps caused by
ex-right/ex-dividend price discontinuities while preserving the actual
T+1-open entry price.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.api.routers.screener import PRESETS, ScreenerRunRequest, _run_preset
from server.common.batch_db import create_batch_engine, read_frame_direct
from server.common.kline_data import get_kline_engine
from tools.audit_screener_input_range import audit_inputs

HORIZONS = (1, 5, 20)
RELEASE_THRESHOLDS = {
    "minimum_universe_coverage": 0.95,
    "minimum_mature_samples_per_horizon": 80,
    "minimum_oos_profit_factor": 1.30,
    "minimum_oos_average_win_loss": 1.0,
    "minimum_shadow_sessions": 20,
    "maximum_data_missing_rate": 0.05,
}


def _trade_dates(start_date: str, end_date: str) -> list[str]:
    with create_batch_engine().connect() as conn:
        rows = conn.execute(text("""
            SELECT trade_date
            FROM si_trade_calendar
            WHERE trade_status = 1
              AND trade_date BETWEEN :start_date AND :end_date
            ORDER BY trade_date
        """), {"start_date": start_date, "end_date": end_date}).fetchall()
    return [str(row[0])[:10] for row in rows]


def _load_prices(start_date: str, end_date: str) -> pd.DataFrame:
    frame = read_frame_direct(text("""
            SELECT
              stock_code, short_name, trade_date,
              `open`, `high`, `low`, `close`,
              volume, amount, pre_close, change_pct
            FROM sm_stock_kline
            WHERE trade_date BETWEEN :start_date AND :end_date
              AND k_type = 1
              AND adjust_type = 0
              AND stock_code REGEXP '^(00|30|60|68|92)[0-9]{4}$'
            ORDER BY trade_date, stock_code
        """), get_kline_engine(), params={"start_date": start_date, "end_date": end_date})
    if frame.empty:
        return frame
    frame["stock_code"] = frame["stock_code"].astype(str).str.strip().str.zfill(6)
    frame["trade_date"] = frame["trade_date"].astype(str).str[:10]
    for column in ("open", "high", "low", "close", "volume", "amount", "pre_close", "change_pct"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _dataset_hash(frame: pd.DataFrame) -> str:
    columns = [
        "stock_code", "trade_date", "open", "high", "low", "close",
        "volume", "amount", "pre_close",
    ]
    payload = frame[columns].to_json(orient="records", double_precision=10)
    return hashlib.sha256(payload.encode()).hexdigest()


def _data_audit(frame: pd.DataFrame, expected_dates: list[str]) -> dict[str, Any]:
    actual_dates = sorted(frame["trade_date"].unique().tolist()) if not frame.empty else []
    duplicate_count = int(frame.duplicated(["stock_code", "trade_date"]).sum()) if not frame.empty else 0
    if frame.empty:
        bad_ohlc = 0
        invalid_price = 0
        missing_pre_close = 0
        inconsistent_reference_returns = 0
    else:
        bad_mask = (
            frame["high"].lt(frame[["open", "low", "close"]].max(axis=1))
            | frame["low"].gt(frame[["open", "high", "close"]].min(axis=1))
        )
        bad_ohlc = int(bad_mask.sum())
        invalid_price = int(
            (frame[["open", "high", "low", "close"]].isna().any(axis=1)
             | frame[["open", "high", "low", "close"]].le(0).any(axis=1)).sum()
        )
        missing_pre_close = int((frame["pre_close"].isna() | frame["pre_close"].le(0)).sum())
        reference_mask = (
            frame["pre_close"].notna()
            & frame["pre_close"].gt(0)
            & frame["close"].notna()
            & frame["change_pct"].notna()
        )
        reference_delta = (
            (frame.loc[reference_mask, "close"] / frame.loc[reference_mask, "pre_close"] - 1.0) * 100.0
            - frame.loc[reference_mask, "change_pct"]
        ).abs()
        inconsistent_reference_returns = int(reference_delta.gt(0.05).sum())
    return {
        "expected_trade_dates": expected_dates,
        "actual_trade_dates": actual_dates,
        "missing_trade_dates": sorted(set(expected_dates) - set(actual_dates)),
        "row_count": len(frame),
        "stock_count": int(frame["stock_code"].nunique()) if not frame.empty else 0,
        "duplicate_business_keys": duplicate_count,
        "bad_ohlc": bad_ohlc,
        "invalid_prices": invalid_price,
        "missing_pre_close_rows": missing_pre_close,
        "inconsistent_reference_return_rows": inconsistent_reference_returns,
        "dataset_sha256": _dataset_hash(frame) if not frame.empty else "",
    }


def _collect_signals(trade_dates: list[str], top: int) -> tuple[dict[str, list[dict]], list[dict]]:
    signals: dict[str, list[dict]] = defaultdict(list)
    run_audit: list[dict] = []
    for target_date in trade_dates:
        for preset in PRESETS:
            key = str(preset["key"])
            try:
                result = _run_preset(
                    ScreenerRunRequest(
                        preset=key,
                        as_of_date=target_date,
                        universe="market",
                        top=top,
                        filters={},
                    ),
                    target_date,
                )
            except Exception as exc:  # pylint: disable=broad-except
                run_audit.append({
                    "date": target_date,
                    "preset": key,
                    "status": "error",
                    "error": f"{type(exc).__name__}: {str(exc)[:300]}",
                    "count": 0,
                })
                continue
            data_date = str(result.get("data_date") or "")[:10]
            freshness = str(result.get("freshness") or "")
            rows = result.get("data") or []
            accepted = data_date == target_date and freshness not in {"fallback", "unavailable", "error"}
            run_audit.append({
                "date": target_date,
                "preset": key,
                "status": "accepted" if accepted else "excluded",
                "data_date": data_date,
                "freshness": freshness,
                "count": len(rows) if accepted else 0,
            })
            if not accepted:
                continue
            for row in rows[:top]:
                code = str(row.get("stock_code") or "").strip().zfill(6)
                if code:
                    signals[key].append({
                        "signal_date": target_date,
                        "stock_code": code,
                        "stock_name": str(row.get("stock_name") or row.get("short_name") or ""),
                        "rank": row.get("rank"),
                        "score": row.get("score"),
                    })
    return dict(signals), run_audit


def _forward_return(
    price_map: dict[tuple[str, str], dict[str, Any]],
    trade_dates: list[str],
    date_index: dict[str, int],
    signal_date: str,
    code: str,
    horizon: int,
) -> tuple[float | None, str]:
    signal_index = date_index[signal_date]
    entry_index = signal_index + 1
    exit_index = signal_index + horizon
    if entry_index >= len(trade_dates) or exit_index >= len(trade_dates):
        return None, "insufficient_forward_dates"
    entry_date = trade_dates[entry_index]
    entry = price_map.get((code, entry_date))
    if not entry:
        return None, "missing_entry_bar"
    entry_open = float(entry.get("open") or 0)
    entry_close = float(entry.get("close") or 0)
    if entry_open <= 0 or entry_close <= 0 or float(entry.get("volume") or 0) <= 0:
        return None, "untradable_entry_bar"

    factor = entry_close / entry_open
    for index in range(entry_index + 1, exit_index + 1):
        row = price_map.get((code, trade_dates[index]))
        if not row:
            return None, "missing_holding_bar"
        close = float(row.get("close") or 0)
        pre_close = float(row.get("pre_close") or 0)
        if close <= 0 or pre_close <= 0:
            return None, "missing_official_reference_price"
        factor *= close / pre_close
    value = factor - 1.0
    if not math.isfinite(value):
        return None, "non_finite_return"
    return value, "ok"


def _summary(values: list[float], round_trip_cost: float) -> dict[str, Any]:
    if not values:
        return {
            "sample": 0,
            "gross_average_pct": None,
            "gross_win_rate_pct": None,
            "gross_profit_factor": None,
            "net_average_pct": None,
            "net_win_rate_pct": None,
            "net_profit_factor": None,
            "net_average_win_loss": None,
            "net_max_drawdown_pct": None,
        }

    def _metrics(series: list[float]) -> tuple[float, float, float | None, float | None, float]:
        positives = sum(value for value in series if value > 0)
        negatives = -sum(value for value in series if value < 0)
        profit_factor = positives / negatives if negatives > 0 else None
        wins = [value for value in series if value > 0]
        losses = [-value for value in series if value < 0]
        average_win_loss = (
            (sum(wins) / len(wins)) / (sum(losses) / len(losses))
            if wins and losses
            else None
        )
        wealth = 1.0
        peak = 1.0
        max_drawdown = 0.0
        for value in series:
            wealth *= 1.0 + value
            peak = max(peak, wealth)
            if peak > 0:
                max_drawdown = max(max_drawdown, (peak - wealth) / peak)
        return (
            sum(series) / len(series) * 100,
            sum(value > 0 for value in series) / len(series) * 100,
            profit_factor,
            average_win_loss,
            max_drawdown * 100,
        )

    gross_avg, gross_win, gross_pf, _gross_awl, _gross_drawdown = _metrics(values)
    net_values = [value - round_trip_cost for value in values]
    net_avg, net_win, net_pf, net_awl, net_drawdown = _metrics(net_values)
    return {
        "sample": len(values),
        "gross_average_pct": round(gross_avg, 4),
        "gross_win_rate_pct": round(gross_win, 2),
        "gross_profit_factor": round(gross_pf, 4) if gross_pf is not None else None,
        "net_average_pct": round(net_avg, 4),
        "net_win_rate_pct": round(net_win, 2),
        "net_profit_factor": round(net_pf, 4) if net_pf is not None else None,
        "net_average_win_loss": round(net_awl, 4) if net_awl is not None else None,
        "net_max_drawdown_pct": round(net_drawdown, 4),
    }


def _release_decision(
    horizons: dict[str, dict[str, Any]],
    audit: dict[str, Any],
    shadow_sessions: int,
) -> dict[str, Any]:
    """Evaluate evidence gates without ever granting automatic order authority."""
    expected = len(audit.get("expected_trade_dates") or [])
    actual = len(audit.get("actual_trade_dates") or [])
    coverage = actual / expected if expected else 0.0
    row_count = int(audit.get("row_count") or 0)
    defect_rows = sum(
        int(audit.get(key) or 0)
        for key in (
            "duplicate_business_keys",
            "bad_ohlc",
            "invalid_prices",
            "missing_pre_close_rows",
            "inconsistent_reference_return_rows",
        )
    )
    missing_rate = min(1.0, defect_rows / row_count) if row_count else 1.0
    checks: dict[str, bool] = {
        "universe_coverage": coverage >= RELEASE_THRESHOLDS["minimum_universe_coverage"],
        "data_missing_rate": missing_rate <= RELEASE_THRESHOLDS["maximum_data_missing_rate"],
        "shadow_sessions": shadow_sessions >= RELEASE_THRESHOLDS["minimum_shadow_sessions"],
    }
    horizon_details: dict[str, Any] = {}
    for horizon in ("T+1", "T+5", "T+20"):
        metrics = horizons.get(horizon) or {}
        detail = {
            "mature_samples": int(metrics.get("sample") or 0) >= RELEASE_THRESHOLDS["minimum_mature_samples_per_horizon"],
            "profit_factor": (metrics.get("net_profit_factor") or 0) >= RELEASE_THRESHOLDS["minimum_oos_profit_factor"],
            "average_win_loss": (metrics.get("net_average_win_loss") or 0) >= RELEASE_THRESHOLDS["minimum_oos_average_win_loss"],
        }
        horizon_details[horizon] = detail
        checks[f"{horizon}_evidence"] = all(detail.values())
    passed = all(checks.values())
    return {
        "status": "PASS_ADVISORY_RELEASE" if passed else "SHADOW_ONLY",
        "passed": passed,
        "checks": checks,
        "horizons": horizon_details,
        "observed": {
            "universe_coverage": round(coverage, 6),
            "data_missing_rate": round(missing_rate, 6),
            "shadow_sessions": shadow_sessions,
        },
        "thresholds": dict(RELEASE_THRESHOLDS),
        "order_authority": False,
        "automatic_real_order_submission": False,
    }


def _benchmark_comparison(
    pairs: list[tuple[float, float]],
    round_trip_cost: float,
) -> dict[str, Any]:
    if not pairs:
        return {
            "benchmark_sample": 0,
            "market_average_pct": None,
            "gross_excess_average_pct": None,
            "net_excess_average_pct": None,
            "net_excess_win_rate_pct": None,
        }
    market = [benchmark for _value, benchmark in pairs]
    gross_excess = [value - benchmark for value, benchmark in pairs]
    net_excess = [value - benchmark - round_trip_cost for value, benchmark in pairs]
    return {
        "benchmark_sample": len(pairs),
        "market_average_pct": round(sum(market) / len(market) * 100, 4),
        "gross_excess_average_pct": round(
            sum(gross_excess) / len(gross_excess) * 100,
            4,
        ),
        "net_excess_average_pct": round(
            sum(net_excess) / len(net_excess) * 100,
            4,
        ),
        "net_excess_win_rate_pct": round(
            sum(value > 0 for value in net_excess) / len(net_excess) * 100,
            2,
        ),
    }


def _market_benchmark_by_date(
    price_map: dict[tuple[str, str], dict[str, Any]],
    trade_dates: list[str],
    date_index: dict[str, int],
    stock_codes: list[str],
) -> dict[tuple[str, int], float]:
    benchmark: dict[tuple[str, int], float] = {}
    for signal_date in trade_dates:
        for horizon in HORIZONS:
            values: list[float] = []
            for code in stock_codes:
                value, _reason = _forward_return(
                    price_map,
                    trade_dates,
                    date_index,
                    signal_date,
                    code,
                    horizon,
                )
                if value is not None:
                    values.append(value)
            if values:
                benchmark[(signal_date, horizon)] = sum(values) / len(values)
    return benchmark


def run_backtest(
    start_date: str,
    end_date: str,
    *,
    top: int = 10,
    round_trip_cost: float = 0.002,
) -> dict[str, Any]:
    trade_dates = _trade_dates(start_date, end_date)
    dependency_start = (
        datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=80)
    ).strftime("%Y-%m-%d")
    dependency_dates = _trade_dates(dependency_start, end_date)
    prices = _load_prices(dependency_start, end_date)
    audit = _data_audit(prices, dependency_dates)
    screener_input_audit = audit_inputs(start_date, end_date)
    hard_failures = {
        "missing_trade_dates": audit["missing_trade_dates"],
        "duplicate_business_keys": audit["duplicate_business_keys"],
        "bad_ohlc": audit["bad_ohlc"],
        "invalid_prices": audit["invalid_prices"],
        "inconsistent_reference_return_rows": audit["inconsistent_reference_return_rows"],
        "screener_inputs": (
            screener_input_audit["hard_failures"]
            if screener_input_audit.get("status") != "pass"
            else {}
        ),
    }
    if (
        hard_failures["missing_trade_dates"]
        or hard_failures["duplicate_business_keys"]
        or hard_failures["bad_ohlc"]
        or hard_failures["invalid_prices"]
        or hard_failures["inconsistent_reference_return_rows"]
        or hard_failures["screener_inputs"]
    ):
        return {
            "status": "blocked",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "start_date": start_date,
            "end_date": end_date,
            "data_dependency_start": dependency_start,
            "data_audit": audit,
            "screener_input_audit": screener_input_audit,
            "hard_failures": hard_failures,
        }

    signals, run_audit = _collect_signals(trade_dates, top)
    price_map = {
        (str(row["stock_code"]), str(row["trade_date"])): row
        for row in prices.to_dict(orient="records")
    }
    date_index = {value: index for index, value in enumerate(trade_dates)}
    benchmark_by_date = _market_benchmark_by_date(
        price_map,
        trade_dates,
        date_index,
        sorted(prices["stock_code"].astype(str).unique().tolist()),
    )

    strategy_results: dict[str, Any] = {}
    combined_unique: dict[tuple[str, str], dict[str, Any]] = {}
    exclusions: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    exclusion_samples: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for preset in PRESETS:
        key = str(preset["key"])
        preset_signals = signals.get(key, [])
        horizon_values: dict[int, list[float]] = {horizon: [] for horizon in HORIZONS}
        horizon_benchmark_pairs: dict[int, list[tuple[float, float]]] = {
            horizon: [] for horizon in HORIZONS
        }
        for signal in preset_signals:
            combined_unique.setdefault(
                (signal["signal_date"], signal["stock_code"]),
                signal,
            )
            for horizon in HORIZONS:
                value, reason = _forward_return(
                    price_map,
                    trade_dates,
                    date_index,
                    signal["signal_date"],
                    signal["stock_code"],
                    horizon,
                )
                if value is None:
                    exclusion_key = f"T+{horizon}:{reason}"
                    exclusions[key][exclusion_key] += 1
                    samples = exclusion_samples[key][exclusion_key]
                    if len(samples) < 5:
                        samples.append(dict(signal))
                else:
                    horizon_values[horizon].append(value)
                    benchmark = benchmark_by_date.get(
                        (signal["signal_date"], horizon)
                    )
                    if benchmark is not None:
                        horizon_benchmark_pairs[horizon].append(
                            (value, benchmark)
                        )
        strategy_results[key] = {
            "name": preset["name"],
            "signal_count": len(preset_signals),
            "horizons": {
                f"T+{horizon}": {
                    **_summary(horizon_values[horizon], round_trip_cost),
                    **_benchmark_comparison(
                        horizon_benchmark_pairs[horizon],
                        round_trip_cost,
                    ),
                }
                for horizon in HORIZONS
            },
            "exclusions": dict(exclusions[key]),
            "exclusion_samples": {
                reason: samples
                for reason, samples in exclusion_samples[key].items()
            },
        }

    combined_values: dict[int, list[float]] = {horizon: [] for horizon in HORIZONS}
    combined_benchmark_pairs: dict[int, list[tuple[float, float]]] = {
        horizon: [] for horizon in HORIZONS
    }
    combined_exclusions: dict[str, int] = defaultdict(int)
    combined_exclusion_samples: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for signal in combined_unique.values():
        for horizon in HORIZONS:
            value, reason = _forward_return(
                price_map,
                trade_dates,
                date_index,
                signal["signal_date"],
                signal["stock_code"],
                horizon,
            )
            if value is None:
                exclusion_key = f"T+{horizon}:{reason}"
                combined_exclusions[exclusion_key] += 1
                if len(combined_exclusion_samples[exclusion_key]) < 5:
                    combined_exclusion_samples[exclusion_key].append(
                        dict(signal)
                    )
            else:
                combined_values[horizon].append(value)
                benchmark = benchmark_by_date.get(
                    (signal["signal_date"], horizon)
                )
                if benchmark is not None:
                    combined_benchmark_pairs[horizon].append(
                        (value, benchmark)
                    )

    combined_horizons = {
        f"T+{horizon}": {
            **_summary(combined_values[horizon], round_trip_cost),
            **_benchmark_comparison(
                combined_benchmark_pairs[horizon],
                round_trip_cost,
            ),
        }
        for horizon in HORIZONS
    }
    release_decision = _release_decision(
        combined_horizons,
        audit,
        len(trade_dates),
    )

    return {
        "status": "ok",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "start_date": start_date,
        "end_date": end_date,
        "data_dependency_start": dependency_start,
        "trade_date_count": len(trade_dates),
        "top_per_preset_per_day": top,
        "round_trip_cost": round_trip_cost,
        "return_method": (
            "entry T+1 open; entry-day close/open; subsequent sessions close/pre_close; "
            "pre_close is the official ex-right reference"
        ),
        "benchmark_method": (
            "same-signal-date and same-horizon equal-weight return of all "
            "tradable audited A-share daily bars"
        ),
        "data_audit": audit,
        "screener_input_audit": screener_input_audit,
        "screener_run_audit": run_audit,
        "release_decision": release_decision,
        "strategies": strategy_results,
        "combined": {
            "unique_signal_count": len(combined_unique),
            "horizons": combined_horizons,
            "exclusions": dict(combined_exclusions),
            "exclusion_samples": dict(combined_exclusion_samples),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--round-trip-cost", type=float, default=0.002)
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    report = run_backtest(
        args.start_date,
        args.end_date,
        top=max(1, min(args.top, 200)),
        round_trip_cost=max(0.0, args.round_trip_cost),
    )
    payload = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    if args.output:
        output = Path(args.output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(payload + "\n", encoding="utf-8")
        print(output)
    else:
        print(payload)
    return 0 if report.get("status") == "ok" else 3


if __name__ == "__main__":
    raise SystemExit(main())
