# -*- coding: utf-8 -*-
"""Research replay for the QMT sector-preheat strategy.

This command intentionally labels a replay that uses a later membership
snapshot as an approximation.  It must not be reported as a point-in-time
forward test.  Production decisions use captured_at <= decision_at in
``server.trading_v2.sector_preheat`` and do not use this approximation.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import sys
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.kline_data import get_kline_engine  # noqa: E402
from server.trading_v2.market_regime import (  # noqa: E402
    classify_market_regime,
)


_SECTOR_SOURCE = os.environ.get("PROBIGA_SECTOR_PREHEAT_SOURCE", "").strip()
if _SECTOR_SOURCE:
    _spec = importlib.util.spec_from_file_location(
        "_probiga_sector_preheat_research",
        _SECTOR_SOURCE,
    )
    if _spec is None or _spec.loader is None:
        raise RuntimeError(
            f"cannot load sector preheat source: {_SECTOR_SOURCE}"
        )
    _module = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_module)
    load_sector_preheat_config = _module.load_sector_preheat_config
    score_sector_preheat = _module.score_sector_preheat
else:
    from server.trading_v2.sector_preheat import (
        load_sector_preheat_config,
        score_sector_preheat,
    )


def _date(value: Any) -> str:
    return str(value or "")[:10]


def _float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError):
        return default


def _candidate_theme_names(candidate: dict[str, Any]) -> list[str]:
    names = {
        str(candidate.get("theme_name") or "").strip(),
        *{
            str(item.get("theme_name") or "").strip()
            for item in candidate.get("theme_matches") or []
        },
    }
    return sorted(name for name in names if name)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="first signal date")
    parser.add_argument("--end", required=True, help="last signal date")
    parser.add_argument(
        "--membership-date",
        default="",
        help="QMT membership snapshot date; default is latest <= end",
    )
    parser.add_argument(
        "--theme",
        action="append",
        default=[],
        help="only report matching theme names in timeline; repeatable",
    )
    parser.add_argument(
        "--theme-unicode-hex",
        action="append",
        default=[],
        help=(
            "theme token as comma-separated Unicode code points, for "
            "locale-safe remote execution"
        ),
    )
    parser.add_argument(
        "--round-trip-cost-pct",
        type=float,
        default=0.25,
        help="approximate buy+sell friction deducted from every return",
    )
    parser.add_argument(
        "--cooldown-sessions",
        type=int,
        default=5,
        help="minimum signal-session gap before reusing the same stock",
    )
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def _latest_snapshot_date(engine, table: str, end: str) -> str:
    with engine.connect() as connection:
        value = connection.execute(
            text(
                f"""
                SELECT MAX(snapshot_date)
                FROM `{table}`
                WHERE snapshot_date <= :end
                  AND quality_status = 'QMT_VALIDATED'
                """
            ),
            {"end": end},
        ).scalar()
    return _date(value)


def _latest_snapshot_date_any(engine, table: str) -> str:
    with engine.connect() as connection:
        value = connection.execute(
            text(
                f"""
                SELECT MAX(snapshot_date)
                FROM `{table}`
                WHERE quality_status = 'QMT_VALIDATED'
                """
            )
        ).scalar()
    return _date(value)


def _load_memberships(
    engine,
    *,
    membership_date: str,
) -> list[dict[str, Any]]:
    memberships: list[dict[str, Any]] = []
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT 'industry' AS sector_type,
                       industry_code AS sector_code,
                       industry_name AS sector_name,
                       stock_code, short_name
                FROM qmt_industry_member_snapshot
                WHERE snapshot_date = :snapshot_date
                  AND quality_status = 'QMT_VALIDATED'
                """
            ),
            {"snapshot_date": membership_date},
        ).mappings()
        memberships.extend(dict(row) for row in rows)
        rows = connection.execute(
            text(
                """
                SELECT 'concept' AS sector_type,
                       concept_code AS sector_code,
                       concept_name AS sector_name,
                       stock_code, short_name
                FROM qmt_concept_member_snapshot
                WHERE snapshot_date = :snapshot_date
                  AND quality_status = 'QMT_VALIDATED'
                """
            ),
            {"snapshot_date": membership_date},
        ).mappings()
        memberships.extend(dict(row) for row in rows)
    return memberships


def _load_bars(
    engine,
    *,
    start: str,
    end: str,
) -> list[dict[str, Any]]:
    history_start = (
        date.fromisoformat(start) - timedelta(days=45)
    ).isoformat()
    forward_end = (
        date.fromisoformat(end) + timedelta(days=15)
    ).isoformat()
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT stock_code, short_name, trade_date, open, close,
                       high, low, pre_close, change_pct, amount
                FROM sm_stock_kline
                WHERE trade_date BETWEEN :history_start AND :forward_end
                  AND k_type = 1
                  AND adjust_type = 0
                  AND data_source = 'gj_big_qmt_inner'
                  AND qmt_code IS NOT NULL
                  AND qmt_code <> ''
                  AND quality_status IN ('VERIFIED', 'QMT_ATTESTED')
                  AND permission_status IN ('SUPPORTED', 'CONFIRMED')
                  AND stock_code REGEXP '^[036][0-9]{5}$'
                ORDER BY trade_date, stock_code
                """
            ),
            {
                "history_start": history_start,
                "forward_end": forward_end,
            },
        ).mappings()
        return [dict(row) for row in rows]


def _profit_stats(returns: list[float]) -> dict[str, Any]:
    gains = sum(value for value in returns if value > 0)
    losses = -sum(value for value in returns if value < 0)
    count = len(returns)
    return {
        "count": count,
        "win_rate_pct": round(
            sum(value > 0 for value in returns) / count * 100.0,
            2,
        )
        if count
        else 0.0,
        "average_return_pct": round(sum(returns) / count, 3)
        if count
        else 0.0,
        "profit_factor": round(gains / losses, 3)
        if losses > 0
        else (999.0 if gains > 0 else 0.0),
    }


def _clamp(value: float) -> float:
    return max(0.0, min(100.0, value))


def _market_regimes(
    by_date: dict[str, list[dict[str, Any]]],
) -> dict[str, dict[str, Any]]:
    aggregates: list[dict[str, Any]] = []
    for day in sorted(by_date):
        changes = [
            _float(row.get("change_pct"))
            for row in by_date[day]
            if row.get("change_pct") is not None
        ]
        if len(changes) < 1000:
            continue
        aggregates.append(
            {
                "trade_date": day,
                "market_change_pct": sum(changes) / len(changes),
                "breadth_pct": (
                    sum(value > 0 for value in changes)
                    / len(changes)
                    * 100.0
                ),
            }
        )
    output: dict[str, dict[str, Any]] = {}
    previous_state = ""
    previous_state_days = 0
    previous_candidate_state = ""
    previous_candidate_streak = 0
    cooldown_remaining = 0
    for index, row in enumerate(aggregates):
        recent5 = aggregates[max(0, index - 4) : index + 1]
        recent20 = aggregates[max(0, index - 19) : index + 1]
        ret5 = sum(item["market_change_pct"] for item in recent5)
        ret20 = sum(item["market_change_pct"] for item in recent20)
        mean5breadth = sum(
            item["breadth_pct"] for item in recent5
        ) / len(recent5)
        breadth = row["breadth_pct"]
        change = row["market_change_pct"]
        trend = _clamp(
            50.0 + ret5 * 3.0 + ret20 + (breadth - 50.0) * 0.4
        )
        switch = _clamp(
            abs(breadth - mean5breadth) * 2.0 + abs(ret5) * 3.0
        )
        risk = _clamp(
            20.0
            + max(0.0, -change) * 10.0
            + max(0.0, 50.0 - breadth) * 1.2
            + max(0.0, -ret5) * 4.0
        )
        inputs = {
            "risk_score": risk,
            "market_change_pct": change,
            "breadth_pct": breadth,
            "trend_score": trend,
            "switch_score": switch,
        }
        decision = classify_market_regime(
            inputs,
            previous_state=previous_state,
            previous_state_days=previous_state_days,
            previous_candidate_state=previous_candidate_state,
            previous_candidate_streak=previous_candidate_streak,
            extreme_cooldown_remaining=cooldown_remaining,
        )
        candidate_streak = (
            previous_candidate_streak + 1
            if decision.candidate_state == previous_candidate_state
            else 1
        )
        state_days = (
            previous_state_days + 1
            if decision.final_state == previous_state
            else 1
        )
        output[row["trade_date"]] = {
            **{key: round(value, 3) for key, value in inputs.items()},
            "candidate_state": decision.candidate_state,
            "market_regime": decision.final_state,
            "candidate_streak": candidate_streak,
            "state_days": state_days,
            "cooldown_remaining": decision.cooldown_remaining,
        }
        previous_state = decision.final_state
        previous_state_days = state_days
        previous_candidate_state = decision.candidate_state
        previous_candidate_streak = candidate_streak
        cooldown_remaining = decision.cooldown_remaining
    return output


def _dynamic_outcome(
    row: dict[str, Any],
    *,
    by_code: dict[str, dict[str, dict[str, Any]]],
    all_dates: list[str],
    date_index: dict[str, int],
    market_regimes: dict[str, dict[str, Any]],
    sector_daily: dict[tuple[str, str], dict[str, Any]],
    cost_pct: float,
) -> dict[str, Any] | None:
    code = row["stock_code"]
    signal_index = date_index.get(row["signal_date"], -1)
    if signal_index < 0 or signal_index + 1 >= len(all_dates):
        return None
    entry_date = all_dates[signal_index + 1]
    entry_bar = by_code.get(code, {}).get(entry_date)
    if not entry_bar:
        return None
    entry_price = _float(entry_bar.get("open"))
    protective_stop = _float(row.get("stop_loss"))
    if (
        entry_price <= 0
        or entry_price > _float(row.get("no_chase_price"))
        or entry_price <= protective_stop
    ):
        return None

    exit_date = ""
    exit_price = 0.0
    exit_reason = ""
    maximum_exit_index = min(signal_index + 5, len(all_dates) - 1)
    for current_index in range(signal_index + 1, maximum_exit_index + 1):
        day = all_dates[current_index]
        bar = by_code.get(code, {}).get(day)
        if not bar:
            continue
        open_price = _float(bar.get("open"))
        close = _float(bar.get("close"))
        low = _float(bar.get("low"), close)
        sellable = current_index > signal_index + 1
        if sellable and protective_stop > 0:
            if open_price > 0 and open_price <= protective_stop:
                exit_date, exit_price, exit_reason = (
                    day,
                    open_price,
                    "PROTECTIVE_STOP_GAP",
                )
                break
            if low > 0 and low <= protective_stop:
                exit_date, exit_price, exit_reason = (
                    day,
                    protective_stop,
                    "PROTECTIVE_STOP",
                )
                break

        day_regime = str(
            (market_regimes.get(day) or {}).get("market_regime") or ""
        )
        sector = sector_daily.get((day, row["theme_code"])) or {}
        sector_broken = (
            _float(sector.get("average_return_1d_pct")) <= -0.8
            and _float(sector.get("positive_breadth_pct")) < 45.0
        )
        code_days = [
            value
            for value in all_dates[: current_index + 1]
            if value in by_code.get(code, {})
        ]
        closes = [
            _float(by_code[code][value].get("close"))
            for value in code_days
        ]
        ma5 = (
            sum(closes[-5:]) / 5.0 if len(closes) >= 5 else None
        )
        ma10 = (
            sum(closes[-10:]) / 10.0 if len(closes) >= 10 else None
        )
        trend_broken = bool(
            ma5 is not None
            and ma10 is not None
            and close < ma5
            and close < ma10
        )
        if sellable and day_regime in {"RISK_OFF", "EXTREME"}:
            exit_date, exit_price, exit_reason = (
                day,
                close,
                f"MARKET_{day_regime}",
            )
            break
        if sellable and sector_broken:
            exit_date, exit_price, exit_reason = (
                day,
                close,
                "SECTOR_TREND_BROKEN",
            )
            break
        if sellable and trend_broken:
            exit_date, exit_price, exit_reason = (
                day,
                close,
                "STOCK_MA5_MA10_BROKEN",
            )
            break
        if ma5 is not None:
            protective_stop = max(
                protective_stop,
                ma5 * 0.97,
            )
        if current_index == maximum_exit_index:
            exit_date, exit_price, exit_reason = (
                day,
                close,
                "FIVE_SESSION_REVIEW",
            )
    if not exit_date or exit_price <= 0:
        return None
    return {
        "entry_date": entry_date,
        "entry_price": round(entry_price, 3),
        "exit_date": exit_date,
        "exit_price": round(exit_price, 3),
        "exit_reason": exit_reason,
        "return_pct": round(
            (exit_price / entry_price - 1.0) * 100.0 - cost_pct,
            3,
        ),
    }


def _portfolio_trial(
    trades: list[dict[str, Any]],
    *,
    maximum_positions: int = 4,
    theme_position_cap: int = 2,
    initial_position_weight: float = 0.11,
) -> dict[str, Any]:
    open_positions: list[dict[str, Any]] = []
    selected: list[dict[str, Any]] = []
    by_entry: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in trades:
        dynamic = row.get("dynamic")
        if dynamic:
            by_entry[dynamic["entry_date"]].append(row)
    for entry_date in sorted(by_entry):
        open_positions = [
            row
            for row in open_positions
            if row["dynamic"]["exit_date"] >= entry_date
        ]
        candidates = sorted(
            by_entry[entry_date],
            key=lambda item: (
                -_float(item.get("score")),
                item["stock_code"],
            ),
        )
        for row in candidates:
            if len(open_positions) >= maximum_positions:
                break
            if any(
                item["stock_code"] == row["stock_code"]
                for item in open_positions
            ):
                continue
            theme_count = sum(
                item["theme_code"] == row["theme_code"]
                for item in open_positions
            )
            if theme_count >= theme_position_cap:
                continue
            open_positions.append(row)
            selected.append(row)
    returns = [
        _float(row["dynamic"]["return_pct"]) for row in selected
    ]
    return {
        "maximum_positions": maximum_positions,
        "theme_position_cap": theme_position_cap,
        "initial_position_weight": initial_position_weight,
        "completed_trade_count": len(selected),
        "trade_stats": _profit_stats(returns),
        "approximate_account_return_pct": round(
            sum(returns) * initial_position_weight,
            3,
        ),
        "trades": selected,
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    engine = get_kline_engine()
    industry_date = _latest_snapshot_date(
        engine,
        "qmt_industry_member_snapshot",
        args.end,
    )
    concept_date = _latest_snapshot_date(
        engine,
        "qmt_concept_member_snapshot",
        args.end,
    )
    available_snapshot_dates = [
        value for value in (industry_date, concept_date) if value
    ]
    if args.membership_date:
        membership_date = args.membership_date
    elif available_snapshot_dates:
        membership_date = min(available_snapshot_dates)
    else:
        fallback_dates = [
            _latest_snapshot_date_any(
                engine,
                "qmt_industry_member_snapshot",
            ),
            _latest_snapshot_date_any(
                engine,
                "qmt_concept_member_snapshot",
            ),
        ]
        membership_date = min(
            value for value in fallback_dates if value
        )
    memberships = _load_memberships(
        engine,
        membership_date=membership_date,
    )
    bars = _load_bars(engine, start=args.start, end=args.end)
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_code: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in bars:
        day = _date(row.get("trade_date"))
        code = str(row.get("stock_code") or "").zfill(6)
        by_date[day].append(row)
        by_code[code][day] = row
    trading_dates = sorted(
        day for day in by_date if args.start <= day <= args.end
    )
    all_dates = sorted(by_date)
    date_index = {day: index for index, day in enumerate(all_dates)}
    market_regimes = _market_regimes(by_date)
    config = load_sector_preheat_config()
    requested_themes = list(args.theme)
    for encoded in args.theme_unicode_hex:
        requested_themes.append(
            "".join(
                chr(int(item, 16))
                for item in encoded.split(",")
                if item.strip()
            )
        )

    signal_rows: list[dict[str, Any]] = []
    sector_daily: dict[tuple[str, str], dict[str, Any]] = {}
    timelines: list[dict[str, Any]] = []
    requested_theme_candidates: list[dict[str, Any]] = []
    hot_theme_counter: Counter[str] = Counter()
    for signal_index, signal_date in enumerate(trading_dates):
        available_bars = [
            row for row in bars if _date(row.get("trade_date")) <= signal_date
        ]
        result = score_sector_preheat(
            memberships=memberships,
            bars=available_bars,
            trade_date=signal_date,
            market_regime=str(
                (market_regimes.get(signal_date) or {}).get(
                    "market_regime"
                )
                or "DATA_BLOCKED"
            ),
            config=config,
        )
        for sector in result["sectors"]:
            sector_daily[(signal_date, sector["sector_code"])] = sector
            if sector["stage"] in {
                "PREHEAT",
                "CONFIRMED",
                "OVERHEATED",
                "COOLDOWN",
            }:
                hot_theme_counter[sector["sector_name"]] += 1
            if requested_themes and any(
                token in str(sector["sector_name"])
                for token in requested_themes
            ):
                timelines.append(
                    {
                        "signal_date": signal_date,
                        "theme_name": sector["sector_name"],
                        "stage": sector["stage"],
                        "stage_reason": sector.get("stage_reason"),
                        "prior_strength_sessions": sector.get(
                            "prior_strength_sessions"
                        ),
                        "discovery_reason_rank": sector.get(
                            "discovery_reason_rank"
                        ),
                        "score": sector["score"],
                        "average_return_1d_pct": sector[
                            "average_return_1d_pct"
                        ],
                        "average_return_5d_pct": sector[
                            "average_return_5d_pct"
                        ],
                        "execution_selected": sector["sector_code"]
                        in set(
                            result.get("execution_hot_sector_codes")
                            or []
                        ),
                        "discovery_selected": sector["sector_code"]
                        in set(
                            result.get("discovery_hot_sector_codes")
                            or []
                        ),
                        "discovery_signal_generated": sector[
                            "sector_code"
                        ]
                        in set(
                            result.get("discovery_signal_sector_codes")
                            or []
                        ),
                    }
                )
        for candidate in result["candidates"]:
            candidate_theme_names = _candidate_theme_names(candidate)
            matched_theme_names = sorted(
                {
                    name
                    for name in candidate_theme_names
                    if any(token in name for token in requested_themes)
                }
            )
            if requested_themes and matched_theme_names:
                requested_theme_candidates.append(
                    {
                        key: candidate.get(key)
                        for key in (
                            "data_date",
                            "stock_code",
                            "stock_name",
                            "theme_name",
                            "sector_stage",
                            "sector_role",
                            "signal_status",
                            "gate_status",
                            "gate_reason",
                            "raw_score",
                            "candidate_return_1d_pct",
                            "candidate_return_3d_pct",
                            "candidate_return_5d_pct",
                            "candidate_amount_ratio_5",
                            "signal_lane",
                        )
                    }
                    | {
                        "matched_theme_names": matched_theme_names,
                        "all_theme_names": candidate_theme_names,
                    }
                )
            if candidate["signal_status"] != "READY":
                continue
            signal_rows.append(
                {
                    "signal_session_index": signal_index,
                    "signal_date": signal_date,
                    "stock_code": candidate["stock_code"],
                    "stock_name": candidate["stock_name"],
                    "theme_name": candidate["theme_name"],
                    "theme_code": candidate["theme_code"],
                    "sector_role": candidate["sector_role"],
                    "score": candidate["raw_score"],
                    "no_chase_price": candidate["no_chase_price"],
                    "stop_loss": candidate["stop_loss"],
                    "sector_stage": candidate["sector_stage"],
                    "candidate_return_1d_pct": candidate[
                        "candidate_return_1d_pct"
                    ],
                    "candidate_return_3d_pct": candidate[
                        "candidate_return_3d_pct"
                    ],
                    "candidate_return_5d_pct": candidate[
                        "candidate_return_5d_pct"
                    ],
                    "candidate_amount_ratio_5": candidate[
                        "candidate_amount_ratio_5"
                    ],
                }
            )

    accepted: list[dict[str, Any]] = []
    last_signal_index: dict[str, int] = {}
    for row in sorted(
        signal_rows,
        key=lambda item: (
            item["signal_date"],
            -float(item["score"]),
            item["stock_code"],
        ),
    ):
        code = row["stock_code"]
        previous = last_signal_index.get(code)
        if (
            previous is not None
            and row["signal_session_index"] - previous
            < args.cooldown_sessions
        ):
            continue
        signal_day_index = date_index.get(row["signal_date"], -1)
        if signal_day_index < 0 or signal_day_index + 1 >= len(all_dates):
            continue
        entry_date = all_dates[signal_day_index + 1]
        entry_bar = by_code.get(code, {}).get(entry_date)
        if not entry_bar:
            continue
        entry = _float(entry_bar.get("open"))
        if (
            entry <= 0
            or entry > _float(row["no_chase_price"])
            or entry <= _float(row["stop_loss"])
        ):
            continue
        accepted_row = dict(row)
        accepted_row.update(
            {
                "entry_date": entry_date,
                "entry_price": round(entry, 3),
                "returns": {},
            }
        )
        for horizon in (1, 3, 5):
            exit_index = signal_day_index + horizon
            if exit_index >= len(all_dates):
                continue
            exit_date = all_dates[exit_index]
            exit_bar = by_code.get(code, {}).get(exit_date)
            if not exit_bar:
                continue
            exit_price = _float(exit_bar.get("close"))
            if exit_price <= 0:
                continue
            accepted_row["returns"][str(horizon)] = round(
                (exit_price / entry - 1.0) * 100.0
                - float(args.round_trip_cost_pct),
                3,
            )
        accepted.append(accepted_row)
        last_signal_index[code] = row["signal_session_index"]

    for row in accepted:
        row["dynamic"] = _dynamic_outcome(
            row,
            by_code=by_code,
            all_dates=all_dates,
            date_index=date_index,
            market_regimes=market_regimes,
            sector_daily=sector_daily,
            cost_pct=float(args.round_trip_cost_pct),
        )

    stats = {}
    for horizon in (1, 3, 5):
        values = [
            row["returns"][str(horizon)]
            for row in accepted
            if str(horizon) in row["returns"]
        ]
        stats[f"{horizon}d"] = _profit_stats(values)
    dynamic_returns = [
        _float(row["dynamic"]["return_pct"])
        for row in accepted
        if row.get("dynamic")
    ]
    dynamic_stats = _profit_stats(dynamic_returns)
    portfolio_trial = _portfolio_trial(accepted)
    theme_returns: dict[str, list[float]] = defaultdict(list)
    for row in accepted:
        if "5" in row["returns"]:
            theme_returns[row["theme_name"]].append(row["returns"]["5"])
    top_theme_results = sorted(
        (
            {
                "theme_name": theme,
                **_profit_stats(values),
            }
            for theme, values in theme_returns.items()
        ),
        key=lambda item: (
            -int(item["count"]),
            -float(item["profit_factor"]),
            item["theme_name"],
        ),
    )[:20]
    return {
        "status": "ok",
        "method": "RESEARCH_APPROXIMATION_CURRENT_MEMBERSHIP",
        "warning": (
            "Membership snapshot may be later than signal date; this is not "
            "a point-in-time forward test and cannot support a profit promise."
        ),
        "strategy_version": config["strategy_version"],
        "start": args.start,
        "end": args.end,
        "membership_snapshot_date": membership_date,
        "membership_row_count": len(memberships),
        "kline_row_count": len(bars),
        "signal_session_count": len(trading_dates),
        "ready_signal_count_before_cooldown": len(signal_rows),
        "simulated_entry_count": len(accepted),
        "cost_assumption_pct": args.round_trip_cost_pct,
        "market_regimes": {
            day: market_regimes.get(day)
            for day in trading_dates
        },
        "stats": stats,
        "dynamic_exit_stats": dynamic_stats,
        "four_slot_portfolio_trial": portfolio_trial,
        "top_hot_themes": [
            {"theme_name": theme, "sessions": count}
            for theme, count in hot_theme_counter.most_common(20)
        ],
        "top_theme_5d_results": top_theme_results,
        "requested_theme_timeline": timelines,
        "requested_theme_candidates": requested_theme_candidates,
        "trades": accepted,
    }


def main() -> int:
    args = _parse_args()
    result = run(args)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, default=str))
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
