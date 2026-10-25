from __future__ import annotations

import numpy as np
import pandas as pd

from biz.analysis.intraday_reversal import summarize_stock_minutes
from biz.analysis.sync_analysis_fast import build_recommendation_rows
from biz.analysis.tactical_market import (
    MODE_PANIC_REBOUND,
    attach_daily_tactical_features,
    classify_mode,
    market_snapshot,
)
from tools.import_sina_minute_history import MINUTE_GRID


def _market_frame(
    *,
    trade_date: str,
    positive: int,
    limit_up: int,
    average_hint: str,
) -> pd.DataFrame:
    changes = np.full(100, -2.8)
    changes[:positive] = np.linspace(0.4, 10.0, positive)
    changes[:limit_up] = 9.9
    return pd.DataFrame({
        "trade_date": [trade_date] * 100,
        "change_pct": changes,
        "amount": [1_500_000_000.0] * 100,
        "average_hint": [average_hint] * 100,
    })


def test_panic_rebound_route_uses_previous_stress_and_current_limit_expansion():
    previous = market_snapshot(
        _market_frame(
            trade_date="2026-10-22",
            positive=5,
            limit_up=3,
            average_hint="stress",
        ),
        "2026-10-22",
    )
    current_frame = _market_frame(
        trade_date="2026-10-23",
        positive=85,
        limit_up=80,
        average_hint="repair",
    )
    current_frame["amount"] = 2_000_000_000.0
    current = market_snapshot(current_frame, "2026-10-23")

    decision = classify_mode(current, previous)

    assert decision["mode"] == MODE_PANIC_REBOUND
    assert decision["prior_stress"] is True
    assert decision["limit_expansion"] is True
    assert decision["amount_expansion"] > 1.1
    assert "趋势" not in decision["tactic"]


def test_daily_tactical_features_separate_reacceleration_from_failed_follower():
    context = {
        "mode": MODE_PANIC_REBOUND,
        "label": "恐慌修复",
        "tactic": "龙头反核与同题材低吸",
        "evidence": ["前一交易日恐慌", "当日宽度修复"],
    }
    frame = pd.DataFrame([
        {
            "stock_code": "603278",
            "previous_change_pct": 10.0,
            "pre_close": 14.51,
            "low": 13.23,
            "high": 15.96,
            "close": 15.96,
            "change_pct": 9.99,
            "amount": 1_660_000_000,
            "amount_ratio_5": 2.0,
            "turnover_ratio": 33.42,
        },
        {
            "stock_code": "002724",
            "previous_change_pct": 10.0,
            "pre_close": 11.21,
            "low": 10.09,
            "high": 12.33,
            "close": 12.33,
            "change_pct": 9.99,
            "amount": 1_720_000_000,
            "amount_ratio_5": 2.2,
            "turnover_ratio": 28.10,
        },
        {
            "stock_code": "002076",
            "previous_change_pct": 10.2,
            "pre_close": 2.70,
            "low": 2.43,
            "high": 2.76,
            "close": 2.49,
            "change_pct": -7.78,
            "amount": 950_000_000,
            "amount_ratio_5": 1.8,
            "turnover_ratio": 36.20,
        },
    ])

    scored = attach_daily_tactical_features(frame, context=context)

    assert scored.set_index("stock_code").loc["603278", "tactical_candidate"]
    assert scored.set_index("stock_code").loc["002724", "tactical_candidate"]
    assert not scored.set_index("stock_code").loc["002076", "tactical_candidate"]


def test_intraday_summary_marks_early_leader_and_late_follower():
    times = [
        pd.Timestamp(f"2026-10-23 {clock}") for clock in MINUTE_GRID
    ]
    prices = [13.50] * len(times)
    changes = [-7.0] * len(times)
    for index, stamp in enumerate(times):
        if stamp.strftime("%H:%M") >= "13:10":
            prices[index] = 15.96
            changes[index] = 9.99
    frame = pd.DataFrame({
        "trade_time": times,
        "price": prices,
        "change_pct": changes,
        "volume": [100] * len(times),
        "amount": [150_000.0] * len(times),
        "pre_close": [14.51] * len(times),
    })

    evidence = summarize_stock_minutes(
        frame,
        stock_code="603278",
        source="sina_public_1m",
    )

    assert evidence["minute_rows"] == 238
    assert evidence["minute_early_seal"] is True
    assert evidence["minute_late_seal"] is False
    assert evidence["minute_rebound_from_low_pct"] > 15


def test_tactical_reversal_enters_observation_pool_without_trend_gates():
    frame = pd.DataFrame([
        {
            "stock_code": code,
            "short_name": name,
            "market_mode": MODE_PANIC_REBOUND,
            "market_mode_label": "恐慌修复",
            "tactical_candidate": True,
            "tactical_score": 100.0,
            "tactical_intraday_role": role,
            "tactical_trigger": "回踩VWAP不破后低吸",
            "tactical_entry_low": entry - 0.1,
            "tactical_entry_high": entry + 0.1,
            "tactical_stop_loss": entry * 0.96,
            "tactical_take_profit": entry * 1.08,
            "recommend_status": "SUSPENDED",
            "recommend_reason": "等待次日确认",
            "signal_status": "WATCH",
            "signal_reason": "等待盘中触发",
            "main_wave_signal": "NOT_APPLICABLE_TACTICAL",
            "event_risk_level": "LOW",
            "ai_score": 70,
            "short_term_score": 80,
            "long_term_score": 45,
            "entry_score": 30,
            "final_trade_score": 60,
            "capital_score": 90,
            "technical_score": 100,
            "risk_reward_ratio": 0.0,
            "turnover_ratio": 30,
            "amount": 1_600_000_000,
            "entry_price_low": entry - 0.1,
            "entry_price_high": entry + 0.1,
            "stop_loss_price": entry * 0.96,
            "take_profit_1": entry * 1.08,
            "take_profit_2": entry * 1.15,
        }
        for code, name, role, entry in (
            ("603278", "大业股份", "龙头封板", 14.56),
            ("002724", "海洋王", "尾盘跟随封板", 10.85),
        )
    ])

    rows = build_recommendation_rows(
        frame,
        "2026-10-23",
        top_n=80,
        min_score=62.0,
    )

    assert {row["stock_code"] for row in rows} == {"603278", "002724"}
    assert all(row["signal_status"] == "WATCH" for row in rows)
    assert all("PANIC_REBOUND" in row["sources"] for row in rows)
    assert all(row["recommend_status"] == "SUSPENDED" for row in rows)
