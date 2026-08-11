from __future__ import annotations

from datetime import date, datetime, timedelta

from server.trading_v3.domain import AlphaForecast
from server.trading_v3.shadow_portfolio import (
    build_shadow_portfolio_rows,
)


NOW = datetime(2026, 8, 1, 15, 0)


def _forecast(
    code: str,
    strategy: str,
    score: float,
    *,
    theme: str = "AI应用",
) -> AlphaForecast:
    return AlphaForecast(
        stock_code=code,
        stock_name=code,
        strategy_key=strategy,
        horizon_days=5,
        expected_return_net_pct=None,
        return_q10_pct=None,
        return_q50_pct=None,
        return_q90_pct=None,
        probability_positive=None,
        expected_mae_pct=None,
        expected_mfe_pct=None,
        profit_factor=None,
        payoff_ratio=None,
        sample_count=0,
        confidence=0.0,
        status="PAPER_DISCOVERY_CANDIDATE",
        feature_time=NOW,
        valid_until=NOW + timedelta(days=5),
        initial_stop_pct=-5.0,
        theme_code=theme,
        raw_score=score,
        features={"theme_cluster_keys": [theme]},
    )


def _theme_signal(
    code: str,
    strategy: str,
    score: float,
    *,
    signal_id: str,
    feature_key: str,
    group: str = "AI_APPLICATION",
) -> dict[str, object]:
    return {
        "theme_signal_id": signal_id,
        "theme_feature_key": feature_key,
        "stock_code": code,
        "short_name": code,
        "strategy_key": strategy,
        "theme_code": "AI_APP",
        "theme_name": "AI应用",
        "theme_cluster_keys": [group],
        "horizon_days": 5,
        "raw_score": score,
        "expected_return_net_pct": None,
        "valid_until": NOW + timedelta(days=5),
    }


def test_shadow_result_key_keeps_same_stock_strategies_distinct():
    forecasts = [
        _forecast("300001", "theme_diffusion", 0.91),
        _forecast("300001", "weak_market_structural_mainline", 0.88),
    ]
    rows = build_shadow_portfolio_rows(
        forecasts,
        run_uid="run-1",
        trade_date=date(2026, 8, 1),
        forecast_ids={
            ("300001", "theme_diffusion"): "forecast-theme",
            (
                "300001",
                "weak_market_structural_mainline",
            ): "forecast-structural",
        },
        policy={
            "strategy_top_k": 20,
            "theme_top_k": 10,
        },
    )
    strategy_rows = [
        row for row in rows if row["portfolio_kind"] == "STRATEGY"
    ]

    assert len(strategy_rows) == 2
    assert len({row["strategy_result_key"] for row in strategy_rows}) == 2
    assert {
        row["source_forecast_id"] for row in strategy_rows
    } == {"forecast-theme", "forecast-structural"}


def test_theme_shadow_selects_one_best_sleeve_per_stock_and_never_orders():
    forecasts = [
        _forecast("300001", "theme_diffusion", 0.91),
        _forecast("300001", "weak_market_structural_mainline", 0.88),
        _forecast("300002", "theme_diffusion", 0.86),
    ]
    forecast_ids = {
        (item.stock_code, item.strategy_key): f"f-{index}"
        for index, item in enumerate(forecasts)
    }
    rows = build_shadow_portfolio_rows(
        forecasts,
        run_uid="run-1",
        trade_date=date(2026, 8, 1),
        forecast_ids=forecast_ids,
        policy={"strategy_top_k": 20, "theme_top_k": 10},
        theme_signals=[
            _theme_signal(
                "300001",
                "theme_diffusion",
                0.91,
                signal_id="ts-1",
                feature_key="key-1",
            ),
            _theme_signal(
                "300001",
                "weak_market_structural_mainline",
                0.88,
                signal_id="ts-2",
                feature_key="key-2",
            ),
            _theme_signal(
                "300002",
                "theme_diffusion",
                0.86,
                signal_id="ts-3",
                feature_key="key-3",
            ),
        ],
    )
    theme_rows = [
        row
        for row in rows
        if row["portfolio_kind"] == "THEME"
        and row["group_key"] == "AI_APPLICATION"
    ]

    assert [row["stock_code"] for row in theme_rows] == [
        "300001",
        "300002",
    ]
    assert theme_rows[0]["strategy_key"] == "theme_diffusion"
    assert all(row["source_theme_signal_id"] for row in theme_rows)
    assert all(row["evidence_kind"] == "SHADOW" for row in rows)
    assert all(row["order_allowed"] == 0 for row in rows)
    assert all(row["can_activate_model"] == 0 for row in rows)
