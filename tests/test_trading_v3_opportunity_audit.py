from datetime import datetime, timedelta

from server.trading_v3.domain import AlphaForecast
from server.trading_v3.portfolio import _paper_opportunity_audit


def _forecast(code: str, theme: str, score: float) -> AlphaForecast:
    now = datetime(2026, 8, 12, 15, 0)
    return AlphaForecast(
        stock_code=code,
        stock_name=code,
        strategy_key="theme_diffusion",
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
        confidence=0.5,
        status="RESEARCH_ONLY_UNCALIBRATED",
        feature_time=now,
        valid_until=now + timedelta(days=5),
        initial_stop_pct=-5.0,
        raw_score=score,
        features={"theme_names": [theme]},
    )


def test_research_groups_follow_each_decisions_dynamic_market_themes() -> None:
    audit = _paper_opportunity_audit(
        [],
        forecasts=[
            _forecast("002240", "锂产业链", 0.91),
            _forecast("688001", "半导体", 0.84),
        ],
        targets=[],
        rejected=[],
        config={
            "theme_research": {
                "minimum_alert_score": 0.82,
                "maximum_audit_theme_rows": 20,
            },
            "sleeves": {"theme_diffusion": {"enabled": True}},
            "paper_discovery": {"maximum_positions": 3},
        },
    )

    assert audit["research_group_mode"] == "DYNAMIC_EACH_DECISION"
    assert audit["research_group_kind"] == "DYNAMIC_ALL_MARKET_THEME_RADAR"
    assert [row["group"] for row in audit["research_groups"]] == [
        "锂产业链",
        "半导体",
    ]
    assert all(
        row["source"] == "DYNAMIC_ALL_MARKET_THEME"
        for row in audit["research_groups"]
    )
    assert "AI应用" not in {
        row["group"] for row in audit["research_groups"]
    }
    assert "机器人" not in {
        row["group"] for row in audit["research_groups"]
    }
