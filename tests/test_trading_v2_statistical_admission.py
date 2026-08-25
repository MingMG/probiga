from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from server.trading_v2.health import _health_state
from server.trading_v2.research import (
    evaluate_oos_gate,
    v2_nav_statistical_guard,
)


def _strong_nav(count: int = 160) -> list[dict[str, object]]:
    pattern = (1.8, 1.4, -0.15, 1.1, -0.2, 1.6, 1.3, -0.1)
    start = date(2026, 1, 1)
    return [
        {
            "trade_date": (start + timedelta(days=index)).isoformat(),
            "return_pct": pattern[index % len(pattern)],
        }
        for index in range(count)
    ]


def test_health_inf_or_missing_nav_can_never_be_green():
    point_metrics = {
        "completed_trade_count": 100,
        "expectancy_cny": "100",
        "profit_factor": "INF",
        "max_drawdown": "0.01",
    }
    missing_guard = v2_nav_statistical_guard(
        None,
        minimum_observations=60,
        minimum_effective_sample_size=30,
        minimum_profit_factor_lcb=Decimal("1.10"),
    )
    assert missing_guard["passed"] is False
    assert _health_state(
        point_metrics,
        drawdown_limit=Decimal("0.10"),
        nav_statistical_guard=missing_guard,
    )[0] == "YELLOW"

    strong_guard = v2_nav_statistical_guard(
        _strong_nav(),
        minimum_observations=60,
        minimum_effective_sample_size=30,
        minimum_profit_factor_lcb=Decimal("1.10"),
    )
    assert strong_guard["passed"] is True
    assert _health_state(
        point_metrics,
        drawdown_limit=Decimal("0.10"),
        nav_statistical_guard=strong_guard,
    )[0] == "YELLOW"

    finite_metrics = {**point_metrics, "profit_factor": "1.50"}
    assert _health_state(
        finite_metrics,
        drawdown_limit=Decimal("0.10"),
        nav_statistical_guard=strong_guard,
    ) == ("GREEN", "NORMAL")

    no_negative_support = v2_nav_statistical_guard(
        [
            {
                "trade_date": (
                    date(2026, 1, 1) + timedelta(days=index)
                ).isoformat(),
                "return_pct": 1.0,
            }
            for index in range(160)
        ],
        minimum_observations=60,
        minimum_effective_sample_size=30,
        minimum_profit_factor_lcb=Decimal("1.10"),
    )
    assert no_negative_support["negative_day_count"] == 0
    assert no_negative_support["passed"] is False
    assert _health_state(
        finite_metrics,
        drawdown_limit=Decimal("0.10"),
        nav_statistical_guard=no_negative_support,
    )[0] == "YELLOW"


def test_research_gate_requires_nav_hac_ess_lcb_and_rejects_inf():
    metrics = {
        "completed_trade_count": 200,
        "expectancy_cny": "10",
        "expectancy_r": "0.2",
        "profit_factor": "1.50",
        "payoff_ratio": "1.80",
        "max_drawdown": "0.05",
        "maximum_single_security_profit_contribution": "0.20",
    }
    doubled = {**metrics, "profit_factor": "1.20"}
    common = dict(
        security_scope="A_SHARE",
        trading_days=600,
        oos_windows=150,
        metrics=metrics,
        doubled_cost_metrics=doubled,
        remove_best_three_net_pnl=Decimal("1"),
        robustness={
            "complete": True,
            "block_bootstrap_paths": 2000,
            "positive_parameter_neighborhood_ratio": "0.80",
        },
        future_data_violations=0,
        impossible_fill_profit=Decimal("0"),
    )
    no_nav = evaluate_oos_gate(**common)
    assert no_nav["status"] == "BLOCK"
    assert next(
        item for item in no_nav["checks"]
        if item["code"] == "NAV_HAC_ESS_LCB"
    )["passed"] is False

    passed = evaluate_oos_gate(
        **common,
        nav_records=_strong_nav(),
        doubled_cost_nav_records=_strong_nav(),
    )
    assert passed["status"] == "PASS"
    assert passed["nav_statistical_guard"]["passed"] is True

    inf_metrics = {**metrics, "profit_factor": "INF", "payoff_ratio": "INF"}
    inf_doubled = {**doubled, "profit_factor": "INF"}
    blocked_inf = evaluate_oos_gate(
        **{**common, "metrics": inf_metrics, "doubled_cost_metrics": inf_doubled},
        nav_records=_strong_nav(),
        doubled_cost_nav_records=_strong_nav(),
    )
    assert blocked_inf["status"] == "BLOCK"
    assert next(
        item for item in blocked_inf["checks"]
        if item["code"] == "PROFIT_FACTOR_1_30"
    )["passed"] is False

    point_estimate_only = evaluate_oos_gate(
        **common,
        nav_records=_strong_nav(20),
        doubled_cost_nav_records=_strong_nav(20),
    )
    assert point_estimate_only["status"] == "BLOCK"
    nav_check = next(
        item for item in point_estimate_only["checks"]
        if item["code"] == "NAV_HAC_ESS_LCB"
    )
    assert nav_check["passed"] is False
    assert nav_check["actual"]["observation_count"] == 20
