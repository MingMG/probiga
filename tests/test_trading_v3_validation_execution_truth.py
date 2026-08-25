from server.trading_v3.validation import model_gate_failures

import pytest


def _config():
    return {
        "decision_intelligence": {
            "execution_revalidation_required": True,
        },
        "profit_gate": {
            "minimum_oos_samples": 80,
            "minimum_expected_return_net_pct": 0.0,
            "minimum_profit_factor": 1.3,
            "minimum_payoff_ratio": 1.0,
            "minimum_portfolio_trades": 80,
            "minimum_portfolio_net_expectancy_pct": 0.0,
            "minimum_portfolio_profit_factor": 1.3,
            "minimum_portfolio_payoff_ratio": 1.0,
            "maximum_drawdown_pct": 12.0,
        },
    }


def _validation():
    return {
        "sample_count": 100,
        "net_expectancy_pct": 0.5,
        "profit_factor": 1.5,
        "payoff_ratio": 1.2,
        "execution_evidence_valid": True,
    }


def _portfolio():
    return {
        "trade_count": 100,
        "net_expectancy_pct": 0.4,
        "profit_factor": 1.4,
        "payoff_ratio": 1.1,
        "maximum_drawdown_pct": 8.0,
        "net_profit_cny": 10_000.0,
        "execution_evidence_valid": True,
    }


def test_execution_revalidation_is_a_real_model_gate_not_decorative_metadata():
    assert model_gate_failures(
        validation=_validation(),
        portfolio=_portfolio(),
        config=_config(),
    ) == ()

    validation = _validation()
    validation.pop("execution_evidence_valid")
    portfolio = _portfolio()
    portfolio["execution_evidence_valid"] = False

    failures = model_gate_failures(
        validation=validation,
        portfolio=portfolio,
        config=_config(),
    )

    assert "OOS_EXECUTION_EVIDENCE_INVALID" in failures
    assert "PORTFOLIO_EXECUTION_EVIDENCE_INVALID" in failures


def test_boolean_cannot_impersonate_a_numeric_profit_gate_metric():
    validation = _validation()
    validation["sample_count"] = True

    failures = model_gate_failures(
        validation=validation,
        portfolio=_portfolio(),
        config=_config(),
    )

    assert "OOS_SAMPLE_COUNT_MISSING" in failures


@pytest.mark.parametrize(
    ("location", "field", "failure"),
    [
        ("validation", "profit_factor", "OOS_PROFIT_FACTOR_MISSING"),
        ("validation", "payoff_ratio", "OOS_PAYOFF_RATIO_MISSING"),
        ("portfolio", "profit_factor", "PORTFOLIO_PROFIT_FACTOR_MISSING"),
        ("portfolio", "payoff_ratio", "PORTFOLIO_PAYOFF_RATIO_MISSING"),
        ("portfolio", "net_profit_cny", "PORTFOLIO_NET_PROFIT_MISSING"),
    ],
)
def test_non_finite_metric_never_passes_an_old_point_gate(
    location,
    field,
    failure,
):
    validation = _validation()
    portfolio = _portfolio()
    target = validation if location == "validation" else portfolio
    target[field] = float("inf")

    failures = model_gate_failures(
        validation=validation,
        portfolio=portfolio,
        config=_config(),
    )

    assert failure in failures
