from __future__ import annotations

import pytest

from server.trading_v3.config import load_v3_config
from server.trading_v3.validation import model_gate_failures
from tools.register_trading_v3_artifact import _verify_blocked_gate


def _blocked_payload() -> dict:
    validation = {
        "sample_count": 10,
        "net_expectancy_pct": -3.64,
        "profit_factor": 0.14,
        "payoff_ratio": 1.26,
    }
    portfolio = {
        "trade_count": 8,
        "net_expectancy_pct": -3.34,
        "profit_factor": 0.18,
        "payoff_ratio": 1.27,
        "maximum_drawdown_pct": 1.83,
        "net_profit_cny": -2357.54,
        "total_cost_cny": 185.54,
    }
    failures = model_gate_failures(
        validation=validation,
        portfolio=portfolio,
        config=load_v3_config(),
    )
    return {
        "gate_status": "BLOCK",
        "block_reasons": list(failures),
        "validation_metrics": validation,
        "portfolio_metrics": portfolio,
    }


def test_blocked_artifact_can_only_be_recorded_with_matching_gate_reasons():
    payload = _blocked_payload()

    assert _verify_blocked_gate(payload, load_v3_config()) == tuple(
        payload["block_reasons"]
    )


def test_blocked_artifact_recording_rejects_missing_gate_reason():
    payload = _blocked_payload()
    payload["block_reasons"] = payload["block_reasons"][:-1]

    with pytest.raises(RuntimeError, match="do not match"):
        _verify_blocked_gate(payload, load_v3_config())
