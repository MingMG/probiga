from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

from server.trading_v2.candidate_context import apply_candidate_context


def _candidate(status: str = "READY") -> dict:
    return {
        "stock_code": "002303",
        "stock_name": "美盈森",
        "raw_score": 80.0,
        "effective_score": 80.0,
        "model_confidence": 80.0,
        "signal_direction": "BUY",
        "signal_status": status,
        "gate_status": "PASS",
        "gate_reason": "技术和板块条件通过",
        "today_signal": "技术和板块条件通过",
        "candidate_amount_cny": 100_000_000.0,
        "evidence_chain": [],
    }


def _context(*, critical: int = 0) -> dict:
    return {
        "by_code": {
            "002303": {
                "capital_flow": {
                    "flow_trade_date": "2026-07-27",
                    "main_net_inflow_3d": 30_000_000.0,
                    "main_inflow_days_3d": 3,
                    "main_outflow_days_3d": 0,
                },
                "finance": {
                    "report_date": "2026-03-31",
                    "net_profit_yoy_gr": 30.0,
                    "roe_wtd": 10.0,
                    "asset_liab_ratio": 40.0,
                },
                "news": {
                    "news_positive": 1,
                    "news_negative": 0,
                    "news_critical": critical,
                    "positive_titles": ["公司中标"],
                    "risk_titles": ["公司被立案调查"] if critical else [],
                },
                "notice": {},
                "hot_rank": {
                    "snapshot_date": "2026-07-27",
                    "fused_rank": 10,
                },
            }
        },
        "sources": {"linked_news": {"status": "AVAILABLE"}},
        "market": {
            "external": {
                "external_market_status": "SUPPORT",
                "external_market_data_quality": "WATCH",
                "external_market_reason": "股指期货代理偏强",
                "external_market_captured_at": "2026-07-27 14:55:00",
            },
            "market_news": {},
        },
        "context_hash": "context-hash",
    }


def _apply(candidate: dict, context: dict) -> dict:
    snapshot = {
        "candidates": [candidate],
        "execution_candidates": [candidate],
        "discovery_candidates": [],
    }
    with patch(
        "server.trading_v2.candidate_context.load_candidate_context",
        return_value=context,
    ):
        return apply_candidate_context(
            snapshot,
            engine=object(),
            trade_date="2026-07-27",
            decision_at=datetime(2026, 7, 27, 22, 43),
            config={
                "context_overlay": {
                    "enabled": True,
                    "maximum_positive_adjustment": 8.0,
                    "maximum_negative_adjustment": -12.0,
                }
            },
        )


def test_positive_context_is_bounded_and_keeps_ready_candidate() -> None:
    result = _apply(_candidate(), _context())
    signal = result["candidates"][0]
    assert signal["context_adjustment"] == 8.0
    assert signal["raw_score"] == 88.0
    assert signal["signal_status"] == "READY"
    assert signal["context_components"]["capital_flow"] == 4.0
    assert signal["context_components"]["external_market"] == 1.0


def test_positive_context_cannot_promote_watch_to_ready() -> None:
    result = _apply(_candidate("WATCH"), _context())
    signal = result["candidates"][0]
    assert signal["raw_score"] == 88.0
    assert signal["signal_status"] == "WATCH"


def test_validated_critical_event_blocks_entry() -> None:
    result = _apply(_candidate(), _context(critical=1))
    signal = result["candidates"][0]
    assert signal["signal_direction"] == "HOLD"
    assert signal["signal_status"] == "BLOCKED"
    assert signal["gate_status"] == "BLOCK"
    assert "重大消息风险" in signal["gate_reason"]
