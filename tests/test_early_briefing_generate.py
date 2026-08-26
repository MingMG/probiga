import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from biz.early_briefing import generate
from integrations.wecom.delivery import WeComDeliveryError


TARGET_DATE = "2026-08-25"


def _valid_market_data() -> dict:
    return {
        "A股数据日期": TARGET_DATE,
        "DB快讯": [],
        "_data_contract": {
            "status": "PASS",
            "target_trade_date": TARGET_DATE,
            "expected_stock_count": 5200,
            "kline_coverage": 1.0,
            "traded_flow_coverage": 1.0,
            "index_count": 5,
            "hot_concept_count": 20,
            "fused_stock_count": 20,
        },
    }


def test_delivery_error_propagates_out_of_early_briefing_main(monkeypatch):
    engine = object()
    market_data = _valid_market_data()
    delivery_error = WeComDeliveryError(
        "briefing webhook is not configured",
        delivery_id="delivery-early-test",
    )
    monkeypatch.setattr(sys, "argv", ["early-briefing"])
    monkeypatch.setattr(generate, "get_engine", lambda: engine)
    monkeypatch.setattr(
        generate, "resolve_target_trade_date", Mock(return_value=TARGET_DATE)
    )
    collect = Mock(return_value=market_data)
    monkeypatch.setattr(generate, "collect_market_data", collect)
    monkeypatch.setattr(generate, "crawl_all_news", Mock(return_value=[]))
    monkeypatch.setattr(
        generate,
        "get_settings",
        lambda: SimpleNamespace(deepseek_api_key=None),
    )
    monkeypatch.setattr(generate, "_generate_fallback", Mock(return_value="早报正文"))
    monkeypatch.setattr(generate, "append_tech_risk", lambda content, _data: content)
    monkeypatch.setattr(generate, "append_research_radar", lambda content, _data: content)
    monkeypatch.setattr(generate, "get_wecom_webhook", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(generate, "deliver_markdown", Mock(side_effect=delivery_error))

    with pytest.raises(WeComDeliveryError) as exc_info:
        generate.main()

    assert exc_info.value is delivery_error
    collect.assert_called_once_with(engine, TARGET_DATE)


def test_stale_contract_fails_before_news_or_delivery(monkeypatch):
    engine = object()
    market_data = _valid_market_data()
    market_data["_data_contract"]["target_trade_date"] = "2026-08-22"
    crawl = Mock(side_effect=AssertionError("stale briefing must not crawl news"))
    push = Mock(side_effect=AssertionError("stale briefing must not be delivered"))
    monkeypatch.setattr(sys, "argv", ["early-briefing"])
    monkeypatch.setattr(generate, "get_engine", lambda: engine)
    monkeypatch.setattr(
        generate, "resolve_target_trade_date", Mock(return_value=TARGET_DATE)
    )
    monkeypatch.setattr(generate, "collect_market_data", Mock(return_value=market_data))
    monkeypatch.setattr(generate, "crawl_all_news", crawl)
    monkeypatch.setattr(generate, "push_to_wecom", push)

    with pytest.raises(RuntimeError, match="DATA_BLOCKED.*stale or incomplete"):
        generate.main()

    crawl.assert_not_called()
    push.assert_not_called()


def test_core_snapshot_rejects_mixed_kline_date(monkeypatch):
    reads = iter(
        [
            [{"stock_code": "000001", "trade_date": "2026-08-22", "volume": 1, "amount": 1}],
            [{"stock_code": "000001", "trade_date": TARGET_DATE, "main_net_inflow": 1}],
        ]
    )
    monkeypatch.setattr(generate, "_read_required_sql", lambda *_args, **_kwargs: next(reads))

    with pytest.raises(RuntimeError, match="DATA_BLOCKED.*mixed dates"):
        generate._load_core_market_snapshot(object(), TARGET_DATE)
