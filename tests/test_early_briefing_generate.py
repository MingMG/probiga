import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from biz.early_briefing import generate
from integrations.wecom.delivery import WeComDeliveryError


def test_delivery_error_propagates_out_of_early_briefing_main(monkeypatch):
    engine = object()
    market_data = {"DB快讯": []}
    delivery_error = WeComDeliveryError(
        "briefing webhook is not configured",
        delivery_id="delivery-early-test",
    )
    monkeypatch.setattr(sys, "argv", ["early-briefing"])
    monkeypatch.setattr(generate, "get_engine", lambda: engine)
    monkeypatch.setattr(generate, "collect_market_data", Mock(return_value=market_data))
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
