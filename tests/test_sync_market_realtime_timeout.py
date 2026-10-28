from __future__ import annotations

from unittest.mock import Mock

from biz.stock_market import realtime_quotes


def test_fetch_list_market_current_uses_bounded_caller_timeout(monkeypatch):
    response = Mock()
    response.status_code = 200
    response.text = (
        'var hq_str_s_sz000001="平安银行,10,10,10,10,'
        '1,1,10,10,10,1,1,1,1,1,1,1,1,1,1,1,1";'
    )
    get = Mock(return_value=response)
    monkeypatch.setattr(realtime_quotes.requests, "get", get)

    realtime_quotes.fetch_list_market_current(
        ["000001"],
        timeout_seconds=2.5,
    )

    assert get.call_args.kwargs["timeout"] == 2.5
