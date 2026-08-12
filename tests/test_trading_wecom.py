from biz.analysis import trading_wecom


def test_sim_trade_notification_only_sends_new_fills(monkeypatch):
    sent = []
    monkeypatch.setattr(trading_wecom, "get_wecom_webhook", lambda *_args, **_kwargs: "https://example.invalid")
    monkeypatch.setattr(trading_wecom, "send_markdown", lambda _url, content: sent.append(content) or {"errcode": 0})

    result = trading_wecom.notify_sim_trade_result({
        "trade_date": "2026-08-12",
        "match_results": [
            {"status": "waiting", "side": "BUY", "stock_code": "000001"},
            {"status": "filled", "side": "BUY", "stock_code": "688059", "shares": 1000, "price": 95.53, "order_id": 16},
        ],
    })

    assert result["status"] == "sent"
    assert "688059" in sent[0]
    assert "真实交易保持关闭" in sent[0]


def test_sim_trade_notification_stays_silent_without_fill(monkeypatch):
    monkeypatch.setattr(trading_wecom, "send_markdown", lambda *_args: (_ for _ in ()).throw(AssertionError("must not send")))
    result = trading_wecom.notify_sim_trade_result({"match_results": []})
    assert result == {"status": "skipped", "reason": "no_new_fill"}
