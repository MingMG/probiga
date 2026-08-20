from __future__ import annotations

from biz.analysis import trading_wecom


def _row(
    code: str,
    *,
    rank: int,
    grade: str = "A",
    eligible: bool = True,
    recommend_status: str = "ALLOW",
    reject_reasons: list[str] | None = None,
) -> dict:
    return {
        "rank": rank,
        "stock_code": code,
        "stock_name": f"股票{code}",
        "score": 80 + rank,
        "ensemble_score": 90 - rank,
        "candidate_grade": grade,
        "portfolio_eligible": eligible,
        "decision_readiness": {"recommend_status": recommend_status},
        "risk_gate": {"reject_reasons": reject_reasons or []},
        "change_pct": 1.25,
    }


def _result(*rows: dict, freshness: str = "exact") -> dict:
    return {
        "preset": {"key": "capital_support", "name": "盘前资金支持"},
        "requested_date": "2026-08-20",
        "data_date": "2026-08-20",
        "freshness": freshness,
        "data": list(rows),
        "run": {"persisted": True, "run_uid": "run-1"},
    }


def test_delivery_selector_excludes_rejected_suspended_and_ineligible_rows():
    result = _result(
        _row("000004", rank=4, reject_reasons=["hard_gate"]),
        _row("000003", rank=3, recommend_status="SUSPENDED"),
        _row("000002", rank=2, eligible=False),
        _row("000001", rank=1, grade="B"),
    )

    selected = trading_wecom.select_screener_delivery_rows(result)

    assert [row["stock_code"] for row in selected] == ["000001"]


def test_delivery_selector_blocks_fallback_or_mismatched_dates():
    row = _row("000001", rank=1)
    assert trading_wecom.select_screener_delivery_rows(
        _result(row, freshness="fallback")
    ) == []
    mismatched = _result(row)
    mismatched["data_date"] = "2026-08-14"
    assert trading_wecom.select_screener_delivery_rows(mismatched) == []


def test_wecom_sends_no_candidate_decision_instead_of_rejected_top_five(
    monkeypatch,
):
    captured: list[str] = []
    monkeypatch.setattr(
        trading_wecom,
        "get_wecom_webhook",
        lambda *_args, **_kwargs: "https://example.invalid/webhook",
    )
    monkeypatch.setattr(
        trading_wecom,
        "send_markdown",
        lambda _url, content: captured.append(content) or {"errcode": 0},
    )
    result = _result(
        _row("000001", rank=1, grade="REJECT", recommend_status="SUSPENDED"),
    )

    notification = trading_wecom.notify_screener_result(result)

    assert notification["status"] == "sent"
    assert notification["delivery_status"] == "no_qualified_candidate"
    assert notification["result_count"] == 0
    assert notification["delivered_codes"] == []
    assert "盘前生产决策" in captured[0]
    assert "今日没有同时通过" in captured[0]
    assert "股票000001" not in captured[0]


def test_wecom_sends_explicit_no_candidate_decision_for_empty_persisted_run(
    monkeypatch,
):
    captured: list[str] = []
    monkeypatch.setattr(
        trading_wecom,
        "get_wecom_webhook",
        lambda *_args, **_kwargs: "https://example.invalid/webhook",
    )
    monkeypatch.setattr(
        trading_wecom,
        "send_markdown",
        lambda _url, content: captured.append(content) or {"errcode": 0},
    )

    notification = trading_wecom.notify_screener_result(_result())

    assert notification["status"] == "sent"
    assert notification["delivery_status"] == "no_qualified_candidate"
    assert notification["screened_count"] == 0
    assert "今日没有同时通过" in captured[0]


def test_wecom_marks_fallback_as_audit_only_and_hides_stock(monkeypatch):
    captured: list[str] = []
    monkeypatch.setattr(
        trading_wecom,
        "get_wecom_webhook",
        lambda *_args, **_kwargs: "https://example.invalid/webhook",
    )
    monkeypatch.setattr(
        trading_wecom,
        "send_markdown",
        lambda _url, content: captured.append(content) or {"errcode": 0},
    )
    result = _result(_row("000001", rank=1), freshness="fallback")

    notification = trading_wecom.notify_screener_result(result)

    assert notification["result_count"] == 0
    assert "只保留审计记录，不发送个股推荐" in captured[0]
    assert "股票000001" not in captured[0]


def test_wecom_delivers_only_qualified_rows_in_rank_order(monkeypatch):
    captured: list[str] = []
    monkeypatch.setattr(
        trading_wecom,
        "get_wecom_webhook",
        lambda *_args, **_kwargs: "https://example.invalid/webhook",
    )
    monkeypatch.setattr(
        trading_wecom,
        "send_markdown",
        lambda _url, content: captured.append(content) or {"errcode": 0},
    )
    result = _result(
        _row("000002", rank=2),
        _row("000009", rank=9, grade="REJECT"),
        _row("000001", rank=1, grade="B"),
    )

    notification = trading_wecom.notify_screener_result(result)

    assert notification["delivered_codes"] == ["000001", "000002"]
    assert captured[0].index("000001") < captured[0].index("000002")
    assert "000009" not in captured[0]


def test_notify_screener_failure_uses_briefing_channel(monkeypatch):
    sent: list[str] = []
    monkeypatch.setattr(
        trading_wecom,
        "get_wecom_webhook",
        lambda kind, **_kwargs: (
            "https://example.invalid" if kind == "briefing" else ""
        ),
    )
    monkeypatch.setattr(
        trading_wecom,
        "send_markdown",
        lambda _url, content: sent.append(content)
        or {"errcode": 0, "errmsg": "ok"},
    )

    result = trading_wecom.notify_screener_failure(
        preset="capital_support",
        reason="database unavailable",
        stage="生成或落库",
    )

    assert result["status"] == "sent"
    assert "09:08" in sent[0]
    assert "database unavailable" in sent[0]


def test_sim_trade_notification_only_sends_new_fills(monkeypatch):
    sent: list[str] = []
    monkeypatch.setattr(
        trading_wecom,
        "get_wecom_webhook",
        lambda *_args, **_kwargs: "https://example.invalid",
    )
    monkeypatch.setattr(
        trading_wecom,
        "send_markdown",
        lambda _url, content: sent.append(content) or {"errcode": 0},
    )

    result = trading_wecom.notify_sim_trade_result(
        {
            "trade_date": "2026-08-12",
            "match_results": [
                {"status": "waiting", "side": "BUY", "stock_code": "000001"},
                {
                    "status": "filled",
                    "side": "BUY",
                    "stock_code": "688059",
                    "shares": 1000,
                    "price": 95.53,
                    "order_id": 16,
                },
            ],
        }
    )

    assert result["status"] == "sent"
    assert "688059" in sent[0]
    assert "真实交易保持关闭" in sent[0]


def test_sim_trade_notification_stays_silent_without_fill(monkeypatch):
    monkeypatch.setattr(
        trading_wecom,
        "send_markdown",
        lambda *_args: (_ for _ in ()).throw(AssertionError("must not send")),
    )
    result = trading_wecom.notify_sim_trade_result({"match_results": []})
    assert result == {"status": "skipped", "reason": "no_new_fill"}
