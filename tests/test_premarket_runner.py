from __future__ import annotations

import pytest

from tools import run_ai_recommendation_premarket as runner


def test_wecom_delivery_without_success_receipt_fails_closed(monkeypatch) -> None:
    updates: list[dict] = []
    monkeypatch.setattr(runner, "get_wecom_webhook", lambda *_args, **_kwargs: "https://example.invalid")
    monkeypatch.setattr(
        runner,
        "_send_theme_forecast_markdown",
        lambda *_args, **_kwargs: {
            "success": False,
            "delivery_id": "delivery-1",
            "segments": 1,
        },
    )
    monkeypatch.setattr(
        runner,
        "mark_forecast_delivery",
        lambda _engine, _run_uid, **payload: updates.append(payload),
    )

    with pytest.raises(RuntimeError, match="未获得企业微信成功回执"):
        runner._deliver_theme_forecast(object(), {"run_uid": "run-1"})

    assert updates[-1]["status"] == "FAILED"


def test_already_delivered_forecast_is_idempotent(monkeypatch) -> None:
    monkeypatch.setattr(
        runner,
        "_send_theme_forecast_markdown",
        lambda *_args, **_kwargs: pytest.fail("delivery must not be repeated"),
    )
    result = runner._deliver_theme_forecast(
        object(),
        {"run_uid": "run-1", "delivery_status": "SUCCESS", "delivery_id": "delivery-1"},
    )
    assert result == {"success": True, "delivery_id": "delivery-1", "skipped": True}


def test_theme_markdown_split_preserves_content_and_byte_limit() -> None:
    content = "标题\n" + ("中文证据\n" * 1200)
    parts = runner._split_theme_markdown(content, max_bytes=240)

    assert len(parts) > 1
    assert "".join(parts) == content
    assert all(len(part.encode("utf-8")) <= 240 for part in parts)
