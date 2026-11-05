"""The briefing CLI publishes website reports and requires opt-in for delivery."""
import sys
from unittest.mock import Mock

import pytest

from biz.early_briefing import generate
from biz.review import daily_reports
from integrations.wecom.delivery import WeComDeliveryError

TARGET_DATE = "2026-10-30"
CONTENT = "# A股早报参考｜2026-11-02\n\n[INFERRED] 等待广度与强势股反馈确认。"


def _bind_target(monkeypatch, *args):
    engine = object()
    authority = Mock(return_value=TARGET_DATE)
    monkeypatch.setattr(sys, "argv", ["early-briefing", *args])
    monkeypatch.setattr(generate, "get_engine", lambda: engine)
    monkeypatch.setattr(generate, "authoritative_closed_trade_date", authority)
    return engine, authority


def test_ready_briefing_pushes_and_writes_the_same_artifact(monkeypatch, tmp_path):
    output = tmp_path / "briefing.md"
    engine, authority = _bind_target(monkeypatch, "--push", "--output", str(output))
    research = Mock(return_value={"content": CONTENT})
    push = Mock(return_value=True)
    monkeypatch.setattr(generate, "generate_briefing", research)
    monkeypatch.setattr(generate, "push_to_wecom", push)
    assert generate.main() == 0
    authority.assert_called_once_with(engine)
    research.assert_called_once_with(engine, TARGET_DATE, persist=True)
    push.assert_called_once_with(CONTENT, engine=engine)
    assert output.read_text(encoding="utf-8") == CONTENT


def test_test_mode_runs_research_without_persistence_or_delivery(monkeypatch, tmp_path):
    output = tmp_path / "briefing-preview.md"
    engine, _ = _bind_target(monkeypatch, "--test", "--output", str(output))
    research = Mock(return_value={"content": CONTENT})
    printed = Mock()
    push = Mock(side_effect=AssertionError("preview must not push"))
    monkeypatch.setattr(generate, "generate_briefing", research)
    monkeypatch.setattr(generate, "_safe_print", printed)
    monkeypatch.setattr(generate, "push_to_wecom", push)
    assert generate.main() == 0
    research.assert_called_once_with(engine, TARGET_DATE, persist=False)
    printed.assert_called_once_with(CONTENT)
    push.assert_not_called()
    assert output.read_text(encoding="utf-8") == CONTENT


@pytest.mark.parametrize("stale", [False, True], ids=["blocked", "stale"])
def test_invalid_frozen_digest_fails_before_research_or_delivery(monkeypatch, stale):
    _bind_target(monkeypatch)
    date = "2026-10-29" if stale else TARGET_DATE
    digest = {
        "review_date": date, "publish_status": "ready" if stale else "blocked",
        "quality_json": {"target_date": date, "source_dates": {"target_bars": date},
                         "errors": [] if stale else ["行情覆盖率不足"]},
    }
    monkeypatch.setattr(daily_reports, "load_digest", Mock(return_value=digest))
    model = Mock(side_effect=AssertionError("invalid facts must not reach the model"))
    monkeypatch.setattr(daily_reports, "_call_codex", model)
    push = Mock(side_effect=AssertionError("invalid facts must not be delivered"))
    monkeypatch.setattr(generate, "push_to_wecom", push)
    with pytest.raises(RuntimeError, match="DATA_BLOCKED stage=research_input"):
        generate.main()
    model.assert_not_called()
    push.assert_not_called()


def test_unavailable_authoritative_session_fails_before_research(monkeypatch):
    _bind_target(monkeypatch)
    monkeypatch.setattr(generate, "authoritative_closed_trade_date", Mock(return_value=""))
    research = Mock(side_effect=AssertionError("calendar must resolve first"))
    monkeypatch.setattr(generate, "generate_briefing", research)
    with pytest.raises(RuntimeError, match="DATA_BLOCKED stage=briefing_calendar"):
        generate.main()
    research.assert_not_called()


def test_delivery_error_propagates_out_of_early_briefing_main(monkeypatch):
    engine, _ = _bind_target(monkeypatch, "--push")
    research = Mock(return_value={"content": CONTENT})
    error = WeComDeliveryError("briefing webhook is not configured", delivery_id="delivery-early-test")
    monkeypatch.setattr(generate, "generate_briefing", research)
    monkeypatch.setattr(generate, "get_wecom_webhook", lambda *_args, **_kwargs: None)
    deliver = Mock(side_effect=error)
    monkeypatch.setattr(generate, "deliver_markdown", deliver)
    with pytest.raises(WeComDeliveryError) as caught:
        generate.main()
    assert caught.value is error
    research.assert_called_once_with(engine, TARGET_DATE, persist=True)
    assert deliver.call_args.args[1] == CONTENT
    assert deliver.call_args.kwargs["delivery_kind"] == "early_briefing"


def test_removed_legacy_option_is_rejected_before_any_work(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["early-briefing", "--legacy"])
    engine = Mock(side_effect=AssertionError("invalid CLI must not connect"))
    monkeypatch.setattr(generate, "get_engine", engine)
    with pytest.raises(SystemExit) as caught:
        generate.main()
    assert caught.value.code == 2
    engine.assert_not_called()
