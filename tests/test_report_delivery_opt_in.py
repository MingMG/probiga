"""Report publication never implies authorization to send a group message."""
from unittest.mock import Mock
import sys
import pytest

@pytest.mark.parametrize("arguments,should_push", [([], False), (["--test"], False), (["--push"], True)])
def test_morning_publishes_by_default_and_delivery_requires_explicit_opt_in(monkeypatch, arguments, should_push):
    from biz.early_briefing import generate as entry
    engine = object()
    generate = Mock(return_value={"content": "canonical automatic briefing"})
    push = Mock()
    monkeypatch.setattr(sys, "argv", ["generate.py", *arguments])
    monkeypatch.setattr(entry, "get_engine", lambda: engine)
    monkeypatch.setattr(entry, "authoritative_closed_trade_date", lambda e: "2026-10-30")
    monkeypatch.setattr(entry, "generate_briefing", generate)
    monkeypatch.setattr(entry, "push_to_wecom", push)
    assert entry.main() == 0
    generate.assert_called_once_with(engine, "2026-10-30", persist="--test" not in arguments)
    assert push.called is should_push
    if should_push:
        push.assert_called_once_with("canonical automatic briefing", engine=engine)

@pytest.mark.parametrize("arguments,should_push", [([], False), (["--test"], False), (["--push"], True)])
def test_evening_uses_the_published_body_without_implicit_delivery(monkeypatch, arguments, should_push):
    from biz.evening_review import generate as entry
    engine, digest = object(), {"source": "published"}
    load = Mock(return_value=digest)
    validate = Mock(return_value="canonical automatic review")
    push = Mock()
    monkeypatch.setattr(sys, "argv", ["generate.py", *arguments])
    monkeypatch.setattr(entry, "get_engine", lambda: engine)
    monkeypatch.setattr(entry, "resolve_review_trade_date", lambda *a: "2026-10-30")
    monkeypatch.setattr(entry, "load_digest", load)
    monkeypatch.setattr(entry, "published_report", validate)
    monkeypatch.setattr(entry, "push_to_wecom", push)
    assert entry.main() == 0
    load.assert_called_once_with(engine, "2026-10-30")
    validate.assert_called_once_with(digest, "2026-10-30")
    assert push.called is should_push
    if should_push:
        push.assert_called_once_with("canonical automatic review", engine=engine)

@pytest.mark.parametrize("kind", ["early_briefing", "evening_review"])
def test_preview_cannot_be_combined_with_delivery(monkeypatch, kind):
    from importlib import import_module
    entry = import_module("biz." + kind + ".generate")
    monkeypatch.setattr(sys, "argv", ["generate.py", "--test", "--push"])
    with pytest.raises(SystemExit) as error:
        entry.main()
    assert error.value.code == 2
