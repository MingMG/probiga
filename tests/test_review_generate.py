"""The daily review is the only scheduled producer of market research."""
import sys
from unittest.mock import Mock

import pytest

from biz.review import generate
from biz.review.market_decision import rules_identity, source_digest_identity
from biz.review.research_flow import artifact_hash

DATE = "2026-10-30"
CONTENT = "# A股盘后复盘｜2026-10-30\n\n[INFERRED] 等待确认。"


def ready_digest():
    digest = {
        "review_date": DATE, "publish_status": "ready", "compact_review": CONTENT,
        "quality_json": {"target_date": DATE, "source_dates": {"target_bars": DATE}, "errors": []},
        "factor_validation_json": {},
    }
    research = {
        "status": "COMPLETE", "report_kind": "review", "review_date": DATE,
        "rules_sha256": rules_identity(), "source_digest_sha256": source_digest_identity(digest),
        "content": CONTENT,
    }
    research["research_sha256"] = artifact_hash(research)
    digest["factor_validation_json"]["market_decision"] = research
    return digest


def bind(monkeypatch, *args):
    engine = object()
    monkeypatch.setattr(sys, "argv", ["daily-review", *args])
    monkeypatch.setattr(generate, "get_engine", lambda: engine)
    resolve = Mock(return_value=DATE)
    monkeypatch.setattr(generate, "resolve_review_trade_date", resolve)
    return engine, resolve


def test_producer_generates_and_persists_once_when_exact_report_is_missing(monkeypatch, capsys):
    engine, resolve = bind(monkeypatch, DATE)
    read = Mock(return_value=None)
    produce = Mock(return_value=ready_digest())
    monkeypatch.setattr(generate, "load_digest", read)
    monkeypatch.setattr(generate, "generate_quant_digest", produce)
    assert generate.main() == 0
    resolve.assert_called_once_with(engine, DATE)
    read.assert_called_once_with(engine, DATE, required=False)
    produce.assert_called_once_with(engine, DATE, persist=True)
    assert capsys.readouterr().out.strip() == CONTENT


def test_same_day_retry_reuses_complete_current_artifact(monkeypatch, capsys):
    engine, _ = bind(monkeypatch, DATE)
    produce = Mock(side_effect=AssertionError("complete same-day research must not be reproduced"))
    monkeypatch.setattr(generate, "load_digest", Mock(return_value=ready_digest()))
    monkeypatch.setattr(generate, "generate_quant_digest", produce)
    assert generate.main() == 0
    produce.assert_not_called()
    assert capsys.readouterr().out.strip() == CONTENT


@pytest.mark.parametrize("problem", ["missing_research", "outdated_rules", "blocked"])
def test_only_producer_rebuilds_unusable_existing_artifact(monkeypatch, problem):
    engine, _ = bind(monkeypatch, DATE)
    previous = ready_digest()
    if problem == "missing_research":
        previous["factor_validation_json"].clear()
    elif problem == "outdated_rules":
        previous["factor_validation_json"]["market_decision"]["rules_sha256"] = "old-rules"
    else:
        previous["publish_status"] = "blocked"
    produce = Mock(return_value=ready_digest())
    monkeypatch.setattr(generate, "load_digest", Mock(return_value=previous))
    monkeypatch.setattr(generate, "generate_quant_digest", produce)
    assert generate.main() == 0
    produce.assert_called_once_with(engine, DATE, persist=True)


def test_force_recomputes_from_sources_without_consuming_previous_report(monkeypatch):
    engine, _ = bind(monkeypatch, DATE, "--force")
    read = Mock(side_effect=AssertionError("force requests a source recomputation"))
    produce = Mock(return_value=ready_digest())
    monkeypatch.setattr(generate, "load_digest", read)
    monkeypatch.setattr(generate, "generate_quant_digest", produce)
    assert generate.main() == 0
    read.assert_not_called()
    produce.assert_called_once_with(engine, DATE, persist=True)


def test_today_uses_authoritative_closed_session(monkeypatch):
    engine, resolve = bind(monkeypatch, "--today")
    monkeypatch.setattr(generate, "load_digest", Mock(return_value=ready_digest()))
    assert generate.main() == 0
    resolve.assert_called_once_with(engine, "")


def test_blocked_source_generation_has_nonzero_terminal_status(monkeypatch, capsys):
    bind(monkeypatch, DATE)
    monkeypatch.setattr(generate, "load_digest", Mock(return_value=None))
    blocked = {"publish_status": "blocked", "quality_json": {"errors": ["行情覆盖率不足"]}}
    monkeypatch.setattr(generate, "generate_quant_digest", Mock(return_value=blocked))
    assert generate.main() == 2
    assert "行情覆盖率不足" in capsys.readouterr().out


@pytest.mark.parametrize("arguments", [[DATE, "--legacy"], [DATE, "--unknown"], [], [DATE, "--today"]])
def test_invalid_cli_is_rejected_before_any_database_access(monkeypatch, arguments):
    monkeypatch.setattr(sys, "argv", ["daily-review", *arguments])
    connect = Mock(side_effect=AssertionError("invalid CLI must not connect"))
    monkeypatch.setattr(generate, "get_engine", connect)
    with pytest.raises(SystemExit) as caught:
        generate.main()
    assert caught.value.code == 2
    connect.assert_not_called()
