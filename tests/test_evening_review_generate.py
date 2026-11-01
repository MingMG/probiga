"""The evening CLI delivers the same verified report shown on the website."""
import sys
from copy import deepcopy
from unittest.mock import Mock, patch

import pytest
from sqlalchemy.sql.elements import TextClause

from biz.evening_review import generate
from biz.review import quant_digest
from biz.review.market_decision import rules_identity, source_digest_identity
from biz.review.research_flow import artifact_hash
from integrations.wecom.delivery import WeComDeliveryError
from test_market_decision import completed_research

REVIEW_DATE = "2026-10-30"
def _automatic_fixture():
    # Exercise the real producer/verifier; only the two provider boundaries are mocked.
    with pytest.MonkeyPatch.context() as monkeypatch:
        research, _, _, data, _ = completed_research(monkeypatch)
    research["research_sha256"] = artifact_hash(research)
    data["factor_validation_json"]["market_decision"] = research
    data["compact_review"] = research["content"]
    return data


_CANONICAL = _automatic_fixture()
CONTENT = _CANONICAL["compact_review"]


def _ready_digest(content=CONTENT):
    digest = deepcopy(_CANONICAL)
    digest["compact_review"] = content
    return digest


@pytest.fixture(autouse=True)
def never_generate_a_second_evening_report(monkeypatch):
    forbidden = Mock(side_effect=AssertionError("evening consumer must never regenerate or call AI"))
    monkeypatch.setattr(quant_digest, "generate_quant_digest", forbidden)
    yield
    forbidden.assert_not_called()


def _bind_target(monkeypatch, *args):
    engine = object()
    resolve = Mock(return_value=REVIEW_DATE)
    monkeypatch.setattr(sys, "argv", ["evening-review", REVIEW_DATE, *args])
    monkeypatch.setattr(generate, "get_engine", lambda: engine)
    monkeypatch.setattr(generate, "resolve_review_trade_date", resolve)
    return engine, resolve


def test_query_wraps_named_parameter_sql_for_sqlalchemy_2():
    engine = object()
    with patch.object(generate, "read_records", return_value=[]) as reader:
        assert generate._query(engine, "SELECT :d AS trade_date", {"d": REVIEW_DATE}) == []
    statement, supplied = reader.call_args.args
    assert isinstance(statement, TextClause)
    assert str(statement) == "SELECT :d AS trade_date"
    assert supplied is engine
    assert reader.call_args.kwargs["params"] == {"d": REVIEW_DATE}


def test_ready_digest_pushes_and_writes_the_same_artifact(monkeypatch, tmp_path):
    output = tmp_path / "review.md"
    engine, resolve = _bind_target(monkeypatch, "--output", str(output))
    research = Mock(return_value=_ready_digest())
    push = Mock(return_value=True)
    monkeypatch.setattr(generate, "load_digest", research)
    monkeypatch.setattr(generate, "push_to_wecom", push)
    assert generate.main() == 0
    resolve.assert_called_once_with(engine, REVIEW_DATE)
    research.assert_called_once_with(engine, REVIEW_DATE)
    push.assert_called_once_with(CONTENT, engine=engine)
    assert output.read_text(encoding="utf-8") == CONTENT


def test_test_mode_previews_canonical_report_without_generation_or_delivery(monkeypatch, tmp_path):
    output = tmp_path / "review-preview.md"
    engine, _ = _bind_target(monkeypatch, "--test", "--output", str(output))
    research = Mock(return_value=_ready_digest())
    printed = Mock()
    push = Mock(side_effect=AssertionError("preview must not push"))
    monkeypatch.setattr(generate, "load_digest", research)
    monkeypatch.setattr(generate, "_safe_print", printed)
    monkeypatch.setattr(generate, "push_to_wecom", push)
    assert generate.main() == 0
    research.assert_called_once_with(engine, REVIEW_DATE)
    printed.assert_called_once_with(CONTENT)
    push.assert_not_called()
    assert output.read_text(encoding="utf-8") == CONTENT


@pytest.mark.parametrize("failure", ["blocked", "stale", "empty"])
def test_invalid_digest_is_never_delivered(monkeypatch, failure):
    _bind_target(monkeypatch)
    digest = _ready_digest()
    if failure == "blocked":
        digest["publish_status"] = quant_digest.PUBLISH_BLOCKED
        digest["quality_json"]["errors"] = ["行情覆盖率 97.0% 低于门禁"]
    elif failure == "stale":
        digest["quality_json"]["source_dates"]["target_bars"] = "2026-10-29"
    else:
        digest["compact_review"] = ""
    monkeypatch.setattr(generate, "load_digest", Mock(return_value=digest))
    push = Mock(side_effect=AssertionError("invalid report must not be delivered"))
    monkeypatch.setattr(generate, "push_to_wecom", push)
    with pytest.raises(RuntimeError, match="DATA_BLOCKED"):
        generate.main()
    push.assert_not_called()


def test_delivery_error_propagates_out_of_evening_main(monkeypatch):
    _bind_target(monkeypatch)
    error = WeComDeliveryError("WeCom delivery incomplete: 1/2 segments delivered", delivery_id="delivery-evening-test")
    monkeypatch.setattr(generate, "load_digest", Mock(return_value=_ready_digest()))
    monkeypatch.setattr(generate, "get_wecom_webhook", lambda *_args, **_kwargs: "https://example.invalid")
    deliver = Mock(side_effect=error)
    monkeypatch.setattr(generate, "deliver_markdown", deliver)
    with pytest.raises(WeComDeliveryError) as caught:
        generate.main()
    assert caught.value is error
    assert deliver.call_args.args[1] == CONTENT
    assert deliver.call_args.kwargs["delivery_kind"] == "evening_review"


def test_removed_legacy_option_is_rejected_before_any_work(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["evening-review", REVIEW_DATE, "--legacy"])
    engine = Mock(side_effect=AssertionError("invalid CLI must not connect"))
    monkeypatch.setattr(generate, "get_engine", engine)
    with pytest.raises(SystemExit) as caught:
        generate.main()
    assert caught.value.code == 2
    engine.assert_not_called()


def test_default_review_date_uses_authoritative_calendar_without_market_max_query(monkeypatch):
    engine = object()
    authority = Mock(return_value=REVIEW_DATE)
    query = Mock(side_effect=AssertionError("default date must not query market MAX"))
    monkeypatch.setattr(generate, "authoritative_closed_trade_date", authority)
    monkeypatch.setattr(generate, "_query", query)
    assert generate.resolve_review_trade_date(engine) == REVIEW_DATE
    authority.assert_called_once_with(engine, now=None)
    query.assert_not_called()


def test_unavailable_authoritative_date_fails_closed(monkeypatch):
    monkeypatch.setattr(generate, "authoritative_closed_trade_date", Mock(return_value=""))
    with pytest.raises(RuntimeError, match="DATA_BLOCKED.*unavailable"):
        generate.resolve_review_trade_date(object())


@pytest.mark.parametrize("requested", ["2026-10-31", "2026-11-02"])
def test_unclosed_session_is_rejected_before_calendar_query(monkeypatch, requested):
    monkeypatch.setattr(generate, "authoritative_closed_trade_date", Mock(return_value=REVIEW_DATE))
    query = Mock(side_effect=AssertionError("future session must be rejected first"))
    monkeypatch.setattr(generate, "_query", query)
    with pytest.raises(RuntimeError, match="DATA_BLOCKED.*not a closed session"):
        generate.resolve_review_trade_date(object(), requested)
    query.assert_not_called()


def test_explicit_historical_date_must_be_an_exchange_session(monkeypatch):
    engine = object()
    monkeypatch.setattr(generate, "authoritative_closed_trade_date", Mock(return_value=REVIEW_DATE))
    query = Mock(return_value=[{"n": 0}])
    monkeypatch.setattr(generate, "_query", query)
    with pytest.raises(RuntimeError, match="DATA_BLOCKED.*not an open session"):
        generate.resolve_review_trade_date(engine, "2026-10-25")
    assert "si_trade_calendar" in query.call_args.args[1]
    assert query.call_args.args[2] == {"d": "2026-10-25"}


@pytest.mark.parametrize("mutation", ["missing", "rules", "source", "hash", "content"])
def test_published_report_rejects_outdated_or_inconsistent_research(mutation):
    digest = _ready_digest()
    research = digest["factor_validation_json"]["market_decision"]
    if mutation == "missing":
        digest["factor_validation_json"].clear()
    elif mutation == "rules":
        research["rules_sha256"] = "previous-rules"
    elif mutation == "source":
        research["source_digest_sha256"] = "other-facts"
    elif mutation == "hash":
        research["research_sha256"] = "altered"
    else:
        digest["compact_review"] = "another report"
    with pytest.raises(RuntimeError, match="review_publication"):
        generate.published_report(digest, REVIEW_DATE)


@pytest.mark.parametrize("mutation", ["manual_source", "edited_body"])
def test_recomputed_publication_hash_does_not_authorize_manual_research(mutation):
    from biz.review.market_decision import ResearchError
    digest = _ready_digest()
    research = digest["factor_validation_json"]["market_decision"]
    if mutation == "manual_source":
        research["generation_attempts"][0]["provider_receipt"]["source"] = "manual"
    else:
        research["draft"]["units"][0]["statement"].append({"text": "人工追加观点。"})
        research["content"] = generate.published_report(_ready_digest(), REVIEW_DATE) + "人工追加观点。"
        digest["compact_review"] = research["content"]
    research["research_sha256"] = artifact_hash({key: value for key, value in research.items() if key != "research_sha256"})
    with pytest.raises(ResearchError, match="automatic_provenance"):
        generate.published_report(digest, REVIEW_DATE)


def test_missing_canonical_report_blocks_before_delivery(monkeypatch):
    _bind_target(monkeypatch)
    read = Mock(side_effect=RuntimeError("DATA_BLOCKED stage=previous_review: exact review is missing"))
    push = Mock()
    monkeypatch.setattr(generate, "load_digest", read)
    monkeypatch.setattr(generate, "push_to_wecom", push)
    with pytest.raises(RuntimeError, match="exact review is missing"):
        generate.main()
    push.assert_not_called()
