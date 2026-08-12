import sys
from unittest.mock import Mock, patch

import pytest
from sqlalchemy.sql.elements import TextClause

from biz.evening_review import generate
from biz.review import quant_digest
from integrations.wecom.delivery import WeComDeliveryError


REVIEW_DATE = "2026-08-11"
THREE_SECTION_REVIEW = """北京时间2026年08月11日 15:30 盘后复盘

大势分析
市场正文。

行业轮动
行业正文。

因子特征
因子正文。"""


def _ready_digest(review: str = THREE_SECTION_REVIEW) -> dict:
    return {
        "publish_status": quant_digest.PUBLISH_READY,
        "compact_review": review,
        "quality_json": {"errors": []},
    }


def _forbid_legacy_path(monkeypatch) -> dict[str, Mock]:
    calls: dict[str, Mock] = {}
    for name in ("collect_market_data", "analyze_with_deepseek", "build_report"):
        mock = Mock(side_effect=AssertionError(f"default path called legacy {name}"))
        monkeypatch.setattr(generate, name, mock)
        calls[name] = mock
    return calls


def test_query_wraps_named_parameter_sql_for_sqlalchemy_2():
    engine = object()

    with patch.object(generate, "read_records", return_value=[]) as reader:
        result = generate._query(engine, "SELECT :d AS trade_date", {"d": "2026-08-11"})

    assert result == []
    statement, supplied_engine = reader.call_args.args
    assert isinstance(statement, TextClause)
    assert str(statement) == "SELECT :d AS trade_date"
    assert supplied_engine is engine
    assert reader.call_args.kwargs["params"] == {"d": "2026-08-11"}


def test_default_ready_digest_is_pushed_unchanged_without_radars(monkeypatch):
    engine = object()
    digest_call = Mock(return_value=_ready_digest())
    pushed: list[tuple[str, object]] = []
    _forbid_legacy_path(monkeypatch)
    monkeypatch.setattr(sys, "argv", ["evening-review", REVIEW_DATE])
    monkeypatch.setattr(generate, "get_engine", lambda: engine)
    monkeypatch.setattr(quant_digest, "generate_quant_digest", digest_call)
    monkeypatch.setattr(
        generate,
        "append_decision_radar",
        Mock(side_effect=AssertionError("quant digest must not append decision radar")),
    )
    monkeypatch.setattr(
        generate,
        "append_research_radar",
        Mock(side_effect=AssertionError("quant digest must not append research radar")),
    )
    monkeypatch.setattr(
        generate,
        "push_to_wecom",
        lambda content, engine=None: pushed.append((content, engine)) or True,
    )

    assert generate.main() == 0

    digest_call.assert_called_once_with(engine, REVIEW_DATE, persist=True)
    assert pushed == [(THREE_SECTION_REVIEW, engine)]


def test_blocked_digest_fails_without_ai_or_push(monkeypatch):
    engine = object()
    digest_call = Mock(
        return_value={
            "publish_status": quant_digest.PUBLISH_BLOCKED,
            "compact_review": "不得发布的正文",
            "quality_json": {"errors": ["行情覆盖率 97.0% 低于 98.0%"]},
        }
    )
    legacy_calls = _forbid_legacy_path(monkeypatch)
    push = Mock(side_effect=AssertionError("blocked digest must not be pushed"))
    monkeypatch.setattr(sys, "argv", ["evening-review", REVIEW_DATE])
    monkeypatch.setattr(generate, "get_engine", lambda: engine)
    monkeypatch.setattr(quant_digest, "generate_quant_digest", digest_call)
    monkeypatch.setattr(generate, "push_to_wecom", push)

    with pytest.raises(RuntimeError, match="量化复盘未通过发布门禁.*97.0%"):
        generate.main()

    digest_call.assert_called_once_with(engine, REVIEW_DATE, persist=True)
    push.assert_not_called()
    for call in legacy_calls.values():
        call.assert_not_called()


def test_legacy_flag_is_the_only_path_to_old_ai_report(monkeypatch):
    engine = object()
    market_data = {"指数": {}}
    collect = Mock(return_value=market_data)
    analyze = Mock(return_value="旧版 AI 分析")
    build = Mock(return_value="旧版正文")
    decision = Mock(side_effect=lambda content, _engine, _date: content + "\n决策雷达")
    research = Mock(side_effect=lambda content, _engine, _date: content + "\n研报雷达")
    printed: list[str] = []
    monkeypatch.setattr(
        quant_digest,
        "generate_quant_digest",
        Mock(side_effect=AssertionError("--legacy must not call quant digest")),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["evening-review", REVIEW_DATE, "--legacy", "--test"],
    )
    monkeypatch.setattr(generate, "get_engine", lambda: engine)
    monkeypatch.setattr(generate, "collect_market_data", collect)
    monkeypatch.setattr(generate, "analyze_with_deepseek", analyze)
    monkeypatch.setattr(generate, "build_report", build)
    monkeypatch.setattr(generate, "append_decision_radar", decision)
    monkeypatch.setattr(generate, "append_research_radar", research)
    monkeypatch.setattr(generate, "_safe_print", printed.append)

    assert generate.main() == 0

    collect.assert_called_once_with(engine, REVIEW_DATE)
    analyze.assert_called_once_with(market_data, REVIEW_DATE)
    build.assert_called_once_with(market_data, REVIEW_DATE, "旧版 AI 分析")
    assert printed == ["旧版正文\n决策雷达\n研报雷达"]


def test_delivery_error_propagates_out_of_evening_main(monkeypatch):
    engine = object()
    delivery_error = WeComDeliveryError(
        "WeCom delivery incomplete: 1/2 segments delivered",
        delivery_id="delivery-evening-test",
    )
    _forbid_legacy_path(monkeypatch)
    monkeypatch.setattr(sys, "argv", ["evening-review", REVIEW_DATE])
    monkeypatch.setattr(generate, "get_engine", lambda: engine)
    monkeypatch.setattr(quant_digest, "generate_quant_digest", Mock(return_value=_ready_digest()))
    monkeypatch.setattr(generate, "get_wecom_webhook", lambda *_args, **_kwargs: "https://example.invalid")
    monkeypatch.setattr(generate, "deliver_markdown", Mock(side_effect=delivery_error))

    with pytest.raises(WeComDeliveryError) as exc_info:
        generate.main()

    assert exc_info.value is delivery_error
