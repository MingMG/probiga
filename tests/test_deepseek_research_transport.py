"""The provider boundary retains incomplete answers without publishing them."""
from copy import deepcopy
import json
from types import SimpleNamespace

import httpx
import pytest

from biz.review import market_decision as core
from test_market_decision import Providers
from test_research_contract import NOW, material


def test_incomplete_http_response_keeps_answer_and_finish_metadata_without_reasoning(monkeypatch):
    partial = '{"unfinished":'
    calls = []

    class Client:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def post(self, endpoint, **kwargs):
            calls.append((endpoint, kwargs["json"]))
            return httpx.Response(200, json={"id": "incomplete-unit", "model": "deepseek-v4-pro",
                "choices": [{"finish_reason": "length", "message": {"content": partial,
                    "reasoning_content": "private-reasoning-sentinel"}}],
                "usage": {"completion_tokens": 16000, "completion_tokens_details": {"reasoning_tokens": 15990}}})

    monkeypatch.setattr(core, "get_settings", lambda: SimpleNamespace(deepseek_api_key="unit-key"))
    monkeypatch.setattr(core.httpx, "AsyncClient", Client)
    monkeypatch.setattr(core.httpx, "AsyncHTTPTransport", lambda **kwargs: object())
    messages = [{"role": "system", "content": "unit"}, {"role": "user", "content":
        json.dumps({"material": {"input": {"task": {"kind": "review"}}}})}]
    with pytest.raises(core.ResearchError, match="response_incomplete") as caught:
        core._call_deepseek(messages, timeout_seconds=2, stage="deepseek_draft")
    error = caught.value
    assert error.partial_raw_response == partial
    assert error.provider_failure_receipt["finish_reason"] == "length"
    assert error.provider_failure_receipt["provider_response_id"] == "incomplete-unit"
    assert error.provider_failure_receipt["usage"]["completion_tokens"] == 16000
    assert error.provider_failure_receipt["request_policy"] == core._deepseek_policy("review")
    assert "private-reasoning-sentinel" not in core._json(error.provider_failure_receipt)
    assert calls[0][0] == "https://api.deepseek.com/v1/chat/completions"
    assert calls[0][1]["model"] == "deepseek-v4-pro"
    assert calls[0][1]["thinking"] == {"type": "disabled"}


def test_partial_provider_answer_remains_failed_in_durable_checkpoint(monkeypatch):
    providers = Providers().install(monkeypatch)
    expected = {"source": "deepseek_api", "finish_reason": "length", "usage": {"completion_tokens": 16000}}

    def fail(messages, *, timeout_seconds, stage):
        error = core.ResearchError("RESEARCH_BLOCKED stage=" + stage + " reason=response_incomplete")
        error.partial_raw_response = '{"unfinished":'
        error.provider_failure_receipt = deepcopy(expected)
        raise error

    monkeypatch.setattr(core, "_call_deepseek", fail)
    data, context = material()
    with pytest.raises(core.ResearchError) as caught:
        core.generate_research(data, report_kind="review", next_trade_date="2026-11-02", context=context,
                               now=NOW, engine=object())
    stage = caught.value.checkpoint["stages"]["deepseek_draft"]
    assert stage["status"] == "FAILED"
    assert stage["provider_failure_receipt"] == expected
    assert stage["partial_raw_sha256"] == core._body_hash(stage["partial_raw_response"])
    assert "raw_response" not in stage and "deepseek_final_review" not in providers.calls
