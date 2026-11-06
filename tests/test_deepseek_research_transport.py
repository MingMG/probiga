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
                'choices':[{'finish_reason':'length','message':{'content':None,'reasoning_content':'private-reasoning-sentinel',
                    'tool_calls':[{'type':'function','function':{'name':'emit_research','arguments':partial}}]}}],
                "usage": {"completion_tokens": 16000, "completion_tokens_details": {"reasoning_tokens": 15990}}})

    monkeypatch.setattr(core, "get_settings", lambda: SimpleNamespace(deepseek_api_key="unit-key"))
    monkeypatch.setattr(core.httpx, "AsyncClient", Client)
    monkeypatch.setattr(core.httpx, "AsyncHTTPTransport", lambda **kwargs: object())
    messages = [{"role": "system", "content": "unit"}, {"role": "user", "content":
        json.dumps({"material": {"input": {"task": {"kind": "review"}}},'response_schema':{'type':'object'}})}]
    with pytest.raises(core.ResearchError, match="response_incomplete") as caught:
        core._call_deepseek(messages, timeout_seconds=2, stage="deepseek_draft")
    error = caught.value
    assert error.partial_raw_response == partial
    assert error.provider_failure_receipt["response_status"] == 'length'
    assert error.provider_failure_receipt["provider_response_id"] == "incomplete-unit"
    assert error.provider_failure_receipt["usage"]["completion_tokens"] == 16000
    assert error.provider_failure_receipt["request_policy"] == core._deepseek_policy("review")
    assert "private-reasoning-sentinel" not in core._json(error.provider_failure_receipt)
    assert calls[0][0] == "https://api.deepseek.com/beta/chat/completions"
    assert calls[0][1]["model"] == "deepseek-v4-pro"
    assert calls[0][1]["thinking"] == {"type": "disabled"}
    assert calls[0][1]['tools'][0]['function']['strict'] is True
    assert calls[0][1]['tool_choice']['function']['name']=='emit_research'
    assert calls[0][1]['parallel_tool_calls'] is False


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


@pytest.mark.parametrize('invalid', [False, True])
def test_strict_contract_validates_whole_arguments_and_preserves_duplicate_failure(monkeypatch, invalid):
    from pathlib import Path
    duplicate = (Path(__file__).parent / 'fixtures/deepseek_duplicate_review_20261102.txt').read_text(encoding='utf-8')
    raw = duplicate if invalid else '{"answer":"条件机会"}'
    schema = {'type':'object','properties':{'answer':{'type':'string'}},'required':['answer'],'additionalProperties':False}
    calls = []
    class Client:
        def __init__(self, **kwargs): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def post(self, endpoint, **kwargs):
            calls.append(kwargs['json'])
            return httpx.Response(200, json={'id':'response-unit','model':'deepseek-flash',
                'choices':[{'finish_reason':'tool_calls','message':{'content':None,
                    'tool_calls':[{'type':'function','function':{'name':'emit_research','arguments':'{"report":'+raw+'}'}}]}}]})
    monkeypatch.setattr(core, 'get_settings', lambda: SimpleNamespace(deepseek_api_key='unit-key'))
    monkeypatch.setattr(core.httpx, 'AsyncClient', Client)
    monkeypatch.setattr(core.httpx, 'AsyncHTTPTransport', lambda **kwargs: object())
    messages=[{'role':'system','content':'JSON'}, {'role':'user','content':json.dumps({'material':{'input':{'task':{'kind':'intraday'}}},'response_schema':schema})}]
    if invalid:
        with pytest.raises(core.ResearchError, match='response_envelope_mismatch') as caught:
            core._call_deepseek(messages,timeout_seconds=2,stage='deepseek_final_review')
        assert caught.value.partial_raw_response == '{"report":'+duplicate+'}'
        assert caught.value.provider_failure_receipt['response_status'] == 'tool_calls'
    else:
        reply=core._call_deepseek(messages,timeout_seconds=2,stage='deepseek_final_review')
        assert reply.raw_response == raw
        assert reply.provider_receipt['response_schema_sha256'] == core._hash(schema)
    assert len(calls)==1 and calls[0]['tools'][0]['function']['parameters']['properties']['report']==core._strict_wire_schema(schema)


def test_real_bad_review_reference_is_excluded_by_strict_enum_and_canonical_check():
    from pathlib import Path
    from biz.review.research_contract import _check_schema,ContractError
    frozen=json.loads((Path(__file__).parent/'fixtures/deepseek_invalid_review_reference_20261102.json').read_text(encoding='utf-8'))
    schema=frozen['schema'];failed=json.loads(frozen['failed_raw'])
    assert any(ref.startswith('C') for item in failed['findings'] for ref in item['fact_ids'])
    with pytest.raises(ContractError,match='schema_enum'):
        _check_schema(failed,schema)
    wire=core._strict_wire_schema(schema)
    permitted=wire['properties']['findings']['items']['properties']['fact_ids']['items']['enum']
    assert all(x.startswith('F') for x in permitted)
    assert permitted==schema['properties']['findings']['items']['properties']['fact_ids']['items']['enum']
    _check_schema(frozen['strict_result'],schema)


def test_schema_compilation_preserves_property_names_and_rejects_unknown_constraints():
    schema={'type':'object','properties':{'title':{'type':'string','const':'value','minLength':1},'maxItems':{'type':'string'}},'required':['title','maxItems'],'additionalProperties':False}
    wire=core._strict_wire_schema(schema)
    assert set(wire['properties'])=={'title','maxItems'}
    assert wire['properties']['title']=={'type':'string','enum':['value']}
    assert wire['required']==schema['required'] and wire['additionalProperties'] is False
    with pytest.raises(ValueError,match='unsupported strict schema keyword'):
        core._strict_wire_schema({'type':'string','unknown_constraint':True})


def test_report_parameter_extraction_preserves_original_model_bytes_and_rejects_other_shapes():
    from pathlib import Path
    raw=(Path(__file__).parent/'fixtures/deepseek_oversized_intraday_20261102.json').read_text(encoding='utf-8')
    arguments=' { "report" :\n'+raw+'\n}\n'
    payload,prefix,suffix=core._report_argument(arguments)
    assert payload==raw and prefix+payload+suffix==arguments
    for invalid in (raw,'{"report":{}}','{"report":{"a":1},"other":2}','{"report":{"a":1},"report":{"b":2}}'):
        with pytest.raises(ValueError):core._report_argument(invalid)
