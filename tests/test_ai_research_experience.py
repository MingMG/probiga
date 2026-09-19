"""Research links only prefill a draft; provider identity is never market evidence."""
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="Node.js unavailable")


def _run(script):
    result = subprocess.run([NODE, "-e", script], cwd=ROOT, capture_output=True, text=True, encoding="utf-8")
    assert result.returncode == 0, result.stdout + result.stderr


def _functions(start, end):
    source = (ROOT / "server/static/js/ai-chat.js").read_text(encoding="utf-8")
    return source[source.index(start):source.index(end, source.index(start))]


def test_research_deep_link_validates_context_and_keeps_questions_editable():
    functions = _functions("  function prefillFromSearch", "  function applyPrefill")
    _run("const assert=require('assert');\n" + functions + """
let draft=prefillFromSearch('?stock_code=600519&question='+encodeURIComponent('有哪些未验证条件？'),'stock');
assert.strictEqual(draft.stockCode,'600519');assert(draft.question.includes('600519'));assert(draft.question.includes('未验证条件'));
draft=prefillFromSearch('?stock_code=000001.SZ','stock');
assert.strictEqual(draft.stockCode,'000001');assert(draft.question.includes('来源与截止日期'));assert(draft.question.includes('反方证据'));
assert.strictEqual(prefillFromSearch('?stock_code=javascript:evil','stock').stockCode,'');
assert.strictEqual(prefillFromSearch('?stock_code=1','stock').question,'');
draft=prefillFromSearch('?stock_code=600519&question='+encodeURIComponent('项目说明'),'general');
assert.strictEqual(draft.stockCode,'');assert.strictEqual(draft.question,'项目说明');
assert.strictEqual(prefillFromSearch('?question='+('x'.repeat(9000)),'general').question.length,8000);
""")


def test_page_initialization_prefills_without_submitting_an_ai_question():
    _run("""
const assert=require('assert'),fs=require('fs'),vm=require('vm');
const nodes={},calls=[];
function node(id){return nodes[id]||(nodes[id]={value:'',textContent:'',hidden:true,style:{},scrollHeight:80,addEventListener(){}});}
const document={body:{dataset:{channel:'stock'}},getElementById:node};
const window={location:{search:'?stock_code=000001&question='+encodeURIComponent('近期风险是什么？')},clearTimeout(){},setTimeout(){}};
const sandbox={document,window,URLSearchParams,Map,Number,Object,String,Promise,Date,console,fetch(url,options){calls.push({url,options});return Promise.resolve({ok:true,json(){return Promise.resolve({jobs:[]});}});}};
vm.runInNewContext(fs.readFileSync('server/static/js/ai-chat.js','utf8'),sandbox);
assert(node('questionInput').value.includes('000001'));assert(node('questionInput').value.includes('近期风险'));
assert.strictEqual(node('stockContext').hidden,false);
assert.strictEqual(node('stockContextLink').href,'/?tab=workbench&stock_code=000001');
assert.strictEqual(calls.length,1);assert(calls[0].url.includes('/questions?channel=stock'));
assert(!calls.some(call=>call.options.method==='POST'));
""")


def test_missing_answer_source_is_not_invented_and_errors_remain_in_diagnostics():
    functions = _functions("  function statusInfo", "  function setHeadline")
    _run("const assert=require('assert');\n" + functions + """
assert.strictEqual(statusInfo({status:'completed'}).label,'来源未确认');
assert.strictEqual(statusInfo({status:'completed',source:'codex_gpt'}).label,'GPT（Codex）');
assert.strictEqual(statusInfo({status:'completed',source:'deepseek_web'}).label,'DeepSeek 网页');
const failure={status:'failed',request_id:'abc',provider_attempt:'codex_gpt',error_message:'bridge worker unavailable'};
assert(!statusInfo(failure).state.includes('bridge worker'));
assert(diagnosticText(failure).includes('bridge worker unavailable'));
assert(diagnosticText(failure).includes('abc'));
""")
