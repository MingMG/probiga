import json
import hashlib
import re
from datetime import date
from pathlib import Path
import shutil
import subprocess
from unittest.mock import patch

import pytest

from server.api.routers import hot_data


def _quant_row(*, review_date="2026-08-12", status="ready"):
    return {
        "review_date": date.fromisoformat(review_date),
        "adjust_type": 0,
        "publish_status": status,
        "compact_review": "大势分析\n真实量化正文" if status == "ready" else "",
        "quality_json": json.dumps(
            {
                "status": "pass" if status == "ready" else "blocked",
                "coverage": {"target": 0.995},
                "errors": [] if status == "ready" else ["目标日有效行情覆盖率不足"],
            },
            ensure_ascii=False,
        ),
        "market_structure_json": "{}",
        "industry_rotation_json": "{}",
        "factor_validation_json": "{}",
        "data_cutoff_at": "2026-08-12 20:57:06",
    }


def test_quant_review_exact_blocked_never_falls_back():
    blocked = _quant_row(status="blocked")
    with patch("server.api.routers.hot_data._read_sql", return_value=[blocked]) as read_sql:
        result = hot_data.quant_daily_review("2026-08-12", 0)

    assert result["date"] == "2026-08-12"
    assert result["fallback"] is False
    assert result["data"] == [blocked]
    assert read_sql.call_count == 1
    sql = read_sql.call_args.args[0]
    assert "review_date = :d" in sql
    assert "review_date < :d AND publish_status = 'ready'" in sql


def test_quant_review_missing_exact_uses_latest_earlier_ready():
    ready = _quant_row(review_date="2026-08-11")
    with patch("server.api.routers.hot_data._read_sql", return_value=[ready]) as read_sql:
        result = hot_data.quant_daily_review("2026-08-12", 0)

    assert result["date"] == "2026-08-11"
    assert result["requested_date"] == "2026-08-12"
    assert result["fallback"] is True
    assert result["data"] == [ready]
    assert "publish_status = 'ready'" in read_sql.call_args.args[0]
    assert read_sql.call_count == 1


def test_daily_review_dates_survives_either_table_missing():
    def read_rows(sql, params=None):
        if "st_quant_review_digest" in sql:
            raise RuntimeError("table does not exist")
        return [{"d": date(2026, 8, 11)}, {"d": "2026-08-10"}]

    with patch("server.api.routers.hot_data._read_sql", side_effect=read_rows):
        result = hot_data.daily_review_dates()

    assert result["dates"] == ["2026-08-11", "2026-08-10"]
    assert result["warnings"] == ["st_quant_review_digest: table does not exist"]


def test_daily_review_dates_merges_and_deduplicates_both_tables():
    with patch(
        "server.api.routers.hot_data._read_sql",
        side_effect=[
            [{"d": date(2026, 8, 12)}, {"d": date(2026, 8, 11)}],
            [{"d": "2026-08-11"}, {"d": "2026-08-05"}],
        ],
    ):
        result = hot_data.daily_review_dates()

    assert result == {"dates": ["2026-08-12", "2026-08-11", "2026-08-05"]}


def test_export_exact_blocked_returns_quality_without_legacy_lookup():
    blocked = _quant_row(status="blocked")
    with patch("server.api.routers.hot_data._read_sql", return_value=[blocked]) as read_sql:
        result = hot_data.export_daily_review("2026-08-12")

    assert result["publish_status"] == "blocked"
    assert result["fallback"] is False
    assert result["error"] == "量化复盘未通过质量门禁"
    assert result["quality"]["errors"] == ["目标日有效行情覆盖率不足"]
    assert "text" not in result
    assert read_sql.call_count == 1


def test_export_ready_quant_is_preferred_and_marks_fallback():
    ready = _quant_row(review_date="2026-08-11")
    with patch("server.api.routers.hot_data._read_sql", return_value=[ready]) as read_sql:
        result = hot_data.export_daily_review("2026-08-12")

    assert result["date"] == "2026-08-11"
    assert result["requested_date"] == "2026-08-12"
    assert result["fallback"] is True
    assert result["publish_status"] == "ready"
    assert result["text"] == "大势分析\n真实量化正文"
    assert read_sql.call_count == 1


def test_export_uses_legacy_only_when_quant_has_no_record():
    with patch(
        "server.api.routers.hot_data._read_sql",
        side_effect=[[], [{"pro_review": "旧版历史复盘"}]],
    ) as read_sql:
        result = hot_data.export_daily_review("2026-08-05")

    assert result == {"date": "2026-08-05", "text": "旧版历史复盘"}
    assert read_sql.call_count == 2


def test_export_does_not_hide_operational_quant_query_failure_with_legacy():
    with patch(
        "server.api.routers.hot_data._read_sql",
        side_effect=RuntimeError("database connection lost"),
    ) as read_sql:
        result = hot_data.export_daily_review("2026-08-12")

    assert result["publish_status"] == "error"
    assert "database connection lost" in result["error"]
    assert read_sql.call_count == 1


def test_export_allows_legacy_when_quant_table_is_not_deployed():
    with patch(
        "server.api.routers.hot_data._read_sql",
        side_effect=[
            RuntimeError("Table 'probiga.st_quant_review_digest' doesn't exist"),
            [{"pro_review": "旧版历史复盘"}],
        ],
    ) as read_sql:
        result = hot_data.export_daily_review("2026-08-05")

    assert result == {"date": "2026-08-05", "text": "旧版历史复盘"}
    assert read_sql.call_count == 2


def test_review_page_contract_requests_quant_and_exports_markdown():
    source = (
        Path(__file__).resolve().parents[1] / "server" / "static" / "js" / "app.js"
    ).read_text(encoding="utf-8")

    assert "apiGet('/daily-review/quant?review_date=' + d)" in source
    assert "统一研究报告为默认视图" in source
    assert "质量门禁未通过" in source
    assert "fetch('/api/hot-data/daily-review/export?review_date='" in source
    assert "text/markdown;charset=utf-8" in source


def test_review_page_renders_bound_early_briefing_without_replacing_review(tmp_path):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js unavailable")
    source = (Path(__file__).resolve().parents[1] / "server/static/js/app.js").read_text(encoding="utf-8")
    renderer = source[source.index("    function _renderProReview(text) {"):source.index("    function portfolioCapitalInflowAlerts(")]
    escaping = source[source.index("    function escAttr(v) {"):source.index("    function safeText(v)")]
    start = source.index("        review: function (d, c) {")
    review = source[start:source.index("        portfolio: function", start)].rstrip().rstrip(",")
    runner = tmp_path / "research-reports.cjs"
    runner.write_text(r"""
const assert = require('node:assert/strict');
const vm = require('node:vm');
let row;
const context = {
  syncDateFromResponse() {},
  jsonF(value, fallback) { if (typeof value === 'object') return value; try { return JSON.parse(value); } catch (_) { return fallback; } },
  apiGet(url) { return Promise.resolve(url.includes('/quant?') ? {date:'2026-10-30',data:[row]} : {data:[]}); }
};
vm.runInNewContext(ESCAPING + RENDERER + '\nvar pages = {' + REVIEW + '};', context);
const morning = {status:'COMPLETE',report_kind:'briefing',review_date:'2026-10-30',next_trade_date:'2026-11-02',generated_at:'2026-11-02T08:30:00+08:00',content:'# 次日早报\n\n| 时间窗 | 验证 |\n|---|---|\n| 开盘 | <script>alert(1)</script> 或 A\\|B |'};
async function render(patch, asJsonString = true) {
  const early = Object.assign({}, morning, patch);
  const factors = {early_briefing:early};
  row = {review_date:'2026-10-30',publish_status:'ready',compact_review:'# 前日复盘\n\n原始收盘判断',quality_json:'{}',factor_validation_json:asJsonString ? JSON.stringify(factors) : factors};
  const container = {innerHTML:''};
  context.pages.review('2026-10-30', container);
  await new Promise(resolve => setImmediate(resolve));
  return container.innerHTML;
}
(async () => {
  for (const asJsonString of [true, false]) {
    const html = await render({}, asJsonString);
    assert.match(html, /原始收盘判断/);
    assert.match(html, /<details class="review-early-briefing"/);
    assert.match(html, /下一交易日早报参考 \| 2026-11-02/);
    assert.match(html, /<h1[^>]*>次日早报<\/h1>/);
    assert.match(html, /<table class="data-table">/);
    assert.match(html, /&lt;script&gt;alert\(1\)&lt;\/script&gt; 或 A\|B/);
    assert.doesNotMatch(html, /<script>/);
    assert.equal(row.compact_review, '# 前日复盘\n\n原始收盘判断');
  }
  for (const patch of [{review_date:'2026-10-29'},{next_trade_date:'2026-10-30'},{next_trade_date:'bad'},{report_kind:'review'},{status:'BLOCKED'},{content:''}]) {
    const html = await render(patch);
    assert.doesNotMatch(html, /review-early-briefing/);
    assert.match(html, /原始收盘判断/);
  }
  row.publish_status = 'blocked';
  const container = {innerHTML:''};
  context.pages.review('2026-10-30', container);
  await new Promise(resolve => setImmediate(resolve));
  assert.doesNotMatch(container.innerHTML, /review-early-briefing/);
})().catch(error => { console.error(error); process.exit(1); });
""".replace("ESCAPING", json.dumps(escaping)).replace("RENDERER", json.dumps(renderer)).replace("REVIEW", json.dumps(review)), encoding="utf-8")
    result = subprocess.run([node, str(runner)], capture_output=True, text=True, encoding="utf-8", timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr



def test_app_script_cache_identity_tracks_its_own_content():
    root = Path(__file__).resolve().parents[1]
    app = (root / "server/static/js/app.js").read_bytes().replace(b"\r\n", b"\n")
    index = (root / "server/static/index.html").read_text(encoding="utf-8")
    script = re.search(r'<script src="(/static/js/app\.js\?v=([0-9a-f]{16}))"></script>', index)
    assert script, "app.js must use its content identity, not the previously cached v=141 URL"
    assert script.group(2) == hashlib.sha256(app).hexdigest()[:16]
    assert script.group(1) != "/static/js/app.js?v=141"
    # The observed old browser cache remains keyed to v=141. A content update
    # must request a different URL even if that entry is still considered fresh.
    old_browser_cache = {"/static/js/app.js?v=141": "old Markdown renderer"}
    assert script.group(1) not in old_browser_cache


def test_report_markdown_preserves_semantics_and_decodes_text_without_allowing_html(tmp_path):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js unavailable")
    source = (Path(__file__).resolve().parents[1] / "server/static/js/app.js").read_text(encoding="utf-8")
    renderer = source[source.index("    function _renderProReview(text) {"):source.index("    function portfolioCapitalInflowAlerts(")]
    escaping = source[source.index("    function escAttr(v) {"):source.index("    function safeText(v)")]
    runner = tmp_path / "readable-reports.cjs"
    runner.write_text(r"""
const assert = require('node:assert/strict');
const vm = require('node:vm');
const context = {};
vm.runInNewContext(ESCAPING + RENDERER, context);
const markdown = '# A股盘后复盘｜2026-10-30\n\n> 行情日：10月30日\n> 新闻截止：23:59\n\n## 主线与身份\n\n先观察**分歧承接**。\n\n- &#x6765;源：同花顺\n- &lt;script&gt;alert(1)&lt;/script&gt;\n\n1. 验证条件\n2. 放弃条件\n\n| 方向 | 证据 |\n|---|---|\n| CRO | A\\|B |\n\n&#x3c;img src=x onerror=alert(2)&#x3e;\n\n<script>alert(3)</script>';
const html = context._renderProReview(markdown);
assert.match(html, /<h1[^>]*>A股盘后复盘｜2026-10-30<\/h1>/);
assert.match(html, /<h2[^>]*>主线与身份<\/h2>/);
assert.match(html, /<blockquote[^>]*><p[^>]*>行情日：10月30日<\/p><p[^>]*>新闻截止：23:59<\/p><\/blockquote>/);
assert.match(html, /<ul[^>]*><li[^>]*>来源：同花顺<\/li>/);
assert.match(html, /<ol[^>]*><li[^>]*>验证条件<\/li><li[^>]*>放弃条件<\/li><\/ol>/);
assert.match(html, /<strong>分歧承接<\/strong>/);
assert.match(html, /<th scope="col">方向<\/th>/);
assert.match(html, /<td>A\|B<\/td>/);
assert.match(html, /&lt;script&gt;alert\(1\)&lt;\/script&gt;/);
assert.match(html, /&lt;img src=x onerror=alert\(2\)&gt;/);
assert.doesNotMatch(html, /<(script|img)\b/);
assert.doesNotMatch(html, /&#x6765;|&amp;#x6765;/);
const imported = String.raw`\# 次日早报
\> 目标：11月2日
\## 盘中验证
\| 时间 | 条件 |
\|---|---|
\| 开盘 | 等待确认 |`;
const importedHtml = context._renderProReview(imported);
assert.match(importedHtml, /<h1[^>]*>次日早报<\/h1>/);
assert.match(importedHtml, /<blockquote/);
assert.match(importedHtml, /<table class="data-table">/);
assert.match(importedHtml, /<td>开盘<\/td>/);
const literal = context._renderProReview('这是 \\# 字面符号，不是报告标题。');
assert.doesNotMatch(literal, /<h1/);
assert.match(literal, /\\# 字面符号/);
""".replace("ESCAPING", json.dumps(escaping)).replace("RENDERER", json.dumps(renderer)), encoding="utf-8")
    result = subprocess.run([node, str(runner)], capture_output=True, text=True, encoding="utf-8", timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
