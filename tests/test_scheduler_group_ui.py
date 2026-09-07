import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js unavailable")
def test_scheduler_renders_known_unknown_and_empty_groups_once_and_safely():
    script = (ROOT / "server/static/js/app.js").read_text(encoding="utf-8")
    start = script.index("        scheduler: function (d, c) {")
    body_start = script.index("function (d, c)", start)
    end = script.index("        'stock-list': function", body_start)
    scheduler = script[body_start:end].rstrip().removesuffix(",")
    escape_start = script.index("    function escHtml(v) {")
    escape_end = script.index("    function safeText", escape_start)
    esc_html = script[escape_start:escape_end]

    harness = f"""
const assert = require('node:assert/strict');
const rendered = [];
const sections = [];
const response = {{
  runtime: {{}},
  data: [
    {{id: 1, task_name: 'known', group_name: '盘中交易'}},
    {{id: 134, task_name: 'research', group_name: 'strategy_v3'}},
    {{id: 135, task_name: 'unsafe', group_name: '<img src=x onerror=alert(1)>'}},
    {{id: 136, task_name: 'prototype', group_name: '__proto__'}},
    {{id: 137, task_name: 'constructor', group_name: 'constructor'}},
    {{id: 2, task_name: 'empty', group_name: ''}},
    {{id: 3, task_name: 'known-two', group_name: '系统管理'}}
  ]
}};
const window = {{
  renderTable(section, tableId, cols, tasks, renderFn, pageSize, titleHtml) {{
    rendered.push({{tableId, ids: tasks.map(task => task.id), pageSize, titleHtml}});
  }}
}};
const document = {{
  createElement() {{ return {{style: {{}}}}; }}
}};
function fetch() {{ return Promise.resolve({{json: () => Promise.resolve(response)}}); }}
{esc_html}
const scheduler = {scheduler};
const container = {{
  innerHTML: '',
  appendChild(section) {{ sections.push(section); }}
}};
(async () => {{
  scheduler('2026-09-07', container);
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(rendered.map(group => group.ids), [[1], [3], [134], [135], [136], [137], [2]]);
  assert.equal(new Set(rendered.flatMap(group => group.ids)).size, response.data.length);
  assert.equal(new Set(rendered.map(group => group.tableId)).size, rendered.length);
  assert.ok(rendered.every(group => group.pageSize === 30));
  assert.match(rendered[2].titleHtml, /策略 V3/);
  assert.doesNotMatch(rendered[3].titleHtml, /<img/);
  assert.match(rendered[3].titleHtml, /&lt;img/);
  assert.match(rendered[4].titleHtml, /__proto__/);
  assert.match(rendered[5].titleHtml, /constructor/);
  assert.match(rendered[6].titleHtml, /其他/);
  process.stdout.write(JSON.stringify({{status: 'PASS'}}));
}})().catch(error => {{ console.error(error); process.exit(1); }});
"""
    result = subprocess.run(
        [shutil.which("node") or "node", "-"],
        input=harness,
        text=True,
        capture_output=True,
        check=False,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout) == {"status": "PASS"}
