import json
from pathlib import Path
import shutil
import subprocess

import pytest
from sqlalchemy import create_engine, text

from server.api.quant_strategy_workspace import read_strategy_workspace


@pytest.fixture
def workspace():
    engine = create_engine("sqlite://")
    schemas = {
        "st_strategy_registry": "strategy_key TEXT,strategy_name TEXT,category TEXT,family_key TEXT,description TEXT,current_version TEXT,current_status TEXT,enabled INT,status_reason TEXT",
        "st_strategy_governance_run": "run_uid TEXT,trade_date TEXT,market_state TEXT,input_ready INT,status TEXT,strategy_count INT,combination_count INT,finished_at TEXT,created_at TEXT,is_canonical INT",
        "st_strategy_health_snapshot": "run_uid TEXT,strategy_key TEXT,strategy_version TEXT,trade_date TEXT,window_days INT,completed_trades INT,coverage_days INT,net_expectancy_pct REAL,profit_factor REAL,max_drawdown_pct REAL,win_rate_pct REAL,profit_gate_passed INT,gate_reason TEXT,market_match_score REAL",
        "st_strategy_pool_snapshot": "run_uid TEXT,pool_level TEXT,stock_code TEXT,stock_name TEXT,dominant_strategy TEXT,opportunity_score REAL,execution_score REAL,gate_status TEXT,reason_json TEXT,rank_no INT",
        "st_scheduled_tasks": "id INT,task_type TEXT,enabled INT,last_run_status TEXT,last_run_at TEXT,last_run_output TEXT",
        "st_external_market_context": "snapshot_id TEXT,captured_at TEXT,context_date TEXT",
    }
    with engine.begin() as c:
        for name, fields in schemas.items():
            c.execute(text(f"CREATE TABLE {name} ({fields})"))
        c.execute(text("INSERT INTO st_strategy_registry VALUES('trend','趋势','stock','trend','研究','v2','SHADOW',1,'证据不足')"))
        c.execute(text("INSERT INTO st_strategy_governance_run VALUES('known','2026-11-20','high_range',1,'COMPLETED',1,0,'2026-11-20 23:00:00','2026-11-20 22:00:00',1),('future','2026-11-23','strong_trend',1,'COMPLETED',1,0,'2026-11-23 23:00:00','2026-11-23 22:00:00',1)"))
        c.execute(text("INSERT INTO st_strategy_health_snapshot VALUES('known','trend','v1','2026-11-20',20,30,20,10,4,2,90,1,'旧版本',80)"))
        c.execute(text("INSERT INTO st_strategy_pool_snapshot VALUES('known','observation','000001','股票','trend',80,0,'WATCH','[]',1)"))
        c.execute(text("INSERT INTO st_external_market_context VALUES('now','2026-11-21 18:00:00','2026-11-21')"))
    yield engine
    engine.dispose()


def test_current_version_never_inherits_previous_versions_returns_or_picks(workspace):
    d = read_strategy_workspace(workspace, "2026-11-20")
    assert d["strategy_count"] == 1
    assert d["governance"]["run_uid"] == "known"
    assert d["strategies"][0]["windows"] == {}
    assert d["strategies"][0]["picks"] == []
    assert d["external_observation"]["snapshot_id"] == "now"
    assert d["real_order_authority"] is False


def test_exact_version_reads_completed_returns_and_selection(workspace):
    with workspace.begin() as c:
        c.execute(text("UPDATE st_strategy_health_snapshot SET strategy_version='v2',net_expectancy_pct=-1.5"))
    d = read_strategy_workspace(workspace, "2026-11-20")
    row = d["strategies"][0]
    assert row["windows"]["20"]["net_expectancy_pct"] == -1.5
    assert row["picks"][0]["stock_code"] == "000001"
    assert d["governance_current"] is True


def test_no_completed_trades_do_not_report_zero_return_or_drawdown(workspace):
    with workspace.begin() as c:
        c.execute(text("UPDATE st_strategy_health_snapshot SET strategy_version='v2',completed_trades=0,net_expectancy_pct=0,profit_factor=0,max_drawdown_pct=0,win_rate_pct=0"))
    m = read_strategy_workspace(workspace, "2026-11-20")["strategies"][0]["windows"]["20"]
    assert m["completed_trades"] == 0
    assert all(m[field] is None for field in ("net_expectancy_pct", "profit_factor", "max_drawdown_pct", "win_rate_pct"))


def test_task_reason_keeps_business_failure_before_retry_marker(workspace):
    output = '\n'.join(json.dumps(row) for row in [
        {"reason": "历史行情认证读取超时", "reason_code": "GOVERNANCE_EVIDENCE_NOT_READY"},
        {"schema": "probiga.scheduler-transient-block.v1", "reason_code": "GOVERNANCE_EVIDENCE_NOT_READY"},
    ])
    with workspace.begin() as c:
        c.execute(text("INSERT INTO st_scheduled_tasks VALUES(85,'strategy_governance_daily',1,'blocked','2026-11-21 18:00:00',:output)"), {"output": output})
    assert read_strategy_workspace(workspace, "2026-11-20")["tasks"][0]["reason"] == "历史行情认证读取超时"


def test_drawdown_comparison_places_smaller_losses_first(workspace, tmp_path):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node is required for the browser module")
    d = read_strategy_workspace(workspace, "2026-11-20")
    base = d["strategies"][0]
    d["strategies"] = [{**base, "strategy_name": name, "windows": {"20": {"max_drawdown_pct": dd}}}
                       for name, dd in [("large loss", 20), ("small loss", 2), ("unknown loss", None)]]
    script = Path(__file__).resolve().parents[1] / "server/static/js/quant-strategy-workspace.js"
    runner = tmp_path / "compare.cjs"
    runner.write_text("global.window={};require(" + json.dumps(str(script)) + ");const d=" + json.dumps(d) + ";const h=window.QuantStrategyWorkspace.render({data:d,window:'20',order:'drawdown'});if(!(h.indexOf('small loss')<h.indexOf('large loss')&&h.indexOf('large loss')<h.indexOf('unknown loss')))process.exit(1);", encoding="utf-8")
    subprocess.run([node, str(runner)], check=True, timeout=10)


def test_workspace_renderer_preserves_unknowns_and_escapes_evidence(workspace, tmp_path):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node is required for the browser module")
    d = read_strategy_workspace(workspace, "2026-11-20")
    d["strategies"][0]["strategy_name"] = '<img onerror="bad">'
    script = Path(__file__).resolve().parents[1] / "server/static/js/quant-strategy-workspace.js"
    runner = tmp_path / "render.cjs"
    runner.write_text("global.window={};require(" + json.dumps(str(script)) + ");const d=" + json.dumps(d) + ";const h=window.QuantStrategyWorkspace.render({data:d,window:'20',order:'net'});if(h.includes('<img')||h.includes('0.00%')||!h.includes('未知')||!h.includes('QMT选股'))process.exit(1);", encoding="utf-8")
    subprocess.run([node, str(runner)], check=True, timeout=10)


def test_retry_request_works_without_secure_context_random_uuid(tmp_path):
    node = shutil.which('node')
    if not node:
        pytest.skip('Node is required for the browser module')
    script = Path(__file__).resolve().parents[1] / 'server/static/js/quant-strategy-workspace.js'
    runner = tmp_path / 'retry.cjs'
    runner.write_text('''
global.window={};global.document={activeElement:null};
Object.defineProperty(global,'crypto',{value:{getRandomValues:a=>require('node:crypto').webcrypto.getRandomValues(a)}});
require('''+json.dumps(str(script))+''');
const mount=Object.assign(new EventTarget(),{isConnected:false,querySelectorAll:()=>[],contains:()=>false});let sent;
QuantStrategyWorkspace=window.QuantStrategyWorkspace;
QuantStrategyWorkspace.load('2026-11-20',mount,async(u,t,o)=>{sent={u,o};return {};},()=>{},()=>{},()=>false);
mount._strategyWorkspace.data={};
(async()=>{await mount.onclick({target:{closest:s=>s==='[data-qsw-retry]'?{dataset:{qswRetry:'native:run:version'}}:null}});
if(!sent||sent.o.method!=='POST'||!/^.*request_uid=[0-9a-f]{32}$/.test(sent.u)||mount._strategyWorkspace.resultError)process.exit(1);
})();
''',encoding='utf-8')
    subprocess.run([node,str(runner)],check=True,timeout=10)


def test_performance_poll_keeps_verified_data_and_user_state_on_domain_failure():
    node = shutil.which('node')
    if not node:
        pytest.skip('Node is required for the browser module')
    runner = Path(__file__).resolve().parent / 'frontend/quant-strategy-workspace-dom.cjs'
    result = subprocess.run([node, str(runner)], capture_output=True, text=True,
                            encoding='utf-8', timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr
