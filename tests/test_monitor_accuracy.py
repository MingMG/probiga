from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import shutil
import subprocess

import pytest

from server.api.routers import hot_data


PREVIOUS = "2026-09-17"
CURRENT = "2026-09-18"


class FrozenNow(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 9, 19, 10)


def overview(up=2000, *, total=4000, amount=2e12):
    return {"up_cnt": up, "down_cnt": total - up, "sideline_cnt": 0,
            "total": total, "total_amount": amount, "small_up_cnt": 200,
            "small_total": 1000, "small_avg_chg": -7.5}


@pytest.fixture
def monitor(monkeypatch):
    monkeypatch.setattr(hot_data, "datetime", FrozenNow)
    monkeypatch.setattr(hot_data, "_cache_get", lambda *a, **kw: None)
    monkeypatch.setattr(hot_data, "_cache_set", lambda *a, **kw: None)
    monkeypatch.setattr(hot_data, "_portfolio_is_trading_day", lambda *a: True)
    monkeypatch.setattr(hot_data, "_get_realtime_overview", lambda **kw: None)
    monkeypatch.setattr(hot_data, "_read_sql", lambda *a, **kw: [])

    def run(*, dates=None, overviews=None, hot=None, indices=None, requested=CURRENT):
        dates = [PREVIOUS, CURRENT] if dates is None else dates
        monkeypatch.setattr(hot_data, "_monitor_resolve_trade_date", lambda _: CURRENT)
        monkeypatch.setattr(hot_data, "_monitor_history_trade_dates", lambda *a, **kw: dates)
        monkeypatch.setattr(hot_data, "_monitor_overview_map", lambda _: overviews if overviews is not None else {PREVIOUS: overview(), CURRENT: overview()})
        monkeypatch.setattr(hot_data, "_monitor_hot_rows_map", lambda *a, **kw: hot or {})
        monkeypatch.setattr(hot_data, "_monitor_index_price_map", lambda _: indices or {})
        return hot_data.monitor_data(requested)
    return run


def test_index_change_is_actual_index_and_board_sample_is_separate(monitor):
    data = monitor(indices={CURRENT: {"price": 6234.56, "change_pct": 1.25}})
    assert data["csi1000"]["change"] == 1.25
    assert data["csi1000"]["price"] == 6234.56
    assert data["board_sample"]["change"] == -7.5
    assert "heat" not in data["csi1000"]
    assert "csi1000_heat" not in data["history"]
    assert data["history"]["csi1000_price"] == [None, 6234.56]
    assert data["history"]["trade_dates"] == [PREVIOUS, CURRENT]


def test_missing_values_stay_missing_and_do_not_imply_neutrality(monitor):
    data = monitor(overviews={CURRENT: overview(amount=None)})
    assert data["heat_change"] is None
    assert data["breadth_change_pp"] is None
    assert data["heat_percentile"] is None
    assert data["heat_percentile_sample_count"] == 0
    assert data["total_amount"] is None
    assert data["csi1000"]["price"] is None
    assert data["csi1000"]["change"] is None
    assert data["csi1000"]["status"] == "unavailable"
    assert data["history"]["heat"] == [None, 500]
    assert data["history"]["amount"] == [None, None]
    assert data["history"]["tmt_ratio"] == [None, None]


def test_all_down_session_is_valid_percentile_observation(monitor):
    data = monitor(overviews={PREVIOUS: overview(up=0), CURRENT: overview(up=2000)})
    assert data["history"]["heat"] == [0, 500]
    assert data["heat_percentile"] == 100
    assert data["heat_percentile_sample_count"] == 1
    assert data["heat_change"] is None  # Relative change has a zero denominator.
    assert data["breadth_change_pp"] == 50


@pytest.mark.parametrize("hour,minute,status", [(12, 5, "paused"), (15, 20, "close")])
def test_current_snapshot_survives_lunch_and_close_in_history(monkeypatch, monitor, hour, minute, status):
    class SessionNow(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 9, 18, hour, minute)

    monkeypatch.setattr(hot_data, "datetime", SessionNow)
    observed_time = "11:30:00" if hour == 12 else "15:00:00"
    current = {**overview(up=3000), "data_time": f"{CURRENT} {observed_time}", "data_source": "sm_stock_current"}
    monkeypatch.setattr(hot_data, "_get_realtime_overview", lambda **kw: current)
    data = monitor(dates=[PREVIOUS], overviews={PREVIOUS: overview()})
    assert data["freshness_status"] == status
    assert data["history"]["heat"][-1] == 750
    assert data["history"]["amount"][-1] == 20000
    assert data["history"]["trade_dates"][-1] == CURRENT


def test_morning_snapshot_is_not_relabelled_as_a_close(monkeypatch, monitor):
    class AfterClose(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 9, 18, 15, 20)
    monkeypatch.setattr(hot_data, "datetime", AfterClose)
    monkeypatch.setattr(hot_data, "_get_realtime_overview", lambda **kw: {
        **overview(), "data_time": f"{CURRENT} 11:30:00", "data_source": "sm_stock_current",
    })
    data = monitor(dates=[PREVIOUS], overviews={PREVIOUS: overview()})
    assert data["freshness_status"] == "stale"
    assert not data["is_realtime"]
    assert data["data_time"] == f"{CURRENT} 11:30:00"


@pytest.mark.parametrize("missing", ["up_cnt", "down_cnt", "sideline_cnt", "total"])
def test_incomplete_breadth_does_not_invent_zero_counts(monitor, missing):
    row = overview()
    row[missing] = None
    data = monitor(overviews={CURRENT: row})
    assert data["status"] == "unavailable"
    assert "market_heat" not in data


def test_previous_rankings_never_become_todays_sector_evidence(monitor):
    row = {"snapshot_date": PREVIOUS, "concept_name": "昨日热榜", "hot_value": 100, "change_pct": 3}
    data = monitor(hot={(PREVIOUS, 3): [row], (PREVIOUS, 1): [row]})
    assert data["top_industries"] == []
    assert data["concept_rows"] == []
    assert data["tmt_ratio"] is None
    assert "周期股补涨" not in data["analysis"]["signal"]
    assert "科技板块轮动" not in data["analysis"]["signal"]


def test_tmt_uses_complete_turnover_sample_and_preserves_metric_units(monitor):
    rows = [{"snapshot_date": CURRENT, "concept_name": f"非TMT{i}", "hot_value": 10,
             "change_pct": 0, "data_source": "qmt_plate_aggregate"} for i in range(10)]
    rows.append({"snapshot_date": CURRENT, "concept_name": "半导体", "hot_value": 5,
                 "change_pct": None, "data_source": "qmt_plate_aggregate"})
    data = monitor(hot={(CURRENT, 3): rows})
    assert data["tmt_ratio"] == 4.76  # Includes the 11th industry, outside displayed top ten.
    assert data["top_industries"][0]["heat_unit"] == "亿元"
    assert data["top_industries"][0]["change"] == 0
    rows[0]["data_source"] = "hot_rank_fallback"
    assert monitor(hot={(CURRENT, 3): rows})["tmt_ratio"] is None


def test_monitor_does_not_disclose_database_errors(monkeypatch, monitor):
    private_detail = "unpublishable database driver diagnostic"
    def fail(*args):
        raise RuntimeError(private_detail)
    monkeypatch.setattr(hot_data, "_portfolio_is_trading_day", fail)
    result = monitor(requested="2026-09-19")
    encoded = json.dumps(result)
    assert result["error"] == "market_monitor_unavailable"
    assert private_detail not in encoded and "trace" not in result


def test_trade_date_fallback_cannot_read_future_or_adjusted_prices(monkeypatch):
    queries = []
    def read(sql, params=None):
        queries.append((sql, params))
        return []
    monkeypatch.setattr(hot_data, "_table_columns", lambda _: set())
    monkeypatch.setattr(hot_data, "_read_sql", read)
    assert hot_data._monitor_resolve_trade_date(CURRENT) == CURRENT
    assert "trade_date <= :d" in queries[-1][0]
    assert "adjust_type = 0" in queries[-1][0]
    assert queries[-1][1] == {"d": CURRENT}
    hot_data._monitor_overview_map_from_kline([CURRENT])
    assert "adjust_type = 0" in queries[-1][0]


def test_monitor_ui_preserves_zero_missing_and_refresh_failure_state(tmp_path):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js required for browser state regression")
    source = Path(__file__).resolve().parents[1] / "server/static/js/monitor.js"
    runner = tmp_path / "monitor-ui.cjs"
    runner.write_text(r"""
const fs = require('fs'), vm = require('vm'), assert = require('assert');
const nodes = {}, handlers = {}, canvas = new Proxy({}, {get:()=>()=>{}}), draws = [];
const document = {hidden:false, addEventListener:(n,fn)=>handlers[n]=fn, getElementById:(id)=>nodes[id]||(nodes[id]={style:{},getContext:()=>canvas})};
let fail = false;
const data = {status:'available',requested_date:'2026-09-18',trade_date:'2026-09-18',data_time:'2026-09-18',freshness_status:'close',total_count:4000,
market_heat:0,breadth_change_pp:0,heat_percentile:null,heat_percentile_sample_count:0,total_amount:0,tmt_ratio:null,sideline_ratio:0,
csi1000:{price:null,change:0,status:'unavailable'},history:{trade_dates:['2026-09-17','2026-09-18'],heat:[500,0],amount:[null,0],csi1000_price:[null,null]},
top_industries:[{name:'<img src=x onerror=alert(1)>',heat:0,change:null,trade_date:'2026-09-18',heat_unit:'亿元'}],concept_rows:[],analysis:{market_temp:'<b>plain</b>'}};
const context = {document,window:{location:{search:''},addEventListener:()=>{},Chart:function(node,config){draws.push(config);this.destroy=()=>{}}},URLSearchParams,AbortController,
setTimeout:()=>1,clearTimeout:()=>{},setInterval:()=>1,fetch:async()=>{if(fail)throw Error('network');return {ok:true,json:async()=>data}}};
vm.runInNewContext(fs.readFileSync(process.argv[2],'utf8'),context);
handlers.DOMContentLoaded();
setImmediate(()=>{
 assert.equal(nodes.heatValue.textContent,'0');assert.equal(nodes.gaugeValue.textContent,'—');assert.equal(nodes.gaugeStatus.textContent,'缺少可比样本');
 assert.equal(nodes.csi1000Heat.textContent,'—');assert.equal(nodes.csi1000Chg.textContent,'0.00%');assert.equal(nodes.tmtRatio.textContent,'—');
 assert.equal(nodes.analysisTemp.textContent,'<b>plain</b>');assert(nodes.industryTableBody.innerHTML.includes('&lt;img'));
 assert.equal(draws[0].data.labels[0],'2026-09-17');assert.equal(draws[0].data.datasets[0].spanGaps,false);
 fail=true;handlers.visibilitychange();setImmediate(()=>{assert.equal(nodes.monitorNotice.hidden,false);assert(nodes.monitorNotice.textContent.includes('保留上次'));assert(nodes.snapshotStatus.textContent.includes('刷新失败'));});
});
""", encoding="utf-8")
    result = subprocess.run([node, str(runner), str(source)], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr


def test_monitor_embedded_frame_lifecycle(tmp_path):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js required for browser state regression")
    source = (Path(__file__).resolve().parents[1] / "server/static/js/app.js").read_text(encoding="utf-8")
    source = source[source.index("    var monitorFrame = null;"):source.index("    function _restoreTab(savedTab)")]
    runner = tmp_path / "monitor-frame.cjs"
    runner.write_text(r"""
const vm = require('vm'), assert = require('assert');
const nodes = {}, messages = [], handlers = {}, document = {hidden:false,addEventListener:(n,fn)=>handlers[n]=fn,getElementById:id=>nodes[id]};
document.createElement=(tag)=>({tag,style:{},dataset:{},setAttribute:()=>{},addEventListener:()=>{},contentWindow:{postMessage:m=>messages.push(m)}});
const container={children:[],appendChild(child){this.children.push(child);child.parentNode=this;nodes[child.id]=child},removeChild(child){this.children=this.children.filter(x=>x!==child);child.parentNode=null}};
const context={document,window:{location:{origin:'http://local'},addEventListener:(n,fn)=>handlers[n]=fn},setTimeout:()=>1,clearTimeout:()=>{},currentDateValue:()=> '2026-09-18'};
vm.runInNewContext(SOURCE,context);context.loadMonitorPage(container,'2026-09-18');
const frame=container.children[1];assert.equal(frame.src,'/static/monitor.html?date=2026-09-18&embedded=1');
handlers.message({origin:'http://attacker',source:frame.contentWindow,data:{type:'probiga-monitor-ready'}});assert.equal(messages.length,0);
handlers.message({origin:'http://local',source:frame.contentWindow,data:{type:'probiga-monitor-ready'}});assert.equal(messages[0].visible,true);
document.hidden=true;handlers.visibilitychange();assert.equal(messages.at(-1).visible,false);
context.window.stopMonitorRefresh();assert.equal(frame.parentNode,null);
""".replace("SOURCE", json.dumps(source)), encoding="utf-8")
    result = subprocess.run([node, str(runner)], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("path,query,expected", [
    ("/static/monitor.html", "embedded=1", "SAMEORIGIN"),
    ("/static/monitor.html", "", "DENY"),
    ("/static/monitor.html", "embedded=0", "DENY"),
    ("/login", "embedded=1", "DENY"),
])
def test_monitor_embedded_headers_remain_same_origin_only(path, query, expected):
    import asyncio
    from fastapi import Request
    from fastapi.responses import Response
    from server.api import main
    from server.api.admin_auth import is_admin_protected_path

    request = Request({"type": "http", "method": "GET", "path": path,
                       "query_string": query.encode(), "headers": [], "scheme": "http",
                       "server": ("testserver", 80)})
    async def next_response(_):
        return Response("page")
    response = asyncio.run(main.add_security_headers(request, next_response))
    assert response.headers["X-Frame-Options"] == expected
    if expected == "SAMEORIGIN":
        assert response.headers["Content-Security-Policy"] == "frame-ancestors 'self'"
    assert is_admin_protected_path("/static/monitor.html", "GET")


def test_market_radar_has_one_canonical_application_route():
    from server.api.main import market_radar_page
    response = market_radar_page()
    assert response.status_code == 307
    assert response.headers["location"] == "/?tab=market-radar"
