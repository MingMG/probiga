from datetime import date, datetime, timedelta
from pathlib import Path
import json
import shutil
import subprocess

import pytest
from sqlalchemy import create_engine, text, event

from server.api.routers import research_tools as tools
from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
from server.common.formal_analysis_store import FormalAnalysisStore
from server.common.qmt_attestation_contract import canonical_digest

DAY = "2026-11-18"


@pytest.fixture
def database(monkeypatch):
    engine = create_engine("sqlite://")
    days = []
    cursor = date.fromisoformat(DAY)
    while len(days) < 30:
        if cursor.weekday() < 5:
            days.append(cursor.isoformat())
        cursor -= timedelta(days=1)
    days.reverse()
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE si_trade_calendar (trade_date TEXT,trade_status INT)")
        conn.exec_driver_sql("CREATE TABLE st_a_list_info (trade_date TEXT,stock_code TEXT,operate_name TEXT,a_buy_amount REAL,a_sell_amount REAL,reason TEXT)")
        conn.exec_driver_sql("CREATE TABLE qmt_history_coverage_manifest (trade_date TEXT,dataset TEXT,status TEXT,strategy_eligible INT,captured_at TEXT,manifest_hash TEXT)")
        conn.exec_driver_sql("CREATE TABLE sm_stock_kline (stock_code TEXT,short_name TEXT,trade_date TEXT,close REAL,high REAL,amount REAL,data_source TEXT,quality_status TEXT,permission_status TEXT,k_type INT,adjust_type INT,etl_sync_at TEXT)")
        conn.execute(text("INSERT INTO si_trade_calendar VALUES (:day,1)"), [{"day": day} for day in days])
    monkeypatch.setattr(tools, "get_engine", lambda: engine)
    monkeypatch.setattr(tools, "get_kline_engine", lambda: engine)
    monkeypatch.setattr(tools, "_membership", lambda *_: ({"industry_hash": "a"*64,"captured_at":DAY+"T16:00:00"},[
        {"stock_code":code,"industry_code":"I1","industry_name":"电子"} for code in ["000001","000002","000003"]
    ]))
    yield engine, days
    engine.dispose()


def quote(conn, code, day, price, *, peak=150, source="qmt_datadir", amount=100):
    conn.execute(text("INSERT INTO sm_stock_kline VALUES (:code,:code,:day,:price,:peak,:amount,:source,'VERIFIED','SUPPORTED',1,0,:known)"),
                 {"code":code,"day":day,"price":price,"peak":peak,"amount":amount,"source":source,"known":day+" 16:00:00"})


def test_seat_observation_deduplicates_disclosures_and_uses_trade_sessions(database):
    engine, _ = database
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO st_a_list_info VALUES (:day,:code,:seat,:buy,0,:reason)"), [
            {"day":"2026-11-13","code":"000001","seat":"公开营业部","buy":100,"reason":"涨幅榜"},
            {"day":"2026-11-13","code":"000001","seat":"公开营业部","buy":100,"reason":"换手榜"},
            {"day":"2026-11-13","code":"000001","seat":"另一营业部","buy":20,"reason":"涨幅榜"},
            {"day":DAY,"code":"000002","seat":"公开营业部","buy":10,"reason":"涨幅榜"},
        ])
        quote(conn,"000001","2026-11-13",100)
        quote(conn,"000001","2026-11-16",105)
        quote(conn,"000002",DAY,80)
    result = tools.build_seat_observation(DAY, seat="公开营业部")
    assert result["buy_amount"] == 110
    assert result["raw_disclosure_count"] == 4 and result["deduplicated_count"] == 3
    assert result["mature_count"] == 1 and result["positive_ratio_pct"] == 100
    assert result["distribution"][2]["count"] == 1  # Exact 5%, not the >5% bucket.
    assert result["co_disclosures"] == [{"seat":"另一营业部","count":1}]
    assert result["rows"][0]["observation_status"] == "NOT_MATURE"
    mature = result["rows"][1]
    assert mature["exit_date"] == "2026-11-16" and mature["price_change_pct"] == 5


def test_period_drawdowns_exclude_missing_sessions_and_unverified_sources(database):
    engine, days = database
    window = days[-3:]
    with engine.begin() as conn:
        for day in window:
            quote(conn,"000001",day,100,peak=150,amount=200)
            quote(conn,"000002",day,100,peak=125)
            quote(conn,"000003",day,10,peak=1000,source="unverified")
        quote(conn,"000004",window[-1],5,peak=1000)
        quote(conn,"000001","2026-11-19",1,peak=10000)
    result = tools.build_period_drawdowns(DAY,start=window[0],tmt=True)
    assert result["complete_sample_count"] == 2
    assert [r["stock_code"] for r in result["rows"]] == ["000001","000002"]
    assert result["rows"][0]["peak"] == 150
    assert result["rows"][0]["drawdown_pct"] == pytest.approx(100/3)


@pytest.mark.parametrize('period',[20,30])
def test_industry_width_requires_complete_verified_window_and_strict_ma(database,period):
    engine, days = database
    with engine.begin() as conn:
        for day in days[-period:]:
            quote(conn,"000001",day,120 if day == DAY else 100)
            quote(conn,"000002",day,100)  # Equal to MA is not above it.
            if day != days[-2]:
                quote(conn,"000003",day,120)
            quote(conn,"000004",day,500,source="unknown")
        quote(conn,"000001","2026-11-19",1)
    # Reproduce the production boundary: a large single aggregation cannot
    # finish, while bounded partitions must preserve the exact whole-window MA.
    batches=[]
    def bounded(_conn,_cursor,sql,_parameters,context,_many):
        if 'SUM(close)' in sql or 'AVG(close)' in sql:
            values=context.compiled_parameters[0]
            count=values['count']
            if count>5:
                raise TimeoutError('whole-window aggregate deadline exceeded')
            batches.append(count)
    event.listen(engine,'before_cursor_execute',bounded)
    result = tools.build_industry_breadth(DAY,period=period)
    assert sum(batches)==period
    assert result["sample_count"] == 2 and result["above_count"] == 1
    assert result["breadth_pct"] == 50
    assert result["rows"] == [{"industry_code":"I1","industry_name":"电子","member_count":3,
                               "sample_count":2,"above_count":1,"breadth_pct":50}]


def test_complete_native_partition_supplies_real_prices_and_survives_reader_restarts(database,monkeypatch):
    engine, days = database
    privileged_migrate_formal_analysis_schema(engine)
    native_day = days[-2]
    core = {"trade_date":native_day,"captured_at":DAY+"T16:00:00","run_id":"native-run",
            "entities":[{"stock_code":"000001","classification":"TRADED",
                         "evidence":{"high":"150","close":"100","amount":"200"}}]}
    manifest = {**core,"manifest_hash":canonical_digest(core)}
    completion = {"row_count":1,"run_id":"native-run"}
    loaded = []
    def verified_archive(connection,day,**kwargs):
        loaded.append(day)
        assert kwargs["manifest_hash"] == manifest["manifest_hash"]
        return manifest,completion
    monkeypatch.setattr(tools,"load_daily_native_evidence",verified_archive)
    monkeypatch.setattr(tools,"load_daily_manifest_completion",lambda *args,**kwargs: completion)
    with engine.begin() as conn:
        for day in days[-3:]:
            quote(conn,"000001",day,100,peak=120,source="unverified" if day==native_day else "qmt_datadir")
        conn.execute(text("INSERT INTO qmt_history_coverage_manifest VALUES (:day,:dataset,'EXACT',1,:known,:hash)"),
                     {"day":native_day,"dataset":tools.NATIVE_EVIDENCE_DATASET,"known":DAY+" 16:00:00","hash":manifest["manifest_hash"]})
    first = tools.build_period_drawdowns(DAY,start=days[-3])
    second = tools.build_period_drawdowns(DAY,start=days[-3])
    assert loaded == [native_day]
    assert first["complete_sample_count"] == second["complete_sample_count"] == 1
    assert first["rows"][0]["peak"] == 150  # The verified native price, not the unqualified vendor row.
    assert native_day in first["native_partitions"]
    # A valid artifact cannot be rebound to a different source manifest identity.
    wrong = "f"*64
    FormalAnalysisStore(engine).put(wrong,"research_native_daily",native_day,
                                   {"manifest":manifest,"completion_sha256":canonical_digest(completion)})
    with engine.begin() as conn:
        conn.execute(text("UPDATE qmt_history_coverage_manifest SET manifest_hash=:hash"),{"hash":wrong})
    with pytest.raises(ValueError,match="数据身份"):
        tools.build_period_drawdowns(DAY,start=days[-3])


def test_research_rendering_escapes_disclosed_names_and_keeps_unknowns():
    node = shutil.which("node")
    if not node:
        pytest.skip("node unavailable")
    path = Path(__file__).resolve().parents[1]/"server/static/js/research-tools.js"
    body = """
const assert=require('node:assert/strict');
const tools=require(PATH);
const html=tools.render('seats',{security_count:1,buy_amount:100,sell_amount:0,mature_count:0,
 positive_ratio_pct:null,mean_change_pct:null,median_change_pct:null,distribution:[],co_disclosures:[],
 rows:[{trade_date:'2026-11-18',stock_code:'000001',short_name:'<script>bad()</script>',price_change_pct:null,
 observation_status:'NOT_MATURE',buy_amount:100,sell_amount:0}]});
assert.ok(html.includes('&lt;script&gt;bad()&lt;/script&gt;'));
assert.ok(!html.includes('<script>bad()'));
assert.ok(html.includes('未成熟'));
assert.ok(!html.includes('NaN'));
""".replace("PATH",json.dumps(str(path)))
    result = subprocess.run([node,"-e",body],capture_output=True,text=True,encoding="utf-8",timeout=10)
    assert result.returncode == 0,result.stdout+result.stderr


def test_global_defaults_latest_and_preserves_history_mode_on_date_change():
    node = shutil.which("node")
    if not node:
        pytest.skip("node unavailable")
    path = Path(__file__).resolve().parents[1]/"server/static/js/research-tools.js"
    body = r'''
const assert=require('node:assert/strict');
global.setTimeout=()=>1;global.clearTimeout=()=>{};
const tools=require(PATH), calls=[], container={dataset:{},innerHTML:''};
const request=async url=>{
 calls.push(url);const q=new URL('http://unit.test'+url).searchParams;
 return {trade_date:q.get('date'),view_mode:q.get('mode'),status:'available',available_count:1,expected_count:41,
  known_at:'2026-11-21 16:00:00',rows:[{display_name:'test',availability:'missing',missing_reason:'<source failed>'}]};
};
(async()=>{
 await tools.load('global','2026-11-20',container,{request});
 assert.match(calls[0],/mode=latest/);assert.match(container.innerHTML,/当前观察/);
 assert.match(container.innerHTML,/&lt;source failed&gt;/);
 container.onchange({target:{closest:()=>({dataset:{rtControl:'mode'},value:'history'})}});
 await new Promise(resolve=>setImmediate(resolve));
 assert.match(calls.at(-1),/mode=history/);
 await tools.load('global','2026-11-19',container,{request});
 assert.match(calls.at(-1),/date=2026-11-19.*mode=history/);
 assert.match(container.innerHTML,/观察日期 2026-11-19/);
})().catch(error=>{console.error(error);process.exitCode=1;});
'''.replace("PATH",json.dumps(str(path)))
    result=subprocess.run([node,"-e",body],capture_output=True,text=True,encoding="utf-8",timeout=10)
    assert result.returncode == 0,result.stdout+result.stderr
