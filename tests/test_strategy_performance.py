import copy
import json
import shutil
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from server.api.strategy_performance import _formal, _research, _research_win_rate_history, _attach_formal_win_rate_history, rank_results, read_performance_detail
from server.trading_v2.config import canonical_json_hash


def formal_row():
    report = {"strategy_key": "trend", "account_initial_cash": "200000", "completed_trade_count": 3,
        "performance": {"total_return": 0.10, "max_drawdown": -0.04},
        "metrics": {"cumulative_net_pnl": "25000", "win_rate": 2 / 3},
        "execution_assumptions": {"cost_basis": "CONFIRMED", "fee_profile": "one"},
        "strategy_binding": {"strategy_id": "trend", "strategy_version": "v1", "config_hash": "c",
            "code_commit_sha": "code", "protocol_version": "v2_research_protocol_1"}}
    return {"backtest_uid": "report01", "strategy_id": "trend", "strategy_version": "v1",
        "strategy_identity_status": "VERIFIED", "status": "COMPLETED", "result": report,
        "result_hash": canonical_json_hash(report), "start_date": "2026-07-01", "end_date": "2026-07-31",
        "data_snapshot_hash": "dataset", "protocol_version": "v2_research_protocol_1", "config_hash": "c",
        "code_commit_sha": "code"}


def test_account_return_is_not_closed_trade_return_and_drawdown_is_positive():
    r = _formal(formal_row())
    assert r["net_return_pct"] == 10
    assert r["net_profit_cny"] == 20000
    assert r["closed_trade_net_pnl_cny"] == 25000
    assert r["max_drawdown_pct"] == 4
    assert r["rank_eligible"] is True


def test_tampered_report_or_unbound_identity_never_enters_rankings():
    raw = formal_row()
    raw["result"]["performance"]["total_return"] = 90
    r = _formal(raw)
    assert r["net_return_pct"] is None and not r["rank_eligible"]
    raw = formal_row()
    raw["strategy_identity_status"] = "UNAVAILABLE"
    r = _formal(raw)
    assert r["net_return_pct"] == 10 and not r["rank_eligible"]
    assert rank_results([r]) == []


def test_same_group_ranks_net_returns_with_ties_dedup_and_distinct_cost_cohorts():
    a = _formal(formal_row())
    b = {**a, "strategy_key": "other", "net_return_pct": -5}
    tie = {**a, "strategy_key": "tied"}
    alternative_cost = formal_row()
    alternative_cost["result"]["execution_assumptions"]["fee_profile"] = "two"
    alternative_cost["result_hash"] = canonical_json_hash(alternative_cost["result"])
    groups = rank_results([a, b, tie, copy.deepcopy(a), _formal(alternative_cost)])
    assert len(groups) == 2
    assert [(r["strategy_key"], r["rank"]) for r in groups[0]["rows"]] == [("tied", 1), ("trend", 1), ("other", 3)]


def test_no_trades_or_failed_execution_has_no_profit_rank():
    raw = formal_row()
    raw["result"]["completed_trade_count"] = 0
    raw["result_hash"] = canonical_json_hash(raw["result"])
    assert _formal(raw)["rank_eligible"] is False
    raw["status"] = "FAILED"
    assert _formal(raw)["net_profit_cny"] is None


def test_blocked_research_preserves_missing_dates_and_unknown_pnl():
    p = {"rule_hash": "abc", "strategy_key": "new", "version": "v1", "name": "新策略",
        "validation": {"status": "BLOCKED", "reason": "缺特征", "available_feature_sessions": 4,
            "required_sessions": 120, "missing_feature_dates": ["2026-07-01"]}}
    r = _research({"run_id": "run"}, p)
    assert r["net_return_pct"] is None and r["net_profit_cny"] is None
    assert r["available_feature_sessions"] == 4
    assert r["missing_feature_dates"] == ["2026-07-01"]
    assert not r["rank_eligible"]


def test_subset_research_has_its_own_rank_and_never_claims_full_strategy_return():
    p = {"rule_hash": "abc", "strategy_key": "new", "version": "v1", "name": "新策略",
         "validation": {"status": "PARTIAL", "source_input_sha256": "common", "dataset_sha256": "per-rule",
             "sessions": ["2026-07-01", "2026-07-02"], "report": {"status": "PARTIAL",
                 "initial_cash_cny": 1000, "return_scope": "VERIFIED_INPUT_SUBSET", "net_return_pct": None,
                 "verified_input_subset_return_pct": 5, "fees": {"kind": "ESTIMATE"},
                 "completed_trade_count": 1, "trades": [{"net_pnl_cny": 50, "estimated_fee_cny": 5}],
                 "nav": [{"nav_cny": 1000}, {"nav_cny": 1050}], "execution_mode": "MODELED_DAILY_BAR"}}}
    r = _research({"run_id": "run"}, p)
    assert r["net_return_pct"] is None and r["net_profit_cny"] is None
    assert r["verified_input_subset_return_pct"] == 5 and r["verified_input_subset_profit_cny"] == 50
    other = copy.deepcopy(p)
    other["strategy_key"] = "other"
    other["validation"]["dataset_sha256"] = "different-rule-signals"
    other["validation"]["report"]["verified_input_subset_return_pct"] = -1
    groups = rank_results([r, _research({"run_id": "run"}, other)])
    assert len(groups) == 1 and len(groups[0]["rows"]) == 2
    assert groups[0]["cohort"]["return_scope"] == "VERIFIED_INPUT_SUBSET"
    assert groups[0]["rows"][0]["strategy_key"] == "new"


def test_win_rate_curve_uses_all_closed_trades_and_aggregates_the_exit_day():
    trades = [{"exit_at":"2026-07-01 09:30:00", "net_pnl_cny":10} for _ in range(51)]
    trades += [{"exit_date":"2026-07-02", "net_pnl_cny":-10},
               {"exit_date":"2026-07-02", "net_pnl_cny":0}]
    report = {"completed_trade_count":53}
    points = _research_win_rate_history(trades, report, ["2026-07-01","2026-07-31"])
    assert len(points) == 2 and points[0]["completed_trades"] == 51
    assert points[-1] == {"date":"2026-07-02", "completed_trades":53,
                          "winning_trades":51, "win_rate_pct":51/53*100}
    for invalid in ({"net_pnl_cny":None}, {"exit_date":""}, {"exit_date":"2026-08-01"}):
        broken = copy.deepcopy(trades)
        broken[-1].update(invalid)
        assert _research_win_rate_history(broken, report, ["2026-07-01","2026-07-31"]) == []
    assert _research_win_rate_history(trades, {"completed_trade_count":54}, ["2026-07-01","2026-07-31"]) == []
    assert _research_win_rate_history([], {"completed_trade_count":0}, ["2026-07-01","2026-07-31"]) == []


def test_actual_sql_trade_pagination_and_coverage():
    engine = create_engine("sqlite://")
    raw = formal_row()
    with engine.begin() as c:
        c.execute(text("""CREATE TABLE st_backtest_run_v2(backtest_uid TEXT,strategy_id TEXT,strategy_version TEXT,
            config_hash TEXT,code_commit_sha TEXT,protocol_version TEXT,status TEXT,result_json TEXT,result_hash TEXT,
            start_date TEXT,end_date TEXT,data_snapshot_hash TEXT)"""))
        c.execute(text("CREATE TABLE st_strategy_version_v2(strategy_id TEXT,version TEXT,config_hash TEXT)"))
        c.execute(text("INSERT INTO st_strategy_version_v2 VALUES('trend','v1','c')"))
        c.execute(text("INSERT INTO st_backtest_run_v2 VALUES(:backtest_uid,:strategy_id,:strategy_version,:config_hash,:code_commit_sha,:protocol_version,:status,:result_json,:result_hash,:start_date,:end_date,:data_snapshot_hash)"),
            {**raw, "result_json": json.dumps(raw["result"])})
        c.execute(text("""CREATE TABLE st_backtest_trade_v2(backtest_uid TEXT,trade_id TEXT,stock_code TEXT,
            entry_date TEXT,exit_date TEXT,quantity INT,buy_fill_amount REAL,sell_fill_amount REAL,
            buy_fees REAL,sell_fees REAL,trade_net_pnl REAL)"""))
        for i in range(3):
            c.execute(text("INSERT INTO st_backtest_trade_v2 VALUES('report01',:id,'000001','2026-07-01','2026-07-03',100,1000,1100,5,5,90)"), {"id": str(i)})
    d = read_performance_detail(engine, "report01", offset=1, limit=1)
    assert d["summary"]["identity_verified"] is True
    assert d["trade_coverage_matches"] is True
    assert d["trade_count"] == 3 and [r["trade_id"] for r in d["trades"]] == ["1"]
    record = d["summary"]
    _attach_formal_win_rate_history(engine, [record])
    assert record["win_rate_history"] == []  # The report says 2 wins, while all 3 SQL rows win.
    with engine.begin() as c:
        c.execute(text("UPDATE st_backtest_trade_v2 SET trade_net_pnl=-20 WHERE trade_id='2'"))
    _attach_formal_win_rate_history(engine, [record])
    assert record["win_rate_history"] == [{"date":"2026-07-03", "completed_trades":3,
        "winning_trades":2, "win_rate_pct":2/3*100}]
    with pytest.raises(KeyError):
        read_performance_detail(engine, "missing")
    engine.dispose()


def test_browser_shows_backtest_status_legacy_report_and_complete_money_metrics(tmp_path):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node unavailable")
    record = _formal(formal_row())
    record["strategy_name"] = '<img onerror="bad">'
    detail = {"summary": record, "trades": [], "offset": 0, "limit": 50, "trade_count": 0}
    data = {"strategies": [], "strategy_count": 0, "governance": {}, "tasks": []}
    performance = {"strategies": [{"strategy_name": "新策略", "strategy_version": "v1", "backtest_status": "BLOCKED", "reason": "缺特征"}],
        "leaderboards": rank_results([record]), "results": [record], "method": "同口径比较"}
    script = Path(__file__).resolve().parents[1] / "server/static/js/quant-strategy-workspace.js"
    runner = tmp_path / "performance.cjs"
    runner.write_text("global.window={};require("+json.dumps(str(script))+");const h=window.QuantStrategyWorkspace.render("+json.dumps({"data": data, "performance": performance, "resultDetail": detail, "window": "20", "order": "net"})+");for(const v of ['回测与赚钱排名','账户累计收益','20000.00元','25000.00元','10.00%','缺特征','前向模拟观察'])if(!h.includes(v))throw new Error(v);if(h.includes('<img'))throw new Error('unescaped evidence');", encoding="utf-8")
    subprocess.run([node, str(runner)], timeout=10, check=True)


def test_browser_win_rate_lines_keep_current_versions_unknown_samples_and_toggles(tmp_path):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node unavailable")
    record = _formal(formal_row())
    record["win_rate_history"] = [{"date":"2026-07-03", "win_rate_pct":50,
        "winning_trades":1, "completed_trades":2}]
    strategies = [{"strategy_key":"trend","strategy_name":"趋势","strategy_version":"v1","result_id":"report01"},
        {"strategy_key":"new","strategy_name":"<img onerror=bad>","strategy_version":"v2","backtest_status":"BLOCKED"}]
    performance = {"strategies":strategies,"results":[record],"leaderboards":[],"method":"证据"}
    state = {"data":{"strategies":[],"strategy_count":0},"performance":performance,"window":"20","order":"name"}
    script = Path(__file__).resolve().parents[1]/"server/static/js/quant-strategy-workspace.js"
    runner = tmp_path/"win-rate.cjs"
    runner.write_text("global.window={};require("+json.dumps(str(script))+");const s="+json.dumps(state)+";const render=window.QuantStrategyWorkspace.render;let h=render(s);for(const t of ['各策略累计胜率','50.00%','盈利 1 / 已平仓 2 笔','暂无有效样本'])if(!h.includes(t))throw Error(t);if((h.match(/<polyline/g)||[]).length!==1||h.includes('<img'))throw Error('invalid series');s.chartHidden=['trend'];h=render(s);if(h.includes('<polyline')||!h.includes('当前曲线均已隐藏'))throw Error('toggle');",encoding="utf-8")
    subprocess.run([node,str(runner)],timeout=10,check=True)



def test_default_experiment_prefers_recent_terminal_over_old_failure_and_honors_pin(monkeypatch):
    from server.api import strategy_performance as performance
    from server.api import quant_strategy_workspace as workspace
    from server.engine import strategy_native_backtest as native
    engine=create_engine('sqlite://')
    monkeypatch.setattr(workspace,'read_strategy_workspace',lambda *_a:{
        'trade_date':'2026-11-20','autonomous_research':{'runs':[]},'strategies':[]})
    runs=[{'run_id':str(index)*64,'status':status,'proposals':[]} for index,status in
          ((1,'NOT_STARTED'),(2,'BLOCKED'),(3,'FAILED'))]
    monkeypatch.setattr(native,'read_runs',lambda *_a,**_kw:runs)
    monkeypatch.setattr(performance.TradingV2ReadRepository,'backtests',lambda *_a:[])
    result=performance.read_performance(engine,'2026-11-20')
    assert result['experiment_id']=='2'*64
    runs.append({'run_id':'4'*64,'status':'RUNNING','proposals':[]})
    assert performance.read_performance(engine,'2026-11-20')['experiment_id']=='4'*64
    assert performance.read_performance(engine,'2026-11-20',experiment_id='3'*64)['experiment_id']=='3'*64
    engine.dispose()
