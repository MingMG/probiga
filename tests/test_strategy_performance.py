import copy
import json
import shutil
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from server.api.strategy_performance import _formal, _research, rank_results, read_performance_detail
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
