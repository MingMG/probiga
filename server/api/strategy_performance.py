"""Version-bound performance readback; GET never starts or fabricates a replay."""
from __future__ import annotations

import math
import time
from collections import defaultdict
from datetime import date

from sqlalchemy import bindparam, text
from sqlalchemy.exc import SQLAlchemyError

from server.trading_v2.config import canonical_json_hash
from server.trading_v2.repository import TradingV2ReadRepository


def _number(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def _percent(value):
    value = _number(value)
    return value * 100 if value is not None else None


def _drawdown(nav):
    peak, worst = 0.0, 0.0
    for row in nav:
        value = _number(row.get("nav_cny"))
        if value is None or value <= 0:
            return None
        peak = max(peak, value)
        worst = max(worst, (1 - value / peak) * 100)
    return worst if nav else None


def _win_rate_history(daily, expected, start, end):
    """Cumulative closed-trade wins; flat trades count as completed, not wins."""
    if not daily or sum(item[1] for item in daily) != expected:
        return []
    points = []
    completed = wins = 0
    try:
        for day, count, winning, unknown in sorted(daily):
            day = date.fromisoformat(str(day)[:10]).isoformat()
            if unknown or not start <= day <= end or not 0 <= winning <= count:
                return []
            completed += count
            wins += winning
            points.append({"date": day, "completed_trades": completed,
                           "winning_trades": wins, "win_rate_pct": wins / completed * 100})
    except (TypeError, ValueError, ZeroDivisionError):
        return []
    return points


def _research_win_rate_history(trades, report, sessions):
    daily = defaultdict(lambda: [0, 0, 0])
    for trade in trades:
        day = str(trade.get("exit_date") or trade.get("exit_at") or "")[:10]
        pnl = _number(trade.get("net_pnl_cny"))
        row = daily[day]
        row[0] += 1
        row[1] += pnl is not None and pnl > 0
        row[2] += pnl is None
    return _win_rate_history([(day, *counts) for day, counts in daily.items()],
        report.get("completed_trade_count"), sessions[0], sessions[-1]) if sessions else []


def _attach_formal_win_rate_history(engine, records):
    current = {r["result_id"]: r for r in records if r["kind"] == "HISTORICAL_BACKTEST"
               and r["integrity_verified"] and r["identity_verified"] and r["status"] == "COMPLETE"}
    if not current:
        return
    statement = text("""SELECT /*+ MAX_EXECUTION_TIME(4000) */ backtest_uid,exit_date,
        COUNT(*) AS completed,SUM(CASE WHEN trade_net_pnl>0 THEN 1 ELSE 0 END) AS wins,
        SUM(CASE WHEN trade_net_pnl IS NULL THEN 1 ELSE 0 END) AS unknown
        FROM st_backtest_trade_v2 WHERE backtest_uid IN :ids GROUP BY backtest_uid,exit_date
        ORDER BY backtest_uid,exit_date""").bindparams(bindparam("ids", expanding=True))
    groups = defaultdict(list)
    with engine.connect() as connection:
        for uid, day, count, wins, unknown in connection.execute(statement, {"ids": list(current)}):
            groups[uid].append((day, int(count), int(wins), int(unknown)))
    for uid, record in current.items():
        points = _win_rate_history(groups[uid], record["completed_trades"],
            record["start_date"], record["end_date"])
        if points and record["win_rate_pct"] is not None and abs(points[-1]["win_rate_pct"]-record["win_rate_pct"]) > 0.01:
            points = []
        record["win_rate_history"] = points


def _formal(row):
    report = row.get("result") if isinstance(row.get("result"), dict) else {}
    metrics = report.get("metrics") if isinstance(report.get("metrics"), dict) else {}
    performance = report.get("performance") if isinstance(report.get("performance"), dict) else {}
    integrity = bool(row.get("result_hash") and canonical_json_hash(report) == row["result_hash"])
    identity = row.get("strategy_identity_status") == "VERIFIED"
    complete = row.get("status") == "COMPLETED"
    initial = _number(report.get("account_initial_cash"))
    net_return = _percent(performance.get("total_return")) if integrity and complete else None
    pnl = initial * net_return / 100 if initial is not None and net_return is not None else None
    costs = report.get("execution_assumptions") or {}
    binding = report.get("strategy_binding") or {}
    cohort = {
        "start_date": str(row.get("start_date"))[:10], "end_date": str(row.get("end_date"))[:10],
        "data_snapshot_hash": row.get("data_snapshot_hash"), "initial_cash_cny": initial,
        "execution_assumptions": costs, "protocol_version": row.get("protocol_version"),
        "universe": report.get("eligible_universe"), "adapter": report.get("adapter"),
    }
    eligible = bool(integrity and identity and complete and net_return is not None
                    and initial and costs and row.get("data_snapshot_hash")
                    and (_number(report.get("completed_trade_count")) or 0) > 0)
    return {
        "result_id": row["backtest_uid"], "kind": "HISTORICAL_BACKTEST",
        "strategy_key": row.get("strategy_id"),
        "reported_strategy_key": report.get("strategy_key"),
        "strategy_version": row.get("strategy_version"), "status": "COMPLETE" if complete else row.get("status"),
        "start_date": cohort["start_date"], "end_date": cohort["end_date"],
        "finished_at": row.get("finished_at"), "initial_cash_cny": initial,
        "net_return_pct": net_return, "net_profit_cny": pnl,
        "closed_trade_net_pnl_cny": _number(metrics.get("cumulative_net_pnl")) if integrity else None,
        "completed_trades": report.get("completed_trade_count"),
        "max_drawdown_pct": abs(_percent(performance.get("max_drawdown")))
            if integrity and _percent(performance.get("max_drawdown")) is not None else None,
        "win_rate_pct": _percent(metrics.get("win_rate")) if integrity else None,
        "win_rate_history": [],
        "integrity_verified": integrity, "identity_verified": identity,
        "rank_eligible": eligible, "cohort_id": canonical_json_hash(cohort) if eligible else None,
        "cohort": cohort, "reason": ("可按相同回测区间、数据、资金与费用口径比较" if eligible else
            "报告哈希无效，禁止使用收益" if not integrity else
            "历史报告缺少完整策略版本绑定，仅作存档，不能计入当前策略排名" if not identity else
            row.get("error_message") or "回测未完成或缺少可比较的资金、费用、收益证据"),
        "code_commit_sha": row.get("code_commit_sha"), "result_hash": row.get("result_hash"),
        "binding": binding, "funding_eligible": False,
    }


def _research(run, proposal, *, native=False):
    validation = proposal.get("validation") or {}
    report = validation.get("report") or {}
    nav = report.get("nav") or []
    trades = report.get("trades") or []
    initial = _number(report.get("initial_cash_cny"))
    net_return = _number(report.get("net_return_pct")) if validation.get("status") == "COMPLETE" else None
    subset_return = (_number(report.get("verified_input_subset_return_pct"))
                     if validation.get("status") == "PARTIAL" else None)
    return_scope = report.get("return_scope", "FULL_REQUESTED_INPUTS")
    cohort = {"start_date": (validation.get("sessions") or [None])[0],
        "end_date": (validation.get("sessions") or [None])[-1],
        "dataset_sha256": validation.get("source_input_sha256") or validation.get("dataset_sha256"), "initial_cash_cny": initial,
        "fees": report.get("fees"), "execution_mode": report.get("execution_mode"), "return_scope": return_scope}
    if native:
        cohort['native_execution_policy'] = report.get('execution_policy')
        if validation.get('evaluation_set_sha256'):
            cohort['evaluation_set_sha256']=validation['evaluation_set_sha256']
            cohort['allocation_policy']=report.get('allocation_policy')
    comparable = net_return if net_return is not None else subset_return
    eligible = bool(comparable is not None and initial and cohort["dataset_sha256"]
                    and cohort["fees"] and report.get("status") in {"COMPLETE", "PARTIAL"} and trades)
    return {"result_id": ("native:" if native else "research:") + run["run_id"] + ":" + proposal["rule_hash"],
        'experiment_id':run['run_id'],'experiment_kind':run.get('experiment_kind'),
        'source_run_id':run.get('source_run_id'),'experiment_status':run.get('status'),
        'phase':run.get('phase'),'current_unit':run.get('current_unit'),
        'last_progress_at':run.get('last_progress_at'),'data_cutoff_at':run.get('price_cutoff'),
        'decision_known_at':run.get('requested_at'),'algorithm_sha256':validation.get('algorithm_sha256'),
        'evaluation_sessions':run.get('sessions',[]),'execution_end_date':run.get('execution_end_date') or (run.get('sessions') or [None])[-1],
        'allocation_policy':run.get('allocation_policy','FIXED_SHADOW'),
        'source_family':(run.get('strategies') or [{}])[0].get('source_kind'),
        "kind": "NATIVE_SHADOW_RETROSPECTIVE" if native else "MODEL_RULE_RETROSPECTIVE", "strategy_key": proposal["strategy_key"],
        "strategy_version": proposal["version"], "strategy_name": proposal["name"],
        "status": validation.get("status", "NOT_STARTED"), **{k: cohort[k] for k in ("start_date", "end_date", "initial_cash_cny")},
        "net_return_pct": net_return, "net_profit_cny": initial * net_return / 100 if initial and net_return is not None else None,
        "verified_input_subset_return_pct": subset_return,
        "verified_input_subset_profit_cny": initial * subset_return / 100 if initial and subset_return is not None else None,
        "return_scope": return_scope, "identity_covered_sessions": validation.get("identity_covered_sessions"),
        "input_exclusion_count": report.get("input_exclusion_count"), "replay_run_id": validation.get("replay_run_id"),
        "reused_input_sessions": validation.get("reused_input_sessions", 0),
        "completed_trades": report.get("completed_trade_count"), "max_drawdown_pct": _drawdown(nav) if eligible else None,
        "closed_trade_net_pnl_cny": sum(_number(t["net_pnl_cny"]) for t in trades)
            if trades and all(_number(t.get("net_pnl_cny")) is not None for t in trades) else None,
        "win_rate_pct": sum(_number(t["net_pnl_cny"]) > 0 for t in trades) / len(trades) * 100
            if eligible and trades and all(_number(t.get("net_pnl_cny")) is not None for t in trades) else None,
        "estimated_fees_cny": sum(float(t["estimated_fee_cny"]) for t in trades)
            if eligible and trades and all(_number(t.get("estimated_fee_cny")) is not None for t in trades) else None,
        "rank_eligible": eligible, "cohort_id": canonical_json_hash(cohort) if eligible else None,
        "cohort": cohort, "integrity_verified": True, "identity_verified": True,
        "reason": validation.get("reason") or "等待历史回放验证",
        "completed_units": validation.get("completed_units"), "total_units": validation.get("total_units"),
        "available_feature_sessions": validation.get("available_feature_sessions"),
        'complete_feature_sessions':validation.get('complete_feature_sessions'),
        'data_requirements':validation.get('data_requirements'),
        "required_sessions": validation.get("required_sessions"),
        "missing_feature_dates": validation.get("missing_feature_dates") or [],
        'partial_feature_dates':validation.get('partial_feature_dates') or [],
        "out_of_sample_status": report.get("out_of_sample_status", "UNKNOWN"),
        "win_rate_history": _research_win_rate_history(trades, report, validation.get("sessions") or [])
            if validation.get("status") in {"COMPLETE", "PARTIAL"} else [],
        "funding_eligible": False}


def rank_results(records):
    """One latest result per exact version in each comparable experiment cohort."""
    groups = {}
    for row in records:
        if not row.get("rank_eligible") or not row.get("cohort_id"):
            continue
        key = row["kind"] + ":" + row["cohort_id"]
        group = groups.setdefault(key, {})
        group.setdefault((row["strategy_key"], row["strategy_version"]), row)
    result = []
    for key, versions in groups.items():
        def value(row):
            return row.get("verified_input_subset_return_pct") if row.get("return_scope") == "VERIFIED_INPUT_SUBSET" else row["net_return_pct"]
        ordered = sorted(versions.values(), key=lambda r: (-value(r), r["strategy_key"]))
        last, rank = None, 0
        for index, row in enumerate(ordered):
            if value(row) != last:
                rank, last = index + 1, value(row)
            row["rank"] = rank
        result.append({"cohort_id": key, "cohort": ordered[0]["cohort"], "rows": ordered})
    return result


def read_performance(engine, day, *, experiment_id=None):
    from server.api.quant_strategy_workspace import read_strategy_workspace
    workspace = read_strategy_workspace(engine, day)
    records = [_research(run, p) for run in workspace["autonomous_research"].get("runs", [])
               for p in run.get("proposals", [])]
    from server.engine.strategy_native_backtest import read_runs
    native_error = ''
    try:
        with engine.connect() as connection:
            native_runs = read_runs(connection)
            if experiment_id and not any(run['run_id']==experiment_id for run in native_runs):
                native_runs=read_runs(connection,run_id=experiment_id)+native_runs
    except (SQLAlchemyError,ValueError) as exc:
        native_runs = []
        native_error = '原生回测审计读取失败：'+type(exc).__name__
    if experiment_id and not native_error and not any(run['run_id']==experiment_id for run in native_runs):
        raise KeyError(experiment_id)
    records.extend(_research(run,p,native=True) for run in native_runs for p in run["proposals"])
    repository = TradingV2ReadRepository(engine)
    sources = {"autonomous_research": {"status": workspace["autonomous_research"].get("status", "COMPLETE"),
                                      "reason": workspace["autonomous_research"].get("reason", "")}}
    sources['native_backtests'] = {'status':'FAILED' if native_error else 'COMPLETE','reason':native_error}
    try:
        formal = [_formal(row) for row in repository.backtests(100)]
        sources["historical_backtests"] = {"status": "COMPLETE", "reason": ""}
    except (SQLAlchemyError, ValueError) as exc:
        formal = []
        sources["historical_backtests"] = {"status": "FAILED", "reason": "历史回测读取失败：" + type(exc).__name__}
    records.extend(formal)
    if experiment_id is None:
        preferred=next((run for run in native_runs if run.get('status')=='RUNNING'),None)
        preferred=preferred or next((run for run in native_runs if run.get('status') in ('COMPLETE','BLOCKED','FAILED')),None)
        if preferred:experiment_id=preferred['run_id']
    strategies = []
    for row in workspace["strategies"]:
        matching = [r for r in records if r["strategy_key"] == row["strategy_key"]
                    and r["strategy_version"] == row["current_version"]]
        pinned=next((result for result in matching if result.get('experiment_id')==experiment_id),None)
        latest = pinned or (matching[0] if matching else None)
        valid=next((result for result in matching if result.get('completed_trades') and result.get('win_rate_pct') is not None),None)
        strategies.append({"strategy_key": row["strategy_key"], "strategy_name": row["strategy_name"],
            "strategy_version": row["current_version"], "result_id": latest["result_id"] if latest else None,
            "backtest_status": latest["status"] if latest else "FAILED" if native_error else "NOT_STARTED",
            'last_valid_result_id':valid['result_id'] if valid else None,
            "reason": latest["reason"] if latest else native_error or "当前精确版本尚无已保存回测；不可复用旧版本或其他策略的收益",
            "forward_windows": row["windows"]})
    current_ids = {s["result_id"] for s in strategies}
    try:
        _attach_formal_win_rate_history(engine, [r for r in records if r["result_id"] in current_ids])
        sources["win_rate_history"] = {"status": "COMPLETE", "reason": ""}
    except (SQLAlchemyError, ValueError) as exc:
        sources["win_rate_history"] = {"status": "FAILED", "reason": "胜率曲线读取失败：" + type(exc).__name__}
    return {"trade_date": workspace["trade_date"], "strategies": strategies,'experiment_id':experiment_id,
        'experiments':[{key:run.get(key) for key in ('run_id','source_run_id','requested_at','phase','status',
            'completed_units','total_units','last_progress_at','reason','experiment_kind','allocation_policy','job_id')} for run in native_runs],
        "results": records, "leaderboards": rank_results(records), "sources": sources,
        "history_limit": 100, "history_limit_reached": len(formal) == 100,
        "method": "历史回测按同区间、同数据、同初始资金及费用执行口径分组。可用输入子集单列研究排名，缺少历史身份的拟入场被排除，其收益不能代表完整策略表现。模型规则回放属于事后研究，未经样本外验证不构成盈利结论。",
        "real_order_authority": False}


def read_performance_version(engine, *, experiment_id=None):
    """Only indexed journal heads and small queue receipts; no result bodies."""
    headers=[]
    from server.common.strategy_audit_heads import latest_head, recent_heads
    deadline=time.monotonic()+4
    with engine.connect() as connection:
        for entity in ('NATIVE_REPLAY','AUTO_RESEARCH'):
            rows=recent_heads(connection,entity,12,deadline=deadline)
            for row in rows:
                headers.append((entity,row['entity_key'],str(row['latest_at']),row['action']))
        if experiment_id and not any(row[1]==experiment_id for row in headers):
            row=latest_head(connection,'NATIVE_REPLAY',experiment_id,deadline=deadline)
            if row:headers.append(('NATIVE_REPLAY',experiment_id,str(row['latest_at']),row['action']))
        jobs=[dict(row) for row in connection.execute(text('''SELECT /*+ MAX_EXECUTION_TIME(2000) */
            job_id,status,result_ref,finished_at FROM st_job_v2 ORDER BY requested_at DESC,job_id DESC LIMIT 24''')).mappings()]
    return {'version':canonical_json_hash({'heads':headers,'jobs':jobs}),'experiment_id':experiment_id,'refresh_seconds':20}


def read_performance_detail(engine, result_id, *, offset=0, limit=50):
    if result_id.startswith(("research:","native:")):
        from server.engine.autonomous_strategy_research import _read_states
        from server.engine.strategy_native_backtest import read_runs
        _, run_id, rule_hash = result_id.split(":")
        with engine.connect() as connection:
            runs = read_runs(connection,run_id=run_id) if result_id.startswith("native:") else _read_states(connection, run_id=run_id, limit=1)
        proposal = next((p for r in runs for p in r.get("proposals", []) if p["rule_hash"] == rule_hash), None)
        if proposal is None:
            raise KeyError(result_id)
        row, report = _research(runs[0], proposal,native=result_id.startswith("native:")), (proposal.get("validation") or {}).get("report") or {}
        trades = report.get("trades") or []
        return {"summary": row, "trades": trades[offset:offset + limit], "trade_count": len(trades),
                "offset": offset, "limit": limit, "nav": report.get("nav") or [],
                "dispositions": (report.get("dispositions") or [])[:100], "fees": report.get("fees"),
                "disposition_count": len(report.get("dispositions") or []),
                "source_gaps": (proposal.get("validation") or {}).get("source_gaps") or [],
                "missing_feature_dates": (proposal.get("validation") or {}).get("missing_feature_dates") or [],
                "partial_feature_dates": (proposal.get("validation") or {}).get("partial_feature_dates") or []}
    repository = TradingV2ReadRepository(engine)
    raw = repository.backtest(result_id)
    if not raw:
        raise KeyError(result_id)
    summary = _formal(raw)
    if not summary["integrity_verified"]:
        return {"summary": summary, "trades": [], "trade_count": None, "offset": offset, "limit": limit, "nav": []}
    with engine.connect() as connection:
        count = connection.execute(text("SELECT COUNT(*) FROM st_backtest_trade_v2 WHERE backtest_uid=:uid"), {"uid": result_id}).scalar()
        trades = [dict(row) for row in connection.execute(text("""
            SELECT /*+ MAX_EXECUTION_TIME(4000) */ trade_id,stock_code,entry_date,exit_date,quantity,
                   buy_fill_amount,sell_fill_amount,buy_fees,sell_fees,trade_net_pnl
            FROM st_backtest_trade_v2 WHERE backtest_uid=:uid
            ORDER BY entry_date,trade_id LIMIT :limit OFFSET :offset
        """), {"uid": result_id, "limit": limit, "offset": offset}).mappings()]
    return {"summary": summary, "trades": trades, "trade_count": count, "offset": offset, "limit": limit,
            "nav": (raw.get("result") or {}).get("nav") or [],
            "fees": (raw.get("result") or {}).get("execution_assumptions"),
            "trade_coverage_matches": count == summary["completed_trades"]}
