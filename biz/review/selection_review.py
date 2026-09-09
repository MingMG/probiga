"""Post-close review of the plans frozen before their execution session.

Selection diagnostics never create trades or promote a strategy. Lifecycle
decisions remain owned by the version-bound governance/forward-evidence gates.
"""
from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta
from typing import Any, Mapping

from sqlalchemy import text

from server.common.daily_delivery_control import canonical_sha256

SCHEMA = "probiga.daily-selection-review.v1"


def _json(value):
    if isinstance(value, Mapping):
        return dict(value)
    return json.loads(value or "{}")


def _number(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _return(close, entry):
    close, entry = _number(close), _number(entry)
    return round((close / entry - 1) * 100, 4) if close and entry and entry > 0 else None


def build_selection_review(*, execution_date: str, plans: list[dict],
                           intents: list[dict], fills: list[dict], bars: list[dict],
                           forward: list[dict], health: list[dict],
                           frozen_universe: list[dict], source_roots: dict,
                           issues: list[str]) -> dict[str, Any]:
    """Pure review: same-day marks and mature version evidence stay separate."""
    target = date.fromisoformat(execution_date)
    issues = list(issues)
    end = target + timedelta(days=1)
    prices = {str(row["stock_code"]): row for row in bars}
    fill_by_intent = defaultdict(list)
    for fill in fills:
        if str(fill.get("side") or "").upper() == "BUY":
            fill_by_intent[str(fill["intent_id"])].append(fill)
    intents_by_plan = defaultdict(list)
    for intent in intents:
        evidence = _json(intent.get("evidence_json"))
        authorization = evidence.get("dynamic_shadow_bootstrap") or {}
        identity = authorization.get("plan_id")
        if not identity:
            identity = "|".join((str(evidence.get("primary_strategy_key") or ""),
                                 str(evidence.get("primary_strategy_version") or intent.get("strategy_version") or ""),
                                 str(intent["stock_code"])))
        intents_by_plan[str(identity)].append(intent)
    rows, grouped, attributed_intents = [], defaultdict(list), set()
    for plan in plans:
        code = str(plan["stock_code"])
        key, version = str(plan["strategy_key"]), str(plan["strategy_version"])
        identity = str(plan.get("plan_id") or "|".join((key, version, code)))
        matches = []
        for intent in intents_by_plan.get(identity, []):
            source = _json(intent.get("evidence_json"))
            authorization = source.get("dynamic_shadow_bootstrap") or {}
            governance = source.get("strategy_governance") or {}
            if authorization:
                bound = (authorization.get("plan_hash") == plan.get("plan_hash")
                    and authorization.get("strategy_key") == key
                    and authorization.get("strategy_version") == version
                    and authorization.get("candidate_run_uid") == plan.get("source_run_uid")
                    and intent.get("decision_run_uid") == plan.get("source_run_uid")
                    and intent.get("account_id") == plan.get("account_id"))
            else:
                bound = (governance.get("governance_run_uid") == plan.get("source_run_uid")
                    and governance.get("target_hash") == plan.get("plan_hash")
                    and governance.get("strategy_key") == key
                    and governance.get("strategy_version") == version)
            if bound:
                matches.append(intent)
                attributed_intents.add(str(intent["intent_id"]))
        actual_fills = {str(fill["fill_id"]): fill for intent in matches
                        for fill in fill_by_intent.get(str(intent["intent_id"]), [])}
        quantity = sum(float(row["quantity"]) for row in actual_fills.values())
        gross = sum(float(row["gross_amount"]) for row in actual_fills.values())
        fees = sum(float(row["fee_amount"]) for row in actual_fills.values())
        bar = prices.get(code, {})
        entry = gross / quantity if quantity > 0 else None
        item = {
            "plan_id": identity, "source_run_uid": plan.get("source_run_uid"),
            "plan_hash": plan.get("plan_hash"), "stock_code": code,
            "strategy_key": key, "strategy_version": version,
            "mode": plan.get("mode", "PAPER"),
            "execution_status": "FILLED" if quantity else ("UNFILLED" if matches else "NOT_MATERIALIZED"),
            "intent_ids": [str(row["intent_id"]) for row in matches],
            "fill_ids": sorted(actual_fills), "entry_quantity": quantity,
            "account_ids": sorted({str(row.get("account_id") or "") for row in matches}),
            "average_entry_price": entry, "entry_fee_cny": round(fees, 4),
            "close": _number(bar.get("close")),
            "entry_to_close_mark_pct": _return(bar.get("close"), entry),
            "open_to_close_proxy_pct": _return(bar.get("close"), bar.get("open")),
            "mark_semantics": "ENTRY_TO_CLOSE_GROSS_MARK_NOT_REALIZED_RETURN",
            "waiting_reasons": sorted({str(row.get("waiting_reason") or row.get("order_status") or "NO_ORDER") for row in matches}),
        }
        rows.append(item)
        grouped[(key, version)].append(item)
    health_by_version = {}
    for row in health:
        health_by_version.setdefault((str(row["strategy_key"]), str(row["strategy_version"])), row)
    strategies = []
    for (key, version), picks in sorted(grouped.items()):
        evidence = [row for row in forward if str(row.get("strategy_key")) == key
                    and str(row.get("strategy_version")) == version
                    and row.get("evidence_status") == "MATURED"
                    and row.get("sample_owner_role") == "PRIMARY"
                    and row.get("attribution_status") == "VERIFIED_SNAPSHOT"
                    and str(row.get("exit_at") or "")[:10] <= target.isoformat()]
        windows = {}
        for days in (20, 60, 120):
            start = (end - timedelta(days=days)).isoformat()
            returns = [_number(row.get("realized_net_return_pct")) for row in evidence
                       if str(row.get("exit_at") or "")[:10] >= start]
            returns = [value for value in returns if value is not None]
            wins, losses = sum(max(value, 0) for value in returns), -sum(min(value, 0) for value in returns)
            windows[str(days)] = {"matured_count": len(returns),
                "net_expectancy_pct": round(sum(returns) / len(returns), 4) if returns else None,
                "win_rate_pct": round(sum(value > 0 for value in returns) / len(returns) * 100, 2) if returns else None,
                "return_profit_factor": round(wins / losses, 4) if losses else None}
        latest = health_by_version.get((key, version), {})
        marks = [row["entry_to_close_mark_pct"] for row in picks if row["entry_to_close_mark_pct"] is not None]
        filled_count = sum(row["execution_status"] == "FILLED" for row in picks)
        short = windows["20"]
        action = "CONTINUE_OBSERVATION"
        if short["matured_count"] >= 20 and short["net_expectancy_pct"] <= 0:
            action = "REVIEW_WEAK_STRATEGY"
        if str(latest.get("recommended_status") or "") in {"SUSPENDED", "REDUCE"}:
            action = "FOLLOW_GOVERNANCE_RISK_REDUCTION"
        strategies.append({"strategy_key": key, "strategy_version": version,
            "selected_count": len(picks), "filled_count": filled_count,
            "mark_coverage_count": len(marks),
            "average_entry_to_close_mark_pct": round(sum(marks) / len(marks), 4) if marks and len(marks) == filled_count else None,
            "windows_calendar_days": windows, "review_action": action,
            "governance_recommended_status": latest.get("recommended_status"),
            "governance_reason": latest.get("gate_reason"),
            "governance_evidence_hash": latest.get("result_hash")})
    selected = {row["stock_code"] for row in rows}
    missed = []
    for stock in frozen_universe:
        code = str(stock["stock_code"])
        if code in selected:
            continue
        bar = prices.get(code, {})
        change = _return(bar.get("close"), bar.get("open"))
        if change is not None:
            missed.append({"stock_code": code, "open_to_close_proxy_pct": change,
                "data_quality_flags": stock.get("data_quality_flags"),
                "finance_data_exclusion": stock.get("finance_data_exclusion"),
                "reason": "FROZEN_UNIVERSE_NOT_IN_BUY_PLAN",
                "executable_opportunity_proven": False})
    missed.sort(key=lambda row: (-row["open_to_close_proxy_pct"], row["stock_code"]))
    missing_marks = sorted({row["stock_code"] for row in rows
                            if row["execution_status"] == "FILLED" and row["entry_to_close_mark_pct"] is None})
    if missing_marks:
        issues.append("SELECTION_REVIEW_CLOSE_MISSING:" + ",".join(missing_marks))
    unbound = sorted({str(row["intent_id"]) for row in intents} - attributed_intents)
    if unbound:
        issues.append("存在未能绑定开盘前冻结计划的交易意图，已单列，未计入事前选股表现。")
    status = "DEGRADED" if issues else ("READY" if source_roots else "NO_FROZEN_PLAN")
    counts = Counter(row["execution_status"] for row in rows)
    findings = list(issues)
    if not rows:
        findings.append("当日没有可核验的事前买入计划，不能据此评价选股收益。")
    if counts["NOT_MATERIALIZED"]:
        findings.append("部分事前计划没有生成交易意图，应检查授权、容量和执行链路。")
    if counts["UNFILLED"]:
        findings.append("部分意图未成交，应检查价格条件、停牌、涨跌停和订单原因。")
    if any(row["review_action"] == "REVIEW_WEAK_STRATEGY" for row in strategies):
        findings.append("同版本已完成样本出现持续负期望，需复查成本、入场、止损和市场适配；由既有治理门槛决定降权或暂停。")
    core = {"schema": SCHEMA, "execution_date": execution_date, "status": status,
        "selected_count": len(rows), "unique_stock_count": len(selected), "execution_counts": dict(counts),
        "source_roots": source_roots, "stocks": rows, "strategies": strategies,
        "unattributed_intent_ids": unbound, "missing_mark_stock_codes": missing_marks,
        "frozen_universe_count": len(frozen_universe),
        "unselected_measured_count": len(missed), "missed_move_diagnostics": missed[:20],
        "missed_semantics": "事前覆盖范围内未选股的开收盘变化，仅作漏选诊断，不是可成交利润。",
        "findings": findings, "strategy_changes_applied_by_review": False,
        "real_order_authority": False}
    return {**core, "review_sha256": canonical_sha256(core)}


def generate_selection_review(engine, execution_date: str, *, bars: list[dict], now: datetime) -> dict:
    from server.common.analysis_pool_receipt import decode_score_snapshot
    from server.common.daily_delivery_control import DailyDeliveryControlError, load_published_analysis_receipt
    from server.engine.dynamic_shadow_ledger import verify_dynamic_shadow_trial_plan
    from server.engine.strategy_governance import _canonical_governance_result_from_row

    target = date.fromisoformat(execution_date)
    start, end = datetime.combine(target, time.min), datetime.combine(target + timedelta(days=1), time.min)
    opened = datetime.combine(target, time(9, 30))
    if now < datetime.combine(target, time(15, 0)):
        raise RuntimeError("SELECTION_REVIEW_SESSION_NOT_CLOSED")
    params = {"start": start, "end": end, "opened": opened, "d": execution_date, "observed": now}
    plans, roots, issues, universe = [], {}, [], []
    with engine.connect() as connection:
        prior = connection.execute(text("SELECT MAX(trade_date) FROM si_trade_calendar WHERE trade_date<:d AND trade_status=1"), params).scalar()
        if prior is None:
            raise RuntimeError("SELECTION_REVIEW_PREVIOUS_SESSION_UNAVAILABLE")
        params["prior"] = str(prior)[:10]
        governance = connection.execute(text("""
            SELECT run_uid, trade_date, input_hash, decision_hash, result_json, result_hash
            FROM st_strategy_governance_run WHERE trade_date=:prior AND status='COMPLETED'
              AND finished_at<=:opened ORDER BY finished_at DESC, run_revision DESC LIMIT 1
        """), params).mappings().first()
        if governance:
            result = _canonical_governance_result_from_row(dict(governance))
            roots["governance"] = result["canonical_result_hash"]
            for row in (result.get("paper_execution_plan") or {}).get("targets") or []:
                if float(row.get("new_buy_delta_bp") or 0) > 0:
                    plans.append({**row, "plan_hash": row["target_hash"], "source_run_uid": result["run_uid"], "mode": "PAPER"})
        trial_rows = connection.execute(text("""
            SELECT plan_id FROM st_dynamic_shadow_trial_plan
            WHERE trade_date=:prior AND created_at<=:opened ORDER BY plan_id
        """), params).mappings().all()
        for row in trial_rows:
            plan = verify_dynamic_shadow_trial_plan(connection, str(row["plan_id"]))
            roots[str(row["plan_id"])] = plan["plan_hash"]
            plans.append({**plan, "source_run_uid": plan["candidate_run_uid"], "mode": "SHADOW"})
        intents = [dict(row) for row in connection.execute(text("""
            SELECT i.*, o.status AS order_status, o.waiting_reason
            FROM st_trade_intent_v2 i LEFT JOIN st_order_v2 o ON o.intent_id=i.intent_id
            WHERE i.action='BUY' AND i.earliest_at>=:start AND i.earliest_at<:end
              AND i.created_at<:end AND i.created_at<=:observed ORDER BY i.created_at, i.intent_id, o.order_id
        """), params).mappings()]
        fills = [dict(row) for row in connection.execute(text("""
            SELECT f.*, o.intent_id FROM st_fill_v2 f JOIN st_order_v2 o ON o.order_id=f.order_id
            WHERE f.filled_at>=:start AND f.filled_at<:end AND f.filled_at<=:observed
            ORDER BY f.filled_at, f.fill_id
        """), params).mappings()]
        forward = [dict(row) for row in connection.execute(text("""
            SELECT evidence_id, strategy_key, strategy_version, sample_owner_role, attribution_status,
                   evidence_status, exit_at, realized_net_return_pct
            FROM st_forward_trade_evidence_v3 WHERE evidence_status='MATURED'
              AND exit_at>=:window_start AND exit_at<:end
              AND updated_at<=:observed ORDER BY evidence_id
        """), {**params, "window_start": start - timedelta(days=120)}).mappings()]
        health = [dict(row) for row in connection.execute(text("""
            SELECT run_uid, strategy_key, strategy_version, trade_date, recommended_status,
                   gate_reason, result_hash FROM st_strategy_health_snapshot
            WHERE trade_date=:d AND created_at<=:observed ORDER BY created_at DESC, run_uid DESC
        """), params).mappings()]
    try:
        receipt = load_published_analysis_receipt(engine, params["prior"], decision_at=opened)
        universe = decode_score_snapshot(receipt["score_snapshot"], trade_date=params["prior"])["scored_rows"]
        roots["score_snapshot"] = receipt["score_snapshot"]["payload_sha256"]
    except DailyDeliveryControlError as exc:
        issues.append(str(exc))
    if not governance:
        issues.append("SELECTION_REVIEW_GOVERNANCE_PLAN_UNAVAILABLE")
    roots.update({"observed_at": now.isoformat(), "intents": canonical_sha256(intents),
        "fills": canonical_sha256(fills), "forward": canonical_sha256(forward),
        "health": canonical_sha256(health), "bars": canonical_sha256(bars)})
    return build_selection_review(execution_date=execution_date, plans=plans, intents=intents,
        fills=fills, bars=bars, forward=forward, health=health, frozen_universe=universe,
        source_roots=roots, issues=issues)


def render_selection_review(review: Mapping[str, Any]) -> str:
    counts = review.get("execution_counts") or {}
    lines = [f"当日选股与策略复盘｜{review.get('status')}：事前计划{review.get('selected_count', 0)}项，"
             f"涉及{review.get('unique_stock_count', 0)}只股票，已成交{counts.get('FILLED', 0)}项，"
             f"未成交{counts.get('UNFILLED', 0)}项，未生成意图{counts.get('NOT_MATERIALIZED', 0)}项。"]
    for strategy in review.get("strategies") or []:
        mark = strategy["average_entry_to_close_mark_pct"]
        result = f"{mark:+.2f}%" if mark is not None else "暂无成交估值"
        lines.append(f"{strategy['strategy_key']} / {strategy['strategy_version']}："
                     f"计划{strategy['selected_count']}项，成交至收盘毛收益标记{result}；"
                     f"同版本60个自然日已完成样本{strategy['windows_calendar_days']['60']['matured_count']}笔。")
    lines.extend(review.get("findings") or [])
    lines.append("当日价格标记与未选股涨幅仅用于诊断；策略升降级使用同版本、扣费后的完成交易和既有治理规则。")
    return "\n".join(lines)
