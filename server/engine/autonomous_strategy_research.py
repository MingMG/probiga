"""Checkpointed strategy hypothesis research, owned by the existing Linux Worker.

Model output is a declarative selection rule. It cannot install Python, change
funding gates, modify a champion, or issue orders. The existing audit ledger is
the durable research journal; the scheduler's renewable lease owns each step.
"""
from __future__ import annotations

from datetime import datetime, timedelta, time as wall_time
import json
import os
import socket
import time
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import text

from server.common.authoritative_market_clock import PRODUCTION_TIMEZONE, authoritative_closed_trade_date
from server.common.analysis_pool_receipt import canonical_sha256
from server.common.daily_delivery_control import load_published_analysis_receipt
from server.engine.strategy_rule_language import (
    RULE_SCHEMA, FIELDS, STOCK_FIELDS, MARKET_FIELDS, COMPARISONS,
    feature_frame, feature_catalog, select_rule_rows, validate_rule,
)


SCHEMA = "probiga.autonomous-strategy-research.v1"
POLICY = {"schema": SCHEMA, "maximum_proposals": 3, "historical_sessions": 120,
          "minimum_training_sessions": 60, "minimum_validation_sessions": 20,
          "maximum_step_seconds": 240, "generation_seconds": 150,
          "maximum_daily_position_count": 5, "automatic_formula_installation": False,
          "automatic_champion_replacement": False, "automatic_real_order_submission": False}
ENTITY = "AUTO_RESEARCH"
LOCK = "probiga:autonomous-strategy-research"
OPERATOR = "autonomous-strategy-research"


def now_shanghai():
    return datetime.now(PRODUCTION_TIMEZONE).replace(tzinfo=None, microsecond=0)


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)


def _read_states(connection, limit=12, run_id=None):
    from server.engine.strategy_governance import _digest
    records = connection.execute(text("""
        SELECT /*+ MAX_EXECUTION_TIME(4000) */ a.payload_json,a.audit_hash,a.after_json
        FROM st_strategy_governance_audit a JOIN (
            SELECT entity_key,MAX(action) AS last_action FROM st_strategy_governance_audit
            WHERE entity_type=:entity GROUP BY entity_key
        ) last ON last.entity_key=a.entity_key AND last.last_action=a.action
        WHERE a.entity_type=:entity AND (:run_id IS NULL OR a.entity_key=:run_id)
        ORDER BY a.created_at DESC,a.audit_id DESC LIMIT :limit
    """), {"entity": ENTITY, "limit": limit, "run_id": run_id}).mappings().all()
    states = []
    for record in records:
        payload = json.loads(str(record["payload_json"]))
        after = json.loads(str(record["after_json"]))
        if _digest(payload) != record["audit_hash"] or payload.get("after") != after or after.get("schema") != SCHEMA:
            raise ValueError("自主研究审计哈希或运行状态不一致")
        states.append(after)
    if len({s["run_id"] for s in states}) != len(states):
        raise ValueError("自主研究检查点序号重复")
    return states


def _owner(connection, *, locking=False):
    from server.common.scheduler_task_lease import read as read_lease
    from server.common.market_dashboard_worker import process_identity
    task_id = str(os.environ.get("PROBIGA_SCHEDULER_TASK_ID") or "")
    if not task_id.isdecimal() or os.environ.get("PROBIGA_SCHEDULER_TASK_TYPE") != "trading_v2_job_worker":
        raise ValueError("自主研究必须由现有Linux研究Worker及其租约执行")
    owners = connection.execute(text("""
        SELECT * FROM st_scheduled_task_history
        WHERE task_id=:task_id AND task_type='trading_v2_job_worker'
          AND status='running' AND host_name=:host ORDER BY id DESC LIMIT 2
    """), {"task_id": int(task_id), "host": socket.gethostname()}).mappings().all()
    if len(owners) != 1:
        raise ValueError("自主研究缺少唯一有效的调度运行身份")
    row = dict(owners[0])
    if locking:
        # Match lease renewal's unique-key lock; a reverse history scan also
        # locks unrelated scheduler rows and can deadlock their heartbeats.
        locked = connection.execute(text("SELECT * FROM st_scheduled_task_history WHERE run_uid=:uid" +
            (" FOR UPDATE" if connection.dialect.name == "mysql" else "")), {'uid':row['run_uid']}).mappings().one_or_none()
        if (locked is None or locked['status']!='running' or locked.get('finished_at') is not None
                or int(locked['task_id'])!=int(task_id) or locked['host_name']!=socket.gethostname()
                or locked['task_type']!='trading_v2_job_worker'):
            raise ValueError("自主研究调度运行身份在加锁前已失效")
        row = dict(locked)
    lease = read_lease(row)
    current = now_shanghai()
    if lease is None or not lease["renewed_at"] <= current <= lease["lease_until"]:
        raise ValueError("自主研究调度租约不可用")
    return {"scheduler_run_uid": row["run_uid"], "host": socket.gethostname(), "pid": os.getpid(),
            "pid_birth": process_identity(os.getpid())}


def _checkpoint(connection, state, owner, *, partition_events=()):
    from server.engine.strategy_governance import _append_audit_connection
    connection.commit()
    live = _owner(connection, locking=True)
    if live != owner or connection.execute(text("SELECT IS_USED_LOCK(:name)=CONNECTION_ID()"), {"name": LOCK}).scalar() != 1:
        raise ValueError("自主研究运行所有权已丢失，禁止发布")
    sequence = int(state.get("checkpoint_sequence") or 0) + 1
    state.update(updated_at=now_shanghai().isoformat(sep=" "), owner=owner, checkpoint_sequence=sequence)
    for event in partition_events:
        _append_audit_connection(connection, **event, reason="研究回放分区已校验并完成", operator=OPERATOR,
                                 before=None, evidence={"research_run_id": state["run_id"], "owner": owner})
    _append_audit_connection(connection, entity_type=ENTITY, entity_key=state["run_id"],
        action=f"RESEARCH_CHECKPOINT_{sequence:08d}", reason=str(state.get("reason") or state["stage"])[:500],
        operator=OPERATOR, before=None, after=state,
        evidence={"analysis_run_uid": state["analysis_run_uid"], "analysis_payload_hash": state["analysis_payload_hash"],
                  "owner_scheduler_run_uid": owner["scheduler_run_uid"], "real_order_authority": False})
    connection.commit()


def _resume_allowed(connection, state, owner):
    old = state.get("owner") or {}
    if state.get("step_status") != "RUNNING" or old == owner:
        return True
    row = connection.execute(text("SELECT status FROM st_scheduled_task_history WHERE run_uid=:uid"),
                             {"uid": old.get("scheduler_run_uid")}).mappings().first()
    if row is None or row["status"] == "running" or old.get("host") != socket.gethostname():
        return False
    if os.name != "posix":
        return False
    from server.common.market_dashboard_worker import process_identity
    birth = process_identity(int(old["pid"]))
    if birth is not None and old.get("pid_birth") is not None and birth != old["pid_birth"]:
        return True
    try:
        os.kill(int(old["pid"]), 0)
    except ProcessLookupError:
        return True
    except (PermissionError, ValueError, KeyError):
        return False
    return False


def _external_context(connection, cutoff):
    from biz.market_context.external_market import _validated_market_time
    from server.engine.strategy_rule_language import finite
    rows = connection.execute(text("""
        SELECT /*+ MAX_EXECUTION_TIME(3000) */ snapshot_id,captured_at,symbol,
               display_name,price,change_pct,market_time,availability,source
        FROM st_external_market_context
        WHERE snapshot_id=(SELECT snapshot_id FROM st_external_market_context WHERE captured_at<=:cutoff
                           ORDER BY captured_at DESC,id DESC LIMIT 1)
        ORDER BY symbol LIMIT 42
    """), {"cutoff": cutoff}).mappings().all()
    if len(rows) > 41 or len({row["symbol"] for row in rows}) != len(rows):
        raise ValueError("自主研究外围批次身份或数量无效")
    result = []
    for raw in rows:
        row = dict(raw)
        try:
            captured = datetime.fromisoformat(str(row["captured_at"]))
            observed = _validated_market_time(row["market_time"], captured_at=captured)
            price = finite(row["price"])
            valid = row["availability"] == "available" and price is not None and price > 0 and observed <= min(captured, datetime.fromisoformat(cutoff))
        except (ValueError, TypeError):
            valid = False
        if not valid:
            row.update(availability="missing", price=None, change_pct=None)
        result.append(row)
    return result


def _performance_feedback(connection, state):
    rows = connection.execute(text("""
        SELECT /*+ MAX_EXECUTION_TIME(4000) */ h.strategy_key,h.strategy_version,h.trade_date,h.window_days,
            h.completed_trades,h.coverage_days,h.net_expectancy_pct,h.max_drawdown_pct,h.profit_gate_passed,h.gate_reason
        FROM st_strategy_health_snapshot h JOIN st_strategy_registry r
          ON r.strategy_key=h.strategy_key AND r.current_version=h.strategy_version
        JOIN st_strategy_governance_run g ON g.run_uid=h.run_uid
        WHERE r.category='自主研究' AND g.is_canonical=1 AND g.status='COMPLETED'
          AND g.finished_at<=:cutoff AND h.trade_date<=:day
        ORDER BY h.trade_date DESC,h.strategy_key,h.window_days LIMIT 90
    """), {"cutoff": state["decision_known_at"], "day": state["trade_date"]}).mappings().all()
    result, seen = [], set()
    for raw in rows:
        row = dict(raw)
        identity = (row["strategy_key"], row["strategy_version"], row["window_days"])
        if identity in seen:
            continue
        seen.add(identity)
        if not row["completed_trades"]:
            row.update(net_expectancy_pct=None, max_drawdown_pct=None)
        result.append(row)
    return result


def _source(connection, state):
    snapshots = []
    receipt = load_published_analysis_receipt(connection, state["trade_date"],
        decision_at=datetime.fromisoformat(state["decision_known_at"]),
        run_uid=state["analysis_run_uid"], capture_score=snapshots.append)
    if receipt["score_snapshot"]["payload_sha256"] != state["analysis_payload_hash"]:
        raise ValueError("自主研究冻结评分输入已改变")
    snapshot = snapshots[0]
    rows, market = feature_frame(snapshot["scored_rows"], state["trade_date"])
    if len(rows) < int(snapshot["stock_count"]) * 0.98:
        raise ValueError("自主研究价格样本覆盖不足，不能当作全市场状态")
    return rows, market, snapshot


def research_input_identity(rows, market, trade_date):
    policy = {key: value for key, value in POLICY.items()
              if key not in {"maximum_step_seconds", "generation_seconds"}}
    return canonical_sha256({"policy": policy, "trade_date": trade_date,
                             "rows": sorted(rows, key=lambda row: row["stock_code"]), "market": market})


def proposal_schema():
    condition = {"type": "object", "additionalProperties": False,
                 "properties": {"field": {"type": "string", "enum": sorted(FIELDS)},
                                "op": {"type": "string", "enum": sorted(COMPARISONS)}, "value": {"type": "number"}},
                 "required": ["field", "op", "value"]}
    ranking = {"type": "object", "additionalProperties": False,
               "properties": {"field": {"type": "string", "enum": list(STOCK_FIELDS)},
                              "ascending": {"type": "boolean"}, "weight": {"type": "number", "exclusiveMinimum": 0, "maximum": 10}},
               "required": ["field", "ascending", "weight"]}
    fields = {"schema": {"type": "string", "enum": [RULE_SCHEMA]},
              **{key: {"type": "array", "items": condition, "maxItems": 12} for key in ("market_all", "entry_all", "entry_any")},
              "ranking": {"type": "array", "items": ranking, "minItems": 1, "maxItems": 6},
              "top_n": {"type": "integer", "minimum": 1, "maximum": 5},
              "holding_days": {"type": "integer", "minimum": 1, "maximum": 20},
              "stop_loss_pct": {"type": "number", "minimum": 1, "maximum": 8}}
    proposal = {"type": "object", "additionalProperties": False,
                "properties": {**{key: {"type": "string", "minLength": 1, "maxLength": size}
                                   for key, size in (("name", 60), ("thesis", 600), ("applicability", 400), ("invalidations", 400))},
                               "rule": {"type": "object", "additionalProperties": False, "properties": fields, "required": list(fields)}},
                "required": ["name", "thesis", "applicability", "invalidations", "rule"]}
    return {"type": "object", "additionalProperties": False,
            "properties": {"summary": {"type": "string", "maxLength": 1000},
                           "proposals": {"type": "array", "items": proposal, "maxItems": POLICY["maximum_proposals"]}},
            "required": ["summary", "proposals"]}


def generate_proposals(packet, *, engine, request_id):
    from biz.review.market_decision import _call_codex
    schema = proposal_schema()
    messages = [{"role": "system", "content": (
        "你是A股策略研究员。根据有时间戳的市场、外围观察和既有研究反馈提出可检验的新交易假设。"
        "优先检查分歧切核心、弱势资金集中、趋势扩散、退潮等待之间的适用差异；这些是研究方向，不是必须凑数的模板。"
        "只能组合给定特征、比较条件、横截面排序和固定持有期/止损，不能输出Python、SQL、股票代码或下单指令。"
        "market_all只允许市场特征，entry_all/entry_any和ranking只允许个股特征；all为且，any为空或至少一项满足。"
        "缺值不是零。只能使用feature_catalog中覆盖充分的字段，避免把未证实资金身份当事实。"
        "分歧后的承接须有价格、量能和同板块相对强度依据；不能只凭过去涨幅命名核心。"
        "sector特征严格指冻结行业，不能冒充概念题材；日线不能证明盘中承接、席位资金身份或龙虎榜确认。"
        "任何未来结果只可用于已成熟的历史研究反馈；不承诺收益，不把回放当事前预测。"
        "策略须包含适用环境和失效说明；当前没有买点仍可提出尚未触发的研究假设；没有新假设可以返回空数组。禁止为增加数量复制已有规则。"
        "入场为决策之后的权威交易日，退出由持有期与止损执行；这些规则不取得资金资格。"
    )}, {"role": "user", "content": _json({"material": {"input": {"task": {"kind": "strategy_research"},
        "research": packet}}, "response_schema": schema})}]
    reply = _call_codex(engine, messages, schema, timeout_seconds=POLICY["generation_seconds"],
                       stage="autonomous_strategy_proposal", request_id=request_id,
                       rules_sha256=canonical_sha256(POLICY))
    result = json.loads(reply.raw_response)
    from biz.review.research_contract import _check_schema
    _check_schema(result, schema)
    seen = set()
    proposals = []
    for proposed in result["proposals"]:
        rule = validate_rule(proposed["rule"], packet["feature_catalog"])
        identity = canonical_sha256(rule)
        if identity in seen:
            raise ValueError("模型提出重复研究规则")
        seen.add(identity)
        proposals.append({**proposed, "rule_hash": identity, "strategy_key": "auto_" + identity[:24],
                          "version": "research-rule-v1:" + identity[:24], "validation": {"status": "NOT_STARTED"},
                          "registration": {"status": "NOT_STARTED"}})
    return {"summary": result["summary"], "proposals": proposals, "provider_receipt": reply.provider_receipt,
            "prompt_sha256": canonical_sha256(messages), "raw_response_sha256": canonical_sha256(result)}


def _registration_payload(proposal, state):
    from server.engine.strategy_execution_adapters import _TRUSTED_STARTUP_ADAPTERS, normalize_execution_binding
    from server.engine.research_rule_adapter import ADAPTER_KEY, ADAPTER_VERSION
    from server.engine.shadow_trial_policy import SHADOW_EXECUTION_POLICY
    adapter = next(adapter for adapter in _TRUSTED_STARTUP_ADAPTERS if adapter.adapter_key == ADAPTER_KEY)
    binding = normalize_execution_binding({"adapter_key": ADAPTER_KEY, "adapter_version": ADAPTER_VERSION,
        "artifact_sha256": adapter.artifact_sha256, "strategy_version": proposal["version"],
        "cost_model": {"model_key": "research.estimated.cn.v1", "currency": "CNY", "commission_pct": 0.03,
                       "stamp_tax_pct": 0.05, "slippage_pct": 0.10, "transfer_fee_pct": 0.001}},
        strategy_version=proposal["version"])
    return {"strategy_key": proposal["strategy_key"], "strategy_name": proposal["name"], "version": proposal["version"],
        "category": "自主研究", "family_key": "autonomous_rules", "description": proposal["thesis"],
        "owner_name": OPERATOR, "evaluator_type": "declarative_daily_research",
        "evaluator_config": {"execution_adapter": binding,
            "market_regime_multipliers": {"trend_bullish": 1.0, "high_range": 1.0, "risk_declining": 1.0, "extreme_event": 0.0},
            "autonomous_research": {"run_id": state["run_id"], "rule_hash": proposal["rule_hash"],
                "proposed_at": state["proposal_generated_at"], "source_analysis_run_uid": state["analysis_run_uid"],
                "historical_validation": proposal["validation"], "fees_are_estimates": True,
                "applicability": proposal["applicability"], "invalidations": proposal["invalidations"]}},
        "parameters": {"rule_spec": proposal["rule"], "max_holding_days": proposal["rule"]["holding_days"],
                       "label_horizon_days": proposal["rule"]["holding_days"], "shadow_execution_policy": SHADOW_EXECUTION_POLICY},
        "reason": "自主提出并冻结的研究假设；仅影子观察，收益与资金资格待独立验证"}


def _historical_inventory(connection, state):
    """Use actual published decision snapshots, never today's features for past days."""
    sessions = connection.execute(text("""
        SELECT /*+ MAX_EXECUTION_TIME(4000) */ DISTINCT trade_date FROM si_trade_calendar
        WHERE trade_status=1 AND trade_date<=:day ORDER BY trade_date DESC LIMIT 120
    """), {"day": state["trade_date"]}).scalars().all()
    days = sorted(str(day)[:10] for day in sessions)
    records = connection.execute(text("""
        SELECT /*+ MAX_EXECUTION_TIME(4000) JOIN_ORDER(s,a) */ s.trade_date,a.output_dataset_id,a.finished_at
        FROM st_daily_stage_attempt a JOIN st_daily_delivery_session s ON s.session_uid=a.session_uid
        WHERE a.stage_name='analysis_fast' AND a.status='SUCCESS'
          AND s.trade_date BETWEEN :start AND :end AND a.finished_at<=:cutoff
        ORDER BY s.trade_date,a.finished_at DESC,a.id DESC
    """), {"start": days[0] if days else state["trade_date"], "end": state["trade_date"],
           "cutoff": state["decision_known_at"]}).mappings().all()
    by_day = {}
    for record in records:
        by_day.setdefault(str(record["trade_date"])[:10], str(record["output_dataset_id"] or ""))
    return days, by_day


def _validate_historical(connection, state, proposal):
    from server.engine.strategy_rule_backtest import run_rule_backtest
    return run_rule_backtest(connection, state, proposal, inventory=_historical_inventory(connection, state))


def _step(connection, state, owner):
    partition_events = []
    deadline = time.monotonic() + POLICY["maximum_step_seconds"]
    state.update(status="RUNNING", step_status="RUNNING", reason="", current_unit=state["stage"], last_progress_at=now_shanghai().isoformat(sep=" "))
    _checkpoint(connection, state, owner)
    if state["stage"] == "GENERATE":
        if not state.get("provider_attempted"):
            rows, market, _snapshot = _source(connection, state)
            catalog = {field: info for field, info in feature_catalog(rows, market).items()
                       if info.get("coverage", 1.0) >= 0.9}
            previous = []
            for prior in _read_states(connection, 4):
                if prior["run_id"] == state["run_id"] or str(prior.get("updated_at") or "") > state["decision_known_at"]:
                    continue
                proposals = []
                for p in prior.get("proposals", []):
                    v, report = p["validation"], p["validation"].get("report") or {}
                    proposals.append({"name": p["name"], "rule": p["rule"], "validation": {
                        "status": v["status"], "reason": v.get("reason"), "out_of_sample_status": v.get("out_of_sample_status"),
                        "completed_trade_count": report.get("completed_trade_count"), "net_return_pct": report.get("net_return_pct")}})
                previous.append({"run_id": prior["run_id"], "trade_date": prior["trade_date"], "proposals": proposals})
            state["proposal_packet"] = {"trade_date": state["trade_date"], "decision_known_at": state["decision_known_at"],
                "time_zone": "Asia/Shanghai", "data_scope": "冻结盘后量价及PIT行业；未接入盘中盘口、概念内身份及席位证据",
                "source_analysis_run_uid": state["analysis_run_uid"], "feature_catalog": catalog, "market": market,
                "external_observations": state["external_observations"], "previous_research": previous,
                "sample_size": len(rows), "mature_forward_feedback": _performance_feedback(connection, state), "policy": POLICY}
            state["provider_attempted"] = True
            _checkpoint(connection, state, owner)
        packet = state["proposal_packet"]
        generated = generate_proposals(packet, engine=connection.engine, request_id=state["provider_request_id"])
        state.update(generated, proposal_generated_at=now_shanghai().isoformat(sep=" "),
                     market=packet["market"], feature_catalog=packet["feature_catalog"], stage="VALIDATE", completed_units=1,
                     total_units=1 + len(generated["proposals"]) * 2)
    elif state["stage"] == "VALIDATE":
        pending = next((p for p in state["proposals"] if p["validation"]["status"] in {"NOT_STARTED", "RUNNING"}), None)
        if pending is not None:
            from server.engine.strategy_price_backtest import supports_price_rule, start_replay, advance_replay
            if supports_price_rule(pending["rule"]):
                proposals = [p for p in state["proposals"] if supports_price_rule(p["rule"])]
                campaign = state.get("price_backtest")
                if campaign is None:
                    days, _ = _historical_inventory(connection, state)
                    campaign = start_replay(connection, state, proposals, days)
                    state["price_backtest"] = campaign
                    state["current_unit"] = "历史回测已创建 · " + campaign["run_id"]
                    _checkpoint(connection, state, owner)
                before = sum(p["validation"]["status"] not in {"NOT_STARTED", "RUNNING"} for p in proposals)
                def replay_checkpoint(current, events, *, completed=True):
                    state.update(price_backtest=current, current_unit=current["current_unit"])
                    if completed:
                        state["last_progress_at"] = now_shanghai().isoformat(sep=" ")
                    _checkpoint(connection, state, owner, partition_events=events)
                campaign, partition_events = advance_replay(connection, state, proposals, campaign, checkpoint=replay_checkpoint)
                state["price_backtest"] = campaign
                after = sum(p["validation"]["status"] not in {"NOT_STARTED", "RUNNING"} for p in proposals)
                state["completed_units"] += after - before
                state["current_unit"] = campaign["current_unit"]
            else:
                pending["validation"] = _validate_historical(connection, state, pending)
                state["current_unit"] = pending["name"] + " · " + str(pending["validation"].get("current_unit") or "历史数据检查")
                partition_events = pending["validation"].pop("_events", [])
                if pending["validation"]["status"] not in {"NOT_STARTED", "RUNNING"}:
                    state["completed_units"] += 1
        if all(p["validation"]["status"] not in {"NOT_STARTED", "RUNNING"} for p in state["proposals"]):
            state["stage"] = "REGISTER"
    elif state["stage"] == "REGISTER":
        from server.engine.strategy_challenger_factory import register_new_strategy
        pending = next((p for p in state["proposals"] if p["registration"]["status"] == "NOT_STARTED"), None)
        if pending is not None:
            state["current_unit"] = "登记规则 · " + pending["name"]
            payload = _registration_payload(pending, state)
            existing = connection.execute(text("""SELECT r.current_version,r.current_status,v.parameters_json
                FROM st_strategy_registry r JOIN st_strategy_version v
                  ON v.strategy_key=r.strategy_key AND v.version=r.current_version WHERE r.strategy_key=:key"""),
                                          {"key": pending["strategy_key"]}).mappings().first()
            if existing is None:
                registered = register_new_strategy(payload, operator=OPERATOR)
            else:
                stored = json.loads(str(existing["parameters_json"]))
                if canonical_sha256(stored.get("rule_spec")) != pending["rule_hash"]:
                    raise ValueError("已注册的研究规则身份冲突，禁止覆盖")
                registered = {"current_status": existing["current_status"], "strategy_key": pending["strategy_key"],
                              "current_version": existing["current_version"]}
            state["current_unit"] = "核对研究候选 · " + pending["name"]
            rows, market, _snapshot = _source(connection, state)
            picks = select_rule_rows(pending["rule"], rows, market)
            pending["registration"] = {"status": "COMPLETE", "lifecycle": registered["current_status"],
                                       "strategy_key": registered["strategy_key"], "version": registered["current_version"]}
            pending["research_picks"] = [{k: r.get(k) for k in ("stock_code", "stock_name", "research_rule_score", "industry")}
                                         for r in picks]
            pending["forward_evidence"] = {"status": "AWAITING_EVALUATION", "proposed_at": state["proposal_generated_at"],
                                           "reason": "已登记规则，等待现有日评估及影子执行器产生候选、计划和成交证据",
                                           "funding_eligible": False, "real_order_authority": False}
            state["completed_units"] += 1
        if all(p["registration"]["status"] == "COMPLETE" for p in state["proposals"]):
            blocked = any(p["validation"]["status"] in {"BLOCKED", "PARTIAL", "FAILED"} for p in state["proposals"])
            state.update(stage="FINISHED", status="BLOCKED" if blocked else "COMPLETE",
                         reason="候选已生成并登记影子观察；历史验证存在数据缺口" if blocked else "候选生成与研究验证已处理；盈利与资金资格仍待前向证据")
    if time.monotonic() > deadline:
        raise TimeoutError("自主研究阶段超过总时限：" + state["stage"])
    if state["stage"] != "FINISHED":
        state["status"] = "NOT_STARTED"
    state.update(step_status="COMPLETE", last_progress_at=now_shanghai().isoformat(sep=" "))
    _checkpoint(connection, state, owner, partition_events=partition_events)
    return state


def _arm_database_retry(state, *, failed_at):
    code = state.get("error_errno")
    limit = (1 if code == 3024 and state.get("stage") == "VALIDATE" and state.get("price_backtest")
             else 2 if code in {2003, 2006, 2013, 1205, 1213} else 0)
    attempt = int(state.get("retry_count") or 0) + 1
    if (attempt > limit or (state.get("stage") == "GENERATE" and state.get("provider_attempted"))):
        return False
    state.update(status="BLOCKED", retryable=True, retry_count=attempt,
                 retry_after=(datetime.fromisoformat(failed_at) + timedelta(minutes=5)).isoformat(sep=" "))
    if state.get("price_backtest") and state.get("stage") == "VALIDATE":
        state["price_backtest"]["status"] = "BLOCKED"
        for proposal in state["proposals"]:
            if (proposal["validation"].get("replay_run_id") == state["price_backtest"]["run_id"]
                    and proposal["validation"]["status"] in {"FAILED", "RUNNING"}):
                proposal["validation"]["status"] = "NOT_STARTED"
    return True


def research_tick(engine):
    """Do one bounded resumable step when the existing V2 queue has no job."""
    with engine.connect() as connection:
        acquired = connection.execute(text("SELECT GET_LOCK(:name,0)"), {"name": LOCK}).scalar()
        if acquired != 1:
            return {"status": "RUNNING", "reason": "已有自主研究运行持有互斥锁"}
        state = None
        try:
            owner = _owner(connection)
            states = _read_states(connection)
            # Complete the oldest unfinished cycle before proposing another batch.
            pending = [s for s in states if s.get("status") in {"NOT_STARTED", "RUNNING"}]
            if pending:
                state = min(pending, key=lambda s: s["created_at"])
            else:
                from server.common.daily_delivery_control import DailyDeliveryControlError
                day = authoritative_closed_trade_date(connection, now=now_shanghai(), close_ready_time=wall_time(15, 35))
                if not day:
                    return {"status": "NOT_STARTED", "reason": "尚无权威已收盘交易日"}
                try:
                    receipt = load_published_analysis_receipt(connection, day)
                except DailyDeliveryControlError as exc:
                    if str(exc) == "ANALYSIS_PUBLISHED_SNAPSHOT_UNAVAILABLE":
                        return {"status": "NOT_STARTED", "trade_date": day, "reason": "等待当日完整冻结评分批次"}
                    raise
                root = receipt["score_snapshot"]["payload_sha256"]
                state = next((s for s in states if root == s.get("analysis_payload_hash")
                              or root in s.get("equivalent_source_hashes", [])), None)
                if state is None:
                    source_state = {"trade_date": day, "analysis_run_uid": receipt["run_uid"],
                                    "analysis_payload_hash": root, "decision_known_at": now_shanghai().isoformat(sep=" ")}
                    rows, market, _snapshot = _source(connection, source_state)
                    run_id = research_input_identity(rows, market, day)
                    exact = _read_states(connection, limit=1, run_id=run_id)
                    state = exact[0] if exact else None
                    if state is not None:
                        # Same verified features republished by another release reuse the original experiment.
                        aliases = state.setdefault("equivalent_source_hashes", [])
                        state["equivalent_source_hashes"] = (aliases + [root])[-24:]
                        _checkpoint(connection, state, owner)
            if (state and state["status"] in {"COMPLETE", "FAILED", "BLOCKED"}
                    and state["stage"] in {"FINISHED", "VALIDATE"}
                    and all(p["registration"]["status"] == "COMPLETE" for p in state["proposals"])):
                from server.engine.strategy_price_backtest import replay_contract, supports_price_rule
                price_proposals = [p for p in state["proposals"] if supports_price_rule(p["rule"])]
                contract = replay_contract() if price_proposals else None
                campaign = state.get("price_backtest") or {}
                # Completed experiments retain their own immutable calculation version.
                completed = (campaign.get("phase") == "FINISHED"
                    and isinstance(campaign.get("contract"), str) and len(campaign["contract"]) == 64
                    and bool(campaign.get("run_id"))) and all(
                    p["validation"].get("backtest_contract") == campaign.get("contract")
                    and p["validation"].get("replay_run_id") == campaign.get("run_id")
                    and p["validation"].get("status") in {"COMPLETE", "PARTIAL", "BLOCKED"}
                    and isinstance(p["validation"].get("report"), dict)
                    and p["validation"]["report"].get("status") == p["validation"]["status"]
                    for p in price_proposals
                )
                if not completed and any(p["validation"].get("backtest_contract") != contract for p in price_proposals):
                    # Evaluation has its own identity and deadline; generation and registration stay immutable.
                    state["generation_deadline_at"] = state.get("generation_deadline_at", state["deadline_at"])
                    price_keys = {p["strategy_key"] for p in price_proposals}
                    completed = 1 + sum(p["registration"]["status"] == "COMPLETE" for p in state["proposals"]) + sum(
                        p["strategy_key"] not in price_keys and p["validation"]["status"] not in {"NOT_STARTED", "RUNNING"}
                        for p in state["proposals"])
                    state.update(status="NOT_STARTED", stage="VALIDATE", step_status="NOT_STARTED", retryable=False,
                        retry_count=0, error_type=None, error_errno=None,
                        deadline_at=(now_shanghai() + timedelta(hours=24)).isoformat(sep=" "),
                        completed_units=completed)
                    if state.get("price_backtest"):
                        state["previous_price_backtest"] = state.pop("price_backtest")
                    for p in price_proposals:
                        p["validation"] = {"status": "NOT_STARTED"}
                    _checkpoint(connection, state, owner)
            if (state and state["status"] == "FAILED" and not state.get("retryable")
                    and _arm_database_retry(state, failed_at=state["updated_at"])):
                _checkpoint(connection, state, owner)
            if state and state["status"] in {"COMPLETE", "FAILED", "BLOCKED"}:
                if (state.get("retryable") is True and int(state.get("retry_count") or 0) <= 2
                        and str(state.get("retry_after") or "9999") <= now_shanghai().isoformat(sep=" ")):
                    state.update(status="RUNNING", step_status="NOT_STARTED", retryable=False)
                else:
                    return {"status": state["status"], "run_id": state["run_id"], "stage": state["stage"], "reused": True}
            if state is None:
                clock = now_shanghai().isoformat(sep=" ")
                state = {"schema": SCHEMA, "run_id": run_id, "trade_date": day, "analysis_run_uid": receipt["run_uid"],
                    "analysis_payload_hash": root, "decision_known_at": clock, "created_at": clock,
                    "status": "NOT_STARTED", "stage": "GENERATE", "step_status": "NOT_STARTED", "proposals": [],
                    "provider_request_id": str(uuid5(NAMESPACE_URL, "probiga:autonomous-strategy:" + run_id)),
                    "deadline_at": (now_shanghai() + timedelta(hours=24)).isoformat(sep=" "),
                    "completed_units": 0, "total_units": 1, "external_observations": _external_context(connection, clock),
                    "policy": POLICY, "automatic_real_order_submission": False, "real_order_authority": False}
                _checkpoint(connection, state, owner)
            elif not _resume_allowed(connection, state, owner):
                return {"status": "BLOCKED", "run_id": state["run_id"], "reason": "旧研究所有者尚不能证明已退出"}
            if state["deadline_at"] < now_shanghai().isoformat(sep=" "):
                state.update(status="FAILED", reason="自主研究批次超过24小时总时限；已完成分区保留")
                _checkpoint(connection, state, owner)
                return {"status": "FAILED", "run_id": state["run_id"], "reason": state["reason"]}
            current = now_shanghai()
            if state["stage"] in {"GENERATE", "REGISTER"} and wall_time(8, 45) <= current.time() <= wall_time(16):
                is_open = connection.execute(text("SELECT COUNT(*) FROM si_trade_calendar WHERE trade_date=:today AND trade_status=1"),
                    {"today": current.date()}).scalar()
                if is_open:
                    state.update(status="NOT_STARTED", reason="生成与登记在盘前或收盘后继续，避免盘中生成规则被回填为开盘已知")
                    _checkpoint(connection, state, owner)
                    return {"status": "NOT_STARTED", "run_id": state["run_id"], "reason": state["reason"]}
            state = _step(connection, state, owner)
            return {key: state.get(key) for key in ("status", "run_id", "stage", "completed_units", "total_units", "reason")}
        except Exception as exc:
            from biz.review.market_decision import ResearchError
            reason = str(exc)[:600] if isinstance(exc, (ValueError, ResearchError, TimeoutError, RuntimeError)) else type(exc).__name__
            if state is not None:
                reason = str(state.get("current_unit") or state.get("stage")) + ": " + reason
                state.update(status="FAILED", step_status="FAILED", reason=reason)
                original = getattr(exc, "orig", exc)
                error_args = getattr(original, "args", ())
                state["error_type"] = type(exc).__name__
                state["error_errno"] = error_args[0] if error_args and type(error_args[0]) is int else None
                _arm_database_retry(state, failed_at=now_shanghai().isoformat(sep=" "))
                if state.get("price_backtest") and state["stage"] == "VALIDATE":
                    state["price_backtest"].update(status=state["status"], reason=reason)
                    for proposal in state["proposals"]:
                        if proposal["validation"]["status"] == "RUNNING":
                            proposal["validation"].update(status="NOT_STARTED" if state.get("retryable") else "FAILED", reason=reason)
                if getattr(exc, "provider_failure_receipt", None):
                    state["provider_failure_receipt"] = exc.provider_failure_receipt
                connection.rollback()
                try:
                    _checkpoint(connection, state, owner)
                except Exception as terminal_error:
                    return {"status": "BLOCKED", "run_id": state["run_id"], "stage": state["stage"],
                            "reason": reason, "terminal_write_error": type(terminal_error).__name__,
                            "durable_checkpoint_requires_owner_reconciliation": True}
            return {"status": state.get("status", "FAILED") if state else "FAILED",
                    "run_id": state.get("run_id") if state else None, "reason": reason}
        finally:
            connection.rollback()
            if not connection.invalidated:
                connection.execute(text("SELECT RELEASE_LOCK(:name)"), {"name": LOCK})
                connection.commit()


def read_research_workspace(connection, *, health_rows=()):
    states = _read_states(connection)
    health = {}
    for row in health_rows:
        if int(row.get("completed_trades") or 0) > 0:
            health.setdefault((row["strategy_key"], row["strategy_version"]), {})[str(row["window_days"])] = row
    for state in states:
        for proposal in state.get("proposals", []):
            windows = health.get((proposal["strategy_key"], proposal["version"]))
            if windows:
                proposal["forward_evidence"] = {"status": "OBSERVED", "basis": "已验证规范治理快照", "windows": windows,
                                                 "funding_eligible": False, "real_order_authority": False}
    return {"schema": SCHEMA, "policy": POLICY, "runs": [
        {key: state.get(key) for key in ("run_id", "trade_date", "created_at", "decision_known_at", "proposal_generated_at", "updated_at", "status", "stage", "reason",
            "completed_units", "total_units", "current_unit", "last_progress_at", "summary", "proposals", "provider_receipt")}
        for state in states], "automatic_real_order_submission": False, "real_order_authority": False}
