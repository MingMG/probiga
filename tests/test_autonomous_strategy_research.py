from copy import deepcopy
from datetime import datetime, timedelta
import json
import hashlib
import os
import socket
import threading
import time
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, text

from server.engine import autonomous_strategy_research as research
from server.engine.strategy_rule_language import RULE_SCHEMA, feature_frame, feature_catalog, select_rule_rows, validate_rule
from server.engine.strategy_rule_replay import replay_rule


@pytest.fixture
def rule():
    return {"schema": RULE_SCHEMA, "market_all": [{"field": "market_breadth_pct", "op": "lte", "value": 50}],
            "entry_all": [{"field": "change_pct", "op": "gt", "value": 0}], "entry_any": [],
            "ranking": [{"field": "relative_change_pct", "ascending": False, "weight": 1}],
            "top_n": 2, "holding_days": 2, "stop_loss_pct": 5}


@pytest.fixture
def raw_rows():
    return [{"stock_code": code, "short_name": code, "close": 10, "change_pct": move,
             "pct_20": move * 2, "previous_change_pct": -1, "amount": 1e8,
             "industry_name": "行业", "industry_snapshot_source": "gj_big_qmt_inner",
             "industry_snapshot_date": "2026-11-20", "membership_proof_sha256": "a" * 64}
            for code, move in [("000001", 3), ("000002", -2), ("000003", -4)]]


def test_weak_market_rule_selects_relative_strength_and_is_order_independent(rule, raw_rows):
    rows, market = feature_frame(raw_rows, "2026-11-20")
    assert market["market_breadth_pct"] == pytest.approx(100 / 3)
    picked = select_rule_rows(rule, rows, market)
    assert [r["stock_code"] for r in picked] == ["000001"]
    assert picked == select_rule_rows(rule, list(reversed(rows)), market)


def test_research_reuses_equal_features_after_unrelated_republication(raw_rows):
    rows, market = feature_frame(raw_rows, "2026-11-20")
    first = research.research_input_identity(rows, market, "2026-11-20")
    for row in raw_rows:
        row["build_sha"] = "unrelated-release"
        row["published_at"] = "2026-11-21 22:00:00"
    same_rows, same_market = feature_frame(list(reversed(raw_rows)), "2026-11-20")
    assert research.research_input_identity(same_rows, same_market, "2026-11-20") == first
    same_rows[0]["change_pct"] += 1
    assert research.research_input_identity(same_rows, same_market, "2026-11-20") != first


@pytest.mark.parametrize("field", ["future_return", "net_pnl", "__import__", "stock_code"])
def test_rule_rejects_future_outcomes_or_executable_content(rule, field):
    rule["entry_all"][0]["field"] = field
    with pytest.raises(ValueError):
        validate_rule(rule)


def test_missing_market_feature_is_not_zero_or_a_valid_empty_pool(rule, raw_rows):
    rows, _ = feature_frame(raw_rows, "2026-11-20")
    with pytest.raises(ValueError, match="市场特征缺失"):
        select_rule_rows(rule, rows, {})


def test_unverified_industry_does_not_supply_sector_features(rule, raw_rows):
    for row in raw_rows:
        row["industry_snapshot_date"] = "2026-11-19"
    rows, market = feature_frame(raw_rows, "2026-11-20")
    assert all(r["industry"] is None for r in rows)
    assert "sector_breadth_pct" not in feature_catalog(rows, market)
    rule["entry_all"] = [{"field": "sector_breadth_pct", "op": "lt", "value": 50}]
    with pytest.raises(ValueError, match="覆盖不足"):
        select_rule_rows(rule, rows, market)


@pytest.mark.parametrize("field,value", [("holding_days", True), ("holding_days", 0), ("stop_loss_pct", 40), ("top_n", 500)])
def test_model_cannot_expand_execution_risk(rule, field, value):
    rule[field] = value
    with pytest.raises(ValueError):
        validate_rule(rule)


@pytest.fixture
def replay_data():
    days = [(datetime(2026, 1, 1) + timedelta(days=i)).date().isoformat() for i in range(27)]
    bars = [{"trade_date": day, "stock_code": "000001", "open": 10, "high": 10.1, "low": 9.9,
             "close": 10, "pre_close": 10, "volume": 1e7, "amount": 1e8} for day in days]
    signals = [{"stock_code": "000001", "stock_name": "测试证券", "industry": "行业", "signal_date": days[20],
                "signal_known_at": days[20] + " 18:00:00", "rank": 1, "score": 90}]
    return days, bars, signals


def test_research_replay_honors_next_session_t_plus_one_and_estimated_fees(rule, replay_data):
    days, bars, signals = replay_data
    rule["holding_days"] = 1
    report = replay_rule(rule, signals, bars, days)
    assert report["status"] == "COMPLETE"
    trade = report["trades"][0]
    assert trade["entry_at"] == days[21] + " 09:30:00"
    assert trade["exit_at"] == days[22] + " 09:30:00"
    assert trade["net_return_pct"] < 0
    assert report["out_of_sample_status"] == "RETROSPECTIVE_NOT_PRISTINE"
    assert report["funding_eligible"] is False


def test_daily_stop_does_not_create_same_day_exit(rule, replay_data):
    days, bars, signals = replay_data
    rule["holding_days"] = 20
    bars[21]["low"] = 9
    result = replay_rule(rule, signals, bars, days)
    assert result["trades"][0]["exit_at"] == days[22] + " 09:30:00"
    assert result["trades"][0]["exit_reason"] == "保护止损"


def test_locked_limit_is_unfilled_not_an_invented_trade(rule, replay_data):
    days, bars, signals = replay_data
    bars[21].update(open=11, high=11, low=11, close=11)
    report = replay_rule(rule, signals, bars, days)
    assert report["trades"] == []
    assert report["dispositions"][0]["reason"] == "LOCKED_LIMIT_UP"


def test_order_is_not_resized_using_future_session_turnover(rule, replay_data):
    days, bars, signals = replay_data
    bars[21]["amount"] = 1000
    report = replay_rule(rule, signals, bars, days)
    assert report["trades"] == []
    assert any("未来成交额" in d["reason"] for d in report["dispositions"])


def test_unknown_corporate_action_does_not_create_fake_loss(rule, replay_data):
    days, bars, signals = replay_data
    rule["holding_days"] = 1
    bars[22].update(open=5, high=5.1, low=4.9, close=5, pre_close=5)
    report = replay_rule(rule, signals, bars, days)
    assert report["status"] == "BLOCKED"
    assert report["net_return_pct"] is None
    assert report["trades"][0]["net_pnl_cny"] is None
    assert report["nav"][22]["nav_cny"] is None


@pytest.fixture
def ledger(monkeypatch):
    engine = create_engine("sqlite://")
    @event.listens_for(engine, "connect")
    def functions(dbapi, _record):
        dbapi.create_function("IS_USED_LOCK", 1, lambda _name: 1)
        dbapi.create_function("CONNECTION_ID", 0, lambda: 1)
    with engine.begin() as c:
        c.execute(text("""CREATE TABLE st_strategy_governance_audit (
            audit_id TEXT PRIMARY KEY,entity_type TEXT,entity_key TEXT,action TEXT,reason TEXT,
            operator_name TEXT,before_json TEXT,after_json TEXT,evidence_json TEXT,payload_json TEXT,
            audit_hash TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP)"""))
        c.execute(text("""CREATE TABLE st_scheduled_task_history (id INTEGER PRIMARY KEY,run_uid TEXT,task_id INT,
            task_name TEXT,task_type TEXT,run_at TEXT,status TEXT,host_name TEXT,scheduler_instance_id TEXT,
            build_sha TEXT,trigger_source TEXT,output TEXT)"""))
        from server.common.scheduler_task_lease import encode
        now = research.now_shanghai()
        identity = {"run_uid": "1" * 32, "task_id": 70, "task_name": "Worker", "task_type": "trading_v2_job_worker",
                    "run_at": now.isoformat(sep=" "), "host_name": socket.gethostname(),
                    "scheduler_instance_id": socket.gethostname() + "-123", "build_sha": "a" * 40,
                    "trigger_source": "scheduled"}
        c.execute(text("""INSERT INTO st_scheduled_task_history VALUES
            (1,:run_uid,:task_id,:task_name,:task_type,:run_at,'running',:host_name,:scheduler_instance_id,:build_sha,:trigger_source,:output)"""),
                  {**identity, "output": encode(identity, now)})
    monkeypatch.setenv("PROBIGA_SCHEDULER_TASK_ID", "70")
    monkeypatch.setenv("PROBIGA_SCHEDULER_TASK_TYPE", "trading_v2_job_worker")
    yield engine
    engine.dispose()


def test_checkpoint_readback_uses_sequence_not_second_precision_or_random_uuid(ledger):
    state = {"schema": research.SCHEMA, "run_id": "a" * 64, "stage": "GENERATE", "analysis_run_uid": "2" * 32,
             "analysis_payload_hash": "b" * 64, "status": "RUNNING"}
    with ledger.connect() as c:
        owner = research._owner(c)
        research._checkpoint(c, state, owner)
        state["stage"] = "VALIDATE"
        research._checkpoint(c, state, owner)
        assert research._read_states(c)[0]["stage"] == "VALIDATE"
        c.execute(text("UPDATE st_strategy_governance_audit SET after_json='{}' WHERE action='RESEARCH_CHECKPOINT_00000002'"))
        c.commit()
        with pytest.raises(ValueError, match="哈希"):
            research._read_states(c)


def test_terminal_scheduler_owner_cannot_publish_research(ledger):
    with ledger.connect() as c:
        owner = research._owner(c)
        c.execute(text("UPDATE st_scheduled_task_history SET status='failed'")); c.commit()
        state = {"schema": research.SCHEMA, "run_id": "a" * 64, "stage": "GENERATE", "analysis_run_uid": "2" * 32,
                 "analysis_payload_hash": "b" * 64}
        with pytest.raises(ValueError, match="运行身份"):
            research._checkpoint(c, state, owner)
        assert c.execute(text("SELECT COUNT(*) FROM st_strategy_governance_audit")).scalar() == 0


def test_complete_feature_history_is_required_without_rebuilding_with_current_factors(ledger, rule):
    from server.engine.strategy_rule_backtest import run_rule_backtest
    state = {"run_id": "a" * 64, "decision_known_at": "2026-11-21 20:00:00"}
    days = [(datetime(2026, 5, 1) + timedelta(days=i)).date().isoformat() for i in range(120)]
    proposal = {"rule": rule, "rule_hash": "b" * 64, "validation": {"status": "NOT_STARTED"}}
    with ledger.connect() as c:
        result = run_rule_backtest(c, state, proposal, inventory=(days, {days[-1]: "c" * 32}))
    assert result["status"] == "BLOCKED"
    assert result["available_feature_sessions"] == 1
    assert len(result["missing_feature_dates"]) == 119
    assert result["net_return_pct"] is None


def test_generated_rule_has_a_real_deployed_candidate_receipt(rule, raw_rows, monkeypatch):
    from server.engine import strategy_execution_adapters as adapters
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "development")
    monkeypatch.setattr(adapters, "_REGISTRY", {})
    monkeypatch.setattr(adapters, "_REGISTRY_SEALED", False)
    adapters.bootstrap_strategy_execution_adapter_registry(registry_rows=())
    proposal = {"name": "弱市相对强势", "rule": rule, "rule_hash": "b" * 64, "strategy_key": "auto_" + "b" * 24,
                "version": "research-rule-v1:b", "thesis": "研究假设", "applicability": "弱市", "invalidations": "强势失效",
                "validation": {"status": "BLOCKED"}}
    state = {"run_id": "a" * 64, "proposal_generated_at": "2026-11-21 20:00:00", "analysis_run_uid": "c" * 32}
    from server.engine.strategy_challenger_factory import _normalized_registration_payload
    from server.engine.strategy_governance import _strategy_content_digest
    payload, version_hash = _normalized_registration_payload(research._registration_payload(proposal, state), operator="test")
    content_hash = _strategy_content_digest(strategy_key=payload["strategy_key"], evaluator_type=payload["evaluator_type"],
        evaluator_config=payload["evaluator_config"], parameters=payload["parameters"], source_kind="runtime_registry")
    strategy = {"strategy_key": payload["strategy_key"], "current_version": payload["version"], "version_hash": version_hash, "content_hash": content_hash,
                "source_kind": "runtime_registry", "evaluator_type": payload["evaluator_type"], "enabled": True, "version_integrity_valid": True,
                "current_status": "SHADOW", "parameters": payload["parameters"], "evaluator_config": payload["evaluator_config"]}
    batch = adapters.execute_dynamic_adapter_candidate_batch(strategy, {"trade_date": "2026-11-20",
        "recommendation_rows": raw_rows, "market": {}, "configs": {}, "metrics": {}})
    assert batch["receipt"]["candidate_count"] == 1
    assert batch["signals"][0]["stock_code"] == "000001"
    assert batch["signals"][0]["real_order_authority"] is False


def test_model_result_reuses_one_durable_request_after_caller_restart(rule, raw_rows, tmp_path, monkeypatch):
    from server.ai_bridge import research as bridge
    from server.ai_bridge.schema import metadata
    engine = create_engine(f"sqlite:///{tmp_path / 'bridge.db'}", connect_args={"check_same_thread": False})
    metadata.create_all(engine)
    monkeypatch.setitem(research.POLICY, "generation_seconds", 5)
    rows, market = feature_frame(raw_rows, "2026-11-20")
    packet = {"trade_date": "2026-11-20", "decision_known_at": "2026-11-21 20:00:00",
              "feature_catalog": feature_catalog(rows, market), "market": market, "previous_research": []}
    response = {"summary": "仅为确定性单元测试响应", "proposals": [{"name": "测试规则", "thesis": "测试假设",
        "applicability": "测试弱市", "invalidations": "测试条件失效", "rule": rule}]}
    request_id, failures = str(uuid4()), []
    def test_provider():
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                claimed = bridge.claim_research(engine, worker_id="unit-test-provider")
                if not claimed:
                    time.sleep(0.02)
                    continue
                payload = claimed["payload"]
                raw = json.dumps(response, ensure_ascii=False)
                receipt = {"source": "codex_gpt", "request_id": request_id, "model": "TEST_ONLY",
                           "input_sha256": payload["input_sha256"], "rules_sha256": payload["rules_sha256"],
                           "raw_sha256": hashlib.sha256(raw.encode()).hexdigest()}
                bridge.complete_research(engine, request_id=request_id, worker_id="unit-test-provider", claim_generation=1,
                                         status="completed", raw_response=raw, provider_receipt=receipt)
                return
            raise TimeoutError("unit test request not claimed")
        except BaseException as exc:
            failures.append(exc)
    thread = threading.Thread(target=test_provider)
    thread.start()
    try:
        first = research.generate_proposals(packet, engine=engine, request_id=request_id)
        thread.join(timeout=5)
        assert not thread.is_alive() and not failures
        second = research.generate_proposals(packet, engine=engine, request_id=request_id)
        assert first == second
        with engine.connect() as c:
            assert c.execute(text("SELECT COUNT(*) FROM st_ai_bridge_job")).scalar() == 1
            assert c.execute(text("SELECT attempts FROM st_ai_bridge_job")).scalar() == 1
    finally:
        thread.join(timeout=5)
        engine.dispose()


def test_unfinished_validation_keeps_its_checkpoint_and_does_not_register(ledger, monkeypatch):
    state = {"schema": research.SCHEMA, "run_id": "a" * 64, "trade_date": "2026-11-20", "stage": "VALIDATE",
             "analysis_run_uid": "b" * 32, "analysis_payload_hash": "c" * 64, "status": "NOT_STARTED",
             "completed_units": 1, "total_units": 3,
             "proposals": [{"name": "测试假设", "validation": {"status": "NOT_STARTED"}, "registration": {"status": "NOT_STARTED"}}]}
    monkeypatch.setattr(research, "_validate_historical", lambda *args: {
        "status": "RUNNING", "current_unit": "SIGNALS:2026-08-01", "cursor": 3, "completed_units": 3, "total_units": 260})
    with ledger.connect() as c:
        result = research._step(c, state, research._owner(c))
        readback = research._read_states(c)[0]
    assert result["stage"] == "VALIDATE"
    assert result["completed_units"] == 1
    assert readback["proposals"][0]["validation"]["cursor"] == 3
    assert readback["proposals"][0]["registration"]["status"] == "NOT_STARTED"
    assert readback["status"] == "NOT_STARTED"


def test_resume_reads_next_partitions_and_reuses_the_durable_cursor(ledger, rule, monkeypatch):
    from server.engine import strategy_rule_backtest as backtest
    days = [(datetime(2026, 5, 1) + timedelta(days=i)).date().isoformat() for i in range(120)]
    with ledger.begin() as c:
        c.execute(text("CREATE TABLE si_trade_calendar (trade_date TEXT,trade_status INT)"))
    seen = []
    def published_partition(_connection, _state, _proposal, day, source):
        seen.append(day)
        return {"day": day, "signals": [], "publication_run_uid": source, "publication_payload_sha256": "e" * 64}
    monkeypatch.setattr(backtest, "_signals", published_partition)
    proposal = {"name": "测试规则", "rule": rule, "rule_hash": "d" * 64,
                "validation": {"status": "NOT_STARTED"}, "registration": {"status": "NOT_STARTED"}}
    state = {"schema": research.SCHEMA, "run_id": "a" * 64, "trade_date": days[-1], "stage": "VALIDATE",
             "analysis_run_uid": "b" * 32, "analysis_payload_hash": "c" * 64, "status": "NOT_STARTED",
             "decision_known_at": "2026-11-21 20:00:00", "completed_units": 1, "total_units": 3, "proposals": [proposal]}
    inventory = (days, {day: "f" * 32 for day in days})
    monkeypatch.setattr(research, "_historical_inventory", lambda *_: inventory)
    with ledger.connect() as c:
        owner = research._owner(c)
        research._step(c, state, owner)
        resumed = research._read_states(c)[0]
        research._step(c, resumed, owner)
        assert research._read_states(c)[0]["proposals"][0]["validation"]["cursor"] == 6
        assert c.execute(text("SELECT COUNT(*) FROM st_strategy_governance_audit WHERE entity_type='AUTO_REPLAY_PART'")).scalar() == 6
    assert seen == days[:6]


def test_schema_diagnostic_retains_errno_without_sql_or_credentials():
    from sqlalchemy.exc import OperationalError
    from tools.check_schema_expected import collect_schema_guard
    def unavailable(_engine):
        raise OperationalError("SELECT secret_column FROM private", {}, Exception(2013, "private connection details"))
    report = collect_schema_guard(object(), validators=(("source", unavailable),))
    assert report["checks"]["source"]["database_errno"] == 2013
    assert report["status"] == "MIGRATION_REQUIRED"
    assert "private" not in json.dumps(report) and "secret" not in json.dumps(report)
