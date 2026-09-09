import copy

import pytest

from server.engine.shadow_capacity_queue import select_capacity_queue, validate_capacity_selection
from server.engine.shadow_trial_policy import SHADOW_EXECUTION_POLICY


def inventory(key, horizon=5, supply=12, lifecycle="SHADOW"):
    return {"strategy_key": key, "strategy_version": key+"-v1", "strategy_version_hash": "a"*64,
            "enabled": True, "lifecycle": lifecycle, "version_created_at": "2026-09-09",
            "maximum_holding_sessions": horizon, "affordable_candidate_count": supply,
            "candidate_count": supply}


def choose(rows, ordinal=1, previous=None, slots=12):
    return select_capacity_queue(previous=previous, inventories=rows, trade_date="2026-09-10",
                                 session_ordinal=ordinal, available_slots=slots, equity_cny=200000)


def test_daily_round_robin_capacity_counterexample_and_focused_feasibility():
    # 12 strategies sharing 12 slots get 60 position-days each in a 60-day
    # window: with five-day holds, roughly 12 closes rather than the required 80.
    assert 12*60/12/5 < 80
    rows = [inventory(f"s{i:02d}") for i in range(12)]
    result = choose(rows)
    assert result["selected"]["strategy_key"] == "s00"
    assert result["inventories"][0]["required_position_slots"] == 9
    assert result["inventories"][0]["theoretical_60_session_completions"] == 144
    assert sum(r["capacity_status"] == "CAPACITY_WAITING" for r in result["inventories"]) == 11
    # A new faster candidate and daily score changes cannot reset the cohort.
    for ordinal in range(2, 121):
        rows[0]["daily_return_pct"] = -99 if ordinal % 2 else 99
        result = choose([inventory("new_fast", 1), *reversed(rows)], ordinal, result, slots=0)
        assert result["selected"]["strategy_key"] == "s00"
        assert result["selected"]["end_session_ordinal"] == 120
    ended = choose(rows, 121, result)
    assert ended["selected"]["strategy_key"] == "s01"
    assert ended["completed_versions"][0]["end_reason"] == "EVALUATION_PERIOD_ENDED"


def test_horizon_and_affordability_are_explicit_failure_states():
    result = choose([inventory("long", 20), inventory("expensive", 3, 0)])
    assert not result["selected"]
    states = {r["strategy_key"]: r["capacity_status"] for r in result["inventories"]}
    assert states == {"long": "CAPACITY_INSUFFICIENT", "expensive": "CANDIDATE_SUPPLY_INSUFFICIENT"}
    assert choose([inventory("short", 3)], slots=4)["selected"] == {}


@pytest.mark.parametrize("lifecycle", ["ACTIVE", "REDUCE", "SUSPENDED", "RETIRED"])
def test_governance_terminal_result_advances_queue_before_deadline(lifecycle):
    rows = [inventory("a"), inventory("b")]
    prior = choose(rows)
    rows[0]["lifecycle"] = lifecycle
    current = choose(rows, 3, prior)
    assert current["selected"]["strategy_key"] == "b"
    assert current["completed_versions"][0]["end_reason"] == "GOVERNANCE_"+lifecycle


def test_queue_version_and_hash_are_required_at_order_authorization():
    decision = choose([inventory("a")])
    authorization = {"strategy_key": "a", "strategy_version": "a-v1", "strategy_version_hash": "a"*64,
                     "trade_date": "2026-09-10", "execution_contract": {"maximum_holding_sessions": 5, "policy": SHADOW_EXECUTION_POLICY}}
    assert validate_capacity_selection(decision, authorization) == decision
    for bad in (None, {**decision, "session_ordinal": 121}):
        with pytest.raises(ValueError, match="SELECTION_INVALID"):
            validate_capacity_selection(bad, authorization)
    with pytest.raises(ValueError, match="SELECTION_INVALID"):
        validate_capacity_selection(decision, {**authorization, "strategy_version": "a-v2"})
    forged = copy.deepcopy(decision)
    forged["selected"]["end_session_ordinal"] = 99999
    with pytest.raises(ValueError, match="PREVIOUS_QUEUE_INVALID"):
        choose([inventory("a")], 2, forged)


def test_strategy_center_submits_only_selected_version_and_audits_waiting(monkeypatch):
    from server.engine import strategy_center as center, strategy_governance as governance
    from server.engine import shadow_capacity_queue as queue_module
    from server.trading_v3 import paper_execution

    connection = object()
    registry = [{"strategy_key": key, "current_version": key+"-v1", "current_status": "SHADOW",
                 "source_kind": "runtime_registry", "enabled": True, "version_hash": "a"*64,
                 "execution_adapter": {"status": "RESEARCH_READY", "executable": True}}
                for key in ("a", "b")]
    monkeypatch.setattr(governance, "load_registry", lambda **kwargs: registry)
    monkeypatch.setattr(center, "current_bound_sql_connection", lambda: connection)
    monkeypatch.setattr(center, "_dynamic_shadow_trade_session_ordinal", lambda *args, **kwargs: 1)
    monkeypatch.setattr(center, "execute_dynamic_adapter_candidate_batch", lambda strategy, *args, **kwargs: {
        "receipt": {"candidate_count": 1, "run_uid": strategy["strategy_key"]},
        "signals": [{"strategy_key": strategy["strategy_key"]}], "candidate_facts": [],
    })
    monkeypatch.setattr(center, "persist_strategy_adapter_run_receipt", lambda *args, **kwargs: None)
    monkeypatch.setattr(center, "persist_strategy_adapter_candidate_facts", lambda *args, **kwargs: None)
    monkeypatch.setattr(center, "create_dynamic_shadow_trial_plans_from_candidate_facts",
                        lambda *args, strategy, **kwargs: {"plan_count": 1, "plan_ids": [strategy["strategy_key"]+"-plan"]})
    decision = choose([inventory("a"), inventory("b")])
    monkeypatch.setattr(queue_module, "capacity_queue_for_run", lambda *args, **kwargs: decision)
    submissions = []
    def materialize(*args, **kwargs):
        pytest.fail("order materialization must follow canonical governance persistence")
    monkeypatch.setattr(paper_execution, "materialize_dynamic_shadow_bootstrap_orders", materialize)
    _, statuses = center._dynamic_execution_signals(trade_date="2026-09-10", recommendation_rows=[],
                        market={}, configs={}, metrics={}, persist_receipts=True)
    assert submissions == []
    by_key = {row["strategy_key"]: row for row in statuses}
    assert by_key["a"]["shadow_capacity_queue"]["authorized_plan_ids"] == ["a-plan"]
    assert by_key["b"]["shadow_bootstrap_result"]["capacity_status"] == "CAPACITY_WAITING"
    assert all(row["shadow_capacity_queue"]["queue_hash"] == decision["queue_hash"] for row in statuses)


@pytest.mark.parametrize("tamper", ["no_slots", "no_equity", "unbounded_deadline", "unsigned_source", "wrong_plan"])
def test_self_signed_capacity_is_insufficient_for_canonical_authority(tamper):
    import hashlib
    import json
    from sqlalchemy import create_engine, text
    from server.common.analysis_pool_receipt import canonical_sha256
    from server.engine.shadow_capacity_queue import load_capacity_source
    decision = choose([inventory("a")])
    decision["authorized_plan_ids"] = ["plan-a"]
    authorization = {"strategy_key": "a", "strategy_version": "a-v1", "strategy_version_hash": "a"*64,
        "trade_date": "2026-09-10", "execution_contract": {"maximum_holding_sessions": 5, "policy": SHADOW_EXECUTION_POLICY}}
    if tamper == "no_slots":
        decision["selected"]["initial_available_position_slots"] = 0
    elif tamper == "no_equity":
        decision["account_equity_cny"] = 0
    elif tamper == "unbounded_deadline":
        decision["selected"]["end_session_ordinal"] = 1000000
    decision["queue_hash"] = canonical_sha256({k: v for k, v in decision.items() if k != "queue_hash"})
    body = json.dumps({"run_uid": "canonical", "is_canonical": True, "shadow_capacity_queue": decision})
    root = hashlib.sha256(body.encode()).hexdigest()
    source = {"governance_run_uid": "canonical", "governance_result_hash": root, "queue_hash": decision["queue_hash"]}
    if tamper == "unsigned_source":
        source["governance_result_hash"] = "f"*64
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE st_strategy_governance_run(run_uid TEXT,trade_date TEXT,status TEXT,is_canonical INTEGER,result_json TEXT,result_hash TEXT)")
        connection.execute(text("INSERT INTO st_strategy_governance_run VALUES ('canonical','2026-09-10','COMPLETED',1,:body,:hash)"), {"body": body, "hash": root})
        with pytest.raises(ValueError):
            load_capacity_source(connection, source, plan_id="forged-plan" if tamper == "wrong_plan" else "plan-a",
                                 authorization=authorization, require_current=True)


def test_zero_plan_exit_day_preserves_completed_queue(monkeypatch):
    from server.engine import strategy_center as center, strategy_governance as governance
    from server.engine import shadow_capacity_queue as queue_module
    previous = choose([inventory("a")])
    exited = choose([], 2, previous)
    assert exited["completed_versions"][0]["strategy_key"] == "a"
    monkeypatch.setattr(governance, "load_registry", lambda: [])
    monkeypatch.setattr(center, "current_bound_sql_connection", object)
    monkeypatch.setattr(center, "_dynamic_shadow_trade_session_ordinal", lambda *args, **kwargs: 2)
    calls = []
    def advance(*args, **kwargs):
        calls.append(kwargs["groups"])
        return exited
    monkeypatch.setattr(queue_module, "capacity_queue_for_run", advance)
    _, statuses = center._dynamic_execution_signals(trade_date="2026-09-10", recommendation_rows=[],
                                                   market={}, configs={}, metrics={}, persist_receipts=True)
    assert calls == [[]]
    assert statuses[0]["shadow_capacity_queue"]["completed_versions"] == exited["completed_versions"]
    assert statuses[0]["shadow_capacity_queue"]["authorized_plan_ids"] == []
