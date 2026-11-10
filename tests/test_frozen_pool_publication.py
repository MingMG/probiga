import copy
import json
from datetime import datetime

import pytest
from sqlalchemy import create_engine, text

from biz.analysis import sync_analysis_fast as analysis
from server.api.routers import trading_v3
from server.common import component_release, daily_delivery_control as delivery
from server.common import frozen_strategy_pool_work as publication
from server.common.analysis_pool_receipt import build_pool_manifest, build_publication_receipt, build_score_snapshot, decode_score_snapshot
from server.common.authoritative_market_clock import PRODUCTION_TIMEZONE, StrategyPoolDecisionSlot
from server.engine import strategy_governance as governance, strategy_governance_orchestrator as orchestration
from server.trading_v3.pool_revision_diff import build_pool_revision_diff
from tests.score_snapshot_helpers import scored_publication


DAY, SESSION = "2026-11-10", "2026-11-11"
RUN, PRIOR_RUN, SUCCESSOR_RUN = "1" * 32, "2" * 32, "3" * 32
BUILD, CONTRACT = "4" * 40, "5" * 40
SOURCE, PARENT, STRATEGY = "6" * 64, "7" * 64, "8" * 64
CUTOFF = SESSION + "T08:30:00"
SLOT = StrategyPoolDecisionSlot(
    "PREMARKET_REVISION", DAY, SESSION,
    datetime.fromisoformat(CUTOFF).replace(tzinfo=PRODUCTION_TIMEZONE),
    datetime.fromisoformat(SESSION + "T08:34:00").replace(tzinfo=PRODUCTION_TIMEZONE),
)


def _revision(run_uid, result_hash, stock_code, *, finished_at):
    return {"run_uid": run_uid, "canonical_result_hash": result_hash,
            "trade_date": DAY, "finished_at": finished_at,
            "pools": {"observation": [{"stock_code": stock_code, "strategies": ["trend"]}],
                      "confirmation": [], "tradable": []}, "paper_execution_plan": {"targets": []}}


def _clock(monkeypatch, *values):
    readings = iter(values)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return next(readings).astimezone(tz)

    monkeypatch.setattr(publication, "datetime", Clock)


@pytest.fixture
def prepared(monkeypatch):
    prior = _revision(PRIOR_RUN, "9" * 64, "000001", finished_at=DAY + "T22:39:00")
    revised = _revision(SUCCESSOR_RUN, "a" * 64, "000002", finished_at=SESSION + "T08:42:00")
    initial = {**delivery.daily_session_identity(DAY, CONTRACT),
               "strategy_release_id": STRATEGY, "receipt_uid": PARENT,
               "governance_run_uid": PRIOR_RUN, "strategy_pool": {"root": prior["canonical_result_hash"]},
               "status": "PASS", "frozen_pool_decision": {
                   "schema": "probiga.frozen-pool-decision.v1", "phase": "INITIAL",
                   "execution_session_date": SESSION, "cutoff_at": DAY + "T22:20:00+08:00",
                   "evaluated_at": DAY + "T22:22:00+08:00", "parent_receipt_uid": None, "diff_sha256": None}}
    initial["receipt_sha256"] = delivery.canonical_sha256(initial)
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE trace (stage TEXT NOT NULL)")
        connection.exec_driver_sql(f"CREATE TABLE {delivery.SESSION_TABLE} (session_uid, run_id, trade_date, release_id, strategy_release_id, status, canonical_receipt_uid)")
        connection.exec_driver_sql(f"CREATE TABLE {delivery.RECEIPT_TABLE} (receipt_uid, receipt_json, receipt_sha256)")
        connection.execute(text(f"INSERT INTO {delivery.SESSION_TABLE} VALUES (:session_uid, :run_id, :trade_date, :release_id, :strategy_release_id, :status, :receipt_uid)"), initial)
        connection.exec_driver_sql(f"INSERT INTO {delivery.RECEIPT_TABLE} VALUES (?, ?, ?)",
                                   (PARENT, json.dumps(initial), initial["receipt_sha256"]))

    fixture = decode_score_snapshot(scored_publication(trade_date=DAY)["score_snapshot"])
    rows = [{**row, "flow_input_decision_at": CUTOFF} for row in fixture["scored_rows"]]
    context = {"trade_date": DAY, "decision_known_at": CUTOFF}
    context["context_sha256"] = delivery.canonical_sha256(context)
    core_inputs = {
        "kline": {"stage": "qmt_stock_daily_canonical", "dataset_id": "bars", "root": "b" * 64},
        "turnover": {"stage": "target_turnover_snapshot", "dataset_id": "turn", "root": "c" * 64},
        "financial": {"stage": "stock_finance", "dataset_id": "finance", "root": "d" * 64},
        "announcement": {"stage": "qmt_announcement_premarket_prepare", "dataset_id": "e" * 32,
                         "root": SOURCE, "input_root": "f" * 64},
    }
    envelope = build_score_snapshot(
        trade_date=DAY, decision_at=CUTOFF, run_uid=RUN, build_sha=BUILD,
        analysis_rows=fixture["analysis_rows"], scored_rows=rows, fixed_market_context=context,
        frozen_core_inputs=core_inputs,
    )
    work = {
        "schema": publication.PREMARKET_POOL_WORK_SCHEMA, "run_uid": RUN, "build_sha": BUILD,
        "trade_date": DAY, "parent_receipt_uid": PARENT, "source_evidence_sha256": SOURCE,
        "scoring": {
            "phase": SLOT.phase, "data_trade_date": DAY, "execution_session_date": SESSION,
            "cutoff_at": SLOT.scheduled_at.isoformat(), "evaluated_at": SLOT.evaluated_at.isoformat(),
            "score_snapshot": envelope, "analysis_rows": fixture["analysis_rows"], "recommendation_rows": [],
            "core_inputs": core_inputs,
        },
    }
    monkeypatch.setattr(component_release, "runtime_contract_build_sha", lambda **_: CONTRACT)
    monkeypatch.setattr(governance, "_require_governance_write_transaction", lambda conn: assert_open(conn))

    def log(connection, step):
        assert_open(connection)
        connection.exec_driver_sql("INSERT INTO trace VALUES (?)", (step,))

    def save(_engine, analysis_rows, rec_rows, target, **kwargs):
        log(kwargs["publication_connection"], "save")
        assert target == DAY and kwargs["score_snapshot"] == envelope
        manifest = build_pool_manifest(trade_date=target, analysis_rows=analysis_rows,
                                       recommendation_rows=rec_rows, score_snapshot=envelope)
        return build_publication_receipt(manifest=manifest, run_uid=RUN,
                                         publisher_task_type=publication.PREMARKET_POOL_TASK,
                                         build_sha=BUILD, published_at=SESSION + "T08:40:00", score_snapshot=envelope)

    def activate(connection, *, run_uid, task_type, publication_receipt, decision_known_at):
        log(connection, "activate")
        assert run_uid == RUN and task_type == publication.PREMARKET_POOL_TASK
        assert decision_known_at == SLOT.scheduled_at
        return {"status": "VERIFIED_EMPTY", "target_trade_date": DAY, **{
            key: publication_receipt[key] for key in ("run_uid", "publisher_task_type", "build_sha",
                "analysis_count", "recommendation_count", "executable_count", "canonical_pool_sha256")}}

    def persist(connection, **kwargs):
        log(connection, "governance")
        assert kwargs["expected_prior_governance_run_uid"] == PRIOR_RUN
        assert kwargs["fixed_scoring"]["fixed_market_context"] == context
        return revised

    monkeypatch.setattr(analysis, "_prebind_direct_publication_history", lambda _engine, **kw: log(kw["publication_connection"], "prebind"))
    monkeypatch.setattr(analysis, "save_outputs", save)
    monkeypatch.setattr(analysis, "_finish_direct_publication_history", lambda _engine, **kw: log(kw["publication_connection"], "finish"))
    monkeypatch.setattr(orchestration, "persist_frozen_governance_revision", persist)
    monkeypatch.setattr(governance, "load_completed_governance_revision_pair", lambda a, b: (prior, revised))
    yield engine, work, activate, prior, revised
    engine.dispose()


def assert_open(connection):
    assert connection.in_transaction()


def _publish(connection, work, activate):
    return publication.publish_prepared_premarket_pool(
        connection, run_uid=RUN, work=work, slot=SLOT, external_context=None, activate_analysis_pool=activate,
    )


def test_revision_publishes_exact_activation_and_uses_public_diff_identity(prepared, monkeypatch):
    engine, work, activate, prior, revised = prepared
    _clock(monkeypatch, SLOT.evaluated_at, SLOT.evaluated_at)
    with engine.begin() as connection:
        score, activation, receipt = _publish(connection, work, activate)
        assert connection.exec_driver_sql("SELECT stage FROM trace").scalars().all() == ["prebind", "save", "finish", "activate", "governance"]
        assert connection.exec_driver_sql(f"SELECT canonical_receipt_uid FROM {delivery.SESSION_TABLE}").scalar_one() == PARENT
    assert score["run_uid"] == activation["run_uid"] == receipt["analysis_run_uid"] == RUN
    assert receipt["formal_pool"]["count"] == 0 and receipt["formal_pool"]["status"] == "EMPTY"
    assert receipt["strategy_pool"]["status"] == "ACTIVE"
    assert receipt["api_checks"] == {"strategy_pool": None, "formal_pool": None}
    assert receipt["core_inputs"] == work["scoring"]["core_inputs"]
    old_identity, old_rows = trading_v3._governance_pool_revision_members(prior)
    new_identity, new_rows = trading_v3._governance_pool_revision_members(revised)
    expected = build_pool_revision_diff(prior_identity=old_identity, next_identity=new_identity,
                                        prior_rows=old_rows, next_rows=new_rows, time_field="finished_at")
    assert expected["counts"] == {"added": 1, "removed": 1, "retained": 0}
    assert receipt["frozen_pool_decision"]["diff_sha256"] == expected["diff_sha256"]


@pytest.mark.parametrize(("key", "message"), [("parent_receipt_uid", "INITIAL_POOL"), ("source_evidence_sha256", "source evidence")])
def test_wrong_parent_or_source_cannot_write_a_projection(prepared, monkeypatch, key, message):
    engine, work, activate, *_ = prepared
    _clock(monkeypatch, SLOT.evaluated_at)
    invalid = {**copy.deepcopy(work), key: "0" * 64}
    with pytest.raises((RuntimeError, ValueError), match=message):
        with engine.begin() as connection:
            _publish(connection, invalid, activate)
    with engine.connect() as connection:
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM trace").scalar_one() == 0


def test_deadline_crossing_after_governance_rolls_back_all_staged_writes(prepared, monkeypatch):
    engine, work, activate, *_ = prepared
    _clock(monkeypatch, SLOT.evaluated_at, SLOT.evaluated_at.replace(hour=9, minute=25))
    with pytest.raises(RuntimeError, match="PREMARKET_POOL_PUBLICATION_WINDOW_CLOSED"):
        with engine.begin() as connection:
            _publish(connection, work, activate)
    with engine.connect() as connection:
        assert connection.exec_driver_sql("SELECT COUNT(*) FROM trace").scalar_one() == 0
