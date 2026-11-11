import hashlib
import json
from contextlib import contextmanager
from datetime import date

import pytest
from sqlalchemy import create_engine, text

from server.engine import strategy_governance as governance
from server.engine import strategy_governance_orchestrator as orchestration


DAY = "2026-11-10"
PARENT = "1" * 32
SUCCESSOR = "2" * 32
ANALYSIS = "3" * 32
BUILD = "4" * 40
SCORE_ROOT = "5" * 64
INITIAL_RECEIPT = "6" * 64
REVISED_RECEIPT = "7" * 64


class _SQLiteConnection:
    """Use actual SQLite writes, adapting MySQL lock syntax at the fixture boundary."""

    def __init__(self, connection, lock_log):
        self.connection = connection
        self.lock_log = lock_log

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def execution_options(self, *, isolation_level):
        assert isolation_level == "REPEATABLE READ"
        return self

    def execute(self, statement, params=None):
        sql = str(statement)
        for operation in ("GET_LOCK", "RELEASE_LOCK"):
            if sql.startswith(f"SELECT {operation}('probiga_strategy_governance_daily_v1'"):
                self.lock_log.append(operation)
                return self.connection.execute(text("SELECT 1"))
        if sql.endswith(" FOR UPDATE"):
            statement = text(sql.removesuffix(" FOR UPDATE"))
        return self.connection.execute(statement, params)


class _SQLiteGovernanceEngine:
    def __init__(self, engine):
        self.engine = engine
        self.lock_log = []

    @contextmanager
    def connect(self):
        with self.engine.connect() as connection:
            yield _SQLiteConnection(connection, self.lock_log)


@pytest.fixture
def revision_db(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'revision.db'}")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_strategy_governance_run (
                run_uid TEXT PRIMARY KEY, trade_date TEXT, run_revision INTEGER,
                created_at TEXT, input_hash TEXT, decision_hash TEXT,
                build_commit_sha TEXT, status TEXT, is_canonical INTEGER,
                result_json TEXT, result_hash TEXT, input_ready INTEGER
            )
        """))
        connection.execute(text("CREATE TABLE score_projection (run_uid TEXT PRIMARY KEY)"))
        connection.execute(text("CREATE TABLE daily_pointer (trade_date TEXT PRIMARY KEY, receipt_uid TEXT)"))
        connection.execute(text("""
            INSERT INTO st_strategy_governance_run (run_uid, trade_date, run_revision,
                created_at, status, is_canonical) VALUES (:parent, :day, 1, 'initial', 'COMPLETED', 1)
        """), {"parent": PARENT, "day": DAY})
        connection.execute(text("INSERT INTO daily_pointer VALUES (:day, :uid)"), {"day": DAY, "uid": INITIAL_RECEIPT})

    def no_secondary_connection():
        pytest.fail("governance opened another database connection within the staged transaction")

    monkeypatch.setattr(governance, "get_engine", no_secondary_connection)
    monkeypatch.setattr(governance, "_validate_canonical_governance_result", lambda _result: None)
    monkeypatch.setattr(governance, "validate_funding_checkpoint_manifest_contract", lambda _result: None)
    yield _SQLiteGovernanceEngine(engine), engine
    engine.dispose()


def _score_input():
    return {
        "fixed_scoring": {"trade_date": DAY, "analysis_run_uid": ANALYSIS,
            "decision_known_at": "2026-11-11T08:30:00+08:00",
            "fixed_market_context": {"context_sha256": "8" * 64}},
        "prepared_analysis_publication": {"trade_date": DAY, "run_uid": ANALYSIS,
            "build_sha": BUILD, "score_snapshot_sha256": SCORE_ROOT},
        "expected_prior_governance_run_uid": PARENT,
        "expected_build_sha": BUILD,
    }


def _write_test_governance(**kwargs):
    connection = kwargs["_governance_connection"]
    assert kwargs["analysis_run_uid"] == ANALYSIS
    assert kwargs["prepared_analysis_publication"]["score_snapshot_sha256"] == SCORE_ROOT
    assert governance._db_read("SELECT run_uid FROM score_projection") == [{"run_uid": ANALYSIS}]
    parent = governance._locked_canonical_governance_parent(
        connection, trade_date=kwargs["trade_date"], expected_run_uid=kwargs["expected_prior_governance_run_uid"],
    )
    payload = {
        "status": "ok", "result_mode": "CANONICAL_PERSISTED", "is_canonical": True,
        "run_uid": SUCCESSOR, "trade_date": DAY, "input_hash": "9" * 64, "decision_hash": "a" * 64,
        "run_revision": parent["run_revision"] + 1, "supersedes_run_uid": parent["run_uid"],
        "build_commit_sha": BUILD, "idempotent_replay": False,
        "execution_disposition": governance.FROZEN_POOL_REVISION_EXECUTION_DISPOSITION,
        "decision_contract_version": "strategy-governance-decision.v7", "summary": {},
        "candidate_source": {"publication_proof": {"status": "COMPLETED",
            "publisher_run_uid": ANALYSIS, "analysis_score_snapshot_sha256": SCORE_ROOT}},
        "automatic_real_order_submission": False, "real_order_authority": False,
        "allocations": [{"target_type": "CASH", "simulated_weight_pct": 100.0, "real_order_authority": False}],
    }
    result_json = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    result_hash = hashlib.sha256(result_json.encode()).hexdigest()
    connection.execute(text("UPDATE st_strategy_governance_run SET is_canonical=0 WHERE run_uid=:uid"), {"uid": parent["run_uid"]})
    connection.execute(text("""
        INSERT INTO st_strategy_governance_run VALUES (:run_uid, :trade_date, :run_revision,
            'revision', :input_hash, :decision_hash, :build_commit_sha, 'COMPLETED', 1,
            :result_json, :result_hash, 1)
    """), {**payload, "result_json": result_json, "result_hash": result_hash})
    return {**payload, "canonical_result_hash": result_hash}


def _stage_score(connection):
    connection.execute(text("INSERT INTO score_projection VALUES (:uid)"), {"uid": ANALYSIS})


def _swap_receipt(connection, *, expected):
    swapped = connection.execute(text("""
        UPDATE daily_pointer SET receipt_uid=:revision WHERE trade_date=:day AND receipt_uid=:parent
    """), {"day": DAY, "revision": REVISED_RECEIPT, "parent": expected})
    if swapped.rowcount != 1:
        raise RuntimeError("daily receipt parent changed")


@pytest.mark.parametrize("receipt_parent_matches", [True, False])
def test_score_governance_and_receipt_commit_or_rollback_together(
    revision_db, monkeypatch, receipt_parent_matches,
):
    governed_engine, raw_engine = revision_db
    monkeypatch.setattr(governance, "governance_snapshot", _write_test_governance)

    def publish():
        with governance.governance_write_transaction(governed_engine) as connection:
            _stage_score(connection)
            result = orchestration.persist_frozen_governance_revision(connection, **_score_input())
            assert result["run_uid"] == SUCCESSOR and result["run_revision"] == 2
            _swap_receipt(connection, expected=INITIAL_RECEIPT if receipt_parent_matches else "stale")

    if receipt_parent_matches:
        publish()
    else:
        with pytest.raises(RuntimeError, match="daily receipt parent changed"):
            publish()
    assert governed_engine.lock_log == ["GET_LOCK", "RELEASE_LOCK"]
    assert governance.current_bound_sql_connection() is None
    with raw_engine.connect() as connection:
        assert connection.execute(text("SELECT run_uid FROM score_projection")).scalar() == (
            ANALYSIS if receipt_parent_matches else None
        )
        assert connection.execute(text("SELECT run_uid FROM st_strategy_governance_run WHERE is_canonical=1")).scalar() == (
            SUCCESSOR if receipt_parent_matches else PARENT
        )
        assert connection.execute(text("SELECT receipt_uid FROM daily_pointer")).scalar() == (
            REVISED_RECEIPT if receipt_parent_matches else INITIAL_RECEIPT
        )


def test_revision_rejects_changed_parent_and_unowned_transaction(revision_db, monkeypatch):
    governed_engine, raw_engine = revision_db
    monkeypatch.setattr(governance, "governance_snapshot", _write_test_governance)
    with raw_engine.begin() as connection:
        with pytest.raises(RuntimeError, match="requires its caller-owned"):
            orchestration.persist_frozen_governance_revision(connection, **_score_input())

    stale = {**_score_input(), "expected_prior_governance_run_uid": "b" * 32}
    with pytest.raises(governance.GovernanceEvidenceNotReady, match="FROZEN_GOVERNANCE_PARENT_CHANGED"):
        with governance.governance_write_transaction(governed_engine) as connection:
            _stage_score(connection)
            orchestration.persist_frozen_governance_revision(connection, **stale)
    with raw_engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM score_projection")).scalar() == 0
        assert connection.execute(text("SELECT run_uid FROM st_strategy_governance_run WHERE is_canonical=1")).scalar() == PARENT


def test_research_revision_is_not_an_order_authority(revision_db, monkeypatch):
    from server.trading_v3 import paper_execution

    governed_engine, raw_engine = revision_db
    bootstrap_calls = []
    monkeypatch.setattr(paper_execution, "materialize_dynamic_shadow_bootstrap_orders",
                        lambda connection, **kwargs: bootstrap_calls.append((connection, kwargs)))
    monkeypatch.setattr(governance, "governance_snapshot", _write_test_governance)
    with governance.governance_write_transaction(governed_engine) as connection:
        _stage_score(connection)
        revised = orchestration.persist_frozen_governance_revision(connection, **_score_input())
        plan = {"shadow_capacity_queue": {"authorized_plan_ids": ["otherwise-actionable"]}}
        governance._materialize_governance_shadow_orders(connection, {**revised, **plan})
        assert bootstrap_calls == []
        governance._materialize_governance_shadow_orders(connection, {"run_uid": PARENT, **plan})
        assert bootstrap_calls == [(connection, {"plan_ids": ["otherwise-actionable"], "governance_run_uid": PARENT})]
        _swap_receipt(connection, expected=INITIAL_RECEIPT)

    with raw_engine.connect() as connection:
        receipt, reason = paper_execution._canonical_governance_buy_receipt(
            connection, trade_date=date.fromisoformat(DAY), stock_code="000001", strategy_keys=["swing"],
        )
    assert receipt is None and reason == "GOVERNANCE_FROZEN_REVISION_RESEARCH_ONLY"
