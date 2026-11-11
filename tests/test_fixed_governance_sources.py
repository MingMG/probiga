from datetime import datetime

import pytest
from sqlalchemy import create_engine, inspect, text

from server.common import analysis_pool_receipt as pools
from server.engine import strategy_center as center
from server.engine import strategy_shadow_trials as native
from server.trading_v3 import paper_execution


DAY = "2026-11-10"
CUTOFF = "2026-11-10T22:20:00+08:00"
INITIAL_UID = "a" * 32
LATER_UID = "b" * 32
CONTEXT_ROOT = "c" * 64
BUILD_SHA = "d" * 40
POOL_ROOT = "e" * 64


@pytest.fixture
def projection_db(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    columns = (*pools.CANONICAL_RECOMMENDATION_COLUMNS,
               "recommend_status", "ordinary_buy_eligible", "publication_status")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE st_recommended_stocks (" + ", ".join(
            f"`{name}`" for name in columns
        ) + ")"))
        connection.execute(text("""
            CREATE TABLE st_recommended_run_history (
                id INTEGER PRIMARY KEY, run_uid TEXT, trade_date TEXT, status TEXT, finished_at TEXT,
                published_at TEXT, publisher_task_type TEXT, canonical_pool_sha256 TEXT,
                passed INT, total INT, build_sha TEXT, membership_snapshot_date TEXT,
                membership_snapshot_source TEXT, membership_proof_sha256 TEXT
            )
        """))
    monkeypatch.setattr(center, "get_engine", lambda: engine)
    def inspector():
        connection = center.current_bound_sql_connection()
        return inspect(connection if connection is not None else engine)

    monkeypatch.setattr(center, "_table_exists", lambda name: inspector().has_table(name))
    monkeypatch.setattr(center, "_table_columns", lambda name: {row["name"] for row in inspector().get_columns(name)})
    yield engine
    engine.dispose()


def _projection(connection, run_uid, code, score, status="ACTIVE"):
    row = {"stock_code": code, "pick_date": DAY, "publisher_run_uid": run_uid,
           "final_trade_score": score, "candidate_recommend_status": "SUSPENDED",
           "candidate_ordinary_buy_eligible": 0, "recommend_status": "SUSPENDED",
           "ordinary_buy_eligible": 0, "publication_status": status}
    connection.execute(text(
        f"INSERT INTO st_recommended_stocks ({', '.join(row)}) VALUES "
        f"({', '.join(':' + key for key in row)})"
    ), row)
    return row


def _history(connection, run_uid, count, published):
    connection.execute(text("""
        INSERT INTO st_recommended_run_history VALUES (
            NULL, :run_uid, :day, 'done', :published, :published, 'analysis_fast', :pool_root,
            :count, 3, :build, :day, 'qmt', :membership_root
        )
    """), {"run_uid": run_uid, "day": DAY, "published": published, "pool_root": POOL_ROOT,
            "count": count, "build": BUILD_SHA, "membership_root": "f" * 64})


def _mock_signed_analysis(monkeypatch, rows, *, publication_mode="RESEARCH_ONLY"):
    published_at = "2026-11-10T22:26:00"
    receipt = {
        "recommendation_count": len(rows), "analysis_count": 3,
        "canonical_pool_sha256": POOL_ROOT, "build_sha": BUILD_SHA,
        "published_at": published_at, "score_snapshot_sha256": "1" * 64,
        "recommendation_sha256": pools.canonical_sha256({"trade_date": DAY,
            "rows": pools._canonical_rows(rows, columns=pools.CANONICAL_RECOMMENDATION_COLUMNS)}),
        "publication_mode": publication_mode, "score_snapshot_verified": True,
        "score_stock_count": 3, "score_data_blocked_count": 0,
        "score_data_excluded_count": 0, "executable_count": 0,
    }
    snapshot = {"run_uid": INITIAL_UID, "fixed_market_context": {"context_sha256": CONTEXT_ROOT}}

    def read(_connection, target, run_uid, *, decision_known_at):
        assert (target, run_uid, decision_known_at) == (DAY, INITIAL_UID, CUTOFF)
        return receipt, snapshot

    monkeypatch.setattr(native, "_analysis_source", read)
    return published_at


def _load_fixed(connection):
    return center._load_fixed_recommendation_inputs(
        connection, trade_date=DAY, analysis_run_uid=INITIAL_UID,
        cutoff=CUTOFF, market_context_sha256=CONTEXT_ROOT,
    )


def test_fixed_projection_excludes_same_day_other_publishers_and_rechecks_active_hash(
    monkeypatch, projection_db,
):
    with projection_db.begin() as connection:
        initial = _projection(connection, INITIAL_UID, "000001", 77)
        _projection(connection, LATER_UID, "000002", 99)
        published_at = _mock_signed_analysis(monkeypatch, [initial])
        _history(connection, INITIAL_UID, 1, published_at)
        _history(connection, LATER_UID, 1, "2026-11-10T23:55:00")
        with center.bind_sql_connection(connection):
            rows, proof, bundle = _load_fixed(connection)
            assert [row["stock_code"] for row in rows] == ["000001"], proof
            assert proof["publisher_run_uid"] == INITIAL_UID
            assert proof["status"] == "COMPLETED", proof
            assert bundle[1]["run_uid"] == INITIAL_UID
            connection.execute(text("UPDATE st_recommended_stocks SET final_trade_score=81 WHERE publisher_run_uid=:uid"), {"uid": INITIAL_UID})
            assert _load_fixed(connection)[1]["reason"] == "ANALYSIS_FIXED_ACTIVE_PROJECTION_MISMATCH"
            connection.execute(text("UPDATE st_recommended_stocks SET publication_status='PENDING' WHERE publisher_run_uid=:uid"), {"uid": INITIAL_UID})
            assert _load_fixed(connection)[1]["status"] == "BLOCKED"


def test_exact_empty_pool_requires_verified_empty_receipt(monkeypatch, projection_db):
    with projection_db.begin() as connection:
        published_at = _mock_signed_analysis(monkeypatch, [], publication_mode="EMPTY")
        _history(connection, INITIAL_UID, 0, published_at)
        with center.bind_sql_connection(connection):
            rows, proof, _ = _load_fixed(connection)
            assert rows == []
            assert proof["status"] == "COMPLETED"
            source = center._candidate_source_contract(DAY, rows, [], fixed_publication_proof=proof)
            assert source["query_completed"] is True
            _mock_signed_analysis(monkeypatch, [])
            rows, proof, bundle = _load_fixed(connection)
            assert rows == [] and bundle is None
            source = center._candidate_source_contract(DAY, rows, [], fixed_publication_proof=proof)
            assert source["query_completed"] is False
            assert source["status"] == "BLOCKED"


def test_native_analysis_uses_exact_run_and_score_time_not_publication_time(monkeypatch):
    def load(connection, target, *, run_uid):
        assert connection == "transaction" and (target, run_uid) == (DAY, INITIAL_UID)
        return {"published_at": "2026-11-10T22:26:00", "score_snapshot": {}}

    monkeypatch.setattr(native, "load_published_analysis_receipt", load)
    monkeypatch.setattr(native, "decode_score_snapshot", lambda _envelope, **_kwargs: {
        "run_uid": INITIAL_UID, "decision_at": "2026-11-10T22:20:00",
    })
    receipt, _ = native._analysis_source("transaction", DAY, INITIAL_UID, decision_known_at=CUTOFF)
    assert receipt["published_at"] > "2026-11-10T22:20:00"
    with pytest.raises(ValueError, match="SHADOW_ANALYSIS_SCORE_CUTOFF_MISMATCH"):
        native._analysis_source("transaction", DAY, INITIAL_UID, decision_known_at="2026-11-11T08:30:00+08:00")
    with pytest.raises(ValueError, match="SHADOW_ANALYSIS_EXACT_RUN_REQUIRED"):
        native._analysis_source("transaction", DAY, decision_known_at=CUTOFF)


def test_v3_as_of_selection_excludes_late_completion_and_keeps_verification_cutoff(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    strategy = {"evaluator_config": {"model_version": "v3"}, "strategy_key": "sleeve", "current_version": "v3:sleeve"}

    def verify(_connection, *, run, now, **_kwargs):
        assert (run["run_uid"], now) == (INITIAL_UID, datetime(2026, 11, 10, 22, 20))
        return {}, 0.0, True, ""

    monkeypatch.setattr(paper_execution, "_verify_persisted_decision_truth", verify)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_decision_run_v3 (run_uid TEXT, trade_date TEXT, model_version TEXT, status TEXT,
                mode TEXT, decision_at TEXT, created_at TEXT, completed_at TEXT)
        """))
        connection.execute(text("CREATE TABLE st_target_portfolio_v3 (run_uid TEXT, rank_no INT, stock_code TEXT)"))
        connection.execute(text("CREATE TABLE st_alpha_forecast_v3 (run_uid TEXT, forecast_id TEXT)"))
        connection.execute(text("CREATE TABLE st_trade_account_v2 (account_id TEXT)"))
        connection.execute(text("INSERT INTO st_trade_account_v2 VALUES ('paper-main-v2')"))
        connection.execute(text("""
            INSERT INTO st_decision_run_v3 VALUES
              (:initial, :day, 'v3', 'COMPLETED', 'close', '2026-11-10 22:05:00', '2026-11-10 22:10:00', '2026-11-10 22:20:00'),
              (:later, :day, 'v3', 'COMPLETED', 'close', '2026-11-10 22:15:00', '2026-11-10 22:16:00', '2026-11-10 22:21:00'),
              ('no-completion', :day, 'v3', 'COMPLETED', 'close', '2026-11-10 22:16:00', '2026-11-10 22:17:00', NULL)
        """), {"day": DAY, "initial": INITIAL_UID, "later": LATER_UID})
        source = native._v3_source(connection, strategy, DAY, decision_known_at=CUTOFF)
        assert source["run"]["run_uid"] == INITIAL_UID
        with pytest.raises(ValueError, match="SHADOW_V3_COMPLETED_SOURCE_UNAVAILABLE_AS_OF"):
            native._v3_source(connection, strategy, DAY, LATER_UID, decision_known_at=CUTOFF)
    engine.dispose()


def test_fixed_metrics_select_by_actual_known_time(projection_db):
    with projection_db.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_strategy_center_metric (
              strategy_key TEXT, as_of_date TEXT, created_at TEXT, sample_count INT, source TEXT
            )
        """))
        connection.execute(text("""
            INSERT INTO st_strategy_center_metric VALUES
              ('swing','2026-11-09','2026-11-10 22:20:00',21,'frozen'),
              ('swing','2026-11-10','2026-11-10 22:21:00',51,'frozen')
        """))
        with center.bind_sql_connection(connection):
            assert center.load_strategy_metrics(DAY, decision_known_at=CUTOFF)["swing"]["sample_count"] == 21
            revision = "2026-11-11T08:30:00+08:00"
            assert center.load_strategy_metrics(DAY, decision_known_at=revision)["swing"]["sample_count"] == 51
