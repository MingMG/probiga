from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, event, text

from biz.intelligence import exposure_risk_watch
from server.api.routers import holding_strategy, hot_data
from server.common.pit_facts import CNINFO_EVENT_SOURCE
from server.common.qmt_announcement_pit import (
    AnnouncementCatalog,
    AnnouncementCheckpoint,
)


def test_exposure_watch_schema_time_consumes_the_same_job_deadline(monkeypatch, tmp_path):
    clock = {"now": 0.0}
    stages = []

    def schema_validation(_engine):
        clock["now"] = 1201.0

    monkeypatch.setattr(
        exposure_risk_watch, "assert_announcement_pit_schema_prepared", schema_validation,
    )
    with pytest.raises(exposure_risk_watch.ExposureRiskWatchError) as failed:
        exposure_risk_watch.run_exposure_event_risk_watch(
            object(), checkpoint_root=tmp_path,
            monotonic_fn=lambda: clock["now"], stage_fn=stages.append,
        )
    assert failed.value.stage == "exposure_scope"
    assert failed.value.reason == "JOB_DEADLINE_EXPIRED"
    assert stages == ["schema_validation", "exposure_scope"]
    assert not list(tmp_path.iterdir())


def test_exposure_universe_contains_portfolio_and_latest_formal_pools_only():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE st_user_portfolio (stock_code TEXT, is_holding INTEGER)"
        ))
        connection.execute(text("""
            CREATE TABLE st_strategy_governance_run (
                run_uid TEXT, trade_date TEXT, run_revision INTEGER,
                created_at TEXT, status TEXT, is_canonical INTEGER
            )
        """))
        connection.execute(text("""
            CREATE TABLE st_strategy_pool_snapshot (
                run_uid TEXT, stock_code TEXT, pool_level TEXT
            )
        """))
        connection.execute(text(
            "INSERT INTO st_user_portfolio VALUES ('000001',1),('600000',0)"
        ))
        connection.execute(text("""
            INSERT INTO st_strategy_governance_run VALUES
            ('old','2026-10-09',1,'2026-10-09 22:00:00','COMPLETED',1),
            ('new','2026-10-10',2,'2026-10-10 22:00:00','COMPLETED',1)
        """))
        connection.execute(text("""
            INSERT INTO st_strategy_pool_snapshot VALUES
            ('old','300001','TRADABLE'),
            ('new','000001','CONFIRMATION'),
            ('new','688001','TRADABLE'),
            ('new','300002','OBSERVATION')
        """))

    universe = exposure_risk_watch.load_exposure_universe(engine)

    assert universe == {
        "000001": ["CANONICAL_CONFIRMATION", "USER_HOLDING"],
        "600000": ["USER_WATCHLIST"],
        "688001": ["CANONICAL_TRADABLE"],
    }


def test_deadline_after_named_lock_acquisition_releases_the_session_lock():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    cutoff = datetime(2026, 10, 11, 10, 0)
    state = {"locked": False, "now": cutoff}
    # Execute real SQL and retain session-lock state independently of SQL
    # rollback, like MySQL GET_LOCK. Only the lock functions are emulated.
    engine.dialect.name = "mysql"

    def register_functions(dbapi_connection, _record):
        def acquire(_name, _timeout):
            state["locked"] = True
            state["now"] = datetime(2026, 10, 11, 10, 21)
            return 1

        def release(_name):
            state["locked"] = False
            return 1

        dbapi_connection.create_function("GET_LOCK", 2, acquire)
        dbapi_connection.create_function("RELEASE_LOCK", 1, release)

    event.listen(engine, "connect", register_functions)
    catalog = AnnouncementCatalog(
        batch_id="scope", manifest_hash="a" * 64, member_set_hash="b" * 64,
        codes=("000001",), qmt_by_code={"000001": "000001.SZ"},
    )
    with pytest.raises(exposure_risk_watch.ExposureRiskWatchError) as failed:
        exposure_risk_watch._publish_batch(
            engine, batch_id="batch", batch_root_hash="c" * 64,
            entries=[{"stock_code": "000001", "source_response_hash": "d" * 64}],
            fact_cutoff_at=cutoff, received_at=cutoff, window_start=cutoff.date(),
            catalog=catalog, results={"000001": []}, receipts={"000001": {}},
            scope={}, deadline_at=datetime(2026, 10, 11, 10, 20),
            now_fn=lambda: state["now"],
        )
    assert failed.value.stage == "database_lock_commit"
    assert state["locked"] is False
    engine.dispose()


def test_checkpoint_progress_distinguishes_partial_from_complete(tmp_path):
    cutoff = datetime(2026, 10, 11, 10, 0)
    catalog = AnnouncementCatalog(
        batch_id="scope",
        manifest_hash="a" * 64,
        member_set_hash="b" * 64,
        codes=("000001", "600000"),
        qmt_by_code={"000001": "000001.SZ", "600000": "600000.SH"},
    )
    checkpoint = AnnouncementCheckpoint.open(
        tmp_path,
        batch_id="batch",
        fact_cutoff_at=cutoff,
        window_start=cutoff.date(),
        window_end=cutoff.date(),
        catalog=catalog,
        resume=False,
        source=CNINFO_EVENT_SOURCE,
        manifest_extra={"capture_purpose": exposure_risk_watch.CAPTURE_PURPOSE},
    )
    checkpoint.save("000001", [])

    partial = exposure_risk_watch.read_exposure_watch_checkpoint_progress(
        tmp_path
    )

    assert partial["status"] == "RUNNING"
    assert partial["completed_units"] == 1
    assert partial["current_unit"] == "600000"
    assert partial["checkpoint_complete"] is False

    checkpoint.save("600000", [])
    checkpoint.mark_complete(batch_root_hash="c" * 64, received_at=cutoff)
    complete = exposure_risk_watch.read_exposure_watch_checkpoint_progress(
        tmp_path
    )

    assert complete["status"] == "COMPLETE"
    assert complete["completed_units"] == complete["total_units"] == 2
    assert complete["checkpoint_complete"] is True


def test_exposure_publish_is_atomic_when_one_coverage_write_fails(monkeypatch):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE probe (stock_code TEXT)"))
    catalog = AnnouncementCatalog(
        batch_id="scope",
        manifest_hash="a" * 64,
        member_set_hash="b" * 64,
        codes=("000001", "600000"),
        qmt_by_code={"000001": "000001.SZ", "600000": "600000.SH"},
    )
    monkeypatch.setattr(
        exposure_risk_watch,
        "append_event_revision",
        lambda *_args, **_kwargs: SimpleNamespace(
            revision_id="c" * 64, content_hash="d" * 64
        ),
    )

    def coverage(connection, *, stock_code, watermark_evidence, **_kwargs):
        connection.execute(
            text("INSERT INTO probe VALUES (:stock_code)"),
            {"stock_code": stock_code},
        )
        if stock_code == "600000":
            raise RuntimeError("second member failed")
        return SimpleNamespace(
            source_response_hash=watermark_evidence["source_response_hash"],
            coverage_id="e" * 64,
        )

    monkeypatch.setattr(exposure_risk_watch, "append_source_coverage", coverage)
    cutoff = datetime(2026, 10, 11, 10, 0)
    entries = [
        {"stock_code": "000001", "source_response_hash": "1" * 64},
        {"stock_code": "600000", "source_response_hash": "2" * 64},
    ]
    with pytest.raises(RuntimeError, match="second member failed"):
        exposure_risk_watch._publish_batch(
            engine,
            batch_id="batch",
            batch_root_hash="f" * 64,
            entries=entries,
            fact_cutoff_at=cutoff,
            received_at=cutoff,
            window_start=cutoff.date(),
            catalog=catalog,
            results={"000001": [], "600000": []},
            receipts={"000001": {}, "600000": {}},
            scope={"scope_hash": "a" * 64},
            deadline_at=datetime(2026, 10, 11, 10, 20),
            now_fn=lambda: cutoff,
        )
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM probe")).scalar_one() == 0


def test_committed_coverage_recovery_accepts_only_the_exact_complete_scope():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_pit_source_coverage (
                stock_code TEXT, coverage_id TEXT, batch_id TEXT,
                fact_kind TEXT, source TEXT, coverage_status TEXT
            )
        """))
        connection.execute(text("""
            INSERT INTO st_pit_source_coverage VALUES
            ('000001','coverage-a','batch','event','cninfo.announcement','COMPLETE'),
            ('600000','coverage-b','batch','event','cninfo.announcement','COMPLETE')
        """))
    catalog = AnnouncementCatalog(
        batch_id="scope",
        manifest_hash="a" * 64,
        member_set_hash="b" * 64,
        codes=("000001", "600000"),
        qmt_by_code={"000001": "000001.SZ", "600000": "600000.SH"},
    )

    assert exposure_risk_watch._published_coverage_ids(
        engine, batch_id="batch", catalog=catalog
    ) == ["coverage-a", "coverage-b"]
    exposure_risk_watch._require_committed_batch_coverage(
        engine, batch_id="batch", catalog=catalog,
        expected_ids=["coverage-a", "coverage-b"],
    )
    with pytest.raises(exposure_risk_watch.ExposureRiskWatchError, match="COMMITTED_COVERAGE_DIFFERS"):
        exposure_risk_watch._require_committed_batch_coverage(
            engine, batch_id="batch", catalog=catalog,
            expected_ids=["coverage-a", "different-coverage"],
        )

    with engine.begin() as connection:
        connection.execute(text(
            "DELETE FROM st_pit_source_coverage WHERE stock_code='600000'"
        ))
    assert exposure_risk_watch._published_coverage_ids(
        engine, batch_id="batch", catalog=catalog
    ) == []
    with pytest.raises(exposure_risk_watch.ExposureRiskWatchError, match="COMMITTED_COVERAGE_DIFFERS"):
        exposure_risk_watch._require_committed_batch_coverage(
            engine, batch_id="batch", catalog=catalog,
            expected_ids=["coverage-a", "coverage-b"],
        )

    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO st_pit_source_coverage VALUES
            ('600000','coverage-b','batch','event','cninfo.announcement','COMPLETE'),
            ('000001','coverage-duplicate','batch','event','cninfo.announcement','COMPLETE')
        """))
    assert exposure_risk_watch._published_coverage_ids(
        engine, batch_id="batch", catalog=catalog
    ) == []


def _holding_dependencies(monkeypatch, overlay):
    def latest_row(_engine, **kwargs):
        if kwargs["table_name"] == "st_recommended_stocks":
            return {
                "pick_date": "2026-10-11",
                "signal_status": "WATCH",
                "main_wave_signal": "WATCH",
            }, ""
        return {
            "analysis_date": "2026-10-11",
            "event_risk_level": "LOW",
            "recommend_status": "HOLD",
        }, ""

    monkeypatch.setattr(holding_strategy, "_latest_pit_row", latest_row)
    monkeypatch.setattr(
        holding_strategy,
        "_daily_price_context",
        lambda *_args, **_kwargs: ({
            "latest_price": 10.0,
            "price_trade_date": "2026-10-11",
            "same_session": True,
            "ma20": 9.5,
        }, ""),
    )
    monkeypatch.setattr(
        holding_strategy,
        "_current_event_risk_overlay",
        lambda *_args, **_kwargs: dict(overlay),
    )


@pytest.mark.parametrize(
    ("level", "expected"),
    (("CRITICAL", "SELL"), ("HIGH", "REDUCE")),
)
def test_current_official_event_overrides_persisted_low_risk(
    monkeypatch, level, expected
):
    _holding_dependencies(monkeypatch, {
        "status": "AVAILABLE",
        "event_risk_level": level,
        "triggered_events": [{"title": "official risk", "level": level}],
    })

    decision = holding_strategy.evaluate_watchlist_holding_exit_at_cutoff(
        object(), "000001", "2026-10-11", "2026-10-11T10:00:00+08:00"
    )

    assert decision["exit_intent"] == expected
    assert decision["evidence"]["current_event_risk"]["event_risk_level"] == level


def test_unproven_current_event_coverage_cannot_authorize_hold(monkeypatch):
    _holding_dependencies(monkeypatch, {
        "status": "DATA_BLOCKED",
        "event_risk_level": "UNKNOWN",
        "reason": "PIT_EVENT_COVERAGE_UNPROVEN",
        "triggered_events": [],
    })

    decision = holding_strategy.evaluate_watchlist_holding_exit_at_cutoff(
        object(), "000001", "2026-10-11", "2026-10-11T10:00:00+08:00"
    )

    assert decision["exit_intent"] == "WAIT_DATA"
    assert "缺失监控" in decision["reason"]


def test_recommendation_overlay_blocks_a_new_critical_official_event(monkeypatch):
    class Batch:
        manifest_hash = "a" * 64
        facts = {"000001": [{
            "title": "关于收到立案调查通知的公告",
            "event_revision_id": "b" * 64,
        }]}

        @staticmethod
        def status_for(_code):
            return "AVAILABLE"

        @staticmethod
        def reason_for(_code):
            return ""

    from server.common import pit_facts

    monkeypatch.setattr(pit_facts, "load_event_facts", lambda *_args, **_kwargs: Batch())
    monkeypatch.setattr(hot_data, "get_engine", lambda: object())
    rows = [{
        "stock_code": "000001",
        "recommend_status": "ALLOW",
        "signal_status": "BUY_READY",
        "chase_risk_status": "ALLOW",
        "ordinary_buy_eligible": 1,
    }]

    summary = hot_data._overlay_current_recommendation_event_risk(
        rows, cutoff=datetime(2026, 10, 11, 10, 0)
    )

    assert summary["critical_count"] == 1
    assert rows[0]["recommend_status"] == "BLOCK"
    assert rows[0]["ordinary_buy_eligible"] == 0
    assert rows[0]["current_event_risk_level"] == "CRITICAL"


def test_scheduler_runs_exposure_watch_hourly_in_weekend_alert_lane():
    from server.api import scheduler_runtime
    from server.common.exposure_risk_watch_task_contract import TASK
    from tools.ensure_quality_gate import TASKS
    from tools.qmt_host_ownership_contract import LINUX_PROVIDER_TASKS_BY_TYPE

    task = next(
        item for item in TASKS if item["task_type"] == "exposure_event_risk_watch"
    )

    assert task == TASK
    assert LINUX_PROVIDER_TASKS_BY_TYPE[task["task_type"]] == TASK
    assert scheduler_runtime.scheduler_task_host_owner(task) == "linux_standalone"
    assert task["interval_minutes"] == 60
    assert task["enabled"] == 1
    assert task["script_path"] == "tools/run_exposure_event_risk_watch.py"
    assert task["task_type"] not in scheduler_runtime.NON_TRADING_DAY_SKIP_TYPES
    assert task["task_type"] in scheduler_runtime.ALERT_LANE_TASK_TYPES
