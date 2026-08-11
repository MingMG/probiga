# -*- coding: utf-8 -*-
import threading
from datetime import datetime
from unittest.mock import patch

from sqlalchemy import create_engine, text

from server.api import qmt_live_runtime
from server.api.qmt_live_runtime import _load_tracked_stock_codes


class _FakeThread:
    def __init__(self):
        self.join_timeout = None
        self._alive = True

    def is_alive(self):
        return self._alive

    def join(self, timeout=None):
        self.join_timeout = timeout
        self._alive = False


class _SharedEngine:
    def __init__(self):
        self.disposed = False

    def dispose(self):
        self.disposed = True


def test_load_tracked_stock_codes_dedupes_sources_and_zfills_codes():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE st_user_portfolio (stock_code TEXT)"))
        conn.execute(text("CREATE TABLE st_sim_position (stock_code TEXT, status TEXT)"))
        conn.execute(text("CREATE TABLE st_recommended_stocks (stock_code TEXT, pick_date TEXT)"))
        conn.execute(text("INSERT INTO st_user_portfolio (stock_code) VALUES ('1'), ('000002')"))
        conn.execute(text("INSERT INTO st_sim_position (stock_code, status) VALUES ('000002', 'holding'), ('3', 'closed')"))
        conn.execute(
            text(
                "INSERT INTO st_recommended_stocks (stock_code, pick_date) "
                "VALUES ('4', '2026-07-01'), ('000005', '2026-07-01'), ('6', '2026-06-30')"
            )
        )

    codes = _load_tracked_stock_codes(engine, candidate_limit=20)

    assert codes == ["000001", "000002", "000004", "000005"]


def test_stop_qmt_live_runtime_signals_and_joins_thread():
    thread = _FakeThread()
    stop_event = threading.Event()
    qmt_live_runtime._live_thread = thread
    qmt_live_runtime._stop_event = stop_event

    qmt_live_runtime.stop_qmt_live_runtime(timeout_seconds=0.2)

    assert stop_event.is_set()
    assert thread.join_timeout == 0.2
    assert qmt_live_runtime._live_thread is None
    assert qmt_live_runtime._stop_event is None


def test_worker_reuses_current_engine_and_leaves_disposal_to_lifespan():
    engine = _SharedEngine()
    stop_event = threading.Event()

    with patch("server.api.qmt_live_runtime.get_current_engine", return_value=engine) as get_current_engine, \
         patch("server.api.qmt_live_runtime.get_qmt_live_runtime_config", return_value={
             "poll_seconds": 1,
             "idle_sleep_seconds": 30,
             "trading_hours_only": False,
             "candidate_limit": 60,
             "index_poll_seconds": 60,
         }), \
         patch("server.api.qmt_live_runtime._run_once") as run_once, \
         patch("server.api.qmt_live_runtime._sleep_with_stop", return_value=True):
        qmt_live_runtime._worker(stop_event)

    get_current_engine.assert_called_once_with()
    run_once.assert_called_once_with(engine)
    assert engine.disposed is False


def test_run_once_uses_public_quote_sync_for_tracked_stocks():
    engine = object()
    metadata_engine = object()
    with patch.object(qmt_live_runtime, "get_qmt_live_runtime_config", return_value={
        "candidate_limit": 60,
        "poll_seconds": 10,
        "index_poll_seconds": 60,
    }), patch.object(qmt_live_runtime, "get_engine", return_value=metadata_engine), patch.object(
        qmt_live_runtime, "_load_tracked_stock_codes", return_value=["000001"]
    ) as tracked_mock, patch.object(
        qmt_live_runtime, "read_sql_rows", return_value=[]
    ), patch.object(
        qmt_live_runtime, "sync_market_realtime", return_value={"source": "sina"}
    ) as sync, patch.object(qmt_live_runtime, "step_index_current"):
        qmt_live_runtime._last_index_poll = 0.0
        qmt_live_runtime._run_once(engine)

    tracked_mock.assert_called_once_with(metadata_engine, 60)
    sync.assert_called_once()
    assert sync.call_args.kwargs["source"] == "sina"


def test_run_once_skips_sina_when_qmt_current_snapshot_is_fresh():
    engine = object()
    with patch.object(qmt_live_runtime, "get_qmt_live_runtime_config", return_value={
        "candidate_limit": 60,
        "poll_seconds": 10,
        "index_poll_seconds": 60,
    }), patch.object(
        qmt_live_runtime, "_load_tracked_stock_codes", return_value=["000001"]
    ), patch.object(
        qmt_live_runtime, "read_sql_rows", return_value=[{
            "stock_code": "000001",
            "snapshot_at": datetime.now(),
        }]
    ), patch.object(
        qmt_live_runtime, "sync_market_realtime"
    ) as sync, patch.object(qmt_live_runtime, "step_index_current"):
        qmt_live_runtime._last_index_poll = 0.0
        qmt_live_runtime._run_once(engine)

    sync.assert_not_called()
