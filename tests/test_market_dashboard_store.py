"""Real durable snapshots, failed captures and read-only HTTP consumers."""
from datetime import datetime, timedelta
import os

import pytest

from server.common import market_dashboard_store as store
from server.common.market_dashboard_worker import _publish_board

NOW = datetime(2026, 11, 23, 10, 30)
DAY = NOW.date().isoformat()
TOKEN = "test-owner"


@pytest.fixture
def snapshots(tmp_path, monkeypatch):
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    original_clock = store.clock
    monkeypatch.setattr(store, "clock", lambda value=None: original_clock(value or NOW))
    store.initialize()
    with store.database(write=True) as db, db:
        db.execute("""INSERT INTO dashboard_owner (slot,token,pid,child_pid,lease_until,run_id,completed_units)
            VALUES (1,?,?,?,?,?,0)""", (TOKEN, os.getpid(), os.getpid(), store.iso(NOW+timedelta(seconds=30)), "run1"))
    return tmp_path


def board_value():
    return {"status": "available", "trade_date": DAY, "source": "sm_stock_current",
            "as_of": store.iso(NOW), "as_of_basis": "qmt_snapshot", "sample_size": 10,
            "market_overview": {"status": "available", "trade_date": DAY, "total_count": 10,
                "up_count": 6, "flat_count": 1, "down_count": 3, "is_realtime": True,
                "data_time": store.iso(NOW), "data_source": "sm_stock_current"},
            "rankings": {"gainers": [], "losers": [], "amount": [], "turnover": []},
            "indices": [{"index_code": "000001.SH", "trade_date": DAY, "price": 4000,
                         "as_of": store.iso(NOW), "source": "public.index"}]}


def test_failed_capture_retains_durable_data_identity_and_time(snapshots):
    store.start_component(DAY, "board", "run1", TOKEN)
    _publish_board(DAY, board_value(), TOKEN, closed=False)
    store.finish_component(DAY, "board", "run1", TOKEN, published=2)
    first = store.board(DAY)
    with store.database(write=True) as db, db:
        db.execute("UPDATE dashboard_owner SET run_id='run2'")
    store.start_component(DAY, "board", "run2", TOKEN)
    store.finish_component(DAY, "board", "run2", TOKEN, published=0, error="source_deadline_exceeded")
    retained = store.board(DAY)
    assert retained["version"] == first["version"]
    assert retained["as_of"] == first["as_of"] == store.iso(NOW)
    assert retained["sample_size"] == 10
    assert retained["observation"]["last_error"] == "source_deadline_exceeded"
    assert retained["source_errors"]["board"]["status"] == "FAILED"


def test_expired_source_is_reference_and_is_not_relabelled_realtime(snapshots):
    _publish_board(DAY, board_value(), TOKEN, closed=False)
    value = store.board(DAY, now=NOW+timedelta(seconds=61))
    assert value["status"] == "stale"
    assert value["observation"]["state"] == "expired"
    assert value["observation"]["age_seconds"] == 61
    assert value["market_overview"]["is_realtime"] is False
    assert value["market_overview"]["freshness_status"] == "stale"
    assert value["sample_size"] == 10
    assert value["as_of"] == store.iso(NOW)


def test_missing_ladder_does_not_prevent_new_quote_publication(snapshots):
    _publish_board(DAY, board_value(), TOKEN, closed=False)
    next_value = board_value()
    next_value["market_overview"]["up_count"] = 7
    next_value["market_overview"]["down_count"] = 2
    _publish_board(DAY, next_value, TOKEN, closed=False)
    result = store.board(DAY)
    assert result["market_overview"]["up_count"] == 7
    assert result["indices"][0]["price"] == 4000
    assert "limit_ladder" in result["unavailable_fields"]


def test_missing_amount_remains_explicit_in_published_quote(snapshots):
    value = board_value()
    value["unavailable_fields"] = ["rankings.amount", "indices", "limit_ladder"]
    value["market_overview"]["total_amount"] = None
    _publish_board(DAY, value, TOKEN, closed=False)
    result = store.board(DAY)
    assert "rankings.amount" in result["unavailable_fields"]
    assert result["market_overview"]["total_amount"] is None


def test_digest_corruption_and_expired_ownership_fail_closed(snapshots):
    _publish_board(DAY, board_value(), TOKEN, closed=False)
    with store.database(write=True) as db, db:
        db.execute("UPDATE dashboard_observation SET payload='{}' WHERE component='quotes'")
    with pytest.raises(ValueError, match="digest"):
        store.board(DAY)
    with store.database(write=True) as db, db:
        db.execute("UPDATE dashboard_owner SET lease_until=?", (store.iso(NOW-timedelta(seconds=1)),))
    with pytest.raises(RuntimeError, match="lease"):
        _publish_board(DAY, board_value(), TOKEN, closed=False)


def test_current_http_routes_read_only_published_snapshots(snapshots, monkeypatch):
    from server.api.routers import market_home
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW.replace(tzinfo=tz)
    monkeypatch.setattr(market_home, "datetime", Clock)
    def forbidden(*args, **kwargs):
        raise AssertionError("HTTP read triggered source acquisition or SQL")
    for name in ("get_engine", "get_current_engine", "get_kline_engine", "read_live_indices",
                 "read_live_turnover", "read_live_liquidity", "build_market_board", "build_sector_heatmap", "_read_event_page"):
        monkeypatch.setattr(market_home, name, forbidden)
    _publish_board(DAY, board_value(), TOKEN, closed=False)
    for kind in ("concept", "industry"):
        for mode in ("change", "net_inflow", "five_day_return"):
            store.publish(DAY, f"heat:{kind}:{mode}", {"status": "available", "rows": []},
                          source_at=store.iso(NOW), basis="source", token=TOKEN)
    store.publish(DAY, "events", {"requested_date": DAY, "events": [], "next_cursor": None},
                  source_at=None, basis="received", token=TOKEN)
    store.publish(DAY, "liquidity", {"status": "available", "trade_date": DAY, "turnover_median_pct": 2},
                  source_at=store.iso(NOW), basis="source", token=TOKEN)
    path = store._path()
    before = path.read_bytes()
    assert market_home.market_board(DAY)["sample_size"] == 10
    assert market_home.sector_heatmap(DAY)["concept"]["change"]["status"] == "available"
    assert market_home.market_liquidity(DAY)["turnover_median_pct"] == 2
    assert market_home.monitor_events(DAY, before=None)["events"] == []
    assert path.read_bytes() == before


def test_receipt_time_is_explicit_when_vendor_source_time_is_unknown(snapshots):
    store.publish(DAY, "ladder", {"trade_date": DAY, "limit_up_count": 0},
                  source_at=None, basis="received", token=TOKEN)
    _, meta = store.read(DAY)[0]["ladder"]
    assert meta["source_at"] is None
    assert meta["age_basis"] == "received"
    assert meta["received_at"] == store.iso(NOW)


def test_quotes_publish_before_slow_independent_index_capture(snapshots, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from server.api.routers import market_home
    from server.common import market_dashboard_worker as worker
    held, release = Event(), Event()
    def indices(*args, **kwargs):
        held.set()
        assert release.wait(3), "index test deadline"
        return []
    monkeypatch.setattr(worker, "_target", lambda: (DAY, NOW, True, NOW+timedelta(days=1)))
    monkeypatch.setattr(market_home, "_live_quotes", lambda *a: ([], None))
    monkeypatch.setattr(market_home, "build_market_board", lambda *a, **kw: board_value())
    monkeypatch.setattr(market_home, "read_live_indices", indices)
    monkeypatch.setattr(market_home, "read_live_turnover", lambda *a, **kw: None)
    monkeypatch.setattr(market_home, "capture_live_ladder", lambda *a, **kw: None)
    monkeypatch.setattr(market_home, "build_sector_heatmap", lambda *a, **kw: {})
    monkeypatch.setattr(market_home, "_capture_liquidity", lambda *a: {})
    monkeypatch.setattr(market_home, "_breadth_history", lambda *a: [])
    monkeypatch.setattr(market_home, "_read_event_page", lambda *a: {"requested_date": DAY, "events": []})
    from time import monotonic, sleep
    with ThreadPoolExecutor(max_workers=1) as pool:
        running = pool.submit(worker.capture_once, TOKEN)
        try:
            assert held.wait(1)
            deadline = monotonic()+2
            while not store.read(DAY)[0].get("quotes") and monotonic() < deadline:
                sleep(.01)
            assert store.board(DAY)["sample_size"] == 10
            assert not running.done(), "slow domain did not remain isolated"
        finally:
            release.set()
        running.result(timeout=3)
    assert store.runtime()["completed_units"] == store.runtime()["total_units"] == 8


def test_wrong_payload_date_and_future_time_are_rejected(snapshots):
    with pytest.raises(ValueError, match="session"):
        store.publish(DAY, "events", {"requested_date": "2026-11-20"}, source_at=None, basis="received", token=TOKEN)
    with pytest.raises(ValueError, match="future"):
        store.publish(DAY, "quotes", {}, source_at=store.iso(NOW+timedelta(seconds=1)), basis="source", token=TOKEN)


def test_lunch_reuses_morning_observation_and_expires_after_reopen(snapshots):
    store.publish(DAY, "events", {"events": []}, source_at=None, basis="received", token=TOKEN)
    with store.database(write=True) as db, db:
        db.execute("UPDATE dashboard_observation SET captured_at=?", (DAY+"T11:30:00+08:00",))
    assert store.component(DAY, "events", now=NOW.replace(hour=12))["observation"]["state"] == "paused"
    assert store.component(DAY, "events", now=NOW.replace(hour=13))["observation"]["state"] == "expired"


def test_websocket_real_handshake_readback_reconnect_and_origin(snapshots, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from starlette.websockets import WebSocketDisconnect
    from server.api import market_dashboard_stream as stream
    monkeypatch.setattr(stream, "hub", stream.SnapshotHub(interval=.01, heartbeat=.02))
    monkeypatch.setattr(stream, "validate_admin_request", lambda request: None)
    _publish_board(DAY, board_value(), TOKEN, closed=False)
    app = FastAPI()
    app.include_router(stream.router, prefix="/api")
    with TestClient(app) as client:
        with client.websocket_connect("/api/monitor/stream?date="+DAY) as socket:
            message = socket.receive_json()
            assert message["initial"] is True
            assert message["components"]["snapshot"]["sample_size"] == 10
            version = message["components"]["snapshot"]["version"]
            assert socket.receive_json()["type"] == "heartbeat"
        with client.websocket_connect("/api/monitor/stream?date="+DAY) as socket:
            assert socket.receive_json()["components"]["snapshot"]["version"] == version
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/api/monitor/stream?date="+DAY, headers={"origin": "https://untrusted.example"}):
                pass


def test_bounded_stream_queue_merges_unread_components():
    import asyncio
    from server.api.market_dashboard_stream import SnapshotHub
    queue = asyncio.Queue(maxsize=1)
    SnapshotHub.offer(queue, {"components": {"snapshot": {"version": "a"}}})
    SnapshotHub.offer(queue, {"components": {"heatmap": {"version": "b"}}})
    SnapshotHub.offer(queue, {"components": {"snapshot": {"version": "c"}}})
    assert queue.qsize() == 1
    assert queue.get_nowait()["components"] == {"snapshot": {"version": "c"}, "heatmap": {"version": "b"}}


def test_external_observation_publishes_its_own_receipt_without_main_calendar(snapshots, monkeypatch):
    from types import SimpleNamespace
    from server.common import market_dashboard_worker as worker
    from tools import env_config, refresh_external_market_context as capture
    monkeypatch.setattr(env_config, "load_project_env", lambda: None)
    monkeypatch.setattr(env_config, "create_tool_engine", lambda **kw: SimpleNamespace(dispose=lambda: None))
    def forbidden():
        raise AssertionError("external capture waited for the A-share calendar")
    monkeypatch.setattr(worker, "_target", forbidden)
    monkeypatch.setattr(capture, "capture_current_observation", lambda engine: {
        "context_date": DAY, "snapshot_id": "verified-external-batch", "available_count": 41,
        "expected_count": 41, "status": "COMPLETE", "persisted_sha256": "a"*64})
    worker.capture_external(TOKEN)
    worker._terminal(TOKEN)
    assert store.runtime()["total_units"] == store.runtime()["completed_units"] == 1
    assert store.runtime()["state"] == "COMPLETE"
    assert store.component(DAY, "external")["snapshot_id"] == "verified-external-batch"
    assert store.runtime()["current_units"][0]["published"] == 41
