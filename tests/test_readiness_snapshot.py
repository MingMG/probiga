from threading import Event
from time import monotonic

from server.common.readiness_snapshot import ReadinessSnapshot
from tools.data_quality_check import _bounded_acquisition_check


def test_slow_readiness_does_not_queue_workers_or_block_callers():
    release = Event()
    entered = Event()
    calls = []
    snapshot = ReadinessSnapshot(wait_seconds=0.01)
    def loader():
        calls.append(1)
        entered.set()
        release.wait(2)
        return {"ready": True}
    try:
        started = monotonic()
        for _ in range(3):
            value, status = snapshot.read(loader)
            assert value is None
            assert status["refreshing"]
        assert monotonic() - started < 0.5
        assert entered.is_set() and len(calls) == 1
    finally:
        release.set()
        assert snapshot._done.wait(1)
    value, status = snapshot.read(loader)
    assert value == {"ready": True}
    assert not status["stale"]
    assert len(calls) == 1


def test_expired_ready_result_is_not_returned_as_current():
    snapshot = ReadinessSnapshot(wait_seconds=0.1)
    assert snapshot.read(lambda: {"ready": True})[0] is not None
    snapshot._finished -= 60
    release = Event()
    try:
        value, status = snapshot.read(lambda: release.wait(2))
        assert value is None and status["stale"]
    finally:
        release.set()
        snapshot._done.wait(1)


def test_check_timeout_does_not_hide_next_result():
    release = Event()
    try:
        try:
            _bounded_acquisition_check(lambda: release.wait(2), timeout_seconds=0.01)
        except TimeoutError:
            pass
        else:
            raise AssertionError("slow check did not time out")
        assert _bounded_acquisition_check(lambda: "PASS", timeout_seconds=0.1) == "PASS"
    finally:
        release.set()


def test_http_readiness_uses_killable_probe_and_never_promotes_pending(monkeypatch):
    from server.api.routers import trading_v3
    from types import SimpleNamespace
    calls = []
    def run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(stdout='{"status":"blocked","data":{"paper_ready":false}}')
    monkeypatch.setattr(trading_v3.subprocess, "run", run)
    result = trading_v3._load_readiness_snapshot()
    assert result["data"]["paper_ready"] is False
    assert calls[0][1]["timeout"] == 30
    assert calls[0][0][1] == "-B"
    monkeypatch.setattr(trading_v3, "_READINESS_SNAPSHOT", SimpleNamespace(
        read=lambda loader: (None, {"error_type": None, "refreshing": True, "stale": True})))
    monkeypatch.setattr(trading_v3, "_envelope", lambda data, status: {"data": data, "status": status})
    response = trading_v3.readiness_snapshot()
    assert response["status"] == "blocked"
    assert response["data"]["paper_ready"] is False
    assert response["data"]["real_trading_enabled"] is False
    assert response["data"]["blocks"] == ["READINESS_CHECK_RUNNING"]
