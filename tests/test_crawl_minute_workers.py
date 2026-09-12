"""Real threads exercise bounded source work and single-owner publication."""
from __future__ import annotations

from collections import Counter
from datetime import datetime
from threading import Barrier, Lock, get_ident

import pytest

from tools import crawl_minute_kline as crawler


@pytest.fixture(autouse=True)
def fast_pacing(monkeypatch):
    monkeypatch.setattr(crawler, "DELAY", 0)
    monkeypatch.setattr(crawler, "JITTER", 0)
    monkeypatch.setattr(crawler, "BATCH_EVERY", 0)
    monkeypatch.setattr(crawler, "RETRY_DELAY", 0)


def test_parallel_fetch_uses_six_independent_sessions_and_retries_same_stock(monkeypatch):
    barrier = Barrier(crawler.FETCH_WORKERS)
    lock = Lock()
    sessions = {}
    attempts = Counter()
    closed = []
    main_thread = get_ident()

    class Session:
        def close(self):
            closed.append(self)

    monkeypatch.setattr(crawler, "_new_minute_session", Session)

    def fetch(code, market, *, session, **kwargs):
        thread = get_ident()
        assert thread != main_thread
        with lock:
            first = thread not in sessions
            assert sessions.setdefault(thread, session) is session
            attempts[code] += 1
            attempt = attempts[code]
        if first:
            barrier.wait(timeout=5)
        if code == "000001" and attempt == 1:
            return None
        return [code]

    codes = [(f"{number:06}", 0) for number in range(1, 49)]
    with crawler._minute_fetch_workers(codes, fetch) as results:
        received = {code: rows for code, rows, captured in results}
    assert received == {code: [code] for code, _ in codes}
    assert len(sessions) == len({id(value) for value in sessions.values()}) == 6
    assert attempts["000001"] == 2
    assert sum(attempts.values()) == 49
    assert {id(value) for value in closed} == {id(value) for value in sessions.values()}


def test_result_queue_is_bounded(monkeypatch):
    class Session:
        def close(self):
            pass

    monkeypatch.setattr(crawler, "_new_minute_session", Session)
    submitted = []
    consumed = []
    actual_pool = crawler.ThreadPoolExecutor

    class Pool(actual_pool):
        def submit(self, function, *args):
            submitted.append(args[0])
            assert len(submitted) - len(consumed) <= 2 * crawler.FETCH_WORKERS
            return super().submit(function, *args)

    monkeypatch.setattr(crawler, "ThreadPoolExecutor", Pool)
    codes = [(f"{number:06}", 0) for number in range(100)]
    with crawler._minute_fetch_workers(codes, lambda code, market, **kw: [code]) as results:
        for code, _, captured in results:
            consumed.append(code)
    assert set(consumed) == {code for code, _ in codes}


def test_each_worker_preserves_request_and_batch_pacing(monkeypatch):
    actual_event = crawler.threading.Event
    waits = []
    fetched = Counter()
    lock = Lock()

    class Event(actual_event):
        def wait(self, timeout=None):
            if timeout in (0.5, 20):
                with lock:
                    waits.append((get_ident(), timeout))
                return self.is_set()
            return super().wait(timeout)

    monkeypatch.setattr(crawler.threading, "Event", Event)
    monkeypatch.setattr(crawler, "DELAY", 0.5)
    monkeypatch.setattr(crawler, "BATCH_EVERY", 2)
    monkeypatch.setattr(crawler, "BATCH_PAUSE", 20)
    monkeypatch.setattr(crawler.random, "uniform", lambda *_: 0)

    def fetch(code, market, **kwargs):
        with lock:
            fetched[get_ident()] += 1
        return [code]

    with crawler._minute_fetch_workers([("000001", 0)] * 60, fetch) as results:
        assert len(list(results)) == 60
    counted = Counter(waits)
    assert sum(fetched.values()) == 60
    for thread, count in fetched.items():
        assert counted[(thread, 0.5)] == count
        assert counted[(thread, 20)] == count // 2


@pytest.mark.parametrize("kind", ["kline", "flow"])
def test_collections_stage_and_publish_on_main_thread_after_all_results(monkeypatch, kind):
    main_thread = get_ident()
    fetched = set()
    staged = []
    published = []
    total = 32
    lock = Lock()

    def fetch(code, market, *, session, **kwargs):
        assert get_ident() != main_thread
        with lock:
            fetched.add(code)
        return ["2026-09-11 09:31,10,10,10,10,1,10,0,0,0,10"]

    def append(_connection, _stage, rows, *_args):
        assert get_ident() == main_thread
        staged.extend(rows)
        return len(rows)

    def publish(*_args, **_kwargs):
        assert get_ident() == main_thread
        assert len(fetched) == total
        published.append(True)
        return len(staged)

    codes = [(f"{number:06}", 0) for number in range(total)]
    context = {
        "started_at": "2026-09-11 09:32:00", "decision_known_at": "2026-09-11 09:32:00",
        "requested_trade_date": "2026-09-11", "build_sha": "a" * 40, "run_uid": "b" * 32,
        "catalog_batch_id": "catalog", "catalog_manifest_hash": "c" * 64,
        "catalog_captured_at": "2026-09-11 08:00:00", "native_no_trade_evidence": None,
    }
    monkeypatch.setattr(crawler, "_now", lambda: datetime(2026, 9, 11, 9, 33))
    if kind == "kline":
        monkeypatch.setattr(crawler, "fetch_minute_kline", fetch)
        monkeypatch.setattr(crawler, "_create_kline_stage", lambda *a: ("stage", object()))
        monkeypatch.setattr(crawler, "_append_kline_stage", append)
        monkeypatch.setattr(crawler, "_publish_kline_stage", publish)
        monkeypatch.setattr(crawler, "_drop_kline_stage", lambda *a: None)
        result = crawler.crawl_kline(object(), codes, "sm_stock_minute", "stock", 0, 1.0,
                                     trade_date="2026-09-11", context=context)
    else:
        monkeypatch.setattr(crawler, "fetch_minute_flow", fetch)
        monkeypatch.setattr(crawler, "_create_flow_stage", lambda *a: ("stage", object()))
        monkeypatch.setattr(crawler, "_append_flow_stage", append)
        monkeypatch.setattr(crawler, "_publish_flow_stage", publish)
        monkeypatch.setattr(crawler, "_drop_flow_stage", lambda *a: None)
        result = crawler.crawl_flow(object(), codes, 0, 1.0, trade_date="2026-09-11", context=context)
    assert result["collected_count"] == result["written_rows"] == total
    assert published == [True]


def test_worker_failure_aborts_before_publish_and_closes_all_sessions(monkeypatch):
    sessions = []

    class Session:
        closed = False

        def __init__(self):
            sessions.append(self)

        def close(self):
            self.closed = True

    monkeypatch.setattr(crawler, "_new_minute_session", Session)

    def fail(*_args, **_kwargs):
        raise RuntimeError("source failure")

    with pytest.raises(RuntimeError, match="source failure"):
        with crawler._minute_fetch_workers([("000001", 0)] * 50, fail) as results:
            list(results)
    assert sessions and all(session.closed for session in sessions)


def test_worker_capture_precedes_throttle_and_main_queue_delay(monkeypatch):
    before = datetime(2026, 9, 11, 9, 35)
    after = datetime(2026, 9, 11, 9, 45)
    clock = [before]
    actual_event = crawler.threading.Event
    class Event(actual_event):
        def wait(self, timeout=None):
            if timeout == 0.5:
                clock[0] = after
                return False
            return super().wait(timeout)
    monkeypatch.setattr(crawler, "_now", lambda: clock[0])
    monkeypatch.setattr(crawler.threading, "Event", Event)
    monkeypatch.setattr(crawler, "DELAY", 0.5)
    monkeypatch.setattr(crawler.random, "uniform", lambda *_: 0)
    with crawler._minute_fetch_workers([("000001", 0)], lambda *_a, **_kw: ["raw"]) as results:
        received = list(results)
    assert clock[0] == after
    assert received == [("000001", ["raw"], before)]
    assert crawler._required_grid("2026-09-11", received[0][2])[-1] == "09:32"


def test_request_crossing_a_minute_uses_response_time_watermark(monkeypatch):
    clock = [datetime(2026, 9, 11, 9, 34, 59)]
    monkeypatch.setattr(crawler, "_now", lambda: clock[0])
    def fetch(*_args, **_kwargs):
        clock[0] = datetime(2026, 9, 11, 9, 35, 1)
        return ["raw"]
    with crawler._minute_fetch_workers([("000001", 0)], fetch) as results:
        received = list(results)
    assert received[0][2] == datetime(2026, 9, 11, 9, 35, 1)
    assert crawler._required_grid("2026-09-11", received[0][2])[-1] == "09:32"
