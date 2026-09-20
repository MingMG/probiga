"""Exercise the sole native polling lifecycle, reentrancy and cancellation."""

import importlib.util
from pathlib import Path
import threading
from types import SimpleNamespace


SOURCE = Path(__file__).resolve().parents[1] / "integrations/bigqmt/qmt_strategy/probiga_big_qmt_bridge.py"


def load_producer():
    spec = importlib.util.spec_from_file_location("callback_liveness_producer", SOURCE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module._tracked_codes = ["000001.SZ"]
    return module


def test_only_timer_reads_native_quotes_and_never_subscribes():
    producer = load_producer()
    producer._all_codes = list(producer._tracked_codes)
    producer._load_config = lambda **kw: False
    producer._quote_phase = lambda now: ("live", "2026-09-18")
    producer._process_one_request = lambda context: None
    producer._cleanup_queue_artifacts = lambda: None
    producer._write_heartbeat = lambda status: None
    producer._atomic_write = lambda *args: None
    calls = []
    context = SimpleNamespace(get_full_tick=lambda codes:
        calls.append(codes) or {codes[0]: {"time": 1789704000000, "lastPrice": 10}})
    producer.bridge_tick(context)
    assert calls == [["000001.SZ"]]
    assert producer._tracked_quotes["000001.SZ"]["lastPrice"] == 10
    assert not hasattr(producer, "whole_quote_callback")


def test_reentrant_timer_does_not_repeat_native_work_and_releases_after_error():
    producer = load_producer()
    calls = []
    statuses = []

    def request(context):
        calls.append("request")
        producer.bridge_tick(context)
        raise OSError("publication unavailable")

    producer._refresh_quote_universe = lambda *a, **k: None
    producer._process_one_request = request
    producer._refresh_full_snapshot = lambda c: None
    producer._write_tracked_snapshot = lambda **k: None
    producer._write_heartbeat = statuses.append
    producer._direct_model = SimpleNamespace(poll=lambda c: calls.append("direct"))
    producer.bridge_tick(object())
    producer.bridge_tick(object())
    assert calls == ["request", "direct", "request", "direct"]
    assert statuses == ["error", "error"]


def test_slow_native_pass_has_no_pending_timer_and_rearms_only_after_return():
    producer = load_producer()
    callbacks = []
    entered, release = threading.Event(), threading.Event()
    calls = []

    def schedule(callback, start, **kwargs):
        assert kwargs["repeat_times"] == 0
        callbacks.append(callback)
        return len(callbacks)

    def work(context):
        calls.append("work")
        entered.set()
        assert release.wait(2)

    context = SimpleNamespace(schedule_run=schedule)
    producer.bridge_tick = work
    producer._schedule_next_tick(context)
    worker = threading.Thread(target=callbacks[0], args=(context,), daemon=True)
    worker.start()
    try:
        assert entered.wait(1)
        assert producer._timer_id is None
        assert len(callbacks) == 1
        callbacks[0](context)  # A duplicate delivery cannot enter active work.
        assert calls == ["work"]
    finally:
        release.set()
        worker.join(2)
    assert not worker.is_alive()
    assert len(callbacks) == 2
    callbacks[0](context)  # Nor can a late delivery consume the next timer.
    assert calls == ["work"]
    assert producer._timer_id == 2


def test_stop_cancels_timer_and_fences_late_timer():
    producer = load_producer()
    scheduled, cancelled, calls = [], [], []

    def schedule(callback, start, **kwargs):
        scheduled.append(callback)
        return 42

    context = SimpleNamespace(schedule_run=schedule, cancel_schedule_run=cancelled.append)
    producer._write_heartbeat = calls.append
    producer._schedule_next_tick(context)
    producer.stop(context)
    scheduled[0](context)
    producer.bridge_tick(context)
    assert cancelled == [42]
    assert calls == ["stopped"]
    assert not producer._tracked_quotes
    assert producer._timer_id is None
    assert len(scheduled) == 1


def test_stop_during_request_never_waits_or_starts_more_native_work():
    producer = load_producer()
    calls = []
    context = object()
    producer._refresh_quote_universe = lambda *a, **k: None

    def request(context):
        producer.stop(context)
        calls.append("request_returned")

    producer._process_one_request = request
    producer._refresh_full_snapshot = lambda c: None
    producer._write_tracked_snapshot = lambda **k: None
    producer._refresh_full_snapshot = lambda c: calls.append("snapshot")
    producer._poll_direct_acquisition = lambda c: calls.append("unexpected_direct")
    producer._write_heartbeat = calls.append
    producer.bridge_tick(context)
    assert calls == ["snapshot", "request_returned", "stopped"]
    assert producer._execution_lock.acquire(False)
    producer._execution_lock.release()


def test_timer_rearms_after_publication_exception():
    producer = load_producer()
    scheduled = []

    def schedule(callback, start, **kwargs):
        scheduled.append(callback)
        return len(scheduled)

    def failing(context):
        raise OSError("disk unavailable")

    producer.bridge_tick = failing
    context = SimpleNamespace(schedule_run=schedule)
    producer._schedule_next_tick(context)
    import pytest
    with pytest.raises(OSError, match="disk unavailable"):
        scheduled[0](context)
    assert len(scheduled) == 2


def test_stop_inside_unmanaged_native_batch_prevents_following_batch():
    import pytest
    producer = load_producer()
    producer._write_heartbeat = lambda status: None
    calls = []
    def native(codes):
        calls.append(list(codes))
        producer.stop(context)
        return {}
    context = SimpleNamespace(get_full_tick=native)
    with pytest.raises(RuntimeError, match="QMT_MODEL_STOPPING"):
        producer._current_rows(producer._QuoteCacheContext(context), {
            "stock_codes": ["%06d.SZ" % n for n in range(21)],
            "batch_size": 20,
        })
    assert len(calls) == 1
    assert len(calls[0]) == 20


def test_stop_during_download_prevents_later_download_and_native_reader():
    import pytest
    producer = load_producer()
    producer._write_heartbeat = lambda status: None
    producer._check_native_history_budget = lambda method, phase="before": None
    calls = []
    context = SimpleNamespace(get_market_data_ex_ori=lambda *args, **kwargs:
        calls.append("unexpected reader"))
    def download(symbol, *args, **kwargs):
        calls.append(symbol)
        producer.stop(context)
    producer.download_history_data = download
    with pytest.raises(RuntimeError, match="QMT_MODEL_STOPPING"):
        producer._market_rows(producer._QuoteCacheContext(context), {
            "stock_codes": ["000001.SZ", "600000.SH"],
            "start_date": "2026-09-18", "end_date": "2026-09-18",
            "download_history": True,
        }, "1d")
    assert calls == ["000001.SZ"]
