"""Exercise native callbacks arriving while the model is doing slow work."""

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


def test_quote_callback_never_publishes_files():
    producer = load_producer()
    writes = []
    producer._atomic_write = lambda *args: writes.append(args)
    producer.whole_quote_callback({"000001.SZ": {"lastPrice": 10.0}})
    assert producer._tracked_quotes["000001.SZ"]["lastPrice"] == 10.0
    assert producer._callback_batch_count == 1
    assert writes == []


def test_native_call_can_wait_for_quote_callback_without_lock_inversion():
    producer = load_producer()
    producer._refresh_subscription = lambda *a, **k: None
    producer._process_one_request = lambda *a: None
    producer._write_tracked_snapshot = lambda **k: None
    producer._cleanup_queue_artifacts = lambda: None
    producer._write_heartbeat = lambda *a: None
    callbacks = []
    completed_inside_native_call = []

    def native_full_snapshot(context):
        done = threading.Event()

        def native_callback():
            producer.whole_quote_callback({"000001.SZ": {"lastPrice": 10.0}})
            done.set()

        thread = threading.Thread(target=native_callback, daemon=True)
        callbacks.append(thread)
        thread.start()
        completed_inside_native_call.append(done.wait(1))

    producer._refresh_full_snapshot = native_full_snapshot
    producer.bridge_tick(object())
    for thread in callbacks:
        thread.join(2)
    assert completed_inside_native_call == [True]
    assert producer._tracked_quotes["000001.SZ"]["lastPrice"] == 10.0


def test_slow_publication_does_not_block_ingress_or_mutate_its_snapshot():
    producer = load_producer()
    producer.whole_quote_callback({"000001.SZ": {"lastPrice": 10.0}})
    completed = []
    callbacks = []
    published = []
    owner = threading.get_ident()
    callback_writes = []

    def write(name, payload):
        if threading.get_ident() != owner:
            callback_writes.append(name)
            return
        done = threading.Event()

        def callback():
            producer.whole_quote_callback({"000001.SZ": {"lastPrice": 11.0}})
            done.set()

        thread = threading.Thread(target=callback, daemon=True)
        callbacks.append(thread)
        thread.start()
        completed.append(done.wait(1))
        published.append(payload)

    producer._atomic_write = write
    producer._write_tracked_snapshot(force=True)
    for thread in callbacks:
        thread.join(2)
    assert completed == [True]
    assert callback_writes == []
    assert published[0]["quotes"]["000001.SZ"]["lastPrice"] == 10.0
    assert producer._tracked_quotes["000001.SZ"]["lastPrice"] == 11.0


def test_reentrant_timer_does_not_repeat_native_work_and_releases_after_error():
    producer = load_producer()
    calls = []
    statuses = []

    def request(context):
        calls.append("request")
        producer.bridge_tick(context)
        raise OSError("publication unavailable")

    producer._refresh_subscription = lambda *a, **k: None
    producer._process_one_request = request
    producer._write_heartbeat = statuses.append
    producer._direct_model = SimpleNamespace(poll=lambda c: calls.append("direct"))
    producer.bridge_tick(object())
    producer.bridge_tick(object())
    assert calls == ["request", "direct", "request", "direct"]
    assert statuses == ["error", "error"]


def test_stop_does_not_hold_the_callback_lock_during_native_unsubscribe():
    producer = load_producer()
    producer._subscription_id = 7
    completed = []
    callbacks = []
    statuses = []

    def unsubscribe(subscription):
        assert subscription == 7
        done = threading.Event()

        def callback():
            producer.whole_quote_callback({"000001.SZ": {"lastPrice": 10.0}})
            done.set()

        thread = threading.Thread(target=callback, daemon=True)
        callbacks.append(thread)
        thread.start()
        completed.append(done.wait(1))

    producer._write_heartbeat = statuses.append
    producer.stop(SimpleNamespace(unsubscribe_quote=unsubscribe))
    for thread in callbacks:
        thread.join(2)
    assert completed == [True]
    assert statuses == ["stopped"]
    assert producer._subscription_id is None


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


def test_stop_cancels_timer_and_fences_late_timer_and_quote_callbacks():
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
    producer.whole_quote_callback({"000001.SZ": {"lastPrice": 11}})
    assert cancelled == [42]
    assert calls == ["stopped"]
    assert not producer._tracked_quotes
    assert producer._timer_id is None
    assert len(scheduled) == 1


def test_stop_during_request_never_waits_or_starts_more_native_work():
    producer = load_producer()
    calls = []
    producer._subscription_id = 9
    context = SimpleNamespace(unsubscribe_quote=lambda seq: calls.append(("unsubscribe", seq)))
    producer._refresh_subscription = lambda *a, **k: None

    def request(context):
        producer.stop(context)
        calls.append("request_returned")

    producer._process_one_request = request
    producer._refresh_full_snapshot = lambda c: calls.append("unexpected_snapshot")
    producer._poll_direct_acquisition = lambda c: calls.append("unexpected_direct")
    producer._write_heartbeat = calls.append
    producer.bridge_tick(context)
    assert calls == ["request_returned", ("unsubscribe", 9), "stopped"]
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
