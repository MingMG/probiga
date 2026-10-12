"""Local socket regressions for the owner cycle's complete transport deadline.

No production DB credentials or mutation: the handshake case uses the installed
PyMySQL driver against an intentionally incomplete loopback MySQL greeting.
"""

import hashlib
import json
import socket
import threading
import time
from types import SimpleNamespace

import pytest

from server.api import scheduler_runtime as runtime
from server.common import engine_factory


@pytest.fixture
def owner_lease(monkeypatch):
    monkeypatch.setattr(runtime, "get_mysql_url", lambda **_kwargs:
                        "mysql+pymysql://unit:unit@127.0.0.1/owner_test")
    monkeypatch.setattr(engine_factory, "_get_runtime_tls_config", lambda:
                        {"required": False, "ssl_ca": None})
    monkeypatch.setattr(runtime, "get_scheduler_runtime_config", lambda:
                        {"poll_seconds": 15, "max_concurrent_tasks": 1})
    monkeypatch.setattr(runtime, "_scheduler_executor_role", lambda _mode: "qmt_windows_edge")
    monkeypatch.setattr(runtime, "_scheduler_build_commit_sha", lambda: "a" * 40)
    monkeypatch.setattr(runtime, "_scheduler_stopping", False)
    lease = runtime._WindowsSchedulerLease(threading.Event())
    try:
        yield lease
    finally:
        lease.stop()


@pytest.mark.parametrize("slow_begin", [False, True])
def test_initial_pymysql_handshake_is_bounded_despite_fragmented_reads(monkeypatch, slow_begin):
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(1)
    stop = threading.Event()
    connected = threading.Event()

    def incomplete_greeting():
        try:
            peer, _address = listener.accept()
            with peer:
                connected.set()
                # A full 1024-byte MySQL packet is promised. Sending fragments
                # faster than read_timeout keeps every individual read alive.
                peer.sendall(b"\x00\x04\x00\x00")
                while not stop.wait(0.005):
                    peer.sendall(b"x")
        except OSError:
            pass  # The owner's actual socket watchdog disconnects the peer.

    producer = threading.Thread(target=incomplete_greeting, daemon=True)
    producer.start()
    port = listener.getsockname()[1]
    monkeypatch.setattr(runtime, "get_mysql_url", lambda **_kwargs:
                        f"mysql+pymysql://unit:unit@127.0.0.1:{port}/owner_test")
    monkeypatch.setattr(engine_factory, "_get_runtime_tls_config", lambda:
                        {"required": False, "ssl_ca": None})
    monkeypatch.setattr(runtime, "get_scheduler_runtime_config", lambda:
                        {"poll_seconds": 15, "max_concurrent_tasks": 1})
    monkeypatch.setattr(runtime, "_scheduler_executor_role", lambda _mode: "qmt_windows_edge")
    monkeypatch.setattr(runtime, "_scheduler_build_commit_sha", lambda: "a" * 40)
    monkeypatch.setattr(runtime, "_scheduler_stopping", False)
    lease = runtime._WindowsSchedulerLease(threading.Event())
    lease.cycle_seconds = 0.15 if slow_begin else 0.08
    if slow_begin:
        original_begin = lease._begin_cycle_evidence

        def delayed_begin(*args, **kwargs):
            original_begin(*args, **kwargs)
            time.sleep(0.10)  # Diagnostic delay, not a fake driver stall.

        monkeypatch.setattr(lease, "_begin_cycle_evidence", delayed_begin)
    logs = []
    monkeypatch.setattr(runtime.logger, "error", lambda *args, **_kwargs: logs.append(args))
    began = time.monotonic()
    try:
        with pytest.raises(runtime.SchedulerOwnerLeaseLost, match="cycle_deadline"):
            lease._cycle(initial=True)
        assert connected.is_set()
        assert time.monotonic() - began < (0.22 if slow_begin else 1)
        assert not logs  # Capturing the failed cycle must not reach a sink.
        assert not lease.verified.is_set()
        assert not lease._cycle_dbapis and not lease._cycle_sockets
        evidence = lease._last_cycle_evidence
        assert evidence["status"] == "FAILED" and evidence["stage"] == "connect_greeting"
        assert evidence["driver_failure"]["stage"] == "connect_greeting"
        assert evidence["operation"] == evidence["statement_sha256"] == ""
        assert evidence["elapsed_ms"] >= evidence["budget_ms"]
        assert "connect_greeting" in evidence["phase_elapsed_ms"]
        assert evidence["watchdog_abort_completed"]
    finally:
        lease.stop()
        stop.set()
        listener.close()
        producer.join(timeout=1)
        assert not producer.is_alive()


def test_complete_cycle_deadline_interrupts_established_fragmented_reply(owner_lease, monkeypatch):
    lease = owner_lease
    reader, peer = socket.socketpair()
    driver = SimpleNamespace(_sock=reader)
    stop = threading.Event()
    seen = threading.Event()

    def stream():
        try:
            while not stop.wait(0.005):
                peer.sendall(b"x")
                seen.set()
        except OSError:
            pass

    def blocked_activation(*_args, **_kwargs):
        lease._bound_transport(driver)
        reader.settimeout(driver._read_timeout)
        while reader.recv(1):
            pass
        return True, "ready"

    monkeypatch.setattr(runtime, "_qmt_windows_loop_activation_ready", blocked_activation)
    lease.cycle_seconds = 0.06
    producer = threading.Thread(target=stream, daemon=True)
    producer.start()
    began = time.monotonic()
    try:
        with pytest.raises(runtime.SchedulerOwnerLeaseLost, match="cycle_deadline"):
            lease._cycle(initial=True)
        assert seen.is_set()
        assert time.monotonic() - began < 1
        assert not lease.verified.is_set()
        assert not lease._cycle_dbapis and not lease._cycle_sockets
    finally:
        stop.set()
        reader.close()
        peer.close()
        producer.join(timeout=1)
        assert not producer.is_alive()


@pytest.mark.parametrize("block_final_registration", [False, True])
def test_total_startup_deadline_bounds_probe_and_final_registration_transport(
    owner_lease, monkeypatch, block_final_registration,
):
    lease = owner_lease
    reader, peer = socket.socketpair()
    driver = SimpleNamespace(_sock=reader)
    stop = threading.Event()
    activations = []
    registrations = []

    def stream():
        try:
            while not stop.wait(0.005):
                peer.sendall(b"x")
        except OSError:
            pass

    def activation(*_args, **_kwargs):
        activations.append(True)
        if block_final_registration and len(activations) == 1:
            return True, "ready"
        lease._bound_transport(driver)
        reader.settimeout(driver._read_timeout)
        while reader.recv(1):
            pass
        return True, "ready"

    monkeypatch.setattr(runtime, "_qmt_windows_loop_activation_ready", activation)
    monkeypatch.setattr(runtime, "_windows_scheduler_registration_ready", lambda *_args: True)
    monkeypatch.setattr(runtime, "_register_windows_scheduler_heartbeat",
                        lambda *_args: registrations.append(True))
    lease.cycle_seconds = 0.5
    lease.startup_seconds = 0.06
    producer = threading.Thread(target=stream, daemon=True)
    producer.start()
    began = time.monotonic()
    try:
        with pytest.raises(runtime.SchedulerOwnerLeaseLost, match="startup_owner_deadline"):
            lease.start()
        assert len(activations) == (2 if block_final_registration else 1)
        assert not registrations
        assert time.monotonic() - began < 1
        assert lease.thread is None and not lease.verified.is_set()
        assert not lease._cycle_dbapis and not lease._cycle_sockets
    finally:
        stop.set()
        reader.close()
        peer.close()
        producer.join(timeout=1)
        assert not producer.is_alive()


@pytest.mark.parametrize("first_raises", [False, True])
def test_cancelled_timer_cannot_close_a_socket_reused_by_the_next_cycle(
    owner_lease, monkeypatch, first_raises,
):
    lease = owner_lease
    reader, peer = socket.socketpair()
    driver = SimpleNamespace(_sock=reader)
    rounds = []
    timers = []
    real_timer = threading.Timer

    def record_timer(*args, **kwargs):
        timer = real_timer(*args, **kwargs)
        timers.append(timer)
        return timer

    def activation(*_args, **_kwargs):
        lease._bound_transport(driver)
        rounds.append(True)
        if len(rounds) == 1:
            if first_raises:
                raise RuntimeError("guard failed")
        else:
            # Cross the first cycle's old deadline while using the same socket.
            time.sleep(0.08)
            reader.settimeout(1)
            peer.sendall(b"still-open")
            assert reader.recv(10) == b"still-open"
        return True, "ready"

    monkeypatch.setattr(runtime.threading, "Timer", record_timer)
    monkeypatch.setattr(runtime, "_qmt_windows_loop_activation_ready", activation)
    monkeypatch.setattr(runtime, "_register_windows_scheduler_heartbeat", lambda *_args: None)
    monkeypatch.setattr(runtime, "_renew_windows_scheduler_heartbeat", lambda *_args: None)
    monkeypatch.setattr(runtime, "_standalone_heartbeat_allows_dispatch", lambda *_args: (True, {}))
    lease.cycle_seconds = 0.04
    try:
        if first_raises:
            with pytest.raises(RuntimeError, match="guard failed"):
                lease._cycle(initial=True)
        else:
            lease._cycle(initial=True)
        assert len(timers) == 1 and not timers[0].is_alive()
        lease.cycle_seconds = 0.5
        lease._cycle()
        assert lease.verified.is_set()
        assert len(timers) == 2 and not any(timer.is_alive() for timer in timers)
        assert not lease._cycle_dbapis and not lease._cycle_sockets
    finally:
        reader.close()
        peer.close()


def test_statement_evidence_retains_only_operation_and_template_hash():
    template = "SELECT private_column FROM private_table WHERE token=%s"
    assert runtime._WindowsSchedulerLease._statement_evidence(template) == {
        "operation": "SELECT", "statement_sha256": hashlib.sha256(template.encode()).hexdigest(),
    }
    assert "private_column" not in json.dumps(runtime._WindowsSchedulerLease._statement_evidence(template))


def test_diagnostic_log_failure_cannot_change_cycle_authority_or_original_error(owner_lease, monkeypatch):
    lease = owner_lease
    monkeypatch.setattr(runtime, "_qmt_windows_loop_activation_ready", lambda *_args, **_kwargs: (True, "ready"))
    monkeypatch.setattr(runtime, "_register_windows_scheduler_heartbeat", lambda *_args: None)
    monkeypatch.setattr(runtime, "_standalone_heartbeat_allows_dispatch", lambda *_args: (True, {}))
    assert lease._cycle(initial=True) is True
    previous_uid = lease._last_cycle_evidence["cycle_uid"]

    class OriginalFailure(RuntimeError):
        pass

    def failed_activation(*_args, **_kwargs):
        raise OriginalFailure("secret DSN/password/error detail must not enter evidence")

    calls = []

    def failed_logger(*args, **_kwargs):
        calls.append(args)
        raise RuntimeError("diagnostic sink unavailable")

    monkeypatch.setattr(runtime, "_qmt_windows_loop_activation_ready", failed_activation)
    monkeypatch.setattr(runtime.logger, "error", failed_logger)
    with pytest.raises(OriginalFailure):
        lease._cycle()
    assert not lease.verified.is_set() and not lease.authorized()
    assert not calls
    evidence = lease._last_cycle_evidence
    assert evidence["cycle_uid"] != previous_uid
    assert evidence["last_successful_cycle"]["cycle_uid"] == previous_uid
    assert evidence["status"] == "FAILED" and evidence["exception_type"] == "OriginalFailure"
    encoded = json.dumps(evidence)
    assert "secret" not in encoded and "diagnostic sink" not in encoded
    assert not lease._cycle_dbapis and not lease._cycle_sockets


def test_blocking_existing_loss_sink_has_no_new_cycle_or_worker_log_call(owner_lease, monkeypatch):
    lease = owner_lease
    lease.interval_seconds = 0.001
    monkeypatch.setattr(runtime, "_running_task_ids", set())
    monkeypatch.setattr(runtime, "_running_history_uids", {})
    monkeypatch.setattr(runtime, "_shutdown_owned_runs", None)
    monkeypatch.setattr(runtime, "_scheduler_wake_event", threading.Event())

    def failed_activation(*_args, **_kwargs):
        raise runtime.SchedulerOwnerLeaseLost("test_existing_loss_boundary")

    entered = threading.Event()
    release = threading.Event()
    calls = []

    def blocking_sink(*args, **_kwargs):
        calls.append(args)
        entered.set()
        assert release.wait(2)

    monkeypatch.setattr(runtime, "_qmt_windows_loop_activation_ready", failed_activation)
    monkeypatch.setattr(runtime.logger, "error", blocking_sink)
    lease.thread = threading.Thread(target=lease._run, name="test-existing-loss-log")
    lease.thread.start()
    try:
        assert entered.wait(1)
        assert len(calls) == 1
        # The one original sink still precedes stop, exactly as before. The
        # diagnostic change must not claim that inherited liveness is fixed.
        assert not lease.stop_event.is_set()
        assert "cycle_evidence=%s" in calls[0][0]
        assert json.loads(calls[0][-1])["status"] == "FAILED"
    finally:
        release.set()
        lease.thread.join(timeout=2)
        assert not lease.thread.is_alive()
    assert lease.stop_event.is_set() and runtime._scheduler_stopping
    assert len(calls) == 1


def test_diagnostic_serialization_failure_preserves_existing_loss_stop(owner_lease, monkeypatch):
    lease = owner_lease
    monkeypatch.setattr(runtime, "_running_task_ids", set())
    monkeypatch.setattr(runtime, "_running_history_uids", {})
    monkeypatch.setattr(runtime, "_shutdown_owned_runs", None)
    monkeypatch.setattr(runtime, "_scheduler_wake_event", threading.Event())

    def failed_activation(*_args, **_kwargs):
        raise runtime.SchedulerOwnerLeaseLost("test_original_error")

    def failed_serialization(*_args, **_kwargs):
        raise RuntimeError("sensitive serialization exception detail")

    calls = []
    monkeypatch.setattr(runtime, "_qmt_windows_loop_activation_ready", failed_activation)
    monkeypatch.setattr(runtime.json, "dumps", failed_serialization)
    monkeypatch.setattr(runtime.logger, "error", lambda *args, **_kwargs: calls.append(args))
    with pytest.raises(runtime.SchedulerOwnerLeaseLost, match="test_original_error"):
        lease._cycle()
    assert not calls and lease._last_cycle_evidence_json == "{}"
    lease._lose("test_original_error")
    assert len(calls) == 1 and calls[0][-1] == "{}"
    assert lease.stop_event.is_set() and runtime._scheduler_stopping
