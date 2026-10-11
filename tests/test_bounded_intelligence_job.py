from __future__ import annotations

import multiprocessing
import os
import time
from multiprocessing.util import Finalize
from pathlib import Path

import pytest

from tools._bounded_intelligence_job import run_bounded_intelligence_job


def _accept(receipt):
    return receipt.get("status") == "COMPLETE" and receipt.get("verified") is True


def _successful_worker(payload, reporter):
    reporter.stage("database_readback")
    reporter.progress({"status": "RUNNING", "pid": os.getpid(), "current_unit": 1})
    time.sleep(0.1)
    return {"status": "COMPLETE", "verified": True, "value": payload["value"]}


def _stalled_worker(_payload, reporter):
    reporter.stage("database_commit")
    reporter.progress({"status": "RUNNING", "pid": os.getpid()})
    time.sleep(10)
    return {"status": "COMPLETE", "verified": True}


def _exception_worker(_payload, reporter):
    reporter.stage("schema_validation")
    reporter.progress({"pid": os.getpid()})
    raise ValueError("schema unavailable")


def _unproven_worker(_payload, reporter):
    reporter.stage("database_readback")
    return {"status": "COMPLETE", "verified": False}


def _exit_seven():
    os._exit(7)


def _result_then_bad_exit_worker(_payload, reporter):
    reporter.stage("database_readback")
    Finalize(None, _exit_seven, exitpriority=0)
    return {"status": "COMPLETE", "verified": True}


class _ExpectedBusinessError(RuntimeError):
    stage = "database_publish"


def _business_error_worker(_payload, reporter):
    reporter.stage("database_publish")
    raise _ExpectedBusinessError("publication unavailable")


def _flooded_worker(_payload, reporter):
    reporter.stage("source_capture")
    while True:
        reporter.progress({"pid": os.getpid(), "current_unit": 1})


def _orphan_supervisor(control, mode):
    def record_worker(progress):
        control.send(progress["pid"])
        if mode == "exit":
            os._exit(9)

    run_bounded_intelligence_job(
        _stalled_worker, timeout_seconds=20,
        receipt_acceptor=_accept, progress_fn=record_worker,
    )


def _pid_running(pid):
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
        kernel.WaitForSingleObject.restype = wintypes.DWORD
        kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
        handle = kernel.OpenProcess(0x00100000, False, pid)
        if not handle:
            return False
        try:
            return kernel.WaitForSingleObject(handle, 0) == 0x00000102
        finally:
            kernel.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    stat = Path(f"/proc/{pid}/stat")
    if stat.exists():
        try:
            # Linux may retain a reparented zombie until its init reaps it;
            # it is already terminal and cannot perform business work.
            return stat.read_text().rsplit(")", 1)[1].strip()[0] != "Z"
        except FileNotFoundError:
            return False
    return True


def _no_child_left(pids):
    assert not set(pids) & {child.pid for child in multiprocessing.active_children()}


def test_spawned_job_runs_once_and_delivers_progress_before_success():
    messages = []
    outcome = run_bounded_intelligence_job(
        _successful_worker, {"value": 42}, timeout_seconds=5,
        receipt_acceptor=_accept, progress_fn=messages.append,
    )

    assert outcome.exit_code == 0
    assert outcome.receipt == {"status": "COMPLETE", "verified": True, "value": 42}
    assert len(messages) == 1
    assert messages[0]["current_unit"] == 1
    assert messages[0]["pid"] != os.getpid()
    _no_child_left([messages[0]["pid"]])


def test_spawned_hard_deadline_stops_worker_and_preserves_last_stage():
    messages = []
    started = time.monotonic()
    outcome = run_bounded_intelligence_job(
        _stalled_worker, timeout_seconds=1.5,
        receipt_acceptor=_accept, progress_fn=messages.append,
    )

    assert time.monotonic() - started < 4
    assert outcome.exit_code == 1
    assert outcome.receipt["status"] == "FAILED"
    assert outcome.receipt["last_stage"] == "database_commit"
    assert outcome.receipt["message"] == "JOB_DEADLINE_EXCEEDED"
    assert outcome.receipt["worker_terminal"] is True
    assert len(messages) == 1
    _no_child_left([messages[0]["pid"]])


def test_spawned_worker_exception_has_stage_and_no_remaining_child():
    messages = []
    outcome = run_bounded_intelligence_job(
        _exception_worker, timeout_seconds=5,
        receipt_acceptor=_accept, progress_fn=messages.append,
    )

    assert outcome.exit_code == 1
    assert outcome.receipt["status"] == "FAILED"
    assert outcome.receipt["stage"] == "schema_validation"
    assert outcome.receipt["error_type"] == "ValueError"
    assert outcome.receipt["worker_terminal"] is True
    _no_child_left([messages[0]["pid"]])


def test_spawned_unproven_receipt_cannot_become_complete():
    outcome = run_bounded_intelligence_job(
        _unproven_worker, timeout_seconds=5, receipt_acceptor=_accept,
    )

    assert outcome.exit_code == 1
    assert outcome.receipt["status"] == "FAILED"
    assert outcome.receipt["message"] == "BUSINESS_RECEIPT_UNPROVEN"


def test_complete_receipt_before_nonzero_process_exit_is_rejected():
    outcome = run_bounded_intelligence_job(
        _result_then_bad_exit_worker, timeout_seconds=5, receipt_acceptor=_accept,
    )

    assert outcome.exit_code == 1
    assert outcome.receipt["status"] == "FAILED"
    assert outcome.receipt["worker_exit_code"] == 7


def test_business_exception_exit_code_is_preserved():
    outcome = run_bounded_intelligence_job(
        _business_error_worker, timeout_seconds=5, receipt_acceptor=_accept,
        exception_exit_codes={"_ExpectedBusinessError": 2},
    )

    assert outcome.exit_code == 2
    assert outcome.receipt["status"] == "FAILED"
    assert outcome.receipt["stage"] == "database_publish"
    assert outcome.receipt["worker_terminal"] is True


def test_progress_flood_cannot_starve_hard_deadline():
    messages = []
    started = time.monotonic()
    outcome = run_bounded_intelligence_job(
        _flooded_worker, timeout_seconds=1.5,
        receipt_acceptor=_accept, progress_fn=messages.append,
    )

    assert time.monotonic() - started < 4
    assert outcome.exit_code == 1
    assert outcome.receipt["last_stage"] == "source_capture"
    assert outcome.receipt["message"] == "JOB_DEADLINE_EXCEEDED"
    assert messages
    _no_child_left([messages[0]["pid"]])


def test_blocked_progress_callback_cannot_delay_worker_termination():
    messages = []

    def blocked_output(progress):
        messages.append(progress)
        time.sleep(10)

    started = time.monotonic()
    outcome = run_bounded_intelligence_job(
        _stalled_worker, timeout_seconds=1.5,
        receipt_acceptor=_accept, progress_fn=blocked_output,
    )

    assert time.monotonic() - started < 4
    assert outcome.exit_code == 1
    assert outcome.receipt["message"] == "JOB_DEADLINE_EXCEEDED"
    assert outcome.receipt["last_stage"] == "database_commit"
    assert messages
    _no_child_left([messages[0]["pid"]])


def test_slow_receipt_validation_cannot_issue_late_complete():
    def blocked_acceptor(_receipt):
        time.sleep(10)
        return True

    started = time.monotonic()
    outcome = run_bounded_intelligence_job(
        _successful_worker, {"value": 42}, timeout_seconds=1.5,
        receipt_acceptor=blocked_acceptor,
    )

    assert time.monotonic() - started < 4
    assert outcome.exit_code == 1
    assert outcome.receipt["status"] == "FAILED"
    assert outcome.receipt["message"] == "JOB_DEADLINE_EXCEEDED"


def test_unpicklable_worker_is_rejected_before_spawn():
    children_before = {child.pid for child in multiprocessing.active_children()}
    outcome = run_bounded_intelligence_job(
        lambda _payload, _reporter: {}, timeout_seconds=1, receipt_acceptor=_accept,
    )

    assert outcome.exit_code == 1
    assert outcome.receipt["status"] == "FAILED"
    assert outcome.receipt["stage"] == "worker_start"
    assert outcome.receipt["worker_terminal"] is True
    assert {child.pid for child in multiprocessing.active_children()} == children_before


@pytest.mark.parametrize("mode", ("exit", "kill"))
def test_business_worker_becomes_terminal_when_cli_parent_disappears(mode):
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    supervisor = context.Process(target=_orphan_supervisor, args=(sender, mode))
    worker_pid = None
    supervisor.start()
    sender.close()
    try:
        assert receiver.poll(5), "spawned business worker did not report its PID"
        worker_pid = receiver.recv()
        if mode == "kill":
            supervisor.kill()
        supervisor.join(timeout=3)
        assert not supervisor.is_alive()
        if mode == "exit":
            assert supervisor.exitcode == 9
        deadline = time.monotonic() + 3
        while _pid_running(worker_pid) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert not _pid_running(worker_pid), "business child survived loss of its CLI owner"
    finally:
        if supervisor.is_alive():
            supervisor.kill()
            supervisor.join(timeout=3)
        receiver.close()
        supervisor.close()


def test_input_transport_is_inside_same_hard_deadline():
    def stalled_input():
        time.sleep(1)
        return {"value": 42}

    children_before = {child.pid for child in multiprocessing.active_children()}
    started = time.monotonic()
    outcome = run_bounded_intelligence_job(
        _successful_worker, timeout_seconds=0.1, receipt_acceptor=_accept,
        prepare_payload=stalled_input,
    )

    assert time.monotonic() - started < 0.5
    assert outcome.exit_code == 1
    assert outcome.receipt["status"] == "FAILED"
    assert outcome.receipt["stage"] == "input_validation"
    assert outcome.receipt["worker_terminal"] is True
    assert {child.pid for child in multiprocessing.active_children()} == children_before
