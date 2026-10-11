"""The single, hard-deadline process boundary for intelligence CLI jobs."""
from __future__ import annotations

import math
import multiprocessing
from multiprocessing.reduction import ForkingPickler
import os
import queue
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class BoundedIntelligenceOutcome:
    receipt: dict[str, Any]
    exit_code: int


class IntelligenceJobReporter:
    """Send durable-job stages and progress to the supervising CLI process."""

    def __init__(self, connection: Any) -> None:
        self._connection = connection
        self.last_stage = "worker_start"

    def stage(self, name: str) -> None:
        self.last_stage = str(name).strip() or self.last_stage
        self._connection.send({"kind": "stage", "stage": self.last_stage})

    def progress(self, payload: Mapping[str, Any]) -> None:
        self._connection.send({"kind": "progress", "payload": dict(payload)})


def _failed(stage: str, error_type: str, message: str, **extra: Any) -> dict[str, Any]:
    return {
        "status": "FAILED",
        "stage": stage,
        "last_stage": stage,
        "error_type": error_type,
        "message": message,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
        **extra,
    }


def _intelligence_worker_entry(
    worker: Callable,
    payload: dict[str, Any],
    connection: Any,
    exception_exit_codes: dict[str, int],
) -> None:
    reporter = IntelligenceJobReporter(connection)
    owner = multiprocessing.parent_process()

    def watch_owner() -> None:
        # The registered CLI owns this single business child. A scheduler kill
        # must not leave it writing after that owner has become terminal.
        while True:
            time.sleep(0.2)
            if owner is not None and not owner.is_alive():
                os._exit(1)

    threading.Thread(target=watch_owner, name="intelligence-worker-owner", daemon=True).start()
    exit_code = 0
    try:
        reporter.stage("worker_start")
        receipt = dict(worker(payload, reporter))
        if receipt.get("status") == "FAILED":
            exit_code = 1
            receipt.setdefault("last_stage", reporter.last_stage)
    except BaseException as exc:
        stage = str(getattr(exc, "stage", "") or reporter.last_stage)
        receipt = _failed(stage, type(exc).__name__, str(exc))
        exit_code = int(exception_exit_codes.get(type(exc).__name__, 1))
    try:
        connection.send({"kind": "result", "receipt": receipt, "exit_code": exit_code})
    finally:
        connection.close()
    raise SystemExit(exit_code)


def _stop_worker(process: Any) -> None:
    """Finish process termination before releasing a terminal CLI receipt."""
    if process.is_alive():
        process.terminate()
        process.join(timeout=1.0)
    if process.is_alive():
        process.kill()
        process.join(timeout=5.0)
    if process.is_alive() or process.exitcode is None:
        raise RuntimeError("intelligence_worker_cleanup: child termination was not proven")
    process.join(timeout=0)


def run_bounded_intelligence_job(
    worker: Callable[[dict[str, Any], IntelligenceJobReporter], Mapping[str, Any]],
    payload: Mapping[str, Any] | None = None,
    *,
    timeout_seconds: float,
    receipt_acceptor: Callable[[Mapping[str, Any]], bool],
    progress_fn: Callable[[dict[str, Any]], None] | None = None,
    exception_exit_codes: Mapping[str, int] | None = None,
    prepare_payload: Callable[[], Mapping[str, Any]] | None = None,
    preparation_stage: str = "input_validation",
) -> BoundedIntelligenceOutcome:
    """Execute exactly once with spawn; input transport shares the hard budget.

    A daemon input reader only transports bounded CLI input. On an input timeout
    no business worker starts, and the CLI can exit without waiting for stdin EOF.
    Business execution, including all network calls, happens only in the child.
    """
    budget = float(timeout_seconds)
    if not math.isfinite(budget) or budget <= 0:
        raise ValueError("intelligence job timeout_seconds must be finite and positive")
    deadline = time.monotonic() + budget
    last_stage = preparation_stage if prepare_payload is not None else "worker_start"
    if prepare_payload is not None:
        prepared: queue.Queue = queue.Queue(maxsize=1)

        def read_input() -> None:
            try:
                prepared.put((True, dict(prepare_payload())))
            except BaseException as exc:
                prepared.put((False, exc))

        threading.Thread(target=read_input, name="intelligence-cli-input", daemon=True).start()
        try:
            ok, value = prepared.get(timeout=max(0.0, deadline - time.monotonic()))
        except queue.Empty:
            return BoundedIntelligenceOutcome(_failed(
                last_stage, "IntelligenceJobTimeout", "JOB_DEADLINE_EXCEEDED",
                timeout_seconds=budget, worker_terminal=True,
            ), 1)
        if not ok:
            return BoundedIntelligenceOutcome(_failed(
                str(getattr(value, "stage", "") or last_stage),
                type(value).__name__, str(value), worker_terminal=True,
            ), 1)
        job_payload = value
    else:
        job_payload = dict(payload or {})

    context = multiprocessing.get_context("spawn")
    parent_connection, child_connection = context.Pipe(duplex=False)
    process = context.Process(
        target=_intelligence_worker_entry,
        args=(worker, job_payload, child_connection, dict(exception_exit_codes or {})),
        name="probiga-intelligence-job",
    )
    started = False
    state_lock = threading.Lock()
    state: dict[str, Any] = {"last_stage": "worker_start", "result": None, "error": None}
    stop_transport = threading.Event()
    reader_done = threading.Event()
    output_idle = threading.Event()
    output_idle.set()
    progress_queue: queue.Queue = queue.Queue(maxsize=64)

    def transport_state() -> dict[str, Any]:
        with state_lock:
            return dict(state)

    def deliver_progress() -> None:
        while not stop_transport.is_set():
            try:
                progress = progress_queue.get(timeout=0.05)
            except queue.Empty:
                continue
            output_idle.clear()
            try:
                if progress_fn is not None and not stop_transport.is_set():
                    progress_fn(progress)
            except Exception as exc:
                with state_lock:
                    state["error"] = exc
            finally:
                output_idle.set()

    def read_worker_pipe() -> None:
        try:
            while not stop_transport.is_set():
                # A partially sent frame cannot block the supervisor's clock:
                # only this daemon transport reader performs a blocking recv.
                message = parent_connection.recv()
                kind = message.get("kind")
                if kind == "stage":
                    with state_lock:
                        state["last_stage"] = str(message.get("stage") or state["last_stage"])
                elif kind == "result":
                    with state_lock:
                        if state["result"] is not None:
                            raise RuntimeError("intelligence worker emitted multiple terminal results")
                        state["result"] = dict(message)
                elif kind == "progress" and progress_fn is not None:
                    progress = dict(message.get("payload") or {})
                    try:
                        progress_queue.put_nowait(progress)
                    except queue.Full:
                        # Progress is a live projection of durable checkpoints.
                        # Preserve recent state rather than blocking business or
                        # retaining an unbounded backlog behind a slow stdout.
                        try:
                            progress_queue.get_nowait()
                        except queue.Empty:
                            pass
                        try:
                            progress_queue.put_nowait(progress)
                        except queue.Full:
                            pass
        except (EOFError, BrokenPipeError):
            pass
        except Exception as exc:
            with state_lock:
                state["error"] = exc
        finally:
            reader_done.set()

    try:
        if time.monotonic() >= deadline:
            return BoundedIntelligenceOutcome(_failed(
                last_stage, "IntelligenceJobTimeout", "JOB_DEADLINE_EXCEEDED",
                timeout_seconds=budget, worker_terminal=True,
            ), 1)
        last_stage = "worker_start"
        # Reject unpicklable callables before Windows can start a partially
        # initialized child process during spawn serialization.
        ForkingPickler.dumps((worker, job_payload))
        process.start()
        started = True
        child_connection.close()
        threading.Thread(target=read_worker_pipe, name="intelligence-worker-pipe", daemon=True).start()
        if progress_fn is not None:
            threading.Thread(target=deliver_progress, name="intelligence-cli-progress", daemon=True).start()
        while True:
            observed = transport_state()
            last_stage = observed["last_stage"]
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                _stop_worker(process)
                return BoundedIntelligenceOutcome(_failed(
                    last_stage, "IntelligenceJobTimeout", "JOB_DEADLINE_EXCEEDED",
                    timeout_seconds=budget, worker_terminal=True,
                    worker_exit_code=process.exitcode,
                ), 1)
            if observed["error"] is not None:
                raise observed["error"]
            if not process.is_alive():
                process.join(timeout=0)
                if reader_done.is_set() and output_idle.is_set() and progress_queue.empty():
                    break
            if reader_done.is_set():
                # EOF is already known; waiting on its set event would spin
                # while bounded progress output is still draining.
                stop_transport.wait(timeout=min(0.01, remaining))
            else:
                reader_done.wait(timeout=min(0.01, remaining))

        child_result = transport_state()["result"]
        receipt = dict((child_result or {}).get("receipt") or {})
        child_exit_code = int((child_result or {}).get("exit_code", 1))
        if process.exitcode != 0 or child_exit_code != 0:
            if receipt.get("status") == "FAILED":
                receipt.setdefault("last_stage", last_stage)
                receipt["worker_terminal"] = True
                return BoundedIntelligenceOutcome(receipt, max(1, child_exit_code))
            return BoundedIntelligenceOutcome(_failed(
                last_stage, "IntelligenceWorkerExitError", "WORKER_EXIT_FAILED",
                worker_terminal=True, worker_exit_code=process.exitcode,
            ), 1)
        accepted: queue.Queue = queue.Queue(maxsize=1)

        def validate_receipt() -> None:
            try:
                accepted.put((True, bool(receipt and receipt_acceptor(receipt))))
            except Exception as exc:
                accepted.put((False, exc))

        threading.Thread(target=validate_receipt, name="intelligence-receipt-check", daemon=True).start()
        try:
            validation_ok, validation = accepted.get(timeout=max(0.0, deadline - time.monotonic()))
        except queue.Empty:
            return BoundedIntelligenceOutcome(_failed(
                last_stage, "IntelligenceJobTimeout", "JOB_DEADLINE_EXCEEDED",
                timeout_seconds=budget, worker_terminal=True,
            ), 1)
        if not validation_ok:
            raise validation
        if time.monotonic() >= deadline:
            return BoundedIntelligenceOutcome(_failed(
                last_stage, "IntelligenceJobTimeout", "JOB_DEADLINE_EXCEEDED",
                timeout_seconds=budget, worker_terminal=True,
            ), 1)
        if not validation:
            return BoundedIntelligenceOutcome(_failed(
                last_stage, "IntelligenceReceiptError", "BUSINESS_RECEIPT_UNPROVEN",
                worker_terminal=True,
            ), 1)
        return BoundedIntelligenceOutcome(receipt, 0)
    except Exception as exc:
        if started:
            _stop_worker(process)
        return BoundedIntelligenceOutcome(_failed(
            last_stage, type(exc).__name__, str(exc), worker_terminal=True,
        ), 1)
    finally:
        stop_transport.set()
        child_connection.close()
        if started:
            _stop_worker(process)
        parent_connection.close()
        process.close()


__all__ = [
    "BoundedIntelligenceOutcome", "IntelligenceJobReporter", "run_bounded_intelligence_job",
]
