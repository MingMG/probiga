"""A slow lease-renewal cycle must not be mistaken for ownership loss.

Production failure this reproduces (2026-10-14 21:54:12, Windows edge):

    ERROR Windows scheduler owner lease lost; stopping new dispatch:
          scheduler_lease_cycle_deadline_exceeded
      "budget_ms":30000.0, "elapsed_ms":30046.443,
      "last_success":{"elapsed_ms":29404.731,"stage":"renew_commit"}

    INFO  Scheduler shutdown started; stop dispatching new tasks
    INFO  Waiting for locally owned scheduler tasks before shutdown: tasks=(114,)
    INFO  任务 国金QMT近期标准历史缺口精确修复 完成: stopped (4970s)

The renewal cycle carries a 30s budget, but a successful renew grants
`authorized_until = began + 2 * poll_seconds` (120s at the shipped poll of 60s).
One slow attempt therefore drained the scheduler and killed an 83-minute repair
job while the DB lease still named this process as owner.  The scheduler
restarted four minutes later and the same thing happened again, all night.
"""
from __future__ import annotations

import threading
import time

import pytest

from server.api import scheduler_runtime as runtime


class _DoneStub:
    """Let `_run` through its first `allow` waits, then finish the loop."""

    def __init__(self, allow: int) -> None:
        self.allow = allow
        self.calls = 0

    def wait(self, timeout: float | None = None) -> bool:
        self.calls += 1
        return self.calls > self.allow

    def is_set(self) -> bool:
        return False

    def set(self) -> None:
        pass


def _lease(monkeypatch, *, authorized_for: float, cycle_results):
    """Build a lease whose `_cycle` replays `cycle_results`.

    Only the attributes `_run` actually reads are set, so the test exercises the
    real retry decision rather than a reimplementation of it.
    """
    shutdown_calls: list[bool] = []
    monkeypatch.setattr(runtime, "begin_scheduler_shutdown",
                        lambda: shutdown_calls.append(True))

    lease = object.__new__(runtime._WindowsSchedulerLease)
    lease.done = _DoneStub(allow=2)
    lease.stop_event = threading.Event()
    lease.lost = threading.Event()
    lease.verified = threading.Event()
    lease.verified.set()
    lease.interval_seconds = 0.001
    lease.cycle_seconds = 30.0
    lease.authorized_until = time.monotonic() + authorized_for
    lease._last_cycle_evidence_json = "{}"

    results = list(cycle_results)
    calls: list[int] = []

    def cycle() -> bool:
        calls.append(len(calls) + 1)
        outcome = results.pop(0) if results else True
        if isinstance(outcome, BaseException):
            raise outcome
        return True

    lease._cycle = cycle
    return lease, calls, shutdown_calls


SLOW = runtime.SchedulerOwnerLeaseLost("scheduler_lease_cycle_deadline_exceeded")


def test_slow_cycle_within_the_granted_window_does_not_drain_the_scheduler(monkeypatch):
    """The regression: a retry must happen, not a shutdown."""
    lease, calls, shutdown_calls = _lease(
        monkeypatch, authorized_for=90.0, cycle_results=[SLOW, True])

    lease._run()

    assert calls == [1, 2], "the slow cycle must be retried, not abandoned"
    assert shutdown_calls == [], "a slow cycle must not begin scheduler shutdown"
    assert not lease.lost.is_set(), "the DB lease still named this owner"
    assert not lease.stop_event.is_set(), "in-flight jobs must not be drained"


def test_slow_cycle_past_the_granted_window_is_still_ownership_loss(monkeypatch):
    """The safety bound is unchanged: an expired window is fatal."""
    lease, calls, shutdown_calls = _lease(
        monkeypatch, authorized_for=0.0, cycle_results=[SLOW])

    lease._run()

    assert calls == [1], "no retry is allowed once the window cannot cover one"
    assert shutdown_calls == [True], "losing a genuinely expired lease must drain"
    assert lease.lost.is_set()
    assert lease.stop_event.is_set()


def test_renewal_retry_budget_is_still_bounded(monkeypatch):
    """Two slow cycles exhaust MAX_ATTEMPTS and then lose the lease."""
    lease, calls, shutdown_calls = _lease(
        monkeypatch, authorized_for=90.0, cycle_results=[SLOW, SLOW])

    lease._run()

    assert calls == [1, 2]
    assert shutdown_calls == [True]


@pytest.mark.parametrize("reason", [
    "scheduler_lease_identity_unavailable:{'errors': ['not_owner']}",
    "scheduler_lease_activation_unavailable:not_ready",
    "scheduler_lease_stopping",
])
def test_reasons_that_prove_ownership_loss_stay_fatal_on_first_occurrence(
        monkeypatch, reason):
    """A real ownership loss must never be retried on a stale window."""
    lease, calls, shutdown_calls = _lease(
        monkeypatch, authorized_for=90.0,
        cycle_results=[runtime.SchedulerOwnerLeaseLost(reason)])

    lease._run()

    assert calls == [1], "ownership loss must not be retried"
    assert shutdown_calls == [True]
    assert lease.lost.is_set()
