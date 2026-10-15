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

The first repair (4905897) made the retry window-aware but kept a fixed
`MAX_ATTEMPTS = 2`, so the count - which knows nothing about the window - still
decided.  Re-reading the same day's 31 lease losses shows both halves:

    18x scheduler_lease_cycle_deadline_exceeded
     7x scheduler_lease_activation_unavailable:qmt_release_activation_check_failed:OperationalError
     2x scheduler_lease_identity_unavailable:['heartbeat_read_failed']
     2x scheduler_lease_activation_unavailable:QMT_EDGE_RELEASE_ACTIVATION_PENDING
     2x scheduler_lease_transport_failed:OperationalError

Two slow cycles consume 60s of a 120s window, so a third attempt both fits and
should run; the fixed count abandoned the lease anyway.  And the activation
probe's `..._check_failed:<ErrorType>` form is the probe saying it could not be
*evaluated*, which says nothing about ownership - the same category as a slow
cycle, not the evaluated `..._PENDING` answer.
"""
from __future__ import annotations

import threading
import time

import pytest

from server.api import scheduler_runtime as runtime


class _Clock:
    """A monotonic clock the replayed cycles advance by their real budget.

    A stub that returns instantly would let three 30s attempts "fit" in a 120s
    window for free, which is exactly the accounting error the fixed attempt
    count made.  Replaying the budget keeps the retry decision timed the way
    production times it - `_run` reads nothing but `time.monotonic()`.
    """

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class _DoneStub:
    """Let `_run` through `allow` waits, then finish the loop."""

    def __init__(self, allow: int) -> None:
        self.allow = allow
        self.calls = 0

    def wait(self, timeout: float | None = None) -> bool:
        self.calls += 1
        return self.calls >= self.allow

    def is_set(self) -> bool:
        return False

    def set(self) -> None:
        pass


POLL_SECONDS = 60.0
# What a successful renew grants, and the window `authorized()` gates on.
GRANTED_WINDOW_SECONDS = 2 * POLL_SECONDS


def _lease(monkeypatch, *, authorized_for: float, cycle_results,
           allow: int, cycle_seconds: float = 30.0):
    """Build a lease whose `_cycle` replays `cycle_results`.

    Only the attributes `_run` actually reads are set, so the test exercises the
    real retry decision rather than a reimplementation of it.
    """
    shutdown_calls: list[bool] = []
    monkeypatch.setattr(runtime, "begin_scheduler_shutdown",
                        lambda: shutdown_calls.append(True))

    clock = _Clock()
    monkeypatch.setattr(runtime.time, "monotonic", clock)

    lease = object.__new__(runtime._WindowsSchedulerLease)
    lease.done = _DoneStub(allow=allow)
    lease.stop_event = threading.Event()
    lease.lost = threading.Event()
    lease.verified = threading.Event()
    lease.verified.set()
    lease.interval_seconds = 0.001
    lease.cycle_seconds = cycle_seconds
    lease.authorized_until = clock() + authorized_for
    lease._last_cycle_evidence_json = "{}"

    results = list(cycle_results)
    calls: list[int] = []

    def cycle() -> bool:
        calls.append(len(calls) + 1)
        clock.advance(cycle_seconds)
        outcome = results.pop(0) if results else True
        if isinstance(outcome, BaseException):
            raise outcome
        return True

    lease._cycle = cycle
    return lease, calls, shutdown_calls


SLOW = runtime.SchedulerOwnerLeaseLost("scheduler_lease_cycle_deadline_exceeded")
ACTIVATION_UNEVALUATED = runtime.SchedulerOwnerLeaseLost(
    "scheduler_lease_activation_unavailable"
    ":qmt_release_activation_check_failed:OperationalError")


def test_slow_cycle_within_the_granted_window_does_not_drain_the_scheduler(monkeypatch):
    """The reported regression: a retry must happen, not a shutdown."""
    lease, calls, shutdown_calls = _lease(
        monkeypatch, authorized_for=GRANTED_WINDOW_SECONDS,
        cycle_results=[SLOW, True], allow=3)

    lease._run()

    assert calls == [1, 2], "the slow cycle must be retried, not abandoned"
    assert shutdown_calls == [], "a slow cycle must not begin scheduler shutdown"
    assert not lease.lost.is_set(), "the DB lease still named this owner"
    assert not lease.stop_event.is_set(), "in-flight jobs must not be drained"


def test_retries_continue_while_the_granted_window_still_covers_an_attempt(monkeypatch):
    """Two slow attempts leave 60s of a 120s window; a third attempt fits.

    This is the half `4905897` did not fix: `MAX_ATTEMPTS = 2` abandoned a lease
    the DB still granted, and the shutdown it started is what killed task 114
    mid-run.  The window, not a count, decides.
    """
    lease, calls, shutdown_calls = _lease(
        monkeypatch, authorized_for=GRANTED_WINDOW_SECONDS,
        cycle_results=[SLOW, SLOW, True], allow=4)

    lease._run()

    assert calls == [1, 2, 3], "a third attempt still fits inside the granted window"
    assert shutdown_calls == [], "the granted window had not run out"
    assert not lease.lost.is_set()
    assert not lease.stop_event.is_set()


def test_renewal_gives_up_when_no_attempt_can_fit_in_the_granted_window(monkeypatch):
    """The safety bound is unchanged: a window that cannot cover a cycle is fatal."""
    lease, calls, shutdown_calls = _lease(
        monkeypatch, authorized_for=60.0, cycle_results=[SLOW] * 5, allow=2)

    lease._run()

    assert calls == [1], "no retry is allowed once the window cannot cover one"
    assert shutdown_calls == [True], "losing a genuinely expired lease must drain"
    assert lease.lost.is_set()
    assert lease.stop_event.is_set()


def test_an_expired_window_is_still_ownership_loss(monkeypatch):
    lease, calls, shutdown_calls = _lease(
        monkeypatch, authorized_for=0.0, cycle_results=[SLOW], allow=2)

    lease._run()

    assert calls == [1]
    assert shutdown_calls == [True]
    assert lease.lost.is_set()
    assert lease.stop_event.is_set()


def test_an_activation_probe_that_could_not_be_evaluated_is_retried(monkeypatch):
    """The probe failing to *run* is not an answer about ownership.

    7 of the day's 31 losses were this shape.  It must not drain in-flight jobs.
    """
    lease, calls, shutdown_calls = _lease(
        monkeypatch, authorized_for=GRANTED_WINDOW_SECONDS,
        cycle_results=[ACTIVATION_UNEVALUATED, True], allow=3)

    lease._run()

    assert calls == [1, 2], "an unevaluable activation probe must be retried"
    assert shutdown_calls == []
    assert not lease.lost.is_set()


def test_an_owner_readback_that_failed_to_run_is_retried(monkeypatch):
    """The other "could not be evaluated" shape (2 losses that day).

    `_standalone_heartbeat_allows_dispatch` returns this token only from the
    `except Exception` arm around the readback query, so it is a transport
    failure, not a verdict about the owner row.
    """
    lease, calls, shutdown_calls = _lease(
        monkeypatch, authorized_for=GRANTED_WINDOW_SECONDS,
        cycle_results=[runtime.SchedulerOwnerLeaseLost(
            "scheduler_lease_identity_unavailable:['heartbeat_read_failed']"), True],
        allow=3)

    lease._run()

    assert calls == [1, 2], "an unevaluable owner readback must be retried"
    assert shutdown_calls == []
    assert not lease.lost.is_set()


@pytest.mark.parametrize("reason", [
    "scheduler_lease_identity_unavailable:{'errors': ['not_owner']}",
    "scheduler_lease_identity_unavailable:['not_owner']",
    "scheduler_lease_activation_unavailable:QMT_EDGE_RELEASE_ACTIVATION_PENDING",
    "scheduler_lease_activation_unavailable:build_identity_unavailable",
    "scheduler_lease_stopping",
])
def test_reasons_that_prove_ownership_loss_stay_fatal_on_first_occurrence(
        monkeypatch, reason):
    """A real ownership loss must never be retried on a stale window."""
    lease, calls, shutdown_calls = _lease(
        monkeypatch, authorized_for=GRANTED_WINDOW_SECONDS,
        cycle_results=[runtime.SchedulerOwnerLeaseLost(reason)], allow=2)

    lease._run()

    assert calls == [1], "ownership loss must not be retried"
    assert shutdown_calls == [True]
    assert lease.lost.is_set()
