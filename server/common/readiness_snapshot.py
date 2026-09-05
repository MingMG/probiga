"""One read-only readiness refresh at a time; HTTP callers never queue work."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from threading import Event, Lock, Thread
from time import monotonic


class ReadinessSnapshot:
    def __init__(self, *, ttl_seconds=15.0, wait_seconds=0.5):
        self.ttl_seconds = ttl_seconds
        self.wait_seconds = wait_seconds
        self._lock = Lock()
        self._done = Event()
        self._running = False
        self._value = None
        self._finished = 0.0
        self._checked_at = None
        self._error = None

    def read(self, loader):
        with self._lock:
            fresh = self._finished and monotonic() - self._finished < self.ttl_seconds
            if not fresh and not self._running:
                self._running = True
                self._done.clear()
                Thread(target=self._refresh, args=(loader,), daemon=True,
                       name="probiga-readiness").start()
            initial = self._value is None
        if initial:
            self._done.wait(self.wait_seconds)
        with self._lock:
            age = monotonic() - self._finished if self._finished else None
            fresh = age is not None and age < self.ttl_seconds
            return (deepcopy(self._value) if fresh else None), {
                "checked_at": self._checked_at,
                "age_seconds": round(age, 3) if age is not None else None,
                "refreshing": self._running,
                "error_type": self._error,
                "stale": not fresh,
            }

    def _refresh(self, loader):
        value, error = None, None
        try:
            value = loader()
        except Exception as exc:
            error = type(exc).__name__
        finally:
            with self._lock:
                self._value = value
                self._error = error
                self._finished = monotonic()
                self._checked_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
                self._running = False
                self._done.set()
