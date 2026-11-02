"""Renewable ownership for an exact scheduler audit, including manual jobs.

The running audit's output is its lease record. Terminal publication replaces
that record atomically; a renew can never overwrite a terminal receipt.
"""
from __future__ import annotations

from datetime import datetime, timedelta
import json
import re
import threading
import time
from typing import Callable

from sqlalchemy import text

from server.common.engine_factory import create_pooled_engine

SCHEMA = "probiga.scheduler-task-owner-lease.v1"
LEASE_SECONDS = 900
RENEW_SECONDS = 15
MAX_RENEW_SECONDS = 90
IDENTITY_FIELDS = (
    "run_uid", "task_id", "task_name", "task_type", "run_at", "host_name",
    "scheduler_instance_id", "build_sha", "trigger_source",
)


def _datetime(value) -> datetime:
    result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if result.tzinfo is not None:
        raise ValueError("scheduler owner lease requires the database wall clock")
    return result.replace(microsecond=0)


def identity(row: dict) -> dict:
    result = {name: row.get(name) for name in IDENTITY_FIELDS}
    result["run_at"] = _datetime(result["run_at"]).isoformat(sep=" ")
    if (type(result["task_id"]) is not int or result["task_id"] < 1
            or re.fullmatch(r"[0-9a-f]{32,64}", str(result["run_uid"])) is None
            or re.fullmatch(r"[0-9a-f]{40}", str(result["build_sha"])) is None
            or result["trigger_source"] not in {"manual", "scheduled", "release_catchup"}
            or not all(isinstance(result[name], str) and result[name]
                       for name in ("host_name", "scheduler_instance_id", "task_type"))
            or re.fullmatch(re.escape(result["host_name"]) + r"-[1-9][0-9]*",
                            result["scheduler_instance_id"]) is None):
        raise ValueError("scheduler owner lease run identity differs")
    return result


def encode(run_identity: dict, renewed_at: datetime) -> str:
    run_identity = identity(run_identity)
    renewed_at = _datetime(renewed_at)
    if renewed_at < _datetime(run_identity["run_at"]):
        raise ValueError("scheduler owner lease clock precedes its claim")
    return json.dumps(dict(schema=SCHEMA, identity=run_identity,
        renewed_at=renewed_at.isoformat(sep=" "),
        lease_until=(renewed_at + timedelta(seconds=LEASE_SECONDS)).isoformat(sep=" ")),
        ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def read(history: dict) -> dict | None:
    try:
        payload = json.loads(str(history.get("output") or ""))
    except (ValueError, TypeError):
        return None
    if not isinstance(payload, dict) or payload.get("schema") != SCHEMA:
        return None
    expected = identity(history)
    if payload.get("identity") != expected:
        raise ValueError("scheduler owner lease original run tuple differs")
    renewed_at, until = _datetime(payload["renewed_at"]), _datetime(payload["lease_until"])
    if (renewed_at < _datetime(expected["run_at"])
            or until - renewed_at != timedelta(seconds=LEASE_SECONDS)):
        raise ValueError("scheduler owner lease time contract differs")
    return {**payload, "renewed_at": renewed_at, "lease_until": until}


def expired(history: dict, *, now: datetime) -> bool:
    payload = read(history)
    return payload is not None and payload["lease_until"] < _datetime(now)


class TaskOwnerLease:
    """Renew only this run; loss may stop only its independently bound child."""

    def __init__(self, engine, run_identity: dict, initial_output: str,
                 *, clock: Callable[[], datetime], on_lost: Callable[[], None]):
        self._owns_engine = str(engine.dialect.name).lower() == "mysql"
        self.engine = (create_pooled_engine(engine.url, pool_size=1, max_overflow=0,
            pool_timeout=5, connect_args={"connect_timeout": 8, "read_timeout": 25,
                                         "write_timeout": 25})
            if self._owns_engine else engine)
        self.identity, self.output = identity(run_identity), initial_output
        self.clock, self.on_lost = clock, on_lost
        self.stop_event, self.lost = threading.Event(), threading.Event()
        self.lock = threading.Lock()
        self.thread: threading.Thread | None = None

    def authorized(self) -> bool:
        with self.lock:
            lease = read({**self.identity, "output": self.output})
        return (not self.lost.is_set() and lease is not None
                and lease["renewed_at"] <= self.clock() <= lease["lease_until"])

    def renew(self) -> bool:
        """Return false for a terminal audit; never rewrite terminal output."""
        started = time.monotonic()
        with self.engine.begin() as connection:
            suffix = "" if connection.dialect.name == "sqlite" else " FOR UPDATE"
            row = connection.execute(text(
                "SELECT run_uid,task_id,task_name,task_type,run_at,host_name,"
                "scheduler_instance_id,build_sha,trigger_source,status,finished_at,output "
                "FROM st_scheduled_task_history WHERE run_uid=:run_uid" + suffix
            ), {"run_uid": self.identity["run_uid"]}).mappings().one_or_none()
            if row is None:
                raise RuntimeError("scheduler owner lease audit disappeared")
            row = dict(row)
            if identity(row) != self.identity:
                raise RuntimeError("scheduler owner lease audit identity changed")
            if row["status"] != "running" or row["finished_at"] is not None:
                return False
            with self.lock:
                previous = self.output
            lease = read(row)
            now = self.clock().replace(microsecond=0)
            if time.monotonic() - started >= MAX_RENEW_SECONDS:
                raise RuntimeError("scheduler owner lease renewal read deadline exceeded")
            if (lease is None or row["output"] != previous
                    or not lease["renewed_at"] <= now <= lease["lease_until"]):
                raise RuntimeError("scheduler owner lease changed or expired")
            renewed = encode(self.identity, now)
            # An immediate renew can be byte-identical on MySQL (changed-row
            # count zero). The locked read already proved this exact lease.
            if renewed != previous:
                changed = connection.execute(text(
                    "UPDATE st_scheduled_task_history SET output=:renewed "
                    "WHERE run_uid=:run_uid AND status='running' AND finished_at IS NULL "
                    "AND output=:previous"
                ), dict(renewed=renewed, run_uid=self.identity["run_uid"], previous=previous))
                if changed.rowcount != 1:
                    raise RuntimeError("scheduler owner lease renewal CAS lost")
        if time.monotonic() - started >= MAX_RENEW_SECONDS:
            raise RuntimeError("scheduler owner lease renewal commit deadline exceeded")
        with self.lock:
            self.output = renewed
        return True

    def _lose(self) -> None:
        self.lost.set()
        self.on_lost()

    def _work(self) -> None:
        failures = 0
        while not self.stop_event.wait(RENEW_SECONDS):
            try:
                if not self.renew():
                    return
                failures = 0
            except Exception:
                failures += 1
                if failures >= 2 or not self.authorized():
                    self._lose()
                    return

    def start(self) -> None:
        if self.thread is not None:
            raise RuntimeError("scheduler owner lease already started")
        # Prove the committed original before preflight or child launch.
        if not self.renew():
            raise RuntimeError("scheduler owner lease is already terminal")
        self.thread = threading.Thread(target=self._work, daemon=True,
            name="scheduler-task-lease-" + self.identity["run_uid"][:12])
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=2.0)
        if self._owns_engine:
            self.engine.dispose()
