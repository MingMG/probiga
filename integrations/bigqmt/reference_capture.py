"""Immutable native reference partitions and their sealed publication input.

This is source evidence, not a release-success cache. A failed publisher can
replay its sealed input; it cannot manufacture input from mutable SQL tables.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
import gzip
import hashlib
import json
import os
import re
from pathlib import Path
import stat
import tempfile
import time
from typing import Any

import pandas as pd

from integrations.bigqmt.spool import bridge_dir, _replace_with_retry
from integrations.bigqmt.release_identity import validate_strategy_release_payload

SCHEMA = "probiga.qmt-reference-capture.v1"
ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "integrations/bigqmt/qmt_strategy/probiga_big_qmt_bridge.py"
MAX_BYTES = 128 * 1024 * 1024
MAX_DISK_BYTES = 512 * 1024 * 1024


def _encode(value: Any) -> Any:
    if isinstance(value, pd.DataFrame):
        return {"__frame__": True, "columns": list(value.columns),
                "rows": _encode(value.to_dict("records")), "attrs": _encode(value.attrs)}
    if isinstance(value, dict):
        return {str(key): _encode(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_encode(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if hasattr(value, "item"):
        return _encode(value.item())
    if isinstance(value, float) and pd.isna(value):
        return None
    return value


def _decode(value: Any) -> Any:
    if isinstance(value, dict):
        if value.get("__frame__") is True:
            frame = pd.DataFrame(value["rows"], columns=value["columns"])
            frame.attrs = value["attrs"]
            return frame
        return {key: _decode(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_decode(item) for item in value]
    return value


def _canonical(value: Any) -> bytes:
    return json.dumps(_encode(value), ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _ordinary(path: Path) -> None:
    for item in (path, *path.parents):
        if not item.exists() and not item.is_symlink():
            continue
        info = item.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise RuntimeError("DATA_BLOCKED: reference capture path is a link/reparse point")


def _identity(evidence: dict[str, Any], expected_build_sha: str) -> dict[str, Any]:
    if re.fullmatch(r"[0-9a-f]{40}", str(evidence.get("collector_build_sha") or "")) is None or evidence["collector_build_sha"] == "0" * 40:
        raise RuntimeError("DATA_BLOCKED: reference collector identity missing")
    proofs = []
    for key in ("capabilities_before", "capabilities_after"):
        capability = evidence[key]
        proof = validate_strategy_release_payload(capability, expected_build_sha=expected_build_sha,
                                                   root=ROOT, source_path=SOURCE)
        proof.update(model_instance_id=capability.get("model_instance_id"))
        # The immutable source artifact, not an unrelated publisher SHA, binds
        # this evidence. Both original observation identities must still agree.
        proof.pop("compatible_app_build_sha", None)
        proof.pop("strategy_compatibility_status", None)
        if not proof["model_instance_id"]:
            raise RuntimeError("DATA_BLOCKED: reference model identity missing")
        proofs.append(proof)
    if proofs[0] != proofs[1]:
        raise RuntimeError("DATA_BLOCKED: reference model changed during capture")
    if "identity" in evidence:
        observed = dict(evidence["identity"])
        observed.pop("compatible_app_build_sha", None)
        observed.pop("strategy_compatibility_status", None)
        if observed != proofs[0]:
            raise RuntimeError("DATA_BLOCKED: reference frozen identity differs")
    started, captured = (datetime.fromisoformat(evidence[key])
                         for key in ("started_at", "captured_at"))
    if started.tzinfo is not None or captured.tzinfo is not None or started > captured:
        raise RuntimeError("DATA_BLOCKED: reference observation interval differs")
    return proofs[0]


class ReferenceCaptureStore:
    def __init__(self, scope: dict[str, Any], *, expected_build_sha: str,
                 root: Path | None = None, timeout_seconds: float = 1800):
        base = root or bridge_dir() / "reference_captures"
        self.scope = _encode(scope)
        self.root = base / _digest(scope)
        self.expected_build_sha = expected_build_sha
        self.deadline = time.monotonic() + timeout_seconds
        self.used: list[str] = []
        self.completed_units = 0
        self.current_unit = None
        self.current_stage = "CAPTURE_NOT_STARTED"
        self.verified_count = 0
        self.last_progress_at = None
        _ordinary(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

    def remaining(self, stage: str) -> float:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"QMT_REFERENCE_DEADLINE: {stage}")
        return remaining

    @contextmanager
    def locked(self):
        path = self.root / ".owner.lock"
        _ordinary(path)
        with path.open("a+b") as stream:
            if path.stat().st_size == 0:
                stream.write(b"0")
                stream.flush()
            stream.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                yield self
            except BaseException as exc:
                # Record a terminal failure while the failing owner still
                # holds the lock. An old exception cannot overwrite a later
                # owner's COMPLETE progress after releasing/reacquiring it.
                try:
                    self.progress("FAILED", self.current_stage,
                                  error=type(exc).__name__ + ":" + str(exc))
                except Exception:
                    pass  # Storage errors must not obscure the original stage.
                raise
            finally:
                stream.seek(0)
                if os.name == "nt":
                    msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(stream.fileno(), fcntl.LOCK_UN)

    def _write(self, name: str, payload: dict[str, Any]) -> dict[str, Any]:
        payload = _encode({"schema": SCHEMA, "scope": self.scope, **payload})
        payload["sha256"] = _digest(payload)
        raw = _canonical(payload)
        if len(raw) > MAX_BYTES:
            raise RuntimeError("DATA_BLOCKED: reference capture exceeds bounded size")
        encoded = gzip.compress(raw, mtime=0)
        path = self.root / name
        _ordinary(path)
        if name != "progress.json.gz" and path.exists():
            existing = self._read(name)
            if existing["sha256"] != payload["sha256"]:
                raise RuntimeError("DATA_BLOCKED: reference capture is immutable")
            return existing
        disk_used = sum(item.stat().st_size for item in self.root.parent.rglob("*.json.gz"))
        if disk_used + len(encoded) > MAX_DISK_BYTES:
            raise RuntimeError("DATA_BLOCKED: reference capture disk budget exhausted")
        fd, temporary = tempfile.mkstemp(prefix=".writing-", dir=self.root)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            _replace_with_retry(Path(temporary), path)
        finally:
            Path(temporary).unlink(missing_ok=True)
        return payload

    def _read(self, name: str) -> dict[str, Any] | None:
        path = self.root / name
        _ordinary(path)
        if not path.exists():
            return None
        try:
            with gzip.open(path, "rb") as stream:
                raw = stream.read(MAX_BYTES + 1)
            if len(raw) > MAX_BYTES:
                raise ValueError("bounded size exceeded")
            payload = json.loads(raw)
            unsigned = {key: value for key, value in payload.items() if key != "sha256"}
            if (payload.get("schema") != SCHEMA or payload.get("scope") != self.scope
                    or payload.get("sha256") != _digest(unsigned)):
                raise ValueError("checksum/contract differs")
            return payload
        except (OSError, ValueError, EOFError) as exc:
            raise RuntimeError(f"DATA_BLOCKED: invalid reference capture {name}") from exc

    def read_partition(self, request: dict[str, Any], session) -> Any:
        self.remaining(request["method"])
        key = _digest(request)
        self.current_unit = key
        self.progress("RUNNING", "native:" + request["method"])
        payload = self._read(f"partition-{key}.json.gz")
        if payload is not None:
            identity = _identity(payload["evidence"], self.expected_build_sha)
            current = dict(session.identity)
            current.pop("compatible_app_build_sha", None)
            current.pop("strategy_compatibility_status", None)
            if identity != current or payload.get("request") != _encode(request):
                raise RuntimeError("DATA_BLOCKED: pending reference partition model/request differs")
        else:
            started = datetime.now().isoformat(timespec="microseconds")
            kwargs = dict(request["kwargs"])
            kwargs["timeout"] = min(float(kwargs.get("timeout", 180)), self.remaining(request["method"]))
            value = session.read(getattr(session.source_bridge, request["method"]),
                                 *request["args"], **kwargs)
            session.verify_complete()
            payload = self._write(f"partition-{key}.json.gz", {
                "request": request, "value": value,
                "evidence": {"started_at": started,
                             "captured_at": datetime.now().isoformat(timespec="microseconds"),
                             "collector_build_sha": self.expected_build_sha,
                             "capabilities_before": session.initial_capabilities,
                             "capabilities_after": session.last_capabilities},
            })
        self.used.append(key)
        self.progress("RUNNING", request["method"])
        return _decode(payload["value"])

    def progress(self, state: str, stage: str, *, error: str = "") -> None:
        stage_number = {"STOCK_DISCOVERY_VERIFIED": 1, "INSTRUMENT_DETAILS_VERIFIED": 2,
                        "PUBLISHING": 3, "ORDINARY_TABLES_PUBLISHED": 4,
                        "IMMUTABLE_PAIR_COMMITTED": 5, "READBACK_VERIFIED": 6,
                        "REFERENCE_PUBLISHED": 7}.get(stage, self.completed_units)
        count = len(set(self.used))
        if stage_number > self.completed_units or count > self.verified_count:
            self.last_progress_at = datetime.now().isoformat(timespec="microseconds")
        self.completed_units = max(self.completed_units, stage_number)
        self.verified_count = count
        self.current_stage = stage
        self._write("progress.json.gz", {"state": state, "stage": stage,
                    "total_units": 7, "completed_units": self.completed_units,
                    "verified_source_partitions": len(set(self.used)),
                    "current_unit": self.current_unit,
                    "last_progress_at": self.last_progress_at,
                    "error": error})

    def seal(self, snapshot: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
        identity = _identity(evidence, self.expected_build_sha)
        if not self.used:
            raise RuntimeError("DATA_BLOCKED: reference capture has no native partitions")
        partitions = {key: self._read(f"partition-{key}.json.gz")["sha256"]
                      for key in sorted(set(self.used))}
        partition_evidence = [self._read(f"partition-{key}.json.gz")["evidence"] for key in partitions]
        if any(_identity(item, self.expected_build_sha) != identity for item in partition_evidence):
            raise RuntimeError("DATA_BLOCKED: reference capture partition identities differ")
        evidence = dict(evidence)
        evidence["started_at"] = min([evidence["started_at"], *[item["started_at"] for item in partition_evidence]])
        payload = self._write("sealed.json.gz", {"snapshot": snapshot,
                              "evidence": evidence, "partitions": partitions})
        self.progress("RUNNING", "PUBLISHING")
        return _decode(payload["snapshot"])

    def load_sealed(self) -> dict[str, Any] | None:
        payload = self._read("sealed.json.gz")
        if payload is None:
            return None
        identity = _identity(payload["evidence"], self.expected_build_sha)
        captured = datetime.fromisoformat(payload["evidence"]["captured_at"])
        if captured.date() != date.today() or captured > datetime.now():
            raise RuntimeError("DATA_BLOCKED: reference capture validity interval differs")
        for key, digest in payload["partitions"].items():
            partition = self._read(f"partition-{key}.json.gz")
            if partition is None or partition["sha256"] != digest:
                raise RuntimeError("DATA_BLOCKED: sealed reference partition missing/changed")
            if _identity(partition["evidence"], self.expected_build_sha) != identity:
                raise RuntimeError("DATA_BLOCKED: sealed reference model inventory differs")
        self.used = list(payload["partitions"])
        self.completed_units = 3
        return _decode(payload["snapshot"])


class CheckpointedReferenceSession:
    def __init__(self, session, store: ReferenceCaptureStore):
        self.session, self.store = session, store
        self.started_at = session.started_at

    def __getattr__(self, name):
        if name == "capabilities":
            def capabilities(**kwargs):
                kwargs["timeout"] = min(float(kwargs.get("timeout", 60)), self.store.remaining("capabilities"))
                return self.session.capabilities(**kwargs)
            return capabilities
        if name not in self.session.METHODS:
            return getattr(self.session, name)
        def read(*args, **kwargs):
            return self.store.read_partition({"method": name, "args": args, "kwargs": kwargs}, self.session)
        return read
