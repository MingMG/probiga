"""Durable closed-session QMT capture evidence; never publication authority."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta
from decimal import Decimal
from functools import wraps
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import tempfile
from uuid import uuid4
import zlib

import pandas as pd

from server.common.qmt_history_coverage import (
    COVERAGE_INCOMPLETE, canonical_digest, require_exact_coverage,
    validate_coverage_bundle,
)


SCHEMA = "probiga.qmt-minute-checkpoint.v1"
MAX_RAW_BYTES = 64 * 1024 * 1024
MAX_FILE_BYTES = 16 * 1024 * 1024
MAX_PENDING_BYTES = 512 * 1024 * 1024
MAX_AGE = timedelta(days=7)


class MinuteCheckpointInvalid(RuntimeError):
    """Existing durable evidence cannot be trusted; do not silently refetch."""


def _checkpoint_io(method):
    @wraps(method)
    def guarded(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except OSError as exc:
            if method.__name__ == "__init__":
                self.close()
            raise MinuteCheckpointInvalid(
                f"local minute checkpoint I/O failed: {method.__name__}"
            ) from exc
    return guarded


def _pack(value):
    if value is pd.NA or value is pd.NaT:
        return {"__type__": "missing"}
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else {"__type__": "float", "value": str(value)}
    if isinstance(value, (datetime, date)):
        return {"__type__": "timestamp", "value": value.isoformat()}
    if isinstance(value, Decimal):
        return {"__type__": "decimal", "value": str(value)}
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("checkpoint mappings require string keys")
        return {key: _pack(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_pack(item) for item in value]
    if hasattr(value, "item"):
        return _pack(value.item())
    raise TypeError(f"unsupported checkpoint value: {type(value).__name__}")


def _unpack(value):
    if isinstance(value, list):
        return [_unpack(item) for item in value]
    if not isinstance(value, dict):
        return value
    kind = value.get("__type__")
    if kind == "missing" and set(value) == {"__type__"}:
        return None
    if set(value) == {"__type__", "value"}:
        if kind == "timestamp":
            return pd.Timestamp(value["value"])
        if kind == "decimal":
            return Decimal(value["value"])
        if kind == "float" and value["value"] in {"nan", "inf", "-inf"}:
            return float(value["value"])
    return {key: _unpack(item) for key, item in value.items()}


def _bytes(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(value):
    return hashlib.sha256(_bytes(_pack(value))).hexdigest()


def frame_payload(frame):
    if not frame.columns.is_unique or any(not isinstance(column, str) for column in frame.columns):
        raise MinuteCheckpointInvalid("checkpoint frame columns are invalid")
    return {"columns": list(frame.columns), "rows": _pack(frame.to_dict("records")),
            "attrs": _pack(dict(frame.attrs))}


def frame_from_payload(payload):
    try:
        columns, rows, attrs = payload["columns"], payload["rows"], payload["attrs"]
        if (not isinstance(columns, list) or any(not isinstance(value, str) for value in columns)
                or len(columns) != len(set(columns)) or not isinstance(rows, list)
                or not isinstance(attrs, dict)
                or any(not isinstance(row, dict) or set(row) != set(columns) for row in rows)):
            raise ValueError("frame schema differs")
        frame = pd.DataFrame(_unpack(rows), columns=columns, dtype=object)
        frame.attrs = _unpack(attrs)
        return frame
    except (KeyError, TypeError, ValueError) as exc:
        raise MinuteCheckpointInvalid("checkpoint native frame is malformed") from exc


def stable_no_trade_identity(evidence):
    """The read cutoff/hash changes per inspection, while source roots do not."""
    if evidence is None:
        return None
    truth = {key: value for key, value in evidence["daily_truth"].items()
             if key not in {"decision_known_at", "truth_hash"}}
    return {"daily_truth": truth, "no_row_contract": evidence["no_row_contract"]}


def _ordinary(path):
    for item in [path, *path.parents]:
        if not item.exists() and not item.is_symlink():
            continue
        info = item.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise MinuteCheckpointInvalid("checkpoint paths cannot be links or reparse points")


@contextmanager
def _pending_capacity_lock(root):
    """Serialize the short disk-budget check/write across capture scopes."""
    path = root / ".pending-capacity.lock"
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
        yield


class MinuteCheckpoint:
    @_checkpoint_io
    def __init__(self, scope, frozen, *, root=None, now=None):
        current = (now or datetime.now()).replace(microsecond=0)
        self.scope = json.loads(_bytes(_pack(scope)))
        self.scope_hash = digest({"schema": SCHEMA, "scope": self.scope})
        if root is None:
            base = os.environ.get("PROBIGA_JOB_LOG_ROOT") or str(
                Path(os.environ.get("ProgramData", "C:/ProgramData")) / "ProBigA" / "jobs")
            root = Path(base) / "qmt-minute-checkpoints"
        root = Path(root).absolute()
        _ordinary(root)
        root.mkdir(parents=True, exist_ok=True)
        self.storage_root = root
        self.root = root / self.scope_hash
        _ordinary(self.root)
        self.root.mkdir(exist_ok=True)
        lock_path = root / (self.scope_hash + ".lock")
        _ordinary(lock_path)
        self._lock_file = lock_path.open("a+b")
        if lock_path.stat().st_size == 0:
            self._lock_file.write(b"0")
            self._lock_file.flush()
        self._lock_file.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self._lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self._lock_file.close()
            raise MinuteCheckpointInvalid("minute checkpoint already has an active owner") from exc
        try:
            self._initialize(frozen, current, root)
        except BaseException:
            self.close()
            raise

    def _initialize(self, frozen, current, root):
        manifest = self._load("manifest")
        if manifest is None and (any(self.root.glob("batch-*.json.gz"))
                                 or any(self.root.glob("pending-*.json.gz"))):
            raise MinuteCheckpointInvalid("checkpoint batches have no frozen manifest")
        if manifest is not None:
            try:
                created = datetime.fromisoformat(manifest["created_at"])
                expires = datetime.fromisoformat(manifest["expires_at"])
            except (TypeError, KeyError, ValueError) as exc:
                raise MinuteCheckpointInvalid("checkpoint lifetime is malformed") from exc
            if (created.tzinfo is not None or expires.tzinfo is not None
                    or created > current or expires != created + MAX_AGE):
                raise MinuteCheckpointInvalid("checkpoint lifetime is invalid")
            completed = self._load("completed")
            if completed is not None and (
                    completed.get("manifest_hash") != digest(manifest)
                    or not isinstance(completed.get("publication"), dict)
                    or completed["publication"].get("publication_state") != "PASS"):
                raise MinuteCheckpointInvalid("completed checkpoint publication identity differs")
            if current >= expires or completed is not None:
                # Failed/expired captures retain their native rows. Completed
                # captures retain compact receipts after publication succeeded.
                if completed is not None:
                    self._discard_published_batches()
                kind = ".completed-" if completed is not None else ".expired-"
                archived = root / (self.scope_hash + kind + uuid4().hex)
                self.root.rename(archived)
                self.root.mkdir()
                manifest = None
        if manifest is None:
            manifest = {"created_at": current.isoformat(),
                        "expires_at": (current + MAX_AGE).isoformat(),
                        "frozen": _pack(frozen)}
            self._save("manifest", manifest)
        self.manifest = manifest
        try:
            self.frozen = _unpack(manifest["frozen"])
            capture = datetime.fromisoformat(self.frozen["coverage_captured_at"])
            if (capture != datetime.fromisoformat(manifest["created_at"])
                    or not self.frozen["minute_run_id"] or not self.frozen["daily_run_id"]
                    or self.frozen["reference_roots"] != _unpack(self.scope["reference_roots"])):
                raise ValueError("frozen run/reference roots differ")
        except (KeyError, TypeError, ValueError) as exc:
            raise MinuteCheckpointInvalid("checkpoint frozen capture identity is invalid") from exc
        self.manifest_hash = digest(manifest)
        self.batch_hashes = []
        self.native_capture_receipts = []
        self._pending_records = self._read_pending_records()

    def close(self):
        stream = getattr(self, "_lock_file", None)
        if stream is not None and not stream.closed:
            # Closing the descriptor releases the OS lock, including on crash.
            stream.close()

    def _path(self, key):
        return self.root / (key + ".json.gz")

    def _load(self, key):
        path = self._path(key)
        _ordinary(path)
        if not path.exists():
            return None
        try:
            if path.stat().st_size > MAX_FILE_BYTES:
                raise ValueError("compressed checkpoint exceeds size limit")
            with gzip.open(path, "rb") as stream:
                raw = stream.read(MAX_RAW_BYTES + 1)
            if len(raw) > MAX_RAW_BYTES:
                raise ValueError("checkpoint exceeds size limit")
            record = json.loads(raw)
            if not isinstance(record, dict) or not isinstance(record.get("payload"), dict):
                raise ValueError("checkpoint record is malformed")
            checksum = record.pop("sha256")
            if (record["schema"] != SCHEMA or record["scope"] != self.scope
                    or record["key"] != key or digest(record) != checksum):
                raise ValueError("checkpoint identity or digest differs")
            return record["payload"]
        except (OSError, EOFError, zlib.error, ValueError, KeyError, TypeError) as exc:
            raise MinuteCheckpointInvalid(f"invalid durable minute checkpoint: {path.name}") from exc

    def _save(self, key, payload, *, remaining_pending_bytes=None):
        record = {"schema": SCHEMA, "scope": self.scope, "key": key, "payload": payload}
        record["sha256"] = digest(record)
        raw = _bytes(record)
        encoded = gzip.compress(raw, mtime=0)
        if len(raw) > MAX_RAW_BYTES or len(encoded) > MAX_FILE_BYTES:
            raise MinuteCheckpointInvalid("checkpoint exceeds bounded size")
        if remaining_pending_bytes is not None and len(encoded) > remaining_pending_bytes:
            raise MinuteCheckpointInvalid("pending minute evidence disk budget is exhausted")
        target = self._path(key)
        _ordinary(target)
        fd, temporary = tempfile.mkstemp(prefix=".writing-", dir=self.root)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    @_checkpoint_io
    def load_batch(self, codes):
        payload = self._load("batch-" + digest(list(codes)))
        if payload is None:
            return None
        if (not isinstance(payload, dict) or payload.get("codes") != list(codes)
                or payload.get("manifest_hash") != self.manifest_hash
                or not {"minute", "daily", "coverage", "source_receipts"}.issubset(payload)):
            raise MinuteCheckpointInvalid("batch frozen capture identity differs")
        require_exact_coverage(payload["coverage"])
        return payload

    @_checkpoint_io
    def save_batch(self, codes, *, minute, daily, coverage, source_receipts):
        require_exact_coverage(coverage)
        payload = {"codes": list(codes), "manifest_hash": self.manifest_hash,
                   "minute": frame_payload(minute), "daily": frame_payload(daily),
                   "coverage": coverage, "source_receipts": _pack(source_receipts)}
        existing = self.load_batch(codes)
        if existing is not None and digest(existing) != digest(payload):
            raise MinuteCheckpointInvalid("verified checkpoint batch is immutable")
        if existing is None:
            self._save("batch-" + digest(list(codes)), payload)
        self._remember(payload)

    def _validate_pending(self, key, payload):
        codes = payload.get("codes")
        if (not isinstance(codes, list) or not codes
                or any(not isinstance(code, str) for code in codes)
                or len(set(codes)) != len(codes)
                or payload.get("manifest_hash") != self.manifest_hash
                or payload.get("validation_status") != COVERAGE_INCOMPLETE
                or not {"minute", "daily", "coverage", "source_receipts"}.issubset(payload)
                or not isinstance(payload["source_receipts"], list)):
            raise MinuteCheckpointInvalid("pending batch capture identity differs")
        manifest = validate_coverage_bundle(payload["coverage"])
        roots = self.frozen["reference_roots"]
        if (manifest["status"] != COVERAGE_INCOMPLETE
                or manifest.get("dataset") != "stock_minute" or manifest.get("period") != "1m"
                or manifest["run_id"] != self.frozen["minute_run_id"]
                or manifest.get("source_batch_id") != self.frozen["minute_run_id"]
                or manifest.get("captured_at") != self.frozen["coverage_captured_at"]
                or manifest["expected_entity_set_hash"] != canonical_digest(sorted(codes))
                or any(manifest.get(field) != roots[field] for field in (
                    "catalog_batch_id", "catalog_manifest_hash", "calendar_batch_id",
                    "calendar_manifest_hash", "trade_date") if field in roots)
                or key != "pending-" + digest(codes) + "-" + digest(payload)):
            raise MinuteCheckpointInvalid("pending coverage identity differs")
        frame_from_payload(payload["minute"])
        frame_from_payload(payload["daily"])
        return {
            "key": key, "sha256": digest(payload), "codes": list(codes),
            "validation_status": COVERAGE_INCOMPLETE,
            "coverage_manifest": manifest,
            "source_receipts": payload["source_receipts"],
            "native_capture_receipts": {
                "minute": payload["minute"]["attrs"].get("bigqmt_capture"),
                "daily": payload["daily"]["attrs"].get("bigqmt_capture"),
            },
        }

    def _read_pending_records(self):
        records = {}
        for path in sorted(self.root.glob("pending-*.json.gz")):
            key = path.name.removesuffix(".json.gz")
            payload = self._load(key)
            if payload is None:
                raise MinuteCheckpointInvalid("pending capture disappeared while locked")
            records[key] = self._validate_pending(key, payload)
        return records

    @_checkpoint_io
    def save_pending_batch(self, codes, *, minute, daily, coverage, source_receipts):
        """Retain an INCOMPLETE response without granting reuse/publication."""
        payload = {"codes": list(codes), "manifest_hash": self.manifest_hash,
                   "validation_status": COVERAGE_INCOMPLETE,
                   "minute": frame_payload(minute), "daily": frame_payload(daily),
                   "coverage": coverage, "source_receipts": _pack(source_receipts)}
        key = "pending-" + digest(list(codes)) + "-" + digest(payload)
        record = self._validate_pending(key, payload)
        existing = self._load(key)
        if existing is not None:
            self._validate_pending(key, existing)
            if digest(existing) != digest(payload):
                raise MinuteCheckpointInvalid("pending source response is immutable")
        else:
            with _pending_capacity_lock(self.storage_root):
                used_bytes = 0
                # Include failed/expired captures from all scopes; a restart or
                # new build cannot reset the total retained-evidence budget.
                for path in self.storage_root.glob("*/pending-*.json.gz"):
                    _ordinary(path)
                    used_bytes += path.stat().st_size
                self._save(key, payload, remaining_pending_bytes=MAX_PENDING_BYTES - used_bytes)
        self._pending_records[key] = record
        return dict(record)

    @_checkpoint_io
    def pending_evidence(self):
        self._pending_records = self._read_pending_records()
        records = list(self._pending_records.values())
        return {"validation_status": "NOT_PUBLICATION_AUTHORITY",
                "response_count": len(records),
                "batch_count": len({digest(record["codes"]) for record in records}),
                "records": records}

    def verify_replayed_batch(self, payload, *, coverage, source_receipts):
        if (digest(payload["coverage"]) != digest(coverage)
                or digest(payload["source_receipts"]) != digest(source_receipts)):
            raise MinuteCheckpointInvalid("replayed native batch evidence differs")
        self._remember(payload)

    def _remember(self, payload):
        self.batch_hashes.append(digest(payload))
        self.native_capture_receipts.append({
            "codes": payload["codes"],
            "minute": payload["minute"]["attrs"].get("bigqmt_capture"),
            "daily": payload["daily"]["attrs"].get("bigqmt_capture"),
        })

    @_checkpoint_io
    def complete(self, publication):
        if publication.get("publication_state") != "PASS" or not self.batch_hashes:
            raise MinuteCheckpointInvalid("checkpoint cleanup requires complete publication")
        # The caller invokes this only after the durable final PASS receipt.
        # Write its compact native evidence before removing reproducible rows.
        self._save("completed", {"manifest_hash": self.manifest_hash,
                                 "publication": _pack(publication),
                                 "native_capture_receipts": self.native_capture_receipts,
                                 "pending_evidence": self.pending_evidence()})
        self._discard_published_batches()

    def _discard_published_batches(self):
        for pattern in ("batch-*.json.gz", "pending-*.json.gz"):
            for path in self.root.glob(pattern):
                _ordinary(path)
                path.unlink()

    def evidence(self):
        pending = self.pending_evidence()
        pending_summary = {key: value for key, value in pending.items() if key != "records"}
        pending_summary["records_sha256"] = digest(pending["records"])
        return {"schema": SCHEMA, "scope_hash": self.scope_hash,
                "manifest_hash": self.manifest_hash, "batch_hashes": list(self.batch_hashes),
                "pending_evidence": pending_summary}
