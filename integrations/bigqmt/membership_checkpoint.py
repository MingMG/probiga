"""Durable, immutable membership captures; database publication is separate."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tempfile
from typing import Any

import pandas as pd

from integrations.bigqmt.release_identity import validate_strategy_release_payload
from integrations.bigqmt.spool import bridge_dir, _replace_with_retry


SCHEMA = "probiga.qmt-membership-capture.v1"
ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "integrations/bigqmt/qmt_strategy/probiga_big_qmt_bridge.py"
MAX_RAW_BYTES = 128 * 1024 * 1024
MAX_PENDING_BYTES = 512 * 1024 * 1024
CONTRACT_FILES = (
    "integrations/bigqmt/reference.py",
    "integrations/bigqmt/membership_checkpoint.py",
    "integrations/bigqmt/membership_snapshot.py",
    "tools/sync_bigqmt_reference.py",
)


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False, default=str).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


COLLECTOR_CONTRACT_HASH = _digest({
    name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
    for name in CONTRACT_FILES
})


class MembershipCaptureInvalid(RuntimeError):
    """Unverified or unavailable facts must never be silently recaptured."""


def _ordinary(path: Path) -> None:
    for item in (path, *path.parents):
        if not item.exists() and not item.is_symlink():
            continue
        info = item.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise MembershipCaptureInvalid("membership capture path is a link/reparse point")


def capture_root() -> Path:
    configured = str(os.environ.get("BIG_QMT_MEMBERSHIP_CAPTURE_DIR") or "").strip()
    return Path(configured) if configured else bridge_dir() / "membership_captures"


def validate_capture(payload: dict[str, Any], *, expected_build_sha: str) -> dict[str, Any]:
    try:
        if not isinstance(payload, dict):
            raise ValueError("capture must be an object")
        unsigned = {key: value for key, value in payload.items() if key != "sha256"}
        if (payload.get("schema") != SCHEMA or payload.get("sha256") != _digest(unsigned)
                or payload.get("collector_contract_sha256") != COLLECTOR_CONTRACT_HASH):
            raise ValueError("capture checksum or collector contract differs")
        target = date.fromisoformat(payload["snapshot_date"])
        evidence = payload["evidence"]
        started = datetime.fromisoformat(evidence["started_at"])
        captured = datetime.fromisoformat(evidence["captured_at"])
        if (started.tzinfo is not None or captured.tzinfo is not None
                or not datetime.combine(target, time(15, 10)) <= started <= captured
                or captured >= datetime.combine(target + timedelta(days=1), time.min)
                or re.fullmatch(r"[0-9a-f]{40}", str(evidence["collector_build_sha"])) is None
                or evidence["collector_build_sha"] == "0" * 40
                or evidence["identity"].get("compatible_app_build_sha") != evidence["collector_build_sha"]
                or not evidence["identity"].get("model_instance_id")):
            raise ValueError("capture is not one actual post-close session")
        identities = []
        for key in ("capabilities_before", "capabilities_after"):
            capability = evidence[key]
            proof = validate_strategy_release_payload(
                capability, expected_build_sha=expected_build_sha,
                root=ROOT, source_path=SOURCE,
            )
            identities.append({**proof, "model_instance_id": capability.get("model_instance_id")})
        # An application release may change only if both the collector contract
        # and the source strategy remain independently content-compatible.
        observed = dict(evidence["identity"])
        for identity in (*identities, observed):
            identity.pop("compatible_app_build_sha", None)
            identity.pop("strategy_compatibility_status", None)
        if identities[0] != identities[1] or observed != identities[0]:
            raise ValueError("capture model/source identity differs")
        from tools.sync_bigqmt_reference import TARGET_COLUMNS
        frames = payload["frames"]
        if set(frames) != set(TARGET_COLUMNS):
            raise ValueError("capture table inventory differs")
        for table, columns in TARGET_COLUMNS.items():
            frame = frames[table]
            if (frame["columns"] != columns or not isinstance(frame["rows"], list)
                    or any(not isinstance(row, dict) or set(row) != set(columns)
                           for row in frame["rows"])):
                raise ValueError("capture frame contract differs")
            for row in frame["rows"]:
                observed_at = datetime.fromisoformat(str(row["etl_sync_at"]))
                if (observed_at.tzinfo is not None
                        or not started.replace(microsecond=0) <= observed_at <= captured):
                    raise ValueError("capture frame observation time differs")
        return payload
    except (KeyError, TypeError, ValueError, RuntimeError) as exc:
        raise MembershipCaptureInvalid(f"DATA_BLOCKED: invalid membership capture: {exc}") from exc


def capture_frames(payload: dict[str, Any]) -> dict[str, pd.DataFrame]:
    frames = {}
    for table, content in payload["frames"].items():
        frame = pd.DataFrame(content["rows"], columns=content["columns"])
        if "etl_sync_at" in frame:
            frame["etl_sync_at"] = pd.to_datetime(frame["etl_sync_at"], errors="raise")
        frames[table] = frame
    return frames


class MembershipCaptureStore:
    def __init__(self, root: Path | None = None):
        self.root = root or capture_root()
        _ordinary(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

    def path(self, target: date) -> Path:
        return self.root / f"capture-{target.isoformat()}.json.gz"

    @contextmanager
    def locked(self):
        path = self.root / ".capture.lock"
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
            finally:
                stream.seek(0)
                if os.name == "nt":
                    msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(stream.fileno(), fcntl.LOCK_UN)

    def pending_dates(self) -> list[date]:
        dates = []
        for path in sorted(self.root.glob("capture-*.json.gz")):
            _ordinary(path)
            try:
                dates.append(date.fromisoformat(path.name[8:-8]))
            except ValueError as exc:
                raise MembershipCaptureInvalid("membership capture filename is invalid") from exc
        return dates

    def load(self, target: date, *, expected_build_sha: str) -> dict[str, Any] | None:
        path = self.path(target)
        _ordinary(path)
        if not path.exists():
            return None
        try:
            with gzip.open(path, "rb") as stream:
                raw = stream.read(MAX_RAW_BYTES + 1)
            if len(raw) > MAX_RAW_BYTES:
                raise ValueError("capture exceeds bounded size")
            payload = json.loads(raw)
            if payload.get("snapshot_date") != target.isoformat():
                raise ValueError("capture filename/date differs")
            return validate_capture(payload, expected_build_sha=expected_build_sha)
        except (OSError, EOFError, ValueError, TypeError) as exc:
            raise MembershipCaptureInvalid(f"DATA_BLOCKED: membership capture cannot be read: {path.name}") from exc

    def _write(self, path: Path, payload: dict[str, Any]) -> None:
        _ordinary(path)
        raw = _canonical(payload)
        if len(raw) > MAX_RAW_BYTES:
            raise MembershipCaptureInvalid("membership capture exceeds bounded size")
        encoded = gzip.compress(raw, mtime=0)
        used = sum(item.stat().st_size for item in self.root.glob("capture-*.json.gz"))
        if path.name.startswith("capture-") and used + len(encoded) > MAX_PENDING_BYTES:
            raise MembershipCaptureInvalid("pending membership capture disk budget is exhausted")
        fd, temporary = tempfile.mkstemp(prefix=".writing-", dir=self.root)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            _replace_with_retry(Path(temporary), path)
        finally:
            Path(temporary).unlink(missing_ok=True)

    def save(self, *, target: date, frames: dict[str, pd.DataFrame], counts: dict[str, Any],
             evidence: dict[str, Any], expected_build_sha: str) -> dict[str, Any]:
        payload = {
            "schema": SCHEMA, "snapshot_date": target.isoformat(),
            "collector_contract_sha256": COLLECTOR_CONTRACT_HASH,
            "evidence": evidence, "counts": counts,
            "frames": {table: {"columns": list(frame.columns),
                                "rows": frame.to_dict("records")}
                       for table, frame in frames.items()},
        }
        # Normalize timestamps into the exact representation read on replay.
        payload = json.loads(_canonical(payload))
        payload["sha256"] = _digest(payload)
        validate_capture(payload, expected_build_sha=expected_build_sha)
        existing = self.load(target, expected_build_sha=expected_build_sha)
        if existing is not None:
            if existing["sha256"] != payload["sha256"]:
                raise MembershipCaptureInvalid("membership capture is immutable")
            return existing
        self._write(self.path(target), payload)
        return self.load(target, expected_build_sha=expected_build_sha)

    def complete(self, capture: dict[str, Any], receipt: dict[str, Any]) -> None:
        target = date.fromisoformat(capture["snapshot_date"])
        from integrations.bigqmt.membership_snapshot import (
            _canonical_hash, _concept_snapshot_frame, _industry_snapshot_frame,
        )
        frames = capture_frames(capture)
        captured_at = datetime.fromisoformat(capture["evidence"]["captured_at"]).replace(microsecond=0)
        arguments = {"snapshot_date": target, "captured_at": captured_at,
                     "source": "gj_big_qmt_inner", "quality_status": "QMT_VALIDATED"}
        concept = _concept_snapshot_frame(frames, **arguments)
        industry = _industry_snapshot_frame(frames, **arguments)
        hashes = {}
        for kind, frame, columns in (
            ("concept", concept, ("concept_code", "concept_name", "stock_code", "short_name")),
            ("industry", industry, ("industry_code", "industry_name", "industry_type", "stock_code", "short_name")),
        ):
            hashes[kind] = _canonical_hash([
                tuple(str(row[column]) for column in columns) for _, row in frame.iterrows()
            ])
        proof = receipt.get("proof") or {}
        try:
            published_capture = datetime.fromisoformat(proof["captured_at"])
        except (KeyError, TypeError, ValueError) as exc:
            raise MembershipCaptureInvalid("publication capture time is unavailable") from exc
        if (receipt.get("status") != "PASS" or receipt.get("snapshot_date") != target.isoformat()
                or proof.get("source") != "gj_big_qmt_inner"
                or proof.get("quality_status") != "QMT_VALIDATED"
                or published_capture.date() != target or published_capture > captured_at
                or proof.get("concept_hash") != hashes["concept"]
                or proof.get("industry_hash") != hashes["industry"]
                or proof.get("concept_relation_count") != len(concept)
                or proof.get("industry_relation_count") != len(industry)):
            raise MembershipCaptureInvalid("capture cleanup requires exact database publication proof")
        completion = {
            "schema": SCHEMA, "capture_sha256": capture["sha256"], "receipt": receipt,
            "collector_contract_sha256": capture["collector_contract_sha256"],
            "evidence": capture["evidence"],
        }
        completion["sha256"] = _digest(completion)
        self._write(self.root / f"published-{target.isoformat()}.json.gz", completion)
        self.path(target).unlink()
