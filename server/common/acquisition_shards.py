"""Durable verified source shards, independent of scheduler attempt lifetimes."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import tempfile
from datetime import date, datetime


def _json_value(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"unsupported shard value: {type(value).__name__}")


def _bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False,
                      default=_json_value).encode("utf-8")


class AcquisitionShards:
    def __init__(self, dataset: str, scope: dict, *, root: Path | None = None):
        if root is None:
            base = os.environ.get("PROBIGA_JOB_LOG_ROOT")
            if not base:
                base = (str(Path(os.environ.get("ProgramData", "C:/ProgramData")) /
                            "ProBigA" / "scheduler") if os.name == "nt"
                        else "/var/lib/probiga/jobs")
            root = Path(base) / "acquisition-shards"
        self.scope = json.loads(_bytes({"dataset": dataset, "identity": scope}))
        self.root = Path(root) / hashlib.sha256(_bytes(self.scope)).hexdigest()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key):
        return self.root / (hashlib.sha256(_bytes(key)).hexdigest() + ".json")

    def load(self, key):
        path = self._path(key)
        try:
            record = json.loads(path.read_bytes())
            digest = record.pop("sha256")
            if (record["scope"] != self.scope or record["key"] != json.loads(_bytes(key))
                    or hashlib.sha256(_bytes(record)).hexdigest() != digest):
                return None
            return record["payload"]
        except (FileNotFoundError, ValueError, KeyError, TypeError):
            return None

    def save(self, key, payload):
        record = {"scope": self.scope, "key": key, "payload": payload}
        record["sha256"] = hashlib.sha256(_bytes(record)).hexdigest()
        fd, name = tempfile.mkstemp(prefix=".writing-", dir=self.root)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(_bytes(record))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, self._path(key))
        finally:
            if os.path.exists(name):
                os.unlink(name)
