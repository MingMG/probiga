from __future__ import annotations

import gzip
import hashlib
import json
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Engine

from integrations.qmt.diagnostics import PROVIDER_ID


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PENDING_ROOT = ROOT / "data" / "qmt_pending_writes"
CHINA_STANDARD_TIME = timezone(timedelta(hours=8), name="Asia/Shanghai")
RAW_MANIFEST_UPSERT = "qmt_raw_manifest_upsert"


@dataclass(frozen=True)
class PendingWriteResult:
    operation: str
    queue_path: str
    payload_hash: str


@dataclass(frozen=True)
class PendingReplayResult:
    attempted: int
    succeeded: int
    failed: int
    remaining: int
    errors: list[dict[str, str]]


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )


def _absolute_pending_root(pending_root: str | Path | None = None) -> Path:
    configured = pending_root or os.environ.get("GJ_QMT_PENDING_WRITE_ROOT") or DEFAULT_PENDING_ROOT
    return Path(configured).expanduser().resolve()


def enqueue_pending_write(
    *,
    operation: str,
    payload: Mapping[str, Any],
    error_message: str,
    pending_root: str | Path | None = None,
) -> PendingWriteResult:
    """Persist one database write operation to local disk for later replay."""
    created_at = datetime.now(CHINA_STANDARD_TIME)
    normalized_payload = dict(payload)
    payload_hash = hashlib.sha256(_canonical_json(normalized_payload).encode("utf-8")).hexdigest()
    envelope = {
        "schema_version": 1,
        "provider": PROVIDER_ID,
        "operation": operation,
        "created_at": created_at.isoformat(),
        "payload_hash": payload_hash,
        "last_error": error_message[:4000],
        "payload": normalized_payload,
    }
    root = _absolute_pending_root(pending_root)
    folder = root / operation / created_at.strftime("%Y") / created_at.strftime("%m") / created_at.strftime("%d")
    folder.mkdir(parents=True, exist_ok=True)
    file_name = f"{created_at.strftime('%H%M%S_%f')}_{payload_hash[:16]}.json.gz"
    target = folder / file_name
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(prefix=".qmt_pending_", suffix=".tmp", dir=folder, delete=False) as temp:
            temp_path = Path(temp.name)
        with gzip.open(temp_path, "wt", encoding="utf-8", newline="\n") as stream:
            json.dump(envelope, stream, ensure_ascii=False, sort_keys=True, default=_json_default)
        os.replace(temp_path, target)
        temp_path = None
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)

    return PendingWriteResult(operation=operation, queue_path=str(target), payload_hash=payload_hash)


def iter_pending_write_paths(
    *,
    operation: str | None = None,
    pending_root: str | Path | None = None,
) -> Iterable[Path]:
    root = _absolute_pending_root(pending_root)
    if not root.exists():
        return []
    pattern = f"{operation or '*'}/**/*.json.gz"
    return sorted(path for path in root.glob(pattern) if path.is_file())


def _load_pending_write(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        return json.load(stream)


def upsert_raw_manifest(engine: Engine, params: Mapping[str, Any]) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO qmt_raw_manifest (
                    manifest_key, batch_id, provider, dataset, api_name, period,
                    file_path, symbol_count, row_count, min_source_time,
                    max_source_time, payload_hash, status, error_message
                ) VALUES (
                    :manifest_key, :batch_id, :provider, :dataset, :api_name, :period,
                    :file_path, :symbol_count, :row_count, :min_source_time,
                    :max_source_time, :payload_hash, 'SUCCESS', NULL
                )
                ON DUPLICATE KEY UPDATE
                    file_path = VALUES(file_path),
                    symbol_count = VALUES(symbol_count),
                    row_count = VALUES(row_count),
                    min_source_time = VALUES(min_source_time),
                    max_source_time = VALUES(max_source_time),
                    payload_hash = VALUES(payload_hash),
                    status = 'SUCCESS',
                    error_message = NULL
                """
            ),
            dict(params),
        )


def replay_pending_write(engine: Engine, path: str | Path) -> None:
    pending_path = Path(path)
    envelope = _load_pending_write(pending_path)
    operation = envelope.get("operation")
    if operation != RAW_MANIFEST_UPSERT:
        raise ValueError(f"Unsupported pending write operation: {operation!r}")
    upsert_raw_manifest(engine, envelope["payload"])
    pending_path.unlink()


def replay_pending_writes(
    engine: Engine,
    *,
    pending_root: str | Path | None = None,
    limit: int | None = None,
) -> PendingReplayResult:
    paths = list(iter_pending_write_paths(pending_root=pending_root))
    if limit is not None:
        paths = paths[: max(limit, 0)]

    attempted = 0
    succeeded = 0
    errors: list[dict[str, str]] = []
    for path in paths:
        attempted += 1
        try:
            replay_pending_write(engine, path)
            succeeded += 1
        except Exception as exc:
            errors.append({"path": str(path), "error": str(exc)})

    remaining = len(list(iter_pending_write_paths(pending_root=pending_root)))
    return PendingReplayResult(
        attempted=attempted,
        succeeded=succeeded,
        failed=len(errors),
        remaining=remaining,
        errors=errors,
    )


def result_dict(result: PendingReplayResult | PendingWriteResult) -> dict[str, Any]:
    return asdict(result)
