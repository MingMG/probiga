from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import tempfile
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Engine

from integrations.qmt.diagnostics import PROVIDER_ID
from integrations.qmt.pending_write import RAW_MANIFEST_UPSERT, enqueue_pending_write


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW_ROOT = ROOT / "data" / "qmt_raw"
SAFE_COMPONENT = re.compile(r"[^0-9A-Za-z_.-]+")
CHINA_STANDARD_TIME = timezone(timedelta(hours=8), name="Asia/Shanghai")


@dataclass(frozen=True)
class RawArchiveResult:
    manifest_key: str
    batch_id: str
    file_path: str
    payload_hash: str
    row_count: int
    symbol_count: int
    manifest_persisted: bool = True
    pending_write_path: str | None = None


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    item = getattr(value, "item", None)
    if callable(item):
        return item()
    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        return tolist()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )


def _safe_component(value: str, fallback: str) -> str:
    cleaned = SAFE_COMPONENT.sub("_", str(value).strip()).strip("._")
    return cleaned[:96] or fallback


def _infer_counts(payload: Any) -> tuple[int, int]:
    rows: list[Any]
    if isinstance(payload, Mapping) and isinstance(payload.get("rows"), list):
        rows = payload["rows"]
    elif isinstance(payload, list):
        rows = payload
    else:
        rows = []
    symbols: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        for key in ("stock_code", "qmt_code", "symbol", "code", "index_code"):
            value = row.get(key)
            if value:
                symbols.add(str(value))
                break
    return len(rows), len(symbols)


def _absolute_raw_root(raw_root: str | Path | None) -> Path:
    configured = raw_root or os.environ.get("GJ_QMT_RAW_ROOT") or DEFAULT_RAW_ROOT
    return Path(configured).expanduser().resolve()


def _upsert_manifest(engine: Engine, params: Mapping[str, Any]) -> None:
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


def archive_payload(
    engine: Engine,
    *,
    dataset: str,
    api_name: str,
    params: Mapping[str, Any] | None,
    payload: Any,
    period: str = "",
    batch_id: str | None = None,
    provenance: Mapping[str, Any] | None = None,
    min_source_time: datetime | None = None,
    max_source_time: datetime | None = None,
    raw_root: str | Path | None = None,
) -> RawArchiveResult:
    """Atomically archive one real QMT response, then register its immutable manifest."""
    captured_at = datetime.now(CHINA_STANDARD_TIME)
    resolved_batch_id = batch_id or uuid.uuid4().hex
    safe_dataset = _safe_component(dataset, "unknown_dataset")
    safe_api = _safe_component(api_name, "unknown_api")
    safe_period = _safe_component(period, "none")
    normalized_params = dict(params or {})
    normalized_provenance = dict(provenance or {})
    payload_json = _canonical_json(payload)
    payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    params_hash = hashlib.sha256(_canonical_json(normalized_params).encode("utf-8")).hexdigest()[:16]
    manifest_key = f"{resolved_batch_id}:{safe_dataset}:{safe_api}:{safe_period}:{params_hash}"
    row_count, symbol_count = _infer_counts(payload)

    envelope = {
        "provider": PROVIDER_ID,
        "batch_id": resolved_batch_id,
        "dataset": dataset,
        "api_name": api_name,
        "period": period,
        "params": normalized_params,
        "captured_at": captured_at.isoformat(),
        "provenance": normalized_provenance,
        "payload_hash": payload_hash,
        "payload": payload,
    }
    root = _absolute_raw_root(raw_root)
    folder = root / safe_dataset / captured_at.strftime("%Y") / captured_at.strftime("%m") / captured_at.strftime("%d")
    folder.mkdir(parents=True, exist_ok=True)
    file_name = f"{captured_at.strftime('%H%M%S_%f')}_{safe_api}_{safe_period}_{params_hash}.json.gz"
    target = folder / file_name

    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(prefix=".qmt_", suffix=".tmp", dir=folder, delete=False) as temp:
            temp_path = Path(temp.name)
        with gzip.open(temp_path, "wt", encoding="utf-8", newline="\n") as stream:
            json.dump(envelope, stream, ensure_ascii=False, sort_keys=True, default=_json_default)
        os.replace(temp_path, target)
        temp_path = None

    except Exception:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
        if target.exists():
            target.unlink(missing_ok=True)
        raise

    manifest_params = {
        "manifest_key": manifest_key,
        "batch_id": resolved_batch_id,
        "provider": PROVIDER_ID,
        "dataset": dataset,
        "api_name": api_name,
        "period": period,
        "file_path": str(target),
        "symbol_count": symbol_count,
        "row_count": row_count,
        "min_source_time": min_source_time,
        "max_source_time": max_source_time,
        "payload_hash": payload_hash,
    }
    manifest_persisted = True
    pending_write_path: str | None = None
    try:
        _upsert_manifest(engine, manifest_params)
    except Exception as exc:
        pending = enqueue_pending_write(
            operation=RAW_MANIFEST_UPSERT,
            payload=manifest_params,
            error_message=str(exc),
        )
        manifest_persisted = False
        pending_write_path = pending.queue_path

    return RawArchiveResult(
        manifest_key=manifest_key,
        batch_id=resolved_batch_id,
        file_path=str(target),
        payload_hash=payload_hash,
        row_count=row_count,
        symbol_count=symbol_count,
        manifest_persisted=manifest_persisted,
        pending_write_path=pending_write_path,
    )


def result_dict(result: RawArchiveResult) -> dict[str, Any]:
    return asdict(result)
