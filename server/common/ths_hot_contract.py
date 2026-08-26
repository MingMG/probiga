"""Exact current-snapshot contract for the two Tonghuashun hot-data feeds."""
from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from server.common.authoritative_market_clock import (
    authoritative_closed_trade_date,
)


THS_HOT_RESULT_SCHEMA = "probiga.ths-hot-result.v1"
THS_HOT_PROVIDER = "adata.ths.current_snapshot"
THS_HOT_SOURCE_CAPABILITY = "CURRENT_SNAPSHOT_ONLY"
THS_HOT_RANK_TASK_TYPE = "hot_rank_ths"
THS_HOT_CONCEPT_TASK_TYPE = "hot_concept"
THS_HOT_RANK_DATASET = "hot_rank_100"
THS_HOT_CONCEPT_DATASET = "hot_concept_20_by_plate_type"
THS_HOT_RANK_MIN_ROWS = 50
THS_HOT_RANK_MAX_ROWS = 100
THS_HOT_CONCEPT_MIN_ROWS_PER_TYPE = 10
THS_HOT_CONCEPT_MAX_ROWS_PER_TYPE = 20
THS_HOT_CONCEPT_PLATE_TYPES = (1, 2)
THS_HOT_READY_TIMES = {
    THS_HOT_CONCEPT_TASK_TYPE: time(17, 10),
    THS_HOT_RANK_TASK_TYPE: time(17, 12),
}
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_HEX64 = re.compile(r"[0-9a-f]{64}\Z")
_A_SHARE_CODE = re.compile(r"(?:0|3|4|6|8|9)[0-9]{5}\Z")


class ThsHotDataBlocked(RuntimeError):
    """The current-only response cannot yet be assigned to a proven date."""


def canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def with_receipt_id(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result.pop("receipt_id", None)
    result["receipt_id"] = canonical_hash(result)
    return result


def receipt_id_valid(payload: Mapping[str, Any]) -> bool:
    supplied = str(payload.get("receipt_id") or "").lower()
    unsigned = dict(payload)
    unsigned.pop("receipt_id", None)
    return _HEX64.fullmatch(supplied) is not None and supplied == canonical_hash(
        unsigned
    )


def shanghai_now(value: datetime | None = None) -> datetime:
    current = value or datetime.now(_SHANGHAI)
    if current.tzinfo is not None:
        current = current.astimezone(_SHANGHAI).replace(tzinfo=None)
    return current.replace(microsecond=0)


def require_capture_window(
    engine,
    *,
    task_type: str,
    requested_date: str,
    now: datetime | None = None,
) -> datetime:
    """Require today's exchange session and the task's post-close ready time."""

    current = shanghai_now(now)
    if task_type not in THS_HOT_READY_TIMES:
        raise ValueError("unknown THS hot task type")
    if requested_date != current.date().isoformat():
        raise ThsHotDataBlocked(
            "CURRENT_ONLY_HISTORICAL_LABEL_PROHIBITED"
        )
    ready_time = THS_HOT_READY_TIMES[task_type]
    closed_date = authoritative_closed_trade_date(
        engine,
        now=current,
        close_ready_time=ready_time,
    )
    if closed_date != requested_date:
        reason = (
            "CURRENT_SESSION_NOT_CLOSED"
            if current.time() < ready_time
            else "REQUEST_DATE_NOT_OPEN_SESSION"
        )
        raise ThsHotDataBlocked(reason)
    return current


def _number(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not number.is_finite():
        return None
    normalized = format(number.normalize(), "f")
    if normalized in {"-0", "-0.0"}:
        return "0"
    return normalized


def _date_text(value: Any) -> str:
    return str(value or "")[:10]


def _timestamp_text(value: Any) -> str:
    if value is None:
        return ""
    raw = (
        value.isoformat(sep=" ", timespec="seconds")
        if hasattr(value, "isoformat")
        else str(value)
    )
    return raw.replace("T", " ").split(".", 1)[0]


def canonical_rank_provider_rows(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    canonical = [
        {
            "rank": int(row.get("rank") or 0),
            "stock_code": str(row.get("stock_code") or "").strip().zfill(6),
            "short_name": str(row.get("short_name") or "").strip(),
            "change_pct": _number(row.get("change_pct")),
            "hot_value": _number(row.get("hot_value")),
            "pop_tag": str(row.get("pop_tag") or "").strip(),
            "concept_tag": str(row.get("concept_tag") or "").strip(),
        }
        for row in rows
    ]
    return sorted(canonical, key=lambda row: (row["rank"], row["stock_code"]))


def canonical_rank_persisted_rows(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    provider = canonical_rank_provider_rows(rows)
    by_identity = {
        (int(item["rank"]), str(item["stock_code"])): item for item in provider
    }
    result = []
    for source in rows:
        identity = (
            int(source.get("rank") or 0),
            str(source.get("stock_code") or "").strip().zfill(6),
        )
        result.append({
            "snapshot_date": _date_text(source.get("snapshot_date")),
            **by_identity[identity],
        })
    return sorted(
        result,
        key=lambda row: (row["snapshot_date"], row["rank"], row["stock_code"]),
    )


def validate_rank_inventory(
    rows: Sequence[Mapping[str, Any]],
    *,
    target_date: str | None = None,
    minimum: int = THS_HOT_RANK_MIN_ROWS,
) -> dict[str, Any]:
    provider = canonical_rank_provider_rows(rows)
    row_count = len(provider)
    if row_count < max(THS_HOT_RANK_MIN_ROWS, int(minimum)) or (
        row_count > THS_HOT_RANK_MAX_ROWS
    ):
        raise RuntimeError(
            "THS hot rank inventory size differs: "
            f"rows={row_count} expected={max(THS_HOT_RANK_MIN_ROWS, int(minimum))}..{THS_HOT_RANK_MAX_ROWS}"
        )
    codes = [row["stock_code"] for row in provider]
    ranks = [int(row["rank"]) for row in provider]
    if (
        len(codes) != len(set(codes))
        or any(_A_SHARE_CODE.fullmatch(code) is None for code in codes)
        or any(not row["short_name"] for row in provider)
        or sorted(ranks) != list(range(1, row_count + 1))
        or len(ranks) != len(set(ranks))
    ):
        raise RuntimeError("THS hot rank code/rank inventory differs")
    persisted = None
    if target_date is not None:
        persisted = canonical_rank_persisted_rows(rows)
        dates = {row["snapshot_date"] for row in persisted}
        if dates != {target_date}:
            raise RuntimeError(
                f"THS hot rank data date differs: observed={sorted(dates)}"
            )
    return {
        "row_count": row_count,
        "provider_payload_sha256": canonical_hash(provider),
        "persisted_row_sha256": (
            canonical_hash(persisted) if persisted is not None else None
        ),
        "code_set_sha256": canonical_hash(sorted(codes)),
        "rank_set_sha256": canonical_hash(sorted(ranks)),
    }


def canonical_concept_provider_rows(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    canonical = [
        {
            "plate_type": int(row.get("plate_type") or 0),
            "rank": int(row.get("rank") or 0),
            "concept_code": str(row.get("concept_code") or "").strip(),
            "concept_name": str(row.get("concept_name") or "").strip(),
            "change_pct": _number(row.get("change_pct")),
            "hot_value": _number(row.get("hot_value")),
            "hot_tag": str(row.get("hot_tag") or "").strip(),
        }
        for row in rows
    ]
    return sorted(
        canonical,
        key=lambda row: (row["plate_type"], row["rank"], row["concept_code"]),
    )


def canonical_concept_persisted_rows(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    provider = canonical_concept_provider_rows(rows)
    by_identity = {
        (
            int(item["plate_type"]),
            int(item["rank"]),
            str(item["concept_code"]),
        ): item
        for item in provider
    }
    result = []
    for source in rows:
        identity = (
            int(source.get("plate_type") or 0),
            int(source.get("rank") or 0),
            str(source.get("concept_code") or "").strip(),
        )
        result.append({
            "snapshot_date": _date_text(source.get("snapshot_date")),
            **by_identity[identity],
        })
    return sorted(
        result,
        key=lambda row: (
            row["snapshot_date"],
            row["plate_type"],
            row["rank"],
            row["concept_code"],
        ),
    )


def validate_concept_inventory(
    rows: Sequence[Mapping[str, Any]],
    *,
    target_date: str | None = None,
    minimum_per_type: int = THS_HOT_CONCEPT_MIN_ROWS_PER_TYPE,
) -> dict[str, Any]:
    provider = canonical_concept_provider_rows(rows)
    counts: dict[str, int] = {}
    identities: list[tuple[int, str]] = []
    for plate_type in THS_HOT_CONCEPT_PLATE_TYPES:
        selected = [row for row in provider if row["plate_type"] == plate_type]
        count = len(selected)
        minimum = max(THS_HOT_CONCEPT_MIN_ROWS_PER_TYPE, int(minimum_per_type))
        if count < minimum or count > THS_HOT_CONCEPT_MAX_ROWS_PER_TYPE:
            raise RuntimeError(
                "THS hot concept plate inventory size differs: "
                f"plate_type={plate_type} rows={count} "
                f"expected={minimum}..{THS_HOT_CONCEPT_MAX_ROWS_PER_TYPE}"
            )
        ranks = [int(row["rank"]) for row in selected]
        codes = [str(row["concept_code"]) for row in selected]
        if (
            sorted(ranks) != list(range(1, count + 1))
            or len(ranks) != len(set(ranks))
            or len(codes) != len(set(codes))
            or any(not code for code in codes)
            or any(not row["concept_name"] for row in selected)
        ):
            raise RuntimeError(
                f"THS hot concept plate_type={plate_type} identity differs"
            )
        counts[str(plate_type)] = count
        identities.extend((plate_type, code) for code in codes)
    unexpected = sorted(
        {int(row["plate_type"]) for row in provider}
        - set(THS_HOT_CONCEPT_PLATE_TYPES)
    )
    if unexpected or len(identities) != len(set(identities)):
        raise RuntimeError(
            f"THS hot concept unexpected/duplicate identities: {unexpected}"
        )
    persisted = None
    if target_date is not None:
        persisted = canonical_concept_persisted_rows(rows)
        dates = {row["snapshot_date"] for row in persisted}
        if dates != {target_date}:
            raise RuntimeError(
                f"THS hot concept data date differs: observed={sorted(dates)}"
            )
    return {
        "row_count": len(provider),
        "plate_type_counts": counts,
        "provider_payload_sha256": canonical_hash(provider),
        "persisted_row_sha256": (
            canonical_hash(persisted) if persisted is not None else None
        ),
        "identity_set_sha256": canonical_hash(sorted(identities)),
    }


def batch_timestamp(rows: Sequence[Mapping[str, Any]]) -> str:
    values = {_timestamp_text(row.get("etl_sync_at")) for row in rows}
    if len(values) != 1 or not next(iter(values), ""):
        raise RuntimeError("THS hot persisted rows span multiple publish batches")
    return next(iter(values))


def _task_dataset(task_type: str) -> str:
    if task_type == THS_HOT_RANK_TASK_TYPE:
        return THS_HOT_RANK_DATASET
    if task_type == THS_HOT_CONCEPT_TASK_TYPE:
        return THS_HOT_CONCEPT_DATASET
    raise ValueError("unknown THS hot task type")


def build_pass_receipt(
    *,
    task_type: str,
    requested_date: str,
    started_at: datetime,
    captured_at: datetime,
    published_at: datetime,
    batch_at: str,
    inventory: Mapping[str, Any],
) -> dict[str, Any]:
    core = {
        "schema": THS_HOT_RESULT_SCHEMA,
        "status": "PASS",
        "task_type": task_type,
        "dataset": _task_dataset(task_type),
        "provider": THS_HOT_PROVIDER,
        "source_capability": THS_HOT_SOURCE_CAPABILITY,
        "requested_date": requested_date,
        "data_date": requested_date,
        "started_at": shanghai_now(started_at).isoformat(sep=" "),
        "captured_at": shanghai_now(captured_at).isoformat(sep=" "),
        "published_at": shanghai_now(published_at).isoformat(sep=" "),
        "batch_at": batch_at,
        **dict(inventory),
    }
    core["batch_id"] = canonical_hash({
        "task_type": task_type,
        "requested_date": requested_date,
        "started_at": core["started_at"],
        "batch_at": batch_at,
        "provider_payload_sha256": core.get("provider_payload_sha256"),
    })
    return with_receipt_id(core)


def build_blocked_receipt(
    *,
    task_type: str,
    requested_date: str,
    started_at: datetime,
    reason: str,
) -> dict[str, Any]:
    return with_receipt_id({
        "schema": THS_HOT_RESULT_SCHEMA,
        "status": "DATA_BLOCKED",
        "task_type": task_type,
        "dataset": _task_dataset(task_type),
        "provider": THS_HOT_PROVIDER,
        "source_capability": THS_HOT_SOURCE_CAPABILITY,
        "requested_date": requested_date,
        "started_at": shanghai_now(started_at).isoformat(sep=" "),
        "reason": str(reason or "CURRENT_SNAPSHOT_UNAVAILABLE"),
        "row_count": 0,
    })


def basic_receipt_disposition(
    payload: Mapping[str, Any],
    *,
    task_type: str,
    return_code: int,
) -> str:
    if (
        payload.get("schema") != THS_HOT_RESULT_SCHEMA
        or payload.get("task_type") != task_type
        or payload.get("dataset") != _task_dataset(task_type)
        or payload.get("provider") != THS_HOT_PROVIDER
        or payload.get("source_capability") != THS_HOT_SOURCE_CAPABILITY
        or not receipt_id_valid(payload)
    ):
        return "failed"
    status = str(payload.get("status") or "")
    if status == "DATA_BLOCKED":
        return "blocked" if int(return_code) == 2 else "failed"
    return "success" if status == "PASS" and int(return_code) == 0 else "failed"


__all__ = [
    "THS_HOT_CONCEPT_DATASET",
    "THS_HOT_CONCEPT_MAX_ROWS_PER_TYPE",
    "THS_HOT_CONCEPT_MIN_ROWS_PER_TYPE",
    "THS_HOT_CONCEPT_PLATE_TYPES",
    "THS_HOT_CONCEPT_TASK_TYPE",
    "THS_HOT_PROVIDER",
    "THS_HOT_RANK_DATASET",
    "THS_HOT_RANK_MAX_ROWS",
    "THS_HOT_RANK_MIN_ROWS",
    "THS_HOT_RANK_TASK_TYPE",
    "THS_HOT_READY_TIMES",
    "THS_HOT_RESULT_SCHEMA",
    "THS_HOT_SOURCE_CAPABILITY",
    "ThsHotDataBlocked",
    "basic_receipt_disposition",
    "batch_timestamp",
    "build_blocked_receipt",
    "build_pass_receipt",
    "canonical_concept_persisted_rows",
    "canonical_concept_provider_rows",
    "canonical_hash",
    "canonical_rank_persisted_rows",
    "canonical_rank_provider_rows",
    "receipt_id_valid",
    "require_capture_window",
    "shanghai_now",
    "validate_concept_inventory",
    "validate_rank_inventory",
    "with_receipt_id",
]
