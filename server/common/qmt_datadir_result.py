"""Machine result contract for canonical QMT local-cache publications."""
from __future__ import annotations

import datetime as dt
import json
import os
import re
from contextlib import nullcontext
from decimal import Decimal
from typing import Any, Mapping, Sequence

from sqlalchemy import text

from server.common.qmt_attestation_contract import canonical_digest


RESULT_SCHEMA = "probiga.qmt-datadir-import-result.v1"
PROVIDER = "qmt_datadir"
SHA40 = re.compile(r"^[0-9a-f]{40}$")
SHA64 = re.compile(r"^[0-9a-f]{64}$")

DATASET_TASK_TYPES = {
    "stock_daily": "qmt_stock_daily_canonical",
    "stock_minute": "qmt_stock_minute_canonical",
    "index_daily": "qmt_index_kline",
    "index_minute": "qmt_index_minute",
}
TASK_TYPE_DATASETS = {
    task_type: dataset for dataset, task_type in DATASET_TASK_TYPES.items()
}
DATASET_TABLES = {
    "stock_daily": "sm_stock_kline",
    "stock_minute": "sm_stock_minute",
    "index_daily": "sm_index_kline",
    "index_minute": "sm_index_minute",
}
DATASET_CODE_COLUMNS = {
    "stock_daily": "stock_code",
    "stock_minute": "stock_code",
    "index_daily": "index_code",
    "index_minute": "index_code",
}
RECEIPT_TABLE = "qmt_datadir_import_partition"


def _normalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value.quantize(Decimal("0.000001")), "f")
    if isinstance(value, dt.datetime):
        return value.replace(microsecond=0).isoformat(sep=" ")
    if isinstance(value, dt.date):
        return value.isoformat()
    return value


def canonical_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {str(key): _normalize(value) for key, value in sorted(row.items())}
        for row in rows
    ]


def row_hash(rows: Sequence[Mapping[str, Any]]) -> str:
    return canonical_digest(canonical_rows(rows))


def _receipt_id(payload: Mapping[str, Any]) -> str:
    unsigned = dict(payload)
    unsigned.pop("receipt_id", None)
    return canonical_digest(unsigned)


def build_result(
    *,
    dataset: str,
    sessions: Sequence[str],
    partitions: Sequence[Mapping[str, Any]],
    build_sha: str = "",
) -> dict[str, Any]:
    task_type = DATASET_TASK_TYPES[dataset]
    normalized_sessions = sorted(str(session) for session in sessions)
    ordered = sorted(
        (dict(partition) for partition in partitions),
        key=lambda partition: str(partition["trade_date"]),
    )
    complete = bool(ordered) and len(ordered) == len(normalized_sessions)
    complete = complete and all(
        partition.get("status") == "COMPLETE"
        and int(partition.get("row_count") or 0) > 0
        and int(partition.get("code_count") or 0) > 0
        and int(partition.get("readback_rows") or 0)
        == int(partition.get("row_count") or 0)
        and int(partition.get("readback_codes") or 0)
        == int(partition.get("code_count") or 0)
        for partition in ordered
    )
    unsigned = {
        "schema": RESULT_SCHEMA,
        "status": "PASS" if complete else "FAILED",
        "dataset": dataset,
        "task_type": task_type,
        "provider": PROVIDER,
        "build_sha": str(build_sha or "").strip().lower(),
        "target_trade_date": (
            normalized_sessions[0] if len(normalized_sessions) == 1 else ""
        ),
        "sessions": normalized_sessions,
        "partitions": ordered,
        "execution": {
            "source_kind": "local_qmt_datadir",
            "network_accessed": False,
        },
    }
    return {**unsigned, "receipt_id": _receipt_id(unsigned)}


def build_unavailable_result(
    *,
    dataset: str,
    sessions: Sequence[str],
    reason: str,
    build_sha: str = "",
) -> dict[str, Any]:
    normalized_sessions = sorted(str(session) for session in sessions)
    unsigned = {
        "schema": RESULT_SCHEMA,
        "status": "BLOCKED",
        "dataset": dataset,
        "task_type": DATASET_TASK_TYPES[dataset],
        "provider": PROVIDER,
        "build_sha": str(build_sha or "").strip().lower(),
        "target_trade_date": (
            normalized_sessions[0] if len(normalized_sessions) == 1 else ""
        ),
        "sessions": normalized_sessions,
        "partitions": [],
        "execution": {
            "source_kind": "local_qmt_datadir",
            "network_accessed": False,
        },
        "reason_code": str(reason or "qmt_datadir_unavailable")[:200],
        "retryable": True,
    }
    return {**unsigned, "receipt_id": _receipt_id(unsigned)}


def load_receipt(
    engine: Any,
    *,
    dataset: str,
    trade_date: str,
) -> dict[str, Any] | None:
    connection_scope = (
        engine.connect() if callable(getattr(engine, "connect", None))
        else nullcontext(engine)
    )
    with connection_scope as connection:
        row = connection.execute(
            text(
                f"SELECT dataset,trade_date,source_root,status,code_count,row_count,"
                f"content_sha256,run_id,started_at,finished_at,detail_json "
                f"FROM `{RECEIPT_TABLE}` "
                "WHERE dataset=:dataset AND trade_date=:trade_date"
            ),
            {"dataset": dataset, "trade_date": trade_date},
        ).mappings().first()
    if row is None:
        return None
    result = dict(row)
    raw_detail = result.pop("detail_json", None)
    try:
        detail = json.loads(raw_detail) if raw_detail else {}
    except (TypeError, json.JSONDecodeError):
        detail = {}
    result["detail"] = detail if isinstance(detail, dict) else {}
    return result


def partition_from_receipt(receipt: Mapping[str, Any]) -> dict[str, Any]:
    detail = receipt.get("detail")
    detail = detail if isinstance(detail, Mapping) else {}
    return {
        "trade_date": str(receipt.get("trade_date") or "")[:10],
        "status": str(receipt.get("status") or ""),
        "source_root": str(receipt.get("source_root") or ""),
        "run_id": str(receipt.get("run_id") or ""),
        "row_count": int(receipt.get("row_count") or 0),
        "code_count": int(receipt.get("code_count") or 0),
        "content_sha256": str(receipt.get("content_sha256") or ""),
        "row_hash": str(detail.get("row_hash") or ""),
        "eligible_code_count": int(detail.get("eligible_code_count") or 0),
        "coverage": float(detail.get("coverage") or 0.0),
        "readback_rows": int(detail.get("readback_rows") or 0),
        "readback_codes": int(detail.get("readback_codes") or 0),
        "attestation_run_id": str(detail.get("attestation_run_id") or ""),
    }


def validate_task_result(
    task_type: str,
    payload: Mapping[str, Any],
    *,
    return_code: int,
    expected_build_sha: str,
) -> str:
    if payload.get("schema") != RESULT_SCHEMA:
        return "failed"
    if payload.get("receipt_id") != _receipt_id(payload):
        return "failed"
    if (
        payload.get("status") == "BLOCKED"
        and return_code in {2, 3}
        and payload.get("retryable") is True
    ):
        return "blocked"
    dataset = TASK_TYPE_DATASETS.get(task_type)
    expected_build = str(expected_build_sha or "").strip().lower()
    sessions = payload.get("sessions")
    partitions = payload.get("partitions")
    execution = payload.get("execution")
    if (
        return_code != 0
        or dataset is None
        or payload.get("status") != "PASS"
        or payload.get("dataset") != dataset
        or payload.get("task_type") != task_type
        or payload.get("provider") != PROVIDER
        or SHA40.fullmatch(expected_build) is None
        or expected_build == "0" * 40
        or payload.get("build_sha") != expected_build
        or not isinstance(sessions, list)
        or not sessions
        or sessions != sorted(set(sessions))
        or payload.get("target_trade_date") != (
            sessions[0] if len(sessions) == 1 else ""
        )
        or not isinstance(partitions, list)
        or len(partitions) != len(sessions)
        or [item.get("trade_date") for item in partitions] != sessions
        or not isinstance(execution, Mapping)
        or execution.get("source_kind") != "local_qmt_datadir"
        or execution.get("network_accessed") is not False
    ):
        return "failed"
    for partition in partitions:
        if not isinstance(partition, Mapping):
            return "failed"
        row_count = int(partition.get("row_count") or 0)
        code_count = int(partition.get("code_count") or 0)
        requires_attestation = dataset == "stock_daily"
        if (
            partition.get("status") != "COMPLETE"
            or not str(partition.get("source_root") or "")
            or not str(partition.get("run_id") or "")
            or SHA64.fullmatch(str(partition.get("content_sha256") or ""))
            is None
            or SHA64.fullmatch(str(partition.get("row_hash") or "")) is None
            or row_count <= 0
            or code_count <= 0
            or float(partition.get("coverage") or 0.0) < 0.95
            or int(partition.get("readback_rows") or 0) != row_count
            or int(partition.get("readback_codes") or 0) != code_count
            or (
                requires_attestation
                and not str(partition.get("attestation_run_id") or "")
            )
        ):
            return "failed"
    return "complete"


def persisted_rows(
    engine: Any,
    *,
    dataset: str,
    trade_date: str,
) -> tuple[int, int, list[dict[str, Any]]]:
    table = DATASET_TABLES[dataset]
    code_column = DATASET_CODE_COLUMNS[dataset]
    if dataset == "stock_daily":
        columns = (
            "stock_code,trade_time,trade_date,k_type,adjust_type,open,close,"
            "high,low,volume,amount,`change`,change_pct,pre_close,qmt_code,"
            "data_source,source_time,batch_id,data_version,quality_status,"
            "permission_status"
        )
        where = (
            "trade_date=:trade_date AND k_type=1 AND adjust_type=0 "
            "AND data_source=:source"
        )
    elif dataset == "stock_minute":
        columns = (
            "stock_code,trade_time,trade_date,price,`change`,change_pct,volume,"
            "amount,qmt_code,data_source,source_time,batch_id,data_version,"
            "quality_status,permission_status"
        )
        where = "trade_date=:trade_date AND data_source=:source"
    elif dataset == "index_daily":
        columns = (
            "index_code,trade_time,trade_date,k_type,open,close,high,low,"
            "volume,amount,`change`,change_pct"
        )
        where = "trade_date=:trade_date AND k_type=1"
    else:
        columns = (
            "index_code,trade_time,trade_date,price,avg_price,`change`,"
            "change_pct,volume,amount,snapshot_at"
        )
        where = "trade_date=:trade_date"
    connection_scope = (
        engine.connect() if callable(getattr(engine, "connect", None))
        else nullcontext(engine)
    )
    with connection_scope as connection:
        counts = connection.execute(
            text(
                f"SELECT COUNT(*),COUNT(DISTINCT `{code_column}`) "
                f"FROM `{table}` WHERE {where}"
            ),
            {"trade_date": trade_date, "source": PROVIDER},
        ).one()
        rows = [
            dict(row)
            for row in connection.execute(
                text(
                    f"SELECT {columns} FROM `{table}` WHERE {where} "
                    f"ORDER BY `{code_column}`,trade_time"
                ),
                {"trade_date": trade_date, "source": PROVIDER},
            ).mappings()
        ]
    return int(counts[0]), int(counts[1]), rows


def persisted_fingerprint(
    engine: Any,
    *,
    dataset: str,
    trade_date: str,
) -> dict[str, Any]:
    row_count, code_count, rows = persisted_rows(
        engine,
        dataset=dataset,
        trade_date=trade_date,
    )
    return {
        "row_count": row_count,
        "code_count": code_count,
        "row_hash": row_hash(rows),
    }


def validate_persisted_result(
    engine: Any,
    *,
    task_type: str,
    payload: Mapping[str, Any],
    expected_session: str = "",
) -> dict[str, Any]:
    expected_build = str(
        os.environ.get("PROBIGA_SCHEDULER_BUILD_SHA")
        or os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or payload.get("build_sha")
        or ""
    ).strip().lower()
    if validate_task_result(
        task_type,
        payload,
        return_code=0,
        expected_build_sha=expected_build,
    ) != "complete":
        raise ValueError("QMT datadir task result is invalid")
    dataset = TASK_TYPE_DATASETS[task_type]
    sessions = list(payload["sessions"])
    if expected_session and sessions != [expected_session]:
        raise ValueError("QMT datadir result session differs")
    verified: list[dict[str, Any]] = []
    for partition in payload["partitions"]:
        trade_date = str(partition["trade_date"])
        receipt = load_receipt(engine, dataset=dataset, trade_date=trade_date)
        if receipt is None or receipt.get("status") != "COMPLETE":
            raise ValueError("QMT datadir completion receipt is missing")
        stored = partition_from_receipt(receipt)
        for key in (
            "source_root",
            "run_id",
            "row_count",
            "code_count",
            "content_sha256",
            "row_hash",
            "eligible_code_count",
            "coverage",
            "readback_rows",
            "readback_codes",
            "attestation_run_id",
        ):
            if stored.get(key) != partition.get(key):
                raise ValueError(f"QMT datadir receipt field differs: {key}")
        row_count, code_count, rows = persisted_rows(
            engine, dataset=dataset, trade_date=trade_date
        )
        if (
            row_count != int(partition["row_count"])
            or code_count != int(partition["code_count"])
            or row_hash(rows) != partition["row_hash"]
        ):
            raise ValueError("QMT datadir persisted partition differs")
        if dataset == "stock_daily":
            from server.common.qmt_daily_market_truth import (
                load_qmt_daily_market_truth,
            )

            truth = load_qmt_daily_market_truth(
                engine,
                start_date=trade_date,
                end_date=trade_date,
                decision_known_at=dt.datetime.now(),
            )
            if truth.run_id != partition["attestation_run_id"]:
                raise ValueError("QMT datadir daily attestation differs")
        verified.append(
            {
                "trade_date": trade_date,
                "row_count": row_count,
                "code_count": code_count,
                "row_hash": partition["row_hash"],
            }
        )
    return {
        "dataset": dataset,
        "sessions": sessions,
        "row_count": sum(item["row_count"] for item in verified),
        "partitions": verified,
    }
