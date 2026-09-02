"""Build-bound Windows QMT edge release-bootstrap audit receipts.

The receipt itself lives in the scheduler audit ledger.  It is deliberately
not recorded as a successful execution of any scheduled foundation job.  Its
catalog and calendar identities point at independently validated, append-only
QMT reference tables.  A fresh application release may reuse a still-current
immutable reference batch; the receipt remains build-bound while the much
slower reference capture keeps its own identity and cadence.
"""
from __future__ import annotations

from datetime import datetime
import json
import re
from typing import Any, Mapping

from sqlalchemy import text

from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_stock_catalog import load_stock_catalog
from server.common.qmt_trade_calendar import load_trade_calendar_receipt


BUILD_SHA_RE = re.compile(r"[0-9a-f]{40}")
QMT_EDGE_RELEASE_RECEIPT_SCHEMA = (
    "probiga.qmt-windows-edge-release-bootstrap.v1"
)
QMT_EDGE_RELEASE_REQUEST_SCHEMA = (
    "probiga.qmt-windows-edge-release-request.v1"
)
QMT_EDGE_RELEASE_RECEIPT_TASK_TYPE = "qmt_edge_release_bootstrap"
QMT_EDGE_RELEASE_RECEIPT_TRIGGER_SOURCE = "release_bootstrap"
QMT_EDGE_RELEASE_REQUEST_TASK_TYPE = "qmt_edge_release_request"
QMT_EDGE_RELEASE_REQUEST_TRIGGER_SOURCE = "release_request"
QMT_EDGE_ROLE = "qmt_windows_edge"

_UNSIGNED_KEYS = frozenset({
    "schema",
    "build_sha",
    "request_run_uid",
    "requested_at",
    "executor_role",
    "host_name",
    "scheduler_instance_id",
    "catalog_batch_id",
    "catalog_manifest_hash",
    "calendar_batch_id",
    "calendar_manifest_hash",
    "calendar_start_date",
    "calendar_end_date",
    "local_history_schema_hash",
    "qmt_capability_hash",
    "captured_at",
})


class QmtEdgeReleaseReceiptError(RuntimeError):
    """Raised when a release bootstrap receipt cannot be trusted."""


def _sha256(value: Any, *, label: str) -> str:
    normalized = str(value or "").strip().lower()
    if re.fullmatch(r"[0-9a-f]{64}", normalized) is None:
        raise QmtEdgeReleaseReceiptError(f"{label} is invalid")
    return normalized


def _build_sha(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if BUILD_SHA_RE.fullmatch(normalized) is None or normalized == "0" * 40:
        raise QmtEdgeReleaseReceiptError("build_sha is invalid")
    return normalized


def _iso_datetime(value: Any) -> str:
    raw = value.isoformat(timespec="seconds") if isinstance(value, datetime) else str(value or "")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise QmtEdgeReleaseReceiptError("captured_at is invalid") from exc
    normalized = parsed.replace(microsecond=0).isoformat(timespec="seconds")
    if raw != normalized:
        raise QmtEdgeReleaseReceiptError("captured_at is not canonical")
    return normalized


def build_qmt_edge_release_receipt(
    *,
    build_sha: str,
    request_run_uid: str,
    requested_at: Any,
    host_name: str,
    scheduler_instance_id: str,
    catalog_batch_id: str,
    catalog_manifest_hash: str,
    calendar_batch_id: str,
    calendar_manifest_hash: str,
    calendar_start_date: str,
    calendar_end_date: str,
    local_history_schema_hash: str,
    qmt_capability_hash: str,
    captured_at: Any,
) -> dict[str, Any]:
    """Return the exact canonical payload stored in the scheduler ledger."""

    sha = _build_sha(build_sha)
    expected_request_uid = qmt_edge_release_request_run_uid(sha)
    if str(request_run_uid or "") != expected_request_uid:
        raise QmtEdgeReleaseReceiptError("request_run_uid is invalid")
    host = str(host_name or "").strip()
    instance = str(scheduler_instance_id or "").strip()
    if not host or len(host) > 128:
        raise QmtEdgeReleaseReceiptError("host_name is invalid")
    if not instance or len(instance) > 128 or not instance.startswith(f"{host}-"):
        raise QmtEdgeReleaseReceiptError("scheduler_instance_id is invalid")
    catalog_batch = str(catalog_batch_id or "").strip()
    calendar_batch = str(calendar_batch_id or "").strip()
    if (
        catalog_batch != calendar_batch
        or re.fullmatch(r"qmt_rel_[0-9a-f]{40}_[0-9]{14}", catalog_batch)
        is None
        or len(catalog_batch) > 64
    ):
        raise QmtEdgeReleaseReceiptError(
            "reference batch is not bound to an immutable QMT release capture"
        )
    unsigned = {
        "schema": QMT_EDGE_RELEASE_RECEIPT_SCHEMA,
        "build_sha": sha,
        "request_run_uid": expected_request_uid,
        "requested_at": _iso_datetime(requested_at),
        "executor_role": QMT_EDGE_ROLE,
        "host_name": host,
        "scheduler_instance_id": instance,
        "catalog_batch_id": catalog_batch,
        "catalog_manifest_hash": _sha256(
            catalog_manifest_hash, label="catalog_manifest_hash"
        ),
        "calendar_batch_id": calendar_batch,
        "calendar_manifest_hash": _sha256(
            calendar_manifest_hash, label="calendar_manifest_hash"
        ),
        "calendar_start_date": str(calendar_start_date or "")[:10],
        "calendar_end_date": str(calendar_end_date or "")[:10],
        "local_history_schema_hash": _sha256(
            local_history_schema_hash, label="local_history_schema_hash"
        ),
        "qmt_capability_hash": _sha256(
            qmt_capability_hash, label="qmt_capability_hash"
        ),
        "captured_at": _iso_datetime(captured_at),
    }
    for name in ("calendar_start_date", "calendar_end_date"):
        try:
            if datetime.strptime(unsigned[name], "%Y-%m-%d").date().isoformat() != unsigned[name]:
                raise ValueError
        except ValueError as exc:
            raise QmtEdgeReleaseReceiptError(f"{name} is invalid") from exc
    if unsigned["calendar_start_date"] > unsigned["calendar_end_date"]:
        raise QmtEdgeReleaseReceiptError("calendar range is invalid")
    return {**unsigned, "receipt_hash": canonical_digest(unsigned)}


def validate_qmt_edge_release_receipt(
    connection: Any,
    value: Mapping[str, Any] | str,
    *,
    expected_build_sha: str,
    expected_host_name: str,
    expected_scheduler_instance_id: str,
) -> dict[str, Any]:
    """Validate the audit payload and its immutable reference receipts."""

    try:
        payload = json.loads(value) if isinstance(value, str) else dict(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise QmtEdgeReleaseReceiptError("release receipt JSON is invalid") from exc
    if set(payload) != _UNSIGNED_KEYS | {"receipt_hash"}:
        raise QmtEdgeReleaseReceiptError("release receipt fields differ")
    rebuilt = build_qmt_edge_release_receipt(
        build_sha=str(payload.get("build_sha") or ""),
        request_run_uid=str(payload.get("request_run_uid") or ""),
        requested_at=payload.get("requested_at"),
        host_name=str(payload.get("host_name") or ""),
        scheduler_instance_id=str(payload.get("scheduler_instance_id") or ""),
        catalog_batch_id=str(payload.get("catalog_batch_id") or ""),
        catalog_manifest_hash=str(payload.get("catalog_manifest_hash") or ""),
        calendar_batch_id=str(payload.get("calendar_batch_id") or ""),
        calendar_manifest_hash=str(payload.get("calendar_manifest_hash") or ""),
        calendar_start_date=str(payload.get("calendar_start_date") or ""),
        calendar_end_date=str(payload.get("calendar_end_date") or ""),
        local_history_schema_hash=str(payload.get("local_history_schema_hash") or ""),
        qmt_capability_hash=str(payload.get("qmt_capability_hash") or ""),
        captured_at=payload.get("captured_at"),
    )
    if payload != rebuilt:
        raise QmtEdgeReleaseReceiptError("release receipt hash/content differs")
    if rebuilt["build_sha"] != _build_sha(expected_build_sha):
        raise QmtEdgeReleaseReceiptError("release receipt build differs")
    if rebuilt["host_name"] != str(expected_host_name or ""):
        raise QmtEdgeReleaseReceiptError("release receipt host differs")
    if rebuilt["scheduler_instance_id"] != str(
        expected_scheduler_instance_id or ""
    ):
        raise QmtEdgeReleaseReceiptError("release receipt instance differs")
    if rebuilt["captured_at"] < rebuilt["requested_at"]:
        raise QmtEdgeReleaseReceiptError("release receipt predates its request")
    request = load_qmt_edge_release_request(
        connection, expected_build_sha=rebuilt["build_sha"]
    )
    if (
        request["request_run_uid"] != rebuilt["request_run_uid"]
        or request["requested_at"] != rebuilt["requested_at"]
    ):
        raise QmtEdgeReleaseReceiptError(
            "release receipt does not match the immutable request"
        )

    decision_known_at = datetime.now().replace(microsecond=0)
    catalog = load_stock_catalog(
        connection,
        batch_id=rebuilt["catalog_batch_id"],
        decision_known_at=decision_known_at,
    )
    if catalog.manifest_hash != rebuilt["catalog_manifest_hash"]:
        raise QmtEdgeReleaseReceiptError("catalog immutable receipt differs")
    calendar = load_trade_calendar_receipt(
        connection,
        batch_id=rebuilt["calendar_batch_id"],
        start_date=rebuilt["calendar_start_date"],
        end_date=rebuilt["calendar_end_date"],
        decision_known_at=decision_known_at,
    )
    if calendar.manifest_hash != rebuilt["calendar_manifest_hash"]:
        raise QmtEdgeReleaseReceiptError("calendar immutable receipt differs")
    return rebuilt


def qmt_edge_release_run_uid(build_sha: str, scheduler_instance_id: str) -> str:
    sha = _build_sha(build_sha)
    identity_hash = canonical_digest({
        "build_sha": sha,
        "scheduler_instance_id": str(scheduler_instance_id or ""),
    })
    return f"qmt-edge-bootstrap-{sha[:12]}-{identity_hash[:24]}"


def qmt_edge_release_request_run_uid(build_sha: str) -> str:
    """Return the one idempotent deployment request identity for a build."""

    return f"qmt-edge-request-{_build_sha(build_sha)}"


def build_qmt_edge_release_request(
    *, build_sha: str, requested_at: Any,
) -> dict[str, Any]:
    sha = _build_sha(build_sha)
    unsigned = {
        "schema": QMT_EDGE_RELEASE_REQUEST_SCHEMA,
        "build_sha": sha,
        "request_run_uid": qmt_edge_release_request_run_uid(sha),
        "requested_at": _iso_datetime(requested_at),
    }
    return {**unsigned, "request_hash": canonical_digest(unsigned)}


def validate_qmt_edge_release_request(
    value: Mapping[str, Any] | str, *, expected_build_sha: str,
) -> dict[str, Any]:
    try:
        payload = json.loads(value) if isinstance(value, str) else dict(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise QmtEdgeReleaseReceiptError("release request JSON is invalid") from exc
    if set(payload) != {
        "schema", "build_sha", "request_run_uid", "requested_at",
        "request_hash",
    }:
        raise QmtEdgeReleaseReceiptError("release request fields differ")
    rebuilt = build_qmt_edge_release_request(
        build_sha=str(payload.get("build_sha") or ""),
        requested_at=payload.get("requested_at"),
    )
    if payload != rebuilt or rebuilt["build_sha"] != _build_sha(expected_build_sha):
        raise QmtEdgeReleaseReceiptError("release request content differs")
    return rebuilt


def _reference_task_id(connection: Any) -> int:
    rows = connection.execute(
        text(
            "SELECT id FROM st_scheduled_tasks "
            "WHERE task_type='qmt_reference_incremental' ORDER BY id"
        )
    ).fetchall()
    if len(rows) != 1 or int(rows[0][0] or 0) <= 0:
        raise QmtEdgeReleaseReceiptError(
            "qmt_reference_incremental task identity is unavailable"
        )
    return int(rows[0][0])


def _validated_release_request_row(
    row: Mapping[str, Any], *, expected_build_sha: str,
) -> dict[str, Any]:
    run_uid = qmt_edge_release_request_run_uid(expected_build_sha)
    if (
        str(row.get("run_uid") or "") != run_uid
        or str(row.get("status") or "") != "pending"
        or str(row.get("task_type") or "")
        != QMT_EDGE_RELEASE_REQUEST_TASK_TYPE
        or str(row.get("build_sha") or "").lower()
        != _build_sha(expected_build_sha)
        or str(row.get("trigger_source") or "")
        != QMT_EDGE_RELEASE_REQUEST_TRIGGER_SOURCE
    ):
        raise QmtEdgeReleaseReceiptError("release request ledger row differs")
    return validate_qmt_edge_release_request(
        str(row.get("output") or ""),
        expected_build_sha=expected_build_sha,
    )


def insert_qmt_edge_release_request(
    connection: Any, request: Mapping[str, Any],
) -> dict[str, Any]:
    payload = validate_qmt_edge_release_request(
        request, expected_build_sha=str(request.get("build_sha") or "")
    )
    run_uid = payload["request_run_uid"]
    serialized = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    existing = connection.execute(
        text(
            "SELECT run_uid, status, task_type, build_sha, trigger_source, "
            "output "
            "FROM st_scheduled_task_history "
            "WHERE run_uid=:run_uid"
        ),
        {"run_uid": run_uid},
    ).mappings().all()
    if existing:
        if len(existing) != 1:
            raise QmtEdgeReleaseReceiptError("release request replay differs")
        persisted = _validated_release_request_row(
            existing[0], expected_build_sha=payload["build_sha"]
        )
        return {"status": "idempotent", **persisted}
    connection.execute(
        text(
            "INSERT INTO st_scheduled_task_history ("
            "run_uid, task_id, task_name, task_type, run_at, finished_at, "
            "status, duration, exit_code, output, host_name, "
            "scheduler_instance_id, build_sha, trigger_source) VALUES ("
            ":run_uid, :task_id, 'QMT Windows edge release request', "
            ":task_type, :requested_at, :requested_at, 'pending', 0, NULL, "
            ":output, 'linux-release', :run_uid, :build_sha, :trigger_source)"
        ),
        {
            "run_uid": run_uid,
            "task_id": _reference_task_id(connection),
            "task_type": QMT_EDGE_RELEASE_REQUEST_TASK_TYPE,
            "requested_at": payload["requested_at"].replace("T", " "),
            "output": serialized,
            "build_sha": payload["build_sha"],
            "trigger_source": QMT_EDGE_RELEASE_REQUEST_TRIGGER_SOURCE,
        },
    )
    return {"status": "inserted", **payload}


def load_qmt_edge_release_request(
    connection: Any, *, expected_build_sha: str,
) -> dict[str, Any]:
    run_uid = qmt_edge_release_request_run_uid(expected_build_sha)
    rows = connection.execute(
        text(
            "SELECT run_uid, status, task_type, build_sha, trigger_source, "
            "output "
            "FROM st_scheduled_task_history WHERE run_uid=:run_uid"
        ),
        {"run_uid": run_uid},
    ).mappings().all()
    if len(rows) != 1:
        raise QmtEdgeReleaseReceiptError("release request is unavailable")
    return _validated_release_request_row(
        rows[0], expected_build_sha=expected_build_sha
    )


def insert_qmt_edge_release_receipt(
    connection: Any,
    receipt: Mapping[str, Any],
) -> dict[str, Any]:
    """Append one final bootstrap audit row, idempotently per edge instance."""

    payload = build_qmt_edge_release_receipt(
        build_sha=str(receipt.get("build_sha") or ""),
        request_run_uid=str(receipt.get("request_run_uid") or ""),
        requested_at=receipt.get("requested_at"),
        host_name=str(receipt.get("host_name") or ""),
        scheduler_instance_id=str(receipt.get("scheduler_instance_id") or ""),
        catalog_batch_id=str(receipt.get("catalog_batch_id") or ""),
        catalog_manifest_hash=str(receipt.get("catalog_manifest_hash") or ""),
        calendar_batch_id=str(receipt.get("calendar_batch_id") or ""),
        calendar_manifest_hash=str(receipt.get("calendar_manifest_hash") or ""),
        calendar_start_date=str(receipt.get("calendar_start_date") or ""),
        calendar_end_date=str(receipt.get("calendar_end_date") or ""),
        local_history_schema_hash=str(receipt.get("local_history_schema_hash") or ""),
        qmt_capability_hash=str(receipt.get("qmt_capability_hash") or ""),
        captured_at=receipt.get("captured_at"),
    )
    if dict(receipt) != payload:
        raise QmtEdgeReleaseReceiptError("release receipt is not canonical")
    run_uid = qmt_edge_release_run_uid(
        payload["build_sha"], payload["scheduler_instance_id"]
    )
    existing = connection.execute(
        text(
            "SELECT output FROM st_scheduled_task_history "
            "WHERE run_uid=:run_uid"
        ),
        {"run_uid": run_uid},
    ).mappings().all()
    serialized = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    if existing:
        if len(existing) != 1 or str(existing[0].get("output") or "") != serialized:
            raise QmtEdgeReleaseReceiptError("release receipt replay differs")
        return {"status": "idempotent", "run_uid": run_uid, **payload}
    request = load_qmt_edge_release_request(
        connection, expected_build_sha=payload["build_sha"]
    )
    if (
        request["request_run_uid"] != payload["request_run_uid"]
        or request["requested_at"] != payload["requested_at"]
    ):
        raise QmtEdgeReleaseReceiptError("release receipt request binding differs")
    connection.execute(
        text(
            "INSERT INTO st_scheduled_task_history ("
            "run_uid, task_id, task_name, task_type, run_at, finished_at, "
            "status, duration, exit_code, output, host_name, "
            "scheduler_instance_id, build_sha, trigger_source) VALUES ("
            ":run_uid, :task_id, 'QMT Windows edge release bootstrap', "
            ":task_type, :captured_at, :captured_at, 'success', 0, 0, "
            ":output, :host_name, :scheduler_instance_id, :build_sha, "
            ":trigger_source)"
        ),
        {
            "run_uid": run_uid,
            "task_id": _reference_task_id(connection),
            "task_type": QMT_EDGE_RELEASE_RECEIPT_TASK_TYPE,
            "captured_at": payload["captured_at"].replace("T", " "),
            "output": serialized,
            "host_name": payload["host_name"],
            "scheduler_instance_id": payload["scheduler_instance_id"],
            "build_sha": payload["build_sha"],
            "trigger_source": QMT_EDGE_RELEASE_RECEIPT_TRIGGER_SOURCE,
        },
    )
    return {"status": "inserted", "run_uid": run_uid, **payload}


__all__ = [
    "QMT_EDGE_RELEASE_RECEIPT_SCHEMA",
    "QMT_EDGE_RELEASE_RECEIPT_TASK_TYPE",
    "QMT_EDGE_RELEASE_RECEIPT_TRIGGER_SOURCE",
    "QMT_EDGE_RELEASE_REQUEST_SCHEMA",
    "QMT_EDGE_RELEASE_REQUEST_TASK_TYPE",
    "QMT_EDGE_RELEASE_REQUEST_TRIGGER_SOURCE",
    "QMT_EDGE_ROLE",
    "QmtEdgeReleaseReceiptError",
    "build_qmt_edge_release_request",
    "build_qmt_edge_release_receipt",
    "insert_qmt_edge_release_request",
    "insert_qmt_edge_release_receipt",
    "load_qmt_edge_release_request",
    "qmt_edge_release_request_run_uid",
    "qmt_edge_release_run_uid",
    "validate_qmt_edge_release_receipt",
]
