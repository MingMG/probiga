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
QMT_EDGE_RELEASE_QUIESCENCE_SCHEMA = (
    "probiga.qmt-windows-edge-release-quiescence.v1"
)
QMT_EDGE_RELEASE_ACTIVATION_SCHEMA = (
    "probiga.qmt-windows-edge-release-activation.v1"
)
QMT_EDGE_RELEASE_RECEIPT_TASK_TYPE = "qmt_edge_release_bootstrap"
QMT_EDGE_RELEASE_RECEIPT_TRIGGER_SOURCE = "release_bootstrap"
QMT_EDGE_RELEASE_REQUEST_TASK_TYPE = "qmt_edge_release_request"
QMT_EDGE_RELEASE_REQUEST_TRIGGER_SOURCE = "release_request"
QMT_EDGE_RELEASE_QUIESCENCE_TRIGGER_SOURCE = "release_quiescence"
QMT_EDGE_RELEASE_ACTIVATION_TRIGGER_SOURCE = "release_activation"
QMT_EDGE_ROLE = "qmt_windows_edge"
QMT_EDGE_RELEASE_REQUEST_TASK_NAME = "QMT Windows edge release request"
QMT_EDGE_RELEASE_QUIESCENCE_TASK_NAME = (
    "QMT Windows edge release quiescence hold"
)
QMT_EDGE_RELEASE_ACTIVATION_TASK_NAME = (
    "QMT Windows edge release activation grant"
)

_RELEASE_CONTROL_LEDGER_COLUMNS = (
    "id",
    "run_uid",
    "task_id",
    "task_name",
    "task_type",
    "run_at",
    "finished_at",
    "status",
    "duration",
    "exit_code",
    "output",
    "host_name",
    "scheduler_instance_id",
    "build_sha",
    "trigger_source",
)
_RELEASE_CONTROL_LEDGER_SELECT = ", ".join(_RELEASE_CONTROL_LEDGER_COLUMNS)

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


def _deployment_attempt_id(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if re.fullmatch(r"[0-9a-f]{32}", normalized) is None or normalized == "0" * 32:
        raise QmtEdgeReleaseReceiptError("deployment_attempt_id is invalid")
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


def qmt_edge_release_quiescence_run_uid(deployment_attempt_id: str) -> str:
    return f"qmt-edge-hold-{_deployment_attempt_id(deployment_attempt_id)}"


def qmt_edge_release_activation_run_uid(deployment_attempt_id: str) -> str:
    return f"qmt-edge-grant-{_deployment_attempt_id(deployment_attempt_id)}"


def build_qmt_edge_release_quiescence_hold(
    *,
    build_sha: str,
    deployment_attempt_id: str,
    requested_at: Any,
) -> dict[str, Any]:
    """Build one append-only request for the Windows writer to remain stopped."""

    sha = _build_sha(build_sha)
    attempt_id = _deployment_attempt_id(deployment_attempt_id)
    unsigned = {
        "schema": QMT_EDGE_RELEASE_QUIESCENCE_SCHEMA,
        "build_sha": sha,
        "deployment_attempt_id": attempt_id,
        "hold_run_uid": qmt_edge_release_quiescence_run_uid(attempt_id),
        "request_run_uid": qmt_edge_release_request_run_uid(sha),
        "requested_at": _iso_datetime(requested_at),
        "real_order": False,
    }
    return {**unsigned, "hold_hash": canonical_digest(unsigned)}


def validate_qmt_edge_release_quiescence_hold(
    value: Mapping[str, Any] | str,
    *,
    expected_build_sha: str,
    expected_deployment_attempt_id: str | None = None,
) -> dict[str, Any]:
    try:
        payload = json.loads(value) if isinstance(value, str) else dict(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise QmtEdgeReleaseReceiptError(
            "release quiescence hold JSON is invalid"
        ) from exc
    if set(payload) != {
        "schema",
        "build_sha",
        "deployment_attempt_id",
        "hold_run_uid",
        "request_run_uid",
        "requested_at",
        "real_order",
        "hold_hash",
    }:
        raise QmtEdgeReleaseReceiptError("release quiescence hold fields differ")
    rebuilt = build_qmt_edge_release_quiescence_hold(
        build_sha=str(payload.get("build_sha") or ""),
        deployment_attempt_id=str(payload.get("deployment_attempt_id") or ""),
        requested_at=payload.get("requested_at"),
    )
    expected_attempt_id = (
        _deployment_attempt_id(expected_deployment_attempt_id)
        if expected_deployment_attempt_id is not None
        else None
    )
    if (
        payload != rebuilt
        or rebuilt["build_sha"] != _build_sha(expected_build_sha)
        or (
            expected_attempt_id is not None
            and rebuilt["deployment_attempt_id"] != expected_attempt_id
        )
    ):
        raise QmtEdgeReleaseReceiptError("release quiescence hold content differs")
    return rebuilt


def build_qmt_edge_release_activation_grant(
    *,
    hold: Mapping[str, Any],
    granted_at: Any,
) -> dict[str, Any]:
    verified_hold = validate_qmt_edge_release_quiescence_hold(
        hold,
        expected_build_sha=str(hold.get("build_sha") or ""),
        expected_deployment_attempt_id=str(
            hold.get("deployment_attempt_id") or ""
        ),
    )
    unsigned = {
        "schema": QMT_EDGE_RELEASE_ACTIVATION_SCHEMA,
        "build_sha": verified_hold["build_sha"],
        "deployment_attempt_id": verified_hold["deployment_attempt_id"],
        "grant_run_uid": qmt_edge_release_activation_run_uid(
            verified_hold["deployment_attempt_id"]
        ),
        "hold_run_uid": verified_hold["hold_run_uid"],
        "hold_hash": verified_hold["hold_hash"],
        "granted_at": _iso_datetime(granted_at),
        "schema_cutover_verified": True,
        "real_order": False,
    }
    if unsigned["granted_at"] < verified_hold["requested_at"]:
        raise QmtEdgeReleaseReceiptError("release activation grant predates hold")
    return {**unsigned, "grant_hash": canonical_digest(unsigned)}


def validate_qmt_edge_release_activation_grant(
    value: Mapping[str, Any] | str,
    *,
    expected_hold: Mapping[str, Any],
) -> dict[str, Any]:
    hold = validate_qmt_edge_release_quiescence_hold(
        expected_hold,
        expected_build_sha=str(expected_hold.get("build_sha") or ""),
        expected_deployment_attempt_id=str(
            expected_hold.get("deployment_attempt_id") or ""
        ),
    )
    try:
        payload = json.loads(value) if isinstance(value, str) else dict(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise QmtEdgeReleaseReceiptError(
            "release activation grant JSON is invalid"
        ) from exc
    if set(payload) != {
        "schema",
        "build_sha",
        "deployment_attempt_id",
        "grant_run_uid",
        "hold_run_uid",
        "hold_hash",
        "granted_at",
        "schema_cutover_verified",
        "real_order",
        "grant_hash",
    }:
        raise QmtEdgeReleaseReceiptError("release activation grant fields differ")
    rebuilt = build_qmt_edge_release_activation_grant(
        hold=hold,
        granted_at=payload.get("granted_at"),
    )
    if payload != rebuilt:
        raise QmtEdgeReleaseReceiptError("release activation grant content differs")
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


def _serialized_payload(payload: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _ledger_timestamp(value: Any) -> str | None:
    """Normalize a driver-returned DATETIME without weakening exact seconds."""

    raw = value.isoformat() if isinstance(value, datetime) else str(value or "")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.microsecond != 0:
        return None
    return parsed.isoformat(timespec="seconds")


def _validate_release_control_ledger_row(
    row: Mapping[str, Any],
    *,
    payload: Mapping[str, Any],
    run_uid: str,
    expected_task_id: int,
    task_name: str,
    status: str,
    event_at: str,
    exit_code: int | None,
    scheduler_instance_id: str,
    trigger_source: str,
    error: str,
) -> None:
    """Validate every persisted column of an append-only release-control row."""

    observed_exit_code = row.get("exit_code")
    exit_code_matches = (
        observed_exit_code is None
        if exit_code is None
        else type(observed_exit_code) is int and observed_exit_code == exit_code
    )
    if (
        set(row) != set(_RELEASE_CONTROL_LEDGER_COLUMNS)
        or type(row.get("id")) is not int
        or int(row["id"]) <= 0
        or str(row.get("run_uid") or "") != run_uid
        or type(row.get("task_id")) is not int
        or int(row["task_id"]) != expected_task_id
        or str(row.get("task_name") or "") != task_name
        or str(row.get("task_type") or "")
        != QMT_EDGE_RELEASE_REQUEST_TASK_TYPE
        or _ledger_timestamp(row.get("run_at")) != event_at
        or _ledger_timestamp(row.get("finished_at")) != event_at
        or str(row.get("status") or "") != status
        or type(row.get("duration")) is not int
        or int(row["duration"]) != 0
        or not exit_code_matches
        or str(row.get("output") or "") != _serialized_payload(payload)
        or str(row.get("host_name") or "") != "linux-release"
        or str(row.get("scheduler_instance_id") or "")
        != scheduler_instance_id
        or str(row.get("build_sha") or "").lower()
        != str(payload.get("build_sha") or "")
        or str(row.get("trigger_source") or "") != trigger_source
    ):
        raise QmtEdgeReleaseReceiptError(error)


def _validated_release_quiescence_row(
    row: Mapping[str, Any],
    *,
    expected_build_sha: str,
    expected_task_id: int,
    expected_deployment_attempt_id: str | None = None,
) -> dict[str, Any]:
    payload = validate_qmt_edge_release_quiescence_hold(
        str(row.get("output") or ""),
        expected_build_sha=expected_build_sha,
        expected_deployment_attempt_id=expected_deployment_attempt_id,
    )
    _validate_release_control_ledger_row(
        row,
        payload=payload,
        run_uid=payload["hold_run_uid"],
        expected_task_id=expected_task_id,
        task_name=QMT_EDGE_RELEASE_QUIESCENCE_TASK_NAME,
        status="pending",
        event_at=payload["requested_at"],
        exit_code=None,
        scheduler_instance_id=payload["deployment_attempt_id"],
        trigger_source=QMT_EDGE_RELEASE_QUIESCENCE_TRIGGER_SOURCE,
        error="release quiescence hold ledger row differs",
    )
    return payload


def insert_qmt_edge_release_quiescence_hold(
    connection: Any,
    hold: Mapping[str, Any],
) -> dict[str, Any]:
    payload = validate_qmt_edge_release_quiescence_hold(
        hold,
        expected_build_sha=str(hold.get("build_sha") or ""),
        expected_deployment_attempt_id=str(
            hold.get("deployment_attempt_id") or ""
        ),
    )
    expected_task_id = _reference_task_id(connection)
    run_uid = payload["hold_run_uid"]
    existing = connection.execute(
        text(
            f"SELECT {_RELEASE_CONTROL_LEDGER_SELECT} "
            "FROM st_scheduled_task_history "
            "WHERE run_uid=:run_uid"
        ),
        {"run_uid": run_uid},
    ).mappings().all()
    if existing:
        if len(existing) != 1:
            raise QmtEdgeReleaseReceiptError(
                "release quiescence hold replay differs"
            )
        persisted = _validated_release_quiescence_row(
            existing[0],
            expected_build_sha=payload["build_sha"],
            expected_task_id=expected_task_id,
            expected_deployment_attempt_id=payload["deployment_attempt_id"],
        )
        return {"status": "idempotent", **persisted}
    connection.execute(
        text(
            "INSERT INTO st_scheduled_task_history ("
            "run_uid, task_id, task_name, task_type, run_at, finished_at, "
            "status, duration, exit_code, output, host_name, "
            "scheduler_instance_id, build_sha, trigger_source) VALUES ("
            ":run_uid, :task_id, :task_name, "
            ":task_type, :requested_at, :requested_at, 'pending', 0, NULL, "
            ":output, 'linux-release', :deployment_attempt_id, :build_sha, "
            ":trigger_source)"
        ),
        {
            "run_uid": run_uid,
            "task_id": expected_task_id,
            "task_name": QMT_EDGE_RELEASE_QUIESCENCE_TASK_NAME,
            "task_type": QMT_EDGE_RELEASE_REQUEST_TASK_TYPE,
            "requested_at": payload["requested_at"].replace("T", " "),
            "output": _serialized_payload(payload),
            "deployment_attempt_id": payload["deployment_attempt_id"],
            "build_sha": payload["build_sha"],
            "trigger_source": QMT_EDGE_RELEASE_QUIESCENCE_TRIGGER_SOURCE,
        },
    )
    return {"status": "inserted", **payload}


def _latest_qmt_edge_release_quiescence_rows(
    connection: Any,
    *,
    expected_build_sha: str,
) -> list[Mapping[str, Any]]:
    return list(
        connection.execute(
            text(
                f"SELECT {_RELEASE_CONTROL_LEDGER_SELECT} "
                "FROM st_scheduled_task_history "
                "WHERE task_type=:task_type AND trigger_source=:trigger_source "
                "AND build_sha=:build_sha ORDER BY id DESC LIMIT 1"
            ),
            {
                "task_type": QMT_EDGE_RELEASE_REQUEST_TASK_TYPE,
                "trigger_source": QMT_EDGE_RELEASE_QUIESCENCE_TRIGGER_SOURCE,
                "build_sha": _build_sha(expected_build_sha),
            },
        ).mappings().all()
    )


def _load_latest_qmt_edge_release_quiescence_hold(
    connection: Any,
    *,
    expected_build_sha: str,
    expected_task_id: int,
    expected_deployment_attempt_id: str | None = None,
) -> dict[str, Any]:
    rows = _latest_qmt_edge_release_quiescence_rows(
        connection,
        expected_build_sha=expected_build_sha,
    )
    if len(rows) != 1:
        raise QmtEdgeReleaseReceiptError(
            "release quiescence hold is unavailable"
        )
    return _validated_release_quiescence_row(
        rows[0],
        expected_build_sha=expected_build_sha,
        expected_task_id=expected_task_id,
        expected_deployment_attempt_id=expected_deployment_attempt_id,
    )


def load_latest_qmt_edge_release_quiescence_hold(
    connection: Any,
    *,
    expected_build_sha: str,
    expected_deployment_attempt_id: str | None = None,
) -> dict[str, Any]:
    expected_task_id = _reference_task_id(connection)
    return _load_latest_qmt_edge_release_quiescence_hold(
        connection,
        expected_build_sha=expected_build_sha,
        expected_task_id=expected_task_id,
        expected_deployment_attempt_id=expected_deployment_attempt_id,
    )


def _validated_release_activation_row(
    row: Mapping[str, Any],
    *,
    expected_hold: Mapping[str, Any],
    expected_task_id: int,
) -> dict[str, Any]:
    payload = validate_qmt_edge_release_activation_grant(
        str(row.get("output") or ""),
        expected_hold=expected_hold,
    )
    _validate_release_control_ledger_row(
        row,
        payload=payload,
        run_uid=payload["grant_run_uid"],
        expected_task_id=expected_task_id,
        task_name=QMT_EDGE_RELEASE_ACTIVATION_TASK_NAME,
        status="success",
        event_at=payload["granted_at"],
        exit_code=0,
        scheduler_instance_id=payload["deployment_attempt_id"],
        trigger_source=QMT_EDGE_RELEASE_ACTIVATION_TRIGGER_SOURCE,
        error="release activation grant ledger row differs",
    )
    return payload


def insert_qmt_edge_release_activation_grant(
    connection: Any,
    grant: Mapping[str, Any],
) -> dict[str, Any]:
    build_sha = _build_sha(grant.get("build_sha"))
    attempt_id = _deployment_attempt_id(grant.get("deployment_attempt_id"))
    expected_task_id = _reference_task_id(connection)
    hold = _load_latest_qmt_edge_release_quiescence_hold(
        connection,
        expected_build_sha=build_sha,
        expected_task_id=expected_task_id,
        expected_deployment_attempt_id=attempt_id,
    )
    payload = validate_qmt_edge_release_activation_grant(
        grant,
        expected_hold=hold,
    )
    run_uid = payload["grant_run_uid"]
    existing = connection.execute(
        text(
            f"SELECT {_RELEASE_CONTROL_LEDGER_SELECT} "
            "FROM st_scheduled_task_history "
            "WHERE run_uid=:run_uid"
        ),
        {"run_uid": run_uid},
    ).mappings().all()
    if existing:
        if len(existing) != 1:
            raise QmtEdgeReleaseReceiptError(
                "release activation grant replay differs"
            )
        persisted = _validated_release_activation_row(
            existing[0],
            expected_hold=hold,
            expected_task_id=expected_task_id,
        )
        return {"status": "idempotent", **persisted}
    connection.execute(
        text(
            "INSERT INTO st_scheduled_task_history ("
            "run_uid, task_id, task_name, task_type, run_at, finished_at, "
            "status, duration, exit_code, output, host_name, "
            "scheduler_instance_id, build_sha, trigger_source) VALUES ("
            ":run_uid, :task_id, :task_name, "
            ":task_type, :granted_at, :granted_at, 'success', 0, 0, "
            ":output, 'linux-release', :deployment_attempt_id, :build_sha, "
            ":trigger_source)"
        ),
        {
            "run_uid": run_uid,
            "task_id": expected_task_id,
            "task_name": QMT_EDGE_RELEASE_ACTIVATION_TASK_NAME,
            "task_type": QMT_EDGE_RELEASE_REQUEST_TASK_TYPE,
            "granted_at": payload["granted_at"].replace("T", " "),
            "output": _serialized_payload(payload),
            "deployment_attempt_id": payload["deployment_attempt_id"],
            "build_sha": payload["build_sha"],
            "trigger_source": QMT_EDGE_RELEASE_ACTIVATION_TRIGGER_SOURCE,
        },
    )
    return {"status": "inserted", **payload}


def _validate_qmt_edge_release_activation_trigger_seal(
    connection: Any,
    *,
    expected_build_sha: str,
) -> dict[str, Any]:
    """Require the current build's VERIFIED privileged trigger seal."""

    sha = _build_sha(expected_build_sha)
    from server.engine.strategy_governance import (
        validate_privileged_trigger_migration_seal,
    )

    try:
        seal = validate_privileged_trigger_migration_seal(
            connection,
            expected_build_sha=sha,
        )
    except (RuntimeError, TypeError, ValueError) as exc:
        raise QmtEdgeReleaseReceiptError(
            "release activation trigger seal is unavailable"
        ) from exc
    if (
        str(seal.get("attested_build_sha") or "") != sha
        or seal.get("authority")
        != "PRIVILEGED_CUTOVER_TABLE_METADATA_SEAL"
    ):
        raise QmtEdgeReleaseReceiptError(
            "release activation trigger seal differs"
        )
    return seal


def check_qmt_edge_release_activation(
    connection: Any,
    *,
    expected_build_sha: str,
    expected_deployment_attempt_id: str | None = None,
) -> tuple[bool, dict[str, Any]]:
    """Prove that the newest hold for a build has one exact activation grant."""

    sha = _build_sha(expected_build_sha)
    _validate_qmt_edge_release_activation_trigger_seal(
        connection,
        expected_build_sha=sha,
    )
    expected_task_id = _reference_task_id(connection)
    expected_attempt = (
        _deployment_attempt_id(expected_deployment_attempt_id)
        if expected_deployment_attempt_id is not None
        else None
    )
    rows = _latest_qmt_edge_release_quiescence_rows(
        connection,
        expected_build_sha=sha,
    )
    if not rows:
        return False, {
            "status": "PENDING",
            "build_sha": sha,
            "deployment_attempt_id": expected_attempt or "",
            "activation_granted": False,
            "reason_code": "QMT_EDGE_RELEASE_ACTIVATION_PENDING",
            "hold": None,
            "grant": None,
        }
    hold = _validated_release_quiescence_row(
        rows[0],
        expected_build_sha=sha,
        expected_task_id=expected_task_id,
        expected_deployment_attempt_id=expected_attempt,
    )
    request = _load_qmt_edge_release_request(
        connection,
        expected_build_sha=sha,
        expected_task_id=expected_task_id,
    )
    if request["request_run_uid"] != hold["request_run_uid"]:
        raise QmtEdgeReleaseReceiptError(
            "release quiescence hold request binding differs"
        )
    grant_rows = connection.execute(
        text(
            f"SELECT {_RELEASE_CONTROL_LEDGER_SELECT} "
            "FROM st_scheduled_task_history "
            "WHERE run_uid=:run_uid"
        ),
        {
            "run_uid": qmt_edge_release_activation_run_uid(
                hold["deployment_attempt_id"]
            )
        },
    ).mappings().all()
    if not grant_rows:
        return False, {
            "status": "PENDING",
            "build_sha": sha,
            "deployment_attempt_id": hold["deployment_attempt_id"],
            "activation_granted": False,
            "reason_code": "QMT_EDGE_RELEASE_ACTIVATION_PENDING",
            "hold": hold,
            "grant": None,
        }
    if len(grant_rows) != 1:
        raise QmtEdgeReleaseReceiptError("release activation grant replay differs")
    grant = _validated_release_activation_row(
        grant_rows[0],
        expected_hold=hold,
        expected_task_id=expected_task_id,
    )
    return True, {
        "status": "READY",
        "build_sha": sha,
        "deployment_attempt_id": hold["deployment_attempt_id"],
        "activation_granted": True,
        "reason_code": "",
        "hold": hold,
        "grant": grant,
    }


def _validated_release_request_row(
    row: Mapping[str, Any], *, expected_build_sha: str, expected_task_id: int,
) -> dict[str, Any]:
    run_uid = qmt_edge_release_request_run_uid(expected_build_sha)
    payload = validate_qmt_edge_release_request(
        str(row.get("output") or ""),
        expected_build_sha=expected_build_sha,
    )
    _validate_release_control_ledger_row(
        row,
        payload=payload,
        run_uid=run_uid,
        expected_task_id=expected_task_id,
        task_name=QMT_EDGE_RELEASE_REQUEST_TASK_NAME,
        status="pending",
        event_at=payload["requested_at"],
        exit_code=None,
        scheduler_instance_id=run_uid,
        trigger_source=QMT_EDGE_RELEASE_REQUEST_TRIGGER_SOURCE,
        error="release request ledger row differs",
    )
    return payload


def insert_qmt_edge_release_request(
    connection: Any, request: Mapping[str, Any],
) -> dict[str, Any]:
    payload = validate_qmt_edge_release_request(
        request, expected_build_sha=str(request.get("build_sha") or "")
    )
    expected_task_id = _reference_task_id(connection)
    run_uid = payload["request_run_uid"]
    serialized = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    existing = connection.execute(
        text(
            f"SELECT {_RELEASE_CONTROL_LEDGER_SELECT} "
            "FROM st_scheduled_task_history "
            "WHERE run_uid=:run_uid"
        ),
        {"run_uid": run_uid},
    ).mappings().all()
    if existing:
        if len(existing) != 1:
            raise QmtEdgeReleaseReceiptError("release request replay differs")
        persisted = _validated_release_request_row(
            existing[0],
            expected_build_sha=payload["build_sha"],
            expected_task_id=expected_task_id,
        )
        return {"status": "idempotent", **persisted}
    connection.execute(
        text(
            "INSERT INTO st_scheduled_task_history ("
            "run_uid, task_id, task_name, task_type, run_at, finished_at, "
            "status, duration, exit_code, output, host_name, "
            "scheduler_instance_id, build_sha, trigger_source) VALUES ("
            ":run_uid, :task_id, :task_name, "
            ":task_type, :requested_at, :requested_at, 'pending', 0, NULL, "
            ":output, 'linux-release', :run_uid, :build_sha, :trigger_source)"
        ),
        {
            "run_uid": run_uid,
            "task_id": expected_task_id,
            "task_name": QMT_EDGE_RELEASE_REQUEST_TASK_NAME,
            "task_type": QMT_EDGE_RELEASE_REQUEST_TASK_TYPE,
            "requested_at": payload["requested_at"].replace("T", " "),
            "output": serialized,
            "build_sha": payload["build_sha"],
            "trigger_source": QMT_EDGE_RELEASE_REQUEST_TRIGGER_SOURCE,
        },
    )
    return {"status": "inserted", **payload}


def _load_qmt_edge_release_request(
    connection: Any, *, expected_build_sha: str, expected_task_id: int,
) -> dict[str, Any]:
    run_uid = qmt_edge_release_request_run_uid(expected_build_sha)
    rows = connection.execute(
        text(
            f"SELECT {_RELEASE_CONTROL_LEDGER_SELECT} "
            "FROM st_scheduled_task_history WHERE run_uid=:run_uid"
        ),
        {"run_uid": run_uid},
    ).mappings().all()
    if len(rows) != 1:
        raise QmtEdgeReleaseReceiptError("release request is unavailable")
    return _validated_release_request_row(
        rows[0],
        expected_build_sha=expected_build_sha,
        expected_task_id=expected_task_id,
    )


def load_qmt_edge_release_request(
    connection: Any, *, expected_build_sha: str,
) -> dict[str, Any]:
    expected_task_id = _reference_task_id(connection)
    return _load_qmt_edge_release_request(
        connection,
        expected_build_sha=expected_build_sha,
        expected_task_id=expected_task_id,
    )


def load_existing_qmt_edge_release_request(
    connection: Any, *, expected_build_sha: str,
) -> dict[str, Any] | None:
    """Return the exact persisted SHA request, or None only when absent."""

    expected_task_id = _reference_task_id(connection)
    run_uid = qmt_edge_release_request_run_uid(expected_build_sha)
    rows = connection.execute(
        text(
            f"SELECT {_RELEASE_CONTROL_LEDGER_SELECT} "
            "FROM st_scheduled_task_history WHERE run_uid=:run_uid"
        ),
        {"run_uid": run_uid},
    ).mappings().all()
    if not rows:
        return None
    if len(rows) != 1:
        raise QmtEdgeReleaseReceiptError("release request replay differs")
    return _validated_release_request_row(
        rows[0],
        expected_build_sha=expected_build_sha,
        expected_task_id=expected_task_id,
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
    "QMT_EDGE_RELEASE_ACTIVATION_SCHEMA",
    "QMT_EDGE_RELEASE_ACTIVATION_TRIGGER_SOURCE",
    "QMT_EDGE_RELEASE_QUIESCENCE_SCHEMA",
    "QMT_EDGE_RELEASE_QUIESCENCE_TRIGGER_SOURCE",
    "QMT_EDGE_RELEASE_RECEIPT_SCHEMA",
    "QMT_EDGE_RELEASE_RECEIPT_TASK_TYPE",
    "QMT_EDGE_RELEASE_RECEIPT_TRIGGER_SOURCE",
    "QMT_EDGE_RELEASE_REQUEST_SCHEMA",
    "QMT_EDGE_RELEASE_REQUEST_TASK_TYPE",
    "QMT_EDGE_RELEASE_REQUEST_TRIGGER_SOURCE",
    "QMT_EDGE_ROLE",
    "QmtEdgeReleaseReceiptError",
    "build_qmt_edge_release_activation_grant",
    "build_qmt_edge_release_quiescence_hold",
    "build_qmt_edge_release_request",
    "build_qmt_edge_release_receipt",
    "check_qmt_edge_release_activation",
    "insert_qmt_edge_release_activation_grant",
    "insert_qmt_edge_release_quiescence_hold",
    "insert_qmt_edge_release_request",
    "insert_qmt_edge_release_receipt",
    "load_existing_qmt_edge_release_request",
    "load_latest_qmt_edge_release_quiescence_hold",
    "load_qmt_edge_release_request",
    "qmt_edge_release_activation_run_uid",
    "qmt_edge_release_quiescence_run_uid",
    "qmt_edge_release_request_run_uid",
    "qmt_edge_release_run_uid",
    "validate_qmt_edge_release_activation_grant",
    "validate_qmt_edge_release_quiescence_hold",
    "validate_qmt_edge_release_receipt",
]
