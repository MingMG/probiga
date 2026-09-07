"""Narrow pre-cutover recovery over the existing protected release ledger.

An abort occupies the SAME unique terminal slot as the v1 grant, but has a
different schema. Old readers/writers reject it; it is never a fake grant.
No timeout, heartbeat expiry, or client-provided boolean authorizes a writer.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import json
import os
from typing import Any, Iterator, Mapping

from sqlalchemy import text

from server.common import qmt_edge_release_receipt as ledger
from server.common.qmt_attestation_contract import canonical_digest


PROTOCOL = "probiga.qmt-edge-precutover-recovery.v1"
CONTEXT_SCHEMA = "probiga.qmt-edge-precutover-context.v1"
ABORT_SCHEMA = "probiga.qmt-edge-precutover-abort.v1"
FORWARD_PROTOCOL = "probiga.qmt-edge-forward-only-supersession.v1"
FORWARD_CONTEXT_SCHEMA = FORWARD_PROTOCOL
FORWARD_SCOPE = "FORWARD_ONLY_SUPERSESSION"
MAX_FORWARD_SUPERSESSION_DEPTH = 32
CONTEXT_TASK_NAME = "QMT edge protected pre-cutover context"
ABORT_TASK_NAME = "QMT edge authorized pre-cutover abort"
CONTROL_LOCK = "probiga.qmt-edge-release-control.v1"


@contextmanager
def release_control_connection(engine: Any) -> Iterator[Any]:
    """Serialize all hold/grant/abort writers on one physical MySQL session."""
    with engine.connect() as connection:
        mysql = connection.dialect.name == "mysql"
        if not mysql and os.environ.get("PROBIGA_DEPLOYMENT_MODE") == "production":
            raise ledger.QmtEdgeReleaseReceiptError("release control requires MySQL")
        acquired = False
        try:
            if mysql:
                acquired = connection.execute(
                    text("SELECT GET_LOCK(:name, 10)"), {"name": CONTROL_LOCK}
                ).scalar_one() == 1
                connection.commit()
                if not acquired:
                    raise ledger.QmtEdgeReleaseReceiptError("release control lock unavailable")
            with connection.begin():
                yield connection
        finally:
            if acquired:
                if connection.in_transaction():
                    connection.rollback()
                released = connection.execute(
                    text("SELECT RELEASE_LOCK(:name)"), {"name": CONTROL_LOCK}
                ).scalar_one()
                connection.commit()
                if released != 1:
                    raise ledger.QmtEdgeReleaseReceiptError("release control lock ownership lost")


def _payload(value: Mapping[str, Any] | str) -> dict[str, Any]:
    try:
        result = json.loads(value) if isinstance(value, str) else dict(value)
    except (ValueError, TypeError) as exc:
        raise ledger.QmtEdgeReleaseReceiptError("recovery payload malformed") from exc
    if type(result) is not dict:
        raise ledger.QmtEdgeReleaseReceiptError("recovery payload is not an object")
    return result


def context_uid(attempt_id: str) -> str:
    return "qmt-edge-context-" + ledger._deployment_attempt_id(attempt_id)


def build_context(
    *, hold: Mapping[str, Any], prior_build_sha: str,
    prior_host_name: str, prior_pid: int, prior_instance_id: str,
    prior_seal_hash: str, captured_at: Any,
) -> dict[str, Any]:
    hold = ledger.validate_qmt_edge_release_quiescence_hold(
        hold, expected_build_sha=str(hold.get("build_sha") or "")
    )
    prior = ledger._build_sha(prior_build_sha)
    if (
        not isinstance(prior_host_name, str) or not prior_host_name
        or len(prior_host_name) > 128 or type(prior_pid) is not int or prior_pid <= 0
        or prior_instance_id != f"{prior_host_name}-{prior_pid}"
        or prior == hold["build_sha"]
    ):
        raise ledger.QmtEdgeReleaseReceiptError("prior live Windows identity differs")
    body = {
        "schema": CONTEXT_SCHEMA, "protocol": PROTOCOL,
        "build_sha": hold["build_sha"],
        "deployment_attempt_id": hold["deployment_attempt_id"],
        "context_run_uid": context_uid(hold["deployment_attempt_id"]),
        "hold_run_uid": hold["hold_run_uid"], "hold_hash": hold["hold_hash"],
        "prior_build_sha": prior, "prior_host_name": prior_host_name,
        "prior_pid": prior_pid, "prior_instance_id": prior_instance_id,
        # This protocol is deliberately limited to a freshly observed running
        # edge. A disabled/stopped/unknown prior edge needs explicit bootstrap.
        "prior_running": True,
        "prior_seal_hash": ledger._sha256(prior_seal_hash, label="prior seal"),
        "captured_at": ledger._iso_datetime(captured_at), "real_order": False,
    }
    if body["captured_at"] != hold["requested_at"]:
        raise ledger.QmtEdgeReleaseReceiptError("recovery context must be atomic with hold")
    return {**body, "context_hash": canonical_digest(body)}


def validate_context(value: Mapping[str, Any] | str, *, hold: Mapping[str, Any]) -> dict[str, Any]:
    payload = _payload(value)
    if payload.get("prior_running") is not True or payload.get("real_order") is not False:
        raise ledger.QmtEdgeReleaseReceiptError("recovery context boolean types differ")
    rebuilt = build_context(
        hold=hold, prior_build_sha=payload.get("prior_build_sha"),
        prior_host_name=payload.get("prior_host_name"),
        prior_pid=payload.get("prior_pid"), prior_instance_id=payload.get("prior_instance_id"),
        prior_seal_hash=payload.get("prior_seal_hash"), captured_at=payload.get("captured_at"),
    )
    if payload != rebuilt:
        raise ledger.QmtEdgeReleaseReceiptError("recovery context content differs")
    return rebuilt


def build_forward_context(
    *, hold: Mapping[str, Any], superseded_hold: Mapping[str, Any],
    superseded_context: Mapping[str, Any], superseded_at: Any,
) -> dict[str, Any]:
    """Bind a new stopped target to one exact, still-pending protected hold."""
    hold = ledger.validate_qmt_edge_release_quiescence_hold(
        hold, expected_build_sha=str(hold.get("build_sha") or "")
    )
    superseded_hold = ledger.validate_qmt_edge_release_quiescence_hold(
        superseded_hold,
        expected_build_sha=str(superseded_hold.get("build_sha") or ""),
    )
    if (
        superseded_context.get("build_sha") != superseded_hold["build_sha"]
        or superseded_context.get("deployment_attempt_id")
        != superseded_hold["deployment_attempt_id"]
        or superseded_context.get("hold_run_uid") != superseded_hold["hold_run_uid"]
        or superseded_context.get("hold_hash") != superseded_hold["hold_hash"]
    ):
        raise ledger.QmtEdgeReleaseReceiptError("superseded context hold binding differs")
    if superseded_context.get("schema") == CONTEXT_SCHEMA:
        original = {
            "build_sha": superseded_context["prior_build_sha"],
            "host_name": superseded_context["prior_host_name"],
            "pid": superseded_context["prior_pid"],
            "instance_id": superseded_context["prior_instance_id"],
            "seal_hash": superseded_context["prior_seal_hash"],
        }
        depth = 1
    elif superseded_context.get("schema") == FORWARD_CONTEXT_SCHEMA:
        original = {
            "build_sha": superseded_context["original_prior_build_sha"],
            "host_name": superseded_context["original_prior_host_name"],
            "pid": superseded_context["original_prior_pid"],
            "instance_id": superseded_context["original_prior_instance_id"],
            "seal_hash": superseded_context["original_prior_seal_hash"],
        }
        depth = superseded_context["supersession_depth"] + 1
    else:
        raise ledger.QmtEdgeReleaseReceiptError("superseded recovery context schema differs")
    if (
        depth > MAX_FORWARD_SUPERSESSION_DEPTH
        or hold["deployment_attempt_id"] == superseded_hold["deployment_attempt_id"]
        or hold["build_sha"] in (superseded_hold["build_sha"], original["build_sha"])
    ):
        raise ledger.QmtEdgeReleaseReceiptError("forward supersession repeats protected identity")
    body = {
        "schema": FORWARD_CONTEXT_SCHEMA,
        "protocol": FORWARD_PROTOCOL,
        "scope": FORWARD_SCOPE,
        "build_sha": hold["build_sha"],
        "deployment_attempt_id": hold["deployment_attempt_id"],
        "context_run_uid": context_uid(hold["deployment_attempt_id"]),
        "hold_run_uid": hold["hold_run_uid"],
        "hold_hash": hold["hold_hash"],
        "supersedes_build_sha": superseded_hold["build_sha"],
        "supersedes_deployment_attempt_id": superseded_hold["deployment_attempt_id"],
        "supersedes_hold_run_uid": superseded_hold["hold_run_uid"],
        "supersedes_hold_hash": superseded_hold["hold_hash"],
        "supersedes_context_run_uid": superseded_context["context_run_uid"],
        "supersedes_context_hash": superseded_context["context_hash"],
        "original_prior_build_sha": original["build_sha"],
        "original_prior_host_name": original["host_name"],
        "original_prior_pid": original["pid"],
        "original_prior_instance_id": original["instance_id"],
        "original_prior_seal_hash": original["seal_hash"],
        "supersession_depth": depth,
        "superseded_at": ledger._iso_datetime(superseded_at),
        "real_order": False,
    }
    if body["superseded_at"] != hold["requested_at"]:
        raise ledger.QmtEdgeReleaseReceiptError("forward context must be atomic with hold")
    if body["superseded_at"] < superseded_hold["requested_at"]:
        raise ledger.QmtEdgeReleaseReceiptError("forward context predates superseded hold")
    return {**body, "context_hash": canonical_digest(body)}


def validate_forward_context(
    value: Mapping[str, Any] | str, *, hold: Mapping[str, Any],
    superseded_hold: Mapping[str, Any], superseded_context: Mapping[str, Any],
) -> dict[str, Any]:
    payload = _payload(value)
    if type(payload.get("supersession_depth")) is not int or payload.get("real_order") is not False:
        raise ledger.QmtEdgeReleaseReceiptError("forward context scalar types differ")
    rebuilt = build_forward_context(
        hold=hold, superseded_hold=superseded_hold,
        superseded_context=superseded_context,
        superseded_at=payload.get("superseded_at"),
    )
    if payload != rebuilt:
        raise ledger.QmtEdgeReleaseReceiptError("forward context content differs")
    return rebuilt


def build_abort(*, context: Mapping[str, Any], aborted_at: Any) -> dict[str, Any]:
    body = {
        "schema": ABORT_SCHEMA, "protocol": PROTOCOL,
        "build_sha": context["build_sha"],
        "deployment_attempt_id": context["deployment_attempt_id"],
        "terminal_run_uid": ledger.qmt_edge_release_activation_run_uid(context["deployment_attempt_id"]),
        "hold_run_uid": context["hold_run_uid"], "hold_hash": context["hold_hash"],
        "context_run_uid": context["context_run_uid"], "context_hash": context["context_hash"],
        "resume_build_sha": context["prior_build_sha"],
        "prior_seal_hash": context["prior_seal_hash"],
        "aborted_at": ledger._iso_datetime(aborted_at),
        "scope": "PRE_CUTOVER_UNCHANGED_SCHEMA", "real_order": False,
    }
    if body["aborted_at"] < context["captured_at"]:
        raise ledger.QmtEdgeReleaseReceiptError("abort predates recovery context")
    return {**body, "abort_hash": canonical_digest(body)}


def _validate_row(connection: Any, row: Mapping[str, Any], payload: Mapping[str, Any], *, kind: str, expected_task_id: int | None = None) -> None:
    is_context = kind == "context"
    ledger._validate_release_control_ledger_row(
        row, payload=payload,
        run_uid=payload["context_run_uid" if is_context else "terminal_run_uid"],
        expected_task_id=expected_task_id if expected_task_id is not None else ledger._reference_task_id(connection),
        task_name=CONTEXT_TASK_NAME if is_context else ABORT_TASK_NAME,
        status="success", event_at=payload[
            ("superseded_at" if payload.get("schema") == FORWARD_CONTEXT_SCHEMA else "captured_at")
            if is_context else "aborted_at"
        ],
        exit_code=0, scheduler_instance_id=payload["deployment_attempt_id"],
        trigger_source=ledger.QMT_EDGE_RELEASE_ACTIVATION_TRIGGER_SOURCE,
        error="protected recovery ledger row differs",
    )


def _row(connection: Any, uid: str) -> Mapping[str, Any] | None:
    rows = connection.execute(text(
        f"SELECT {ledger._RELEASE_CONTROL_LEDGER_SELECT} FROM st_scheduled_task_history "
        "WHERE run_uid=:uid"
    ), {"uid": uid}).mappings().all()
    if len(rows) > 1:
        raise ledger.QmtEdgeReleaseReceiptError("recovery ledger replay differs")
    return rows[0] if rows else None


def _insert(connection: Any, payload: Mapping[str, Any], *, kind: str) -> dict[str, Any]:
    is_context = kind == "context"
    uid = payload["context_run_uid" if is_context else "terminal_run_uid"]
    existing = _row(connection, uid)
    if existing is not None:
        _validate_row(connection, existing, payload, kind=kind)
        return {"status": "idempotent", **payload}
    timestamp = payload[
        ("superseded_at" if payload.get("schema") == FORWARD_CONTEXT_SCHEMA else "captured_at")
        if is_context else "aborted_at"
    ]
    connection.execute(text(
        "INSERT INTO st_scheduled_task_history (run_uid, task_id, task_name, task_type, "
        "run_at, finished_at, status, duration, exit_code, output, host_name, "
        "scheduler_instance_id, build_sha, trigger_source) VALUES "
        "(:uid,:task_id,:task_name,:task_type,:at,:at,'success',0,0,:output,"
        "'linux-release',:attempt,:build,:trigger)"
    ), {
        "uid": uid, "task_id": ledger._reference_task_id(connection),
        "task_name": CONTEXT_TASK_NAME if is_context else ABORT_TASK_NAME,
        "task_type": ledger.QMT_EDGE_RELEASE_REQUEST_TASK_TYPE,
        "at": timestamp.replace("T", " "), "output": ledger._serialized_payload(payload),
        "attempt": payload["deployment_attempt_id"], "build": payload["build_sha"],
        "trigger": ledger.QMT_EDGE_RELEASE_ACTIVATION_TRIGGER_SOURCE,
    })
    _validate_row(connection, _row(connection, uid), payload, kind=kind)
    return {"status": "inserted", **payload}


def latest_hold(connection: Any, *, expected_task_id: int | None = None) -> dict[str, Any] | None:
    rows = connection.execute(text(
        f"SELECT {ledger._RELEASE_CONTROL_LEDGER_SELECT} FROM st_scheduled_task_history "
        "WHERE task_type=:task_type AND trigger_source=:trigger ORDER BY id DESC LIMIT 1"
    ), {
        "task_type": ledger.QMT_EDGE_RELEASE_REQUEST_TASK_TYPE,
        "trigger": ledger.QMT_EDGE_RELEASE_QUIESCENCE_TRIGGER_SOURCE,
    }).mappings().all()
    if not rows:
        return None
    return ledger._validated_release_quiescence_row(
        rows[0], expected_build_sha=rows[0]["build_sha"],
        expected_task_id=expected_task_id if expected_task_id is not None else ledger._reference_task_id(connection),
    )


def _hold_by_uid(
    connection: Any, uid: str, *, expected_task_id: int | None = None,
) -> dict[str, Any]:
    row = _row(connection, uid)
    if row is None:
        raise ledger.QmtEdgeReleaseReceiptError("superseded hold is missing")
    return ledger._validated_release_quiescence_row(
        row, expected_build_sha=str(row.get("build_sha") or ""),
        expected_task_id=(expected_task_id if expected_task_id is not None
                          else ledger._reference_task_id(connection)),
    )


def load_context(
    connection: Any, hold: Mapping[str, Any], *, expected_task_id: int | None = None,
    _visited: set[str] | None = None,
) -> dict[str, Any] | None:
    row = _row(connection, context_uid(hold["deployment_attempt_id"]))
    if row is None:
        return None
    raw = _payload(row["output"])
    if raw.get("schema") == CONTEXT_SCHEMA:
        payload = validate_context(raw, hold=hold)
    elif raw.get("schema") == FORWARD_CONTEXT_SCHEMA:
        visited = set() if _visited is None else set(_visited)
        current_uid = context_uid(hold["deployment_attempt_id"])
        if current_uid in visited or len(visited) >= MAX_FORWARD_SUPERSESSION_DEPTH:
            raise ledger.QmtEdgeReleaseReceiptError("forward context chain cycles or is too deep")
        visited.add(current_uid)
        previous_hold = _hold_by_uid(
            connection, str(raw.get("supersedes_hold_run_uid") or ""),
            expected_task_id=expected_task_id,
        )
        previous_context = load_context(
            connection, previous_hold, expected_task_id=expected_task_id,
            _visited=visited,
        )
        if previous_context is None:
            raise ledger.QmtEdgeReleaseReceiptError("superseded protected context is missing")
        payload = validate_forward_context(
            raw, hold=hold, superseded_hold=previous_hold,
            superseded_context=previous_context,
        )
    else:
        raise ledger.QmtEdgeReleaseReceiptError("recovery context schema differs")
    _validate_row(connection, row, payload, kind="context", expected_task_id=expected_task_id)
    return payload


def load_context_chain(
    connection: Any, hold: Mapping[str, Any], *, expected_task_id: int | None = None,
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Return latest-to-root contexts after validating every immutable link."""
    chain: list[tuple[dict[str, Any], dict[str, Any]]] = []
    seen: set[str] = set()
    current_hold = dict(hold)
    while True:
        context = load_context(connection, current_hold, expected_task_id=expected_task_id)
        if context is None:
            raise ledger.QmtEdgeReleaseReceiptError("protected context chain is incomplete")
        uid = context["context_run_uid"]
        # At depth 32 the validated chain contains 32 forward edges plus its
        # v1 root context, so 33 nodes remain readable.
        if uid in seen or len(chain) > MAX_FORWARD_SUPERSESSION_DEPTH:
            raise ledger.QmtEdgeReleaseReceiptError("forward context chain cycles or is too deep")
        seen.add(uid)
        chain.append((current_hold, context))
        if context["schema"] == CONTEXT_SCHEMA:
            return chain
        current_hold = _hold_by_uid(
            connection, context["supersedes_hold_run_uid"],
            expected_task_id=expected_task_id,
        )


def load_abort(connection: Any, context: Mapping[str, Any], *, expected_task_id: int | None = None) -> dict[str, Any] | None:
    row = _row(connection, ledger.qmt_edge_release_activation_run_uid(context["deployment_attempt_id"]))
    if row is None:
        return None
    payload = _payload(row["output"])
    if payload.get("schema") != ABORT_SCHEMA:
        return None
    if context.get("schema") == FORWARD_CONTEXT_SCHEMA:
        raise ledger.QmtEdgeReleaseReceiptError("forward context cannot be pre-cutover aborted")
    if payload.get("real_order") is not False:
        raise ledger.QmtEdgeReleaseReceiptError("abort boolean types differ")
    rebuilt = build_abort(context=context, aborted_at=payload.get("aborted_at"))
    if payload != rebuilt:
        raise ledger.QmtEdgeReleaseReceiptError("abort content differs")
    _validate_row(connection, row, payload, kind="abort", expected_task_id=expected_task_id)
    return payload


def seal_identity_hash(seal: Mapping[str, Any]) -> str:
    """Bind stable sealed database facts, not client-specific TLS telemetry."""
    keys = (
        "attested_build_sha", "trigger_inventory_server_uuid",
        "trigger_inventory_contract_hash", "trigger_inventory_table_comment",
    )
    if any(not isinstance(seal.get(key), str) or not seal[key] for key in keys):
        raise ledger.QmtEdgeReleaseReceiptError("prior trigger seal identity incomplete")
    return canonical_digest({key: seal[key] for key in keys})


def has_protected_context(connection: Any) -> bool:
    return connection.execute(text(
        "SELECT id FROM st_scheduled_task_history WHERE task_type=:task_type "
        "AND trigger_source=:trigger AND task_name=:name LIMIT 1"
    ), {"task_type": ledger.QMT_EDGE_RELEASE_REQUEST_TASK_TYPE,
        "trigger": ledger.QMT_EDGE_RELEASE_ACTIVATION_TRIGGER_SOURCE,
        "name": CONTEXT_TASK_NAME}).first() is not None


def writer_allowed_by_latest_context(connection: Any, *, build_sha: str, seal: Mapping[str, Any], expected_task_id: int) -> bool:
    """Additional fence; returning True does not replace any v1 grant checks."""
    hold = latest_hold(connection, expected_task_id=expected_task_id)
    if hold is None:
        return True
    context = load_context(connection, hold, expected_task_id=expected_task_id)
    if context is None:
        # Legacy bootstrap is permitted only before any protected context was
        # installed. A later legacy hold cannot revive a superseded writer.
        return not has_protected_context(connection)
    if context["schema"] == FORWARD_CONTEXT_SCHEMA:
        # Forward supersession cannot revive either the original prior or any
        # superseded candidate. The target still needs the normal exact grant.
        return context["build_sha"] == build_sha
    if context["build_sha"] == build_sha:
        return load_abort(connection, context, expected_task_id=expected_task_id) is None
    abort = load_abort(connection, context, expected_task_id=expected_task_id)
    return bool(
        abort is not None and abort["resume_build_sha"] == build_sha
        and abort["prior_seal_hash"] == seal_identity_hash(seal)
    )


def insert_context(connection: Any, context: Mapping[str, Any]) -> dict[str, Any]:
    hold = latest_hold(connection)
    if hold is None or hold["deployment_attempt_id"] != context["deployment_attempt_id"]:
        raise ledger.QmtEdgeReleaseReceiptError("recovery context is not globally latest")
    return _insert(connection, validate_context(context, hold=hold), kind="context")


def insert_forward_context(connection: Any, context: Mapping[str, Any]) -> dict[str, Any]:
    hold = latest_hold(connection)
    if hold is None or hold["deployment_attempt_id"] != context.get("deployment_attempt_id"):
        raise ledger.QmtEdgeReleaseReceiptError("forward context is not globally latest")
    previous_hold = _hold_by_uid(connection, str(context.get("supersedes_hold_run_uid") or ""))
    previous_context = load_context(connection, previous_hold)
    if previous_context is None:
        raise ledger.QmtEdgeReleaseReceiptError("superseded protected context is missing")
    return _insert(connection, validate_forward_context(
        context, hold=hold, superseded_hold=previous_hold,
        superseded_context=previous_context,
    ), kind="context")


def insert_abort(connection: Any, *, expected_attempt: str, prior_build_sha: str, prior_seal_hash: str, now: datetime) -> dict[str, Any]:
    hold = latest_hold(connection)
    if hold is None or hold["deployment_attempt_id"] != ledger._deployment_attempt_id(expected_attempt):
        raise ledger.QmtEdgeReleaseReceiptError("RECOVERY_BLOCKED: attempt is not globally latest")
    context = load_context(connection, hold)
    if (
        context is None or context.get("schema") != CONTEXT_SCHEMA
        or context["prior_build_sha"] != prior_build_sha
        or context["prior_seal_hash"] != prior_seal_hash
    ):
        raise ledger.QmtEdgeReleaseReceiptError("RECOVERY_BLOCKED: prior identity/schema changed or legacy hold")
    existing = load_abort(connection, context)
    payload = existing or build_abort(context=context, aborted_at=now)
    return _insert(connection, payload, kind="abort")
