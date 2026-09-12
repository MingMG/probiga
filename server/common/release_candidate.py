"""Candidate validation evidence, separate from permission to activate a release."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta
from typing import Any, Mapping

from sqlalchemy import text

SCHEMA = "probiga.windows-release-candidate.v1"
TRIGGER = "release_candidate"
TASK_TYPE = "qmt_edge_release_bootstrap"  # Existing append-only audit protection.
CHECKS = ("dependency_imports", "myquant_runtime", "powershell_preflight", "recovery_health")
MAX_AGE = timedelta(minutes=30)


class CandidateValidationError(ValueError):
    pass


def _hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                    ensure_ascii=True).encode()).hexdigest()


def validate_candidate(value: Mapping[str, Any], *, build_sha: str, tree_sha: str,
                       prior_build_sha: str, now: datetime) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise CandidateValidationError("candidate evidence must be an object")
    payload = dict(value)
    expected = {"schema", "build_sha", "tree_sha", "prior_build_sha", "host_name",
                "validated_at", "runtime_fingerprint", "qmt_client_pid",
                "model_instance_id", "checks", "activation_granted", "receipt_hash"}
    if set(payload) != expected or payload["schema"] != SCHEMA:
        raise CandidateValidationError("candidate evidence schema differs")
    for key, wanted in (("build_sha", build_sha), ("tree_sha", tree_sha),
                        ("prior_build_sha", prior_build_sha)):
        if not re.fullmatch(r"[0-9a-f]{40}", str(wanted)) or payload[key] != wanted:
            raise CandidateValidationError(f"candidate {key} differs")
    if payload["activation_granted"] is not False:
        raise CandidateValidationError("candidate evidence cannot grant activation")
    if payload["checks"] != {key: "PASS" for key in CHECKS}:
        raise CandidateValidationError("candidate checks have not all passed")
    if (type(payload["qmt_client_pid"]) is not int or payload["qmt_client_pid"] <= 0
            or not isinstance(payload["host_name"], str) or not payload["host_name"].strip()
            or not isinstance(payload["model_instance_id"], str)
            or not payload["model_instance_id"].strip()
            or not re.fullmatch(r"[0-9a-f]{64}", str(payload["runtime_fingerprint"]))):
        raise CandidateValidationError("candidate runtime identity is invalid")
    try:
        captured = datetime.fromisoformat(payload["validated_at"])
    except (TypeError, ValueError) as exc:
        raise CandidateValidationError("candidate timestamp is invalid") from exc
    if captured.tzinfo is not None or now.tzinfo is not None:
        raise CandidateValidationError("candidate timestamps must use Shanghai wall time")
    if not timedelta(0) <= now - captured <= MAX_AGE:
        raise CandidateValidationError("candidate evidence is stale or future-dated")
    unsigned = {key: val for key, val in payload.items() if key != "receipt_hash"}
    if payload["receipt_hash"] != _hash(unsigned):
        raise CandidateValidationError("candidate evidence content differs")
    return payload


def build_candidate(*, build_sha: str, tree_sha: str, prior_build_sha: str,
                    host_name: str, validated_at: datetime, runtime_fingerprint: str,
                    qmt_client_pid: int, model_instance_id: str) -> dict[str, Any]:
    payload = dict(schema=SCHEMA, build_sha=build_sha, tree_sha=tree_sha,
                   prior_build_sha=prior_build_sha, host_name=host_name,
                   validated_at=validated_at.isoformat(timespec="seconds"),
                   runtime_fingerprint=runtime_fingerprint, qmt_client_pid=qmt_client_pid,
                   model_instance_id=model_instance_id,
                   checks={key: "PASS" for key in CHECKS}, activation_granted=False)
    payload["receipt_hash"] = _hash(payload)
    return validate_candidate(payload, build_sha=build_sha, tree_sha=tree_sha,
                              prior_build_sha=prior_build_sha, now=validated_at)


def append_candidate(connection: Any, payload: Mapping[str, Any], *, now: datetime) -> None:
    from server.common.qmt_edge_release_receipt import _reference_task_id

    value = validate_candidate(payload, build_sha=payload["build_sha"],
                               tree_sha=payload["tree_sha"],
                               prior_build_sha=payload["prior_build_sha"], now=now)
    run_uid = "qmt-candidate-" + value["receipt_hash"][:48]
    params = dict(value, run_uid=run_uid, task_id=_reference_task_id(connection),
                  task_type=TASK_TYPE, trigger_source=TRIGGER,
                  output=json.dumps(value, sort_keys=True, separators=(",", ":")))
    existing = connection.execute(text(
        "SELECT output FROM st_scheduled_task_history WHERE run_uid=:run_uid"
    ), params).scalar_one_or_none()
    if existing is not None:
        if json.loads(existing) != value:
            raise CandidateValidationError("candidate audit replay differs")
        return
    connection.execute(text(
        "INSERT INTO st_scheduled_task_history "
        "(run_uid,task_id,task_name,task_type,run_at,finished_at,status,duration,"
        "exit_code,output,host_name,scheduler_instance_id,build_sha,trigger_source) "
        "VALUES (:run_uid,:task_id,'Windows candidate validation',:task_type,"
        ":validated_at,:validated_at,'success',0,0,:output,:host_name,:run_uid,"
        ":build_sha,:trigger_source)"
    ), params)


def read_candidate(connection: Any, *, build_sha: str, tree_sha: str,
                   prior_build_sha: str, now: datetime) -> dict[str, Any]:
    from server.common.qmt_edge_release_receipt import _reference_task_id
    # Preparation does not grant writer authority. An interrupted cutover can
    # revalidate while its prior scheduler is stopped; the existing protected
    # handoff still proves live/stopped writer identities before any cutover.
    hosts = connection.execute(text(
        "SELECT DISTINCT host_name FROM st_scheduler_runtime "
        "WHERE executor_role='qmt_windows_edge' AND build_sha=:prior_build_sha"
    ), {"prior_build_sha": prior_build_sha}).scalars().all()
    if len(hosts) != 1 or not hosts[0]:
        raise CandidateValidationError("prior Windows host is not unique")
    host = hosts[0]
    row = connection.execute(text(
        "SELECT run_uid,task_id,run_at,finished_at,output,scheduler_instance_id "
        "FROM st_scheduled_task_history WHERE task_type=:task_type "
        "AND trigger_source=:trigger_source AND build_sha=:build_sha "
        "AND host_name=:host_name AND status='success' AND exit_code=0 "
        "ORDER BY id DESC LIMIT 1"
    ), dict(task_type=TASK_TYPE, trigger_source=TRIGGER,
            build_sha=build_sha, host_name=host)).mappings().first()
    if row is None:
        raise CandidateValidationError("Windows candidate has not passed validation")
    try:
        payload = json.loads(row["output"])
    except (TypeError, ValueError) as exc:
        raise CandidateValidationError("candidate audit JSON is invalid") from exc
    value = validate_candidate(payload, build_sha=build_sha, tree_sha=tree_sha,
                               prior_build_sha=prior_build_sha, now=now)
    uid = "qmt-candidate-" + value["receipt_hash"][:48]
    if (row["task_id"] != _reference_task_id(connection)
            or row["run_uid"] != uid or row["scheduler_instance_id"] != uid
            or value["host_name"] != host):
        raise CandidateValidationError("candidate audit identity differs")
    for field in ("run_at", "finished_at"):
        if datetime.fromisoformat(str(row[field])) != datetime.fromisoformat(value["validated_at"]):
            raise CandidateValidationError("candidate audit timestamp differs")
    return value
