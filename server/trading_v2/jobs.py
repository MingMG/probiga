"""Asynchronous job creation and guarded strategy lifecycle transitions."""
from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .config import canonical_json_hash


LIFECYCLE_TRANSITIONS: dict[str, frozenset[str]] = {
    "DRAFT_BLOCKED": frozenset({"DRAFT"}),
    "DRAFT": frozenset({"RESEARCH"}),
    "RESEARCH": frozenset({"OOS_PASSED", "PAPER_TRIAL"}),
    "OOS_PASSED": frozenset({"SHADOW"}),
    "SHADOW": frozenset(
        {"PAPER_TRIAL", "PAPER_ACTIVE", "SUSPENDED"}
    ),
    "PAPER_TRIAL": frozenset({"PAPER_ACTIVE", "SUSPENDED"}),
    "PAPER_ACTIVE": frozenset({"SUSPENDED"}),
    "SUSPENDED": frozenset({"RETIRED"}),
    "RETIRED": frozenset(),
}


def enqueue_job(
    engine: Engine,
    *,
    job_type: str,
    request: dict[str, Any],
    requested_by: str,
) -> dict[str, Any]:
    key_payload: dict[str, Any] = {
        "job_type": job_type,
        "request": request,
    }
    if job_type == "BACKTEST":
        # A research request must not silently reuse a completed job after
        # its executable protocol or source artifact has changed.
        from .job_worker import RESEARCH_PROTOCOL_VERSION
        from .versioning import code_version

        key_payload["research_protocol_version"] = (
            RESEARCH_PROTOCOL_VERSION
        )
        key_payload["code_commit_sha"] = code_version()[0]
    key = canonical_json_hash(key_payload)
    with engine.begin() as connection:
        existing = connection.execute(
            text(
                """
                SELECT job_id, status, result_ref FROM st_job_v2
                WHERE idempotency_key = :key
                """
            ),
            {"key": key},
        ).mappings().first()
        if existing:
            return {
                "job_id": existing["job_id"],
                "status": existing["status"],
                "result_ref": existing["result_ref"],
                "idempotent_hit": True,
            }
        job_id = uuid.uuid4().hex
        connection.execute(
            text(
                """
                INSERT INTO st_job_v2
                (job_id, job_type, idempotency_key, request_json, status,
                 requested_by, requested_at)
                VALUES
                (:job_id, :job_type, :key, :request_json, 'PENDING',
                 :requested_by, :requested_at)
                """
            ),
            {
                "job_id": job_id,
                "job_type": job_type,
                "key": key,
                "request_json": json.dumps(
                    request,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ),
                "requested_by": requested_by,
                "requested_at": datetime.now(),
            },
        )
    return {
        "job_id": job_id,
        "status": "PENDING",
        "result_ref": None,
        "idempotent_hit": False,
    }


def transition_strategy(
    engine: Engine,
    *,
    strategy_id: str,
    strategy_version: str,
    next_status: str,
    reason: str,
    operator: str,
    validation_patch: dict[str, Any] | None = None,
) -> dict[str, Any]:
    next_status = next_status.upper()
    with engine.begin() as connection:
        row = connection.execute(
            text(
                """
                SELECT lifecycle_status, validation_json
                FROM st_strategy_version_v2
                WHERE strategy_id = :strategy_id AND version = :version
                FOR UPDATE
                """
            ),
            {"strategy_id": strategy_id, "version": strategy_version},
        ).mappings().first()
        if not row:
            raise ValueError("strategy version not found")
        previous = str(row["lifecycle_status"])
        if next_status not in LIFECYCLE_TRANSITIONS.get(previous, frozenset()):
            raise ValueError(f"illegal lifecycle transition: {previous} -> {next_status}")
        validation = json.loads(str(row["validation_json"] or "{}"))
        if not isinstance(validation, dict):
            validation = {}
        if validation_patch:
            validation.update(validation_patch)
        if next_status == "PAPER_TRIAL":
            if validation.get("paper_trial_authorized") is not True:
                raise ValueError(
                    "paper trial blocked: paper_trial_authorized is not true"
                )
            if validation.get("real_trading_enabled") is not False:
                raise ValueError(
                    "paper trial blocked: real_trading_enabled must be false"
                )
        if next_status in {"OOS_PASSED", "SHADOW", "PAPER_ACTIVE"}:
            required_flag = {
                "OOS_PASSED": "oos_gate_passed",
                "SHADOW": "shadow_gate_passed",
                "PAPER_ACTIVE": "paper_gate_passed",
            }[next_status]
            if validation.get(required_flag) is not True:
                raise ValueError(f"promotion blocked: {required_flag} is not true")
        occurred_at = datetime.now()
        event_payload = {
            "strategy_id": strategy_id,
            "strategy_version": strategy_version,
            "previous_status": previous,
            "next_status": next_status,
            "reason": reason,
            "operator": operator,
            "validation": validation,
            "occurred_at": occurred_at.isoformat(timespec="microseconds"),
        }
        event_hash = canonical_json_hash(event_payload)
        event_id = event_hash[:32]
        connection.execute(
            text(
                """
                INSERT INTO st_strategy_lifecycle_event_v2
                (event_id, strategy_id, strategy_version, previous_status,
                 next_status, reason, operator_name, evidence_json,
                 event_hash, occurred_at)
                VALUES
                (:event_id, :strategy_id, :strategy_version, :previous_status,
                 :next_status, :reason, :operator, :evidence,
                 :event_hash, :occurred_at)
                """
            ),
            {
                "event_id": event_id,
                "strategy_id": strategy_id,
                "strategy_version": strategy_version,
                "previous_status": previous,
                "next_status": next_status,
                "reason": reason[:500],
                "operator": operator[:80],
                "evidence": json.dumps(validation, ensure_ascii=False),
                "event_hash": event_hash,
                "occurred_at": occurred_at,
            },
        )
        connection.execute(
            text(
                """
                UPDATE st_strategy_version_v2
                SET lifecycle_status = :next_status,
                    validation_json = :validation,
                    promoted_at = CASE
                        WHEN :next_status IN
                             ('OOS_PASSED','SHADOW','PAPER_TRIAL','PAPER_ACTIVE')
                        THEN :occurred_at ELSE promoted_at END,
                    suspended_at = CASE
                        WHEN :next_status = 'SUSPENDED'
                        THEN :occurred_at ELSE suspended_at END
                WHERE strategy_id = :strategy_id AND version = :strategy_version
                """
            ),
            {
                "next_status": next_status,
                "validation": json.dumps(
                    validation,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ),
                "occurred_at": occurred_at,
                "strategy_id": strategy_id,
                "strategy_version": strategy_version,
            },
        )
    return {
        "event_id": event_id,
        "strategy_id": strategy_id,
        "strategy_version": strategy_version,
        "previous_status": previous,
        "next_status": next_status,
    }
