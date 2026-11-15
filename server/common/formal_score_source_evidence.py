"""Read the original, verified dependency runs of a sealed initial score."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import nullcontext
from datetime import date, datetime, timedelta
import json
import re
from typing import Any

from sqlalchemy import bindparam, text

from server.common.component_release_attestation import (
    load_component_attestation,
    require_compatible_component_build,
)
from server.common.daily_delivery_control import (
    _validated_completed_stage_checkpoint,
    daily_session_identity,
    select_frozen_initial_announcement_source,
)
from server.common.pit_facts import (
    FINANCE_ATOMIC_BATCH_CODE,
    FINANCE_ATOMIC_BATCH_SOURCE,
    normalize_decision_at,
)
from server.common.release_data_readiness_contract import DAILY_RESULT_RECOVERY_DEPENDENCIES


_TASK = "analysis_fast"
_ANNOUNCEMENT = "qmt_announcement_pit"
_FINANCE = "stock_finance"
_REQUIRED = tuple(sorted(DAILY_RESULT_RECOVERY_DEPENDENCIES[_TASK]))
_MAX_TERMINAL_ATTEMPTS = 32
_SHA40 = re.compile(r"[0-9a-f]{40}")
_SHA64 = re.compile(r"[0-9a-f]{64}")


def _finance_seal_is_durable(connection, checkpoint: Mapping[str, Any], *, target: str,
                             run_at: datetime, finished_at: datetime, stage_finished_at: datetime,
                             cutoff: datetime) -> bool:
    from server.api.scheduler_runtime import _iter_scheduler_output_payloads

    receipts = [
        item for item in _iter_scheduler_output_payloads(checkpoint["replay_output"])
        if item.get("schema") in {"probiga.finance-sync-result.v2", "probiga.finance-atomic-batch-result.v1"}
    ]
    if len(receipts) != 1:
        return False
    receipt = receipts[0]
    seal = receipt.get("atomic_batch") if receipt["schema"] == "probiga.finance-sync-result.v2" else receipt
    if not isinstance(seal, Mapping) or seal.get("as_of_date") != target:
        return False
    seal_id = str(seal.get("seal_coverage_id") or "")
    seal_root = str(seal.get("batch_root_sha256") or "")
    if not _SHA64.fullmatch(seal_id) or not _SHA64.fullmatch(seal_root):
        return False
    completed = normalize_decision_at(seal["completed_known_at"])
    if (
        not run_at - timedelta(minutes=5) <= completed <= cutoff
        or completed >= min(finished_at, stage_finished_at) + timedelta(seconds=1)
    ):
        return False
    hint = "/*+ MAX_EXECUTION_TIME(3000) */ " if connection.dialect.name == "mysql" else ""
    # The existing scheduler validator proved the complete seal. Read that
    # exact immutable primary-key row and its original fact root again.
    return connection.execute(text(f"""
        SELECT {hint}coverage_id FROM st_pit_source_coverage
         WHERE coverage_id=:coverage_id AND fact_kind='finance' AND source=:source
           AND stock_code=:stock_code AND coverage_status='COMPLETE'
           AND result_count=0 AND window_start='1900-01-01' AND window_end=:target
           AND known_at=:completed AND received_at=:completed AND covered_through_at=:completed
           AND batch_id=:batch_id
           AND JSON_EXTRACT(payload_json,'$.watermark.evidence.batch_root_sha256')=JSON_EXTRACT(:root_json,'$')
    """), {
        "coverage_id": seal_id, "source": FINANCE_ATOMIC_BATCH_SOURCE, "stock_code": FINANCE_ATOMIC_BATCH_CODE,
        "target": target, "completed": completed, "batch_id": f"finance-atomic-{seal_root[:32]}",
        "root_json": json.dumps(seal_root),
    }).scalar_one_or_none() == seal_id


def _terminal_inputs(connection, *, trade_date: str, contract_sha: str, cutoff: datetime,
                     announcement_uid: str) -> list[dict[str, Any]]:
    hint = "/*+ MAX_EXECUTION_TIME(3000) */ " if connection.dialect.name == "mysql" else ""
    scope = daily_session_identity(trade_date, contract_sha)
    # A compatible announcement proof can retain its original audit from a
    # previous coordinated release. Its exact UID is selected by the signed reader.
    sql = text(f"""
        SELECT {hint}h.id AS history_id,h.task_id,h.run_uid,h.task_type,h.run_at,h.finished_at,
                    h.status,h.exit_code,h.output,h.build_sha,
                    a.scheduler_run_uid,a.status AS stage_status,a.started_at AS stage_started_at,
                    a.finished_at AS stage_finished_at,a.input_root_sha256,a.checkpoint_json,
                    s.trade_date,s.release_id
          FROM st_daily_stage_attempt AS a
          JOIN st_daily_delivery_session AS s ON s.session_uid=a.session_uid
          JOIN st_scheduled_task_history AS h ON h.run_uid=a.scheduler_run_uid
         WHERE s.trade_date=:trade_date
           AND ((a.session_uid=:session_uid AND a.stage_name IN :standard_inputs)
                OR (a.stage_name=:announcement_task AND a.scheduler_run_uid=:announcement_uid))
           AND a.shard_id='main' AND h.task_type=a.stage_name
           AND a.status IN ('SUCCESS','DEGRADED') AND h.status IN ('success','degraded')
           AND h.exit_code=0 AND a.finished_at<=:cutoff AND h.finished_at<=:cutoff
         ORDER BY h.id DESC LIMIT :limit
    """).bindparams(bindparam("standard_inputs", expanding=True))
    rows = connection.execute(sql, {
        **scope, "standard_inputs": [task for task in _REQUIRED if task != _ANNOUNCEMENT],
        "announcement_task": _ANNOUNCEMENT, "announcement_uid": announcement_uid,
        "cutoff": cutoff, "limit": _MAX_TERMINAL_ATTEMPTS + 1,
    }).mappings().all()
    if len(rows) > _MAX_TERMINAL_ATTEMPTS:
        raise ValueError("original formal input history exceeds its bound")
    return [dict(row) for row in rows]


def frozen_initial_source_runs(
    engine, *, trade_date: str, decision_at: datetime, build_sha: str,
) -> dict[str, str]:
    """Reconstruct the five original UIDs as seen at the sealed score's cutoff."""
    from server.api.scheduler_runtime import (
        _coerce_datetime,
        _history_validation_evidence,
        _task_component_role,
        evaluate_immutable_daily_dependency_histories,
    )

    if (
        date.fromisoformat(trade_date).isoformat() != trade_date
        or not _SHA40.fullmatch(build_sha) or build_sha == "0" * 40
    ):
        raise ValueError("invalid original formal input build or trade date")
    cutoff = normalize_decision_at(decision_at)
    with (nullcontext(engine) if hasattr(engine, "execute") else engine.connect()) as connection:
        original = load_component_attestation(connection, build_sha)
        if original["linux_build_sha"] != build_sha:
            raise ValueError("original formal input Linux component differs")
        builds = {"linux": original["linux_build_sha"], "windows": original["windows_build_sha"]}
        contract = original["contract_build_sha"]
        announcement = select_frozen_initial_announcement_source(
            connection, trade_date=trade_date, decision_at=cutoff, consumer_release_id=contract,
        )
        if announcement is None:
            raise ValueError("original formal input signed announcement evidence is unavailable")
        bound = announcement.score_binding
        rows = _terminal_inputs(
            connection, trade_date=trade_date, contract_sha=contract,
            cutoff=cutoff, announcement_uid=str(bound["scheduler_run_uid"]),
        )
        selected: dict[str, dict[str, Any]] = {}
        for row in rows:
            task = row["task_type"]
            if task in selected:
                continue
            producer = row["build_sha"]
            if task not in {_FINANCE, _ANNOUNCEMENT} and producer != builds[_task_component_role(task)]:
                continue
            checkpoint = _validated_completed_stage_checkpoint(
                {**row, "status": row["stage_status"]}, session=row, stage_name=task, engine=connection,
            )
            evidence = _history_validation_evidence(row["output"])
            producer_identity = load_component_attestation(connection, producer)
            if (
                evidence is None or evidence != checkpoint
                or evidence["task_id"] != row["task_id"] or evidence["build_sha"] != producer
                or producer_identity[f"{_task_component_role(task)}_build_sha"] != producer
                or producer_identity["contract_build_sha"] != row["release_id"]
                or not _coerce_datetime(row["stage_started_at"]) <= _coerce_datetime(row["stage_finished_at"]) <= cutoff
            ):
                raise ValueError(f"original formal input terminal evidence differs for {task}")
            if task == _ANNOUNCEMENT and (
                evidence["evidence_sha256"] != bound["evidence_sha256"]
                or row["run_uid"] != bound["scheduler_run_uid"]
                or row["build_sha"] != bound["producer_build_sha"]
                or row["release_id"] != bound["contract_release_id"]
                or evidence["input_receipt_root_sha256"] != bound["input_receipt_root_sha256"]
            ):
                raise ValueError("original formal input announcement audit differs")
            if task == _FINANCE:
                if producer != build_sha:
                    require_compatible_component_build(connection, producer, build_sha)
                if not _finance_seal_is_durable(
                    connection, checkpoint, target=trade_date, run_at=_coerce_datetime(row["run_at"]),
                    finished_at=_coerce_datetime(row["finished_at"]),
                    stage_finished_at=_coerce_datetime(row["stage_finished_at"]), cutoff=cutoff,
                ):
                    raise ValueError("original formal input finance seal is unavailable")
            selected[task] = row

    valid, reason = evaluate_immutable_daily_dependency_histories(
        _TASK, list(selected.values()), now=cutoff, expected_trade_date=trade_date,
        expected_build_sha=build_sha, component_build_shas=builds,
        completed_stock_finance_history=selected.get(_FINANCE),
        certified_initial_announcement_history=selected.get(_ANNOUNCEMENT),
    )
    if not valid or set(selected) != set(_REQUIRED):
        raise ValueError(f"original formal input terminal histories are unavailable: {reason}")
    return {task: str(selected[task]["run_uid"]) for task in _REQUIRED}
