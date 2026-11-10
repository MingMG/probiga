"""Durable staging for the scheduled next-session strategy pool revision."""
from __future__ import annotations

import json
import math
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Mapping

from server.common.analysis_pool_receipt import PREMARKET_POOL_TASK, decode_score_snapshot
from server.common.authoritative_market_clock import (
    PRODUCTION_TIMEZONE, STRATEGY_POOL_PREMARKET_PUBLICATION_DEADLINE, StrategyPoolDecisionSlot,
)
from server.common.scheduler_terminal_journal import TerminalJournal, digest


PREMARKET_POOL_WORK_SCHEMA = "probiga.premarket-pool-work.v1"
PREMARKET_POOL_WORK_REF_SCHEMA = "probiga.premarket-pool-work-reference.v1"
WORK_RECORD = "FROZEN_INPUT"


def _json_value(value):
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("prepared strategy input has a non-finite number")
        return value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("prepared strategy input has a non-finite decimal")
        return str(value)
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise ValueError("prepared strategy input has a non-string key")
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    raise TypeError(f"prepared strategy input has an unsupported value type: {type(value).__name__}")


def _require_identity(run_uid, build_sha, trade_date, parent_receipt_uid, source_evidence_sha256):
    if (
        re.fullmatch(r"[0-9a-f]{32}", str(run_uid)) is None
        or re.fullmatch(r"[0-9a-f]{40}", str(build_sha)) is None
        or str(build_sha) == "0" * 40
        or date.fromisoformat(trade_date).isoformat() != trade_date
        or any(re.fullmatch(r"[0-9a-f]{64}", str(value)) is None
               for value in (parent_receipt_uid, source_evidence_sha256))
    ):
        raise ValueError("prepared strategy pool identity differs")


def stage_frozen_pool_work(
    journal: TerminalJournal, *, run_uid: str, build_sha: str,
    parent_receipt_uid: str, source_evidence_sha256: str, scoring: Mapping,
) -> dict:
    trade_date = scoring["data_trade_date"]
    _require_identity(run_uid, build_sha, trade_date, parent_receipt_uid, source_evidence_sha256)
    work = _json_value({
        "schema": PREMARKET_POOL_WORK_SCHEMA, "run_uid": run_uid, "build_sha": build_sha,
        "trade_date": trade_date, "parent_receipt_uid": parent_receipt_uid,
        "source_evidence_sha256": source_evidence_sha256,
        "scoring": scoring,
    })
    _validate_work(work)
    raw = journal.preserve(run_uid, WORK_RECORD, work)
    return {
        "schema": PREMARKET_POOL_WORK_REF_SCHEMA, "run_uid": run_uid, "build_sha": build_sha,
        "trade_date": trade_date, "sha256": digest(raw), "bytes": len(raw),
        "automatic_real_order_submission": False, "real_order_authority": False,
    }


def _validate_work(work: dict) -> dict:
    if set(work) != {"schema", "run_uid", "build_sha", "trade_date", "parent_receipt_uid",
                     "source_evidence_sha256", "scoring"} or work["schema"] != PREMARKET_POOL_WORK_SCHEMA:
        raise ValueError("prepared strategy pool record differs")
    _require_identity(work["run_uid"], work["build_sha"], work["trade_date"],
                      work["parent_receipt_uid"], work["source_evidence_sha256"])
    scoring = work["scoring"]
    snapshot = decode_score_snapshot(scoring["score_snapshot"], trade_date=work["trade_date"])
    cutoff = datetime.fromisoformat(scoring["cutoff_at"])
    evaluated = datetime.fromisoformat(scoring["evaluated_at"])
    if (
        scoring["phase"] != "PREMARKET_REVISION"
        or scoring["data_trade_date"] != work["trade_date"]
        or cutoff.utcoffset() is None or evaluated.utcoffset() is None or evaluated < cutoff
        or cutoff.date().isoformat() != scoring["execution_session_date"]
        or snapshot["run_uid"] != work["run_uid"] or snapshot["build_sha"] != work["build_sha"]
        or snapshot["decision_at"] != cutoff.replace(tzinfo=None).isoformat(timespec="seconds")
        or snapshot["input_decision_at"] != cutoff.replace(tzinfo=None).isoformat(timespec="seconds")
        or not isinstance(scoring["analysis_rows"], list)
        or not isinstance(scoring["recommendation_rows"], list)
    ):
        raise ValueError("prepared strategy pool score identity differs")
    return snapshot


def read_frozen_pool_work(
    journal: TerminalJournal, reference: Mapping, *, run_uid: str, build_sha: str, trade_date: str,
) -> dict:
    if (
        set(reference) != {"schema", "run_uid", "build_sha", "trade_date", "sha256", "bytes",
                           "automatic_real_order_submission", "real_order_authority"}
        or reference["schema"] != PREMARKET_POOL_WORK_REF_SCHEMA
        or reference["run_uid"] != run_uid or reference["build_sha"] != build_sha
        or reference["trade_date"] != trade_date
        or reference["automatic_real_order_submission"] is not False
        or reference["real_order_authority"] is not False
        or type(reference["bytes"]) is not int
    ):
        raise ValueError("prepared strategy pool reference differs")
    raw = journal.read(run_uid, WORK_RECORD)
    if not raw or len(raw) != reference["bytes"] or digest(raw) != reference["sha256"]:
        raise ValueError("prepared strategy pool work is unavailable or has changed")
    work = json.loads(raw)
    _validate_work(work)
    if any(work[key] != reference[key] for key in ("run_uid", "build_sha", "trade_date")):
        raise ValueError("prepared strategy pool sealed identity differs")
    return work


def require_premarket_publication_window(slot: StrategyPoolDecisionSlot) -> None:
    """Reject publication outside the next session's 08:30–09:25 interval."""
    now = datetime.now(PRODUCTION_TIMEZONE)
    if (
        slot.phase != "PREMARKET_REVISION"
        or now.date().isoformat() != slot.execution_session_date
        or not slot.scheduled_at <= slot.evaluated_at <= now
        or now.time() >= STRATEGY_POOL_PREMARKET_PUBLICATION_DEADLINE
    ):
        raise RuntimeError("PREMARKET_POOL_PUBLICATION_WINDOW_CLOSED")


def publish_prepared_premarket_pool(
    connection,
    *,
    run_uid: str,
    work: dict,
    slot: StrategyPoolDecisionSlot,
    external_context: Mapping | None,
    activate_analysis_pool,
) -> tuple[dict, dict, dict]:
    """Stage score, activation and governance inside the caller's writer transaction."""
    from biz.analysis.sync_analysis_fast import (
        _finish_direct_publication_history, _prebind_direct_publication_history, save_outputs,
    )
    from server.api.routers.trading_v3 import _governance_pool_revision_members
    from server.common.component_release import runtime_contract_build_sha
    from server.common.daily_delivery_control import (
        analysis_publication_degradations, bind_frozen_pool_decision, build_terminal_delivery_receipt,
        canonical_sha256, daily_session_identity, load_frozen_initial_delivery,
    )
    from server.engine.strategy_center import bind_external_market_overlay
    from server.engine.strategy_governance import (
        _require_governance_write_transaction, load_completed_governance_revision_pair,
    )
    from server.engine.strategy_governance_orchestrator import persist_frozen_governance_revision
    from server.trading_v3.pool_revision_diff import build_pool_revision_diff

    _require_governance_write_transaction(connection)
    require_premarket_publication_window(slot)
    snapshot = _validate_work(work)
    score = work["scoring"]
    target, build = work["trade_date"], work["build_sha"]
    if (
        work["run_uid"] != run_uid or slot.data_trade_date != target
        or score["execution_session_date"] != slot.execution_session_date
        or score["cutoff_at"] != slot.scheduled_at.isoformat(timespec="seconds")
        or score["evaluated_at"] != slot.evaluated_at.isoformat(timespec="seconds")
    ):
        raise ValueError("prepared premarket pool differs from its exact run or decision slot")
    core_inputs = score.get("core_inputs")
    market_context = snapshot.get("fixed_market_context")
    if (
        not isinstance(core_inputs, Mapping)
        or not isinstance(core_inputs.get("announcement"), Mapping)
        or core_inputs != snapshot.get("frozen_core_inputs")
        or core_inputs["announcement"].get("root") != work["source_evidence_sha256"]
        or not isinstance(market_context, dict)
    ):
        raise ValueError("prepared premarket pool source evidence differs")

    contract = runtime_contract_build_sha(expected_build_sha=build)
    identity = daily_session_identity(target, contract)
    initial = load_frozen_initial_delivery(connection, identity["session_uid"])
    if (
        initial is None or initial.get("receipt_uid") != work["parent_receipt_uid"]
        or initial["frozen_pool_decision"].get("execution_session_date") != slot.execution_session_date
        or re.fullmatch(r"[0-9a-f]{32}", str(initial.get("governance_run_uid") or "")) is None
        or re.fullmatch(r"[0-9a-f]{64}", str((initial.get("strategy_pool") or {}).get("root") or "")) is None
    ):
        raise RuntimeError("PREMARKET_INITIAL_POOL_UNAVAILABLE_OR_CHANGED")

    engine = connection.engine
    _prebind_direct_publication_history(
        engine, run_uid=run_uid, task_type=PREMARKET_POOL_TASK, build_sha=build,
        trade_date=target, min_score=62.0, top_n=80, strict_prev_trade_day=True,
        execution_time=score["cutoff_at"], publication_connection=connection,
    )
    publication = save_outputs(
        engine, score["analysis_rows"], score["recommendation_rows"], target,
        publication_run_uid=run_uid, publisher_task_type=PREMARKET_POOL_TASK,
        publisher_build_sha=build, score_snapshot=score["score_snapshot"],
        publication_connection=connection,
    )
    _finish_direct_publication_history(
        engine, run_uid=run_uid, success=True, publication_receipt=publication,
        publication_connection=connection,
    )
    activation = activate_analysis_pool(
        connection, run_uid=run_uid, task_type=PREMARKET_POOL_TASK,
        publication_receipt=publication, decision_known_at=slot.scheduled_at,
    )
    empty_recommendations = publication["recommendation_count"] == 0
    if (
        activation.get("status") != ("VERIFIED_EMPTY" if empty_recommendations else "VERIFIED_ACTIVE")
        or activation.get("target_trade_date") != target
        or any(activation.get(key) != publication[key] for key in (
            "run_uid", "publisher_task_type", "build_sha", "analysis_count",
            "recommendation_count", "executable_count", "canonical_pool_sha256",
        ))
    ):
        raise RuntimeError("premarket analysis activation differs from the staged publication")

    with bind_external_market_overlay(external_context):
        governance = persist_frozen_governance_revision(
            connection,
            fixed_scoring={"trade_date": target, "analysis_run_uid": run_uid,
                           "decision_known_at": score["cutoff_at"], "fixed_market_context": market_context},
            prepared_analysis_publication=publication,
            expected_prior_governance_run_uid=initial["governance_run_uid"], expected_build_sha=build,
        )
    prior, successor = load_completed_governance_revision_pair(initial["governance_run_uid"], governance["run_uid"])
    if (
        prior["canonical_result_hash"] != initial["strategy_pool"]["root"]
        or successor["canonical_result_hash"] != governance["canonical_result_hash"]
    ):
        raise RuntimeError("premarket governance revision pair differs from the sealed receipt")
    prior_identity, prior_rows = _governance_pool_revision_members(prior)
    next_identity, next_rows = _governance_pool_revision_members(successor)
    diff = build_pool_revision_diff(
        prior_identity=prior_identity, next_identity=next_identity, prior_rows=prior_rows,
        next_rows=next_rows, time_field="finished_at",
    )
    empty_governance = next_identity["stock_count"] == 0
    degradations = analysis_publication_degradations(publication)
    receipt = build_terminal_delivery_receipt(
        session=initial, scheduler_run_uid=run_uid, stage_name=PREMARKET_POOL_TASK,
        status="DEGRADED" if degradations else "PASS", strategy_release_id=initial["strategy_release_id"],
        legacy_receipt={
            "status": "VERIFIED_EMPTY" if empty_recommendations and empty_governance else "VERIFIED_DELIVERED",
            "target_trade_date": target,
            "base_data_receipt_root_sha256": canonical_sha256(core_inputs),
            "score_snapshot_sha256": publication["score_snapshot_sha256"],
            "analysis_run_uid": run_uid,
            "governance_run_uid": governance["run_uid"],
            "governance_result_sha256": governance["canonical_result_hash"],
            "governance_tradable_count": len(successor["pools"]["tradable"]),
            "governance_status": "COMPLETED",
            "strategy_pool_status": "EMPTY" if empty_governance else "ACTIVE",
            "ticket_pool_status": "EMPTY" if empty_recommendations else "ACTIVE",
            "recommendation_count": publication["recommendation_count"],
            "executable_count": publication["executable_count"],
            "canonical_pool_sha256": publication["canonical_pool_sha256"],
        },
        exact_core_inputs=core_inputs, degradations=degradations,
    )
    receipt = bind_frozen_pool_decision(
        receipt, slot=slot, score_snapshot=score["score_snapshot"],
        parent_receipt_uid=initial["receipt_uid"], diff_sha256=diff["diff_sha256"],
    )
    require_premarket_publication_window(slot)
    return publication, activation, receipt
