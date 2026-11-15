"""Durable results and atomic publication for the initial formal strategy pool."""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Mapping

from server.common.analysis_pool_receipt import (
    DURABLE_SCORE_SNAPSHOT_ENCODING,
    _read_score_manifest,
    build_durable_score_snapshot,
    decode_score_snapshot,
    is_executable_recommendation,
    rebind_durable_score_snapshot,
)
from server.common.daily_delivery_control import canonical_sha256
from server.common.formal_analysis_store import (
    FormalAnalysisArtifactIntegrityError,
    FormalAnalysisArtifactMissing,
    FormalAnalysisConflict,
    FormalAnalysisStore,
)
from server.common.frozen_strategy_pool_work import _json_value


FORMAL_SCORE_WORK_SCHEMA = "probiga.formal-initial-score-reference.v1"
FORMAL_SCORE_BLOCKED_SCHEMA = "probiga.formal-initial-score-blocked.v1"
WORK_STAGE = "initial_result"
WORK_KEY = "selection"


class FormalScoreContractError(ValueError):
    """The sealed input or score is invalid and retrying the same data cannot fix it."""


def formal_score_blocked(error: Exception) -> dict | None:
    if not isinstance(error, (
        FormalScoreContractError, FormalAnalysisArtifactIntegrityError,
        FormalAnalysisArtifactMissing, FormalAnalysisConflict,
    )):
        return None
    return {
        "schema": FORMAL_SCORE_BLOCKED_SCHEMA, "status": "blocked",
        "blocking_stage": "analysis_fast", "retryable": False,
        "reason_code": "FORMAL_SCORE_CONTRACT_INVALID", "error_detail": str(error)[:500],
    }


def seal_initial_selection(
    store: FormalAnalysisStore, *, generation: str, trade_date: str,
    decision_at: str, run_uid: str, build_sha: str, analysis_rows: list,
    recommendation_rows: list, scored, market_mood_score: float, flow_date: str,
    hot_date: str,
) -> dict:
    try:
        snapshot = build_durable_score_snapshot(
            store=store, generation_sha256=generation, trade_date=trade_date,
            decision_at=decision_at, run_uid=run_uid, build_sha=build_sha,
            analysis_rows=analysis_rows, scored_rows=scored.to_dict("records"),
            fixed_market_context=scored.attrs.get("fixed_market_context"),
            frozen_announcement_source=scored.attrs.get("frozen_announcement_source"),
            frozen_core_inputs=scored.attrs.get("frozen_core_inputs"),
        )
        store.put(generation, WORK_STAGE, WORK_KEY, _json_value({
            "score_snapshot": snapshot,
            "recommendation_rows": recommendation_rows,
            "market_mood_score": market_mood_score,
            "flow_date": flow_date,
            "hot_date": hot_date,
        }))
    except (TypeError, ValueError) as exc:
        raise FormalScoreContractError(f"score result could not be sealed: {exc}") from exc
    return load_initial_selection(store, generation=generation, run_uid=run_uid, build_sha=build_sha)


def load_initial_selection(
    store: FormalAnalysisStore, *, generation: str, run_uid: str, build_sha: str,
) -> dict | None:
    selection = store.get(generation, WORK_STAGE, WORK_KEY)
    if selection is None:
        return None
    snapshot = rebind_durable_score_snapshot(
        selection["score_snapshot"], store=store, run_uid=run_uid, build_sha=build_sha,
    )
    recommendations = selection["recommendation_rows"]
    core = {
        "schema": FORMAL_SCORE_WORK_SCHEMA,
        "generation_sha256": generation,
        "selection_sha256": canonical_sha256(selection),
        "run_uid": run_uid,
        "build_sha": build_sha,
        "trade_date": snapshot["trade_date"],
        "decision_at": snapshot["decision_at"],
        "score_snapshot": snapshot,
        "analysis_count": snapshot["stock_count"],
        "recommendation_count": len(recommendations),
        "executable_count": sum(is_executable_recommendation(row) for row in recommendations),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    return {
        "reference": {**core, "reference_sha256": canonical_sha256(core)},
        **{key: selection[key] for key in ("market_mood_score", "flow_date", "hot_date")},
    }


def verify_initial_selection(
    store: FormalAnalysisStore, reference: Mapping[str, Any], *,
    run_uid: str, build_sha: str, trade_date: str,
) -> dict:
    generation = str(reference.get("generation_sha256") or "")
    core = {key: value for key, value in reference.items() if key != "reference_sha256"}
    if (
        reference.get("schema") != FORMAL_SCORE_WORK_SCHEMA
        or reference.get("run_uid") != run_uid
        or reference.get("build_sha") != build_sha
        or reference.get("trade_date") != trade_date
        or reference.get("automatic_real_order_submission") is not False
        or reference.get("real_order_authority") is not False
        or re.fullmatch(r"[0-9a-f]{64}", generation) is None
        or canonical_sha256(core) != reference.get("reference_sha256")
    ):
        raise FormalScoreContractError("initial score reference differs from the exact scheduler run")
    snapshot = reference.get("score_snapshot") or {}
    if snapshot.get("encoding") != DURABLE_SCORE_SNAPSHOT_ENCODING:
        raise FormalScoreContractError("initial score reference does not address durable score shards")
    manifest = _read_score_manifest(snapshot, trade_date=trade_date, store=store)
    header = manifest["metadata"]
    selection = store.get(generation, WORK_STAGE, WORK_KEY)
    if not isinstance(selection, dict) or canonical_sha256(selection) != reference.get("selection_sha256"):
        raise FormalScoreContractError("initial score selection is unavailable or changed")
    original = selection["score_snapshot"]
    original_manifest = _read_score_manifest(original, trade_date=trade_date, store=store)
    expected_manifest = {
        **original_manifest,
        "metadata": {**original_manifest["metadata"], "run_uid": run_uid, "build_sha": build_sha},
    }
    if (
        snapshot.get("generation_sha256") != generation
        or header["run_uid"] != run_uid or header["build_sha"] != build_sha
        or header["decision_at"] != reference.get("decision_at")
        or header["input_decision_at"] != header["decision_at"]
        or reference.get("analysis_count") != header["stock_count"]
        or reference.get("recommendation_count") != len(selection["recommendation_rows"])
        or reference.get("executable_count") != sum(
            is_executable_recommendation(row) for row in selection["recommendation_rows"]
        )
        or manifest != expected_manifest
    ):
        raise FormalScoreContractError("initial score manifest differs from its sealed calculation")
    return selection


def publish_initial_selection(
    connection, reference: Mapping[str, Any], *, run_uid: str, build_sha: str,
    trade_date: str,
) -> dict:
    from biz.analysis.sync_analysis_fast import _finish_direct_publication_history, save_outputs

    if not connection.in_transaction():
        raise ValueError("formal strategy pool publication requires the scheduler transaction")
    store = FormalAnalysisStore(connection)
    selection = verify_initial_selection(
        store, reference, run_uid=run_uid, build_sha=build_sha, trade_date=trade_date,
    )
    snapshot = decode_score_snapshot(reference["score_snapshot"], trade_date=trade_date, engine=connection)
    publication = save_outputs(
        connection.engine, snapshot["analysis_rows"], selection["recommendation_rows"], trade_date,
        publication_run_uid=run_uid, publisher_task_type="analysis_fast", publisher_build_sha=build_sha,
        score_snapshot=reference["score_snapshot"], publication_connection=connection,
        decoded_score_snapshot=snapshot,
    )
    _finish_direct_publication_history(
        connection.engine, run_uid=run_uid, success=True, publication_receipt=publication,
        publication_connection=connection,
        score_stats={key: selection[key] for key in ("market_mood_score", "flow_date", "hot_date")},
    )
    return publication


def pin_initial_cutoff(engine, *, trade_date: str, release_id: str, source_runs: Mapping[str, str],
                       proposed_cutoff: str) -> str:
    """Bind the first eligible dispatch to its exact certified upstream runs."""
    identity = canonical_sha256({
        "schema": "probiga.formal-initial-source-generation.v1",
        "trade_date": trade_date, "strategy_release_id": release_id,
        "source_runs": dict(source_runs),
    })
    value = FormalAnalysisStore(engine).get_or_put(
        identity, "initial_input", "cutoff", {"decision_at": proposed_cutoff},
    )
    cutoff = datetime.fromisoformat(value["decision_at"])
    if cutoff.tzinfo is not None or cutoff > datetime.fromisoformat(proposed_cutoff):
        raise FormalScoreContractError("initial score cutoff differs from its certified inputs")
    return cutoff.isoformat(timespec="seconds")
