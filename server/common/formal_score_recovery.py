"""Adopt an exact frozen analysis when only its publication validation changed."""
from __future__ import annotations

import ast
import math
import os
import re
import stat
import subprocess
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping

from sqlalchemy import text

from server.common.analysis_pool_receipt import _read_score_manifest
from server.common.authoritative_market_clock import STRATEGY_POOL_INITIAL_TIME
from server.common.component_release import load_runtime_component_release
from server.common.component_release_attestation import (
    load_component_attestation,
    require_compatible_component_build,
)
from server.common.component_release_ledger import component_release_row_state
from server.common.component_release_proof import load_runtime_component_release_lite
from server.common.daily_delivery_control import canonical_sha256, strategy_release_identity
from server.common.formal_analysis_store import FormalAnalysisStore
from server.common.formal_score_publication import (
    FORMAL_SCORE_WORK_SCHEMA,
    FormalScoreContractError,
    load_initial_selection,
    verify_initial_selection,
)


_RELEASE_ROOT = Path("/opt/ProBigA-releases")
_PERMITTED_VALIDATION_FILES = frozenset({
    "server/common/formal_score_recovery.py",
    "server/common/formal_score_source_evidence.py",
    "server/common/scheduler_validation.py",
    "tools/run_ai_recommendation_premarket.py",
})
_SCORER_FILES = (
    "biz/analysis/sync_analysis_fast.py",
    "biz/analysis/formal_scoring_checkpoints.py",
    "server/common/analysis_pool_receipt.py",
    "server/common/formal_score_publication.py",
    "server/common/pit_facts.py",
    "server/engine/strategy_industry_history.py",
)
_REQUIRED_INPUTS = frozenset({
    "qmt_stock_daily_canonical", "target_turnover_snapshot", "capital_flow_batch_fast",
    "qmt_announcement_pit", "stock_finance",
})
_SHA40 = re.compile(r"[0-9a-f]{40}")
_RUN_UID = re.compile(r"[0-9a-f]{32}")
_SCHEDULER_RUNTIME = "server/api/scheduler_runtime.py"
_MAX_COMPONENT_PARENT_LINKS = 8
_MAX_RECOVERY_ATTEMPTS = 8
_ABANDONED_PUBLICATION_MARKERS = frozenset({
    "TASK_OWNER_LEASE_EXPIRED: exact run lease expired; original process result retained in OBSERVED; no publication retried.",
    "DAILY_DELIVERY_FINALIZATION_FAILED: publication lease expired; original process result retained in OBSERVED; no publication retried.",
})
_SERVICE_STOP_MARKER = "调度服务停止；子进程已确认退出。"
_EXPIRED_TASK_OWNER_TAKEOVER = re.compile(
    r"INTERRUPTED_OWNER_GONE: exact scheduler task owner exited before completion; "
    r"previous_instance=(?P<owner>[^;\r\n]{1,160}); previous_build=(?P<old>[0-9a-f]{40}); "
    r"current_build=(?P<new>[0-9a-f]{40}); "
    r"owner_evidence=expired_exact_run_lease:(?P<task_expiration>[0-9T:.-]{19,26}); "
    r"released_for_scheduler_catchup=true"
)


def _local_decision(value: str | datetime, *, whole_seconds: bool = True) -> datetime:
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise FormalScoreContractError("formal recovery decision is invalid") from exc
    if result.tzinfo is not None or (whole_seconds and result.microsecond):
        raise FormalScoreContractError("formal recovery needs a local second-precision decision")
    return result


def _trusted_checkout(build_sha: str) -> Path:
    if not _SHA40.fullmatch(build_sha) or build_sha == "0" * 40:
        raise FormalScoreContractError("formal recovery checkout identity is invalid")
    root = _RELEASE_ROOT / build_sha
    try:
        for candidate in (_RELEASE_ROOT, root, root / ".git"):
            info = candidate.lstat()
            is_expected_type = stat.S_ISREG(info.st_mode) if candidate.name == ".git" else stat.S_ISDIR(info.st_mode)
            if not is_expected_type or info.st_uid != 0 or info.st_mode & 0o022:
                raise FormalScoreContractError("formal recovery checkout is not an immutable release")
    except OSError as exc:
        raise FormalScoreContractError("formal recovery retained checkout is unavailable") from exc
    return root


def _git(root: Path, *args: str) -> bytes:
    safe_env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    safe_env["GIT_OPTIONAL_LOCKS"] = "0"
    try:
        command = subprocess.run(
            [
                "git", "--no-replace-objects", "-c", f"safe.directory={root}",
                "-c", "core.fsmonitor=false", "-C", str(root), *args,
            ],
            env=safe_env, capture_output=True, check=True, timeout=5,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise FormalScoreContractError("formal recovery retained Git proof is unavailable") from exc
    if len(command.stdout) > 256 * 1024:
        raise FormalScoreContractError("formal recovery Git proof exceeds its bound")
    return command.stdout


def _scorer_source(root: Path, relative: str) -> bytes:
    path = root / relative
    try:
        for parent in path.parents:
            if parent == root:
                break
            owner = parent.lstat()
            if not stat.S_ISDIR(owner.st_mode) or owner.st_uid != 0 or owner.st_mode & 0o022:
                raise FormalScoreContractError("formal recovery scorer path differs from its retained release")
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022 or info.st_size > 8 * 1024 * 1024:
            raise FormalScoreContractError("formal recovery scorer file is not an immutable release file")
        return path.read_bytes()
    except OSError as exc:
        raise FormalScoreContractError("formal recovery retained scorer source is unavailable") from exc


def _scheduler_outside_terminal_projection(source: bytes) -> str:
    tree = ast.parse(source, filename=_SCHEDULER_RUNTIME)
    snapshots = [node for node in tree.body
                 if isinstance(node, ast.FunctionDef) and node.name == "_terminal_snapshot"]
    if len(snapshots) != 1:
        raise FormalScoreContractError("formal recovery scheduler terminal projection is unavailable")
    snapshots[0].body = []
    return ast.dump(tree)


def _same_scorer(previous_build: str, current_build: str) -> bool:
    previous, current = _trusted_checkout(previous_build), _trusted_checkout(current_build)
    if Path(__file__).resolve().parents[2] != current:
        raise FormalScoreContractError("formal recovery is not running from the sealed current checkout")
    for root, build in ((previous, previous_build), (current, current_build)):
        if (
            _git(root, "rev-parse", "--verify", "HEAD").strip() != build.encode("ascii")
            or _git(root, "status", "--porcelain=v1", "-z", "--untracked-files=no")
        ):
            raise FormalScoreContractError("formal recovery checkout does not match its sealed build")
    if previous == current:
        return True
    changed = _git(current, "diff", "--name-only", "-z", "--no-renames", previous_build, current_build, "--")
    paths = changed.decode("utf-8").rstrip("\0").split("\0") if changed else []
    if not paths or any(
        path != _SCHEDULER_RUNTIME and path not in _PERMITTED_VALIDATION_FILES and not path.startswith("tests/")
        for path in paths
    ):
        return False
    if _SCHEDULER_RUNTIME in paths and (
        _scheduler_outside_terminal_projection(_scorer_source(previous, _SCHEDULER_RUNTIME))
        != _scheduler_outside_terminal_projection(_scorer_source(current, _SCHEDULER_RUNTIME))
    ):
        return False
    return all(_scorer_source(previous, name) == _scorer_source(current, name) for name in _SCORER_FILES)


def _exact_prior_terminal(connection, *, run_uid: str, build_sha: str, trade_date: str,
                          contract_build: str, strategy_release: str) -> dict:
    current = connection.execute(text("""
        SELECT /*+ MAX_EXECUTION_TIME(3000) */ a.id AS stage_id, a.session_uid,
            a.status AS stage_status, a.started_at AS stage_started_at,
            a.shard_id, a.lease_owner, a.fencing_token,
            h.status AS audit_status, h.task_type, h.build_sha, h.scheduler_instance_id,
            r.trade_date AS score_trade_date,r.min_score,r.top_n,r.execution_time
        FROM st_daily_stage_attempt a
        JOIN st_daily_delivery_session s ON s.session_uid=a.session_uid
        JOIN st_scheduled_task_history h ON h.run_uid=a.scheduler_run_uid
        LEFT JOIN st_recommended_run_history r ON r.run_uid=h.run_uid AND r.scheduler_job_id=h.run_uid
        WHERE a.scheduler_run_uid=:run_uid AND a.stage_name='analysis_fast'
          AND s.trade_date=:trade_date AND s.release_id=:contract
          AND s.strategy_release_id=:strategy_release
    """), {
        "run_uid": run_uid, "trade_date": trade_date, "contract": contract_build,
        "strategy_release": strategy_release,
    }).mappings().one_or_none()
    if (
        current is None or current["stage_status"] != "RUNNING" or current["audit_status"] != "running"
        or current["task_type"] != "analysis_fast" or current["build_sha"] != build_sha
        or str(current["score_trade_date"])[:10] != trade_date
    ):
        raise FormalScoreContractError("formal recovery is outside the exact current scheduler claim")
    return dict(current)


def _attempts_in_session(connection, session_uid: str, *, shard_id: str, excluding: str | None = None,
                         exact: str | None = None) -> list[dict]:
    rows = connection.execute(text("""
        SELECT /*+ MAX_EXECUTION_TIME(3000) */ a.id AS stage_id, a.scheduler_run_uid AS run_uid,
            a.status AS stage_status, a.started_at AS stage_started_at, a.finished_at AS stage_finished_at,
            a.lease_owner, a.lease_until, a.fencing_token, a.error_code, a.output_dataset_id,
            h.status AS audit_status,h.finished_at,h.exit_code,h.build_sha,h.task_type,
            h.scheduler_instance_id,SUBSTR(h.output, 1, 60000) AS output,
            r.trade_date AS score_trade_date,r.min_score,r.top_n,r.execution_time,
            r.published_at,r.canonical_pool_sha256
        FROM st_daily_stage_attempt a
        LEFT JOIN st_scheduled_task_history h ON h.run_uid=a.scheduler_run_uid
        LEFT JOIN st_recommended_run_history r ON r.run_uid=h.run_uid AND r.scheduler_job_id=h.run_uid
        WHERE a.session_uid=:session_uid AND a.stage_name='analysis_fast' AND a.shard_id=:shard_id
          AND (:excluding IS NULL OR a.scheduler_run_uid<>:excluding)
          AND (:exact IS NULL OR a.scheduler_run_uid=:exact)
        ORDER BY a.id DESC LIMIT :limit
    """), {"session_uid": session_uid, "shard_id": shard_id, "excluding": excluding, "exact": exact,
               "limit": 1 if exact is not None else _MAX_RECOVERY_ATTEMPTS}).mappings().all()
    return [dict(row) for row in rows]


def _unpublished_terminal(record: Mapping[str, Any], builds: set[str], trade_date: str) -> bool:
    return (
        record["build_sha"] in builds and record["task_type"] == "analysis_fast"
        and str(record["score_trade_date"])[:10] == trade_date
        and record["finished_at"] is not None and record["stage_finished_at"] is not None
        and record["published_at"] is None and record["canonical_pool_sha256"] is None
    )


def _successful_child_failed_publication(record: Mapping[str, Any]) -> bool:
    return record["stage_status"] == "FAILED" and record["audit_status"] == "failed" and record["exit_code"] == 0


def _expired_takeover(record: Mapping[str, Any], newer: Mapping[str, Any]) -> bool:
    if (
        record["error_code"] != "LEASE_EXPIRED_TAKEOVER" or record["audit_status"] != "failed"
        or record["exit_code"] is not None or record["output_dataset_id"] is not None
        or type(record["fencing_token"]) is not int or type(newer["fencing_token"]) is not int
        or not 0 < record["fencing_token"] < newer["fencing_token"]
        or record["lease_owner"] != record["scheduler_instance_id"]
        or newer["lease_owner"] != newer["scheduler_instance_id"]
        or not newer["lease_owner"] or record["lease_owner"] == newer["lease_owner"]
    ):
        return False
    audit = _EXPIRED_TASK_OWNER_TAKEOVER.fullmatch(record["output"] or "")
    if (
        audit is None or audit["owner"] != record["lease_owner"]
        or audit["old"] != record["build_sha"] or audit["new"] != newer["build_sha"]
    ):
        return False
    try:
        stage_started = _local_decision(record["stage_started_at"], whole_seconds=False)
        stage_expires = _local_decision(record["lease_until"], whole_seconds=False)
        task_expires = _local_decision(audit["task_expiration"], whole_seconds=False)
        audit_finished = _local_decision(record["finished_at"], whole_seconds=False)
        superseded = _local_decision(record["stage_finished_at"], whole_seconds=False)
        newer_started = _local_decision(newer["stage_started_at"], whole_seconds=False)
    except FormalScoreContractError:
        return False
    return (
        stage_started <= min(task_expires, stage_expires)
        and max(task_expires, stage_expires) < audit_finished <= superseded == newer_started
    )


def _abandoned_without_publication(record: Mapping[str, Any], newer: Mapping[str, Any]) -> bool:
    if record["stage_status"] == "SUPERSEDED":
        return _expired_takeover(record, newer)
    lines = (record["output"] or "").splitlines()
    if record["stage_status"] == "FAILED" and record["audit_status"] == "failed":
        return record["exit_code"] is None and any(line in _ABANDONED_PUBLICATION_MARKERS for line in lines)
    return (
        record["stage_status"] == "STOPPED" and record["audit_status"] == "stopped"
        and record["exit_code"] != 0 and lines[:1] == [_SERVICE_STOP_MARKER]
    )


def _verify_component_lineage(connection, runtime: Mapping[str, str]) -> dict[str, str]:
    build_sha, parent = runtime["linux_build_sha"], runtime["parent_linux_build_sha"]
    try:
        attestation = load_component_attestation(connection, build_sha)
    except RuntimeError as exc:
        # The production fast-release health path trusts the same root-sealed
        # local proof only when the signed ledger says its row is absent.
        if component_release_row_state(connection, build_sha) != "absent":
            raise FormalScoreContractError("formal recovery current component ledger cannot authenticate") from exc
        try:
            load_runtime_component_release_lite(runtime)
        except RuntimeError as proof_error:
            raise RuntimeError("formal recovery current component publication is pending") from proof_error
        previous = load_component_attestation(connection, parent)
        coordinated_anchor = previous["scope"] == "COORDINATED" and previous["linux_build_sha"] == previous["contract_build_sha"]
        if not coordinated_anchor and previous["contract_sha256"] != runtime["contract_sha256"]:
            raise FormalScoreContractError("formal recovery component parent contract differs")
        return previous
    if attestation != runtime:
        raise FormalScoreContractError("formal recovery current component seals differ")
    return require_compatible_component_build(connection, parent, build_sha)


def _compatible_recovery_builds(connection, runtime: Mapping[str, str]) -> set[str]:
    builds = {runtime["linux_build_sha"]}
    parent_sha = runtime["parent_linux_build_sha"]
    parent = _verify_component_lineage(connection, runtime)
    for _ in range(_MAX_COMPONENT_PARENT_LINKS):
        if (
            parent_sha in builds or parent["linux_build_sha"] != parent_sha
            or parent["contract_build_sha"] != runtime["contract_build_sha"]
            or parent["windows_build_sha"] != runtime["windows_build_sha"]
        ):
            raise FormalScoreContractError("formal recovery component parent scope differs")
        builds.add(parent_sha)
        if parent_sha == runtime["contract_build_sha"]:
            if parent["scope"] != "COORDINATED":
                raise FormalScoreContractError("formal recovery coordinated component anchor differs")
            return builds
        if parent["scope"] != "LINUX" or parent["contract_sha256"] != runtime["contract_sha256"]:
            raise FormalScoreContractError("formal recovery component parent contract differs")
        parent_sha = parent["parent_linux_build_sha"]
        if parent_sha in builds:
            raise FormalScoreContractError("formal recovery component lineage contains a cycle")
        parent = load_component_attestation(connection, parent_sha)
    raise FormalScoreContractError("formal recovery component lineage exceeds its bound")


def _candidate(
    engine, *, trade_date: str, proposed_cutoff: str | datetime, run_uid: str,
    build_sha: str, min_score: float | None, top_n: int | None, model_version: str,
) -> dict | None:
    from server.common.formal_score_source_evidence import frozen_initial_source_runs
    from server.common.scheduler_validation import _single_nested_machine_payload

    if not _RUN_UID.fullmatch(run_uid) or not _SHA40.fullmatch(build_sha):
        raise FormalScoreContractError("formal recovery current run or build is invalid")
    target = date.fromisoformat(trade_date)
    proposal = _local_decision(proposed_cutoff)
    earliest = datetime.combine(target, STRATEGY_POOL_INITIAL_TIME)
    if target.isoformat() != trade_date or proposal < earliest:
        raise FormalScoreContractError("formal recovery proposal differs from the closed target")

    runtime = load_runtime_component_release()
    if runtime["linux_build_sha"] != build_sha:
        raise FormalScoreContractError("formal recovery runtime build differs")
    if runtime["scope"] != "LINUX":
        return None
    with engine.connect() as connection:
        current = _exact_prior_terminal(
            connection, run_uid=run_uid, build_sha=build_sha, trade_date=trade_date,
            contract_build=runtime["contract_build_sha"], strategy_release=strategy_release_identity(),
        )
        attempts = _attempts_in_session(
            connection, current["session_uid"], shard_id=current["shard_id"], excluding=run_uid,
        )
        if not attempts:
            return None
        builds = _compatible_recovery_builds(connection, runtime)
    current_started = _local_decision(current["stage_started_at"], whole_seconds=False)
    newer = current
    for prior in attempts:
        if prior["stage_id"] >= current["stage_id"] or not _unpublished_terminal(prior, builds, trade_date):
            return None
        if any(_local_decision(prior[field], whole_seconds=False) > current_started for field in (
            "stage_finished_at", "finished_at",
        )):
            raise FormalScoreContractError("formal recovery discarded attempt is not earlier")
        if _successful_child_failed_publication(prior):
            break
        if not _abandoned_without_publication(prior, newer):
            return None
        newer = prior
    else:
        return None

    latest = _single_nested_machine_payload(prior["output"], schema=FORMAL_SCORE_WORK_SCHEMA)
    if not latest:
        return None

    store = FormalAnalysisStore(engine)
    verify_initial_selection(store, latest, run_uid=prior["run_uid"], build_sha=prior["build_sha"], trade_date=trade_date)
    origin = _read_score_manifest(latest["score_snapshot"], trade_date=trade_date, store=store)["origin"]
    origin_build = origin["build_sha"]
    if origin_build not in builds or not _same_scorer(origin_build, build_sha):
        return None
    if prior["run_uid"] == origin["run_uid"]:
        original = latest
    else:
        with engine.connect() as connection:
            original_records = _attempts_in_session(
                connection, current["session_uid"], shard_id=current["shard_id"], exact=origin["run_uid"],
            )
        if not original_records:
            raise FormalScoreContractError("formal recovery original scheduler audit is unavailable")
        original_record = original_records[0]
        if (
            original_record["stage_id"] >= prior["stage_id"]
            or not _unpublished_terminal(original_record, {origin_build}, trade_date)
            or not _successful_child_failed_publication(original_record)
        ):
            raise FormalScoreContractError("formal recovery original scheduler audit is unavailable")
        original = _single_nested_machine_payload(original_record["output"], schema=FORMAL_SCORE_WORK_SCHEMA)
        if not original or any(latest.get(field) != original.get(field) for field in (
            "trade_date", "decision_at", "generation_sha256", "selection_sha256",
        )):
            raise FormalScoreContractError("formal recovery failed rebind has different original evidence")
        prior = original_record
    if prior["build_sha"] != origin_build or prior["run_uid"] != origin["run_uid"]:
        raise FormalScoreContractError("formal recovery manifest origin differs from its scheduler audit")

    try:
        score = float(current["min_score"])
        quota = int(current["top_n"])
        original_cutoff = _local_decision(original.get("decision_at"))
        if (
            not math.isfinite(score) or not 0 <= score <= 100 or not 1 <= quota <= 200
            or (min_score is not None and score != min_score)
            or (top_n is not None and quota != top_n)
            or str(prior["score_trade_date"])[:10] != trade_date
            or _local_decision(prior["execution_time"]) != original_cutoff
            or _local_decision(current["execution_time"]) != proposal
            or not earliest <= original_cutoff <= proposal
            or _local_decision(prior["stage_finished_at"], whole_seconds=False)
                > _local_decision(current["stage_started_at"], whole_seconds=False)
            or _local_decision(prior["finished_at"], whole_seconds=False)
                > _local_decision(current["stage_started_at"], whole_seconds=False)
        ):
            raise FormalScoreContractError("formal recovery frozen cutoff or original scoring parameters differ")
        if float(prior["min_score"]) != score or int(prior["top_n"]) != quota:
            return None
        source_runs = frozen_initial_source_runs(
            engine, trade_date=trade_date, decision_at=original_cutoff, build_sha=origin_build,
        )
        if set(source_runs) != _REQUIRED_INPUTS:
            raise FormalScoreContractError("formal recovery original input scope differs")
        generation = canonical_sha256({
            "schema": "probiga.initial-selection-generation.v1", "trade_date": trade_date,
            "decision_at": original_cutoff.isoformat(timespec="seconds"), "build_sha": origin_build,
            "model_version": model_version, "source_runs_sha256": canonical_sha256(source_runs),
            "min_score": score, "top_n": quota,
        })
        if generation != original.get("generation_sha256"):
            raise FormalScoreContractError("formal recovery exact original input generation differs")
        verify_initial_selection(store, original, run_uid=prior["run_uid"], build_sha=origin_build, trade_date=trade_date)
        metadata = _read_score_manifest(original["score_snapshot"], trade_date=trade_date, store=store)["metadata"]
        if (
            metadata.get("decision_at") != original.get("decision_at")
            or not isinstance(metadata.get("fixed_market_context"), dict)
            or set(metadata.get("frozen_core_inputs", {})) != {"kline", "turnover", "financial", "announcement"}
            or metadata.get("frozen_announcement_source", {}).get("scheduler_run_uid")
                != source_runs["qmt_announcement_pit"]
        ):
            raise FormalScoreContractError("formal recovery original score is missing its exact frozen inputs")
    except FormalScoreContractError:
        raise
    except (KeyError, TypeError, RuntimeError, ValueError) as exc:
        raise FormalScoreContractError("formal recovery exact original evidence could not be verified") from exc
    return original


def recover_initial_selection(
    engine, *, trade_date: str, proposed_cutoff: str | datetime, run_uid: str,
    build_sha: str, min_score: float, top_n: int, model_version: str,
) -> dict | None:
    """Rebind the verified prior calculation through the current audit, without rescoring."""
    original = _candidate(
        engine, trade_date=trade_date, proposed_cutoff=proposed_cutoff, run_uid=run_uid,
        build_sha=build_sha, min_score=min_score, top_n=top_n, model_version=model_version,
    )
    if original is None:
        return None
    try:
        prepared = load_initial_selection(
            FormalAnalysisStore(engine), generation=original["generation_sha256"],
            run_uid=run_uid, build_sha=build_sha,
        )
        if not prepared or prepared["reference"]["selection_sha256"] != original["selection_sha256"]:
            raise FormalScoreContractError("formal recovery rebound selection differs")
    except (KeyError, TypeError, RuntimeError, ValueError) as exc:
        raise FormalScoreContractError("formal recovery could not bind the verified calculation") from exc
    return prepared


def verify_recovered_initial_selection(
    engine, reference: Mapping[str, Any], *, proposed_cutoff: str | datetime,
) -> None:
    """Recheck the original authorization for an earlier-cutoff prepared reference."""
    from biz.analysis.sync_analysis_fast import MODEL_VERSION

    try:
        if _local_decision(reference["decision_at"]) >= _local_decision(proposed_cutoff):
            raise FormalScoreContractError("formal recovery requires its actual earlier decision")
        original = _candidate(
            engine, trade_date=reference["trade_date"], proposed_cutoff=proposed_cutoff,
            run_uid=reference["run_uid"], build_sha=reference["build_sha"],
            min_score=None, top_n=None, model_version=MODEL_VERSION,
        )
        if original is None or any(reference[field] != original[field] for field in (
            "trade_date", "decision_at", "generation_sha256", "selection_sha256",
            "analysis_count", "recommendation_count", "executable_count",
        )):
            raise FormalScoreContractError("formal recovery rebound proof differs from its original archived selection")
        verify_initial_selection(
            FormalAnalysisStore(engine), reference,
            run_uid=reference["run_uid"], build_sha=reference["build_sha"], trade_date=reference["trade_date"],
        )
    except FormalScoreContractError:
        raise
    except (KeyError, TypeError, RuntimeError, ValueError) as exc:
        raise FormalScoreContractError("formal recovery rebound proof could not be verified") from exc
