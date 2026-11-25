"""Adopt an exact frozen analysis when only its publication validation changed."""
from __future__ import annotations

import ast
import hashlib
import math
import os
import re
import stat
import subprocess
from contextlib import nullcontext
from datetime import date, datetime
from pathlib import Path
from time import monotonic
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
from server.common.daily_delivery_control import (
    ANALYSIS_PUBLICATION_REFERENCE_SCHEMA,
    DailyDeliveryControlError,
    analysis_publication_reference,
    canonical_sha256,
    daily_session_identity,
    load_published_analysis_receipt,
    strategy_release_identity,
)
from server.common.formal_analysis_store import FormalAnalysisStore
from server.common.formal_score_publication import (
    FORMAL_SCORE_WORK_SCHEMA,
    FormalScoreContractError,
    load_initial_selection,
    verify_initial_selection,
)


_RELEASE_ROOT = Path("/opt/ProBigA-releases")
_RELEASE_GIT_CACHE = Path("/var/lib/probiga/release-sources/probiga.git")
_MAX_SCORER_SOURCE_BYTES = 8 * 1024 * 1024
_PERMITTED_VALIDATION_FILES = frozenset({
    "deploy/production_deploy.sh",
    "server/common/formal_score_recovery.py",
    "server/common/formal_score_source_evidence.py",
    "server/common/scheduler_validation.py",
    "server/engine/strategy_governance.py",
    "server/engine/strategy_governance_orchestrator.py",
    # These execution/evidence readers are not called by the formal scorer.
    "server/engine/governance_daily_market.py",
    "server/engine/strategy_native_backtest.py",
    "server/engine/native_research_jobs.py",
    "server/engine/native_research_prepare.py",
    # Only dated replay sets event_trade_date and calls this reader.
    "server/common/historical_announcement_read.py",
    "server/static/js/trading-v3.js",
    "server/static/trading-v3.html",
    "tools/run_strategy_governance_daily.py",
    "tools/run_trading_v2_job_worker.py",
    "tools/classify_release_scope.py",
    "biz/stock_finance/sync_finance.py",
    "server/static/index.html",
    "server/static/js/app.js",
    # These display-only modules are not imported by the scoring process.
    "server/api/main.py",
    "server/api/routers/market_home.py",
    "server/api/routers/research_futures.py",
    "server/static/js/research-tools.js",
    "server/static/js/market-home.js",
    "server/static/css/research-tools.css",
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
_VALIDATION_ONLY_BODIES = {
    # Historical reconstruction IO does not change a frozen score's formula or inputs.
    "server/common/pit_facts.py": ("_bind_historical_event_snapshot",),
    "server/api/routers/hot_data.py": ("_recommended_stocks_v2", "_recommended_data_freshness"),
    "server/api/scheduler_runtime.py": (
        "_terminal_snapshot", "_daily_result_recovery_target",
        "_daily_delivery_runtime_health", "_build_daily_result_delivery_receipt",
        "_strategy_pipeline_dependencies_ready",
    ),
    "server/engine/strategy_center.py": ("_dynamic_execution_signals",),
    "server/engine/strategy_shadow_trials.py": ("native_trial_candidate_batch",),
}
_COMPONENT_LINEAGE_TIMEOUT_SECONDS = 60
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


def _protected_release_path(path: Path, *, directory: bool = False) -> os.stat_result:
    info = path.lstat()
    expected_type = stat.S_ISDIR if directory else stat.S_ISREG
    if not expected_type(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
        raise FormalScoreContractError("formal recovery checkout is not an immutable release")
    return info


def _trusted_checkout(build_sha: str) -> Path:
    if not _SHA40.fullmatch(build_sha) or build_sha == "0" * 40:
        raise FormalScoreContractError("formal recovery checkout identity is invalid")
    root = _RELEASE_ROOT / build_sha
    try:
        _protected_release_path(_RELEASE_ROOT, directory=True)
        _protected_release_path(root, directory=True)
        pointer = root / ".git"
        if _protected_release_path(pointer).st_size > 4096:
            raise FormalScoreContractError("formal recovery Git worktree pointer exceeds its bound")
        marker = pointer.read_text(encoding="utf-8").rstrip("\n")
        control = Path(marker.removeprefix("gitdir: "))
        if (
            not marker.startswith("gitdir: ") or control.name in {"", ".", ".."}
            or control.parent != _RELEASE_GIT_CACHE / "worktrees"
        ):
            raise FormalScoreContractError("formal recovery Git worktree is outside the release cache")
        for directory in (*_RELEASE_GIT_CACHE.parents, _RELEASE_GIT_CACHE,
                          _RELEASE_GIT_CACHE / "objects", control.parent, control):
            _protected_release_path(directory, directory=True)
        for path in (_RELEASE_GIT_CACHE / "config", control / "HEAD", control / "index"):
            _protected_release_path(path)
        common_dir = control / "commondir"
        if _protected_release_path(common_dir).st_size > 16 or common_dir.read_bytes().strip() != b"../..":
            raise FormalScoreContractError("formal recovery Git common directory differs")
        for name in ("alternates", "http-alternates"):
            if os.path.lexists(_RELEASE_GIT_CACHE / "objects" / "info" / name):
                raise FormalScoreContractError("formal recovery Git cache has object alternates")
    except OSError as exc:
        raise FormalScoreContractError("formal recovery active checkout or Git cache is unavailable") from exc
    return root


def _git(root: Path, *args: str, output_limit: int = 256 * 1024) -> bytes:
    safe_env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    safe_env.update(
        GIT_OPTIONAL_LOCKS="0", GIT_CONFIG_NOSYSTEM="1",
        GIT_CONFIG_GLOBAL=os.devnull, GIT_NO_LAZY_FETCH="1", GIT_TERMINAL_PROMPT="0",
    )
    try:
        command = subprocess.run(
            [
                "git" if os.name == "nt" else "/usr/bin/git",
                "--no-replace-objects", "--literal-pathspecs", "-c", f"safe.directory={root}",
                "-c", "core.fsmonitor=false", "-c", f"core.hooksPath={os.devnull}",
                "-c", "protocol.allow=never", "-C", str(root), *args,
            ],
            env=safe_env, stdin=subprocess.DEVNULL, capture_output=True, check=True, timeout=5,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise FormalScoreContractError("formal recovery retained Git proof is unavailable") from exc
    if len(command.stdout) > output_limit:
        raise FormalScoreContractError("formal recovery Git proof exceeds its bound")
    return command.stdout


def _scorer_source(root: Path, relative: str) -> bytes:
    path = root / relative
    try:
        for parent in path.parents:
            if parent == root:
                break
            _protected_release_path(parent, directory=True)
        if _protected_release_path(path).st_size > _MAX_SCORER_SOURCE_BYTES:
            raise FormalScoreContractError("formal recovery scorer file is not an immutable release file")
        return path.read_bytes()
    except OSError as exc:
        raise FormalScoreContractError("formal recovery retained scorer source is unavailable") from exc


def _without_validation_body(path: str, source: bytes) -> str:
    tree = ast.parse(source, filename=path)
    if path == "server/api/scheduler_runtime.py":
        # Only the governance retry gate is independent of shared input selection.
        retry_gate = ast.dump(ast.parse(
            """
if ready and task_type == "strategy_governance_daily" and _daily_delivery_requires_production_runtime():
    ready, reason = _governance_database_retry_allowed(
        engine, trade_date=expected_trade_date, build_sha=expected_build_sha,
        score_run_uid=latest_histories["analysis_fast"]["run_uid"], now=now,
    )
"""
        ).body[0])
        dependencies = next((
            node for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "_strategy_pipeline_dependencies_ready"
        ), None)
        for branch in ast.walk(dependencies) if dependencies is not None else ():
            body = getattr(branch, "body", None)
            if isinstance(body, list):
                body[:] = [node for node in body if ast.dump(node) != retry_gate]
        old_output = ast.parse('(stdout or "") + "\\n" + (stderr or "")', mode="eval").body
        governance_output = ast.parse(
            'stdout or "" if task_type == "strategy_governance_daily" else (stdout or "") + "\\n" + (stderr or "")',
            mode="eval",
        ).body
        for function in tree.body:
            if isinstance(function, ast.FunctionDef) and function.name == "_run_task_impl":
                for node in ast.walk(function):
                    if isinstance(node, ast.Assign) and ast.dump(node.value) == ast.dump(governance_output):
                        node.value = old_output
        tree.body[:] = [
            node for node in tree.body
            if not isinstance(node, ast.FunctionDef) or node.name != "_governance_database_retry_allowed"
        ]
    for name in _VALIDATION_ONLY_BODIES[path]:
        functions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name]
        if len(functions) != 1:
            raise FormalScoreContractError("formal recovery isolated validation function is unavailable")
        function = functions[0]
        if name == "_recommended_data_freshness":
            # The display-only reader gained one optional status input.
            arguments = function.args
            if arguments.kwonlyargs and arguments.kwonlyargs[-1].arg == "publication_status":
                parameter = arguments.kwonlyargs[-1]
                default = arguments.kw_defaults[-1]
                if (
                    isinstance(parameter.annotation, ast.Name) and parameter.annotation.id == "str"
                    and isinstance(default, ast.Constant) and default.value == ""
                ):
                    arguments.kwonlyargs.pop()
                    arguments.kw_defaults.pop()
        function.body = []
    return ast.dump(tree)


def _scorer_blobs(root: Path, build: str, paths: set[str]) -> dict[str, str]:
    result = {}
    entries = _git(root, "ls-tree", "-z", "--full-tree", build, "--", *sorted(paths))
    for entry in entries.split(b"\0")[:-1]:
        metadata, _, relative = entry.partition(b"\t")
        path = relative.decode("utf-8")
        match = re.fullmatch(rb"100(?:644|755) blob ([0-9a-f]{40})", metadata)
        if path not in paths or path in result or match is None:
            raise FormalScoreContractError("formal recovery scorer Git entry differs")
        result[path] = match.group(1).decode("ascii")
    if result.keys() != paths:
        raise FormalScoreContractError("formal recovery scorer Git entries are unavailable")
    return result


def _scorer_blob_bytes(root: Path, blob: str) -> bytes:
    size = _git(root, "cat-file", "-s", blob, output_limit=32).strip()
    if not size.isdigit() or int(size) > _MAX_SCORER_SOURCE_BYTES:
        raise FormalScoreContractError("formal recovery scorer Git blob exceeds its bound")
    source = _git(root, "cat-file", "blob", blob, output_limit=int(size))
    if len(source) != int(size) or _blob_oid(source) != blob:
        raise FormalScoreContractError("formal recovery retained scorer differs from its Git blob")
    return source


def _blob_oid(source: bytes) -> str:
    return hashlib.sha1(f"blob {len(source)}\0".encode("ascii") + source, usedforsecurity=False).hexdigest()


def _same_scorer(previous_build: str, current_build: str) -> bool:
    if not _SHA40.fullmatch(previous_build) or previous_build == "0" * 40:
        raise FormalScoreContractError("formal recovery previous Git identity is invalid")
    current = _trusted_checkout(current_build)
    if Path(__file__).resolve().parents[2] != current:
        raise FormalScoreContractError("formal recovery is not running from the sealed current checkout")
    if (
        _git(current, "rev-parse", "--verify", "HEAD").strip() != current_build.encode("ascii")
        or _git(current, "status", "--porcelain=v1", "-z", "--untracked-files=no", "--ignore-submodules=none")
    ):
        raise FormalScoreContractError("formal recovery checkout does not match its sealed build")
    # The caller restricts both builds to signed component lineage and verifies
    # the originating terminal and immutable score. The root-owned Git cache
    # retains those commits independently of old deployed worktree directories.
    if previous_build != current_build and _git(current, "cat-file", "-t", previous_build) != b"commit\n":
        raise FormalScoreContractError("formal recovery previous Git object is not a commit")
    changed = b""
    if previous_build != current_build:
        changed = _git(
            current, "diff", "--name-only", "-z", "--no-renames", "--no-ext-diff",
            "--no-textconv", "--ignore-submodules=none", previous_build, current_build, "--",
        )
    paths = changed.decode("utf-8").rstrip("\0").split("\0") if changed else []
    if (previous_build != current_build and not paths) or any(
        path not in _VALIDATION_ONLY_BODIES and path not in _PERMITTED_VALIDATION_FILES and not path.startswith("tests/")
        for path in paths
    ):
        return False
    masked_paths = _VALIDATION_ONLY_BODIES.keys() & set(paths)
    scorer_paths = set(_SCORER_FILES) | masked_paths
    current_blobs = _scorer_blobs(current, current_build, scorer_paths)
    previous_blobs = current_blobs if previous_build == current_build else _scorer_blobs(current, previous_build, scorer_paths)
    for path in sorted(scorer_paths):
        source = _scorer_source(current, path)
        oid = _blob_oid(source)
        if oid != current_blobs[path]:
            raise FormalScoreContractError("formal recovery current scorer differs from its Git blob")
        if path in masked_paths:
            old_source = _scorer_blob_bytes(current, previous_blobs[path])
            if _without_validation_body(path, old_source) != _without_validation_body(path, source):
                return False
        elif previous_blobs[path] != oid:
            return False
    return True


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
            r.status AS score_status,r.published_at,r.canonical_pool_sha256
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


def _terminal_in_scope(record: Mapping[str, Any], builds: set[str], trade_date: str) -> bool:
    return (
        record["build_sha"] in builds and record["task_type"] == "analysis_fast"
        and str(record["score_trade_date"])[:10] == trade_date
        and record["finished_at"] is not None and record["stage_finished_at"] is not None
    )


def _unpublished_terminal(record: Mapping[str, Any], builds: set[str], trade_date: str) -> bool:
    return (
        _terminal_in_scope(record, builds, trade_date)
        and record["published_at"] is None and record["canonical_pool_sha256"] is None
    )


def _published_terminal(record: Mapping[str, Any], builds: set[str], trade_date: str) -> bool:
    pool_hash = record["canonical_pool_sha256"]
    return (
        _terminal_in_scope(record, builds, trade_date)
        and record["stage_status"] == "SUCCESS" and record["audit_status"] == "success"
        and record["exit_code"] == 0 and _RUN_UID.fullmatch(record["run_uid"] or "") is not None
        and record["output_dataset_id"] == record["run_uid"] and record["score_status"] == "done"
        and record["published_at"] is not None and isinstance(pool_hash, str)
        and re.fullmatch(r"[0-9a-f]{64}", pool_hash) is not None and pool_hash != "0" * 64
    )


def _verify_terminal_publication(
    engine, record: Mapping[str, Any], work: Mapping[str, Any], *, trade_date: str,
    consumer_started_at: datetime,
) -> None:
    from server.common.scheduler_validation import _single_nested_machine_payload

    try:
        with (nullcontext(engine) if hasattr(engine, "execute") else engine.connect()) as connection:
            receipt = load_published_analysis_receipt(
                connection, trade_date, decision_at=consumer_started_at, run_uid=record["run_uid"],
            )
            reference = analysis_publication_reference(receipt, engine=connection)
        published_at = _local_decision(receipt["published_at"], whole_seconds=False)
        if (
            receipt["publisher_task_type"] != "analysis_fast"
            or receipt["trade_date"] != trade_date or receipt["run_uid"] != record["run_uid"]
            or receipt["build_sha"] != record["build_sha"]
            or receipt["canonical_pool_sha256"] != record["canonical_pool_sha256"]
            or receipt["score_snapshot"] != work["score_snapshot"]
            or any(receipt[field] != work[field] for field in (
                "analysis_count", "recommendation_count", "executable_count",
            ))
            or published_at != _local_decision(record["published_at"], whole_seconds=False)
            or not _local_decision(record["finished_at"], whole_seconds=False) <= published_at
                <= _local_decision(record["stage_finished_at"], whole_seconds=False) <= consumer_started_at
            or _single_nested_machine_payload(record["output"], schema=ANALYSIS_PUBLICATION_REFERENCE_SCHEMA)
                != reference
        ):
            raise FormalScoreContractError("formal recovery completed publication differs from its scheduler audit")
    except (DailyDeliveryControlError, KeyError, TypeError) as exc:
        raise FormalScoreContractError("formal recovery completed publication could not be verified") from exc


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
    from server.common.qmt_announcement_pit import _live_announcement_reply_budget

    deadline = monotonic() + _COMPONENT_LINEAGE_TIMEOUT_SECONDS
    mysql = getattr(getattr(connection, "dialect", None), "name", None) == "mysql"
    builds = {runtime["linux_build_sha"]}
    parent_sha = runtime["parent_linux_build_sha"]
    with (_live_announcement_reply_budget(connection, deadline=deadline, stage="formal-component-lineage")
          if mysql else nullcontext()):
        parent = _verify_component_lineage(connection, runtime)
    # Release count does not invalidate a signed, unchanged scoring contract.
    while monotonic() < deadline:
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
        with (_live_announcement_reply_budget(connection, deadline=deadline, stage="formal-component-lineage")
              if mysql else nullcontext()):
            parent = load_component_attestation(connection, parent_sha)
    raise RuntimeError("formal recovery component lineage validation deadline exceeded")


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
        if prior["stage_id"] >= current["stage_id"]:
            return None
        published = prior["stage_status"] == "SUCCESS"
        if published:
            if prior["build_sha"] == build_sha or not _published_terminal(prior, builds, trade_date):
                return None
        elif not _unpublished_terminal(prior, builds, trade_date):
            return None
        if any(_local_decision(prior[field], whole_seconds=False) > current_started for field in (
            "stage_finished_at", "finished_at",
        )):
            raise FormalScoreContractError("formal recovery prior attempt is not earlier")
        if published or _successful_child_failed_publication(prior):
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
    if published:
        if prior["build_sha"] != origin_build and not _same_scorer(prior["build_sha"], build_sha):
            return None
        _verify_terminal_publication(
            engine, prior, latest, trade_date=trade_date, consumer_started_at=current_started,
        )
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
        original_published = _published_terminal(original_record, {origin_build}, trade_date)
        original_failed = (
            _unpublished_terminal(original_record, {origin_build}, trade_date)
            and _successful_child_failed_publication(original_record)
        )
        if original_record["stage_id"] >= prior["stage_id"] or not (original_published or original_failed):
            raise FormalScoreContractError("formal recovery original scheduler audit is unavailable")
        original = _single_nested_machine_payload(original_record["output"], schema=FORMAL_SCORE_WORK_SCHEMA)
        if not original or any(latest.get(field) != original.get(field) for field in (
            "trade_date", "decision_at", "generation_sha256", "selection_sha256",
        )):
            raise FormalScoreContractError("formal recovery rebind has different original evidence")
        previous_claim = _local_decision(prior["stage_started_at"], whole_seconds=False)
        if any(_local_decision(original_record[field], whole_seconds=False) > previous_claim for field in (
            "stage_finished_at", "finished_at",
        )):
            raise FormalScoreContractError("formal recovery origin was unavailable to its prior rebind")
        if original_published:
            _verify_terminal_publication(
                engine, original_record, original, trade_date=trade_date, consumer_started_at=previous_claim,
            )
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


def reusable_initial_source_runs(
    engine, *, trade_date: str, build_sha: str, decision_at: datetime,
) -> dict | None:
    """Prove published frozen inputs before scheduling their existing recovery."""
    from biz.analysis.sync_analysis_fast import MODEL_VERSION
    from server.common.formal_score_source_evidence import frozen_initial_source_runs
    from server.common.qmt_announcement_pit import _live_announcement_reply_budget
    from server.common.scheduler_validation import _single_nested_machine_payload

    deadline = monotonic() + 90
    decision = _local_decision(decision_at, whole_seconds=False)
    runtime = load_runtime_component_release()
    if runtime["scope"] != "LINUX" or runtime["linux_build_sha"] != build_sha:
        return None
    scope = daily_session_identity(trade_date, runtime["contract_build_sha"])
    with engine.connect() as connection:
        mysql = connection.dialect.name == "mysql"
        with (_live_announcement_reply_budget(connection, deadline=deadline, stage="formal-dispatch-history")
              if mysql else nullcontext()):
            attempts = _attempts_in_session(connection, scope["session_uid"], shard_id="main")
        if not attempts or attempts[0]["build_sha"] == build_sha:
            return None
        prior = attempts[0]
        builds = _compatible_recovery_builds(connection, runtime)
        if not _published_terminal(prior, builds, trade_date):
            return None
        work = _single_nested_machine_payload(prior["output"], schema=FORMAL_SCORE_WORK_SCHEMA)
        if not work:
            return None
        with (_live_announcement_reply_budget(connection, deadline=deadline, stage="formal-dispatch-publication")
              if mysql else nullcontext()):
            store = FormalAnalysisStore(connection)
            verify_initial_selection(store, work, run_uid=prior["run_uid"], build_sha=prior["build_sha"], trade_date=trade_date)
            _verify_terminal_publication(connection, prior, work, trade_date=trade_date, consumer_started_at=decision)
            manifest = _read_score_manifest(work["score_snapshot"], trade_date=trade_date, store=store)
        origin = manifest["origin"]
        if origin["build_sha"] not in builds or not all(
            _same_scorer(producer, build_sha) for producer in {prior["build_sha"], origin["build_sha"]}
        ):
            return None
        cutoff = _local_decision(work["decision_at"])
        if not datetime.combine(date.fromisoformat(trade_date), STRATEGY_POOL_INITIAL_TIME) <= cutoff <= decision:
            raise FormalScoreContractError("formal recovery dispatch cutoff differs")
        with (_live_announcement_reply_budget(connection, deadline=deadline, stage="formal-dispatch-sources")
              if mysql else nullcontext()):
            sources = frozen_initial_source_runs(connection, trade_date=trade_date, decision_at=cutoff, build_sha=origin["build_sha"])
    if monotonic() >= deadline:
        raise TimeoutError("FORMAL_DISPATCH_INPUTS_90_SECONDS")
    generation = canonical_sha256({
        "schema": "probiga.initial-selection-generation.v1", "trade_date": trade_date,
        "decision_at": cutoff.isoformat(timespec="seconds"), "build_sha": origin["build_sha"],
        "model_version": MODEL_VERSION, "source_runs_sha256": canonical_sha256(sources),
        "min_score": 62.0, "top_n": 80,
    })
    if (set(sources) != _REQUIRED_INPUTS or generation != work["generation_sha256"]
            or float(prior["min_score"]) != 62.0 or int(prior["top_n"]) != 80):
        return None
    return {"decision_at": cutoff, "source_runs": sources}


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
