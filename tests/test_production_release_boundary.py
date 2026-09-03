from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from server.common import adata_release
from tools import validate_production_release_boundary as boundary


ROOT = Path(__file__).resolve().parents[1]


def test_recovery_code_tree_allows_only_exact_sealed_release_manifest(
    tmp_path: Path,
) -> None:
    deploy = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    body = _shell_function_bodies(deploy)[
        "controlled_guard_assert_recovery_code_tree_clean"
    ]
    assert "ls-files --others --exclude-standard -z" in body
    assert 'test "${#untracked_paths[@]}" -eq 1' in body
    assert 'test "${untracked_paths[0]}" = probiga.release.json' in body
    assert 'controlled_guard_assert_file "$manifest_path" 444' in body
    marker = "<<'PY' || return 1\n"
    verifier = body.split(marker, 1)[1].split("\nPY\n", 1)[0]
    expected_release = "a" * 40
    expected_tree = "b" * 64
    core = {
        "schema": "probiga.release-manifest.v1",
        "release_id": expected_release,
        "source_tree_hash": expected_tree,
        "migration_version": "migration-v1",
        "built_at": "2026-09-03T09:30:00+08:00",
        "artifact_hash": "c" * 64,
    }
    seal = hashlib.sha256(
        json.dumps(
            core,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    manifest_path = tmp_path / "probiga.release.json"
    manifest_path.write_text(
        json.dumps({**core, "manifest_sha256": seal}),
        encoding="utf-8",
    )

    valid = subprocess.run(
        [
            sys.executable,
            "-I",
            "-",
            str(manifest_path),
            expected_release,
            expected_tree,
        ],
        input=verifier,
        capture_output=True,
        text=True,
        check=False,
    )
    assert valid.returncode == 0, valid.stderr

    manifest_path.write_text(
        json.dumps({**core, "manifest_sha256": "0" * 64}),
        encoding="utf-8",
    )
    drifted = subprocess.run(
        [
            sys.executable,
            "-I",
            "-",
            str(manifest_path),
            expected_release,
            expected_tree,
        ],
        input=verifier,
        capture_output=True,
        text=True,
        check=False,
    )
    assert drifted.returncode != 0


def test_every_guard_recovery_code_root_uses_sealed_manifest_policy() -> None:
    deploy = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    bodies = _shell_function_bodies(deploy)
    runtime_verifier = bodies["controlled_guard_verify_restored_runtime"]
    database_guard = bodies["controlled_database_guard_recovery"]
    explicit_failure = bodies["explicit_v2_recovery_failure"]

    assert (
        'controlled_guard_assert_recovery_code_tree_clean '
        '"$deferred_code_root" "$deferred_expected_sha"'
        in _normalized_shell(runtime_verifier)
    )
    assert "status --porcelain=v1 --untracked-files=all" not in (
        runtime_verifier
    )
    assert (
        'controlled_guard_assert_recovery_code_tree_clean '
        '"$code_root" "$guarded_sha"'
        in _normalized_shell(database_guard)
    )
    assert "status --porcelain=v1 --untracked-files=all" not in database_guard
    assert "v2 recovery failed step=%s" in explicit_failure
    explicit_dispatch = deploy[deploy.index(
        'if [ "$DEPLOY_OPERATION" = recover-database-guard ]; then',
        deploy.index("explicit_v2_recovery_failure()"),
    ):deploy.index(': "${EXPECTED_SHA:?EXPECTED_SHA is required}"')]
    assert "V2_RECOVERY_STEP=dispatch" in explicit_dispatch
    assert "explicit_v2_recovery_failure" in explicit_dispatch
    assert "trap - ERR" in explicit_dispatch


def test_v2_rollback_recovery_persists_granular_failure_step() -> None:
    deploy = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    bodies = _shell_function_bodies(deploy)
    recovery = _normalized_shell(
        bodies["controlled_v2_rollback_only_recovery"]
    )
    failure = bodies["v2_recovery_failure"]

    ordered_steps = (
        "rollback-validate-writer-directory",
        "rollback-validate-writer-file",
        "rollback-read-writer-state",
        "rollback-validate-writer-line-count",
        "rollback-validate-writer-schema",
        "rollback-validate-writer-release",
        "rollback-parse-writer-main",
        "rollback-parse-writer-scheduler",
        "rollback-parse-writer-ai-service",
        "rollback-parse-writer-ai-timer",
        "rollback-validate-writer-main",
        "rollback-validate-writer-scheduler",
        "rollback-validate-writer-ai-service",
        "rollback-validate-writer-ai-timer",
    )
    positions = [
        recovery.index(f"V2_RECOVERY_STEP={step}") for step in ordered_steps
    ]
    assert positions == sorted(positions)
    for step in (
        "rollback-fast-validate-restore",
        "rollback-fast-assert-old-set",
        "rollback-fast-verify-old-governance",
        "rollback-fast-verify-old-runtime",
        "rollback-fast-remove-restore",
        "rollback-fast-retire-journal",
        "rollback-fast-verify-retired",
        "rollback-validate-restore",
        "rollback-create-restore",
        "rollback-validate-guard",
        "rollback-recreate-guard",
    ):
        assert f"V2_RECOVERY_STEP={step}" in recovery
    assert 'CUTOVER_STEP="v2_${recovery_step//-/_}"' in failure
    assert "CUTOVER_STEP=v2_unknown" in failure
    assert '"$audit_recovery_step" >&7' in failure


def test_old_runtime_verified_cleanup_uses_stable_governance_projection() -> None:
    deploy = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    bodies = _shell_function_bodies(deploy)
    recovery = _normalized_shell(
        bodies["controlled_v2_rollback_only_recovery"]
    )
    capture = _normalized_shell(
        bodies["controlled_guard_capture_current_governance_snapshot"]
    )

    fast_path_start = recovery.index('if [ "$phase" = old-runtime-verified ]')
    fast_path_end = recovery.index("return 0", fast_path_start)
    fast_path = recovery[fast_path_start:fast_path_end]
    assert (
        'controlled_guard_capture_current_governance_snapshot "$guarded_sha" '
        '"$old_runtime_sha" verify-stable'
        in fast_path
    )
    assert 'case "$rollback_verification_action"' in capture
    assert "verify|verify-stable" in capture
    assert capture.count('"$rollback_verification_action" rollback-') == 2


@pytest.mark.parametrize(
    ("recovery_step", "expected_cutover_step"),
    (
        ("rollback-fast-retire-journal", "v2_rollback_fast_retire_journal"),
        ("unsafe step=value", "v2_unknown"),
    ),
)
def test_v2_recovery_failure_maps_only_safe_step_to_failure_audit(
    tmp_path: Path,
    recovery_step: str,
    expected_cutover_step: str,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable recovery audit test")
    deploy = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    failure = _shell_function_bodies(deploy)["v2_recovery_failure"]
    output = tmp_path / "checkpoint"
    harness = f"""
set -u
V2_RECOVERY_STEP={recovery_step!r}
precutover_failure() {{
  printf '%s:%s:%s\\n' "$CUTOVER_STEP" "$1" "$2" > {output.as_posix()!r}
}}
v2_recovery_failure() {{
{failure}
}}
exec 7>/dev/null
v2_recovery_failure 17 8113
"""
    completed = subprocess.run(
        [bash, "-c", harness],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert output.read_text(encoding="utf-8") == (
        f"{expected_cutover_step}:17:8113\n"
    )


def test_read_model_only_release_reuses_completed_strategy_batch() -> None:
    deploy = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )

    assert "server/static/*" in deploy
    assert "server/api/routers/hot_data.py" in deploy
    assert "server/api/routers/holding_strategy.py" in deploy
    assert "server/api/routers/trading_v3.py" in deploy
    assert "server/common/canonical_decision_bridge.py" in deploy
    assert "tests/*" in deploy
    assert "GOVERNANCE_DEPLOYMENT_ONLY=1" in deploy
    assert "strategy_governance reuse_current_completed" in deploy


def test_code_release_does_not_block_on_qmt_history_rescan() -> None:
    deploy = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )

    assert "readonly QMT_HISTORY_DEPLOY_BLOCKING=0" in deploy
    assert (
        "QMT history release scan skipped; data readiness remains "
        "scheduler-owned"
    ) in deploy
    assert (
        "CUTOVER_STEP=resolve_strategy_governance_trade_date_without_history_scan"
        in deploy
    )


class _AdataBoundaryPath:
    def __init__(self, *, uid: int, mode: int) -> None:
        self.uid = uid
        self.mode = mode

    def stat(self):
        return SimpleNamespace(st_uid=self.uid, st_mode=self.mode)


def test_root_broker_evaluates_adata_mutability_as_non_root_service(
    monkeypatch,
) -> None:
    monkeypatch.setattr(adata_release.os, "geteuid", lambda: 0, raising=False)

    assert not adata_release._writable_by_production_service(
        _AdataBoundaryPath(uid=0, mode=0o755)
    )
    assert adata_release._writable_by_production_service(
        _AdataBoundaryPath(uid=0, mode=0o775)
    )
    assert adata_release._writable_by_production_service(
        _AdataBoundaryPath(uid=1000, mode=0o555)
    )


def test_non_root_adata_mutability_check_uses_effective_access(monkeypatch) -> None:
    candidate = _AdataBoundaryPath(uid=0, mode=0o555)
    monkeypatch.setattr(adata_release.os, "geteuid", lambda: 1000, raising=False)
    monkeypatch.setattr(
        adata_release.os,
        "access",
        lambda path, mode: path is candidate and mode == adata_release.os.W_OK,
    )

    assert adata_release._writable_by_production_service(candidate)


def test_scheduler_heartbeat_schema_ddl_uses_only_privileged_fenced_migrator():
    deploy = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    schema_tool = (
        ROOT / "tools/prepare_strategy_governance_schema.py"
    ).read_text(encoding="utf-8")

    assert "migrate_scheduler_runtime_heartbeat.py" not in deploy
    assert "migrate_scheduler_runtime_heartbeat(boundary.migrator_engine)" in (
        schema_tool
    )
    assert "preflight_scheduler_runtime_heartbeat_schema(" in schema_tool
    assert '_preflight_diagnostic_scope("scheduler_runtime_schema")' in (
        schema_tool
    )
    assert "validate_scheduler_runtime_heartbeat_schema(\n                boundary.migrator_engine" in schema_tool


def test_qmt_and_detached_job_state_roots_are_external_and_service_owned():
    deploy = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy)
    bodies = _shell_function_bodies(deploy)
    expected = {
        "prepare_qmt_full_market_history_state_root": (
            "/var/lib/probiga/qmt-full-market-history"
        ),
        "prepare_qmt_local_gap_repair_state_root": (
            "/var/lib/probiga/qmt-local-gap-repair"
        ),
        "prepare_probiga_job_log_root": "/var/lib/probiga/jobs",
    }

    for function_name, state_root in expected.items():
        assert state_root in deploy
        body = _normalized_shell(bodies[function_name])
        assert 'install -d -o "$SERVICE_USER" -g "$SERVICE_USER" -m 0700' in body
        assert "readlink -f" in body
        assert "stat -c '%U:%G'" in body
        assert "stat -c '%a'" in body
        if function_name != "prepare_probiga_job_log_root":
            assert "-perm /7177" in body
        assert function_name in normalized

    job_body = bodies["prepare_probiga_job_log_root"]
    assert "target-turnover-snapshot-v1.json" in job_body
    assert "stock-finance-daily-v2.json" in job_body
    assert "allowed_modes = {0o600, 0o644}" in job_body
    assert "observed.st_nlink == 1" in job_body
    assert "os.fchmod" not in job_body
    assert "chmod 0600" not in job_body

    migration = bodies["migrate_probiga_job_log_legacy_modes"]
    assert "os.O_DIRECTORY" in migration
    assert "os.O_NOFOLLOW" in migration
    assert "dir_fd=directory_fd" in migration
    assert "os.fstat(descriptor)" in migration
    assert "opened.st_dev, opened.st_ino" in migration
    assert "os.fchmod(descriptor, 0o600)" in migration
    assert "os.fsync(descriptor)" in migration
    assert "os.fsync(directory_fd)" in migration
    assert "os.lstat(name, dir_fd=directory_fd)" in migration
    assert "metadata.st_nlink == 1" in migration
    assert "! -links 1" in migration
    assert "chmod 0600" not in migration
    assert "Environment=PROBIGA_JOB_LOG_ROOT=$PROBIGA_JOB_LOG_ROOT" in deploy


def _job_log_mode_migration_python() -> str:
    deploy = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    body = _shell_function_bodies(deploy)[
        "migrate_probiga_job_log_legacy_modes"
    ]
    match = re.search(r"<<'PY' \|\| return 2\n(?P<script>.*?)\nPY", body, re.S)
    assert match is not None
    return match.group("script")


def _run_job_log_mode_migration(root: Path) -> subprocess.CompletedProcess[str]:
    if os.name != "posix" or not hasattr(os, "O_DIRECTORY"):
        pytest.skip("secure openat behavior requires POSIX")
    return subprocess.run(
        [
            sys.executable,
            "-I",
            "-",
            str(root),
            str(os.geteuid()),
            str(os.getegid()),
        ],
        input=_job_log_mode_migration_python(),
        text=True,
        capture_output=True,
        check=False,
    )


def _write_job_log(path: Path, mode: int = 0o600) -> None:
    path.write_text("{}\n", encoding="utf-8")
    path.chmod(mode)


def test_job_log_legacy_modes_are_migrated_only_for_exact_allowlist(
    tmp_path: Path,
) -> None:
    root = tmp_path / "jobs"
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    turnover = root / "target-turnover-snapshot-v1.json"
    finance = root / "stock-finance-daily-v2.json"
    ordinary = root / "scheduler.log"
    _write_job_log(turnover, 0o644)
    _write_job_log(finance, 0o644)
    _write_job_log(ordinary, 0o600)

    completed = _run_job_log_mode_migration(root)

    assert completed.returncode == 0, completed.stderr
    assert stat.S_IMODE(turnover.stat().st_mode) == 0o600
    assert stat.S_IMODE(finance.stat().st_mode) == 0o600
    assert stat.S_IMODE(ordinary.stat().st_mode) == 0o600


@pytest.mark.parametrize(
    ("name", "mode"),
    [
        ("target-turnover-snapshot-v1.json", 0o640),
        ("not-allowlisted.json", 0o644),
    ],
)
def test_job_log_mode_migration_rejects_abnormal_or_unknown_legacy_mode(
    tmp_path: Path,
    name: str,
    mode: int,
) -> None:
    root = tmp_path / "jobs"
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    candidate = root / name
    _write_job_log(candidate, mode)

    completed = _run_job_log_mode_migration(root)

    assert completed.returncode != 0
    assert stat.S_IMODE(candidate.lstat().st_mode) == mode


@pytest.mark.parametrize("entry_kind", ["symlink", "hardlink"])
def test_job_log_mode_migration_rejects_links_without_touching_external_target(
    tmp_path: Path,
    entry_kind: str,
) -> None:
    if os.name != "posix":
        pytest.skip("secure link behavior requires POSIX")
    root = tmp_path / "jobs"
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    external = tmp_path / "external.json"
    _write_job_log(external, 0o644)
    candidate = root / "target-turnover-snapshot-v1.json"
    if entry_kind == "symlink":
        candidate.symlink_to(external)
    else:
        os.link(external, candidate)

    completed = _run_job_log_mode_migration(root)

    assert completed.returncode != 0
    assert stat.S_IMODE(external.stat().st_mode) == 0o644


def test_job_log_mode_migration_rejects_foreign_owner_when_privileged(
    tmp_path: Path,
) -> None:
    if os.name != "posix" or os.geteuid() != 0:
        pytest.skip("foreign-owner behavior requires root")
    root = tmp_path / "jobs"
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    candidate = root / "target-turnover-snapshot-v1.json"
    _write_job_log(candidate, 0o644)
    os.chown(candidate, 1, os.getegid())

    completed = _run_job_log_mode_migration(root)

    assert completed.returncode != 0
    assert stat.S_IMODE(candidate.stat().st_mode) == 0o644


def test_job_log_mode_migration_runs_after_scheduler_and_writer_quiescence() -> None:
    deploy = _normalized_shell(
        (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    )
    stop = deploy.index("CUTOVER_STEP=stop_linux_scheduler_before_writer_quiescence")
    stopped = deploy.index("PRE_CUTOVER_SCHEDULER_STOPPED=1", stop)
    proof = deploy.index(
        'p.get("live_writer_count")==0 and p.get("live_writers")==[]', stopped
    )
    migration = deploy.index(
        "CUTOVER_STEP=migrate_probiga_job_log_legacy_modes_after_writer_quiescence",
        proof,
    )
    call = deploy.index("\nmigrate_probiga_job_log_legacy_modes\n", migration)

    assert stop < stopped < proof < migration < call


def _bash() -> str | None:
    discovered = shutil.which("bash")
    if discovered:
        return discovered
    git_bash = Path(r"C:\Program Files\Git\bin\bash.exe")
    return str(git_bash) if git_bash.is_file() else None


def _normalized_shell(source: str) -> str:
    """Join shell continuations without changing command ordering."""

    return re.sub(r"[ \t]*\\\r?\n[ \t]*", " ", source)


def test_deploy_revalidates_release_identity_after_all_pruning() -> None:
    deploy_script = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    prune_end = deploy_script.rindex("if ! prune_release_temp_files; then")
    post_prune_identity = deploy_script.rindex(
        "CUTOVER_STEP=verify_post_prune_release_identity"
    )
    post_prune_health = deploy_script.rindex("CUTOVER_STEP=verify_post_prune_health")
    persist_receipt = deploy_script.rindex(
        "CUTOVER_STEP=persist_deployed_receipt_pending"
    )
    deploy_succeeded = deploy_script.rindex("DEPLOY_SUCCEEDED=1")
    journal_cleanup = deploy_script.rindex(
        "CUTOVER_STEP=remove_finalized_activation_journal"
    )

    assert prune_end < post_prune_identity < post_prune_health < persist_receipt
    assert persist_receipt < deploy_succeeded < journal_cleanup
    final_validation = deploy_script[post_prune_identity:persist_receipt]
    assert 'release_identity_check 1 "$PREPARED_CODE_ROOT"' in final_validation
    assert "http://127.0.0.1/api/health" in final_validation
    assert "http://127.0.0.1/api/health/runtime" in final_validation


def _shell_function_bodies(source: str) -> dict[str, str]:
    return {
        match.group("name"): match.group("body")
        for match in re.finditer(
            r"(?ms)^(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)\(\) \{\n"
            r"(?P<body>.*?)^[ \t]*\}\s*$",
            source,
        )
    }


def _shell_function_closure(
    function_bodies: dict[str, str], root_name: str
) -> str:
    pending = [root_name]
    visited: set[str] = set()
    bodies: list[str] = []
    while pending:
        name = pending.pop()
        if name in visited or name not in function_bodies:
            continue
        visited.add(name)
        body = function_bodies[name]
        bodies.append(body)
        pending.extend(
            token
            for token in re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", body)
            if token in function_bodies
        )
    return "\n".join(bodies)


def _required_shell_position(source: str, pattern: str) -> int:
    match = re.search(pattern, source, flags=re.MULTILINE)
    assert match is not None, f"missing shell contract: {pattern}"
    return match.start()


def test_production_boundary_pass_means_activation_stays_blocked() -> None:
    result = boundary.validate_production_boundary(require_git_anchor=False)

    assert result["deployment_safety"] == "PASS"
    assert result["activation_readiness"] == "BLOCK"
    assert result["database"] == {
        "status": "DEFERRED_MIGRATION_IN_PROGRESS",
        "tests_run": False,
        "counts_as_pass": False,
    }
    assert all(
        item["activation_eligible"] is False
        for item in result["research_releases"]
    )


def test_production_boundary_rejects_a_real_order_route(monkeypatch) -> None:
    real_reader = boundary._strict_json
    original = real_reader(boundary.ROOT / "strategies/trading_v3.json")
    forged = deepcopy(original)
    forged["automatic_real_order_submission"] = True

    monkeypatch.setattr(boundary, "_strict_json", lambda _path: forged)
    with pytest.raises(boundary.ProductionBoundaryError, match="real-order"):
        boundary._validate_current_production_route()


def test_production_packages_are_absent_from_runtime_imports() -> None:
    assert boundary._validate_no_production_imports() > 0


@pytest.mark.parametrize(
    "source",
    [
        "from server import trading_v4",
        "import server.trading_v5.models as models",
        "import importlib as il\nil.import_module('server.' + 'trading_v6.models')",
        "from importlib import import_module as load\nload('tools.trading_v4')",
        "__import__('server.trading_v6.pit_finance')",
        "from builtins import __import__ as load\nload('server.trading_v4')",
        "import importlib\nload = importlib.import_module\nload('server.trading_v5')",
        "import importlib\ngetattr(importlib, 'import_module')('server.trading_v6')",
        "import runpy\nrunpy.run_module('server.trading_v4')",
        "import subprocess\nsubprocess.run(['python', '-m', 'server.trading_v5'])",
    ],
)
def test_static_import_scan_rejects_cross_version_bypasses(source) -> None:
    found = boundary._forbidden_imports_from_source(
        source,
        module_name="server.api.example",
    )
    assert found


@pytest.mark.parametrize(
    "source",
    [
        "import sys\nfrom pathlib import Path\nROOT=Path('.')\nsys.path.insert(0, str(ROOT/'adata'))",
        "build_child_env(ROOT, extra_python_paths=[ROOT / 'adata'])",
        "import sys\nsys.path.insert(0, '/repo')\nfrom adata.sentiment.hot import Hot",
    ],
)
def test_static_scan_rejects_mutable_adata_path_injection(source) -> None:
    assert boundary._mutable_adata_path_injections_from_source(source)


def test_boundary_independently_rejects_open_runtime(monkeypatch) -> None:
    real_reader = boundary._strict_json

    def altered(path):
        document = real_reader(path)
        if path.as_posix().endswith("trading_v6.0.0-research/runtime.json"):
            document = deepcopy(document)
            document["execution_boundary"]["real_orders_allowed"] = True
        return document

    monkeypatch.setattr(boundary, "_strict_json", altered)
    releases = (
        boundary.validate_v4_release(),
        boundary.validate_v5_release(),
        boundary.validate_v6_release(),
    )
    with pytest.raises(boundary.ProductionBoundaryError, match="execution boundary"):
        for release in releases:
            boundary._validate_runtime_boundary(release.document)


def test_boundary_accepts_closed_v4_runtime_schema() -> None:
    release = boundary.validate_v4_release()
    boundary._validate_runtime_boundary(release.document)


def test_git_delivery_set_uses_dependency_paths_not_mapping_names() -> None:
    releases = (
        boundary.validate_v4_release(),
        boundary.validate_v5_release(),
        boundary.validate_v6_release(),
    )
    required = boundary._required_release_files(releases)

    assert "tools/research_trading_v4_ml_campaign.py" in required
    assert "legacy_runner" not in required


def test_deploy_workflow_pins_identity_environment_and_rollback_contracts() -> None:
    workflow_source = (ROOT / ".github/workflows/deploy.yml").read_text(
        encoding="utf-8"
    )
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    root_broker = (ROOT / "deploy/production_deploy_root.sh").read_text(
        encoding="utf-8"
    )
    workflow = workflow_source + "\n" + root_broker + "\n" + deploy_script
    deploy_job = workflow_source[workflow_source.index("\n  deploy:"):]

    # The exact-data completion suite adds three bounded, directly auditable
    # pytest batches while keeping the workflow comfortably below GitHub's
    # practical size limits.
    assert len(workflow_source) < 34_000
    assert "envs: EXPECTED_SHA" in deploy_job
    assert (
        'sudo -n /usr/local/sbin/probiga-production-deploy "$EXPECTED_SHA"'
        in _normalized_shell(deploy_job)
    )
    for forbidden in (
        "RESOLVED_REQUIREMENTS_B64",
        "EXPECTED_REQUIREMENTS_SHA256",
        "EXPECTED_ADATA_SHA",
        "EXPECTED_ADATA_TREE_SHA256",
        "resolved_requirements_b64:",
        "resolved_requirements_sha256:",
    ):
        assert forbidden not in deploy_job
    assert "${{" not in deploy_script
    assert "actions/checkout@v4" not in workflow
    assert "actions/setup-python@v5" not in workflow
    assert "appleboy/ssh-action@v1.0.3" not in workflow
    assert "SERVER_HOST_FINGERPRINT must be an exact SHA256 fingerprint" in workflow
    assert "^SHA256:[A-Za-z0-9+/]{43}$" in workflow
    assert "vars.PRODUCTION_DEPLOY_ENABLED == 'true'" in workflow_source
    assert "vars.PRODUCTION_DEPLOY_ENABLED != 'true'" in workflow_source
    assert "Production deployment disabled" in workflow_source
    assert "this revision was **not deployed**" in workflow_source
    assert "SERVER_HOST: ${{ secrets.SERVER_HOST }}" in workflow_source
    assert "SERVER_HOST: 47.113.123.190" not in workflow_source
    assert 'test "$SERVER_USER" != root' in workflow
    assert 'test "$SERVICE_USER" != root' in workflow
    assert 'WorkingDirectory=/opt/ProBigA' in deploy_script
    assert "Environment=GIT_OPTIONAL_LOCKS=0" in deploy_script
    assert "git config --system" not in deploy_script
    assert "/usr/bin/git --no-replace-objects" in deploy_script
    assert "GIT_CONFIG_NOSYSTEM=1" in deploy_script
    assert "seal_release_checkout" in deploy_script
    assert "git ls-files --stage -z" in deploy_script
    assert "git clean" not in deploy_script
    assert "probiga-production-deploy" in workflow
    assert "DEPLOY_LOCK_ROOT=/run/probiga" in deploy_script
    assert (
        'DEPLOY_LOCK_FILE="$DEPLOY_LOCK_ROOT/production-deploy.lock"'
        in deploy_script
    )
    assert 'exec 9>"$DEPLOY_LOCK_FILE"' in deploy_script
    assert "flock -n 9" in deploy_script
    assert "validate_hashed_requirements_lock" in deploy_script
    assert "prepare_trusted_wheelhouse" in deploy_script
    assert "CODE_RELEASE_ROOT=/opt/ProBigA-releases" in deploy_script
    assert "RELEASE_VENV_ROOT=/var/lib/probiga/release-venvs" in deploy_script
    assert not re.search(
        r"(?m)^RELEASE_VENV_ROOT=/opt/ProBigA/\.release_venvs$",
        deploy_script,
    )
    assert (
        "LEGACY_RELEASE_VENV_ROOT=/opt/ProBigA/.release_venvs"
        in deploy_script
    )
    assert 'install -d -o root -g root -m 0755 "$RELEASE_VENV_ROOT"' in deploy_script
    assert 'chmod 0555 "$RELEASE_VENV_ROOT"' in deploy_script
    assert 'sudo -u "$SERVICE_USER" test -x "$RELEASE_VENV_ROOT"' in deploy_script
    assert "RELEASE_VENV_RETENTION=2" in deploy_script
    assert 'prune_release_venvs "$EXPECTED_SHA"' in deploy_script
    assert (
        'prune_code_releases "$PREPARED_CODE_ROOT" "$PREVIOUS_CODE_ROOT"'
        in deploy_script
    )
    assert (
        "Warning: release venv cleanup failed before final verification"
        in deploy_script
    )
    assert (
        "Warning: immutable code release cleanup failed before final verification"
        in deploy_script
    )
    assert (
        "Warning: release temp cleanup failed before final verification"
        in deploy_script
    )
    assert "prune_code_releases" in deploy_script
    assert "prune_release_temp_files" in deploy_script
    assert 'test "$(dirname -- "$build_real")" = "$RELEASE_VENV_ROOT"' in deploy_script
    assert 'path_is_runtime_referenced "$build_real"' in deploy_script
    assert "timeout-minutes: 165" in workflow
    assert "command_timeout: 150m" in workflow
    assert "probiga.deploy-receipt.v4" in workflow
    assert '"expected_input_lock_sha256":"%s"' in workflow
    assert '"previous_input_lock_sha256":"%s"' in workflow
    assert '"active_input_lock_sha256":"%s"' in workflow
    assert '"expected_resolved_freeze_sha256":"%s"' in workflow
    assert '"active_resolved_freeze_sha256":"%s"' in workflow
    assert '"requirements_sha256":"%s"' not in workflow
    assert 'sudo chmod 0700 "$RECEIPT_DIR"' in workflow
    assert 'sudo mktemp' in workflow
    assert "persist_deployed_receipt_pending" in deploy_script
    assert "publish_deployed_receipt_pending" in deploy_script
    assert 'mv -fT "$pending_tmp" "$ACTIVATION_RECEIPT_PENDING"' in deploy_script
    assert 'mv -fT "$receipt_tmp" "$receipt_target"' in deploy_script
    assert 'sudo tee "$RECEIPT_DIR/$RECEIPT_ID.json"' not in workflow
    assert 'clean_git_ssh ls-remote "$TRUSTED_REMOTE" refs/heads/main' in root_broker
    assert (
        'clean_git_ssh --git-dir="$CODE_GIT_CACHE" fetch --no-tags origin '
        '"+refs/heads/main:refs/remotes/origin/main"'
        in _normalized_shell(root_broker)
    )
    assert 'REMOTE_SHA="$(clean_git_ssh ls-remote' in root_broker
    assert (
        'git --git-dir="$CODE_GIT_CACHE" worktree add --detach'
        in _normalized_shell(deploy_script)
    )
    assert "trap 'rollback 143' TERM" in workflow
    assert "trap 'rollback 129' HUP" in workflow
    assert workflow.count("--retry-all-errors") >= 2
    assert 'if [ "$rollback_failed" -ne 0 ]; then' in workflow
    assert 'write_receipt "ROLLBACK_FAILED"' in workflow
    assert 'write_receipt "ROLLED_BACK"' in workflow


def test_deploy_git_trust_is_exact_process_local_and_available_from_start() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    git_body = _shell_function_bodies(deploy_script)["git"]

    repository_assignment = deploy_script.index(
        "REPOSITORY_ROOT=/opt/ProBigA"
    )
    git_wrapper = deploy_script.index("\ngit() {")
    first_legacy_git_read = deploy_script.index(
        'LEGACY_LIVE_SHA="$(git rev-parse HEAD)"'
    )

    assert repository_assignment < git_wrapper < first_legacy_git_read
    assert '-c "safe.directory=$REPOSITORY_ROOT"' in git_body
    assert git_body.index('-c "safe.directory=$REPOSITORY_ROOT"') < (
        git_body.index('"$@"')
    )
    assert "safe.directory=*" not in deploy_script
    assert "safe.directory='*'" not in deploy_script
    assert "safe.directory=\"*\"" not in deploy_script
    assert "git config --global" not in deploy_script
    assert "git config --system" not in deploy_script


def test_root_broker_has_exact_normal_and_recovery_argv_contracts() -> None:
    broker = (ROOT / "deploy/production_deploy_root.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(broker)
    parser = _normalized_shell(_shell_function_bodies(broker)[
        "parse_broker_invocation"
    ])

    assert 'case "$#" in' in parser
    assert "1)" in parser
    assert "2)" in parser
    assert 'test "$1" = --recover-database-guard' in parser
    assert "BROKER_OPERATION=deploy" in parser
    assert "BROKER_OPERATION=recover-database-guard" in parser
    assert "expected one trusted-main SHA or an exact guard recovery request" in parser
    assert '[[ "$EXPECTED_RECOVERY_GUARD_SHA" =~ ^[0-9a-f]{40}$ ]]' in parser
    assert 'test "$RECORDED_RECOVERY_SHA" = "$EXPECTED_RECOVERY_GUARD_SHA"' in broker
    assert "probiga.database-writer-guard.v2" in broker
    assert "probiga.database-writer-restore.v1" in broker
    assert 'stat -c \'%U:%G\'' in broker
    assert 'stat -c \'%a\'' in broker
    assert '"${GIT[@]}" show "${EXPECTED_SHA}:deploy/production_deploy.sh"' in normalized
    assert (
        '"${GIT[@]}" show "${EXPECTED_RECOVERY_TOOL_SHA}:'
        'deploy/production_deploy.sh"'
        in normalized
    )
    assert 'EXPECTED_RECOVERY_TOOL_SHA="$(' in normalized
    assert '"${GIT[@]}" rev-parse refs/remotes/origin/main' in normalized
    assert (
        '"${GIT[@]}" merge-base --is-ancestor '
        '"$EXPECTED_RECOVERY_GUARD_SHA" "$EXPECTED_RECOVERY_TOOL_SHA"'
        in normalized
    )
    assert "RECOVERY_PROTOCOL_VERSION=probiga-database-guard-recovery-v2" in broker
    assert "REQUIRED_RECOVERY_PROTOCOL=probiga-database-guard-recovery-v2" in (
        ROOT / "deploy/production_deploy.sh"
    ).read_text(encoding="utf-8")
    recovery_command = (
        '/usr/bin/bash --noprofile --norc "$BOOTSTRAP_FILE" '
        "--recover-database-guard"
    )
    assert recovery_command in normalized
    assert normalized.count(recovery_command) == 1
    assert 'bash "$BOOTSTRAP_FILE" "$@"' not in normalized

    recovery_execution = normalized[
        normalized.rindex("else\n  /usr/bin/env -i"):
    ]
    assert "IFS= read -r RESOLVED_REQUIREMENTS_B64" not in recovery_execution
    assert "RESOLVED_REQUIREMENTS_B64=" not in recovery_execution
    assert 'PROBIGA_RECOVERY_GUARD_SHA="$EXPECTED_RECOVERY_GUARD_SHA"' in recovery_execution
    assert 'PROBIGA_RECOVERY_TOOL_SHA="$EXPECTED_RECOVERY_TOOL_SHA"' in recovery_execution


def test_root_broker_argument_parser_rejects_every_other_shape() -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the broker argv regression")
    broker = (ROOT / "deploy/production_deploy_root.sh").read_text(
        encoding="utf-8"
    )
    parser_body = _shell_function_bodies(broker)["parse_broker_invocation"]
    harness = (
        "set -u\n"
        "fail() { printf 'ERROR:%s\\n' \"$*\" >&2; exit 2; }\n"
        "parse_broker_invocation() {\n"
        + parser_body
        + "}\n"
        "parse_broker_invocation \"$@\"\n"
        "printf '%s|%s|%s|%s|%s|%s\\n' \"$BROKER_OPERATION\" "
        "\"$EXPECTED_SHA\" \"$EXPECTED_INPUT_LOCK_SHA256\" "
        "\"$EXPECTED_ADATA_SHA\" \"$EXPECTED_ADATA_TREE_SHA256\" "
        "\"$EXPECTED_RECOVERY_GUARD_SHA\"\n"
    )
    sha = "a" * 40
    digest = "b" * 64
    adata_sha = "c" * 40
    tree = "d" * 64

    normal = subprocess.run(
        [bash, "-c", harness, "broker-test", sha],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert normal.returncode == 0, normal.stderr
    assert normal.stdout.strip() == f"deploy|{sha}||||"

    recovery = subprocess.run(
        [bash, "-c", harness, "broker-test", "--recover-database-guard", sha],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert recovery.returncode == 0, recovery.stderr
    assert recovery.stdout.strip() == f"recover-database-guard|||||{sha}"

    capability = subprocess.run(
        [bash, "-c", harness, "broker-test", "--capabilities"],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert capability.returncode == 0, capability.stderr
    assert capability.stdout.strip() == "capabilities|||||"

    invalid_argv = (
        (),
        ("A" * 40,),
        ("--recover-database-guard",),
        ("--unknown", sha),
        ("--recover-database-guard", "A" * 40),
        (sha, digest),
        (sha, digest, adata_sha),
        (sha, digest, adata_sha, tree),
        (sha, digest, adata_sha, tree, "extra"),
        ("--recover-database-guard", digest, adata_sha, tree),
    )
    for argv in invalid_argv:
        rejected = subprocess.run(
            [bash, "-c", harness, "broker-test", *argv],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert rejected.returncode == 2, (argv, rejected.stdout, rejected.stderr)


def test_v4_workflow_cannot_request_privileged_guard_recovery() -> None:
    workflow = (ROOT / ".github/workflows/deploy.yml").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(workflow)
    deploy_job = workflow[workflow.index("\n  deploy:"):]

    assert "workflow_dispatch:" not in workflow
    assert "recover_database_guard:" not in workflow
    assert "environment: production" in workflow
    assert "timeout-minutes: 165" in workflow
    assert "command_timeout: 150m" in workflow
    assert "--recover-database-guard" not in normalized
    assert (
        'sudo -n /usr/local/sbin/probiga-production-deploy "$EXPECTED_SHA"'
        in normalized
    )
    assert "envs: EXPECTED_SHA" in deploy_job
    assert workflow.count("python tools/scan_tracked_secrets.py") == 2
    before_scan = workflow.index(
        "Scan tracked secrets before dependency installation"
    )
    dependency_install = workflow.index("Install regression dependencies")
    after_scan = workflow.index(
        "Scan tracked secrets after dependency installation"
    )
    assert before_scan < dependency_install < after_scan


def test_dynamic_governance_completion_regression_inventory_is_frozen() -> None:
    workflow = (ROOT / ".github/workflows/deploy.yml").read_text(
        encoding="utf-8"
    )
    start = workflow.index(
        "- name: Run dynamic strategy governance foundation regressions"
    )
    end = workflow.index(
        "- name: Run QMT and point-in-time truth regressions", start
    )
    steps = workflow[start:end]
    expected = [
        "tests/test_prepare_strategy_governance_schema.py",
        "tests/test_strategy_center.py",
        "tests/test_strategy_execution_contracts.py",
        "tests/test_strategy_governance_roles.py",
        "tests/test_sync_strategy_industry_history.py",
        "tests/test_trading_v3_truth_risk.py",
        "tests/test_dynamic_shadow_ledger.py",
        "tests/test_dynamic_shadow_schema_upgrade.py",
        "tests/test_strategy_center_membership_history.py",
        "tests/test_strategy_challenger_factory.py",
        "tests/test_strategy_center_error_log_sanitization.py",
        "tests/test_strategy_funding_checkpoint.py",
        "tests/test_strategy_funding_checkpoint_mysql84_metadata.py",
        "tests/test_strategy_funding_detail_api.py",
        "tests/test_strategy_governance_orchestrator.py",
        "tests/test_strategy_governance_orchestrator_api.py",
        "tests/test_strategy_governance_write_contract.py",
        "tests/test_strategy_governance_history_paging.py",
        "tests/test_strategy_membership_api_truth.py",
        "tests/test_strategy_metric_artifact_paging.py",
        "tests/test_prepare_strategy_governance_deferred_schema.py",
        "tests/test_strategy_metric_artifact_size_limit.py",
        "tests/test_strategy_statistical_guards.py",
        "tests/test_strategy_governance_deferred_mode.py",
        "tests/test_strategy_governance_deferred_scheduler.py",
        "tests/test_production_schema_evidence_validators.py",
        "tests/test_api_generic_error_sanitization.py",
        "tests/test_trading_v2_error_sanitization.py",
    ]
    observed = re.findall(
        r"(?m)^\s+(tests/[A-Za-z0-9_./-]+\.py)\s*$",
        steps,
    )

    assert observed == expected


def test_production_health_parser_freezes_compact_funding_and_exact40() -> None:
    deploy = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    body = deploy[
        deploy.index("controlled_guard_parse_governance_health_result() {"):
        deploy.index("controlled_guard_parse_governance_cutover_result() {")
    ]

    for required in (
        'manifest_detail.get("strategy_checkpoint_count")',
        'manifest_detail.get("combination_recipe_count")',
        'manifest_detail.get("funding_ready_count")',
        '"checkpoint_root_hash"',
        '"combination_recipe_root_hash"',
        '"ineligible_root_hash"',
        'metric_trigger_detail.get("core_metric_review_contract_hash")',
        'append_trigger_detail.get("core_metric_review_contract_hash")',
    ):
        assert required in body
    for frozen_hash in (
        "47b44f4c1e5201b4ea7cd51f61073fdb4229c245214685c338e24809435a7bde",
        "5a1a19e0664c715ae0cac7cfa8dd87c47da1b63b1d2df869561cecf3c995f01f",
        "bf537f9ed5fb1d31195092ae6a24262511de6f45bf9addacefebc88e25b6b9d8",
        "c217a42eb6c2a5f7bed592bb7c7e724499546f997061c4daad1db957317bdf28",
        "1fcde61ce5a5ea0cc16f1910d94da431d044c667383fafd2224217709f555943",
        "0dbaa644427139c472bab0c3f719d78bd292bb6a7726a0f0ef195adc2e37fa84",
    ):
        assert frozen_hash in body


def test_retired_v2_protocol_fails_before_lock_or_cutover_side_effects() -> None:
    engine = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    protocol_guard = engine.index(
        'case "${PROBIGA_DEPLOY_PROTOCOL_VERSION:-}" in'
    )
    assert "RETIRED_DEPLOY_PROTOCOL_V2=probiga-production-deploy-v2" in engine
    assert "COMPATIBLE_DEPLOY_PROTOCOL_V2" not in engine
    retired_start = engine.index(
        '"$RETIRED_DEPLOY_PROTOCOL_V2")', protocol_guard
    )
    retired_end = engine.index(";;", retired_start)
    retired_branch = engine[retired_start:retired_end]

    assert "DEPLOY_ARTIFACT_MODE=" not in retired_branch
    assert "exit 2" in retired_branch
    assert re.search(
        r"(?is)v2.*(?:retired|unsupported|not supported)", retired_branch
    )
    for first_side_effect in (
        'install -d -o root -g root -m 0700 "$DEPLOY_LOCK_ROOT"',
        'exec 9>"$DEPLOY_LOCK_FILE"',
        "flock -n 9",
    ):
        assert retired_end < engine.index(first_side_effect)


def test_production_deploy_publishes_an_immutable_code_release() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy_script)
    function_bodies = _shell_function_bodies(deploy_script)

    assert (
        "CODE_GIT_CACHE=/var/lib/probiga/release-sources/probiga.git"
        in deploy_script
    )
    assert "CODE_RELEASE_ROOT=/opt/ProBigA-releases" in deploy_script
    assert (
        'STAGING_WORKTREE="$CODE_RELEASE_ROOT/.build-$EXPECTED_SHA-$RANDOM"'
        in deploy_script
    )
    assert (
        'PREPARED_CODE_ROOT="$CODE_RELEASE_ROOT/$EXPECTED_SHA"'
        in deploy_script
    )
    assert "RELEASE_VENV_ROOT=/var/lib/probiga/release-venvs" in deploy_script
    assert (
        "LEGACY_RELEASE_VENV_ROOT=/opt/ProBigA/.release_venvs"
        in deploy_script
    )

    stage_add = re.search(
        r'git --git-dir="\$CODE_GIT_CACHE" worktree add --detach\s+'
        r'"\$STAGING_WORKTREE" "\$EXPECTED_SHA"',
        normalized,
    )
    assert stage_add is not None

    prepare_body = _normalized_shell(function_bodies["prepare_release"])
    code_prepare = prepare_body.index("prepare_code_staging")
    venv_prepare = prepare_body.index("prepare_release_venv")
    seal = prepare_body.index('seal_release_checkout "$STAGING_WORKTREE"')
    immutability = prepare_body.index(
        'assert_service_cannot_write_release_paths "$STAGING_WORKTREE"',
        seal,
    )
    boundary_check = prepare_body.index(
        "tools/validate_production_release_boundary.py"
    )
    delivery_check = prepare_body.index(
        "tools/ensure_quality_gate.py --validate-review-delivery"
    )
    identity_check = prepare_body.index(
        'release_identity_check 1 "$CODE_VALIDATION_ROOT"'
    )
    publish = prepare_body.index(
        'git --git-dir="$CODE_GIT_CACHE" worktree move '
        '"$STAGING_WORKTREE" "$PREPARED_CODE_ROOT"'
    )
    disarm_cleanup = prepare_body.index('STAGING_WORKTREE=""', publish)
    assert code_prepare < venv_prepare
    assert venv_prepare < boundary_check < delivery_check < identity_check
    assert identity_check < seal < immutability < publish < disarm_cleanup

    cleanup_body = _normalized_shell(
        function_bodies["cleanup_staging_worktree"]
    )
    assert re.search(
        r'"\$CODE_RELEASE_ROOT"/\.build-(?:"\$EXPECTED_SHA"-)?\*',
        cleanup_body,
    )
    assert re.search(
        r'worktree remove --force\s+"\$STAGING_WORKTREE"', cleanup_body
    )
    assert 'rm -rf -- "$STAGING_WORKTREE"' in cleanup_body
    assert "$PREPARED_CODE_ROOT" not in cleanup_body
    assert 'rm -rf -- "$PREPARED_CODE_ROOT"' not in deploy_script
    assert not re.search(
        r'worktree remove --force\s+"\$PREPARED_CODE_ROOT"', normalized
    )

    exit_handlers = re.findall(
        r"(?m)^trap\s+(?P<handler>.+?)\s+EXIT\s*$",
        deploy_script,
    )
    assert any("release_lock" in handler for handler in exit_handlers)
    release_lock_closure = _shell_function_closure(
        function_bodies, "release_lock"
    )
    assert "cleanup_staging_worktree" in release_lock_closure


def test_production_deploy_bounds_isolated_dependency_downloads() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy_script)

    assert "readonly DEPENDENCY_DOWNLOAD_TIMEOUT=30m" in deploy_script
    bounded_download = (
        "/usr/bin/timeout --signal=TERM --kill-after=10s "
        '"$DEPENDENCY_DOWNLOAD_TIMEOUT" '
        '"$BOOTSTRAP_PYTHON" -I -m pip download'
    )
    assert normalized.count(bounded_download) == 2


def test_production_deploy_finishes_slow_prepare_before_cutover_fence() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy_script)
    function_bodies = _shell_function_bodies(deploy_script)
    prepare_body = _normalized_shell(function_bodies["prepare_release"])
    prepare_closure = _normalized_shell(
        _shell_function_closure(function_bodies, "prepare_release")
    )
    initial_preflight = _normalized_shell(
        function_bodies["run_initial_database_schema_preflight"]
    )

    required_prepare_commands = (
        'git --git-dir="$CODE_GIT_CACHE" worktree add',
        '-m venv "$EXPECTED_BUILD"',
        'clean_root_pip "$EXPECTED_BUILD/bin/python" install ',
        '"$EXPECTED_BUILD/bin/python" -I -m pip wheel --no-deps ',
        "tools/validate_production_release_boundary.py",
        "tools/ensure_quality_gate.py --validate-review-delivery",
        'release_identity_check 1 "$CODE_VALIDATION_ROOT"',
        'git --git-dir="$CODE_GIT_CACHE" worktree move '
        '"$STAGING_WORKTREE" "$PREPARED_CODE_ROOT"',
    )
    for command in required_prepare_commands:
        assert command in prepare_closure, f"slow prepare omitted: {command}"
    assert (
        "tools/ensure_quality_gate.py --task-type analysis_premarket_external"
        not in prepare_closure
    )
    assert "cleanup_staging_worktree" not in prepare_body

    prepare_definition = deploy_script.index("prepare_release() {")
    prepare_calls = [
        match.start()
        for match in re.finditer(
            r"(?m)^[ \t]*prepare_release[ \t]*$", deploy_script
        )
        if match.start() > prepare_definition
    ]
    assert len(prepare_calls) == 1
    prepare_call = prepare_calls[0]
    schema_prepare = deploy_script.index(
        "CUTOVER_STEP=initial_database_schema_preflight",
        prepare_call,
    )
    schema_prepare_command = deploy_script.index(
        "run_initial_database_schema_preflight",
        schema_prepare,
    )
    deferred_dispatch = deploy_script.index(
        'if [ "$STRATEGY_GOVERNANCE_MODE" = DEFERRED_DB ]; then',
        schema_prepare_command,
    )
    cutover_fence = deploy_script.index("CUTOVER_STARTED=1", prepare_call)
    first_cutover_stop = _required_shell_position(
        deploy_script[cutover_fence:], r"sudo systemctl stop\b"
    ) + cutover_fence
    assert (
        prepare_call
        < schema_prepare
        < schema_prepare_command
        < deferred_dispatch
        < cutover_fence
        < first_cutover_stop
    )
    assert "prepare_strategy_governance_schema.py" in initial_preflight
    assert "--phase preflight" in initial_preflight
    assert "validate_initial_database_schema_preflight_json" in initial_preflight
    assert "prepare_strategy_governance_deferred_schema.py" not in initial_preflight
    pre_cutover = _normalized_shell(
        deploy_script[schema_prepare:cutover_fence]
    )
    assert "prepare_strategy_governance_qmt_history.py" not in pre_cutover
    assert "sync_guojin_qmt_reference_data.py" not in pre_cutover
    assert "sync_qmt_announcement_pit.py" not in pre_cutover
    assert "--apply" not in pre_cutover
    assert "--schema-only" not in deploy_script[schema_prepare:cutover_fence]
    assert "DATABASE_FORWARD_MIGRATION_STARTED=1" not in deploy_script[
        schema_prepare:cutover_fence
    ]

    rollback_start = deploy_script.index("rollback() {")
    rollback_cutover = deploy_script.index(
        'if [ "$CUTOVER_STARTED" -eq 0 ]; then', rollback_start
    )
    rollback_failure_path_end = deploy_script.index(
        'echo "Deployment failed; rolling back to $PREVIOUS_SHA"',
        rollback_cutover,
    )
    prepare_failure_path = deploy_script[
        rollback_cutover:rollback_failure_path_end
    ]
    scheduler_restore = prepare_failure_path.index(
        'if [ "${PRE_CUTOVER_SCHEDULER_STOPPED:-0}" -eq 1 ]'
    )
    scheduler_restore_end = prepare_failure_path.index(
        "PRE_CUTOVER_SCHEDULER_STOPPED=0", scheduler_restore
    )
    assert "sudo systemctl start probiga-scheduler" in prepare_failure_path[
        scheduler_restore:scheduler_restore_end
    ]
    assert "sudo systemctl stop probiga-scheduler" in prepare_failure_path[
        scheduler_restore:scheduler_restore_end
    ]
    assert "systemctl stop" not in prepare_failure_path[
        scheduler_restore_end:
    ]
    assert 'write_receipt "PREPARATION_FAILED"' in prepare_failure_path
    assert 'exit "$failed_status"' in prepare_failure_path
    assert (
        "Forward-only QMT schema preparation may remain installed"
        in prepare_failure_path
    )
    pre_cutover_runtime = deploy_script[schema_prepare:cutover_fence]
    assert pre_cutover_runtime.count(
        "sudo systemctl stop probiga-scheduler"
    ) == 1
    assert 'sudo systemctl stop "$MAIN_SERVICE"' not in pre_cutover_runtime

    assert 'DEPLOY_MAIN_BASHPID="$BASHPID"' in deploy_script
    child_guard = deploy_script[rollback_start:rollback_cutover]
    assert 'if [ "$BASHPID" != "$DEPLOY_MAIN_BASHPID" ]; then' in child_guard
    assert "trap - ERR TERM INT" in child_guard
    assert 'exit "$failed_status"' in child_guard
    assert "systemctl stop" not in child_guard

    rollback_err_trap = "trap 'rollback \"$?\" \"$LINENO\"' ERR"
    assert normalized.count(rollback_err_trap) == 1
    assert 'local failed_line="${2:-0}"' in deploy_script
    assert (
        "deploy_failure phase=cutover cutover_step=%s line=%s status=%s"
        in deploy_script
    )
    assert "trap 'rollback 143' TERM" in normalized
    assert "trap 'rollback 130' INT" in normalized
    assert "trap 'rollback 129' HUP" in normalized
    assert not re.search(
        r"(?m)^trap\s+[^\n]*rollback[^\n]*\sEXIT\s*$", deploy_script
    )


def test_production_deploy_can_recover_from_external_writer_block() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )

    recovery_start = deploy_script.index(
        'PREVIOUS_MAIN_STATE="$(systemctl show "$MAIN_SERVICE"'
    )
    recovery_end = deploy_script.index(
        "runtime_environment_value() {", recovery_start
    )
    recovery = deploy_script[recovery_start:recovery_end]

    assert "inactive|failed" in recovery
    assert '"$PREVIOUS_DROPIN_PRESENT" -ne 1' in recovery
    assert "API_EMBEDDED_SCHEDULER_ENABLED=false" in recovery
    assert "systemctl is-active --quiet probiga-scheduler" in recovery
    assert "systemctl is-enabled --quiet probiga-scheduler" in recovery
    assert 'sudo systemctl start "$MAIN_SERVICE"' in recovery
    assert "http://127.0.0.1/api/health/runtime" in recovery
    assert recovery.count(
        'PREVIOUS_MAIN_PID="$(systemctl show "$MAIN_SERVICE"'
    ) == 1
    assert "recovered probiga service did not expose a valid main PID" in recovery


def test_main_service_downtime_only_runs_bounded_activation_work() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy_script)
    cutover = normalized.index("trap 'rollback \"$?\" \"$LINENO\"' ERR")
    api_stop = _required_shell_position(
        normalized[cutover:],
        r'sudo systemctl stop (?:"\$MAIN_SERVICE"|probiga)(?:[ \t]|$)',
    ) + cutover
    service_start = _required_shell_position(
        normalized[api_stop:],
        r'sudo systemctl start (?:"\$MAIN_SERVICE"|probiga)(?:[ \t]|$)',
    ) + api_stop
    downtime = normalized[api_stop:service_start]
    function_bodies = _shell_function_bodies(deploy_script)
    downtime_closure = downtime + _normalized_shell(
        _shell_function_closure(function_bodies, "install_prepared_dropins")
    )

    writer_fence_start = normalized.index(
        "CUTOVER_STEP=writer_fence_before_api_stop", cutover
    )
    qmt_edge_request = normalized.index(
        "CUTOVER_STEP=request_qmt_windows_edge_before_service_stop", cutover
    )
    writer_fence_end = normalized.index(
        "CUTOVER_STEP=stop_auxiliary_writers", writer_fence_start
    )
    writer_fence = normalized[writer_fence_start:writer_fence_end]
    expected_writer_fence_command = (
        'sudo -u "$SERVICE_USER" /usr/bin/env -i '
        'PATH=/usr/sbin:/usr/bin:/sbin:/bin GIT_OPTIONAL_LOCKS=0 '
        "PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 "
        "PROBIGA_DEPLOYMENT_MODE=production "
        'PROBIGA_EXPECTED_GIT_SHA="$EXPECTED_SHA" '
        'PROBIGA_BUILD_COMMIT_SHA="$EXPECTED_SHA" '
        'PROBIGA_CODE_ROOT="$PREPARED_CODE_ROOT" '
        'PROBIGA_EXPECTED_ADATA_SHA="$EXPECTED_ADATA_SHA" '
        'PROBIGA_EXPECTED_ADATA_TREE_SHA256="$EXPECTED_ADATA_TREE_SHA256" '
        'PROBIGA_ADATA_SOURCE_DIR="$ADATA_SOURCE" '
        'PROBIGA_RELEASE_TREE_SHA256="$EXPECTED_RELEASE_TREE_SHA256" '
        'PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256='
        '"$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256" '
        '"PYTHONPATH=$ADATA_SOURCE:$PREPARED_CODE_ROOT" '
        '"$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" -P '
        "tools/add_trading_v3_tasks.py --fence-only "
        "--require-no-live-scheduler-writers "
        "--writer-drain-timeout-seconds 0 "
        "--writer-drain-poll-seconds 5"
    )
    assert writer_fence.count(expected_writer_fence_command) == 1
    assert qmt_edge_request < writer_fence_start < writer_fence_end < api_stop
    request_window = normalized[qmt_edge_request:writer_fence_start]
    assert "run_qmt_windows_edge_release_bootstrap.py" in request_window
    assert '--request --expected-build-sha "$EXPECTED_SHA" --compact' in (
        request_window
    )
    assert 'p.get("database_writes") is True' in request_window
    python_cutover_commands = [
        line.strip()
        for line in downtime_closure.splitlines()
        if "python" in line and "grep -F --" not in line
    ]
    assert python_cutover_commands == [
        'run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/add_trading_v3_tasks.py" '
        '--writer-fence',
        'if ! run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/add_qmt_announcement_task.py" '
        '--disabled; then',
        'if ! run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/add_qmt_operations_tasks.py" '
        '--disabled; then',
        'run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/migrate_qmt_local_history_provenance.py" '
        '--check-via-primary',
        'QMT_EDGE_REQUEST_OUTPUT="$(run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/run_qmt_windows_edge_release_bootstrap.py" '
        '--request --expected-build-sha "$EXPECTED_SHA" --compact)"',
        'if QMT_EDGE_IDENTITY_OUTPUT="$(run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/check_qmt_windows_edge.py" '
        '--identity-only --expected-build-sha "$EXPECTED_SHA" '
        '--expected-poll-seconds 60 --compact)"; then',
        'if QMT_EDGE_BOOTSTRAP_OUTPUT="$(run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/check_qmt_windows_edge.py" '
        '--release-bootstrap-only --expected-build-sha "$EXPECTED_SHA" '
        '--expected-poll-seconds 60 --compact)"; then',
        'QMT_HISTORY_PREFLIGHT_OUTPUT="$(run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/prepare_strategy_governance_qmt_history.py" '
        '--readiness-only)"',
        'run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/prepare_strategy_governance_qmt_history.py" '
        '--expected-target-trade-date "$QMT_HISTORY_TARGET_TRADE_DATE" '
        '--expected-start-date "$QMT_HISTORY_START_DATE" '
        '--expected-end-date "$QMT_HISTORY_END_DATE" '
        '--expected-session-window-sha256 '
        '"$QMT_HISTORY_SESSION_WINDOW_SHA256"',
        'QMT_HISTORY_TARGET_TRADE_DATE="$(run_prepared_python_tool -c '
        "'from server.common.authoritative_market_clock import "
        "authoritative_closed_trade_date; "
        "from server.common.batch_db import create_batch_engine; "
        "from tools.env_config import load_project_env; load_project_env(); "
        "engine=create_batch_engine(future=True); "
        "value=authoritative_closed_trade_date(engine); engine.dispose(); "
        "print(value)')\"",
        'if QMT_ANNOUNCEMENT_RUN_OUTPUT="$(run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/sync_qmt_announcement_pit.py" '
        '--validate-existing-complete-batch --window-days 30 '
        '--expected-trade-date "$QMT_HISTORY_TARGET_TRADE_DATE")"; then',
        "printf '%s' \"$QMT_ANNOUNCEMENT_RUN_OUTPUT\" | "
        'run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/sync_qmt_announcement_pit.py" '
        '--validate-existing-result-exit "$QMT_ANNOUNCEMENT_RUN_STATUS" '
        '--expected-trade-date "$QMT_HISTORY_TARGET_TRADE_DATE"',
        'if ! run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/add_strategy_governance_task.py" '
        '--disabled --schema-prepared; then',
            'if GOVERNANCE_RUN_OUTPUT="$(run_prepared_python_tool '
            '"$PREPARED_CODE_ROOT/tools/run_strategy_governance_daily.py" '
            '"${GOVERNANCE_RUN_ARGS[@]}")"; then',
        "printf '%s' \"$GOVERNANCE_RUN_OUTPUT\" | "
        'run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/run_strategy_governance_daily.py" '
        '--validate-result-exit "$GOVERNANCE_RUN_STATUS" '
        '"${GOVERNANCE_RUN_ARGS[@]}"',
        'run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/add_strategy_governance_task.py" '
        '--schema-prepared',
        'run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/ensure_quality_gate.py" '
        '--task-type analysis_upper_evidence_prepare '
        '--task-type analysis_fast',
        'run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/add_qmt_announcement_task.py"',
        'run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/add_qmt_operations_tasks.py"',
        'run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/add_strategy_governance_task.py" '
        '--capture-snapshot "$GOVERNANCE_TASK_NEW_SOURCE"',
        'run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/add_qmt_announcement_task.py" '
        '--capture-snapshot "$QMT_ANNOUNCEMENT_TASK_NEW_SOURCE"',
        '/usr/bin/python3.14 -I - "$ACTIVATION_RECEIPT_PENDING" '
        '"$expected_release" <<\'PY\'',
    ]
    assert downtime.index(
        "prepare_strategy_governance_qmt_history.py"
    ) < downtime.index("run_strategy_governance_daily.py")
    assert "DATABASE_FORWARD_MIGRATION_STARTED=1" in downtime_closure
    assert "Database schema changes are forward-only additive" in deploy_script

    fence_command = writer_fence.index(expected_writer_fence_command)
    status_capture = writer_fence.index(
        ") || WRITER_FENCE_STATUS=$?", fence_command
    )
    status_failure = writer_fence.index(
        'if [ "$WRITER_FENCE_STATUS" -ne 0 ]; then', status_capture
    )
    exit_three = writer_fence.index(
        'if [ "$WRITER_FENCE_STATUS" -eq 3 ]; then', status_failure
    )
    external_writer_block = writer_fence.index(
        "EXTERNAL_WRITER_BLOCKED=1", exit_three
    )
    abort_cutover = writer_fence.index("false", external_writer_block)
    assert (
        fence_command
        < status_capture
        < status_failure
        < exit_three
        < external_writer_block
        < abort_cutover
    )

    scheduler_stop = normalized.index(
        "sudo systemctl stop probiga-scheduler", cutover
    )
    fence_position = writer_fence_start
    dropin_position = normalized.index(
        "install_prepared_dropins", fence_position
    )
    governance_install = normalized.index(
        "CUTOVER_STEP=install_strategy_governance", dropin_position
    )
    governance_run = normalized.index(
        "CUTOVER_STEP=run_strategy_governance", governance_install
    )
    governance_enable = normalized.index(
        "CUTOVER_STEP=enable_strategy_governance_task", governance_run
    )
    governance_prestart_check = normalized.index(
        "CUTOVER_STEP=verify_strategy_governance_before_start",
        governance_enable,
    )
    daemon_reload = normalized.index(
        "systemctl daemon-reload", governance_prestart_check
    )
    assert (
        cutover
        < scheduler_stop
        < fence_position
        < api_stop
        < dropin_position
        < governance_install
        < governance_run
        < governance_enable
        < governance_prestart_check
        < daemon_reload
        < service_start
    )
    governance_activation = normalized[
        governance_install:governance_prestart_check
    ]
    assert (
        'case "$GOVERNANCE_RUN_STATUS:$GOVERNANCE_JSON_STATUS" in'
        in governance_activation
    )
    assert "0:completed|0:not_due)" in governance_activation
    assert "2:not_ready)" in governance_activation
    assert "3:integrity_error)" in governance_activation
    assert "4:program_error)" in governance_activation
    assert '--validate-result-exit "$GOVERNANCE_RUN_STATUS"' in (
        governance_activation
    )
    assert (
        "printf '%s' \"$GOVERNANCE_RUN_OUTPUT\" | "
        "run_prepared_python_tool "
        '"$PREPARED_CODE_ROOT/tools/run_strategy_governance_daily.py"'
    ) in governance_activation
    assert "GOVERNANCE_RUN_ARGS=()" in governance_activation
    assert 'GOVERNANCE_RUN_ARGS=(--expected-build-sha "$EXPECTED_SHA")' in (
        governance_activation
    )
    assert governance_activation.count('"${GOVERNANCE_RUN_ARGS[@]}"') == 2
    assert governance_activation.index(
        "tools/run_strategy_governance_daily.py"
    ) < governance_activation.index(
        '"${GOVERNANCE_RUN_ARGS[@]}"'
    ) < governance_activation.index(
        '--validate-result-exit "$GOVERNANCE_RUN_STATUS"'
    ) < governance_activation.rindex(
        '"${GOVERNANCE_RUN_ARGS[@]}"'
    )
    assert "strategy_governance reused_completed build=%s release=%s" in (
        governance_activation
    )
    assert "GOVERNANCE_HEALTH_DISPOSITION=completed" in governance_activation
    not_ready_branch = governance_activation[
        governance_activation.index("2:not_ready)"):
        governance_activation.index("3:integrity_error)")
    ]
    assert "deferring data catch-up" in not_ready_branch
    assert "false" not in not_ready_branch
    assert "GOVERNANCE_HEALTH_DISPOSITION=input_not_ready" in (
        governance_activation
    )
    assert "GOVERNANCE_INPUT_NOT_READY" not in normalized
    assert "--allow-input-not-ready" in governance_activation
    assert normalized.count("--allow-input-not-ready") == 3
    assert '--expected-build-sha "$EXPECTED_SHA"' in normalized

    assert 'install_prepared_dropins' in downtime
    assert "systemctl daemon-reload" in downtime
    forbidden = (
        "git ",
        "git\t",
        "fetch",
        "checkout",
        "quarantine_",
        "seal_release_checkout",
        "find ",
        "release_identity_check",
        "pip ",
    )
    for token in forbidden:
        assert token not in downtime_closure, (
            f"slow cutover operation found: {token}"
        )

    health = normalized.index("http://127.0.0.1/api/health", service_start)
    static_switch = normalized.index(
        'point_static_release_to_checkout "$PREPARED_CODE_ROOT"', health
    )
    ai_restore = normalized.index(
        "CUTOVER_STEP=restore_ai_worker_previous_state", static_switch
    )
    scheduler_quiescence = normalized.index(
        "CUTOVER_STEP=verify_scheduler_triggers_quiescent", ai_restore
    )
    quality_gate = normalized.index(
        "CUTOVER_STEP=verify_premarket_quality_gate", scheduler_quiescence
    )
    premarket_probe = normalized.index(
        '"$PREPARED_CODE_ROOT/tools/ensure_quality_gate.py"',
        quality_gate,
    )
    premarket_task = premarket_probe + len(
        '"$PREPARED_CODE_ROOT/tools/ensure_quality_gate.py"'
    )
    code_cleanup = normalized.index(
        'prune_code_releases "$PREPARED_CODE_ROOT" "$PREVIOUS_CODE_ROOT"',
        premarket_task,
    )
    post_prune_identity = normalized.index(
        "CUTOVER_STEP=verify_post_prune_release_identity", code_cleanup,
    )
    post_prune_health = normalized.index(
        "CUTOVER_STEP=verify_post_prune_health", post_prune_identity,
    )
    login_smoke = normalized.index(
        "CUTOVER_STEP=verify_account_login_api_and_page_smoke",
        post_prune_health,
    )
    governance_smoke = normalized.index(
        "CUTOVER_STEP=verify_strategy_governance_api_and_page_smoke",
        login_smoke,
    )
    pool_smoke = normalized.index(
        "CUTOVER_STEP=verify_strategy_pool_api_and_page_smoke",
        governance_smoke,
    )
    receipt_pending = normalized.index(
        "persist_deployed_receipt_pending", pool_smoke
    )
    deployed_receipt = normalized.index(
        "publish_deployed_receipt_pending", receipt_pending
    )
    journal_finalize = normalized.rindex(
        "controlled_guard_finalize_successful_activation",
        receipt_pending,
        deployed_receipt,
    )
    journal_remove = normalized.index(
        "activation_snapshot_remove_finalized", deployed_receipt
    )
    assert (
        service_start
        < health
        < static_switch
        < ai_restore
        < scheduler_quiescence
        < quality_gate
        < premarket_probe
    )
    assert (
        premarket_probe
        < premarket_task
        < code_cleanup
        < post_prune_identity
        < post_prune_health
        < login_smoke
        < governance_smoke
        < pool_smoke
        < receipt_pending
        < journal_finalize
        < deployed_receipt
        < journal_remove
    )


def test_normal_activation_rechecks_governance_after_fresh_scheduler_heartbeat() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy_script)
    activation_start = normalized.index(
        "CUTOVER_STEP=verify_strategy_governance_before_start"
    )
    activation_end = normalized.index("DEPLOY_SUCCEEDED=1", activation_start)
    activation = normalized[activation_start:activation_end]
    governance_health = (
        '"$PREPARED_CODE_ROOT/tools/check_strategy_governance_health.py"'
    )
    guard_removal = activation.index(
        "CUTOVER_STEP=remove_database_writer_guard_after_full_prestart"
    )
    api_start = activation.index("CUTOVER_STEP=start_api", guard_removal)

    first_health = activation.index(governance_health)
    second_health = activation.index(governance_health, first_health + 1)
    heartbeat_wait = activation.index(
        "CUTOVER_STEP=wait_for_first_scheduler_heartbeat"
    )
    strict_recheck = activation.index(
        "CUTOVER_STEP=verify_strategy_governance_with_scheduler_heartbeat"
    )

    assert activation.count(governance_health) == 2
    assert first_health < guard_removal < api_start
    assert api_start < heartbeat_wait < strict_recheck < second_health
    assert '--expected-scheduler-pid "$SCHEDULER_MAIN_PID"' in activation[
        strict_recheck:
    ]
    assert 'GOVERNANCE_TRADE_DATE="$(' in activation[strict_recheck:]


def test_final_governance_api_and_page_smoke_is_fail_closed_before_receipt():
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy_script)
    smoke = deploy_script[
        deploy_script.index(
            "verify_strategy_governance_api_and_page_smoke() {"
        ):
        deploy_script.index("release_identity_check() {")
    ]

    for required in (
        "/api/strategy-center/governance?trade_date=$expected_trade_date",
        'payload.get("build_commit_sha") == expected_sha',
        'payload.get("result_mode") == "CANONICAL_PERSISTED"',
        'payload.get("is_canonical") is True',
        'run_uid = str(payload.get("run_uid") or "")',
        'result_hash = str(payload.get("canonical_result_hash") or "")',
        'set(pools) != {"observation", "confirmation", "tradable"}',
        'set(rankings) != {"strategy", "combination"}',
        'metadata.get("run_uid") == run_uid',
        'metadata.get("canonical_result_hash") == result_hash',
        'sum(weights) != Decimal("100")',
        "verify_authority(payload)",
        '"real_order_submission"',
        '"real_orders_allowed"',
        '"real_order_allowed"',
        '"automatic_real_order_authority"',
        'data-tab="strategy-center"',
        'id="tab-strategy-center"',
        "动态策略竞技场",
        "/api/strategy-center/governance",
        "真实下单权限：关闭",
    ):
        assert required in smoke
    assert "|| true" not in smoke
    pool_smoke = deploy_script[
        deploy_script.index(
            "verify_strategy_pool_api_and_page_smoke() {"
        ):
        deploy_script.index("release_identity_check() {")
    ]
    for required in (
        "/api/v3/stock-pool?trade_date=$expected_trade_date",
        "/api/v3/stock-pool?before_session_date=$expected_trade_date",
        "/api/v3/context?trade_date=$expected_trade_date",
        'envelope.get("code_commit_sha") != expected_sha',
        'pool.get("pool_readable") is True',
        'pool.get("run_status") == "COMPLETED"',
        'pool.get("decision_integrity_verified") is True',
        'status in {"READY", "EMPTY"}',
        'candidate_count != sum(',
        'status == "READY" and candidate_count > 0',
        'status == "EMPTY" and candidate_count == 0',
        'def unavailable_pool(pool):',
        'mode = "UNAVAILABLE_NO_VERIFIED_POOL"',
        'fail("historical_pool_unavailable_contract")',
        'latest.get("decision_session_date") or "") >= expected_trade_date',
        'latest.get("before_session_date") == expected_trade_date',
        'latest.get("requested_trade_date") == expected_trade_date',
        'latest.get("is_historical_fallback") is True',
        'latest.get("historical_read_only") is True',
        'latest.get("historical_fallback_status")',
        '== "HISTORICAL_READ_ONLY"',
        'latest.get("historical_fallback_session_date")',
        '== latest.get("decision_session_date")',
        'context.get("run_uid") != exact.get("run_uid")',
        'exact.get("is_as_of_fallback") is True',
        'exact.get("requested_trade_date") == expected_trade_date',
        'context.get("data_date") == exact_session',
        'context.get("is_as_of_fallback") is True',
        'mode = "LATEST_COMPLETED_AS_OF"',
        "HISTORICAL_READ_ONLY",
        "/static/trading-v3.html",
        "/static/js/trading-v3.js",
    ):
        assert required in pool_smoke
    assert "|| true" not in pool_smoke
    post_prune_health = normalized.index(
        "CUTOVER_STEP=verify_post_prune_health"
    )
    login_smoke_call = normalized.index(
        "CUTOVER_STEP=verify_account_login_api_and_page_smoke",
        post_prune_health,
    )
    governance_smoke_call = normalized.index(
        "CUTOVER_STEP=verify_strategy_governance_api_and_page_smoke",
        login_smoke_call,
    )
    pool_smoke_call = normalized.index(
        "CUTOVER_STEP=verify_strategy_pool_api_and_page_smoke",
        governance_smoke_call,
    )
    receipt = normalized.index(
        "CUTOVER_STEP=persist_deployed_receipt_pending", pool_smoke_call,
    )
    assert (
        post_prune_health
        < login_smoke_call
        < governance_smoke_call
        < pool_smoke_call
        < receipt
    )


def test_final_account_login_smoke_exercises_runtime_schema_and_real_page():
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy_script)
    login_smoke = deploy_script[
        deploy_script.index("verify_account_login_api_and_page_smoke() {"):
        deploy_script.index(
            "verify_strategy_governance_api_and_page_smoke() {"
        )
    ]

    for required in (
        "http://127.0.0.1/api/auth/status",
        "http://127.0.0.1/api/auth/login",
        "__release_probe_",
        "secrets.token_hex(16)",
        '[ "$login_http_code" != 401 ]',
        'status.get("required") is True',
        'status.get("user_initialized") is True',
        'status["user_count"] >= 1',
        'status.get("registration_open") is False',
        'login.get("error") == "invalid_credentials"',
        "http://127.0.0.1/login",
        "/static/js/login.js",
        "server/static/login.html",
        "server/static/js/login.js",
    ):
        assert required in login_smoke
    assert "|| true" not in login_smoke

    post_prune_health = normalized.index(
        "CUTOVER_STEP=verify_post_prune_health"
    )
    login_call = normalized.index(
        "CUTOVER_STEP=verify_account_login_api_and_page_smoke",
        post_prune_health,
    )
    governance_call = normalized.index(
        "CUTOVER_STEP=verify_strategy_governance_api_and_page_smoke",
        login_call,
    )
    receipt = normalized.index(
        "CUTOVER_STEP=persist_deployed_receipt_pending", governance_call,
    )
    assert post_prune_health < login_call < governance_call < receipt


def test_qmt_announcement_checkpoint_state_is_persistent_and_separate_from_code() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy_script)
    bodies = _shell_function_bodies(deploy_script)
    state_root = "/var/lib/probiga/qmt-announcement-checkpoints"

    assert f"QMT_ANNOUNCEMENT_CHECKPOINT_ROOT={state_root}" in deploy_script
    prepare_root = _normalized_shell(
        bodies["prepare_qmt_announcement_checkpoint_root"]
    )
    for required in (
        'install -d -o "$SERVICE_USER" -g "$SERVICE_USER" -m 0700',
        'test ! -L "$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT"',
        'readlink -f -- "$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT"',
        "stat -c '%U:%G'",
        "stat -c '%a'",
        'find -P "$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT" -mindepth 1 -type l',
        'sudo -u "$SERVICE_USER" test -w',
    ):
        assert required in prepare_root
    service_user = normalized.index('test "$SERVICE_USER" != root')
    prepare_call = normalized.index(
        "prepare_qmt_announcement_checkpoint_root", service_user
    )
    first_capture = normalized.index(
        "CUTOVER_STEP=validate_existing_qmt_announcement_full_market_batch",
        prepare_call,
    )
    assert service_user < prepare_call < first_capture
    assert (
        'QMT_ANNOUNCEMENT_CHECKPOINT_DIR="$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT"'
        in bodies["run_prepared_python_tool"]
    )
    for name in ("write_dropin", "write_scheduler_dropin"):
        assert "QMT_ANNOUNCEMENT_CHECKPOINT_DIR=" in bodies[name]
    capture = normalized[first_capture:]
    assert "--validate-existing-complete-batch --window-days 30" in capture
    assert '--expected-trade-date "$QMT_HISTORY_TARGET_TRADE_DATE"' in capture
    assert '--checkpoint-dir "$QMT_ANNOUNCEMENT_CHECKPOINT_ROOT"' not in (
        capture.split("CUTOVER_STEP=install_runtime_units", 1)[0]
    )
    assert (
        "--checkpoint-dir /var/lib/probiga/qmt-announcement-checkpoints"
        in deploy_script
    )


def test_initial_qmt_history_gate_cannot_be_waived_before_governance() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy_script)
    writer_fence = normalized.index(
        "CUTOVER_STEP=writer_fence_before_api_stop"
    )
    history_gate = normalized.index(
        "CUTOVER_STEP=prepare_strategy_governance_qmt_history",
        writer_fence,
    )
    governance_run = normalized.index(
        "CUTOVER_STEP=run_strategy_governance", history_gate
    )
    api_start = normalized.index("CUTOVER_STEP=start_api", governance_run)
    gate_body = normalized[history_gate:governance_run]

    assert writer_fence < history_gate < governance_run < api_start
    assert gate_body.count(
        'run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/'
        'prepare_strategy_governance_qmt_history.py"'
    ) == 1
    assert "--schema-only" not in gate_body
    assert "if run_prepared_python_tool" not in gate_body
    assert "|| true" not in gate_body
    assert "GOVERNANCE_INPUT_NOT_READY" not in gate_body
    assert "--allow-input-not-ready" not in gate_body
    for argument in (
        "--expected-target-trade-date",
        "--expected-start-date",
        "--expected-end-date",
        "--expected-session-window-sha256",
    ):
        assert gate_body.count(argument) == 1
    assert '"$QMT_HISTORY_TARGET_TRADE_DATE"' in gate_body
    assert '"$QMT_HISTORY_START_DATE"' in gate_body
    assert '"$QMT_HISTORY_END_DATE"' in gate_body
    assert '"$QMT_HISTORY_SESSION_WINDOW_SHA256"' in gate_body
    assert "set -Eeuo pipefail" in normalized


def test_qmt_history_and_reference_reads_start_only_after_schema_cutover() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy_script)
    schema_recover = normalized.index(
        "CUTOVER_STEP=recover_strategy_governance_database_trust"
    )
    provenance_check = normalized.index(
        "CUTOVER_STEP=verify_qmt_local_history_provenance_schema_after_cutover",
        schema_recover,
    )
    release_request = normalized.index(
        "CUTOVER_STEP=request_qmt_windows_edge_release_bootstrap",
        provenance_check,
    )
    edge_identity = normalized.index(
        "CUTOVER_STEP=wait_for_qmt_windows_edge_identity", release_request
    )
    edge_bootstrap = normalized.index(
        "CUTOVER_STEP=wait_for_qmt_windows_edge_release_bootstrap",
        edge_identity,
    )
    readiness = normalized.index(
        "CUTOVER_STEP=read_strategy_governance_qmt_history_readiness_after_schema",
        edge_bootstrap,
    )
    history_apply = normalized.index(
        "CUTOVER_STEP=prepare_strategy_governance_qmt_history", readiness
    )
    announcement_batch = normalized.index(
        "CUTOVER_STEP=validate_existing_qmt_announcement_full_market_batch",
        history_apply,
    )
    governance_run = normalized.index(
        "CUTOVER_STEP=run_strategy_governance", announcement_batch
    )
    body = normalized[provenance_check:history_apply]

    assert (
        schema_recover
        < provenance_check
        < release_request
        < edge_identity
        < edge_bootstrap
        < readiness
        < history_apply
        < announcement_batch
        < governance_run
    )
    assert body.count(
        'run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/'
        'migrate_qmt_local_history_provenance.py" --check-via-primary'
    ) == 1
    announcement_body = normalized[announcement_batch:governance_run]
    assert "--validate-existing-complete-batch --window-days 30" in (
        announcement_body
    )
    assert announcement_body.count(
        '--expected-trade-date "$QMT_HISTORY_TARGET_TRADE_DATE"'
    ) == 2
    assert "--batch-size" not in announcement_body
    assert "--checkpoint-dir" not in announcement_body
    assert '--validate-existing-result-exit "$QMT_ANNOUNCEMENT_RUN_STATUS"' in (
        announcement_body
    )
    assert (
        'case "$QMT_ANNOUNCEMENT_RUN_STATUS:'
        '$QMT_ANNOUNCEMENT_DISPOSITION" in'
    ) in announcement_body
    assert "0:complete)" in announcement_body
    assert "2:data_blocked)" in announcement_body
    assert "deferring data catch-up" in announcement_body
    assert 'test "$QMT_ANNOUNCEMENT_RUN_STATUS" -eq 0' not in announcement_body
    assert "sync_guojin_qmt_reference_data.py" not in body
    assert "run_qmt_windows_edge_release_bootstrap.py" in body
    assert "--request --expected-build-sha \"$EXPECTED_SHA\"" in body
    assert "--identity-only --expected-build-sha \"$EXPECTED_SHA\"" in body
    assert "--release-bootstrap-only" in body
    assert body.count("--expected-poll-seconds 60 --compact") == 2
    assert 'QMT_HISTORY_PREFLIGHT_OUTPUT="$(run_prepared_python_tool' in body
    assert "session_window_sha256" in body
    assert "readonly QMT_HISTORY_TARGET_TRADE_DATE" in body
    assert "|| true" not in body
    assert body.count(
        'run_prepared_python_tool '
        '"$PREPARED_CODE_ROOT/tools/'
        'prepare_strategy_governance_qmt_history.py" --readiness-only'
    ) == 1


def test_rollback_restores_previous_immutable_runtime_without_checkout() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    rollback = deploy_script[
        deploy_script.index("rollback() {"):
        deploy_script.index("trap 'rollback \"$?\" \"$LINENO\"' ERR")
    ]
    normalized_rollback = _normalized_shell(rollback)

    assert "git checkout" not in rollback
    assert "seal_release_checkout" not in rollback
    assert "$PREVIOUS_CODE_ROOT" in rollback
    assert "$PREVIOUS_VENV" in rollback
    assert re.search(
        r'sudo install\s+-o root -g root -m 0644\s+'
        r'"\$PREVIOUS_DROPIN"\s+'
        r'"\$MAIN_RELEASE_DROPIN"',
        normalized_rollback,
    )
    assert re.search(
        r'sudo install\s+-o root -g root -m 0644\s+'
        r'"\$PREVIOUS_AI_WORKER_DROPIN"\s+"\$AI_WORKER_DROPIN"',
        normalized_rollback,
    )
    assert re.search(
        r'point_static_release_to_checkout\s+"\$PREVIOUS_CODE_ROOT"',
        normalized_rollback,
    )
    assert re.search(
        r'assert_ai_worker_runtime\s+"\$PREVIOUS_RELEASE_REVISION"\s+'
        r'"\$PREVIOUS_VENV"\s+"\$PREVIOUS_CODE_ROOT"',
        normalized_rollback,
    )
    assert "restore previous probiga drop-in" in rollback
    assert "restore previous AI recommendation worker drop-in" in rollback
    assert "restore previous scheduler task set" in rollback
    assert "prepared_restore_and_verify_governance_snapshot" in rollback
    assert "controlled_guard_restore_and_verify_governance_snapshot" not in rollback
    assert '"$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT"' in rollback
    assert "point Nginx static assets at previous code release" in rollback
    assert 'write_receipt "ROLLED_BACK"' in rollback


def test_previous_code_fallback_and_code_retention_keep_two_generations() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy_script)
    function_bodies = _shell_function_bodies(deploy_script)

    assert "PREVIOUS_CODE_ROOT" in deploy_script
    assert "PROBIGA_CODE_ROOT" in deploy_script
    assert "REPOSITORY_ROOT=/opt/ProBigA" in deploy_script
    assert 'PREVIOUS_CODE_ROOT="$REPOSITORY_ROOT"' in deploy_script
    assert '"$CODE_RELEASE_ROOT/$PREVIOUS_SHA"' in deploy_script
    assert '"$PREVIOUS_CODE_ROOT"' in deploy_script

    retention_body = _normalized_shell(
        function_bodies["prune_code_releases"]
    )
    bindings = re.findall(
        r'local\s+([a-zA-Z_][a-zA-Z0-9_]*)="\$(1|2)"',
        retention_body,
    )
    assert {position for _name, position in bindings} == {"1", "2"}
    for name, _position in bindings:
        assert retention_body.count(f'"${name}"') >= 2
    assert re.search(
        r'case "\$entry_real" in\s+'
        r'"\$active_root"\|"\$rollback_root"\) continue ;;',
        retention_body,
    )
    assert "readlink -f" in retention_body
    assert "-L" in retention_body
    assert "$CODE_RELEASE_ROOT" in retention_body
    assert re.search(
        r'git --git-dir="\$CODE_GIT_CACHE" worktree remove --force',
        retention_body,
    )

    prune_call = (
        'prune_code_releases "$PREPARED_CODE_ROOT" "$PREVIOUS_CODE_ROOT"'
    )
    assert normalized.count(prune_call) == 1
    prune_position = normalized.index(prune_call)
    service_start = _required_shell_position(
        normalized[
            normalized.index("trap 'rollback \"$?\" \"$LINENO\"' ERR"):
        ],
        r'sudo systemctl start (?:"\$MAIN_SERVICE"|probiga)(?:[ \t]|$)',
    ) + normalized.index("trap 'rollback \"$?\" \"$LINENO\"' ERR")
    static_switch = normalized.index(
        'point_static_release_to_checkout "$PREPARED_CODE_ROOT"',
        service_start,
    )
    post_prune_identity = normalized.index(
        "CUTOVER_STEP=verify_post_prune_release_identity", static_switch
    )
    post_prune_health = normalized.index(
        "CUTOVER_STEP=verify_post_prune_health", post_prune_identity
    )
    receipt_pending = normalized.index(
        "persist_deployed_receipt_pending", post_prune_health
    )
    journal_finalize = normalized.rindex(
        "controlled_guard_finalize_successful_activation",
        static_switch,
        normalized.index("publish_deployed_receipt_pending", receipt_pending),
    )
    receipt = normalized.index("publish_deployed_receipt_pending", journal_finalize)
    journal_remove = normalized.index(
        "activation_snapshot_remove_finalized_before_deploy", receipt
    )
    assert (
        service_start
        < static_switch
        < prune_position
        < post_prune_identity
        < post_prune_health
        < receipt_pending
        < journal_finalize
        < receipt
        < journal_remove
    )


def test_warning_only_retention_helpers_fail_closed_on_unsafe_paths() -> None:
    """Do not rely on errexit inside an ``if ! prune_*`` condition.

    Bash disables ``set -e`` for the complete body of a function used as an
    ``if`` condition.  Every path-containment guard before a removal must
    therefore return explicitly when it fails.
    """

    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    function_bodies = _shell_function_bodies(deploy_script)
    venv_prune = _normalized_shell(
        function_bodies["prune_release_venvs"]
    )
    code_prune = _normalized_shell(
        function_bodies["prune_code_releases"]
    )
    temp_prune = _normalized_shell(
        function_bodies["prune_release_temp_files"]
    )

    assert 'if ! prune_release_venvs "$EXPECTED_SHA"' in deploy_script
    assert 'if ! prune_code_releases "$PREPARED_CODE_ROOT"' in deploy_script
    assert "if ! prune_release_temp_files; then" in deploy_script

    required_venv_guards = (
        'release_root_real="$(readlink -f -- "$RELEASE_VENV_ROOT")" '
        '|| return 2',
        'test "$release_root_real" = "$RELEASE_VENV_ROOT" || return 2',
        '[[ "$protected_sha" =~ ^[0-9a-f]{40}$ ]] || return 2',
        'release_scan="$(find "$RELEASE_VENV_ROOT" -mindepth 1 '
        '-maxdepth 1 -type l -printf \'%T@ %f\\n\')" || return 2',
        'test -L "$link" || return 2',
        'target="$(readlink -f -- "$link")" || return 2',
        '[[ "$rollback_sha" =~ ^[0-9a-f]{40}$ ]] || return 2',
        'target="$(readlink -f -- '
        '"$RELEASE_VENV_ROOT/$rollback_sha")" || return 2',
        'build_real="$(readlink -f -- "$build_dir")" || return 2',
        'test "$(dirname -- "$build_real")" = "$RELEASE_VENV_ROOT" '
        '|| return 2',
        'rm -f -- "$link" || return 2',
        'candidate_bytes="$(du -sb -- "$build_real" | awk \'{print $1}\')" '
        '|| return 2',
        'rm -rf -- "$build_real" || return 2',
    )
    for guard in required_venv_guards:
        assert guard in venv_prune, f"venv prune guard is not fail-closed: {guard}"
    assert venv_prune.count(
        'test "$(dirname -- "$target")" = "$RELEASE_VENV_ROOT" '
        '|| return 2'
    ) >= 3
    assert (
        '*) echo "protected release venv escaped its immutable root" >&2; '
        'return 2 ;;'
        in venv_prune
    )
    assert (
        '*) echo "rollback release venv escaped its immutable root" >&2; '
        'return 2 ;;'
        in venv_prune
    )
    assert (
        '*) echo "refusing unsafe release venv target: $build_real" >&2; '
        'return 2 ;;'
        in venv_prune
    )

    required_code_guards = (
        'test ! -L "$CODE_RELEASE_ROOT" || return 2',
        'release_root_real="$(readlink -f -- "$CODE_RELEASE_ROOT")" '
        '|| return 2',
        'test "$release_root_real" = "$CODE_RELEASE_ROOT" || return 2',
        '[[ "$active_name" =~ ^[0-9a-f]{40}$ ]] || return 2',
        'test "$active_root" = "$CODE_RELEASE_ROOT/$active_name" '
        '|| return 2',
        '[[ "$rollback_name" =~ ^[0-9a-f]{40}$ ]] || return 2',
        'test "$rollback_root" = "$CODE_RELEASE_ROOT/$rollback_name" '
        '|| return 2',
        'unsafe_link="$(find "$CODE_RELEASE_ROOT" -mindepth 1 '
        '-maxdepth 1 -type l -print -quit)" || return 2',
        'entry_real="$(readlink -f -- "$entry")" || return 2',
        'test "$(dirname -- "$entry_real")" = "$CODE_RELEASE_ROOT" '
        '|| return 2',
        'candidate_bytes="$(du -sb -- "$entry_real" | awk \'{print $1}\')" '
        '|| return 2',
        'git --git-dir="$CODE_GIT_CACHE" worktree remove --force '
        '"$entry_real" || return 2',
        'rm -rf -- "$entry_real" || return 2',
        'git --git-dir="$CODE_GIT_CACHE" worktree prune || return 2',
    )
    for guard in required_code_guards:
        assert guard in code_prune, f"code prune guard is not fail-closed: {guard}"
    symlink_rejection = code_prune.index(
        'echo "refusing symlink inside immutable code release root" >&2'
    )
    symlink_branch_end = code_prune.index("fi", symlink_rejection)
    assert "return 2" in code_prune[symlink_rejection:symlink_branch_end]
    assert (
        '"$active_root"|"$rollback_root") continue ;;' in code_prune
    )

    allowed_temp_paths = (
        '/tmp/probiga-release-*.tar.gz|/tmp/probiga-*.bundle) ;;'
    )
    rejected_temp_path = (
        '*) echo "refusing unsafe release temp path: $temp_file" >&2; '
        'return 2 ;;'
    )
    assert allowed_temp_paths in temp_prune
    assert rejected_temp_path in temp_prune
    assert (
        'candidate_bytes="$(stat -c \'%s\' -- "$temp_file")" || return 2'
        in temp_prune
    )
    assert 'rm -f -- "$temp_file" || return 2' in temp_prune
    guard_position = temp_prune.index(rejected_temp_path)
    stat_position = temp_prune.index('stat -c', guard_position)
    remove_position = temp_prune.index('rm -f -- "$temp_file"', stat_position)
    assert guard_position < stat_position < remove_position


def test_prebuild_space_reclamation_is_guarded_and_precedes_build() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy_script)
    body = _normalized_shell(
        _shell_function_bodies(deploy_script)["prebuild_reclaim_release_space"]
    )

    assert "PREBUILD_MIN_AVAILABLE_BYTES=2147483648" in deploy_script
    assert 'CUTOVER_STEP=prebuild_release_space' in normalized
    reclaim = normalized.index("prebuild_reclaim_release_space\n")
    prepare = normalized.index("CUTOVER_STEP=prepare_release", reclaim)
    assert reclaim < prepare
    assert 'prune_code_releases "$PREVIOUS_CODE_ROOT"' not in normalized[:prepare]

    guard = body.index('for journal_path in')
    retired_qmt = body.index("remove_retired_qmt_server_project || return 2")
    temp_prune = body.index("prune_release_temp_files || return 2")
    prune = body.index(
        'prune_release_venvs "$PREVIOUS_RELEASE_REVISION" "$EXPECTED_SHA"'
    )
    space = body.index('df -P -B1 -- "$space_path"')
    assert guard < retired_qmt < temp_prune < prune < space
    assert "-mmin +10 -print0" in deploy_script
    retired_body = _normalized_shell(
        _shell_function_bodies(deploy_script)[
            "remove_retired_qmt_server_project"
        ]
    )
    assert "qmt-agent.service qmt-agent-scheduler.service" in retired_body
    assert 'test ! -L "$retired_path" || return 2' in retired_body
    assert '/opt/qmt-agent|/opt/qmt-agent-data)' in retired_body
    assert 'rm -rf -- "$retired_real" || return 2' in retired_body
    for journal in (
        '"$ACTIVATION_UNIT_SNAPSHOT_DIR"',
        '"$DATABASE_WRITER_GUARD_FILE"',
        '"$DATABASE_WRITER_RESTORE_FILE"',
    ):
        assert journal in body
    assert 'if [ -e "$journal_path" ] || [ -L "$journal_path" ]; then' in body
    assert "awk 'NR == 2 {print $4}'" in body
    for space_path in (
        "/tmp",
        "/var/tmp",
        '"$RELEASE_VENV_ROOT"',
        '"$RELEASE_ARTIFACT_ROOT"',
        '"$CODE_RELEASE_ROOT"',
    ):
        assert space_path in body
    assert "f_bavail" in body
    assert 'sudo -u "$BUILD_USER" mktemp -d /tmp/.probiga-prebuild.XXXXXX' in body
    assert 'sudo -u "$BUILD_USER" rmdir -- "$build_temp_probe"' in body
    assert '"$LEGACY_RELEASE_VENV_ROOT/$PREVIOUS_RELEASE_REVISION")' in body
    assert "Skipped prebuild release venv cleanup for legacy active runtime" in body


def test_prebuild_space_reclamation_never_deletes_with_guard_or_legacy_active(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the prebuild cleanup regression")

    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    body = _shell_function_bodies(deploy_script)[
        "prebuild_reclaim_release_space"
    ]
    function_definition = (
        "prebuild_reclaim_release_space() {\n" + body + "}\n"
    )
    sandbox = tmp_path.as_posix()
    harness = function_definition + f'''\nset -u
TEST_ROOT={sandbox!r}
mkdir -p "$TEST_ROOT/venvs" "$TEST_ROOT/legacy"
RELEASE_VENV_ROOT="$TEST_ROOT/venvs"
RELEASE_ARTIFACT_ROOT="$TEST_ROOT/artifacts"
CODE_RELEASE_ROOT="$TEST_ROOT/code"
mkdir -p "$CODE_RELEASE_ROOT"
LEGACY_RELEASE_VENV_ROOT="$TEST_ROOT/legacy"
DATABASE_WRITER_GUARD_FILE="$TEST_ROOT/guard"
DATABASE_WRITER_RESTORE_FILE="$TEST_ROOT/restore"
ACTIVATION_UNIT_SNAPSHOT_DIR="$TEST_ROOT/activation"
PREBUILD_MIN_AVAILABLE_BYTES=2147483648
BUILD_USER=probiga-build
PREVIOUS_RELEASE_REVISION=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
EXPECTED_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
PREVIOUS_VENV="$RELEASE_VENV_ROOT/$PREVIOUS_RELEASE_REVISION"
DELETE_MARKER="$TEST_ROOT/delete-reached"
prune_release_venvs() {{ : > "$DELETE_MARKER"; return 0; }}
prune_release_temp_files() {{ return 0; }}
remove_retired_qmt_server_project() {{ return 0; }}
install() {{ mkdir -p "${{@: -1}}"; }}
readlink() {{
  if [ "$1" = -f ]; then printf '%s\n' "${{@: -1}}"; else command readlink "$@"; fi
}}
sudo() {{
  if [ "$1" = -u ]; then shift 2; fi
  "$@"
}}
test() {{
  if [ "${{1:-}}" = -d ] && [ "${{2:-}}" = /var/tmp ]; then return 0; fi
  builtin test "$@"
}}
df() {{ printf 'Filesystem 1-blocks Used Available Capacity Mounted on\nmock 1 0 3000000000 0%% /\n'; }}

: > "$DATABASE_WRITER_GUARD_FILE"
if prebuild_reclaim_release_space; then exit 20; fi
test ! -e "$DELETE_MARKER" || exit 21
rm -f "$DATABASE_WRITER_GUARD_FILE"

PREVIOUS_VENV="$LEGACY_RELEASE_VENV_ROOT/$PREVIOUS_RELEASE_REVISION"
prebuild_reclaim_release_space || exit 22
test ! -e "$DELETE_MARKER" || exit 23

df() {{ printf 'Filesystem 1-blocks Used Available Capacity Mounted on\nmock 1 0 1 100%% /\n'; }}
if prebuild_reclaim_release_space; then exit 24; fi
test ! -e "$DELETE_MARKER" || exit 25
'''
    completed = subprocess.run(
        [bash, "-c", harness],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=15,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_prebuild_venv_retention_counts_only_a_published_incoming_release() -> None:
    if sys.platform == "win32":
        pytest.skip("Git Bash cannot create native release symlinks on Windows")
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the prebuild retention regression")

    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    body = _shell_function_bodies(deploy_script)["prune_release_venvs"]
    definition = "prune_release_venvs() {\n" + body + "}\n"
    harness = definition + r'''
set -Eeuo pipefail
RELEASE_VENV_RETENTION=2
active=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
incoming=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
second=cccccccccccccccccccccccccccccccccccccccc
stale=dddddddddddddddddddddddddddddddddddddddd
referenced=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
path_is_runtime_referenced() { [ "${RUNTIME_REF:-}" = "$1" ]; }
path_is_opt_link_target() { return 1; }

make_release() {
  local root="$1" sha="$2" stamp="$3"
  mkdir -p "$root/build-$sha-1"
  ln -s "$root/build-$sha-1" "$root/$sha"
  touch -h -d "@$stamp" "$root/$sha"
}

with_link="$(readlink -f "$(mktemp -d)")"
without_link="$(readlink -f "$(mktemp -d)")"
trap 'rm -rf -- "$with_link" "$without_link"' EXIT

for root in "$with_link" "$without_link"; do
  make_release "$root" "$active" 100
  make_release "$root" "$second" 300
  make_release "$root" "$stale" 200
done
make_release "$with_link" "$incoming" 400
make_release "$with_link" "$referenced" 250
mkdir -p "$without_link/build-$incoming-1"

RELEASE_VENV_ROOT="$with_link"
RUNTIME_REF="$with_link/$referenced"
prune_release_venvs "$active" "$incoming"
test -L "$with_link/$active"
test -L "$with_link/$incoming"
test ! -e "$with_link/$second"
test ! -e "$with_link/build-$second-1"
test ! -e "$with_link/$stale"
test -L "$with_link/$referenced"
test -d "$with_link/build-$referenced-1"

RELEASE_VENV_ROOT="$without_link"
RUNTIME_REF=""
prune_release_venvs "$active" "$incoming"
test -L "$without_link/$active"
test -L "$without_link/$second"
test ! -e "$without_link/$incoming"
test ! -e "$without_link/build-$incoming-1"
test ! -e "$without_link/$stale"
'''
    completed = subprocess.run(
        [bash, "-c", harness],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=15,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_code_retention_guards_survive_if_not_conditional_context() -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the retention errexit regression")

    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    function_body = _shell_function_bodies(deploy_script)[
        "prune_code_releases"
    ]
    function_definition = (
        "prune_code_releases() {\n" + function_body + "}\n"
    )
    harness = (
        "set -Eeuo pipefail\n"
        + function_definition
        + r'''
sandbox="$(mktemp -d)"
trap 'command rm -rf -- "$sandbox"' EXIT
delete_log="$sandbox/delete.log"
: > "$delete_log"
rm() {
  printf 'rm %s\n' "$*" >> "$delete_log"
  return 0
}
git() {
  printf 'git %s\n' "$*" >> "$delete_log"
  return 0
}
path_is_runtime_referenced() { return 1; }
path_is_opt_link_target() { return 1; }

active_sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
REPOSITORY_ROOT=/opt/ProBigA
CODE_GIT_CACHE="$sandbox/code.git"

mkdir "$sandbox/releases-real"
ln -s "$sandbox/releases-real" "$sandbox/releases-link"
if [ -L "$sandbox/releases-link" ]; then
  CODE_RELEASE_ROOT="$sandbox/releases-link"
  symlink_rejected=0
  if ! prune_code_releases \
    "$CODE_RELEASE_ROOT/$active_sha" "$REPOSITORY_ROOT"; then
    symlink_rejected=1
  fi
  if [ "$symlink_rejected" -ne 1 ]; then
    echo 'symlink release root was not rejected' >&2
    exit 20
  fi
  if [ -s "$delete_log" ]; then
    echo 'symlink guard failure reached a deletion command' >&2
    cat "$delete_log" >&2
    exit 21
  fi
fi

: > "$delete_log"
mkdir "$sandbox/releases-readlink-failure"
CODE_RELEASE_ROOT="$sandbox/releases-readlink-failure"
readlink() { return 1; }
readlink_rejected=0
if ! prune_code_releases \
  "$CODE_RELEASE_ROOT/$active_sha" "$REPOSITORY_ROOT"; then
  readlink_rejected=1
fi
if [ "$readlink_rejected" -ne 1 ]; then
  echo 'readlink guard failure was not propagated' >&2
  exit 22
fi
if [ -s "$delete_log" ]; then
  echo 'readlink guard failure reached a deletion command' >&2
  cat "$delete_log" >&2
  exit 23
fi
'''
    )

    completed = subprocess.run(
        [bash, "-c", harness],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_deploy_workflow_pins_separate_adata_runtime() -> None:
    workflow_source = (ROOT / ".github/workflows/deploy.yml").read_text(
        encoding="utf-8"
    )
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    workflow = workflow_source + "\n" + deploy_script

    assert "pip install -e ./adata" not in workflow
    release_manifest = (ROOT / "deploy/production_release.env").read_text(
        encoding="utf-8"
    )
    assert (
        "ADATA_RELEASE_SHA=b14f4e57b2175302f18b6eaf934f7dff9207a141"
        in release_manifest
    )
    assert (
        "ADATA_TREE_SHA256="
        "17126239386512958368e428a0c72630cb3c1b20d6ff41bcc2234ebf5159a1a7"
        in release_manifest
    )
    assert "deploy/production_release.env" in workflow_source
    assert "https://github.com/1nchaos/adata.git" in workflow
    assert "ADATA_GIT_CACHE=/var/lib/probiga/release-sources/adata.git" in workflow
    assert "LEGACY_ADATA_GIT_CACHE" not in workflow
    assert 'git clone --mirror --no-hardlinks "$adata_seed"' not in workflow
    assert "legacy mutable adata checkout cannot be used as a rollback seed" in workflow
    assert "http.lowSpeedTime=30" in workflow
    assert "EXPECTED_ADATA_TREE_SHA256" in workflow
    assert "server.common.adata_release seal" in workflow
    assert "pip wheel --no-deps" in workflow
    assert 'config core.autocrlf false' in workflow_source
    assert (
        'rev-parse \\\n'
        '            "FETCH_HEAD^{commit}"'
    ) in workflow_source
    assert 'archive "$FETCHED_ADATA_SHA"' in workflow_source
    assert "PROBIGA_EXPECTED_ADATA_SHA" in workflow
    assert "PROBIGA_EXPECTED_ADATA_TREE_SHA256" in workflow
    assert "PROBIGA_ADATA_SOURCE_DIR" in workflow
    assert "probiga.deploy-receipt.v4" in workflow
    assert "PROBIGA_ADMIN_AUTH_ENABLED=true" in workflow
    assert "\n          tests/test_trading_v3_research_api.py\n" in workflow_source
    assert "\n           tests/test_trading_v3_research_api.py\n" not in workflow_source
    assert "systemctl stop probiga-scheduler" in workflow
    assert "systemctl disable probiga-scheduler" in workflow
    assert 'print(f"{sys.version_info.major}.{sys.version_info.minor}")' in workflow
    assert "PYTHONDONTWRITEBYTECODE" in workflow
    assert "-name '*.pyc'" in workflow
    assert "service account can modify protected release paths" in workflow
    assert "probiga-scheduler.timer" in workflow
    assert "probiga-scheduler.path" in workflow
    assert "probiga-scheduler.socket" in workflow
    assert "assert_scheduler_triggers_quiescent" in workflow
    preflight = workflow.index("\nassert_scheduler_triggers_quiescent\n")
    rollback_trap = workflow.index(
        "trap 'rollback \"$?\" \"$LINENO\"' ERR"
    )
    assert preflight < rollback_trap
    assert '"$checkout_root/.git" "$checkout_root/.github"' in workflow
    assert 'find "$checkout_root" -maxdepth 1' in workflow
    assert '"$checkout_root/scripts"' in workflow
    assert '"$checkout_root/strategies"' in workflow
    assert '"$checkout_root/versions"' in workflow
    assert "reused release virtual environment" in workflow
    assert "new release virtual environment" in workflow
    assert 'chmod -R a+rX,a-w "$EXPECTED_BUILD"' in deploy_script
    assert 'sudo -u "$SERVICE_USER" test -x "$EXPECTED_BUILD/bin/python"' in deploy_script
    assert 'chmod -R a+rX,a-w "$ADATA_SOURCE"' in deploy_script
    assert (
        'sudo -u "$SERVICE_USER" test -r '
        '"$ADATA_SOURCE/.probiga-adata.gitsha"'
    ) in deploy_script
    assert (
        'sudo -u "$SERVICE_USER" test -r '
        '"$ADATA_SOURCE/.probiga-adata.tree.sha256"'
    ) in deploy_script
    assert "previous release virtual environment" in workflow
    assert "tools/ensure_quality_gate.py" in deploy_script
    assert "--task-type analysis_premarket_external" not in deploy_script
    assert 'find "$tree_root"' in workflow
    assert '! -type l -perm /0222 -print -quit' in workflow
    assert "-perm /0222 -print -quit" in workflow
    assert "-writable -print -quit 2>/dev/null || true" in workflow
    validate_boundary = deploy_script.index(
        "tools/validate_production_release_boundary.py"
    )
    validate_boundary_env = deploy_script.rindex(
        "PYTHONDONTWRITEBYTECODE=1", 0, validate_boundary
    )
    validate_scheduler_manifest = deploy_script.index(
        "tools/ensure_quality_gate.py", validate_boundary
    )
    restart_service = _required_shell_position(
        deploy_script[validate_scheduler_manifest:],
        r'sudo systemctl (?:start|restart) '
        r'(?:"\$MAIN_SERVICE"|probiga)(?:[ \t]|$)',
    ) + validate_scheduler_manifest
    assert validate_boundary - validate_boundary_env < 160
    assert validate_boundary < validate_scheduler_manifest < restart_service
    assert 'sudo -u "$SERVICE_USER" /usr/bin/env -i' in workflow
    assert "tools/ensure_quality_gate.py --validate-review-delivery" in workflow
    assert "--apply-review-delivery-with-snapshot" not in workflow
    assert "--restore-review-delivery" not in workflow


def test_production_deploy_pins_scheduler_flag_in_execstart() -> None:
    deploy_script = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy_script)
    function_bodies = _shell_function_bodies(deploy_script)
    main_dropin = _normalized_shell(function_bodies["write_dropin"])
    scheduler_dropin = _normalized_shell(
        function_bodies["write_scheduler_dropin"]
    )
    worker_dropin = _normalized_shell(
        function_bodies["write_ai_worker_dropin"]
    )

    assert "WorkingDirectory=/opt/ProBigA" in main_dropin
    assert "WorkingDirectory=$code_root" in scheduler_dropin
    assert "WorkingDirectory=/opt/ProBigA" not in scheduler_dropin
    assert "WorkingDirectory=/opt/ProBigA" in worker_dropin
    assert "[Unit]" in scheduler_dropin
    assert "Description=ProBigA standalone scheduler" in scheduler_dropin
    assert "Restart=on-failure" in scheduler_dropin
    assert "[Install]" in scheduler_dropin
    assert "WantedBy=multi-user.target" in scheduler_dropin
    assert (
        "SCHEDULER_UNIT=/etc/systemd/system/probiga-scheduler.service"
        in deploy_script
    )
    assert 'test "$SCHEDULER_UNIT_PRESENT" -eq 1' not in deploy_script
    assert "SCHEDULER_UNIT_TOUCHED=0" in deploy_script
    assert "disable first-install scheduler unit" in deploy_script
    assert "first-install scheduler remained active after rollback" in deploy_script
    assert "first-install scheduler remained enabled after rollback" in deploy_script
    assert 'PREVIOUS_MAIN_PID="$(systemctl show "$MAIN_SERVICE"' in deploy_script
    assert 'tr \'\\0\' \'\\n\' < "/proc/$PREVIOUS_MAIN_PID/environ"' in deploy_script
    assert "runtime_environment_value PROBIGA_CODE_ROOT" in deploy_script
    assert (
        'PREVIOUS_RELEASE_REVISION="$(runtime_environment_value '
        'PROBIGA_EXPECTED_GIT_SHA)"'
        in deploy_script
    )
    assert '-name "build-$PREVIOUS_RELEASE_REVISION-*" -print' in deploy_script
    assert '"/proc/$PREVIOUS_MAIN_PID/cmdline"' in deploy_script
    assert "runtime_python_argv0" in deploy_script
    assert '"/proc/$PREVIOUS_MAIN_PID/maps"' in deploy_script
    assert "Recovered active release venv link" in deploy_script
    assert "runtime_environment_value PROBIGA_EXPECTED_ADATA_SHA" in deploy_script
    assert "runtime_environment_value PROBIGA_EXPECTED_ADATA_TREE_SHA256" in deploy_script
    assert "runtime_environment_value PROBIGA_ADATA_SOURCE_DIR" in deploy_script
    assert (
        "ExecStart=/usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin "
        "API_EMBEDDED_SCHEDULER_ENABLED=false "
        in main_dropin
    )
    assert (
        "ExecStart=/usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin "
        "API_EMBEDDED_SCHEDULER_ENABLED=false "
        in scheduler_dropin
    )
    assert "PROBIGA_CODE_ROOT=$code_root" in main_dropin
    assert "PROBIGA_BUILD_COMMIT_SHA=$revision" in main_dropin
    assert "PYTHONPATH=$adata_source:$code_root" in main_dropin
    assert "PYTHONSAFEPATH=1" in main_dropin
    assert "$RELEASE_VENV_ROOT/$revision/bin/python -P -m uvicorn" in main_dropin
    assert "server.api.main:app --app-dir $code_root" in main_dropin
    assert "PROBIGA_CODE_ROOT=$code_root" in scheduler_dropin
    assert "PROBIGA_BUILD_COMMIT_SHA=$revision" in scheduler_dropin
    assert "PYTHONPATH=$adata_source:$code_root" in scheduler_dropin
    assert "PYTHONSAFEPATH=1" in scheduler_dropin
    assert (
        "$RELEASE_VENV_ROOT/$revision/bin/python -P "
        "$code_root/tools/run_scheduler_daemon.py"
        in scheduler_dropin
    )
    assert "PROBIGA_CODE_ROOT=$code_root" in worker_dropin
    assert "PYTHONPATH=$adata_source:$code_root" in worker_dropin
    assert "PYTHONSAFEPATH=1" in worker_dropin
    assert (
        "$RELEASE_VENV_ROOT/$revision/bin/python -P "
        "$code_root/tools/run_ai_recommendation_worker.py --once"
        in worker_dropin
    )
    assert "PYTHONPATH=$adata_source:/opt/ProBigA" not in main_dropin
    assert "PYTHONPATH=$adata_source:/opt/ProBigA" not in scheduler_dropin
    assert "PYTHONPATH=$adata_source:/opt/ProBigA" not in worker_dropin
    assert (
        'write_dropin "$EXPECTED_SHA" "$PREPARED_CODE_ROOT"'
        in normalized
    )
    assert (
        'write_scheduler_dropin "$EXPECTED_SHA" "$PREPARED_CODE_ROOT"'
        in normalized
    )
    assert (
        'write_ai_worker_dropin "$EXPECTED_SHA" "$PREPARED_CODE_ROOT"'
        in normalized
    )
    assert (
        "systemctl show probiga-scheduler --property=ExecStart --value"
        not in deploy_script
    )
    assert (
        'cmp --silent "$PREPARED_MAIN_DROPIN" '
        '"$MAIN_RELEASE_DROPIN"'
        in normalized
    )
    assert (
        'cmp --silent "$PREPARED_SCHEDULER_DROPIN" "$SCHEDULER_UNIT"'
        in normalized
    )
    assert (
        'cmp --silent "$PREPARED_AI_WORKER_DROPIN" "$AI_WORKER_DROPIN"'
        in normalized
    )
    assert "CUTOVER_STEP=verify_installed_runtime_units" in deploy_script
    assert "MAIN_RELEASE_DROPIN=/etc/systemd/system/probiga.service.d/scheduler.conf" in deploy_script
    assert "LEGACY_MAIN_OVERRIDE_DROPINS=(" in deploy_script
    assert "MAIN_LIMITS_DROPIN=" in deploy_script
    assert "MAIN_MARKET_RADAR_DROPIN=" in deploy_script
    assert "MAIN_SERVICE_USER_DROPIN=" in deploy_script
    assert 'sudo rm -f "$legacy_main_dropin"' in deploy_script
    assert "main_identity missing_release_dropin=%q" in deploy_script
    assert "main_identity unexpected_dropin=%q" in deploy_script
    assert '"$MAIN_DATABASE_WRITER_GUARD_DROPIN") ;;' in normalized
    assert "main_identity unexpected_argv0=%q" in deploy_script
    assert 'test "${MAIN_CMDLINE[4]}" = server.api.main:app' in deploy_script
    assert 'test "${MAIN_CMDLINE[5]}" = --app-dir' in deploy_script
    assert 'test "${MAIN_CMDLINE[6]}" = "$PREPARED_CODE_ROOT"' in deploy_script
    assert "--workers 2 --limit-concurrency 64 --backlog 256" in deploy_script
    assert (
        "--limit-max-requests 400 --limit-max-requests-jitter 100 "
        "--timeout-keep-alive 5"
        in deploy_script
    )
    assert 'test "${MAIN_CMDLINE[12]}" = 2' in deploy_script
    assert 'test "${MAIN_CMDLINE[18]}" = 400' in deploy_script
    assert 'test "${MAIN_CMDLINE[20]}" = 100' in deploy_script
    assert (
        "/etc/systemd/system/probiga-scheduler.service.d/release.conf"
        in deploy_script
    )
    assert "release-path.conf" in deploy_script
    assert "release-revision.conf" in deploy_script
    assert "zz-probiga-env.conf" in deploy_script
    assert "SCHEDULER_LIMITS_DROPIN=" in deploy_script
    assert 'sudo rm -f "$legacy_scheduler_dropin"' in deploy_script
    assert "CUTOVER_STEP=verify_no_scheduler_dropins" in deploy_script
    assert "scheduler_identity unexpected_dropins=%q" in deploy_script
    assert (
        'EXPECTED_SCHEDULER_DROPIN_PATHS='
        '"$SCHEDULER_DATABASE_WRITER_GUARD_DROPIN $SCHEDULER_LIMITS_DROPIN"'
        in deploy_script
    )
    assert (
        '"$PREVIOUS_LEGACY_SCHEDULER_DROPIN_DIR/'
        '$(basename "$legacy_scheduler_dropin")" '
        '"$legacy_scheduler_dropin"'
        in normalized
    )
    assert (
        "grep -Fx 'Environment=API_EMBEDDED_SCHEDULER_ENABLED=false'"
        in deploy_script
    )
    assert "grep -zFx -- 'API_EMBEDDED_SCHEDULER_ENABLED=false'" in deploy_script
    assert (
        '"PROBIGA_ADATA_SOURCE_DIR=$ADATA_SOURCE" '
        '"/proc/$SCHEDULER_MAIN_PID/environ"'
        in normalized
    )
    assert (
        "grep -zFx -- 'PROBIGA_STRATEGY_GOVERNANCE_MODE=REQUIRED' "
        '"/proc/$SCHEDULER_MAIN_PID/environ"'
        in normalized
    )
    assert (
        'mapfile -d \'\' -t SCHEDULER_CMDLINE '
        '< "/proc/$SCHEDULER_MAIN_PID/cmdline"'
        in normalized
    )
    assert 'EXPECTED_VENV_TARGET="$EXPECTED_BUILD"' in deploy_script
    assert (
        '"$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python"| '
        '"$EXPECTED_VENV_TARGET/bin/python"'
        in normalized
    )
    assert "scheduler_identity unexpected_argv0=%q" in deploy_script
    assert (
        'test "${SCHEDULER_CMDLINE[2]}" = '
        '"$PREPARED_CODE_ROOT/tools/run_scheduler_daemon.py"'
        in normalized
    )
    assert '"/proc/$SERVICE_MAIN_PID/environ"' in deploy_script
    assert '"/proc/$SCHEDULER_MAIN_PID/environ"' in deploy_script
    assert "curl --fail-with-body" in deploy_script
    assert 'cat "$HEALTH_RESPONSE" >&2' in deploy_script
    assert 'release_identity_check 1 "$CODE_VALIDATION_ROOT"' in deploy_script
    assert 'release_identity_check 1 "$PREPARED_CODE_ROOT"' in deploy_script
    assert "PROBIGA_RELEASE_IDENTITY_REQUIRE_CLEAN" in deploy_script
    final_seal = deploy_script.index(
        'seal_release_checkout "$STAGING_WORKTREE"'
    )
    final_identity_check = deploy_script.index(
        'release_identity_check 1 "$PREPARED_CODE_ROOT"'
    )
    assert final_seal < final_identity_check
    assert "GIT_OPTIONAL_LOCKS=0 PYTHONDONTWRITEBYTECODE=1" in deploy_script
    assert "probiga-ai-recommendation-worker.service" in deploy_script
    assert "probiga-ai-recommendation-worker.timer" in deploy_script
    assert (
        "probiga-ai-recommendation-worker.service.d/release-runtime.conf"
        in deploy_script
    )
    assert 'User=$SERVICE_USER' in deploy_script
    assert 'Group=$SERVICE_USER' in deploy_script
    assert 'sudo systemctl stop "$AI_WORKER_TIMER"' in deploy_script
    assert 'sudo systemctl stop "$AI_WORKER_SERVICE"' in deploy_script
    assert 'PREVIOUS_AI_WORKER_DROPIN="$(mktemp)"' in deploy_script
    assert 'sudo cat "$AI_WORKER_DROPIN" > "$PREVIOUS_AI_WORKER_DROPIN"' in deploy_script
    assert "restore previous AI recommendation worker drop-in" in deploy_script
    assert "verify previous AI recommendation worker runtime" in deploy_script
    assert (
        'assert_ai_worker_runtime "$EXPECTED_SHA" '
        '"$RELEASE_VENV_ROOT/$EXPECTED_SHA" "$PREPARED_CODE_ROOT"'
        in normalized
    )
    assert "STATIC_RELEASE_LINK=/opt/ProBigA-current" in deploy_script
    assert 'sudo mv -Tf "$link_build/current" "$STATIC_RELEASE_LINK"' in deploy_script
    assert 'test "$(readlink -f "$STATIC_RELEASE_LINK")" = "$checkout_root"' in deploy_script
    assert 'cmp --silent "$checkout_root/server/static/$asset" "$response"' in deploy_script
    assert "point Nginx static assets at previous code release" in deploy_script
    assert "verify previous Nginx static assets" in deploy_script
    worker_stop = normalized.index(
        'sudo systemctl stop "$AI_WORKER_TIMER"',
        normalized.index("trap 'rollback \"$?\" \"$LINENO\"' ERR"),
    )
    release_publish = normalized.index(
        'git --git-dir="$CODE_GIT_CACHE" worktree move '
        '"$STAGING_WORKTREE" "$PREPARED_CODE_ROOT"'
    )
    worker_pin = normalized.index(
        'write_ai_worker_dropin "$EXPECTED_SHA"', release_publish
    )
    service_restart = _required_shell_position(
        normalized[worker_stop:],
        r'sudo systemctl start '
        r'(?:"\$MAIN_SERVICE"|probiga)(?:[ \t]|$)',
    ) + worker_stop
    static_switch = normalized.index(
        'point_static_release_to_checkout "$PREPARED_CODE_ROOT"',
        normalized.index('cat "$HEALTH_RESPONSE"'),
    )
    worker_restore = normalized.index(
        "restore_ai_worker_previous_state",
        static_switch,
    )
    assert (
        release_publish
        < worker_pin
        < worker_stop
        < service_restart
        < static_switch
        < worker_restore
    )


def test_frozen_crlf_evidence_is_marked_binary_for_git() -> None:
    attributes = (ROOT / ".gitattributes").read_text(encoding="utf-8")
    lines = [
        line.strip()
        for line in attributes.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert lines[0] == "* text=auto eol=lf"
    assert "artifacts/trading_v5/regime_expert_capacity_oos_20260802.json -text" in attributes
    assert "artifacts/trading_v6/multi_sleeve_pit_finance_oos_20260802.json -text" in attributes
    assert "*.sh text eol=lf" in attributes


def test_production_dependency_lock_respects_server_mirror_ceiling() -> None:
    requirements = (ROOT / "requirements-platform.txt").read_text(
        encoding="utf-8"
    )

    assert "charset-normalizer==3.5.0" in requirements
    assert "idna==3.18" in requirements
    assert "uvicorn==0.52.3" in requirements
    assert "uvicorn[standard]" not in requirements


def test_sector_heat_runtime_cache_is_outside_tracked_data() -> None:
    from tools import fetch_sector_heat_east_daily

    assert fetch_sector_heat_east_daily.CACHE_FILE == (
        ROOT / "runtime/cache/east_sector_heat_cache.json"
    )


def test_mutable_runtime_roots_reject_live_git_tracked_files(monkeypatch) -> None:
    assert boundary.MUTABLE_RUNTIME_TRACKED_ALLOWLIST == {"data/.gitkeep"}

    def fake_git(*args):
        if args[:2] == ("ls-files", "-z"):
            return "data/.gitkeep\0runtime/cache/live.json\0"
        if args[:2] == ("diff", "--name-only"):
            return ""
        raise AssertionError(args)

    monkeypatch.setattr(boundary, "_git", fake_git)

    with pytest.raises(
        boundary.ProductionBoundaryError,
        match="runtime/cache/live.json",
    ):
        boundary._validate_mutable_runtime_roots_untracked()


def test_mutable_runtime_roots_allow_only_git_reported_deletions(
    monkeypatch,
) -> None:
    def fake_git(*args):
        if args[:2] == ("ls-files", "-z"):
            return "data/.gitkeep\0data/legacy.log\0"
        if args[:2] == ("diff", "--name-only"):
            return "data/legacy.log\0"
        raise AssertionError(args)

    monkeypatch.setattr(boundary, "_git", fake_git)

    assert boundary._validate_mutable_runtime_roots_untracked() == (
        "data/.gitkeep",
    )


def test_local_only_diagnostics_are_exactly_ignored_and_release_denied() -> None:
    expected = {
        "artifacts/production_selector_readiness_20260811.stdout.json",
        "tools/_diagnose_lithium_and_sim_production.py",
        "tools/_finish_qmt_attestation_20260811.py",
        "tools/_notify_wecom_briefing_production.py",
    }
    ignored = {
        line.strip().removeprefix("/")
        for line in (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
        if line.strip().startswith("/")
    }

    assert boundary.LOCAL_ONLY_RELEASE_PATHS == expected
    assert expected <= ignored


def test_local_only_diagnostic_becoming_tracked_blocks_release(monkeypatch) -> None:
    tracked = "tools/_diagnose_lithium_and_sim_production.py"

    def fake_git(*args):
        assert args[:2] == ("ls-files", "-z")
        assert set(args[3:]) == boundary.LOCAL_ONLY_RELEASE_PATHS
        return tracked + "\0"

    monkeypatch.setattr(boundary, "_git", fake_git)

    with pytest.raises(
        boundary.ProductionBoundaryError,
        match="local-only diagnostics/evidence entered",
    ):
        boundary._validate_local_only_release_paths_untracked()


def test_git_delivery_rejects_ignored_local_only_file(tmp_path, monkeypatch) -> None:
    local_script = tmp_path / "tools/_diagnose_lithium_and_sim_production.py"
    local_script.parent.mkdir()
    local_script.write_text("print('local only')\n", encoding="utf-8")
    monkeypatch.setattr(boundary, "ROOT", tmp_path)
    monkeypatch.setattr(
        boundary,
        "_git",
        lambda *args: "a" * 40
        if args[:2] == ("rev-parse", "HEAD")
        else pytest.fail(f"unexpected Git call after local-only detection: {args}"),
    )

    result = boundary._git_delivery_status(
        (SimpleNamespace(document={}),),
        None,
    )

    assert result["ready"] is False
    assert "local-only diagnostics/evidence exist" in result["reason"]


def test_git_delivery_protects_scripts_and_configs_but_excludes_runtime_data(
    monkeypatch,
) -> None:
    assert "scripts" in boundary.PROTECTED_RELEASE_PATHS
    assert "strategies" in boundary.PROTECTED_RELEASE_PATHS
    assert "data" not in boundary.PROTECTED_RELEASE_PATHS
    monkeypatch.setattr(boundary, "_required_release_files", lambda _items: {"x"})
    monkeypatch.setattr(boundary, "_untracked_root_shadow_files", lambda: [])
    monkeypatch.setattr(boundary, "_local_only_release_paths_present", lambda: ())

    def fake_git(*args):
        if args[:2] == ("rev-parse", "HEAD"):
            return "a" * 40
        if args[:2] == ("ls-files", "--"):
            return "x"
        if args and args[0] == "status":
            assert "--untracked-files=all" in args
            assert "scripts" in args
            assert "data" not in args
            return ""
        if args[:3] == ("ls-files", "--others", "--exclude-standard"):
            return ""
        raise AssertionError(args)

    monkeypatch.setattr(boundary, "_git", fake_git)
    result = boundary._git_delivery_status((SimpleNamespace(document={}),), None)
    assert result["ready"] is True


def test_git_delivery_rejects_ignored_root_import_shadow(monkeypatch) -> None:
    monkeypatch.setattr(boundary, "_required_release_files", lambda _items: {"x"})
    monkeypatch.setattr(boundary, "_local_only_release_paths_present", lambda: ())
    monkeypatch.setattr(
        boundary,
        "_untracked_root_shadow_files",
        lambda: ["uvicorn.pyc"],
    )

    def fake_git(*args):
        if args[:2] == ("rev-parse", "HEAD"):
            return "a" * 40
        if args[:2] == ("ls-files", "--"):
            return "x"
        if args and args[0] == "status":
            return ""
        if args[:3] == ("ls-files", "--others", "--exclude-standard"):
            return ""
        raise AssertionError(args)

    monkeypatch.setattr(boundary, "_git", fake_git)
    result = boundary._git_delivery_status((SimpleNamespace(document={}),), None)
    assert result["ready"] is False
    assert "root import shadows" in result["reason"]


def test_boundary_bytecode_scan_recurses_strategies_and_versions(
    tmp_path,
    monkeypatch,
) -> None:
    strategy_cache = tmp_path / "strategies" / "__pycache__"
    version_cache = tmp_path / "versions" / "release" / "__pycache__"
    extension_package = tmp_path / "numpy"
    strategy_cache.mkdir(parents=True)
    version_cache.mkdir(parents=True)
    extension_package.mkdir()
    (strategy_cache / "runner.cpython-314.pyc").write_bytes(b"pyc")
    (version_cache / "manifest.cpython-314.pyo").write_bytes(b"pyo")
    (extension_package / "__init__.cp314-win_amd64.pyd").write_bytes(b"pyd")
    monkeypatch.setattr(boundary, "ROOT", tmp_path)
    monkeypatch.setattr(boundary, "_git", lambda *_args: "")

    found = boundary._untracked_root_shadow_files()

    assert "strategies/__pycache__/runner.cpython-314.pyc" in found
    assert "versions/release/__pycache__/manifest.cpython-314.pyo" in found
    assert "numpy/__init__.cp314-win_amd64.pyd" in found


def test_production_deploy_has_a_fixed_tls_database_window_runner_only() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    schema_tool = (
        ROOT / "tools/prepare_strategy_governance_schema.py"
    ).read_text(encoding="utf-8")
    function_bodies = _shell_function_bodies(deploy_script)

    migration_runner = _normalized_shell(
        function_bodies["run_prepared_database_migration_tool"]
    )
    runtime_runner = _normalized_shell(
        function_bodies["run_prepared_python_tool"]
    )
    runtime_units = "\n".join(
        function_bodies[name]
        for name in (
            "write_dropin",
            "write_scheduler_dropin",
            "write_ai_worker_dropin",
        )
    )

    assert (
        'ADMIN_OPTION_FILE = Path("/etc/probiga/mysql-trigger-admin.ini")'
        in schema_tool
    )
    assert (
        'MIGRATOR_OPTION_FILE = Path("/etc/probiga/mysql-migrator.ini")'
        in schema_tool
    )
    assert 'FIXED_TLS_CA_FILE = Path("/etc/probiga/mysql84-ca.pem")' in schema_tool
    assert "/etc/probiga/mysql-ca.pem" not in deploy_script
    assert "/etc/probiga/mysql-ca.pem" not in schema_tool
    assert 'EXPECTED_ADMIN_USER = "probiga_trigger_admin@127.0.0.1"' in schema_tool
    assert 'EXPECTED_MIGRATOR_USER = "probiga_migrator@127.0.0.1"' in schema_tool
    assert 'EXPECTED_CLIENT_ENDPOINT_PORT = 13306' in schema_tool
    assert 'EXPECTED_SERVER_PORT = 3306' in schema_tool
    assert 'values["port"] != str(EXPECTED_CLIENT_ENDPOINT_PORT)' in schema_tool
    assert 'state.server_port != EXPECTED_SERVER_PORT' in schema_tool
    assert 'values["protocol"].casefold() != "tcp"' in schema_tool
    assert "ssl_ca=str(ssl_ca)" in schema_tool
    assert "ssl_verify_cert=True" in schema_tool
    assert "prepare_strategy_governance_schema.py" in migration_runner
    assert '[ "$1" = --phase ] && [ "$2" = preflight ]' in migration_runner
    assert '[ "$1" = --phase ] && [ "$2" = recover ]' in migration_runner
    assert '[ "$1" = --phase ]' in migration_runner
    assert '[ "$2" = cutover ]' in migration_runner
    assert '[ "$2" = resume ]' in migration_runner
    assert '[ "$3" = --writers-fenced ]' in migration_runner
    assert (
        "database migration runner rejected non-allowlisted arguments"
        in migration_runner
    )
    assert 'test "$entrypoint" = ' in migration_runner
    assert '"$entrypoint" "$@"' in migration_runner
    assert '"PYTHONPATH=$PREPARED_CODE_ROOT"' in migration_runner
    assert (
        'PROBIGA_PREVIOUS_GIT_SHA="$PREVIOUS_RELEASE_REVISION"'
        in migration_runner
    )
    assert '"PYTHONPATH=$ADATA_SOURCE:$PREPARED_CODE_ROOT"' not in migration_runner
    assert "run_prepared_database_migration_tool" not in runtime_runner
    assert 'cd "$PREPARED_CODE_ROOT" || return 1' in runtime_runner
    for forbidden in (
        "SET GLOBAL",
        "privileged_ddl_executor",
        "prepare_strategy_governance_schema.py",
    ):
        assert forbidden not in runtime_runner
        assert forbidden not in runtime_units

    ordinary_runtime_source = "\n".join(
        (migration_runner, runtime_runner, runtime_units, deploy_script)
    )
    assert not re.search(
        r"\bSET\s+GLOBAL\s+log_bin_trust_function_creators\b",
        ordinary_runtime_source,
        flags=re.IGNORECASE,
    )
    assert len(
        re.findall(
            r"SET\s+GLOBAL\s+log_bin_trust_function_creators\s*=\s*(?:ON|OFF)",
            schema_tool,
            flags=re.IGNORECASE,
        )
    ) == 2
    assert not re.search(
        r"\bGRANT\s+SUPER\b", schema_tool, flags=re.IGNORECASE
    )
    assert not re.search(
        r"(?:mysql|mariadb)://[^\s'\"]+:[^\s@'\"]+@",
        "\n".join((deploy_script, schema_tool)),
        flags=re.IGNORECASE,
    )
    assert schema_tool.count("SHOW GRANTS FOR CURRENT_USER()") >= 2
    assert (
        "runtime identity privileges differ from the audited boundary"
        in schema_tool
    )
    for exact_runtime_policy in (
        'TARGET_RUNTIME_PRIVILEGE_CONTRACT = "TARGET_LEAST_PRIVILEGE"',
        'LEGACY_RUNTIME_PRIVILEGE_CONTRACT = "LEGACY_DDL_COMPATIBILITY"',
        '"BIGA.*": frozenset({"SELECT"})',
        '"PROBIGA.*": frozenset({',
        '"PROBIGA_QMT_HISTORY.*": frozenset({"SELECT"})',
        '"CREATE TEMPORARY TABLES"',
        '"ALTER"',
        '"CREATE"',
        '"DROP"',
        '"INDEX"',
        '"REFERENCES"',
        "len(global_entries) != 1",
        'global_entries[0] != {"USAGE"}',
        "observed_schema == TARGET_RUNTIME_SCHEMA_PRIVILEGES",
        "observed_schema == LEGACY_RUNTIME_SCHEMA_PRIVILEGES",
        '"observed_contract": observed_contract',
        '"persistent_ddl_privileges": persistent_ddl_privileges',
        '"require_ssl": True',
        '"roles": []',
        '"grant_option": False',
    ):
        assert exact_runtime_policy in schema_tool


def test_database_migration_runner_passes_exact_previous_release_to_child(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is unavailable")
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    body = _shell_function_bodies(deploy_script)[
        "run_prepared_database_migration_tool"
    ]
    prepared_root = tmp_path / "prepared"
    entrypoint = prepared_root / "tools" / "prepare_strategy_governance_schema.py"
    expected_sha = "a" * 40
    previous_sha = "b" * 40
    fake_python = tmp_path / "releases" / expected_sha / "bin" / "python"
    receipt = tmp_path / "migration-env.txt"
    entrypoint.parent.mkdir(parents=True)
    fake_python.parent.mkdir(parents=True)
    entrypoint.write_text("# sealed test entrypoint\n", encoding="utf-8")
    fake_python.write_text(
        "#!/usr/bin/env bash\n"
        f"printf '%s' \"$PROBIGA_PREVIOUS_GIT_SHA|"
        "$PROBIGA_EXPECTED_GIT_SHA|$PROBIGA_BUILD_COMMIT_SHA\" > "
        f"'{receipt.as_posix()}'\n",
        encoding="utf-8",
    )
    fake_python.chmod(fake_python.stat().st_mode | stat.S_IEXEC)

    harness = tmp_path / "migration-runner.sh"
    harness.write_text(
        "#!/usr/bin/env bash\nset -euo pipefail\n"
        "stat() { printf 'root\\n'; }\n"
        "sudo() { return 0; }\n"
        f"PREPARED_CODE_ROOT='{prepared_root.as_posix()}'\n"
        f"RELEASE_VENV_ROOT='{(tmp_path / 'releases').as_posix()}'\n"
        f"EXPECTED_SHA='{expected_sha}'\n"
        f"PREVIOUS_RELEASE_REVISION='{previous_sha}'\n"
        "SERVICE_USER='probiga'\n"
        "EXPECTED_ADATA_SHA='c'\n"
        "EXPECTED_ADATA_TREE_SHA256='d'\n"
        "ADATA_SOURCE='/sealed/adata'\n"
        "EXPECTED_RELEASE_TREE_SHA256='e'\n"
        "EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256='f'\n"
        "run_prepared_database_migration_tool() {\n"
        f"{body}\n"
        "}\n"
        "run_prepared_database_migration_tool "
        '"$PREPARED_CODE_ROOT/tools/prepare_strategy_governance_schema.py" '
        "--phase cutover --writers-fenced\n",
        encoding="utf-8",
    )

    completed = subprocess.run(
        [bash, str(harness)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert receipt.read_text(encoding="utf-8") == (
        f"{previous_sha}|{expected_sha}|{expected_sha}"
    )


def test_strategy_schema_preflight_cutover_and_recovery_order_fail_closed() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    initial_runner = _shell_function_bodies(deploy_script)[
        "run_initial_database_schema_preflight"
    ]
    history_tool = (
        ROOT / "tools/prepare_strategy_governance_qmt_history.py"
    ).read_text(encoding="utf-8")
    history_start = history_tool.index("def prepare_governance_qmt_history(")
    history_end = history_tool.index("def main(", history_start)
    production_history = history_tool[history_start:history_end]
    runner_definition = deploy_script.index(
        "run_prepared_database_migration_tool() {"
    )
    runner_calls = [
        match.start()
        for match in re.finditer(
            r"(?m)^[ \t]*run_prepared_database_migration_tool\b",
            deploy_script,
        )
        if match.start() > runner_definition
    ]
    assert len(runner_calls) == 5
    (
        preflight_call,
        fenced_preflight_call,
        resume_call,
        cutover_call,
        recover_call,
    ) = runner_calls
    restore_journal = deploy_script.index(
        "CUTOVER_STEP=persist_database_writer_restore_journal"
    )
    cutover_fence = deploy_script.index("CUTOVER_STARTED=1")
    first_service_stop = _required_shell_position(
        deploy_script[cutover_fence:], r"sudo systemctl stop\b"
    ) + cutover_fence
    guard_dropins = deploy_script.index(
        "CUTOVER_STEP=install_database_writer_guard_dropins", restore_journal
    )
    guard_file = deploy_script.index(
        "CUTOVER_STEP=persist_database_writer_guard", guard_dropins
    )
    guard_daemon_reload = deploy_script.index(
        "sudo systemctl daemon-reload", guard_file
    )
    writer_fence = deploy_script.index(
        "CUTOVER_STEP=writer_fence_before_api_stop", cutover_fence
    )
    fenced_selector_step = deploy_script.index(
        "CUTOVER_STEP=select_strategy_governance_database_schema_phase",
        writer_fence,
    )
    fenced_selector_call = deploy_script.index(
        "select_fenced_strategy_governance_schema_phase",
        fenced_selector_step,
    )
    schema_step = deploy_script.index(
        "CUTOVER_STEP=prepare_strategy_governance_database_schema",
        fenced_selector_call,
    )
    stage_trading_v3_tasks = deploy_script.index(
        "CUTOVER_STEP=stage_trading_v3_tasks_disabled", recover_call
    )
    qmt_history = deploy_script.index(
        "CUTOVER_STEP=prepare_strategy_governance_qmt_history", recover_call
    )
    task_install = deploy_script.index(
        '"$PREPARED_CODE_ROOT/tools/add_strategy_governance_task.py"',
        qmt_history,
    )
    install_units = deploy_script.index(
        "CUTOVER_STEP=install_runtime_units", qmt_history
    )
    activation_journal_sync = deploy_script.index(
        "CUTOVER_STEP=sync_activation_journal_before_guard_removal",
        install_units,
    )
    guard_removal = deploy_script.index(
        "CUTOVER_STEP=remove_database_writer_guard_after_full_prestart",
        activation_journal_sync,
    )
    api_start = deploy_script.index("CUTOVER_STEP=start_api", guard_removal)

    assert (
        preflight_call
        < restore_journal
        < cutover_fence
        < writer_fence
        < first_service_stop
    )
    assert (
        restore_journal
        < guard_dropins
        < guard_file
        < guard_daemon_reload
        < writer_fence
        < first_service_stop
        < fenced_selector_step
        < fenced_selector_call
        < schema_step
        < resume_call
        < cutover_call
        < recover_call
        < stage_trading_v3_tasks
        < qmt_history
        < install_units
        < task_install
        < activation_journal_sync
        < guard_removal
        < api_start
    )
    preflight_command = _normalized_shell(initial_runner)
    fenced_preflight_command = _normalized_shell(
        _shell_function_bodies(deploy_script)[
            "select_fenced_strategy_governance_schema_phase"
        ]
    )
    cutover_command = _normalized_shell(
        deploy_script[schema_step:recover_call]
    )
    recover_command = _normalized_shell(
        deploy_script[recover_call:qmt_history]
    )
    assert "prepare_strategy_governance_schema.py" in preflight_command
    assert "--phase preflight" in preflight_command
    assert "--writers-fenced" not in preflight_command
    assert "--phase preflight" in fenced_preflight_command
    assert "--writers-fenced" not in fenced_preflight_command
    assert "validate_initial_database_schema_preflight_json" in (
        fenced_preflight_command
    )
    assert "resume_required" in fenced_preflight_command
    assert "prepare_strategy_governance_schema.py" in cutover_command
    assert "--phase resume --writers-fenced" in cutover_command
    assert "--phase cutover --writers-fenced" in cutover_command
    assert "prepare_strategy_governance_schema.py" in recover_command
    assert "--phase recover" in recover_command
    assert "add_trading_v3_tasks.py" in recover_command
    assert "--writer-fence" in recover_command
    assert "prepare_strategy_governance_qmt_history.py" not in preflight_command
    assert "sync_guojin_qmt_reference_data.py" not in preflight_command
    assert "--readiness-only" in recover_command
    assert "sync_guojin_qmt_reference_data.py" not in recover_command
    assert "run_qmt_windows_edge_release_bootstrap.py" in recover_command
    assert "--request --expected-build-sha \"$EXPECTED_SHA\"" in (
        recover_command
    )
    assert "--identity-only --expected-build-sha \"$EXPECTED_SHA\"" in (
        recover_command
    )
    assert "--release-bootstrap-only" in recover_command
    assert "--apply" not in preflight_command
    assert "--schema-only" not in preflight_command
    assert "ensure_attestation_tables(" not in production_history
    assert "plan_legacy_completed_run_binding(" in production_history
    assert "validate_attestation_schema(" in production_history
    assert "schema_prepared=True" in production_history

    task_command_end = deploy_script.index("; then", task_install)
    task_command = _normalized_shell(
        deploy_script[task_install:task_command_end]
    )
    assert "--schema-prepared" in task_command
    task_path_line_start = deploy_script.rfind("\n", qmt_history, task_install)
    task_line_start = deploy_script.rfind(
        "\n", qmt_history, task_path_line_start
    )
    assert "run_prepared_python_tool" in deploy_script[
        task_line_start:task_install
    ]

    rollback_start = deploy_script.index("rollback() {")
    prepare_failure_start = deploy_script.index(
        'if [ "$CUTOVER_STARTED" -eq 0 ]; then', rollback_start
    )
    prepare_failure_end = deploy_script.index(
        'echo "Deployment failed; rolling back to $PREVIOUS_SHA"',
        prepare_failure_start,
    )
    prepare_failure_path = deploy_script[
        prepare_failure_start:prepare_failure_end
    ]
    scheduler_restore = prepare_failure_path.index(
        'if [ "${PRE_CUTOVER_SCHEDULER_STOPPED:-0}" -eq 1 ]'
    )
    scheduler_restore_end = prepare_failure_path.index(
        "PRE_CUTOVER_SCHEDULER_STOPPED=0", scheduler_restore
    )
    assert "sudo systemctl start probiga-scheduler" in prepare_failure_path[
        scheduler_restore:scheduler_restore_end
    ]
    assert "systemctl stop" not in prepare_failure_path[
        scheduler_restore_end:
    ]
    assert 'exit "$failed_status"' in prepare_failure_path


def test_uncertified_failed_cutover_guard_persists_and_never_restarts_any_writer() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy_script)

    assert "DATABASE_GUARD_MIGRATION_UNVERIFIED=0" in deploy_script
    restore_journal = deploy_script.index(
        "CUTOVER_STEP=persist_database_writer_restore_journal"
    )
    latch_on = deploy_script.index(
        "DATABASE_GUARD_MIGRATION_UNVERIFIED=1",
        restore_journal,
    )
    guard_dropins = deploy_script.index(
        "CUTOVER_STEP=install_database_writer_guard_dropins", latch_on
    )
    guard_persist = deploy_script.index(
        "persist_database_writer_guard", guard_dropins
    )
    first_stop = _required_shell_position(
        deploy_script[guard_persist:], r"sudo systemctl (?:stop|disable)\b"
    ) + guard_persist
    schema_step = deploy_script.index(
        "CUTOVER_STEP=prepare_strategy_governance_database_schema",
        first_stop,
    )
    cutover_call = deploy_script.index(
        "run_prepared_database_migration_tool", latch_on
    )
    recover_step = deploy_script.index(
        "CUTOVER_STEP=recover_strategy_governance_database_trust",
        cutover_call,
    )
    recover_call = deploy_script.index(
        "run_prepared_database_migration_tool", recover_step
    )
    qmt_history = deploy_script.index(
        "CUTOVER_STEP=prepare_strategy_governance_qmt_history", recover_call
    )
    guard_removal = deploy_script.index(
        "remove_database_writer_guard_after_recovery", qmt_history
    )
    latch_off = deploy_script.index(
        "DATABASE_GUARD_MIGRATION_UNVERIFIED=0", guard_removal
    )
    install_units = deploy_script.index("CUTOVER_STEP=install_runtime_units")
    assert (
        restore_journal
        < latch_on
        < guard_dropins
        < guard_persist
        < first_stop
        < schema_step
        < cutover_call
        < recover_step
        < recover_call
        < qmt_history
        < install_units
        < guard_removal
        < latch_off
    )

    assert "DATABASE_WRITER_GUARD_DIR=/var/lib/probiga/deploy-guards" in deploy_script
    assert "ConditionPathExists=!$DATABASE_WRITER_GUARD_FILE" in deploy_script
    for guard_dropin in (
        "MAIN_DATABASE_WRITER_GUARD_DROPIN",
        "SCHEDULER_DATABASE_WRITER_GUARD_DROPIN",
        "AI_SERVICE_DATABASE_WRITER_GUARD_DROPIN",
        "AI_TIMER_DATABASE_WRITER_GUARD_DROPIN",
    ):
        assert guard_dropin in deploy_script
    assert "sudo chown root:root \"$guard_tmp\"" in deploy_script
    assert "sudo chmod 0600 \"$guard_tmp\"" in deploy_script
    assert "sudo sync -f \"$DATABASE_WRITER_GUARD_FILE\"" in deploy_script
    assert 'sync -f "$DATABASE_WRITER_RESTORE_FILE"' in deploy_script
    assert 'sync -f "$DATABASE_WRITER_GUARD_DIR"' in deploy_script

    rollback_start = deploy_script.index("rollback() {")
    rollback_end = deploy_script.index(
        "trap 'rollback \"$?\" \"$LINENO\"' ERR", rollback_start
    )
    rollback = _normalized_shell(deploy_script[rollback_start:rollback_end])
    assert 'write_receipt "BLOCKED_DATABASE_GUARDS"' in rollback
    assert rollback.count("DATABASE_GUARD_MIGRATION_UNVERIFIED") >= 6
    assert "DATABASE_WRITER_GUARD_PERSISTED" in rollback
    assert '[ -e "$DATABASE_WRITER_GUARD_FILE" ]' in rollback
    assert "persistent database writer guard was removed or changed" in rollback
    assert "database writer guard drop-ins were removed or unloaded" in rollback
    assert "remove_database_writer_guard_after_recovery" not in rollback
    assert 'rm -f -- "$DATABASE_WRITER_GUARD_FILE"' not in rollback
    assert re.search(
        r'DATABASE_GUARD_MIGRATION_UNVERIFIED" -eq 1[^\n]*'
        r'(?:\n|.)*?systemctl stop "\$MAIN_SERVICE"',
        rollback,
    )
    assert re.search(
        r'DATABASE_GUARD_MIGRATION_UNVERIFIED" -eq 1[^\n]*'
        r'(?:\n|.)*?systemctl disable probiga-scheduler',
        rollback,
    )
    assert re.search(
        r'DATABASE_GUARD_MIGRATION_UNVERIFIED" -eq 1[^\n]*'
        r'(?:\n|.)*?systemctl disable "\$AI_WORKER_TIMER"',
        rollback,
    )
    assert re.search(
        r'DATABASE_GUARD_MIGRATION_UNVERIFIED" -eq 1[^\n]*'
        r'(?:\n|.)*?systemctl disable "\$AI_WORKER_SERVICE"',
        rollback,
    )
    assert "probiga restarted after database writer block" in rollback
    assert "probiga-scheduler restarted after database writer block" in rollback
    assert "assert_ai_worker_writer_fence" in rollback


def test_ci_normal_rollback_releases_guard_only_after_exact_certification() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    bodies = {
        name: _normalized_shell(body)
        for name, body in _shell_function_bodies(deploy_script).items()
    }
    rollback_start = deploy_script.index("rollback() {")
    rollback_end = deploy_script.index(
        "trap 'rollback \"$?\" \"$LINENO\"' ERR", rollback_start
    )
    rollback = _normalized_shell(deploy_script[rollback_start:rollback_end])
    release = bodies["prepared_v2_rollback_release_database_guard"]
    finalize = bodies["controlled_guard_restore_and_finalize"]

    for requirement in (
        'test "$DEPLOY_OPERATION" = deploy',
        'test "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1',
        'test "$EXTERNAL_WRITER_BLOCKED" -eq 0',
        'test "$DATABASE_GUARD_MIGRATION_UNVERIFIED" -eq 1',
        'test "$DATABASE_WRITER_GUARD_PERSISTED" -eq 1',
        'test "$DATABASE_WRITER_RESTORE_PERSISTED" -eq 1',
    ):
        assert requirement in release

    restore_file = release.index("controlled_guard_assert_restore_file")
    boundary = release.index("controlled_guard_assert_boundary", restore_file)
    restore_old = release.index("activation_snapshot_restore_old_set", boundary)
    daemon_reload = release.index("systemctl daemon-reload", restore_old)
    old_set = release.index("activation_snapshot_assert_old_set", daemon_reload)
    post_restore_boundary = release.index(
        "controlled_guard_assert_boundary", old_set
    )
    projected_verify = release.index(
        "controlled_guard_governance_contract_snapshot verify",
        post_restore_boundary,
    )
    assert "rollback-governance" in release[
        projected_verify:projected_verify + 300
    ]
    qmt_projected_verify = release.index(
        "controlled_guard_governance_contract_snapshot verify",
        projected_verify + 1,
    )
    assert "rollback-qmt" in release[
        qmt_projected_verify:qmt_projected_verify + 300
    ]
    cross_runtime_verify = release.index(
        "controlled_guard_capture_current_governance_snapshot",
        qmt_projected_verify,
    )
    triggers = release.index(
        "assert_scheduler_triggers_quiescent", cross_runtime_verify
    )
    cleanup = release.index("controlled_guard_cleanup", triggers)
    assert (
        restore_file
        < boundary
        < restore_old
        < daemon_reload
        < old_set
        < post_restore_boundary
        < projected_verify
        < qmt_projected_verify
        < cross_runtime_verify
        < triggers
        < cleanup
    )
    assert 'test "$old_runtime_sha" = "$PREVIOUS_RELEASE_REVISION"' in release

    rollback_gate = rollback.index(
        '[ "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1 ]'
    )
    release_call = rollback.index(
        "prepared_v2_rollback_release_database_guard", rollback_gate
    )
    guard_clear = rollback.index(
        "DATABASE_WRITER_GUARD_PERSISTED=0", release_call
    )
    migration_clear = rollback.index(
        "DATABASE_GUARD_MIGRATION_UNVERIFIED=0", guard_clear
    )
    first_main_start = rollback.index(
        'sudo systemctl start "$MAIN_SERVICE"', migration_clear
    )
    assert rollback_gate < release_call < guard_clear < migration_clear
    assert migration_clear < first_main_start
    assert (
        '"certify and release the v2 database guard for previous runtime"'
        in rollback[release_call:guard_clear]
    )
    for gate in (
        '[ "$restoration_ready" -eq 1 ]',
        '[ "$EXTERNAL_WRITER_BLOCKED" -eq 0 ]',
        '[ "$DATABASE_GUARD_MIGRATION_UNVERIFIED" -eq 1 ]',
    ):
        assert gate in rollback[rollback_gate - 300:release_call]

    finalize_call = rollback.index(
        "controlled_guard_restore_and_finalize", first_main_start
    )
    runtime_route = rollback.index(
        'case "$DEPLOY_ARTIFACT_MODE" in', first_main_start
    )
    ci_route = rollback.index(
        "ci-resolved-freeze-v1) guard_governance_runtime=prepared",
        runtime_route,
    )
    assert runtime_route < ci_route < finalize_call
    assert (
        '"$guard_governance_runtime"; then'
        in rollback[finalize_call:finalize_call + 500]
    )
    assert 'local governance_runtime="${6:-controlled}"' in finalize
    assert 'test "$DEPLOY_OPERATION" = deploy' in finalize
    assert 'test "$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1' in finalize
    assert 'test "$guarded_sha" = "$EXPECTED_SHA"' in finalize
    controlled_restore = finalize.index(
        "controlled_guard_restore_and_verify_governance_snapshot"
    )
    prepared_finalize_verify = finalize.rindex(
        "controlled_guard_governance_contract_snapshot verify"
    )
    writer_restore = finalize.index("controlled_guard_restore_previous_writer_states")
    assert controlled_restore < writer_restore
    assert prepared_finalize_verify < writer_restore
    assert "any drift must re-fence" in finalize

    # Persistent recovery never opts into the same-process prepared runtime.
    for name in (
        "controlled_database_guard_recovery",
        "controlled_database_writer_restore_recovery",
    ):
        recovery = bodies[name]
        recovery_finalize = recovery.index("controlled_guard_restore_and_finalize")
        assert " prepared" not in recovery[
            recovery_finalize:recovery_finalize + 300
        ]


def test_static_wheel_normal_rollback_uses_controlled_governance_runtime() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    rollback_start = deploy_script.index("rollback() {")
    rollback_end = deploy_script.index(
        "trap 'rollback \"$?\" \"$LINENO\"' ERR", rollback_start
    )
    rollback = _normalized_shell(deploy_script[rollback_start:rollback_end])
    triggers = rollback.index("assert_scheduler_triggers_quiescent")
    runtime_route = rollback.index(
        'case "$DEPLOY_ARTIFACT_MODE" in', triggers
    )
    finalizer_gate = rollback.index(
        'if [ "$rollback_failed" -eq 0 ]', runtime_route
    )
    route = rollback[runtime_route:finalizer_gate]
    finalize_call = rollback.index(
        "controlled_guard_restore_and_finalize", finalizer_gate
    )

    assert (
        "ci-resolved-freeze-v1) guard_governance_runtime=prepared ;;"
        in route
    )
    assert (
        "static-wheel-lock-v2) guard_governance_runtime=controlled ;;"
        in route
    )
    assert (
        '*) rollback_failure "select the rollback governance runtime" ;;'
        in route
    )
    assert (
        '"$guard_governance_runtime"; then'
        in rollback[finalize_call:finalize_call + 500]
    )
    assert " prepared; then" not in rollback[finalize_call:finalize_call + 500]


def test_activation_journal_closes_every_cutover_guard_gap() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy_script)
    function_bodies = {
        name: _normalized_shell(body)
        for name, body in _shell_function_bodies(deploy_script).items()
    }
    cutover = normalized.index(
        "CUTOVER_STEP=persist_database_writer_restore_journal"
    )
    first_unit_mutation = _required_shell_position(
        normalized[cutover:], r"sudo systemctl (?:stop|disable)\b"
    ) + cutover
    journal_write = normalized.index(
        "\npersist_database_writer_restore_journal\n", cutover
    ) + 1
    marker_write = normalized.index(
        "\npersist_database_writer_guard\n", journal_write
    ) + 1
    assert cutover < journal_write < marker_write < first_unit_mutation
    journal_writer = function_bodies["persist_database_writer_restore_journal"]
    journal_syncer = function_bodies["controlled_guard_sync_activation_journal"]
    marker_writer = function_bodies["persist_database_writer_guard"]
    assert "controlled_guard_write_restore_file" in journal_writer
    assert "controlled_guard_sync_activation_journal" in journal_writer
    assert 'sync -f "$DATABASE_WRITER_RESTORE_FILE"' in journal_syncer
    assert 'sync -f "$DATABASE_WRITER_GUARD_DIR"' in journal_syncer
    assert marker_writer.index('mv -fT "$guard_tmp"') < marker_writer.index(
        'sync -f "$DATABASE_WRITER_GUARD_FILE"'
    )

    qmt = normalized.index(
        "CUTOVER_STEP=prepare_strategy_governance_qmt_history",
        first_unit_mutation,
    )
    runtime_install = normalized.index("CUTOVER_STEP=install_runtime_units", qmt)
    task_install = normalized.index("CUTOVER_STEP=install_strategy_governance", qmt)
    prestart = normalized.index(
        "CUTOVER_STEP=verify_strategy_governance_before_start", task_install
    )
    activation_sync = normalized.index(
        "CUTOVER_STEP=sync_activation_journal_before_guard_removal", prestart
    )
    guard_removal = normalized.index(
        "CUTOVER_STEP=remove_database_writer_guard_after_full_prestart",
        activation_sync,
    )
    api_start = normalized.index("CUTOVER_STEP=start_api", guard_removal)
    assert (
        qmt
        < runtime_install
        < task_install
        < prestart
        < activation_sync
        < guard_removal
        < api_start
    )

    ai_restore_step = normalized.index(
        "CUTOVER_STEP=restore_ai_worker_previous_state", api_start
    )
    ai_restore = normalized.index(
        "restore_ai_worker_previous_state",
        ai_restore_step + len("CUTOVER_STEP=restore_ai_worker_previous_state"),
    )
    scheduler_quiescence_step = normalized.index(
        "CUTOVER_STEP=verify_scheduler_triggers_quiescent", ai_restore
    )
    scheduler_quiescence = normalized.index(
        "assert_scheduler_triggers_quiescent", scheduler_quiescence_step
    )
    quality_gate_step = normalized.index(
        "CUTOVER_STEP=verify_premarket_quality_gate", scheduler_quiescence
    )
    quality_gate = normalized.index(
        '"$PREPARED_CODE_ROOT/tools/ensure_quality_gate.py"', quality_gate_step
    )
    receipt_pending = normalized.index(
        "persist_deployed_receipt_pending", quality_gate
    )
    receipt = normalized.index(
        "publish_deployed_receipt_pending", receipt_pending
    )
    journal_cleanup = normalized.rindex(
        "controlled_guard_finalize_successful_activation", receipt_pending, receipt
    )
    journal_remove = normalized.index(
        "activation_snapshot_remove_finalized_before_deploy", receipt
    )
    assert (
        api_start
        < ai_restore_step
        < ai_restore
        < scheduler_quiescence_step
        < scheduler_quiescence
        < quality_gate_step
        < quality_gate
        < receipt_pending
    )
    assert receipt_pending < journal_cleanup
    assert journal_cleanup < receipt < journal_remove
    cutover_body = normalized[cutover:journal_cleanup]
    assert 'rm -f -- "$DATABASE_WRITER_RESTORE_FILE"' not in cutover_body


def test_controlled_database_guard_recovery_is_explicit_and_fail_closed() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    function_bodies = _shell_function_bodies(deploy_script)
    recovery = _normalized_shell(
        function_bodies["controlled_database_guard_recovery"]
    )
    cleanup = _normalized_shell(function_bodies["controlled_guard_cleanup"])
    cleanup_restore = _normalized_shell(
        function_bodies["controlled_guard_restore_after_cleanup_failure"]
    )
    restore_file_writer = _normalized_shell(
        function_bodies["controlled_guard_write_restore_file"]
    )
    restore_finalize = _normalized_shell(
        function_bodies["controlled_guard_restore_and_finalize"]
    )
    restore_previous_states = _normalized_shell(
        function_bodies["controlled_guard_restore_previous_writer_states"]
    )
    apply_unit_state = _normalized_shell(
        function_bodies["controlled_guard_apply_unit_state"]
    )
    refence_after_restore_failure = _normalized_shell(
        function_bodies["controlled_guard_refence_after_restore_failure"]
    )
    force_all_writers_fenced = _normalized_shell(
        function_bodies["controlled_guard_force_all_writers_fenced"]
    )
    restore_only_recovery = _normalized_shell(
        function_bodies["controlled_database_writer_restore_recovery"]
    )
    state_record_assertion = _normalized_shell(
        function_bodies["controlled_guard_assert_state_record"]
    )
    guard_inventory = _normalized_shell(
        function_bodies["database_writer_guard_inventory"]
    )
    writer_fence = _normalized_shell(
        function_bodies["controlled_guard_assert_all_writers_fenced"]
    )
    inventory_fence = _normalized_shell(
        function_bodies["controlled_guard_assert_unit_inventory_fenced"]
    )
    normalize_fence = _normalized_shell(
        function_bodies["controlled_guard_normalize_unit_fenced"]
    )
    strict_fence = _normalized_shell(
        function_bodies["controlled_guard_assert_unit_fenced"]
    )
    boundary = _normalized_shell(
        function_bodies["controlled_guard_assert_boundary"]
    )
    dropin_boundary = _normalized_shell(
        function_bodies["controlled_guard_assert_dropin_boundary"]
    )
    dropin_contract = _normalized_shell(
        function_bodies["controlled_guard_assert_dropin_contract"]
    )
    marker_assertion = _normalized_shell(
        function_bodies["controlled_guard_assert_marker"]
    )
    recover_validator = function_bodies["controlled_guard_validate_recover_json"]
    resume_validator = function_bodies["controlled_guard_validate_resume_json"]
    writer_fence_validator = function_bodies[
        "controlled_guard_validate_writer_fence_json"
    ]
    preflight_validator = function_bodies[
        "controlled_guard_validate_preflight_json"
    ]
    guarded_runner = _normalized_shell(
        function_bodies["controlled_guard_run_schema_tool"]
    )
    guarded_writer_fence_runner = _normalized_shell(
        function_bodies["controlled_guard_run_writer_fence"]
    )

    explicit_mode = deploy_script.index(
        '[ "$1" = --recover-database-guard ]'
    )
    recovery_dispatch = deploy_script.index(
        'if [ "$DEPLOY_OPERATION" = recover-database-guard ]; then'
    )
    normal_release_inputs = deploy_script.index(
        ': "${EXPECTED_SHA:?EXPECTED_SHA is required}"'
    )
    stale_guard_rejection = deploy_script.index(
        "persistent database writer guard/restore state requires controlled recovery"
    )
    assert explicit_mode < recovery_dispatch < normal_release_inputs
    assert recovery_dispatch < normal_release_inputs < stale_guard_rejection
    assert 'test "$guarded_sha" = "$PROBIGA_RECOVERY_GUARD_SHA"' in recovery
    assert (
        'test "$guarded_sha" = "$PROBIGA_RECOVERY_GUARD_SHA"'
        in restore_only_recovery
    )

    repair_dropins = recovery.index("controlled_guard_install_dropins")
    repair_reload = recovery.index("systemctl daemon-reload", repair_dropins)
    initial_writer_fence = recovery.index(
        "controlled_guard_force_all_writers_fenced", repair_reload
    )
    initial_boundary = recovery.index(
        "controlled_guard_assert_boundary", initial_writer_fence
    )
    assert repair_dropins < repair_reload < initial_writer_fence < initial_boundary
    assert "release=*" in recovery
    assert '"$CODE_RELEASE_ROOT/$guarded_sha"' in recovery
    assert '"$RELEASE_VENV_ROOT/$guarded_sha"' in recovery
    assert 'git -C "$code_root" rev-parse HEAD' in recovery
    venv_owner = recovery.index(
        'test "$(stat -c \'%U\' "$release_venv_target")" = root'
    )
    venv_seal = recovery.index(
        'controlled_guard_assert_immutable_venv_tree "$release_venv_target"'
    )
    venv_runtime_write_check = recovery.index(
        'sudo -u "$service_user" test ! -w "$release_venv_target"'
    )
    assert venv_owner < venv_seal < venv_runtime_write_check
    assert 'find -P "$release_venv_target"' not in recovery
    assert "--phase" not in recovery
    initial_recover_run = recovery.index(
        'controlled_guard_run_schema_tool "$code_root" '
        '"$release_venv" recover "$guarded_sha"'
    )
    initial_recover_check = recovery.index(
        "controlled_guard_validate_recover_json", initial_recover_run
    )
    post_initial_recover_boundary = recovery.index(
        "controlled_guard_assert_boundary", initial_recover_check
    )
    writer_fence_run = recovery.index(
        "controlled_guard_run_writer_fence", post_initial_recover_boundary
    )
    writer_fence_check = recovery.index(
        "controlled_guard_validate_writer_fence_json", writer_fence_run
    )
    post_writer_fence_boundary = recovery.index(
        "controlled_guard_assert_boundary", writer_fence_check
    )
    resume_run = recovery.index(
        'controlled_guard_run_schema_tool "$code_root" '
        '"$release_venv" resume "$guarded_sha"',
        post_writer_fence_boundary,
    )
    resume_check = recovery.index(
        "controlled_guard_validate_resume_json", resume_run
    )
    post_resume_boundary = recovery.index(
        "controlled_guard_assert_boundary", resume_check
    )
    final_recover_run = recovery.index(
        'controlled_guard_run_schema_tool "$code_root" '
        '"$release_venv" recover "$guarded_sha"',
        post_resume_boundary,
    )
    final_recover_check = recovery.index(
        "controlled_guard_validate_recover_json", final_recover_run
    )
    post_final_recover_boundary = recovery.index(
        "controlled_guard_assert_boundary", final_recover_check
    )
    preflight_run = recovery.index(
        'controlled_guard_run_schema_tool "$code_root" '
        '"$release_venv" preflight "$guarded_sha"',
        post_final_recover_boundary,
    )
    preflight_check = recovery.index("controlled_guard_validate_preflight_json")
    final_boundary = recovery.index(
        "controlled_guard_assert_boundary", preflight_check
    )
    restore_file_write = recovery.index(
        "controlled_guard_write_restore_file", final_boundary
    )
    cleanup_call = recovery.index("controlled_guard_cleanup", restore_file_write)
    restore_finalize_call = recovery.index(
        "controlled_guard_restore_and_finalize", cleanup_call
    )
    assert (
        initial_boundary
        < initial_recover_run
        < initial_recover_check
        < post_initial_recover_boundary
        < writer_fence_run
        < writer_fence_check
        < post_writer_fence_boundary
        < resume_run
        < resume_check
        < post_resume_boundary
        < final_recover_run
        < final_recover_check
        < post_final_recover_boundary
        < preflight_run
        < preflight_check
        < final_boundary
        < restore_file_write
        < cleanup_call
        < restore_finalize_call
    )
    assert 'rm -f -- "$DATABASE_WRITER_GUARD_FILE"' not in recovery

    for unit in (
        "probiga disabled",
        "probiga-scheduler disabled",
        "probiga-ai-recommendation-worker.service disabled:static",
        "probiga-ai-recommendation-worker.timer disabled",
    ):
        assert unit in writer_fence
    for activation_unit in (
        "probiga-scheduler.timer",
        "probiga-scheduler.path",
        "probiga-scheduler.socket",
    ):
        assert activation_unit in writer_fence
    assert "loaded)" in inventory_fence
    assert "not-found)" in inventory_fence
    assert 'LoadState --value "$unit")" = not-found' in inventory_fence
    assert "failed)" in normalize_fence
    assert 'test "${main_pid:-0}" = 0' in normalize_fence
    assert 'test "${exec_main_pid:-0}" = 0' in normalize_fence
    assert 'systemctl stop "$unit"' in normalize_fence
    assert 'systemctl reset-failed "$unit"' in normalize_fence
    assert 'test "$active_state" = inactive' in strict_fence
    assert 'test "${main_pid:-0}" = 0' in strict_fence
    assert 'test "${exec_main_pid:-0}" = 0' in strict_fence
    assert "scheduler_unit=*" in recovery
    assert "ai_service_unit=*" in recovery
    assert "ai_timer_unit=*" in recovery
    for guard_dropin in (
        "MAIN_DATABASE_WRITER_GUARD_DROPIN",
        "SCHEDULER_DATABASE_WRITER_GUARD_DROPIN",
        "AI_SERVICE_DATABASE_WRITER_GUARD_DROPIN",
        "AI_TIMER_DATABASE_WRITER_GUARD_DROPIN",
    ):
        assert guard_dropin in dropin_contract
    assert "controlled_guard_assert_marker" in boundary
    assert "controlled_guard_assert_storage" in marker_assertion
    assert "probiga.database-writer-guard.v2" in marker_assertion
    for inventory_line in (
        'main_unit=$main_record',
        'scheduler_unit=$scheduler_record',
        'ai_service_unit=$ai_service_record',
        'ai_timer_unit=$ai_timer_record',
    ):
        assert inventory_line in marker_assertion
    for record_kind in ("main", "scheduler", "ai-service", "ai-timer"):
        assert f"controlled_guard_assert_state_record {record_kind}" in marker_assertion
    for exact_inventory in (
        'loaded,$PREVIOUS_MAIN_ACTIVE_STATE,$PREVIOUS_MAIN_UNIT_FILE_STATE',
        'loaded,$PREVIOUS_SCHEDULER_ACTIVE_STATE,$PREVIOUS_SCHEDULER_UNIT_FILE_STATE',
        'loaded,$PREVIOUS_AI_WORKER_SERVICE_ACTIVE_STATE,$PREVIOUS_AI_WORKER_SERVICE_UNIT_FILE_STATE',
        'loaded,$PREVIOUS_AI_WORKER_TIMER_ACTIVE_STATE,$PREVIOUS_AI_WORKER_TIMER_UNIT_FILE_STATE',
    ):
        assert exact_inventory in guard_inventory
    assert "not-found,not-found,not-found" in guard_inventory
    assert "main:loaded:active:enabled" in state_record_assertion
    assert "scheduler:not-found:not-found:not-found" in state_record_assertion
    assert "ai-service:loaded:active:static" in state_record_assertion
    assert "ai-timer:not-found:not-found:not-found" in state_record_assertion
    assert "controlled_guard_assert_dropin_boundary" in boundary
    assert "controlled_guard_assert_all_writers_fenced" in dropin_boundary

    for required_recovery_evidence in (
        'p.get("trust_restoration_verified") is True',
        'p.get("restore_primary_verified") is True',
        'p.get("restore_secondary_verified") is True',
        'p.get("runtime_trust_off_verified") is True',
    ):
        assert required_recovery_evidence in recover_validator
    for required_resume_evidence in (
        'p.get("phase") == "resume"',
        'p.get("runtime_privilege_boundary_verified") is True',
        'p.get("runtime_least_privilege_verified")',
        'p.get("runtime_legacy_ddl_compatibility")',
        'security.get("observed_contract") == observed_contract',
        'security.get("persistent_ddl_privileges") == expected_persistent_ddl',
        'security.get("global_privileges") == ["USAGE"]',
        'security.get("schema_privileges") == observed_schema',
        'security.get("funding_append_only_tables") == expected_funding_tables',
        'security.get("funding_append_only_verified") is True',
        'security.get("funding_structural_bypass_privileges")',
        '== expected_persistent_ddl',
        'security.get("truncate_denied_by_absent_drop_privilege")',
        'is (observed_contract == target_contract)',
        'security.get("trigger_drop_denied_by_absent_trigger_privilege") is True',
        'security.get("require_ssl") is True',
        'security.get("roles") == []',
        'security.get("grant_option") is False',
        'p.get("runtime_definer_routine_count") == 0',
        'p.get("runtime_definer_routine_inventory_verified") is True',
        'binding.get("legacy_binding_pending") is False',
        'binding.get("legacy_binding_marker_present")',
        'is bool(binding["legacy_run_count"])',
        'p.get("trust_restoration_verified") is True',
        'p.get("restore_primary_verified") is True',
        'p.get("restore_secondary_verified") is True',
        'p.get("runtime_trust_off_verified") is True',
        'x.get("status") in {"applied", "exists"}',
        'repair.get("post_validation_verified") is True',
        'candidates == sorted(set(candidates))',
        'repaired == candidates',
        'set(candidates) <= allowed',
        'p.get("trigger_trust_window_count") == len(windows)',
        'p["seeded_strategy_count"] > 0',
        'p.get("funding_checkpoint_table_count") == 2',
        'p.get("funding_checkpoint_trigger_count") == 4',
        'p.get("governance_append_only_trigger_count") == 38',
        'p.get("governance_metric_review_trigger_count") == 2',
        'p["governance_trigger_count"] == 40',
        'p.get("governance_trigger_source_contract_hash")',
        'p.get("governance_append_only_physical_contract_hash")',
        'p.get("governance_metric_review_physical_contract_hash")',
        'p.get("governance_append_only_core_contract_hash")',
        'p.get("governance_metric_review_core_contract_hash")',
        'governance_names_hash == expected_trigger_names_hash',
        'funding.get("batch_max_rows") == 100',
        'funding.get("batch_max_bytes") == 4194304',
        'funding.get("manifest_max_bytes") == 1048576',
        'funding.get("audit_max_bytes") == 131072',
        'runtime_bundle.get("contract_hash") == expected_runtime_bundle_hash',
        'runtime_bundle.get("recovery_planner_count") == 6',
        'runtime_bundle.get("recovery_planner_names")',
        'runtime_bundle.get("recovery_plan_count") == 6',
        'set(runtime_bundle["recovery_plans"])',
        'get("status") == "PLANNED"',
        'get("read_only") is True',
        '"ready_for_privileged_apply"',
        'r"[0-9a-f]{64}"',
        'runtime_bundle.get("recovery_ready_for_privileged_apply") is True',
        'runtime_bundle_runtime.get("contract_hash")',
        'expected_full_count = 174 if full_optional_v4_count == 32 else 142',
        'type(full_optional_v4_count) is int',
        'full_optional_v4_count in {0, 32}',
        'full_trigger_inventory.get("expected_count") == expected_full_count',
        'full_trigger_inventory.get("observed_count") == expected_full_count',
        'full_trigger_inventory.get("managed_count") == 101',
        'full_trigger_inventory.get("v2_count") == 41',
        'full_trigger_inventory_exact',
    ):
        assert required_resume_evidence in resume_validator
    for required_writer_fence_evidence in (
        'p.get("mode")=="writer-fence"',
        'p.get("writer_fence_active") is True',
        'p.get("layer4_writers_enabled") is False',
        'q.get("checked") is True',
        'q.get("ready") is True',
        'q.get("live_writers")==[]',
    ):
        assert required_writer_fence_evidence in writer_fence_validator
    for required_preflight_evidence in (
        'p.get("runtime_privilege_boundary_verified") is True',
        'p.get("runtime_least_privilege_verified")',
        'p.get("runtime_legacy_ddl_compatibility")',
        'security.get("observed_contract") == observed_contract',
        'security.get("persistent_ddl_privileges") == expected_persistent_ddl',
        'security.get("global_privileges") == ["USAGE"]',
        'security.get("schema_privileges") == observed_schema',
        'security.get("funding_append_only_tables") == expected_funding_tables',
        'security.get("funding_append_only_verified") is True',
        'security.get("funding_structural_bypass_privileges")',
        '== expected_persistent_ddl',
        'security.get("truncate_denied_by_absent_drop_privilege")',
        'is (observed_contract == target_contract)',
        'security.get("trigger_drop_denied_by_absent_trigger_privilege") is True',
        'security.get("require_ssl") is True',
        'security.get("roles") == []',
        'security.get("grant_option") is False',
        'p.get("runtime_definer_routine_count") == 0',
        'p.get("runtime_definer_routine_inventory_verified") is True',
        'binding.get("legacy_binding_pending") is False',
        'binding.get("legacy_binding_marker_present")',
        'is bool(binding["legacy_run_count"])',
        'p.get("pending_v3_versions") == []',
        'x.get("status") == "exists"',
        'trigger.get("metadata_frozen") is True',
        'trigger.get("legacy_rehome_names") == []',
            'p.get("qmt_table_count") == 4',
            'p.get("governance_table_count") == 15',
        'governance_source.get("source_contract_hash")',
        'governance_source.get("core_append_only_contract_hash")',
        'governance_source.get("core_metric_review_contract_hash")',
        'governance_names_hash == expected_trigger_names_hash',
        'runtime_bundle.get("contract_hash") == expected_runtime_bundle_hash',
        'runtime_bundle.get("recovery_planner_count") == 6',
        'runtime_bundle.get("recovery_planner_names")',
        'runtime_bundle.get("recovery_plan_count") == 6',
        'set(runtime_bundle["recovery_plans"])',
        'get("status") == "PLANNED"',
        'get("read_only") is True',
        '"ready_for_privileged_apply"',
        'r"[0-9a-f]{64}"',
        'runtime_bundle.get("recovery_ready_for_privileged_apply") is True',
        'or not runtime_bundle.get("recovery_ready_for_privileged_apply")',
        'runtime_bundle.get("migration_required")',
    ):
        assert required_preflight_evidence in preflight_validator
    for exact_runtime_grant in (
        '"BIGA.*": ["SELECT"]',
        '"PROBIGA.*": [',
        '"CREATE TEMPORARY TABLES", "DELETE"',
        '"INSERT", "SELECT", "UPDATE"',
        '"ALTER", "CREATE", "CREATE TEMPORARY TABLES", "DELETE", "DROP"',
        '"INDEX", "INSERT", "REFERENCES", "SELECT", "UPDATE"',
        '"PROBIGA_QMT_HISTORY.*": ["SELECT"]',
        'target_contract = "TARGET_LEAST_PRIVILEGE"',
        'legacy_contract = "LEGACY_DDL_COMPATIBILITY"',
    ):
        assert exact_runtime_grant in resume_validator
        assert exact_runtime_grant in preflight_validator
    for frozen_contract_literal in (
        "47b44f4c1e5201b4ea7cd51f61073fdb4229c245214685c338e24809435a7bde",
        "5a1a19e0664c715ae0cac7cfa8dd87c47da1b63b1d2df869561cecf3c995f01f",
        "a2f74c8b1d4fa984e2d6aadb6169e13e8d041a1f414f2523aeb5835dc4376e13",
        "bf537f9ed5fb1d31195092ae6a24262511de6f45bf9addacefebc88e25b6b9d8",
        "c217a42eb6c2a5f7bed592bb7c7e724499546f997061c4daad1db957317bdf28",
        "1fcde61ce5a5ea0cc16f1910d94da431d044c667383fafd2224217709f555943",
        "0dbaa644427139c472bab0c3f719d78bd292bb6a7726a0f0ef195adc2e37fa84",
        "61f9ddfb3179f30c9976a090fce00adb8613d4e38d698c6cfc954f957084845f",
    ):
        assert frozen_contract_literal in resume_validator
        assert frozen_contract_literal in preflight_validator
    for resume_only_trigger_literal in (
        "076a2b84c15b9dbb54901c63f980c2f85ab17f7652d9334ab661d89ad990d0bc",
        "a1c6aa0e9f241a419bbb87c101fbac7d8dd1404aa9f95493afbd604370644a87",
        "5167f36ee731c2544be73590e4e00716f334c58b5746f776e610254904cf8883",
        "7e42c91e534dd3d61d212f0c16fa7297c29b8f4756812de2e072874179537423",
    ):
        assert resume_only_trigger_literal in resume_validator

    assert cleanup.count("controlled_guard_restore_after_cleanup_failure") >= 2
    assert 'rm -f -- "$DATABASE_WRITER_GUARD_FILE"' in cleanup
    assert 'rm -f -- "$dropin"' not in cleanup
    assert "controlled_guard_assert_dropin_boundary" in cleanup
    assert "systemctl daemon-reload" not in cleanup
    assert "controlled_guard_recreate_file" in cleanup_restore
    assert "controlled_guard_install_dropins" in cleanup_restore
    assert "systemctl daemon-reload" in cleanup_restore
    assert "controlled_guard_force_all_writers_fenced" in cleanup_restore
    assert "controlled_guard_assert_boundary" in cleanup_restore
    assert "probiga.database-writer-restore.v1" in restore_file_writer
    assert 'chown root:root "$restore_tmp"' in restore_file_writer
    assert 'chmod 0600 "$restore_tmp"' in restore_file_writer
    assert 'mv -fT "$restore_tmp" "$DATABASE_WRITER_RESTORE_FILE"' in restore_file_writer
    assert 'sync -f "$DATABASE_WRITER_RESTORE_FILE"' in restore_file_writer
    assert "controlled_guard_assert_restore_file" in restore_finalize
    assert "controlled_guard_restore_previous_writer_states" in restore_finalize
    assert "old-runtime-missing-safe-fence" in restore_finalize
    assert "safe_main_record=loaded,inactive,disabled" in restore_finalize
    assert "safe_scheduler_record=loaded,inactive,disabled" in restore_finalize
    assert "safe_ai_service_record=loaded,inactive,static" in restore_finalize
    assert "safe_ai_timer_record=loaded,inactive,disabled" in restore_finalize
    assert 'restore_verification_mode=rollback-only' in restore_finalize
    assert '"$safe_ai_service_record"' in restore_finalize
    assert '"$safe_ai_timer_record" "$restore_verification_mode"' in restore_finalize
    assert restore_finalize.count("controlled_guard_refence_after_restore_failure") >= 1
    old_runtime_commit = restore_finalize.index(
        'activation_snapshot_set_phase "$guarded_sha" old-runtime-verified'
    )
    restore_finalize_after_commit = restore_finalize[old_runtime_commit:]
    assert (
        "controlled_guard_refence_after_restore_failure"
        not in restore_finalize_after_commit
    )
    assert "controlled_guard_write_restore_file" not in restore_finalize_after_commit
    assert 'rm -f -- "$DATABASE_WRITER_RESTORE_FILE"' in restore_finalize
    for unit in (
        "probiga",
        "probiga-scheduler",
        "probiga-ai-recommendation-worker.service",
        "probiga-ai-recommendation-worker.timer",
    ):
        assert f"controlled_guard_apply_unit_state {unit}" in restore_previous_states
    assert "http://127.0.0.1/api/health" in restore_previous_states
    for exact_action in (
        'systemctl enable "$unit"',
        'systemctl disable "$unit"',
        'systemctl start "$unit"',
        'systemctl stop "$unit"',
        'test "$active_state" = "$expected_active"',
        '"$expected_unit_file"',
    ):
        assert exact_action in apply_unit_state
    refence_recreate = refence_after_restore_failure.index(
        "controlled_guard_recreate_file"
    )
    refence_install = refence_after_restore_failure.index(
        "controlled_guard_install_dropins", refence_recreate
    )
    refence_reload = refence_after_restore_failure.index(
        "systemctl daemon-reload", refence_install
    )
    refence_force = refence_after_restore_failure.index(
        "controlled_guard_force_all_writers_fenced", refence_reload
    )
    refence_boundary = refence_after_restore_failure.index(
        "controlled_guard_assert_boundary", refence_force
    )
    assert refence_recreate < refence_install < refence_reload < refence_force
    assert refence_force < refence_boundary
    for unit in (
        "probiga loaded disabled",
        "probiga-scheduler",
        "probiga-ai-recommendation-worker.service",
        "probiga-ai-recommendation-worker.timer",
        "probiga-scheduler.timer",
        "probiga-scheduler.path",
        "probiga-scheduler.socket",
    ):
        assert unit in force_all_writers_fenced
    assert "probiga.database-writer-restore.v1" in restore_only_recovery
    assert 'test ! -e "$DATABASE_WRITER_GUARD_FILE"' in restore_only_recovery
    restore_marker = restore_only_recovery.index(
        "controlled_guard_recreate_file"
    )
    restore_dropins = restore_only_recovery.index(
        "controlled_guard_install_dropins", restore_marker
    )
    restore_reload = restore_only_recovery.index(
        "systemctl daemon-reload", restore_dropins
    )
    restore_fence = restore_only_recovery.index(
        "controlled_guard_force_all_writers_fenced", restore_reload
    )
    restore_boundary = restore_only_recovery.index(
        "controlled_guard_assert_boundary", restore_fence
    )
    restore_cleanup = restore_only_recovery.index(
        "controlled_guard_cleanup", restore_boundary
    )
    restore_exact = restore_only_recovery.index(
        "controlled_guard_restore_and_finalize", restore_cleanup
    )
    assert (
        restore_marker
        < restore_dropins
        < restore_reload
        < restore_fence
        < restore_boundary
        < restore_cleanup
        < restore_exact
    )
    assert "controlled_guard_refence_after_restore_failure" in restore_only_recovery
    dispatch = _normalized_shell(
        deploy_script[recovery_dispatch:normal_release_inputs]
    )
    assert 'if [ -e "$DATABASE_WRITER_GUARD_FILE" ]' in dispatch
    assert 'elif [ -e "$DATABASE_WRITER_RESTORE_FILE" ]' in dispatch
    assert "controlled_database_guard_recovery" in dispatch
    assert "controlled_database_writer_restore_recovery" in dispatch
    assert 'phase_args+=(--writers-fenced)' in guarded_runner
    assert 'preflight|recover)' in guarded_runner
    assert 'resume)' in guarded_runner
    assert (
        'adata_source="$ADATA_RUNTIME_ROOT/$adata_sha-$adata_tree_sha"'
        in guarded_runner
    )
    adata_marker = guarded_runner.index(
        'adata_tree_sha="$(cat "$release_venv/.adata.tree.sha256")"'
    )
    adata_source = guarded_runner.index(
        'adata_source="$ADATA_RUNTIME_ROOT/$adata_sha-$adata_tree_sha"'
    )
    adata_source_seal = guarded_runner.index(
        '"$adata_source/.probiga-adata.tree.sha256"', adata_source
    )
    adata_source_immutable = guarded_runner.index(
        'find -P "$adata_source" -xdev', adata_source_seal
    )
    guarded_clean_env = guarded_runner.index("/usr/bin/env -i")
    assert (
        adata_marker
        < adata_source
        < adata_source_seal
        < adata_source_immutable
        < guarded_clean_env
    )
    assert 'PROBIGA_ADATA_SOURCE_DIR="$adata_source"' in guarded_runner
    assert '"PYTHONPATH=$code_root"' in guarded_runner
    assert '"PYTHONPATH=$adata_source:$code_root"' not in guarded_runner
    assert "--writer-fence" in guarded_writer_fence_runner
    assert "--require-no-live-scheduler-writers" in guarded_writer_fence_runner
    assert 'sudo -u "$service_user"' in guarded_writer_fence_runner
    assert '"PYTHONPATH=$adata_source:$code_root"' in guarded_writer_fence_runner
    clean_env = guarded_writer_fence_runner.index("/usr/bin/env -i")
    unset_database_url = guarded_writer_fence_runner.index(
        "-u MYSQL_URL -u DATABASE_URL -u MYSQL_PWD", clean_env
    )
    clean_path = guarded_writer_fence_runner.index(
        "PATH=/usr/sbin:/usr/bin:/sbin:/bin", clean_env
    )
    assert clean_env < unset_database_url < clean_path


def test_v2_normal_deploy_has_narrow_prepared_rollback_only_recovery() -> None:
    deploy = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy)
    bodies = {
        name: _normalized_shell(body)
        for name, body in _shell_function_bodies(deploy).items()
    }
    recovery = bodies["controlled_v2_rollback_only_recovery"]
    fenced_old_set_retire = bodies[
        "controlled_v2_retire_fenced_old_set_for_newer_deploy"
    ]
    capture = bodies["controlled_guard_capture_current_governance_snapshot"]
    restore_runtime = bodies[
        "controlled_guard_assert_governance_restore_runtime"
    ]
    recovery_code_tree = bodies[
        "controlled_guard_assert_recovery_code_tree_clean"
    ]
    rollback_receipt = bodies[
        "activation_snapshot_validate_rollback_receipt_state"
    ]
    missing_guard = bodies[
        "activation_snapshot_allows_missing_guard_for_recovery"
    ]
    venv_seal = bodies["controlled_guard_assert_immutable_venv_tree"]
    verifier = _normalized_shell(deploy[
        deploy.index("controlled_guard_verify_restored_runtime() {"):
        deploy.index("controlled_guard_force_unit_fenced() {")
    ])
    gate_deadline = bodies[
        "controlled_guard_run_service_gate_with_deadline"
    ]
    capture_deadline = bodies[
        "controlled_guard_capture_service_gate_with_deadline"
    ]
    health_result_parser = _normalized_shell(deploy[
        deploy.index("controlled_guard_parse_governance_health_result() {"):
        deploy.index("controlled_guard_parse_governance_cutover_result() {")
    ])
    cutover_result_parser = _normalized_shell(deploy[
        deploy.index("controlled_guard_parse_governance_cutover_result() {"):
        deploy.index("controlled_guard_parse_governance_runner_result() {")
    ])
    runner_result_parser = _normalized_shell(deploy[
        deploy.index("controlled_guard_parse_governance_runner_result() {"):
        deploy.index("controlled_guard_verify_restored_runtime() {")
    ])
    governance_snapshot = bodies["controlled_guard_governance_snapshot"]

    assert 'test "$DEPLOY_OPERATION" = deploy' in recovery
    assert 'test "$DEPLOY_OPERATION" = deploy' in fenced_old_set_retire
    assert 'test "$phase" = old-set-restored' in fenced_old_set_retire
    assert 'test "$EXPECTED_SHA" != "$guarded_sha"' in fenced_old_set_retire
    assert "controlled_guard_governance_contract_snapshot verify" in (
        fenced_old_set_retire
    )
    assert "rollback-governance" in fenced_old_set_retire
    assert "rollback-qmt" in fenced_old_set_retire
    assert "controlled_guard_force_all_writers_fenced" in fenced_old_set_retire
    assert "controlled_guard_cleanup" in fenced_old_set_retire
    assert "activation_snapshot_remove_old_runtime_verified" in (
        fenced_old_set_retire
    )
    fast_retire_call = recovery.index(
        "controlled_v2_retire_fenced_old_set_for_newer_deploy"
    )
    normal_old_restore = recovery.index("activation_snapshot_restore_old_set")
    assert fast_retire_call < normal_old_restore
    assert (
        "ci-resolved-freeze-v1|static-wheel-lock-v2" in recovery
    )
    assert "PROBIGA_DEFERRED_SCHEDULER_EXPECTED_GIT_SHA" in verifier
    assert "PROBIGA_DEFERRED_SCHEDULER_CODE_ROOT" in verifier
    assert 'test -n "$deferred_expected_sha"' in verifier
    assert 'scheduler_expected_sha="$deferred_expected_sha"' in verifier
    assert 'scheduler_build_sha="$deferred_expected_sha"' in verifier
    assert '"$scheduler_code_root/tools/run_scheduler_daemon.py"' in verifier
    assert 'test "$guarded_sha" != "$EXPECTED_SHA"' not in recovery
    assert (
        "prepared|runtime-units-installing|runtime-units-installed|"
        " restoring-old|old-set-restored|old-runtime-verified"
        in recovery
    )
    phase_gate = recovery[
        recovery.index('phase="$(activation_snapshot_phase)"'):
        recovery.index("esac", recovery.index('phase="$(activation_snapshot_phase)"'))
    ]
    for disallowed_phase in (
        "new-runtime-verified",
        "finalized",
    ):
        assert disallowed_phase not in phase_gate
    assert (
        "runtime-units-installed|restoring-old|old-set-restored|"
        "old-runtime-verified"
        in recovery
    )
    assert "activation_snapshot_validate_governance_new" in recovery
    receipt_validation = recovery.index(
        "activation_snapshot_validate_rollback_receipt_state"
    )
    writer_state = recovery.index('test "${#state_lines[@]}" -eq 6')
    assert receipt_validation < writer_state
    assert (
        "runtime-units-installed|restoring-old|old-set-restored|"
        " old-runtime-verified"
        in rollback_receipt
    )
    assert "activation_snapshot_validate_receipt_pending" in rollback_receipt
    for receipt_path in (
        "$ACTIVATION_RECEIPT_PENDING",
        "$ACTIVATION_RECEIPT_PENDING_SHA",
    ):
        assert f'[ -e "{receipt_path}" ]' in rollback_receipt
        assert f'[ -L "{receipt_path}" ]' in rollback_receipt
    for state_path in (
        "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT",
        "$ACTIVATION_GOVERNANCE_NEW_SHA",
    ):
        assert f'[ -e "{state_path}" ]' in recovery
        assert f'[ -L "{state_path}" ]' in recovery
    assert 'test "${#state_lines[@]}" -eq 6' in recovery
    assert "controlled_guard_recreate_file" in recovery
    assert "controlled_guard_force_all_writers_fenced" in recovery
    assert 'if [ "$phase" = old-runtime-verified ]' in recovery
    assert "activation_snapshot_allows_missing_guard_for_recovery" in recovery
    assert recovery.count(
        "activation_snapshot_allows_missing_guard_for_recovery"
    ) >= 2
    assert (
        "prepared|runtime-units-installed|restoring-old|old-set-restored|"
        " old-runtime-verified"
        in missing_guard
    )
    guard_branch_end = recovery.index("fi", recovery.index(
        "controlled_guard_recreate_file"
    ))
    install_fence = recovery.index("controlled_guard_install_dropins", guard_branch_end)
    reload_fence = recovery.index("systemctl daemon-reload", install_fence)
    force_fence = recovery.index(
        "controlled_guard_force_all_writers_fenced", reload_fence
    )

    marker = recovery.index("controlled_guard_assert_marker")
    restore_file = recovery.index("controlled_guard_assert_restore_file")
    boundary = recovery.index("controlled_guard_assert_boundary", marker)
    fence_failure = recovery.index(
        'if [ "$fence_status" -ne 0 ]', boundary
    )
    refence = recovery.index(
        "controlled_guard_refence_after_restore_failure", fence_failure
    )
    assert (
        guard_branch_end
        < install_fence
        < reload_fence
        < force_fence
        < boundary
        < fence_failure
        < refence
    )
    restore_old_set = recovery.index(
        "activation_snapshot_restore_old_set", boundary
    )
    restore_governance = recovery.index(
        "controlled_guard_restore_and_verify_governance_snapshot", boundary
    )
    old_set = recovery.index("activation_snapshot_assert_old_set", restore_old_set)
    governance = recovery.index(
        "controlled_guard_capture_current_governance_snapshot", old_set
    )
    cleanup = recovery.index("controlled_guard_cleanup", governance)
    restore_states = recovery.index(
        "controlled_guard_restore_previous_writer_states", cleanup
    )
    verify_runtime = recovery.index(
        "controlled_guard_verify_restored_runtime", restore_states
    )
    verified_phase = recovery.index(
        "old-runtime-verified", verify_runtime
    )
    restore_journal_remove = recovery.index(
        'rm -f -- "$DATABASE_WRITER_RESTORE_FILE"', verified_phase
    )
    activation_journal_remove = recovery.index(
        "activation_snapshot_remove_old_runtime_verified",
        restore_journal_remove,
    )
    assert (
        restore_file
        < marker
        < boundary
        < restore_governance
        < restore_old_set
        < old_set
        < governance
        < cleanup
        < restore_states
        < verify_runtime
        < verified_phase
        < restore_journal_remove
        < activation_journal_remove
    )
    assert "rollback-only" in recovery
    assert recovery.count("controlled_guard_refence_after_restore_failure") >= 5
    assert "controlled_guard_write_restore_file" in recovery
    assert "prepare_strategy_governance_qmt_history.py" not in recovery
    assert "prepare_strategy_governance_schema.py" not in recovery

    ready_check = recovery.index(
        "controlled_guard_assert_governance_restore_runtime"
    )
    venv_present = recovery.index(
        '[ -e "$RELEASE_VENV_ROOT/$guarded_sha" ]'
    )
    fallback_capture = recovery.index(
        "controlled_guard_capture_current_governance_snapshot", ready_check
    )
    state_validation = recovery.index('test "${#state_lines[@]}" -eq 6')
    assert venv_present < ready_check < fallback_capture < state_validation
    assert "controlled_guard_assert_governance_restore_runtime \"$guarded_sha\" || return 1" in recovery[
        venv_present:fallback_capture
    ]
    assert 'activation_snapshot_validate "$guarded_sha"' in restore_runtime
    assert 'git -C "$code_root" rev-parse HEAD' in restore_runtime
    assert "controlled_guard_assert_recovery_code_tree_clean" in restore_runtime
    assert "ls-files --others --exclude-standard" in recovery_code_tree
    assert "ls-files --others --exclude-standard -z" in recovery_code_tree
    assert 'test "${#untracked_paths[@]}" -eq 1' in recovery_code_tree
    assert 'test "${untracked_paths[0]}" = probiga.release.json' in (
        recovery_code_tree
    )
    assert 'local expected_release="$2"' in recovery_code_tree
    assert 'rev-parse "${expected_release}^{tree}"' in recovery_code_tree
    assert "probiga.release-manifest.v1" in recovery_code_tree
    assert 'payload["release_id"] != expected_release' in recovery_code_tree
    assert 'payload["source_tree_hash"] != expected_tree' in recovery_code_tree
    assert 'payload["manifest_sha256"] != seal' in recovery_code_tree
    assert 'controlled_guard_assert_file "$manifest_path" 444' in (
        recovery_code_tree
    )
    assert "diff --no-ext-diff --cached --quiet" in recovery_code_tree
    assert "diff --no-ext-diff --ignore-cr-at-eol --quiet" in recovery_code_tree
    assert (
        'controlled_guard_assert_recovery_code_tree_clean "$code_root" '
        '"$guarded_sha"'
    ) in restore_runtime
    assert "controlled_guard_assert_immutable_venv_tree" in restore_runtime
    assert 'test "$service_user" != root' in restore_runtime

    assert "activation_snapshot_validate" in capture
    assert 'local old_runtime_sha="$2"' in capture
    assert 'local release_venv="$RELEASE_VENV_ROOT/$old_runtime_sha"' in capture
    assert '$RELEASE_VENV_ROOT/$guarded_sha' not in capture
    assert "controlled_guard_assert_immutable_venv_tree" in capture
    assert "local expected_owner=root" in venv_seal
    assert "local expected_owner_group=root:root" in venv_seal
    assert '! -user "$expected_owner"' in venv_seal
    assert '! -type l -perm /022' in venv_seal
    assert "-type l" in venv_seal
    assert "shift" not in venv_seal
    assert 'readlink -- "$link_path"' in venv_seal
    assert "/usr/bin/realpath -ms" in venv_seal
    assert 'readlink -f -- "$link_path"' in venv_seal
    assert '"$VENV_TREE_ROOT"|"$VENV_TREE_ROOT"/*' in venv_seal
    assert 'local bootstrap_entry=/usr/bin/python3.14' in venv_seal
    assert 'readlink -f -- "$bootstrap_entry"' in venv_seal
    assert '"$VENV_BOOTSTRAP_ENTRY"' in venv_seal
    assert '"$VENV_TRUSTED_BOOTSTRAP_PYTHON"' in venv_seal
    assert (
        'test "$(activation_snapshot_old_release "$guarded_sha")" = '
        '"$old_runtime_sha"'
    ) in capture
    assert (
        'test "$(<"$release_venv/.probiga.gitsha")" = "$old_runtime_sha"'
        in capture
    )
    assert "release_identity_lines[3]#release_tree_sha256=" in capture
    assert (
        "release_identity_lines[4]#adapter_registry_seal_sha256=" in capture
    )
    legacy_runtime_identity = capture.index(
        'if [ -e "$release_venv/.release-tree.sha256" ]'
    )
    legacy_runtime_identity_end = capture.index("fi", legacy_runtime_identity)
    legacy_runtime_identity_block = capture[
        legacy_runtime_identity:legacy_runtime_identity_end
    ]
    for marker in (
        ".release-tree.sha256",
        ".adapter-registry-seal.sha256",
    ):
        assert f'[ -e "$release_venv/{marker}" ]' in legacy_runtime_identity_block
        assert f'[ -L "$release_venv/{marker}" ]' in legacy_runtime_identity_block
        assert f'test -f "$release_venv/{marker}"' in legacy_runtime_identity_block
        assert f'test ! -L "$release_venv/{marker}"' in legacy_runtime_identity_block
    assert (
        'controlled_guard_capture_current_governance_snapshot "$guarded_sha" '
        '"$old_runtime_sha"'
    ) in recovery
    assert 'git -C "$code_root" rev-parse HEAD' in capture
    assert "controlled_guard_assert_recovery_code_tree_clean" in capture
    assert 'local rollback_verification_action="${3:-verify}"' in capture
    assert (
        '"$rollback_verification_action" rollback-governance' in capture
    )
    assert '"$rollback_verification_action" rollback-qmt' in capture
    assert '< "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT"' in capture
    assert '< "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT"' in capture
    assert "--capture-snapshot" not in capture
    assert "cmp --silent" not in capture
    assert "tools/add_strategy_governance_task.py" not in capture
    assert "tools/add_qmt_announcement_task.py" not in capture
    assert "--restore-snapshot" not in capture

    assert 'local verification_mode="${6:-full}"' in verifier
    assert 'local input_readiness_mode="${7:-strict}"' in verifier
    assert 'local activation_deadline_epoch="${8:-}"' in verifier
    assert "strict|recover-input-readiness)" in verifier
    runtime_health = verifier.rindex("http://127.0.0.1/api/health/runtime")
    rollback_gate = verifier.index(
        'if [ "$verification_mode" = rollback-only ]; then'
    )
    recovery_readiness = verifier.index(
        'if [ "$input_readiness_mode" = recover-input-readiness ]; then',
        rollback_gate,
    )
    strict_capture = verifier.index(
        ".governance-health-strict.", recovery_readiness
    )
    strict_parse = verifier.index(
        "controlled_guard_parse_governance_health_result", strict_capture
    )
    probe_capture = verifier.index(
        ".governance-health-probe.", strict_parse
    )
    probe_allow = verifier.index(
        '"${governance_health_args[@]}" --allow-input-not-ready',
        probe_capture,
    )
    probe_parse = verifier.index(
        "controlled_guard_parse_governance_health_result", probe_allow
    )
    runner_capture = verifier.index(".governance-recheck.", probe_parse)
    fixed_date_runner = verifier.index(
        '--trade-date "$governance_trade_date"', runner_capture
    )
    runner_parse = verifier.index(
        "controlled_guard_parse_governance_runner_result", fixed_date_runner
    )
    final_expected_date = verifier.index(
        '--expected-trade-date "$governance_trade_date"', runner_parse
    )
    final_allow = verifier.index(
        "governance_health_args+=(--allow-input-not-ready)",
        final_expected_date,
    )
    final_capture = verifier.index(
        ".governance-health-final.", final_allow
    )
    final_parse = verifier.index(
        "controlled_guard_parse_governance_health_result", final_capture
    )
    normal_strict_health = verifier.index(
        "RESTORED_RUNTIME_FAILURE_CODE=governance-health", final_parse
    )
    quality_gate = verifier.index(
        "ensure_quality_gate.py", normal_strict_health
    )
    final_date_capture = verifier.index(
        ".governance-date-final.", quality_gate
    )
    final_authoritative_date = verifier.index(
        '"$python_path" -P -c', final_date_capture
    )
    final_date_match = verifier.index(
        "controlled_guard_parse_governance_cutover_result",
        final_authoritative_date,
    )
    final_deadline = verifier.index(
        'RESTORED_RUNTIME_GOVERNANCE_CUTOVER_EPOCH="$cutover_deadline_epoch"',
        final_date_match,
    )
    assert (
        runtime_health
        < rollback_gate
        < recovery_readiness
        < strict_capture
        < strict_parse
        < probe_capture
        < probe_allow
        < probe_parse
        < runner_capture
        < fixed_date_runner
        < runner_parse
        < final_expected_date
        < final_allow
        < final_capture
        < final_parse
        < normal_strict_health
        < quality_gate
        < final_date_capture
        < final_authoritative_date
        < final_date_match
        < final_deadline
    )
    assert verifier.count("--allow-input-not-ready") == 2
    assert verifier.count(
        'controlled_guard_capture_service_gate_with_deadline "$service_user"'
    ) == 5
    assert "RESTORED_RUNTIME_FAILURE_CODE=runtime-identity" in verifier
    assert "RESTORED_RUNTIME_FAILURE_CODE=governance-health" in verifier
    for failure_code in (
        "governance-health-strict",
        "governance-health-probe",
        "governance-recheck",
        "governance-health-final",
        "governance-date-final",
    ):
        assert f"RESTORED_RUNTIME_FAILURE_CODE={failure_code}" in verifier
    assert "RESTORED_RUNTIME_FAILURE_CODE=premarket-task-ensure" in verifier
    assert verifier.count(
        'controlled_guard_run_service_gate_with_deadline "$service_user"'
    ) == 2
    assert 'return "$gate_status"' in capture_deadline
    assert ') > "$output_file" || gate_status=$?' in capture_deadline
    assert "completed|input_not_ready)" in health_result_parser
    assert "registry_sealed" in health_result_parser
    assert "registry_integrity_ready" in health_result_parser
    assert "adapter_configured" in health_result_parser
    assert "candidate_execution_ready" in health_result_parser
    assert "funding_pipeline_ready" in health_result_parser
    assert "governance_paper_execution_ready" in health_result_parser
    assert "production_execution_ready" in health_result_parser
    assert "real_order_submission_enabled" in health_result_parser
    assert "automatic_real_order_submission" in health_result_parser
    assert "trade_date_source" in health_result_parser
    assert "safe_before_epoch" in cutover_result_parser
    assert "cutoff - safe_before == reserve" in cutover_result_parser
    assert "sample < safe_before" in cutover_result_parser
    assert "controlled_guard_governance_cutover_probe_code" in normalized
    assert '"$cutover_probe_code"' in verifier
    assert "check_governance_cutover_window.py" not in normalized
    assert "0|2)" in runner_result_parser
    assert "set(payload) == required" in runner_result_parser
    sudo_boundary = gate_deadline.index('/usr/bin/sudo -u "$service_user"')
    timeout_boundary = gate_deadline.index(
        "/usr/bin/timeout --signal=TERM", sudo_boundary
    )
    assert sudo_boundary < timeout_boundary
    assert '"--kill-after=$CONTROLLED_DATABASE_GATE_KILL_AFTER"' in gate_deadline
    assert "--foreground" not in gate_deadline
    assert "CONTROLLED_DATABASE_GATE_TIMEOUT=30m" in normalized
    assert "CONTROLLED_DATABASE_GATE_KILL_AFTER=30s" in normalized
    assert (
        'controlled_guard_run_service_gate_with_deadline "$service_user"'
        in governance_snapshot
    )
    assert (
        'controlled_guard_run_service_gate_with_deadline "$service_user"'
        in capture
    )
    assert 'sudo -u "$service_user" /usr/bin/env -i' not in governance_snapshot
    assert 'sudo -u "$service_user" /usr/bin/env -i' not in capture

    service_user = normalized.index('test "$SERVICE_USER" != root')
    caller = normalized.index(
        "CUTOVER_STEP=v2_rollback_only_recovery", service_user
    )
    previous_state = normalized.index(
        "PREVIOUS_MAIN_ACTIVE_STATE=", caller
    )
    stale_guard_rejection = normalized.index(
        "persistent database writer guard/restore state requires controlled recovery",
        previous_state,
    )
    assert service_user < caller < previous_state < stale_guard_rejection
    caller_block = normalized[service_user:previous_state]
    assert '"$DEPLOY_ARTIFACT_MODE" = ci-resolved-freeze-v1' in caller_block
    assert '"$DEPLOY_ARTIFACT_MODE" = static-wheel-lock-v2' in caller_block
    for state_path in (
        "DATABASE_WRITER_GUARD_FILE",
        "DATABASE_WRITER_RESTORE_FILE",
        "ACTIVATION_UNIT_SNAPSHOT_DIR",
    ):
        assert f'[ -e "${state_path}" ]' in caller_block
        assert f'[ -L "${state_path}" ]' in caller_block
    assert "ACTIVATION_UNIT_SNAPSHOT_PHASE" in caller_block
    for recoverable_phase in (
        "prepared",
        "runtime-units-installing",
        "runtime-units-installed",
        "restoring-old",
        "old-set-restored",
        "old-runtime-verified",
    ):
        assert recoverable_phase in caller_block
    assert "controlled_v2_rollback_only_recovery" in caller_block
    assert "if ! controlled_v2_rollback_only_recovery" not in caller_block
    assert "exec 7>&2" in caller_block
    assert "v2_recovery_failure" in caller_block

    forward_choice = recovery.rindex(
        "controlled_v2_forward_preserve_no_receipt_recovery"
    )
    old_governance_choice = recovery.index(
        'controlled_guard_governance_contract_snapshot verify "$guarded_sha" '
        '"$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" rollback-governance',
        forward_choice,
    )
    assert forward_choice < old_governance_choice
    selector_start = recovery.rfind(
        'if { [ "$phase" = runtime-units-installed ]', 0, forward_choice
    )
    selector = recovery[selector_start:forward_choice]
    assert "activation_snapshot_assert_new_set" not in selector
    assert "activation_snapshot_validate_governance_new" in selector
    assert "rollback-governance" not in selector


def test_forward_no_receipt_recovery_has_distinct_commit_and_retire_contract() -> None:
    deploy = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    bodies = {
        name: _normalized_shell(body)
        for name, body in _shell_function_bodies(deploy).items()
    }
    preserve = bodies["controlled_v2_forward_preserve_no_receipt_recovery"]
    removal = bodies[
        "activation_snapshot_remove_new_runtime_preserved_no_receipt"
    ]
    begin_forward = preserve.index(
        'activation_snapshot_set_phase "$guarded_sha" '
        "restoring-new-no-receipt"
    )
    verify_current_governance = preserve.index(
        "controlled_guard_governance_contract_snapshot verify",
        begin_forward,
    )
    restore_governance = preserve.index(
        "controlled_guard_governance_contract_snapshot restore",
        verify_current_governance,
    )
    verify_restored_governance = preserve.index(
        "controlled_guard_governance_contract_snapshot verify",
        restore_governance,
    )
    governance_boundary = preserve.index(
        "controlled_guard_assert_boundary", verify_restored_governance
    )
    restore_units = preserve.index(
        "activation_snapshot_restore_new_set", governance_boundary
    )
    fenced_verify = preserve.index(
        'controlled_guard_verify_restored_runtime "$fenced_main_record"'
    )
    full_gate = preserve.index('"$fenced_ai_timer_record" full', fenced_verify)
    recovery_readiness = preserve.index("recover-input-readiness", full_gate)
    refence_on_gate_failure = preserve.index(
        "controlled_guard_refence_after_restore_failure", recovery_readiness
    )
    gate_failure_return = preserve.index("return 1", refence_on_gate_failure)
    cutover_result = preserve.index(
        'cutover_deadline_epoch="$RESTORED_RUNTIME_GOVERNANCE_CUTOVER_EPOCH"',
        gate_failure_return,
    )
    install_cutover_gate = preserve.index(
        "controlled_guard_install_recovery_cutover_dropins", cutover_result
    )
    boundary_recheck = preserve.index(
        "controlled_guard_assert_boundary", install_cutover_gate
    )
    refence_on_boundary_failure = preserve.index(
        "controlled_guard_refence_after_restore_failure", boundary_recheck
    )
    boundary_failure_return = preserve.index("return 1", refence_on_boundary_failure)
    precleanup_deadline = preserve.index(
        "controlled_guard_assert_activation_deadline", boundary_failure_return
    )
    cleanup = preserve.index("controlled_guard_cleanup", precleanup_deadline)
    runtime_verify = preserve.index(
        'controlled_guard_verify_restored_runtime "$forward_main_record"',
        cleanup,
    )
    rollback_only = preserve.index(
        '"$ai_timer_record" rollback-only', runtime_verify
    )
    live_governance = preserve.index(
        "controlled_guard_governance_contract_snapshot verify", rollback_only
    )
    remove_cutover_gate = preserve.index(
        "controlled_guard_remove_recovery_cutover_dropins", live_governance
    )
    commit = preserve.index(
        "new-runtime-preserved-no-receipt", remove_cutover_gate
    )
    remove_restore = preserve.index(
        'rm -f -- "$DATABASE_WRITER_RESTORE_FILE"', commit
    )
    retire = preserve.index(
        "activation_snapshot_remove_new_runtime_preserved_no_receipt",
        remove_restore,
    )
    assert (
        begin_forward
        < verify_current_governance
        < restore_governance
        < verify_restored_governance
        < governance_boundary
        < restore_units
        < fenced_verify
        < full_gate
        < recovery_readiness
        < refence_on_gate_failure
        < gate_failure_return
        < cutover_result
        < install_cutover_gate
        < boundary_recheck
        < refence_on_boundary_failure
        < boundary_failure_return
        < precleanup_deadline
        < cleanup
        < runtime_verify
        < rollback_only
        < live_governance
        < remove_cutover_gate
        < commit
        < remove_restore
        < retire
    )
    for failure_code in (
        "runtime-identity",
        "governance-health",
        "governance-health-strict",
        "governance-health-probe",
        "governance-recheck",
        "governance-health-final",
        "governance-date-final",
        "premarket-task-ensure",
    ):
        assert failure_code in preserve
    assert "allow-input-not-ready" not in preserve
    assert "forward-verify-boundary-fenced" in preserve
    assert '"$cutover_deadline_epoch"' in preserve[cleanup:runtime_verify]
    assert (
        'rollback-only strict "$cutover_deadline_epoch"'
        in preserve[runtime_verify:live_governance]
    )
    assert "controlled_guard_restore_and_verify_governance_snapshot" not in preserve
    assert "controlled_guard_governance_snapshot" not in preserve

    atomic_retire = removal.index("activation_snapshot_retire_verified_transaction")
    for path in (
        "$DATABASE_WRITER_GUARD_FILE",
        "$DATABASE_WRITER_RESTORE_FILE",
    ):
        assert f'test ! -e "{path}"' in removal[:atomic_retire]
        assert f'test ! -L "{path}"' in removal[:atomic_retire]
    assert "publish_deployed_receipt_pending" not in removal


def test_transport_and_forward_finalize_boundaries_are_retryable() -> None:
    deploy = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy)
    bodies = {
        name: _normalized_shell(body)
        for name, body in _shell_function_bodies(deploy).items()
    }

    set_options = deploy.index("set -Eeuo pipefail")
    global_pipe = deploy.index("trap '' PIPE", set_options)
    umask = deploy.index("umask 022", global_pipe)
    assert set_options < global_pipe < umask

    detach = bodies["detach_failure_handler_from_transport"]
    ignore_signals = detach.index("trap '' PIPE TERM INT HUP")
    clear_err = detach.index("trap - ERR", ignore_signals)
    disable_errexit = detach.index("set +e", clear_err)
    detach_fds = detach.index("exec >/dev/null 2>&1", disable_errexit)
    assert ignore_signals < clear_err < disable_errexit < detach_fds

    diagnostic_fd = normalized.index("exec 6>&2")
    failure_trap = normalized.index(
        "trap 'precutover_failure \"$?\" \"$LINENO\"' ERR"
    )
    assert diagnostic_fd < failure_trap

    persist_audit = bodies["persist_deploy_failure_audit"]
    assert "probiga.production-deploy-failure-audit.v1" in persist_audit
    assert "preflight|preparation|cutover" in persist_audit
    assert '[[ "$step" =~ ^[a-z0-9][a-z0-9_]*$ ]]' in persist_audit
    assert 'test "$failed_status" -le 255' in persist_audit
    assert 'install -d -o root -g root -m 0700 "$DEPLOY_FAILURE_AUDIT_DIR"' in persist_audit
    assert 'chmod 0444 "$audit_tmp"' in persist_audit
    assert '$RECEIPT_ID-failure-$audit_sha.json' in persist_audit
    assert 'sync -f "$DEPLOY_FAILURE_AUDIT_DIR"' in persist_audit

    emit_audit = bodies["emit_deploy_failure_checkpoint"]
    assert "deploy_failure_checkpoint schema=" in emit_audit
    assert "preflight|preparation|cutover" in emit_audit
    assert '[[ "$step" =~ ^[a-z0-9][a-z0-9_]*$ ]]' in emit_audit
    assert '[[ "$expected_sha" =~ ^[0-9a-f]{40}$ ]]' in emit_audit
    assert '[[ "$previous_sha" =~ ^[0-9a-f]{40}$ ]]' in emit_audit
    for field in (
        "phase=%s",
        "cutover_step=%s",
        "line=%s",
        "status=%s",
        "expected_sha=%s",
        "previous_sha=%s",
        "audit_sha256=%s",
    ):
        assert field in emit_audit
    assert ">&6 || true" in emit_audit

    precutover = bodies["precutover_failure"]
    precutover_detach = precutover.index("detach_failure_handler_from_transport")
    precutover_audit = precutover.index("persist_deploy_failure_audit")
    precutover_checkpoint = precutover.index("emit_deploy_failure_checkpoint")
    precutover_output = precutover.index("deploy_failure phase=preflight")
    precutover_receipt = precutover.index("write_receipt", precutover_output)
    assert (
        precutover_detach
        < precutover_audit
        < precutover_checkpoint
        < precutover_output
        < precutover_receipt
    )
    rollback = bodies["rollback"]
    rollback_detach = rollback.index("detach_failure_handler_from_transport")
    success_gate = rollback.index('if [ "${DEPLOY_SUCCEEDED:-0}" -eq 1 ]')
    rollback_audit = rollback.index("persist_deploy_failure_audit", success_gate)
    rollback_checkpoint = rollback.index(
        "emit_deploy_failure_checkpoint", rollback_audit
    )
    rollback_state = rollback.index('if [ -e "$DATABASE_WRITER_GUARD_FILE" ]')
    rollback_output = rollback.index("deploy_failure phase=", rollback_state)
    assert (
        rollback_detach
        < success_gate
        < rollback_audit
        < rollback_checkpoint
        < rollback_state
        < rollback_output
    )
    assert rollback.index('local failure_step="${CUTOVER_STEP:-unknown}"') < (
        rollback_audit
    )
    assert '"$failure_step" "$failed_line" "$failed_status"' in rollback
    assert "trap 'rollback 143 \"$LINENO\"' TERM" in normalized
    assert "trap 'rollback 130 \"$LINENO\"' INT" in normalized
    assert "trap 'rollback 129 \"$LINENO\"' HUP" in normalized
    rollback_signal_traps = re.findall(
        r"(?m)^[ \t]*trap 'rollback (143|130|129)(?: \"\$LINENO\")?' "
        r"(TERM|INT|HUP)$",
        deploy,
    )
    assert sorted(rollback_signal_traps) == sorted(
        [("143", "TERM"), ("130", "INT"), ("129", "HUP")] * 2
    )
    for signal_status, signal_name in rollback_signal_traps:
        assert (
            f'trap \'rollback {signal_status} "$LINENO"\' {signal_name}'
            in deploy
        )

    forward = bodies["controlled_v2_forward_finalize_recovery"]
    for phase in ("new-runtime-verified", "finalized"):
        assert phase in forward
    new_set = forward.index("activation_snapshot_assert_new_set")
    receipt = forward.index("activation_snapshot_validate_receipt_pending")
    request_identity = forward.index(
        "activation_snapshot_receipt_matches_current_v2_request"
    )
    runtime = forward.index("controlled_guard_verify_restored_runtime")
    governance = forward.index(
        "controlled_guard_governance_contract_snapshot verify"
    )
    restore_remove = forward.index('rm -f -- "$DATABASE_WRITER_RESTORE_FILE"')
    finalized = forward.index("activation_snapshot_set_phase", restore_remove)
    journal_remove = forward.index(
        "activation_snapshot_remove_finalized_before_deploy", finalized
    )
    assert (
        receipt
        < request_identity
        < new_set
        < runtime
        < governance
        < restore_remove
        < finalized
    )
    assert finalized < journal_remove
    assert "rollback-only" in forward

    removal = bodies["activation_snapshot_remove_finalized_before_deploy"]
    publish = removal.index("publish_deployed_receipt_pending")
    atomic_retire = removal.index("activation_snapshot_retire_verified_transaction")
    assert publish < atomic_retire

    retire = bodies["activation_snapshot_retire_verified_transaction"]
    retire_target = retire.index("mktemp -d")
    clear_placeholder = retire.index('rmdir -- "$retired_dir"', retire_target)
    logical_commit = retire.index(
        'mv -T -- "$ACTIVATION_UNIT_SNAPSHOT_DIR" "$retired_dir"',
        clear_placeholder,
    )
    tombstone_cleanup = retire.index('rm -rf -- "$retired_dir"', logical_commit)
    assert retire_target < clear_placeholder < logical_commit < tombstone_cleanup

    service_user = normalized.index('test "$SERVICE_USER" != root')
    forward_call = normalized.index(
        "CUTOVER_STEP=v2_forward_finalize_recovery", service_user
    )
    rollback_call = normalized.index(
        "CUTOVER_STEP=v2_rollback_only_recovery", forward_call
    )
    forward_window = normalized[forward_call:rollback_call]
    assert "trap '' TERM INT HUP" in forward_window
    assert (
        "controlled_v2_forward_finalize_recovery >/dev/null 2>&1"
        in forward_window
    )
    same_sha = forward_window.index(
        '[ "$V2_FORWARD_FINALIZED_SHA" = "$EXPECTED_SHA" ]'
    )
    request_match = forward_window.index(
        '[ "$V2_FORWARD_FINALIZED_REQUEST_MATCH" -eq 1 ]', same_sha
    )
    same_sha_exit = forward_window.index("exit 0", same_sha)
    restore_signal_handlers = forward_window.index(
        "precutover_failure 143", same_sha_exit
    )
    assert same_sha < request_match < same_sha_exit < restore_signal_handlers
    for signal, status in (("TERM", 143), ("INT", 130), ("HUP", 129)):
        assert f"precutover_failure {status}" in forward_window
        assert signal in forward_window
    previous_state = normalized.index("PREVIOUS_MAIN_ACTIVE_STATE=", rollback_call)
    rollback_window = normalized[rollback_call:previous_state]
    assert "trap '' TERM INT HUP" in rollback_window
    assert (
        "controlled_v2_rollback_only_recovery >/dev/null 2>&1"
        in rollback_window
    )

    success_start = normalized.rindex(
        "CUTOVER_STEP=persist_deployed_receipt_pending"
    )
    success = normalized[success_start:]
    success_publish = success.index("publish_deployed_receipt_pending")
    success_flag = success.index("DEPLOY_SUCCEEDED=1", success_publish)
    ignore_success_signals = success.index("trap '' TERM INT HUP", success_flag)
    success_remove = success.index(
        "activation_snapshot_remove_finalized_before_deploy",
        ignore_success_signals,
    )
    clear_handlers = success.index("trap - ERR TERM INT HUP", success_remove)
    assert (
        success_publish
        < success_flag
        < ignore_success_signals
        < success_remove
        < clear_handlers
    )

    finalize = bodies["controlled_guard_finalize_successful_activation"]
    verified_commit = finalize.index(
        "activation_snapshot_set_phase \"$guarded_sha\" new-runtime-verified"
    )
    finalize_after_commit = finalize[verified_commit:]
    assert "controlled_guard_refence_after_restore_failure" not in finalize_after_commit
    assert "controlled_guard_write_restore_file" not in finalize_after_commit

    cleanup = bodies["cleanup_prepare_artifacts"]
    journal_reference = cleanup.index(
        '[ -e "$ACTIVATION_UNIT_SNAPSHOT_DIR" ]'
    )
    runtime_reference = cleanup.index("path_is_runtime_referenced")
    venv_link_remove = cleanup.index(
        'rm -f -- "$RELEASE_VENV_ROOT/$EXPECTED_SHA"'
    )
    venv_build_remove = cleanup.index('rm -rf -- "$EXPECTED_BUILD"')
    assert (
        journal_reference
        < runtime_reference
        < venv_link_remove
        < venv_build_remove
    )


def test_exact_request_rerun_is_a_verified_read_only_noop() -> None:
    deploy = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy)
    body = _normalized_shell(
        _shell_function_bodies(deploy)["prepared_request_is_already_active"]
    )
    runtime_recheck = _normalized_shell(
        _shell_function_bodies(deploy)[
            "assert_prepared_runtime_units_still_current"
        ]
    )
    for final_runtime_identity in (
        'cmp --silent "$MAIN_RELEASE_DROPIN" "$PREPARED_MAIN_DROPIN"',
        'cmp --silent "$SCHEDULER_UNIT" "$PREPARED_SCHEDULER_DROPIN"',
        "NeedDaemonReload",
    ):
        assert final_runtime_identity in runtime_recheck
    for exact_identity in (
        'test "$PREVIOUS_SHA" = "$EXPECTED_SHA"',
        'test "$PREVIOUS_INPUT_LOCK_SHA256" = "$EXPECTED_INPUT_LOCK_SHA256"',
        '"$EXPECTED_RESOLVED_FREEZE_SHA256"',
        'test "$PREVIOUS_ADATA_SHA" = "$EXPECTED_ADATA_SHA"',
        '"$EXPECTED_ADATA_TREE_SHA256"',
        'test "$PREVIOUS_CODE_ROOT" = "$PREPARED_CODE_ROOT"',
        'test "$PREVIOUS_VENV" = "$RELEASE_VENV_ROOT/$EXPECTED_SHA"',
        'cmp --silent "$PREVIOUS_DROPIN" "$PREPARED_MAIN_DROPIN"',
        'cmp --silent "$MAIN_RELEASE_DROPIN" "$PREPARED_MAIN_DROPIN"',
        'cmp --silent "$PREVIOUS_SCHEDULER_DROPIN"',
        'cmp --silent "$SCHEDULER_UNIT" "$PREPARED_SCHEDULER_DROPIN"',
        "assert_database_writer_guard_dropins_loaded",
        "--property=DropInPaths --value",
        "API_EMBEDDED_SCHEDULER_ENABLED=false",
        "PROBIGA_DEPLOYMENT_MODE=production",
        "PYTHONPATH=$ADATA_SOURCE:$PREPARED_CODE_ROOT",
        'PROBIGA_RELEASE_TREE_SHA256=$EXPECTED_RELEASE_TREE_SHA256',
        "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=",
        "http://127.0.0.1/api/health/runtime",
        "assert_nginx_static_matches_checkout",
        "assert_scheduler_triggers_quiescent",
        "finalized_receipt_matches_current_v2_request",
        "tools/check_strategy_governance_health.py",
        '--expected-build-sha "$EXPECTED_SHA"',
        "assert_prepared_runtime_units_still_current",
    ):
        assert exact_identity in body
    for forbidden_mutation in (
        "systemctl start",
        "systemctl stop",
        "systemctl enable",
        "systemctl disable",
        "activation_snapshot_create",
        "persist_database_writer_restore_journal",
    ):
        assert forbidden_mutation not in body

    prepare = normalized.index("CUTOVER_STEP=prepare_release")
    prepare_call = normalized.index("prepare_release", prepare)
    database_preflight = normalized.index(
        "CUTOVER_STEP=initial_database_schema_preflight", prepare_call
    )
    same_sha_gate = normalized.index(
        'if [ "$PREVIOUS_SHA" = "$EXPECTED_SHA" ]; then', database_preflight
    )
    cutover_journal = normalized.index(
        "CUTOVER_STEP=persist_database_writer_restore_journal",
        same_sha_gate,
    )
    noop = normalized[same_sha_gate:cutover_journal]
    verifier = noop.index("if ! prepared_request_is_already_active; then")
    mismatch = noop.index("complete finalized request identity", verifier)
    fail_closed = noop.index("false", mismatch)
    ignore_transport = noop.index("trap '' TERM INT HUP")
    receipt = noop.index('write_receipt DEPLOYED "$EXPECTED_SHA"')
    success = noop.index("DEPLOY_SUCCEEDED=1", receipt)
    clear_handlers = noop.index("trap - ERR TERM INT HUP", success)
    successful_exit = noop.index("exit 0", clear_handlers)
    assert (
        prepare
        < prepare_call
        < database_preflight
        < same_sha_gate
        < cutover_journal
    )
    assert verifier < mismatch < fail_closed < ignore_transport
    assert ignore_transport < receipt < success < clear_handlers < successful_exit
    assert "persist_database_writer_restore_journal" not in noop


def test_activation_snapshot_binds_governance_writer_state_and_receipt() -> None:
    deploy = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    bodies = {
        name: _normalized_shell(body)
        for name, body in _shell_function_bodies(deploy).items()
    }
    create = _normalized_shell(deploy[
        deploy.index("activation_snapshot_create() {"):
        deploy.index("activation_snapshot_assert_old_set() {")
    ])
    assert "governance-task-old.json" in create
    assert "governance-task-old.sha256" in create
    assert "writer-state.sha256" in create
    for field in (
        "new_release=$EXPECTED_SHA",
        "old_release=$PREVIOUS_RELEASE_REVISION",
        "release_tree_sha256=$EXPECTED_RELEASE_TREE_SHA256",
        "adapter_registry_seal_sha256=$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256",
    ):
        assert field in create

    cutover = _normalized_shell(deploy[deploy.index("CUTOVER_STEP=prepare_release"):])
    old_capture = cutover.index("CUTOVER_STEP=capture_strategy_governance_task_before_cutover")
    old_seal = cutover.index(
        'chown root:root "$GOVERNANCE_TASK_OLD_SOURCE"', old_capture
    )
    rollback_preflight = cutover.index(
        "CUTOVER_STEP=preflight_strategy_governance_rollback_channel",
        old_seal,
    )
    rollback_preflight_verify = cutover.index(
        'prepared_governance_snapshot verify "$GOVERNANCE_TASK_OLD_SOURCE"',
        rollback_preflight,
    )
    journal = cutover.index("CUTOVER_STEP=persist_database_writer_restore_journal")
    cutover_started = cutover.index("CUTOVER_STARTED=1", journal)
    writer_fence = cutover.index(
        "CUTOVER_STEP=writer_fence_before_api_stop", cutover_started
    )
    first_writer_stop = cutover.index("CUTOVER_STEP=stop_auxiliary_writers", cutover_started)
    new_enable = cutover.index("CUTOVER_STEP=enable_strategy_governance_task")
    new_capture = cutover.index("CUTOVER_STEP=capture_strategy_governance_task_after_enable")
    new_seal = cutover.index("activation_snapshot_install_governance_new", new_capture)
    new_verify = cutover.index("prepared_governance_snapshot verify", new_seal)
    assert (
        old_capture
        < old_seal
        < rollback_preflight
        < rollback_preflight_verify
        < journal
        < cutover_started
        < writer_fence
        < first_writer_stop
        < new_enable
        < new_capture
        < new_seal
        < new_verify
    )
    assert 'chmod 0600 "$GOVERNANCE_TASK_OLD_SOURCE"' in cutover[
        old_seal:rollback_preflight
    ]
    assert (
        'controlled_guard_assert_file "$GOVERNANCE_TASK_OLD_SOURCE" 600'
        in cutover[old_seal:rollback_preflight]
    )

    old_restore = bodies["controlled_guard_restore_and_finalize"]
    assert old_restore.index(
        "controlled_guard_restore_and_verify_governance_snapshot"
    ) < old_restore.index("controlled_guard_restore_previous_writer_states")
    assert '"$old_runtime_sha"' in old_restore

    snapshot_handoff = bodies["controlled_guard_governance_snapshot"]
    # The activation snapshot remains root:root 0600.  Root opens the sealed
    # file for stdin before sudo changes identity, so the service process can
    # consume the exact bytes without gaining path access or write access.
    assert '"--${action}-snapshot" - < "$snapshot"' in snapshot_handoff
    assert '"--${action}-snapshot" "$snapshot"' not in snapshot_handoff
    deadline_handoff = snapshot_handoff.index(
        'controlled_guard_run_service_gate_with_deadline "$service_user"'
    )
    assert snapshot_handoff.index(
        'controlled_guard_assert_file "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT" 600'
    ) < deadline_handoff
    qmt_snapshot_handoff = bodies[
        "controlled_guard_qmt_announcement_snapshot"
    ]
    assert '"$code_root/tools/add_qmt_announcement_task.py"' in (
        qmt_snapshot_handoff
    )
    assert '"--${action}-snapshot" - < "$snapshot"' in (
        qmt_snapshot_handoff
    )
    assert "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT" in (
        qmt_snapshot_handoff
    )
    assert "$ACTIVATION_QMT_ANNOUNCEMENT_NEW_SNAPSHOT" in (
        qmt_snapshot_handoff
    )

    restore_and_verify = bodies[
        "controlled_guard_restore_and_verify_governance_snapshot"
    ]
    initial_verify = restore_and_verify.index(
        "controlled_guard_governance_contract_snapshot verify"
    )
    assert "rollback-governance" in restore_and_verify[
        initial_verify:initial_verify + 250
    ]
    restore = restore_and_verify.index(
        "controlled_guard_governance_contract_snapshot restore",
        initial_verify,
    )
    final_verify = restore_and_verify.index(
        "controlled_guard_governance_contract_snapshot verify",
        restore,
    )
    assert initial_verify < restore < final_verify
    assert "return 0" in restore_and_verify[initial_verify:restore]
    assert restore_and_verify.count("rollback-governance") == 3
    assert restore_and_verify.count("rollback-qmt") == 3
    assert "controlled_guard_governance_snapshot" not in restore_and_verify
    assert "controlled_guard_qmt_announcement_snapshot" not in restore_and_verify

    prepared_handoff = bodies["prepared_governance_snapshot"]
    assert "restore|verify" in prepared_handoff
    assert "source_restore_rejected" in prepared_handoff
    assert "new_restore_rejected" in prepared_handoff
    assert '"$GOVERNANCE_TASK_OLD_SOURCE"' in prepared_handoff
    assert '"$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT"' in prepared_handoff
    assert '"$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"' in prepared_handoff
    assert 'controlled_guard_assert_file "$snapshot" 600' in prepared_handoff
    assert '[ -L "$PREPARED_CODE_ROOT" ]' in prepared_handoff
    assert '[ -L "$entrypoint" ]' in prepared_handoff
    assert 'test ! -w "$entrypoint"' in prepared_handoff
    assert '"--${action}-snapshot" - < "$snapshot"' in prepared_handoff
    assert 'run_prepared_python_tool "$entrypoint"' in prepared_handoff
    prepared_qmt_handoff = bodies["prepared_qmt_announcement_snapshot"]
    assert "add_qmt_announcement_task.py" in prepared_qmt_handoff
    assert "$QMT_ANNOUNCEMENT_TASK_OLD_SOURCE" in prepared_qmt_handoff
    assert "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT" in (
        prepared_qmt_handoff
    )
    assert "$ACTIVATION_QMT_ANNOUNCEMENT_NEW_SNAPSHOT" in (
        prepared_qmt_handoff
    )
    prepared_restore = bodies[
        "prepared_restore_and_verify_governance_snapshot"
    ]
    assert 'test "$1" = "$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT"' in (
        prepared_restore
    )
    assert (
        'controlled_guard_restore_and_verify_governance_snapshot "$EXPECTED_SHA"'
        in prepared_restore
    )
    assert "prepared_governance_snapshot" not in prepared_restore
    assert "prepared_qmt_announcement_snapshot" not in prepared_restore

    recovery = bodies["controlled_activation_snapshot_only_recovery"]
    old_branch = recovery[: recovery.index("activation_snapshot_restore_new_set")]
    new_branch = recovery[recovery.index("activation_snapshot_restore_new_set"):]
    assert old_branch.index(
        '"$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT"'
    ) < old_branch.index("controlled_guard_restore_previous_writer_states")
    assert new_branch.index(
        '"$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"'
    ) < new_branch.index("systemctl start probiga")
    assert "publish_deployed_receipt_pending" in new_branch

    success = cutover[cutover.index("CUTOVER_STEP=persist_deployed_receipt_pending"):]
    pending = success.index("persist_deployed_receipt_pending")
    finalized = success.index("controlled_guard_finalize_successful_activation")
    published = success.index("publish_deployed_receipt_pending")
    removed = success.index("activation_snapshot_remove_finalized_before_deploy")
    assert pending < finalized < published < removed


def test_governance_contract_recovery_tool_is_authenticated_and_guarded() -> None:
    deploy = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
        encoding="utf-8"
    )
    for regression in (
        "tests/test_production_deploy_broker.py",
        "tests/test_production_deploy_recovery_state_machine.py",
        "tests/test_production_db_boundary_bootstrap.py",
        "tests/test_production_governance_contract_recovery.py",
    ):
        assert regression in workflow
    bodies = {
        name: _normalized_shell(body)
        for name, body in _shell_function_bodies(deploy).items()
    }
    materialize = bodies["materialize_controlled_governance_contract_tool"]
    assert 'local source_sha="$1"' in materialize
    assert (
        'git --git-dir="$CODE_GIT_CACHE" cat-file -e '
        '"${source_sha}^{commit}"'
        in materialize
    )
    assert (
        '"${source_sha}:deploy/'
        'production_governance_contract_recovery.py"'
        in materialize
    )
    assert "mktemp /tmp/.probiga-governance-contract.XXXXXX" in materialize
    assert 'chown root:root "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL"' in materialize
    assert 'chmod 0444 "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL"' in materialize
    assert 'test "$tool_size" -le 131072' in materialize
    assert materialize.count("sha256sum") >= 2
    assert (
        'test "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL_SHA256" = '
        '"$source_digest"'
        in materialize
    )

    release_lock = bodies["release_lock"]
    assert "/tmp/.probiga-governance-contract.*" in release_lock
    assert 'rm -f -- "$CONTROLLED_GOVERNANCE_CONTRACT_TOOL"' in release_lock

    handoff = bodies["controlled_guard_governance_contract_snapshot"]
    assert 'case "$action" in' in handoff
    assert "restore|verify)" in handoff
    assert 'local snapshot_kind="${4:-forward-governance}"' in handoff
    for sealed_route in (
        "forward-governance:$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT",
        "rollback-governance:$ACTIVATION_GOVERNANCE_OLD_SNAPSHOT",
        "rollback-qmt:$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SNAPSHOT",
    ):
        assert sealed_route in handoff
    assert "activation_snapshot_validate_governance_new" in handoff
    assert "$ACTIVATION_GOVERNANCE_OLD_SHA" in handoff
    assert "$ACTIVATION_QMT_ANNOUNCEMENT_OLD_SHA" in handoff
    assert "controlled_guard_assert_governance_restore_runtime" in handoff
    digest_check = handoff.index(
        'test "$tool_digest" = '
        '"$CONTROLLED_GOVERNANCE_CONTRACT_TOOL_SHA256"'
    )
    deadline = handoff.index("controlled_guard_run_service_gate_with_deadline")
    assert digest_check < deadline
    assert 'sudo -u "$service_user" test -r' in handoff[:deadline]
    assert 'cd "$code_root"' in handoff[:deadline]
    assert '"$release_venv/bin/python" -P' in handoff[deadline:]
    assert (
        '"$CONTROLLED_GOVERNANCE_CONTRACT_TOOL" "$action" '
        '"$snapshot_kind"'
        in handoff
    )
    assert '< "$snapshot"' in handoff
    for failure_code in (
        "snapshot-envelope",
        "sealed-identity",
        "contract-shape",
        "engine-schema",
        "live-count",
        "live-id",
        "live-identity",
        "projection",
        "update-rowcount",
        "volatile-drift",
        "database-runtime",
    ):
        assert (
            f"probiga_governance_contract_failure={failure_code}" in handoff
        )
    assert 'printf "%s\\n" "$gate_output"' not in handoff
    assert 'echo "$gate_output"' not in handoff
    assert "EXPECTED_SHA" not in handoff
    assert "PREPARED_CODE_ROOT" not in handoff

    normalized = _normalized_shell(deploy)
    assert (
        "materialize_controlled_governance_contract_tool "
        '"$PROBIGA_RECOVERY_TOOL_SHA"'
        in normalized
    )
    assert (
        'materialize_controlled_governance_contract_tool "$EXPECTED_SHA"'
        in normalized
    )


def test_governance_contract_handoff_reads_all_release_metadata_before_runner(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the governance metadata regression")

    deploy = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    handoff_body = _shell_function_bodies(deploy)[
        "controlled_guard_governance_contract_snapshot"
    ]
    handoff_definition = (
        "controlled_guard_governance_contract_snapshot() {\n"
        + handoff_body
        + "}\n"
    )
    harness = (
        "set -Eeuo pipefail\n"
        + handoff_definition
        + r'''
sandbox="$(mktemp -d)"
controlled_tool="$(mktemp /tmp/.probiga-governance-contract.XXXXXX)"
trap 'command chmod 0600 "$controlled_tool" 2>/dev/null || true; command rm -f -- "$controlled_tool"; command rm -rf -- "$sandbox"' EXIT

guarded_sha=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
expected_adata_sha=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
expected_adata_tree_sha=cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
expected_release_tree_sha=dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
expected_adapter_seal_sha=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee

CODE_RELEASE_ROOT="$sandbox/releases"
RELEASE_VENV_ROOT="$sandbox/venvs"
ADATA_RUNTIME_ROOT="$sandbox/adata"
ACTIVATION_GOVERNANCE_NEW_SNAPSHOT="$sandbox/governance-new.json"
ACTIVATION_QMT_ANNOUNCEMENT_NEW_SNAPSHOT="$sandbox/qmt-announcement-new.json"
CONTROLLED_GOVERNANCE_CONTRACT_TOOL="$controlled_tool"
runner_trace="$sandbox/runner-trace"

mkdir -p "$CODE_RELEASE_ROOT/$guarded_sha"
mkdir -p "$RELEASE_VENV_ROOT/$guarded_sha/bin"
mkdir -p "$ADATA_RUNTIME_ROOT/$expected_adata_sha-$expected_adata_tree_sha"
printf '%s\n' "$expected_adata_sha" > "$RELEASE_VENV_ROOT/$guarded_sha/.adata.gitsha"
printf '%s\n' "$expected_adata_tree_sha" > "$RELEASE_VENV_ROOT/$guarded_sha/.adata.tree.sha256"
printf '%s\n' "$expected_release_tree_sha" > "$RELEASE_VENV_ROOT/$guarded_sha/.release-tree.sha256"
printf '%s\n' "$expected_adapter_seal_sha" > "$RELEASE_VENV_ROOT/$guarded_sha/.adapter-registry-seal.sha256"
printf '%s\n' snapshot > "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT"
printf '%s\n' tool > "$controlled_tool"
chmod 0444 "$controlled_tool"
CONTROLLED_GOVERNANCE_CONTRACT_TOOL_SHA256="$(sha256sum "$controlled_tool" | cut -d' ' -f1)"

activation_snapshot_validate_governance_new() { return 0; }
controlled_guard_qmt_announcement_snapshot() { return 0; }
controlled_guard_assert_governance_restore_runtime() { return 0; }
controlled_guard_assert_file() { test -f "$1"; }
systemctl() { printf '%s\n' probiga; }
sudo() { return 0; }
controlled_guard_run_service_gate_with_deadline() {
  local argument
  local observed_adata=""
  local observed_adata_tree=""
  local observed_release_tree=""
  local observed_adapter=""
  for argument in "$@"; do
    case "$argument" in
      PROBIGA_EXPECTED_ADATA_SHA=*) observed_adata="${argument#*=}" ;;
      PROBIGA_EXPECTED_ADATA_TREE_SHA256=*) observed_adata_tree="${argument#*=}" ;;
      PROBIGA_RELEASE_TREE_SHA256=*) observed_release_tree="${argument#*=}" ;;
      PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=*) observed_adapter="${argument#*=}" ;;
    esac
  done
  test "$observed_adata" = "$expected_adata_sha" || return 81
  test "$observed_adata_tree" = "$expected_adata_tree_sha" || return 82
  test "$observed_release_tree" = "$expected_release_tree_sha" || return 83
  test "$observed_adapter" = "$expected_adapter_seal_sha" || return 84
  printf '%s,%s,%s,%s\n' \
    "${#observed_adata}" "${#observed_adata_tree}" \
    "${#observed_release_tree}" "${#observed_adapter}" > "$runner_trace"
  return 0
}

GOVERNANCE_CONTRACT_FAILURE_CODE=unset
controlled_guard_governance_contract_snapshot verify "$guarded_sha" \
  "$ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" || exit 90
test "$GOVERNANCE_CONTRACT_FAILURE_CODE" = "" || exit 91
test "$(<"$runner_trace")" = 40,64,64,64 || exit 92
'''
    )
    harness_path = tmp_path / "governance-metadata-handoff.sh"
    harness_path.write_text(harness, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        [bash, str(harness_path)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=20,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_deploy_has_no_multiline_redirection_only_command_substitution() -> None:
    deploy = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )

    assert re.search(r"\$\([ \t]*\r?\n[ \t]*<", deploy) is None


def test_runtime_identity_checks_every_active_writer_and_attested_environment() -> None:
    deploy = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    body = _normalized_shell(
        _shell_function_bodies(deploy)["controlled_guard_verify_restored_runtime"]
    )
    inactive_restore = body.index(
        'if [ "$verification_mode" = rollback-only ] && '
        '[ "$main_active" = inactive ]; then'
    )
    code_root = body.index('test -d "$code_root" || return 1')
    assert inactive_restore < code_root
    assert 'inactive|not-found) ;;' in body[inactive_restore:code_root]
    assert 'RESTORED_RUNTIME_FAILURE_CODE=inactive-rollback-verified' in (
        body[inactive_restore:code_root]
    )
    main = body.index('if [ "$main_active" = active ]; then')
    scheduler = body.index('if [ "$scheduler_load" = loaded ]; then')
    ai = body.index('if [ "$ai_service_load" = loaded ]; then')
    assert main < scheduler < ai < body.rindex("return 0")
    assert "return 0" not in body[main:scheduler]
    for unit in ("probiga", "probiga-scheduler", "probiga-ai-recommendation-worker.service"):
        assert unit in body
    for identity in (
        "PROBIGA_EXPECTED_GIT_SHA=$expected_sha",
        "PROBIGA_CODE_ROOT=$code_root",
        "PROBIGA_RELEASE_TREE_SHA256=$release_tree_sha",
        "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$adapter_registry_seal_sha",
    ):
        assert body.count(identity) >= 2
    for selected_ai_identity in (
        "PROBIGA_EXPECTED_GIT_SHA=$ai_expected_sha",
        "PROBIGA_CODE_ROOT=$ai_code_root",
        "PROBIGA_RELEASE_TREE_SHA256=$ai_release_tree_sha",
        "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$ai_adapter_registry_seal_sha",
    ):
        assert selected_ai_identity in body[ai:]


def test_rollback_runtime_binds_legacy_auxiliary_identity_and_fenced_scheduler() -> None:
    deploy = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    body = _normalized_shell(
        _shell_function_bodies(deploy)["controlled_guard_verify_restored_runtime"]
    )
    deferred_mode = body.index(
        "PROBIGA_STRATEGY_GOVERNANCE_MODE=DEFERRED_DB"
    )
    deferred = body.index(
        "PROBIGA_DEFERRED_SCHEDULER_EXPECTED_GIT_SHA=//p"
    )
    scheduler = body.index('if [ "$scheduler_load" = loaded ]; then')
    ai = body.index('if [ "$ai_service_load" = loaded ]; then')
    assert deferred_mode < deferred < scheduler < ai
    deferred_block = body[deferred:scheduler]
    for proof in (
        '"$scheduler_active:$scheduler_unit_file" = inactive:disabled',
        '"$CODE_RELEASE_ROOT/$deferred_expected_sha"',
        'git -C "$deferred_code_root" rev-parse HEAD',
        '.probiga.gitsha',
        '.adata.gitsha',
        '.adata.tree.sha256',
        '.release-tree.sha256',
        '.adapter-registry-seal.sha256',
    ):
        assert proof in deferred_block
    scheduler_block = body[scheduler:ai]
    assert 'if [ "$deferred_scheduler_fenced" -eq 1 ]; then' in scheduler_block
    assert 'test "$scheduler_active" = inactive' in scheduler_block
    assert 'test "$scheduler_unit_file" = disabled' in scheduler_block
    assert 'systemctl show -p MainPID --value probiga-scheduler' in scheduler_block
    assert 'test -n "$deferred_expected_sha"' in scheduler_block
    assert 'scheduler_expected_sha="$deferred_expected_sha"' in scheduler_block
    assert 'scheduler_code_root="$deferred_code_root"' in scheduler_block
    assert "PROBIGA_ADATA_SOURCE_DIR=$scheduler_adata_source" in scheduler_block
    assert "PYTHONPATH=$scheduler_adata_source:$scheduler_code_root" in scheduler_block
    assert 'if [ "$scheduler_active" = active ]; then' in scheduler_block
    ai_block = body[ai:body.index('case "$ai_timer_load:$ai_timer_active"', ai)]
    assert 'test -n "$deferred_expected_sha"' in ai_block
    assert (
        "$deferred_python_path -P "
        "$deferred_code_root/tools/run_ai_recommendation_worker.py --once"
    ) in ai_block
    for selected_identity in (
        'ai_expected_sha="$deferred_expected_sha"',
        'ai_code_root="$deferred_code_root"',
        'ai_python_path="$deferred_python_path"',
        'ai_adata_sha="$deferred_adata_sha"',
        'ai_adata_tree_sha="$deferred_adata_tree_sha"',
        'ai_adata_source="$deferred_adata_source"',
        'ai_release_tree_sha="$deferred_release_tree_sha"',
        'ai_adapter_registry_seal_sha="$deferred_adapter_registry_seal_sha"',
    ):
        assert selected_identity in ai_block
    for exact_runtime_proof in (
        "PROBIGA_EXPECTED_GIT_SHA=$ai_expected_sha",
        "PROBIGA_CODE_ROOT=$ai_code_root",
        "PROBIGA_EXPECTED_ADATA_SHA=$ai_adata_sha",
        "PROBIGA_EXPECTED_ADATA_TREE_SHA256=$ai_adata_tree_sha",
        "PROBIGA_ADATA_SOURCE_DIR=$ai_adata_source",
        "PYTHONPATH=$ai_adata_source:$ai_code_root",
        "PROBIGA_RELEASE_TREE_SHA256=$ai_release_tree_sha",
        "PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=$ai_adapter_registry_seal_sha",
        '"${cmdline[0]}" = "$ai_python_path"',
        '"$ai_code_root/tools/run_ai_recommendation_worker.py"',
    ):
        assert exact_runtime_proof in ai_block
    assert (
        "$python_path -P $code_root/tools/run_ai_recommendation_worker.py --once"
    ) in ai_block


def test_new_systemd_units_and_activation_identity_bind_tree_and_registry_seal() -> None:
    deploy = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    bodies = _shell_function_bodies(deploy)
    for writer in ("write_dropin", "write_scheduler_dropin", "write_ai_worker_dropin"):
        body = bodies[writer]
        assert "Environment=PROBIGA_RELEASE_TREE_SHA256=" in body
        assert "Environment=PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256=" in body
    assert deploy.count(
        'grep -zFx -- "PROBIGA_RELEASE_TREE_SHA256=$EXPECTED_RELEASE_TREE_SHA256"'
    ) >= 2
    assert deploy.count(
        '"PROBIGA_EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256='
        '$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256"'
    ) >= 2
    trigger = _normalized_shell(bodies["assert_scheduler_triggers_quiescent"])
    assert 'if [ "$active_state" != inactive ]; then' in trigger
    assert "disabled)" in trigger
    assert "inactive|failed" not in trigger


def test_database_guard_cleanup_keeps_fixed_systemd_fence_dropins() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    function_bodies = _shell_function_bodies(deploy_script)
    cleanup = _normalized_shell(
        function_bodies["remove_database_writer_guard_after_recovery"]
    )
    cleanup_restore = _normalized_shell(
        function_bodies[
            "restore_database_writer_guard_after_cleanup_failure"
        ]
    )

    marker_removal = cleanup.index(
        'sudo rm -f -- "$DATABASE_WRITER_GUARD_FILE"'
    )
    assert cleanup.index("assert_database_writer_guard_dropins_loaded") < marker_removal
    assert cleanup.index(
        "assert_database_writer_guard_dropins_loaded", marker_removal
    ) > marker_removal
    assert 'rm -f -- "$dropin"' not in cleanup
    assert "systemctl daemon-reload" not in cleanup
    assert cleanup.count(
        "restore_database_writer_guard_after_cleanup_failure"
    ) >= 3
    assert "controlled_guard_recreate_file" in cleanup_restore
    assert "controlled_guard_install_dropins" in cleanup_restore
    assert "systemctl daemon-reload" in cleanup_restore
    assert "controlled_guard_force_all_writers_fenced" in cleanup_restore
    assert "controlled_guard_assert_boundary" in cleanup_restore
    assert "controlled_guard_assert_dropin_boundary" in cleanup


def test_ai_worker_service_and_timer_states_are_fenced_and_restored() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    function_bodies = _shell_function_bodies(deploy_script)
    fence_assertion = _normalized_shell(
        function_bodies["assert_ai_worker_writer_fence"]
    )
    restore = _normalized_shell(
        function_bodies["restore_ai_worker_previous_state"]
    )
    restore_assertion = _normalized_shell(
        function_bodies["assert_ai_worker_previous_state_restored"]
    )

    for state_name in (
        "PREVIOUS_AI_WORKER_SERVICE_ACTIVE",
        "PREVIOUS_AI_WORKER_SERVICE_ENABLED",
        "PREVIOUS_AI_WORKER_SERVICE_UNIT_FILE_STATE",
        "PREVIOUS_AI_WORKER_TIMER_ACTIVE",
        "PREVIOUS_AI_WORKER_TIMER_ENABLED",
        "PREVIOUS_AI_WORKER_TIMER_UNIT_FILE_STATE",
    ):
        assert state_name in deploy_script

    assert '"$AI_WORKER_SERVICE"' in fence_assertion
    assert '"$AI_WORKER_TIMER"' in fence_assertion
    assert "ActiveState" in fence_assertion
    assert "UnitFileState" in fence_assertion
    assert "disabled|static" in fence_assertion
    assert 'test "$timer_unit_file_state" = disabled' in fence_assertion
    assert restore.count('systemctl enable "$AI_WORKER_SERVICE"') == 1
    assert restore.count('systemctl disable "$AI_WORKER_SERVICE"') == 1
    assert restore.count('systemctl start "$AI_WORKER_SERVICE"') == 1
    assert restore.count('systemctl stop "$AI_WORKER_SERVICE"') == 1
    assert restore.count('systemctl enable "$AI_WORKER_TIMER"') == 1
    assert restore.count('systemctl disable "$AI_WORKER_TIMER"') == 1
    assert restore.count('systemctl start "$AI_WORKER_TIMER"') == 1
    assert restore.count('systemctl stop "$AI_WORKER_TIMER"') == 1
    assert "PREVIOUS_AI_WORKER_SERVICE_UNIT_FILE_STATE" in restore_assertion
    assert "PREVIOUS_AI_WORKER_TIMER_UNIT_FILE_STATE" in restore_assertion
    assert "PREVIOUS_AI_WORKER_SERVICE_ACTIVE" in restore_assertion
    assert "PREVIOUS_AI_WORKER_TIMER_ACTIVE" in restore_assertion

    cutover_start = deploy_script.index("CUTOVER_STEP=stop_auxiliary_writers")
    cutover_end = deploy_script.index("CUTOVER_STEP=stop_scheduler", cutover_start)
    cutover = _normalized_shell(deploy_script[cutover_start:cutover_end])
    timer_disable = cutover.index('systemctl disable "$AI_WORKER_TIMER"')
    service_disable = cutover.index('systemctl disable "$AI_WORKER_SERVICE"')
    timer_stop = cutover.index('systemctl stop "$AI_WORKER_TIMER"')
    service_stop = cutover.index('systemctl stop "$AI_WORKER_SERVICE"')
    fence_check = cutover.index("assert_ai_worker_writer_fence")
    assert timer_disable < service_disable < timer_stop < service_stop < fence_check

    rollback_start = deploy_script.index("rollback() {")
    rollback_end = deploy_script.index(
        "trap 'rollback \"$?\" \"$LINENO\"' ERR", rollback_start
    )
    rollback = _normalized_shell(deploy_script[rollback_start:rollback_end])
    assert "restore_ai_worker_previous_state" in rollback
    assert "assert_ai_worker_previous_state_restored" in rollback
    assert 'systemctl disable "$AI_WORKER_SERVICE"' in rollback
    assert 'systemctl disable "$AI_WORKER_TIMER"' in rollback
    assert "assert_ai_worker_writer_fence" in rollback

    success_start = deploy_script.index(
        "CUTOVER_STEP=restore_ai_worker_previous_state"
    )
    success_end = deploy_script.index(
        "CUTOVER_STEP=verify_scheduler_triggers_quiescent", success_start
    )
    success = _normalized_shell(deploy_script[success_start:success_end])
    assert "restore_ai_worker_previous_state" in success
    assert "assert_ai_worker_previous_state_restored" in success


def test_schema_preflight_is_read_only_and_global_trust_isolated() -> None:
    schema_tool = (
        ROOT / "tools/prepare_strategy_governance_schema.py"
    ).read_text(encoding="utf-8")
    preflight_start = schema_tool.index(
        "def _preflight_governance_cutover_recovery("
    )
    preflight_end = schema_tool.index("def _connect_admin(", preflight_start)
    preflight = schema_tool[preflight_start:preflight_end]
    trust_setter_start = schema_tool.index("def _set_trust(")
    trust_setter_end = schema_tool.index("def _acquire_lock(", trust_setter_start)
    trust_setter = schema_tool[trust_setter_start:trust_setter_end]
    prepare_start = schema_tool.index("def prepare_schema(")
    prepare_end = schema_tool.index("def main(", prepare_start)
    prepare = schema_tool[prepare_start:prepare_end]

    assert "run_v3_migrations(boundary.migrator_engine, dry_run=True)" in preflight
    assert "validate_attestation_schema(" in preflight
    assert "require_triggers=False" in preflight
    assert "ensure_attestation_tables(" not in preflight
    assert "ensure_strategy_governance_tables(" not in preflight
    assert "seed_governance_registry(" not in preflight
    assert "SET GLOBAL" not in preflight
    assert not re.search(
        r'execute\s*\(\s*(?:text\s*\(\s*)?["\']\s*'
        r"(?:CREATE|ALTER|DROP|INSERT|UPDATE|DELETE|REPLACE|TRUNCATE)\b",
        preflight,
        flags=re.IGNORECASE,
    )

    assert "SET GLOBAL log_bin_trust_function_creators = ON" in trust_setter
    assert "SET GLOBAL log_bin_trust_function_creators = OFF" in trust_setter
    assert 'if phase == "recover":' in prepare
    assert 'if phase == "preflight":' in prepare
    assert "else:" in prepare
    assert "_recover_trust(recovery_boundary)" in prepare
    assert "_preflight_schema(boundary)" in prepare
    assert "_cutover_schema(" in prepare
    assert 'repair_interrupted_legacy=phase == "resume"' in prepare
    assert 'fenced_phases = {"cutover", "resume"}' in prepare
    assert "if phase in fenced_phases and not writers_fenced:" in prepare
    assert "_recover_trust(_open_recovery_boundary())" in prepare


def test_database_boundary_bootstrap_precedes_database_preflight() -> None:
    deploy = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    bodies = _shell_function_bodies(deploy)
    initial = bodies["run_initial_database_schema_preflight"]
    required = initial.split("    REQUIRED)", 1)[1].split("    *)", 1)[0]
    bootstrap = initial.index("CUTOVER_STEP=prepare_production_database_boundary")
    preflight = initial.index(
        "CUTOVER_STEP=preflight_strategy_governance_database_schema"
    )
    writer_journal = deploy.index("CUTOVER_STEP=persist_database_writer_restore_journal")
    commit = deploy.index("CUTOVER_STEP=commit_production_database_boundary")
    guard = deploy.index("CUTOVER_STEP=install_database_writer_guard_dropins")
    preflight_validator = initial.index(
        "validate_initial_database_schema_preflight_json",
        preflight,
    )
    required_start = initial.index("    REQUIRED)")
    assert required_start < bootstrap < preflight
    assert "run_database_boundary_bootstrap prepare" in required
    assert initial.count("--phase preflight") == 1
    assert preflight < preflight_validator

    definition = deploy.index("run_initial_database_schema_preflight() {")
    calls = [
        match.start()
        for match in re.finditer(
            r"(?m)^run_initial_database_schema_preflight$",
            deploy,
        )
        if match.start() > definition
    ]
    assert len(calls) == 1
    initial_call = calls[0]
    deferred_dispatch = deploy.index(
        'if [ "$STRATEGY_GOVERNANCE_MODE" = DEFERRED_DB ]; then',
        initial_call,
    )
    same_sha = deploy.index(
        'if [ "$PREVIOUS_SHA" = "$EXPECTED_SHA" ]; then',
        deferred_dispatch,
    )
    stop_api = deploy.index("CUTOVER_STEP=stop_api", same_sha)
    assert initial_call < deferred_dispatch < same_sha
    assert same_sha < writer_journal < commit < guard < stop_api
    assert "run_database_boundary_bootstrap rollback" in deploy


def test_initial_database_preflight_rejects_unready_recovery_before_api_stop():
    deploy = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    validator_body = _shell_function_bodies(deploy)[
        "validate_initial_database_schema_preflight_json"
    ]
    python_source = validator_body.split("    '\n", 1)[1].rsplit("\n'", 1)[0]
    planner_names = [
        "ai_bridge",
        "analysis_output",
        "recommended_run_history",
        "sim_trade",
        "qmt_catalog",
        "qmt_audit",
    ]
    validator_names = [f"validator_{index}" for index in range(33)]
    payload = {
        "status": "ok",
        "phase": "preflight",
        "runtime_privilege_boundary_verified": True,
        "runtime_least_privilege_verified": True,
        "runtime_legacy_ddl_compatibility": False,
        "runtime_self_definer_routine_count": 0,
        "migrator_self_definer_routine_count": 0,
        "runtime_definer_routine_count": 0,
        "runtime_definer_routine_inventory_verified": True,
        "runtime_definer_routine_inventory_complete": True,
        "runtime_privileges_changed": False,
        "global_trust_changed": False,
        "trust_restoration_verified": True,
        "automatic_real_order_submission": False,
        "governance_cutover_recovery": {
            "schema": "probiga.strategy-governance-cutover-recovery.v1",
            "status": "CUTOVER_READY",
            "read_only": True,
            "full_migration_marker_present": False,
            "full_migration_marker_hash_verified": False,
            "expected_trigger_count": 0,
            "installed_trigger_count": 0,
            "missing_trigger_count": 0,
            "resume_required": False,
        },
        "runtime_schema_bundle": {
            "schema": "probiga.production-runtime-schema-bundle.v1",
            "contract_hash": (
                "61f9ddfb3179f30c9976a090fce00adb8613d4e38d698c6cfc954f957084845f"
            ),
            "migration_count": 30,
            "seed_count": 3,
            "trigger_installation_policy": "FROZEN_RELEASE_BROKER_ONLY",
            "broker_owned_trigger_migration_names": [
                "qmt_stock_catalog_truth",
                "qmt_trade_calendar",
                "market_field_capture",
                "auxiliary_runtime",
            ],
            "validator_names": validator_names,
            "validator_count": 33,
            "contracts": {
                name: {
                    "status": (
                        "MIGRATION_REQUIRED"
                        if name == validator_names[-1] else "READY"
                    ),
                    "read_only": True,
                }
                for name in validator_names
            },
            "contract_count": 33,
            "recovery_planner_names": planner_names,
            "recovery_planner_count": 6,
            "recovery_plans": {
                name: {
                    "status": "PLANNED",
                    "read_only": True,
                    "ready_for_privileged_apply": True,
                    "plan_sha256": character * 64,
                    **(
                        {
                            "recovery_bundle_sha256": character * 64,
                            "atomic_plan_sha256": atomic * 64,
                        }
                        if atomic is not None else {}
                    ),
                }
                for name, character, atomic in zip(
                    planner_names,
                    "abcdef",
                    ("1", "2", None, None, None, None),
                )
            },
            "recovery_plan_count": 6,
            "recovery_ready_for_privileged_apply": True,
            "migration_required": True,
            "read_only": True,
        },
    }

    accepted = subprocess.run(
        [sys.executable, "-I", "-c", python_source, "0"],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        check=False,
    )
    assert accepted.returncode == 0, accepted.stdout + accepted.stderr
    assert json.loads(accepted.stdout) == payload

    rejected_status_mismatch = subprocess.run(
        [sys.executable, "-I", "-c", python_source, "2"],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        check=False,
    )
    assert rejected_status_mismatch.returncode == 2
    assert rejected_status_mismatch.stdout == ""
    assert rejected_status_mismatch.stderr == ""

    interrupted = deepcopy(payload)
    interrupted["governance_cutover_recovery"] = {
        "schema": "probiga.strategy-governance-cutover-recovery.v1",
        "status": "RESUME_REQUIRED",
        "read_only": True,
        "full_migration_marker_present": True,
        "full_migration_marker_hash_verified": True,
        "expected_trigger_count": 40,
        "installed_trigger_count": 0,
        "missing_trigger_count": 40,
        "resume_required": True,
    }
    accepted_interrupted = subprocess.run(
        [sys.executable, "-I", "-c", python_source, "0"],
        input=json.dumps(interrupted),
        capture_output=True,
        text=True,
        check=False,
    )
    assert accepted_interrupted.returncode == 0, (
        accepted_interrupted.stdout + accepted_interrupted.stderr
    )
    assert json.loads(accepted_interrupted.stdout) == interrupted

    false_resume = deepcopy(interrupted)
    false_resume["governance_cutover_recovery"]["resume_required"] = False
    rejected_false_resume = subprocess.run(
        [sys.executable, "-I", "-c", python_source, "0"],
        input=json.dumps(false_resume),
        capture_output=True,
        text=True,
        check=False,
    )
    assert rejected_false_resume.returncode == 2, (
        rejected_false_resume.stdout + rejected_false_resume.stderr
    )

    blocked = deepcopy(payload)
    blocked["runtime_schema_bundle"][
        "recovery_ready_for_privileged_apply"
    ] = False
    rejected = subprocess.run(
        [sys.executable, "-I", "-c", python_source, "0"],
        input=json.dumps(blocked),
        capture_output=True,
        text=True,
        check=False,
    )
    assert rejected.returncode == 2, rejected.stdout + rejected.stderr

    invalid_atomic = deepcopy(payload)
    invalid_atomic["runtime_schema_bundle"]["recovery_plans"][
        "analysis_output"
    ]["atomic_plan_sha256"] = "not-a-sha256"
    rejected_atomic = subprocess.run(
        [sys.executable, "-I", "-c", python_source, "0"],
        input=json.dumps(invalid_atomic),
        capture_output=True,
        text=True,
        check=False,
    )
    assert rejected_atomic.returncode == 2, (
        rejected_atomic.stdout + rejected_atomic.stderr
    )

    bodies = _shell_function_bodies(deploy)
    initial = bodies["run_initial_database_schema_preflight"]
    preflight = initial.index(
        "CUTOVER_STEP=preflight_strategy_governance_database_schema"
    )
    validation = initial.index(
        "| validate_initial_database_schema_preflight_json", preflight
    )
    assert preflight < validation

    definition = deploy.index("run_initial_database_schema_preflight() {")
    initial_call = next(
        match.start()
        for match in re.finditer(
            r"(?m)^run_initial_database_schema_preflight$",
            deploy,
        )
        if match.start() > definition
    )
    deferred_dispatch = deploy.index(
        'if [ "$STRATEGY_GOVERNANCE_MODE" = DEFERRED_DB ]; then',
        initial_call,
    )
    writer_journal = deploy.index(
        "CUTOVER_STEP=persist_database_writer_restore_journal",
        deferred_dispatch,
    )
    stop_api = deploy.index("CUTOVER_STEP=stop_api", writer_journal)
    assert initial_call < deferred_dispatch < writer_journal < stop_api


def test_initial_preflight_failure_diagnostic_is_allowlisted_before_emission():
    from tools import prepare_strategy_governance_schema as schema_tool

    deploy = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    bodies = _shell_function_bodies(deploy)
    validator_body = bodies["validate_initial_database_schema_preflight_json"]
    python_source = validator_body.split("    '\n", 1)[1].rsplit("\n'", 1)[0]
    payload = {
        "status": "blocked",
        "phase": "preflight",
        "reason": "database schema preparation failed closed",
        "diagnostic_schema": (
            "probiga.strategy-governance-preflight-diagnostic.v1"
        ),
        "preflight_substage": "runtime_privilege_boundary",
        "reason_code": "PREFLIGHT_RUNTIME_PRIVILEGE_BOUNDARY_BLOCKED",
        "global_trust_changed": False,
        "trust_restoration_verified": False,
        "restore_primary_verified": False,
        "restore_secondary_verified": False,
        "restore_fresh_admin_verified": False,
        "runtime_trust_off_verified": False,
        "runtime_privileges_changed": False,
        "automatic_real_order_submission": False,
    }

    accepted = subprocess.run(
        [sys.executable, "-I", "-c", python_source, "2"],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        check=False,
    )
    assert accepted.returncode == 0, accepted.stdout + accepted.stderr
    assert json.loads(accepted.stdout) == payload

    stage_reason_codes = {
        **schema_tool.PREFLIGHT_STAGE_REASON_CODES,
        schema_tool.PREFLIGHT_UNCLASSIFIED_STAGE: (
            schema_tool.PREFLIGHT_UNCLASSIFIED_REASON_CODE
        ),
    }
    for substage, reason_code in sorted(stage_reason_codes.items()):
        candidate = {
            **payload,
            "preflight_substage": substage,
            "reason_code": reason_code,
        }
        accepted_candidate = subprocess.run(
            [sys.executable, "-I", "-c", python_source, "2"],
            input=json.dumps(candidate),
            capture_output=True,
            text=True,
            check=False,
        )
        assert accepted_candidate.returncode == 0, (
            substage,
            accepted_candidate.stdout + accepted_candidate.stderr,
        )
        assert json.loads(accepted_candidate.stdout) == candidate

    rejected_wrong_status = subprocess.run(
        [sys.executable, "-I", "-c", python_source, "0"],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        check=False,
    )
    assert rejected_wrong_status.returncode == 2
    assert rejected_wrong_status.stdout == ""
    assert rejected_wrong_status.stderr == ""

    invalid_payloads = []
    mismatched_code = deepcopy(payload)
    mismatched_code["reason_code"] = "PREFLIGHT_DATABASE_BOUNDARY_BLOCKED"
    invalid_payloads.append(mismatched_code)
    injected_stage = deepcopy(payload)
    injected_stage["preflight_substage"] += "\nsecret=value"
    invalid_payloads.append(injected_stage)
    injected_reason = deepcopy(payload)
    injected_reason["reason"] += "\npassword=do-not-print"
    invalid_payloads.append(injected_reason)
    extra_field = deepcopy(payload)
    extra_field["detail"] = "do-not-print"
    invalid_payloads.append(extra_field)
    non_boolean = deepcopy(payload)
    non_boolean["runtime_privileges_changed"] = 0
    invalid_payloads.append(non_boolean)
    for invalid in invalid_payloads:
        rejected = subprocess.run(
            [sys.executable, "-I", "-c", python_source, "2"],
            input=json.dumps(invalid),
            capture_output=True,
            text=True,
            check=False,
        )
        assert rejected.returncode == 2, rejected.stdout + rejected.stderr
        assert rejected.stdout == ""

    duplicate_reason = (
        '{"reason":"password=do-not-print",'
        + json.dumps(payload, separators=(",", ":"))[1:]
    )
    rejected_duplicate = subprocess.run(
        [sys.executable, "-I", "-c", python_source, "2"],
        input=duplicate_reason,
        capture_output=True,
        text=True,
        check=False,
    )
    assert rejected_duplicate.returncode == 2
    assert rejected_duplicate.stdout == ""
    assert rejected_duplicate.stderr == ""
    assert "do-not-print" not in (
        rejected_duplicate.stdout + rejected_duplicate.stderr
    )

    for function_name in (
        "run_initial_database_schema_preflight",
        "select_fenced_strategy_governance_schema_phase",
    ):
        runner = bodies[function_name]
        validation = runner.index(
            "| validate_initial_database_schema_preflight_json"
        )
        emission = runner.index("printf '%s\\n' \"$output\"", validation)
        assert validation < emission
        assert 'test "$tool_status" -eq 0 || return "$tool_status"' in runner
        assert 'output="$validated_output"' in runner


@pytest.mark.parametrize(
    "runner_name",
    (
        "run_initial_database_schema_preflight",
        "select_fenced_strategy_governance_schema_phase",
    ),
)
@pytest.mark.parametrize(
    ("payload_kind", "tool_status", "expected_status", "emitted"),
    (
        ("blocked", 2, 2, True),
        ("blocked", 0, 2, False),
        ("ok", 0, 0, True),
        ("ok", 2, 2, False),
    ),
)
def test_schema_preflight_runners_bind_payload_to_tool_status_and_emit_only_canonical(
    runner_name,
    payload_kind,
    tool_status,
    expected_status,
    emitted,
):
    bash = _bash()
    if bash is None:
        pytest.skip("bash is unavailable")
    deploy = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    runner = _shell_function_bodies(deploy)[runner_name]
    raw_payload = json.dumps(
        {
            "status": payload_kind,
            "untrusted": "password=do-not-print",
        },
        separators=(",", ":"),
    )
    canonical_payloads = {
        "blocked": json.dumps(
            {
                "preflight_substage": "database_boundary",
                "reason_code": "PREFLIGHT_DATABASE_BOUNDARY_BLOCKED",
                "status": "blocked",
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        "ok": json.dumps(
            {
                "governance_cutover_recovery": {
                    "resume_required": False,
                },
                "status": "ok",
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
    }
    script = f"""
set -uo pipefail
STRATEGY_GOVERNANCE_MODE=DEFERRED_DB
PREPARED_CODE_ROOT=/prepared
RELEASE_VENV_ROOT=/venv
EXPECTED_SHA={'a' * 40}
BOOTSTRAP_PYTHON='{Path(sys.executable).as_posix()}'
RAW_PAYLOAD={raw_payload!r}
PAYLOAD_KIND={payload_kind!r}
TOOL_STATUS={tool_status}
run_prepared_database_migration_tool() {{
  printf '%s' "$RAW_PAYLOAD"
  return "$TOOL_STATUS"
}}
run_database_boundary_bootstrap() {{ return 99; }}
validate_initial_database_schema_preflight_json() {{
  local ignored_python="$1"
  local observed_status="$2"
  local observed_payload
  observed_payload="$(cat)"
  test "$observed_payload" = "$RAW_PAYLOAD" || return 2
  case "$PAYLOAD_KIND:$observed_status" in
    blocked:2) printf '%s' {canonical_payloads['blocked']!r} ;;
    ok:0) printf '%s' {canonical_payloads['ok']!r} ;;
    *) return 2 ;;
  esac
}}
{runner_name}() {{
{runner}
}}
{runner_name}
runner_status=$?
exit "$runner_status"
"""

    completed = subprocess.run(
        [bash, "-c", script],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == expected_status, (
        completed.stdout + completed.stderr
    )
    if emitted:
        assert completed.stdout == canonical_payloads[payload_kind] + "\n"
    else:
        assert completed.stdout == ""
    assert "do-not-print" not in completed.stdout + completed.stderr


def test_deferred_dispatch_uses_the_same_strict_initial_schema_preflight():
    deploy = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    bodies = _shell_function_bodies(deploy)
    runner = bodies["run_initial_database_schema_preflight"]
    assert "DEFERRED_DB) ;;" in runner
    assert runner.count("prepare_strategy_governance_schema.py") == 1
    assert runner.count("--phase preflight") == 1
    assert runner.count("validate_initial_database_schema_preflight_json") == 1
    assert "prepare_strategy_governance_deferred_schema.py" not in runner
    assert "systemctl stop" not in runner

    definition = deploy.index("run_initial_database_schema_preflight() {")
    initial_call = next(
        match.start()
        for match in re.finditer(
            r"(?m)^run_initial_database_schema_preflight$",
            deploy,
        )
        if match.start() > definition
    )
    deferred_dispatch = deploy.index(
        'if [ "$STRATEGY_GOVERNANCE_MODE" = DEFERRED_DB ]; then',
        initial_call,
    )
    assert initial_call < deferred_dispatch


@pytest.mark.parametrize(
    ("mode", "expected_events"),
    (
        (
            "DEFERRED_DB",
            [
                "required_tool",
                "required_validator",
                "deferred_dispatch",
                "stop_api",
            ],
        ),
        (
            "REQUIRED",
            ["boundary:prepare", "required_tool", "required_validator"],
        ),
    ),
)
def test_initial_schema_preflight_runtime_branch_order(mode, expected_events):
    bash = _bash()
    if bash is None:
        pytest.skip("bash is unavailable")
    deploy = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    runner = _shell_function_bodies(deploy)[
        "run_initial_database_schema_preflight"
    ]
    script = f"""
set -euo pipefail
STRATEGY_GOVERNANCE_MODE={mode}
PREPARED_CODE_ROOT=/prepared
RELEASE_VENV_ROOT=/venv
EXPECTED_SHA={'a' * 40}
VALIDATOR_STATUS=0
run_prepared_database_migration_tool() {{
  printf '%s\n' required_tool >&2
  printf '%s' '{{"status":"ok"}}'
}}
run_database_boundary_bootstrap() {{
  printf 'boundary:%s\n' "$1" >&2
}}
validate_initial_database_schema_preflight_json() {{
  cat
  printf '%s\n' required_validator >&2
  return "$VALIDATOR_STATUS"
}}
run_initial_database_schema_preflight() {{
{runner}
}}
run_initial_database_schema_preflight
if [ "$STRATEGY_GOVERNANCE_MODE" = DEFERRED_DB ]; then
  printf '%s\n' deferred_dispatch >&2
  printf '%s\n' stop_api >&2
fi
"""

    completed = subprocess.run(
        [bash, "-c", script],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert completed.stderr.splitlines() == expected_events


def test_rejected_deferred_initial_preflight_never_reaches_writer_stop():
    bash = _bash()
    if bash is None:
        pytest.skip("bash is unavailable")
    deploy = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    runner = _shell_function_bodies(deploy)[
        "run_initial_database_schema_preflight"
    ]
    script = f"""
set -euo pipefail
STRATEGY_GOVERNANCE_MODE=DEFERRED_DB
PREPARED_CODE_ROOT=/prepared
RELEASE_VENV_ROOT=/venv
EXPECTED_SHA={'a' * 40}
run_prepared_database_migration_tool() {{
  printf '%s\n' required_tool >&2
  printf '%s' '{{"status":"blocked"}}'
}}
validate_initial_database_schema_preflight_json() {{
  cat >/dev/null
  printf '%s\n' required_validator_rejected >&2
  return 2
}}
run_database_boundary_bootstrap() {{ return 99; }}
run_initial_database_schema_preflight() {{
{runner}
}}
run_initial_database_schema_preflight
printf '%s\n' deferred_dispatch >&2
printf '%s\n' stop_api >&2
"""

    completed = subprocess.run(
        [bash, "-c", script],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 2, completed.stdout + completed.stderr
    assert completed.stderr.splitlines() == [
        "required_tool",
        "required_validator_rejected",
    ]


@pytest.mark.parametrize(
    ("resume_required", "expected_phase"),
    ((False, "cutover"), (True, "resume")),
)
def test_fenced_schema_phase_selector_uses_only_validated_recovery_state(
    resume_required,
    expected_phase,
):
    bash = _bash()
    if bash is None:
        pytest.skip("bash is unavailable")
    deploy = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    selector = _shell_function_bodies(deploy)[
        "select_fenced_strategy_governance_schema_phase"
    ]
    payload = json.dumps({
        "governance_cutover_recovery": {
            "resume_required": resume_required,
        },
    })
    bootstrap_python = Path(sys.executable).as_posix()
    script = f"""
set -euo pipefail
PREPARED_CODE_ROOT=/prepared
RELEASE_VENV_ROOT=/venv
EXPECTED_SHA={'a' * 40}
BOOTSTRAP_PYTHON='{bootstrap_python}'
run_prepared_database_migration_tool() {{
  test "$2" = --phase
  test "$3" = preflight
  printf '%s' '{payload}'
}}
validate_initial_database_schema_preflight_json() {{ cat; }}
select_fenced_strategy_governance_schema_phase() {{
{selector}
}}
select_fenced_strategy_governance_schema_phase
printf '%s\n' "$FENCED_STRATEGY_GOVERNANCE_SCHEMA_PHASE"
"""

    completed = subprocess.run(
        [bash, "-c", script],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert completed.stdout.splitlines()[-1] == expected_phase


def test_all_legacy_mutable_production_entrypoints_are_retired() -> None:
    retired_entrypoints = (
        "deploy/deploy.sh",
        "deploy/deploy.ps1",
        "deploy/deploy_today.py",
        "deploy/migrate_to_cloud.ps1",
        "deploy/publish_to_cloud.bat",
        "deploy/upload_and_restart.ps1",
        "deploy/upload_and_restart.bat",
        "deploy/upload_capital_flow_fixes.py",
        "deploy/upload_portfolio_profit_fix.ps1",
        "deploy/upload_portfolio_profit_fix.bat",
        "tools/_kill_and_restart.py",
        "tools/_restart_probiga.py",
        "tools/_run_akshare_fill.py",
        "tools/_run_kline_incremental.py",
        "tools/_run_plate_sync.py",
        "tools/_upload_sync_sm.py",
    )
    for relative in retired_entrypoints:
        source = (ROOT / relative).read_text(encoding="utf-8")
        assert "retired" in source.casefold(), relative

    python_fences = (
        "deploy/deploy_today.py",
        "tools/_kill_and_restart.py",
        "tools/_restart_probiga.py",
        "tools/_run_akshare_fill.py",
        "tools/_run_kline_incremental.py",
        "tools/_run_plate_sync.py",
        "tools/_upload_sync_sm.py",
    )
    forbidden_tokens = (
        "production_ssh_client",
        "production_ssh_connect_kwargs",
        "paramiko",
        ".connect(",
        "open_sftp",
        "sftp",
        "exec_command",
        "open_session",
        "invoke_shell",
        "subprocess",
        "os.system",
        "pip install",
        "systemctl",
        "restart",
        "nohup",
    )
    for relative in python_fences:
        source = (ROOT / relative).read_text(encoding="utf-8")
        folded = source.casefold()
        assert "RETIRED_EXIT_CODE = 2" in source, relative
        assert "return RETIRED_EXIT_CODE" in source, relative
        assert "raise SystemExit(main())" in source, relative
        for forbidden in forbidden_tokens:
            assert forbidden not in folded, (relative, forbidden)

        completed = subprocess.run(
            [sys.executable, str(ROOT / relative)],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert completed.returncode == 2, relative
        assert completed.stdout == "", relative
        assert "retired" in completed.stderr.casefold(), relative


def test_deferred_database_release_installs_base_schema_and_stays_fail_closed():
    deploy = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    bodies = _shell_function_bodies(deploy)
    deferred = bodies["deploy_deferred_database_release"]
    scheduler_cmdline = bodies[
        "assert_deferred_scheduler_process_cmdline"
    ]
    capture_identity = bodies["capture_deferred_scheduler_identity"]
    prepare_release = bodies["prepare_release"]
    main_dropin = bodies["write_dropin"]
    auth_header_writer = bodies["write_admin_auth_header_file"]
    static_cutover = bodies["point_static_release_to_checkout"]
    static_verifier = bodies["assert_nginx_static_matches_checkout"]
    deferred_writer_fence = bodies["fence_deferred_release_writers"]
    verifier = deploy[
        deploy.index("assert_deferred_database_runtime() {"):
        deploy.index("rollback_deferred_database_release() {")
    ]
    rollback = deploy[
        deploy.index("rollback_deferred_database_release() {"):
        deploy.index("deploy_deferred_database_release() {")
    ]

    assert "STRATEGY_GOVERNANCE_MODE=DEFERRED_DB" in deploy
    assert "capture_deferred_scheduler_identity" in prepare_release
    assert prepare_release.index("capture_deferred_scheduler_identity") < (
        prepare_release.index("write_dropin")
    )
    assert "PROBIGA_EXPECTED_GIT_SHA" in capture_identity
    assert "PROBIGA_BUILD_COMMIT_SHA" in capture_identity
    assert "PROBIGA_CODE_ROOT" in capture_identity
    assert 'test "$observed_build_sha" = "$observed_expected_sha"' in (
        capture_identity
    )
    assert "active:enabled" in capture_identity
    assert "inactive:disabled" in capture_identity
    assert 'DEFERRED_SCHEDULER_EXPECTED_SHA="$observed_expected_sha"' in (
        capture_identity
    )
    assert 'DEFERRED_SCHEDULER_CODE_ROOT="$observed_code_root"' in (
        capture_identity
    )
    assert "assert_deferred_scheduler_process_cmdline" in capture_identity
    assert "assert_deferred_scheduler_process_cmdline" in deferred
    for exact_argv_proof in (
        'mapfile -d \'\' -t scheduler_argv < "/proc/$scheduler_pid/cmdline"',
        'test "${#scheduler_argv[@]}" -eq 3',
        '"$RELEASE_VENV_ROOT/$expected_sha/bin/python"',
        'test "${scheduler_argv[1]}" = -P',
        '"$expected_code_root/tools/run_scheduler_daemon.py"',
    ):
        assert exact_argv_proof in scheduler_cmdline
    for variable in (
        "PROBIGA_DEFERRED_SCHEDULER_EXPECTED_GIT_SHA",
        "PROBIGA_DEFERRED_SCHEDULER_CODE_ROOT",
    ):
        assert variable in main_dropin
        assert variable in verifier
    assert "PROBIGA_ADMIN_TOKEN" not in deploy
    assert "get_admin_auth_config" in auth_header_writer
    assert "chown root:root" in auth_header_writer
    assert "chmod 0600" in auth_header_writer
    assert "print(" not in auth_header_writer
    assert "/usr/sbin/nginx -t" in static_cutover
    assert "systemctl reload nginx" in static_cutover
    assert "systemctl is-active --quiet nginx" in static_cutover
    assert "for attempt in $(seq 1 15)" in static_verifier
    assert "cmp --silent" in static_verifier
    for absent_path in (
        "/etc/probiga/mysql-trigger-admin.ini",
        "/etc/probiga/mysql-migrator.ini",
        "/home/probiga-deploy/.probiga-db-boundary-stage",
    ):
        assert absent_path in deploy
    assert "--deferred-disable --snapshot-file" in deferred
    assert "fence_deferred_release_writers" in deferred
    assert "--deferred-release-fence-only" in deferred_writer_fence
    assert "systemctl stop probiga-scheduler" in deferred
    assert "systemctl disable probiga-scheduler" in deferred
    assert "systemctl start probiga-scheduler" not in deferred
    assert (
        'test "$(systemctl show -p MainPID --value probiga-scheduler)" = 0'
        in deferred
    )
    assert 'systemctl stop "$MAIN_SERVICE"' in deferred
    assert deferred.index('systemctl stop "$MAIN_SERVICE"') < deferred.index(
        "--deferred-disable --snapshot-file"
    )
    assert deferred.index("systemctl stop probiga-scheduler") < deferred.index(
        "--deferred-disable --snapshot-file"
    )
    assert "prepare_strategy_governance_deferred_schema.py" in deferred
    assert "--apply --writers-fenced" in _normalized_shell(deferred)
    assert 'if schema_result="$(run_prepared_python_tool' in deferred
    assert "deploy_failure phase=cutover cutover_step=%s status=%s" in deferred
    assert 'return "$schema_status"' in deferred
    assert deferred.index("printf '%s\\n' \"$schema_result\" >&2") < (
        deferred.index('return "$schema_status"')
    )
    assert deferred.index("--deferred-disable --snapshot-file") < deferred.index(
        "--apply --writers-fenced"
    )
    assert deferred.index("fence_deferred_release_writers") < deferred.index(
        "--apply --writers-fenced"
    )
    assert deferred.index("--apply --writers-fenced") < (
        deferred.index("install_deferred_main_runtime")
    )
    assert "DEPLOYED_CODE_ONLY_DEGRADED" in deferred
    assert "run_database_boundary_bootstrap" not in deferred
    assert "prepare_strategy_governance_schema.py" not in deferred

    assert "PROBIGA_STRATEGY_GOVERNANCE_MODE=DEFERRED_DB" in verifier
    assert "PROBIGA_STRATEGY_GOVERNANCE_BASE_SCHEMA_READY=true" in verifier
    assert '--header @"$admin_header"' in verifier
    assert "fence_deferred_release_writers" in verifier
    assert 'payload.get("status") == "degraded"' in verifier
    assert 'payload.get("base_schema_ready") is True' in verifier
    assert 'payload.get("activation_enabled") is False' in verifier
    assert 'revision.get("expected_git_sha") == expected_sha' in verifier
    assert 'standalone.get("fenced") is True' in verifier
    assert 'standalone.get("active") is False' in verifier
    assert 'standalone.get("enabled") is False' in verifier
    assert 'identity.get("identity_mode") == "FENCED_DEFERRED"' in verifier
    assert 'identity.get("api_build_sha") == expected_sha' in verifier
    assert 'identity.get("expected_build_sha") == expected_sha' in verifier
    assert 'identity.get("observed_build_sha") is None' in verifier
    assert 'identity.get("same_build_as_api") is None' in verifier
    assert 'revision.get("expected_sha") == expected_sha' not in verifier
    assert "--retry 45" in verifier
    assert "--retry-max-time 120" in verifier
    assert 'allocations[0].get("target_type") == "CASH"' in verifier
    assert "for deferred_v3_endpoint in context readiness stock-pool" in verifier
    assert 'data.get("decision_status") == "BLOCKED"' in verifier
    assert 'data.get("paper_ready") is False' in verifier
    assert 'item.get("actionability") == "RESEARCH_ONLY"' in verifier
    assert 'item["action_plan"].get("buy_range") is None' in verifier
    assert "verify_trading_v3_production.py" in verifier
    assert "--real-trading-closed-only" in verifier
    assert "assert_nginx_static_matches_checkout" in verifier
    assert 'verify_account_login_api_and_page_smoke "$EXPECTED_SHA"' in verifier
    for gate in (
        "health_contract",
        "governance_contract",
        "v3_deferred_contract",
        "runtime_health",
        "deferred_schema_verify",
        "governance_task_disabled",
        "deferred_writer_fence",
        "real_trading_closed",
        "static_release_identity",
        "account_login",
    ):
        assert f"deferred_runtime_gate_failed gate={gate}" in verifier

    assert "ROLLED_BACK_DEFERRED_SCHEMA_RETAINED" in rollback
    assert "ROLLED_BACK_GOVERNANCE_TASK_DISABLED" in rollback
    assert "CUTOVER_BASE_SCHEMA_STARTED" in rollback
    assert "scheduler_safe_to_start" in rollback
    assert "DEFERRED_RELEASE_WRITER_FENCE_STARTED" in rollback
    assert "fence_deferred_release_writers" in rollback
    assert "systemctl enable probiga-scheduler" in rollback
    assert "systemctl disable probiga-scheduler" in rollback
    assert "--deferred-disable" in rollback
    assert 'point_static_release_to_checkout "$PREVIOUS_CODE_ROOT"' in rollback
    assert "--restore-snapshot" not in rollback
    assert 'if [ "$PREVIOUS_SHA" = "$EXPECTED_SHA" ]; then' in rollback
    assert "preserve_same_sha_scheduler_fence=1" in rollback
    assert "scheduler_safe_to_start=0" in rollback
    assert "assert_deferred_database_runtime || rollback_failed=1" in rollback
    assert "ROLLED_FORWARD_DEFERRED_SCHEDULER_FENCED" in rollback
    preserve = rollback.index("preserve_same_sha_scheduler_fence=1")
    possible_restart = rollback.index("systemctl start probiga-scheduler")
    assert preserve < possible_restart


def test_qmt_release_request_and_quiescence_precede_api_stop() -> None:
    deploy_script = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy_script)
    cutover = normalized.index(
        "CUTOVER_STEP=capture_strategy_governance_task_before_cutover"
    )
    qmt_request = normalized.index(
        "CUTOVER_STEP=request_qmt_windows_edge_before_service_stop", cutover
    )
    scheduler_quiesce = normalized.index(
        "CUTOVER_STEP=stop_linux_scheduler_before_writer_quiescence",
        qmt_request,
    )
    cross_host_proof = normalized.index(
        "CUTOVER_STEP=verify_cross_host_writer_quiescence_before_api_stop",
        scheduler_quiesce,
    )
    cutover_started = normalized.index("CUTOVER_STARTED=1", cross_host_proof)
    writer_fence = normalized.index(
        "CUTOVER_STEP=writer_fence_before_api_stop", cutover_started
    )
    scheduler_stop = normalized.index("CUTOVER_STEP=stop_scheduler", writer_fence)
    api_stop = normalized.index("CUTOVER_STEP=stop_api", scheduler_stop)

    request_window = normalized[qmt_request:scheduler_quiesce]
    assert "run_qmt_windows_edge_release_bootstrap.py" in request_window
    assert '--request --expected-build-sha "$EXPECTED_SHA" --compact' in (
        request_window
    )
    assert 'p.get("database_writes") is True' in request_window
    proof_window = normalized[cross_host_proof:cutover_started]
    assert "trading_v3_layer4_maintenance.py" in proof_window
    assert "wait-writers --timeout-seconds 120 --poll-seconds 5" in proof_window
    assert 'p.get("live_writer_count")==0' in proof_window
    stop_window = normalized[scheduler_stop:api_stop]
    api_stop_window = normalized[api_stop:]
    assert "sudo systemctl stop probiga-scheduler" in stop_window
    assert 'sudo systemctl stop "$MAIN_SERVICE"' in api_stop_window
    assert (
        qmt_request
        < scheduler_quiesce
        < cross_host_proof
        < cutover_started
        < writer_fence
        < scheduler_stop
        < api_stop
    )
