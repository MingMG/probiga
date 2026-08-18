from __future__ import annotations

import re
import shutil
import subprocess
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import validate_production_release_boundary as boundary


ROOT = Path(__file__).resolve().parents[1]


def _normalized_shell(source: str) -> str:
    """Join shell continuations without changing command ordering."""

    return re.sub(r"[ \t]*\\\r?\n[ \t]*", " ", source)


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

    assert len(workflow_source) < 21_000
    assert "deploy/production_deploy.sh" in workflow_source
    assert "envs: EXPECTED_SHA,RESOLVED_REQUIREMENTS_B64" in workflow_source
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
    assert 'git config --system --add safe.directory "$REPOSITORY_ROOT"' in deploy_script
    assert 'git config --system --add safe.directory "$LEGACY_ADATA_REPOSITORY"' in deploy_script
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
    assert "resolved_requirements_sha256" in workflow
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
    assert "Warning: release venv cleanup failed after activation" in deploy_script
    assert (
        "Warning: immutable code release cleanup failed after activation"
        in deploy_script
    )
    assert "Warning: release temp cleanup failed after activation" in deploy_script
    assert "prune_code_releases" in deploy_script
    assert "prune_release_temp_files" in deploy_script
    assert 'test "$(dirname -- "$build_real")" = "$RELEASE_VENV_ROOT"' in deploy_script
    assert 'path_is_runtime_referenced "$build_real"' in deploy_script
    assert "command_timeout: 25m" in workflow
    assert "probiga.deploy-receipt.v3" in workflow
    assert '"expected_requirements_sha256":"%s"' in workflow
    assert '"previous_requirements_sha256":"%s"' in workflow
    assert '"active_requirements_sha256":"%s"' in workflow
    assert '"requirements_sha256":"%s"' not in workflow
    assert 'sudo chmod 0700 "$RECEIPT_DIR"' in workflow
    assert 'sudo mktemp' in workflow
    assert 'sudo tee "$receipt_tmp"' in workflow
    assert 'sudo chmod 0600 "$receipt_tmp"' in workflow
    assert 'sudo mv -f "$receipt_tmp" "$RECEIPT_DIR/$RECEIPT_ID.json"' in workflow
    assert 'sudo tee "$RECEIPT_DIR/$RECEIPT_ID.json"' not in workflow
    assert 'git ls-remote "$TRUSTED_REMOTE" refs/heads/main' in root_broker
    assert (
        '"${GIT[@]}" fetch --no-tags origin '
        '"+refs/heads/main:refs/remotes/origin/main"'
        in _normalized_shell(root_broker)
    )
    assert 'REMOTE_SHA="$(GIT_SSH_COMMAND="$REMOTE_GIT_SSH"' in root_broker
    assert (
        'git --git-dir="$CODE_GIT_CACHE" worktree add --detach'
        in _normalized_shell(deploy_script)
    )
    assert "trap 'rollback 143' TERM" in workflow
    assert workflow.count("--retry-all-errors") >= 2
    assert 'if [ "$rollback_failed" -ne 0 ]; then' in workflow
    assert 'write_receipt "ROLLBACK_FAILED"' in workflow
    assert 'write_receipt "ROLLED_BACK"' in workflow


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

    required_prepare_commands = (
        'git --git-dir="$CODE_GIT_CACHE" worktree add',
        '-m venv "$EXPECTED_BUILD"',
        '"$EXPECTED_BUILD/bin/python" -m pip install -r ',
        '"$EXPECTED_BUILD/bin/python" -m pip wheel --no-deps ',
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
    cutover_fence = deploy_script.index("CUTOVER_STARTED=1", prepare_call)
    first_cutover_stop = _required_shell_position(
        deploy_script[cutover_fence:], r"sudo systemctl stop\b"
    ) + cutover_fence
    assert prepare_call < cutover_fence < first_cutover_stop

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
    assert "systemctl stop" not in prepare_failure_path
    assert 'write_receipt "PREPARATION_FAILED"' in prepare_failure_path
    assert 'exit "$failed_status"' in prepare_failure_path

    assert 'DEPLOY_MAIN_BASHPID="$BASHPID"' in deploy_script
    child_guard = deploy_script[rollback_start:rollback_cutover]
    assert 'if [ "$BASHPID" != "$DEPLOY_MAIN_BASHPID" ]; then' in child_guard
    assert "trap - ERR TERM INT" in child_guard
    assert 'exit "$failed_status"' in child_guard
    assert "systemctl stop" not in child_guard

    assert normalized.count("trap 'rollback $?' ERR") == 1
    assert "trap 'rollback 143' TERM" in normalized
    assert "trap 'rollback 130' INT" in normalized
    assert not re.search(
        r"(?m)^trap\s+[^\n]*rollback[^\n]*\sEXIT\s*$", deploy_script
    )


def test_main_service_downtime_only_activates_prepared_dropins() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(deploy_script)
    cutover = normalized.index("trap 'rollback $?' ERR")
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

    writer_fence_start = downtime.index("WRITER_FENCE_STATUS=0")
    dropin_install = downtime.index(
        "install_prepared_dropins", writer_fence_start
    )
    writer_fence = downtime[writer_fence_start:dropin_install]
    expected_writer_fence_command = (
        'sudo -u "$SERVICE_USER" env GIT_OPTIONAL_LOCKS=0 '
        "PYTHONDONTWRITEBYTECODE=1 PYTHONSAFEPATH=1 "
        "PROBIGA_DEPLOYMENT_MODE=production "
        'PROBIGA_BUILD_COMMIT_SHA="$EXPECTED_SHA" '
        '"PYTHONPATH=$ADATA_SOURCE:$PREPARED_CODE_ROOT" '
        '"$RELEASE_VENV_ROOT/$EXPECTED_SHA/bin/python" -P '
        "tools/add_trading_v3_tasks.py --writer-fence "
        "--require-no-live-scheduler-writers "
        "--writer-drain-timeout-seconds 150 "
        "--writer-drain-poll-seconds 5"
    )
    assert writer_fence.count(expected_writer_fence_command) == 1
    python_cutover_commands = [
        line.strip()
        for line in downtime_closure.splitlines()
        if "python" in line and "grep -F --" not in line
    ]
    assert python_cutover_commands == [expected_writer_fence_command]

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
    fence_position = normalized.index("WRITER_FENCE_STATUS=0", api_stop)
    dropin_position = normalized.index(
        "install_prepared_dropins", fence_position
    )
    daemon_reload = normalized.index("systemctl daemon-reload", dropin_position)
    assert (
        cutover
        < scheduler_stop
        < api_stop
        < fence_position
        < dropin_position
        < daemon_reload
        < service_start
    )

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
    premarket_probe = normalized.index(
        '"$PREPARED_CODE_ROOT/tools/ensure_quality_gate.py"', static_switch
    )
    premarket_task = normalized.index(
        "--task-type analysis_premarket_external", premarket_probe
    )
    code_cleanup = normalized.index(
        'prune_code_releases "$PREPARED_CODE_ROOT" "$PREVIOUS_CODE_ROOT"',
        premarket_task,
    )
    deployed_receipt = normalized.index(
        'write_receipt "DEPLOYED" "$EXPECTED_SHA"', code_cleanup
    )
    assert service_start < health < static_switch < premarket_probe
    assert premarket_probe < premarket_task < code_cleanup < deployed_receipt


def test_rollback_restores_previous_immutable_runtime_without_checkout() -> None:
    deploy_script = (ROOT / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )
    rollback = deploy_script[
        deploy_script.index("rollback() {"):
        deploy_script.index("trap 'rollback $?' ERR")
    ]
    normalized_rollback = _normalized_shell(rollback)

    assert "git checkout" not in rollback
    assert "seal_release_checkout" not in rollback
    assert "$PREVIOUS_CODE_ROOT" in rollback
    assert "$PREVIOUS_VENV" in rollback
    assert re.search(
        r'sudo install\s+-o root -g root -m 0644\s+'
        r'"\$PREVIOUS_DROPIN"\s+'
        r'/etc/systemd/system/probiga\.service\.d/scheduler\.conf',
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
        normalized[normalized.index("trap 'rollback $?' ERR"):],
        r'sudo systemctl start (?:"\$MAIN_SERVICE"|probiga)(?:[ \t]|$)',
    ) + normalized.index("trap 'rollback $?' ERR")
    static_switch = normalized.index(
        'point_static_release_to_checkout "$PREPARED_CODE_ROOT"',
        service_start,
    )
    receipt = normalized.index(
        'write_receipt "DEPLOYED" "$EXPECTED_SHA"', prune_position
    )
    assert service_start < static_switch < prune_position < receipt


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


def test_code_retention_guards_survive_if_not_conditional_context() -> None:
    bash = shutil.which("bash")
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
    assert (
        "ADATA_RELEASE_SHA: b14f4e57b2175302f18b6eaf934f7dff9207a141"
        in workflow
    )
    assert "https://github.com/1nchaos/adata.git" in workflow
    assert "ADATA_GIT_CACHE=/var/lib/probiga/release-sources/adata.git" in workflow
    assert (
        "LEGACY_ADATA_GIT_CACHE=/opt/ProBigA/.release_sources/adata.git"
        in workflow
    )
    assert 'git clone --mirror --no-hardlinks "$adata_seed"' in workflow
    assert "http.lowSpeedTime=30" in workflow
    assert "EXPECTED_ADATA_TREE_SHA256" in workflow
    assert "server.common.adata_release seal" in workflow
    assert "pip wheel --no-deps" in workflow
    assert "PROBIGA_EXPECTED_ADATA_SHA" in workflow
    assert "PROBIGA_EXPECTED_ADATA_TREE_SHA256" in workflow
    assert "PROBIGA_ADATA_SOURCE_DIR" in workflow
    assert "probiga.deploy-receipt.v3" in workflow
    assert "PROBIGA_ADMIN_AUTH_ENABLED=true" in workflow
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
    rollback_trap = workflow.index("trap 'rollback $?' ERR")
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
    assert "--task-type analysis_premarket_external" in deploy_script
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
    validate_scheduler_manifest = deploy_script.index("tools/ensure_quality_gate.py")
    restart_service = _required_shell_position(
        deploy_script[validate_scheduler_manifest:],
        r'sudo systemctl (?:start|restart) '
        r'(?:"\$MAIN_SERVICE"|probiga)(?:[ \t]|$)',
    ) + validate_scheduler_manifest
    assert validate_boundary - validate_boundary_env < 160
    assert validate_boundary < validate_scheduler_manifest < restart_service
    assert 'sudo -u "$SERVICE_USER" env GIT_OPTIONAL_LOCKS=0' in workflow
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
    assert "WorkingDirectory=/opt/ProBigA" in scheduler_dropin
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
    assert (
        "ExecStart=/usr/bin/env API_EMBEDDED_SCHEDULER_ENABLED=false "
        in main_dropin
    )
    assert (
        "ExecStart=/usr/bin/env API_EMBEDDED_SCHEDULER_ENABLED=false "
        in scheduler_dropin
    )
    assert "PROBIGA_CODE_ROOT=$code_root" in main_dropin
    assert "PROBIGA_BUILD_COMMIT_SHA=$revision" in main_dropin
    assert "PYTHONPATH=$adata_source:$code_root" in main_dropin
    assert "PYTHONSAFEPATH=1" in main_dropin
    assert "$RELEASE_VENV_ROOT/$revision/bin/python -P -m uvicorn" in main_dropin
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
        in deploy_script
    )
    assert (
        "grep -F -- 'API_EMBEDDED_SCHEDULER_ENABLED=false'"
        in deploy_script
    )
    assert "grep -zFx -- 'API_EMBEDDED_SCHEDULER_ENABLED=false'" in deploy_script
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
        normalized.index("trap 'rollback $?'"),
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
        'sudo systemctl start "$AI_WORKER_TIMER"',
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
    assert "artifacts/trading_v5/regime_expert_capacity_oos_20260802.json -text" in attributes
    assert "artifacts/trading_v6/multi_sleeve_pit_finance_oos_20260802.json -text" in attributes
    assert "*.sh text eol=lf" in attributes


def test_production_dependency_lock_respects_server_mirror_ceiling() -> None:
    requirements = (ROOT / "requirements-platform.txt").read_text(
        encoding="utf-8"
    )

    assert "charset-normalizer==3.5.0" in requirements


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
