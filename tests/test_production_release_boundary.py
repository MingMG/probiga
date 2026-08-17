from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import validate_production_release_boundary as boundary


ROOT = Path(__file__).resolve().parents[1]


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
    assert 'LEGACY_STATE_DIR="$RECEIPT_DIR/legacy-state-$RECEIPT_ID"' in deploy_script
    assert "preserve_tracked_worktree_state" in deploy_script
    assert "quarantine_unsafe_untracked_release_files" in deploy_script
    assert 'LEGACY_STATE_DIR/untracked-release-files.manifest' in deploy_script
    assert 'LEGACY_STATE_DIR/untracked-release-files/$relative_path' in deploy_script
    assert "sha256sum -- \"$source_path\"" in deploy_script
    assert "mv -- \"$source_path\" \"$target_path\"" in deploy_script
    assert "server biz integrations tools scripts strategies versions" in deploy_script
    assert "git ls-files --others --exclude-standard -z" in deploy_script
    assert "artifacts/trading_v4 artifacts/trading_v5 artifacts/trading_v6" in deploy_script
    assert "-mindepth 2 -maxdepth 2" in deploy_script
    assert "-name '*.pyc' -o -name '*.pyo'" in deploy_script
    assert "seal_release_checkout" in deploy_script
    assert "git ls-files --stage -z" in deploy_script
    assert 'git checkout --detach --force "$EXPECTED_SHA"' in deploy_script
    assert "git clean" not in deploy_script
    assert "probiga-production-deploy" in workflow
    assert ".probiga_deploy_lock" in workflow
    assert "resolved_requirements_sha256" in workflow
    assert ".release_venvs" in workflow
    assert 'chown root:root "$RELEASE_VENV_ROOT"' in deploy_script
    assert 'chmod 0555 "$RELEASE_VENV_ROOT"' in deploy_script
    assert 'sudo -u "$SERVICE_USER" test -x "$RELEASE_VENV_ROOT"' in deploy_script
    assert "RELEASE_VENV_RETENTION=2" in deploy_script
    assert "LEGACY_RELEASE_RETENTION=0" in deploy_script
    assert 'prune_release_venvs "$PREVIOUS_RELEASE_REVISION"' in deploy_script
    assert 'prune_release_venvs "$EXPECTED_SHA"' in deploy_script
    assert "prune_legacy_release_copies" in deploy_script
    assert "prune_release_temp_files" in deploy_script
    assert 'test "$(dirname -- "$build_real")" = "$RELEASE_VENV_ROOT"' in deploy_script
    assert 'test "$(dirname -- "$entry_real")" = "$LEGACY_RELEASE_ROOT"' in deploy_script
    assert 'path_is_runtime_referenced "$build_real"' in deploy_script
    assert 'path_is_opt_link_target "$entry_real"' in deploy_script
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
    assert 'fetch --no-tags "$TRUSTED_REMOTE" refs/heads/main' in root_broker
    assert 'REMOTE_SHA="$(GIT_SSH_COMMAND="$REMOTE_GIT_SSH"' in root_broker
    assert 'git checkout --detach --force "$EXPECTED_SHA"' in deploy_script
    assert "trap 'rollback 143' TERM" in workflow
    assert workflow.count("--retry-all-errors") == 2
    rollback = workflow.index("rollback() {")
    stop_service = workflow.index("sudo systemctl stop probiga", rollback)
    checkout_previous = workflow.index(
        'git checkout --detach "$PREVIOUS_SHA"', rollback
    )
    assert stop_service < checkout_previous
    assert 'if [ "$rollback_failed" -ne 0 ]; then' in workflow
    assert 'write_receipt "ROLLBACK_FAILED"' in workflow
    assert 'write_receipt "ROLLED_BACK"' in workflow


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
    assert 'git clone --mirror "$LEGACY_ADATA_REPOSITORY"' in workflow
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
    assert ".git .github deploy server biz integrations tools scripts" in workflow
    assert "find . -maxdepth 1" in workflow
    assert "scripts strategies versions" in workflow
    assert "reused release virtual environment" in workflow
    assert "new release virtual environment" in workflow
    assert 'chmod -R a+rX,a-w "$EXPECTED_BUILD"' in deploy_script
    assert 'sudo -u "$SERVICE_USER" test -x "$EXPECTED_BUILD/bin/python"' in deploy_script
    assert 'sudo chmod -R a+rX,a-w "$ADATA_SOURCE"' in deploy_script
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
    restart_service = deploy_script.index(
        "sudo systemctl restart probiga", validate_scheduler_manifest
    )
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

    assert (
        "ExecStart=/usr/bin/env API_EMBEDDED_SCHEDULER_ENABLED=false "
        in deploy_script
    )
    assert "systemctl show probiga --property=ExecStart --value" in deploy_script
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
    assert "release_identity_check 1" in deploy_script
    assert "release_identity_check 0 >&2 || true" in deploy_script
    assert "PROBIGA_RELEASE_IDENTITY_REQUIRE_CLEAN" in deploy_script
    final_seal = deploy_script.rindex("\nseal_release_checkout\n")
    final_identity_check = deploy_script.index("\nrelease_identity_check 1\n")
    assert final_seal < final_identity_check
    assert "reseal previous Git checkout" in deploy_script
    assert "GIT_OPTIONAL_LOCKS=0 PYTHONDONTWRITEBYTECODE=1" in deploy_script
    assert "probiga-ai-recommendation-worker.service" in deploy_script
    assert "probiga-ai-recommendation-worker.timer" in deploy_script
    assert (
        "probiga-ai-recommendation-worker.service.d/release-runtime.conf"
        in deploy_script
    )
    assert 'User=$SERVICE_USER' in deploy_script
    assert 'Group=$SERVICE_USER' in deploy_script
    assert (
        "$RELEASE_VENV_ROOT/$revision/bin/python "
        "tools/run_ai_recommendation_worker.py --once"
        in deploy_script
    )
    assert 'sudo systemctl stop "$AI_WORKER_TIMER"' in deploy_script
    assert 'sudo systemctl stop "$AI_WORKER_SERVICE"' in deploy_script
    assert "pin previous AI recommendation worker runtime" in deploy_script
    assert 'assert_ai_worker_runtime "$EXPECTED_SHA"' in deploy_script
    assert "STATIC_RELEASE_LINK=/opt/ProBigA-current" in deploy_script
    assert 'sudo mv -Tf "$link_build/current" "$STATIC_RELEASE_LINK"' in deploy_script
    assert 'test "$(readlink -f "$STATIC_RELEASE_LINK")" = "$REPOSITORY_ROOT"' in deploy_script
    assert 'cmp --silent "$REPOSITORY_ROOT/server/static/$asset" "$response"' in deploy_script
    assert "point Nginx static assets at previous checkout" in deploy_script
    assert "verify previous Nginx static assets" in deploy_script
    worker_stop = deploy_script.index(
        'sudo systemctl stop "$AI_WORKER_TIMER"',
        deploy_script.index("trap 'rollback $?'"),
    )
    release_checkout = deploy_script.index('git checkout --detach --force "$EXPECTED_SHA"')
    worker_pin = deploy_script.index('write_ai_worker_dropin "$EXPECTED_SHA"')
    service_restart = deploy_script.index("sudo systemctl restart probiga")
    static_switch = deploy_script.index(
        "point_static_release_to_checkout",
        deploy_script.index('cat "$HEALTH_RESPONSE"'),
    )
    worker_restore = deploy_script.index(
        'sudo systemctl start "$AI_WORKER_TIMER"',
        deploy_script.index('write_ai_worker_dropin "$EXPECTED_SHA"'),
    )
    assert (
        worker_stop
        < release_checkout
        < worker_pin
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
