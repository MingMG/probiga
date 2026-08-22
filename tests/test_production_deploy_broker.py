import hashlib
import json
import re
import os
import shutil
import subprocess
import textwrap
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _bash() -> str | None:
    discovered = shutil.which("bash")
    if discovered:
        return discovered
    git_bash = Path(r"C:\Program Files\Git\bin\bash.exe")
    return str(git_bash) if git_bash.is_file() else None


def _normalized_shell(source: str) -> str:
    """Join shell continuations so command-level assertions stay readable."""

    return re.sub(r"[ \t]*\\\r?\n[ \t]*", " ", source)


def _shell_function_body(source: str, name: str) -> str:
    match = re.search(
        rf"(?ms)^{re.escape(name)}\(\) \{{\s*$\n(.*?)^\}}\s*$",
        source,
    )
    assert match is not None
    return match.group(1)


def test_workflow_uses_fixed_root_owned_deploy_broker() -> None:
    workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
        encoding="utf-8"
    )

    assert "sudo -n /usr/local/sbin/probiga-production-deploy" in workflow
    assert "bash \"$DEPLOY_BOOTSTRAP\"" not in workflow


def test_old_broker_capabilities_fail_before_the_deploy_command() -> None:
    bash = _bash()
    if bash is None:
        return
    workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
        encoding="utf-8"
    )
    blocks = re.findall(
        r"EXPECTED_CAPABILITIES=\"\$\(cat <<'EOF'\r?\n"
        r"(.*?)\r?\n\s*EOF\r?\n\s*\)\"",
        workflow,
        re.DOTALL,
    )
    assert len(blocks) == 2
    expected = textwrap.dedent(blocks[0]).strip()
    assert textwrap.dedent(blocks[1]).strip() == expected
    script = (
        "set -Eeuo pipefail\n"
        "EXPECTED_CAPABILITIES=\"$(cat <<'EOF'\n"
        + expected
        + "\nEOF\n)\"\n"
        "test \"$ACTUAL_CAPABILITIES\" = \"$EXPECTED_CAPABILITIES\"\n"
        "printf 'DEPLOY_COMMAND_REACHED\\n'\n"
    )
    old_v3 = expected.replace(
        "deploy_protocol=probiga-production-deploy-v4",
        "deploy_protocol=probiga-production-deploy-v3",
    )
    rejected = subprocess.run(
        [bash, "-c", script],
        env={**os.environ, "ACTUAL_CAPABILITIES": old_v3},
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert rejected.returncode != 0
    assert "DEPLOY_COMMAND_REACHED" not in rejected.stdout
    legacy_v4 = "\n".join(
        line
        for line in expected.splitlines()
        if not line.startswith((
            "governance_task_snapshot=",
            "receipt_pending_recovery=",
            "activation_release_identity=",
            "release_tree_and_adapter_seal=",
        ))
    )
    rejected_legacy_v4 = subprocess.run(
        [bash, "-c", script],
        env={**os.environ, "ACTUAL_CAPABILITIES": legacy_v4},
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert rejected_legacy_v4.returncode != 0
    assert "DEPLOY_COMMAND_REACHED" not in rejected_legacy_v4.stdout
    accepted = subprocess.run(
        [bash, "-c", script],
        env={**os.environ, "ACTUAL_CAPABILITIES": expected},
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert accepted.returncode == 0, accepted.stderr
    assert accepted.stdout.strip() == "DEPLOY_COMMAND_REACHED"

    remote = workflow[workflow.index("script: |", workflow.index("deploy:")):]
    capability_probe = remote.index("--capabilities")
    deploy_command = remote.index(
        'sudo -n /usr/local/sbin/probiga-production-deploy "$EXPECTED_SHA"'
    )
    assert capability_probe < deploy_command


def test_blocked_wheel_lock_exits_before_any_remote_deploy(tmp_path: Path) -> None:
    bash = _bash()
    if bash is None:
        return
    marker = (tmp_path / "remote-command-reached").as_posix()
    script = f"""
set -Eeuo pipefail
mapfile -t release_manifest < deploy/production_release.env
test "${{#release_manifest[@]}}" -eq 10
test "${{release_manifest[2]}}" = PROBIGA_PRODUCTION_LOCK_STATUS=READY
printf reached > {marker!r}
"""
    completed = subprocess.run(
        [bash, "-c", script],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert completed.returncode != 0
    assert not (tmp_path / "remote-command-reached").exists()
    workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
        encoding="utf-8"
    )
    gate = workflow.index(
        "Fail closed unless the complete production artifact lock is ready"
    )
    remote = workflow.index("Deploy to Alibaba Cloud ECS", gate)
    assert gate < remote


def test_clean_git_ignores_hostile_environment_and_replace_refs(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        return
    broker = (ROOT / "deploy" / "production_deploy_root.sh").read_text(
        encoding="utf-8"
    )
    body = _shell_function_body(broker, "clean_git").replace(
        "/usr/bin/git", '"$TEST_GIT"'
    )
    repository = (tmp_path / "repository").as_posix()
    harness = f"""
set -Eeuo pipefail
TEST_GIT="$(command -v git)"
clean_git() {{
{body}
}}
repo={repository!r}
"$TEST_GIT" init -q "$repo"
"$TEST_GIT" -C "$repo" config user.email release-test@example.invalid
"$TEST_GIT" -C "$repo" config user.name release-test
printf original > "$repo/content"
"$TEST_GIT" -C "$repo" add content
"$TEST_GIT" -C "$repo" commit -qm original
original="$("$TEST_GIT" -C "$repo" rev-parse HEAD)"
printf replacement > "$repo/content"
"$TEST_GIT" -C "$repo" commit -qam replacement
replacement="$("$TEST_GIT" -C "$repo" rev-parse HEAD)"
"$TEST_GIT" -C "$repo" replace "$original" "$replacement"
printf '[core]\n\thooksPath = hostile-hooks\n' > "$repo/hostile.gitconfig"
export BASH_ENV="$repo/hostile-bash-env"
export GIT_CONFIG_GLOBAL="$repo/hostile.gitconfig"
export GIT_CONFIG_SYSTEM="$repo/hostile.gitconfig"
export GIT_ALTERNATE_OBJECT_DIRECTORIES="$repo/missing-objects"
export GIT_EXTERNAL_DIFF="$repo/missing-diff"
test "$(clean_git -C "$repo" show -s --format=%s "$original")" = original
if clean_git config --global --get core.hooksPath >/dev/null 2>&1; then
  echo 'clean_git inherited hostile global config' >&2
  exit 20
fi
clean_git --version >/dev/null
"""
    completed = subprocess.run(
        [bash, "-c", harness],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_manual_broker_installer_probes_candidate_before_atomic_replace() -> None:
    installer = _normalized_shell(
        (ROOT / "deploy" / "install_production_deploy_broker.sh").read_text(
            encoding="utf-8"
        )
    )
    staged_probe = installer.index("STAGED_CAPABILITIES=")
    staged_exact = installer.index(
        'test "$STAGED_CAPABILITIES" = "$EXPECTED_CAPABILITIES"',
        staged_probe,
    )
    atomic_replace = installer.index('mv -fT "$TARGET_TMP" "$TARGET"')
    installed_probe = installer.index("CAPABILITIES=", atomic_replace)
    assert staged_probe < staged_exact < atomic_replace < installed_probe


def test_broker_restricts_caller_remote_and_revision() -> None:
    broker = (ROOT / "deploy" / "production_deploy_root.sh").read_text(
        encoding="utf-8"
    )

    assert "SUDO_USER" in broker
    assert "probiga-deploy" in broker
    assert "git@github.com:MingMG/probiga.git" in broker
    assert "/etc/probiga/github-readonly-ed25519" in broker
    assert "/etc/probiga/github_known_hosts" in broker
    assert 'GIT_SSH_COMMAND="$REMOTE_GIT_SSH"' in broker
    assert "refs/heads/main" in broker
    assert "requested revision is not the current trusted main revision" in broker
    assert "deploy/production_requirements.lock" in broker
    assert "root-derived input lock digest is invalid" in broker
    assert "deploy/production_wheel_manifest.lock" in broker
    assert "EXPECTED_ADATA_TREE_SHA256" in broker
    assert "deploy/production_release.env" in broker


def test_blocked_broker_fails_before_any_lock_cache_or_network_side_effect() -> None:
    broker = (ROOT / "deploy" / "production_deploy_root.sh").read_text(
        encoding="utf-8"
    )
    release = (ROOT / "deploy" / "production_release.env").read_text(
        encoding="utf-8"
    )
    status = "BLOCKED_CROSS_PLATFORM_REGEN_REQUIRED"
    assert f"readonly BROKER_COMPILED_LOCK_STATUS={status}" in broker
    assert f"PROBIGA_PRODUCTION_LOCK_STATUS={status}" in release
    blocker = broker.index(
        '[ "$BROKER_COMPILED_LOCK_STATUS" != READY ]'
    )
    for side_effect in (
        'install -d -o root -g root -m 0700 "$BROKER_LOCK_ROOT"',
        'touch "$BROKER_LOCK_FILE"',
        'clean_git_ssh ls-remote',
        'clean_git_ssh --git-dir="$CODE_GIT_CACHE" fetch',
    ):
        assert blocker < broker.index(side_effect)


def test_broker_binds_release_tree_registry_seal_and_snapshot_identity() -> None:
    broker = (ROOT / "deploy" / "production_deploy_root.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(broker)
    assert 'test "${#RELEASE_MANIFEST_LINES[@]}" -eq 10' in broker
    assert "ADAPTER_REGISTRY_SEAL_SHA256=*" in broker
    assert "${EXPECTED_SHA}^{tree}" in broker
    assert '{"kind":"git-tree","tree":"%s"}' in broker
    assert 'EXPECTED_RELEASE_TREE_SHA256="$EXPECTED_RELEASE_TREE_SHA256"' in broker
    assert (
        'EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256='
        '"$EXPECTED_ADAPTER_REGISTRY_SEAL_SHA256"' in normalized
    )
    assert "ACTIVATION_RELEASE_IDENTITY_SHA" in broker
    assert "activation release identity digest differs" in broker
    release = (ROOT / "deploy" / "production_release.env").read_text(
        encoding="utf-8"
    )
    declared = next(
        line.split("=", 1)[1]
        for line in release.splitlines()
        if line.startswith("ADAPTER_REGISTRY_SEAL_SHA256=")
    )
    empty_manifest = {
        "schema": "probiga.strategy-adapter-registry-seal.v1",
        "adapters": [],
    }
    assert declared == hashlib.sha256(
        json.dumps(
            empty_manifest,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def test_ci_full_suite_is_bound_to_exact_cp314_linux_wheel_manifest() -> None:
    workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
        encoding="utf-8"
    )
    start = workflow.index("Run the full suite in the exact production wheel set")
    end = workflow.index("\n  deployment-disabled:", start)
    block = workflow[start:end]
    assert "steps.production_lock.outputs.ready == 'true'" in block
    assert "sha256sum deploy/production_requirements.lock" in block
    assert "sha256sum deploy/production_wheel_manifest.lock" in block
    assert "--require-hashes --only-binary=:all: --no-deps" in block
    assert "actual != expected" in block
    assert "--require-hashes --no-index --only-binary=:all:" in block
    assert '"$production_venv/bin/python" -P -m pytest -q' in block


def test_broker_and_engine_use_fd_locks_and_a_versioned_protocol() -> None:
    broker = (ROOT / "deploy" / "production_deploy_root.sh").read_text(
        encoding="utf-8"
    )
    engine = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )

    assert (
        "readonly DEPLOY_PROTOCOL_VERSION=probiga-production-deploy-v4"
        in broker
    )
    assert 'PROBIGA_DEPLOY_PROTOCOL_VERSION="$DEPLOY_PROTOCOL_VERSION"' in broker
    protocol_export = broker.index(
        'PROBIGA_DEPLOY_PROTOCOL_VERSION="$DEPLOY_PROTOCOL_VERSION"'
    )
    assert protocol_export < broker.index(
        '/usr/bin/bash --noprofile --norc "$BOOTSTRAP_FILE"'
    )

    assert "readonly BROKER_LOCK_ROOT=/run/probiga" in broker
    assert (
        'readonly BROKER_LOCK_FILE="$BROKER_LOCK_ROOT/production-broker.lock"'
        in broker
    )
    assert 'exec 8>"$BROKER_LOCK_FILE"' in broker
    assert "flock -n 8" in broker
    assert not re.search(r"\b(?:mkdir|rmdir)\b[^\n]*BROKER_LOCK", broker)

    assert "REQUIRED_DEPLOY_PROTOCOL=probiga-production-deploy-v4" in engine
    protocol_guard = engine.index(
        'if [ "${PROBIGA_DEPLOY_PROTOCOL_VERSION:-}" '
        '!= "$REQUIRED_DEPLOY_PROTOCOL" ]; then'
    )
    assert "production deploy broker protocol mismatch" in engine
    assert protocol_guard < engine.index(": \"${EXPECTED_SHA:")
    assert protocol_guard < engine.index("prepare_release() {")
    assert protocol_guard < engine.index("systemctl stop")

    assert "DEPLOY_LOCK_ROOT=/run/probiga" in engine
    assert 'DEPLOY_LOCK_FILE="$DEPLOY_LOCK_ROOT/production-deploy.lock"' in engine
    assert 'exec 9>"$DEPLOY_LOCK_FILE"' in engine
    assert "flock -n 9" in engine
    assert not re.search(r"\b(?:mkdir|rmdir)\b[^\n]*DEPLOY_LOCK", engine)


def test_broker_fetches_only_into_the_root_owned_bare_code_mirror() -> None:
    broker = (ROOT / "deploy" / "production_deploy_root.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(broker)

    assert "RELEASE_SOURCE_ROOT=/var/lib/probiga/release-sources" in broker
    assert 'CODE_GIT_CACHE="$RELEASE_SOURCE_ROOT/probiga.git"' in broker
    assert re.search(
        r'git init --bare\s+"\$REPOSITORY_BUILD/repository\.git"',
        normalized,
    )
    assert re.search(
        r'mv\s+"\$REPOSITORY_BUILD/repository\.git"\s+'
        r'"\$CODE_GIT_CACHE"',
        normalized,
    )
    assert re.search(
        r'clean_git --git-dir="\$cache"\s+rev-parse\s+'
        r'--is-bare-repository',
        normalized,
    )
    assert 'GIT=(clean_git --git-dir="$CODE_GIT_CACHE")' in broker
    assert re.search(
        r'clean_git_ssh --git-dir="\$CODE_GIT_CACHE"\s+fetch\s+'
        r'--no-tags\s+origin\s+'
        r'"\+refs/heads/main:refs/remotes/origin/main"',
        normalized,
    )
    assert '"${GIT[@]}" cat-file -e "${EXPECTED_SHA}^{commit}"' in normalized
    assert (
        '"${GIT[@]}" show "${EXPECTED_SHA}:deploy/production_deploy.sh"'
        in normalized
    )

    fetch_commands = [
        line for line in normalized.splitlines() if re.search(r"\bfetch\b", line)
    ]
    assert fetch_commands
    assert all("/opt/ProBigA" not in command for command in fetch_commands)
    assert 'readonly REPOSITORY=/opt/ProBigA' not in broker
    assert '-C "$LEGACY_REPOSITORY"' not in normalized
    assert not re.search(
        r'(?:clean_git|\bgit)\s+-C\s+"?\$LEGACY_REPOSITORY"?\s+(?:status|diff)\b',
        normalized,
    )


def test_recovery_uses_only_the_sealed_offline_release_mirror() -> None:
    broker = (ROOT / "deploy" / "production_deploy_root.sh").read_text(
        encoding="utf-8"
    )
    normalized = _normalized_shell(broker)
    fetch = normalized.index(
        'clean_git_ssh --git-dir="$CODE_GIT_CACHE" fetch --no-tags origin'
    )
    fetch_guard = normalized.rfind(
        'if [ "$BROKER_OPERATION" = deploy ]; then', 0, fetch
    )
    fetch_guard_end = normalized.index("fi", fetch)
    assert fetch_guard < fetch < fetch_guard_end
    assert 'sealed offline release mirror is missing for recovery' in broker
    recovery_engine = normalized[
        normalized.rindex("else\n  /usr/bin/env -i"):
    ]
    assert "clean_git_ssh" not in recovery_engine
    assert "ls-remote" not in recovery_engine


def test_broker_uses_clean_git_and_rejects_git_extension_points() -> None:
    broker = (ROOT / "deploy" / "production_deploy_root.sh").read_text(
        encoding="utf-8"
    ).lower()

    assert "/usr/bin/git --no-replace-objects" in broker
    assert "git_config_nosystem=1" in broker
    assert "git_config_global=/dev/null" in broker
    assert "core.hookspath=/dev/null" in broker
    assert "core.fsmonitor=false" in broker
    assert "objects/info/alternates" in broker
    assert "refs/replace" in broker
    assert "local hooks are forbidden" in broker
    assert "diff\\..*\\.command" in broker
    assert "! -user root -o ! -group root -o -perm /022" in broker
