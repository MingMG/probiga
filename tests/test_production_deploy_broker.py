import hashlib
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest


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


def _last_shell_function_body(source: str, name: str) -> str:
    matches = list(
        re.finditer(
            rf"(?ms)^{re.escape(name)}\(\) \{{\s*$\n(.*?)^\}}\s*$",
            source,
        )
    )
    assert matches
    return matches[-1].group(1)


def _function(name: str, body: str) -> str:
    return f"{name}() {{\n{body}\n}}\n"


def _portable_wheelhouse_verifier(source: str) -> str:
    """Keep content checks executable where root Unix ownership is unavailable."""

    return source.replace(
        'test "$(stat -c \'%U:%G\' "$wheelhouse")" = root:root || return 1',
        "true",
    ).replace(
        'test "$(stat -c \'%a\' "$wheelhouse")" = 555 || return 1',
        "true",
    ).replace(
        " -o ! -user root -o ! -group root -o -perm /222", ""
    ).replace(
        'sudo -u "$SERVICE_USER" test ! -w "$wheelhouse" || return 1',
        "true",
    ).replace(
        'sudo -u "$BUILD_USER" test ! -w "$wheelhouse" || return 1',
        "true",
    )


def test_installer_pins_root_owned_broker_and_legacy_upload_is_retired() -> None:
    installer = (ROOT / "deploy" / "install_production_deploy_broker.sh").read_text(
        encoding="utf-8"
    )
    legacy = (ROOT / "deploy" / "deploy.ps1").read_text(encoding="utf-8")

    assert not (ROOT / ".github" / "workflows" / "deploy.yml").exists()
    assert "TARGET=/usr/local/sbin/probiga-production-deploy" in installer
    assert 'install -o root -g root -m 0755 "$SOURCE" "$TARGET_TMP"' in installer
    assert 'test "$(sha256sum "$TARGET" | cut -d\' \' -f1)" = "$EXPECTED_SHA256"' in installer
    assert "Legacy mutable-file production upload is retired" in legacy
    assert "throw" in legacy


def test_root_broker_uses_the_v4_single_sha_deploy_contract() -> None:
    broker = (ROOT / "deploy" / "production_deploy_root.sh").read_text(
        encoding="utf-8"
    )
    parse = _normalized_shell(_shell_function_body(broker, "parse_broker_invocation"))

    assert "case \"$#\" in" in parse
    assert "BROKER_OPERATION=deploy" in parse
    assert 'EXPECTED_SHA="$1"' in parse
    assert '[[ "$EXPECTED_SHA" =~ ^[0-9a-f]{40}$ ]]' in parse
    assert "$3" not in parse
    assert 'REMOTE_SHA="$(clean_git_ssh ls-remote' in broker
    assert 'test "$REMOTE_SHA" = "$EXPECTED_SHA"' in broker
    assert 'show "${EXPECTED_SHA}:deploy/production_deploy.sh"' in broker


def test_v4_deploy_uses_a_ready_cp314_manylinux_2_28_static_lock() -> None:
    broker = (ROOT / "deploy" / "production_deploy_root.sh").read_text(
        encoding="utf-8"
    )
    release = (ROOT / "deploy" / "production_release.env").read_text(
        encoding="utf-8"
    )
    requirements_input_path = ROOT / "deploy" / "production_requirements.in"
    platform_input_path = ROOT / "requirements-platform.txt"
    requirements_path = ROOT / "deploy" / "production_requirements.lock"
    requirements = requirements_path.read_text(encoding="utf-8")
    wheel_manifest_path = ROOT / "deploy" / "production_wheel_manifest.lock"
    wheel_manifest = wheel_manifest_path.read_text(encoding="utf-8")

    assert 'show "${EXPECTED_SHA}:deploy/production_requirements.lock"' in broker
    assert 'show "${EXPECTED_SHA}:deploy/production_release.env"' in broker
    assert 'show "${EXPECTED_SHA}:deploy/production_wheel_manifest.lock"' in broker
    assert "PROBIGA_PRODUCTION_LOCK_STATUS=READY" in release
    assert (
        "PROBIGA_PRODUCTION_LOCK_TARGET=cp314-manylinux_2_28_x86_64"
        in release
    )
    assert "STATUS=READY" in requirements
    assert "TARGET=cp314-manylinux_2_28_x86_64" in requirements
    assert "STATUS=READY" in wheel_manifest
    assert "TARGET=cp314-manylinux_2_28_x86_64" in wheel_manifest
    assert "BLOCKED_CROSS_PLATFORM_REGEN_REQUIRED" not in (
        release + requirements + wheel_manifest
    )

    release_fields = dict(
        line.split("=", 1)
        for line in release.splitlines()
        if line and not line.startswith("#") and "=" in line
    )
    assert release_fields["INPUT_LOCK_SHA256"] == hashlib.sha256(
        requirements_path.read_bytes()
    ).hexdigest()
    assert release_fields["WHEEL_MANIFEST_SHA256"] == hashlib.sha256(
        wheel_manifest_path.read_bytes()
    ).hexdigest()
    assert (
        "# REQUIREMENTS_INPUT_SHA256="
        + hashlib.sha256(requirements_input_path.read_bytes()).hexdigest()
        in requirements
    )
    assert "# PLATFORM_SOURCE=requirements-platform.txt" in requirements
    assert (
        "# PLATFORM_REQUIREMENTS_SHA256="
        + hashlib.sha256(platform_input_path.read_bytes()).hexdigest()
        in requirements
    )


def test_engine_installs_and_checks_the_exact_hashed_production_lock() -> None:
    engine = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    prepare = _normalized_shell(
        _shell_function_body(engine, "prepare_release_venv")
    )
    verify = _normalized_shell(
        _shell_function_body(engine, "verify_venv_dependency_lock")
    )

    assert 'validate_hashed_requirements_lock "$RESOLVED_LOCK"' in prepare
    assert "--require-hashes --no-index --only-binary=:all:" in prepare
    assert '--find-links "$TRUSTED_WHEELHOUSE" -r "$RESOLVED_LOCK"' in prepare
    assert '"$venv_path/bin/python" -I -m pip check' in verify
    assert "pip install --upgrade" not in prepare
    assert "pip install -r requirements-platform.txt" not in prepare


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
    normalized = _normalized_shell(broker)
    for trusted_path in (
        "deploy/production_release.env",
        "deploy/production_requirements.lock",
        "deploy/production_wheel_manifest.lock",
    ):
        assert f'"${{EXPECTED_SHA}}:{trusted_path}"' in normalized
    assert "root-derived input lock digest is invalid" in broker
    assert "trusted input lock digest differs from release manifest" in broker
    assert "trusted wheel manifest digest differs from release manifest" in broker


def test_ready_broker_checks_its_compiled_lock_status_before_side_effects() -> None:
    broker = (ROOT / "deploy" / "production_deploy_root.sh").read_text(
        encoding="utf-8"
    )
    release = (ROOT / "deploy" / "production_release.env").read_text(
        encoding="utf-8"
    )
    status = "READY"
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


def test_broker_validates_and_routes_no_receipt_forward_recovery_phases() -> None:
    broker = (ROOT / "deploy" / "production_deploy_root.sh").read_text(
        encoding="utf-8"
    )
    snapshot = _shell_function_body(broker, "activation_snapshot_release")
    assert "restoring-new-no-receipt" in snapshot
    assert "new-runtime-preserved-no-receipt" in snapshot
    phase_enum = snapshot.index("restoring-new-no-receipt")
    no_receipt_start = snapshot.index("restoring-new-no-receipt", phase_enum + 1)
    no_receipt = snapshot[
        no_receipt_start : snapshot.index(
            "new-runtime-verified|finalized", no_receipt_start
        )
    ]
    assert "ACTIVATION_GOVERNANCE_NEW_SNAPSHOT" in no_receipt
    assert "ACTIVATION_GOVERNANCE_NEW_SHA" in no_receipt
    assert "ACTIVATION_RECEIPT_PENDING" in no_receipt
    assert "ACTIVATION_RECEIPT_PENDING_SHA" in no_receipt
    assert "test ! -e" in no_receipt
    assert "test ! -L" in no_receipt

    recovery = broker[broker.index('if [ "$BROKER_OPERATION" = recover-database-guard ]'):]
    snapshot_only_start = recovery.index(
        'SNAPSHOT_RECOVERY_PHASE="$(<"$ACTIVATION_UNIT_SNAPSHOT_PHASE")"'
    )
    snapshot_only = recovery[
        snapshot_only_start : recovery.index(
            "RECOVERY_STATE_COUNT=$((RECOVERY_STATE_COUNT + 1))",
            snapshot_only_start,
        )
    ]
    assert "new-runtime-preserved-no-receipt" in snapshot_only
    assert "restoring-new-no-receipt" in snapshot_only
    assert "intermediate forward recovery requires a restore journal" in snapshot_only


def test_v4_engine_builds_and_verifies_a_hashed_static_wheelhouse() -> None:
    engine = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    assert "validate_hashed_requirements_lock" in engine
    assert (
        '"$CODE_VALIDATION_ROOT/deploy/production_requirements.in"'
        in engine
    )
    assert '"$CODE_VALIDATION_ROOT/requirements-platform.txt"' in engine
    assert "# REQUIREMENTS_INPUT_SHA256=" in engine
    assert "# PLATFORM_REQUIREMENTS_SHA256=" in engine
    assert "hashlib.sha256(requirements_input).hexdigest()" in engine
    assert "hashlib.sha256(platform_input).hexdigest()" in engine
    assert "prepare_trusted_wheelhouse" in engine
    assert "--require-hashes --only-binary=:all: --no-deps" in engine
    assert "PROBIGA_TRUSTED_WHEEL_MANIFEST_VERSION=1" in engine
    assert "TARGET=cp314-manylinux_2_28_x86_64" in engine
    assert "STATUS=READY" in engine
    assert "--require-hashes --no-index --only-binary=:all:" in engine
    prepare_release_venv = _shell_function_body(engine, "prepare_release_venv")
    validated_lock = prepare_release_venv.index(
        'validate_hashed_requirements_lock "$RESOLVED_LOCK"'
    )
    readable_lock = prepare_release_venv.index('chmod 0444 "$RESOLVED_LOCK"')
    isolated_download = prepare_release_venv.index("prepare_trusted_wheelhouse")
    assert validated_lock < readable_lock < isolated_download
    assert (
        '"$EXPECTED_BUILD/bin/python" -I -m pip wheel --no-deps '
        "--no-build-isolation --no-index"
        in _normalized_shell(engine)
    )
    assert 'sudo -u "$BUILD_USER" test ! -w "$EXPECTED_BUILD"' in engine
    assert '"$venv_path/bin/python" -I -m pip check' in engine


def test_dependency_wheel_cache_is_content_addressed_and_reverified() -> None:
    engine = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    trusted = _normalized_shell(
        _shell_function_body(engine, "prepare_trusted_wheelhouse")
    )
    ci_resolved = _normalized_shell(
        _shell_function_body(engine, "prepare_ci_resolved_wheelhouse")
    )
    verifier = _normalized_shell(
        _shell_function_body(engine, "verify_sealed_wheelhouse")
    )
    cleanup = _normalized_shell(
        _last_shell_function_body(engine, "cleanup_prepare_artifacts")
    )
    prepare_venv = _normalized_shell(
        _shell_function_body(engine, "prepare_release_venv")
    )

    assert (
        'cache_name="wheelhouse-cache-static-$EXPECTED_INPUT_LOCK_SHA256"'
        in trusted
    )
    assert 'cache_name="$cache_name-$EXPECTED_WHEEL_MANIFEST_SHA256"' in trusted
    assert (
        'cache_name="wheelhouse-cache-ci-$EXPECTED_INPUT_LOCK_SHA256"'
        in ci_resolved
    )
    assert "$EXPECTED_SHA" not in trusted
    assert "$EXPECTED_SHA" not in ci_resolved
    assert trusted.index('if [ -e "$cache_path" ]') < trusted.index(
        '"$BOOTSTRAP_PYTHON" -I -m pip download'
    )
    assert ci_resolved.index('if [ -e "$cache_path" ]') < ci_resolved.index(
        '"$BOOTSTRAP_PYTHON" -I -m pip download'
    )
    assert 'verify_sealed_wheelhouse "$TRUSTED_WHEELHOUSE"' in trusted
    assert 'verify_sealed_wheelhouse "$TRUSTED_WHEELHOUSE"' in ci_resolved
    assert 'mv -T -- "$TRUSTED_WHEELHOUSE" "$cache_path"' in trusted
    assert 'mv -T -- "$TRUSTED_WHEELHOUSE" "$cache_path"' in ci_resolved
    assert 'cmp --silent "$manifest_file"' in verifier
    assert 'sha256sum "$wheelhouse/$wheel_file"' in verifier
    assert 'cmp --silent "$expected_files" "$actual_files"' in verifier
    assert "! -user root -o ! -group root -o -perm /222" in verifier
    assert "-o ! -links 1" in verifier
    assert 'test "$(stat -c \'%a\' "$wheelhouse")" = 555' in verifier
    assert 'test "${#cache_name}" -le 247' in trusted
    assert 'test "${#cache_name}" -le 247' in ci_resolved
    assert '"$RELEASE_ARTIFACT_ROOT"/.wheelhouse-cache-*' in cleanup
    assert '"$RELEASE_ARTIFACT_ROOT"/wheelhouse-cache-*' in cleanup
    assert cleanup.index('rm -rf -- "$TRUSTED_WHEELHOUSE"') < cleanup.index(
        "A content-addressed, root-owned cache is a durable release input"
    )
    existing_venv = prepare_venv.index(
        'if [ -e "$RELEASE_VENV_ROOT/$EXPECTED_SHA" ]'
    )
    new_venv = prepare_venv.index("\n  else\n", existing_venv)
    assert existing_venv < new_venv < prepare_venv.index(
        "prepare_trusted_wheelhouse"
    )
    assert "--require-hashes --no-index --only-binary=:all:" in prepare_venv
    prebuild = _normalized_shell(
        _shell_function_body(engine, "prebuild_reclaim_release_space")
    )
    assert 'rm -rf -- "$RELEASE_ARTIFACT_ROOT"' not in prebuild
    assert "wheelhouse-cache-" not in prebuild


def test_wheelhouse_verifier_rejects_changed_or_extra_wheels() -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable wheel-cache test")
    engine = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    verifier = _function(
        "verify_sealed_wheelhouse",
        _portable_wheelhouse_verifier(
            _shell_function_body(engine, "verify_sealed_wheelhouse")
        ),
    )
    # The production helper requires root ownership and immutable Unix mode
    # bits. The portable harness preserves its real manifest/file-set/SHA
    # checks while the static regression above covers those root-only guards.
    harness = f"""
set -u
TEST_ROOT="$(mktemp -d)"
trap 'chmod -R u+rwX "$TEST_ROOT"; rm -rf -- "$TEST_ROOT"' EXIT
WHEELHOUSE="$TEST_ROOT/wheelhouse"
MANIFEST="$TEST_ROOT/manifest"
mkdir "$WHEELHOUSE"
printf 'verified wheel bytes\n' > "$WHEELHOUSE/demo_pkg-1.0-py3-none-any.whl"
WHEEL_SHA="$(sha256sum "$WHEELHOUSE/demo_pkg-1.0-py3-none-any.whl" | cut -d' ' -f1)"
printf '%s\n' \
  PROBIGA_TRUSTED_WHEEL_MANIFEST_VERSION=1 \
  TARGET=cp314-manylinux_2_28_x86_64 \
  STATUS=READY \
  "$WHEEL_SHA  demo_pkg-1.0-py3-none-any.whl" > "$MANIFEST"
cp "$MANIFEST" "$WHEELHOUSE/.probiga-wheel-manifest"
chmod 0444 "$MANIFEST" "$WHEELHOUSE"/* \
  "$WHEELHOUSE/.probiga-wheel-manifest"
chmod 0555 "$WHEELHOUSE"
SERVICE_USER=unused
BUILD_USER=unused
{verifier}
verify_sealed_wheelhouse "$WHEELHOUSE" "$MANIFEST" || exit 20
chmod 0644 "$WHEELHOUSE/demo_pkg-1.0-py3-none-any.whl"
printf 'tampered wheel bytes\n' > "$WHEELHOUSE/demo_pkg-1.0-py3-none-any.whl"
chmod 0444 "$WHEELHOUSE/demo_pkg-1.0-py3-none-any.whl"
if verify_sealed_wheelhouse "$WHEELHOUSE" "$MANIFEST"; then exit 21; fi
chmod 0644 "$WHEELHOUSE/demo_pkg-1.0-py3-none-any.whl"
printf 'verified wheel bytes\n' > "$WHEELHOUSE/demo_pkg-1.0-py3-none-any.whl"
chmod 0755 "$WHEELHOUSE"
printf 'unexpected wheel\n' > "$WHEELHOUSE/extra_pkg-1.0-py3-none-any.whl"
chmod 0444 "$WHEELHOUSE"/*.whl
chmod 0555 "$WHEELHOUSE"
if verify_sealed_wheelhouse "$WHEELHOUSE" "$MANIFEST"; then exit 22; fi
chmod 0755 "$WHEELHOUSE"
rm -f "$WHEELHOUSE/extra_pkg-1.0-py3-none-any.whl"
chmod 0555 "$WHEELHOUSE"
chmod 0644 "$MANIFEST"
printf '# changed manifest\n' >> "$MANIFEST"
chmod 0444 "$MANIFEST"
if verify_sealed_wheelhouse "$WHEELHOUSE" "$MANIFEST"; then exit 23; fi
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


def test_static_wheelhouse_cache_hit_is_shared_across_commit_shas() -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable wheel-cache test")
    engine = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    prepare = _function(
        "prepare_trusted_wheelhouse",
        _shell_function_body(engine, "prepare_trusted_wheelhouse"),
    )
    harness = f"""
set -u
TEST_ROOT="$(mktemp -d)"
trap 'rm -rf -- "$TEST_ROOT"' EXIT
RELEASE_ARTIFACT_ROOT="$TEST_ROOT/artifacts"
mkdir -p "$RELEASE_ARTIFACT_ROOT"
EXPECTED_INPUT_LOCK_SHA256={'a' * 64}
EXPECTED_WHEEL_MANIFEST_SHA256={'b' * 64}
TRUSTED_WHEEL_MANIFEST="$TEST_ROOT/manifest"
RESOLVED_LOCK="$TEST_ROOT/lock"
BUILD_USER=unused
SERVICE_USER=unused
DEPENDENCY_DOWNLOAD_TIMEOUT=1s
BOOTSTRAP_PYTHON=/unused/python
EXPECTED_SHA={'c' * 40}
EXPECTED_CACHE="$RELEASE_ARTIFACT_ROOT/wheelhouse-cache-static-$EXPECTED_INPUT_LOCK_SHA256-$EXPECTED_WHEEL_MANIFEST_SHA256"
mkdir "$EXPECTED_CACHE"
VERIFY_OK=0
VERIFY_COUNT=0
install() {{ return 0; }}
verify_sealed_wheelhouse() {{
  VERIFY_COUNT=$((VERIFY_COUNT + 1))
  test "$1" = "$EXPECTED_CACHE" || return 91
  test "$2" = "$TRUSTED_WHEEL_MANIFEST" || return 92
  test "$VERIFY_OK" -eq 1
}}
sudo() {{ printf 'unexpected download\n' >> "$TEST_ROOT/downloaded"; return 93; }}
{prepare}
if prepare_trusted_wheelhouse; then exit 20; fi
test "$VERIFY_COUNT" -eq 1 || exit 21
test ! -e "$TEST_ROOT/downloaded" || exit 22
VERIFY_OK=1
prepare_trusted_wheelhouse || exit 23
FIRST_PATH="$TRUSTED_WHEELHOUSE"
EXPECTED_SHA={'d' * 40}
prepare_trusted_wheelhouse || exit 24
test "$TRUSTED_WHEELHOUSE" = "$FIRST_PATH" || exit 25
test "$TRUSTED_WHEELHOUSE" = "$EXPECTED_CACHE" || exit 26
test "$VERIFY_COUNT" -eq 3 || exit 27
test ! -e "$TEST_ROOT/downloaded" || exit 28
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


def test_static_wheelhouse_cache_miss_is_sealed_then_reused(
    tmp_path: Path,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable wheel-cache test")
    engine = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    verifier = _function(
        "verify_sealed_wheelhouse",
        _portable_wheelhouse_verifier(
            _shell_function_body(engine, "verify_sealed_wheelhouse")
        ),
    )
    seal_body = _shell_function_body(engine, "seal_wheelhouse_cache").replace(
        "install -o root -g root -m", "install -m"
    )
    seal = _function("seal_wheelhouse_cache", seal_body)
    prepare_body = _shell_function_body(
        engine, "prepare_trusted_wheelhouse"
    ).replace("install -d -o root -g root -m", "install -d -m")
    prepare = _function("prepare_trusted_wheelhouse", prepare_body)
    harness = f"""
set -u
TEST_ROOT="$(mktemp -d)"
trap 'chmod -R u+rwX "$TEST_ROOT"; rm -rf -- "$TEST_ROOT"' EXIT
RELEASE_ARTIFACT_ROOT="$TEST_ROOT/artifacts"
RESOLVED_LOCK="$TEST_ROOT/requirements.lock"
TRUSTED_WHEEL_MANIFEST="$TEST_ROOT/manifest"
WHEEL_NAME=demo_pkg-1.0-py3-none-any.whl
WHEEL_BYTES='verified wheel bytes'
printf 'locked\n' > "$RESOLVED_LOCK"
printf '%s\n' "$WHEEL_BYTES" > "$TEST_ROOT/reference-wheel"
WHEEL_SHA="$(sha256sum "$TEST_ROOT/reference-wheel" | cut -d' ' -f1)"
printf '%s\n' \
  PROBIGA_TRUSTED_WHEEL_MANIFEST_VERSION=1 \
  TARGET=cp314-manylinux_2_28_x86_64 \
  STATUS=READY \
  "$WHEEL_SHA  $WHEEL_NAME" > "$TRUSTED_WHEEL_MANIFEST"
EXPECTED_INPUT_LOCK_SHA256={'a' * 64}
EXPECTED_WHEEL_MANIFEST_SHA256={'b' * 64}
EXPECTED_SHA={'c' * 40}
BUILD_USER=unused
SERVICE_USER=unused
DEPENDENCY_DOWNLOAD_TIMEOUT=1s
BOOTSTRAP_PYTHON=/unused/python
TRUSTED_WHEELHOUSE=
DOWNLOAD_COUNT=0
chown() {{ return 0; }}
sudo() {{
  if [ "${{3:-}}" = /usr/bin/env ]; then
    DOWNLOAD_COUNT=$((DOWNLOAD_COUNT + 1))
    printf '%s\n' "$WHEEL_BYTES" > "$TRUSTED_WHEELHOUSE/$WHEEL_NAME"
  fi
  return 0
}}
{verifier}
{seal}
{prepare}
prepare_trusted_wheelhouse || exit 20
EXPECTED_CACHE="$RELEASE_ARTIFACT_ROOT/wheelhouse-cache-static-$EXPECTED_INPUT_LOCK_SHA256-$EXPECTED_WHEEL_MANIFEST_SHA256"
test "$TRUSTED_WHEELHOUSE" = "$EXPECTED_CACHE" || exit 21
test -d "$EXPECTED_CACHE" || exit 22
cmp --silent "$TRUSTED_WHEEL_MANIFEST" \
  "$EXPECTED_CACHE/.probiga-wheel-manifest" || exit 23
test "$DOWNLOAD_COUNT" -eq 1 || exit 24
EXPECTED_SHA={'d' * 40}
prepare_trusted_wheelhouse || exit 25
test "$TRUSTED_WHEELHOUSE" = "$EXPECTED_CACHE" || exit 26
test "$DOWNLOAD_COUNT" -eq 1 || exit 27
chmod 0644 "$EXPECTED_CACHE/$WHEEL_NAME"
printf 'tampered wheel bytes\n' > "$EXPECTED_CACHE/$WHEEL_NAME"
chmod 0444 "$EXPECTED_CACHE/$WHEEL_NAME"
if prepare_trusted_wheelhouse; then exit 28; fi
test "$DOWNLOAD_COUNT" -eq 1 || exit 29
"""
    harness_path = tmp_path / "wheelhouse-cache-miss.sh"
    harness_path.write_text(harness, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        [bash, str(harness_path)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_wheelhouse_cleanup_removes_only_interrupted_staging_cache() -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("bash is required for the executable wheel-cache test")
    engine = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    cleanup = _function(
        "cleanup_prepare_artifacts",
        _last_shell_function_body(engine, "cleanup_prepare_artifacts"),
    )
    harness = f"""
set -u
TEST_ROOT="$(mktemp -d)"
trap 'rm -rf -- "$TEST_ROOT"' EXIT
RELEASE_ARTIFACT_ROOT="$TEST_ROOT/artifacts"
mkdir -p "$RELEASE_ARTIFACT_ROOT"
STAGING="$RELEASE_ARTIFACT_ROOT/.wheelhouse-cache-static-{'a' * 64}-{'b' * 64}.ABCDEF"
CACHE="$RELEASE_ARTIFACT_ROOT/wheelhouse-cache-static-{'a' * 64}-{'b' * 64}"
mkdir "$STAGING" "$CACHE"
RESOLVED_LOCK=
TRUSTED_WHEEL_MANIFEST=
HEALTH_RESPONSE=
ADATA_SOURCE_BUILD=
ADATA_RUNTIME_ROOT="$TEST_ROOT/adata"
ADATA_BUILD_SOURCE=
ADATA_WHEEL_DIR=
ADATA_CACHE_BUILD=
DEPLOY_SUCCEEDED=1
EXPECTED_BUILD=
ACTIVATION_UNIT_SNAPSHOT_DIR="$TEST_ROOT/activation"
RELEASE_VENV_ROOT="$TEST_ROOT/venvs"
EXPECTED_SHA={'c' * 40}
NEW_VENV_LINK=0
PREVIOUS_DROPIN=
PREVIOUS_SCHEDULER_DROPIN=
PREVIOUS_AI_WORKER_DROPIN=
PREVIOUS_LEGACY_MAIN_DROPIN_DIR=
PREVIOUS_LEGACY_SCHEDULER_DROPIN_DIR=
PREVIOUS_LOCK_SNAPSHOT=
GOVERNANCE_TASK_OLD_SOURCE=
GOVERNANCE_TASK_NEW_SOURCE=
QMT_ANNOUNCEMENT_TASK_OLD_SOURCE=
QMT_ANNOUNCEMENT_TASK_NEW_SOURCE=
PREPARED_MAIN_DROPIN=
PREPARED_SCHEDULER_DROPIN=
PREPARED_AI_WORKER_DROPIN=
cleanup_staging_worktree() {{ return 0; }}
path_is_runtime_referenced() {{ return 1; }}
{cleanup}
TRUSTED_WHEELHOUSE="$STAGING"
cleanup_prepare_artifacts || exit 20
test ! -e "$STAGING" || exit 21
test -d "$CACHE" || exit 22
TRUSTED_WHEELHOUSE="$CACHE"
cleanup_prepare_artifacts || exit 23
test -d "$CACHE" || exit 24
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

    assert "REQUIRED_DEPLOY_PROTOCOL_V4=probiga-production-deploy-v4" in engine
    assert "RETIRED_DEPLOY_PROTOCOL_V2=probiga-production-deploy-v2" in engine
    assert "COMPATIBLE_DEPLOY_PROTOCOL_V2" not in engine
    protocol_guard = engine.index(
        'case "${PROBIGA_DEPLOY_PROTOCOL_VERSION:-}" in'
    )
    assert "production deploy broker protocol mismatch" in engine
    retired_start = engine.index(
        '"$RETIRED_DEPLOY_PROTOCOL_V2")', protocol_guard
    )
    retired_branch = engine[
        retired_start:engine.index(";;", retired_start)
    ]
    assert "DEPLOY_ARTIFACT_MODE=" not in retired_branch
    assert "exit 2" in retired_branch
    assert re.search(r"(?is)v2.*(?:retired|unsupported|not supported)", retired_branch)
    assert protocol_guard < engine.index(": \"${EXPECTED_SHA:")
    assert protocol_guard < engine.index("prepare_release() {")
    assert protocol_guard < engine.index('exec 9>"$DEPLOY_LOCK_FILE"')
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
    assert 'EXPECTED_RECOVERY_TOOL_SHA="$(' in normalized
    assert '"${GIT[@]}" rev-parse refs/remotes/origin/main' in normalized
    assert (
        '"${GIT[@]}" merge-base --is-ancestor '
        '"$EXPECTED_RECOVERY_GUARD_SHA" "$EXPECTED_RECOVERY_TOOL_SHA"'
        in normalized
    )
    assert (
        '"${GIT[@]}" show "${EXPECTED_RECOVERY_TOOL_SHA}:'
        'deploy/production_deploy.sh"'
        in normalized
    )
    assert (
        '"${GIT[@]}" show "${EXPECTED_RECOVERY_GUARD_SHA}:'
        'deploy/production_deploy.sh"'
        not in normalized
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
