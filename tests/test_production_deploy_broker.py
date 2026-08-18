import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _normalized_shell(source: str) -> str:
    """Join shell continuations so command-level assertions stay readable."""

    return re.sub(r"[ \t]*\\\r?\n[ \t]*", " ", source)


def test_workflow_uses_fixed_root_owned_deploy_broker() -> None:
    workflow = (ROOT / ".github" / "workflows" / "deploy.yml").read_text(
        encoding="utf-8"
    )

    assert "sudo -n /usr/local/sbin/probiga-production-deploy" in workflow
    assert "bash \"$DEPLOY_BOOTSTRAP\"" not in workflow


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
    assert "resolved requirements digest differs" in broker


def test_broker_and_engine_use_fd_locks_and_a_versioned_protocol() -> None:
    broker = (ROOT / "deploy" / "production_deploy_root.sh").read_text(
        encoding="utf-8"
    )
    engine = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )

    assert (
        "readonly DEPLOY_PROTOCOL_VERSION=probiga-production-deploy-v2"
        in broker
    )
    assert (
        'export PROBIGA_DEPLOY_PROTOCOL_VERSION="$DEPLOY_PROTOCOL_VERSION"'
        in broker
    )
    protocol_export = broker.index(
        'export PROBIGA_DEPLOY_PROTOCOL_VERSION="$DEPLOY_PROTOCOL_VERSION"'
    )
    assert protocol_export < broker.index('bash "$BOOTSTRAP_FILE"')

    assert "readonly BROKER_LOCK_ROOT=/run/probiga" in broker
    assert (
        'readonly BROKER_LOCK_FILE="$BROKER_LOCK_ROOT/production-broker.lock"'
        in broker
    )
    assert 'exec 8>"$BROKER_LOCK_FILE"' in broker
    assert "flock -n 8" in broker
    assert not re.search(r"\b(?:mkdir|rmdir)\b[^\n]*BROKER_LOCK", broker)

    assert "REQUIRED_DEPLOY_PROTOCOL=probiga-production-deploy-v2" in engine
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
        r'git --git-dir="\$CODE_GIT_CACHE"\s+rev-parse\s+'
        r'--is-bare-repository',
        normalized,
    )
    assert 'GIT=(git --git-dir="$CODE_GIT_CACHE")' in broker
    assert re.search(
        r'"\$\{GIT\[@\]\}"\s+fetch\s+--no-tags\s+origin\s+'
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
