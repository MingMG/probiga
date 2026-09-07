"""Execute deployment boundary helpers without touching a production host."""

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
BROKER = (ROOT / "deploy/production_deploy_root.sh").read_text(encoding="utf-8")
ENGINE = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")


def function(source: str, name: str) -> str:
    match = re.search(rf"(?ms)^{name}\(\) \{{\n.*?^\}}$", source)
    assert match is not None
    return match.group(0)


def run_bash(script: str, **environment: str) -> subprocess.CompletedProcess[str]:
    bash = shutil.which("bash")
    if not bash:
        candidate = Path(r"C:\Program Files\Git\bin\bash.exe")
        if candidate.exists():
            bash = str(candidate)
    if not bash:
        pytest.skip("bash is required for executable deployment preflight tests")
    return subprocess.run(
        [bash, "-c", script],
        env={**os.environ, **environment},
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=15,
        check=False,
    )


@pytest.mark.parametrize(
    ("status", "detail", "reason"),
    [
        ("124", "", "network_timeout"),
        ("128", "ssh: connect to host github.com port 22: Connection timed out", "network_timeout"),
        ("128", "Could not resolve hostname github.com", "dns_resolution_failed"),
        ("128", "Permission denied (publickey)", "authentication_failed"),
        ("128", "Host key verification failed", "ssh_host_identity_failed"),
        ("128", "Connection refused", "connection_refused"),
        ("128", "Repository not found", "repository_access_failed"),
        ("128", "unexpected failure", "git_transport_failed"),
    ],
)
def test_broker_network_failure_is_classified_and_stops_before_mutation(
    status: str, detail: str, reason: str
) -> None:
    transport = function(BROKER, "clean_git_ssh").replace(
        "/usr/bin/env -i", "fake_transport"
    )
    script = "\n".join([
        "set -euo pipefail",
        "REMOTE_GIT_SSH=fixed-trusted-ssh",
        function(BROKER, "fail"),
        function(BROKER, "git_network_failure_reason"),
        'fake_transport() { printf "%s\\n" "$FAKE_DETAIL"; return "$FAKE_STATUS"; }',
        transport,
        'REMOTE_SHA="$(clean_git_ssh ls-remote fixed-remote refs/heads/main)"',
        'echo SERVICE_STOP_WOULD_RUN',
    ])
    completed = run_bash(
        script,
        FAKE_STATUS=status,
        FAKE_DETAIL=detail + " https://user:secret-token@example.invalid/path?token=private",
    )
    assert completed.returncode == 2
    assert "stage=git_remote_main" in completed.stderr
    assert f"reason={reason}" in completed.stderr
    assert f"exit={status}" in completed.stderr
    assert "effective_proxy=none" in completed.stderr
    assert "SERVICE_STOP_WOULD_RUN" not in completed.stdout
    assert "secret-token" not in completed.stdout + completed.stderr
    assert "private" not in completed.stdout + completed.stderr


def test_broker_success_filters_diagnostics_from_remote_identity() -> None:
    transport = function(BROKER, "clean_git_ssh").replace(
        "/usr/bin/env -i", "fake_transport"
    )
    sha = "a" * 40
    completed = run_bash("\n".join([
        "set -euo pipefail",
        "REMOTE_GIT_SSH=fixed-trusted-ssh",
        'fake_transport() { printf "%s\\n" "$FAKE_DETAIL"; }',
        transport,
        'clean_git_ssh ls-remote fixed-remote refs/heads/main',
    ]), FAKE_DETAIL=f"warning with sensitive-userinfo\n{sha}\trefs/heads/main")
    assert completed.returncode == 0
    assert completed.stdout.strip() == f"{sha}\trefs/heads/main"
    assert "state=completed" in completed.stderr
    assert "sensitive-userinfo" not in completed.stdout + completed.stderr


@pytest.mark.parametrize("kind", ["broker", "adata"])
def test_real_network_deadline_terminates_hung_git_before_followup(kind: str) -> None:
    source, name = (BROKER, "clean_git_ssh") if kind == "broker" else (ENGINE, "fetch_adata_git")
    transport = function(source, name).replace("45s", "0.2s").replace("120s", "0.2s")
    transport = transport.replace(
        "/usr/bin/git --no-replace-objects", "/usr/bin/bash -c 'sleep 5' --"
    )
    completed = run_bash("\n".join([
        "set -euo pipefail",
        "REMOTE_GIT_SSH=fixed-ssh ADATA_GIT_CACHE=unused EXPECTED_ADATA_SHA=unused",
        function(BROKER, "fail"),
        function(BROKER, "git_network_failure_reason"),
        transport,
        "clean_git_ssh ls-remote fixed-remote refs/heads/main" if kind == "broker" else "fetch_adata_git",
        "echo SERVICE_STOP_WOULD_RUN",
    ]))
    assert completed.returncode == 2, completed.stderr
    assert "reason=network_timeout" in completed.stderr
    assert "exit=124" in completed.stderr
    assert "SERVICE_STOP_WOULD_RUN" not in completed.stdout


@pytest.mark.parametrize(
    ("detail", "reason"),
    [
        ("SSL certificate problem", "tls_verification_failed"),
        ("Authentication failed", "authentication_failed"),
        ("Could not resolve host", "dns_resolution_failed"),
        ("CONNECT tunnel failed", "proxy_connection_failed"),
    ],
)
def test_adata_failure_retains_stage_and_redacts_output(detail: str, reason: str) -> None:
    transport = function(ENGINE, "fetch_adata_git").replace("/usr/bin/env -i", "fake_transport")
    completed = run_bash("\n".join([
        "set -euo pipefail",
        "ADATA_GIT_CACHE=unused EXPECTED_ADATA_SHA=unused CUTOVER_STEP=preparation",
        'fake_transport() { printf "%s\\n" "$FAKE_DETAIL"; return 128; }',
        transport,
        'if fetch_adata_git; then exit 99; fi',
        'printf "retained_step=%s\\n" "$CUTOVER_STEP"',
    ]), FAKE_DETAIL=detail + " https://user:secret-token@example.invalid")
    assert completed.returncode == 0
    assert "retained_step=git_adata_fetch" in completed.stdout
    assert f"reason={reason}" in completed.stderr
    assert "secret-token" not in completed.stdout + completed.stderr


def test_permission_denial_exits_before_any_engine_mutation() -> None:
    permissions = function(ENGINE, "preflight_linux_execution_permissions")
    # Windows lacks systemd/sudo. Keep the real control flow and replace only
    # OS entrypoints; the first identity query succeeds and user switch fails.
    permissions = permissions.replace('[ ! -x "$path" ]', "false")
    permissions = permissions.replace("/usr/bin/timeout", "fake_timeout")
    completed = run_bash("\n".join([
        "set -euo pipefail",
        "DEPLOY_OPERATION=deploy",
        'fake_timeout() { case "$*" in *systemctl*) echo probiga;; *) return 1;; esac; }',
        permissions,
        'preflight_linux_execution_permissions || exit 2',
        'echo SERVICE_STOP_WOULD_RUN',
    ]))
    assert completed.returncode == 2
    assert "reason=service_user_switch_denied" in completed.stderr
    assert "SERVICE_STOP_WOULD_RUN" not in completed.stdout
    check = ENGINE.index("preflight_linux_execution_permissions || exit 2")
    assert check < ENGINE.index('cd "$REPOSITORY_ROOT"')
    assert check < ENGINE.index('install -d -o root -g root -m 0700 "$DEPLOY_LOCK_ROOT"')


@pytest.mark.parametrize("parent_is_file", [False, True])
def test_permission_preflight_checks_parent_without_creating_runtime_directory(
    tmp_path: Path, parent_is_file: bool
) -> None:
    parent = tmp_path / "runtime-parent"
    if parent_is_file:
        parent.write_text("not a directory", encoding="utf-8")
    else:
        parent.mkdir()
    missing = parent / "probiga" / "nested"
    permissions = function(ENGINE, "preflight_linux_execution_permissions")
    permissions = permissions.replace('[ ! -x "$path" ]', "false")
    permissions = permissions.replace("/usr/bin/timeout", "fake_timeout")
    permissions = permissions.replace(
        "for path in /etc/systemd/system /opt /var/lib/probiga /run/probiga; do",
        f'for path in "{missing.as_posix()}"; do',
    )
    completed = run_bash("\n".join([
        "set -euo pipefail",
        "DEPLOY_OPERATION=deploy",
        'fake_timeout() { case "$*" in *systemctl*) echo probiga;; *) return 0;; esac; }',
        permissions,
        'preflight_linux_execution_permissions || exit 2',
    ]))
    assert completed.returncode == (2 if parent_is_file else 0), completed.stderr
    assert not missing.exists()
    if parent_is_file:
        assert "reason=deployment_path_not_writable" in completed.stderr
    else:
        assert "state=completed" in completed.stderr


def test_transport_uses_reviewed_configuration_and_no_persistent_proxy_changes() -> None:
    for setting in (
        "-F /dev/null", "-o ConnectTimeout=10", "-o ConnectionAttempts=1",
        "-o StrictHostKeyChecking=yes", "-o ServerAliveInterval=15",
    ):
        assert setting in BROKER
    for transport in (function(BROKER, "clean_git_ssh"), function(ENGINE, "fetch_adata_git")):
        assert "/usr/bin/env -i" in transport
        assert "GIT_CONFIG_NOSYSTEM=1" in transport
        assert "GIT_CONFIG_GLOBAL=/dev/null" in transport
        assert "/usr/bin/timeout --signal=TERM --kill-after=5s" in transport
        assert "config --global" not in transport
        assert "config --system" not in transport
    assert "-c http.sslVerify=true" in function(ENGINE, "fetch_adata_git")
    assert "-c remote.origin.proxy=" in function(ENGINE, "fetch_adata_git")
    assert "/usr/bin/git --no-replace-objects -C /" in function(BROKER, "clean_git_ssh")
    assert ENGINE.index("  prepare_adata_release\n") < ENGINE.index("CUTOVER_STEP=request_qmt_windows_edge_quiescence_before_service_stop")
