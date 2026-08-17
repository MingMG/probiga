# -*- coding: utf-8 -*-
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import run_remote_mysql_tunnel
from tools import run_production_acceptance_job
import remote_support as root_remote_support
from tools.remote_support import (
    DEFAULT_SSH_AUTH_TIMEOUT_SECONDS,
    DEFAULT_SSH_BANNER_TIMEOUT_SECONDS,
    DEFAULT_SSH_CONNECT_TIMEOUT_SECONDS,
    UnsafeProductionSshError,
    UnsafeRemoteRuntimeError,
    production_release_command,
    production_ssh_client,
    production_ssh_connect_kwargs,
    remote_host,
    remote_pythonpath,
    remote_root,
    remote_user,
    ssh_connect_kwargs,
)


def test_remote_host_and_user_use_environment(monkeypatch):
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_HOST", "example.internal")
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_USER", "deploy")

    assert remote_host() == "example.internal"
    assert remote_user() == "deploy"


def test_remote_root_trims_trailing_slash(monkeypatch):
    monkeypatch.setenv("PROBIGA_REMOTE_ROOT", "/srv/probiga/")

    assert remote_root() == "/srv/probiga"


def test_remote_pythonpath_rejects_mutable_checkout_runtime():
    with pytest.raises(
        UnsafeRemoteRuntimeError,
        match="refusing to construct PYTHONPATH from mutable checkout",
    ):
        remote_pythonpath("/srv/probiga/")


def test_production_release_command_uses_active_pins_and_sealed_adata():
    command = production_release_command(
        "tools/verify_trading_v3_production.py",
        ("--local-runtime",),
        root="/opt/ProBigA",
    )

    assert "http://127.0.0.1/api/health" in command
    assert 'RELEASE_VENV_ROOT="$ROOT/.release_venvs"' in command
    assert ".probiga.gitsha" in command
    assert 'PROBIGA_BUILD_COMMIT_SHA="$EXPECTED_SHA"' in command
    assert "systemctl show -p MainPID --value probiga" in command
    assert "/var/lib/probiga/release-sources/adata" in command
    assert 'PYTHONPATH="$ADATA_SOURCE:$ROOT"' in command
    assert '"$RELEASE_VENV/bin/python"' in command
    assert "/opt/ProBigA/venv/bin/python" not in command
    assert "/opt/ProBigA/adata" not in command


@pytest.mark.parametrize(
    ("entrypoint", "root"),
    (
        ("../tools/verify.py", "/opt/ProBigA"),
        ("/tmp/verify.py", "/opt/ProBigA"),
        ("tools/verify.sh", "/opt/ProBigA"),
        ("tools/migrate_production.py", "/opt/ProBigA"),
        ("tools/verify.py", "relative/root"),
        ("tools/verify_trading_v3_production.py", "/opt/ProBigA/../other"),
    ),
)
def test_production_release_command_rejects_unpinned_paths(entrypoint, root):
    with pytest.raises(UnsafeRemoteRuntimeError):
        production_release_command(entrypoint, root=root)


def test_production_acceptance_start_fails_before_connect(monkeypatch):
    args = SimpleNamespace(
        action="start",
        name="unsafe-adata-probe",
        memory_high="450M",
        memory_max="700M",
        cpu_quota="70%",
        runtime_max="7200",
        lines=120,
        command=["--", "tools/sync_kline_adata.py"],
    )
    monkeypatch.setattr(
        run_production_acceptance_job,
        "parse_args",
        lambda: args,
    )
    monkeypatch.setattr(
        run_production_acceptance_job,
        "_connect",
        lambda: pytest.fail("SSH connection happened before the runtime guard"),
    )

    with pytest.raises(UnsafeRemoteRuntimeError):
        run_production_acceptance_job.main()


def test_ssh_connect_kwargs_uses_remote_helpers(monkeypatch):
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_HOST", "example.internal")
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_USER", "deploy")
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_PASSWORD", "secret")

    kwargs = ssh_connect_kwargs(timeout=5)

    assert kwargs["hostname"] == "example.internal"
    assert kwargs["username"] == "deploy"
    assert kwargs["password"] == "secret"
    assert kwargs["timeout"] == 5
    assert kwargs["auth_timeout"] == DEFAULT_SSH_AUTH_TIMEOUT_SECONDS
    assert kwargs["banner_timeout"] == DEFAULT_SSH_BANNER_TIMEOUT_SECONDS


def test_ssh_connect_kwargs_adds_default_timeouts(monkeypatch):
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_PASSWORD", "secret")

    kwargs = ssh_connect_kwargs()

    assert kwargs["timeout"] == DEFAULT_SSH_CONNECT_TIMEOUT_SECONDS
    assert kwargs["auth_timeout"] == DEFAULT_SSH_AUTH_TIMEOUT_SECONDS
    assert kwargs["banner_timeout"] == DEFAULT_SSH_BANNER_TIMEOUT_SECONDS


def test_ssh_connect_kwargs_accepts_password_override(monkeypatch):
    monkeypatch.delenv("PROBIGA_REMOTE_SSH_PASSWORD", raising=False)

    kwargs = ssh_connect_kwargs(password="from-option")

    assert kwargs["password"] == "from-option"


def test_root_remote_support_shim_forwards_shared_helpers():
    assert root_remote_support.remote_host is remote_host
    assert root_remote_support.ssh_connect_kwargs is ssh_connect_kwargs
    assert root_remote_support.production_ssh_client is production_ssh_client
    assert (
        root_remote_support.production_release_command
        is production_release_command
    )
    assert (
        root_remote_support.production_ssh_connect_kwargs
        is production_ssh_connect_kwargs
    )
    assert root_remote_support.DEFAULT_SSH_CONNECT_TIMEOUT_SECONDS == DEFAULT_SSH_CONNECT_TIMEOUT_SECONDS


def test_mysql_tunnel_defaults_to_remote_support(monkeypatch):
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_HOST", "example.internal")
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_USER", "deploy")

    args = run_remote_mysql_tunnel.parse_args([])

    assert args.ssh_host == "example.internal"
    assert args.ssh_user == "deploy"


def test_production_ssh_kwargs_require_named_key_only_identity(
    monkeypatch, tmp_path
):
    key = tmp_path / "deploy-key"
    key.write_text("test-only-key-placeholder", encoding="utf-8")
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_HOST", "prod.internal")
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_USER", "deploy")
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_KEY_FILE", str(key))
    monkeypatch.delenv("PROBIGA_REMOTE_SSH_PASSWORD", raising=False)

    kwargs = production_ssh_connect_kwargs(timeout=7)

    assert kwargs == {
        "hostname": "prod.internal",
        "username": "deploy",
        "key_filename": str(key.resolve()),
        "look_for_keys": False,
        "allow_agent": False,
        "timeout": 7,
        "auth_timeout": DEFAULT_SSH_AUTH_TIMEOUT_SECONDS,
        "banner_timeout": DEFAULT_SSH_BANNER_TIMEOUT_SECONDS,
    }


@pytest.mark.parametrize(
    ("user", "password", "message"),
    (
        ("root", "", "root is forbidden"),
        ("deploy", "shared-secret", "password authentication is disabled"),
    ),
)
def test_production_ssh_kwargs_reject_root_and_shared_password(
    monkeypatch, tmp_path, user, password, message
):
    key = tmp_path / "deploy-key"
    key.write_text("test-only-key-placeholder", encoding="utf-8")
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_HOST", "prod.internal")
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_USER", user)
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_KEY_FILE", str(key))
    if password:
        monkeypatch.setenv("PROBIGA_REMOTE_SSH_PASSWORD", password)
    else:
        monkeypatch.delenv("PROBIGA_REMOTE_SSH_PASSWORD", raising=False)

    with pytest.raises(UnsafeProductionSshError, match=message):
        production_ssh_connect_kwargs()


def test_production_ssh_client_requires_known_hosts_and_rejects_unknown(
    monkeypatch, tmp_path
):
    known_hosts = tmp_path / "known_hosts"
    known_hosts.write_text("prod.internal ssh-ed25519 AAAATEST\n", encoding="utf-8")
    monkeypatch.setenv("PROBIGA_SSH_KNOWN_HOSTS", str(known_hosts))
    events = []

    class FakeClient:
        def load_system_host_keys(self):
            events.append(("system", None))

        def load_host_keys(self, path):
            events.append(("explicit", path))

        def set_missing_host_key_policy(self, policy):
            events.append(("policy", policy))

    reject_policy = object()
    fake_paramiko = SimpleNamespace(
        SSHClient=FakeClient,
        RejectPolicy=lambda: reject_policy,
    )

    client = production_ssh_client(fake_paramiko)

    assert isinstance(client, FakeClient)
    assert events == [
        ("explicit", str(known_hosts.resolve())),
        ("policy", reject_policy),
    ]


def test_deploy_release_venv_and_layer4_ci_are_git_sha_bound():
    root = Path(__file__).resolve().parents[1]
    workflow = (root / ".github/workflows/deploy.yml").read_text(
        encoding="utf-8"
    )

    assert workflow.count(".probiga.gitsha") >= 2
    assert (
        'printf \'%s\\n\' "$EXPECTED_SHA" '
        '> "$EXPECTED_BUILD/.probiga.gitsha"'
    ) in workflow
    api_dropin = workflow[
        workflow.index("            write_dropin() {") :
        workflow.index("            write_scheduler_dropin() {")
    ]
    scheduler_dropin = workflow[
        workflow.index("            write_scheduler_dropin() {") :
        workflow.index("            BOOTSTRAP_PYTHON=")
    ]
    build_identity = '"Environment=PROBIGA_BUILD_COMMIT_SHA=$revision"'
    expected_identity = '"Environment=PROBIGA_EXPECTED_GIT_SHA=$revision"'
    assert workflow.count(build_identity) == 2
    assert api_dropin.count(expected_identity) == 1
    assert api_dropin.count(build_identity) == 1
    assert scheduler_dropin.count(expected_identity) == 1
    assert scheduler_dropin.count(build_identity) == 1
    for test_file in (
        "tests/test_trading_v3_horizon_models.py",
        "tests/test_trading_v3_horizon_artifact_registry.py",
        "tests/test_trading_v3_horizon_runtime_selection.py",
        "tests/test_trading_v3_continuous_calibration.py",
        "tests/test_trading_v3_shadow_intelligence_runtime.py",
        "tests/test_trading_v3_production_activation.py",
        "tests/test_trading_v3_research_api.py",
    ):
        assert test_file in workflow


def test_example_environment_documents_key_only_production_verification():
    root = Path(__file__).resolve().parents[1]
    example = (root / ".env.example").read_text(encoding="utf-8")

    assert "PROBIGA_REMOTE_SSH_HOST=" in example
    assert "PROBIGA_REMOTE_SSH_USER=" in example
    assert "PROBIGA_REMOTE_SSH_KEY_FILE=" in example
    assert "PROBIGA_SSH_KNOWN_HOSTS=" in example
    assert "PROBIGA_MANUAL_PRODUCTION_DEPLOY_ENABLED=0" in example
