# -*- coding: utf-8 -*-
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import run_production_mysql_forward
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
        root="/opt/ProBigA-current",
    )

    assert command == (
        "sudo -n /usr/local/sbin/probiga-production-deploy "
        "--verify-trading-v3"
    )


@pytest.mark.parametrize(
    "arguments",
    ((), ("--real-trading-closed-only",), ("--local-runtime", "extra")),
)
def test_production_release_command_rejects_variable_verifier_arguments(arguments):
    with pytest.raises(UnsafeRemoteRuntimeError, match="fixed local-runtime"):
        production_release_command(
            "tools/verify_trading_v3_production.py",
            arguments,
        )


@pytest.mark.parametrize(
    ("entrypoint", "root"),
    (
        ("../tools/verify.py", "/opt/ProBigA-current"),
        ("/tmp/verify.py", "/opt/ProBigA-current"),
        ("tools/verify.sh", "/opt/ProBigA-current"),
        ("tools/migrate_production.py", "/opt/ProBigA-current"),
        ("tools/verify.py", "relative/root"),
        ("tools/verify_trading_v3_production.py", ""),
        ("tools/verify_trading_v3_production.py", "/opt/ProBigA"),
        (
            "tools/verify_trading_v3_production.py",
            "/opt/ProBigA-current/../other",
        ),
    ),
)
def test_production_release_command_rejects_unpinned_paths(entrypoint, root):
    with pytest.raises(UnsafeRemoteRuntimeError):
        production_release_command(entrypoint, root=root)


def test_ssh_connect_kwargs_uses_remote_helpers(monkeypatch):
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_HOST", "example.internal")
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_USER", "deploy")
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_PASSWORD", "secret")
    monkeypatch.delenv("PROBIGA_REMOTE_SSH_KEY_FILE", raising=False)

    kwargs = ssh_connect_kwargs(timeout=5)

    assert kwargs["hostname"] == "example.internal"
    assert kwargs["username"] == "deploy"
    assert kwargs["password"] == "secret"
    assert kwargs["timeout"] == 5
    assert kwargs["auth_timeout"] == DEFAULT_SSH_AUTH_TIMEOUT_SECONDS
    assert kwargs["banner_timeout"] == DEFAULT_SSH_BANNER_TIMEOUT_SECONDS


def test_ssh_connect_kwargs_adds_default_timeouts(monkeypatch):
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_HOST", "example.internal")
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_USER", "deploy")
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_PASSWORD", "secret")

    kwargs = ssh_connect_kwargs()

    assert kwargs["timeout"] == DEFAULT_SSH_CONNECT_TIMEOUT_SECONDS
    assert kwargs["auth_timeout"] == DEFAULT_SSH_AUTH_TIMEOUT_SECONDS
    assert kwargs["banner_timeout"] == DEFAULT_SSH_BANNER_TIMEOUT_SECONDS


def test_ssh_connect_kwargs_rejects_password_override(monkeypatch):
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_HOST", "example.internal")
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_USER", "deploy")
    monkeypatch.delenv("PROBIGA_REMOTE_SSH_PASSWORD", raising=False)

    with pytest.raises(
        UnsafeProductionSshError,
        match="may only come from PROBIGA_REMOTE_SSH_PASSWORD",
    ):
        ssh_connect_kwargs(password="from-option")


def test_ssh_connect_kwargs_prefers_explicit_key(monkeypatch, tmp_path):
    key = tmp_path / "deploy-key"
    key.write_text("test-only-key-placeholder", encoding="utf-8")
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_HOST", "example.internal")
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_USER", "deploy")
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_KEY_FILE", str(key))
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_PASSWORD", "legacy-secret")

    kwargs = ssh_connect_kwargs()

    assert kwargs["key_filename"] == str(key.resolve())
    assert "password" not in kwargs


def test_production_mysql_forward_defaults_to_remote_support(monkeypatch):
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_HOST", "example.internal")
    monkeypatch.setenv("PROBIGA_REMOTE_SSH_USER", "deploy")

    args = run_production_mysql_forward.parse_args([])

    assert args.ssh_host == "example.internal"
    assert args.ssh_user == "deploy"
    assert args.local_host == "127.0.0.1"
    assert args.local_port == 3306
    assert args.remote_host == "127.0.0.1"
    assert args.remote_port == 13306


@pytest.mark.parametrize("value", ["0.0.0.0", "example.internal", "172.28.84.10"])
def test_production_mysql_forward_rejects_non_loopback_endpoints(value):
    with pytest.raises(ValueError, match="explicit loopback"):
        run_production_mysql_forward._loopback(value, label="test endpoint")


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


def test_deploy_release_venv_and_engine_are_git_sha_bound():
    root = Path(__file__).resolve().parents[1]
    root_broker = (root / "deploy/production_deploy_root.sh").read_text(
        encoding="utf-8"
    )
    deploy_script = (root / "deploy/production_deploy.sh").read_text(
        encoding="utf-8"
    )

    assert deploy_script.count(".probiga.gitsha") >= 2
    assert (
        'printf \'%s\\n\' "$EXPECTED_SHA" '
        '> "$EXPECTED_BUILD/.probiga.gitsha"'
    ) in deploy_script
    api_dropin = deploy_script[
        deploy_script.index("write_dropin() {") :
        deploy_script.index("write_scheduler_dropin() {")
    ]
    scheduler_dropin = deploy_script[
        deploy_script.index("write_scheduler_dropin() {") :
        deploy_script.index("write_ai_worker_dropin() {")
    ]
    ai_worker_dropin = deploy_script[
        deploy_script.index("write_ai_worker_dropin() {") :
        deploy_script.index("assert_ai_worker_runtime() {")
    ]
    build_identity = '"Environment=PROBIGA_BUILD_COMMIT_SHA=$revision"'
    expected_identity = '"Environment=PROBIGA_EXPECTED_GIT_SHA=$revision"'
    assert deploy_script.count(build_identity) == 3
    assert api_dropin.count(expected_identity) == 1
    assert api_dropin.count(build_identity) == 1
    assert scheduler_dropin.count(expected_identity) == 1
    assert scheduler_dropin.count(build_identity) == 1
    assert ai_worker_dropin.count(expected_identity) == 1
    assert ai_worker_dropin.count(build_identity) == 1

    remote_tip = root_broker.index(
        'REMOTE_SHA="$(clean_git_ssh ls-remote '
    )
    exact_tip = root_broker.index(
        'test "$REMOTE_SHA" = "$EXPECTED_SHA"', remote_tip
    )
    fetched_tip = root_broker.index(
        'rev-parse refs/remotes/origin/main)" = "$EXPECTED_SHA"',
        exact_tip,
    )
    materialize = root_broker.index(
        '"${GIT[@]}" show '
        '"${EXPECTED_SHA}:deploy/production_deploy.sh"',
        fetched_tip,
    )
    digest = root_broker.index(
        "trusted deploy engine digest differs", materialize
    )
    protocol = root_broker.index(
        'PROBIGA_DEPLOY_PROTOCOL_VERSION="$DEPLOY_PROTOCOL_VERSION"',
        digest,
    )
    launch = root_broker.index(
        '/usr/bin/bash --noprofile --norc "$BOOTSTRAP_FILE"',
        protocol,
    )
    assert remote_tip < exact_tip < fetched_tip < materialize < digest
    assert digest < protocol < launch
    assert 'EXPECTED_SHA="$EXPECTED_SHA"' in root_broker[protocol:launch]


def test_example_environment_documents_key_only_production_verification():
    root = Path(__file__).resolve().parents[1]
    example = (root / ".env.example").read_text(encoding="utf-8")

    assert "PROBIGA_REMOTE_SSH_HOST=" in example
    assert "PROBIGA_REMOTE_SSH_USER=" in example
    assert "PROBIGA_REMOTE_SSH_KEY_FILE=" in example
    assert "PROBIGA_SSH_KNOWN_HOSTS=" in example
    assert "PROBIGA_MANUAL_PRODUCTION_DEPLOY_ENABLED=0" in example
