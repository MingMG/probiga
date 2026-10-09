# -*- coding: utf-8 -*-
"""Shared helpers for ad-hoc remote maintenance scripts."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, NoReturn, Sequence


DEFAULT_REMOTE_ROOT = "/opt/ProBigA"
DEFAULT_SSH_CONNECT_TIMEOUT_SECONDS = 30
DEFAULT_SSH_AUTH_TIMEOUT_SECONDS = 30
DEFAULT_SSH_BANNER_TIMEOUT_SECONDS = 30
PRODUCTION_ADATA_RELEASE_ROOT = "/var/lib/probiga/release-sources/adata"
PRODUCTION_CODE_RELEASE_ROOT = "/opt/ProBigA-releases"
PRODUCTION_CURRENT_RELEASE_LINK = "/opt/ProBigA-current"
PRODUCTION_RELEASE_VENV_ROOT = "/var/lib/probiga/release-venvs"
PRODUCTION_DEPLOY_BROKER = "/usr/local/sbin/probiga-production-deploy"
PRODUCTION_READ_ONLY_ENTRYPOINTS = frozenset({
    "tools/verify_trading_v3_production.py",
})


class UnsafeRemoteRuntimeError(RuntimeError):
    """Raised when an old remote launcher lacks a pinned runtime identity."""


class UnsafeProductionSshError(RuntimeError):
    """Raised before an unsafe production SSH connection can be attempted."""


def remote_host() -> str:
    value = os.environ.get("PROBIGA_REMOTE_SSH_HOST", "").strip()
    if not value:
        raise UnsafeProductionSshError(
            "PROBIGA_REMOTE_SSH_HOST is required for remote SSH"
        )
    return value


def remote_user() -> str:
    value = os.environ.get("PROBIGA_REMOTE_SSH_USER", "").strip()
    if not value:
        raise UnsafeProductionSshError(
            "PROBIGA_REMOTE_SSH_USER is required for remote SSH"
        )
    return value


def ssh_connect_kwargs(**overrides: Any) -> dict[str, Any]:
    if "password" in overrides:
        raise UnsafeProductionSshError(
            "SSH passwords may only come from PROBIGA_REMOTE_SSH_PASSWORD"
        )
    hostname = str(overrides.pop("hostname", None) or remote_host()).strip()
    username = str(overrides.pop("username", None) or remote_user()).strip()
    key_value = str(
        overrides.pop("key_filename", None)
        or os.environ.get("PROBIGA_REMOTE_SSH_KEY_FILE", "")
    ).strip()
    password = os.environ.get("PROBIGA_REMOTE_SSH_PASSWORD", "").strip()
    if not key_value and not password:
        raise UnsafeProductionSshError(
            "PROBIGA_REMOTE_SSH_KEY_FILE is required; legacy password "
            "authentication requires explicit PROBIGA_REMOTE_SSH_PASSWORD"
        )
    kwargs: dict[str, Any] = {
        "hostname": hostname,
        "username": username,
        "look_for_keys": False,
        "allow_agent": False,
        "timeout": DEFAULT_SSH_CONNECT_TIMEOUT_SECONDS,
        "auth_timeout": DEFAULT_SSH_AUTH_TIMEOUT_SECONDS,
        "banner_timeout": DEFAULT_SSH_BANNER_TIMEOUT_SECONDS,
    }
    if key_value:
        key_path = Path(key_value).expanduser().resolve()
        if not key_path.is_file():
            raise UnsafeProductionSshError(
                f"SSH key file does not exist: {key_path}"
            )
        kwargs["key_filename"] = str(key_path)
    else:
        kwargs["password"] = password
    kwargs.update(overrides)
    return kwargs


def production_ssh_connect_kwargs(**overrides: Any) -> dict[str, Any]:
    """Return key-only connection arguments for production maintenance.

    This deliberately does not inherit the legacy host, ``root`` user, or
    shared-password behavior above.  Production callers must name an
    unprivileged account and a concrete private key explicitly.
    """

    password_override = overrides.pop("password", None)
    if password_override is not None or os.environ.get(
        "PROBIGA_REMOTE_SSH_PASSWORD", ""
    ).strip():
        raise UnsafeProductionSshError(
            "password authentication is disabled for production SSH"
        )
    if "allow_agent" in overrides or "look_for_keys" in overrides:
        raise UnsafeProductionSshError(
            "production SSH authentication policy cannot be overridden"
        )

    hostname = str(
        overrides.pop("hostname", None)
        or os.environ.get("PROBIGA_REMOTE_SSH_HOST", "")
    ).strip()
    username = str(
        overrides.pop("username", None)
        or os.environ.get("PROBIGA_REMOTE_SSH_USER", "")
    ).strip()
    key_value = str(
        overrides.pop("key_filename", None)
        or os.environ.get("PROBIGA_REMOTE_SSH_KEY_FILE", "")
    ).strip()
    if not hostname:
        raise UnsafeProductionSshError(
            "PROBIGA_REMOTE_SSH_HOST is required for production SSH"
        )
    if not username:
        raise UnsafeProductionSshError(
            "PROBIGA_REMOTE_SSH_USER is required for production SSH"
        )
    if username.casefold() == "root":
        raise UnsafeProductionSshError(
            "root is forbidden for production SSH; use a named deploy account"
        )
    if not key_value:
        raise UnsafeProductionSshError(
            "PROBIGA_REMOTE_SSH_KEY_FILE is required for production SSH"
        )
    key_path = Path(key_value).expanduser().resolve()
    if not key_path.is_file():
        raise UnsafeProductionSshError(
            f"production SSH key file does not exist: {key_path}"
        )

    kwargs: dict[str, Any] = {
        "hostname": hostname,
        "username": username,
        "key_filename": str(key_path),
        "look_for_keys": False,
        "allow_agent": False,
        "timeout": DEFAULT_SSH_CONNECT_TIMEOUT_SECONDS,
        "auth_timeout": DEFAULT_SSH_AUTH_TIMEOUT_SECONDS,
        "banner_timeout": DEFAULT_SSH_BANNER_TIMEOUT_SECONDS,
    }
    kwargs.update(overrides)
    return kwargs


def production_ssh_client(paramiko_module: Any | None = None) -> Any:
    """Create a production SSH client pinned to an explicit known-hosts file."""

    module = paramiko_module
    if module is None:
        import paramiko as module

    known_hosts_value = os.environ.get("PROBIGA_SSH_KNOWN_HOSTS", "").strip()
    if not known_hosts_value:
        raise UnsafeProductionSshError(
            "PROBIGA_SSH_KNOWN_HOSTS is required for production SSH"
        )
    known_hosts_path = Path(known_hosts_value).expanduser().resolve()
    if not known_hosts_path.is_file():
        raise UnsafeProductionSshError(
            f"SSH known-hosts file does not exist: {known_hosts_path}"
        )

    client = module.SSHClient()
    client.load_host_keys(str(known_hosts_path))
    client.set_missing_host_key_policy(module.RejectPolicy())
    return client


def remote_root() -> str:
    return os.environ.get("PROBIGA_REMOTE_ROOT", DEFAULT_REMOTE_ROOT).rstrip("/")


def remote_pythonpath(root: str | None = None) -> NoReturn:
    """Reject the legacy mutable-checkout Python path.

    A ProBigA Git revision does not identify the separately versioned
    ``adata`` checkout.  The old helper returned ``<repo>:<repo>/adata`` and
    also paired that path with the shared ``venv`` in its callers.  That can
    execute uncommitted dependency bytes on production.  Remote jobs must be
    redesigned to consume the active release venv plus the sealed adata
    source, Git SHA, and tree SHA as one identity before this API can return a
    path again.
    """
    remote = (root or remote_root()).rstrip("/")
    raise UnsafeRemoteRuntimeError(
        "Legacy remote Python runtime is blocked: refusing to construct "
        f"PYTHONPATH from mutable checkout {remote}/adata; use the active "
        "pinned release venv and sealed adata identity."
    )


def production_release_command(
    entrypoint: str,
    arguments: Sequence[str] = (),
    *,
    root: str | None = None,
) -> str:
    """Build a fail-closed command for the *active* production release.

    The root-owned production broker is the only authority allowed to inspect
    the service process and protected runtime configuration.  It proves the
    active SHA-addressed code, venv, and adata identities before executing the
    single approved verifier as the non-root service account.

    This helper deliberately does not accept caller-supplied release hashes or
    a Python path.  Doing so would let a workstation verify one release while
    the service is running another.
    """

    current_release_link = (
        PRODUCTION_CURRENT_RELEASE_LINK if root is None else root
    )
    if current_release_link != PRODUCTION_CURRENT_RELEASE_LINK:
        raise UnsafeRemoteRuntimeError(
            "production current-release link must be the authoritative "
            f"{PRODUCTION_CURRENT_RELEASE_LINK} path"
        )
    relative_text = str(entrypoint)
    if relative_text not in PRODUCTION_READ_ONLY_ENTRYPOINTS:
        raise UnsafeRemoteRuntimeError(
            "production release entrypoint is not an approved read-only verifier"
        )
    normalized_arguments = tuple(str(value) for value in arguments)
    if normalized_arguments != ("--local-runtime",):
        raise UnsafeRemoteRuntimeError(
            "production verifier only accepts its fixed local-runtime operation"
        )
    return f"sudo -n {PRODUCTION_DEPLOY_BROKER} --verify-trading-v3"
