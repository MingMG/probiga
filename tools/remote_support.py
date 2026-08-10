# -*- coding: utf-8 -*-
"""Shared helpers for ad-hoc remote maintenance scripts."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, NoReturn


DEFAULT_REMOTE_SSH_HOST = "47.113.123.190"
DEFAULT_REMOTE_SSH_USER = "root"
DEFAULT_REMOTE_ROOT = "/opt/ProBigA"
DEFAULT_SSH_CONNECT_TIMEOUT_SECONDS = 30
DEFAULT_SSH_AUTH_TIMEOUT_SECONDS = 30
DEFAULT_SSH_BANNER_TIMEOUT_SECONDS = 30


class UnsafeRemoteRuntimeError(RuntimeError):
    """Raised when an old remote launcher lacks a pinned runtime identity."""


class UnsafeProductionSshError(RuntimeError):
    """Raised before an unsafe production SSH connection can be attempted."""


def remote_host() -> str:
    return os.environ.get("PROBIGA_REMOTE_SSH_HOST", DEFAULT_REMOTE_SSH_HOST).strip()


def remote_user() -> str:
    return os.environ.get("PROBIGA_REMOTE_SSH_USER", DEFAULT_REMOTE_SSH_USER).strip()


def ssh_connect_kwargs(**overrides: Any) -> dict[str, Any]:
    password_override = overrides.pop("password", None)
    password = str(password_override or os.environ.get("PROBIGA_REMOTE_SSH_PASSWORD", "")).strip()
    if not password:
        raise RuntimeError("Missing PROBIGA_REMOTE_SSH_PASSWORD")
    kwargs: dict[str, Any] = {
        "hostname": remote_host(),
        "username": remote_user(),
        "password": password,
        "look_for_keys": False,
        "allow_agent": False,
        "timeout": DEFAULT_SSH_CONNECT_TIMEOUT_SECONDS,
        "auth_timeout": DEFAULT_SSH_AUTH_TIMEOUT_SECONDS,
        "banner_timeout": DEFAULT_SSH_BANNER_TIMEOUT_SECONDS,
    }
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
    client.load_system_host_keys()
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
