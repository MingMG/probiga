# -*- coding: utf-8 -*-
"""Shared helpers for ad-hoc remote maintenance scripts."""
from __future__ import annotations

import os
from pathlib import Path, PurePosixPath
import shlex
from typing import Any, NoReturn, Sequence


DEFAULT_REMOTE_SSH_HOST = "47.113.123.190"
DEFAULT_REMOTE_SSH_USER = "root"
DEFAULT_REMOTE_ROOT = "/opt/ProBigA"
DEFAULT_SSH_CONNECT_TIMEOUT_SECONDS = 30
DEFAULT_SSH_AUTH_TIMEOUT_SECONDS = 30
DEFAULT_SSH_BANNER_TIMEOUT_SECONDS = 30
PRODUCTION_ADATA_RELEASE_ROOT = "/var/lib/probiga/release-sources/adata"
PRODUCTION_BOOTSTRAP_PYTHON = "/usr/bin/python3.14"
PRODUCTION_READ_ONLY_ENTRYPOINTS = frozenset({
    "tools/verify_trading_v3_production.py",
})


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

    The active API health document is the source of the service's actual Git
    and adata identity.  Before executing the requested read-only entrypoint,
    the command proves that the running process uses the SHA-addressed release
    virtual environment and that the adata source is the sealed, content-
    addressed tree outside the mutable repository checkout.

    This helper deliberately does not accept caller-supplied release hashes or
    a Python path.  Doing so would let a workstation verify one release while
    the service is running another.
    """

    release_root = (root or remote_root()).rstrip("/")
    release_root_path = PurePosixPath(release_root)
    if (
        not release_root.startswith("/")
        or release_root in {"", "/"}
        or release_root_path.as_posix() != release_root
        or ".." in release_root_path.parts
        or any(
            character in release_root for character in ("\x00", "\r", "\n")
        )
    ):
        raise UnsafeRemoteRuntimeError(
            "production release root must be a single absolute POSIX path"
        )
    relative = PurePosixPath(str(entrypoint))
    relative_text = relative.as_posix()
    if (
        relative.is_absolute()
        or not relative.parts
        or any(part in {"", ".", ".."} for part in relative.parts)
        or relative.suffix != ".py"
        or relative_text not in PRODUCTION_READ_ONLY_ENTRYPOINTS
    ):
        raise UnsafeRemoteRuntimeError(
            "production release entrypoint is not an approved read-only verifier"
        )
    normalized_arguments = tuple(str(value) for value in arguments)
    if any("\x00" in value for value in normalized_arguments):
        raise UnsafeRemoteRuntimeError(
            "production release arguments must not contain NUL bytes"
        )

    quoted_root = shlex.quote(release_root)
    quoted_entrypoint = shlex.quote(relative_text)
    quoted_arguments = " ".join(shlex.quote(value) for value in normalized_arguments)
    invocation = " ".join(
        value for value in ('"$ROOT/$ENTRYPOINT"', quoted_arguments) if value
    )
    identity_parser = r'''import json, os, re
payload = json.loads(os.environ["HEALTH_JSON"])
revision = payload.get("release_revision") or {}
adata = payload.get("adata_release_revision") or {}
if payload.get("status") != "ok":
    raise SystemExit("production health status is not ok")
if revision.get("deployment_mode") != "production":
    raise SystemExit("API is not running in production mode")
if revision.get("matches_expected") is not True:
    raise SystemExit("active Git revision does not match its pin")
if revision.get("code_worktree_clean") is not True:
    raise SystemExit("active production code is not clean")
if adata.get("verified") is not True or adata.get("read_only") is not True:
    raise SystemExit("active adata release is not sealed read-only evidence")
values = (
    revision.get("expected_git_sha"),
    adata.get("expected_git_sha"),
    adata.get("expected_tree_sha256"),
    adata.get("source_dir"),
)
if not isinstance(values[0], str) or re.fullmatch(r"[0-9a-f]{40}", values[0]) is None:
    raise SystemExit("active Git SHA is invalid")
if not isinstance(values[1], str) or re.fullmatch(r"[0-9a-f]{40}", values[1]) is None:
    raise SystemExit("active adata Git SHA is invalid")
if not isinstance(values[2], str) or re.fullmatch(r"[0-9a-f]{64}", values[2]) is None:
    raise SystemExit("active adata tree SHA is invalid")
if not isinstance(values[3], str) or "\n" in values[3] or "\r" in values[3]:
    raise SystemExit("active adata source path is invalid")
for value in values:
    print(value)
'''
    script = f"""set -Eeuo pipefail
ROOT={quoted_root}
ENTRYPOINT={quoted_entrypoint}
BOOTSTRAP_PYTHON={shlex.quote(PRODUCTION_BOOTSTRAP_PYTHON)}
ADATA_RUNTIME_ROOT={shlex.quote(PRODUCTION_ADATA_RELEASE_ROOT)}
RELEASE_VENV_ROOT="$ROOT/.release_venvs"
test -x "$BOOTSTRAP_PYTHON"
test "$(stat -c '%U' "$BOOTSTRAP_PYTHON")" = root
test ! -w "$BOOTSTRAP_PYTHON"
HEALTH_JSON="$(curl --fail --silent --show-error --max-time 20 http://127.0.0.1/api/health)"
mapfile -t RELEASE_IDENTITY < <(
  HEALTH_JSON="$HEALTH_JSON" "$BOOTSTRAP_PYTHON" -I - <<'PY'
{identity_parser}PY
)
test "${{#RELEASE_IDENTITY[@]}}" -eq 4
EXPECTED_SHA="${{RELEASE_IDENTITY[0]}}"
EXPECTED_ADATA_SHA="${{RELEASE_IDENTITY[1]}}"
EXPECTED_ADATA_TREE_SHA256="${{RELEASE_IDENTITY[2]}}"
ADATA_SOURCE="${{RELEASE_IDENTITY[3]}}"
[[ "$EXPECTED_SHA" =~ ^[0-9a-f]{{40}}$ ]]
[[ "$EXPECTED_ADATA_SHA" =~ ^[0-9a-f]{{40}}$ ]]
[[ "$EXPECTED_ADATA_TREE_SHA256" =~ ^[0-9a-f]{{64}}$ ]]
test "$ADATA_SOURCE" = "$ADATA_RUNTIME_ROOT/$EXPECTED_ADATA_SHA-$EXPECTED_ADATA_TREE_SHA256"
test "$ADATA_SOURCE" != "$ROOT/adata"
test "$(git -C "$ROOT" rev-parse HEAD)" = "$EXPECTED_SHA"
test -f "$ROOT/$ENTRYPOINT"
RELEASE_VENV="$RELEASE_VENV_ROOT/$EXPECTED_SHA"
test -L "$RELEASE_VENV"
RELEASE_VENV_TARGET="$(readlink -f "$RELEASE_VENV")"
case "$RELEASE_VENV_TARGET" in
  "$RELEASE_VENV_ROOT"/build-"$EXPECTED_SHA"-*) ;;
  *) echo "active release venv escaped its SHA-addressed root" >&2; exit 2 ;;
esac
test -x "$RELEASE_VENV/bin/python"
test "$(cat "$RELEASE_VENV/.probiga.gitsha")" = "$EXPECTED_SHA"
test "$(cat "$RELEASE_VENV/.adata.gitsha")" = "$EXPECTED_ADATA_SHA"
test "$(cat "$RELEASE_VENV/.adata.tree.sha256")" = "$EXPECTED_ADATA_TREE_SHA256"
MAIN_PID="$(systemctl show -p MainPID --value probiga)"
[[ "$MAIN_PID" =~ ^[1-9][0-9]*$ ]]
ACTIVE_ARGV0="$(tr '\0' '\n' < "/proc/$MAIN_PID/cmdline" | sed -n '1p')"
test "$ACTIVE_ARGV0" = "$RELEASE_VENV/bin/python"
"$BOOTSTRAP_PYTHON" -I "$ROOT/server/common/adata_release.py" verify \
  --source "$ADATA_SOURCE" --git-sha "$EXPECTED_ADATA_SHA" \
  --tree-sha256 "$EXPECTED_ADATA_TREE_SHA256" >/dev/null
cd "$ROOT"
exec env -i \
  PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
  LANG=C.UTF-8 PYTHONUTF8=1 PYTHONDONTWRITEBYTECODE=1 \
  PROBIGA_DEPLOYMENT_MODE=production \
  PROBIGA_REMOTE_ROOT="$ROOT" \
  PROBIGA_EXPECTED_GIT_SHA="$EXPECTED_SHA" \
  PROBIGA_BUILD_COMMIT_SHA="$EXPECTED_SHA" \
  PROBIGA_EXPECTED_ADATA_SHA="$EXPECTED_ADATA_SHA" \
  PROBIGA_EXPECTED_ADATA_TREE_SHA256="$EXPECTED_ADATA_TREE_SHA256" \
  PROBIGA_ADATA_SOURCE_DIR="$ADATA_SOURCE" \
  PYTHONPATH="$ADATA_SOURCE:$ROOT" \
  "$RELEASE_VENV/bin/python" {invocation}
"""
    return "/bin/bash -ceu " + shlex.quote(script)
