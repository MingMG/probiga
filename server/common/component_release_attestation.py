"""Read signed component identities from the permanent release ledger.

The root-owned signing key issues manifests. The public key is pinned by
privileged table metadata, so ordinary runtime DML cannot forge an identity.
"""
from __future__ import annotations

import re
from typing import Any

from sqlalchemy import text

from server.common.component_release_ledger import load_component_release_row
from server.common.scheduler_runtime_health import (
    LINUX_STANDALONE_ROLE,
    _classify_current_heartbeats,
)


def _build(value: object) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{40}", value) is None or value == "0" * 40:
        raise RuntimeError("component attestation build is invalid")
    return value


def load_component_attestation(connection: Any, linux_build_sha: str) -> dict[str, str]:
    """Read an exact build only after metadata, schema and signature proof."""
    return load_component_release_row(connection, _build(linux_build_sha))


def require_compatible_component_build(
    connection: Any, producer_build_sha: str, current_linux_build_sha: str,
) -> dict[str, str]:
    """Require two privileged identities to attest the same complete contract."""
    current = load_component_attestation(connection, current_linux_build_sha)
    producer = current if producer_build_sha == current_linux_build_sha else load_component_attestation(connection, producer_build_sha)
    if any(producer[key] != current[key] for key in (
        "windows_build_sha", "contract_build_sha", "contract_sha256",
    )):
        raise RuntimeError("component producer contract differs")
    return producer


def resolve_active_linux_component(
    connection: Any, windows_build_sha: str, *, expected_poll_seconds: int,
) -> dict[str, str]:
    """Resolve a Linux candidate only after lease and privileged identity proof.

    The caller still validates the complete Linux lease and activation receipt,
    including the instance ID and start time, against the returned actual SHA.
    """
    windows = _build(windows_build_sha)
    rows = [dict(row) for row in connection.execute(text(
        "SELECT build_sha, poll_seconds, "
        "TIMESTAMPDIFF(SECOND, heartbeat_at, NOW()) AS heartbeat_age_seconds "
        "FROM st_scheduler_runtime WHERE executor_role=:executor_role"
    ), {"executor_role": LINUX_STANDALONE_ROLE}).mappings()]
    fresh, _future, errors = _classify_current_heartbeats(rows, expected_poll_seconds=expected_poll_seconds)
    if errors or len(fresh) != 1:
        raise RuntimeError("active Linux component lease is not unique and current")
    candidate = _build(fresh[0].get("build_sha"))
    manifest = require_compatible_component_build(connection, windows, candidate)
    if manifest["windows_build_sha"] != windows or manifest["contract_build_sha"] != windows:
        raise RuntimeError("active Linux component does not authorize this Windows build")
    return load_component_attestation(connection, candidate)
