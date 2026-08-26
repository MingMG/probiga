# -*- coding: utf-8 -*-
"""Wait for an activated production build's complete data-readiness proof.

The deploy transaction deliberately does not run this command: the required
data jobs can take much longer than a safe code cutover.  This entrypoint is a
separate, read-only observer.  It polls the existing release-readiness SELECT
validator and exits successfully only for the exact build that is still active
on the production host.
"""
from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import stat
import time
from typing import Any
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from tools import ensure_quality_gate
from tools.env_config import create_tool_engine, load_project_env
from tools.remote_support import (
    production_ssh_client,
    production_ssh_connect_kwargs,
)


_BUILD_SHA = re.compile(r"[0-9a-f]{40}")
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_SCHEMA = "probiga.release-data-readiness-wait.v1"
_PUBLIC_STATUS_SCHEMA = "probiga.release-data-readiness-status.v1"
_PRODUCTION_ENV_FILE = Path("/opt/ProBigA/.env")
_STATUS_ROOT_TEXT = "/var/lib/probiga/release-data-readiness"
_STATUS_ROOT = Path("/var/lib/probiga/release-data-readiness")
_DEFAULT_TIMEOUT_SECONDS = 6 * 60 * 60
_MAX_TIMEOUT_SECONDS = 48 * 60 * 60
_DEFAULT_POLL_SECONDS = 60
_MAX_POLL_SECONDS = 30 * 60
_MAX_STATUS_AGE_SECONDS = 15 * 60
_PUBLIC_BASE_FIELDS = frozenset(
    {
        "schema",
        "status",
        "build_sha",
        "attempts",
        "elapsed_seconds",
        "retryable",
        "updated_at",
    }
)
_PUBLIC_READY_FIELDS = _PUBLIC_BASE_FIELDS | {
    "task_count",
    "validated_at",
    "proof_sha256",
    "qmt_strategy_input_window_sha256",
}
_PUBLIC_BLOCKED_FIELDS = _PUBLIC_BASE_FIELDS | {"reason_code", "reason_sha256"}


class ActiveReleaseChangedError(RuntimeError):
    """Raised when the observer is no longer attached to its requested build."""


class ProductionRuntimeConfigError(RuntimeError):
    """Raised when the protected service configuration boundary differs."""


def _valid_build_sha(value: object) -> str:
    build_sha = str(value or "").strip().lower()
    if _BUILD_SHA.fullmatch(build_sha) is None or build_sha == "0" * 40:
        raise ValueError("expected build SHA must be a non-zero 40-hex revision")
    return build_sha


def _validate_local_runtime_identity(
    expected_build_sha: str,
    environ: Mapping[str, str] = os.environ,
) -> None:
    """Fail closed unless this process is the exact immutable production build."""

    expected = _valid_build_sha(expected_build_sha)
    observed = str(environ.get("PROBIGA_BUILD_COMMIT_SHA") or "").strip().lower()
    pinned = str(environ.get("PROBIGA_EXPECTED_GIT_SHA") or "").strip().lower()
    mode = str(environ.get("PROBIGA_DEPLOYMENT_MODE") or "").strip().lower()
    code_root = str(environ.get("PROBIGA_CODE_ROOT") or "").strip().replace("\\", "/")
    if mode != "production":
        raise ActiveReleaseChangedError("readiness waiter is not in production mode")
    if observed != expected or pinned != expected:
        raise ActiveReleaseChangedError(
            "readiness waiter runtime build differs from requested build"
        )
    if code_root != f"/opt/ProBigA-releases/{expected}":
        raise ActiveReleaseChangedError(
            "readiness waiter is not running from the immutable build checkout"
        )


def _load_protected_production_env(
    path: Path = _PRODUCTION_ENV_FILE,
) -> None:
    """Load the existing root:service configuration without exposing it.

    Immutable release checkouts intentionally do not contain ``.env``.  The
    long-running API and scheduler read the protected legacy runtime file, so
    this service-UID observer must use that same fixed boundary instead of
    relying on the SSH deploy account's permissions.
    """

    if os.name != "posix":
        raise ProductionRuntimeConfigError(
            "protected production configuration requires a POSIX runtime"
        )
    parent = path.parent
    try:
        parent_info = parent.lstat()
        file_info = path.lstat()
    except OSError as exc:
        raise ProductionRuntimeConfigError(
            "protected production configuration is unavailable"
        ) from exc
    permitted_groups = {os.getegid(), *os.getgroups()}
    if (
        not stat.S_ISDIR(parent_info.st_mode)
        or parent_info.st_uid != 0
        or stat.S_IMODE(parent_info.st_mode) != 0o755
        or not stat.S_ISREG(file_info.st_mode)
        or file_info.st_uid != 0
        or file_info.st_gid not in permitted_groups
        or stat.S_IMODE(file_info.st_mode) != 0o640
        or file_info.st_nlink != 1
    ):
        raise ProductionRuntimeConfigError(
            "protected production configuration metadata differs"
        )
    load_project_env(path)


def _canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _public_status_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return the credential-free receipt persisted for the deploy account."""

    build_sha = _valid_build_sha(payload.get("build_sha"))
    status_value = str(payload.get("status") or "").strip().upper()
    if status_value not in {
        "NOT_READY",
        "DATA_BLOCKED",
        "READY",
        "OBSERVER_FAILED",
    }:
        raise ValueError("readiness observer status is invalid")
    attempts = payload.get("attempts")
    elapsed = payload.get("elapsed_seconds")
    retryable = payload.get("retryable")
    if (
        isinstance(attempts, bool)
        or not isinstance(attempts, int)
        or attempts < 0
        or isinstance(elapsed, bool)
        or not isinstance(elapsed, int)
        or elapsed < 0
        or not isinstance(retryable, bool)
    ):
        raise ValueError("readiness observer counters are invalid")
    result: dict[str, Any] = {
        "schema": _PUBLIC_STATUS_SCHEMA,
        "status": status_value,
        "build_sha": build_sha,
        "attempts": attempts,
        "elapsed_seconds": elapsed,
        "retryable": retryable,
        "updated_at": datetime.now(_SHANGHAI).isoformat(timespec="seconds"),
    }
    if status_value == "READY":
        proof = payload.get("proof")
        if not isinstance(proof, Mapping):
            raise ValueError("ready status has no proof")
        task_count = proof.get("task_count")
        if (
            isinstance(task_count, bool)
            or not isinstance(task_count, int)
            or task_count
            != len(ensure_quality_gate.RELEASE_DATA_READINESS_TASK_TYPES)
            or proof.get("status") != "READY"
            or proof.get("phase") != "post_activation_data_readiness"
            or str(proof.get("build_sha") or "").lower() != build_sha
        ):
            raise ValueError("ready proof identity is invalid")
        qmt_window = proof.get("qmt_strategy_input_window")
        validated_at = str(proof.get("validated_at") or "")
        try:
            parsed_validated_at = datetime.fromisoformat(validated_at)
        except ValueError as exc:
            raise ValueError("ready proof validation timestamp is invalid") from exc
        if (
            parsed_validated_at.tzinfo is not None
            or parsed_validated_at.isoformat(sep=" ", timespec="seconds")
            != validated_at
        ):
            raise ValueError("ready proof validation timestamp is invalid")
        result.update(
            {
                "task_count": task_count,
                "validated_at": validated_at,
                "proof_sha256": _canonical_sha256(proof),
                "qmt_strategy_input_window_sha256": _canonical_sha256(
                    qmt_window if isinstance(qmt_window, Mapping) else {}
                ),
            }
        )
    else:
        reason = str(payload.get("last_error") or status_value).strip()
        reason_code = (
            str(payload.get("reason_code") or "release_data_not_ready")
            .strip()
            .lower()
        )
        if re.fullmatch(r"[a-z0-9][a-z0-9_.:-]{0,63}", reason_code) is None:
            raise ValueError("readiness observer reason code is invalid")
        result.update(
            {
                "reason_code": reason_code,
                "reason_sha256": hashlib.sha256(
                    reason.encode("utf-8", errors="replace")
                ).hexdigest(),
            }
        )
    return result


class _StatusFilePublisher:
    """Publish a sanitized receipt through one root-precreated regular file."""

    def __init__(self, expected_build_sha: str, path: str | Path) -> None:
        self.build_sha = _valid_build_sha(expected_build_sha)
        self.path = Path(path)
        expected_path = _STATUS_ROOT / f"{self.build_sha}.json"
        if self.path != expected_path:
            raise ValueError("readiness status file path is not build-addressed")

    def publish(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        if os.name != "posix":
            raise RuntimeError("readiness status publication requires POSIX")
        public = _public_status_payload(payload)
        encoded = (
            json.dumps(public, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            + "\n"
        ).encode("utf-8")
        directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        nofollow = getattr(os, "O_NOFOLLOW", 0)
        directory_fd = os.open(str(_STATUS_ROOT), directory_flags | nofollow)
        file_fd = -1
        try:
            directory_info = os.fstat(directory_fd)
            if (
                not stat.S_ISDIR(directory_info.st_mode)
                or directory_info.st_uid != 0
                or stat.S_IMODE(directory_info.st_mode) != 0o755
            ):
                raise RuntimeError("readiness status directory metadata differs")
            file_fd = os.open(
                self.path.name,
                os.O_WRONLY | nofollow,
                dir_fd=directory_fd,
            )
            file_info = os.fstat(file_fd)
            if (
                not stat.S_ISREG(file_info.st_mode)
                or file_info.st_uid != os.geteuid()
                or stat.S_IMODE(file_info.st_mode) != 0o644
                or file_info.st_nlink != 1
            ):
                raise RuntimeError("readiness status file metadata differs")
            import fcntl

            fcntl.flock(file_fd, fcntl.LOCK_EX)
            os.lseek(file_fd, 0, os.SEEK_SET)
            os.ftruncate(file_fd, 0)
            offset = 0
            while offset < len(encoded):
                offset += os.write(file_fd, encoded[offset:])
            os.fsync(file_fd)
            fcntl.flock(file_fd, fcntl.LOCK_UN)
        finally:
            if file_fd >= 0:
                os.close(file_fd)
            os.close(directory_fd)
        return public


def _active_release_health(*, timeout_seconds: int = 20) -> dict[str, Any]:
    request = Request(
        "http://127.0.0.1/api/health",
        headers={"Accept": "application/json", "User-Agent": "probiga-readiness/1"},
        method="GET",
    )
    with urlopen(request, timeout=max(1, int(timeout_seconds))) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise ActiveReleaseChangedError("production health payload is invalid")
    return payload


def _assert_active_release(
    expected_build_sha: str,
    *,
    health_loader: Callable[[], Mapping[str, Any]] = _active_release_health,
) -> None:
    """Re-check active API identity before every database-readiness attempt."""

    expected = _valid_build_sha(expected_build_sha)
    payload = health_loader()
    revision = payload.get("release_revision")
    if not isinstance(revision, Mapping):
        raise ActiveReleaseChangedError("production health has no release identity")
    if (
        payload.get("status") != "ok"
        or revision.get("deployment_mode") != "production"
        or revision.get("matches_expected") is not True
        or str(revision.get("expected_git_sha") or "").strip().lower() != expected
        or str(revision.get("actual_git_sha") or "").strip().lower() != expected
    ):
        raise ActiveReleaseChangedError(
            "active production release differs from requested readiness build"
        )


def _wait_payload(
    *,
    status: str,
    build_sha: str,
    attempts: int,
    elapsed_seconds: int,
    retryable: bool,
    detail: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema": _SCHEMA,
        "status": status,
        "build_sha": build_sha,
        "attempts": attempts,
        "elapsed_seconds": max(0, int(elapsed_seconds)),
        "retryable": retryable,
        **dict(detail),
    }


def wait_for_release_data_readiness(
    engine: Any,
    *,
    expected_build_sha: str,
    timeout_seconds: int,
    poll_seconds: int,
    validator: Callable[[Any, str, datetime], dict[str, Any]] = (
        ensure_quality_gate.validate_release_data_readiness
    ),
    active_release_check: Callable[[str], None] = _assert_active_release,
    wall_clock: Callable[[], datetime] = lambda: datetime.now(_SHANGHAI).replace(
        tzinfo=None
    ),
    monotonic_clock: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
    emit: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Poll the pure-SELECT gate without joining the deployment transaction."""

    build_sha = _valid_build_sha(expected_build_sha)
    if isinstance(timeout_seconds, bool) or not 0 <= timeout_seconds <= _MAX_TIMEOUT_SECONDS:
        raise ValueError(
            f"timeout seconds must be between 0 and {_MAX_TIMEOUT_SECONDS}"
        )
    if isinstance(poll_seconds, bool) or not 1 <= poll_seconds <= _MAX_POLL_SECONDS:
        raise ValueError(
            f"poll seconds must be between 1 and {_MAX_POLL_SECONDS}"
        )
    started = monotonic_clock()
    deadline = started + timeout_seconds
    attempts = 0
    last_error = "release data has not been checked"
    while True:
        attempts += 1
        # A rollback or a newer release invalidates this observer immediately;
        # it must never report an old build READY after the active link changes.
        active_release_check(build_sha)
        try:
            proof = validator(engine, build_sha, wall_clock())
        except RuntimeError as exc:
            last_error = str(exc).strip() or type(exc).__name__
        else:
            if (
                not isinstance(proof, dict)
                or proof.get("status") != "READY"
                or str(proof.get("build_sha") or "").strip().lower() != build_sha
                or proof.get("phase") != "post_activation_data_readiness"
            ):
                raise RuntimeError("release readiness validator returned invalid proof")
            elapsed = int(max(0.0, monotonic_clock() - started))
            return _wait_payload(
                status="READY",
                build_sha=build_sha,
                attempts=attempts,
                elapsed_seconds=elapsed,
                retryable=False,
                detail={"proof": proof},
            )

        now_monotonic = monotonic_clock()
        progress = _wait_payload(
            status="NOT_READY",
            build_sha=build_sha,
            attempts=attempts,
            elapsed_seconds=int(max(0.0, now_monotonic - started)),
            retryable=True,
            detail={"last_error": last_error[:1000]},
        )
        if emit is not None:
            emit(progress)
        if now_monotonic >= deadline:
            return {
                **progress,
                "status": "DATA_BLOCKED",
            }
        sleeper(min(float(poll_seconds), max(0.0, deadline - now_monotonic)))


def _print_json(payload: Mapping[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str), flush=True)


def _run_local(args: argparse.Namespace) -> int:
    build_sha = _valid_build_sha(args.expected_build_sha)
    publisher = (
        _StatusFilePublisher(build_sha, args.status_file)
        if args.status_file
        else None
    )

    def emit(payload: dict[str, Any]) -> None:
        displayed = publisher.publish(payload) if publisher else payload
        _print_json(displayed)

    engine = None
    try:
        _validate_local_runtime_identity(build_sha)
        _load_protected_production_env()
        engine = create_tool_engine(pool_pre_ping=True)
        result = wait_for_release_data_readiness(
            engine,
            expected_build_sha=build_sha,
            timeout_seconds=args.timeout_seconds,
            poll_seconds=args.poll_seconds,
            emit=emit,
        )
    except Exception as exc:
        if publisher is not None:
            failure = _wait_payload(
                status="OBSERVER_FAILED",
                build_sha=build_sha,
                attempts=0,
                elapsed_seconds=0,
                retryable=False,
                detail={
                    "reason_code": (
                        "active_release_changed"
                        if isinstance(exc, ActiveReleaseChangedError)
                        else "observer_failed"
                    ),
                    "last_error": f"{type(exc).__name__}:{exc}",
                },
            )
            try:
                public_failure = publisher.publish(failure)
            except Exception:
                # The root-precreated receipt boundary is fail-closed.  Avoid
                # printing either the original exception or a publisher
                # traceback when its metadata no longer matches.
                return 4
            _print_json(public_failure)
        # Do not send raw database/configuration exceptions to the journal or
        # deploy account.  Exit 3 means the active build changed; exit 4 means
        # the observer itself failed.  The transient unit prevents both from
        # restarting.  Exit 0 and exit 2 are restarted after a quiet interval
        # so READY stays fresh and retryable DATA_BLOCKED can converge later.
        return 3 if isinstance(exc, ActiveReleaseChangedError) else 4
    finally:
        if engine is not None:
            engine.dispose()
    emit(result)
    return 0 if result["status"] == "READY" else 2


def _production_status_snapshot_command(expected_build_sha: str) -> str:
    """Build the deploy-user command that reads only health and public receipt."""

    build_sha = _valid_build_sha(expected_build_sha)
    script = f"""set -Eeuo pipefail
EXPECTED_SHA={shlex.quote(build_sha)}
STATUS_ROOT={shlex.quote(_STATUS_ROOT_TEXT)}
STATUS_FILE="$STATUS_ROOT/$EXPECTED_SHA.json"
HEALTH_JSON="$(curl --fail --silent --show-error --max-time 20 http://127.0.0.1/api/health)"
RECEIPT_JSON='{{}}'
if [ -e "$STATUS_ROOT" ] || [ -L "$STATUS_ROOT" ]; then
  test -d "$STATUS_ROOT"
  test ! -L "$STATUS_ROOT"
  test "$(stat -c '%U:%G' -- "$STATUS_ROOT")" = root:root
  test "$(stat -c '%a' -- "$STATUS_ROOT")" = 755
  if [ -e "$STATUS_FILE" ] || [ -L "$STATUS_FILE" ]; then
    SERVICE_USER="$(systemctl show -p User --value probiga)"
    test -n "$SERVICE_USER"
    test "$SERVICE_USER" != root
    SERVICE_GROUP="$(id -gn "$SERVICE_USER")"
    test -f "$STATUS_FILE"
    test ! -L "$STATUS_FILE"
    test "$(stat -c '%U:%G' -- "$STATUS_FILE")" = "$SERVICE_USER:$SERVICE_GROUP"
    test "$(stat -c '%a' -- "$STATUS_FILE")" = 644
    test "$(stat -c '%h' -- "$STATUS_FILE")" = 1
    RECEIPT_JSON="$(/usr/bin/flock -s "$STATUS_FILE" /usr/bin/cat -- "$STATUS_FILE")"
    [ -n "$RECEIPT_JSON" ] || RECEIPT_JSON='{{}}'
  fi
fi
printf '%s\n%s\n' "$HEALTH_JSON" "$RECEIPT_JSON"
"""
    return "/bin/bash -ceu " + shlex.quote(script)


def _parse_remote_status_snapshot(
    output: str,
    *,
    expected_build_sha: str,
    decision_time: datetime | None = None,
) -> dict[str, Any]:
    build_sha = _valid_build_sha(expected_build_sha)
    lines = str(output or "").splitlines()
    if len(lines) != 2:
        raise RuntimeError("production readiness snapshot shape is invalid")
    try:
        health = json.loads(lines[0])
        receipt = json.loads(lines[1])
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("production readiness snapshot is not JSON") from exc
    if not isinstance(health, Mapping) or not isinstance(receipt, Mapping):
        raise RuntimeError("production readiness snapshot payload is invalid")
    _assert_active_release(build_sha, health_loader=lambda: health)
    if not receipt:
        return {
            "schema": _PUBLIC_STATUS_SCHEMA,
            "status": "NOT_READY",
            "build_sha": build_sha,
            "attempts": 0,
            "elapsed_seconds": 0,
            "retryable": True,
            "reason_code": "observer_pending",
        }
    if (
        receipt.get("schema") != _PUBLIC_STATUS_SCHEMA
        or str(receipt.get("build_sha") or "").lower() != build_sha
        or str(receipt.get("status") or "")
        not in {"NOT_READY", "DATA_BLOCKED", "READY", "OBSERVER_FAILED"}
        or not isinstance(receipt.get("retryable"), bool)
        or isinstance(receipt.get("attempts"), bool)
        or not isinstance(receipt.get("attempts"), int)
        or int(receipt.get("attempts")) < 0
        or isinstance(receipt.get("elapsed_seconds"), bool)
        or not isinstance(receipt.get("elapsed_seconds"), int)
        or int(receipt.get("elapsed_seconds")) < 0
    ):
        raise RuntimeError("production readiness receipt identity is invalid")
    status_value = str(receipt["status"])
    expected_fields = (
        _PUBLIC_READY_FIELDS
        if status_value == "READY"
        else _PUBLIC_BLOCKED_FIELDS
    )
    if set(receipt) != expected_fields:
        raise RuntimeError("production readiness receipt fields differ")
    updated_raw = str(receipt.get("updated_at") or "")
    try:
        updated_at = datetime.fromisoformat(updated_raw)
    except ValueError as exc:
        raise RuntimeError("production readiness receipt timestamp is invalid") from exc
    if (
        updated_at.tzinfo is None
        or updated_at.utcoffset() != _SHANGHAI.utcoffset(updated_at)
        or updated_at.isoformat(timespec="seconds") != updated_raw
    ):
        raise RuntimeError("production readiness receipt timestamp is invalid")
    observed_at = decision_time or datetime.now(_SHANGHAI)
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=_SHANGHAI)
    age_seconds = (observed_at - updated_at).total_seconds()
    if age_seconds < -300 or age_seconds > _MAX_STATUS_AGE_SECONDS:
        raise RuntimeError("production readiness receipt is stale")
    if status_value == "READY":
        validated_at_raw = str(receipt.get("validated_at") or "")
        try:
            validated_at = datetime.fromisoformat(validated_at_raw)
        except ValueError as exc:
            raise RuntimeError(
                "production READY receipt validation timestamp is invalid"
            ) from exc
        if (
            receipt.get("retryable") is not False
            or isinstance(receipt.get("task_count"), bool)
            or not isinstance(receipt.get("task_count"), int)
            or int(receipt.get("task_count"))
            != len(ensure_quality_gate.RELEASE_DATA_READINESS_TASK_TYPES)
            or re.fullmatch(r"[0-9a-f]{64}", str(receipt.get("proof_sha256") or ""))
            is None
            or re.fullmatch(
                r"[0-9a-f]{64}",
                str(receipt.get("qmt_strategy_input_window_sha256") or ""),
            )
            is None
            or validated_at.tzinfo is not None
            or validated_at.isoformat(sep=" ", timespec="seconds")
            != validated_at_raw
        ):
            raise RuntimeError("production READY receipt proof is invalid")
    elif (
        re.fullmatch(
            r"[a-z0-9][a-z0-9_.:-]{0,63}",
            str(receipt.get("reason_code") or ""),
        )
        is None
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(receipt.get("reason_sha256") or ""),
        )
        is None
    ):
        raise RuntimeError("production readiness receipt reason is invalid")
    return dict(receipt)


def _remote_status_snapshot(client: Any, build_sha: str) -> dict[str, Any]:
    _stdin, stdout, stderr = client.exec_command(
        _production_status_snapshot_command(build_sha),
        timeout=60,
    )
    output = stdout.read().decode("utf-8", errors="replace")
    # Consume stderr so the channel cannot retain unread data, but never echo
    # remote diagnostics: only the sanitized public receipt is user-visible.
    stderr.read()
    return_code = int(stdout.channel.recv_exit_status())
    if return_code != 0:
        raise RuntimeError("production readiness receipt is unavailable")
    return _parse_remote_status_snapshot(
        output,
        expected_build_sha=build_sha,
    )


def _run_remote(args: argparse.Namespace) -> int:
    load_project_env()
    build_sha = _valid_build_sha(args.expected_build_sha)
    client = production_ssh_client()
    client.connect(**production_ssh_connect_kwargs(timeout=30))
    started = time.monotonic()
    deadline = started + args.timeout_seconds
    try:
        while True:
            try:
                receipt = _remote_status_snapshot(client, build_sha)
            except ActiveReleaseChangedError:
                blocked = {
                    "schema": _PUBLIC_STATUS_SCHEMA,
                    "status": "OBSERVER_FAILED",
                    "build_sha": build_sha,
                    "attempts": 0,
                    "elapsed_seconds": int(time.monotonic() - started),
                    "retryable": False,
                    "reason_code": "active_release_changed",
                }
                _print_json(blocked)
                return 2
            except RuntimeError:
                receipt = {
                    "schema": _PUBLIC_STATUS_SCHEMA,
                    "status": "NOT_READY",
                    "build_sha": build_sha,
                    "attempts": 0,
                    "elapsed_seconds": int(time.monotonic() - started),
                    "retryable": True,
                    "reason_code": "readiness_snapshot_unavailable",
                }
            status_value = str(receipt.get("status") or "")
            if status_value == "READY":
                _print_json(receipt)
                return 0
            if status_value == "OBSERVER_FAILED" or (
                status_value == "DATA_BLOCKED"
                and receipt.get("retryable") is not True
            ):
                _print_json(receipt)
                return 2
            _print_json(receipt)
            now_monotonic = time.monotonic()
            if now_monotonic >= deadline:
                timeout_receipt = {
                    "schema": _PUBLIC_STATUS_SCHEMA,
                    "status": "DATA_BLOCKED",
                    "build_sha": build_sha,
                    "attempts": int(receipt.get("attempts") or 0),
                    "elapsed_seconds": int(now_monotonic - started),
                    "retryable": True,
                    "reason_code": "remote_wait_timeout",
                }
                _print_json(timeout_receipt)
                return 2
            time.sleep(
                min(float(args.poll_seconds), max(0.0, deadline - now_monotonic))
            )
    finally:
        client.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--expected-build-sha",
        required=True,
        help="exact non-zero 40-hex production build that must become ready",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=_DEFAULT_TIMEOUT_SECONDS,
        help="maximum observation time; zero performs exactly one read attempt",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=_DEFAULT_POLL_SECONDS,
        help="seconds between pure-read validation attempts",
    )
    parser.add_argument(
        "--local-runtime",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--status-file",
        default="",
        help=argparse.SUPPRESS,
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if not 0 <= args.timeout_seconds <= _MAX_TIMEOUT_SECONDS:
        raise SystemExit(
            f"--timeout-seconds must be between 0 and {_MAX_TIMEOUT_SECONDS}"
        )
    if not 1 <= args.poll_seconds <= _MAX_POLL_SECONDS:
        raise SystemExit(
            f"--poll-seconds must be between 1 and {_MAX_POLL_SECONDS}"
        )
    if args.status_file and not args.local_runtime:
        raise SystemExit("--status-file is only valid for the local service observer")
    return _run_local(args) if args.local_runtime else _run_remote(args)


if __name__ == "__main__":
    raise SystemExit(main())
