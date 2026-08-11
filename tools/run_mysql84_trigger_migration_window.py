#!/usr/bin/env python3
"""Run one trigger-creating migration in a guarded MySQL 8.4 window.

Oracle MySQL enables the binary log by default.  When
``log_bin_trust_function_creators`` is OFF, a schema-scoped migration account
cannot create triggers without a global administrative privilege.  This tool
keeps that global variable OFF during normal operation and changes it only for
one explicitly offline migration command.

The administrator credential is accepted only through a MySQL client option
file.  The option file, environment, and child arguments are never copied to
the JSON evidence.  This tool does not stop services or applications and does
not authorize production activation; the operator must stop business writers
before supplying the exact offline acknowledgement.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
import uuid

import pymysql


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.mysql_version_policy import (  # noqa: E402
    MYSQL_84_ISOLATED_ACCEPTANCE,
    PRODUCTION_DATABASE_ACTIVATION_ALLOWED,
    is_oracle_mysql_distribution,
    isolated_acceptance_version,
)
from tools.mysql_acceptance_tls import require_mysql_acceptance_ssl_ca  # noqa: E402


OFFLINE_ACK = "BUSINESS_WRITES_STOPPED"
NESTED_WINDOW_ENV = "PROBIGA_MYSQL84_TRIGGER_MIGRATION_WINDOW_ACTIVE"
WINDOW_LOCK_NAME = "probiga:mysql84:trigger-migration-window"
EVIDENCE_SCHEMA_VERSION = 1
SAFETY_FAILURE_EXIT_CODE = 70
PREFLIGHT_FAILURE_EXIT_CODE = 2
INTERRUPTED_EXIT_CODE = 130

_CHANGE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_PASSWORD_URL_RE = re.compile(r"://[^/@:\s]+:[^/@\s]+@")
_FORBIDDEN_CHILD_ARGUMENT_RE = re.compile(
    r"^(?:-p|--password(?:=|$)|--?(?:passwd|password-file)(?:=|$))",
    re.IGNORECASE,
)
_PRODUCTION_ACTIVATION_ARGUMENTS = frozenset(
    {
        "--activate-production",
        "--enable-production-activation",
        "--production-activation",
        "--request-production-activation",
    }
)


class WindowFailure(RuntimeError):
    """A fail-closed error whose code is safe to put in evidence."""

    def __init__(self, code: str, public_message: str) -> None:
        super().__init__(public_message)
        self.code = code
        self.public_message = public_message


@dataclass(frozen=True, slots=True)
class WindowConfig:
    admin_option_file: Path
    target_ssl_ca: Path
    expected_server_uuid: str
    expected_server_port: int
    business_offline_ack: str
    change_id: str
    production_activation_requested: bool = False


@dataclass(frozen=True, slots=True)
class ValidatedWindowConfig:
    admin_option_file: Path
    target_ssl_ca: Path
    expected_server_uuid: str
    expected_server_port: int
    change_id: str


@dataclass(frozen=True, slots=True)
class TargetState:
    server_version: str
    server_version_comment: str
    server_uuid: str
    server_port: int
    ssl_cipher: str
    log_bin: int
    binlog_format: str
    log_bin_trust_function_creators: int

    def evidence(self) -> dict[str, Any]:
        return {
            "server_version": self.server_version,
            "server_version_comment": self.server_version_comment,
            "server_uuid": self.server_uuid,
            "server_port": self.server_port,
            "ssl_cipher": self.ssl_cipher,
            "log_bin": self.log_bin,
            "binlog_format": self.binlog_format,
            "log_bin_trust_function_creators": (
                self.log_bin_trust_function_creators
            ),
        }


@dataclass(frozen=True, slots=True)
class WindowOutcome:
    exit_code: int
    evidence: dict[str, Any]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _require_absolute_file(path: Path, *, label: str) -> Path:
    if not path.is_absolute():
        raise WindowFailure(
            f"invalid_{label}", f"{label.replace('_', ' ')} must be absolute"
        )
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise WindowFailure(
            f"invalid_{label}", f"{label.replace('_', ' ')} does not exist"
        ) from exc
    if not resolved.is_file():
        raise WindowFailure(
            f"invalid_{label}", f"{label.replace('_', ' ')} must name a file"
        )
    return resolved


def _normalize_uuid(value: object) -> str:
    candidate = str(value or "").strip()
    try:
        parsed = uuid.UUID(candidate)
    except (ValueError, AttributeError) as exc:
        raise WindowFailure(
            "invalid_expected_server_uuid",
            "expected server UUID must be a canonical UUID",
        ) from exc
    canonical = str(parsed)
    if candidate.casefold() != canonical:
        raise WindowFailure(
            "invalid_expected_server_uuid",
            "expected server UUID must be a canonical UUID",
        )
    return canonical


def validate_config(config: WindowConfig) -> ValidatedWindowConfig:
    """Validate every operator assertion before opening a DB connection."""

    if PRODUCTION_DATABASE_ACTIVATION_ALLOWED:
        raise WindowFailure(
            "repository_policy_violation",
            "repository policy unexpectedly allows production activation",
        )
    if config.production_activation_requested:
        raise WindowFailure(
            "production_activation_rejected",
            "this maintenance window never authorizes production activation",
        )
    if config.business_offline_ack != OFFLINE_ACK:
        raise WindowFailure(
            "business_offline_not_acknowledged",
            f"business writers must be stopped and acknowledged as {OFFLINE_ACK}",
        )
    if not isinstance(config.expected_server_port, int) or isinstance(
        config.expected_server_port, bool
    ):
        raise WindowFailure(
            "invalid_expected_server_port", "expected server port is invalid"
        )
    if not 1 <= config.expected_server_port <= 65535:
        raise WindowFailure(
            "invalid_expected_server_port", "expected server port is invalid"
        )
    change_id = str(config.change_id or "").strip()
    if _CHANGE_ID_RE.fullmatch(change_id) is None:
        raise WindowFailure(
            "invalid_change_id",
            "change id must be a short non-secret identifier",
        )
    option_file = _require_absolute_file(
        config.admin_option_file, label="admin_option_file"
    )
    ca_file = _require_absolute_file(config.target_ssl_ca, label="target_ssl_ca")
    try:
        verified_ca = require_mysql_acceptance_ssl_ca(str(ca_file))
    except ValueError as exc:
        raise WindowFailure(
            "invalid_target_ssl_ca", "target SSL CA is invalid"
        ) from exc
    if option_file == Path(verified_ca.ssl_ca):
        raise WindowFailure(
            "credential_file_alias",
            "administrator option file and target SSL CA must be different files",
        )
    return ValidatedWindowConfig(
        admin_option_file=option_file,
        target_ssl_ca=Path(verified_ca.ssl_ca),
        expected_server_uuid=_normalize_uuid(config.expected_server_uuid),
        expected_server_port=config.expected_server_port,
        change_id=change_id,
    )


def _validate_child_command(
    command: Sequence[str], *, admin_option_file: Path
) -> tuple[str, ...]:
    if not command:
        raise WindowFailure("missing_child_command", "a child command is required")
    normalized: list[str] = []
    option_file_text = os.path.normcase(str(admin_option_file))
    for index, raw in enumerate(command):
        if not isinstance(raw, str) or not raw or "\x00" in raw:
            raise WindowFailure(
                "invalid_child_command", "child command contains an invalid argument"
            )
        candidate = raw.strip()
        lowered = candidate.casefold()
        if _PASSWORD_URL_RE.search(candidate) or _FORBIDDEN_CHILD_ARGUMENT_RE.match(
            candidate
        ):
            raise WindowFailure(
                "credential_in_child_command",
                "credentials are forbidden in child command arguments",
            )
        if os.path.normcase(candidate) == option_file_text:
            raise WindowFailure(
                "admin_credential_forwarding_rejected",
                "administrator option file must not be forwarded to the child",
            )
        option_assignment = candidate.split("=", 1)
        if len(option_assignment) == 2 and os.path.normcase(
            option_assignment[1].strip('"\'')
        ) == option_file_text:
            raise WindowFailure(
                "admin_credential_forwarding_rejected",
                "administrator option file must not be forwarded to the child",
            )
        if lowered.split("=", 1)[0] in _PRODUCTION_ACTIVATION_ARGUMENTS:
            raise WindowFailure(
                "production_activation_rejected",
                "production activation arguments are forbidden in this window",
            )
        if index == 0 and candidate.startswith("-"):
            raise WindowFailure(
                "invalid_child_executable", "child executable is invalid"
            )
        normalized.append(raw)
    return tuple(normalized)


def _connect(config: ValidatedWindowConfig) -> pymysql.Connection:
    """Connect using option-file credentials and one mandatory CA policy."""

    return pymysql.connect(
        read_default_file=str(config.admin_option_file),
        read_default_group="client",
        charset="utf8mb4",
        autocommit=True,
        connect_timeout=10,
        read_timeout=30,
        write_timeout=30,
        local_infile=False,
        ssl_ca=str(config.target_ssl_ca),
        ssl_verify_cert=True,
        ssl_verify_identity=False,
        program_name="probiga-mysql84-trigger-migration-window",
    )


def _as_binary_setting(value: object, *, name: str) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int) and value in (0, 1):
        return value
    text = str(value or "").strip().upper()
    if text in {"0", "OFF"}:
        return 0
    if text in {"1", "ON"}:
        return 1
    raise WindowFailure("invalid_target_state", f"target {name} is not binary")


def _read_target_state(connection: pymysql.Connection) -> TargetState:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT @@version, @@version_comment, @@server_uuid, @@port, "
            "@@GLOBAL.log_bin, @@GLOBAL.binlog_format, "
            "@@GLOBAL.log_bin_trust_function_creators"
        )
        row = cursor.fetchone()
        cursor.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
        ssl_row = cursor.fetchone()
    if not isinstance(row, (tuple, list)) or len(row) != 7:
        raise WindowFailure(
            "target_identity_unavailable", "target identity query was incomplete"
        )
    cipher = (
        ssl_row[1]
        if isinstance(ssl_row, (tuple, list)) and len(ssl_row) >= 2
        else ""
    )
    return TargetState(
        server_version=str(row[0] or "").strip(),
        server_version_comment=str(row[1] or "").strip(),
        server_uuid=str(row[2] or "").strip().casefold(),
        server_port=int(row[3]),
        ssl_cipher=str(cipher or "").strip(),
        log_bin=_as_binary_setting(row[4], name="log_bin"),
        binlog_format=str(row[5] or "").strip().upper(),
        log_bin_trust_function_creators=_as_binary_setting(
            row[6], name="log_bin_trust_function_creators"
        ),
    )


def _validate_target_identity(
    state: TargetState, config: ValidatedWindowConfig
) -> None:
    if (
        isolated_acceptance_version(state.server_version)
        != MYSQL_84_ISOLATED_ACCEPTANCE
        or not is_oracle_mysql_distribution(
            state.server_version, state.server_version_comment
        )
    ):
        raise WindowFailure(
            "target_version_mismatch", "target is not validated Oracle MySQL 8.4.11"
        )
    if state.server_uuid != config.expected_server_uuid:
        raise WindowFailure("target_uuid_mismatch", "target server UUID mismatch")
    if state.server_port != config.expected_server_port:
        raise WindowFailure("target_port_mismatch", "target server port mismatch")
    if not state.ssl_cipher:
        raise WindowFailure("target_tls_missing", "target connection did not use TLS")


def _validate_initial_target_state(
    state: TargetState, config: ValidatedWindowConfig
) -> None:
    _validate_target_identity(state, config)
    if state.log_bin != 1:
        raise WindowFailure("binary_log_disabled", "target binary log must be ON")
    if state.binlog_format != "ROW":
        raise WindowFailure(
            "binary_log_format_mismatch", "target binary log format must be ROW"
        )
    if state.log_bin_trust_function_creators != 0:
        raise WindowFailure(
            "trust_initially_enabled",
            "log_bin_trust_function_creators must initially be OFF",
        )


def _set_trust(connection: pymysql.Connection, *, enabled: bool) -> None:
    statement = (
        "SET GLOBAL log_bin_trust_function_creators = ON"
        if enabled
        else "SET GLOBAL log_bin_trust_function_creators = OFF"
    )
    with connection.cursor() as cursor:
        cursor.execute(statement)


def _acquire_window_lock(connection: pymysql.Connection) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT GET_LOCK(%s, 0)", (WINDOW_LOCK_NAME,))
        row = cursor.fetchone()
    return bool(isinstance(row, (tuple, list)) and row and row[0] == 1)


def _release_window_lock(connection: pymysql.Connection) -> bool:
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT RELEASE_LOCK(%s)", (WINDOW_LOCK_NAME,))
            row = cursor.fetchone()
        return bool(isinstance(row, (tuple, list)) and row and row[0] == 1)
    except Exception:
        return False


def _close_quietly(connection: pymysql.Connection | None) -> None:
    if connection is None:
        return
    try:
        connection.close()
    except Exception:
        pass


def _command_fingerprint(command: Sequence[str]) -> str:
    serialized = json.dumps(
        list(command), ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()


def _terminate_interrupted_child(process: subprocess.Popen[Any]) -> int | None:
    try:
        process.terminate()
    except Exception:
        pass
    try:
        return process.wait(timeout=10)
    except Exception:
        try:
            process.kill()
        except Exception:
            pass
        try:
            return process.wait(timeout=10)
        except Exception:
            return None


def _run_child(
    command: Sequence[str],
    *,
    environ: Mapping[str, str],
    popen_factory: Callable[..., subprocess.Popen[Any]],
) -> tuple[int | None, bool, bool]:
    child_env = dict(environ)
    child_env[NESTED_WINDOW_ENV] = "1"
    process = popen_factory(
        list(command),
        shell=False,
        env=child_env,
        close_fds=True,
    )
    try:
        return_code = process.wait()
    except KeyboardInterrupt:
        return _terminate_interrupted_child(process), True, True
    except BaseException:
        # Never relax the server setting while a child whose state is unknown
        # may still be creating triggers.
        _terminate_interrupted_child(process)
        raise
    if not isinstance(return_code, int) or isinstance(return_code, bool):
        raise WindowFailure(
            "invalid_child_exit", "child process returned an invalid exit code"
        )
    return return_code, False, True


def _safe_failure(exc: BaseException) -> tuple[str, str]:
    if isinstance(exc, WindowFailure):
        return exc.code, exc.public_message
    if isinstance(exc, pymysql.MySQLError):
        error_number = exc.args[0] if exc.args and isinstance(exc.args[0], int) else None
        suffix = f" (MySQL error {error_number})" if error_number is not None else ""
        return "mysql_operation_failed", f"MySQL maintenance operation failed{suffix}"
    if isinstance(exc, OSError):
        suffix = f" (OS error {exc.errno})" if exc.errno is not None else ""
        return "os_operation_failed", f"maintenance operation failed{suffix}"
    return "unexpected_failure", "unexpected maintenance-window failure"


def _restore_and_double_verify(
    *,
    config: ValidatedWindowConfig,
    primary: pymysql.Connection | None,
    connect_factory: Callable[[ValidatedWindowConfig], pymysql.Connection],
) -> tuple[bool, bool]:
    """Set OFF once, then verify OFF through a separate fresh connection."""

    restore_connection = primary
    owns_restore_connection = False
    primary_verified = False
    try:
        if restore_connection is None:
            restore_connection = connect_factory(config)
            owns_restore_connection = True
        before = _read_target_state(restore_connection)
        _validate_target_identity(before, config)
        _set_trust(restore_connection, enabled=False)
        after = _read_target_state(restore_connection)
        _validate_target_identity(after, config)
        primary_verified = after.log_bin_trust_function_creators == 0
    except Exception:
        primary_verified = False
    finally:
        if owns_restore_connection:
            _close_quietly(restore_connection)

    # If the original connection died after SET GLOBAL ON, recover through a
    # new exact-identity connection before performing the independent check.
    if not primary_verified:
        recovery: pymysql.Connection | None = None
        try:
            recovery = connect_factory(config)
            recovery_state = _read_target_state(recovery)
            _validate_target_identity(recovery_state, config)
            _set_trust(recovery, enabled=False)
            recovery_state = _read_target_state(recovery)
            _validate_target_identity(recovery_state, config)
            primary_verified = (
                recovery_state.log_bin_trust_function_creators == 0
            )
        except Exception:
            primary_verified = False
        finally:
            _close_quietly(recovery)

    secondary: pymysql.Connection | None = None
    secondary_verified = False
    try:
        secondary = connect_factory(config)
        final_state = _read_target_state(secondary)
        _validate_target_identity(final_state, config)
        secondary_verified = final_state.log_bin_trust_function_creators == 0
    except Exception:
        secondary_verified = False
    finally:
        _close_quietly(secondary)
    return primary_verified, secondary_verified


def execute_window(
    config: WindowConfig,
    command: Sequence[str],
    *,
    environ: Mapping[str, str] | None = None,
    connect_factory: Callable[[ValidatedWindowConfig], pymysql.Connection] = _connect,
    popen_factory: Callable[..., subprocess.Popen[Any]] = subprocess.Popen,
) -> WindowOutcome:
    """Execute one guarded window and return safe, credential-free evidence."""

    started_at = _utc_now()
    source_env = os.environ if environ is None else environ
    evidence: dict[str, Any] = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "tool": "run_mysql84_trigger_migration_window",
        "started_at_utc": started_at,
        "finished_at_utc": None,
        "change_id": None,
        "business_offline_acknowledged": False,
        "production_activation_allowed": False,
        "nested_window_detected": NESTED_WINDOW_ENV in source_env,
        "target": None,
        "named_lock_acquired": False,
        "trust_transition": {
            "enable_attempted": False,
            "enabled_verified": False,
            "restore_attempted": False,
            "restore_primary_verified": False,
            "restore_secondary_verified": False,
        },
        "child": {
            "argument_count": len(command),
            "argv_sha256": None,
            "started": False,
            "return_code": None,
            "interrupted": False,
        },
        "outcome": "preflight_failed",
        "failure": None,
    }
    validated: ValidatedWindowConfig | None = None
    admin: pymysql.Connection | None = None
    lock_acquired = False
    trust_may_be_on = False
    child_exit: int | None = None
    interrupted = False
    failure: BaseException | None = None

    try:
        if NESTED_WINDOW_ENV in source_env:
            raise WindowFailure(
                "nested_window_rejected", "nested migration windows are forbidden"
            )
        validated = validate_config(config)
        evidence["change_id"] = validated.change_id
        evidence["business_offline_acknowledged"] = True
        safe_command = _validate_child_command(
            command, admin_option_file=validated.admin_option_file
        )
        evidence["child"]["argv_sha256"] = _command_fingerprint(safe_command)

        admin = connect_factory(validated)
        identity = _read_target_state(admin)
        _validate_target_identity(identity, validated)
        if not _acquire_window_lock(admin):
            raise WindowFailure(
                "window_lock_unavailable", "another migration window is active"
            )
        lock_acquired = True
        evidence["named_lock_acquired"] = True

        # Read all global preconditions after taking the server-side lock so
        # concurrent invocations cannot both observe an OFF starting state.
        initial = _read_target_state(admin)
        _validate_initial_target_state(initial, validated)
        evidence["target"] = initial.evidence()

        evidence["trust_transition"]["enable_attempted"] = True
        trust_may_be_on = True
        _set_trust(admin, enabled=True)
        enabled = _read_target_state(admin)
        _validate_target_identity(enabled, validated)
        if enabled.log_bin_trust_function_creators != 1:
            raise WindowFailure(
                "trust_enable_verification_failed",
                "temporary trust setting did not become ON",
            )
        evidence["trust_transition"]["enabled_verified"] = True

        child_exit, interrupted, child_started = _run_child(
            safe_command,
            environ=source_env,
            popen_factory=popen_factory,
        )
        evidence["child"]["started"] = child_started
        evidence["child"]["return_code"] = child_exit
        evidence["child"]["interrupted"] = interrupted
    except KeyboardInterrupt as exc:
        interrupted = True
        evidence["child"]["interrupted"] = True
        failure = exc
    except BaseException as exc:
        failure = exc
    finally:
        if trust_may_be_on and validated is not None:
            evidence["trust_transition"]["restore_attempted"] = True
            primary_ok, secondary_ok = _restore_and_double_verify(
                config=validated,
                primary=admin,
                connect_factory=connect_factory,
            )
            evidence["trust_transition"]["restore_primary_verified"] = primary_ok
            evidence["trust_transition"]["restore_secondary_verified"] = secondary_ok
        if lock_acquired and admin is not None:
            evidence["named_lock_released"] = _release_window_lock(admin)
        else:
            evidence["named_lock_released"] = False
        _close_quietly(admin)

    restoration_required = trust_may_be_on
    restoration_ok = (
        not restoration_required
        or (
            evidence["trust_transition"]["restore_primary_verified"]
            and evidence["trust_transition"]["restore_secondary_verified"]
        )
    )
    if not restoration_ok:
        exit_code = SAFETY_FAILURE_EXIT_CODE
        evidence["outcome"] = "restoration_failed"
        evidence["failure"] = {
            "code": "trust_restore_verification_failed",
            "message": "could not prove the global trust setting is OFF",
        }
    elif interrupted:
        exit_code = INTERRUPTED_EXIT_CODE
        evidence["outcome"] = "interrupted"
        evidence["failure"] = {
            "code": "child_interrupted",
            "message": "child command was interrupted",
        }
    elif failure is not None:
        exit_code = PREFLIGHT_FAILURE_EXIT_CODE
        code, message = _safe_failure(failure)
        evidence["outcome"] = (
            "child_launch_failed"
            if evidence["trust_transition"]["enabled_verified"]
            else "preflight_failed"
        )
        evidence["failure"] = {"code": code, "message": message}
    elif child_exit is None:
        exit_code = PREFLIGHT_FAILURE_EXIT_CODE
        evidence["outcome"] = "child_launch_failed"
        evidence["failure"] = {
            "code": "missing_child_exit",
            "message": "child process did not return an exit code",
        }
    else:
        exit_code = child_exit
        evidence["outcome"] = "success" if child_exit == 0 else "child_failed"
    evidence["finished_at_utc"] = _utc_now()
    return WindowOutcome(exit_code=exit_code, evidence=evidence)


def atomic_write_evidence(
    path: Path, evidence: Mapping[str, Any], *, overwrite: bool = False
) -> Path:
    """Atomically write final evidence without ever serializing credentials."""

    if not path.is_absolute():
        raise ValueError("evidence path must be absolute")
    resolved = path.resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    if resolved.exists() and not overwrite:
        raise FileExistsError(f"evidence output already exists: {resolved}")
    partial = resolved.with_name(f".{resolved.name}.partial-{os.getpid()}")
    if partial.exists():
        raise FileExistsError(f"partial evidence output already exists: {partial}")
    try:
        with partial.open("x", encoding="utf-8", newline="\n") as stream:
            json.dump(evidence, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(partial, resolved)
    finally:
        if partial.exists():
            partial.unlink()
    return resolved


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admin-option-file", type=Path, required=True)
    parser.add_argument("--target-ssl-ca", type=Path, required=True)
    parser.add_argument("--expected-server-uuid", required=True)
    parser.add_argument("--expected-server-port", type=int, required=True)
    parser.add_argument("--business-offline-ack", required=True)
    parser.add_argument("--change-id", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--overwrite-evidence", action="store_true")
    parser.add_argument(
        "--request-production-activation",
        action="store_true",
        help="safety tripwire: this request is always rejected",
    )
    return parser


def parse_cli(
    argv: Sequence[str] | None = None,
) -> tuple[argparse.Namespace, tuple[str, ...]]:
    raw = list(sys.argv[1:] if argv is None else argv)
    if raw in (["-h"], ["--help"]):
        _parser().parse_args(raw)
    if "--" not in raw:
        _parser().error("the child command must follow a literal -- delimiter")
    delimiter = raw.index("--")
    wrapper_args = raw[:delimiter]
    command = tuple(raw[delimiter + 1 :])
    args = _parser().parse_args(wrapper_args)
    if not command:
        _parser().error("the child command after -- must not be empty")
    return args, command


def main(argv: Sequence[str] | None = None) -> int:
    args, command = parse_cli(argv)
    config = WindowConfig(
        admin_option_file=args.admin_option_file,
        target_ssl_ca=args.target_ssl_ca,
        expected_server_uuid=args.expected_server_uuid,
        expected_server_port=args.expected_server_port,
        business_offline_ack=args.business_offline_ack,
        change_id=args.change_id,
        production_activation_requested=args.request_production_activation,
    )
    evidence_path = args.evidence
    if not evidence_path.is_absolute():
        print("ERROR: evidence path must be absolute", file=sys.stderr)
        return PREFLIGHT_FAILURE_EXIT_CODE
    resolved_evidence = evidence_path.resolve()
    if resolved_evidence.exists() and not args.overwrite_evidence:
        print("ERROR: evidence output already exists", file=sys.stderr)
        return PREFLIGHT_FAILURE_EXIT_CODE

    outcome = execute_window(config, command)
    try:
        atomic_write_evidence(
            resolved_evidence,
            outcome.evidence,
            overwrite=args.overwrite_evidence,
        )
    except (OSError, ValueError) as exc:
        print(
            f"ERROR: could not atomically write evidence ({type(exc).__name__})",
            file=sys.stderr,
        )
        return SAFETY_FAILURE_EXIT_CODE
    if outcome.evidence["failure"] is not None:
        print(f"ERROR: {outcome.evidence['failure']['message']}", file=sys.stderr)
    print(f"evidence: {resolved_evidence}")
    return outcome.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
