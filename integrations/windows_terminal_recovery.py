"""Synchronous, failure-triggered QMT login recovery on the local Windows host.

The public operation owns one authentication attempt. It never schedules work,
submits an order, edits an account binding, or accepts credentials as arguments.
"""
from __future__ import annotations

import contextvars
import json
import os
from pathlib import Path
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
MUTEX_NAME = r"Local\ProBigA.BigQmtStrategyRecovery"
STATE_PATH = ROOT / "data" / "qmt_terminal_login_recovery.state.json"
_RECOVERING = contextvars.ContextVar("qmt_terminal_login_recovering", default=False)
_CODES = frozenset({
    "WINDOWS_REQUIRED", "QMT_RECOVERY_REENTRANT", "QMT_RECOVERY_LOCK_TIMEOUT",
    "QMT_RECOVERY_LOCK_FAILED", "QMT_CLIENT_NOT_UNIQUE", "QMT_CLIENT_IDENTITY_INVALID",
    "QMT_CLIENT_ACCESS_DENIED", "QMT_CLIENT_SESSION_MISMATCH", "QMT_DESKTOP_UNAVAILABLE",
    "QMT_VERSION_UNSUPPORTED", "QMT_LOGIN_LAYOUT_UNSUPPORTED", "QMT_MODAL_PRESENT",
    "QMT_FOREGROUND_CHANGED", "QMT_FOCUS_CHANGED", "QMT_WINDOW_CHANGED", "QMT_INPUT_OCCLUDED",
    "QMT_INPUT_MODIFIERS_HELD", "QMT_INPUT_FAILED", "QMT_CARET_NOT_VERIFIED",
    "QMT_CAPTURE_FAILED", "QMT_CREDENTIAL_NOT_FOUND", "QMT_CREDENTIAL_INVALID",
    "QMT_CREDENTIAL_READ_FAILED", "QMT_CREDENTIAL_CHANGED", "QMT_LOGIN_ATTEMPT_UNRESOLVED",
    "QMT_LOGIN_REJECTED_OR_VERIFICATION_REQUIRED", "QMT_LOGIN_STATE_INVALID",
    "QMT_LOGIN_STATE_WRITE_FAILED", "QMT_TERMINAL_START_FAILED", "QMT_TERMINAL_START_TIMEOUT",
    "QMT_BRIDGE_NOT_READY", "QMT_BRIDGE_RECOVERY_FAILED", "QMT_RECOVERY_INTERNAL_ERROR",
})


class QmtTerminalRecoveryError(RuntimeError):
    """Only an allowlisted, non-sensitive code crosses the recovery boundary."""

    def __init__(self, code: str):
        self.code = code if code in _CODES else "QMT_RECOVERY_INTERNAL_ERROR"
        super().__init__(self.code)


class _LoginState:
    """Persist an ambiguous/rejected attempt across collection and process retries.

    A successful manual login or a newly enrolled Credential Manager record can
    retire a previous attempt. Restarting QMT or the collector cannot do so.
    The revision is Credential Manager's LastWritten timestamp, never a secret
    digest, account name, password length, or terminal-generated error text.
    """

    def __init__(self, path: Path = STATE_PATH):
        self.path = path

    def read(self) -> dict | None:
        try:
            if not self.path.exists():
                return None
            if self.path.is_symlink() or self.path.stat().st_size > 2048:
                raise ValueError
            value = json.loads(self.path.read_text(encoding="utf-8"))
            if (
                type(value) is not dict
                or set(value) != {"schema_version", "credential_revision", "code"}
                or value["schema_version"] != 1
                or type(value["schema_version"]) is not int
                or type(value["credential_revision"]) is not int
                or not 0 < value["credential_revision"] < 2**64
                or value["code"] not in {
                    "QMT_LOGIN_ATTEMPT_UNRESOLVED",
                    "QMT_LOGIN_REJECTED_OR_VERIFICATION_REQUIRED",
                }
            ):
                raise ValueError
            return value
        except QmtTerminalRecoveryError:
            raise
        except Exception:
            raise QmtTerminalRecoveryError("QMT_LOGIN_STATE_INVALID") from None

    def begin(self, revision: int) -> None:
        self._write(revision, "QMT_LOGIN_ATTEMPT_UNRESOLVED")

    def rejected(self, revision: int) -> None:
        self._write(revision, "QMT_LOGIN_REJECTED_OR_VERIFICATION_REQUIRED")

    def _write(self, revision: int, code: str) -> None:
        temporary = None
        try:
            if type(revision) is not int or not 0 < revision < 2**64:
                raise ValueError
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if self.path.is_symlink():
                raise ValueError
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=self.path.parent,
                prefix=".qmt-login-state-", suffix=".json", delete=False,
            ) as handle:
                temporary = Path(handle.name)
                json.dump({"schema_version": 1, "credential_revision": revision, "code": code}, handle,
                          sort_keys=True, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
        except Exception:
            raise QmtTerminalRecoveryError("QMT_LOGIN_STATE_WRITE_FAILED") from None
        finally:
            if temporary is not None:
                try:
                    temporary.unlink(missing_ok=True)
                except OSError:
                    pass

    def clear(self) -> None:
        try:
            if self.path.is_symlink():
                raise ValueError
            self.path.unlink(missing_ok=True)
        except Exception:
            raise QmtTerminalRecoveryError("QMT_LOGIN_STATE_WRITE_FAILED") from None


def _recover_session(driver, state: _LoginState) -> bool:
    """The production driver supplies all observation and input operations."""
    with driver.recovery_lock(MUTEX_NAME):
        current = driver.observe()
        if current.status == "logged_in":
            # An actual authenticated window is independent evidence that a
            # previous rejected/ambiguous login was resolved by the user.
            state.clear()
            return False
        if current.status == "absent":
            driver.start_terminal()
            current = driver.wait_for_login_window()
        if current.status == "logged_in":
            target = current
        elif current.status != "login_required":
            raise QmtTerminalRecoveryError("QMT_LOGIN_LAYOUT_UNSUPPORTED")
        else:
            target = driver.prepare_login(current)
            revision = driver.credential_revision()
            previous = state.read()
            if previous is not None and previous["credential_revision"] == revision:
                raise QmtTerminalRecoveryError(previous["code"])

            try:
                # Editing either field cannot submit: input excludes control
                # characters/Enter. Persist only at the final submit boundary,
                # so a pre-submit focus/occlusion failure remains recoverable.
                driver.login(target, expected_credential_revision=revision,
                             before_submit=lambda: state.begin(revision))
            except QmtTerminalRecoveryError as exc:
                if exc.code == "QMT_LOGIN_REJECTED_OR_VERIFICATION_REQUIRED":
                    state.rejected(revision)
                raise
    # The formal model reloader acquires this same mutex in another process.
    # Release it before synchronous cold-start recovery, then reacquire before
    # clearing the durable authentication-attempt guard.
    driver.wait_for_recovered_bridge(target)
    with driver.recovery_lock(MUTEX_NAME):
        driver.confirm_logged_in(target)
        state.clear()
    return True


def recover_qmt_session_after_failure() -> bool:
    """Recover a confirmed QMT login loss once, synchronously.

    Return True only after login and a fresh heartbeat from that exact terminal
    process. Return False when the terminal is already authenticated. All other
    outcomes are fixed-code exceptions; callers retain their existing request
    identity, data validation and one-recovery retry budget.
    """
    if sys.platform != "win32":
        raise QmtTerminalRecoveryError("WINDOWS_REQUIRED")
    if _RECOVERING.get():
        raise QmtTerminalRecoveryError("QMT_RECOVERY_REENTRANT")
    token = _RECOVERING.set(True)
    try:
        from integrations.windows_qmt_login import WindowsQmtLoginDriver

        return _recover_session(WindowsQmtLoginDriver(), _LoginState())
    except QmtTerminalRecoveryError:
        raise
    except Exception:
        # Native exceptions, provider text and local variables must never leak
        # through a collection failure report or subprocess capture.
        raise QmtTerminalRecoveryError("QMT_RECOVERY_INTERNAL_ERROR") from None
    finally:
        _RECOVERING.reset(token)
