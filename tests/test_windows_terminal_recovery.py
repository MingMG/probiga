from contextlib import contextmanager
import io
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest import mock
import tempfile

from integrations import windows_terminal_recovery as recovery


class _Driver:
    def __init__(self, status="login_required", *, after_start="login_required", revision=100):
        self.status, self.after_start, self.revision = status, after_start, revision
        self.events = []
        self.held = False
        self.error = None
        self.pre_submit_error = None
        self.target = SimpleNamespace(identity="exact-process")

    @contextmanager
    def recovery_lock(self, name):
        assert name == recovery.MUTEX_NAME
        assert not self.held
        self.held = True
        self.events.append("lock")
        try:
            yield
        finally:
            self.held = False
            self.events.append("unlock")

    def observe(self):
        assert self.held
        self.events.append("observe")
        return SimpleNamespace(status=self.status, identity="exact-process")

    def start_terminal(self):
        assert self.held
        self.events.append("start")

    def wait_for_login_window(self):
        self.events.append("wait_window")
        return SimpleNamespace(status=self.after_start, identity="exact-process")

    def prepare_login(self, current):
        assert self.held
        self.events.append("prepare")
        return self.target

    def credential_revision(self):
        self.events.append("revision")
        return self.revision

    def login(self, target, *, expected_credential_revision, before_submit):
        assert self.held
        assert expected_credential_revision == self.revision
        self.events.append("login")
        if self.pre_submit_error:
            raise self.pre_submit_error
        before_submit()
        if self.error:
            raise self.error

    def wait_for_recovered_bridge(self, target):
        assert not self.held, "the formal model reloader must acquire the mutex itself"
        self.events.append("bridge")

    def confirm_logged_in(self, target):
        assert self.held
        self.events.append("confirm")


class RecoveryStateTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.state = recovery._LoginState(Path(self.directory.name) / "state.json")

    def test_online_never_starts_reads_credentials_or_logs_in(self):
        driver = _Driver("logged_in")
        self.state.begin(100)
        self.assertIs(recovery._recover_session(driver, self.state), False)
        self.assertEqual(driver.events, ["lock", "observe", "unlock"])
        self.assertIsNone(self.state.read())

    def test_login_records_attempt_at_submit_and_unlocks_before_model_recovery(self):
        driver = _Driver()
        original = driver.login
        def login(*args, **kwargs):
            self.assertIsNone(self.state.read())
            original(*args, **kwargs)
            self.assertEqual(self.state.read()["code"], "QMT_LOGIN_ATTEMPT_UNRESOLVED")
        driver.login = login
        self.assertIs(recovery._recover_session(driver, self.state), True)
        self.assertEqual(driver.events, ["lock", "observe", "prepare", "revision", "login", "unlock", "bridge", "lock", "confirm", "unlock"])
        self.assertIsNone(self.state.read())

    def test_absent_terminal_uses_independent_start_then_login(self):
        driver = _Driver("absent")
        self.assertTrue(recovery._recover_session(driver, self.state))
        self.assertEqual(driver.events[1:5], ["observe", "start", "wait_window", "prepare"])

    def test_vendor_auto_login_after_start_still_validates_bridge_and_returns_true(self):
        driver = _Driver("absent", after_start="logged_in")
        self.assertIs(recovery._recover_session(driver, self.state), True)
        self.assertNotIn("prepare", driver.events)
        self.assertNotIn("revision", driver.events)
        self.assertNotIn("login", driver.events)
        self.assertIn("bridge", driver.events)

    def test_rejected_credential_stops_future_collectors_and_pid_restarts(self):
        first = _Driver()
        first.error = recovery.QmtTerminalRecoveryError("QMT_LOGIN_REJECTED_OR_VERIFICATION_REQUIRED")
        with self.assertRaisesRegex(recovery.QmtTerminalRecoveryError, "^QMT_LOGIN_REJECTED_OR_VERIFICATION_REQUIRED$"):
            recovery._recover_session(first, self.state)
        second = _Driver("absent")
        with self.assertRaisesRegex(recovery.QmtTerminalRecoveryError, "^QMT_LOGIN_REJECTED_OR_VERIFICATION_REQUIRED$"):
            recovery._recover_session(second, self.state)
        self.assertNotIn("login", second.events)
        self.assertNotIn("bridge", second.events)

    def test_unknown_input_outcome_persists_stop_until_verified_new_credential(self):
        first = _Driver()
        first.error = recovery.QmtTerminalRecoveryError("QMT_FOCUS_CHANGED")
        with self.assertRaises(recovery.QmtTerminalRecoveryError):
            recovery._recover_session(first, self.state)
        with self.assertRaisesRegex(recovery.QmtTerminalRecoveryError, "^QMT_LOGIN_ATTEMPT_UNRESOLVED$"):
            recovery._recover_session(_Driver(), self.state)
        self.assertTrue(recovery._recover_session(_Driver(revision=101), self.state))

    def test_pre_submit_focus_failure_does_not_block_future_authentication(self):
        first = _Driver()
        first.pre_submit_error = recovery.QmtTerminalRecoveryError("QMT_FOCUS_CHANGED")
        with self.assertRaises(recovery.QmtTerminalRecoveryError):
            recovery._recover_session(first, self.state)
        self.assertIsNone(self.state.read())
        self.assertTrue(recovery._recover_session(_Driver(), self.state))

    def test_state_write_failure_prevents_submit_and_bridge_recovery(self):
        driver = _Driver()
        with mock.patch.object(self.state, "begin", side_effect=recovery.QmtTerminalRecoveryError("QMT_LOGIN_STATE_WRITE_FAILED")):
            with self.assertRaises(recovery.QmtTerminalRecoveryError):
                recovery._recover_session(driver, self.state)
        self.assertNotIn("bridge", driver.events)

    def test_malformed_state_never_silently_unlocks_authentication(self):
        invalid = [[], {}, {"schema_version": True, "credential_revision": 1, "code": "QMT_LOGIN_ATTEMPT_UNRESOLVED"},
                   {"schema_version": 1, "credential_revision": True, "code": "QMT_LOGIN_ATTEMPT_UNRESOLVED"},
                   {"schema_version": 1, "credential_revision": 1, "code": "arbitrary provider text"}]
        for value in invalid:
            with self.subTest(kind=type(value).__name__):
                self.state.path.write_text(json.dumps(value), encoding="utf-8")
                driver = _Driver()
                with self.assertRaisesRegex(recovery.QmtTerminalRecoveryError, "^QMT_LOGIN_STATE_INVALID$"):
                    recovery._recover_session(driver, self.state)
                self.assertNotIn("login", driver.events)

    def test_final_identity_change_keeps_durable_attempt(self):
        driver = _Driver()
        driver.confirm_logged_in = mock.Mock(side_effect=recovery.QmtTerminalRecoveryError("QMT_WINDOW_CHANGED"))
        with self.assertRaises(recovery.QmtTerminalRecoveryError):
            recovery._recover_session(driver, self.state)
        self.assertEqual(self.state.read()["code"], "QMT_LOGIN_ATTEMPT_UNRESOLVED")

    def test_state_contains_only_revision_and_fixed_code(self):
        self.state.begin(123456)
        value = json.loads(self.state.path.read_text(encoding="utf-8"))
        self.assertEqual(set(value), {"schema_version", "credential_revision", "code"})
        self.assertFalse(any(self.state.path.parent.glob(".qmt-login-state-*")))

    def test_public_exception_and_output_never_expose_native_exception_detail(self):
        output = io.StringIO()
        hidden = "synthetic confidential value"
        with mock.patch.object(recovery.sys, "platform", "win32"), \
                mock.patch("integrations.windows_qmt_login.WindowsQmtLoginDriver", side_effect=RuntimeError(hidden)), \
                mock.patch("sys.stdout", output), mock.patch("sys.stderr", output):
            with self.assertRaises(recovery.QmtTerminalRecoveryError) as failure:
                recovery.recover_qmt_session_after_failure()
        self.assertEqual(str(failure.exception), "QMT_RECOVERY_INTERNAL_ERROR")
        self.assertEqual(output.getvalue(), "")
        self.assertNotIn(hidden, str(failure.exception))
        self.assertFalse(recovery._RECOVERING.get())

    def test_public_reentrant_recovery_is_refused(self):
        token = recovery._RECOVERING.set(True)
        try:
            with mock.patch.object(recovery.sys, "platform", "win32"):
                with self.assertRaisesRegex(recovery.QmtTerminalRecoveryError, "^QMT_RECOVERY_REENTRANT$"):
                    recovery.recover_qmt_session_after_failure()
        finally:
            recovery._RECOVERING.reset(token)


if __name__ == "__main__":
    unittest.main()
