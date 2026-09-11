import ctypes
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest import mock

from integrations import windows_qmt_login as login
from integrations.windows_terminal_recovery import QmtTerminalRecoveryError as Error


class DriverTests(unittest.TestCase):
    def driver(self):
        value = login.WindowsQmtLoginDriver.__new__(login.WindowsQmtLoginDriver)
        value.native = mock.Mock()
        value.submitted_at = 1000.0
        return value

    def test_installed_driver_is_stdlib_and_native_input_abi_is_correct(self):
        self.assertEqual(ctypes.sizeof(login._INPUT), 40)
        self.assertEqual(ctypes.sizeof(login._KEYBDINPUT), 24)

    def test_online_observation_does_not_activate_read_credentials_or_start(self):
        driver = self.driver()
        identity = login._ProcessIdentity(10, 20, 1)
        driver.native.process_ids.return_value = [10]
        driver.native.identity.return_value = identity
        driver.native.windows.return_value = [30]
        driver.native.class_name.return_value = "Qt5QWindowIcon"
        driver.native.title.return_value = "12345 - " + login.QMT_TITLE
        with mock.patch.object(driver, "_check_installation"):
            observed = driver.observe()
        self.assertEqual(observed.status, "logged_in")
        self.assertEqual(observed.identity, identity)
        driver.native.user.SetForegroundWindow.assert_not_called()
        driver.native.read_credential.assert_not_called()

    def test_unknown_visible_qt_modal_stops_without_reading_its_contents(self):
        driver = self.driver()
        driver.native.process_ids.return_value = [10]
        driver.native.identity.return_value = login._ProcessIdentity(10, 20, 1)
        driver.native.windows.return_value = [30, 40]
        driver.native.class_name.return_value = "Qt5QWindowIcon"
        driver.native.title.side_effect = [login.QMT_TITLE, "unknown dialog"]
        with mock.patch.object(driver, "_check_installation"):
            with self.assertRaisesRegex(Error, "^QMT_MODAL_PRESENT$"):
                driver.observe()
        driver.native.read_credential.assert_not_called()

    def test_duplicate_processes_are_not_resolved_by_killing_a_client(self):
        driver = self.driver()
        driver.native.process_ids.return_value = [10, 11]
        with self.assertRaisesRegex(Error, "^QMT_CLIENT_NOT_UNIQUE$"):
            driver.observe()
        driver.native.read_credential.assert_not_called()

    def test_absent_start_uses_only_exact_explorer_launch(self):
        driver = self.driver()
        driver.native.process_ids.return_value = []
        with mock.patch.object(driver, "_check_installation"), \
                mock.patch("integrations.windows_explorer_launch.launch_qmt_via_explorer") as launch:
            driver.start_terminal()
        launch.assert_called_once_with(login.QMT_EXE)

    def test_supervisor_start_race_is_rechecked_before_explorer_launch(self):
        driver = self.driver()
        driver.native.process_ids.return_value = [42]
        with mock.patch.object(driver, "_check_installation"), \
                mock.patch("integrations.windows_explorer_launch.launch_qmt_via_explorer") as launch:
            driver.start_terminal()
        driver.native.identity.assert_called_once_with(42)
        launch.assert_not_called()

    def test_guard_rejects_foreground_change_before_input(self):
        driver = self.driver()
        identity = login._ProcessIdentity(10, 20, 1)
        target = login._LoginTarget(identity, 30, (100, 100, 724, 543))
        driver.native.identity.return_value = identity
        driver.native.owner.return_value = 10
        driver.native.title.return_value = login.QMT_TITLE
        driver.native.rect.return_value = target.rect
        driver.native.user.GetForegroundWindow.return_value = 99
        with self.assertRaisesRegex(Error, "^QMT_FOREGROUND_CHANGED$"):
            driver._guard(target)
        driver.native.replace_text.assert_not_called()

    def test_last_input_change_between_caret_and_password_prevents_password_send(self):
        driver = self.driver()
        target = mock.Mock()
        profile = SimpleNamespace(account=login.Rect(192, 237, 432, 268),
                                  password=login.Rect(192, 272, 432, 303),
                                  login_button=login.Rect(192, 331, 288, 362))
        driver.native.read_credential.return_value = login._Credential("fixture-account", "fixture-password", 5)
        driver.native.last_input_tick.side_effect = [10, 21]
        with mock.patch.object(driver, "_profile", return_value=profile), \
                mock.patch.object(driver, "_guard"), \
                mock.patch.object(driver, "_focus_field", side_effect=[10, 20]):
            with self.assertRaisesRegex(Error, "^QMT_FOCUS_CHANGED$"):
                driver.login(target, expected_credential_revision=5, before_submit=mock.Mock())
        self.assertEqual(driver.native.replace_text.call_count, 1)
        driver.native.click.assert_not_called()

    def test_unverified_password_caret_never_sends_password_or_submits(self):
        driver = self.driver()
        profile = SimpleNamespace(account=login.Rect(192, 237, 432, 268),
                                  password=login.Rect(192, 272, 432, 303))
        driver.native.read_credential.return_value = login._Credential("fixture-account", "fixture-password", 5)
        driver.native.last_input_tick.return_value = 10
        with mock.patch.object(driver, "_profile", return_value=profile), \
                mock.patch.object(driver, "_guard"), \
                mock.patch.object(driver, "_focus_field", side_effect=[10, Error("QMT_CARET_NOT_VERIFIED")]):
            with self.assertRaisesRegex(Error, "^QMT_CARET_NOT_VERIFIED$"):
                driver.login(mock.Mock(), expected_credential_revision=5, before_submit=mock.Mock())
        self.assertEqual(driver.native.replace_text.call_count, 1)
        driver.native.click.assert_not_called()

    def test_credential_revision_race_stops_before_any_text_input(self):
        driver = self.driver()
        driver.native.read_credential.return_value = login._Credential("fixture-account", "fixture-password", 6)
        with mock.patch.object(driver, "_profile"), mock.patch.object(driver, "_focus_field", return_value=1):
            with self.assertRaisesRegex(Error, "^QMT_CREDENTIAL_CHANGED$"):
                driver.login(mock.Mock(), expected_credential_revision=5, before_submit=mock.Mock())
        driver.native.replace_text.assert_not_called()

    def test_submit_is_persisted_after_field_guards_and_progress_gets_no_more_input(self):
        driver = self.driver()
        identity = login._ProcessIdentity(10, 20, 1)
        target = login._LoginTarget(identity, 30, (100, 100, 724, 543))
        profile = SimpleNamespace(account=login.Rect(192, 237, 432, 268),
                                  password=login.Rect(192, 272, 432, 303),
                                  login_button=login.Rect(192, 331, 288, 362))
        driver.native.read_credential.return_value = login._Credential("fixture-account", "fixture-password", 5)
        driver.native.last_input_tick.return_value = 10
        events = []
        driver.native.replace_text.side_effect = lambda unused: events.append("field")
        driver.native.click.side_effect = lambda *unused: events.append("click")
        with mock.patch.object(driver, "_profile", return_value=profile), \
                mock.patch.object(driver, "_guard"), mock.patch.object(driver, "_focus_field", return_value=10), \
                mock.patch.object(driver, "observe", side_effect=[Error("QMT_MODAL_PRESENT"), login.Observation("logged_in", identity)]), \
                mock.patch.object(login.time, "sleep"):
            driver.login(target, expected_credential_revision=5, before_submit=lambda: events.append("persist"))
        self.assertEqual(events, ["field", "field", "persist", "click"])

    def test_failed_attempt_persistence_prevents_login_button_click(self):
        driver = self.driver()
        profile = SimpleNamespace(account=login.Rect(192, 237, 432, 268),
                                  password=login.Rect(192, 272, 432, 303),
                                  login_button=login.Rect(192, 331, 288, 362))
        driver.native.read_credential.return_value = login._Credential("fixture-account", "fixture-password", 5)
        driver.native.last_input_tick.return_value = 10
        with mock.patch.object(driver, "_profile", return_value=profile), mock.patch.object(driver, "_guard"), \
                mock.patch.object(driver, "_focus_field", return_value=10):
            with self.assertRaisesRegex(Error, "^QMT_LOGIN_STATE_WRITE_FAILED$"):
                driver.login(mock.Mock(), expected_credential_revision=5,
                             before_submit=mock.Mock(side_effect=Error("QMT_LOGIN_STATE_WRITE_FAILED")))
        self.assertEqual(driver.native.replace_text.call_count, 2)
        driver.native.click.assert_not_called()

    def test_fresh_bridge_needs_same_pid_and_post_login_timestamp(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            path = home / "userdata" / "probiga_bridge" / "heartbeat.json"
            path.parent.mkdir(parents=True)
            driver = self.driver()
            target = SimpleNamespace(identity=login._ProcessIdentity(10, 20, 1))
            valid = {"pid": 10, "source": "gj_big_qmt_inner", "status": "running",
                     "updated_ts": 1001.0, "model_instance_id": "fixture-model"}
            with mock.patch.object(login, "QMT_HOME", home), mock.patch.object(login.time, "time", return_value=1002.0):
                for update in ({"pid": 11}, {"updated_ts": 999}, {"pid": True}, {"updated_ts": True}, {"status": "stopped"}):
                    path.write_text(json.dumps(valid | update), encoding="utf-8")
                    self.assertFalse(driver._fresh_bridge(target))
                path.write_text("[]", encoding="utf-8")
                self.assertFalse(driver._fresh_bridge(target))
                path.write_text(json.dumps(valid), encoding="utf-8")
                self.assertTrue(driver._fresh_bridge(target))

    def test_missing_bridge_invokes_formal_recovery_and_revalidates(self):
        driver = self.driver()
        with mock.patch.object(driver, "confirm_logged_in") as confirm, \
                mock.patch.object(driver, "_fresh_bridge", side_effect=[False, True]), \
                mock.patch.object(driver, "_recover_model") as recover:
            driver.wait_for_recovered_bridge(mock.sentinel.target)
        recover.assert_called_once_with(mock.sentinel.target)
        self.assertEqual(confirm.call_count, 2)

    def test_existing_fresh_bridge_does_not_reload_model(self):
        driver = self.driver()
        with mock.patch.object(driver, "confirm_logged_in"), \
                mock.patch.object(driver, "_fresh_bridge", return_value=True), \
                mock.patch.object(driver, "_recover_model") as recover:
            driver.wait_for_recovered_bridge(mock.sentinel.target)
        recover.assert_not_called()

    def test_formal_reloader_is_exact_frozen_build_and_receipt_bound(self):
        driver = self.driver()
        target = SimpleNamespace(identity=login._ProcessIdentity(10, 20, 1))
        build = "a" * 40
        receipt = {"schema": "probiga.bigqmt-ui-release-reload.v1", "status": "COLD_START_COMPLETE",
                   "expected_build_sha": build, "qmt_client_pid": 10, "database_writes": False,
                   "authentication_attempted": False, "automatic_order_submission": False,
                   "direct_python_strategy_execution": False}
        result = SimpleNamespace(returncode=0, stdout=json.dumps(receipt).encode(), stderr=b"")
        with mock.patch.object(driver, "confirm_logged_in"), \
                mock.patch.dict(login.os.environ, {"PROBIGA_BUILD_COMMIT_SHA": build}), \
                mock.patch.object(login.subprocess, "run", return_value=result) as run:
            driver._recover_model(target)
            arguments = run.call_args.args[0]
            self.assertEqual(arguments[-1], "-ColdStartRecovery")
            self.assertEqual(arguments[arguments.index("-ExpectedBuildSha") + 1], build)
            self.assertEqual(arguments[arguments.index("-RegisteredRoot") + 1], str(login.ROOT))
            self.assertTrue(run.call_args.kwargs["capture_output"])
            result.stdout = json.dumps(receipt | {"qmt_client_pid": 11}).encode()
            with self.assertRaisesRegex(Error, "^QMT_BRIDGE_RECOVERY_FAILED$"):
                driver._recover_model(target)


class NativeIdentityTests(unittest.TestCase):
    def native(self, *, equal=True, denied=False):
        native = login._Native.__new__(login._Native)
        native.kernel = mock.Mock()
        native.kernel.GetCurrentProcess.return_value = 99
        buffers = []
        def open_token(process, access, output):
            self.assertEqual(access, 8)
            ctypes.cast(output, ctypes.POINTER(login.w.HANDLE))[0] = process + 100
            return not denied
        def get_token(token, kind, buffer, size, output):
            self.assertEqual(kind, 1)
            ctypes.cast(output, ctypes.POINTER(login.w.DWORD))[0] = 64
            if buffer is None:
                return False
            buffers.append(buffer)
            ctypes.cast(buffer, ctypes.POINTER(ctypes.c_void_p))[0] = ctypes.addressof(buffer) + 16
            return True
        native.advapi = SimpleNamespace(OpenProcessToken=open_token, GetTokenInformation=get_token,
                                        EqualSid=mock.Mock(return_value=equal))
        return native, buffers

    def test_same_user_token_is_verified_and_native_buffers_released(self):
        native, buffers = self.native()
        native._require_current_user(42)
        native.advapi.EqualSid.assert_called_once()
        self.assertEqual(native.kernel.CloseHandle.call_count, 2)
        self.assertTrue(all(not any(bytes(buffer)) for buffer in buffers))

    def test_different_user_in_same_session_is_rejected(self):
        native, _ = self.native(equal=False)
        with self.assertRaisesRegex(Error, "^QMT_CLIENT_IDENTITY_INVALID$"):
            native._require_current_user(42)
        self.assertEqual(native.kernel.CloseHandle.call_count, 2)

    def test_inaccessible_token_never_weakens_user_check(self):
        native, _ = self.native(denied=True)
        with self.assertRaisesRegex(Error, "^QMT_CLIENT_ACCESS_DENIED$"):
            native._require_current_user(42)
        native.advapi.EqualSid.assert_not_called()

    def test_incomplete_process_enumeration_does_not_report_terminal_absent(self):
        native = login._Native.__new__(login._Native)
        native.kernel = mock.Mock()
        native.kernel.CreateToolhelp32Snapshot.return_value = 42
        native.kernel.Process32FirstW.return_value = False
        with mock.patch.object(login.ctypes, "get_last_error", return_value=5):
            with self.assertRaisesRegex(Error, "^QMT_CLIENT_ACCESS_DENIED$"):
                native.process_ids()
        native.kernel.CloseHandle.assert_called_once_with(42)


class NativeCredentialTests(unittest.TestCase):
    def api(self, **changes):
        password = "synthetic-only-password"
        raw = password.encode("utf-16-le")
        blob = (login.w.BYTE * len(raw)).from_buffer_copy(raw)
        entry = login._CREDENTIAL()
        entry.type, entry.persist = 1, 2
        entry.target, entry.comment = login._CREDENTIAL_TARGET, login._CREDENTIAL_COMMENT
        entry.username = "synthetic-only-account"
        entry.written.dwLowDateTime = 77
        entry.blob, entry.blob_size = ctypes.cast(blob, ctypes.POINTER(login.w.BYTE)), len(raw)
        for key, value in changes.items():
            setattr(entry, key, value)
        calls = []
        def read(target, kind, flags, output):
            calls.append((target, kind, flags))
            ctypes.cast(output, ctypes.POINTER(ctypes.POINTER(login._CREDENTIAL)))[0] = ctypes.pointer(entry)
            return True
        def free(pointer):
            self.assertFalse(any(blob))
            calls.append("free")
        native = login._Native.__new__(login._Native)
        native.advapi = SimpleNamespace(CredReadW=read, CredFree=free)
        return native, calls, entry, blob

    def test_fixed_target_and_native_blob_wiped_before_free(self):
        native, calls, _, _ = self.api()
        credential = native.read_credential()
        self.assertEqual(credential.revision, 77)
        self.assertEqual(calls, [("ProBigA/AppLogin/qmt", 1, 0), "free"])
        self.assertNotIn("synthetic-only", repr(credential))

    def test_revision_does_not_decode_password(self):
        native, _, entry, blob = self.api()
        blob[0], blob[1] = 0, 0xD8  # Invalid unpaired UTF-16 surrogate.
        self.assertEqual(native.read_credential(revision_only=True), 77)

    def test_metadata_rejects_other_targets_types_or_aliases(self):
        for changes in ({"target": "ProBigA/AppLogin/myquant"}, {"type": 2}, {"persist": 3},
                        {"alias": "alias"}, {"comment": "unknown"}, {"flags": 1}):
            native, _, _, _ = self.api(**changes)
            with self.subTest(field=next(iter(changes))):
                with self.assertRaisesRegex(Error, "^QMT_CREDENTIAL_INVALID$"):
                    native.read_credential()

    def test_control_characters_cannot_be_sent_as_credential_input(self):
        native, _, _, _ = self.api(username="fixture\taccount")
        with self.assertRaisesRegex(Error, "^QMT_CREDENTIAL_INVALID$"):
            native.read_credential()


class NativeInputTests(unittest.TestCase):
    def test_literal_unicode_is_one_bounded_batch_and_input_memory_is_zeroed(self):
        native = login._Native.__new__(login._Native)
        captured = []
        arrays = []
        def send(count, values, size):
            arrays.append(values)
            captured.extend((values[i].type, values[i].value.keyboard.vk,
                             values[i].value.keyboard.scan, values[i].value.keyboard.flags) for i in range(count))
            return count
        native.user = SimpleNamespace(GetAsyncKeyState=lambda key: 0, SendInput=send)
        native.replace_text("a+^中")
        self.assertEqual(len(arrays), 1)
        self.assertTrue(all(kind == 1 and vk == 0 and flags in {4, 6} for kind, vk, scan, flags in captured[4:]))
        self.assertFalse(any(bytes(arrays[0])))

    def test_partial_input_releases_modifiers_and_mouse_without_retrying_secret(self):
        native = login._Native.__new__(login._Native)
        sizes = []
        def send(count, values, size):
            sizes.append(count)
            return 1 if len(sizes) == 1 else count
        native.user = SimpleNamespace(GetAsyncKeyState=lambda key: 0, SendInput=send)
        with self.assertRaisesRegex(Error, "^QMT_INPUT_FAILED$"):
            native.replace_text("fixture")
        self.assertEqual(sizes[1:], [3])


if __name__ == "__main__":
    unittest.main()
