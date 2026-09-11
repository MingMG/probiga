from pathlib import Path
from unittest.mock import Mock

import pytest

from integrations import windows_explorer_launch as launch


def executable(tmp_path):
    path = tmp_path / "bin.x64" / "XtItClient.exe"
    path.parent.mkdir()
    path.write_bytes(b"fixture")
    return path


def test_terminal_launch_closes_desktop_reference_without_credential_arguments(tmp_path, monkeypatch):
    path = executable(tmp_path)
    desktop = Mock()
    monkeypatch.setattr(launch.sys, "platform", "win32")
    monkeypatch.setattr(launch, "_ExplorerDesktop", Mock(return_value=desktop))
    launch.launch_qmt_via_explorer(path)
    desktop.launch.assert_called_once_with(path)
    desktop.close.assert_called_once_with()


def test_failed_shell_call_is_redacted_and_releases_com(tmp_path, monkeypatch):
    path = executable(tmp_path)
    desktop = Mock()
    desktop.launch.side_effect = RuntimeError("native text must not cross this boundary")
    monkeypatch.setattr(launch.sys, "platform", "win32")
    monkeypatch.setattr(launch, "_ExplorerDesktop", Mock(return_value=desktop))
    with pytest.raises(launch.ExplorerLaunchError, match="^QMT_TERMINAL_START_FAILED$") as failure:
        launch.launch_qmt_via_explorer(path)
    assert failure.value.__suppress_context__
    desktop.close.assert_called_once_with()


@pytest.mark.parametrize("kind", ["relative", "missing", "different_exe", "different_folder", "directory"])
def test_only_qmt_executable_paths_reach_the_desktop(tmp_path, monkeypatch, kind):
    path = executable(tmp_path)
    if kind == "relative":
        path = Path("bin.x64/XtItClient.exe")
    elif kind == "missing":
        path.unlink()
    elif kind == "different_exe":
        path = path.rename(path.with_name("other.exe"))
    elif kind == "different_folder":
        path = path.rename(tmp_path / path.name)
    else:
        path.unlink()
        path.mkdir()
    factory = Mock()
    monkeypatch.setattr(launch.sys, "platform", "win32")
    monkeypatch.setattr(launch, "_ExplorerDesktop", factory)
    with pytest.raises(launch.ExplorerLaunchError, match="^QMT_TERMINAL_START_FAILED$"):
        launch.launch_qmt_via_explorer(path)
    factory.assert_not_called()


def test_cleanup_releases_interfaces_in_reverse_order_and_balances_com():
    desktop = object.__new__(launch._ExplorerDesktop)
    desktop.refs = [11, 22, 33]
    desktop.initialized = True
    desktop._call = Mock()
    desktop.ole = Mock()
    desktop.close()
    assert [call.args[:2] for call in desktop._call.call_args_list] == [(33, 2), (22, 2), (11, 2)]
    desktop.ole.CoUninitialize.assert_called_once_with()
    desktop.close()
    assert desktop._call.call_count == 3
    desktop.ole.CoUninitialize.assert_called_once_with()


def test_send_uses_current_desktop_and_fixed_non_elevated_empty_arguments(tmp_path):
    path = tmp_path / "bin.x64" / "XtItClient.exe"
    desktop = object.__new__(launch._ExplorerDesktop)
    desktop.dispatch = 123
    desktop._validate_desktop_owner = Mock()
    desktop._invoke = Mock(return_value=launch._Variant())
    desktop.auto = Mock()
    desktop.launch(path)
    desktop._validate_desktop_owner.assert_called_once_with()
    desktop._invoke.assert_called_once_with(123, "ShellExecute", 1,
                                           [str(path), "", str(path.parent), "open", 7])
    desktop.auto.VariantClear.assert_called_once()
