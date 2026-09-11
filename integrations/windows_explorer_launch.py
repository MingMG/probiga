"""Launch QMT through the existing desktop owner, outside the collector Job.

The scheduler deliberately kills its own process tree on exit. A direct Popen,
including a detached Popen, would put the terminal in that tree. Bind the actual
Explorer desktop view and ask its Shell automation object to start the terminal.
No account, password, arbitrary arguments, elevation verb or new launch service
is involved. The recovery driver owns executable validation and serialization.

The desktop binding follows Microsoft's documented implementation:
https://devblogs.microsoft.com/oldnewthing/20131118-00/?p=2643
https://devblogs.microsoft.com/oldnewthing/20130318-00/?p=4933
"""
from __future__ import annotations

import ctypes as C
from pathlib import Path
import os
import sys
import uuid


class ExplorerLaunchError(RuntimeError):
    def __init__(self):
        super().__init__("QMT_TERMINAL_START_FAILED")


class _GUID(C.Structure):
    _fields_ = [("data", C.c_ubyte * 16)]

    @classmethod
    def parse(cls, value):
        return cls.from_buffer_copy(uuid.UUID(value).bytes_le)


class _Value(C.Union):
    _fields_ = [("integer", C.c_int32), ("pointer", C.c_void_p),
                ("record", C.c_void_p * 2), ("alignment", C.c_int64)]


class _Variant(C.Structure):
    _anonymous_ = ("value",)
    _fields_ = [("vt", C.c_uint16), ("reserved", C.c_uint16 * 3), ("value", _Value)]


class _Params(C.Structure):
    _fields_ = [("arguments", C.POINTER(_Variant)), ("named", C.POINTER(C.c_int32)),
                ("count", C.c_uint32), ("named_count", C.c_uint32)]


_IID_DISPATCH = _GUID.parse("00020400-0000-0000-C000-000000000046")
_IID_NULL = _GUID.parse("00000000-0000-0000-0000-000000000000")
_CLSID_WINDOWS = _GUID.parse("9BA05972-F6A8-11CF-A442-00A0C90A8F39")
_IID_WINDOWS = _GUID.parse("85CB6900-4D95-11CF-960C-0080C7F4EE85")
_IID_SERVICE = _GUID.parse("6D5140C1-7436-11CE-8034-00AA006009FA")
_SID_BROWSER = _GUID.parse("4C96BE40-915C-11CF-99D3-00AA004AE837")
_IID_BROWSER = _GUID.parse("000214E2-0000-0000-C000-000000000046")


def _check(result):
    if result < 0:
        raise ExplorerLaunchError()


class _ExplorerDesktop:
    """A bounded-lived reference to the current user's existing desktop COM view."""

    def __init__(self):
        if sys.platform != "win32":
            raise ExplorerLaunchError()
        self.refs = []
        self.initialized = False
        self.kernel = C.WinDLL("kernel32", use_last_error=True)
        self.user = C.WinDLL("user32", use_last_error=True)
        self.advapi = C.WinDLL("advapi32", use_last_error=True)
        self.ole = C.WinDLL("ole32")
        self.auto = C.WinDLL("oleaut32")
        self._declare()
        try:
            self.shell_window = self.user.GetShellWindow()
            self.shell_pid = self._window_pid(self.shell_window)
            self._validate_desktop_owner()
            result = self.ole.CoInitializeEx(None, 2)
            _check(result)
            self.initialized = True
            windows = C.c_void_p()
            _check(self.ole.CoCreateInstance(
                C.byref(_CLSID_WINDOWS), None, 4, C.byref(_IID_WINDOWS), C.byref(windows)))
            self._own(windows)
            location, empty = _Variant(), _Variant()
            location.vt, location.integer = 3, 0  # VT_I4, CSIDL_DESKTOP
            hwnd, dispatch = C.c_int32(), C.c_void_p()
            _check(self._call(windows, 15, [C.POINTER(_Variant), C.POINTER(_Variant),
                C.c_int32, C.POINTER(C.c_int32), C.c_int32, C.POINTER(C.c_void_p)],
                C.byref(location), C.byref(empty), 8, C.byref(hwnd), 1, C.byref(dispatch)))
            self._own(dispatch)
            if self._window_pid(hwnd.value & 0xFFFFFFFF) != self.shell_pid:
                raise ExplorerLaunchError()
            service = self._query(dispatch, _IID_SERVICE)
            browser = C.c_void_p()
            _check(self._call(service, 3, [C.POINTER(_GUID), C.POINTER(_GUID), C.POINTER(C.c_void_p)],
                             C.byref(_SID_BROWSER), C.byref(_IID_BROWSER), C.byref(browser)))
            self._own(browser)
            view = C.c_void_p()
            _check(self._call(browser, 15, [C.POINTER(C.c_void_p)], C.byref(view)))
            self._own(view)
            background = C.c_void_p()
            _check(self._call(view, 15, [C.c_uint32, C.POINTER(_GUID), C.POINTER(C.c_void_p)],
                             0, C.byref(_IID_DISPATCH), C.byref(background)))
            self._own(background)
            application = self._invoke(background, "Application", 2, [])
            try:
                if application.vt != 9 or not application.pointer:
                    raise ExplorerLaunchError()
                self.dispatch = C.c_void_p(application.pointer)
                self._call(self.dispatch, 1, [], result_type=C.c_uint32)
                self._own(self.dispatch)
            finally:
                self.auto.VariantClear(C.byref(application))
        except Exception:
            self.close()
            raise ExplorerLaunchError() from None

    def _declare(self):
        declarations = [
            (self.kernel, "GetCurrentProcess", C.c_void_p, []),
            (self.kernel, "OpenProcess", C.c_void_p, [C.c_uint32, C.c_int, C.c_uint32]),
            (self.kernel, "CloseHandle", C.c_int, [C.c_void_p]),
            (self.kernel, "QueryFullProcessImageNameW", C.c_int,
             [C.c_void_p, C.c_uint32, C.c_wchar_p, C.POINTER(C.c_uint32)]),
            (self.kernel, "ProcessIdToSessionId", C.c_int, [C.c_uint32, C.POINTER(C.c_uint32)]),
            (self.user, "GetShellWindow", C.c_void_p, []),
            (self.user, "GetWindowThreadProcessId", C.c_uint32, [C.c_void_p, C.POINTER(C.c_uint32)]),
            (self.advapi, "OpenProcessToken", C.c_int,
             [C.c_void_p, C.c_uint32, C.POINTER(C.c_void_p)]),
            (self.advapi, "GetTokenInformation", C.c_int,
             [C.c_void_p, C.c_uint32, C.c_void_p, C.c_uint32, C.POINTER(C.c_uint32)]),
            (self.advapi, "EqualSid", C.c_int, [C.c_void_p, C.c_void_p]),
            (self.ole, "CoInitializeEx", C.c_int32, [C.c_void_p, C.c_uint32]),
            (self.ole, "CoUninitialize", None, []),
            (self.ole, "CoCreateInstance", C.c_int32,
             [C.POINTER(_GUID), C.c_void_p, C.c_uint32, C.POINTER(_GUID), C.POINTER(C.c_void_p)]),
            (self.auto, "SysAllocString", C.c_void_p, [C.c_wchar_p]),
            (self.auto, "VariantClear", C.c_int32, [C.POINTER(_Variant)]),
        ]
        for library, name, result, arguments in declarations:
            function = getattr(library, name)
            function.restype, function.argtypes = result, arguments

    def _call(self, pointer, slot, argument_types, *arguments, result_type=C.c_int32):
        table = C.cast(pointer, C.POINTER(C.POINTER(C.c_void_p))).contents
        method = C.WINFUNCTYPE(result_type, C.c_void_p, *argument_types)(table[slot])
        return method(pointer, *arguments)

    def _own(self, pointer):
        if not pointer.value:
            raise ExplorerLaunchError()
        self.refs.append(pointer)
        return pointer

    def _query(self, pointer, iid):
        result = C.c_void_p()
        _check(self._call(pointer, 0, [C.POINTER(_GUID), C.POINTER(C.c_void_p)],
                         C.byref(iid), C.byref(result)))
        return self._own(result)

    def _window_pid(self, hwnd):
        pid = C.c_uint32()
        if not hwnd or not self.user.GetWindowThreadProcessId(hwnd, C.byref(pid)) or not pid.value:
            raise ExplorerLaunchError()
        return pid.value

    def _token_user(self, process):
        token, size = C.c_void_p(), C.c_uint32()
        if not self.advapi.OpenProcessToken(process, 8, C.byref(token)):
            raise ExplorerLaunchError()
        try:
            self.advapi.GetTokenInformation(token, 1, None, 0, C.byref(size))
            if not 0 < size.value <= 4096:
                raise ExplorerLaunchError()
            value = C.create_string_buffer(size.value)
            if not self.advapi.GetTokenInformation(token, 1, value, len(value), C.byref(size)):
                raise ExplorerLaunchError()
            return value  # keep backing SID storage alive until EqualSid
        finally:
            self.kernel.CloseHandle(token)

    def _validate_desktop_owner(self):
        if self.user.GetShellWindow() != self.shell_window or self._window_pid(self.shell_window) != self.shell_pid:
            raise ExplorerLaunchError()
        shell_session, own_session = C.c_uint32(), C.c_uint32()
        if (not self.kernel.ProcessIdToSessionId(self.shell_pid, C.byref(shell_session))
            or not self.kernel.ProcessIdToSessionId(os.getpid(), C.byref(own_session))
            or shell_session.value != own_session.value):
            raise ExplorerLaunchError()
        process = self.kernel.OpenProcess(0x1000, False, self.shell_pid)
        if not process:
            raise ExplorerLaunchError()
        try:
            path, length = C.create_unicode_buffer(32768), C.c_uint32(32768)
            if not self.kernel.QueryFullProcessImageNameW(process, 0, path, C.byref(length)):
                raise ExplorerLaunchError()
            expected = Path(os.environ["SystemRoot"]) / "explorer.exe"
            if Path(path.value).resolve() != expected.resolve():
                raise ExplorerLaunchError()
            own = self._token_user(self.kernel.GetCurrentProcess())
            other = self._token_user(process)
            if not self.advapi.EqualSid(C.cast(own, C.POINTER(C.c_void_p))[0],
                                       C.cast(other, C.POINTER(C.c_void_p))[0]):
                raise ExplorerLaunchError()
        finally:
            self.kernel.CloseHandle(process)

    def _invoke(self, dispatch, name, flags, arguments):
        names = (C.c_wchar_p * 1)(name)
        dispid = C.c_int32()
        _check(self._call(dispatch, 5, [C.POINTER(_GUID), C.POINTER(C.c_wchar_p),
            C.c_uint32, C.c_uint32, C.POINTER(C.c_int32)],
            C.byref(_IID_NULL), names, 1, 0, C.byref(dispid)))
        values = (_Variant * len(arguments))()
        result = _Variant()
        try:
            for value, argument in zip(values, reversed(arguments)):
                if type(argument) is int:
                    value.vt, value.integer = 3, argument
                elif type(argument) is str:
                    value.vt, value.pointer = 8, self.auto.SysAllocString(argument)
                    if not value.pointer:
                        raise ExplorerLaunchError()
                else:
                    raise ExplorerLaunchError()
            params = _Params(values, None, len(values), 0)
            _check(self._call(dispatch, 6, [C.c_int32, C.POINTER(_GUID), C.c_uint32, C.c_uint16,
                C.POINTER(_Params), C.POINTER(_Variant), C.c_void_p, C.c_void_p],
                dispid.value, C.byref(_IID_NULL), 0, flags, C.byref(params), C.byref(result), None, None))
            return result
        except Exception:
            self.auto.VariantClear(C.byref(result))
            raise
        finally:
            for value in values:
                self.auto.VariantClear(C.byref(value))

    def launch(self, executable: Path):
        self._validate_desktop_owner()
        result = self._invoke(self.dispatch, "ShellExecute", 1,
                              [str(executable), "", str(executable.parent), "open", 7])
        self.auto.VariantClear(C.byref(result))

    def close(self):
        for pointer in reversed(self.refs):
            self._call(pointer, 2, [], result_type=C.c_uint32)
        self.refs.clear()
        if self.initialized:
            self.ole.CoUninitialize()
            self.initialized = False


def launch_qmt_via_explorer(executable: Path) -> None:
    """Launch the validated QMT executable; the caller holds the UI mutex."""
    desktop = None
    try:
        try:
            if sys.platform != "win32" or not isinstance(executable, Path) or not executable.is_absolute():
                raise ExplorerLaunchError()
            resolved = executable.resolve(strict=True)
            if (not resolved.is_file() or executable.is_symlink() or resolved != executable
                or resolved.name.casefold() != "xtitclient.exe" or resolved.parent.name.casefold() != "bin.x64"):
                raise ExplorerLaunchError()
            desktop = _ExplorerDesktop()
            desktop.launch(resolved)
        finally:
            if desktop is not None:
                desktop.close()
    except Exception:
        raise ExplorerLaunchError() from None
