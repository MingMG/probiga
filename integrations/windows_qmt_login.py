"""Version-bound native QMT login driver; no command-line credential channel."""
from __future__ import annotations

from contextlib import contextmanager
import ctypes
from ctypes import wintypes as w
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import time

from integrations.windows_terminal_recovery import ROOT, QmtTerminalRecoveryError as Error
from integrations.qmt_login_profile import Frame, Rect, blink_caret, validate_login_profile


QMT_EXE = Path(r"D:\国金证券QMT交易端\bin.x64\XtItClient.exe")
QMT_HOME = QMT_EXE.parent.parent
QMT_TITLE = "国金证券QMT交易端 2.1.19.0"
QMT_VERSION = "2.1.19.0"
VENDOR_BANNER_SHA256 = "872dc92861f096d12ece99383426c8c74a61c0e4702ccc3aafa927a8822fe8c0"
_CREDENTIAL_TARGET = "ProBigA/AppLogin/qmt"
_CREDENTIAL_COMMENT = "ProBigA app login credential v1; local user only"


class _POINT(ctypes.Structure):
    _fields_ = [("x", w.LONG), ("y", w.LONG)]


class _LASTINPUTINFO(ctypes.Structure):
    _fields_ = [("size", w.UINT), ("tick", w.DWORD)]


class _RECT(ctypes.Structure):
    _fields_ = [("left", w.LONG), ("top", w.LONG), ("right", w.LONG), ("bottom", w.LONG)]


class _PROCESSENTRY32(ctypes.Structure):
    _fields_ = [("dwSize", w.DWORD), ("cntUsage", w.DWORD), ("th32ProcessID", w.DWORD),
                ("th32DefaultHeapID", ctypes.c_size_t), ("th32ModuleID", w.DWORD),
                ("cntThreads", w.DWORD), ("th32ParentProcessID", w.DWORD),
                ("pcPriClassBase", w.LONG), ("dwFlags", w.DWORD), ("szExeFile", w.WCHAR * 260)]


class _BITMAPINFOHEADER(ctypes.Structure):
    _fields_ = [("size", w.DWORD), ("width", w.LONG), ("height", w.LONG),
                ("planes", w.WORD), ("bits", w.WORD), ("compression", w.DWORD),
                ("image_size", w.DWORD), ("xppm", w.LONG), ("yppm", w.LONG),
                ("used", w.DWORD), ("important", w.DWORD)]


class _BITMAPINFO(ctypes.Structure):
    _fields_ = [("header", _BITMAPINFOHEADER), ("colors", w.DWORD)]


class _KEYBDINPUT(ctypes.Structure):
    _fields_ = [("vk", w.WORD), ("scan", w.WORD), ("flags", w.DWORD),
                ("time", w.DWORD), ("extra", ctypes.c_size_t)]


class _MOUSEINPUT(ctypes.Structure):
    _fields_ = [("dx", w.LONG), ("dy", w.LONG), ("data", w.DWORD),
                ("flags", w.DWORD), ("time", w.DWORD), ("extra", ctypes.c_size_t)]


class _INPUTUNION(ctypes.Union):
    _fields_ = [("keyboard", _KEYBDINPUT), ("mouse", _MOUSEINPUT)]


class _INPUT(ctypes.Structure):
    _fields_ = [("type", w.DWORD), ("value", _INPUTUNION)]


class _CREDENTIAL(ctypes.Structure):
    _fields_ = [("flags", w.DWORD), ("type", w.DWORD), ("target", w.LPWSTR),
                ("comment", w.LPWSTR), ("written", w.FILETIME), ("blob_size", w.DWORD),
                ("blob", ctypes.POINTER(w.BYTE)), ("persist", w.DWORD),
                ("attribute_count", w.DWORD), ("attributes", ctypes.c_void_p),
                ("alias", w.LPWSTR), ("username", w.LPWSTR)]


@dataclass(frozen=True, repr=False)
class _Credential:
    username: str
    password: str
    revision: int


@dataclass(frozen=True)
class _ProcessIdentity:
    pid: int
    started: int
    session: int


@dataclass(frozen=True)
class Observation:
    status: str
    identity: _ProcessIdentity | None = None
    hwnd: int = 0


@dataclass(frozen=True)
class _LoginTarget:
    identity: _ProcessIdentity
    hwnd: int
    rect: tuple[int, int, int, int]


def _ft(value: w.FILETIME) -> int:
    return int(value.dwHighDateTime) << 32 | int(value.dwLowDateTime)


def _secret_text_valid(value: str, maximum: int) -> bool:
    return (type(value) is str and bool(value) and not any(ord(c) < 32 or ord(c) == 127 for c in value)
            and len(value.encode("utf-16-le")) <= maximum * 2)


class _Native:
    def __init__(self):
        self.kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        self.user = ctypes.WinDLL("user32", use_last_error=True)
        self.gdi = ctypes.WinDLL("gdi32", use_last_error=True)
        self.advapi = ctypes.WinDLL("advapi32", use_last_error=True)
        self.version = ctypes.WinDLL("version", use_last_error=True)
        self._enum_callback = ctypes.WINFUNCTYPE(w.BOOL, w.HWND, w.LPARAM)
        self._bind()

    def _bind(self):
        bindings = [
            (self.kernel, "CreateToolhelp32Snapshot", w.HANDLE, [w.DWORD, w.DWORD]),
            (self.kernel, "Process32FirstW", w.BOOL, [w.HANDLE, ctypes.POINTER(_PROCESSENTRY32)]),
            (self.kernel, "Process32NextW", w.BOOL, [w.HANDLE, ctypes.POINTER(_PROCESSENTRY32)]),
            (self.kernel, "OpenProcess", w.HANDLE, [w.DWORD, w.BOOL, w.DWORD]),
            (self.kernel, "CloseHandle", w.BOOL, [w.HANDLE]),
            (self.kernel, "QueryFullProcessImageNameW", w.BOOL, [w.HANDLE, w.DWORD, w.LPWSTR, ctypes.POINTER(w.DWORD)]),
            (self.kernel, "GetProcessTimes", w.BOOL, [w.HANDLE] + [ctypes.POINTER(w.FILETIME)] * 4),
            (self.kernel, "ProcessIdToSessionId", w.BOOL, [w.DWORD, ctypes.POINTER(w.DWORD)]),
            (self.kernel, "GetCurrentThreadId", w.DWORD, []),
            (self.kernel, "GetCurrentProcess", w.HANDLE, []),
            (self.kernel, "CreateMutexW", w.HANDLE, [ctypes.c_void_p, w.BOOL, w.LPCWSTR]),
            (self.kernel, "WaitForSingleObject", w.DWORD, [w.HANDLE, w.DWORD]),
            (self.kernel, "ReleaseMutex", w.BOOL, [w.HANDLE]),
            (self.user, "EnumWindows", w.BOOL, [self._enum_callback, w.LPARAM]),
            (self.user, "GetWindowThreadProcessId", w.DWORD, [w.HWND, ctypes.POINTER(w.DWORD)]),
            (self.user, "GetWindowTextW", ctypes.c_int, [w.HWND, w.LPWSTR, ctypes.c_int]),
            (self.user, "GetClassNameW", ctypes.c_int, [w.HWND, w.LPWSTR, ctypes.c_int]),
            (self.user, "IsWindow", w.BOOL, [w.HWND]),
            (self.user, "IsWindowVisible", w.BOOL, [w.HWND]),
            (self.user, "IsWindowEnabled", w.BOOL, [w.HWND]),
            (self.user, "IsIconic", w.BOOL, [w.HWND]),
            (self.user, "ShowWindow", w.BOOL, [w.HWND, ctypes.c_int]),
            (self.user, "GetWindowRect", w.BOOL, [w.HWND, ctypes.POINTER(_RECT)]),
            (self.user, "GetDpiForWindow", w.UINT, [w.HWND]),
            (self.user, "GetForegroundWindow", w.HWND, []),
            (self.user, "SetForegroundWindow", w.BOOL, [w.HWND]),
            (self.user, "WindowFromPoint", w.HWND, [_POINT]),
            (self.user, "GetAncestor", w.HWND, [w.HWND, w.UINT]),
            (self.user, "GetThreadDesktop", w.HANDLE, [w.DWORD]),
            (self.user, "OpenInputDesktop", w.HANDLE, [w.DWORD, w.BOOL, w.DWORD]),
            (self.user, "CloseDesktop", w.BOOL, [w.HANDLE]),
            (self.user, "GetUserObjectInformationW", w.BOOL, [w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD, ctypes.POINTER(w.DWORD)]),
            (self.user, "GetAsyncKeyState", w.SHORT, [ctypes.c_int]),
            (self.user, "GetLastInputInfo", w.BOOL, [ctypes.POINTER(_LASTINPUTINFO)]),
            (self.user, "SetCursorPos", w.BOOL, [ctypes.c_int, ctypes.c_int]),
            (self.user, "SendInput", w.UINT, [w.UINT, ctypes.POINTER(_INPUT), ctypes.c_int]),
            (self.user, "GetDC", w.HDC, [w.HWND]),
            (self.user, "ReleaseDC", ctypes.c_int, [w.HWND, w.HDC]),
            (self.gdi, "CreateCompatibleDC", w.HDC, [w.HDC]),
            (self.gdi, "DeleteDC", w.BOOL, [w.HDC]),
            (self.gdi, "CreateDIBSection", w.HBITMAP, [w.HDC, ctypes.POINTER(_BITMAPINFO), w.UINT, ctypes.POINTER(ctypes.c_void_p), w.HANDLE, w.DWORD]),
            (self.gdi, "SelectObject", w.HGDIOBJ, [w.HDC, w.HGDIOBJ]),
            (self.gdi, "DeleteObject", w.BOOL, [w.HGDIOBJ]),
            (self.gdi, "BitBlt", w.BOOL, [w.HDC, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int, w.HDC, ctypes.c_int, ctypes.c_int, w.DWORD]),
            (self.advapi, "CredReadW", w.BOOL, [w.LPCWSTR, w.DWORD, w.DWORD, ctypes.POINTER(ctypes.POINTER(_CREDENTIAL))]),
            (self.advapi, "CredFree", None, [ctypes.c_void_p]),
            (self.advapi, "OpenProcessToken", w.BOOL, [w.HANDLE, w.DWORD, ctypes.POINTER(w.HANDLE)]),
            (self.advapi, "GetTokenInformation", w.BOOL, [w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD, ctypes.POINTER(w.DWORD)]),
            (self.advapi, "EqualSid", w.BOOL, [ctypes.c_void_p, ctypes.c_void_p]),
            (self.version, "GetFileVersionInfoSizeW", w.DWORD, [w.LPCWSTR, ctypes.POINTER(w.DWORD)]),
            (self.version, "GetFileVersionInfoW", w.BOOL, [w.LPCWSTR, w.DWORD, w.DWORD, ctypes.c_void_p]),
            (self.version, "VerQueryValueW", w.BOOL, [ctypes.c_void_p, w.LPCWSTR, ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(w.UINT)]),
        ]
        for library, name, result, arguments in bindings:
            method = getattr(library, name)
            method.restype, method.argtypes = result, arguments

    @contextmanager
    def mutex(self, name: str):
        handle = self.kernel.CreateMutexW(None, False, name)
        if not handle:
            raise Error("QMT_RECOVERY_LOCK_FAILED")
        held = False
        try:
            result = self.kernel.WaitForSingleObject(handle, 20000)
            if result == 258:
                raise Error("QMT_RECOVERY_LOCK_TIMEOUT")
            if result not in (0, 128):
                raise Error("QMT_RECOVERY_LOCK_FAILED")
            held = True
            yield
        finally:
            if held:
                self.kernel.ReleaseMutex(handle)
            self.kernel.CloseHandle(handle)

    def process_ids(self) -> list[int]:
        snapshot = self.kernel.CreateToolhelp32Snapshot(2, 0)
        if not snapshot or snapshot == ctypes.c_void_p(-1).value:
            raise Error("QMT_CLIENT_ACCESS_DENIED")
        try:
            entry = _PROCESSENTRY32()
            entry.dwSize = ctypes.sizeof(entry)
            found = []
            valid = self.kernel.Process32FirstW(snapshot, ctypes.byref(entry))
            while valid:
                if entry.szExeFile.casefold() == "xtitclient.exe":
                    found.append(int(entry.th32ProcessID))
                valid = self.kernel.Process32NextW(snapshot, ctypes.byref(entry))
            if ctypes.get_last_error() != 18:  # ERROR_NO_MORE_FILES is the only complete enumeration.
                raise Error("QMT_CLIENT_ACCESS_DENIED")
            return found
        finally:
            self.kernel.CloseHandle(snapshot)

    def _require_current_user(self, process) -> None:
        tokens, buffers = [], []
        try:
            for handle in (process, self.kernel.GetCurrentProcess()):
                token = w.HANDLE()
                if not self.advapi.OpenProcessToken(handle, 8, ctypes.byref(token)):
                    raise Error("QMT_CLIENT_ACCESS_DENIED")
                tokens.append(token)
                size = w.DWORD()
                self.advapi.GetTokenInformation(token, 1, None, 0, ctypes.byref(size))
                if not ctypes.sizeof(ctypes.c_void_p) < size.value <= 4096:
                    raise Error("QMT_CLIENT_ACCESS_DENIED")
                buffer = ctypes.create_string_buffer(size.value)
                if not self.advapi.GetTokenInformation(token, 1, buffer, size, ctypes.byref(size)):
                    raise Error("QMT_CLIENT_ACCESS_DENIED")
                buffers.append(buffer)
            # TOKEN_USER begins with SID_AND_ATTRIBUTES, whose first field is
            # the SID pointer. Compare the two OS-returned SIDs without ever
            # serializing either identity or querying another credential store.
            sids = [ctypes.cast(buffer, ctypes.POINTER(ctypes.c_void_p))[0] for buffer in buffers]
            if not all(sids) or not self.advapi.EqualSid(*sids):
                raise Error("QMT_CLIENT_IDENTITY_INVALID")
        finally:
            for buffer in buffers:
                ctypes.memset(ctypes.addressof(buffer), 0, ctypes.sizeof(buffer))
            for token in tokens:
                self.kernel.CloseHandle(token)

    def identity(self, pid: int) -> _ProcessIdentity:
        handle = self.kernel.OpenProcess(0x1000, False, pid)
        if not handle:
            raise Error("QMT_CLIENT_ACCESS_DENIED")
        try:
            text = ctypes.create_unicode_buffer(32768)
            length = w.DWORD(len(text))
            if not self.kernel.QueryFullProcessImageNameW(handle, 0, text, ctypes.byref(length)):
                raise Error("QMT_CLIENT_ACCESS_DENIED")
            if os.path.normcase(os.path.abspath(text.value)) != os.path.normcase(str(QMT_EXE)):
                raise Error("QMT_CLIENT_IDENTITY_INVALID")
            started, ended, kernel, user = (w.FILETIME() for _ in range(4))
            if not self.kernel.GetProcessTimes(handle, ctypes.byref(started), ctypes.byref(ended), ctypes.byref(kernel), ctypes.byref(user)):
                raise Error("QMT_CLIENT_ACCESS_DENIED")
            session, current = w.DWORD(), w.DWORD()
            if (not self.kernel.ProcessIdToSessionId(pid, ctypes.byref(session))
                    or not self.kernel.ProcessIdToSessionId(os.getpid(), ctypes.byref(current))):
                raise Error("QMT_CLIENT_ACCESS_DENIED")
            if session.value == 0 or session.value != current.value:
                raise Error("QMT_CLIENT_SESSION_MISMATCH")
            self._require_current_user(handle)
            return _ProcessIdentity(pid, _ft(started), int(session.value))
        finally:
            self.kernel.CloseHandle(handle)

    def installed_version(self) -> str:
        ignored = w.DWORD()
        size = self.version.GetFileVersionInfoSizeW(str(QMT_EXE), ctypes.byref(ignored))
        if not 0 < size < 2**20:
            raise Error("QMT_VERSION_UNSUPPORTED")
        buffer = ctypes.create_string_buffer(size)
        if not self.version.GetFileVersionInfoW(str(QMT_EXE), 0, size, buffer):
            raise Error("QMT_VERSION_UNSUPPORTED")
        pointer, length = ctypes.c_void_p(), w.UINT()
        if not self.version.VerQueryValueW(buffer, "\\", ctypes.byref(pointer), ctypes.byref(length)) or length.value < 16:
            raise Error("QMT_VERSION_UNSUPPORTED")
        values = ctypes.cast(pointer, ctypes.POINTER(w.DWORD))
        if values[0] != 0xFEEF04BD:
            raise Error("QMT_VERSION_UNSUPPORTED")
        return ".".join(str(v) for v in (values[2] >> 16, values[2] & 65535, values[3] >> 16, values[3] & 65535))

    def owner(self, hwnd: int) -> int:
        value = w.DWORD()
        self.user.GetWindowThreadProcessId(hwnd, ctypes.byref(value))
        return int(value.value)

    def title(self, hwnd: int) -> str:
        text = ctypes.create_unicode_buffer(512)
        self.user.GetWindowTextW(hwnd, text, len(text))
        return text.value

    def class_name(self, hwnd: int) -> str:
        text = ctypes.create_unicode_buffer(128)
        self.user.GetClassNameW(hwnd, text, len(text))
        return text.value

    def windows(self, pid: int) -> list[int]:
        windows = []
        @self._enum_callback
        def callback(hwnd, unused):
            if self.owner(hwnd) == pid and self.user.IsWindowVisible(hwnd):
                windows.append(int(hwnd))
            return True
        if not self.user.EnumWindows(callback, 0):
            raise Error("QMT_CLIENT_ACCESS_DENIED")
        return windows

    def rect(self, hwnd: int) -> tuple[int, int, int, int]:
        value = _RECT()
        if not self.user.GetWindowRect(hwnd, ctypes.byref(value)):
            raise Error("QMT_WINDOW_CHANGED")
        return value.left, value.top, value.right, value.bottom

    def desktop_available(self) -> None:
        desktop = self.user.OpenInputDesktop(0, False, 1)
        if not desktop:
            raise Error("QMT_DESKTOP_UNAVAILABLE")
        try:
            for handle in (desktop, self.user.GetThreadDesktop(self.kernel.GetCurrentThreadId())):
                name = ctypes.create_unicode_buffer(256)
                size = w.DWORD()
                if (not handle or not self.user.GetUserObjectInformationW(handle, 2, name, ctypes.sizeof(name), ctypes.byref(size))
                        or name.value != "Default"):
                    raise Error("QMT_DESKTOP_UNAVAILABLE")
        finally:
            self.user.CloseDesktop(desktop)

    def read_credential(self, *, revision_only: bool = False) -> _Credential | int:
        pointer = ctypes.POINTER(_CREDENTIAL)()
        if not self.advapi.CredReadW(_CREDENTIAL_TARGET, 1, 0, ctypes.byref(pointer)):
            code = "QMT_CREDENTIAL_NOT_FOUND" if ctypes.get_last_error() == 1168 else "QMT_CREDENTIAL_READ_FAILED"
            raise Error(code)
        try:
            if not pointer:
                raise Error("QMT_CREDENTIAL_INVALID")
            entry = pointer.contents
            if (entry.type != 1 or entry.persist != 2 or entry.flags != 0 or entry.target != _CREDENTIAL_TARGET
                    or entry.comment != _CREDENTIAL_COMMENT or entry.attribute_count or entry.attributes
                    or entry.alias is not None or not entry.blob or not 2 <= entry.blob_size <= 512 or entry.blob_size % 2
                    or not _secret_text_valid(entry.username, 513) or not entry.username.strip() or _ft(entry.written) <= 0):
                raise Error("QMT_CREDENTIAL_INVALID")
            if revision_only:
                return _ft(entry.written)
            try:
                password = ctypes.string_at(entry.blob, entry.blob_size).decode("utf-16-le")
                if not _secret_text_valid(password, 256):
                    raise ValueError
            except (UnicodeError, ValueError):
                raise Error("QMT_CREDENTIAL_INVALID") from None
            return _Credential(entry.username, password, _ft(entry.written))
        finally:
            if pointer:
                entry = pointer.contents
                if entry.blob and 0 < entry.blob_size <= 2560:
                    ctypes.memset(entry.blob, 0, entry.blob_size)
                self.advapi.CredFree(pointer)

    def capture(self, x: int, y: int, width: int, height: int) -> Frame:
        source = self.user.GetDC(None)
        target = self.gdi.CreateCompatibleDC(source) if source else None
        bitmap = previous = None
        bits = ctypes.c_void_p()
        try:
            if not source or not target or not 0 < width * height <= 624 * 443:
                raise Error("QMT_CAPTURE_FAILED")
            info = _BITMAPINFO()
            info.header = _BITMAPINFOHEADER(ctypes.sizeof(_BITMAPINFOHEADER), width, -height, 1, 32, 0, 0, 0, 0, 0, 0)
            bitmap = self.gdi.CreateDIBSection(source, ctypes.byref(info), 0, ctypes.byref(bits), None, 0)
            if not bitmap or not bits.value:
                raise Error("QMT_CAPTURE_FAILED")
            previous = self.gdi.SelectObject(target, bitmap)
            if not previous or not self.gdi.BitBlt(target, 0, 0, width, height, source, x, y, 0x00CC0020):
                raise Error("QMT_CAPTURE_FAILED")
            return Frame(width, height, ctypes.string_at(bits, width * height * 4))
        finally:
            if bits.value:
                ctypes.memset(bits, 0, width * height * 4)
            if previous:
                self.gdi.SelectObject(target, previous)
            if bitmap:
                self.gdi.DeleteObject(bitmap)
            if target:
                self.gdi.DeleteDC(target)
            if source:
                self.user.ReleaseDC(None, source)

    def _send(self, values) -> None:
        entries = (_INPUT * len(values))(*values)
        try:
            if self.user.SendInput(len(entries), entries, ctypes.sizeof(_INPUT)) != len(entries):
                # A partial batch must not leave Ctrl/A or the mouse button
                # held. These releases contain no characters or credentials.
                release = (_INPUT * 3)(
                    _INPUT(1, _INPUTUNION(keyboard=_KEYBDINPUT(65, 0, 2, 0, 0))),
                    _INPUT(1, _INPUTUNION(keyboard=_KEYBDINPUT(17, 0, 2, 0, 0))),
                    _INPUT(0, _INPUTUNION(mouse=_MOUSEINPUT(0, 0, 0, 4, 0, 0))),
                )
                try:
                    self.user.SendInput(len(release), release, ctypes.sizeof(_INPUT))
                finally:
                    ctypes.memset(release, 0, ctypes.sizeof(release))
                raise Error("QMT_INPUT_FAILED")
        finally:
            ctypes.memset(entries, 0, ctypes.sizeof(entries))

    def no_modifiers(self) -> None:
        if any(self.user.GetAsyncKeyState(key) & 0x8000 for key in (1, 2, 16, 17, 18, 91, 92)):
            raise Error("QMT_INPUT_MODIFIERS_HELD")

    def last_input_tick(self) -> int:
        value = _LASTINPUTINFO(ctypes.sizeof(_LASTINPUTINFO), 0)
        if not self.user.GetLastInputInfo(ctypes.byref(value)):
            raise Error("QMT_FOCUS_CHANGED")
        return int(value.tick)

    def click(self, x: int, y: int) -> None:
        self.no_modifiers()
        if not self.user.SetCursorPos(x, y):
            raise Error("QMT_INPUT_FAILED")
        self._send([_INPUT(0, _INPUTUNION(mouse=_MOUSEINPUT(0, 0, 0, flag, 0, 0))) for flag in (2, 4)])

    def replace_text(self, value: str) -> None:
        self.no_modifiers()
        keys = [(17, 0), (65, 0), (65, 2), (17, 2)]
        entries = [_INPUT(1, _INPUTUNION(keyboard=_KEYBDINPUT(vk, 0, flags, 0, 0))) for vk, flags in keys]
        encoded = bytearray(value.encode("utf-16-le"))
        try:
            for index in range(0, len(encoded), 2):
                unit = encoded[index] | encoded[index + 1] << 8
                entries.extend(_INPUT(1, _INPUTUNION(keyboard=_KEYBDINPUT(0, unit, flags, 0, 0))) for flags in (4, 6))
            self._send(entries)
        finally:
            for entry in entries:
                ctypes.memset(ctypes.byref(entry), 0, ctypes.sizeof(entry))
            for index in range(len(encoded)):
                encoded[index] = 0


class WindowsQmtLoginDriver:
    def __init__(self):
        self.native = _Native()
        self.submitted_at = 0.0

    def recovery_lock(self, name: str):
        return self.native.mutex(name)

    def _check_installation(self) -> None:
        if self.native.installed_version() != QMT_VERSION:
            raise Error("QMT_VERSION_UNSUPPORTED")
        try:
            data = (QMT_HOME / "resource" / "tc_img_login_deploy.png").read_bytes()
            if len(data) != 140005 or hashlib.sha256(data).hexdigest() != VENDOR_BANNER_SHA256:
                raise ValueError
        except Exception:
            raise Error("QMT_LOGIN_LAYOUT_UNSUPPORTED") from None

    def observe(self) -> Observation:
        self.native.desktop_available()
        pids = self.native.process_ids()
        if not pids:
            return Observation("absent")
        if len(pids) != 1:
            raise Error("QMT_CLIENT_NOT_UNIQUE")
        identity = self.native.identity(pids[0])
        self._check_installation()
        matches = []
        for hwnd in self.native.windows(identity.pid):
            if not self.native.class_name(hwnd).startswith("Qt5QWindow"):
                continue
            title = self.native.title(hwnd)
            if title == QMT_TITLE:
                matches.append(Observation("login_required", identity, hwnd))
            elif re.fullmatch(r"\s*\d+\s*-\s*" + re.escape(QMT_TITLE), title):
                matches.append(Observation("logged_in", identity, hwnd))
            else:
                raise Error("QMT_MODAL_PRESENT")
        if len(matches) != 1:
            raise Error("QMT_LOGIN_LAYOUT_UNSUPPORTED")
        return matches[0]

    def wait_for_login_window(self) -> Observation:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                current = self.observe()
                if current.status in {"login_required", "logged_in"}:
                    return current
            except Error as exc:
                if exc.code not in {"QMT_LOGIN_LAYOUT_UNSUPPORTED", "QMT_MODAL_PRESENT"}:
                    raise
            time.sleep(0.25)
        raise Error("QMT_TERMINAL_START_TIMEOUT")

    def start_terminal(self) -> None:
        # The collector belongs to a kill-on-close Job. Its independent market
        # terminal must be created by the existing interactive Explorer owner.
        self.native.desktop_available()
        self._check_installation()
        pids = self.native.process_ids()
        if pids:
            if len(pids) != 1:
                raise Error("QMT_CLIENT_NOT_UNIQUE")
            self.native.identity(pids[0])
            return
        from integrations.windows_explorer_launch import launch_qmt_via_explorer

        self.submitted_at = time.time()
        try:
            launch_qmt_via_explorer(QMT_EXE)
        except Exception:
            raise Error("QMT_TERMINAL_START_FAILED") from None

    def _guard(self, target: _LoginTarget, *, point: tuple[int, int] | None = None) -> None:
        self.native.desktop_available()
        if self.native.identity(target.identity.pid) != target.identity:
            raise Error("QMT_WINDOW_CHANGED")
        if (not self.native.user.IsWindow(target.hwnd) or self.native.owner(target.hwnd) != target.identity.pid
                or self.native.title(target.hwnd) != QMT_TITLE or self.native.rect(target.hwnd) != target.rect
                or not self.native.user.IsWindowVisible(target.hwnd) or not self.native.user.IsWindowEnabled(target.hwnd)):
            raise Error("QMT_WINDOW_CHANGED")
        if self.native.user.GetForegroundWindow() != target.hwnd:
            raise Error("QMT_FOREGROUND_CHANGED")
        for hwnd in self.native.windows(target.identity.pid):
            if hwnd != target.hwnd and self.native.class_name(hwnd).startswith("Qt5QWindow"):
                raise Error("QMT_MODAL_PRESENT")
        if self.native.user.GetDpiForWindow(target.hwnd) != 96:
            raise Error("QMT_LOGIN_LAYOUT_UNSUPPORTED")
        if point is not None:
            absolute = _POINT(target.rect[0] + point[0], target.rect[1] + point[1])
            hit = self.native.user.WindowFromPoint(absolute)
            if self.native.owner(hit) != target.identity.pid or self.native.user.GetAncestor(hit, 2) != target.hwnd:
                raise Error("QMT_INPUT_OCCLUDED")

    def _profile(self, target: _LoginTarget):
        self._guard(target)
        for point in ((13, 45), (610, 173), (195, 242), (425, 264), (195, 277), (425, 299), (200, 340), (350, 340)):
            self._guard(target, point=point)
        frame = self.native.capture(target.rect[0], target.rect[1], 624, 443)
        self._guard(target)
        try:
            return validate_login_profile(frame, executable_version=QMT_VERSION, dpi=96)
        except ValueError:
            raise Error("QMT_LOGIN_LAYOUT_UNSUPPORTED") from None

    def prepare_login(self, current: Observation) -> _LoginTarget:
        if current.status != "login_required" or current.identity is None:
            raise Error("QMT_LOGIN_LAYOUT_UNSUPPORTED")
        if self.native.user.IsIconic(current.hwnd):
            self.native.user.ShowWindow(current.hwnd, 9)
        self.native.user.SetForegroundWindow(current.hwnd)
        target = _LoginTarget(current.identity, current.hwnd, self.native.rect(current.hwnd))
        if (target.rect[2] - target.rect[0], target.rect[3] - target.rect[1]) != (624, 443):
            raise Error("QMT_LOGIN_LAYOUT_UNSUPPORTED")
        self._profile(target)
        return target

    def credential_revision(self) -> int:
        return self.native.read_credential(revision_only=True)

    def _focus_field(self, target: _LoginTarget, rectangle: Rect) -> int:
        self._profile(target)
        point = ((rectangle.left + rectangle.right) // 2, (rectangle.top + rectangle.bottom) // 2)
        self._guard(target, point=point)
        self.native.click(target.rect[0] + point[0], target.rect[1] + point[1])
        time.sleep(0.05)
        input_tick = self.native.last_input_tick()
        roi = Rect(rectangle.left + 5, rectangle.top + 4, rectangle.right - 25, rectangle.bottom - 4)
        frames = []
        try:
            for _ in range(36):
                self._guard(target, point=point)
                if self.native.last_input_tick() != input_tick:
                    raise Error("QMT_FOCUS_CHANGED")
                frames.append(self.native.capture(target.rect[0] + roi.left, target.rect[1] + roi.top,
                                                  roi.right - roi.left, roi.bottom - roi.top))
                time.sleep(0.08)
            self._guard(target, point=point)
            blink_caret(frames, Rect(0, 0, roi.right - roi.left, roi.bottom - roi.top))
            return input_tick
        except ValueError:
            raise Error("QMT_CARET_NOT_VERIFIED") from None
        finally:
            frames.clear()

    def login(self, target: _LoginTarget, *, expected_credential_revision: int, before_submit) -> None:
        profile = self._profile(target)
        input_tick = self._focus_field(target, profile.account)
        credential = self.native.read_credential()
        try:
            if credential.revision != expected_credential_revision:
                raise Error("QMT_CREDENTIAL_CHANGED")
            self._profile(target)
            self._guard(target, point=((profile.account.left + profile.account.right) // 2,
                                       (profile.account.top + profile.account.bottom) // 2))
            if self.native.last_input_tick() != input_tick:
                raise Error("QMT_FOCUS_CHANGED")
            self.native.replace_text(credential.username)
            input_tick = self._focus_field(target, profile.password)
            # The password enters only after a fresh full profile, same process,
            # foreground/occlusion guard, and a real blinking caret in its ROI.
            self._profile(target)
            self._guard(target, point=((profile.password.left + profile.password.right) // 2,
                                       (profile.password.top + profile.password.bottom) // 2))
            if self.native.last_input_tick() != input_tick:
                raise Error("QMT_FOCUS_CHANGED")
            self.native.replace_text(credential.password)
            self._profile(target)
            button = profile.login_button
            point = ((button.left + button.right) // 2, (button.top + button.bottom) // 2)
            self._guard(target, point=point)
            before_submit()
            self._guard(target, point=point)
            self.submitted_at = time.time()
            self.native.click(target.rect[0] + point[0], target.rect[1] + point[1])
        finally:
            credential = None
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            try:
                current = self.observe()
                if current.identity != target.identity:
                    raise Error("QMT_WINDOW_CHANGED")
                if current.status == "logged_in":
                    return
            except Error as exc:
                if exc.code not in {"QMT_MODAL_PRESENT", "QMT_LOGIN_LAYOUT_UNSUPPORTED"}:
                    raise
                # Login progress may replace the form before the authenticated
                # window appears. Observe only: unknown panels, MFA and errors
                # receive no further clicks or input, and cannot unlock another
                # authentication attempt if the bounded wait expires.
            time.sleep(0.25)
        raise Error("QMT_LOGIN_REJECTED_OR_VERIFICATION_REQUIRED")

    def confirm_logged_in(self, target) -> None:
        current = self.observe()
        if current.status != "logged_in" or current.identity != target.identity:
            raise Error("QMT_WINDOW_CHANGED")

    def _fresh_bridge(self, target) -> bool:
        path = QMT_HOME / "userdata" / "probiga_bridge" / "heartbeat.json"
        try:
            if path.stat().st_size > 131072:
                return False
            value = json.loads(path.read_text(encoding="utf-8"))
            if type(value) is not dict:
                return False
            updated = value.get("updated_ts")
            return bool(
                type(value.get("pid")) is int and value["pid"] == target.identity.pid
                and value.get("source") == "gj_big_qmt_inner" and value.get("status") in {"running", "busy"}
                and type(updated) in {int, float} and math.isfinite(updated)
                and self.submitted_at <= updated <= time.time() + 2 and time.time() - updated <= 10
                and isinstance(value.get("model_instance_id"), str) and value["model_instance_id"]
            )
        except (OSError, ValueError, TypeError):
            return False

    def _recover_model(self, target) -> None:
        self.confirm_logged_in(target)
        build = os.environ.get("PROBIGA_BUILD_COMMIT_SHA", "")
        if re.fullmatch(r"[0-9a-f]{40}", build) is None or build == "0" * 40:
            raise Error("QMT_BRIDGE_RECOVERY_FAILED")
        script = ROOT / "tools" / "reload_big_qmt_strategy.ps1"
        powershell = Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"
        try:
            result = subprocess.run(
                [str(powershell), "-NoProfile", "-NonInteractive", "-File", str(script),
                 "-RegisteredRoot", str(ROOT), "-ExpectedBuildSha", build, "-ColdStartRecovery"],
                cwd=ROOT, capture_output=True, timeout=210, check=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            # Only the final structured receipt is interpreted. Native errors,
            # paths, window titles, and any provider text are never forwarded.
            lines = result.stdout.decode("utf-8", errors="strict").splitlines()
            receipt = json.loads(lines[-1]) if lines else None
            if (result.returncode != 0 or type(receipt) is not dict
                    or receipt.get("schema") != "probiga.bigqmt-ui-release-reload.v1"
                    or receipt.get("expected_build_sha") != build
                    or receipt.get("status") not in {"COLD_START_COMPLETE", "COMPLETE", "IDEMPOTENT"}
                    or receipt.get("database_writes") is not False
                    or receipt.get("authentication_attempted") is not False
                    or receipt.get("automatic_order_submission") is not False
                    or receipt.get("direct_python_strategy_execution") is not False
                    or ("qmt_client_pid" in receipt and receipt["qmt_client_pid"] != target.identity.pid)):
                raise ValueError
        except Exception:
            raise Error("QMT_BRIDGE_RECOVERY_FAILED") from None

    def wait_for_recovered_bridge(self, target) -> None:
        self.confirm_logged_in(target)
        if self._fresh_bridge(target):
            return
        self._recover_model(target)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            self.confirm_logged_in(target)
            if self._fresh_bridge(target):
                return
            time.sleep(0.25)
        raise Error("QMT_BRIDGE_NOT_READY")
