"""Append-only local observations for an exact scheduler terminal transaction.

These owner-private job-log records are recovery data, not authority to finish a
run.  The database writer must still compare the complete original run and
daily-stage identities in the same transaction.  No file is deleted or replaced,
including partial files.  A commit marker is written only after database
readback; process disappearance and old task-summary output are never inputs.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import threading


MAX_BYTES = 32 * 1024 * 1024
_FAILED_CLOSES = []
_LOCK = threading.RLock()
_UID = re.compile(r"[0-9a-f]{32}")
_NAME = re.compile(r"(?:OBSERVED|REJECTED|FAILED_FINALIZATION|COMMITTED|PREPARED\.[0-9a-f]{64})")


class TerminalJournalError(RuntimeError):
    pass


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def _fail():
    raise TerminalJournalError("scheduler terminal original is unavailable or differs")


def _parse(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                _fail()
            value[key] = item
        return value
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs)
        if type(value) is not dict or canonical(value) != raw:
            _fail()
        return value
    except (ValueError, UnicodeError, RecursionError):
        _fail()


def _close(fd):
    try:
        os.close(fd)
    except BaseException:
        _FAILED_CLOSES.append(fd)  # Never retry a possibly reused numeric fd.
        raise


def _state(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_nlink, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns)


def _matching_open(path_info, fd_info):
    # CPython Windows lstat/fstat can expose different deprecated st_ctime
    # semantics. Compare identity/size/write-time across APIs, and compare each
    # API's own complete metadata before/after the actual read below.
    return (_state(path_info)[:-1] == _state(fd_info)[:-1] if os.name == "nt"
            else _state(path_info) == _state(fd_info))


def _ordinary(path, *, directory):
    info = os.lstat(path)
    junction = getattr(path, "is_junction", lambda: False)()
    if (stat.S_ISLNK(info.st_mode) or junction
            or bool(getattr(info, "st_file_attributes", 0) & 0x400)
            or (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)) is not True
            or (not directory and info.st_nlink != 1)):
        _fail()
    if os.name != "nt" and (info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != (0o700 if directory else 0o600)):
        _fail()
    return info


def _sync_directory(path):
    # NTFS file fsync/close is the normal Windows durable-write boundary.
    # Python does not expose a directory fsync there; do not claim a separate
    # power-loss namespace proof. POSIX explicitly syncs the parent directory.
    if os.name != "nt":
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            _close(fd)


class TerminalJournal:
    """Fixed child directory of the already validated scheduler job-log root."""
    def __init__(self, job_log_root: Path):
        if _FAILED_CLOSES:
            _fail()
        base = Path(job_log_root)
        _ordinary(base, directory=True)
        self.root = base / "scheduler-terminal"
        try:
            self.root.mkdir(mode=0o700)
            _sync_directory(base)
        except FileExistsError:
            pass
        _ordinary(self.root, directory=True)

    def _directory(self, uid, *, create=False):
        if _FAILED_CLOSES or type(uid) is not str or _UID.fullmatch(uid) is None:
            _fail()
        _ordinary(self.root, directory=True)
        path = self.root / uid
        if create:
            try:
                path.mkdir(mode=0o700)
                _sync_directory(self.root)
            except FileExistsError:
                pass
        _ordinary(path, directory=True)
        return path

    def read(self, uid, name):
        if _NAME.fullmatch(name) is None:
            _fail()
        directory = self._directory(uid)
        path = directory / name
        try:
            before = _ordinary(path, directory=False)
        except FileNotFoundError:
            return None
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags)
        try:
            opened = os.fstat(fd)
            if not _matching_open(before, opened) or not 0 < opened.st_size <= MAX_BYTES:
                _fail()
            raw = bytearray()
            while len(raw) <= opened.st_size:
                block = os.read(fd, min(65536, opened.st_size + 1 - len(raw)))
                if not block:
                    break
                raw.extend(block)
            if (len(raw) != opened.st_size or _state(os.fstat(fd)) != _state(opened)
                    or _state(_ordinary(path, directory=False)) != _state(before)):
                _fail()
            _parse(bytes(raw))
            return bytes(raw)
        finally:
            _close(fd)

    def preserve(self, uid, name, value):
        raw = canonical(value)
        if _NAME.fullmatch(name) is None or not 0 < len(raw) <= MAX_BYTES:
            _fail()
        with _LOCK:
            directory = self._directory(uid, create=True)
            previous = self.read(uid, name)
            if previous is not None:
                if previous != raw:
                    _fail()
                # A previous fsync may have failed after all bytes reached the
                # file. Retry durability on that exact existing inode, never
                # truncate or manufacture a replacement observation.
                path = directory / name
                before = _ordinary(path, directory=False)
                fd = os.open(path, os.O_RDWR | getattr(os, "O_BINARY", 0)
                             | getattr(os, "O_NOFOLLOW", 0))
                try:
                    opened = os.fstat(fd)
                    if not _matching_open(before, opened):
                        _fail()
                    os.fsync(fd)
                    if (_state(os.fstat(fd)) != _state(opened)
                            or _state(_ordinary(path, directory=False)) != _state(before)):
                        _fail()
                finally:
                    _close(fd)
                _sync_directory(directory)
                if self.read(uid, name) != raw:
                    _fail()
                return raw
            path = directory / name
            flags = (os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
                     | getattr(os, "O_NOFOLLOW", 0))
            fd = os.open(path, flags, 0o600)
            try:
                before = os.fstat(fd)
                if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                        or before.st_size != 0):
                    _fail()
                offset = 0
                while offset < len(raw):
                    written = os.write(fd, raw[offset:])
                    if written <= 0:
                        _fail()
                    offset += written
                os.fsync(fd)
                os.lseek(fd, 0, os.SEEK_SET)
                observed = bytearray()
                while len(observed) <= len(raw):
                    block = os.read(fd, min(65536, len(raw) + 1 - len(observed)))
                    if not block:
                        break
                    observed.extend(block)
                after = os.fstat(fd)
                if (bytes(observed) != raw or after.st_size != len(raw)
                        or (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino)
                        # NTFS can publish final write timestamps only when
                        # the last writer closes. Verify physical identity and
                        # bytes now; full stable metadata is checked below by
                        # the fresh read-only descriptor after close.
                        or _state(_ordinary(path, directory=False))[:5] != _state(after)[:5]):
                    _fail()
            finally:
                _close(fd)
            _sync_directory(directory)
            if self.read(uid, name) != raw:
                _fail()
            return raw

    def records(self, uid, prefix):
        directory = self._directory(uid)
        for entry in directory.iterdir():
            if _NAME.fullmatch(entry.name) is None:
                _fail()
            if entry.name.startswith(prefix):
                yield entry.name, self.read(uid, entry.name)

    def pending(self):
        # Stream the retained namespace; do not accumulate all prior raw output.
        for entry in self.root.iterdir():
            uid = entry.name
            if _UID.fullmatch(uid) is None:
                _fail()
            self._directory(uid)
            observed = self.read(uid, "OBSERVED")
            if observed is None:
                _fail()  # Partial/unknown originals are not removed or adopted.
            if self.read(uid, "COMMITTED") is None:
                yield uid, _parse(observed)


__all__ = ["TerminalJournal", "TerminalJournalError"]
