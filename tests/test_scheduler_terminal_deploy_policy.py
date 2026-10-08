"""Actual embedded deployment policy, POSIX fd model plus isolated POSIX I/O.

The model runs on Windows too; it is not evidence of Linux ownership/openat.
Recovery bytes are deliberately invalid/partial: installation only checks their
physical namespace and never interprets, repairs, or discards an outcome.
"""
from __future__ import annotations

import builtins
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import sys
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
UID = "a" * 32
RECORDS = ("OBSERVED", "REJECTED", "FAILED_FINALIZATION", "COMMITTED", "PREPARED." + "b" * 64)


def _body(name):
    source = (ROOT / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    return source.split(name + "() {", 1)[1].split("\n}\n", 1)[0]


def _script(name):
    match = re.search(r"<<'PY' \|\| return 2\n(.*?)\nPY", _body(name), re.S)
    assert match is not None
    return match.group(1)


class _PosixModel:
    """Explicit uid/gid/openat model, not a replacement production backend."""
    O_RDONLY, O_WRONLY, O_CREAT, O_EXCL = 0, 1, 0x40, 0x80
    O_DIRECTORY, O_NOFOLLOW, O_CLOEXEC, O_NONBLOCK = 0x100, 0x200, 0x400, 0x800

    def __init__(self):
        self.nodes = {}
        self.handles = {}
        self.mutations = []
        self.next_fd = 10
        self.next_inode = 100
        self.directory("/jobs")

    def node(self, path, *, mode, raw=b"", uid=41, gid=42, nlink=1, attributes=0):
        self.next_inode += 1
        node = dict(st_dev=1, st_ino=self.next_inode, st_mode=mode, st_uid=uid,
                    st_gid=gid, st_nlink=nlink, st_file_attributes=attributes, raw=raw)
        self.nodes[path] = node
        return node

    def directory(self, path, **kwargs):
        return self.node(path, mode=stat.S_IFDIR | 0o700, **kwargs)

    def file(self, path, raw=b"partial original", **kwargs):
        return self.node(path, mode=stat.S_IFREG | 0o600, raw=raw, **kwargs)

    def journal(self):
        self.directory("/jobs/scheduler-terminal")
        self.directory("/jobs/scheduler-terminal/" + UID)
        for index, name in enumerate(RECORDS):
            self.file("/jobs/scheduler-terminal/" + UID + "/" + name,
                      raw=b"" if index == 0 else b'{"partial')
        return self

    def _path(self, path, dir_fd=None):
        if dir_fd is not None:
            path = self.handles[dir_fd][0] + "/" + path
        return str(PurePosixPath(path))

    @staticmethod
    def _metadata(node):
        return SimpleNamespace(**{key: value for key, value in node.items() if key != "raw"})

    def lstat(self, path, *, dir_fd=None):
        path = self._path(path, dir_fd)
        if path not in self.nodes:
            raise FileNotFoundError(path)
        return self._metadata(self.nodes[path])

    def open(self, path, flags, mode=0o777, *, dir_fd=None):
        path = self._path(path, dir_fd)
        if flags & self.O_CREAT:
            if path in self.nodes and flags & self.O_EXCL:
                raise FileExistsError(path)
            if path not in self.nodes:
                self.node(path, mode=stat.S_IFREG | mode)
                self.mutations.append(("create", path))
        if path not in self.nodes:
            raise FileNotFoundError(path)
        node = self.nodes[path]
        if flags & self.O_NOFOLLOW and stat.S_ISLNK(node["st_mode"]):
            raise OSError("MODEL nofollow")
        if flags & self.O_DIRECTORY and not stat.S_ISDIR(node["st_mode"]):
            raise NotADirectoryError(path)
        self.next_fd += 1
        self.handles[self.next_fd] = (path, node)
        return self.next_fd

    def fstat(self, fd):
        return self._metadata(self.handles[fd][1])

    def listdir(self, fd):
        parent = self.handles[fd][0].rstrip("/") + "/"
        return [path[len(parent):] for path in self.nodes
                if path.startswith(parent) and "/" not in path[len(parent):]]

    def close(self, fd):
        del self.handles[fd]

    def write(self, fd, raw):
        path, node = self.handles[fd]
        self.mutations.append(("write", path))
        node["raw"] += raw
        return len(raw)

    def fsync(self, fd):
        assert fd in self.handles

    def fchmod(self, fd, mode):
        path, node = self.handles[fd]
        self.mutations.append(("chmod", path))
        node["st_mode"] = stat.S_IFMT(node["st_mode"]) | mode

    def unlink(self, path, *, dir_fd=None):
        path = self._path(path, dir_fd)
        self.mutations.append(("unlink", path))
        del self.nodes[path]


def _run(model, name="validate_scheduler_terminal_job_log_tree"):
    real_import = builtins.__import__
    def import_model(module, *args, **kwargs):
        if module == "os":
            return model
        if module == "sys":
            return SimpleNamespace(argv=["-", "/jobs", "41", "42"])
        return real_import(module, *args, **kwargs)
    namespace = {"__builtins__": dict(vars(builtins), __import__=import_model)}
    exec(compile(_script(name), name, "exec"), namespace)


def _snapshot(model):
    return {path: dict(node) for path, node in model.nodes.items()}


def test_prepare_migrate_and_final_find_use_the_one_closed_validator():
    prepare = _body("prepare_probiga_job_log_root")
    migrate = _body("migrate_probiga_job_log_legacy_modes")
    for body in (prepare, migrate):
        assert body.count("validate_scheduler_terminal_job_log_tree || return 2") == 2
        assert body.index("validate_scheduler_terminal_job_log_tree || return 2") < body.index("<<'PY'")
        assert body.rindex("validate_scheduler_terminal_job_log_tree || return 2") > body.index("\nPY")
        assert 'name == "scheduler-terminal"' in body
    assert "-name acquisition-shards -o -name scheduler-terminal" in migrate
    validator = _script("validate_scheduler_terminal_job_log_tree")
    assert not any(operation in validator for operation in ("os.write(", "os.unlink(", "os.chmod(", "os.fchmod(", "json.loads("))


@pytest.mark.parametrize("layout", ["absent", "empty_root", "empty_run", "all_records"])
def test_exact_journal_and_empty_or_partial_originals_are_preserved(layout):
    model = _PosixModel()
    if layout != "absent":
        model.directory("/jobs/scheduler-terminal")
    if layout == "empty_run":
        model.directory("/jobs/scheduler-terminal/" + UID)
    elif layout == "all_records":
        model.journal()
    original = _snapshot(model)
    for name in ("validate_scheduler_terminal_job_log_tree", "prepare_probiga_job_log_root",
                 "migrate_probiga_job_log_legacy_modes", "validate_scheduler_terminal_job_log_tree"):
        _run(model, name)
    assert _snapshot(model) == original
    assert not model.handles
    assert not any("scheduler-terminal" in path for _, path in model.mutations)


@pytest.mark.parametrize("path,field,value", [
    ("/jobs", "st_uid", 99), ("/jobs", "st_gid", 99), ("/jobs", "st_mode", stat.S_IFDIR | 0o755),
    ("/jobs/scheduler-terminal", "st_uid", 99),
    ("/jobs/scheduler-terminal", "st_gid", 99),
    ("/jobs/scheduler-terminal", "st_mode", stat.S_IFDIR | 0o755),
    ("/jobs/scheduler-terminal", "st_mode", stat.S_IFREG | 0o600),
    ("/jobs/scheduler-terminal/" + UID, "st_mode", stat.S_IFDIR | 0o755),
    ("/jobs/scheduler-terminal/" + UID, "st_uid", 99),
    ("/jobs/scheduler-terminal/" + UID, "st_gid", 99),
    ("/jobs/scheduler-terminal/" + UID + "/OBSERVED", "st_mode", stat.S_IFREG | 0o644),
    ("/jobs/scheduler-terminal/" + UID + "/OBSERVED", "st_mode", stat.S_IFDIR | 0o700),
    ("/jobs/scheduler-terminal/" + UID + "/OBSERVED", "st_mode", stat.S_IFLNK | 0o600),
    ("/jobs/scheduler-terminal/" + UID + "/OBSERVED", "st_nlink", 2),
    ("/jobs/scheduler-terminal/" + UID + "/OBSERVED", "st_uid", 99),
    ("/jobs/scheduler-terminal/" + UID + "/OBSERVED", "st_gid", 99),
    ("/jobs/scheduler-terminal/" + UID + "/OBSERVED", "st_file_attributes", 0x400),
])
def test_unsafe_physical_entries_are_rejected_without_repair(path, field, value):
    model = _PosixModel().journal()
    model.nodes[path][field] = value
    original = _snapshot(model)
    with pytest.raises((SystemExit, OSError)):
        _run(model)
    assert _snapshot(model) == original
    assert not model.handles and not model.mutations


@pytest.mark.parametrize("relative,directory", [
    ("UNKNOWN", True), ("A" * 32, True), ("b" * 31, True),
    (UID + "/UNKNOWN", False), (UID + "/observed", False),
    (UID + "/PREPARED." + "A" * 64, False),
    (UID + "/PREPARED." + "b" * 63, False),
    (UID + "/nested", True), (UID + "/OBSERVED.extra", False),
])
def test_unknown_names_and_nested_entries_are_never_adopted(relative, directory):
    model = _PosixModel().journal()
    path = "/jobs/scheduler-terminal/" + relative
    (model.directory if directory else model.file)(path)
    original = _snapshot(model)
    with pytest.raises(SystemExit):
        _run(model)
    assert _snapshot(model) == original
    assert not model.handles and not model.mutations


@pytest.mark.parametrize("path", ["/jobs", "/jobs/scheduler-terminal", "/jobs/scheduler-terminal/" + UID])
def test_symlink_directory_is_rejected(path):
    model = _PosixModel().journal()
    model.nodes[path]["st_mode"] = stat.S_IFLNK | 0o700
    with pytest.raises((SystemExit, OSError)):
        _run(model)
    assert not model.handles and not model.mutations


def test_record_replacement_during_open_is_detected_and_both_bytes_are_kept():
    model = _PosixModel().journal()
    original_open = model.open
    replaced = []
    path = "/jobs/scheduler-terminal/" + UID + "/OBSERVED"
    def substitute(name, flags, mode=0o777, *, dir_fd=None):
        fd = original_open(name, flags, mode, dir_fd=dir_fd)
        if model._path(name, dir_fd) == path:
            replaced.append(model.handles[fd][1])
            model.file(path, raw=b"replacement")
        return fd
    model.open = substitute
    with pytest.raises(SystemExit, match="record changed"):
        _run(model)
    assert replaced[0]["raw"] == b"" and model.nodes[path]["raw"] == b"replacement"
    assert not model.handles


def test_new_unknown_entry_during_inventory_is_not_ignored():
    model = _PosixModel().journal()
    original_listdir = model.listdir
    visits = 0
    def append_unknown(fd):
        nonlocal visits
        if model.handles[fd][0] == "/jobs/scheduler-terminal/" + UID:
            visits += 1
            if visits == 2:
                model.file(model.handles[fd][0] + "/UNKNOWN", raw=b"unknown retained")
        return original_listdir(fd)
    model.listdir = append_unknown
    with pytest.raises(SystemExit, match="namespace changed"):
        _run(model)
    assert model.nodes["/jobs/scheduler-terminal/" + UID + "/UNKNOWN"]["raw"] == b"unknown retained"
    assert not model.handles


@pytest.mark.skipif(os.name != "posix", reason="actual production ownership/openat needs POSIX")
def test_actual_posix_originals_are_kept_and_links_are_rejected(tmp_path):
    jobs = tmp_path / "jobs"
    run = jobs / "scheduler-terminal" / UID
    run.mkdir(mode=0o700, parents=True)
    jobs.chmod(0o700)
    run.parent.chmod(0o700)
    for index, name in enumerate(RECORDS):
        path = run / name
        path.write_bytes(b"" if index == 0 else b'{"partial')
        path.chmod(0o600)
    original = {path.name: (path.stat().st_ino, path.read_bytes()) for path in run.iterdir()}
    def check():
        return subprocess.run([sys.executable, "-I", "-", str(jobs), str(os.geteuid()), str(os.getegid())],
                              input=_script("validate_scheduler_terminal_job_log_tree"),
                              text=True, capture_output=True, timeout=5)
    assert check().returncode == 0
    external = tmp_path / "external"
    external.write_bytes(b"external original")
    external.chmod(0o600)
    suspicious = run / ("PREPARED." + "c" * 64)
    suspicious.symlink_to(external)
    assert check().returncode != 0
    suspicious.unlink()
    os.link(external, suspicious)
    assert check().returncode != 0
    assert external.read_bytes() == b"external original"
    assert {name: ((run / name).stat().st_ino, (run / name).read_bytes()) for name in original} == original
