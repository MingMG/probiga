#!/usr/bin/env python3
"""Classify a complete, trusted Git release delta without importing project code.

Explicit schema files, the strategy-schema preparation tools and the protected
QMT release controller path are cross-end. Application, UI, deployment,
dependency and unrelated tool changes remain normal Linux code releases; the
Windows/QMT updater tracks its own strategy source independently. Schema
transitions live under ``server/db``, in ``.sql`` files, in the guarded
strategy-schema preparation tools, or in the QMT release controller path.
Run this installed trusted tool, never the copy from an untrusted target commit.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from dataclasses import dataclass


SCHEMA = "probiga.release-scope.v3"
_SHA = re.compile(r"[0-9a-fA-F]{40}\Z")
_REGULAR_MODES = frozenset({"100644", "100755"})


class ScopeError(ValueError):
    """Only fixed, non-sensitive error codes may escape the Git boundary."""


@dataclass(frozen=True)
class Entry:
    mode: str
    oid: str


def _git_executable() -> str:
    # Do not resolve executables through caller-controlled PATH or repository
    # files. Production runs the system Git; these Windows locations also permit
    # the exact same policy tests to run on the development endpoint.
    candidates = (
        ("C:/Program Files/Git/cmd/git.exe", "C:/Program Files/Git/bin/git.exe")
        if os.name == "nt" else ("/usr/bin/git", "/bin/git")
    )
    for candidate in candidates:
        if Path(candidate).is_file():
            return candidate
    raise ScopeError("trusted_git_unavailable")


def _git_environment() -> dict[str, str]:
    # In particular, discard GIT_CONFIG_COUNT, object-directory/alternate
    # overrides, replace refs, external diff, SSH commands and lazy-fetch knobs.
    # cat-file/ls-tree never apply smudge, clean, textconv or external filters.
    env = {
        "PATH": os.defpath,
        "LANG": "C",
        "LC_ALL": "C",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_SYSTEM": os.devnull,
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_ATTR_NOSYSTEM": "1",
        "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_NO_LAZY_FETCH": "1",
        "GIT_OPTIONAL_LOCKS": "0",
        "GIT_TERMINAL_PROMPT": "0",
    }
    if os.name == "nt":
        env["SYSTEMROOT"] = "C:\\Windows"
    return env


class Git:
    def __init__(self, *, repository: str | Path | None = None,
                 git_dir: str | Path | None = None):
        if (repository is None) == (git_dir is None):
            raise ScopeError("exactly_one_repository_required")
        location = Path(repository if repository is not None else git_dir)
        if not location.is_absolute() or not location.is_dir():
            raise ScopeError("repository_must_be_absolute_directory")
        self.command = [
            _git_executable(), "--no-replace-objects", "--literal-pathspecs",
            "-c", "core.hooksPath=" + os.devnull,
            "-c", "core.fsmonitor=false", "-c", "core.preloadIndex=false",
            "-c", "core.attributesFile=" + os.devnull,
            "-c", "diff.external=", "-c", "protocol.allow=never",
        ]
        self.command += (["-C", str(location)] if repository is not None
                         else ["--git-dir=" + str(location)])
        self.environment = _git_environment()

    def run(self, *args: str, allow_false: bool = False,
            input_data: bytes | None = None) -> bytes | None:
        try:
            result = subprocess.run(
                self.command + list(args), env=self.environment,
                input=input_data,
                stdin=subprocess.DEVNULL if input_data is None else None,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL, timeout=120, check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ScopeError("git_read_failed") from exc
        if allow_false and result.returncode == 1:
            return None
        if result.returncode != 0:
            raise ScopeError("git_read_failed")
        return result.stdout

    def tree(self, sha: str) -> dict[str, Entry]:
        raw = self.run("ls-tree", "-r", "-t", "-z", "--full-tree", sha)
        entries: dict[str, Entry] = {}
        directories: set[str] = set()
        paths: set[str] = set()
        for record in raw.split(b"\0"):
            if not record:
                continue
            try:
                metadata, path_bytes = record.split(b"\t", 1)
                mode, kind, oid = metadata.decode("ascii").split(" ")
                path = path_bytes.decode("utf-8", errors="strict")
            except (ValueError, UnicodeError) as exc:
                raise ScopeError("unsafe_tree_entry") from exc
            parts = path.split("/")
            if (not path or "\\" in path or ":" in path
                    or any(ord(char) < 32 or ord(char) == 127 for char in path)
                    or any(part in {"", ".", ".."} for part in parts)
                    or any(part.endswith((".", " ")) for part in parts)
                    or any(part.rstrip(". ").lower() == ".git" for part in parts)
                    or path in paths):
                raise ScopeError("unsafe_tree_entry")
            paths.add(path)
            if mode == "040000" and kind == "tree" and _SHA.fullmatch(oid):
                directories.add(path)
                continue
            # Symlinks and gitlinks are not ordinary release files. Reject the
            # classification outright, including when hidden in docs/static;
            # COORDINATED must not become a way to approve unsafe archive paths.
            if mode not in _REGULAR_MODES or kind != "blob" or not _SHA.fullmatch(oid):
                raise ScopeError("unsafe_tree_entry")
            entries[path] = Entry(mode, oid)
        # Ordinary Git commits do not contain empty directories. Inspect them
        # too so a hand-crafted empty .git/../ tree cannot evade path checks.
        parents = {path.rsplit("/", 1)[0] for path in paths if "/" in path}
        if not directories.issubset(parents):
            raise ScopeError("unsafe_tree_entry")
        return entries

    def blobs(self, oids: list[str]) -> list[bytes]:
        if not oids:
            return []
        raw = self.run(
            "cat-file", "--batch",
            input_data=("\n".join(oids) + "\n").encode("ascii"),
        )
        result: list[bytes] = []
        offset = 0
        for expected in oids:
            header_end = raw.find(b"\n", offset)
            if header_end < 0:
                raise ScopeError("git_read_failed")
            header = raw[offset:header_end].split(b" ")
            if (len(header) != 3 or header[0].decode("ascii", "ignore") != expected
                    or header[1] != b"blob"):
                raise ScopeError("git_read_failed")
            try:
                size = int(header[2])
            except ValueError as exc:
                raise ScopeError("git_read_failed") from exc
            start = header_end + 1
            end = start + size
            if size < 0 or end >= len(raw) or raw[end:end + 1] != b"\n":
                raise ScopeError("git_read_failed")
            result.append(raw[start:end])
            offset = end + 1
        return result


_DDL_SQL = re.compile(
    r"\b(?:CREATE\s+(?:TABLE|TRIGGER|INDEX|VIEW)|ALTER\s+TABLE|"
    r"DROP\s+(?:TABLE|TRIGGER|INDEX|VIEW))\b",
    re.IGNORECASE,
)
_DDL_CODE_ROOTS = ("server/db/", "db/")
_CROSS_END_CONTRACT_CODE_FILES = frozenset({
    "server/common/limit_up_event_schema.py",
    "server/common/qmt_edge_release_recovery.py",
    "tools/prepare_strategy_governance_deferred_schema.py",
    "tools/prepare_strategy_governance_qmt_history.py",
    "tools/prepare_strategy_governance_schema.py",
    "tools/reload_big_qmt_strategy.ps1",
    "tools/resume_qmt_prior_edge.ps1",
    "tools/run_big_qmt_bridge.py",
    "tools/run_qmt_windows_edge_release_bootstrap.py",
    "tools/update_qmt_windows_edge.ps1",
})


def _ddl_strings(path: str, blob: bytes) -> list[str]:
    if path.endswith(".sql"):
        text = blob.decode("utf-8", "replace")
        return [text] if _DDL_SQL.search(text) else []
    try:
        tree = ast.parse(blob)
    except (SyntaxError, ValueError):
        text = blob.decode("utf-8", "replace")
        return [text] if _DDL_SQL.search(text) else []
    strings: list[str] = []
    for node in ast.walk(tree):
        if (isinstance(node, ast.Constant) and isinstance(node.value, str)
                and _DDL_SQL.search(node.value)):
            strings.append(node.value.replace("\r\n", "\n"))
    return strings


def _contract(git: Git, tree: dict[str, Entry]) -> str:
    paths = [
        path for path in sorted(tree)
        if path in _CROSS_END_CONTRACT_CODE_FILES
        or path.endswith(".sql")
        or (path.endswith(".py") and path.startswith(_DDL_CODE_ROOTS))
    ]
    records = []
    for path, blob in zip(paths, git.blobs([tree[path].oid for path in paths])):
        if path in _CROSS_END_CONTRACT_CODE_FILES:
            records.append({
                "path": path,
                "sha256": hashlib.sha256(blob).hexdigest(),
            })
            continue
        strings = _ddl_strings(path, blob)
        if strings:
            records.append([path, strings])
    # The schema and policy version are bound into the digest.  Only executable
    # DDL string constants participate, so ordinary code edits stay fast while a
    # real schema transition still selects the coordinated database cutover.
    canonical = json.dumps({"schema": SCHEMA, "files": records},
                           ensure_ascii=True, separators=(",", ":")).encode("ascii")
    return hashlib.sha256(canonical).hexdigest()


def classify_release_scope(*, base_sha: str, target_sha: str,
                           repository: str | Path | None = None,
                           git_dir: str | Path | None = None) -> dict[str, object]:
    if not _SHA.fullmatch(base_sha) or not _SHA.fullmatch(target_sha):
        raise ScopeError("full_commit_sha_required")
    base_sha, target_sha = base_sha.lower(), target_sha.lower()
    git = Git(repository=repository, git_dir=git_dir)
    for sha in {base_sha, target_sha}:
        if git.run("cat-file", "-t", sha) != b"commit\n":
            raise ScopeError("commit_object_required")
    if git.run("merge-base", "--is-ancestor", base_sha, target_sha,
               allow_false=True) is None:
        raise ScopeError("base_not_ancestor")
    base, target = git.tree(base_sha), git.tree(target_sha)
    # Comparing both complete committed trees is the full base→target diff.
    # It cannot omit an earlier commit as target^ would. Rename detection is
    # deliberately unnecessary: every rename includes a forbidden deletion.
    changed = sorted(path for path in base.keys() | target.keys()
                     if base.get(path) != target.get(path))
    base_contract = _contract(git, base)
    target_contract = _contract(git, target)
    reasons: set[str] = set()
    if base_contract != target_contract:
        reasons.add("schema_contract_changed")
    return {
        "schema": SCHEMA,
        "base_sha": base_sha,
        "target_sha": target_sha,
        "scope": "COORDINATED" if reasons else "LINUX",
        "changed_paths": changed,
        "reason_codes": sorted(reasons) if reasons else ["code_only_delta"],
        "base_contract_sha256": base_contract,
        "contract_sha256": target_contract,
    }


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        # argparse normally echoes unrecognized inputs, which may contain a
        # caller's sensitive path or token. Keep errors as fixed codes instead.
        print(json.dumps({"schema": SCHEMA, "error": "invalid_arguments"}), file=sys.stderr)
        raise SystemExit(2)


def main(argv: list[str] | None = None) -> int:
    parser = _Parser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--repository")
    source.add_argument("--git-dir")
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--target-sha", required=True)
    args = parser.parse_args(argv)
    try:
        result = classify_release_scope(**vars(args))
    except ScopeError as exc:
        print(json.dumps({"schema": SCHEMA, "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
