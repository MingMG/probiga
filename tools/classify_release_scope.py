#!/usr/bin/env python3
"""Classify a complete, trusted Git release delta without importing project code.

LINUX means the API and scheduler may move together without moving Windows/QMT.
Everything outside the small reviewed list is COORDINATED. New private modules
must be reviewed and added here; there is intentionally no CLI policy override.
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


SCHEMA = "probiga.release-scope.v1"
_SHA = re.compile(r"[0-9a-fA-F]{40}\Z")
_PRIVATE_MODULES = frozenset({
    "server/api/data_monitor.py",
    "server/api/routers/trading_day.py",
    "server/common/trading_day_store.py",
})
_MAIN = "server/api/main.py"
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

    def run(self, *args: str, allow_false: bool = False) -> bytes | None:
        try:
            result = subprocess.run(
                self.command + list(args), env=self.environment,
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
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


def _excluded(path: str) -> bool:
    return (path.startswith(("server/static/", "docs/", "tests/"))
            or path in _PRIVATE_MODULES)


def _journal_registration(node: ast.AST) -> bool:
    if not isinstance(node, ast.Expr) or not isinstance(node.value, ast.Call):
        return False
    call = node.value
    return (
        isinstance(call.func, ast.Attribute) and call.func.attr == "include_router"
        and isinstance(call.func.value, ast.Name) and call.func.value.id == "app"
        and len(call.args) == 1 and isinstance(call.args[0], ast.Attribute)
        and call.args[0].attr == "router"
        and isinstance(call.args[0].value, ast.Name)
        and call.args[0].value.id == "trading_day"
        and len(call.keywords) == 1 and call.keywords[0].arg == "prefix"
        and isinstance(call.keywords[0].value, ast.Constant)
        and call.keywords[0].value.value == "/api"
    )


def _sanitized_main(blob: bytes) -> str | None:
    try:
        tree = ast.parse(blob)
    except (SyntaxError, ValueError, UnicodeError):
        return None
    body: list[ast.stmt] = []
    imports = registrations = 0
    for node in tree.body:
        if (isinstance(node, ast.ImportFrom) and node.level == 0
                and node.module == "server.api.routers"):
            names = []
            for alias in node.names:
                if alias.name == "trading_day" and alias.asname is None:
                    imports += 1
                else:
                    names.append(alias)
            node.names = names
            if not names:
                continue
        if _journal_registration(node):
            registrations += 1
            continue
        body.append(node)
    if imports > 1 or registrations > 1 or imports != registrations:
        return None
    tree.body = body
    return ast.dump(tree, annotate_fields=True, include_attributes=False)


def _contract(git: Git, tree: dict[str, Entry]) -> tuple[str, str | None]:
    records = []
    main_ast = None
    for path, entry in sorted(tree.items()):
        if _excluded(path):
            continue
        content = "git-blob:" + entry.oid
        if path == _MAIN:
            main_ast = _sanitized_main(git.run("cat-file", "blob", entry.oid))
            if main_ast is not None:
                content = "python-ast:" + main_ast
        records.append([path, entry.mode, content])
    # The schema and policy version are bound into the digest. Git blob IDs bind
    # every non-excluded file, including deployment tools and runtime identity.
    canonical = json.dumps({"schema": SCHEMA, "files": records},
                           ensure_ascii=True, separators=(",", ":")).encode("ascii")
    return hashlib.sha256(canonical).hexdigest(), main_ast


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
    base_contract, base_ast = _contract(git, base)
    target_contract, target_ast = _contract(git, target)
    reasons: set[str] = set()
    for path in changed:
        old, new = base.get(path), target.get(path)
        if new is None:
            reasons.add("deleted_or_renamed_path")
        elif old is not None and old.mode != new.mode:
            reasons.add("file_mode_changed")
        elif old is None and new.mode == "100755":
            reasons.add("executable_added")
        elif _excluded(path):
            continue
        elif path == _MAIN and old is not None and base_ast is not None and target_ast == base_ast:
            continue
        else:
            reasons.add("unreviewed_runtime_path")
    if base_contract != target_contract:
        reasons.add("runtime_contract_changed")
    return {
        "schema": SCHEMA,
        "base_sha": base_sha,
        "target_sha": target_sha,
        "scope": "COORDINATED" if reasons else "LINUX",
        "changed_paths": changed,
        "reason_codes": sorted(reasons) if reasons else ["reviewed_linux_only_delta"],
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
