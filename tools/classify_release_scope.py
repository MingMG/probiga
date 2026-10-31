#!/usr/bin/env python3
"""Classify a complete, trusted Git release delta without importing project code.

Schema transitions live under ``server/db``, in ``.sql`` files, in the guarded
strategy-schema preparation tools, or in the QMT release controller path.
Application, UI, deployment, dependency and unrelated tool changes remain normal
Linux code releases; the Windows/QMT updater tracks its own strategy source
independently.

The cross-end contract for the policy files below is the set of their
module-level bindings, not their whole bytes.  A change to a declaration
(threshold, task table, receipt schema, protocol constant) therefore selects the
coordinated release, while an ordinary change to a function body, a docstring or
a comment stays a single-end Linux release.  PowerShell controllers have no
trusted stdlib parser, so they stay whole-file: that is a deliberate, documented
boundary rather than an oversight.

Run this installed trusted tool, never the copy from an untrusted target commit.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
from dataclasses import dataclass


SCHEMA = "probiga.release-scope.v4"
_CONTRACT_POLICY_VERSION = 2
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

# Python contract files contribute their module-level bindings only.  Listing the
# files (instead of every contract symbol) keeps the policy reviewable while the
# extraction rule stays mechanical: a symbol that is added, removed or revalued
# anywhere in these files changes the digest, and no hand-maintained symbol list
# can silently downgrade a release to LINUX by omission.
_CONTRACT_PYTHON_FILES = (
    "server/api/scheduler_runtime.py",
    "server/common/limit_up_event_schema.py",
    "server/common/qmt_daily_market_truth.py",
    "server/common/qmt_datadir_result.py",
    "server/common/qmt_edge_release_receipt.py",
    "server/common/qmt_edge_release_recovery.py",
    "server/common/scheduler_args.py",
    "server/common/scheduler_runtime_health.py",
    "server/common/scheduler_script_policy.py",
    "server/common/scheduler_task_retirement.py",
    "server/common/scheduler_tasks.py",
    "server/common/scheduler_validation.py",
    "tools/apply_public_market_profile.py",
    "tools/import_qmt_datadir_daily.py",
    "tools/import_qmt_datadir_history.py",
    "tools/import_qmt_datadir_index.py",
    "tools/prepare_strategy_governance_deferred_schema.py",
    "tools/prepare_strategy_governance_qmt_history.py",
    "tools/prepare_strategy_governance_schema.py",
    "tools/qmt_host_ownership_contract.py",
    "tools/run_big_qmt_bridge.py",
    "tools/run_qmt_windows_edge_release_bootstrap.py",
)

# Whole-file contract entries are files whose bytes, not whose bindings, are
# what the other endpoint agrees on.  PowerShell controllers belong here because
# there is no standard-library parser with stable output, so a binding-level
# rule cannot be applied without inventing one.  The QMT strategy source belongs
# here because the Linux API compares it byte for byte: qmt_strategy_bridge_proof
# hashes the source from the server's own checkout and requires the Windows
# bridge to report that exact SHA-256 and Git blob.  A release that changed this
# file without moving Windows would therefore leave the two endpoints unable to
# agree, so it must classify as COORDINATED instead of a Linux-only delta.
_CONTRACT_WHOLE_FILE_FILES = (
    "integrations/bigqmt/qmt_strategy/probiga_big_qmt_bridge.py",
    "tools/reload_big_qmt_strategy.ps1",
    "tools/resume_qmt_prior_edge.ps1",
    "tools/start_local_live_services.ps1",
    "tools/update_qmt_windows_edge.ps1",
)

# The export list is packaging, not a runtime contract with the other endpoint.
_CONTRACT_EXCLUDED_NAMES = frozenset({"__all__"})

_CONTAINER_LITERAL_CALLS = {
    "frozenset": frozenset, "list": list, "set": set, "tuple": tuple,
}


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


def _contract_policy() -> dict[str, object]:
    # Bind the policy itself into the digest so that adding, dropping or
    # retyping a policy path is itself a visible contract transition.
    if (set(_CONTRACT_PYTHON_FILES) & set(_CONTRACT_WHOLE_FILE_FILES)
            or any(not path.endswith(".py") for path in _CONTRACT_PYTHON_FILES)
            or any(not path.endswith((".py", ".ps1"))
                   for path in _CONTRACT_WHOLE_FILE_FILES)):
        raise ScopeError("contract_policy_invalid")
    return {
        "version": _CONTRACT_POLICY_VERSION,
        "python": list(_CONTRACT_PYTHON_FILES),
        "whole_file": list(_CONTRACT_WHOLE_FILE_FILES),
        "excluded": sorted(_CONTRACT_EXCLUDED_NAMES),
    }


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"),
                      allow_nan=False)


def _canonical_value(value: object) -> object:
    # Only a normalised, type-preserving projection may enter the digest: two
    # spellings of the same value must hash alike, and two different values must
    # never hash alike.
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ScopeError("contract_value_unsupported")
        return value
    if isinstance(value, (bytes, bytearray)):
        return {"bytes": bytes(value).hex()}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_canonical_value(item) for item in value),
                      key=_canonical_json)
    if isinstance(value, dict):
        # Order-preserving: reordering a mapping literal is a real behavioural
        # delta in iteration order, so ``sort_keys`` would hide it.
        return [[_canonical_value(key), _canonical_value(item)]
                for key, item in value.items()]
    raise ScopeError("contract_value_unsupported")


def _literal_value(node: ast.AST) -> tuple[bool, object]:
    try:
        return True, ast.literal_eval(node)
    except (ValueError, TypeError, SyntaxError, MemoryError, RecursionError):
        pass
    # ``frozenset({...})`` is a container call, not a bare literal, yet it is
    # still declared content.  Evaluate exactly one positional literal argument.
    if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            and node.func.id in _CONTAINER_LITERAL_CALLS
            and len(node.args) == 1 and not node.keywords):
        found, inner = _literal_value(node.args[0])
        if found and isinstance(inner, (list, tuple, set, frozenset, dict)):
            try:
                return True, _CONTAINER_LITERAL_CALLS[node.func.id](inner)
            except TypeError:
                # Unhashable elements: fall back to the expression text rather
                # than letting a TypeError escape as an unclassified crash.
                return False, None
    return False, None


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _source_record(text: str, *, name: str | None = None) -> dict[str, object]:
    # A computed declaration is still declared content: hash the normalised
    # expression so that reformatting is free and any real edit is not.
    return {"name": name, "mode": "source", "sha256": _digest(text)}


def _binding_record(name: str, value: ast.AST) -> dict[str, object]:
    found, literal = _literal_value(value)
    if found:
        return {"name": name, "mode": "value",
                "sha256": _digest(_canonical_json(_canonical_value(literal)))}
    return _source_record(ast.unparse(value), name=name)


def _module_bindings(module: ast.Module) -> list[dict[str, object]]:
    bindings: list[dict[str, object]] = []
    names: set[str] = set()
    for statement in module.body:
        if isinstance(statement, ast.AnnAssign):
            if statement.value is None:
                continue
            targets = [statement.target]
        elif isinstance(statement, ast.Assign):
            targets = statement.targets
        else:
            continue
        name = (
            targets[0].id
            if len(targets) == 1 and isinstance(targets[0], ast.Name) else None
        )
        if name is None:
            # Multi-target, attribute or subscript assignment: the whole
            # statement is the declaration, targets included.
            bindings.append(_source_record(ast.unparse(statement)))
            continue
        if name in _CONTRACT_EXCLUDED_NAMES:
            continue
        if name in names:
            raise ScopeError("contract_symbol_duplicate")
        names.add(name)
        bindings.append(_binding_record(name, statement.value))
    return bindings


def _contract_file_record(path: str, blob: bytes, *,
                          strict: bool) -> dict[str, object]:
    if path in _CONTRACT_WHOLE_FILE_FILES:
        return {"path": path, "kind": "whole_file",
                "sha256": hashlib.sha256(blob).hexdigest()}
    try:
        bindings = _module_bindings(ast.parse(blob))
    except ScopeError:
        # ScopeError is a ValueError: this clause must precede the parse-error
        # clause or a precise contract code would be reported as a parse fault.
        if strict:
            raise
        return {"path": path, "kind": "python_bindings", "state": "unparsable"}
    except (SyntaxError, ValueError):
        if strict:
            raise ScopeError("contract_parse_failed") from None
        return {"path": path, "kind": "python_bindings", "state": "unparsable"}
    if not bindings:
        if strict:
            raise ScopeError("contract_extraction_empty")
        return {"path": path, "kind": "python_bindings", "state": "empty"}
    return {"path": path, "kind": "python_bindings", "bindings": bindings}


def _contract(git: Git, tree: dict[str, Entry], *, strict: bool) -> str:
    policy_paths = _CONTRACT_PYTHON_FILES + _CONTRACT_WHOLE_FILE_FILES
    ddl_paths = [
        path for path in sorted(tree)
        if path.endswith(".sql")
        or (path.endswith(".py") and path.startswith(_DDL_CODE_ROOTS))
    ]
    wanted = ddl_paths + [path for path in policy_paths if path in tree]
    blobs = dict(zip(wanted, git.blobs([tree[path].oid for path in wanted])))
    records: list[object] = []
    for path in ddl_paths:
        strings = _ddl_strings(path, blobs[path])
        if strings:
            records.append([path, strings])
    for path in policy_paths:
        # A policy file absent from one side is a marker, not an error: a
        # legitimate deletion must classify as a contract change, and the base
        # tree predates any file this release introduces.
        records.append(
            _contract_file_record(path, blobs[path], strict=strict)
            if path in blobs else {"path": path, "kind": "absent"}
        )
    # The schema and policy version are bound into the digest.  Only executable
    # DDL string constants and declared bindings participate, so ordinary code
    # edits stay fast while a real schema or contract transition still selects
    # the coordinated database cutover.
    canonical = json.dumps(
        {"schema": SCHEMA, "policy": _contract_policy(), "files": records},
        ensure_ascii=True, separators=(",", ":"), allow_nan=False,
    ).encode("ascii")
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
    # The target tree is the release being authorised, so an unreadable or
    # unextractable contract file aborts instead of silently classifying as
    # LINUX.  The base tree is history that no release can repair, and a same-SHA
    # recomputation (the deploy drift check) must not depend on parsability.
    paired = base_sha != target_sha
    base_contract = _contract(git, base, strict=False)
    target_contract = _contract(git, target, strict=paired)
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
