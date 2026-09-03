"""Fail-closed scheduler script path policy.

The scheduler may receive task rows from storage, so a stored ``script_path``
is data rather than authority.  Only repository-owned Python files may be
resolved, and research-only Trading V4/V5/V6 code is never schedulable.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import subprocess

from server.common.release_manifest import (
    MANIFEST_FILE_NAME,
    ReleaseManifestError,
    verify_runtime_release_manifest,
)


class SchedulerScriptPolicyError(ValueError):
    """Raised when a scheduler row names an unsafe executable path."""


FORBIDDEN_RESEARCH_MARKERS = (
    "trading_v4",
    "trading_v5",
    "trading_v6",
)
ALLOWED_SCHEDULER_ROOTS = {"biz", "tools"}


def resolve_scheduler_script(
    root: Path,
    script_path: object,
    *,
    require_git_tracked: bool = True,
) -> Path:
    """Resolve one repository-relative Python script without following links."""

    if (
        not isinstance(script_path, str)
        or not script_path
        or script_path != script_path.strip()
        or "\\" in script_path
        or "//" in script_path
    ):
        raise SchedulerScriptPolicyError("script_path must be canonical text")
    relative = PurePosixPath(script_path)
    if (
        relative.is_absolute()
        or relative.as_posix() != script_path
        or any(part in {"", ".", ".."} for part in relative.parts)
        or relative.suffix.lower() != ".py"
    ):
        raise SchedulerScriptPolicyError(
            "script_path must be a canonical relative Python file"
        )
    lowered_parts = tuple(part.lower() for part in relative.parts)
    if not lowered_parts or lowered_parts[0] not in ALLOWED_SCHEDULER_ROOTS:
        raise SchedulerScriptPolicyError(
            "scheduler scripts must live under biz/ or tools/"
        )
    if any(
        marker in part
        for marker in FORBIDDEN_RESEARCH_MARKERS
        for part in lowered_parts
    ):
        raise SchedulerScriptPolicyError(
            "Trading V4/V5/V6 research code cannot be scheduled"
        )

    lexical_root = root.resolve()
    lexical = lexical_root / Path(*relative.parts)
    _reject_reparse_points(lexical, lexical_root)
    resolved = lexical.resolve(strict=False)
    try:
        resolved.relative_to(lexical_root)
    except ValueError as exc:
        raise SchedulerScriptPolicyError(
            "script_path escapes the repository root"
        ) from exc
    if require_git_tracked:
        _require_clean_head_file(lexical_root, relative.as_posix())
    return resolved


def _require_clean_head_file(root: Path, relative_path: str) -> None:
    """Bind the executable bytes to the sealed release and Git HEAD.

    Do not use ``git diff`` here.  Scheduler children share a memory-limited
    cgroup and a throttled diff process can exceed the policy timeout even
    when the immutable release file is unchanged.  Two read-only object
    lookups plus an in-process Git blob digest prove the same property without
    refreshing the index or walking the worktree.
    """

    git_environment = os.environ.copy()
    git_environment["GIT_OPTIONAL_LOCKS"] = "0"
    command_prefix = ["git", "-c", f"safe.directory={root}"]

    def git_stdout(*arguments: str) -> str:
        last_error: BaseException | None = None
        command_name = " ".join(arguments[:2])
        # A timeout is operational throttling, not evidence of drift.  Retry
        # once, then remain fail-closed if Git still cannot attest the file.
        for attempt in range(2):
            try:
                completed = subprocess.run(
                    [*command_prefix, *arguments],
                    cwd=root,
                    check=True,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="strict",
                    env=git_environment,
                    timeout=30,
                )
                return str(completed.stdout or "")
            except subprocess.TimeoutExpired as exc:
                last_error = exc
                if attempt == 0:
                    continue
                break
            except (OSError, subprocess.SubprocessError) as exc:
                last_error = exc
                break
        error_type = type(last_error).__name__
        exit_code = (
            getattr(last_error, "returncode", None)
            if last_error is not None
            else None
        )
        detail = (
            f"command={command_name} error={error_type}"
            + (f" exit_code={exit_code}" if exit_code is not None else "")
        )
        raise SchedulerScriptPolicyError(
            "scheduler script is not an unchanged file from Git HEAD: "
            + detail
        ) from last_error

    try:
        revision_lines = [
            line.strip().lower()
            for line in git_stdout(
                "rev-parse",
                "HEAD",
                f"HEAD:{relative_path}",
            ).splitlines()
            if line.strip()
        ]
        if (
            len(revision_lines) != 2
            or re.fullmatch(r"[0-9a-f]{40}", revision_lines[0]) is None
            or re.fullmatch(
                r"[0-9a-f]{40}|[0-9a-f]{64}", revision_lines[1]
            ) is None
        ):
            raise ValueError("Git HEAD identity is invalid")
        head_revision, head_blob = revision_lines
        index_lines = [
            line
            for line in git_stdout(
                "-c",
                "core.quotePath=false",
                "ls-files",
                "--stage",
                "--",
                relative_path,
            ).splitlines()
            if line
        ]
        if len(index_lines) != 1:
            raise ValueError("scheduler script index identity is ambiguous")
        match = re.fullmatch(
            r"(100644|100755) ([0-9a-f]{40}|[0-9a-f]{64}) 0\t(.+)",
            index_lines[0],
        )
        if match is None or match.group(3) != relative_path:
            raise ValueError("scheduler script is not a regular stage-zero file")
        index_blob = match.group(2).lower()
        content = (root / Path(*PurePosixPath(relative_path).parts)).read_bytes()
        header = f"blob {len(content)}\0".encode("ascii")
        algorithm = hashlib.sha1 if len(head_blob) == 40 else hashlib.sha256
        worktree_blob = algorithm(header + content).hexdigest()
        if head_blob != index_blob or head_blob != worktree_blob:
            raise ValueError("scheduler script Git blob differs")

        expected_revision = str(
            os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
            or os.environ.get("PROBIGA_EXPECTED_GIT_SHA")
            or ""
        ).strip().lower()
        if expected_revision and expected_revision != head_revision:
            raise ValueError("scheduler script HEAD differs from runtime build")

        production = str(
            os.environ.get("PROBIGA_DEPLOYMENT_MODE") or ""
        ).strip().lower() == "production"
        configured_root = str(os.environ.get("PROBIGA_CODE_ROOT") or "").strip()
        if production:
            if not configured_root or Path(configured_root).resolve() != root:
                raise ValueError("scheduler script root differs from release root")
            verification = verify_runtime_release_manifest(root)
            manifest = verification.get("manifest")
            if (
                verification.get("verified") is not True
                or not isinstance(manifest, dict)
                or manifest.get("release_id") != head_revision
            ):
                raise ValueError("release manifest and Git HEAD differ")
        elif (root / MANIFEST_FILE_NAME).is_file():
            verification = verify_runtime_release_manifest(root)
            manifest = verification.get("manifest")
            if (
                verification.get("verified") is not True
                or not isinstance(manifest, dict)
                or manifest.get("release_id") != head_revision
            ):
                raise ValueError("release manifest and Git HEAD differ")
    except SchedulerScriptPolicyError:
        raise
    except (OSError, UnicodeError, ValueError, ReleaseManifestError) as exc:
        try:
            reason = str(exc).strip()
        except Exception:
            reason = "identity validation failed"
        suffix = f": {reason}" if reason else ""
        raise SchedulerScriptPolicyError(
            "scheduler script is not an unchanged file from Git HEAD" + suffix
        ) from exc


def _reject_reparse_points(path: Path, root: Path) -> None:
    current = path
    while True:
        if current.exists() or current.is_symlink():
            try:
                metadata = current.stat(follow_symlinks=False)
            except OSError as exc:
                raise SchedulerScriptPolicyError(
                    f"cannot inspect scheduler path metadata: {current}"
                ) from exc
            if current.is_symlink() or (
                getattr(metadata, "st_file_attributes", 0) & 0x400
            ):
                raise SchedulerScriptPolicyError(
                    "scheduler script path cannot use symlinks/reparse points"
                )
        if current == root:
            return
        if root not in current.parents:
            raise SchedulerScriptPolicyError(
                "scheduler script parent chain escapes the repository"
            )
        current = current.parent


__all__ = [
    "SchedulerScriptPolicyError",
    "resolve_scheduler_script",
]
