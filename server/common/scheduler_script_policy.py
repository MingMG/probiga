"""Fail-closed scheduler script path policy.

The scheduler may receive task rows from storage, so a stored ``script_path``
is data rather than authority.  Only repository-owned Python files may be
resolved, and research-only Trading V4/V5/V6 code is never schedulable.
"""

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath
import subprocess


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
    git_environment = os.environ.copy()
    git_environment["GIT_OPTIONAL_LOCKS"] = "0"
    commands = (
        ("ls-files", "--error-unmatch", "--", relative_path),
        ("diff", "--quiet", "HEAD", "--", relative_path),
        ("diff", "--cached", "--quiet", "HEAD", "--", relative_path),
    )
    for command in commands:
        try:
            subprocess.run(
                ["git", "-c", f"safe.directory={root}", *command],
                cwd=root,
                check=True,
                capture_output=True,
                env=git_environment,
                timeout=10,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise SchedulerScriptPolicyError(
                "scheduler script is not an unchanged file from Git HEAD"
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
