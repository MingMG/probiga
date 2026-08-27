# -*- coding: utf-8 -*-
"""Fail-closed resolution for the separately versioned ``adata`` runtime.

The repository historically bundled ``adata`` as an ignored nested Git
checkout and several runtime entry points placed that checkout at the front of
``sys.path``.  A pinned ProBigA commit therefore did not identify all Python
code executed by the service.  Production now consumes an extracted,
read-only source tree whose Git commit and canonical content digest are both
declared by the deployment environment.

Development keeps the existing checkout fallback so local work is not
destroyed or silently rewritten.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
from typing import Iterable


ADATA_SOURCE_ENV = "PROBIGA_ADATA_SOURCE_DIR"
ADATA_GIT_SHA_ENV = "PROBIGA_EXPECTED_ADATA_SHA"
ADATA_TREE_SHA_ENV = "PROBIGA_EXPECTED_ADATA_TREE_SHA256"
ADATA_GIT_MARKER = ".probiga-adata.gitsha"
ADATA_TREE_MARKER = ".probiga-adata.tree.sha256"
_MARKERS = frozenset({ADATA_GIT_MARKER, ADATA_TREE_MARKER})
_GIT_SHA = re.compile(r"[0-9a-f]{40}\Z")
_TREE_SHA = re.compile(r"[0-9a-f]{64}\Z")


class AdataReleaseError(RuntimeError):
    """Raised when the separately versioned runtime cannot be proven."""


def _writable_by_production_service(path: Path) -> bool:
    """Evaluate the non-root runtime boundary even in a root broker process."""

    if hasattr(os, "geteuid") and os.geteuid() == 0:
        try:
            state = path.stat()
        except OSError as exc:
            raise AdataReleaseError(
                "adata release source metadata is unavailable"
            ) from exc
        # The production service is intentionally non-root.  A root-owned
        # path without group/other write bits is therefore read-only to that
        # service even though os.access(..., W_OK) is always true for the
        # privileged schema broker itself.  Any non-root owner is rejected
        # conservatively because it could be the runtime identity.
        return state.st_uid != 0 or bool(stat.S_IMODE(state.st_mode) & 0o022)
    return os.access(path, os.W_OK)


def _exact_sha(value: str, *, tree: bool = False) -> str:
    normalized = str(value or "").strip()
    pattern = _TREE_SHA if tree else _GIT_SHA
    label = "tree SHA256" if tree else "Git SHA"
    if pattern.fullmatch(normalized) is None:
        raise AdataReleaseError(f"adata {label} must be lowercase hexadecimal")
    return normalized


def _iter_source_files(root: Path) -> Iterable[tuple[str, Path]]:
    for candidate in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        relative = candidate.relative_to(root)
        if ".git" in relative.parts:
            continue
        if candidate.is_symlink():
            raise AdataReleaseError(
                f"adata release source must not contain symlinks: {relative.as_posix()}"
            )
        if candidate.is_file() and relative.as_posix() not in _MARKERS:
            yield relative.as_posix(), candidate


def canonical_adata_tree_sha256(source_dir: str | Path) -> str:
    """Hash every regular source file with unambiguous path framing."""
    root = Path(source_dir)
    if not root.is_absolute():
        raise AdataReleaseError("adata release source path must be absolute")
    try:
        root = root.resolve(strict=True)
    except OSError as exc:
        raise AdataReleaseError("adata release source does not exist") from exc
    if not root.is_dir():
        raise AdataReleaseError("adata release source must be a directory")

    digest = hashlib.sha256()
    count = 0
    for relative, candidate in _iter_source_files(root):
        path_bytes = relative.encode("utf-8")
        file_digest = hashlib.sha256()
        try:
            with candidate.open("rb") as handle:
                while chunk := handle.read(1024 * 1024):
                    file_digest.update(chunk)
        except OSError as exc:
            raise AdataReleaseError(
                f"cannot read adata release file: {relative}"
            ) from exc
        digest.update(len(path_bytes).to_bytes(8, "big"))
        digest.update(path_bytes)
        digest.update(file_digest.digest())
        count += 1
    if count == 0:
        raise AdataReleaseError("adata release source contains no files")
    return digest.hexdigest()


def seal_adata_release_source(source_dir: str | Path, git_sha: str) -> dict[str, str]:
    """Write deterministic markers into a newly extracted release source."""
    root = Path(source_dir)
    if not root.is_absolute():
        raise AdataReleaseError("adata release source path must be absolute")
    root = root.resolve(strict=True)
    normalized_git_sha = _exact_sha(git_sha)
    tree_sha = canonical_adata_tree_sha256(root)
    (root / ADATA_GIT_MARKER).write_text(normalized_git_sha + "\n", encoding="ascii")
    (root / ADATA_TREE_MARKER).write_text(tree_sha + "\n", encoding="ascii")
    return {
        "git_sha": normalized_git_sha,
        "tree_sha256": tree_sha,
        "source_dir": str(root),
    }


def validate_adata_release_source(
    source_dir: str | Path,
    *,
    expected_git_sha: str,
    expected_tree_sha256: str,
    repository_root: str | Path | None = None,
    require_read_only: bool = False,
) -> dict[str, str | bool]:
    """Validate markers, source bytes, location, and production mutability."""
    normalized_git_sha = _exact_sha(expected_git_sha)
    normalized_tree_sha = _exact_sha(expected_tree_sha256, tree=True)
    source = Path(source_dir)
    if not source.is_absolute():
        raise AdataReleaseError("adata release source path must be absolute")
    try:
        source = source.resolve(strict=True)
    except OSError as exc:
        raise AdataReleaseError("adata release source does not exist") from exc
    if not source.is_dir():
        raise AdataReleaseError("adata release source must be a directory")

    if repository_root is not None:
        repository_checkout = (Path(repository_root).resolve() / "adata").resolve()
        if source == repository_checkout:
            raise AdataReleaseError(
                "production adata source must not be the mutable nested checkout"
            )

    try:
        recorded_git_sha = (source / ADATA_GIT_MARKER).read_text(
            encoding="ascii"
        ).strip()
        recorded_tree_sha = (source / ADATA_TREE_MARKER).read_text(
            encoding="ascii"
        ).strip()
    except (OSError, UnicodeError) as exc:
        raise AdataReleaseError("adata release markers are missing or unreadable") from exc
    if recorded_git_sha != normalized_git_sha:
        raise AdataReleaseError("adata release Git SHA marker differs")
    if recorded_tree_sha != normalized_tree_sha:
        raise AdataReleaseError("adata release tree SHA marker differs")

    actual_tree_sha = canonical_adata_tree_sha256(source)
    if actual_tree_sha != normalized_tree_sha:
        raise AdataReleaseError("adata release source tree hash differs")

    writable_paths: list[str] = []
    if require_read_only:
        ancestors: list[Path] = []
        current = source.parent
        for _level in range(3):
            ancestors.append(current)
            if current.parent == current:
                break
            current = current.parent
        candidates = [
            *ancestors,
            source,
            source / ADATA_GIT_MARKER,
            source / ADATA_TREE_MARKER,
        ]
        candidates.extend(
            path for path in source.rglob("*")
            if path.is_dir() and not path.is_symlink()
        )
        candidates.extend(path for _relative, path in _iter_source_files(source))
        writable_paths = [
            str(path) for path in candidates
            if _writable_by_production_service(path)
        ]
        if writable_paths:
            raise AdataReleaseError(
                "adata release source is writable by the service account"
            )
    return {
        "git_sha": normalized_git_sha,
        "tree_sha256": actual_tree_sha,
        "source_dir": str(source),
        "read_only": not writable_paths,
    }


def production_mode() -> bool:
    return os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower() == "production"


def resolve_adata_source(repository_root: str | Path) -> Path:
    """Resolve the only adata source that runtime code may prepend."""
    root = Path(repository_root).resolve()
    if not production_mode():
        configured = os.environ.get(ADATA_SOURCE_ENV, "").strip()
        return Path(configured).resolve() if configured else (root / "adata").resolve()

    source_value = os.environ.get(ADATA_SOURCE_ENV, "").strip()
    git_sha = os.environ.get(ADATA_GIT_SHA_ENV, "").strip()
    tree_sha = os.environ.get(ADATA_TREE_SHA_ENV, "").strip()
    if not source_value or not git_sha or not tree_sha:
        raise AdataReleaseError(
            "production adata release source and both expected hashes are required"
        )
    result = validate_adata_release_source(
        source_value,
        expected_git_sha=git_sha,
        expected_tree_sha256=tree_sha,
        repository_root=root,
        require_read_only=True,
    )
    return Path(str(result["source_dir"]))


def ensure_adata_import_path(repository_root: str | Path) -> Path:
    """Prepend the verified source and remove the mutable bundled checkout."""
    root = Path(repository_root).resolve()
    source = resolve_adata_source(root)
    mutable_checkout = str((root / "adata").resolve())
    source_text = str(source)
    retained = []
    for item in sys.path:
        resolved_item = Path(item or os.curdir).resolve()
        if str(resolved_item) != mutable_checkout:
            retained.append(item)
    sys.path[:] = [source_text, *[item for item in retained if item != source_text]]
    return source


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seal or verify an adata release source")
    subparsers = parser.add_subparsers(dest="command", required=True)
    seal = subparsers.add_parser("seal")
    seal.add_argument("--source", required=True)
    seal.add_argument("--git-sha", required=True)
    verify = subparsers.add_parser("verify")
    verify.add_argument("--source", required=True)
    verify.add_argument("--git-sha", required=True)
    verify.add_argument("--tree-sha256", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "seal":
            result = seal_adata_release_source(args.source, args.git_sha)
        else:
            result = validate_adata_release_source(
                args.source,
                expected_git_sha=args.git_sha,
                expected_tree_sha256=args.tree_sha256,
            )
    except AdataReleaseError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
