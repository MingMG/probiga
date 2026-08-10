from __future__ import annotations

import hashlib
import os
from pathlib import Path

from .config import PROJECT_ROOT


SOURCE_ARTIFACT_PATHS: tuple[str, ...] = (
    "server/trading_v3",
    "server/api/routers/trading_v3.py",
    "server/db/migrations_v3.py",
    "strategies/trading_v3.json",
    "tools/run_trading_v3_decision.py",
    "tools/run_trading_v3_counterfactual.py",
)


def source_artifact_sha256() -> str:
    digest = hashlib.sha256()
    files: list[Path] = []
    for relative in SOURCE_ARTIFACT_PATHS:
        path = PROJECT_ROOT / relative
        if path.is_dir():
            files.extend(
                child
                for child in path.rglob("*.py")
                if "__pycache__" not in child.parts
            )
        elif path.is_file():
            files.append(path)
    for path in sorted(
        set(files),
        key=lambda item: item.relative_to(PROJECT_ROOT).as_posix(),
    ):
        relative = path.relative_to(PROJECT_ROOT).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def code_version() -> tuple[str, str]:
    configured = str(
        os.getenv("PROBIGA_BUILD_COMMIT_SHA") or ""
    ).strip()
    if configured:
        return configured, "git_commit"
    return source_artifact_sha256(), "source_artifact_sha256"
