#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.trading_v3.research_pool import (
    ResearchPoolValidationError,
    load_research_payload_file,
    publish_research_pool,
    research_pool_store_root,
)
from tools.env_config import load_project_env


def _current_git_sha() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )
    return completed.stdout.strip().lower()


def _git_output(*arguments: str) -> str:
    completed = subprocess.run(
        ["git", *arguments],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )
    return completed.stdout.strip()


def _assert_release_checkout(expected_build_sha: str) -> str:
    current = _current_git_sha()
    if current != expected_build_sha:
        raise ResearchPoolValidationError(
            "publisher checkout differs from expected build"
        )
    if _git_output("branch", "--show-current") != "main":
        raise ResearchPoolValidationError("publisher checkout is not main")
    if _git_output("rev-parse", "refs/remotes/origin/main").lower() != current:
        raise ResearchPoolValidationError("publisher checkout differs from origin/main")
    if _git_output("status", "--porcelain", "--untracked-files=no"):
        raise ResearchPoolValidationError("publisher checkout has tracked changes")
    configured_build = str(os.environ.get("PROBIGA_BUILD_COMMIT_SHA") or "").lower()
    if configured_build and configured_build != current:
        raise ResearchPoolValidationError("runtime build differs from publisher checkout")
    configured_root = str(os.environ.get("PROBIGA_CODE_ROOT") or "").strip()
    if configured_root and Path(configured_root).resolve(strict=True) != ROOT.resolve(strict=True):
        raise ResearchPoolValidationError("runtime code root differs from publisher checkout")
    return current


def _workspace_root() -> Path:
    completed = subprocess.run(
        ["git", "rev-parse", "--git-common-dir"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )
    common = Path(completed.stdout.strip())
    if not common.is_absolute():
        common = ROOT / common
    common = common.resolve(strict=True)
    return common.parent if common.name == ".git" else ROOT


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Publish one verified retrospective V3 research pool artifact.",
    )
    parser.add_argument("--input", required=True)
    parser.add_argument("--expected-build-sha", required=True)
    parser.add_argument("--store-root", default="")
    args = parser.parse_args(argv)
    try:
        load_project_env()
        expected_build_sha = str(args.expected_build_sha or "").strip().lower()
        current_build_sha = _assert_release_checkout(expected_build_sha)
        store_root = research_pool_store_root(
            Path(args.store_root) if args.store_root else None
        )
        job_root = store_root
        payload, source_bytes = load_research_payload_file(
            Path(args.input),
            allowed_roots=(ROOT, _workspace_root(), job_root),
        )
        receipt = publish_research_pool(
            payload,
            publisher_build_sha=current_build_sha,
            store_root=store_root,
            source_bytes=source_bytes,
        )
    except Exception as exc:
        print(json.dumps({
            "schema": "probiga.trading-v3-research-pool-publication.v1",
            "status": "failed",
            "publication_status": "FAILED",
            "error": str(exc),
            "database_writes": False,
            "notifications_sent": False,
        }, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
