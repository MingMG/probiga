# -*- coding: utf-8 -*-
"""Shared runtime configuration helpers for scripts under tools/."""
from __future__ import annotations

import os
import sys
from collections.abc import MutableMapping
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.common.config import get_mysql_url


def load_project_env(
    env_path: str | Path | None = None,
    *,
    environ: MutableMapping[str, str] | None = None,
) -> None:
    """Load simple KEY=VALUE entries from the project .env without overwriting."""
    path = Path(env_path) if env_path is not None else ROOT / ".env"
    if not path.exists():
        return
    target = os.environ if environ is None else environ
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        target.setdefault(key, value)


def require_mysql_url() -> str:
    return get_mysql_url(required=True)


def create_tool_engine(url: str | None = None, **kwargs):
    """Create a batch-style engine for standalone tools."""
    return create_batch_engine(url, **kwargs)


def resolve_tool_mysql_url() -> str:
    """Resolve MYSQL_URL lazily for standalone tools."""
    return os.environ.get("MYSQL_URL") or require_mysql_url()
