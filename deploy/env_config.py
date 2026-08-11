# -*- coding: utf-8 -*-
"""Shared runtime configuration helpers for scripts under deploy/."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.common.config import get_mysql_url


def require_mysql_url() -> str:
    return get_mysql_url(required=True)


def resolve_tool_mysql_url() -> str:
    return require_mysql_url()


def create_tool_engine(url: str | None = None, **kwargs):
    """Create a batch-style engine for deploy scripts."""
    return create_batch_engine(url, **kwargs)
