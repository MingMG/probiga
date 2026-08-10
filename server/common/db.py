# -*- coding: utf-8 -*-
"""
可选数据库连接。未配置 ``DATABASE_URL`` 时 ``get_engine()`` 返回 ``None``。

首次使用 SQLite 时请先创建目录：``mkdir data``（仓库已含 ``data/.gitkeep``）。
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from server.common.config import get_settings
from server.common.engine_factory import create_pooled_engine

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

_engine: Engine | None = None


def get_engine():
    """返回全局 Engine；未配置 ``DATABASE_URL`` 时返回 ``None``。"""
    global _engine
    url = get_settings().database_url
    if not url:
        return None
    if _engine is None:
        _engine = create_pooled_engine(url, echo=False, future=True)
    return _engine
