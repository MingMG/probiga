# -*- coding: utf-8 -*-
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from server.common.config import get_api_mysql_pool_config, get_mysql_url

_ENGINE: Engine | None = None


def get_engine() -> Engine:
    global _ENGINE
    if _ENGINE is None:
        url = get_mysql_url(required=True)
        pool = get_api_mysql_pool_config()
        _ENGINE = create_engine(
            url,
            pool_pre_ping=True,
            pool_size=pool["pool_size"],
            max_overflow=pool["max_overflow"],
            pool_recycle=pool["pool_recycle"],
        )
    return _ENGINE
