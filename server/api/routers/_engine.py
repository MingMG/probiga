# -*- coding: utf-8 -*-
import threading

from sqlalchemy.engine import Engine

from server.common.config import get_api_mysql_pool_config, get_mysql_url
from server.common.engine_factory import create_pooled_engine

_ENGINE: Engine | None = None
_ENGINE_LOCK = threading.Lock()


def get_engine() -> Engine:
    global _ENGINE
    if _ENGINE is None:
        with _ENGINE_LOCK:
            if _ENGINE is None:
                url = get_mysql_url(required=True)
                pool = get_api_mysql_pool_config()
                _ENGINE = create_pooled_engine(
                    url,
                    pool_config=pool,
                )
    return _ENGINE


def dispose_engine() -> None:
    """Dispose the shared API database engine if it has been initialized."""
    global _ENGINE
    with _ENGINE_LOCK:
        engine = _ENGINE
        _ENGINE = None
        if engine is not None:
            engine.dispose()
