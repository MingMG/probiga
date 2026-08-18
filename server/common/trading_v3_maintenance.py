"""Cross-process maintenance exclusion for every Trading V3 writer.

MySQL named locks are connection scoped.  A maintenance process holds the
same lock for the complete DDL window, while each V3 writer holds it for its
complete write cycle.  This intentionally serializes V3 writers: production's
standalone scheduler is already single-concurrency, and fail-closed exclusion
is more important than allowing an untracked manual writer to race DDL.
"""
from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from functools import wraps
from typing import Any, ParamSpec, TypeVar

from sqlalchemy.engine import Engine

from server.common.mysql_lock import mysql_named_lock


TRADING_V3_MAINTENANCE_LOCK_NAME = "probiga:trading_v3:maintenance"
TRADING_V3_WRITER_LOCK_TIMEOUT_SECONDS = 0

P = ParamSpec("P")
R = TypeVar("R")


class TradingV3WriterLeaseUnavailable(RuntimeError):
    """A V3 writer could not prove that no maintenance window is active."""


def _mysql_engine(engine: Engine) -> bool:
    return str(getattr(getattr(engine, "dialect", None), "name", "")).lower() in {
        "mysql",
        "mariadb",
    }


@contextmanager
def trading_v3_writer_lease(
    engine: Engine,
    *,
    timeout_seconds: int = TRADING_V3_WRITER_LOCK_TIMEOUT_SECONDS,
) -> Iterator[None]:
    """Hold the production V3 maintenance exclusion for one writer cycle.

    Non-MySQL engines are used by isolated unit tests and do not share a
    production server, so the cross-process lock is intentionally a no-op
    there.  MySQL/MariaDB acquisition errors always fail closed.
    """

    if not _mysql_engine(engine):
        yield
        return
    try:
        with mysql_named_lock(
            engine,
            TRADING_V3_MAINTENANCE_LOCK_NAME,
            timeout_seconds=max(0, int(timeout_seconds)),
        ):
            yield
    except TimeoutError as exc:
        raise TradingV3WriterLeaseUnavailable(
            "TRADING_V3_MAINTENANCE_WINDOW_ACTIVE_OR_WRITER_BUSY"
        ) from exc


def trading_v3_writer(
    function: Callable[P, R],
) -> Callable[P, R]:
    """Decorate a writer whose first positional argument is its primary DB."""

    @wraps(function)
    def guarded(primary_engine: Engine, *args: Any, **kwargs: Any) -> R:
        with trading_v3_writer_lease(primary_engine):
            return function(primary_engine, *args, **kwargs)

    return guarded  # type: ignore[return-value]


__all__ = [
    "TRADING_V3_MAINTENANCE_LOCK_NAME",
    "TRADING_V3_WRITER_LOCK_TIMEOUT_SECONDS",
    "TradingV3WriterLeaseUnavailable",
    "trading_v3_writer",
    "trading_v3_writer_lease",
]
