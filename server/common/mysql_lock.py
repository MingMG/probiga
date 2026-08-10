# -*- coding: utf-8 -*-
"""Small MySQL advisory-lock helpers for cross-process data jobs.

The scheduler can be restarted or run by more than one supervisor.  An
in-process ``threading.Lock`` therefore cannot protect a replace-style ETL
write.  MySQL named locks live in the server and are released automatically
when the owning connection disappears.
"""
from __future__ import annotations

import re
import logging
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

_LOCK_NAME_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
LOGGER = logging.getLogger(__name__)


@contextmanager
def mysql_named_lock(
    engine: Engine,
    name: str,
    *,
    timeout_seconds: int = 0,
    connection: Connection | None = None,
) -> Iterator[Connection]:
    """Hold a MySQL advisory lock for the lifetime of the context.

    The connection must remain checked out while the protected work runs;
    releasing it would release the lock and reintroduce the race this helper
    is intended to prevent.  When ``connection`` is supplied, ownership stays
    with the caller so identity checks and protected work can share one exact
    physical checkout.
    """
    lock_name = str(name or "").strip()
    if not _LOCK_NAME_RE.fullmatch(lock_name):
        raise ValueError(f"invalid MySQL lock name: {name!r}")
    timeout = max(0, int(timeout_seconds))
    conn = connection if connection is not None else engine.connect()
    owns_connection = connection is None
    acquired = False
    try:
        result = conn.execute(
            text("SELECT GET_LOCK(:lock_name, :timeout_seconds)"),
            {"lock_name": lock_name, "timeout_seconds": timeout},
        ).scalar()
        acquired = int(result or 0) == 1
        if not acquired:
            raise TimeoutError(
                f"timed out waiting for MySQL advisory lock {lock_name!r} "
                f"after {timeout}s"
            )
        yield conn
    finally:
        if acquired:
            try:
                conn.execute(text("SELECT RELEASE_LOCK(:lock_name)"), {"lock_name": lock_name})
            except Exception as cleanup_error:
                # MySQL releases the lock when the connection closes.  Do not
                # hide the original ETL exception with a cleanup failure.
                LOGGER.debug("MySQL advisory-lock cleanup failed: %s", cleanup_error)
        if owns_connection:
            conn.close()
