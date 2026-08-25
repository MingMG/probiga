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
from datetime import datetime
from typing import Iterator

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

_LOCK_NAME_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
LOGGER = logging.getLogger(__name__)

# Every production writer of ``sm_stock_minute`` must use this one freeze
# domain.  A table-name-derived lock is unsafe because the QMT receipt barrier,
# exact-key registry publisher and public minute crawler would otherwise be
# able to interleave commits while each believed it had exclusive ownership.
STOCK_MINUTE_FREEZE_LOCK_NAME = "probiga:stock_minute"
STOCK_KLINE_FREEZE_LOCK_NAME = "probiga:stock_kline"
CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME = "probiga:capital_flow_daily"


def supersede_overlapping_qmt_minute_forward_receipts(
    engine: Engine,
    *,
    first_trade_time: datetime,
    last_trade_time: datetime,
    reason: str,
) -> int:
    """Revoke authority before a non-QMT writer changes the minute window.

    The authority registry may live on a different MySQL server from the
    physical minute table, so this transaction deliberately commits before
    target DML.  A later target failure therefore leaves authority
    conservatively disabled rather than endorsing stale or mixed rows.
    Callers must hold ``STOCK_MINUTE_FREEZE_LOCK_NAME`` on the target server
    for the whole revoke-then-write sequence.
    """

    if not isinstance(first_trade_time, datetime) or not isinstance(
        last_trade_time, datetime
    ):
        raise TypeError("QMT minute receipt revocation requires datetime bounds")
    if first_trade_time > last_trade_time:
        raise ValueError("QMT minute receipt revocation window is invalid")
    with engine.begin() as connection:
        result = connection.execute(
            text(
                """
                UPDATE st_qmt_minute_sync_receipt_v2
                SET forward_eligible=0,
                    quality_status='SUPERSEDED'
                WHERE forward_eligible=1
                  AND quality_status='PASS'
                  AND first_trade_time<=:last_trade_time
                  AND last_trade_time>=:first_trade_time
                """
            ),
            {
                "first_trade_time": first_trade_time,
                "last_trade_time": last_trade_time,
            },
        )
    revoked = max(0, int(getattr(result, "rowcount", 0) or 0))
    if revoked:
        LOGGER.info(
            "Superseded %s overlapping QMT minute forward receipts before %s",
            revoked,
            str(reason or "non-QMT minute publish"),
        )
    return revoked


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
