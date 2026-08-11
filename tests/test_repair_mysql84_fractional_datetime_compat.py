from __future__ import annotations

from datetime import datetime

import pytest

from tools.repair_mysql84_fractional_datetime_compat import (
    CompatibilityRepairError,
    TABLE_PRIMARY_KEYS,
    _table_sql,
    digest_rows,
)


def test_reviewed_table_manifest_is_fixed() -> None:
    assert TABLE_PRIMARY_KEYS == {
        "st_etf_forward_observation": ("id",),
        "st_strategy_health_daily_v2": (
            "strategy_version",
            "trade_date",
            "window_days",
        ),
        "st_worker_heartbeat_v2": ("worker_name",),
    }


def test_row_digest_detects_one_second_datetime_drift() -> None:
    source = [(1, datetime(2026, 8, 8, 21, 9, 44))]
    rounded = [(1, datetime(2026, 8, 8, 21, 9, 45))]

    assert digest_rows(source) != digest_rows(rounded)
    assert digest_rows(source) == digest_rows(list(source))


def test_table_sql_rejects_unreviewed_tables() -> None:
    with pytest.raises(CompatibilityRepairError, match="outside"):
        _table_sql("some_other_table")
