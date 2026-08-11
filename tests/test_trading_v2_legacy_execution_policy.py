from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from server.trading_v2.domain import OrderSide, WaitingReason
from server.trading_v2.legacy_execution_policy import (
    LegacySectorPreheatExecutionPolicy,
)


class _Result:
    def __init__(self, row):
        self._row = row

    def mappings(self):
        return self

    def first(self):
        return self._row


class _Connection:
    def __init__(self, row=None, *, error: Exception | None = None):
        self.row = row
        self.error = error
        self.calls = []

    def execute(self, statement, params=None):
        self.calls.append((str(statement), params))
        if self.error is not None:
            raise self.error
        return _Result(self.row)


def _passing_row(now: datetime) -> dict[str, object]:
    return {
        "snapshot_at": now - timedelta(seconds=30),
        "direction": "UP",
        "score": Decimal("20"),
        "breadth_pct": Decimal("10"),
    }


def test_policy_is_legacy_buy_only_and_skips_database_for_other_orders():
    policy = LegacySectorPreheatExecutionPolicy()
    connection = _Connection(error=AssertionError("must not query"))
    now = datetime(2026, 8, 3, 10, 0)

    assert policy.sector_entry_wait_reason(
        connection,
        strategy_version="another-strategy",
        theme_code="AI",
        side=OrderSide.BUY,
        now=now,
    ) == ""
    assert policy.sector_entry_wait_reason(
        connection,
        strategy_version="sector_preheat:legacy",
        theme_code="AI",
        side=OrderSide.SELL,
        now=now,
    ) == ""
    assert connection.calls == []


def test_sector_confirmation_passes_only_complete_fresh_up_evidence():
    policy = LegacySectorPreheatExecutionPolicy(
        confirmation_max_age_seconds=180
    )
    now = datetime(2026, 8, 3, 10, 0)
    kwargs = {
        "strategy_version": "sector_preheat:legacy",
        "theme_code": "AI",
        "side": OrderSide.BUY,
        "now": now,
    }

    connection = _Connection(_passing_row(now))
    assert policy.sector_entry_wait_reason(connection, **kwargs) == ""
    assert connection.calls[0][1] == {"theme_code": "AI"}

    for row in (
        None,
        {**_passing_row(now), "snapshot_at": now - timedelta(seconds=181)},
        {**_passing_row(now), "snapshot_at": now + timedelta(seconds=1)},
        {**_passing_row(now), "direction": "DOWN"},
        {**_passing_row(now), "score": Decimal("19.999")},
        {**_passing_row(now), "breadth_pct": Decimal("9.999")},
    ):
        assert policy.sector_entry_wait_reason(_Connection(row), **kwargs) == (
            WaitingReason.WAIT_SECTOR_CONFIRMATION.value
        )


def test_sector_confirmation_fails_closed_on_missing_theme_or_read_error():
    policy = LegacySectorPreheatExecutionPolicy()
    now = datetime(2026, 8, 3, 10, 0)
    kwargs = {
        "strategy_version": "sector_preheat:legacy",
        "side": OrderSide.BUY,
        "now": now,
    }

    assert policy.sector_entry_wait_reason(
        _Connection(error=AssertionError("must not query")),
        theme_code=" ",
        **kwargs,
    ) == WaitingReason.WAIT_SECTOR_CONFIRMATION.value
    assert policy.sector_entry_wait_reason(
        _Connection(error=RuntimeError("database unavailable")),
        theme_code="AI",
        **kwargs,
    ) == WaitingReason.WAIT_SECTOR_CONFIRMATION.value


def test_entry_trend_gate_is_confined_to_legacy_sector_buys():
    policy = LegacySectorPreheatExecutionPolicy()

    assert policy.entry_trend_wait_reason(
        strategy_version="sector_preheat:legacy",
        side=OrderSide.BUY,
        fill_price=Decimal("9.99"),
        initial_stop=Decimal("10"),
    ) == WaitingReason.WAIT_ENTRY_TREND_INVALID.value
    assert policy.entry_trend_wait_reason(
        strategy_version="sector_preheat:legacy",
        side=OrderSide.SELL,
        fill_price=Decimal("9.99"),
        initial_stop=Decimal("10"),
    ) == ""
    assert policy.entry_trend_wait_reason(
        strategy_version="another-strategy",
        side=OrderSide.BUY,
        fill_price=Decimal("9.99"),
        initial_stop=Decimal("10"),
    ) == ""


@pytest.mark.parametrize("value", [-1, True, 1.5])
def test_policy_rejects_invalid_confirmation_age(value):
    with pytest.raises(ValueError):
        LegacySectorPreheatExecutionPolicy(
            confirmation_max_age_seconds=value
        )


def test_strategy_evidence_sql_is_outside_execution_and_neutral_core():
    repository_root = Path(__file__).resolve().parents[1]
    execution_source = (
        repository_root / "server" / "trading_v2" / "execution.py"
    ).read_text(encoding="utf-8")
    assert "sm_market_radar_sector" not in execution_source

    neutral_root = repository_root / "server" / "trading_core"
    neutral_source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in neutral_root.rglob("*.py")
    )
    assert "legacy_execution_policy" not in neutral_source
    assert "LegacySectorPreheatExecutionPolicy" not in neutral_source
