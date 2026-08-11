from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pytest

from server.api import scheduler_runtime
from tools.run_etf_forward_simulation import (
    is_month_end_close,
    load_config,
    stable_hash,
    validate_observation_date,
)


def test_frozen_config_hash_is_stable() -> None:
    path = (
        Path(__file__).resolve().parents[1]
        / "strategies"
        / "etf_trend_risk_v2.json"
    )
    config, first_hash = load_config(path)
    assert first_hash == stable_hash(config)
    assert config["forward_protocol"]["backfill"] == "prohibited"
    assert config["forward_protocol"]["automatic_order_submission"] is False


def test_backfill_and_future_observations_are_rejected() -> None:
    registered_at = datetime(2026, 7, 25, 12, 0, 0)
    with pytest.raises(ValueError, match="backfill prohibited"):
        validate_observation_date(
            data_date=date(2026, 7, 24),
            forward_start_date=date(2026, 7, 27),
            registered_at=registered_at,
            local_today=date(2026, 7, 27),
        )
    with pytest.raises(ValueError, match="future observation prohibited"):
        validate_observation_date(
            data_date=date(2026, 7, 28),
            forward_start_date=date(2026, 7, 27),
            registered_at=registered_at,
            local_today=date(2026, 7, 27),
        )


def test_month_end_uses_real_next_trading_date() -> None:
    assert is_month_end_close(
        date(2026, 7, 31),
        date(2026, 8, 3),
    )
    assert not is_month_end_close(
        date(2026, 7, 30),
        date(2026, 7, 31),
    )


def test_forward_daily_scheduler_skips_non_trading_days() -> None:
    assert "etf_forward_daily" in (
        scheduler_runtime.NON_TRADING_DAY_SKIP_TYPES
    )
