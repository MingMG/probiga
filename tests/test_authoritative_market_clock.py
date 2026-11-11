from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, text

from server.common.authoritative_market_clock import (
    PRODUCTION_TIMEZONE,
    resolve_strategy_pool_decision_slot,
)


@pytest.fixture
def market_calendar():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE si_trade_calendar (trade_date DATE, trade_status INT)"))
        connection.execute(text(
            "INSERT INTO si_trade_calendar VALUES "
            "('2026-11-06',1),('2026-11-07',0),('2026-11-08',0),('2026-11-09',1),"
            "('2026-11-10',1),('2026-11-11',1)"
        ))
    yield engine
    engine.dispose()


def test_initial_preserves_late_actual_time_and_fixed_shanghai_slot(market_calendar):
    actual = datetime(2026, 11, 11, 0, 47, 12, 123456)
    slot = resolve_strategy_pool_decision_slot(
        market_calendar, data_trade_date="2026-11-10", phase="INITIAL", evaluated_at=actual
    )

    assert slot.phase == "INITIAL"
    assert slot.data_trade_date == "2026-11-10"
    assert slot.execution_session_date == "2026-11-11"
    assert slot.scheduled_at == datetime(2026, 11, 10, 22, 20, tzinfo=PRODUCTION_TIMEZONE)
    assert slot.scheduled_at.tzinfo is PRODUCTION_TIMEZONE
    assert slot.evaluated_at == actual.replace(tzinfo=PRODUCTION_TIMEZONE)


def test_revision_uses_next_open_session_after_weekend_and_converts_utc(market_calendar):
    with market_calendar.connect() as connection:
        slot = resolve_strategy_pool_decision_slot(
            connection,
            data_trade_date="2026-11-06",
            phase="PREMARKET_REVISION",
            evaluated_at=datetime(2026, 11, 9, 0, 30, 2, tzinfo=timezone.utc),
        )

    assert slot.phase == "PREMARKET_REVISION"
    assert slot.data_trade_date == "2026-11-06"
    assert slot.execution_session_date == "2026-11-09"
    assert slot.scheduled_at == datetime(2026, 11, 9, 8, 30, tzinfo=PRODUCTION_TIMEZONE)
    assert slot.evaluated_at == datetime(2026, 11, 9, 8, 30, 2, tzinfo=PRODUCTION_TIMEZONE)
    assert slot.evaluated_at.tzinfo is PRODUCTION_TIMEZONE


@pytest.mark.parametrize(("phase", "early", "due"), [
    ("INITIAL", datetime(2026, 11, 10, 22, 19, 59), datetime(2026, 11, 10, 22, 20)),
    ("PREMARKET_REVISION", datetime(2026, 11, 11, 8, 29, 59), datetime(2026, 11, 11, 8, 30)),
])
def test_decision_evaluation_must_reach_exact_slot(market_calendar, phase, early, due):
    with pytest.raises(ValueError, match="evaluation precedes"):
        resolve_strategy_pool_decision_slot(
            market_calendar, data_trade_date="2026-11-10", phase=phase, evaluated_at=early
        )
    slot = resolve_strategy_pool_decision_slot(
        market_calendar, data_trade_date="2026-11-10", phase=phase, evaluated_at=due
    )
    assert slot.evaluated_at == slot.scheduled_at


@pytest.mark.parametrize(("data_trade_date", "phase", "error"), [
    ("2026-11-07", "INITIAL", "not an open trading session"),
    ("2026-11-11", "PREMARKET_REVISION", "next open trading session is unavailable"),
    ("20261110", "INITIAL", "must use YYYY-MM-DD"),
    ("2026-11-10", "EVENING", "unknown strategy pool decision phase"),
])
def test_decision_slot_rejects_unknown_session_or_phase(market_calendar, data_trade_date, phase, error):
    with pytest.raises(ValueError, match=error):
        resolve_strategy_pool_decision_slot(
            market_calendar, data_trade_date=data_trade_date, phase=phase,
            evaluated_at=datetime(2026, 11, 12, 8, 30),
        )
