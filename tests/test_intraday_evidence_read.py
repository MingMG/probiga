from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import StaticPool

from biz.analysis.intraday_reversal import load_intraday_evidence


DAY = date(2026, 11, 10)
CODES = ("000001", "600000")


def _minute_rows(day: date, code: str) -> list[dict]:
    clocks = [
        datetime.combine(day, time(9, 30)) + timedelta(minutes=offset)
        for offset in range(121)
    ] + [
        datetime.combine(day, time(13, 1)) + timedelta(minutes=offset)
        for offset in range(120)
    ]
    return [
        {
            "stock_code": code,
            "trade_date": day.isoformat(),
            "trade_time": stamp.isoformat(sep=" "),
            "price": 10.0 + offset / 1000,
            "change": offset / 1000,
            "change_pct": offset / 100,
            "volume": 100.0,
            "amount": (10.0 + offset / 1000) * 100.0,
            "data_source": "qmt_datadir",
        }
        for offset, stamp in enumerate(clocks)
    ]


@pytest.fixture
def minute_engine():
    engine = create_engine("sqlite://", poolclass=StaticPool)
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE sm_stock_minute (stock_code TEXT, trade_date TEXT, "
            "trade_time TEXT, price REAL, `change` REAL, change_pct REAL, "
            "volume REAL, amount REAL, data_source TEXT)"
        ))
        # The production incident has this index, without a leading date key.
        connection.execute(text(
            "CREATE INDEX idx_sm_code_time ON sm_stock_minute(stock_code, trade_time)"
        ))
        connection.execute(text(
            "CREATE TABLE sm_stock_kline (stock_code TEXT, trade_date TEXT, "
            "k_type INTEGER, adjust_type INTEGER, pre_close REAL)"
        ))
        rows = [
            row
            for offset in range(61)
            for code in CODES
            for row in _minute_rows(DAY - timedelta(days=offset), code)
        ]
        connection.execute(text(
            "INSERT INTO sm_stock_minute VALUES (:stock_code, :trade_date, "
            ":trade_time, :price, :change, :change_pct, :volume, :amount, :data_source)"
        ), rows)
        connection.execute(text(
            "INSERT INTO sm_stock_kline VALUES (:code, :day, 1, 0, 10.0)"
        ), [{"code": code, "day": DAY.isoformat()} for code in CODES])
    yield engine
    engine.dispose()


@contextmanager
def _vm_budget(engine, *, steps: int):
    """Bound actual SQLite execution work without a wall-clock race."""
    raw = engine.raw_connection()
    connection = raw.driver_connection
    raw.close()
    work = {"steps": 0}

    def progress():
        work["steps"] += 100
        return int(work["steps"] > steps)

    connection.set_progress_handler(progress, 100)
    try:
        yield work
    finally:
        connection.set_progress_handler(None, 0)


def test_intraday_read_uses_day_range_instead_of_scanning_stock_history(minute_engine):
    # Reproduce the observed query on the same index and historical layout.
    # Its missing time range exceeds the budget even for just two securities.
    with _vm_budget(minute_engine, steps=40_000):
        with pytest.raises(OperationalError, match="interrupted"):
            with minute_engine.connect() as connection:
                connection.execute(text(
                    "SELECT stock_code, trade_time, price, `change`, change_pct, "
                    "volume, amount, data_source FROM sm_stock_minute "
                    "WHERE trade_date=:day AND stock_code IN ('000001','600000') "
                    "AND price IS NOT NULL AND price>0 ORDER BY stock_code, trade_time"
                ), {"day": DAY.isoformat()}).all()

    with _vm_budget(minute_engine, steps=40_000):
        actual = load_intraday_evidence(
            minute_engine, trade_date=DAY.isoformat(), stock_codes=CODES
        )

    assert list(actual["stock_code"]) == list(CODES)
    assert actual["minute_available"].all()
    assert list(actual["minute_rows"]) == [241, 241]
    for row in actual.to_dict("records"):
        event = json.loads(row["intraday_event_json"])
        assert event["first_trade_time"] == "2026-11-10 09:30:00"
        assert event["last_trade_time"] == "2026-11-10 15:00:00"
        assert event["amount_cny"] == pytest.approx(243892.0)


def test_intraday_read_keeps_date_and_price_checks_and_blocks_mixed_sources(minute_engine):
    with minute_engine.begin() as connection:
        connection.execute(text(
            "UPDATE sm_stock_minute SET data_source='another_source' "
            "WHERE stock_code='600000' AND trade_time='2026-11-10 09:30:00'"
        ))
        # A bad date label must not pull next-day data into today's summary.
        connection.execute(text(
            "INSERT INTO sm_stock_minute VALUES "
            "('000001','2026-11-10','2026-11-11 00:00:00',99,89,890,100,9900,'qmt_datadir'),"
            "('000001','2026-11-09','2026-11-10 09:29:00',1,-9,-90,100,100,'qmt_datadir'),"
            "('000001','2026-11-10','2026-11-10 09:29:00',0,-10,-100,100,0,'qmt_datadir')"
        ))

    actual = load_intraday_evidence(
        minute_engine, trade_date=DAY.isoformat(), stock_codes=["600000", "1", "000001"]
    ).set_index("stock_code")

    assert list(actual.index) == list(CODES)
    assert actual.loc["000001", "minute_available"]
    assert actual.loc["000001", "minute_rows"] == 241
    assert not actual.loc["600000", "minute_available"]
    event = json.loads(actual.loc["600000", "intraday_event_json"])
    assert event == {
        "status": "DATA_BLOCKED",
        "reason": "mixed minute sources",
        "sources": ["another_source", "qmt_datadir"],
    }


def test_intraday_read_bounds_executed_batches_without_omitting_securities(minute_engine):
    additional_codes = [str(500000 + offset) for offset in range(81)]
    expected_codes = set(CODES) | set(additional_codes)
    with minute_engine.begin() as connection:
        connection.execute(text(
            "INSERT INTO sm_stock_minute VALUES (:stock_code, :trade_date, "
            ":trade_time, :price, :change, :change_pct, :volume, :amount, :data_source)"
        ), [row for code in additional_codes for row in _minute_rows(DAY, code)])
        connection.execute(text(
            "INSERT INTO sm_stock_kline VALUES (:code, :day, 1, 0, 10.0)"
        ), [{"code": code, "day": DAY.isoformat()} for code in additional_codes])

    executed_batches = []

    def record_batch(connection, cursor, statement, parameters, context, executemany):
        if "FROM sm_stock_minute" in statement:
            bound = context.compiled_parameters[0]
            executed_batches.append([
                value for key, value in bound.items() if key.startswith("stock_codes_")
            ])

    event.listen(minute_engine, "before_cursor_execute", record_batch)
    try:
        actual = load_intraday_evidence(
            minute_engine,
            trade_date=DAY.isoformat(),
            stock_codes=additional_codes[::-1] + list(CODES) + additional_codes[:2],
        )
    finally:
        event.remove(minute_engine, "before_cursor_execute", record_batch)

    assert [len(batch) for batch in executed_batches] == [40, 40, 3]
    executed_codes = [code for batch in executed_batches for code in batch]
    assert len(executed_codes) == len(expected_codes)
    assert set(executed_codes) == expected_codes
    assert set(actual["stock_code"]) == expected_codes
    assert len(actual) == len(expected_codes)
    assert actual["minute_available"].all()
    assert actual["minute_rows"].eq(241).all()
    assert actual["minute_source"].eq("qmt_datadir").all()


@pytest.mark.parametrize("codes", [[], ["999999"]])
def test_intraday_read_does_not_fill_missing_securities(minute_engine, codes):
    actual = load_intraday_evidence(
        minute_engine, trade_date=DAY.isoformat(), stock_codes=codes
    )
    assert actual.empty
