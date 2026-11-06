import json
from contextlib import contextmanager
from datetime import date, datetime

import pytest
from sqlalchemy import create_engine, text

from server.common import market_close_ladder as ladder


DAY = date(2026, 11, 6)
OBSERVED = datetime(2026, 11, 7, 2, 0, 0, 123456)


def _raw(*, qdate=20261106, count=2, duplicate=False):
    return json.dumps({"rc": 0, "data": {"qdate": qdate, "tc": count, "pool": [
        {"c": "000001", "n": "样本甲", "p": 10100, "lbc": 2},
        {"c": "000001" if duplicate else "000002", "n": "样本乙", "p": 12300, "lbc": 1},
    ]}})


@pytest.fixture
def source(monkeypatch, tmp_path):
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    monkeypatch.setattr(ladder, "_now", lambda: OBSERVED)
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)"))
        conn.execute(text("INSERT INTO si_trade_calendar VALUES ('2026-11-06',1)"))
        conn.execute(text("""CREATE TABLE sm_stock_kline (
            stock_code TEXT, trade_date TEXT, close REAL, data_source TEXT, quality_status TEXT,
            permission_status TEXT, k_type INTEGER, adjust_type INTEGER, etl_sync_at TEXT)"""))
        conn.execute(text("""INSERT INTO sm_stock_kline VALUES
            ('000001','2026-11-06',10.10,'qmt_datadir','VERIFIED','SUPPORTED',1,0,'2026-11-06 17:00:00'),
            ('000002','2026-11-06',12.30,'qmt_datadir','VERIFIED','SUPPORTED',1,0,'2026-11-06 17:00:00')"""))

    @contextmanager
    def lease(selected, name):
        assert selected is engine and name.endswith(DAY.isoformat())
        with engine.connect() as conn:
            yield conn

    monkeypatch.setattr(ladder, "mysql_named_lock", lease)
    yield engine
    engine.dispose()


@pytest.mark.parametrize("raw", [_raw(qdate=20261105), _raw(count=3), _raw(duplicate=True)])
def test_rejects_wrong_date_incomplete_pool_and_duplicates(raw):
    with pytest.raises(ladder.MarketCloseLadderBlocked):
        ladder._pool(raw, DAY)


def test_capture_is_exact_immutable_and_uses_actual_availability(source, monkeypatch):
    calls = []

    def fetch(day, **kwargs):
        assert day == DAY and kwargs["timeout_seconds"] == 8 and kwargs["max_payload_bytes"] > 0
        calls.append(day)
        return [], _raw()

    monkeypatch.setattr(ladder, "_fetch_pool", fetch)
    first = ladder.capture_market_close_ladder(source, source, DAY)
    second = ladder.capture_market_close_ladder(source, source, DAY)
    assert first["status"] == second["status"] == "COMPLETE"
    assert first["receipt_sha256"] == second["receipt_sha256"]
    assert not first["reused"] and second["reused"] and calls == [DAY]
    quotes = ladder._load_qmt_closes(source, DAY, ladder._pool(_raw(), DAY), OBSERVED)
    result = ladder.load_market_close_ladder(DAY, quotes=quotes, now=OBSERVED)
    assert result["available_at"] == "2026-11-07T02:00:00.123456+08:00"
    assert result["limit_up_count"] == 2 and result["max_streak"] == 2
    assert result["rows"] == [{"streak": 2, "count": 1, "stocks": ["样本甲"]},
                              {"streak": 1, "count": 1, "stocks": ["样本乙"]}]
    with pytest.raises(ladder.MarketCloseLadderBlocked, match="availability"):
        ladder.load_market_close_ladder(DAY, quotes=quotes, now=datetime(2026, 11, 6, 21))
    quotes[0]["price"] += 0.01
    with pytest.raises(ladder.MarketCloseLadderBlocked, match="differs from QMT"):
        ladder.load_market_close_ladder(DAY, quotes=quotes, now=OBSERVED)


def test_mismatched_close_cannot_be_published(source, monkeypatch):
    monkeypatch.setattr(ladder, "_fetch_pool", lambda *_args, **_kwargs: ([], _raw().replace("10100", "10110")))
    with pytest.raises(ladder.MarketCloseLadderBlocked, match="differs from QMT"):
        ladder.capture_market_close_ladder(source, source, DAY)
    assert not ladder._path(DAY).exists()


def test_exact_explicit_empty_pool_does_not_create_streaks(source, monkeypatch):
    raw = json.dumps({"rc": 0, "data": {"qdate": 20261106, "tc": 0, "pool": []}})
    monkeypatch.setattr(ladder, "_fetch_pool", lambda *_args, **_kwargs: ([], raw))
    assert ladder.capture_market_close_ladder(source, source, DAY)["sample_count"] == 0
    result = ladder.load_market_close_ladder(DAY, quotes=[], now=OBSERVED)
    assert result["limit_up_count"] == 0 and result["max_streak"] is None and result["rows"] == []
