from datetime import date, datetime, timedelta
from types import SimpleNamespace

from sqlalchemy import create_engine, text

from server.api.routers import market_stage


def _fixture(monkeypatch):
    first = date(2024, 1, 1)
    days = [first + timedelta(days=i) for i in range(560)]
    sessions = [day.isoformat() for day in days if day.weekday() < 5]
    calendar = SimpleNamespace(
        start_date=sessions[0], end_date=sessions[-1], batch_id="fixture-calendar",
        sessions_between=lambda start, end: [day for day in sessions if start <= day <= end],
    )
    monkeypatch.setattr(market_stage, "load_trade_calendar_window_receipt", lambda *_a, **_k: calendar)
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)"))
        connection.execute(text("INSERT INTO si_trade_calendar VALUES (:date,1)"), [{"date": day} for day in sessions])
        connection.execute(text("""
            CREATE TABLE sm_index_kline (index_code TEXT, k_type INT, trade_date TEXT,
                trade_time TEXT, open REAL, high REAL, low REAL, close REAL, amount REAL, etl_sync_at TEXT)
        """))
        connection.execute(text("""
            INSERT INTO sm_index_kline VALUES ('000001',1,:day,:day,:price,:high,:low,:price,1000,:known)
        """), [dict(day=day, price=100+i, high=101+i, low=99+i, known=f"{day} 16:00:00")
                 for i, day in enumerate(sessions)])
    return engine, sessions


def test_before_close_read_uses_previous_session_and_does_not_project_today(monkeypatch):
    engine, days = _fixture(monkeypatch)
    today, expected = days[-2], days[-3]
    result = market_stage.build_market_stage(
        engine, engine, trade_date=None, now=datetime.fromisoformat(f"{today} 16:15:00"),
    )
    assert result["status"] == "READY"
    assert result["trade_date"] == result["snapshot"]["periods"]["daily"]["end_date"] == expected


def test_explicit_decision_blocks_a_target_imported_after_the_cutoff(monkeypatch):
    engine, days = _fixture(monkeypatch)
    target = days[-3]
    with engine.begin() as connection:
        connection.execute(text("UPDATE sm_index_kline SET etl_sync_at=:later WHERE trade_date=:day"),
                           {"day": target, "later": f"{target} 21:00:00"})
    result = market_stage.build_market_stage(
        engine, engine, trade_date=date.fromisoformat(target),
        now=datetime.fromisoformat(f"{days[-2]} 21:00:00"),
    )
    assert result["status"] == "DATA_BLOCKED"
    assert result["trade_date"] == target


def test_replay_keeps_twenty_session_realizations_out_of_decision_features(monkeypatch):
    engine, days = _fixture(monkeypatch)
    result = market_stage.build_market_stage(
        engine, engine, trade_date=None, now=datetime.fromisoformat(f"{days[-1]} 21:00:00"), replay=True,
    )
    report = result["replay"]
    assert report["status"] == "READY"
    assert report["end_date"] == days[-21]
    assert len(report["rows"]) == 20
    last = report["rows"][-1]
    assert last["assessment"]["periods"]["daily"]["end_date"] == days[-21]
    assert last["outcomes"]["20"] is not None
