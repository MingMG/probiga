from datetime import date, datetime, timedelta
import json
from types import SimpleNamespace

from sqlalchemy import create_engine, text

from server.api.routers import market_stage
from server.common.index_daily_publication import FREE_RECEIPT_SCHEMA, canonical_free_index_rows
from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_datadir_result import row_hash


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
                trade_time TEXT, open REAL, high REAL, low REAL, close REAL, amount REAL, etl_sync_at TEXT,
                volume REAL, `change` REAL, change_pct REAL)
        """))
        connection.execute(text("""
            INSERT INTO sm_index_kline VALUES ('000001',1,:day,:day,:price,:high,:low,:price,1000,:known,2,0,0)
        """), [dict(day=day, price=100+i, high=101+i, low=99+i, known=f"{day} 16:00:00")
                 for i, day in enumerate(sessions)])
        connection.execute(text("""CREATE TABLE qmt_datadir_import_partition (
            dataset TEXT, status TEXT, trade_date TEXT, row_count INT, code_count INT,
            content_sha256 TEXT, finished_at TEXT, detail_json TEXT)"""))
        connection.execute(text("""CREATE TABLE acquisition_partition_state (
            dataset TEXT, source TEXT, partition_key TEXT, status TEXT, target_date TEXT,
            request_id TEXT, written_rows INT, last_success_at TEXT, detail_json TEXT)"""))
        rows = connection.execute(text("""SELECT index_code,trade_time,trade_date,k_type,open,close,
            high,low,volume,amount,`change`,change_pct FROM sm_index_kline""")).mappings().all()
        connection.execute(text("""INSERT INTO qmt_datadir_import_partition VALUES
            ('index_daily','COMPLETE',:day,1,1,:digest,:finished,:detail)"""), [
                dict(day=row["trade_date"], digest="a"*64, finished=f"{row['trade_date']} 17:00:00",
                     detail=json.dumps(dict(readback_rows=1, readback_codes=1, row_hash=row_hash([row]))))
                for row in rows
            ])
    return engine, sessions


def test_before_close_read_uses_previous_session_and_does_not_project_today(monkeypatch):
    engine, days = _fixture(monkeypatch)
    today, expected = days[-2], days[-3]
    result = market_stage.build_market_stage(
        engine, engine, trade_date=None, now=datetime.fromisoformat(f"{today} 16:15:00"),
    )
    assert result["status"] == "READY"
    assert result["trade_date"] == result["snapshot"]["periods"]["daily"]["end_date"] == expected


def test_explicit_decision_blocks_a_target_published_after_the_cutoff(monkeypatch):
    engine, days = _fixture(monkeypatch)
    target = days[-3]
    with engine.begin() as connection:
        connection.execute(text("UPDATE sm_index_kline SET etl_sync_at=:started WHERE trade_date=:day"),
                           {"day": target, "started": f"{target} 19:59:00"})
        connection.execute(text("UPDATE qmt_datadir_import_partition SET finished_at=:later WHERE trade_date=:day"),
                           {"day": target, "later": f"{target} 20:01:00"})
    result = market_stage.build_market_stage(
        engine, engine, trade_date=date.fromisoformat(target),
        now=datetime.fromisoformat(f"{days[-2]} 21:00:00"),
    )
    assert result["status"] == "DATA_BLOCKED"
    assert result["trade_date"] == target


def test_latest_observation_reads_cross_midnight_publication(monkeypatch):
    engine, days = _fixture(monkeypatch)
    now_day, target = days[-1], days[-2]
    with engine.begin() as connection:
        connection.execute(text("UPDATE sm_index_kline SET etl_sync_at=:later WHERE trade_date=:day"),
                           {"day": target, "later": f"{now_day} 00:30:00"})
        connection.execute(text("UPDATE qmt_datadir_import_partition SET finished_at=:later WHERE trade_date=:day"),
                           {"day": target, "later": f"{now_day} 00:31:00"})
    result = market_stage.build_market_stage(
        engine, engine, trade_date=None, now=datetime.fromisoformat(f"{now_day} 10:00:00"),
    )
    assert result["status"] == "READY"
    assert result["evidence_mode"] == "CURRENT_OBSERVATION"
    assert result["snapshot"]["periods"]["daily"]["end_date"] == target


def test_receipt_hash_mismatch_blocks_historical_decision(monkeypatch):
    engine, days = _fixture(monkeypatch)
    target = days[-3]
    with engine.begin() as connection:
        connection.execute(text("UPDATE sm_index_kline SET close=close+0.5 WHERE trade_date=:day"), {"day": target})
    result = market_stage.build_market_stage(
        engine, engine, trade_date=date.fromisoformat(target),
        now=datetime.fromisoformat(f"{days[-1]} 21:00:00"),
    )
    assert result["status"] == "DATA_BLOCKED"
    assert result["evidence_mode"] == "VERIFIED_PUBLICATION"


def test_verified_public_index_receipt_is_used_with_its_persisted_fingerprint(monkeypatch):
    engine, days = _fixture(monkeypatch)
    target = days[-3]
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM qmt_datadir_import_partition WHERE trade_date=:day"), {"day": target})
        rows = connection.execute(text("SELECT * FROM sm_index_kline WHERE trade_date=:day"),
                                  {"day": target}).mappings().all()
        captured = f"{target} 17:45:00"
        code_hash = canonical_digest(["000001"])
        receipt = dict(schema=FREE_RECEIPT_SCHEMA, dataset="index_daily", provider="tencent_east_verified",
                       target_date=target, status="complete", captured_at=captured, row_count=1, code_count=1,
                       expected_count=1, code_set_hash=code_hash, expected_code_set_hash=code_hash,
                       missing_codes=[], qmt_attestation_claimed=False,
                       row_hash=canonical_digest(canonical_free_index_rows(rows)))
        receipt_id = canonical_digest(receipt)
        connection.execute(text("""INSERT INTO acquisition_partition_state VALUES
            ('index_daily','tencent_east_verified','__index_daily_partition__','complete',:day,:receipt,1,
             :captured,:detail)"""), dict(day=target, receipt=receipt_id, captured=captured,
                                          detail=json.dumps({**receipt, "receipt_id": receipt_id})))
    result = market_stage.build_market_stage(
        engine, engine, trade_date=date.fromisoformat(target),
        now=datetime.fromisoformat(f"{days[-1]} 21:00:00"),
    )
    assert result["status"] == "READY"
    assert result["evidence_mode"] == "VERIFIED_PUBLICATION"


def test_latest_null_bar_returns_data_blocked_instead_of_raising(monkeypatch):
    engine, days = _fixture(monkeypatch)
    with engine.begin() as connection:
        connection.execute(text("UPDATE sm_index_kline SET open=NULL WHERE trade_date=:day"), {"day": days[-1]})
    result = market_stage.build_market_stage(
        engine, engine, trade_date=None, now=datetime.fromisoformat(f"{days[-1]} 21:00:00"),
    )
    assert result["status"] == "DATA_BLOCKED"


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
