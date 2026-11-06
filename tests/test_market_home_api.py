import json
from datetime import date, datetime
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from server.api.routers import market_home
from server.common.turnover_snapshot import _qmt_target_from_row, _row_insert_params, parse_eastmoney_turnover_response


DAY = date(2026, 11, 6)
DAY_TEXT = DAY.isoformat()


@pytest.fixture
def market_db(monkeypatch):
    engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    with engine.begin() as connection:
        for sql in (
            "CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)",
            "CREATE TABLE sm_stock_kline (id INTEGER PRIMARY KEY, stock_code TEXT, short_name TEXT, trade_date TEXT, close REAL, "
            "pre_close REAL, amount REAL, turnover_ratio REAL, data_source TEXT, quality_status TEXT, "
            "permission_status TEXT, k_type INTEGER, adjust_type INTEGER, etl_sync_at TEXT, "
            "open REAL DEFAULT 100, high REAL DEFAULT 110, low REAL DEFAULT 90, volume REAL DEFAULT 10)",
            "CREATE TABLE sm_index_kline (index_code TEXT, trade_date TEXT, trade_time TEXT, close REAL, "
            "change_pct REAL, amount REAL, k_type INTEGER, etl_sync_at TEXT)",
            "CREATE TABLE st_qmt_realtime_sync_receipt_v2 (receipt_id TEXT, source_provider TEXT, "
            "source_snapshot_token TEXT, source_generated_at TEXT, expected_count INTEGER, observed_count INTEGER, "
            "coverage REAL, published_at TEXT, capture_mode TEXT, quality_status TEXT, evidence_json TEXT, created_at TEXT)",
            "CREATE TABLE sm_stock_current (stock_code TEXT, short_name TEXT, price REAL, change_pct REAL, "
            "amount REAL, snapshot_at TEXT, batch_id TEXT, data_source TEXT, quality_status TEXT, permission_status TEXT)",
            "CREATE TABLE sm_market_radar_event (event_id INTEGER, event_type TEXT, direction TEXT, stock_code TEXT, "
            "sector_name TEXT, snapshot_at TEXT, created_at TEXT, data_source TEXT, detail_json TEXT)",
            "CREATE TABLE sm_limit_up_event (event_id INTEGER, trade_date TEXT, event_type TEXT, stock_code TEXT, "
            "short_name TEXT, event_at TEXT, source_snapshot_at TEXT, created_at TEXT, source_provider TEXT, "
            "limit_status_basis TEXT, evidence_json TEXT)",
            "CREATE TABLE st_market_field_capture_run (run_id TEXT, collector_build_sha TEXT, target_date TEXT, "
            "published_at TEXT, decision_at TEXT, capture_kind TEXT, provider TEXT, source_field TEXT, unit TEXT, "
            "status TEXT, expected_count INTEGER, fetched_count INTEGER, valid_count INTEGER, matched_count INTEGER, "
            "promoted_count INTEGER, semantic_sha256 TEXT, field_value_root_sha256 TEXT)",
            "CREATE TABLE st_market_field_capture_row (run_id TEXT, stock_code TEXT, trade_date TEXT, k_type INTEGER, "
            "adjust_type INTEGER, target_row_id INTEGER, field_value_decimal REAL, qmt_open REAL, qmt_high REAL, qmt_low REAL, qmt_close REAL, "
            "qmt_volume_shares REAL, qmt_amount REAL, qmt_data_source TEXT, qmt_quality_status TEXT, "
            "qmt_permission_status TEXT, validation_status TEXT, promoted_at TEXT, "
            "source_open REAL, source_high REAL, source_low REAL, source_close REAL, source_volume_shares REAL, "
            "source_amount REAL, raw_row_text TEXT, raw_payload BLOB, raw_payload_sha256 TEXT, snapshot_row_sha256 TEXT, "
            "captured_at TEXT, provider_observed_at_text TEXT, provider_observed_at TEXT, qmt_received_at TEXT, "
            "qmt_batch_id TEXT, qmt_data_version TEXT, target_prewrite_sha256 TEXT, target_fact_sha256 TEXT, validation_error TEXT)",
        ):
            connection.execute(text(sql))
        connection.execute(text("INSERT INTO si_trade_calendar VALUES (:day, 1)"), {"day": DAY_TEXT})
    for name in ("get_engine", "get_kline_engine", "get_current_engine"):
        monkeypatch.setattr(market_home, name, lambda: engine)
    market_home._cache.clear()
    yield engine
    market_home._cache.clear()
    engine.dispose()


def _stock(connection, code, close, *, day=DAY_TEXT, amount=100, turnover=2, source="qmt_datadir",
           quality="VERIFIED"):
    connection.execute(text("""
        INSERT INTO sm_stock_kline (stock_code,short_name,trade_date,close,pre_close,amount,turnover_ratio,
                                    data_source,quality_status,permission_status,k_type,adjust_type,etl_sync_at)
        VALUES (:code,:code,:day,:close,100,:amount,:turnover,:source,:quality,'SUPPORTED',1,0,:etl)
    """), {"code": code, "day": day, "close": close, "amount": amount, "turnover": turnover,
           "source": source, "quality": quality, "etl": f"{day} 16:00:00"})


def _index(connection, code, *, day=DAY_TEXT, trade_time="close", etl="after_close"):
    connection.execute(text("""
        INSERT INTO sm_index_kline VALUES (:code,:day,:trade_time,1000,-2,100,1,:etl)
    """), {"code": code, "day": day, "trade_time": f"{day} 15:00:00" if trade_time == "close" else trade_time,
           "etl": f"{day} 16:00:00" if etl == "after_close" else etl})


def test_board_uses_only_verified_exact_session_and_keeps_unsupported_values_unknown(market_db):
    with market_db.begin() as connection:
        for i, value in enumerate((93, 95, 97, 99, 100, 101, 103, 105, 107), 1):
            _stock(connection, f"{i:06d}", value, amount=200 if value == 100 else 100)
        _stock(connection, "000010", 140, day="2026-11-05")
        _stock(connection, "000011", 150, source="unknown")
        _stock(connection, "000012", 160, quality="RAW")
        _stock(connection, "000013", 170)
        _stock(connection, "000013", 180)
        _index(connection, "000680")
        _index(connection, "000688", day="2026-11-05")
        _index(connection, "000300", trade_time="2026-11-05 15:00:00")
        _index(connection, "000905")

    result = market_home.build_market_board(DAY, now=datetime(2026, 11, 7))
    assert result["status"] == "partial"
    assert result["trade_date"] == DAY_TEXT
    assert result["as_of"] == "2026-11-06T15:00:00+08:00"
    assert result["sample_size"] == sum(row["count"] for row in result["distribution"]) == 9
    assert result["strong"] == {"up_3pct": 3, "down_3pct": 3}
    assert result["rankings"]["gainers"][0]["stock_code"] == "000009"
    assert result["rankings"]["losers"][0]["stock_code"] == "000001"
    assert [row["stock_code"] for row in result["rankings"]["amount"]][:3] == ["000005", "000001", "000002"]
    assert result["rankings"]["turnover"] == []
    assert {"rankings.turnover", "indices", "limit_ladder", "events"} <= set(result["unavailable_fields"])
    assert [row["index_code"] for row in result["indices"]] == ["000680"]
    assert result["indices"][0]["source"] == "sm_index_kline"


def test_complete_close_survives_slower_quote_read_and_caches_exact_date(market_db, monkeypatch):
    with market_db.begin() as connection:
        _stock(connection, "000001", 103, source=sorted(market_home.VERIFIED_FREE_DAILY_ROW_SOURCES)[0],
               quality="SOURCE_VERIFIED")
        for code in market_home.INDEX_NAMES:
            _index(connection, code)

    elapsed = 0.0
    read_quotes = market_home._day_quotes

    def slow_quotes(*args):
        nonlocal elapsed
        rows = read_quotes(*args)
        elapsed = 8.5
        return rows

    monkeypatch.setattr(market_home, "monotonic", lambda: elapsed)
    monkeypatch.setattr(market_home, "_day_quotes", slow_quotes)
    monkeypatch.setattr(market_home, "load_market_close_ladder", lambda *_, **__: {"count": 1})
    monkeypatch.setattr(market_home, "_limit_up_events", lambda *_: [{"snapshot_at": "2026-11-06T10:00:00"}])

    result = market_home.build_market_board(DAY, now=datetime(2026, 11, 7))
    assert result["status"] == "available"
    assert result["unavailable_fields"] == []

    market_home._cache[DAY_TEXT] = (0.0, result)
    elapsed = 20.0
    monkeypatch.setattr(market_home, "build_market_board", lambda *_, **__: {"status": "unavailable"})
    assert market_home.market_board(DAY_TEXT) == result
    elapsed = 31.0
    assert market_home.market_board(DAY_TEXT)["status"] == "unavailable"


def test_index_close_requires_exact_marker_and_post_close_ingestion(market_db):
    with market_db.begin() as connection:
        _index(connection, "000001")
        _index(connection, "399001", trade_time="2026-11-06 09:35:00")
        _index(connection, "399006", etl="2026-11-06 14:58:00")
        _index(connection, "000688", trade_time=None)
        _index(connection, "000300", etl=None)
        _index(connection, "000852", etl="2026-11-07 01:00:00")
    result = market_home.build_market_board(DAY, now=datetime(2026, 11, 7))
    assert [row["index_code"] for row in result["indices"]] == ["000001"]
    index = result["indices"][0]
    assert index["as_of"] == "2026-11-06T15:00:00+08:00"
    assert index["ingested_at"] == "2026-11-06T16:00:00+08:00"
    assert index["evidence_mode"] == "CURRENT_OBSERVATION"


def test_board_keeps_missing_amount_and_unbound_live_quotes_out(market_db):
    with market_db.begin() as connection:
        _stock(connection, "000001", 102, amount=None, source="tencent_qt_verified", quality="SOURCE_VERIFIED")
        _stock(connection, "000002", 101, turnover=4, source="tencent_qt_verified", quality="SOURCE_VERIFIED")
        connection.execute(text("""
            INSERT INTO st_qmt_realtime_sync_receipt_v2 VALUES
            ('id','gj_big_qmt_inner','token','2026-11-06 10:00:00',1,1,1,'2026-11-06 10:00:01',
             'LIVE_FORWARD','PASS','{"full_batch_id":"verified"}','2026-11-06 10:00:01')
        """))
        connection.execute(text("""
            INSERT INTO sm_stock_current VALUES
            ('000003','unbound',12,6,12,'2026-11-06 10:00:00','unbound','gj_big_qmt_inner','VALIDATED','SUPPORTED')
        """))
    live = market_home.build_market_board(DAY, now=datetime(2026, 11, 6, 10, 0, 5))
    assert live["status"] == "unavailable"
    assert live["sample_size"] == 0 and live["strong"]["up_3pct"] is None

    with market_db.begin() as connection:
        connection.execute(text("""
            INSERT INTO sm_stock_current VALUES
            ('000004','bound',12,3,15,'2026-11-06 10:00:00','verified','gj_big_qmt_inner','VALIDATED','SUPPORTED')
        """))
    live = market_home.build_market_board(DAY, now=datetime(2026, 11, 6, 10, 0, 5))
    assert live["source"] == "sm_stock_current"
    assert live["strong"]["up_3pct"] == 1
    assert [row["stock_code"] for row in live["rankings"]["gainers"]] == ["000004"]

    daily = market_home.build_market_board(DAY, now=datetime(2026, 11, 7))
    assert daily["rankings"]["amount"] == []
    assert daily["rankings"]["gainers"][0]["amount"] is None
    assert daily["rankings"]["turnover"][0]["stock_code"] == "000002"


@pytest.mark.parametrize("problem", [None, "stage", "field_root", "missing_capture", "quote_mismatch"])
def test_board_native_turnover_requires_complete_evidence_and_exact_quotes(market_db, monkeypatch, problem):
    items = [{"stock_code": code, "trade_date": DAY_TEXT, "turnover_percent": rate}
             for code, rate in (("000001", "2"), ("000002", "4.5"))]
    root = market_home.canonical_digest(items) if problem != "field_root" else "0" * 64
    with market_db.begin() as connection:
        _stock(connection, "000001", 102, turnover=2)
        _stock(connection, "000002", 103, turnover=4.5)
        connection.execute(text("""
            INSERT INTO st_market_field_capture_run VALUES
            ('capture','build',:day,'2026-11-06 16:30:00','2026-11-06 16:20:00','DAILY_TURNOVER_F61',
             'eastmoney.push2his.kline','f61','PERCENT','COMPLETED',2,2,2,2,2,'semantic',:root)
        """), {"day": DAY_TEXT, "root": root})
        connection.execute(text("""
            INSERT INTO st_market_field_capture_row (run_id,stock_code,trade_date,k_type,adjust_type,target_row_id,
             field_value_decimal,qmt_open,qmt_high,qmt_low,qmt_close,qmt_volume_shares,qmt_amount,qmt_data_source,
             qmt_quality_status,qmt_permission_status,validation_status,promoted_at) VALUES
            ('capture','000001',:day,1,0,1,2,100,110,90,102,10,100,'qmt_datadir','VERIFIED','SUPPORTED',
             'MATCHED','2026-11-06 16:30:00'),
            ('capture','000002',:day,1,0,2,4.5,100,110,90,:close,10,100,'qmt_datadir','VERIFIED','SUPPORTED',
             'MATCHED','2026-11-06 16:30:00')
        """), {"day": DAY_TEXT, "close": 104 if problem == "quote_mismatch" else 103})
        if problem == "missing_capture":
            connection.execute(text("DELETE FROM st_market_field_capture_row WHERE stock_code='000002'"))

    def completion(_, **kwargs):
        assert kwargs["raw_run_id"] == "capture" and kwargs["target_date"] == DAY_TEXT
        assert kwargs["stage_name"] == "target_turnover_snapshot"
        if problem == "stage":
            raise RuntimeError("missing verified stage")
        return {"known_at": "2026-11-06 16:31:00", "receipt": {
            "expected_count": 2, "promoted_count": 2, "semantic_sha256": "semantic", "field_value_root_sha256": root,
            "decision_at": "2026-11-06 16:20:00", "published_at": "2026-11-06 16:30:00",
        }}

    monkeypatch.setattr(market_home, "load_completed_market_capture_receipt", completion)
    result = market_home.build_market_board(DAY, now=datetime(2026, 11, 7))
    assert result["sample_size"] == 2
    assert len(result["rankings"]["gainers"]) == 2
    if problem in {"stage", "field_root", "missing_capture"}:
        assert not result["rankings"]["turnover"]
        assert result["turnover_evidence"] is None
        assert result["turnover_coverage"]["status"] == "unavailable"
        assert "rankings.turnover" in result["unavailable_fields"]
    elif problem == "quote_mismatch":
        assert [(row["stock_code"], row["turnover_rate"]) for row in result["rankings"]["turnover"]] == [("000001", 2)]
        assert result["turnover_coverage"] == {"status": "partial", "verified_count": 1, "sample_size": 2,
                                               "excluded_count": 1, "excluded_codes": ["000002"]}
        assert result["turnover_evidence"]["field_value_root_sha256"] == root
        assert "rankings.turnover" not in result["unavailable_fields"]
    else:
        rank = result["rankings"]["turnover"]
        assert [(row["stock_code"], row["turnover_rate"]) for row in rank] == [("000002", 4.5), ("000001", 2)]
        assert {row["turnover_rate_source"] for row in rank} == {"eastmoney.push2his.kline"}
        assert result["turnover_evidence"]["known_at"] == "2026-11-06T16:31:00+08:00"
        assert result["turnover_coverage"]["status"] == "complete"


@pytest.mark.parametrize("source_amount, corrupt", [(12690811.41, False), (12690811.51, False), (12690811.41, True)])
def test_corrected_native_turnover_requires_exact_retained_source(market_db, monkeypatch, source_amount, corrupt):
    code = "920717"
    decision_at = datetime(2026, 11, 6, 16, 20)
    published_at = datetime(2026, 11, 6, 16, 30)
    target = _qmt_target_from_row({
        "id": 1, "stock_code": code, "trade_date": DAY, "k_type": 1, "adjust_type": 0,
        "open": 6.35, "high": 6.43, "low": 6.18, "close": 6.29, "volume": 642900, "amount": 4072608,
        "received_at": datetime(2026, 11, 6, 15, 44, 47), "data_source": "qmt_datadir",
        "batch_id": "original", "data_version": "original", "quality_status": "VERIFIED", "permission_status": "SUPPORTED",
    }, target_date=DAY, decision_at=decision_at)
    captured = parse_eastmoney_turnover_response(
        target=target, raw_payload=json.dumps({"rc": 0, "data": {"code": code, "klines": [
            f"{DAY_TEXT},6.35,6.29,6.43,6.18,20580,{source_amount},3.94,-0.94,-0.06,4.65",
        ]}}).encode(), provider_http_date="Fri, 06 Nov 2026 08:15:00 GMT",
        captured_at=datetime(2026, 11, 6, 16, 16), decision_at=decision_at,
    )
    params = _row_insert_params(SimpleNamespace(run_id="capture"), captured, published_at=published_at)
    if corrupt:
        params["raw_payload"] = params["raw_payload"].replace(b'920717', b'920718')
    root = market_home.canonical_digest([{"stock_code": code, "trade_date": DAY_TEXT, "turnover_percent": "4.65"}])
    with market_db.begin() as connection:
        _stock(connection, code, 6.29, amount=12690811, turnover=4.65)
        connection.execute(text("UPDATE sm_stock_kline SET open=6.35,high=6.43,low=6.18,volume=2058000"))
        connection.execute(text("""
            INSERT INTO st_market_field_capture_run VALUES
            ('capture','build',:day,:published,:decision,'DAILY_TURNOVER_F61','eastmoney.push2his.kline','f61',
             'PERCENT','COMPLETED',1,1,1,1,1,'semantic',:root)
        """), {"day": DAY_TEXT, "published": published_at, "decision": decision_at, "root": root})
        columns = ",".join(params)
        values = ",".join(f":{key}" for key in params)
        connection.execute(text(f"INSERT INTO st_market_field_capture_row ({columns}) VALUES ({values})"), params)
    monkeypatch.setattr(market_home, "load_completed_market_capture_receipt", lambda *_args, **_kwargs: {
        "known_at": "2026-11-06 16:31:00", "receipt": {
            "expected_count": 1, "promoted_count": 1, "semantic_sha256": "semantic", "field_value_root_sha256": root,
            "decision_at": decision_at, "published_at": published_at,
        },
    })
    result = market_home.build_market_board(DAY, now=datetime(2026, 11, 7))
    assert result["sample_size"] == 1
    if corrupt or source_amount > 12690811.50:
        assert not result["rankings"]["turnover"]
    else:
        assert result["rankings"]["turnover"][0]["turnover_rate"] == 4.65
        assert result["turnover_coverage"]["status"] == "complete"
        assert result["turnover_evidence"]["corrected_binding_count"] == 1


def test_board_invalid_date_is_rejected_and_repeat_uses_bounded_cache(monkeypatch):
    route = next(item for item in market_home.router.routes if item.path == "/monitor/board")
    assert route.methods == {"GET"}
    assert route.dependant.query_params[0].alias == "date"
    calls = []
    market_home._cache.clear()
    monkeypatch.setattr(market_home, "build_market_board", lambda day, **_: calls.append(day) or {"date": str(day)})
    try:
        for invalid in ("0", "2026-02-29", "2026-11-6"):
            with pytest.raises(HTTPException) as exc:
                market_home.market_board(invalid)
            assert exc.value.status_code == 422
        for _ in range(2):
            assert market_home.market_board(DAY_TEXT) == {"date": DAY_TEXT}
        assert calls == [DAY]
    finally:
        market_home._cache.clear()


@pytest.mark.parametrize(("stamp", "before", "after"), [
    ("10:00:00", "10:01:30", "10:01:31"),
    ("11:29:50", "11:30:00", "11:30:01"),
])
def test_cache_never_serves_live_quotes_past_freshness_or_session(market_db, monkeypatch, stamp, before, after):
    now = datetime.fromisoformat(f"{DAY_TEXT}T{before}")

    class Clock(datetime):
        @classmethod
        def now(cls, tz):
            return now.replace(tzinfo=tz)

    monkeypatch.setattr(market_home, "datetime", Clock)
    monkeypatch.setattr(market_home, "monotonic", lambda: 5.0)
    cached = {"source": "sm_stock_current", "as_of": f"{DAY_TEXT}T{stamp}+08:00"}
    market_home._cache[DAY_TEXT] = (4.0, cached)
    calls = []
    monkeypatch.setattr(market_home, "build_market_board", lambda _, **kw: calls.append(kw["now"]) or {})

    assert market_home.market_board(DAY_TEXT) == cached
    assert calls == []
    now = datetime.fromisoformat(f"{DAY_TEXT}T{after}")
    assert market_home.market_board(DAY_TEXT) == {}
    assert calls == [now]


def test_live_snapshot_that_expires_during_reads_is_withheld(market_db, monkeypatch):
    elapsed = 0

    def load_events(*_):
        nonlocal elapsed
        elapsed = 2
        return []

    monkeypatch.setattr(market_home, "monotonic", lambda: elapsed)
    monkeypatch.setattr(market_home, "_live_quotes", lambda *_: ([{
        "stock_code": "000001", "trade_date": DAY_TEXT, "price": 12, "change_pct": 3,
        "data_source": market_home.LIVE_SOURCE,
    }], datetime(2026, 11, 6, 10)))
    monkeypatch.setattr(market_home, "_market_events", load_events)

    result = market_home.build_market_board(DAY, now=datetime(2026, 11, 6, 10, 1, 29))
    assert result["sample_size"] == 0
    assert result["reason"] == "live_quote_expired"


@pytest.mark.parametrize("now", [datetime(2026, 11, 6, 12), datetime(2026, 11, 7)])
def test_board_event_projection_is_same_date_and_public_only(market_db, now):
    with market_db.begin() as connection:
        connection.execute(text("""
            INSERT INTO sm_market_radar_event VALUES
            (1,'stock_anomaly','UP','000001',NULL,'2026-11-06 10:00:00','2026-11-06 10:00:05',
             'qmt_full_tick_5level','{"stock":{"short_name":"样本股","unrelated":"omitted"}}'),
            (2,'stock_anomaly','DOWN','000002',NULL,'2026-11-05 10:00:00','2026-11-05 10:00:05',
             'qmt_full_tick_5level','{}'),
            (3,'stock_anomaly','UP','000003',NULL,'2026-11-06 11:00:00','2026-11-06 11:00:05',
             'other_source','{}'),
            (4,'stock_anomaly','UP','000004',NULL,'2026-11-06 12:00:00','2026-11-06 12:00:05',
             'qmt_full_tick_5level','{}')
        """))
    result = market_home.build_market_board(DAY, now=now)
    assert result["sample_size"] == 0
    assert result["as_of_basis"] == "radar_observation"
    assert result["events"] == [{"event_type": "stock_anomaly", "direction": "UP", "stock_code": "000001",
                                 "sector_name": None, "snapshot_at": "2026-11-06T10:00:00+08:00",
                                 "data_source": "qmt_full_tick_5level", "detail_json": {"short_name": "样本股"}}]
    assert "events" not in result["unavailable_fields"]


def test_board_limit_events_keep_actual_type_and_publication_time(market_db):
    with market_db.begin() as connection:
        connection.execute(text("""
            INSERT INTO sm_limit_up_event VALUES
            (1,:day,'RESEAL','000001','样本股','2026-11-06 15:00:02','2026-11-06 15:00:04',
             '2026-11-06 15:00:05','gj_big_qmt_inner',:basis,:evidence),
            (2,:day,'RESEAL','000002','wrong provider','2026-11-06 15:00:02','2026-11-06 15:00:04',
             '2026-11-06 15:00:05','other_provider',:basis,:evidence),
            (3,:day,'QUEUE_SNAPSHOT','000003','queue sample','2026-11-06 15:00:02','2026-11-06 15:00:04',
             '2026-11-06 15:00:05','gj_big_qmt_inner',:basis,:evidence),
            (4,:day,'SEAL','000004','future evidence','2026-11-06 15:00:02','2026-11-06 15:00:04',
             '2026-11-07 15:00:05','gj_big_qmt_inner',:basis,:evidence)
        """), {"day": DAY_TEXT, "basis": market_home.LIMIT_STATUS_BASIS,
               "evidence": '{"at_limit":true,"sealed":true,"limit_status_basis":"PRE_CLOSE_BOARD_RULE_ROUND_HALF_UP"}'})

    early = market_home.build_market_board(DAY, now=datetime(2026, 11, 6, 15, 0, 4))
    assert early["events"] == []
    result = market_home.build_market_board(DAY, now=datetime(2026, 11, 7))
    assert result["source"] == "sm_limit_up_event"
    assert result["limit_ladder"] is None
    assert result["events"] == [{
        "event_type": "RESEAL", "event_family": "LIMIT_UP_LIFECYCLE", "stock_code": "000001",
        "snapshot_at": "2026-11-06T15:00:02+08:00", "source_snapshot_at": "2026-11-06T15:00:04+08:00",
        "available_at": "2026-11-06T15:00:05+08:00", "data_source": "gj_big_qmt_inner",
        "detail_json": {"short_name": "样本股"},
    }]
