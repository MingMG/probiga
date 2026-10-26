from __future__ import annotations

import datetime as dt
import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from decimal import Decimal
from threading import Barrier

import pytest
import requests
from sqlalchemy import create_engine, text

from tools.import_sina_minute_history import (
    MINUTE_GRID,
    TENCENT_SOURCE,
    DailyAnchor,
    ImportErrorBase,
    SOURCE,
    _read_cached_payload,
    _write_cached_payload,
    _market_suffix,
    _bounded_json_get,
    _proof_digest,
    _paced_fetch,
    _readback_proofs,
    _load_anchors,
    _normalize_storage_rows,
    _row_digest,
    _source_circuit_tripped,
    parse_sina_session,
    parse_tencent_session,
    parse_tencent_kline_session,
)


def _payload() -> list[dict]:
    target = dt.date(2026, 10, 23)
    rows = []
    for clock in MINUTE_GRID:
        stamp = dt.datetime.combine(
            target, dt.time.fromisoformat(clock)
        )
        close = "11.50" if clock == "15:00:00" else "11.20"
        rows.append({
            "day": stamp.strftime("%Y-%m-%d %H:%M:%S"),
            "open": "11.20",
            "high": "11.50",
            "low": "10.90",
            "close": close,
            "volume": "10000",
            "amount": "112000",
        })
    return rows


def _anchor() -> DailyAnchor:
    return DailyAnchor(
        stock_code="603278",
        qmt_code="603278.SH",
        pre_close=Decimal("10.45"),
        close=Decimal("11.50"),
        high=Decimal("11.50"),
        low=Decimal("10.90"),
        volume=Decimal("2380000"),
        amount=Decimal("26656000"),
    )


def test_sina_parser_accepts_exact_238_bar_contract():
    rows = parse_sina_session(
        _payload(),
        anchor=_anchor(),
        trade_date="2026-10-23",
        captured_at=dt.datetime(2026, 10, 24, 9, 0),
        batch_id="sina-test",
    )

    assert len(rows) == 238
    assert rows[0]["trade_time"].strftime("%H:%M:%S") == "09:31:00"
    assert rows[-1]["trade_time"].strftime("%H:%M:%S") == "15:00:00"
    assert rows[-1]["data_source"] == "sina_public_1m"
    assert rows[-1]["price"] == Decimal("11.50")


def test_sina_parser_rejects_an_incomplete_session():
    payload = _payload()
    payload.pop(100)

    with pytest.raises(
        ImportErrorBase,
        match="minute volume differs from the verified daily anchor",
    ):
        parse_sina_session(
            payload,
            anchor=_anchor(),
            trade_date="2026-10-23",
            captured_at=dt.datetime(2026, 10, 24, 9, 0),
            batch_id="sina-test",
        )


def test_sina_fills_only_volume_conserving_no_trade_minutes():
    payload = _payload()
    payload.pop(100)
    anchor = replace(
        _anchor(),
        volume=Decimal("2370000"),
        amount=Decimal("26544000"),
    )

    rows = parse_sina_session(
        payload,
        anchor=anchor,
        trade_date="2026-10-23",
        captured_at=dt.datetime(2026, 10, 24, 9, 0),
        batch_id="sina-test",
    )

    assert len(rows) == 238
    assert rows[100]["volume"] == Decimal("0")
    assert rows[100]["amount"] == Decimal("0")
    assert sum(row["volume"] for row in rows) == anchor.volume


def test_bse_codes_use_the_bj_public_symbol_namespace():
    assert _market_suffix("920001") == "BJ"


def test_source_checkpoint_round_trips_the_exact_payload(tmp_path):
    captured = dt.datetime(2026, 10, 26, 23, 30)
    payload = _payload()

    _write_cached_payload(
        tmp_path,
        source=SOURCE,
        stock_code="603278",
        payload=payload,
        captured_at=captured,
    )
    restored = _read_cached_payload(
        tmp_path,
        source=SOURCE,
        stock_code="603278",
    )

    assert restored == (payload, captured)


def test_bounded_json_response_enforces_a_total_deadline(monkeypatch):
    def timed_out(*_args, **_kwargs):
        raise subprocess.TimeoutExpired("curl", timeout=1)

    monkeypatch.setattr(
        "tools.import_sina_minute_history.subprocess.run",
        timed_out,
    )

    with pytest.raises(requests.Timeout, match="deadline exceeded"):
        _bounded_json_get(
            object(),
            "https://example.invalid",
            params={},
            timeout=1,
        )


def test_each_fetch_worker_uses_its_own_source_session(monkeypatch):
    sessions = []
    barrier = Barrier(2)

    class Session:
        pass

    def new_session(_proxy):
        session = Session()
        sessions.append(session)
        return session

    def fetch(_anchor, *, session, **_kwargs):
        barrier.wait(timeout=2)
        return session

    monkeypatch.setattr(
        "tools.import_sina_minute_history._new_source_session",
        new_session,
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        result = list(pool.map(
            lambda _: _paced_fetch(
                fetch,
                _anchor(),
                request_delay=0,
            ),
            range(2),
        ))

    assert len(sessions) == 2
    assert {id(session) for session in result} == {
        id(session) for session in sessions
    }


def test_source_circuit_opens_after_twenty_consecutive_failures():
    assert not _source_circuit_tripped(19)
    assert _source_circuit_tripped(20)


def test_storage_normalization_matches_mysql_decimal_rounding():
    rows = _normalize_storage_rows([{
        "price": Decimal("1.2345675"),
        "avg_price": None,
        "change": Decimal("-0.0000001"),
        "change_pct": Decimal("2.3456785"),
        "volume": Decimal("10000"),
        "amount": Decimal("123.0"),
    }])

    assert rows[0]["price"] == Decimal("1.234568")
    assert rows[0]["change"] == Decimal("0.000000")
    assert rows[0]["change_pct"] == Decimal("2.345679")


def test_streaming_readback_uses_per_code_proofs():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE sm_stock_minute ("
                "stock_code TEXT, trade_time TEXT, price NUMERIC, "
                "`change` NUMERIC, change_pct NUMERIC, volume NUMERIC, "
                "amount NUMERIC, trade_date TEXT, data_source TEXT)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO sm_stock_minute VALUES "
                "('000001','2026-10-26 09:31:00',11.2,0.1,0.9,100,1120,"
                "'2026-10-26','sina_public_1m'),"
                "('000001','2026-10-26 09:32:00',11.3,0.1,0.9,200,2260,"
                "'2026-10-26','sina_public_1m')"
            )
        )

    rows, codes, digest = _readback_proofs(
        engine,
        trade_date="2026-10-26",
        sources=[SOURCE],
    )

    assert rows == 2
    assert codes == 1
    assert digest == _proof_digest([[
        "000001",
        2,
        _row_digest([
            {
                "stock_code": "000001",
                "trade_time": dt.datetime(2026, 10, 26, 9, 31),
                "price": Decimal("11.2"),
                "change": Decimal("0.1"),
                "change_pct": Decimal("0.9"),
                "volume": Decimal("100"),
                "amount": Decimal("1120"),
            },
            {
                "stock_code": "000001",
                "trade_time": dt.datetime(2026, 10, 26, 9, 32),
                "price": Decimal("11.3"),
                "change": Decimal("0.1"),
                "change_pct": Decimal("0.9"),
                "volume": Decimal("200"),
                "amount": Decimal("2260"),
            },
        ]),
    ]])


def test_daily_anchors_exclude_explicit_suspensions():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE sm_stock_kline ("
                "stock_code TEXT, trade_date TEXT, k_type INTEGER, "
                "adjust_type INTEGER, pre_close TEXT, open TEXT, close TEXT, "
                "high TEXT, low TEXT, volume TEXT, amount TEXT, "
                "quality_status TEXT, "
                "permission_status TEXT)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO sm_stock_kline VALUES "
                "('000001','2026-10-26',1,0,'11.10','11.15','11.30',"
                "'11.40','11.00','1000','11500','VERIFIED','SUPPORTED'),"
                "('000016','2026-10-26',1,0,'2.46','0','2.46',"
                "'0','0','0','0','SUSPENDED','NO_TRADE')"
            )
        )

    anchors, no_trade = _load_anchors(
        engine,
        trade_date="2026-10-26",
        stock_codes=["000001", "000016"],
    )

    assert set(anchors) == {"000001"}
    assert no_trade == frozenset({"000016"})


def test_tencent_fallback_builds_per_minute_values_from_cumulative_snapshots():
    raw_rows = ["0930 10.45 0 0"]
    for index, clock in enumerate(MINUTE_GRID, start=1):
        price = "11.50" if clock == "15:00:00" else "11.20"
        raw_rows.append(
            f"{clock.replace(':', '')[:4]} {price} {index * 100} {index * 112000}"
        )
    payload = {
        "data": {
            "sh603278": {
                "data": {
                    "date": "20261023",
                    "data": raw_rows,
                }
            }
        }
    }

    rows = parse_tencent_session(
        payload,
        anchor=_anchor(),
        trade_date="2026-10-23",
        captured_at=dt.datetime(2026, 10, 24, 9, 0),
        batch_id="tencent-test",
    )

    assert len(rows) == 238
    assert rows[0]["volume"] == Decimal("10000")
    assert rows[0]["amount"] == Decimal("112000")
    assert rows[-1]["price"] == Decimal("11.50")
    assert all(row["data_source"] == TENCENT_SOURCE for row in rows)


def test_tencent_fallback_preserves_missing_amounts_as_unknown():
    raw_rows = ["0930 10.45 0"]
    for index, clock in enumerate(MINUTE_GRID, start=1):
        price = "11.50" if clock == "15:00:00" else "11.20"
        raw_rows.append(f"{clock.replace(':', '')[:4]} {price} {index * 100}")
    payload = {
        "data": {
            "sh603278": {
                "data": {
                    "date": "20261023",
                    "data": raw_rows,
                }
            }
        }
    }

    rows = parse_tencent_session(
        payload,
        anchor=replace(_anchor(), amount=None),
        trade_date="2026-10-23",
        captured_at=dt.datetime(2026, 10, 24, 9, 0),
        batch_id="tencent-test",
    )

    assert all(row["amount"] is None for row in rows)


def test_tencent_m1_history_maps_to_the_238_bar_contract():
    target = dt.date(2026, 10, 23)
    clocks = [
        dt.datetime(2000, 1, 1, 9, 30) + dt.timedelta(minutes=index)
        for index in range(121)
    ] + [
        dt.datetime(2000, 1, 1, 13, 1) + dt.timedelta(minutes=index)
        for index in range(120)
    ]
    raw_rows = []
    for clock in clocks:
        stamp = dt.datetime.combine(target, clock.time()).strftime("%Y%m%d%H%M")
        price = "11.50" if clock.strftime("%H:%M") == "15:00" else "11.20"
        raw_rows.append([stamp, price, price, price, price, "1", {}, "0"])
    payload = {
        "data": {
            "sh603278": {
                "m1": raw_rows,
            }
        }
    }

    rows = parse_tencent_kline_session(
        payload,
        anchor=replace(_anchor(), volume=Decimal("24100")),
        trade_date="2026-10-23",
        captured_at=dt.datetime(2026, 10, 24, 9, 0),
        batch_id="tencent-m1-test",
    )

    assert len(rows) == 238
    assert sum(row["volume"] for row in rows) == Decimal("24100")
    assert rows[0]["trade_time"].strftime("%H:%M") == "09:31"
    assert rows[-1]["trade_time"].strftime("%H:%M") == "15:00"
    assert all(row["amount"] is None for row in rows)
