from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import hashlib
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from integrations.bigqmt import bridge as bigqmt_bridge
from tools import run_etf_forward_daily as daily
from tools import run_etf_forward_simulation as forward
from tools import sync_etf_bigqmt_daily as etf


BUILD_SHA = "a" * 40


def _proof() -> dict[str, object]:
    return {
        "strategy_release_protocol": "release-v2",
        "strategy_identity_protocol": "identity-v1",
        "strategy_identity_frozen": True,
        "strategy_build_sha": BUILD_SHA,
        "strategy_git_blob": "b" * 40,
        "strategy_source_sha256": "c" * 64,
        "strategy_artifact_sha256": "d" * 64,
        "strategy_loaded_identity_sha256": "e" * 64,
    }


def _capture(*, omit: str = "", duplicate: str = "") -> dict[str, object]:
    proof = _proof()
    rows: list[dict[str, object]] = []
    for index, code in enumerate(etf.ETF_CODES):
        if code == omit:
            continue
        price = 10 + index / 10
        row = {
            "qmt_code": etf.etf_qmt_symbol(code),
            "stock_code": code,
            "trade_time": "2026-08-26 15:00:00",
            "trade_date": "2026-08-26",
            "open": price,
            "close": price + 0.1,
            "high": price + 0.2,
            "low": price - 0.1,
            "volume": 1000 + index,
            "amount": 100000 + index,
            "pre_close": price,
            "pre_close_origin": "NATIVE_QMT",
        }
        rows.append(row)
        if code == duplicate:
            rows.append(dict(row))
    return {
        "status": "ok",
        "action": "kline",
        "source": etf.PROVIDER_ID,
        "bridge_version": etf.BRIDGE_VERSION,
        "request_id": "request-1",
        "rows": rows,
        **proof,
    }


def _normalized_rows() -> list[dict[str, object]]:
    now = datetime(2026, 8, 26, 15, 20)
    rows: list[dict[str, object]] = []
    for dividend_type, adjust_type in etf.ADJUSTMENTS:
        capture = _capture()
        capture["request_id"] = f"request-{dividend_type}"
        rows.extend(
            etf._normalized_source_rows(
                capture,
                release_proof=_proof(),
                trade_date="2026-08-26",
                dividend_type=dividend_type,
                adjust_type=adjust_type,
                batch_id="etf_20260826_test",
                observed_at=now,
            )
        )
    return rows


def _sqlite_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    converted: list[dict[str, object]] = []
    for source in rows:
        row = dict(source)
        for key, value in tuple(row.items()):
            if isinstance(value, Decimal):
                row[key] = float(value)
        converted.append(row)
    return converted


def test_etf_hash_accepts_declines_without_relaxing_price_checks():
    row = _normalized_rows()[0]
    row.update(close=Decimal("9.9"), low=Decimal("9.8"),
               change=Decimal("-0.1"), change_pct=Decimal("-1"))
    result = etf._canonical_partition_row(row)
    assert result["change"] == "-0.100000"
    assert result["change_pct"] == "-1.00000000"
    for field, value in [("open", Decimal("-1")), ("change", Decimal("NaN")),
                         ("change_pct", Decimal("Infinity"))]:
        with pytest.raises(RuntimeError):
            etf._canonical_partition_row({**row, field: value})


def _etf_sqlite_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE sm_etf_kline (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                etf_code TEXT NOT NULL,
                short_name TEXT NOT NULL,
                trade_time DATETIME NOT NULL,
                trade_date DATE NOT NULL,
                k_type INTEGER NOT NULL,
                adjust_type INTEGER NOT NULL,
                `open` NUMERIC NOT NULL,
                `close` NUMERIC NOT NULL,
                high NUMERIC NOT NULL,
                low NUMERIC NOT NULL,
                volume NUMERIC NOT NULL,
                amount NUMERIC,
                pre_close NUMERIC,
                `change` NUMERIC,
                change_pct NUMERIC,
                data_source TEXT NOT NULL,
                validation_source TEXT NOT NULL,
                validation_status TEXT NOT NULL,
                validation_price_max_delta NUMERIC,
                validation_volume_delta_pct NUMERIC,
                validation_checked_at DATETIME NOT NULL,
                received_at DATETIME NOT NULL,
                batch_id TEXT NOT NULL,
                data_version TEXT NOT NULL,
                quality_status TEXT NOT NULL,
                permission_status TEXT NOT NULL,
                UNIQUE(etf_code,trade_date,k_type,adjust_type)
            )
            """
        )
    return engine


def test_frozen_etf_universe_is_exactly_the_reviewed_fourteen() -> None:
    assert len(etf.ETF_CODES) == 14
    assert len(set(etf.ETF_CODES)) == 14
    expected = hashlib.sha256("\n".join(etf.ETF_CODES).encode("ascii")).hexdigest()
    assert etf.code_set_hash(etf.ETF_CODES) == expected
    assert "512890" in etf.ETF_CODES


def test_kline_capture_preserves_response_identity(monkeypatch) -> None:
    response = {"status": "ok", "request_id": "r1", "rows": [{"stock_code": "510300"}]}
    observed: dict[str, object] = {}

    def fake_call(action: str, **kwargs):
        observed.update({"action": action, **kwargs})
        return response

    monkeypatch.setattr(bigqmt_bridge, "_call", fake_call)
    capture = bigqmt_bridge.kline_capture(
        ["510300"],
        start_date="2026-08-26",
        end_date="2026-08-26",
        dividend_type="front",
    )
    frame = bigqmt_bridge.kline(
        ["510300"],
        start_date="2026-08-26",
        end_date="2026-08-26",
        dividend_type="front",
    )
    assert capture is response
    assert frame.to_dict("records") == response["rows"]
    assert observed["action"] == "kline"
    assert observed["dividend_type"] == "front"


def test_etf_capture_requires_exact_set_date_native_preclose_and_identity() -> None:
    rows = etf._normalized_source_rows(
        _capture(),
        release_proof=_proof(),
        trade_date="2026-08-26",
        dividend_type="none",
        adjust_type=0,
        batch_id="batch",
        observed_at=datetime(2026, 8, 26, 15, 20),
    )
    assert [row["etf_code"] for row in rows] == list(etf.ETF_CODES)
    assert {row["data_source"] for row in rows} == {etf.PROVIDER_ID}
    assert all(row["volume"] >= Decimal(100_000) for row in rows)

    with pytest.raises(RuntimeError, match="code set differs"):
        etf._normalized_source_rows(
            _capture(omit=etf.ETF_CODES[0]),
            release_proof=_proof(),
            trade_date="2026-08-26",
            dividend_type="none",
            adjust_type=0,
            batch_id="batch",
            observed_at=datetime(2026, 8, 26, 15, 20),
        )
    bad_identity = _capture()
    bad_identity["strategy_build_sha"] = "f" * 40
    with pytest.raises(RuntimeError, match="release identity differs"):
        etf._normalized_source_rows(
            bad_identity,
            release_proof=_proof(),
            trade_date="2026-08-26",
            dividend_type="none",
            adjust_type=0,
            batch_id="batch",
            observed_at=datetime(2026, 8, 26, 15, 20),
        )
    bad_preclose = _capture()
    bad_preclose["rows"][0]["pre_close_origin"] = "MISSING_NATIVE_QMT"
    with pytest.raises(RuntimeError, match="native pre_close"):
        etf._normalized_source_rows(
            bad_preclose,
            release_proof=_proof(),
            trade_date="2026-08-26",
            dividend_type="none",
            adjust_type=0,
            batch_id="batch",
            observed_at=datetime(2026, 8, 26, 15, 20),
        )


def test_etf_two_adjustment_groups_replace_and_read_back_atomically() -> None:
    engine = _etf_sqlite_engine()
    rows = _sqlite_rows(_normalized_rows())
    proof = etf.replace_daily_partition(
        engine, trade_date="2026-08-26", rows=rows
    )
    assert proof["row_count"] == 28
    assert set(proof["group_hashes"]) == {"none", "front"}
    with engine.connect() as connection:
        groups = connection.execute(
            text(
                "SELECT adjust_type,COUNT(*) FROM sm_etf_kline "
                "GROUP BY adjust_type ORDER BY adjust_type"
            )
        ).all()
    assert groups == [(0, 14), (1, 14)]


def test_etf_insert_failure_rolls_back_partition_delete() -> None:
    engine = _etf_sqlite_engine()
    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            INSERT INTO sm_etf_kline
              (etf_code,short_name,trade_time,trade_date,k_type,adjust_type,
               `open`,`close`,high,low,volume,amount,pre_close,`change`,change_pct,
               data_source,validation_source,validation_status,
               validation_price_max_delta,validation_volume_delta_pct,
               validation_checked_at,received_at,batch_id,data_version,
               quality_status,permission_status)
            VALUES
              ('999999','old','2026-08-26 15:00:00','2026-08-26',1,0,
               1,1,1,1,1,1,1,0,0,'old','old','passed',0,0,
               '2026-08-26 15:20:00','2026-08-26 15:20:00','old',
               'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff',
               'validated','SUPPORTED')
            """
        )
        connection.exec_driver_sql(
            """
            CREATE TRIGGER fail_etf_insert BEFORE INSERT ON sm_etf_kline
            WHEN NEW.etf_code='510300'
            BEGIN SELECT RAISE(ABORT, 'forced insert failure'); END
            """
        )
    with pytest.raises(Exception, match="forced insert failure"):
        etf.replace_daily_partition(
            engine,
            trade_date="2026-08-26",
            rows=_sqlite_rows(_normalized_rows()),
        )
    with engine.connect() as connection:
        rows = connection.execute(
            text("SELECT etf_code,data_source FROM sm_etf_kline")
        ).all()
    assert rows == [("999999", "old")]


def test_forward_observation_rejects_any_historical_latest_close() -> None:
    with pytest.raises(RuntimeError, match="latest validated close must equal today"):
        forward.validate_observation_date(
            data_date=date(2026, 8, 25),
            forward_start_date=date(2026, 7, 27),
            registered_at=datetime(2026, 7, 27, 15, 20),
            local_today=date(2026, 8, 26),
        )


def test_forward_rerun_uses_only_observations_before_current_date(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_read(engine, sql, params, **kwargs):
        calls.append((sql, params))
        return []

    monkeypatch.setattr(forward, "read_sql_rows", fake_read)
    target = date(2026, 8, 26)
    assert forward._latest_observation(
        object(), "strategy-v1", before_date=target
    ) is None
    assert forward._latest_monthly_signal(
        object(), "strategy-v1", before_date=target
    ) is None
    assert len(calls) == 2
    assert all("data_date<:before_date" in sql for sql, _params in calls)
    assert all(params["before_date"] == target for _sql, params in calls)


def test_historical_etf_repair_never_invokes_forward_ledger(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        daily,
        "run_sync",
        lambda *args, **kwargs: {
            "receipt_id": "market",
            "status": "PASS",
            "trade_date": "2026-08-25",
            "database": {"row_count": 28},
        },
    )
    monkeypatch.setattr(
        daily,
        "run_forward",
        lambda *args, **kwargs: calls.append("forward"),
    )
    receipt = daily.run_daily(
        object(),
        trade_date="2026-08-25",
        expected_build_sha=BUILD_SHA,
        now=datetime(2026, 8, 26, 16, 0),
    )
    assert receipt["status"] == "PASS"
    assert receipt["forward_ledger"]["status"] == (
        "NOT_RUN_HISTORICAL_BACKFILL_PROHIBITED"
    )
    assert calls == []


@pytest.mark.parametrize(
    ("now", "expected"),
    (
        (datetime(2026, 8, 27, 2, 0), "2026-08-26"),
        (datetime(2026, 8, 27, 15, 9, 59), "2026-08-26"),
        (datetime(2026, 8, 27, 15, 10), "2026-08-27"),
        (datetime(2026, 8, 29, 12, 0), "2026-08-28"),
        (datetime(2026, 8, 28, 16, 0), "2026-08-28"),
    ),
)
def test_etf_implicit_target_is_latest_calendar_session_closed_by_1510(
    now: datetime,
    expected: str,
) -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "CREATE TABLE si_trade_calendar "
            "(trade_date DATE PRIMARY KEY, trade_status INTEGER NOT NULL)"
        )
        connection.exec_driver_sql(
            "INSERT INTO si_trade_calendar VALUES "
            "('2026-08-26',1),('2026-08-27',1),('2026-08-28',1)"
        )

    assert daily.resolve_target_trade_date(engine, now=now) == expected


def test_etf_current_session_source_not_ready_never_runs_forward(
    monkeypatch,
) -> None:
    forward_calls: list[str] = []
    monkeypatch.setattr(
        daily,
        "run_sync",
        lambda *args, **kwargs: {
            "status": "DATA_BLOCKED",
            "trade_date": "2026-08-27",
        },
    )
    monkeypatch.setattr(
        daily,
        "run_forward",
        lambda *args, **kwargs: forward_calls.append("forward"),
    )

    with pytest.raises(RuntimeError, match="market-data phase did not pass"):
        daily.run_daily(
            object(),
            trade_date="2026-08-27",
            expected_build_sha=BUILD_SHA,
            now=datetime(2026, 8, 27, 15, 10),
        )
    assert forward_calls == []


def test_etf_runtime_scripts_contain_no_runtime_ddl() -> None:
    for relative in (
        "tools/sync_etf_bigqmt_daily.py",
        "tools/run_etf_forward_daily.py",
        "tools/run_etf_forward_simulation.py",
    ):
        source = Path(relative).read_text(encoding="utf-8").upper()
        assert "CREATE TABLE" not in source
        assert "ALTER TABLE" not in source
        assert "DROP TABLE" not in source
