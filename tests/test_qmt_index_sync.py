from __future__ import annotations

import pandas as pd

from biz.stock_market import sync_stock_market


def _daily_rows(symbols: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "stock_code": symbol.split(".", 1)[0],
                "trade_time": "2026-07-17 15:00:00",
                "trade_date": "2026-07-17",
                "open": 100,
                "close": 101,
                "high": 102,
                "low": 99,
                "volume": 1000,
                "amount": 101000,
                "change": 1,
                "change_pct": 1,
            }
            for symbol in symbols
        ]
    )


def test_qmt_index_kline_is_batched_and_never_truncates(monkeypatch):
    calls: list[list[str]] = []
    written: list[pd.DataFrame] = []
    write_engines: list[object] = []
    history_engine = object()

    monkeypatch.setattr(sync_stock_market, "_index_source", lambda _kind: "qmt")
    monkeypatch.setattr(sync_stock_market, "get_kline_engine", lambda: history_engine)
    monkeypatch.setenv("QMT_PRODUCTION_INDEX_KLINE_BATCH_SIZE", "5")
    monkeypatch.setenv("QMT_INDEX_KLINE_MIN_COVERAGE", "1")

    def fake_kline(symbols, **_kwargs):
        calls.append(list(symbols))
        return _daily_rows(list(symbols))

    monkeypatch.setattr("integrations.qmt.bridge.kline", fake_kline)
    monkeypatch.setattr(
        sync_stock_market,
        "_replace_qmt_index_window",
        lambda target_engine, frame, **_kwargs: (
            write_engines.append(target_engine) or written.append(frame.copy()) or len(frame)
        ),
    )
    monkeypatch.setattr(
        sync_stock_market,
        "truncate_only",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not truncate")),
    )

    sync_stock_market.step_index_kline(
        object(),
        ["000001", "399001", "000300", "000905", "000852", "000016", "000688", "399006", "000906", "000985", "000986"],
        "2026-07-17",
        "2026-07-17",
    )

    assert [len(batch) for batch in calls] == [5, 5, 1]
    assert sum(len(frame) for frame in written) == 11
    assert write_engines == [history_engine, history_engine, history_engine]


def test_tencent_index_kline_derives_change_from_provider_previous_close(monkeypatch):
    captured: dict[str, object] = {}

    class Response:
        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            return {
                "code": 0,
                "data": {
                    "sh000001": {
                        "day": [
                            ["2026-07-22", "99", "100", "101", "98", "1000"],
                            ["2026-07-23", "100", "102", "103", "99", "1200"],
                        ]
                    }
                },
            }

    def fake_get(_url, **kwargs):
        captured.update(kwargs)
        return Response()

    monkeypatch.setattr(sync_stock_market.requests, "get", fake_get)
    frame = sync_stock_market._fetch_tencent_index_kline(
        "000001",
        "2026-07-23",
        "2026-07-23",
    )

    assert len(frame) == 1
    row = frame.iloc[0]
    assert row["index_code"] == "000001"
    assert row["change"] == 2
    assert row["change_pct"] == 2
    assert row["volume"] == 1200
    assert pd.isna(row["amount"])
    assert captured["params"]["param"].startswith("sh000001,day,2026-07-09,")


def test_tencent_index_kline_retries_transient_source_failure(monkeypatch):
    attempts = 0

    class Response:
        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            return {
                "code": 0,
                "data": {
                    "sh000001": {
                        "day": [
                            ["2026-07-22", "99", "100", "101", "98", "1000"],
                            ["2026-07-23", "100", "102", "103", "99", "1200"],
                        ]
                    }
                },
            }

    def fake_get(*_args, **_kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise sync_stock_market.requests.Timeout("temporary")
        return Response()

    monkeypatch.setattr(sync_stock_market.requests, "get", fake_get)
    monkeypatch.setattr(sync_stock_market.time, "sleep", lambda _seconds: None)

    frame = sync_stock_market._fetch_tencent_index_kline(
        "000001",
        "2026-07-23",
        "2026-07-23",
    )

    assert attempts == 2
    assert len(frame) == 1


def test_shenzhen_information_index_codes_use_shenzhen_provider_symbols():
    assert sync_stock_market._tencent_index_symbol("399001") == "sz399001"
    assert sync_stock_market._tencent_index_symbol("970070") == "sz970070"
    assert sync_stock_market._tencent_index_symbol("980016") == "sz980016"
    assert sync_stock_market._tencent_index_symbol("000001") == "sh000001"
    assert sync_stock_market._sina_index_symbol("970070") == "sz970070"
    assert sync_stock_market._sina_index_symbol("980016") == "sz980016"


def test_cnindex_kline_parses_official_units_and_percent(monkeypatch):
    class Response:
        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            return {
                "code": 200,
                "data": {
                    "item": [
                        "timestamp",
                        "current",
                        "high",
                        "open",
                        "low",
                        "close",
                        "chg",
                        "percent",
                        "amount",
                        "volume",
                        "avg",
                    ],
                    "data": [
                        [
                            "2026-07-23",
                            5986.3237,
                            6043.4106,
                            5987.9082,
                            5912.9503,
                            5986.3237,
                            -40.4505,
                            "-0.67%",
                            456.51,
                            1017.36,
                            None,
                        ]
                    ],
                },
            }

    class Session:
        trust_env = True

        def __init__(self):
            self.headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        @staticmethod
        def get(_url, **_kwargs):
            return Response()

    monkeypatch.setattr(sync_stock_market.requests, "Session", Session)
    frame = sync_stock_market._fetch_cnindex_index_kline(
        "980016",
        "2026-07-23",
        "2026-07-23",
    )

    assert len(frame) == 1
    row = frame.iloc[0]
    assert row["index_code"] == "980016"
    assert row["change_pct"] == -0.67
    assert row["volume"] == 1_017_360_000
    assert row["amount"] == 45_651_000_000


def test_verified_external_index_kline_prefers_official_cnindex(monkeypatch):
    official = pd.DataFrame(
        [
            {
                "index_code": "980016",
                "trade_time": pd.Timestamp("2026-07-23 15:00:00"),
                "trade_date": pd.Timestamp("2026-07-23"),
                "k_type": 1,
                "open": 100,
                "close": 101,
                "high": 102,
                "low": 99,
                "volume": 1000,
                "amount": 10000,
                "change": 1,
                "change_pct": 1,
            }
        ]
    )
    monkeypatch.setattr(
        sync_stock_market,
        "_fetch_cnindex_index_kline",
        lambda *_args: official,
    )
    monkeypatch.setattr(
        sync_stock_market,
        "_fetch_tencent_index_kline",
        lambda *_args: (_ for _ in ()).throw(AssertionError("official source must be primary")),
    )

    frame, status = sync_stock_market._fetch_verified_external_index_kline(
        "980016",
        "2026-07-23",
        "2026-07-23",
    )

    assert status == "cnindex_official"
    assert frame.equals(official)


def test_cnindex_current_parses_official_realtime_units(monkeypatch):
    class Response:
        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            return {
                "code": 200,
                "data": {
                    "item": [
                        "timestamp",
                        "current",
                        "high",
                        "open",
                        "low",
                        "close",
                        "chg",
                        "percent",
                        "amount",
                        "volume",
                        "avg",
                    ],
                    "data": [
                        [
                            1784863800000,
                            7176.656847034532,
                            7331.104163886321,
                            7225.444518087403,
                            7113.940785558325,
                            0.0,
                            -212.0850529654681,
                            -0.028703811262573414,
                            "70129246344.02",
                            "1036166379.00",
                            None,
                        ]
                    ],
                },
            }

    class Session:
        trust_env = True

        def __init__(self):
            self.headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        @staticmethod
        def get(_url, **_kwargs):
            return Response()

    monkeypatch.setattr(sync_stock_market.requests, "Session", Session)
    frame = sync_stock_market._fetch_cnindex_index_current(
        "970070",
        pd.Timestamp("2026-07-24 12:00:00"),
    )

    assert len(frame) == 1
    row = frame.iloc[0]
    assert row["trade_time"] == pd.Timestamp("2026-07-24 11:30:00")
    assert row["trade_date"] == "2026-07-24"
    assert row["volume"] == 1_036_166_379
    assert row["amount"] == 70_129_246_344.02
    assert round(row["change_pct"], 6) == -2.870381


def test_index_current_supplements_missing_cnindex_rows(monkeypatch):
    current = pd.DataFrame(
        [
            {
                "index_code": "399001",
                "trade_time": pd.Timestamp("2026-07-23 15:00:00"),
                "trade_date": "2026-07-23",
                "open": 100,
                "price": 101,
                "high": 102,
                "low": 99,
                "volume": 1000,
                "amount": 10000,
                "change": 1,
                "change_pct": 1,
                "snapshot_at": pd.Timestamp("2026-07-24 12:00:00"),
            }
        ]
    )
    official = pd.DataFrame(
        [
            {
                "index_code": "980016",
                "trade_time": pd.Timestamp("2026-07-23 15:00:00"),
                "trade_date": "2026-07-23",
                "open": 5987.9082,
                "price": 5986.3237,
                "high": 6043.4106,
                "low": 5912.9503,
                "volume": 1_017_360_000,
                "amount": 45_651_000_000,
                "change": -40.4505,
                "change_pct": -0.67,
                "snapshot_at": pd.Timestamp("2026-07-24 12:00:00"),
            }
        ]
    )
    monkeypatch.setattr(
        sync_stock_market,
        "_fetch_cnindex_index_current",
        lambda code, snapshot_at: (
            official
            if (code, snapshot_at)
            == ("980016", pd.Timestamp("2026-07-24 12:00:00"))
            else pd.DataFrame()
        ),
    )

    result = sync_stock_market._supplement_cnindex_index_current(
        current,
        ["399001", "980016"],
        pd.Timestamp("2026-07-24 12:00:00"),
    )

    assert set(result["index_code"]) == {"399001", "980016"}
    supplemented = result[result["index_code"] == "980016"].iloc[0]
    assert supplemented["price"] == 5986.3237
    assert supplemented["amount"] == 45_651_000_000


def test_tencent_index_kline_uses_incremental_window_and_atomic_replace(monkeypatch):
    history_engine = object()
    captured: dict[str, object] = {}
    monkeypatch.setattr(sync_stock_market, "_index_source", lambda _kind: "tencent")
    monkeypatch.setattr(sync_stock_market, "get_kline_engine", lambda: history_engine)
    monkeypatch.setattr(sync_stock_market, "_latest_index_kline_date", lambda _engine: "2026-07-22")
    monkeypatch.setenv("TENCENT_INDEX_KLINE_MIN_COVERAGE", "1")

    def fake_fetch(code, start, end):
        assert start == "2026-07-22"
        assert end == "2026-07-23"
        return pd.DataFrame(
            [
                {
                    "index_code": code,
                    "trade_time": "2026-07-23 15:00:00",
                    "trade_date": "2026-07-23",
                    "k_type": 1,
                    "open": 100,
                    "close": 101,
                    "high": 102,
                    "low": 99,
                    "volume": 1000,
                    "amount": None,
                    "change": 1,
                    "change_pct": 1,
                }
            ]
        )

    def fake_replace(frame, table_name, engine, **kwargs):
        captured["frame"] = frame
        captured["table_name"] = table_name
        captured["engine"] = engine
        captured["kwargs"] = kwargs
        return len(frame)

    monkeypatch.setattr(
        sync_stock_market,
        "_fetch_verified_external_index_kline",
        lambda code, start, end: (fake_fetch(code, start, end), "cross_checked"),
    )
    monkeypatch.setattr(sync_stock_market, "replace_table_rows", fake_replace)
    monkeypatch.setattr(
        sync_stock_market,
        "truncate_only",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not truncate")),
    )

    sync_stock_market.step_index_kline(
        object(),
        ["000001", "399001"],
        kline_end="2026-07-23",
    )

    assert len(captured["frame"]) == 2
    assert captured["table_name"] == "sm_index_kline"
    assert captured["engine"] is history_engine
    assert captured["kwargs"]["params"] == {
        "start_date": "2026-07-22",
        "end_date": "2026-07-23",
    }


def test_tencent_index_kline_defaults_to_completed_stock_daily_watermark(monkeypatch):
    history_engine = object()
    captured: dict[str, object] = {}
    monkeypatch.setattr(sync_stock_market, "_index_source", lambda _kind: "tencent")
    monkeypatch.setattr(sync_stock_market, "get_kline_engine", lambda: history_engine)
    monkeypatch.setattr(sync_stock_market, "_latest_index_kline_date", lambda _engine: "2026-07-23")
    monkeypatch.setattr(
        sync_stock_market,
        "_latest_completed_stock_kline_date",
        lambda _engine: "2026-07-23",
    )
    monkeypatch.setenv("TENCENT_INDEX_KLINE_MIN_COVERAGE", "1")

    def fake_fetch(code, start, end):
        captured["range"] = (start, end)
        return (
            pd.DataFrame(
                [
                    {
                        "index_code": code,
                        "trade_time": "2026-07-23 15:00:00",
                        "trade_date": "2026-07-23",
                        "k_type": 1,
                        "open": 100,
                        "close": 101,
                        "high": 102,
                        "low": 99,
                        "volume": 1000,
                        "amount": 10000,
                        "change": 1,
                        "change_pct": 1,
                    }
                ]
            ),
            "cross_checked",
        )

    monkeypatch.setattr(sync_stock_market, "_fetch_verified_external_index_kline", fake_fetch)
    monkeypatch.setattr(sync_stock_market, "replace_table_rows", lambda *_args, **_kwargs: 1)

    sync_stock_market.step_index_kline(object(), ["000001"])

    assert captured["range"] == ("2026-07-23", "2026-07-23")


def test_verified_external_index_kline_keeps_cross_checked_amount(monkeypatch):
    common = {
        "index_code": "000001",
        "trade_time": pd.Timestamp("2026-07-23 15:00:00"),
        "trade_date": pd.Timestamp("2026-07-23"),
        "k_type": 1,
        "open": 100.0,
        "close": 102.0,
        "high": 103.0,
        "low": 99.0,
        "volume": 1200.0,
        "change": 2.0,
        "change_pct": 2.0,
    }
    tencent = pd.DataFrame([{**common, "amount": None}])
    east = pd.DataFrame([{**common, "amount": 123456.0}])
    monkeypatch.setattr(
        sync_stock_market,
        "_fetch_tencent_index_kline",
        lambda *_args: tencent,
    )
    monkeypatch.setattr(
        sync_stock_market,
        "_fetch_east_index_kline",
        lambda *_args: east,
    )

    frame, status = sync_stock_market._fetch_verified_external_index_kline(
        "000001",
        "2026-07-23",
        "2026-07-23",
    )

    assert status == "cross_checked"
    assert frame.iloc[0]["amount"] == 123456


def test_qmt_index_minute_is_batched_and_never_truncates(monkeypatch):
    calls: list[list[str]] = []

    monkeypatch.setattr(sync_stock_market, "_index_source", lambda _kind: "qmt")
    monkeypatch.setattr(sync_stock_market, "get_kline_engine", lambda: object())
    monkeypatch.setattr(sync_stock_market, "_default_myquant_minute_date", lambda _engine: "2026-07-17")
    monkeypatch.setattr(sync_stock_market, "_is_complete_index_universe", lambda *_args: False)
    monkeypatch.setenv("QMT_PRODUCTION_INDEX_MINUTE_BATCH_SIZE", "5")
    monkeypatch.setenv("QMT_INDEX_MINUTE_MIN_COVERAGE", "1")

    def fake_minute(symbols, **_kwargs):
        calls.append(list(symbols))
        return pd.DataFrame(
            [
                {
                    "stock_code": symbol.split(".", 1)[0],
                    "trade_time": "2026-07-17 09:31:00",
                    "trade_date": "2026-07-17",
                    "price": 100,
                    "change": 0,
                    "change_pct": 0,
                    "volume": 100,
                    "amount": 10000,
                }
                for symbol in symbols
            ]
        )

    monkeypatch.setattr("integrations.qmt.bridge.minute", fake_minute)
    monkeypatch.setattr(
        sync_stock_market,
        "_replace_qmt_index_window",
        lambda _engine, frame, **_kwargs: len(frame),
    )
    monkeypatch.setattr(
        sync_stock_market,
        "truncate_only",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not truncate")),
    )

    sync_stock_market.step_index_minute(
        object(),
        ["000001", "399001", "000300", "000905", "000852", "000016"],
    )

    assert [len(batch) for batch in calls] == [5, 1]


def test_index_current_snapshot_is_validated_and_atomically_replaced(monkeypatch):
    captured: dict[str, object] = {}
    frame = pd.DataFrame(
        [
            {"index_code": "000001", "price": 3000, "trade_date": "2026-07-17"},
            {"index_code": "399001", "price": 10000, "trade_date": "2026-07-17"},
            # Duplicate and unrelated rows must not leak into the canonical snapshot.
            {"index_code": "399001", "price": 10001, "trade_date": "2026-07-17"},
            {"index_code": "999999", "price": 1, "trade_date": "2026-07-17"},
        ]
    )
    monkeypatch.setenv("QMT_INDEX_CURRENT_MIN_COVERAGE", "1")

    def fake_replace(replacement, table_name, _engine, **kwargs):
        captured["frame"] = replacement.copy()
        captured["table"] = table_name
        captured["kwargs"] = kwargs
        return len(replacement)

    monkeypatch.setattr(sync_stock_market, "replace_table_rows", fake_replace)
    monkeypatch.setattr(
        sync_stock_market,
        "upsert_current_frame",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must replace, not upsert")),
    )

    written = sync_stock_market._replace_index_current_snapshot(
        object(),
        frame,
        ["000001", "399001"],
        source="QMT",
    )

    assert written == 2
    assert captured["table"] == "sm_index_current"
    assert set(captured["frame"]["index_code"]) == {"000001", "399001"}
    assert "where_sql" not in captured["kwargs"]


def test_index_current_snapshot_rejects_low_coverage_before_delete(monkeypatch):
    monkeypatch.setenv("QMT_INDEX_CURRENT_MIN_COVERAGE", "1")
    monkeypatch.setattr(
        sync_stock_market,
        "replace_table_rows",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must preserve old snapshot")),
    )

    frame = pd.DataFrame([{"index_code": "000001", "price": 3000}])
    try:
        sync_stock_market._replace_index_current_snapshot(
            object(), frame, ["000001", "399001"], source="QMT"
        )
    except RuntimeError as exc:
        assert "coverage below threshold" in str(exc)
    else:
        raise AssertionError("low coverage must fail")
