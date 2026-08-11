from __future__ import annotations

import json
import gzip
import os
import os

import pandas as pd

from integrations.bigqmt.qmt_strategy import probiga_big_qmt_bridge as strategy


def test_standard_qmt_strategy_retries_transient_replace_lock(tmp_path, monkeypatch):
    temporary = tmp_path / "tracked.tmp"
    target = tmp_path / "tracked.json"
    temporary.write_text("new", encoding="ascii")
    target.write_text("old", encoding="ascii")
    real_replace = os.replace
    attempts = 0

    def flaky_replace(source, destination):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise PermissionError(13, "sharing violation", str(destination))
        return real_replace(source, destination)

    monkeypatch.setattr(strategy.os, "replace", flaky_replace)

    strategy._replace_with_retry(str(temporary), str(target), retry_seconds=0.2, retry_interval=0.01)

    assert target.read_text(encoding="ascii") == "new"
    assert attempts == 2


def test_standard_qmt_strategy_retries_transient_replace_lock(tmp_path, monkeypatch):
    temporary = tmp_path / "tracked.tmp"
    target = tmp_path / "tracked.json"
    temporary.write_text("new", encoding="ascii")
    target.write_text("old", encoding="ascii")
    real_replace = os.replace
    attempts = 0

    def flaky_replace(source, destination):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise PermissionError(13, "sharing violation", str(destination))
        return real_replace(source, destination)

    monkeypatch.setattr(strategy.os, "replace", flaky_replace)

    strategy._replace_with_retry(str(temporary), str(target), retry_seconds=0.2, retry_interval=0.01)

    assert target.read_text(encoding="ascii") == "new"
    assert attempts == 2


class FakeContext:
    def __init__(self):
        self.callback = None
        self.timer = None
        self.unsubscribed = []

    def subscribe_whole_quote(self, codes, callback=None):
        self.callback = callback
        self.subscribed = list(codes)
        return 7

    def unsubscribe_quote(self, subscription_id):
        self.unsubscribed.append(subscription_id)

    def get_full_tick(self, codes):
        return {
            code: {
                "time": 1784602800000,
                "lastPrice": 10.5,
                "lastClose": 10.0,
                "volume": 100,
                "amount": 1000,
            }
            for code in codes
        }

    def run_time(self, function_name, period, start_time):
        self.timer = (function_name, period, start_time)

    def get_stock_list_in_sector(self, sector_name, *_args):
        return {
            "上证A股": ["600000.SH"],
            "沪深300": ["600000.SH", "000001.SZ"],
            "人工智能": ["000001.SZ"],
        }.get(sector_name, [])

    def get_instrument_detail(self, code, *_args):
        names = {
            "000300.SH": "沪深300",
            "600000.SH": "浦发银行",
            "000001.SZ": "平安银行",
        }
        return {
            "InstrumentName": names.get(code, code),
            "ExchangeID": code.split(".")[-1],
            "OpenDate": "19910101",
        }

    def get_market_data_ex(self, _fields, codes, **kwargs):
        period = kwargs["period"]
        index = ["20260721"] if period == "1d" else ["20260721093100"]
        return {
            code: pd.DataFrame(
                [{
                    "open": 10.0,
                    "high": 10.8,
                    "low": 9.9,
                    "close": 10.5,
                    "preClose": 10.0,
                    "volume": 100,
                    "amount": 1000,
                }],
                index=index,
            )
            for code in codes
        }

    def get_market_data_ex_ori(self, _fields, codes, **kwargs):
        stime = "20260721" if kwargs["period"] == "1d" else "20260721093100"
        return {
            code: [{
                "stime": stime,
                "open": 10.0,
                "high": 10.8,
                "low": 9.9,
                "close": 10.5,
                "preClose": 10.0,
                "volume": 100,
                "amount": 1000,
            }]
            for code in codes
        }


def test_standard_qmt_strategy_exports_without_trading_calls(tmp_path, monkeypatch):
    config = {
        "all_codes": ["000001.SZ", "600000.SH"],
        "tracked_codes": ["000001.SZ"],
        "full_refresh_seconds": 30,
        "tracked_flush_seconds": 1,
        "full_batch_size": 800,
    }
    (tmp_path / "watchlist.json").write_text(json.dumps(config), encoding="ascii")
    monkeypatch.setattr(strategy, "_find_bridge_root", lambda: str(tmp_path))
    monkeypatch.setattr(strategy, "_config_mtime", None)
    monkeypatch.setattr(strategy, "_last_full_refresh", 0.0)
    monkeypatch.setattr(strategy, "_last_tracked_flush", 0.0)
    monkeypatch.setattr(strategy, "_subscription_id", None)
    monkeypatch.setattr(strategy, "_tracked_quotes", {})

    context = FakeContext()
    strategy.init(context)
    strategy.after_init(context)

    assert context.subscribed == ["000001.SZ"]
    assert context.timer == ("bridge_tick", "5nSecond", "2000-01-01 00:00:00")
    full = json.loads((tmp_path / "full_quotes.json").read_text(encoding="ascii"))
    tracked = json.loads((tmp_path / "tracked_quotes.json").read_text(encoding="ascii"))
    assert full["source"] == "gj_big_qmt_inner"
    assert set(full["quotes"]) == {"000001.SZ", "600000.SH"}
    assert tracked["quotes"] == {}
    context.callback(context.get_full_tick(["000001.SZ"]))
    strategy._write_tracked_snapshot(force=True)
    tracked = json.loads((tmp_path / "tracked_quotes.json").read_text(encoding="ascii"))
    assert tracked["quotes"]["000001.SZ"]["_probiga_received_at"]
    assert tracked["last_callback_ts"] > 0
    assert tracked["callback_batch_count"] >= 1
    assert not any(hasattr(context, name) for name in ("passorder", "order", "cancel_order"))

    strategy.stop(context)
    assert context.unsubscribed == [7]


def test_standard_qmt_strategy_processes_reference_and_kline_requests(tmp_path, monkeypatch):
    (tmp_path / "watchlist.json").write_text(
        json.dumps({"all_codes": [], "tracked_codes": []}), encoding="ascii"
    )
    monkeypatch.setattr(strategy, "_find_bridge_root", lambda: str(tmp_path))
    monkeypatch.setattr(strategy, "_config_mtime", None)
    monkeypatch.setattr(strategy, "_last_full_refresh", 0.0)
    monkeypatch.setattr(strategy, "_last_tracked_flush", 0.0)
    monkeypatch.setattr(strategy, "_subscription_id", None)
    monkeypatch.setattr(strategy, "_tracked_quotes", {})
    monkeypatch.setattr(
        strategy,
        "get_sector_list",
        lambda node: (["人工智能"], []) if node == "概念" else ([], ["概念"]) if node == "" else ([], []),
        raising=False,
    )
    monkeypatch.setattr(strategy, "download_history_data", lambda *_args, **_kwargs: None, raising=False)

    context = FakeContext()
    strategy.init(context)

    requests = tmp_path / "requests"
    responses = tmp_path / "responses"
    payloads = {
        "sector": {"request_id": "sector", "action": "sector_list", "params": {}},
        "members": {
            "request_id": "members",
            "action": "index_members_many",
            "params": {"index_codes": ["000300.SH"]},
        },
        "kline": {
            "request_id": "kline",
            "action": "kline",
            "params": {
                "stock_codes": ["000001.SZ"],
                "start_date": "2026-07-21",
                "end_date": "2026-07-21",
                "download_history": True,
            },
        },
    }
    results = {}
    for request_id, payload in payloads.items():
        (requests / f"{request_id}.json").write_text(json.dumps(payload), encoding="utf-8")
        assert strategy._process_one_request(context) is True
        with gzip.open(responses / f"{request_id}.json.gz", "rt", encoding="utf-8") as handle:
            results[request_id] = json.load(handle)

    assert results["sector"]["rows"] == [
        {"sector_name": "人工智能", "parent_name": "概念", "parent_path": "概念"}
    ]
    assert {row["stock_code"] for row in results["members"]["rows"]} == {"600000", "000001"}
    assert results["kline"]["rows"][0]["trade_date"] == "2026-07-21"
    assert results["kline"]["rows"][0]["close"] == 10.5


def test_standard_qmt_strategy_prefers_batch_history_download(monkeypatch):
    calls = []

    monkeypatch.setattr(
        strategy,
        "download_history_data2",
        lambda **kwargs: calls.append(kwargs),
        raising=False,
    )
    monkeypatch.setattr(
        strategy,
        "download_history_data",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("single-symbol downloader must not be used")
        ),
        raising=False,
    )

    strategy._download_history(
        ["000001.SZ", "600000.SH"],
        "1m",
        "20260727000000",
        "20260727235959",
    )

    assert calls == [{
        "stock_list": ["000001.SZ", "600000.SH"],
        "period": "1m",
        "start_time": "20260727000000",
        "end_time": "20260727235959",
    }]


def _bar_payload(codes, period="1d"):
    stime = "20260810" if period == "1d" else "20260810100100"
    return {
        code: [{
            "stime": stime,
            "open": 10.0,
            "high": 10.8,
            "low": 9.9,
            "close": 10.5,
            "preClose": 10.0,
            "volume": 100,
            "amount": 1000,
        }]
        for code in codes
    }


def test_market_rows_batches_download_reader_and_busy_heartbeat(monkeypatch):
    symbols = ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ", "000005.SZ"]
    download_batches = []
    reader_batches = []
    heartbeats = []

    class Context:
        def get_market_data_ex_ori(self, _fields, codes, **kwargs):
            reader_batches.append(list(codes))
            return _bar_payload(codes, kwargs["period"])

    monkeypatch.setattr(
        strategy,
        "_download_history",
        lambda codes, *_args: download_batches.append(list(codes)),
    )
    monkeypatch.setattr(strategy, "_write_heartbeat", heartbeats.append)

    rows = strategy._market_rows(Context(), {
        "stock_codes": symbols,
        "start_date": "2026-08-10",
        "end_date": "2026-08-10",
        "download_history": True,
        "batch_size": 2,
    }, "1d")

    expected_batches = [symbols[:2], symbols[2:4], symbols[4:]]
    assert download_batches == expected_batches
    assert reader_batches == expected_batches
    assert heartbeats == ["busy"] * 6
    assert [row["qmt_code"] for row in rows] == symbols


def test_market_rows_batches_get_market_data_ex_fallback(monkeypatch):
    symbols = ["%06d.SZ" % number for number in range(1, 202)]
    reader_batches = []
    heartbeats = []

    class Context:
        get_market_data_ex_ori = None

        def get_market_data_ex(self, _fields, codes, **kwargs):
            reader_batches.append(list(codes))
            return _bar_payload(codes, kwargs["period"])

    monkeypatch.setattr(strategy, "_write_heartbeat", heartbeats.append)

    rows = strategy._market_rows(Context(), {
        "stock_codes": symbols,
        "start_date": "2026-08-10",
        "end_date": "2026-08-10",
    }, "1d")

    assert reader_batches == [symbols[:200], symbols[200:]]
    assert heartbeats == ["busy", "busy"]
    assert [row["qmt_code"] for row in rows] == symbols


def test_current_rows_refreshes_busy_heartbeat_between_batches(monkeypatch):
    symbols = ["%06d.SZ" % number for number in range(1, 42)]
    batches = []
    heartbeats = []

    class Context:
        def get_full_tick(self, codes):
            batches.append(list(codes))
            return {
                code: {
                    "time": 1786344000000,
                    "lastPrice": 10.5,
                    "lastClose": 10.0,
                }
                for code in codes
            }

    monkeypatch.setattr(strategy, "_write_heartbeat", heartbeats.append)

    rows = strategy._current_rows(Context(), {
        "stock_codes": symbols,
        "batch_size": 20,
    })

    assert [len(batch) for batch in batches] == [20, 20, 1]
    assert heartbeats == ["busy", "busy", "busy"]
    assert len(rows) == len(symbols)


def test_full_snapshot_refreshes_busy_heartbeat_between_batches(monkeypatch):
    symbols = ["%06d.SZ" % number for number in range(1, 102)]
    batches = []
    heartbeats = []
    writes = []

    class Context:
        def get_full_tick(self, codes):
            batches.append(list(codes))
            return dict((code, {"lastPrice": 10.5}) for code in codes)

    monkeypatch.setattr(strategy, "_all_codes", symbols)
    monkeypatch.setattr(strategy, "_config", {
        "full_batch_size": 50,
        "full_refresh_seconds": 5,
    })
    monkeypatch.setattr(strategy, "_last_full_refresh", 0.0)
    monkeypatch.setattr(strategy, "_write_heartbeat", heartbeats.append)
    monkeypatch.setattr(strategy, "_atomic_write", lambda name, payload: writes.append((name, payload)))

    strategy._refresh_full_snapshot(Context())

    assert [len(batch) for batch in batches] == [50, 50, 1]
    assert heartbeats == ["busy", "busy", "busy"]
    assert writes[0][0] == "full_quotes.json"
    assert writes[0][1]["quote_count"] == len(symbols)
