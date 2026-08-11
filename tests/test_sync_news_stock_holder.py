# -*- coding: utf-8 -*-
import sys
import types
from unittest.mock import patch

import pandas as pd

from biz.news import sync_news
from biz.stock_info import sync_stock_holder


class _FakeNewsResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeNewsClient:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return _FakeNewsResponse(self.payload)


class _PagedNewsClient:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return _FakeNewsResponse(self.payloads.pop(0))


def _fake_stock_info_modules() -> dict[str, types.ModuleType]:
    adata = types.ModuleType("adata")
    adata.__path__ = []
    stock = types.ModuleType("adata.stock")
    stock.__path__ = []
    info = types.ModuleType("adata.stock.info")
    info.__path__ = []
    stock_info = types.ModuleType("adata.stock.info.stock_info")

    class StockInfo:
        pass

    stock_info.StockInfo = StockInfo
    return {
        "adata": adata,
        "adata.stock": stock,
        "adata.stock.info": info,
        "adata.stock.info.stock_info": stock_info,
    }


def test_sync_news_get_engine_uses_batch_engine():
    engine = object()

    with patch("biz.news.sync_news.create_batch_engine", return_value=engine) as create_batch_engine:
        assert sync_news.get_engine() is engine

    create_batch_engine.assert_called_once_with(pool_size=2, max_overflow=2)


def test_sync_news_fetchers_use_explicit_request_timeout():
    cases = [
        (sync_news.fetch_cls, {"data": {"roll_data": []}}),
        (sync_news.fetch_eastmoney, {"data": {"fastNewsList": []}}),
        (sync_news.fetch_sina, {"result": {"data": {"feed": {"list": []}}}}),
    ]

    for fetcher, payload in cases:
        client = _FakeNewsClient(payload)
        assert fetcher(client, pages=1) == []
        assert client.calls
        assert client.calls[0][1]["timeout"] == sync_news.NEWS_REQUEST_TIMEOUT_SECONDS


def test_fetch_cls_uses_strict_cursor_and_deduplicates_across_pages():
    first = {
        "errno": 0,
        "data": {"roll_data": [
            {"id": 101, "ctime": 100, "content": "newest"},
            {"id": 100, "ctime": 90, "content": "boundary"},
        ]},
    }
    second = {
        "errno": 0,
        "data": {"roll_data": [
            {"id": 100, "ctime": 90, "content": "boundary duplicate"},
            {"id": 99, "ctime": 80, "content": "older"},
        ]},
    }
    client = _PagedNewsClient([first, second])

    with patch("biz.news.sync_news.time.time", return_value=110):
        items = sync_news.fetch_cls(client, pages=2)

    assert [item["source_id"] for item in items] == ["101", "100", "99"]
    assert len(client.calls) == 2
    assert client.calls[0][1]["params"] == {
        "rn": 10,
        "lastTime": 110,
        "name": "telegraph",
    }
    assert client.calls[1][1]["params"]["lastTime"] == 89


def test_fetch_cls_stops_when_page_has_no_usable_cursor():
    page = {
        "errno": 0,
        "data": {"roll_data": [{"id": 101, "content": "missing timestamp"}]},
    }
    client = _PagedNewsClient([page])

    assert len(sync_news.fetch_cls(client, pages=3)) == 1
    assert len(client.calls) == 1


def test_sync_stock_holder_engine_uses_batch_engine():
    engine = object()

    with patch("biz.stock_info.sync_stock_holder.create_batch_engine", return_value=engine) as create_batch_engine:
        assert sync_stock_holder._engine() is engine

    create_batch_engine.assert_called_once_with(pool_size=3, max_overflow=5)


def test_sync_stock_holder_main_reads_codes_with_batch_reader():
    engine = object()
    codes = pd.DataFrame({"stock_code": ["1", "600000"]})

    with patch.dict(sys.modules, _fake_stock_info_modules()), patch.object(
        sync_stock_holder.sys,
        "argv",
        ["sync_stock_holder.py", "--limit", "1", "--sleep", "0"],
    ), patch("biz.stock_info.sync_stock_holder._engine", return_value=engine), patch(
        "biz.stock_info.sync_stock_holder.run_ddl",
    ) as run_ddl, patch(
        "biz.stock_info.sync_stock_holder.read_frame",
        return_value=codes,
    ) as read_frame, patch(
        "biz.stock_info.sync_stock_holder.sync_one",
        return_value=1,
    ) as sync_one:
        assert sync_stock_holder.main() == 0

    run_ddl.assert_called_once_with(engine)
    read_frame.assert_called_once()
    assert read_frame.call_args.args[1] is engine
    assert "si_all_code" in str(read_frame.call_args.args[0])
    sync_one.assert_called_once()
    assert sync_one.call_args.args[0] is engine
    assert sync_one.call_args.args[2] == "000001"
