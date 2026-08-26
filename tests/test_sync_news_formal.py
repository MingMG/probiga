from __future__ import annotations

import json
from datetime import datetime

import pytest

from tools import sync_news_formal as news


NOW = datetime(2026, 8, 26, 17, 30)


def _item(source: str, source_id: str, minute: int = 0):
    return {
        "source": source,
        "source_id": source_id,
        "title": f"title-{source_id}",
        "content": f"content-{source_id}",
        "publish_time": datetime(2026, 8, 26, 17, minute),
        "level": "B",
        "stocks": [{"code": "600000", "name": "浦发银行"}],
        "subjects": [{"name": "银行"}],
        "reading_num": 10,
        "is_top": True,
        "jpush": False,
    }


class _Connection:
    def __init__(self):
        self.executions = []

    def execute(self, statement, params=None):
        self.executions.append((str(statement), params))
        return object()


class _Begin:
    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        return self.engine.connection

    def __exit__(self, exc_type, exc, _traceback):
        self.engine.committed = exc_type is None
        self.engine.rolled_back = exc_type is not None
        return False


class _Engine:
    def __init__(self):
        self.connection = _Connection()
        self.committed = False
        self.rolled_back = False

    def begin(self):
        return _Begin(self)


def _as_db_rows(items):
    rows = []
    for item in items:
        row = dict(item)
        row["publish_time"] = datetime.fromisoformat(item["publish_time"])
        row["stocks"] = json.dumps(item["stocks"], ensure_ascii=False, sort_keys=True)
        row["subjects"] = json.dumps(item["subjects"], ensure_ascii=False, sort_keys=True)
        row["is_top"] = 1 if item["is_top"] else 0
        row["jpush"] = 1 if item["jpush"] else 0
        rows.append(row)
    return rows


def _receipt_hash(receipt):
    payload = dict(receipt)
    payload.pop("receipt_id")
    return news._hash_payload(payload)


def test_formal_news_reports_each_source_and_verifies_db_readback(monkeypatch):
    engine = _Engine()
    monkeypatch.setattr(
        news,
        "_readback_batch",
        lambda _connection, items: _as_db_rows(items),
    )
    fetchers = {
        "cls": lambda _client, pages: [_item("cls", "c1", 1), _item("cls", "c2", 2)],
        "eastmoney": lambda _client, pages: [],
        "sina": lambda _client, pages: (_ for _ in ()).throw(RuntimeError("blocked")),
    }

    receipt = news.sync_news_formal(
        engine,
        pages=2,
        client=object(),
        fetchers=fetchers,
        now=NOW,
    )

    assert receipt["status"] == "PASS"
    assert receipt["attempted_sources"] == ["cls", "eastmoney", "sina"]
    assert receipt["successful_sources"] == ["cls", "eastmoney"]
    assert receipt["nonempty_sources"] == ["cls"]
    assert receipt["empty_sources"] == ["eastmoney"]
    assert receipt["failed_sources"] == ["sina"]
    assert receipt["source_results"]["cls"]["fetched_count"] == 2
    assert receipt["source_results"]["eastmoney"]["status"] == "SUCCESS"
    assert receipt["source_results"]["eastmoney"]["outcome"] == "EMPTY"
    assert receipt["source_results"]["sina"]["status"] == "FAILED"
    assert receipt["evidence"]["persisted_count"] == 2
    assert receipt["evidence"]["latest_publish_time"] == "2026-08-26T17:02:00"
    assert len(receipt["evidence"]["row_hash"]) == 64
    assert receipt["delivery_attempted"] is False
    assert receipt["receipt_id"] == _receipt_hash(receipt)
    assert engine.committed is True
    assert len(engine.connection.executions) == 1


@pytest.mark.parametrize("mode", ["failed", "empty"])
def test_formal_news_all_failed_or_empty_is_non_success(mode):
    if mode == "failed":
        fetchers = {
            source: (lambda _client, _pages: (_ for _ in ()).throw(RuntimeError("down")))
            for source in news.SOURCE_FETCHERS
        }
    else:
        fetchers = {source: (lambda _client, _pages: []) for source in news.SOURCE_FETCHERS}

    expected_message = "all formal news sources" if mode == "failed" else "formal news result is empty"
    with pytest.raises(news.NewsSyncContractError, match=expected_message) as caught:
        news.sync_news_formal(
            object(),
            pages=2,
            client=object(),
            fetchers=fetchers,
            now=NOW,
        )

    expected = "FAILED" if mode == "failed" else "SUCCESS"
    assert {
        result["status"] for result in caught.value.source_results.values()
    } == {expected}
    if mode == "empty":
        assert {
            result["outcome"] for result in caught.value.source_results.values()
        } == {"EMPTY"}


def test_invalid_rows_fail_only_their_source_when_another_source_is_valid(monkeypatch):
    engine = _Engine()
    monkeypatch.setattr(news, "_readback_batch", lambda _connection, items: _as_db_rows(items))
    bad = _item("eastmoney", "wrong-owner")
    fetchers = {
        "cls": lambda _client, _pages: [bad],
        "eastmoney": lambda _client, _pages: [_item("eastmoney", "e1")],
        "sina": lambda _client, _pages: [],
    }

    receipt = news.sync_news_formal(
        engine,
        client=object(),
        fetchers=fetchers,
        now=NOW,
    )

    assert receipt["failed_sources"] == ["cls"]
    assert receipt["successful_sources"] == ["eastmoney", "sina"]
    assert receipt["nonempty_sources"] == ["eastmoney"]
    assert receipt["evidence"]["persisted_count"] == 1


def test_readback_mismatch_raises_inside_transaction_and_rolls_back(monkeypatch):
    engine = _Engine()
    monkeypatch.setattr(news, "_readback_batch", lambda _connection, _items: [])

    with pytest.raises(news.NewsSyncContractError, match="readback differs"):
        news.persist_and_verify(
            engine,
            [_item("cls", "c1")],
            etl_sync_at=NOW,
        )

    assert engine.committed is False
    assert engine.rolled_back is True


def test_formal_news_cli_failure_is_one_hashed_receipt_and_nonzero(monkeypatch, capsys):
    source_results = {
        source: {
            "status": "FAILED",
            "requested_pages": 1,
            "fetched_count": 0,
            "error": "down",
        }
        for source in news.SOURCE_FETCHERS
    }
    monkeypatch.setattr(news, "create_batch_engine", lambda **_kwargs: object())
    monkeypatch.setattr(
        news,
        "sync_news_formal",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            news.NewsSyncContractError("all down", source_results=source_results)
        ),
    )

    exit_code = news.main(["--pages", "2", "--json"])

    lines = capsys.readouterr().out.strip().splitlines()
    assert exit_code == 1
    assert len(lines) == 1
    receipt = json.loads(lines[0])
    assert receipt["status"] == "FAILED"
    assert receipt["failed_sources"] == ["cls", "eastmoney", "sina"]
    assert receipt["delivery_attempted"] is False
    assert receipt["receipt_id"] == _receipt_hash(receipt)


def test_formal_news_cli_success_is_one_hashed_receipt(monkeypatch, capsys):
    engine = object()
    started = NOW
    success = news._receipt(
        status="PASS",
        started_at=started,
        finished_at=started,
        source_results={
            "cls": {
                "status": "SUCCESS",
                "outcome": "NONEMPTY",
                "requested_pages": 2,
                "fetched_count": 1,
            },
            "eastmoney": {
                "status": "SUCCESS",
                "outcome": "EMPTY",
                "requested_pages": 1,
                "fetched_count": 0,
            },
            "sina": {
                "status": "FAILED",
                "requested_pages": 1,
                "fetched_count": 0,
            },
        },
        pages=2,
        evidence={
            "persisted_count": 1,
            "latest_publish_time": "2026-08-26T17:00:00",
            "row_hash": "b" * 64,
        },
    )
    monkeypatch.setattr(news, "create_batch_engine", lambda **_kwargs: engine)
    monkeypatch.setattr(
        news,
        "sync_news_formal",
        lambda observed_engine, **_kwargs: success if observed_engine is engine else None,
    )

    exit_code = news.main(["--pages", "2", "--json"])

    lines = capsys.readouterr().out.strip().splitlines()
    assert exit_code == 0
    assert len(lines) == 1
    receipt = json.loads(lines[0])
    assert receipt["status"] == "PASS"
    assert receipt["receipt_id"] == _receipt_hash(receipt)
