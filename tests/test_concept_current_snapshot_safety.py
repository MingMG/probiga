from __future__ import annotations

import pytest

from tools import crawl_concept_east_current, crawl_concept_ths_current


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_eastmoney_interrupted_pagination_fails_closed(monkeypatch):
    class _Session:
        def get(self, _url, *, params, timeout):
            del timeout
            if int(params["pn"]) == 1:
                return _Response(
                    {"data": {"total": 3, "diff": [{"f12": "C1"}, {"f12": "C2"}]}}
                )
            raise ConnectionError("injected page-two outage")

    monkeypatch.setattr(crawl_concept_east_current, "PAGE_SIZE", 2)
    monkeypatch.setattr(crawl_concept_east_current.time, "sleep", lambda _seconds: None)

    with pytest.raises(RuntimeError, match="page 2 failed"):
        crawl_concept_east_current.fetch_all(_Session())


def test_eastmoney_complete_pages_carry_publishable_evidence(monkeypatch):
    class _Session:
        def get(self, _url, *, params, timeout):
            del timeout
            page = int(params["pn"])
            diff = (
                [{"f12": "C1"}, {"f12": "C2"}]
                if page == 1
                else [{"f12": "C3"}]
            )
            return _Response({"data": {"total": 3, "diff": diff}})

    captured = {}
    monkeypatch.setattr(crawl_concept_east_current, "PAGE_SIZE", 2)
    monkeypatch.setattr(crawl_concept_east_current.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        crawl_concept_east_current,
        "replace_table_rows",
        lambda frame, table, engine, **kwargs: captured.update(
            frame=frame, table=table, engine=engine, kwargs=kwargs
        ),
    )

    frame = crawl_concept_east_current.fetch_all(_Session())
    crawl_concept_east_current.save_to_db(object(), frame)

    assert frame.attrs["snapshot_evidence"]["pages_fetched"] == 2
    assert frame.attrs["snapshot_evidence"]["code_count"] == 3
    assert captured["table"] == "sm_concept_east_current"
    assert "where_sql" not in captured["kwargs"]


def test_ths_limit_or_item_failure_updates_only_successful_codes(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        crawl_concept_ths_current,
        "replace_table_rows",
        lambda frame, table, engine, **kwargs: captured.update(
            frame=frame, table=table, engine=engine, kwargs=kwargs
        ),
    )

    mode = crawl_concept_ths_current.save_to_db(
        object(),
        [{"index_code": "C1", "trade_date": "2026-08-25", "price": 1.0}],
        expected_codes=["C1", "C2", "C3"],
        attempted_codes=["C1", "C2"],
        failed_codes=["C2"],
    )

    assert mode == "partitions"
    assert captured["kwargs"]["where_sql"] == "index_code IN (:index_code_0)"
    assert captured["kwargs"]["params"] == {"index_code_0": "C1"}


def test_ths_mixed_trade_dates_are_never_published(monkeypatch):
    monkeypatch.setattr(
        crawl_concept_ths_current,
        "replace_table_rows",
        lambda *_args, **_kwargs: pytest.fail("mixed dates must not touch the table"),
    )

    with pytest.raises(ValueError, match="mixed or missing trade dates"):
        crawl_concept_ths_current.save_to_db(
            object(),
            [
                {"index_code": "C1", "trade_date": "2026-08-25"},
                {"index_code": "C2", "trade_date": "2026-08-24"},
            ],
            expected_codes=["C1", "C2"],
            attempted_codes=["C1", "C2"],
            failed_codes=[],
        )
