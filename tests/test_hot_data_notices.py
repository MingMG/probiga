from unittest.mock import patch

from server.api.routers import hot_data
from server.api.routers.hot_data import stock_notices


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


def test_stock_notices_filters_future_dates_by_default():
    with patch("server.api.routers.hot_data._read_sql", return_value=[]) as read_sql:
        result = stock_notices(stock_code="", include_future=False)

    assert result["include_future"] is False
    sql = read_sql.call_args.args[0]
    assert "notice_date <= CURDATE()" in sql


def test_stock_notices_can_include_future_dates_explicitly():
    with patch("server.api.routers.hot_data._read_sql", return_value=[]) as read_sql:
        result = stock_notices(stock_code="", include_future=True)

    assert result["include_future"] is True
    sql = read_sql.call_args.args[0]
    assert "notice_date <= CURDATE()" not in sql


def test_hot_data_news_fetchers_use_explicit_request_timeout():
    cases = [
        (hot_data._fetch_cls_news, {"data": {"roll_data": []}}),
        (hot_data._fetch_eastmoney_news, {"data": {"fastNewsList": []}}),
        (hot_data._fetch_sina_news, {"result": {"data": {"feed": {"list": []}}}}),
    ]

    for fetcher, payload in cases:
        client = _FakeNewsClient(payload)
        assert fetcher(client, pages=1) == []
        assert client.calls
        assert client.calls[0][1]["timeout"] == hot_data.NEWS_REQUEST_TIMEOUT_SECONDS
