from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urlsplit, parse_qs

import pytest

from biz.news.sync_news import fetch_cls


class Client:
    def __init__(self, payloads):
        self.payloads = iter(payloads)
        self.urls = []

    def get(self, url):
        self.urls.append(url)
        payload = next(self.payloads)

        class Response:
            def raise_for_status(self):
                pass

            def json(self):
                return payload

        return Response()


def feed(*times):
    return {"errno": 0, "data": {"roll_data": [
        {"id": ts, "ctime": ts, "content": "<b>公开电报</b>", "level": "B"}
        for ts in times
    ]}}


def test_cls_uses_current_official_endpoint_and_native_cursor():
    client = Client([feed(1789174078, 1789174000), feed(1789173900)])
    items = fetch_cls(client, 2)
    assert len(items) == 3
    assert "/api/cache?" in client.urls[0]
    assert "lastTime" not in client.urls[0]
    assert "/v1/roll/get_roll_list?" in client.urls[1]
    assert "last_time=1789174000" in client.urls[1]
    assert parse_qs(urlsplit(client.urls[1]).query)["refresh_type"] == ["1"]
    assert len(parse_qs(urlsplit(client.urls[1]).query)["sign"][0]) == 32
    assert all("nodeapi" not in url for url in client.urls)
    assert items[0]["publish_time"] == datetime.fromtimestamp(1789174078, ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)
    assert items[0]["source"] == "cls"
    assert items[0]["content"] == "公开电报"


@pytest.mark.parametrize("payload", [{"errno": 1001, "data": {}}, {"errno": 0, "data": {}}, {"errno": 0, "data": {"roll_data": [{}]}}])
def test_cls_rejects_api_errors_and_unproven_empty_shape(payload):
    with pytest.raises(ValueError):
        fetch_cls(Client([payload]), 1)


def test_cls_rejects_repeating_pagination():
    with pytest.raises(ValueError, match="did not advance"):
        fetch_cls(Client([feed(1789174078), feed(1789174078)]), 2)
