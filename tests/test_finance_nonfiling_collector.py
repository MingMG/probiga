from datetime import date, datetime
import json

import pytest

from biz.stock_finance import sync_finance as sync
from server.common import finance_nonfiling_evidence as body


class Response:
    def __init__(self, raw):
        self.raw = raw
        self.closed = False

    def raise_for_status(self):
        return None

    def iter_content(self, *, chunk_size):
        yield self.raw

    def close(self):
        self.closed = True


def row(announcement_id, day, title="关于继续停牌的风险提示公告"):
    published = datetime(2026, 9, day, tzinfo=sync.ZoneInfo("Asia/Shanghai"))
    return {"secCode": "002731", "orgId": "9900022974",
            "announcementId": str(announcement_id), "announcementTitle": title,
            "announcementTime": int(published.timestamp() * 1000),
            "adjunctUrl": f"finalpage/2026-09-{day:02}/{announcement_id}.PDF"}


@pytest.fixture
def capture(monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 9, 12, 14, 0, tzinfo=tz)

    monkeypatch.setattr(sync, "datetime", Clock)
    # Network/parser boundary uses text fixtures; the PDF decoder itself has
    # dedicated malformed/size/encryption tests and a real official-PDF probe.
    monkeypatch.setattr(body, "extract_document_text", lambda raw: raw.decode())
    return monkeypatch


def install_network(monkeypatch, pages, documents):
    seen = []
    responses = []

    def post(url, **kwargs):
        assert url == sync.CNINFO_ANNOUNCEMENT_ENDPOINT
        assert kwargs["stream"] and kwargs["allow_redirects"] is False
        assert kwargs["timeout"] == (10, 30)
        page = int(kwargs["data"]["pageNum"])
        result = Response(json.dumps(pages[page - 1]).encode())
        responses.append(result)
        return result

    def get(url, **kwargs):
        assert url.startswith(sync.CNINFO_STATIC_ROOT + "finalpage/")
        assert kwargs["stream"] and kwargs["allow_redirects"] is False
        seen.append(url.rsplit("/", 1)[-1].removesuffix(".PDF"))
        result = Response(documents[seen[-1]].encode())
        responses.append(result)
        return result

    monkeypatch.setattr(sync.requests, "post", post)
    monkeypatch.setattr(sync.requests, "get", get)
    return seen, responses


H1 = "证券代码：002731。因公司未在法定期限内（2026年8月31日）披露2026年半年度报告，股票继续停牌。"
ANNUAL = "证券代码：002731。因公司未在法定期限内披露2025年年度报告，股票继续停牌。"


def test_body_finds_current_period_behind_unrelated_title_and_wrong_period(capture):
    seen, responses = install_network(capture, [{
        "totalRecordNum": 2, "hasMore": False,
        "announcements": [row("1225554537", 9), row("1225549792", 7)],
    }], {"1225554537": ANNUAL, "1225549792": H1})
    evidence = sync.fetch_cninfo_nonfiling_evidence(
        "002731", as_of=date(2026, 9, 11), expected_report_date=date(2026, 6, 30),
    )
    assert evidence["announcement_id"] == "1225549792"
    assert evidence["valid_until"] == "2026-09-14"
    assert evidence["next_retry_date"] == "2026-09-13"
    assert evidence["catalog_identity"]["window_end"] == "2026-09-12"
    assert evidence["catalog_identity"]["complete"] is True
    assert seen == ["1225554537", "1225549792"]
    assert all(response.closed for response in responses)
    body.validate_document_body_evidence(evidence, "002731", "2026-06-30")


@pytest.mark.parametrize("case", ["truncated", "duplicate", "wrong_issuer", "document_date", "missing_more"])
def test_catalogue_must_be_complete_and_identity_bound(capture, case):
    item = row("1", 7)
    payload = {"totalRecordNum": 1, "hasMore": False, "announcements": [item]}
    if case == "truncated": payload["totalRecordNum"] = 2
    elif case == "duplicate": payload.update(totalRecordNum=2, announcements=[item, item])
    elif case == "wrong_issuer": item["orgId"] = "other"
    elif case == "document_date": item["adjunctUrl"] = "finalpage/2026-09-08/1.PDF"
    else: payload.pop("hasMore")
    seen, _ = install_network(capture, [payload], {"1": H1})
    with pytest.raises(RuntimeError, match="DATA_BLOCKED"):
        sync.fetch_cninfo_nonfiling_evidence("002731", as_of=date(2026, 9, 11), expected_report_date=date(2026, 6, 30))
    assert seen == []


def test_actual_report_on_later_page_invalidates_older_absence(capture):
    first = [row(str(index), 7) for index in range(30)]
    install_network(capture, [
        {"totalRecordNum": 31, "hasMore": True, "announcements": first},
        {"totalRecordNum": 31, "hasMore": False, "announcements": [row("31", 12, "2026年半年度报告")]},
    ], {})
    with pytest.raises(RuntimeError, match="required report exists"):
        sync.fetch_cninfo_nonfiling_evidence("002731", as_of=date(2026, 9, 11), expected_report_date=date(2026, 6, 30))


def test_only_wrong_period_body_does_not_create_absence(capture):
    install_network(capture, [{"totalRecordNum": 1, "hasMore": False, "announcements": [row("1", 9)]}], {"1": ANNUAL})
    with pytest.raises(RuntimeError, match="no official PDF proves"):
        sync.fetch_cninfo_nonfiling_evidence("002731", as_of=date(2026, 9, 11), expected_report_date=date(2026, 6, 30))


def test_response_bound_closes_stream_on_failure():
    response = Response(b"x" * 20)
    with pytest.raises(RuntimeError, match="exceeds bound"):
        sync._cninfo_response_bytes(response, limit=19)
    assert response.closed
