from hashlib import sha256
import json
import subprocess
import sys
import time

import pytest

from server.common import official_listing_evidence as source


def disclosure(code="001381", day="2026年11月3日"):
    return ("首次公开发行股票并在主板上市之上市公告书提示性公告\n"
            f"经深圳证券交易所审核同意，本公司发行的人民币普通股股票将于 {day} 在深圳证券交易所主板上市。\n"
            f"股票代码：{code}")


def evidence():
    text = disclosure()
    return dict(schema=source.SCHEMA, stock_code="001381", listing_date="2026-11-03",
                source_url="https://static.cninfo.com.cn/finalpage/2026-11-02/1225646928.PDF",
                source_text=text, source_text_sha256=sha256(text.encode()).hexdigest(),
                official_document_sha256="a" * 64, publication_date="2026-11-02")


def test_live_failure_prelisting_date_survives_pdf_whitespace():
    assert source.listing_date_from_text(disclosure(day="2026 年 11 月 3\n日"), stock_code="001381") == "2026-11-03"
    assert source.listing_date_from_text(disclosure()+"\n3、首次公开发行后总股本：468,980,000股", stock_code="001381") == "2026-11-03"
    assert source.validate_official_listing_evidence(evidence(), stock_code="001381", target_date="2026-11-02") == evidence()


@pytest.mark.parametrize("text", [
    disclosure("001382"),
    "首次公开发行上市公告书 股票代码：001381。公告日2026年11月3日。尚未确定上市日期。",
    disclosure().replace("股票将于", "股票预计将于"),
    disclosure() + "上市时间：2026年11月4日",
])
def test_only_identity_bound_confirmed_listing_statement_proves_date(text):
    with pytest.raises(ValueError):
        source.listing_date_from_text(text, stock_code="001381")


@pytest.mark.parametrize("field,value", [
    ("listing_date", "2026-11-04"),
    ("source_url", "https://static.cninfo.com.cn.attacker.test/finalpage/2026-11-02/1225646928.PDF"),
    ("source_url", "https://static.cninfo.com.cn/finalpage/2026-11-03/1225646928.PDF"),
    ("source_text_sha256", "b" * 64),
    ("official_document_sha256", ""),
])
def test_historical_replay_rejects_tampering_and_future_disclosures(field, value):
    item = evidence()
    item[field] = value
    with pytest.raises(ValueError):
        source.validate_official_listing_evidence(item, stock_code="001381", target_date="2026-11-02")


def test_listing_day_is_not_exempt():
    with pytest.raises(ValueError):
        source.validate_official_listing_evidence(evidence(), stock_code="001381", target_date="2026-11-03")


def test_official_inventory_plus_document_produces_verified_evidence(monkeypatch):
    catalog = {"stockList": [{"code":"001381", "category":"A股", "orgId":"9900063630"}]}
    item = {"secCode":"001381", "orgId":"9900063630", "announcementTitle":"首次公开发行股票并在主板上市之上市公告书提示性公告",
            "announcementTime":1793548800000, "adjunctUrl":"finalpage/2026-11-02/1225646928.PDF"}
    def fetch(session, method, url, **kwargs):
        if url.endswith("sz_stock.json"):
            return json.dumps(catalog).encode()
        if url.endswith("query"):
            return json.dumps({"announcements":[item], "hasMore":False}).encode()
        return b"%PDF-fixture"
    monkeypatch.setattr(source, "_fetch", fetch)
    monkeypatch.setattr(source, "extract_document_text", lambda raw: disclosure())
    result = source._load_in_process(["001381"], target_date="2026-11-02")
    assert result["001381"]["listing_date"] == "2026-11-03"
    item["orgId"] = "other_issuer"
    assert source._load_in_process(["001381"], target_date="2026-11-02") == {}


def test_unknown_issuer_and_unbounded_requests_are_terminal(monkeypatch):
    monkeypatch.setattr(source, "_fetch", lambda *a, **k: b'{"stockList": []}')
    with pytest.raises(source.OfficialListingEvidenceError, match="issuer_directory"):
        source._load_in_process(["001381"], target_date="2026-11-02")
    with pytest.raises(ValueError):
        source.load_official_listing_evidence(["001381"], target_date="2026-11-02", deadline_seconds=0)


def test_shanghai_uses_bulk_disclosures_without_nonexistent_static_directory(monkeypatch):
    body = disclosure("600001").replace("深圳", "上海")
    def fetch(session, method, url, **kwargs):
        assert not url.endswith("_stock.json")
        if url.endswith("query"):
            return json.dumps({"hasMore":False,"announcements":[{
                "secCode":"600001", "orgId":"990000001", "announcementTime":1793548800000,
                "announcementTitle":"首次公开发行股票并在主板上市之上市公告书提示性公告",
                "adjunctUrl":"finalpage/2026-11-02/1225646928.PDF"}]}).encode()
        return b"%PDF-fixture"
    monkeypatch.setattr(source, "_fetch", fetch)
    monkeypatch.setattr(source, "extract_document_text", lambda raw: body)
    result = source._load_in_process(["600001"], target_date="2026-11-02")
    assert result["600001"]["listing_date"] == "2026-11-03"


def test_streamed_response_size_and_total_deadline_are_bounded(monkeypatch):
    class Response:
        status_code = 200
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def raise_for_status(self): pass
        def iter_content(self, *args): yield b"oversized"
    class Session:
        def request(self, *args, **kwargs): return Response()
    with pytest.raises(source.OfficialListingEvidenceError, match="response_size_limit"):
        source._fetch(Session(), "GET", "https://www.cninfo.com.cn/", deadline=source.time.monotonic()+5,
                      stage="official_document", cap=1)
    with pytest.raises(source.OfficialListingEvidenceError, match="total_deadline_exceeded"):
        source._fetch(Session(), "GET", "https://www.cninfo.com.cn/", deadline=0, stage="official_document", cap=1)


def test_worker_is_terminated_when_network_or_parser_never_returns(monkeypatch):
    run = subprocess.run
    def stuck_worker(command, **kwargs):
        return run([sys.executable, "-c", "import time; time.sleep(30)"], **kwargs)
    monkeypatch.setattr(source.subprocess, "run", stuck_worker)
    started = time.monotonic()
    with pytest.raises(source.OfficialListingEvidenceError, match="total_deadline_exceeded"):
        source.load_official_listing_evidence(["001381"], target_date="2026-11-02", deadline_seconds=0.2)
    assert time.monotonic()-started < 3
