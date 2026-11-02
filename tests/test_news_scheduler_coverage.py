"""Final scheduler acceptance replays Fed source evidence and actual DB rows."""
import base64
from copy import deepcopy
from datetime import datetime, timedelta
import hashlib
import json

import pytest
from sqlalchemy import create_engine, text

from server.common import federal_reserve_news_coverage as fed
from server.common import scheduler_validation as validation
from tools import sync_news_formal as news


NOW = datetime(2026, 11, 3, 8)


@pytest.fixture(autouse=True)
def _isolate_sources(monkeypatch, tmp_path):
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))


def _document(url, body):
    raw = body.encode("utf-8")
    return {"url": url, "http_status": 200,
            "request_started_at": NOW.isoformat(),
            "captured_at": (NOW + timedelta(seconds=1)).isoformat(),
            "source_response_sha256": hashlib.sha256(raw).hexdigest(),
            "body_base64": base64.b64encode(raw).decode("ascii")}


def _fed_snapshot(nonempty):
    def archive(url, title, releases):
        rows = "".join('<div class="row"><time>' + day + '</time>'
                       '<p class="eventlist__press">Monetary Policy</p>'
                       '<a href="/newsevents/pressreleases/monetary' + identity +
                       '.htm">Official policy announcement</a></div>'
                       for day, identity in releases)
        return _document(url, '<html><body><h1>' + title + '</h1>' + rows +
                         '<footer>Last Update: November 3, 2026</footer></body></html>')

    urls = fed.archive_urls(NOW)
    documents = [archive(urls[0], "2026 Press Releases",
                        [("11/02/2026", "20261102a")] if nonempty
                        else [("10/15/2026", "20261015a")]),
                 archive(urls[1], "2026 FOMC Press Releases", [("10/28/2026", "20261028a")])]
    items = ('<item><title>FOMC statement</title><link>https://www.federalreserve.gov/'
             'newsevents/pressreleases/monetary20261028a.htm</link>'
             '<description>Original statement</description>'
             '<pubDate>Wed, 28 Oct 2026 14:00:00 -0400</pubDate></item>')
    if nonempty:
        items += ('<item><title>Board policy announcement</title><link>https://www.federalreserve.gov/'
                  'newsevents/pressreleases/monetary20261102a.htm</link>'
                  '<description>Original exact publication content</description>'
                  '<pubDate>Mon, 02 Nov 2026 14:00:00 -0500</pubDate></item>')
    documents.append(_document(fed.RSS_URL, '<rss><channel>' + items + '</channel></rss>'))
    return fed.seal_federal_reserve_news_coverage(
        documents, observed_at=NOW, captured_at=NOW + timedelta(seconds=1))


def _case(*, nonempty=False):
    fed_rows, proof = _fed_snapshot(nonempty)
    rows = news.canonical_news_items([
        {"source": source, "source_id": source + "-1", "title": "Current news",
         "content": "Complete source content", "publish_time": NOW - timedelta(minutes=10)}
        for source in ("cls", "eastmoney", "sina")
    ] + fed_rows)
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE st_news_flash (
            source TEXT, source_id TEXT, title TEXT, content TEXT, publish_time DATETIME,
            level TEXT, stocks TEXT, subjects TEXT, reading_num INTEGER, is_top INTEGER,
            jpush INTEGER, etl_sync_at DATETIME, PRIMARY KEY(source,source_id))"""))
        conn.execute(text("""INSERT INTO st_news_flash VALUES
            (:source,:source_id,:title,:content,:publish_time,:level,:stocks,:subjects,
             :reading_num,:is_top,:jpush,:etl_sync_at)"""),
            [{**row, "stocks": json.dumps(row["stocks"]), "subjects": json.dumps(row["subjects"]),
              "etl_sync_at": NOW + timedelta(seconds=1)} for row in rows])
    results = {}
    for source in ("cls", "eastmoney", "federal_reserve", "sina"):
        selected = [row for row in rows if row["source"] == source]
        latest = max((row["publish_time"] for row in selected), default=None)
        results[source] = {"status": "SUCCESS", "outcome": "NONEMPTY" if selected else "EMPTY",
            "requested_pages": 2 if source == "cls" else 1, "fetched_count": len(selected),
            "health_schema": news.SOURCE_HEALTH_SCHEMA, "latest_publish_time": latest,
            "latest_age_seconds": (NOW - datetime.fromisoformat(latest)).total_seconds() if latest else None,
            "max_age_seconds": 86400}
    results["federal_reserve"]["archive_coverage"] = proof
    receipt = news._receipt(status="PASS", started_at=NOW, finished_at=NOW + timedelta(seconds=2),
        source_results=results, pages=2, evidence={"persisted_count": len(rows),
            "latest_publish_time": max(row["publish_time"] for row in rows),
            "row_hash": news.news_row_hash(rows)})
    return engine, receipt, rows


def _resign(receipt):
    receipt = deepcopy(receipt)
    receipt.pop("receipt_id", None)
    return news._with_receipt_id(receipt)


def _validate(engine, receipt):
    return validation._validate_news_sync_receipt(engine, output=json.dumps(receipt),
        started_at=NOW, now=NOW + timedelta(seconds=3))


@pytest.mark.parametrize("nonempty", [False, True])
def test_complete_fed_archive_empty_or_nonempty_requires_exact_persisted_projection(nonempty):
    engine, receipt, _rows = _case(nonempty=nonempty)
    assert validation._news_sync_output_status(json.dumps(receipt), return_code=0) == "success"
    ok, message = _validate(engine, receipt)
    assert ok, message
    assert receipt["empty_sources"] == ([] if nonempty else ["federal_reserve"])
    engine.dispose()


@pytest.mark.parametrize("nonempty", [False, True])
def test_successful_fed_source_cannot_omit_archive_coverage(nonempty):
    engine, receipt, _rows = _case(nonempty=nonempty)
    del receipt["source_results"]["federal_reserve"]["archive_coverage"]
    receipt = _resign(receipt)
    assert validation._news_sync_output_status(json.dumps(receipt), return_code=0) == "failed"
    assert not _validate(engine, receipt)[0]
    engine.dispose()


def test_fed_source_cannot_disappear_from_an_otherwise_valid_receipt():
    engine, receipt, _rows = _case()
    del receipt["source_results"]["federal_reserve"]
    receipt["attempted_sources"].remove("federal_reserve")
    receipt["successful_sources"].remove("federal_reserve")
    receipt["empty_sources"] = []
    receipt = _resign(receipt)
    assert validation._news_sync_output_status(json.dumps(receipt), return_code=0) == "failed"
    engine.dispose()


def test_rolling_source_empty_cannot_borrow_complete_fed_proof():
    engine, receipt, _rows = _case()
    receipt["source_results"]["sina"] = deepcopy(receipt["source_results"]["federal_reserve"])
    receipt["empty_sources"].append("sina")
    receipt["nonempty_sources"].remove("sina")
    receipt = _resign(receipt)
    assert validation._news_sync_output_status(json.dumps(receipt), return_code=0) == "failed"
    engine.dispose()


def test_matching_receipt_and_database_hash_cannot_replace_exact_fed_source_content():
    engine, receipt, rows = _case(nonempty=True)
    for row in rows:
        if row["source"] == "federal_reserve":
            row["content"] = "Replaced content with same identity, timestamp and count"
    with engine.begin() as conn:
        conn.execute(text("UPDATE st_news_flash SET content=:content WHERE source='federal_reserve'"),
                     {"content": next(row["content"] for row in rows if row["source"] == "federal_reserve")})
    receipt["evidence"]["row_hash"] = news.news_row_hash(rows)
    receipt = _resign(receipt)
    assert validation._news_sync_output_status(json.dumps(receipt), return_code=0) == "success"
    ok, message = _validate(engine, receipt)
    assert not ok and "exact archive coverage" in message
    engine.dispose()


def test_declared_fed_count_must_equal_independently_replayed_inventory():
    engine, receipt, _rows = _case(nonempty=True)
    receipt["source_results"]["federal_reserve"]["fetched_count"] += 1
    receipt = _resign(receipt)
    assert validation._news_sync_output_status(json.dumps(receipt), return_code=0) == "failed"
    engine.dispose()
