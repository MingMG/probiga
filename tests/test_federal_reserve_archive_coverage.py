from __future__ import annotations

import base64
from datetime import datetime, timedelta
import hashlib
import json

import pytest

from server.common import federal_reserve_news_coverage as coverage

NOW = datetime(2026, 11, 2, 23, 5)


def archive_document(year=2026, *, fomc=False, entries=None):
    entries = entries if entries is not None else [("10/28/2026", "monetary20261028a.htm")]
    suffix = "-fomc" if fomc else ""
    rows = "".join(f'<div class="row"><div class="eventlist__time"><time>{day}</time></div>'
                   f'<div class="eventlist__event"><p><a href="/newsevents/pressreleases/{link}">Release</a></p>'
                   '<p class="eventlist__press"><em><strong>Monetary Policy</strong></em></p></div></div>'
                   for day, link in entries)
    body = (f'<html><body><h2>{year}{" FOMC" if fomc else ""} Press Releases</h2>{rows}'
            '<p>Last Update: November 2, 2026</p></body></html>').encode()
    return document(f'https://www.federalreserve.gov/newsevents/pressreleases/{year}-press{suffix}.htm', body)


def document(url, body):
    return {"url": url, "http_status": 200, "body_base64": base64.b64encode(body).decode(),
            "request_started_at": (NOW+timedelta(seconds=1)).isoformat(),
            "captured_at": (NOW+timedelta(seconds=4)).isoformat(),
            "source_response_sha256": hashlib.sha256(body).hexdigest()}


def source_documents(*, nonempty=False):
    recent = ('<item><title>Federal Reserve monetary update</title>'
              '<link>https://www.federalreserve.gov/newsevents/pressreleases/monetary20261102a.htm</link>'
              '<description>Native official text</description><pubDate>Mon, 02 Nov 2026 15:00:00 GMT</pubDate></item>') if nonempty else ''
    rss = ('<rss><channel><item><title>Federal Reserve issues FOMC statement</title>'
           '<link>https://www.federalreserve.gov/newsevents/pressreleases/monetary20261028a.htm</link>'
           '<description>Native statement</description><pubDate>Wed, 28 Oct 2026 18:00:00 GMT</pubDate>'
           f'</item>{recent}</channel></rss>').encode()
    entries = [("11/2/2026", "monetary20261102a.htm")] if nonempty else [("10/13/2026", "monetary20261013a.htm")]
    return [archive_document(entries=entries), archive_document(fomc=True), document(coverage.RSS_URL, rss)]


def seal(tmp_path, *, nonempty=False, documents=None, captured_at=None):
    documents = source_documents(nonempty=nonempty) if documents is None else documents
    if captured_at is not None:
        for doc in documents:
            doc['request_started_at'] = (captured_at-timedelta(seconds=3)).isoformat()
            doc['captured_at'] = captured_at.isoformat()
    return coverage.seal_federal_reserve_news_coverage(
        documents,
        observed_at=NOW, captured_at=captured_at or NOW+timedelta(seconds=4), root=tmp_path)


def replay(proof, tmp_path, *, started_at=NOW, finished_at=None):
    return coverage.validate_federal_reserve_news_coverage(
        proof, batch_started_at=started_at, batch_finished_at=finished_at or NOW+timedelta(seconds=10), root=tmp_path)


def test_full_general_and_fomc_archive_proves_empty_without_rolling_empty_success(tmp_path):
    rows, proof = seal(tmp_path)
    assert rows == replay(proof, tmp_path) == []
    assert proof['archive_document_count'] == 2
    assert proof['monetary_release_count'] == 2
    assert proof['item_count'] == 0
    assert len(json.dumps(proof)) < 2400


def test_real_bootstrap_layout_rows_do_not_merge_nested_year_events(tmp_path):
    docs = source_documents()
    for index in (0, 1):
        body = base64.b64decode(docs[index]['body_base64']).replace(b'<body>', b'<body><div class="row"><div class="content">').replace(b'</body>', b'</div></div></body>')
        docs[index] = document(docs[index]['url'], body)
    rows, proof = seal(tmp_path, documents=docs)
    assert rows == replay(proof, tmp_path) == []


def test_nonempty_replay_reconstructs_exact_original_native_content_and_time(tmp_path):
    rows, proof = seal(tmp_path, nonempty=True)
    assert rows == replay(proof, tmp_path)
    assert rows[0]['content'] == 'Native official text'
    assert rows[0]['publish_time'] == '2026-11-02T23:00:00'
    assert proof['item_count'] == 1


@pytest.mark.parametrize('mutation', ['missing_fomc', 'truncated', 'wrong_year', 'pagination', 'missing_category', 'source_mismatch', 'archive_missing_rss_release', 'rss_missing_archive_time'])
def test_unproven_or_inconsistent_complete_scope_is_rejected(tmp_path, mutation):
    docs = source_documents(nonempty=True)
    if mutation == 'missing_fomc':
        docs.pop(1)
    elif mutation == 'source_mismatch':
        docs[0]['url'] = docs[0]['url'].replace('federalreserve.gov', 'example.test')
    elif mutation == 'archive_missing_rss_release':
        docs[0] = archive_document(entries=[('10/13/2026', 'monetary20261013a.htm')])
    elif mutation == 'rss_missing_archive_time':
        docs[-1] = source_documents()[-1]
    else:
        body = base64.b64decode(docs[0]['body_base64'])
        if mutation == 'truncated': body = body.replace(b'</html>', b'')
        elif mutation == 'wrong_year': body = body.replace(b'2026 Press Releases', b'2025 Press Releases')
        elif mutation == 'pagination': body = body.replace(b'</body>', b'<nav class="pagination"></nav></body>')
        elif mutation == 'missing_category': body = body.replace(b'Monetary Policy', b'')
        docs[0] = document(docs[0]['url'], body)
    with pytest.raises(ValueError): seal(tmp_path, documents=docs)


def test_proof_cannot_retime_capture_or_skip_original_shards(tmp_path):
    _, proof = seal(tmp_path)
    with pytest.raises(ValueError): replay(proof, tmp_path, finished_at=NOW)
    with pytest.raises(ValueError): replay(proof, tmp_path, started_at=NOW-timedelta(minutes=1))
    with pytest.raises(ValueError): replay(proof, tmp_path/'missing')
    proof['item_count'] = 4
    with pytest.raises(ValueError): replay(proof, tmp_path)


def test_resigned_proof_cannot_move_capture_away_from_immutable_source(tmp_path):
    _, proof = seal(tmp_path)
    proof['request_started_at'] = (NOW+timedelta(seconds=35)).isoformat()
    proof['captured_at'] = (NOW+timedelta(seconds=40)).isoformat()
    proof['proof_sha256'] = coverage._hash({k:v for k,v in proof.items() if k != 'proof_sha256'})
    with pytest.raises(ValueError, match='PROJECTED_CONTENT'):
        replay(proof, tmp_path, finished_at=NOW+timedelta(seconds=45))


def test_even_resigned_proof_cannot_exceed_real_source_request_deadline(tmp_path):
    docs = source_documents()
    for doc in docs: doc['captured_at'] = (NOW+timedelta(seconds=40)).isoformat()
    with pytest.raises(ValueError, match='CAPTURE_WINDOW'):
        seal(tmp_path, documents=docs)


def test_healthy_capture_after_other_sources_is_not_subject_to_fake_25s_batch_cap(tmp_path):
    _, proof = seal(tmp_path, captured_at=NOW+timedelta(seconds=40))
    assert replay(proof, tmp_path, finished_at=NOW+timedelta(seconds=45)) == []


def test_new_york_year_boundary_requires_both_kinds_for_both_years():
    urls = coverage.archive_urls(datetime(2027, 1, 2, 1))
    assert len(urls) == 4
    assert any('/2026-' in url for url in urls)
    assert any('/2027-' in url for url in urls)


def test_worker_timeout_is_terminal_and_waited(monkeypatch):
    from biz.news import federal_reserve_archive as source
    def timeout(*args, **kwargs):
        assert kwargs['timeout'] <= 20
        raise source.subprocess.TimeoutExpired(args[0], kwargs['timeout'])
    monkeypatch.setattr(source.subprocess, 'run', timeout)
    with pytest.raises(RuntimeError, match='DEADLINE'):
        source.fetch_federal_reserve_news(observed_at=NOW)
