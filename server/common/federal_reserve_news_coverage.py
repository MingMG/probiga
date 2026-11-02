"""Replay official annual Fed archives and their native publication timestamps."""
from __future__ import annotations

import base64
import hashlib
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
import json
import re
from urllib.parse import urljoin
import uuid
from xml.etree import ElementTree
from zoneinfo import ZoneInfo

from server.common.acquisition_shards import AcquisitionShards

SCHEMA = "probiga.federal-reserve-news-coverage.v1"
DATASET = "federal-reserve-news-source"
RSS_URL = "https://www.federalreserve.gov/feeds/press_monetary.xml"
SHANGHAI = ZoneInfo("Asia/Shanghai")
NEW_YORK = ZoneInfo("America/New_York")
MAX_DOCUMENT_BYTES = 1024 * 1024
_RELEASE = re.compile(r"https://www\.federalreserve\.gov/newsevents/pressreleases/monetary(\d{8})[a-z]\.htm\Z")


def _hash(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _time(value):
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(SHANGHAI).replace(tzinfo=None)
    return parsed.replace(microsecond=0)


def archive_urls(observed_at):
    end = _time(observed_at).replace(tzinfo=SHANGHAI).astimezone(NEW_YORK)
    start = end - timedelta(hours=24)
    return tuple(f"https://www.federalreserve.gov/newsevents/pressreleases/{year}-press{suffix}.htm"
                 for year in range(start.year, end.year + 1) for suffix in ("", "-fomc"))


class _Archive(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.depth = 0
        self.row = None
        self.parent_rows = []
        self.capture = None
        self.headings = []
        self.rows = []
        self.closed = False
        self.has_update = False
        self.paginated = False

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag not in {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}:
            self.depth += 1
        classes = attrs.get("class", "").split()
        if "pagination" in classes or attrs.get("rel") == "next":
            self.paginated = True
        if tag == "div" and "row" in classes:
            self.parent_rows.append(self.row)
            self.row = {"depth": self.depth, "dates": [], "links": [], "categories": []}
        if tag in {"h1", "h2"}:
            self.capture = (tag, self.depth, [], "heading")
        elif self.row is not None and tag == "time":
            self.capture = (tag, self.depth, [], "date")
        elif self.row is not None and tag == "a":
            self.row["links"].append({"url": urljoin(RSS_URL, attrs.get("href", "")), "title": []})
            self.capture = (tag, self.depth, self.row["links"][-1]["title"], "link")
        elif self.row is not None and tag == "p" and "eventlist__press" in classes:
            self.capture = (tag, self.depth, [], "category")

    def handle_data(self, data):
        if "Last Update:" in data:
            self.has_update = True
        if self.capture is not None:
            self.capture[2].append(data)

    def handle_endtag(self, tag):
        if tag in {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}:
            return
        if self.capture is not None and self.capture[0] == tag and self.capture[1] == self.depth:
            text = " ".join(" ".join(self.capture[2]).split())
            kind = self.capture[3]
            if kind == "heading":
                self.headings.append(text)
            elif self.row is not None and kind == "date":
                self.row["dates"].append(text)
            elif self.row is not None and kind == "category":
                self.row["categories"].append(text)
            self.capture = None
        if self.row is not None and tag == "div" and self.row["depth"] == self.depth:
            if self.row["dates"]:
                self.rows.append(self.row)
            self.row = self.parent_rows.pop()
        if tag == "html":
            self.closed = True
        self.depth -= 1


def _archive_rows(body, url):
    match = re.fullmatch(r"https://www\.federalreserve\.gov/newsevents/pressreleases/(\d{4})-press(-fomc)?\.htm", url)
    if match is None:
        raise ValueError("FED_ARCHIVE_SOURCE_IDENTITY_INVALID")
    year, fomc = int(match[1]), bool(match[2])
    parser = _Archive()
    parser.feed(body.decode("utf-8", errors="strict"))
    parser.close()
    title = f"{year}{' FOMC' if fomc else ''} Press Releases"
    if title not in parser.headings or not parser.closed or not parser.has_update or parser.paginated or not parser.rows:
        raise ValueError("FED_ARCHIVE_FULL_YEAR_PAGE_UNPROVEN")
    records, identities = [], set()
    for row in parser.rows:
        if len(row["dates"]) != 1 or len(row["categories"]) != 1 or len(row["links"]) != 1:
            raise ValueError("FED_ARCHIVE_ROW_STRUCTURE_INVALID")
        day = datetime.strptime(row["dates"][0], "%m/%d/%Y").date()
        if day.year != year:
            raise ValueError("FED_ARCHIVE_YEAR_IDENTITY_DIFFERS")
        link = row["links"][0]
        if not " ".join(link["title"]).strip() or link["url"] in identities:
            raise ValueError("FED_ARCHIVE_ROW_IDENTITY_INVALID")
        identities.add(link["url"])
        category = row["categories"][0]
        if category not in {"Monetary Policy", "Orders on Banking Applications", "Enforcement Actions", "Other Announcements", "Banking and Consumer Regulatory Policy"}:
            raise ValueError("FED_ARCHIVE_CATEGORY_UNPROVEN")
        monetary = category == "Monetary Policy"
        if fomc and not monetary:
            raise ValueError("FED_FOMC_ARCHIVE_CATEGORY_DIFFERS")
        if monetary:
            release = _RELEASE.fullmatch(link["url"])
            if release is None or release[1] != day.strftime("%Y%m%d"):
                raise ValueError("FED_ARCHIVE_MONETARY_IDENTITY_DIFFERS")
        records.append({"date": day.isoformat(), "url": link["url"], "monetary": monetary})
    if len(records) > 2000:
        raise ValueError("FED_ARCHIVE_ROW_BUDGET_EXCEEDED")
    return records


def _rss_rows(body):
    if b"<!DOCTYPE" in body.upper() or b"<!ENTITY" in body.upper():
        raise ValueError("FED_RSS_ENTITY_DECLARATION_FORBIDDEN")
    root = ElementTree.fromstring(body)
    if root.tag != "rss" or root.find("channel") is None:
        raise ValueError("FED_RSS_STRUCTURE_INVALID")
    rows = {}
    for node in root.findall("./channel/item"):
        title, url, published = (str(node.findtext(k) or "").strip() for k in ("title", "link", "pubDate"))
        identity = _RELEASE.fullmatch(url)
        if not title or identity is None or not published or url in rows:
            raise ValueError("FED_RSS_ITEM_IDENTITY_INVALID")
        native_time = parsedate_to_datetime(published)
        if native_time.tzinfo is None or native_time.astimezone(NEW_YORK).strftime("%Y%m%d") != identity[1]:
            raise ValueError("FED_RSS_PUBLICATION_DATE_DIFFERS")
        fomc = "FOMC" in title
        rows[url] = {
            "source": "federal_reserve", "source_id": "fed:" + hashlib.sha256(url.encode()).hexdigest()[:40],
            "title": ("美联储FOMC公布利率决议" if "FOMC statement" in title else title)[:512],
            "content": str(node.findtext("description") or title).strip(),
            "publish_time": native_time.astimezone(SHANGHAI).replace(tzinfo=None).isoformat(timespec="seconds"),
            "level": "A" if fomc else "B", "stocks": [],
            "subjects": [{"name": "美联储"}, {"name": "利率决议"}],
            "reading_num": 0, "is_top": fomc, "jpush": fomc,
        }
    if not rows or len(rows) > 200:
        raise ValueError("FED_RSS_ITEM_INVENTORY_UNPROVEN")
    return rows


def _project(documents, observed_at):
    end = _time(observed_at)
    start = end - timedelta(hours=24)
    expected = set(archive_urls(end)) | {RSS_URL}
    if not isinstance(documents, list) or len(documents) != len(expected):
        raise ValueError("FED_ARCHIVE_DOCUMENT_SET_DIFFERS")
    by_url, archive, metadata, captures, requests = {}, [], [], [], []
    for document in documents:
        url = document.get("url")
        if url not in expected or url in by_url or document.get("http_status") != 200:
            raise ValueError("FED_SOURCE_DOCUMENT_IDENTITY_INVALID")
        body = base64.b64decode(document.get("body_base64", ""), validate=True)
        if not 0 < len(body) <= MAX_DOCUMENT_BYTES or hashlib.sha256(body).hexdigest() != document.get("source_response_sha256"):
            raise ValueError("FED_SOURCE_DOCUMENT_CONTENT_DIFFERS")
        requested, captured = _time(document["request_started_at"]), _time(document["captured_at"])
        if not end <= requested <= captured <= requested + timedelta(seconds=25):
            raise ValueError("FED_SOURCE_DOCUMENT_CAPTURE_WINDOW_INVALID")
        requests.append(requested)
        captures.append(captured)
        by_url[url] = body
        metadata.append({"url": url, "source_response_sha256": document["source_response_sha256"], "bytes": len(body),
                         "request_started_at": requested.isoformat(), "captured_at": captured.isoformat()})
        if url != RSS_URL:
            archive.extend(_archive_rows(body, url))
    native = _rss_rows(by_url[RSS_URL])
    first_day = start.replace(tzinfo=SHANGHAI).astimezone(NEW_YORK).date().isoformat()
    last_day = end.replace(tzinfo=SHANGHAI).astimezone(NEW_YORK).date().isoformat()
    monetary = {r["url"] for r in archive if r["monetary"]}
    if len(monetary) != sum(r["monetary"] for r in archive):
        raise ValueError("FED_ARCHIVE_DUPLICATE_MONETARY_RELEASE")
    candidates = {r["url"] for r in archive if r["monetary"] and first_day <= r["date"] <= last_day}
    if not candidates.issubset(native):
        raise ValueError("FED_ARCHIVE_NATIVE_PUBLICATION_TIMES_INCOMPLETE")
    items = sorted((r for r in native.values() if start <= _time(r["publish_time"]) <= end), key=lambda r: (r["source"], r["source_id"]))
    included_urls = {url for url, row in native.items() if start <= _time(row["publish_time"]) <= end}
    if not included_urls.issubset(monetary):
        raise ValueError("FED_ARCHIVE_RSS_WINDOW_SET_DIFFERS")
    if max(captures) > min(requests) + timedelta(seconds=25):
        raise ValueError("FED_SOURCE_REQUEST_TOTAL_DEADLINE_EXCEEDED")
    return items, {"request_started_at": min(requests).isoformat(), "captured_at": max(captures).isoformat(),
                   "archive_set_sha256": _hash(sorted(metadata, key=lambda x: x["url"])),
                   "archive_document_count": len(expected) - 1, "archive_row_count": len(archive),
                   "monetary_release_count": len(monetary), "item_count": len(items), "item_row_hash": _hash(items)}


def seal_federal_reserve_news_coverage(documents, *, observed_at, captured_at, root=None):
    observed, captured = _time(observed_at), _time(captured_at)
    if not observed <= captured:
        raise ValueError("FED_SOURCE_CAPTURE_WINDOW_INVALID")
    items, projection = _project(documents, observed)
    if projection["captured_at"] != captured.isoformat():
        raise ValueError("FED_SOURCE_CAPTURE_DIFFERS_FROM_ORIGINALS")
    scope = {"schema": SCHEMA, "observed_at": observed.isoformat(), "capture_id": uuid.uuid4().hex}
    shards = AcquisitionShards(DATASET, scope, root=root)
    shards.save("official_documents", documents)
    proof = {"schema": SCHEMA, "provider": "federal_reserve", "coverage_status": "COMPLETE",
             "captured_at": captured.isoformat(), "window_start_at": (observed-timedelta(hours=24)).isoformat(),
             "window_end_at": observed.isoformat(), "checkpoint_scope": scope, **projection}
    proof["proof_sha256"] = _hash(proof)
    return items, proof


def validate_federal_reserve_news_coverage(proof, *, batch_started_at, batch_finished_at, root=None):
    try:
        if not isinstance(proof, dict):
            raise ValueError("FED_SOURCE_PROOF_MISSING")
        unsigned = {k: v for k, v in proof.items() if k != "proof_sha256"}
        if _hash(unsigned) != proof.get("proof_sha256") or proof.get("schema") != SCHEMA or proof.get("provider") != "federal_reserve" or proof.get("coverage_status") != "COMPLETE":
            raise ValueError("FED_SOURCE_PROOF_SEAL_INVALID")
        start, finished = _time(batch_started_at), _time(batch_finished_at)
        captured, end = _time(proof["captured_at"]), _time(proof["window_end_at"])
        requested = _time(proof["request_started_at"])
        if not start <= requested <= captured <= min(finished, requested+timedelta(seconds=25)) or end != start or _time(proof["window_start_at"]) != end-timedelta(hours=24):
            raise ValueError("FED_SOURCE_PROOF_CAPTURE_OR_SCOPE_DIFFERS")
        scope = proof["checkpoint_scope"]
        if set(scope) != {"schema", "observed_at", "capture_id"} or scope["schema"] != SCHEMA or scope["observed_at"] != end.isoformat() or re.fullmatch(r"[a-f0-9]{32}", str(scope["capture_id"])) is None:
            raise ValueError("FED_SOURCE_PROOF_CHECKPOINT_IDENTITY_DIFFERS")
        documents = AcquisitionShards(DATASET, scope, root=root, create=False).load("official_documents")
        items, projection = _project(documents, end)
        if any(proof.get(k) != value for k, value in projection.items()):
            raise ValueError("FED_SOURCE_PROOF_PROJECTED_CONTENT_DIFFERS")
        return items
    except (KeyError, TypeError, AttributeError, OverflowError, UnicodeError, ElementTree.ParseError) as exc:
        raise ValueError("FED_SOURCE_PROOF_MALFORMED") from exc
