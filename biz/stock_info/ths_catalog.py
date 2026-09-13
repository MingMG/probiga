"""THS directory bound to native CID/index pairs and the provider's published total."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import re
import threading
import time
from typing import Callable

from bs4 import BeautifulSoup
import requests


CATALOG_URL = "https://q.10jqka.com.cn/gn/"
DETAIL_URL = "https://q.10jqka.com.cn/gn/detail/code/{cid}/"
STOCK_CONCEPT_URL = "https://basic.10jqka.com.cn/{stock_code}/concept.html"
SOURCE_SCHEMA = "probiga.ths-f10-reference.v1"
INDEX_PATTERN = re.compile(r"88\d{4}\Z")


def identity_hash(values) -> str:
    return hashlib.sha256("\n".join(sorted(values)).encode("utf8")).hexdigest()


def _valid_text(value: str) -> bool:
    return bool(value) and not any(0xD800 <= ord(char) <= 0xDFFF or char == "\ufffd" for char in value)


class PublicReader:
    def __init__(self):
        self._local = threading.local()
        self._rate_lock = threading.Lock()
        self._next_request = 0.0

    @staticmethod
    def _new_session():
        session = requests.Session()
        session.trust_env = False
        session.headers.update({"User-Agent": "Mozilla/5.0", "Referer": CATALOG_URL})
        return session

    def _request_slot(self):
        while True:
            with self._rate_lock:
                delay = self._next_request - time.monotonic()
                if delay <= 0:
                    self._next_request = time.monotonic() + 0.25
                    return
            time.sleep(min(delay, 30.0))

    def __call__(self, url: str) -> str:
        if not hasattr(self._local, "session"):
            self._local.session = self._new_session()
        for attempt in range(3):
            try:
                self._request_slot()
                response = self._local.session.get(url, timeout=10)
                response.raise_for_status()
                # Preserve malformed bytes in unrelated embedded news/ads.
                # Every extracted identity is validated below; no replacement
                # character is allowed to alter a published name or code.
                content = response.content.decode("gb18030", errors="surrogateescape")
                if "upass.10jqka.com.cn/login" in content and len(content) < 2000:
                    raise RuntimeError("THS returned a login page instead of reference data")
                return content
            except (requests.RequestException, UnicodeError) as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if attempt == 2:
                    raise RuntimeError(f"THS public request failed: {url}: {type(exc).__name__}: status={status}") from exc
                if status in {403, 429}:
                    # Pause the whole reader when the provider refuses a burst;
                    # other workers must not continue hammering the same host.
                    with self._rate_lock:
                        self._next_request = max(self._next_request, time.monotonic() + 30 * (attempt + 1))
                    if status == 403:
                        self._local.session.close()
                        self._local.session = self._new_session()
                else:
                    time.sleep(0.5 * (attempt + 1))
        raise AssertionError("unreachable")


def parse_catalog_page(content: str) -> set[str]:
    soup = BeautifulSoup(content, "html.parser")
    cids = {
        match.group(1)
        for anchor in soup.select("a[href]")
        if (match := re.search(r"/gn/detail/code/(\d{6})(?:/|$)", anchor["href"]))
    }
    graph = soup.select_one("#gnSection")
    if not cids or graph is None:
        raise RuntimeError("THS public catalog structure differs")
    records = json.loads(graph.get("value", ""))
    if not isinstance(records, dict) or not records:
        raise RuntimeError("THS public catalog graph is absent")
    for row in records.values():
        if not isinstance(row, dict) or not re.fullmatch(r"\d{6}", str(row.get("cid", ""))):
            raise RuntimeError("THS catalog graph contains invalid native CID")
        if not INDEX_PATTERN.fullmatch(str(row.get("platecode", ""))):
            raise RuntimeError("THS catalog graph contains invalid native index")
        cids.add(row["cid"])
    return cids


def parse_detail_page(content: str, cid: str) -> tuple[dict, int]:
    soup = BeautifulSoup(content, "html.parser")
    native = soup.select_one("#clid")
    query = soup.select_one("#requestQuery")
    heading = soup.select_one("h3")
    rank_label = soup.find("dt", string="涨幅排名")
    rank = rank_label.find_next_sibling("dd") if rank_label else None
    index_code = str(native.get("value", "")) if native else ""
    if query is None or query.get("value") != f"code/{cid}":
        raise RuntimeError(f"THS detail CID identity differs: {cid}")
    if not INDEX_PATTERN.fullmatch(index_code) or heading is None or rank is None:
        raise RuntimeError(f"THS detail native identity/total missing: {cid}")
    rank_match = re.fullmatch(r"\d+/(\d+)", rank.get_text(strip=True))
    title = heading.get_text(strip=True)
    if not rank_match or not title.endswith(index_code):
        raise RuntimeError(f"THS detail native identity/total differs: {cid}")
    name = title[:-len(index_code)].strip()
    total = int(rank_match.group(1))
    if not _valid_text(name) or total <= 0:
        raise RuntimeError(f"THS detail has empty name or total: {cid}")
    return {"index_code": index_code, "concept_code": cid, "name": name}, total


def parse_stock_concepts(content: str, stock_code: str) -> dict[str, str]:
    soup = BeautifulSoup(content, "html.parser")
    native = soup.select_one("#stockCode")
    title = soup.title.get_text() if soup.title else ""
    if native is None or native.get("value") != stock_code or f"({stock_code})" not in title:
        raise RuntimeError(f"THS stock discovery identity differs: {stock_code}")
    result = {}
    for cell in soup.select("td.gnName[clid]"):
        index_code = cell.get("clid", "")
        # Some non-indexed descriptive themes have an empty clid. They cannot
        # be converted into a quoted concept index or included in its total.
        if not index_code:
            continue
        if not INDEX_PATTERN.fullmatch(index_code):
            raise RuntimeError(f"THS stock page has an invalid concept index: {stock_code}")
        name = cell.get_text(" ", strip=True)
        if not _valid_text(name) or (index_code in result and result[index_code] != name):
            raise RuntimeError(f"THS stock page has conflicting concept identities: {stock_code}")
        result[index_code] = name
    return result


def parse_stock_concept_bindings(content: str, stock_code: str) -> dict[str, str]:
    identities = parse_stock_concepts(content, stock_code)
    soup = BeautifulSoup(content, "html.parser")
    bindings = {}
    for cell in soup.select("td.gnName[clid]"):
        index_code = cell.get("clid", "")
        if index_code not in identities:
            continue
        sources = cell.parent.select(".retractCon[cid]")
        cids = {item.get("cid", "") for item in sources}
        if len(cids) != 1:
            continue
        cid = cids.pop()
        if re.fullmatch(r"\d{6}", cid):
            bindings[index_code] = cid
    return bindings


def collect_catalog(stock_codes, *, fetch: Callable[[str], str] | None = None):
    fetch = fetch or PublicReader()
    cids = sorted(parse_catalog_page(fetch(CATALOG_URL)))
    with ThreadPoolExecutor(max_workers=4) as pool:
        details = list(pool.map(lambda cid: parse_detail_page(fetch(DETAIL_URL.format(cid=cid)), cid), cids))
    totals = {total for _, total in details}
    if len(totals) != 1:
        raise RuntimeError("THS live directory totals changed during collection")
    expected = totals.pop()
    catalog = {row["index_code"]: row for row, _ in details}
    if len(catalog) != len(details) or len(catalog) > expected:
        raise RuntimeError("THS directory native identities are duplicated or exceed its total")
    discovery_errors = []
    codes = sorted(set(stock_codes))
    # Spread each discovery batch over the whole current exchange universe.
    # Discovery is finished only by the independent native directory total,
    # never by having exhausted a chosen number of stock requests.
    order = [codes[i] for offset in range(min(100, len(codes))) for i in range(offset, len(codes), 100)]
    while len(catalog) < expected and order:
        batch, order = order[:100], order[100:]
        def discover(code):
            try:
                content = fetch(STOCK_CONCEPT_URL.format(stock_code=code))
                return parse_stock_concept_bindings(content, code), None
            except Exception as exc:
                return {}, f"{code}:{type(exc).__name__}:{exc}"
        with ThreadPoolExecutor(max_workers=4) as pool:
            discoveries = list(pool.map(discover, batch))
        for identities, error in discoveries:
            if error:
                discovery_errors.append(error)
            for index_code, cid in identities.items():
                if index_code not in catalog:
                    row, total = parse_detail_page(fetch(DETAIL_URL.format(cid=cid)), cid)
                    if row["index_code"] != index_code or total != expected:
                        raise RuntimeError("THS stock/quote concept identity or universe total differs")
                    catalog[index_code] = row
        if len(catalog) > expected:
            raise RuntimeError("THS discovered concept identities exceed native directory total")
    if len(catalog) != expected:
        raise RuntimeError(f"THS directory is incomplete: expected={expected}, observed={len(catalog)}; discovery_errors={discovery_errors[:10]}")
    # Recheck the native total after discovery; a changing directory cannot
    # be certified from pages collected across different source inventories.
    first = next(iter(catalog.values()))
    confirmed, total = parse_detail_page(fetch(DETAIL_URL.format(cid=first["concept_code"])), first["concept_code"])
    if total != expected or confirmed["index_code"] != first["index_code"]:
        raise RuntimeError("THS directory changed during acquisition")
    return tuple(catalog[key] for key in sorted(catalog)), {
        "schema": SOURCE_SCHEMA, "kind": "authoritative_total", "complete": True,
        "expected_total": expected, "received_rows": len(catalog),
        "concept_set_hash": identity_hash(catalog),
    }
