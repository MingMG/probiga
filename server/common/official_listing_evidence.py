"""Bounded, source-authenticated IPO listing evidence from CNINFO disclosures.

Only missing securities are resolved. A confirmed listing disclosure supplies an
exact listing date. IPO subscription or payment terms can independently establish
an inclusive pre-listing period when the daily coverage manifest requests it.
"""
from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any, Mapping
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import requests

from server.common.finance_nonfiling_evidence import extract_document_text

SCHEMA = "probiga.cninfo-listing-evidence.v1"
ISSUANCE_SCHEMA = "probiga.cninfo-ipo-issuance-evidence.v1"
BASE = "https://www.cninfo.com.cn"
DOCUMENT_BASE = "https://static.cninfo.com.cn/"
MAX_CODES = 40
MAX_QUERY_PAGES = 4
MAX_DOCUMENTS_PER_CODE = 2
MAX_DOCUMENT_BYTES = 8 * 1024 * 1024
_CODE = re.compile(r"(?:证券|股票)\s*代码\s*[:：]\s*([0-9]{6})(?![0-9])")
_DATE = r"([12][0-9]{3})年([0-9]{1,2})月([0-9]{1,2})日"
_LISTING = re.compile(
    r"将于" + _DATE + r"在(?:上海|深圳|北京)证券交易所(?:主板|科创板|创业板)?上市"
    r"|上市(?:时间|日期)[:：]?" + _DATE
)
_DOCUMENT = re.compile(r"/finalpage/([0-9]{4}-[0-9]{2}-[0-9]{2})/[0-9]+\.PDF", re.I)
_ORG_ID = re.compile(r"[a-zA-Z0-9]{1,32}")
_BSE_IPO = "向不特定合格投资者公开发行股票并在北京证券交易所上市"
_BSE_LISTING_PROMPT = _BSE_IPO + "提示性公告"
_LISTING_TITLE = re.compile(
    r"首次公开发行股票并在.+上市之上市公告书(?:提示性公告)?|"
    + _BSE_IPO + r"(?:(?:之上市公告书|公告书)(?:提示性公告)?|提示性公告)"
)
_ISSUANCE_TITLE = re.compile(
    r"(?:首次公开发行股票并在(?:主板|创业板|科创板)上市|"
    + _BSE_IPO + r")(?:发行公告|网上申购情况及中签率公告)"
)
_ISSUER_CODE = re.compile(r"(?:证券|股票)代码(?:为|[:：])[“\"]?([0-9]{6})(?![0-9])")
_PAYMENT = re.compile(r"确保其资金账户在" + _DATE
                      + r"(?:[（(]T\+2日[）)])?日终有足额的新股认购资金")
_ALLOTMENT = re.compile(r"获配后在" + _DATE + r"[（(]T\+2日[）)]缴纳认购资金")
_SUBSCRIPTION = re.compile(r"(?:网上申购日(?:期)?|网下申购日与网上申购日同|(?:网上|网下)申购时间)"
                           r"为" + _DATE + r"[（(]T日[）)]")


class OfficialListingEvidenceError(RuntimeError):
    def __init__(self, stage: str, reason: str):
        super().__init__(f"OFFICIAL_LISTING_EVIDENCE_FAILED: stage={stage}; reason={reason}")


def listing_date_from_text(source_text: str, *, stock_code: str) -> str:
    if not isinstance(source_text, str) or not source_text or len(source_text.encode("utf-8")) > 256 * 1024:
        raise ValueError("listing document text is invalid")
    compact = re.sub(r"\s+", "", source_text)
    beijing_ipo = stock_code.startswith(("4", "8", "9")) and _BSE_IPO in compact
    if (set(_CODE.findall(source_text)) != {stock_code}
            or ("首次公开发行" not in compact and not beijing_ipo)
            or ("上市公告书" not in compact and not (beijing_ipo and _BSE_LISTING_PROMPT in compact))):
        raise ValueError("IPO document security identity differs")
    candidates = set()
    for match in _LISTING.finditer(compact):
        if re.search(r"预计|拟|如果|假如|计划", compact[max(0, match.start()-8):match.start()]):
            continue
        values = match.groups()[:3] if match.group(1) else match.groups()[3:]
        candidates.add(date(*(int(value) for value in values)).isoformat())
    if len(candidates) != 1:
        raise ValueError("unambiguous confirmed IPO listing date is absent")
    return candidates.pop()


def _issuance_through(source_text: str, *, stock_code: str) -> str:
    if not source_text or len(source_text.encode("utf-8")) > 256 * 1024:
        raise ValueError("issuance document text is invalid")
    compact = re.sub(r"\s+", "", source_text)
    if (not _ISSUANCE_TITLE.search(compact[:240]) or set(_ISSUER_CODE.findall(compact)) != {stock_code}):
        raise ValueError("IPO issuance document security identity differs")
    milestones = []
    for patterns in ((_PAYMENT, _ALLOTMENT), (_SUBSCRIPTION,)):
        dates = {date(*(int(part) for part in m.groups()))
                 for pattern in patterns for m in pattern.finditer(compact)}
        if len(dates) > 1:
            raise ValueError("IPO issuance milestone is ambiguous")
        milestones.extend(dates)
    if not milestones:
        raise ValueError("dated IPO subscription or payment obligation is absent")
    return max(milestones).isoformat()


def _issuance_document_text(raw: bytes) -> str:
    # Offering notices may append thousands of pages of allocation tables.
    # Only their opening terms can establish the issuer, subscription and payment.
    from pypdf import PdfReader
    if not raw.startswith(b"%PDF-") or len(raw) > MAX_DOCUMENT_BYTES:
        raise ValueError("IPO issuance PDF is invalid")
    try:
        reader = PdfReader(BytesIO(raw), strict=True)
        if reader.is_encrypted:
            raise ValueError("encrypted PDF")
        pages, size = [], 0
        for page in reader.pages[:16]:
            part = page.extract_text() or ""
            size += len(part.encode("utf-8")) + 1
            if size > 256 * 1024 or "\ufffd" in part:
                raise ValueError("issuance extract is invalid")
            pages.append(part)
        return "\n".join(pages)
    except Exception as exc:
        raise ValueError("unreadable IPO issuance PDF") from exc


def validate_official_listing_evidence(value: Mapping[str, Any], *, stock_code: str,
                                       target_date: str) -> dict:
    result = dict(value)
    parsed = urlparse(str(result.get("source_url") or ""))
    match = _DOCUMENT.fullmatch(parsed.path)
    if (result.get("schema") not in {SCHEMA, ISSUANCE_SCHEMA} or result.get("stock_code") != stock_code
            or parsed.scheme != "https" or parsed.hostname != "static.cninfo.com.cn"
            or parsed.username or parsed.password or parsed.query or parsed.fragment
            or match is None or match.group(1) > target_date
            or result.get("publication_date") != match.group(1)):
        raise ValueError("official listing document provenance differs")
    date.fromisoformat(target_date)
    text = result.get("source_text")
    if (not isinstance(text, str) or result.get("source_text_sha256") != sha256(text.encode("utf-8")).hexdigest()
            or re.fullmatch(r"[0-9a-f]{64}", str(result.get("official_document_sha256") or "")) is None):
        raise ValueError("official listing document seal differs")
    if result["schema"] == ISSUANCE_SCHEMA:
        through = _issuance_through(text, stock_code=stock_code)
        captured = datetime.fromisoformat(str(result.get("captured_at") or ""))
        if (captured.tzinfo is None or captured.utcoffset() is None
                or captured.astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat() < match.group(1)
                or "listing_date" in result or result.get("not_listed_through") != through
                or through < target_date
                or parsed.netloc != "static.cninfo.com.cn"
                or result.get("announcement_id") != parsed.path.rsplit("/", 1)[1].split(".")[0]):
            raise ValueError("target date is outside the proven IPO issuance period")
        return result
    listing = listing_date_from_text(text, stock_code=stock_code)
    if result.get("listing_date") != listing or listing <= target_date:
        raise ValueError("security is not proven to be pre-listing on target date")
    return result


def _remaining(deadline: float, stage: str) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise OfficialListingEvidenceError(stage, "total_deadline_exceeded")
    return remaining


def _fetch(session, method: str, url: str, *, deadline: float, stage: str,
           cap: int, data: dict | None = None) -> bytes:
    remaining = _remaining(deadline, stage)
    try:
        with session.request(method, url, data=data, stream=True, allow_redirects=False,
                             timeout=(min(5.0, remaining), min(10.0, remaining))) as response:
            response.raise_for_status()
            if response.status_code != 200:
                raise OfficialListingEvidenceError(stage, "unexpected_http_status")
            chunks, size = [], 0
            for chunk in response.iter_content(64 * 1024):
                _remaining(deadline, stage)
                size += len(chunk)
                if size > cap:
                    raise OfficialListingEvidenceError(stage, "response_size_limit")
                chunks.append(chunk)
            return b"".join(chunks)
    except OfficialListingEvidenceError:
        raise
    except requests.RequestException as exc:
        raise OfficialListingEvidenceError(stage, type(exc).__name__) from None


def _json(raw: bytes, stage: str):
    try:
        return json.loads(raw)
    except (ValueError, UnicodeError):
        raise OfficialListingEvidenceError(stage, "invalid_json") from None


def _load_in_process(codes: list[str], *, target_date: str,
                     deadline_seconds: float = 20, include_issuance: bool = False) -> dict[str, dict]:
    """Return only verified future-listing facts; unresolved codes remain blocked.

    There are no retries. All calls share one deadline, bounded query pages and
    response sizes. One bulk directory and batched issuer query serve each market.
    """
    target = date.fromisoformat(target_date)
    requested = sorted(set(codes))
    if (len(requested) > MAX_CODES or any(re.fullmatch(r"[0-9]{6}", code) is None for code in requested)
            or not 0 < float(deadline_seconds) <= 120):
        raise ValueError("official listing evidence request bounds differ")
    if not requested:
        return {}
    deadline = time.monotonic() + float(deadline_seconds)
    groups: dict[str, list[str]] = {}
    for code in requested:
        market = "sh" if code.startswith("6") else "bj" if code.startswith(("4", "8", "9")) else "sz"
        groups.setdefault(market, []).append(code)
    result = {}
    with requests.Session() as session:
        session.headers.update({"User-Agent": "Mozilla/5.0", "Referer": BASE + "/"})
        for market, market_codes in groups.items():
            # CNINFO's consolidated szse directory also includes Shanghai
            # and Beijing issuers, including prefixed alphanumeric orgIds.
            directory_market = "sz" if market == "sz" else "szse"
            column = "sse" if market == "sh" else "szse"
            identities = None
            if market != "sh" or include_issuance:
                stage = "issuer_directory"
                catalog = _json(_fetch(session, "GET", f"{BASE}/new/data/{directory_market}_stock.json",
                                      deadline=deadline, stage=stage, cap=4 * 1024 * 1024), stage)
                if not isinstance(catalog, dict) or not isinstance(catalog.get("stockList"), list):
                    raise OfficialListingEvidenceError(stage, "directory_shape_differs")
                selected = [row for row in catalog["stockList"] if isinstance(row, dict) and row.get("code") in market_codes]
                identity_counts = Counter(row["code"] for row in selected)
                identities = {row["code"]: row for row in selected if identity_counts[row["code"]] == 1 and row.get("category") == "A股"
                              and _ORG_ID.fullmatch(str(row.get("orgId") or ""))}
                if set(identities) != set(market_codes) and not include_issuance:
                    raise OfficialListingEvidenceError(stage, "requested_issuer_identity_missing")
                market_codes = sorted(identities)
                if not market_codes:
                    continue
            resolved = set()
            for issuance in ([False, True] if include_issuance else [False]):
                wanted = set(market_codes) - set(resolved)
                if not wanted:
                    break
                scoped = "" if identities is None else ";".join(
                    f"{code},{identities[code]['orgId']}" for code in sorted(wanted))
                announcements = []
                for page in range(1, MAX_QUERY_PAGES + 1):
                    stage = "announcement_inventory"
                    lookback = 14 if issuance else 45
                    fields = {"pageNum": str(page), "pageSize": "30", "column": column, "tabName": "fulltext",
                              "stock": scoped, "seDate": f"{target-timedelta(days=lookback)}~{target}"}
                    if market != "bj" or issuance:
                        fields.update(searchkey="上市发行公告" if issuance else "上市公告书",
                                      sortName="time", sortType="desc", isHLtitle="false")
                    payload = _json(_fetch(session, "POST", BASE + "/new/hisAnnouncement/query",
                        deadline=deadline, stage=stage, cap=2 * 1024 * 1024, data=fields), stage)
                    if not isinstance(payload, dict) or not isinstance(payload.get("announcements") or [], list):
                        raise OfficialListingEvidenceError(stage, "inventory_shape_differs")
                    announcements.extend(payload.get("announcements") or [])
                    if not payload.get("hasMore"):
                        break
                else:
                    raise OfficialListingEvidenceError(stage, "pagination_limit")
                for code in sorted(wanted):
                    candidates = [row for row in announcements if isinstance(row, dict)
                        and row.get("secCode") == code
                        and ((identities is None and _ORG_ID.fullmatch(str(row.get("orgId") or "")))
                             or (identities is not None and str(row.get("orgId")) == str(identities[code]["orgId"])))
                        and (_ISSUANCE_TITLE if issuance else _LISTING_TITLE).fullmatch(
                            str(row.get("announcementTitle") or "").removeprefix(str(row.get("secName") or "")))]
                    candidates.sort(key=lambda row: (
                        not row["announcementTitle"].endswith("发行公告") if issuance else "提示性公告" not in row["announcementTitle"],
                        -int(row.get("announcementTime") or 0)))
                    for row in candidates[:MAX_DOCUMENTS_PER_CODE]:
                        adjunct = str(row.get("adjunctUrl") or "")
                        match = _DOCUMENT.fullmatch("/" + adjunct)
                        if match is None or match.group(1) > target_date:
                            continue
                        url = DOCUMENT_BASE + adjunct
                        raw = _fetch(session, "GET", url, deadline=deadline, stage="official_document", cap=MAX_DOCUMENT_BYTES)
                        _remaining(deadline, "document_parse")
                        try:
                            if issuance:
                                source_text = _issuance_document_text(raw)
                                through = _issuance_through(source_text, stock_code=code)
                                if through < target_date:
                                    continue
                                identity = dict(schema=ISSUANCE_SCHEMA, not_listed_through=through)
                            else:
                                source_text = extract_document_text(raw)
                                listing = listing_date_from_text(source_text, stock_code=code)
                                if listing <= target_date:
                                    resolved.add(code)
                                    break
                                identity = dict(schema=SCHEMA, listing_date=listing)
                        except ValueError:
                            continue
                        _remaining(deadline, "document_parse")
                        evidence = dict(**identity, stock_code=code, source_url=url,
                            source_text=source_text, source_text_sha256=sha256(source_text.encode("utf-8")).hexdigest(),
                            official_document_sha256=sha256(raw).hexdigest(), publication_date=match.group(1),
                            announcement_id=str(row.get("announcementId") or ""),
                            captured_at=datetime.now(ZoneInfo("Asia/Shanghai")).replace(microsecond=0).isoformat())
                        result[code] = validate_official_listing_evidence(evidence, stock_code=code, target_date=target_date)
                        resolved.add(code)
                        break
    return result


def load_official_listing_evidence(codes: list[str], *, target_date: str,
                                   deadline_seconds: float = 20, include_issuance: bool = False) -> dict[str, dict]:
    """Enforce one wall-clock deadline across HTTP, PDF parsing and cleanup.

    A separate bounded worker is necessary: HTTP idle timeouts cannot bound a
    continuously trickling response, and synchronous PDF parsing is not safely
    interruptible by a Python thread. This worker owns no database or runtime
    control handles and starts no descendants.
    """
    requested = sorted(set(codes))
    date.fromisoformat(target_date)
    if (len(requested) > MAX_CODES or any(re.fullmatch(r"[0-9]{6}", code) is None for code in requested)
            or not 0 < float(deadline_seconds) <= 120):
        raise ValueError("official listing evidence request bounds differ")
    if not requested:
        return {}
    from server.common.process_env import build_child_env
    root = Path(__file__).resolve().parents[2]
    try:
        completed = subprocess.run(
            [sys.executable, "-B", "-m", __name__, "--worker"],
            input=json.dumps(dict(codes=requested, target_date=target_date, deadline_seconds=deadline_seconds,
                                  include_issuance=include_issuance)),
            capture_output=True, text=True, encoding="utf-8", cwd=root,
            env=build_child_env(root), timeout=float(deadline_seconds),
            **({"creationflags": subprocess.CREATE_NO_WINDOW} if sys.platform == "win32" else {}),
        )
    except subprocess.TimeoutExpired:
        raise OfficialListingEvidenceError("worker", "total_deadline_exceeded") from None
    if len(completed.stdout.encode("utf-8")) > MAX_CODES * 300 * 1024:
        raise OfficialListingEvidenceError("worker", "output_size_limit")
    payload = _json(completed.stdout.encode("utf-8"), "worker")
    if completed.returncode or not isinstance(payload, dict) or "error" in payload:
        reason = str(payload.get("error") or "worker_failed")[:300] if isinstance(payload, dict) else "invalid_output"
        raise OfficialListingEvidenceError("worker", reason)
    if set(payload) - set(requested):
        raise OfficialListingEvidenceError("worker", "unrequested_security")
    return {code: validate_official_listing_evidence(item, stock_code=code, target_date=target_date)
            for code, item in payload.items()}


if __name__ == "__main__":
    try:
        if sys.argv[1:] != ["--worker"]:
            raise ValueError("official listing worker invocation differs")
        request = json.loads(sys.stdin.read(32 * 1024))
        print(json.dumps(_load_in_process(**request), ensure_ascii=True), flush=True)
    except Exception as exc:
        print(json.dumps({"error": str(exc)[:300] if isinstance(exc, (OfficialListingEvidenceError, ValueError))
                          else type(exc).__name__}), flush=True)
        raise SystemExit(2)
