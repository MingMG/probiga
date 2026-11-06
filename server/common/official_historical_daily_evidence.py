"""Replayable official evidence for a historical session with no daily bar."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from hashlib import sha256
import json
from pathlib import Path
import re
import subprocess
import sys
import time
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import requests

from server.common.finance_nonfiling_evidence import extract_document_text
from server.common.official_listing_evidence import (
    BASE, MAX_DOCUMENT_BYTES, _fetch, _remaining,
)

SCHEMA = "probiga.official-historical-daily-evidence.v1"
CHINA = ZoneInfo("Asia/Shanghai")
_CODE = re.compile(r"(?:证券|股票)代码[:：]([0-9]{6})(?![0-9])")
_URL = re.compile(r"/finalpage/(\d{4}-\d{2}-\d{2})/\d+\.PDF", re.I)
_HASH = re.compile(r"[0-9a-f]{64}")
_DAY = r"(\d{4})年(\d{1,2})月(\d{1,2})日"
_WEEKDAY = r"(?:[（(](?:星期|周)[一二三四五六日天][）)])?"
_SUSPEND = re.compile(r"(?:自|于)" + _DAY + _WEEKDAY + r"(?:开市)?起(?:开始)?停牌")
_RESUME = re.compile(r"(?:将于|自|于)" + _DAY + _WEEKDAY + r"(?:开市)?起复牌")
_DELIST = re.compile(r"摘牌日期[:：]" + _DAY)
_SERIES = re.compile(r"于(\d{4}年\d{1,2}月\d{1,2}日(?:、(?:\d{4}年)?\d{1,2}月\d{1,2}日){1,15})连续停牌")
_RECEIVED = re.compile(r"于" + _DAY + r"收到(?:了)?上海证券交易所[^。]{1,180}终止上市[^。]{0,20}事先告知书")


def _document_date(url: str) -> date:
    if not isinstance(url, str):
        raise ValueError("historical daily evidence source URL differs")
    parsed = urlparse(url)
    match = _URL.fullmatch(parsed.path)
    if parsed.scheme != "https" or parsed.netloc != "static.cninfo.com.cn" or parsed.query or parsed.fragment or not match:
        raise ValueError("historical daily evidence source URL differs")
    return date.fromisoformat(match.group(1))


def _dates(pattern, text: str) -> set[date]:
    return {date(*(int(part) for part in match.groups())) for match in pattern.finditer(text)
            if not re.search(r"预计|如果|假如|拟于", text[max(0, match.start() - 12):match.start() + 1])}


def _explicit_sessions(text: str) -> set[date]:
    result = set()
    for match in _SERIES.finditer(text):
        year = int(match[1][:4])
        for y, m, d in re.findall(r"(?:(\d{4})年)?(\d{1,2})月(\d{1,2})日", match[1]):
            year = int(y) if y else year
            result.add(date(year, int(m), int(d)))
    return result


def _classification(texts: list[tuple[date, str]], target: date) -> str:
    # Explicit resumed sessions always invalidate any competing no-trade inference.
    if any(target in _dates(_RESUME, body) for _, body in texts):
        raise ValueError("official evidence shows trading resumed on target session")
    if any(target in _dates(_DELIST, body) and "股票终止上市暨摘牌" in body for _, body in texts):
        return "DELISTED"
    for published, body in texts:
        starts = _dates(_SUSPEND, body)
        resumes = _dates(_RESUME, body)
        if target in _explicit_sessions(body):
            return "NO_TRADE"
        if (len(starts) == len(resumes) == 1 and min(starts) <= target < min(resumes)
                and ("股票停牌" in body or "停牌期间" in body)):
            return "NO_TRADE"
        # A later issuer filing may reaffirm a start date and the still-active
        # mandatory suspension. An earlier open-ended notice cannot do so.
        if (len(starts) == 1 and min(starts) <= target <= published and not resumes
                and "交易类强制退市" in body
                and ("终止上市事先告知书" in body or "不进入退市整理期" in body)):
            return "NO_TRADE"
    # Two dedicated same-issuer notices may bind one interval even when the
    # resumption notice does not repeat the original suspension date.
    starts = {start for published, body in texts if "停牌公告" in body
              for start in _dates(_SUSPEND, body) if start == published}
    resumes = {resume for published, body in texts if "复牌" in body and published >= target
               for resume in _dates(_RESUME, body)}
    if len(starts) == len(resumes) == 1 and min(starts) <= target < min(resumes):
        return "NO_TRADE"
    # The SSE regulatory letter explicitly makes review wait for the hearing
    # filing period in its separate advance notice. Five calendar days are a
    # conservative lower bound for the five exchange sessions in that notice.
    hearing = {received for _, body in texts
               if "收到本通知后5个交易日内" in body and "书面听证申请" in body
               for received in _dates(_RECEIVED, body)}
    if len(hearing) == 1:
        until = min(hearing) + timedelta(days=5)
        if any("监管工作函" in body and "有关期限届满或者听证程序结束后" in body
               and "作出相应的终止上市决定" in body
               and any(start <= target <= until for start in _dates(_SUSPEND, body))
               for _, body in texts):
            return "NO_TRADE"
    raise ValueError("official no-bar lifecycle is unproven for target session")


def historical_daily_evidence(documents: list[dict], *, stock_code: str, day: str,
                              cutoff: datetime) -> tuple[str, dict]:
    if not 1 <= len(documents) <= 4:
        raise ValueError("historical daily evidence document count differs")
    target = date.fromisoformat(day)
    cutoff = cutoff.replace(tzinfo=CHINA) if cutoff.tzinfo is None else cutoff.astimezone(CHINA)
    ordered = sorted(documents, key=lambda item: item["source_url"])
    if len({item["source_url"] for item in ordered}) != len(ordered):
        raise ValueError("duplicate historical daily evidence document")
    texts = []
    for item in ordered:
        published = _document_date(item["source_url"])
        observed = datetime.fromisoformat(item["observed_at"])
        source_text = item["source_text"]
        compact = re.sub(r"\s+", "", source_text)
        if (not observed.tzinfo or not published <= observed.astimezone(CHINA).date()
                or observed > cutoff or len(source_text.encode("utf-8")) > 128 * 1024
                or set(_CODE.findall(compact)) != {stock_code}
                or item["source_text_sha256"] != sha256(source_text.encode("utf-8")).hexdigest()
                or not _HASH.fullmatch(item["official_document_sha256"])):
            raise ValueError("historical daily evidence document identity or time differs")
        texts.append((published, compact))
    classification = _classification(texts, target)
    return classification, {"kind": "OFFICIAL_HISTORICAL_LIFECYCLE", "schema": SCHEMA,
                            "stock_code": stock_code, "target_date": day, "documents": ordered}


def _read_documents(locators: dict[str, list[str]], timeout_seconds: float) -> dict[str, list[dict]]:
    deadline = time.monotonic() + timeout_seconds
    result = {}
    with requests.Session() as session:
        session.headers.update({"User-Agent": "Mozilla/5.0", "Referer": BASE + "/"})
        for code, urls in locators.items():
            result[code] = []
            for url in urls:
                raw = _fetch(session, "GET", url, deadline=deadline, stage="historical_lifecycle_document",
                             cap=MAX_DOCUMENT_BYTES)
                observed = datetime.now(CHINA).replace(microsecond=0).isoformat()
                source_text = extract_document_text(raw)
                _remaining(deadline, "historical_lifecycle_parse")
                result[code].append({"source_url": url, "observed_at": observed, "source_text": source_text,
                                     "source_text_sha256": sha256(source_text.encode("utf-8")).hexdigest(),
                                     "official_document_sha256": sha256(raw).hexdigest()})
    return result


def _validate_locators(locators: dict[str, list[str]]) -> None:
    if not isinstance(locators, dict) or not locators or len(locators) > 20:
        raise ValueError("historical daily evidence locator bounds differ")
    for code, urls in locators.items():
        if not re.fullmatch(r"\d{6}", code) or not isinstance(urls, list) or not 1 <= len(urls) <= 4:
            raise ValueError("historical daily evidence locator identity differs")
        for url in urls:
            _document_date(url)


def load_historical_documents(locators: dict[str, list[str]], *, timeout_seconds: float = 75) -> dict:
    """Only the official PDF locators are supplied by the operator."""
    _validate_locators(locators)
    if not 0 < timeout_seconds <= 120:
        raise ValueError("historical lifecycle deadline differs")
    from server.common.process_env import build_child_env

    root = Path(__file__).resolve().parents[2]
    try:
        worker = subprocess.run(
            [sys.executable, "-B", "-m", __name__], cwd=root, env=build_child_env(root),
            input=json.dumps({"locators": locators, "timeout_seconds": timeout_seconds}),
            capture_output=True, text=True, encoding="utf-8", timeout=timeout_seconds,
            **({"creationflags": subprocess.CREATE_NO_WINDOW} if sys.platform == "win32" else {}),
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("historical_lifecycle_document: total deadline exceeded") from None
    if worker.returncode or len(worker.stdout.encode("utf-8")) > 4 * 1024 * 1024:
        raise RuntimeError("historical_lifecycle_document: official evidence worker failed")
    result = json.loads(worker.stdout)
    if set(result) != set(locators):
        raise ValueError("historical daily evidence worker inventory differs")
    return result


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    args = json.load(sys.stdin)
    _validate_locators(args["locators"])
    print(json.dumps(_read_documents(**args), ensure_ascii=False))
