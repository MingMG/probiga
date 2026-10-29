"""Official macro releases that must not depend on commercial news feeds."""
from __future__ import annotations

import html
import re
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.request import Request, build_opener, ProxyHandler
from xml.etree import ElementTree
from zoneinfo import ZoneInfo


FED_MONETARY_RSS = "https://www.federalreserve.gov/feeds/press_monetary.xml"


def _read(url: str, *, timeout: int = 15) -> str:
    request = Request(url, headers={"User-Agent": "Mozilla/5.0 ProBigA-macro"})
    with build_opener(ProxyHandler({})).open(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def _plain_html(value: str) -> str:
    text = re.sub(r"<script\b.*?</script>", " ", value, flags=re.I | re.S)
    text = re.sub(r"<style\b.*?</style>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def _fed_statement_summary(url: str) -> str:
    try:
        text = _plain_html(_read(url))
    except Exception:
        return ""
    range_match = re.search(
        r"maintain the target range for the federal funds rate at "
        r"(.+?percent)",
        text,
        flags=re.I,
    )
    if not range_match:
        return ""
    target = range_match.group(1).strip()
    target = re.sub(r"\s+to\s+", "—", target, flags=re.I)
    target = target.replace("3-3/4", "3.75").replace("1/4", "0.25")
    summary = f"FOMC决定将联邦基金利率目标区间维持在 {target}。"
    if re.search(r"Voting against.+raise.+1/4 percentage point", text, flags=re.I):
        summary += "两名委员反对，主张加息25个基点。"
    return summary


def fetch_official_macro_news(
    *,
    now: datetime | None = None,
    lookback_hours: int = 72,
) -> list[dict[str, Any]]:
    """Return recent official Fed monetary-policy releases in DB-ready form."""

    cutoff = (now or datetime.now()) - timedelta(hours=lookback_hours)
    root = ElementTree.fromstring(_read(FED_MONETARY_RSS))
    items: list[dict[str, Any]] = []
    for node in root.findall("./channel/item"):
        title = str(node.findtext("title") or "").strip()
        link = str(node.findtext("link") or "").strip()
        published_raw = str(node.findtext("pubDate") or "").strip()
        if not title or not link or not published_raw:
            continue
        try:
            published = parsedate_to_datetime(published_raw).astimezone(
                ZoneInfo("Asia/Shanghai")
            ).replace(tzinfo=None)
        except (TypeError, ValueError, OverflowError):
            continue
        if published < cutoff:
            continue
        content = str(node.findtext("description") or title).strip()
        if "FOMC statement" in title:
            content = _fed_statement_summary(link) or content
            title = "美联储FOMC公布利率决议"
        items.append({
            "source": "federal_reserve",
            "source_id": link,
            "title": title,
            "content": content,
            "publish_time": published,
            "level": "A" if "FOMC" in title else "B",
            "subjects": [{"name": "美联储"}, {"name": "利率决议"}],
            "stocks": [],
            "reading_num": 0,
            "is_top": "FOMC" in title,
            "jpush": "FOMC" in title,
            "bold": "FOMC" in title,
            "author": "Federal Reserve",
        })
    return items


def format_official_macro_news(items: list[dict[str, Any]]) -> list[str]:
    result = []
    for item in items:
        timestamp = item.get("publish_time")
        time_text = (
            timestamp.strftime("%Y-%m-%d %H:%M:%S")
            if isinstance(timestamp, datetime)
            else ""
        )
        result.append(
            f"[美联储官方 {time_text}] {item.get('title', '')}："
            f"{item.get('content', '')}"
        )
    return result


__all__ = [
    "FED_MONETARY_RSS",
    "fetch_official_macro_news",
    "format_official_macro_news",
]
