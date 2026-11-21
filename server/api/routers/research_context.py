"""Official macro releases and the existing captured cross-asset observations."""
from datetime import date, datetime, time
from time import monotonic
import json
import sqlite3
from urllib.parse import urljoin, urlparse
import re

from bs4 import BeautifulSoup
import requests
from fastapi import APIRouter, Query

from server.api.routers._engine import get_engine
from server.api.routers.market_home import SHANGHAI
from server.api.routers.research_tools import _read, _serve

router = APIRouter(tags=["research-context"])
NBS_RELEASES = "https://www.stats.gov.cn/sj/zxfb/"


def _cutoff(day):
    selected = date.fromisoformat(day)
    now = datetime.now(SHANGHAI).replace(tzinfo=None)
    if selected > now.date():
        raise ValueError("观察日期不能晚于今天")
    return min(datetime.combine(selected, time.max), now)


def _official_html(session, url, deadline):
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise TimeoutError("official-macro deadline exceeded")
    host = urlparse(url)
    if host.scheme != "https" or host.hostname != "www.stats.gov.cn":
        raise ValueError("宏观发布链接不是国家统计局来源")
    with session.get(url, timeout=(min(4, remaining), min(8, remaining)),
                     stream=True, allow_redirects=False) as response:
        response.raise_for_status()
        if response.status_code != 200:
            raise ValueError("国家统计局发布页返回了重定向")
        body = bytearray()
        for block in response.iter_content(16384):
            body.extend(block)
            if monotonic() > deadline or len(body) > 2_000_000:
                raise TimeoutError("official-macro response exceeds its time or byte bound")
    return BeautifulSoup(body.decode("utf-8"), "html.parser")


def _release_candidates(document, cutoff):
    candidates = {}
    for link in document.select("a[href]"):
        title = link.get_text("", strip=True)
        match = re.search(r"/t(\d{8})_\d+\.html$", str(link["href"]))
        if not match or "国民经济运行" not in title or any(x in title for x in ("答记者问", "解读")):
            continue
        published = datetime.strptime(match[1], "%Y%m%d").date()
        url = urljoin(NBS_RELEASES, link["href"])
        if published <= cutoff.date() and urlparse(url).hostname == "www.stats.gov.cn":
            candidates[url] = {"title":title, "date":published.isoformat(), "url":url}
    return sorted(candidates.values(), key=lambda x:(x["date"],x["url"]), reverse=True)


def _macro_table(document):
    for table in document.select("table"):
        rows = [[re.sub(r"\s+", "", cell.get_text("", strip=True))
                 for cell in tr.find_all(["td","th"], recursive=False)]
                for tr in table.select("tr")]
        if not rows or "指标" not in "".join(rows[0]) or "同比增长" not in "".join(sum(rows[:3], [])):
            continue
        # Keep the official absolute amounts and units, including legal ellipses.
        headers = rows[0]
        output = [{"indicator":r[0], "current_value":r[1], "current_yoy":r[2],
                   "cumulative_value":r[3], "cumulative_yoy":r[4]}
                  for r in rows[2:] if len(r)==5 and r[0]]
        if not output or len(output)>250:
            raise ValueError("国家统计局指标表结构不完整")
        return headers, output
    raise ValueError("发布页没有可核对的国民经济指标原表")


def build_macro_release(day):
    cutoff, deadline = _cutoff(day), monotonic()+24
    with requests.Session() as session:
        session.trust_env = False
        listing = _official_html(session, NBS_RELEASES, deadline)
        candidates = _release_candidates(listing, cutoff)
        if not candidates:
            return {"trade_date":day,"status":"unavailable", "reason":"当前官方发布目录没有所选日前的经济运行原表"}
        release = candidates[0]
        document = _official_html(session, release["url"], deadline)
    title = document.find("meta", attrs={"name":"ArticleTitle"})
    published = document.find("meta", attrs={"name":"PubDate"})
    if title is None or published is None or "国民经济运行" not in str(title.get("content","")):
        raise ValueError("国家统计局原表的标题或发布时间缺失")
    published_at = datetime.strptime(published["content"], "%Y/%m/%d %H:%M")
    if published_at > cutoff or published_at.date().isoformat()!=release["date"]:
        raise ValueError("宏观原表发布时间超出所选日期")
    headers, rows = _macro_table(document)
    return {"trade_date":day,"status":"available","title":title["content"],
            "published_at":published_at.isoformat(),"known_at":datetime.now(SHANGHAI).isoformat(timespec="seconds"),
            "source_url":release["url"],"headers":headers,"rows":rows,"releases":candidates[:12],
            "method":"直接展示国家统计局经济运行发布原表；当月与累计口径分列，保留原单位和缺值符号。市场一致预期没有来源时不填入。",
            "evidence_mode":"CURRENT_OBSERVATION"}


def build_global_context(day, mode="latest"):
    if mode not in ("latest", "history"):
        raise ValueError("无效的外围观察模式")
    cutoff, deadline = _cutoff(day), monotonic()+18
    if mode == "latest":
        cutoff = datetime.now(SHANGHAI).replace(tzinfo=None)
    date_filter = ("context_date=:day AND" if mode == "history" else
        "context_date=(SELECT MAX(context_date) FROM st_external_market_context WHERE context_date<=:today) AND")
    params = {"day":day,"cutoff":cutoff,"today":cutoff.date()}
    # Reuse the collector's batch identity; never combine different capture times.
    latest = _read(get_engine, f"""
        SELECT /*+ MAX_EXECUTION_TIME(2000) */ snapshot_id
        FROM st_external_market_context WHERE {date_filter} captured_at<=:cutoff
        ORDER BY captured_at DESC,id DESC LIMIT 1
    """, params,deadline=deadline,limit=1,stage="global-market-batch")
    rows = _read(get_engine, f"""
        SELECT /*+ MAX_EXECUTION_TIME(3000) */ snapshot_id,captured_at,symbol,display_name,
               price,change_pct,previous_close,market_time,availability,source,payload_json
        FROM st_external_market_context WHERE {date_filter} captured_at<=:cutoff AND snapshot_id=:snapshot
        ORDER BY id DESC LIMIT 42
    """, {**params,"snapshot":latest[0]["snapshot_id"] if latest else ""},deadline=deadline,limit=41,stage="global-market-observation")
    selected = [r for r in rows if r["snapshot_id"]==rows[0]["snapshot_id"]] if rows else []
    symbols = [r["symbol"] for r in selected]
    if len(symbols)!=len(set(symbols)):
        raise ValueError("外围市场快照包含重复标的")
    valid = 0
    for row in selected:
        payload = row.pop("payload_json") or "{}"
        payload = json.loads(payload) if isinstance(payload, str) else payload
        row["missing_reason"] = str(payload.get("error") or "该批次未取得有效报价")[:600] if row["availability"] != "available" else ""
        observed = row["market_time"]
        observed = datetime.fromisoformat(str(observed)) if observed else None
        if observed is not None and observed.tzinfo is not None:
            observed = observed.astimezone(SHANGHAI).replace(tzinfo=None)
        row["availability"] = ("available" if row["availability"]=="available" and
                               row["price"] is not None and row["price"]>0 and observed is not None and
                               observed<=min(cutoff,datetime.fromisoformat(str(row["captured_at"]))) else "missing")
        if row["availability"]!="available":
            row["price"] = row["change_pct"] = None
            row["missing_reason"] = row["missing_reason"] or "报价时间超出本批次范围或缺少有效价格"
        else:
            valid += 1
        row["price"] = float(row["price"]) if row["price"] is not None else None
        row["change_pct"] = float(row["change_pct"]) if row["change_pct"] is not None else None
        row["market_time"] = str(row["market_time"] or "")
        row.pop("previous_close")
    from biz.market_context.external_market import EXTERNAL_MARKET_SYMBOLS
    order = {symbol: i for i, (symbol, _) in enumerate(EXTERNAL_MARKET_SYMBOLS)}
    selected.sort(key=lambda item: order.get(item["symbol"], len(order)))
    producer = None
    if mode == "latest":
        from server.common import market_dashboard_store
        from server.common.trading_day_store import journal_runtime_root
        root = journal_runtime_root() / "external-market"
        try:
            producer = {key: value for key, value in market_dashboard_store.runtime(root=root).items() if key != "token"}
            if selected:
                receipt = market_dashboard_store.component(str(selected[0]["captured_at"])[:10], "external", root=root)
                producer["publication"] = {key: value for key, value in (receipt or {}).items()
                    if key in ("snapshot_id", "persisted_sha256", "status", "captured_at", "context_date", "available_count", "expected_count", "missing_symbols")}
            if (producer.get("lease_until") and producer.get("state") in ("RUNNING", "COMPLETE")
                    and market_dashboard_store.stamp(producer["lease_until"]) < cutoff):
                producer.update(state="FAILED", error="publisher_owner_lease_expired")
        except (OSError, sqlite3.Error, RuntimeError, ValueError):
            producer = {"state": "FAILED", "error": "runtime_metadata_unavailable"}
    return {"trade_date":day,"view_mode":mode,"producer":producer,"status":"available" if valid else "unavailable",
            "snapshot_id":selected[0]["snapshot_id"] if selected else None,
            "known_at":str(selected[0]["captured_at"]) if selected else None,
            "available_count":valid,"expected_count":len(EXTERNAL_MARKET_SYMBOLS),"rows":selected,
            "acquisition_status":"COMPLETE" if valid==len(EXTERNAL_MARKET_SYMBOLS) else "PARTIAL" if valid else "FAILED",
            "method":("最新观察按后台约2分钟采集的实际批次展示，不按全局历史日期回放。" if mode == "latest" else "历史批次只使用所选日期截止时已取得的记录，缺失项不以今天的行情补写。")+
                      "同批次逐项保留市场时间与来源；现货、期货、ETF、汇率及收益率保留实际名称。快照不是逐笔实时行情。",
            "horizon_note":"原快照未记录连续分钟价格，30分钟、1小时、4小时及24小时变化不能据此推算。韩指和台指现货不能冒充相应期货。"}


@router.get("/research/macro-release")
def macro_release(day: str = Query(alias="date",pattern=r"^\d{4}-\d{2}-\d{2}$")):
    return _serve(build_macro_release,day)


@router.get("/research/global-context")
def global_context(day: str = Query(alias="date",pattern=r"^\d{4}-\d{2}-\d{2}$"),
                   mode: str = Query(default="latest", pattern="^(latest|history)$")):
    return _serve(build_global_context,day,mode=mode,cache_seconds=0 if mode == "latest" else 120)
