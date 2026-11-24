"""Complete bounded bulk capture of source-specific intraday liquidity samples."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing
from datetime import datetime, time, timedelta
import json
import math
import sqlite3
from statistics import median
from threading import Lock
from time import monotonic
from zoneinfo import ZoneInfo

from server.common.market_dashboard_store import EXPIRED_SECONDS
from server.common.qmt_attestation_contract import canonical_digest
from server.common.sector_heatmap_store import _path
from tools.crawl_intraday_capital_flow_fast import EASTMONEY_MARKETS
from tools.sync_eastmoney_concept_market import _resolved_curl_json, _page_items

_LOCK = Lock()
_ZONE = ZoneInfo("Asia/Shanghai")


def _fetch(deadline, *, selected, now):
    started = monotonic()
    def page(number):
        failure = None
        for attempt in range(2):
            remaining = deadline-monotonic()
            if remaining < 1:
                raise TimeoutError(f"liquidity_live: page {number} deadline exceeded")
            try:
                payload = _resolved_curl_json("https://push2delay.eastmoney.com/api/qt/clist/get", {
                    "fs": EASTMONEY_MARKETS, "fields": "f12,f2,f8,f10,f124",
                    "fid": "f12", "po": 1, "pz": 100, "pn": number, "np": 1, "fltt": 2, "invt": 2,
                }, host="push2delay.eastmoney.com", timeout=min(2, int(remaining)))
                if payload.get("rc") != 0:
                    raise ValueError("source rejected request")
                return _page_items(payload, page=number)
            except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
                failure = exc
        raise RuntimeError(f"liquidity_live: page {number} failed after two attempts") from failure
    total, first = page(1)
    if not 3000 <= total <= 7000 or len(first) != 100:
        raise ValueError("liquidity_live: A-share source inventory differs")
    with closing(sqlite3.connect(_path(create=True), timeout=1)) as db:
        db.execute("""CREATE TABLE IF NOT EXISTS live_liquidity_page (
            page INTEGER PRIMARY KEY, day TEXT, inventory INTEGER, captured_at TEXT, digest TEXT, payload TEXT)""")
        pages = {1: first}
        with db:
            db.execute("DELETE FROM live_liquidity_page WHERE day<>? OR inventory<>? OR captured_at<?",
                       (selected, total, (now-timedelta(seconds=30)).isoformat()))
        for number, captured, digest, encoded in db.execute(
                "SELECT page,captured_at,digest,payload FROM live_liquidity_page WHERE day=? AND inventory=?",
                (selected, total)).fetchall():
            if 1 < number <= (total+99)//100 and 0 <= (now-datetime.fromisoformat(captured)).total_seconds() <= 30:
                items = json.loads(encoded)
                if canonical_digest(items) != digest:
                    raise ValueError("liquidity_live: partial page digest differs")
                if len(items) == min(100, total-(number-1)*100):
                    pages[number] = items
        failures = []
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {pool.submit(page, number): number for number in range(2, (total+99)//100+1) if number not in pages}
            for future in as_completed(futures):
                number = futures[future]
                try:
                    count, items = future.result()
                    if count != total or len(items) != min(100, total-(number-1)*100):
                        raise ValueError("pagination coverage differs")
                    encoded, digest = json.dumps(items, allow_nan=False), canonical_digest(items)
                    with db:
                        db.execute("INSERT OR REPLACE INTO live_liquidity_page VALUES (?,?,?,?,?,?)",
                                   (number, selected, total, (now+timedelta(seconds=monotonic()-started)).isoformat(), digest, encoded))
                    pages[number] = items
                except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
                    failures.append(number)
        if failures:
            raise RuntimeError(f"liquidity_live: missing pages {sorted(failures)}; completed pages checkpointed")
        rows = [row for number in sorted(pages) for row in pages[number]]
        codes = [row.get("f12") for row in rows]
        if len(rows) != total or len(set(codes)) != total:
            raise ValueError("liquidity_live: duplicate or missing source identities")
        # Successful captures start a new observation; only failed runs resume pages.
        with db:
            db.execute("DELETE FROM live_liquidity_page")
    return rows


def _normalize(rows, selected, now):
    anchor = now
    if time(11, 30) < now.time() < time(13):
        anchor = now.replace(hour=11, minute=30, second=0, microsecond=0)
    elif now.time() >= time(15):
        anchor = now.replace(hour=15, minute=0, second=0, microsecond=0)
    rates, ratios, stamps = [], [], []
    dated_count = 0
    for row in rows:
        try:
            code = row["f12"]
            stamp = datetime.fromtimestamp(int(row["f124"]), _ZONE).replace(tzinfo=None)
            price = float(row["f2"])
            if (not isinstance(code, str) or not code.isascii() or len(code) != 6 or not code.isdecimal()
                    or isinstance(row["f2"], bool) or not math.isfinite(price) or price <= 0 or stamp.date().isoformat() != selected
                    or stamp.time() < time(9, 25) or stamp > now or (anchor-stamp).total_seconds() > EXPIRED_SECONDS):
                continue
        except (KeyError, ValueError, TypeError, OverflowError, OSError):
            continue
        dated_count += 1
        contributes = False
        for field, values in (("f8", rates), ("f10", ratios)):
            try:
                if isinstance(row[field], bool):
                    continue
                value = float(row[field])
                if math.isfinite(value) and value >= 0:
                    values.append(value)
                    contributes = True
            except (KeyError, ValueError, TypeError):
                pass
        if contributes:
            stamps.append(stamp)
    if len(stamps) < math.ceil(len(rows)*0.9) or (not rates and not ratios):
        raise ValueError("liquidity_live: insufficient current-session source coverage")
    return {"status": "available", "requested_date": selected, "trade_date": selected,
            "mode": "intraday", "source": "eastmoney.push2.f8_f10",
            "as_of": min(stamps).replace(tzinfo=_ZONE).isoformat(),
            "source_time_min": min(stamps).replace(tzinfo=_ZONE).isoformat(),
            "source_time_max": max(stamps).replace(tzinfo=_ZONE).isoformat(),
            "turnover_median_pct": median(rates) if rates else None, "turnover_sample_count": len(rates),
            "volume_ratio_median": median(ratios) if ratios else None, "volume_sample_count": len(ratios),
            "provider_total": len(rows), "dated_quote_count": dated_count,
            "fresh_quote_count": len(stamps), "excluded_quote_count": len(rows)-len(stamps),
            "freshness_budget_seconds": EXPIRED_SECONDS,
            "definition": "东财当日60秒内有效贡献样本：逐股换手率、源端量比的中位数"}


def read_live_liquidity(selected, *, now, deadline):
    started = monotonic()
    remaining = deadline-monotonic()
    if selected != now.date().isoformat() or remaining <= 0 or not _LOCK.acquire(timeout=min(18, remaining)):
        raise TimeoutError("liquidity_live: capture owner/date deadline exceeded")
    try:
        now += timedelta(seconds=monotonic()-started)
        with closing(sqlite3.connect(_path(create=True), timeout=1)) as db:
            db.execute("CREATE TABLE IF NOT EXISTS live_liquidity_snapshot (slot INTEGER PRIMARY KEY CHECK(slot=1), day TEXT, captured_at TEXT, digest TEXT, payload TEXT)")
            stored = db.execute("SELECT day,captured_at,digest,payload FROM live_liquidity_snapshot WHERE slot=1").fetchone()
            if stored and stored[0] == selected and 0 <= (now-datetime.fromisoformat(stored[1])).total_seconds() < 15:
                rows = json.loads(stored[3])
                if canonical_digest(rows) != stored[2]:
                    raise ValueError("liquidity_live: persisted digest differs")
                return _normalize(rows, selected, now)
            started = monotonic()
            rows = _fetch(deadline, selected=selected, now=now)
            observed = now+timedelta(seconds=monotonic()-started)
            result = _normalize(rows, selected, observed)
            encoded, digest = json.dumps(rows, allow_nan=False), canonical_digest(rows)
            with db:
                db.execute("INSERT OR REPLACE INTO live_liquidity_snapshot VALUES (1,?,?,?,?)", (selected, observed.isoformat(), digest, encoded))
            if db.execute("SELECT digest,payload FROM live_liquidity_snapshot WHERE slot=1").fetchone() != (digest, encoded):
                raise RuntimeError("liquidity_live: durable readback differs")
            return result
    finally:
        _LOCK.release()
