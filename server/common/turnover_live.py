"""Dated Eastmoney A-share turnover rankings for the Linux dashboard."""
from contextlib import closing
from datetime import datetime, time, timedelta
import json
import math
import re
import sqlite3
from threading import Lock
from time import monotonic
from zoneinfo import ZoneInfo

from server.common.qmt_attestation_contract import canonical_digest
from server.common.sector_heatmap_store import _path
from tools.crawl_intraday_capital_flow_fast import EASTMONEY_MARKETS
from tools.sync_eastmoney_concept_market import _resolved_curl_json, _page_items

_LOCK = Lock()
_ZONE = ZoneInfo("Asia/Shanghai")
_SOURCE = "eastmoney.push2.f8"


def _fetch(timeout):
    payload = _resolved_curl_json("https://push2delay.eastmoney.com/api/qt/clist/get", {
        "fs": EASTMONEY_MARKETS, "fields": "f12,f14,f2,f3,f6,f8,f124",
        "fid": "f8", "po": 1, "pz": 20, "pn": 1, "fltt": 2, "invt": 2,
    }, host="push2delay.eastmoney.com", timeout=timeout)
    total, rows = _page_items(payload, page=1)
    if payload.get("rc") != 0 or total < 3000 or len(rows) != 20:
        raise ValueError("turnover_live: A-share ranking source coverage differs")
    return {"total": total, "rows": rows}


def _normalize(payload, selected, now):
    anchor = now
    if time(11, 30) < now.time() < time(13):
        anchor = now.replace(hour=11, minute=30, second=0, microsecond=0)
    elif now.time() >= time(15):
        anchor = now.replace(hour=15, minute=0, second=0, microsecond=0)
    rows, stamps = [], []
    raw = payload["rows"]
    codes = [item.get("f12") for item in raw]
    previous = math.inf
    for item in raw:
        try:
            code, name = item["f12"], str(item["f14"]).strip()
            stamp = datetime.fromtimestamp(int(item["f124"]), _ZONE).replace(tzinfo=None)
            price, change, turnover = (float(item[key]) for key in ("f2", "f3", "f8"))
            if (not re.fullmatch(r"\d{6}", code) or codes.count(code) != 1 or not name
                    or not name.isprintable() or len(name) > 128 or stamp.date().isoformat() != selected
                    or stamp.time() < time(9, 25) or stamp > now or (anchor-stamp).total_seconds() > 180
                    or not all(math.isfinite(value) for value in (price, change, turnover))
                    or price <= 0 or turnover < 0 or change <= -100):
                continue
        except (KeyError, ValueError, TypeError, OverflowError, OSError):
            continue
        if turnover > previous:
            raise ValueError("turnover_live: vendor ranking order differs")
        previous = turnover
        stamps.append(stamp)
        rows.append({"stock_code": code, "short_name": name, "trade_date": selected,
                     "price": price, "change_pct": change, "turnover_rate": turnover,
                     "turnover_rate_source": _SOURCE, "source": _SOURCE,
                     "as_of": stamp.replace(tzinfo=_ZONE).isoformat()})
    if not rows:
        raise ValueError("turnover_live: no fresh dated turnover observations")
    return {"rows": rows, "as_of": min(stamps).replace(tzinfo=_ZONE).isoformat(),
            "source": _SOURCE, "trade_date": selected, "provider_total": payload["total"],
            "observed_count": len(raw), "excluded_count": len(raw)-len(rows),
            "scope": "东财A股换手榜有效盘中样本"}


def read_live_turnover(selected, *, now, deadline):
    if selected != now.date().isoformat():
        return None
    remaining = deadline-monotonic()
    if remaining <= 0 or not _LOCK.acquire(timeout=min(4, remaining)):
        raise TimeoutError("turnover_live: capture owner deadline exceeded")
    try:
        with closing(sqlite3.connect(_path(create=True), timeout=1)) as db:
            db.execute("CREATE TABLE IF NOT EXISTS live_turnover_snapshot (slot INTEGER PRIMARY KEY CHECK(slot=1), day TEXT, captured_at TEXT, digest TEXT, payload TEXT)")
            stored = db.execute("SELECT day,captured_at,digest,payload FROM live_turnover_snapshot WHERE slot=1").fetchone()
            if stored and stored[0] == selected and 0 <= (now-datetime.fromisoformat(stored[1])).total_seconds() < 15:
                payload = json.loads(stored[3])
                if canonical_digest(payload) != stored[2]:
                    raise ValueError("turnover_live: persisted digest differs")
                return _normalize(payload, selected, now)
            remaining = deadline-monotonic()
            if remaining < 1:
                raise TimeoutError("turnover_live: source deadline exceeded")
            started = monotonic()
            payload = _fetch(min(3, int(remaining)))
            observed = now+timedelta(seconds=monotonic()-started)
            result = _normalize(payload, selected, observed)
            encoded, digest = json.dumps(payload, allow_nan=False), canonical_digest(payload)
            with db:
                db.execute("INSERT OR REPLACE INTO live_turnover_snapshot VALUES (1,?,?,?,?)", (selected, observed.isoformat(), digest, encoded))
            if db.execute("SELECT digest,payload FROM live_turnover_snapshot WHERE slot=1").fetchone() != (digest, encoded):
                raise RuntimeError("turnover_live: durable readback differs")
            return result
    finally:
        _LOCK.release()
