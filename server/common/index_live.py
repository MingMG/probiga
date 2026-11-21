"""Linux dashboard index observations with dated, verifiable source timestamps."""
from contextlib import closing
from datetime import datetime, time, timedelta
import json
import math
import sqlite3
from threading import Lock
from time import monotonic
from zoneinfo import ZoneInfo

from server.common.qmt_attestation_contract import canonical_digest
from server.common.sector_heatmap_store import _path
from tools.sync_eastmoney_concept_market import _resolved_curl_json

_LOCK = Lock()
_ZONE = ZoneInfo("Asia/Shanghai")


def _fetch(names, timeout):
    payload = _resolved_curl_json("https://push2delay.eastmoney.com/api/qt/ulist.np/get", {
        "secids": ",".join(f"{0 if code.startswith('399') else 1}.{code}" for code in names),
        "fields": "f12,f13,f14,f2,f3,f6,f18,f124", "fltt": 2, "invt": 2,
    }, host="push2delay.eastmoney.com", timeout=timeout)
    data = payload.get("data") or {}
    rows = data.get("diff")
    if (payload.get("rc") != 0 or not isinstance(rows, list)
            or not 0 < len(rows) <= len(names) or data.get("total") != len(rows)):
        raise ValueError("index_live: invalid bulk response coverage")
    return rows


def _normalize(rows, names, selected, now):
    anchor = now
    if time(11, 30) < now.time() < time(13):
        anchor = now.replace(hour=11, minute=30, second=0, microsecond=0)
    elif now.time() >= time(15):
        anchor = now.replace(hour=15, minute=0, second=0, microsecond=0)
    result = []
    codes = [row.get("f12") for row in rows]
    for row in rows:
        code = row.get("f12")
        if code not in names or codes.count(code) != 1 or row.get("f13") != (0 if code.startswith("399") else 1):
            continue
        try:
            stamp = datetime.fromtimestamp(int(row["f124"]), _ZONE).replace(tzinfo=None)
            price, previous, change = (float(row[key]) for key in ("f2", "f18", "f3"))
            if (stamp.date().isoformat() != selected or stamp > now or stamp.time() < time(9, 25)
                    or (anchor - stamp).total_seconds() > 180
                    or not all(math.isfinite(value) for value in (price, previous, change))
                    or min(price, previous) <= 0 or abs((price / previous - 1) * 100 - change) > 0.011):
                continue
        except (KeyError, TypeError, ValueError, OverflowError, OSError):
            continue
        result.append({"index_code": code, "index_name": names[code], "trade_date": selected,
                       "price": price, "change_pct": change, "as_of": stamp.replace(tzinfo=_ZONE).isoformat(),
                       "source": "eastmoney.push2.index", "evidence_mode": "LIVE_OBSERVATION"})
    return result


def read_live_indices(names, selected, *, now, deadline):
    """One bounded bulk request, or a 15-second verified durable checkpoint."""
    started = monotonic()
    if selected != now.date().isoformat():
        return []
    remaining = deadline - monotonic()
    if remaining <= 0 or not _LOCK.acquire(timeout=min(5, remaining)):
        raise TimeoutError("index_live: capture owner deadline exceeded")
    try:
        now += timedelta(seconds=monotonic()-started)
        with closing(sqlite3.connect(_path(create=True), timeout=1)) as db:
            db.execute("CREATE TABLE IF NOT EXISTS live_index_snapshot (slot INTEGER PRIMARY KEY CHECK(slot=1), day TEXT, captured_at TEXT, digest TEXT, payload TEXT)")
            stored = db.execute("SELECT day,captured_at,digest,payload FROM live_index_snapshot WHERE slot=1").fetchone()
            if stored and stored[0] == selected and 0 <= (now - datetime.fromisoformat(stored[1])).total_seconds() < 15:
                rows = json.loads(stored[3])
                if canonical_digest(rows) != stored[2]:
                    raise ValueError("index_live: persisted digest differs")
                return _normalize(rows, names, selected, now)
            remaining = deadline - monotonic()
            if remaining < 1:
                raise TimeoutError("index_live: source deadline exceeded")
            started = monotonic()
            rows = _fetch(names, min(3, int(remaining)))
            observed_at = now + timedelta(seconds=monotonic() - started)
            normalized = _normalize(rows, names, selected, observed_at)
            if not normalized:
                raise ValueError("index_live: no fresh, dated index observations")
            payload, digest = json.dumps(rows, allow_nan=False), canonical_digest(rows)
            with db:
                db.execute("INSERT OR REPLACE INTO live_index_snapshot VALUES (1,?,?,?,?)", (selected, observed_at.isoformat(), digest, payload))
            if db.execute("SELECT digest,payload FROM live_index_snapshot WHERE slot=1").fetchone() != (digest, payload):
                raise RuntimeError("index_live: durable readback differs")
            return normalized
    finally:
        _LOCK.release()
