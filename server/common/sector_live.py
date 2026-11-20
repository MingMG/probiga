"""Bounded, demand-driven Eastmoney board observations for the Linux dashboard.

The visible dashboard renews these complete bulk snapshots every 20 seconds. They
are private observations, never published as completed daily acquisition rows.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, time, timedelta
import json
import re
import sqlite3
from threading import Lock
from time import monotonic
from zoneinfo import ZoneInfo

from server.common.qmt_attestation_contract import canonical_digest
from server.common.sector_heatmap_store import _path
from tools.fetch_sector_heat_east_daily import _source_timestamp

_LOCK = Lock()
_ZONE = ZoneInfo("Asia/Shanghai")


def _validate(rows, selected, now):
    if not isinstance(rows, list) or not 1 <= len(rows) <= 1500:
        raise ValueError("sector_live: invalid row count")
    codes = set()
    for row in rows:
        stamp = _source_timestamp(row.get("f124"))
        if (not re.fullmatch(r"BK[0-9]{4,8}", str(row.get("f12", "")))
                or row["f12"] in codes or not isinstance(row.get("f14"), str)
                or not row["f14"].strip() or stamp is None
                or stamp.date().isoformat() != selected or stamp > now
                or stamp.time() < time(9, 25)):
            raise ValueError("sector_live: board identity or source date differs")
        codes.add(row["f12"])
    return rows


def _fetch(kind, selected, now, deadline):
    from tools.sync_eastmoney_concept_market import _resolved_curl_json, _page_items, DIRECTORY_URL

    started = monotonic()

    def page(number):
        remaining = deadline - monotonic()
        if remaining < 1:
            raise TimeoutError("sector_live: bulk source deadline exceeded")
        payload = _resolved_curl_json(DIRECTORY_URL, {
            "fs": f"m:90+t:{kind}", "fields": "f12,f14,f2,f3,f4,f6,f124,f62,f66,f72",
            "pn": number, "pz": 100, "fid": "f12", "po": 1, "np": 1, "fltt": 2,
        }, host="push2delay.eastmoney.com", timeout=min(3, int(remaining)))
        if payload.get("rc") != 0:
            raise ValueError("sector_live: source rejected bulk request")
        return _page_items(payload, page=number)

    total, rows = page(1)
    if not 1 <= total <= 1500 or len(rows) != min(total, 100):
        raise ValueError("sector_live: incomplete first page")
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [(number, pool.submit(page, number)) for number in range(2, (total + 99) // 100 + 1)]
        for number, future in futures:
            count, items = future.result()
            if count != total or len(items) != min(100, total - (number - 1) * 100):
                raise ValueError("sector_live: pagination coverage differs")
            rows.extend(items)
    return _validate(rows, selected, now + timedelta(seconds=monotonic() - started))


def read_live_boards(selected: str, *, now: datetime) -> dict:
    """Reuse a verified 15-second checkpoint; refresh each source independently."""
    started = monotonic()
    now = now.astimezone(_ZONE).replace(tzinfo=None) if now.tzinfo else now
    if selected != now.date().isoformat():
        raise ValueError("sector_live: only the current session may be captured")
    if not _LOCK.acquire(timeout=12):
        raise TimeoutError("sector_live: capture owner deadline exceeded")
    try:
        now += timedelta(seconds=monotonic()-started)
        with closing(sqlite3.connect(_path(create=True), timeout=2)) as db:
            db.execute("CREATE TABLE IF NOT EXISTS live_board_snapshot (kind TEXT PRIMARY KEY, day TEXT, captured_at TEXT, digest TEXT, payload TEXT)")
            result, missing = {}, []
            for kind in (3, 2):
                stored = db.execute("SELECT day,captured_at,digest,payload FROM live_board_snapshot WHERE kind=?", (str(kind),)).fetchone()
                if stored and stored[0] == selected and 0 <= (now - datetime.fromisoformat(stored[1])).total_seconds() < 15:
                    rows = json.loads(stored[3])
                    if canonical_digest(rows) != stored[2]:
                        raise ValueError("sector_live: persisted digest differs")
                    result[kind] = _validate(rows, selected, now)
                else:
                    missing.append(kind)
            deadline = monotonic() + 10
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = {kind: pool.submit(_fetch, kind, selected, now, deadline) for kind in missing}
                for kind, future in futures.items():
                    try:
                        rows = future.result()
                    except (RuntimeError, TimeoutError, ValueError) as exc:
                        result[f"error_{kind}"] = str(exc)
                        continue
                    payload = json.dumps(rows, ensure_ascii=False, allow_nan=False)
                    digest = canonical_digest(rows)
                    with db:
                        db.execute("INSERT OR REPLACE INTO live_board_snapshot VALUES (?,?,?,?,?)", (str(kind), selected, now.isoformat(), digest, payload))
                    check = db.execute("SELECT digest,payload FROM live_board_snapshot WHERE kind=?", (str(kind),)).fetchone()
                    if check != (digest, payload):
                        raise RuntimeError("sector_live: durable readback differs")
                    result[kind] = rows
            return result
    finally:
        _LOCK.release()
