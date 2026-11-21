"""Current-session vendor limit pools, with explicit receipt-time evidence."""
from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
import hashlib
import json
from time import monotonic

from server.common.eastmoney_upper_limit_history import _fetch_pool
from server.common.market_close_ladder import _pool
from server.common.market_dashboard_store import clock, iso


def capture_live_ladder(selected: str, *, now: datetime, deadline: float) -> dict:
    target = date.fromisoformat(selected)
    if target != clock(now).date():
        raise ValueError("live limit pool requires the current session")
    pools, receipts, evidence, errors = {}, {}, {}, {}
    def capture(kind):
        remaining = deadline-monotonic()
        if remaining < 1:
            raise TimeoutError("live limit pool source deadline exceeded")
        _, raw = _fetch_pool(target, timeout_seconds=min(8, remaining), max_payload_bytes=512*1024, pool_kind=kind)
        received = clock()
        payload = json.loads(raw)
        if str((payload.get("data") or {}).get("qdate")) != target.strftime("%Y%m%d"):
            raise ValueError("live limit pool source session differs")
        rows = _pool(raw, target, observed_at=received, pool_kind=kind)
        return rows, raw, received
    kinds = ("limit_up", "limit_down", "opened_limit_up")
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {kind: pool.submit(capture, kind) for kind in kinds}
        for kind, future in futures.items():
            try:
                rows, raw, received = future.result()
                pools[kind] = rows
                evidence[kind] = raw
                receipts[kind] = {"sample_count": len(rows), "received_at": iso(received),
                                  "source_date": selected, "source_at": None,
                                  "raw_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest()}
            except (OSError, RuntimeError, ValueError) as exc:
                errors[kind] = type(exc).__name__
    if not pools:
        raise RuntimeError("all live limit pool sources unavailable")
    by_streak = defaultdict(list)
    for row in pools.get("limit_up", []):
        by_streak[row["streak"]].append(row["short_name"])
    up = len(pools["limit_up"]) if "limit_up" in pools else None
    opened = len(pools["opened_limit_up"]) if "opened_limit_up" in pools else None
    disjoint = not ({row["stock_code"] for row in pools.get("limit_up", [])}
                    & {row["stock_code"] for row in pools.get("opened_limit_up", [])})
    seal_rate = 100*up/(up+opened) if up is not None and opened is not None and up+opened and disjoint else None
    return {"trade_date": selected, "available_at": iso(clock()), "source_at": None,
            "source": "东财盘中涨跌停池", "scope": "东财当前交易日收录样本；源端更新时间未提供",
            "evidence_mode": "LIVE_OBSERVATION", "count": up, "limit_up_count": up,
            "limit_down_count": len(pools["limit_down"]) if "limit_down" in pools else None,
            "opened_limit_up_count": opened, "max_streak": max(by_streak, default=0) if up is not None else None,
            "seal_rate_pct": seal_rate, "pool_receipts": receipts, "source_errors": errors,
            "rows": [{"streak": streak, "count": len(names), "stocks": names}
                     for streak, names in sorted(by_streak.items(), reverse=True)], "_evidence": evidence}
