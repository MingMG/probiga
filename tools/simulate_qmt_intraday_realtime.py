from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.api.qmt_live_runtime import _load_tracked_stock_codes
from server.common.config import get_mysql_url
from tools.sync_qmt_realtime import sync_qmt_realtime


DEFAULT_CODES = ["000001"]


def _resolve_codes(engine, raw_codes: str, *, limit: int) -> list[str]:
    codes = [item.strip().zfill(6) for item in raw_codes.split(",") if item.strip()]
    if codes:
        return codes[:limit] if limit > 0 else codes
    tracked = _load_tracked_stock_codes(engine, max(limit, 20))
    if tracked:
        return tracked[:limit] if limit > 0 else tracked
    return DEFAULT_CODES[:limit] if limit > 0 else DEFAULT_CODES


def _current_rows(engine, codes: list[str]) -> list[dict[str, Any]]:
    if not codes:
        return []
    placeholders = ", ".join(f":code_{idx}" for idx, _ in enumerate(codes))
    params = {f"code_{idx}": code for idx, code in enumerate(codes)}
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT stock_code, short_name, price, `change`, change_pct,
                       volume, amount, snapshot_at, received_at, data_source,
                       qmt_code, batch_id, permission_status, quality_status
                FROM sm_stock_current
                WHERE stock_code IN ({placeholders})
                ORDER BY stock_code
                """
            ),
            params,
        ).mappings().fetchall()
    now = datetime.now()
    today = now.date().isoformat()
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        snapshot_at = item.get("snapshot_at")
        received_at = item.get("received_at")
        snapshot_text = str(snapshot_at) if snapshot_at else ""
        received_latency = None
        if received_at:
            try:
                received_latency = round((now - received_at).total_seconds(), 3)
            except Exception:
                received_latency = None
        item["snapshot_is_today"] = snapshot_text.startswith(today)
        item["received_latency_seconds"] = received_latency
        item["page_live_quote_eligible"] = bool(
            item["snapshot_is_today"] and received_latency is not None and received_latency <= 20
        )
        out.append(item)
    return out


def run_simulation(
    *,
    cycles: int,
    interval_seconds: float,
    codes: list[str],
    use_gateway: bool,
) -> dict[str, Any]:
    if not use_gateway:
        os.environ["QMT_GATEWAY_ENABLED"] = "0"
    engine = create_engine(get_mysql_url(required=True), pool_pre_ping=True, future=True)
    cycle_results: list[dict[str, Any]] = []
    started_at = datetime.now()
    for cycle in range(1, max(1, cycles) + 1):
        cycle_started = time.perf_counter()
        sync_result = sync_qmt_realtime(
            engine=engine,
            codes=codes,
            archive_snapshot=False,
            run_rt_ddl=False,
            skip_closed=False,
            min_coverage=0.0,
            replace_scope="subset",
        )
        rows = _current_rows(engine, codes)
        elapsed_ms = int((time.perf_counter() - cycle_started) * 1000)
        cycle_results.append(
            {
                "cycle": cycle,
                "elapsed_ms": elapsed_ms,
                "sync": sync_result,
                "rows": rows,
            }
        )
        if cycle < cycles and interval_seconds > 0:
            time.sleep(interval_seconds)
    return {
        "status": "success",
        "mode": "simulated_intraday_realtime",
        "started_at": started_at.isoformat(timespec="seconds"),
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "cycles": len(cycle_results),
        "interval_seconds": interval_seconds,
        "codes": codes,
        "gateway_used": use_gateway,
        "note": "snapshot_at comes from QMT source; on weekends/off-hours it may be the last trading timestamp, not a simulated tick.",
        "results": cycle_results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Simulate intraday QMT realtime refresh cycles without waiting for market hours.")
    parser.add_argument("--codes", default="", help="Comma-separated stock codes. Empty means portfolio/recommendation universe fallback.")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--use-gateway", action="store_true", help="Use the persistent QMT gateway instead of a fresh worker process.")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    engine = create_engine(get_mysql_url(required=True), pool_pre_ping=True, future=True)
    codes = _resolve_codes(engine, args.codes, limit=max(1, args.limit))
    result = run_simulation(
        cycles=max(1, args.cycles),
        interval_seconds=max(0.0, args.interval_seconds),
        codes=codes,
        use_gateway=args.use_gateway,
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    else:
        print(f"{result['status']}: cycles={result['cycles']}, codes={','.join(codes)}")
        for item in result["results"]:
            print(
                f"- cycle {item['cycle']}: {item['elapsed_ms']}ms, "
                f"received={item['sync'].get('received')}, coverage={item['sync'].get('coverage')}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
