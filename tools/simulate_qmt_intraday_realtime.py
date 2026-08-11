from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.api.qmt_live_runtime import _load_tracked_stock_codes
from server.common.batch_db import create_batch_engine
from server.common.process_env import temporary_env
from tools.run_big_qmt_bridge import sync_big_qmt_realtime
from tools.sync_qmt_realtime import sync_qmt_realtime


DEFAULT_CODES = ["000001"]

CURRENT_ROW_COLUMNS = (
    "stock_code",
    "short_name",
    "price",
    "change",
    "change_pct",
    "volume",
    "amount",
    "snapshot_at",
    "received_at",
    "etl_sync_at",
    "data_source",
    "qmt_code",
    "batch_id",
    "permission_status",
    "quality_status",
)


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
        table_columns = {
            str(row[0])
            for row in conn.execute(
                text(
                    """
                    SELECT COLUMN_NAME
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'sm_stock_current'
                    """
                )
            ).fetchall()
        }
        selected_columns = [column for column in CURRENT_ROW_COLUMNS if column in table_columns]
        if "stock_code" not in selected_columns:
            return []
        quoted_columns = ", ".join(f"`{column}`" for column in selected_columns)
        rows = conn.execute(
            text(
                f"""
                SELECT {quoted_columns}
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
        received_at = item.get("received_at") or item.get("etl_sync_at")
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
    engine=None,
) -> dict[str, Any]:
    overrides = {} if use_gateway else {"QMT_GATEWAY_ENABLED": "0"}
    with temporary_env(overrides):
        engine = engine or create_batch_engine(future=True)
        cycle_results: list[dict[str, Any]] = []
        started_at = datetime.now()
        for cycle in range(1, max(1, cycles) + 1):
            cycle_started = time.perf_counter()
            if use_gateway:
                # Explicit compatibility mode for the retired miniQMT path.
                sync_result = sync_qmt_realtime(
                    engine=engine,
                    codes=codes,
                    archive_snapshot=False,
                    run_rt_ddl=False,
                    skip_closed=False,
                    min_coverage=0.0,
                    replace_scope="subset",
                )
            else:
                # Production uses the standard BigQMT in-process strategy and
                # local spool, never a fresh miniQMT worker process.
                sync_result = sync_big_qmt_realtime(engine=engine, codes=codes)
                requested = int(sync_result.get("requested") or len(codes))
                received = int(sync_result.get("received") or sync_result.get("tracked_rows") or 0)
                sync_result["requested"] = requested
                sync_result["received"] = received
                sync_result["coverage"] = round(received / max(requested, 1), 4)
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
            "quote_path": "legacy_miniqmt_gateway" if use_gateway else "standard_bigqmt_spool",
            "note": "snapshot_at comes from QMT source; on weekends/off-hours it may be the last trading timestamp, not a simulated tick.",
            "results": cycle_results,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="Simulate intraday QMT realtime refresh cycles without waiting for market hours.")
    parser.add_argument("--codes", default="", help="Comma-separated stock codes. Empty means portfolio/recommendation universe fallback.")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument(
        "--use-gateway",
        action="store_true",
        help="Compatibility only: test the retired miniQMT gateway instead of the production BigQMT spool.",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    engine = create_batch_engine(future=True)
    codes = _resolve_codes(engine, args.codes, limit=max(1, args.limit))
    result = run_simulation(
        cycles=max(1, args.cycles),
        interval_seconds=max(0.0, args.interval_seconds),
        codes=codes,
        use_gateway=args.use_gateway,
        engine=engine,
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
