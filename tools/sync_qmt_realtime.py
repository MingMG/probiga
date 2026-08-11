from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.stock_market.realtime_quotes import save_to_mysql
from integrations.qmt import QmtBackend
from integrations.qmt.backend import to_qmt_symbol
from integrations.qmt.safe_upsert import safe_upsert_rows
from server.common.batch_db import create_batch_engine
from tools.crawl_realtime_batch import is_trading_time


def _read_codes(engine, limit: int) -> list[str]:
    sql = "SELECT stock_code FROM si_all_code WHERE stock_code REGEXP '^(0|3|4|6|8|9)' ORDER BY stock_code"
    if limit > 0:
        sql += " LIMIT :limit"
        params = {"limit": limit}
    else:
        params = {}
    with engine.connect() as conn:
        return [str(row[0]).strip().zfill(6) for row in conn.execute(text(sql), params).fetchall()]


def _load_short_name_map(engine) -> dict[str, str]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT stock_code, short_name FROM si_all_code")).fetchall()
    return {str(code).strip().zfill(6): (str(name or "").strip()) for code, name in rows}


def _write_current_table(engine, df: pd.DataFrame, *, replace_scope: str = "all") -> int:
    out = df.copy()
    now = datetime.now().replace(microsecond=0)
    batch_id = f"qmt_realtime_{now.strftime('%Y%m%d%H%M%S')}"
    out["etl_sync_at"] = now
    out["qmt_code"] = out["stock_code"].astype(str).map(to_qmt_symbol)
    out["source_time"] = pd.to_datetime(out.get("snapshot_at"), errors="coerce")
    out["source_time"] = out["source_time"].where(out["source_time"].notna(), None)
    result = safe_upsert_rows(
        engine,
        table_name="sm_stock_current",
        rows=out.to_dict(orient="records"),
        key_columns=["stock_code"],
        batch_id=batch_id,
        permission_status="SUPPORTED",
        quality_status="PENDING",
    )
    return result.accepted_rows


def sync_qmt_realtime(
    *,
    engine=None,
    codes: list[str] | None = None,
    limit: int = 0,
    archive_snapshot: bool = True,
    run_rt_ddl: bool = True,
    min_coverage: float = 0.60,
    skip_closed: bool = True,
    replace_scope: str = "all",
) -> dict[str, Any]:
    engine = engine or create_batch_engine(future=True)
    if skip_closed and not is_trading_time(engine):
        return {
            "status": "skipped",
            "reason": "market_closed",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }

    selected_codes = [str(code).strip().zfill(6) for code in (codes or []) if str(code).strip()]
    if not selected_codes:
        selected_codes = _read_codes(engine, limit)
    if not selected_codes:
        raise RuntimeError("no stock codes available from si_all_code")

    short_name_map = _load_short_name_map(engine)
    backend = QmtBackend()
    df = backend.fetch_current(selected_codes, short_name_map=short_name_map)
    if df is None or df.empty:
        raise RuntimeError("QMT returned no realtime rows")

    df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)
    distinct_count = int(df["stock_code"].nunique())
    coverage = distinct_count / max(len(selected_codes), 1)
    if coverage < min_coverage:
        raise RuntimeError(
            f"QMT realtime coverage below threshold: {distinct_count}/{len(selected_codes)} ({coverage:.1%})"
        )

    written_current = _write_current_table(engine, df, replace_scope=replace_scope)
    written_snapshot = 0
    if archive_snapshot:
        snapshot_cols = ["stock_code", "short_name", "price", "change", "change_pct", "volume", "amount"]
        written_snapshot = save_to_mysql(df[snapshot_cols], run_ddl=run_rt_ddl, engine=engine)

    return {
        "status": "success",
        "current_rows": written_current,
        "snapshot_rows": written_snapshot,
        "requested": len(selected_codes),
        "received": distinct_count,
        "coverage": round(coverage, 4),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync realtime stock quotes from QMT")
    parser.add_argument("--codes", default="", help="comma-separated stock codes; empty means full si_all_code universe")
    parser.add_argument("--limit", type=int, default=0, help="limit si_all_code rows when --codes is empty")
    parser.add_argument("--min-coverage", type=float, default=0.60)
    parser.add_argument("--no-archive-snapshot", action="store_true")
    parser.add_argument("--no-rt-ddl", action="store_true")
    parser.add_argument("--no-skip-closed", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    codes = [item.strip() for item in args.codes.split(",") if item.strip()]
    result = sync_qmt_realtime(
        codes=codes,
        limit=args.limit,
        archive_snapshot=not args.no_archive_snapshot,
        run_rt_ddl=not args.no_rt_ddl,
        min_coverage=args.min_coverage,
        skip_closed=not args.no_skip_closed,
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, default=str))
    else:
        print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
