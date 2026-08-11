from __future__ import annotations

"""Synchronize realtime quotes without depending on miniQMT.

Small, explicitly selected universes (portfolio, positions and candidates) use
Sina's batch quote endpoint and are upserted without replacing the full-market
snapshot.  A full-universe run delegates to the existing Eastmoney snapshot
job, which already performs coverage validation and an atomic table swap.
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.stock_market.realtime_quotes import fetch_list_market_current, save_to_mysql
from integrations.qmt.safe_upsert import safe_upsert_rows
from server.common.batch_db import create_batch_engine
from tools.crawl_realtime_batch import is_trading_time, refresh_snapshot


def _read_codes(engine, limit: int) -> list[str]:
    sql = "SELECT stock_code FROM si_all_code WHERE stock_code REGEXP '^(0|3|4|6|8|9)' ORDER BY stock_code"
    params: dict[str, Any] = {}
    if limit > 0:
        sql += " LIMIT :limit"
        params["limit"] = int(limit)
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()
    return [str(row[0]).strip().zfill(6) for row in rows if str(row[0] or "").strip()]


def _load_short_name_map(engine) -> dict[str, str]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT stock_code, short_name FROM si_all_code")).fetchall()
    return {
        str(code).strip().zfill(6): str(name or "").strip()
        for code, name in rows
        if str(code or "").strip()
    }


def _chunks(items: list[str], size: int):
    for offset in range(0, len(items), size):
        yield items[offset : offset + size]


def _fetch_sina_quotes(codes: list[str], short_name_map: dict[str, str]) -> pd.DataFrame:
    batch_size = max(20, int(os.environ.get("SINA_CURRENT_BATCH_SIZE", "300")))
    timeout_seconds = max(2.0, float(os.environ.get("SINA_CURRENT_TIMEOUT_SECONDS", "8")))
    now = datetime.now().replace(microsecond=0)
    parts: list[pd.DataFrame] = []

    for batch in _chunks(codes, batch_size):
        frame = fetch_list_market_current(batch, timeout_seconds=timeout_seconds)
        if frame is None or frame.empty:
            continue
        frame = frame.copy()
        frame["stock_code"] = frame["stock_code"].astype(str).str.strip().str.zfill(6)
        frame["short_name"] = frame["short_name"].fillna("").astype(str).str.strip()
        missing_name = frame["short_name"].eq("")
        frame.loc[missing_name, "short_name"] = frame.loc[missing_name, "stock_code"].map(short_name_map).fillna("")
        for column in ("price", "change", "change_pct", "volume", "amount"):
            frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
        valid = (
            frame["price"].notna()
            & frame["price"].gt(0)
            & frame["volume"].fillna(0).ge(0)
            & frame["amount"].fillna(0).ge(0)
        )
        frame = frame.loc[valid].copy()
        if frame.empty:
            continue
        frame["snapshot_at"] = now
        parts.append(frame)

    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True).drop_duplicates(subset=["stock_code"], keep="last")


def _write_current_subset(engine, frame: pd.DataFrame, *, source: str) -> int:
    now = datetime.now().replace(microsecond=0)
    out = frame.copy()
    out["etl_sync_at"] = now
    out["qmt_code"] = None
    out["data_source"] = source
    out["source_time"] = pd.to_datetime(out["snapshot_at"], errors="coerce")
    out["received_at"] = now
    batch_id = f"{source}_realtime_{now.strftime('%Y%m%d%H%M%S')}"
    result = safe_upsert_rows(
        engine,
        table_name="sm_stock_current",
        rows=out.to_dict(orient="records"),
        key_columns=["stock_code"],
        batch_id=batch_id,
        permission_status="PUBLIC",
        quality_status="VALIDATED",
    )
    return result.accepted_rows


def sync_market_realtime(
    *,
    engine=None,
    codes: list[str] | None = None,
    limit: int = 0,
    source: str = "auto",
    archive_snapshot: bool = True,
    run_rt_ddl: bool = True,
    min_coverage: float = 0.60,
    skip_closed: bool = True,
    replace_scope: str = "subset",
) -> dict[str, Any]:
    """Refresh current quotes from public sources.

    ``replace_scope`` is retained for call-site compatibility.  Explicit code
    lists are always safely upserted; they can never truncate the full-market
    snapshot.
    """

    del replace_scope
    engine = engine or create_batch_engine(future=True)
    if skip_closed and not is_trading_time(engine):
        return {
            "status": "skipped",
            "reason": "market_closed",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }

    explicit_codes = list(dict.fromkeys(
        str(code).strip().zfill(6) for code in (codes or []) if str(code).strip()
    ))
    selected_source = str(source or "auto").strip().lower()
    if selected_source == "auto":
        selected_source = "sina" if explicit_codes or limit > 0 else "eastmoney"

    if selected_source in {"east", "eastmoney", "em"} and not explicit_codes and limit <= 0:
        written = refresh_snapshot(
            engine,
            min_coverage=min_coverage,
            archive_snapshot=archive_snapshot,
        )
        if written <= 0:
            raise RuntimeError("Eastmoney returned no valid realtime rows")
        return {
            "status": "success",
            "source": "eastmoney",
            "current_rows": written,
            "snapshot_rows": written if archive_snapshot else 0,
            "requested": written,
            "received": written,
            "coverage": 1.0,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }

    if selected_source != "sina":
        raise ValueError(f"unsupported realtime source for a subset refresh: {selected_source}")

    selected_codes = explicit_codes or _read_codes(engine, limit)
    if not selected_codes:
        raise RuntimeError("no stock codes available for realtime synchronization")

    frame = _fetch_sina_quotes(selected_codes, _load_short_name_map(engine))
    if frame.empty:
        raise RuntimeError("Sina returned no valid realtime rows")
    received = int(frame["stock_code"].nunique())
    coverage = received / max(len(selected_codes), 1)
    if coverage < max(0.0, min(1.0, float(min_coverage))):
        raise RuntimeError(
            f"Sina realtime coverage below threshold: {received}/{len(selected_codes)} ({coverage:.1%})"
        )

    written_current = _write_current_subset(engine, frame, source="sina")
    written_snapshot = 0
    if archive_snapshot:
        snapshot_columns = ["stock_code", "short_name", "price", "change", "change_pct", "volume", "amount"]
        written_snapshot = save_to_mysql(
            frame[snapshot_columns],
            run_ddl=run_rt_ddl,
            engine=engine,
        )

    return {
        "status": "success",
        "source": "sina",
        "current_rows": written_current,
        "snapshot_rows": written_snapshot,
        "requested": len(selected_codes),
        "received": received,
        "coverage": round(coverage, 4),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync realtime stock quotes without miniQMT")
    parser.add_argument("--codes", default="", help="comma-separated stock codes; empty means a full-market refresh")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--source", choices=["auto", "sina", "eastmoney"], default="auto")
    parser.add_argument("--min-coverage", type=float, default=0.60)
    parser.add_argument("--no-archive-snapshot", action="store_true")
    parser.add_argument("--no-rt-ddl", action="store_true")
    parser.add_argument("--no-skip-closed", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    result = sync_market_realtime(
        codes=[item.strip() for item in args.codes.split(",") if item.strip()],
        limit=args.limit,
        source=args.source,
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
