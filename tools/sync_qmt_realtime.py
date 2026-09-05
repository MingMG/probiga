from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import bindparam, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.stock_market.realtime_quotes import save_to_mysql
from integrations.bigqmt.backend import BigQmtBackend
from integrations.bigqmt.spool import PROVIDER_ID
from integrations.qmt.backend import to_qmt_symbol
from integrations.qmt.safe_upsert import CHINA_STANDARD_TIME, safe_upsert_rows
from server.common.batch_db import create_batch_engine
from server.common.mysql_lock import mysql_named_lock


MAX_CURRENT_AGE_SECONDS = 120.0
MAX_CURRENT_FUTURE_SECONDS = 2.0


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


def _valid_current_price(value: Any) -> bool:
    try:
        price = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return False
    return price.is_finite() and price > 0


def _write_current_table(engine, df: pd.DataFrame, *, replace_scope: str = "all") -> int:
    """Upsert under the existing quote writer lock and verify committed rows.

    Return the verified target count, including a concurrently newer full-QMT
    row retained instead of overwriting it with an older cached quote.
    """
    out = df.copy()
    now = datetime.now(CHINA_STANDARD_TIME).replace(tzinfo=None, microsecond=0)
    batch_id = f"bigqmt_realtime_{now.strftime('%Y%m%d%H%M%S')}"
    out["etl_sync_at"] = now
    out["qmt_code"] = out["stock_code"].astype(str).map(to_qmt_symbol)
    out["source_time"] = pd.to_datetime(out.get("snapshot_at"), errors="coerce")
    out["source_time"] = out["source_time"].where(out["source_time"].notna(), None)
    out["quality_status"] = "PENDING"
    query = text("""
        SELECT stock_code, data_source, source_time, price
        FROM sm_stock_current WHERE stock_code IN :codes
    """).bindparams(bindparam("codes", expanding=True))
    params = {"codes": out["stock_code"].tolist()}
    with mysql_named_lock(engine, "probiga:stock_current", timeout_seconds=1):
        with engine.connect() as conn:
            existing = {str(row["stock_code"]): row for row in
                        conn.execute(query, params).mappings().all()}
        pending = []
        expected = out.to_dict(orient="records")
        for row in expected:
            current = existing.get(row["stock_code"], {})
            current_time = pd.to_datetime(current.get("source_time"), errors="coerce")
            if (current.get("data_source") == PROVIDER_ID
                    and pd.notna(current_time) and current_time > row["source_time"]):
                if (current_time > now + pd.Timedelta(seconds=MAX_CURRENT_FUTURE_SECONDS)
                        or not _valid_current_price(current.get("price"))):
                    raise RuntimeError("Full QMT realtime existing newer row is invalid")
                continue
            pending.append(row)
        if pending:
            result = safe_upsert_rows(
                engine, table_name="sm_stock_current", rows=pending,
                key_columns=["stock_code"], batch_id=batch_id,
                permission_status="SUPPORTED", quality_status="PENDING",
            )
            if result.accepted_rows != len(pending):
                raise RuntimeError("Full QMT realtime accepted-row count differs")
        with engine.connect() as conn:
            observed = {str(row["stock_code"]): row for row in
                        conn.execute(query, params).mappings().all()}
        for row in expected:
            actual = observed.get(row["stock_code"], {})
            actual_time = pd.to_datetime(actual.get("source_time"), errors="coerce")
            if (actual.get("data_source") != PROVIDER_ID or pd.isna(actual_time)
                    or not _valid_current_price(actual.get("price"))
                    or actual_time < row["source_time"]
                    or actual_time > now + pd.Timedelta(seconds=MAX_CURRENT_FUTURE_SECONDS)
                    or (actual_time == row["source_time"]
                        and Decimal(str(actual.get("price"))) != Decimal(str(row["price"])))):
                raise RuntimeError("Full QMT realtime committed readback differs")
    return len(expected)


def _fresh_current_rows(df: pd.DataFrame, codes: list[str], *, now: datetime) -> pd.DataFrame:
    required = {"stock_code", "snapshot_at", "source_time", "data_source", "price", "volume", "amount"}
    if not required.issubset(df.columns) or not df["data_source"].eq(PROVIDER_ID).all():
        raise RuntimeError("Full QMT realtime native source contract differs")
    out = df.copy()
    out["stock_code"] = out["stock_code"].astype(str).str.zfill(6)
    if not out["stock_code"].isin(codes).all() or out["stock_code"].duplicated().any():
        raise RuntimeError("Full QMT realtime requested stock set differs")
    source = pd.to_datetime(out["source_time"], errors="coerce")
    snapshot = pd.to_datetime(out["snapshot_at"], errors="coerce")
    age = (pd.Timestamp(now) - source).dt.total_seconds()
    valid = (source.eq(snapshot) & source.dt.date.eq(now.date())
             & age.between(-MAX_CURRENT_FUTURE_SECONDS, MAX_CURRENT_AGE_SECONDS))
    for field in ("price", "volume", "amount"):
        numeric = pd.to_numeric(out[field], errors="coerce")
        valid &= numeric.ge(0) & numeric.lt(float("inf"))
        if field == "price":
            valid &= numeric.gt(0)
    return out.loc[valid].reset_index(drop=True)


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
    if not 0 < float(min_coverage) <= 1:
        raise ValueError("min_coverage must be greater than 0 and at most 1")
    engine = engine or create_batch_engine(future=True)
    from tools.crawl_realtime_batch import is_trading_time

    if skip_closed and not is_trading_time(engine):
        return {
            "status": "skipped",
            "reason": "market_closed",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }

    selected_codes = list(dict.fromkeys(
        str(code).strip().zfill(6) for code in (codes or []) if str(code).strip()
    ))
    if not selected_codes:
        selected_codes = _read_codes(engine, limit)
    if not selected_codes:
        raise RuntimeError("no stock codes available from si_all_code")

    short_name_map = _load_short_name_map(engine)
    backend = BigQmtBackend()
    df = backend.fetch_current(
        selected_codes, short_name_map=short_name_map,
        max_age_seconds=MAX_CURRENT_AGE_SECONDS, require_native_source_time=True,
    )
    if df is None or df.empty:
        raise RuntimeError("Full QMT returned no realtime rows with native timestamps")

    df = _fresh_current_rows(
        df, selected_codes,
        now=datetime.now(CHINA_STANDARD_TIME).replace(tzinfo=None),
    )
    distinct_count = int(df["stock_code"].nunique())
    coverage = distinct_count / max(len(selected_codes), 1)
    if distinct_count == 0 or coverage < min_coverage:
        raise RuntimeError(
            f"QMT realtime coverage below threshold: {distinct_count}/{len(selected_codes)} ({coverage:.1%})"
        )

    written_current = _write_current_table(engine, df, replace_scope=replace_scope)
    if written_current != distinct_count:
        raise RuntimeError("Full QMT realtime verified target count differs")
    written_snapshot = 0
    if archive_snapshot:
        snapshot_cols = ["stock_code", "short_name", "price", "change", "change_pct", "volume", "amount"]
        written_snapshot = save_to_mysql(
            df[snapshot_cols],
            run_ddl=run_rt_ddl,
            engine=engine,
        )

    return {
        "status": "success",
        "data_source": PROVIDER_ID,
        "transport": "FULL_QMT_LOCAL_SNAPSHOT",
        "quality_status": "PENDING",
        "current_rows": written_current,
        "verified_current_rows": written_current,
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
