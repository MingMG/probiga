from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt import bridge
from integrations.qmt.backend import to_qmt_symbol
from integrations.qmt.info import CORE_INDEXES, to_qmt_index_symbol
from integrations.qmt.safe_upsert import safe_upsert_rows
from integrations.qmt.sectors import fetch_sector_datasets
from server.common.config import get_mysql_url


PROVIDER_ID = "gj_qmt"


def _quote(identifier: str) -> str:
    value = str(identifier or "").strip()
    if not value.replace("_", "").isalnum():
        raise ValueError(f"unsafe identifier: {identifier!r}")
    return f"`{value}`"


def _table_columns(engine: Engine, table_name: str) -> set[str]:
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        return set()
    return {str(column["name"]) for column in inspector.get_columns(table_name)}


def _ensure_column(engine: Engine, table_name: str, column_name: str, ddl: str) -> None:
    if column_name in _table_columns(engine, table_name):
        return
    with engine.begin() as conn:
        conn.execute(text(f"ALTER TABLE {_quote(table_name)} ADD COLUMN {_quote(column_name)} {ddl}"))


def ensure_reference_tables(engine: Engine) -> None:
    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS qmt_sector_list (
            sector_name VARCHAR(191) NOT NULL PRIMARY KEY,
            source VARCHAR(32) NOT NULL DEFAULT 'qmt',
            etl_sync_at DATETIME NULL,
            data_source VARCHAR(32) NULL,
            received_at DATETIME NULL,
            batch_id VARCHAR(64) NULL,
            quality_status VARCHAR(32) NULL,
            permission_status VARCHAR(32) NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS qmt_sector_member (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            sector_name VARCHAR(191) NOT NULL,
            stock_code VARCHAR(64) NOT NULL,
            qmt_code VARCHAR(64) NOT NULL DEFAULT '',
            exchange VARCHAR(16) NOT NULL DEFAULT '',
            etl_sync_at DATETIME NULL,
            data_source VARCHAR(32) NULL,
            received_at DATETIME NULL,
            batch_id VARCHAR(64) NULL,
            quality_status VARCHAR(32) NULL,
            permission_status VARCHAR(32) NULL,
            UNIQUE KEY uk_qmt_sector_member (sector_name, stock_code),
            KEY idx_qmt_sector_member_stock (stock_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS qmt_instrument_detail (
            qmt_code VARCHAR(64) NOT NULL PRIMARY KEY,
            stock_code VARCHAR(64) NOT NULL,
            exchange VARCHAR(16) NULL,
            short_name VARCHAR(128) NULL,
            list_date DATE NULL,
            expire_date DATE NULL,
            pre_close DECIMAL(20, 6) NULL,
            up_stop_price DECIMAL(20, 6) NULL,
            down_stop_price DECIMAL(20, 6) NULL,
            float_volume DECIMAL(24, 4) NULL,
            total_volume DECIMAL(24, 4) NULL,
            instrument_type VARCHAR(32) NULL,
            etl_sync_at DATETIME NULL,
            data_source VARCHAR(32) NULL,
            received_at DATETIME NULL,
            batch_id VARCHAR(64) NULL,
            quality_status VARCHAR(32) NULL,
            permission_status VARCHAR(32) NULL,
            KEY idx_qmt_instrument_stock (stock_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        """
        CREATE TABLE IF NOT EXISTS qmt_index_weight (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            index_qmt_code VARCHAR(64) NOT NULL,
            index_code VARCHAR(32) NOT NULL,
            qmt_code VARCHAR(64) NOT NULL,
            stock_code VARCHAR(64) NOT NULL,
            exchange VARCHAR(16) NULL,
            weight DECIMAL(20, 8) NULL,
            etl_sync_at DATETIME NULL,
            data_source VARCHAR(32) NULL,
            received_at DATETIME NULL,
            batch_id VARCHAR(64) NULL,
            quality_status VARCHAR(32) NULL,
            permission_status VARCHAR(32) NULL,
            UNIQUE KEY uk_qmt_index_weight (index_code, stock_code),
            KEY idx_qmt_index_weight_stock (stock_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
    ]
    with engine.begin() as conn:
        for ddl in ddl_statements:
            conn.execute(text(ddl))

    for table_name, column_name, ddl in [
        ("qmt_sector_member", "stock_code", "VARCHAR(64) NOT NULL"),
        ("qmt_sector_member", "qmt_code", "VARCHAR(64) NOT NULL DEFAULT ''"),
        ("qmt_instrument_detail", "qmt_code", "VARCHAR(64) NOT NULL"),
        ("qmt_instrument_detail", "stock_code", "VARCHAR(64) NOT NULL"),
        ("qmt_index_weight", "index_qmt_code", "VARCHAR(64) NOT NULL"),
        ("qmt_index_weight", "qmt_code", "VARCHAR(64) NOT NULL"),
        ("qmt_index_weight", "stock_code", "VARCHAR(64) NOT NULL"),
    ]:
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {_quote(table_name)} MODIFY COLUMN {_quote(column_name)} {ddl}"))

    if _table_columns(engine, "si_index_constituent"):
        _ensure_column(engine, "si_index_constituent", "index_qmt_code", "VARCHAR(32) NULL")
        _ensure_column(engine, "si_index_constituent", "exchange", "VARCHAR(16) NULL")
        _ensure_column(engine, "si_index_constituent", "weight", "DECIMAL(20, 8) NULL")


def _fmt_date(value: Any) -> str | None:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(digits) != 8:
        return None
    formatted = f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    try:
        datetime.strptime(formatted, "%Y-%m-%d")
    except ValueError:
        return None
    return formatted


def _stamp(df: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    out = df.copy()
    now = datetime.now().replace(microsecond=0)
    out["etl_sync_at"] = now
    out["data_source"] = PROVIDER_ID
    out["received_at"] = now
    out["batch_id"] = batch_id
    out["quality_status"] = "PENDING"
    out["permission_status"] = "SUPPORTED"
    return out


def _records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    return df.astype(object).where(pd.notna(df), None).to_dict(orient="records")


def _chunks(items: Sequence[Mapping[str, Any]], size: int) -> Iterable[list[Mapping[str, Any]]]:
    chunk_size = max(1, int(size))
    for offset in range(0, len(items), chunk_size):
        yield list(items[offset : offset + chunk_size])


def _safe_upsert_frame(
    engine: Engine,
    *,
    table_name: str,
    frame: pd.DataFrame,
    key_columns: Sequence[str],
    batch_id: str,
    chunk_size: int = 50000,
) -> dict[str, Any]:
    rows = _records(frame)
    if not rows:
        return {"status": "EMPTY", "source_rows": 0, "accepted_rows": 0, "duplicate_rows": 0}
    total_source = 0
    total_accepted = 0
    total_duplicates = 0
    statuses: set[str] = set()
    for chunk in _chunks(rows, chunk_size):
        result = safe_upsert_rows(
            engine,
            table_name=table_name,
            rows=chunk,
            key_columns=key_columns,
            batch_id=batch_id,
            permission_status="SUPPORTED",
            quality_status="PENDING",
        )
        total_source += result.source_rows
        total_accepted += result.accepted_rows
        total_duplicates += result.duplicate_rows
        statuses.add(result.status)
    return {
        "status": "UPSERTED" if "UPSERTED" in statuses else sorted(statuses)[0],
        "source_rows": total_source,
        "accepted_rows": total_accepted,
        "duplicate_rows": total_duplicates,
    }


def _read_stock_qmt_codes(engine: Engine) -> list[str]:
    with engine.begin() as conn:
        rows = conn.execute(
            text("SELECT stock_code FROM si_all_code WHERE stock_code REGEXP '^(0|3|4|6|8|9)' ORDER BY stock_code")
        ).fetchall()
    result: list[str] = []
    seen: set[str] = set()
    for row in rows:
        symbol = to_qmt_symbol(str(row[0] or ""))
        if symbol and symbol not in seen:
            seen.add(symbol)
            result.append(symbol)
    return result


def _read_index_qmt_codes(engine: Engine) -> list[str]:
    symbols = list(CORE_INDEXES)
    queries = [
        "SELECT index_code FROM si_all_index_code",
        "SELECT DISTINCT index_code FROM si_index_constituent",
    ]
    with engine.begin() as conn:
        for sql in queries:
            try:
                rows = conn.execute(text(sql)).fetchall()
            except Exception:
                continue
            for row in rows:
                symbol = to_qmt_index_symbol(str(row[0] or ""))
                if symbol:
                    symbols.append(symbol)
    result: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        value = str(symbol or "").strip().upper()
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _fetch_instrument_details(qmt_codes: Sequence[str], *, iscomplete: bool, batch_size: int, timeout: int) -> pd.DataFrame:
    if not qmt_codes:
        return pd.DataFrame()
    df = bridge.instrument_details(qmt_codes, iscomplete=iscomplete, batch_size=batch_size, timeout=timeout)
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out["qmt_code"] = out["qmt_code"].astype(str).str.upper()
    out["stock_code"] = out["stock_code"].astype(str).str.zfill(6)
    out["list_date"] = out.get("list_date", "").map(_fmt_date)
    out["expire_date"] = out.get("expire_date", "").map(_fmt_date)
    return out.drop_duplicates(subset=["qmt_code"], keep="first").reset_index(drop=True)


def _business_stock_info_rows(details: pd.DataFrame) -> pd.DataFrame:
    if details.empty:
        return pd.DataFrame(columns=["stock_code", "short_name", "exchange", "list_date", "qmt_code"])
    out = details[["stock_code", "short_name", "exchange", "list_date", "qmt_code"]].copy()
    out["short_name"] = out["short_name"].fillna("").astype(str).str.strip().str[:128]
    return out[out["short_name"] != ""].drop_duplicates(subset=["stock_code"], keep="first")


def _business_index_rows(details: pd.DataFrame) -> pd.DataFrame:
    if details.empty:
        return pd.DataFrame(columns=["index_code", "concept_code", "name", "source"])
    out = pd.DataFrame()
    out["index_code"] = details["stock_code"].astype(str).str.zfill(6)
    out["concept_code"] = ""
    out["name"] = details["short_name"].fillna("").astype(str).str.strip()
    out["source"] = "qmt"
    return out[(out["index_code"] != "") & (out["name"] != "")].drop_duplicates(subset=["index_code"], keep="first")


def _fetch_trading_calendar(start_year: int, end_year: int) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for year in range(start_year, end_year + 1):
        df = bridge.trading_calendar(
            "SH",
            start_date=f"{year}0101",
            end_date=f"{year}1231",
            timeout=120,
        )
        if df is not None and not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["calendar_year", "trade_date", "trade_status", "day_week"])
    out = pd.concat(frames, ignore_index=True)
    out["calendar_year"] = pd.to_numeric(out["calendar_year"], errors="coerce").astype("Int64")
    out["trade_status"] = 1
    out["day_week"] = pd.to_numeric(out["day_week"], errors="coerce").astype("Int64")
    return out[["calendar_year", "trade_date", "trade_status", "day_week"]].drop_duplicates(
        subset=["calendar_year", "trade_date"],
        keep="first",
    )


def _append_replace_source(engine: Engine, table_name: str, df: pd.DataFrame, *, source_column: str, source_value: str) -> int:
    if df.empty or not _table_columns(engine, table_name):
        return 0
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {_quote(table_name)} WHERE {_quote(source_column)} = :source"), {"source": source_value})
    df.to_sql(table_name, engine, if_exists="append", index=False, chunksize=2000, method="multi")
    return int(len(df))


def _delete_qmt_batch_rows(engine: Engine, table_name: str) -> None:
    if not _table_columns(engine, table_name):
        return
    with engine.begin() as conn:
        columns = _table_columns(engine, table_name)
        if "data_source" in columns:
            conn.execute(text(f"DELETE FROM {_quote(table_name)} WHERE data_source = :source"), {"source": PROVIDER_ID})
        elif "source" in columns:
            conn.execute(text(f"DELETE FROM {_quote(table_name)} WHERE source = 'qmt'"))


def sync_reference_data(
    *,
    start_year: int,
    end_year: int,
    iscomplete: bool,
    refresh_timeout: int,
    skip_refresh: bool = False,
    skip_calendar: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    engine = create_engine(get_mysql_url(required=True), pool_pre_ping=True, future=True)
    ensure_reference_tables(engine)
    batch_id = f"qmt_reference_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    refresh_result: dict[str, Any] = {"status": "skipped"} if skip_refresh else {}
    if not skip_refresh:
        try:
            refresh_result = bridge.refresh_reference_data(
                ["download_sector_data", "download_index_weight", "download_holiday_data"],
                timeout=refresh_timeout,
            )
        except Exception as exc:
            refresh_result = {"status": "warning", "error": str(exc)}

    sector_df = bridge.sector_list(timeout=180)
    sector_df = sector_df if sector_df is not None else pd.DataFrame()
    sector_df = _stamp(sector_df.drop_duplicates(subset=["sector_name"], keep="first"), batch_id) if not sector_df.empty else sector_df

    sector_members = bridge.sector_members_many(
        sector_df["sector_name"].astype(str).tolist() if not sector_df.empty else [],
        timeout=1800,
    )
    sector_members = sector_members if sector_members is not None else pd.DataFrame()
    if not sector_members.empty:
        sector_members = sector_members.copy()
        sector_members["stock_code"] = sector_members["stock_code"].astype(str).str.zfill(6)
        sector_members["qmt_code"] = sector_members["qmt_code"].astype(str).str.upper()
        sector_members["exchange"] = sector_members["qmt_code"].str.split(".", n=1).str[1].fillna("")
        sector_members = _stamp(
            sector_members.drop_duplicates(subset=["sector_name", "stock_code"], keep="first"),
            batch_id,
        )

    sector_datasets = fetch_sector_datasets()

    stock_details = _fetch_instrument_details(
        _read_stock_qmt_codes(engine),
        iscomplete=iscomplete,
        batch_size=400,
        timeout=900,
    )
    index_details = _fetch_instrument_details(
        _read_index_qmt_codes(engine),
        iscomplete=iscomplete,
        batch_size=300,
        timeout=600,
    )
    all_details = pd.concat([stock_details, index_details], ignore_index=True).drop_duplicates(
        subset=["qmt_code"],
        keep="first",
    )
    if not all_details.empty:
        all_details["instrument_type"] = all_details["qmt_code"].where(
            all_details["qmt_code"].str[:3].isin(["000", "399"]),
            "",
        )
        all_details = _stamp(all_details, batch_id)

    index_symbols = _read_index_qmt_codes(engine)
    index_weight = bridge.index_weight_many(index_symbols, timeout=1200)
    index_weight = index_weight if index_weight is not None else pd.DataFrame()
    if not index_weight.empty:
        index_weight = index_weight.copy()
        index_weight["index_code"] = index_weight["index_code"].astype(str).str.zfill(6)
        index_weight["stock_code"] = index_weight["stock_code"].astype(str).str.zfill(6)
        index_weight = _stamp(
            index_weight.drop_duplicates(subset=["index_code", "stock_code"], keep="first"),
            batch_id,
        )

    calendar_error = ""
    if skip_calendar:
        calendar = pd.DataFrame(columns=["calendar_year", "trade_date", "trade_status", "day_week"])
    else:
        try:
            calendar = _stamp(_fetch_trading_calendar(start_year, end_year), batch_id)
        except Exception as exc:
            calendar_error = str(exc)
            calendar = pd.DataFrame(columns=["calendar_year", "trade_date", "trade_status", "day_week"])

    if dry_run:
        return {
            "status": "dry_run",
            "batch_id": batch_id,
            "refresh": refresh_result,
            "rows": {
                "qmt_sector_list": int(len(sector_df)),
                "qmt_sector_member": int(len(sector_members)),
                "qmt_instrument_detail": int(len(all_details)),
                "qmt_index_weight": int(len(index_weight)),
                "si_trade_calendar": int(len(calendar)),
                "si_concept_code_east": int(len(sector_datasets.get("concept_catalog", pd.DataFrame()))),
                "si_concept_constituent_east": int(len(sector_datasets.get("concept_constituents", pd.DataFrame()))),
                "si_industry_sw": int(len(sector_datasets.get("industry_sw", pd.DataFrame()))),
                "si_all_code": int(len(stock_details)),
                "si_all_index_code": int(len(index_details)),
            },
            "calendar_error": calendar_error,
        }

    results: dict[str, Any] = {"refresh": refresh_result, "batch_id": batch_id, "tables": {}}

    write_plan: list[tuple[str, pd.DataFrame, Sequence[str]]] = [
        ("qmt_sector_list", sector_df, ["sector_name"]),
        ("qmt_sector_member", sector_members, ["sector_name", "stock_code"]),
        ("qmt_instrument_detail", all_details, ["qmt_code"]),
        ("qmt_index_weight", index_weight, ["index_code", "stock_code"]),
        ("si_all_code", _stamp(_business_stock_info_rows(stock_details), batch_id), ["stock_code"]),
        ("si_all_index_code", _stamp(_business_index_rows(index_details), batch_id), ["index_code"]),
        ("si_concept_code_east", _stamp(sector_datasets.get("concept_catalog", pd.DataFrame()), batch_id), ["concept_code"]),
        (
            "si_concept_constituent_east",
            _stamp(sector_datasets.get("concept_constituents", pd.DataFrame()), batch_id),
            ["concept_code", "stock_code"],
        ),
    ]
    if not calendar.empty:
        write_plan.append(("si_trade_calendar", calendar, ["calendar_year", "trade_date"]))
    for table_name, frame, keys in write_plan:
        results["tables"][table_name] = _safe_upsert_frame(
            engine,
            table_name=table_name,
            key_columns=keys,
            batch_id=batch_id,
            frame=frame,
        )

    industry_sw = sector_datasets.get("industry_sw", pd.DataFrame())
    if industry_sw is not None and not industry_sw.empty:
        industry_sw = industry_sw.copy()
        industry_sw["etl_sync_at"] = datetime.now().replace(microsecond=0)
        results["tables"]["si_industry_sw"] = {
            "status": "REPLACED_SOURCE",
            "accepted_rows": _append_replace_source(engine, "si_industry_sw", industry_sw, source_column="source", source_value="qmt"),
        }

    if not index_weight.empty and _table_columns(engine, "si_index_constituent"):
        index_business = index_weight[
            ["index_code", "stock_code", "qmt_code", "index_qmt_code", "exchange", "weight", "etl_sync_at", "data_source", "received_at", "batch_id", "quality_status", "permission_status"]
        ].copy()
        detail_names = {}
        if not all_details.empty:
            detail_names = (
                all_details.drop_duplicates(subset=["stock_code"], keep="first")
                .set_index("stock_code")["short_name"]
                .fillna("")
                .astype(str)
                .to_dict()
            )
        index_business["short_name"] = index_business["stock_code"].map(detail_names).fillna("")
        _delete_qmt_batch_rows(engine, "si_index_constituent")
        index_business.to_sql("si_index_constituent", engine, if_exists="append", index=False, chunksize=2000, method="multi")
        results["tables"]["si_index_constituent"] = {
            "status": "REPLACED_QMT_ROWS",
            "accepted_rows": int(len(index_business)),
        }

    results["status"] = "success"
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync Guojin QMT reference data into local business tables.")
    parser.add_argument("--start-year", type=int, default=1990)
    parser.add_argument("--end-year", type=int, default=date.today().year + 1)
    parser.add_argument("--iscomplete", action="store_true", help="Request complete instrument details when QMT supports it.")
    parser.add_argument("--refresh-timeout", type=int, default=900)
    parser.add_argument("--skip-refresh", action="store_true")
    parser.add_argument("--include-calendar", action="store_true", help="Also call QMT get_trading_calendar. Disabled by default.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    result = sync_reference_data(
        start_year=args.start_year,
        end_year=args.end_year,
        iscomplete=args.iscomplete,
        refresh_timeout=max(1, args.refresh_timeout),
        skip_refresh=args.skip_refresh,
        skip_calendar=not args.include_calendar,
        dry_run=args.dry_run,
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    else:
        print(result)
    return 0 if result.get("status") in {"success", "dry_run"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
