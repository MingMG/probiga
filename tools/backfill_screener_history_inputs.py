#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Backfill and validate historical capital-flow and dragon-tiger inputs.

The job is intentionally narrow and recoverable:

* capital-flow rows are inserted only when missing, except rows whose component
  identities are already proven invalid;
* dragon-tiger daily rows are inserted only for empty stock/date keys;
* seat details are replaced only for dates that are missing/partial or contain
  exact duplicate provider rows, and only after a complete replacement frame
  has been fetched and validated;
* every corrected/replaced row is exported before the transaction commits.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.adata_release import ensure_adata_import_path  # noqa: E402

ensure_adata_import_path(ROOT)

from biz.sentiment.sync_sentiment import (  # noqa: E402
    _coerce_a_list_daily_columns,
    _finalize_a_list_info_df,
)
from server.common.batch_db import create_batch_engine, write_frame  # noqa: E402
from server.common.kline_data import get_kline_engine  # noqa: E402
from server.common.mysql_lock import mysql_named_lock  # noqa: E402
from tools.crawl_stock_fund_flow import (  # noqa: E402
    _fetch_push2his_curl,
    _fetch_push2his_socket,
)
from tools.crawl_all_stock_flow import fetch_one_stock as _fetch_push2delay  # noqa: E402
from tools.fetch_sm_stock_capital_flow_daily import _fetch_baidu_dates  # noqa: E402

FLOW_COLUMNS = (
    "stock_code", "trade_date", "main_net_inflow", "max_net_inflow",
    "lg_net_inflow", "mid_net_inflow", "sm_net_inflow",
)
LHB_DAILY_COLUMNS = (
    "trade_date", "short_name", "stock_code", "close", "change_cpt",
    "turnover_ratio", "a_net_amount", "a_buy_amount", "a_sell_amount",
    "a_amount", "amount", "net_amount_rate", "a_amount_rate", "reason",
)
LHB_INFO_COLUMNS = (
    "trade_date", "stock_code", "operate_code", "operate_name",
    "a_buy_amount", "a_sell_amount", "a_net_amount",
    "a_buy_amount_rate", "a_sell_amount_rate", "reason",
)
FLOW_SOURCE = "push2hist"
LHB_SOURCE = "eastmoney_datacenter"
SUPPORTED_CODE_PREFIXES = {"00", "30", "60", "68", "92"}


def _date(value: Any) -> str:
    return str(value or "")[:10]


def _code(value: Any) -> str:
    return str(value or "").strip().zfill(6)


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime, Decimal)):
        return str(value)
    raise TypeError(type(value).__name__)


def _valid_code(value: Any) -> bool:
    code = _code(value)
    return len(code) == 6 and code.isdigit() and code[:2] in SUPPORTED_CODE_PREFIXES


def _flow_tolerance(values: Iterable[float]) -> float:
    values = list(values)
    return max(max(abs(value) for value in values) * 0.001, 1_000_000.0)


def flow_components_valid(row: dict[str, Any]) -> bool:
    values = [
        float(row[column])
        for column in (
            "main_net_inflow", "max_net_inflow", "lg_net_inflow",
            "mid_net_inflow", "sm_net_inflow",
        )
    ]
    if not all(math.isfinite(value) for value in values):
        return False
    main, maximum, large, middle, small = values
    tolerance = _flow_tolerance(values)
    return (
        abs(main - maximum - large) <= tolerance
        and abs(main + middle + small) <= tolerance
    )


def normalize_flow_rows(
    rows: Iterable[dict[str, Any]],
    targets_by_code: dict[str, set[str]],
) -> list[dict[str, Any]]:
    output: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in rows:
        code = _code(raw.get("stock_code"))
        trade_date = _date(raw.get("trade_date"))
        if trade_date not in targets_by_code.get(code, set()):
            continue
        row = {column: raw.get(column) for column in FLOW_COLUMNS}
        row["stock_code"] = code
        row["trade_date"] = trade_date
        if flow_components_valid(row):
            output[(code, trade_date)] = row
    return list(output.values())


def normalize_lhb_daily(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=LHB_DAILY_COLUMNS)
    frame = _coerce_a_list_daily_columns(frame.copy())
    frame["stock_code"] = frame["stock_code"].map(_code)
    frame["trade_date"] = frame["trade_date"].map(_date)
    frame = frame[frame["stock_code"].map(_valid_code)]
    frame = frame.drop_duplicates(["trade_date", "stock_code"], keep="last")
    return frame[[column for column in LHB_DAILY_COLUMNS if column in frame.columns]]


def normalize_lhb_info(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=LHB_INFO_COLUMNS)
    frame = _finalize_a_list_info_df(frame.copy())
    frame["stock_code"] = frame["stock_code"].map(_code)
    frame["trade_date"] = frame["trade_date"].map(_date)
    frame = frame[frame["stock_code"].map(_valid_code)]
    return frame[[column for column in LHB_INFO_COLUMNS if column in frame.columns]]


def _trade_dates(engine, start_date: str, end_date: str) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT trade_date FROM si_trade_calendar
            WHERE trade_status = 1
              AND trade_date BETWEEN :start_date AND :end_date
            ORDER BY trade_date
        """), {"start_date": start_date, "end_date": end_date}).fetchall()
    return [_date(row[0]) for row in rows]


def _flow_gap_plan(business_engine, kline_engine, start_date: str, end_date: str):
    with kline_engine.connect() as conn:
        expected_rows = conn.execute(text("""
            SELECT DISTINCT stock_code, trade_date
            FROM sm_stock_kline
            WHERE trade_date BETWEEN :start_date AND :end_date
              AND k_type = 1 AND adjust_type = 0 AND volume > 0
              AND stock_code REGEXP '^(00|30|60|68)[0-9]{4}$'
        """), {"start_date": start_date, "end_date": end_date}).fetchall()
    expected = {(_code(code), _date(trade_date)) for code, trade_date in expected_rows}
    with business_engine.connect() as conn:
        current = conn.execute(text("""
            SELECT stock_code, trade_date, main_net_inflow, max_net_inflow,
                   lg_net_inflow, mid_net_inflow, sm_net_inflow, data_source
            FROM sm_stock_capital_flow_daily
            WHERE trade_date BETWEEN :start_date AND :end_date
              AND stock_code REGEXP '^(00|30|60|68)[0-9]{4}$'
        """), {"start_date": start_date, "end_date": end_date}).mappings().all()
    current_by_key = {
        (_code(row["stock_code"]), _date(row["trade_date"])): dict(row)
        for row in current
    }
    missing = expected - set(current_by_key)
    invalid = {
        key for key, row in current_by_key.items()
        if key in expected and not flow_components_valid(row)
    }
    targets = missing | invalid
    targets_by_code: dict[str, set[str]] = defaultdict(set)
    for code, trade_date in targets:
        targets_by_code[code].add(trade_date)
    return expected, missing, invalid, targets_by_code, current_by_key


def _fetch_flow_code(code: str, wanted_dates: set[str]) -> tuple[str, list[dict], str]:
    last_error = ""
    # The push2delay address uses a separate Eastmoney edge and is therefore a
    # real transport fallback, unlike socket/curl which share the same origin
    # quota.  All three return the same provider schema and are identity-checked
    # before a row is accepted.
    for name, fetcher in (
        ("push2delay", _fetch_push2delay),
        ("socket", _fetch_push2his_socket),
        ("curl", _fetch_push2his_curl),
    ):
        for attempt in range(3):
            try:
                rows = fetcher(code) or []
                normalized = normalize_flow_rows(rows, {code: wanted_dates})
                if normalized:
                    return code, normalized, name
                last_error = "provider returned no requested dates"
                break
            except Exception as exc:  # pylint: disable=broad-except
                last_error = f"{type(exc).__name__}: {str(exc)[:200]}"
                if attempt < 2:
                    time.sleep(0.5 * (attempt + 1))
    return code, [], last_error


def _fetch_flow_code_baidu(
    code: str,
    wanted_dates: set[str],
) -> tuple[str, list[dict[str, Any]], str]:
    last_error = ""
    for attempt in range(4):
        try:
            frame = _fetch_baidu_dates(code, wanted_dates)
            if frame is not None and not frame.empty:
                rows = normalize_flow_rows(
                    frame.to_dict("records"),
                    {code: wanted_dates},
                )
                if rows:
                    for row in rows:
                        row["_data_source"] = "baidu"
                    return code, rows, ""
            last_error = "provider returned no requested dates"
        except Exception as exc:  # pylint: disable=broad-except
            last_error = f"{type(exc).__name__}: {str(exc)[:200]}"
        if attempt < 3:
            time.sleep(0.5 * (attempt + 1))
    return code, [], last_error


def _write_jsonl_gz(path: Path, rows: Iterable[dict[str, Any]]) -> tuple[int, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    count = 0
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as stream:
        for row in rows:
            line = json.dumps(row, ensure_ascii=False, default=_json_default, sort_keys=True) + "\n"
            stream.write(line)
            digest.update(line.encode("utf-8"))
            count += 1
    return count, digest.hexdigest()


def backfill_flow(
    business_engine,
    kline_engine,
    start_date: str,
    end_date: str,
    *,
    workers: int,
    evidence_dir: Path,
    dry_run: bool,
    baidu_only: bool = False,
) -> dict[str, Any]:
    expected, missing, invalid, targets_by_code, current_by_key = _flow_gap_plan(
        business_engine, kline_engine, start_date, end_date,
    )
    target_count = sum(len(dates) for dates in targets_by_code.values())
    print(
        f"capital-flow plan: expected={len(expected)} missing={len(missing)} "
        f"invalid={len(invalid)} target_codes={len(targets_by_code)}"
    )
    fetched: dict[tuple[str, str], dict[str, Any]] = {}
    errors: dict[str, str] = {}
    source_methods: dict[str, int] = defaultdict(int)
    if targets_by_code and not baidu_only:
        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            futures = {
                pool.submit(_fetch_flow_code, code, dates): code
                for code, dates in targets_by_code.items()
            }
            for index, future in enumerate(as_completed(futures), 1):
                code, rows, source = future.result()
                if rows:
                    source_methods[source] += 1
                    for row in rows:
                        fetched[(row["stock_code"], row["trade_date"])] = row
                else:
                    errors[code] = source
                if index % 500 == 0 or index == len(futures):
                    print(
                        f"capital-flow fetch: {index}/{len(futures)} "
                        f"pairs={len(fetched)}/{target_count} errors={len(errors)}"
                    )
    unresolved = sorted(set(missing | invalid) - set(fetched))
    # Baidu exposes a separate historical provider and is used only for pairs
    # that all Eastmoney transports could not return.  Its rows pass the same
    # component identities before they are accepted.
    if unresolved:
        unresolved_by_code: dict[str, set[str]] = defaultdict(set)
        for code, trade_date in unresolved:
            unresolved_by_code[code].add(trade_date)
        fallback_errors: dict[str, str] = {}
        with ThreadPoolExecutor(max_workers=min(max(1, workers), 16)) as pool:
            futures = {
                pool.submit(_fetch_flow_code_baidu, code, dates): code
                for code, dates in unresolved_by_code.items()
            }
            for future in as_completed(futures):
                code, rows, error = future.result()
                if rows:
                    for row in rows:
                        fetched[(row["stock_code"], row["trade_date"])] = row
                    source_methods["baidu"] += len(rows)
                else:
                    fallback_errors[code] = error
        unresolved = sorted(set(missing | invalid) - set(fetched))
        if fallback_errors:
            errors.update(fallback_errors)
    evidence_rows = [current_by_key[key] for key in sorted(invalid) if key in current_by_key]
    evidence_path = evidence_dir / "capital-flow-corrected-rows-before.jsonl.gz"
    evidence_count, evidence_sha = _write_jsonl_gz(evidence_path, evidence_rows)
    if not dry_run and fetched:
        now = datetime.now().replace(microsecond=0)
        records = []
        for row in fetched.values():
            item = dict(row)
            data_source = str(item.pop("_data_source", FLOW_SOURCE))
            item.update({"etl_sync_at": now, "data_source": data_source})
            records.append(item)
        statement = text("""
            INSERT INTO sm_stock_capital_flow_daily (
              stock_code, trade_date, main_net_inflow, max_net_inflow,
              lg_net_inflow, mid_net_inflow, sm_net_inflow,
              etl_sync_at, data_source
            ) VALUES (
              :stock_code, :trade_date, :main_net_inflow, :max_net_inflow,
              :lg_net_inflow, :mid_net_inflow, :sm_net_inflow,
              :etl_sync_at, :data_source
            )
            ON DUPLICATE KEY UPDATE
              main_net_inflow=VALUES(main_net_inflow),
              max_net_inflow=VALUES(max_net_inflow),
              lg_net_inflow=VALUES(lg_net_inflow),
              mid_net_inflow=VALUES(mid_net_inflow),
              sm_net_inflow=VALUES(sm_net_inflow),
              etl_sync_at=VALUES(etl_sync_at),
              data_source=VALUES(data_source)
        """)
        with mysql_named_lock(business_engine, "probiga:capital_flow_daily", timeout_seconds=30):
            with business_engine.begin() as conn:
                for start in range(0, len(records), 1000):
                    conn.execute(statement, records[start:start + 1000])
    return {
        "expected_pair_count": len(expected),
        "missing_pair_count_before": len(missing),
        "invalid_pair_count_before": len(invalid),
        "fetched_pair_count": len(fetched),
        "unresolved_pair_count": len(unresolved),
        "unresolved_pair_samples": [
            {"stock_code": code, "trade_date": trade_date}
            for code, trade_date in unresolved[:100]
        ],
        "fetch_error_code_count": len(errors),
        "fetch_methods": dict(source_methods),
        "baidu_only": baidu_only,
        "evidence_path": str(evidence_path),
        "evidence_row_count": evidence_count,
        "evidence_sha256": evidence_sha,
        "dry_run": dry_run,
    }


def _retry_lhb_daily(trade_date: str) -> tuple[str, pd.DataFrame, str]:
    from adata.sentiment import sentiment as sentiment_api  # pylint: disable=import-outside-toplevel

    last_error = ""
    for attempt in range(4):
        try:
            frame = normalize_lhb_daily(
                sentiment_api.hot.list_a_list_daily(report_date=trade_date)
            )
            if not frame.empty:
                return trade_date, frame, ""
            last_error = "provider returned no rows"
        except Exception as exc:  # pylint: disable=broad-except
            last_error = f"{type(exc).__name__}: {str(exc)[:200]}"
        if attempt < 3:
            time.sleep(1.0 * (attempt + 1))
    return trade_date, pd.DataFrame(columns=LHB_DAILY_COLUMNS), last_error


def _fetch_lhb_report_page(report: str, trade_date: str, page: int) -> tuple[list[dict], int]:
    from adata.common.utils import requests as adata_requests  # pylint: disable=import-outside-toplevel

    url = (
        "https://datacenter-web.eastmoney.com/api/data/v1/get?"
        f"reportName={report}&columns=ALL&filter=(TRADE_DATE='{trade_date}')"
        f"&pageNumber={page}&pageSize=500&sortTypes=-1&sortColumns=BUY"
        "&source=WEB&client=WEB"
    )
    response = adata_requests.request(method="post", url=url).json()
    result = response.get("result") or {}
    return list(result.get("data") or []), int(result.get("pages") or 0)


def _fetch_lhb_info_date(trade_date: str) -> tuple[str, pd.DataFrame, str]:
    raw_rows: list[dict[str, Any]] = []
    for report in (
        "RPT_BILLBOARD_DAILYDETAILSBUY",
        "RPT_BILLBOARD_DAILYDETAILSSELL",
    ):
        page = 1
        while True:
            last_error = ""
            for attempt in range(4):
                try:
                    rows, pages = _fetch_lhb_report_page(report, trade_date, page)
                    break
                except Exception as exc:  # pylint: disable=broad-except
                    last_error = f"{type(exc).__name__}: {str(exc)[:200]}"
                    if attempt < 3:
                        time.sleep(1.0 * (attempt + 1))
            else:
                return trade_date, pd.DataFrame(columns=LHB_INFO_COLUMNS), last_error
            raw_rows.extend(rows)
            if page >= max(pages, 1) or not rows:
                break
            page += 1
    if not raw_rows:
        return trade_date, pd.DataFrame(columns=LHB_INFO_COLUMNS), "provider returned no rows"
    rename = {
        "SECURITY_CODE": "stock_code",
        "TRADE_DATE": "trade_date",
        "OPERATEDEPT_CODE": "operate_code",
        "OPERATEDEPT_NAME": "operate_name",
        "BUY": "a_buy_amount",
        "SELL": "a_sell_amount",
        "NET": "a_net_amount",
        "TOTAL_BUYRIO": "a_buy_amount_rate",
        "TOTAL_SELLRIO": "a_sell_amount_rate",
        "EXPLANATION": "reason",
    }
    frame = pd.DataFrame(raw_rows).rename(columns=rename)
    return trade_date, normalize_lhb_info(frame), ""


def _existing_lhb_state(engine, start_date: str, end_date: str):
    with engine.connect() as conn:
        daily_rows = conn.execute(text("""
            SELECT * FROM st_a_list_daily
            WHERE trade_date BETWEEN :start_date AND :end_date
        """), {"start_date": start_date, "end_date": end_date}).mappings().all()
        info_rows = conn.execute(text("""
            SELECT * FROM st_a_list_info
            WHERE trade_date BETWEEN :start_date AND :end_date
        """), {"start_date": start_date, "end_date": end_date}).mappings().all()
    return [dict(row) for row in daily_rows], [dict(row) for row in info_rows]


def _info_exact_key(row: dict[str, Any]) -> tuple[str, ...]:
    return tuple(
        "<NULL>" if row.get(column) is None else str(row.get(column))
        for column in LHB_INFO_COLUMNS
    )


def backfill_lhb(
    engine,
    start_date: str,
    end_date: str,
    *,
    workers: int,
    evidence_dir: Path,
    include_info: bool,
    dry_run: bool,
) -> dict[str, Any]:
    trade_dates = _trade_dates(engine, start_date, end_date)
    existing_daily, existing_info = _existing_lhb_state(engine, start_date, end_date)
    existing_daily_keys = {
        (_code(row["stock_code"]), _date(row["trade_date"]))
        for row in existing_daily if _valid_code(row["stock_code"])
    }
    existing_daily_dates = {trade_date for _, trade_date in existing_daily_keys}
    missing_dates = [date for date in trade_dates if date not in existing_daily_dates]
    daily_frames: dict[str, pd.DataFrame] = {}
    daily_errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(_retry_lhb_daily, date): date for date in missing_dates}
        for index, future in enumerate(as_completed(futures), 1):
            trade_date, frame, error = future.result()
            if frame.empty:
                daily_errors[trade_date] = error
            else:
                daily_frames[trade_date] = frame
            if index % 20 == 0 or index == len(futures):
                print(f"dragon-tiger daily: {index}/{len(futures)} errors={len(daily_errors)}")
    daily_insert = pd.concat(daily_frames.values(), ignore_index=True) if daily_frames else pd.DataFrame(columns=LHB_DAILY_COLUMNS)
    if not daily_insert.empty:
        daily_insert = daily_insert[
            ~daily_insert.apply(
                lambda row: (_code(row["stock_code"]), _date(row["trade_date"])) in existing_daily_keys,
                axis=1,
            )
        ]
        daily_insert["etl_sync_at"] = datetime.now().replace(microsecond=0)
        daily_insert["data_source"] = LHB_SOURCE
    if not dry_run and not daily_insert.empty:
        with mysql_named_lock(engine, "probiga:dragon_tiger_daily", timeout_seconds=30):
            with engine.begin() as conn:
                write_frame(
                    daily_insert,
                    "st_a_list_daily",
                    conn,
                    if_exists="append",
                    index=False,
                    chunksize=1000,
                    method="multi",
                )

    info_result: dict[str, Any] = {"enabled": include_info}
    if include_info:
        # Refresh state after inserting daily rows.
        current_daily, current_info = _existing_lhb_state(engine, start_date, end_date)
        daily_keys_by_date: dict[str, set[str]] = defaultdict(set)
        for row in current_daily:
            if _valid_code(row["stock_code"]):
                daily_keys_by_date[_date(row["trade_date"])].add(_code(row["stock_code"]))
        info_keys_by_date: dict[str, set[str]] = defaultdict(set)
        info_rows_by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in current_info:
            if _valid_code(row["stock_code"]):
                trade_date = _date(row["trade_date"])
                info_keys_by_date[trade_date].add(_code(row["stock_code"]))
                info_rows_by_date[trade_date].append(row)
        duplicate_dates = {
            date for date, rows in info_rows_by_date.items()
            if len(rows) != len({_info_exact_key(row) for row in rows})
        }
        info_target_dates = sorted({
            date for date, codes in daily_keys_by_date.items()
            if codes - info_keys_by_date.get(date, set())
        } | duplicate_dates)
        info_frames: dict[str, pd.DataFrame] = {}
        info_errors: dict[str, str] = {}
        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            futures = {pool.submit(_fetch_lhb_info_date, date): date for date in info_target_dates}
            for index, future in enumerate(as_completed(futures), 1):
                trade_date, frame, error = future.result()
                fetched_codes = set(frame["stock_code"].astype(str)) if not frame.empty else set()
                missing_codes = daily_keys_by_date.get(trade_date, set()) - fetched_codes
                if frame.empty or missing_codes:
                    info_errors[trade_date] = error or f"missing {len(missing_codes)} daily stock codes"
                else:
                    info_frames[trade_date] = frame
                if index % 20 == 0 or index == len(futures):
                    print(f"dragon-tiger info: {index}/{len(futures)} errors={len(info_errors)}")
        replace_dates = sorted(info_frames)
        evidence_info_rows = [
            row for date in replace_dates for row in info_rows_by_date.get(date, [])
        ]
        evidence_path = evidence_dir / "dragon-tiger-info-rows-before.jsonl.gz"
        evidence_count, evidence_sha = _write_jsonl_gz(evidence_path, evidence_info_rows)
        if not dry_run and info_frames:
            now = datetime.now().replace(microsecond=0)
            with mysql_named_lock(engine, "probiga:dragon_tiger_info", timeout_seconds=30):
                with engine.begin() as conn:
                    for date in replace_dates:
                        conn.execute(
                            text("DELETE FROM st_a_list_info WHERE trade_date = :date"),
                            {"date": date},
                        )
                    combined = pd.concat(info_frames.values(), ignore_index=True)
                    combined["etl_sync_at"] = now
                    write_frame(
                        combined,
                        "st_a_list_info",
                        conn,
                        if_exists="append",
                        index=False,
                        chunksize=1000,
                        method="multi",
                    )
        info_result = {
            "enabled": True,
            "target_date_count": len(info_target_dates),
            "replaced_date_count": len(replace_dates),
            "error_date_count": len(info_errors),
            "errors": info_errors,
            "evidence_path": str(evidence_path),
            "evidence_row_count": evidence_count,
            "evidence_sha256": evidence_sha,
        }
    return {
        "trade_date_count": len(trade_dates),
        "missing_daily_date_count_before": len(missing_dates),
        "inserted_daily_row_count": len(daily_insert),
        "daily_error_date_count": len(daily_errors),
        "daily_errors": daily_errors,
        "info": info_result,
        "dry_run": dry_run,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--flow-workers", type=int, default=8)
    parser.add_argument(
        "--flow-baidu-only",
        action="store_true",
        help="skip unavailable Eastmoney transports and batch missing dates by stock via Baidu",
    )
    parser.add_argument("--lhb-workers", type=int, default=4)
    parser.add_argument("--skip-flow", action="store_true")
    parser.add_argument("--skip-lhb", action="store_true")
    parser.add_argument("--skip-lhb-info", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--evidence-dir", default="")
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    run_id = datetime.now().strftime("%Y%m%dT%H%M%S")
    evidence_dir = (
        Path(args.evidence_dir).resolve()
        if args.evidence_dir
        else (ROOT / "runtime" / "backfill_evidence" / f"screener-inputs-{run_id}")
    )
    evidence_dir.mkdir(parents=True, exist_ok=True)
    business_engine = create_batch_engine()
    report: dict[str, Any] = {
        "run_id": run_id,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "evidence_dir": str(evidence_dir),
        "dry_run": args.dry_run,
    }
    if not args.skip_flow:
        report["capital_flow"] = backfill_flow(
            business_engine,
            get_kline_engine(),
            args.start_date,
            args.end_date,
            workers=max(1, args.flow_workers),
            evidence_dir=evidence_dir,
            dry_run=args.dry_run,
            baidu_only=args.flow_baidu_only,
        )
    if not args.skip_lhb:
        report["dragon_tiger"] = backfill_lhb(
            business_engine,
            args.start_date,
            args.end_date,
            workers=max(1, args.lhb_workers),
            evidence_dir=evidence_dir,
            include_info=not args.skip_lhb_info,
            dry_run=args.dry_run,
        )
    output_path = Path(args.output).resolve() if args.output else evidence_dir / "manifest.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=_json_default) + "\n",
        encoding="utf-8",
    )
    print(output_path)
    unresolved = int(report.get("capital_flow", {}).get("unresolved_pair_count", 0))
    unresolved += int(report.get("dragon_tiger", {}).get("daily_error_date_count", 0))
    unresolved += int(report.get("dragon_tiger", {}).get("info", {}).get("error_date_count", 0))
    return 0 if unresolved == 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
