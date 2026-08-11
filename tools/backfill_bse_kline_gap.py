#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Backfill a Beijing Stock Exchange daily-K gap with dual-source evidence.

Sina is the primary row source. Tonghuashun is an independent licensed quote
source used to verify every OHLC row. Forward-adjusted Tonghuashun closes are
used only to derive the economically correct previous-close return reference.

The default mode is a dry run. ``--apply`` atomically replaces only 920xxx
rows for the requested trading dates in the primary and K-line read databases.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.stock_market.sina_kline_fetch import fetch_sina_a_daily_kline  # noqa: E402
from biz.stock_market.stock_kline_akshare import (  # noqa: E402
    akshare_daily_to_sm_kline,
    em_code_to_sina_symbol,
)
from server.common.batch_db import create_batch_engine, write_frame  # noqa: E402
from server.common.mysql_lock import mysql_named_lock  # noqa: E402
from tools.fetch_sm_stock_kline_daily import (  # noqa: E402
    _build_source_trace,
    _dataset_hash,
    _distinct_kline_url,
    _ensure_provenance_tables,
    _fetch_ths_history,
    _rows_match,
    _ths_auth_headers,
    _validate_daily_frame,
)


BUSINESS_COLUMNS = [
    "stock_code",
    "short_name",
    "trade_time",
    "trade_date",
    "k_type",
    "adjust_type",
    "open",
    "close",
    "high",
    "low",
    "volume",
    "amount",
    "change",
    "change_pct",
    "turnover_ratio",
    "pre_close",
    "etl_sync_at",
]

_BSE_BULK_TRADE_API = (
    "https://www.bse.cn/tnqfgkcjxxController/btcjxxList.do"
)


@dataclass
class CodeOutcome:
    code: str
    frame: pd.DataFrame = field(default_factory=pd.DataFrame)
    matched_rows: int = 0
    no_data_dates: list[str] = field(default_factory=list)
    primary_only_dates: list[str] = field(default_factory=list)
    reference_only_dates: list[str] = field(default_factory=list)
    mismatch_rows: list[dict[str, Any]] = field(default_factory=list)
    error: str = ""


def _date_text(value: Any) -> str:
    return str(value or "")[:10]


def _load_target_dates(engine: Engine, start_date: str, end_date: str) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT trade_date
            FROM si_trade_calendar
            WHERE trade_status = 1
              AND trade_date BETWEEN :start_date AND :end_date
            ORDER BY trade_date
        """), {"start_date": start_date, "end_date": end_date}).fetchall()
    return [_date_text(row[0]) for row in rows]


def _load_universe(
    engine: Engine,
    end_date: str,
) -> tuple[list[str], dict[str, str], dict[str, str]]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT stock_code, short_name, list_date
            FROM si_all_code
            WHERE stock_code REGEXP '^92'
              AND (list_date IS NULL OR list_date <= :end_date)
            ORDER BY stock_code
        """), {"end_date": end_date}).fetchall()
    codes: list[str] = []
    short_names: dict[str, str] = {}
    list_dates: dict[str, str] = {}
    for code, short_name, list_date in rows:
        normalized = str(code or "").strip().zfill(6)
        if not normalized.startswith("92"):
            continue
        codes.append(normalized)
        short_names[normalized] = str(short_name or "").strip()
        list_dates[normalized] = _date_text(list_date)
    return codes, short_names, list_dates


def _sina_history(
    code: str,
    short_name: str,
    history_start: str,
    end_date: str,
) -> pd.DataFrame:
    symbol = em_code_to_sina_symbol(code)
    if not symbol:
        return pd.DataFrame()
    raw = fetch_sina_a_daily_kline(
        symbol,
        history_start.replace("-", ""),
        end_date.replace("-", ""),
        "",
    )
    if raw is None or raw.empty:
        return pd.DataFrame()
    frame = akshare_daily_to_sm_kline(
        raw,
        code,
        1,
        0,
        short_name=short_name,
    )
    if frame is None or frame.empty:
        return pd.DataFrame()
    frame = frame.copy()
    frame["trade_date"] = frame["trade_date"].astype(str).str[:10]
    return frame.sort_values("trade_date").drop_duplicates(
        subset=["trade_date"],
        keep="last",
    )


def _records_by_date(frame: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if frame is None or frame.empty:
        return {}
    return {
        str(row["trade_date"])[:10]: row
        for row in frame.to_dict(orient="records")
    }


def _parse_bse_jsonp(text_value: str) -> Any:
    matched = re.match(
        r"^[^(]*\((.*)\)\s*;?\s*$",
        str(text_value or "").strip(),
        flags=re.DOTALL,
    )
    if not matched:
        raise ValueError("BSE bulk-trade response is not valid JSONP")
    body = matched.group(1)
    # The public endpoint appends a legacy single-quoted date marker to an
    # otherwise valid JSON payload (for example: ``..., '']``).
    legacy_tail = re.search(r",\s*'([^']*)'\s*\]\s*$", body)
    if legacy_tail:
        body = (
            body[:legacy_tail.start()]
            + ", "
            + json.dumps(legacy_tail.group(1))
            + "]"
        )
    return json.loads(body)


def _find_content_page(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        if isinstance(value.get("content"), list):
            return value
        for child in value.values():
            found = _find_content_page(child)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_content_page(child)
            if found is not None:
                return found
    return None


def _fetch_bse_bulk_trades(
    start_date: str,
    end_date: str,
) -> dict[tuple[str, str], dict[str, float]]:
    """Fetch official BSE bulk trades and aggregate them by security/date."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.bse.cn/disclosure/agreement_transfer.html",
        "X-Requested-With": "XMLHttpRequest",
    })
    aggregated: dict[tuple[str, str], dict[str, float]] = {}
    page_number = 0
    while True:
        params = {
            "page": page_number,
            "startDate": start_date,
            "endDate": end_date,
            "btzqdm": "",
            "keywords": "",
            "xxfcbj": "2",
            "xxfcbjs": "2",
            "position": "now",
            "sortfield": "hqjsrq",
            "sorttype": "asc",
            "callback": "probigaBseBulk",
        }
        response = session.get(
            _BSE_BULK_TRADE_API,
            params=params,
            timeout=25,
        )
        response.raise_for_status()
        page = _find_content_page(_parse_bse_jsonp(response.text))
        if page is None:
            raise ValueError("BSE bulk-trade response has no content page")
        for item in page.get("content") or []:
            code = str(item.get("hqzqdm") or "").strip().zfill(6)
            raw_date = str(item.get("hqjsrq") or "").strip()
            if not code.startswith("92") or len(raw_date) != 8:
                continue
            trade_date = (
                f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
            )
            price = float(item.get("hqcjjg") or 0)
            volume = float(item.get("hqcjsl") or 0)
            if price <= 0 or volume <= 0:
                raise ValueError(
                    f"invalid official BSE bulk trade: {code} {trade_date}"
                )
            key = (code, trade_date)
            total = aggregated.setdefault(
                key,
                {"volume": 0.0, "amount": 0.0, "trade_count": 0.0},
            )
            total["volume"] += volume
            total["amount"] += price * volume
            total["trade_count"] += 1.0
        total_pages = int(page.get("totalPages") or 0)
        if page_number + 1 >= max(total_pages, 1):
            break
        page_number += 1
    return aggregated


def _reference_pre_close(
    raw_close: float,
    target_date: str,
    adjusted_rows: list[dict[str, Any]],
) -> float | None:
    ordered = sorted(
        (
            row for row in adjusted_rows
            if str(row.get("trade_date") or "")[:10] <= target_date
        ),
        key=lambda row: str(row.get("trade_date") or "")[:10],
    )
    if len(ordered) < 2:
        return None
    current = ordered[-1]
    previous = ordered[-2]
    if str(current.get("trade_date") or "")[:10] != target_date:
        return None
    current_close = float(current.get("close") or 0)
    previous_close = float(previous.get("close") or 0)
    if raw_close <= 0 or current_close <= 0 or previous_close <= 0:
        return None
    return raw_close / (current_close / previous_close)


def _match_bar_totals(
    primary: dict[str, float],
    reference: dict[str, float],
    primary_volume: float,
    reference_volume: float,
) -> tuple[bool, dict[str, float]]:
    matched, differences = _rows_match(
        primary,
        reference,
        price_tolerance=0.011,
        amount_relative_tolerance=0.002,
    )
    volume_difference = abs(primary_volume - reference_volume)
    differences["volume"] = round(volume_difference, 6)
    if volume_difference > max(
        2.0,
        abs(reference_volume) * 0.000001,
    ):
        matched = False
    return matched, differences


def _fetch_code(
    code: str,
    short_name: str,
    target_dates: list[str],
    history_start: str,
    end_date: str,
    *,
    request_delay: float,
    official_bulk_trades: dict[tuple[str, str], dict[str, float]] | None = None,
) -> CodeOutcome:
    try:
        sina = _sina_history(code, short_name, history_start, end_date)
        ths_actual = _fetch_ths_history(code, count=120, adjust_type="actual")
        ths_forward = _fetch_ths_history(code, count=120, adjust_type="forward")
        primary_by_date = _records_by_date(sina)
        reference_by_date = _records_by_date(ths_actual)
        adjusted_rows = ths_forward.to_dict(orient="records") if not ths_forward.empty else []

        rows: list[dict[str, Any]] = []
        outcome = CodeOutcome(code=code)
        for target_date in target_dates:
            primary = primary_by_date.get(target_date)
            reference = reference_by_date.get(target_date)
            if primary is None and reference is None:
                outcome.no_data_dates.append(target_date)
                continue
            if primary is not None and reference is None:
                outcome.primary_only_dates.append(target_date)
                continue
            if primary is None and reference is not None:
                outcome.reference_only_dates.append(target_date)
                continue
            assert primary is not None and reference is not None
            official_bulk = (official_bulk_trades or {}).get(
                (code, target_date),
                {},
            )
            official_bulk_volume = float(official_bulk.get("volume") or 0)
            official_bulk_amount = float(official_bulk.get("amount") or 0)
            primary_values = {
                column: float(primary[column])
                for column in ("open", "high", "low", "close")
            }
            primary_values["amount"] = float(primary.get("amount") or 0)
            reference_values = {
                column: float(reference[column])
                for column in ("open", "high", "low", "close", "amount")
            }
            primary_volume = float(primary.get("volume") or 0)
            reference_volume = float(reference.get("volume") or 0)
            matched, differences = _match_bar_totals(
                primary_values,
                reference_values,
                primary_volume,
                reference_volume,
            )
            selected_volume = reference_volume
            selected_amount = float(reference.get("amount") or 0)
            source_name = "sina"
            reconciliation = "vendors_match"

            # Provider quote totals do not expose a stable flag indicating
            # whether official BSE bulk trades are already included. First
            # accept a direct vendor match. Only when the vendors differ do
            # we use the official bulk list to reconcile either direction,
            # selecting the larger exchange-total representation.
            if not matched and official_bulk_volume > 0:
                primary_plus_bulk = dict(primary_values)
                primary_plus_bulk["amount"] += official_bulk_amount
                plus_primary_matched, plus_primary_differences = (
                    _match_bar_totals(
                        primary_plus_bulk,
                        reference_values,
                        primary_volume + official_bulk_volume,
                        reference_volume,
                    )
                )
                reference_plus_bulk = dict(reference_values)
                reference_plus_bulk["amount"] += official_bulk_amount
                plus_reference_matched, plus_reference_differences = (
                    _match_bar_totals(
                        primary_values,
                        reference_plus_bulk,
                        primary_volume,
                        reference_volume + official_bulk_volume,
                    )
                )
                if plus_primary_matched:
                    matched = True
                    differences = plus_primary_differences
                    source_name = "sina+bse_official_bulk"
                    reconciliation = "official_bulk_added_to_sina"
                elif plus_reference_matched:
                    matched = True
                    differences = plus_reference_differences
                    selected_volume = primary_volume
                    selected_amount = float(primary_values["amount"])
                    source_name = "sina+bse_official_bulk"
                    reconciliation = "official_bulk_added_to_ths"
            if not matched:
                outcome.mismatch_rows.append({
                    "stock_code": code,
                    "trade_date": target_date,
                    "differences": differences,
                    "primary": primary_values,
                    "reference": reference_values,
                })
                continue

            row = dict(primary)
            row["volume"] = selected_volume
            row["amount"] = selected_amount
            close = float(row.get("close") or 0)
            pre_close = _reference_pre_close(
                close,
                target_date,
                adjusted_rows,
            )
            row["pre_close"] = pre_close
            row["change"] = close - pre_close if pre_close and pre_close > 0 else None
            row["change_pct"] = (
                (close / pre_close - 1.0) * 100.0
                if pre_close and pre_close > 0
                else None
            )
            row["_data_source"] = source_name
            row["_cross_validation"] = reconciliation
            rows.append(row)
            outcome.matched_rows += 1
        outcome.frame = pd.DataFrame(rows)
        if request_delay > 0:
            time.sleep(request_delay + random.uniform(0, request_delay))
        return outcome
    except Exception as exc:  # pylint: disable=broad-except
        return CodeOutcome(
            code=code,
            error=f"{type(exc).__name__}: {str(exc)[:300]}",
        )


def _build_frames(
    outcomes: list[CodeOutcome],
    target_dates: list[str],
) -> dict[str, pd.DataFrame]:
    parts = [item.frame for item in outcomes if not item.frame.empty]
    combined = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    by_date: dict[str, pd.DataFrame] = {}
    for target_date in target_dates:
        frame = combined[
            combined["trade_date"].astype(str).str[:10] == target_date
        ].copy() if not combined.empty else pd.DataFrame()
        if not frame.empty:
            frame = _validate_daily_frame(frame, target_date)
        by_date[target_date] = frame
    return by_date


def _run_records(
    by_date: dict[str, pd.DataFrame],
    expected_by_date: dict[str, int],
    *,
    started_at: datetime,
) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    finished_at = datetime.now().replace(microsecond=0)
    run_records: list[dict[str, Any]] = []
    traces: list[pd.DataFrame] = []
    for target_date, frame in by_date.items():
        run_id = str(uuid.uuid4())
        verified_codes = {
            str(code).zfill(6): "ths"
            for code in frame["stock_code"].tolist()
        }
        expected = expected_by_date[target_date]
        coverage = len(frame) / max(expected, 1)
        trace = _build_source_trace(
            frame,
            run_id=run_id,
            verified_codes=verified_codes,
            fetched_at=finished_at,
        )
        traces.append(trace)
        cross_validation = {
            "status": "pass",
            "primary_source": "sina",
            "reference_source": "ths",
            "compared": len(frame),
            "matched": len(frame),
            "mismatched": 0,
            "unavailable": 0,
        }
        run_records.append({
            "run_id": run_id,
            "target_date": target_date,
            "mode": "bse_dual_source_gap_repair",
            "source_chain": "sina,bse_official_bulk,ths",
            "universe_source": "si_all_code:list_date",
            "expected_count": expected,
            "fetched_count": len(frame),
            "coverage": round(coverage, 6),
            "source_counts_json": json.dumps(
                {
                    str(source): int(count)
                    for source, count in frame["_data_source"].value_counts().items()
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            "cross_validation_json": json.dumps(
                cross_validation,
                ensure_ascii=False,
                sort_keys=True,
            ),
            "dataset_sha256": _dataset_hash(frame),
            "status": "written",
            "started_at": started_at,
            "finished_at": finished_at,
        })
    return run_records, pd.concat(traces, ignore_index=True)


def _write_range(
    engine: Engine,
    by_date: dict[str, pd.DataFrame],
    expected_by_date: dict[str, int],
    *,
    started_at: datetime,
    provenance: bool,
) -> int:
    combined = pd.concat(list(by_date.values()), ignore_index=True)
    combined = combined.replace({np.nan: None, pd.NaT: None})
    combined["etl_sync_at"] = datetime.now().replace(microsecond=0)
    run_records: list[dict[str, Any]] = []
    traces = pd.DataFrame()
    if provenance:
        _ensure_provenance_tables(engine)
        run_records, traces = _run_records(
            by_date,
            expected_by_date,
            started_at=started_at,
        )
    range_start = min(by_date)
    range_end = max(by_date)

    with engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM sm_stock_kline
            WHERE trade_date BETWEEN :range_start AND :range_end
              AND k_type = 1
              AND adjust_type = 0
              AND stock_code REGEXP '^92'
        """), {
            "range_start": range_start,
            "range_end": range_end,
        })
        written = write_frame(
            combined[BUSINESS_COLUMNS],
            "sm_stock_kline",
            conn,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",
        )
        if provenance:
            conn.execute(text("""
                DELETE FROM st_kline_source_trace
                WHERE trade_date BETWEEN :range_start AND :range_end
                  AND k_type = 1
                  AND adjust_type = 0
                  AND stock_code REGEXP '^92'
            """), {
                "range_start": range_start,
                "range_end": range_end,
            })
            write_frame(
                traces,
                "st_kline_source_trace",
                conn,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
            )
            conn.execute(text("""
                INSERT INTO st_kline_ingestion_run (
                  run_id, target_date, mode, source_chain, universe_source,
                  expected_count, fetched_count, coverage, source_counts_json,
                  cross_validation_json, dataset_sha256, status, started_at,
                  finished_at
                ) VALUES (
                  :run_id, :target_date, :mode, :source_chain, :universe_source,
                  :expected_count, :fetched_count, :coverage, :source_counts_json,
                  :cross_validation_json, :dataset_sha256, :status, :started_at,
                  :finished_at
                )
            """), run_records)
    return int(written or len(combined))


def backfill_bse_gap(
    start_date: str,
    end_date: str,
    *,
    min_coverage: float = 0.97,
    workers: int = 4,
    apply: bool = False,
) -> int:
    started_at = datetime.now().replace(microsecond=0)
    engine = create_batch_engine()
    target_dates = _load_target_dates(engine, start_date, end_date)
    if not target_dates:
        print("No open trading dates in requested range")
        return 2
    codes, short_names, list_dates = _load_universe(engine, end_date)
    if not codes:
        print("No 920xxx BSE codes in si_all_code")
        return 2

    # Resolve the reference-source credential before worker fan-out.
    _ths_auth_headers()
    official_bulk_trades = _fetch_bse_bulk_trades(
        target_dates[0],
        target_dates[-1],
    )
    history_start = (
        datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=45)
    ).strftime("%Y-%m-%d")
    request_delay = max(
        0.0,
        float(os.environ.get("BSE_KLINE_BACKFILL_REQUEST_DELAY", "0.05")),
    )
    print(
        f"BSE dual-source backfill: {start_date}..{end_date}, "
        f"trading_dates={len(target_dates)}, universe={len(codes)}, "
        f"workers={workers}, official_bulk_rows={len(official_bulk_trades)}, "
        f"apply={apply}",
        flush=True,
    )

    outcomes: list[CodeOutcome] = []
    with ThreadPoolExecutor(max_workers=max(1, min(workers, 8))) as pool:
        futures = {
            pool.submit(
                _fetch_code,
                code,
                short_names.get(code, ""),
                target_dates,
                history_start,
                end_date,
                request_delay=request_delay,
                official_bulk_trades=official_bulk_trades,
            ): code
            for code in codes
        }
        for done, future in enumerate(as_completed(futures), start=1):
            outcome = future.result()
            outcomes.append(outcome)
            if outcome.error:
                print(
                    f"  {outcome.code} error: {outcome.error}",
                    flush=True,
                )
            if done % 50 == 0 or done == len(futures):
                print(
                    f"  progress {done}/{len(futures)}",
                    flush=True,
                )

    errors = [item for item in outcomes if item.error]
    primary_only = [
        (item.code, target_date)
        for item in outcomes
        for target_date in item.primary_only_dates
    ]
    reference_only = [
        (item.code, target_date)
        for item in outcomes
        for target_date in item.reference_only_dates
    ]
    mismatches = [
        row
        for item in outcomes
        for row in item.mismatch_rows
    ]
    by_date = _build_frames(outcomes, target_dates)
    expected_by_date = {
        target_date: sum(
            not list_dates.get(code) or list_dates[code] <= target_date
            for code in codes
        )
        for target_date in target_dates
    }
    coverage_by_date = {
        target_date: (
            len(by_date[target_date]) / max(expected_by_date[target_date], 1)
        )
        for target_date in target_dates
    }
    summary = {
        "status": "pass",
        "target_dates": target_dates,
        "universe_count": len(codes),
        "expected_by_date": expected_by_date,
        "matched_by_date": {
            target_date: len(by_date[target_date])
            for target_date in target_dates
        },
        "coverage_by_date": {
            target_date: round(value, 6)
            for target_date, value in coverage_by_date.items()
        },
        "worker_errors": len(errors),
        "primary_only_rows": len(primary_only),
        "reference_only_rows": len(reference_only),
        "mismatched_rows": len(mismatches),
        "mismatch_samples": mismatches[:5],
        "primary_only_samples": primary_only[:10],
        "reference_only_samples": reference_only[:10],
    }
    blocked = (
        bool(errors)
        or bool(primary_only)
        or bool(reference_only)
        or bool(mismatches)
        or any(value < min_coverage for value in coverage_by_date.values())
    )
    if blocked:
        summary["status"] = "fail"
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
        print("BSE backfill blocked; existing production rows retained")
        return 3

    print(
        json.dumps(
            summary,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        flush=True,
    )
    if not apply:
        hashes = {
            target_date: _dataset_hash(frame)
            for target_date, frame in by_date.items()
        }
        print(f"[dry-run] all gates passed; database unchanged; hashes={hashes}")
        return 0

    written = _write_range(
        engine,
        by_date,
        expected_by_date,
        started_at=started_at,
        provenance=True,
    )
    mirror_url = _distinct_kline_url(str(engine.url))
    mirrored = 0
    if mirror_url:
        mirror_engine = create_batch_engine(mirror_url)
        mirrored = _write_range(
            mirror_engine,
            by_date,
            expected_by_date,
            started_at=started_at,
            provenance=False,
        )
    print(
        f"BSE gap write completed: primary_rows={written}, "
        f"mirror_rows={mirrored}"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Dual-source repair for missing 920xxx BSE daily K-lines."
    )
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--min-coverage", type=float, default=0.97)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write after all dual-source gates pass. Default is dry-run.",
    )
    args = parser.parse_args()
    engine = create_batch_engine()
    lock_timeout = max(
        0,
        int(os.environ.get("BSE_KLINE_BACKFILL_LOCK_TIMEOUT", "60")),
    )
    with mysql_named_lock(
        engine,
        "probiga:stock_kline_daily",
        timeout_seconds=lock_timeout,
    ):
        return backfill_bse_gap(
            args.start_date,
            args.end_date,
            min_coverage=max(0.0, min(1.0, args.min_coverage)),
            workers=max(1, args.workers),
            apply=args.apply,
        )


if __name__ == "__main__":
    raise SystemExit(main())
